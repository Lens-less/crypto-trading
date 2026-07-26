use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Symbol};
use crypto_trading_exchange::{
    BinancePublicExchange, HyperliquidFundingRate, HyperliquidPublicExchange,
};

use crate::{
    market_data::{
        MAX_MARKET_DATA_TARGETS, MAX_MARKET_SYMBOL_BYTES, MarketDataClock, MarketDataError,
        MarketDataEvent, MarketDataObservation, MarketInstrument, classify_exchange_failure,
    },
    market_supervisor::{MarketDataEventFuture, MarketDataEventSource},
};

const BINANCE_EXCHANGE: &str = "binance";
const HYPERLIQUID_EXCHANGE: &str = "hyperliquid";
const MAX_POLL_DELAY: Duration = Duration::from_secs(60 * 60);

/// Exact mapping from one canonical read-plane instrument to a Binance wire
/// symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinancePollingRoute {
    instrument: MarketInstrument,
    wire_symbol: Symbol,
}

impl BinancePollingRoute {
    /// Creates one explicit route without synthesizing an exchange symbol.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::SymbolTooLong`] if the wire symbol exceeds
    /// the read-plane symbol bound.
    pub fn new(instrument: MarketInstrument, wire_symbol: Symbol) -> Result<Self, MarketDataError> {
        if wire_symbol.as_str().len() > MAX_MARKET_SYMBOL_BYTES {
            return Err(MarketDataError::SymbolTooLong {
                bytes: wire_symbol.as_str().len(),
                limit: MAX_MARKET_SYMBOL_BYTES,
            });
        }
        Ok(Self {
            instrument,
            wire_symbol,
        })
    }

    pub const fn instrument(&self) -> &MarketInstrument {
        &self.instrument
    }

    pub const fn wire_symbol(&self) -> &Symbol {
        &self.wire_symbol
    }
}

/// Deterministic cadence and reconnect backoff for a polling source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketPollingPolicy {
    poll_interval: Duration,
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
}

impl MarketPollingPolicy {
    /// Creates a bounded deterministic exponential-backoff policy.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InvalidPollingPolicy`] for zero, oversized,
    /// or inverted durations.
    pub fn new(
        poll_interval: Duration,
        initial_retry_delay: Duration,
        max_retry_delay: Duration,
    ) -> Result<Self, MarketDataError> {
        for (label, value) in [
            ("poll interval", poll_interval),
            ("initial retry delay", initial_retry_delay),
            ("maximum retry delay", max_retry_delay),
        ] {
            if value.is_zero() {
                return Err(MarketDataError::InvalidPollingPolicy(match label {
                    "poll interval" => "poll interval must be positive",
                    "initial retry delay" => "initial retry delay must be positive",
                    _ => "maximum retry delay must be positive",
                }));
            }
            if value > MAX_POLL_DELAY {
                return Err(MarketDataError::InvalidPollingPolicy(match label {
                    "poll interval" => "poll interval exceeds the one-hour hard limit",
                    "initial retry delay" => "initial retry delay exceeds the one-hour hard limit",
                    _ => "maximum retry delay exceeds the one-hour hard limit",
                }));
            }
        }
        if initial_retry_delay > max_retry_delay {
            return Err(MarketDataError::InvalidPollingPolicy(
                "initial retry delay must not exceed the maximum",
            ));
        }
        Ok(Self {
            poll_interval,
            initial_retry_delay,
            max_retry_delay,
        })
    }

    pub const fn poll_interval(self) -> Duration {
        self.poll_interval
    }

    pub const fn initial_retry_delay(self) -> Duration {
        self.initial_retry_delay
    }

    pub const fn max_retry_delay(self) -> Duration {
        self.max_retry_delay
    }

    /// Returns the deterministic delay after the given number of consecutive
    /// failures.
    ///
    /// Zero failures have no retry delay. Positive counts grow exponentially
    /// from the configured initial delay and saturate at the configured
    /// maximum without integer overflow.
    pub fn retry_delay_after(self, consecutive_failures: u32) -> Duration {
        if consecutive_failures == 0 {
            return Duration::ZERO;
        }
        let exponent = consecutive_failures.saturating_sub(1).min(31);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        self.initial_retry_delay
            .checked_mul(multiplier)
            .unwrap_or(self.max_retry_delay)
            .min(self.max_retry_delay)
    }
}

#[derive(Debug, Clone)]
struct PollingTarget {
    route: BinancePollingRoute,
    revision: u64,
    consecutive_failures: u32,
    next_delay: Duration,
}

impl PollingTarget {
    fn new(route: BinancePollingRoute) -> Self {
        Self {
            route,
            revision: 0,
            consecutive_failures: 0,
            next_delay: Duration::ZERO,
        }
    }
}

/// Long-lived, credential-free Binance Spot polling adapter.
///
/// The adapter accepts explicit canonical-to-wire routes, polls one target at
/// a time in deterministic round-robin order, and converts recoverable venue
/// failures into source-unavailable events. It exposes no order, account, or
/// reconciliation authority.
#[derive(Debug)]
pub struct BinancePublicPollingSource {
    exchange: BinancePublicExchange,
    targets: Vec<PollingTarget>,
    cursor: usize,
    policy: MarketPollingPolicy,
    clock: Arc<dyn MarketDataClock>,
}

impl BinancePublicPollingSource {
    /// Binds a Binance public adapter to a bounded exact Spot universe.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for an empty/oversized route set,
    /// non-Binance or non-Spot instruments, or ambiguous canonical/wire
    /// mappings.
    pub fn new<C>(
        exchange: BinancePublicExchange,
        mut routes: Vec<BinancePollingRoute>,
        policy: MarketPollingPolicy,
        clock: Arc<C>,
    ) -> Result<Self, MarketDataError>
    where
        C: MarketDataClock + 'static,
    {
        if routes.is_empty() {
            return Err(MarketDataError::EmptyUniverse);
        }
        if routes.len() > MAX_MARKET_DATA_TARGETS {
            return Err(MarketDataError::UniverseTooLarge {
                count: routes.len(),
                limit: MAX_MARKET_DATA_TARGETS,
            });
        }
        routes.sort_by(|left, right| left.instrument.cmp(&right.instrument));
        if let Some(duplicate) = routes
            .windows(2)
            .find(|pair| pair[0].instrument == pair[1].instrument)
            .map(|pair| pair[0].instrument.clone())
        {
            return Err(MarketDataError::DuplicatePollingInstrument {
                instrument: duplicate,
            });
        }
        let mut wire_symbols = HashSet::with_capacity(routes.len());
        for route in &routes {
            if route.instrument.exchange() != BINANCE_EXCHANGE
                || route.instrument.market_type != MarketType::Spot
            {
                return Err(MarketDataError::UnsupportedPollingInstrument {
                    instrument: route.instrument.clone(),
                });
            }
            if !wire_symbols.insert(route.wire_symbol.as_str().to_owned()) {
                return Err(MarketDataError::DuplicatePollingWireSymbol {
                    symbol: route.wire_symbol.clone(),
                });
            }
        }
        Ok(Self {
            exchange,
            targets: routes.into_iter().map(PollingTarget::new).collect(),
            cursor: 0,
            policy,
            clock,
        })
    }

    fn advance_cursor(&mut self) {
        self.cursor = (self.cursor + 1) % self.targets.len();
    }

    async fn fetch_next(&mut self) -> Result<Option<MarketDataEvent>, MarketDataError> {
        let index = self.cursor;
        let delay = self.targets[index].next_delay;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        let instrument = self.targets[index].route.instrument.clone();
        let wire_symbol = self.targets[index].route.wire_symbol.clone();
        let result = self.exchange.fetch_snapshot(&wire_symbol).await;
        let received_at = self.clock.now();
        self.advance_cursor();

        match result {
            Ok(mut snapshot) => {
                let target = &mut self.targets[index];
                target.revision = target
                    .revision
                    .checked_add(1)
                    .ok_or(MarketDataError::RevisionExhausted)?;
                target.consecutive_failures = 0;
                target.next_delay = self.policy.poll_interval;
                snapshot.symbol = instrument.symbol;
                snapshot.market_type = instrument.market_type;
                Ok(Some(MarketDataEvent::Observation(
                    MarketDataObservation::new(snapshot, target.revision, received_at)?,
                )))
            }
            Err(error) => {
                let target = &mut self.targets[index];
                target.consecutive_failures = target.consecutive_failures.saturating_add(1);
                target.next_delay = self.policy.retry_delay_after(target.consecutive_failures);
                Ok(Some(MarketDataEvent::SourceUnavailable {
                    exchange: BINANCE_EXCHANGE.to_owned(),
                    failure: classify_exchange_failure(&error),
                    observed_at: received_at,
                }))
            }
        }
    }
}

impl MarketDataEventSource for BinancePublicPollingSource {
    fn source_id(&self) -> &str {
        BINANCE_EXCHANGE
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(self.fetch_next())
    }
}

/// Exact mapping from one canonical read-plane instrument to a Hyperliquid
/// perpetual wire coin (for example `BTCUSDT` -> `BTC`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidPollingRoute {
    instrument: MarketInstrument,
    wire_coin: Symbol,
}

impl HyperliquidPollingRoute {
    /// Creates one explicit route without synthesizing an exchange coin.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::SymbolTooLong`] if the wire coin exceeds the
    /// read-plane symbol bound.
    pub fn new(instrument: MarketInstrument, wire_coin: Symbol) -> Result<Self, MarketDataError> {
        if wire_coin.as_str().len() > MAX_MARKET_SYMBOL_BYTES {
            return Err(MarketDataError::SymbolTooLong {
                bytes: wire_coin.as_str().len(),
                limit: MAX_MARKET_SYMBOL_BYTES,
            });
        }
        Ok(Self {
            instrument,
            wire_coin,
        })
    }

    pub const fn instrument(&self) -> &MarketInstrument {
        &self.instrument
    }

    pub const fn wire_coin(&self) -> &Symbol {
        &self.wire_coin
    }
}

/// One retained funding-rate sample from the Hyperliquid public feed.
///
/// The rate is the venue's hourly funding fraction; the observation carries
/// the polling revision and receive time of the snapshot it arrived with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRateObservation {
    pub rate: HyperliquidFundingRate,
    pub revision: u64,
    pub observed_at: DateTime<Utc>,
}

/// Cloneable read-only side channel carrying the latest funding rate per
/// instrument.
///
/// The domain [`crypto_trading_domain::MarketSnapshot`] and the adapter-neutral
/// market-data event contract deliberately stay funding-free; this feed is the
/// minimal-intrusion path for funding-aware consumers. It is bounded by the
/// polling source's exact route set and never fabricates a rate: instruments
/// whose venue context omits funding simply stay absent.
#[derive(Debug, Clone, Default)]
pub struct FundingRateFeed {
    latest: Arc<Mutex<HashMap<MarketInstrument, FundingRateObservation>>>,
}

impl FundingRateFeed {
    /// Returns the latest funding observation for one exact instrument, if the
    /// source has published one.
    #[must_use]
    pub fn latest(&self, instrument: &MarketInstrument) -> Option<FundingRateObservation> {
        self.lock().get(instrument).cloned()
    }

    fn record(&self, instrument: MarketInstrument, observation: FundingRateObservation) {
        self.lock().insert(instrument, observation);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MarketInstrument, FundingRateObservation>> {
        // A poisoned lock only means a panicking writer left a fully written
        // map behind; the last coherent samples remain safe to read.
        self.latest.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug, Clone)]
struct HyperliquidPollingTarget {
    route: HyperliquidPollingRoute,
    revision: u64,
    consecutive_failures: u32,
    next_delay: Duration,
}

impl HyperliquidPollingTarget {
    fn new(route: HyperliquidPollingRoute) -> Self {
        Self {
            route,
            revision: 0,
            consecutive_failures: 0,
            next_delay: Duration::ZERO,
        }
    }
}

/// Long-lived, credential-free Hyperliquid perpetual polling adapter.
///
/// The adapter accepts explicit canonical-to-coin routes, polls one target at
/// a time in deterministic round-robin order, and converts recoverable venue
/// failures into source-unavailable events. Successful observations also
/// publish the venue's hourly funding rate into a bounded side feed. It
/// exposes no order, account, or reconciliation authority.
#[derive(Debug)]
pub struct HyperliquidPublicPollingSource {
    exchange: HyperliquidPublicExchange,
    targets: Vec<HyperliquidPollingTarget>,
    cursor: usize,
    policy: MarketPollingPolicy,
    clock: Arc<dyn MarketDataClock>,
    funding: FundingRateFeed,
}

impl HyperliquidPublicPollingSource {
    /// Binds a Hyperliquid public adapter to a bounded exact perpetual
    /// universe.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for an empty/oversized route set,
    /// non-Hyperliquid or non-perpetual instruments, or ambiguous
    /// canonical/coin mappings.
    pub fn new<C>(
        exchange: HyperliquidPublicExchange,
        mut routes: Vec<HyperliquidPollingRoute>,
        policy: MarketPollingPolicy,
        clock: Arc<C>,
    ) -> Result<Self, MarketDataError>
    where
        C: MarketDataClock + 'static,
    {
        if routes.is_empty() {
            return Err(MarketDataError::EmptyUniverse);
        }
        if routes.len() > MAX_MARKET_DATA_TARGETS {
            return Err(MarketDataError::UniverseTooLarge {
                count: routes.len(),
                limit: MAX_MARKET_DATA_TARGETS,
            });
        }
        routes.sort_by(|left, right| left.instrument.cmp(&right.instrument));
        if let Some(duplicate) = routes
            .windows(2)
            .find(|pair| pair[0].instrument == pair[1].instrument)
            .map(|pair| pair[0].instrument.clone())
        {
            return Err(MarketDataError::DuplicatePollingInstrument {
                instrument: duplicate,
            });
        }
        let mut wire_coins = HashSet::with_capacity(routes.len());
        for route in &routes {
            if route.instrument.exchange() != HYPERLIQUID_EXCHANGE
                || route.instrument.market_type != MarketType::Perpetual
            {
                return Err(MarketDataError::UnsupportedPollingInstrument {
                    instrument: route.instrument.clone(),
                });
            }
            if !wire_coins.insert(route.wire_coin.as_str().to_owned()) {
                return Err(MarketDataError::DuplicatePollingWireSymbol {
                    symbol: route.wire_coin.clone(),
                });
            }
        }
        Ok(Self {
            exchange,
            targets: routes
                .into_iter()
                .map(HyperliquidPollingTarget::new)
                .collect(),
            cursor: 0,
            policy,
            clock,
            funding: FundingRateFeed::default(),
        })
    }

    /// Returns a cloneable handle over this source's latest funding rates.
    ///
    /// Clone it before the source moves into a supervisor; the handle stays
    /// valid for the lifetime of the polling loop.
    #[must_use]
    pub fn funding_feed(&self) -> FundingRateFeed {
        self.funding.clone()
    }

    fn advance_cursor(&mut self) {
        self.cursor = (self.cursor + 1) % self.targets.len();
    }

    async fn fetch_next(&mut self) -> Result<Option<MarketDataEvent>, MarketDataError> {
        let index = self.cursor;
        let delay = self.targets[index].next_delay;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        let instrument = self.targets[index].route.instrument.clone();
        let wire_coin = self.targets[index].route.wire_coin.clone();
        let result = self.exchange.fetch_observation(wire_coin.as_str()).await;
        let received_at = self.clock.now();
        self.advance_cursor();

        match result {
            Ok(observation) => {
                let target = &mut self.targets[index];
                target.revision = target
                    .revision
                    .checked_add(1)
                    .ok_or(MarketDataError::RevisionExhausted)?;
                target.consecutive_failures = 0;
                target.next_delay = self.policy.poll_interval;
                let mut snapshot = observation.snapshot;
                snapshot.symbol = instrument.symbol.clone();
                snapshot.market_type = instrument.market_type;
                if let Some(rate) = observation.funding {
                    self.funding.record(
                        instrument,
                        FundingRateObservation {
                            rate,
                            revision: target.revision,
                            observed_at: received_at,
                        },
                    );
                }
                Ok(Some(MarketDataEvent::Observation(
                    MarketDataObservation::new(snapshot, target.revision, received_at)?,
                )))
            }
            Err(error) => {
                let target = &mut self.targets[index];
                target.consecutive_failures = target.consecutive_failures.saturating_add(1);
                target.next_delay = self.policy.retry_delay_after(target.consecutive_failures);
                Ok(Some(MarketDataEvent::SourceUnavailable {
                    exchange: HYPERLIQUID_EXCHANGE.to_owned(),
                    failure: classify_exchange_failure(&error),
                    observed_at: received_at,
                }))
            }
        }
    }
}

impl MarketDataEventSource for HyperliquidPublicPollingSource {
    fn source_id(&self) -> &str {
        HYPERLIQUID_EXCHANGE
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(self.fetch_next())
    }
}
