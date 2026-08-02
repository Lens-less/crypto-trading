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
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

use crate::{
    market_data::{
        MAX_MARKET_DATA_TARGETS, MAX_MARKET_SYMBOL_BYTES, MarketDataClock, MarketDataError,
        MarketDataEvent, MarketDataObservation, MarketInstrument, MarketTimestampProvenance,
        classify_exchange_failure,
    },
    market_supervisor::{MarketDataEventFuture, MarketDataEventSource},
};

const BINANCE_EXCHANGE: &str = "binance";
const HYPERLIQUID_EXCHANGE: &str = "hyperliquid";
const MAX_POLL_DELAY: Duration = Duration::from_secs(60 * 60);
const SLOW_POLL_FLOOR: Duration = Duration::from_secs(1);

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
    next_poll_at: Instant,
}

impl PollingTarget {
    fn new(route: BinancePollingRoute) -> Self {
        Self {
            route,
            revision: 0,
            consecutive_failures: 0,
            next_poll_at: Instant::now(),
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
        let poll_started_at = wait_for_poll_slot(self.targets[index].next_poll_at).await;
        let instrument = self.targets[index].route.instrument.clone();
        let wire_symbol = self.targets[index].route.wire_symbol.clone();
        let target_count = self.targets.len();
        let result = self
            .exchange
            .fetch_observation(&wire_symbol, self.clock.now())
            .await;
        let received_at = self.clock.now();
        self.advance_cursor();

        match result {
            Ok(observation) => {
                let target = &mut self.targets[index];
                let instrument_label = instrument.symbol.as_str().to_owned();
                // REST book-ticker observations are complete snapshots. A
                // venue update id orders those snapshots, but skipped ids do
                // not prove that this polling consumer dropped a snapshot.
                // Keep delivery continuity on the local per-route counter and
                // carry the venue id separately as source evidence.
                let revision = target
                    .revision
                    .checked_add(1)
                    .ok_or(MarketDataError::RevisionExhausted)?;
                target.revision = revision;
                target.consecutive_failures = 0;
                let backoff = self.policy.poll_interval;
                target.next_poll_at = schedule_next_poll(poll_started_at, backoff);
                let provenance = if let Some(event_time) = observation.event_time {
                    let mut snapshot = observation.snapshot;
                    snapshot.timestamp = event_time;
                    (snapshot, MarketTimestampProvenance::VenueEventTime)
                } else {
                    let mut snapshot = observation.snapshot;
                    snapshot.timestamp = received_at;
                    (snapshot, MarketTimestampProvenance::LocalReceipt)
                };
                let (mut snapshot, timestamp_provenance) = provenance;
                snapshot.symbol = instrument.symbol;
                snapshot.market_type = instrument.market_type;
                let elapsed = poll_started_at.elapsed();
                log_poll_result(
                    BINANCE_EXCHANGE,
                    &instrument_label,
                    target_count,
                    elapsed,
                    "success",
                    Some(backoff),
                    false,
                );
                Ok(Some(MarketDataEvent::Observation(
                    MarketDataObservation::with_source_metadata(
                        snapshot,
                        revision,
                        received_at,
                        timestamp_provenance,
                        observation.source_sequence,
                    )?,
                )))
            }
            Err(error) => {
                let target = &mut self.targets[index];
                target.consecutive_failures = target.consecutive_failures.saturating_add(1);
                let backoff = self.policy.retry_delay_after(target.consecutive_failures);
                target.next_poll_at = schedule_next_poll(poll_started_at, backoff);
                let failure = classify_exchange_failure(&error);
                let elapsed = poll_started_at.elapsed();
                log_poll_result(
                    BINANCE_EXCHANGE,
                    instrument.symbol.as_str(),
                    target_count,
                    elapsed,
                    failure_label(failure),
                    Some(backoff),
                    true,
                );
                Ok(Some(MarketDataEvent::SourceUnavailable {
                    exchange: BINANCE_EXCHANGE.to_owned(),
                    failure,
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
    next_poll_at: Instant,
}

impl HyperliquidPollingTarget {
    fn new(route: HyperliquidPollingRoute) -> Self {
        Self {
            route,
            revision: 0,
            consecutive_failures: 0,
            next_poll_at: Instant::now(),
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
        let poll_started_at = wait_for_poll_slot(self.targets[index].next_poll_at).await;
        let instrument = self.targets[index].route.instrument.clone();
        let wire_coin = self.targets[index].route.wire_coin.clone();
        let target_count = self.targets.len();
        let result = self
            .exchange
            .fetch_observation_at(wire_coin.as_str(), self.clock.now())
            .await;
        let received_at = self.clock.now();
        self.advance_cursor();

        match result {
            Ok(observation) => {
                let target = &mut self.targets[index];
                let revision = target
                    .revision
                    .checked_add(1)
                    .ok_or(MarketDataError::RevisionExhausted)?;
                target.revision = revision;
                target.consecutive_failures = 0;
                let backoff = self.policy.poll_interval;
                target.next_poll_at = schedule_next_poll(poll_started_at, backoff);
                let mut snapshot = observation.snapshot;
                let timestamp_provenance = if let Some(event_time) = observation.event_time {
                    snapshot.timestamp = event_time;
                    MarketTimestampProvenance::VenueEventTime
                } else {
                    snapshot.timestamp = received_at;
                    MarketTimestampProvenance::LocalReceipt
                };
                snapshot.symbol = instrument.symbol.clone();
                snapshot.market_type = instrument.market_type;
                if let Some(rate) = observation.funding {
                    self.funding.record(
                        instrument,
                        FundingRateObservation {
                            rate,
                            revision,
                            observed_at: received_at,
                        },
                    );
                }
                let elapsed = poll_started_at.elapsed();
                log_poll_result(
                    HYPERLIQUID_EXCHANGE,
                    wire_coin.as_str(),
                    target_count,
                    elapsed,
                    "success",
                    Some(backoff),
                    false,
                );
                Ok(Some(MarketDataEvent::Observation(
                    MarketDataObservation::with_source_metadata(
                        snapshot,
                        revision,
                        received_at,
                        timestamp_provenance,
                        None,
                    )?,
                )))
            }
            Err(error) => {
                let target = &mut self.targets[index];
                target.consecutive_failures = target.consecutive_failures.saturating_add(1);
                let backoff = self.policy.retry_delay_after(target.consecutive_failures);
                target.next_poll_at = schedule_next_poll(poll_started_at, backoff);
                let failure = classify_exchange_failure(&error);
                let elapsed = poll_started_at.elapsed();
                log_poll_result(
                    HYPERLIQUID_EXCHANGE,
                    wire_coin.as_str(),
                    target_count,
                    elapsed,
                    failure_label(failure),
                    Some(backoff),
                    true,
                );
                Ok(Some(MarketDataEvent::SourceUnavailable {
                    exchange: HYPERLIQUID_EXCHANGE.to_owned(),
                    failure,
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

async fn wait_for_poll_slot(next_poll_at: Instant) -> Instant {
    if next_poll_at > Instant::now() {
        sleep_until(next_poll_at).await;
    }
    Instant::now()
}

fn schedule_next_poll(poll_started_at: Instant, backoff: Duration) -> Instant {
    poll_started_at + backoff
}

fn failure_label(failure: crate::MarketDataSourceFailure) -> &'static str {
    match failure {
        crate::MarketDataSourceFailure::Disconnected => "disconnected",
        crate::MarketDataSourceFailure::TimedOut => "timed_out",
        crate::MarketDataSourceFailure::Backpressure => "backpressure",
        crate::MarketDataSourceFailure::InvalidPayload => "invalid_payload",
        crate::MarketDataSourceFailure::Rejected => "rejected",
        crate::MarketDataSourceFailure::Unknown => "unknown",
    }
}

fn log_poll_result(
    venue: &str,
    instrument: &str,
    target_count: usize,
    elapsed: Duration,
    result_class: &'static str,
    backoff: Option<Duration>,
    failed: bool,
) {
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    let backoff_ms = backoff.map_or(0, |value| {
        u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
    });
    if failed || elapsed >= SLOW_POLL_FLOOR {
        warn!(
            venue,
            instrument, target_count, elapsed_ms, result_class, backoff_ms, "market polling cycle"
        );
    } else {
        debug!(
            venue,
            instrument, target_count, elapsed_ms, result_class, backoff_ms, "market polling cycle"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{schedule_next_poll, wait_for_poll_slot};
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test]
    async fn round_scheduler_waits_once_for_two_ready_targets() {
        let round_delay = Duration::from_millis(20);

        let first_started = wait_for_poll_slot(Instant::now()).await;
        let second_started = wait_for_poll_slot(Instant::now()).await;
        let first_next = schedule_next_poll(first_started, round_delay);
        let second_next = schedule_next_poll(second_started, round_delay);

        assert!(second_started.duration_since(first_started) < Duration::from_millis(10));

        let third_started = wait_for_poll_slot(first_next).await;
        let fourth_wait_started = Instant::now();
        let fourth_started = wait_for_poll_slot(second_next).await;

        assert!(third_started.duration_since(second_started) >= round_delay);
        assert!(fourth_started.duration_since(fourth_wait_started) < Duration::from_millis(10));
    }
}
