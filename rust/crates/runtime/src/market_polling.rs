use std::{collections::HashSet, sync::Arc, time::Duration};

use crypto_trading_domain::{MarketType, Symbol};
use crypto_trading_exchange::BinancePublicExchange;

use crate::{
    market_data::{
        MAX_MARKET_DATA_TARGETS, MAX_MARKET_SYMBOL_BYTES, MarketDataClock, MarketDataError,
        MarketDataEvent, MarketDataObservation, MarketInstrument, classify_exchange_failure,
    },
    market_supervisor::{MarketDataEventFuture, MarketDataEventSource},
};

const BINANCE_EXCHANGE: &str = "binance";
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
