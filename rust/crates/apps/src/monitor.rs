//! Read-only continuous arbitrage analysis over the bounded market-data plane.
//!
//! This module deliberately projects spread facts only. It has no exchange
//! handle, execution policy, order router, or method that can submit an order.

use std::{
    collections::HashMap,
    fmt,
    fs::File,
    io::Read,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result as AnyResult, bail};
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketSnapshot, Price};
use crypto_trading_runtime::{
    DecisionRecord, MAX_MARKET_DATA_EVENTS, MarketContinuity, MarketDataBook, MarketDataClock,
    MarketDataError, MarketDataEvent, MarketDataFreshness, MarketDataObservation, MarketDataUpdate,
    MarketInstrument,
};
use crypto_trading_strategy::{SpreadCalculator, StrategyError};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// Stable schema version for journaled read-only arbitrage monitor events.
pub const ARBITRAGE_MONITOR_EVENT_SCHEMA_VERSION: u16 = 1;
/// Maximum bytes accepted by the deterministic JSONL replay adapter.
pub const MAX_MONITOR_REPLAY_BYTES: u64 = 8 * 1_024 * 1_024;
/// Maximum bytes accepted for one replay snapshot record.
pub const MAX_MONITOR_REPLAY_RECORD_BYTES: usize = 64 * 1_024;

const MAX_MONITOR_REPLAY_SYMBOL_BYTES: usize = 128;
const REPLAY_SNAPSHOT_FIELDS: &[&str] = &[
    "exchange",
    "symbol",
    "market_type",
    "bid",
    "ask",
    "last",
    "bid_quantity",
    "ask_quantity",
    "timestamp",
];

/// Deterministic high-water clock used by finite replay.
#[derive(Clone)]
pub struct ReplayMarketDataClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl ReplayMarketDataClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    /// Advances to the supplied event time without ever moving backwards.
    pub fn advance(&self, candidate: DateTime<Utc>) {
        let mut now = self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now = (*now).max(candidate);
    }
}

impl fmt::Debug for ReplayMarketDataClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplayMarketDataClock")
            .finish_non_exhaustive()
    }
}

impl MarketDataClock for ReplayMarketDataClock {
    fn now(&self) -> DateTime<Utc> {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Loads a strict, bounded JSONL sequence of domain market snapshots and
/// assigns deterministic per-instrument revisions in physical file order.
///
/// # Errors
///
/// Returns an error for non-files, size changes, partial tails, unknown fields,
/// invalid domain snapshots, oversized identities, or event-count overflow.
pub fn load_market_snapshot_replay(path: &Path) -> AnyResult<Vec<MarketDataEvent>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open monitor replay {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect monitor replay {}", path.display()))?;
    if !metadata.is_file() {
        bail!("monitor replay {} must be a file", path.display());
    }
    if metadata.len() > MAX_MONITOR_REPLAY_BYTES {
        bail!(
            "monitor replay has {} bytes; maximum is {MAX_MONITOR_REPLAY_BYTES}",
            metadata.len()
        );
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(usize::MAX)
            .min(usize::try_from(MAX_MONITOR_REPLAY_BYTES).unwrap_or(usize::MAX))
            .saturating_add(1),
    );
    file.take(MAX_MONITOR_REPLAY_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read monitor replay {}", path.display()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_MONITOR_REPLAY_BYTES {
        bail!("monitor replay exceeded the {MAX_MONITOR_REPLAY_BYTES}-byte read limit");
    }
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != metadata.len() {
        bail!("monitor replay changed while it was being read");
    }
    if bytes.is_empty() {
        bail!("monitor replay must contain at least one snapshot");
    }
    if !bytes.ends_with(b"\n") {
        bail!("monitor replay has a partial final record");
    }

    let mut events = Vec::new();
    let mut revisions = HashMap::<MarketInstrument, u64>::new();
    for (index, raw_line) in bytes[..bytes.len() - 1]
        .split(|byte| *byte == b'\n')
        .enumerate()
    {
        let line_number = index.saturating_add(1);
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            bail!("monitor replay record {line_number} is empty");
        }
        if line.len().saturating_add(1) > MAX_MONITOR_REPLAY_RECORD_BYTES {
            bail!(
                "monitor replay record {line_number} exceeds {MAX_MONITOR_REPLAY_RECORD_BYTES} bytes"
            );
        }
        if events.len() >= MAX_MARKET_DATA_EVENTS {
            bail!("monitor replay exceeds {MAX_MARKET_DATA_EVENTS} events");
        }
        let value: Value = serde_json::from_slice(line)
            .with_context(|| format!("monitor replay record {line_number} is invalid JSON"))?;
        let object = value
            .as_object()
            .with_context(|| format!("monitor replay record {line_number} must be an object"))?;
        if let Some(field) = object
            .keys()
            .find(|field| !REPLAY_SNAPSHOT_FIELDS.contains(&field.as_str()))
        {
            bail!("unknown replay field {field} at record {line_number}");
        }
        let snapshot: MarketSnapshot = serde_json::from_value(value)
            .with_context(|| format!("monitor replay record {line_number} is invalid"))?;
        if snapshot.symbol.as_str().len() > MAX_MONITOR_REPLAY_SYMBOL_BYTES {
            bail!(
                "monitor replay symbol at record {line_number} exceeds {MAX_MONITOR_REPLAY_SYMBOL_BYTES} bytes"
            );
        }
        let instrument = MarketInstrument::from_snapshot(&snapshot)
            .with_context(|| format!("monitor replay record {line_number} has invalid identity"))?;
        let revision = revisions.entry(instrument).or_insert(0);
        *revision = revision
            .checked_add(1)
            .context("monitor replay revision counter is exhausted")?;
        let received_at = snapshot.timestamp;
        events.push(MarketDataEvent::Observation(MarketDataObservation::new(
            snapshot,
            *revision,
            received_at,
        )?));
    }
    Ok(events)
}

/// One continuous, read-only arbitrage evaluator bound to an exact pair.
#[derive(Debug)]
pub struct ReadOnlyArbitrageMonitor {
    book: MarketDataBook,
    left: MarketInstrument,
    right: MarketInstrument,
    threshold_percent: Decimal,
    sequence: u64,
}

impl ReadOnlyArbitrageMonitor {
    /// Binds a monitor to two distinct instruments already present in the book.
    ///
    /// # Errors
    ///
    /// Returns [`ArbitrageMonitorError`] for an invalid threshold, duplicate
    /// legs, or legs outside the book's exact universe.
    pub fn new(
        book: MarketDataBook,
        left: MarketInstrument,
        right: MarketInstrument,
        threshold_percent: Decimal,
    ) -> Result<Self, ArbitrageMonitorError> {
        if threshold_percent < Decimal::ZERO {
            return Err(ArbitrageMonitorError::NegativeThreshold);
        }
        if left == right {
            return Err(ArbitrageMonitorError::DuplicateLeg);
        }
        for instrument in [&left, &right] {
            if !book.universe().contains(instrument) {
                return Err(MarketDataError::InstrumentOutsideUniverse {
                    instrument: instrument.clone(),
                }
                .into());
            }
        }
        Ok(Self {
            book,
            left,
            right,
            threshold_percent,
            sequence: 0,
        })
    }

    pub const fn book(&self) -> &MarketDataBook {
        &self.book
    }

    /// Returns the exact ordered pair bound to this monitor.
    pub const fn legs(&self) -> (&MarketInstrument, &MarketInstrument) {
        (&self.left, &self.right)
    }

    /// Applies one source event and emits exactly one operator-visible monitor
    /// event. Missing, stale, future, duplicate, out-of-order, lagged, and
    /// disconnected inputs become waiting outcomes rather than being hidden.
    ///
    /// # Errors
    ///
    /// Returns [`ArbitrageMonitorError`] only for contract violations that make
    /// it unsafe to continue the bound stream.
    pub fn process(
        &mut self,
        event: MarketDataEvent,
    ) -> Result<ArbitrageMonitorEvent, ArbitrageMonitorError> {
        let market_update = self.book.apply(event)?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(ArbitrageMonitorError::SequenceExhausted)?;
        let view = self.book.view();
        let outcome = match view.current_pair(&self.left, &self.right) {
            Ok(pair) => match SpreadCalculator::best(&pair.left, &pair.right) {
                Ok(spread)
                    if spread.percent > Decimal::ZERO
                        && spread.percent >= self.threshold_percent =>
                {
                    ArbitrageMonitorOutcome::Opportunity {
                        buy_exchange: spread.buy_exchange,
                        sell_exchange: spread.sell_exchange,
                        buy_price: spread.buy_price,
                        sell_price: spread.sell_price,
                        absolute_spread: spread.absolute,
                        spread_percent: spread.percent,
                        threshold_percent: self.threshold_percent,
                    }
                }
                Ok(spread) => ArbitrageMonitorOutcome::NoOpportunity {
                    buy_exchange: spread.buy_exchange,
                    sell_exchange: spread.sell_exchange,
                    buy_price: spread.buy_price,
                    sell_price: spread.sell_price,
                    absolute_spread: spread.absolute,
                    spread_percent: spread.percent,
                    threshold_percent: self.threshold_percent,
                },
                Err(error) => ArbitrageMonitorOutcome::AnalysisRejected {
                    failure: MonitorAnalysisFailure::from(&error),
                },
            },
            Err(MarketDataError::InstrumentNotReady {
                instrument,
                freshness,
                continuity,
            }) => ArbitrageMonitorOutcome::Waiting {
                instrument,
                freshness,
                continuity,
            },
            Err(error) => return Err(error.into()),
        };
        Ok(ArbitrageMonitorEvent {
            schema_version: ARBITRAGE_MONITOR_EVENT_SCHEMA_VERSION,
            sequence: self.sequence,
            recorded_at: view.observed_at,
            market_generation: view.generation,
            market_update,
            left: self.left.clone(),
            right: self.right.clone(),
            outcome,
        })
    }
}

/// Stable, non-secret classification of a pure analysis rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorAnalysisFailure {
    InvalidConfig,
    SnapshotMismatch,
    MissingMarketData,
    InvalidFinancialValue,
}

impl MonitorAnalysisFailure {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::SnapshotMismatch => "snapshot_mismatch",
            Self::MissingMarketData => "missing_market_data",
            Self::InvalidFinancialValue => "invalid_financial_value",
        }
    }
}

impl From<&StrategyError> for MonitorAnalysisFailure {
    fn from(error: &StrategyError) -> Self {
        match error {
            StrategyError::InvalidConfig(_) => Self::InvalidConfig,
            StrategyError::SnapshotMismatch(_) => Self::SnapshotMismatch,
            StrategyError::MissingMarketData(_) => Self::MissingMarketData,
            StrategyError::InvalidFinancialValue(_) => Self::InvalidFinancialValue,
        }
    }
}

/// Outcome projected for an operator; no variant contains order intents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArbitrageMonitorOutcome {
    Waiting {
        instrument: MarketInstrument,
        freshness: MarketDataFreshness,
        continuity: MarketContinuity,
    },
    NoOpportunity {
        buy_exchange: String,
        sell_exchange: String,
        buy_price: Price,
        sell_price: Price,
        absolute_spread: Decimal,
        spread_percent: Decimal,
        threshold_percent: Decimal,
    },
    Opportunity {
        buy_exchange: String,
        sell_exchange: String,
        buy_price: Price,
        sell_price: Price,
        absolute_spread: Decimal,
        spread_percent: Decimal,
        threshold_percent: Decimal,
    },
    AnalysisRejected {
        failure: MonitorAnalysisFailure,
    },
}

impl ArbitrageMonitorOutcome {
    const fn decision_label(&self) -> &'static str {
        match self {
            Self::Waiting { .. } => "monitor_waiting",
            Self::NoOpportunity { .. } => "monitor_no_opportunity",
            Self::Opportunity { .. } => "monitor_opportunity",
            Self::AnalysisRejected { .. } => "monitor_analysis_rejected",
        }
    }

    fn details(&self) -> Value {
        match self {
            Self::Waiting {
                instrument,
                freshness,
                continuity,
            } => json!({
                "type": "waiting",
                "instrument": instrument_details(instrument),
                "freshness": freshness,
                "continuity": continuity,
            }),
            Self::NoOpportunity {
                buy_exchange,
                sell_exchange,
                buy_price,
                sell_price,
                absolute_spread,
                spread_percent,
                threshold_percent,
            } => spread_details(
                "no_opportunity",
                SpreadDetails {
                    buy_exchange,
                    sell_exchange,
                    buy_price: *buy_price,
                    sell_price: *sell_price,
                    absolute_spread: *absolute_spread,
                    spread_percent: *spread_percent,
                    threshold_percent: *threshold_percent,
                },
            ),
            Self::Opportunity {
                buy_exchange,
                sell_exchange,
                buy_price,
                sell_price,
                absolute_spread,
                spread_percent,
                threshold_percent,
            } => spread_details(
                "opportunity",
                SpreadDetails {
                    buy_exchange,
                    sell_exchange,
                    buy_price: *buy_price,
                    sell_price: *sell_price,
                    absolute_spread: *absolute_spread,
                    spread_percent: *spread_percent,
                    threshold_percent: *threshold_percent,
                },
            ),
            Self::AnalysisRejected { failure } => json!({
                "type": "analysis_rejected",
                "failure": failure.as_str(),
            }),
        }
    }
}

/// One event emitted after every accepted source event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbitrageMonitorEvent {
    pub schema_version: u16,
    pub sequence: u64,
    pub recorded_at: DateTime<Utc>,
    pub market_generation: u64,
    pub market_update: MarketDataUpdate,
    pub left: MarketInstrument,
    pub right: MarketInstrument,
    pub outcome: ArbitrageMonitorOutcome,
}

impl ArbitrageMonitorEvent {
    /// Projects the typed event into the bounded append-only legacy journal
    /// contract consumed by the current control plane.
    pub fn to_record(&self) -> DecisionRecord {
        DecisionRecord {
            timestamp: self.recorded_at,
            strategy: "arbitrage_monitor".to_owned(),
            symbol: monitor_symbol(&self.left, &self.right),
            decision: self.outcome.decision_label().to_owned(),
            details: json!({
                "schema_version": self.schema_version,
                "sequence": self.sequence,
                "market_generation": self.market_generation,
                "market_update": market_update_label(self.market_update),
                "left": instrument_details(&self.left),
                "right": instrument_details(&self.right),
                "outcome": self.outcome.details(),
            }),
        }
    }
}

#[derive(Clone, Copy)]
struct SpreadDetails<'a> {
    buy_exchange: &'a str,
    sell_exchange: &'a str,
    buy_price: Price,
    sell_price: Price,
    absolute_spread: Decimal,
    spread_percent: Decimal,
    threshold_percent: Decimal,
}

fn spread_details(outcome_type: &str, spread: SpreadDetails<'_>) -> Value {
    json!({
        "type": outcome_type,
        "buy_exchange": spread.buy_exchange,
        "sell_exchange": spread.sell_exchange,
        "buy_price": spread.buy_price.as_decimal().to_string(),
        "sell_price": spread.sell_price.as_decimal().to_string(),
        "absolute_spread": spread.absolute_spread.to_string(),
        "spread_percent": spread.spread_percent.to_string(),
        "threshold_percent": spread.threshold_percent.to_string(),
    })
}

fn instrument_details(instrument: &MarketInstrument) -> Value {
    json!({
        "exchange": instrument.exchange(),
        "symbol": instrument.symbol.as_str(),
        "market_type": format!("{:?}", instrument.market_type).to_ascii_lowercase(),
    })
}

pub(crate) fn monitor_symbol(left: &MarketInstrument, right: &MarketInstrument) -> String {
    if left.symbol == right.symbol {
        left.symbol.to_string()
    } else {
        "cross-symbol-pair".to_owned()
    }
}

const fn market_update_label(update: MarketDataUpdate) -> &'static str {
    match update {
        MarketDataUpdate::Accepted => "accepted",
        MarketDataUpdate::AcceptedWithGap => "accepted_with_gap",
        MarketDataUpdate::IgnoredDuplicate => "ignored_duplicate",
        MarketDataUpdate::IgnoredOutOfOrder => "ignored_out_of_order",
        MarketDataUpdate::IgnoredDuplicateTimestamp => "ignored_duplicate_timestamp",
        MarketDataUpdate::IgnoredOutOfOrderTimestamp => "ignored_out_of_order_timestamp",
        MarketDataUpdate::IgnoredOutOfOrderReceipt => "ignored_out_of_order_receipt",
        MarketDataUpdate::SourceDegraded => "source_degraded",
    }
}

/// Fatal contract failures for one bound monitor stream.
#[derive(Debug)]
pub enum ArbitrageMonitorError {
    NegativeThreshold,
    DuplicateLeg,
    SequenceExhausted,
    Market(Box<MarketDataError>),
}

impl From<MarketDataError> for ArbitrageMonitorError {
    fn from(error: MarketDataError) -> Self {
        Self::Market(Box::new(error))
    }
}

impl std::fmt::Display for ArbitrageMonitorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeThreshold => {
                formatter.write_str("arbitrage monitor spread threshold must not be negative")
            }
            Self::DuplicateLeg => {
                formatter.write_str("arbitrage monitor needs two distinct exact instruments")
            }
            Self::SequenceExhausted => {
                formatter.write_str("arbitrage monitor sequence is exhausted")
            }
            Self::Market(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for ArbitrageMonitorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Market(source) => Some(source.as_ref()),
            Self::NegativeThreshold | Self::DuplicateLeg | Self::SequenceExhausted => None,
        }
    }
}
