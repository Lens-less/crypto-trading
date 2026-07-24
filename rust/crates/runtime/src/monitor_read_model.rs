use chrono::{DateTime, Utc};
use crypto_trading_domain::MarketType;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    JournalPageBoundary, JournalSnapshot, LegacyJsonlJournalReader, OperationEventEnvelope,
    ProjectionStatus, ReadModelError,
};

/// Stable schema version for the bounded arbitrage-monitor projection.
pub const ARBITRAGE_MONITOR_READ_MODEL_SCHEMA_VERSION: u16 = 1;

const MONITOR_STRATEGY: &str = "arbitrage_monitor";
const MAX_MONITOR_TEXT_BYTES: usize = 128;
const MAX_DECIMAL_TEXT_BYTES: usize = 64;

/// Latest bounded projection of read-only arbitrage monitor events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArbitrageMonitorReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub journal_head_sequence: Option<u64>,
    pub projection_status: ProjectionStatus,
    pub latest: Option<ArbitrageMonitorView>,
    pub invalid_event_count: u64,
}

impl ArbitrageMonitorReadModel {
    /// Projects monitor facts from one immutable legacy journal snapshot.
    ///
    /// Non-monitor records are ignored. Malformed monitor records degrade the
    /// projection and preserve the last valid monitor fact. Malformed physical
    /// JSONL records remain hard journal errors.
    ///
    /// # Errors
    ///
    /// Returns [`ReadModelError`] for journal failures or a non-advancing page.
    pub fn from_legacy_snapshot(snapshot: &JournalSnapshot) -> Result<Self, ReadModelError> {
        MonitorProjectionBuilder::new(snapshot.journal_id()).project(snapshot)
    }
}

/// Stable high-level state of the last valid monitor event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorProjectionState {
    Waiting,
    NoOpportunity,
    Opportunity,
    AnalysisRejected,
}

/// Safe exact leg identity copied from a validated monitor event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorLegView {
    pub exchange: String,
    pub symbol: String,
    pub market_type: MarketType,
}

/// Bounded freshness classification retained by the monitor projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorFreshnessState {
    Missing,
    Fresh,
    Stale,
    Future,
}

/// Bounded continuity classification retained by the monitor projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorContinuityState {
    Missing,
    Continuous,
    Gap,
    Duplicate,
    OutOfOrder,
    DuplicateTimestamp,
    OutOfOrderTimestamp,
    OutOfOrderReceipt,
    SourceGap,
    Unavailable,
}

/// Operator-safe outcome; arbitrary journal details and order-shaped fields are
/// deliberately not retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArbitrageMonitorProjection {
    Waiting {
        instrument: MonitorLegView,
        freshness: MonitorFreshnessState,
        continuity: MonitorContinuityState,
    },
    NoOpportunity {
        buy_exchange: String,
        sell_exchange: String,
        buy_price: String,
        sell_price: String,
        absolute_spread: String,
        spread_percent: String,
        threshold_percent: String,
    },
    Opportunity {
        buy_exchange: String,
        sell_exchange: String,
        buy_price: String,
        sell_price: String,
        absolute_spread: String,
        spread_percent: String,
        threshold_percent: String,
    },
    AnalysisRejected {
        failure: String,
    },
}

/// Last valid monitor fact with its durable journal identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArbitrageMonitorView {
    pub source_sequence: u64,
    pub event_id: Uuid,
    pub recorded_at: DateTime<Utc>,
    pub monitor_sequence: u64,
    pub market_generation: u64,
    pub symbol: String,
    pub state: MonitorProjectionState,
    pub left: MonitorLegView,
    pub right: MonitorLegView,
    pub projection: ArbitrageMonitorProjection,
}

struct MonitorProjectionBuilder {
    journal_id: Uuid,
    journal_head_sequence: Option<u64>,
    projection_status: ProjectionStatus,
    latest: Option<ArbitrageMonitorView>,
    invalid_event_count: u64,
}

impl MonitorProjectionBuilder {
    const fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            journal_head_sequence: None,
            projection_status: ProjectionStatus::Complete,
            latest: None,
            invalid_event_count: 0,
        }
    }

    fn project(
        mut self,
        snapshot: &JournalSnapshot,
    ) -> Result<ArbitrageMonitorReadModel, ReadModelError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            if let Some(event) = page.events().last() {
                self.journal_head_sequence = Some(event.sequence());
            }
            for event in page.events() {
                self.apply_event(event);
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    self.projection_status = ProjectionStatus::Degraded;
                    break;
                }
                JournalPageBoundary::PageLimit => {
                    let next = page
                        .next_cursor()
                        .cloned()
                        .ok_or(ReadModelError::NonAdvancingPage)?;
                    if cursor.as_ref().is_some_and(|previous| {
                        previous.next_offset() == next.next_offset()
                            && previous.after_sequence() == next.after_sequence()
                    }) {
                        return Err(ReadModelError::NonAdvancingPage);
                    }
                    cursor = Some(next);
                }
            }
        }
        Ok(ArbitrageMonitorReadModel {
            schema_version: ARBITRAGE_MONITOR_READ_MODEL_SCHEMA_VERSION,
            journal_id: self.journal_id,
            journal_head_sequence: self.journal_head_sequence,
            projection_status: self.projection_status,
            latest: self.latest,
            invalid_event_count: self.invalid_event_count,
        })
    }

    fn apply_event(&mut self, event: &OperationEventEnvelope) {
        match parse_monitor_event(event) {
            Ok(Some(view)) => self.latest = Some(view),
            Ok(None) => {}
            Err(()) => {
                self.projection_status = ProjectionStatus::Degraded;
                self.invalid_event_count = self.invalid_event_count.saturating_add(1);
            }
        }
    }
}

fn parse_monitor_event(event: &OperationEventEnvelope) -> Result<Option<ArbitrageMonitorView>, ()> {
    let payload = object(event.payload())?;
    let strategy = required_text(payload, "strategy")?;
    if strategy != MONITOR_STRATEGY {
        return Ok(None);
    }
    let decision = required_text(payload, "decision")?;
    let symbol = required_text(payload, "symbol")?;
    let details = object(required(payload, "details")?)?;
    if required_u64(details, "schema_version")?
        != u64::from(ARBITRAGE_MONITOR_READ_MODEL_SCHEMA_VERSION)
    {
        return Err(());
    }
    let monitor_sequence = required_u64(details, "sequence")?;
    if monitor_sequence == 0 {
        return Err(());
    }
    let market_generation = required_u64(details, "market_generation")?;
    if market_generation == 0 {
        return Err(());
    }
    if !matches!(
        required_text(details, "market_update")?.as_str(),
        "accepted"
            | "accepted_with_gap"
            | "ignored_duplicate"
            | "ignored_out_of_order"
            | "ignored_duplicate_timestamp"
            | "ignored_out_of_order_timestamp"
            | "ignored_out_of_order_receipt"
            | "source_degraded"
    ) {
        return Err(());
    }
    let left = parse_leg(required(details, "left")?)?;
    let right = parse_leg(required(details, "right")?)?;
    if left == right {
        return Err(());
    }
    let expected_symbol = if left.symbol == right.symbol {
        left.symbol.as_str()
    } else {
        "cross-symbol-pair"
    };
    if symbol != expected_symbol {
        return Err(());
    }
    let outcome = object(required(details, "outcome")?)?;
    let (state, projection) = parse_projection(&decision, outcome)?;
    Ok(Some(ArbitrageMonitorView {
        source_sequence: event.sequence(),
        event_id: event.event_id(),
        recorded_at: event.recorded_at(),
        monitor_sequence,
        market_generation,
        symbol,
        state,
        left,
        right,
        projection,
    }))
}

fn parse_projection(
    decision: &str,
    outcome: &Map<String, Value>,
) -> Result<(MonitorProjectionState, ArbitrageMonitorProjection), ()> {
    let outcome_type = required_text(outcome, "type")?;
    match (decision, outcome_type.as_str()) {
        ("monitor_waiting", "waiting") => Ok((
            MonitorProjectionState::Waiting,
            ArbitrageMonitorProjection::Waiting {
                instrument: parse_leg(required(outcome, "instrument")?)?,
                freshness: parse_freshness(required(outcome, "freshness")?)?,
                continuity: parse_continuity(required(outcome, "continuity")?)?,
            },
        )),
        ("monitor_no_opportunity", "no_opportunity") => Ok((
            MonitorProjectionState::NoOpportunity,
            parse_spread_projection(outcome, false)?,
        )),
        ("monitor_opportunity", "opportunity") => Ok((
            MonitorProjectionState::Opportunity,
            parse_spread_projection(outcome, true)?,
        )),
        ("monitor_analysis_rejected", "analysis_rejected") => {
            let failure = required_text(outcome, "failure")?;
            if !matches!(
                failure.as_str(),
                "invalid_config"
                    | "snapshot_mismatch"
                    | "missing_market_data"
                    | "invalid_financial_value"
            ) {
                return Err(());
            }
            Ok((
                MonitorProjectionState::AnalysisRejected,
                ArbitrageMonitorProjection::AnalysisRejected { failure },
            ))
        }
        _ => Err(()),
    }
}

fn parse_spread_projection(
    outcome: &Map<String, Value>,
    opportunity: bool,
) -> Result<ArbitrageMonitorProjection, ()> {
    let buy_exchange = required_text(outcome, "buy_exchange")?;
    let sell_exchange = required_text(outcome, "sell_exchange")?;
    let buy_price = required_decimal_text(outcome, "buy_price")?;
    let sell_price = required_decimal_text(outcome, "sell_price")?;
    let absolute_spread = required_decimal_text(outcome, "absolute_spread")?;
    let spread_percent = required_decimal_text(outcome, "spread_percent")?;
    let threshold_percent = required_decimal_text(outcome, "threshold_percent")?;
    if opportunity {
        Ok(ArbitrageMonitorProjection::Opportunity {
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            absolute_spread,
            spread_percent,
            threshold_percent,
        })
    } else {
        Ok(ArbitrageMonitorProjection::NoOpportunity {
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            absolute_spread,
            spread_percent,
            threshold_percent,
        })
    }
}

fn parse_leg(value: &Value) -> Result<MonitorLegView, ()> {
    let leg = object(value)?;
    let market_type = match required_text(leg, "market_type")?.as_str() {
        "spot" => MarketType::Spot,
        "perpetual" => MarketType::Perpetual,
        _ => return Err(()),
    };
    Ok(MonitorLegView {
        exchange: required_text(leg, "exchange")?,
        symbol: required_text(leg, "symbol")?,
        market_type,
    })
}

fn parse_freshness(value: &Value) -> Result<MonitorFreshnessState, ()> {
    match required_text(object(value)?, "status")?.as_str() {
        "missing" => Ok(MonitorFreshnessState::Missing),
        "fresh" => Ok(MonitorFreshnessState::Fresh),
        "stale" => Ok(MonitorFreshnessState::Stale),
        "future" => Ok(MonitorFreshnessState::Future),
        _ => Err(()),
    }
}

fn parse_continuity(value: &Value) -> Result<MonitorContinuityState, ()> {
    match required_text(object(value)?, "status")?.as_str() {
        "missing" => Ok(MonitorContinuityState::Missing),
        "continuous" => Ok(MonitorContinuityState::Continuous),
        "gap" => Ok(MonitorContinuityState::Gap),
        "duplicate" => Ok(MonitorContinuityState::Duplicate),
        "out_of_order" => Ok(MonitorContinuityState::OutOfOrder),
        "duplicate_timestamp" => Ok(MonitorContinuityState::DuplicateTimestamp),
        "out_of_order_timestamp" => Ok(MonitorContinuityState::OutOfOrderTimestamp),
        "out_of_order_receipt" => Ok(MonitorContinuityState::OutOfOrderReceipt),
        "source_gap" => Ok(MonitorContinuityState::SourceGap),
        "unavailable" => Ok(MonitorContinuityState::Unavailable),
        _ => Err(()),
    }
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ()> {
    object.get(key).ok_or(())
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ()> {
    required(object, key)?.as_u64().ok_or(())
}

fn required_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?.trim();
    if value.is_empty() || value.len() > MAX_MONITOR_TEXT_BYTES {
        return Err(());
    }
    Ok(value.to_owned())
}

fn required_decimal_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?;
    if value.is_empty()
        || value.len() > MAX_DECIMAL_TEXT_BYTES
        || !valid_decimal_text(value.as_bytes())
    {
        return Err(());
    }
    Ok(value.to_owned())
}

fn valid_decimal_text(value: &[u8]) -> bool {
    let mut digits = 0usize;
    let mut decimal_points = 0usize;
    for (index, byte) in value.iter().enumerate() {
        if byte.is_ascii_digit() {
            digits = digits.saturating_add(1);
        } else if *byte == b'.' {
            decimal_points = decimal_points.saturating_add(1);
        } else if *byte != b'-' || index != 0 {
            return false;
        }
    }
    digits > 0 && decimal_points <= 1
}

fn object(value: &Value) -> Result<&Map<String, Value>, ()> {
    value.as_object().ok_or(())
}
