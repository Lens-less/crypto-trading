//! Bounded projection of the dedicated spread-history journal chain.
//!
//! The read model replays one immutable [`JournalSnapshot`] captured from the
//! spread-history chain (sealed segments plus the active file, in order) and
//! retains the most recent bounded set of valid `spread_history_record_v1`
//! samples. Its failure semantics match the other legacy-journal read models:
//! physically malformed JSONL is a hard journal error, while a structurally
//! valid record with an invalid spread payload degrades the projection and is
//! counted instead of being silently repaired.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::spread_history::{
    SPREAD_HISTORY_RECORD_SCHEMA_VERSION, SPREAD_HISTORY_RECORD_V1_DECISION,
    SPREAD_HISTORY_STRATEGY, SpreadHistoryRecord, valid_spread_decimal_text,
};
use crate::{
    JournalPageBoundary, JournalSnapshot, LegacyJsonlJournalReader, OperationEventEnvelope,
    ProjectionStatus, ReadModelError,
};

/// Stable schema version for the bounded spread-history projection.
pub const SPREAD_HISTORY_READ_MODEL_SCHEMA_VERSION: u16 = 1;
/// Maximum retained samples; older valid samples are dropped by design so the
/// projection is always the bounded most-recent window of the chain.
pub const MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES: usize = 4_096;

const MAX_TEXT_BYTES: usize = 128;

/// One retained sample plus its durable journal identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpreadHistorySampleView {
    pub source_sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub exchange_buy: String,
    pub exchange_sell: String,
    pub price_buy: String,
    pub price_sell: String,
    pub spread_bps: String,
    pub funding_rate_buy: Option<String>,
    pub funding_rate_sell: Option<String>,
    pub funding_rate_diff: Option<String>,
    pub funding_rate_diff_annual_pct: Option<String>,
}

/// Latest bounded projection of spread-history samples.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpreadHistoryReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub projection_status: ProjectionStatus,
    /// Most recent valid samples in journal order, bounded by
    /// [`MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES`].
    pub samples: Vec<SpreadHistorySampleView>,
    pub invalid_record_count: u64,
}

impl SpreadHistoryReadModel {
    /// Projects spread-history samples from one immutable journal snapshot.
    ///
    /// Records of other strategies are ignored. Malformed spread records
    /// degrade the projection and are counted; malformed physical JSONL
    /// records remain hard journal errors.
    ///
    /// # Errors
    ///
    /// Returns [`ReadModelError`] for journal failures or a non-advancing page.
    pub fn from_legacy_snapshot(snapshot: &JournalSnapshot) -> Result<Self, ReadModelError> {
        let mut projection_status = ProjectionStatus::Complete;
        let mut samples: std::collections::VecDeque<SpreadHistorySampleView> =
            std::collections::VecDeque::new();
        let mut invalid_record_count = 0u64;
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            for event in page.events() {
                match parse_spread_history_event(event) {
                    Ok(Some(view)) => {
                        samples.push_back(view);
                        if samples.len() > MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES {
                            samples.pop_front();
                        }
                    }
                    Ok(None) => {}
                    Err(()) => {
                        projection_status = ProjectionStatus::Degraded;
                        invalid_record_count = invalid_record_count.saturating_add(1);
                    }
                }
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    projection_status = ProjectionStatus::Degraded;
                    break;
                }
                JournalPageBoundary::PageLimit => {
                    let next = page
                        .next_cursor()
                        .cloned()
                        .ok_or(ReadModelError::NonAdvancingPage)?;
                    if cursor
                        .as_ref()
                        .is_some_and(|previous: &crate::JournalCursor| {
                            previous.next_offset() == next.next_offset()
                                && previous.after_sequence() == next.after_sequence()
                        })
                    {
                        return Err(ReadModelError::NonAdvancingPage);
                    }
                    cursor = Some(next);
                }
            }
        }
        Ok(Self {
            schema_version: SPREAD_HISTORY_READ_MODEL_SCHEMA_VERSION,
            journal_id: snapshot.journal_id(),
            projection_status,
            samples: samples.into_iter().collect(),
            invalid_record_count,
        })
    }

    /// Returns the retained samples inside `[end - window, end]`, preserving
    /// journal order. The bounds are inclusive to match the strategy
    /// machine's window filter. The result is bounded by the projection.
    #[must_use]
    pub fn recent_window(
        &self,
        end: DateTime<Utc>,
        window: chrono::Duration,
    ) -> Vec<&SpreadHistorySampleView> {
        let start = end - window;
        self.samples
            .iter()
            .filter(|sample| sample.timestamp >= start && sample.timestamp <= end)
            .collect()
    }

    /// Converts a retained sample view back into a writable record shape.
    #[must_use]
    pub fn record_of(sample: &SpreadHistorySampleView) -> SpreadHistoryRecord {
        SpreadHistoryRecord {
            timestamp: sample.timestamp,
            symbol: sample.symbol.clone(),
            exchange_buy: sample.exchange_buy.clone(),
            exchange_sell: sample.exchange_sell.clone(),
            price_buy: sample.price_buy.clone(),
            price_sell: sample.price_sell.clone(),
            spread_bps: sample.spread_bps.clone(),
            funding_rate_buy: sample.funding_rate_buy.clone(),
            funding_rate_sell: sample.funding_rate_sell.clone(),
            funding_rate_diff: sample.funding_rate_diff.clone(),
            funding_rate_diff_annual_pct: sample.funding_rate_diff_annual_pct.clone(),
        }
    }
}

fn parse_spread_history_event(
    event: &OperationEventEnvelope,
) -> Result<Option<SpreadHistorySampleView>, ()> {
    let payload = object(event.payload())?;
    let strategy = required_text(payload, "strategy")?;
    if strategy != SPREAD_HISTORY_STRATEGY {
        return Ok(None);
    }
    let decision = required_text(payload, "decision")?;
    if decision != SPREAD_HISTORY_RECORD_V1_DECISION {
        return Err(());
    }
    let symbol = required_text(payload, "symbol")?;
    let details = object(required(payload, "details")?)?;
    if required(details, "schema_version")?.as_u64()
        != Some(u64::from(SPREAD_HISTORY_RECORD_SCHEMA_VERSION))
    {
        return Err(());
    }
    let exchange_buy = required_text(details, "exchange_buy")?;
    let exchange_sell = required_text(details, "exchange_sell")?;
    if exchange_buy == exchange_sell {
        return Err(());
    }
    Ok(Some(SpreadHistorySampleView {
        source_sequence: event.sequence(),
        timestamp: event.recorded_at(),
        symbol,
        exchange_buy,
        exchange_sell,
        price_buy: required_decimal_text(details, "price_buy")?,
        price_sell: required_decimal_text(details, "price_sell")?,
        spread_bps: required_decimal_text(details, "spread_bps")?,
        funding_rate_buy: optional_decimal_text(details, "funding_rate_buy")?,
        funding_rate_sell: optional_decimal_text(details, "funding_rate_sell")?,
        funding_rate_diff: optional_decimal_text(details, "funding_rate_diff")?,
        funding_rate_diff_annual_pct: optional_decimal_text(
            details,
            "funding_rate_diff_annual_pct",
        )?,
    }))
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ()> {
    object.get(key).ok_or(())
}

fn required_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?.trim();
    if value.is_empty() || value.len() > MAX_TEXT_BYTES {
        return Err(());
    }
    Ok(value.to_owned())
}

fn required_decimal_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?;
    if !valid_spread_decimal_text(value) {
        return Err(());
    }
    Ok(value.to_owned())
}

fn optional_decimal_text(object: &Map<String, Value>, key: &str) -> Result<Option<String>, ()> {
    match required(object, key)? {
        Value::Null => Ok(None),
        Value::String(value) if valid_spread_decimal_text(value) => Ok(Some(value.clone())),
        _ => Err(()),
    }
}

fn object(value: &Value) -> Result<&Map<String, Value>, ()> {
    value.as_object().ok_or(())
}
