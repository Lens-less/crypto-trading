//! Durable spread-history journal for the arbitrage history decision mode.
//!
//! One dedicated JSONL domain (default `var/history/spread-history.jsonl`)
//! persists sampled cross-exchange spreads through the same append-only
//! [`JsonlHistory`] machinery as every other journal: synced appends, one
//! writer lease per chain, and sealed-segment rotation bounded by
//! [`crate::MAX_HISTORY_SEALED_SEGMENTS`] and [`crate::MAX_HISTORY_CHAIN_BYTES`].
//! Sealed segments are the archive tier; no secondary database exists.
//!
//! The record schema (`spread_history_record_v1`) mirrors the field list of
//! the frozen Python recorder
//! (`archive/python-legacy/.../history/spread_history_recorder.py:536`):
//! timestamp, symbol, buy/sell exchange, both quotes, the spread, and the
//! optional funding-rate fields. Funding fields stay `None` whenever the
//! market-data source publishes no funding rates.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use thiserror::Error;

use crate::history::{DecisionRecord, HistoryError, JsonlHistory};

/// Stable version of one persisted spread-history record.
pub const SPREAD_HISTORY_RECORD_SCHEMA_VERSION: u16 = 1;
/// Journal strategy label for spread-history records.
pub const SPREAD_HISTORY_STRATEGY: &str = "spread_history";
/// Versioned decision label of the v1 record shape.
pub const SPREAD_HISTORY_RECORD_V1_DECISION: &str = "spread_history_record_v1";
/// Maximum records accepted by one bounded batch append.
pub const MAX_SPREAD_HISTORY_BATCH_RECORDS: usize = 256;
/// Default location of the dedicated spread-history journal chain.
pub const DEFAULT_SPREAD_HISTORY_FILE: &str = "var/history/spread-history.jsonl";

const MAX_SPREAD_HISTORY_TEXT_BYTES: usize = 128;
const MAX_SPREAD_HISTORY_DECIMAL_BYTES: usize = 64;

/// One sampled spread observation with optional funding-rate context.
///
/// Numeric fields are validated decimal text so this crate stays off any
/// decimal arithmetic dependency; strategy consumers parse them on read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpreadHistoryRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub exchange_buy: String,
    pub exchange_sell: String,
    pub price_buy: String,
    pub price_sell: String,
    /// Spread of this direction in basis points.
    pub spread_bps: String,
    /// 8-hour funding rate of the buy (long) leg, decimal fraction.
    pub funding_rate_buy: Option<String>,
    /// 8-hour funding rate of the sell (short) leg, decimal fraction.
    pub funding_rate_sell: Option<String>,
    /// Signed 8-hour funding-rate difference earned by the short leg.
    pub funding_rate_diff: Option<String>,
    /// Annualized funding-rate difference in percent per year.
    pub funding_rate_diff_annual_pct: Option<String>,
}

impl SpreadHistoryRecord {
    fn validate(&self) -> Result<(), SpreadHistoryError> {
        for (field, value) in [
            ("symbol", &self.symbol),
            ("exchange_buy", &self.exchange_buy),
            ("exchange_sell", &self.exchange_sell),
        ] {
            if value.trim().is_empty() || value.len() > MAX_SPREAD_HISTORY_TEXT_BYTES {
                return Err(SpreadHistoryError::InvalidRecord { field });
            }
        }
        if self.exchange_buy == self.exchange_sell {
            return Err(SpreadHistoryError::InvalidRecord {
                field: "exchange_sell",
            });
        }
        for (field, value) in [
            ("price_buy", &self.price_buy),
            ("price_sell", &self.price_sell),
            ("spread_bps", &self.spread_bps),
        ] {
            if !valid_spread_decimal_text(value) {
                return Err(SpreadHistoryError::InvalidRecord { field });
            }
        }
        for (field, value) in [
            ("funding_rate_buy", &self.funding_rate_buy),
            ("funding_rate_sell", &self.funding_rate_sell),
            ("funding_rate_diff", &self.funding_rate_diff),
            (
                "funding_rate_diff_annual_pct",
                &self.funding_rate_diff_annual_pct,
            ),
        ] {
            if value
                .as_ref()
                .is_some_and(|text| !valid_spread_decimal_text(text))
            {
                return Err(SpreadHistoryError::InvalidRecord { field });
            }
        }
        Ok(())
    }

    fn to_decision_record(&self) -> DecisionRecord {
        DecisionRecord {
            timestamp: self.timestamp,
            strategy: SPREAD_HISTORY_STRATEGY.to_owned(),
            symbol: self.symbol.clone(),
            decision: SPREAD_HISTORY_RECORD_V1_DECISION.to_owned(),
            details: json!({
                "schema_version": SPREAD_HISTORY_RECORD_SCHEMA_VERSION,
                "exchange_buy": self.exchange_buy,
                "exchange_sell": self.exchange_sell,
                "price_buy": self.price_buy,
                "price_sell": self.price_sell,
                "spread_bps": self.spread_bps,
                "funding_rate_buy": optional_text(self.funding_rate_buy.as_deref()),
                "funding_rate_sell": optional_text(self.funding_rate_sell.as_deref()),
                "funding_rate_diff": optional_text(self.funding_rate_diff.as_deref()),
                "funding_rate_diff_annual_pct":
                    optional_text(self.funding_rate_diff_annual_pct.as_deref()),
            }),
        }
    }
}

fn optional_text(value: Option<&str>) -> Value {
    value.map_or(Value::Null, |text| Value::String(text.to_owned()))
}

/// Strict decimal text: optional leading minus, digits, at most one point.
pub(crate) fn valid_spread_decimal_text(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_SPREAD_HISTORY_DECIMAL_BYTES {
        return false;
    }
    let mut digits = 0usize;
    let mut decimal_points = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        if byte.is_ascii_digit() {
            digits = digits.saturating_add(1);
        } else if byte == b'.' {
            decimal_points = decimal_points.saturating_add(1);
        } else if byte != b'-' || index != 0 {
            return false;
        }
    }
    digits > 0 && decimal_points <= 1
}

/// Append-only writer for one dedicated spread-history journal chain.
///
/// Rotation, chain budgets, sync-on-append, and cross-process writer
/// exclusion are inherited from [`JsonlHistory`]; sealed segments serve as
/// the archive tier and are never rewritten.
#[derive(Clone, Debug)]
pub struct SpreadHistoryWriter {
    history: JsonlHistory,
}

impl SpreadHistoryWriter {
    #[must_use]
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            history: JsonlHistory::new(path.into()),
        }
    }

    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        self.history.path()
    }

    /// Validates and durably appends one record.
    ///
    /// # Errors
    ///
    /// Returns [`SpreadHistoryError`] for an invalid record or any journal
    /// failure; nothing is written on failure.
    pub async fn append(&self, record: &SpreadHistoryRecord) -> Result<(), SpreadHistoryError> {
        self.append_batch(std::slice::from_ref(record)).await
    }

    /// Validates and durably appends one bounded group of records.
    ///
    /// # Errors
    ///
    /// Returns [`SpreadHistoryError`] when the batch exceeds
    /// [`MAX_SPREAD_HISTORY_BATCH_RECORDS`], any record is invalid, or the
    /// journal append fails; nothing is written on failure.
    pub async fn append_batch(
        &self,
        records: &[SpreadHistoryRecord],
    ) -> Result<(), SpreadHistoryError> {
        if records.is_empty() {
            return Ok(());
        }
        if records.len() > MAX_SPREAD_HISTORY_BATCH_RECORDS {
            return Err(SpreadHistoryError::TooManyRecords {
                count: records.len(),
                limit: MAX_SPREAD_HISTORY_BATCH_RECORDS,
            });
        }
        let mut decisions = Vec::new();
        decisions
            .try_reserve_exact(records.len())
            .map_err(|_| SpreadHistoryError::InvalidRecord { field: "batch" })?;
        for record in records {
            record.validate()?;
            decisions.push(record.to_decision_record());
        }
        self.history
            .append_batch(&decisions)
            .await
            .map_err(SpreadHistoryError::Journal)
    }
}

/// Typed failures of the spread-history write side.
#[derive(Debug, Error)]
pub enum SpreadHistoryError {
    #[error("spread history batch has {count} records; maximum is {limit}")]
    TooManyRecords { count: usize, limit: usize },
    #[error("spread history record field {field} is invalid")]
    InvalidRecord { field: &'static str },
    #[error("spread history journal failed: {0}")]
    Journal(#[from] HistoryError),
}
