use std::str::FromStr;

use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Price, Symbol};
use crypto_trading_runtime::{DecisionRecord, MarketInstrument, OperationEventEnvelope};
use crypto_trading_strategy::AlertKind;
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use super::notification::{AlertNotification, AlertOccurrenceId, validate_adapter_id};

pub(crate) const PRICE_ALERT_STRATEGY: &str = "price_alert";
pub(crate) const PRICE_ALERT_SAMPLED: &str = "price_alert_sampled";
pub(crate) const PRICE_ALERT_OCCURRED: &str = "price_alert_occurred";
pub(crate) const PRICE_ALERT_DELIVERY_PENDING: &str = "price_alert_delivery_pending";
pub(crate) const PRICE_ALERT_DELIVERY_DROPPED: &str = "price_alert_delivery_dropped";
pub(crate) const PRICE_ALERT_DELIVERY_SUCCEEDED: &str = "price_alert_delivery_succeeded";
pub(crate) const PRICE_ALERT_DELIVERY_FAILED: &str = "price_alert_delivery_failed";
pub(crate) const PRICE_ALERT_DELIVERY_TIMED_OUT: &str = "price_alert_delivery_timed_out";
pub(crate) const PRICE_ALERT_ACKNOWLEDGED: &str = "price_alert_acknowledged";

pub const PRICE_ALERT_EVENT_SCHEMA_VERSION: u16 = 1;

const MAX_ALERT_TEXT_BYTES: usize = 128;
const MAX_DECIMAL_TEXT_BYTES: usize = 64;

pub(crate) fn sample_record(
    instrument: &MarketInstrument,
    recorded_at: DateTime<Utc>,
    revision: u64,
    market_generation: u64,
    price: Price,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: recorded_at,
        strategy: PRICE_ALERT_STRATEGY.to_owned(),
        symbol: instrument.symbol.to_string(),
        decision: PRICE_ALERT_SAMPLED.to_owned(),
        details: json!({
            "schema_version": PRICE_ALERT_EVENT_SCHEMA_VERSION,
            "exchange": instrument.exchange(),
            "market_type": market_type_label(instrument.market_type),
            "revision": revision,
            "market_generation": market_generation,
            "price": price.as_decimal().to_string(),
        }),
    }
}

pub(crate) fn occurrence_record(
    notification: &AlertNotification,
    market_revision: u64,
    market_generation: u64,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: notification.occurred_at,
        strategy: PRICE_ALERT_STRATEGY.to_owned(),
        symbol: notification.occurrence_id.instrument.symbol.to_string(),
        decision: PRICE_ALERT_OCCURRED.to_owned(),
        details: json!({
            "schema_version": PRICE_ALERT_EVENT_SCHEMA_VERSION,
            "sequence": notification.occurrence_id.sequence,
            "exchange": notification.occurrence_id.instrument.exchange(),
            "market_type": market_type_label(notification.occurrence_id.instrument.market_type),
            "kind": alert_kind_label(notification.kind),
            "price": notification.price.as_decimal().to_string(),
            "change_percent": notification.change_percent.map(|value| value.to_string()),
            "market_revision": market_revision,
            "market_generation": market_generation,
        }),
    }
}

pub(crate) fn pending_record(notification: &AlertNotification, adapter_id: &str) -> DecisionRecord {
    delivery_record(
        notification,
        adapter_id,
        PRICE_ALERT_DELIVERY_PENDING,
        None,
        notification.occurred_at,
    )
}

pub(crate) fn dropped_record(
    notification: &AlertNotification,
    adapter_id: &str,
    failure: &'static str,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    delivery_record(
        notification,
        adapter_id,
        PRICE_ALERT_DELIVERY_DROPPED,
        Some(failure),
        recorded_at,
    )
}

pub(crate) fn delivery_record(
    notification: &AlertNotification,
    adapter_id: &str,
    decision: &str,
    failure: Option<&str>,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: recorded_at,
        strategy: PRICE_ALERT_STRATEGY.to_owned(),
        symbol: notification.occurrence_id.instrument.symbol.to_string(),
        decision: decision.to_owned(),
        details: json!({
            "schema_version": PRICE_ALERT_EVENT_SCHEMA_VERSION,
            "sequence": notification.occurrence_id.sequence,
            "exchange": notification.occurrence_id.instrument.exchange(),
            "market_type": market_type_label(notification.occurrence_id.instrument.market_type),
            "adapter_id": adapter_id,
            "failure": failure,
        }),
    }
}

pub(crate) fn acknowledgement_record(
    occurrence_id: &AlertOccurrenceId,
    acknowledged_at: DateTime<Utc>,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: acknowledged_at,
        strategy: PRICE_ALERT_STRATEGY.to_owned(),
        symbol: occurrence_id.instrument.symbol.to_string(),
        decision: PRICE_ALERT_ACKNOWLEDGED.to_owned(),
        details: json!({
            "schema_version": PRICE_ALERT_EVENT_SCHEMA_VERSION,
            "sequence": occurrence_id.sequence,
            "exchange": occurrence_id.instrument.exchange(),
            "market_type": market_type_label(occurrence_id.instrument.market_type),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveredAlertFact {
    Sample {
        instrument: MarketInstrument,
        recorded_at: DateTime<Utc>,
        price: Price,
    },
    Occurrence {
        notification: AlertNotification,
    },
    Acknowledged {
        occurrence_id: AlertOccurrenceId,
        acknowledged_at: DateTime<Utc>,
    },
    Delivery {
        occurrence_id: AlertOccurrenceId,
    },
}

pub(crate) fn parse_alert_fact(
    event: &OperationEventEnvelope,
) -> Result<Option<RecoveredAlertFact>, ()> {
    let payload = object(event.payload())?;
    let strategy = required_text(payload, "strategy")?;
    if strategy != PRICE_ALERT_STRATEGY {
        return Ok(None);
    }
    require_exact_keys(payload, &["strategy", "symbol", "decision", "details"])?;
    let symbol = required_text(payload, "symbol")?;
    let decision = required_text(payload, "decision")?;
    let details = object(required(payload, "details")?)?;
    require_schema(details)?;

    match decision.as_str() {
        PRICE_ALERT_SAMPLED => parse_sample(event, details, symbol).map(Some),
        PRICE_ALERT_OCCURRED => parse_occurrence(event, details, symbol).map(Some),
        PRICE_ALERT_ACKNOWLEDGED => parse_acknowledgement(event, details, symbol).map(Some),
        PRICE_ALERT_DELIVERY_PENDING
        | PRICE_ALERT_DELIVERY_DROPPED
        | PRICE_ALERT_DELIVERY_SUCCEEDED
        | PRICE_ALERT_DELIVERY_FAILED
        | PRICE_ALERT_DELIVERY_TIMED_OUT => parse_delivery(details, symbol, &decision).map(Some),
        _ => Err(()),
    }
}

fn parse_sample(
    event: &OperationEventEnvelope,
    details: &Map<String, Value>,
    symbol: String,
) -> Result<RecoveredAlertFact, ()> {
    require_exact_keys(
        details,
        &[
            "schema_version",
            "exchange",
            "market_type",
            "revision",
            "market_generation",
            "price",
        ],
    )?;
    if required_u64(details, "revision")? == 0 || required_u64(details, "market_generation")? == 0 {
        return Err(());
    }
    Ok(RecoveredAlertFact::Sample {
        instrument: parse_instrument(details, symbol)?,
        recorded_at: event.recorded_at(),
        price: parse_price(details, "price")?,
    })
}

fn parse_occurrence(
    event: &OperationEventEnvelope,
    details: &Map<String, Value>,
    symbol: String,
) -> Result<RecoveredAlertFact, ()> {
    require_exact_keys(
        details,
        &[
            "schema_version",
            "sequence",
            "exchange",
            "market_type",
            "kind",
            "price",
            "change_percent",
            "market_revision",
            "market_generation",
        ],
    )?;
    let sequence = required_u64(details, "sequence")?;
    if sequence == 0
        || required_u64(details, "market_revision")? == 0
        || required_u64(details, "market_generation")? == 0
    {
        return Err(());
    }
    Ok(RecoveredAlertFact::Occurrence {
        notification: AlertNotification {
            occurrence_id: AlertOccurrenceId {
                instrument: parse_instrument(details, symbol)?,
                sequence,
            },
            kind: parse_alert_kind(&required_text(details, "kind")?)?,
            price: parse_price(details, "price")?,
            change_percent: optional_decimal(details, "change_percent")?,
            occurred_at: event.recorded_at(),
        },
    })
}

fn parse_acknowledgement(
    event: &OperationEventEnvelope,
    details: &Map<String, Value>,
    symbol: String,
) -> Result<RecoveredAlertFact, ()> {
    require_exact_keys(
        details,
        &["schema_version", "sequence", "exchange", "market_type"],
    )?;
    let sequence = required_u64(details, "sequence")?;
    if sequence == 0 {
        return Err(());
    }
    Ok(RecoveredAlertFact::Acknowledged {
        occurrence_id: AlertOccurrenceId {
            instrument: parse_instrument(details, symbol)?,
            sequence,
        },
        acknowledged_at: event.recorded_at(),
    })
}

fn parse_delivery(
    details: &Map<String, Value>,
    symbol: String,
    decision: &str,
) -> Result<RecoveredAlertFact, ()> {
    require_exact_keys(
        details,
        &[
            "schema_version",
            "sequence",
            "exchange",
            "market_type",
            "adapter_id",
            "failure",
        ],
    )?;
    let sequence = required_u64(details, "sequence")?;
    if sequence == 0 {
        return Err(());
    }
    let occurrence_id = AlertOccurrenceId {
        instrument: parse_instrument(details, symbol)?,
        sequence,
    };
    validate_adapter_id(&required_text(details, "adapter_id")?).map_err(|_| ())?;
    let failure = details.get("failure").ok_or(())?;
    match (decision, failure.as_str()) {
        (PRICE_ALERT_DELIVERY_PENDING | PRICE_ALERT_DELIVERY_SUCCEEDED, None)
            if failure.is_null() => {}
        (PRICE_ALERT_DELIVERY_DROPPED, Some("backpressure" | "adapter_closed"))
        | (PRICE_ALERT_DELIVERY_FAILED, Some("device_unavailable" | "backpressure" | "rejected"))
        | (PRICE_ALERT_DELIVERY_TIMED_OUT, Some("timeout")) => {}
        _ => return Err(()),
    }
    Ok(RecoveredAlertFact::Delivery { occurrence_id })
}

pub(crate) const fn alert_kind_label(kind: AlertKind) -> &'static str {
    match kind {
        AlertKind::VolatilityUp => "volatility_up",
        AlertKind::VolatilityDown => "volatility_down",
        AlertKind::UpperLimit => "upper_limit",
        AlertKind::LowerLimit => "lower_limit",
    }
}

pub(crate) fn market_type_label(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "spot",
        MarketType::Perpetual => "perpetual",
    }
}

fn parse_alert_kind(value: &str) -> Result<AlertKind, ()> {
    match value {
        "volatility_up" => Ok(AlertKind::VolatilityUp),
        "volatility_down" => Ok(AlertKind::VolatilityDown),
        "upper_limit" => Ok(AlertKind::UpperLimit),
        "lower_limit" => Ok(AlertKind::LowerLimit),
        _ => Err(()),
    }
}

fn parse_instrument(details: &Map<String, Value>, symbol: String) -> Result<MarketInstrument, ()> {
    let exchange = required_text(details, "exchange")?;
    let symbol = Symbol::new(symbol).map_err(|_| ())?;
    let market_type = match required_text(details, "market_type")?.as_str() {
        "spot" => MarketType::Spot,
        "perpetual" => MarketType::Perpetual,
        _ => return Err(()),
    };
    MarketInstrument::new(exchange, symbol, market_type).map_err(|_| ())
}

fn require_schema(details: &Map<String, Value>) -> Result<(), ()> {
    if required_u64(details, "schema_version")? != u64::from(PRICE_ALERT_EVENT_SCHEMA_VERSION) {
        return Err(());
    }
    Ok(())
}

fn parse_price(details: &Map<String, Value>, key: &str) -> Result<Price, ()> {
    let value = required_decimal_text(details, key)?;
    Price::from_str(&value).map_err(|_| ())
}

fn optional_decimal(details: &Map<String, Value>, key: &str) -> Result<Option<Decimal>, ()> {
    let value = required(details, key)?;
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_str().ok_or(())?;
    if !valid_decimal_text(value.as_bytes()) || value.len() > MAX_DECIMAL_TEXT_BYTES {
        return Err(());
    }
    Decimal::from_str(value).map(Some).map_err(|_| ())
}

fn require_exact_keys(object: &Map<String, Value>, expected: &[&str]) -> Result<(), ()> {
    if object.len() != expected.len() || object.keys().any(|key| !expected.contains(&key.as_str()))
    {
        return Err(());
    }
    Ok(())
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ()> {
    object.get(key).ok_or(())
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ()> {
    required(object, key)?.as_u64().ok_or(())
}

fn required_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?;
    if value.is_empty() || value.len() > MAX_ALERT_TEXT_BYTES || value.chars().any(char::is_control)
    {
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
