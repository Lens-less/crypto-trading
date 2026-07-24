use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::MarketType;
use crypto_trading_runtime::{
    AlertDeliveryFailure, AlertDeliveryStatus, AlertOccurrenceKind, JournalPageBoundary,
    PRICE_ALERT_READ_MODEL_SCHEMA_VERSION, PriceAlertReadModel, ProjectionStatus,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn valid_alert_lifecycle_projects_occurrence_delivery_and_acknowledgement() {
    let snapshot = snapshot(&lifecycle_records());

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.schema_version, PRICE_ALERT_READ_MODEL_SCHEMA_VERSION);
    assert_eq!(model.journal_head_sequence, Some(6));
    assert_eq!(model.boundary, JournalPageBoundary::SnapshotEnd);
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.invalid_event_count, 0);
    assert!(!model.occurrences_truncated);
    assert_eq!(model.occurrences.len(), 1);

    let occurrence = &model.occurrences[0];
    assert_eq!(occurrence.alert_sequence, 1);
    assert_eq!(occurrence.exchange, "binance");
    assert_eq!(occurrence.symbol, "BTC-USDT");
    assert_eq!(occurrence.market_type, MarketType::Perpetual);
    assert_eq!(occurrence.kind, AlertOccurrenceKind::VolatilityUp);
    assert_eq!(occurrence.price, "101.25");
    assert_eq!(occurrence.change_percent.as_deref(), Some("2.5"));
    assert_eq!(occurrence.deliveries.len(), 2);
    assert!(occurrence.acknowledged_at.is_some());
    assert!(occurrence.deliveries.iter().any(|delivery| {
        delivery.adapter_id == "local_notice"
            && delivery.status == AlertDeliveryStatus::Succeeded
            && delivery.failure.is_none()
    }));
    assert!(occurrence.deliveries.iter().any(|delivery| {
        delivery.adapter_id == "deterministic"
            && delivery.status == AlertDeliveryStatus::Failed
            && delivery.failure == Some(AlertDeliveryFailure::Rejected)
    }));
}

#[test]
fn more_than_256_occurrences_window_the_projection() {
    let records = (1..=257u64)
        .map(|sequence| {
            alert_record(
                "price_alert_occurred",
                &json!({
                    "schema_version": 1,
                    "sequence": sequence,
                    "exchange": "binance",
                    "market_type": "perpetual",
                    "kind": "upper_limit",
                    "price": "100",
                    "change_percent": null,
                    "market_revision": sequence,
                    "market_generation": sequence,
                }),
                i64::try_from(sequence).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let snapshot = snapshot(&records);

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Windowed);
    assert!(model.occurrences_truncated);
    assert_eq!(model.invalid_event_count, 0);
    assert_eq!(model.occurrences.len(), 256);
    assert_eq!(model.occurrences.first().unwrap().alert_sequence, 2);
    assert_eq!(model.occurrences.last().unwrap().alert_sequence, 257);
}

#[test]
fn unknown_field_in_alert_fact_degrades_and_hides_occurrences() {
    let snapshot = snapshot(&[alert_record(
        "price_alert_occurred",
        &json!({
            "schema_version": 1,
            "sequence": 1,
            "exchange": "binance",
            "market_type": "perpetual",
            "kind": "lower_limit",
            "price": "99",
            "change_percent": null,
            "market_revision": 1,
            "market_generation": 1,
            "unexpected": true,
        }),
        0,
    )]);

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert!(model.occurrences.is_empty());
    assert!(!model.occurrences_truncated);
}

#[test]
fn contradictory_delivery_state_degrades_and_hides_occurrences() {
    let snapshot = snapshot(&[
        alert_record(
            "price_alert_occurred",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "kind": "volatility_down",
                "price": "98",
                "change_percent": "-3",
                "market_revision": 1,
                "market_generation": 1,
            }),
            0,
        ),
        alert_record(
            "price_alert_delivery_pending",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "local_notice",
                "failure": null,
            }),
            1,
        ),
        alert_record(
            "price_alert_delivery_failed",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "local_notice",
                "failure": "device_unavailable",
            }),
            2,
        ),
        alert_record(
            "price_alert_delivery_succeeded",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "local_notice",
                "failure": null,
            }),
            3,
        ),
    ]);

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert!(model.occurrences.is_empty());
}

#[test]
fn orphan_reference_degrades_and_hides_occurrences() {
    let snapshot = snapshot(&[alert_record(
        "price_alert_delivery_pending",
        &json!({
            "schema_version": 1,
            "sequence": 9,
            "exchange": "binance",
            "market_type": "perpetual",
            "adapter_id": "local_notice",
            "failure": null,
        }),
        0,
    )]);

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert!(model.occurrences.is_empty());
}

#[test]
fn first_occurrence_must_start_at_sequence_one() {
    let snapshot = snapshot(&[alert_record(
        "price_alert_occurred",
        &json!({
            "schema_version": 1,
            "sequence": 2,
            "exchange": "binance",
            "market_type": "perpetual",
            "kind": "upper_limit",
            "price": "101",
            "change_percent": null,
            "market_revision": 1,
            "market_generation": 1,
        }),
        0,
    )]);

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert!(model.occurrences.is_empty());
}

#[test]
fn partial_tail_degrades_and_hides_even_valid_occurrences() {
    let mut body = jsonl(&[alert_record(
        "price_alert_occurred",
        &json!({
            "schema_version": 1,
            "sequence": 1,
            "exchange": "binance",
            "market_type": "perpetual",
            "kind": "upper_limit",
            "price": "101",
            "change_percent": null,
            "market_revision": 1,
            "market_generation": 1,
        }),
        0,
    )]);
    body.extend_from_slice(br#"{"timestamp":"#);
    let snapshot = crypto_trading_runtime::JournalSnapshot::new(fixed_uuid(1), body).unwrap();

    let model = PriceAlertReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert!(matches!(
        model.boundary,
        JournalPageBoundary::PartialTail { .. }
    ));
    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert!(model.occurrences.is_empty());
}

fn snapshot(records: &[Value]) -> crypto_trading_runtime::JournalSnapshot {
    crypto_trading_runtime::JournalSnapshot::new(fixed_uuid(1), jsonl(records)).unwrap()
}

fn jsonl(records: &[Value]) -> Vec<u8> {
    let mut body = Vec::new();
    for record in records {
        body.extend_from_slice(serde_json::to_string(record).unwrap().as_bytes());
        body.push(b'\n');
    }
    body
}

fn alert_record(decision: &str, details: &Value, offset_seconds: i64) -> Value {
    json!({
        "timestamp": timestamp(offset_seconds),
        "strategy": "price_alert",
        "symbol": "BTC-USDT",
        "decision": decision,
        "details": details,
    })
}

fn lifecycle_records() -> Vec<Value> {
    vec![
        alert_record(
            "price_alert_occurred",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "kind": "volatility_up",
                "price": "101.25",
                "change_percent": "2.5",
                "market_revision": 9,
                "market_generation": 4,
            }),
            0,
        ),
        alert_record(
            "price_alert_delivery_pending",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "local_notice",
                "failure": null,
            }),
            1,
        ),
        alert_record(
            "price_alert_delivery_succeeded",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "local_notice",
                "failure": null,
            }),
            2,
        ),
        alert_record(
            "price_alert_delivery_pending",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "deterministic",
                "failure": null,
            }),
            3,
        ),
        alert_record(
            "price_alert_delivery_failed",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
                "adapter_id": "deterministic",
                "failure": "rejected",
            }),
            4,
        ),
        alert_record(
            "price_alert_acknowledged",
            &json!({
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "perpetual",
            }),
            5,
        ),
    ]
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}

fn fixed_uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
