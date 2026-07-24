use std::sync::Arc;

use crypto_trading_control_plane::{
    AlertDeliveryStatus, AlertOccurrenceKind, ProjectionStatus, ReadControlPlane,
};
use crypto_trading_runtime::MemoryJournalSnapshotSource;
use serde_json::json;
use uuid::Uuid;

#[test]
fn control_plane_projects_alerts_from_the_same_frozen_journal_generation() {
    let journal_id = Uuid::from_u128(42);
    let source = Arc::new(
        MemoryJournalSnapshotSource::new(
            journal_id,
            jsonl(&[
                alert_record(
                    "price_alert_occurred",
                    &json!({
                        "schema_version": 1,
                        "sequence": 1,
                        "exchange": "binance",
                        "market_type": "spot",
                        "kind": "upper_limit",
                        "price": "101.25",
                        "change_percent": null,
                        "market_revision": 7,
                        "market_generation": 3,
                    }),
                    "2026-07-24T00:00:00Z",
                ),
                alert_record(
                    "price_alert_delivery_pending",
                    &json!({
                        "schema_version": 1,
                        "sequence": 1,
                        "exchange": "binance",
                        "market_type": "spot",
                        "adapter_id": "local_notice",
                        "failure": null,
                    }),
                    "2026-07-24T00:00:01Z",
                ),
                alert_record(
                    "price_alert_delivery_succeeded",
                    &json!({
                        "schema_version": 1,
                        "sequence": 1,
                        "exchange": "binance",
                        "market_type": "spot",
                        "adapter_id": "local_notice",
                        "failure": null,
                    }),
                    "2026-07-24T00:00:02Z",
                ),
            ]),
        )
        .unwrap(),
    );
    let control_plane = ReadControlPlane::new(source).unwrap();

    let read = control_plane.snapshot_with_events_after(None).unwrap();

    assert_eq!(read.snapshot.alerts.journal_id, journal_id);
    assert_eq!(read.events.journal_id, journal_id);
    assert_eq!(read.snapshot.alerts.journal_head_sequence, Some(3));
    assert_eq!(
        read.snapshot.alerts.projection_status,
        ProjectionStatus::Complete
    );
    assert_eq!(read.snapshot.alerts.occurrences.len(), 1);
    let occurrence = &read.snapshot.alerts.occurrences[0];
    assert_eq!(occurrence.kind, AlertOccurrenceKind::UpperLimit);
    assert_eq!(occurrence.price, "101.25");
    assert_eq!(occurrence.deliveries.len(), 1);
    assert_eq!(
        occurrence.deliveries[0].status,
        AlertDeliveryStatus::Succeeded
    );
}

fn alert_record(decision: &str, details: &serde_json::Value, timestamp: &str) -> serde_json::Value {
    json!({
        "timestamp": timestamp,
        "strategy": "price_alert",
        "symbol": "BTC-USDT",
        "decision": decision,
        "details": details,
    })
}

fn jsonl(records: &[serde_json::Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(serde_json::to_string(record).unwrap().as_bytes());
        bytes.push(b'\n');
    }
    bytes
}
