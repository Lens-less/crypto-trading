use std::sync::Arc;

use crypto_trading_control_plane::{CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION, ReadControlPlane};
use crypto_trading_runtime::{
    MemoryJournalSnapshotSource, ProjectionStatus, VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn scanner_projection_shares_the_frozen_control_plane_snapshot() {
    let source = MemoryJournalSnapshotSource::new(
        fixed_uuid(1),
        jsonl(&[
            decision_record("grid", "hold", &json!({})),
            scanner_record("scan-control-plane"),
        ]),
    )
    .unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let snapshot = control_plane.snapshot().unwrap();

    assert_eq!(
        snapshot.schema_version,
        CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION
    );
    assert_eq!(CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION, 7);
    assert_eq!(
        snapshot.scanner.schema_version,
        VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION
    );
    assert_eq!(
        snapshot.scanner.projection_status,
        ProjectionStatus::Complete
    );
    assert_eq!(snapshot.scanner.journal_head_sequence, Some(2));
    assert_eq!(
        snapshot.scanner.latest.unwrap().run_id,
        "scan-control-plane"
    );
    assert_eq!(snapshot.operator.head_sequence, Some(2));
}

#[test]
fn invalid_scanner_fact_degrades_only_the_scanner_projection() {
    let mut invalid = scanner_record("scan-invalid");
    invalid["details"]["rows"][0]["orders"] = json!(["must-not-project"]);
    let source = MemoryJournalSnapshotSource::new(fixed_uuid(2), jsonl(&[invalid])).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let snapshot = control_plane.snapshot().unwrap();

    assert_eq!(
        snapshot.scanner.projection_status,
        ProjectionStatus::Degraded
    );
    assert!(snapshot.scanner.latest.is_none());
    assert_eq!(snapshot.scanner.invalid_event_count, 1);
    assert_eq!(
        snapshot.operator.projection_status,
        ProjectionStatus::Complete
    );
    assert!(snapshot.operator.batches.is_empty());
}

fn scanner_record(run_id: &str) -> Value {
    decision_record(
        "virtual_grid_scanner",
        "scanner_ranked",
        &json!({
            "schema_version": 1,
            "run_id": run_id,
            "ranking_policy": "explicit_benchmark_then_apr_desc",
            "apr_window_seconds": 300,
            "min_complete_cycles": 0,
            "row_limit": 50,
            "candidate_count": 1,
            "eligible_count": 1,
            "filtered_by_cycles_count": 0,
            "truncated": false,
            "rows": [{
                "rank": 1,
                "activity": "active",
                "priority": "standard",
                "instrument": {
                    "exchange": "fixture",
                    "symbol": "ETH-USDC",
                    "market_type": "spot",
                },
                "started_at": "2026-07-25T00:00:00Z",
                "last_observed_at": "2026-07-25T00:04:00Z",
                "observation_count": 5,
                "last_observation_sequence": 5,
                "current_price": "100",
                "lower_price": "95",
                "upper_price": "105",
                "pending_buy_price": "99",
                "pending_sell_price": "101",
                "grid_width_percent": "10",
                "grid_interval_percent": "1",
                "grid_count": 10,
                "running_seconds": 300,
                "buy_crosses": 2,
                "sell_crosses": 2,
                "total_crosses": 4,
                "complete_cycles": 2,
                "recent_five_minute_cycles": 2,
                "cycles_per_hour": "10",
                "estimated_apr": "500",
                "volume_24h_usdc": "1000000",
                "price_change_24h_percent": null,
                "rating_grade": "s",
                "rating_score": "95",
            }],
        }),
    )
}

fn decision_record(strategy: &str, decision: &str, details: &Value) -> Value {
    json!({
        "timestamp": "2026-07-25T00:05:00Z",
        "strategy": strategy,
        "symbol": "control-plane",
        "decision": decision,
        "details": details,
    })
}

fn jsonl(records: &[Value]) -> Vec<u8> {
    let mut body = Vec::new();
    for record in records {
        body.extend_from_slice(serde_json::to_string(record).unwrap().as_bytes());
        body.push(b'\n');
    }
    body
}

fn fixed_uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
