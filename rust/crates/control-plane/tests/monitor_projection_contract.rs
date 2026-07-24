use std::sync::Arc;

use chrono::{TimeZone, Utc};
use crypto_trading_control_plane::{
    ArbitrageMonitorProjection, CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION, MonitorProjectionState,
    ReadControlPlane,
};
use crypto_trading_runtime::MemoryJournalSnapshotSource;
use serde_json::json;
use uuid::Uuid;

#[test]
fn control_plane_projects_monitor_and_execution_from_one_journal_generation() {
    let source = Arc::new(
        MemoryJournalSnapshotSource::new(
            Uuid::from_u128(41),
            jsonl(&[
                json!({
                    "timestamp": Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap(),
                    "strategy": "arbitrage_monitor",
                    "symbol": "BTC-USDT",
                    "decision": "monitor_opportunity",
                    "details": {
                        "schema_version": 1,
                        "sequence": 2,
                        "market_generation": 2,
                        "market_update": "accepted",
                        "left": leg("left"),
                        "right": leg("right"),
                        "outcome": {
                            "type": "opportunity",
                            "buy_exchange": "left",
                            "sell_exchange": "right",
                            "buy_price": "100",
                            "sell_price": "102",
                            "absolute_spread": "2",
                            "spread_percent": "2",
                            "threshold_percent": "0.5",
                        },
                    },
                }),
                json!({
                    "timestamp": Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 1).single().unwrap(),
                    "strategy": "grid",
                    "symbol": "BTC-USDT",
                    "decision": "hold",
                    "details": {},
                }),
            ]),
        )
        .unwrap(),
    );
    let control_plane = ReadControlPlane::new(source).unwrap();

    let snapshot = control_plane.snapshot().unwrap();

    assert_eq!(
        snapshot.schema_version,
        CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION
    );
    assert_eq!(snapshot.monitor.journal_head_sequence, Some(2));
    let latest = snapshot.monitor.latest.unwrap();
    assert_eq!(latest.source_sequence, 1);
    assert_eq!(latest.state, MonitorProjectionState::Opportunity);
    assert!(matches!(
        latest.projection,
        ArbitrageMonitorProjection::Opportunity {
            ref spread_percent,
            ..
        } if spread_percent == "2"
    ));
    assert_eq!(snapshot.operator.head_sequence, Some(2));
}

fn jsonl(records: &[serde_json::Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(serde_json::to_string(record).unwrap().as_bytes());
        bytes.push(b'\n');
    }
    bytes
}

fn leg(exchange: &str) -> serde_json::Value {
    json!({
        "exchange": exchange,
        "symbol": "BTC-USDT",
        "market_type": "perpetual",
    })
}
