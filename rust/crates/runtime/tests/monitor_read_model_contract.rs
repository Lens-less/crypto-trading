use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_runtime::{
    ARBITRAGE_MONITOR_READ_MODEL_SCHEMA_VERSION, ArbitrageMonitorProjection,
    ArbitrageMonitorReadModel, MonitorContinuityState, MonitorFreshnessState,
    MonitorProjectionState, ProjectionStatus,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn no_monitor_records_is_an_explicit_unobserved_projection() {
    let snapshot = snapshot(&[record("grid", "hold", &json!({}), 0)]);

    let model = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(
        model.schema_version,
        ARBITRAGE_MONITOR_READ_MODEL_SCHEMA_VERSION
    );
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.journal_head_sequence, Some(1));
    assert!(model.latest.is_none());
    assert_eq!(model.invalid_event_count, 0);
}

#[test]
fn latest_valid_monitor_event_is_projected_without_order_payloads() {
    let snapshot = snapshot(&[
        monitor_record(
            "monitor_waiting",
            1,
            1,
            &json!({
                "type": "waiting",
                "instrument": leg("right"),
                "freshness": {"status": "missing"},
                "continuity": {"status": "missing"},
            }),
            0,
        ),
        monitor_record(
            "monitor_opportunity",
            2,
            2,
            &json!({
                "type": "opportunity",
                "buy_exchange": "left",
                "sell_exchange": "right",
                "buy_price": "100",
                "sell_price": "102",
                "absolute_spread": "2",
                "spread_percent": "2",
                "threshold_percent": "0.5",
                "intents": ["must-not-leak"],
            }),
            1,
        ),
    ]);

    let model = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let latest = model.latest.unwrap();

    assert_eq!(latest.source_sequence, 2);
    assert_eq!(latest.monitor_sequence, 2);
    assert_eq!(latest.market_generation, 2);
    assert_eq!(latest.state, MonitorProjectionState::Opportunity);
    assert_eq!(latest.left.exchange, "left");
    assert_eq!(latest.right.exchange, "right");
    assert!(matches!(
        latest.projection,
        ArbitrageMonitorProjection::Opportunity {
            ref buy_exchange,
            ref sell_exchange,
            ref spread_percent,
            ref threshold_percent,
            ..
        } if buy_exchange == "left"
            && sell_exchange == "right"
            && spread_percent == "2"
            && threshold_percent == "0.5"
    ));
    let serialized = serde_json::to_value(&latest).unwrap();
    assert!(serialized.get("intents").is_none());
    assert!(serialized.get("orders").is_none());
}

#[test]
fn waiting_projection_keeps_freshness_and_continuity_classifications() {
    let snapshot = snapshot(&[monitor_record(
        "monitor_waiting",
        7,
        9,
        &json!({
            "type": "waiting",
            "instrument": leg("right"),
            "freshness": {
                "status": "stale",
                "age_millis": 11_000,
                "limit_millis": 10_000,
            },
            "continuity": {
                "status": "source_gap",
                "skipped": 4,
            },
        }),
        0,
    )]);

    let latest = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot)
        .unwrap()
        .latest
        .unwrap();

    assert_eq!(latest.state, MonitorProjectionState::Waiting);
    assert!(matches!(
        latest.projection,
        ArbitrageMonitorProjection::Waiting {
            freshness: MonitorFreshnessState::Stale,
            continuity: MonitorContinuityState::SourceGap,
            ..
        }
    ));
}

#[test]
fn pair_skew_waiting_projection_remains_valid_and_operator_visible() {
    let snapshot = snapshot(&[monitor_record(
        "monitor_waiting",
        8,
        10,
        &json!({
            "type": "waiting",
            "instrument": leg("left"),
            "freshness": {
                "status": "pair_skew",
                "skew_millis": 2_000,
                "tolerance_millis": 1_000,
            },
            "continuity": { "status": "continuous" },
        }),
        0,
    )]);

    let model = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let latest = model.latest.unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(latest.state, MonitorProjectionState::Waiting);
    assert!(matches!(
        latest.projection,
        ArbitrageMonitorProjection::Waiting {
            freshness: MonitorFreshnessState::PairSkew,
            continuity: MonitorContinuityState::Continuous,
            ..
        }
    ));
}

#[test]
fn malformed_monitor_event_degrades_projection_and_preserves_last_valid_fact() {
    let snapshot = snapshot(&[
        monitor_record(
            "monitor_no_opportunity",
            1,
            2,
            &json!({
                "type": "no_opportunity",
                "buy_exchange": "left",
                "sell_exchange": "right",
                "buy_price": "100",
                "sell_price": "100",
                "absolute_spread": "0",
                "spread_percent": "0",
                "threshold_percent": "0.5",
            }),
            0,
        ),
        monitor_record(
            "monitor_opportunity",
            2,
            3,
            &json!({"type": "opportunity", "spread_percent": "secret"}),
            1,
        ),
    ]);

    let model = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert_eq!(
        model.latest.unwrap().state,
        MonitorProjectionState::NoOpportunity
    );
}

#[test]
fn partial_tail_degrades_at_the_last_complete_monitor_event() {
    let mut body = jsonl(&[monitor_record(
        "monitor_analysis_rejected",
        1,
        1,
        &json!({"type": "analysis_rejected", "failure": "invalid_financial_value"}),
        0,
    )]);
    body.extend_from_slice(br#"{"timestamp":"#);
    let snapshot = crypto_trading_runtime::JournalSnapshot::new(fixed_uuid(1), body).unwrap();

    let model = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(
        model.latest.unwrap().state,
        MonitorProjectionState::AnalysisRejected
    );
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

fn monitor_record(
    decision: &str,
    monitor_sequence: u64,
    market_generation: u64,
    outcome: &Value,
    offset_seconds: i64,
) -> Value {
    record(
        "arbitrage_monitor",
        decision,
        &json!({
            "schema_version": 1,
            "sequence": monitor_sequence,
            "market_generation": market_generation,
            "market_update": "accepted",
            "left": leg("left"),
            "right": leg("right"),
            "outcome": outcome,
        }),
        offset_seconds,
    )
}

fn record(strategy: &str, decision: &str, details: &Value, offset_seconds: i64) -> Value {
    json!({
        "timestamp": timestamp(offset_seconds),
        "strategy": strategy,
        "symbol": "BTC-USDT",
        "decision": decision,
        "details": details,
    })
}

fn leg(exchange: &str) -> Value {
    json!({
        "exchange": exchange,
        "symbol": "BTC-USDT",
        "market_type": "perpetual",
    })
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}

fn fixed_uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
