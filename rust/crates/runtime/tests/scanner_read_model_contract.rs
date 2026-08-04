use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::MarketType;
use crypto_trading_runtime::{
    MAX_VIRTUAL_GRID_SCANNER_ROWS, ProjectionStatus,
    VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION, VirtualGridScannerReadModel,
};
use serde_json::{Value, json};
use uuid::Uuid;

type RankingMutation = Box<dyn Fn(&mut Value)>;

#[test]
fn no_scanner_record_is_an_explicit_unobserved_projection() {
    let snapshot = snapshot(&[record("grid", "hold", &json!({}), 0)]);

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(
        model.schema_version,
        VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION
    );
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.journal_head_sequence, Some(1));
    assert!(model.latest.is_none());
    assert_eq!(model.invalid_event_count, 0);
}

#[test]
fn valid_ranking_projects_exact_order_metrics_and_safe_identity() {
    let snapshot = snapshot(&[scanner_record_v2("scan-1", &valid_rows_v2(), 0)]);

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let latest = model.latest.unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(latest.source_sequence, 1);
    assert_eq!(latest.run_id, "scan-1");
    assert_eq!(latest.recorded_at, timestamp(300));
    assert_eq!(latest.candidate_count, 2);
    assert_eq!(latest.eligible_count, 2);
    assert_eq!(latest.filtered_by_cycles_count, 0);
    assert!(!latest.truncated);
    assert_eq!(latest.rows.len(), 2);
    assert_eq!(latest.rows[0].rank, 1);
    assert_eq!(latest.rows[0].instrument.symbol, "BTC-USDC");
    assert_eq!(latest.rows[0].instrument.market_type, MarketType::Spot);
    assert!(latest.rows[0].is_benchmark());
    assert_eq!(latest.rows[0].estimated_apr, "100");
    assert_eq!(
        latest.estimated_apr_kind,
        crypto_trading_runtime::ScannerAprEstimateKindView::Heuristic
    );
    assert_eq!(latest.estimated_apr_assumptions.order_notional_usdc, "100");
    assert_eq!(
        latest.estimated_apr_assumptions.round_trip_fee_percent,
        "0.2"
    );
    assert_eq!(
        latest.rows[0].estimated_apr_kind,
        crypto_trading_runtime::ScannerAprEstimateKindView::Heuristic
    );
    assert_eq!(latest.rows[0].rating_grade.to_string(), "c");
    assert_eq!(latest.rows[0].rating_score, "50");
    assert_eq!(latest.rows[1].instrument.symbol, "ETH-USDC");
    assert_eq!(latest.rows[1].estimated_apr, "500");

    let encoded = serde_json::to_string(&latest).unwrap();
    for forbidden in ["orders", "intents", "api_key", "authorization", "secret"] {
        assert!(
            !encoded.contains(forbidden),
            "{forbidden} leaked in {encoded}"
        );
    }
}

#[test]
fn legacy_v1_ranking_projects_unknown_kind_and_legacy_apr_assumptions() {
    let snapshot = snapshot(&[scanner_record_v1("scan-v1", &valid_rows_v1(), 0)]);

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let latest = model.latest.unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(latest.run_id, "scan-v1");
    assert_eq!(
        latest.estimated_apr_kind,
        crypto_trading_runtime::ScannerAprEstimateKindView::Unknown
    );
    assert_eq!(latest.estimated_apr_assumptions.order_notional_usdc, "10");
    assert_eq!(
        latest.estimated_apr_assumptions.round_trip_fee_percent,
        "0.004"
    );
    assert_eq!(
        latest.rows[0].estimated_apr_kind,
        crypto_trading_runtime::ScannerAprEstimateKindView::Unknown
    );
}

#[test]
fn checked_web_fixture_projects_three_ordered_rows() {
    let snapshot = crypto_trading_runtime::JournalSnapshot::new(
        fixed_uuid(321),
        include_bytes!("../../../fixtures/m3-scanner-journal.jsonl").to_vec(),
    )
    .unwrap();

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let latest = model.latest.unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(latest.run_id, "scanner-browser-fixture");
    assert_eq!(latest.rows.len(), 3);
    assert_eq!(latest.rows[0].instrument.symbol, "BTC-USDC");
    assert_eq!(latest.rows[1].instrument.symbol, "ETH-USDC");
    assert_eq!(latest.rows[2].instrument.symbol, "SOL-USDC");
}

#[test]
fn crlf_is_accepted_without_changing_the_projection() {
    let value = scanner_record_v2("scan-crlf", &valid_rows_v2(), 0);
    let lf = jsonl(std::slice::from_ref(&value));
    let crlf = String::from_utf8(lf.clone())
        .unwrap()
        .replace('\n', "\r\n")
        .into_bytes();

    let lf_model = VirtualGridScannerReadModel::from_legacy_snapshot(
        &crypto_trading_runtime::JournalSnapshot::new(fixed_uuid(1), lf).unwrap(),
    )
    .unwrap();
    let crlf_model = VirtualGridScannerReadModel::from_legacy_snapshot(
        &crypto_trading_runtime::JournalSnapshot::new(fixed_uuid(1), crlf).unwrap(),
    )
    .unwrap();

    assert_eq!(lf_model, crlf_model);
}

#[test]
fn malformed_scanner_fact_degrades_and_preserves_last_valid_ranking() {
    let valid = scanner_record_v2("scan-valid", &valid_rows_v2(), 0);
    let mut invalid = scanner_record_v2("scan-invalid", &valid_rows_v2(), 1);
    invalid["details"]["rows"][0]["rank"] = json!(2);
    let snapshot = snapshot(&[valid, invalid]);

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert_eq!(model.latest.unwrap().run_id, "scan-valid");
}

#[test]
fn partial_tail_degrades_and_preserves_the_last_complete_ranking() {
    let mut bytes = jsonl(&[scanner_record_v2("scan-valid", &valid_rows_v2(), 0)]);
    bytes.extend_from_slice(br#"{"timestamp":"2026-07-25T00:05:01Z""#);
    let snapshot = crypto_trading_runtime::JournalSnapshot::new(fixed_uuid(1), bytes).unwrap();

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 0);
    assert_eq!(model.latest.unwrap().run_id, "scan-valid");
}

#[test]
fn strict_schema_rejects_ambiguous_or_incoherent_rankings() {
    let cases: Vec<(&str, RankingMutation)> = vec![
        (
            "unknown field",
            Box::new(|value| value["details"]["raw_error"] = json!("must not survive")),
        ),
        (
            "noncanonical decimal",
            Box::new(|value| {
                value["details"]["rows"][0]["estimated_apr"] = json!("0100.0");
            }),
        ),
        (
            "duplicate identity",
            Box::new(|value| {
                value["details"]["rows"][1]["instrument"] =
                    value["details"]["rows"][0]["instrument"].clone();
            }),
        ),
        (
            "spoofable unicode identity",
            Box::new(|value| {
                value["details"]["rows"][0]["instrument"]["symbol"] = json!("BTC-USDC\u{202e}");
            }),
        ),
        (
            "wrong deterministic order",
            Box::new(|value| {
                value["details"]["rows"][0]["priority"] = json!("standard");
                value["details"]["rows"][1]["priority"] = json!("benchmark");
            }),
        ),
        (
            "count contradiction",
            Box::new(|value| {
                value["details"]["filtered_by_cycles_count"] = json!(1);
            }),
        ),
        (
            "timestamp contradiction",
            Box::new(|value| {
                value["details"]["rows"][0]["last_observed_at"] = json!("2026-07-25T00:06:00Z");
            }),
        ),
        (
            "rating contradiction",
            Box::new(|value| {
                value["details"]["rows"][0]["rating_score"] = json!("51");
            }),
        ),
        (
            "cross contradiction",
            Box::new(|value| {
                value["details"]["rows"][0]["total_crosses"] = json!(5);
            }),
        ),
        (
            "v2 missing assumptions",
            Box::new(|value| {
                value["details"]
                    .as_object_mut()
                    .unwrap()
                    .remove("estimated_apr_assumptions");
            }),
        ),
        (
            "v2 missing row apr kind",
            Box::new(|value| {
                value["details"]["rows"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("estimated_apr_kind");
            }),
        ),
    ];

    for (name, mutate) in cases {
        let mut value = scanner_record_v2("scan-invalid", &valid_rows_v2(), 0);
        mutate(&mut value);
        let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot(&[value])).unwrap();
        assert_eq!(
            model.projection_status,
            ProjectionStatus::Degraded,
            "{name}"
        );
        assert_eq!(model.invalid_event_count, 1, "{name}");
        assert!(model.latest.is_none(), "{name}");
    }
}

#[test]
fn scanner_specific_row_bound_is_enforced_before_projection_allocation_grows() {
    let mut rows = Vec::new();
    for index in 0..=MAX_VIRTUAL_GRID_SCANNER_ROWS {
        let mut row = standard_row_v2(index + 1, &format!("S{index:03}-USDC"), "100", "c", "50");
        row["instrument"]["exchange"] = json!("fixture");
        rows.push(row);
    }
    let mut value = scanner_record_v2("scan-too-large", &rows, 0);
    value["details"]["candidate_count"] = json!(MAX_VIRTUAL_GRID_SCANNER_ROWS + 1);
    value["details"]["eligible_count"] = json!(MAX_VIRTUAL_GRID_SCANNER_ROWS + 1);
    value["details"]["row_limit"] = json!(MAX_VIRTUAL_GRID_SCANNER_ROWS);
    value["details"]["truncated"] = json!(true);

    let model = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot(&[value])).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert!(model.latest.is_none());
}

fn scanner_record_v2(run_id: &str, rows: &[Value], offset_seconds: i64) -> Value {
    let row_count = rows.len();
    record(
        "virtual_grid_scanner",
        "scanner_ranked",
        &json!({
            "schema_version": 2,
            "run_id": run_id,
            "ranking_policy": "explicit_benchmark_then_apr_desc",
            "apr_window_seconds": 300,
            "estimated_apr_kind": "heuristic",
            "estimated_apr_assumptions": {
                "order_notional_usdc": "100",
                "round_trip_fee_percent": "0.2",
            },
            "min_complete_cycles": 0,
            "row_limit": 50,
            "candidate_count": row_count,
            "eligible_count": row_count,
            "filtered_by_cycles_count": 0,
            "truncated": false,
            "rows": rows,
        }),
        offset_seconds,
    )
}

fn scanner_record_v1(run_id: &str, rows: &[Value], offset_seconds: i64) -> Value {
    let row_count = rows.len();
    record(
        "virtual_grid_scanner",
        "scanner_ranked",
        &json!({
            "schema_version": 1,
            "run_id": run_id,
            "ranking_policy": "explicit_benchmark_then_apr_desc",
            "apr_window_seconds": 300,
            "min_complete_cycles": 0,
            "row_limit": 50,
            "candidate_count": row_count,
            "eligible_count": row_count,
            "filtered_by_cycles_count": 0,
            "truncated": false,
            "rows": rows,
        }),
        offset_seconds,
    )
}

fn valid_rows_v2() -> Vec<Value> {
    vec![
        benchmark_row_v2(1, "BTC-USDC", "100", "c", "50"),
        standard_row_v2(2, "ETH-USDC", "500", "s", "95"),
    ]
}

fn valid_rows_v1() -> Vec<Value> {
    vec![
        benchmark_row_v1(1, "BTC-USDC", "100", "c", "50"),
        standard_row_v1(2, "ETH-USDC", "500", "s", "95"),
    ]
}

fn benchmark_row_v2(rank: usize, symbol: &str, apr: &str, grade: &str, score: &str) -> Value {
    let mut row = standard_row_v2(rank, symbol, apr, grade, score);
    row["priority"] = json!("benchmark");
    row
}

fn benchmark_row_v1(rank: usize, symbol: &str, apr: &str, grade: &str, score: &str) -> Value {
    let mut row = standard_row_v1(rank, symbol, apr, grade, score);
    row["priority"] = json!("benchmark");
    row
}

fn standard_row_v2(rank: usize, symbol: &str, apr: &str, grade: &str, score: &str) -> Value {
    let cycles_per_hour = if apr == "100" { "4" } else { "10" };
    json!({
        "rank": rank,
        "activity": "active",
        "priority": "standard",
        "instrument": {
            "exchange": "fixture",
            "symbol": symbol,
            "market_type": "spot",
        },
        "started_at": timestamp(0),
        "last_observed_at": timestamp(240),
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
        "cycles_per_hour": cycles_per_hour,
        "estimated_apr": apr,
        "estimated_apr_kind": "heuristic",
        "volume_24h_usdc": "1000000",
        "price_change_24h_percent": "2.5",
        "rating_grade": grade,
        "rating_score": score,
    })
}

fn standard_row_v1(rank: usize, symbol: &str, apr: &str, grade: &str, score: &str) -> Value {
    let mut row = standard_row_v2(rank, symbol, apr, grade, score);
    row.as_object_mut().unwrap().remove("estimated_apr_kind");
    row
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

fn record(strategy: &str, decision: &str, details: &Value, offset_seconds: i64) -> Value {
    json!({
        "timestamp": timestamp(300 + offset_seconds),
        "strategy": strategy,
        "symbol": "control-plane",
        "decision": decision,
        "details": details,
    })
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}

fn fixed_uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
