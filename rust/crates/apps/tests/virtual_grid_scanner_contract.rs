use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_cli::scanner::{
    DeterministicVirtualGridScanner, ScannerActivity, ScannerPriority, VirtualGridScanCandidate,
    VirtualGridScanObservation, VirtualGridScanRequest, VirtualGridScannerError,
};
use crypto_trading_domain::{MarketType, Price, Symbol};
use crypto_trading_runtime::{
    JournalSnapshot, JsonlHistory, MarketInstrument, ProjectionStatus, VirtualGridScannerReadModel,
};
use rust_decimal::Decimal;
use serde_json::Value;

static TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn explicit_benchmark_precedes_apr_and_exact_identity_breaks_ties() {
    let first_path = temp_path("scanner-order-a", "jsonl");
    let second_path = temp_path("scanner-order-b", "jsonl");
    let first_history = JsonlHistory::new(&first_path);
    let second_history = JsonlHistory::new(&second_path);
    let candidates = vec![
        candidate("SOL-USDC", "5", ScannerPriority::Standard, 2),
        candidate("BTC-USDC", "10", ScannerPriority::Benchmark, 1),
        candidate("ETH-USDC", "5", ScannerPriority::Standard, 2),
    ];

    let first = DeterministicVirtualGridScanner::run_and_record(
        request("scan-order-a", candidates.clone(), 0, 10),
        &first_history,
    )
    .await
    .unwrap();
    let second = DeterministicVirtualGridScanner::run_and_record(
        request(
            "scan-order-b",
            candidates.into_iter().rev().collect(),
            0,
            10,
        ),
        &second_history,
    )
    .await
    .unwrap();

    assert_eq!(row_symbols(&first), ["BTC-USDC", "ETH-USDC", "SOL-USDC"]);
    assert_eq!(row_symbols(&first), row_symbols(&second));
    assert_eq!(
        first.rows[0].activity(),
        ScannerActivity::Active,
        "the published ranking contains evaluated rows only"
    );
    assert!(first.rows[0].is_benchmark());
    assert!(
        first.rows[0].estimated_apr() < first.rows[1].estimated_apr(),
        "benchmark priority must be explicit rather than disguised as APR order"
    );
    assert_eq!(first.rows[1].estimated_apr(), first.rows[2].estimated_apr());

    cleanup_file(first_path);
    cleanup_file(second_path);
}

#[tokio::test]
async fn scan_metrics_match_virtual_grid_apr_and_rating_contract() {
    let path = temp_path("scanner-golden", "jsonl");
    let history = JsonlHistory::new(&path);
    let report = DeterministicVirtualGridScanner::run_and_record(
        request(
            "scan-golden",
            vec![candidate("ETH-USDC", "10", ScannerPriority::Standard, 2)],
            0,
            10,
        ),
        &history,
    )
    .await
    .unwrap();
    let row = &report.rows[0];

    assert_eq!(row.rank(), 1);
    assert_eq!(row.buy_crosses(), 2);
    assert_eq!(row.sell_crosses(), 2);
    assert_eq!(row.complete_cycles(), 2);
    assert_eq!(row.recent_five_minute_cycles(), 2);
    assert_eq!(
        row.cycles_per_hour(),
        Decimal::from_str_exact("24.00000000000000000000000001").unwrap()
    );
    assert_eq!(
        row.estimated_apr(),
        Decimal::from_str_exact("20939.904000000000000000000009").unwrap()
    );
    assert_eq!(row.rating_grade().to_string(), "s");
    assert_eq!(row.rating_score(), Decimal::from(95));
    assert_eq!(row.last_observation_sequence(), 5);
    assert_eq!(row.last_observed_at(), timestamp(240));
    assert_eq!(row.evaluated_at(), timestamp(300));

    let records = journal_records(&path);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record["strategy"], "virtual_grid_scanner");
    assert_eq!(record["symbol"], "control-plane");
    assert_eq!(record["decision"], "scanner_ranked");
    assert_eq!(
        record["details"]["ranking_policy"],
        "explicit_benchmark_then_apr_desc"
    );
    assert_eq!(
        record["details"]["rows"][0]["estimated_apr"],
        "20939.904000000000000000000009"
    );
    let encoded = serde_json::to_string(record).unwrap();
    for forbidden in [
        "orders",
        "intents",
        "api_key",
        "authorization",
        "secret",
        "private_key",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "{forbidden} leaked in {encoded}"
        );
    }
    let snapshot = JournalSnapshot::new(
        "00000000-0000-0000-0000-000000000001".parse().unwrap(),
        fs::read(&path).unwrap(),
    )
    .unwrap();
    let projection = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();
    assert_eq!(projection.projection_status, ProjectionStatus::Complete);
    assert_eq!(projection.latest.unwrap().run_id, "scan-golden");

    cleanup_file(path);
}

#[tokio::test]
async fn minimum_cycle_filter_keeps_only_explicit_benchmark_exception() {
    let path = temp_path("scanner-filter", "jsonl");
    let history = JsonlHistory::new(&path);
    let report = DeterministicVirtualGridScanner::run_and_record(
        request(
            "scan-filter",
            vec![
                candidate("BTC-USDC", "10", ScannerPriority::Benchmark, 0),
                candidate("ETH-USDC", "10", ScannerPriority::Standard, 0),
            ],
            1,
            10,
        ),
        &history,
    )
    .await
    .unwrap();

    assert_eq!(report.candidate_count, 2);
    assert_eq!(report.eligible_count, 1);
    assert_eq!(report.filtered_by_cycles_count, 1);
    assert_eq!(row_symbols(&report), ["BTC-USDC"]);
    assert_eq!(report.rows[0].complete_cycles(), 0);

    cleanup_file(path);
}

#[tokio::test]
async fn canonical_ascii_market_separators_round_trip_through_the_projection() {
    let path = temp_path("scanner-canonical-identity", "jsonl");
    let history = JsonlHistory::new(&path);
    let instrument = MarketInstrument::new(
        "fixture.test_v1",
        Symbol::new("UBTC/USDC:USDC@142").unwrap(),
        MarketType::Spot,
    )
    .unwrap();
    let candidate = VirtualGridScanCandidate::new(
        instrument,
        Decimal::TEN,
        Decimal::ONE,
        Decimal::ZERO,
        None,
        ScannerPriority::Standard,
        vec![observation(1, "100", 0)],
    )
    .unwrap();

    DeterministicVirtualGridScanner::run_and_record(
        request("canonical-identity", vec![candidate], 0, 1),
        &history,
    )
    .await
    .unwrap();
    let snapshot = JournalSnapshot::new(
        "00000000-0000-0000-0000-000000000142".parse().unwrap(),
        fs::read(&path).unwrap(),
    )
    .unwrap();
    let projection = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let projected = &projection.latest.unwrap().rows[0].instrument;

    assert_eq!(projected.exchange, "fixture.test_v1");
    assert_eq!(projected.symbol, "UBTC/USDC:USDC@142");

    cleanup_file(path);
}

#[test]
fn invalid_replay_shape_fails_before_any_journal_can_be_selected() {
    let instrument = instrument("ETH-USDC");
    let spoofed_identity = VirtualGridScanCandidate::new(
        MarketInstrument::new(
            "fixture",
            Symbol::new("BTC-USDC\u{202e}").unwrap(),
            MarketType::Spot,
        )
        .unwrap(),
        Decimal::TEN,
        Decimal::ONE,
        Decimal::ZERO,
        None,
        ScannerPriority::Standard,
        vec![observation(1, "100", 0)],
    )
    .unwrap_err();
    assert!(matches!(
        spoofed_identity,
        VirtualGridScannerError::InvalidInstrumentIdentity
    ));

    let non_monotonic = VirtualGridScanCandidate::new(
        instrument.clone(),
        Decimal::TEN,
        Decimal::ONE,
        Decimal::from(1_000_000),
        None,
        ScannerPriority::Standard,
        vec![observation(1, "100", 2), observation(2, "99", 1)],
    )
    .unwrap_err();
    assert!(matches!(
        non_monotonic,
        VirtualGridScannerError::NonMonotonicObservationTime { .. }
    ));

    let duplicate = VirtualGridScanRequest::new(
        "duplicate",
        timestamp(300),
        300,
        0,
        10,
        vec![
            candidate("ETH-USDC", "10", ScannerPriority::Standard, 0),
            candidate("ETH-USDC", "10", ScannerPriority::Standard, 0),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        duplicate,
        VirtualGridScannerError::DuplicateInstrument { .. }
    ));

    let future = VirtualGridScanRequest::new(
        "future",
        timestamp(1),
        300,
        0,
        10,
        vec![
            VirtualGridScanCandidate::new(
                instrument,
                Decimal::TEN,
                Decimal::ONE,
                Decimal::ZERO,
                None,
                ScannerPriority::Standard,
                vec![observation(1, "100", 2)],
            )
            .unwrap(),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        future,
        VirtualGridScannerError::EvaluationPrecedesObservation { .. }
    ));
}

#[tokio::test]
async fn journal_failure_never_returns_an_uncommitted_success_report() {
    let directory = temp_path("scanner-journal-directory", "dir");
    fs::create_dir_all(&directory).unwrap();
    let history = JsonlHistory::new(&directory);

    let error = DeterministicVirtualGridScanner::run_and_record(
        request(
            "scan-journal-failure",
            vec![candidate("ETH-USDC", "10", ScannerPriority::Standard, 1)],
            0,
            10,
        ),
        &history,
    )
    .await
    .unwrap_err();

    assert!(matches!(error, VirtualGridScannerError::Journal(_)));
    fs::remove_dir_all(directory).unwrap();
}

fn request(
    run_id: &str,
    candidates: Vec<VirtualGridScanCandidate>,
    min_complete_cycles: u64,
    row_limit: usize,
) -> VirtualGridScanRequest {
    VirtualGridScanRequest::new(
        run_id,
        timestamp(300),
        300,
        min_complete_cycles,
        row_limit,
        candidates,
    )
    .unwrap()
}

fn candidate(
    symbol: &str,
    width: &str,
    priority: ScannerPriority,
    complete_cycles: usize,
) -> VirtualGridScanCandidate {
    let mut observations = vec![observation(1, "100", 0)];
    let mut sequence = 2_u64;
    for cycle in 0..complete_cycles {
        let offset = i64::try_from(cycle).unwrap() * 120;
        observations.push(observation(sequence, "99", offset + 60));
        sequence += 1;
        observations.push(observation(sequence, "100", offset + 120));
        sequence += 1;
    }
    VirtualGridScanCandidate::new(
        instrument(symbol),
        Decimal::from_str_exact(width).unwrap(),
        Decimal::ONE,
        Decimal::from(1_000_000),
        Some(Decimal::new(25, 1)),
        priority,
        observations,
    )
    .unwrap()
}

fn observation(sequence: u64, price: &str, offset_seconds: i64) -> VirtualGridScanObservation {
    VirtualGridScanObservation::new(
        sequence,
        Price::new(Decimal::from_str_exact(price).unwrap()).unwrap(),
        timestamp(offset_seconds),
    )
    .unwrap()
}

fn instrument(symbol: &str) -> MarketInstrument {
    MarketInstrument::new("fixture", Symbol::new(symbol).unwrap(), MarketType::Spot).unwrap()
}

fn row_symbols(report: &crypto_trading_cli::scanner::VirtualGridScanReport) -> Vec<&str> {
    report
        .rows
        .iter()
        .map(|row| row.instrument().symbol.as_str())
        .collect()
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}

fn journal_records(path: &PathBuf) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn temp_path(stem: &str, extension: &str) -> PathBuf {
    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "crypto-trading-{stem}-{}-{id}.{extension}",
        std::process::id()
    ))
}

fn cleanup_file(path: PathBuf) {
    if path.exists() {
        fs::remove_file(path).unwrap();
    }
}
