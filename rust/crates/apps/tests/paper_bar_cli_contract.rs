use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_backtest::{
    CausalSpotEvaluator, CostSchedule, DatasetManifest, EvaluationSplitConfig, SelectionPhase,
    Sha256Digest, SpotBar, SpotKlineDataset, SpotStrategyConfig, TimestampUnit,
};
use crypto_trading_domain::{MarketType, Money, Price, Symbol};
use rust_decimal::Decimal;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn temp_path(label: &str, extension: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-{label}-{}-{nonce}.{extension}",
        std::process::id()
    ))
}

fn write_temp(label: &str, extension: &str, body: &str) -> PathBuf {
    let path = temp_path(label, extension);
    std::fs::write(&path, body).unwrap();
    path
}

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn money(value: &str) -> Money {
    Money::new(decimal(value))
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn bar(day: i64, open: &str, close: &str) -> SpotBar {
    let open_time = Utc.timestamp_opt(day * 86_400, 0).unwrap();
    SpotBar::new(
        open_time,
        open_time + Duration::days(1) - Duration::milliseconds(1),
        price(open),
        price(&decimal(open).max(decimal(close)).to_string()),
        price(&decimal(open).min(decimal(close)).to_string()),
        price(close),
        Decimal::ONE,
        decimal("100"),
        1,
    )
    .unwrap()
}

fn digest(character: char) -> Sha256Digest {
    Sha256Digest::new(&character.to_string().repeat(64)).unwrap()
}

fn verified_dataset(bars: &[SpotBar]) -> SpotKlineDataset {
    let mut csv = String::new();
    for bar in bars {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            bar.open_time.timestamp_millis(),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.close_time.timestamp_millis(),
            bar.quote_volume,
            bar.trade_count,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        )
        .unwrap();
    }
    let first = bars.first().unwrap();
    let last = bars.last().unwrap();
    let sealed_at = last.close_time + Duration::milliseconds(1);
    SpotKlineDataset::parse_csv(
        DatasetManifest {
            source_url: "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/test.zip"
                .to_owned(),
            retrieved_at: sealed_at,
            venue: "binance".to_owned(),
            product: MarketType::Spot,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval_micros: 86_400_000_000,
            timezone: "UTC".to_owned(),
            timestamp_unit: TimestampUnit::Milliseconds,
            archive_sha256: digest('a'),
            content_sha256: Sha256Digest::from_bytes(csv.as_bytes()),
            parser_version: "binance-spot-kline-v1".to_owned(),
            expected_first_open: first.open_time,
            expected_last_close: last.close_time,
            expected_bar_count: bars.len(),
        },
        &csv,
        &digest('a'),
        sealed_at,
    )
    .unwrap()
}

fn dataset_with_holdout(mut bars: Vec<SpotBar>) -> SpotKlineDataset {
    let last = bars.last().unwrap();
    let next_open = last.open_time + Duration::days(1);
    bars.push(
        SpotBar::new(
            next_open,
            next_open + Duration::days(1) - Duration::milliseconds(1),
            last.close,
            last.close,
            last.close,
            last.close,
            Decimal::ONE,
            decimal("100"),
            1,
        )
        .unwrap(),
    );
    verified_dataset(&bars)
}

fn live_csv(bars: &[SpotBar]) -> String {
    let mut body = String::new();
    for bar in bars {
        writeln!(
            body,
            "{},{},{},{},{},{},{},{},{},0,0,0",
            bar.open_time.timestamp_millis(),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.close_time.timestamp_millis(),
            bar.quote_volume,
            bar.trade_count,
        )
        .unwrap();
    }
    body
}

fn history_lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct JournalTrade {
    occurred_at: chrono::DateTime<Utc>,
    side: String,
    quantity: String,
    fill_price: String,
}

fn completed_trades(lines: &[serde_json::Value]) -> Vec<JournalTrade> {
    lines
        .iter()
        .filter(|line| line["strategy"] == "paper_bar" && line["decision"] == "execution_completed")
        .map(|line| {
            let order = &line["details"]["receipts"][0]["order"];
            JournalTrade {
                occurred_at: order["updated_at"].as_str().unwrap().parse().unwrap(),
                side: order["intent"]["side"].as_str().unwrap().to_owned(),
                quantity: order["filled_quantity"].as_str().unwrap().to_owned(),
                fill_price: order["average_fill_price"].as_str().unwrap().to_owned(),
            }
        })
        .collect()
}

fn decision_indexes(lines: &[serde_json::Value]) -> Vec<usize> {
    lines
        .iter()
        .filter(|line| line["strategy"] == "paper_bar" && line["decision"] == "paper_bar_decided")
        .map(|line| usize::try_from(line["details"]["bar_index"].as_u64().unwrap()).unwrap())
        .collect()
}

#[test]
fn paper_bar_cli_matches_causal_evaluator_with_absolute_indexes_and_actual_targets() {
    let prefix = vec![
        bar(0, "100", "101"),
        bar(1, "101", "103"),
        bar(2, "103", "99"),
        bar(3, "99", "104"),
        bar(4, "104", "98"),
    ];
    let live = vec![
        bar(5, "98", "100"),
        bar(6, "100", "130"),
        bar(7, "130", "84"),
        bar(8, "84", "140"),
        bar(9, "140", "88"),
        bar(10, "88", "150"),
    ];
    let warmup_csv = write_temp("paper-bar-warmup", "csv", &live_csv(&prefix));
    let bars_csv = write_temp("paper-bar-live", "csv", &live_csv(&live));
    let history_path = temp_path("paper-bar-history", "jsonl");

    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "paper-bar",
            "--task-id",
            "paper-bar-owner",
            "--warmup-bars-csv",
            warmup_csv.to_str().unwrap(),
            "--bars-csv",
            bars_csv.to_str().unwrap(),
            "--history-path",
            history_path.to_str().unwrap(),
            "--symbol",
            "BTC-USDT-SPOT",
            "--start-bar-index",
            "5",
            "--initial-available",
            "1000",
            "--fee-bps",
            "10",
            "--half-spread-bps",
            "5",
            "--slippage-bps",
            "7",
            "--latency-bps",
            "3",
            "capped-volatility-target",
            "--lookback-returns",
            "2",
            "--annual-target",
            "0.15",
            "--rebalance-band",
            "0.20",
            "--rebalance-every-bars",
            "3",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let lines = history_lines(&history_path);
    assert_eq!(decision_indexes(&lines), vec![5, 6, 7, 8, 9, 10]);

    let mut full_bars = prefix.clone();
    full_bars.extend(live.clone());
    let dataset = dataset_with_holdout(full_bars.clone());
    let sample = SelectionPhase::new(&dataset, EvaluationSplitConfig::new(1, 1, 1, 0, 1).unwrap())
        .unwrap()
        .sample(5..11)
        .unwrap();
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(decimal("10"), decimal("5"), decimal("7"), decimal("3")).unwrap(),
    )
    .unwrap();
    let mut strategy = SpotStrategyConfig::CappedVolatilityTarget {
        lookback_returns: 2,
        annual_target: decimal("0.15"),
        rebalance_band: decimal("0.20"),
        rebalance_every_bars: 3,
    }
    .build()
    .unwrap();
    let expected = evaluator.run(&sample, &mut strategy).unwrap();
    let expected_trades = expected
        .trades
        .into_iter()
        .map(|trade| JournalTrade {
            occurred_at: trade.trade.fill.occurred_at,
            side: match trade.trade.fill.side {
                crypto_trading_domain::Side::Buy => "buy".to_owned(),
                crypto_trading_domain::Side::Sell => "sell".to_owned(),
            },
            quantity: trade.trade.fill.quantity.to_string(),
            fill_price: trade.trade.fill.fill_price.to_string(),
        })
        .collect::<Vec<_>>();

    assert!(!expected_trades.is_empty());
    assert_eq!(completed_trades(&lines), expected_trades);

    let _ = std::fs::remove_file(warmup_csv);
    let _ = std::fs::remove_file(bars_csv);
    let _ = std::fs::remove_file(history_path);
}

#[test]
fn paper_bar_cli_uses_account_risk_to_block_entry() {
    let bars = vec![bar(0, "100", "100"), bar(1, "100", "100")];
    let bars_csv = write_temp("paper-bar-risk-bars", "csv", &live_csv(&bars));
    let history_path = temp_path("paper-bar-risk-history", "jsonl");
    let risk_config = write_temp(
        "paper-bar-risk-config",
        "yaml",
        r#"
max_symbol_exposure: "10"
max_total_exposure: "10"
min_balance_warning: "1"
min_balance_close_position: "1"
max_position_duration_seconds: 86400
max_daily_trades: 100
disabled_symbols: []
high_risk_symbols: []
"#,
    );

    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "paper-bar",
            "--task-id",
            "paper-bar-risk-owner",
            "--bars-csv",
            bars_csv.to_str().unwrap(),
            "--history-path",
            history_path.to_str().unwrap(),
            "--symbol",
            "BTC-USDT-SPOT",
            "--initial-available",
            "1000",
            "--paper-account-risk-config",
            risk_config.to_str().unwrap(),
            "buy-and-hold",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let lines = history_lines(&history_path);
    assert!(
        lines
            .iter()
            .any(|line| line["decision"] == "paper_bar_risk_rejected")
    );
    assert!(
        !lines
            .iter()
            .any(|line| line["decision"] == "execution_completed")
    );

    let _ = std::fs::remove_file(bars_csv);
    let _ = std::fs::remove_file(history_path);
    let _ = std::fs::remove_file(risk_config);
}

#[test]
fn paper_bar_cli_refuses_to_reuse_a_non_empty_journal() {
    let bars = vec![bar(0, "100", "101"), bar(1, "101", "102")];
    let bars_csv = write_temp("paper-bar-restart-bars", "csv", &live_csv(&bars));
    let history_path = temp_path("paper-bar-restart-history", "jsonl");

    let first = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "paper-bar",
            "--task-id",
            "paper-bar-restart-owner",
            "--bars-csv",
            bars_csv.to_str().unwrap(),
            "--history-path",
            history_path.to_str().unwrap(),
            "--symbol",
            "BTC-USDT-SPOT",
            "--initial-available",
            "1000",
            "buy-and-hold",
        ])
        .output()
        .unwrap();
    assert!(first.status.success(), "{first:?}");

    let second = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "paper-bar",
            "--task-id",
            "paper-bar-restart-owner",
            "--bars-csv",
            bars_csv.to_str().unwrap(),
            "--history-path",
            history_path.to_str().unwrap(),
            "--symbol",
            "BTC-USDT-SPOT",
            "--initial-available",
            "1000",
            "buy-and-hold",
        ])
        .output()
        .unwrap();

    assert!(!second.status.success(), "{second:?}");
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("does not support journal recovery"),
        "{second:?}"
    );

    let _ = std::fs::remove_file(bars_csv);
    let _ = std::fs::remove_file(history_path);
}
