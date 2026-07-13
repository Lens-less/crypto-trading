use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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

#[test]
fn config_check_validates_an_existing_grid_file() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(["config-check", "config/grid/lighter-long-perp-btc.yaml"])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("grid"), "{stdout}");
    assert!(stdout.contains("valid"), "{stdout}");
}

#[test]
fn config_check_returns_nonzero_for_a_missing_file() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(["config-check", "config/does-not-exist.yaml"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("does-not-exist"), "{stderr}");
}

#[test]
fn live_grid_rejects_an_incorrect_acknowledgement() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "grid",
            "config/grid/lighter-long-perp-btc.yaml",
            "--live",
            "--acknowledge-risk",
            "yes",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("I_UNDERSTAND_LIVE_TRADING"), "{stderr}");
}

#[test]
fn grid_once_executes_a_complete_paper_slice() {
    let history = temp_path("grid-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "grid",
            "config/grid/lighter-long-perp-btc.yaml",
            "--price",
            "100000",
            "--once",
            "--history-path",
        ])
        .arg(&history)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("paper executed"), "{stdout}");
    assert!(stdout.contains("100 orders"), "{stdout}");
    assert_history_phases(&history, 100);
    std::fs::remove_file(history).unwrap();
}

#[test]
fn arbitrage_once_executes_strategy_router_and_two_paper_exchanges() {
    let history = temp_path("arbitrage-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "arbitrage",
            "--once",
            "--left-exchange",
            "paper-left",
            "--left-symbol",
            "BTC-USDC-PERP",
            "--left-bid",
            "99.9",
            "--left-ask",
            "100",
            "--right-exchange",
            "paper-right",
            "--right-symbol",
            "BTC-USDC-PERP",
            "--right-bid",
            "101",
            "--right-ask",
            "101.1",
            "--history-path",
        ])
        .arg(&history)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("decision=Open"), "{stdout}");
    assert!(stdout.contains("receipts=2"), "{stdout}");
    assert_history_phases(&history, 2);
    std::fs::remove_file(history).unwrap();
}

#[test]
fn config_check_recognizes_every_supported_schema() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "config-check",
            "config/grid/lighter-long-perp-btc.yaml",
            "config/arbitrage/arbitrage_segmented.yaml",
            "config/arbitrage/monitor_v2.yaml",
            "config/volume_maker/backpack_btc_volume_maker.yaml",
            "config/price_alert/binance_alert.yaml",
            "config/symbol_conversion.yaml",
            "config/exchanges/paradex_config.example.yaml",
            "--json",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    for kind in [
        "grid",
        "arbitrage",
        "monitor",
        "volume-maker",
        "price-alert",
        "symbol-conversion",
        "exchange-auth",
    ] {
        assert!(stdout.contains(&format!(r#""kind": "{kind}""#)), "{stdout}");
    }
}

#[test]
fn config_check_accepts_every_checked_in_exchange_yaml() {
    let exchange_dir = repo_root().join("config/exchanges");
    let mut paths = std::fs::read_dir(&exchange_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect::<Vec<_>>();
    paths.sort();
    assert!(!paths.is_empty());

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .args(&paths)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.matches("valid: exchange-auth").count(), paths.len());
}

#[test]
fn config_check_rejects_an_unknown_mapping_schema() {
    let config = temp_path("unknown-config", "yaml");
    std::fs::write(&config, "logging:\n  level: INFO\n").unwrap();
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&config)
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("unsupported configuration schema"),
        "{stderr}"
    );
}

fn assert_history_phases(path: &Path, receipt_count: u64) {
    let body = std::fs::read_to_string(path).unwrap();
    let records = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2, "{body}");
    assert_eq!(records[0]["decision"], "decision");
    assert_eq!(records[1]["decision"], "receipt");
    assert_eq!(records[1]["details"]["receipt_count"], receipt_count);
}
