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

fn write_temp(label: &str, extension: &str, body: &str) -> PathBuf {
    let path = temp_path(label, extension);
    std::fs::write(&path, body).unwrap();
    path
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
    assert!(stdout.contains("legacy-parseable"), "{stdout}");
}

#[test]
fn capabilities_json_reports_the_fail_closed_runtime_contract() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(["capabilities", "--json"])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["schema_version"], 1);
    assert_eq!(payload["release_stage"], "paper-only");
    assert_eq!(payload["live_trading_enabled"], false);
    let capabilities = payload["capabilities"].as_array().unwrap();
    assert!(capabilities.iter().any(|entry| {
        entry["id"] == "runtime.live"
            && entry["level"] == "unavailable"
            && entry["scope"]["access"] == "mainnet-trading"
            && entry["scope"]["environments"]
                .as_array()
                .is_some_and(|environments| environments.iter().any(|mode| mode == "mainnet"))
    }));
}

#[test]
fn config_check_returns_nonzero_for_a_missing_file() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(["config-check", "config/does-not-exist.yaml"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("unsupported"), "{stdout}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("configuration check failed"), "{stderr}");
}

#[test]
fn unfinished_commands_fail_closed_instead_of_reporting_success() {
    let cases = [
        vec!["monitor"],
        vec!["volume-maker"],
        vec!["price-alert"],
        vec!["scanner"],
        vec!["arbitrage"],
    ];

    for args in cases {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .args(&args)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}: {output:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("unavailable")
                || stderr.contains("not implemented")
                || stderr.contains("emergency stop"),
            "{args:?}: {stderr}"
        );
    }
}

#[test]
fn volume_maker_cli_enforces_emergency_stop_before_unavailable_runtime() {
    let config = write_temp(
        "volume-maker-emergency-stop",
        "yaml",
        r"
volume_maker:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  order_mode: limit
  order_size: 0.5
  emergency_stop: true
",
    );

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("volume-maker")
        .arg(&config)
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("emergency stop"), "{stderr}");
    assert!(!stderr.contains("runtime is unavailable"), "{stderr}");
}

#[test]
fn scanner_checks_an_explicit_config_path_before_failing_closed() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(["scanner", "--config", "config/does-not-exist.yaml"])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("does-not-exist"), "{stderr}");
}

#[test]
fn grid_price_without_once_is_rejected_without_history() {
    let history = temp_path("grid-price-without-once", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "grid",
            "config/grid/lighter-long-perp-btc.yaml",
            "--price",
            "100000",
            "--history-path",
        ])
        .arg(&history)
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn non_once_cli_rejects_an_oversized_config_before_parsing() {
    let config = temp_path("oversized-grid-config", "yaml");
    std::fs::write(&config, vec![b' '; 1_048_577]).unwrap();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("grid")
        .arg(&config)
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("maximum is 1048576"), "{stderr}");
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
fn grid_once_simulates_paper_order_placement_without_fills() {
    let history = temp_path("grid-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "grid",
            "config/grid/paper-once-btc.yaml",
            "--price",
            "110",
            "--once",
            "--history-path",
        ])
        .arg(&history)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("paper placement simulated"), "{stdout}");
    assert!(stdout.contains("2 orders"), "{stdout}");
    assert_history_phases(&history, 2);
    assert_grid_orders_are_resting(&history, 2);
    std::fs::remove_file(history).unwrap();
}

#[test]
fn grid_once_rejects_ignored_safety_keys_but_config_check_classifies_legacy() {
    let history = temp_path("grid-ignored-safety-history", "jsonl");
    let config = write_temp(
        "grid-ignored-safety",
        "yaml",
        r"
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
  follow_timeout: 10
  take_profit_enabled: true
",
    );

    let check = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&config)
        .output()
        .unwrap();
    assert!(check.status.success(), "{check:?}");
    let check_stdout = String::from_utf8(check.stdout).unwrap();
    assert!(
        check_stdout.contains("legacy-parseable: grid"),
        "{check_stdout}"
    );
    assert!(
        check_stdout.contains("take_profit_enabled"),
        "{check_stdout}"
    );
    assert!(check_stdout.contains("follow_timeout"), "{check_stdout}");

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("grid")
        .arg(&config)
        .args(["--once", "--price", "110", "--history-path"])
        .arg(&history)
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("take_profit_enabled"), "{stderr}");
    assert!(stderr.contains("follow_timeout"), "{stderr}");
    assert!(stderr.contains("config-check"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_once_executes_strategy_router_and_two_paper_exchanges() {
    let history = temp_path("arbitrage-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "arbitrage",
            "--config",
            "config/arbitrage/paper-once-eth.yaml",
            "--monitor-config",
            "config/arbitrage/paper-monitor-eth.yaml",
            "--once",
            "--left-exchange",
            "paper-left",
            "--left-symbol",
            "ETH-USDC-PERP",
            "--left-bid",
            "99.9",
            "--left-ask",
            "100",
            "--left-bid-quantity",
            "0",
            "--left-ask-quantity",
            "1",
            "--right-exchange",
            "paper-right",
            "--right-symbol",
            "ETH-USDC-PERP",
            "--right-bid",
            "101",
            "--right-ask",
            "101.1",
            "--right-bid-quantity",
            "1",
            "--right-ask-quantity",
            "0",
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
    assert_arbitrage_receipts_filled(&history, 2);
    std::fs::remove_file(history).unwrap();
}

#[test]
fn arbitrage_hold_persists_a_non_nil_recoverable_empty_batch() {
    let history = temp_path("arbitrage-hold-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "arbitrage",
            "--config",
            "config/arbitrage/paper-once-eth.yaml",
            "--monitor-config",
            "config/arbitrage/paper-monitor-eth.yaml",
            "--once",
            "--left-exchange",
            "paper-left",
            "--left-symbol",
            "ETH-USDC-PERP",
            "--left-bid",
            "100",
            "--left-ask",
            "100.1",
            "--left-bid-quantity",
            "1",
            "--left-ask-quantity",
            "1",
            "--right-exchange",
            "paper-right",
            "--right-symbol",
            "ETH-USDC-PERP",
            "--right-bid",
            "100",
            "--right-ask",
            "100.1",
            "--right-bid-quantity",
            "1",
            "--right-ask-quantity",
            "1",
            "--history-path",
        ])
        .arg(&history)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("decision=Hold"), "{stdout}");
    assert_history_phases(&history, 0);
    std::fs::remove_file(history).unwrap();
}

#[test]
fn arbitrage_once_rejects_typos_nested_ignored_keys_and_unknown_monitor_fields() {
    let history = temp_path("arbitrage-strict-schema-history", "jsonl");
    let arbitrage = write_temp(
        "arbitrage-unknown-keys",
        "yaml",
        r"
mode: segmented
enabld: true
system_mode:
  monitor_only: false
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
min_spread_pct: 0.02
base_quantity: 0.04
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
max_position_value: 5000
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
    grid_config:
      initial_spread_threshold: 0.02
      grid_step: 0.03
      max_segments: 5
      retry_policy: forever
    quantity_config:
      base_quantity: 0.04
    risk_config:
      max_position_value: 5000
",
    );
    let monitor = write_temp(
        "monitor-unknown-keys",
        "yaml",
        r"
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
thresholds:
  min_spread_pct: 0.02
health_check:
  interval: 10
  data_timeout: 30
  debug_cli: true
",
    );

    let typo = paper_arbitrage_command(
        &arbitrage,
        &repo_root().join("config/arbitrage/paper-monitor-eth.yaml"),
        &history,
        "1",
        "1",
    );
    assert!(!typo.status.success(), "{typo:?}");
    let stderr = String::from_utf8(typo.stderr).unwrap();
    assert!(stderr.contains("enabld"), "{stderr}");
    assert!(stderr.contains("retry_policy"), "{stderr}");

    let companion = paper_arbitrage_command(
        &repo_root().join("config/arbitrage/paper-once-eth.yaml"),
        &monitor,
        &history,
        "1",
        "1",
    );

    let check = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&monitor)
        .output()
        .unwrap();
    assert!(check.status.success(), "{check:?}");
    let check_stdout = String::from_utf8(check.stdout).unwrap();
    assert!(
        check_stdout.contains("legacy-parseable: monitor"),
        "{check_stdout}"
    );
    assert!(check_stdout.contains("thresholds"), "{check_stdout}");
    assert!(
        check_stdout.contains("health_check.interval"),
        "{check_stdout}"
    );

    std::fs::remove_file(arbitrage).unwrap();
    std::fs::remove_file(monitor).unwrap();
    assert!(!companion.status.success(), "{companion:?}");
    let stderr = String::from_utf8(companion.stderr).unwrap();
    assert!(stderr.contains("health_check.debug_cli"), "{stderr}");
    assert!(stderr.contains("health_check.interval"), "{stderr}");
    assert!(stderr.contains("thresholds"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_once_requires_an_explicit_monitor_freshness_limit() {
    let history = temp_path("arbitrage-missing-freshness-history", "jsonl");
    let monitor = write_temp(
        "monitor-missing-freshness",
        "yaml",
        r"
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
",
    );

    let check = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&monitor)
        .output()
        .unwrap();
    assert!(check.status.success(), "{check:?}");
    let stdout = String::from_utf8(check.stdout).unwrap();
    assert!(stdout.contains("legacy-parseable: monitor"), "{stdout}");
    assert!(stdout.contains("health_check.data_timeout"), "{stdout}");

    let output = paper_arbitrage_command(
        &repo_root().join("config/arbitrage/paper-once-eth.yaml"),
        &monitor,
        &history,
        "1",
        "1",
    );
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("health_check.data_timeout"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );

    std::fs::remove_file(monitor).unwrap();
}

#[test]
fn arbitrage_once_rejects_unified_mode_instead_of_silently_running_segmented() {
    let history = temp_path("arbitrage-unified-mode-history", "jsonl");
    let config = write_temp(
        "arbitrage-unified-mode",
        "yaml",
        r"
mode: unified
enabled: true
system_mode:
  monitor_only: false
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
min_spread_pct: 0.02
base_quantity: 0.04
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
max_position_value: 5000
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
    grid_config:
      initial_spread_threshold: 0.02
      grid_step: 0.03
      max_segments: 5
    quantity_config:
      base_quantity: 0.04
    risk_config:
      max_position_value: 5000
",
    );

    let check = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&config)
        .output()
        .unwrap();
    assert!(check.status.success(), "{check:?}");
    let check_stdout = String::from_utf8(check.stdout).unwrap();
    assert!(
        check_stdout.contains("legacy-parseable: arbitrage"),
        "{check_stdout}"
    );
    assert!(check_stdout.contains("only segmented"), "{check_stdout}");

    let output = paper_arbitrage_command(
        &config,
        &repo_root().join("config/arbitrage/paper-monitor-eth.yaml"),
        &history,
        "1",
        "1",
    );

    std::fs::remove_file(config).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("mode=unified"), "{stderr}");
    assert!(stderr.contains("only segmented"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_strict_classification_requires_controls_allowlists_and_position_limits() {
    let history = temp_path("arbitrage-strict-controls-history", "jsonl");
    let missing_enabled = write_temp(
        "arbitrage-missing-enabled",
        "yaml",
        r"
mode: segmented
system_mode: { monitor_only: false }
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
min_spread_pct: 0.02
base_quantity: 0.04
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
max_position_value: 5000
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
",
    );
    let empty_allowlists = write_temp(
        "arbitrage-empty-allowlists",
        "yaml",
        r"
mode: segmented
enabled: true
system_mode: { monitor_only: false }
exchanges: []
symbols: []
min_spread_pct: 0.02
base_quantity: 0.04
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
max_position_value: 5000
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
",
    );
    let missing_position_limit = write_temp(
        "arbitrage-missing-position-limit",
        "yaml",
        r"
mode: segmented
enabled: true
system_mode: { monitor_only: false }
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
min_spread_pct: 0.02
base_quantity: 0.04
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
",
    );

    for (path, expected) in [
        (&missing_enabled, "enabled"),
        (&empty_allowlists, "non-empty list"),
        (&missing_position_limit, "max_position_value"),
    ] {
        let check = Command::new(binary())
            .current_dir(repo_root())
            .arg("config-check")
            .arg(path)
            .output()
            .unwrap();
        assert!(check.status.success(), "{check:?}");
        let stdout = String::from_utf8(check.stdout).unwrap();
        assert!(stdout.contains("legacy-parseable: arbitrage"), "{stdout}");
        assert!(stdout.contains(expected), "expected {expected}: {stdout}");
    }

    let output = paper_arbitrage_command(
        &empty_allowlists,
        &repo_root().join("config/arbitrage/paper-monitor-eth.yaml"),
        &history,
        "1",
        "1",
    );

    for path in [missing_enabled, empty_allowlists, missing_position_limit] {
        std::fs::remove_file(path).unwrap();
    }
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("exchanges"), "{stderr}");
    assert!(stderr.contains("symbols"), "{stderr}");
    assert!(stderr.contains("non-empty list"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_risk_limit_rejects_the_batch_before_execution_or_history() {
    let history = temp_path("arbitrage-risk-rejected-history", "jsonl");
    let arbitrage = write_temp(
        "arbitrage-risk-rejected",
        "yaml",
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [AAA-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.1
    grid_step: 0.1
    max_segments: 1
  quantity_config:
    base_quantity: 1
  risk_config:
    max_position_value: 100.5
symbol_configs:
  AAA-PERP:
    enabled: true
",
    );
    let monitor = write_temp(
        "monitor-risk-rejected",
        "yaml",
        "exchanges: [left, right]\nsymbols: [AAA-PERP]\nhealth_check:\n  data_timeout: 30\n",
    );
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("arbitrage")
        .arg("--config")
        .arg(&arbitrage)
        .arg("--monitor-config")
        .arg(&monitor)
        .args([
            "--once",
            "--left-exchange",
            "left",
            "--left-symbol",
            "AAA-PERP",
            "--left-bid",
            "99.9",
            "--left-ask",
            "100",
            "--left-bid-quantity",
            "1",
            "--left-ask-quantity",
            "1",
            "--right-exchange",
            "right",
            "--right-symbol",
            "AAA-PERP",
            "--right-bid",
            "101",
            "--right-ask",
            "101.1",
            "--right-bid-quantity",
            "1",
            "--right-ask-quantity",
            "1",
            "--history-path",
        ])
        .arg(&history)
        .output()
        .unwrap();

    std::fs::remove_file(arbitrage).unwrap();
    std::fs::remove_file(monitor).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("risk rejected"), "{stderr}");
    assert!(stderr.contains("projected: 101"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_requires_a_position_value_limit_before_paper_execution() {
    let history = temp_path("arbitrage-missing-risk-history", "jsonl");
    let arbitrage = write_temp(
        "arbitrage-missing-risk",
        "yaml",
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [edgex, lighter]
symbols: [ETH-USDC-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.1
    grid_step: 0.1
    max_segments: 1
  quantity_config:
    base_quantity: 1
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
",
    );
    let monitor = write_temp(
        "monitor-missing-risk",
        "yaml",
        "exchanges: [edgex, lighter]\nsymbols: [ETH-USDC-PERP]\nhealth_check:\n  data_timeout: 30\n",
    );
    let output = arbitrage_command(&arbitrage, &monitor, &history, "1", "1");

    std::fs::remove_file(arbitrage).unwrap();
    std::fs::remove_file(monitor).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("max_position_value is required"),
        "{stderr}"
    );
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_rejects_insufficient_executable_depth_before_writing_history() {
    let history = temp_path("arbitrage-insufficient-depth-history", "jsonl");
    let root = repo_root();
    let output = paper_arbitrage_command(
        &root.join("config/arbitrage/paper-once-eth.yaml"),
        &root.join("config/arbitrage/paper-monitor-eth.yaml"),
        &history,
        "0.1",
        "1",
    );

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("insufficient paper top-of-book depth"),
        "{stderr}"
    );
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

#[test]
fn arbitrage_rejects_negative_depth_at_the_domain_boundary() {
    let history = temp_path("arbitrage-negative-depth-history", "jsonl");
    let root = repo_root();
    let output = paper_arbitrage_command(
        &root.join("config/arbitrage/paper-once-eth.yaml"),
        &root.join("config/arbitrage/paper-monitor-eth.yaml"),
        &history,
        "-1",
        "1",
    );

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("must not be negative"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected history at {}",
        history.display()
    );
}

fn arbitrage_command(
    config: &Path,
    monitor: &Path,
    history: &Path,
    left_ask_quantity: &str,
    right_bid_quantity: &str,
) -> std::process::Output {
    Command::new(binary())
        .current_dir(repo_root())
        .arg("arbitrage")
        .arg("--config")
        .arg(config)
        .arg("--monitor-config")
        .arg(monitor)
        .args([
            "--once",
            "--left-exchange",
            "edgex",
            "--left-symbol",
            "ETH-USDC-PERP",
            "--left-bid",
            "99.9",
            "--left-ask",
            "100",
            "--left-bid-quantity",
            "1",
            "--left-ask-quantity",
            left_ask_quantity,
            "--right-exchange",
            "lighter",
            "--right-symbol",
            "ETH-USDC-PERP",
            "--right-bid",
            "101",
            "--right-ask",
            "101.1",
            "--right-bid-quantity",
            right_bid_quantity,
            "--right-ask-quantity",
            "1",
            "--history-path",
        ])
        .arg(history)
        .output()
        .unwrap()
}

fn paper_arbitrage_command(
    config: &Path,
    monitor: &Path,
    history: &Path,
    left_ask_quantity: &str,
    right_bid_quantity: &str,
) -> std::process::Output {
    Command::new(binary())
        .current_dir(repo_root())
        .arg("arbitrage")
        .arg("--config")
        .arg(config)
        .arg("--monitor-config")
        .arg(monitor)
        .args([
            "--once",
            "--left-exchange",
            "paper-left",
            "--left-symbol",
            "ETH-USDC-PERP",
            "--left-bid",
            "99.9",
            "--left-ask",
            "100",
            "--left-bid-quantity",
            "1",
            "--left-ask-quantity",
            left_ask_quantity,
            "--right-exchange",
            "paper-right",
            "--right-symbol",
            "ETH-USDC-PERP",
            "--right-bid",
            "101",
            "--right-ask",
            "101.1",
            "--right-bid-quantity",
            right_bid_quantity,
            "--right-ask-quantity",
            "1",
            "--history-path",
        ])
        .arg(history)
        .output()
        .unwrap()
}

#[test]
fn arbitrage_rejects_disabled_and_unconfigured_strategy_symbols() {
    for symbol in ["PAXG-USD-PERP", "TOTALLY-UNCONFIGURED"] {
        let history = temp_path("arbitrage-rejected-symbol", "jsonl");
        let output = Command::new(binary())
            .current_dir(repo_root())
            .args([
                "arbitrage",
                "--once",
                "--left-exchange",
                "edgex",
                "--left-symbol",
                symbol,
                "--left-bid",
                "99.9",
                "--left-ask",
                "100",
                "--left-bid-quantity",
                "1",
                "--left-ask-quantity",
                "1",
                "--right-exchange",
                "lighter",
                "--right-symbol",
                symbol,
                "--right-bid",
                "101",
                "--right-ask",
                "101.1",
                "--right-bid-quantity",
                "1",
                "--right-ask-quantity",
                "1",
                "--history-path",
            ])
            .arg(&history)
            .output()
            .unwrap();

        assert!(!output.status.success(), "{symbol}: {output:?}");
        assert!(
            !history.exists(),
            "unexpected history at {}",
            history.display()
        );
    }
}

#[test]
fn arbitrage_rejects_exchanges_outside_the_monitor_allowlist() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "arbitrage",
            "--config",
            "config/arbitrage/paper-once-eth.yaml",
            "--monitor-config",
            "config/arbitrage/paper-monitor-eth.yaml",
            "--once",
            "--left-exchange",
            "not-configured",
            "--left-symbol",
            "ETH-USDC-PERP",
            "--left-bid",
            "99.9",
            "--left-ask",
            "100",
            "--left-bid-quantity",
            "1",
            "--left-ask-quantity",
            "1",
            "--right-exchange",
            "paper-right",
            "--right-symbol",
            "ETH-USDC-PERP",
            "--right-bid",
            "101",
            "--right-ask",
            "101.1",
            "--right-bid-quantity",
            "1",
            "--right-ask-quantity",
            "1",
            "--history-path",
            "NUL",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("monitor exchange allowlist"), "{stderr}");
}

#[test]
fn arbitrage_requires_strategy_key_when_leg_symbols_differ() {
    let history = temp_path("arbitrage-different-legs-history", "jsonl");
    let arbitrage = write_temp(
        "arbitrage-different-legs",
        "yaml",
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [AAA-PERP, BBB-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.1
    grid_step: 0.1
    max_segments: 2
  quantity_config:
    base_quantity: 1
  risk_config:
    max_position_value: 1000
symbol_configs:
  CROSS_PAIR:
    enabled: true
",
    );
    let monitor = write_temp(
        "monitor-different-legs",
        "yaml",
        "exchanges: [left, right]\nsymbols: [AAA-PERP, BBB-PERP]\nhealth_check:\n  data_timeout: 30\n",
    );
    let base_args = [
        "arbitrage",
        "--once",
        "--left-exchange",
        "left",
        "--left-symbol",
        "AAA-PERP",
        "--left-bid",
        "99.9",
        "--left-ask",
        "100",
        "--left-bid-quantity",
        "10",
        "--left-ask-quantity",
        "10",
        "--right-exchange",
        "right",
        "--right-symbol",
        "BBB-PERP",
        "--right-bid",
        "101",
        "--right-ask",
        "101.1",
        "--right-bid-quantity",
        "10",
        "--right-ask-quantity",
        "10",
    ];
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg(base_args[0])
        .arg("--config")
        .arg(&arbitrage)
        .arg("--monitor-config")
        .arg(&monitor)
        .arg("--history-path")
        .arg(&history)
        .args(&base_args[1..])
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--strategy-key"), "{stderr}");

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg(base_args[0])
        .arg("--config")
        .arg(&arbitrage)
        .arg("--monitor-config")
        .arg(&monitor)
        .arg("--strategy-key")
        .arg("CROSS_PAIR")
        .arg("--history-path")
        .arg(&history)
        .args(&base_args[1..])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    std::fs::remove_file(arbitrage).unwrap();
    std::fs::remove_file(monitor).unwrap();
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
fn config_check_classifies_strict_paper_profiles_explicitly() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "config-check",
            "config/grid/paper-once-btc.yaml",
            "config/arbitrage/paper-once-eth.yaml",
            "config/arbitrage/paper-monitor-eth.yaml",
            "--json",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let summaries: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let summaries = summaries.as_array().unwrap();
    let classification = |kind: &str| {
        summaries
            .iter()
            .find(|summary| summary["kind"] == kind)
            .unwrap()["classification"]
            .as_str()
            .unwrap()
    };
    assert_eq!(classification("grid"), "runtime-executable");
    assert_eq!(classification("arbitrage"), "runtime-executable");
    assert_eq!(classification("monitor"), "auxiliary");
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
    assert_eq!(
        stdout.matches("legacy-parseable: exchange-auth").count(),
        paths.len()
    );
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
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("unsupported"), "{stdout}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("configuration check failed"), "{stderr}");
}

#[test]
fn config_check_json_reports_every_path_before_returning_nonzero() {
    let grid = write_temp(
        "config-check-grid",
        "yaml",
        r"
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
",
    );
    let unknown = write_temp("config-check-unknown", "yaml", "unknown: true\n");
    let auxiliary = write_temp("logging", "yaml", "logging:\n  level: INFO\n");

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&grid)
        .arg(&unknown)
        .arg(&auxiliary)
        .arg("--json")
        .output()
        .unwrap();

    std::fs::remove_file(grid).unwrap();
    std::fs::remove_file(unknown).unwrap();
    std::fs::remove_file(auxiliary).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let summaries: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let summaries = summaries.as_array().unwrap();
    assert_eq!(summaries.len(), 3, "{summaries:?}");
    assert_eq!(summaries[0]["classification"], "runtime-executable");
    assert_eq!(summaries[1]["classification"], "unsupported");
    assert_eq!(summaries[2]["classification"], "auxiliary");
}

#[test]
fn config_check_recursively_expands_directory_arguments() {
    let directory = temp_path("config-check-directory", "dir");
    let nested = directory.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(
        directory.join("01-grid.yaml"),
        r"
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
",
    )
    .unwrap();
    std::fs::write(nested.join("02-unknown.yaml"), "unknown: true\n").unwrap();
    std::fs::write(directory.join("README.md"), "not a config\n").unwrap();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&directory)
        .arg("--json")
        .output()
        .unwrap();

    std::fs::remove_dir_all(directory).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let summaries: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let summaries = summaries.as_array().unwrap();
    assert_eq!(summaries.len(), 2, "{summaries:?}");
    assert_eq!(summaries[0]["kind"], "grid");
    assert_eq!(summaries[1]["classification"], "unsupported");
}

#[test]
fn config_check_rejects_a_directory_without_supported_config_files() {
    let directory = temp_path("config-check-empty-directory", "dir");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("README.md"), "not a config\n").unwrap();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("config-check")
        .arg(&directory)
        .arg("--json")
        .output()
        .unwrap();

    std::fs::remove_dir_all(directory).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let summaries: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let summaries = summaries.as_array().unwrap();
    assert_eq!(summaries.len(), 1, "{summaries:?}");
    assert_eq!(summaries[0]["classification"], "unsupported");
    assert!(
        summaries[0]["error"]
            .as_str()
            .unwrap()
            .contains("no supported configuration files")
    );
}

fn assert_history_phases(path: &Path, receipt_count: usize) {
    let body = std::fs::read_to_string(path).unwrap();
    let records = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2, "{body}");
    assert_eq!(records[0]["decision"], "execution_planned");
    assert_eq!(records[1]["decision"], "execution_completed");
    assert_eq!(
        records[0]["details"]["batch_id"], records[1]["details"]["batch_id"],
        "{body}"
    );
    assert_eq!(
        records[0]["details"]["legs"].as_array().unwrap().len(),
        receipt_count,
        "{body}"
    );
    let recovered: crypto_trading_runtime::ExecutionBatch =
        serde_json::from_value(records[0]["details"]["recovery_batch"].clone()).unwrap();
    assert_eq!(
        recovered.id().to_string(),
        records[0]["details"]["batch_id"].as_str().unwrap()
    );
    assert_eq!(recovered.intents().len(), receipt_count);
    assert_ne!(
        recovered.id().to_string(),
        "00000000-0000-0000-0000-000000000000"
    );
    for (intent, leg) in recovered
        .intents()
        .iter()
        .zip(records[0]["details"]["legs"].as_array().unwrap())
    {
        assert_eq!(intent.client_order_id.to_string(), leg["client_order_id"]);
    }
    assert_eq!(
        records[1]["details"]["receipt_count"].as_u64(),
        Some(u64::try_from(receipt_count).unwrap())
    );
}

fn assert_arbitrage_receipts_filled(path: &Path, receipt_count: usize) {
    let body = std::fs::read_to_string(path).unwrap();
    let completed =
        serde_json::from_str::<serde_json::Value>(body.lines().last().unwrap()).unwrap();
    assert_eq!(
        completed["details"]["filled"].as_u64(),
        Some(u64::try_from(receipt_count).unwrap()),
        "{body}"
    );
    assert_eq!(completed["details"]["open"].as_u64(), Some(0), "{body}");
    assert_eq!(
        completed["details"]["cancelled"].as_u64(),
        Some(0),
        "{body}"
    );
}

fn assert_grid_orders_are_resting(path: &Path, receipt_count: usize) {
    let body = std::fs::read_to_string(path).unwrap();
    let completed =
        serde_json::from_str::<serde_json::Value>(body.lines().last().unwrap()).unwrap();
    assert_eq!(
        completed["details"]["open"].as_u64(),
        Some(u64::try_from(receipt_count).unwrap()),
        "{body}"
    );
    assert_eq!(completed["details"]["filled"].as_u64(), Some(0), "{body}");
    assert_eq!(
        completed["details"]["cancelled"].as_u64(),
        Some(0),
        "{body}"
    );
}
