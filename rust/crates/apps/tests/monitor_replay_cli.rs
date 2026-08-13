use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

#[test]
fn monitor_replay_emits_multiple_read_only_events_and_a_bounded_journal() {
    let history = temp_path("monitor-replay-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "monitor",
            "--config",
            "config/arbitrage/paper-monitor-eth.yaml",
            "--replay",
            "fixtures/m3-monitor-replay.jsonl",
            "--history-path",
            history.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("read-only monitor replay"), "{stdout}");
    assert!(stdout.contains("events=4"), "{stdout}");
    assert!(stdout.contains("opportunities=2"), "{stdout}");

    let journal = std::fs::read_to_string(&history).unwrap();
    let records = journal.lines().collect::<Vec<_>>();
    assert_eq!(records.len(), 4);
    assert!(
        records
            .iter()
            .all(|record| record.contains("arbitrage_monitor"))
    );
    assert!(!journal.contains("\"intents\""));
    assert!(!journal.contains("\"orders\""));

    std::fs::remove_file(history).unwrap();
}

#[test]
fn replay_rejects_unknown_fields_without_touching_history() {
    let replay = temp_path("monitor-replay-unknown-field", "jsonl");
    let history = temp_path("monitor-replay-unknown-field-history", "jsonl");
    std::fs::write(
        &replay,
        r#"{"exchange":"paper-left","symbol":"ETH-USDC-PERP","market_type":"perpetual","bid":"99","ask":"100","timestamp":"2026-07-24T00:00:00Z","api_key":"must-not-be-accepted"}
"#,
    )
    .unwrap();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "monitor",
            "--config",
            "config/arbitrage/paper-monitor-eth.yaml",
            "--replay",
            replay.to_str().unwrap(),
            "--history-path",
            history.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unknown replay field"), "{stderr}");
    assert!(!history.exists());

    std::fs::remove_file(replay).unwrap();
}

#[test]
fn replay_rejects_multi_exchange_configs_instead_of_silently_truncating_scope() {
    let config = temp_path("monitor-replay-multi-exchange", "yaml");
    let history = temp_path("monitor-replay-multi-exchange-history", "jsonl");
    std::fs::write(
        &config,
        "exchanges: [paper-left, paper-right, paper-third]\nsymbols: [BTC-USDC-PERP]\nthresholds:\n  min_spread_pct: 0.05\nhealth_check:\n  data_timeout: 30\n  max_pair_skew_ms: 1000\n",
    )
    .unwrap();
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "monitor",
            "--config",
            config.to_str().unwrap(),
            "--replay",
            "fixtures/m3-monitor-replay.jsonl",
            "--history-path",
            history.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("requires exactly two configured exchanges"),
        "{stderr}"
    );
    assert!(!history.exists());

    std::fs::remove_file(config).unwrap();
}

#[test]
fn configured_pair_skew_changes_replay_monitor_outcome() {
    let replay = temp_path("monitor-pair-skew-replay", "jsonl");
    std::fs::write(
        &replay,
        concat!(
            r#"{"exchange":"paper-left","symbol":"ETH-USDC-PERP","market_type":"perpetual","bid":"99","ask":"100","timestamp":"2026-07-24T00:00:00.000Z"}"#,
            "\n",
            r#"{"exchange":"paper-right","symbol":"ETH-USDC-PERP","market_type":"perpetual","bid":"102","ask":"103","timestamp":"2026-07-24T00:00:00.300Z"}"#,
            "\n",
        ),
    )
    .unwrap();

    for (tolerance_ms, expected_decision) in [
        (250_u64, "monitor_waiting"),
        (500_u64, "monitor_opportunity"),
    ] {
        let config = temp_path(&format!("monitor-pair-skew-{tolerance_ms}"), "yaml");
        let history = temp_path(
            &format!("monitor-pair-skew-{tolerance_ms}-history"),
            "jsonl",
        );
        std::fs::write(
            &config,
            format!(
                "exchanges: [paper-left, paper-right]\nsymbols: [ETH-USDC-PERP]\nthresholds:\n  min_spread_pct: 0\nhealth_check:\n  data_timeout: 30\n  max_pair_skew_ms: {tolerance_ms}\n"
            ),
        )
        .unwrap();

        let output = Command::new(binary())
            .current_dir(repo_root())
            .args([
                "monitor",
                "--config",
                config.to_str().unwrap(),
                "--replay",
                replay.to_str().unwrap(),
                "--history-path",
                history.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let journal = std::fs::read_to_string(&history).unwrap();
        let last: serde_json::Value =
            serde_json::from_str(journal.lines().last().unwrap()).unwrap();
        assert_eq!(last["decision"], expected_decision, "{journal}");

        std::fs::remove_file(config).unwrap();
        std::fs::remove_file(history).unwrap();
    }
    std::fs::remove_file(replay).unwrap();
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
