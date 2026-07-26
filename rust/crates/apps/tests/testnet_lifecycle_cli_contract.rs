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

fn temp_history(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-testnet-lifecycle-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn base_args(history: &Path, acknowledgement: &str) -> Vec<String> {
    vec![
        "testnet-lifecycle".to_owned(),
        "--acknowledge-testnet-lifecycle".to_owned(),
        acknowledgement.to_owned(),
        "--campaign-id".to_owned(),
        "binance-spot-open-001".to_owned(),
        "--client-order-id".to_owned(),
        "0f3c807d-776f-4de4-85d0-93760a82dfcf".to_owned(),
        "--history-path".to_owned(),
        history.display().to_string(),
        "--side".to_owned(),
        "buy".to_owned(),
        "--quantity".to_owned(),
        "0.001".to_owned(),
        "--price".to_owned(),
        "49000.1".to_owned(),
    ]
}

#[test]
fn exact_testnet_acknowledgement_is_checked_before_credentials_or_journal_writes() {
    let history = temp_history("ack");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(base_args(&history, "I UNDERSTAND"))
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE"),
        "{stderr}"
    );
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn missing_testnet_credentials_fail_before_creating_a_campaign() {
    let history = temp_history("credentials");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(base_args(
            &history,
            "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE",
        ))
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(!stderr.contains("fixture-secret"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}
