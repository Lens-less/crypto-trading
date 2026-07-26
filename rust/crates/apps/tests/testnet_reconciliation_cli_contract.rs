use std::{
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    JsonlHistory, PaperAccountAuthority, PaperAccountConfig, PaperCostModel, PaperReservationLeg,
    PaperReservationRequest,
};
use rust_decimal::Decimal;
use uuid::Uuid;

const JOURNAL_ID: &str = "85ad0b40-5930-4ac8-9857-f3d2ec679394";
const RESERVATION_ID: &str = "5252fd91-cd35-4bff-9cfa-fe8634c38cc3";
const BATCH_ID: &str = "aa2ce047-b50a-48b4-b5b8-b68c1a78d5fb";

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
        "crypto-trading-testnet-reconcile-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn base_args(history: &Path) -> Vec<String> {
    vec![
        "testnet-reconcile".to_owned(),
        "--history-path".to_owned(),
        history.display().to_string(),
        "--journal-id".to_owned(),
        JOURNAL_ID.to_owned(),
        "--account-id".to_owned(),
        "paper-main".to_owned(),
        "--initial-available".to_owned(),
        "1000".to_owned(),
        "--reservation-id".to_owned(),
        RESERVATION_ID.to_owned(),
    ]
}

async fn committed_history(path: &Path) {
    let journal_id = Uuid::parse_str(JOURNAL_ID).unwrap();
    let reservation_id = Uuid::parse_str(RESERVATION_ID).unwrap();
    let batch_id = Uuid::parse_str(BATCH_ID).unwrap();
    let authority = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(path),
        PaperAccountConfig::new("paper-main", Money::new(Decimal::from(1000))).unwrap(),
    )
    .unwrap();
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(Decimal::new(1, 3)).unwrap(),
    );
    authority
        .reserve(
            PaperReservationRequest::new(
                reservation_id,
                "grid-btc",
                "grid-btc-001",
                batch_id,
                PaperCostModel::v1(0, 0, 0).unwrap(),
                vec![
                    PaperReservationLeg::from_intent(
                        0,
                        &intent,
                        Money::new(Decimal::from_str("100").unwrap()),
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    authority
        .commit(reservation_id, Money::new(Decimal::from(100)))
        .await
        .unwrap();
}

#[test]
fn apply_acknowledgement_is_checked_before_credentials_or_journal_access() {
    let history = temp_history("ack");
    let mut args = base_args(&history);
    args.extend([
        "--apply-reconciliation".to_owned(),
        "I UNDERSTAND".to_owned(),
    ]);
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(args)
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("I APPLY VERIFIED BINANCE TESTNET RECONCILIATION"),
        "{stderr}"
    );
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[tokio::test]
async fn missing_credentials_leave_the_committed_paper_journal_unchanged() {
    let history = temp_history("credentials");
    committed_history(&history).await;
    let before = std::fs::read(&history).unwrap();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .args(base_args(&history))
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(!stderr.contains("fixture-secret"), "{stderr}");
    assert_eq!(std::fs::read(&history).unwrap(), before);
}
