use std::{io::Write, str::FromStr};

use chrono::Utc;
use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    DecisionRecord, FileJournalSnapshotSource, JournalSnapshotSource, JsonlHistory,
    PaperAccountAuthority, PaperAccountConfig, PaperAccountError, PaperAccountReadModel,
    PaperCostModel, PaperReservationAdmission, PaperReservationLeg, PaperReservationRequest,
    ProjectionStatus,
};
use rust_decimal::Decimal;
use serde_json::json;
use uuid::Uuid;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn money(value: &str) -> Money {
    Money::new(decimal(value))
}

fn reservation_request() -> PaperReservationRequest {
    let left = OrderIntent::market(
        "paper-left",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    let right = OrderIntent::market(
        "paper-right",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Sell,
        Quantity::new(decimal("1")).unwrap(),
    );
    PaperReservationRequest::new(
        Uuid::new_v4(),
        "arb:btc",
        "open:0001",
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![
            PaperReservationLeg::from_intent(0, &left, money("100")).unwrap(),
            PaperReservationLeg::from_intent(1, &right, money("100")).unwrap(),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn invalid_paper_fact_degrades_without_overwriting_last_valid_reservation() {
    let path = temp_path("invalid-fact");
    let journal_id = Uuid::new_v4();
    let history = JsonlHistory::new(&path);
    let authority = PaperAccountAuthority::new(
        journal_id,
        history.clone(),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let request = reservation_request();
    let reservation_id = request.reservation_id();
    let batch_id = request.batch_id();
    let PaperReservationAdmission::Reserved(expected) = authority.reserve(request).await.unwrap()
    else {
        panic!("first reservation must be new");
    };

    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_committed".to_owned(),
            details: json!({
                "schema_version": 1,
                "account_id": "paper-main",
                "reservation_id": reservation_id,
                "batch_id": batch_id,
                "confirmed_exposure": "150",
                "reason": null,
                "unexpected": true,
            }),
        })
        .await
        .unwrap();

    let degraded = authority.snapshot().await.unwrap();
    assert_eq!(degraded.projection_status, ProjectionStatus::Degraded);
    assert_eq!(degraded.invalid_event_count, 1);
    assert_eq!(degraded.reservations, vec![expected]);

    let error = authority
        .commit(reservation_id, money("150"))
        .await
        .unwrap_err();
    assert!(matches!(error, PaperAccountError::DurableStateDegraded));
    assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 2);
}

#[tokio::test]
async fn partial_tail_is_visible_and_blocks_all_new_account_writes() {
    let path = temp_path("partial-tail");
    let journal_id = Uuid::new_v4();
    let authority = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let first = reservation_request();
    authority.reserve(first).await.unwrap();

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(br#"{"timestamp":"incomplete""#).unwrap();
    file.sync_data().unwrap();

    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let model = PaperAccountReadModel::from_legacy_snapshot(&source.snapshot().unwrap()).unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.accounts.len(), 1);
    assert_eq!(model.accounts[0].reservations.len(), 1);

    let error = authority.reserve(reservation_request()).await.unwrap_err();
    assert!(matches!(error, PaperAccountError::DurableStateDegraded));
}

#[tokio::test]
async fn numeric_money_and_confusable_identity_are_rejected_by_projection() {
    let path = temp_path("strict-wire");
    let journal_id = Uuid::new_v4();
    let history = JsonlHistory::new(&path);
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "paper_account".to_owned(),
            symbol: "paper\u{202e}main".to_owned(),
            decision: "paper_account_reserved".to_owned(),
            details: json!({
                "schema_version": 1,
                "account_id": "paper\u{202e}main",
                "initial_available": 1000,
                "request": {
                    "reservation_id": Uuid::new_v4(),
                    "task_id": "arb:btc",
                    "idempotency_key": "open:0001",
                    "batch_id": Uuid::new_v4(),
                    "cost_model": {
                        "version": 1,
                        "fee_bps": 10,
                        "funding_buffer_bps": 5,
                        "slippage_bps": 15
                    },
                    "legs": []
                },
                "reserved_exposure": 1
            }),
        })
        .await
        .unwrap();

    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let model = PaperAccountReadModel::from_legacy_snapshot(&source.snapshot().unwrap()).unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert!(model.accounts.is_empty());
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "crypto-trading-paper-account-read-{label}-{}.jsonl",
        Uuid::new_v4()
    ))
}
