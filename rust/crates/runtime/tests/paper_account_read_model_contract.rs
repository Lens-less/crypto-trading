use std::{io::Write, str::FromStr};

use chrono::Utc;
use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    DecisionRecord, FileJournalSnapshotSource, JournalSnapshotSource, JsonlHistory,
    PaperAccountAuthority, PaperAccountConfig, PaperAccountError, PaperAccountReadModel,
    PaperCostModel, PaperReconciliationEvidence, PaperReconciliationOutcome,
    PaperReconciliationProof, PaperReservationAdmission, PaperReservationLeg,
    PaperReservationRequest, ProjectionStatus,
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
    reservation_request_with_identity("arb:btc", "open:0001")
}

fn reservation_request_with_identity(
    task_id: &str,
    idempotency_key: &str,
) -> PaperReservationRequest {
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
        task_id,
        idempotency_key,
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![
            PaperReservationLeg::from_intent(0, &left, money("100")).unwrap(),
            PaperReservationLeg::from_intent(1, &right, money("100")).unwrap(),
        ],
    )
    .unwrap()
}

fn mismatch_proof(
    account_id: &str,
    reservation_id: Uuid,
    batch_id: Uuid,
    snapshot_id: &str,
    snapshot_sequence: u64,
    source_state_digest: &str,
) -> PaperReconciliationProof {
    PaperReconciliationProof::from_evidence(
        PaperReconciliationEvidence::mismatch(
            "contract-fixture",
            source_state_digest,
            account_id,
            reservation_id,
            batch_id,
            snapshot_id,
            snapshot_sequence,
            money("1000"),
            "fixture_mismatch",
        )
        .unwrap(),
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
    let PaperReservationAdmission::Reserved(_expected) = authority.reserve(request).await.unwrap()
    else {
        panic!("first reservation must be new");
    };
    authority
        .commit(reservation_id, money("150"))
        .await
        .unwrap();
    let committed = authority.snapshot().await.unwrap().reservations[0].clone();

    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_reconcile_failed".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": "paper-main",
                "reservation_id": reservation_id,
                "batch_id": batch_id,
                "confirmed_exposure": null,
                "reason": null,
                "proof": {
                    "account_id": "paper-main",
                    "reservation_id": reservation_id,
                    "batch_id": Uuid::new_v4(),
                    "snapshot_id": "binance/account-2026-07-25T00:20:00Z",
                    "snapshot_sequence": 9,
                    "digest_algorithm": "fnv1a64",
                    "digest": "0123456789abcdef"
                }
            }),
        })
        .await
        .unwrap();

    let degraded = authority.snapshot().await.unwrap();
    assert_eq!(degraded.projection_status, ProjectionStatus::Degraded);
    assert_eq!(degraded.invalid_event_count, 1);
    assert_eq!(degraded.reservations, vec![committed]);

    let error = authority
        .record_reconciliation_failure(
            crypto_trading_runtime::PaperReconciliationProof::new(
                "paper-main",
                reservation_id,
                batch_id,
                "binance/account-2026-07-25T00:20:01Z",
                10,
                crypto_trading_runtime::PaperReconciliationDigestAlgorithm::Fnv1a64,
                "fedcba9876543210",
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, PaperAccountError::DurableStateDegraded));
    assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 3);
}

#[tokio::test]
async fn partial_tail_is_visible_then_quarantined_before_the_next_account_write() {
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

    authority
        .reserve(reservation_request_with_identity("arb:eth", "open:0002"))
        .await
        .unwrap();

    let recovered = authority.snapshot().await.unwrap();
    assert_eq!(recovered.projection_status, ProjectionStatus::Complete);
    assert_eq!(recovered.reservations.len(), 2);
    assert!(std::fs::read(&path).unwrap().ends_with(b"\n"));
    let quarantines = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|candidate| {
            candidate
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(path.file_name().unwrap().to_str().unwrap())
                        && name.ends_with(".quarantine")
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(quarantines.len(), 1);
    assert_eq!(
        std::fs::read(&quarantines[0]).unwrap(),
        br#"{"timestamp":"incomplete""#
    );
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

#[tokio::test]
async fn reconciliation_failure_fixture_keeps_committed_exposure_visible_after_restart() {
    let journal_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let path = temp_path("reconciliation-failed-fixture");
    std::fs::write(
        &path,
        include_bytes!("../../../fixtures/m4-reconciliation-failed.jsonl"),
    )
    .unwrap();

    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let model = PaperAccountReadModel::from_legacy_snapshot(&source.snapshot().unwrap()).unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.invalid_event_count, 0);
    assert_eq!(model.accounts.len(), 1);
    let account = &model.accounts[0];
    assert_eq!(account.account_id, "paper-main");
    assert_eq!(account.available, money("850"));
    assert_eq!(account.committed_exposure, money("150"));
    assert_eq!(account.reservations.len(), 1);
    let reservation = &account.reservations[0];
    assert_eq!(
        reservation.phase,
        crypto_trading_runtime::PaperReservationPhase::Committed
    );
    let reconciliation = reservation.reconciliation.as_ref().unwrap();
    assert_eq!(reconciliation.outcome, PaperReconciliationOutcome::Failed);
    assert_eq!(
        reconciliation.proof.snapshot_id(),
        "binance/account-2026-07-25T00:00:02Z"
    );
    assert_eq!(reconciliation.proof.snapshot_sequence(), 42);

    let authority = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let snapshot = authority.snapshot().await.unwrap();
    assert_eq!(snapshot.available, money("850"));
    assert_eq!(snapshot.committed_exposure, money("150"));
    assert_eq!(
        snapshot.reservations[0]
            .reconciliation
            .as_ref()
            .unwrap()
            .outcome,
        PaperReconciliationOutcome::Failed
    );
}

#[tokio::test]
async fn replay_rejects_lower_sequence_from_different_snapshot_id() {
    let path = temp_path("replay-lower-sequence");
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

    authority.reserve(request).await.unwrap();
    authority
        .commit(reservation_id, money("150"))
        .await
        .unwrap();
    authority
        .record_reconciliation_failure(mismatch_proof(
            "paper-main",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:30:00Z",
            7,
            "0123456789abcdef",
        ))
        .await
        .unwrap();

    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_reconcile_failed".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": "paper-main",
                "reservation_id": reservation_id,
                "batch_id": batch_id,
                "confirmed_exposure": null,
                "reason": null,
                "proof": {
                    "account_id": "paper-main",
                    "reservation_id": reservation_id,
                    "batch_id": batch_id,
                    "snapshot_id": "binance/account-2026-07-25T00:29:59Z",
                    "snapshot_sequence": 6,
                    "digest_algorithm": "fnv1a64",
                    "digest": "fedcba9876543210"
                }
            }),
        })
        .await
        .unwrap();

    let degraded = authority.snapshot().await.unwrap();
    assert_eq!(degraded.projection_status, ProjectionStatus::Degraded);
    assert_eq!(degraded.invalid_event_count, 1);
    let reservation = &degraded.reservations[0];
    assert_eq!(
        reservation.phase,
        crypto_trading_runtime::PaperReservationPhase::Committed
    );
    let reconciliation = reservation.reconciliation.as_ref().unwrap();
    assert_eq!(reconciliation.outcome, PaperReconciliationOutcome::Failed);
    assert_eq!(
        reconciliation.proof.snapshot_id(),
        "binance/account-2026-07-25T00:30:00Z"
    );
    assert_eq!(reconciliation.proof.snapshot_sequence(), 7);
}

#[tokio::test]
async fn replay_rejects_equal_sequence_from_different_snapshot_id() {
    let path = temp_path("replay-equal-sequence");
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

    authority.reserve(request).await.unwrap();
    authority
        .commit(reservation_id, money("150"))
        .await
        .unwrap();
    authority
        .record_reconciliation_failure(mismatch_proof(
            "paper-main",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:31:00Z",
            8,
            "0123456789abcdef",
        ))
        .await
        .unwrap();

    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_reconcile_failed".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": "paper-main",
                "reservation_id": reservation_id,
                "batch_id": batch_id,
                "confirmed_exposure": null,
                "reason": null,
                "proof": {
                    "account_id": "paper-main",
                    "reservation_id": reservation_id,
                    "batch_id": batch_id,
                    "snapshot_id": "binance/account-2026-07-25T00:31:01Z",
                    "snapshot_sequence": 8,
                    "digest_algorithm": "fnv1a64",
                    "digest": "fedcba9876543210"
                }
            }),
        })
        .await
        .unwrap();

    let degraded = authority.snapshot().await.unwrap();
    assert_eq!(degraded.projection_status, ProjectionStatus::Degraded);
    assert_eq!(degraded.invalid_event_count, 1);
    let reservation = &degraded.reservations[0];
    assert_eq!(
        reservation.phase,
        crypto_trading_runtime::PaperReservationPhase::Committed
    );
    let reconciliation = reservation.reconciliation.as_ref().unwrap();
    assert_eq!(reconciliation.outcome, PaperReconciliationOutcome::Failed);
    assert_eq!(
        reconciliation.proof.snapshot_id(),
        "binance/account-2026-07-25T00:31:00Z"
    );
    assert_eq!(reconciliation.proof.snapshot_sequence(), 8);
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "crypto-trading-paper-account-read-{label}-{}.jsonl",
        Uuid::new_v4()
    ))
}
