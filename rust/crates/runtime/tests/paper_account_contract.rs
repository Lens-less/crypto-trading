use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    JsonlHistory, PAPER_COST_MODEL_VERSION, PaperAccountAuthority, PaperAccountConfig,
    PaperAccountError, PaperCostModel, PaperReservationAdmission, PaperReservationLeg,
    PaperReservationPhase, PaperReservationRequest, ProjectionStatus,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn money(value: &str) -> Money {
    Money::new(decimal(value))
}

fn intent(exchange: &str, side: Side) -> OrderIntent {
    OrderIntent::market(
        exchange,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        side,
        Quantity::new(decimal("1")).unwrap(),
    )
}

fn reservation_request(
    task_id: &str,
    idempotency_key: &str,
    reservation_id: Uuid,
    batch_id: Uuid,
) -> PaperReservationRequest {
    let left = intent("paper-left", Side::Buy);
    let right = intent("paper-right", Side::Sell);
    PaperReservationRequest::new(
        reservation_id,
        task_id,
        idempotency_key,
        batch_id,
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![
            PaperReservationLeg::from_intent(0, &left, money("100")).unwrap(),
            PaperReservationLeg::from_intent(1, &right, money("100")).unwrap(),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn reservation_is_durable_atomic_and_cost_buffered() {
    let path = temp_path("durable-reservation");
    let history = JsonlHistory::new(&path);
    let authority = PaperAccountAuthority::new(
        Uuid::new_v4(),
        history,
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let request = reservation_request("arb:btc", "open:0001", Uuid::new_v4(), Uuid::new_v4());

    let admission = authority.reserve(request.clone()).await.unwrap();
    let PaperReservationAdmission::Reserved(reservation) = admission else {
        panic!("first admission must append one durable reservation");
    };

    assert_eq!(reservation.phase, PaperReservationPhase::Pending);
    assert_eq!(reservation.reserved_exposure, money("200.60"));
    assert_eq!(
        reservation.cost_model,
        PaperCostModel::v1(10, 5, 15).unwrap()
    );

    let snapshot = authority.snapshot().await.unwrap();
    assert_eq!(snapshot.projection_status, ProjectionStatus::Complete);
    assert_eq!(snapshot.account_id, "paper-main");
    assert_eq!(snapshot.initial_available, money("1000"));
    assert_eq!(snapshot.available, money("799.40"));
    assert_eq!(snapshot.pending_reserved, money("200.60"));
    assert_eq!(snapshot.uncertain_reserved, Money::default());
    assert_eq!(snapshot.committed_exposure, Money::default());
    assert_eq!(snapshot.reservations, vec![reservation]);

    let records = std::fs::read_to_string(&path).unwrap();
    assert_eq!(records.lines().count(), 1);
    assert!(records.contains("\"decision\":\"paper_account_reserved\""));
    let record: serde_json::Value = serde_json::from_str(records.trim()).unwrap();
    assert_eq!(
        record["details"]["request"]["cost_model"]["version"],
        PAPER_COST_MODEL_VERSION
    );
    assert_eq!(
        record["details"]["journal_id"],
        authority.journal_id().to_string()
    );
    assert!(!records.contains("secret"));
}

#[tokio::test]
async fn same_request_is_idempotent_and_conflicting_identity_fails_closed() {
    let path = temp_path("idempotency");
    let authority = PaperAccountAuthority::new(
        Uuid::new_v4(),
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let request = reservation_request("arb:btc", "open:0001", Uuid::new_v4(), Uuid::new_v4());

    let first = authority.reserve(request.clone()).await.unwrap();
    let second = authority.reserve(request.clone()).await.unwrap();
    assert!(matches!(first, PaperReservationAdmission::Reserved(_)));
    assert!(matches!(second, PaperReservationAdmission::Existing(_)));
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);

    let conflict = reservation_request("arb:btc", "open:0001", Uuid::new_v4(), Uuid::new_v4());
    let error = authority.reserve(conflict).await.unwrap_err();
    assert!(matches!(error, PaperAccountError::IdempotencyConflict));
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);

    let same_key_other_task =
        reservation_request("arb:eth", "open:0001", Uuid::new_v4(), Uuid::new_v4());
    assert!(matches!(
        authority.reserve(same_key_other_task).await.unwrap(),
        PaperReservationAdmission::Reserved(_)
    ));
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 2);
}

#[tokio::test]
async fn account_scope_prevents_overcommit_and_parallel_double_admission() {
    let path = temp_path("overcommit");
    let authority = Arc::new(
        PaperAccountAuthority::new(
            Uuid::new_v4(),
            JsonlHistory::new(&path),
            PaperAccountConfig::new("paper-main", money("250")).unwrap(),
        )
        .unwrap(),
    );
    let request = reservation_request("arb:btc", "open:0001", Uuid::new_v4(), Uuid::new_v4());
    let executions = Arc::new(AtomicUsize::new(0));

    let left = {
        let authority = Arc::clone(&authority);
        let request = request.clone();
        let executions = Arc::clone(&executions);
        tokio::spawn(async move {
            let admission = authority.reserve(request).await.unwrap();
            if matches!(admission, PaperReservationAdmission::Reserved(_)) {
                executions.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    let right = {
        let authority = Arc::clone(&authority);
        let request = request.clone();
        let executions = Arc::clone(&executions);
        tokio::spawn(async move {
            let admission = authority.reserve(request).await.unwrap();
            if matches!(admission, PaperReservationAdmission::Reserved(_)) {
                executions.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    left.await.unwrap();
    right.await.unwrap();

    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);

    let second = reservation_request("arb:eth", "open:0002", Uuid::new_v4(), Uuid::new_v4());
    let error = authority.reserve(second).await.unwrap_err();
    assert!(matches!(
        error,
        PaperAccountError::InsufficientAvailable { .. }
    ));
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);
}

#[tokio::test]
async fn uncertain_commit_and_safe_release_transitions_survive_restart() {
    let path = temp_path("restart-transitions");
    let journal_id = Uuid::new_v4();
    let config = PaperAccountConfig::new("paper-main", money("1000")).unwrap();
    let authority =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config.clone()).unwrap();
    let request = reservation_request("arb:btc", "open:0001", Uuid::new_v4(), Uuid::new_v4());
    let reservation_id = request.reservation_id();

    authority.reserve(request).await.unwrap();
    let uncertain = authority.mark_uncertain(reservation_id).await.unwrap();
    assert_eq!(uncertain.phase, PaperReservationPhase::Uncertain);

    let restarted =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config).unwrap();
    let recovered = restarted.snapshot().await.unwrap();
    assert_eq!(recovered.uncertain_reserved, money("200.60"));
    assert_eq!(recovered.available, money("799.40"));

    let committed = restarted
        .commit(reservation_id, money("150"))
        .await
        .unwrap();
    assert_eq!(committed.phase, PaperReservationPhase::Committed);
    assert_eq!(committed.held_exposure, money("150"));
    let committed_snapshot = restarted.snapshot().await.unwrap();
    assert_eq!(committed_snapshot.available, money("850"));
    assert_eq!(committed_snapshot.committed_exposure, money("150"));

    let error = restarted
        .release(reservation_id, "reconciled_closed")
        .await
        .unwrap_err();
    assert!(matches!(error, PaperAccountError::InvalidTransition));
    let still_committed = restarted.snapshot().await.unwrap();
    assert_eq!(still_committed.available, money("850"));
    assert_eq!(still_committed.committed_exposure, money("150"));

    let releasable = reservation_request("arb:eth", "open:0002", Uuid::new_v4(), Uuid::new_v4());
    let releasable_id = releasable.reservation_id();
    restarted.reserve(releasable).await.unwrap();
    restarted.mark_uncertain(releasable_id).await.unwrap();
    let released = restarted
        .release(releasable_id, "confirmed_no_submission")
        .await
        .unwrap();
    assert_eq!(released.phase, PaperReservationPhase::Released);
    let released_snapshot = restarted.snapshot().await.unwrap();
    assert_eq!(released_snapshot.available, money("850"));
    assert_eq!(released_snapshot.committed_exposure, money("150"));
}

#[tokio::test]
async fn journal_generation_mismatch_degrades_and_closes_writes() {
    let path = temp_path("generation-mismatch");
    let original_id = Uuid::new_v4();
    let config = PaperAccountConfig::new("paper-main", money("1000")).unwrap();
    let original =
        PaperAccountAuthority::new(original_id, JsonlHistory::new(&path), config.clone()).unwrap();
    original
        .reserve(reservation_request(
            "arb:btc",
            "open:0001",
            Uuid::new_v4(),
            Uuid::new_v4(),
        ))
        .await
        .unwrap();

    let wrong_generation =
        PaperAccountAuthority::new(Uuid::new_v4(), JsonlHistory::new(&path), config).unwrap();
    let degraded = wrong_generation.snapshot().await.unwrap();
    assert_eq!(degraded.projection_status, ProjectionStatus::Degraded);
    assert_eq!(degraded.invalid_event_count, 1);
    assert!(degraded.reservations.is_empty());

    let error = wrong_generation
        .reserve(reservation_request(
            "arb:eth",
            "open:0002",
            Uuid::new_v4(),
            Uuid::new_v4(),
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, PaperAccountError::DurableStateDegraded));

    let recovered = original.snapshot().await.unwrap();
    assert_eq!(recovered.projection_status, ProjectionStatus::Complete);
    assert_eq!(recovered.reservations.len(), 1);
    assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 1);
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "crypto-trading-paper-account-{label}-{}.jsonl",
        Uuid::new_v4()
    ))
}
