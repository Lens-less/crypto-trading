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
    PaperAccountError, PaperCostModel, PaperReconciliationDigestAlgorithm,
    PaperReconciliationEvidence, PaperReconciliationOutcome, PaperReconciliationProof,
    PaperReservationAdmission, PaperReservationLeg, PaperReservationPhase, PaperReservationRequest,
    ProjectionStatus,
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

fn reconciliation_match_proof(
    account_id: &str,
    reservation_id: Uuid,
    batch_id: Uuid,
    snapshot_id: &str,
    snapshot_sequence: u64,
    digest: &str,
    expected_available: Money,
) -> PaperReconciliationProof {
    PaperReconciliationProof::from_evidence(
        PaperReconciliationEvidence::clean_match(
            "contract-fixture",
            digest,
            account_id,
            reservation_id,
            batch_id,
            snapshot_id,
            snapshot_sequence,
            expected_available,
        )
        .unwrap(),
    )
    .unwrap()
}

fn reconciliation_mismatch_proof(
    account_id: &str,
    reservation_id: Uuid,
    batch_id: Uuid,
    snapshot_id: &str,
    snapshot_sequence: u64,
    digest: &str,
    expected_available: Money,
) -> PaperReconciliationProof {
    PaperReconciliationProof::from_evidence(
        PaperReconciliationEvidence::mismatch(
            "contract-fixture",
            digest,
            account_id,
            reservation_id,
            batch_id,
            snapshot_id,
            snapshot_sequence,
            expected_available,
            "fixture_mismatch",
        )
        .unwrap(),
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
async fn uncertain_commit_reconcile_and_safe_release_transitions_survive_restart() {
    let path = temp_path("restart-transitions");
    let journal_id = Uuid::new_v4();
    let config = PaperAccountConfig::new("paper-main", money("1000")).unwrap();
    let authority =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config.clone()).unwrap();
    let request = reservation_request("arb:btc", "open:0001", Uuid::new_v4(), Uuid::new_v4());
    let reservation_id = request.reservation_id();

    authority.reserve(request.clone()).await.unwrap();
    let uncertain = authority.mark_uncertain(reservation_id).await.unwrap();
    assert_eq!(uncertain.phase, PaperReservationPhase::Uncertain);

    let restarted =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config.clone()).unwrap();
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
    assert!(
        still_committed.reservations[0].reconciliation.is_none(),
        "reason-only release must not invent reconcile evidence"
    );

    let failed = restarted
        .record_reconciliation_failure(reconciliation_mismatch_proof(
            "paper-main",
            reservation_id,
            request.batch_id(),
            "binance/account-2026-07-25T00:00:01Z",
            41,
            "0123456789abcdef",
            money("1000"),
        ))
        .await
        .unwrap();
    assert_eq!(failed.phase, PaperReservationPhase::Committed);
    assert_eq!(failed.held_exposure, money("150"));
    let failed_record = failed.reconciliation.as_ref().unwrap();
    assert_eq!(failed_record.outcome, PaperReconciliationOutcome::Failed);
    assert_eq!(failed_record.proof.snapshot_sequence(), 41);

    let restarted_again =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config).unwrap();
    let persisted_failed = restarted_again.snapshot().await.unwrap();
    assert_eq!(persisted_failed.available, money("850"));
    assert_eq!(persisted_failed.committed_exposure, money("150"));
    assert_eq!(
        persisted_failed.reservations[0]
            .reconciliation
            .as_ref()
            .unwrap()
            .outcome,
        PaperReconciliationOutcome::Failed
    );

    let released = restarted_again
        .reconcile_release(reconciliation_match_proof(
            "paper-main",
            reservation_id,
            request.batch_id(),
            "binance/account-2026-07-25T00:00:01Z",
            42,
            "fedcba9876543210",
            money("1000"),
        ))
        .await
        .unwrap();
    assert_eq!(released.phase, PaperReservationPhase::Released);
    assert_eq!(released.held_exposure, Money::default());
    assert_eq!(
        released.reconciliation.as_ref().unwrap().outcome,
        PaperReconciliationOutcome::Released
    );
    let released_snapshot = restarted_again.snapshot().await.unwrap();
    assert_eq!(released_snapshot.available, money("1000"));
    assert_eq!(released_snapshot.committed_exposure, Money::default());

    let records = std::fs::read_to_string(&path).unwrap();
    assert!(records.contains("\"decision\":\"paper_account_reconcile_failed\""));
    assert!(records.contains("\"decision\":\"paper_account_released\""));
    assert!(records.contains("\"snapshot_sequence\":42"));
    assert!(records.contains("\"source_state_digest\":\"fedcba9876543210\""));
}

#[tokio::test]
async fn uncertain_reservation_still_accepts_reason_based_release() {
    let path = temp_path("uncertain-reason-release");
    let authority = PaperAccountAuthority::new(
        Uuid::new_v4(),
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let request = reservation_request("arb:eth", "open:0002", Uuid::new_v4(), Uuid::new_v4());
    let reservation_id = request.reservation_id();

    authority.reserve(request).await.unwrap();
    authority.mark_uncertain(reservation_id).await.unwrap();
    let released = authority
        .release(reservation_id, "confirmed_no_submission")
        .await
        .unwrap();

    assert_eq!(released.phase, PaperReservationPhase::Released);
    let snapshot = authority.snapshot().await.unwrap();
    assert_eq!(snapshot.available, money("1000"));
    assert_eq!(snapshot.committed_exposure, Money::default());
}

#[tokio::test]
async fn committed_reconciliation_requires_bound_non_conflicting_proof() {
    let path = temp_path("reconcile-guards");
    let authority = PaperAccountAuthority::new(
        Uuid::new_v4(),
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let request = reservation_request("arb:btc", "open:guard", Uuid::new_v4(), Uuid::new_v4());
    let reservation_id = request.reservation_id();
    let batch_id = request.batch_id();

    authority.reserve(request).await.unwrap();
    authority
        .commit(reservation_id, money("150"))
        .await
        .unwrap();

    let wrong_account = authority
        .reconcile_release(reconciliation_match_proof(
            "paper-alt",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:10:00Z",
            1,
            "0011223344556677",
            money("1000"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        wrong_account,
        PaperAccountError::InvalidTransition
    ));

    authority
        .record_reconciliation_failure(reconciliation_mismatch_proof(
            "paper-main",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:10:00Z",
            5,
            "0011223344556677",
            money("1000"),
        ))
        .await
        .unwrap();
    let conflicting = authority
        .reconcile_release(reconciliation_match_proof(
            "paper-main",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:10:00Z",
            5,
            "8899aabbccddeeff",
            money("1000"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(conflicting, PaperAccountError::InvalidTransition));

    let lower_different_snapshot = authority
        .reconcile_release(reconciliation_match_proof(
            "paper-main",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:09:59Z",
            4,
            "0011223344556677",
            money("1000"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        lower_different_snapshot,
        PaperAccountError::InvalidTransition
    ));

    let equal_different_snapshot = authority
        .record_reconciliation_failure(reconciliation_mismatch_proof(
            "paper-main",
            reservation_id,
            batch_id,
            "binance/account-2026-07-25T00:10:01Z",
            5,
            "0011223344556677",
            money("1000"),
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        equal_different_snapshot,
        PaperAccountError::InvalidTransition
    ));
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 3);

    let malformed = PaperReconciliationProof::new(
        "paper-main",
        reservation_id,
        batch_id,
        "binance/account-2026-07-25T00:10:00Z",
        6,
        PaperReconciliationDigestAlgorithm::Fnv1a64,
        "not-hex",
    )
    .unwrap_err();
    assert!(matches!(malformed, PaperAccountError::InvalidRequest(_)));
}

#[tokio::test]
async fn opaque_digest_cannot_release_committed_exposure() {
    let path = temp_path("opaque-reconcile-proof");
    let authority = PaperAccountAuthority::new(
        Uuid::new_v4(),
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let request = reservation_request("arb:btc", "open:opaque", Uuid::new_v4(), Uuid::new_v4());
    let reservation_id = request.reservation_id();
    let batch_id = request.batch_id();
    authority.reserve(request).await.unwrap();
    authority
        .commit(reservation_id, money("150"))
        .await
        .unwrap();

    let error = authority
        .reconcile_release(
            PaperReconciliationProof::new(
                "paper-main",
                reservation_id,
                batch_id,
                "binance/account-2026-07-25T00:09:00Z",
                1,
                PaperReconciliationDigestAlgorithm::Fnv1a64,
                "0011223344556677",
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PaperAccountError::InvalidTransition));
    let snapshot = authority.snapshot().await.unwrap();
    assert_eq!(snapshot.available, money("850"));
    assert_eq!(snapshot.committed_exposure, money("150"));
    assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 2);
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

#[tokio::test]
async fn sealed_chain_without_active_file_replays_the_reserved_balance() {
    let path = temp_path("sealed-chain");
    let journal_id = Uuid::new_v4();
    let config = PaperAccountConfig::new("paper-main", money("1000")).unwrap();
    let authority =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config.clone()).unwrap();
    let admission = authority
        .reserve(reservation_request(
            "arb:btc",
            "open:0001",
            Uuid::new_v4(),
            Uuid::new_v4(),
        ))
        .await
        .unwrap();
    let PaperReservationAdmission::Reserved(reservation) = admission else {
        panic!("the reservation must be admitted against the fresh journal");
    };

    // Crash point between sealing the active file and recreating it: the
    // sealed chain `<path>.1` alone is the complete durable record and must
    // replay every reservation fact, not silently reset the balance to
    // `initial_available`.
    let sealed = {
        let mut sealed = path.clone().into_os_string();
        sealed.push(".1");
        std::path::PathBuf::from(sealed)
    };
    std::fs::rename(&path, &sealed).unwrap();

    let restarted =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(&path), config.clone()).unwrap();
    let snapshot = restarted.snapshot().await.unwrap();
    assert_eq!(snapshot.projection_status, ProjectionStatus::Complete);
    assert_eq!(snapshot.available, money("799.40"));
    assert_eq!(snapshot.pending_reserved, money("200.60"));
    assert_eq!(snapshot.reservations.len(), 1);

    // Transitions on the pre-crash reservation keep working: the append
    // recreates the active file behind the sealed segment.
    let uncertain = restarted
        .mark_uncertain(reservation.reservation_id)
        .await
        .unwrap();
    assert_eq!(uncertain.phase, PaperReservationPhase::Uncertain);

    // A journal with neither an active file nor sealed segments still loads
    // as a fresh empty account.
    let fresh = PaperAccountAuthority::new(
        Uuid::new_v4(),
        JsonlHistory::new(temp_path("sealed-chain-fresh")),
        config,
    )
    .unwrap();
    let snapshot = fresh.snapshot().await.unwrap();
    assert_eq!(snapshot.available, money("1000"));
    assert!(snapshot.reservations.is_empty());
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "crypto-trading-paper-account-{label}-{}.jsonl",
        Uuid::new_v4()
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_spellings_of_one_journal_share_a_single_capacity_authority() {
    // The authority serializes the read-modify-write of available capacity.
    // If it keyed on the raw path while the journal writer keyed on the
    // normalized one, two spellings of the same file would each get their own
    // authority and could admit the same capacity twice. The alias here is a
    // lexical one (a redundant `.` component) because it names the same file
    // on every filesystem; case-different spellings only alias on Windows and
    // are covered by the platform-specific test below.
    let directory =
        std::env::temp_dir().join(format!("crypto-trading-lock-key-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let direct = directory.join("paper-account.jsonl");
    let lexical_alias = directory.join(".").join("paper-account.jsonl");
    assert_two_spellings_share_capacity(&directory, &direct, &lexical_alias).await;
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn case_spellings_of_one_journal_share_a_single_capacity_authority() {
    // Case-different spellings name the same file only on case-insensitive
    // filesystems; on Linux they are genuinely distinct journals, so this
    // aliasing contract is Windows-specific.
    let directory =
        std::env::temp_dir().join(format!("crypto-trading-lock-case-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let lowercase = directory.join("paper-account.jsonl");
    let mixed_case = directory.join("Paper-Account.jsonl");
    assert_two_spellings_share_capacity(&directory, &lowercase, &mixed_case).await;
}

async fn assert_two_spellings_share_capacity(
    directory: &std::path::Path,
    first_spelling: &std::path::Path,
    second_spelling: &std::path::Path,
) {
    let journal_id = Uuid::new_v4();
    let config = PaperAccountConfig::new("paper-main", money("300")).unwrap();
    let first = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(first_spelling),
        config.clone(),
    )
    .unwrap();
    let second =
        PaperAccountAuthority::new(journal_id, JsonlHistory::new(second_spelling), config).unwrap();

    // Each reservation costs 200.60, so exactly one of the two fits in 300.
    // Issuing them concurrently is what distinguishes a shared authority from
    // two independent ones: without a common lock both can read the starting
    // capacity before either appends, and both admit.
    let (left, right) = tokio::join!(
        first.reserve(reservation_request(
            "arb:btc",
            "open:0001",
            Uuid::new_v4(),
            Uuid::new_v4(),
        )),
        second.reserve(reservation_request(
            "arb:eth",
            "open:0002",
            Uuid::new_v4(),
            Uuid::new_v4(),
        )),
    );

    let admitted = [&left, &right]
        .into_iter()
        .filter(|outcome| matches!(outcome, Ok(PaperReservationAdmission::Reserved(_))))
        .count();
    assert_eq!(
        admitted, 1,
        "exactly one concurrent reservation may consume the shared capacity; \
         left={left:?} right={right:?}"
    );

    std::fs::remove_dir_all(directory).ok();
}
