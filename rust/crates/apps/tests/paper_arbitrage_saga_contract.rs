use std::{
    future::pending,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use crypto_trading_cli::{
    DurablePaperArbitrageSaga, PaperArbitrageRecoveryStage, PaperArbitrageRequest,
    PaperArbitrageRun, PaperArbitrageSagaError,
};
use crypto_trading_domain::{
    MarketType, Money, Order, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    ExecutionBatch, FileJournalSnapshotSource, JournalSnapshotSource, JsonlHistory,
    OperatorReadModel, PaperAccountAuthority, PaperAccountConfig, PaperCostModel,
    PaperReservationLeg, PaperReservationPhase, PaperReservationRequest, RuntimeError,
};
use rust_decimal::Decimal;

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

fn request(task_id: &str, idempotency_key: &str) -> PaperArbitrageRequest {
    let batch = ExecutionBatch::planned(vec![
        intent("paper-left", Side::Buy),
        intent("paper-right", Side::Sell),
    ])
    .unwrap();
    let reservation = PaperReservationRequest::planned(
        task_id,
        idempotency_key,
        batch.id(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        batch
            .intents()
            .iter()
            .enumerate()
            .map(|(index, intent)| {
                PaperReservationLeg::from_intent(index, intent, money("100")).unwrap()
            })
            .collect(),
    )
    .unwrap();
    PaperArbitrageRequest::new(Symbol::new("BTC-USDT").unwrap(), batch, reservation).unwrap()
}

fn saga(
    label: &str,
) -> (
    DurablePaperArbitrageSaga,
    PaperAccountAuthority,
    std::path::PathBuf,
) {
    let path = temp_path(label);
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    (
        DurablePaperArbitrageSaga::new(account.clone(), history).unwrap(),
        account,
        path,
    )
}

fn receipt(intent: &OrderIntent, index: usize) -> TradingReceipt {
    receipt_with_disposition(intent, index, SubmissionDisposition::Filled)
}

fn receipt_with_disposition(
    intent: &OrderIntent,
    index: usize,
    disposition: SubmissionDisposition,
) -> TradingReceipt {
    let (filled_quantity, average_fill_price, status) = match disposition {
        SubmissionDisposition::Filled => (
            intent.quantity,
            Some(Price::new(decimal("100")).unwrap()),
            OrderStatus::Filled,
        ),
        SubmissionDisposition::Cancelled => (Quantity::default(), None, OrderStatus::Cancelled),
        SubmissionDisposition::Open | SubmissionDisposition::AlreadyProcessed => {
            (Quantity::default(), None, OrderStatus::Open)
        }
    };
    TradingReceipt::Submitted {
        order: Order {
            id: format!("paper-{index}"),
            intent: intent.clone(),
            filled_quantity,
            average_fill_price,
            status,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        disposition,
    }
}

fn successful_receipts(batch: &ExecutionBatch) -> Vec<TradingReceipt> {
    batch
        .intents()
        .iter()
        .enumerate()
        .map(|(index, intent)| receipt(intent, index))
        .collect()
}

#[tokio::test]
async fn exact_pair_is_reserved_then_planned_before_execution_and_replay_is_idempotent() {
    let (saga, account, path) = saga("journal-first");
    let request = request("arb:btc", "open:0001");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_path = path.clone();
    let observed_calls = Arc::clone(&calls);

    let result = saga
        .run(request.clone(), move |batch| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            let records = records(&observed_path);
            assert_eq!(
                decisions(&records),
                vec!["paper_account_reserved", "execution_planned"]
            );
            Ok(successful_receipts(&batch))
        })
        .await
        .unwrap();
    let PaperArbitrageRun::Completed { receipts } = result else {
        panic!("first run must execute");
    };
    assert_eq!(receipts.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let journal_records = records(&path);
    assert_eq!(
        decisions(&journal_records),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_completed",
            "paper_account_committed",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.pending_reserved, Money::default());
    assert_eq!(snapshot.committed_exposure, money("200"));
    assert_eq!(snapshot.available, money("800"));

    let result = saga
        .run(request, |_| async {
            panic!("a completed idempotency key must never execute again");
        })
        .await
        .unwrap();
    assert!(matches!(result, PaperArbitrageRun::AlreadyCompleted { .. }));
    assert_eq!(records(&path).len(), 4);
}

#[tokio::test]
async fn two_nonfilled_receipts_are_incomplete_and_hold_uncertain_capacity() {
    let (saga, account, path) = saga("two-nonfilled");
    let request = request("arb:btc", "open:0001");

    let error = saga
        .run(request.clone(), |batch| async move {
            Ok(vec![
                receipt_with_disposition(&batch.intents()[0], 0, SubmissionDisposition::Open),
                receipt_with_disposition(
                    &batch.intents()[1],
                    1,
                    SubmissionDisposition::AlreadyProcessed,
                ),
            ])
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::Incomplete(receipts) if receipts.len() == 2
    ));
    assert_eq!(
        decisions(&records(&path)),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_incomplete",
            "paper_account_uncertain",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.uncertain_reserved, money("200.60"));

    let error = saga
        .run(request, |_| async {
            panic!("an incomplete pair must never be retried automatically");
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::RecoveryRequired {
            stage: PaperArbitrageRecoveryStage::Incomplete,
            ..
        }
    ));
}

#[tokio::test]
async fn filled_receipts_for_the_wrong_legs_cannot_commit_capacity() {
    let (saga, account, path) = saga("wrong-filled-legs");
    let request = request("arb:btc", "open:0001");

    let error = saga
        .run(request, |batch| async move {
            Ok(vec![
                receipt(&batch.intents()[0], 0),
                receipt(&batch.intents()[0], 1),
            ])
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::Incomplete(receipts) if receipts.len() == 2
    ));
    assert_eq!(
        decisions(&records(&path)),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_incomplete",
            "paper_account_uncertain",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.committed_exposure, Money::default());
    assert_eq!(snapshot.uncertain_reserved, money("200.60"));
}

#[tokio::test]
async fn filled_receipts_are_correlated_by_identity_not_vector_position() {
    let (saga, account, path) = saga("reversed-filled");
    let request = request("arb:btc", "open:0001");

    let result = saga
        .run(request, |batch| async move {
            Ok(vec![
                receipt(&batch.intents()[1], 1),
                receipt(&batch.intents()[0], 0),
            ])
        })
        .await
        .unwrap();
    assert!(matches!(result, PaperArbitrageRun::Completed { .. }));
    assert_eq!(
        decisions(&records(&path)),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_completed",
            "paper_account_committed",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.committed_exposure, money("200"));
}

#[tokio::test]
async fn two_confirmed_cancellations_release_capacity_but_are_not_completed() {
    let (saga, account, path) = saga("two-cancelled");
    let request = request("arb:btc", "open:0001");

    let error = saga
        .run(request, |batch| async move {
            Ok(vec![
                receipt_with_disposition(&batch.intents()[1], 1, SubmissionDisposition::Cancelled),
                receipt_with_disposition(&batch.intents()[0], 0, SubmissionDisposition::Cancelled),
            ])
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::Incomplete(receipts) if receipts.len() == 2
    ));
    assert_eq!(
        decisions(&records(&path)),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_incomplete",
            "paper_account_released",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.available, money("1000"));
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Released
    );
}

#[tokio::test]
async fn cancelled_receipts_for_the_wrong_legs_cannot_release_capacity() {
    let (saga, account, path) = saga("wrong-cancelled-legs");
    let request = request("arb:btc", "open:0001");

    let error = saga
        .run(request, |batch| async move {
            Ok(vec![
                receipt_with_disposition(&batch.intents()[0], 0, SubmissionDisposition::Cancelled),
                receipt_with_disposition(&batch.intents()[0], 1, SubmissionDisposition::Cancelled),
            ])
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::Incomplete(receipts) if receipts.len() == 2
    ));
    assert_eq!(
        decisions(&records(&path)),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_incomplete",
            "paper_account_uncertain",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.available, money("799.40"));
    assert_eq!(snapshot.uncertain_reserved, money("200.60"));
}

#[tokio::test]
async fn restart_after_planned_only_never_resubmits_and_requires_reconcile() {
    let (saga, account, path) = saga("planned-only");
    let request = request("arb:btc", "open:0001");
    let (started, observed) = tokio::sync::oneshot::channel();
    let running = {
        let saga = saga.clone();
        let request = request.clone();
        tokio::spawn(async move {
            saga.run(request, move |_| async move {
                let _ = started.send(());
                pending::<Result<Vec<TradingReceipt>, RuntimeError>>().await
            })
            .await
        })
    };
    observed.await.unwrap();
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());

    assert_eq!(
        decisions(&records(&path)),
        vec!["paper_account_reserved", "execution_planned"]
    );
    let recovered_account = PaperAccountAuthority::new(
        account.journal_id(),
        JsonlHistory::new(&path),
        account.config().clone(),
    )
    .unwrap();
    let restarted =
        DurablePaperArbitrageSaga::new(recovered_account, JsonlHistory::new(&path)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let error = restarted
        .run(request, move |_| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PaperArbitrageSagaError::RecoveryRequired {
            stage: PaperArbitrageRecoveryStage::OutcomeUnknown,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(records(&path).len(), 2);
}

#[tokio::test]
async fn restart_after_reservation_only_never_invents_an_execution_plan() {
    let (saga, account, path) = saga("reservation-only");
    let request = request("arb:btc", "open:0001");
    account
        .reserve(request.reservation().clone())
        .await
        .unwrap();

    let error = saga
        .run(request, |_| async {
            panic!("an orphan durable reservation must not invent a plan");
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::RecoveryRequired {
            stage: PaperArbitrageRecoveryStage::ReservedOnly,
            ..
        }
    ));
    assert_eq!(decisions(&records(&path)), vec!["paper_account_reserved"]);
}

#[tokio::test]
async fn in_flight_duplicate_request_cannot_cross_the_execution_seam_twice() {
    let (saga, _, path) = saga("in-flight-duplicate");
    let request = request("arb:btc", "open:0001");
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let first = {
        let saga = saga.clone();
        let request = request.clone();
        let calls = Arc::clone(&calls);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        tokio::spawn(async move {
            saga.run(request, move |batch| async move {
                calls.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                Ok(successful_receipts(&batch))
            })
            .await
        })
    };
    started.notified().await;

    let second_calls = Arc::clone(&calls);
    let error = saga
        .run(request, move |_| async move {
            second_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::RecoveryRequired {
            stage: PaperArbitrageRecoveryStage::OutcomeUnknown,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        decisions(&records(&path)),
        vec!["paper_account_reserved", "execution_planned"]
    );

    release.notify_one();
    assert!(matches!(
        first.await.unwrap().unwrap(),
        PaperArbitrageRun::Completed { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn single_leg_failure_is_durable_uncertain_and_never_auto_retried() {
    let (saga, account, path) = saga("partial");
    let request = request("arb:btc", "open:0001");
    let batch_id = request.batch().id();
    let error = saga
        .run(request.clone(), |batch| async move {
            Err(RuntimeError::PartialExecution {
                batch_id: batch.id(),
                failed_index: 1,
                completed: vec![receipt(&batch.intents()[0], 0)],
                failed_intent: Box::new(batch.intents()[1].clone()),
                unattempted: Vec::new(),
                reconciliation: Vec::new(),
                source: Box::new(RuntimeError::UnknownExchange(
                    "fixture single-leg failure".to_owned(),
                )),
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        PaperArbitrageSagaError::Execution(RuntimeError::PartialExecution { .. })
    ));

    assert_eq!(
        decisions(&records(&path)),
        vec![
            "paper_account_reserved",
            "execution_planned",
            "execution_partial",
            "paper_account_uncertain",
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.uncertain_reserved, money("200.60"));
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Uncertain
    );

    let source = FileJournalSnapshotSource::new(account.journal_id(), &path).unwrap();
    let operator = OperatorReadModel::from_legacy_snapshot(&source.snapshot().unwrap()).unwrap();
    let batch = operator
        .batches
        .iter()
        .find(|batch| batch.batch_id == batch_id)
        .unwrap();
    assert_eq!(batch.failed_index, Some(1));
    assert_eq!(batch.receipt_count, Some(1));
    assert_eq!(batch.unattempted_count, Some(0));

    let retry = saga
        .run(request, |_| async {
            panic!("partial execution must never be retried automatically");
        })
        .await
        .unwrap_err();
    assert!(matches!(
        retry,
        PaperArbitrageSagaError::RecoveryRequired {
            stage: PaperArbitrageRecoveryStage::Partial,
            ..
        }
    ));
    assert_eq!(records(&path).len(), 4);
}

#[tokio::test]
async fn outcome_journal_failure_preserves_receipts_and_restart_never_resubmits() {
    let (saga, account, path) = saga("outcome-journal-failure");
    let request = request("arb:btc", "open:0001");
    let restart_request = request.clone();
    let sabotaged_path = path.clone();
    let backup_path = temp_path("outcome-journal-backup");
    let callback_backup = backup_path.clone();

    let error = saga
        .run(request, move |batch| async move {
            let receipts = successful_receipts(&batch);
            std::fs::copy(&sabotaged_path, &callback_backup).unwrap();
            std::fs::remove_file(&sabotaged_path).unwrap();
            std::fs::create_dir(&sabotaged_path).unwrap();
            Ok(receipts)
        })
        .await
        .unwrap_err();

    let display = error.to_string();
    assert!(display.contains("completed outcome with 2 receipt(s)"));
    assert!(!display.contains("paper-0"));
    assert!(!display.contains("BTC-USDT"));
    assert!(!display.contains("TradingReceipt"));

    let PaperArbitrageSagaError::OutcomeJournal {
        outcome: crypto_trading_cli::PaperArbitragePreservedOutcome::Completed(receipts),
        ..
    } = error
    else {
        panic!("completed receipts must survive an outcome journal failure");
    };
    assert_eq!(receipts.len(), 2);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(backup_path, &path).unwrap();

    let recovered_account = PaperAccountAuthority::new(
        account.journal_id(),
        JsonlHistory::new(&path),
        account.config().clone(),
    )
    .unwrap();
    let restarted =
        DurablePaperArbitrageSaga::new(recovered_account, JsonlHistory::new(&path)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let restart_error = restarted
        .run(restart_request, move |_| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        restart_error,
        PaperArbitrageSagaError::RecoveryRequired {
            stage: PaperArbitrageRecoveryStage::OutcomeUnknown,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        decisions(&records(&path)),
        vec!["paper_account_reserved", "execution_planned"]
    );
}

#[test]
fn non_exact_or_same_side_pair_is_rejected_before_any_journal_is_selected() {
    let one_leg = ExecutionBatch::planned(vec![intent("paper-left", Side::Buy)]).unwrap();
    let one_leg_reservation = PaperReservationRequest::planned(
        "arb:btc",
        "open:0001",
        one_leg.id(),
        PaperCostModel::v1(0, 0, 0).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &one_leg.intents()[0], money("100")).unwrap()],
    )
    .unwrap();
    let error = PaperArbitrageRequest::new(
        Symbol::new("BTC-USDT").unwrap(),
        one_leg,
        one_leg_reservation,
    )
    .unwrap_err();
    assert!(matches!(error, PaperArbitrageSagaError::InvalidRequest(_)));

    let same_side = ExecutionBatch::planned(vec![
        intent("paper-left", Side::Buy),
        intent("paper-right", Side::Buy),
    ])
    .unwrap();
    let same_side_reservation = PaperReservationRequest::planned(
        "arb:btc",
        "open:0002",
        same_side.id(),
        PaperCostModel::v1(0, 0, 0).unwrap(),
        same_side
            .intents()
            .iter()
            .enumerate()
            .map(|(index, intent)| {
                PaperReservationLeg::from_intent(index, intent, money("100")).unwrap()
            })
            .collect(),
    )
    .unwrap();
    let error = PaperArbitrageRequest::new(
        Symbol::new("BTC-USDT").unwrap(),
        same_side,
        same_side_reservation,
    )
    .unwrap_err();
    assert!(matches!(error, PaperArbitrageSagaError::InvalidRequest(_)));
}

fn records(path: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn decisions(records: &[serde_json::Value]) -> Vec<&str> {
    records
        .iter()
        .map(|record| record["decision"].as_str().unwrap())
        .collect()
}

fn temp_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-paper-arbitrage-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}
