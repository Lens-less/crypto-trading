use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use crypto_trading_cli::{
    DurablePaperSingleLegSaga, PaperSingleLegRequest, PaperSingleLegRun, PaperSingleLegSagaError,
};
use crypto_trading_domain::{
    MarketType, Money, Order, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    ExecutionBatch, JsonlHistory, PaperAccountAuthority, PaperAccountConfig, PaperCostModel,
    PaperReservationLeg, PaperReservationPhase, PaperReservationRequest, RuntimeError,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn request(task_id: &str, key: &str) -> PaperSingleLegRequest {
    let intent = OrderIntent::limit(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
        Price::new(decimal("100")).unwrap(),
    );
    let batch = ExecutionBatch::planned(vec![intent.clone()]).unwrap();
    let reservation = PaperReservationRequest::planned(
        task_id,
        key,
        batch.id(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &intent, Money::new(decimal("100"))).unwrap()],
    )
    .unwrap();
    PaperSingleLegRequest::new(Symbol::new("BTC-USDT").unwrap(), batch, reservation).unwrap()
}

fn saga(
    label: &str,
) -> (
    DurablePaperSingleLegSaga,
    PaperAccountAuthority,
    std::path::PathBuf,
) {
    let path = temp_path(label);
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new("paper-grid", Money::new(decimal("1000"))).unwrap(),
    )
    .unwrap();
    (
        DurablePaperSingleLegSaga::new(account.clone(), history).unwrap(),
        account,
        path,
    )
}

fn receipt(intent: &OrderIntent, disposition: SubmissionDisposition) -> TradingReceipt {
    let (status, filled_quantity, average_fill_price) = match disposition {
        SubmissionDisposition::Filled => (
            OrderStatus::Filled,
            intent.quantity,
            Some(Price::new(decimal("100")).unwrap()),
        ),
        SubmissionDisposition::Cancelled => (OrderStatus::Cancelled, Quantity::default(), None),
        SubmissionDisposition::Open | SubmissionDisposition::AlreadyProcessed => {
            (OrderStatus::Open, Quantity::default(), None)
        }
    };
    TradingReceipt::Submitted {
        order: Order {
            id: "paper-grid-order".to_owned(),
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

#[tokio::test]
async fn one_grid_leg_is_reserved_planned_and_committed_once() {
    let (saga, account, path) = saga("commit");
    let request = request("grid:btc/op/000001", "cross:000001");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);

    let result = saga
        .run(request.clone(), move |batch| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![receipt(
                &batch.intents()[0],
                SubmissionDisposition::Filled,
            )])
        })
        .await
        .unwrap();
    assert!(matches!(result, PaperSingleLegRun::Completed { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        account.snapshot().await.unwrap().reservations[0].phase,
        PaperReservationPhase::Committed
    );

    let replay = saga
        .run(request, |_| async {
            panic!("terminal single-leg operation must not execute twice");
        })
        .await
        .unwrap();
    assert!(matches!(
        replay,
        PaperSingleLegRun::AlreadyTerminal {
            phase: PaperReservationPhase::Committed,
            ..
        }
    ));
    assert_eq!(decisions(&path).len(), 4);
}

#[tokio::test]
async fn confirmed_cancel_releases_but_open_cancel_result_stays_uncertain() {
    let (saga, account, _) = saga("cancel");
    let cancelled = request("grid:btc/op/000001", "cross:000001");
    let result = saga
        .run(cancelled, |batch| async move {
            Ok(vec![receipt(
                &batch.intents()[0],
                SubmissionDisposition::Cancelled,
            )])
        })
        .await
        .unwrap();
    assert!(matches!(result, PaperSingleLegRun::Cancelled { .. }));
    assert_eq!(
        account.snapshot().await.unwrap().reservations[0].phase,
        PaperReservationPhase::Released
    );

    let uncertain = request("grid:btc/op/000002", "cross:000002");
    let error = saga
        .run(uncertain, |batch| async move {
            Ok(vec![receipt(
                &batch.intents()[0],
                SubmissionDisposition::Open,
            )])
        })
        .await
        .unwrap_err();
    assert!(matches!(error, PaperSingleLegSagaError::Incomplete(_)));
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(
        snapshot.reservations[1].phase,
        PaperReservationPhase::Uncertain
    );
    assert!(snapshot.uncertain_reserved > Money::default());
}

#[tokio::test]
async fn execution_error_is_uncertain_and_restart_never_resubmits() {
    let (saga, account, _) = saga("error");
    let request = request("grid:btc/op/000001", "cross:000001");
    let error = saga
        .run(request.clone(), |_| async {
            Err(RuntimeError::InvalidExecutionPolicy(
                "simulated dispatch timeout",
            ))
        })
        .await
        .unwrap_err();
    assert!(matches!(error, PaperSingleLegSagaError::Execution(_)));
    assert_eq!(
        account.snapshot().await.unwrap().reservations[0].phase,
        PaperReservationPhase::Uncertain
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let replay = saga
        .run(request, move |_| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .await
        .unwrap_err();
    assert!(matches!(
        replay,
        PaperSingleLegSagaError::RecoveryRequired {
            phase: PaperReservationPhase::Uncertain,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

fn decisions(path: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["decision"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn temp_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-paper-single-leg-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}
