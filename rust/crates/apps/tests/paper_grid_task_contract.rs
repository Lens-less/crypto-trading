use std::{
    collections::VecDeque,
    future::pending,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_cli::{
    GridPaperExecutionFuture, GridPaperExecutor, GridPaperTask, GridPaperTaskConfig,
    GridPaperTaskError, GridPaperTaskExit, GridPaperTaskFailure, GridPaperTaskPhase,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderStatus, Price, Quantity, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    ExecutionBatch, JsonlHistory, MarketDataEvent, MarketDataEventFuture, MarketDataEventSource,
    MarketDataObservation, MarketSupervisorConfig, PaperAccountAuthority, PaperAccountConfig,
    PaperCostModel, PaperReconciliationDigestAlgorithm, PaperReconciliationProof,
    PaperReservationPhase, ReadOnlyTaskKind, ReadOnlyTaskPhase, RuntimeError,
};
use crypto_trading_strategy::{VirtualGrid, VirtualGridConfig};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).unwrap()
}

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap()
}

fn grid() -> VirtualGrid {
    VirtualGrid::new(
        VirtualGridConfig {
            symbol: Symbol::new("BTC-USDT").unwrap(),
            initial_price: price("100"),
            grid_width_percent: decimal("10"),
            grid_interval_percent: Decimal::ONE,
        },
        base_time(),
    )
    .unwrap()
}

fn config(task_id: &str, grace: StdDuration) -> GridPaperTaskConfig {
    GridPaperTaskConfig::new(
        task_id,
        "paper-grid",
        MarketType::Perpetual,
        quantity("1"),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        MarketSupervisorConfig::new(grace).unwrap(),
    )
    .unwrap()
}

fn account(label: &str) -> (PaperAccountAuthority, JsonlHistory, std::path::PathBuf) {
    let path = temp_path(label);
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new("paper-grid", Money::new(decimal("10000"))).unwrap(),
    )
    .unwrap();
    (account, history, path)
}

fn observation(last: &str, revision: u64, received_at: chrono::DateTime<Utc>) -> MarketDataEvent {
    let mut snapshot = MarketSnapshot::new(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        price("96.9"),
        price("97.1"),
        received_at,
    )
    .unwrap();
    snapshot.last = Some(price(last));
    MarketDataEvent::Observation(
        MarketDataObservation::new(snapshot, revision, received_at).unwrap(),
    )
}

#[derive(Debug)]
struct VecSource {
    events: VecDeque<MarketDataEvent>,
}

impl VecSource {
    fn new(events: Vec<MarketDataEvent>) -> Self {
        Self {
            events: events.into(),
        }
    }
}

impl MarketDataEventSource for VecSource {
    fn source_id(&self) -> &'static str {
        "paper-grid"
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(async move { Ok(self.events.pop_front()) })
    }
}

#[derive(Debug)]
struct BlockingSource {
    first: Option<MarketDataEvent>,
}

impl MarketDataEventSource for BlockingSource {
    fn source_id(&self) -> &'static str {
        "paper-grid"
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        if let Some(event) = self.first.take() {
            return Box::pin(async move { Ok(Some(event)) });
        }
        Box::pin(pending())
    }
}

#[derive(Debug, Default)]
struct FillExecutor {
    calls: AtomicUsize,
}

impl GridPaperExecutor for FillExecutor {
    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let intent = batch.intents()[0].clone();
            Ok(vec![TradingReceipt::Submitted {
                order: Order {
                    id: format!("paper-{}", intent.client_order_id),
                    intent: intent.clone(),
                    filled_quantity: intent.quantity,
                    average_fill_price: intent.price,
                    status: OrderStatus::Filled,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                },
                disposition: SubmissionDisposition::Filled,
            }])
        })
    }
}

#[derive(Debug, Default)]
struct TimeoutExecutor;

impl GridPaperExecutor for TimeoutExecutor {
    fn execute(&self, _batch: ExecutionBatch) -> GridPaperExecutionFuture {
        Box::pin(async {
            Err(RuntimeError::InvalidExecutionPolicy(
                "simulated grid dispatch timeout",
            ))
        })
    }
}

#[derive(Debug, Default)]
struct PendingExecutor {
    started: AtomicBool,
}

impl GridPaperExecutor for PendingExecutor {
    fn execute(&self, _batch: ExecutionBatch) -> GridPaperExecutionFuture {
        self.started.store(true, Ordering::SeqCst);
        Box::pin(pending())
    }
}

#[tokio::test]
async fn price_gap_emits_three_independent_single_leg_operations() {
    let (account, history, _) = account("three-crosses");
    let executor = Arc::new(FillExecutor::default());
    let source = VecSource::new(vec![observation(
        "97",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:btc", StdDuration::from_secs(1)),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::SourceEnded);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 3);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 3);
    assert_eq!(
        snapshot
            .reservations
            .iter()
            .map(|reservation| reservation.task_id.as_str())
            .collect::<Vec<_>>(),
        [
            "grid:btc/op/000001",
            "grid:btc/op/000002",
            "grid:btc/op/000003",
        ]
    );
    assert!(snapshot.reservations.iter().all(|reservation| {
        reservation.phase == PaperReservationPhase::Committed
            && reservation.idempotency_key
                == format!(
                    "grid:{:06}",
                    reservation
                        .task_id
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap()
                )
    }));
    let durable = task.durable_status().await.unwrap();
    assert_eq!(durable.kind, ReadOnlyTaskKind::GridPaper);
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(durable.sources.len(), 1);
    assert_eq!(task.status().operation_count, 3);
}

#[tokio::test]
async fn timeout_marks_operation_uncertain_and_restart_fails_closed() {
    let (account, history, _) = account("timeout");
    let source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:timeout", StdDuration::from_secs(1)),
        grid(),
        source,
        account.clone(),
        history.clone(),
        Arc::new(TimeoutExecutor),
    )
    .await
    .unwrap();

    let error = task.wait().await.unwrap_err();
    assert!(matches!(
        error,
        GridPaperTaskError::Saga(_) | GridPaperTaskError::Runtime(_)
    ));
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::ExecutionFailed)
    );
    assert_eq!(
        account.snapshot().await.unwrap().reservations[0].phase,
        PaperReservationPhase::Uncertain
    );

    let restart = GridPaperTask::start(
        config("grid:timeout", StdDuration::from_secs(1)),
        grid(),
        VecSource::new(Vec::new()),
        account,
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();
    assert!(matches!(restart, GridPaperTaskError::RecoveryRequired));
}

#[tokio::test]
async fn cancel_during_unknown_execution_retains_capacity_without_release() {
    let (account, history, path) = account("cancel-unknown");
    let executor = Arc::new(PendingExecutor::default());
    let source = BlockingSource {
        first: Some(observation("99", 1, base_time() + Duration::seconds(10))),
    };
    let mut task = GridPaperTask::start(
        config("grid:cancel", StdDuration::from_millis(250)),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| executor.started.load(Ordering::SeqCst)).await;

    let error = task.cancel().await.unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Uncertain
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
    assert!(!body.contains("\"decision\":\"paper_account_released\""));
}

#[tokio::test]
async fn failed_reconciliation_blocks_a_new_owner_before_registration() {
    let (account, history, path) = account("failed-reconcile");
    let source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut first = GridPaperTask::start(
        config("grid:first", StdDuration::from_secs(1)),
        grid(),
        source,
        account.clone(),
        history.clone(),
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    first.wait().await.unwrap();

    let reservation = account.snapshot().await.unwrap().reservations[0].clone();
    account
        .record_reconciliation_failure(
            PaperReconciliationProof::new(
                "paper-grid",
                reservation.reservation_id,
                reservation.batch_id,
                "binance-testnet/account-snapshot-1",
                1,
                PaperReconciliationDigestAlgorithm::Fnv1a64,
                "0011223344556677",
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let records_before = std::fs::read_to_string(&path).unwrap().lines().count();

    let restart = GridPaperTask::start(
        config("grid:second", StdDuration::from_secs(1)),
        grid(),
        VecSource::new(Vec::new()),
        account,
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();
    assert!(matches!(restart, GridPaperTaskError::RecoveryRequired));
    assert_eq!(
        std::fs::read_to_string(path).unwrap().lines().count(),
        records_before,
        "failed preflight must not append a new task registration"
    );
}

#[tokio::test]
async fn clean_stop_can_restart_the_stable_owner_without_degrading_projection() {
    let (account, history, _) = account("clean-restart");
    let first_source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut first = GridPaperTask::start(
        config("grid:restart", StdDuration::from_secs(1)),
        grid(),
        first_source,
        account.clone(),
        history.clone(),
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    first.wait().await.unwrap();

    let second_source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(20),
    )]);
    let mut second = GridPaperTask::start(
        config("grid:restart", StdDuration::from_secs(1)),
        grid(),
        second_source,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    second.wait().await.unwrap();

    let durable = second.durable_status().await.unwrap();
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(durable.sources.len(), 1);
    assert_eq!(second.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 2);
    assert_eq!(snapshot.reservations[1].task_id, "grid:restart/op/000002");
}

#[tokio::test]
async fn stop_without_an_inflight_operation_is_durable_and_opens_no_reservation() {
    let (account, history, _) = account("stop-idle");
    let source = BlockingSource { first: None };
    let mut task = GridPaperTask::start(
        config("grid:idle", StdDuration::from_millis(250)),
        grid(),
        source,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();

    assert_eq!(task.stop().await.unwrap(), GridPaperTaskExit::StopRequested);
    assert_eq!(task.status().phase, GridPaperTaskPhase::Stopped);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    assert_eq!(
        task.durable_status().await.unwrap().phase,
        ReadOnlyTaskPhase::Stopped
    );
}

async fn wait_until(predicate: impl Fn() -> bool) {
    for _ in 0..100 {
        if predicate() {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("condition was not observed within the test deadline");
}

fn temp_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-grid-owner-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}
