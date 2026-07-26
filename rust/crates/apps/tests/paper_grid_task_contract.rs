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
    MarketSnapshot, MarketType, Money, Order, OrderStatus, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    AccountRiskAuthority, ExecutionBatch, JsonlHistory, MarketDataEvent, MarketDataEventFuture,
    MarketDataEventSource, MarketDataObservation, MarketSupervisorConfig, PaperAccountAuthority,
    PaperAccountConfig, PaperCostModel, PaperReconciliationEvidence, PaperReconciliationProof,
    PaperReservationPhase, ReadOnlyTaskKind, ReadOnlyTaskPhase, RuntimeError,
};
use crypto_trading_strategy::{
    AccountRiskLimits, AccountRiskPolicy, GridDirection, GridProtectionGeometry,
    GridProtectionMachine, GridProtectionPolicies, PriceLockPolicyConfig, StopLossPolicyConfig,
    VirtualGrid, VirtualGridConfig,
};
use rust_decimal::Decimal;
use tokio::sync::Semaphore;

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

#[derive(Debug)]
struct GatedFillExecutor {
    started: AtomicBool,
    release: Arc<Semaphore>,
}

impl Default for GatedFillExecutor {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            release: Arc::new(Semaphore::new(0)),
        }
    }
}

impl GatedFillExecutor {
    fn release_one(&self) {
        self.release.add_permits(1);
    }
}

impl GridPaperExecutor for GatedFillExecutor {
    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        self.started.store(true, Ordering::SeqCst);
        let permit = self.release.clone();
        Box::pin(async move {
            permit.acquire_owned().await.unwrap().forget();
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
        config("grid:btc", StdDuration::from_secs(30)),
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

fn account_risk(
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
    limits: AccountRiskLimits,
) -> AccountRiskAuthority {
    AccountRiskAuthority::new(
        account.journal_id(),
        history.clone(),
        "paper",
        AccountRiskPolicy::new(limits).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn account_risk_rejections_skip_entry_crossings_without_reservations() {
    let (account, history, path) = account("account-risk-rejects");
    let risk = account_risk(
        &account,
        &history,
        AccountRiskLimits {
            disabled_symbols: std::collections::BTreeSet::from(["BTC-USDT".to_owned()]),
            ..AccountRiskLimits::default()
        },
    );
    let executor = Arc::new(FillExecutor::default());
    let source = VecSource::new(vec![observation(
        "97",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:risk-reject", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    // Every entry crossing is refused before any reservation exists; the
    // owner stays alive and completes when the source ends.
    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::SourceEnded);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(task.status().operation_count, 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let state = risk.state().await.unwrap();
    assert_eq!(state.rejected_count, 3);
    assert_eq!(state.last_rejection.as_deref(), Some("symbol_disabled"));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_rejected\""));
    assert!(!body.contains("\"decision\":\"paper_account_reserved\""));
}

#[tokio::test]
async fn engaged_kill_switch_stops_the_grid_owner_before_any_entry() {
    let (account, history, path) = account("account-risk-kill");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time())
        .await
        .unwrap();
    let executor = Arc::new(FillExecutor::default());
    let source = VecSource::new(vec![observation(
        "97",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:risk-kill", StdDuration::from_secs(30)).with_account_risk(risk),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    // The close-all directive is consumed like a protection exit: one durable
    // fact, a clean stop, and no execution.
    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::StopRequested);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("kill_switch:operator drill"));
}

#[tokio::test]
async fn stop_during_a_multi_cross_event_never_checkpoints_unexecuted_crosses_as_consumed() {
    let (account, history, path) = account("stop-multi-cross");
    let executor = Arc::new(GatedFillExecutor::default());
    let source = BlockingSource {
        first: Some(observation("97", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config("grid:stop-gap", StdDuration::from_secs(30)),
        grid(),
        source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| executor.started.load(Ordering::SeqCst)).await;

    let (stop_result, ()) = tokio::join!(task.stop(), async {
        tokio::task::yield_now().await;
        executor.release_one();
    });

    assert!(matches!(
        stop_result,
        Err(GridPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 1);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Committed
    );
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(!body.contains("\"decision\":\"task_checkpointed\""));
    assert!(!body.contains("\"decision\":\"task_stopped\""));

    let restart = GridPaperTask::start(
        config("grid:stop-gap", StdDuration::from_secs(30)),
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
async fn timeout_marks_operation_uncertain_and_restart_fails_closed() {
    let (account, history, _) = account("timeout");
    let source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:timeout", StdDuration::from_secs(30)),
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
        config("grid:timeout", StdDuration::from_secs(30)),
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
        config("grid:first", StdDuration::from_secs(30)),
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
            PaperReconciliationProof::from_evidence(
                PaperReconciliationEvidence::mismatch(
                    "contract-fixture",
                    "0011223344556677",
                    "paper-grid",
                    reservation.reservation_id,
                    reservation.batch_id,
                    "binance-testnet/account-snapshot-1",
                    1,
                    Money::new(decimal("10000")),
                    "fixture_mismatch",
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let records_before = std::fs::read_to_string(&path).unwrap().lines().count();

    let restart = GridPaperTask::start(
        config("grid:second", StdDuration::from_secs(30)),
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
        config("grid:restart", StdDuration::from_secs(30)),
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
        config("grid:restart", StdDuration::from_secs(30)),
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
        config("grid:idle", StdDuration::from_secs(30)),
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

/// Grid-protection machine matching the test grid geometry (100 +- 5%,
/// ten one-percent levels).
fn protection(policies: GridProtectionPolicies) -> GridProtectionMachine {
    GridProtectionMachine::new(
        GridProtectionGeometry::new(GridDirection::Long, price("95"), price("105"), 10).unwrap(),
        policies,
    )
    .unwrap()
}

/// Source that releases one event per semaphore permit so a multi-event test
/// is not conflated by the supervisor's latest-event retention.
#[derive(Debug)]
struct SteppedSource {
    events: VecDeque<MarketDataEvent>,
    release: Arc<Semaphore>,
}

impl SteppedSource {
    fn new(events: Vec<MarketDataEvent>, release: Arc<Semaphore>) -> Self {
        Self {
            events: events.into(),
            release,
        }
    }
}

impl MarketDataEventSource for SteppedSource {
    fn source_id(&self) -> &'static str {
        "paper-grid"
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        let Some(event) = self.events.pop_front() else {
            return Box::pin(async move { Ok(None) });
        };
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            release.acquire_owned().await.unwrap().forget();
            Ok(Some(event))
        })
    }
}

#[derive(Debug, Default)]
struct RecordingExecutor {
    intents: std::sync::Mutex<Vec<(Side, Decimal)>>,
}

impl GridPaperExecutor for RecordingExecutor {
    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        let intent = batch.intents()[0].clone();
        self.intents
            .lock()
            .unwrap()
            .push((intent.side, intent.price.unwrap().as_decimal()));
        Box::pin(async move {
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

#[tokio::test]
async fn stop_loss_exit_stops_the_task_and_journals_the_exit_all_fact() {
    let (account, history, path) = account("stop-loss-exit");
    let executor = Arc::new(FillExecutor::default());
    // $94 sits at/below the 100% stop-loss trigger ($95); the second
    // observation arrives after the one-second escape timeout with zero
    // completed cycles, so the APR gate decides to exit.
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("94", 1, base_time() + Duration::seconds(70)),
            observation("94", 2, base_time() + Duration::seconds(80)),
        ],
        Arc::clone(&stepper),
    );
    let machine = protection(GridProtectionPolicies {
        stop_loss: Some(StopLossPolicyConfig::new(decimal("100"), 1, decimal("50")).unwrap()),
        ..GridProtectionPolicies::default()
    });
    let mut task = GridPaperTask::start(
        config("grid:stop-loss", StdDuration::from_secs(30)).with_protection(machine),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        // The first observation crosses the five buy levels; release the
        // second only after they are all executed.
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 5).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), GridPaperTaskExit::StopRequested);
    assert_eq!(task.status().phase, GridPaperTaskPhase::Stopped);
    assert_eq!(
        task.durable_status().await.unwrap().phase,
        ReadOnlyTaskPhase::Stopped
    );
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("\"decision\":\"exit_all\""));
    assert!(body.contains("\"reason\":\"stop_loss_apr_below_threshold\""));
    assert!(body.contains("\"strategy\":\"grid_protection\""));
}

#[tokio::test]
async fn price_lock_freezes_entries_without_closing_or_stopping() {
    let (account, history, path) = account("price-lock");
    let executor = Arc::new(FillExecutor::default());
    // $107 escapes above the grid ($105) and reaches the $106 lock threshold;
    // without the lock these observations would emit sell operations.
    let source = VecSource::new(vec![
        observation("107", 1, base_time() + Duration::seconds(70)),
        observation("107", 2, base_time() + Duration::seconds(80)),
    ]);
    let machine = protection(GridProtectionPolicies {
        price_lock: Some(PriceLockPolicyConfig::new(price("106"))),
        ..GridProtectionPolicies::default()
    });
    let mut task = GridPaperTask::start(
        config("grid:price-lock", StdDuration::from_secs(30)).with_protection(machine),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::SourceEnded);
    assert_eq!(task.status().phase, GridPaperTaskPhase::Stopped);
    assert_eq!(task.status().processed_event_count, 2);
    // Frozen entries: no operations, no position close, no reservation.
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("\"decision\":\"freeze_entries\""));
    assert!(body.contains("\"reason\":\"price_lock_active\""));
    assert!(!body.contains("\"decision\":\"exit_all\""));
    // The steady-state freeze journals once, not per observation.
    assert_eq!(body.matches("\"decision\":\"freeze_entries\"").count(), 1);
}

#[tokio::test]
async fn filled_level_reposts_the_reverse_side_one_interval_away() {
    // Martingale/grid runtime semantics: a filled buy immediately re-arms the
    // reverse sell one interval above the fill, mirroring the legacy reverse
    // order flow (`grid_config.py:206-212`).
    let (account, history, _) = account("reverse-repost");
    let executor = Arc::new(RecordingExecutor::default());
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("99", 1, base_time() + Duration::seconds(70)),
            observation("100", 2, base_time() + Duration::seconds(80)),
        ],
        Arc::clone(&stepper),
    );
    let mut task = GridPaperTask::start(
        config("grid:reverse", StdDuration::from_secs(30)),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.intents.lock().unwrap().len() == 1).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), GridPaperTaskExit::SourceEnded);
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![(Side::Buy, decimal("99")), (Side::Sell, decimal("100"))]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 2);
}

async fn wait_until(predicate: impl Fn() -> bool) {
    // Deadline-based with a CI-jitter margin: the predicate normally flips in
    // milliseconds, so the budget only bounds a genuinely stuck condition.
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(10);
    loop {
        if predicate() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition was not observed within the test deadline"
        );
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
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
