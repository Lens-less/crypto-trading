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
    GridPaperExecutionFuture, GridPaperExecutor, GridPaperObservationFuture, GridPaperTask,
    GridPaperTaskConfig, GridPaperTaskError, GridPaperTaskExit, GridPaperTaskFailure,
    GridPaperTaskPhase,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderIntent, OrderStatus, Price, Quantity, Side,
    Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    AccountRiskAuthority, DecisionRecord, ExecutionBatch, JsonlHistory, MarketDataEvent,
    MarketDataEventFuture, MarketDataEventSource, MarketDataObservation, MarketSupervisorConfig,
    PaperAccountAuthority, PaperAccountConfig, PaperCostModel, PaperReconciliationEvidence,
    PaperReconciliationProof, PaperReservationLeg, PaperReservationPhase, PaperReservationRequest,
    ProjectionStatus, ReadOnlyTaskKind, ReadOnlyTaskPhase, RuntimeError,
};
use crypto_trading_strategy::{
    AccountRiskLimits, AccountRiskPolicy, GridDirection, GridProtectionGeometry,
    GridProtectionMachine, GridProtectionPolicies, PriceLockPolicyConfig, StopLossPolicyConfig,
    VirtualGrid, VirtualGridConfig,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use uuid::Uuid;

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
    account_with_available(label, "10000")
}

fn account_with_available(
    label: &str,
    initial_available: &str,
) -> (PaperAccountAuthority, JsonlHistory, std::path::PathBuf) {
    let path = temp_path(label);
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new("paper-grid", Money::new(decimal(initial_available))).unwrap(),
    )
    .unwrap();
    (account, history, path)
}

async fn seed_open_lot(
    account: &PaperAccountAuthority,
    symbol: &str,
    side: Side,
    fill_price: &str,
) {
    seed_open_lot_for_task(
        account,
        "unrelated-owner/op/000001",
        symbol,
        side,
        fill_price,
    )
    .await;
}

async fn seed_open_lot_for_task(
    account: &PaperAccountAuthority,
    task_id: &str,
    symbol: &str,
    side: Side,
    fill_price: &str,
) {
    let symbol = Symbol::new(symbol).unwrap();
    let intent = OrderIntent::market(
        "paper-grid",
        symbol,
        MarketType::Perpetual,
        side,
        quantity("1"),
    );
    let notional = Money::new(decimal(fill_price));
    let request = PaperReservationRequest::new(
        Uuid::new_v4(),
        task_id,
        format!("seed:{}", Uuid::new_v4()),
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &intent, notional).unwrap()],
    )
    .unwrap();
    let reservation_id = request.reservation_id();
    account.reserve(request).await.unwrap();
    account
        .settle_execution(
            reservation_id,
            &[TradingReceipt::Submitted {
                order: Order {
                    id: "paper-unrelated-open".to_owned(),
                    intent: intent.clone(),
                    filled_quantity: intent.quantity,
                    average_fill_price: Some(price(fill_price)),
                    status: OrderStatus::Filled,
                    created_at: base_time(),
                    updated_at: base_time(),
                },
                disposition: SubmissionDisposition::Filled,
            }],
        )
        .await
        .unwrap();
}

fn last_decision(path: &std::path::Path, decision: &str) -> Value {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|record| record["decision"] == decision)
        .unwrap_or_else(|| panic!("missing durable {decision} record"))
}

fn observation(last: &str, revision: u64, received_at: chrono::DateTime<Utc>) -> MarketDataEvent {
    observation_with_book("96.9", "97.1", last, revision, received_at)
}

fn observation_with_book(
    bid: &str,
    ask: &str,
    last: &str,
    revision: u64,
    received_at: chrono::DateTime<Utc>,
) -> MarketDataEvent {
    let mut snapshot = MarketSnapshot::new(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        price(bid),
        price(ask),
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
    observed: AtomicBool,
}

impl GridPaperExecutor for FillExecutor {
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        self.observed.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }

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

#[tokio::test]
async fn shared_account_operation_lease_rechecks_fifo_after_the_first_owner_settles() {
    let (first_account, history, _) = account("shared-operation-lease");
    let second_account = PaperAccountAuthority::new(
        first_account.journal_id(),
        history.clone(),
        PaperAccountConfig::new("paper-grid", Money::new(decimal("10000"))).unwrap(),
    )
    .unwrap();
    let first_source_gate = Arc::new(Semaphore::new(0));
    let second_source_gate = Arc::new(Semaphore::new(0));
    let first_executor = Arc::new(GatedFillExecutor::default());
    let second_executor = Arc::new(FillExecutor::default());
    let mut first = GridPaperTask::start(
        config("grid:lease-first", StdDuration::from_secs(2)),
        grid(),
        SteppedSource::new(
            vec![observation("99", 1, base_time() + Duration::seconds(10))],
            Arc::clone(&first_source_gate),
        ),
        first_account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    let mut second = GridPaperTask::start(
        config("grid:lease-second", StdDuration::from_secs(2)),
        grid(),
        SteppedSource::new(
            vec![observation("99", 1, base_time() + Duration::seconds(10))],
            Arc::clone(&second_source_gate),
        ),
        second_account.clone(),
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();

    first_source_gate.add_permits(1);
    wait_until(|| first_executor.started.load(Ordering::SeqCst)).await;
    let before_first_settle = second_account.snapshot().await.unwrap();
    assert_eq!(
        before_first_settle.reservations.len(),
        1,
        "{before_first_settle:?}"
    );
    assert_eq!(
        before_first_settle.reservations[0].phase,
        PaperReservationPhase::Pending,
        "executor dispatch must be preceded by a durable pending reservation"
    );
    second_source_gate.add_permits(1);
    wait_until(|| second_executor.observed.load(Ordering::SeqCst)).await;

    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(
        second_executor.calls.load(Ordering::SeqCst),
        0,
        "the second owner must wait behind the first owner's in-flight account operation"
    );

    first_executor.release_one();
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), first.wait())
            .await
            .expect("the first owner should settle and reach source EOF")
            .unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), second.wait())
            .await
            .expect("the second owner should recheck after acquiring the lease")
            .unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);

    let snapshot = second_account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    assert!(
        snapshot
            .reservations
            .iter()
            .all(|reservation| { reservation.task_id.starts_with("grid:lease-first/op/") }),
        "the second owner must fail FIFO isolation before reserving: {snapshot:?}"
    );
}

#[tokio::test]
async fn shutdown_timeout_handoff_blocks_a_queued_owner_before_uncertain_retention() {
    let (first_account, history, path) = account("shutdown-operation-lease-handoff");
    let second_account = PaperAccountAuthority::new(
        first_account.journal_id(),
        history.clone(),
        PaperAccountConfig::new("paper-grid", Money::new(decimal("10000"))).unwrap(),
    )
    .unwrap();
    let first_source_gate = Arc::new(Semaphore::new(0));
    let second_source_gate = Arc::new(Semaphore::new(0));
    let first_executor = Arc::new(PendingExecutor::default());
    let second_executor = Arc::new(FillExecutor::default());
    let mut first = GridPaperTask::start(
        config("grid:handoff-first", StdDuration::from_millis(250)),
        grid(),
        SteppedSource::new(
            vec![observation("99", 1, base_time() + Duration::seconds(10))],
            Arc::clone(&first_source_gate),
        ),
        first_account,
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    let mut second = GridPaperTask::start(
        config("grid:handoff-second", StdDuration::from_secs(2)),
        grid(),
        SteppedSource::new(
            vec![observation("99", 1, base_time() + Duration::seconds(10))],
            Arc::clone(&second_source_gate),
        ),
        second_account.clone(),
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();

    first_source_gate.add_permits(1);
    wait_until(|| first_executor.started.load(Ordering::SeqCst)).await;
    let before_abort = second_account.snapshot().await.unwrap();
    assert_eq!(before_abort.reservations.len(), 1, "{before_abort:?}");
    assert_eq!(
        before_abort.reservations[0].phase,
        PaperReservationPhase::Pending,
        "executor dispatch must be preceded by a durable pending reservation"
    );
    second_source_gate.add_permits(1);
    wait_until(|| second_executor.observed.load(Ordering::SeqCst)).await;

    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), first.stop())
            .await
            .expect("the outer shutdown deadline must abort the pending owner")
            .unwrap_err(),
        GridPaperTaskError::ShutdownTimedOut
    ));
    assert_eq!(
        second_executor.calls.load(Ordering::SeqCst),
        0,
        "a queued owner must fail on the active foreign reservation before execution"
    );
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), second.wait())
            .await
            .expect("the queued owner should fail after its lease-held recheck")
            .unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));

    let snapshot = second_account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1, "{snapshot:?}");
    assert_eq!(
        snapshot.reservations[0].task_id,
        "grid:handoff-first/op/000001"
    );
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Uncertain
    );

    let records = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let uncertain_index = records
        .iter()
        .position(|record| record["decision"] == "paper_account_uncertain")
        .expect("the aborted reservation must be durably retained");
    let second_failed_index = records
        .iter()
        .position(|record| {
            record["decision"] == "task_failed"
                && record["details"]["task_id"] == "grid:handoff-second"
        })
        .expect("the queued owner must durably fail closed");
    assert!(
        uncertain_index < second_failed_index,
        "the handoff lease must keep the queued owner blocked until retention is durable"
    );
}

#[derive(Debug, Default)]
struct TimeoutExecutor;

impl GridPaperExecutor for TimeoutExecutor {
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        Box::pin(async { Ok(()) })
    }

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
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        Box::pin(async { Ok(()) })
    }

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
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        Box::pin(async { Ok(()) })
    }

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
        "103",
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

    assert!(matches!(
        task.wait().await.unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
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
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Failed);
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

    // An empty-account upward Sell is an opening short, so all three entry
    // crossings must be refused before any reservation exists. The owner
    // stays alive and completes when the source ends.
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
async fn reservation_failures_cancel_admitted_grid_entries_without_leaking_owner_risk() {
    let (account, history, path) = account_with_available("reserve-after-admit", "50");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FillExecutor::default());
    let first_source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut first = GridPaperTask::start(
        config("grid:reserve-fail:first", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        grid(),
        first_source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();

    let first_error = first.wait().await.unwrap_err();
    assert!(matches!(first_error, GridPaperTaskError::Saga(_)));
    assert_eq!(first.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        first.status().failure,
        Some(GridPaperTaskFailure::AccountContract)
    );

    let second_source = VecSource::new(vec![observation(
        "99",
        2,
        base_time() + Duration::seconds(20),
    )]);
    let mut second = GridPaperTask::start(
        config("grid:reserve-fail:second", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        grid(),
        second_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let second_error = second.wait().await.unwrap_err();
    assert!(matches!(second_error, GridPaperTaskError::Saga(_)));
    assert_eq!(second.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        second.status().failure,
        Some(GridPaperTaskFailure::AccountContract)
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());

    let state = risk.state().await.unwrap();
    assert!(state.open_positions.is_empty());
    assert_eq!(state.admitted_count, 2);
    assert_eq!(state.rejected_count, 0);

    let body = std::fs::read_to_string(path).unwrap();
    assert_eq!(
        body.matches("\"decision\":\"account_risk_admitted\"")
            .count(),
        2,
        "{body}"
    );
    assert_eq!(
        body.matches("\"decision\":\"account_risk_admission_cancelled\"")
            .count(),
        2,
        "{body}"
    );
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
async fn account_risk_without_directive_allows_entry_but_open_eof_requires_recovery() {
    let (account, history, _) = account("account-risk-continue");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FillExecutor::default());
    let source = VecSource::new(vec![observation(
        "99",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:risk-continue", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert!(matches!(
        task.wait().await.unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
}

#[tokio::test]
async fn balance_threshold_sees_the_account_before_its_first_reservation() {
    let (account, history, path) = account("account-risk-initial-balance");
    let risk = account_risk(
        &account,
        &history,
        AccountRiskLimits {
            min_balance_close: Some(Money::new(decimal("500"))),
            ..AccountRiskLimits::default()
        },
    );
    let executor = Arc::new(FillExecutor::default());
    let source = VecSource::new(vec![observation(
        "100",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:risk-initial-balance", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        grid(),
        source,
        account,
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::SourceEnded);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(
        risk.directives(base_time() + Duration::seconds(70))
            .await
            .unwrap()
            .is_empty()
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"paper_account_initialized\""));
    assert!(!body.contains("balance_below_close_threshold"));
}

#[tokio::test]
async fn first_upward_sell_is_an_admitted_short_entry() {
    let (account, history, path) = account("account-risk-first-short");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let source = VecSource::new(vec![observation(
        "101",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:first-short", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert!(matches!(
        task.wait().await.unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![(Side::Sell, decimal("101"), false)]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    assert_eq!(snapshot.open_lots[0].side, Side::Sell);
    assert_eq!(risk.state().await.unwrap().open_positions.len(), 1);
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"risk_scope_id\":\"paper\""), "{body}");
    assert!(
        body.contains("\"risk_admission_ticket_id\":"),
        "the exact admission ticket must be bound into the reservation: {body}"
    );
}

#[tokio::test]
async fn gap_sell_uses_the_same_conservative_touch_for_admission_and_reservation() {
    let (account, history, path) = account("account-risk-gap-short");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let source = VecSource::new(vec![observation_with_book(
        "101.5",
        "102",
        "101",
        1,
        base_time() + Duration::seconds(70),
    )]);
    let mut task = GridPaperTask::start(
        config("grid:gap-short", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    // The short remains open when the finite source ends, so EOF itself is a
    // recovery boundary. The entry must nevertheless have reserved cleanly
    // with the same conservative $101.5 notional that risk admitted.
    assert!(matches!(
        task.wait().await.unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![(Side::Sell, decimal("101"), false)]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1, "{snapshot:?}");
    assert_eq!(
        snapshot.reservations[0].legs[0].reserved_notional(),
        Money::new(decimal("101.5"))
    );
    assert_eq!(risk.state().await.unwrap().admitted_count, 1);
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"notional\":\"101.5\""), "{body}");
    assert!(body.contains("\"risk_scope_id\":\"paper\""), "{body}");
}

#[tokio::test]
async fn kill_switch_closes_existing_long_without_a_new_tick_then_stops() {
    let (account, history, path) = account("account-risk-kill-close");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let source = BlockingSource {
        first: Some(observation("99", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config("grid:risk-kill-close", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    wait_until(|| executor.intents.lock().unwrap().len() == 1).await;
    loop {
        let snapshot = account.snapshot().await.unwrap();
        let state = risk.state().await.unwrap();
        if snapshot.open_lots.len() == 1 && state.open_positions.len() == 1 {
            assert_eq!(snapshot.open_lots[0].side, Side::Buy);
            assert_eq!(snapshot.open_lots[0].remaining_quantity, quantity("1"));
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(75))
        .await
        .unwrap();
    wait_until(|| executor.intents.lock().unwrap().len() == 2).await;

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::StopRequested);
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Buy, decimal("99"), false),
            (Side::Sell, decimal("96.9"), true),
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.open_lots.is_empty(), "{snapshot:?}");
    assert!(snapshot.reservations.is_empty(), "{snapshot:?}");
    let state = risk.state().await.unwrap();
    assert!(state.open_positions.is_empty(), "{state:?}");
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("\"decision\":\"account_risk_directive_exit\"")
    );
    assert_eq!(task.status().operation_count, 2);
    let forced = last_decision(&path, "grid_forced_close_planned");
    assert_eq!(forced["details"]["operation_sequence"], 2);
    assert_eq!(forced["details"]["operation_count"], 2);
}

#[tokio::test]
async fn kill_switch_closes_only_the_grid_instrument_in_a_shared_account() {
    let (account, history, _) = account("account-risk-kill-shared-account");
    seed_open_lot(&account, "ETH-USDT", Side::Buy, "50").await;
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let source = BlockingSource {
        first: Some(observation("99", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config("grid:risk-kill-shared", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            let snapshot = account.snapshot().await.unwrap();
            if snapshot.open_lots.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(75))
        .await
        .unwrap();

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::StopRequested);
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Buy, decimal("99"), false),
            (Side::Sell, decimal("96.9"), true),
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    assert_eq!(
        snapshot.open_lots[0].symbol,
        Symbol::new("ETH-USDT").unwrap()
    );
    assert_eq!(snapshot.open_lots[0].side, Side::Buy);
    assert_eq!(snapshot.open_lots[0].remaining_quantity, quantity("1"));
    assert!(risk.state().await.unwrap().open_positions.is_empty());
}

#[tokio::test]
async fn kill_switch_covers_an_owned_short_at_the_cached_best_ask() {
    let (account, history, _) = account("account-risk-kill-cover-short");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let source = BlockingSource {
        first: Some(observation("101", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config("grid:risk-kill-cover-short", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    wait_until(|| executor.intents.lock().unwrap().len() == 1).await;
    // The entry fill must settle into the account and the risk clock before
    // the kill switch engages; otherwise the drill races the in-flight
    // operation and lands on the fail-closed uncertain-pending path instead
    // of the clean cached-quote cover exercised here.
    loop {
        let snapshot = account.snapshot().await.unwrap();
        let state = risk.state().await.unwrap();
        if snapshot.open_lots.len() == 1 && state.open_positions.len() == 1 {
            assert_eq!(snapshot.open_lots[0].side, Side::Sell);
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(75))
        .await
        .unwrap();

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::StopRequested);
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Sell, decimal("101"), false),
            (Side::Buy, decimal("97.1"), true),
        ]
    );
    assert!(account.snapshot().await.unwrap().open_lots.is_empty());
}

#[tokio::test]
async fn kill_switch_does_not_close_a_foreign_lot_on_the_same_instrument() {
    let (account, history, _) = account("account-risk-foreign-same-instrument");
    seed_open_lot_for_task(
        &account,
        "grid:risk-foreign-flat/op/000002/foreign",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time())
        .await
        .unwrap();
    let executor = Arc::new(RecordingExecutor::default());
    let mut task = GridPaperTask::start(
        config("grid:risk-foreign-flat", StdDuration::from_secs(30)).with_account_risk(risk),
        grid(),
        BlockingSource { first: None },
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap(),
        GridPaperTaskExit::StopRequested
    );
    assert!(executor.intents.lock().unwrap().is_empty());
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    assert_eq!(snapshot.open_lots[0].side, Side::Buy);
}

#[tokio::test]
async fn crossing_fails_closed_before_touching_a_foreign_same_instrument_lot() {
    let (account, history, _) = account("foreign-same-instrument-cross");
    seed_open_lot_for_task(
        &account,
        "grid:foreign-cross/op/000001/nested",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    let executor = Arc::new(RecordingExecutor::default());
    let mut task = GridPaperTask::start(
        config("grid:foreign-cross", StdDuration::from_secs(30)),
        grid(),
        VecSource::new(vec![observation(
            "101",
            1,
            base_time() + Duration::seconds(70),
        )]),
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let error = task.wait().await.unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    assert!(executor.intents.lock().unwrap().is_empty());
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    assert_eq!(snapshot.open_lots[0].side, Side::Buy);
}

#[tokio::test]
async fn kill_switch_fails_closed_when_owned_and_foreign_lots_share_the_instrument() {
    let (account, history, _) = account("account-risk-mixed-owner");
    seed_open_lot_for_task(
        &account,
        "grid:risk-mixed/op/000001",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    seed_open_lot_for_task(
        &account,
        "grid:risk-mixed/op/000002/foreign",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let source = BlockingSource {
        first: Some(observation("100", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config("grid:risk-mixed", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    wait_until(|| task.status().processed_event_count == 1).await;
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(75))
        .await
        .unwrap();

    let error = tokio::time::timeout(StdDuration::from_secs(2), task.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    assert!(executor.intents.lock().unwrap().is_empty());
    assert_eq!(account.snapshot().await.unwrap().open_lots.len(), 2);
}

#[tokio::test]
async fn kill_switch_with_an_owned_lot_and_no_cached_quote_requires_recovery() {
    let (account, history, path) = account("account-risk-no-quote-open");
    seed_open_lot_for_task(
        &account,
        "grid:risk-no-quote/op/000001",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time())
        .await
        .unwrap();
    let executor = Arc::new(RecordingExecutor::default());
    let mut task = GridPaperTask::start(
        config("grid:risk-no-quote", StdDuration::from_secs(30)).with_account_risk(risk),
        grid(),
        BlockingSource { first: None },
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let error = tokio::time::timeout(StdDuration::from_secs(2), task.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    assert!(executor.intents.lock().unwrap().is_empty());
    assert_eq!(account.snapshot().await.unwrap().open_lots.len(), 1);
    assert_eq!(task.status().operation_count, 1);
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"price\":\"unavailable\""), "{body}");
}

#[tokio::test]
async fn risk_timer_without_a_directive_or_cached_quote_keeps_running() {
    let (account, history, _) = account("account-risk-no-quote-continue");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let mut task = GridPaperTask::start(
        config("grid:risk-no-quote-continue", StdDuration::from_secs(30)).with_account_risk(risk),
        grid(),
        BlockingSource { first: None },
        account,
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    tokio::time::sleep(StdDuration::from_millis(600)).await;
    assert_eq!(task.status().phase, GridPaperTaskPhase::Running);
    assert!(executor.intents.lock().unwrap().is_empty());
    assert_eq!(task.stop().await.unwrap(), GridPaperTaskExit::StopRequested);
}

#[tokio::test]
async fn source_end_with_an_owned_open_lot_is_durable_recovery_required() {
    let (account, history, _) = account("source-end-owned-open");
    seed_open_lot_for_task(
        &account,
        "grid:source-end-open/op/000001",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    let mut task = GridPaperTask::start(
        config("grid:source-end-open", StdDuration::from_secs(30)),
        grid(),
        VecSource::new(Vec::new()),
        account.clone(),
        history,
        Arc::new(RecordingExecutor::default()),
    )
    .await
    .unwrap();

    let error = task.wait().await.unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 1);
    assert_eq!(account.snapshot().await.unwrap().open_lots.len(), 1);
}

#[tokio::test]
async fn source_end_ignores_a_prefix_spoofed_foreign_open_lot() {
    let (account, history, _) = account("source-end-foreign-open");
    seed_open_lot_for_task(
        &account,
        "grid:source-end-foreign/op/000001/nested",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    let mut task = GridPaperTask::start(
        config("grid:source-end-foreign", StdDuration::from_secs(30)),
        grid(),
        VecSource::new(Vec::new()),
        account.clone(),
        history,
        Arc::new(RecordingExecutor::default()),
    )
    .await
    .unwrap();

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::SourceEnded);
    assert_eq!(task.status().phase, GridPaperTaskPhase::Stopped);
    assert_eq!(account.snapshot().await.unwrap().open_lots.len(), 1);
}

#[tokio::test]
async fn degraded_account_journal_cannot_drive_a_grid_control_decision() {
    let (account, history, _) = account("degraded-control-decision");
    account.ensure_initialized().await.unwrap();
    let executor = Arc::new(RecordingExecutor::default());
    let release = Arc::new(Semaphore::new(0));
    let source = SteppedSource::new(
        vec![observation("99", 1, base_time() + Duration::seconds(70))],
        Arc::clone(&release),
    );
    let mut task = GridPaperTask::start(
        config("grid:degraded", StdDuration::from_secs(30)),
        grid(),
        source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();

    history
        .append(&DecisionRecord {
            timestamp: base_time() + Duration::seconds(60),
            strategy: "paper_account".to_owned(),
            symbol: "paper-grid".to_owned(),
            decision: "paper_account_reconcile_failed".to_owned(),
            details: json!({ "schema_version": 1 }),
        })
        .await
        .unwrap();
    release.add_permits(1);

    let error = task.wait().await.unwrap_err();
    assert!(matches!(error, GridPaperTaskError::Account(_)), "{error:?}");
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::AccountContract)
    );
    assert!(executor.intents.lock().unwrap().is_empty());
    let diagnostic = account.snapshot().await.unwrap();
    assert_eq!(diagnostic.projection_status, ProjectionStatus::Degraded);
    assert!(diagnostic.reservations.is_empty());
}

#[tokio::test]
async fn virtual_flat_does_not_close_the_risk_clock_while_an_owned_lot_remains() {
    let (account, history, _) = account("actual-position-risk-clock");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::default());
    let release = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("99", 1, base_time() + Duration::seconds(70)),
            observation("100", 2, base_time() + Duration::seconds(80)),
        ],
        Arc::clone(&release),
    );
    let mut task = GridPaperTask::start(
        config("grid:actual-clock", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    loop {
        let snapshot = account.snapshot().await.unwrap();
        let state = risk.state().await.unwrap();
        if executor.intents.lock().unwrap().len() == 1
            && snapshot.open_lots.len() == 1
            && state.open_positions.len() == 1
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    seed_open_lot_for_task(
        &account,
        "grid:actual-clock/op/999999",
        "BTC-USDT",
        Side::Buy,
        "99",
    )
    .await;
    release.add_permits(1);

    assert!(matches!(
        task.wait().await.unwrap_err(),
        GridPaperTaskError::RecoveryRequired
    ));
    assert_eq!(executor.intents.lock().unwrap().len(), 2);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    let state = risk.state().await.unwrap();
    assert_eq!(state.open_positions.len(), 1, "{state:?}");
    assert_eq!(state.open_positions[0].task_id, "grid:actual-clock");
}

#[derive(Debug, Default)]
struct PendingCloseExecutor {
    intents: std::sync::Mutex<Vec<(Side, Decimal, bool)>>,
    calls: AtomicUsize,
}

impl GridPaperExecutor for PendingCloseExecutor {
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        let intent = batch.intents()[0].clone();
        self.intents.lock().unwrap().push((
            intent.side,
            intent.price.unwrap().as_decimal(),
            intent.reduce_only,
        ));
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call_index == 0 {
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
            } else {
                pending::<Result<Vec<TradingReceipt>, RuntimeError>>().await
            }
        })
    }
}

#[derive(Debug, Default)]
struct FailingCloseExecutor {
    intents: std::sync::Mutex<Vec<(Side, Decimal, bool)>>,
    calls: AtomicUsize,
}

impl GridPaperExecutor for FailingCloseExecutor {
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        let intent = batch.intents()[0].clone();
        self.intents.lock().unwrap().push((
            intent.side,
            intent.price.unwrap().as_decimal(),
            intent.reduce_only,
        ));
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call_index == 0 {
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
            } else {
                Err(RuntimeError::InvalidExecutionPolicy(
                    "simulated close failure",
                ))
            }
        })
    }
}

#[tokio::test]
async fn kill_switch_close_failure_fails_closed_with_recovery_required() {
    let (account, history, _) = account("account-risk-kill-close-fail");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FailingCloseExecutor::default());
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("99", 1, base_time() + Duration::seconds(70)),
            observation("101", 2, base_time() + Duration::seconds(80)),
        ],
        Arc::clone(&stepper),
    );
    let mut task = GridPaperTask::start(
        config("grid:risk-kill-close-fail", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    wait_until(|| executor.intents.lock().unwrap().len() == 1).await;
    loop {
        let snapshot = account.snapshot().await.unwrap();
        let state = risk.state().await.unwrap();
        if snapshot.open_lots.len() == 1 && state.open_positions.len() == 1 {
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    stepper.add_permits(1);
    wait_until(|| executor.intents.lock().unwrap().len() == 2).await;

    let error = task.wait().await.unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::RecoveryRequired)
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    let state = risk.state().await.unwrap();
    assert_eq!(state.open_positions.len(), 1, "{state:?}");
}

#[tokio::test]
async fn pending_kill_close_hits_the_owner_deadline_and_retains_uncertain_capacity() {
    let (account, history, path) = account("account-risk-kill-close-pending");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(PendingCloseExecutor::default());
    let source = BlockingSource {
        first: Some(observation("99", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config(
            "grid:risk-kill-close-pending",
            StdDuration::from_millis(250),
        )
        .with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    loop {
        let snapshot = account.snapshot().await.unwrap();
        if executor.calls.load(Ordering::SeqCst) == 1 && snapshot.open_lots.len() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(75))
        .await
        .unwrap();

    let error = tokio::time::timeout(StdDuration::from_secs(2), task.wait())
        .await
        .expect("forced-close deadline must terminate the owner")
        .unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1, "{snapshot:?}");
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation.task_id == "grid:risk-kill-close-pending/op/000002"
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"grid_forced_close_planned\""));
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
}

#[tokio::test]
async fn risk_is_polled_while_an_execution_is_pending_and_retains_uncertain_capacity() {
    let (account, history, path) = account("account-risk-pending-operation");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(PendingExecutor::default());
    let source = BlockingSource {
        first: Some(observation("99", 1, base_time() + Duration::seconds(70))),
    };
    let mut task = GridPaperTask::start(
        config("grid:risk-pending", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    wait_until(|| executor.started.load(Ordering::SeqCst)).await;
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(75))
        .await
        .unwrap();

    let error = tokio::time::timeout(StdDuration::from_secs(2), task.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, GridPaperTaskError::RecoveryRequired));
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1, "{snapshot:?}");
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Uncertain
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
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
    assert!(
        matches!(error, GridPaperTaskError::RecoveryRequired),
        "unexpected timeout error: {error:?}"
    );
    assert_eq!(task.status().phase, GridPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(GridPaperTaskFailure::RecoveryRequired)
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
    // The owner may preserve the unknown reservation before the outer
    // two-grace deadline or the caller may observe that deadline first. Both
    // outcomes are fail-closed and retain the same uncertain capacity.
    assert!(
        matches!(
            error,
            GridPaperTaskError::RecoveryRequired | GridPaperTaskError::ShutdownTimedOut
        ),
        "unexpected cancellation error: {error:?}"
    );
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
    seed_open_lot(&account, "ETH-USDT", Side::Buy, "50").await;
    let source = VecSource::new(Vec::new());
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

    let account_snapshot = account.snapshot().await.unwrap();
    let reservation = account_snapshot.reservations[0].clone();
    let expected_available = Money::new(
        account_snapshot
            .available
            .as_decimal()
            .checked_add(reservation.held_exposure.as_decimal())
            .unwrap(),
    );
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
                    expected_available,
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
        "100",
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
        "100",
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
    assert_eq!(second.status().operation_count, 0);
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.reservations.is_empty());
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
    intents: std::sync::Mutex<Vec<(Side, Decimal, bool)>>,
}

impl GridPaperExecutor for RecordingExecutor {
    fn observe_market(&self, _observation: MarketDataObservation) -> GridPaperObservationFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        let intent = batch.intents()[0].clone();
        self.intents.lock().unwrap().push((
            intent.side,
            intent.price.unwrap().as_decimal(),
            intent.reduce_only,
        ));
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
async fn stop_loss_reset_flattens_the_real_position_before_reanchoring() {
    let (account, history, path) = account("stop-loss-reset");
    let executor = Arc::new(RecordingExecutor::default());
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("99", 1, base_time() + Duration::seconds(10)),
            observation("100", 2, base_time() + Duration::seconds(20)),
            observation("99", 3, base_time() + Duration::seconds(30)),
            observation("94", 4, base_time() + Duration::seconds(40)),
            observation("94", 5, base_time() + Duration::seconds(50)),
        ],
        Arc::clone(&stepper),
    );
    let machine = protection(GridProtectionPolicies {
        stop_loss: Some(StopLossPolicyConfig::new(decimal("100"), 1, decimal("50")).unwrap()),
        ..GridProtectionPolicies::default()
    });
    let mut task = GridPaperTask::start(
        config("grid:stop-loss-reset", StdDuration::from_secs(30)).with_protection(machine),
        grid(),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    for processed in 1..5 {
        wait_until(|| task.status().processed_event_count >= processed).await;
        stepper.add_permits(1);
    }

    assert_eq!(task.wait().await.unwrap(), GridPaperTaskExit::SourceEnded);
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.open_lots.is_empty(), "{snapshot:?}");
    assert!(snapshot.reservations.is_empty(), "{snapshot:?}");
    let intents = executor.intents.lock().unwrap();
    assert!(
        intents
            .last()
            .is_some_and(|(_, _, reduce_only)| *reduce_only),
        "{intents:?}"
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"reset_grid\""), "{body}");
    assert!(
        body.contains("\"reason\":\"stop_loss_apr_recovered\""),
        "{body}"
    );
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
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.open_lots.is_empty(), "{snapshot:?}");
    assert!(snapshot.reservations.is_empty(), "{snapshot:?}");
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("\"decision\":\"exit_all\""));
    assert!(body.contains("\"reason\":\"stop_loss_apr_below_threshold\""));
    assert!(body.contains("\"strategy\":\"grid_protection\""));
    assert_eq!(task.status().operation_count, 6);
    let forced = last_decision(&path, "grid_forced_close_planned");
    assert_eq!(forced["details"]["trigger"], "protection-close");
    assert_eq!(forced["details"]["operation_sequence"], 6);
    assert_eq!(forced["details"]["operation_count"], 6);
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
        vec![
            (Side::Buy, decimal("99"), false),
            (Side::Sell, decimal("100"), true),
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert!(
        snapshot.reservations.is_empty(),
        "completed reverse repost must prune released reservations"
    );
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
