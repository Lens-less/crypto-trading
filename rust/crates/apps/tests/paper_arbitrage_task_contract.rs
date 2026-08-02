use std::{
    collections::BTreeMap,
    future::pending,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_cli::{
    ArbitragePaperExecutionFuture, ArbitragePaperExecutor, ArbitragePaperTask,
    ArbitragePaperTaskConfig, ArbitragePaperTaskError, ArbitragePaperTaskExit,
    ArbitragePaperTaskFailure, ArbitragePaperTaskPhase,
    monitor::{ReadOnlyArbitrageMonitor, ReplayMarketDataClock},
};
use crypto_trading_config::{
    ArbitrageConfig, ArbitrageHistoryDecisionConfig, ArbitrageSymbolConfig,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderStatus, Price, Quantity, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    AccountRiskAuthority, ExecutionBatch, JsonlHistory, MarketDataBook, MarketDataEvent,
    MarketDataEventFuture, MarketDataEventSource, MarketDataObservation, MarketFreshnessPolicy,
    MarketInstrument, MarketSupervisorConfig, MarketUniverse, PaperAccountAuthority,
    PaperAccountConfig, PaperCostModel, PaperReconciliationEvidence, PaperReconciliationProof,
    PaperReservationPhase, ReadOnlyTaskFailure, ReadOnlyTaskKind, ReadOnlyTaskPhase,
    ReadOnlyTaskRecovery, SpreadHistoryRecord, SpreadHistoryWriter,
};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy};
use rust_decimal::Decimal;
use tokio::sync::{Semaphore, mpsc};

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

fn symbol() -> Symbol {
    Symbol::new("BTC-USDT").unwrap()
}

fn instrument(exchange: &str) -> MarketInstrument {
    MarketInstrument::new(exchange, symbol(), MarketType::Perpetual).unwrap()
}

fn monitor() -> ReadOnlyArbitrageMonitor {
    monitor_for("left", "right")
}

fn monitor_for(left_exchange: &str, right_exchange: &str) -> ReadOnlyArbitrageMonitor {
    let left = instrument(left_exchange);
    let right = instrument(right_exchange);
    let universe = MarketUniverse::new(vec![left.clone(), right.clone()]).unwrap();
    let book = MarketDataBook::new(
        universe,
        MarketFreshnessPolicy::new(Duration::minutes(5), Duration::seconds(1))
            .unwrap()
            .with_max_pair_skew(Duration::minutes(5))
            .unwrap(),
        Arc::new(ReplayMarketDataClock::new(
            base_time() + Duration::minutes(1),
        )),
    );
    ReadOnlyArbitrageMonitor::new(book, left, right, decimal("1")).unwrap()
}

fn strategy_config() -> ArbitrageConfig {
    let symbol = symbol();
    let mut symbol_configs = BTreeMap::new();
    symbol_configs.insert(
        symbol.clone(),
        ArbitrageSymbolConfig {
            enabled: true,
            min_spread_pct: None,
            grid_step_pct: None,
            max_segments: None,
            base_quantity: None,
            max_position_value: None,
        },
    );
    ArbitrageConfig {
        mode: "segmented".to_owned(),
        monitor_only: false,
        enabled: true,
        exchanges: vec!["left".to_owned(), "right".to_owned()],
        symbols: vec![symbol],
        min_spread_pct: decimal("1"),
        base_quantity: quantity("1"),
        grid_step_pct: decimal("1"),
        max_segments: 5,
        first_close_ratio: decimal("0.5"),
        max_position_value: Some(decimal("10000")),
        symbol_configs,
        history_decision: None,
    }
}

fn task_config(task_id: &str, grace: StdDuration) -> ArbitragePaperTaskConfig {
    task_config_with_max(task_id, grace, "10000")
}

fn task_config_with_max(
    task_id: &str,
    grace: StdDuration,
    max_position_value: &str,
) -> ArbitragePaperTaskConfig {
    let mut strategy = strategy_config();
    strategy.max_position_value = Some(decimal(max_position_value));
    ArbitragePaperTaskConfig::new(
        task_id,
        &strategy,
        Duration::minutes(5),
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
        PaperAccountConfig::new("paper-arbitrage", Money::new(decimal("100000"))).unwrap(),
    )
    .unwrap();
    (account, history, path)
}

fn observation(
    exchange: &str,
    bid: &str,
    ask: &str,
    revision: u64,
    at: chrono::DateTime<Utc>,
) -> MarketDataEvent {
    observation_with_depth(exchange, bid, ask, revision, at, "20")
}

fn observation_with_depth(
    exchange: &str,
    bid: &str,
    ask: &str,
    revision: u64,
    at: chrono::DateTime<Utc>,
    depth: &str,
) -> MarketDataEvent {
    let mut snapshot = MarketSnapshot::new(
        exchange,
        symbol(),
        MarketType::Perpetual,
        price(bid),
        price(ask),
        at,
    )
    .unwrap();
    snapshot.bid_quantity = Some(quantity(depth));
    snapshot.ask_quantity = Some(quantity(depth));
    MarketDataEvent::Observation(MarketDataObservation::new(snapshot, revision, at).unwrap())
}

#[derive(Debug)]
struct ChannelSource {
    source_id: String,
    receiver: mpsc::Receiver<MarketDataEvent>,
}

impl ChannelSource {
    fn new(source_id: &str) -> (Self, mpsc::Sender<MarketDataEvent>) {
        let (sender, receiver) = mpsc::channel(16);
        (
            Self {
                source_id: source_id.to_owned(),
                receiver,
            },
            sender,
        )
    }
}

impl MarketDataEventSource for ChannelSource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(async move { Ok(self.receiver.recv().await) })
    }
}

#[derive(Debug, Default)]
struct FillExecutor {
    calls: AtomicUsize,
}

#[derive(Debug)]
struct GatedFillExecutor {
    calls: AtomicUsize,
    permits: Arc<Semaphore>,
}

impl Default for GatedFillExecutor {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            permits: Arc::new(Semaphore::new(0)),
        }
    }
}

impl GatedFillExecutor {
    fn release_one(&self) {
        self.permits.add_permits(1);
    }
}

impl ArbitragePaperExecutor for GatedFillExecutor {
    fn execute(&self, batch: ExecutionBatch) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let permits = Arc::clone(&self.permits);
        Box::pin(async move {
            permits.acquire_owned().await.unwrap().forget();
            Ok(batch
                .intents()
                .iter()
                .enumerate()
                .map(|(index, intent)| TradingReceipt::Submitted {
                    order: Order {
                        id: format!("paper-{index}-{}", intent.client_order_id),
                        intent: intent.clone(),
                        filled_quantity: intent.quantity,
                        average_fill_price: intent.price,
                        status: OrderStatus::Filled,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                    },
                    disposition: SubmissionDisposition::Filled,
                })
                .collect())
        })
    }
}

#[derive(Debug, Default)]
struct PendingExecutor {
    calls: AtomicUsize,
}

impl ArbitragePaperExecutor for PendingExecutor {
    fn execute(&self, _batch: ExecutionBatch) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(pending())
    }
}

impl ArbitragePaperExecutor for FillExecutor {
    fn execute(&self, batch: ExecutionBatch) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(batch
                .intents()
                .iter()
                .enumerate()
                .map(|(index, intent)| TradingReceipt::Submitted {
                    order: Order {
                        id: format!("paper-{index}-{}", intent.client_order_id),
                        intent: intent.clone(),
                        filled_quantity: intent.quantity,
                        average_fill_price: intent.price,
                        status: OrderStatus::Filled,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                    },
                    disposition: SubmissionDisposition::Filled,
                })
                .collect())
        })
    }
}

#[tokio::test]
async fn exact_pair_opportunity_commits_one_independent_two_leg_operation() {
    let (account, history, _) = account("success");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:btc", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    assert_eq!(
        task.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(snapshot.reservations[0].task_id, "arbitrage:btc/op/000001");
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Committed
    );
    assert_eq!(snapshot.reservations[0].legs.len(), 2);
    let durable = task.durable_status().await.unwrap();
    assert_eq!(durable.kind, ReadOnlyTaskKind::ArbitragePaper);
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(durable.sources.len(), 2);
}

#[tokio::test]
async fn risk_rejection_happens_before_any_reservation() {
    let (account, history, path) = account("risk-rejected");
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config_with_max("arbitrage:risk", StdDuration::from_secs(30), "50"),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();

    assert!(matches!(
        task.wait().await.unwrap_err(),
        ArbitragePaperTaskError::RiskRejected(_)
    ));
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    assert!(
        !std::fs::read_to_string(path)
            .unwrap()
            .contains("\"decision\":\"paper_account_reserved\"")
    );
}

fn account_risk_authority(
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
async fn account_risk_pause_skips_opportunities_without_failing_the_owner() {
    let (account, history, path) = account("account-risk-paused");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    risk.pause("exchange maintenance", base_time())
        .await
        .unwrap();
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:risk-paused", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    // The paused authority records a durable rejection; the opportunity is
    // skipped without any reservation and the owner keeps running.
    wait_until(|| {
        std::fs::read_to_string(&path)
            .is_ok_and(|body| body.contains("\"decision\":\"account_risk_rejected\""))
    })
    .await;
    drop(left_sender);
    drop(right_sender);
    assert_eq!(
        task.wait().await.unwrap(),
        ArbitragePaperTaskExit::SourceEnded
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let state = risk.state().await.unwrap();
    assert_eq!(state.rejected_count, 1);
    assert_eq!(state.last_rejection.as_deref(), Some("paused"));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_rejected\""));
    assert!(!body.contains("\"decision\":\"paper_account_reserved\""));
}

#[tokio::test]
async fn engaged_kill_switch_stops_the_arbitrage_owner_before_any_entry() {
    let (account, history, path) = account("account-risk-kill");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time())
        .await
        .unwrap();
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, _right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:risk-kill", StdDuration::from_secs(30)).with_account_risk(risk),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();

    assert_eq!(
        task.wait().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("kill_switch:operator drill"));
}

#[tokio::test]
async fn liquidity_rejection_happens_before_any_reservation() {
    let (account, history, path) = account("liquidity-rejected");
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:liquidity", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    left_sender
        .send(observation_with_depth(
            "left",
            "99",
            "100",
            1,
            base_time(),
            "0.5",
        ))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();

    assert!(matches!(
        task.wait().await.unwrap_err(),
        ArbitragePaperTaskError::LiquidityRejected
    ));
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    assert!(
        !std::fs::read_to_string(path)
            .unwrap()
            .contains("\"decision\":\"paper_account_reserved\"")
    );
}

#[tokio::test]
async fn stop_drains_the_inflight_pair_without_admitting_a_coalesced_reservation() {
    let (account, history, _) = account("stop-inflight");
    let executor = Arc::new(GatedFillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:stop", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
    right_sender
        .send(observation(
            "right",
            "103",
            "103.5",
            2,
            base_time() + Duration::seconds(2),
        ))
        .await
        .unwrap();
    wait_until(|| task.status().processed_event_count >= 3).await;

    let (stop, ()) = tokio::join!(task.stop(), async {
        tokio::time::sleep(StdDuration::from_millis(25)).await;
        executor.release_one();
    });

    assert_eq!(stop.unwrap(), ArbitragePaperTaskExit::StopRequested);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Committed
    );
}

#[tokio::test]
async fn cancel_retains_unknown_two_leg_capacity_as_uncertain_and_never_releases_it() {
    let (account, history, path) = account("cancel-unknown");
    let executor = Arc::new(PendingExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:cancel", StdDuration::from_millis(250)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    // Whether the owner notices the cancel signal before the 2x-grace
    // deadline decides which error variant surfaces; both paths retain the
    // capacity as uncertain and record the same durable failure, so either
    // variant satisfies the contract.
    assert!(matches!(
        task.cancel().await.unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired | ArbitragePaperTaskError::ShutdownTimedOut
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
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
async fn stop_timeout_is_durably_recovery_required_and_retains_pending_capacity() {
    let (account, history, path) = account("stop-timeout");
    let executor = Arc::new(PendingExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:timeout", StdDuration::from_millis(50)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    assert!(matches!(
        task.stop().await.unwrap_err(),
        ArbitragePaperTaskError::ShutdownTimedOut
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Uncertain
    );
    let durable = task.durable_status().await.unwrap();
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Failed);
    assert_eq!(durable.failure, Some(ReadOnlyTaskFailure::RecoveryRequired));
    assert_eq!(durable.recovery, ReadOnlyTaskRecovery::Investigate);
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
    assert!(!body.contains("\"decision\":\"paper_account_released\""));
    let records_before = body.lines().count();

    let (restart_left, _restart_left_sender) = ChannelSource::new("left");
    let (restart_right, _restart_right_sender) = ChannelSource::new("right");
    let restart = ArbitragePaperTask::start(
        task_config("arbitrage:timeout", StdDuration::from_millis(50)),
        monitor(),
        restart_left,
        restart_right,
        account,
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();
    assert!(matches!(restart, ArbitragePaperTaskError::RecoveryRequired));
    assert_eq!(
        std::fs::read_to_string(path).unwrap().lines().count(),
        records_before
    );
}

#[tokio::test]
async fn failed_reconciliation_blocks_a_new_owner_before_registration() {
    let (account, history, path) = account("failed-reconcile");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut first = ArbitragePaperTask::start(
        task_config("arbitrage:reconcile", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();
    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
    first.stop().await.unwrap();

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
                    "paper-arbitrage",
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
    let (new_left, _new_left_sender) = ChannelSource::new("left");
    let (new_right, _new_right_sender) = ChannelSource::new("right");

    let restart = ArbitragePaperTask::start(
        task_config("arbitrage:reconcile", StdDuration::from_secs(30)),
        monitor(),
        new_left,
        new_right,
        account,
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();

    assert!(matches!(restart, ArbitragePaperTaskError::RecoveryRequired));
    assert_eq!(
        std::fs::read_to_string(path).unwrap().lines().count(),
        records_before,
        "failed preflight must not append task registration"
    );
}

#[tokio::test]
async fn source_mismatch_is_zero_write_and_clean_stable_owner_restart_remains_projectable() {
    let (mismatch_account, mismatch_history, mismatch_path) = account("source-mismatch");
    let (wrong_left, _wrong_left_sender) = ChannelSource::new("right");
    let (right, _right_sender) = ChannelSource::new("right");
    let mismatch = ArbitragePaperTask::start(
        task_config("arbitrage:mismatch", StdDuration::from_secs(30)),
        monitor(),
        wrong_left,
        right,
        mismatch_account,
        mismatch_history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        mismatch,
        ArbitragePaperTaskError::InvalidSourceBinding
    ));
    assert!(!mismatch_path.exists());

    let (account, history, _) = account("clean-restart");
    let (first_left, _first_left_sender) = ChannelSource::new("left");
    let (first_right, _first_right_sender) = ChannelSource::new("right");
    let mut first = ArbitragePaperTask::start(
        task_config("arbitrage:restart", StdDuration::from_secs(30)),
        monitor(),
        first_left,
        first_right,
        account.clone(),
        history.clone(),
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    assert_eq!(first.status().task_id, "arbitrage:restart");
    first.stop().await.unwrap();

    let records_before_drift = std::fs::read_to_string(history.path())
        .unwrap()
        .lines()
        .count();
    let (drift_left, _drift_left_sender) = ChannelSource::new("drift-left");
    let (drift_right, _drift_right_sender) = ChannelSource::new("right");
    let drift = ArbitragePaperTask::start(
        task_config("arbitrage:restart", StdDuration::from_secs(30)),
        monitor_for("drift-left", "right"),
        drift_left,
        drift_right,
        account.clone(),
        history.clone(),
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();
    assert!(matches!(drift, ArbitragePaperTaskError::RecoveryRequired));
    assert_eq!(
        std::fs::read_to_string(history.path())
            .unwrap()
            .lines()
            .count(),
        records_before_drift,
        "restart source drift must fail before registration"
    );

    let (second_left, _second_left_sender) = ChannelSource::new("left");
    let (second_right, _second_right_sender) = ChannelSource::new("right");
    let mut second = ArbitragePaperTask::start(
        task_config("arbitrage:restart", StdDuration::from_secs(30)),
        monitor(),
        second_left,
        second_right,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    second.stop().await.unwrap();

    let durable = second.durable_status().await.unwrap();
    assert_eq!(second.status().task_id, "arbitrage:restart");
    assert_eq!(durable.task_id, "arbitrage:restart");
    assert_eq!(durable.kind, ReadOnlyTaskKind::ArbitragePaper);
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(durable.sources.len(), 2);
    assert_eq!(durable.sources[0].source_id, "left");
    assert_eq!(durable.sources[1].source_id, "right");
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
}

#[tokio::test]
async fn inflight_opportunities_coalesce_into_one_latest_pair_re_evaluation() {
    let (account, history, _) = account("coalesce");
    let executor = Arc::new(GatedFillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:coalesce", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    for (index, (revision, bid, ask)) in [
        (2, "102.5", "103"),
        (3, "103.5", "104"),
        (4, "104.5", "105"),
    ]
    .into_iter()
    .enumerate()
    {
        right_sender
            .send(observation(
                "right",
                bid,
                ask,
                revision,
                base_time() + Duration::seconds(i64::try_from(revision).unwrap()),
            ))
            .await
            .unwrap();
        wait_until(|| task.status().processed_event_count >= 3 + u64::try_from(index).unwrap())
            .await;
    }
    assert_eq!(task.status().coalesced_opportunity_count, 3);

    executor.release_one();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 2).await;
    tokio::task::yield_now().await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    executor.release_one();
    assert_eq!(
        task.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );

    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 2);
    assert!(snapshot.reservations.iter().all(|reservation| {
        reservation.phase == PaperReservationPhase::Committed && reservation.legs.len() == 2
    }));
    assert_eq!(
        snapshot
            .reservations
            .iter()
            .map(|reservation| reservation.task_id.as_str())
            .collect::<Vec<_>>(),
        [
            "arbitrage:coalesce/op/000001",
            "arbitrage:coalesce/op/000002",
        ]
    );
}

fn history_task_config(
    task_id: &str,
    min_samples: u32,
    spread_history_path: Option<&std::path::Path>,
) -> ArbitragePaperTaskConfig {
    let mut strategy = strategy_config();
    strategy.history_decision = Some(ArbitrageHistoryDecisionConfig {
        enabled: true,
        window_seconds: 3_600,
        min_samples,
        deviation_threshold_bps: decimal("1"),
        funding_rate_annual_threshold_pct: decimal("10"),
        spread_history_path: spread_history_path.map(|path| path.to_string_lossy().into_owned()),
    });
    ArbitragePaperTaskConfig::new(
        task_id,
        &strategy,
        Duration::minutes(5),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        MarketSupervisorConfig::new(StdDuration::from_secs(30)).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn history_mode_holds_orders_on_insufficient_history_then_opens_after_enough_samples() {
    let (account, history, _) = account("history-mode");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        history_task_config("arbitrage:history", 3, None),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    // Two spread observations (150 bps, 160 bps): fewer than min_samples=3
    // same-direction samples, so InsufficientHistory blocks any order.
    right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| task.status().processed_event_count >= 2).await;
    right_sender
        .send(observation(
            "right",
            "101.6",
            "102",
            2,
            base_time() + Duration::seconds(2),
        ))
        .await
        .unwrap();
    wait_until(|| task.status().processed_event_count >= 3).await;
    tokio::task::yield_now().await;
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        0,
        "insufficient history must not place any order"
    );

    // Third observation: window now holds three samples with median 160 bps;
    // the 400 bps spread deviates by 240 bps >= 1 bps, so the gate opens.
    right_sender
        .send(observation(
            "right",
            "104",
            "104.5",
            3,
            base_time() + Duration::seconds(3),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    assert_eq!(
        task.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(
        snapshot.reservations[0].task_id,
        "arbitrage:history/op/000001"
    );
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Committed
    );
}

#[tokio::test]
async fn history_mode_backfills_the_sample_buffer_from_the_spread_history_journal() {
    let spread_history_path = temp_path("history-backfill-spread");
    let writer = SpreadHistoryWriter::new(&spread_history_path);
    // Three persisted same-direction samples inside the window: the machine
    // starts warm and the first live opportunity may open immediately.
    writer
        .append_batch(
            &[(-180i64, "150"), (-120, "155"), (-60, "160")]
                .into_iter()
                .map(|(offset, spread_bps)| SpreadHistoryRecord {
                    timestamp: base_time() + Duration::seconds(offset),
                    symbol: "BTC-USDT".to_owned(),
                    exchange_buy: "left".to_owned(),
                    exchange_sell: "right".to_owned(),
                    price_buy: "100".to_owned(),
                    price_sell: "101.5".to_owned(),
                    spread_bps: spread_bps.to_owned(),
                    funding_rate_buy: None,
                    funding_rate_sell: None,
                    funding_rate_diff: None,
                    funding_rate_diff_annual_pct: None,
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();

    let (account, history, _) = account("history-backfill");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        history_task_config("arbitrage:backfill", 3, Some(&spread_history_path)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    // One live opportunity at 400 bps against the backfilled natural spread
    // of 155 bps is enough: no warm-up phase is required after a restart.
    right_sender
        .send(observation(
            "right",
            "104",
            "104.5",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    assert_eq!(
        task.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    std::fs::remove_file(spread_history_path).unwrap();
}

#[tokio::test]
async fn history_mode_fails_closed_on_a_corrupted_spread_history_journal() {
    let spread_history_path = temp_path("history-corrupt-spread");
    std::fs::write(&spread_history_path, b"this-is-not-json\n").unwrap();

    let (account, history, path) = account("history-corrupt");
    let (left_source, _left_sender) = ChannelSource::new("left");
    let (right_source, _right_sender) = ChannelSource::new("right");
    let error = ArbitragePaperTask::start(
        history_task_config("arbitrage:corrupt", 3, Some(&spread_history_path)),
        monitor(),
        left_source,
        right_source,
        account,
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ArbitragePaperTaskError::Projection(_)));
    assert!(
        !path.exists(),
        "a corrupted spread history must fail before task registration"
    );
    std::fs::remove_file(spread_history_path).unwrap();
}

async fn wait_until(predicate: impl Fn() -> bool) {
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if predicate() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn temp_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-arbitrage-owner-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}
