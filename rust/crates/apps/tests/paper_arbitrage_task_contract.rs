use std::{
    collections::BTreeMap,
    future::pending,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_cli::{
    ArbitragePaperExecutionFuture, ArbitragePaperExecutor, ArbitragePaperMarketEventFuture,
    ArbitragePaperTask, ArbitragePaperTaskConfig, ArbitragePaperTaskError, ArbitragePaperTaskExit,
    ArbitragePaperTaskFailure, ArbitragePaperTaskPhase, DurablePaperArbitrageSaga,
    PaperArbitrageRequest,
    monitor::{ReadOnlyArbitrageMonitor, ReplayMarketDataClock},
};
use crypto_trading_config::{
    ArbitrageConfig, ArbitrageHistoryDecisionConfig, ArbitrageSymbolConfig,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderIntent, OrderStatus, Price, Quantity, Side,
    Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    AccountRiskAuthority, DecisionRecord, ExecutionBatch, JsonlHistory, MarketDataBook,
    MarketDataEvent, MarketDataEventFuture, MarketDataEventSource, MarketDataObservation,
    MarketFreshnessPolicy, MarketInstrument, MarketSupervisorConfig, MarketUniverse,
    ObservedMarketPair, PaperAccountAuthority, PaperAccountConfig, PaperCostModel,
    PaperReconciliationEvidence, PaperReconciliationProof, PaperReservationLeg,
    PaperReservationPhase, PaperReservationRequest, ReadOnlyTaskFailure, ReadOnlyTaskKind,
    ReadOnlyTaskPhase, ReadOnlyTaskRecovery, SpreadHistoryRecord, SpreadHistoryWriter,
};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy};
use rust_decimal::Decimal;
use serde_json::json;
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
    account_with_available(label, "100000")
}

fn account_with_available(
    label: &str,
    available: &str,
) -> (PaperAccountAuthority, JsonlHistory, std::path::PathBuf) {
    let path = temp_path(label);
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new("paper-arbitrage", Money::new(decimal(available))).unwrap(),
    )
    .unwrap();
    (account, history, path)
}

async fn seed_open_pair(task_id: &str, account: &PaperAccountAuthority, history: &JsonlHistory) {
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config(task_id, StdDuration::from_secs(30)),
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
    assert_eq!(
        task.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
}

async fn seed_legacy_open_pair_without_owner_lease(
    task_id: &str,
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
) {
    let intents = vec![
        OrderIntent::limit(
            "left",
            symbol(),
            MarketType::Perpetual,
            Side::Buy,
            quantity("1"),
            price("100"),
        ),
        OrderIntent::limit(
            "right",
            symbol(),
            MarketType::Perpetual,
            Side::Sell,
            quantity("1"),
            price("101.5"),
        ),
    ];
    let batch = ExecutionBatch::planned(intents).unwrap();
    let legs = batch
        .intents()
        .iter()
        .enumerate()
        .map(|(index, intent)| {
            let notional = intent
                .price
                .unwrap()
                .as_decimal()
                .checked_mul(intent.quantity.as_decimal())
                .map(Money::new)
                .unwrap();
            PaperReservationLeg::from_intent(index, intent, notional).unwrap()
        })
        .collect();
    let reservation = PaperReservationRequest::planned(
        task_id,
        format!("legacy-seed:{task_id}"),
        batch.id(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        legs,
    )
    .unwrap();
    let request = PaperArbitrageRequest::new(symbol(), batch, reservation).unwrap();
    DurablePaperArbitrageSaga::new(account.clone(), history.clone())
        .unwrap()
        .run(request, |batch| async move {
            Ok(filled_receipts(batch.intents().iter().enumerate()))
        })
        .await
        .unwrap();
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

async fn send_open_opportunity(
    left: &mpsc::Sender<MarketDataEvent>,
    right: &mpsc::Sender<MarketDataEvent>,
) {
    left.send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    right
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
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

fn observed_snapshot(event: &MarketDataEvent) -> MarketSnapshot {
    match event {
        MarketDataEvent::Observation(observation) => observation.snapshot.clone(),
        MarketDataEvent::SourceGap { .. } | MarketDataEvent::SourceUnavailable { .. } => {
            panic!("expected a market observation")
        }
    }
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

fn filled_receipts<'a>(
    intents: impl IntoIterator<Item = (usize, &'a crypto_trading_domain::OrderIntent)>,
) -> Vec<TradingReceipt> {
    intents
        .into_iter()
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
        .collect()
}

#[derive(Debug, Default)]
struct FillExecutor {
    calls: AtomicUsize,
    pairs: Mutex<Vec<ObservedMarketPair>>,
}

#[derive(Debug)]
struct GatedFillExecutor {
    calls: AtomicUsize,
    permits: Arc<Semaphore>,
    pairs: Arc<Mutex<Vec<ObservedMarketPair>>>,
    intents: Arc<Mutex<Vec<Vec<crypto_trading_domain::OrderIntent>>>>,
}

impl Default for GatedFillExecutor {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            permits: Arc::new(Semaphore::new(0)),
            pairs: Arc::new(Mutex::new(Vec::new())),
            intents: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl GatedFillExecutor {
    fn release_one(&self) {
        self.permits.add_permits(1);
    }

    fn recorded_intents(&self) -> Vec<Vec<crypto_trading_domain::OrderIntent>> {
        self.intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ArbitragePaperExecutor for GatedFillExecutor {
    fn observe_market_event(&self, _event: MarketDataEvent) -> ArbitragePaperMarketEventFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(
        &self,
        batch: ExecutionBatch,
        pair: ObservedMarketPair,
    ) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let permits = Arc::clone(&self.permits);
        let pairs = Arc::clone(&self.pairs);
        let intents = Arc::clone(&self.intents);
        Box::pin(async move {
            permits.acquire_owned().await.unwrap().forget();
            pairs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(pair);
            intents
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(batch.intents().to_vec());
            Ok(filled_receipts(batch.intents().iter().enumerate()))
        })
    }
}

#[derive(Debug, Default)]
struct PendingExecutor {
    calls: AtomicUsize,
    pairs: Mutex<Vec<ObservedMarketPair>>,
}

impl ArbitragePaperExecutor for PendingExecutor {
    fn observe_market_event(&self, _event: MarketDataEvent) -> ArbitragePaperMarketEventFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(
        &self,
        _batch: ExecutionBatch,
        pair: ObservedMarketPair,
    ) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.pairs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(pair);
        Box::pin(pending())
    }
}

impl ArbitragePaperExecutor for FillExecutor {
    fn observe_market_event(&self, _event: MarketDataEvent) -> ArbitragePaperMarketEventFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(
        &self,
        batch: ExecutionBatch,
        pair: ObservedMarketPair,
    ) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.pairs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(pair);
        Box::pin(async move { Ok(filled_receipts(batch.intents().iter().enumerate())) })
    }
}

#[derive(Debug, Clone, Copy)]
enum ScriptedOutcome {
    FillAll,
    FillCount(usize),
}

#[derive(Debug)]
struct ScriptedExecutor {
    calls: AtomicUsize,
    outcomes: Mutex<Vec<ScriptedOutcome>>,
    intents: Mutex<Vec<Vec<crypto_trading_domain::OrderIntent>>>,
}

impl ScriptedExecutor {
    fn new(outcomes: Vec<ScriptedOutcome>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            outcomes: Mutex::new(outcomes),
            intents: Mutex::new(Vec::new()),
        }
    }

    fn recorded_intents(&self) -> Vec<Vec<crypto_trading_domain::OrderIntent>> {
        self.intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ArbitragePaperExecutor for ScriptedExecutor {
    fn observe_market_event(&self, _event: MarketDataEvent) -> ArbitragePaperMarketEventFuture {
        Box::pin(async { Ok(()) })
    }

    fn execute(
        &self,
        batch: ExecutionBatch,
        pair: ObservedMarketPair,
    ) -> ArbitragePaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(0);
        let recorded = batch.intents().to_vec();
        self.intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(recorded);
        let _ = pair;
        Box::pin(async move {
            let receipts = match outcome {
                ScriptedOutcome::FillAll => filled_receipts(batch.intents().iter().enumerate()),
                ScriptedOutcome::FillCount(count) => {
                    filled_receipts(batch.intents().iter().enumerate().take(count))
                }
            };
            Ok(receipts)
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

fn assert_open_admission_binding_and_unbound_forced_close(body: &str) {
    let records = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let reservations = records
        .iter()
        .filter(|record| record["decision"] == "paper_account_reserved")
        .collect::<Vec<_>>();
    assert_eq!(reservations.len(), 2);
    assert_eq!(
        reservations[0]["details"]["request"]["risk_scope_id"],
        "paper"
    );
    assert!(
        reservations[0]["details"]["request"]["risk_admission_ticket_id"]
            .as_str()
            .is_some_and(|ticket| !ticket.is_empty())
    );
    assert!(
        reservations[1]["details"]["request"]["risk_scope_id"].is_null(),
        "the forced reduce-only close must not consume a new risk admission"
    );
    assert!(
        reservations[1]["details"]["request"]["risk_admission_ticket_id"].is_null(),
        "the forced reduce-only close must remain unbound"
    );
    let stopped_index = records
        .iter()
        .rposition(|record| record["decision"] == "task_stopped")
        .unwrap();
    let forced_close_index = records
        .iter()
        .rposition(|record| record["decision"] == "paper_account_reserved")
        .unwrap();
    assert!(forced_close_index < stopped_index);
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
async fn reservation_rejection_cancels_the_previously_admitted_account_risk_ticket() {
    let (account, history, path) = account_with_available("admission-cancelled", "1");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:admission-cancelled", StdDuration::from_secs(30))
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

    assert!(matches!(
        task.wait().await.unwrap_err(),
        ArbitragePaperTaskError::Saga(_)
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let risk_state = risk.state().await.unwrap();
    assert_eq!(risk_state.admitted_count, 1);
    assert!(risk_state.open_positions.is_empty());
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_admitted\""));
    assert!(body.contains("\"decision\":\"account_risk_admission_cancelled\""));
}

#[tokio::test]
async fn engaged_kill_switch_stops_the_arbitrage_owner_before_any_entry() {
    let (account, history, path) = account("account-risk-kill");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time())
        .await
        .unwrap();
    let executor = Arc::new(FillExecutor::default());
    let (left_source, _left_sender) = ChannelSource::new("left");
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

    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("kill_switch:operator drill"));
}

#[tokio::test]
async fn kill_switch_during_a_blocked_open_reprojects_and_closes_the_raced_position() {
    let (account, history, path) = account("account-risk-kill-raced-open");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(GatedFillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:risk-raced-open", StdDuration::from_secs(30))
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
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
    assert!(
        account.snapshot().await.unwrap().open_lots.is_empty(),
        "the owner state is still flat while the opening execution is blocked"
    );

    risk.engage_kill_switch("raced open drill", base_time() + Duration::seconds(2))
        .await
        .unwrap();
    wait_until(|| {
        std::fs::read_to_string(&path).is_ok_and(|body| {
            body.contains("\"decision\":\"account_risk_directive_exit\"")
                && body.contains("kill_switch:raced open drill")
        })
    })
    .await;

    executor.release_one();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 2).await;
    executor.release_one();

    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Stopped);
    assert_eq!(task.status().operation_count, 2);
    let recorded = executor.recorded_intents();
    assert_eq!(recorded.len(), 2);
    assert!(recorded[0].iter().all(|intent| !intent.reduce_only));
    assert!(recorded[1].iter().all(|intent| intent.reduce_only));
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.reservations.is_empty());
    assert!(snapshot.open_lots.is_empty());

    let body = std::fs::read_to_string(path).unwrap();
    assert!(
        body.rfind("\"decision\":\"paper_account_reserved\"")
            .unwrap()
            < body.rfind("\"decision\":\"task_stopped\"").unwrap()
    );
}

#[tokio::test]
async fn kill_switch_treats_lookalike_foreign_pair_lots_as_flat_and_never_closes_them() {
    let (account, history, _) = account("account-risk-foreign-lots");
    let foreign_executor = Arc::new(FillExecutor::default());
    let (foreign_left, foreign_left_sender) = ChannelSource::new("left");
    let (foreign_right, foreign_right_sender) = ChannelSource::new("right");
    let mut foreign = ArbitragePaperTask::start(
        task_config("arbitrage:isolated/op/foreign", StdDuration::from_secs(30)),
        monitor(),
        foreign_left,
        foreign_right,
        account.clone(),
        history.clone(),
        foreign_executor.clone(),
    )
    .await
    .unwrap();
    foreign_left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    foreign_right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| foreign_executor.calls.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        foreign.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let before = account.snapshot().await.unwrap();
    assert_eq!(before.open_lots.len(), 2);

    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let isolated_executor = Arc::new(FillExecutor::default());
    let (isolated_left, isolated_left_sender) = ChannelSource::new("left");
    let (isolated_right, isolated_right_sender) = ChannelSource::new("right");
    let mut isolated = ArbitragePaperTask::start(
        task_config("arbitrage:isolated", StdDuration::from_secs(30))
            .with_account_risk(risk.clone()),
        monitor(),
        isolated_left,
        isolated_right,
        account.clone(),
        history,
        isolated_executor.clone(),
    )
    .await
    .unwrap();
    isolated_left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    isolated_right_sender
        .send(observation(
            "right",
            "100",
            "101",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| isolated.status().processed_event_count == 2).await;

    risk.engage_kill_switch(
        "foreign isolation drill",
        base_time() + Duration::seconds(2),
    )
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), isolated.wait())
            .await
            .unwrap()
            .unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(isolated_executor.calls.load(Ordering::SeqCst), 0);
    let after = account.snapshot().await.unwrap();
    assert_eq!(after.open_lots, before.open_lots);
}

#[tokio::test]
async fn ordinary_operation_refuses_to_fifo_net_a_foreign_reverse_pair() {
    let (account, history, path) = account("foreign-reverse-fifo-isolation");
    let foreign_executor = Arc::new(FillExecutor::default());
    let (foreign_left, foreign_left_sender) = ChannelSource::new("left");
    let (foreign_right, foreign_right_sender) = ChannelSource::new("right");
    let mut foreign = ArbitragePaperTask::start(
        task_config("arbitrage:foreign-reverse", StdDuration::from_secs(30)),
        monitor(),
        foreign_left,
        foreign_right,
        account.clone(),
        history.clone(),
        foreign_executor.clone(),
    )
    .await
    .unwrap();
    foreign_left_sender
        .send(observation("left", "101.5", "102", 1, base_time()))
        .await
        .unwrap();
    foreign_right_sender
        .send(observation(
            "right",
            "99",
            "100",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| foreign_executor.calls.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        foreign.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let foreign_snapshot = account.snapshot().await.unwrap();
    assert_eq!(foreign_snapshot.open_lots.len(), 2);

    let isolated_executor = Arc::new(FillExecutor::default());
    let (isolated_left, isolated_left_sender) = ChannelSource::new("left");
    let (isolated_right, isolated_right_sender) = ChannelSource::new("right");
    let mut isolated = ArbitragePaperTask::start(
        task_config("arbitrage:isolated-entry", StdDuration::from_secs(30)),
        monitor(),
        isolated_left,
        isolated_right,
        account.clone(),
        history,
        isolated_executor.clone(),
    )
    .await
    .unwrap();
    isolated_left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    isolated_right_sender
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
        tokio::time::timeout(StdDuration::from_secs(2), isolated.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(isolated.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        isolated.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(isolated_executor.calls.load(Ordering::SeqCst), 0);
    let after = account.snapshot().await.unwrap();
    assert_eq!(after.open_lots, foreign_snapshot.open_lots);
    assert!(
        !after
            .reservations
            .iter()
            .any(|reservation| { reservation.task_id == "arbitrage:isolated-entry/op/000001" })
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"failure\":\"recovery_required\""));
}

#[tokio::test]
async fn ordinary_operation_refuses_to_mix_with_a_foreign_same_direction_pair() {
    let (account, history, path) = account("foreign-same-direction-isolation");
    seed_open_pair("arbitrage:foreign-same-direction", &account, &history).await;
    let foreign_snapshot = account.snapshot().await.unwrap();
    assert_eq!(foreign_snapshot.open_lots.len(), 2);

    let isolated_executor = Arc::new(FillExecutor::default());
    let (isolated_left, isolated_left_sender) = ChannelSource::new("left");
    let (isolated_right, isolated_right_sender) = ChannelSource::new("right");
    let mut isolated = ArbitragePaperTask::start(
        task_config(
            "arbitrage:isolated-same-direction",
            StdDuration::from_secs(30),
        ),
        monitor(),
        isolated_left,
        isolated_right,
        account.clone(),
        history,
        isolated_executor.clone(),
    )
    .await
    .unwrap();
    isolated_left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    isolated_right_sender
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
        tokio::time::timeout(StdDuration::from_secs(2), isolated.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(isolated.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        isolated.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(isolated_executor.calls.load(Ordering::SeqCst), 0);
    let after = account.snapshot().await.unwrap();
    assert_eq!(after.open_lots, foreign_snapshot.open_lots);
    assert!(!after.reservations.iter().any(|reservation| {
        reservation.task_id == "arbitrage:isolated-same-direction/op/000001"
    }));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"failure\":\"recovery_required\""));
}

#[tokio::test]
async fn concurrent_owners_recheck_fifo_isolation_after_the_operation_lease() {
    let (first_account, history, _) = account("shared-operation-lease");
    let second_account = PaperAccountAuthority::new(
        first_account.journal_id(),
        history.clone(),
        PaperAccountConfig::new("paper-arbitrage", Money::new(decimal("100000"))).unwrap(),
    )
    .unwrap();

    let first_executor = Arc::new(GatedFillExecutor::default());
    let (first_left, first_left_sender) = ChannelSource::new("left");
    let (first_right, first_right_sender) = ChannelSource::new("right");
    let mut first = ArbitragePaperTask::start(
        task_config("arbitrage:lease-first", StdDuration::from_secs(30)),
        monitor(),
        first_left,
        first_right,
        first_account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();

    let second_executor = Arc::new(FillExecutor::default());
    let (second_left, second_left_sender) = ChannelSource::new("left");
    let (second_right, second_right_sender) = ChannelSource::new("right");
    let mut second = ArbitragePaperTask::start(
        task_config("arbitrage:lease-second", StdDuration::from_secs(30)),
        monitor(),
        second_left,
        second_right,
        second_account,
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();

    send_open_opportunity(&first_left_sender, &first_right_sender).await;
    wait_until(|| first_executor.calls.load(Ordering::SeqCst) == 1).await;

    send_open_opportunity(&second_left_sender, &second_right_sender).await;
    wait_until(|| second.status().processed_event_count == 2).await;
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.status().phase, ArbitragePaperTaskPhase::Running);
    let while_first_is_pending = first_account.snapshot().await.unwrap();
    assert!(while_first_is_pending.open_lots.is_empty());
    assert!(
        while_first_is_pending
            .reservations
            .iter()
            .any(|reservation| {
                reservation.task_id == "arbitrage:lease-first/op/000001"
                    && reservation.phase == PaperReservationPhase::Pending
            })
    );
    assert!(
        !while_first_is_pending
            .reservations
            .iter()
            .any(|reservation| reservation.task_id == "arbitrage:lease-second/op/000001")
    );

    first_executor.release_one();
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), second.wait())
            .await
            .expect("the waiting owner must resume after the first operation releases its lease")
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(second.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        second.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        first.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let settled = first_account.snapshot().await.unwrap();
    assert_eq!(settled.open_lots.len(), 2);
    assert!(
        !settled
            .reservations
            .iter()
            .any(|reservation| reservation.task_id == "arbitrage:lease-second/op/000001")
    );
}

#[tokio::test]
async fn timeout_abort_keeps_a_queued_owner_out_until_pending_is_uncertain() {
    let (first_account, history, _) = account("timeout-operation-lease-handoff");
    let second_account = PaperAccountAuthority::new(
        first_account.journal_id(),
        history.clone(),
        PaperAccountConfig::new("paper-arbitrage", Money::new(decimal("100000"))).unwrap(),
    )
    .unwrap();
    let first_executor = Arc::new(PendingExecutor::default());
    let (first_left, first_left_sender) = ChannelSource::new("left");
    let (first_right, first_right_sender) = ChannelSource::new("right");
    let mut first = ArbitragePaperTask::start(
        task_config(
            "arbitrage:timeout-lease-first",
            StdDuration::from_millis(50),
        ),
        monitor(),
        first_left,
        first_right,
        first_account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    let second_executor = Arc::new(FillExecutor::default());
    let (second_left, second_left_sender) = ChannelSource::new("left");
    let (second_right, second_right_sender) = ChannelSource::new("right");
    let mut second = ArbitragePaperTask::start(
        task_config("arbitrage:timeout-lease-second", StdDuration::from_secs(30)),
        monitor(),
        second_left,
        second_right,
        second_account,
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();

    send_open_opportunity(&first_left_sender, &first_right_sender).await;
    wait_until(|| first_executor.calls.load(Ordering::SeqCst) == 1).await;
    send_open_opportunity(&second_left_sender, &second_right_sender).await;
    wait_until(|| second.status().processed_event_count == 2).await;
    tokio::time::sleep(StdDuration::from_millis(25)).await;
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);

    assert!(matches!(
        first.stop().await.unwrap_err(),
        ArbitragePaperTaskError::ShutdownTimedOut
    ));
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), second.wait())
            .await
            .expect("the queued owner must recheck after timeout retention")
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    let snapshot = first_account.snapshot().await.unwrap();
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation.task_id == "arbitrage:timeout-lease-first/op/000001"
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    assert!(
        !snapshot.reservations.iter().any(|reservation| {
            reservation.task_id == "arbitrage:timeout-lease-second/op/000001"
        })
    );
}

#[tokio::test]
async fn kill_switch_fails_recovery_before_close_when_owned_and_foreign_pair_lots_coexist() {
    let (account, history, _) = account("account-risk-mixed-owner-lots");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let owner_executor = Arc::new(FillExecutor::default());
    let (owner_left, owner_left_sender) = ChannelSource::new("left");
    let (owner_right, owner_right_sender) = ChannelSource::new("right");
    let mut owner = ArbitragePaperTask::start(
        task_config("arbitrage:owned", StdDuration::from_secs(30)).with_account_risk(risk.clone()),
        monitor(),
        owner_left,
        owner_right,
        account.clone(),
        history.clone(),
        owner_executor.clone(),
    )
    .await
    .unwrap();
    owner_left_sender
        .send(observation("left", "99", "100", 1, base_time()))
        .await
        .unwrap();
    owner_right_sender
        .send(observation(
            "right",
            "101.5",
            "102",
            1,
            base_time() + Duration::seconds(1),
        ))
        .await
        .unwrap();
    wait_until(|| owner_executor.calls.load(Ordering::SeqCst) == 1).await;
    tokio::time::timeout(StdDuration::from_secs(2), async {
        loop {
            if account.snapshot().await.unwrap().open_lots.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Simulate a legacy/cross-process writer that predates the process-local
    // owner lease, so recovery still proves it will not flatten mixed lots.
    seed_legacy_open_pair_without_owner_lease("arbitrage:foreign/op/000001", &account, &history)
        .await;
    let before_kill = account.snapshot().await.unwrap();
    assert_eq!(before_kill.open_lots.len(), 4);

    risk.engage_kill_switch("mixed ownership drill", base_time() + Duration::seconds(2))
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), owner.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(owner.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        owner.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(owner_executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        account.snapshot().await.unwrap().open_lots,
        before_kill.open_lots
    );
}

#[tokio::test]
async fn source_end_with_owned_exposure_is_durable_recovery_required() {
    let (account, history, path) = account("source-end-owned-exposure");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:eof-owned", StdDuration::from_secs(30)),
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
    drop(left_sender);
    drop(right_sender);

    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(account.snapshot().await.unwrap().open_lots.len(), 2);
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"failure\":\"recovery_required\""));
    assert!(!body.contains("\"exit\":\"source_ended\",\"failure\":null"));
}

#[tokio::test]
async fn source_end_with_only_foreign_exposure_stops_cleanly_without_closing_it() {
    let (account, history, _) = account("source-end-foreign-exposure");
    seed_open_pair("arbitrage:eof-foreign", &account, &history).await;
    let before = account.snapshot().await.unwrap();
    assert_eq!(before.open_lots.len(), 2);

    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:eof-isolated", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();
    drop(left_sender);
    drop(right_sender);

    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap(),
        ArbitragePaperTaskExit::SourceEnded
    );
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Stopped);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        account.snapshot().await.unwrap().open_lots,
        before.open_lots
    );
}

#[tokio::test]
async fn kill_switch_bounds_a_permanently_pending_execution_and_requires_recovery() {
    let (account, history, path) = account("account-risk-pending-kill");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(PendingExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:pending-kill", StdDuration::from_secs(30))
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
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    risk.engage_kill_switch(
        "pending execution drill",
        base_time() + Duration::seconds(2),
    )
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .expect("kill processing must have a bounded response")
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.open_lots.is_empty());
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation.task_id == "arbitrage:pending-kill/op/000001"
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
    assert!(body.contains("\"failure\":\"recovery_required\""));
    assert!(!body.contains("\"decision\":\"task_stopped\""));
}

#[tokio::test]
async fn kill_switch_bounds_a_permanently_pending_forced_close_and_requires_recovery() {
    let (account, history, path) = account("account-risk-pending-forced-close");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(GatedFillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:pending-forced-close", StdDuration::from_secs(30))
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
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
    executor.release_one();
    tokio::time::timeout(StdDuration::from_secs(2), async {
        loop {
            if account.snapshot().await.unwrap().open_lots.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    risk.engage_kill_switch(
        "pending forced close drill",
        base_time() + Duration::seconds(2),
    )
    .await
    .unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 2).await;
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .expect("forced-close execution must have a bounded response")
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 2);
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation.task_id == "arbitrage:pending-forced-close/op/000002"
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
    assert!(body.contains("\"failure\":\"recovery_required\""));
    assert!(!body.contains("\"decision\":\"task_stopped\""));
}

#[tokio::test]
async fn engaged_kill_switch_closes_an_existing_position_reduce_only_before_stopping() {
    let (account, history, path) = account("account-risk-close-open-position");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(ScriptedExecutor::new(vec![
        ScriptedOutcome::FillAll,
        ScriptedOutcome::FillAll,
    ]));
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:risk-close", StdDuration::from_secs(30)).with_account_risk(risk),
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
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if !account.snapshot().await.unwrap().open_lots.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(2))
        .await
        .unwrap();

    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);

    let recorded = executor.recorded_intents();
    assert_eq!(recorded.len(), 2);
    assert!(recorded[1].iter().all(|intent| intent.reduce_only));
    assert!(
        recorded[1]
            .iter()
            .any(|intent| intent.side == crypto_trading_domain::Side::Buy)
    );
    assert!(
        recorded[1]
            .iter()
            .any(|intent| intent.side == crypto_trading_domain::Side::Sell)
    );

    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.reservations.is_empty());
    assert!(snapshot.open_lots.is_empty());

    let risk_state = risk.state().await.unwrap();
    assert!(risk_state.open_positions.is_empty());

    let replay_account = PaperAccountAuthority::new(
        account.journal_id(),
        history.clone(),
        PaperAccountConfig::new("paper-arbitrage", Money::new(decimal("100000"))).unwrap(),
    )
    .unwrap();
    let replay_snapshot = replay_account.snapshot().await.unwrap();
    assert!(replay_snapshot.reservations.is_empty());
    assert!(replay_snapshot.open_lots.is_empty());

    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("kill_switch:operator drill"));
    assert_open_admission_binding_and_unbound_forced_close(&body);
    assert_eq!(task.status().operation_count, 2);
}

#[tokio::test]
async fn engaged_kill_switch_with_partial_close_fails_recovery_required_instead_of_clean_stop() {
    let (account, history, path) = account("account-risk-close-partial");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(ScriptedExecutor::new(vec![
        ScriptedOutcome::FillAll,
        ScriptedOutcome::FillCount(1),
    ]));
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:risk-close-partial", StdDuration::from_secs(30))
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
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if !account.snapshot().await.unwrap().open_lots.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(2))
        .await
        .unwrap();

    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);

    let snapshot = account.snapshot().await.unwrap();
    assert!(!snapshot.open_lots.is_empty());
    assert!(
        snapshot
            .reservations
            .iter()
            .any(|reservation| reservation.phase == PaperReservationPhase::Uncertain)
    );

    let risk_state = risk.state().await.unwrap();
    assert!(!risk_state.open_positions.is_empty());

    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("\"decision\":\"execution_incomplete\""));
    assert!(body.contains("\"failure\":\"recovery_required\""));
}

#[tokio::test]
async fn kill_switch_without_cached_pair_ignores_a_foreign_owner_position() {
    let (account, history, path) = account("account-risk-restart-no-pair");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut first = ArbitragePaperTask::start(
        task_config("arbitrage:risk-seed", StdDuration::from_secs(30)),
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

    assert_eq!(
        first.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    let foreign_snapshot = account.snapshot().await.unwrap();
    assert!(!foreign_snapshot.open_lots.is_empty());

    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("restart drill", base_time() + Duration::seconds(2))
        .await
        .unwrap();
    let (restart_left, _restart_left_sender) = ChannelSource::new("left");
    let (restart_right, _restart_right_sender) = ChannelSource::new("right");
    let mut restart = ArbitragePaperTask::start(
        task_config("arbitrage:risk-restart", StdDuration::from_secs(30)).with_account_risk(risk),
        monitor(),
        restart_left,
        restart_right,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();

    assert_eq!(
        tokio::time::timeout(StdDuration::from_secs(2), restart.wait())
            .await
            .unwrap()
            .unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(restart.status().phase, ArbitragePaperTaskPhase::Stopped);
    let durable = restart.durable_status().await.unwrap();
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(
        account.snapshot().await.unwrap().open_lots,
        foreign_snapshot.open_lots
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("\"decision\":\"task_stopped\""));
}

#[tokio::test]
async fn malformed_account_risk_fact_during_poll_fails_owner_durably() {
    let (account, history, path) = account("account-risk-poll-malformed");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let (left_source, _left_sender) = ChannelSource::new("left");
    let (right_source, _right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:risk-poll-malformed", StdDuration::from_secs(30))
            .with_account_risk(risk),
        monitor(),
        left_source,
        right_source,
        account,
        history.clone(),
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();

    history
        .append(&DecisionRecord {
            timestamp: base_time(),
            strategy: "account_risk".to_owned(),
            symbol: "paper".to_owned(),
            decision: "account_risk_kill_switch_engaged".to_owned(),
            details: json!({"malformed": true}),
        })
        .await
        .unwrap();

    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::AccountRisk(_)
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::AccountContract)
    );
    let durable = task.durable_status().await.unwrap();
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Failed);
    assert_eq!(durable.failure, Some(ReadOnlyTaskFailure::AccountContract));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"task_failed\""));
    assert!(body.contains("\"failure\":\"account_contract\""));
}

#[tokio::test]
async fn malformed_account_risk_during_pending_execution_is_retained_uncertain_for_recovery() {
    let (account, history, path) = account("account-risk-poll-malformed-inflight");
    let risk = account_risk_authority(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(PendingExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config(
            "arbitrage:risk-poll-malformed-inflight",
            StdDuration::from_secs(30),
        )
        .with_account_risk(risk),
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
    history
        .append(&DecisionRecord {
            timestamp: base_time() + Duration::seconds(2),
            strategy: "account_risk".to_owned(),
            symbol: "paper".to_owned(),
            decision: "account_risk_kill_switch_engaged".to_owned(),
            details: json!({"malformed": true}),
        })
        .await
        .unwrap();

    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .expect("risk projection failure must stop an in-flight owner promptly")
            .unwrap_err(),
        ArbitragePaperTaskError::RecoveryRequired
    ));
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::RecoveryRequired)
    );
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.open_lots.is_empty());
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation.task_id == "arbitrage:risk-poll-malformed-inflight/op/000001"
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"paper_account_uncertain\""));
    assert!(body.contains("\"failure\":\"recovery_required\""));
    assert!(!body.contains("\"decision\":\"task_stopped\""));
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
async fn degraded_account_projection_is_never_used_to_plan_an_operation() {
    let (account, history, _) = account("degraded-decision-snapshot");
    let executor = Arc::new(FillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:degraded-decision", StdDuration::from_secs(30)),
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
    wait_until(|| task.status().processed_event_count == 1).await;
    history
        .append(&DecisionRecord {
            timestamp: base_time() + Duration::milliseconds(500),
            strategy: "paper_account".to_owned(),
            symbol: "paper-arbitrage".to_owned(),
            decision: "paper_account_reserved".to_owned(),
            details: json!({"schema_version": 1}),
        })
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
        tokio::time::timeout(StdDuration::from_secs(2), task.wait())
            .await
            .unwrap()
            .unwrap_err(),
        ArbitragePaperTaskError::Account(_)
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(task.status().phase, ArbitragePaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(ArbitragePaperTaskFailure::AccountContract)
    );
    let snapshot = account.snapshot().await.unwrap();
    assert_ne!(
        snapshot.projection_status,
        crypto_trading_runtime::ProjectionStatus::Complete
    );
    assert!(snapshot.reservations.is_empty());
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

#[tokio::test]
async fn inflight_future_ticks_do_not_change_the_pair_frozen_for_execution() {
    let (account, history, _) = account("frozen-execution-pair");
    let executor = Arc::new(GatedFillExecutor::default());
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ArbitragePaperTask::start(
        task_config("arbitrage:frozen-pair", StdDuration::from_secs(30)),
        monitor(),
        left_source,
        right_source,
        account,
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let triggering_left = observation("left", "99", "100", 1, base_time());
    let triggering_right = observation(
        "right",
        "101.5",
        "102",
        1,
        base_time() + Duration::seconds(1),
    );
    let expected_left = observed_snapshot(&triggering_left);
    let expected_right = observed_snapshot(&triggering_right);
    left_sender.send(triggering_left).await.unwrap();
    right_sender.send(triggering_right).await.unwrap();
    wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;

    left_sender
        .send(observation(
            "left",
            "100.8",
            "101",
            2,
            base_time() + Duration::seconds(2),
        ))
        .await
        .unwrap();
    right_sender
        .send(observation(
            "right",
            "100.8",
            "101",
            2,
            base_time() + Duration::seconds(3),
        ))
        .await
        .unwrap();
    wait_until(|| task.status().processed_event_count >= 4).await;
    assert!(
        executor
            .pairs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "the execution future must still be blocked while later ticks are consumed"
    );

    executor.release_one();
    wait_until(|| {
        executor
            .pairs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == 1
    })
    .await;
    wait_until(|| task.status().operation_count == 1).await;
    {
        let pairs = executor
            .pairs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(pairs[0].left, expected_left);
        assert_eq!(pairs[0].right, expected_right);
    }
    assert_eq!(
        task.stop().await.unwrap(),
        ArbitragePaperTaskExit::StopRequested
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
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
