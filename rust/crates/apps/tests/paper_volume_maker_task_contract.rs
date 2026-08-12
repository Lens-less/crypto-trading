use std::{
    collections::VecDeque,
    future::pending,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_cli::{
    VolumeMakerPaperExecutionFuture, VolumeMakerPaperExecutor, VolumeMakerPaperTask,
    VolumeMakerPaperTaskConfig, VolumeMakerPaperTaskError, VolumeMakerPaperTaskExit,
    VolumeMakerPaperTaskFailure, VolumeMakerPaperTaskPhase,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderStatus, OrderType, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    AccountRiskAuthority, ExecutionBatch, JsonlHistory, MarketDataEvent, MarketDataEventFuture,
    MarketDataEventSource, MarketDataObservation, MarketSupervisorConfig, PaperAccountAuthority,
    PaperAccountConfig, PaperCostModel, PaperReservationPhase, ReadOnlyTaskKind, ReadOnlyTaskPhase,
    ReadOnlyTaskRecovery, RuntimeError,
};
use crypto_trading_strategy::{
    AccountRiskLimits, AccountRiskPolicy, VolumeMakerMode, VolumeMakerPlanConfig,
    VolumeMakerStrategy,
};
use rust_decimal::Decimal;
use tokio::sync::Semaphore;

const EXCHANGE: &str = "paper-volume";

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

fn strategy(mode: VolumeMakerMode) -> VolumeMakerStrategy {
    VolumeMakerStrategy::new(VolumeMakerPlanConfig {
        exchange: EXCHANGE.to_owned(),
        symbol: Symbol::new("BTC-USDT").unwrap(),
        market_type: MarketType::Perpetual,
        mode,
        order_quantity: quantity("1"),
        reverse_trading: false,
        post_only: false,
    })
    .unwrap()
}

fn config(task_id: &str, mode: VolumeMakerMode, grace: StdDuration) -> VolumeMakerPaperTaskConfig {
    VolumeMakerPaperTaskConfig::new(
        task_id,
        strategy(mode),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        MarketSupervisorConfig::new(grace).unwrap(),
    )
    .unwrap()
}

fn account(label: &str) -> (PaperAccountAuthority, JsonlHistory, PathBuf) {
    account_with_available(label, "10000")
}

fn account_with_available(
    label: &str,
    initial_available: &str,
) -> (PaperAccountAuthority, JsonlHistory, PathBuf) {
    let path = temp_path(label, "jsonl");
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new(EXCHANGE, Money::new(decimal(initial_available))).unwrap(),
    )
    .unwrap();
    (account, history, path)
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

/// One observation with visible top-of-book depth (bid side deeper, so the
/// market-imbalance strategy opens with a buy).
fn observation(
    bid: &str,
    ask: &str,
    revision: u64,
    received_at: chrono::DateTime<Utc>,
) -> MarketDataEvent {
    let mut snapshot = MarketSnapshot::new(
        EXCHANGE,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        price(bid),
        price(ask),
        received_at,
    )
    .unwrap();
    snapshot.last = Some(price(bid));
    snapshot.bid_quantity = Some(quantity("5"));
    snapshot.ask_quantity = Some(quantity("2"));
    MarketDataEvent::Observation(
        MarketDataObservation::new(snapshot, revision, received_at).unwrap(),
    )
}

/// One observation without any visible depth, so the market mode waits.
fn depthless_observation(
    bid: &str,
    ask: &str,
    revision: u64,
    received_at: chrono::DateTime<Utc>,
) -> MarketDataEvent {
    let snapshot = MarketSnapshot::new(
        EXCHANGE,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        price(bid),
        price(ask),
        received_at,
    )
    .unwrap();
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
        EXCHANGE
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
        EXCHANGE
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        if let Some(event) = self.first.take() {
            return Box::pin(async move { Ok(Some(event)) });
        }
        Box::pin(pending())
    }
}

#[derive(Debug)]
struct SignaledOnceSource {
    first: Option<MarketDataEvent>,
    delivered: Arc<AtomicBool>,
}

impl MarketDataEventSource for SignaledOnceSource {
    fn source_id(&self) -> &'static str {
        EXCHANGE
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        let Some(event) = self.first.take() else {
            return Box::pin(pending());
        };
        let delivered = Arc::clone(&self.delivered);
        Box::pin(async move {
            delivered.store(true, Ordering::SeqCst);
            Ok(Some(event))
        })
    }
}

/// Source that releases one event per semaphore permit so multi-event tests
/// are not conflated by the supervisor's latest-event retention.
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
        EXCHANGE
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

#[derive(Debug)]
struct SignaledThenSteppedSource {
    events: VecDeque<MarketDataEvent>,
    first_delivered: Arc<AtomicBool>,
    release: Arc<Semaphore>,
}

impl SignaledThenSteppedSource {
    fn new(
        events: Vec<MarketDataEvent>,
        first_delivered: Arc<AtomicBool>,
        release: Arc<Semaphore>,
    ) -> Self {
        Self {
            events: events.into(),
            first_delivered,
            release,
        }
    }
}

impl MarketDataEventSource for SignaledThenSteppedSource {
    fn source_id(&self) -> &'static str {
        EXCHANGE
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        let Some(event) = self.events.pop_front() else {
            return Box::pin(async move { Ok(None) });
        };
        let first_delivered = Arc::clone(&self.first_delivered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            release.acquire_owned().await.unwrap().forget();
            first_delivered.store(true, Ordering::SeqCst);
            Ok(Some(event))
        })
    }
}

#[derive(Debug, Default)]
struct FillExecutor {
    calls: AtomicUsize,
    market_fill_prices: Mutex<VecDeque<Price>>,
}

impl FillExecutor {
    fn with_market_fill_prices(prices: &[&str]) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            market_fill_prices: Mutex::new(prices.iter().map(|value| price(value)).collect()),
        }
    }
}

fn filled_receipts(batch: &ExecutionBatch, average_fill_price: Price) -> Vec<TradingReceipt> {
    let intent = batch.intents()[0].clone();
    vec![TradingReceipt::Submitted {
        order: Order {
            id: format!("paper-{}", intent.client_order_id),
            intent: intent.clone(),
            filled_quantity: intent.quantity,
            average_fill_price: Some(intent.price.unwrap_or(average_fill_price)),
            status: OrderStatus::Filled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        disposition: SubmissionDisposition::Filled,
    }]
}

impl VolumeMakerPaperExecutor for FillExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let intent = &batch.intents()[0];
        let average_fill_price = intent
            .price
            .or_else(|| self.market_fill_prices.lock().unwrap().pop_front());
        Box::pin(async move {
            let average_fill_price = average_fill_price.ok_or(
                RuntimeError::InvalidExecutionPolicy("test fixture omitted a market fill price"),
            )?;
            Ok(filled_receipts(&batch, average_fill_price))
        })
    }
}

/// One executed intent summary: side, order type, limit price, reduce-only.
type RecordedIntent = (Side, OrderType, Option<Decimal>, bool);

/// Records `(side, order_type, price, reduce_only)` per executed intent.
#[derive(Debug, Default)]
struct RecordingExecutor {
    intents: Mutex<Vec<RecordedIntent>>,
    calls: AtomicUsize,
    market_fill_prices: Mutex<VecDeque<Price>>,
}

impl RecordingExecutor {
    fn with_market_fill_prices(prices: &[&str]) -> Self {
        Self {
            intents: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            market_fill_prices: Mutex::new(prices.iter().map(|value| price(value)).collect()),
        }
    }
}

impl VolumeMakerPaperExecutor for RecordingExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        let intent = batch.intents()[0].clone();
        self.intents.lock().unwrap().push((
            intent.side,
            intent.order_type,
            intent.price.map(Price::as_decimal),
            intent.reduce_only,
        ));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let average_fill_price = intent
            .price
            .or_else(|| self.market_fill_prices.lock().unwrap().pop_front());
        Box::pin(async move {
            let average_fill_price = average_fill_price.ok_or(
                RuntimeError::InvalidExecutionPolicy("test fixture omitted a market fill price"),
            )?;
            Ok(filled_receipts(&batch, average_fill_price))
        })
    }
}

#[derive(Debug, Default)]
struct FailingExecutor;

impl VolumeMakerPaperExecutor for FailingExecutor {
    fn execute(&self, _batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        Box::pin(async {
            Err(RuntimeError::InvalidExecutionPolicy(
                "simulated volume-maker dispatch timeout",
            ))
        })
    }
}

#[derive(Debug, Default)]
struct FailSecondExecutionExecutor {
    calls: AtomicUsize,
}

#[derive(Debug, Default)]
struct FillThenPendingExecutor {
    calls: AtomicUsize,
}

impl VolumeMakerPaperExecutor for FillThenPendingExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return Box::pin(async move { Ok(filled_receipts(&batch, price("101"))) });
        }
        Box::pin(pending())
    }
}

impl VolumeMakerPaperExecutor for FailSecondExecutionExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 0 {
                Ok(filled_receipts(&batch, price("101")))
            } else {
                Err(RuntimeError::InvalidExecutionPolicy(
                    "simulated forced close failure",
                ))
            }
        })
    }
}

#[derive(Debug, Default)]
struct PendingExecutor {
    started: AtomicBool,
}

impl VolumeMakerPaperExecutor for PendingExecutor {
    fn execute(&self, _batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        self.started.store(true, Ordering::SeqCst);
        Box::pin(pending())
    }
}

#[derive(Debug)]
struct GateFirstExecutionExecutor {
    calls: AtomicUsize,
    first_started: AtomicBool,
    first_release: Arc<Semaphore>,
    intents: Mutex<Vec<RecordedIntent>>,
}

impl GateFirstExecutionExecutor {
    fn new(first_release: Arc<Semaphore>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            first_started: AtomicBool::new(false),
            first_release,
            intents: Mutex::new(Vec::new()),
        }
    }
}

impl VolumeMakerPaperExecutor for GateFirstExecutionExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let intent = batch.intents()[0].clone();
        self.intents.lock().unwrap().push((
            intent.side,
            intent.order_type,
            intent.price.map(Price::as_decimal),
            intent.reduce_only,
        ));
        if call == 0 {
            self.first_started.store(true, Ordering::SeqCst);
            let release = Arc::clone(&self.first_release);
            return Box::pin(async move {
                release.acquire_owned().await.unwrap().forget();
                Ok(filled_receipts(&batch, price("101")))
            });
        }
        Box::pin(async move { Ok(filled_receipts(&batch, price("99"))) })
    }
}

#[derive(Debug)]
struct GateSecondExecutionExecutor {
    calls: AtomicUsize,
    second_started: AtomicBool,
    second_release: Arc<Semaphore>,
}

impl GateSecondExecutionExecutor {
    fn new(second_release: Arc<Semaphore>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            second_started: AtomicBool::new(false),
            second_release,
        }
    }
}

impl VolumeMakerPaperExecutor for GateSecondExecutionExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return Box::pin(async move { Ok(filled_receipts(&batch, price("101"))) });
        }
        self.second_started.store(true, Ordering::SeqCst);
        let release = Arc::clone(&self.second_release);
        Box::pin(async move {
            release.acquire_owned().await.unwrap().forget();
            Ok(filled_receipts(&batch, price("99")))
        })
    }
}

async fn seed_external_open_lot(
    task_id: &str,
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
) {
    let mut seed = VolumeMakerPaperTask::start(
        config(
            task_id,
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        VecSource::new(vec![observation(
            "100",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )]),
        account.clone(),
        history.clone(),
        Arc::new(FillExecutor::with_market_fill_prices(&["101"])),
    )
    .await
    .unwrap();
    assert!(matches!(
        seed.wait().await,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
}

#[tokio::test]
async fn market_mode_cycle_opens_and_closes_with_independent_reservations() {
    let (account, history, path) = account("market-cycle");
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["101", "102"]));
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("100", "101", 1, base_time() + Duration::seconds(10)),
            observation("102", "103", 2, base_time() + Duration::seconds(20)),
        ],
        Arc::clone(&stepper),
    );
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:market",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        // The second observation is released only after the open executed, so
        // the close deterministically consumes it.
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::SourceEnded);

    // Legacy market-mode semantics: the deeper bid side means the thin ask is
    // eaten (market buy), then the position closes reduce-only on the next
    // observation.
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Buy, OrderType::Market, None, false),
            (Side::Sell, OrderType::Market, None, true),
        ]
    );
    let snapshot = account.snapshot().await.unwrap();
    assert!(
        snapshot.reservations.is_empty(),
        "completed market cycles should prune released reservations from the live snapshot"
    );
    assert!(snapshot.open_lots.is_empty());
    assert_eq!(snapshot.cumulative_fees, Money::new(decimal("0.203")));
    assert_eq!(snapshot.realized_pnl, Money::new(decimal("0.797")));

    let status = task.status();
    assert_eq!(status.completed_cycle_count, 1);
    assert_eq!(status.operation_count, 2);
    let durable = task.durable_status().await.unwrap();
    assert_eq!(durable.kind, ReadOnlyTaskKind::VolumeMaker);
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(durable.recovery, ReadOnlyTaskRecovery::None);
    assert_eq!(durable.sources.len(), 1);

    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"task_kind\":\"volume_maker\""), "{body}");
    assert!(body.contains("\"strategy\":\"volume_maker\""), "{body}");
    assert!(!body.contains("\"strategy\":\"grid\""), "{body}");
    assert!(body.contains("\"decision\":\"execution_planned\""));
    assert!(body.contains("\"decision\":\"execution_completed\""));
    // The final partial hour is exported once on stop, like the legacy
    // hourly tracker.
    assert!(body.contains("\"decision\":\"volume_maker_statistics\""));
    assert!(body.contains("\"completed_cycles\":1"), "{body}");
    assert!(body.contains("\"total_volume\":\"1\""), "{body}");
    assert!(body.contains("\"reason\":\"stop\""), "{body}");
}

#[tokio::test]
async fn limit_mode_virtual_quote_fills_only_after_a_crossing_observation() {
    let (account, history, path) = account("limit-quote");
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["98"]));
    let stepper = Arc::new(Semaphore::new(1));
    // Quote at 100/101, then the book falls through the standing bid, then a
    // final observation closes the bought position.
    let source = SteppedSource::new(
        vec![
            observation("100", "101", 1, base_time() + Duration::seconds(10)),
            observation("98", "99", 2, base_time() + Duration::seconds(20)),
            observation("98", "99", 3, base_time() + Duration::seconds(30)),
        ],
        Arc::clone(&stepper),
    );
    let history_path = path.clone();
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:limit",
            VolumeMakerMode::LimitBoth,
            StdDuration::from_secs(1),
        ),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        // The quote-only first observation executes nothing; its durable
        // checkpoint gates the crossing observation.
        wait_until(|| checkpoint_count(&history_path) >= 1).await;
        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        stepper.add_permits(1);
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::SourceEnded);

    // The open leg is the standing quote's bid executed as one marketable
    // limit; the close is the reduce-only market leg.
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Buy, OrderType::Limit, Some(decimal("100")), false),
            (Side::Sell, OrderType::Market, None, true),
        ]
    );
    assert_eq!(task.status().completed_cycle_count, 1);
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert!(
        snapshot.reservations.is_empty(),
        "completed limit cycles should prune released reservations from the live snapshot"
    );
    let body = std::fs::read_to_string(path).unwrap();
    // Realized cycle: bought at 100, closed at the observed bid 98.
    assert!(body.contains("\"realized_pnl\":\"-2\""), "{body}");
}

#[tokio::test]
async fn account_risk_rejections_skip_cycles_without_reservations() {
    let (account, history, path) = account("risk-rejects");
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
        "100",
        "101",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-reject",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    // The open leg is refused before any reservation exists; the owner stays
    // alive and completes when the source ends.
    assert_eq!(
        task.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::SourceEnded
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(task.status().operation_count, 0);
    assert_eq!(task.status().completed_cycle_count, 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let state = risk.state().await.unwrap();
    assert_eq!(state.rejected_count, 1);
    assert_eq!(state.last_rejection.as_deref(), Some("symbol_disabled"));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_rejected\""));
    assert!(!body.contains("\"decision\":\"paper_account_reserved\""));
    assert!(body.contains("\"rejected_entries\":1"), "{body}");
}

#[tokio::test]
async fn admitted_open_binds_its_exact_risk_ticket_but_reduce_only_close_does_not() {
    let (account, history, path) = account("risk-ticket-binding");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FillExecutor::with_market_fill_prices(&["101", "100"]));
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("100", "101", 1, base_time() + Duration::seconds(10)),
            observation("100", "101", 2, base_time() + Duration::seconds(20)),
        ],
        Arc::clone(&stepper),
    );
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-ticket-binding",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk),
        source,
        account,
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::SourceEnded);

    let records = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let admitted_ticket = records
        .iter()
        .find(|record| record["decision"] == "account_risk_admitted")
        .and_then(|record| record["details"]["ticket_id"].as_str())
        .map(str::to_owned)
        .unwrap();
    let reservations = records
        .into_iter()
        .filter(|record| record["decision"] == "paper_account_reserved")
        .map(|record| record["details"]["request"].clone())
        .collect::<Vec<_>>();
    assert_eq!(reservations.len(), 2, "one open and one close must reserve");
    assert_eq!(reservations[0]["risk_scope_id"], "paper");
    assert_eq!(
        reservations[0]["risk_admission_ticket_id"],
        admitted_ticket.as_str(),
        "the opening reservation must bind the exact admitted ticket",
    );
    assert!(
        reservations[1].get("risk_scope_id").is_none()
            && reservations[1].get("risk_admission_ticket_id").is_none(),
        "the reduce-only close must not request a second admission"
    );
}

#[tokio::test]
async fn reservation_failures_cancel_admitted_cycles_without_leaking_owner_risk() {
    let (account, history, path) = account_with_available("reserve-after-admit", "50");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FillExecutor::default());
    let first_source = VecSource::new(vec![observation(
        "100",
        "101",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut first = VolumeMakerPaperTask::start(
        config(
            "volume:reserve-fail:first",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        first_source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();

    let first_error = first.wait().await.unwrap_err();
    assert!(matches!(first_error, VolumeMakerPaperTaskError::Saga(_)));
    assert_eq!(first.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        first.status().failure,
        Some(VolumeMakerPaperTaskFailure::AccountContract)
    );

    let second_source = VecSource::new(vec![observation(
        "100",
        "101",
        2,
        base_time() + Duration::seconds(20),
    )]);
    let mut second = VolumeMakerPaperTask::start(
        config(
            "volume:reserve-fail:second",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        second_source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let second_error = second.wait().await.unwrap_err();
    assert!(matches!(second_error, VolumeMakerPaperTaskError::Saga(_)));
    assert_eq!(second.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        second.status().failure,
        Some(VolumeMakerPaperTaskFailure::AccountContract)
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
async fn engaged_kill_switch_stops_the_owner_before_any_entry() {
    let (account, history, path) = account("risk-kill");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time())
        .await
        .unwrap();
    let executor = Arc::new(FillExecutor::default());
    let source = VecSource::new(vec![observation(
        "100",
        "101",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert_eq!(
        task.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::StopRequested
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("kill_switch:operator drill"));
}

#[tokio::test]
async fn kill_switch_treats_an_external_same_instrument_lot_as_owner_flat() {
    let (account, history, _) = account("risk-kill-external-only");
    seed_external_open_lot("volume:external-observer-shadow", &account, &history).await;
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(15))
        .await
        .unwrap();
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&[]));
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:external-observer",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk),
        BlockingSource { first: None },
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert_eq!(
        task.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::StopRequested
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let snapshot = account.decision_snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1);
    assert_eq!(snapshot.open_lots[0].remaining_quantity, quantity("1"));
}

#[tokio::test]
async fn risk_enabled_open_refuses_a_foreign_same_instrument_fifo_before_admission() {
    let (account, history, _) = account("risk-kill-mixed-owner");
    seed_external_open_lot("volume:mixed-owner-shadow", &account, &history).await;
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&[]));
    let source = VecSource::new(vec![observation(
        "99",
        "101",
        2,
        base_time() + Duration::seconds(20),
    )]);
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:mixed-owner",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert!(matches!(
        task.wait().await,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(task.status().operation_count, 0);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(risk.state().await.unwrap().admitted_count, 0);
    assert_eq!(
        account.decision_snapshot().await.unwrap().open_lots.len(),
        1
    );
}

#[tokio::test]
async fn market_open_opportunity_fails_before_touching_a_foreign_same_instrument_fifo() {
    let (account, history, path) = account("foreign-fifo-market-open");
    seed_external_open_lot("volume:foreign-market-shadow", &account, &history).await;
    let before = account.decision_snapshot().await.unwrap();
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&[]));
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:foreign-market",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        VecSource::new(vec![observation(
            "100",
            "101",
            2,
            base_time() + Duration::seconds(20),
        )]),
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    assert!(matches!(
        task.wait().await,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(VolumeMakerPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 0);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let after = account.decision_snapshot().await.unwrap();
    assert_eq!(after.open_lots, before.open_lots);
    assert_eq!(after.reservations, before.reservations);
    let body = std::fs::read_to_string(path).unwrap();
    assert!(!body.contains("volume:foreign-market/op/"), "{body}");
}

#[tokio::test]
async fn limit_crossing_opportunity_fails_before_touching_a_foreign_same_instrument_fifo() {
    let (account, history, path) = account("foreign-fifo-limit-open");
    seed_external_open_lot("volume:foreign-limit-shadow", &account, &history).await;
    let before = account.decision_snapshot().await.unwrap();
    let baseline_checkpoints = checkpoint_count(&path);
    let release = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("100", "101", 2, base_time() + Duration::seconds(20)),
            observation("98", "99", 3, base_time() + Duration::seconds(30)),
        ],
        Arc::clone(&release),
    );
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&[]));
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:foreign-limit",
            VolumeMakerMode::LimitBoth,
            StdDuration::from_secs(1),
        ),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (result, ()) = tokio::join!(task.wait(), async {
        wait_until(|| checkpoint_count(&path) > baseline_checkpoints).await;
        release.add_permits(1);
    });
    assert!(matches!(
        result,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(task.status().operation_count, 0);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let after = account.decision_snapshot().await.unwrap();
    assert_eq!(after.open_lots, before.open_lots);
    assert_eq!(after.reservations, before.reservations);
    let body = std::fs::read_to_string(path).unwrap();
    assert!(!body.contains("volume:foreign-limit/op/"), "{body}");
}

#[tokio::test]
async fn shared_account_operation_lease_serializes_snapshot_through_settlement() {
    let (account, history, _) = account("shared-operation-lease");
    let first_release = Arc::new(Semaphore::new(0));
    let first_executor = Arc::new(GateFirstExecutionExecutor::new(Arc::clone(&first_release)));
    let first_steps = Arc::new(Semaphore::new(1));
    let mut first = VolumeMakerPaperTask::start(
        config(
            "volume:lease:first",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        SteppedSource::new(
            vec![
                observation("99", "101", 1, base_time() + Duration::seconds(10)),
                observation("99", "100", 2, base_time() + Duration::seconds(20)),
            ],
            Arc::clone(&first_steps),
        ),
        account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| first_executor.first_started.load(Ordering::SeqCst)).await;

    let second_delivered = Arc::new(AtomicBool::new(false));
    let second_executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["101"]));
    let mut second = VolumeMakerPaperTask::start(
        config(
            "volume:lease:second",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        SignaledOnceSource {
            first: Some(observation(
                "99",
                "101",
                1,
                base_time() + Duration::seconds(15),
            )),
            delivered: Arc::clone(&second_delivered),
        },
        account.clone(),
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| second_delivered.load(Ordering::SeqCst)).await;

    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(
        second_executor.calls.load(Ordering::SeqCst),
        0,
        "the later owner must wait before entering its executor"
    );
    let pending = account.decision_snapshot().await.unwrap();
    assert!(
        pending
            .reservations
            .iter()
            .all(|reservation| { !reservation.task_id.starts_with("volume:lease:second/op/") })
    );

    first_release.add_permits(1);
    assert!(matches!(
        second.wait().await,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(second.status().operation_count, 0);
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        account.decision_snapshot().await.unwrap().open_lots.len(),
        1
    );

    first_steps.add_permits(1);
    assert_eq!(
        first.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::SourceEnded
    );
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 2);
    let settled = account.decision_snapshot().await.unwrap();
    assert!(settled.open_lots.is_empty());
    assert!(settled.reservations.is_empty());
}

#[tokio::test]
async fn shutdown_timeout_retains_before_a_queued_owner_can_cross_the_execution_seam() {
    let (account, history, path) = account("shutdown-retention-barrier");
    let first_executor = Arc::new(PendingExecutor::default());
    let mut first = VolumeMakerPaperTask::start(
        config(
            "volume:retention:first",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_millis(250),
        ),
        BlockingSource {
            first: Some(observation(
                "99",
                "101",
                1,
                base_time() + Duration::seconds(10),
            )),
        },
        account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| first_executor.started.load(Ordering::SeqCst)).await;

    let second_delivered = Arc::new(AtomicBool::new(false));
    let second_executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["101"]));
    let mut second = VolumeMakerPaperTask::start(
        config(
            "volume:retention:queued",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        SignaledThenSteppedSource::new(
            vec![observation(
                "99",
                "101",
                1,
                base_time() + Duration::seconds(20),
            )],
            Arc::clone(&second_delivered),
            Arc::new(Semaphore::new(1)),
        ),
        account.clone(),
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| second_delivered.load(Ordering::SeqCst)).await;

    assert!(matches!(
        first.stop().await,
        Err(VolumeMakerPaperTaskError::ShutdownTimedOut)
    ));
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(2), second.wait())
            .await
            .expect("the queued owner must fail closed after the abort handoff"),
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(second.status().operation_count, 0);
    assert_eq!(
        second_executor.calls.load(Ordering::SeqCst),
        0,
        "the queued owner must not cross the execution seam"
    );
    let snapshot = account.decision_snapshot().await.unwrap();
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation
            .task_id
            .starts_with("volume:retention:first/op/")
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    assert!(snapshot.reservations.iter().all(|reservation| {
        !reservation
            .task_id
            .starts_with("volume:retention:queued/op/")
    }));
    let body = std::fs::read_to_string(path).unwrap();
    let records = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let uncertain_index = records
        .iter()
        .position(|record| record["decision"] == "paper_account_uncertain")
        .expect("the aborted reservation must be retained durably");
    let queued_failure_index = records
        .iter()
        .position(|record| {
            record["decision"] == "task_failed"
                && record["details"]["task_id"] == "volume:retention:queued"
        })
        .expect("the queued owner must record its fail-closed outcome");
    assert!(
        uncertain_index < queued_failure_index,
        "the handoff lease must keep the queued owner out until retention is durable: {body}"
    );
}

#[tokio::test]
async fn forced_close_holds_the_shared_operation_lease_through_its_flat_post_check() {
    let (account, history, _) = account("forced-close-operation-lease");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let forced_close_release = Arc::new(Semaphore::new(0));
    let first_executor = Arc::new(GateSecondExecutionExecutor::new(Arc::clone(
        &forced_close_release,
    )));
    let mut first = VolumeMakerPaperTask::start(
        config(
            "volume:lease:forced-close",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        BlockingSource {
            first: Some(observation(
                "99",
                "101",
                1,
                base_time() + Duration::seconds(10),
            )),
        },
        account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| first_executor.calls.load(Ordering::SeqCst) == 1).await;
    risk.engage_kill_switch("operation lease drill", base_time() + Duration::seconds(15))
        .await
        .unwrap();
    wait_until(|| first_executor.second_started.load(Ordering::SeqCst)).await;

    let second_executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["101", "99"]));
    let second_steps = Arc::new(Semaphore::new(1));
    let second_delivered = Arc::new(AtomicBool::new(false));
    let mut second = VolumeMakerPaperTask::start(
        config(
            "volume:lease:after-forced-close",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        SignaledThenSteppedSource::new(
            vec![
                observation("99", "101", 1, base_time() + Duration::seconds(20)),
                observation("99", "100", 2, base_time() + Duration::seconds(30)),
            ],
            Arc::clone(&second_delivered),
            Arc::clone(&second_steps),
        ),
        account.clone(),
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| second_delivered.load(Ordering::SeqCst)).await;
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(
        second_executor.calls.load(Ordering::SeqCst),
        0,
        "a normal owner must not enter while forced close is unresolved"
    );

    forced_close_release.add_permits(1);
    assert_eq!(
        first.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::StopRequested
    );
    wait_until(|| second_executor.calls.load(Ordering::SeqCst) == 1).await;
    second_steps.add_permits(1);
    assert_eq!(
        second.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::SourceEnded
    );
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 2);
    let settled = account.decision_snapshot().await.unwrap();
    assert!(settled.open_lots.is_empty());
    assert!(settled.reservations.is_empty());
}

#[tokio::test]
async fn cancelled_forced_close_leaves_an_active_barrier_for_a_queued_owner() {
    let (account, history, path) = account("forced-close-cancel-barrier");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let first_executor = Arc::new(FillThenPendingExecutor::default());
    let mut first = VolumeMakerPaperTask::start(
        config(
            "volume:barrier:forced-close",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_millis(250),
        )
        .with_account_risk(risk.clone()),
        BlockingSource {
            first: Some(observation(
                "99",
                "101",
                1,
                base_time() + Duration::seconds(10),
            )),
        },
        account.clone(),
        history.clone(),
        first_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| first_executor.calls.load(Ordering::SeqCst) == 1).await;
    risk.engage_kill_switch(
        "forced close cancel barrier",
        base_time() + Duration::seconds(15),
    )
    .await
    .unwrap();
    wait_until(|| first_executor.calls.load(Ordering::SeqCst) == 2).await;

    let second_delivered = Arc::new(AtomicBool::new(false));
    let second_executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["101"]));
    let mut second = VolumeMakerPaperTask::start(
        config(
            "volume:barrier:queued",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        SignaledThenSteppedSource::new(
            vec![observation(
                "99",
                "101",
                1,
                base_time() + Duration::seconds(20),
            )],
            Arc::clone(&second_delivered),
            Arc::new(Semaphore::new(1)),
        ),
        account.clone(),
        history,
        second_executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| second_delivered.load(Ordering::SeqCst)).await;
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);

    assert!(matches!(
        first.wait().await,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert!(matches!(
        second.wait().await,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(second.status().operation_count, 0);
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    let snapshot = account.decision_snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1);
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation
            .task_id
            .starts_with("volume:barrier:forced-close/op/")
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    assert!(
        snapshot
            .reservations
            .iter()
            .all(|reservation| { !reservation.task_id.starts_with("volume:barrier:queued/op/") })
    );
    let body = std::fs::read_to_string(path).unwrap();
    let records = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let uncertain_index = records
        .iter()
        .position(|record| record["decision"] == "paper_account_uncertain")
        .expect("the forced-close reservation must become uncertain");
    let queued_failure_index = records
        .iter()
        .position(|record| {
            record["decision"] == "task_failed"
                && record["details"]["task_id"] == "volume:barrier:queued"
        })
        .expect("the queued owner must fail closed");
    assert!(uncertain_index < queued_failure_index, "{body}");
}

#[tokio::test]
async fn engaged_kill_switch_closes_an_open_position_before_stopping_and_restart_stays_flat() {
    let (account, history, path) = account("risk-kill-close");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(RecordingExecutor::with_market_fill_prices(&["101", "99"]));
    let source = BlockingSource {
        first: Some(observation(
            "99",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )),
    };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill-close",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(15))
            .await
            .unwrap();
    });

    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::StopRequested);
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Buy, OrderType::Market, None, false),
            (Side::Sell, OrderType::Market, None, true),
        ]
    );
    assert_eq!(task.status().completed_cycle_count, 1);
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert!(snapshot.reservations.is_empty());
    assert!(snapshot.open_lots.is_empty());
    assert!(risk.state().await.unwrap().open_positions.is_empty());

    let mut restart = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill-close",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        VecSource::new(Vec::new()),
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    assert_eq!(
        restart.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::SourceEnded
    );
    assert!(account.snapshot().await.unwrap().open_lots.is_empty());
    assert!(risk.state().await.unwrap().open_positions.is_empty());

    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(!body.contains("\"decision\":\"task_failed\""), "{body}");
}

#[tokio::test]
async fn kill_switch_during_a_resolving_open_closes_the_settled_position_before_stopping() {
    let (account, history, _) = account("risk-kill-resolving-open");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let first_release = Arc::new(Semaphore::new(0));
    let executor = Arc::new(GateFirstExecutionExecutor::new(Arc::clone(&first_release)));
    let source = BlockingSource {
        first: Some(observation(
            "99",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )),
    };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill-resolving-open",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.first_started.load(Ordering::SeqCst)).await;
        risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(15))
            .await
            .unwrap();
        first_release.add_permits(1);
    });

    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::StopRequested);
    assert_eq!(task.status().operation_count, 2);
    assert_eq!(
        executor.intents.lock().unwrap().clone(),
        vec![
            (Side::Buy, OrderType::Market, None, false),
            (Side::Sell, OrderType::Market, None, true),
        ]
    );
    let snapshot = account.decision_snapshot().await.unwrap();
    assert!(snapshot.open_lots.is_empty());
    assert!(snapshot.reservations.is_empty());
}

#[tokio::test]
async fn account_risk_forced_close_failure_fails_closed_instead_of_stopping() {
    let (account, history, path) = account("risk-kill-close-fail");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FailSecondExecutionExecutor::default());
    let source = BlockingSource {
        first: Some(observation(
            "99",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )),
    };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill-close-fail",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (result, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(15))
            .await
            .unwrap();
    });

    let error = result.unwrap_err();
    assert!(matches!(error, VolumeMakerPaperTaskError::RecoveryRequired));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(VolumeMakerPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.snapshot().await.unwrap();
    assert!(
        snapshot
            .reservations
            .iter()
            .any(|reservation| reservation.phase == PaperReservationPhase::Uncertain)
    );
    assert!(
        !snapshot.open_lots.is_empty(),
        "failed forced close must not pretend the open lot is gone"
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"task_failed\""), "{body}");
    assert!(!body.contains("\"decision\":\"task_stopped\""), "{body}");
}

#[tokio::test]
async fn stuck_account_risk_forced_close_times_out_to_recovery_with_operation_count() {
    let (account, history, path) = account("risk-kill-close-pending");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(FillThenPendingExecutor::default());
    let source = BlockingSource {
        first: Some(observation(
            "99",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )),
    };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill-close-pending",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_millis(250),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (result, ()) = tokio::join!(
        tokio::time::timeout(StdDuration::from_secs(2), task.wait()),
        async {
            wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
            risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(15))
                .await
                .unwrap();
        }
    );
    let error = result
        .expect("a stuck forced close must obey the owner shutdown bound")
        .unwrap_err();
    assert!(matches!(error, VolumeMakerPaperTaskError::RecoveryRequired));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(task.status().operation_count, 2);
    let snapshot = account.decision_snapshot().await.unwrap();
    assert_eq!(snapshot.open_lots.len(), 1);
    assert!(snapshot.reservations.iter().any(|reservation| {
        reservation.task_id.ends_with("/op/000002")
            && reservation.phase == PaperReservationPhase::Uncertain
    }));
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"task_failed\""), "{body}");
    assert!(!body.contains("\"decision\":\"task_stopped\""), "{body}");
}

#[tokio::test]
async fn kill_switch_during_stuck_open_fails_recovery_instead_of_waiting_forever() {
    let (account, history, path) = account("risk-kill-pending-open");
    let risk = account_risk(&account, &history, AccountRiskLimits::default());
    let executor = Arc::new(PendingExecutor::default());
    let source = BlockingSource {
        first: Some(observation(
            "99",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )),
    };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:risk-kill-pending-open",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_millis(250),
        )
        .with_account_risk(risk.clone()),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| executor.started.load(Ordering::SeqCst)).await;
    risk.engage_kill_switch("operator drill", base_time() + Duration::seconds(15))
        .await
        .unwrap();

    let result = tokio::time::timeout(StdDuration::from_secs(2), task.wait())
        .await
        .expect("a kill switch must not wait forever on a stuck adapter");
    assert!(matches!(
        result,
        Err(VolumeMakerPaperTaskError::RecoveryRequired)
    ));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(VolumeMakerPaperTaskFailure::RecoveryRequired)
    );
    let snapshot = account.decision_snapshot().await.unwrap();
    assert_eq!(snapshot.reservations.len(), 1);
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Uncertain
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"account_risk_directive_exit\""));
    assert!(body.contains("\"decision\":\"task_failed\""), "{body}");
    assert!(!body.contains("\"decision\":\"task_stopped\""), "{body}");
}

#[tokio::test]
async fn execution_failure_marks_reservation_uncertain_and_restart_fails_closed() {
    let (account, history, _) = account("execution-failure");
    let source = VecSource::new(vec![observation(
        "100",
        "101",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:failure",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        source,
        account.clone(),
        history.clone(),
        Arc::new(FailingExecutor),
    )
    .await
    .unwrap();

    let error = task.wait().await.unwrap_err();
    assert!(
        matches!(error, VolumeMakerPaperTaskError::RecoveryRequired),
        "unexpected execution failure: {error:?}"
    );
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(VolumeMakerPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(
        account.snapshot().await.unwrap().reservations[0].phase,
        PaperReservationPhase::Uncertain
    );

    let restart = VolumeMakerPaperTask::start(
        config(
            "volume:failure",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        VecSource::new(Vec::new()),
        account,
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        restart,
        VolumeMakerPaperTaskError::RecoveryRequired
    ));
}

#[tokio::test]
async fn source_end_after_an_open_leg_fails_recovery_instead_of_stopping_with_exposure() {
    let (account, history, path) = account("source-end-open");
    let source = VecSource::new(vec![observation(
        "100",
        "101",
        1,
        base_time() + Duration::seconds(10),
    )]);
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:source-end-open",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        source,
        account.clone(),
        history,
        Arc::new(FillExecutor::with_market_fill_prices(&["101"])),
    )
    .await
    .unwrap();

    let error = task.wait().await.unwrap_err();
    assert!(matches!(error, VolumeMakerPaperTaskError::RecoveryRequired));
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Failed);
    assert_eq!(
        task.status().failure,
        Some(VolumeMakerPaperTaskFailure::RecoveryRequired)
    );
    assert_eq!(task.status().operation_count, 1);
    assert_eq!(
        account.decision_snapshot().await.unwrap().open_lots.len(),
        1
    );
    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"decision\":\"task_failed\""), "{body}");
    assert!(!body.contains("\"decision\":\"task_stopped\""), "{body}");
}

#[tokio::test]
async fn cancel_during_unknown_execution_retains_capacity_without_release() {
    let (account, history, path) = account("cancel-unknown");
    let executor = Arc::new(PendingExecutor::default());
    let source = BlockingSource {
        first: Some(observation(
            "100",
            "101",
            1,
            base_time() + Duration::seconds(10),
        )),
    };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:cancel",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_millis(250),
        ),
        source,
        account.clone(),
        history,
        executor.clone(),
    )
    .await
    .unwrap();
    wait_until(|| executor.started.load(Ordering::SeqCst)).await;

    let error = task.cancel().await.unwrap_err();
    // The outer shutdown deadline may win the race with the owner recording
    // recovery-required. Both outcomes retain the unknown reservation.
    assert!(
        matches!(
            error,
            VolumeMakerPaperTaskError::RecoveryRequired
                | VolumeMakerPaperTaskError::ShutdownTimedOut
        ),
        "unexpected cancellation failure: {error:?}"
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
async fn max_cycles_bound_stops_with_completed_exit_and_clean_restart() {
    let (account, history, path) = account("max-cycles");
    let executor = Arc::new(FillExecutor::with_market_fill_prices(&["101", "100"]));
    let stepper = Arc::new(Semaphore::new(1));
    let source = SteppedSource::new(
        vec![
            observation("100", "101", 1, base_time() + Duration::seconds(10)),
            observation("100", "101", 2, base_time() + Duration::seconds(20)),
        ],
        Arc::clone(&stepper),
    );
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:bounded",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        )
        .with_max_cycles(1),
        source,
        account.clone(),
        history.clone(),
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::BoundsReached);
    assert_eq!(task.status().completed_cycle_count, 1);
    let durable = task.durable_status().await.unwrap();
    assert_eq!(durable.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(durable.recovery, ReadOnlyTaskRecovery::None);
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("\"exit\":\"completed\""), "{body}");

    // A terminal completed bound leaves the stable identity restartable.
    let mut second = VolumeMakerPaperTask::start(
        config(
            "volume:bounded",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        VecSource::new(Vec::new()),
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();
    assert_eq!(
        second.wait().await.unwrap(),
        VolumeMakerPaperTaskExit::SourceEnded
    );
    assert!(
        account.snapshot().await.unwrap().reservations.is_empty(),
        "restart after a bounded run should not retain released reservations"
    );
}

#[tokio::test]
async fn hour_rollover_journals_one_bounded_statistics_fact() {
    let (account, history, path) = account("hour-rollover");
    let executor = Arc::new(FillExecutor::with_market_fill_prices(&["101", "100"]));
    let stepper = Arc::new(Semaphore::new(1));
    // One completed cycle inside hour zero, then a depth-free observation in
    // hour one flushes the closed bucket without opening a new cycle.
    let source = SteppedSource::new(
        vec![
            observation("100", "101", 1, base_time() + Duration::minutes(10)),
            observation("100", "101", 2, base_time() + Duration::minutes(11)),
            depthless_observation("100", "101", 3, base_time() + Duration::minutes(65)),
        ],
        Arc::clone(&stepper),
    );
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:hourly",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_secs(1),
        ),
        source,
        account,
        history,
        executor.clone(),
    )
    .await
    .unwrap();

    let (exit, ()) = tokio::join!(task.wait(), async {
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 1).await;
        stepper.add_permits(1);
        wait_until(|| executor.calls.load(Ordering::SeqCst) == 2).await;
        stepper.add_permits(1);
    });
    assert_eq!(exit.unwrap(), VolumeMakerPaperTaskExit::SourceEnded);

    let body = std::fs::read_to_string(path).unwrap();
    assert!(body.contains("\"reason\":\"hour_rollover\""), "{body}");
    assert!(body.contains("\"completed_cycles\":1"), "{body}");
    // The fresh hour-one bucket saw no work, so stop exports nothing more.
    assert_eq!(
        body.matches("\"decision\":\"volume_maker_statistics\"")
            .count(),
        1,
        "{body}"
    );
}

#[tokio::test]
async fn stop_without_an_inflight_operation_is_durable_and_opens_no_reservation() {
    let (account, history, _) = account("stop-idle");
    let source = BlockingSource { first: None };
    let mut task = VolumeMakerPaperTask::start(
        config(
            "volume:idle",
            VolumeMakerMode::MarketImbalance,
            StdDuration::from_millis(250),
        ),
        source,
        account.clone(),
        history,
        Arc::new(FillExecutor::default()),
    )
    .await
    .unwrap();

    assert_eq!(
        task.stop().await.unwrap(),
        VolumeMakerPaperTaskExit::StopRequested
    );
    assert_eq!(task.status().phase, VolumeMakerPaperTaskPhase::Stopped);
    assert!(account.snapshot().await.unwrap().reservations.is_empty());
    assert_eq!(
        task.durable_status().await.unwrap().phase,
        ReadOnlyTaskPhase::Stopped
    );
}

// ---------------------------------------------------------------------------
// CLI contract: the four-mode `volume-maker` command surface.
// ---------------------------------------------------------------------------

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

fn control_token() -> &'static str {
    "0123456789abcdef0123456789abcdef"
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

const CLI_CONFIG: &str = "
volume_maker:
  exchange: paper
  symbol: BTC-USDT-PERP
  market_type: perpetual
  order_mode: market
  order_size: 0.5
  emergency_stop: false
";

fn write_cli_config(label: &str) -> PathBuf {
    let path = temp_path(label, "yaml");
    std::fs::write(&path, CLI_CONFIG).unwrap();
    path
}

fn write_account_risk_config(label: &str) -> PathBuf {
    let path = temp_path(label, "yaml");
    std::fs::write(
        &path,
        "max_symbol_exposure: 1000\nmax_total_exposure: 2000\nmin_balance_warning: 1000\nmin_balance_close_position: 500\nmax_position_duration_seconds: 86400\nmax_daily_trades: 10000\ndisabled_symbols: []\nhigh_risk_symbols: []\n",
    )
    .unwrap();
    path
}

#[test]
fn volume_maker_default_mode_validates_a_clean_config_and_succeeds() {
    let config = write_cli_config("volume-maker-validate");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("volume-maker")
        .arg(&config)
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("valid: volume-maker"), "{stdout}");
    assert!(stdout.contains("exchange=paper"), "{stdout}");
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());
}

#[test]
fn volume_maker_serve_without_replay_fails_closed() {
    let config = write_cli_config("volume-maker-no-replay");
    let history = temp_path("volume-maker-no-replay-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("volume-maker")
        .arg(&config)
        .args([
            "--mode",
            "serve",
            "--task-id",
            "volume-maker-no-replay",
            "--history-path",
            history.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("requires --replay"), "{stderr}");
    assert!(!history.exists());
}

#[test]
fn volume_maker_serve_without_explicit_account_risk_config_fails_closed() {
    let config = write_cli_config("volume-maker-no-account-risk");
    let replay = temp_path("volume-maker-no-account-risk-replay", "jsonl");
    std::fs::write(
        &replay,
        "{\"exchange\":\"paper\",\"symbol\":\"BTC-USDT-PERP\",\"market_type\":\"perpetual\",\"bid\":\"100\",\"ask\":\"101\",\"last\":\"100\",\"bid_quantity\":\"5\",\"ask_quantity\":\"2\",\"timestamp\":\"2026-07-25T00:00:00Z\"}\n",
    )
    .unwrap();
    let history = temp_path("volume-maker-no-account-risk-history", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .arg("volume-maker")
        .arg(&config)
        .args([
            "--mode",
            "serve",
            "--replay",
            replay.to_str().unwrap(),
            "--task-id",
            "volume-maker-no-account-risk",
            "--history-path",
            history.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    std::fs::remove_file(config).unwrap();
    std::fs::remove_file(replay).unwrap();
    if history.exists() {
        std::fs::remove_file(&history).unwrap();
    }
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("requires --paper-account-risk-config"),
        "{stderr}"
    );
}

#[test]
fn volume_maker_serve_runs_a_finite_replay_and_status_degrades_to_projection() {
    let config = write_cli_config("volume-maker-serve");
    let account_risk = write_account_risk_config("volume-maker-serve-account-risk");
    let replay = temp_path("volume-maker-serve-replay", "jsonl");
    let mut lines = String::new();
    for second in 0..6 {
        use std::fmt::Write as _;
        writeln!(
            lines,
            "{{\"exchange\":\"paper\",\"symbol\":\"BTC-USDT-PERP\",\"market_type\":\"perpetual\",\"bid\":\"100\",\"ask\":\"101\",\"last\":\"100\",\"bid_quantity\":\"5\",\"ask_quantity\":\"2\",\"timestamp\":\"2026-07-25T00:00:0{second}Z\"}}"
        )
        .unwrap();
    }
    std::fs::write(&replay, lines).unwrap();
    let history = temp_path("volume-maker-serve-history", "jsonl");
    let task_id = format!("volume-maker-serve-smoke-{}", std::process::id());
    let control_port = free_port();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .arg("volume-maker")
        .arg(&config)
        .args([
            "--mode",
            "serve",
            "--replay",
            replay.to_str().unwrap(),
            "--task-id",
            task_id.as_str(),
            "--history-path",
            history.to_str().unwrap(),
            "--paper-account-risk-config",
            account_risk.to_str().unwrap(),
            "--control-port",
            &control_port.to_string(),
            "--control-poll-interval-ms",
            "25",
            "--shutdown-grace-ms",
            "30000",
        ])
        .output()
        .unwrap();
    // The finite replay drains on its own; the serve loop then observes the
    // terminal owner and exits without any control interaction.
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("continuous volume-maker task started"),
        "{stdout}"
    );
    assert!(stdout.contains("phase=stopped"), "{stdout}");
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());

    let journal = std::fs::read_to_string(&history).unwrap();
    assert!(
        journal.contains("\"task_kind\":\"volume_maker\""),
        "{journal}"
    );
    for decision in [
        "\"decision\":\"task_registered\"",
        "\"decision\":\"task_running\"",
        "\"decision\":\"task_stopping\"",
        "\"decision\":\"task_stopped\"",
    ] {
        assert!(journal.contains(decision), "{journal}");
    }

    // With the control endpoint gone, status must degrade to the durable
    // journal projection instead of failing or fabricating liveness.
    let status = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .arg("volume-maker")
        .arg(&config)
        .args([
            "--mode",
            "status",
            "--task-id",
            task_id.as_str(),
            "--history-path",
            history.to_str().unwrap(),
            "--control-port",
            &control_port.to_string(),
        ])
        .output()
        .unwrap();
    assert!(status.status.success(), "{status:?}");
    let status_stdout = String::from_utf8(status.stdout).unwrap();
    assert!(status_stdout.contains("phase=stopped"), "{status_stdout}");
    assert!(status_stdout.contains("recovery=none"), "{status_stdout}");

    std::fs::remove_file(config).unwrap();
    std::fs::remove_file(account_risk).unwrap();
    std::fs::remove_file(replay).unwrap();
    std::fs::remove_file(history).unwrap();
}

fn checkpoint_count(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |body| {
        body.matches("\"decision\":\"task_checkpointed\"").count()
    })
}

async fn wait_until(predicate: impl Fn() -> bool) {
    for _ in 0..200 {
        if predicate() {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("condition was not observed within the test deadline");
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn temp_path(label: &str, extension: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-volume-maker-{label}-{}-{nonce}.{extension}",
        std::process::id()
    ))
}
