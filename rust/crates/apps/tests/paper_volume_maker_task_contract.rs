use std::{
    collections::VecDeque,
    future::pending,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
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
    let path = temp_path(label, "jsonl");
    let history = JsonlHistory::new(&path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new(EXCHANGE, Money::new(decimal("10000"))).unwrap(),
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
struct PendingExecutor {
    started: AtomicBool,
}

impl VolumeMakerPaperExecutor for PendingExecutor {
    fn execute(&self, _batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        self.started.store(true, Ordering::SeqCst);
        Box::pin(pending())
    }
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
fn volume_maker_serve_runs_a_finite_replay_and_status_degrades_to_projection() {
    let config = write_cli_config("volume-maker-serve");
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

    let child = Command::new(binary())
        .current_dir(repo_root())
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
            "--control-port",
            &control_port.to_string(),
            "--control-poll-interval-ms",
            "25",
            "--shutdown-grace-ms",
            "30000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // The finite replay drains on its own; the serve loop then observes the
    // terminal owner and exits without any control interaction.
    let output = wait_with_output(child, StdDuration::from_secs(120));
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

fn wait_with_output(mut child: Child, timeout: StdDuration) -> Output {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child did not exit in time"
        );
        std::thread::sleep(StdDuration::from_millis(50));
    }
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
