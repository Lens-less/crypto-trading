use std::{
    num::NonZeroUsize,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, OrderType, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeStatus,
    MarketSubscription, PaperExchange, ReconcileReceipt, ReconcileScope, SubscriptionReceipt,
    TradingCommand, TradingReceipt,
};
use crypto_trading_runtime::{
    ExchangeRouter, ExecutionBatch, ExecutionClock, ExecutionMode, ExecutionPolicy, IntentExecutor,
    LIVE_ACKNOWLEDGEMENT, MAX_EXECUTION_BATCH_ORDERS, RuntimeError,
};
use rust_decimal::Decimal;
use tokio::time::Instant as TokioInstant;
use uuid::Uuid;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn policy(now: chrono::DateTime<Utc>, snapshots: Vec<MarketSnapshot>) -> ExecutionPolicy {
    ExecutionPolicy::new(true, false, now, Duration::seconds(5), snapshots).unwrap()
}

fn snapshot_for(exchange: &str, symbol: Symbol, now: chrono::DateTime<Utc>) -> MarketSnapshot {
    MarketSnapshot::new(
        exchange,
        symbol,
        MarketType::Perpetual,
        Price::new(decimal("100")).unwrap(),
        Price::new(decimal("101")).unwrap(),
        now,
    )
    .unwrap()
}

fn market_intent(exchange: &str) -> OrderIntent {
    OrderIntent::market(
        exchange,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    )
}

fn assert_constructor_and_recovery_reject(intent: &OrderIntent, expected: &str) {
    let constructor_error = ExecutionBatch::new(Uuid::new_v4(), vec![intent.clone()]).unwrap_err();
    assert!(
        constructor_error.to_string().contains(expected),
        "unexpected constructor error: {constructor_error}"
    );

    let payload = serde_json::json!({
        "id": Uuid::new_v4(),
        "intents": [intent],
    });
    let recovery_error = serde_json::from_value::<ExecutionBatch>(payload).unwrap_err();
    assert!(
        recovery_error.to_string().contains(expected),
        "unexpected recovery error: {recovery_error}"
    );
}

#[test]
fn planned_empty_batches_receive_unique_non_nil_ids() {
    let first = ExecutionBatch::planned(Vec::new()).unwrap();
    let second = ExecutionBatch::planned(Vec::new()).unwrap();

    assert!(!first.id().is_nil());
    assert!(!second.id().is_nil());
    assert_ne!(first.id(), second.id());
}

#[test]
fn execution_batch_constructor_rejects_a_nil_batch_id() {
    let error = ExecutionBatch::new(Uuid::nil(), Vec::new()).unwrap_err();

    assert!(matches!(error, RuntimeError::InvalidExecutionBatchId));
}

#[test]
fn execution_batch_recovery_rejects_a_nil_batch_id() {
    let payload = serde_json::json!({
        "id": Uuid::nil(),
        "intents": [],
    });

    let error = serde_json::from_value::<ExecutionBatch>(payload).unwrap_err();

    assert!(error.to_string().contains("batch id must not be nil"));
}

#[test]
fn execution_batch_constructor_rejects_a_nil_client_order_id() {
    let mut intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    intent.client_order_id = Uuid::nil();

    let error = ExecutionBatch::new(Uuid::new_v4(), vec![intent]).unwrap_err();

    assert!(matches!(error, RuntimeError::InvalidClientOrderId));
}

#[test]
fn execution_batch_recovery_rejects_a_nil_client_order_id() {
    let mut intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    intent.client_order_id = Uuid::nil();
    let payload = serde_json::json!({
        "id": Uuid::new_v4(),
        "intents": [intent],
    });

    let error = serde_json::from_value::<ExecutionBatch>(payload).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("client order id must not be nil")
    );
}

#[test]
fn execution_batch_recovery_round_trip_preserves_client_order_ids() {
    let intents = vec![
        OrderIntent::market(
            "paper",
            Symbol::new("BTC-USDT").unwrap(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(decimal("0.01")).unwrap(),
        ),
        OrderIntent::market(
            "paper",
            Symbol::new("ETH-USDT").unwrap(),
            MarketType::Perpetual,
            Side::Sell,
            Quantity::new(decimal("0.02")).unwrap(),
        ),
    ];
    let expected_client_ids = intents
        .iter()
        .map(|intent| intent.client_order_id)
        .collect::<Vec<_>>();
    let batch = ExecutionBatch::planned(intents).unwrap();

    let encoded = serde_json::to_vec(&batch).unwrap();
    let recovered: ExecutionBatch = serde_json::from_slice(&encoded).unwrap();
    let recovered_client_ids = recovered
        .intents()
        .iter()
        .map(|intent| intent.client_order_id)
        .collect::<Vec<_>>();

    assert_eq!(recovered.id(), batch.id());
    assert_eq!(recovered_client_ids, expected_client_ids);
}

#[test]
fn execution_batch_recovery_rejects_duplicate_and_oversized_payloads() {
    let intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    let duplicate_payload = serde_json::json!({
        "id": Uuid::new_v4(),
        "intents": [intent.clone(), intent.clone()],
    });
    let duplicate_error = serde_json::from_value::<ExecutionBatch>(duplicate_payload).unwrap_err();
    assert!(
        duplicate_error
            .to_string()
            .contains("appears more than once")
    );

    let oversized_payload = serde_json::json!({
        "id": Uuid::new_v4(),
        "intents": vec![intent; MAX_EXECUTION_BATCH_ORDERS + 1],
    });
    let oversized_json = serde_json::to_vec(&oversized_payload).unwrap();
    let oversized_error = serde_json::from_slice::<ExecutionBatch>(&oversized_json).unwrap_err();
    assert!(oversized_error.to_string().contains("maximum is"));
}

#[test]
fn execution_batch_limit_is_256_and_constructor_rejects_257_orders() {
    assert_eq!(MAX_EXECUTION_BATCH_ORDERS, 256);

    let at_limit = (0..MAX_EXECUTION_BATCH_ORDERS)
        .map(|_| market_intent("paper"))
        .collect();
    assert!(ExecutionBatch::planned(at_limit).is_ok());

    let over_limit = (0..=MAX_EXECUTION_BATCH_ORDERS)
        .map(|_| market_intent("paper"))
        .collect();
    let error = ExecutionBatch::planned(over_limit).unwrap_err();
    assert!(matches!(
        error,
        RuntimeError::BatchTooLarge {
            count: 257,
            limit: 256
        }
    ));
}

#[test]
fn execution_batch_constructor_and_recovery_share_order_invariants() {
    let mut empty_exchange = market_intent("paper");
    empty_exchange.exchange = "   ".to_owned();
    assert_constructor_and_recovery_reject(&empty_exchange, "exchange must not be empty");

    let mut zero_quantity = market_intent("paper");
    zero_quantity.quantity = Quantity::default();
    assert_constructor_and_recovery_reject(&zero_quantity, "quantity must be greater than zero");

    let mut priced_market = market_intent("paper");
    priced_market.price = Some(Price::new(decimal("100")).unwrap());
    assert_constructor_and_recovery_reject(
        &priced_market,
        "market orders must not include a limit price",
    );

    let mut unpriced_limit = market_intent("paper");
    unpriced_limit.order_type = OrderType::Limit;
    assert_constructor_and_recovery_reject(&unpriced_limit, "limit orders require a limit price");

    let mut post_only_market = market_intent("paper");
    post_only_market.time_in_force = TimeInForce::PostOnly;
    assert_constructor_and_recovery_reject(&post_only_market, "market orders cannot be post-only");
}

#[test]
fn execution_batch_recovery_requires_every_order_field() {
    let intent = serde_json::to_value(market_intent("paper")).unwrap();
    for required in [
        "client_order_id",
        "exchange",
        "symbol",
        "market_type",
        "side",
        "order_type",
        "quantity",
        "price",
        "reduce_only",
        "time_in_force",
    ] {
        let mut missing = intent.clone();
        missing.as_object_mut().unwrap().remove(required);
        let payload = serde_json::json!({
            "id": Uuid::new_v4(),
            "intents": [missing],
        });
        let result = serde_json::from_value::<ExecutionBatch>(payload);
        assert!(
            result.is_err(),
            "missing {required} was accepted: {result:?}"
        );
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(required),
            "missing {required} produced unexpected error: {error}"
        );
    }
}

#[test]
fn execution_batch_recovery_rejects_misspelled_reduce_only() {
    let mut intent = serde_json::to_value(market_intent("paper")).unwrap();
    let intent = intent.as_object_mut().unwrap();
    intent.remove("reduce_only");
    intent.insert("reduce_onli".to_owned(), serde_json::Value::Bool(false));
    let payload = serde_json::json!({
        "id": Uuid::new_v4(),
        "intents": [intent],
    });

    let error = serde_json::from_value::<ExecutionBatch>(payload).unwrap_err();
    assert!(error.to_string().contains("unknown field"), "{error}");
}

struct StatusOnlyExchange {
    exchange: String,
    mode: ExchangeMode,
    availability: ExchangeAvailability,
    execute_calls: AtomicUsize,
}

impl StatusOnlyExchange {
    fn new(
        exchange: impl Into<String>,
        mode: ExchangeMode,
        availability: ExchangeAvailability,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            mode,
            availability,
            execute_calls: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug)]
struct FakeExecutionClock {
    now: Mutex<chrono::DateTime<Utc>>,
}

impl FakeExecutionClock {
    fn new(now: chrono::DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

impl ExecutionClock for FakeExecutionClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

struct AdvancingFirstPaperExchange {
    inner: PaperExchange,
    calls: AtomicUsize,
    clock: Arc<FakeExecutionClock>,
}

struct DeadlineRecordingExchange {
    direct_execute_calls: AtomicUsize,
    execute_before_calls: AtomicUsize,
    remaining_millis: AtomicUsize,
}

struct RecordingPaperExchange {
    inner: PaperExchange,
    fail_execute: bool,
    reconcile_scopes: Mutex<Vec<ReconcileScope>>,
    execute_calls: AtomicUsize,
}

impl RecordingPaperExchange {
    fn new(exchange: &str, fail_execute: bool) -> Self {
        Self {
            inner: PaperExchange::new(exchange, NonZeroUsize::new(8).unwrap()).unwrap(),
            fail_execute,
            reconcile_scopes: Mutex::new(Vec::new()),
            execute_calls: AtomicUsize::new(0),
        }
    }

    async fn publish_snapshot(&self, snapshot: MarketSnapshot) {
        self.inner.publish_snapshot(snapshot).await.unwrap();
    }

    fn reconcile_scopes(&self) -> Vec<ReconcileScope> {
        self.reconcile_scopes.lock().unwrap().clone()
    }
}

impl DeadlineRecordingExchange {
    const fn new() -> Self {
        Self {
            direct_execute_calls: AtomicUsize::new(0),
            execute_before_calls: AtomicUsize::new(0),
            remaining_millis: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ExchangeHandle for DeadlineRecordingExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        self.direct_execute_calls.fetch_add(1, Ordering::SeqCst);
        Err(ExchangeError::rejected(
            "runtime bypassed the freshness-aware execution seam",
        ))
    }

    async fn execute_before(
        &self,
        _command: TradingCommand,
        deadline: TokioInstant,
    ) -> Result<TradingReceipt, ExchangeError> {
        self.execute_before_calls.fetch_add(1, Ordering::SeqCst);
        let remaining = deadline.saturating_duration_since(TokioInstant::now());
        self.remaining_millis.store(
            usize::try_from(remaining.as_millis()).unwrap_or(usize::MAX),
            Ordering::SeqCst,
        );
        Err(ExchangeError::rejected("recorded freshness deadline"))
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        Ok(ReconcileReceipt {
            scope,
            orders: Vec::new(),
            foreign_orders: Vec::new(),
            positions: Vec::new(),
            observed_at: Utc::now(),
        })
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        panic!("not used")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Ok(ExchangeStatus {
            exchange: "deadline".to_owned(),
            mode: ExchangeMode::Paper,
            availability: ExchangeAvailability::Ready,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}

#[async_trait]
impl ExchangeHandle for AdvancingFirstPaperExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        let is_first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let result = self.inner.execute(command).await;
        if is_first && result.is_ok() {
            self.clock.advance(Duration::milliseconds(11));
        }
        result
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        self.inner.reconcile(scope).await
    }

    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        self.inner.subscribe(subscription).await
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        self.inner.status().await
    }
}

#[async_trait]
impl ExchangeHandle for StatusOnlyExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        Err(ExchangeError::rejected(
            "status-only adapter must not execute in readiness tests",
        ))
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        panic!("not used")
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        panic!("not used")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Ok(ExchangeStatus {
            exchange: self.exchange.clone(),
            mode: self.mode,
            availability: self.availability,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}

#[async_trait]
impl ExchangeHandle for RecordingPaperExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_execute {
            return Err(ExchangeError::rejected("scripted adapter failure"));
        }
        self.inner.execute(command).await
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        self.reconcile_scopes.lock().unwrap().push(scope.clone());
        self.inner.reconcile(scope).await
    }

    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        self.inner.subscribe(subscription).await
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        self.inner.status().await
    }
}

#[tokio::test]
async fn intent_executor_empty_batches_still_require_authority_and_operator_policy() {
    let now = Utc::now();
    let exchange = Arc::new(StatusOnlyExchange::new(
        "status-only",
        ExchangeMode::Paper,
        ExchangeAvailability::Unavailable,
    ));
    let live_mode = ExecutionMode::live(Some(LIVE_ACKNOWLEDGEMENT)).unwrap();

    let live_error = IntentExecutor::new(Arc::clone(&exchange), live_mode, policy(now, Vec::new()))
        .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(live_error, RuntimeError::LiveExecutionUnavailable));

    let monitor_error = IntentExecutor::new(
        Arc::clone(&exchange),
        ExecutionMode::Monitor,
        policy(now, Vec::new()),
    )
    .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
    .await
    .unwrap_err();
    assert!(matches!(monitor_error, RuntimeError::ModeDisallowsOrders));

    let disabled_policy =
        ExecutionPolicy::new(false, false, now, Duration::seconds(5), Vec::new()).unwrap();
    let disabled_error =
        IntentExecutor::new(Arc::clone(&exchange), ExecutionMode::Paper, disabled_policy)
            .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
            .await
            .unwrap_err();
    assert!(matches!(disabled_error, RuntimeError::ExecutionDisabled));

    let receipts = IntentExecutor::new(exchange, ExecutionMode::Paper, policy(now, Vec::new()))
        .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
        .await
        .unwrap();
    assert!(receipts.is_empty());
}

#[tokio::test]
async fn exchange_router_empty_batches_still_require_authority_and_operator_policy() {
    let now = Utc::now();
    let live_mode = ExecutionMode::live(Some(LIVE_ACKNOWLEDGEMENT)).unwrap();

    let live_error = ExchangeRouter::new(live_mode, policy(now, Vec::new()))
        .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(live_error, RuntimeError::LiveExecutionUnavailable));

    let monitor_error = ExchangeRouter::new(ExecutionMode::Monitor, policy(now, Vec::new()))
        .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(monitor_error, RuntimeError::ModeDisallowsOrders));

    let disabled_policy =
        ExecutionPolicy::new(false, false, now, Duration::seconds(5), Vec::new()).unwrap();
    let disabled_error = ExchangeRouter::new(ExecutionMode::Paper, disabled_policy)
        .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(disabled_error, RuntimeError::ExecutionDisabled));

    let receipts = ExchangeRouter::new(ExecutionMode::Paper, policy(now, Vec::new()))
        .execute_batch(ExecutionBatch::planned(Vec::new()).unwrap())
        .await
        .unwrap();
    assert!(receipts.is_empty());
}

#[tokio::test]
async fn paper_executor_submits_intents_through_the_exchange_seam() {
    let paper = Arc::new(PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap());
    let symbol = Symbol::new("BTC-USDT").unwrap();
    let now = Utc::now();
    let snapshot = snapshot_for("paper", symbol.clone(), now);
    paper.publish_snapshot(snapshot.clone()).await.unwrap();

    let executor = IntentExecutor::new(
        Arc::clone(&paper),
        ExecutionMode::Paper,
        policy(now, vec![snapshot]),
    );
    let receipts = executor
        .execute_all(vec![OrderIntent::market(
            "paper",
            symbol.clone(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(decimal("0.01")).unwrap(),
        )])
        .await
        .unwrap();

    assert_eq!(receipts.len(), 1);
    let account = paper
        .reconcile(ReconcileScope::Orders {
            symbol: Some(symbol),
        })
        .await
        .unwrap();
    assert_eq!(account.orders.len(), 1);
}

#[tokio::test]
async fn intent_executor_rejects_mixed_exchange_batches_before_submission() {
    let exchange = Arc::new(StatusOnlyExchange::new(
        "paper",
        ExchangeMode::Paper,
        ExchangeAvailability::Ready,
    ));
    let now = Utc::now();
    let first = market_intent("paper");
    let second = market_intent("other");
    let policy = policy(
        now,
        vec![
            snapshot_for("paper", first.symbol.clone(), now),
            snapshot_for("other", second.symbol.clone(), now),
        ],
    );

    let error = IntentExecutor::new(Arc::clone(&exchange), ExecutionMode::Paper, policy)
        .execute_all(vec![first, second])
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeError::MixedExchangeBatch {
            index: 1,
            ref expected,
            ref actual,
        } if expected == "paper" && actual == "other"
    ));
    assert_eq!(exchange.execute_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn intent_executor_rejects_adapter_identity_mismatch_before_submission() {
    let exchange = Arc::new(StatusOnlyExchange::new(
        "actual",
        ExchangeMode::Paper,
        ExchangeAvailability::Ready,
    ));
    let now = Utc::now();
    let intent = market_intent("expected");
    let policy = policy(
        now,
        vec![snapshot_for("expected", intent.symbol.clone(), now)],
    );

    let error = IntentExecutor::new(Arc::clone(&exchange), ExecutionMode::Paper, policy)
        .execute_all(vec![intent])
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeError::AdapterIdentityMismatch {
            ref expected,
            ref actual,
        } if expected == "expected" && actual == "actual"
    ));
    assert_eq!(exchange.execute_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn exchange_router_rejects_registration_identity_mismatch_before_submission() {
    let exchange = Arc::new(StatusOnlyExchange::new(
        "actual",
        ExchangeMode::Paper,
        ExchangeAvailability::Ready,
    ));
    let now = Utc::now();
    let intent = market_intent("registered");
    let policy = policy(
        now,
        vec![snapshot_for("registered", intent.symbol.clone(), now)],
    );
    let mut router = ExchangeRouter::new(ExecutionMode::Paper, policy);
    router.register("registered", Arc::clone(&exchange));

    let error = router.execute_all(vec![intent]).await.unwrap_err();

    assert!(matches!(
        error,
        RuntimeError::AdapterIdentityMismatch {
            ref expected,
            ref actual,
        } if expected == "registered" && actual == "actual"
    ));
    assert_eq!(exchange.execute_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn monitor_mode_cannot_cross_the_order_seam() {
    let paper = Arc::new(PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap());
    let executor = IntentExecutor::new(
        paper,
        ExecutionMode::Monitor,
        policy(Utc::now(), Vec::new()),
    );
    let intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );

    let error = executor.execute_all(vec![intent]).await.unwrap_err();
    assert!(matches!(error, RuntimeError::ModeDisallowsOrders));
}

#[tokio::test]
async fn partial_execution_preserves_confirmed_receipts_and_failed_intent() {
    let paper = Arc::new(PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap());
    let symbol = Symbol::new("BTC-USDT").unwrap();
    let now = Utc::now();
    let snapshot = snapshot_for("paper", symbol.clone(), now);
    paper.publish_snapshot(snapshot.clone()).await.unwrap();
    let first = OrderIntent::market(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    let mut failed = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    failed.reduce_only = true;

    let policy =
        ExecutionPolicy::new(true, false, now, Duration::seconds(5), vec![snapshot]).unwrap();
    let error = IntentExecutor::new(paper, ExecutionMode::Paper, policy)
        .execute_all(vec![first, failed.clone()])
        .await
        .unwrap_err();
    let RuntimeError::PartialExecution {
        completed,
        failed_intent,
        ..
    } = error
    else {
        panic!("expected a structured partial execution outcome");
    };
    assert_eq!(completed.len(), 1);
    assert_eq!(*failed_intent, failed);
}

#[tokio::test]
async fn unavailable_adapter_is_rejected_before_order_submission() {
    let exchange = Arc::new(StatusOnlyExchange::new(
        "status-only",
        ExchangeMode::Paper,
        ExchangeAvailability::Unavailable,
    ));
    let intent = OrderIntent::market(
        "status-only",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );

    let now = Utc::now();
    let policy = policy(
        now,
        vec![snapshot_for("status-only", intent.symbol.clone(), now)],
    );
    let error = IntentExecutor::new(exchange, ExecutionMode::Paper, policy)
        .execute_all(vec![intent])
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::AdapterUnavailable { .. }));
}

#[tokio::test]
async fn live_execution_remains_closed_even_for_a_ready_live_adapter() {
    let exchange = Arc::new(StatusOnlyExchange::new(
        "status-only",
        ExchangeMode::Live,
        ExchangeAvailability::Ready,
    ));
    let intent = OrderIntent::market(
        "status-only",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    let mode = ExecutionMode::live(Some(LIVE_ACKNOWLEDGEMENT)).unwrap();

    let error = IntentExecutor::new(exchange, mode, policy(Utc::now(), Vec::new()))
        .execute_all(vec![intent])
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::LiveExecutionUnavailable));
}

#[tokio::test]
async fn partial_execution_keeps_batch_identity_index_and_unattempted_intents() {
    let now = Utc::now();
    let paper = Arc::new(PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap());
    let symbol = Symbol::new("BTC-USDT").unwrap();
    let snapshot = MarketSnapshot::new(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Price::new(decimal("100")).unwrap(),
        Price::new(decimal("101")).unwrap(),
        now,
    )
    .unwrap();
    paper.publish_snapshot(snapshot.clone()).await.unwrap();

    let first = OrderIntent::market(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    let mut failed = OrderIntent::market(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    failed.reduce_only = true;
    let unattempted = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        Quantity::new(decimal("1")).unwrap(),
    );
    let batch_id = Uuid::new_v4();
    let batch =
        ExecutionBatch::new(batch_id, vec![first, failed.clone(), unattempted.clone()]).unwrap();
    let policy =
        ExecutionPolicy::new(true, false, now, Duration::seconds(5), vec![snapshot]).unwrap();

    let error = IntentExecutor::new(paper, ExecutionMode::Paper, policy)
        .execute_batch(batch)
        .await
        .unwrap_err();
    let RuntimeError::PartialExecution {
        batch_id: actual_batch_id,
        failed_index,
        completed,
        failed_intent,
        unattempted: actual_unattempted,
        reconciliation,
        ..
    } = error
    else {
        panic!("expected a structured partial execution outcome");
    };

    assert_eq!(actual_batch_id, batch_id);
    assert_eq!(failed_index, 1);
    assert_eq!(completed.len(), 1);
    assert_eq!(*failed_intent, failed);
    assert_eq!(actual_unattempted, vec![unattempted]);
    assert_eq!(reconciliation.len(), 1);
    assert!(reconciliation[0].result.is_ok());
}

#[tokio::test]
async fn exchange_router_partial_execution_reconciles_each_prevalidated_adapter_once() {
    let now = Utc::now();
    let symbol = Symbol::new("BTC-USDT").unwrap();
    let alpha = Arc::new(RecordingPaperExchange::new("alpha", false));
    let beta = Arc::new(RecordingPaperExchange::new("beta", true));
    alpha
        .publish_snapshot(snapshot_for("alpha", symbol.clone(), now))
        .await;
    beta.publish_snapshot(snapshot_for("beta", symbol.clone(), now))
        .await;

    let first = OrderIntent::market(
        "alpha",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    let failed = OrderIntent::market(
        "beta",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Sell,
        Quantity::new(decimal("0.02")).unwrap(),
    );
    let unattempted = OrderIntent::market(
        "alpha",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.03")).unwrap(),
    );
    let batch_id = Uuid::new_v4();
    let batch = ExecutionBatch::new(
        batch_id,
        vec![first.clone(), failed.clone(), unattempted.clone()],
    )
    .unwrap();
    let policy = policy(
        now,
        vec![
            snapshot_for("alpha", symbol.clone(), now),
            snapshot_for("beta", symbol, now),
        ],
    );
    let mut router = ExchangeRouter::new(ExecutionMode::Paper, policy);
    router.register("alpha", Arc::clone(&alpha));
    router.register("beta", Arc::clone(&beta));

    let error = router.execute_batch(batch).await.unwrap_err();
    let RuntimeError::PartialExecution {
        batch_id: actual_batch_id,
        failed_index,
        completed,
        failed_intent,
        unattempted: actual_unattempted,
        reconciliation,
        ..
    } = error
    else {
        panic!("expected a structured partial execution outcome");
    };

    assert_eq!(actual_batch_id, batch_id);
    assert_eq!(failed_index, 1);
    assert_eq!(completed.len(), 1);
    assert_eq!(*failed_intent, failed);
    assert_eq!(actual_unattempted, vec![unattempted]);
    assert_eq!(reconciliation.len(), 2);
    assert!(reconciliation.iter().all(|entry| entry.result.is_ok()));
    assert_eq!(alpha.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(beta.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(alpha.reconcile_scopes(), vec![ReconcileScope::All]);
    assert_eq!(beta.reconcile_scopes(), vec![ReconcileScope::All]);
}

#[tokio::test]
async fn market_freshness_is_rechecked_before_each_leg_submission() {
    let now = Utc::now();
    let symbol = Symbol::new("BTC-USDT").unwrap();
    let snapshot = snapshot_for("paper", symbol.clone(), now);
    let paper = PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap();
    paper.publish_snapshot(snapshot.clone()).await.unwrap();
    let clock = Arc::new(FakeExecutionClock::new(now));
    let exchange = Arc::new(AdvancingFirstPaperExchange {
        inner: paper,
        calls: AtomicUsize::new(0),
        clock: Arc::clone(&clock),
    });
    let intents = vec![
        OrderIntent::market(
            "paper",
            symbol.clone(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(decimal("0.01")).unwrap(),
        ),
        OrderIntent::market(
            "paper",
            symbol,
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(decimal("0.01")).unwrap(),
        ),
    ];
    let policy = ExecutionPolicy::new(true, false, now, Duration::milliseconds(10), vec![snapshot])
        .unwrap()
        .with_clock(clock);

    let error = IntentExecutor::new(exchange, ExecutionMode::Paper, policy)
        .execute_all(intents)
        .await
        .unwrap_err();
    let RuntimeError::PartialExecution {
        failed_index,
        completed,
        source,
        ..
    } = error
    else {
        panic!("expected freshness expiry to preserve a partial outcome");
    };
    assert_eq!(failed_index, 1);
    assert_eq!(completed.len(), 1);
    assert!(matches!(*source, RuntimeError::StaleMarketData { .. }));
}

#[tokio::test]
async fn runtime_passes_remaining_market_freshness_to_the_exchange_dispatch_seam() {
    let now = Utc::now();
    let symbol = Symbol::new("BTC-USDT").unwrap();
    let snapshot = snapshot_for("deadline", symbol.clone(), now - Duration::seconds(3));
    let exchange = Arc::new(DeadlineRecordingExchange::new());
    let intent = OrderIntent::market(
        "deadline",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    let policy =
        ExecutionPolicy::new(true, false, now, Duration::seconds(5), vec![snapshot]).unwrap();

    let error = IntentExecutor::new(Arc::clone(&exchange), ExecutionMode::Paper, policy)
        .execute_all(vec![intent])
        .await
        .unwrap_err();

    assert!(matches!(error, RuntimeError::PartialExecution { .. }));
    assert_eq!(exchange.direct_execute_calls.load(Ordering::SeqCst), 0);
    assert_eq!(exchange.execute_before_calls.load(Ordering::SeqCst), 1);
    let remaining_millis = exchange.remaining_millis.load(Ordering::SeqCst);
    assert!(remaining_millis > 0);
    assert!(remaining_millis <= 2_000);
}

#[test]
fn execution_policy_rejects_stale_future_and_unconfigured_instruments() {
    let now = Utc::now();
    let target = MarketSnapshot::new(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("100")).unwrap(),
        Price::new(decimal("101")).unwrap(),
        now - Duration::seconds(6),
    )
    .unwrap();
    let other = MarketSnapshot::new(
        "paper",
        Symbol::new("ETH-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("10")).unwrap(),
        Price::new(decimal("11")).unwrap(),
        now,
    )
    .unwrap();
    let policy =
        ExecutionPolicy::new(true, false, now, Duration::seconds(5), vec![target, other]).unwrap();
    let target_intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    assert!(matches!(
        policy.validate(std::slice::from_ref(&target_intent)),
        Err(RuntimeError::StaleMarketData { .. })
    ));

    let unknown = OrderIntent::market(
        "paper",
        Symbol::new("SOL-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    assert!(matches!(
        policy.validate(&[unknown]),
        Err(RuntimeError::MissingMarketData { .. })
    ));

    let future_snapshot = MarketSnapshot::new(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("100")).unwrap(),
        Price::new(decimal("101")).unwrap(),
        now + Duration::milliseconds(1),
    )
    .unwrap();
    let future = ExecutionPolicy::new(
        true,
        false,
        now,
        Duration::seconds(5),
        vec![future_snapshot],
    )
    .unwrap();
    assert!(matches!(
        future.validate(&[target_intent]),
        Err(RuntimeError::FutureMarketData { .. })
    ));
}
