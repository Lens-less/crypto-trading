use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use crypto_trading_domain::{MarketType, Order, OrderIntent, OrderStatus, Quantity, Symbol};
use crypto_trading_exchange::{
    BoundedExchangeHandle, ExchangeError, ExchangeHandle, ExchangeOperation, ExchangeOperationKey,
    ExchangeStatus, MarketSubscription, PaperExchange, ReconcileReceipt, ReconcileScope,
    SubmissionDisposition, SubscriptionReceipt, TradingCommand, TradingReceipt,
};
use tokio::sync::Notify;
use tokio::time::{Instant, sleep};

struct BlockingExchange {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    execute_count: AtomicUsize,
}

struct PanickingExchange;

struct SlowExchange {
    entered: Arc<Notify>,
    delay: Duration,
}

struct PriorityExchange {
    submit_count: AtomicUsize,
    first_submit_entered: Arc<Notify>,
    release_first_submit: Arc<Notify>,
    calls: Arc<StdMutex<Vec<&'static str>>>,
}

struct DeadlineDispatchExchange {
    submit_count: AtomicUsize,
    first_submit_entered: Arc<Notify>,
    release_first_submit: Arc<Notify>,
    cancel_entered: Arc<Notify>,
    release_cancel: Arc<Notify>,
}

struct QuarantineExchange {
    execute_count: AtomicUsize,
    first_submit_entered: Arc<Notify>,
    first_delay: Duration,
    reconcile_count: AtomicUsize,
    reconcile_observed_at: StdMutex<VecDeque<chrono::DateTime<chrono::Utc>>>,
    reconcile_scope_override: Option<ReconcileScope>,
}

struct DelayedPaperExchange {
    inner: PaperExchange,
    first_execute_entered: Arc<Notify>,
    release_first_execute: Arc<Notify>,
    execute_count: AtomicUsize,
}

struct FreshnessExchange {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    execute_before_count: AtomicUsize,
}

#[async_trait]
impl ExchangeHandle for BlockingExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        let index = self.execute_count.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Err(ExchangeError::rejected("released test request"))
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        Ok(ReconcileReceipt {
            scope,
            orders: Vec::new(),
            positions: Vec::new(),
            observed_at: chrono::Utc::now(),
        })
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Err(ExchangeError::rejected("blocking exchange status barrier"))
    }
}

#[async_trait]
impl ExchangeHandle for PanickingExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        panic!("simulate an adapter process loss after request admission");
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        unreachable!("not used by this contract test")
    }
}

#[async_trait]
impl ExchangeHandle for SlowExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        self.entered.notify_one();
        sleep(self.delay).await;
        Err(ExchangeError::rejected("slow test request completed"))
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        unreachable!("not used by this contract test")
    }
}

#[async_trait]
impl ExchangeHandle for PriorityExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        match command {
            TradingCommand::Submit(_) => {
                let index = self.submit_count.fetch_add(1, Ordering::SeqCst);
                self.calls
                    .lock()
                    .expect("test call log must not be poisoned")
                    .push(if index == 0 { "submit-1" } else { "submit-2" });
                if index == 0 {
                    self.first_submit_entered.notify_one();
                    self.release_first_submit.notified().await;
                }
            }
            TradingCommand::Cancel { .. } => {
                self.calls
                    .lock()
                    .expect("test call log must not be poisoned")
                    .push("cancel");
            }
            TradingCommand::CancelAll { .. } => {
                self.calls
                    .lock()
                    .expect("test call log must not be poisoned")
                    .push("cancel-all");
            }
        }
        Err(ExchangeError::rejected("recorded test request"))
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        unreachable!("not used by this contract test")
    }
}

#[async_trait]
impl ExchangeHandle for DeadlineDispatchExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        match command {
            TradingCommand::Submit(_) => {
                let index = self.submit_count.fetch_add(1, Ordering::SeqCst);
                if index == 0 {
                    self.first_submit_entered.notify_one();
                    self.release_first_submit.notified().await;
                }
            }
            TradingCommand::Cancel { .. } => {
                self.cancel_entered.notify_one();
                self.release_cancel.notified().await;
            }
            TradingCommand::CancelAll { .. } => {}
        }
        Err(ExchangeError::rejected("recorded deadline test request"))
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Err(ExchangeError::rejected("deadline test status barrier"))
    }
}

#[async_trait]
impl ExchangeHandle for QuarantineExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        let index = self.execute_count.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            self.first_submit_entered.notify_one();
            sleep(self.first_delay).await;
        }
        Err(ExchangeError::rejected("recorded quarantine test request"))
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        self.reconcile_count.fetch_add(1, Ordering::SeqCst);
        let observed_at = self
            .reconcile_observed_at
            .lock()
            .expect("test reconcile watermark lock must not be poisoned")
            .pop_front()
            .unwrap_or_else(chrono::Utc::now);
        Ok(ReconcileReceipt {
            scope: self.reconcile_scope_override.clone().unwrap_or(scope),
            orders: Vec::new(),
            positions: Vec::new(),
            observed_at,
        })
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        unreachable!("not used by this contract test")
    }
}

#[async_trait]
impl ExchangeHandle for DelayedPaperExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        if self.execute_count.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_execute_entered.notify_one();
            self.release_first_execute.notified().await;
        }
        self.inner.execute(command).await
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
impl ExchangeHandle for FreshnessExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        self.entered.notify_one();
        Err(ExchangeError::rejected(
            "bounded handle bypassed execute_before",
        ))
    }

    async fn execute_before(
        &self,
        command: TradingCommand,
        deadline: Instant,
    ) -> Result<TradingReceipt, ExchangeError> {
        if Instant::now() >= deadline {
            return Err(ExchangeError::rejected(
                "freshness exchange was polled after its dispatch deadline",
            ));
        }
        self.execute_before_count.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;

        let TradingCommand::Submit(intent) = command else {
            return Err(ExchangeError::rejected(
                "freshness exchange expected a submit",
            ));
        };
        let now = chrono::Utc::now();
        Ok(TradingReceipt::Submitted {
            order: Order {
                id: "freshness-test-order".to_owned(),
                intent,
                filled_quantity: Quantity::default(),
                average_fill_price: None,
                status: OrderStatus::Open,
                created_at: now,
                updated_at: now,
            },
            disposition: SubmissionDisposition::Open,
        })
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        unreachable!("not used by this contract test")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        unreachable!("not used by this contract test")
    }
}

fn command() -> TradingCommand {
    command_for(
        "4d36e96e-e325-11ce-bfc1-08002be10318",
        "BTC-USDT",
        "perpetual",
    )
}

fn command_for(client_order_id: &str, symbol: &str, market_type: &str) -> TradingCommand {
    let intent: OrderIntent = serde_json::from_value(serde_json::json!({
        "client_order_id": client_order_id,
        "exchange": "paper",
        "symbol": symbol,
        "market_type": market_type,
        "side": "buy",
        "order_type": "market",
        "quantity": "1",
        "reduce_only": false,
        "time_in_force": "gtc"
    }))
    .unwrap();
    TradingCommand::Submit(intent)
}

fn resting_command_for(client_order_id: &str) -> TradingCommand {
    let intent: OrderIntent = serde_json::from_value(serde_json::json!({
        "client_order_id": client_order_id,
        "exchange": "paper",
        "symbol": "BTC-USDT",
        "market_type": "perpetual",
        "side": "buy",
        "order_type": "limit",
        "quantity": "1",
        "price": "90",
        "reduce_only": false,
        "time_in_force": "gtc"
    }))
    .unwrap();
    TradingCommand::Submit(intent)
}

#[tokio::test]
async fn bounded_handle_rejects_excess_work_before_it_is_enqueued() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(BlockingExchange {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        execute_count: AtomicUsize::new(0),
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(1).unwrap()).unwrap();

    let first_handle = handle.clone();
    let first = tokio::spawn(async move { first_handle.execute(command()).await });
    entered.notified().await;

    let overloaded = handle.execute(command()).await.unwrap_err();
    assert!(matches!(
        overloaded,
        ExchangeError::Backpressure { capacity: 1 }
    ));

    release.notify_one();
    assert!(first.await.unwrap().is_err());
}

#[tokio::test]
async fn a_lost_execution_response_is_an_ambiguous_outcome() {
    let handle =
        BoundedExchangeHandle::spawn(Arc::new(PanickingExchange), NonZeroUsize::new(1).unwrap())
            .unwrap();

    let error = handle.execute(command()).await.unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::SubmitOrder,
            client_order_id: Some(_),
            ..
        }
    ));
}

#[tokio::test]
async fn cancelling_a_caller_quarantines_submits_after_the_adapter_finishes() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(BlockingExchange {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        execute_count: AtomicUsize::new(0),
    });
    let handle =
        BoundedExchangeHandle::spawn(Arc::clone(&adapter), NonZeroUsize::new(1).unwrap()).unwrap();

    let first_handle = handle.clone();
    let first = tokio::spawn(async move { first_handle.execute(command()).await });
    entered.notified().await;
    first.abort();

    let overloaded = handle.execute(command()).await.unwrap_err();
    assert!(matches!(
        overloaded,
        ExchangeError::Backpressure { capacity: 1 }
    ));
    release.notify_one();

    let barrier_result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match handle.status().await {
                Err(ExchangeError::Backpressure { .. }) => tokio::task::yield_now().await,
                result => break result,
            }
        }
    })
    .await
    .expect("actor should finish the abandoned adapter call");
    assert!(matches!(
        barrier_result,
        Err(ExchangeError::Rejected { .. })
    ));

    let quarantined = handle.execute(command()).await.unwrap_err();
    assert!(matches!(
        quarantined,
        ExchangeError::Rejected { ref reason } if reason.contains("quarantined")
    ));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);

    handle.reconcile(ReconcileScope::All).await.unwrap();
    assert!(handle.execute(command()).await.is_err());
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn paper_reconciliation_releases_quarantine_after_a_caller_abort() {
    let first_execute_entered = Arc::new(Notify::new());
    let release_first_execute = Arc::new(Notify::new());
    let adapter_time = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let adapter = Arc::new(DelayedPaperExchange {
        inner: PaperExchange::with_clock("paper", NonZeroUsize::new(8).unwrap(), move || {
            adapter_time
        })
        .unwrap(),
        first_execute_entered: Arc::clone(&first_execute_entered),
        release_first_execute: Arc::clone(&release_first_execute),
        execute_count: AtomicUsize::new(0),
    });
    let handle =
        BoundedExchangeHandle::spawn(Arc::clone(&adapter), NonZeroUsize::new(2).unwrap()).unwrap();
    let baseline = handle.reconcile(ReconcileScope::All).await.unwrap();
    assert_eq!(baseline.observed_at, adapter_time);

    let first = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(resting_command_for("4d36e96e-e325-11ce-bfc1-08002be10370"))
                .await
        }
    });
    first_execute_entered.notified().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let queued = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(resting_command_for("4d36e96e-e325-11ce-bfc1-08002be10371"))
                .await
        }
    });
    release_first_execute.notify_one();

    let quarantined = queued.await.unwrap().unwrap_err();
    assert!(matches!(
        quarantined,
        ExchangeError::Rejected { ref reason } if reason.contains("quarantined")
    ));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);

    let reconciliation = handle.reconcile(ReconcileScope::All).await.unwrap();
    assert_eq!(reconciliation.orders.len(), 1);
    assert!(
        handle
            .execute(resting_command_for("4d36e96e-e325-11ce-bfc1-08002be10372",))
            .await
            .is_ok()
    );
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_adapter_timeout_is_ambiguous_and_releases_actor_capacity() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(BlockingExchange {
        entered,
        release,
        execute_count: AtomicUsize::new(0),
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        adapter,
        NonZeroUsize::new(1).unwrap(),
        Duration::from_millis(20),
    )
    .unwrap();

    let error = handle.execute(command()).await.unwrap_err();
    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::SubmitOrder,
            client_order_id: Some(_),
            ..
        }
    ));

    let second_error = handle.execute(command()).await.unwrap_err();
    assert!(!matches!(second_error, ExchangeError::Backpressure { .. }));
}

#[tokio::test]
async fn ambiguous_submit_requires_all_state_reconciliation() {
    let first_submit_entered = Arc::new(Notify::new());
    let adapter = Arc::new(QuarantineExchange {
        execute_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        first_delay: Duration::from_millis(100),
        reconcile_count: AtomicUsize::new(0),
        reconcile_observed_at: StdMutex::new(VecDeque::new()),
        reconcile_scope_override: None,
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(2).unwrap(),
        Duration::from_millis(30),
    )
    .unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10362",
                    "BTC-USDT",
                    "perpetual",
                ))
                .await
        }
    });
    first_submit_entered.notified().await;
    let queued = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10363",
                    "ETH-USDT",
                    "perpetual",
                ))
                .await
        }
    });

    assert!(matches!(
        first.await.unwrap(),
        Err(ExchangeError::AmbiguousOutcome { .. })
    ));
    assert!(queued.await.unwrap().is_err());
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);

    handle
        .reconcile(ReconcileScope::Positions {
            symbol: Some(Symbol::new("ETH-USDT").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(adapter.reconcile_count.load(Ordering::SeqCst), 1);
    let unrelated_error = handle
        .execute(command_for(
            "4d36e96e-e325-11ce-bfc1-08002be10364",
            "SOL-USDT",
            "perpetual",
        ))
        .await
        .unwrap_err();
    assert!(matches!(unrelated_error, ExchangeError::Rejected { .. }));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);

    handle
        .reconcile(ReconcileScope::Orders {
            symbol: Some(Symbol::new("BTC-USDT").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(adapter.reconcile_count.load(Ordering::SeqCst), 2);
    let orders_only_error = handle
        .execute(command_for(
            "4d36e96e-e325-11ce-bfc1-08002be10365",
            "SOL-USDT",
            "perpetual",
        ))
        .await
        .unwrap_err();
    assert!(matches!(orders_only_error, ExchangeError::Rejected { .. }));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);

    handle.reconcile(ReconcileScope::All).await.unwrap();
    assert_eq!(adapter.reconcile_count.load(Ordering::SeqCst), 3);
    assert!(
        handle
            .execute(command_for(
                "4d36e96e-e325-11ce-bfc1-08002be10366",
                "SOL-USDT",
                "perpetual",
            ))
            .await
            .is_err()
    );
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stale_reconciliation_receipt_does_not_clear_quarantine() {
    let newer = chrono::Utc::now();
    let older = newer - chrono::TimeDelta::seconds(1);
    let adapter = Arc::new(QuarantineExchange {
        execute_count: AtomicUsize::new(0),
        first_submit_entered: Arc::new(Notify::new()),
        first_delay: Duration::from_millis(100),
        reconcile_count: AtomicUsize::new(0),
        reconcile_observed_at: StdMutex::new(VecDeque::from([newer, older])),
        reconcile_scope_override: None,
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(1).unwrap(),
        Duration::from_millis(30),
    )
    .unwrap();

    let baseline = handle.reconcile(ReconcileScope::All).await.unwrap();
    assert_eq!(baseline.observed_at, newer);
    assert!(matches!(
        handle.execute(command()).await,
        Err(ExchangeError::AmbiguousOutcome { .. })
    ));
    let reconcile_error = handle.reconcile(ReconcileScope::All).await.unwrap_err();
    assert!(matches!(
        reconcile_error,
        ExchangeError::InvalidResponse { ref reason, .. } if reason.contains("regressed")
    ));

    let error = handle
        .execute(command_for(
            "4d36e96e-e325-11ce-bfc1-08002be10365",
            "BTC-USDT",
            "perpetual",
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ExchangeError::Rejected { ref reason } if reason.contains("quarantined")
    ));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn mismatched_reconciliation_receipt_scope_does_not_clear_quarantine() {
    let adapter = Arc::new(QuarantineExchange {
        execute_count: AtomicUsize::new(0),
        first_submit_entered: Arc::new(Notify::new()),
        first_delay: Duration::from_millis(100),
        reconcile_count: AtomicUsize::new(0),
        reconcile_observed_at: StdMutex::new(VecDeque::new()),
        reconcile_scope_override: Some(ReconcileScope::Orders {
            symbol: Some(Symbol::new("BTC-USDT").unwrap()),
        }),
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(1).unwrap(),
        Duration::from_millis(30),
    )
    .unwrap();

    assert!(matches!(
        handle.execute(command()).await,
        Err(ExchangeError::AmbiguousOutcome { .. })
    ));
    let reconcile_error = handle.reconcile(ReconcileScope::All).await.unwrap_err();
    assert!(matches!(
        reconcile_error,
        ExchangeError::InvalidResponse { ref reason, .. } if reason.contains("returned scope")
    ));

    let error = handle
        .execute(command_for(
            "4d36e96e-e325-11ce-bfc1-08002be10366",
            "BTC-USDT",
            "perpetual",
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ExchangeError::Rejected { ref reason } if reason.contains("quarantined")
    ));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reconciliation_has_reserved_admission_when_normal_capacity_is_full() {
    let first_submit_entered = Arc::new(Notify::new());
    let adapter = Arc::new(QuarantineExchange {
        execute_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        first_delay: Duration::from_millis(30),
        reconcile_count: AtomicUsize::new(0),
        reconcile_observed_at: StdMutex::new(VecDeque::new()),
        reconcile_scope_override: None,
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(1),
    )
    .unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move { handle.execute(command()).await }
    });
    first_submit_entered.notified().await;
    let reconciliation = handle.reconcile(ReconcileScope::All).await;

    assert!(reconciliation.is_ok());
    assert!(first.await.unwrap().is_err());
    assert_eq!(adapter.reconcile_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn operation_deadline_includes_time_spent_waiting_in_the_actor_queue() {
    let entered = Arc::new(Notify::new());
    let adapter = Arc::new(SlowExchange {
        entered: Arc::clone(&entered),
        delay: Duration::from_millis(120),
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        adapter,
        NonZeroUsize::new(2).unwrap(),
        Duration::from_millis(150),
    )
    .unwrap();

    let first_handle = handle.clone();
    let first = tokio::spawn(async move { first_handle.execute(command()).await });
    entered.notified().await;

    let started = Instant::now();
    let error = handle.execute(command()).await.unwrap_err();
    let elapsed = started.elapsed();

    assert!(matches!(error, ExchangeError::AmbiguousOutcome { .. }));
    assert!(
        elapsed < Duration::from_millis(210),
        "queue wait was excluded from the deadline: {elapsed:?}"
    );
    assert!(first.await.unwrap().is_err());
}

#[tokio::test]
async fn a_request_expired_in_the_queue_is_never_polled_by_the_adapter() {
    let first_submit_entered = Arc::new(Notify::new());
    let release_first_submit = Arc::new(Notify::new());
    let cancel_entered = Arc::new(Notify::new());
    let release_cancel = Arc::new(Notify::new());
    let adapter = Arc::new(DeadlineDispatchExchange {
        submit_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        release_first_submit: Arc::clone(&release_first_submit),
        cancel_entered: Arc::clone(&cancel_entered),
        release_cancel: Arc::clone(&release_cancel),
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(2).unwrap(),
        Duration::from_millis(300),
    )
    .unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10360",
                    "BTC-USDT",
                    "perpetual",
                ))
                .await
        }
    });
    first_submit_entered.notified().await;
    let expired = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10361",
                    "ETH-USDT",
                    "perpetual",
                ))
                .await
        }
    });

    sleep(Duration::from_millis(100)).await;
    let cancel = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(TradingCommand::Cancel {
                    order_id: "paper-0000000000000060".to_owned(),
                })
                .await
        }
    });
    release_first_submit.notify_one();
    cancel_entered.notified().await;

    let expired_error = expired.await.unwrap().unwrap_err();
    assert!(matches!(
        expired_error,
        ExchangeError::AmbiguousOutcome { .. }
    ));
    release_cancel.notify_one();
    assert!(first.await.unwrap().is_err());
    assert!(cancel.await.unwrap().is_err());

    let _status_barrier = handle.status().await;
    assert_eq!(adapter.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn queued_submit_past_dispatch_deadline_is_not_polled_or_quarantined() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(BlockingExchange {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        execute_count: AtomicUsize::new(0),
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(2).unwrap(),
        Duration::from_millis(500),
    )
    .unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10367",
                    "BTC-USDT",
                    "perpetual",
                ))
                .await
        }
    });
    entered.notified().await;

    let dispatch_before = Instant::now()
        .checked_add(Duration::from_millis(80))
        .unwrap();
    let stale = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute_before(
                    command_for(
                        "4d36e96e-e325-11ce-bfc1-08002be10368",
                        "ETH-USDT",
                        "perpetual",
                    ),
                    dispatch_before,
                )
                .await
        }
    });
    tokio::time::sleep_until(dispatch_before + Duration::from_millis(20)).await;
    release.notify_one();

    assert!(first.await.unwrap().is_err());
    let stale_error = stale.await.unwrap().unwrap_err();
    assert!(matches!(
        stale_error,
        ExchangeError::Rejected { ref reason }
            if reason.contains("expired before adapter dispatch")
    ));
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 1);

    assert!(
        handle
            .execute(command_for(
                "4d36e96e-e325-11ce-bfc1-08002be10369",
                "SOL-USDT",
                "perpetual",
            ))
            .await
            .is_err()
    );
    assert_eq!(adapter.execute_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn timely_dispatch_can_finish_after_freshness_deadline_before_operation_timeout() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(FreshnessExchange {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        execute_before_count: AtomicUsize::new(0),
    });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        Arc::clone(&adapter),
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(1),
    )
    .unwrap();
    let dispatch_before = Instant::now()
        .checked_add(Duration::from_millis(150))
        .unwrap();

    let execution = tokio::spawn({
        let handle = handle.clone();
        async move { handle.execute_before(command(), dispatch_before).await }
    });
    entered.notified().await;
    tokio::time::sleep_until(dispatch_before + Duration::from_millis(20)).await;
    release.notify_one();

    assert!(matches!(
        execution.await.unwrap(),
        Ok(TradingReceipt::Submitted { .. })
    ));
    assert_eq!(adapter.execute_before_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_has_reserved_admission_and_runs_before_queued_normal_work() {
    let first_submit_entered = Arc::new(Notify::new());
    let release_first_submit = Arc::new(Notify::new());
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let adapter = Arc::new(PriorityExchange {
        submit_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        release_first_submit: Arc::clone(&release_first_submit),
        calls: Arc::clone(&calls),
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(2).unwrap()).unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move { handle.execute(command()).await }
    });
    first_submit_entered.notified().await;
    let second = tokio::spawn({
        let handle = handle.clone();
        async move { handle.execute(command()).await }
    });
    tokio::task::yield_now().await;
    assert!(matches!(
        handle.execute(command()).await,
        Err(ExchangeError::Backpressure { capacity: 2 })
    ));

    let cancel = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(TradingCommand::Cancel {
                    order_id: "paper-0000000000000001".to_owned(),
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    release_first_submit.notify_one();

    let cancel_error = cancel.await.unwrap().unwrap_err();
    assert!(!matches!(cancel_error, ExchangeError::Backpressure { .. }));
    assert!(first.await.unwrap().is_err());
    assert!(second.await.unwrap().is_err());
    assert_eq!(
        *calls.lock().expect("test call log must not be poisoned"),
        ["submit-1", "cancel", "submit-2"]
    );
}

#[tokio::test]
async fn cancel_all_has_reserved_admission_when_cancel_one_is_already_queued() {
    let first_submit_entered = Arc::new(Notify::new());
    let release_first_submit = Arc::new(Notify::new());
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let adapter = Arc::new(PriorityExchange {
        submit_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        release_first_submit: Arc::clone(&release_first_submit),
        calls: Arc::clone(&calls),
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(1).unwrap()).unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move { handle.execute(command()).await }
    });
    first_submit_entered.notified().await;
    let cancel_one = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(TradingCommand::Cancel {
                    order_id: "paper-0000000000000061".to_owned(),
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    let cancel_all = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(TradingCommand::CancelAll {
                    symbol: None,
                    market_type: None,
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    release_first_submit.notify_one();

    assert!(first.await.unwrap().is_err());
    assert!(!matches!(
        cancel_all.await.unwrap(),
        Err(ExchangeError::Backpressure { .. })
    ));
    assert!(!matches!(
        cancel_one.await.unwrap(),
        Err(ExchangeError::Backpressure { .. })
    ));
    assert_eq!(
        *calls.lock().expect("test call log must not be poisoned"),
        ["submit-1", "cancel-all", "cancel"]
    );
}

#[tokio::test]
async fn cancel_all_is_a_barrier_for_matching_submits_already_waiting_for_dispatch() {
    let first_submit_entered = Arc::new(Notify::new());
    let release_first_submit = Arc::new(Notify::new());
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let adapter = Arc::new(PriorityExchange {
        submit_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        release_first_submit: Arc::clone(&release_first_submit),
        calls: Arc::clone(&calls),
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(2).unwrap()).unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move { handle.execute(command()).await }
    });
    first_submit_entered.notified().await;
    let queued = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10350",
                    "BTC-USDT",
                    "perpetual",
                ))
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(matches!(
        handle.execute(command()).await,
        Err(ExchangeError::Backpressure { capacity: 2 })
    ));

    let cancel_all = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(TradingCommand::CancelAll {
                    symbol: None,
                    market_type: None,
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    release_first_submit.notify_one();

    assert!(first.await.unwrap().is_err());
    assert!(cancel_all.await.unwrap().is_err());
    let queued_error = queued.await.unwrap().unwrap_err();
    assert!(matches!(
        queued_error,
        ExchangeError::Rejected { ref reason }
            if reason.contains("cancel_all") && reason.contains("before adapter dispatch")
    ));

    let post_barrier = handle
        .execute(command_for(
            "4d36e96e-e325-11ce-bfc1-08002be10351",
            "BTC-USDT",
            "perpetual",
        ))
        .await;
    assert!(post_barrier.is_err());
    assert_eq!(
        *calls.lock().expect("test call log must not be poisoned"),
        ["submit-1", "cancel-all", "submit-2"]
    );
}

#[tokio::test]
async fn scoped_cancel_all_only_blocks_matching_queued_submits() {
    let first_submit_entered = Arc::new(Notify::new());
    let release_first_submit = Arc::new(Notify::new());
    let calls = Arc::new(StdMutex::new(Vec::new()));
    let adapter = Arc::new(PriorityExchange {
        submit_count: AtomicUsize::new(0),
        first_submit_entered: Arc::clone(&first_submit_entered),
        release_first_submit: Arc::clone(&release_first_submit),
        calls: Arc::clone(&calls),
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(3).unwrap()).unwrap();

    let first = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10352",
                    "SOL-USDT",
                    "perpetual",
                ))
                .await
        }
    });
    first_submit_entered.notified().await;
    let matching = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10353",
                    "BTC-USDT",
                    "perpetual",
                ))
                .await
        }
    });
    let non_matching = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(command_for(
                    "4d36e96e-e325-11ce-bfc1-08002be10354",
                    "ETH-USDT",
                    "spot",
                ))
                .await
        }
    });
    tokio::task::yield_now().await;

    let cancel_all = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .execute(TradingCommand::CancelAll {
                    symbol: Some(Symbol::new("BTC-USDT").unwrap()),
                    market_type: Some(MarketType::Perpetual),
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    release_first_submit.notify_one();

    assert!(first.await.unwrap().is_err());
    assert!(cancel_all.await.unwrap().is_err());
    assert!(matches!(
        matching.await.unwrap(),
        Err(ExchangeError::Rejected { ref reason }) if reason.contains("cancel_all")
    ));
    assert!(non_matching.await.unwrap().is_err());
    assert_eq!(
        *calls.lock().expect("test call log must not be poisoned"),
        ["submit-1", "cancel-all", "submit-2"]
    );
}

#[tokio::test]
async fn ambiguous_cancellation_preserves_its_order_id_operation_key() {
    let handle =
        BoundedExchangeHandle::spawn(Arc::new(PanickingExchange), NonZeroUsize::new(1).unwrap())
            .unwrap();
    let order_id = "paper-0000000000000042".to_owned();

    let error = handle
        .execute(TradingCommand::Cancel {
            order_id: order_id.clone(),
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::CancelOrder,
            operation_key: Some(ExchangeOperationKey::OrderId(candidate)),
            ..
        } if candidate == order_id
    ));
}

#[tokio::test]
async fn ambiguous_cancel_all_preserves_its_scope_operation_key() {
    let handle =
        BoundedExchangeHandle::spawn(Arc::new(PanickingExchange), NonZeroUsize::new(1).unwrap())
            .unwrap();
    let symbol = Symbol::new("BTC-USDT").unwrap();

    let error = handle
        .execute(TradingCommand::CancelAll {
            symbol: Some(symbol.clone()),
            market_type: Some(MarketType::Perpetual),
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::CancelAll,
            operation_key: Some(ExchangeOperationKey::CancelAll {
                symbol: Some(candidate),
                market_type: Some(MarketType::Perpetual),
            }),
            ..
        } if candidate == symbol
    ));
}

#[tokio::test]
async fn oversized_command_capacity_is_rejected_before_actor_allocation() {
    let result = BoundedExchangeHandle::spawn(
        Arc::new(PanickingExchange),
        NonZeroUsize::new(usize::MAX).unwrap(),
    );

    assert!(matches!(
        result,
        Err(ExchangeError::ResourceLimit {
            resource: "exchange command capacity",
            ..
        })
    ));
}
