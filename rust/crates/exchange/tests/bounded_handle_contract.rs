use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use crypto_trading_domain::OrderIntent;
use crypto_trading_exchange::{
    BoundedExchangeHandle, ExchangeError, ExchangeHandle, ExchangeOperation, ExchangeStatus,
    MarketSubscription, ReconcileReceipt, ReconcileScope, SubscriptionReceipt, TradingCommand,
    TradingReceipt,
};
use tokio::sync::Notify;

struct BlockingExchange {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct PanickingExchange;

#[async_trait]
impl ExchangeHandle for BlockingExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        self.entered.notify_one();
        self.release.notified().await;
        Err(ExchangeError::rejected("released test request"))
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

fn command() -> TradingCommand {
    let intent: OrderIntent = serde_json::from_value(serde_json::json!({
        "client_order_id": "4d36e96e-e325-11ce-bfc1-08002be10318",
        "exchange": "paper",
        "symbol": "BTC-USDT",
        "market_type": "perpetual",
        "side": "buy",
        "order_type": "market",
        "quantity": "1",
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
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(1).unwrap());

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
        BoundedExchangeHandle::spawn(Arc::new(PanickingExchange), NonZeroUsize::new(1).unwrap());

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
async fn cancelling_a_caller_does_not_release_capacity_while_the_adapter_is_running() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(BlockingExchange {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let handle = BoundedExchangeHandle::spawn(adapter, NonZeroUsize::new(1).unwrap());

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
}

#[tokio::test]
async fn an_adapter_timeout_is_ambiguous_and_releases_actor_capacity() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let adapter = Arc::new(BlockingExchange { entered, release });
    let handle = BoundedExchangeHandle::spawn_with_timeout(
        adapter,
        NonZeroUsize::new(1).unwrap(),
        Duration::from_millis(20),
    );

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
