use std::{num::NonZeroUsize, str::FromStr, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeStatus,
    MarketSubscription, PaperExchange, ReconcileReceipt, ReconcileScope, SubscriptionReceipt,
    TradingCommand, TradingReceipt,
};
use crypto_trading_runtime::{ExecutionMode, IntentExecutor, LIVE_ACKNOWLEDGEMENT, RuntimeError};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

struct StatusOnlyExchange {
    mode: ExchangeMode,
    availability: ExchangeAvailability,
}

#[async_trait]
impl ExchangeHandle for StatusOnlyExchange {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        panic!("runtime must reject this adapter before execution")
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
            exchange: "status-only".to_owned(),
            mode: self.mode,
            availability: self.availability,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}

#[tokio::test]
async fn paper_executor_submits_intents_through_the_exchange_seam() {
    let paper = Arc::new(PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap());
    let symbol = Symbol::new("BTC-USDT").unwrap();
    paper
        .publish_snapshot(
            MarketSnapshot::new(
                "paper",
                symbol.clone(),
                MarketType::Perpetual,
                Price::new(decimal("100")).unwrap(),
                Price::new(decimal("101")).unwrap(),
                Utc::now(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let executor = IntentExecutor::new(Arc::clone(&paper), ExecutionMode::Paper);
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
async fn monitor_mode_cannot_cross_the_order_seam() {
    let paper = Arc::new(PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap());
    let executor = IntentExecutor::new(paper, ExecutionMode::Monitor);
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
    paper
        .publish_snapshot(
            MarketSnapshot::new(
                "paper",
                symbol.clone(),
                MarketType::Perpetual,
                Price::new(decimal("100")).unwrap(),
                Price::new(decimal("101")).unwrap(),
                Utc::now(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let first = OrderIntent::market(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );
    let failed = OrderIntent::market(
        "wrong-exchange",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.01")).unwrap(),
    );

    let error = IntentExecutor::new(paper, ExecutionMode::Paper)
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
    let exchange = Arc::new(StatusOnlyExchange {
        mode: ExchangeMode::Paper,
        availability: ExchangeAvailability::Unavailable,
    });
    let intent = OrderIntent::market(
        "status-only",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );

    let error = IntentExecutor::new(exchange, ExecutionMode::Paper)
        .execute_all(vec![intent])
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::AdapterUnavailable { .. }));
}

#[tokio::test]
async fn live_execution_remains_closed_even_for_a_ready_live_adapter() {
    let exchange = Arc::new(StatusOnlyExchange {
        mode: ExchangeMode::Live,
        availability: ExchangeAvailability::Ready,
    });
    let intent = OrderIntent::market(
        "status-only",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    );
    let mode = ExecutionMode::live(Some(LIVE_ACKNOWLEDGEMENT)).unwrap();

    let error = IntentExecutor::new(exchange, mode)
        .execute_all(vec![intent])
        .await
        .unwrap_err();
    assert!(matches!(error, RuntimeError::LiveExecutionUnavailable));
}
