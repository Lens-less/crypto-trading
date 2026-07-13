use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeOperation,
    ExchangeStatus, MarketSubscription, ReconcileReceipt, ReconcileScope, SubscriptionReceipt,
    TradingCommand, TradingReceipt,
};

/// Explicit placeholder for live venues until authenticated protocols are verified.
#[derive(Debug, Clone)]
pub struct UnsupportedLiveExchange {
    exchange: Arc<str>,
}

impl UnsupportedLiveExchange {
    /// Creates an explicit unsupported live-trading placeholder.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when the exchange name is empty.
    pub fn new(exchange: impl Into<String>) -> Result<Self, ExchangeError> {
        let exchange = exchange.into();
        let exchange = exchange.trim();
        if exchange.is_empty() {
            return Err(ExchangeError::invalid("exchange name must not be empty"));
        }
        Ok(Self {
            exchange: Arc::from(exchange),
        })
    }

    fn unsupported(&self, operation: ExchangeOperation) -> ExchangeError {
        ExchangeError::Unsupported {
            exchange: self.exchange.to_string(),
            operation,
        }
    }
}

#[async_trait]
impl ExchangeHandle for UnsupportedLiveExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        Err(self.unsupported(command.operation()))
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        Err(self.unsupported(ExchangeOperation::Reconcile))
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        Err(self.unsupported(ExchangeOperation::Subscribe))
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Ok(ExchangeStatus {
            exchange: self.exchange.to_string(),
            mode: ExchangeMode::Live,
            availability: ExchangeAvailability::Unavailable,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}
