//! Typed exchange boundary with deterministic paper execution.

mod binance;
mod bounded;
mod error;
mod model;
mod paper;
mod unsupported;

pub use binance::BinancePublicExchange;
pub use bounded::BoundedExchangeHandle;
pub use error::{ExchangeError, ExchangeOperation};
pub use model::{
    CancellationDisposition, ExchangeAvailability, ExchangeMode, ExchangeStatus,
    MarketSubscription, ReconcileReceipt, ReconcileScope, SubmissionDisposition,
    SubscriptionReceipt, TradingCommand, TradingReceipt,
};
pub use paper::PaperExchange;
pub use unsupported::UnsupportedLiveExchange;

use async_trait::async_trait;

/// Object-safe boundary consumed by runtimes and strategies.
#[async_trait]
pub trait ExchangeHandle: Send + Sync {
    /// Executes a typed trading command.
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError>;

    /// Returns an authoritative point-in-time view for the requested scope.
    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError>;

    /// Creates a bounded market-data subscription.
    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError>;

    /// Reports adapter readiness without performing trading I/O.
    async fn status(&self) -> Result<ExchangeStatus, ExchangeError>;
}
