//! Typed exchange boundary with deterministic paper execution.

mod binance;
mod binance_testnet;
mod binance_testnet_exchange;
mod bounded;
mod endpoint;
mod error;
mod hyperliquid_public;
mod hyperliquid_testnet;
mod instrument;
mod model;
mod paper;
mod remote;
mod sha256;
mod symbol;
mod unsupported;

pub use binance::BinancePublicExchange;
pub use binance_testnet::{
    BinanceHmacSha256Signer, BinanceRequestSigner, BinanceServerOrderRef, BinanceTestnetBalance,
    BinanceTestnetProtocol,
};
pub use binance_testnet_exchange::{BinanceTestnetAccountSnapshot, BinanceTestnetExchange};
pub use bounded::BoundedExchangeHandle;
pub use endpoint::{
    BinanceProduct, BinanceTestnetEndpoints, HyperliquidPublicEndpoint, HyperliquidTestnetEndpoint,
};
pub use error::{ExchangeError, ExchangeOperation, RemoteFailureMetadata, RemoteRetryAfter};
pub use hyperliquid_public::{
    HyperliquidFundingRate, HyperliquidPublicExchange, HyperliquidPublicObservation,
    MAX_HYPERLIQUID_PUBLIC_CATALOG_COINS, hyperliquid_usdt_symbol_catalog,
};
pub use hyperliquid_testnet::{
    HyperliquidAction, HyperliquidAsset, HyperliquidAssetCatalog, HyperliquidRequestSigner,
    HyperliquidSignature, HyperliquidTestnetProtocol,
};
pub use instrument::{
    InstrumentRuleCatalog, InstrumentRules, InstrumentRulesMode, InstrumentRulesStatus,
};
pub use model::{
    CancellationDisposition, ExchangeAvailability, ExchangeMode, ExchangeOperationKey,
    ExchangeStatus, ForeignOrder, MarketSubscription, ReconcileReceipt, ReconcileScope,
    SubmissionDisposition, SubscriptionReceipt, TradingCommand, TradingReceipt,
};
pub use paper::{PaperExchange, PaperLedgerLimits};
pub use remote::{
    RemoteHttpMethod, RemoteHttpRequest, RemoteHttpResponse, RemoteHttpTransport,
    ReqwestHttpTransport,
};
pub use symbol::{ExchangeSymbol, ExchangeSymbolCatalog};
pub use unsupported::UnsupportedLiveExchange;

use async_trait::async_trait;
use tokio::time::Instant;

/// Object-safe boundary consumed by runtimes and strategies.
#[async_trait]
pub trait ExchangeHandle: Send + Sync {
    /// Executes a typed trading command.
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError>;

    /// Executes a command only if the adapter can begin dispatch before the
    /// caller's absolute monotonic deadline.
    ///
    /// Wrappers that queue work must override this method and carry the same
    /// deadline through to the point immediately before they poll the wrapped
    /// adapter. The default is appropriate for direct adapters without an
    /// internal queue.
    async fn execute_before(
        &self,
        command: TradingCommand,
        deadline: Instant,
    ) -> Result<TradingReceipt, ExchangeError> {
        if Instant::now() >= deadline {
            return Err(ExchangeError::rejected(
                "trading command expired before adapter dispatch",
            ));
        }
        self.execute(command).await
    }

    /// Returns an authoritative point-in-time view sampled during this call for
    /// the requested scope.
    ///
    /// `observed_at` is an adapter-relative logical watermark. Successful calls
    /// from the same adapter must never move it backwards, and implementations
    /// must not reuse a cached view that predates the current reconciliation
    /// request. Wrappers may compare this watermark with earlier receipts from
    /// the same adapter, but must not compare it with an independent wall clock.
    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError>;

    /// Creates a bounded market-data subscription.
    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError>;

    /// Reports adapter readiness without performing trading I/O.
    async fn status(&self) -> Result<ExchangeStatus, ExchangeError>;
}
