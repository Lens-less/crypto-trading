//! Authority-typed Binance Spot MAINNET adapters.
//!
//! The wire protocol is identical to the Binance Spot testnet protocol, so
//! both adapters wrap the shared [`BinanceTestnetExchange`] machinery (signed
//! requests, one-shot clock-skew recovery, rate-limit weight budget, and
//! Retry-After handling). Authority is separated at the type level:
//!
//! * [`BinanceMainnetSpotReadExchange`] is constructed only from
//!   [`BinanceMainnetReadEndpoints`] and exposes no submit, cancel, or any
//!   other mutating method. It does not implement [`ExchangeHandle`].
//! * [`BinanceMainnetSpotExchange`] is constructed only from
//!   [`BinanceMainnetTradeEndpoints`] and carries the one-shot Spot LIMIT
//!   lifecycle authority (submit, query, cancel) plus signed reads.
//!
//! Both are Spot-only: any USDⓈ-M product route fails closed inside the
//! endpoint authority before a request is built.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Order, OrderIntent, Symbol};
use uuid::Uuid;

use crate::{
    BinanceMainnetReadEndpoints, BinanceMainnetTradeEndpoints, BinanceProduct,
    BinanceRequestSigner, BinanceTestnetAccountSnapshot, BinanceTestnetExchange,
    BinanceTestnetProtocol, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeStatus,
    ExchangeSymbolCatalog, InstrumentRuleCatalog, MarketSubscription, ReconcileReceipt,
    ReconcileScope, RemoteHttpRequest, RemoteHttpTransport, SubscriptionReceipt, TradingCommand,
    TradingReceipt, endpoint::BinanceRestEndpointAuthority,
};

/// One Spot-scoped Binance MAINNET account truth snapshot.
///
/// Shape-identical to the testnet snapshot because both come from the same
/// wire protocol; the alias keeps mainnet call sites honest about what they
/// hold.
pub type BinanceMainnetSpotAccountSnapshot = BinanceTestnetAccountSnapshot;

/// Read-only Binance Spot MAINNET adapter.
///
/// Holds no mutation authority by construction: there is no submit, cancel,
/// or cancel-all surface on this type, and it does not implement
/// [`ExchangeHandle`], so it can never be handed to execution code.
pub struct BinanceMainnetSpotReadExchange {
    inner: BinanceTestnetExchange,
}

impl std::fmt::Debug for BinanceMainnetSpotReadExchange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BinanceMainnetSpotReadExchange")
            .finish_non_exhaustive()
    }
}

impl BinanceMainnetSpotReadExchange {
    /// Builds a mainnet read adapter from authority-typed read endpoints.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a malformed or blank API
    /// key. Secrets are retained only by the signer implementation.
    pub fn new<S>(
        endpoints: BinanceMainnetReadEndpoints,
        symbols: ExchangeSymbolCatalog,
        rules: InstrumentRuleCatalog,
        signer: Arc<S>,
        transport: Arc<dyn RemoteHttpTransport>,
    ) -> Result<Self, ExchangeError>
    where
        S: BinanceRequestSigner + 'static,
    {
        Self::with_clock(endpoints, symbols, rules, signer, transport, Utc::now)
    }

    /// Builds a mainnet read adapter with an injected clock for tests.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a malformed or blank API
    /// key.
    pub fn with_clock<S, F>(
        endpoints: BinanceMainnetReadEndpoints,
        symbols: ExchangeSymbolCatalog,
        rules: InstrumentRuleCatalog,
        signer: Arc<S>,
        transport: Arc<dyn RemoteHttpTransport>,
        clock: F,
    ) -> Result<Self, ExchangeError>
    where
        S: BinanceRequestSigner + 'static,
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        let protocol = BinanceTestnetProtocol::authenticated_with_authority(
            BinanceRestEndpointAuthority::MainnetRead(endpoints),
            symbols,
            rules,
            signer,
        )?;
        Ok(Self {
            inner: BinanceTestnetExchange::with_clock(protocol, transport, clock),
        })
    }

    /// Builds an unsigned exact-symbol Spot `exchangeInfo` request.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for an invalid wire symbol.
    pub fn build_exchange_info_request(
        endpoints: &BinanceMainnetReadEndpoints,
        wire_symbol: &str,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        BinanceTestnetProtocol::build_exchange_info_request_with_authority(
            &BinanceRestEndpointAuthority::MainnetRead(endpoints.clone()),
            BinanceProduct::Spot,
            wire_symbol,
        )
    }

    /// Queries Spot balance and open-order truth with two complete
    /// consecutive samples; observed drift fails closed.
    ///
    /// # Errors
    ///
    /// Returns a bounded exchange error if any component cannot be sampled or
    /// parsed. Partial snapshots are never returned.
    pub async fn account_snapshot(
        &self,
    ) -> Result<BinanceMainnetSpotAccountSnapshot, ExchangeError> {
        self.inner.account_snapshot(BinanceProduct::Spot).await
    }

    /// Reports adapter readiness without performing trading I/O.
    ///
    /// # Errors
    ///
    /// Propagates status errors from the shared adapter machinery.
    pub async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        let status = self.inner.status().await?;
        Ok(ExchangeStatus {
            mode: ExchangeMode::ReadOnly,
            ..status
        })
    }
}

/// One-shot-lifecycle Binance Spot MAINNET trading adapter.
///
/// Constructed only from [`BinanceMainnetTradeEndpoints`]; supports Spot
/// LIMIT submit, exact-identity query, single-order cancel, signed account
/// snapshots, and symbol-scoped open-order reconciliation. Cancel-all and
/// every USDⓈ-M surface fail closed.
pub struct BinanceMainnetSpotExchange {
    inner: BinanceTestnetExchange,
}

impl std::fmt::Debug for BinanceMainnetSpotExchange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BinanceMainnetSpotExchange")
            .finish_non_exhaustive()
    }
}

impl BinanceMainnetSpotExchange {
    /// Builds a mainnet trade adapter from authority-typed trade endpoints.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a malformed or blank API
    /// key. Secrets are retained only by the signer implementation.
    pub fn new<S>(
        endpoints: BinanceMainnetTradeEndpoints,
        symbols: ExchangeSymbolCatalog,
        rules: InstrumentRuleCatalog,
        signer: Arc<S>,
        transport: Arc<dyn RemoteHttpTransport>,
    ) -> Result<Self, ExchangeError>
    where
        S: BinanceRequestSigner + 'static,
    {
        Self::with_clock(endpoints, symbols, rules, signer, transport, Utc::now)
    }

    /// Builds a mainnet trade adapter with an injected clock for tests.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a malformed or blank API
    /// key.
    pub fn with_clock<S, F>(
        endpoints: BinanceMainnetTradeEndpoints,
        symbols: ExchangeSymbolCatalog,
        rules: InstrumentRuleCatalog,
        signer: Arc<S>,
        transport: Arc<dyn RemoteHttpTransport>,
        clock: F,
    ) -> Result<Self, ExchangeError>
    where
        S: BinanceRequestSigner + 'static,
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        let protocol = BinanceTestnetProtocol::authenticated_with_authority(
            BinanceRestEndpointAuthority::MainnetTrade(endpoints),
            symbols,
            rules,
            signer,
        )?;
        Ok(Self {
            inner: BinanceTestnetExchange::with_clock(protocol, transport, clock),
        })
    }

    /// Builds an unsigned exact-symbol Spot `exchangeInfo` request.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for an invalid wire symbol.
    pub fn build_exchange_info_request(
        endpoints: &BinanceMainnetTradeEndpoints,
        wire_symbol: &str,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        BinanceTestnetProtocol::build_exchange_info_request_with_authority(
            &BinanceRestEndpointAuthority::MainnetTrade(endpoints.clone()),
            BinanceProduct::Spot,
            wire_symbol,
        )
    }

    /// Queries one Spot order by the UUID client identity persisted before
    /// dispatch. This is the query-first recovery seam: it never expands to
    /// all open orders and never resubmits.
    ///
    /// # Errors
    ///
    /// Returns a bounded exchange error when the request fails, the response
    /// is malformed, or Binance returns a different client identity.
    pub async fn query_order(
        &self,
        symbol: &Symbol,
        client_order_id: Uuid,
    ) -> Result<Order, ExchangeError> {
        self.inner
            .query_order(symbol, MarketType::Spot, client_order_id)
            .await
    }

    /// Queries Spot balance and open-order truth with two complete
    /// consecutive samples; observed drift fails closed.
    ///
    /// # Errors
    ///
    /// Returns a bounded exchange error if any component cannot be sampled or
    /// parsed. Partial snapshots are never returned.
    pub async fn account_snapshot(
        &self,
    ) -> Result<BinanceMainnetSpotAccountSnapshot, ExchangeError> {
        self.inner.account_snapshot(BinanceProduct::Spot).await
    }

    fn ensure_spot_intent(intent: &OrderIntent) -> Result<(), ExchangeError> {
        if intent.market_type != MarketType::Spot {
            return Err(ExchangeError::invalid(
                "Binance mainnet trade authority is Spot-only; perpetual intents are unavailable",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeHandle for BinanceMainnetSpotExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        match &command {
            TradingCommand::Submit(intent) => Self::ensure_spot_intent(intent)?,
            TradingCommand::Cancel { .. } => {}
            TradingCommand::CancelAll { .. } => {
                return Err(ExchangeError::invalid(
                    "Binance mainnet cancel-all is not part of the acknowledged one-shot lifecycle authority",
                ));
            }
        }
        self.inner.execute(command).await
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        match &scope {
            ReconcileScope::Orders { symbol: Some(_) } => self.inner.reconcile(scope).await,
            ReconcileScope::Orders { symbol: None }
            | ReconcileScope::All
            | ReconcileScope::Positions { .. } => Err(ExchangeError::invalid(
                "Binance mainnet reconciliation is limited to explicit Spot symbol open orders",
            )),
        }
    }

    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        self.inner.subscribe(subscription).await
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        let status = self.inner.status().await?;
        Ok(ExchangeStatus {
            mode: ExchangeMode::Live,
            ..status
        })
    }
}
