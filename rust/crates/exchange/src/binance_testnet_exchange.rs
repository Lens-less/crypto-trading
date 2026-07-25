use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};

use crate::{
    BinanceProduct, BinanceServerOrderRef, BinanceTestnetProtocol, CancellationDisposition,
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeStatus,
    MarketSubscription, ReconcileReceipt, ReconcileScope, RemoteHttpResponse, RemoteHttpTransport,
    SubscriptionReceipt, TradingCommand, TradingReceipt,
};

const EXCHANGE: &str = "binance";

type Clock = dyn Fn() -> DateTime<Utc> + Send + Sync;

/// Executable Binance Spot/USD-M testnet adapter backed by the authenticated
/// protocol and a caller-supplied transport.
pub struct BinanceTestnetExchange {
    protocol: BinanceTestnetProtocol,
    transport: Arc<dyn RemoteHttpTransport>,
    clock: Arc<Clock>,
    time_offset_ms: Mutex<i64>,
    observed_at: Mutex<DateTime<Utc>>,
}

impl std::fmt::Debug for BinanceTestnetExchange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BinanceTestnetExchange")
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

impl BinanceTestnetExchange {
    /// Builds a testnet adapter with the wall clock as its local timestamp
    /// source.
    pub fn new(protocol: BinanceTestnetProtocol, transport: Arc<dyn RemoteHttpTransport>) -> Self {
        Self::with_clock(protocol, transport, Utc::now)
    }

    /// Builds a testnet adapter with an injected clock for deterministic tests.
    pub fn with_clock<F>(
        protocol: BinanceTestnetProtocol,
        transport: Arc<dyn RemoteHttpTransport>,
        clock: F,
    ) -> Self
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        let now = clock();
        Self {
            protocol,
            transport,
            clock: Arc::new(clock),
            time_offset_ms: Mutex::new(0),
            observed_at: Mutex::new(now),
        }
    }

    async fn execute_submit(
        &self,
        intent: crypto_trading_domain::OrderIntent,
    ) -> Result<TradingReceipt, ExchangeError> {
        let product = product_for_market(intent.market_type);
        let response = self
            .dispatch_with_clock_retry(product, |timestamp_ms| {
                let intent = intent.clone();
                async move {
                    self.protocol
                        .dispatch_order(&*self.transport, &intent, intent.price, timestamp_ms)
                        .await
                }
            })
            .await?;
        let observed_at = self.observe_response(&response);
        self.protocol
            .parse_order_response(product, response.body(), observed_at)
    }

    async fn execute_cancel(
        &self,
        order_ref: BinanceServerOrderRef,
    ) -> Result<TradingReceipt, ExchangeError> {
        let product = product_for_market(order_ref.market_type);
        let symbol = order_ref.symbol.clone();
        let market_type = order_ref.market_type;
        let order_id = order_ref.order_id;
        let response = self
            .dispatch_with_clock_retry(product, |timestamp_ms| {
                let symbol = symbol.clone();
                async move {
                    self.protocol
                        .dispatch_cancel(
                            &*self.transport,
                            &symbol,
                            market_type,
                            order_id,
                            timestamp_ms,
                        )
                        .await
                }
            })
            .await?;
        let observed_at = self.observe_response(&response);
        let receipt = self
            .protocol
            .parse_order_response(product, response.body(), observed_at)?;
        let TradingReceipt::Submitted { order, .. } = receipt else {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance single-order cancel returned a non-order receipt",
            ));
        };
        if order.status != crypto_trading_domain::OrderStatus::Cancelled {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance single-order cancel did not return a cancelled order",
            ));
        }
        Ok(TradingReceipt::Cancelled {
            orders: vec![order],
            disposition: CancellationDisposition::Cancelled,
        })
    }

    async fn execute_cancel_all(
        &self,
        symbol: Option<crypto_trading_domain::Symbol>,
        market_type: Option<crypto_trading_domain::MarketType>,
    ) -> Result<TradingReceipt, ExchangeError> {
        let Some(market_type) = market_type.or_else(|| infer_market_type(symbol.as_ref())) else {
            return Err(ExchangeError::invalid(
                "Binance cancel_all needs an explicit market type or a market-qualified symbol",
            ));
        };
        let product = product_for_market(market_type);
        let response = self
            .dispatch_with_clock_retry(product, |timestamp_ms| {
                let symbol = symbol.as_ref().unwrap_or_else(|| unreachable!()).clone();
                async move {
                    self.protocol
                        .dispatch_cancel_all(&*self.transport, &symbol, market_type, timestamp_ms)
                        .await
                }
            })
            .await?;
        let observed_at = self.observe_response(&response);
        match product {
            BinanceProduct::Spot => {
                let (orders, foreign_orders) = self.protocol.parse_open_orders_response(
                    product,
                    response.body(),
                    observed_at,
                )?;
                if !foreign_orders.is_empty() {
                    return Err(ExchangeError::invalid_response(
                        EXCHANGE,
                        "Binance spot cancel-all returned foreign orders that cannot be correlated to owned receipts",
                    ));
                }
                let disposition = if orders.is_empty() {
                    CancellationDisposition::NoMatchingOrders
                } else {
                    CancellationDisposition::Cancelled
                };
                Ok(TradingReceipt::Cancelled {
                    orders,
                    disposition,
                })
            }
            BinanceProduct::UsdM => Ok(TradingReceipt::Cancelled {
                orders: Vec::new(),
                disposition: CancellationDisposition::Cancelled,
            }),
        }
    }

    async fn dispatch_with_clock_retry<F, Fut>(
        &self,
        product: BinanceProduct,
        send: F,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        F: Fn(u64) -> Fut,
        Fut: std::future::Future<Output = Result<RemoteHttpResponse, ExchangeError>>,
    {
        let mut timestamp_ms = self.timestamp_ms()?;
        let mut response = send(timestamp_ms).await;
        if matches!(&response, Err(error) if BinanceTestnetProtocol::is_clock_skew_error(error)) {
            self.sync_server_time(product).await?;
            timestamp_ms = self.timestamp_ms()?;
            response = send(timestamp_ms).await;
        }
        response
    }

    async fn send_authenticated_request<F>(
        &self,
        product: BinanceProduct,
        build_request: F,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        F: Fn(u64) -> Result<crate::RemoteHttpRequest, ExchangeError>,
    {
        let mut request = build_request(self.timestamp_ms()?)?;
        let mut response = self.transport.send(request).await?;
        if !response.is_success() {
            let error = BinanceTestnetProtocol::remote_failure_from_response(&response);
            if BinanceTestnetProtocol::is_clock_skew_error(&error) {
                self.sync_server_time(product).await?;
                request = build_request(self.timestamp_ms()?)?;
                response = self.transport.send(request).await?;
            }
        }
        Ok(response)
    }

    async fn sync_server_time(
        &self,
        product: BinanceProduct,
    ) -> Result<DateTime<Utc>, ExchangeError> {
        let request = self.protocol.build_server_time_request(product)?;
        let response = self.transport.send(request).await?;
        if !response.is_success() {
            return Err(BinanceTestnetProtocol::remote_failure_from_response(
                &response,
            ));
        }
        let server_time = self.protocol.parse_server_time_response(response.body())?;
        let local_now = (self.clock)();
        let offset = server_time
            .signed_duration_since(local_now)
            .num_milliseconds();
        *self
            .time_offset_ms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset;
        self.observe_at(server_time);
        Ok(server_time)
    }

    fn timestamp_ms(&self) -> Result<u64, ExchangeError> {
        let local_now = (self.clock)();
        let offset = *self
            .time_offset_ms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let adjusted = local_now
            .checked_add_signed(TimeDelta::milliseconds(offset))
            .ok_or_else(|| ExchangeError::invalid("Binance timestamp offset overflowed"))?;
        u64::try_from(adjusted.timestamp_millis())
            .map_err(|_| ExchangeError::invalid("Binance timestamp must not be negative"))
    }

    fn observe_response(&self, response: &RemoteHttpResponse) -> DateTime<Utc> {
        let fallback = (self.clock)();
        self.observe_at(response.server_time().unwrap_or(fallback))
    }

    fn observe_at(&self, candidate: DateTime<Utc>) -> DateTime<Utc> {
        let mut observed = self
            .observed_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *observed = (*observed).max(candidate);
        *observed
    }

    async fn reconcile_orders(
        &self,
        market_type: crypto_trading_domain::MarketType,
        symbol: Option<&crypto_trading_domain::Symbol>,
    ) -> Result<
        (
            Vec<crypto_trading_domain::Order>,
            Vec<crate::ForeignOrder>,
            DateTime<Utc>,
        ),
        ExchangeError,
    > {
        let response = self
            .send_authenticated_request(product_for_market(market_type), |timestamp_ms| {
                self.protocol
                    .build_open_orders_request(market_type, symbol, timestamp_ms)
            })
            .await?;
        if !response.is_success() {
            return Err(BinanceTestnetProtocol::remote_failure_from_response(
                &response,
            ));
        }
        let observed_at = self.observe_response(&response);
        let (orders, foreign_orders) = self.protocol.parse_open_orders_response(
            product_for_market(market_type),
            response.body(),
            observed_at,
        )?;
        Ok((orders, foreign_orders, observed_at))
    }

    async fn reconcile_positions(
        &self,
        symbol: Option<&crypto_trading_domain::Symbol>,
    ) -> Result<(Vec<crypto_trading_domain::Position>, DateTime<Utc>), ExchangeError> {
        let response = self
            .send_authenticated_request(BinanceProduct::UsdM, |timestamp_ms| {
                self.protocol.build_positions_request(symbol, timestamp_ms)
            })
            .await?;
        if !response.is_success() {
            return Err(BinanceTestnetProtocol::remote_failure_from_response(
                &response,
            ));
        }
        let observed_at = self.observe_response(&response);
        let positions = self
            .protocol
            .parse_positions_response(response.body(), observed_at)?;
        Ok((positions, observed_at))
    }
}

#[async_trait]
impl ExchangeHandle for BinanceTestnetExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        match command {
            TradingCommand::Submit(intent) => self.execute_submit(intent).await,
            TradingCommand::Cancel { order_id } => {
                let order_ref = self.protocol.parse_server_order_ref(&order_id)?;
                self.execute_cancel(order_ref).await
            }
            TradingCommand::CancelAll {
                symbol,
                market_type,
            } => {
                let Some(symbol) = symbol else {
                    return Err(ExchangeError::invalid(
                        "Binance cancel_all requires an explicit symbol",
                    ));
                };
                self.execute_cancel_all(Some(symbol), market_type).await
            }
        }
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        match &scope {
            ReconcileScope::All => {
                let (mut spot_orders, mut spot_foreign, observed_spot) = self
                    .reconcile_orders(crypto_trading_domain::MarketType::Spot, None)
                    .await?;
                let (mut usdm_orders, mut usdm_foreign, observed_usdm_orders) = self
                    .reconcile_orders(crypto_trading_domain::MarketType::Perpetual, None)
                    .await?;
                let (positions, observed_positions) = self.reconcile_positions(None).await?;
                let observed_at = self.observe_at(
                    observed_spot
                        .max(observed_usdm_orders)
                        .max(observed_positions),
                );
                spot_orders.append(&mut usdm_orders);
                spot_foreign.append(&mut usdm_foreign);
                Ok(ReconcileReceipt {
                    scope,
                    orders: spot_orders,
                    foreign_orders: spot_foreign,
                    positions,
                    observed_at,
                })
            }
            ReconcileScope::Orders { symbol } => {
                if let Some(symbol) = symbol.as_ref() {
                    let supported = [
                        crypto_trading_domain::MarketType::Spot,
                        crypto_trading_domain::MarketType::Perpetual,
                    ]
                    .into_iter()
                    .any(|market_type| self.protocol.supports_symbol_market(symbol, market_type));
                    if !supported {
                        return Err(ExchangeError::invalid(format!(
                            "Binance does not support reconcile orders for explicit symbol {symbol}"
                        )));
                    }
                }
                let mut orders = Vec::new();
                let mut foreign_orders = Vec::new();
                let mut observed_at = *self
                    .observed_at
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for market_type in products_for_scope(&self.protocol, symbol.as_ref()) {
                    let (mut lane_orders, mut lane_foreign, lane_observed) =
                        self.reconcile_orders(market_type, symbol.as_ref()).await?;
                    observed_at = observed_at.max(lane_observed);
                    orders.append(&mut lane_orders);
                    foreign_orders.append(&mut lane_foreign);
                }
                Ok(ReconcileReceipt {
                    scope,
                    orders,
                    foreign_orders,
                    positions: Vec::new(),
                    observed_at: self.observe_at(observed_at),
                })
            }
            ReconcileScope::Positions { symbol } => {
                if let Some(symbol) = symbol.as_ref()
                    && !self.protocol.supports_symbol_market(
                        symbol,
                        crypto_trading_domain::MarketType::Perpetual,
                    )
                {
                    return Err(ExchangeError::invalid(format!(
                        "Binance does not support reconcile positions for explicit symbol {symbol}"
                    )));
                }
                let symbol = symbol.as_ref();
                let (positions, observed_at) = self.reconcile_positions(symbol).await?;
                Ok(ReconcileReceipt {
                    scope,
                    orders: Vec::new(),
                    foreign_orders: Vec::new(),
                    positions,
                    observed_at: self.observe_at(observed_at),
                })
            }
        }
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        Err(ExchangeError::Unsupported {
            exchange: EXCHANGE.to_owned(),
            operation: crate::ExchangeOperation::Subscribe,
        })
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Ok(ExchangeStatus {
            exchange: EXCHANGE.to_owned(),
            mode: ExchangeMode::Testnet,
            availability: ExchangeAvailability::Ready,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}

fn infer_market_type(
    symbol: Option<&crypto_trading_domain::Symbol>,
) -> Option<crypto_trading_domain::MarketType> {
    let symbol = symbol?;
    if symbol.as_str().ends_with("-SPOT") {
        Some(crypto_trading_domain::MarketType::Spot)
    } else if symbol.as_str().ends_with("-PERP") {
        Some(crypto_trading_domain::MarketType::Perpetual)
    } else {
        None
    }
}

fn product_for_market(market_type: crypto_trading_domain::MarketType) -> BinanceProduct {
    match market_type {
        crypto_trading_domain::MarketType::Spot => BinanceProduct::Spot,
        crypto_trading_domain::MarketType::Perpetual => BinanceProduct::UsdM,
    }
}

fn products_for_scope(
    protocol: &BinanceTestnetProtocol,
    symbol: Option<&crypto_trading_domain::Symbol>,
) -> Vec<crypto_trading_domain::MarketType> {
    match symbol {
        Some(symbol) => [
            crypto_trading_domain::MarketType::Spot,
            crypto_trading_domain::MarketType::Perpetual,
        ]
        .into_iter()
        .filter(|market_type| protocol.supports_symbol_market(symbol, *market_type))
        .collect(),
        None => vec![
            crypto_trading_domain::MarketType::Spot,
            crypto_trading_domain::MarketType::Perpetual,
        ],
    }
}
