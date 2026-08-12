use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};

use crate::{
    BinanceProduct, BinanceServerOrderRef, BinanceTestnetBalance, BinanceTestnetProtocol,
    CancellationDisposition, ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode,
    ExchangeStatus, ForeignOrder, MarketSubscription, ReconcileReceipt, ReconcileScope,
    RemoteHttpResponse, RemoteHttpTransport, SubscriptionReceipt, TradingCommand, TradingReceipt,
};

const EXCHANGE: &str = "binance";

/// Largest difference accepted between the venue clock and the local clock.
///
/// Binance's default `recvWindow` is 5 seconds; this bound is deliberately
/// wider so ordinary network latency still synchronises, while a nonsensical
/// server time or a tampered `Date` header is rejected rather than adopted.
const MAXIMUM_CLOCK_OFFSET_MS: i64 = 60_000;

type Clock = dyn Fn() -> DateTime<Utc> + Send + Sync;

/// One product-scoped Binance Testnet account truth snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceTestnetAccountSnapshot {
    pub product: BinanceProduct,
    pub balances: Vec<BinanceTestnetBalance>,
    pub orders: Vec<crypto_trading_domain::Order>,
    pub foreign_orders: Vec<ForeignOrder>,
    pub positions: Vec<crypto_trading_domain::Position>,
    pub observed_at: DateTime<Utc>,
}

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

    /// Queries one order by the UUID client identity that callers persisted
    /// before dispatch.
    ///
    /// This is the recovery seam for ambiguous submissions: it never expands
    /// to all open orders and retries once after an authoritative clock sync.
    ///
    /// # Errors
    ///
    /// Returns a bounded exchange error when the request fails, the response
    /// is malformed, or Binance returns a different client identity.
    pub async fn query_order(
        &self,
        symbol: &crypto_trading_domain::Symbol,
        market_type: crypto_trading_domain::MarketType,
        client_order_id: uuid::Uuid,
    ) -> Result<crypto_trading_domain::Order, ExchangeError> {
        let response = self
            .send_authenticated_request(product_for_market(market_type), |timestamp_ms| {
                self.protocol.build_query_order_request(
                    symbol,
                    market_type,
                    client_order_id,
                    timestamp_ms,
                )
            })
            .await?;
        if !response.is_success() {
            return Err(BinanceTestnetProtocol::remote_failure_from_response(
                &response,
            ));
        }
        let observed_at = self.observe_response(&response);
        let receipt = self.protocol.parse_order_response(
            product_for_market(market_type),
            response.body(),
            observed_at,
        )?;
        let TradingReceipt::Submitted { order, .. } = receipt else {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance single-order query returned a non-order receipt",
            ));
        };
        if order.intent.client_order_id != client_order_id {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance single-order query returned a different client order id",
            ));
        }
        Ok(order)
    }

    /// Queries balance, open-order, and position truth for exactly one Testnet
    /// product using two complete consecutive samples.
    ///
    /// Spot snapshots contain no positions. USD-M snapshots include the
    /// product-wide position-risk response. The two samples must have identical
    /// balance/order/position state; observed drift fails closed instead of
    /// returning a torn composite snapshot. Every signed route retains the
    /// adapter's one-shot clock-skew recovery and bounded response handling.
    ///
    /// # Errors
    ///
    /// Returns a bounded exchange error if any component cannot be sampled or
    /// parsed. Partial snapshots are never returned.
    pub async fn account_snapshot(
        &self,
        product: BinanceProduct,
    ) -> Result<BinanceTestnetAccountSnapshot, ExchangeError> {
        let first = self.sample_account_snapshot(product).await?;
        let second = self.sample_account_snapshot(product).await?;
        if !same_account_state(&first, &second) {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance Testnet account state changed across the bounded double sample",
            ));
        }
        Ok(second)
    }

    async fn sample_account_snapshot(
        &self,
        product: BinanceProduct,
    ) -> Result<BinanceTestnetAccountSnapshot, ExchangeError> {
        let (balances, observed_balances) = self.account_balances(product).await?;
        let market_type = match product {
            BinanceProduct::Spot => crypto_trading_domain::MarketType::Spot,
            BinanceProduct::UsdM => crypto_trading_domain::MarketType::Perpetual,
        };
        let (orders, foreign_orders, observed_orders) =
            self.reconcile_orders(market_type, None).await?;
        let (positions, observed_positions) = match product {
            BinanceProduct::Spot => (Vec::new(), observed_orders),
            BinanceProduct::UsdM => self.reconcile_positions(None).await?,
        };
        let observed_at = self.observe_at(
            observed_balances
                .max(observed_orders)
                .max(observed_positions),
        );
        Ok(BinanceTestnetAccountSnapshot {
            product,
            balances,
            orders,
            foreign_orders,
            positions,
            observed_at,
        })
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
        // Binance has no account-wide cancel endpoint: every cancel-all is
        // scoped to one symbol. Reject a missing symbol here rather than
        // panicking deeper in the dispatch closure.
        let Some(symbol) = symbol else {
            return Err(ExchangeError::invalid(
                "Binance cancel_all needs an explicit symbol",
            ));
        };
        let product = product_for_market(market_type);
        let response = self
            .dispatch_with_clock_retry(product, |timestamp_ms| {
                let symbol = symbol.clone();
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
            BinanceProduct::UsdM => {
                BinanceTestnetProtocol::parse_usdm_cancel_all_response(response.body())?;
                Ok(TradingReceipt::Cancelled {
                    orders: Vec::new(),
                    disposition: CancellationDisposition::Cancelled,
                })
            }
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
        // An unbounded offset lets one wrong server time silently shift every
        // later signed request, which the venue then rejects as clock skew.
        // Refuse the sample instead of adopting it.
        if offset.saturating_abs() > MAXIMUM_CLOCK_OFFSET_MS {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance server time differs from local time beyond the accepted clock offset",
            ));
        }
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

    /// Observation timestamps are monotonic and drive reconciliation
    /// sequencing, so a single far-future response header would otherwise
    /// permanently block later snapshots. Candidates are clamped to the local
    /// clock plus the accepted offset before they can advance the watermark.
    fn observe_at(&self, candidate: DateTime<Utc>) -> DateTime<Utc> {
        let ceiling = (self.clock)()
            .checked_add_signed(TimeDelta::milliseconds(MAXIMUM_CLOCK_OFFSET_MS))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let mut observed = self
            .observed_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *observed = (*observed).max(candidate.min(ceiling));
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

    async fn account_balances(
        &self,
        product: BinanceProduct,
    ) -> Result<(Vec<BinanceTestnetBalance>, DateTime<Utc>), ExchangeError> {
        let response = self
            .send_authenticated_request(product, |timestamp_ms| {
                self.protocol
                    .build_account_balances_request(product, timestamp_ms)
            })
            .await?;
        if !response.is_success() {
            return Err(BinanceTestnetProtocol::remote_failure_from_response(
                &response,
            ));
        }
        let observed_at = self.observe_response(&response);
        let balances = self
            .protocol
            .parse_account_balances_response(product, response.body())?;
        Ok((balances, observed_at))
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

fn same_account_state(
    left: &BinanceTestnetAccountSnapshot,
    right: &BinanceTestnetAccountSnapshot,
) -> bool {
    let mut left_balances = left.balances.clone();
    let mut right_balances = right.balances.clone();
    left_balances.sort_by(|left, right| left.asset.cmp(&right.asset));
    right_balances.sort_by(|left, right| left.asset.cmp(&right.asset));

    let mut left_orders = left.orders.clone();
    let mut right_orders = right.orders.clone();
    left_orders.sort_by(compare_orders_by_identity);
    right_orders.sort_by(compare_orders_by_identity);

    let mut left_foreign_orders = left.foreign_orders.clone();
    let mut right_foreign_orders = right.foreign_orders.clone();
    left_foreign_orders.sort_by(compare_foreign_orders_by_identity);
    right_foreign_orders.sort_by(compare_foreign_orders_by_identity);

    let mut left_positions = left.positions.clone();
    let mut right_positions = right.positions.clone();
    left_positions.sort_by(compare_positions_by_identity);
    right_positions.sort_by(compare_positions_by_identity);

    left.product == right.product
        && left_balances == right_balances
        && left_orders == right_orders
        && left_foreign_orders == right_foreign_orders
        && left_positions == right_positions
}

fn compare_orders_by_identity(
    left: &crypto_trading_domain::Order,
    right: &crypto_trading_domain::Order,
) -> std::cmp::Ordering {
    left.id
        .cmp(&right.id)
        .then_with(|| {
            left.intent
                .client_order_id
                .cmp(&right.intent.client_order_id)
        })
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.updated_at.cmp(&right.updated_at))
}

fn compare_foreign_orders_by_identity(
    left: &ForeignOrder,
    right: &ForeignOrder,
) -> std::cmp::Ordering {
    left.id
        .cmp(&right.id)
        .then_with(|| left.client_order_id.cmp(&right.client_order_id))
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.updated_at.cmp(&right.updated_at))
}

fn compare_positions_by_identity(
    left: &crypto_trading_domain::Position,
    right: &crypto_trading_domain::Position,
) -> std::cmp::Ordering {
    left.exchange
        .cmp(&right.exchange)
        .then_with(|| left.symbol.cmp(&right.symbol))
        .then_with(|| market_type_rank(left.market_type).cmp(&market_type_rank(right.market_type)))
        .then_with(|| position_side_rank(left.side).cmp(&position_side_rank(right.side)))
        .then_with(|| left.updated_at.cmp(&right.updated_at))
}

const fn market_type_rank(market_type: crypto_trading_domain::MarketType) -> u8 {
    match market_type {
        crypto_trading_domain::MarketType::Spot => 0,
        crypto_trading_domain::MarketType::Perpetual => 1,
    }
}

const fn position_side_rank(side: crypto_trading_domain::PositionSide) -> u8 {
    match side {
        crypto_trading_domain::PositionSide::Long => 0,
        crypto_trading_domain::PositionSide::Short => 1,
        crypto_trading_domain::PositionSide::Flat => 2,
    }
}
