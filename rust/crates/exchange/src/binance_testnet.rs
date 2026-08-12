use std::{fmt, sync::Arc};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderIntent, OrderStatus, OrderType, Position,
    PositionSide, Price, Quantity, Side, Symbol, TimeInForce,
};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    BinanceProduct, BinanceTestnetEndpoints, ExchangeError, ExchangeOperation,
    ExchangeOperationKey, ExchangeSymbol, ExchangeSymbolCatalog, ForeignOrder,
    InstrumentRuleCatalog, InstrumentRuleOptions, InstrumentRules, RemoteHttpMethod,
    RemoteHttpRequest, RemoteHttpResponse, RemoteHttpTransport, SubmissionDisposition,
    TradingReceipt, sha256::HmacSha256Key,
};

const EXCHANGE: &str = "binance";
const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;
const MAX_RECV_WINDOW_MS: u64 = 60_000;
const MAX_API_KEY_BYTES: usize = 1_024;
const MAX_SECRET_BYTES: usize = 2_048;
const MAX_SIGNATURE_BYTES: usize = 2_048;
const MAX_SERVER_ORDER_REF_BYTES: usize = 128;
const MAX_ACCOUNT_BALANCES: usize = 4_096;
const MAX_ASSET_BYTES: usize = 32;
const MAX_EXCHANGE_INFO_SYMBOLS: usize = 16;

/// Credential-backed signing seam for Binance authenticated requests.
///
/// Implementations own secret storage and the concrete HMAC/RSA/Ed25519
/// algorithm. The protocol presents the exact encoded parameter string that
/// Binance verifies.
pub trait BinanceRequestSigner: Send + Sync {
    fn api_key(&self) -> &str;

    /// Signs the exact URL-encoded query excluding the `signature` parameter.
    ///
    /// # Errors
    ///
    /// Returns an exchange error when credentials or signing are unavailable.
    fn sign(&self, payload: &str) -> Result<String, ExchangeError>;
}

/// Production HMAC-SHA256 signer for Binance testnet requests.
#[derive(Clone)]
pub struct BinanceHmacSha256Signer {
    api_key: String,
    secret: HmacSha256Key,
}

impl fmt::Debug for BinanceHmacSha256Signer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceHmacSha256Signer")
            .field("api_key_bytes", &self.api_key.len())
            .finish_non_exhaustive()
    }
}

impl BinanceHmacSha256Signer {
    /// Builds a signer from one API key and one API secret.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when either credential is
    /// blank or structurally unsafe.
    pub fn new(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
    ) -> Result<Self, ExchangeError> {
        let api_key = api_key.into();
        let api_secret = api_secret.into();
        validate_secret_text("Binance API key", &api_key, MAX_API_KEY_BYTES)?;
        validate_secret_text("Binance API secret", &api_secret, MAX_SECRET_BYTES)?;
        let mut secret_bytes = api_secret.into_bytes();
        let secret = HmacSha256Key::new(&secret_bytes);
        secret_bytes.fill(0);
        Ok(Self { api_key, secret })
    }
}

impl BinanceRequestSigner for BinanceHmacSha256Signer {
    fn api_key(&self) -> &str {
        &self.api_key
    }

    fn sign(&self, payload: &str) -> Result<String, ExchangeError> {
        Ok(hex_lower(&self.secret.sign(payload.as_bytes())))
    }
}

/// Stable server-order identifier embedded in Binance typed receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceServerOrderRef {
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub wire_symbol: String,
    pub order_id: u64,
}

/// One asset balance from a signed Binance Testnet account snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceTestnetBalance {
    pub asset: String,
    pub wallet_balance: Decimal,
    pub available_balance: Decimal,
    pub locked_balance: Option<Decimal>,
}

/// Signed parameters for `userDataStream.subscribe.signature` on the Binance
/// Spot websocket API.
#[derive(Clone, PartialEq, Eq)]
pub struct BinanceWsApiUserDataStreamSubscription {
    pub api_key: String,
    pub timestamp_ms: u64,
    pub recv_window_ms: Option<u64>,
    pub signature: String,
}

impl fmt::Debug for BinanceWsApiUserDataStreamSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceWsApiUserDataStreamSubscription")
            .field("api_key", &"<redacted>")
            .field("timestamp_ms", &self.timestamp_ms)
            .field("recv_window_ms", &self.recv_window_ms)
            .field("signature", &"<redacted>")
            .finish()
    }
}

impl BinanceWsApiUserDataStreamSubscription {
    #[must_use]
    pub fn signed_payload(&self) -> String {
        match self.recv_window_ms {
            Some(recv_window_ms) => format!(
                "apiKey={}&recvWindow={recv_window_ms}&timestamp={}",
                self.api_key, self.timestamp_ms
            ),
            None => format!("apiKey={}&timestamp={}", self.api_key, self.timestamp_ms),
        }
    }
}

/// One parsed Binance Spot user-data event.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceUserDataEvent {
    ExecutionReport(BinanceExecutionReportEvent),
    AccountUpdate(BinanceAccountUpdateEvent),
    StreamTerminated(BinanceStreamTerminatedEvent),
    Unsupported(BinanceUnsupportedUserDataEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceExecutionReportEvent {
    pub event_time: DateTime<Utc>,
    pub transaction_time: DateTime<Utc>,
    pub symbol: Symbol,
    pub client_order_id: String,
    pub execution_type: String,
    pub order_status: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub order_id: u64,
    pub execution_id: Option<u64>,
    pub quantity: Quantity,
    pub price: Option<Price>,
    pub last_executed_quantity: Option<Quantity>,
    pub cumulative_filled_quantity: Quantity,
    pub last_executed_price: Option<Price>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceAccountUpdateEvent {
    pub event_time: DateTime<Utc>,
    pub account_update_time: DateTime<Utc>,
    pub balances: Vec<BinanceUserDataBalance>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUserDataBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceStreamTerminatedEvent {
    pub event_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUnsupportedUserDataEvent {
    pub event_type: String,
    pub event_time: Option<DateTime<Utc>>,
}

/// One exact Binance exchangeInfo selection resolved into shared symbol/rule types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceExchangeInfoSymbol {
    pub symbol: ExchangeSymbol,
    pub rules: InstrumentRules,
}

/// Deterministic Binance Spot and USDⓈ-M testnet request protocol.
pub struct BinanceTestnetProtocol {
    endpoints: BinanceTestnetEndpoints,
    symbols: ExchangeSymbolCatalog,
    rules: InstrumentRuleCatalog,
    signer: Arc<dyn BinanceRequestSigner>,
    recv_window_ms: u64,
}

impl fmt::Debug for BinanceTestnetProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinanceTestnetProtocol")
            .field("endpoints", &self.endpoints)
            .field("symbol_count", &self.symbols.len())
            .field("rule_count", &self.rules.len())
            .field("recv_window_ms", &self.recv_window_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookTickerWire {
    symbol: String,
    bid_price: String,
    bid_qty: String,
    ask_price: String,
    ask_qty: String,
}

/// Code Binance USD-M reports in the body of a successful cancel-all.
const USDM_CANCEL_ALL_SUCCESS_CODE: i64 = 200;

#[derive(Debug, Deserialize)]
struct BinanceErrorWire {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceServerTimeWire {
    server_time: i64,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoWire {
    #[serde(default)]
    symbols: Vec<BinanceExchangeInfoSymbolWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceExchangeInfoSymbolWire {
    symbol: String,
    status: String,
    #[serde(default)]
    base_asset: Option<String>,
    #[serde(default)]
    quote_asset: Option<String>,
    #[serde(default)]
    pair: Option<String>,
    #[serde(default)]
    contract_type: Option<String>,
    #[serde(default)]
    filters: Vec<BinanceExchangeInfoFilterWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceExchangeInfoFilterWire {
    filter_type: String,
    #[serde(default)]
    min_price: Option<String>,
    #[serde(default)]
    max_price: Option<String>,
    #[serde(default)]
    tick_size: Option<String>,
    #[serde(default)]
    min_qty: Option<String>,
    #[serde(default)]
    max_qty: Option<String>,
    #[serde(default)]
    step_size: Option<String>,
    #[serde(default, alias = "notional")]
    min_notional: Option<String>,
    #[serde(default)]
    max_notional: Option<String>,
    #[serde(default)]
    apply_to_market: Option<bool>,
    #[serde(default)]
    apply_min_to_market: Option<bool>,
    #[serde(default)]
    apply_max_to_market: Option<bool>,
    #[serde(default)]
    avg_price_mins: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceOrderWire {
    symbol: String,
    order_id: u64,
    client_order_id: String,
    /// Cancel responses report the cancelled order's identity here and reuse
    /// `clientOrderId` for the cancel request itself, so this field is the
    /// authoritative identity whenever Binance sends it.
    #[serde(default)]
    orig_client_order_id: Option<String>,
    #[serde(default)]
    transact_time: Option<i64>,
    #[serde(default)]
    update_time: Option<i64>,
    #[serde(default)]
    price: String,
    orig_qty: String,
    executed_qty: String,
    #[serde(default)]
    avg_price: Option<String>,
    #[serde(default)]
    cummulative_quote_qty: Option<String>,
    #[serde(default)]
    cum_quote: Option<String>,
    status: String,
    #[serde(default)]
    time_in_force: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    #[serde(default)]
    reduce_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinancePositionWire {
    symbol: String,
    position_amt: String,
    #[serde(default)]
    position_side: String,
    #[serde(default)]
    entry_price: String,
    #[serde(default)]
    mark_price: String,
    #[serde(default)]
    update_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BinanceSpotAccountWire {
    balances: Vec<BinanceSpotBalanceWire>,
}

#[derive(Debug, Deserialize)]
struct BinanceSpotBalanceWire {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceUsdMBalanceWire {
    asset: String,
    balance: String,
    available_balance: String,
}

enum ParsedBinanceOrder {
    Owned(Order),
    Foreign(ForeignOrder),
}

impl BinanceTestnetProtocol {
    /// Builds an authenticated, testnet-only protocol.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a malformed or blank API
    /// key. Secrets are retained only by the signer implementation.
    pub fn authenticated<S>(
        endpoints: BinanceTestnetEndpoints,
        symbols: ExchangeSymbolCatalog,
        rules: InstrumentRuleCatalog,
        signer: Arc<S>,
    ) -> Result<Self, ExchangeError>
    where
        S: BinanceRequestSigner + 'static,
    {
        validate_secret_text("Binance API key", signer.api_key(), MAX_API_KEY_BYTES)?;
        Ok(Self {
            endpoints,
            symbols,
            rules,
            signer,
            recv_window_ms: DEFAULT_RECV_WINDOW_MS,
        })
    }

    /// Overrides Binance's receive window while retaining the documented hard
    /// maximum.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] outside `1..=60000`.
    pub fn set_recv_window_ms(&mut self, recv_window_ms: u64) -> Result<(), ExchangeError> {
        if !(1..=MAX_RECV_WINDOW_MS).contains(&recv_window_ms) {
            return Err(ExchangeError::invalid(format!(
                "Binance recvWindow must be in 1..={MAX_RECV_WINDOW_MS} milliseconds"
            )));
        }
        self.recv_window_ms = recv_window_ms;
        Ok(())
    }

    /// Builds the signed websocket-API parameters for
    /// `userDataStream.subscribe.signature`.
    ///
    /// The websocket API signs a plain `apiKey[..]&timestamp[..]` parameter
    /// string sorted lexicographically by parameter name. This differs from
    /// the REST URL builder and intentionally does not percent-encode the
    /// payload before signing.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when `recvWindow` is outside
    /// Binance's accepted range, or a signing/credential error from the
    /// configured signer.
    pub fn build_user_data_stream_subscribe_signature(
        &self,
        timestamp_ms: u64,
        recv_window_ms: Option<u64>,
    ) -> Result<BinanceWsApiUserDataStreamSubscription, ExchangeError> {
        if let Some(recv_window_ms) = recv_window_ms
            && !(1..=MAX_RECV_WINDOW_MS).contains(&recv_window_ms)
        {
            return Err(ExchangeError::invalid(format!(
                "Binance recvWindow must be in 1..={MAX_RECV_WINDOW_MS} milliseconds"
            )));
        }
        let subscription = BinanceWsApiUserDataStreamSubscription {
            api_key: self.signer.api_key().to_owned(),
            timestamp_ms,
            recv_window_ms,
            signature: String::new(),
        };
        let payload = subscription.signed_payload();
        let signature = self.signer.sign(&payload)?;
        validate_secret_text("Binance signature", &signature, MAX_SIGNATURE_BYTES)?;
        Ok(BinanceWsApiUserDataStreamSubscription {
            signature,
            ..subscription
        })
    }

    /// Parses one Binance Spot websocket user-data event. Current production
    /// payloads are wrapped as `{subscriptionId,event:{...}}`; flat top-level
    /// payloads remain accepted only for backward-compatible fixtures.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] when the payload is not
    /// valid JSON, is missing the event discriminator, or carries invalid
    /// timestamps/financial values.
    pub fn parse_user_data_event(payload: &[u8]) -> Result<BinanceUserDataEvent, ExchangeError> {
        let value: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let event = value.get("event").unwrap_or(&value);
        let event_type = event
            .get("e")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    EXCHANGE,
                    "Binance user-data payload is missing event type",
                )
            })?;
        match event_type {
            "executionReport" => parse_execution_report_event(event),
            "outboundAccountPosition" => parse_account_update_event(event),
            "eventStreamTerminated" => Ok(BinanceUserDataEvent::StreamTerminated(
                BinanceStreamTerminatedEvent {
                    event_time: optional_event_time(event)?,
                },
            )),
            other => Ok(BinanceUserDataEvent::Unsupported(
                BinanceUnsupportedUserDataEvent {
                    event_type: other.to_owned(),
                    event_time: optional_event_time(event)?,
                },
            )),
        }
    }

    /// Builds one fully signed Spot or USDⓈ-M testnet order request.
    ///
    /// Instrument rules and product semantics are checked before the signer is
    /// invoked.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid intent, missing exact symbol/rule,
    /// invalid product semantics, or signing failure.
    pub fn build_order_request(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Price>,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        if !intent.exchange.eq_ignore_ascii_case(EXCHANGE) {
            return Err(ExchangeError::invalid(format!(
                "Binance protocol cannot submit an order for exchange {}",
                intent.exchange
            )));
        }
        if intent.client_order_id.is_nil() {
            return Err(ExchangeError::invalid(
                "Binance client order id must not be nil",
            ));
        }
        validate_order_shape(intent)?;
        self.rules.validate_order(intent, reference_price)?;

        let product = product_for_market(intent.market_type);
        let wire_symbol = self
            .symbols
            .to_wire(EXCHANGE, &intent.symbol, intent.market_type)?;
        validate_binance_wire_symbol(wire_symbol)?;
        let mut parameters = vec![
            ("symbol", wire_symbol.to_owned()),
            (
                "side",
                match intent.side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                }
                .to_owned(),
            ),
        ];
        append_order_parameters(&mut parameters, product, intent)?;
        parameters.push((
            "newClientOrderId",
            intent.client_order_id.hyphenated().to_string(),
        ));
        parameters.push(("recvWindow", self.recv_window_ms.to_string()));
        parameters.push(("timestamp", timestamp_ms.to_string()));

        let path = match product {
            BinanceProduct::Spot => "/api/v3/order",
            BinanceProduct::UsdM => "/fapi/v1/order",
        };
        self.signed_request(RemoteHttpMethod::Post, product, path, &parameters)
    }

    /// Builds and dispatches one authenticated order with conservative unknown
    /// outcome semantics.
    ///
    /// Once the request is handed to a transport, transport failures, HTTP 408,
    /// HTTP 5xx, and Binance `-1007` are ambiguous and require reconciliation.
    ///
    /// # Errors
    ///
    /// Returns request-validation/signing failures before dispatch, a definite
    /// remote failure for known non-success responses, or
    /// [`ExchangeError::AmbiguousOutcome`] when execution cannot be proven.
    pub async fn dispatch_order<T>(
        &self,
        transport: &T,
        intent: &OrderIntent,
        reference_price: Option<Price>,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        T: RemoteHttpTransport + ?Sized,
    {
        let request = self.build_order_request(intent, reference_price, timestamp_ms)?;
        let key = ExchangeOperationKey::ClientOrderId {
            client_order_id: intent.client_order_id,
        };
        let result = transport.send(request).await;
        classify_mutating_response(
            result,
            ExchangeOperation::SubmitOrder,
            Some(intent.client_order_id),
            key,
        )
    }

    /// Builds a signed cancellation for one known Binance server order.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero order ID, missing exact symbol mapping, or
    /// signing failure.
    pub fn build_cancel_request(
        &self,
        symbol: &crypto_trading_domain::Symbol,
        market_type: MarketType,
        order_id: u64,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        if order_id == 0 {
            return Err(ExchangeError::invalid(
                "Binance server order id must not be zero",
            ));
        }
        let product = product_for_market(market_type);
        let wire_symbol = self.symbols.to_wire(EXCHANGE, symbol, market_type)?;
        validate_binance_wire_symbol(wire_symbol)?;
        let path = match product {
            BinanceProduct::Spot => "/api/v3/order",
            BinanceProduct::UsdM => "/fapi/v1/order",
        };
        self.signed_request(
            RemoteHttpMethod::Delete,
            product,
            path,
            &[
                ("symbol", wire_symbol.to_owned()),
                ("orderId", order_id.to_string()),
                ("recvWindow", self.recv_window_ms.to_string()),
                ("timestamp", timestamp_ms.to_string()),
            ],
        )
    }

    /// Builds and dispatches one server-order cancellation.
    ///
    /// # Errors
    ///
    /// Returns request-validation/signing errors before dispatch and an
    /// ambiguous outcome for indeterminate transport/server results.
    pub async fn dispatch_cancel<T>(
        &self,
        transport: &T,
        symbol: &crypto_trading_domain::Symbol,
        market_type: MarketType,
        order_id: u64,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        T: RemoteHttpTransport + ?Sized,
    {
        let request = self.build_cancel_request(symbol, market_type, order_id, timestamp_ms)?;
        let market_label = match market_type {
            MarketType::Spot => "spot",
            MarketType::Perpetual => "usdm",
        };
        let wire_symbol = self.symbols.to_wire(EXCHANGE, symbol, market_type)?;
        validate_binance_wire_symbol(wire_symbol)?;
        classify_mutating_response(
            transport.send(request).await,
            ExchangeOperation::CancelOrder,
            None,
            ExchangeOperationKey::OrderId(format!(
                "{EXCHANGE}:{market_label}:{wire_symbol}:{order_id}"
            )),
        )
    }

    /// Builds a product-scoped cancel-all request for one exact symbol.
    ///
    /// # Errors
    ///
    /// Returns an error for missing exact symbol mapping or signing failure.
    pub fn build_cancel_all_request(
        &self,
        symbol: &crypto_trading_domain::Symbol,
        market_type: MarketType,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let product = product_for_market(market_type);
        let wire_symbol = self.symbols.to_wire(EXCHANGE, symbol, market_type)?;
        validate_binance_wire_symbol(wire_symbol)?;
        let path = match product {
            BinanceProduct::Spot => "/api/v3/openOrders",
            BinanceProduct::UsdM => "/fapi/v1/allOpenOrders",
        };
        self.signed_request(
            RemoteHttpMethod::Delete,
            product,
            path,
            &[
                ("symbol", wire_symbol.to_owned()),
                ("recvWindow", self.recv_window_ms.to_string()),
                ("timestamp", timestamp_ms.to_string()),
            ],
        )
    }

    /// Dispatches one exact-symbol cancel-all request with conservative
    /// unknown-outcome semantics.
    ///
    /// # Errors
    ///
    /// Returns request-validation/signing failures before dispatch and
    /// [`ExchangeError::AmbiguousOutcome`] when neither the transport nor the
    /// remote server proves whether the cancellation took effect.
    pub async fn dispatch_cancel_all<T>(
        &self,
        transport: &T,
        symbol: &crypto_trading_domain::Symbol,
        market_type: MarketType,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        T: RemoteHttpTransport + ?Sized,
    {
        let request = self.build_cancel_all_request(symbol, market_type, timestamp_ms)?;
        classify_mutating_response(
            transport.send(request).await,
            ExchangeOperation::CancelAll,
            None,
            ExchangeOperationKey::CancelAll {
                symbol: Some(symbol.clone()),
                market_type: Some(market_type),
            },
        )
    }

    /// Builds an authoritative single-order query keyed by the client order
    /// identifier that was made durable before submission.
    ///
    /// # Errors
    ///
    /// Returns an error for a nil client order ID, a missing exact symbol
    /// mapping, or signing failure.
    pub fn build_query_order_request(
        &self,
        symbol: &crypto_trading_domain::Symbol,
        market_type: MarketType,
        client_order_id: uuid::Uuid,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        if client_order_id.is_nil() {
            return Err(ExchangeError::invalid(
                "Binance client order id must not be nil",
            ));
        }
        let product = product_for_market(market_type);
        let wire_symbol = self.symbols.to_wire(EXCHANGE, symbol, market_type)?;
        validate_binance_wire_symbol(wire_symbol)?;
        let path = match product {
            BinanceProduct::Spot => "/api/v3/order",
            BinanceProduct::UsdM => "/fapi/v1/order",
        };
        self.signed_request(
            RemoteHttpMethod::Get,
            product,
            path,
            &[
                ("symbol", wire_symbol.to_owned()),
                (
                    "origClientOrderId",
                    client_order_id.hyphenated().to_string(),
                ),
                ("recvWindow", self.recv_window_ms.to_string()),
                ("timestamp", timestamp_ms.to_string()),
            ],
        )
    }

    /// Builds an authoritative open-orders query for one Binance product.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatched/missing symbol mapping or signing
    /// failure.
    pub fn build_open_orders_request(
        &self,
        market_type: MarketType,
        symbol: Option<&crypto_trading_domain::Symbol>,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let product = product_for_market(market_type);
        let mut parameters = Vec::new();
        if let Some(symbol) = symbol {
            let wire_symbol = self.symbols.to_wire(EXCHANGE, symbol, market_type)?;
            validate_binance_wire_symbol(wire_symbol)?;
            parameters.push(("symbol", wire_symbol.to_owned()));
        }
        parameters.push(("recvWindow", self.recv_window_ms.to_string()));
        parameters.push(("timestamp", timestamp_ms.to_string()));
        let path = match product {
            BinanceProduct::Spot => "/api/v3/openOrders",
            BinanceProduct::UsdM => "/fapi/v1/openOrders",
        };
        self.signed_request(RemoteHttpMethod::Get, product, path, &parameters)
    }

    /// Builds an authoritative USDⓈ-M position-risk query.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing perpetual symbol mapping or signing
    /// failure.
    pub fn build_positions_request(
        &self,
        symbol: Option<&crypto_trading_domain::Symbol>,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let mut parameters = Vec::new();
        if let Some(symbol) = symbol {
            let wire_symbol = self
                .symbols
                .to_wire(EXCHANGE, symbol, MarketType::Perpetual)?;
            validate_binance_wire_symbol(wire_symbol)?;
            parameters.push(("symbol", wire_symbol.to_owned()));
        }
        parameters.push(("recvWindow", self.recv_window_ms.to_string()));
        parameters.push(("timestamp", timestamp_ms.to_string()));
        self.signed_request(
            RemoteHttpMethod::Get,
            BinanceProduct::UsdM,
            "/fapi/v2/positionRisk",
            &parameters,
        )
    }

    /// Builds an authoritative account-balance request for one Binance
    /// Testnet product.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixed product route cannot be signed.
    pub fn build_account_balances_request(
        &self,
        product: BinanceProduct,
        timestamp_ms: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let mut parameters = Vec::new();
        if product == BinanceProduct::Spot {
            parameters.push(("omitZeroBalances", "true".to_owned()));
        }
        parameters.push(("recvWindow", self.recv_window_ms.to_string()));
        parameters.push(("timestamp", timestamp_ms.to_string()));
        let path = match product {
            BinanceProduct::Spot => "/api/v3/account",
            BinanceProduct::UsdM => "/fapi/v3/balance",
        };
        self.signed_request(RemoteHttpMethod::Get, product, path, &parameters)
    }

    /// Builds an unsigned one-shot top-of-book query for either Binance
    /// product.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact product symbol is not cataloged.
    pub fn build_book_ticker_request(
        &self,
        symbol: &crypto_trading_domain::Symbol,
        market_type: MarketType,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let product = product_for_market(market_type);
        let wire_symbol = self.symbols.to_wire(EXCHANGE, symbol, market_type)?;
        validate_binance_wire_symbol(wire_symbol)?;
        let path = match product {
            BinanceProduct::Spot => "/api/v3/ticker/bookTicker",
            BinanceProduct::UsdM => "/fapi/v1/ticker/bookTicker",
        };
        let mut url = self.endpoints.rest_url(product, path)?;
        url.query_pairs_mut().append_pair("symbol", wire_symbol);
        Ok(RemoteHttpRequest::new(
            RemoteHttpMethod::Get,
            url,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Builds an unsigned server-time query for one Binance testnet product.
    ///
    /// # Errors
    ///
    /// Returns an error when the fixed testnet route cannot be resolved.
    pub fn build_server_time_request(
        &self,
        product: BinanceProduct,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let path = match product {
            BinanceProduct::Spot => "/api/v3/time",
            BinanceProduct::UsdM => "/fapi/v1/time",
        };
        Ok(RemoteHttpRequest::new(
            RemoteHttpMethod::Get,
            self.endpoints.rest_url(product, path)?,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Builds an unsigned exact-symbol `exchangeInfo` request for one Binance product.
    ///
    /// # Errors
    ///
    /// Returns an error when the selected product route or requested wire
    /// symbol is invalid.
    pub fn build_exchange_info_request(
        endpoints: &BinanceTestnetEndpoints,
        product: BinanceProduct,
        wire_symbol: &str,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        validate_binance_wire_symbol(wire_symbol)?;
        let path = match product {
            BinanceProduct::Spot => "/api/v3/exchangeInfo",
            BinanceProduct::UsdM => "/fapi/v1/exchangeInfo",
        };
        let mut url = endpoints.rest_url(product, path)?;
        url.query_pairs_mut().append_pair("symbol", wire_symbol);
        Ok(RemoteHttpRequest::new(
            RemoteHttpMethod::Get,
            url,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Parses one exact symbol from a Binance `exchangeInfo` payload.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed filters,
    /// duplicate/missing symbols, mismatched asset identity, disabled trading
    /// status, or a non-perpetual USD-M contract.
    pub fn parse_exchange_info_symbol(
        product: BinanceProduct,
        payload: &[u8],
        standard_symbol: Symbol,
        requested_wire_symbol: &str,
    ) -> Result<BinanceExchangeInfoSymbol, ExchangeError> {
        validate_binance_wire_symbol(requested_wire_symbol)?;
        let wire: BinanceExchangeInfoWire = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        bounded_exchange_info_symbol_count(wire.symbols.len())?;
        let mut matches = wire
            .symbols
            .into_iter()
            .filter(|candidate| candidate.symbol == requested_wire_symbol);
        let selected = matches.next().ok_or_else(|| {
            ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "Binance exchangeInfo did not return the exact requested symbol {requested_wire_symbol}",
                ),
            )
        })?;
        if matches.next().is_some() {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "Binance exchangeInfo returned duplicate exact symbol entries for {requested_wire_symbol}",
                ),
            ));
        }
        if selected.status != "TRADING" {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "Binance exchangeInfo symbol {requested_wire_symbol} is not trading (status={})",
                    selected.status
                ),
            ));
        }
        if product == BinanceProduct::UsdM && selected.contract_type.as_deref() != Some("PERPETUAL")
        {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "Binance USD-M exchangeInfo symbol {requested_wire_symbol} is not a perpetual contract",
                ),
            ));
        }
        validate_exchange_info_identity(
            product,
            &selected,
            &standard_symbol,
            requested_wire_symbol,
        )?;
        let market_type = market_for_product(product);
        let symbol = ExchangeSymbol::new(
            EXCHANGE,
            standard_symbol.clone(),
            market_type,
            requested_wire_symbol,
        )
        .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let rules = parse_exchange_info_rules(
            standard_symbol,
            market_type,
            requested_wire_symbol,
            &selected,
        )?;
        Ok(BinanceExchangeInfoSymbol { symbol, rules })
    }

    /// Parses a Spot or USDⓈ-M `bookTicker` response through the exact reverse
    /// symbol catalog.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed decimals,
    /// crossed quotes, or an unknown product-specific wire symbol.
    pub fn parse_book_ticker(
        &self,
        product: BinanceProduct,
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<MarketSnapshot, ExchangeError> {
        let wire: BookTickerWire = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let market_type = market_for_product(product);
        let symbol = self
            .symbols
            .to_standard(EXCHANGE, &wire.symbol, market_type)?
            .clone();
        let bid = parse_price(&wire.bid_price)?;
        let ask = parse_price(&wire.ask_price)?;
        let bid_quantity = parse_quantity(&wire.bid_qty)?;
        let ask_quantity = parse_quantity(&wire.ask_qty)?;
        let mut snapshot =
            MarketSnapshot::new(EXCHANGE, symbol, market_type, bid, ask, received_at)
                .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        snapshot.bid_quantity = Some(bid_quantity);
        snapshot.ask_quantity = Some(ask_quantity);
        Ok(snapshot)
    }

    /// Parses a Binance server-time response.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed or out-of-range
    /// timestamps.
    pub fn parse_server_time_response(
        &self,
        payload: &[u8],
    ) -> Result<DateTime<Utc>, ExchangeError> {
        let wire: BinanceServerTimeWire = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        parse_required_millis(wire.server_time)
    }

    /// Parses a submit/cancel order object produced by this protocol into the
    /// shared typed receipt.
    ///
    /// Only UUID client IDs owned by this runtime are accepted. Manual or
    /// foreign order IDs require a separate reconciliation representation and
    /// are rejected here instead of being silently synthesized.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed, foreign, or
    /// internally inconsistent order data.
    pub fn parse_order_response(
        &self,
        product: BinanceProduct,
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<TradingReceipt, ExchangeError> {
        let wire: BinanceOrderWire = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let ParsedBinanceOrder::Owned(order) =
            self.parse_wire_order(product, wire, received_at, false)?
        else {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance order response must belong to an owned UUID-backed order",
            ));
        };
        let (_, disposition) = parse_order_status_label(order.status)?;
        Ok(TradingReceipt::Submitted { order, disposition })
    }

    /// Parses an authoritative open-orders response into owned and foreign
    /// order sets without inventing UUID ownership.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed arrays or
    /// internally inconsistent order data.
    pub fn parse_open_orders_response(
        &self,
        product: BinanceProduct,
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<(Vec<Order>, Vec<ForeignOrder>), ExchangeError> {
        let wires: Vec<BinanceOrderWire> = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let mut orders = Vec::new();
        let mut foreign_orders = Vec::new();
        orders
            .try_reserve(wires.len())
            .map_err(|_| ExchangeError::unavailable("unable to reserve Binance owned orders"))?;
        foreign_orders
            .try_reserve(wires.len())
            .map_err(|_| ExchangeError::unavailable("unable to reserve Binance foreign orders"))?;
        for wire in wires {
            match self.parse_wire_order(product, wire, received_at, true)? {
                ParsedBinanceOrder::Owned(order) => orders.push(order),
                ParsedBinanceOrder::Foreign(order) => foreign_orders.push(order),
            }
        }
        Ok((orders, foreign_orders))
    }

    /// Confirms a USD-M cancel-all acknowledgement.
    ///
    /// USD-M answers `DELETE /fapi/v1/allOpenOrders` with HTTP 200 and reports
    /// the real outcome in the body, so treating the status code as success
    /// would report a refused cancellation as a completed one.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] when the body is malformed or
    /// carries any code other than the documented success code.
    pub fn parse_usdm_cancel_all_response(payload: &[u8]) -> Result<(), ExchangeError> {
        let acknowledgement: BinanceErrorWire = serde_json::from_slice(payload).map_err(|_| {
            ExchangeError::invalid_response(
                EXCHANGE,
                "Binance USD-M cancel-all response is not a recognised acknowledgement",
            )
        })?;
        if acknowledgement.code == USDM_CANCEL_ALL_SUCCESS_CODE {
            return Ok(());
        }
        Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance USD-M cancel-all reported code {}",
                acknowledgement.code
            ),
        ))
    }

    /// Parses a USD-M position-risk snapshot into typed positions.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed decimals or
    /// symbol mismatches.
    pub fn parse_positions_response(
        &self,
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<Vec<Position>, ExchangeError> {
        let wires: Vec<BinancePositionWire> = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let mut positions = Vec::new();
        positions
            .try_reserve(wires.len())
            .map_err(|_| ExchangeError::unavailable("unable to reserve Binance positions"))?;
        for wire in &wires {
            let Some(position) = self.parse_position_wire(wire, received_at)? else {
                continue;
            };
            positions.push(position);
        }
        Ok(positions)
    }

    /// Parses one product-specific signed account balance response.
    ///
    /// Spot exposes free and locked balances from `/api/v3/account`; USD-M
    /// exposes wallet and available balances from `/fapi/v3/balance`.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed, duplicate, or
    /// unsafe asset balances, and [`ExchangeError::ResourceLimit`] for an
    /// account response outside the supported bound.
    pub fn parse_account_balances_response(
        &self,
        product: BinanceProduct,
        payload: &[u8],
    ) -> Result<Vec<BinanceTestnetBalance>, ExchangeError> {
        let mut balances = match product {
            BinanceProduct::Spot => {
                let wire: BinanceSpotAccountWire =
                    serde_json::from_slice(payload).map_err(|error| {
                        ExchangeError::invalid_response(EXCHANGE, error.to_string())
                    })?;
                bounded_account_balance_count(wire.balances.len())?;
                wire.balances
                    .into_iter()
                    .map(|balance| {
                        validate_binance_asset(&balance.asset)?;
                        let available_balance =
                            parse_balance_decimal("Spot free balance", &balance.free, false)?;
                        let locked_balance =
                            parse_balance_decimal("Spot locked balance", &balance.locked, false)?;
                        let wallet_balance = available_balance
                            .checked_add(locked_balance)
                            .ok_or_else(|| {
                                ExchangeError::invalid_response(
                                    EXCHANGE,
                                    "Binance Spot balance overflowed",
                                )
                            })?;
                        Ok(BinanceTestnetBalance {
                            asset: balance.asset,
                            wallet_balance,
                            available_balance,
                            locked_balance: Some(locked_balance),
                        })
                    })
                    .collect::<Result<Vec<_>, ExchangeError>>()?
            }
            BinanceProduct::UsdM => {
                let wire: Vec<BinanceUsdMBalanceWire> =
                    serde_json::from_slice(payload).map_err(|error| {
                        ExchangeError::invalid_response(EXCHANGE, error.to_string())
                    })?;
                bounded_account_balance_count(wire.len())?;
                wire.into_iter()
                    .map(|balance| {
                        validate_binance_asset(&balance.asset)?;
                        Ok(BinanceTestnetBalance {
                            asset: balance.asset,
                            wallet_balance: parse_balance_decimal(
                                "USD-M wallet balance",
                                &balance.balance,
                                true,
                            )?,
                            available_balance: parse_balance_decimal(
                                "USD-M available balance",
                                &balance.available_balance,
                                true,
                            )?,
                            locked_balance: None,
                        })
                    })
                    .collect::<Result<Vec<_>, ExchangeError>>()?
            }
        };
        balances.sort_by(|left, right| left.asset.cmp(&right.asset));
        if balances
            .windows(2)
            .any(|pair| pair[0].asset == pair[1].asset)
        {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance account balance response contains duplicate assets",
            ));
        }
        Ok(balances)
    }

    /// Parses a stable server-order reference emitted by this protocol.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for malformed or unknown
    /// encoded identifiers.
    pub fn parse_server_order_ref(
        &self,
        value: &str,
    ) -> Result<BinanceServerOrderRef, ExchangeError> {
        if value.is_empty() || value.len() > MAX_SERVER_ORDER_REF_BYTES {
            return Err(ExchangeError::invalid(
                "Binance server order reference exceeds the bounded identifier shape",
            ));
        }
        let mut parts = value.split(':');
        let exchange = parts.next();
        let product = parts.next();
        let wire_symbol = parts.next();
        let order_id = parts.next();
        if parts.next().is_some()
            || exchange != Some(EXCHANGE)
            || wire_symbol.is_none()
            || product.is_none()
            || order_id.is_none()
        {
            return Err(ExchangeError::invalid(
                "Binance server order reference must use binance:{spot|usdm}:{wire_symbol}:{order_id}",
            ));
        }
        let market_type = match product.unwrap_or_default() {
            "spot" => MarketType::Spot,
            "usdm" => MarketType::Perpetual,
            _ => {
                return Err(ExchangeError::invalid(
                    "Binance server order reference uses an unknown product label",
                ));
            }
        };
        let wire_symbol = wire_symbol.unwrap_or_default();
        validate_binance_wire_symbol(wire_symbol)?;
        let order_id = order_id
            .unwrap_or_default()
            .parse::<u64>()
            .map_err(|_| ExchangeError::invalid("Binance server order id must be an integer"))?;
        if order_id == 0 {
            return Err(ExchangeError::invalid(
                "Binance server order id must not be zero",
            ));
        }
        let symbol = self
            .symbols
            .to_standard(EXCHANGE, wire_symbol, market_type)?
            .clone();
        Ok(BinanceServerOrderRef {
            symbol,
            market_type,
            wire_symbol: wire_symbol.to_owned(),
            order_id,
        })
    }

    /// Returns whether this protocol has an exact symbol mapping for one market.
    pub fn supports_symbol_market(&self, symbol: &Symbol, market_type: MarketType) -> bool {
        self.symbols.to_wire(EXCHANGE, symbol, market_type).is_ok()
    }

    /// Classifies one non-success Binance HTTP response without losing
    /// bounded retry/backoff metadata.
    pub fn remote_failure_from_response(response: &RemoteHttpResponse) -> ExchangeError {
        remote_failure_from_response(response)
    }

    pub(crate) fn is_clock_skew_error(error: &ExchangeError) -> bool {
        error
            .remote_failure_metadata()
            .and_then(|metadata| metadata.exchange_code.as_deref())
            == Some("-1021")
    }

    fn signed_request(
        &self,
        method: RemoteHttpMethod,
        product: BinanceProduct,
        path: &str,
        parameters: &[(&str, String)],
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let mut url = self.endpoints.rest_url(product, path)?;
        {
            let mut query = url.query_pairs_mut();
            for (name, value) in parameters {
                query.append_pair(name, value);
            }
        }
        let payload = url.query().unwrap_or_default();
        let signature = self.signer.sign(payload)?;
        validate_secret_text("Binance signature", &signature, MAX_SIGNATURE_BYTES)?;
        url.query_pairs_mut().append_pair("signature", &signature);
        Ok(RemoteHttpRequest::new(
            method,
            url,
            vec![("X-MBX-APIKEY".to_owned(), self.signer.api_key().to_owned())],
            Vec::new(),
        ))
    }
}

fn classify_mutating_response(
    result: Result<RemoteHttpResponse, ExchangeError>,
    operation: ExchangeOperation,
    client_order_id: Option<uuid::Uuid>,
    operation_key: ExchangeOperationKey,
) -> Result<RemoteHttpResponse, ExchangeError> {
    let response = match result {
        Ok(response) => response,
        Err(
            ref error @ ExchangeError::RemoteFailure {
                status,
                ref metadata,
                ..
            },
        ) if metadata.retry_after.is_some() || matches!(status, Some(418 | 429)) => {
            return Err(error.clone());
        }
        Err(_) => {
            return Err(ExchangeError::AmbiguousOutcome {
                operation,
                client_order_id,
                operation_key: Some(operation_key.clone()),
                reason: "authenticated request reached the transport but no outcome was returned; reconcile before retrying"
                    .to_owned(),
            });
        }
    };
    let exchange_error = serde_json::from_slice::<BinanceErrorWire>(response.body()).ok();
    if response.status() == 408
        || response.status() >= 500
        || exchange_error
            .as_ref()
            .is_some_and(|error| error.code == -1007)
    {
        return Err(ExchangeError::AmbiguousOutcome {
            operation,
            client_order_id,
            operation_key: Some(operation_key),
            reason: format!(
                "Binance returned an indeterminate response (HTTP {}); reconcile before retrying",
                response.status()
            ),
        });
    }
    if response.is_success() {
        return Ok(response);
    }
    Err(remote_failure_from_response(&response))
}

fn bounded_reason(reason: &str) -> String {
    const LIMIT: usize = 512;
    if reason.len() <= LIMIT {
        return reason.to_owned();
    }
    let mut end = LIMIT - 3;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &reason[..end])
}

fn product_for_market(market_type: MarketType) -> BinanceProduct {
    match market_type {
        MarketType::Spot => BinanceProduct::Spot,
        MarketType::Perpetual => BinanceProduct::UsdM,
    }
}

const fn market_for_product(product: BinanceProduct) -> MarketType {
    match product {
        BinanceProduct::Spot => MarketType::Spot,
        BinanceProduct::UsdM => MarketType::Perpetual,
    }
}

fn validate_order_shape(intent: &OrderIntent) -> Result<(), ExchangeError> {
    if intent.market_type == MarketType::Spot && intent.reduce_only {
        return Err(ExchangeError::invalid(
            "Binance Spot orders do not support reduceOnly",
        ));
    }
    match (intent.order_type, intent.price) {
        (OrderType::Market, None) => {
            if intent.time_in_force != TimeInForce::Gtc {
                return Err(ExchangeError::invalid(
                    "Binance market orders do not accept timeInForce",
                ));
            }
        }
        (OrderType::Limit, Some(_)) => {}
        (OrderType::Market, Some(_)) => {
            return Err(ExchangeError::invalid(
                "Binance market orders must not include price",
            ));
        }
        (OrderType::Limit, None) => {
            return Err(ExchangeError::invalid("Binance limit orders require price"));
        }
    }
    Ok(())
}

fn append_order_parameters(
    parameters: &mut Vec<(&'static str, String)>,
    product: BinanceProduct,
    intent: &OrderIntent,
) -> Result<(), ExchangeError> {
    match intent.order_type {
        OrderType::Market => {
            parameters.push(("type", "MARKET".to_owned()));
            parameters.push(("quantity", intent.quantity.to_string()));
        }
        OrderType::Limit => {
            let price = intent
                .price
                .ok_or_else(|| ExchangeError::invalid("Binance limit order requires price"))?;
            let (order_type, time_in_force) = match (product, intent.time_in_force) {
                (BinanceProduct::Spot, TimeInForce::PostOnly) => ("LIMIT_MAKER", None),
                (BinanceProduct::UsdM, TimeInForce::PostOnly) => ("LIMIT", Some("GTX")),
                (_, TimeInForce::Gtc) => ("LIMIT", Some("GTC")),
                (_, TimeInForce::Ioc) => ("LIMIT", Some("IOC")),
                (_, TimeInForce::Fok) => ("LIMIT", Some("FOK")),
            };
            parameters.push(("type", order_type.to_owned()));
            parameters.push(("quantity", intent.quantity.to_string()));
            parameters.push(("price", price.to_string()));
            if let Some(time_in_force) = time_in_force {
                parameters.push(("timeInForce", time_in_force.to_owned()));
            }
        }
    }
    if product == BinanceProduct::UsdM && intent.reduce_only {
        parameters.push(("reduceOnly", "true".to_owned()));
    }
    Ok(())
}

fn validate_binance_wire_symbol(symbol: &str) -> Result<(), ExchangeError> {
    if symbol.is_empty()
        || symbol.len() > 64
        || !symbol
            .chars()
            .all(|character| character.is_ascii_uppercase() || character.is_ascii_digit())
    {
        return Err(ExchangeError::invalid(
            "Binance wire symbol must contain 1..=64 uppercase ASCII letters or digits",
        ));
    }
    Ok(())
}

fn validate_binance_asset(asset: &str) -> Result<(), ExchangeError> {
    if asset.is_empty()
        || asset.len() > MAX_ASSET_BYTES
        || !asset
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance asset must contain 1..={MAX_ASSET_BYTES} uppercase ASCII letters or digits"
            ),
        ));
    }
    Ok(())
}

fn bounded_account_balance_count(count: usize) -> Result<(), ExchangeError> {
    if count > MAX_ACCOUNT_BALANCES {
        return Err(ExchangeError::resource_limit(
            "Binance account balances",
            MAX_ACCOUNT_BALANCES,
            count,
        ));
    }
    Ok(())
}

fn bounded_exchange_info_symbol_count(count: usize) -> Result<(), ExchangeError> {
    if count > MAX_EXCHANGE_INFO_SYMBOLS {
        return Err(ExchangeError::resource_limit(
            "Binance exchangeInfo symbols",
            MAX_EXCHANGE_INFO_SYMBOLS,
            count,
        ));
    }
    Ok(())
}

fn parse_balance_decimal(
    label: &str,
    value: &str,
    allow_negative: bool,
) -> Result<Decimal, ExchangeError> {
    let balance = value.parse::<Decimal>().map_err(|_| {
        ExchangeError::invalid_response(EXCHANGE, format!("{label} is not a decimal"))
    })?;
    if !allow_negative && balance.is_sign_negative() {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!("{label} must not be negative"),
        ));
    }
    Ok(balance)
}

fn validate_secret_text(label: &str, value: &str, max_bytes: usize) -> Result<(), ExchangeError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        return Err(ExchangeError::invalid(format!(
            "{label} must contain 1..={max_bytes} non-whitespace bytes"
        )));
    }
    Ok(())
}

fn parse_price(value: &str) -> Result<Price, ExchangeError> {
    value
        .parse()
        .map_err(|error: crypto_trading_domain::DomainError| {
            ExchangeError::invalid_response(EXCHANGE, error.to_string())
        })
}

fn parse_quantity(value: &str) -> Result<Quantity, ExchangeError> {
    value
        .parse()
        .map_err(|error: crypto_trading_domain::DomainError| {
            ExchangeError::invalid_response(EXCHANGE, error.to_string())
        })
}

fn parse_optional_price_bound(
    label: &str,
    value: Option<&str>,
) -> Result<Option<Price>, ExchangeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if decimal_is_zero(value) {
        return Ok(None);
    }
    parse_price(value).map(Some).map_err(|_| {
        ExchangeError::invalid_response(EXCHANGE, format!("Binance {label} is not a valid price"))
    })
}

fn parse_optional_quantity_bound(
    label: &str,
    value: Option<&str>,
) -> Result<Option<Quantity>, ExchangeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if decimal_is_zero(value) {
        return Ok(None);
    }
    parse_quantity(value).map(Some).map_err(|_| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!("Binance {label} is not a valid quantity"),
        )
    })
}

fn parse_optional_money_bound(
    label: &str,
    value: Option<&str>,
) -> Result<Option<Money>, ExchangeError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if decimal_is_zero(value) {
        return Ok(None);
    }
    let decimal = value.parse::<Decimal>().map_err(|_| {
        ExchangeError::invalid_response(EXCHANGE, format!("Binance {label} is not a valid decimal"))
    })?;
    if decimal.is_sign_negative() {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!("Binance {label} must not be negative"),
        ));
    }
    Ok(Some(Money::new(decimal)))
}

fn required_filter<'a>(
    wire_symbol: &str,
    filter_type: &str,
    filter: Option<&'a BinanceExchangeInfoFilterWire>,
) -> Result<&'a BinanceExchangeInfoFilterWire, ExchangeError> {
    filter.ok_or_else(|| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!("Binance exchangeInfo symbol {wire_symbol} is missing {filter_type}"),
        )
    })
}

struct SelectedExchangeInfoFilters<'a> {
    price: &'a BinanceExchangeInfoFilterWire,
    lot_size: &'a BinanceExchangeInfoFilterWire,
    market_lot_size: Option<&'a BinanceExchangeInfoFilterWire>,
    notional: &'a BinanceExchangeInfoFilterWire,
}

struct ParsedQuantityRules {
    step: Quantity,
    minimum: Quantity,
    maximum: Option<Quantity>,
    market_step: Option<Quantity>,
    market_minimum: Option<Quantity>,
    market_maximum: Option<Quantity>,
}

struct ParsedNotionalRules {
    minimum: Money,
    maximum: Option<Money>,
    apply_minimum_to_market: bool,
    apply_maximum_to_market: bool,
    average_price_minutes: Option<u32>,
}

fn select_exchange_info_filters<'a>(
    requested_wire_symbol: &str,
    selected: &'a BinanceExchangeInfoSymbolWire,
) -> Result<SelectedExchangeInfoFilters<'a>, ExchangeError> {
    let mut price_filter = None;
    let mut lot_size = None;
    let mut market_lot_size = None;
    let mut min_notional = None;
    let mut notional = None;
    for filter in &selected.filters {
        match filter.filter_type.as_str() {
            "PRICE_FILTER" => assign_filter_once(
                requested_wire_symbol,
                "PRICE_FILTER",
                &mut price_filter,
                filter,
            )?,
            "LOT_SIZE" => {
                assign_filter_once(requested_wire_symbol, "LOT_SIZE", &mut lot_size, filter)?;
            }
            "MARKET_LOT_SIZE" => assign_filter_once(
                requested_wire_symbol,
                "MARKET_LOT_SIZE",
                &mut market_lot_size,
                filter,
            )?,
            "MIN_NOTIONAL" => assign_filter_once(
                requested_wire_symbol,
                "MIN_NOTIONAL",
                &mut min_notional,
                filter,
            )?,
            "NOTIONAL" => {
                assign_filter_once(requested_wire_symbol, "NOTIONAL", &mut notional, filter)?;
            }
            unknown => {
                return Err(ExchangeError::invalid_response(
                    EXCHANGE,
                    format!(
                        "Binance exchangeInfo symbol {requested_wire_symbol} returned unsupported filter {unknown}",
                    ),
                ));
            }
        }
    }
    if min_notional.is_some() && notional.is_some() {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance exchangeInfo symbol {requested_wire_symbol} returned both MIN_NOTIONAL and NOTIONAL filters",
            ),
        ));
    }

    Ok(SelectedExchangeInfoFilters {
        price: required_filter(requested_wire_symbol, "PRICE_FILTER", price_filter)?,
        lot_size: required_filter(requested_wire_symbol, "LOT_SIZE", lot_size)?,
        market_lot_size,
        notional: required_filter(
            requested_wire_symbol,
            "MIN_NOTIONAL or NOTIONAL",
            min_notional.or(notional),
        )?,
    })
}

fn parse_price_filter(
    requested_wire_symbol: &str,
    price_filter: &BinanceExchangeInfoFilterWire,
) -> Result<(Price, Option<Price>, Option<Price>), ExchangeError> {
    let price_tick = parse_price(
        price_filter
            .tick_size
            .as_deref()
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    EXCHANGE,
                    format!(
                        "Binance exchangeInfo symbol {requested_wire_symbol} is missing PRICE_FILTER.tickSize",
                    ),
                )
            })?,
    )?;
    let min_price =
        parse_optional_price_bound("PRICE_FILTER.minPrice", price_filter.min_price.as_deref())?;
    let max_price =
        parse_optional_price_bound("PRICE_FILTER.maxPrice", price_filter.max_price.as_deref())?;
    Ok((price_tick, min_price, max_price))
}

fn parse_quantity_filters(
    requested_wire_symbol: &str,
    lot_size: &BinanceExchangeInfoFilterWire,
    market_lot_size: Option<&BinanceExchangeInfoFilterWire>,
) -> Result<ParsedQuantityRules, ExchangeError> {
    let quantity_step = parse_quantity(lot_size.step_size.as_deref().ok_or_else(|| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance exchangeInfo symbol {requested_wire_symbol} is missing LOT_SIZE.stepSize",
            ),
        )
    })?)?;
    let min_quantity = parse_quantity(lot_size.min_qty.as_deref().ok_or_else(|| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance exchangeInfo symbol {requested_wire_symbol} is missing LOT_SIZE.minQty",
            ),
        )
    })?)?;
    let max_quantity =
        parse_optional_quantity_bound("LOT_SIZE.maxQty", lot_size.max_qty.as_deref())?;

    let market_quantity_step = market_lot_size
        .map(|filter| {
            parse_optional_quantity_bound(
                "MARKET_LOT_SIZE.stepSize",
                filter.step_size.as_deref(),
            )?
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    EXCHANGE,
                    format!(
                        "Binance exchangeInfo symbol {requested_wire_symbol} has a disabled MARKET_LOT_SIZE.stepSize",
                    ),
                )
            })
        })
        .transpose()?;
    let market_min_quantity = market_lot_size
        .map(|filter| {
            parse_optional_quantity_bound(
                "MARKET_LOT_SIZE.minQty",
                filter.min_qty.as_deref(),
            )?
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    EXCHANGE,
                    format!(
                        "Binance exchangeInfo symbol {requested_wire_symbol} has a disabled MARKET_LOT_SIZE.minQty",
                    ),
                )
            })
        })
        .transpose()?;
    let market_max_quantity = market_lot_size
        .map(|filter| {
            parse_optional_quantity_bound("MARKET_LOT_SIZE.maxQty", filter.max_qty.as_deref())
        })
        .transpose()?
        .flatten();
    Ok(ParsedQuantityRules {
        step: quantity_step,
        minimum: min_quantity,
        maximum: max_quantity,
        market_step: market_quantity_step,
        market_minimum: market_min_quantity,
        market_maximum: market_max_quantity,
    })
}

fn parse_notional_filter(
    requested_wire_symbol: &str,
    market_type: MarketType,
    notional_filter: &BinanceExchangeInfoFilterWire,
) -> Result<ParsedNotionalRules, ExchangeError> {
    let min_notional_value = parse_optional_money_bound(
        "notional minimum",
        notional_filter.min_notional.as_deref(),
    )?
    .ok_or_else(|| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance exchangeInfo symbol {requested_wire_symbol} is missing a positive notional minimum",
            ),
        )
    })?;
    let max_notional =
        parse_optional_money_bound("notional maximum", notional_filter.max_notional.as_deref())?;
    let missing_field = |field: &str| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!("Binance exchangeInfo symbol {requested_wire_symbol} is missing {field}"),
        )
    };
    let (apply_min_notional_to_market, apply_max_notional_to_market, average_price_minutes) = match (
        market_type,
        notional_filter.filter_type.as_str(),
    ) {
        (MarketType::Spot, "NOTIONAL") => (
            notional_filter
                .apply_min_to_market
                .ok_or_else(|| missing_field("NOTIONAL.applyMinToMarket"))?,
            notional_filter
                .apply_max_to_market
                .ok_or_else(|| missing_field("NOTIONAL.applyMaxToMarket"))?,
            Some(
                notional_filter
                    .avg_price_mins
                    .ok_or_else(|| missing_field("NOTIONAL.avgPriceMins"))?,
            ),
        ),
        (MarketType::Spot, "MIN_NOTIONAL") => (
            notional_filter
                .apply_to_market
                .ok_or_else(|| missing_field("MIN_NOTIONAL.applyToMarket"))?,
            true,
            Some(
                notional_filter
                    .avg_price_mins
                    .ok_or_else(|| missing_field("MIN_NOTIONAL.avgPriceMins"))?,
            ),
        ),
        (MarketType::Perpetual, "MIN_NOTIONAL") => (true, true, None),
        (MarketType::Perpetual, "NOTIONAL") => {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "Binance USD-M exchangeInfo symbol {requested_wire_symbol} returned undocumented NOTIONAL semantics",
                ),
            ));
        }
        _ => {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "Binance exchangeInfo symbol {requested_wire_symbol} returned invalid notional metadata",
                ),
            ));
        }
    };
    Ok(ParsedNotionalRules {
        minimum: min_notional_value,
        maximum: max_notional,
        apply_minimum_to_market: apply_min_notional_to_market,
        apply_maximum_to_market: apply_max_notional_to_market,
        average_price_minutes,
    })
}

fn parse_exchange_info_rules(
    standard_symbol: Symbol,
    market_type: MarketType,
    requested_wire_symbol: &str,
    selected: &BinanceExchangeInfoSymbolWire,
) -> Result<InstrumentRules, ExchangeError> {
    let filters = select_exchange_info_filters(requested_wire_symbol, selected)?;
    let (price_tick, min_price, max_price) =
        parse_price_filter(requested_wire_symbol, filters.price)?;
    let quantity = parse_quantity_filters(
        requested_wire_symbol,
        filters.lot_size,
        filters.market_lot_size,
    )?;
    let notional = parse_notional_filter(requested_wire_symbol, market_type, filters.notional)?;

    InstrumentRules::with_options(
        EXCHANGE,
        standard_symbol,
        market_type,
        price_tick,
        quantity.step,
        quantity.minimum,
        InstrumentRuleOptions {
            min_notional: notional.minimum,
            min_price,
            max_price,
            max_quantity: quantity.maximum,
            market_quantity_step: quantity.market_step,
            market_min_quantity: quantity.market_minimum,
            market_max_quantity: quantity.market_maximum,
            max_notional: notional.maximum,
            apply_min_notional_to_market: notional.apply_minimum_to_market,
            apply_max_notional_to_market: notional.apply_maximum_to_market,
            market_notional_average_minutes: notional.average_price_minutes,
            requires_authoritative_market_notional_reference: notional.apply_minimum_to_market
                || (notional.maximum.is_some() && notional.apply_maximum_to_market),
        },
    )
    .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))
}

fn assign_filter_once<'a>(
    wire_symbol: &str,
    filter_type: &str,
    slot: &mut Option<&'a BinanceExchangeInfoFilterWire>,
    filter: &'a BinanceExchangeInfoFilterWire,
) -> Result<(), ExchangeError> {
    if slot.replace(filter).is_some() {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance exchangeInfo symbol {wire_symbol} returned duplicate {filter_type} filters",
            ),
        ));
    }
    Ok(())
}

fn validate_exchange_info_identity(
    product: BinanceProduct,
    selected: &BinanceExchangeInfoSymbolWire,
    standard_symbol: &Symbol,
    requested_wire_symbol: &str,
) -> Result<(), ExchangeError> {
    let base_asset = required_exchange_info_identity(
        requested_wire_symbol,
        "baseAsset",
        selected.base_asset.as_deref(),
    )?;
    let quote_asset = required_exchange_info_identity(
        requested_wire_symbol,
        "quoteAsset",
        selected.quote_asset.as_deref(),
    )?;
    let expected_wire_symbol = format!("{base_asset}{quote_asset}");
    let product_suffix = match product {
        BinanceProduct::Spot => "SPOT",
        BinanceProduct::UsdM => "PERP",
    };
    let expected_standard_symbol = format!("{base_asset}-{quote_asset}-{product_suffix}");
    let pair_matches =
        product != BinanceProduct::UsdM || selected.pair.as_deref() == Some(requested_wire_symbol);
    if expected_wire_symbol != requested_wire_symbol
        || standard_symbol.as_str() != expected_standard_symbol
        || !pair_matches
    {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!(
                "Binance exchangeInfo asset identity does not match {standard_symbol}/{requested_wire_symbol}",
            ),
        ));
    }
    Ok(())
}

fn required_exchange_info_identity<'a>(
    wire_symbol: &str,
    field: &str,
    value: Option<&'a str>,
) -> Result<&'a str, ExchangeError> {
    value.filter(|value| !value.is_empty()).ok_or_else(|| {
        ExchangeError::invalid_response(
            EXCHANGE,
            format!("Binance exchangeInfo symbol {wire_symbol} is missing {field}"),
        )
    })
}

fn parse_side(value: &str) -> Result<Side, ExchangeError> {
    match value {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!("unknown Binance order side {value:?}"),
        )),
    }
}

fn parse_order_shape(
    order_type: &str,
    price: &str,
    time_in_force: &str,
) -> Result<(OrderType, Option<Price>, TimeInForce), ExchangeError> {
    match order_type {
        "MARKET" => {
            if !price.is_empty() && !decimal_is_zero(price) {
                return Err(ExchangeError::invalid_response(
                    EXCHANGE,
                    "Binance market order response contains a positive limit price",
                ));
            }
            Ok((OrderType::Market, None, TimeInForce::Gtc))
        }
        "LIMIT" | "LIMIT_MAKER" => {
            let price = Some(parse_price(price)?);
            let time_in_force = if order_type == "LIMIT_MAKER" {
                TimeInForce::PostOnly
            } else {
                match time_in_force {
                    "GTC" => TimeInForce::Gtc,
                    "IOC" => TimeInForce::Ioc,
                    "FOK" => TimeInForce::Fok,
                    "GTX" => TimeInForce::PostOnly,
                    _ => {
                        return Err(ExchangeError::invalid_response(
                            EXCHANGE,
                            format!("unknown Binance limit timeInForce {time_in_force:?}"),
                        ));
                    }
                }
            };
            Ok((OrderType::Limit, price, time_in_force))
        }
        _ => Err(ExchangeError::invalid_response(
            EXCHANGE,
            format!("unknown Binance order type {order_type:?}"),
        )),
    }
}

fn parse_order_status(value: &str) -> Result<(OrderStatus, SubmissionDisposition), ExchangeError> {
    let result = match value {
        "PENDING_NEW" => (OrderStatus::Pending, SubmissionDisposition::Open),
        "NEW" => (OrderStatus::Open, SubmissionDisposition::Open),
        "PARTIALLY_FILLED" => (OrderStatus::PartiallyFilled, SubmissionDisposition::Open),
        "FILLED" => (OrderStatus::Filled, SubmissionDisposition::Filled),
        "CANCELED" | "CANCELLED" | "EXPIRED" | "EXPIRED_IN_MATCH" => {
            (OrderStatus::Cancelled, SubmissionDisposition::Cancelled)
        }
        "REJECTED" => (OrderStatus::Rejected, SubmissionDisposition::Cancelled),
        _ => {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!("unknown Binance order status {value:?}"),
            ));
        }
    };
    Ok(result)
}

fn parse_order_status_label(
    status: OrderStatus,
) -> Result<(OrderStatus, SubmissionDisposition), ExchangeError> {
    let label = match status {
        OrderStatus::Pending => "PENDING_NEW",
        OrderStatus::Open => "NEW",
        OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
        OrderStatus::Filled => "FILLED",
        OrderStatus::Cancelled => "CANCELED",
        OrderStatus::Rejected => "REJECTED",
    };
    parse_order_status(label)
}

fn parse_optional_millis(
    value: Option<i64>,
    fallback: DateTime<Utc>,
) -> Result<DateTime<Utc>, ExchangeError> {
    let Some(value) = value else {
        return Ok(fallback);
    };
    parse_required_millis(value)
}

fn decimal_is_zero(value: &str) -> bool {
    value
        .parse::<rust_decimal::Decimal>()
        .is_ok_and(|decimal| decimal.is_zero())
}

fn derive_average_fill_price(
    wire: &BinanceOrderWire,
    filled_quantity: Quantity,
) -> Result<Option<Price>, ExchangeError> {
    if let Some(value) = wire
        .avg_price
        .as_deref()
        .filter(|value| !decimal_is_zero(value))
    {
        return parse_price(value).map(Some);
    }
    if filled_quantity.as_decimal().is_zero() {
        return Ok(None);
    }
    let quote = wire
        .cummulative_quote_qty
        .as_deref()
        .or(wire.cum_quote.as_deref())
        .ok_or_else(|| {
            ExchangeError::invalid_response(
                EXCHANGE,
                "filled Binance order response is missing cumulative quote quantity",
            )
        })?;
    let quote = quote
        .parse::<rust_decimal::Decimal>()
        .map_err(|_| ExchangeError::invalid_response(EXCHANGE, "invalid cumulative quote value"))?;
    let average = quote
        .checked_div(filled_quantity.as_decimal())
        .ok_or_else(|| {
            ExchangeError::invalid_response(EXCHANGE, "unable to derive average fill price")
        })?;
    Price::new(average)
        .map(Some)
        .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))
}

impl BinanceTestnetProtocol {
    fn parse_wire_order(
        &self,
        product: BinanceProduct,
        wire: BinanceOrderWire,
        received_at: DateTime<Utc>,
        allow_foreign: bool,
    ) -> Result<ParsedBinanceOrder, ExchangeError> {
        if wire.order_id == 0 {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance response contains a zero server order id",
            ));
        }
        let market_type = market_for_product(product);
        let symbol = self
            .symbols
            .to_standard(EXCHANGE, &wire.symbol, market_type)?
            .clone();
        let quantity = parse_quantity(&wire.orig_qty)?;
        let filled_quantity = parse_quantity(&wire.executed_qty)?;
        if filled_quantity > quantity {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance executed quantity exceeds original quantity",
            ));
        }
        let side = parse_side(&wire.side)?;
        let (order_type, price, time_in_force) =
            parse_order_shape(&wire.order_type, &wire.price, &wire.time_in_force)?;
        if product == BinanceProduct::Spot && wire.reduce_only {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance Spot response unexpectedly contains reduceOnly",
            ));
        }
        let (status, _) = parse_order_status(&wire.status)?;
        let average_fill_price = derive_average_fill_price(&wire, filled_quantity)?;
        if !filled_quantity.as_decimal().is_zero()
            && average_fill_price.is_none()
            && status == OrderStatus::Filled
        {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                "filled Binance order response is missing a positive average price",
            ));
        }
        let created_at =
            parse_optional_millis(wire.transact_time.or(wire.update_time), received_at)?;
        let updated_at =
            parse_optional_millis(wire.update_time.or(wire.transact_time), received_at)?;
        let order_id = server_order_id(product, &wire.symbol, wire.order_id);
        // A cancel response carries the cancelled order's identity in
        // `origClientOrderId` and the cancel request's own generated id in
        // `clientOrderId`. Correlating on the latter would misread every
        // cancelled order as foreign.
        let reported_client_order_id = wire
            .orig_client_order_id
            .filter(|candidate| !candidate.trim().is_empty())
            .unwrap_or(wire.client_order_id);
        match uuid::Uuid::parse_str(&reported_client_order_id) {
            Ok(client_order_id) if !client_order_id.is_nil() => {
                Ok(ParsedBinanceOrder::Owned(Order {
                    id: order_id,
                    intent: OrderIntent {
                        client_order_id,
                        exchange: EXCHANGE.to_owned(),
                        symbol,
                        market_type,
                        side,
                        order_type,
                        quantity,
                        price,
                        reduce_only: wire.reduce_only,
                        time_in_force,
                    },
                    filled_quantity,
                    average_fill_price,
                    status,
                    created_at,
                    updated_at,
                }))
            }
            _ if allow_foreign => Ok(ParsedBinanceOrder::Foreign(ForeignOrder {
                id: order_id,
                client_order_id: (!reported_client_order_id.trim().is_empty())
                    .then_some(reported_client_order_id),
                exchange: EXCHANGE.to_owned(),
                symbol,
                market_type,
                side,
                order_type,
                quantity,
                price,
                reduce_only: wire.reduce_only,
                time_in_force,
                filled_quantity,
                average_fill_price,
                status,
                created_at,
                updated_at,
            })),
            _ => Err(ExchangeError::invalid_response(
                EXCHANGE,
                "Binance response client order id is not an owned UUID",
            )),
        }
    }

    fn parse_position_wire(
        &self,
        wire: &BinancePositionWire,
        received_at: DateTime<Utc>,
    ) -> Result<Option<Position>, ExchangeError> {
        let signed_quantity = wire.position_amt.parse::<Decimal>().map_err(|_| {
            ExchangeError::invalid_response(EXCHANGE, "invalid Binance position size")
        })?;
        if signed_quantity.is_zero() {
            return Ok(None);
        }
        let symbol = self
            .symbols
            .to_standard(EXCHANGE, &wire.symbol, MarketType::Perpetual)?
            .clone();
        let side = match wire.position_side.as_str() {
            "LONG" => PositionSide::Long,
            "SHORT" => PositionSide::Short,
            _ if signed_quantity.is_sign_positive() => PositionSide::Long,
            _ => PositionSide::Short,
        };
        let quantity = Quantity::new(signed_quantity.abs())
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let entry_price = parse_optional_decimal_price(&wire.entry_price)?;
        let mark_price = parse_optional_decimal_price(&wire.mark_price)?;
        let unrealized_pnl = unrealized_pnl(side, quantity.as_decimal(), entry_price, mark_price)?;
        Ok(Some(Position {
            exchange: EXCHANGE.to_owned(),
            symbol,
            market_type: MarketType::Perpetual,
            side,
            quantity,
            entry_price,
            mark_price,
            unrealized_pnl,
            updated_at: parse_optional_millis(wire.update_time, received_at)?,
        }))
    }
}

fn parse_required_millis(value: i64) -> Result<DateTime<Utc>, ExchangeError> {
    if value < 0 {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            "Binance timestamp must not be negative",
        ));
    }
    DateTime::from_timestamp_millis(value).ok_or_else(|| {
        ExchangeError::invalid_response(EXCHANGE, "Binance timestamp is outside UTC range")
    })
}

fn parse_execution_report_event(
    event: &serde_json::Value,
) -> Result<BinanceUserDataEvent, ExchangeError> {
    let event_time = required_event_time(event)?;
    let transaction_time = parse_required_millis(required_i64(event, "T")?)?;
    let symbol = Symbol::new(required_str(event, "s")?.to_owned())
        .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
    let client_order_id = required_str(event, "c")?.to_owned();
    let quantity = parse_quantity(required_str(event, "q")?)?;
    let price = parse_optional_decimal_price(required_str(event, "p")?)?;
    let cumulative_filled_quantity = parse_quantity(required_str(event, "z")?)?;
    let last_executed_quantity = optional_quantity(event, "l")?;
    let last_executed_price = optional_price(event, "L")?;
    Ok(BinanceUserDataEvent::ExecutionReport(
        BinanceExecutionReportEvent {
            event_time,
            transaction_time,
            symbol,
            client_order_id,
            execution_type: required_str(event, "x")?.to_owned(),
            order_status: required_str(event, "X")?.to_owned(),
            side: required_str(event, "S")?.to_owned(),
            order_type: required_str(event, "o")?.to_owned(),
            time_in_force: required_str(event, "f")?.to_owned(),
            order_id: required_u64(event, "i")?,
            execution_id: optional_u64(event, "I")?,
            quantity,
            price,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
        },
    ))
}

fn parse_account_update_event(
    event: &serde_json::Value,
) -> Result<BinanceUserDataEvent, ExchangeError> {
    let event_time = required_event_time(event)?;
    let account_update_time = parse_required_millis(required_i64(event, "u")?)?;
    let balances = event
        .get("B")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ExchangeError::invalid_response(
                EXCHANGE,
                "Binance account update payload is missing balances",
            )
        })?
        .iter()
        .map(|balance| {
            Ok(BinanceUserDataBalance {
                asset: required_str(balance, "a")?.to_owned(),
                free: parse_balance_decimal(
                    "Binance free balance",
                    required_str(balance, "f")?,
                    false,
                )?,
                locked: parse_balance_decimal(
                    "Binance locked balance",
                    required_str(balance, "l")?,
                    false,
                )?,
            })
        })
        .collect::<Result<Vec<_>, ExchangeError>>()?;
    let mut balances = balances;
    balances.sort_by(|left, right| left.asset.cmp(&right.asset));
    Ok(BinanceUserDataEvent::AccountUpdate(
        BinanceAccountUpdateEvent {
            event_time,
            account_update_time,
            balances,
        },
    ))
}

fn required_str<'a>(event: &'a serde_json::Value, field: &str) -> Result<&'a str, ExchangeError> {
    event
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            ExchangeError::invalid_response(EXCHANGE, format!("Binance payload is missing {field}"))
        })
}

fn required_i64(event: &serde_json::Value, field: &str) -> Result<i64, ExchangeError> {
    event
        .get(field)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            ExchangeError::invalid_response(EXCHANGE, format!("Binance payload is missing {field}"))
        })
}

fn required_u64(event: &serde_json::Value, field: &str) -> Result<u64, ExchangeError> {
    event
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            ExchangeError::invalid_response(EXCHANGE, format!("Binance payload is missing {field}"))
        })
}

fn optional_u64(event: &serde_json::Value, field: &str) -> Result<Option<u64>, ExchangeError> {
    match event.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            ExchangeError::invalid_response(EXCHANGE, format!("Binance field {field} is invalid"))
        }),
    }
}

fn required_event_time(event: &serde_json::Value) -> Result<DateTime<Utc>, ExchangeError> {
    parse_required_millis(required_i64(event, "E")?)
}

fn optional_event_time(event: &serde_json::Value) -> Result<Option<DateTime<Utc>>, ExchangeError> {
    let parsed = match event.get("E") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(parse_required_millis).transpose(),
    };
    parsed.map_err(|_| ExchangeError::invalid_response(EXCHANGE, "Binance field E is invalid"))
}

fn optional_quantity(
    event: &serde_json::Value,
    field: &str,
) -> Result<Option<Quantity>, ExchangeError> {
    match event.get(field).and_then(serde_json::Value::as_str) {
        None | Some("") => Ok(None),
        Some(value) => parse_quantity(value).map(Some),
    }
}

fn optional_price(event: &serde_json::Value, field: &str) -> Result<Option<Price>, ExchangeError> {
    match event.get(field).and_then(serde_json::Value::as_str) {
        None => Ok(None),
        Some(value) if value.is_empty() || decimal_is_zero(value) => Ok(None),
        Some(value) => parse_price(value).map(Some),
    }
}

fn parse_optional_decimal_price(value: &str) -> Result<Option<Price>, ExchangeError> {
    if value.is_empty() || decimal_is_zero(value) {
        return Ok(None);
    }
    parse_price(value).map(Some)
}

fn unrealized_pnl(
    side: PositionSide,
    quantity: Decimal,
    entry_price: Option<Price>,
    mark_price: Option<Price>,
) -> Result<Money, ExchangeError> {
    let Some((entry, mark)) = entry_price.zip(mark_price) else {
        return Ok(Money::default());
    };
    let pnl = match side {
        PositionSide::Long => mark
            .as_decimal()
            .checked_sub(entry.as_decimal())
            .and_then(|difference| difference.checked_mul(quantity)),
        PositionSide::Short => entry
            .as_decimal()
            .checked_sub(mark.as_decimal())
            .and_then(|difference| difference.checked_mul(quantity)),
        PositionSide::Flat => Some(Decimal::ZERO),
    }
    .ok_or_else(|| {
        ExchangeError::invalid_response(EXCHANGE, "Binance unrealized PnL overflowed")
    })?;
    Ok(Money::new(pnl))
}

fn server_order_id(product: BinanceProduct, wire_symbol: &str, order_id: u64) -> String {
    let product_label = match product {
        BinanceProduct::Spot => "spot",
        BinanceProduct::UsdM => "usdm",
    };
    format!("{EXCHANGE}:{product_label}:{wire_symbol}:{order_id}")
}

fn remote_failure_from_response(response: &RemoteHttpResponse) -> ExchangeError {
    let exchange_error = serde_json::from_slice::<BinanceErrorWire>(response.body()).ok();
    let reason = exchange_error.as_ref().map_or_else(
        || format!("Binance request failed with HTTP {}", response.status()),
        |error| bounded_reason(&format!("Binance error {}: {}", error.code, error.msg)),
    );
    let mut failure = ExchangeError::remote_failure(EXCHANGE, Some(response.status()), reason)
        .with_remote_metadata(response.remote_failure_metadata());
    if let Some(error) = exchange_error {
        failure = failure.with_exchange_code(error.code.to_string());
    }
    failure
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
