use std::{collections::HashMap, fmt, sync::Arc};

use crypto_trading_domain::{
    MarketType, Order, OrderIntent, OrderStatus, OrderType, Price, Quantity, Side, Symbol,
    TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ExchangeError, ExchangeOperation, ExchangeOperationKey, HyperliquidTestnetEndpoint,
    InstrumentRuleCatalog, RemoteHttpMethod, RemoteHttpRequest, RemoteHttpResponse,
    RemoteHttpTransport, SubmissionDisposition, TradingReceipt,
};

const EXCHANGE: &str = "hyperliquid";
const SPOT_ASSET_ID_OFFSET: u32 = 10_000;
const MAX_ASSETS: usize = 10_000;
const MAX_COIN_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AssetKey {
    symbol: Symbol,
    market_type: MarketType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AssetIdKey {
    asset_id: u32,
    market_type: MarketType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoinKey {
    coin: String,
    market_type: MarketType,
}

/// Explicit Hyperliquid metadata required to translate a domain instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidAsset {
    key: AssetKey,
    asset_id: u32,
    coin: String,
}

impl HyperliquidAsset {
    /// Builds one product-aware asset mapping.
    ///
    /// Hyperliquid perpetual IDs are metadata indices below 10000; Spot order
    /// IDs are `10000 + spotMeta.universe index`.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a product/ID mismatch or
    /// malformed coin identifier.
    pub fn new(
        symbol: Symbol,
        market_type: MarketType,
        asset_id: u32,
        coin: impl Into<String>,
    ) -> Result<Self, ExchangeError> {
        if (market_type == MarketType::Spot && asset_id < SPOT_ASSET_ID_OFFSET)
            || (market_type == MarketType::Perpetual && asset_id >= SPOT_ASSET_ID_OFFSET)
        {
            return Err(ExchangeError::invalid(format!(
                "Hyperliquid {market_type:?} asset id {asset_id} is outside its product range"
            )));
        }
        let coin = coin.into();
        if coin.is_empty()
            || coin.len() > MAX_COIN_BYTES
            || coin.chars().any(char::is_whitespace)
            || coin.chars().any(char::is_control)
        {
            return Err(ExchangeError::invalid(format!(
                "Hyperliquid coin must contain 1..={MAX_COIN_BYTES} non-whitespace bytes"
            )));
        }
        Ok(Self {
            key: AssetKey {
                symbol,
                market_type,
            },
            asset_id,
            coin,
        })
    }

    pub const fn symbol(&self) -> &Symbol {
        &self.key.symbol
    }

    pub const fn market_type(&self) -> MarketType {
        self.key.market_type
    }

    pub const fn asset_id(&self) -> u32 {
        self.asset_id
    }

    pub fn coin(&self) -> &str {
        &self.coin
    }
}

/// Bounded exact Hyperliquid asset metadata catalog.
#[derive(Debug, Clone, Default)]
pub struct HyperliquidAssetCatalog {
    by_standard: HashMap<AssetKey, HyperliquidAsset>,
    by_id: HashMap<AssetIdKey, AssetKey>,
    by_coin: HashMap<CoinKey, AssetKey>,
}

impl HyperliquidAssetCatalog {
    /// Builds a catalog and rejects ambiguity in any lookup direction.
    ///
    /// # Errors
    ///
    /// Returns an error for oversized, duplicate, ambiguous, or unreservable
    /// metadata.
    pub fn new(assets: Vec<HyperliquidAsset>) -> Result<Self, ExchangeError> {
        if assets.len() > MAX_ASSETS {
            return Err(ExchangeError::resource_limit(
                "Hyperliquid asset catalog",
                MAX_ASSETS,
                assets.len(),
            ));
        }
        let mut by_standard = HashMap::new();
        let mut by_id = HashMap::new();
        let mut by_coin = HashMap::new();
        by_standard.try_reserve(assets.len()).map_err(|_| {
            ExchangeError::unavailable("unable to reserve Hyperliquid asset catalog")
        })?;
        by_id.try_reserve(assets.len()).map_err(|_| {
            ExchangeError::unavailable("unable to reserve Hyperliquid asset-id catalog")
        })?;
        by_coin.try_reserve(assets.len()).map_err(|_| {
            ExchangeError::unavailable("unable to reserve Hyperliquid coin catalog")
        })?;

        for asset in assets {
            let key = asset.key.clone();
            if by_id
                .insert(
                    AssetIdKey {
                        asset_id: asset.asset_id,
                        market_type: key.market_type,
                    },
                    key.clone(),
                )
                .is_some()
            {
                return Err(ExchangeError::invalid(
                    "Hyperliquid asset catalog contains an ambiguous product asset id",
                ));
            }
            if by_coin
                .insert(
                    CoinKey {
                        coin: asset.coin.clone(),
                        market_type: key.market_type,
                    },
                    key.clone(),
                )
                .is_some()
            {
                return Err(ExchangeError::invalid(
                    "Hyperliquid asset catalog contains an ambiguous product coin",
                ));
            }
            if by_standard.insert(key, asset).is_some() {
                return Err(ExchangeError::invalid(
                    "Hyperliquid asset catalog contains a duplicate domain instrument",
                ));
            }
        }
        Ok(Self {
            by_standard,
            by_id,
            by_coin,
        })
    }

    pub fn len(&self) -> usize {
        self.by_standard.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_standard.is_empty()
    }

    /// Resolves an exact domain instrument.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when metadata is missing.
    pub fn resolve(
        &self,
        symbol: &Symbol,
        market_type: MarketType,
    ) -> Result<&HyperliquidAsset, ExchangeError> {
        self.by_standard
            .get(&AssetKey {
                symbol: symbol.clone(),
                market_type,
            })
            .ok_or_else(|| {
                ExchangeError::invalid(format!(
                    "missing Hyperliquid asset metadata for {symbol}/{market_type:?}"
                ))
            })
    }

    /// Resolves a response coin through exact product metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for unknown metadata.
    pub fn resolve_coin(
        &self,
        coin: &str,
        market_type: MarketType,
    ) -> Result<&HyperliquidAsset, ExchangeError> {
        let key = self
            .by_coin
            .get(&CoinKey {
                coin: coin.to_owned(),
                market_type,
            })
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    EXCHANGE,
                    format!("unknown Hyperliquid {market_type:?} coin {coin:?}"),
                )
            })?;
        self.by_standard.get(key).ok_or_else(|| {
            ExchangeError::invalid_response(EXCHANGE, "Hyperliquid coin catalog is inconsistent")
        })
    }

    /// Resolves a response asset ID through exact product metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for unknown metadata.
    pub fn resolve_id(
        &self,
        asset_id: u32,
        market_type: MarketType,
    ) -> Result<&HyperliquidAsset, ExchangeError> {
        let key = self
            .by_id
            .get(&AssetIdKey {
                asset_id,
                market_type,
            })
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    EXCHANGE,
                    format!("unknown Hyperliquid {market_type:?} asset id {asset_id}"),
                )
            })?;
        self.by_standard.get(key).ok_or_else(|| {
            ExchangeError::invalid_response(
                EXCHANGE,
                "Hyperliquid asset-id catalog is inconsistent",
            )
        })
    }
}

/// One exact exchange action presented to a signing implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum HyperliquidAction {
    #[serde(rename = "order")]
    Order {
        orders: Vec<HyperliquidWireOrder>,
        grouping: String,
    },
    #[serde(rename = "cancel")]
    Cancel { cancels: Vec<HyperliquidWireCancel> },
}

/// Hyperliquid's compact order wire object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HyperliquidWireOrder {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "s")]
    size: String,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: HyperliquidWireOrderType,
    #[serde(rename = "c")]
    client_order_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct HyperliquidWireOrderType {
    limit: HyperliquidWireLimit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct HyperliquidWireLimit {
    tif: HyperliquidTimeInForce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
enum HyperliquidTimeInForce {
    Gtc,
    Ioc,
    Alo,
}

/// Hyperliquid's compact server-order cancellation object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HyperliquidWireCancel {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "o")]
    order_id: u64,
}

/// Validated ECDSA signature fields returned by a signing implementation.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct HyperliquidSignature {
    r: String,
    s: String,
    v: u8,
}

impl fmt::Debug for HyperliquidSignature {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HyperliquidSignature")
            .field("r", &"[REDACTED]")
            .field("s", &"[REDACTED]")
            .field("v", &self.v)
            .finish()
    }
}

impl HyperliquidSignature {
    /// Builds validated signature fields.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] unless `r` and `s` are
    /// 32-byte lowercase/uppercase hex values prefixed with `0x`, and `v` is 27
    /// or 28.
    pub fn new(r: impl Into<String>, s: impl Into<String>, v: u8) -> Result<Self, ExchangeError> {
        let r = r.into();
        let s = s.into();
        if !is_hex_word(&r) || !is_hex_word(&s) || !matches!(v, 27 | 28) {
            return Err(ExchangeError::invalid(
                "Hyperliquid signature requires 32-byte r/s hex words and v 27 or 28",
            ));
        }
        Ok(Self { r, s, v })
    }
}

/// Credential-backed Hyperliquid action-signing seam.
pub trait HyperliquidRequestSigner: Send + Sync {
    /// Address whose account state should be queried. For API wallets this is
    /// the actual master/subaccount address, not the API wallet address.
    fn account_address(&self) -> &str;

    /// Signs one typed action using Hyperliquid's exact action-hash and EIP-712
    /// rules.
    ///
    /// # Errors
    ///
    /// Returns an exchange error when signing is unavailable.
    fn sign(
        &self,
        action: &HyperliquidAction,
        nonce: u64,
        vault_address: Option<&str>,
    ) -> Result<HyperliquidSignature, ExchangeError>;
}

/// Deterministic Hyperliquid testnet request protocol.
pub struct HyperliquidTestnetProtocol {
    endpoint: HyperliquidTestnetEndpoint,
    assets: HyperliquidAssetCatalog,
    rules: InstrumentRuleCatalog,
    signer: Arc<dyn HyperliquidRequestSigner>,
    vault_address: Option<String>,
}

impl fmt::Debug for HyperliquidTestnetProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HyperliquidTestnetProtocol")
            .field("endpoint", &self.endpoint)
            .field("asset_count", &self.assets.len())
            .field("rule_count", &self.rules.len())
            .field("uses_vault", &self.vault_address.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidExchangeEnvelope<'a> {
    action: &'a HyperliquidAction,
    nonce: u64,
    signature: &'a HyperliquidSignature,
    vault_address: Option<&'a str>,
}

#[derive(Serialize)]
struct HyperliquidInfoRequest<'a> {
    #[serde(rename = "type")]
    request_type: &'a str,
    user: &'a str,
}

#[derive(Debug, Deserialize)]
struct HyperliquidOrderResponse {
    #[serde(rename = "type")]
    response_type: String,
    data: HyperliquidOrderResponseData,
}

#[derive(Debug, Deserialize)]
struct HyperliquidOrderResponseData {
    statuses: Vec<HyperliquidOrderStatusWire>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum HyperliquidOrderStatusWire {
    Resting { resting: HyperliquidRestingWire },
    Filled { filled: HyperliquidFilledWire },
    Error { error: String },
}

#[derive(Debug, Deserialize)]
struct HyperliquidRestingWire {
    oid: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HyperliquidFilledWire {
    total_sz: String,
    avg_px: String,
    oid: u64,
}

struct ParsedHyperliquidOrder {
    order_id: u64,
    filled_quantity: Quantity,
    average_fill_price: Option<Price>,
    status: OrderStatus,
    disposition: SubmissionDisposition,
}

impl HyperliquidTestnetProtocol {
    /// Builds an authenticated, testnet-only Hyperliquid protocol.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for malformed account or vault
    /// addresses.
    pub fn authenticated<S>(
        endpoint: HyperliquidTestnetEndpoint,
        assets: HyperliquidAssetCatalog,
        rules: InstrumentRuleCatalog,
        signer: Arc<S>,
        vault_address: Option<String>,
    ) -> Result<Self, ExchangeError>
    where
        S: HyperliquidRequestSigner + 'static,
    {
        validate_address("Hyperliquid account address", signer.account_address())?;
        if let Some(address) = vault_address.as_deref() {
            validate_address("Hyperliquid vault address", address)?;
        }
        Ok(Self {
            endpoint,
            assets,
            rules,
            signer,
            vault_address,
        })
    }

    /// Builds a signed limit-order request for Spot or perpetual testnet.
    ///
    /// Hyperliquid market orders require a caller-defined slippage policy and
    /// are therefore rejected here rather than converted silently.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid semantics, missing exact metadata/rules, or
    /// signing/serialization failure.
    pub fn build_order_request(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Price>,
        nonce: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        if !intent.exchange.eq_ignore_ascii_case(EXCHANGE) {
            return Err(ExchangeError::invalid(format!(
                "Hyperliquid protocol cannot submit an order for exchange {}",
                intent.exchange
            )));
        }
        if intent.client_order_id.is_nil() {
            return Err(ExchangeError::invalid(
                "Hyperliquid client order id must not be nil",
            ));
        }
        if intent.market_type == MarketType::Spot && intent.reduce_only {
            return Err(ExchangeError::invalid(
                "Hyperliquid Spot orders do not support reduce-only semantics",
            ));
        }
        if intent.order_type != OrderType::Limit {
            return Err(ExchangeError::invalid(
                "Hyperliquid market orders require an explicit slippage policy",
            ));
        }
        let price = intent
            .price
            .ok_or_else(|| ExchangeError::invalid("Hyperliquid limit order requires price"))?;
        let time_in_force = match intent.time_in_force {
            TimeInForce::Gtc => HyperliquidTimeInForce::Gtc,
            TimeInForce::Ioc => HyperliquidTimeInForce::Ioc,
            TimeInForce::PostOnly => HyperliquidTimeInForce::Alo,
            TimeInForce::Fok => {
                return Err(ExchangeError::invalid(
                    "Hyperliquid does not support fill-or-kill limit orders",
                ));
            }
        };
        self.rules.validate_order(intent, reference_price)?;
        let asset = self.assets.resolve(&intent.symbol, intent.market_type)?;
        let action = HyperliquidAction::Order {
            orders: vec![HyperliquidWireOrder {
                asset: asset.asset_id,
                is_buy: intent.side == Side::Buy,
                price: price.to_string(),
                size: intent.quantity.to_string(),
                reduce_only: intent.reduce_only,
                order_type: HyperliquidWireOrderType {
                    limit: HyperliquidWireLimit { tif: time_in_force },
                },
                client_order_id: format!("0x{}", intent.client_order_id.simple()),
            }],
            grouping: "na".to_owned(),
        };
        self.exchange_request(&action, nonce)
    }

    /// Builds and dispatches one authenticated action with conservative unknown
    /// outcome semantics.
    ///
    /// # Errors
    ///
    /// Returns request-validation/signing errors before dispatch. Transport
    /// failures, HTTP 408, and HTTP 5xx after dispatch become
    /// [`ExchangeError::AmbiguousOutcome`].
    pub async fn dispatch_order<T>(
        &self,
        transport: &T,
        intent: &OrderIntent,
        reference_price: Option<Price>,
        nonce: u64,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        T: RemoteHttpTransport + ?Sized,
    {
        let request = self.build_order_request(intent, reference_price, nonce)?;
        let result = transport.send(request).await;
        classify_hyperliquid_mutation(
            result,
            ExchangeOperation::SubmitOrder,
            Some(intent.client_order_id),
            ExchangeOperationKey::ClientOrderId {
                client_order_id: intent.client_order_id,
            },
        )
    }

    /// Builds an authenticated cancellation for one known server order.
    ///
    /// # Errors
    ///
    /// Returns an error for missing metadata, a zero order ID, signing, or
    /// serialization failure.
    pub fn build_cancel_request(
        &self,
        symbol: &Symbol,
        market_type: MarketType,
        order_id: u64,
        nonce: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        if order_id == 0 {
            return Err(ExchangeError::invalid(
                "Hyperliquid server order id must not be zero",
            ));
        }
        let asset = self.assets.resolve(symbol, market_type)?;
        let action = HyperliquidAction::Cancel {
            cancels: vec![HyperliquidWireCancel {
                asset: asset.asset_id,
                order_id,
            }],
        };
        self.exchange_request(&action, nonce)
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
        symbol: &Symbol,
        market_type: MarketType,
        order_id: u64,
        nonce: u64,
    ) -> Result<RemoteHttpResponse, ExchangeError>
    where
        T: RemoteHttpTransport + ?Sized,
    {
        let request = self.build_cancel_request(symbol, market_type, order_id, nonce)?;
        let market_label = match market_type {
            MarketType::Spot => "spot",
            MarketType::Perpetual => "perpetual",
        };
        classify_hyperliquid_mutation(
            transport.send(request).await,
            ExchangeOperation::CancelOrder,
            None,
            ExchangeOperationKey::OrderId(format!("{EXCHANGE}:{market_label}:{symbol}:{order_id}")),
        )
    }

    /// Builds the account open-orders request.
    ///
    /// # Errors
    ///
    /// Returns an error only if the fixed endpoint or JSON serialization fails.
    pub fn build_open_orders_request(&self) -> Result<RemoteHttpRequest, ExchangeError> {
        self.info_request("openOrders")
    }

    /// Builds the perpetual clearinghouse-state request.
    ///
    /// # Errors
    ///
    /// Returns an error only if the fixed endpoint or JSON serialization fails.
    pub fn build_perpetual_state_request(&self) -> Result<RemoteHttpRequest, ExchangeError> {
        self.info_request("clearinghouseState")
    }

    /// Builds the Spot clearinghouse-state request.
    ///
    /// # Errors
    ///
    /// Returns an error only if the fixed endpoint or JSON serialization fails.
    pub fn build_spot_state_request(&self) -> Result<RemoteHttpRequest, ExchangeError> {
        self.info_request("spotClearinghouseState")
    }

    /// Parses one order-action response for a previously submitted intent.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::Rejected`] for an explicit exchange error and
    /// [`ExchangeError::InvalidResponse`] for malformed or inconsistent data.
    pub fn parse_order_response(
        &self,
        intent: &OrderIntent,
        payload: &[u8],
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<TradingReceipt, ExchangeError> {
        if !intent.exchange.eq_ignore_ascii_case(EXCHANGE) {
            return Err(ExchangeError::invalid(
                "Hyperliquid response intent belongs to another exchange",
            ));
        }
        let asset = self.assets.resolve(&intent.symbol, intent.market_type)?;
        let parsed = parse_hyperliquid_order(payload, intent.quantity)?;
        let market_label = match intent.market_type {
            MarketType::Spot => "spot",
            MarketType::Perpetual => "perpetual",
        };
        Ok(TradingReceipt::Submitted {
            order: Order {
                id: format!(
                    "{EXCHANGE}:{market_label}:{}:{}",
                    asset.asset_id, parsed.order_id
                ),
                intent: intent.clone(),
                filled_quantity: parsed.filled_quantity,
                average_fill_price: parsed.average_fill_price,
                status: parsed.status,
                created_at: received_at,
                updated_at: received_at,
            },
            disposition: parsed.disposition,
        })
    }

    fn exchange_request(
        &self,
        action: &HyperliquidAction,
        nonce: u64,
    ) -> Result<RemoteHttpRequest, ExchangeError> {
        let signature = self
            .signer
            .sign(action, nonce, self.vault_address.as_deref())?;
        let body = serde_json::to_vec(&HyperliquidExchangeEnvelope {
            action,
            nonce,
            signature: &signature,
            vault_address: self.vault_address.as_deref(),
        })
        .map_err(|error| ExchangeError::invalid(format!("invalid Hyperliquid request: {error}")))?;
        Ok(RemoteHttpRequest::new(
            RemoteHttpMethod::Post,
            self.endpoint.rest_url("/exchange")?,
            vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body,
        ))
    }

    fn info_request(&self, request_type: &str) -> Result<RemoteHttpRequest, ExchangeError> {
        let body = serde_json::to_vec(&HyperliquidInfoRequest {
            request_type,
            user: self.signer.account_address(),
        })
        .map_err(|error| {
            ExchangeError::invalid(format!("invalid Hyperliquid info request: {error}"))
        })?;
        Ok(RemoteHttpRequest::new(
            RemoteHttpMethod::Post,
            self.endpoint.rest_url("/info")?,
            vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body,
        ))
    }
}

fn parse_hyperliquid_order(
    payload: &[u8],
    submitted_quantity: Quantity,
) -> Result<ParsedHyperliquidOrder, ExchangeError> {
    let envelope: Value = serde_json::from_slice(payload)
        .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
    let response_value = successful_hyperliquid_response(&envelope)?;
    let response: HyperliquidOrderResponse = serde_json::from_value(response_value)
        .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
    if response.response_type != "order" || response.data.statuses.len() != 1 {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            "Hyperliquid single order response must contain exactly one order status",
        ));
    }
    let status = response.data.statuses.into_iter().next().ok_or_else(|| {
        ExchangeError::invalid_response(EXCHANGE, "Hyperliquid order response status disappeared")
    })?;
    match status {
        HyperliquidOrderStatusWire::Resting { resting } => parse_hyperliquid_resting(&resting),
        HyperliquidOrderStatusWire::Filled { filled } => {
            parse_hyperliquid_filled(&filled, submitted_quantity)
        }
        HyperliquidOrderStatusWire::Error { error } => {
            Err(ExchangeError::rejected(bounded_hyperliquid_reason(&error)))
        }
    }
}

fn successful_hyperliquid_response(envelope: &Value) -> Result<Value, ExchangeError> {
    match envelope.get("status").and_then(Value::as_str) {
        Some("ok") => envelope.get("response").cloned().ok_or_else(|| {
            ExchangeError::invalid_response(
                EXCHANGE,
                "Hyperliquid order response is missing response data",
            )
        }),
        Some("err") => {
            let reason = envelope
                .get("response")
                .and_then(Value::as_str)
                .unwrap_or("Hyperliquid rejected the order");
            Err(ExchangeError::rejected(bounded_hyperliquid_reason(reason)))
        }
        _ => Err(ExchangeError::invalid_response(
            EXCHANGE,
            "Hyperliquid order response has an unknown status",
        )),
    }
}

fn parse_hyperliquid_resting(
    resting: &HyperliquidRestingWire,
) -> Result<ParsedHyperliquidOrder, ExchangeError> {
    if resting.oid == 0 {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            "Hyperliquid resting order id must not be zero",
        ));
    }
    Ok(ParsedHyperliquidOrder {
        order_id: resting.oid,
        filled_quantity: Quantity::default(),
        average_fill_price: None,
        status: OrderStatus::Open,
        disposition: SubmissionDisposition::Open,
    })
}

fn parse_hyperliquid_filled(
    filled: &HyperliquidFilledWire,
    submitted_quantity: Quantity,
) -> Result<ParsedHyperliquidOrder, ExchangeError> {
    if filled.oid == 0 {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            "Hyperliquid filled order id must not be zero",
        ));
    }
    let filled_quantity: Quantity =
        filled
            .total_sz
            .parse()
            .map_err(|error: crypto_trading_domain::DomainError| {
                ExchangeError::invalid_response(EXCHANGE, error.to_string())
            })?;
    if filled_quantity > submitted_quantity {
        return Err(ExchangeError::invalid_response(
            EXCHANGE,
            "Hyperliquid filled size exceeds submitted size",
        ));
    }
    let average_fill_price: Price =
        filled
            .avg_px
            .parse()
            .map_err(|error: crypto_trading_domain::DomainError| {
                ExchangeError::invalid_response(EXCHANGE, error.to_string())
            })?;
    Ok(ParsedHyperliquidOrder {
        order_id: filled.oid,
        filled_quantity,
        average_fill_price: Some(average_fill_price),
        status: OrderStatus::Filled,
        disposition: SubmissionDisposition::Filled,
    })
}

fn classify_hyperliquid_mutation(
    result: Result<RemoteHttpResponse, ExchangeError>,
    operation: ExchangeOperation,
    client_order_id: Option<uuid::Uuid>,
    operation_key: ExchangeOperationKey,
) -> Result<RemoteHttpResponse, ExchangeError> {
    let response = result.map_err(|_| ExchangeError::AmbiguousOutcome {
        operation,
        client_order_id,
        operation_key: Some(operation_key.clone()),
        reason: "authenticated request reached the transport but no outcome was returned; reconcile before retrying"
            .to_owned(),
    })?;
    if response.status() == 408 || response.status() >= 500 {
        return Err(ExchangeError::AmbiguousOutcome {
            operation,
            client_order_id,
            operation_key: Some(operation_key),
            reason: format!(
                "Hyperliquid returned an indeterminate response (HTTP {}); reconcile before retrying",
                response.status()
            ),
        });
    }
    if response.is_success() {
        return Ok(response);
    }
    Err(ExchangeError::remote_failure(
        EXCHANGE,
        Some(response.status()),
        format!("Hyperliquid request failed with HTTP {}", response.status()),
    ))
}

fn validate_address(label: &str, address: &str) -> Result<(), ExchangeError> {
    if address.len() != 42
        || !address.starts_with("0x")
        || !address[2..]
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(ExchangeError::invalid(format!(
            "{label} must be a 20-byte 0x-prefixed hex value"
        )));
    }
    Ok(())
}

fn is_hex_word(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..]
            .chars()
            .all(|character| character.is_ascii_hexdigit())
}

fn bounded_hyperliquid_reason(reason: &str) -> String {
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
