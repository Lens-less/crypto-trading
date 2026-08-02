//! Read-only Hyperliquid public market-data adapter.
//!
//! Wire-shape source: the documented Hyperliquid info endpoint
//! (`POST https://api.hyperliquid.xyz/info` with body
//! `{"type":"metaAndAssetCtxs"}`) returns a two-element JSON array: element
//! zero is the perpetuals metadata (`universe` entries with `name`), and
//! element one is a parallel array of asset contexts whose decimal-string
//! fields include `funding` (the current hourly funding rate as a decimal
//! fraction), `oraclePx`, `markPx`, `midPx`, and `impactPxs`
//! (`[impactBidPx, impactAskPx]`). This module was written offline against
//! that documented shape; fixture-driven contract tests pin the accepted
//! subset, and any deviation fails closed as an invalid response instead of
//! being guessed around.

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeOperation,
    ExchangeStatus, ExchangeSymbol, ExchangeSymbolCatalog, HyperliquidPublicEndpoint,
    MarketSubscription, ReconcileReceipt, ReconcileScope, SubscriptionReceipt, TradingCommand,
    TradingReceipt,
};

const EXCHANGE: &str = "hyperliquid";
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;
const MAX_ERROR_REASON_TEXT_BYTES: usize = 256;
const MAX_ASSET_CONTEXTS: usize = 10_000;
const MAX_COIN_BYTES: usize = 32;

/// Hard upper bound for one explicit USDT-quoted coin catalog.
pub const MAX_HYPERLIQUID_PUBLIC_CATALOG_COINS: usize = 1_024;

/// Absolute plausibility guard for one hourly funding rate (100% per hour).
///
/// Real Hyperliquid hourly funding stays far below this; anything outside the
/// guard is treated as a malformed response rather than a market fact.
const MAX_ABS_HOURLY_FUNDING: Decimal = Decimal::ONE;

/// One validated hourly funding-rate fraction (`0.0001 == 0.01%` per hour).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct HyperliquidFundingRate(Decimal);

impl HyperliquidFundingRate {
    /// Validates one hourly funding-rate fraction against the plausibility
    /// guard.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] when the magnitude exceeds
    /// the hard bound.
    pub fn new(hourly_rate: Decimal) -> Result<Self, ExchangeError> {
        if hourly_rate.abs() > MAX_ABS_HOURLY_FUNDING {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!("hourly funding rate {hourly_rate} is outside the plausibility bound"),
            ));
        }
        Ok(Self(hourly_rate))
    }

    /// Returns the hourly funding-rate fraction.
    #[must_use]
    pub const fn as_decimal(self) -> Decimal {
        self.0
    }
}

/// One coherent public observation: a top-of-book proxy plus optional funding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidPublicObservation {
    /// Perpetual snapshot with `bid`/`ask` taken from the impact prices and
    /// `last` carrying the mid price when the venue publishes one.
    pub snapshot: MarketSnapshot,
    /// Venue event time, absent for the documented REST `metaAndAssetCtxs`
    /// response shape.
    pub event_time: Option<DateTime<Utc>>,
    /// Hourly funding rate, absent when the venue omits it for the coin.
    pub funding: Option<HyperliquidFundingRate>,
}

/// Read-only Hyperliquid public market-data adapter.
///
/// The adapter posts only the credential-free `metaAndAssetCtxs` info request
/// and never attaches keys, signatures, or account addresses.
#[derive(Debug, Clone)]
pub struct HyperliquidPublicExchange {
    client: reqwest::Client,
    info_url: reqwest::Url,
}

#[derive(Debug, Serialize)]
struct InfoRequestWire {
    #[serde(rename = "type")]
    request_type: &'static str,
}

#[derive(Debug, Deserialize)]
struct MetaWire {
    universe: Vec<UniverseEntryWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UniverseEntryWire {
    name: String,
    #[serde(default)]
    is_delisted: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetCtxWire {
    #[serde(default)]
    funding: Option<String>,
    #[serde(default)]
    mid_px: Option<String>,
    #[serde(default)]
    impact_pxs: Option<Vec<String>>,
}

impl HyperliquidPublicExchange {
    /// Creates an adapter against the official Hyperliquid mainnet public-info
    /// endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client or the fixed info route cannot be
    /// constructed.
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_endpoint(&HyperliquidPublicEndpoint::official())
    }

    /// Creates an adapter against one whitelist-validated endpoint (official
    /// mainnet or explicit literal loopback).
    ///
    /// # Errors
    ///
    /// Returns an error for HTTP client construction failure or an unsafe
    /// info route.
    pub fn with_endpoint(endpoint: &HyperliquidPublicEndpoint) -> Result<Self, ExchangeError> {
        let info_url = endpoint.rest_url("/info")?;
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .user_agent("crypto-trading/0.1 public-market-data")
            .build()
            .map_err(|error| ExchangeError::unavailable(error.to_string()))?;
        Ok(Self { client, info_url })
    }

    /// Fetches the current asset context for one exact perpetual coin.
    ///
    /// One credential-free `metaAndAssetCtxs` request covers every listed
    /// perpetual; this method selects exactly the requested coin and fails
    /// closed on any ambiguity.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::RemoteFailure`] for transport or non-success
    /// HTTP responses, [`ExchangeError::ResourceLimit`] for oversized bodies,
    /// and [`ExchangeError::InvalidResponse`] for malformed, ambiguous, or
    /// out-of-bound response data.
    pub async fn fetch_observation(
        &self,
        coin: &str,
    ) -> Result<HyperliquidPublicObservation, ExchangeError> {
        self.fetch_observation_at(coin, Utc::now()).await
    }

    /// Fetches one asset context using a caller-supplied local receive time.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::RemoteFailure`] for transport or non-success
    /// HTTP responses, [`ExchangeError::ResourceLimit`] for oversized bodies,
    /// and [`ExchangeError::InvalidResponse`] for malformed, ambiguous, or
    /// out-of-bound response data.
    pub async fn fetch_observation_at(
        &self,
        coin: &str,
        received_at: DateTime<Utc>,
    ) -> Result<HyperliquidPublicObservation, ExchangeError> {
        let body = serde_json::to_vec(&InfoRequestWire {
            request_type: "metaAndAssetCtxs",
        })
        .map_err(|error| ExchangeError::invalid(error.to_string()))?;
        let response = self
            .client
            .post(self.info_url.clone())
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| ExchangeError::remote_failure(EXCHANGE, None, error.to_string()))?;
        let status = response.status();
        let payload = Self::read_response_body(response).await?;
        if !status.is_success() {
            return Err(Self::map_remote_failure(status, &payload));
        }
        Self::parse_meta_and_asset_ctxs(&payload, coin, received_at)
    }

    /// Parses one documented `metaAndAssetCtxs` payload for one exact coin.
    ///
    /// Hyperliquid does not include an event timestamp in this response, so
    /// the caller supplies the local receive time used by the domain snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] above the context bound and
    /// [`ExchangeError::InvalidResponse`] for malformed JSON, a
    /// universe/context arity mismatch, an unknown/duplicated/delisted coin,
    /// missing or crossed impact prices, or an implausible funding value.
    pub fn parse_meta_and_asset_ctxs(
        payload: &[u8],
        coin: &str,
        received_at: DateTime<Utc>,
    ) -> Result<HyperliquidPublicObservation, ExchangeError> {
        let (meta, contexts): (MetaWire, Vec<AssetCtxWire>) = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        if meta.universe.len() > MAX_ASSET_CONTEXTS {
            return Err(ExchangeError::resource_limit(
                "Hyperliquid asset contexts",
                MAX_ASSET_CONTEXTS,
                meta.universe.len(),
            ));
        }
        if meta.universe.len() != contexts.len() {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "universe has {} entries but {} asset contexts were returned",
                    meta.universe.len(),
                    contexts.len()
                ),
            ));
        }
        let mut selected: Option<(&UniverseEntryWire, &AssetCtxWire)> = None;
        for (entry, context) in meta.universe.iter().zip(&contexts) {
            if entry.name != coin {
                continue;
            }
            if selected.is_some() {
                return Err(ExchangeError::invalid_response(
                    EXCHANGE,
                    format!("coin {coin:?} appears more than once in the universe"),
                ));
            }
            selected = Some((entry, context));
        }
        let Some((entry, context)) = selected else {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!("coin {coin:?} is not present in the returned universe"),
            ));
        };
        if entry.is_delisted == Some(true) {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!("coin {coin:?} is marked delisted; refusing to quote it"),
            ));
        }
        let impact_pxs = context.impact_pxs.as_ref().ok_or_else(|| {
            ExchangeError::invalid_response(
                EXCHANGE,
                format!("coin {coin:?} has no impact prices in its asset context"),
            )
        })?;
        let [impact_bid, impact_ask] = impact_pxs.as_slice() else {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "coin {coin:?} impact prices must contain exactly a bid and an ask; found {}",
                    impact_pxs.len()
                ),
            ));
        };
        let bid = parse_price(impact_bid)?;
        let ask = parse_price(impact_ask)?;
        let symbol = Symbol::new(coin)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let mut snapshot = MarketSnapshot::new(
            EXCHANGE,
            symbol,
            MarketType::Perpetual,
            bid,
            ask,
            received_at,
        )
        .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        if let Some(mid) = context.mid_px.as_deref() {
            snapshot.last = Some(parse_price(mid)?);
        }
        let funding = context
            .funding
            .as_deref()
            .map(|raw| {
                let rate = Decimal::from_str(raw).map_err(|error| {
                    ExchangeError::invalid_response(
                        EXCHANGE,
                        format!("invalid funding rate {raw:?}: {error}"),
                    )
                })?;
                HyperliquidFundingRate::new(rate)
            })
            .transpose()?;
        Ok(HyperliquidPublicObservation {
            snapshot,
            event_time: None,
            funding,
        })
    }

    async fn read_response_body(mut response: reqwest::Response) -> Result<Vec<u8>, ExchangeError> {
        let status = response.status();
        if let Some(content_length) = response.content_length() {
            let requested = usize::try_from(content_length).unwrap_or(usize::MAX);
            if requested > MAX_RESPONSE_BODY_BYTES {
                return Err(ExchangeError::resource_limit(
                    "Hyperliquid response body",
                    MAX_RESPONSE_BODY_BYTES,
                    requested,
                ));
            }
        }
        let mut payload = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            ExchangeError::remote_failure(EXCHANGE, Some(status.as_u16()), error.to_string())
        })? {
            let requested = payload.len().checked_add(chunk.len()).ok_or_else(|| {
                ExchangeError::resource_limit(
                    "Hyperliquid response body",
                    MAX_RESPONSE_BODY_BYTES,
                    usize::MAX,
                )
            })?;
            if requested > MAX_RESPONSE_BODY_BYTES {
                return Err(ExchangeError::resource_limit(
                    "Hyperliquid response body",
                    MAX_RESPONSE_BODY_BYTES,
                    requested,
                ));
            }
            payload.try_reserve(chunk.len()).map_err(|_| {
                ExchangeError::unavailable("unable to reserve bounded Hyperliquid response storage")
            })?;
            payload.extend_from_slice(&chunk);
        }
        Ok(payload)
    }

    fn map_remote_failure(status: reqwest::StatusCode, payload: &[u8]) -> ExchangeError {
        let status_reason = status.canonical_reason().unwrap_or("HTTP request failed");
        let reason = match bounded_error_text(payload) {
            Some((text, was_truncated)) => {
                bounded_reason(status_reason, Some(&text), was_truncated)
            }
            None => status_reason.to_owned(),
        };
        ExchangeError::remote_failure(EXCHANGE, Some(status.as_u16()), reason)
    }
}

/// Builds the explicit, bounded USDT-quoted symbol catalog for Hyperliquid
/// perpetual coins.
///
/// Each wire coin `BTC` maps to the canonical symbol `BTCUSDT`, aligning the
/// venue's coin identifiers with the workspace's USDT-quoted pair semantics.
/// The mapping is exact and bidirectional; nothing outside the given coins is
/// ever synthesized.
///
/// # Errors
///
/// Returns [`ExchangeError`] for an empty or oversized coin list, a malformed
/// coin identifier, or an ambiguous catalog.
pub fn hyperliquid_usdt_symbol_catalog(
    coins: &[&str],
) -> Result<ExchangeSymbolCatalog, ExchangeError> {
    if coins.is_empty() {
        return Err(ExchangeError::invalid(
            "Hyperliquid public symbol catalog requires at least one coin",
        ));
    }
    if coins.len() > MAX_HYPERLIQUID_PUBLIC_CATALOG_COINS {
        return Err(ExchangeError::resource_limit(
            "Hyperliquid public symbol catalog",
            MAX_HYPERLIQUID_PUBLIC_CATALOG_COINS,
            coins.len(),
        ));
    }
    let mut symbols = Vec::with_capacity(coins.len());
    for coin in coins {
        if coin.is_empty()
            || coin.len() > MAX_COIN_BYTES
            || !coin.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(ExchangeError::invalid(format!(
                "Hyperliquid public coin must contain 1..={MAX_COIN_BYTES} ASCII alphanumeric bytes"
            )));
        }
        let standard = Symbol::new(format!("{coin}USDT"))
            .map_err(|error| ExchangeError::invalid(error.to_string()))?;
        symbols.push(ExchangeSymbol::new(
            EXCHANGE,
            standard,
            MarketType::Perpetual,
            *coin,
        )?);
    }
    ExchangeSymbolCatalog::new(symbols)
}

fn parse_price(raw: &str) -> Result<Price, ExchangeError> {
    raw.parse()
        .map_err(|error: crypto_trading_domain::DomainError| {
            ExchangeError::invalid_response(EXCHANGE, error.to_string())
        })
}

fn bounded_error_text(payload: &[u8]) -> Option<(String, bool)> {
    let limit = payload.len().min(MAX_ERROR_REASON_TEXT_BYTES);
    let slice = &payload[..limit];
    let was_truncated = payload.len() > MAX_ERROR_REASON_TEXT_BYTES;
    let text = match std::str::from_utf8(slice) {
        Ok(text) => text.to_owned(),
        Err(error) if error.error_len().is_none() && was_truncated => {
            let valid_prefix = &slice[..error.valid_up_to()];
            String::from_utf8_lossy(valid_prefix).into_owned()
        }
        Err(_) => String::from_utf8_lossy(slice).into_owned(),
    };
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return None;
    }
    Some((text, was_truncated))
}

fn bounded_reason(prefix: &str, detail: Option<&str>, force_ellipsis: bool) -> String {
    let Some(detail) = detail else {
        return bounded_text(prefix, MAX_ERROR_REASON_TEXT_BYTES, false);
    };
    let head = format!("{prefix}: ");
    let available = MAX_ERROR_REASON_TEXT_BYTES.saturating_sub(head.len());
    if available == 0 {
        return bounded_text(prefix, MAX_ERROR_REASON_TEXT_BYTES, true);
    }
    let detail = bounded_text(detail, available, force_ellipsis);
    if detail.is_empty() {
        return bounded_text(prefix, MAX_ERROR_REASON_TEXT_BYTES, false);
    }
    format!("{head}{detail}")
}

fn bounded_text(text: &str, max_bytes: usize, force_ellipsis: bool) -> String {
    if text.len() <= max_bytes && !force_ellipsis {
        return text.to_owned();
    }
    if max_bytes == 0 {
        return String::new();
    }
    if max_bytes <= 3 {
        return ".".repeat(max_bytes);
    }

    let content_limit = max_bytes.saturating_sub(3);
    let mut end = text.len().min(content_limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }

    let mut bounded = text[..end].split_whitespace().collect::<Vec<_>>().join(" ");
    bounded.push_str("...");
    bounded
}

#[async_trait]
impl ExchangeHandle for HyperliquidPublicExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        Err(ExchangeError::Unsupported {
            exchange: EXCHANGE.to_owned(),
            operation: command.operation(),
        })
    }

    async fn reconcile(&self, _scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        Err(ExchangeError::Unsupported {
            exchange: EXCHANGE.to_owned(),
            operation: ExchangeOperation::Reconcile,
        })
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        Err(ExchangeError::Unsupported {
            exchange: EXCHANGE.to_owned(),
            operation: ExchangeOperation::Subscribe,
        })
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Ok(ExchangeStatus {
            exchange: EXCHANGE.to_owned(),
            mode: ExchangeMode::ReadOnly,
            availability: ExchangeAvailability::Ready,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}
