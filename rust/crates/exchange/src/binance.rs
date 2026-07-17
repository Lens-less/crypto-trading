use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Quantity, Symbol};
use serde::Deserialize;

use crate::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeOperation,
    ExchangeStatus, MarketSubscription, ReconcileReceipt, ReconcileScope, SubscriptionReceipt,
    TradingCommand, TradingReceipt,
};

const EXCHANGE: &str = "binance";
const DEFAULT_BASE_URL: &str = "https://data-api.binance.vision";
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;

/// Read-only Binance Spot public-market-data adapter.
#[derive(Debug, Clone)]
pub struct BinancePublicExchange {
    client: reqwest::Client,
    book_ticker_url: reqwest::Url,
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

impl BinancePublicExchange {
    /// Creates an adapter against Binance's market-data-only base endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client or the official endpoint cannot be
    /// constructed.
    pub fn new() -> Result<Self, ExchangeError> {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    /// Creates an adapter against a caller-selected compatible public endpoint.
    ///
    /// This is useful for Binance test environments and deterministic local
    /// contract tests. The adapter never adds credentials or signatures.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid base URL or HTTP client construction
    /// failure.
    pub fn with_base_url(base_url: &str) -> Result<Self, ExchangeError> {
        let base_url = reqwest::Url::parse(base_url)
            .map_err(|error| ExchangeError::invalid(error.to_string()))?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(ExchangeError::invalid(
                "Binance public base URL must use http or https",
            ));
        }
        let book_ticker_url = base_url
            .join("/api/v3/ticker/bookTicker")
            .map_err(|error| ExchangeError::invalid(error.to_string()))?;
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .user_agent("crypto-trading/0.1 public-market-data")
            .build()
            .map_err(|error| ExchangeError::unavailable(error.to_string()))?;
        Ok(Self {
            client,
            book_ticker_url,
        })
    }

    /// Fetches the current best bid and ask for one exact Binance Spot symbol.
    ///
    /// The request uses only the public `bookTicker` route and never attaches
    /// API keys, credentials, timestamps, or signatures. Binance wire symbols
    /// such as `BTCUSDT` must be supplied directly.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::RemoteFailure`] for transport or non-success
    /// HTTP responses, and [`ExchangeError::InvalidResponse`] for malformed or
    /// mismatched response data.
    pub async fn fetch_snapshot(&self, symbol: &Symbol) -> Result<MarketSnapshot, ExchangeError> {
        let mut response = self
            .client
            .get(self.book_ticker_url.clone())
            .query(&[("symbol", symbol.as_str())])
            .send()
            .await
            .map_err(|error| ExchangeError::remote_failure(EXCHANGE, None, error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ExchangeError::remote_failure(
                EXCHANGE,
                Some(status.as_u16()),
                status.canonical_reason().unwrap_or("HTTP request failed"),
            ));
        }
        if let Some(content_length) = response.content_length() {
            let requested = usize::try_from(content_length).unwrap_or(usize::MAX);
            if requested > MAX_RESPONSE_BODY_BYTES {
                return Err(ExchangeError::resource_limit(
                    "Binance response body",
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
                    "Binance response body",
                    MAX_RESPONSE_BODY_BYTES,
                    usize::MAX,
                )
            })?;
            if requested > MAX_RESPONSE_BODY_BYTES {
                return Err(ExchangeError::resource_limit(
                    "Binance response body",
                    MAX_RESPONSE_BODY_BYTES,
                    requested,
                ));
            }
            payload.try_reserve(chunk.len()).map_err(|_| {
                ExchangeError::unavailable("unable to reserve bounded Binance response storage")
            })?;
            payload.extend_from_slice(&chunk);
        }
        let snapshot = Self::parse_book_ticker(&payload, Utc::now())?;
        if snapshot.symbol != *symbol {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!("requested symbol {symbol}, received {}", snapshot.symbol),
            ));
        }
        Ok(snapshot)
    }

    /// Parses the documented Binance `bookTicker` object using exact decimals.
    ///
    /// Binance does not include an event timestamp in this response, so the
    /// caller supplies the local receive time used by the domain snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed JSON, invalid
    /// financial values, empty symbols, or crossed quotes.
    pub fn parse_book_ticker(
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<MarketSnapshot, ExchangeError> {
        let wire: BookTickerWire = serde_json::from_slice(payload)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let symbol = Symbol::new(wire.symbol)
            .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        let bid: Price =
            wire.bid_price
                .parse()
                .map_err(|error: crypto_trading_domain::DomainError| {
                    ExchangeError::invalid_response(EXCHANGE, error.to_string())
                })?;
        let ask: Price =
            wire.ask_price
                .parse()
                .map_err(|error: crypto_trading_domain::DomainError| {
                    ExchangeError::invalid_response(EXCHANGE, error.to_string())
                })?;
        let bid_quantity: Quantity =
            wire.bid_qty
                .parse()
                .map_err(|error: crypto_trading_domain::DomainError| {
                    ExchangeError::invalid_response(EXCHANGE, error.to_string())
                })?;
        let ask_quantity: Quantity =
            wire.ask_qty
                .parse()
                .map_err(|error: crypto_trading_domain::DomainError| {
                    ExchangeError::invalid_response(EXCHANGE, error.to_string())
                })?;
        let mut snapshot =
            MarketSnapshot::new(EXCHANGE, symbol, MarketType::Spot, bid, ask, received_at)
                .map_err(|error| ExchangeError::invalid_response(EXCHANGE, error.to_string()))?;
        snapshot.bid_quantity = Some(bid_quantity);
        snapshot.ask_quantity = Some(ask_quantity);
        Ok(snapshot)
    }
}

#[async_trait]
impl ExchangeHandle for BinancePublicExchange {
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
