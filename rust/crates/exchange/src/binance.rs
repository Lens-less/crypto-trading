use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Quantity, Symbol};
use serde::Deserialize;

use crate::remote::metadata_from_reqwest_headers;
use crate::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeOperation,
    ExchangeStatus, MarketSubscription, ReconcileReceipt, ReconcileScope, SubscriptionReceipt,
    TradingCommand, TradingReceipt,
};

const EXCHANGE: &str = "binance";
const DEFAULT_BASE_URL: &str = "https://data-api.binance.vision";
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY_BYTES: usize = 1_048_576;
const MAX_ERROR_REASON_TEXT_BYTES: usize = 256;

/// Read-only Binance Spot public-market-data adapter.
#[derive(Debug, Clone)]
pub struct BinancePublicExchange {
    client: reqwest::Client,
    book_ticker_url: reqwest::Url,
}

/// One public Binance book-ticker observation plus the bounded source metadata
/// the runtime can safely rely on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinancePublicObservation {
    pub snapshot: MarketSnapshot,
    pub event_time: Option<DateTime<Utc>>,
    pub source_sequence: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookTickerWire {
    symbol: String,
    bid_price: String,
    bid_qty: String,
    ask_price: String,
    ask_qty: String,
    #[serde(rename = "updateId", alias = "u", default)]
    source_sequence: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BinanceErrorWire {
    code: i64,
    msg: String,
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
        Ok(self.fetch_observation(symbol, Utc::now()).await?.snapshot)
    }

    /// Fetches one public Binance Spot book-ticker observation using a
    /// caller-supplied local receive time.
    ///
    /// Binance's REST `bookTicker` payload does not carry an event timestamp.
    /// The runtime therefore injects the local receive time it wants the
    /// resulting snapshot to carry, while still preserving any optional source
    /// sequence compatible endpoints expose via `updateId`/`u`.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::RemoteFailure`] for transport or non-success
    /// HTTP responses, and [`ExchangeError::InvalidResponse`] for malformed or
    /// mismatched response data.
    pub async fn fetch_observation(
        &self,
        symbol: &Symbol,
        received_at: DateTime<Utc>,
    ) -> Result<BinancePublicObservation, ExchangeError> {
        let response = self
            .client
            .get(self.book_ticker_url.clone())
            .query(&[("symbol", symbol.as_str())])
            .send()
            .await
            .map_err(|error| ExchangeError::remote_failure(EXCHANGE, None, error.to_string()))?;
        let status = response.status();
        let failure_metadata = metadata_from_reqwest_headers(response.headers());
        let payload = Self::read_response_body(response).await?;

        if !status.is_success() {
            return Err(
                Self::map_remote_failure(status, &payload).with_remote_metadata(failure_metadata)
            );
        }
        let observation = Self::parse_book_ticker_observation(&payload, received_at)?;
        if observation.snapshot.symbol != *symbol {
            return Err(ExchangeError::invalid_response(
                EXCHANGE,
                format!(
                    "requested symbol {symbol}, received {}",
                    observation.snapshot.symbol
                ),
            ));
        }
        Ok(observation)
    }

    async fn read_response_body(mut response: reqwest::Response) -> Result<Vec<u8>, ExchangeError> {
        let status = response.status();
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
        Ok(payload)
    }

    fn map_remote_failure(status: reqwest::StatusCode, payload: &[u8]) -> ExchangeError {
        if let Ok(error) = serde_json::from_slice::<BinanceErrorWire>(payload) {
            let reason = Self::bounded_reason(
                &format!("Binance error {}", error.code),
                Some(&error.msg),
                false,
            );
            return ExchangeError::remote_failure(EXCHANGE, Some(status.as_u16()), reason)
                .with_exchange_code(error.code.to_string());
        }

        let status_reason = status.canonical_reason().unwrap_or("HTTP request failed");
        let reason = match Self::bounded_error_text(payload) {
            Some((text, was_truncated)) => {
                Self::bounded_reason(status_reason, Some(&text), was_truncated)
            }
            None => status_reason.to_owned(),
        };
        ExchangeError::remote_failure(EXCHANGE, Some(status.as_u16()), reason)
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
            return Self::bounded_text(prefix, MAX_ERROR_REASON_TEXT_BYTES, false);
        };
        let head = format!("{prefix}: ");
        let available = MAX_ERROR_REASON_TEXT_BYTES.saturating_sub(head.len());
        if available == 0 {
            return Self::bounded_text(prefix, MAX_ERROR_REASON_TEXT_BYTES, true);
        }
        let detail = Self::bounded_text(detail, available, force_ellipsis);
        if detail.is_empty() {
            return Self::bounded_text(prefix, MAX_ERROR_REASON_TEXT_BYTES, false);
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
        Ok(Self::parse_book_ticker_observation(payload, received_at)?.snapshot)
    }

    /// Parses the documented Binance `bookTicker` object into one observation
    /// with explicit source metadata.
    ///
    /// Compatible payloads may expose a source sequence as either `updateId`
    /// or `u`. Binance's REST response does not include an event timestamp, so
    /// `event_time` stays absent and the caller-provided local receive time is
    /// written into the snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for malformed JSON, invalid
    /// financial values, empty symbols, or crossed quotes.
    pub fn parse_book_ticker_observation(
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<BinancePublicObservation, ExchangeError> {
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
        Ok(BinancePublicObservation {
            snapshot,
            event_time: None,
            source_sequence: wire.source_sequence,
        })
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
