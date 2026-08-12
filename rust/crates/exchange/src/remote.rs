use std::{fmt, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Timelike, Utc};
use crypto_trading_domain::{
    OperationalRestObservation, record_operational_rest_response,
    record_operational_rest_transport_error,
};
use reqwest::Url;

use crate::{
    ExchangeAvailability, ExchangeError,
    error::{RemoteFailureMetadata, RemoteRetryAfter},
};

const MAX_REMOTE_RESPONSE_BYTES: usize = 1_048_576;
const MAX_REMOTE_METADATA_HEADERS: usize = 8;
const MAX_REMOTE_HEADER_NAME_BYTES: usize = 64;
const MAX_REMOTE_HEADER_VALUE_BYTES: usize = 512;
const BINANCE_OPERATOR_USED_WEIGHT_HIGH_WATER_1M: u64 = 240;

/// Small local margin after the next UTC minute boundary before requests can
/// resume from the repo's conservative used-weight gate.
const BINANCE_HIGH_WATER_SAFETY_MARGIN: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinanceRateLimitSnapshot {
    observed_at: DateTime<Utc>,
    retry_after: Option<RemoteRetryAfter>,
    retry_after_deadline: Option<DateTime<Utc>>,
    used_weight: Option<u64>,
    used_weight_1m: Option<u64>,
    order_count_10s: Option<u64>,
    order_count_1m: Option<u64>,
    order_count_1d: Option<u64>,
}

impl BinanceRateLimitSnapshot {
    fn from_headers<'a>(
        headers: impl IntoIterator<Item = (&'a str, &'a str)>,
        observed_at: DateTime<Utc>,
    ) -> Self {
        let mut snapshot = Self {
            observed_at,
            retry_after: None,
            retry_after_deadline: None,
            used_weight: None,
            used_weight_1m: None,
            order_count_10s: None,
            order_count_1m: None,
            order_count_1d: None,
        };
        for (name, value) in headers {
            match name.trim().to_ascii_lowercase().as_str() {
                "retry-after" => {
                    snapshot.retry_after = parse_retry_after(value);
                    snapshot.retry_after_deadline = snapshot
                        .retry_after
                        .as_ref()
                        .and_then(|retry_after| retry_after_deadline(retry_after, observed_at));
                }
                "x-mbx-used-weight" => {
                    snapshot.used_weight = parse_header_counter(value);
                }
                "x-mbx-used-weight-1m" => {
                    snapshot.used_weight_1m = parse_header_counter(value);
                }
                "x-mbx-order-count-10s" => {
                    snapshot.order_count_10s = parse_header_counter(value);
                }
                "x-mbx-order-count-1m" => {
                    snapshot.order_count_1m = parse_header_counter(value);
                }
                "x-mbx-order-count-1d" => {
                    snapshot.order_count_1d = parse_header_counter(value);
                }
                _ => {}
            }
        }
        snapshot
    }

    pub(crate) fn used_weight_watermark(&self) -> Option<u64> {
        self.used_weight_1m.or(self.used_weight)
    }

    pub(crate) fn order_count_watermark(&self) -> Option<u64> {
        self.order_count_10s
            .or(self.order_count_1m)
            .or(self.order_count_1d)
    }

    pub(crate) fn retry_after_deadline(&self) -> Option<DateTime<Utc>> {
        self.retry_after_deadline
    }

    fn is_high_water(&self, policy: BinanceRateLimitPolicy) -> bool {
        policy.used_weight_1m_high_water.is_some_and(|limit| {
            self.used_weight_1m
                .or(self.used_weight)
                .is_some_and(|value| value >= limit)
        }) || policy
            .order_count_10s_high_water
            .is_some_and(|limit| self.order_count_10s.is_some_and(|value| value >= limit))
            || policy
                .order_count_1m_high_water
                .is_some_and(|limit| self.order_count_1m.is_some_and(|value| value >= limit))
            || policy
                .order_count_1d_high_water
                .is_some_and(|limit| self.order_count_1d.is_some_and(|value| value >= limit))
    }

    fn retry_after_unix_seconds(&self) -> Option<u64> {
        self.retry_after_deadline.and_then(datetime_to_unix_seconds)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BinanceRateLimitPolicy {
    pub(crate) used_weight_1m_high_water: Option<u64>,
    pub(crate) order_count_10s_high_water: Option<u64>,
    pub(crate) order_count_1m_high_water: Option<u64>,
    pub(crate) order_count_1d_high_water: Option<u64>,
    pub(crate) high_water_safety_margin: Duration,
}

impl Default for BinanceRateLimitPolicy {
    fn default() -> Self {
        Self {
            used_weight_1m_high_water: Some(BINANCE_OPERATOR_USED_WEIGHT_HIGH_WATER_1M),
            order_count_10s_high_water: None,
            order_count_1m_high_water: None,
            order_count_1d_high_water: None,
            high_water_safety_margin: BINANCE_HIGH_WATER_SAFETY_MARGIN,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct BinanceRateBudgetState {
    latest: Option<BinanceRateLimitSnapshot>,
    throttle_until: Option<DateTime<Utc>>,
}

impl BinanceRateBudgetState {
    pub(crate) fn observe_response(
        &mut self,
        snapshot: BinanceRateLimitSnapshot,
        policy: BinanceRateLimitPolicy,
    ) {
        let throttle_until = snapshot
            .retry_after_deadline()
            .or_else(|| {
                if snapshot.is_high_water(policy) {
                    next_utc_minute_boundary(snapshot.observed_at, policy.high_water_safety_margin)
                } else {
                    None
                }
            })
            .map(|candidate| match self.throttle_until {
                Some(current) => current.max(candidate),
                None => candidate,
            });
        self.latest = Some(snapshot);
        self.throttle_until = throttle_until.or(self.throttle_until);
    }

    pub(crate) fn availability(&self, now: DateTime<Utc>) -> ExchangeAvailability {
        if self.throttle_until.is_some_and(|deadline| deadline > now) {
            ExchangeAvailability::Unavailable
        } else {
            ExchangeAvailability::Ready
        }
    }

    pub(crate) fn active_rejection(&self, now: DateTime<Utc>) -> Option<ExchangeError> {
        let snapshot = self.latest.as_ref()?;
        let deadline = self.throttle_until?;
        if deadline <= now {
            return None;
        }
        let retry_after = snapshot
            .retry_after
            .clone()
            .or(Some(RemoteRetryAfter::At(deadline)));
        let reason = if snapshot
            .retry_after_deadline()
            .is_some_and(|value| value > now)
        {
            "Binance retry-after gate is still active; remote calls are blocked until the durable deadline elapses"
        } else {
            "Binance local used-weight gate is still active; remote calls are blocked until the next UTC minute window opens"
        };
        Some(
            ExchangeError::remote_failure("binance", Some(429), reason).with_remote_metadata(
                RemoteFailureMetadata {
                    exchange_code: None,
                    retry_after,
                    server_time: Some(snapshot.observed_at),
                },
            ),
        )
    }
}

/// Minimal HTTP methods required by exchange REST protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteHttpMethod {
    Get,
    Post,
    Delete,
}

/// Transport-neutral HTTP request with secret-safe diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteHttpRequest {
    method: RemoteHttpMethod,
    url: Url,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RemoteHttpRequest {
    pub(crate) fn new(
        method: RemoteHttpMethod,
        url: Url,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            method,
            url,
            headers,
            body,
        }
    }

    pub const fn method(&self) -> RemoteHttpMethod {
        self.method
    }

    pub const fn url(&self) -> &Url {
        &self.url
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn headers(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl fmt::Debug for RemoteHttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let query_parameter_names = self
            .url
            .query_pairs()
            .map(|(name, _)| name.into_owned())
            .collect::<Vec<_>>();
        let header_names = self
            .headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("RemoteHttpRequest")
            .field("method", &self.method)
            .field("path", &self.url.path())
            .field("query_parameter_names", &query_parameter_names)
            .field("header_names", &header_names)
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

/// Bounded transport-neutral HTTP response.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl fmt::Debug for RemoteHttpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteHttpResponse")
            .field("status", &self.status)
            .field(
                "header_names",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

impl RemoteHttpResponse {
    /// Builds a response fixture or transport result under the global body
    /// limit.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for an invalid HTTP status and
    /// [`ExchangeError::ResourceLimit`] for an oversized body.
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Result<Self, ExchangeError> {
        Self::new_with_headers(status, Vec::new(), body)
    }

    /// Builds a response fixture or transport result while preserving response
    /// headers for later exchange-specific classification.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] for an invalid HTTP status and
    /// [`ExchangeError::ResourceLimit`] for an oversized body.
    pub fn new_with_headers(
        status: u16,
        headers: impl IntoIterator<Item = (String, String)>,
        body: impl Into<Vec<u8>>,
    ) -> Result<Self, ExchangeError> {
        if !(100..=599).contains(&status) {
            return Err(ExchangeError::invalid_response(
                "http",
                format!("invalid HTTP status {status}"),
            ));
        }
        let body = body.into();
        if body.len() > MAX_REMOTE_RESPONSE_BYTES {
            return Err(ExchangeError::resource_limit(
                "remote HTTP response body",
                MAX_REMOTE_RESPONSE_BYTES,
                body.len(),
            ));
        }
        Ok(Self {
            status,
            headers: bounded_metadata_headers(headers)?,
            body,
        })
    }

    pub const fn status(&self) -> u16 {
        self.status
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn headers(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub const fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    pub fn retry_after(&self) -> Option<RemoteRetryAfter> {
        parse_retry_after(self.header("retry-after")?)
    }

    pub fn server_time(&self) -> Option<DateTime<Utc>> {
        parse_http_date(self.header("date")?)
    }

    pub fn remote_failure_metadata(&self) -> RemoteFailureMetadata {
        RemoteFailureMetadata {
            exchange_code: None,
            retry_after: self.retry_after(),
            server_time: self.server_time(),
        }
    }

    pub(crate) fn binance_rate_limit_snapshot(
        &self,
        observed_at: DateTime<Utc>,
    ) -> BinanceRateLimitSnapshot {
        BinanceRateLimitSnapshot::from_headers(self.headers(), observed_at)
    }
}

/// Injectable asynchronous transport used by offline protocol tests and later
/// real testnet clients.
#[async_trait]
pub trait RemoteHttpTransport: Send + Sync {
    async fn send(&self, request: RemoteHttpRequest) -> Result<RemoteHttpResponse, ExchangeError>;
}

/// Bounded `reqwest` transport for explicitly selected testnet or loopback
/// protocol endpoints.
#[derive(Debug, Clone)]
pub struct ReqwestHttpTransport {
    client: reqwest::Client,
}

impl ReqwestHttpTransport {
    /// Builds a transport with one total request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a zero timeout or
    /// [`ExchangeError::Unavailable`] if the HTTP client cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, ExchangeError> {
        if timeout.is_zero() {
            return Err(ExchangeError::invalid(
                "remote HTTP timeout must be greater than zero",
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent("crypto-trading/0.1 testnet-protocol")
            // Signed requests carry the venue API key in a custom header and
            // the signature in the query string. reqwest preserves custom
            // headers across redirects, so following one would replay both to
            // whatever host the response names. Endpoint construction already
            // pins the scheme and host; refusing redirects keeps a response
            // from widening that pin. A 3xx surfaces as an ordinary
            // non-success response instead.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ExchangeError::unavailable("unable to build remote HTTP transport"))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl RemoteHttpTransport for ReqwestHttpTransport {
    async fn send(&self, request: RemoteHttpRequest) -> Result<RemoteHttpResponse, ExchangeError> {
        let started_at = std::time::Instant::now();
        let method = match request.method {
            RemoteHttpMethod::Get => reqwest::Method::GET,
            RemoteHttpMethod::Post => reqwest::Method::POST,
            RemoteHttpMethod::Delete => reqwest::Method::DELETE,
        };
        let mut builder = self.client.request(method, request.url);
        for (name, value) in request.headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ExchangeError::invalid("remote HTTP header name is invalid"))?;
            let value = reqwest::header::HeaderValue::from_str(&value)
                .map_err(|_| ExchangeError::invalid("remote HTTP header value is invalid"))?;
            builder = builder.header(name, value);
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body);
        }
        let mut response = builder.send().await.map_err(|error| {
            record_operational_rest_transport_error(elapsed_micros(started_at));
            if error.is_timeout() {
                ExchangeError::unavailable("remote HTTP request timed out")
            } else {
                ExchangeError::unavailable("remote HTTP transport failed")
            }
        })?;
        let status = response.status().as_u16();
        if let Some(content_length) = response.content_length() {
            let requested = usize::try_from(content_length).unwrap_or(usize::MAX);
            if requested > MAX_REMOTE_RESPONSE_BYTES {
                return Err(ExchangeError::resource_limit(
                    "remote HTTP response body",
                    MAX_REMOTE_RESPONSE_BYTES,
                    requested,
                ));
            }
        }
        let mut body = Vec::new();
        let headers = retained_metadata_headers(response.headers());
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            record_operational_rest_transport_error(elapsed_micros(started_at));
            ExchangeError::unavailable("remote HTTP response body failed")
        })? {
            let requested = body.len().checked_add(chunk.len()).ok_or_else(|| {
                ExchangeError::resource_limit(
                    "remote HTTP response body",
                    MAX_REMOTE_RESPONSE_BYTES,
                    usize::MAX,
                )
            })?;
            if requested > MAX_REMOTE_RESPONSE_BYTES {
                return Err(ExchangeError::resource_limit(
                    "remote HTTP response body",
                    MAX_REMOTE_RESPONSE_BYTES,
                    requested,
                ));
            }
            body.try_reserve(chunk.len()).map_err(|_| {
                ExchangeError::unavailable("unable to reserve remote HTTP response storage")
            })?;
            body.extend_from_slice(&chunk);
        }
        let response = RemoteHttpResponse::new_with_headers(status, headers, body)?;
        record_operational_rest_response(observation_from_response(
            &response,
            response.server_time().unwrap_or_else(Utc::now),
            elapsed_micros(started_at),
        ));
        Ok(response)
    }
}

fn parse_retry_after(value: &str) -> Option<RemoteRetryAfter> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(RemoteRetryAfter::Seconds(seconds));
    }
    parse_http_date(value).map(RemoteRetryAfter::At)
}

fn parse_http_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn parse_header_counter(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

fn retry_after_deadline(
    retry_after: &RemoteRetryAfter,
    observed_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match retry_after {
        RemoteRetryAfter::Seconds(seconds) => {
            let delay = TimeDelta::seconds(i64::try_from(*seconds).ok()?);
            observed_at.checked_add_signed(delay)
        }
        RemoteRetryAfter::At(deadline) => Some(*deadline),
    }
}

fn next_utc_minute_boundary(
    observed_at: DateTime<Utc>,
    safety_margin: Duration,
) -> Option<DateTime<Utc>> {
    let seconds_into_minute = i64::from(observed_at.second());
    let nanos_into_second = i64::from(observed_at.nanosecond());
    let truncated = observed_at
        .checked_sub_signed(TimeDelta::seconds(seconds_into_minute))?
        .checked_sub_signed(TimeDelta::nanoseconds(nanos_into_second))?;
    let boundary = truncated.checked_add_signed(TimeDelta::minutes(1))?;
    let margin = TimeDelta::from_std(safety_margin).ok()?;
    boundary.checked_add_signed(margin)
}

fn datetime_to_unix_seconds(value: DateTime<Utc>) -> Option<u64> {
    u64::try_from(value.timestamp()).ok()
}

fn elapsed_micros(started_at: std::time::Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub(crate) fn metadata_from_reqwest_headers(
    headers: &reqwest::header::HeaderMap,
) -> RemoteFailureMetadata {
    let retry_after = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after);
    let server_time = headers
        .get(reqwest::header::DATE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_http_date);
    RemoteFailureMetadata {
        exchange_code: None,
        retry_after,
        server_time,
    }
}

pub(crate) fn retained_metadata_headers(
    headers: &reqwest::header::HeaderMap,
) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            if !is_retained_metadata_header(name.as_str()) {
                return None;
            }
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect()
}

pub(crate) fn observation_from_response(
    response: &RemoteHttpResponse,
    observed_at: DateTime<Utc>,
    latency_micros: u64,
) -> OperationalRestObservation {
    let snapshot = response.binance_rate_limit_snapshot(observed_at);
    OperationalRestObservation {
        latency_micros,
        status: response.status(),
        used_weight: snapshot.used_weight_watermark(),
        order_count: snapshot.order_count_watermark(),
        retry_after_unix_seconds: snapshot.retry_after_unix_seconds(),
    }
}

fn bounded_metadata_headers(
    headers: impl IntoIterator<Item = (String, String)>,
) -> Result<Vec<(String, String)>, ExchangeError> {
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(MAX_REMOTE_METADATA_HEADERS)
        .map_err(|_| {
            ExchangeError::unavailable("unable to reserve bounded remote response metadata")
        })?;
    for (name, value) in headers {
        if !is_retained_metadata_header(&name) {
            continue;
        }
        let normalized_name = name.trim().to_ascii_lowercase();
        let normalized_value = value.trim();
        if normalized_name.is_empty()
            || normalized_name.len() > MAX_REMOTE_HEADER_NAME_BYTES
            || !normalized_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(ExchangeError::invalid_response(
                "http",
                "remote response metadata header name is invalid",
            ));
        }
        if normalized_value.len() > MAX_REMOTE_HEADER_VALUE_BYTES
            || normalized_value
                .bytes()
                .any(|byte| matches!(byte, b'\r' | b'\n' | 0))
        {
            return Err(ExchangeError::resource_limit(
                "remote response metadata header value",
                MAX_REMOTE_HEADER_VALUE_BYTES,
                normalized_value.len(),
            ));
        }
        if retained
            .iter()
            .any(|(existing, _)| existing == &normalized_name)
        {
            return Err(ExchangeError::invalid_response(
                "http",
                "remote response contains duplicate metadata headers",
            ));
        }
        if retained.len() == MAX_REMOTE_METADATA_HEADERS {
            return Err(ExchangeError::resource_limit(
                "remote response metadata headers",
                MAX_REMOTE_METADATA_HEADERS,
                retained.len().saturating_add(1),
            ));
        }
        retained.push((normalized_name, normalized_value.to_owned()));
    }
    Ok(retained)
}

fn is_retained_metadata_header(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "date"
            | "retry-after"
            | "x-mbx-used-weight"
            | "x-mbx-used-weight-1m"
            | "x-mbx-order-count-10s"
            | "x-mbx-order-count-1m"
            | "x-mbx-order-count-1d"
            | "x-response-time"
    )
}
