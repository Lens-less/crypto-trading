use std::{fmt, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Url;

use crate::{
    ExchangeError,
    error::{RemoteFailureMetadata, RemoteRetryAfter},
};

const MAX_REMOTE_RESPONSE_BYTES: usize = 1_048_576;
const MAX_REMOTE_METADATA_HEADERS: usize = 8;
const MAX_REMOTE_HEADER_NAME_BYTES: usize = 64;
const MAX_REMOTE_HEADER_VALUE_BYTES: usize = 512;

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
        let headers = response
            .headers()
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
            .collect::<Vec<_>>();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ExchangeError::unavailable("remote HTTP response body failed"))?
        {
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
        RemoteHttpResponse::new_with_headers(status, headers, body)
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
