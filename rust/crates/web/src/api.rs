use std::{
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Query, Request, State, rejection::QueryRejection},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER, WWW_AUTHENTICATE},
    },
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use crypto_trading_control_plane::{
    ArbitrageMonitorReadModel, CapabilityManifest, ControlPlaneEventsPage, ControlPlaneSnapshot,
    ExecutionBatchState, OperatorReadModel, PaperAccountReadModel, PriceAlertReadModel,
    ProjectionStatus, ReadControlPlane, ReadFailureKind, ReadOnlyTaskReadModel, RecoveryDirective,
    ReleaseStage, VirtualGridScannerReadModel,
};
use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const API_SCHEMA_VERSION: u16 = 1;
const MIN_BEARER_TOKEN_BYTES: usize = 32;
const MAX_BEARER_TOKEN_BYTES: usize = 512;
const MAX_CURSOR_QUERY_BYTES: usize = 256;
const EVENT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const EVENT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const LAST_EVENT_ID_HEADER: &str = "last-event-id";
pub const SETTINGS_SCHEMA_VERSION: u16 = 1;
pub const WEB_REQUEST_LIMIT_PER_MINUTE: u32 = 240;

const CACHE_CONTROL_VALUE: &str = "no-store";
const API_CONTENT_SECURITY_POLICY_VALUE: &str =
    "default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const UI_CONTENT_SECURITY_POLICY_VALUE: &str = "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-src 'none'; frame-ancestors 'none'; object-src 'none'";
const PERMISSIONS_POLICY_VALUE: &str = "camera=(), microphone=(), geolocation=()";

#[derive(Clone)]
struct ApiState {
    control_plane: Arc<ReadControlPlane>,
    authentication_required: bool,
    settings: Arc<SettingsResponse>,
}

#[derive(Clone)]
struct ApiAccessState {
    access: WebAccessPolicy,
    rate_limiter: WebRequestRateLimiter,
}

/// Optional bearer-token policy layered on top of mandatory loopback binding.
#[derive(Clone, Default)]
pub struct WebAccessPolicy {
    bearer_token: Option<Arc<[u8]>>,
}

impl WebAccessPolicy {
    #[must_use]
    pub const fn loopback_open() -> Self {
        Self { bearer_token: None }
    }

    /// Enables bearer authentication without exposing the token through Debug.
    ///
    /// # Errors
    ///
    /// Returns [`WebAccessPolicyError::InvalidTokenLength`] when the token is
    /// too weak or too large for a bounded request comparison.
    pub fn bearer(token: impl Into<String>) -> Result<Self, WebAccessPolicyError> {
        let token = token.into();
        if !(MIN_BEARER_TOKEN_BYTES..=MAX_BEARER_TOKEN_BYTES).contains(&token.len()) {
            return Err(WebAccessPolicyError::InvalidTokenLength {
                bytes: token.len(),
                min: MIN_BEARER_TOKEN_BYTES,
                max: MAX_BEARER_TOKEN_BYTES,
            });
        }
        Ok(Self {
            bearer_token: Some(Arc::from(token.into_bytes())),
        })
    }

    #[must_use]
    pub const fn authentication_required(&self) -> bool {
        self.bearer_token.is_some()
    }

    fn authorizes(&self, request: &Request) -> bool {
        let Some(expected) = self.bearer_token.as_deref() else {
            return true;
        };
        let Some(supplied) = request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        constant_time_eq(expected, supplied.as_bytes())
    }
}

impl fmt::Debug for WebAccessPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebAccessPolicy")
            .field("authentication_required", &self.authentication_required())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum WebAccessPolicyError {
    #[error("bearer token has {bytes} bytes; expected {min}..={max}")]
    InvalidTokenLength {
        bytes: usize,
        min: usize,
        max: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialConfiguration {
    Configured,
    Partial,
    NotConfigured,
    NotAccepted,
    NotProjected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLogSink {
    StdoutStderr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationEvidence {
    JournalProjection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperProfileKind {
    Grid,
    Arbitrage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperProfileSettings {
    pub kind: PaperProfileKind,
    pub task_id: String,
    pub strategy_id: String,
    pub strategy_revision: String,
    pub configuration_files: Vec<String>,
    pub replay_file: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialSettings {
    pub web_bearer: CredentialConfiguration,
    pub binance_testnet: CredentialConfiguration,
    pub mainnet: CredentialConfiguration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLimitSettings {
    pub maximum_requests: u32,
    pub window_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsResponse {
    pub schema_version: u16,
    pub data_directory: Option<String>,
    pub journal_path: Option<String>,
    pub log_sink: RuntimeLogSink,
    pub notification_evidence: NotificationEvidence,
    pub credentials: CredentialSettings,
    pub paper_principal_id: Option<String>,
    pub paper_profiles: Vec<PaperProfileSettings>,
    pub request_limit: RequestLimitSettings,
}

impl Default for SettingsResponse {
    fn default() -> Self {
        Self {
            schema_version: SETTINGS_SCHEMA_VERSION,
            data_directory: None,
            journal_path: None,
            log_sink: RuntimeLogSink::StdoutStderr,
            notification_evidence: NotificationEvidence::JournalProjection,
            credentials: CredentialSettings {
                web_bearer: CredentialConfiguration::NotProjected,
                binance_testnet: CredentialConfiguration::NotProjected,
                mainnet: CredentialConfiguration::NotAccepted,
            },
            paper_principal_id: None,
            paper_profiles: Vec::new(),
            request_limit: WebRequestRateLimiter::default().settings(),
        }
    }
}

#[derive(Clone)]
pub struct WebRequestRateLimiter {
    inner: Arc<Mutex<RequestWindow>>,
    maximum_requests: u32,
    window: Duration,
}

impl WebRequestRateLimiter {
    #[must_use]
    pub fn settings(&self) -> RequestLimitSettings {
        RequestLimitSettings {
            maximum_requests: self.maximum_requests,
            window_seconds: self.window.as_secs(),
        }
    }

    /// Consumes one request slot or returns a bounded Retry-After delay.
    ///
    /// # Errors
    ///
    /// Returns the number of whole seconds before the active window resets
    /// after its request budget is exhausted.
    pub fn try_acquire(&self) -> Result<(), u64> {
        let now = Instant::now();
        let mut window = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed = now.saturating_duration_since(window.started_at);
        if elapsed >= self.window {
            window.started_at = now;
            window.used = 0;
        }
        if window.used >= self.maximum_requests {
            return Err(self.window.saturating_sub(elapsed).as_secs().max(1));
        }
        window.used = window.used.saturating_add(1);
        Ok(())
    }
}

impl Default for WebRequestRateLimiter {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RequestWindow {
                started_at: Instant::now(),
                used: 0,
            })),
            maximum_requests: WEB_REQUEST_LIMIT_PER_MINUTE,
            window: Duration::from_secs(60),
        }
    }
}

struct RequestWindow {
    started_at: Instant,
    used: u32,
}

/// Builds the read-only API router. The resulting router has no missing state.
pub fn api_router(control_plane: Arc<ReadControlPlane>, access: WebAccessPolicy) -> Router {
    api_router_with_settings(control_plane, access, SettingsResponse::default())
}

/// Builds the read-only API router with trusted deployment metadata.
pub fn api_router_with_settings(
    control_plane: Arc<ReadControlPlane>,
    access: WebAccessPolicy,
    settings: SettingsResponse,
) -> Router {
    Router::new()
        .nest(
            "/api/v1",
            api_routes_with_settings(
                control_plane,
                access,
                settings,
                WebRequestRateLimiter::default(),
            ),
        )
        .fallback(not_found)
        .layer(middleware::from_fn(add_security_headers))
}

pub(crate) fn api_routes_with_settings(
    control_plane: Arc<ReadControlPlane>,
    access: WebAccessPolicy,
    mut settings: SettingsResponse,
    rate_limiter: WebRequestRateLimiter,
) -> Router {
    settings.request_limit = rate_limiter.settings();
    let state = ApiState {
        control_plane,
        authentication_required: access.authentication_required(),
        settings: Arc::new(settings),
    };
    let access_state = ApiAccessState {
        access,
        rate_limiter,
    };
    let protected = Router::new()
        .route("/system", get(system))
        .route("/capabilities", get(capabilities))
        .route("/monitor", get(monitor))
        .route("/alerts", get(alerts))
        .route("/tasks", get(tasks))
        .route("/scanner", get(scanner))
        .route("/risk", get(risk))
        .route("/settings", get(runtime_settings))
        .route("/executions", get(executions))
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(
            access_state.clone(),
            authorize,
        ));
    // The readiness probe stays unauthenticated so a supervisor can reach it,
    // but it is still rate limited: without that it is an unauthenticated
    // route any local process could hammer.
    let readiness = Router::new()
        .route("/health", get(health))
        .layer(middleware::from_fn_with_state(access_state, rate_limit));
    Router::new()
        .merge(readiness)
        .merge(protected)
        .with_state(state)
}

/// Reports process readiness without reading the journal.
///
/// This is the only unauthenticated route. It answers from the static
/// capability manifest, so an unauthenticated caller can neither observe
/// operational data nor force a journal projection.
async fn health(State(state): State<ApiState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        schema_version: API_SCHEMA_VERSION,
        status: HealthStatus::Ready,
        live_trading_enabled: state.control_plane.capabilities().live_trading_enabled,
    })
}

async fn capabilities(State(state): State<ApiState>) -> Json<CapabilityManifest> {
    Json(state.control_plane.capabilities().clone())
}

async fn system(State(state): State<ApiState>) -> Result<Json<SystemResponse>, ApiError> {
    let snapshot = load_snapshot(state.control_plane).await?;
    Ok(Json(SystemResponse::from_snapshot(
        snapshot,
        state.authentication_required,
    )))
}

async fn monitor(
    State(state): State<ApiState>,
) -> Result<Json<ArbitrageMonitorReadModel>, ApiError> {
    let snapshot = load_snapshot(state.control_plane).await?;
    Ok(Json(snapshot.monitor))
}

async fn alerts(State(state): State<ApiState>) -> Result<Json<PriceAlertReadModel>, ApiError> {
    let snapshot = load_snapshot(state.control_plane).await?;
    Ok(Json(snapshot.alerts))
}

async fn tasks(State(state): State<ApiState>) -> Result<Json<ReadOnlyTaskReadModel>, ApiError> {
    let snapshot = load_snapshot(state.control_plane).await?;
    Ok(Json(snapshot.tasks))
}

async fn scanner(
    State(state): State<ApiState>,
) -> Result<Json<VirtualGridScannerReadModel>, ApiError> {
    let snapshot = load_snapshot(state.control_plane).await?;
    Ok(Json(snapshot.scanner))
}

async fn risk(State(state): State<ApiState>) -> Result<Json<PaperAccountReadModel>, ApiError> {
    let snapshot = load_snapshot(state.control_plane).await?;
    Ok(Json(snapshot.paper_accounts))
}

async fn runtime_settings(State(state): State<ApiState>) -> Json<SettingsResponse> {
    Json((*state.settings).clone())
}

async fn executions(
    State(state): State<ApiState>,
    query: Result<Query<ExecutionsQuery>, QueryRejection>,
) -> Result<Json<ExecutionsResponse>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::invalid_query())?;
    if query
        .cursor
        .as_ref()
        .is_some_and(|cursor| cursor.len() > MAX_CURSOR_QUERY_BYTES)
    {
        return Err(ApiError::from_failure(ReadFailureKind::InvalidCursor));
    }
    let control_plane = state.control_plane;
    let response = tokio::task::spawn_blocking(move || {
        let read = control_plane
            .snapshot_with_events_after(query.cursor.as_deref())
            .map_err(|error| error.kind())?;
        Ok::<_, ReadFailureKind>(ExecutionsResponse {
            schema_version: API_SCHEMA_VERSION,
            operator: read.snapshot.operator,
            changes: read.events,
        })
    })
    .await
    .map_err(|_| ApiError::internal())?
    .map_err(ApiError::from_failure)?;
    Ok(Json(response))
}

async fn events(
    State(state): State<ApiState>,
    headers: HeaderMap,
    query: Result<Query<EventsQuery>, QueryRejection>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::invalid_query())?;
    let cursor = resolve_resume_cursor(query.cursor, &headers)?;
    let first_page = load_events(state.control_plane.clone(), cursor.clone()).await?;
    Ok(Sse::new(operation_event_stream(
        state.control_plane,
        cursor,
        first_page,
    ))
    .keep_alive(
        KeepAlive::new()
            .interval(EVENT_KEEP_ALIVE_INTERVAL)
            .text("keep-alive"),
    ))
}

fn resolve_resume_cursor(
    query_cursor: Option<String>,
    headers: &HeaderMap,
) -> Result<Option<String>, ApiError> {
    let mut header_values = headers.get_all(LAST_EVENT_ID_HEADER).iter();
    let header_cursor = header_values
        .next()
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| ApiError::invalid_query())?
        .filter(|value| !value.is_empty());
    if header_values.next().is_some() {
        return Err(ApiError::invalid_query());
    }
    let cursor = match (query_cursor, header_cursor) {
        (Some(query), Some(header)) if query != header => {
            return Err(ApiError::invalid_query());
        }
        (Some(query), _) => Some(query),
        (None, header) => header,
    };
    if cursor
        .as_ref()
        .is_some_and(|value| value.len() > MAX_CURSOR_QUERY_BYTES)
    {
        return Err(ApiError::from_failure(ReadFailureKind::InvalidCursor));
    }
    Ok(cursor)
}

fn operation_event_stream(
    control_plane: Arc<ReadControlPlane>,
    cursor: Option<String>,
    first_page: ControlPlaneEventsPage,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let state = EventStreamState {
        control_plane,
        cursor,
        pending: Some(first_page),
        sent_initial: false,
        poll_immediately: true,
        terminal: false,
    };
    stream::unfold(state, |mut state| async move {
        if state.terminal {
            return None;
        }
        loop {
            let page = if let Some(page) = state.pending.take() {
                page
            } else {
                if !state.poll_immediately {
                    tokio::time::sleep(EVENT_POLL_INTERVAL).await;
                }
                match load_events(state.control_plane.clone(), state.cursor.clone()).await {
                    Ok(page) => page,
                    Err(error) => {
                        state.terminal = true;
                        return Some((Ok(error.sse_event()), state));
                    }
                }
            };
            state.cursor.clone_from(&page.next_cursor);
            state.poll_immediately = page.has_more_in_snapshot();
            let should_emit = !state.sent_initial || !page.events.is_empty();
            state.sent_initial = true;
            if !should_emit {
                continue;
            }
            let event = if let Ok(event) = operation_page_event(&page) {
                event
            } else {
                state.terminal = true;
                ApiError::internal().sse_event()
            };
            return Some((Ok(event), state));
        }
    })
}

fn operation_page_event(page: &ControlPlaneEventsPage) -> Result<Event, axum::Error> {
    let mut event = Event::default().event("operation_page");
    if let Some(cursor) = page.next_cursor.as_deref() {
        if cursor
            .as_bytes()
            .iter()
            .any(|byte| matches!(byte, b'\0' | b'\r' | b'\n'))
        {
            return Err(axum::Error::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "control-plane cursor is not SSE-safe",
            )));
        }
        event = event.id(cursor);
    }
    event.json_data(page)
}

async fn load_events(
    control_plane: Arc<ReadControlPlane>,
    cursor: Option<String>,
) -> Result<ControlPlaneEventsPage, ApiError> {
    tokio::task::spawn_blocking(move || {
        control_plane
            .events_after(cursor.as_deref())
            .map_err(|error| error.kind())
    })
    .await
    .map_err(|_| ApiError::internal())?
    .map_err(ApiError::from_failure)
}

async fn load_snapshot(
    control_plane: Arc<ReadControlPlane>,
) -> Result<ControlPlaneSnapshot, ApiError> {
    tokio::task::spawn_blocking(move || control_plane.snapshot())
        .await
        .map_err(|_| ApiError::internal())?
        .map_err(|error| ApiError::from_failure(error.kind()))
}

async fn authorize(State(state): State<ApiAccessState>, request: Request, next: Next) -> Response {
    if !state.access.authorizes(&request) {
        let mut response = ApiError::unauthorized().into_response();
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    if let Err(retry_after) = state.rate_limiter.try_acquire() {
        let mut response = ApiError::rate_limited().into_response();
        if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        return response;
    }
    next.run(request).await
}

/// Applies backpressure without requiring authentication.
///
/// Used for the readiness probe, which a supervisor must be able to reach
/// before it holds a token.
async fn rate_limit(State(state): State<ApiAccessState>, request: Request, next: Next) -> Response {
    if let Err(retry_after) = state.rate_limiter.try_acquire() {
        let mut response = ApiError::rate_limited().into_response();
        if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        return response;
    }
    next.run(request).await
}

pub async fn add_security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let ui_asset = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.starts_with("text/html")
                || value.starts_with("text/css")
                || value.starts_with("text/javascript")
        });
    let content_security_policy = if ui_asset {
        UI_CONTENT_SECURITY_POLICY_VALUE
    } else {
        API_CONTENT_SECURITY_POLICY_VALUE
    };
    let headers = response.headers_mut();
    for (name, value) in [
        ("cache-control", CACHE_CONTROL_VALUE),
        ("content-security-policy", content_security_policy),
        ("permissions-policy", PERMISSIONS_POLICY_VALUE),
        ("referrer-policy", "no-referrer"),
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
    ] {
        headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    response
}

pub(crate) async fn not_found() -> impl IntoResponse {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "resource not found")
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionsQuery {
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsQuery {
    cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HealthStatus {
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthResponse {
    schema_version: u16,
    status: HealthStatus,
    live_trading_enabled: bool,
}

/// Stable system summary for the Overview shell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemResponse {
    pub schema_version: u16,
    pub product_version: String,
    pub release_stage: ReleaseStage,
    pub live_trading_enabled: bool,
    pub access_scope: AccessScope,
    pub authentication_required: bool,
    pub projection_status: ProjectionStatus,
    pub journal_id: String,
    pub head_sequence: Option<u64>,
    pub execution_batch_count: usize,
    pub recovery_required_count: usize,
    pub conflict_count: usize,
    pub warning_count: usize,
    pub truncation: ProjectionTruncation,
    pub kill_switch: OperationalSignal,
    pub market_data_freshness: OperationalSignal,
    pub adapter_health: OperationalSignal,
}

impl SystemResponse {
    fn from_snapshot(snapshot: ControlPlaneSnapshot, authentication_required: bool) -> Self {
        let recovery_required_count = snapshot
            .operator
            .batches
            .iter()
            .filter(|batch| batch.recovery != RecoveryDirective::None)
            .count();
        let conflict_count = snapshot
            .operator
            .batches
            .iter()
            .filter(|batch| batch.state == ExecutionBatchState::Conflict)
            .count();
        Self {
            schema_version: API_SCHEMA_VERSION,
            product_version: snapshot.capabilities.product_version,
            release_stage: snapshot.capabilities.release_stage,
            live_trading_enabled: snapshot.capabilities.live_trading_enabled,
            access_scope: AccessScope::Loopback,
            authentication_required,
            projection_status: snapshot.operator.projection_status,
            journal_id: snapshot.operator.journal_id.to_string(),
            head_sequence: snapshot.operator.head_sequence,
            execution_batch_count: snapshot.operator.batches.len(),
            recovery_required_count,
            conflict_count,
            warning_count: snapshot.operator.warnings.len(),
            truncation: ProjectionTruncation {
                batches: snapshot.operator.batches_truncated,
                warnings: snapshot.operator.warnings_truncated,
            },
            kill_switch: OperationalSignal::NotAvailable,
            market_data_freshness: OperationalSignal::NotAvailable,
            adapter_health: OperationalSignal::NotAvailable,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessScope {
    Loopback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalSignal {
    NotAvailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionTruncation {
    pub batches: bool,
    pub warnings: bool,
}

/// Full bounded execution projection plus a cursor-bearing change watermark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionsResponse {
    pub schema_version: u16,
    pub operator: OperatorReadModel,
    pub changes: ControlPlaneEventsPage,
}

#[derive(Clone, Copy, Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    const fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }

    const fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "authentication_required",
            "valid bearer authentication is required",
        )
    }

    const fn invalid_query() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_query",
            "query parameters are invalid",
        )
    }

    const fn rate_limited() -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "the local API request limit was reached",
        )
    }

    const fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the request could not be completed",
        )
    }

    const fn from_failure(kind: ReadFailureKind) -> Self {
        match kind {
            ReadFailureKind::InvalidCursor => Self::new(
                StatusCode::BAD_REQUEST,
                "invalid_cursor",
                "the event cursor is invalid",
            ),
            ReadFailureKind::ExpiredCursor => Self::new(
                StatusCode::GONE,
                "cursor_expired",
                "the event cursor no longer matches this journal generation",
            ),
            ReadFailureKind::SourceUnavailable => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "journal_unavailable",
                "the operation journal is temporarily unavailable",
            ),
            ReadFailureKind::ResourceLimit => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "read_limit_exceeded",
                "the bounded operator view cannot represent this source",
            ),
            ReadFailureKind::InvalidJournal => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "journal_invalid",
                "the operation journal failed integrity validation",
            ),
        }
    }

    fn sse_event(self) -> Event {
        Event::default()
            .event("stream_error")
            .json_data(ErrorEnvelope {
                schema_version: API_SCHEMA_VERSION,
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            })
            .unwrap_or_else(|_| {
                Event::default().event("stream_error").data(
                    r#"{"schema_version":1,"error":{"code":"internal_error","message":"the event stream could not continue"}}"#,
                )
            })
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                schema_version: API_SCHEMA_VERSION,
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    schema_version: u16,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

struct EventStreamState {
    control_plane: Arc<ReadControlPlane>,
    cursor: Option<String>,
    pending: Option<ControlPlaneEventsPage>,
    sent_initial: bool,
    poll_immediately: bool,
    terminal: bool,
}

fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
    if expected.len() != supplied.len() {
        return false;
    }
    expected
        .iter()
        .zip(supplied)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}
