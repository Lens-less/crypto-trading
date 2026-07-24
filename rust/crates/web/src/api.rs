use std::{convert::Infallible, fmt, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Query, Request, State, rejection::QueryRejection},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE},
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
    ExecutionBatchState, OperatorReadModel, ProjectionStatus, ReadControlPlane, ReadFailureKind,
    RecoveryDirective, ReleaseStage,
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

const CACHE_CONTROL_VALUE: &str = "no-store";
const API_CONTENT_SECURITY_POLICY_VALUE: &str =
    "default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const UI_CONTENT_SECURITY_POLICY_VALUE: &str = "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-src 'none'; frame-ancestors 'none'; object-src 'none'";
const PERMISSIONS_POLICY_VALUE: &str = "camera=(), microphone=(), geolocation=()";

#[derive(Clone)]
struct ApiState {
    control_plane: Arc<ReadControlPlane>,
    authentication_required: bool,
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

/// Builds the read-only API router. The resulting router has no missing state.
pub fn api_router(control_plane: Arc<ReadControlPlane>, access: WebAccessPolicy) -> Router {
    Router::new()
        .nest("/api/v1", api_routes(control_plane, access))
        .fallback(not_found)
        .layer(middleware::from_fn(add_security_headers))
}

pub(crate) fn api_routes(control_plane: Arc<ReadControlPlane>, access: WebAccessPolicy) -> Router {
    let state = ApiState {
        control_plane,
        authentication_required: access.authentication_required(),
    };
    Router::new()
        .route("/system", get(system))
        .route("/capabilities", get(capabilities))
        .route("/monitor", get(monitor))
        .route("/executions", get(executions))
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(access, authorize))
        .with_state(state)
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

async fn authorize(
    State(access): State<WebAccessPolicy>,
    request: Request,
    next: Next,
) -> Response {
    if access.authorizes(&request) {
        return next.run(request).await;
    }
    let mut response = ApiError::unauthorized().into_response();
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

pub(crate) async fn add_security_headers(request: Request, next: Next) -> Response {
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
