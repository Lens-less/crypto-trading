//! Authenticated HTTP transport for the trusted submit composition root.
//!
//! This module intentionally lives outside the read-only `web` adapter. It
//! owns the only POST route, binds it to loopback, and requires a bearer token
//! before a request body is read or deserialized.

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{
        HeaderValue, StatusCode,
        header::{
            AUTHORIZATION, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, WWW_AUTHENTICATE,
        },
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use crypto_trading_control_plane::{
    ReadControlPlane, SubmitEnvelope, SubmitFailureKind, SubmitPermission, SubmitReceipt,
    SubmitRole, SubmitService, SubmitStatus, SubmitValidationError,
};
use crypto_trading_web::{WebAccessPolicy, WebServerConfig, app_router};
use serde::Serialize;
use tokio::net::TcpListener;

pub const MAX_TRUSTED_SUBMIT_BODY_BYTES: usize = 32 * 1024;
const SUBMIT_HTTP_SCHEMA_VERSION: u16 = 1;

#[derive(Clone)]
struct SubmitHttpState {
    service: Arc<SubmitService>,
    bearer_token: Arc<[u8]>,
    identity: TrustedSubmitIdentity,
}

/// Server-side identity and single-role grant bound to one bearer.
///
/// Envelope permission fields remain audit context. They grant no authority
/// unless they exactly match this trusted composition-root policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedSubmitIdentity {
    permission: SubmitPermission,
}

impl TrustedSubmitIdentity {
    /// Creates a paper-operator-only server identity.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitValidationError`] when `principal_id` is not a bounded
    /// identity.
    pub fn paper_operator(principal_id: impl Into<String>) -> Result<Self, SubmitValidationError> {
        Ok(Self {
            permission: SubmitPermission::new(principal_id, SubmitRole::PaperOperator)?,
        })
    }

    /// Creates a reconciler-only server identity.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitValidationError`] when `principal_id` is not a bounded
    /// identity.
    pub fn reconciler(principal_id: impl Into<String>) -> Result<Self, SubmitValidationError> {
        Ok(Self {
            permission: SubmitPermission::new(principal_id, SubmitRole::Reconciler)?,
        })
    }

    fn authorizes(&self, supplied: &SubmitPermission) -> bool {
        supplied == &self.permission
    }
}

/// Loopback listener and fully composed trusted application router.
pub struct TrustedSubmitApplication {
    listener: TcpListener,
    router: Router,
    address: SocketAddr,
}

impl TrustedSubmitApplication {
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn into_parts(self) -> (TcpListener, Router) {
        (self.listener, self.router)
    }
}

/// Binds the read control plane and trusted POST route to loopback.
///
/// The same mandatory bearer protects both read routes and `/api/v1/submit`.
/// There is deliberately no loopback-open constructor for a write-capable
/// router.
///
/// # Errors
///
/// Returns an error when the bearer policy rejects the token, the loopback
/// listener cannot bind, or its bound address cannot be inspected.
pub async fn bind_trusted_submit_app(
    port: u16,
    control_plane: Arc<ReadControlPlane>,
    service: Arc<SubmitService>,
    bearer_token: String,
    identity: TrustedSubmitIdentity,
) -> Result<TrustedSubmitApplication> {
    let read_access = WebAccessPolicy::bearer(bearer_token.clone())
        .context("trusted submit requires a valid bearer token")?;
    let read_journal_id = control_plane
        .events_after(None)
        .context("failed to verify the trusted submit read journal")?
        .journal_id;
    if read_journal_id != service.journal_id() {
        bail!("trusted submit and read control plane journal generations differ");
    }
    let listener = WebServerConfig::loopback(port)
        .bind()
        .await
        .context("failed to bind trusted submit to loopback")?;
    let address = listener
        .local_addr()
        .context("failed to inspect trusted submit loopback address")?;
    let state = SubmitHttpState {
        service,
        bearer_token: Arc::from(bearer_token.into_bytes()),
        identity,
    };
    let submit_router = Router::new()
        .route("/api/v1/submit", post(submit))
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, authorize_submit));
    let router = app_router(control_plane, read_access).merge(submit_router);
    Ok(TrustedSubmitApplication {
        listener,
        router,
        address,
    })
}

async fn authorize_submit(
    State(state): State<SubmitHttpState>,
    request: Request,
    next: Next,
) -> Response {
    if supplied_bearer(request.headers())
        .is_some_and(|supplied| constant_time_eq(state.bearer_token.as_ref(), supplied.as_bytes()))
    {
        return next.run(request).await;
    }
    let mut response = error_response(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "bearer authentication required",
    );
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

fn supplied_bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let supplied = values.next()?.to_str().ok()?.strip_prefix("Bearer ")?;
    if values.next().is_some() {
        return None;
    }
    Some(supplied)
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

async fn submit(State(state): State<SubmitHttpState>, request: Request<Body>) -> Response {
    if !is_json_content_type(request.headers()) {
        return error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "content type must be application/json",
        );
    }
    let Ok(body) = to_bytes(request.into_body(), MAX_TRUSTED_SUBMIT_BODY_BYTES).await else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "submit body exceeds the bounded request limit",
        );
    };
    let Ok(envelope) = serde_json::from_slice::<SubmitEnvelope>(&body) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_json",
            "submit body does not match the versioned command schema",
        );
    };
    if envelope.validate().is_err() {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_envelope",
            "submit envelope failed validation",
        );
    }
    if !state.identity.authorizes(envelope.permission()) {
        return error_response(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "submit identity or role is not authorized",
        );
    }

    match state.service.submit(envelope).await {
        Ok(receipt) => receipt_response(&receipt),
        Err(error) => match error.kind() {
            SubmitFailureKind::InvalidEnvelope => error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_envelope",
                "submit envelope failed validation",
            ),
            SubmitFailureKind::Conflict => error_response(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "submit identifiers are bound to a different command",
            ),
            SubmitFailureKind::JournalUnavailable | SubmitFailureKind::InvalidJournal => {
                error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "submit_unavailable",
                    "durable submit state is unavailable",
                )
            }
        },
    }
}

fn is_json_content_type(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn receipt_response(receipt: &SubmitReceipt) -> Response {
    let status = match receipt.status() {
        SubmitStatus::Applied => StatusCode::OK,
        SubmitStatus::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
        SubmitStatus::OutcomeUnknown => StatusCode::ACCEPTED,
    };
    json_response(status, receipt)
}

#[derive(Serialize)]
struct ErrorResponse {
    schema_version: u16,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

fn error_response(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    json_response(
        status,
        &ErrorResponse {
            schema_version: SUBMIT_HTTP_SCHEMA_VERSION,
            error: ErrorBody { code, message },
        },
    )
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response {
    let mut response = (status, Json(value)).into_response();
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}
