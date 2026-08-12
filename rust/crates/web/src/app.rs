use std::sync::Arc;

use axum::{Router, middleware};
use tokio::sync::watch;

use crate::{
    ReadControlPlane, SettingsResponse, WebAccessPolicy, WebRequestRateLimiter,
    api::{
        add_security_headers, api_routes_with_settings, api_routes_with_settings_and_shutdown,
        not_found,
    },
    ui::ui_router,
};

/// Builds the full Web application router with a public shell and authenticated API.
pub fn app_router(control_plane: Arc<ReadControlPlane>, access: WebAccessPolicy) -> Router {
    let rate_limiter = WebRequestRateLimiter::default();
    app_router_with_settings(
        control_plane,
        access,
        SettingsResponse::default(),
        &rate_limiter,
    )
}

/// Builds the full application with trusted read-only deployment metadata and
/// a caller-owned request limiter shared with any merged mutation route.
pub fn app_router_with_settings(
    control_plane: Arc<ReadControlPlane>,
    access: WebAccessPolicy,
    settings: SettingsResponse,
    rate_limiter: &WebRequestRateLimiter,
) -> Router {
    Router::new()
        .merge(ui_router())
        .nest(
            "/api/v1",
            api_routes_with_settings(control_plane, access, settings, rate_limiter),
        )
        .fallback(not_found)
        .layer(middleware::from_fn(add_security_headers))
}

/// Builds the full application with deployment metadata and a lifecycle signal.
/// Event streams close promptly when `shutdown` changes to `true`.
pub fn app_router_with_settings_and_shutdown(
    control_plane: Arc<ReadControlPlane>,
    access: WebAccessPolicy,
    settings: SettingsResponse,
    rate_limiter: &WebRequestRateLimiter,
    shutdown: watch::Receiver<bool>,
) -> Router {
    Router::new()
        .merge(ui_router())
        .nest(
            "/api/v1",
            api_routes_with_settings_and_shutdown(
                control_plane,
                access,
                settings,
                rate_limiter,
                shutdown,
            ),
        )
        .fallback(not_found)
        .layer(middleware::from_fn(add_security_headers))
}
