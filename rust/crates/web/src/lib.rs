//! Read-only HTTP adapter for the operator control plane.
//!
//! The adapter owns transport concerns only: loopback binding, optional bearer
//! authentication, safe error mapping, and response headers. It receives a
//! constructed [`ReadControlPlane`] and has no filesystem or execution API.

mod api;
mod app;
mod server;
mod ui;

pub use api::{
    CredentialConfiguration, CredentialSettings, ExecutionsResponse, NotificationEvidence,
    PaperProfileKind, PaperProfileSettings, RequestLimitSettings, RuntimeLogSink,
    SETTINGS_SCHEMA_VERSION, SettingsResponse, SystemResponse, WEB_REQUEST_LIMIT_PER_MINUTE,
    WebAccessPolicy, WebAccessPolicyError, WebRequestRateLimiter, add_security_headers, api_router,
    api_router_with_settings, api_router_with_settings_and_shutdown, api_router_with_shutdown,
};
pub use app::{app_router, app_router_with_settings, app_router_with_settings_and_shutdown};
pub use crypto_trading_control_plane::ReadControlPlane;
pub use server::{DEFAULT_WEB_PORT, WebServerConfig, WebServerConfigError, serve_with_shutdown};
pub use ui::{embedded_ui_assets, ui_assets_embedded};
