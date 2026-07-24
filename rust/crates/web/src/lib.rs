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
    ExecutionsResponse, SystemResponse, WebAccessPolicy, WebAccessPolicyError, api_router,
};
pub use app::app_router;
pub use crypto_trading_control_plane::ReadControlPlane;
pub use server::{DEFAULT_WEB_PORT, WebServerConfig, WebServerConfigError, serve_with_shutdown};
