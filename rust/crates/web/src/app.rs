use std::sync::Arc;

use axum::{Router, middleware};

use crate::{
    ReadControlPlane, WebAccessPolicy,
    api::{add_security_headers, api_routes, not_found},
    ui::ui_router,
};

/// Builds the full Web application router with a public shell and authenticated API.
pub fn app_router(control_plane: Arc<ReadControlPlane>, access: WebAccessPolicy) -> Router {
    Router::new()
        .merge(ui_router())
        .nest("/api/v1", api_routes(control_plane, access))
        .fallback(not_found)
        .layer(middleware::from_fn(add_security_headers))
}
