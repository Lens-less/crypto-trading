use axum::{
    Router,
    http::{HeaderValue, StatusCode, header::HeaderName},
    response::{Html, IntoResponse, Response},
    routing::get,
};

const TEXT_CSS_UTF8: &str = "text/css; charset=utf-8";
const TEXT_JAVASCRIPT_UTF8: &str = "text/javascript; charset=utf-8";

const INDEX_HTML: &str = include_str!("../assets/index.html");
const APP_CSS: &str = include_str!("../assets/app.css");
const APP_JS: &str = include_str!("../assets/app.js");

pub(crate) fn ui_router() -> Router {
    Router::new()
        .route("/", get(shell))
        .route("/overview", get(shell))
        .route("/scanner", get(shell))
        .route("/alerts", get(shell))
        .route("/executions", get(shell))
        .route("/integrations", get(shell))
        .route("/strategies", get(shell))
        .route("/risk", get(shell))
        .route("/replay", get(shell))
        .route("/settings", get(shell))
        .route("/assets/app.css", get(app_css))
        .route("/assets/app.js", get(app_js))
}

async fn shell() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_css() -> Response {
    static_asset(APP_CSS, TEXT_CSS_UTF8)
}

async fn app_js() -> Response {
    static_asset(APP_JS, TEXT_JAVASCRIPT_UTF8)
}

fn static_asset(body: &'static str, content_type: &'static str) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static(content_type),
    );
    response
}
