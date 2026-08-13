//! First-layer UI contract: guarantees that hold no matter which bundle was
//! embedded at compile time.
//!
//! The tests run in two modes. With `frontend/dist/` present at build time
//! they audit the real Vite bundle; without it they audit the placeholder
//! shell. Both modes must satisfy every security assertion — only the
//! mode-specific markers differ.

use std::{collections::BTreeSet, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, Response, StatusCode, header::CONTENT_TYPE},
};
use crypto_trading_control_plane::ReadControlPlane;
use crypto_trading_runtime::MemoryJournalSnapshotSource;
use crypto_trading_web::{WebAccessPolicy, app_router, embedded_ui_assets, ui_assets_embedded};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";
const SHELL_PATHS: &[&str] = &[
    "/",
    "/overview",
    "/executions",
    "/integrations",
    "/strategies",
    "/risk",
    "/replay",
    "/settings",
];
const WRITE_METHODS: &[Method] = &[Method::POST, Method::PUT, Method::PATCH, Method::DELETE];

/// Inert URL prefixes (scheme stripped) that are allowed to appear inside
/// embedded files. None of them is ever fetched by the browser:
/// - `www.w3.org/` — XML namespace identifier constants in the React DOM
///   renderer (`xmlns`, SVG/MathML namespaces).
/// - `reactjs.org/` — the production error-decoder link React embeds in
///   thrown error messages.
/// - `json-schema.org/` — `$schema` identifier strings emitted by Zod's
///   JSON-Schema converter.
/// - `[` — Zod's IPv6 validity probe `new URL("http://[...]")`, a parser
///   round-trip on a literal, not a request.
/// - `tailwindcss.com` — the Tailwind license banner comment in the CSS.
///
/// Anything else is treated as an external reference and fails the test.
/// Runtime egress is independently blocked by `connect-src 'self'`.
const INERT_URL_PREFIXES: &[&str] = &[
    "www.w3.org/",
    "reactjs.org/",
    "json-schema.org/",
    "[",
    "tailwindcss.com",
];

/// Control surfaces that must never ship to the browser.
const FORBIDDEN_SURFACES: &[&str] = &[
    "document.cookie",
    "live-enable",
    "live_enable",
    "order-submit",
    "order_submit",
    "reconcile_release",
];

#[tokio::test]
async fn semantic_routes_serve_the_shell_with_ui_csp_and_no_store() {
    let app = fixture_app(WebAccessPolicy::loopback_open());

    for path in SHELL_PATHS {
        let response = app
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_security_headers(&response);
        assert_ui_csp(&response);
        assert_content_type_prefix(&response, "text/html");

        let shell = response_text(response).await;
        let lower = shell.to_ascii_lowercase();
        assert!(
            lower.contains("<!doctype html") || lower.contains("<html"),
            "expected an HTML shell at {path}"
        );
        assert!(
            !shell.contains("<style") && !shell.contains("style=\""),
            "inline styles at {path} would violate the UI CSP"
        );
        if ui_assets_embedded() {
            assert!(
                shell.contains("/assets/") && shell.contains("theme-init.js"),
                "embedded shell at {path} must reference the built bundle"
            );
        } else {
            assert!(
                shell.contains("UI 资产未构建") && shell.contains("API 正常服务"),
                "placeholder shell at {path} must explain the missing bundle"
            );
        }
    }
}

#[tokio::test]
async fn bearer_protects_the_api_but_the_data_free_shell_never_leaks_the_token() {
    let app = fixture_app(WebAccessPolicy::bearer(TOKEN).unwrap());

    let unauthorized = app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/system"))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized.headers()["www-authenticate"], "Bearer");
    assert_security_headers(&unauthorized);
    assert_api_csp(&unauthorized);

    for path in SHELL_PATHS {
        let response = app
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let shell = response_text(response).await;
        assert!(
            !shell.contains(TOKEN),
            "shell must not leak the bearer token at {path}"
        );
    }
}

#[tokio::test]
async fn shell_asset_references_are_same_origin_and_served_with_expected_mime() {
    let app = fixture_app(WebAccessPolicy::loopback_open());
    let shell = response_text(
        app.clone()
            .oneshot(request(Method::GET, "/"))
            .await
            .unwrap(),
    )
    .await;
    let asset_refs = shell_asset_refs(&shell);
    if ui_assets_embedded() {
        assert!(
            asset_refs.iter().any(|value| has_extension(value, "js"))
                && asset_refs.iter().any(|value| has_extension(value, "css")),
            "embedded shell must reference at least one script and one stylesheet: {asset_refs:?}"
        );
    }

    for asset_ref in asset_refs {
        assert!(
            !asset_ref.starts_with("http://")
                && !asset_ref.starts_with("https://")
                && !asset_ref.starts_with("//"),
            "external asset reference is not allowed: {asset_ref}"
        );
        let uri = if asset_ref.starts_with('/') {
            asset_ref.clone()
        } else {
            format!("/{}", asset_ref.trim_start_matches("./"))
        };
        let response = app
            .clone()
            .oneshot(request(Method::GET, &uri))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{asset_ref}");
        assert_security_headers(&response);
        if let Some(expected) = expected_mime_prefix(&asset_ref) {
            assert_content_type_prefix(&response, expected);
        }
    }
}

#[tokio::test]
async fn every_embedded_file_is_served_byte_identical_with_correct_mime() {
    let app = fixture_app(WebAccessPolicy::loopback_open());
    let assets = embedded_ui_assets();
    assert!(!assets.is_empty(), "the shell document is always embedded");
    if ui_assets_embedded() {
        assert!(
            assets.iter().any(|(route, _)| route == "/theme-init.js"),
            "the CSP-compatible theme bootstrap must ship as a standalone file"
        );
        assert!(
            assets
                .iter()
                .any(|(route, _)| route.starts_with("/assets/") && has_extension(route, "js")),
            "the hashed Vite entry chunk must be embedded"
        );
    }

    for (route, bytes) in assets {
        let response = app
            .clone()
            .oneshot(request(Method::GET, &route))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{route}");
        assert_security_headers(&response);
        if let Some(expected) = expected_mime_prefix(&route) {
            assert_content_type_prefix(&response, expected);
        }
        if route == "/" {
            assert_content_type_prefix(&response, "text/html");
        }
        let body = to_bytes(response.into_body(), 8 * 1_048_576).await.unwrap();
        assert_eq!(
            body.as_ref(),
            bytes,
            "{route} must serve the embedded bytes unchanged"
        );
    }
}

#[tokio::test]
async fn embedded_files_reference_no_external_origins() {
    for (route, bytes) in embedded_ui_assets() {
        let text = String::from_utf8_lossy(bytes);
        for marker in ["//cdn", "//fonts", "@import url(http"] {
            assert!(
                !text.contains(marker),
                "{route} contains external reference marker {marker}"
            );
        }
        if route == "/" || has_extension(&route, "html") {
            // Documents must be entirely same-origin; not even inert
            // identifier URLs are acceptable in markup.
            assert!(
                !text.contains("http://") && !text.contains("https://"),
                "{route} must not contain any absolute URL"
            );
            continue;
        }
        assert_only_inert_urls(&route, &text);
    }
}

#[tokio::test]
async fn embedded_files_expose_no_forbidden_control_surface() {
    for (route, bytes) in embedded_ui_assets() {
        let text = String::from_utf8_lossy(bytes);
        for forbidden in FORBIDDEN_SURFACES {
            assert!(
                !text.contains(forbidden),
                "{route} contains forbidden surface {forbidden}"
            );
        }
    }
}

#[tokio::test]
async fn write_methods_are_rejected_and_unknown_routes_fail_closed() {
    let app = fixture_app(WebAccessPolicy::loopback_open());

    let mut write_targets: Vec<String> = SHELL_PATHS.iter().map(ToString::to_string).collect();
    write_targets.extend(
        ["/api/v1/system", "/api/v1/executions", "/api/v1/events"]
            .iter()
            .map(ToString::to_string),
    );
    write_targets.extend(
        embedded_ui_assets()
            .into_iter()
            .map(|(route, _)| route)
            .filter(|route| route != "/"),
    );
    for path in write_targets {
        for method in WRITE_METHODS {
            let response = app
                .clone()
                .oneshot(request(method.clone(), &path))
                .await
                .unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_FOUND
                ),
                "{method} {path} unexpectedly returned {}",
                response.status()
            );
            assert_security_headers(&response);
        }
    }

    for path in [
        "/totally-unknown",
        "/assets/does-not-exist.js",
        "/index.html",
    ] {
        let unknown = app
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND, "{path}");
        assert_security_headers(&unknown);
        assert_api_csp(&unknown);
        assert_content_type_prefix(&unknown, "application/json");
        assert_eq!(response_json(unknown).await["error"]["code"], "not_found");
    }
}

fn assert_only_inert_urls(route: &str, text: &str) {
    for scheme in ["http://", "https://"] {
        let mut rest = text;
        while let Some(index) = rest.find(scheme) {
            let after = &rest[index + scheme.len()..];
            assert!(
                INERT_URL_PREFIXES
                    .iter()
                    .any(|prefix| after.starts_with(prefix)),
                "{route} contains a non-inert external URL: {scheme}{}",
                after.chars().take(60).collect::<String>()
            );
            rest = after;
        }
    }
}

fn fixture_app(access: WebAccessPolicy) -> Router {
    let source = MemoryJournalSnapshotSource::new(fixed_uuid(7), Vec::new()).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();
    app_router(Arc::new(control_plane), access)
}

fn request(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn assert_security_headers(response: &Response<Body>) {
    let headers = response.headers();
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(headers["referrer-policy"], "no-referrer");
    assert_eq!(headers["x-frame-options"], "DENY");
    assert!(headers["permissions-policy"].to_str().is_ok());

    let csp = headers["content-security-policy"].to_str().unwrap();
    assert!(
        csp.contains("frame-ancestors 'none'") && csp.contains("base-uri 'none'"),
        "missing CSP hardening: {csp}"
    );
    assert!(
        !csp.contains("http:") && !csp.contains("https:") && !csp.contains("//"),
        "CSP must stay same-origin only: {csp}"
    );
}

fn assert_ui_csp(response: &Response<Body>) {
    let csp = response.headers()["content-security-policy"]
        .to_str()
        .unwrap();
    for directive in [
        "style-src 'self'",
        "script-src 'self'",
        "connect-src 'self'",
    ] {
        assert!(
            csp.contains(directive),
            "UI CSP is missing {directive}: {csp}"
        );
    }
}

fn assert_api_csp(response: &Response<Body>) {
    let csp = response.headers()["content-security-policy"]
        .to_str()
        .unwrap();
    assert!(
        csp.contains("default-src 'none'"),
        "API CSP is not closed: {csp}"
    );
    for directive in ["style-src", "script-src", "connect-src"] {
        assert!(
            !csp.contains(directive),
            "API response unnecessarily expands {directive}: {csp}"
        );
    }
}

fn assert_content_type_prefix(response: &Response<Body>, expected_prefix: &str) {
    let content_type = response.headers()[CONTENT_TYPE].to_str().unwrap();
    assert!(
        content_type.starts_with(expected_prefix),
        "expected content type {expected_prefix}, got {content_type}"
    );
}

fn shell_asset_refs(shell: &str) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    refs.extend(attribute_values(shell, "href"));
    refs.extend(attribute_values(shell, "src"));
    refs.retain(|value| {
        !value.starts_with('#')
            && !value.starts_with("mailto:")
            && !value.starts_with("javascript:")
            && (value.contains('.') || value.contains("/assets/"))
    });
    refs
}

fn attribute_values(shell: &str, attribute: &str) -> BTreeSet<String> {
    let mut values = BTreeSet::new();
    let needle = format!("{attribute}=\"");
    let mut rest = shell;
    while let Some(start) = rest.find(&needle) {
        let after = &rest[start + needle.len()..];
        let Some(end) = after.find('"') else {
            break;
        };
        values.insert(after[..end].to_owned());
        rest = &after[end + 1..];
    }
    values
}

/// Case-insensitive extension check that satisfies clippy's
/// `case_sensitive_file_extension_comparisons` pedantic lint.
fn has_extension(path: &str, extension: &str) -> bool {
    path.rsplit_once('.')
        .is_some_and(|(_, found)| found.eq_ignore_ascii_case(extension))
}

fn expected_mime_prefix(reference: &str) -> Option<&'static str> {
    let path = reference.split('?').next().unwrap_or(reference);
    let extension = path.rsplit_once('.').map_or("", |(_, extension)| extension);
    match extension.to_ascii_lowercase().as_str() {
        "css" => Some("text/css"),
        "js" | "mjs" => Some("text/javascript"),
        "html" => Some("text/html"),
        "svg" => Some("image/svg+xml"),
        "ico" => Some("image/"),
        "woff2" => Some("font/woff2"),
        "json" | "webmanifest" => Some("application/"),
        _ => None,
    }
}

async fn response_text(response: Response<Body>) -> String {
    let bytes = to_bytes(response.into_body(), 8 * 1_048_576).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn response_json(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}
