//! Serves the operator UI that is compiled into the binary.
//!
//! When the React bundle in `frontend/dist/` exists at compile time, the
//! build script sets the `ct_ui_embedded` cfg and the whole bundle is
//! embedded through `include_dir!`. Without a bundle the router serves a
//! minimal placeholder shell so the read-only API keeps working. Either way
//! the crate has no runtime filesystem access: every served byte is fixed at
//! compile time and every route is registered explicitly, so unknown paths
//! keep failing closed through the JSON 404 fallback.

use axum::{
    Router,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};

/// Semantic operator routes. Each one serves the single-page shell document
/// so a browser refresh on a client-side route keeps working (SPA fallback
/// limited to this fixed list, never a wildcard).
const SEMANTIC_SHELL_PATHS: &[&str] = &[
    "/",
    "/overview",
    "/scanner",
    "/alerts",
    "/executions",
    "/integrations",
    "/strategies",
    "/risk",
    "/replay",
    "/settings",
];

const TEXT_HTML_UTF8: &str = "text/html; charset=utf-8";

#[cfg(ct_ui_embedded)]
static UI_BUNDLE: include_dir::Dir<'static> = include_dir::include_dir!("$CT_UI_DIST_DIR");

/// Shell served when the binary was compiled without `frontend/dist/`.
///
/// It carries no inline styles or scripts (the UI CSP forbids both) and no
/// external references; it only states that the UI bundle is missing while
/// the API remains available.
const PLACEHOLDER_SHELL: &str = "<!doctype html>\n\
<html lang=\"zh-CN\">\n\
  <head>\n\
    <meta charset=\"utf-8\">\n\
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n\
    <title>Crypto Trading 控制面(UI 资产未构建)</title>\n\
  </head>\n\
  <body>\n\
    <main>\n\
      <h1>UI 资产未构建</h1>\n\
      <p>此二进制在编译时没有找到 frontend/dist/,因此未嵌入操作界面;只读 API 正常服务。</p>\n\
      <p>如需完整界面:在 frontend/ 目录执行 pnpm install 与 pnpm build,然后重新编译本程序。</p>\n\
      <p>就绪探针:<a href=\"/api/v1/health\">/api/v1/health</a></p>\n\
    </main>\n\
  </body>\n\
</html>\n";

pub(crate) fn ui_router() -> Router {
    let mut router = Router::new();
    for path in SEMANTIC_SHELL_PATHS {
        router = router.route(path, get(shell));
    }
    // Every embedded file gets an explicit route. Anything not on this list
    // stays a fail-closed JSON 404 through the application fallback.
    for (route, bytes) in embedded_ui_assets() {
        if route == "/" {
            continue;
        }
        let content_type = static_content_type(&route);
        router = router.route(
            &route,
            get(move || std::future::ready(static_asset(bytes, content_type))),
        );
    }
    router
}

/// Reports whether a built frontend bundle was embedded at compile time.
#[must_use]
pub const fn ui_assets_embedded() -> bool {
    cfg!(ct_ui_embedded)
}

/// Route path and raw bytes for the shell document (`/`) and every embedded
/// static asset.
///
/// Exposed so contract tests can audit the exact bytes the router serves
/// (external references, forbidden control surfaces) without giving the
/// crate any filesystem capability.
#[must_use]
pub fn embedded_ui_assets() -> Vec<(String, &'static [u8])> {
    let mut assets = vec![(String::from("/"), shell_document())];
    #[cfg(ct_ui_embedded)]
    collect_bundle_files(&UI_BUNDLE, &mut assets);
    assets
}

fn shell_document() -> &'static [u8] {
    #[cfg(ct_ui_embedded)]
    {
        // The build script only sets the cfg when index.html exists, so the
        // placeholder arm is unreachable in practice but keeps this total.
        UI_BUNDLE
            .get_file("index.html")
            .map_or(PLACEHOLDER_SHELL.as_bytes(), include_dir::File::contents)
    }
    #[cfg(not(ct_ui_embedded))]
    {
        PLACEHOLDER_SHELL.as_bytes()
    }
}

#[cfg(ct_ui_embedded)]
fn collect_bundle_files(
    dir: &include_dir::Dir<'static>,
    assets: &mut Vec<(String, &'static [u8])>,
) {
    for entry in dir.entries() {
        match entry {
            include_dir::DirEntry::Dir(subdir) => collect_bundle_files(subdir, assets),
            include_dir::DirEntry::File(file) => {
                // include_dir records paths with the separators of the build
                // host; routes are always forward-slash.
                let route = format!("/{}", file.path().to_string_lossy().replace('\\', "/"));
                if route == "/index.html" {
                    // Served through the semantic shell routes instead.
                    continue;
                }
                assets.push((route, file.contents()));
            }
        }
    }
}

async fn shell() -> Response {
    static_asset(shell_document(), TEXT_HTML_UTF8)
}

/// Maps an asset path to its response content type by file extension.
fn static_content_type(path: &str) -> &'static str {
    let extension = path.rsplit_once('.').map_or("", |(_, extension)| extension);
    match extension.to_ascii_lowercase().as_str() {
        "html" => TEXT_HTML_UTF8,
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "json" | "webmanifest" => "application/json",
        _ => "application/octet-stream",
    }
}

fn static_asset(body: &'static [u8], content_type: &'static str) -> Response {
    let mut response = (StatusCode::OK, body).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}
