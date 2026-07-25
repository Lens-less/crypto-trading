use std::{collections::BTreeSet, path::Path, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, Response, StatusCode, header::CONTENT_TYPE},
};
use crypto_trading_control_plane::ReadControlPlane;
use crypto_trading_runtime::MemoryJournalSnapshotSource;
use crypto_trading_web::{WebAccessPolicy, app_router};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";
const SHELL_PATHS: &[&str] = &[
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
const WRITE_METHODS: &[Method] = &[Method::POST, Method::PUT, Method::PATCH, Method::DELETE];

#[tokio::test]
async fn root_and_semantic_paths_serve_the_embedded_shell_with_safe_headers() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());

    for path in SHELL_PATHS {
        let response = app
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_shell_headers(&response);
        assert_ui_csp(&response);
        assert_content_type_prefix(&response, "text/html");

        let shell = response_text(response).await;
        assert_shell_markup(&shell);
        assert!(
            !shell.contains("live-enable"),
            "shell must not expose a live-enable affordance at {path}"
        );
        assert!(
            !shell.contains("order-submit"),
            "shell must not expose an order-submit affordance at {path}"
        );
    }
}

#[tokio::test]
async fn shell_assets_are_same_origin_and_serve_expected_mime_types() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());
    let shell = app
        .clone()
        .oneshot(request(Method::GET, "/"))
        .await
        .unwrap();
    let shell = response_text(shell).await;
    let asset_refs = shell_asset_refs(&shell);

    assert!(
        !asset_refs.is_empty(),
        "embedded shell should reference at least one same-origin asset"
    );

    for asset_ref in asset_refs {
        assert_same_origin_reference(&asset_ref);

        let response = app
            .clone()
            .oneshot(request(Method::GET, &resolve_asset_uri(&asset_ref)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{asset_ref}");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_ui_csp(&response);

        if let Some(expected) = expected_mime_prefix(&asset_ref) {
            assert_content_type_prefix(&response, expected);
        } else {
            assert!(
                response.headers().contains_key(CONTENT_TYPE),
                "{asset_ref} is missing a content type"
            );
        }
    }
}

#[tokio::test]
async fn embedded_assets_lock_the_operator_design_and_secret_boundary() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());
    let html = response_text(
        app.clone()
            .oneshot(request(Method::GET, "/"))
            .await
            .unwrap(),
    )
    .await;
    let css = response_text(
        app.clone()
            .oneshot(request(Method::GET, "/assets/app.css"))
            .await
            .unwrap(),
    )
    .await;
    let javascript = response_text(
        app.oneshot(request(Method::GET, "/assets/app.js"))
            .await
            .unwrap(),
    )
    .await;
    let html = html.replace("\r\n", "\n");
    let css = css.replace("\r\n", "\n");
    let javascript = javascript.replace("\r\n", "\n");

    for external in ["http://", "https://", "//cdn", "//fonts"] {
        assert!(!html.contains(external), "HTML references {external}");
        assert!(!css.contains(external), "CSS references {external}");
        assert!(
            !javascript.contains(external),
            "JavaScript references {external}"
        );
    }
    assert!(!html.contains("<style"), "inline styles violate the UI CSP");
    assert!(
        !html.contains("style="),
        "inline style attributes violate the UI CSP"
    );
    assert!(html.contains("src=\"/assets/app.js\""));
    assert!(html.contains("href=\"/assets/app.css\""));
    assert!(html.contains("class=\"initial-loading\""));
    assert!(html.contains("正在加载只读运行事实"));

    for forbidden in [
        "localStorage",
        "sessionStorage",
        "document.cookie",
        ".innerHTML",
        "insertAdjacentHTML",
        "method: \"PUT\"",
        "method: \"PATCH\"",
        "method: \"DELETE\"",
        "live-enable",
        "order-submit",
    ] {
        assert!(
            !javascript.contains(forbidden),
            "browser asset contains forbidden surface {forbidden}"
        );
    }
    for endpoint in [
        "/api/v1/system",
        "/api/v1/alerts",
        "/api/v1/tasks",
        "/api/v1/scanner",
        "/api/v1/capabilities",
        "/api/v1/executions",
        "/api/v1/events",
    ] {
        assert!(
            javascript.contains(endpoint),
            "browser asset does not consume {endpoint}"
        );
    }
    for forbidden in ["gradient", "transition: all", "font-style: italic"] {
        assert!(
            !css.contains(forbidden),
            "CSS violates the selected design system with {forbidden}"
        );
    }
    assert!(css.contains("@media (prefers-reduced-motion: reduce)"));
    assert!(javascript.contains("TOKEN_LABELS"));
    assert!(javascript.contains("不透明恢复游标只保存在页面内存"));
    assert!(javascript.contains("market_data_freshness"));
    assert_monitor_surface_contract(&javascript, &css);
    assert_alert_surface_contract(&javascript, &css);
    assert_task_surface_contract(&javascript, &css);
    assert_scanner_surface_contract(&javascript, &css);
    assert_trusted_submit_surface_contract(&javascript);
    assert_m6_information_architecture_contract(&javascript, &css);
    assert!(javascript.contains("kill_switch"));
    assert_protected_state_contract(&javascript);
}

fn assert_trusted_submit_surface_contract(javascript: &str) {
    for required in [
        "/api/v1/submit",
        "method: \"POST\"",
        "schema_version: SUBMIT_SCHEMA_VERSION",
        "local-paper-operator",
        "paper_operator",
        "paper_only",
        "start_paper_arbitrage",
        "start_paper_grid",
        "stop_task",
        "cancel_task",
        "journal_projection",
        "source",
        "outcome_unknown",
        "pendingSubmission",
        "lockedByOutcomeUnknown",
        "baselineTaskFingerprint",
        "function taskProjectionFingerprint(task)",
        "function syncStrategySubmitLocks()",
        "function validateSubmitReceipt(receipt, envelope)",
        "function updateStrategySubmitField(",
        "acceptedStatuses: [422]",
        "[\"running\", \"stopped\", \"failed\"].includes(task.phase)",
        "state.tasks?.projection_status !== \"complete\"",
        "secure_random_unavailable",
        "/api/v1/tasks",
    ] {
        assert!(
            javascript.contains(required),
            "browser asset is missing trusted submit contract {required}"
        );
    }
    for forbidden in [
        "reconciliation_evidence_verified",
        "reconcile_release",
        "record_reconcile_failure",
        "role\":\"reconciler\"",
        "live_enable",
        "order_submit",
    ] {
        assert!(
            !javascript.contains(forbidden),
            "browser asset must not expose forbidden submit surface {forbidden}"
        );
    }

    let (_, reset_tail) = javascript
        .split_once("function resetStrategySubmitTransientState()")
        .expect("transient reset helper");
    let (reset_body, _) = reset_tail
        .split_once("function latestTaskForKind(")
        .expect("latest task helper follows transient reset");
    assert!(
        !reset_body.contains("form.pendingSubmission = null"),
        "authentication changes must retain the pending envelope identity"
    );
}

fn assert_m6_information_architecture_contract(javascript: &str, css: &str) {
    for required in [
        "[\"strategies\", \"策略\"]",
        "[\"risk\", \"风险\"]",
        "[\"replay\", \"回放\"]",
        "[\"settings\", \"设置\"]",
        "function renderStrategiesView()",
        "function renderRiskView()",
        "function renderReplayView()",
        "function renderSettingsView()",
        "策略运行面",
        "只读策略证据",
        "风险总览",
        "恢复账本",
        "回放快照",
        "历史投影时间",
        "访问与外壳",
        "只读边界",
    ] {
        assert!(
            javascript.contains(required),
            "browser asset is missing M6 information architecture contract {required}"
        );
    }
    assert_eq!(
        javascript
            .matches("function renderStrategiesView()")
            .count(),
        1,
        "the strategies surface must have one authoritative renderer"
    );
    for untranslated in [
        "Strategy surfaces",
        "Risk overview",
        "Replay snapshot",
        "Access and shell",
    ] {
        assert!(
            !javascript.contains(untranslated),
            "Chinese-first operator surface retains untranslated heading {untranslated}"
        );
    }
    assert!(css.contains(".subregion {"));
    assert!(css.contains("grid-template-columns: repeat(3, minmax(0, 1fr));"));
    assert!(css.contains(
        ".confirm-row {\n  grid-template-columns: auto minmax(0, 1fr);\n  align-items: center;\n  gap: 10px;\n  min-height: 40px;"
    ));
}

fn assert_monitor_surface_contract(javascript: &str, css: &str) {
    assert!(css.contains(".detail-block {\n  min-width: 0;"));
    assert!(css.contains(
        ".metric-value {\n  font-size: 24px;\n  line-height: 1.2;\n  overflow-wrap: anywhere;"
    ));
    assert!(javascript.contains("/api/v1/monitor"));
    assert!(javascript.contains("只读套利监控"));
    assert!(javascript.contains("不代表当前实时行情仍然新鲜"));
    assert!(javascript.contains("state.monitor && projectionStatus !== \"complete\""));
    assert!(javascript.contains("监控投影已降级"));
    assert!(javascript.contains("最后一个有效结果停止展示"));
    assert!(javascript.contains("保留事实"));
    assert!(javascript.contains("function eventStreamLabel()"));
    assert!(javascript.contains("function compactEventStreamLabel()"));
    assert!(javascript.contains("已连接 / 仅通知"));
    assert!(javascript.contains("读取方式"));
    assert!(javascript.contains("历史快照"));
    for forbidden in ["return \"实时\";", "return \"新鲜 / 流式更新\";"] {
        assert!(
            !javascript.contains(forbidden),
            "execution SSE must not overstate monitor freshness with {forbidden}"
        );
    }
}

fn assert_alert_surface_contract(javascript: &str, css: &str) {
    for required in [
        "const MAX_ALERT_OCCURRENCES = 256;",
        "const TRUSTED_ALERT_PROJECTION_IDS = new Set([\"complete\", \"windowed\"]);",
        "function renderAlertsView()",
        "function alertProjectionLabel(model)",
        "function visibleAlertOccurrences(model)",
        "model.occurrences.length > MAX_ALERT_OCCURRENCES",
        "model.boundary?.kind !== \"snapshot_end\"",
        "价格预警明细，可横向滚动",
        "预警投影已窗口化",
        "可信 / 已截断",
        "预警投影已降级",
        "降级 / 契约不一致",
        "所有 occurrence 与最近预警都停止展示",
        "预警快照刷新失败",
        "保留旧快照",
        "未发生窗口截断",
        "规则定义",
        "冷却状态",
        "当前投影未提供",
        "触发 ${formatDateTime(occurrence.recorded_at)}",
        "确认 ${formatDateTime(occurrence.acknowledged_at)}",
        "formatDateTime(delivery.updated_at)",
        "state.loads.alerts === \"error\" &&\n    state.alerts &&\n    isTrustedAlertProjection(state.alerts)",
        "最后记录：未决",
        "function countPendingAlertDeliveries(model)",
        "存在未决通知记录",
        "历史事实 / 不保证重放",
        "恢复默认不重放，本页不会把它解释为仍在排队",
        "通知 worker 异常终止",
    ] {
        assert!(
            javascript.contains(required),
            "browser asset is missing alert-surface contract {required}"
        );
    }
    assert!(css.contains(".alert-table {\n  min-width: 880px;"));
    assert!(css.contains(".alert-table th {\n  white-space: nowrap;"));
    assert!(css.contains(".risk-status-block .status-list li {\n    display: grid;"));
    assert!(css.contains("grid-template-columns: repeat(3, minmax(0, 1fr));"));
}

fn assert_task_surface_contract(javascript: &str, css: &str) {
    for required in [
        "function loadTasks()",
        "function renderTasksRegion()",
        "function taskTone(task)",
        "/api/v1/tasks",
        "只读连续任务",
        "最后持久阶段、双源健康与事件计数",
        "不证明进程仍存活",
        "任务存活性未验证",
        "历史事实 / 不自动重放",
        "不会在重启后自动重连数据源",
        "任务快照刷新失败",
        "保留旧快照",
        "没有启动、停止、重连或自动恢复入口",
        "仅有历史事实；需核对进程",
        "持久终态已闭合",
    ] {
        assert!(
            javascript.contains(required),
            "browser asset is missing task-surface contract {required}"
        );
    }
    assert!(css.contains(".task-table {\n  min-width: 760px;"));
    assert!(css.contains(".task-source-line {\n  display: flex;"));
    for forbidden in ["startTask(", "stopTask(", "restartTask(", "reconnectTask("] {
        assert!(
            !javascript.contains(forbidden),
            "task surface must remain read-only: {forbidden}"
        );
    }
}

fn assert_scanner_surface_contract(javascript: &str, css: &str) {
    for required in [
        "function loadScanner()",
        "function renderScannerView()",
        "function renderScannerSummaryRegion()",
        "/api/v1/scanner",
        "确定性虚拟网格排行",
        "最后一次离线历史排行",
        "显式 benchmark 优先",
        "benchmark 是展示优先级，不是评分加成",
        "不证明 scanner 进程仍存活",
        "不证明行情仍然新鲜",
        "不是投资建议",
        "scanner 投影已降级",
        "保留最后有效历史排行",
        "排行已截断",
        "可横向滚动",
        "exact instrument",
    ] {
        assert!(
            javascript.contains(required),
            "browser asset is missing scanner-surface contract {required}"
        );
    }
    assert!(css.contains(".scanner-table {\n  min-width: 980px;"));
    assert!(css.contains(".scanner-policy-note {\n  display: grid;"));
    for forbidden in [
        "startScanner(",
        "stopScanner(",
        "restartScanner(",
        "reconnectScanner(",
        "submitScannerOrder(",
    ] {
        assert!(
            !javascript.contains(forbidden),
            "scanner surface must remain read-only: {forbidden}"
        );
    }
}

fn assert_protected_state_contract(javascript: &str) {
    for required in [
        "function clearProtectedState()",
        "state.system = null;",
        "state.alerts = null;",
        "state.tasks = null;",
        "state.scanner = null;",
        "state.capabilities = null;",
        "state.executions = null;",
        "state.lastPage = null;",
        "state.cursor = \"\";",
        "function markAuthenticationRequired(problem)",
        "function replaceAuthToken(nextToken)",
        "function findBatchActionTarget(batchId, action)",
        "state.sessionGeneration += 1;",
        "generation !== state.sessionGeneration",
    ] {
        assert!(
            javascript.contains(required),
            "browser asset is missing protected-state contract {required}"
        );
    }
    assert!(
        !javascript.contains("[data-batch-id=\"${"),
        "drawer focus restoration must not interpolate batch IDs into selectors"
    );
    assert!(
        javascript
            .matches("if (generation !== state.sessionGeneration)")
            .count()
            >= 8,
        "every protected JSON success and error path must reject stale auth sessions"
    );
}

#[tokio::test]
async fn bearer_protects_the_api_but_not_the_data_free_shell() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::bearer(TOKEN).unwrap());

    let unauthorized = app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/system"))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized.headers()["www-authenticate"], "Bearer");
    assert_shell_headers(&unauthorized);
    assert_api_csp(&unauthorized);

    for path in SHELL_PATHS {
        let response = app
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_shell_headers(&response);
        assert_ui_csp(&response);
        assert_content_type_prefix(&response, "text/html");

        let shell = response_text(response).await;
        assert_shell_markup(&shell);
        assert!(
            !shell.contains(TOKEN),
            "shell must not leak the bearer token at {path}"
        );
    }
}

#[tokio::test]
async fn app_router_remains_read_only_and_unknown_routes_fail_closed() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());

    for path in [
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
        "/api/v1/system",
        "/api/v1/monitor",
        "/api/v1/alerts",
        "/api/v1/tasks",
        "/api/v1/scanner",
        "/api/v1/capabilities",
        "/api/v1/executions",
        "/api/v1/events",
    ] {
        for method in WRITE_METHODS {
            let response = app
                .clone()
                .oneshot(request(method.clone(), path))
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
            assert_shell_headers(&response);
        }
    }

    let unknown = app
        .oneshot(request(Method::GET, "/totally-unknown"))
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    assert_shell_headers(&unknown);
    assert_api_csp(&unknown);
    assert_content_type_prefix(&unknown, "application/json");
    assert_eq!(response_json(unknown).await["error"]["code"], "not_found");
}

fn fixture_app(bytes: Vec<u8>, access: WebAccessPolicy) -> Router {
    let source = MemoryJournalSnapshotSource::new(fixed_uuid(7), bytes).unwrap();
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

fn assert_shell_headers(response: &Response<Body>) {
    let headers = response.headers();
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(headers["referrer-policy"], "no-referrer");
    assert_eq!(headers["x-frame-options"], "DENY");

    let csp = headers["content-security-policy"].to_str().unwrap();
    assert!(
        csp.contains("frame-ancestors 'none'"),
        "missing frame-ancestors hardening: {csp}"
    );
    assert!(
        csp.contains("base-uri 'none'"),
        "missing base-uri hardening: {csp}"
    );
    assert!(
        !csp.contains("http:") && !csp.contains("https:") && !csp.contains("//"),
        "CSP must stay same-origin only: {csp}"
    );

    assert!(headers["permissions-policy"].to_str().is_ok());
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

fn assert_shell_markup(shell: &str) {
    let lower = shell.to_ascii_lowercase();
    assert!(
        lower.contains("<!doctype html") || lower.contains("<html"),
        "expected HTML shell, got {shell}"
    );
    for label in ["Overview", "Alerts", "Executions", "Integrations"] {
        assert!(shell.contains(label), "missing shell label {label}");
    }
}

fn shell_asset_refs(shell: &str) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    refs.extend(attribute_values(shell, "href"));
    refs.extend(attribute_values(shell, "src"));
    refs.retain(|value| {
        !value.starts_with('#')
            && !matches!(
                value.as_str(),
                "/" | "/overview" | "/alerts" | "/executions" | "/integrations"
            )
            && !value.starts_with("mailto:")
            && !value.starts_with("javascript:")
            && (value.contains('.')
                || value.starts_with("/assets/")
                || value.starts_with("assets/"))
    });
    refs
}

fn attribute_values(shell: &str, attribute: &str) -> BTreeSet<String> {
    let mut values = BTreeSet::new();
    for quote in ['"', '\''] {
        let needle = format!("{attribute}={quote}");
        let mut rest = shell;
        while let Some(start) = rest.find(&needle) {
            let value_start = start + needle.len();
            let after = &rest[value_start..];
            let Some(value_end) = after.find(quote) else {
                break;
            };
            values.insert(after[..value_end].to_owned());
            rest = &after[value_end + 1..];
        }
    }
    values
}

fn assert_same_origin_reference(reference: &str) {
    assert!(
        !reference.starts_with("http://")
            && !reference.starts_with("https://")
            && !reference.starts_with("//"),
        "external asset reference is not allowed: {reference}"
    );
}

fn resolve_asset_uri(reference: &str) -> String {
    if reference.starts_with('/') {
        return reference.to_owned();
    }
    if let Some(relative) = reference.strip_prefix("./") {
        return format!("/{relative}");
    }
    format!("/{reference}")
}

fn expected_mime_prefix(reference: &str) -> Option<&'static str> {
    let path = reference.split('?').next().unwrap_or(reference);
    let extension = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if extension.eq_ignore_ascii_case("css") {
        Some("text/css")
    } else if extension.eq_ignore_ascii_case("js") || extension.eq_ignore_ascii_case("mjs") {
        Some("text/javascript")
    } else if extension.eq_ignore_ascii_case("svg") {
        Some("image/svg+xml")
    } else if extension.eq_ignore_ascii_case("png") {
        Some("image/png")
    } else if extension.eq_ignore_ascii_case("ico") {
        Some("image/")
    } else if extension.eq_ignore_ascii_case("json")
        || extension.eq_ignore_ascii_case("webmanifest")
    {
        Some("application/")
    } else {
        None
    }
}

async fn response_text(response: Response<Body>) -> String {
    let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn response_json(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}
