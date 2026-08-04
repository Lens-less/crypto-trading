//! Cross-stack fixture contract: locks the exact JSON bytes of every
//! read-only endpoint over the checked-in `rust/fixtures/web-api/` journal.
//!
//! The same snapshot files are parsed by the frontend test
//! `frontend/src/lib/api-fixtures.test.ts` with the zod schemas from
//! `frontend/src/lib/api-types.ts`. A backend serialization change therefore
//! fails this test, and a frontend schema change that no longer accepts the
//! served bytes fails the vitest side — the two ends cannot drift apart
//! silently.
//!
//! Regenerating after an intentional schema change:
//!
//! ```text
//! UPDATE_FIXTURES=1 cargo test -p crypto-trading-web --test api_fixture_contract
//! ```
//!
//! then review and commit the fixture diff together with the schema change.

use std::{fs, path::PathBuf, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::CONTENT_TYPE},
};
use crypto_trading_control_plane::ReadControlPlane;
use crypto_trading_runtime::FileJournalSnapshotSource;
use crypto_trading_web::{WebAccessPolicy, api_router};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

/// Durable generation of the checked-in fixture journal. Changing it changes
/// every cursor and `journal_id` field, so it is part of the contract.
const FIXTURE_JOURNAL_ID: &str = "77777777-7777-4777-8777-777777777777";

/// Read-only endpoints locked byte-for-byte against `rust/fixtures/web-api/`.
const SNAPSHOT_ENDPOINTS: &[(&str, &str)] = &[
    ("/api/v1/system", "system.json"),
    ("/api/v1/capabilities", "capabilities.json"),
    ("/api/v1/monitor", "monitor.json"),
    ("/api/v1/alerts", "alerts.json"),
    ("/api/v1/tasks", "tasks.json"),
    ("/api/v1/scanner", "scanner.json"),
    ("/api/v1/risk", "risk.json"),
    ("/api/v1/settings", "settings.json"),
    ("/api/v1/executions", "executions.json"),
];

#[tokio::test]
async fn read_endpoints_serve_the_checked_in_fixture_bytes() {
    let app = fixture_app();
    let update = std::env::var_os("UPDATE_FIXTURES").is_some();

    for (endpoint, file_name) in SNAPSHOT_ENDPOINTS {
        let response = app.clone().oneshot(get(endpoint)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{endpoint}");
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json"),
            "{endpoint} must serve JSON"
        );
        let body = to_bytes(response.into_body(), 8 * 1_048_576).await.unwrap();

        let path = fixture_path(file_name);
        if update {
            // Snapshot files store the served bytes verbatim plus one
            // trailing newline so text tooling stays quiet.
            let mut contents = body.to_vec();
            contents.push(b'\n');
            fs::write(&path, contents).unwrap();
            continue;
        }

        let expected = fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "missing snapshot {}: {error}; regenerate with UPDATE_FIXTURES=1",
                path.display()
            )
        });
        let expected = expected.strip_suffix(b"\n").unwrap_or(&expected);
        assert_eq!(
            body.as_ref(),
            expected,
            "{endpoint} no longer serves the bytes checked in at {}; if the \
             change is intentional, regenerate with UPDATE_FIXTURES=1 and \
             update the frontend schema expectations in the same commit",
            path.display()
        );
    }
}

/// The fixture journal must stay meaningful: every projection is complete,
/// nothing was rejected, and each endpoint has non-degenerate content, so the
/// snapshots keep exercising the full read-model shape on both stacks.
#[tokio::test]
async fn fixture_journal_projects_cleanly_and_populates_every_read_model() {
    let app = fixture_app();

    let system = get_json(&app, "/api/v1/system").await;
    assert_eq!(system["projection_status"], "complete");
    assert_eq!(system["journal_id"], FIXTURE_JOURNAL_ID);
    assert_eq!(system["warning_count"], 0);
    assert_eq!(system["kill_switch"], "normal");
    assert_eq!(system["market_data_freshness"], "not_available");
    assert_eq!(system["adapter_health"], "normal");

    let monitor = get_json(&app, "/api/v1/monitor").await;
    assert_eq!(monitor["projection_status"], "complete");
    assert_eq!(monitor["invalid_event_count"], 0);
    assert!(!monitor["latest"].is_null(), "monitor fixture is empty");

    let alerts = get_json(&app, "/api/v1/alerts").await;
    assert_eq!(alerts["projection_status"], "complete");
    assert_eq!(alerts["invalid_event_count"], 0);
    assert!(!alerts["occurrences"].as_array().unwrap().is_empty());

    let tasks = get_json(&app, "/api/v1/tasks").await;
    assert_eq!(tasks["projection_status"], "complete");
    assert_eq!(tasks["invalid_event_count"], 0);
    assert!(!tasks["tasks"].as_array().unwrap().is_empty());

    let scanner = get_json(&app, "/api/v1/scanner").await;
    assert_eq!(scanner["projection_status"], "complete");
    assert_eq!(scanner["invalid_event_count"], 0);
    assert!(!scanner["latest"]["rows"].as_array().unwrap().is_empty());

    let risk = get_json(&app, "/api/v1/risk").await;
    assert_eq!(risk["paper_accounts"]["projection_status"], "complete");
    assert_eq!(risk["paper_accounts"]["invalid_event_count"], 0);
    assert!(
        !risk["paper_accounts"]["accounts"]
            .as_array()
            .unwrap()
            .is_empty(),
        "paper-account fixture is empty"
    );
    assert_eq!(risk["account_risk"]["projection_status"], "complete");
    assert_eq!(risk["account_risk"]["invalid_event_count"], 0);
    assert!(
        !risk["account_risk"]["scopes"]
            .as_array()
            .unwrap()
            .is_empty(),
        "account-risk fixture is empty"
    );

    let executions = get_json(&app, "/api/v1/executions").await;
    assert_eq!(executions["operator"]["projection_status"], "complete");
    assert!(
        !executions["operator"]["batches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(executions["changes"]["next_cursor"].is_string());
}

fn fixture_app() -> Router {
    let journal_id = Uuid::parse_str(FIXTURE_JOURNAL_ID).unwrap();
    let source = FileJournalSnapshotSource::new(journal_id, fixture_path("journal.jsonl"))
        .expect("fixture journal source");
    let control_plane = ReadControlPlane::new(Arc::new(source)).expect("capability manifest");
    api_router(Arc::new(control_plane), WebAccessPolicy::loopback_open())
}

fn fixture_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/web-api")
        .join(file_name)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

async fn get_json(app: &Router, uri: &str) -> Value {
    let response = app.clone().oneshot(get(uri)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    let bytes = to_bytes(response.into_body(), 8 * 1_048_576).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
