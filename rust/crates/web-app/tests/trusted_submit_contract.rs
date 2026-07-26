use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
};
use crypto_trading_control_plane::{
    ReadControlPlane, SubmitCommand, SubmitDispatchFuture, SubmitDispatchOutcome, SubmitDispatcher,
    SubmitEnvelope, SubmitPermission, SubmitRiskConfirmation, SubmitRole, SubmitService,
};
use crypto_trading_runtime::{
    FileJournalSnapshotSource, PaperReconciliationDigestAlgorithm, PaperReconciliationProof,
};
use crypto_trading_web_app::{
    MAX_TRUSTED_SUBMIT_BODY_BYTES, TrustedSubmitIdentity, bind_trusted_submit_app,
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const BEARER: &str = "0123456789abcdef0123456789abcdef";

#[derive(Clone)]
struct RecordingDispatcher {
    calls: Arc<AtomicUsize>,
}

impl SubmitDispatcher for RecordingDispatcher {
    fn dispatch(&self, _envelope: SubmitEnvelope) -> SubmitDispatchFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { SubmitDispatchOutcome::Applied })
    }
}

fn paper_stop(command_id: Uuid, key: &str, target: &str) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        key,
        target,
        SubmitPermission::new("operator-a", SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        SubmitCommand::StopTask,
    )
    .unwrap()
}

fn temporary_journal(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "crypto-trading-web-{label}-{}.jsonl",
        Uuid::new_v4()
    ));
    std::fs::write(&path, []).unwrap();
    path
}

async fn application(
    label: &str,
) -> (
    tokio::net::TcpListener,
    axum::Router,
    PathBuf,
    Arc<AtomicUsize>,
) {
    let path = temporary_journal(label);
    let journal_id = Uuid::new_v4();
    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let read = Arc::new(ReadControlPlane::new(Arc::new(source)).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let submit = Arc::new(
        SubmitService::new(
            journal_id,
            &path,
            Arc::new(RecordingDispatcher {
                calls: calls.clone(),
            }),
        )
        .unwrap(),
    );
    let identity = TrustedSubmitIdentity::paper_operator("operator-a").unwrap();
    let application = bind_trusted_submit_app(0, read, submit, BEARER.to_owned(), identity)
        .await
        .unwrap();
    assert!(application.address().ip().is_loopback());
    let (listener, router) = application.into_parts();
    (listener, router, path, calls)
}

fn request(method: &str, uri: &str, body: Body, authenticated: bool) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json");
    if authenticated {
        request = request.header(AUTHORIZATION, format!("Bearer {BEARER}"));
    }
    request.body(body).unwrap()
}

#[tokio::test]
async fn submit_requires_bearer_while_existing_get_routes_remain_read_control_plane_backed() {
    let (listener, router, path, calls) = application("auth").await;
    let envelope = paper_stop(Uuid::new_v4(), "stop-auth", "paper-grid-btc-usdt");

    let unauthorized = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&envelope).unwrap()),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let system = router
        .clone()
        .oneshot(request("GET", "/api/v1/system", Body::empty(), true))
        .await
        .unwrap();
    assert_eq!(system.status(), StatusCode::OK);

    let submitted = router
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&envelope).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(submitted.status(), StatusCode::OK);
    let body = to_bytes(submitted.into_body(), 16_384).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["command_id"], envelope.command_id().to_string());
    assert_eq!(body["target_task_id"], envelope.target_task_id());
    assert_eq!(body["status"], "applied");
    assert_eq!(body["journal_projection"], "submit_command_v1");
    assert_eq!(body["source"], "durable_journal");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn replay_is_durable_and_conflicting_payload_does_not_redispatch() {
    let (listener, router, path, calls) = application("replay").await;
    let command_id = Uuid::new_v4();
    let envelope = paper_stop(command_id, "stop-replay", "paper-grid-btc-usdt");
    let encoded = serde_json::to_vec(&envelope).unwrap();

    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(request(
                "POST",
                "/api/v1/submit",
                Body::from(encoded.clone()),
                true,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let conflict = paper_stop(command_id, "stop-conflict", "paper-grid-eth-usdt");
    let response = router
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&conflict).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn server_identity_rejects_principal_spoof_and_reconciler_escalation() {
    let (listener, router, path, calls) = application("identity").await;
    let spoofed = SubmitEnvelope::new(
        Uuid::new_v4(),
        "stop-spoofed",
        "paper-grid-btc-usdt",
        SubmitPermission::new("operator-b", SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        SubmitCommand::StopTask,
    )
    .unwrap();
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&spoofed).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = to_bytes(response.into_body(), 16_384).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "permission_denied");
    assert!(!body.to_string().contains("operator-b"));

    let proof = PaperReconciliationProof::new(
        "paper-account",
        Uuid::new_v4(),
        Uuid::new_v4(),
        "reconcile-snapshot",
        1,
        PaperReconciliationDigestAlgorithm::Fnv1a64,
        "0123456789abcdef",
    )
    .unwrap();
    let escalated = SubmitEnvelope::new(
        Uuid::new_v4(),
        "reconcile-escalation",
        "paper-grid-btc-usdt",
        SubmitPermission::new("operator-a", SubmitRole::Reconciler).unwrap(),
        SubmitRiskConfirmation::ReconciliationEvidenceVerified,
        SubmitCommand::ReconcileRelease { proof },
    )
    .unwrap();
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&escalated).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = to_bytes(response.into_body(), 16_384).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "permission_denied");
    assert!(!body.to_string().contains("reconciler"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    drop(router);
    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn invalid_unknown_and_oversized_bodies_fail_closed_without_echoing_input() {
    let (listener, router, path, calls) = application("invalid").await;
    let envelope = paper_stop(Uuid::new_v4(), "stop-invalid", "paper-grid-btc-usdt");

    let mut invalid = serde_json::to_value(&envelope).unwrap();
    invalid["schema_version"] = json!(99);
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&invalid).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let mut unknown = serde_json::to_value(&envelope).unwrap();
    unknown["command"]["kind"] = json!("enable_mainnet");
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&unknown).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let secret = "never-echo-this-secret";
    let oversized = format!(
        "{{\"secret\":\"{secret}\",\"padding\":\"{}\"}}",
        "x".repeat(MAX_TRUSTED_SUBMIT_BODY_BYTES)
    );
    let response = router
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(oversized),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = to_bytes(response.into_body(), 16_384).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(!body.contains(secret));
    assert!(!body.contains("padding"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn submit_route_carries_the_same_security_headers_and_401_code_as_the_read_api() {
    let (_listener, router, _path, _calls) = application("headers").await;
    let envelope = paper_stop(Uuid::new_v4(), "stop-headers", "paper-grid-btc-usdt");

    let unauthorized = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&envelope).unwrap()),
            false,
        ))
        .await
        .unwrap();

    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    // The submit router is merged into the application router, so it must sit
    // under the same security-header layer every read route does.
    let headers = unauthorized.headers();
    for header in [
        "cache-control",
        "content-security-policy",
        "permissions-policy",
        "referrer-policy",
        "x-content-type-options",
        "x-frame-options",
    ] {
        assert!(
            headers.contains_key(header),
            "submit response is missing {header}"
        );
    }

    // The browser client keys re-authentication off this code, so the two
    // routes must not spell it differently.
    let body = to_bytes(unauthorized.into_body(), 65_536).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "authentication_required");
}
