use std::{
    path::PathBuf,
    str::FromStr,
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
use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    FileJournalSnapshotSource, JsonlHistory, PaperAccountAuthority, PaperAccountConfig,
    PaperCostModel, PaperReconciliationDigestAlgorithm, PaperReconciliationEvidence,
    PaperReconciliationOutcome, PaperReconciliationProof, PaperReservationLeg,
    PaperReservationPhase, PaperReservationRequest,
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

#[derive(Clone)]
struct ReconciliationDispatcher {
    calls: Arc<AtomicUsize>,
    authority: Arc<PaperAccountAuthority>,
}

impl SubmitDispatcher for ReconciliationDispatcher {
    fn dispatch(&self, envelope: SubmitEnvelope) -> SubmitDispatchFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let authority = self.authority.clone();
        Box::pin(async move {
            match envelope.command() {
                SubmitCommand::ReconcileRelease { proof } => authority
                    .reconcile_release(proof.clone())
                    .await
                    .map_or(SubmitDispatchOutcome::Rejected, |_| {
                        SubmitDispatchOutcome::Applied
                    }),
                SubmitCommand::RecordReconcileFailure { proof } => authority
                    .record_reconciliation_failure(proof.clone())
                    .await
                    .map_or(SubmitDispatchOutcome::Rejected, |_| {
                        SubmitDispatchOutcome::Applied
                    }),
                _ => SubmitDispatchOutcome::Rejected,
            }
        })
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

fn money(value: &str) -> Money {
    Money::from_str(value).unwrap()
}

fn reconciliation_ids() -> (Uuid, Uuid, Uuid) {
    (
        Uuid::parse_str("85ad0b40-5930-4ac8-9857-f3d2ec679394").unwrap(),
        Uuid::parse_str("5252fd91-cd35-4bff-9cfa-fe8634c38cc3").unwrap(),
        Uuid::parse_str("aa2ce047-b50a-48b4-b5b8-b68c1a78d5fb").unwrap(),
    )
}

fn reconcile_release(
    command_id: Uuid,
    key: &str,
    target: &str,
    proof: PaperReconciliationProof,
) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        key,
        target,
        SubmitPermission::new("reconciler-a", SubmitRole::Reconciler).unwrap(),
        SubmitRiskConfirmation::ReconciliationEvidenceVerified,
        SubmitCommand::ReconcileRelease { proof },
    )
    .unwrap()
}

fn reconcile_failure(
    command_id: Uuid,
    key: &str,
    target: &str,
    proof: PaperReconciliationProof,
) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        key,
        target,
        SubmitPermission::new("reconciler-a", SubmitRole::Reconciler).unwrap(),
        SubmitRiskConfirmation::ReconciliationEvidenceVerified,
        SubmitCommand::RecordReconcileFailure { proof },
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

async fn committed_reconciliation_authority(path: &PathBuf) -> Arc<PaperAccountAuthority> {
    let (journal_id, reservation_id, batch_id) = reconciliation_ids();
    let authority = Arc::new(
        PaperAccountAuthority::new(
            journal_id,
            JsonlHistory::new(path),
            PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
        )
        .unwrap(),
    );
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        Quantity::from_str("0.001").unwrap(),
    );
    authority
        .reserve(
            PaperReservationRequest::new(
                reservation_id,
                "grid-btc",
                "grid-btc-001",
                batch_id,
                PaperCostModel::v1(0, 0, 0).unwrap(),
                vec![PaperReservationLeg::from_intent(0, &intent, money("100")).unwrap()],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    authority
        .commit(reservation_id, money("100"))
        .await
        .unwrap();
    authority
}

async fn reconciler_application(
    label: &str,
) -> (
    tokio::net::TcpListener,
    axum::Router,
    PathBuf,
    Arc<AtomicUsize>,
    Arc<PaperAccountAuthority>,
) {
    let path = temporary_journal(label);
    let (journal_id, _, _) = reconciliation_ids();
    let authority = committed_reconciliation_authority(&path).await;
    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let read = Arc::new(ReadControlPlane::new(Arc::new(source)).unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let submit = Arc::new(
        SubmitService::new(
            journal_id,
            &path,
            Arc::new(ReconciliationDispatcher {
                calls: calls.clone(),
                authority: authority.clone(),
            }),
        )
        .unwrap(),
    );
    let identity = TrustedSubmitIdentity::reconciler("reconciler-a").unwrap();
    let application = bind_trusted_submit_app(0, read, submit, BEARER.to_owned(), identity)
        .await
        .unwrap();
    let (listener, router) = application.into_parts();
    (listener, router, path, calls, authority)
}

fn reconciliation_match_proof(snapshot_sequence: u64, digest: &str) -> PaperReconciliationProof {
    let (_, reservation_id, batch_id) = reconciliation_ids();
    PaperReconciliationProof::from_evidence(
        PaperReconciliationEvidence::clean_match(
            "contract-fixture",
            digest,
            "paper-main",
            reservation_id,
            batch_id,
            format!("binance-testnet-match-{snapshot_sequence}"),
            snapshot_sequence,
            money("1000"),
        )
        .unwrap(),
    )
    .unwrap()
}

fn reconciliation_mismatch_proof(snapshot_sequence: u64, digest: &str) -> PaperReconciliationProof {
    let (_, reservation_id, batch_id) = reconciliation_ids();
    PaperReconciliationProof::from_evidence(
        PaperReconciliationEvidence::mismatch(
            "contract-fixture",
            digest,
            "paper-main",
            reservation_id,
            batch_id,
            format!("binance-testnet-mismatch-{snapshot_sequence}"),
            snapshot_sequence,
            money("1000"),
            "fixture_mismatch",
        )
        .unwrap(),
    )
    .unwrap()
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
async fn readiness_probe_budget_is_isolated_from_trusted_submit() {
    let (listener, router, path, calls) = application("health-budget").await;
    let envelope = paper_stop(Uuid::new_v4(), "stop-health-budget", "paper-grid-btc-usdt");

    for _ in 0..crypto_trading_web::WEB_REQUEST_LIMIT_PER_MINUTE {
        let health = router
            .clone()
            .oneshot(request("GET", "/api/v1/health", Body::empty(), false))
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
    }

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
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn authenticated_read_budget_is_isolated_from_trusted_submit() {
    let (listener, router, path, calls) = application("read-budget").await;
    let envelope = paper_stop(Uuid::new_v4(), "stop-read-budget", "paper-grid-btc-usdt");

    for _ in 0..crypto_trading_web::WEB_REQUEST_LIMIT_PER_MINUTE {
        let response = router
            .clone()
            .oneshot(request("GET", "/api/v1/system", Body::empty(), true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let limited = router
        .clone()
        .oneshot(request("GET", "/api/v1/system", Body::empty(), true))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

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

#[tokio::test]
async fn same_reconciliation_proof_replay_does_not_redispatch() {
    let (listener, router, path, calls, authority) =
        reconciler_application("reconcile-replay").await;
    let proof = reconciliation_match_proof(42, "0123456789abcdef");
    let envelope = reconcile_release(
        Uuid::new_v4(),
        "reconcile-release-replay",
        "paper-grid-btc-usdt",
        proof,
    );
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

    let snapshot = authority.snapshot().await.unwrap();
    assert!(snapshot.reservations.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn conflicting_reconciliation_digest_for_the_same_reservation_fails_closed() {
    let (listener, router, path, calls, authority) =
        reconciler_application("reconcile-digest-conflict").await;
    let accepted = reconcile_failure(
        Uuid::new_v4(),
        "reconcile-failure-accepted",
        "paper-grid-btc-usdt",
        reconciliation_mismatch_proof(42, "0123456789abcdef"),
    );
    let conflict = reconcile_failure(
        Uuid::new_v4(),
        "reconcile-failure-conflict",
        "paper-grid-btc-usdt",
        reconciliation_mismatch_proof(42, "fedcba9876543210"),
    );

    let first = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&accepted).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = router
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&conflict).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let snapshot = authority.snapshot().await.unwrap();
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Committed
    );
    assert_eq!(
        snapshot.reservations[0]
            .reconciliation
            .as_ref()
            .unwrap()
            .outcome,
        PaperReconciliationOutcome::Failed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn release_followed_by_failure_is_rejected_without_state_reversal() {
    let (listener, router, path, calls, authority) =
        reconciler_application("reconcile-release-then-failure").await;
    let release = reconcile_release(
        Uuid::new_v4(),
        "reconcile-release-first",
        "paper-grid-btc-usdt",
        reconciliation_match_proof(42, "0123456789abcdef"),
    );
    let failure = reconcile_failure(
        Uuid::new_v4(),
        "reconcile-failure-second",
        "paper-grid-btc-usdt",
        reconciliation_mismatch_proof(43, "fedcba9876543210"),
    );

    let first = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&release).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = router
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&failure).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let snapshot = authority.snapshot().await.unwrap();
    assert!(snapshot.reservations.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn failure_followed_by_release_is_rejected_without_state_reversal() {
    let (listener, router, path, calls, authority) =
        reconciler_application("reconcile-failure-then-release").await;
    let failure = reconcile_failure(
        Uuid::new_v4(),
        "reconcile-failure-first",
        "paper-grid-btc-usdt",
        reconciliation_mismatch_proof(42, "0123456789abcdef"),
    );
    let release = reconcile_release(
        Uuid::new_v4(),
        "reconcile-release-second",
        "paper-grid-btc-usdt",
        reconciliation_match_proof(43, "fedcba9876543210"),
    );

    let first = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&failure).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = router
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(&release).unwrap()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let snapshot = authority.snapshot().await.unwrap();
    assert_eq!(
        snapshot.reservations[0].phase,
        PaperReservationPhase::Committed
    );
    assert_eq!(
        snapshot.reservations[0]
            .reconciliation
            .as_ref()
            .unwrap()
            .outcome,
        PaperReconciliationOutcome::Failed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    drop(listener);
    std::fs::remove_file(path).unwrap();
}
