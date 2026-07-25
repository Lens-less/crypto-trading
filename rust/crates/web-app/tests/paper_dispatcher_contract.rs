use std::{path::PathBuf, sync::Arc, time::Duration as StdDuration};

use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
};
use crypto_trading_cli::{
    ArbitragePaperProfileInput, GridPaperProfileInput, PaperProfileCatalog,
    PaperProfileCatalogInput,
};
use crypto_trading_control_plane::{
    ReadControlPlane, SubmitCommand, SubmitDispatcher, SubmitEnvelope, SubmitPermission,
    SubmitRiskConfirmation, SubmitRole, SubmitService,
};
use crypto_trading_runtime::FileJournalSnapshotSource;
use crypto_trading_web_app::{
    TrustedPaperSubmitDispatcher, TrustedSubmitIdentity, bind_trusted_submit_app,
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const BEARER: &str = "0123456789abcdef0123456789abcdef";
const PRINCIPAL: &str = "local-paper-operator";
const JOURNAL_ID: Uuid = Uuid::from_u128(0x7777);

struct ApplicationFixture {
    listener: tokio::net::TcpListener,
    router: axum::Router,
    history_path: PathBuf,
    dispatcher: Arc<TrustedPaperSubmitDispatcher>,
}

fn request(method: &str, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, format!("Bearer {BEARER}"))
        .body(body)
        .unwrap()
}

fn grid_start(
    command_id: Uuid,
    idempotency_key: &str,
    task_id: &str,
    revision: &str,
) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        idempotency_key,
        task_id,
        SubmitPermission::new(PRINCIPAL, SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        SubmitCommand::StartPaperGrid {
            strategy_id: "grid.strategy".to_owned(),
            strategy_revision: revision.to_owned(),
        },
    )
    .unwrap()
}

fn arbitrage_start(
    command_id: Uuid,
    idempotency_key: &str,
    task_id: &str,
    revision: &str,
) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        idempotency_key,
        task_id,
        SubmitPermission::new(PRINCIPAL, SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        SubmitCommand::StartPaperArbitrage {
            strategy_id: "arb.strategy".to_owned(),
            strategy_revision: revision.to_owned(),
        },
    )
    .unwrap()
}

fn mutation(
    command_id: Uuid,
    idempotency_key: &str,
    task_id: &str,
    command: SubmitCommand,
) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        idempotency_key,
        task_id,
        SubmitPermission::new(PRINCIPAL, SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        command,
    )
    .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn temp_path(label: &str, extension: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "crypto-trading-web-{label}-{}.{}",
        Uuid::new_v4(),
        extension
    ))
}

fn write(path: &PathBuf, body: &str) {
    std::fs::write(path, body).unwrap();
}

fn grid_config() -> String {
    r"
grid_system:
  exchange: paper-grid
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
"
    .trim()
    .to_owned()
}

fn grid_replay() -> String {
    [
        json!({
            "exchange": "paper-grid",
            "symbol": "BTC-USDC-PERP",
            "market_type": "perpetual",
            "bid": "109",
            "ask": "111",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "last": "110",
            "timestamp": "2026-07-25T00:00:00Z",
        }),
        json!({
            "exchange": "paper-grid",
            "symbol": "BTC-USDC-PERP",
            "market_type": "perpetual",
            "bid": "98",
            "ask": "100",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "last": "99",
            "timestamp": "2026-07-25T00:00:01Z",
        }),
    ]
    .into_iter()
    .map(|line| serde_json::to_string(&line).unwrap())
    .collect::<Vec<_>>()
    .join("\n")
        + "\n"
}

fn idle_grid_replay() -> String {
    [
        json!({
            "exchange": "paper-grid",
            "symbol": "BTC-USDC-PERP",
            "market_type": "perpetual",
            "bid": "109",
            "ask": "111",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "last": "110",
            "timestamp": "2026-07-25T00:00:00Z",
        }),
        json!({
            "exchange": "paper-grid",
            "symbol": "BTC-USDC-PERP",
            "market_type": "perpetual",
            "bid": "108",
            "ask": "110",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "last": "109",
            "timestamp": "2026-07-25T00:00:01Z",
        }),
    ]
    .into_iter()
    .map(|line| serde_json::to_string(&line).unwrap())
    .collect::<Vec<_>>()
    .join("\n")
        + "\n"
}

fn arbitrage_config() -> String {
    r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges:
  - paper-left
  - paper-right
symbols:
  - ETH-USDC-PERP
min_spread_pct: 0.02
base_quantity: 0.04
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
max_position_value: 5000
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
    grid_config:
      initial_spread_threshold: 0.02
      grid_step: 0.03
      max_segments: 5
    quantity_config:
      base_quantity: 0.04
    risk_config:
      max_position_value: 5000
"
    .trim()
    .to_owned()
}

fn monitor_config() -> String {
    r"
exchanges:
  - paper-left
  - paper-right
symbols:
  - ETH-USDC-PERP
health_check:
  data_timeout: 30
"
    .trim()
    .to_owned()
}

fn arbitrage_replay() -> String {
    [
        json!({
            "exchange": "paper-left",
            "symbol": "ETH-USDC-PERP",
            "market_type": "perpetual",
            "bid": "99",
            "ask": "100",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "timestamp": "2026-07-25T00:00:00Z",
        }),
        json!({
            "exchange": "paper-right",
            "symbol": "ETH-USDC-PERP",
            "market_type": "perpetual",
            "bid": "102",
            "ask": "103",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "timestamp": "2026-07-25T00:00:00Z",
        }),
    ]
    .into_iter()
    .map(|line| serde_json::to_string(&line).unwrap())
    .collect::<Vec<_>>()
    .join("\n")
        + "\n"
}

fn idle_arbitrage_replay() -> String {
    [
        json!({
            "exchange": "paper-left",
            "symbol": "ETH-USDC-PERP",
            "market_type": "perpetual",
            "bid": "99",
            "ask": "100",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "timestamp": "2026-07-25T00:00:00Z",
        }),
        json!({
            "exchange": "paper-right",
            "symbol": "ETH-USDC-PERP",
            "market_type": "perpetual",
            "bid": "100",
            "ask": "101",
            "bid_quantity": "20",
            "ask_quantity": "20",
            "timestamp": "2026-07-25T00:00:00Z",
        }),
    ]
    .into_iter()
    .map(|line| serde_json::to_string(&line).unwrap())
    .collect::<Vec<_>>()
    .join("\n")
        + "\n"
}

fn catalog_from_replays(
    label: &str,
    grid_replay_body: &str,
    arbitrage_replay_body: Option<&str>,
) -> PaperProfileCatalog {
    let grid_config_path = temp_path(&format!("{label}-grid-config"), "yaml");
    let grid_replay_path = temp_path(&format!("{label}-grid-replay"), "jsonl");
    write(&grid_config_path, &grid_config());
    write(&grid_replay_path, grid_replay_body);

    let arbitrage = if let Some(replay_body) = arbitrage_replay_body {
        let arbitrage_config_path = temp_path(&format!("{label}-arb-config"), "yaml");
        let monitor_config_path = temp_path(&format!("{label}-monitor-config"), "yaml");
        let arbitrage_replay_path = temp_path(&format!("{label}-arb-replay"), "jsonl");
        write(&arbitrage_config_path, &arbitrage_config());
        write(&monitor_config_path, &monitor_config());
        write(&arbitrage_replay_path, replay_body);
        Some(ArbitragePaperProfileInput {
            task_id: "paper-arbitrage-owner".to_owned(),
            strategy_id: "arb.strategy".to_owned(),
            strategy_revision: "arb.v1".to_owned(),
            arbitrage_config_path,
            monitor_config_path,
            replay_path: arbitrage_replay_path,
            shutdown_grace: StdDuration::from_millis(10),
        })
    } else {
        None
    };

    PaperProfileCatalog::new(PaperProfileCatalogInput {
        grid: Some(GridPaperProfileInput {
            task_id: "paper-grid-owner".to_owned(),
            strategy_id: "grid.strategy".to_owned(),
            strategy_revision: "grid.v1".to_owned(),
            config_path: grid_config_path,
            replay_path: grid_replay_path,
            shutdown_grace: StdDuration::from_millis(10),
        }),
        arbitrage,
    })
    .unwrap()
}

fn catalog(label: &str, include_arbitrage: bool) -> PaperProfileCatalog {
    let grid = grid_replay();
    let arbitrage = include_arbitrage.then(arbitrage_replay);
    catalog_from_replays(label, &grid, arbitrage.as_deref())
}

fn idle_catalog(label: &str) -> PaperProfileCatalog {
    let grid = idle_grid_replay();
    let arbitrage = idle_arbitrage_replay();
    catalog_from_replays(label, &grid, Some(&arbitrage))
}

async fn application(
    label: &str,
    catalog: PaperProfileCatalog,
    existing_journal: Option<String>,
) -> ApplicationFixture {
    let history_path = temp_path(&format!("{label}-history"), "jsonl");
    std::fs::write(&history_path, existing_journal.unwrap_or_default()).unwrap();
    let source = FileJournalSnapshotSource::new(JOURNAL_ID, &history_path).unwrap();
    let read = Arc::new(ReadControlPlane::new(Arc::new(source)).unwrap());
    let dispatcher = Arc::new(TrustedPaperSubmitDispatcher::new(
        JOURNAL_ID,
        history_path.clone(),
        catalog,
    ));
    let submit_dispatcher: Arc<dyn SubmitDispatcher> = dispatcher.clone();
    let submit =
        Arc::new(SubmitService::new(JOURNAL_ID, &history_path, submit_dispatcher).unwrap());
    let identity = TrustedSubmitIdentity::paper_operator(PRINCIPAL).unwrap();
    let app = bind_trusted_submit_app(0, read, submit, BEARER.to_owned(), identity)
        .await
        .unwrap();
    let (listener, router) = app.into_parts();
    ApplicationFixture {
        listener,
        router,
        history_path,
        dispatcher,
    }
}

async fn submit(router: &axum::Router, envelope: &SubmitEnvelope) -> Value {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/v1/submit",
            Body::from(serde_json::to_vec(envelope).unwrap()),
        ))
        .await
        .unwrap();
    let http_status = response.status();
    let payload = response_json(response).await;
    let expected = match payload["status"].as_str() {
        Some("applied") => StatusCode::OK,
        Some("rejected") => StatusCode::UNPROCESSABLE_ENTITY,
        Some("outcome_unknown") => StatusCode::ACCEPTED,
        status => panic!("unexpected submit receipt status {status:?}: {payload}"),
    };
    assert_eq!(http_status, expected, "{payload}");
    payload
}

async fn tasks(router: &axum::Router) -> Value {
    let response = router
        .clone()
        .oneshot(request("GET", "/api/v1/tasks", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn wait_for_task(router: &axum::Router, task_id: &str, phase: &str) -> Value {
    let mut last_payload = Value::Null;
    for _ in 0..50 {
        let payload = tasks(router).await;
        if let Some(task) = payload["tasks"]
            .as_array()
            .and_then(|tasks| {
                tasks
                    .iter()
                    .find(|task| task["task_id"] == task_id && task["phase"] == phase)
            })
            .cloned()
        {
            return task;
        }
        last_payload = payload;
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    panic!("task {task_id} never reached phase {phase}; last projection: {last_payload}");
}

async fn wait_for_journal(path: &PathBuf, expected: &str) -> String {
    let mut last_journal = String::new();
    for _ in 0..50 {
        last_journal = std::fs::read_to_string(path).unwrap();
        if last_journal.contains(expected) {
            return last_journal;
        }
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
    panic!("journal never contained {expected:?}; last journal: {last_journal}");
}

fn accepted_only_record(envelope: &SubmitEnvelope) -> String {
    serde_json::to_string(&json!({
        "timestamp": "2026-07-25T00:00:00Z",
        "strategy": "trusted-submit",
        "symbol": envelope.target_task_id(),
        "decision": "submit_accepted",
        "details": {
            "submit": {
                "stage": "accepted",
                "schema_version": 1,
                "envelope": envelope,
            }
        }
    }))
    .unwrap()
        + "\n"
}

#[tokio::test]
async fn exact_profile_match_and_unknown_tasks_fail_closed() {
    let fixture = application("exact-match", catalog("exact-match", false), None).await;
    let unknown = submit(
        &fixture.router,
        &grid_start(Uuid::new_v4(), "grid-unknown", "missing-task", "grid.v1"),
    )
    .await;
    assert_eq!(unknown["status"], "rejected");

    let mismatch = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "grid-mismatch",
            "paper-grid-owner",
            "grid.v2",
        ),
    )
    .await;
    assert_eq!(mismatch["status"], "rejected");

    let stop_unknown = submit(
        &fixture.router,
        &mutation(
            Uuid::new_v4(),
            "stop-unknown",
            "missing-task",
            SubmitCommand::StopTask,
        ),
    )
    .await;
    assert_eq!(stop_unknown["status"], "rejected");

    drop(fixture.listener);
}

#[tokio::test]
async fn duplicate_start_rejects_and_task_status_stays_journal_backed() {
    let fixture = application("duplicate", catalog("duplicate", false), None).await;
    let started = submit(
        &fixture.router,
        &grid_start(Uuid::new_v4(), "grid-start", "paper-grid-owner", "grid.v1"),
    )
    .await;
    assert_eq!(started["status"], "applied");

    let task = wait_for_task(&fixture.router, "paper-grid-owner", "running").await;
    assert_eq!(task["task_id"], "paper-grid-owner");
    assert_eq!(task["phase"], "running");

    let duplicate = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "grid-start-2",
            "paper-grid-owner",
            "grid.v1",
        ),
    )
    .await;
    assert_eq!(duplicate["status"], "rejected");

    assert_eq!(
        fixture.dispatcher.shutdown().await,
        crypto_trading_control_plane::SubmitDispatchOutcome::Applied
    );
    wait_for_task(&fixture.router, "paper-grid-owner", "stopped").await;
    drop(fixture.listener);
}

#[tokio::test]
async fn start_stop_restart_and_cancel_flow_projects_only_through_tasks_endpoint() {
    let fixture = application("lifecycle", idle_catalog("lifecycle"), None).await;

    let grid_started = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "grid-start-1",
            "paper-grid-owner",
            "grid.v1",
        ),
    )
    .await;
    assert_eq!(grid_started["status"], "applied");
    wait_for_task(&fixture.router, "paper-grid-owner", "running").await;

    let stop = submit(
        &fixture.router,
        &mutation(
            Uuid::new_v4(),
            "grid-stop-1",
            "paper-grid-owner",
            SubmitCommand::StopTask,
        ),
    )
    .await;
    assert_eq!(stop["status"], "applied");
    let stopped = wait_for_task(&fixture.router, "paper-grid-owner", "stopped").await;
    assert_eq!(stopped["exit"], "stop_requested");

    let restarted = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "grid-start-2",
            "paper-grid-owner",
            "grid.v1",
        ),
    )
    .await;
    assert_eq!(restarted["status"], "applied");
    wait_for_task(&fixture.router, "paper-grid-owner", "running").await;

    let arbitrage_started = submit(
        &fixture.router,
        &arbitrage_start(
            Uuid::new_v4(),
            "arb-start-1",
            "paper-arbitrage-owner",
            "arb.v1",
        ),
    )
    .await;
    assert_eq!(arbitrage_started["status"], "applied");
    wait_for_task(&fixture.router, "paper-arbitrage-owner", "running").await;

    let cancel = submit(
        &fixture.router,
        &mutation(
            Uuid::new_v4(),
            "arb-cancel-1",
            "paper-arbitrage-owner",
            SubmitCommand::CancelTask,
        ),
    )
    .await;
    assert_eq!(cancel["status"], "applied");
    let cancelled = wait_for_task(&fixture.router, "paper-arbitrage-owner", "stopped").await;
    assert_eq!(cancelled["exit"], "stop_requested");

    let journal = std::fs::read_to_string(&fixture.history_path).unwrap();
    assert!(journal.contains("\"decision\":\"submit_accepted\""));
    assert!(journal.contains("\"strategy\":\"read_only_task\""));

    drop(fixture.listener);
}

#[tokio::test]
async fn matching_grid_profile_executes_through_paper_exchange_and_account_authority() {
    let fixture = application("paper-execution", catalog("paper-execution", false), None).await;
    let started = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "paper-execution-start",
            "paper-grid-owner",
            "grid.v1",
        ),
    )
    .await;
    assert_eq!(started["status"], "applied");

    let journal = wait_for_journal(
        &fixture.history_path,
        "\"decision\":\"paper_account_committed\"",
    )
    .await;
    assert!(journal.contains("\"decision\":\"execution_completed\""));
    assert!(journal.contains("\"id\":\"paper-grid-"));
    wait_for_journal(
        &fixture.history_path,
        "\"processed_event_count\":2,\"schema_version\":1",
    )
    .await;

    let stopped = submit(
        &fixture.router,
        &mutation(
            Uuid::new_v4(),
            "paper-execution-stop",
            "paper-grid-owner",
            SubmitCommand::StopTask,
        ),
    )
    .await;
    assert_eq!(stopped["status"], "applied");
    wait_for_task(&fixture.router, "paper-grid-owner", "stopped").await;

    drop(fixture.listener);
}

#[tokio::test]
async fn checked_in_grid_replay_keeps_the_owner_running_after_every_cross() {
    let grid = include_str!("../../../fixtures/m4-grid-paper-replay.jsonl");
    let fixture = application(
        "checked-in-grid-replay",
        catalog_from_replays("checked-in-grid-replay", grid, None),
        None,
    )
    .await;
    let started = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "checked-in-grid-start",
            "paper-grid-owner",
            "grid.v1",
        ),
    )
    .await;
    assert_eq!(started["status"], "applied");

    let journal = wait_for_journal(
        &fixture.history_path,
        "\"decision\":\"paper_account_committed\"",
    )
    .await;
    assert!(!journal.contains("\"decision\":\"task_failed\""));
    wait_for_task(&fixture.router, "paper-grid-owner", "running").await;

    let stopped = submit(
        &fixture.router,
        &mutation(
            Uuid::new_v4(),
            "checked-in-grid-stop",
            "paper-grid-owner",
            SubmitCommand::StopTask,
        ),
    )
    .await;
    assert_eq!(stopped["status"], "applied");
    wait_for_task(&fixture.router, "paper-grid-owner", "stopped").await;

    drop(fixture.listener);
}

#[tokio::test]
async fn accepted_only_restart_never_redispatches_the_start_command() {
    let envelope = grid_start(Uuid::new_v4(), "grid-replay", "paper-grid-owner", "grid.v1");
    let journal = accepted_only_record(&envelope);
    let fixture = application(
        "accepted-only",
        catalog("accepted-only", false),
        Some(journal),
    )
    .await;

    let replay = submit(&fixture.router, &envelope).await;
    assert_eq!(replay["status"], "outcome_unknown");

    let tasks = tasks(&fixture.router).await;
    assert_eq!(tasks["tasks"].as_array().unwrap().len(), 0);
    assert_eq!(
        std::fs::read_to_string(&fixture.history_path)
            .unwrap()
            .lines()
            .count(),
        1
    );

    drop(fixture.listener);
}

#[tokio::test]
async fn graceful_shutdown_stops_all_owners_and_closes_command_admission() {
    let fixture = application("graceful-shutdown", idle_catalog("graceful-shutdown"), None).await;

    for envelope in [
        grid_start(
            Uuid::new_v4(),
            "shutdown-grid-start",
            "paper-grid-owner",
            "grid.v1",
        ),
        arbitrage_start(
            Uuid::new_v4(),
            "shutdown-arbitrage-start",
            "paper-arbitrage-owner",
            "arb.v1",
        ),
    ] {
        let receipt = submit(&fixture.router, &envelope).await;
        assert_eq!(receipt["status"], "applied");
    }
    wait_for_task(&fixture.router, "paper-grid-owner", "running").await;
    wait_for_task(&fixture.router, "paper-arbitrage-owner", "running").await;

    assert_eq!(
        fixture.dispatcher.shutdown().await,
        crypto_trading_control_plane::SubmitDispatchOutcome::Applied
    );
    wait_for_task(&fixture.router, "paper-grid-owner", "stopped").await;
    wait_for_task(&fixture.router, "paper-arbitrage-owner", "stopped").await;

    let after_shutdown = submit(
        &fixture.router,
        &grid_start(
            Uuid::new_v4(),
            "shutdown-grid-restart",
            "paper-grid-owner",
            "grid.v1",
        ),
    )
    .await;
    assert_eq!(after_shutdown["status"], "rejected");

    drop(fixture.listener);
}
