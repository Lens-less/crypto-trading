use crypto_trading_runtime::{
    JournalSnapshot, MAX_READ_ONLY_TASKS, ProjectionStatus,
    READ_ONLY_TASK_READ_MODEL_SCHEMA_VERSION, ReadModelError, ReadOnlyTaskExit,
    ReadOnlyTaskFailure, ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel,
    ReadOnlyTaskRecovery, ReadOnlyTaskSourceHealth, ReadOnlyTaskSourcePhase,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn lifecycle_projects_exact_sources_and_a_terminal_task() {
    let snapshot = snapshot(jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_running",
            "running",
            0,
            running_sources(0, "unknown", "unknown"),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_checkpointed",
            "running",
            2,
            running_sources(1, "healthy", "degraded"),
            None,
            None,
            "2026-07-25T00:00:02Z",
        ),
        task_record(
            "task_stopping",
            "stopping",
            2,
            stopping_sources(1),
            None,
            None,
            "2026-07-25T00:00:03Z",
        ),
        task_record(
            "task_stopped",
            "stopped",
            2,
            stopped_sources(1),
            Some("stop_requested"),
            None,
            "2026-07-25T00:00:04Z",
        ),
    ]));

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(
        model.schema_version,
        READ_ONLY_TASK_READ_MODEL_SCHEMA_VERSION
    );
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.journal_head_sequence, Some(5));
    assert_eq!(model.invalid_event_count, 0);
    assert_eq!(model.tasks.len(), 1);
    let task = &model.tasks[0];
    assert_eq!(task.task_id, "arb-btc-usdt");
    assert_eq!(task.kind, ReadOnlyTaskKind::ArbitrageMonitor);
    assert_eq!(task.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(task.recovery, ReadOnlyTaskRecovery::None);
    assert_eq!(task.processed_event_count, 2);
    assert_eq!(task.first_sequence, 1);
    assert_eq!(task.last_sequence, 5);
    assert_eq!(task.sources.len(), 2);
    assert_eq!(task.sources[0].source_id, "binance");
    assert_eq!(task.sources[0].phase, ReadOnlyTaskSourcePhase::Stopped);
    assert_eq!(task.sources[0].health, ReadOnlyTaskSourceHealth::Healthy);
    assert_eq!(task.sources[0].event_sequence, 1);
    assert_eq!(task.sources[1].source_id, "other");
    assert_eq!(task.sources[1].health, ReadOnlyTaskSourceHealth::Degraded);
    assert_eq!(task.exit, Some(ReadOnlyTaskExit::StopRequested));
    assert_eq!(task.failure, None);
}

#[test]
fn nonterminal_durable_state_is_recovered_as_unverified_not_auto_resumed() {
    let bytes = jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_running",
            "running",
            0,
            running_sources(0, "unknown", "unknown"),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_checkpointed",
            "running",
            1,
            running_sources(1, "healthy", "unknown"),
            None,
            None,
            "2026-07-25T00:00:02Z",
        ),
    ]);

    let first = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(bytes.clone())).unwrap();
    let restarted = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(bytes)).unwrap();

    assert_eq!(first, restarted);
    assert_eq!(restarted.tasks[0].phase, ReadOnlyTaskPhase::Running);
    assert_eq!(
        restarted.tasks[0].recovery,
        ReadOnlyTaskRecovery::Investigate
    );
    assert_eq!(restarted.tasks[0].processed_event_count, 1);
}

#[test]
fn normal_terminal_requires_every_source_to_be_durably_stopped() {
    let snapshot = snapshot(jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_running",
            "running",
            0,
            running_sources(0, "unknown", "unknown"),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_stopped",
            "stopped",
            0,
            running_sources(0, "unknown", "unknown"),
            Some("stop_requested"),
            None,
            "2026-07-25T00:00:02Z",
        ),
    ]));

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert_eq!(model.tasks[0].phase, ReadOnlyTaskPhase::Running);
    assert_eq!(model.tasks[0].recovery, ReadOnlyTaskRecovery::Investigate);
    assert_eq!(model.tasks[0].exit, None);
}

#[test]
fn conflicting_terminal_fact_degrades_and_marks_the_task_for_investigation() {
    let snapshot = snapshot(jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_running",
            "running",
            0,
            running_sources(0, "unknown", "unknown"),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_stopped",
            "stopped",
            0,
            stopped_sources(0),
            Some("source_ended"),
            None,
            "2026-07-25T00:00:02Z",
        ),
        task_record(
            "task_failed",
            "failed",
            0,
            stopped_sources(0),
            None,
            Some("source_contract"),
            "2026-07-25T00:00:03Z",
        ),
    ]));

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert_eq!(model.tasks.len(), 1);
    assert_eq!(model.tasks[0].phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(model.tasks[0].recovery, ReadOnlyTaskRecovery::Investigate);
}

#[test]
fn orphan_terminal_and_unknown_failure_do_not_fabricate_tasks() {
    let orphan = task_record(
        "task_failed",
        "failed",
        0,
        stopped_sources(0),
        None,
        Some("source_contract"),
        "2026-07-25T00:00:00Z",
    );
    let mut unknown = task_record(
        "task_failed",
        "failed",
        0,
        stopped_sources(0),
        None,
        Some("source_contract"),
        "2026-07-25T00:00:01Z",
    );
    unknown["details"]["failure"] = json!("raw-remote-error");
    unknown["details"]["task_id"] = json!("another-task");

    let model =
        ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(jsonl(&[orphan, unknown]))).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 2);
    assert!(model.tasks.is_empty());
}

#[test]
fn partial_tail_keeps_complete_facts_and_degrades_the_projection() {
    let mut bytes = jsonl(&[task_record(
        "task_registered",
        "registered",
        0,
        registered_sources(),
        None,
        None,
        "2026-07-25T00:00:00Z",
    )]);
    bytes.extend_from_slice(br#"{"timestamp":"2026-07-25T00:00:01Z""#);

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(bytes)).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 0);
    assert_eq!(model.tasks.len(), 1);
    assert_eq!(model.tasks[0].phase, ReadOnlyTaskPhase::Registered);
    assert_eq!(model.tasks[0].recovery, ReadOnlyTaskRecovery::Investigate);
}

#[test]
fn lf_and_crlf_project_the_same_task_state() {
    let lf = jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_failed",
            "failed",
            0,
            stopped_sources(0),
            None,
            Some("monitor_contract"),
            "2026-07-25T00:00:01Z",
        ),
    ]);
    let crlf = String::from_utf8(lf.clone())
        .unwrap()
        .replace('\n', "\r\n")
        .into_bytes();

    let lf_model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(lf)).unwrap();
    let crlf_model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(crlf)).unwrap();

    assert_eq!(lf_model, crlf_model);
    assert_eq!(lf_model.tasks[0].phase, ReadOnlyTaskPhase::Failed);
    assert_eq!(
        lf_model.tasks[0].failure,
        Some(ReadOnlyTaskFailure::MonitorContract)
    );
    assert_eq!(
        lf_model.tasks[0].recovery,
        ReadOnlyTaskRecovery::Investigate
    );
}

#[test]
fn shutdown_timeout_is_terminal_but_still_requires_investigation() {
    let snapshot = snapshot(jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_running",
            "running",
            0,
            running_sources(0, "unknown", "unknown"),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_stopped",
            "stopped",
            0,
            stopped_sources(0),
            Some("shutdown_timed_out"),
            None,
            "2026-07-25T00:00:02Z",
        ),
    ]));

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.tasks[0].phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(
        model.tasks[0].exit,
        Some(ReadOnlyTaskExit::ShutdownTimedOut)
    );
    assert_eq!(model.tasks[0].recovery, ReadOnlyTaskRecovery::Investigate);
}

#[test]
fn source_status_regression_degrades_without_advancing_the_last_valid_checkpoint() {
    let mut regressed = running_sources(1, "healthy", "degraded");
    regressed[0]["health"] = json!("degraded");
    let snapshot = snapshot(jsonl(&[
        task_record(
            "task_registered",
            "registered",
            0,
            registered_sources(),
            None,
            None,
            "2026-07-25T00:00:00Z",
        ),
        task_record(
            "task_running",
            "running",
            0,
            running_sources(0, "unknown", "unknown"),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_checkpointed",
            "running",
            1,
            running_sources(1, "healthy", "degraded"),
            None,
            None,
            "2026-07-25T00:00:02Z",
        ),
        task_record(
            "task_checkpointed",
            "running",
            2,
            regressed,
            None,
            None,
            "2026-07-25T00:00:03Z",
        ),
    ]));

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.invalid_event_count, 1);
    assert_eq!(model.tasks[0].processed_event_count, 1);
    assert_eq!(model.tasks[0].recovery, ReadOnlyTaskRecovery::Investigate);
}

#[test]
fn distinct_task_cardinality_is_hard_bounded() {
    let records = (0..=MAX_READ_ONLY_TASKS)
        .map(|index| {
            let mut record = task_record(
                "task_registered",
                "registered",
                0,
                registered_sources(),
                None,
                None,
                "2026-07-25T00:00:00Z",
            );
            record["details"]["task_id"] = json!(format!("task-{index}"));
            record
        })
        .collect::<Vec<_>>();

    let error =
        ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(jsonl(&records))).unwrap_err();

    assert!(matches!(
        error,
        ReadModelError::TaskLimitExceeded {
            limit: MAX_READ_ONLY_TASKS
        }
    ));
}

fn task_record(
    decision: &str,
    phase: &str,
    processed_event_count: u64,
    sources: Value,
    exit: Option<&str>,
    failure: Option<&str>,
    timestamp: &str,
) -> Value {
    let mut record = json!({
        "timestamp": timestamp,
        "strategy": "read_only_task",
        "symbol": "control-plane",
        "decision": decision,
        "details": {
            "schema_version": 1,
            "task_id": "arb-btc-usdt",
            "task_kind": "arbitrage_monitor",
            "phase": phase,
            "processed_event_count": processed_event_count,
            "sources": Value::Null,
            "exit": exit,
            "failure": failure,
        },
    });
    record["details"]["sources"] = sources;
    record
}

fn registered_sources() -> Value {
    json!([
        source(None, "binance", "starting", "unknown", 0, 0, None),
        source(None, "other", "starting", "unknown", 0, 0, None),
    ])
}

fn running_sources(event_sequence: u64, left_health: &str, right_health: &str) -> Value {
    json!([
        source(
            Some("00000000-0000-0000-0000-000000000101"),
            "binance",
            "running",
            left_health,
            event_sequence,
            0,
            None,
        ),
        source(
            Some("00000000-0000-0000-0000-000000000102"),
            "other",
            "running",
            right_health,
            event_sequence,
            u32::from(right_health == "degraded"),
            None,
        ),
    ])
}

fn stopping_sources(event_sequence: u64) -> Value {
    json!([
        source(
            Some("00000000-0000-0000-0000-000000000101"),
            "binance",
            "stopping",
            "healthy",
            event_sequence,
            0,
            None,
        ),
        source(
            Some("00000000-0000-0000-0000-000000000102"),
            "other",
            "stopping",
            "degraded",
            event_sequence,
            1,
            None,
        ),
    ])
}

fn stopped_sources(event_sequence: u64) -> Value {
    let left_health = if event_sequence == 0 {
        "unknown"
    } else {
        "healthy"
    };
    let right_health = if event_sequence == 0 {
        "unknown"
    } else {
        "degraded"
    };
    let right_failures = u32::from(event_sequence > 0);
    json!([
        source(
            Some("00000000-0000-0000-0000-000000000101"),
            "binance",
            "stopped",
            left_health,
            event_sequence,
            0,
            Some("stop_requested"),
        ),
        source(
            Some("00000000-0000-0000-0000-000000000102"),
            "other",
            "stopped",
            right_health,
            event_sequence,
            right_failures,
            Some("stop_requested"),
        ),
    ])
}

fn source(
    task_id: Option<&str>,
    source_id: &str,
    phase: &str,
    health: &str,
    event_sequence: u64,
    consecutive_source_failures: u32,
    exit: Option<&str>,
) -> Value {
    json!({
        "schema_version": 1,
        "task_id": task_id,
        "source_id": source_id,
        "phase": phase,
        "health": health,
        "event_sequence": event_sequence,
        "consecutive_source_failures": consecutive_source_failures,
        "last_event_at": if event_sequence == 0 {
            Value::Null
        } else {
            json!("2026-07-25T00:00:02Z")
        },
        "exit": exit,
    })
}

fn jsonl(records: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(&serde_json::to_vec(record).unwrap());
        bytes.push(b'\n');
    }
    bytes
}

fn snapshot(bytes: Vec<u8>) -> JournalSnapshot {
    JournalSnapshot::new(Uuid::from_u128(900), bytes).unwrap()
}
