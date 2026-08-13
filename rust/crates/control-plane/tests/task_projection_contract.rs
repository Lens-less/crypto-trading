use std::sync::Arc;

use crypto_trading_control_plane::{
    CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION, ReadControlPlane, ReadFailureKind,
};
use crypto_trading_runtime::{
    MAX_READ_ONLY_TASKS, MemoryJournalSnapshotSource, ProjectionStatus, ReadOnlyTaskExit,
    ReadOnlyTaskPhase, ReadOnlyTaskRecovery,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn snapshot_and_change_watermark_share_one_durable_task_generation() {
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
            running_sources(),
            None,
            None,
            "2026-07-25T00:00:01Z",
        ),
        task_record(
            "task_stopped",
            "stopped",
            3,
            stopped_sources(),
            Some("source_ended"),
            None,
            "2026-07-25T00:00:02Z",
        ),
    ]);
    let source = MemoryJournalSnapshotSource::new(Uuid::from_u128(701), bytes).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let read = control_plane.snapshot_with_events_after(None).unwrap();

    assert_eq!(
        read.snapshot.schema_version,
        CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION
    );
    assert_eq!(
        read.snapshot.tasks.projection_status,
        ProjectionStatus::Complete
    );
    assert_eq!(read.snapshot.tasks.journal_head_sequence, Some(3));
    assert_eq!(read.snapshot.tasks.tasks.len(), 1);
    let task = &read.snapshot.tasks.tasks[0];
    assert_eq!(task.task_id, "arb-btc-usdt");
    assert_eq!(task.phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(task.exit, Some(ReadOnlyTaskExit::SourceEnded));
    assert_eq!(task.recovery, ReadOnlyTaskRecovery::None);
    assert_eq!(task.processed_event_count, 3);
    assert_eq!(read.events.events.len(), 3);
    assert_eq!(read.events.events.last().unwrap().sequence, 3);
    assert!(read.events.next_cursor.is_some());
}

#[test]
fn malformed_task_fact_degrades_only_the_task_projection() {
    let mut malformed = task_record(
        "task_registered",
        "registered",
        0,
        registered_sources(),
        None,
        None,
        "2026-07-25T00:00:00Z",
    );
    malformed["details"]["phase"] = json!("raw-runtime-state");
    let source =
        MemoryJournalSnapshotSource::new(Uuid::from_u128(702), jsonl(&[malformed])).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let snapshot = control_plane.snapshot().unwrap();

    assert_eq!(snapshot.tasks.projection_status, ProjectionStatus::Degraded);
    assert_eq!(snapshot.tasks.invalid_event_count, 1);
    assert!(snapshot.tasks.tasks.is_empty());
    assert_eq!(
        snapshot.operator.projection_status,
        ProjectionStatus::Complete
    );
    assert_eq!(
        snapshot.monitor.projection_status,
        ProjectionStatus::Complete
    );
}

#[test]
fn task_cardinality_limit_maps_to_the_bounded_resource_failure() {
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
    let source = MemoryJournalSnapshotSource::new(Uuid::from_u128(703), jsonl(&records)).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let error = control_plane.snapshot().unwrap_err();

    assert_eq!(error.kind(), ReadFailureKind::ResourceLimit);
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
        source(None, "left", "starting", "unknown", 0, None),
        source(None, "right", "starting", "unknown", 0, None),
    ])
}

fn running_sources() -> Value {
    json!([
        source(
            Some("00000000-0000-0000-0000-000000000201"),
            "left",
            "running",
            "healthy",
            1,
            None,
        ),
        source(
            Some("00000000-0000-0000-0000-000000000202"),
            "right",
            "running",
            "degraded",
            2,
            None,
        ),
    ])
}

fn stopped_sources() -> Value {
    json!([
        source(
            Some("00000000-0000-0000-0000-000000000201"),
            "left",
            "stopped",
            "healthy",
            1,
            Some("source_ended"),
        ),
        source(
            Some("00000000-0000-0000-0000-000000000202"),
            "right",
            "stopped",
            "degraded",
            2,
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
    exit: Option<&str>,
) -> Value {
    json!({
        "schema_version": 1,
        "task_id": task_id,
        "source_id": source_id,
        "phase": phase,
        "health": health,
        "event_sequence": event_sequence,
        "consecutive_source_failures": u32::from(health == "degraded"),
        "last_event_at": if event_sequence == 0 {
            Value::Null
        } else {
            json!("2026-07-25T00:00:01Z")
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
