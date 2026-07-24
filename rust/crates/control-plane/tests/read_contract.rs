use std::sync::{Arc, RwLock};

use crypto_trading_control_plane::{
    CONTROL_PLANE_EVENTS_SCHEMA_VERSION, CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION,
    ControlPlaneEventsError, ReadControlPlane,
};
use crypto_trading_runtime::{
    CapabilityLevel, CursorError, ExecutionBatch, ExecutionBatchState, JournalPageBoundary,
    JournalSnapshot, JournalSnapshotSource, ProjectionStatus,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn snapshot_is_deterministic_and_never_expands_live_authority() {
    let batch = ExecutionBatch::new(fixed_uuid(11), Vec::new()).unwrap();
    let source = MutableJournalSource::new(
        fixed_uuid(1),
        jsonl(&[
            execution_record(
                "execution_planned",
                &batch,
                &planned_details(&batch),
                "2026-07-24T00:00:00Z",
            ),
            execution_record(
                "execution_completed",
                &batch,
                &completed_details(batch.id()),
                "2026-07-24T00:00:01Z",
            ),
        ]),
    );
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let first = control_plane.snapshot().unwrap();
    let second = control_plane.snapshot().unwrap();

    assert_eq!(first, second);
    assert_eq!(first.schema_version, CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION);
    assert_eq!(first.capabilities, *control_plane.capabilities());
    assert!(!first.capabilities.live_trading_enabled);
    let web = first.capabilities.capability("control-plane.web").unwrap();
    assert_eq!(web.level, CapabilityLevel::Unavailable);
    assert!(
        web.blockers
            .iter()
            .any(|blocker| blocker.contains("HTTP, SSE"))
    );
    assert!(
        web.evidence
            .iter()
            .any(|path| path == "rust/crates/control-plane/tests/read_contract.rs")
    );
    assert_eq!(first.operator.projection_status, ProjectionStatus::Complete);
    assert_eq!(first.operator.batches.len(), 1);
    assert_eq!(
        first.operator.batches[0].state,
        ExecutionBatchState::Completed
    );
}

#[test]
fn opaque_cursor_resumes_after_an_external_append_without_replaying() {
    let source = MutableJournalSource::new(
        fixed_uuid(2),
        jsonl(&[decision_record("hold", &json!({}), "2026-07-24T00:00:00Z")]),
    );
    let writer = source.clone();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let first = control_plane.events_after(None).unwrap();
    let cursor = first.next_cursor.clone().unwrap();
    assert_eq!(first.schema_version, CONTROL_PLANE_EVENTS_SCHEMA_VERSION);
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].sequence, 1);

    writer.append(&decision_record(
        "alert",
        &json!({"severity": "warning"}),
        "2026-07-24T00:00:01Z",
    ));
    let resumed = control_plane.events_after(Some(&cursor)).unwrap();

    assert_eq!(resumed.events.len(), 1);
    assert_eq!(resumed.events[0].sequence, 2);
    assert_eq!(resumed.events[0].kind, "legacy_decision");
    assert_eq!(resumed.boundary, JournalPageBoundary::SnapshotEnd);
}

#[test]
fn event_pages_redact_payloads_by_construction() {
    let source = MutableJournalSource::new(
        fixed_uuid(3),
        jsonl(&[decision_record(
            "hold",
            &json!({
                "api_key": "super-secret",
                "authorization": "Bearer should-not-leak",
                "error": "private diagnostic",
            }),
            "2026-07-24T00:00:00Z",
        )]),
    );
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let page = control_plane.events_after(None).unwrap();
    let encoded = serde_json::to_string(&page).unwrap();

    assert!(encoded.contains("\"kind\":\"legacy_decision\""));
    for secret in [
        "api_key",
        "super-secret",
        "authorization",
        "should-not-leak",
        "private diagnostic",
    ] {
        assert!(!encoded.contains(secret), "{secret} leaked in {encoded}");
    }
}

#[test]
fn malformed_and_expired_cursors_never_restart_from_the_beginning() {
    let original = MutableJournalSource::new(
        fixed_uuid(4),
        jsonl(&[decision_record("hold", &json!({}), "2026-07-24T00:00:00Z")]),
    );
    let original_plane = ReadControlPlane::new(Arc::new(original)).unwrap();
    let cursor = original_plane
        .events_after(None)
        .unwrap()
        .next_cursor
        .unwrap();

    assert!(matches!(
        original_plane
            .events_after(Some("not-a-cursor"))
            .unwrap_err(),
        ControlPlaneEventsError::Cursor(CursorError::Malformed(_))
    ));

    let replacement = MutableJournalSource::new(
        fixed_uuid(5),
        jsonl(&[decision_record("hold", &json!({}), "2026-07-24T00:00:00Z")]),
    );
    let replacement_plane = ReadControlPlane::new(Arc::new(replacement)).unwrap();
    assert!(matches!(
        replacement_plane.events_after(Some(&cursor)).unwrap_err(),
        ControlPlaneEventsError::Cursor(CursorError::Expired)
    ));
}

#[test]
fn partial_tail_is_reported_without_emitting_an_incomplete_event() {
    let mut bytes = jsonl(&[decision_record("hold", &json!({}), "2026-07-24T00:00:00Z")]);
    bytes.extend_from_slice(br#"{"timestamp":"2026-07-24T00:00:01Z""#);
    let source = MutableJournalSource::new(fixed_uuid(6), bytes);
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let page = control_plane.events_after(None).unwrap();

    assert_eq!(page.events.len(), 1);
    assert!(matches!(
        page.boundary,
        JournalPageBoundary::PartialTail { bytes, .. } if bytes > 0
    ));
}

#[derive(Clone)]
struct MutableJournalSource {
    journal_id: Uuid,
    bytes: Arc<RwLock<Vec<u8>>>,
}

impl MutableJournalSource {
    fn new(journal_id: Uuid, bytes: Vec<u8>) -> Self {
        Self {
            journal_id,
            bytes: Arc::new(RwLock::new(bytes)),
        }
    }

    fn append(&self, record: &Value) {
        let mut bytes = self.bytes.write().unwrap();
        bytes.extend_from_slice(&serde_json::to_vec(record).unwrap());
        bytes.push(b'\n');
    }
}

impl JournalSnapshotSource for MutableJournalSource {
    fn snapshot(&self) -> Result<JournalSnapshot, crypto_trading_runtime::JournalReadError> {
        JournalSnapshot::new(self.journal_id, self.bytes.read().unwrap().clone())
    }
}

fn execution_record(
    decision: &str,
    batch: &ExecutionBatch,
    details: &Value,
    timestamp: &str,
) -> Value {
    debug_assert_eq!(details["batch_id"], json!(batch.id()));
    json!({
        "timestamp": timestamp,
        "strategy": "grid",
        "symbol": "BTC-USDT",
        "decision": decision,
        "details": details,
    })
}

fn planned_details(batch: &ExecutionBatch) -> Value {
    json!({
        "batch_id": batch.id(),
        "legs": [],
        "recovery_batch": batch,
        "context": {},
    })
}

fn completed_details(batch_id: Uuid) -> Value {
    json!({
        "batch_id": batch_id,
        "receipt_count": 0,
        "receipts": [],
        "receipts_truncated": false,
        "open": 0,
        "filled": 0,
        "cancelled": 0,
        "already_processed": 0,
    })
}

fn decision_record(decision: &str, details: &Value, timestamp: &str) -> Value {
    json!({
        "timestamp": timestamp,
        "strategy": "control-plane-test",
        "symbol": "BTC-USDT",
        "decision": decision,
        "details": details,
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

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}
