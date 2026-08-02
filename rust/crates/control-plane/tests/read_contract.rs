use std::sync::{
    Arc, RwLock,
    atomic::{AtomicUsize, Ordering},
};

use crypto_trading_control_plane::{
    CONTROL_PLANE_EVENTS_SCHEMA_VERSION, CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION,
    ControlPlaneEventsError, ReadControlPlane, ReadFailureKind,
};
use crypto_trading_runtime::{
    AccountRiskReadModel, ArbitrageMonitorReadModel, CapabilityAccess, CapabilityEnvironment,
    CapabilityLevel, CursorError, ExecutionBatch, ExecutionBatchState, JournalPageBoundary,
    JournalSnapshot, JournalSnapshotSource, MAX_JOURNAL_PAGE_EVENTS, MemoryJournalSnapshotSource,
    OperatorReadModel, PAPER_ACCOUNT_SCHEMA_VERSION, PRICE_ALERT_READ_MODEL_SCHEMA_VERSION,
    PaperAccountReadModel, PriceAlertReadModel, ProjectionStatus, ReadOnlyTaskReadModel,
    VirtualGridScannerReadModel, project_control_plane_state,
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
    assert_eq!(web.level, CapabilityLevel::Available);
    assert_eq!(
        web.scope.environments,
        vec![CapabilityEnvironment::Offline, CapabilityEnvironment::Paper]
    );
    assert_eq!(web.scope.access, CapabilityAccess::PaperTrading);
    assert!(
        web.blockers
            .iter()
            .any(|blocker| blocker.contains("mainnet authority are not exposed"))
    );
    assert!(
        web.evidence
            .iter()
            .any(|path| path == "rust/crates/web/tests/http_contract.rs")
    );
    assert_eq!(
        first.alerts.schema_version,
        PRICE_ALERT_READ_MODEL_SCHEMA_VERSION
    );
    assert!(first.alerts.occurrences.is_empty());
    assert_eq!(
        first.paper_accounts.schema_version,
        PAPER_ACCOUNT_SCHEMA_VERSION
    );
    assert!(first.paper_accounts.accounts.is_empty());
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
fn combined_read_uses_one_journal_generation_for_projection_and_watermark() {
    let batch = ExecutionBatch::new(fixed_uuid(12), Vec::new()).unwrap();
    let source = MutableJournalSource::new(
        fixed_uuid(7),
        jsonl(&[execution_record(
            "execution_planned",
            &batch,
            &planned_details(&batch),
            "2026-07-24T00:00:00Z",
        )]),
    );
    let observer = source.clone();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let read = control_plane.snapshot_with_events_after(None).unwrap();

    assert_eq!(observer.snapshot_count(), 1);
    assert_eq!(read.events.events.len(), 1);
    assert_eq!(read.events.events[0].sequence, 1);
    assert_eq!(read.snapshot.operator.head_sequence, Some(1));
    assert_eq!(read.snapshot.operator.batches.len(), 1);
    assert_eq!(read.snapshot.operator.batches[0].batch_id, batch.id());
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

    let malformed = original_plane
        .events_after(Some("not-a-cursor"))
        .unwrap_err();
    assert!(matches!(
        malformed,
        ControlPlaneEventsError::Cursor(CursorError::Malformed(_))
    ));
    assert_eq!(malformed.kind(), ReadFailureKind::InvalidCursor);

    let replacement = MutableJournalSource::new(
        fixed_uuid(5),
        jsonl(&[decision_record("hold", &json!({}), "2026-07-24T00:00:00Z")]),
    );
    let replacement_plane = ReadControlPlane::new(Arc::new(replacement)).unwrap();
    let expired = replacement_plane.events_after(Some(&cursor)).unwrap_err();
    assert!(matches!(
        expired,
        ControlPlaneEventsError::Cursor(CursorError::Expired)
    ));
    assert_eq!(expired.kind(), ReadFailureKind::ExpiredCursor);
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

#[test]
fn multi_page_snapshot_matches_legacy_models_with_one_shared_state_replay() {
    let source = multi_page_control_plane_source();
    let expected = source.snapshot().unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let projection = project_control_plane_state(&expected).unwrap();
    let stats = projection.stats;
    let snapshot = control_plane.snapshot().unwrap();

    assert_eq!(stats.state_page_reads, 2);
    assert_eq!(
        snapshot.schema_version,
        CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION
    );
    assert_eq!(snapshot.capabilities, *control_plane.capabilities());
    assert_eq!(
        snapshot.operator,
        OperatorReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        snapshot.monitor,
        ArbitrageMonitorReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        snapshot.alerts,
        PriceAlertReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        snapshot.tasks,
        ReadOnlyTaskReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        snapshot.scanner,
        VirtualGridScannerReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        snapshot.paper_accounts,
        PaperAccountReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        snapshot.account_risk,
        AccountRiskReadModel::from_legacy_snapshot(&expected).unwrap()
    );
}

#[test]
fn multi_page_combined_read_keeps_events_separate_from_one_shared_state_replay() {
    let source = multi_page_control_plane_source();
    let expected = source.snapshot().unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();

    let stats = project_control_plane_state(&expected).unwrap().stats;
    let read = control_plane.snapshot_with_events_after(None).unwrap();

    assert_eq!(stats.state_page_reads, 2);
    assert_eq!(
        read.snapshot.operator,
        OperatorReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        read.snapshot.monitor,
        ArbitrageMonitorReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        read.snapshot.alerts,
        PriceAlertReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        read.snapshot.tasks,
        ReadOnlyTaskReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        read.snapshot.scanner,
        VirtualGridScannerReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        read.snapshot.paper_accounts,
        PaperAccountReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(
        read.snapshot.account_risk,
        AccountRiskReadModel::from_legacy_snapshot(&expected).unwrap()
    );
    assert_eq!(read.events.events.len(), MAX_JOURNAL_PAGE_EVENTS);
    assert_eq!(read.events.events[0].sequence, 1);
    assert_eq!(read.events.boundary, JournalPageBoundary::PageLimit);
}

#[derive(Clone)]
struct MutableJournalSource {
    journal_id: Uuid,
    bytes: Arc<RwLock<Vec<u8>>>,
    snapshot_count: Arc<AtomicUsize>,
}

impl MutableJournalSource {
    fn new(journal_id: Uuid, bytes: Vec<u8>) -> Self {
        Self {
            journal_id,
            bytes: Arc::new(RwLock::new(bytes)),
            snapshot_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn append(&self, record: &Value) {
        let mut bytes = self.bytes.write().unwrap();
        bytes.extend_from_slice(&serde_json::to_vec(record).unwrap());
        bytes.push(b'\n');
    }

    fn snapshot_count(&self) -> usize {
        self.snapshot_count.load(Ordering::SeqCst)
    }
}

impl JournalSnapshotSource for MutableJournalSource {
    fn snapshot(&self) -> Result<JournalSnapshot, crypto_trading_runtime::JournalReadError> {
        self.snapshot_count.fetch_add(1, Ordering::SeqCst);
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

fn multi_page_control_plane_source() -> MemoryJournalSnapshotSource {
    MemoryJournalSnapshotSource::new(multi_page_journal_id(), multi_page_control_plane_bytes())
        .unwrap()
}

fn multi_page_control_plane_bytes() -> Vec<u8> {
    let mut records = (0..260)
        .map(|index| decision_record("hold", &json!({ "index": index }), "2026-07-24T00:00:00Z"))
        .collect::<Vec<_>>();
    records.extend(
        include_str!("../../../fixtures/web-api/journal.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()),
    );
    jsonl(&records)
}

fn multi_page_journal_id() -> Uuid {
    Uuid::parse_str("77777777-7777-4777-8777-777777777777").unwrap()
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}
