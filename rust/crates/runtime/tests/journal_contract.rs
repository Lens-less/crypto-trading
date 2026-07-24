use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_runtime::{
    AggregateRef, CursorError, EventContractError, JOURNAL_CURSOR_SCHEMA_VERSION, JournalCursor,
    MAX_JOURNAL_CURSOR_BYTES, MAX_OPERATION_EVENT_PAYLOAD_BYTES, OPERATION_EVENT_SCHEMA_VERSION,
    OperationEventEnvelope,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn v1_event_round_trips_as_a_deterministic_checked_envelope() {
    let event = fixed_event();

    assert_eq!(event.schema_version(), OPERATION_EVENT_SCHEMA_VERSION);
    assert_eq!(event.sequence(), 7);
    assert_eq!(event.kind(), "execution_planned");
    assert_eq!(event.aggregate().kind(), "execution_batch");
    assert_eq!(event.producer(), "cli");
    assert_eq!(event.integrity_algorithm(), "fnv1a64");
    assert_eq!(event.integrity_checksum(), "9ca0c2c9fe26dadb");
    event.validate().unwrap();

    let encoded = serde_json::to_vec(&event).unwrap();
    assert!(!encoded.windows(2).any(|bytes| bytes == b"\r\n"));
    let decoded: OperationEventEnvelope = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, event);
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), encoded);
}

#[test]
fn event_deserialization_rejects_content_or_integrity_tampering() {
    let event = fixed_event();
    let mut content = serde_json::to_value(&event).unwrap();
    content["payload"]["status"] = json!("completed");
    let error = serde_json::from_value::<OperationEventEnvelope>(content).unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error}");

    let mut integrity = serde_json::to_value(&event).unwrap();
    integrity["integrity"]["algorithm"] = json!("none");
    let error = serde_json::from_value::<OperationEventEnvelope>(integrity).unwrap_err();
    assert!(error.to_string().contains("integrity algorithm"), "{error}");

    let mut unknown = serde_json::to_value(&event).unwrap();
    unknown["future_authority"] = json!("live");
    let error = serde_json::from_value::<OperationEventEnvelope>(unknown).unwrap_err();
    assert!(error.to_string().contains("unknown field"), "{error}");
}

#[test]
fn event_constructor_rejects_invalid_identity_sequence_labels_and_payload_budget() {
    assert!(matches!(
        AggregateRef::new("execution_batch", Uuid::nil()),
        Err(EventContractError::NilId {
            field: "aggregate id"
        })
    ));

    let aggregate = AggregateRef::new("execution_batch", fixed_uuid(3)).unwrap();
    let invalid_sequence = OperationEventEnvelope::new(
        fixed_uuid(1),
        0,
        fixed_uuid(2),
        fixed_time(),
        "execution_planned",
        aggregate.clone(),
        "cli",
        Value::Null,
    );
    assert!(matches!(
        invalid_sequence,
        Err(EventContractError::InvalidSequence)
    ));

    let invalid_label = OperationEventEnvelope::new(
        fixed_uuid(1),
        1,
        fixed_uuid(2),
        fixed_time(),
        "Execution Planned",
        aggregate.clone(),
        "cli",
        Value::Null,
    );
    assert!(matches!(
        invalid_label,
        Err(EventContractError::InvalidLabel {
            field: "event kind"
        })
    ));

    let oversized = OperationEventEnvelope::new(
        fixed_uuid(1),
        1,
        fixed_uuid(2),
        fixed_time(),
        "execution_planned",
        aggregate,
        "cli",
        Value::String("x".repeat(MAX_OPERATION_EVENT_PAYLOAD_BYTES)),
    );
    assert!(matches!(
        oversized,
        Err(EventContractError::PayloadTooLarge { .. })
    ));
}

#[test]
fn cursor_round_trips_as_an_opaque_checked_transport_string() {
    let event = fixed_event();
    let cursor = JournalCursor::after_event(&event, 4_096).unwrap();
    let encoded = cursor.to_string();

    assert_eq!(JOURNAL_CURSOR_SCHEMA_VERSION, 1);
    assert!(encoded.len() <= MAX_JOURNAL_CURSOR_BYTES);
    assert_eq!(
        encoded,
        "ctc.1.01010101010101010101010101010101.0000000000000007.0000000000001000.02020202020202020202020202020202.6749874ed2fdbb1c"
    );
    assert!(!encoded.contains('\\'));
    assert!(!encoded.contains('/'));
    assert_eq!(encoded.split('.').nth(1), Some("1"));

    let parsed = JournalCursor::from_str(&encoded).unwrap();
    assert_eq!(parsed, cursor);
    assert_eq!(parsed.journal_id(), event.journal_id());
    assert_eq!(parsed.after_sequence(), event.sequence());
    assert_eq!(parsed.next_offset(), 4_096);
    assert_eq!(parsed.last_event_id(), event.event_id());

    let as_json = serde_json::to_string(&cursor).unwrap();
    assert_eq!(
        serde_json::from_str::<JournalCursor>(&as_json).unwrap(),
        cursor
    );
}

#[test]
fn cursor_detects_accidental_mutation_and_expired_source_bounds() {
    let event = fixed_event();
    let cursor = JournalCursor::after_event(&event, 4_096).unwrap();
    let encoded = cursor.to_string();
    let mut tampered = encoded.into_bytes();
    let index = tampered
        .iter()
        .position(|byte| *byte == b'7')
        .expect("fixed cursor contains the event sequence");
    tampered[index] = b'8';
    let tampered = String::from_utf8(tampered).unwrap();
    assert_eq!(
        JournalCursor::decode(&tampered).unwrap_err(),
        CursorError::ChecksumMismatch
    );

    cursor
        .validate_source_bounds(event.journal_id(), 4_096)
        .unwrap();
    assert_eq!(
        cursor
            .validate_source_bounds(fixed_uuid(9), 4_096)
            .unwrap_err(),
        CursorError::Expired
    );
    assert_eq!(
        cursor
            .validate_source_bounds(event.journal_id(), 4_095)
            .unwrap_err(),
        CursorError::Expired
    );
}

fn fixed_event() -> OperationEventEnvelope {
    OperationEventEnvelope::new(
        fixed_uuid(1),
        7,
        fixed_uuid(2),
        fixed_time(),
        "execution_planned",
        AggregateRef::new("execution_batch", fixed_uuid(3)).unwrap(),
        "cli",
        json!({
            "batch_id": fixed_uuid(3),
            "status": "planned",
        }),
    )
    .unwrap()
}

fn fixed_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 8, 30, 0)
        .single()
        .unwrap()
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}
