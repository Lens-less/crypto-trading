use std::{
    fs::{File, OpenOptions},
    io::Write,
};

use chrono::{TimeZone, Utc};
use crypto_trading_runtime::{
    CursorError, FileJournalSnapshotSource, JournalPageBoundary, JournalReadError, JournalSnapshot,
    JournalSnapshotSource, LegacyJsonlJournalReader, MAX_HISTORY_RECORD_BYTES,
    MAX_JOURNAL_PAGE_EVENTS, MAX_JOURNAL_SOURCE_BYTES, MemoryJournalSnapshotSource,
};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn file_and_memory_sources_capture_the_same_bounded_page() {
    let journal_id = fixed_uuid(1);
    let bytes = jsonl(
        &[
            decision_record("execution_planned", fixed_uuid(10), 0, "BTC-USDT"),
            decision_record("execution_completed", fixed_uuid(10), 1, "BTC-USDT"),
        ],
        LineEnding::Lf,
    );
    let memory = MemoryJournalSnapshotSource::new(journal_id, bytes.clone()).unwrap();
    let memory_page =
        LegacyJsonlJournalReader::read_page(&memory.snapshot().unwrap(), None).unwrap();

    let path = temp_path("journal-reader-adapters");
    std::fs::write(&path, bytes).unwrap();
    let file = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let file_page = LegacyJsonlJournalReader::read_page(&file.snapshot().unwrap(), None).unwrap();
    std::fs::remove_file(path).unwrap();

    assert_eq!(memory_page, file_page);
    assert_eq!(memory_page.events().len(), 2);
    assert_eq!(memory_page.boundary(), &JournalPageBoundary::SnapshotEnd);
    assert_eq!(memory_page.next_cursor().unwrap().after_sequence(), 2);
}

#[test]
fn line_endings_do_not_change_mapped_event_identity_or_content() {
    let journal_id = fixed_uuid(2);
    let records = [
        decision_record("execution_planned", fixed_uuid(20), 0, "ETH-USDT"),
        decision_record("execution_incomplete", fixed_uuid(20), 1, "ETH-USDT"),
    ];
    let lf = JournalSnapshot::new(journal_id, jsonl(&records, LineEnding::Lf)).unwrap();
    let crlf = JournalSnapshot::new(journal_id, jsonl(&records, LineEnding::CrLf)).unwrap();

    let lf_page = LegacyJsonlJournalReader::read_page(&lf, None).unwrap();
    let crlf_page = LegacyJsonlJournalReader::read_page(&crlf, None).unwrap();

    assert_eq!(lf_page.events(), crlf_page.events());
    assert_eq!(lf_page.boundary(), crlf_page.boundary());
    assert_eq!(
        lf_page.next_cursor().unwrap().after_sequence(),
        crlf_page.next_cursor().unwrap().after_sequence()
    );
    assert_eq!(
        lf_page.next_cursor().unwrap().last_event_id(),
        crlf_page.next_cursor().unwrap().last_event_id()
    );
    assert_ne!(
        lf_page.next_cursor().unwrap().next_offset(),
        crlf_page.next_cursor().unwrap().next_offset()
    );
}

#[test]
fn cursor_resumes_after_an_external_append_and_rejects_a_rewritten_anchor() {
    let journal_id = fixed_uuid(3);
    let first = jsonl(
        &[decision_record(
            "execution_planned",
            fixed_uuid(30),
            0,
            "BTC-USDT",
        )],
        LineEnding::Lf,
    );
    let second = jsonl(
        &[decision_record(
            "execution_completed",
            fixed_uuid(30),
            1,
            "BTC-USDT",
        )],
        LineEnding::Lf,
    );
    let path = temp_path("journal-reader-resume");
    std::fs::write(&path, &first).unwrap();
    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();

    let first_page =
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap();
    let cursor = first_page.next_cursor().unwrap().clone();
    let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
    writer.write_all(&second).unwrap();
    writer.flush().unwrap();

    let resumed =
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), Some(&cursor)).unwrap();
    assert_eq!(resumed.events().len(), 1);
    assert_eq!(resumed.events()[0].sequence(), 2);
    assert_eq!(resumed.events()[0].kind(), "execution_completed");

    let mut rewritten = first;
    rewritten.extend_from_slice(&second);
    replace_equal_length(&mut rewritten, b"BTC-USDT", b"ETH-USDT");
    let rewritten = JournalSnapshot::new(journal_id, rewritten).unwrap();
    assert!(matches!(
        LegacyJsonlJournalReader::read_page(&rewritten, Some(&cursor)).unwrap_err(),
        JournalReadError::Cursor(CursorError::Expired)
    ));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn partial_tail_does_not_advance_the_cursor_and_can_complete_later() {
    let journal_id = fixed_uuid(4);
    let first = jsonl(
        &[decision_record(
            "execution_planned",
            fixed_uuid(40),
            0,
            "SOL-USDT",
        )],
        LineEnding::Lf,
    );
    let second = jsonl(
        &[decision_record(
            "execution_partial",
            fixed_uuid(40),
            1,
            "SOL-USDT",
        )],
        LineEnding::Lf,
    );
    let split = second.len() / 2;
    let mut initial = first;
    initial.extend_from_slice(&second[..split]);
    let path = temp_path("journal-reader-tail");
    std::fs::write(&path, initial).unwrap();
    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();

    let page = LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap();
    let cursor = page.next_cursor().unwrap().clone();
    assert_eq!(page.events().len(), 1);
    assert!(matches!(
        page.boundary(),
        JournalPageBoundary::PartialTail { bytes, .. } if *bytes == split
    ));

    let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
    writer.write_all(&second[split..]).unwrap();
    writer.flush().unwrap();
    let resumed =
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), Some(&cursor)).unwrap();
    assert_eq!(resumed.events().len(), 1);
    assert_eq!(resumed.events()[0].kind(), "execution_partial");
    assert_eq!(resumed.boundary(), &JournalPageBoundary::SnapshotEnd);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn malformed_middle_records_and_oversized_records_fail_closed() {
    let journal_id = fixed_uuid(5);
    let valid = jsonl(
        &[decision_record(
            "execution_planned",
            fixed_uuid(50),
            0,
            "BTC-USDC",
        )],
        LineEnding::Lf,
    );
    let mut malformed = valid.clone();
    malformed.extend_from_slice(b"{not-json}\n");
    malformed.extend_from_slice(&valid);
    let malformed = JournalSnapshot::new(journal_id, malformed).unwrap();
    assert!(matches!(
        LegacyJsonlJournalReader::read_page(&malformed, None).unwrap_err(),
        JournalReadError::MalformedRecord { sequence: 2, .. }
    ));

    let mut oversized = vec![b'x'; MAX_HISTORY_RECORD_BYTES];
    oversized.push(b'\n');
    let oversized = JournalSnapshot::new(journal_id, oversized).unwrap();
    assert!(matches!(
        LegacyJsonlJournalReader::read_page(&oversized, None).unwrap_err(),
        JournalReadError::RecordTooLarge { .. }
    ));
}

#[test]
fn page_limit_resumes_without_skipping_the_next_physical_record() {
    let journal_id = fixed_uuid(6);
    let records = (0..=MAX_JOURNAL_PAGE_EVENTS)
        .map(|index| {
            json!({
                "timestamp": fixed_time(i64::try_from(index).unwrap()),
                "strategy": "grid",
                "symbol": "BTC-USDT",
                "decision": "hold",
                "details": {"index": index},
            })
        })
        .collect::<Vec<_>>();
    let snapshot = JournalSnapshot::new(journal_id, jsonl(&records, LineEnding::Lf)).unwrap();

    let first = LegacyJsonlJournalReader::read_page(&snapshot, None).unwrap();
    assert_eq!(first.events().len(), MAX_JOURNAL_PAGE_EVENTS);
    assert_eq!(first.boundary(), &JournalPageBoundary::PageLimit);
    let cursor = first.next_cursor().unwrap();
    assert_eq!(
        cursor.after_sequence(),
        u64::try_from(MAX_JOURNAL_PAGE_EVENTS).unwrap()
    );

    let second = LegacyJsonlJournalReader::read_page(&snapshot, Some(cursor)).unwrap();
    assert_eq!(second.events().len(), 1);
    assert_eq!(
        second.events()[0].sequence(),
        u64::try_from(MAX_JOURNAL_PAGE_EVENTS + 1).unwrap()
    );
    assert_eq!(second.boundary(), &JournalPageBoundary::SnapshotEnd);
}

#[test]
fn source_identity_and_size_budgets_are_enforced_before_reading() {
    assert!(matches!(
        MemoryJournalSnapshotSource::new(Uuid::nil(), Vec::new()).unwrap_err(),
        JournalReadError::NilJournalId
    ));

    let path = temp_path("journal-reader-oversized-source");
    let file = File::create(&path).unwrap();
    file.set_len(MAX_JOURNAL_SOURCE_BYTES + 1).unwrap();
    let source = FileJournalSnapshotSource::new(fixed_uuid(7), &path).unwrap();
    assert!(matches!(
        source.snapshot().unwrap_err(),
        JournalReadError::SourceTooLarge { .. }
    ));
    std::fs::remove_file(path).unwrap();
}

#[derive(Clone, Copy)]
enum LineEnding {
    Lf,
    CrLf,
}

fn jsonl(records: &[Value], line_ending: LineEnding) -> Vec<u8> {
    let delimiter = match line_ending {
        LineEnding::Lf => b"\n".as_slice(),
        LineEnding::CrLf => b"\r\n".as_slice(),
    };
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(&serde_json::to_vec(record).unwrap());
        bytes.extend_from_slice(delimiter);
    }
    bytes
}

fn decision_record(decision: &str, batch_id: Uuid, offset_seconds: i64, symbol: &str) -> Value {
    json!({
        "timestamp": fixed_time(offset_seconds),
        "strategy": "arbitrage",
        "symbol": symbol,
        "decision": decision,
        "details": {
            "batch_id": batch_id,
            "receipt_count": 1,
        },
    })
}

fn fixed_time(offset_seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_785_400_000 + offset_seconds, 0)
        .single()
        .unwrap()
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("{label}-{}.jsonl", Uuid::new_v4()))
}

fn replace_equal_length(bytes: &mut [u8], from: &[u8], to: &[u8]) {
    assert_eq!(from.len(), to.len());
    let offset = bytes
        .windows(from.len())
        .position(|window| window == from)
        .unwrap();
    bytes[offset..offset + to.len()].copy_from_slice(to);
}
