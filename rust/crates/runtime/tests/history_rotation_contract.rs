//! Sealed-segment rotation contract for the append-only operation journal.
//!
//! Rotation seals the active file as the next read-only chain segment and
//! continues in a fresh active file. These tests pin the durable facts: sealing
//! happens exactly at the file limit, replaying a rotated chain is
//! byte-for-byte identical to an unrotated journal, the cross-process writer
//! lease covers the whole chain, tampered or ambiguous chains fail closed, and
//! the segment/byte budgets only postpone — never cancel — the fail-closed
//! append limit.

use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use chrono::{TimeZone, Utc};
use crypto_trading_runtime::{
    CursorError, DecisionRecord, FileJournalSnapshotSource, HistoryError, JournalPageBoundary,
    JournalReadError, JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader,
    MAX_HISTORY_CHAIN_BYTES, MAX_HISTORY_FILE_BYTES, MAX_HISTORY_SEALED_SEGMENTS,
    MAX_JOURNAL_CHAIN_SOURCE_BYTES, MemoryJournalSnapshotSource, read_journal_chain,
};
use serde_json::json;
use uuid::Uuid;

const HOLD_LEASE_HELPER_TEST_NAME: &str = "hold_writer_lease_on_a_rotated_chain_until_released";
const HISTORY_PATH_ENV: &str = "JSONL_ROTATION_LOCK_PATH";
const READY_PATH_ENV: &str = "JSONL_ROTATION_LOCK_READY_PATH";
const RELEASE_PATH_ENV: &str = "JSONL_ROTATION_LOCK_RELEASE_PATH";

#[tokio::test]
async fn appends_across_the_file_limit_seal_a_segment_and_continue() {
    let root = temp_root("history-rotation-seal");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let history = JsonlHistory::new(&path);
    let record = record("rotation_fill", 0);
    let record_bytes = u64::try_from(encoded(&record).len()).unwrap();
    newline_terminated_fill(&path, MAX_HISTORY_FILE_BYTES - record_bytes);

    history.append(&record).await.unwrap();
    assert_eq!(file_len(&path), MAX_HISTORY_FILE_BYTES);
    assert!(!sealed(&path, 1).exists());

    history.append(&record).await.unwrap();
    assert_eq!(file_len(&sealed(&path, 1)), MAX_HISTORY_FILE_BYTES);
    assert_eq!(file_len(&path), record_bytes);

    history.append(&record).await.unwrap();
    assert_eq!(file_len(&path), record_bytes * 2);
    assert!(!sealed(&path, 2).exists());

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn rotated_chain_replays_byte_for_byte_like_an_unrotated_journal() {
    let root = temp_root("history-rotation-replay");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let history = JsonlHistory::new(&path);
    let records = (0..5)
        .map(|index| record("execution_planned", index))
        .collect::<Vec<_>>();
    let mut expected = Vec::new();
    for record in &records {
        expected.extend_from_slice(&encoded(record));
    }

    history.append(&records[0]).await.unwrap();
    history.append(&records[1]).await.unwrap();
    // Seal exactly as the writer does: rename the active file to the next
    // sealed sequence. This is also the crash point between sealing and
    // recreating the active file; the next append must rebuild from it.
    std::fs::rename(&path, sealed(&path, 1)).unwrap();
    history.append(&records[2]).await.unwrap();

    let journal_id = Uuid::from_bytes([7; 16]);
    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let mid_page = LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap();
    assert_eq!(mid_page.events().len(), 3);
    let cursor = mid_page.next_cursor().unwrap().clone();

    std::fs::rename(&path, sealed(&path, 2)).unwrap();
    history.append(&records[3]).await.unwrap();
    history.append(&records[4]).await.unwrap();

    // The rotated chain replays byte-for-byte like a never-rotated journal.
    assert_eq!(read_journal_chain(&path).unwrap(), expected);

    let chain_page =
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap();
    let memory = MemoryJournalSnapshotSource::new(journal_id, expected).unwrap();
    let memory_page =
        LegacyJsonlJournalReader::read_page(&memory.snapshot().unwrap(), None).unwrap();
    assert_eq!(chain_page, memory_page);
    assert_eq!(chain_page.boundary(), &JournalPageBoundary::SnapshotEnd);
    let sequences = chain_page
        .events()
        .iter()
        .map(crypto_trading_runtime::OperationEventEnvelope::sequence)
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![1, 2, 3, 4, 5]);

    // A cursor taken mid-chain resumes transparently across segment boundaries.
    let resumed =
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), Some(&cursor)).unwrap();
    assert_eq!(resumed.events().len(), 2);
    assert_eq!(resumed.events()[0].sequence(), 4);
    assert_eq!(resumed.boundary(), &JournalPageBoundary::SnapshotEnd);

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn sealed_chain_without_an_active_file_recovers_from_facts() {
    let root = temp_root("history-rotation-recover");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let first = encoded(&record("execution_planned", 0));
    std::fs::write(sealed(&path, 1), &first).unwrap();

    // Reads treat the missing active file as an empty tail behind the chain.
    assert_eq!(read_journal_chain(&path).unwrap(), first);
    let source = FileJournalSnapshotSource::new(Uuid::from_bytes([9; 16]), &path).unwrap();
    let page = LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap();
    assert_eq!(page.events().len(), 1);

    // The next append rebuilds the active file and continues the chain.
    let history = JsonlHistory::new(&path);
    let second = record("execution_completed", 1);
    history.append(&second).await.unwrap();
    let mut expected = first;
    expected.extend_from_slice(&encoded(&second));
    assert_eq!(read_journal_chain(&path).unwrap(), expected);

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn chain_gaps_and_empty_sealed_segments_fail_closed() {
    let gap_root = temp_root("history-rotation-gap");
    std::fs::create_dir_all(&gap_root).unwrap();
    let gap_path = gap_root.join("decisions.jsonl");
    let line = encoded(&record("execution_planned", 0));
    std::fs::write(sealed(&gap_path, 2), &line).unwrap();
    std::fs::write(&gap_path, &line).unwrap();

    assert!(matches!(
        read_journal_chain(&gap_path).unwrap_err(),
        JournalReadError::SealedSegmentGap { .. }
    ));
    let gap_history = JsonlHistory::new(&gap_path);
    let error = gap_history.append(&record("blocked", 1)).await.unwrap_err();
    assert!(matches!(error, HistoryError::SealedSegmentGap { .. }));
    assert_eq!(file_len(&gap_path), u64::try_from(line.len()).unwrap());

    let empty_root = temp_root("history-rotation-empty-segment");
    std::fs::create_dir_all(&empty_root).unwrap();
    let empty_path = empty_root.join("decisions.jsonl");
    std::fs::write(sealed(&empty_path, 1), b"").unwrap();
    std::fs::write(&empty_path, &line).unwrap();

    assert!(matches!(
        read_journal_chain(&empty_path).unwrap_err(),
        JournalReadError::SealedSegmentBytes { bytes: 0, .. }
    ));
    let empty_history = JsonlHistory::new(&empty_path);
    let error = empty_history
        .append(&record("blocked", 1))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        HistoryError::SealedSegmentBytes { bytes: 0, .. }
    ));

    drop((gap_history, empty_history));
    std::fs::remove_dir_all(gap_root).unwrap();
    std::fs::remove_dir_all(empty_root).unwrap();
}

#[test]
fn tampered_sealed_segments_fail_closed_for_readers() {
    let root = temp_root("history-rotation-tamper");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let mut segment = encoded(&record("execution_planned", 0));
    segment.extend_from_slice(&encoded(&record("execution_completed", 1)));
    std::fs::write(sealed(&path, 1), &segment).unwrap();
    std::fs::write(&path, encoded(&record("execution_partial", 2))).unwrap();

    let journal_id = Uuid::from_bytes([11; 16]);
    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();

    // Anchor a cursor at the segment boundary using only the sealed prefix.
    let prefix = MemoryJournalSnapshotSource::new(journal_id, segment.clone()).unwrap();
    let prefix_page =
        LegacyJsonlJournalReader::read_page(&prefix.snapshot().unwrap(), None).unwrap();
    let cursor = prefix_page.next_cursor().unwrap().clone();
    let resumed =
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), Some(&cursor)).unwrap();
    assert_eq!(resumed.events().len(), 1);
    assert_eq!(resumed.events()[0].sequence(), 3);

    // An equal-length rewrite inside the sealed segment expires the cursor.
    let mut rewritten = segment.clone();
    replace_last_equal_length(&mut rewritten, b"BTC-USDT", b"ETH-USDT");
    std::fs::write(sealed(&path, 1), &rewritten).unwrap();
    assert!(matches!(
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), Some(&cursor))
            .unwrap_err(),
        JournalReadError::Cursor(CursorError::Expired)
    ));

    // A structurally corrupted sealed record fails a full replay.
    let mut corrupted = segment.clone();
    corrupted[0] = b'X';
    std::fs::write(sealed(&path, 1), &corrupted).unwrap();
    assert!(matches!(
        LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap_err(),
        JournalReadError::MalformedRecord { sequence: 1, .. }
    ));

    // A sealed segment without a terminating newline cannot be trusted at all.
    let truncated = &segment[..segment.len() - 1];
    std::fs::write(sealed(&path, 1), truncated).unwrap();
    assert!(matches!(
        read_journal_chain(&path).unwrap_err(),
        JournalReadError::SealedSegmentPartialTail { .. }
    ));
    assert!(source.snapshot().is_err());

    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn segment_and_chain_budgets_fail_closed_without_writing() {
    assert_eq!(
        MAX_HISTORY_CHAIN_BYTES,
        (MAX_HISTORY_SEALED_SEGMENTS + 1) * MAX_HISTORY_FILE_BYTES
    );
    assert_eq!(MAX_JOURNAL_CHAIN_SOURCE_BYTES, MAX_HISTORY_CHAIN_BYTES);

    let root = temp_root("history-rotation-budget");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let line = encoded(&record("execution_planned", 0));
    for sequence in 1..=MAX_HISTORY_SEALED_SEGMENTS {
        std::fs::write(sealed(&path, sequence), &line).unwrap();
    }
    let overflow = record("rotation_overflow", 1);
    let overflow_bytes = u64::try_from(encoded(&overflow).len()).unwrap();
    newline_terminated_fill(&path, MAX_HISTORY_FILE_BYTES - overflow_bytes + 1);
    let before = file_len(&path);

    // A crossing append on a full chain fails closed instead of sealing.
    let history = JsonlHistory::new(&path);
    let error = history.append(&overflow).await.unwrap_err();
    assert!(matches!(
        error,
        HistoryError::TooManySegments {
            segments: MAX_HISTORY_SEALED_SEGMENTS,
            limit: MAX_HISTORY_SEALED_SEGMENTS,
        }
    ));
    assert_eq!(file_len(&path), before);
    assert!(!sealed(&path, MAX_HISTORY_SEALED_SEGMENTS + 1).exists());

    // A chain already past the segment budget fails closed on both sides.
    std::fs::write(sealed(&path, MAX_HISTORY_SEALED_SEGMENTS + 1), &line).unwrap();
    assert!(matches!(
        read_journal_chain(&path).unwrap_err(),
        JournalReadError::TooManySegments { .. }
    ));
    let error = history.append(&overflow).await.unwrap_err();
    assert!(matches!(error, HistoryError::TooManySegments { .. }));

    drop(history);
    std::fs::remove_dir_all(root).unwrap();

    // An oversized sealed segment breaks the per-file invariant and fails
    // closed for readers and writers alike.
    let oversized_root = temp_root("history-rotation-oversized-segment");
    std::fs::create_dir_all(&oversized_root).unwrap();
    let oversized_path = oversized_root.join("decisions.jsonl");
    let segment = std::fs::File::create(sealed(&oversized_path, 1)).unwrap();
    segment.set_len(MAX_HISTORY_FILE_BYTES + 1).unwrap();
    drop(segment);
    std::fs::write(&oversized_path, &line).unwrap();

    assert!(matches!(
        read_journal_chain(&oversized_path).unwrap_err(),
        JournalReadError::SealedSegmentBytes { .. }
    ));
    let oversized_history = JsonlHistory::new(&oversized_path);
    let error = oversized_history
        .append(&record("blocked", 2))
        .await
        .unwrap_err();
    assert!(matches!(error, HistoryError::SealedSegmentBytes { .. }));

    drop(oversized_history);
    std::fs::remove_dir_all(oversized_root).unwrap();
}

#[tokio::test]
async fn crash_left_partial_tail_is_truncated_at_the_last_complete_record_before_append() {
    let root = temp_root("history-rotation-partial-tail");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let history = JsonlHistory::new(&path);
    let first = record("execution_planned", 0);
    history.append(&first).await.unwrap();
    let partial = encoded(&record("execution_partial", 1));

    // Crash mid-write: the last record is cut off before its closing bytes.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(&partial[..partial.len() - 2]);
    std::fs::write(&path, &bytes).unwrap();

    // Readers still see a detectable, recoverable partial-tail boundary...
    let source = FileJournalSnapshotSource::new(Uuid::from_bytes([13; 16]), &path).unwrap();
    let page = LegacyJsonlJournalReader::read_page(&source.snapshot().unwrap(), None).unwrap();
    assert!(matches!(
        page.boundary(),
        JournalPageBoundary::PartialTail { .. }
    ));

    // ...and the writer quarantines only the crash-left tail before
    // continuing, preserving the valid prefix.
    let resumed = record("execution_completed", 2);
    history.append(&resumed).await.unwrap();
    let mut expected = encoded(&first);
    expected.extend_from_slice(&encoded(&resumed));
    assert_eq!(read_journal_chain(&path).unwrap(), expected);
    let quarantines = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|candidate| {
            candidate
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("decisions.jsonl.partial-tail.")
                        && name.ends_with(".quarantine")
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(quarantines.len(), 1);
    assert_eq!(
        std::fs::read(&quarantines[0]).unwrap(),
        partial[..partial.len() - 2]
    );

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn crash_left_first_active_record_after_rotation_is_quarantined_before_append() {
    let root = temp_root("history-rotation-unanchored-tail");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let sealed_record = record("execution_planned", 0);
    std::fs::write(sealed(&path, 1), encoded(&sealed_record)).unwrap();
    let interrupted = encoded(&record("execution_interrupted", 1));
    let fragment = &interrupted[..interrupted.len() - 2];
    std::fs::write(&path, fragment).unwrap();

    // A restart after rotation sees no newline anchor in the fresh active
    // file. The fragment is nevertheless an exact crash-left suffix: it must
    // be quarantined in full so the durable sealed prefix can keep running.
    let history = JsonlHistory::new(&path);
    let resumed = record("execution_resumed", 2);

    history.append(&resumed).await.unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), encoded(&resumed));
    let mut expected = encoded(&sealed_record);
    expected.extend_from_slice(&encoded(&resumed));
    assert_eq!(read_journal_chain(&path).unwrap(), expected);

    let quarantines = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|candidate| {
            candidate
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("decisions.jsonl.partial-tail.")
                        && name.ends_with(".quarantine")
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(quarantines.len(), 1);
    assert_eq!(std::fs::read(&quarantines[0]).unwrap(), fragment);

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn crash_inside_the_final_utf8_code_point_is_quarantined_before_append() {
    let root = temp_root("history-rotation-unanchored-utf8-tail");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let sealed_record = record("execution_planned", 0);
    std::fs::write(sealed(&path, 1), encoded(&sealed_record)).unwrap();

    let mut interrupted = record("placeholder", 1);
    interrupted.decision = "执行中断🚀".to_owned();
    let interrupted = encoded(&interrupted);
    let emoji = "🚀".as_bytes();
    let emoji_start = interrupted
        .windows(emoji.len())
        .rposition(|window| window == emoji)
        .unwrap();
    let fragment = &interrupted[..emoji_start + 2];
    let utf8_error = std::str::from_utf8(fragment).unwrap_err();
    assert_eq!(utf8_error.valid_up_to(), emoji_start);
    assert_eq!(utf8_error.error_len(), None);
    std::fs::write(&path, fragment).unwrap();

    // SIGKILL can split the final UTF-8 code point emitted by the serializer.
    // The incomplete code point is part of the exact crash-left suffix, not a
    // complete malformed fact that should poison every later restart.
    let history = JsonlHistory::new(&path);
    let resumed = record("execution_resumed", 2);
    history.append(&resumed).await.unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), encoded(&resumed));
    let mut expected = encoded(&sealed_record);
    expected.extend_from_slice(&encoded(&resumed));
    assert_eq!(read_journal_chain(&path).unwrap(), expected);
    let quarantines = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|candidate| {
            candidate
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("decisions.jsonl.partial-tail.")
                        && name.ends_with(".quarantine")
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(quarantines.len(), 1);
    assert_eq!(std::fs::read(&quarantines[0]).unwrap(), fragment);

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn definite_invalid_utf8_at_the_active_tail_still_fails_closed() {
    let root = temp_root("history-rotation-invalid-utf8-tail");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let mut corrupted_record = record("placeholder", 0);
    corrupted_record.decision = "执行中断🚀".to_owned();
    let encoded = encoded(&corrupted_record);
    let emoji = "🚀".as_bytes();
    let emoji_start = encoded
        .windows(emoji.len())
        .rposition(|window| window == emoji)
        .unwrap();
    let mut corrupted = encoded[..emoji_start].to_vec();
    corrupted.push(0xff);
    let utf8_error = std::str::from_utf8(&corrupted).unwrap_err();
    assert_eq!(utf8_error.valid_up_to(), emoji_start);
    assert_eq!(utf8_error.error_len(), Some(1));
    std::fs::write(&path, &corrupted).unwrap();
    let history = JsonlHistory::new(&path);

    let error = history.append(&record("blocked", 1)).await.unwrap_err();

    assert!(matches!(
        error,
        HistoryError::MalformedActiveRecord { line: 1, .. }
    ));
    assert_eq!(std::fs::read(&path).unwrap(), corrupted);
    assert_eq!(
        std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|candidate| {
                candidate
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".partial-tail."))
            })
            .count(),
        0
    );

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn invalid_utf8_inside_a_complete_active_record_still_fails_closed() {
    let root = temp_root("history-rotation-invalid-utf8-middle");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let mut corrupted_record = record("placeholder", 0);
    corrupted_record.decision = "执行中断🚀".to_owned();
    let mut corrupted = encoded(&corrupted_record);
    assert_eq!(corrupted.pop(), Some(b'\n'));
    let emoji = "🚀".as_bytes();
    let emoji_start = corrupted
        .windows(emoji.len())
        .rposition(|window| window == emoji)
        .unwrap();
    corrupted[emoji_start] = 0xff;
    let utf8_error = std::str::from_utf8(&corrupted).unwrap_err();
    assert_eq!(utf8_error.valid_up_to(), emoji_start);
    assert_eq!(utf8_error.error_len(), Some(1));
    assert!(emoji_start + 1 < corrupted.len());
    std::fs::write(&path, &corrupted).unwrap();
    let history = JsonlHistory::new(&path);

    let error = history.append(&record("blocked", 1)).await.unwrap_err();

    assert!(matches!(
        error,
        HistoryError::MalformedActiveRecord { line: 1, .. }
    ));
    assert_eq!(std::fs::read(&path).unwrap(), corrupted);

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn complete_active_record_without_a_terminator_is_preserved_before_append() {
    let root = temp_root("history-rotation-complete-unterminated");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let completed = record("execution_completed_before_crash", 0);
    let mut completed_without_terminator = encoded(&completed);
    assert_eq!(completed_without_terminator.pop(), Some(b'\n'));
    std::fs::write(&path, &completed_without_terminator).unwrap();

    // The JSON body is a complete durable fact. A crash may have lost only
    // its line terminator, so recovery must retain the fact instead of
    // quarantining it as though the body itself were incomplete.
    let history = JsonlHistory::new(&path);
    let resumed = record("execution_resumed", 1);
    history.append(&resumed).await.unwrap();

    let mut expected = encoded(&completed);
    expected.extend_from_slice(&encoded(&resumed));
    assert_eq!(read_journal_chain(&path).unwrap(), expected);
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(
        std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|candidate| {
                candidate
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".partial-tail."))
            })
            .count(),
        0
    );

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn complete_unterminated_tail_after_an_anchored_prefix_is_preserved() {
    let root = temp_root("history-rotation-complete-anchored-tail");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let first = record("execution_planned", 0);
    let completed = record("execution_completed_before_crash", 1);
    let mut bytes = encoded(&first);
    bytes.extend_from_slice(&serde_json::to_vec(&completed).unwrap());
    std::fs::write(&path, bytes).unwrap();

    let history = JsonlHistory::new(&path);
    let resumed = record("execution_resumed", 2);
    history.append(&resumed).await.unwrap();

    let mut expected = encoded(&first);
    expected.extend_from_slice(&encoded(&completed));
    expected.extend_from_slice(&encoded(&resumed));
    assert_eq!(read_journal_chain(&path).unwrap(), expected);

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn complete_malformed_unterminated_active_record_still_fails_closed() {
    let root = temp_root("history-rotation-malformed-unterminated");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let malformed = br#"{"decision":not-json}"#;
    std::fs::write(&path, malformed).unwrap();
    let history = JsonlHistory::new(&path);

    let error = history.append(&record("blocked", 0)).await.unwrap_err();

    assert!(matches!(
        error,
        HistoryError::MalformedActiveRecord { line: 1, .. }
    ));
    assert_eq!(std::fs::read(&path).unwrap(), malformed);
    assert_eq!(
        std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|candidate| {
                candidate
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".partial-tail."))
            })
            .count(),
        0
    );

    drop(history);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn recovery_refuses_to_truncate_past_a_complete_malformed_record() {
    let root = temp_root("history-rotation-malformed-prefix");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let history = JsonlHistory::new(&path);
    let mut bytes = encoded(&record("execution_planned", 0));
    bytes.extend_from_slice(b"{bad json}\n");
    bytes.extend_from_slice(br#"{"decision":"partial"#);
    std::fs::write(&path, &bytes).unwrap();

    let error = history.append(&record("blocked", 1)).await.unwrap_err();

    assert!(matches!(
        error,
        HistoryError::MalformedActiveRecord { line: 2, .. }
    ));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cross_process_writers_stay_excluded_across_rotation() {
    let root = temp_root("history-rotation-lease");
    std::fs::create_dir_all(&root).unwrap();
    let history_path = root.join("decisions.jsonl");
    std::fs::write(
        sealed(&history_path, 1),
        encoded(&record("execution_planned", 0)),
    )
    .unwrap();

    // A second process is excluded from the whole chain, not just one file.
    let ready_path = root.join("holder.ready");
    let release_path = root.join("holder.release");
    let mut child = spawn_lease_holder(&history_path, &ready_path, &release_path);
    wait_for_path(&ready_path);
    let blocked = JsonlHistory::new(&history_path);
    let error = runtime()
        .block_on(blocked.append(&record("blocked", 1)))
        .unwrap_err();
    assert!(matches!(
        &error,
        HistoryError::CrossProcessLockBusy { path }
            if path.file_name() == Some(OsStr::new("decisions.jsonl.jsonl.lock"))
    ));
    std::fs::write(&release_path, b"release").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "holder child failed: {status}");
    drop(blocked);

    // The recovering writer seals a segment under its own lease.
    let recovered = JsonlHistory::new(&history_path);
    let padding = record("rotation_fill", 2);
    let padding_bytes = u64::try_from(encoded(&padding).len()).unwrap();
    newline_terminated_fill(&history_path, MAX_HISTORY_FILE_BYTES - padding_bytes);
    runtime().block_on(recovered.append(&padding)).unwrap();
    runtime().block_on(recovered.append(&padding)).unwrap();
    assert_eq!(file_len(&sealed(&history_path, 2)), MAX_HISTORY_FILE_BYTES);
    assert_eq!(file_len(&history_path), padding_bytes);
    drop(recovered);

    // Exclusion is unchanged after rotation.
    let ready_path = root.join("holder-after.ready");
    let release_path = root.join("holder-after.release");
    let mut child = spawn_lease_holder(&history_path, &ready_path, &release_path);
    wait_for_path(&ready_path);
    let blocked = JsonlHistory::new(&history_path);
    let error = runtime()
        .block_on(blocked.append(&record("blocked", 3)))
        .unwrap_err();
    assert!(matches!(error, HistoryError::CrossProcessLockBusy { .. }));
    std::fs::write(&release_path, b"release").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "holder child failed: {status}");
    drop(blocked);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn hold_writer_lease_on_a_rotated_chain_until_released() {
    let Some(history_path) = std::env::var_os(HISTORY_PATH_ENV).map(PathBuf::from) else {
        return;
    };
    let ready_path = PathBuf::from(std::env::var_os(READY_PATH_ENV).unwrap());
    let release_path = PathBuf::from(std::env::var_os(RELEASE_PATH_ENV).unwrap());
    let history = JsonlHistory::new(&history_path);

    runtime()
        .block_on(history.append(&record("holder", 0)))
        .unwrap();
    std::fs::write(&ready_path, b"ready").unwrap();
    wait_for_path(&release_path);
}

fn spawn_lease_holder(
    history_path: &Path,
    ready_path: &Path,
    release_path: &Path,
) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(HOLD_LEASE_HELPER_TEST_NAME)
        .arg("--nocapture")
        .env(HISTORY_PATH_ENV, history_path)
        .env(READY_PATH_ENV, ready_path)
        .env(RELEASE_PATH_ENV, release_path)
        .env("RUST_TEST_THREADS", "1")
        .spawn()
        .unwrap()
}

fn record(decision: &str, index: i64) -> DecisionRecord {
    DecisionRecord {
        timestamp: Utc
            .timestamp_opt(1_785_500_000 + index, 0)
            .single()
            .unwrap(),
        strategy: "rotation-contract".to_owned(),
        symbol: "BTC-USDT".to_owned(),
        decision: decision.to_owned(),
        details: json!({ "index": index }),
    }
}

fn encoded(record: &DecisionRecord) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(record).unwrap();
    bytes.push(b'\n');
    bytes
}

/// Grows `path` to `len` bytes ending in a record terminator, so size fixtures
/// pass the writer's partial-tail check.
fn newline_terminated_fill(path: &Path, len: u64) {
    let file = std::fs::File::create(path).unwrap();
    file.set_len(len - 1).unwrap();
    drop(file);
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    std::io::Write::write_all(&mut file, b"\n").unwrap();
}

fn sealed(path: &Path, sequence: u64) -> PathBuf {
    let name = path.file_name().unwrap().to_str().unwrap();
    path.with_file_name(format!("{name}.{sequence}"))
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

fn temp_root(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {}", path.display());
}

fn replace_last_equal_length(bytes: &mut [u8], from: &[u8], to: &[u8]) {
    assert_eq!(from.len(), to.len());
    let offset = bytes
        .windows(from.len())
        .rposition(|window| window == from)
        .unwrap();
    bytes[offset..offset + to.len()].copy_from_slice(to);
}
