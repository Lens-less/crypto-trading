use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::history::{MAX_HISTORY_FILE_BYTES, stable_history_path_for_read};
use crate::{
    AggregateRef, CursorError, DecisionRecord, EventContractError, JournalCursor,
    MAX_HISTORY_RECORD_BYTES, OperationEventEnvelope,
};

pub const MAX_JOURNAL_SOURCE_BYTES: u64 = MAX_HISTORY_FILE_BYTES;
pub const MAX_JOURNAL_PAGE_EVENTS: usize = 256;
pub const MAX_JOURNAL_PAGE_BYTES: usize = 4 * 1_024 * 1_024;

const LEGACY_EVENT_PRODUCER: &str = "legacy_jsonl";
const LEGACY_FALLBACK_KIND: &str = "legacy_decision";
const EXECUTION_AGGREGATE_KIND: &str = "execution_batch";
const LEGACY_AGGREGATE_KIND: &str = "legacy_record";
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Immutable, bounded bytes captured from one append-only journal generation.
#[derive(Clone, Debug)]
pub struct JournalSnapshot {
    journal_id: Uuid,
    bytes: Vec<u8>,
}

impl JournalSnapshot {
    /// Creates an in-memory snapshot with an explicit durable generation ID.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] when the ID is nil or the bytes exceed the
    /// global journal source budget.
    pub fn new(journal_id: Uuid, bytes: Vec<u8>) -> Result<Self, JournalReadError> {
        validate_journal_id(journal_id)?;
        validate_source_len(bytes.len())?;
        Ok(Self { journal_id, bytes })
    }

    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Captures one immutable view of a journal for deterministic parsing.
pub trait JournalSnapshotSource {
    /// Reads a bounded snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] for invalid identity, I/O, allocation, or
    /// source-size failures.
    fn snapshot(&self) -> Result<JournalSnapshot, JournalReadError>;
}

/// Deterministic source adapter for tests and embedded fixtures.
#[derive(Clone, Debug)]
pub struct MemoryJournalSnapshotSource {
    snapshot: JournalSnapshot,
}

impl MemoryJournalSnapshotSource {
    /// Creates a bounded fixture source.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] under the same conditions as
    /// [`JournalSnapshot::new`].
    pub fn new(journal_id: Uuid, bytes: Vec<u8>) -> Result<Self, JournalReadError> {
        Ok(Self {
            snapshot: JournalSnapshot::new(journal_id, bytes)?,
        })
    }
}

impl JournalSnapshotSource for MemoryJournalSnapshotSource {
    fn snapshot(&self) -> Result<JournalSnapshot, JournalReadError> {
        Ok(self.snapshot.clone())
    }
}

/// File adapter that freezes the file length before reading.
///
/// Appends completed after the initial metadata read are deliberately excluded
/// from the returned snapshot. The caller supplies a durable generation ID; it
/// must change when a journal is intentionally replaced or rotated.
#[derive(Clone, Debug)]
pub struct FileJournalSnapshotSource {
    journal_id: Uuid,
    path: PathBuf,
}

impl FileJournalSnapshotSource {
    /// Creates a file source while resolving a relative path against the
    /// current directory once, rather than on every read.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] when the journal ID is nil or the current
    /// directory cannot be resolved.
    pub fn new(journal_id: Uuid, path: impl Into<PathBuf>) -> Result<Self, JournalReadError> {
        validate_journal_id(journal_id)?;
        let path = stable_history_path_for_read(&path.into());
        Ok(Self { journal_id, path })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl JournalSnapshotSource for FileJournalSnapshotSource {
    fn snapshot(&self) -> Result<JournalSnapshot, JournalReadError> {
        let file = File::open(&self.path).map_err(JournalReadError::Open)?;
        let metadata = file.metadata().map_err(JournalReadError::Metadata)?;
        if !metadata.is_file() {
            return Err(JournalReadError::NotAFile);
        }
        if metadata.len() > MAX_JOURNAL_SOURCE_BYTES {
            return Err(JournalReadError::SourceTooLarge {
                bytes: metadata.len(),
                limit: MAX_JOURNAL_SOURCE_BYTES,
            });
        }

        let expected_bytes =
            usize::try_from(metadata.len()).map_err(|_| JournalReadError::SourceTooLarge {
                bytes: metadata.len(),
                limit: MAX_JOURNAL_SOURCE_BYTES,
            })?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(expected_bytes)
            .map_err(|_| JournalReadError::Allocation {
                bytes: expected_bytes,
            })?;
        file.take(metadata.len())
            .read_to_end(&mut bytes)
            .map_err(JournalReadError::Read)?;
        if bytes.len() != expected_bytes {
            return Err(JournalReadError::SourceChanged {
                expected_bytes,
                actual_bytes: bytes.len(),
            });
        }
        JournalSnapshot::new(self.journal_id, bytes)
    }
}

/// Why a page stopped advancing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum JournalPageBoundary {
    SnapshotEnd,
    PartialTail { offset: u64, bytes: usize },
    PageLimit,
}

/// Bounded page of versioned operation events.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JournalPage {
    journal_id: Uuid,
    events: Vec<OperationEventEnvelope>,
    next_cursor: Option<JournalCursor>,
    boundary: JournalPageBoundary,
}

impl JournalPage {
    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    #[must_use]
    pub fn events(&self) -> &[OperationEventEnvelope] {
        &self.events
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&JournalCursor> {
        self.next_cursor.as_ref()
    }

    #[must_use]
    pub const fn boundary(&self) -> &JournalPageBoundary {
        &self.boundary
    }

    #[must_use]
    pub fn into_events(self) -> Vec<OperationEventEnvelope> {
        self.events
    }
}

/// Deterministically adapts legacy `DecisionRecord` JSONL into v1 events.
#[derive(Clone, Copy, Debug, Default)]
pub struct LegacyJsonlJournalReader;

impl LegacyJsonlJournalReader {
    /// Reads one bounded page and validates a supplied cursor against the
    /// physical record at its byte boundary.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] for malformed records, oversized records,
    /// stale cursor anchors, or event-contract failures. Only a final record
    /// without a newline is tolerated as a partial concurrent-write tail.
    pub fn read_page(
        snapshot: &JournalSnapshot,
        cursor: Option<&JournalCursor>,
    ) -> Result<JournalPage, JournalReadError> {
        let (mut offset, mut sequence) = if let Some(cursor) = cursor {
            cursor.validate_source_bounds(
                snapshot.journal_id,
                u64::try_from(snapshot.bytes.len()).unwrap_or(u64::MAX),
            )?;
            verify_cursor_anchor(snapshot, cursor)?;
            (
                usize::try_from(cursor.next_offset())
                    .map_err(|_| JournalReadError::Cursor(CursorError::Expired))?,
                cursor.after_sequence(),
            )
        } else {
            (0, 0)
        };

        let mut events = Vec::with_capacity(MAX_JOURNAL_PAGE_EVENTS);
        let mut logical_page_bytes = 0usize;
        let boundary;

        loop {
            if offset == snapshot.bytes.len() {
                boundary = JournalPageBoundary::SnapshotEnd;
                break;
            }
            if events.len() == MAX_JOURNAL_PAGE_EVENTS {
                boundary = JournalPageBoundary::PageLimit;
                break;
            }

            let Some(relative_newline) = snapshot.bytes[offset..]
                .iter()
                .position(|byte| *byte == b'\n')
            else {
                let tail_bytes = snapshot.bytes.len().saturating_sub(offset);
                if tail_bytes.saturating_add(1) > MAX_HISTORY_RECORD_BYTES {
                    return Err(JournalReadError::RecordTooLarge {
                        offset: to_u64(offset),
                        bytes: tail_bytes.saturating_add(1),
                        limit: MAX_HISTORY_RECORD_BYTES,
                    });
                }
                boundary = JournalPageBoundary::PartialTail {
                    offset: to_u64(offset),
                    bytes: tail_bytes,
                };
                break;
            };

            let line_end = offset.saturating_add(relative_newline);
            let next_offset = line_end.saturating_add(1);
            let line = trim_carriage_return(&snapshot.bytes[offset..line_end]);
            let logical_record_bytes = line.len().saturating_add(1);

            if line.is_empty() {
                return Err(JournalReadError::EmptyRecord {
                    sequence: sequence.saturating_add(1),
                    offset: to_u64(offset),
                });
            }
            if logical_record_bytes > MAX_HISTORY_RECORD_BYTES {
                return Err(JournalReadError::RecordTooLarge {
                    offset: to_u64(offset),
                    bytes: logical_record_bytes,
                    limit: MAX_HISTORY_RECORD_BYTES,
                });
            }
            if !events.is_empty()
                && logical_page_bytes.saturating_add(logical_record_bytes) > MAX_JOURNAL_PAGE_BYTES
            {
                boundary = JournalPageBoundary::PageLimit;
                break;
            }

            sequence = sequence
                .checked_add(1)
                .ok_or(JournalReadError::SequenceOverflow)?;
            let event = parse_legacy_event(snapshot.journal_id, sequence, offset, line)?;
            events.push(event);
            offset = next_offset;
            logical_page_bytes = logical_page_bytes.saturating_add(logical_record_bytes);
        }

        let next_cursor = match events.last() {
            Some(event) => Some(JournalCursor::after_event(event, to_u64(offset))?),
            None => cursor.cloned(),
        };
        Ok(JournalPage {
            journal_id: snapshot.journal_id,
            events,
            next_cursor,
            boundary,
        })
    }
}

fn verify_cursor_anchor(
    snapshot: &JournalSnapshot,
    cursor: &JournalCursor,
) -> Result<(), JournalReadError> {
    let target_offset = usize::try_from(cursor.next_offset())
        .map_err(|_| JournalReadError::Cursor(CursorError::Expired))?;
    let mut offset = 0usize;
    let mut sequence = 0u64;
    let mut last_event_id = None;

    while offset < target_offset {
        let Some(relative_newline) = snapshot.bytes[offset..target_offset]
            .iter()
            .position(|byte| *byte == b'\n')
        else {
            return Err(JournalReadError::Cursor(CursorError::Expired));
        };
        let line_end = offset.saturating_add(relative_newline);
        let next_offset = line_end.saturating_add(1);
        let line = trim_carriage_return(&snapshot.bytes[offset..line_end]);
        if line.is_empty() || line.len().saturating_add(1) > MAX_HISTORY_RECORD_BYTES {
            return Err(JournalReadError::Cursor(CursorError::Expired));
        }
        sequence = sequence
            .checked_add(1)
            .ok_or(JournalReadError::SequenceOverflow)?;
        let event = parse_legacy_event(snapshot.journal_id, sequence, offset, line)?;
        last_event_id = Some(event.event_id());
        offset = next_offset;
    }

    if offset != target_offset
        || sequence != cursor.after_sequence()
        || last_event_id != Some(cursor.last_event_id())
    {
        return Err(JournalReadError::Cursor(CursorError::Expired));
    }
    Ok(())
}

fn parse_legacy_event(
    journal_id: Uuid,
    sequence: u64,
    offset: usize,
    line: &[u8],
) -> Result<OperationEventEnvelope, JournalReadError> {
    let record = serde_json::from_slice::<DecisionRecord>(line).map_err(|source| {
        JournalReadError::MalformedRecord {
            sequence,
            offset: to_u64(offset),
            source,
        }
    })?;
    let event_id = deterministic_event_id(journal_id, sequence, line);
    let batch_id = legacy_batch_id(&record);
    let event_kind = if is_execution_decision(&record.decision) {
        record.decision.as_str()
    } else {
        LEGACY_FALLBACK_KIND
    };
    let aggregate = match batch_id {
        Some(batch_id) => AggregateRef::new(EXECUTION_AGGREGATE_KIND, batch_id),
        None => AggregateRef::new(LEGACY_AGGREGATE_KIND, event_id),
    }
    .map_err(|source| JournalReadError::EventContract { sequence, source })?;
    let payload = json!({
        "decision": record.decision,
        "strategy": record.strategy,
        "symbol": record.symbol,
        "details": record.details,
    });
    OperationEventEnvelope::new(
        journal_id,
        sequence,
        event_id,
        record.timestamp,
        event_kind,
        aggregate,
        LEGACY_EVENT_PRODUCER,
        payload,
    )
    .map_err(|source| JournalReadError::EventContract { sequence, source })
}

fn legacy_batch_id(record: &DecisionRecord) -> Option<Uuid> {
    if !is_execution_decision(&record.decision) {
        return None;
    }
    record
        .details
        .get("batch_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
}

fn is_execution_decision(decision: &str) -> bool {
    matches!(
        decision,
        "execution_planned"
            | "execution_completed"
            | "execution_partial"
            | "execution_incomplete"
            | "execution_failed"
    )
}

fn deterministic_event_id(journal_id: Uuid, sequence: u64, line: &[u8]) -> Uuid {
    let sequence = sequence.to_be_bytes();
    let left = fnv1a64_parts(&[b"legacy-event-left", journal_id.as_bytes(), &sequence, line]);
    let right = fnv1a64_parts(&[
        b"legacy-event-right",
        journal_id.as_bytes(),
        &sequence,
        line,
    ]);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&left.to_be_bytes());
    bytes[8..].copy_from_slice(&right.to_be_bytes());
    // RFC 9562 UUIDv8: application-defined bits with the standard variant.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn fnv1a64_parts(parts: &[&[u8]]) -> u64 {
    parts.iter().fold(FNV1A64_OFFSET_BASIS, |hash, part| {
        part.iter().fold(hash, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV1A64_PRIME)
        })
    })
}

fn trim_carriage_return(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn validate_journal_id(journal_id: Uuid) -> Result<(), JournalReadError> {
    if journal_id.is_nil() {
        return Err(JournalReadError::NilJournalId);
    }
    Ok(())
}

fn validate_source_len(bytes: usize) -> Result<(), JournalReadError> {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    if bytes > MAX_JOURNAL_SOURCE_BYTES {
        return Err(JournalReadError::SourceTooLarge {
            bytes,
            limit: MAX_JOURNAL_SOURCE_BYTES,
        });
    }
    Ok(())
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Debug, Error)]
pub enum JournalReadError {
    #[error("journal generation ID must not be nil")]
    NilJournalId,
    #[error("journal source has {bytes} bytes; maximum is {limit}")]
    SourceTooLarge { bytes: u64, limit: u64 },
    #[error("journal source path does not identify a regular file")]
    NotAFile,
    #[error("failed to open journal source: {0}")]
    Open(std::io::Error),
    #[error("failed to inspect journal source: {0}")]
    Metadata(std::io::Error),
    #[error("failed to reserve {bytes} bytes for a journal snapshot")]
    Allocation { bytes: usize },
    #[error("failed to read journal source: {0}")]
    Read(std::io::Error),
    #[error(
        "journal source changed while reading: expected {expected_bytes} bytes, read {actual_bytes}"
    )]
    SourceChanged {
        expected_bytes: usize,
        actual_bytes: usize,
    },
    #[error("journal record at offset {offset} has {bytes} bytes; maximum is {limit}")]
    RecordTooLarge {
        offset: u64,
        bytes: usize,
        limit: usize,
    },
    #[error("journal record {sequence} at offset {offset} is empty")]
    EmptyRecord { sequence: u64, offset: u64 },
    #[error("journal record {sequence} at offset {offset} is malformed: {source}")]
    MalformedRecord {
        sequence: u64,
        offset: u64,
        source: serde_json::Error,
    },
    #[error("journal sequence overflowed")]
    SequenceOverflow,
    #[error("journal event {sequence} violates the event contract: {source}")]
    EventContract {
        sequence: u64,
        source: EventContractError,
    },
    #[error(transparent)]
    Cursor(#[from] CursorError),
}
