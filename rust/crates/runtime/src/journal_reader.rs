use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::history::{
    MAX_HISTORY_CHAIN_BYTES, MAX_HISTORY_FILE_BYTES, MAX_HISTORY_SEALED_SEGMENTS, SealedChainError,
    SealedSegment, inspect_sealed_segments, stable_history_path_for_read,
};
use crate::{
    AggregateRef, CursorError, EventContractError, JournalCursor, MAX_HISTORY_RECORD_BYTES,
    OperationEventEnvelope,
};

/// Maximum size of one chain file (a sealed segment or the active file).
pub const MAX_JOURNAL_SOURCE_BYTES: u64 = MAX_HISTORY_FILE_BYTES;
/// Maximum total size of one journal chain captured into a snapshot.
pub const MAX_JOURNAL_CHAIN_SOURCE_BYTES: u64 = MAX_HISTORY_CHAIN_BYTES;
pub const MAX_JOURNAL_PAGE_EVENTS: usize = 256;
pub const MAX_JOURNAL_PAGE_BYTES: usize = 4 * 1_024 * 1_024;
pub const MAX_CURSOR_ANCHOR_SCAN_BYTES: usize =
    CURSOR_ANCHOR_CHECKPOINT_INTERVAL_BYTES + MAX_HISTORY_RECORD_BYTES + 1;

const LEGACY_EVENT_PRODUCER: &str = "legacy_jsonl";
const LEGACY_FALLBACK_KIND: &str = "legacy_decision";
const EXECUTION_AGGREGATE_KIND: &str = "execution_batch";
const LEGACY_AGGREGATE_KIND: &str = "legacy_record";
const CURSOR_ANCHOR_CHECKPOINT_INTERVAL_BYTES: usize = 256 * 1_024;
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;
const SLOW_JOURNAL_SNAPSHOT: Duration = Duration::from_millis(100);

/// Immutable, bounded bytes captured from one append-only journal generation.
#[derive(Clone, Debug)]
pub struct JournalSnapshot {
    journal_id: Uuid,
    bytes: Vec<u8>,
    anchor_checkpoints: Vec<AnchorCheckpoint>,
}

#[derive(Clone, Copy, Debug)]
struct AnchorCheckpoint {
    offset: usize,
    sequence: u64,
}

impl JournalSnapshot {
    /// Creates an in-memory snapshot with an explicit durable generation ID.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] when the ID is nil or the bytes exceed the
    /// journal chain budget [`MAX_JOURNAL_CHAIN_SOURCE_BYTES`].
    pub fn new(journal_id: Uuid, bytes: Vec<u8>) -> Result<Self, JournalReadError> {
        validate_journal_id(journal_id)?;
        validate_source_len(bytes.len())?;
        let anchor_checkpoints = build_anchor_checkpoints(&bytes);
        Ok(Self {
            journal_id,
            bytes,
            anchor_checkpoints,
        })
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

/// File adapter that captures a whole journal chain as one frozen snapshot.
///
/// Sealed segments `<path>.1 ..= <path>.N` are read in sequence order before
/// the active file, whose length is frozen before reading; appends completed
/// after that metadata read are deliberately excluded. Byte offsets, event
/// sequences, and cursor anchors are continuous across segment boundaries, and
/// a rotation completing mid-capture is detected and the capture retried, so
/// rotation is invisible to snapshot consumers. The caller supplies a durable
/// generation ID; it must change when a journal is intentionally replaced.
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
        let started = Instant::now();
        let result = read_chain_bytes(&self.path)
            .and_then(|bytes| JournalSnapshot::new(self.journal_id, bytes));
        let elapsed = started.elapsed();
        let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        match &result {
            Ok(snapshot) if elapsed >= SLOW_JOURNAL_SNAPSHOT => tracing::warn!(
                journal_id = %self.journal_id,
                snapshot_bytes = snapshot.len(),
                elapsed_ms,
                "journal snapshot capture exceeded the latency budget"
            ),
            Ok(snapshot) => tracing::debug!(
                journal_id = %self.journal_id,
                snapshot_bytes = snapshot.len(),
                elapsed_ms,
                "journal snapshot captured"
            ),
            Err(_) => tracing::warn!(
                journal_id = %self.journal_id,
                elapsed_ms,
                "journal snapshot capture failed closed"
            ),
        }
        result
    }
}

/// Reads one journal's sealed segments and active file as a single ordered,
/// bounded byte stream.
///
/// Sealed segments `<path>.1 ..= <path>.N` are concatenated in sequence order
/// before the active file, so replaying the stream is byte-for-byte identical
/// to replaying a journal that was never rotated. A missing active file behind
/// at least one sealed segment is the well-defined crash point between sealing
/// and recreating the active file and reads as an empty tail. A rotation that
/// completes while the chain is being captured is detected by re-inspecting
/// the sealed chain afterwards and the capture is retried, so a snapshot never
/// silently omits a just-sealed segment. Every other inconsistency — segment
/// gaps, empty, oversized, or unterminated sealed segments, or a chain past
/// the segment or byte budget — fails closed.
///
/// # Errors
///
/// Returns [`JournalReadError`] for I/O or allocation failures and for any
/// chain-integrity violation described above.
pub fn read_journal_chain(path: impl AsRef<Path>) -> Result<Vec<u8>, JournalReadError> {
    read_chain_bytes(&stable_history_path_for_read(path.as_ref()))
}

/// Bound on re-capturing a chain after a rotation landed mid-capture. Each
/// retry requires the writer to fill and seal a whole active file inside the
/// capture window, so the bound exists to guarantee termination rather than
/// because exhaustion is expected.
const MAX_CHAIN_CAPTURE_ATTEMPTS: usize = 3;

fn read_chain_bytes(path: &Path) -> Result<Vec<u8>, JournalReadError> {
    let mut rotated_totals = (0u64, 0u64);
    for _ in 0..MAX_CHAIN_CAPTURE_ATTEMPTS {
        let sealed = inspect_sealed_segments(path)?;
        let capture = capture_chain_once(path, &sealed)?;
        // A writer can seal the active file into segment N+1 between the
        // sealed-chain inspection and the active-file open; the capture would
        // then hold segments 1..N plus the new active file with the
        // just-sealed segment silently absent. Sealed segments are immutable,
        // so any difference in the re-inspection proves a rotation landed
        // mid-capture and the capture cannot be trusted.
        let recheck = inspect_sealed_segments(path)?;
        if !sealed_chains_match(&sealed, &recheck) {
            rotated_totals = (sealed_chain_bytes(&sealed), sealed_chain_bytes(&recheck));
            continue;
        }
        return match capture {
            ChainCapture::Full(bytes) => Ok(bytes),
            ChainCapture::MissingActive { bytes, source } => {
                if sealed.is_empty() {
                    Err(JournalReadError::Open(source))
                } else {
                    // Crash point between sealing and recreating the active
                    // file: the sealed chain is the complete durable record;
                    // the tail is empty.
                    Ok(bytes)
                }
            }
        };
    }
    Err(JournalReadError::SourceChanged {
        expected_bytes: usize::try_from(rotated_totals.0).unwrap_or(usize::MAX),
        actual_bytes: usize::try_from(rotated_totals.1).unwrap_or(usize::MAX),
    })
}

/// One attempted capture of the chain described by `sealed` plus the active
/// file; the caller must re-inspect the sealed chain before trusting it.
enum ChainCapture {
    /// Sealed segments plus the frozen active-file prefix.
    Full(Vec<u8>),
    /// Sealed segments with no active file behind them. The open error is
    /// preserved so an entirely absent journal keeps its original error.
    MissingActive {
        bytes: Vec<u8>,
        source: std::io::Error,
    },
}

fn capture_chain_once(
    path: &Path,
    sealed: &[SealedSegment],
) -> Result<ChainCapture, JournalReadError> {
    let mut bytes = Vec::new();
    let mut chain_bytes = 0u64;
    for segment in sealed {
        chain_bytes = chain_bytes.saturating_add(segment.bytes);
        validate_chain_budget(chain_bytes)?;
        append_sealed_segment(&segment.path, segment.bytes, &mut bytes)?;
        if !bytes.ends_with(b"\n") {
            return Err(JournalReadError::SealedSegmentPartialTail {
                path: segment.path.clone(),
            });
        }
    }

    match File::open(path) {
        Ok(file) => {
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
            chain_bytes = chain_bytes.saturating_add(metadata.len());
            validate_chain_budget(chain_bytes)?;
            append_frozen_prefix(file, metadata.len(), &mut bytes)?;
            Ok(ChainCapture::Full(bytes))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(ChainCapture::MissingActive { bytes, source })
        }
        Err(source) => Err(JournalReadError::Open(source)),
    }
}

/// Sealed segment paths are derived from their sequence numbers and segments
/// are immutable once sealed, so length plus per-segment byte counts decide
/// whether two inspections saw the same chain.
fn sealed_chains_match(before: &[SealedSegment], after: &[SealedSegment]) -> bool {
    before.len() == after.len()
        && before
            .iter()
            .zip(after)
            .all(|(before, after)| before.bytes == after.bytes)
}

fn sealed_chain_bytes(segments: &[SealedSegment]) -> u64 {
    segments
        .iter()
        .fold(0u64, |total, segment| total.saturating_add(segment.bytes))
}

fn append_sealed_segment(
    path: &Path,
    expected: u64,
    sink: &mut Vec<u8>,
) -> Result<(), JournalReadError> {
    let file = File::open(path).map_err(JournalReadError::Open)?;
    let metadata = file.metadata().map_err(JournalReadError::Metadata)?;
    if !metadata.is_file() {
        return Err(JournalReadError::SealedSegmentNotAFile {
            path: path.to_path_buf(),
        });
    }
    if metadata.len() != expected {
        return Err(JournalReadError::SourceChanged {
            expected_bytes: usize::try_from(expected).unwrap_or(usize::MAX),
            actual_bytes: usize::try_from(metadata.len()).unwrap_or(usize::MAX),
        });
    }
    append_frozen_prefix(file, expected, sink)
}

fn append_frozen_prefix(
    file: File,
    expected: u64,
    sink: &mut Vec<u8>,
) -> Result<(), JournalReadError> {
    let expected_bytes =
        usize::try_from(expected).map_err(|_| JournalReadError::SourceTooLarge {
            bytes: expected,
            limit: MAX_JOURNAL_SOURCE_BYTES,
        })?;
    sink.try_reserve_exact(expected_bytes)
        .map_err(|_| JournalReadError::Allocation {
            bytes: expected_bytes,
        })?;
    let before = sink.len();
    file.take(expected)
        .read_to_end(sink)
        .map_err(JournalReadError::Read)?;
    let actual_bytes = sink.len().saturating_sub(before);
    if actual_bytes != expected_bytes {
        return Err(JournalReadError::SourceChanged {
            expected_bytes,
            actual_bytes,
        });
    }
    Ok(())
}

const fn validate_chain_budget(bytes: u64) -> Result<(), JournalReadError> {
    if bytes > MAX_JOURNAL_CHAIN_SOURCE_BYTES {
        return Err(JournalReadError::ChainTooLarge {
            bytes,
            limit: MAX_JOURNAL_CHAIN_SOURCE_BYTES,
        });
    }
    Ok(())
}

impl From<SealedChainError> for JournalReadError {
    fn from(error: SealedChainError) -> Self {
        match error {
            SealedChainError::Inspect { path, source } => {
                Self::SealedSegmentInspect { path, source }
            }
            SealedChainError::NotAFile { path } => Self::SealedSegmentNotAFile { path },
            SealedChainError::Bytes { path, bytes } => Self::SealedSegmentBytes {
                path,
                bytes,
                limit: MAX_JOURNAL_SOURCE_BYTES,
            },
            SealedChainError::Gap { missing, found } => Self::SealedSegmentGap { missing, found },
            SealedChainError::TooManySegments { segments } => Self::TooManySegments {
                segments,
                limit: MAX_HISTORY_SEALED_SEGMENTS,
            },
        }
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
    let checkpoint = snapshot
        .anchor_checkpoints
        .iter()
        .rev()
        .find(|checkpoint| checkpoint.offset < target_offset)
        .copied()
        .unwrap_or(AnchorCheckpoint {
            offset: 0,
            sequence: 0,
        });
    if target_offset.saturating_sub(checkpoint.offset) > MAX_CURSOR_ANCHOR_SCAN_BYTES {
        return Err(JournalReadError::Cursor(CursorError::Expired));
    }

    let mut offset = checkpoint.offset;
    let mut sequence = checkpoint.sequence;
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
    let record = serde_json::from_slice::<LegacyDecisionRecord>(line).map_err(|source| {
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

pub(crate) fn event_from_decision_record(
    journal_id: Uuid,
    sequence: u64,
    record: &crate::DecisionRecord,
) -> Result<OperationEventEnvelope, JournalReadError> {
    let line = serde_json::to_vec(record).map_err(|source| JournalReadError::MalformedRecord {
        sequence,
        offset: 0,
        source,
    })?;
    let bytes = line.len().saturating_add(1);
    if bytes > MAX_HISTORY_RECORD_BYTES {
        return Err(JournalReadError::RecordTooLarge {
            offset: 0,
            bytes,
            limit: MAX_HISTORY_RECORD_BYTES,
        });
    }
    parse_legacy_event(journal_id, sequence, 0, &line)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyDecisionRecord {
    timestamp: DateTime<Utc>,
    strategy: String,
    symbol: String,
    decision: String,
    details: serde_json::Value,
}

fn legacy_batch_id(record: &LegacyDecisionRecord) -> Option<Uuid> {
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

fn build_anchor_checkpoints(bytes: &[u8]) -> Vec<AnchorCheckpoint> {
    let mut checkpoints = vec![AnchorCheckpoint {
        offset: 0,
        sequence: 0,
    }];
    let mut sequence = 0u64;
    let mut checkpoint_offset = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        sequence = sequence.saturating_add(1);
        let offset = index.saturating_add(1);
        if offset.saturating_sub(checkpoint_offset) >= CURSOR_ANCHOR_CHECKPOINT_INTERVAL_BYTES {
            checkpoints.push(AnchorCheckpoint { offset, sequence });
            checkpoint_offset = offset;
        }
    }
    checkpoints
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
    validate_chain_budget(u64::try_from(bytes).unwrap_or(u64::MAX))
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
    #[error("journal chain has {bytes} bytes; maximum is {limit}")]
    ChainTooLarge { bytes: u64, limit: u64 },
    #[error("journal chain has {segments} sealed segments; maximum is {limit}")]
    TooManySegments { segments: u64, limit: u64 },
    #[error("journal chain is missing sealed segment {missing} while {found} exists")]
    SealedSegmentGap { missing: PathBuf, found: PathBuf },
    #[error(
        "sealed journal segment {path} has {bytes} bytes; sealed segments must hold 1..={limit} bytes"
    )]
    SealedSegmentBytes {
        path: PathBuf,
        bytes: u64,
        limit: u64,
    },
    #[error("sealed journal segment {path} is not a regular file")]
    SealedSegmentNotAFile { path: PathBuf },
    #[error("sealed journal segment {path} does not end with a complete record")]
    SealedSegmentPartialTail { path: PathBuf },
    #[error("failed to inspect sealed journal segment {path}: {source}")]
    SealedSegmentInspect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_journal_root(prefix: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn test_sealed_segment_path(active_path: &Path, sequence: u64) -> PathBuf {
        let file_name = active_path.file_name().unwrap().to_string_lossy();
        active_path.with_file_name(format!("{file_name}.{sequence}"))
    }

    #[test]
    fn rotation_completed_during_capture_is_detected_and_the_read_retries() {
        let root = temp_journal_root("journal-capture-race");
        let path = root.join("journal.jsonl");
        std::fs::write(&path, b"{\"sealed\":1}\n").unwrap();

        // Freeze the reader's pre-capture view of the sealed chain, then let
        // the writer rotate: the active file becomes segment 1 and a fresh
        // active file continues the journal.
        let stale = inspect_sealed_segments(&path).unwrap();
        assert!(stale.is_empty());
        std::fs::rename(&path, test_sealed_segment_path(&path, 1)).unwrap();
        std::fs::write(&path, b"{\"active\":2}\n").unwrap();

        // The stale capture reads the new active file and silently misses the
        // just-sealed segment...
        let capture = capture_chain_once(&path, &stale).unwrap();
        let ChainCapture::Full(bytes) = capture else {
            panic!("expected a full capture of the new active file");
        };
        assert_eq!(bytes, b"{\"active\":2}\n");
        // ...which only the sealed-chain re-inspection can prove.
        let recheck = inspect_sealed_segments(&path).unwrap();
        assert!(!sealed_chains_match(&stale, &recheck));

        // The composed read re-inspects, retries, and returns the whole chain.
        assert_eq!(
            read_chain_bytes(&path).unwrap(),
            b"{\"sealed\":1}\n{\"active\":2}\n"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rotation_awaiting_the_recreated_active_file_is_detected_mid_capture() {
        let root = temp_journal_root("journal-capture-race-gap");
        let path = root.join("journal.jsonl");
        std::fs::write(&path, b"{\"sealed\":1}\n").unwrap();

        // The rename-to-recreate sub-window of rotation: the active file is
        // already sealed as segment 1 and the fresh active file does not
        // exist yet.
        let stale = inspect_sealed_segments(&path).unwrap();
        assert!(stale.is_empty());
        std::fs::rename(&path, test_sealed_segment_path(&path, 1)).unwrap();

        // Against the stale inspection this is indistinguishable from an
        // absent journal; the re-inspection exposes the rotation.
        let capture = capture_chain_once(&path, &stale).unwrap();
        let ChainCapture::MissingActive { bytes, .. } = capture else {
            panic!("expected the active file to be missing");
        };
        assert!(bytes.is_empty());
        let recheck = inspect_sealed_segments(&path).unwrap();
        assert!(!sealed_chains_match(&stale, &recheck));

        // The retried read lands on the documented crash-point contract: the
        // sealed chain is the complete durable record with an empty tail.
        assert_eq!(read_chain_bytes(&path).unwrap(), b"{\"sealed\":1}\n");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn absent_journal_keeps_its_not_found_open_error() {
        let root = temp_journal_root("journal-capture-absent");
        let path = root.join("journal.jsonl");

        // Loaders distinguish a fresh journal from the crash point by this
        // exact error; capture validation must not change it.
        let error = read_chain_bytes(&path).unwrap_err();
        assert!(matches!(
            error,
            JournalReadError::Open(source) if source.kind() == std::io::ErrorKind::NotFound
        ));

        std::fs::remove_dir_all(root).unwrap();
    }
}
