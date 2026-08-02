use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ffi::{OsStr, OsString},
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex as AsyncMutex,
};
use tracing::{debug, warn};

pub const MAX_HISTORY_RECORD_BYTES: usize = 1_048_576;
pub const MAX_HISTORY_BATCH_BYTES: usize = 8_388_608;
pub const MAX_HISTORY_FILE_BYTES: u64 = 64 * 1_024 * 1_024;
/// Maximum number of sealed, read-only segments in one journal chain.
///
/// Together with the active file this bounds a chain at 64 files. Sealed
/// segments are never compacted or rewritten, so this is also the bound on how
/// often rotation can postpone the fail-closed append limit.
pub const MAX_HISTORY_SEALED_SEGMENTS: u64 = 63;
/// Total byte budget for one journal chain (sealed segments plus the active
/// file): 64 files of at most 64 MiB each, i.e. 4 GiB of replayable facts.
///
/// The budget is the arithmetic closure of the per-file and segment-count
/// limits; it is still enforced independently so metadata anomalies or future
/// constant drift fail closed instead of silently unbounding the chain.
pub const MAX_HISTORY_CHAIN_BYTES: u64 = (MAX_HISTORY_SEALED_SEGMENTS + 1) * MAX_HISTORY_FILE_BYTES;

type PathLock = AsyncMutex<()>;
type CrossProcessLeaseState = StdMutex<Option<Arc<CrossProcessHistoryLease>>>;
const MIN_PATH_LOCK_CLEANUP_SIZE: usize = 64;
const DEAD_PATH_LOCK_CLEANUP_THRESHOLD: usize = 64;
const HISTORY_CROSS_PROCESS_LOCK_SUFFIX: &str = "jsonl.lock";
static DEAD_PATH_LOCK_HINT: AtomicUsize = AtomicUsize::new(0);
static PATH_LOCKS: OnceLock<StdMutex<PathLockRegistry>> = OnceLock::new();
static CROSS_PROCESS_LEASE_STATES: OnceLock<StdMutex<CrossProcessLeaseRegistry>> = OnceLock::new();
static APPEND_RECEIPTS: OnceLock<StdMutex<AppendReceiptRegistry>> = OnceLock::new();

struct ByteBudgetWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl ByteBudgetWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for ByteBudgetWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }

        let sentinel_limit = self.limit.saturating_add(1);
        let copy_len = input
            .len()
            .min(sentinel_limit.saturating_sub(self.bytes.len()));
        if copy_len > 0 {
            self.bytes
                .try_reserve(copy_len)
                .map_err(|_| io::Error::other("failed to reserve memory for a history record"))?;
            self.bytes.extend_from_slice(&input[..copy_len]);
        }
        if copy_len < input.len() || self.bytes.len() > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("history record byte limit exceeded"));
        }
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct CrossProcessHistoryLease {
    _lock_path: PathBuf,
    lock_file: std::fs::File,
}

impl Drop for CrossProcessHistoryLease {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

#[derive(Clone, Debug)]
enum CrossProcessLeaseFailure {
    CreateDirectory {
        kind: io::ErrorKind,
        message: String,
    },
    Open {
        path: PathBuf,
        kind: io::ErrorKind,
        message: String,
    },
    Claim {
        path: PathBuf,
        kind: io::ErrorKind,
        message: String,
    },
    Busy {
        path: PathBuf,
    },
}

impl CrossProcessLeaseFailure {
    fn from_create_directory(source: &io::Error) -> Self {
        Self::CreateDirectory {
            kind: source.kind(),
            message: source.to_string(),
        }
    }

    fn from_open(path: PathBuf, source: &io::Error) -> Self {
        Self::Open {
            path,
            kind: source.kind(),
            message: source.to_string(),
        }
    }

    fn from_claim(path: PathBuf, source: &io::Error) -> Self {
        Self::Claim {
            path,
            kind: source.kind(),
            message: source.to_string(),
        }
    }

    fn into_history_error(self) -> HistoryError {
        match self {
            Self::CreateDirectory { kind, message } => {
                HistoryError::CreateDirectory(io::Error::new(kind, message))
            }
            Self::Open {
                path,
                kind,
                message,
            } => HistoryError::LockOpen {
                path,
                source: io::Error::new(kind, message),
            },
            Self::Claim {
                path,
                kind,
                message,
            } => HistoryError::LockClaim {
                path,
                source: io::Error::new(kind, message),
            },
            Self::Busy { path } => HistoryError::CrossProcessLockBusy { path },
        }
    }
}

/// One sealed, read-only member of a journal chain, described by metadata only.
#[derive(Clone, Debug)]
pub(crate) struct SealedSegment {
    pub(crate) path: PathBuf,
    pub(crate) bytes: u64,
}

/// Why a sealed-segment chain could not be trusted.
///
/// Chain inspection rebuilds state from on-disk facts; any layout that the
/// writer could not itself have produced is ambiguous and must fail closed.
#[derive(Debug)]
pub(crate) enum SealedChainError {
    Inspect { path: PathBuf, source: io::Error },
    NotAFile { path: PathBuf },
    Bytes { path: PathBuf, bytes: u64 },
    Gap { missing: PathBuf, found: PathBuf },
    TooManySegments { segments: u64 },
}

impl SealedChainError {
    fn into_history_error(self) -> HistoryError {
        match self {
            Self::Inspect { path, source } => HistoryError::SegmentInspect { path, source },
            Self::NotAFile { path } => HistoryError::SealedSegmentNotAFile { path },
            Self::Bytes { path, bytes } => HistoryError::SealedSegmentBytes {
                path,
                bytes,
                limit: MAX_HISTORY_FILE_BYTES,
            },
            Self::Gap { missing, found } => HistoryError::SealedSegmentGap { missing, found },
            Self::TooManySegments { segments } => HistoryError::TooManySegments {
                segments,
                limit: MAX_HISTORY_SEALED_SEGMENTS,
            },
        }
    }
}

/// Maps an active journal path onto the sealed segment for `sequence`.
pub(crate) fn sealed_segment_path(active_path: &Path, sequence: u64) -> PathBuf {
    let mut file_name = history_file_name(active_path);
    file_name.push(format!(".{sequence}"));
    active_path.with_file_name(file_name)
}

/// Enumerates the sealed chain `<path>.1 ..= <path>.N` from filesystem facts.
///
/// A contiguous run starting at `1` is the only layout the writer produces, so
/// gaps, empty or oversized segments, non-files, and chains beyond the segment
/// budget all fail closed rather than being repaired or skipped.
pub(crate) fn inspect_sealed_segments(
    active_path: &Path,
) -> Result<Vec<SealedSegment>, SealedChainError> {
    let parent = active_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut segments = BTreeMap::new();
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(SealedChainError::Inspect {
                path: parent.to_path_buf(),
                source,
            });
        }
    };

    for entry in entries {
        let entry = entry.map_err(|source| SealedChainError::Inspect {
            path: parent.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Some(sequence) = sealed_segment_sequence(active_path, &entry.file_name(), &path)?
        else {
            continue;
        };
        if !(1..=MAX_HISTORY_SEALED_SEGMENTS).contains(&sequence) {
            return Err(SealedChainError::TooManySegments {
                segments: MAX_HISTORY_SEALED_SEGMENTS.saturating_add(1),
            });
        }
        let metadata = entry
            .metadata()
            .map_err(|source| SealedChainError::Inspect {
                path: path.clone(),
                source,
            })?;
        if !metadata.is_file() {
            return Err(SealedChainError::NotAFile { path });
        }
        if metadata.len() == 0 || metadata.len() > MAX_HISTORY_FILE_BYTES {
            return Err(SealedChainError::Bytes {
                path,
                bytes: metadata.len(),
            });
        }
        if segments
            .insert(
                sequence,
                SealedSegment {
                    path,
                    bytes: metadata.len(),
                },
            )
            .is_some()
        {
            return Err(duplicate_sealed_sequence(&candidate_path_for_sequence(
                active_path,
                sequence,
            )));
        }
    }

    let mut contiguous = Vec::with_capacity(segments.len());
    let mut expected = 1u64;
    for (sequence, segment) in segments {
        if sequence != expected {
            return Err(SealedChainError::Gap {
                missing: sealed_segment_path(active_path, expected),
                found: segment.path,
            });
        }
        contiguous.push(segment);
        expected = expected.saturating_add(1);
    }
    Ok(contiguous)
}

const fn ensure_chain_budget(
    sealed_bytes: u64,
    active_bytes: u64,
    batch_bytes: u64,
) -> Result<(), HistoryError> {
    let chain_bytes = sealed_bytes
        .saturating_add(active_bytes)
        .saturating_add(batch_bytes);
    if chain_bytes > MAX_HISTORY_CHAIN_BYTES {
        return Err(HistoryError::ChainTooLarge {
            chain_bytes,
            batch_bytes,
            limit: MAX_HISTORY_CHAIN_BYTES,
        });
    }
    Ok(())
}

struct PathLockRegistry {
    locks: HashMap<PathBuf, Weak<PathLock>>,
    next_cleanup_size: usize,
}

impl Default for PathLockRegistry {
    fn default() -> Self {
        Self {
            locks: HashMap::new(),
            next_cleanup_size: MIN_PATH_LOCK_CLEANUP_SIZE,
        }
    }
}

struct CrossProcessLeaseRegistry {
    leases: HashMap<PathBuf, Weak<CrossProcessLeaseState>>,
    next_cleanup_size: usize,
}

impl Default for CrossProcessLeaseRegistry {
    fn default() -> Self {
        Self {
            leases: HashMap::new(),
            next_cleanup_size: MIN_PATH_LOCK_CLEANUP_SIZE,
        }
    }
}

const MAX_APPEND_RECEIPTS_PER_PATH: usize = 256;
const MAX_APPEND_RECEIPT_RECORDS: usize = 2_048;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HistoryChainHead {
    pub(crate) sealed_segment_bytes: Vec<u64>,
    pub(crate) active_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct HistoryDelta {
    pub(crate) head_before: HistoryChainHead,
    pub(crate) head_after: HistoryChainHead,
    pub(crate) records: Vec<DecisionRecord>,
}

#[derive(Clone, Debug)]
struct AppendReceiptEntry {
    head_before: HistoryChainHead,
    head_after: HistoryChainHead,
    records: Vec<DecisionRecord>,
}

#[derive(Default)]
struct AppendReceiptRegistry {
    receipts: HashMap<PathBuf, VecDeque<AppendReceiptEntry>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DecisionRecord {
    pub timestamp: DateTime<Utc>,
    pub strategy: String,
    pub symbol: String,
    pub decision: String,
    pub details: Value,
}

/// Process-local serialized JSONL history.
///
/// Every successful append is flushed and `sync_data` is completed before the
/// call returns. Clones and separately constructed handles for the same
/// canonical path share one in-process lock, including lexical aliases and
/// aliases through an existing parent directory. Cross-process writers share a
/// dedicated sibling lock file and fail closed immediately when another
/// process already owns that lease. The lease is held for the whole chain:
/// sealed segments `<path>.1 ..= <path>.N` plus the active file.
///
/// When an append would push the active file past [`MAX_HISTORY_FILE_BYTES`],
/// the active file is first sealed by renaming it to the next segment in the
/// chain and a fresh active file continues the journal. Sealed segments are
/// read-only facts: they are never rewritten, truncated, or compacted, so the
/// append-only replay invariant extends across the whole chain. Rotation only
/// postpones the fail-closed limit; once [`MAX_HISTORY_SEALED_SEGMENTS`] or
/// [`MAX_HISTORY_CHAIN_BYTES`] would be exceeded, appends are rejected again.
///
/// Locks are keyed by normalized paths rather than filesystem object identity.
/// Existing hard links and paths retargeted after construction can therefore
/// still refer to one file through separate locks. Windows case folding is a
/// conservative Unicode approximation, not an NTFS file-identity lookup.
#[derive(Clone, Debug)]
pub struct JsonlHistory {
    path: PathBuf,
    lock_key: PathBuf,
    path_lock: Arc<PathLock>,
    cross_process_lease_state: Arc<CrossProcessLeaseState>,
    startup_failure: Option<CrossProcessLeaseFailure>,
}

#[derive(Clone, Copy, Debug, Default)]
struct AppendBatchStats {
    batch_bytes: u64,
    rotated: bool,
}

struct AppendTarget {
    file: tokio::fs::File,
    head_before: HistoryChainHead,
    head_after: HistoryChainHead,
}

impl Drop for JsonlHistory {
    fn drop(&mut self) {
        if Arc::strong_count(&self.path_lock) == 1 {
            DEAD_PATH_LOCK_HINT.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl JsonlHistory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = stable_history_path(&path.into());
        let lock_key = normalized_lock_key(&path);
        let path_lock = shared_path_lock(lock_key.clone());
        let cross_process_lease_state = shared_cross_process_lease_state(lock_key.clone());
        let startup_failure = prime_cross_process_lease(&path, &cross_process_lease_state).err();
        Self {
            path,
            lock_key,
            path_lock,
            cross_process_lease_state,
            startup_failure,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Quarantines and truncates a crash-left partial active tail when a valid
    /// complete-record prefix anchors the repair.
    ///
    /// Authority refreshes call this before replay so a normal process restart
    /// can recover without first attempting a write. An unanchored fragment or
    /// malformed complete prefix remains fail-closed.
    pub(crate) async fn repair_recoverable_tail(&self) -> Result<(), HistoryError> {
        let _guard = self.path_lock.lock().await;
        let _lease = self.active_cross_process_lease()?;
        let mut file = match tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .await
        {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => return Err(HistoryError::Open(source)),
        };
        let existing_bytes = file.metadata().await.map_err(HistoryError::Metadata)?.len();
        if existing_bytes > MAX_HISTORY_FILE_BYTES {
            return Err(HistoryError::FileTooLarge {
                existing_bytes,
                batch_bytes: 0,
                limit: MAX_HISTORY_FILE_BYTES,
            });
        }
        let retained_bytes = self
            .ensure_active_tail_is_complete(&mut file, existing_bytes)
            .await?;
        if retained_bytes < existing_bytes {
            drop(file);
            self.truncate_active_tail(retained_bytes).await?;
        }
        Ok(())
    }

    pub(crate) async fn inspect_chain_head(&self) -> Result<HistoryChainHead, HistoryError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || inspect_chain_head(&path))
            .await
            .map_err(|_| {
                HistoryError::Metadata(io::Error::other("history head inspection task failed"))
            })?
    }

    pub(crate) fn same_process_delta_since(&self, head: &HistoryChainHead) -> Option<HistoryDelta> {
        let registry =
            APPEND_RECEIPTS.get_or_init(|| StdMutex::new(AppendReceiptRegistry::default()));
        let registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let receipts = registry.receipts.get(&self.lock_key)?;
        let start = receipts
            .iter()
            .position(|entry| &entry.head_before == head)?;
        let mut records = Vec::new();
        let mut expected = head.clone();
        let mut head_after = None;
        for entry in receipts.iter().skip(start) {
            if entry.head_before != expected {
                return None;
            }
            records.extend(entry.records.iter().cloned());
            expected = entry.head_after.clone();
            head_after = Some(expected.clone());
        }
        Some(HistoryDelta {
            head_before: head.clone(),
            head_after: head_after?,
            records,
        })
    }

    /// Appends one JSON record and syncs its data to the file before returning.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] when validation, serialization, directory
    /// creation, opening, writing, flushing, or syncing fails.
    pub async fn append(&self, record: &DecisionRecord) -> Result<(), HistoryError> {
        self.append_batch(std::slice::from_ref(record)).await
    }

    /// Appends a group of records under one process-local lock and one sync.
    ///
    /// This prevents another task in the process from interleaving bytes inside
    /// the group. It is deliberately not described as a transaction: an OS
    /// write failure can still leave a partial tail. Later appends
    /// automatically truncate only the crash-left suffix after the last
    /// complete, valid record. A tail without a complete-record anchor, or any
    /// complete malformed record in that retained prefix, still fails closed so
    /// the writer never launders deeper corruption into the middle of the
    /// journal.
    ///
    /// If the batch would push the active file past [`MAX_HISTORY_FILE_BYTES`],
    /// the active file is sealed as the next read-only chain segment before the
    /// batch is written to a fresh active file. A crash between sealing and
    /// recreating the active file leaves a contiguous sealed chain without an
    /// active file; that state is unambiguous, and the next append rebuilds the
    /// active file from it. Any other chain inconsistency (segment gaps, empty
    /// or oversized segments, chains past the segment or byte budget) fails
    /// closed without writing.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] on validation, chain-integrity, chain-budget,
    /// or I/O failure.
    pub async fn append_batch(&self, records: &[DecisionRecord]) -> Result<(), HistoryError> {
        if records.is_empty() {
            return Ok(());
        }

        let started_at = Instant::now();
        let mut stats = AppendBatchStats::default();
        let result = self.append_batch_inner(records, &mut stats).await;
        let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);

        match &result {
            Ok(()) if stats.rotated => warn!(
                batch_records = records.len(),
                batch_bytes = stats.batch_bytes,
                elapsed_ms,
                category = "rotation",
                "history append batch completed"
            ),
            Ok(()) => debug!(
                batch_records = records.len(),
                batch_bytes = stats.batch_bytes,
                elapsed_ms,
                category = "success",
                "history append batch completed"
            ),
            Err(HistoryError::PartialTail { .. }) => warn!(
                batch_records = records.len(),
                batch_bytes = stats.batch_bytes,
                elapsed_ms,
                category = "partial_tail",
                "history append batch failed"
            ),
            Err(_) => warn!(
                batch_records = records.len(),
                batch_bytes = stats.batch_bytes,
                elapsed_ms,
                category = "failure",
                "history append batch failed"
            ),
        }

        result
    }

    async fn append_batch_inner(
        &self,
        records: &[DecisionRecord],
        stats: &mut AppendBatchStats,
    ) -> Result<(), HistoryError> {
        debug_assert!(!records.is_empty());

        let mut batch = Vec::new();
        for (index, record) in records.iter().enumerate() {
            let mut writer = ByteBudgetWriter::new(MAX_HISTORY_RECORD_BYTES);
            let serialization = serde_json::to_writer(&mut writer, record);
            if writer.exceeded {
                return Err(HistoryError::RecordTooLarge {
                    index,
                    bytes: writer.bytes.len(),
                    limit: MAX_HISTORY_RECORD_BYTES,
                });
            }
            serialization.map_err(HistoryError::Serialize)?;
            let encoded_bytes =
                writer
                    .bytes
                    .len()
                    .checked_add(1)
                    .ok_or(HistoryError::RecordTooLarge {
                        index,
                        bytes: usize::MAX,
                        limit: MAX_HISTORY_RECORD_BYTES,
                    })?;
            if encoded_bytes > MAX_HISTORY_RECORD_BYTES {
                return Err(HistoryError::RecordTooLarge {
                    index,
                    bytes: encoded_bytes,
                    limit: MAX_HISTORY_RECORD_BYTES,
                });
            }
            let mut encoded = writer.into_inner();
            encoded
                .try_reserve(1)
                .map_err(|_| HistoryError::Allocation {
                    resource: "history record",
                    bytes: encoded_bytes,
                })?;
            encoded.push(b'\n');
            let next_size =
                batch
                    .len()
                    .checked_add(encoded.len())
                    .ok_or(HistoryError::BatchTooLarge {
                        bytes: usize::MAX,
                        limit: MAX_HISTORY_BATCH_BYTES,
                    })?;
            if next_size > MAX_HISTORY_BATCH_BYTES {
                return Err(HistoryError::BatchTooLarge {
                    bytes: next_size,
                    limit: MAX_HISTORY_BATCH_BYTES,
                });
            }
            batch
                .try_reserve(encoded.len())
                .map_err(|_| HistoryError::Allocation {
                    resource: "history batch",
                    bytes: next_size,
                })?;
            batch.extend_from_slice(&encoded);
        }

        stats.batch_bytes = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let _guard = self.path_lock.lock().await;
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(HistoryError::CreateDirectory)?;
        }
        let _lease = self.active_cross_process_lease()?;

        let AppendTarget {
            mut file,
            head_before,
            head_after,
        } = self
            .open_active_within_chain_budget(stats.batch_bytes, stats)
            .await?;
        file.write_all(&batch).await.map_err(HistoryError::Write)?;
        file.flush().await.map_err(HistoryError::Flush)?;
        file.sync_data().await.map_err(HistoryError::Sync)?;
        self.record_append_receipt(head_before, head_after, records);
        Ok(())
    }

    /// Opens the active file with room for `batch_bytes`, sealing it as the
    /// next read-only chain segment first when the batch would cross
    /// [`MAX_HISTORY_FILE_BYTES`].
    ///
    /// Must be called with the in-process path lock held and the cross-process
    /// writer lease claimed; sealing is only legal under that lease.
    async fn open_active_within_chain_budget(
        &self,
        batch_bytes: u64,
        stats: &mut AppendBatchStats,
    ) -> Result<AppendTarget, HistoryError> {
        let sealed = self.inspect_sealed_segments_for_append().await?;
        let sealed_bytes = sealed
            .iter()
            .fold(0u64, |total, segment| total.saturating_add(segment.bytes));
        let sealed_segment_bytes = sealed
            .iter()
            .map(|segment| segment.bytes)
            .collect::<Vec<_>>();

        let mut file = self.open_active().await?;
        let mut existing_bytes = file.metadata().await.map_err(HistoryError::Metadata)?.len();
        if existing_bytes > MAX_HISTORY_FILE_BYTES || batch_bytes > MAX_HISTORY_FILE_BYTES {
            // An oversized active file was not produced by this writer; sealing
            // it would launder the violation into the read-only chain.
            return Err(HistoryError::FileTooLarge {
                existing_bytes,
                batch_bytes,
                limit: MAX_HISTORY_FILE_BYTES,
            });
        }
        let retained_bytes = self
            .ensure_active_tail_is_complete(&mut file, existing_bytes)
            .await?;
        if retained_bytes < existing_bytes {
            drop(file);
            self.truncate_active_tail(retained_bytes).await?;
            file = self.open_active().await?;
            existing_bytes = retained_bytes;
        } else {
            existing_bytes = retained_bytes;
        }
        ensure_chain_budget(sealed_bytes, existing_bytes, batch_bytes)?;
        let head_before = HistoryChainHead {
            sealed_segment_bytes: sealed_segment_bytes.clone(),
            active_bytes: existing_bytes,
        };
        if existing_bytes.saturating_add(batch_bytes) <= MAX_HISTORY_FILE_BYTES {
            return Ok(AppendTarget {
                file,
                head_before: head_before.clone(),
                head_after: HistoryChainHead {
                    sealed_segment_bytes,
                    active_bytes: existing_bytes.saturating_add(batch_bytes),
                },
            });
        }

        // Rotation is reached only with a non-empty active file, so sealed
        // segments are never empty.
        let segments = u64::try_from(sealed.len()).unwrap_or(u64::MAX);
        if segments >= MAX_HISTORY_SEALED_SEGMENTS {
            return Err(HistoryError::TooManySegments {
                segments,
                limit: MAX_HISTORY_SEALED_SEGMENTS,
            });
        }
        let sealed_path = sealed_segment_path(&self.path, segments.saturating_add(1));
        drop(file);
        tokio::fs::rename(&self.path, &sealed_path)
            .await
            .map_err(|source| HistoryError::Seal {
                path: sealed_path,
                source,
            })?;
        stats.rotated = true;
        let file = self.open_active().await?;
        let existing_bytes = file.metadata().await.map_err(HistoryError::Metadata)?.len();
        if existing_bytes.saturating_add(batch_bytes) > MAX_HISTORY_FILE_BYTES {
            return Err(HistoryError::FileTooLarge {
                existing_bytes,
                batch_bytes,
                limit: MAX_HISTORY_FILE_BYTES,
            });
        }
        let mut head_after_segments = sealed_segment_bytes;
        head_after_segments.push(head_before.active_bytes);
        Ok(AppendTarget {
            file,
            head_before,
            head_after: HistoryChainHead {
                sealed_segment_bytes: head_after_segments,
                active_bytes: existing_bytes.saturating_add(batch_bytes),
            },
        })
    }

    async fn inspect_sealed_segments_for_append(&self) -> Result<Vec<SealedSegment>, HistoryError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            run_append_inspection_test_hook();
            inspect_sealed_segments(&path).map_err(SealedChainError::into_history_error)
        })
        .await
        .map_err(|_| {
            HistoryError::Metadata(io::Error::other(
                "history sealed-segment inspection task failed",
            ))
        })?
    }

    fn record_append_receipt(
        &self,
        head_before: HistoryChainHead,
        head_after: HistoryChainHead,
        records: &[DecisionRecord],
    ) {
        let registry =
            APPEND_RECEIPTS.get_or_init(|| StdMutex::new(AppendReceiptRegistry::default()));
        let mut registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queue = registry.receipts.entry(self.lock_key.clone()).or_default();
        queue.push_back(AppendReceiptEntry {
            head_before,
            head_after,
            records: records.to_vec(),
        });
        while queue.len() > MAX_APPEND_RECEIPTS_PER_PATH
            || queue.iter().map(|entry| entry.records.len()).sum::<usize>()
                > MAX_APPEND_RECEIPT_RECORDS
        {
            queue.pop_front();
        }
    }

    /// Ensures the active file ends at the last complete record boundary.
    ///
    /// A crash or failed write can leave a partial record at the tail. Readers
    /// tolerate that state as a recoverable partial-tail boundary, but only
    /// while it stays at the end of the chain: appending would glue the
    /// fragment and the next record into one malformed line in the middle of
    /// the journal, and sealing would freeze it into a segment every future
    /// chain read rejects. When there is an anchored complete prefix, the
    /// caller truncates only the trailing fragment and syncs that repair
    /// before continuing. Without such an anchor, or when the anchored prefix
    /// already contains a complete malformed record, the writer still fails
    /// closed.
    async fn ensure_active_tail_is_complete(
        &self,
        file: &mut tokio::fs::File,
        existing_bytes: u64,
    ) -> Result<u64, HistoryError> {
        if existing_bytes == 0 {
            return Ok(0);
        }
        let mut tail = [0u8; 1];
        file.seek(io::SeekFrom::End(-1))
            .await
            .map_err(HistoryError::TailInspect)?;
        file.read_exact(&mut tail)
            .await
            .map_err(HistoryError::TailInspect)?;
        if tail == [b'\n'] {
            return Ok(existing_bytes);
        }

        let bytes = self.read_active_bytes(file, existing_bytes).await?;
        let Some(complete_prefix_len) = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map(|index| index + 1)
        else {
            return Err(HistoryError::PartialTail {
                path: self.path.clone(),
            });
        };
        self.validate_complete_prefix(&bytes[..complete_prefix_len])?;
        let quarantined = self
            .quarantine_partial_tail(&bytes[complete_prefix_len..])
            .await?;
        warn!(
            retained_bytes = complete_prefix_len,
            quarantine_path = %quarantined.display(),
            "history partial tail quarantined before append"
        );

        Ok(u64::try_from(complete_prefix_len).unwrap_or(u64::MAX))
    }

    async fn open_active(&self) -> Result<tokio::fs::File, HistoryError> {
        tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(HistoryError::Open)
    }

    fn active_cross_process_lease(&self) -> Result<Arc<CrossProcessHistoryLease>, HistoryError> {
        if let Some(failure) = self.startup_failure.clone() {
            return Err(failure.into_history_error());
        }

        self.cross_process_lease_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned()
            .ok_or_else(|| HistoryError::LockClaim {
                path: history_lock_path(&self.path),
                source: io::Error::other(
                    "history writer lease was not available after successful startup",
                ),
            })
    }

    async fn read_active_bytes(
        &self,
        file: &mut tokio::fs::File,
        existing_bytes: u64,
    ) -> Result<Vec<u8>, HistoryError> {
        let expected_bytes = usize::try_from(existing_bytes).unwrap_or(usize::MAX);
        let mut bytes = Vec::new();
        bytes
            .try_reserve(expected_bytes)
            .map_err(|_| HistoryError::Allocation {
                resource: "history recovery buffer",
                bytes: expected_bytes,
            })?;
        file.seek(io::SeekFrom::Start(0))
            .await
            .map_err(HistoryError::TailInspect)?;
        file.read_to_end(&mut bytes)
            .await
            .map_err(HistoryError::TailInspect)?;
        if bytes.len() != expected_bytes {
            return Err(HistoryError::TailInspect(io::Error::other(format!(
                "history file changed while repairing a partial tail: expected {expected_bytes} bytes, read {}",
                bytes.len()
            ))));
        }
        Ok(bytes)
    }

    fn validate_complete_prefix(&self, bytes: &[u8]) -> Result<(), HistoryError> {
        for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            serde_json::from_slice::<DecisionRecord>(line).map_err(|source| {
                HistoryError::MalformedActiveRecord {
                    path: self.path.clone(),
                    line: index + 1,
                    source,
                }
            })?;
        }
        Ok(())
    }

    async fn truncate_active_tail(&self, retained_bytes: u64) -> Result<(), HistoryError> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(HistoryError::TailRepair)?;
            file.set_len(retained_bytes)
                .map_err(HistoryError::TailRepair)?;
            file.sync_data().map_err(HistoryError::Sync)?;
            warn!(
                retained_bytes,
                path = %path.display(),
                "history partial tail truncated before append"
            );
            Ok(())
        })
        .await
        .map_err(|_| {
            HistoryError::TailRepair(io::Error::other("history tail repair task failed"))
        })?
    }

    async fn quarantine_partial_tail(&self, suffix: &[u8]) -> Result<PathBuf, HistoryError> {
        let path = self.path.clone();
        let quarantine_fallback = quarantine_candidate_path(&path, &uuid::Uuid::nil());
        let suffix = suffix.to_vec();
        tokio::task::spawn_blocking(move || quarantine_partial_tail(&path, &suffix))
            .await
            .map_err(|_| HistoryError::TailQuarantine {
                path: quarantine_fallback,
                source: io::Error::other("history partial-tail quarantine task failed"),
            })?
    }
}

fn stable_history_path(path: &Path) -> PathBuf {
    canonicalize_existing_prefix(&absolute_key(path))
}

fn history_file_name(path: &Path) -> OsString {
    path.file_name()
        .map_or_else(|| OsString::from("history.jsonl"), OsStr::to_os_string)
}

fn duplicate_sealed_sequence(path: &Path) -> SealedChainError {
    SealedChainError::Inspect {
        path: path.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::InvalidData,
            "multiple sealed segment names resolve to the same sequence",
        ),
    }
}

fn noncanonical_sealed_segment(
    active_path: &Path,
    candidate_path: &Path,
    sequence: u64,
) -> SealedChainError {
    SealedChainError::Gap {
        missing: sealed_segment_path(active_path, sequence),
        found: candidate_path.to_path_buf(),
    }
}

fn candidate_path_for_sequence(active_path: &Path, sequence: u64) -> PathBuf {
    sealed_segment_path(active_path, sequence)
}

#[cfg(unix)]
fn sealed_segment_sequence(
    active_path: &Path,
    candidate: &OsStr,
    candidate_path: &Path,
) -> Result<Option<u64>, SealedChainError> {
    use std::os::unix::ffi::OsStrExt;

    let active_name = history_file_name(active_path);
    let active_bytes = active_name.as_os_str().as_bytes();
    let candidate_bytes = candidate.as_bytes();
    if !candidate_bytes.starts_with(active_bytes) {
        return Ok(None);
    }
    let Some(suffix) = candidate_bytes.get(active_bytes.len()..) else {
        return Ok(None);
    };
    let Some(suffix) = suffix.strip_prefix(b".") else {
        return Ok(None);
    };
    if suffix.is_empty() || !suffix.iter().all(|byte| byte.is_ascii_digit()) {
        return Ok(None);
    }
    let Ok(sequence_text) = std::str::from_utf8(suffix) else {
        return Ok(None);
    };
    let Ok(sequence) = sequence_text.parse::<u64>() else {
        return Ok(None);
    };
    if sequence.to_string().as_bytes() != suffix {
        return Err(noncanonical_sealed_segment(
            active_path,
            candidate_path,
            sequence,
        ));
    }
    Ok(Some(sequence))
}

#[cfg(not(unix))]
fn sealed_segment_sequence(
    active_path: &Path,
    candidate: &OsStr,
    candidate_path: &Path,
) -> Result<Option<u64>, SealedChainError> {
    let active_name = history_file_name(active_path);
    let Some(active_name) = active_name.to_str() else {
        return Err(SealedChainError::Inspect {
            path: active_path.to_path_buf(),
            source: io::Error::new(
                io::ErrorKind::InvalidData,
                "active history file name is not valid UTF-8 on this platform",
            ),
        });
    };
    let Some(candidate) = candidate.to_str() else {
        return Ok(None);
    };
    let Some(suffix) = candidate
        .strip_prefix(active_name)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return Ok(None);
    };
    if suffix.is_empty() || !suffix.as_bytes().iter().all(u8::is_ascii_digit) {
        return Ok(None);
    }
    let Ok(sequence) = suffix.parse::<u64>() else {
        return Ok(None);
    };
    if sequence.to_string() != suffix {
        return Err(noncanonical_sealed_segment(
            active_path,
            candidate_path,
            sequence,
        ));
    }
    Ok(Some(sequence))
}

#[cfg(test)]
type AppendInspectionHook = Arc<dyn Fn() + Send + Sync + 'static>;
#[cfg(test)]
static APPEND_INSPECTION_HOOK: OnceLock<StdMutex<Option<AppendInspectionHook>>> = OnceLock::new();

#[cfg(test)]
fn run_append_inspection_test_hook() {
    let hook = APPEND_INSPECTION_HOOK
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(not(test))]
fn run_append_inspection_test_hook() {}

pub(crate) fn stable_history_path_for_read(path: &Path) -> PathBuf {
    stable_history_path(path)
}

fn history_lock_path(history_path: &Path) -> PathBuf {
    let mut file_name = history_file_name(history_path);
    file_name.push(format!(".{HISTORY_CROSS_PROCESS_LOCK_SUFFIX}"));
    history_path.with_file_name(file_name)
}

fn prime_cross_process_lease(
    history_path: &Path,
    lease_state: &CrossProcessLeaseState,
) -> Result<(), CrossProcessLeaseFailure> {
    let mut lease_state = lease_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = lease_state.as_ref() {
        let _ = existing;
        return Ok(());
    }
    if let Some(parent) = history_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|source| CrossProcessLeaseFailure::from_create_directory(&source))?;
    }

    let lease = Arc::new(acquire_cross_process_lease(history_path)?);
    *lease_state = Some(lease);
    Ok(())
}

fn acquire_cross_process_lease(
    history_path: &Path,
) -> Result<CrossProcessHistoryLease, CrossProcessLeaseFailure> {
    let lock_path = history_lock_path(history_path);
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| CrossProcessLeaseFailure::from_open(lock_path.clone(), &source))?;
    match lock_file.try_lock() {
        Ok(()) => Ok(CrossProcessHistoryLease {
            _lock_path: lock_path,
            lock_file,
        }),
        Err(std::fs::TryLockError::WouldBlock) => {
            Err(CrossProcessLeaseFailure::Busy { path: lock_path })
        }
        Err(std::fs::TryLockError::Error(source)) => {
            Err(CrossProcessLeaseFailure::from_claim(lock_path, &source))
        }
    }
}

fn absolute_key(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
    }
}

/// Maps a journal path onto the key every lock guarding it must agree on.
///
/// Two spellings of one path — case differences on Windows, or a relative
/// versus absolute form — must resolve to the same key, or separate locks
/// would each believe they hold exclusive access to the same journal.
pub(crate) fn normalized_lock_key(path: &Path) -> PathBuf {
    normalize_key_case(path)
}

fn canonicalize_existing_prefix(path: &Path) -> PathBuf {
    let mut candidate = Some(path);
    while let Some(prefix) = candidate {
        if let Ok(mut canonical) = std::fs::canonicalize(prefix) {
            if let Ok(suffix) = path.strip_prefix(prefix) {
                canonical.push(suffix);
            }
            return lexical_normalize(&canonical);
        }
        candidate = prefix.parent();
    }
    lexical_normalize(path)
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let can_pop = normalized
                    .file_name()
                    .is_some_and(|name| name != Component::ParentDir.as_os_str());
                if can_pop {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(windows)]
fn normalize_key_case(path: &Path) -> PathBuf {
    PathBuf::from(path.as_os_str().to_string_lossy().to_uppercase())
}

#[cfg(not(windows))]
fn normalize_key_case(path: &Path) -> PathBuf {
    path.to_path_buf()
}

fn shared_path_lock(path: PathBuf) -> Arc<PathLock> {
    let registry = PATH_LOCKS.get_or_init(|| StdMutex::new(PathLockRegistry::default()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cleanup_for_growth = registry.locks.len() >= registry.next_cleanup_size;
    let dead_cleanup_threshold = registry
        .locks
        .len()
        .saturating_div(4)
        .max(DEAD_PATH_LOCK_CLEANUP_THRESHOLD);
    let cleanup_for_dead_handles =
        DEAD_PATH_LOCK_HINT.load(Ordering::Relaxed) >= dead_cleanup_threshold;
    if cleanup_for_growth || cleanup_for_dead_handles {
        DEAD_PATH_LOCK_HINT.swap(0, Ordering::Relaxed);
        registry
            .locks
            .retain(|_, path_lock| path_lock.strong_count() > 0);
        registry.next_cleanup_size = registry
            .locks
            .len()
            .saturating_mul(2)
            .max(MIN_PATH_LOCK_CLEANUP_SIZE);
    }
    if let Some(existing) = registry.locks.get(&path).and_then(Weak::upgrade) {
        return existing;
    }
    let lock = Arc::new(AsyncMutex::new(()));
    registry.locks.insert(path, Arc::downgrade(&lock));
    lock
}

fn shared_cross_process_lease_state(path: PathBuf) -> Arc<CrossProcessLeaseState> {
    let registry = CROSS_PROCESS_LEASE_STATES
        .get_or_init(|| StdMutex::new(CrossProcessLeaseRegistry::default()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let cleanup_for_growth = registry.leases.len() >= registry.next_cleanup_size;
    let dead_cleanup_threshold = registry
        .leases
        .len()
        .saturating_div(4)
        .max(DEAD_PATH_LOCK_CLEANUP_THRESHOLD);
    let cleanup_for_dead_handles =
        DEAD_PATH_LOCK_HINT.load(Ordering::Relaxed) >= dead_cleanup_threshold;
    if cleanup_for_growth || cleanup_for_dead_handles {
        DEAD_PATH_LOCK_HINT.swap(0, Ordering::Relaxed);
        registry
            .leases
            .retain(|_, lease_state| lease_state.strong_count() > 0);
        registry.next_cleanup_size = registry
            .leases
            .len()
            .saturating_mul(2)
            .max(MIN_PATH_LOCK_CLEANUP_SIZE);
    }
    if let Some(existing) = registry.leases.get(&path).and_then(Weak::upgrade) {
        return existing;
    }
    let lease_state = Arc::new(StdMutex::new(None));
    registry.leases.insert(path, Arc::downgrade(&lease_state));
    lease_state
}

fn inspect_chain_head(path: &Path) -> Result<HistoryChainHead, HistoryError> {
    let sealed = inspect_sealed_segments(path).map_err(SealedChainError::into_history_error)?;
    let active_bytes = match std::fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(HistoryError::SealedSegmentNotAFile {
                    path: path.to_path_buf(),
                });
            }
            if metadata.len() > MAX_HISTORY_FILE_BYTES {
                return Err(HistoryError::FileTooLarge {
                    existing_bytes: metadata.len(),
                    batch_bytes: 0,
                    limit: MAX_HISTORY_FILE_BYTES,
                });
            }
            metadata.len()
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => return Err(HistoryError::Metadata(source)),
    };
    Ok(HistoryChainHead {
        sealed_segment_bytes: sealed.into_iter().map(|segment| segment.bytes).collect(),
        active_bytes,
    })
}

fn quarantine_candidate_path(path: &Path, quarantine_id: &uuid::Uuid) -> PathBuf {
    let mut file_name = history_file_name(path);
    file_name.push(format!(".partial-tail.{quarantine_id}.quarantine"));
    path.with_file_name(file_name)
}

fn quarantine_partial_tail(path: &Path, suffix: &[u8]) -> Result<PathBuf, HistoryError> {
    let quarantine_id = uuid::Uuid::new_v4();
    let quarantine_path = quarantine_candidate_path(path, &quarantine_id);
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&quarantine_path)
        .map_err(|source| HistoryError::TailQuarantine {
            path: quarantine_path.clone(),
            source,
        })?;
    file.write_all(suffix)
        .map_err(|source| HistoryError::TailQuarantine {
            path: quarantine_path.clone(),
            source,
        })?;
    file.sync_data()
        .map_err(|source| HistoryError::TailQuarantine {
            path: quarantine_path.clone(),
            source,
        })?;
    Ok(quarantine_path)
}

#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("history record {index} has {bytes} bytes; maximum is {limit}")]
    RecordTooLarge {
        index: usize,
        bytes: usize,
        limit: usize,
    },
    #[error("history batch has {bytes} bytes; maximum is {limit}")]
    BatchTooLarge { bytes: usize, limit: usize },
    #[error(
        "history file has {existing_bytes} bytes and cannot append {batch_bytes} bytes; maximum is {limit}"
    )]
    FileTooLarge {
        existing_bytes: u64,
        batch_bytes: u64,
        limit: u64,
    },
    #[error(
        "history journal chain would hold {chain_bytes} bytes including this {batch_bytes}-byte batch; maximum is {limit}"
    )]
    ChainTooLarge {
        chain_bytes: u64,
        batch_bytes: u64,
        limit: u64,
    },
    #[error("history journal chain already holds {segments} sealed segments; maximum is {limit}")]
    TooManySegments { segments: u64, limit: u64 },
    #[error("history journal chain is missing sealed segment {missing} while {found} exists")]
    SealedSegmentGap { missing: PathBuf, found: PathBuf },
    #[error(
        "sealed history segment {path} has {bytes} bytes; sealed segments must hold 1..={limit} bytes"
    )]
    SealedSegmentBytes {
        path: PathBuf,
        bytes: u64,
        limit: u64,
    },
    #[error("sealed history segment {path} is not a regular file")]
    SealedSegmentNotAFile { path: PathBuf },
    #[error("failed to inspect sealed history segment {path}: {source}")]
    SegmentInspect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to seal history segment {path}: {source}")]
    Seal {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to reserve {bytes} bytes for {resource}")]
    Allocation {
        resource: &'static str,
        bytes: usize,
    },
    #[error("failed to read the history file tail: {0}")]
    TailInspect(std::io::Error),
    #[error("failed to truncate the history file tail: {0}")]
    TailRepair(std::io::Error),
    #[error("failed to quarantine the partial history tail in {path}: {source}")]
    TailQuarantine {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "history file {path} ends in a partial record; quarantine the crash-left tail before appending"
    )]
    PartialTail { path: PathBuf },
    #[error("history file {path} contains a malformed complete record at line {line}: {source}")]
    MalformedActiveRecord {
        path: PathBuf,
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to create history directory: {0}")]
    CreateDirectory(std::io::Error),
    #[error("failed to serialize decision record: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to open history file: {0}")]
    Open(std::io::Error),
    #[error("failed to inspect history file metadata: {0}")]
    Metadata(std::io::Error),
    #[error("failed to append history record: {0}")]
    Write(std::io::Error),
    #[error("failed to flush history file: {0}")]
    Flush(std::io::Error),
    #[error("failed to sync history data: {0}")]
    Sync(std::io::Error),
    #[error("failed to open history writer lock {path}: {source}")]
    LockOpen {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to claim history writer lock {path}: {source}")]
    LockClaim {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("history writer lock is already held for {path}")]
    CrossProcessLockBusy { path: PathBuf },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };
    use uuid::Uuid;

    const STABLE_CWD_TEST_NAME: &str =
        "history::tests::relative_paths_remain_stable_after_cwd_changes";

    #[test]
    fn lexical_aliases_share_one_process_lock() {
        let root = std::env::temp_dir().join(format!("history-alias-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        std::fs::write(&path, b"").unwrap();
        let direct = JsonlHistory::new(&path);
        let dot_alias = JsonlHistory::new(root.join(".").join("decisions.jsonl"));
        let alias = JsonlHistory::new(root.join("unused").join("..").join("decisions.jsonl"));

        assert!(Arc::ptr_eq(&direct.path_lock, &dot_alias.path_lock));
        assert!(Arc::ptr_eq(&direct.path_lock, &alias.path_lock));

        drop((direct, dot_alias, alias));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn symlink_parent_segments_use_filesystem_resolution_for_the_lock_key() {
        let root = std::env::temp_dir().join(format!("history-symlink-{}", Uuid::new_v4()));
        let target = root.join("target");
        let nested = target.join("nested");
        let link = root.join("link");
        std::fs::create_dir_all(&nested).unwrap();
        if let Err(error) = create_directory_symlink(&nested, &link) {
            std::fs::remove_dir_all(&root).unwrap();
            #[cfg(windows)]
            if error.raw_os_error() == Some(1314) {
                return;
            }
            panic!("failed to create directory symlink: {error}");
        }

        let alias = link.join("..").join("decisions.jsonl");
        let resolved_parent = std::fs::canonicalize(alias.parent().unwrap()).unwrap();
        let direct = resolved_parent.join("decisions.jsonl");
        let direct_history = JsonlHistory::new(direct);
        let alias_history = JsonlHistory::new(alias);

        assert!(Arc::ptr_eq(
            &direct_history.path_lock,
            &alias_history.path_lock
        ));

        drop((direct_history, alias_history));
        remove_directory_symlink(&link).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn path_lock_registry_prunes_dead_entries_on_registration() {
        let tag = Uuid::new_v4().to_string();
        let root = std::env::temp_dir().join(format!("history-registry-{tag}"));
        let histories = (0..128)
            .map(|index| JsonlHistory::new(root.join(format!("{index}.jsonl"))))
            .collect::<Vec<_>>();

        assert_eq!(matching_registry_entries(&tag), 128);
        drop(histories);

        // The registry and its cleanup thresholds are process-global, so
        // concurrently running tests shift when a registration sweeps. The
        // contract is that dead entries are eventually pruned, not that the
        // first registration after the drop is the one that prunes them.
        let mut pruned = false;
        for index in 0..1024 {
            let survivor = JsonlHistory::new(root.join(format!("survivor-{index}.jsonl")));
            let remaining = matching_registry_entries(&tag);
            drop(survivor);
            if remaining <= 2 {
                pruned = true;
                break;
            }
        }
        assert!(pruned, "dead registry entries were never pruned");
    }

    #[test]
    fn inspect_sealed_segments_accepts_empty_chain_and_ignores_unrelated_files() {
        let root = temp_root("history-inspect-empty");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        std::fs::write(root.join("decisions.jsonl.tmp"), b"tmp").unwrap();
        std::fs::write(root.join("decisions.jsonl.abc"), b"abc").unwrap();
        std::fs::write(root.join("other.jsonl.1"), b"other").unwrap();

        let segments = inspect_sealed_segments(&path).unwrap();

        assert!(segments.is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn inspect_sealed_segments_treats_a_missing_parent_directory_as_an_empty_chain() {
        let root = temp_root("history-inspect-missing-parent");
        let path = root.join("missing").join("decisions.jsonl");

        let segments = inspect_sealed_segments(&path).unwrap();

        assert!(segments.is_empty());
    }

    #[test]
    fn inspect_sealed_segments_rejects_gaps() {
        let root = temp_root("history-inspect-gap");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        std::fs::write(sealed_segment_path(&path, 2), b"segment\n").unwrap();

        let error = inspect_sealed_segments(&path).unwrap_err();

        assert!(matches!(
            error,
            SealedChainError::Gap { missing, found }
                if missing == sealed_segment_path(&path, 1)
                    && found == sealed_segment_path(&path, 2)
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn inspect_sealed_segments_supports_non_utf8_history_file_names() {
        use std::os::unix::ffi::OsStringExt;

        let root = temp_root("history-inspect-nonutf8");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join(OsString::from_vec(vec![
            b'd', b'e', b'c', b'i', b's', b'i', b'o', b'n', b's', b'.', b'\xFF',
        ]));
        let sealed = sealed_segment_path(&path, 1);
        std::fs::write(&sealed, b"segment\n").unwrap();

        let segments = inspect_sealed_segments(&path).unwrap();

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].path, sealed);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn inspect_sealed_segments_rejects_non_canonical_numeric_suffixes() {
        let root = temp_root("history-inspect-noncanonical");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        std::fs::write(path.with_file_name("decisions.jsonl.01"), b"segment\n").unwrap();

        let error = inspect_sealed_segments(&path).unwrap_err();

        assert!(matches!(
            error,
            SealedChainError::Gap { missing, found }
                if missing == sealed_segment_path(&path, 1)
                    && found == path.with_file_name("decisions.jsonl.01")
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn inspect_sealed_segments_rejects_duplicate_numeric_sequences() {
        let root = temp_root("history-inspect-duplicate");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        std::fs::write(sealed_segment_path(&path, 1), b"segment\n").unwrap();
        std::fs::write(path.with_file_name("decisions.jsonl.01"), b"segment\n").unwrap();

        let error = inspect_sealed_segments(&path).unwrap_err();

        assert!(matches!(
            error,
            SealedChainError::Gap { missing, found }
                if missing == sealed_segment_path(&path, 1)
                    && found == path.with_file_name("decisions.jsonl.01")
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn inspect_sealed_segments_rejects_out_of_range_segments() {
        let root = temp_root("history-inspect-overflow");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        std::fs::write(
            sealed_segment_path(&path, MAX_HISTORY_SEALED_SEGMENTS + 1),
            b"segment\n",
        )
        .unwrap();

        let error = inspect_sealed_segments(&path).unwrap_err();

        assert!(matches!(
            error,
            SealedChainError::TooManySegments { segments }
                if segments == MAX_HISTORY_SEALED_SEGMENTS + 1
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn byte_budget_writer_stops_after_one_over_limit_byte() {
        let mut writer = ByteBudgetWriter::new(4);

        let error = writer.write_all(&[0; 128]).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(writer.exceeded);
        assert_eq!(writer.bytes.len(), 5);
    }

    #[test]
    fn relative_paths_remain_stable_after_cwd_changes() {
        if std::env::var_os("JSONL_HISTORY_CWD_CHILD").is_some() {
            run_relative_paths_remain_stable_after_cwd_changes();
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(STABLE_CWD_TEST_NAME)
            .arg("--nocapture")
            .env("JSONL_HISTORY_CWD_CHILD", "1")
            .env("RUST_TEST_THREADS", "1")
            .status()
            .unwrap();

        assert!(status.success(), "child test failed: {status}");
    }

    #[test]
    fn append_inspection_runs_off_the_current_thread_runtime() {
        let root = temp_root("history-inspect-runtime");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        let history = JsonlHistory::new(&path);
        let record = DecisionRecord {
            timestamp: Utc::timestamp_opt(&Utc, 1_700_000_000, 0).unwrap(),
            strategy: "spawn-blocking".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Null,
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (timer_tx, timer_rx) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let hook_release = Arc::clone(&release);
        let _hook = TestInspectHook::install(move || {
            started_tx.send(()).unwrap();
            while !hook_release.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        });

        let join = std::thread::spawn(move || {
            runtime().block_on(async move {
                let append = tokio::spawn({
                    let history = history.clone();
                    let record = record.clone();
                    async move { history.append(&record).await }
                });
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    timer_tx.send(()).unwrap();
                });
                append.await.unwrap()
            })
        });

        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        timer_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        release.store(true, Ordering::Release);
        join.join().unwrap().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn history_allows_exact_file_limit_then_seals_a_segment_and_continues() {
        let path = std::env::temp_dir().join(format!("history-file-limit-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let record = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "limit-test".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Null,
        };
        let record_bytes = serde_json::to_vec(&record).unwrap().len() as u64 + 1;
        newline_terminated_fill(&path, MAX_HISTORY_FILE_BYTES - record_bytes);

        history.append(&record).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            MAX_HISTORY_FILE_BYTES
        );
        let sealed = sealed_segment_path(history.path(), 1);
        assert!(!sealed.exists());

        history.append(&record).await.unwrap();
        assert_eq!(
            std::fs::metadata(&sealed).unwrap().len(),
            MAX_HISTORY_FILE_BYTES
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), record_bytes);

        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(sealed).unwrap();
    }

    #[tokio::test]
    async fn append_still_fails_closed_when_a_partial_tail_has_no_complete_record_anchor() {
        let path = std::env::temp_dir().join(format!("history-partial-tail-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        // A crash mid-write leaves the last record without its terminator.
        let fragment: &[u8] = b"{\"decision\":\"trunc";
        std::fs::write(&path, fragment).unwrap();
        let record = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "partial-tail".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Null,
        };

        let error = history.append(&record).await.unwrap_err();

        assert!(matches!(error, HistoryError::PartialTail { .. }));
        // The fragment stays at the end of the chain, where readers can still
        // detect and quarantine it.
        assert_eq!(std::fs::read(&path).unwrap(), fragment);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn append_truncates_only_the_tail_after_the_last_complete_record() {
        let path = std::env::temp_dir().join(format!("history-tail-recover-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let first = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "partial-recover".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "first".to_owned(),
            details: Value::Null,
        };
        let resumed = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_001, 0).unwrap(),
            strategy: "partial-recover".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "resumed".to_owned(),
            details: Value::Null,
        };
        history.append(&first).await.unwrap();
        let partial = b"{\"timestamp\":\"2023-11-14T22:13:20Z\",\"strategy\":\"partial-recover\",";
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(partial);
        std::fs::write(&path, &bytes).unwrap();

        history.append(&resumed).await.unwrap();

        let mut expected = serde_json::to_vec(&first).unwrap();
        expected.push(b'\n');
        expected.extend_from_slice(&serde_json::to_vec(&resumed).unwrap());
        expected.push(b'\n');
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn rotation_refuses_to_seal_a_partial_tail_without_a_complete_record_anchor() {
        let path = std::env::temp_dir().join(format!("history-partial-seal-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        // An active file at the limit whose tail is a partial record: the
        // crossing append must fail closed instead of sealing a segment that
        // every future chain read would reject.
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(MAX_HISTORY_FILE_BYTES).unwrap();
        drop(file);
        let record = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "partial-seal".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Null,
        };

        let error = history.append(&record).await.unwrap_err();

        assert!(matches!(error, HistoryError::PartialTail { .. }));
        assert!(!sealed_segment_path(history.path(), 1).exists());
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            MAX_HISTORY_FILE_BYTES
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn tail_recovery_refuses_to_truncate_past_a_complete_malformed_record() {
        let path = std::env::temp_dir().join(format!("history-tail-malformed-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let first = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "partial-malformed".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "first".to_owned(),
            details: Value::Null,
        };
        let mut bytes = serde_json::to_vec(&first).unwrap();
        bytes.push(b'\n');
        bytes.extend_from_slice(b"{bad json}\n");
        bytes.extend_from_slice(b"{\"decision\":\"partial\"");
        std::fs::write(&path, &bytes).unwrap();

        let error = history.append(&first).await.unwrap_err();

        assert!(matches!(
            error,
            HistoryError::MalformedActiveRecord { line: 2, .. }
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn chain_budget_rejects_totals_beyond_the_chain_limit() {
        assert!(ensure_chain_budget(MAX_HISTORY_CHAIN_BYTES - 10, 4, 6).is_ok());
        assert!(matches!(
            ensure_chain_budget(MAX_HISTORY_CHAIN_BYTES - 10, 5, 6),
            Err(HistoryError::ChainTooLarge {
                batch_bytes: 6,
                limit: MAX_HISTORY_CHAIN_BYTES,
                ..
            })
        ));
        assert!(matches!(
            ensure_chain_budget(u64::MAX, 1, 1),
            Err(HistoryError::ChainTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn history_rejects_an_already_oversized_file_without_writing() {
        let path = std::env::temp_dir().join(format!("history-over-limit-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(MAX_HISTORY_FILE_BYTES + 1).unwrap();
        drop(file);
        let record = DecisionRecord {
            timestamp: Utc::now(),
            strategy: "limit-test".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Null,
        };

        let error = history.append(&record).await.unwrap_err();

        assert!(matches!(error, HistoryError::FileTooLarge { .. }));
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            MAX_HISTORY_FILE_BYTES + 1
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn append_batch_accepts_a_batch_that_exactly_fills_the_byte_budget() {
        let path = std::env::temp_dir().join(format!("history-batch-exact-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let record = sized_history_record(MAX_HISTORY_RECORD_BYTES);
        let records = vec![record; MAX_HISTORY_BATCH_BYTES / MAX_HISTORY_RECORD_BYTES];

        history.append_batch(&records).await.unwrap();

        assert_eq!(
            u64::try_from(MAX_HISTORY_BATCH_BYTES).unwrap(),
            std::fs::metadata(&path).unwrap().len()
        );
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn append_batch_rejects_one_byte_over_budget_without_touching_the_file() {
        let path = std::env::temp_dir().join(format!("history-batch-over-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let sentinel = b"seed\n";
        std::fs::write(&path, sentinel).unwrap();

        let minimum = minimum_history_record_bytes();
        let overflow_record = sized_history_record(MAX_HISTORY_RECORD_BYTES + 1 - minimum);
        let mut records = vec![
            sized_history_record(MAX_HISTORY_RECORD_BYTES);
            (MAX_HISTORY_BATCH_BYTES / MAX_HISTORY_RECORD_BYTES) - 1
        ];
        records.push(overflow_record);
        records.push(sized_history_record(minimum));

        let error = history.append_batch(&records).await.unwrap_err();

        assert!(matches!(
            error,
            HistoryError::BatchTooLarge {
                bytes,
                limit: MAX_HISTORY_BATCH_BYTES,
            } if bytes == MAX_HISTORY_BATCH_BYTES + 1
        ));
        assert_eq!(std::fs::read(&path).unwrap(), sentinel);
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_path_keys_are_case_normalized() {
        let tag = Uuid::new_v4().to_string();
        let path = std::env::temp_dir().join(format!("history-\u{00C4}-case-{tag}.jsonl"));
        let upper = PathBuf::from(path.to_string_lossy().to_ascii_uppercase());
        let lower = PathBuf::from(path.to_string_lossy().to_lowercase());

        let upper_history = JsonlHistory::new(upper);
        let lower_history = JsonlHistory::new(lower);

        assert!(Arc::ptr_eq(
            &upper_history.path_lock,
            &lower_history.path_lock
        ));

        let final_sigma = JsonlHistory::new(
            std::env::temp_dir().join(format!("history-\u{03C2}-case-{tag}.jsonl")),
        );
        let standard_sigma = JsonlHistory::new(
            std::env::temp_dir().join(format!("history-\u{03C3}-case-{tag}.jsonl")),
        );
        assert!(Arc::ptr_eq(
            &final_sigma.path_lock,
            &standard_sigma.path_lock
        ));
    }

    fn matching_registry_entries(tag: &str) -> usize {
        let tag = tag.to_lowercase();
        PATH_LOCKS
            .get_or_init(|| StdMutex::new(PathLockRegistry::default()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .locks
            .keys()
            .filter(|path| path.to_string_lossy().to_lowercase().contains(&tag))
            .count()
    }

    /// Grows `path` to `len` bytes ending in a record terminator, so size
    /// fixtures pass the writer's partial-tail check.
    fn newline_terminated_fill(path: &Path, len: u64) {
        let file = std::fs::File::create(path).unwrap();
        file.set_len(len - 1).unwrap();
        drop(file);
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"\n").unwrap();
    }

    fn minimum_history_record_bytes() -> usize {
        history_record_bytes(0)
    }

    fn sized_history_record(target_bytes: usize) -> DecisionRecord {
        let minimum = minimum_history_record_bytes();
        assert!(
            (minimum..=MAX_HISTORY_RECORD_BYTES).contains(&target_bytes),
            "target bytes must be within [{minimum}, {MAX_HISTORY_RECORD_BYTES}], got {target_bytes}",
        );

        let padding = target_bytes - minimum;
        let record = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "batch-budget".to_owned(),
            symbol: "BTC-USDT".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Object(
                [("pad".to_owned(), Value::String("a".repeat(padding)))]
                    .into_iter()
                    .collect(),
            ),
        };
        assert_eq!(
            history_record_bytes(record_padding_len(&record)),
            target_bytes
        );
        record
    }

    fn history_record_bytes(padding: usize) -> usize {
        let record = DecisionRecord {
            timestamp: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            strategy: "batch-budget".to_owned(),
            symbol: "BTC-USDT".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Object(
                [("pad".to_owned(), Value::String("a".repeat(padding)))]
                    .into_iter()
                    .collect(),
            ),
        };
        serde_json::to_vec(&record).unwrap().len() + 1
    }

    fn record_padding_len(record: &DecisionRecord) -> usize {
        record
            .details
            .get("pad")
            .and_then(Value::as_str)
            .map_or(0, str::len)
    }

    #[cfg(unix)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(unix)]
    fn remove_directory_symlink(link: &Path) -> std::io::Result<()> {
        std::fs::remove_file(link)
    }

    #[cfg(windows)]
    fn remove_directory_symlink(link: &Path) -> std::io::Result<()> {
        std::fs::remove_dir(link)
    }

    fn run_relative_paths_remain_stable_after_cwd_changes() {
        let root = std::env::temp_dir().join(format!("history-stable-cwd-{}", Uuid::new_v4()));
        let original = root.join("original");
        let drift = root.join("drift");
        let relative = PathBuf::from("logs").join("decisions.jsonl");
        let expected = stable_history_path(&original.join(&relative));
        std::fs::create_dir_all(&original).unwrap();
        std::fs::create_dir_all(&drift).unwrap();

        let cwd_guard = CurrentDirGuard::capture().unwrap();
        std::env::set_current_dir(&original).unwrap();
        let relative_history = JsonlHistory::new(&relative);
        assert_eq!(relative_history.path(), expected.as_path());

        std::env::set_current_dir(&drift).unwrap();
        let direct_history = JsonlHistory::new(&expected);
        assert_eq!(direct_history.path(), expected.as_path());
        assert!(Arc::ptr_eq(
            &relative_history.path_lock,
            &direct_history.path_lock
        ));

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(relative_history.append(&DecisionRecord {
                timestamp: Utc::now(),
                strategy: "stable-path".to_owned(),
                symbol: "BTC".to_owned(),
                decision: "hold".to_owned(),
                details: Value::Null,
            }))
            .unwrap();

        drop(cwd_guard);

        let rows = std::fs::read_to_string(&expected).unwrap().lines().count();
        assert_eq!(rows, 1);
        assert!(!drift.join(&relative).exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    struct CurrentDirGuard {
        original: PathBuf,
    }

    impl CurrentDirGuard {
        fn capture() -> std::io::Result<Self> {
            Ok(Self {
                original: std::env::current_dir()?,
            })
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.original);
        }
    }

    struct TestInspectHook;

    impl TestInspectHook {
        fn install<F>(hook: F) -> TestInspectHookGuard
        where
            F: Fn() + Send + Sync + 'static,
        {
            let mut slot = APPEND_INSPECTION_HOOK
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(slot.is_none(), "append inspection hook already installed");
            *slot = Some(Arc::new(hook));
            TestInspectHookGuard
        }
    }

    struct TestInspectHookGuard;

    impl Drop for TestInspectHookGuard {
        fn drop(&mut self) {
            let mut slot = APPEND_INSPECTION_HOOK
                .get_or_init(|| StdMutex::new(None))
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *slot = None;
        }
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
}
