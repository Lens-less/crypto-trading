use std::{
    collections::HashMap,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, OnceLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{io::AsyncWriteExt, sync::Mutex as AsyncMutex};

pub const MAX_HISTORY_RECORD_BYTES: usize = 1_048_576;
pub const MAX_HISTORY_BATCH_BYTES: usize = 8_388_608;
pub const MAX_HISTORY_FILE_BYTES: u64 = 64 * 1_024 * 1_024;

type PathLock = AsyncMutex<()>;
type CrossProcessLeaseState = StdMutex<Option<Arc<CrossProcessHistoryLease>>>;
const MIN_PATH_LOCK_CLEANUP_SIZE: usize = 64;
const DEAD_PATH_LOCK_CLEANUP_THRESHOLD: usize = 64;
const HISTORY_CROSS_PROCESS_LOCK_SUFFIX: &str = "jsonl.lock";
static DEAD_PATH_LOCK_HINT: AtomicUsize = AtomicUsize::new(0);
static PATH_LOCKS: OnceLock<StdMutex<PathLockRegistry>> = OnceLock::new();
static CROSS_PROCESS_LEASE_STATES: OnceLock<StdMutex<CrossProcessLeaseRegistry>> = OnceLock::new();

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
/// process already owns that lease.
///
/// Locks are keyed by normalized paths rather than filesystem object identity.
/// Existing hard links and paths retargeted after construction can therefore
/// still refer to one file through separate locks. Windows case folding is a
/// conservative Unicode approximation, not an NTFS file-identity lookup.
#[derive(Clone, Debug)]
pub struct JsonlHistory {
    path: PathBuf,
    path_lock: Arc<PathLock>,
    cross_process_lease_state: Arc<CrossProcessLeaseState>,
    startup_failure: Option<CrossProcessLeaseFailure>,
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
        let cross_process_lease_state = shared_cross_process_lease_state(lock_key);
        let startup_failure = prime_cross_process_lease(&path, &cross_process_lease_state).err();
        Self {
            path,
            path_lock,
            cross_process_lease_state,
            startup_failure,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
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
    /// write failure can still leave a partial tail that recovery code must
    /// detect and quarantine.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] on validation or I/O failure.
    pub async fn append_batch(&self, records: &[DecisionRecord]) -> Result<(), HistoryError> {
        if records.is_empty() {
            return Ok(());
        }

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

        let _guard = self.path_lock.lock().await;
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(HistoryError::CreateDirectory)?;
        }
        let _lease = self.active_cross_process_lease()?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(HistoryError::Open)?;
        let existing_bytes = file.metadata().await.map_err(HistoryError::Metadata)?.len();
        let batch_bytes = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let next_file_bytes = existing_bytes.saturating_add(batch_bytes);
        if next_file_bytes > MAX_HISTORY_FILE_BYTES {
            return Err(HistoryError::FileTooLarge {
                existing_bytes,
                batch_bytes,
                limit: MAX_HISTORY_FILE_BYTES,
            });
        }
        file.write_all(&batch).await.map_err(HistoryError::Write)?;
        file.flush().await.map_err(HistoryError::Flush)?;
        file.sync_data().await.map_err(HistoryError::Sync)
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
}

fn stable_history_path(path: &Path) -> PathBuf {
    canonicalize_existing_prefix(&absolute_key(path))
}

pub(crate) fn stable_history_path_for_read(path: &Path) -> PathBuf {
    stable_history_path(path)
}

fn history_lock_path(history_path: &Path) -> PathBuf {
    let file_name = history_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("history.jsonl");
    history_path.with_file_name(format!("{file_name}.{HISTORY_CROSS_PROCESS_LOCK_SUFFIX}"))
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

fn normalized_lock_key(path: &Path) -> PathBuf {
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
    #[error("failed to reserve {bytes} bytes for {resource}")]
    Allocation {
        resource: &'static str,
        bytes: usize,
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

        let survivor = JsonlHistory::new(root.join("survivor.jsonl"));
        assert_eq!(matching_registry_entries(&tag), 1);
        drop(survivor);
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

    #[tokio::test]
    async fn history_allows_exact_file_limit_then_rejects_without_writing() {
        let path = std::env::temp_dir().join(format!("history-file-limit-{}", Uuid::new_v4()));
        let history = JsonlHistory::new(&path);
        let record = DecisionRecord {
            timestamp: Utc::now(),
            strategy: "limit-test".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: Value::Null,
        };
        let record_bytes = serde_json::to_vec(&record).unwrap().len() as u64 + 1;
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(MAX_HISTORY_FILE_BYTES - record_bytes).unwrap();
        drop(file);

        history.append(&record).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            MAX_HISTORY_FILE_BYTES
        );

        let error = history.append(&record).await.unwrap_err();
        assert!(matches!(
            error,
            HistoryError::FileTooLarge {
                existing_bytes: MAX_HISTORY_FILE_BYTES,
                limit: MAX_HISTORY_FILE_BYTES,
                ..
            }
        ));
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            MAX_HISTORY_FILE_BYTES
        );
        std::fs::remove_file(path).unwrap();
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
}
