//! Least-authority, read-only seam for operator-facing adapters.
//!
//! Trusted bootstrap code injects a bounded journal source once. HTTP, SSE,
//! CLI, and future desktop adapters consume this facade instead of receiving
//! filesystem paths or execution primitives.
//!
//! Journal payloads remain inside the trusted boundary. Event consumers receive
//! bounded notices and then refetch the operator snapshot; transports do not
//! choose their own payload-redaction policy.

use std::sync::{Arc, PoisonError, RwLock};

use chrono::{DateTime, Utc};
use crypto_trading_runtime::{
    AccountRiskProjectionError, CapabilityError, CursorError, JournalCursor, JournalPage,
    JournalPageBoundary, JournalReadError, JournalSnapshot, JournalSnapshotSource,
    LegacyJsonlJournalReader, MAX_CURSOR_ANCHOR_SCAN_BYTES, OperationEventEnvelope,
    PaperAccountProjectionError, ReadModelError, current_capability_manifest,
    project_control_plane_state,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

mod submit;

pub use submit::{
    SUBMIT_JOURNAL_PROJECTION, SUBMIT_JOURNAL_SOURCE, SUBMIT_SCHEMA_VERSION, SubmitCommand,
    SubmitDispatchFuture, SubmitDispatchOutcome, SubmitDispatcher, SubmitEnvelope,
    SubmitFailureKind, SubmitPermission, SubmitReceipt, SubmitRiskConfirmation, SubmitRole,
    SubmitService, SubmitServiceError, SubmitStatus, SubmitValidationError,
};

pub use crypto_trading_runtime::{
    ACCOUNT_RISK_SCHEMA_VERSION, AccountRiskOpenPositionView, AccountRiskReadModel,
    AccountRiskStateView, ArbitrageMonitorProjection, ArbitrageMonitorReadModel,
    ArbitrageMonitorView, CapabilityManifest, ExecutionBatchState, MonitorContinuityState,
    MonitorFreshnessState, MonitorLegView, MonitorProjectionState, OperatorReadModel,
    PAPER_ACCOUNT_SCHEMA_VERSION, PaperAccountReadModel, PaperAccountSnapshot, ProjectionStatus,
    READ_ONLY_TASK_READ_MODEL_SCHEMA_VERSION, ReadOnlyTaskExit, ReadOnlyTaskFailure,
    ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel, ReadOnlyTaskRecovery,
    ReadOnlyTaskSourceExit, ReadOnlyTaskSourceHealth, ReadOnlyTaskSourcePhase,
    ReadOnlyTaskSourceView, ReadOnlyTaskView, RecoveryDirective, ReleaseStage,
};

pub const CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION: u16 = 8;
pub const CONTROL_PLANE_EVENTS_SCHEMA_VERSION: u16 = 1;

/// Stable transport-independent classification for safe public error mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadFailureKind {
    InvalidCursor,
    ExpiredCursor,
    SourceUnavailable,
    ResourceLimit,
    InvalidJournal,
}

/// Thread-safe source handle constructed only by trusted bootstrap code.
pub type SharedJournalSnapshotSource = dyn JournalSnapshotSource + Send + Sync;

/// Read-only facade presented to transport adapters.
#[derive(Clone)]
pub struct ReadControlPlane {
    journal: Arc<SharedJournalSnapshotSource>,
    capabilities: CapabilityManifest,
    projection_cache: Arc<RwLock<ProjectionCache>>,
}

impl ReadControlPlane {
    /// Builds a read-only control plane from a trusted, bounded source.
    ///
    /// The current capability manifest is validated once and retained as the
    /// only authority snapshot exposed through this instance.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityError`] if the build's capability source would
    /// advertise an invalid or expanded authority.
    pub fn new(journal: Arc<SharedJournalSnapshotSource>) -> Result<Self, CapabilityError> {
        let capabilities = current_capability_manifest();
        capabilities.validate()?;
        Ok(Self {
            journal,
            capabilities,
            projection_cache: Arc::new(RwLock::new(ProjectionCache::default())),
        })
    }

    /// Returns the validated capability single source of truth.
    #[must_use]
    pub const fn capabilities(&self) -> &CapabilityManifest {
        &self.capabilities
    }

    /// Returns resident projection-cache counters for operational tests.
    ///
    #[must_use]
    pub fn projection_cache_stats(&self) -> ControlPlaneProjectionCacheStats {
        self.projection_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .stats
    }

    /// Captures one immutable journal generation and projects its operator
    /// state without adding wall-clock-dependent fields.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneSnapshotError`] when the source cannot be
    /// captured or its durable execution facts cannot be projected safely.
    pub fn snapshot(&self) -> Result<ControlPlaneSnapshot, ControlPlaneSnapshotError> {
        let journal = self.journal.snapshot()?;
        self.cached_snapshot_or_reproject(&journal, map_snapshot_projection_error)
    }

    /// Reads one bounded, payload-redacted event page after an opaque cursor.
    ///
    /// `cursor` is decoded and validated here so transport adapters never
    /// depend on its representation or silently restart an expired stream.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneEventsError::Cursor`] for malformed or expired
    /// cursors and [`ControlPlaneEventsError::Journal`] for source or durable
    /// record failures.
    pub fn events_after(
        &self,
        cursor: Option<&str>,
    ) -> Result<ControlPlaneEventsPage, ControlPlaneEventsError> {
        let cursor = cursor
            .map(str::parse)
            .transpose()
            .map_err(ControlPlaneEventsError::Cursor)?;
        let snapshot = self.journal.snapshot()?;
        let page = LegacyJsonlJournalReader::read_page(&snapshot, cursor.as_ref())
            .map_err(classify_events_error)?;
        Ok(control_plane_events_page(&page))
    }

    /// Captures one journal generation for both the operator projection and
    /// its cursor-bearing change page.
    ///
    /// This is the correct API for transports that return both views in one
    /// response: a concurrent append can never make the projection newer than
    /// the accompanying cursor watermark.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneReadError`] when the cursor, journal, or bounded
    /// projection cannot be represented safely.
    pub fn snapshot_with_events_after(
        &self,
        cursor: Option<&str>,
    ) -> Result<ControlPlaneRead, ControlPlaneReadError> {
        let cursor = cursor
            .map(str::parse)
            .transpose()
            .map_err(ControlPlaneReadError::Cursor)?;
        let journal = self
            .journal
            .snapshot()
            .map_err(ControlPlaneReadError::Journal)?;
        let page =
            LegacyJsonlJournalReader::read_page(&journal, cursor.as_ref()).map_err(|error| {
                match error {
                    JournalReadError::Cursor(source) => ControlPlaneReadError::Cursor(source),
                    source => ControlPlaneReadError::Journal(source),
                }
            })?;
        Ok(ControlPlaneRead {
            snapshot: self.cached_snapshot_or_reproject(&journal, map_read_projection_error)?,
            events: control_plane_events_page(&page),
        })
    }

    fn cached_snapshot_or_reproject<E>(
        &self,
        journal: &JournalSnapshot,
        map_error: fn(crypto_trading_runtime::ControlPlaneProjectionError) -> E,
    ) -> Result<ControlPlaneSnapshot, E> {
        if let Some(snapshot) = self.try_cached_snapshot(journal) {
            self.record_cache_hit();
            return Ok(snapshot);
        }

        let projection = project_control_plane_state(journal).map_err(map_error)?;
        let snapshot = control_plane_snapshot(self.capabilities.clone(), projection);
        let entry = ProjectionCacheEntry::capture(journal, snapshot.clone());
        self.record_cache_miss(entry);
        Ok(snapshot)
    }

    fn try_cached_snapshot(&self, journal: &JournalSnapshot) -> Option<ControlPlaneSnapshot> {
        if journal.len() > MAX_CURSOR_ANCHOR_SCAN_BYTES {
            return None;
        }

        let entry = self
            .projection_cache
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .entry
            .clone()?;
        if entry.journal_id != journal.journal_id() || entry.snapshot_len != journal.len() {
            return None;
        }

        let validation =
            LegacyJsonlJournalReader::read_page(journal, Some(&entry.terminal_cursor)).ok()?;
        if validation.events().is_empty()
            && matches!(
                validation.boundary(),
                JournalPageBoundary::SnapshotEnd | JournalPageBoundary::PartialTail { .. }
            )
        {
            return Some(entry.snapshot);
        }
        None
    }

    fn record_cache_hit(&self) {
        let mut cache = self
            .projection_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        cache.stats.hits = cache.stats.hits.saturating_add(1);
    }

    fn record_cache_miss(&self, entry: Option<ProjectionCacheEntry>) {
        let mut cache = self
            .projection_cache
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        cache.stats.misses = cache.stats.misses.saturating_add(1);
        cache.entry = entry;
        if cache.entry.is_some() {
            cache.stats.stores = cache.stats.stores.saturating_add(1);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlPlaneProjectionCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub stores: u64,
}

#[derive(Clone, Debug, Default)]
struct ProjectionCache {
    entry: Option<ProjectionCacheEntry>,
    stats: ControlPlaneProjectionCacheStats,
}

#[derive(Clone, Debug)]
struct ProjectionCacheEntry {
    journal_id: Uuid,
    snapshot_len: usize,
    terminal_cursor: JournalCursor,
    snapshot: ControlPlaneSnapshot,
}

impl ProjectionCacheEntry {
    fn capture(journal: &JournalSnapshot, snapshot: ControlPlaneSnapshot) -> Option<Self> {
        if journal.len() > MAX_CURSOR_ANCHOR_SCAN_BYTES {
            return None;
        }

        Some(Self {
            journal_id: journal.journal_id(),
            snapshot_len: journal.len(),
            terminal_cursor: snapshot_terminal_cursor(journal)?,
            snapshot,
        })
    }
}

fn snapshot_terminal_cursor(snapshot: &JournalSnapshot) -> Option<JournalCursor> {
    let mut cursor = None;

    loop {
        let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref()).ok()?;
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd | JournalPageBoundary::PartialTail { .. } => {
                return page.next_cursor().cloned();
            }
            JournalPageBoundary::PageLimit => {
                let next = page.next_cursor().cloned()?;
                if cursor.as_ref().is_some_and(|previous| {
                    previous.next_offset() == next.next_offset()
                        && previous.after_sequence() == next.after_sequence()
                }) {
                    return None;
                }
                cursor = Some(next);
            }
        }
    }
}

fn classify_events_error(error: JournalReadError) -> ControlPlaneEventsError {
    match error {
        JournalReadError::Cursor(source) => ControlPlaneEventsError::Cursor(source),
        source => ControlPlaneEventsError::Journal(source),
    }
}

fn control_plane_events_page(page: &JournalPage) -> ControlPlaneEventsPage {
    ControlPlaneEventsPage {
        schema_version: CONTROL_PLANE_EVENTS_SCHEMA_VERSION,
        journal_id: page.journal_id(),
        events: page
            .events()
            .iter()
            .map(ControlPlaneEventNotice::from)
            .collect(),
        next_cursor: page.next_cursor().map(ToString::to_string),
        boundary: page.boundary().clone(),
    }
}

fn control_plane_snapshot(
    capabilities: CapabilityManifest,
    projection: crypto_trading_runtime::ControlPlaneProjection,
) -> ControlPlaneSnapshot {
    ControlPlaneSnapshot {
        schema_version: CONTROL_PLANE_SNAPSHOT_SCHEMA_VERSION,
        capabilities,
        operator: projection.operator,
        monitor: projection.monitor,
        tasks: projection.tasks,
        paper_accounts: projection.paper_accounts,
        account_risk: projection.account_risk,
    }
}

fn map_snapshot_projection_error(
    error: crypto_trading_runtime::ControlPlaneProjectionError,
) -> ControlPlaneSnapshotError {
    match error {
        crypto_trading_runtime::ControlPlaneProjectionError::Projection(source) => {
            ControlPlaneSnapshotError::Projection(source)
        }
        crypto_trading_runtime::ControlPlaneProjectionError::PaperAccountProjection(source) => {
            ControlPlaneSnapshotError::PaperAccountProjection(source)
        }
        crypto_trading_runtime::ControlPlaneProjectionError::AccountRiskProjection(source) => {
            ControlPlaneSnapshotError::AccountRiskProjection(source)
        }
    }
}

fn map_read_projection_error(
    error: crypto_trading_runtime::ControlPlaneProjectionError,
) -> ControlPlaneReadError {
    match error {
        crypto_trading_runtime::ControlPlaneProjectionError::Projection(source) => {
            ControlPlaneReadError::Projection(source)
        }
        crypto_trading_runtime::ControlPlaneProjectionError::PaperAccountProjection(source) => {
            ControlPlaneReadError::PaperAccountProjection(source)
        }
        crypto_trading_runtime::ControlPlaneProjectionError::AccountRiskProjection(source) => {
            ControlPlaneReadError::AccountRiskProjection(source)
        }
    }
}

/// Deterministic operator snapshot consumed by read-only adapters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneSnapshot {
    pub schema_version: u16,
    pub capabilities: CapabilityManifest,
    pub operator: OperatorReadModel,
    pub monitor: ArbitrageMonitorReadModel,
    pub tasks: ReadOnlyTaskReadModel,
    pub paper_accounts: PaperAccountReadModel,
    pub account_risk: AccountRiskReadModel,
}

/// Payload-free notification that tells an adapter which snapshot fact changed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneEventNotice {
    pub sequence: u64,
    pub event_id: Uuid,
    pub recorded_at: DateTime<Utc>,
    pub kind: String,
    pub aggregate_kind: String,
    pub aggregate_id: Uuid,
    pub producer: String,
}

impl From<&OperationEventEnvelope> for ControlPlaneEventNotice {
    fn from(event: &OperationEventEnvelope) -> Self {
        Self {
            sequence: event.sequence(),
            event_id: event.event_id(),
            recorded_at: event.recorded_at(),
            kind: event.kind().to_owned(),
            aggregate_kind: event.aggregate().kind().to_owned(),
            aggregate_id: event.aggregate().id(),
            producer: event.producer().to_owned(),
        }
    }
}

/// One bounded event page with an opaque resume cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneEventsPage {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub events: Vec<ControlPlaneEventNotice>,
    pub next_cursor: Option<String>,
    pub boundary: JournalPageBoundary,
}

impl ControlPlaneEventsPage {
    /// Whether another page can be read immediately from the same snapshot.
    #[must_use]
    pub const fn has_more_in_snapshot(&self) -> bool {
        matches!(self.boundary, JournalPageBoundary::PageLimit)
    }
}

/// Coherent projection and event watermark captured from one journal generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlPlaneRead {
    pub snapshot: ControlPlaneSnapshot,
    pub events: ControlPlaneEventsPage,
}

#[derive(Debug, Error)]
pub enum ControlPlaneSnapshotError {
    #[error(transparent)]
    Journal(#[from] JournalReadError),
    #[error(transparent)]
    Projection(#[from] ReadModelError),
    #[error(transparent)]
    PaperAccountProjection(#[from] PaperAccountProjectionError),
    #[error(transparent)]
    AccountRiskProjection(#[from] AccountRiskProjectionError),
}

impl ControlPlaneSnapshotError {
    #[must_use]
    pub const fn kind(&self) -> ReadFailureKind {
        match self {
            Self::Journal(source)
            | Self::Projection(ReadModelError::Journal(source))
            | Self::PaperAccountProjection(PaperAccountProjectionError::Journal(source))
            | Self::AccountRiskProjection(AccountRiskProjectionError::Journal(source)) => {
                journal_failure_kind(source)
            }
            Self::Projection(
                ReadModelError::BatchLimitExceeded { .. }
                | ReadModelError::TaskLimitExceeded { .. },
            )
            | Self::AccountRiskProjection(
                AccountRiskProjectionError::ScopeLimitExceeded { .. }
                | AccountRiskProjectionError::OpenPositionLimitExceeded { .. }
                | AccountRiskProjectionError::OpenAdmissionLimitExceeded { .. },
            ) => ReadFailureKind::ResourceLimit,
            Self::Projection(ReadModelError::NonAdvancingPage)
            | Self::PaperAccountProjection(
                PaperAccountProjectionError::NonAdvancingPage
                | PaperAccountProjectionError::ArithmeticOverflow,
            )
            | Self::AccountRiskProjection(
                AccountRiskProjectionError::NonAdvancingPage
                | AccountRiskProjectionError::InvalidAdmissionTicket,
            ) => ReadFailureKind::InvalidJournal,
        }
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneEventsError {
    #[error(transparent)]
    Cursor(CursorError),
    #[error(transparent)]
    Journal(#[from] JournalReadError),
}

impl ControlPlaneEventsError {
    #[must_use]
    pub const fn kind(&self) -> ReadFailureKind {
        match self {
            Self::Cursor(source) => cursor_failure_kind(source),
            Self::Journal(source) => journal_failure_kind(source),
        }
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneReadError {
    #[error(transparent)]
    Cursor(CursorError),
    #[error(transparent)]
    Journal(JournalReadError),
    #[error(transparent)]
    Projection(ReadModelError),
    #[error(transparent)]
    PaperAccountProjection(PaperAccountProjectionError),
    #[error(transparent)]
    AccountRiskProjection(AccountRiskProjectionError),
}

impl ControlPlaneReadError {
    #[must_use]
    pub const fn kind(&self) -> ReadFailureKind {
        match self {
            Self::Cursor(source) => cursor_failure_kind(source),
            Self::Journal(source)
            | Self::Projection(ReadModelError::Journal(source))
            | Self::PaperAccountProjection(PaperAccountProjectionError::Journal(source))
            | Self::AccountRiskProjection(AccountRiskProjectionError::Journal(source)) => {
                journal_failure_kind(source)
            }
            Self::Projection(
                ReadModelError::BatchLimitExceeded { .. }
                | ReadModelError::TaskLimitExceeded { .. },
            )
            | Self::AccountRiskProjection(
                AccountRiskProjectionError::ScopeLimitExceeded { .. }
                | AccountRiskProjectionError::OpenPositionLimitExceeded { .. }
                | AccountRiskProjectionError::OpenAdmissionLimitExceeded { .. },
            ) => ReadFailureKind::ResourceLimit,
            Self::Projection(ReadModelError::NonAdvancingPage)
            | Self::PaperAccountProjection(
                PaperAccountProjectionError::NonAdvancingPage
                | PaperAccountProjectionError::ArithmeticOverflow,
            )
            | Self::AccountRiskProjection(
                AccountRiskProjectionError::NonAdvancingPage
                | AccountRiskProjectionError::InvalidAdmissionTicket,
            ) => ReadFailureKind::InvalidJournal,
        }
    }
}

const fn cursor_failure_kind(error: &CursorError) -> ReadFailureKind {
    match error {
        CursorError::Expired => ReadFailureKind::ExpiredCursor,
        CursorError::TooLong { .. }
        | CursorError::Malformed(_)
        | CursorError::UnsupportedVersion(_)
        | CursorError::ChecksumMismatch
        | CursorError::InvalidValue(_) => ReadFailureKind::InvalidCursor,
    }
}

const fn journal_failure_kind(error: &JournalReadError) -> ReadFailureKind {
    match error {
        JournalReadError::Cursor(source) => cursor_failure_kind(source),
        JournalReadError::SourceTooLarge { .. }
        | JournalReadError::ChainTooLarge { .. }
        | JournalReadError::TooManySegments { .. }
        | JournalReadError::Allocation { .. }
        | JournalReadError::RecordTooLarge { .. } => ReadFailureKind::ResourceLimit,
        JournalReadError::NotAFile
        | JournalReadError::Open(_)
        | JournalReadError::Metadata(_)
        | JournalReadError::Read(_)
        | JournalReadError::SourceChanged { .. }
        | JournalReadError::SealedSegmentInspect { .. } => ReadFailureKind::SourceUnavailable,
        JournalReadError::NilJournalId
        | JournalReadError::EmptyRecord { .. }
        | JournalReadError::MalformedRecord { .. }
        | JournalReadError::SequenceOverflow
        | JournalReadError::SealedSegmentGap { .. }
        | JournalReadError::SealedSegmentBytes { .. }
        | JournalReadError::SealedSegmentNotAFile { .. }
        | JournalReadError::SealedSegmentPartialTail { .. }
        | JournalReadError::EventContract { .. } => ReadFailureKind::InvalidJournal,
    }
}
