use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    JournalPageBoundary, JournalSnapshot, LegacyJsonlJournalReader, OperationEventEnvelope,
    ProjectionStatus, ReadModelError,
};

/// Stable schema version for durable read-only task lifecycle projections.
pub const READ_ONLY_TASK_READ_MODEL_SCHEMA_VERSION: u16 = 1;
/// Hard bound on distinct task identities represented by one snapshot.
pub const MAX_READ_ONLY_TASKS: usize = 64;

const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";
const MAX_TASK_TEXT_BYTES: usize = 128;
const SINGLE_SOURCE_COUNT: usize = 1;
const ARBITRAGE_SOURCE_COUNT: usize = 2;

/// Durable task lifecycle projection reconstructed only from journal facts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlyTaskReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub journal_head_sequence: Option<u64>,
    pub projection_status: ProjectionStatus,
    pub tasks: Vec<ReadOnlyTaskView>,
    pub invalid_event_count: u64,
}

impl ReadOnlyTaskReadModel {
    /// Projects bounded task lifecycle facts from one immutable journal
    /// snapshot. The projection never auto-resumes external work.
    ///
    /// # Errors
    ///
    /// Returns [`ReadModelError`] for journal failures, a non-advancing page,
    /// or more distinct task identities than the hard projection limit.
    pub fn from_legacy_snapshot(snapshot: &JournalSnapshot) -> Result<Self, ReadModelError> {
        TaskProjectionBuilder::new(snapshot.journal_id()).project(snapshot)
    }
}

/// Read-only task kind currently supported by the durable lifecycle contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskKind {
    ArbitrageMonitor,
    ArbitragePaper,
    GridPaper,
    PriceAlert,
    Scanner,
    VolumeMaker,
}

/// Last durably recorded aggregate phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskPhase {
    Registered,
    Running,
    Stopping,
    Stopped,
    Failed,
}

/// Recovery guidance derived from durable facts, not process-local liveness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskRecovery {
    None,
    Investigate,
}

/// Bounded terminal reason for a normally stopped read-only task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
    Completed,
}

/// Bounded failure bucket. Raw remote, panic, filesystem, and adapter text is
/// deliberately excluded from the public projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskFailure {
    StartupFailed,
    SourceContract,
    MonitorContract,
    JournalUnavailable,
    TaskPanicked,
    TaskCancelled,
    InvalidRequest,
    RecoveryRequired,
    AccountContract,
    ExecutionIncomplete,
    ExecutionFailed,
}

/// Last durably recorded source-supervisor phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskSourcePhase {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

/// Last durably recorded source health.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskSourceHealth {
    Unknown,
    Healthy,
    Degraded,
}

/// Last durably recorded normal source-supervisor exit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadOnlyTaskSourceExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Safe, bounded source-supervisor status retained by the task projection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlyTaskSourceView {
    pub task_id: Option<Uuid>,
    pub source_id: String,
    pub phase: ReadOnlyTaskSourcePhase,
    pub health: ReadOnlyTaskSourceHealth,
    pub event_sequence: u64,
    pub consecutive_source_failures: u32,
    pub last_event_at: Option<DateTime<Utc>>,
    pub exit: Option<ReadOnlyTaskSourceExit>,
}

/// Last valid lifecycle fact for one stable task identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlyTaskView {
    pub task_id: String,
    pub kind: ReadOnlyTaskKind,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub registered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub phase: ReadOnlyTaskPhase,
    pub recovery: ReadOnlyTaskRecovery,
    pub processed_event_count: u64,
    pub sources: Vec<ReadOnlyTaskSourceView>,
    pub exit: Option<ReadOnlyTaskExit>,
    pub failure: Option<ReadOnlyTaskFailure>,
}

struct TaskProjectionBuilder {
    journal_id: Uuid,
    journal_head_sequence: Option<u64>,
    projection_status: ProjectionStatus,
    tasks: Vec<ReadOnlyTaskView>,
    invalid_event_count: u64,
}

impl TaskProjectionBuilder {
    const fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            journal_head_sequence: None,
            projection_status: ProjectionStatus::Complete,
            tasks: Vec::new(),
            invalid_event_count: 0,
        }
    }

    fn project(
        mut self,
        snapshot: &JournalSnapshot,
    ) -> Result<ReadOnlyTaskReadModel, ReadModelError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            if let Some(event) = page.events().last() {
                self.journal_head_sequence = Some(event.sequence());
            }
            for event in page.events() {
                self.apply_event(event)?;
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    self.projection_status = ProjectionStatus::Degraded;
                    break;
                }
                JournalPageBoundary::PageLimit => {
                    let next = page
                        .next_cursor()
                        .cloned()
                        .ok_or(ReadModelError::NonAdvancingPage)?;
                    if cursor.as_ref().is_some_and(|previous| {
                        previous.next_offset() == next.next_offset()
                            && previous.after_sequence() == next.after_sequence()
                    }) {
                        return Err(ReadModelError::NonAdvancingPage);
                    }
                    cursor = Some(next);
                }
            }
        }
        Ok(ReadOnlyTaskReadModel {
            schema_version: READ_ONLY_TASK_READ_MODEL_SCHEMA_VERSION,
            journal_id: self.journal_id,
            journal_head_sequence: self.journal_head_sequence,
            projection_status: self.projection_status,
            tasks: self.tasks,
            invalid_event_count: self.invalid_event_count,
        })
    }

    fn apply_event(&mut self, event: &OperationEventEnvelope) -> Result<(), ReadModelError> {
        match parse_task_event(event) {
            ParsedTaskEvent::Other => {}
            ParsedTaskEvent::Invalid { task_id } => self.record_invalid(task_id.as_deref()),
            ParsedTaskEvent::Fact(fact) => match self.apply_fact(fact) {
                Ok(()) => {}
                Err(ApplyFactError::Invalid(task_id)) => {
                    self.record_invalid(Some(&task_id));
                }
                Err(ApplyFactError::TaskLimitExceeded) => {
                    return Err(ReadModelError::TaskLimitExceeded {
                        limit: MAX_READ_ONLY_TASKS,
                    });
                }
            },
        }
        Ok(())
    }

    fn apply_fact(&mut self, fact: TaskFact) -> Result<(), ApplyFactError> {
        let existing = self
            .tasks
            .iter()
            .position(|task| task.task_id == fact.task_id);
        let Some(index) = existing else {
            if fact.phase != ReadOnlyTaskPhase::Registered {
                return Err(ApplyFactError::Invalid(fact.task_id));
            }
            if self.tasks.len() >= MAX_READ_ONLY_TASKS {
                return Err(ApplyFactError::TaskLimitExceeded);
            }
            let recovery = recovery_for(fact.phase, fact.exit);
            self.tasks.push(ReadOnlyTaskView {
                task_id: fact.task_id,
                kind: fact.kind,
                first_sequence: fact.source_sequence,
                last_sequence: fact.source_sequence,
                registered_at: fact.recorded_at,
                updated_at: fact.recorded_at,
                phase: fact.phase,
                recovery,
                processed_event_count: fact.processed_event_count,
                sources: fact.sources,
                exit: fact.exit,
                failure: fact.failure,
            });
            return Ok(());
        };

        let task = &mut self.tasks[index];
        let restarting = task.phase == ReadOnlyTaskPhase::Stopped
            && task.recovery == ReadOnlyTaskRecovery::None
            && fact.decision == TaskDecision::Registered
            && task.kind == fact.kind
            && same_source_identities(&task.sources, &fact.sources);
        if restarting {
            // One stable owner identity may have multiple clean process runs.
            // Registration resets only the lifecycle/source instance; account
            // operations retain their independent durable identities.
            task.first_sequence = fact.source_sequence;
            task.last_sequence = fact.source_sequence;
            task.registered_at = fact.recorded_at;
            task.updated_at = fact.recorded_at;
            task.phase = fact.phase;
            task.recovery = recovery_for(fact.phase, fact.exit);
            task.processed_event_count = fact.processed_event_count;
            task.sources = fact.sources;
            task.exit = fact.exit;
            task.failure = fact.failure;
            return Ok(());
        }
        if !valid_transition(task.phase, fact.phase, fact.decision)
            || task.kind != fact.kind
            || fact.processed_event_count < task.processed_event_count
            || !same_source_contract(&task.sources, &fact.sources)
        {
            return Err(ApplyFactError::Invalid(fact.task_id));
        }
        task.last_sequence = fact.source_sequence;
        task.updated_at = task.updated_at.max(fact.recorded_at);
        task.phase = fact.phase;
        task.recovery = recovery_for(fact.phase, fact.exit);
        task.processed_event_count = fact.processed_event_count;
        task.sources = fact.sources;
        task.exit = fact.exit;
        task.failure = fact.failure;
        Ok(())
    }

    fn record_invalid(&mut self, task_id: Option<&str>) {
        self.projection_status = ProjectionStatus::Degraded;
        self.invalid_event_count = self.invalid_event_count.saturating_add(1);
        if let Some(task) =
            task_id.and_then(|task_id| self.tasks.iter_mut().find(|task| task.task_id == task_id))
        {
            task.recovery = ReadOnlyTaskRecovery::Investigate;
        }
    }
}

enum ApplyFactError {
    Invalid(String),
    TaskLimitExceeded,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskDecision {
    Registered,
    Running,
    Checkpointed,
    Stopping,
    Stopped,
    Failed,
}

struct TaskFact {
    source_sequence: u64,
    recorded_at: DateTime<Utc>,
    task_id: String,
    kind: ReadOnlyTaskKind,
    decision: TaskDecision,
    phase: ReadOnlyTaskPhase,
    processed_event_count: u64,
    sources: Vec<ReadOnlyTaskSourceView>,
    exit: Option<ReadOnlyTaskExit>,
    failure: Option<ReadOnlyTaskFailure>,
}

enum ParsedTaskEvent {
    Other,
    Invalid { task_id: Option<String> },
    Fact(TaskFact),
}

fn parse_task_event(event: &OperationEventEnvelope) -> ParsedTaskEvent {
    let Ok(payload) = object(event.payload()) else {
        return ParsedTaskEvent::Other;
    };
    let Some(strategy) = payload.get("strategy").and_then(Value::as_str) else {
        return ParsedTaskEvent::Other;
    };
    if strategy != TASK_STRATEGY {
        return ParsedTaskEvent::Other;
    }
    let task_id = payload
        .get("details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("task_id"))
        .and_then(Value::as_str)
        .and_then(|value| bounded_text(value).ok());
    match parse_task_fact(event, payload) {
        Ok(fact) => ParsedTaskEvent::Fact(fact),
        Err(()) => ParsedTaskEvent::Invalid { task_id },
    }
}

fn parse_task_fact(
    event: &OperationEventEnvelope,
    payload: &Map<String, Value>,
) -> Result<TaskFact, ()> {
    if required_text(payload, "symbol")? != TASK_SYMBOL {
        return Err(());
    }
    let decision = match required_text(payload, "decision")?.as_str() {
        "task_registered" => TaskDecision::Registered,
        "task_running" => TaskDecision::Running,
        "task_checkpointed" => TaskDecision::Checkpointed,
        "task_stopping" => TaskDecision::Stopping,
        "task_stopped" => TaskDecision::Stopped,
        "task_failed" => TaskDecision::Failed,
        _ => return Err(()),
    };
    let details = object(required(payload, "details")?)?;
    ensure_exact_fields(
        details,
        &[
            "schema_version",
            "task_id",
            "task_kind",
            "phase",
            "processed_event_count",
            "sources",
            "exit",
            "failure",
        ],
    )?;
    if required_u64(details, "schema_version")?
        != u64::from(READ_ONLY_TASK_READ_MODEL_SCHEMA_VERSION)
    {
        return Err(());
    }
    let task_id = required_text(details, "task_id")?;
    let kind = match required_text(details, "task_kind")?.as_str() {
        "arbitrage_monitor" => ReadOnlyTaskKind::ArbitrageMonitor,
        "arbitrage_paper" => ReadOnlyTaskKind::ArbitragePaper,
        "grid_paper" => ReadOnlyTaskKind::GridPaper,
        "price_alert" => ReadOnlyTaskKind::PriceAlert,
        "scanner" => ReadOnlyTaskKind::Scanner,
        "volume_maker" => ReadOnlyTaskKind::VolumeMaker,
        _ => return Err(()),
    };
    let phase_text = required_text(details, "phase")?;
    let phase = parse_phase(&phase_text)?;
    if !decision_matches_phase(decision, phase) {
        return Err(());
    }
    let processed_event_count = required_u64(details, "processed_event_count")?;
    let sources = parse_sources(required(details, "sources")?)?;
    let expected_source_count = match kind {
        ReadOnlyTaskKind::GridPaper
        | ReadOnlyTaskKind::PriceAlert
        | ReadOnlyTaskKind::Scanner
        | ReadOnlyTaskKind::VolumeMaker => SINGLE_SOURCE_COUNT,
        ReadOnlyTaskKind::ArbitrageMonitor | ReadOnlyTaskKind::ArbitragePaper => {
            ARBITRAGE_SOURCE_COUNT
        }
    };
    if sources.len() != expected_source_count {
        return Err(());
    }
    let exit = optional_exit(required(details, "exit")?)?;
    let failure = optional_failure(required(details, "failure")?)?;
    validate_fact_shape(
        decision,
        phase,
        processed_event_count,
        &sources,
        exit,
        failure,
    )?;
    Ok(TaskFact {
        source_sequence: event.sequence(),
        recorded_at: event.recorded_at(),
        task_id,
        kind,
        decision,
        phase,
        processed_event_count,
        sources,
        exit,
        failure,
    })
}

fn validate_fact_shape(
    decision: TaskDecision,
    phase: ReadOnlyTaskPhase,
    processed_event_count: u64,
    sources: &[ReadOnlyTaskSourceView],
    exit: Option<ReadOnlyTaskExit>,
    failure: Option<ReadOnlyTaskFailure>,
) -> Result<(), ()> {
    match phase {
        ReadOnlyTaskPhase::Registered => {
            if processed_event_count != 0
                || exit.is_some()
                || failure.is_some()
                || sources.iter().any(|source| {
                    source.task_id.is_some()
                        || source.phase != ReadOnlyTaskSourcePhase::Starting
                        || source.health != ReadOnlyTaskSourceHealth::Unknown
                        || source.event_sequence != 0
                        || source.consecutive_source_failures != 0
                        || source.last_event_at.is_some()
                        || source.exit.is_some()
                })
            {
                return Err(());
            }
        }
        ReadOnlyTaskPhase::Running | ReadOnlyTaskPhase::Stopping => {
            if exit.is_some()
                || failure.is_some()
                || sources.iter().any(|source| source.task_id.is_none())
            {
                return Err(());
            }
        }
        ReadOnlyTaskPhase::Stopped => {
            if exit.is_none()
                || failure.is_some()
                || sources.iter().any(|source| source.task_id.is_none())
                || (exit != Some(ReadOnlyTaskExit::ShutdownTimedOut)
                    && sources
                        .iter()
                        .any(|source| source.phase != ReadOnlyTaskSourcePhase::Stopped))
            {
                return Err(());
            }
        }
        ReadOnlyTaskPhase::Failed => {
            if exit.is_some() || failure.is_none() {
                return Err(());
            }
            if decision != TaskDecision::Failed {
                return Err(());
            }
        }
    }
    Ok(())
}

fn parse_sources(value: &Value) -> Result<Vec<ReadOnlyTaskSourceView>, ()> {
    let rows = value.as_array().ok_or(())?;
    if rows.is_empty() || rows.len() > ARBITRAGE_SOURCE_COUNT {
        return Err(());
    }
    let mut sources = Vec::with_capacity(rows.len());
    for row in rows {
        sources.push(parse_source(row)?);
    }
    if sources.iter().enumerate().any(|(index, source)| {
        sources[..index]
            .iter()
            .any(|previous| previous.source_id == source.source_id)
    }) {
        return Err(());
    }
    Ok(sources)
}

fn parse_source(value: &Value) -> Result<ReadOnlyTaskSourceView, ()> {
    let source = object(value)?;
    ensure_exact_fields(
        source,
        &[
            "schema_version",
            "task_id",
            "source_id",
            "phase",
            "health",
            "event_sequence",
            "consecutive_source_failures",
            "last_event_at",
            "exit",
        ],
    )?;
    if required_u64(source, "schema_version")?
        != u64::from(crate::MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION)
    {
        return Err(());
    }
    let task_id = optional_uuid(required(source, "task_id")?)?;
    let source_id = required_text(source, "source_id")?;
    let phase = match required_text(source, "phase")?.as_str() {
        "starting" => ReadOnlyTaskSourcePhase::Starting,
        "running" => ReadOnlyTaskSourcePhase::Running,
        "stopping" => ReadOnlyTaskSourcePhase::Stopping,
        "stopped" => ReadOnlyTaskSourcePhase::Stopped,
        "failed" => ReadOnlyTaskSourcePhase::Failed,
        _ => return Err(()),
    };
    let health = match required_text(source, "health")?.as_str() {
        "unknown" => ReadOnlyTaskSourceHealth::Unknown,
        "healthy" => ReadOnlyTaskSourceHealth::Healthy,
        "degraded" => ReadOnlyTaskSourceHealth::Degraded,
        _ => return Err(()),
    };
    let event_sequence = required_u64(source, "event_sequence")?;
    let failures = required_u64(source, "consecutive_source_failures")?;
    let consecutive_source_failures = u32::try_from(failures).map_err(|_| ())?;
    let last_event_at = optional_timestamp(required(source, "last_event_at")?)?;
    if (event_sequence == 0) != last_event_at.is_none() {
        return Err(());
    }
    let exit = optional_source_exit(required(source, "exit")?)?;
    if (phase == ReadOnlyTaskSourcePhase::Stopped) != exit.is_some() {
        return Err(());
    }
    Ok(ReadOnlyTaskSourceView {
        task_id,
        source_id,
        phase,
        health,
        event_sequence,
        consecutive_source_failures,
        last_event_at,
        exit,
    })
}

fn same_source_identities(
    previous: &[ReadOnlyTaskSourceView],
    next: &[ReadOnlyTaskSourceView],
) -> bool {
    previous.len() == next.len()
        && previous
            .iter()
            .zip(next)
            .all(|(previous, next)| previous.source_id == next.source_id)
}

fn same_source_contract(
    previous: &[ReadOnlyTaskSourceView],
    next: &[ReadOnlyTaskSourceView],
) -> bool {
    previous.len() == next.len()
        && previous.iter().zip(next).all(|(previous, next)| {
            previous.source_id == next.source_id
                && next.event_sequence >= previous.event_sequence
                && previous
                    .task_id
                    .is_none_or(|task_id| next.task_id == Some(task_id))
                && !(previous.task_id.is_some() && next.task_id.is_none())
                && valid_source_transition(previous.phase, next.phase)
                && (next.event_sequence != previous.event_sequence
                    || (next.last_event_at == previous.last_event_at
                        && next.consecutive_source_failures
                            == previous.consecutive_source_failures
                        && next.health == previous.health))
        })
}

const fn valid_source_transition(
    previous: ReadOnlyTaskSourcePhase,
    next: ReadOnlyTaskSourcePhase,
) -> bool {
    matches!(
        (previous, next),
        (
            ReadOnlyTaskSourcePhase::Starting,
            ReadOnlyTaskSourcePhase::Starting
                | ReadOnlyTaskSourcePhase::Running
                | ReadOnlyTaskSourcePhase::Stopping
                | ReadOnlyTaskSourcePhase::Stopped
                | ReadOnlyTaskSourcePhase::Failed
        ) | (
            ReadOnlyTaskSourcePhase::Running,
            ReadOnlyTaskSourcePhase::Running
                | ReadOnlyTaskSourcePhase::Stopping
                | ReadOnlyTaskSourcePhase::Stopped
                | ReadOnlyTaskSourcePhase::Failed
        ) | (
            ReadOnlyTaskSourcePhase::Stopping,
            ReadOnlyTaskSourcePhase::Stopping
                | ReadOnlyTaskSourcePhase::Stopped
                | ReadOnlyTaskSourcePhase::Failed
        ) | (
            ReadOnlyTaskSourcePhase::Stopped,
            ReadOnlyTaskSourcePhase::Stopped
        ) | (
            ReadOnlyTaskSourcePhase::Failed,
            ReadOnlyTaskSourcePhase::Failed
        )
    )
}

const fn valid_transition(
    previous: ReadOnlyTaskPhase,
    next: ReadOnlyTaskPhase,
    decision: TaskDecision,
) -> bool {
    matches!(
        (previous, next, decision),
        (
            ReadOnlyTaskPhase::Registered,
            ReadOnlyTaskPhase::Running,
            TaskDecision::Running
        ) | (
            ReadOnlyTaskPhase::Registered
                | ReadOnlyTaskPhase::Running
                | ReadOnlyTaskPhase::Stopping,
            ReadOnlyTaskPhase::Failed,
            TaskDecision::Failed
        ) | (
            ReadOnlyTaskPhase::Running,
            ReadOnlyTaskPhase::Running,
            TaskDecision::Checkpointed
        ) | (
            ReadOnlyTaskPhase::Running,
            ReadOnlyTaskPhase::Stopping,
            TaskDecision::Stopping
        ) | (
            ReadOnlyTaskPhase::Running | ReadOnlyTaskPhase::Stopping,
            ReadOnlyTaskPhase::Stopped,
            TaskDecision::Stopped
        )
    )
}

const fn decision_matches_phase(decision: TaskDecision, phase: ReadOnlyTaskPhase) -> bool {
    matches!(
        (decision, phase),
        (TaskDecision::Registered, ReadOnlyTaskPhase::Registered)
            | (
                TaskDecision::Running | TaskDecision::Checkpointed,
                ReadOnlyTaskPhase::Running
            )
            | (TaskDecision::Stopping, ReadOnlyTaskPhase::Stopping)
            | (TaskDecision::Stopped, ReadOnlyTaskPhase::Stopped)
            | (TaskDecision::Failed, ReadOnlyTaskPhase::Failed)
    )
}

const fn recovery_for(
    phase: ReadOnlyTaskPhase,
    exit: Option<ReadOnlyTaskExit>,
) -> ReadOnlyTaskRecovery {
    match (phase, exit) {
        (
            ReadOnlyTaskPhase::Stopped,
            Some(
                ReadOnlyTaskExit::StopRequested
                | ReadOnlyTaskExit::SourceEnded
                | ReadOnlyTaskExit::Completed,
            ),
        ) => ReadOnlyTaskRecovery::None,
        _ => ReadOnlyTaskRecovery::Investigate,
    }
}

fn parse_phase(value: &str) -> Result<ReadOnlyTaskPhase, ()> {
    match value {
        "registered" => Ok(ReadOnlyTaskPhase::Registered),
        "running" => Ok(ReadOnlyTaskPhase::Running),
        "stopping" => Ok(ReadOnlyTaskPhase::Stopping),
        "stopped" => Ok(ReadOnlyTaskPhase::Stopped),
        "failed" => Ok(ReadOnlyTaskPhase::Failed),
        _ => Err(()),
    }
}

fn optional_exit(value: &Value) -> Result<Option<ReadOnlyTaskExit>, ()> {
    optional_text(value)?
        .map(|value| match value.as_str() {
            "stop_requested" => Ok(ReadOnlyTaskExit::StopRequested),
            "source_ended" => Ok(ReadOnlyTaskExit::SourceEnded),
            "shutdown_timed_out" => Ok(ReadOnlyTaskExit::ShutdownTimedOut),
            "completed" => Ok(ReadOnlyTaskExit::Completed),
            _ => Err(()),
        })
        .transpose()
}

fn optional_failure(value: &Value) -> Result<Option<ReadOnlyTaskFailure>, ()> {
    optional_text(value)?
        .map(|value| match value.as_str() {
            "startup_failed" => Ok(ReadOnlyTaskFailure::StartupFailed),
            "source_contract" => Ok(ReadOnlyTaskFailure::SourceContract),
            "monitor_contract" => Ok(ReadOnlyTaskFailure::MonitorContract),
            "journal_unavailable" => Ok(ReadOnlyTaskFailure::JournalUnavailable),
            "task_panicked" => Ok(ReadOnlyTaskFailure::TaskPanicked),
            "task_cancelled" => Ok(ReadOnlyTaskFailure::TaskCancelled),
            "invalid_request" => Ok(ReadOnlyTaskFailure::InvalidRequest),
            "recovery_required" => Ok(ReadOnlyTaskFailure::RecoveryRequired),
            "account_contract" => Ok(ReadOnlyTaskFailure::AccountContract),
            "execution_incomplete" => Ok(ReadOnlyTaskFailure::ExecutionIncomplete),
            "execution_failed" => Ok(ReadOnlyTaskFailure::ExecutionFailed),
            _ => Err(()),
        })
        .transpose()
}

fn optional_source_exit(value: &Value) -> Result<Option<ReadOnlyTaskSourceExit>, ()> {
    optional_text(value)?
        .map(|value| match value.as_str() {
            "stop_requested" => Ok(ReadOnlyTaskSourceExit::StopRequested),
            "source_ended" => Ok(ReadOnlyTaskSourceExit::SourceEnded),
            "shutdown_timed_out" => Ok(ReadOnlyTaskSourceExit::ShutdownTimedOut),
            _ => Err(()),
        })
        .transpose()
}

fn optional_uuid(value: &Value) -> Result<Option<Uuid>, ()> {
    let Some(value) = optional_text(value)? else {
        return Ok(None);
    };
    let task_id = Uuid::parse_str(&value).map_err(|_| ())?;
    if task_id.is_nil() {
        return Err(());
    }
    Ok(Some(task_id))
}

fn optional_timestamp(value: &Value) -> Result<Option<DateTime<Utc>>, ()> {
    let Some(value) = optional_text(value)? else {
        return Ok(None);
    };
    value.parse().map(Some).map_err(|_| ())
}

fn optional_text(value: &Value) -> Result<Option<String>, ()> {
    if value.is_null() {
        return Ok(None);
    }
    value.as_str().ok_or(()).and_then(bounded_text).map(Some)
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ()> {
    object.get(key).ok_or(())
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ()> {
    required(object, key)?.as_u64().ok_or(())
}

fn required_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    required(object, key)?
        .as_str()
        .ok_or(())
        .and_then(bounded_text)
}

fn bounded_text(value: &str) -> Result<String, ()> {
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_TASK_TEXT_BYTES {
        return Err(());
    }
    Ok(value.to_owned())
}

fn ensure_exact_fields(object: &Map<String, Value>, fields: &[&str]) -> Result<(), ()> {
    if object.len() != fields.len() || fields.iter().any(|field| !object.contains_key(*field)) {
        return Err(());
    }
    Ok(())
}

fn object(value: &Value) -> Result<&Map<String, Value>, ()> {
    value.as_object().ok_or(())
}
