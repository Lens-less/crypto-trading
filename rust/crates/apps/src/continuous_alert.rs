//! Durable, read-only owner for one single-source price-alert evaluator.
//!
//! The public seam deliberately exposes only `start`, `status`, and `stop`.
//! Source supervision, journal ordering, and Tokio lifecycle machinery remain
//! private. The owner holds no exchange execution handle: the wrapped
//! [`PriceAlertRuntime`] journals alert facts itself, and this module only adds
//! the durable task lifecycle around it.

use std::{fmt, time::Duration};

use chrono::{DateTime, Utc};
use crypto_trading_runtime::{
    DecisionRecord, HistoryError, JsonlHistory, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
    MarketDataEvent, MarketDataEventSource, MarketSupervisor, MarketSupervisorConfig,
    MarketSupervisorError, MarketSupervisorExit, MarketSupervisorHealth, MarketSupervisorPhase,
    MarketSupervisorStatus,
};
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::Instant,
};

use crate::alert::{NotificationDispatcherExit, PriceAlertRuntime, PriceAlertRuntimeError};
use crate::task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture};

/// Stable schema version for the process-local task status surface.
pub const CONTINUOUS_ALERT_TASK_STATUS_SCHEMA_VERSION: u16 = 1;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const MAX_TASK_ID_BYTES: usize = 128;
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";

/// Validated configuration for one exact single-source alert owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuousAlertTaskConfig {
    task_id: String,
    source_id: String,
    supervisor: MarketSupervisorConfig,
}

impl ContinuousAlertTaskConfig {
    /// Creates one stable task identity, exact source binding, and bounded
    /// source shutdown policy.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuousAlertTaskError::InvalidConfig`] for an empty,
    /// oversized, or transport-unsafe task or source identity.
    pub fn new(
        task_id: impl Into<String>,
        source_id: impl Into<String>,
        supervisor: MarketSupervisorConfig,
    ) -> Result<Self, ContinuousAlertTaskError> {
        let task_id = task_id.into();
        let task_id = task_id.trim();
        let source_id = source_id.into();
        let source_id = source_id.trim();
        if !safe_identity(task_id) || !safe_identity(source_id) {
            return Err(ContinuousAlertTaskError::InvalidConfig);
        }
        Ok(Self {
            task_id: task_id.to_owned(),
            source_id: source_id.to_owned(),
            supervisor,
        })
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

fn safe_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TASK_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

/// Aggregate process-local lifecycle phase. Durable readers separately treat
/// every nonterminal phase as unverified after restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousAlertTaskPhase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl ContinuousAlertTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

/// Bounded normal terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousAlertTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Bounded failure bucket suitable for operator status and durable facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousAlertTaskFailure {
    StartupFailed,
    SourceContract,
    AlertContract,
    JournalUnavailable,
    TaskPanicked,
    TaskCancelled,
}

/// Latest durable lifecycle status plus an explicitly process-local fault.
///
/// Every field except `runtime_failure` advances only after its corresponding
/// journal append. `runtime_failure` is never serialized; it reports that the
/// owner terminated before a new durable lifecycle fact could be committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuousAlertTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: ContinuousAlertTaskPhase,
    pub processed_event_count: u64,
    pub sources: Vec<MarketSupervisorStatus>,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub exit: Option<ContinuousAlertTaskExit>,
    pub failure: Option<ContinuousAlertTaskFailure>,
    pub runtime_failure: Option<ContinuousAlertTaskFailure>,
}

impl ContinuousAlertTaskStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Opaque owner of one source supervisor and one alert-evaluation loop.
#[derive(Debug)]
pub struct ContinuousAlertTask {
    stop: watch::Sender<bool>,
    status_sender: watch::Sender<ContinuousAlertTaskStatus>,
    status: watch::Receiver<ContinuousAlertTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<ContinuousAlertTaskExit, ContinuousAlertTaskFailure>>,
    history: JsonlHistory,
    shutdown_grace: Duration,
}

impl ContinuousAlertTask {
    /// Durably registers and starts one exact single-source read-only
    /// price-alert owner.
    ///
    /// The source identity must match the configured exact binding.
    /// Registration is synced before the source supervisor starts; running
    /// status is synced before this method returns.
    ///
    /// # Errors
    ///
    /// Returns a bounded configuration/source error before registration, or a
    /// journal/startup error while establishing the durable lifecycle.
    pub async fn start<S>(
        config: ContinuousAlertTaskConfig,
        runtime: PriceAlertRuntime,
        source: S,
        history: JsonlHistory,
    ) -> Result<Self, ContinuousAlertTaskError>
    where
        S: MarketDataEventSource,
    {
        if source.source_id() != config.source_id {
            return Err(ContinuousAlertTaskError::InvalidSourceBinding);
        }

        let registered_at = Utc::now();
        history
            .append(&registered_record(
                &config.task_id,
                &config.source_id,
                registered_at,
            ))
            .await
            .map_err(ContinuousAlertTaskError::Journal)?;

        let Ok(mut supervisor) = MarketSupervisor::start_new(source, config.supervisor) else {
            history
                .append(&lifecycle_record(
                    &config.task_id,
                    "task_failed",
                    "failed",
                    0,
                    Value::Array(vec![placeholder_source_value(&config.source_id)]),
                    None,
                    Some("startup_failed"),
                    Utc::now().max(registered_at),
                ))
                .await
                .map_err(ContinuousAlertTaskError::Journal)?;
            return Err(ContinuousAlertTaskError::SourceContract);
        };

        let running_at = Utc::now().max(registered_at);
        let initial = ContinuousAlertTaskStatus {
            schema_version: CONTINUOUS_ALERT_TASK_STATUS_SCHEMA_VERSION,
            task_id: config.task_id.clone(),
            phase: ContinuousAlertTaskPhase::Running,
            processed_event_count: 0,
            sources: vec![supervisor.status()],
            last_recorded_at: Some(running_at),
            exit: None,
            failure: None,
            runtime_failure: None,
        };
        if let Err(error) = history
            .append(&status_record(&initial, "task_running", running_at))
            .await
        {
            let _ = supervisor.stop().await;
            return Err(ContinuousAlertTaskError::Journal(error));
        }

        let (stop, stop_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial);
        let task_status_sender = status_sender.clone();
        let task_history = history.clone();
        let join = tokio::spawn(async move {
            run_owner(
                runtime,
                supervisor,
                task_history,
                task_status_sender,
                stop_receiver,
                running_at,
            )
            .await
        });

        Ok(Self {
            stop,
            status_sender,
            status,
            join: Some(join),
            completion: None,
            history,
            shutdown_grace: config.supervisor.shutdown_grace(),
        })
    }

    /// Returns the latest status without waiting or exposing the watch channel.
    #[must_use]
    pub fn status(&self) -> ContinuousAlertTaskStatus {
        self.status.borrow().clone()
    }

    /// Requests cancellation and gives the owner one configured source-shutdown
    /// grace. Any fallback terminal write shares one outer deadline of twice
    /// that grace, so the complete close path remains bounded. Successful stops
    /// are idempotent.
    ///
    /// # Errors
    ///
    /// Returns a typed runtime/journal failure, or a bounded previous-failure
    /// bucket on a later idempotent call.
    pub async fn stop(&mut self) -> Result<ContinuousAlertTaskExit, ContinuousAlertTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(ContinuousAlertTaskError::PreviouslyFailed);
        }
        let _ = self.stop.send(true);
        let Some(mut join) = self.join.take() else {
            return Err(ContinuousAlertTaskError::TaskCancelled);
        };
        let started_at = Instant::now();
        let owner_deadline = started_at + self.shutdown_grace;
        let shutdown_deadline = started_at + self.shutdown_grace.saturating_mul(2);
        let result = if let Ok(joined) = tokio::time::timeout_at(owner_deadline, &mut join).await {
            self.map_join_result(joined, shutdown_deadline).await
        } else {
            join.abort();
            self.record_forced_exit(ContinuousAlertTaskExit::ShutdownTimedOut, shutdown_deadline)
                .await
        };
        self.completion = Some(match &result {
            Ok(exit) => Ok(*exit),
            Err(error) => Err(error.failure_bucket()),
        });
        result
    }

    async fn map_join_result(
        &mut self,
        joined: Result<TaskResult, JoinError>,
        deadline: Instant,
    ) -> Result<ContinuousAlertTaskExit, ContinuousAlertTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) if error.is_panic() => {
                self.record_external_failure(ContinuousAlertTaskFailure::TaskPanicked, deadline)
                    .await?;
                Err(ContinuousAlertTaskError::TaskPanicked)
            }
            Err(_) => {
                self.record_external_failure(ContinuousAlertTaskFailure::TaskCancelled, deadline)
                    .await?;
                Err(ContinuousAlertTaskError::TaskCancelled)
            }
        }
    }

    async fn record_forced_exit(
        &mut self,
        exit: ContinuousAlertTaskExit,
        deadline: Instant,
    ) -> Result<ContinuousAlertTaskExit, ContinuousAlertTaskError> {
        let mut status = self.status();
        status.phase = ContinuousAlertTaskPhase::Stopped;
        status.exit = Some(exit);
        status.failure = None;
        status.runtime_failure = None;
        let recorded_at = Utc::now().max(status.last_recorded_at.unwrap_or_else(Utc::now));
        status.last_recorded_at = Some(recorded_at);
        match tokio::time::timeout_at(
            deadline,
            self.history
                .append(&status_record(&status, "task_stopped", recorded_at)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousAlertTaskFailure::JournalUnavailable,
                );
                return Err(ContinuousAlertTaskError::Journal(error));
            }
            Err(_) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousAlertTaskFailure::TaskCancelled,
                );
                return Err(ContinuousAlertTaskError::TaskCancelled);
            }
        }
        self.status_sender.send_replace(status);
        Ok(exit)
    }

    async fn record_external_failure(
        &mut self,
        failure: ContinuousAlertTaskFailure,
        deadline: Instant,
    ) -> Result<(), ContinuousAlertTaskError> {
        let mut status = self.status();
        status.phase = ContinuousAlertTaskPhase::Failed;
        status.exit = None;
        status.failure = Some(failure);
        status.runtime_failure = None;
        let recorded_at = Utc::now().max(status.last_recorded_at.unwrap_or_else(Utc::now));
        status.last_recorded_at = Some(recorded_at);
        match tokio::time::timeout_at(
            deadline,
            self.history
                .append(&status_record(&status, "task_failed", recorded_at)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousAlertTaskFailure::JournalUnavailable,
                );
                return Err(ContinuousAlertTaskError::Journal(error));
            }
            Err(_) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousAlertTaskFailure::TaskCancelled,
                );
                return Err(ContinuousAlertTaskError::TaskCancelled);
            }
        }
        self.status_sender.send_replace(status);
        Ok(())
    }
}

impl Drop for ContinuousAlertTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type TaskResult = Result<ContinuousAlertTaskExit, ContinuousAlertTaskError>;

async fn run_owner(
    mut runtime: PriceAlertRuntime,
    mut source: MarketSupervisor,
    history: JsonlHistory,
    status_sender: watch::Sender<ContinuousAlertTaskStatus>,
    mut stop: watch::Receiver<bool>,
    mut last_recorded_at: DateTime<Utc>,
) -> TaskResult {
    loop {
        let selected = tokio::select! {
            stop_result = stop.changed() => {
                if stop_result.is_err() || *stop.borrow_and_update() {
                    Selected::Stop
                } else {
                    continue;
                }
            }
            result = source.next_event() => Selected::Source(result.map(|event| event.map(Box::new))),
        };
        match selected {
            Selected::Stop => {
                return stop_owner(
                    &mut runtime,
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousAlertTaskExit::StopRequested,
                )
                .await;
            }
            Selected::Source(Ok(Some(event))) => {
                let event = *event;
                if let Some(terminal) = apply_event(
                    &mut runtime,
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    event,
                )
                .await
                {
                    return terminal;
                }
            }
            Selected::Source(Ok(None)) => {
                return stop_owner(
                    &mut runtime,
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousAlertTaskExit::SourceEnded,
                )
                .await;
            }
            Selected::Source(Err(_)) => {
                return fail_owner(
                    &mut runtime,
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousAlertTaskFailure::SourceContract,
                )
                .await;
            }
        }
    }
}

enum Selected {
    Stop,
    Source(Result<Option<Box<MarketDataEvent>>, MarketSupervisorError>),
}

/// Applies one market event and durably checkpoints the owner status.
///
/// The runtime journals sample/occurrence facts before this owner appends its
/// own lifecycle checkpoint. Returns `Some` when the owner reached a terminal
/// outcome.
async fn apply_event(
    runtime: &mut PriceAlertRuntime,
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousAlertTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    event: MarketDataEvent,
) -> Option<TaskResult> {
    match runtime.process(event).await {
        Ok(_) => {}
        Err(PriceAlertRuntimeError::History(error)) => {
            let _ = runtime.stop().await;
            let _ = source.stop().await;
            publish_runtime_failure(
                status_sender,
                ContinuousAlertTaskFailure::JournalUnavailable,
            );
            return Some(Err(ContinuousAlertTaskError::Journal(error)));
        }
        Err(_) => {
            return Some(
                fail_owner(
                    runtime,
                    source,
                    history,
                    status_sender,
                    last_recorded_at,
                    ContinuousAlertTaskFailure::AlertContract,
                )
                .await,
            );
        }
    }
    let mut next = status_sender.borrow().clone();
    let Some(processed_event_count) = next.processed_event_count.checked_add(1) else {
        return Some(
            fail_owner(
                runtime,
                source,
                history,
                status_sender,
                last_recorded_at,
                ContinuousAlertTaskFailure::AlertContract,
            )
            .await,
        );
    };
    let recorded_at = Utc::now().max(*last_recorded_at);
    next.processed_event_count = processed_event_count;
    next.sources = vec![source.status()];
    next.last_recorded_at = Some(recorded_at);
    next.runtime_failure = None;
    if let Err(error) = history
        .append(&status_record(&next, "task_checkpointed", recorded_at))
        .await
    {
        let _ = runtime.stop().await;
        let _ = source.stop().await;
        publish_runtime_failure(
            status_sender,
            ContinuousAlertTaskFailure::JournalUnavailable,
        );
        return Some(Err(ContinuousAlertTaskError::Journal(error)));
    }
    *last_recorded_at = recorded_at;
    status_sender.send_replace(next);
    None
}

async fn stop_owner(
    runtime: &mut PriceAlertRuntime,
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousAlertTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    requested_exit: ContinuousAlertTaskExit,
) -> TaskResult {
    let stopping_at = Utc::now().max(*last_recorded_at);
    let mut stopping = status_sender.borrow().clone();
    stopping.phase = ContinuousAlertTaskPhase::Stopping;
    stopping.sources = vec![source.status()];
    stopping.last_recorded_at = Some(stopping_at);
    stopping.runtime_failure = None;
    if let Err(error) = history
        .append(&status_record(&stopping, "task_stopping", stopping_at))
        .await
    {
        let _ = runtime.stop().await;
        let _ = source.stop().await;
        publish_runtime_failure(
            status_sender,
            ContinuousAlertTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousAlertTaskError::Journal(error));
    }
    status_sender.send_replace(stopping);
    *last_recorded_at = stopping_at;

    let dispatcher_exit = runtime.stop().await;
    let Ok(source_exit) = source.stop().await else {
        return fail_owner(
            runtime,
            source,
            history,
            status_sender,
            last_recorded_at,
            ContinuousAlertTaskFailure::SourceContract,
        )
        .await;
    };
    let aggregate_exit = aggregate_stop_exit(requested_exit, source_exit, dispatcher_exit);

    let stopped_at = Utc::now().max(*last_recorded_at);
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = ContinuousAlertTaskPhase::Stopped;
    stopped.sources = vec![source.status()];
    stopped.last_recorded_at = Some(stopped_at);
    stopped.exit = Some(aggregate_exit);
    stopped.failure = None;
    stopped.runtime_failure = None;
    if let Err(error) = history
        .append(&status_record(&stopped, "task_stopped", stopped_at))
        .await
    {
        publish_runtime_failure(
            status_sender,
            ContinuousAlertTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousAlertTaskError::Journal(error));
    }
    status_sender.send_replace(stopped);
    Ok(aggregate_exit)
}

async fn fail_owner(
    runtime: &mut PriceAlertRuntime,
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousAlertTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    failure: ContinuousAlertTaskFailure,
) -> TaskResult {
    let _ = runtime.stop().await;
    let _ = source.stop().await;
    let failed_at = Utc::now().max(*last_recorded_at);
    let mut failed = status_sender.borrow().clone();
    failed.phase = ContinuousAlertTaskPhase::Failed;
    failed.sources = vec![source.status()];
    failed.last_recorded_at = Some(failed_at);
    failed.exit = None;
    failed.failure = Some(failure);
    failed.runtime_failure = None;
    if let Err(journal_error) = history
        .append(&status_record(&failed, "task_failed", failed_at))
        .await
    {
        publish_runtime_failure(
            status_sender,
            ContinuousAlertTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousAlertTaskError::Journal(journal_error));
    }
    status_sender.send_replace(failed);
    Err(failure.into_error())
}

const fn aggregate_stop_exit(
    requested: ContinuousAlertTaskExit,
    source: MarketSupervisorExit,
    dispatcher: NotificationDispatcherExit,
) -> ContinuousAlertTaskExit {
    if matches!(source, MarketSupervisorExit::ShutdownTimedOut)
        || matches!(dispatcher, NotificationDispatcherExit::AbortedAfterGrace)
    {
        ContinuousAlertTaskExit::ShutdownTimedOut
    } else {
        requested
    }
}

fn publish_runtime_failure(
    status_sender: &watch::Sender<ContinuousAlertTaskStatus>,
    failure: ContinuousAlertTaskFailure,
) {
    let mut status = status_sender.borrow().clone();
    status.runtime_failure = Some(failure);
    status_sender.send_replace(status);
}

fn registered_record(task_id: &str, source_id: &str, recorded_at: DateTime<Utc>) -> DecisionRecord {
    lifecycle_record(
        task_id,
        "task_registered",
        "registered",
        0,
        Value::Array(vec![placeholder_source_value(source_id)]),
        None,
        None,
        recorded_at,
    )
}

fn status_record(
    status: &ContinuousAlertTaskStatus,
    decision: &'static str,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    lifecycle_record(
        &status.task_id,
        decision,
        task_phase_label(status.phase),
        status.processed_event_count,
        Value::Array(status.sources.iter().map(source_status_value).collect()),
        status.exit.map(task_exit_label),
        status.failure.map(task_failure_label),
        recorded_at,
    )
}

#[allow(clippy::too_many_arguments)]
fn lifecycle_record(
    task_id: &str,
    decision: &'static str,
    phase: &'static str,
    processed_event_count: u64,
    sources: Value,
    exit: Option<&'static str>,
    failure: Option<&'static str>,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    let mut details = json!({
        "schema_version": TASK_RECORD_SCHEMA_VERSION,
        "task_id": task_id,
        "task_kind": "price_alert",
        "phase": phase,
        "processed_event_count": processed_event_count,
        "sources": Value::Null,
        "exit": exit,
        "failure": failure,
    });
    details["sources"] = sources;
    DecisionRecord {
        timestamp: recorded_at,
        strategy: TASK_STRATEGY.to_owned(),
        symbol: TASK_SYMBOL.to_owned(),
        decision: decision.to_owned(),
        details,
    }
}

fn placeholder_source_value(source_id: &str) -> Value {
    json!({
        "schema_version": MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
        "task_id": Value::Null,
        "source_id": source_id,
        "phase": "starting",
        "health": "unknown",
        "event_sequence": 0,
        "dropped_event_count": 0,
        "consecutive_source_failures": 0,
        "last_event_at": Value::Null,
        "exit": Value::Null,
    })
}

fn source_status_value(status: &MarketSupervisorStatus) -> Value {
    json!({
        "schema_version": status.schema_version,
        "task_id": status.task_id,
        "source_id": status.source_id.as_str(),
        "phase": source_phase_label(status.phase),
        "health": source_health_label(status.health),
        "event_sequence": status.event_sequence,
        "dropped_event_count": status.dropped_event_count,
        "consecutive_source_failures": status.consecutive_source_failures,
        "last_event_at": status.last_event_at,
        "exit": status.exit.map(source_exit_label),
    })
}

const fn task_phase_label(phase: ContinuousAlertTaskPhase) -> &'static str {
    match phase {
        ContinuousAlertTaskPhase::Running => "running",
        ContinuousAlertTaskPhase::Stopping => "stopping",
        ContinuousAlertTaskPhase::Stopped => "stopped",
        ContinuousAlertTaskPhase::Failed => "failed",
    }
}

impl fmt::Display for ContinuousAlertTaskPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_phase_label(*self))
    }
}

const fn task_exit_label(exit: ContinuousAlertTaskExit) -> &'static str {
    match exit {
        ContinuousAlertTaskExit::StopRequested => "stop_requested",
        ContinuousAlertTaskExit::SourceEnded => "source_ended",
        ContinuousAlertTaskExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

impl fmt::Display for ContinuousAlertTaskExit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_exit_label(*self))
    }
}

/// Durable buckets stay inside the closed task read-model failure vocabulary:
/// an alert evaluator contract breach is recorded as `invalid_request`.
const fn task_failure_label(failure: ContinuousAlertTaskFailure) -> &'static str {
    match failure {
        ContinuousAlertTaskFailure::StartupFailed => "startup_failed",
        ContinuousAlertTaskFailure::SourceContract => "source_contract",
        ContinuousAlertTaskFailure::AlertContract => "invalid_request",
        ContinuousAlertTaskFailure::JournalUnavailable => "journal_unavailable",
        ContinuousAlertTaskFailure::TaskPanicked => "task_panicked",
        ContinuousAlertTaskFailure::TaskCancelled => "task_cancelled",
    }
}

impl fmt::Display for ContinuousAlertTaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_failure_label(*self))
    }
}

const fn source_phase_label(phase: MarketSupervisorPhase) -> &'static str {
    match phase {
        MarketSupervisorPhase::Starting => "starting",
        MarketSupervisorPhase::Running => "running",
        MarketSupervisorPhase::Stopping => "stopping",
        MarketSupervisorPhase::Stopped => "stopped",
        MarketSupervisorPhase::Failed => "failed",
    }
}

const fn source_health_label(health: MarketSupervisorHealth) -> &'static str {
    match health {
        MarketSupervisorHealth::Unknown => "unknown",
        MarketSupervisorHealth::Healthy => "healthy",
        MarketSupervisorHealth::Degraded => "degraded",
    }
}

const fn source_exit_label(exit: MarketSupervisorExit) -> &'static str {
    match exit {
        MarketSupervisorExit::StopRequested => "stop_requested",
        MarketSupervisorExit::SourceEnded => "source_ended",
        MarketSupervisorExit::ReconnectExhausted => "reconnect_exhausted",
        MarketSupervisorExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

impl ContinuousAlertTaskFailure {
    const fn into_error(self) -> ContinuousAlertTaskError {
        match self {
            Self::StartupFailed => ContinuousAlertTaskError::InvalidConfig,
            Self::SourceContract => ContinuousAlertTaskError::SourceContract,
            Self::AlertContract => ContinuousAlertTaskError::AlertContract,
            Self::JournalUnavailable | Self::TaskCancelled => {
                ContinuousAlertTaskError::TaskCancelled
            }
            Self::TaskPanicked => ContinuousAlertTaskError::TaskPanicked,
        }
    }
}

/// Typed construction/runtime failures. Only journal errors retain local
/// diagnostics; durable records contain bounded failure buckets.
#[derive(Debug)]
pub enum ContinuousAlertTaskError {
    InvalidConfig,
    InvalidSourceBinding,
    Journal(HistoryError),
    SourceContract,
    AlertContract,
    TaskPanicked,
    TaskCancelled,
    PreviouslyFailed(ContinuousAlertTaskFailure),
}

impl ContinuousAlertTaskError {
    const fn failure_bucket(&self) -> ContinuousAlertTaskFailure {
        match self {
            Self::InvalidConfig | Self::InvalidSourceBinding => {
                ContinuousAlertTaskFailure::StartupFailed
            }
            Self::Journal(_) => ContinuousAlertTaskFailure::JournalUnavailable,
            Self::SourceContract => ContinuousAlertTaskFailure::SourceContract,
            Self::AlertContract => ContinuousAlertTaskFailure::AlertContract,
            Self::TaskPanicked => ContinuousAlertTaskFailure::TaskPanicked,
            Self::TaskCancelled => ContinuousAlertTaskFailure::TaskCancelled,
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

impl fmt::Display for ContinuousAlertTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => {
                formatter.write_str("invalid continuous price-alert task config")
            }
            Self::InvalidSourceBinding => formatter
                .write_str("continuous price-alert source does not match its exact binding"),
            Self::Journal(source) => {
                write!(formatter, "continuous price-alert journal failed: {source}")
            }
            Self::SourceContract => {
                formatter.write_str("continuous price-alert source contract failed")
            }
            Self::AlertContract => {
                formatter.write_str("continuous price-alert evaluator contract failed")
            }
            Self::TaskPanicked => formatter.write_str("continuous price-alert task panicked"),
            Self::TaskCancelled => {
                formatter.write_str("continuous price-alert task was cancelled unexpectedly")
            }
            Self::PreviouslyFailed(failure) => {
                write!(
                    formatter,
                    "continuous price-alert task previously failed: {failure:?}"
                )
            }
        }
    }
}

impl std::error::Error for ContinuousAlertTaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(source) => Some(source),
            Self::InvalidConfig
            | Self::InvalidSourceBinding
            | Self::SourceContract
            | Self::AlertContract
            | Self::TaskPanicked
            | Self::TaskCancelled
            | Self::PreviouslyFailed(_) => None,
        }
    }
}

impl TaskHostStatus for ContinuousAlertTaskStatus {
    fn is_terminal(&self) -> bool {
        ContinuousAlertTaskStatus::is_terminal(self)
    }
}

impl TaskHost for ContinuousAlertTask {
    type Status = ContinuousAlertTaskStatus;
    type Exit = ContinuousAlertTaskExit;
    type Error = ContinuousAlertTaskError;

    fn status(&self) -> Self::Status {
        ContinuousAlertTask::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(async move { ContinuousAlertTask::stop(self).await })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ContinuousAlertTaskExit, MarketSupervisorExit, NotificationDispatcherExit,
        aggregate_stop_exit,
    };

    #[test]
    fn child_shutdown_timeout_is_never_downgraded_to_a_normal_task_exit() {
        assert_eq!(
            aggregate_stop_exit(
                ContinuousAlertTaskExit::StopRequested,
                MarketSupervisorExit::ShutdownTimedOut,
                NotificationDispatcherExit::Drained,
            ),
            ContinuousAlertTaskExit::ShutdownTimedOut
        );
        assert_eq!(
            aggregate_stop_exit(
                ContinuousAlertTaskExit::SourceEnded,
                MarketSupervisorExit::SourceEnded,
                NotificationDispatcherExit::AbortedAfterGrace,
            ),
            ContinuousAlertTaskExit::ShutdownTimedOut
        );
    }
}
