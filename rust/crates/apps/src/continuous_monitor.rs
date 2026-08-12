//! Durable, read-only owner for one exact two-source arbitrage monitor.
//!
//! The public seam deliberately exposes only `start`, `status`, and `stop`.
//! Source supervision, merge fairness, journal ordering, and Tokio lifecycle
//! machinery remain private. No exchange execution handle enters this module.

use std::{fmt, time::Duration};

use chrono::{DateTime, Utc};
use crypto_trading_runtime::{
    DecisionRecord, HistoryError, JsonlHistory, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
    MarketDataEvent, MarketDataEventSource, MarketSupervisor, MarketSupervisorConfig,
    MarketSupervisorError, MarketSupervisorExit, MarketSupervisorHealth, MarketSupervisorPhase,
    MarketSupervisorStatus, SpreadHistoryError, SpreadHistoryRecord, SpreadHistoryWriter,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::Instant,
};

use crate::monitor::{
    ArbitrageMonitorError, ArbitrageMonitorEvent, ArbitrageMonitorOutcome,
    ReadOnlyArbitrageMonitor, monitor_symbol,
};
use crate::task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture};

/// Stable schema version for the process-local task status surface.
pub const CONTINUOUS_MONITOR_TASK_STATUS_SCHEMA_VERSION: u16 = 2;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const MAX_TASK_ID_BYTES: usize = 128;
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";

/// Validated configuration for one exact-pair owner task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuousMonitorTaskConfig {
    task_id: String,
    supervisor: MarketSupervisorConfig,
}

impl ContinuousMonitorTaskConfig {
    /// Creates one stable task identity and bounded source shutdown policy.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuousMonitorTaskError::InvalidConfig`] for an empty,
    /// oversized, or transport-unsafe task identity.
    pub fn new(
        task_id: impl Into<String>,
        supervisor: MarketSupervisorConfig,
    ) -> Result<Self, ContinuousMonitorTaskError> {
        let task_id = task_id.into();
        let normalized = task_id.trim();
        if normalized.is_empty()
            || normalized.len() > MAX_TASK_ID_BYTES
            || !normalized.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(ContinuousMonitorTaskError::InvalidConfig);
        }
        Ok(Self {
            task_id: normalized.to_owned(),
            supervisor,
        })
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

/// Aggregate process-local lifecycle phase. Durable readers separately treat
/// every nonterminal phase as unverified after restart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousMonitorTaskPhase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl ContinuousMonitorTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

/// Bounded normal terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousMonitorTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Bounded failure bucket suitable for operator status and durable facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousMonitorTaskFailure {
    StartupFailed,
    SourceContract,
    MonitorContract,
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
pub struct ContinuousMonitorTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: ContinuousMonitorTaskPhase,
    pub processed_event_count: u64,
    pub sources: Vec<MarketSupervisorStatus>,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub exit: Option<ContinuousMonitorTaskExit>,
    pub failure: Option<ContinuousMonitorTaskFailure>,
    pub runtime_failure: Option<ContinuousMonitorTaskFailure>,
}

impl ContinuousMonitorTaskStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Opaque owner of two source supervisors and one monitor loop.
#[derive(Debug)]
pub struct ContinuousMonitorTask {
    stop: watch::Sender<bool>,
    status_sender: watch::Sender<ContinuousMonitorTaskStatus>,
    status: watch::Receiver<ContinuousMonitorTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<ContinuousMonitorTaskExit, ContinuousMonitorTaskFailure>>,
    history: JsonlHistory,
    shutdown_grace: Duration,
}

impl ContinuousMonitorTask {
    /// Durably registers and starts one exact two-source read-only monitor.
    ///
    /// Source identities must be distinct and match the monitor's ordered
    /// exact legs. Registration is synced before source supervisors start;
    /// running status is synced before this method returns.
    ///
    /// # Errors
    ///
    /// Returns a bounded configuration/source error before registration, or a
    /// journal/startup error while establishing the durable lifecycle.
    pub async fn start<L, R>(
        config: ContinuousMonitorTaskConfig,
        monitor: ReadOnlyArbitrageMonitor,
        left_source: L,
        right_source: R,
        history: JsonlHistory,
    ) -> Result<Self, ContinuousMonitorTaskError>
    where
        L: MarketDataEventSource,
        R: MarketDataEventSource,
    {
        Self::start_with_spread_history(config, monitor, left_source, right_source, history, None)
            .await
    }

    /// Like [`Self::start`], but additionally persists one spread-history
    /// record into the dedicated spread-history journal for every spread
    /// observation. A spread-history write failure is treated exactly like a
    /// monitor journal write failure: the owner fails closed.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::start`].
    pub async fn start_with_spread_history<L, R>(
        config: ContinuousMonitorTaskConfig,
        monitor: ReadOnlyArbitrageMonitor,
        left_source: L,
        right_source: R,
        history: JsonlHistory,
        spread_history: Option<SpreadHistoryWriter>,
    ) -> Result<Self, ContinuousMonitorTaskError>
    where
        L: MarketDataEventSource,
        R: MarketDataEventSource,
    {
        let (left_leg, right_leg) = monitor.legs();
        let left_source_id = left_source.source_id().to_owned();
        let right_source_id = right_source.source_id().to_owned();
        if left_source_id == right_source_id
            || left_source_id != left_leg.exchange()
            || right_source_id != right_leg.exchange()
        {
            return Err(ContinuousMonitorTaskError::InvalidSourceBinding);
        }
        let source_ids = [left_source_id, right_source_id];

        let registered_at = Utc::now();
        history
            .append(&registered_record(
                &config.task_id,
                [&source_ids[0], &source_ids[1]],
                registered_at,
            ))
            .await
            .map_err(ContinuousMonitorTaskError::Journal)?;

        let Ok(mut left) = MarketSupervisor::start_new(left_source, config.supervisor) else {
            record_startup_failure(
                &history,
                &config.task_id,
                &source_ids,
                [None, None],
                registered_at,
            )
            .await?;
            return Err(ContinuousMonitorTaskError::SourceContract);
        };
        let Ok(mut right) = MarketSupervisor::start_new(right_source, config.supervisor) else {
            let _ = left.stop().await;
            let left_status = left.status();
            record_startup_failure(
                &history,
                &config.task_id,
                &source_ids,
                [Some(&left_status), None],
                registered_at,
            )
            .await?;
            return Err(ContinuousMonitorTaskError::SourceContract);
        };

        let running_at = Utc::now().max(registered_at);
        let initial = ContinuousMonitorTaskStatus {
            schema_version: CONTINUOUS_MONITOR_TASK_STATUS_SCHEMA_VERSION,
            task_id: config.task_id.clone(),
            phase: ContinuousMonitorTaskPhase::Running,
            processed_event_count: 0,
            sources: source_statuses(&left, &right),
            last_recorded_at: Some(running_at),
            exit: None,
            failure: None,
            runtime_failure: None,
        };
        if let Err(error) = history
            .append(&status_record(&initial, "task_running", running_at))
            .await
        {
            let _ = tokio::join!(left.stop(), right.stop());
            return Err(ContinuousMonitorTaskError::Journal(error));
        }

        let (stop, stop_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial);
        let task_status_sender = status_sender.clone();
        let task_history = history.clone();
        let join = tokio::spawn(async move {
            run_owner(
                monitor,
                left,
                right,
                task_history,
                spread_history,
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
    pub fn status(&self) -> ContinuousMonitorTaskStatus {
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
    pub async fn stop(&mut self) -> Result<ContinuousMonitorTaskExit, ContinuousMonitorTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(ContinuousMonitorTaskError::PreviouslyFailed);
        }
        let _ = self.stop.send(true);
        let Some(mut join) = self.join.take() else {
            return Err(ContinuousMonitorTaskError::TaskCancelled);
        };
        let started_at = Instant::now();
        let owner_deadline = started_at + self.shutdown_grace;
        let shutdown_deadline = started_at + self.shutdown_grace.saturating_mul(2);
        let result = if let Ok(joined) = tokio::time::timeout_at(owner_deadline, &mut join).await {
            self.map_join_result(joined, shutdown_deadline).await
        } else {
            join.abort();
            self.record_forced_exit(
                ContinuousMonitorTaskExit::ShutdownTimedOut,
                shutdown_deadline,
            )
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
    ) -> Result<ContinuousMonitorTaskExit, ContinuousMonitorTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) if error.is_panic() => {
                self.record_external_failure(ContinuousMonitorTaskFailure::TaskPanicked, deadline)
                    .await?;
                Err(ContinuousMonitorTaskError::TaskPanicked)
            }
            Err(_) => {
                self.record_external_failure(ContinuousMonitorTaskFailure::TaskCancelled, deadline)
                    .await?;
                Err(ContinuousMonitorTaskError::TaskCancelled)
            }
        }
    }

    async fn record_forced_exit(
        &mut self,
        exit: ContinuousMonitorTaskExit,
        deadline: Instant,
    ) -> Result<ContinuousMonitorTaskExit, ContinuousMonitorTaskError> {
        let mut status = self.status();
        status.phase = ContinuousMonitorTaskPhase::Stopped;
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
                    ContinuousMonitorTaskFailure::JournalUnavailable,
                );
                return Err(ContinuousMonitorTaskError::Journal(error));
            }
            Err(_) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousMonitorTaskFailure::TaskCancelled,
                );
                return Err(ContinuousMonitorTaskError::TaskCancelled);
            }
        }
        self.status_sender.send_replace(status);
        Ok(exit)
    }

    async fn record_external_failure(
        &mut self,
        failure: ContinuousMonitorTaskFailure,
        deadline: Instant,
    ) -> Result<(), ContinuousMonitorTaskError> {
        let mut status = self.status();
        status.phase = ContinuousMonitorTaskPhase::Failed;
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
                    ContinuousMonitorTaskFailure::JournalUnavailable,
                );
                return Err(ContinuousMonitorTaskError::Journal(error));
            }
            Err(_) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousMonitorTaskFailure::TaskCancelled,
                );
                return Err(ContinuousMonitorTaskError::TaskCancelled);
            }
        }
        self.status_sender.send_replace(status);
        Ok(())
    }
}

impl Drop for ContinuousMonitorTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type TaskResult = Result<ContinuousMonitorTaskExit, ContinuousMonitorTaskError>;

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_owner(
    mut monitor: ReadOnlyArbitrageMonitor,
    mut left: MarketSupervisor,
    mut right: MarketSupervisor,
    history: JsonlHistory,
    spread_history: Option<SpreadHistoryWriter>,
    status_sender: watch::Sender<ContinuousMonitorTaskStatus>,
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
            result = left.next_event() => Selected::Source(result.map(|event| event.map(Box::new))),
            result = right.next_event() => Selected::Source(result.map(|event| event.map(Box::new))),
        };
        match selected {
            Selected::Stop => {
                return stop_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousMonitorTaskExit::StopRequested,
                )
                .await;
            }
            Selected::Source(Ok(Some(event))) => {
                let event = *event;
                let Ok(monitor_event) = monitor.process(event) else {
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        ContinuousMonitorTaskFailure::MonitorContract,
                        ContinuousMonitorTaskError::MonitorContract,
                    )
                    .await;
                };
                let mut next = status_sender.borrow().clone();
                let Some(processed_event_count) = next.processed_event_count.checked_add(1) else {
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        ContinuousMonitorTaskFailure::MonitorContract,
                        ContinuousMonitorTaskError::MonitorContract,
                    )
                    .await;
                };
                let recorded_at = Utc::now().max(last_recorded_at);
                next.processed_event_count = processed_event_count;
                next.sources = source_statuses(&left, &right);
                next.last_recorded_at = Some(recorded_at);
                next.runtime_failure = None;
                let Ok(spread_record) = spread_history_record(&monitor_event) else {
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        ContinuousMonitorTaskFailure::MonitorContract,
                        ContinuousMonitorTaskError::MonitorContract,
                    )
                    .await;
                };
                let records = [
                    monitor_event.to_record(),
                    status_record(&next, "task_checkpointed", recorded_at),
                ];
                if let Err(error) = history.append_batch(&records).await {
                    let _ = tokio::join!(left.stop(), right.stop());
                    publish_runtime_failure(
                        &status_sender,
                        ContinuousMonitorTaskFailure::JournalUnavailable,
                    );
                    return Err(ContinuousMonitorTaskError::Journal(error));
                }
                if let (Some(writer), Some(record)) = (&spread_history, &spread_record)
                    && let Err(error) = writer.append(record).await
                {
                    // A spread-history write failure shares the monitor
                    // journal's fail-closed semantics: no degraded recording.
                    let _ = tokio::join!(left.stop(), right.stop());
                    publish_runtime_failure(
                        &status_sender,
                        ContinuousMonitorTaskFailure::JournalUnavailable,
                    );
                    return Err(ContinuousMonitorTaskError::SpreadHistory(error));
                }
                last_recorded_at = recorded_at;
                status_sender.send_replace(next);
            }
            Selected::Source(Ok(None)) => {
                return stop_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousMonitorTaskExit::SourceEnded,
                )
                .await;
            }
            Selected::Source(Err(_)) => {
                return fail_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousMonitorTaskFailure::SourceContract,
                    ContinuousMonitorTaskError::SourceContract,
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

async fn stop_owner(
    left: &mut MarketSupervisor,
    right: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousMonitorTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    exit: ContinuousMonitorTaskExit,
) -> TaskResult {
    let stopping_at = Utc::now().max(*last_recorded_at);
    let mut stopping = status_sender.borrow().clone();
    stopping.phase = ContinuousMonitorTaskPhase::Stopping;
    stopping.sources = source_statuses(left, right);
    stopping.last_recorded_at = Some(stopping_at);
    stopping.runtime_failure = None;
    if let Err(error) = history
        .append(&status_record(&stopping, "task_stopping", stopping_at))
        .await
    {
        let _ = tokio::join!(left.stop(), right.stop());
        publish_runtime_failure(
            status_sender,
            ContinuousMonitorTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousMonitorTaskError::Journal(error));
    }
    status_sender.send_replace(stopping);
    *last_recorded_at = stopping_at;

    let (left_exit, right_exit) = tokio::join!(left.stop(), right.stop());
    let (Ok(left_exit), Ok(right_exit)) = (left_exit, right_exit) else {
        return fail_owner(
            left,
            right,
            history,
            status_sender,
            last_recorded_at,
            ContinuousMonitorTaskFailure::SourceContract,
            ContinuousMonitorTaskError::SourceContract,
        )
        .await;
    };
    let aggregate_exit = aggregate_stop_exit(exit, left_exit, right_exit);

    let stopped_at = Utc::now().max(*last_recorded_at);
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = ContinuousMonitorTaskPhase::Stopped;
    stopped.sources = source_statuses(left, right);
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
            ContinuousMonitorTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousMonitorTaskError::Journal(error));
    }
    status_sender.send_replace(stopped);
    Ok(aggregate_exit)
}

async fn fail_owner(
    left: &mut MarketSupervisor,
    right: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousMonitorTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    failure: ContinuousMonitorTaskFailure,
    error: ContinuousMonitorTaskError,
) -> TaskResult {
    let _ = tokio::join!(left.stop(), right.stop());
    let failed_at = Utc::now().max(*last_recorded_at);
    let mut failed = status_sender.borrow().clone();
    failed.phase = ContinuousMonitorTaskPhase::Failed;
    failed.sources = source_statuses(left, right);
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
            ContinuousMonitorTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousMonitorTaskError::Journal(journal_error));
    }
    status_sender.send_replace(failed);
    Err(error)
}

async fn record_startup_failure(
    history: &JsonlHistory,
    task_id: &str,
    source_ids: &[String; 2],
    statuses: [Option<&MarketSupervisorStatus>; 2],
    registered_at: DateTime<Utc>,
) -> Result<(), ContinuousMonitorTaskError> {
    let failed_at = Utc::now().max(registered_at);
    let sources = source_ids
        .iter()
        .zip(statuses)
        .map(|(source_id, status)| {
            status.map_or_else(|| placeholder_source_value(source_id), source_status_value)
        })
        .collect();
    history
        .append(&lifecycle_record(
            task_id,
            "task_failed",
            "failed",
            0,
            Value::Array(sources),
            None,
            Some("startup_failed"),
            failed_at,
        ))
        .await
        .map_err(ContinuousMonitorTaskError::Journal)
}

fn source_statuses(
    left: &MarketSupervisor,
    right: &MarketSupervisor,
) -> Vec<MarketSupervisorStatus> {
    vec![left.status(), right.status()]
}

const fn aggregate_stop_exit(
    requested: ContinuousMonitorTaskExit,
    left: MarketSupervisorExit,
    right: MarketSupervisorExit,
) -> ContinuousMonitorTaskExit {
    if matches!(left, MarketSupervisorExit::ShutdownTimedOut)
        || matches!(right, MarketSupervisorExit::ShutdownTimedOut)
    {
        ContinuousMonitorTaskExit::ShutdownTimedOut
    } else {
        requested
    }
}

/// Projects one spread-bearing monitor outcome into the versioned
/// spread-history record. Waiting and rejected outcomes carry no spread and
/// yield `Ok(None)`. Funding-rate fields stay `None` because no wired
/// market-data source publishes funding rates yet.
fn spread_history_record(event: &ArbitrageMonitorEvent) -> Result<Option<SpreadHistoryRecord>, ()> {
    let (buy_exchange, sell_exchange, buy_price, sell_price, spread_percent) = match &event.outcome
    {
        ArbitrageMonitorOutcome::NoOpportunity {
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            spread_percent,
            ..
        }
        | ArbitrageMonitorOutcome::Opportunity {
            buy_exchange,
            sell_exchange,
            buy_price,
            sell_price,
            spread_percent,
            ..
        } => (
            buy_exchange,
            sell_exchange,
            *buy_price,
            *sell_price,
            *spread_percent,
        ),
        ArbitrageMonitorOutcome::Waiting { .. }
        | ArbitrageMonitorOutcome::AnalysisRejected { .. } => {
            return Ok(None);
        }
    };
    let spread_bps = spread_percent.checked_mul(Decimal::ONE_HUNDRED).ok_or(())?;
    Ok(Some(SpreadHistoryRecord {
        timestamp: event.recorded_at,
        symbol: monitor_symbol(&event.left, &event.right),
        exchange_buy: buy_exchange.clone(),
        exchange_sell: sell_exchange.clone(),
        price_buy: buy_price.as_decimal().to_string(),
        price_sell: sell_price.as_decimal().to_string(),
        spread_bps: spread_bps.to_string(),
        funding_rate_buy: None,
        funding_rate_sell: None,
        funding_rate_diff: None,
        funding_rate_diff_annual_pct: None,
    }))
}

fn publish_runtime_failure(
    status_sender: &watch::Sender<ContinuousMonitorTaskStatus>,
    failure: ContinuousMonitorTaskFailure,
) {
    let mut status = status_sender.borrow().clone();
    status.runtime_failure = Some(failure);
    status_sender.send_replace(status);
}

fn registered_record(
    task_id: &str,
    source_ids: [&str; 2],
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    let sources = source_ids
        .map(placeholder_source_value)
        .into_iter()
        .collect();
    lifecycle_record(
        task_id,
        "task_registered",
        "registered",
        0,
        Value::Array(sources),
        None,
        None,
        recorded_at,
    )
}

fn status_record(
    status: &ContinuousMonitorTaskStatus,
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
        "task_kind": "arbitrage_monitor",
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

const fn task_phase_label(phase: ContinuousMonitorTaskPhase) -> &'static str {
    match phase {
        ContinuousMonitorTaskPhase::Running => "running",
        ContinuousMonitorTaskPhase::Stopping => "stopping",
        ContinuousMonitorTaskPhase::Stopped => "stopped",
        ContinuousMonitorTaskPhase::Failed => "failed",
    }
}

impl fmt::Display for ContinuousMonitorTaskPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_phase_label(*self))
    }
}

const fn task_exit_label(exit: ContinuousMonitorTaskExit) -> &'static str {
    match exit {
        ContinuousMonitorTaskExit::StopRequested => "stop_requested",
        ContinuousMonitorTaskExit::SourceEnded => "source_ended",
        ContinuousMonitorTaskExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

impl fmt::Display for ContinuousMonitorTaskExit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_exit_label(*self))
    }
}

const fn task_failure_label(failure: ContinuousMonitorTaskFailure) -> &'static str {
    match failure {
        ContinuousMonitorTaskFailure::StartupFailed => "startup_failed",
        ContinuousMonitorTaskFailure::SourceContract => "source_contract",
        ContinuousMonitorTaskFailure::MonitorContract => "monitor_contract",
        ContinuousMonitorTaskFailure::JournalUnavailable => "journal_unavailable",
        ContinuousMonitorTaskFailure::TaskPanicked => "task_panicked",
        ContinuousMonitorTaskFailure::TaskCancelled => "task_cancelled",
    }
}

impl fmt::Display for ContinuousMonitorTaskFailure {
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
        MarketSupervisorExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

/// Typed construction/runtime failures. Only journal errors retain local
/// diagnostics; durable records contain bounded failure buckets.
#[derive(Debug)]
pub enum ContinuousMonitorTaskError {
    InvalidConfig,
    InvalidSourceBinding,
    Journal(HistoryError),
    SpreadHistory(SpreadHistoryError),
    SourceContract,
    MonitorContract,
    TaskPanicked,
    TaskCancelled,
    PreviouslyFailed(ContinuousMonitorTaskFailure),
}

impl ContinuousMonitorTaskError {
    const fn failure_bucket(&self) -> ContinuousMonitorTaskFailure {
        match self {
            Self::InvalidConfig | Self::InvalidSourceBinding => {
                ContinuousMonitorTaskFailure::StartupFailed
            }
            Self::Journal(_) | Self::SpreadHistory(_) => {
                ContinuousMonitorTaskFailure::JournalUnavailable
            }
            Self::SourceContract => ContinuousMonitorTaskFailure::SourceContract,
            Self::MonitorContract => ContinuousMonitorTaskFailure::MonitorContract,
            Self::TaskPanicked => ContinuousMonitorTaskFailure::TaskPanicked,
            Self::TaskCancelled => ContinuousMonitorTaskFailure::TaskCancelled,
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

impl fmt::Display for ContinuousMonitorTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid continuous monitor task config"),
            Self::InvalidSourceBinding => {
                formatter.write_str("continuous monitor sources do not match the exact pair")
            }
            Self::Journal(source) => {
                write!(formatter, "continuous monitor journal failed: {source}")
            }
            Self::SpreadHistory(source) => {
                write!(
                    formatter,
                    "continuous monitor spread-history journal failed: {source}"
                )
            }
            Self::SourceContract => {
                formatter.write_str("continuous monitor source contract failed")
            }
            Self::MonitorContract => {
                formatter.write_str("continuous monitor analysis contract failed")
            }
            Self::TaskPanicked => formatter.write_str("continuous monitor task panicked"),
            Self::TaskCancelled => {
                formatter.write_str("continuous monitor task was cancelled unexpectedly")
            }
            Self::PreviouslyFailed(failure) => {
                write!(
                    formatter,
                    "continuous monitor task previously failed: {failure:?}"
                )
            }
        }
    }
}

impl std::error::Error for ContinuousMonitorTaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(source) => Some(source),
            Self::SpreadHistory(source) => Some(source),
            Self::InvalidConfig
            | Self::InvalidSourceBinding
            | Self::SourceContract
            | Self::MonitorContract
            | Self::TaskPanicked
            | Self::TaskCancelled
            | Self::PreviouslyFailed(_) => None,
        }
    }
}

impl From<ArbitrageMonitorError> for ContinuousMonitorTaskError {
    fn from(_: ArbitrageMonitorError) -> Self {
        Self::MonitorContract
    }
}

impl TaskHostStatus for ContinuousMonitorTaskStatus {
    fn is_terminal(&self) -> bool {
        ContinuousMonitorTaskStatus::is_terminal(self)
    }
}

impl TaskHost for ContinuousMonitorTask {
    type Status = ContinuousMonitorTaskStatus;
    type Exit = ContinuousMonitorTaskExit;
    type Error = ContinuousMonitorTaskError;

    fn status(&self) -> Self::Status {
        ContinuousMonitorTask::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(async move { ContinuousMonitorTask::stop(self).await })
    }
}

#[cfg(test)]
mod tests {
    use super::{ContinuousMonitorTaskExit, MarketSupervisorExit, aggregate_stop_exit};

    #[test]
    fn child_shutdown_timeout_is_never_downgraded_to_a_normal_task_exit() {
        assert_eq!(
            aggregate_stop_exit(
                ContinuousMonitorTaskExit::StopRequested,
                MarketSupervisorExit::StopRequested,
                MarketSupervisorExit::ShutdownTimedOut,
            ),
            ContinuousMonitorTaskExit::ShutdownTimedOut
        );
        assert_eq!(
            aggregate_stop_exit(
                ContinuousMonitorTaskExit::SourceEnded,
                MarketSupervisorExit::ShutdownTimedOut,
                MarketSupervisorExit::SourceEnded,
            ),
            ContinuousMonitorTaskExit::ShutdownTimedOut
        );
    }
}
