//! Durable continuous owner for one exact-pair paper arbitrage strategy.
//!
//! Market events remain the only execution trigger. The owner admits at most
//! one two-leg saga at a time and coalesces every additional opportunity into
//! one bounded "evaluate the latest pair again" signal. Its stable lifecycle
//! identity is never reused as an account reservation identity: operations use
//! `owner/op/NNNNNN`.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    future::Future,
    io::ErrorKind,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use crypto_trading_config::ArbitrageConfig;
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, OrderType, Position, PositionSide, Quantity,
    Side, Symbol,
};
use crypto_trading_exchange::TradingReceipt;
use crypto_trading_runtime::{
    DecisionRecord, ExecutionBatch, FileJournalSnapshotSource, HistoryError, JournalReadError,
    JournalSnapshot, JournalSnapshotSource, JsonlHistory, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
    MarketDataError, MarketDataEvent, MarketDataEventSource, MarketSupervisor,
    MarketSupervisorConfig, MarketSupervisorError, MarketSupervisorExit, MarketSupervisorHealth,
    MarketSupervisorPhase, MarketSupervisorStatus, ObservedMarketPair, PaperAccountAuthority,
    PaperAccountError, PaperAccountSnapshot, PaperCostModel, PaperReconciliationOutcome,
    PaperReservationLeg, PaperReservationPhase, PaperReservationRequest, ProjectionStatus,
    ReadModelError, ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel,
    ReadOnlyTaskRecovery, ReadOnlyTaskView, RuntimeError,
};
use crypto_trading_strategy::{
    AccountRiskSnapshot, ArbitrageDecision, ArbitrageState, ArbitrageStrategy, PairStrategyMachine,
    RiskDecision, RiskEngine, RiskLimits, RiskRejection, StrategyError,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use crate::{
    DurablePaperArbitrageSaga, PaperArbitrageRequest, PaperArbitrageRun, PaperArbitrageSagaError,
    monitor::{ArbitrageMonitorError, ArbitrageMonitorOutcome, ReadOnlyArbitrageMonitor},
    task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture},
};

/// Stable version of the process-local arbitrage owner status.
pub const ARBITRAGE_PAPER_TASK_STATUS_SCHEMA_VERSION: u16 = 1;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";
const MAX_TASK_ID_BYTES: usize = 96;
const OPERATION_SUFFIX_BYTES: usize = "/op/00000000000000000000".len();

/// Boxed execution future behind the trusted paper adapter seam.
pub type ArbitragePaperExecutionFuture =
    Pin<Box<dyn Future<Output = Result<Vec<TradingReceipt>, RuntimeError>> + Send + 'static>>;

/// Minimal object-safe two-leg execution seam owned by the task process.
pub trait ArbitragePaperExecutor: Send + Sync + 'static {
    fn execute(&self, batch: ExecutionBatch) -> ArbitragePaperExecutionFuture;
}

/// Validated execution, risk, lifecycle, and reservation policy.
#[derive(Clone, Debug)]
pub struct ArbitragePaperTaskConfig {
    task_id: String,
    strategy: ArbitrageStrategy,
    risk: RiskEngine,
    cost_model: PaperCostModel,
    supervisor: MarketSupervisorConfig,
}

impl ArbitragePaperTaskConfig {
    /// Creates one operator-scoped continuous arbitrage owner configuration.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration or strategy failure when execution is not
    /// explicitly enabled, risk is unbounded, or the stable identity is unsafe.
    pub fn new(
        task_id: impl Into<String>,
        arbitrage: &ArbitrageConfig,
        max_snapshot_age: chrono::Duration,
        cost_model: PaperCostModel,
        supervisor: MarketSupervisorConfig,
    ) -> Result<Self, ArbitragePaperTaskError> {
        let task_id = task_id.into();
        let task_id = task_id.trim();
        if task_id.is_empty()
            || task_id.len() > MAX_TASK_ID_BYTES
            || task_id.len().saturating_add(OPERATION_SUFFIX_BYTES) > 128
            || !safe_identity(task_id)
        {
            return Err(ArbitragePaperTaskError::InvalidConfig);
        }
        let strategy =
            ArbitrageStrategy::try_from(arbitrage).map_err(ArbitragePaperTaskError::Strategy)?;
        let max_position_value = arbitrage
            .max_position_value
            .ok_or(ArbitragePaperTaskError::InvalidConfig)?;
        let risk = RiskEngine::new(RiskLimits {
            max_position_value,
            max_snapshot_age,
        })
        .map_err(ArbitragePaperTaskError::Strategy)?;
        Ok(Self {
            task_id: task_id.to_owned(),
            strategy,
            risk,
            cost_model,
            supervisor,
        })
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

/// Durable aggregate lifecycle phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArbitragePaperTaskPhase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl ArbitragePaperTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

/// Bounded normal terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArbitragePaperTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Bounded task failure suitable for the durable task projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArbitragePaperTaskFailure {
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

/// Latest durable lifecycle status plus bounded process-local owner counters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArbitragePaperTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: ArbitragePaperTaskPhase,
    pub processed_event_count: u64,
    pub operation_count: u64,
    pub coalesced_opportunity_count: u64,
    pub sources: Vec<MarketSupervisorStatus>,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub exit: Option<ArbitragePaperTaskExit>,
    pub failure: Option<ArbitragePaperTaskFailure>,
    pub runtime_failure: Option<ArbitragePaperTaskFailure>,
}

impl TaskHostStatus for ArbitragePaperTaskStatus {
    fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Opaque owner of one exact pair, two supervisors, and one in-flight saga.
#[derive(Debug)]
pub struct ArbitragePaperTask {
    stop: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    status_sender: watch::Sender<ArbitragePaperTaskStatus>,
    status: watch::Receiver<ArbitragePaperTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<ArbitragePaperTaskExit, ArbitragePaperTaskFailure>>,
    account: PaperAccountAuthority,
    history: JsonlHistory,
    shutdown_grace: Duration,
}

impl ArbitragePaperTask {
    /// Starts one durable exact-pair event-driven paper owner.
    ///
    /// Recovery preflight happens before registration. Degraded projections,
    /// failed reconciliation, pending/uncertain/committed owner exposure, and
    /// a previous nonterminal owner all fail closed.
    ///
    /// # Errors
    ///
    /// Returns a typed source, configuration, recovery, account, projection,
    /// strategy, or journal failure.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn start<L, R>(
        config: ArbitragePaperTaskConfig,
        monitor: ReadOnlyArbitrageMonitor,
        left_source: L,
        right_source: R,
        account: PaperAccountAuthority,
        history: JsonlHistory,
        executor: Arc<dyn ArbitragePaperExecutor>,
    ) -> Result<Self, ArbitragePaperTaskError>
    where
        L: MarketDataEventSource,
        R: MarketDataEventSource,
    {
        let (left_leg, right_leg) = monitor.legs();
        let left_source_id = left_source.source_id().to_owned();
        let right_source_id = right_source.source_id().to_owned();
        if account.history_path() != history.path()
            || left_leg.symbol != right_leg.symbol
            || left_source_id == right_source_id
            || left_source_id != left_leg.exchange()
            || right_source_id != right_leg.exchange()
        {
            return Err(ArbitragePaperTaskError::InvalidSourceBinding);
        }
        let source_ids = [left_source_id, right_source_id];
        let operation_sequence =
            recovery_preflight(&config.task_id, &source_ids, &account, &history).await?;

        let registered_at = Utc::now();
        history
            .append(&registered_record(
                &config.task_id,
                [&source_ids[0], &source_ids[1]],
                registered_at,
            ))
            .await
            .map_err(ArbitragePaperTaskError::Journal)?;

        let Ok(mut left) = MarketSupervisor::start_new(left_source, config.supervisor) else {
            record_startup_failure(
                &history,
                &config.task_id,
                &source_ids,
                [None, None],
                registered_at,
            )
            .await?;
            return Err(ArbitragePaperTaskError::SourceContract);
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
            return Err(ArbitragePaperTaskError::SourceContract);
        };

        tokio::task::yield_now().await;
        let running_at = Utc::now().max(registered_at);
        let initial = ArbitragePaperTaskStatus {
            schema_version: ARBITRAGE_PAPER_TASK_STATUS_SCHEMA_VERSION,
            task_id: config.task_id.clone(),
            phase: ArbitragePaperTaskPhase::Running,
            processed_event_count: 0,
            operation_count: operation_sequence,
            coalesced_opportunity_count: 0,
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
            return Err(ArbitragePaperTaskError::Journal(error));
        }

        let saga = DurablePaperArbitrageSaga::new(account.clone(), history.clone())
            .map_err(ArbitragePaperTaskError::Saga)?;
        let (stop, stop_receiver) = watch::channel(false);
        let (cancel, cancel_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial);
        let task_status = status_sender.clone();
        let task_history = history.clone();
        let task_config = config.clone();
        let join = tokio::spawn(async move {
            run_owner(
                task_config,
                monitor,
                left,
                right,
                saga,
                executor,
                task_history,
                task_status,
                stop_receiver,
                cancel_receiver,
                running_at,
                operation_sequence,
            )
            .await
        });

        Ok(Self {
            stop,
            cancel,
            status_sender,
            status,
            join: Some(join),
            completion: None,
            account,
            history,
            shutdown_grace: config.supervisor.shutdown_grace(),
        })
    }

    /// Returns the latest lifecycle status and process-local owner counters.
    #[must_use]
    pub fn status(&self) -> ArbitragePaperTaskStatus {
        self.status.borrow().clone()
    }

    /// Reprojects the stable owner lifecycle from its journal.
    ///
    /// # Errors
    ///
    /// Returns snapshot, read-model, or recovery failures and never substitutes
    /// process-local state.
    pub async fn durable_status(&self) -> Result<ReadOnlyTaskView, ArbitragePaperTaskError> {
        durable_task_view(&self.account, self.history.path(), &self.status().task_id)
            .await?
            .ok_or(ArbitragePaperTaskError::RecoveryRequired)
    }

    /// Waits for a finite source to terminate without requesting a stop.
    ///
    /// # Errors
    ///
    /// Returns the owner result or a typed join failure.
    pub async fn wait(&mut self) -> Result<ArbitragePaperTaskExit, ArbitragePaperTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(ArbitragePaperTaskError::PreviouslyFailed);
        }
        let Some(join) = self.join.take() else {
            return Err(ArbitragePaperTaskError::TaskCancelled);
        };
        let result = Self::map_join(join.await);
        self.store_completion(&result);
        result
    }

    /// Stops admitting reservations, drains the current saga to a durable
    /// terminal fact, and then stops the owner.
    ///
    /// # Errors
    ///
    /// Returns a typed execution, journal, recovery, or shutdown failure.
    pub async fn stop(&mut self) -> Result<ArbitragePaperTaskExit, ArbitragePaperTaskError> {
        self.finish_with_signal(false).await
    }

    /// Cancels an in-flight saga without releasing unknown exposure.
    ///
    /// A pending reservation is retained as uncertain. A cancel with no
    /// in-flight operation is equivalent to a normal stop.
    ///
    /// # Errors
    ///
    /// Returns a typed recovery or lifecycle failure.
    pub async fn cancel(&mut self) -> Result<ArbitragePaperTaskExit, ArbitragePaperTaskError> {
        self.finish_with_signal(true).await
    }

    async fn finish_with_signal(
        &mut self,
        cancel: bool,
    ) -> Result<ArbitragePaperTaskExit, ArbitragePaperTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(ArbitragePaperTaskError::PreviouslyFailed);
        }
        if cancel {
            let _ = self.cancel.send(true);
        } else {
            let _ = self.stop.send(true);
        }
        let Some(mut join) = self.join.take() else {
            return Err(ArbitragePaperTaskError::TaskCancelled);
        };
        let deadline = self.shutdown_grace.saturating_mul(2);
        let result = if let Ok(joined) = tokio::time::timeout(deadline, &mut join).await {
            Self::map_join(joined)
        } else {
            join.abort();
            let _ = join.await;
            self.retain_active_capacity().await;
            self.record_external_failure(ArbitragePaperTaskFailure::RecoveryRequired)
                .await?;
            Err(ArbitragePaperTaskError::ShutdownTimedOut)
        };
        self.store_completion(&result);
        result
    }

    fn map_join(
        joined: Result<TaskResult, JoinError>,
    ) -> Result<ArbitragePaperTaskExit, ArbitragePaperTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(ArbitragePaperTaskError::TaskPanicked),
            Err(_) => Err(ArbitragePaperTaskError::TaskCancelled),
        }
    }

    fn store_completion(
        &mut self,
        result: &Result<ArbitragePaperTaskExit, ArbitragePaperTaskError>,
    ) {
        self.completion = Some(match result {
            Ok(exit) => Ok(*exit),
            Err(error) => Err(error.failure_bucket()),
        });
    }

    async fn retain_active_capacity(&self) {
        let Ok(snapshot) = self.account.snapshot().await else {
            return;
        };
        let prefix = operation_prefix(&self.status().task_id);
        for reservation in snapshot.reservations.iter().filter(|reservation| {
            reservation.task_id.starts_with(&prefix)
                && reservation.phase == PaperReservationPhase::Pending
        }) {
            let _ = self
                .account
                .mark_uncertain(reservation.reservation_id)
                .await;
        }
    }

    async fn record_external_failure(
        &mut self,
        failure: ArbitragePaperTaskFailure,
    ) -> Result<(), ArbitragePaperTaskError> {
        let mut status = self.status();
        status.phase = ArbitragePaperTaskPhase::Failed;
        status.failure = Some(failure);
        status.exit = None;
        status.runtime_failure = None;
        let recorded_at = Utc::now().max(status.last_recorded_at.unwrap_or_else(Utc::now));
        status.last_recorded_at = Some(recorded_at);
        self.history
            .append(&status_record(&status, "task_failed", recorded_at))
            .await
            .map_err(ArbitragePaperTaskError::Journal)?;
        self.status_sender.send_replace(status);
        Ok(())
    }
}

impl TaskHost for ArbitragePaperTask {
    type Status = ArbitragePaperTaskStatus;
    type Exit = ArbitragePaperTaskExit;
    type Error = ArbitragePaperTaskError;

    fn status(&self) -> Self::Status {
        Self::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(Self::stop(self))
    }
}

impl Drop for ArbitragePaperTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type TaskResult = Result<ArbitragePaperTaskExit, ArbitragePaperTaskError>;
type OperationJoinResult = Result<Result<PaperArbitrageRun, PaperArbitrageSagaError>, JoinError>;

#[derive(Debug)]
struct InFlightOperation {
    request: PaperArbitrageRequest,
    decision: ArbitrageDecision,
    execution_started: Arc<AtomicBool>,
    join: Option<JoinHandle<Result<PaperArbitrageRun, PaperArbitrageSagaError>>>,
}

impl InFlightOperation {
    fn join_mut(&mut self) -> &mut JoinHandle<Result<PaperArbitrageRun, PaperArbitrageSagaError>> {
        self.join
            .as_mut()
            .expect("an in-flight arbitrage operation always owns its join handle")
    }

    async fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
            let _ = join.await;
        }
    }

    fn execution_started(&self) -> bool {
        self.execution_started.load(Ordering::Acquire)
    }
}

impl Drop for InFlightOperation {
    fn drop(&mut self) {
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

#[derive(Debug)]
struct PlannedOperation {
    request: PaperArbitrageRequest,
    decision: ArbitrageDecision,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_owner(
    config: ArbitragePaperTaskConfig,
    mut monitor: ReadOnlyArbitrageMonitor,
    mut left: MarketSupervisor,
    mut right: MarketSupervisor,
    saga: DurablePaperArbitrageSaga,
    executor: Arc<dyn ArbitragePaperExecutor>,
    history: JsonlHistory,
    status_sender: watch::Sender<ArbitragePaperTaskStatus>,
    mut stop: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    mut last_recorded_at: DateTime<Utc>,
    mut operation_sequence: u64,
) -> TaskResult {
    let mut state = ArbitrageState::default();
    let mut in_flight: Option<InFlightOperation> = None;
    let mut pending_opportunity = false;

    loop {
        let selected = if let Some(operation) = in_flight.as_mut() {
            tokio::select! {
                biased;
                cancel_result = cancel.changed() => {
                    if cancel_result.is_err() || *cancel.borrow_and_update() {
                        Selected::Cancel
                    } else {
                        continue;
                    }
                }
                stop_result = stop.changed() => {
                    if stop_result.is_err() || *stop.borrow_and_update() {
                        Selected::Stop
                    } else {
                        continue;
                    }
                }
                result = operation.join_mut() => Selected::Operation(result),
                result = left.next_event() => Selected::Left(result),
                result = right.next_event() => Selected::Right(result),
            }
        } else {
            tokio::select! {
                biased;
                cancel_result = cancel.changed() => {
                    if cancel_result.is_err() || *cancel.borrow_and_update() {
                        Selected::Cancel
                    } else {
                        continue;
                    }
                }
                stop_result = stop.changed() => {
                    if stop_result.is_err() || *stop.borrow_and_update() {
                        Selected::Stop
                    } else {
                        continue;
                    }
                }
                result = left.next_event() => Selected::Left(result),
                result = right.next_event() => Selected::Right(result),
            }
        };

        match selected {
            Selected::Stop | Selected::Cancel => {
                let cancel_requested = matches!(selected, Selected::Cancel);
                return stop_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ArbitragePaperTaskExit::StopRequested,
                    in_flight.take(),
                    cancel_requested,
                    saga.account(),
                    &mut state,
                )
                .await;
            }
            Selected::Operation(result) => {
                let operation = in_flight
                    .take()
                    .ok_or(ArbitragePaperTaskError::TaskCancelled)?;
                if let Err(error) = complete_operation(result, &operation.decision, &mut state) {
                    let (failure, error) = classify_operation_error(error);
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        failure,
                        error,
                    )
                    .await;
                }
                if pending_opportunity && !*stop.borrow() && !*cancel.borrow() {
                    pending_opportunity = false;
                    match plan_latest_operation(
                        &config,
                        &monitor,
                        saga.account(),
                        &state,
                        &mut operation_sequence,
                    )
                    .await
                    {
                        Ok(Some(planned)) if !*stop.borrow() && !*cancel.borrow() => {
                            in_flight =
                                Some(start_operation(&saga, Arc::clone(&executor), planned));
                            publish_operation_count(&status_sender, operation_sequence);
                        }
                        Ok(Some(_) | None) => {}
                        Err(error) => {
                            let failure = error.failure_bucket();
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                error,
                            )
                            .await;
                        }
                    }
                }
            }
            Selected::Left(Ok(Some(event))) | Selected::Right(Ok(Some(event))) => {
                if *stop.borrow() || *cancel.borrow() {
                    continue;
                }
                let monitor_event = match monitor.process(event) {
                    Ok(event) => event,
                    Err(error) => {
                        abort_inflight(&mut in_flight, saga.account()).await;
                        return fail_owner(
                            &mut left,
                            &mut right,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            ArbitragePaperTaskFailure::MonitorContract,
                            ArbitragePaperTaskError::Monitor(error),
                        )
                        .await;
                    }
                };
                let is_opportunity = matches!(
                    monitor_event.outcome,
                    ArbitrageMonitorOutcome::Opportunity { .. }
                );
                let mut next = status_sender.borrow().clone();
                next.processed_event_count =
                    if let Some(value) = next.processed_event_count.checked_add(1) {
                        value
                    } else {
                        abort_inflight(&mut in_flight, saga.account()).await;
                        return fail_owner(
                            &mut left,
                            &mut right,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            ArbitragePaperTaskFailure::MonitorContract,
                            ArbitragePaperTaskError::InvalidRequest,
                        )
                        .await;
                    };
                if is_opportunity && in_flight.is_some() {
                    pending_opportunity = true;
                    next.coalesced_opportunity_count =
                        match next.coalesced_opportunity_count.checked_add(1) {
                            Some(value) => value,
                            None => {
                                return fail_owner(
                                    &mut left,
                                    &mut right,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    ArbitragePaperTaskFailure::MonitorContract,
                                    ArbitragePaperTaskError::InvalidRequest,
                                )
                                .await;
                            }
                        };
                }
                let recorded_at = Utc::now().max(last_recorded_at);
                next.sources = source_statuses(&left, &right);
                next.last_recorded_at = Some(recorded_at);
                next.runtime_failure = None;
                let records = [
                    monitor_event.to_record(),
                    status_record(&next, "task_checkpointed", recorded_at),
                ];
                if let Err(error) = history.append_batch(&records).await {
                    abort_inflight(&mut in_flight, saga.account()).await;
                    let _ = tokio::join!(left.stop(), right.stop());
                    publish_runtime_failure(
                        &status_sender,
                        ArbitragePaperTaskFailure::JournalUnavailable,
                    );
                    return Err(ArbitragePaperTaskError::Journal(error));
                }
                last_recorded_at = recorded_at;
                status_sender.send_replace(next);

                if is_opportunity && in_flight.is_none() && !*stop.borrow() && !*cancel.borrow() {
                    match plan_latest_operation(
                        &config,
                        &monitor,
                        saga.account(),
                        &state,
                        &mut operation_sequence,
                    )
                    .await
                    {
                        Ok(Some(planned)) if !*stop.borrow() && !*cancel.borrow() => {
                            in_flight =
                                Some(start_operation(&saga, Arc::clone(&executor), planned));
                            publish_operation_count(&status_sender, operation_sequence);
                        }
                        Ok(Some(_) | None) => {}
                        Err(error) => {
                            let failure = error.failure_bucket();
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                error,
                            )
                            .await;
                        }
                    }
                }
            }
            Selected::Left(Ok(None)) | Selected::Right(Ok(None)) => {
                return stop_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ArbitragePaperTaskExit::SourceEnded,
                    in_flight.take(),
                    false,
                    saga.account(),
                    &mut state,
                )
                .await;
            }
            Selected::Left(Err(error)) | Selected::Right(Err(error)) => {
                abort_inflight(&mut in_flight, saga.account()).await;
                return fail_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ArbitragePaperTaskFailure::SourceContract,
                    ArbitragePaperTaskError::Source(error),
                )
                .await;
            }
        }
    }
}

enum Selected {
    Stop,
    Cancel,
    Left(Result<Option<MarketDataEvent>, MarketSupervisorError>),
    Right(Result<Option<MarketDataEvent>, MarketSupervisorError>),
    Operation(OperationJoinResult),
}

async fn plan_latest_operation(
    config: &ArbitragePaperTaskConfig,
    monitor: &ReadOnlyArbitrageMonitor,
    account: &PaperAccountAuthority,
    state: &ArbitrageState,
    operation_sequence: &mut u64,
) -> Result<Option<PlannedOperation>, ArbitragePaperTaskError> {
    let (left_leg, right_leg) = monitor.legs();
    let pair = monitor.book().current_pair(left_leg, right_leg)?;
    let decision = config
        .strategy
        .evaluate_pair(state, &pair.left, &pair.right)?;
    if decision.intents.is_empty() {
        return Ok(None);
    }
    let account_snapshot = account.snapshot().await?;
    let next_sequence = operation_sequence
        .checked_add(1)
        .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
    let request = build_operation(
        config,
        &pair,
        state,
        &account_snapshot,
        &decision,
        next_sequence,
    )?;
    *operation_sequence = next_sequence;
    Ok(Some(PlannedOperation { request, decision }))
}

fn build_operation(
    config: &ArbitragePaperTaskConfig,
    pair: &ObservedMarketPair,
    state: &ArbitrageState,
    account: &PaperAccountSnapshot,
    decision: &ArbitrageDecision,
    operation_sequence: u64,
) -> Result<PaperArbitrageRequest, ArbitragePaperTaskError> {
    if account.projection_status != ProjectionStatus::Complete
        || account.reservations.iter().any(|reservation| {
            reservation
                .reconciliation
                .as_ref()
                .is_some_and(|record| record.outcome == PaperReconciliationOutcome::Failed)
        })
    {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    if decision.intents.len() != 2
        || decision.intents[0].symbol != decision.intents[1].symbol
        || decision.intents[0].exchange == decision.intents[1].exchange
    {
        return Err(ArbitragePaperTaskError::InvalidRequest);
    }
    let positions = strategy_positions(state, pair)?;
    let account_risk = AccountRiskSnapshot {
        equity: account.initial_available,
        available_balance: account.available,
        kill_switch: false,
        timestamp: pair.observed_at,
    };
    let markets = [pair.left.clone(), pair.right.clone()];
    match config.risk.authorize_batch(
        &decision.intents,
        &account_risk,
        &positions,
        &markets,
        pair.observed_at,
    ) {
        RiskDecision::Authorized => {}
        RiskDecision::Rejected(rejection) => {
            return Err(ArbitragePaperTaskError::RiskRejected(rejection));
        }
    }
    validate_liquidity(&decision.intents, &markets)?;

    let batch = ExecutionBatch::planned(decision.intents.clone())?;
    let legs = batch
        .intents()
        .iter()
        .enumerate()
        .map(|(index, intent)| {
            let market = matching_market(intent, &markets)?;
            let execution_price = intent.price.unwrap_or_else(|| match intent.side {
                Side::Buy => market.ask(),
                Side::Sell => market.bid(),
            });
            let notional = execution_price
                .as_decimal()
                .checked_mul(intent.quantity.as_decimal())
                .map(Money::new)
                .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
            PaperReservationLeg::from_intent(index, intent, notional)
                .map_err(ArbitragePaperTaskError::Account)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let task_id = format!("{}/op/{operation_sequence:06}", config.task_id);
    let idempotency_key = format!("arbitrage:{operation_sequence:06}");
    let reservation = PaperReservationRequest::planned(
        task_id,
        idempotency_key,
        batch.id(),
        config.cost_model,
        legs,
    )?;
    PaperArbitrageRequest::new(decision.intents[0].symbol.clone(), batch, reservation)
        .map_err(ArbitragePaperTaskError::Saga)
}

fn strategy_positions(
    state: &ArbitrageState,
    pair: &ObservedMarketPair,
) -> Result<Vec<Position>, ArbitragePaperTaskError> {
    if state.position_quantity.is_zero() {
        return Ok(Vec::new());
    }
    let direction = state
        .direction
        .as_ref()
        .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
    let quantity = Quantity::new(state.position_quantity)
        .map_err(|_| ArbitragePaperTaskError::InvalidRequest)?;
    let mut positions = Vec::with_capacity(2);
    for (exchange, symbol, market_type, side) in [
        (
            direction.buy_exchange.as_str(),
            &direction.buy_symbol,
            direction.buy_market_type,
            PositionSide::Long,
        ),
        (
            direction.sell_exchange.as_str(),
            &direction.sell_symbol,
            direction.sell_market_type,
            PositionSide::Short,
        ),
    ] {
        let market = [&pair.left, &pair.right]
            .into_iter()
            .find(|market| {
                market.exchange() == exchange
                    && market.symbol == *symbol
                    && market.market_type == market_type
            })
            .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
        positions.push(Position {
            exchange: exchange.to_owned(),
            symbol: symbol.clone(),
            market_type,
            side,
            quantity,
            entry_price: None,
            mark_price: Some(market.mid_price()),
            unrealized_pnl: Money::default(),
            updated_at: pair.observed_at,
        });
    }
    Ok(positions)
}

fn validate_liquidity(
    intents: &[OrderIntent],
    markets: &[MarketSnapshot; 2],
) -> Result<(), ArbitragePaperTaskError> {
    let mut required = HashMap::<(String, Symbol, MarketType, Side), Decimal>::new();
    for intent in intents {
        let market = matching_market(intent, markets)?;
        let immediately_executable = match intent.order_type {
            OrderType::Market => true,
            OrderType::Limit => {
                let price = intent
                    .price
                    .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
                match intent.side {
                    Side::Buy => price >= market.ask(),
                    Side::Sell => price <= market.bid(),
                }
            }
        };
        if !immediately_executable {
            return Err(ArbitragePaperTaskError::LiquidityRejected);
        }
        let total = required
            .entry((
                intent.exchange.clone(),
                intent.symbol.clone(),
                intent.market_type,
                intent.side,
            ))
            .or_default();
        *total = total
            .checked_add(intent.quantity.as_decimal())
            .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
    }
    for ((exchange, symbol, market_type, side), needed) in required {
        let market = markets
            .iter()
            .find(|market| {
                market.exchange() == exchange
                    && market.symbol == symbol
                    && market.market_type == market_type
            })
            .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
        let available = match side {
            Side::Buy => market.ask_quantity,
            Side::Sell => market.bid_quantity,
        }
        .ok_or(ArbitragePaperTaskError::LiquidityRejected)?
        .as_decimal();
        if available < needed {
            return Err(ArbitragePaperTaskError::LiquidityRejected);
        }
    }
    Ok(())
}

fn matching_market<'a>(
    intent: &OrderIntent,
    markets: &'a [MarketSnapshot; 2],
) -> Result<&'a MarketSnapshot, ArbitragePaperTaskError> {
    markets
        .iter()
        .find(|market| {
            market.exchange() == intent.exchange
                && market.symbol == intent.symbol
                && market.market_type == intent.market_type
        })
        .ok_or(ArbitragePaperTaskError::InvalidRequest)
}

fn start_operation(
    saga: &DurablePaperArbitrageSaga,
    executor: Arc<dyn ArbitragePaperExecutor>,
    planned: PlannedOperation,
) -> InFlightOperation {
    let request = planned.request.clone();
    let saga = saga.clone();
    let request_for_task = planned.request;
    let execution_started = Arc::new(AtomicBool::new(false));
    let task_execution_started = Arc::clone(&execution_started);
    let join = tokio::spawn(async move {
        saga.run(request_for_task, move |batch| {
            task_execution_started.store(true, Ordering::Release);
            executor.execute(batch)
        })
        .await
    });
    InFlightOperation {
        request,
        decision: planned.decision,
        execution_started,
        join: Some(join),
    }
}

fn complete_operation(
    result: OperationJoinResult,
    decision: &ArbitrageDecision,
    state: &mut ArbitrageState,
) -> Result<(), ArbitragePaperTaskError> {
    match result {
        Ok(Ok(PaperArbitrageRun::Completed { .. })) => {
            state.position_quantity = decision.target_quantity;
            state.direction.clone_from(&decision.direction);
            Ok(())
        }
        Ok(Ok(PaperArbitrageRun::AlreadyCompleted { .. })) => {
            Err(ArbitragePaperTaskError::RecoveryRequired)
        }
        Ok(Err(error)) => Err(ArbitragePaperTaskError::Saga(error)),
        Err(error) if error.is_panic() => Err(ArbitragePaperTaskError::TaskPanicked),
        Err(_) => Err(ArbitragePaperTaskError::TaskCancelled),
    }
}

async fn retain_cancelled_operation(
    account: &PaperAccountAuthority,
    request: &PaperArbitrageRequest,
) -> bool {
    let reservation_id = request.reservation().reservation_id();
    let Ok(snapshot) = account.snapshot().await else {
        return true;
    };
    let Some(reservation) = snapshot
        .reservations
        .iter()
        .find(|reservation| reservation.reservation_id == reservation_id)
    else {
        return false;
    };
    if reservation.phase == PaperReservationPhase::Pending {
        let _ = account.mark_uncertain(reservation_id).await;
    }
    true
}

async fn abort_inflight(
    operation: &mut Option<InFlightOperation>,
    account: &PaperAccountAuthority,
) {
    if let Some(mut operation) = operation.take() {
        operation.abort().await;
        let _ = retain_cancelled_operation(account, &operation.request).await;
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn stop_owner(
    left: &mut MarketSupervisor,
    right: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ArbitragePaperTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    requested_exit: ArbitragePaperTaskExit,
    mut operation: Option<InFlightOperation>,
    cancel_requested: bool,
    account: &PaperAccountAuthority,
    state: &mut ArbitrageState,
) -> TaskResult {
    let mut cancelled_reservation_needs_recovery = false;
    if let Some(active) = operation.as_mut()
        && (cancel_requested || !active.execution_started())
    {
        active.abort().await;
        cancelled_reservation_needs_recovery =
            retain_cancelled_operation(account, &active.request).await;
        operation = None;
    }

    let stopping_at = Utc::now().max(*last_recorded_at);
    let mut stopping = status_sender.borrow().clone();
    stopping.phase = ArbitragePaperTaskPhase::Stopping;
    stopping.sources = source_statuses(left, right);
    stopping.last_recorded_at = Some(stopping_at);
    stopping.runtime_failure = None;
    if let Err(error) = history
        .append(&status_record(&stopping, "task_stopping", stopping_at))
        .await
    {
        if let Some(mut operation) = operation.take() {
            operation.abort().await;
            let _ = retain_cancelled_operation(account, &operation.request).await;
        }
        let _ = tokio::join!(left.stop(), right.stop());
        publish_runtime_failure(status_sender, ArbitragePaperTaskFailure::JournalUnavailable);
        return Err(ArbitragePaperTaskError::Journal(error));
    }
    status_sender.send_replace(stopping);
    *last_recorded_at = stopping_at;

    let (left_exit, right_exit) = tokio::join!(left.stop(), right.stop());
    let (Ok(left_exit), Ok(right_exit)) = (left_exit, right_exit) else {
        if let Some(mut operation) = operation.take() {
            operation.abort().await;
            let _ = retain_cancelled_operation(account, &operation.request).await;
        }
        return fail_owner(
            left,
            right,
            history,
            status_sender,
            last_recorded_at,
            ArbitragePaperTaskFailure::SourceContract,
            ArbitragePaperTaskError::SourceContract,
        )
        .await;
    };

    if cancelled_reservation_needs_recovery {
        return fail_owner(
            left,
            right,
            history,
            status_sender,
            last_recorded_at,
            ArbitragePaperTaskFailure::RecoveryRequired,
            ArbitragePaperTaskError::RecoveryRequired,
        )
        .await;
    }

    if let Some(mut operation) = operation {
        if cancel_requested {
            operation.abort().await;
            let _ = retain_cancelled_operation(account, &operation.request).await;
            return fail_owner(
                left,
                right,
                history,
                status_sender,
                last_recorded_at,
                ArbitragePaperTaskFailure::RecoveryRequired,
                ArbitragePaperTaskError::RecoveryRequired,
            )
            .await;
        }
        let result = operation.join_mut().await;
        let _ = operation.join.take();
        if let Err(error) = complete_operation(result, &operation.decision, state) {
            let (failure, error) = classify_operation_error(error);
            return fail_owner(
                left,
                right,
                history,
                status_sender,
                last_recorded_at,
                failure,
                error,
            )
            .await;
        }
    }

    if matches!(left_exit, MarketSupervisorExit::ShutdownTimedOut)
        || matches!(right_exit, MarketSupervisorExit::ShutdownTimedOut)
    {
        return fail_owner(
            left,
            right,
            history,
            status_sender,
            last_recorded_at,
            ArbitragePaperTaskFailure::RecoveryRequired,
            ArbitragePaperTaskError::ShutdownTimedOut,
        )
        .await;
    }

    let exit = requested_exit;
    let stopped_at = Utc::now().max(*last_recorded_at);
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = ArbitragePaperTaskPhase::Stopped;
    stopped.sources = source_statuses(left, right);
    stopped.last_recorded_at = Some(stopped_at);
    stopped.exit = Some(exit);
    stopped.failure = None;
    stopped.runtime_failure = None;
    history
        .append(&status_record(&stopped, "task_stopped", stopped_at))
        .await
        .map_err(ArbitragePaperTaskError::Journal)?;
    status_sender.send_replace(stopped);
    Ok(exit)
}

async fn fail_owner(
    left: &mut MarketSupervisor,
    right: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ArbitragePaperTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    failure: ArbitragePaperTaskFailure,
    error: ArbitragePaperTaskError,
) -> TaskResult {
    let _ = tokio::join!(left.stop(), right.stop());
    let failed_at = Utc::now().max(*last_recorded_at);
    let mut failed = status_sender.borrow().clone();
    failed.phase = ArbitragePaperTaskPhase::Failed;
    failed.sources = source_statuses(left, right);
    failed.last_recorded_at = Some(failed_at);
    failed.exit = None;
    failed.failure = Some(failure);
    failed.runtime_failure = None;
    if let Err(journal_error) = history
        .append(&status_record(&failed, "task_failed", failed_at))
        .await
    {
        publish_runtime_failure(status_sender, ArbitragePaperTaskFailure::JournalUnavailable);
        return Err(ArbitragePaperTaskError::Journal(journal_error));
    }
    status_sender.send_replace(failed);
    Err(error)
}

fn classify_operation_error(
    error: ArbitragePaperTaskError,
) -> (ArbitragePaperTaskFailure, ArbitragePaperTaskError) {
    (error.failure_bucket(), error)
}

async fn recovery_preflight(
    task_id: &str,
    source_ids: &[String; 2],
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
) -> Result<u64, ArbitragePaperTaskError> {
    let account_snapshot = account.snapshot().await?;
    if account_snapshot.projection_status != ProjectionStatus::Complete {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    if account_snapshot.reservations.iter().any(|reservation| {
        reservation
            .reconciliation
            .as_ref()
            .is_some_and(|record| record.outcome == PaperReconciliationOutcome::Failed)
    }) {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }

    let prefix = operation_prefix(task_id);
    let mut last_operation = 0_u64;
    for reservation in account_snapshot
        .reservations
        .iter()
        .filter(|reservation| reservation.task_id.starts_with(&prefix))
    {
        if matches!(
            reservation.phase,
            PaperReservationPhase::Pending
                | PaperReservationPhase::Uncertain
                | PaperReservationPhase::Committed
        ) {
            return Err(ArbitragePaperTaskError::RecoveryRequired);
        }
        let suffix = reservation
            .task_id
            .strip_prefix(&prefix)
            .unwrap_or_default();
        let sequence = suffix
            .parse::<u64>()
            .map_err(|_| ArbitragePaperTaskError::RecoveryRequired)?;
        last_operation = last_operation.max(sequence);
    }

    if let Some(task) = durable_task_view(account, history.path(), task_id).await? {
        let same_sources = task.sources.len() == 2
            && task.sources[0].source_id == source_ids[0]
            && task.sources[1].source_id == source_ids[1];
        if task.kind != ReadOnlyTaskKind::ArbitragePaper
            || task.phase != ReadOnlyTaskPhase::Stopped
            || task.recovery != ReadOnlyTaskRecovery::None
            || !same_sources
        {
            return Err(ArbitragePaperTaskError::RecoveryRequired);
        }
    }
    Ok(last_operation)
}

async fn durable_task_view(
    account: &PaperAccountAuthority,
    path: &Path,
    task_id: &str,
) -> Result<Option<ReadOnlyTaskView>, ArbitragePaperTaskError> {
    let journal_id = account.journal_id();
    let source = FileJournalSnapshotSource::new(journal_id, path)?;
    let source_path = source.path().to_owned();
    let snapshot = tokio::task::spawn_blocking(move || match std::fs::metadata(&source_path) {
        Ok(_) => source.snapshot(),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            JournalSnapshot::new(journal_id, Vec::new())
        }
        Err(error) => Err(JournalReadError::Metadata(error)),
    })
    .await
    .map_err(|_| ArbitragePaperTaskError::SnapshotTaskFailed)??;
    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot)?;
    if model.projection_status != ProjectionStatus::Complete {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    Ok(model.tasks.into_iter().find(|task| task.task_id == task_id))
}

async fn record_startup_failure(
    history: &JsonlHistory,
    task_id: &str,
    source_ids: &[String; 2],
    statuses: [Option<&MarketSupervisorStatus>; 2],
    registered_at: DateTime<Utc>,
) -> Result<(), ArbitragePaperTaskError> {
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
            &Value::Array(sources),
            None,
            Some("startup_failed"),
            failed_at,
        ))
        .await
        .map_err(ArbitragePaperTaskError::Journal)
}

fn publish_operation_count(
    status_sender: &watch::Sender<ArbitragePaperTaskStatus>,
    operation_sequence: u64,
) {
    let mut status = status_sender.borrow().clone();
    status.operation_count = operation_sequence;
    status_sender.send_replace(status);
}

fn publish_runtime_failure(
    status_sender: &watch::Sender<ArbitragePaperTaskStatus>,
    failure: ArbitragePaperTaskFailure,
) {
    let mut status = status_sender.borrow().clone();
    status.runtime_failure = Some(failure);
    status_sender.send_replace(status);
}

fn source_statuses(
    left: &MarketSupervisor,
    right: &MarketSupervisor,
) -> Vec<MarketSupervisorStatus> {
    vec![left.status(), right.status()]
}

fn operation_prefix(task_id: &str) -> String {
    format!("{task_id}/op/")
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
        &Value::Array(sources),
        None,
        None,
        recorded_at,
    )
}

fn status_record(
    status: &ArbitragePaperTaskStatus,
    decision: &'static str,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    lifecycle_record(
        &status.task_id,
        decision,
        task_phase_label(status.phase),
        status.processed_event_count,
        &Value::Array(status.sources.iter().map(source_status_value).collect()),
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
    sources: &Value,
    exit: Option<&'static str>,
    failure: Option<&'static str>,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: recorded_at,
        strategy: TASK_STRATEGY.to_owned(),
        symbol: TASK_SYMBOL.to_owned(),
        decision: decision.to_owned(),
        details: json!({
            "schema_version": TASK_RECORD_SCHEMA_VERSION,
            "task_id": task_id,
            "task_kind": "arbitrage_paper",
            "phase": phase,
            "processed_event_count": processed_event_count,
            "sources": sources,
            "exit": exit,
            "failure": failure,
        }),
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
        "consecutive_source_failures": 0,
        "last_event_at": Value::Null,
        "exit": Value::Null,
    })
}

fn source_status_value(status: &MarketSupervisorStatus) -> Value {
    json!({
        "schema_version": status.schema_version,
        "task_id": status.task_id,
        "source_id": status.source_id,
        "phase": source_phase_label(status.phase),
        "health": source_health_label(status.health),
        "event_sequence": status.event_sequence,
        "consecutive_source_failures": status.consecutive_source_failures,
        "last_event_at": status.last_event_at,
        "exit": status.exit.map(source_exit_label),
    })
}

const fn task_phase_label(phase: ArbitragePaperTaskPhase) -> &'static str {
    match phase {
        ArbitragePaperTaskPhase::Running => "running",
        ArbitragePaperTaskPhase::Stopping => "stopping",
        ArbitragePaperTaskPhase::Stopped => "stopped",
        ArbitragePaperTaskPhase::Failed => "failed",
    }
}

const fn task_exit_label(exit: ArbitragePaperTaskExit) -> &'static str {
    match exit {
        ArbitragePaperTaskExit::StopRequested => "stop_requested",
        ArbitragePaperTaskExit::SourceEnded => "source_ended",
        ArbitragePaperTaskExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

const fn task_failure_label(failure: ArbitragePaperTaskFailure) -> &'static str {
    match failure {
        ArbitragePaperTaskFailure::StartupFailed => "startup_failed",
        ArbitragePaperTaskFailure::SourceContract => "source_contract",
        ArbitragePaperTaskFailure::MonitorContract => "monitor_contract",
        ArbitragePaperTaskFailure::JournalUnavailable => "journal_unavailable",
        ArbitragePaperTaskFailure::TaskPanicked => "task_panicked",
        ArbitragePaperTaskFailure::TaskCancelled => "task_cancelled",
        ArbitragePaperTaskFailure::InvalidRequest => "invalid_request",
        ArbitragePaperTaskFailure::RecoveryRequired => "recovery_required",
        ArbitragePaperTaskFailure::AccountContract => "account_contract",
        ArbitragePaperTaskFailure::ExecutionIncomplete => "execution_incomplete",
        ArbitragePaperTaskFailure::ExecutionFailed => "execution_failed",
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

fn safe_identity(value: &str) -> bool {
    value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
    })
}

/// Typed owner failures.
#[derive(Debug)]
pub enum ArbitragePaperTaskError {
    InvalidConfig,
    InvalidSourceBinding,
    InvalidRequest,
    LiquidityRejected,
    RiskRejected(RiskRejection),
    RecoveryRequired,
    ShutdownTimedOut,
    SnapshotTaskFailed,
    Journal(HistoryError),
    JournalRead(JournalReadError),
    Projection(ReadModelError),
    Account(PaperAccountError),
    Source(MarketSupervisorError),
    SourceContract,
    Monitor(ArbitrageMonitorError),
    Market(MarketDataError),
    Strategy(StrategyError),
    Runtime(RuntimeError),
    Saga(PaperArbitrageSagaError),
    TaskPanicked,
    TaskCancelled,
    PreviouslyFailed(ArbitragePaperTaskFailure),
}

impl ArbitragePaperTaskError {
    const fn failure_bucket(&self) -> ArbitragePaperTaskFailure {
        match self {
            Self::InvalidConfig
            | Self::InvalidSourceBinding
            | Self::InvalidRequest
            | Self::LiquidityRejected
            | Self::RiskRejected(_)
            | Self::Market(_)
            | Self::Strategy(_)
            | Self::Runtime(_) => ArbitragePaperTaskFailure::InvalidRequest,
            Self::RecoveryRequired | Self::ShutdownTimedOut => {
                ArbitragePaperTaskFailure::RecoveryRequired
            }
            Self::Journal(_) | Self::JournalRead(_) | Self::Projection(_) => {
                ArbitragePaperTaskFailure::JournalUnavailable
            }
            Self::Account(_) => ArbitragePaperTaskFailure::AccountContract,
            Self::Source(_) | Self::SourceContract => ArbitragePaperTaskFailure::SourceContract,
            Self::Monitor(_) => ArbitragePaperTaskFailure::MonitorContract,
            Self::Saga(error) => classify_saga_error(error),
            Self::TaskPanicked => ArbitragePaperTaskFailure::TaskPanicked,
            Self::TaskCancelled | Self::SnapshotTaskFailed => {
                ArbitragePaperTaskFailure::TaskCancelled
            }
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

const fn classify_saga_error(error: &PaperArbitrageSagaError) -> ArbitragePaperTaskFailure {
    match error {
        PaperArbitrageSagaError::RecoveryRequired { .. } => {
            ArbitragePaperTaskFailure::RecoveryRequired
        }
        PaperArbitrageSagaError::Account(_)
        | PaperArbitrageSagaError::AccountFinalization { .. } => {
            ArbitragePaperTaskFailure::AccountContract
        }
        PaperArbitrageSagaError::JournalRead(_)
        | PaperArbitrageSagaError::Projection(_)
        | PaperArbitrageSagaError::JournalWrite(_)
        | PaperArbitrageSagaError::OutcomeJournal { .. } => {
            ArbitragePaperTaskFailure::JournalUnavailable
        }
        PaperArbitrageSagaError::Execution(_) => ArbitragePaperTaskFailure::ExecutionFailed,
        PaperArbitrageSagaError::Incomplete(_) => ArbitragePaperTaskFailure::ExecutionIncomplete,
        PaperArbitrageSagaError::InvalidRequest(_) => ArbitragePaperTaskFailure::InvalidRequest,
        PaperArbitrageSagaError::SnapshotTaskFailed => ArbitragePaperTaskFailure::TaskCancelled,
    }
}

impl From<HistoryError> for ArbitragePaperTaskError {
    fn from(value: HistoryError) -> Self {
        Self::Journal(value)
    }
}

impl From<PaperAccountError> for ArbitragePaperTaskError {
    fn from(value: PaperAccountError) -> Self {
        Self::Account(value)
    }
}

impl From<JournalReadError> for ArbitragePaperTaskError {
    fn from(value: JournalReadError) -> Self {
        Self::JournalRead(value)
    }
}

impl From<ReadModelError> for ArbitragePaperTaskError {
    fn from(value: ReadModelError) -> Self {
        Self::Projection(value)
    }
}

impl From<RuntimeError> for ArbitragePaperTaskError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<StrategyError> for ArbitragePaperTaskError {
    fn from(value: StrategyError) -> Self {
        Self::Strategy(value)
    }
}

impl From<MarketDataError> for ArbitragePaperTaskError {
    fn from(value: MarketDataError) -> Self {
        Self::Market(value)
    }
}

impl fmt::Display for ArbitragePaperTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => {
                formatter.write_str("invalid arbitrage paper task configuration")
            }
            Self::InvalidSourceBinding => {
                formatter.write_str("arbitrage paper sources do not match the exact pair")
            }
            Self::InvalidRequest => formatter.write_str("arbitrage paper operation is invalid"),
            Self::LiquidityRejected => {
                formatter.write_str("arbitrage paper top-of-book liquidity rejected the operation")
            }
            Self::RiskRejected(rejection) => {
                write!(
                    formatter,
                    "arbitrage paper risk rejected the operation: {rejection:?}"
                )
            }
            Self::RecoveryRequired => {
                formatter.write_str("arbitrage paper durable state requires reconciliation")
            }
            Self::ShutdownTimedOut => {
                formatter.write_str("arbitrage paper shutdown timed out; recovery is required")
            }
            Self::SnapshotTaskFailed => {
                formatter.write_str("arbitrage paper snapshot worker failed")
            }
            Self::Journal(error) => error.fmt(formatter),
            Self::JournalRead(error) => error.fmt(formatter),
            Self::Projection(error) => error.fmt(formatter),
            Self::Account(error) => error.fmt(formatter),
            Self::Source(error) => error.fmt(formatter),
            Self::SourceContract => formatter.write_str("arbitrage paper source contract failed"),
            Self::Monitor(error) => error.fmt(formatter),
            Self::Market(error) => error.fmt(formatter),
            Self::Strategy(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Saga(error) => error.fmt(formatter),
            Self::TaskPanicked => formatter.write_str("arbitrage paper task panicked"),
            Self::TaskCancelled => formatter.write_str("arbitrage paper task was cancelled"),
            Self::PreviouslyFailed(failure) => {
                write!(
                    formatter,
                    "arbitrage paper task already failed: {failure:?}"
                )
            }
        }
    }
}

impl Error for ArbitragePaperTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::JournalRead(error) => Some(error),
            Self::Projection(error) => Some(error),
            Self::Account(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::Monitor(error) => Some(error),
            Self::Market(error) => Some(error),
            Self::Strategy(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Saga(error) => Some(error),
            Self::InvalidConfig
            | Self::InvalidSourceBinding
            | Self::InvalidRequest
            | Self::LiquidityRejected
            | Self::RiskRejected(_)
            | Self::RecoveryRequired
            | Self::ShutdownTimedOut
            | Self::SnapshotTaskFailed
            | Self::SourceContract
            | Self::TaskPanicked
            | Self::TaskCancelled
            | Self::PreviouslyFailed(_) => None,
        }
    }
}
