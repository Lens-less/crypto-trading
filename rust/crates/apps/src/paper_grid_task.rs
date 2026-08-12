//! Durable continuous owner for one single-source virtual paper grid.
//!
//! Every crossed grid level becomes one independent single-leg saga
//! operation. The owner never reuses its stable task identity as an account
//! reservation identity: operations use `owner/op/NNNNNN`, so one slow or
//! uncertain crossing cannot alias a later crossing.

use std::{
    error::Error,
    fmt,
    future::Future,
    io::ErrorKind,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, Price, Quantity, Side,
};
use crypto_trading_exchange::TradingReceipt;
use crypto_trading_runtime::{
    AccountRiskAdmission, AccountRiskAdmissionTicket, AccountRiskAuthority, AccountRiskCandidate,
    AccountRiskDirective, AccountRiskError, DecisionRecord, ExecutionBatch,
    FileJournalSnapshotSource, HistoryError, JournalReadError, JournalSnapshot,
    JournalSnapshotSource, JsonlHistory, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION, MarketDataEvent,
    MarketDataEventSource, MarketDataObservation, MarketSupervisor, MarketSupervisorConfig,
    MarketSupervisorError, MarketSupervisorExit, MarketSupervisorHealth, MarketSupervisorPhase,
    MarketSupervisorStatus, PaperAccountAuthority, PaperAccountError, PaperAccountOperationLease,
    PaperAccountSnapshot, PaperCostModel, PaperReconciliationOutcome, PaperReservationLeg,
    PaperReservationPhase, PaperReservationRequest, ProjectionStatus, ReadModelError,
    ReadOnlyTaskPhase, ReadOnlyTaskReadModel, ReadOnlyTaskRecovery, ReadOnlyTaskView, RuntimeError,
};
use crypto_trading_strategy::{
    GridDirective, GridFill, GridProtectionMachine, GridProtectionObservation,
    GridProtectionReason, StrategyError, VirtualGrid, VirtualGridCross,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::{self, MissedTickBehavior},
};

use crate::{
    DurablePaperSingleLegSaga, PaperSingleLegRequest, PaperSingleLegSagaError,
    paper_admission::{
        PaperAdmissionCompensationError, discard_planned_admission as discard_shared_admission,
        retain_cancelled_reservation,
    },
    task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture},
};

/// Stable version of the process-local grid owner status.
pub const GRID_PAPER_TASK_STATUS_SCHEMA_VERSION: u16 = 1;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";
const PROTECTION_STRATEGY: &str = "grid_protection";
const PROTECTION_RECORD_SCHEMA_VERSION: u16 = 1;
const PROTECTION_APR_WINDOW_MINUTES: i64 = 10;
const MAX_TASK_ID_BYTES: usize = 96;
const OPERATION_SUFFIX_BYTES: usize = "/op/00000000000000000000".len();

type ActiveOperationLeaseSlot = Arc<StdMutex<Option<PaperAccountOperationLease>>>;

/// Registers an acquired account-operation lease in the task handle's
/// synchronous handoff slot. Dropping this guard clears the slot before its
/// own lease clone is released, so every early return preserves the same
/// cleanup ordering without bespoke branch logic.
struct RegisteredOperationLease {
    lease: PaperAccountOperationLease,
    active_slot: ActiveOperationLeaseSlot,
}

impl RegisteredOperationLease {
    async fn acquire(
        account: &PaperAccountAuthority,
        active_slot: &ActiveOperationLeaseSlot,
    ) -> Self {
        let lease = account.acquire_operation_lease().await;
        let replaced = active_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(lease.clone());
        debug_assert!(
            replaced.is_none(),
            "one Grid owner cannot register overlapping account operations"
        );
        Self {
            lease,
            active_slot: Arc::clone(active_slot),
        }
    }

    const fn lease(&self) -> &PaperAccountOperationLease {
        &self.lease
    }
}

impl Drop for RegisteredOperationLease {
    fn drop(&mut self) {
        self.active_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

fn clone_active_operation_lease(
    active_slot: &ActiveOperationLeaseSlot,
) -> Option<PaperAccountOperationLease> {
    active_slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Boxed execution future behind the trusted paper adapter seam.
pub type GridPaperExecutionFuture =
    Pin<Box<dyn Future<Output = Result<Vec<TradingReceipt>, RuntimeError>> + Send + 'static>>;
/// Boxed hook that applies one consumed market observation to an execution
/// adapter before the owner evaluates crossings against it.
pub type GridPaperObservationFuture =
    Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'static>>;

/// Minimal object-safe execution seam owned by the trusted task process.
pub trait GridPaperExecutor: Send + Sync + 'static {
    /// Applies one observation at consumer time.
    ///
    /// Live executors implement an explicit no-op. Replay executors use this
    /// hook to prevent a fast source from advancing their paper book beyond
    /// the event the owner is currently processing. Keeping the method
    /// required makes a newly added replay executor choose its clock semantics
    /// deliberately instead of silently inheriting live behavior.
    fn observe_market(&self, observation: MarketDataObservation) -> GridPaperObservationFuture;

    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture;
}

/// Validated owner configuration. The virtual grid itself owns symbol and
/// geometry; this type owns execution and lifecycle identity.
#[derive(Clone, Debug)]
pub struct GridPaperTaskConfig {
    task_id: String,
    exchange: String,
    market_type: MarketType,
    quantity: Quantity,
    cost_model: PaperCostModel,
    supervisor: MarketSupervisorConfig,
    protection: Option<GridProtectionMachine>,
    account_risk: Option<AccountRiskAuthority>,
}

impl GridPaperTaskConfig {
    /// Creates a bounded single-source grid owner configuration.
    ///
    /// # Errors
    ///
    /// Returns [`GridPaperTaskError::InvalidConfig`] for unsafe identities or
    /// a non-positive quantity.
    pub fn new(
        task_id: impl Into<String>,
        exchange: impl Into<String>,
        market_type: MarketType,
        quantity: Quantity,
        cost_model: PaperCostModel,
        supervisor: MarketSupervisorConfig,
    ) -> Result<Self, GridPaperTaskError> {
        let task_id = task_id.into();
        let task_id = task_id.trim();
        let exchange = exchange.into();
        let exchange = exchange.trim();
        if task_id.is_empty()
            || task_id.len() > MAX_TASK_ID_BYTES
            || task_id.len().saturating_add(OPERATION_SUFFIX_BYTES) > 128
            || !safe_identity(task_id)
            || exchange.is_empty()
            || exchange.len() > 128
            || !safe_identity(exchange)
            || quantity.as_decimal() <= rust_decimal::Decimal::ZERO
        {
            return Err(GridPaperTaskError::InvalidConfig);
        }
        Ok(Self {
            task_id: task_id.to_owned(),
            exchange: exchange.to_owned(),
            market_type,
            quantity,
            cost_model,
            supervisor,
            protection: None,
            account_risk: None,
        })
    }

    /// Attaches the pure grid-protection arbitration machine. The owner
    /// translates its directives into durable `grid_protection` facts and
    /// bounded paper actions.
    #[must_use]
    pub fn with_protection(mut self, protection: GridProtectionMachine) -> Self {
        self.protection = Some(protection);
        self
    }

    /// Attaches the durable account-level risk authority. Entry-side
    /// operations must pass its admission before any reservation is created;
    /// its close directives are consumed like grid-protection exits.
    #[must_use]
    pub fn with_account_risk(mut self, account_risk: AccountRiskAuthority) -> Self {
        self.account_risk = Some(account_risk);
        self
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }
}

/// Durable aggregate lifecycle phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GridPaperTaskPhase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl GridPaperTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

/// Bounded normal terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GridPaperTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Bounded task failure suitable for the durable task projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GridPaperTaskFailure {
    StartupFailed,
    SourceContract,
    JournalUnavailable,
    TaskPanicked,
    TaskCancelled,
    InvalidRequest,
    RecoveryRequired,
    AccountContract,
    ExecutionIncomplete,
    ExecutionFailed,
}

/// Latest durable lifecycle status plus operation count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GridPaperTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: GridPaperTaskPhase,
    pub processed_event_count: u64,
    pub operation_count: u64,
    pub sources: Vec<MarketSupervisorStatus>,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub exit: Option<GridPaperTaskExit>,
    pub failure: Option<GridPaperTaskFailure>,
    pub runtime_failure: Option<GridPaperTaskFailure>,
}

impl TaskHostStatus for GridPaperTaskStatus {
    fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Opaque owner of one source supervisor and one virtual-grid execution loop.
#[derive(Debug)]
pub struct GridPaperTask {
    stop: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    status_sender: watch::Sender<GridPaperTaskStatus>,
    status: watch::Receiver<GridPaperTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<GridPaperTaskExit, GridPaperTaskFailure>>,
    account: PaperAccountAuthority,
    history: JsonlHistory,
    active_operation_lease: ActiveOperationLeaseSlot,
    shutdown_grace: Duration,
}

impl GridPaperTask {
    /// Starts one durable single-source grid owner.
    ///
    /// Recovery preflight reads the account and task projections before
    /// registration. Pending/uncertain owner operations, any failed account
    /// reconciliation, degraded projections, and a previous nonterminal owner
    /// all fail closed.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, recovery, source, account, projection,
    /// or journal failure.
    pub async fn start<S>(
        config: GridPaperTaskConfig,
        grid: VirtualGrid,
        source: S,
        account: PaperAccountAuthority,
        history: JsonlHistory,
        executor: Arc<dyn GridPaperExecutor>,
    ) -> Result<Self, GridPaperTaskError>
    where
        S: MarketDataEventSource,
    {
        if account.history_path() != history.path()
            || grid.config().symbol.as_str().trim().is_empty()
            || source.source_id() != config.exchange
        {
            return Err(GridPaperTaskError::InvalidSourceBinding);
        }
        let operation_sequence = recovery_preflight(&config.task_id, &account, &history).await?;
        if config.account_risk.is_some() {
            account.ensure_initialized().await?;
        }
        let registered_at = Utc::now();
        history
            .append(&registered_record(
                &config.task_id,
                source.source_id(),
                registered_at,
            ))
            .await
            .map_err(GridPaperTaskError::Journal)?;

        let mut supervisor = match MarketSupervisor::start_new(source, config.supervisor) {
            Ok(supervisor) => supervisor,
            Err(error) => {
                history
                    .append(&lifecycle_record(
                        &config.task_id,
                        "task_failed",
                        "failed",
                        0,
                        &Value::Array(vec![placeholder_source_value(&config.exchange)]),
                        None,
                        Some("startup_failed"),
                        Utc::now().max(registered_at),
                    ))
                    .await
                    .map_err(GridPaperTaskError::Journal)?;
                return Err(GridPaperTaskError::Source(error));
            }
        };
        tokio::task::yield_now().await;
        let running_at = Utc::now().max(registered_at);
        let initial = GridPaperTaskStatus {
            schema_version: GRID_PAPER_TASK_STATUS_SCHEMA_VERSION,
            task_id: config.task_id.clone(),
            phase: GridPaperTaskPhase::Running,
            processed_event_count: 0,
            operation_count: operation_sequence,
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
            return Err(GridPaperTaskError::Journal(error));
        }

        let saga = DurablePaperSingleLegSaga::new(account.clone(), history.clone())
            .map_err(GridPaperTaskError::Saga)?;
        let (stop, stop_receiver) = watch::channel(false);
        let (cancel, cancel_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial);
        let task_status = status_sender.clone();
        let task_history = history.clone();
        let task_config = config.clone();
        let active_operation_lease = Arc::new(StdMutex::new(None));
        let owner_operation_lease = Arc::clone(&active_operation_lease);
        let join = tokio::spawn(async move {
            Box::pin(run_owner(
                task_config,
                grid,
                supervisor,
                saga,
                executor,
                task_history,
                task_status,
                owner_operation_lease,
                stop_receiver,
                cancel_receiver,
                running_at,
                operation_sequence,
            ))
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
            active_operation_lease,
            shutdown_grace: config.supervisor.shutdown_grace(),
        })
    }

    /// Returns the latest status whose non-runtime fields were durably written.
    #[must_use]
    pub fn status(&self) -> GridPaperTaskStatus {
        self.status.borrow().clone()
    }

    /// Reprojects the stable owner status from the journal.
    ///
    /// # Errors
    ///
    /// Returns snapshot, read-model, or worker failures and never substitutes
    /// process-local state.
    pub async fn durable_status(&self) -> Result<ReadOnlyTaskView, GridPaperTaskError> {
        durable_task_view(&self.account, self.history.path(), &self.status().task_id)
            .await?
            .ok_or(GridPaperTaskError::RecoveryRequired)
    }

    /// Waits for a finite source to terminate without requesting a stop.
    ///
    /// # Errors
    ///
    /// Returns the owner result or a typed join failure.
    pub async fn wait(&mut self) -> Result<GridPaperTaskExit, GridPaperTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(GridPaperTaskError::PreviouslyFailed);
        }
        let Some(join) = self.join.take() else {
            return Err(GridPaperTaskError::TaskCancelled);
        };
        let result = Self::map_join(join.await);
        self.store_completion(&result);
        result
    }

    /// Stops admitting new crossings and waits for the current operation to
    /// reach a terminal durable outcome.
    ///
    /// # Errors
    ///
    /// Returns a typed execution, recovery, journal, or bounded shutdown
    /// failure.
    pub async fn stop(&mut self) -> Result<GridPaperTaskExit, GridPaperTaskError> {
        self.finish_with_signal(false).await
    }

    /// Requests cancellation. If an operation has crossed the execution seam,
    /// its reservation is retained as uncertain; cancellation never releases
    /// capacity without a confirmed cancelled receipt.
    ///
    /// # Errors
    ///
    /// Returns a typed recovery or lifecycle failure.
    pub async fn cancel(&mut self) -> Result<GridPaperTaskExit, GridPaperTaskError> {
        self.finish_with_signal(true).await
    }

    async fn finish_with_signal(
        &mut self,
        cancel: bool,
    ) -> Result<GridPaperTaskExit, GridPaperTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(GridPaperTaskError::PreviouslyFailed);
        }
        if cancel {
            let _ = self.cancel.send(true);
        } else {
            let _ = self.stop.send(true);
        }
        let Some(mut join) = self.join.take() else {
            return Err(GridPaperTaskError::TaskCancelled);
        };
        let deadline = self.shutdown_grace.saturating_mul(2);
        let result = if let Ok(joined) = tokio::time::timeout(deadline, &mut join).await {
            Self::map_join(joined)
        } else {
            // Clone the active guard before aborting its owner. The shared
            // lease keeps the account lane continuously exclusive while the
            // owner future is dropped and external retention takes over.
            let handoff_lease = clone_active_operation_lease(&self.active_operation_lease);
            join.abort();
            let _ = join.await;
            let retention_lease = if let Some(lease) = handoff_lease {
                lease
            } else {
                // No registered lease means the owner had not begun a new
                // snapshot/reserve/executor sequence. The saga reserves before
                // executor dispatch, so there is no crossed execution seam to
                // hand off; acquire a fresh lane for the defensive scan.
                self.account.acquire_operation_lease().await
            };
            self.retain_active_capacity(&retention_lease).await;
            self.record_external_failure(GridPaperTaskFailure::RecoveryRequired)
                .await?;
            Err(GridPaperTaskError::ShutdownTimedOut)
        };
        self.store_completion(&result);
        result
    }

    fn map_join(
        joined: Result<TaskResult, JoinError>,
    ) -> Result<GridPaperTaskExit, GridPaperTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(GridPaperTaskError::TaskPanicked),
            Err(_) => Err(GridPaperTaskError::TaskCancelled),
        }
    }

    fn store_completion(&mut self, result: &Result<GridPaperTaskExit, GridPaperTaskError>) {
        self.completion = Some(match result {
            Ok(exit) => Ok(*exit),
            Err(error) => Err(error.failure_bucket()),
        });
    }

    async fn retain_active_capacity(&self, _operation_lease: &PaperAccountOperationLease) {
        let Ok(snapshot) = self.account.decision_snapshot().await else {
            return;
        };
        let prefix = operation_prefix(&self.status().task_id);
        for reservation in snapshot.reservations.iter().filter(|reservation| {
            operation_task_belongs_to_owner(&reservation.task_id, &prefix)
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
        failure: GridPaperTaskFailure,
    ) -> Result<(), GridPaperTaskError> {
        let mut status = self.status();
        status.phase = GridPaperTaskPhase::Failed;
        status.failure = Some(failure);
        status.exit = None;
        status.runtime_failure = None;
        let recorded_at = Utc::now().max(status.last_recorded_at.unwrap_or_else(Utc::now));
        status.last_recorded_at = Some(recorded_at);
        self.history
            .append(&status_record(&status, "task_failed", recorded_at))
            .await
            .map_err(GridPaperTaskError::Journal)?;
        self.status_sender.send_replace(status);
        Ok(())
    }
}

impl TaskHost for GridPaperTask {
    type Status = GridPaperTaskStatus;
    type Exit = GridPaperTaskExit;
    type Error = GridPaperTaskError;

    fn status(&self) -> Self::Status {
        Self::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(Self::stop(self))
    }
}

impl Drop for GridPaperTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type TaskResult = Result<GridPaperTaskExit, GridPaperTaskError>;

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_owner(
    config: GridPaperTaskConfig,
    mut grid: VirtualGrid,
    mut source: MarketSupervisor,
    saga: DurablePaperSingleLegSaga,
    executor: Arc<dyn GridPaperExecutor>,
    history: JsonlHistory,
    status_sender: watch::Sender<GridPaperTaskStatus>,
    active_operation_lease: ActiveOperationLeaseSlot,
    mut stop: watch::Receiver<bool>,
    mut cancel: watch::Receiver<bool>,
    mut last_recorded_at: DateTime<Utc>,
    mut operation_sequence: u64,
) -> TaskResult {
    let mut protection = config.protection.clone();
    let mut last_protection: Option<ProtectionSignature> = None;
    let mut last_observation: Option<(MarketSnapshot, DateTime<Utc>)> = None;
    let mut risk_poll = time::interval(Duration::from_millis(250));
    risk_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    risk_poll.tick().await;
    loop {
        let selected = tokio::select! {
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
            _ = risk_poll.tick(), if config.account_risk.is_some() => Selected::RiskPoll(Utc::now()),
            result = source.next_event() => Selected::Source(result.map(|event| event.map(Box::new))),
        };
        match selected {
            Selected::Cancel | Selected::Stop => {
                return stop_owner(
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    GridPaperTaskExit::StopRequested,
                )
                .await;
            }
            Selected::Source(Ok(Some(event))) => {
                let event = *event;
                if *cancel.borrow() || *stop.borrow() {
                    return stop_owner(
                        &mut source,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        GridPaperTaskExit::StopRequested,
                    )
                    .await;
                }
                let mut next = status_sender.borrow().clone();
                next.processed_event_count = next
                    .processed_event_count
                    .checked_add(1)
                    .ok_or(GridPaperTaskError::InvalidRequest)?;
                next.sources = vec![source.status()];
                let observed = match observation_view(&config, &grid, &event) {
                    Ok(observed) => observed,
                    Err(error) => {
                        return fail_owner(
                            &mut source,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            GridPaperTaskFailure::InvalidRequest,
                            error,
                        )
                        .await;
                    }
                };
                let market_snapshot = match &event {
                    MarketDataEvent::Observation(observation) => Some(observation.snapshot.clone()),
                    MarketDataEvent::SourceGap { .. }
                    | MarketDataEvent::SourceUnavailable { .. } => None,
                };
                if let MarketDataEvent::Observation(observation) = &event
                    && let Err(error) = executor.observe_market(observation.clone()).await
                {
                    return fail_owner(
                        &mut source,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        GridPaperTaskFailure::ExecutionFailed,
                        GridPaperTaskError::Runtime(error),
                    )
                    .await;
                }
                if let (Some(snapshot), Some((_, observed_at))) =
                    (market_snapshot.as_ref(), observed)
                {
                    last_observation = Some((snapshot.clone(), observed_at));
                }
                // Durable account-risk close directives run first: a kill
                // switch, a critically low balance, or an expired position
                // clock stops the owner exactly like a protection exit.
                if let Some(risk) = config.account_risk.as_ref()
                    && let Some((_, observed_at)) = observed
                {
                    let operation_lease =
                        RegisteredOperationLease::acquire(saga.account(), &active_operation_lease)
                            .await;
                    match account_risk_exit(
                        risk,
                        saga.account(),
                        operation_lease.lease(),
                        &config,
                        &grid,
                        &history,
                        market_snapshot.as_ref(),
                        observed_at,
                        &mut last_recorded_at,
                    )
                    .await
                    {
                        Ok(AccountRiskExitAction::Stop) => {
                            return stop_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskExit::StopRequested,
                            )
                            .await;
                        }
                        Ok(AccountRiskExitAction::Continue) => drop(operation_lease),
                        Ok(AccountRiskExitAction::Close {
                            side,
                            quantity,
                            price,
                        }) => {
                            let Some(next_operation) = operation_sequence.checked_add(1) else {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::RecoveryRequired,
                                    GridPaperTaskError::RecoveryRequired,
                                )
                                .await;
                            };
                            operation_sequence = next_operation;
                            let Ok(request) = build_account_risk_close_operation(
                                &config,
                                &grid,
                                side,
                                quantity,
                                price,
                                operation_sequence,
                            ) else {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::RecoveryRequired,
                                    GridPaperTaskError::RecoveryRequired,
                                )
                                .await;
                            };
                            publish_operation_count(&status_sender, operation_sequence);
                            if let Err(error) = publish_forced_close_plan(
                                &history,
                                &config,
                                &grid,
                                "risk-close",
                                side,
                                quantity,
                                price,
                                operation_sequence,
                                &mut last_recorded_at,
                            )
                            .await
                            {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::JournalUnavailable,
                                    error,
                                )
                                .await;
                            }
                            match run_operation(
                                &saga,
                                Arc::clone(&executor),
                                request,
                                &mut stop,
                                &mut cancel,
                                operation_lease.lease(),
                                OperationRunPolicy::forced_close(
                                    &config.task_id,
                                    config.supervisor.shutdown_grace(),
                                ),
                            )
                            .await
                            {
                                OperationOutcome::Terminal(Ok(_), _) => {
                                    let Ok(snapshot) = saga.account().decision_snapshot().await
                                    else {
                                        return fail_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            GridPaperTaskFailure::RecoveryRequired,
                                            GridPaperTaskError::RecoveryRequired,
                                        )
                                        .await;
                                    };
                                    if target_instrument_close_plan(&snapshot, &config, &grid)?
                                        .is_some()
                                    {
                                        return fail_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            GridPaperTaskFailure::RecoveryRequired,
                                            GridPaperTaskError::RecoveryRequired,
                                        )
                                        .await;
                                    }
                                    if (risk
                                        .record_position_closed(&config.task_id, observed_at)
                                        .await)
                                        .is_err()
                                    {
                                        return fail_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            GridPaperTaskFailure::RecoveryRequired,
                                            GridPaperTaskError::RecoveryRequired,
                                        )
                                        .await;
                                    }
                                    next.operation_count = operation_sequence;
                                    status_sender.send_replace(next);
                                    return stop_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskExit::StopRequested,
                                    )
                                    .await;
                                }
                                OperationOutcome::Cancelled(request)
                                | OperationOutcome::TimedOut(request) => {
                                    if let Err(retain_error) = retain_cancelled_operation(
                                        saga.account(),
                                        config.account_risk.as_ref(),
                                        &config.task_id,
                                        None,
                                        &request,
                                        observed_at,
                                    )
                                    .await
                                    {
                                        let failure = retain_error.failure_bucket();
                                        return fail_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            failure,
                                            retain_error,
                                        )
                                        .await;
                                    }
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskFailure::RecoveryRequired,
                                        GridPaperTaskError::RecoveryRequired,
                                    )
                                    .await;
                                }
                                OperationOutcome::Terminal(Err(_), _)
                                | OperationOutcome::RiskInterrupted { .. }
                                | OperationOutcome::RiskUnavailable(_) => {
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskFailure::RecoveryRequired,
                                        GridPaperTaskError::RecoveryRequired,
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(error) => {
                            let failure = error.failure_bucket();
                            return fail_owner(
                                &mut source,
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
                // Protection arbitration runs before crossings are consumed so
                // a frozen or resetting grid never silently consumes levels.
                let mut protection_operation_lease = if observed.is_some() && protection.is_some() {
                    Some(
                        RegisteredOperationLease::acquire(saga.account(), &active_operation_lease)
                            .await,
                    )
                } else {
                    None
                };
                let directive = if let Some((price, observed_at)) = observed {
                    match protection_directive(
                        &mut protection,
                        &config,
                        &grid,
                        saga.account(),
                        protection_operation_lease
                            .as_ref()
                            .map(RegisteredOperationLease::lease),
                        price,
                        observed_at,
                    )
                    .await
                    {
                        Ok(directive) => directive,
                        Err(error) => {
                            let failure = error.failure_bucket();
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                error,
                            )
                            .await;
                        }
                    }
                } else {
                    GridDirective::Continue
                };

                let mut scalp_request = None;
                let crosses = match (directive, observed) {
                    (GridDirective::Continue, _) | (_, None) => {
                        if observed.is_some() {
                            last_protection = None;
                        }
                        match grid_crosses(&config, &mut grid, &event) {
                            Ok(crosses) => crosses,
                            Err(error) => {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::InvalidRequest,
                                    error,
                                )
                                .await;
                            }
                        }
                    }
                    (GridDirective::FreezeEntries { reason }, Some((price, _))) => {
                        // Freeze admits no new entry operations but keeps the
                        // position and the task alive.
                        let signature = ProtectionSignature::Freeze(reason);
                        if last_protection.as_ref() != Some(&signature) {
                            let recorded_at = Utc::now().max(last_recorded_at);
                            if let Err(error) = history
                                .append(&protection_record(
                                    &config.task_id,
                                    grid.config().symbol.as_str(),
                                    &directive,
                                    price,
                                    recorded_at,
                                ))
                                .await
                            {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::JournalUnavailable,
                                    GridPaperTaskError::Journal(error),
                                )
                                .await;
                            }
                            last_recorded_at = recorded_at;
                            last_protection = Some(signature);
                        }
                        Vec::new()
                    }
                    (
                        GridDirective::Scalp {
                            side,
                            quantity,
                            take_profit_price,
                            ..
                        },
                        Some((price, _)),
                    ) => {
                        let signature = ProtectionSignature::Scalp {
                            side,
                            quantity: quantity.as_decimal(),
                            price: take_profit_price.as_decimal(),
                        };
                        if last_protection.as_ref() != Some(&signature) {
                            let recorded_at = Utc::now().max(last_recorded_at);
                            if let Err(error) = history
                                .append(&protection_record(
                                    &config.task_id,
                                    grid.config().symbol.as_str(),
                                    &directive,
                                    price,
                                    recorded_at,
                                ))
                                .await
                            {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::JournalUnavailable,
                                    GridPaperTaskError::Journal(error),
                                )
                                .await;
                            }
                            last_recorded_at = recorded_at;
                            last_protection = Some(signature);
                            operation_sequence = operation_sequence
                                .checked_add(1)
                                .ok_or(GridPaperTaskError::InvalidRequest)?;
                            match build_protection_operation(
                                &config,
                                &grid,
                                side,
                                quantity,
                                take_profit_price,
                                operation_sequence,
                            ) {
                                Ok(request) => scalp_request = Some(request),
                                Err(error) => {
                                    next.operation_count = operation_sequence;
                                    status_sender.send_replace(next);
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskFailure::InvalidRequest,
                                        error,
                                    )
                                    .await;
                                }
                            }
                        }
                        Vec::new()
                    }
                    (GridDirective::ResetGrid { .. }, Some((price, observed_at))) => {
                        last_protection = None;
                        let recorded_at = Utc::now().max(last_recorded_at);
                        if let Err(error) = history
                            .append(&protection_record(
                                &config.task_id,
                                grid.config().symbol.as_str(),
                                &directive,
                                price,
                                recorded_at,
                            ))
                            .await
                        {
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::JournalUnavailable,
                                GridPaperTaskError::Journal(error),
                            )
                            .await;
                        }
                        last_recorded_at = recorded_at;
                        if let Err(error) = close_target_instrument_position(
                            &saga,
                            Arc::clone(&executor),
                            saga.account(),
                            protection_operation_lease
                                .as_ref()
                                .map(RegisteredOperationLease::lease)
                                .ok_or(GridPaperTaskError::RecoveryRequired)?,
                            config.account_risk.as_ref(),
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            &config,
                            &grid,
                            market_snapshot.as_ref(),
                            observed_at,
                            &mut operation_sequence,
                            &mut stop,
                            &mut cancel,
                            "protection-close",
                        )
                        .await
                        {
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                error,
                            )
                            .await;
                        }
                        next.operation_count = operation_sequence;
                        status_sender.send_replace(next.clone());
                        // This owner keeps no resting orders, so the reset
                        // analog of "cancel and rebuild" is a fresh virtual
                        // grid anchored at the observed price.
                        let mut grid_config = grid.config().clone();
                        grid_config.initial_price = price;
                        grid = match VirtualGrid::new(grid_config, observed_at) {
                            Ok(grid) => grid,
                            Err(error) => {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::InvalidRequest,
                                    GridPaperTaskError::Strategy(error),
                                )
                                .await;
                            }
                        };
                        Vec::new()
                    }
                    (GridDirective::ExitAll { .. }, Some((price, observed_at))) => {
                        let recorded_at = Utc::now().max(last_recorded_at);
                        if let Err(error) = history
                            .append(&protection_record(
                                &config.task_id,
                                grid.config().symbol.as_str(),
                                &directive,
                                price,
                                recorded_at,
                            ))
                            .await
                        {
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::JournalUnavailable,
                                GridPaperTaskError::Journal(error),
                            )
                            .await;
                        }
                        last_recorded_at = recorded_at;
                        if let Err(error) = close_target_instrument_position(
                            &saga,
                            Arc::clone(&executor),
                            saga.account(),
                            protection_operation_lease
                                .as_ref()
                                .map(RegisteredOperationLease::lease)
                                .ok_or(GridPaperTaskError::RecoveryRequired)?,
                            config.account_risk.as_ref(),
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            &config,
                            &grid,
                            market_snapshot.as_ref(),
                            observed_at,
                            &mut operation_sequence,
                            &mut stop,
                            &mut cancel,
                            "protection-close",
                        )
                        .await
                        {
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                error,
                            )
                            .await;
                        }
                        next.operation_count = operation_sequence;
                        status_sender.send_replace(next);
                        return stop_owner(
                            &mut source,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            GridPaperTaskExit::StopRequested,
                        )
                        .await;
                    }
                };

                let cross_count = crosses.len();
                let mut completed_cross_count = 0_usize;
                let mut stop_after_operation = false;
                if let Some(request) = scalp_request {
                    let operation_lease = protection_operation_lease
                        .take()
                        .ok_or(GridPaperTaskError::RecoveryRequired)?;
                    match run_operation(
                        &saga,
                        Arc::clone(&executor),
                        request,
                        &mut stop,
                        &mut cancel,
                        operation_lease.lease(),
                        OperationRunPolicy::monitored(
                            config.account_risk.as_ref(),
                            &config.task_id,
                        ),
                    )
                    .await
                    {
                        OperationOutcome::Terminal(Ok(_), stop_requested) => {
                            next.operation_count = operation_sequence;
                            stop_after_operation |= stop_requested;
                        }
                        OperationOutcome::Terminal(Err(error), _) => {
                            let (failure, error) = classify_saga_error(error);
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                error,
                            )
                            .await;
                        }
                        OperationOutcome::Cancelled(request)
                        | OperationOutcome::RiskUnavailable(request)
                        | OperationOutcome::TimedOut(request) => {
                            if let Err(retain_error) = retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                None,
                                &request,
                                Utc::now(),
                            )
                            .await
                            {
                                let failure = retain_error.failure_bucket();
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    retain_error,
                                )
                                .await;
                            }
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                GridPaperTaskError::RecoveryRequired,
                            )
                            .await;
                        }
                        OperationOutcome::RiskInterrupted {
                            request,
                            reason,
                            detected_at,
                        } => {
                            if let Err(retain_error) = retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                None,
                                &request,
                                detected_at,
                            )
                            .await
                            {
                                let failure = retain_error.failure_bucket();
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    retain_error,
                                )
                                .await;
                            }
                            if let Err(error) = publish_pending_risk_directive(
                                &history,
                                &config,
                                &grid,
                                market_snapshot.as_ref(),
                                &reason,
                                detected_at,
                                &mut last_recorded_at,
                            )
                            .await
                            {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::JournalUnavailable,
                                    error,
                                )
                                .await;
                            }
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                GridPaperTaskError::RecoveryRequired,
                            )
                            .await;
                        }
                    }
                }
                drop(protection_operation_lease);
                for cross in crosses {
                    if *cancel.borrow() || *stop.borrow() {
                        next.operation_count = operation_sequence;
                        status_sender.send_replace(next);
                        return fail_owner(
                            &mut source,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            GridPaperTaskFailure::RecoveryRequired,
                            GridPaperTaskError::RecoveryRequired,
                        )
                        .await;
                    }
                    // Crossing classification uses the real target-instrument
                    // net book, not the virtual grid's directional counts.
                    let observed_at = observed.map_or_else(Utc::now, |(_, at)| at);
                    let order_side = match cross.side {
                        GridFill::Buy => Side::Buy,
                        GridFill::Sell => Side::Sell,
                    };
                    let operation_lease =
                        RegisteredOperationLease::acquire(saga.account(), &active_operation_lease)
                            .await;
                    let account_snapshot =
                        match operation_decision_snapshot(saga.account(), operation_lease.lease())
                            .await
                        {
                            Ok(snapshot) => snapshot,
                            Err(error) => {
                                let failure = error.failure_bucket();
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    error,
                                )
                                .await;
                            }
                        };
                    match target_instrument_has_foreign_lots(&account_snapshot, &config, &grid) {
                        Ok(false) => {}
                        Ok(true) | Err(_) => {
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                GridPaperTaskError::RecoveryRequired,
                            )
                            .await;
                        }
                    }
                    let current_side =
                        match target_instrument_close_plan(&account_snapshot, &config, &grid) {
                            Ok(Some((Side::Sell, _))) => Some(Side::Buy),
                            Ok(Some((Side::Buy, _))) => Some(Side::Sell),
                            Ok(None) => None,
                            Err(error) => {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::RecoveryRequired,
                                    error,
                                )
                                .await;
                            }
                        };
                    let reduce_only = current_side.is_some_and(|side| side != order_side);
                    let reservation_price =
                        conservative_reservation_price(&event, cross.side, cross.trigger_price);
                    let admission_ticket = if let Some(risk) = config.account_risk.as_ref()
                        && !reduce_only
                    {
                        match admit_grid_entry(risk, &config, &grid, reservation_price, observed_at)
                            .await
                        {
                            Ok(ticket) => ticket,
                            Err(error) => {
                                let failure = error.failure_bucket();
                                next.operation_count = operation_sequence;
                                status_sender.send_replace(next);
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    error,
                                )
                                .await;
                            }
                        }
                    } else {
                        None
                    };
                    if !reduce_only && config.account_risk.is_some() && admission_ticket.is_none() {
                        completed_cross_count += 1;
                        drop(operation_lease);
                        continue;
                    }
                    let Some(next_operation) = operation_sequence.checked_add(1) else {
                        if !reduce_only
                            && let Some(risk) = config.account_risk.as_ref()
                            && let Err(error) = discard_planned_admission(
                                Some(risk),
                                &config.task_id,
                                admission_ticket.as_ref(),
                                observed_at,
                            )
                            .await
                        {
                            let failure = error.failure_bucket();
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                error,
                            )
                            .await;
                        }
                        return fail_owner(
                            &mut source,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            GridPaperTaskFailure::InvalidRequest,
                            GridPaperTaskError::InvalidRequest,
                        )
                        .await;
                    };
                    operation_sequence = next_operation;
                    let request = match build_operation(
                        &config,
                        &grid,
                        cross,
                        reduce_only,
                        reservation_price,
                        operation_sequence,
                        config.account_risk.as_ref().zip(admission_ticket.as_ref()),
                    ) {
                        Ok(request) => request,
                        Err(error) => {
                            if let Err(cancel_error) = discard_planned_admission(
                                config.account_risk.as_ref(),
                                &config.task_id,
                                admission_ticket.as_ref(),
                                observed_at,
                            )
                            .await
                            {
                                let failure = cancel_error.failure_bucket();
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    cancel_error,
                                )
                                .await;
                            }
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::InvalidRequest,
                                error,
                            )
                            .await;
                        }
                    };
                    let recovery_request = request.clone();
                    match run_operation(
                        &saga,
                        Arc::clone(&executor),
                        request,
                        &mut stop,
                        &mut cancel,
                        operation_lease.lease(),
                        OperationRunPolicy::monitored(
                            config.account_risk.as_ref(),
                            &config.task_id,
                        ),
                    )
                    .await
                    {
                        OperationOutcome::Terminal(Ok(_), stop_requested) => {
                            completed_cross_count += 1;
                            next.operation_count = operation_sequence;
                            stop_after_operation |= stop_requested;
                        }
                        OperationOutcome::Terminal(Err(error), _) => {
                            let needs_recovery = match retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                admission_ticket.as_ref(),
                                &recovery_request,
                                observed_at,
                            )
                            .await
                            {
                                Ok(needs_recovery) => needs_recovery,
                                Err(retain_error) => {
                                    let failure = retain_error.failure_bucket();
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        failure,
                                        retain_error,
                                    )
                                    .await;
                                }
                            };
                            if needs_recovery {
                                next.operation_count = operation_sequence;
                                status_sender.send_replace(next);
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::RecoveryRequired,
                                    GridPaperTaskError::RecoveryRequired,
                                )
                                .await;
                            }
                            let (failure, error) = classify_saga_error(error);
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                error,
                            )
                            .await;
                        }
                        OperationOutcome::Cancelled(request) => {
                            let needs_recovery = match retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                admission_ticket.as_ref(),
                                &request,
                                observed_at,
                            )
                            .await
                            {
                                Ok(needs_recovery) => needs_recovery,
                                Err(retain_error) => {
                                    let failure = retain_error.failure_bucket();
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        failure,
                                        retain_error,
                                    )
                                    .await;
                                }
                            };
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            if needs_recovery {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::RecoveryRequired,
                                    GridPaperTaskError::RecoveryRequired,
                                )
                                .await;
                            }
                            return stop_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskExit::StopRequested,
                            )
                            .await;
                        }
                        OperationOutcome::RiskInterrupted {
                            request,
                            reason,
                            detected_at,
                        } => {
                            if let Err(retain_error) = retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                admission_ticket.as_ref(),
                                &request,
                                detected_at,
                            )
                            .await
                            {
                                let failure = retain_error.failure_bucket();
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    retain_error,
                                )
                                .await;
                            }
                            if let Err(error) = publish_pending_risk_directive(
                                &history,
                                &config,
                                &grid,
                                market_snapshot.as_ref(),
                                &reason,
                                detected_at,
                                &mut last_recorded_at,
                            )
                            .await
                            {
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::JournalUnavailable,
                                    error,
                                )
                                .await;
                            }
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                GridPaperTaskError::RecoveryRequired,
                            )
                            .await;
                        }
                        OperationOutcome::RiskUnavailable(request)
                        | OperationOutcome::TimedOut(request) => {
                            if let Err(retain_error) = retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                admission_ticket.as_ref(),
                                &request,
                                Utc::now(),
                            )
                            .await
                            {
                                let failure = retain_error.failure_bucket();
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    retain_error,
                                )
                                .await;
                            }
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                GridPaperTaskError::RecoveryRequired,
                            )
                            .await;
                        }
                    }
                    drop(operation_lease);
                    if stop_after_operation {
                        break;
                    }
                }

                if completed_cross_count < cross_count {
                    next.operation_count = operation_sequence;
                    status_sender.send_replace(next);
                    return fail_owner(
                        &mut source,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        GridPaperTaskFailure::RecoveryRequired,
                        GridPaperTaskError::RecoveryRequired,
                    )
                    .await;
                }

                // Close the owner-level risk clock only from its actual
                // reservation-owned paper lots. Virtual crossing counters can
                // be flat while an externally seeded or partially reduced lot
                // is still open.
                if let Some(risk) = config.account_risk.as_ref()
                    && cross_count > 0
                {
                    let account_snapshot = match saga.account().decision_snapshot().await {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::AccountContract,
                                GridPaperTaskError::Account(error),
                            )
                            .await;
                        }
                    };
                    let owner_flat =
                        match target_instrument_close_plan(&account_snapshot, &config, &grid) {
                            Ok(None) => true,
                            Ok(Some(_)) => false,
                            Err(error) => {
                                next.operation_count = operation_sequence;
                                status_sender.send_replace(next);
                                return fail_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskFailure::RecoveryRequired,
                                    error,
                                )
                                .await;
                            }
                        };
                    if owner_flat {
                        let closed_at = observed.map_or_else(Utc::now, |(_, at)| at);
                        if let Err(error) = risk
                            .record_position_closed(&config.task_id, closed_at)
                            .await
                        {
                            next.operation_count = operation_sequence;
                            status_sender.send_replace(next);
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::AccountContract,
                                GridPaperTaskError::AccountRisk(error),
                            )
                            .await;
                        }
                    }
                }

                let recorded_at = Utc::now().max(last_recorded_at);
                next.sources = vec![source.status()];
                next.last_recorded_at = Some(recorded_at);
                history
                    .append(&status_record(&next, "task_checkpointed", recorded_at))
                    .await
                    .map_err(GridPaperTaskError::Journal)?;
                last_recorded_at = recorded_at;
                status_sender.send_replace(next);
                if stop_after_operation {
                    return stop_owner(
                        &mut source,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        GridPaperTaskExit::StopRequested,
                    )
                    .await;
                }
            }
            Selected::Source(Ok(None)) => {
                let snapshot = match saga.account().decision_snapshot().await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        return fail_owner(
                            &mut source,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            GridPaperTaskFailure::AccountContract,
                            GridPaperTaskError::Account(error),
                        )
                        .await;
                    }
                };
                match target_instrument_close_plan(&snapshot, &config, &grid) {
                    Ok(None) => {}
                    Ok(Some(_)) | Err(_) => {
                        return fail_owner(
                            &mut source,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            GridPaperTaskFailure::RecoveryRequired,
                            GridPaperTaskError::RecoveryRequired,
                        )
                        .await;
                    }
                }
                return stop_owner(
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    GridPaperTaskExit::SourceEnded,
                )
                .await;
            }
            Selected::Source(Err(error)) => {
                return fail_owner(
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    GridPaperTaskFailure::SourceContract,
                    GridPaperTaskError::Source(error),
                )
                .await;
            }
            Selected::RiskPoll(polled_at) => {
                if let Some((snapshot, _)) = last_observation.as_ref() {
                    if let Some(risk) = config.account_risk.as_ref() {
                        let operation_lease = RegisteredOperationLease::acquire(
                            saga.account(),
                            &active_operation_lease,
                        )
                        .await;
                        match account_risk_exit(
                            risk,
                            saga.account(),
                            operation_lease.lease(),
                            &config,
                            &grid,
                            &history,
                            Some(snapshot),
                            polled_at,
                            &mut last_recorded_at,
                        )
                        .await
                        {
                            Ok(AccountRiskExitAction::Stop) => {
                                return stop_owner(
                                    &mut source,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    GridPaperTaskExit::StopRequested,
                                )
                                .await;
                            }
                            Ok(AccountRiskExitAction::Continue) => drop(operation_lease),
                            Ok(AccountRiskExitAction::Close {
                                side,
                                quantity,
                                price,
                            }) => {
                                let Some(next_operation) = operation_sequence.checked_add(1) else {
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskFailure::RecoveryRequired,
                                        GridPaperTaskError::RecoveryRequired,
                                    )
                                    .await;
                                };
                                operation_sequence = next_operation;
                                let Ok(request) = build_account_risk_close_operation(
                                    &config,
                                    &grid,
                                    side,
                                    quantity,
                                    price,
                                    operation_sequence,
                                ) else {
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskFailure::RecoveryRequired,
                                        GridPaperTaskError::RecoveryRequired,
                                    )
                                    .await;
                                };
                                publish_operation_count(&status_sender, operation_sequence);
                                if let Err(error) = publish_forced_close_plan(
                                    &history,
                                    &config,
                                    &grid,
                                    "risk-close",
                                    side,
                                    quantity,
                                    price,
                                    operation_sequence,
                                    &mut last_recorded_at,
                                )
                                .await
                                {
                                    return fail_owner(
                                        &mut source,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        GridPaperTaskFailure::JournalUnavailable,
                                        error,
                                    )
                                    .await;
                                }
                                match run_operation(
                                    &saga,
                                    Arc::clone(&executor),
                                    request,
                                    &mut stop,
                                    &mut cancel,
                                    operation_lease.lease(),
                                    OperationRunPolicy::forced_close(
                                        &config.task_id,
                                        config.supervisor.shutdown_grace(),
                                    ),
                                )
                                .await
                                {
                                    OperationOutcome::Terminal(Ok(_), _) => {
                                        let Ok(snapshot) = saga.account().decision_snapshot().await
                                        else {
                                            return fail_owner(
                                                &mut source,
                                                &history,
                                                &status_sender,
                                                &mut last_recorded_at,
                                                GridPaperTaskFailure::RecoveryRequired,
                                                GridPaperTaskError::RecoveryRequired,
                                            )
                                            .await;
                                        };
                                        if target_instrument_close_plan(&snapshot, &config, &grid)?
                                            .is_some()
                                        {
                                            return fail_owner(
                                                &mut source,
                                                &history,
                                                &status_sender,
                                                &mut last_recorded_at,
                                                GridPaperTaskFailure::RecoveryRequired,
                                                GridPaperTaskError::RecoveryRequired,
                                            )
                                            .await;
                                        }
                                        if (risk
                                            .record_position_closed(&config.task_id, polled_at)
                                            .await)
                                            .is_err()
                                        {
                                            return fail_owner(
                                                &mut source,
                                                &history,
                                                &status_sender,
                                                &mut last_recorded_at,
                                                GridPaperTaskFailure::RecoveryRequired,
                                                GridPaperTaskError::RecoveryRequired,
                                            )
                                            .await;
                                        }
                                        return stop_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            GridPaperTaskExit::StopRequested,
                                        )
                                        .await;
                                    }
                                    OperationOutcome::Cancelled(request)
                                    | OperationOutcome::TimedOut(request) => {
                                        if let Err(retain_error) = retain_cancelled_operation(
                                            saga.account(),
                                            config.account_risk.as_ref(),
                                            &config.task_id,
                                            None,
                                            &request,
                                            polled_at,
                                        )
                                        .await
                                        {
                                            let failure = retain_error.failure_bucket();
                                            return fail_owner(
                                                &mut source,
                                                &history,
                                                &status_sender,
                                                &mut last_recorded_at,
                                                failure,
                                                retain_error,
                                            )
                                            .await;
                                        }
                                        return fail_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            GridPaperTaskFailure::RecoveryRequired,
                                            GridPaperTaskError::RecoveryRequired,
                                        )
                                        .await;
                                    }
                                    OperationOutcome::Terminal(Err(_), _)
                                    | OperationOutcome::RiskInterrupted { .. }
                                    | OperationOutcome::RiskUnavailable(_) => {
                                        return fail_owner(
                                            &mut source,
                                            &history,
                                            &status_sender,
                                            &mut last_recorded_at,
                                            GridPaperTaskFailure::RecoveryRequired,
                                            GridPaperTaskError::RecoveryRequired,
                                        )
                                        .await;
                                    }
                                }
                            }
                            Err(error) => {
                                let failure = error.failure_bucket();
                                return fail_owner(
                                    &mut source,
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
                } else if let Some(risk) = config.account_risk.as_ref() {
                    let operation_lease =
                        RegisteredOperationLease::acquire(saga.account(), &active_operation_lease)
                            .await;
                    match account_risk_exit(
                        risk,
                        saga.account(),
                        operation_lease.lease(),
                        &config,
                        &grid,
                        &history,
                        None,
                        polled_at,
                        &mut last_recorded_at,
                    )
                    .await
                    {
                        Ok(AccountRiskExitAction::Stop) => {
                            return stop_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskExit::StopRequested,
                            )
                            .await;
                        }
                        Ok(AccountRiskExitAction::Continue) => drop(operation_lease),
                        Ok(AccountRiskExitAction::Close { .. }) => {
                            return fail_owner(
                                &mut source,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                GridPaperTaskFailure::RecoveryRequired,
                                GridPaperTaskError::RecoveryRequired,
                            )
                            .await;
                        }
                        Err(error) => {
                            let failure = error.failure_bucket();
                            return fail_owner(
                                &mut source,
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
        }
    }
}

enum Selected {
    Stop,
    Cancel,
    RiskPoll(DateTime<Utc>),
    Source(Result<Option<Box<MarketDataEvent>>, MarketSupervisorError>),
}

enum OperationOutcome {
    Terminal(
        Result<crate::PaperSingleLegRun, PaperSingleLegSagaError>,
        bool,
    ),
    Cancelled(PaperSingleLegRequest),
    RiskInterrupted {
        request: PaperSingleLegRequest,
        reason: String,
        detected_at: DateTime<Utc>,
    },
    RiskUnavailable(PaperSingleLegRequest),
    TimedOut(PaperSingleLegRequest),
}

#[derive(Clone, Copy)]
struct OperationRunPolicy<'a> {
    risk: Option<&'a AccountRiskAuthority>,
    owner_task_id: &'a str,
    timeout: Option<Duration>,
}

impl<'a> OperationRunPolicy<'a> {
    const fn monitored(risk: Option<&'a AccountRiskAuthority>, owner_task_id: &'a str) -> Self {
        Self {
            risk,
            owner_task_id,
            timeout: None,
        }
    }

    const fn forced_close(owner_task_id: &'a str, timeout: Duration) -> Self {
        Self {
            risk: None,
            owner_task_id,
            timeout: Some(timeout),
        }
    }
}

enum AccountRiskExitAction {
    Stop,
    Continue,
    Close {
        side: Side,
        quantity: Quantity,
        price: Price,
    },
}

async fn run_operation(
    saga: &DurablePaperSingleLegSaga,
    executor: Arc<dyn GridPaperExecutor>,
    request: PaperSingleLegRequest,
    stop: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
    _operation_lease: &PaperAccountOperationLease,
    policy: OperationRunPolicy<'_>,
) -> OperationOutcome {
    let cancel_request = request.clone();
    {
        let run = saga.run(request, move |batch| executor.execute(batch));
        tokio::pin!(run);
        let mut stop_requested = false;
        let mut risk_poll = time::interval(Duration::from_millis(250));
        risk_poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        risk_poll.tick().await;
        let deadline = async move {
            if let Some(timeout) = policy.timeout {
                time::sleep(timeout).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                cancel_result = cancel.changed() => {
                    if cancel_result.is_err() || *cancel.borrow_and_update() {
                        break OperationOutcome::Cancelled(cancel_request);
                    }
                }
                stop_result = stop.changed(), if !stop_requested => {
                    if stop_result.is_err() || *stop.borrow_and_update() {
                        stop_requested = true;
                    }
                }
                // Poll the risk read as part of this select branch. The saga
                // can hold the shared authority lock across journal I/O; if a
                // completed tick won first and awaited directives outside the
                // select, the suspended saga could never release that lock.
                directives = async {
                    let risk = policy.risk.expect("risk poll is guarded by is_some");
                    risk_poll.tick().await;
                    let observed_at = Utc::now();
                    (observed_at, risk.directives(observed_at).await)
                }, if policy.risk.is_some() => {
                    let (observed_at, directives) = directives;
                    match directives {
                        Ok(directives) => {
                            if let Some(reason) = account_risk_exit_reason(&directives, policy.owner_task_id) {
                                break OperationOutcome::RiskInterrupted {
                                    request: cancel_request,
                                    reason,
                                    detected_at: observed_at,
                                };
                            }
                        }
                        Err(_) => break OperationOutcome::RiskUnavailable(cancel_request),
                    }
                }
                () = &mut deadline => {
                    break OperationOutcome::TimedOut(cancel_request);
                }
                result = &mut run => {
                    break OperationOutcome::Terminal(result, stop_requested);
                }
            }
        }
    }
}

/// Reads the decision snapshot while proving the caller owns the account
/// operation lease and rejects any reservation whose execution outcome is not
/// yet final. This is also the lease-handoff barrier after an externally
/// aborted owner drops its in-flight guard before marking capacity uncertain.
/// The durable saga reserves before invoking the executor, so a handoff
/// snapshot with no active reservation proves that the aborted operation had
/// not crossed the execution side-effect seam; terminal reservations are safe
/// for the next owner to evaluate normally.
async fn operation_decision_snapshot(
    account: &PaperAccountAuthority,
    _operation_lease: &PaperAccountOperationLease,
) -> Result<PaperAccountSnapshot, GridPaperTaskError> {
    let snapshot = account.decision_snapshot().await?;
    if snapshot.reservations.iter().any(|reservation| {
        matches!(
            reservation.phase,
            PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
        )
    }) {
        return Err(GridPaperTaskError::RecoveryRequired);
    }
    Ok(snapshot)
}

async fn retain_cancelled_operation(
    account: &PaperAccountAuthority,
    risk: Option<&AccountRiskAuthority>,
    owner_task_id: &str,
    admission_ticket: Option<&AccountRiskAdmissionTicket>,
    request: &PaperSingleLegRequest,
    now: DateTime<Utc>,
) -> Result<bool, GridPaperTaskError> {
    retain_cancelled_reservation(
        account,
        risk,
        owner_task_id,
        admission_ticket,
        request.reservation().reservation_id(),
        now,
    )
    .await
    .map_err(GridPaperTaskError::from)
}

async fn discard_planned_admission(
    risk: Option<&AccountRiskAuthority>,
    task_id: &str,
    ticket: Option<&AccountRiskAdmissionTicket>,
    now: DateTime<Utc>,
) -> Result<(), GridPaperTaskError> {
    discard_shared_admission(risk, task_id, ticket, now)
        .await
        .map_err(GridPaperTaskError::from)
}

fn grid_crosses(
    config: &GridPaperTaskConfig,
    grid: &mut VirtualGrid,
    event: &MarketDataEvent,
) -> Result<Vec<VirtualGridCross>, GridPaperTaskError> {
    match observation_view(config, grid, event)? {
        Some((price, received_at)) => grid
            .consume_crosses_at(price, received_at)
            .map_err(GridPaperTaskError::Strategy),
        None => Ok(Vec::new()),
    }
}

fn observation_view(
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    event: &MarketDataEvent,
) -> Result<Option<(Price, DateTime<Utc>)>, GridPaperTaskError> {
    match event {
        MarketDataEvent::Observation(observation) => {
            if observation.snapshot.exchange() != config.exchange
                || observation.snapshot.symbol != grid.config().symbol
                || observation.snapshot.market_type != config.market_type
            {
                return Err(GridPaperTaskError::InvalidSourceBinding);
            }
            let price = observation
                .snapshot
                .last
                .unwrap_or_else(|| observation.snapshot.mid_price());
            Ok(Some((price, observation.received_at)))
        }
        MarketDataEvent::SourceGap { .. } | MarketDataEvent::SourceUnavailable { .. } => Ok(None),
    }
}

/// Evaluates durable account-risk close directives at the observed instant.
/// A demanded closure journals one bounded `account_risk` directive fact and
/// either stops immediately on a flat book or returns the exact reduce-only
/// close quantity to execute before stopping.
#[allow(clippy::too_many_arguments)]
async fn account_risk_exit(
    risk: &AccountRiskAuthority,
    account: &PaperAccountAuthority,
    operation_lease: &PaperAccountOperationLease,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    history: &JsonlHistory,
    market: Option<&MarketSnapshot>,
    observed_at: DateTime<Utc>,
    last_recorded_at: &mut DateTime<Utc>,
) -> Result<AccountRiskExitAction, GridPaperTaskError> {
    let directives = risk
        .directives(observed_at)
        .await
        .map_err(GridPaperTaskError::AccountRisk)?;
    let Some(reason) = account_risk_exit_reason(&directives, &config.task_id) else {
        return Ok(AccountRiskExitAction::Continue);
    };
    let reference_price = market
        .map(|snapshot| snapshot.last.unwrap_or_else(|| snapshot.mid_price()))
        .map_or_else(|| "unavailable".to_owned(), |price| price.to_string());
    let recorded_at = Utc::now().max(*last_recorded_at);
    history
        .append(&account_risk_directive_record(
            &config.task_id,
            "grid_paper",
            grid.config().symbol.as_str(),
            &reason,
            &reference_price,
            recorded_at,
        ))
        .await
        .map_err(GridPaperTaskError::Journal)?;
    *last_recorded_at = recorded_at;
    let snapshot = operation_decision_snapshot(account, operation_lease).await?;
    let Some((side, quantity)) = target_instrument_close_plan(&snapshot, config, grid)? else {
        return Ok(AccountRiskExitAction::Stop);
    };
    let market = market.ok_or(GridPaperTaskError::RecoveryRequired)?;
    let price = marketable_close_price(market, side);
    Ok(AccountRiskExitAction::Close {
        side,
        quantity,
        price,
    })
}

/// Admits one entry-side crossing through the account-level risk authority.
async fn admit_grid_entry(
    risk: &AccountRiskAuthority,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    reservation_price: Price,
    observed_at: DateTime<Utc>,
) -> Result<Option<AccountRiskAdmissionTicket>, GridPaperTaskError> {
    let notional = reservation_price
        .as_decimal()
        .checked_mul(config.quantity.as_decimal())
        .map(Money::new)
        .ok_or(GridPaperTaskError::InvalidRequest)?;
    let candidate = AccountRiskCandidate::new(
        config.task_id.clone(),
        grid.config().symbol.as_str(),
        notional,
    )
    .map_err(GridPaperTaskError::AccountRisk)?;
    match risk
        .admit(&candidate, observed_at)
        .await
        .map_err(GridPaperTaskError::AccountRisk)?
    {
        AccountRiskAdmission::Admitted { ticket, .. } => Ok(Some(ticket)),
        AccountRiskAdmission::Rejected(_) => Ok(None),
    }
}

/// One bounded durable fact naming the consumed account-risk directive.
pub(crate) fn account_risk_directive_record(
    task_id: &str,
    task_kind: &'static str,
    symbol: &str,
    reason: &str,
    price: &str,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: recorded_at,
        strategy: "account_risk".to_owned(),
        symbol: symbol.to_owned(),
        decision: "account_risk_directive_exit".to_owned(),
        details: json!({
            "schema_version": 1,
            "task_id": task_id,
            "task_kind": task_kind,
            "reason": reason,
            "price": price,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn forced_close_record(
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    trigger: &'static str,
    side: Side,
    quantity: Quantity,
    price: Price,
    operation_sequence: u64,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: recorded_at,
        strategy: "grid".to_owned(),
        symbol: grid.config().symbol.as_str().to_owned(),
        decision: "grid_forced_close_planned".to_owned(),
        details: json!({
            "schema_version": 1,
            "task_id": config.task_id,
            "task_kind": "grid_paper",
            "trigger": trigger,
            "side": match side { Side::Buy => "buy", Side::Sell => "sell" },
            "quantity": quantity.to_string(),
            "price": price.to_string(),
            "operation_sequence": operation_sequence,
            "operation_count": operation_sequence,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
async fn publish_forced_close_plan(
    history: &JsonlHistory,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    trigger: &'static str,
    side: Side,
    quantity: Quantity,
    price: Price,
    operation_sequence: u64,
    last_recorded_at: &mut DateTime<Utc>,
) -> Result<(), GridPaperTaskError> {
    let recorded_at = Utc::now().max(*last_recorded_at);
    history
        .append(&forced_close_record(
            config,
            grid,
            trigger,
            side,
            quantity,
            price,
            operation_sequence,
            recorded_at,
        ))
        .await
        .map_err(GridPaperTaskError::Journal)?;
    *last_recorded_at = recorded_at;
    Ok(())
}

async fn publish_pending_risk_directive(
    history: &JsonlHistory,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    market: Option<&MarketSnapshot>,
    reason: &str,
    detected_at: DateTime<Utc>,
    last_recorded_at: &mut DateTime<Utc>,
) -> Result<(), GridPaperTaskError> {
    let price = market
        .map(|snapshot| snapshot.last.unwrap_or_else(|| snapshot.mid_price()))
        .map_or_else(|| "unavailable".to_owned(), |price| price.to_string());
    let recorded_at = detected_at.max(*last_recorded_at);
    history
        .append(&account_risk_directive_record(
            &config.task_id,
            "grid_paper",
            grid.config().symbol.as_str(),
            reason,
            &price,
            recorded_at,
        ))
        .await
        .map_err(GridPaperTaskError::Journal)?;
    *last_recorded_at = recorded_at;
    Ok(())
}

/// Extracts the first close demand that applies to this owner.
pub(crate) fn account_risk_exit_reason(
    directives: &[AccountRiskDirective],
    task_id: &str,
) -> Option<String> {
    directives.iter().find_map(|directive| match directive {
        AccountRiskDirective::CloseAllPositions { reason } => Some(reason.clone()),
        AccountRiskDirective::ClosePosition {
            task_id: target, ..
        } if target == task_id => Some("position_duration_exceeded".to_owned()),
        AccountRiskDirective::ClosePosition { .. } => None,
    })
}

/// Feeds one observed price plus the owner's tracked position, paper equity,
/// and realized cycle rate into the pure protection machine.
async fn protection_directive(
    protection: &mut Option<GridProtectionMachine>,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    account: &PaperAccountAuthority,
    operation_lease: Option<&PaperAccountOperationLease>,
    price: Price,
    observed_at: DateTime<Utc>,
) -> Result<GridDirective, GridPaperTaskError> {
    let Some(machine) = protection.as_mut() else {
        return Ok(GridDirective::Continue);
    };
    let operation_lease = operation_lease.ok_or(GridPaperTaskError::RecoveryRequired)?;
    let snapshot = operation_decision_snapshot(account, operation_lease).await?;
    let current_collateral = snapshot
        .available
        .as_decimal()
        .checked_add(snapshot.pending_reserved.as_decimal())
        .and_then(|value| value.checked_add(snapshot.uncertain_reserved.as_decimal()))
        .and_then(|value| value.checked_add(snapshot.committed_exposure.as_decimal()))
        .ok_or(GridPaperTaskError::InvalidRequest)?;
    let position_quantity = match target_instrument_close_plan(&snapshot, config, grid)? {
        Some((Side::Sell, quantity)) => quantity.as_decimal(),
        Some((Side::Buy, quantity)) => Decimal::ZERO
            .checked_sub(quantity.as_decimal())
            .ok_or(GridPaperTaskError::InvalidRequest)?,
        None => Decimal::ZERO,
    };
    let recent_cycles = grid.recent_cycles_at(
        observed_at,
        ChronoDuration::minutes(PROTECTION_APR_WINDOW_MINUTES),
    );
    let cycles_per_hour = Decimal::from(u64::try_from(recent_cycles).unwrap_or(u64::MAX))
        .checked_mul(Decimal::from(6_u32))
        .ok_or(GridPaperTaskError::InvalidRequest)?;
    machine
        .observe(&GridProtectionObservation {
            price,
            observed_at,
            position_quantity,
            current_collateral,
            cycles_per_hour,
        })
        .map_err(GridPaperTaskError::Strategy)
}

/// One durable `grid_protection` fact naming the directive and its reason.
fn protection_record(
    task_id: &str,
    symbol: &str,
    directive: &GridDirective,
    price: Price,
    recorded_at: DateTime<Utc>,
) -> DecisionRecord {
    let mut details = json!({
        "schema_version": PROTECTION_RECORD_SCHEMA_VERSION,
        "task_id": task_id,
        "task_kind": "grid_paper",
        "reason": directive.reason().map(GridProtectionReason::as_str),
        "price": price.to_string(),
    });
    if let GridDirective::Scalp {
        side,
        quantity,
        take_profit_price,
        ..
    } = directive
        && let Some(map) = details.as_object_mut()
    {
        map.insert(
            "side".to_owned(),
            Value::from(match side {
                Side::Buy => "buy",
                Side::Sell => "sell",
            }),
        );
        map.insert("quantity".to_owned(), Value::from(quantity.to_string()));
        map.insert(
            "take_profit_price".to_owned(),
            Value::from(take_profit_price.to_string()),
        );
    }
    DecisionRecord {
        timestamp: recorded_at,
        strategy: PROTECTION_STRATEGY.to_owned(),
        symbol: symbol.to_owned(),
        decision: directive.label().to_owned(),
        details,
    }
}

/// Builds the single reduce-side scalp take-profit operation.
fn build_protection_operation(
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    side: Side,
    quantity: Quantity,
    price: Price,
    operation_sequence: u64,
) -> Result<PaperSingleLegRequest, GridPaperTaskError> {
    let intent = OrderIntent::limit(
        config.exchange.clone(),
        grid.config().symbol.clone(),
        config.market_type,
        side,
        quantity,
        price,
    );
    let batch = ExecutionBatch::planned(vec![intent.clone()])?;
    let reserved_notional = price
        .as_decimal()
        .checked_mul(quantity.as_decimal())
        .map(Money::new)
        .ok_or(GridPaperTaskError::InvalidRequest)?;
    let task_id = format!("{}/op/{operation_sequence:06}", config.task_id);
    let idempotency_key = format!("scalp:{operation_sequence:06}");
    let reservation = PaperReservationRequest::planned(
        task_id,
        idempotency_key,
        batch.id(),
        config.cost_model,
        vec![PaperReservationLeg::from_intent(
            0,
            &intent,
            reserved_notional,
        )?],
    )?;
    PaperSingleLegRequest::new(grid.config().symbol.clone(), batch, reservation)
        .map_err(GridPaperTaskError::Saga)
}

fn build_account_risk_close_operation(
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    side: Side,
    quantity: Quantity,
    price: Price,
    operation_sequence: u64,
) -> Result<PaperSingleLegRequest, GridPaperTaskError> {
    build_target_instrument_close_operation(
        config,
        grid,
        "risk-close",
        side,
        quantity,
        price,
        operation_sequence,
    )
}

const fn marketable_close_price(snapshot: &MarketSnapshot, side: Side) -> Price {
    match side {
        Side::Buy => snapshot.ask(),
        Side::Sell => snapshot.bid(),
    }
}

fn build_target_instrument_close_operation(
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    idempotency_prefix: &str,
    side: Side,
    quantity: Quantity,
    price: Price,
    operation_sequence: u64,
) -> Result<PaperSingleLegRequest, GridPaperTaskError> {
    let mut intent = OrderIntent::limit(
        config.exchange.clone(),
        grid.config().symbol.clone(),
        config.market_type,
        side,
        quantity,
        price,
    );
    intent.reduce_only = true;
    let batch = ExecutionBatch::planned(vec![intent.clone()])?;
    let reserved_notional = price
        .as_decimal()
        .checked_mul(quantity.as_decimal())
        .map(Money::new)
        .ok_or(GridPaperTaskError::InvalidRequest)?;
    let task_id = format!("{}/op/{operation_sequence:06}", config.task_id);
    let idempotency_key = format!("{idempotency_prefix}:{operation_sequence:06}");
    let reservation = PaperReservationRequest::planned(
        task_id,
        idempotency_key,
        batch.id(),
        config.cost_model,
        vec![PaperReservationLeg::from_intent(
            0,
            &intent,
            reserved_notional,
        )?],
    )?;
    PaperSingleLegRequest::new(grid.config().symbol.clone(), batch, reservation)
        .map_err(GridPaperTaskError::Saga)
}

fn target_instrument_close_plan(
    snapshot: &PaperAccountSnapshot,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
) -> Result<Option<(Side, Quantity)>, GridPaperTaskError> {
    let mut buy_quantity = Decimal::ZERO;
    let mut sell_quantity = Decimal::ZERO;
    let mut owner_matched = false;
    let mut foreign_matched = false;
    let owner_prefix = operation_prefix(&config.task_id);
    for lot in &snapshot.open_lots {
        if lot.exchange != config.exchange
            || lot.symbol != grid.config().symbol
            || lot.market_type != config.market_type
        {
            continue;
        }
        let mut reservations = snapshot
            .reservations
            .iter()
            .filter(|reservation| reservation.reservation_id == lot.source_reservation_id);
        let Some(source) = reservations.next() else {
            return Err(GridPaperTaskError::RecoveryRequired);
        };
        if reservations.next().is_some() {
            return Err(GridPaperTaskError::RecoveryRequired);
        }
        if !operation_task_belongs_to_owner(&source.task_id, &owner_prefix) {
            foreign_matched = true;
            continue;
        }
        owner_matched = true;
        match lot.side {
            Side::Buy => {
                buy_quantity = buy_quantity
                    .checked_add(lot.remaining_quantity.as_decimal())
                    .ok_or(GridPaperTaskError::RecoveryRequired)?;
            }
            Side::Sell => {
                sell_quantity = sell_quantity
                    .checked_add(lot.remaining_quantity.as_decimal())
                    .ok_or(GridPaperTaskError::RecoveryRequired)?;
            }
        }
    }
    if owner_matched && foreign_matched {
        // Paper reduce-only settlement consumes instrument lots FIFO. With a
        // foreign lot in the same queue, no order can prove that it will
        // reduce only this owner's exposure.
        return Err(GridPaperTaskError::RecoveryRequired);
    }
    if !owner_matched {
        return Ok(None);
    }
    let buy_open = buy_quantity > Decimal::ZERO;
    let sell_open = sell_quantity > Decimal::ZERO;
    if buy_open && sell_open {
        return Err(GridPaperTaskError::RecoveryRequired);
    }
    if buy_open {
        return Ok(Some((
            Side::Sell,
            Quantity::new(buy_quantity).map_err(|_| GridPaperTaskError::RecoveryRequired)?,
        )));
    }
    if sell_open {
        return Ok(Some((
            Side::Buy,
            Quantity::new(sell_quantity).map_err(|_| GridPaperTaskError::RecoveryRequired)?,
        )));
    }
    Ok(None)
}

fn target_instrument_has_foreign_lots(
    snapshot: &PaperAccountSnapshot,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
) -> Result<bool, GridPaperTaskError> {
    let owner_prefix = operation_prefix(&config.task_id);
    for lot in &snapshot.open_lots {
        if lot.exchange != config.exchange
            || lot.symbol != grid.config().symbol
            || lot.market_type != config.market_type
        {
            continue;
        }
        let mut reservations = snapshot
            .reservations
            .iter()
            .filter(|reservation| reservation.reservation_id == lot.source_reservation_id);
        let Some(source) = reservations.next() else {
            return Err(GridPaperTaskError::RecoveryRequired);
        };
        if reservations.next().is_some() {
            return Err(GridPaperTaskError::RecoveryRequired);
        }
        if !operation_task_belongs_to_owner(&source.task_id, &owner_prefix) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn operation_task_belongs_to_owner(task_id: &str, owner_prefix: &str) -> bool {
    task_id.strip_prefix(owner_prefix).is_some_and(|sequence| {
        !sequence.is_empty() && sequence.bytes().all(|byte| byte.is_ascii_digit())
    })
}

#[allow(clippy::too_many_arguments)]
async fn close_target_instrument_position(
    saga: &DurablePaperSingleLegSaga,
    executor: Arc<dyn GridPaperExecutor>,
    account: &PaperAccountAuthority,
    operation_lease: &PaperAccountOperationLease,
    risk: Option<&AccountRiskAuthority>,
    history: &JsonlHistory,
    status_sender: &watch::Sender<GridPaperTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    market: Option<&MarketSnapshot>,
    observed_at: DateTime<Utc>,
    operation_sequence: &mut u64,
    stop: &mut watch::Receiver<bool>,
    cancel: &mut watch::Receiver<bool>,
    idempotency_prefix: &'static str,
) -> Result<bool, GridPaperTaskError> {
    let snapshot = operation_decision_snapshot(account, operation_lease).await?;
    let Some((side, quantity)) = target_instrument_close_plan(&snapshot, config, grid)? else {
        return Ok(false);
    };
    let market = market.ok_or(GridPaperTaskError::RecoveryRequired)?;
    let price = marketable_close_price(market, side);
    let Some(next_operation) = operation_sequence.checked_add(1) else {
        return Err(GridPaperTaskError::RecoveryRequired);
    };
    let request = build_target_instrument_close_operation(
        config,
        grid,
        idempotency_prefix,
        side,
        quantity,
        price,
        next_operation,
    )?;
    *operation_sequence = next_operation;
    publish_operation_count(status_sender, next_operation);
    publish_forced_close_plan(
        history,
        config,
        grid,
        idempotency_prefix,
        side,
        quantity,
        price,
        next_operation,
        last_recorded_at,
    )
    .await?;
    match run_operation(
        saga,
        executor,
        request,
        stop,
        cancel,
        operation_lease,
        OperationRunPolicy::forced_close(&config.task_id, config.supervisor.shutdown_grace()),
    )
    .await
    {
        OperationOutcome::Terminal(Ok(_), _) => {
            let snapshot = account
                .decision_snapshot()
                .await
                .map_err(GridPaperTaskError::Account)?;
            if target_instrument_close_plan(&snapshot, config, grid)?.is_some() {
                return Err(GridPaperTaskError::RecoveryRequired);
            }
            if let Some(risk) = risk
                && risk
                    .record_position_closed(&config.task_id, observed_at)
                    .await
                    .is_err()
            {
                return Err(GridPaperTaskError::RecoveryRequired);
            }
            Ok(true)
        }
        OperationOutcome::Cancelled(request) | OperationOutcome::TimedOut(request) => {
            retain_cancelled_operation(account, risk, &config.task_id, None, &request, observed_at)
                .await?;
            Err(GridPaperTaskError::RecoveryRequired)
        }
        OperationOutcome::Terminal(Err(_), _)
        | OperationOutcome::RiskInterrupted { .. }
        | OperationOutcome::RiskUnavailable(_) => Err(GridPaperTaskError::RecoveryRequired),
    }
}

/// Deduplication signature so steady-state directives journal once per change.
#[derive(Clone, Debug, PartialEq)]
enum ProtectionSignature {
    Freeze(GridProtectionReason),
    Scalp {
        side: Side,
        quantity: Decimal,
        price: Decimal,
    },
}

fn build_operation(
    config: &GridPaperTaskConfig,
    grid: &VirtualGrid,
    cross: VirtualGridCross,
    reduce_only: bool,
    reservation_price: Price,
    operation_sequence: u64,
    account_risk_admission: Option<(&AccountRiskAuthority, &AccountRiskAdmissionTicket)>,
) -> Result<PaperSingleLegRequest, GridPaperTaskError> {
    let side = match cross.side {
        GridFill::Buy => Side::Buy,
        GridFill::Sell => Side::Sell,
    };
    let mut intent = OrderIntent::limit(
        config.exchange.clone(),
        grid.config().symbol.clone(),
        config.market_type,
        side,
        config.quantity,
        cross.trigger_price,
    );
    intent.reduce_only = reduce_only;
    let batch = ExecutionBatch::planned(vec![intent.clone()])?;
    let reserved_notional = reservation_price
        .as_decimal()
        .checked_mul(config.quantity.as_decimal())
        .map(Money::new)
        .ok_or(GridPaperTaskError::InvalidRequest)?;
    let task_id = format!("{}/op/{operation_sequence:06}", config.task_id);
    let idempotency_key = format!("grid:{operation_sequence:06}");
    let mut reservation = PaperReservationRequest::planned(
        task_id,
        idempotency_key,
        batch.id(),
        config.cost_model,
        vec![PaperReservationLeg::from_intent(
            0,
            &intent,
            reserved_notional,
        )?],
    )?;
    if let Some((risk, ticket)) = account_risk_admission {
        reservation = reservation.with_account_risk_admission(risk.scope_id(), ticket)?;
    }
    PaperSingleLegRequest::new(grid.config().symbol.clone(), batch, reservation)
        .map_err(GridPaperTaskError::Saga)
}

fn conservative_reservation_price(
    event: &MarketDataEvent,
    side: GridFill,
    trigger_price: Price,
) -> Price {
    let MarketDataEvent::Observation(observation) = event else {
        return trigger_price;
    };
    let touch = match side {
        GridFill::Buy => observation.snapshot.ask(),
        GridFill::Sell => observation.snapshot.bid(),
    };
    if touch.as_decimal() > trigger_price.as_decimal() {
        touch
    } else {
        trigger_price
    }
}

async fn stop_owner(
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<GridPaperTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    requested_exit: GridPaperTaskExit,
) -> TaskResult {
    let stopping_at = Utc::now().max(*last_recorded_at);
    let mut stopping = status_sender.borrow().clone();
    stopping.phase = GridPaperTaskPhase::Stopping;
    stopping.sources = vec![source.status()];
    stopping.last_recorded_at = Some(stopping_at);
    history
        .append(&status_record(&stopping, "task_stopping", stopping_at))
        .await
        .map_err(GridPaperTaskError::Journal)?;
    status_sender.send_replace(stopping);
    *last_recorded_at = stopping_at;

    let source_exit = source.stop().await.map_err(GridPaperTaskError::Source)?;
    let exit = if source_exit == MarketSupervisorExit::ShutdownTimedOut {
        GridPaperTaskExit::ShutdownTimedOut
    } else {
        requested_exit
    };
    let stopped_at = Utc::now().max(*last_recorded_at);
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = GridPaperTaskPhase::Stopped;
    stopped.sources = vec![source.status()];
    stopped.last_recorded_at = Some(stopped_at);
    stopped.exit = Some(exit);
    stopped.failure = None;
    history
        .append(&status_record(&stopped, "task_stopped", stopped_at))
        .await
        .map_err(GridPaperTaskError::Journal)?;
    status_sender.send_replace(stopped);
    Ok(exit)
}

async fn fail_owner(
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<GridPaperTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    failure: GridPaperTaskFailure,
    error: GridPaperTaskError,
) -> TaskResult {
    let _ = source.stop().await;
    let failed_at = Utc::now().max(*last_recorded_at);
    let mut failed = status_sender.borrow().clone();
    failed.phase = GridPaperTaskPhase::Failed;
    failed.sources = vec![source.status()];
    failed.last_recorded_at = Some(failed_at);
    failed.exit = None;
    failed.failure = Some(failure);
    if let Err(journal_error) = history
        .append(&status_record(&failed, "task_failed", failed_at))
        .await
    {
        return Err(GridPaperTaskError::Journal(journal_error));
    }
    status_sender.send_replace(failed);
    Err(error)
}

fn classify_saga_error(
    error: PaperSingleLegSagaError,
) -> (GridPaperTaskFailure, GridPaperTaskError) {
    let failure = match &error {
        PaperSingleLegSagaError::RecoveryRequired { .. } => GridPaperTaskFailure::RecoveryRequired,
        PaperSingleLegSagaError::Account(_) => GridPaperTaskFailure::AccountContract,
        PaperSingleLegSagaError::Journal(_) => GridPaperTaskFailure::JournalUnavailable,
        PaperSingleLegSagaError::Execution(_) => GridPaperTaskFailure::ExecutionFailed,
        PaperSingleLegSagaError::Incomplete(_) => GridPaperTaskFailure::ExecutionIncomplete,
        PaperSingleLegSagaError::InvalidRequest(_) => GridPaperTaskFailure::InvalidRequest,
    };
    (failure, GridPaperTaskError::Saga(error))
}

async fn recovery_preflight(
    task_id: &str,
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
) -> Result<u64, GridPaperTaskError> {
    let account_snapshot = account.decision_snapshot().await?;
    if account_snapshot.projection_status != ProjectionStatus::Complete {
        return Err(GridPaperTaskError::RecoveryRequired);
    }
    if account_snapshot.reservations.iter().any(|reservation| {
        reservation
            .reconciliation
            .as_ref()
            .is_some_and(|record| record.outcome == PaperReconciliationOutcome::Failed)
    }) {
        return Err(GridPaperTaskError::RecoveryRequired);
    }

    let prefix = operation_prefix(task_id);
    let mut last_operation = 0_u64;
    for reservation in account_snapshot
        .reservations
        .iter()
        .filter(|reservation| operation_task_belongs_to_owner(&reservation.task_id, &prefix))
    {
        if matches!(
            reservation.phase,
            PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
        ) {
            return Err(GridPaperTaskError::RecoveryRequired);
        }
        let suffix = reservation
            .task_id
            .strip_prefix(&prefix)
            .unwrap_or_default();
        let sequence = suffix
            .parse::<u64>()
            .map_err(|_| GridPaperTaskError::RecoveryRequired)?;
        last_operation = last_operation.max(sequence);
    }

    if let Some(task) = durable_task_view(account, history.path(), task_id).await?
        && (task.phase != ReadOnlyTaskPhase::Stopped || task.recovery != ReadOnlyTaskRecovery::None)
    {
        return Err(GridPaperTaskError::RecoveryRequired);
    }
    Ok(last_operation)
}

async fn durable_task_view(
    account: &PaperAccountAuthority,
    path: &Path,
    task_id: &str,
) -> Result<Option<ReadOnlyTaskView>, GridPaperTaskError> {
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
    .map_err(|_| GridPaperTaskError::SnapshotTaskFailed)??;
    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot)?;
    if model.projection_status != ProjectionStatus::Complete {
        return Err(GridPaperTaskError::RecoveryRequired);
    }
    Ok(model.tasks.into_iter().find(|task| task.task_id == task_id))
}

fn operation_prefix(task_id: &str) -> String {
    format!("{task_id}/op/")
}

fn publish_operation_count(
    status_sender: &watch::Sender<GridPaperTaskStatus>,
    operation_count: u64,
) {
    let mut status = status_sender.borrow().clone();
    status.operation_count = operation_count;
    status_sender.send_replace(status);
}

fn registered_record(task_id: &str, source_id: &str, recorded_at: DateTime<Utc>) -> DecisionRecord {
    lifecycle_record(
        task_id,
        "task_registered",
        "registered",
        0,
        &Value::Array(vec![placeholder_source_value(source_id)]),
        None,
        None,
        recorded_at,
    )
}

fn status_record(
    status: &GridPaperTaskStatus,
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
            "task_kind": "grid_paper",
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
        "source_id": status.source_id,
        "phase": source_phase_label(status.phase),
        "health": source_health_label(status.health),
        "event_sequence": status.event_sequence,
        "dropped_event_count": status.dropped_event_count,
        "consecutive_source_failures": status.consecutive_source_failures,
        "last_event_at": status.last_event_at,
        "exit": status.exit.map(source_exit_label),
    })
}

const fn task_phase_label(phase: GridPaperTaskPhase) -> &'static str {
    match phase {
        GridPaperTaskPhase::Running => "running",
        GridPaperTaskPhase::Stopping => "stopping",
        GridPaperTaskPhase::Stopped => "stopped",
        GridPaperTaskPhase::Failed => "failed",
    }
}

const fn task_exit_label(exit: GridPaperTaskExit) -> &'static str {
    match exit {
        GridPaperTaskExit::StopRequested => "stop_requested",
        GridPaperTaskExit::SourceEnded => "source_ended",
        GridPaperTaskExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

const fn task_failure_label(failure: GridPaperTaskFailure) -> &'static str {
    match failure {
        GridPaperTaskFailure::StartupFailed => "startup_failed",
        GridPaperTaskFailure::SourceContract => "source_contract",
        GridPaperTaskFailure::JournalUnavailable => "journal_unavailable",
        GridPaperTaskFailure::TaskPanicked => "task_panicked",
        GridPaperTaskFailure::TaskCancelled => "task_cancelled",
        GridPaperTaskFailure::InvalidRequest => "invalid_request",
        GridPaperTaskFailure::RecoveryRequired => "recovery_required",
        GridPaperTaskFailure::AccountContract => "account_contract",
        GridPaperTaskFailure::ExecutionIncomplete => "execution_incomplete",
        GridPaperTaskFailure::ExecutionFailed => "execution_failed",
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

fn safe_identity(value: &str) -> bool {
    value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
    })
}

/// Typed owner failures.
#[derive(Debug)]
pub enum GridPaperTaskError {
    InvalidConfig,
    InvalidSourceBinding,
    InvalidRequest,
    RecoveryRequired,
    ShutdownTimedOut,
    SnapshotTaskFailed,
    Journal(HistoryError),
    JournalRead(JournalReadError),
    Projection(ReadModelError),
    Account(PaperAccountError),
    AccountRisk(AccountRiskError),
    Source(MarketSupervisorError),
    Strategy(StrategyError),
    Runtime(RuntimeError),
    Saga(PaperSingleLegSagaError),
    TaskPanicked,
    TaskCancelled,
    PreviouslyFailed(GridPaperTaskFailure),
}

impl GridPaperTaskError {
    const fn failure_bucket(&self) -> GridPaperTaskFailure {
        match self {
            Self::InvalidConfig | Self::InvalidSourceBinding | Self::InvalidRequest => {
                GridPaperTaskFailure::InvalidRequest
            }
            Self::RecoveryRequired | Self::ShutdownTimedOut => {
                GridPaperTaskFailure::RecoveryRequired
            }
            Self::Journal(_) | Self::JournalRead(_) | Self::Projection(_) => {
                GridPaperTaskFailure::JournalUnavailable
            }
            Self::Account(_) | Self::AccountRisk(_) => GridPaperTaskFailure::AccountContract,
            Self::Source(_) => GridPaperTaskFailure::SourceContract,
            Self::Strategy(_) | Self::Runtime(_) => GridPaperTaskFailure::InvalidRequest,
            Self::Saga(error) => classify_saga_error_ref(error),
            Self::TaskPanicked => GridPaperTaskFailure::TaskPanicked,
            Self::TaskCancelled | Self::SnapshotTaskFailed => GridPaperTaskFailure::TaskCancelled,
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

const fn classify_saga_error_ref(error: &PaperSingleLegSagaError) -> GridPaperTaskFailure {
    match error {
        PaperSingleLegSagaError::RecoveryRequired { .. } => GridPaperTaskFailure::RecoveryRequired,
        PaperSingleLegSagaError::Account(_) => GridPaperTaskFailure::AccountContract,
        PaperSingleLegSagaError::Journal(_) => GridPaperTaskFailure::JournalUnavailable,
        PaperSingleLegSagaError::Execution(_) => GridPaperTaskFailure::ExecutionFailed,
        PaperSingleLegSagaError::Incomplete(_) => GridPaperTaskFailure::ExecutionIncomplete,
        PaperSingleLegSagaError::InvalidRequest(_) => GridPaperTaskFailure::InvalidRequest,
    }
}

impl From<PaperAccountError> for GridPaperTaskError {
    fn from(value: PaperAccountError) -> Self {
        Self::Account(value)
    }
}

impl From<PaperAdmissionCompensationError> for GridPaperTaskError {
    fn from(value: PaperAdmissionCompensationError) -> Self {
        match value {
            PaperAdmissionCompensationError::Account(error) => Self::Account(error),
            PaperAdmissionCompensationError::AccountRisk(error) => Self::AccountRisk(error),
            PaperAdmissionCompensationError::RecoveryRequired => Self::RecoveryRequired,
        }
    }
}

impl From<JournalReadError> for GridPaperTaskError {
    fn from(value: JournalReadError) -> Self {
        Self::JournalRead(value)
    }
}

impl From<ReadModelError> for GridPaperTaskError {
    fn from(value: ReadModelError) -> Self {
        Self::Projection(value)
    }
}

impl From<RuntimeError> for GridPaperTaskError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl fmt::Display for GridPaperTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid grid paper task configuration"),
            Self::InvalidSourceBinding => {
                formatter.write_str("grid paper source does not match its exact owner binding")
            }
            Self::InvalidRequest => formatter.write_str("grid paper operation is invalid"),
            Self::RecoveryRequired => {
                formatter.write_str("grid paper durable state requires reconciliation")
            }
            Self::ShutdownTimedOut => {
                formatter.write_str("grid paper shutdown timed out; recovery is required")
            }
            Self::SnapshotTaskFailed => formatter.write_str("grid paper snapshot worker failed"),
            Self::Journal(error) => error.fmt(formatter),
            Self::JournalRead(error) => error.fmt(formatter),
            Self::Projection(error) => error.fmt(formatter),
            Self::Account(error) => error.fmt(formatter),
            Self::AccountRisk(error) => error.fmt(formatter),
            Self::Source(error) => error.fmt(formatter),
            Self::Strategy(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Saga(error) => error.fmt(formatter),
            Self::TaskPanicked => formatter.write_str("grid paper task panicked"),
            Self::TaskCancelled => formatter.write_str("grid paper task was cancelled"),
            Self::PreviouslyFailed(failure) => {
                write!(formatter, "grid paper task already failed: {failure:?}")
            }
        }
    }
}

impl Error for GridPaperTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::JournalRead(error) => Some(error),
            Self::Projection(error) => Some(error),
            Self::Account(error) => Some(error),
            Self::AccountRisk(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::Strategy(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Saga(error) => Some(error),
            Self::InvalidConfig
            | Self::InvalidSourceBinding
            | Self::InvalidRequest
            | Self::RecoveryRequired
            | Self::ShutdownTimedOut
            | Self::SnapshotTaskFailed
            | Self::TaskPanicked
            | Self::TaskCancelled
            | Self::PreviouslyFailed(_) => None,
        }
    }
}
