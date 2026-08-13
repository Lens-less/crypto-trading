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
    future::{Future, pending},
    io::ErrorKind,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        Arc, Mutex as StdMutex,
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
    AccountRiskAdmission, AccountRiskAdmissionTicket, AccountRiskAuthority, AccountRiskCandidate,
    AccountRiskError, DecisionRecord, ExecutionBatch, FileJournalSnapshotSource, HistoryError,
    JournalReadError, JournalSnapshot, JournalSnapshotSource, JsonlHistory,
    MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION, MarketDataError, MarketDataEvent,
    MarketDataEventSource, MarketSupervisor, MarketSupervisorConfig, MarketSupervisorError,
    MarketSupervisorExit, MarketSupervisorHealth, MarketSupervisorPhase, MarketSupervisorStatus,
    ObservedMarketPair, PaperAccountAuthority, PaperAccountError, PaperAccountOperationLease,
    PaperAccountSnapshot, PaperCostModel, PaperReconciliationOutcome, PaperReservationLeg,
    PaperReservationPhase, PaperReservationRequest, ProjectionStatus, ReadModelError,
    ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel, ReadOnlyTaskRecovery,
    ReadOnlyTaskView, RuntimeError, SpreadHistoryReadModel, SpreadHistorySampleView,
    read_journal_chain,
};
use crypto_trading_strategy::{
    AccountRiskSnapshot, ArbitrageDecision, ArbitrageDecisionKind, ArbitrageDirection,
    ArbitrageState, ArbitrageStrategy, HistoryDecisionKind, HistoryDecisionMachine,
    PairStrategyMachine, RiskDecision, RiskEngine, RiskLimits, RiskRejection, SpreadQuote,
    SpreadSample, StrategyError,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};
use uuid::Uuid;

use crate::{
    DurablePaperArbitrageSaga, PaperArbitrageRequest, PaperArbitrageRun, PaperArbitrageSagaError,
    monitor::{
        ArbitrageMonitorError, ArbitrageMonitorEvent, ArbitrageMonitorOutcome,
        ReadOnlyArbitrageMonitor,
    },
    paper_admission::{
        PaperAdmissionCompensationError, discard_planned_admission as discard_shared_admission,
        retain_cancelled_reservation,
    },
    paper_grid_task::{account_risk_directive_record, account_risk_exit_reason},
    task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture},
};

/// Stable version of the process-local arbitrage owner status.
pub const ARBITRAGE_PAPER_TASK_STATUS_SCHEMA_VERSION: u16 = 1;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";
const MAX_TASK_ID_BYTES: usize = 96;
const OPERATION_SUFFIX_BYTES: usize = "/op/00000000000000000000".len();
const ACCOUNT_RISK_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Boxed execution future behind the trusted paper adapter seam.
pub type ArbitragePaperExecutionFuture =
    Pin<Box<dyn Future<Output = Result<Vec<TradingReceipt>, RuntimeError>> + Send + 'static>>;
/// Boxed hook that applies one owner-consumed market event to an execution
/// adapter before the owner evaluates the exact pair against it.
pub type ArbitragePaperMarketEventFuture =
    Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'static>>;

/// Minimal object-safe two-leg execution seam owned by the task process.
pub trait ArbitragePaperExecutor: Send + Sync + 'static {
    /// Applies one complete market event at consumer time.
    ///
    /// Live executors implement an explicit no-op. Replay executors use this
    /// hook to validate global tape order and advance only the monitor clock;
    /// execution books advance later from the frozen pair passed to
    /// [`Self::execute`]. Keeping the method required makes every executor
    /// choose its clock semantics deliberately.
    fn observe_market_event(&self, event: MarketDataEvent) -> ArbitragePaperMarketEventFuture;

    /// Executes against the exact market pair that produced `batch`.
    ///
    /// The owner keeps consuming and coalescing later market events while a
    /// durable saga reserves capital. Carrying the pair through the plan
    /// prevents those later events from changing this operation's execution
    /// context.
    fn execute(
        &self,
        batch: ExecutionBatch,
        pair: ObservedMarketPair,
    ) -> ArbitragePaperExecutionFuture;
}

/// Validated execution, risk, lifecycle, and reservation policy.
#[derive(Clone, Debug)]
pub struct ArbitragePaperTaskConfig {
    task_id: String,
    strategy: ArbitrageStrategy,
    risk: RiskEngine,
    cost_model: PaperCostModel,
    supervisor: MarketSupervisorConfig,
    /// Optional history ("natural spread") gate: when present, opportunities
    /// only become operations after the machine judges `Open`.
    history_decision: Option<HistoryDecisionMachine>,
    /// Optional dedicated spread-history journal used to backfill the
    /// history machine on cold start.
    spread_history_path: Option<PathBuf>,
    /// Optional durable account-level risk authority: opening decisions must
    /// pass its admission and its close directives stop the owner.
    account_risk: Option<AccountRiskAuthority>,
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
        let (history_decision, spread_history_path) = match &arbitrage.history_decision {
            Some(history) if history.enabled => (
                Some(
                    HistoryDecisionMachine::try_from(history)
                        .map_err(ArbitragePaperTaskError::Strategy)?,
                ),
                history.spread_history_path.as_ref().map(PathBuf::from),
            ),
            _ => (None, None),
        };
        Ok(Self {
            task_id: task_id.to_owned(),
            strategy,
            risk,
            cost_model,
            supervisor,
            history_decision,
            spread_history_path,
            account_risk: None,
        })
    }

    /// Attaches the durable account-level risk authority. Opening decisions
    /// must pass its admission before any reservation is created; its close
    /// directives stop the owner fail-closed.
    #[must_use]
    pub fn with_account_risk(mut self, account_risk: AccountRiskAuthority) -> Self {
        self.account_risk = Some(account_risk);
        self
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
    active_operation_lease: ActiveOperationLease,
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
        mut config: ArbitragePaperTaskConfig,
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
        if config.account_risk.is_some() {
            account.ensure_initialized().await?;
        }

        if let Some(path) = config.spread_history_path.clone()
            && let Some(machine) = config.history_decision.as_mut()
        {
            backfill_history_machine(machine, &path).await?;
        }

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
        let active_operation_lease = Arc::new(StdMutex::new(None));
        let task_active_operation_lease = Arc::clone(&active_operation_lease);
        let join = tokio::spawn(async move {
            Box::pin(run_owner(
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
                task_active_operation_lease,
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
            shutdown_grace: config.supervisor.shutdown_grace(),
            active_operation_lease,
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
        if let Some(operation_lease) = clone_active_operation_lease(&self.active_operation_lease) {
            self.retain_active_capacity(Some(operation_lease)).await;
        }
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
            let result = Self::map_join(joined);
            if let Some(operation_lease) =
                clone_active_operation_lease(&self.active_operation_lease)
            {
                self.retain_active_capacity(Some(operation_lease)).await;
            }
            result
        } else {
            // Clone the registered lease before aborting the owner. Its
            // in-flight Drop only signals child cancellation; this clone keeps
            // the lane closed through external Pending -> Uncertain retention.
            let operation_lease = clone_active_operation_lease(&self.active_operation_lease);
            join.abort();
            let _ = join.await;
            self.retain_active_capacity(operation_lease).await;
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

    async fn retain_active_capacity(&self, operation_lease: Option<PaperAccountOperationLease>) {
        let operation_lease = match operation_lease
            .or_else(|| clone_active_operation_lease(&self.active_operation_lease))
        {
            Some(operation_lease) => operation_lease,
            None => self.account.acquire_operation_lease().await,
        };
        let Ok(snapshot) = account_decision_snapshot(&self.account).await else {
            return;
        };
        let prefix = operation_prefix(&self.status().task_id);
        for reservation in snapshot.reservations.iter().filter(|reservation| {
            owner_operation_sequence(&reservation.task_id, &prefix).is_some()
                && reservation.phase == PaperReservationPhase::Pending
        }) {
            if self
                .account
                .mark_uncertain(reservation.reservation_id)
                .await
                .is_err()
            {
                return;
            }
        }
        clear_active_operation_lease(&self.active_operation_lease);
        drop(operation_lease);
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
type OperationRunResult = Result<PaperArbitrageRun, PaperArbitrageSagaError>;
type OperationJoinResult = Result<(OperationRunResult, PaperAccountOperationLease), JoinError>;
type ActiveOperationLease = Arc<StdMutex<Option<PaperAccountOperationLease>>>;

fn register_active_operation_lease(
    active: &ActiveOperationLease,
    lease: &PaperAccountOperationLease,
) -> Result<(), ArbitragePaperTaskError> {
    let mut slot = active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if slot.is_some() {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    *slot = Some(lease.clone());
    Ok(())
}

fn clone_active_operation_lease(
    active: &ActiveOperationLease,
) -> Option<PaperAccountOperationLease> {
    active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn clear_active_operation_lease(active: &ActiveOperationLease) {
    *active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

#[derive(Debug)]
struct InFlightOperation {
    request: PaperArbitrageRequest,
    decision: ArbitrageDecision,
    admission_ticket: Option<AccountRiskAdmissionTicket>,
    execution_started: Arc<AtomicBool>,
    join: Option<JoinHandle<(OperationRunResult, PaperAccountOperationLease)>>,
}

impl InFlightOperation {
    fn join_mut(&mut self) -> &mut JoinHandle<(OperationRunResult, PaperAccountOperationLease)> {
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
    pair: ObservedMarketPair,
    admission_ticket: Option<AccountRiskAdmissionTicket>,
    operation_lease: PaperAccountOperationLease,
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
    active_operation_lease: ActiveOperationLease,
) -> TaskResult {
    let mut state = match restore_state_from_account(saga.account(), &config.task_id).await {
        Ok(state) => state,
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
    };
    let mut in_flight: Option<InFlightOperation> = None;
    let mut pending_reevaluation: Option<PlanScope> = None;
    let mut history_machine = config.history_decision.clone();
    let mut latest_history_sample: Option<SpreadSample> = None;
    let mut last_exact_pair: Option<ObservedMarketPair> = None;
    let mut account_risk_poll = config.account_risk.as_ref().map(|_| {
        let mut interval = tokio::time::interval(ACCOUNT_RISK_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    });

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
                () = async {
                    if let Some(interval) = account_risk_poll.as_mut() {
                        interval.tick().await;
                    } else {
                        pending::<()>().await;
                    }
                } => Selected::AccountRiskPoll,
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
                () = async {
                    if let Some(interval) = account_risk_poll.as_mut() {
                        interval.tick().await;
                    } else {
                        pending::<()>().await;
                    }
                } => Selected::AccountRiskPoll,
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
                    config.account_risk.as_ref(),
                    &config.task_id,
                    config.cost_model,
                    config.supervisor.shutdown_grace(),
                    &saga,
                    Arc::clone(&executor),
                    None,
                    &mut operation_sequence,
                    &mut state,
                    &active_operation_lease,
                )
                .await;
            }
            Selected::AccountRiskPoll => {
                let Some(risk) = config.account_risk.as_ref() else {
                    continue;
                };
                let directive_decision = match handle_account_risk_directive(
                    risk,
                    &config.task_id,
                    monitor.legs().0.symbol.as_str(),
                    &history,
                    Utc::now(),
                    &mut last_recorded_at,
                    &state,
                    last_exact_pair.clone(),
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        if let Err(abort_error) = abort_inflight(
                            &mut in_flight,
                            saga.account(),
                            config.account_risk.as_ref(),
                            &config.task_id,
                        )
                        .await
                        {
                            let failure = abort_error.failure_bucket();
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                abort_error,
                            )
                            .await;
                        }
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
                };
                let Some(directive_close_pair) = directive_decision else {
                    continue;
                };
                return stop_owner(
                    &mut left,
                    &mut right,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ArbitragePaperTaskExit::StopRequested,
                    in_flight.take(),
                    false,
                    saga.account(),
                    config.account_risk.as_ref(),
                    &config.task_id,
                    config.cost_model,
                    config.supervisor.shutdown_grace(),
                    &saga,
                    Arc::clone(&executor),
                    directive_close_pair,
                    &mut operation_sequence,
                    &mut state,
                    &active_operation_lease,
                )
                .await;
            }
            Selected::Operation(result) => {
                let operation = in_flight
                    .take()
                    .ok_or(ArbitragePaperTaskError::TaskCancelled)?;
                let operation_lease =
                    match complete_operation(result, &operation.decision, &mut state) {
                        Ok(operation_lease) => operation_lease,
                        Err(operation_error) => {
                            let error = match retain_cancelled_operation(
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                                operation.admission_ticket.as_ref(),
                                &operation.request,
                                Utc::now(),
                            )
                            .await
                            {
                                Ok(false) => operation_error,
                                Ok(true) => ArbitragePaperTaskError::RecoveryRequired,
                                Err(error) => error,
                            };
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
                    };
                // Settlement is durable and any cancellation compensation is
                // complete, so the next coalesced plan may take the account
                // operation lane without recursively waiting on this owner.
                drop(operation);
                // A flat strategy position closes the owner-level risk clock.
                if let Some(risk) = config.account_risk.as_ref()
                    && state.position_quantity.is_zero()
                    && let Err(error) = risk
                        .record_position_closed(&config.task_id, Utc::now())
                        .await
                {
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        ArbitragePaperTaskFailure::AccountContract,
                        ArbitragePaperTaskError::AccountRisk(error),
                    )
                    .await;
                }
                clear_active_operation_lease(&active_operation_lease);
                drop(operation_lease);
                if let Some(scope) = pending_reevaluation.take()
                    && !*stop.borrow()
                    && !*cancel.borrow()
                {
                    let gate_open = match history_gate(
                        history_machine.as_ref(),
                        latest_history_sample.as_ref(),
                    ) {
                        Ok(open) => open,
                        Err(error) => {
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                ArbitragePaperTaskFailure::InvalidRequest,
                                ArbitragePaperTaskError::Strategy(error),
                            )
                            .await;
                        }
                    };
                    if !gate_open {
                        continue;
                    }
                    match plan_latest_operation(
                        &config,
                        &monitor,
                        saga.account(),
                        &state,
                        scope,
                        &mut operation_sequence,
                        &active_operation_lease,
                    )
                    .await
                    {
                        Ok(Some(planned)) if !*stop.borrow() && !*cancel.borrow() => {
                            in_flight =
                                Some(start_operation(&saga, Arc::clone(&executor), planned));
                            publish_operation_count(&status_sender, operation_sequence);
                        }
                        Ok(Some(planned)) => {
                            if let Err(error) = discard_planned_admission(
                                config.account_risk.as_ref(),
                                &config.task_id,
                                planned.admission_ticket.as_ref(),
                                Utc::now(),
                            )
                            .await
                            {
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
                        Ok(None) => {}
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
                if let Err(error) = executor.observe_market_event(event.clone()).await {
                    if let Err(abort_error) = abort_inflight(
                        &mut in_flight,
                        saga.account(),
                        config.account_risk.as_ref(),
                        &config.task_id,
                    )
                    .await
                    {
                        let failure = abort_error.failure_bucket();
                        return fail_owner(
                            &mut left,
                            &mut right,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            failure,
                            abort_error,
                        )
                        .await;
                    }
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        ArbitragePaperTaskFailure::ExecutionFailed,
                        ArbitragePaperTaskError::Runtime(error),
                    )
                    .await;
                }
                let monitor_event = match monitor.process(event) {
                    Ok(event) => event,
                    Err(error) => {
                        if let Err(abort_error) = abort_inflight(
                            &mut in_flight,
                            saga.account(),
                            config.account_risk.as_ref(),
                            &config.task_id,
                        )
                        .await
                        {
                            let failure = abort_error.failure_bucket();
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                abort_error,
                            )
                            .await;
                        }
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
                if let Some(pair) = current_exact_pair(&monitor) {
                    last_exact_pair = Some(pair);
                }
                let is_opportunity = matches!(
                    monitor_event.outcome,
                    ArbitrageMonitorOutcome::Opportunity { .. }
                );
                // A converged (no-opportunity) update must also reach the
                // strategy so an open position can plan its reduce-only close;
                // opening stays gated on the monitor's opportunity verdict.
                // Waiting and rejected outcomes carry no coherent pair and
                // stay inert.
                let event_plan_scope = if is_opportunity {
                    Some(PlanScope::Full)
                } else if matches!(
                    monitor_event.outcome,
                    ArbitrageMonitorOutcome::NoOpportunity { .. }
                ) {
                    Some(PlanScope::ReduceOnly)
                } else {
                    None
                };
                if history_machine.is_some() {
                    match history_sample_of(&monitor_event) {
                        Ok(Some(sample)) => {
                            if let Some(machine) = history_machine.as_mut()
                                && let Err(error) = machine.observe(sample.clone())
                            {
                                if let Err(abort_error) = abort_inflight(
                                    &mut in_flight,
                                    saga.account(),
                                    config.account_risk.as_ref(),
                                    &config.task_id,
                                )
                                .await
                                {
                                    let failure = abort_error.failure_bucket();
                                    return fail_owner(
                                        &mut left,
                                        &mut right,
                                        &history,
                                        &status_sender,
                                        &mut last_recorded_at,
                                        failure,
                                        abort_error,
                                    )
                                    .await;
                                }
                                return fail_owner(
                                    &mut left,
                                    &mut right,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    ArbitragePaperTaskFailure::InvalidRequest,
                                    ArbitragePaperTaskError::Strategy(error),
                                )
                                .await;
                            }
                            latest_history_sample = Some(sample);
                        }
                        Ok(None) => {}
                        Err(()) => {
                            if let Err(abort_error) = abort_inflight(
                                &mut in_flight,
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                            )
                            .await
                            {
                                let failure = abort_error.failure_bucket();
                                return fail_owner(
                                    &mut left,
                                    &mut right,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    abort_error,
                                )
                                .await;
                            }
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
                    }
                }
                let mut next = status_sender.borrow().clone();
                next.processed_event_count =
                    if let Some(value) = next.processed_event_count.checked_add(1) {
                        value
                    } else {
                        if let Err(abort_error) = abort_inflight(
                            &mut in_flight,
                            saga.account(),
                            config.account_risk.as_ref(),
                            &config.task_id,
                        )
                        .await
                        {
                            let failure = abort_error.failure_bucket();
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                failure,
                                abort_error,
                            )
                            .await;
                        }
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
                if let Some(scope) = event_plan_scope
                    && in_flight.is_some()
                {
                    pending_reevaluation = Some(match pending_reevaluation {
                        Some(PlanScope::Full) => PlanScope::Full,
                        Some(PlanScope::ReduceOnly) | None => scope,
                    });
                }
                if is_opportunity && in_flight.is_some() {
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
                    if let Err(abort_error) = abort_inflight(
                        &mut in_flight,
                        saga.account(),
                        config.account_risk.as_ref(),
                        &config.task_id,
                    )
                    .await
                    {
                        let failure = abort_error.failure_bucket();
                        return fail_owner(
                            &mut left,
                            &mut right,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            failure,
                            abort_error,
                        )
                        .await;
                    }
                    let _ = tokio::join!(left.stop(), right.stop());
                    publish_runtime_failure(
                        &status_sender,
                        ArbitragePaperTaskFailure::JournalUnavailable,
                    );
                    return Err(ArbitragePaperTaskError::Journal(error));
                }
                last_recorded_at = recorded_at;
                status_sender.send_replace(next);

                // Durable account-risk close directives stop the owner
                // fail-closed before any further opportunity is planned.
                if let Some(risk) = config.account_risk.as_ref() {
                    let directive_decision = match handle_account_risk_directive(
                        risk,
                        &config.task_id,
                        monitor.legs().0.symbol.as_str(),
                        &history,
                        monitor_event.recorded_at,
                        &mut last_recorded_at,
                        &state,
                        last_exact_pair.clone(),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            if let Err(abort_error) = abort_inflight(
                                &mut in_flight,
                                saga.account(),
                                config.account_risk.as_ref(),
                                &config.task_id,
                            )
                            .await
                            {
                                let failure = abort_error.failure_bucket();
                                return fail_owner(
                                    &mut left,
                                    &mut right,
                                    &history,
                                    &status_sender,
                                    &mut last_recorded_at,
                                    failure,
                                    abort_error,
                                )
                                .await;
                            }
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
                    };
                    if let Some(directive_close_pair) = directive_decision {
                        return stop_owner(
                            &mut left,
                            &mut right,
                            &history,
                            &status_sender,
                            &mut last_recorded_at,
                            ArbitragePaperTaskExit::StopRequested,
                            in_flight.take(),
                            false,
                            saga.account(),
                            config.account_risk.as_ref(),
                            &config.task_id,
                            config.cost_model,
                            config.supervisor.shutdown_grace(),
                            &saga,
                            Arc::clone(&executor),
                            directive_close_pair,
                            &mut operation_sequence,
                            &mut state,
                            &active_operation_lease,
                        )
                        .await;
                    }
                }

                if let Some(scope) = event_plan_scope
                    && in_flight.is_none()
                    && !*stop.borrow()
                    && !*cancel.borrow()
                {
                    let gate_open = match history_gate(
                        history_machine.as_ref(),
                        latest_history_sample.as_ref(),
                    ) {
                        Ok(open) => open,
                        Err(error) => {
                            return fail_owner(
                                &mut left,
                                &mut right,
                                &history,
                                &status_sender,
                                &mut last_recorded_at,
                                ArbitragePaperTaskFailure::InvalidRequest,
                                ArbitragePaperTaskError::Strategy(error),
                            )
                            .await;
                        }
                    };
                    if !gate_open {
                        continue;
                    }
                    match plan_latest_operation(
                        &config,
                        &monitor,
                        saga.account(),
                        &state,
                        scope,
                        &mut operation_sequence,
                        &active_operation_lease,
                    )
                    .await
                    {
                        Ok(Some(planned)) if !*stop.borrow() && !*cancel.borrow() => {
                            in_flight =
                                Some(start_operation(&saga, Arc::clone(&executor), planned));
                            publish_operation_count(&status_sender, operation_sequence);
                        }
                        Ok(Some(planned)) => {
                            if let Err(error) = discard_planned_admission(
                                config.account_risk.as_ref(),
                                &config.task_id,
                                planned.admission_ticket.as_ref(),
                                Utc::now(),
                            )
                            .await
                            {
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
                        Ok(None) => {}
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
                    config.account_risk.as_ref(),
                    &config.task_id,
                    config.cost_model,
                    config.supervisor.shutdown_grace(),
                    &saga,
                    Arc::clone(&executor),
                    None,
                    &mut operation_sequence,
                    &mut state,
                    &active_operation_lease,
                )
                .await;
            }
            Selected::Left(Err(error)) | Selected::Right(Err(error)) => {
                if let Err(abort_error) = abort_inflight(
                    &mut in_flight,
                    saga.account(),
                    config.account_risk.as_ref(),
                    &config.task_id,
                )
                .await
                {
                    let failure = abort_error.failure_bucket();
                    return fail_owner(
                        &mut left,
                        &mut right,
                        &history,
                        &status_sender,
                        &mut last_recorded_at,
                        failure,
                        abort_error,
                    )
                    .await;
                }
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
    AccountRiskPoll,
    Left(Result<Option<MarketDataEvent>, MarketSupervisorError>),
    Right(Result<Option<MarketDataEvent>, MarketSupervisorError>),
    Operation(OperationJoinResult),
}

fn current_exact_pair(monitor: &ReadOnlyArbitrageMonitor) -> Option<ObservedMarketPair> {
    let (left_leg, right_leg) = monitor.legs();
    monitor.book().current_pair(left_leg, right_leg).ok()
}

#[allow(clippy::too_many_arguments)]
async fn handle_account_risk_directive(
    risk: &AccountRiskAuthority,
    task_id: &str,
    symbol: &str,
    history: &JsonlHistory,
    observed_at: DateTime<Utc>,
    last_recorded_at: &mut DateTime<Utc>,
    state: &ArbitrageState,
    last_exact_pair: Option<ObservedMarketPair>,
) -> Result<Option<Option<ObservedMarketPair>>, ArbitragePaperTaskError> {
    let directives = risk
        .directives(observed_at)
        .await
        .map_err(ArbitragePaperTaskError::AccountRisk)?;
    let Some(reason) = account_risk_exit_reason(&directives, task_id) else {
        return Ok(None);
    };
    let directive_recorded_at = Utc::now().max(*last_recorded_at);
    history
        .append(&account_risk_directive_record(
            task_id,
            "arbitrage_paper",
            symbol,
            &reason,
            "",
            directive_recorded_at,
        ))
        .await
        .map_err(ArbitragePaperTaskError::Journal)?;
    *last_recorded_at = directive_recorded_at;
    if !state.position_quantity.is_zero() && last_exact_pair.is_none() {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    // Preserve the exact cached pair even when the pre-operation state is
    // flat. An opening saga may already be executing while `state` still
    // reflects its pre-operation position; shutdown reprojects the durable
    // account after draining that saga and uses this pair to close any raced
    // exposure.
    Ok(Some(last_exact_pair))
}

/// Bounds which strategy decisions a planning pass may turn into an
/// operation. Opening exposure stays gated on the monitor's opportunity
/// verdict, while a converged (no-opportunity) update may still plan the
/// reduce-only close of an open position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanScope {
    /// Opportunity-triggered pass: any strategy decision may execute.
    Full,
    /// Convergence-triggered pass: only reducing decisions may execute.
    ReduceOnly,
}

async fn plan_latest_operation(
    config: &ArbitragePaperTaskConfig,
    monitor: &ReadOnlyArbitrageMonitor,
    account: &PaperAccountAuthority,
    state: &ArbitrageState,
    scope: PlanScope,
    operation_sequence: &mut u64,
    active_operation_lease: &ActiveOperationLease,
) -> Result<Option<PlannedOperation>, ArbitragePaperTaskError> {
    let (left_leg, right_leg) = monitor.legs();
    let pair = monitor.book().current_pair(left_leg, right_leg)?;
    let decision = config
        .strategy
        .evaluate_pair(state, &pair.left, &pair.right)?;
    if decision.intents.is_empty() {
        return Ok(None);
    }
    if scope == PlanScope::ReduceOnly && decision.kind != ArbitrageDecisionKind::Reduce {
        return Ok(None);
    }
    // Serialize the complete account operation lane before admission or any
    // account-derived decision. The lease follows the plan into the in-flight
    // saga and is released only after settlement or cancellation retention.
    let operation_lease = account.acquire_operation_lease().await;
    register_active_operation_lease(active_operation_lease, &operation_lease)?;
    let result = async {
        // Opening exposure passes the durable account-level admission before any
        // reservation exists; a recorded rejection skips the opportunity while
        // reducing decisions stay exempt so risk can always be closed out.
        let admission_ticket = match admit_planned_operation(config, &pair, &decision).await? {
            PlannedOperationAdmission::Proceed(ticket) => ticket,
            PlannedOperationAdmission::Rejected => return Ok(None),
        };
        let account_snapshot = match account_decision_snapshot(account).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                discard_planned_admission(
                    config.account_risk.as_ref(),
                    &config.task_id,
                    admission_ticket.as_ref(),
                    pair.observed_at,
                )
                .await?;
                return Err(error);
            }
        };
        let Some(next_sequence) = operation_sequence.checked_add(1) else {
            discard_planned_admission(
                config.account_risk.as_ref(),
                &config.task_id,
                admission_ticket.as_ref(),
                pair.observed_at,
            )
            .await?;
            return Err(ArbitragePaperTaskError::InvalidRequest);
        };
        let request = match build_operation(
            config,
            &pair,
            state,
            &account_snapshot,
            &decision,
            admission_ticket.as_ref(),
            next_sequence,
        ) {
            Ok(request) => request,
            Err(error) => {
                discard_planned_admission(
                    config.account_risk.as_ref(),
                    &config.task_id,
                    admission_ticket.as_ref(),
                    pair.observed_at,
                )
                .await?;
                return Err(error);
            }
        };
        *operation_sequence = next_sequence;
        Ok(Some(PlannedOperation {
            request,
            decision,
            pair,
            admission_ticket,
            operation_lease,
        }))
    };
    let result = result.await;
    if !matches!(result.as_ref(), Ok(Some(_))) {
        clear_active_operation_lease(active_operation_lease);
    }
    result
}

enum PlannedOperationAdmission {
    Proceed(Option<AccountRiskAdmissionTicket>),
    Rejected,
}

async fn admit_planned_operation(
    config: &ArbitragePaperTaskConfig,
    pair: &ObservedMarketPair,
    decision: &ArbitrageDecision,
) -> Result<PlannedOperationAdmission, ArbitragePaperTaskError> {
    let Some(risk) = config.account_risk.as_ref() else {
        return Ok(PlannedOperationAdmission::Proceed(None));
    };
    if !matches!(
        decision.kind,
        ArbitrageDecisionKind::Open | ArbitrageDecisionKind::Increase
    ) {
        return Ok(PlannedOperationAdmission::Proceed(None));
    }
    let markets = [pair.left.clone(), pair.right.clone()];
    let mut notional = Decimal::ZERO;
    for intent in &decision.intents {
        let market = matching_market(intent, &markets)?;
        let execution_price = intent.price.unwrap_or_else(|| match intent.side {
            Side::Buy => market.ask(),
            Side::Sell => market.bid(),
        });
        notional = execution_price
            .as_decimal()
            .checked_mul(intent.quantity.as_decimal())
            .and_then(|value| notional.checked_add(value))
            .ok_or(ArbitragePaperTaskError::InvalidRequest)?;
    }
    let candidate = AccountRiskCandidate::new(
        config.task_id.clone(),
        decision.intents[0].symbol.as_str(),
        Money::new(notional),
    )
    .map_err(ArbitragePaperTaskError::AccountRisk)?;
    match risk
        .admit(&candidate, pair.observed_at)
        .await
        .map_err(ArbitragePaperTaskError::AccountRisk)?
    {
        AccountRiskAdmission::Admitted { ticket, .. } => {
            Ok(PlannedOperationAdmission::Proceed(Some(ticket)))
        }
        AccountRiskAdmission::Rejected(_) => Ok(PlannedOperationAdmission::Rejected),
    }
}

/// Applies the optional history ("natural spread") gate: without the mode
/// every opportunity passes; with the mode an opportunity becomes an
/// operation only after the machine judges `Open`. `InsufficientHistory`
/// and `Hold` both refuse to trade (fail closed, no order).
fn history_gate(
    machine: Option<&HistoryDecisionMachine>,
    latest: Option<&SpreadSample>,
) -> Result<bool, StrategyError> {
    match (machine, latest) {
        (None, _) => Ok(true),
        (Some(_), None) => Ok(false),
        (Some(machine), Some(sample)) => Ok(matches!(
            machine.evaluate(sample)?.kind,
            HistoryDecisionKind::Open
        )),
    }
}

/// Projects one spread-bearing monitor outcome into a strategy spread sample.
/// Waiting and rejected outcomes carry no spread and yield `Ok(None)`.
/// Funding fields stay `None`: no wired market-data source publishes funding
/// rates yet, so history decisions run funding-degraded.
fn history_sample_of(event: &ArbitrageMonitorEvent) -> Result<Option<SpreadSample>, ()> {
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
    Ok(Some(SpreadSample {
        timestamp: event.recorded_at,
        buy_exchange: buy_exchange.clone(),
        sell_exchange: sell_exchange.clone(),
        buy_price: buy_price.as_decimal(),
        sell_price: sell_price.as_decimal(),
        spread_bps,
        funding_rate_buy: None,
        funding_rate_sell: None,
    }))
}

/// Cold-start backfill: replays the bounded recent window of the dedicated
/// spread-history chain into the history machine. A missing journal is an
/// empty history; a degraded or corrupted projection fails closed.
async fn backfill_history_machine(
    machine: &mut HistoryDecisionMachine,
    path: &Path,
) -> Result<(), ArbitragePaperTaskError> {
    let chain_path = path.to_owned();
    let bytes = tokio::task::spawn_blocking(move || match read_journal_chain(&chain_path) {
        Ok(bytes) => Ok(bytes),
        Err(JournalReadError::Open(source)) if source.kind() == ErrorKind::NotFound => {
            Ok(Vec::new())
        }
        Err(error) => Err(error),
    })
    .await
    .map_err(|_| ArbitragePaperTaskError::SnapshotTaskFailed)?
    .map_err(ArbitragePaperTaskError::JournalRead)?;
    if bytes.is_empty() {
        return Ok(());
    }
    let snapshot = JournalSnapshot::new(Uuid::new_v4(), bytes)?;
    let model = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot)?;
    if model.projection_status != ProjectionStatus::Complete {
        // Corrupted or partially readable spread history must not silently
        // seed the machine.
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    let Some(newest) = model.samples.last().map(|sample| sample.timestamp) else {
        return Ok(());
    };
    let mut window: Vec<&SpreadHistorySampleView> =
        model.recent_window(newest, machine.config().window);
    window.sort_by_key(|sample| sample.timestamp);
    for view in window {
        machine
            .observe(history_sample_from_view(view)?)
            .map_err(ArbitragePaperTaskError::Strategy)?;
    }
    Ok(())
}

fn history_sample_from_view(
    view: &SpreadHistorySampleView,
) -> Result<SpreadSample, ArbitragePaperTaskError> {
    fn parse(text: &str) -> Result<Decimal, ArbitragePaperTaskError> {
        Decimal::from_str(text).map_err(|_| ArbitragePaperTaskError::RecoveryRequired)
    }
    fn parse_optional(text: Option<&str>) -> Result<Option<Decimal>, ArbitragePaperTaskError> {
        text.map(parse).transpose()
    }
    Ok(SpreadSample {
        timestamp: view.timestamp,
        buy_exchange: view.exchange_buy.clone(),
        sell_exchange: view.exchange_sell.clone(),
        buy_price: parse(&view.price_buy)?,
        sell_price: parse(&view.price_sell)?,
        spread_bps: parse(&view.spread_bps)?,
        funding_rate_buy: parse_optional(view.funding_rate_buy.as_deref())?,
        funding_rate_sell: parse_optional(view.funding_rate_sell.as_deref())?,
    })
}

fn build_operation(
    config: &ArbitragePaperTaskConfig,
    pair: &ObservedMarketPair,
    state: &ArbitrageState,
    account: &PaperAccountSnapshot,
    decision: &ArbitrageDecision,
    admission_ticket: Option<&AccountRiskAdmissionTicket>,
    operation_sequence: u64,
) -> Result<PaperArbitrageRequest, ArbitragePaperTaskError> {
    if account.projection_status != ProjectionStatus::Complete
        || account.reservations.iter().any(|reservation| {
            matches!(
                reservation.phase,
                PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
            )
        })
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
    ensure_operation_fifo_isolation(account, &config.task_id, &decision.intents)?;
    let positions = strategy_positions(state, pair)?;
    let account_risk = AccountRiskSnapshot {
        // Use the live settled equity base, not the bootstrap deposit, so
        // opening authority shrinks after realized losses and fees.
        equity: account.settled_equity_base,
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
    let reservation = if let Some(ticket) = admission_ticket {
        let risk = config
            .account_risk
            .as_ref()
            .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
        reservation.with_account_risk_admission(risk.scope_id(), ticket)?
    } else {
        reservation
    };
    PaperArbitrageRequest::new(decision.intents[0].symbol.clone(), batch, reservation)
        .map_err(ArbitragePaperTaskError::Saga)
}

#[allow(clippy::too_many_lines)]
fn build_forced_close_operation(
    task_id: &str,
    cost_model: PaperCostModel,
    pair: &ObservedMarketPair,
    state: &ArbitrageState,
    account: &PaperAccountSnapshot,
    operation_sequence: u64,
    operation_lease: PaperAccountOperationLease,
) -> Result<PlannedOperation, ArbitragePaperTaskError> {
    if account.projection_status != ProjectionStatus::Complete
        || account.reservations.iter().any(|reservation| {
            matches!(
                reservation.phase,
                PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
            )
        })
        || account.reservations.iter().any(|reservation| {
            reservation
                .reconciliation
                .as_ref()
                .is_some_and(|record| record.outcome == PaperReconciliationOutcome::Failed)
        })
    {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    let direction = state
        .direction
        .as_ref()
        .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
    let quantity = Quantity::new(state.position_quantity)
        .map_err(|_| ArbitragePaperTaskError::RecoveryRequired)?;
    let original_buy = matching_snapshot(
        direction.buy_exchange.as_str(),
        &direction.buy_symbol,
        direction.buy_market_type,
        pair,
    )?;
    let original_sell = matching_snapshot(
        direction.sell_exchange.as_str(),
        &direction.sell_symbol,
        direction.sell_market_type,
        pair,
    )?;
    let mut buy_to_cover = OrderIntent::limit(
        original_sell.exchange().to_owned(),
        original_sell.symbol.clone(),
        original_sell.market_type,
        Side::Buy,
        quantity,
        original_sell.ask(),
    );
    buy_to_cover.reduce_only = true;
    let mut sell_long = OrderIntent::limit(
        original_buy.exchange().to_owned(),
        original_buy.symbol.clone(),
        original_buy.market_type,
        Side::Sell,
        quantity,
        original_buy.bid(),
    );
    sell_long.reduce_only = true;
    let intents = vec![buy_to_cover, sell_long];
    let markets = [pair.left.clone(), pair.right.clone()];
    validate_liquidity(&intents, &markets)?;

    let buy_price = original_buy.ask();
    let sell_price = original_sell.bid();
    let absolute = sell_price
        .as_decimal()
        .checked_sub(buy_price.as_decimal())
        .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
    let percent = absolute
        .checked_div(buy_price.as_decimal())
        .and_then(|value| value.checked_mul(Decimal::ONE_HUNDRED))
        .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
    let decision = ArbitrageDecision {
        kind: ArbitrageDecisionKind::Reduce,
        segment: 0,
        target_quantity: Decimal::ZERO,
        delta_quantity: state.position_quantity,
        spread: SpreadQuote {
            buy_exchange: direction.buy_exchange.clone(),
            sell_exchange: direction.sell_exchange.clone(),
            buy_symbol: direction.buy_symbol.clone(),
            sell_symbol: direction.sell_symbol.clone(),
            buy_market_type: direction.buy_market_type,
            sell_market_type: direction.sell_market_type,
            buy_price,
            sell_price,
            absolute,
            percent,
        },
        direction: None,
        intents,
    };
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
                .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
            PaperReservationLeg::from_intent(index, intent, notional)
                .map_err(ArbitragePaperTaskError::Account)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let operation_task_id = format!("{task_id}/op/{operation_sequence:06}");
    let idempotency_key = format!("arbitrage:{operation_sequence:06}");
    let reservation = PaperReservationRequest::planned(
        operation_task_id,
        idempotency_key,
        batch.id(),
        cost_model,
        legs,
    )?;
    let request = PaperArbitrageRequest::new(direction.buy_symbol.clone(), batch, reservation)
        .map_err(ArbitragePaperTaskError::Saga)?;
    Ok(PlannedOperation {
        request,
        decision,
        pair: pair.clone(),
        admission_ticket: None,
        operation_lease,
    })
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

fn matching_snapshot<'a>(
    exchange: &str,
    symbol: &Symbol,
    market_type: MarketType,
    pair: &'a ObservedMarketPair,
) -> Result<&'a MarketSnapshot, ArbitragePaperTaskError> {
    [&pair.left, &pair.right]
        .into_iter()
        .find(|market| {
            market.exchange() == exchange
                && market.symbol == *symbol
                && market.market_type == market_type
        })
        .ok_or(ArbitragePaperTaskError::RecoveryRequired)
}

fn start_operation(
    saga: &DurablePaperArbitrageSaga,
    executor: Arc<dyn ArbitragePaperExecutor>,
    planned: PlannedOperation,
) -> InFlightOperation {
    let request = planned.request.clone();
    let admission_ticket = planned.admission_ticket.clone();
    let saga = saga.clone();
    let request_for_task = planned.request;
    let pair = planned.pair;
    let operation_lease = planned.operation_lease;
    let execution_started = Arc::new(AtomicBool::new(false));
    let task_execution_started = Arc::clone(&execution_started);
    let join = tokio::spawn(async move {
        let result = saga
            .run(request_for_task, move |batch| {
                task_execution_started.store(true, Ordering::Release);
                executor.execute(batch, pair)
            })
            .await;
        (result, operation_lease)
    });
    InFlightOperation {
        request,
        decision: planned.decision,
        admission_ticket,
        execution_started,
        join: Some(join),
    }
}

fn complete_operation(
    result: OperationJoinResult,
    decision: &ArbitrageDecision,
    state: &mut ArbitrageState,
) -> Result<PaperAccountOperationLease, ArbitragePaperTaskError> {
    match result {
        Ok((Ok(PaperArbitrageRun::Completed { .. }), operation_lease)) => {
            state.position_quantity = decision.target_quantity;
            state.direction.clone_from(&decision.direction);
            Ok(operation_lease)
        }
        Ok((Ok(PaperArbitrageRun::AlreadyCompleted { .. }), _operation_lease)) => {
            Err(ArbitragePaperTaskError::RecoveryRequired)
        }
        Ok((Err(error), _operation_lease)) => Err(ArbitragePaperTaskError::Saga(error)),
        Err(error) if error.is_panic() => Err(ArbitragePaperTaskError::TaskPanicked),
        Err(_) => Err(ArbitragePaperTaskError::TaskCancelled),
    }
}

async fn retain_cancelled_operation(
    account: &PaperAccountAuthority,
    risk: Option<&AccountRiskAuthority>,
    owner_task_id: &str,
    admission_ticket: Option<&AccountRiskAdmissionTicket>,
    request: &PaperArbitrageRequest,
    now: DateTime<Utc>,
) -> Result<bool, ArbitragePaperTaskError> {
    retain_cancelled_reservation(
        account,
        risk,
        owner_task_id,
        admission_ticket,
        request.reservation().reservation_id(),
        now,
    )
    .await
    .map_err(ArbitragePaperTaskError::from)
}

async fn discard_planned_admission(
    risk: Option<&AccountRiskAuthority>,
    task_id: &str,
    ticket: Option<&AccountRiskAdmissionTicket>,
    now: DateTime<Utc>,
) -> Result<(), ArbitragePaperTaskError> {
    discard_shared_admission(risk, task_id, ticket, now)
        .await
        .map_err(ArbitragePaperTaskError::from)
}

async fn abort_inflight(
    operation: &mut Option<InFlightOperation>,
    account: &PaperAccountAuthority,
    risk: Option<&AccountRiskAuthority>,
    owner_task_id: &str,
) -> Result<(), ArbitragePaperTaskError> {
    if let Some(mut operation) = operation.take() {
        operation.abort().await;
        if retain_cancelled_operation(
            account,
            risk,
            owner_task_id,
            operation.admission_ticket.as_ref(),
            &operation.request,
            Utc::now(),
        )
        .await?
        {
            return Err(ArbitragePaperTaskError::RecoveryRequired);
        }
    }
    Ok(())
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
    risk: Option<&AccountRiskAuthority>,
    owner_task_id: &str,
    cost_model: PaperCostModel,
    operation_grace: Duration,
    saga: &DurablePaperArbitrageSaga,
    executor: Arc<dyn ArbitragePaperExecutor>,
    directive_close_pair: Option<ObservedMarketPair>,
    operation_sequence: &mut u64,
    state: &mut ArbitrageState,
    active_operation_lease: &ActiveOperationLease,
) -> TaskResult {
    let directive_shutdown = directive_close_pair.is_some();
    let mut cancelled_reservation_needs_recovery = false;
    if let Some(active) = operation.as_mut()
        && (cancel_requested || !active.execution_started())
    {
        active.abort().await;
        cancelled_reservation_needs_recovery = match retain_cancelled_operation(
            account,
            risk,
            owner_task_id,
            active.admission_ticket.as_ref(),
            &active.request,
            Utc::now(),
        )
        .await
        {
            Ok(needs_recovery) => needs_recovery,
            Err(error) => {
                let failure = error.failure_bucket();
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
        };
        clear_active_operation_lease(active_operation_lease);
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
            match retain_cancelled_operation(
                account,
                risk,
                owner_task_id,
                operation.admission_ticket.as_ref(),
                &operation.request,
                Utc::now(),
            )
            .await
            {
                Ok(true) => {
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
                Ok(false) => {}
                Err(retain_error) => {
                    let failure = retain_error.failure_bucket();
                    return fail_owner(
                        left,
                        right,
                        history,
                        status_sender,
                        last_recorded_at,
                        failure,
                        retain_error,
                    )
                    .await;
                }
            }
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
            match retain_cancelled_operation(
                account,
                risk,
                owner_task_id,
                operation.admission_ticket.as_ref(),
                &operation.request,
                Utc::now(),
            )
            .await
            {
                Ok(true) => {
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
                Ok(false) => {}
                Err(retain_error) => {
                    let failure = retain_error.failure_bucket();
                    return fail_owner(
                        left,
                        right,
                        history,
                        status_sender,
                        last_recorded_at,
                        failure,
                        retain_error,
                    )
                    .await;
                }
            }
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
            let needs_recovery = match retain_cancelled_operation(
                account,
                risk,
                owner_task_id,
                operation.admission_ticket.as_ref(),
                &operation.request,
                Utc::now(),
            )
            .await
            {
                Ok(needs_recovery) => needs_recovery,
                Err(error) => {
                    let failure = error.failure_bucket();
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
            };
            if needs_recovery {
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
        } else {
            let result = if directive_shutdown
                || matches!(requested_exit, ArbitragePaperTaskExit::SourceEnded)
            {
                let Ok(result) = tokio::time::timeout(operation_grace, operation.join_mut()).await
                else {
                    operation.abort().await;
                    let _retention = retain_cancelled_operation(
                        account,
                        risk,
                        owner_task_id,
                        operation.admission_ticket.as_ref(),
                        &operation.request,
                        Utc::now(),
                    )
                    .await;
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
                };
                result
            } else {
                operation.join_mut().await
            };
            let _ = operation.join.take();
            let operation_lease = match complete_operation(result, &operation.decision, state) {
                Ok(operation_lease) => operation_lease,
                Err(_) if directive_shutdown => {
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
                Err(error) => {
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
            };
            if let Some(risk) = risk
                && state.position_quantity.is_zero()
                && let Err(error) = risk.record_position_closed(owner_task_id, Utc::now()).await
            {
                return fail_owner(
                    left,
                    right,
                    history,
                    status_sender,
                    last_recorded_at,
                    ArbitragePaperTaskFailure::AccountContract,
                    ArbitragePaperTaskError::AccountRisk(error),
                )
                .await;
            }
            clear_active_operation_lease(active_operation_lease);
            drop(operation_lease);
        }
    }

    // A risk-directed close reprojects and settles under one account-wide
    // lease. The drained opening operation above has already dropped its own
    // lease, so this acquisition cannot recursively wait on the same owner.
    let mut directive_operation_lease = None;
    if directive_shutdown {
        let operation_lease = account.acquire_operation_lease().await;
        if register_active_operation_lease(active_operation_lease, &operation_lease).is_err() {
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
        directive_operation_lease = Some(operation_lease);
    }
    if directive_shutdown || matches!(requested_exit, ArbitragePaperTaskExit::SourceEnded) {
        *state = match restore_state_from_account(account, owner_task_id).await {
            Ok(restored) => restored,
            Err(_) => {
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
        };
    }
    if matches!(requested_exit, ArbitragePaperTaskExit::SourceEnded)
        && !state.position_quantity.is_zero()
    {
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

    if let Some(pair) = directive_close_pair
        && !state.position_quantity.is_zero()
    {
        let Some(operation_lease) = directive_operation_lease.take() else {
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
        };
        let account_snapshot = match account_decision_snapshot(account).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return fail_owner(
                    left,
                    right,
                    history,
                    status_sender,
                    last_recorded_at,
                    error.failure_bucket(),
                    error,
                )
                .await;
            }
        };
        let Some(next_sequence) = operation_sequence.checked_add(1) else {
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
        };
        let Ok(planned) = build_forced_close_operation(
            owner_task_id,
            cost_model,
            &pair,
            state,
            &account_snapshot,
            next_sequence,
            operation_lease,
        ) else {
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
        };
        *operation_sequence = next_sequence;
        publish_operation_count(status_sender, *operation_sequence);
        let mut close_operation = start_operation(saga, executor, planned);
        let Ok(result) = tokio::time::timeout(operation_grace, close_operation.join_mut()).await
        else {
            close_operation.abort().await;
            let _retention = retain_cancelled_operation(
                account,
                risk,
                owner_task_id,
                close_operation.admission_ticket.as_ref(),
                &close_operation.request,
                Utc::now(),
            )
            .await;
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
        };
        let _ = close_operation.join.take();
        let Ok(close_operation_lease) =
            complete_operation(result, &close_operation.decision, state)
        else {
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
        };
        *state = match restore_state_from_account(account, owner_task_id).await {
            Ok(restored) => restored,
            Err(_) => {
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
        };
        if !state.position_quantity.is_zero() {
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
        if let Some(risk) = risk
            && let Err(error) = risk.record_position_closed(owner_task_id, Utc::now()).await
        {
            return fail_owner(
                left,
                right,
                history,
                status_sender,
                last_recorded_at,
                ArbitragePaperTaskFailure::AccountContract,
                ArbitragePaperTaskError::AccountRisk(error),
            )
            .await;
        }
        clear_active_operation_lease(active_operation_lease);
        drop(close_operation_lease);
    }
    if directive_operation_lease.is_some() {
        clear_active_operation_lease(active_operation_lease);
    }
    drop(directive_operation_lease);

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
    let account_snapshot = account_decision_snapshot(account).await?;

    let prefix = operation_prefix(task_id);
    let mut last_operation = 0_u64;
    for reservation in &account_snapshot.reservations {
        let Some(sequence) = owner_operation_sequence(&reservation.task_id, &prefix) else {
            continue;
        };
        if matches!(
            reservation.phase,
            PaperReservationPhase::Pending
                | PaperReservationPhase::Uncertain
                | PaperReservationPhase::Committed
        ) {
            return Err(ArbitragePaperTaskError::RecoveryRequired);
        }
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

fn lot_source_is_owned_by(
    snapshot: &PaperAccountSnapshot,
    source_reservation_id: Uuid,
    owner_prefix: &str,
) -> Result<bool, ArbitragePaperTaskError> {
    let mut sources = snapshot
        .reservations
        .iter()
        .filter(|reservation| reservation.reservation_id == source_reservation_id);
    let Some(source) = sources.next() else {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    };
    if sources.next().is_some() {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    let owned = owner_operation_sequence(&source.task_id, owner_prefix).is_some();
    if owned && source.phase != PaperReservationPhase::Committed {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    Ok(owned)
}

fn ensure_operation_fifo_isolation(
    snapshot: &PaperAccountSnapshot,
    owner_task_id: &str,
    intents: &[OrderIntent],
) -> Result<(), ArbitragePaperTaskError> {
    let owner_prefix = operation_prefix(owner_task_id);
    for intent in intents {
        for lot in &snapshot.open_lots {
            let same_exact_instrument = lot.exchange == intent.exchange
                && lot.symbol == intent.symbol
                && lot.market_type == intent.market_type;
            if same_exact_instrument
                && !lot_source_is_owned_by(snapshot, lot.source_reservation_id, &owner_prefix)?
            {
                // Exact paper settlement owns one global FIFO namespace per
                // instrument. Even a same-side foreign lot would leave the
                // tasks mixed and make a later close owner-ambiguous.
                return Err(ArbitragePaperTaskError::RecoveryRequired);
            }
        }
    }
    Ok(())
}

async fn restore_state_from_account(
    account: &PaperAccountAuthority,
    owner_task_id: &str,
) -> Result<ArbitrageState, ArbitragePaperTaskError> {
    let snapshot = account_decision_snapshot(account).await?;
    let owner_prefix = operation_prefix(owner_task_id);
    let mut owned_lots = Vec::new();
    let mut foreign_lots = Vec::new();
    for lot in &snapshot.open_lots {
        if lot_source_is_owned_by(&snapshot, lot.source_reservation_id, &owner_prefix)? {
            owned_lots.push(lot);
        } else {
            foreign_lots.push(lot);
        }
    }
    if owned_lots.is_empty() {
        return Ok(ArbitrageState::default());
    }
    let buy_lot = owned_lots
        .iter()
        .find(|lot| lot.side == Side::Buy)
        .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
    let sell_lot = owned_lots
        .iter()
        .find(|lot| lot.side == Side::Sell)
        .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
    let mut buy_quantity = Decimal::ZERO;
    let mut sell_quantity = Decimal::ZERO;
    for lot in &owned_lots {
        let reference = match lot.side {
            Side::Buy => {
                buy_quantity = buy_quantity
                    .checked_add(lot.remaining_quantity.as_decimal())
                    .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
                buy_lot
            }
            Side::Sell => {
                sell_quantity = sell_quantity
                    .checked_add(lot.remaining_quantity.as_decimal())
                    .ok_or(ArbitragePaperTaskError::RecoveryRequired)?;
                sell_lot
            }
        };
        if lot.exchange != reference.exchange
            || lot.symbol != reference.symbol
            || lot.market_type != reference.market_type
        {
            return Err(ArbitragePaperTaskError::RecoveryRequired);
        }
    }
    if buy_lot.symbol != sell_lot.symbol
        || buy_quantity != sell_quantity
        || buy_quantity <= Decimal::ZERO
        || buy_lot.exchange == sell_lot.exchange
    {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    if foreign_lots.iter().any(|foreign| {
        [buy_lot, sell_lot].into_iter().any(|owned| {
            foreign.exchange == owned.exchange
                && foreign.symbol == owned.symbol
                && foreign.market_type == owned.market_type
                && foreign.side == owned.side
        })
    }) {
        // Exact paper settlement consumes matching lots FIFO without an owner
        // discriminator. When another owner shares either close queue, no
        // reduce-only order can prove it will touch only this owner's lot.
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    Ok(ArbitrageState {
        position_quantity: buy_quantity,
        direction: Some(ArbitrageDirection {
            buy_exchange: buy_lot.exchange.clone(),
            sell_exchange: sell_lot.exchange.clone(),
            buy_symbol: buy_lot.symbol.clone(),
            sell_symbol: sell_lot.symbol.clone(),
            buy_market_type: buy_lot.market_type,
            sell_market_type: sell_lot.market_type,
        }),
    })
}

async fn account_decision_snapshot(
    account: &PaperAccountAuthority,
) -> Result<PaperAccountSnapshot, ArbitragePaperTaskError> {
    let snapshot = account.decision_snapshot().await?;
    if snapshot.reservations.iter().any(|reservation| {
        reservation
            .reconciliation
            .as_ref()
            .is_some_and(|record| record.outcome == PaperReconciliationOutcome::Failed)
    }) {
        return Err(ArbitragePaperTaskError::RecoveryRequired);
    }
    Ok(snapshot)
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

fn owner_operation_sequence(task_id: &str, owner_prefix: &str) -> Option<u64> {
    let sequence = task_id.strip_prefix(owner_prefix)?;
    if sequence.is_empty() || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    sequence.parse::<u64>().ok()
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
    AccountRisk(AccountRiskError),
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
            Self::Account(_) | Self::AccountRisk(_) => ArbitragePaperTaskFailure::AccountContract,
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

impl From<PaperAdmissionCompensationError> for ArbitragePaperTaskError {
    fn from(value: PaperAdmissionCompensationError) -> Self {
        match value {
            PaperAdmissionCompensationError::Account(error) => Self::Account(error),
            PaperAdmissionCompensationError::AccountRisk(error) => Self::AccountRisk(error),
            PaperAdmissionCompensationError::RecoveryRequired => Self::RecoveryRequired,
        }
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
            Self::AccountRisk(error) => error.fmt(formatter),
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
            Self::AccountRisk(error) => Some(error),
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
