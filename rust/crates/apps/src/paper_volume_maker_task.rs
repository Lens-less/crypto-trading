//! Durable continuous owner for one single-source paper volume-maker.
//!
//! The owner consumes [`VolumeMakerStrategy`] plans over a replay-backed
//! market source and turns them into independent single-leg saga operations:
//! one open leg (a crossed standing quote in limit mode, an imbalance-driven
//! market order in market mode) followed by one reduce-only market close.
//! Mirroring the legacy Python service, one completed cycle is exactly one
//! filled open leg plus its close, and bounded per-hour statistics facts are
//! journaled at every hour rollover and on stop.
//!
//! Bounded deviations from the legacy service, by design:
//! - The owner keeps no resting orders. A limit-mode quote is virtual: it is
//!   held in memory and executed as one marketable single-leg operation only
//!   after a later observation crosses it, exactly like virtual grid levels.
//! - The close executes on the next observation after the open, which mirrors
//!   the legacy "wait for a price change, then close" market mode.
//! - Stability waits, retry ladders, and consecutive-failure sleeps are
//!   replaced by the fail-closed saga/recovery discipline of this repository.

use std::{
    error::Error, fmt, future::Future, io::ErrorKind, path::Path, pin::Pin, sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use crypto_trading_domain::{MarketSnapshot, Money, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::TradingReceipt;
use crypto_trading_runtime::{
    AccountRiskAdmission, AccountRiskAdmissionTicket, AccountRiskAuthority, AccountRiskCandidate,
    AccountRiskError, DecisionRecord, ExecutionBatch, FileJournalSnapshotSource, HistoryError,
    JournalReadError, JournalSnapshot, JournalSnapshotSource, JsonlHistory,
    MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION, MarketDataEvent, MarketDataEventSource,
    MarketSupervisor, MarketSupervisorConfig, MarketSupervisorError, MarketSupervisorExit,
    MarketSupervisorHealth, MarketSupervisorPhase, MarketSupervisorStatus, PaperAccountAuthority,
    PaperAccountError, PaperCostModel, PaperReconciliationOutcome, PaperReservationLeg,
    PaperReservationPhase, PaperReservationRequest, ProjectionStatus, ReadModelError,
    ReadOnlyTaskPhase, ReadOnlyTaskReadModel, ReadOnlyTaskRecovery, ReadOnlyTaskView, RuntimeError,
};
use crypto_trading_strategy::{
    StrategyError, StrategyMachine, VolumeMakerMode, VolumeMakerState, VolumeMakerStrategy,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use crate::{
    DurablePaperSingleLegSaga, PaperSingleLegRequest, PaperSingleLegRun, PaperSingleLegSagaError,
    paper_admission::{
        PaperAdmissionCompensationError, discard_planned_admission as discard_shared_admission,
        retain_cancelled_reservation,
    },
    paper_grid_task::{account_risk_directive_record, account_risk_exit_reason},
    task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture},
};

/// Stable version of the process-local volume-maker owner status.
pub const VOLUME_MAKER_PAPER_TASK_STATUS_SCHEMA_VERSION: u16 = 1;
/// Stable version of the durable `volume_maker_statistics` fact.
pub const VOLUME_MAKER_STATISTICS_SCHEMA_VERSION: u16 = 1;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";
const TASK_KIND: &str = "volume_maker";
const VOLUME_MAKER_STRATEGY: &str = "volume_maker";
const MAX_TASK_ID_BYTES: usize = 96;
const OPERATION_SUFFIX_BYTES: usize = "/op/00000000000000000000".len();

/// Boxed execution future behind the trusted paper adapter seam.
pub type VolumeMakerPaperExecutionFuture =
    Pin<Box<dyn Future<Output = Result<Vec<TradingReceipt>, RuntimeError>> + Send + 'static>>;

/// Minimal object-safe execution seam owned by the trusted task process.
pub trait VolumeMakerPaperExecutor: Send + Sync + 'static {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture;
}

/// Validated owner configuration. The pure strategy owns instrument identity
/// and per-cycle decisions; this type owns execution and lifecycle identity.
#[derive(Clone, Debug)]
pub struct VolumeMakerPaperTaskConfig {
    task_id: String,
    strategy: VolumeMakerStrategy,
    cost_model: PaperCostModel,
    supervisor: MarketSupervisorConfig,
    account_risk: Option<AccountRiskAuthority>,
    cycle_interval: Duration,
    max_cycles: Option<u64>,
    target_volume: Option<Decimal>,
}

impl VolumeMakerPaperTaskConfig {
    /// Creates a bounded single-source volume-maker owner configuration.
    ///
    /// # Errors
    ///
    /// Returns [`VolumeMakerPaperTaskError::InvalidConfig`] for unsafe
    /// identities.
    pub fn new(
        task_id: impl Into<String>,
        strategy: VolumeMakerStrategy,
        cost_model: PaperCostModel,
        supervisor: MarketSupervisorConfig,
    ) -> Result<Self, VolumeMakerPaperTaskError> {
        let task_id = task_id.into();
        let task_id = task_id.trim();
        let exchange = strategy.config().exchange.trim();
        if task_id.is_empty()
            || task_id.len() > MAX_TASK_ID_BYTES
            || task_id.len().saturating_add(OPERATION_SUFFIX_BYTES) > 128
            || !safe_identity(task_id)
            || exchange.is_empty()
            || exchange.len() > 128
            || !safe_identity(exchange)
        {
            return Err(VolumeMakerPaperTaskError::InvalidConfig);
        }
        Ok(Self {
            task_id: task_id.to_owned(),
            strategy,
            cost_model,
            supervisor,
            account_risk: None,
            cycle_interval: Duration::ZERO,
            max_cycles: None,
            target_volume: None,
        })
    }

    /// Attaches the durable account-level risk authority. Open legs must pass
    /// its admission before any reservation is created; its close directives
    /// stop the owner exactly like a legacy emergency stop.
    #[must_use]
    pub fn with_account_risk(mut self, account_risk: AccountRiskAuthority) -> Self {
        self.account_risk = Some(account_risk);
        self
    }

    /// Minimum event-time distance between one completed cycle and the next
    /// open leg, mirroring the legacy `cycle_interval` pacing.
    #[must_use]
    pub const fn with_cycle_interval(mut self, cycle_interval: Duration) -> Self {
        self.cycle_interval = cycle_interval;
        self
    }

    /// Stops the owner cleanly once this many cycles have completed,
    /// mirroring the legacy `max_cycles` bound.
    #[must_use]
    pub const fn with_max_cycles(mut self, max_cycles: u64) -> Self {
        self.max_cycles = Some(max_cycles);
        self
    }

    /// Stops the owner cleanly once accumulated open-leg base volume reaches
    /// this bound.
    #[must_use]
    pub fn with_target_volume(mut self, target_volume: Quantity) -> Self {
        self.target_volume = Some(target_volume.as_decimal());
        self
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.strategy.config().exchange
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        &self.strategy.config().symbol
    }
}

/// Durable aggregate lifecycle phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeMakerPaperTaskPhase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl VolumeMakerPaperTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

impl fmt::Display for VolumeMakerPaperTaskPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_phase_label(*self))
    }
}

/// Bounded normal terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeMakerPaperTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
    /// The configured `max_cycles` or `target_volume` bound was reached.
    BoundsReached,
}

impl fmt::Display for VolumeMakerPaperTaskExit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_exit_label(*self))
    }
}

/// Bounded task failure suitable for the durable task projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VolumeMakerPaperTaskFailure {
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

impl fmt::Display for VolumeMakerPaperTaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_failure_label(*self))
    }
}

/// Latest durable lifecycle status plus operation and cycle counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeMakerPaperTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: VolumeMakerPaperTaskPhase,
    pub processed_event_count: u64,
    pub operation_count: u64,
    pub completed_cycle_count: u64,
    pub sources: Vec<MarketSupervisorStatus>,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub exit: Option<VolumeMakerPaperTaskExit>,
    pub failure: Option<VolumeMakerPaperTaskFailure>,
    pub runtime_failure: Option<VolumeMakerPaperTaskFailure>,
}

impl TaskHostStatus for VolumeMakerPaperTaskStatus {
    fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Opaque owner of one source supervisor and one volume-maker cycle loop.
#[derive(Debug)]
pub struct VolumeMakerPaperTask {
    stop: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    status_sender: watch::Sender<VolumeMakerPaperTaskStatus>,
    status: watch::Receiver<VolumeMakerPaperTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskFailure>>,
    account: PaperAccountAuthority,
    history: JsonlHistory,
    shutdown_grace: Duration,
}

impl VolumeMakerPaperTask {
    /// Starts one durable single-source volume-maker owner.
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
        config: VolumeMakerPaperTaskConfig,
        source: S,
        account: PaperAccountAuthority,
        history: JsonlHistory,
        executor: Arc<dyn VolumeMakerPaperExecutor>,
    ) -> Result<Self, VolumeMakerPaperTaskError>
    where
        S: MarketDataEventSource,
    {
        if account.history_path() != history.path()
            || config.symbol().as_str().trim().is_empty()
            || source.source_id() != config.exchange()
        {
            return Err(VolumeMakerPaperTaskError::InvalidSourceBinding);
        }
        let operation_sequence = recovery_preflight(&config.task_id, &account, &history).await?;
        let registered_at = Utc::now();
        history
            .append(&registered_record(
                &config.task_id,
                source.source_id(),
                registered_at,
            ))
            .await
            .map_err(VolumeMakerPaperTaskError::Journal)?;

        let mut supervisor = match MarketSupervisor::start_new(source, config.supervisor) {
            Ok(supervisor) => supervisor,
            Err(error) => {
                history
                    .append(&lifecycle_record(
                        &config.task_id,
                        "task_failed",
                        "failed",
                        0,
                        &Value::Array(vec![placeholder_source_value(config.exchange())]),
                        None,
                        Some("startup_failed"),
                        Utc::now().max(registered_at),
                    ))
                    .await
                    .map_err(VolumeMakerPaperTaskError::Journal)?;
                return Err(VolumeMakerPaperTaskError::Source(error));
            }
        };
        tokio::task::yield_now().await;
        let running_at = Utc::now().max(registered_at);
        let initial = VolumeMakerPaperTaskStatus {
            schema_version: VOLUME_MAKER_PAPER_TASK_STATUS_SCHEMA_VERSION,
            task_id: config.task_id.clone(),
            phase: VolumeMakerPaperTaskPhase::Running,
            processed_event_count: 0,
            operation_count: operation_sequence,
            completed_cycle_count: 0,
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
            return Err(VolumeMakerPaperTaskError::Journal(error));
        }

        let saga = DurablePaperSingleLegSaga::new(account.clone(), history.clone())
            .map_err(VolumeMakerPaperTaskError::Saga)?
            .with_strategy_label(VOLUME_MAKER_STRATEGY);
        let (stop, stop_receiver) = watch::channel(false);
        let (cancel, cancel_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial);
        let task_status = status_sender.clone();
        let task_history = history.clone();
        let task_config = config.clone();
        let join = tokio::spawn(async move {
            run_owner(OwnerContext {
                config: task_config,
                source: supervisor,
                saga,
                executor,
                history: task_history,
                status_sender: task_status,
                stop: stop_receiver,
                cancel: cancel_receiver,
                last_recorded_at: running_at,
                operation_sequence,
            })
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

    /// Returns the latest status whose non-runtime fields were durably written.
    #[must_use]
    pub fn status(&self) -> VolumeMakerPaperTaskStatus {
        self.status.borrow().clone()
    }

    /// Reprojects the stable owner status from the journal.
    ///
    /// # Errors
    ///
    /// Returns snapshot, read-model, or worker failures and never substitutes
    /// process-local state.
    pub async fn durable_status(&self) -> Result<ReadOnlyTaskView, VolumeMakerPaperTaskError> {
        durable_task_view(&self.account, self.history.path(), &self.status().task_id)
            .await?
            .ok_or(VolumeMakerPaperTaskError::RecoveryRequired)
    }

    /// Waits for a finite source to terminate without requesting a stop.
    ///
    /// # Errors
    ///
    /// Returns the owner result or a typed join failure.
    pub async fn wait(&mut self) -> Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(VolumeMakerPaperTaskError::PreviouslyFailed);
        }
        let Some(join) = self.join.take() else {
            return Err(VolumeMakerPaperTaskError::TaskCancelled);
        };
        let result = Self::map_join(join.await);
        self.store_completion(&result);
        result
    }

    /// Stops admitting new cycles and waits for the current operation to reach
    /// a terminal durable outcome.
    ///
    /// # Errors
    ///
    /// Returns a typed execution, recovery, journal, or bounded shutdown
    /// failure.
    pub async fn stop(&mut self) -> Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError> {
        self.finish_with_signal(false).await
    }

    /// Requests cancellation. If an operation has crossed the execution seam,
    /// its reservation is retained as uncertain; cancellation never releases
    /// capacity without a confirmed cancelled receipt.
    ///
    /// # Errors
    ///
    /// Returns a typed recovery or lifecycle failure.
    pub async fn cancel(&mut self) -> Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError> {
        self.finish_with_signal(true).await
    }

    async fn finish_with_signal(
        &mut self,
        cancel: bool,
    ) -> Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(VolumeMakerPaperTaskError::PreviouslyFailed);
        }
        if cancel {
            let _ = self.cancel.send(true);
        } else {
            let _ = self.stop.send(true);
        }
        let Some(mut join) = self.join.take() else {
            return Err(VolumeMakerPaperTaskError::TaskCancelled);
        };
        let deadline = self.shutdown_grace.saturating_mul(2);
        let result = if let Ok(joined) = tokio::time::timeout(deadline, &mut join).await {
            Self::map_join(joined)
        } else {
            join.abort();
            let _ = join.await;
            self.retain_active_capacity().await;
            self.record_external_failure(VolumeMakerPaperTaskFailure::RecoveryRequired)
                .await?;
            Err(VolumeMakerPaperTaskError::ShutdownTimedOut)
        };
        self.store_completion(&result);
        result
    }

    fn map_join(
        joined: Result<TaskResult, JoinError>,
    ) -> Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(VolumeMakerPaperTaskError::TaskPanicked),
            Err(_) => Err(VolumeMakerPaperTaskError::TaskCancelled),
        }
    }

    fn store_completion(
        &mut self,
        result: &Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError>,
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
        failure: VolumeMakerPaperTaskFailure,
    ) -> Result<(), VolumeMakerPaperTaskError> {
        let mut status = self.status();
        status.phase = VolumeMakerPaperTaskPhase::Failed;
        status.failure = Some(failure);
        status.exit = None;
        status.runtime_failure = None;
        let recorded_at = Utc::now().max(status.last_recorded_at.unwrap_or_else(Utc::now));
        status.last_recorded_at = Some(recorded_at);
        self.history
            .append(&status_record(&status, "task_failed", recorded_at))
            .await
            .map_err(VolumeMakerPaperTaskError::Journal)?;
        self.status_sender.send_replace(status);
        Ok(())
    }
}

impl TaskHost for VolumeMakerPaperTask {
    type Status = VolumeMakerPaperTaskStatus;
    type Exit = VolumeMakerPaperTaskExit;
    type Error = VolumeMakerPaperTaskError;

    fn status(&self) -> Self::Status {
        Self::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(Self::stop(self))
    }
}

impl Drop for VolumeMakerPaperTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type TaskResult = Result<VolumeMakerPaperTaskExit, VolumeMakerPaperTaskError>;

/// One standing virtual maker quote awaiting a crossing observation.
#[derive(Clone, Copy, Debug)]
struct StandingQuote {
    bid: Price,
    ask: Price,
}

/// Position opened by the current cycle, awaiting its reduce-only close.
#[derive(Clone, Copy, Debug)]
struct OpenPosition {
    side: Side,
    quantity: Quantity,
    open_price: Decimal,
}

/// One bounded per-hour statistics accumulator mirroring the legacy
/// hourly tracker semantics on the repository's journal discipline.
#[derive(Clone, Copy, Debug)]
struct StatisticsBucket {
    hour_start: DateTime<Utc>,
    completed_cycles: u64,
    buy_volume: Decimal,
    sell_volume: Decimal,
    realized_pnl: Decimal,
    rejected_entries: u64,
}

impl StatisticsBucket {
    const fn new(hour_start: DateTime<Utc>) -> Self {
        Self {
            hour_start,
            completed_cycles: 0,
            buy_volume: Decimal::ZERO,
            sell_volume: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            rejected_entries: 0,
        }
    }

    const fn is_empty(&self) -> bool {
        self.completed_cycles == 0 && self.rejected_entries == 0
    }
}

/// What one observation asks the owner to execute.
#[derive(Clone, Debug)]
struct PlannedOperation {
    intent: OrderIntent,
    kind: PlannedKind,
    reference_price: Price,
    admission_ticket: Option<AccountRiskAdmissionTicket>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlannedKind {
    Open,
    Close,
}

struct OwnerContext {
    config: VolumeMakerPaperTaskConfig,
    source: MarketSupervisor,
    saga: DurablePaperSingleLegSaga,
    executor: Arc<dyn VolumeMakerPaperExecutor>,
    history: JsonlHistory,
    status_sender: watch::Sender<VolumeMakerPaperTaskStatus>,
    stop: watch::Receiver<bool>,
    cancel: watch::Receiver<bool>,
    last_recorded_at: DateTime<Utc>,
    operation_sequence: u64,
}

#[allow(clippy::too_many_lines)]
async fn run_owner(mut context: OwnerContext) -> TaskResult {
    let mut quote: Option<StandingQuote> = None;
    let mut position: Option<OpenPosition> = None;
    let mut bucket: Option<StatisticsBucket> = None;
    let mut total_volume = Decimal::ZERO;
    let mut next_cycle_at: Option<DateTime<Utc>> = None;
    loop {
        let selected = tokio::select! {
            biased;
            cancel_result = context.cancel.changed() => {
                if cancel_result.is_err() || *context.cancel.borrow_and_update() {
                    Selected::Cancel
                } else {
                    continue;
                }
            }
            stop_result = context.stop.changed() => {
                if stop_result.is_err() || *context.stop.borrow_and_update() {
                    Selected::Stop
                } else {
                    continue;
                }
            }
            result = context.source.next_event() => Selected::Source(result),
        };
        match selected {
            Selected::Cancel | Selected::Stop => {
                return stop_owner(
                    &mut context,
                    bucket.take(),
                    VolumeMakerPaperTaskExit::StopRequested,
                )
                .await;
            }
            Selected::Source(Ok(Some(event))) => {
                if *context.cancel.borrow() || *context.stop.borrow() {
                    return stop_owner(
                        &mut context,
                        bucket.take(),
                        VolumeMakerPaperTaskExit::StopRequested,
                    )
                    .await;
                }
                let mut next = context.status_sender.borrow().clone();
                next.processed_event_count = next
                    .processed_event_count
                    .checked_add(1)
                    .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                next.sources = vec![context.source.status()];
                let observed = match observation_view(&context.config, &event) {
                    Ok(observed) => observed,
                    Err(error) => {
                        return fail_owner(
                            &mut context,
                            VolumeMakerPaperTaskFailure::InvalidRequest,
                            error,
                        )
                        .await;
                    }
                };
                let Some((snapshot, observed_at)) = observed else {
                    // Source gaps and unavailability keep the owner alive but
                    // never advance a cycle or refresh a stale quote.
                    let recorded_at = Utc::now().max(context.last_recorded_at);
                    next.last_recorded_at = Some(recorded_at);
                    if let Err(error) = context
                        .history
                        .append(&status_record(&next, "task_checkpointed", recorded_at))
                        .await
                    {
                        return fail_owner(
                            &mut context,
                            VolumeMakerPaperTaskFailure::JournalUnavailable,
                            VolumeMakerPaperTaskError::Journal(error),
                        )
                        .await;
                    }
                    context.last_recorded_at = recorded_at;
                    context.status_sender.send_replace(next);
                    continue;
                };

                // Hour rollover: flush the closed statistics bucket first so
                // its fact precedes any operation of the new hour.
                let bucket_hour = hour_bucket(observed_at);
                if let Some(current) = bucket
                    && current.hour_start < bucket_hour
                {
                    if let Err(error) =
                        append_statistics(&mut context, &current, "hour_rollover").await
                    {
                        return fail_owner(
                            &mut context,
                            VolumeMakerPaperTaskFailure::JournalUnavailable,
                            error,
                        )
                        .await;
                    }
                    bucket = None;
                }
                let stats = bucket.get_or_insert_with(|| StatisticsBucket::new(bucket_hour));

                // Durable account-risk close directives run first: a kill
                // switch, a critically low balance, or an expired position
                // clock stops the owner exactly like a legacy emergency stop.
                if context.config.account_risk.is_some() {
                    match account_risk_exit(&mut context, &snapshot, observed_at).await {
                        Ok(false) => {}
                        Ok(true) => {
                            return stop_owner(
                                &mut context,
                                bucket.take(),
                                VolumeMakerPaperTaskExit::StopRequested,
                            )
                            .await;
                        }
                        Err(error) => {
                            let failure = error.failure_bucket();
                            return fail_owner(&mut context, failure, error).await;
                        }
                    }
                }

                let planned = match plan_operation(
                    &context.config,
                    position,
                    &mut quote,
                    next_cycle_at,
                    &snapshot,
                    observed_at,
                ) {
                    Ok(planned) => planned,
                    Err(error) => {
                        return fail_owner(
                            &mut context,
                            VolumeMakerPaperTaskFailure::InvalidRequest,
                            error,
                        )
                        .await;
                    }
                };

                let admitted = match planned {
                    Some(operation)
                        if operation.kind == PlannedKind::Open
                            && context.config.account_risk.is_some() =>
                    {
                        match admit_open(&context, &operation, observed_at).await {
                            Ok(Some(ticket)) => Some(PlannedOperation {
                                admission_ticket: Some(ticket),
                                ..operation
                            }),
                            Ok(None) => {
                                // A durable rejection consumes the standing
                                // quote and skips this cycle without failing.
                                stats.rejected_entries = stats.rejected_entries.saturating_add(1);
                                quote = None;
                                None
                            }
                            Err(error) => {
                                let failure = error.failure_bucket();
                                return fail_owner(&mut context, failure, error).await;
                            }
                        }
                    }
                    other => other,
                };

                let mut stop_after_operation = false;
                if let Some(operation) = admitted {
                    let Some(next_operation) = context.operation_sequence.checked_add(1) else {
                        if let Err(error) = discard_planned_admission(
                            context.config.account_risk.as_ref(),
                            &context.config.task_id,
                            operation.admission_ticket.as_ref(),
                            observed_at,
                        )
                        .await
                        {
                            let failure = error.failure_bucket();
                            return fail_owner(&mut context, failure, error).await;
                        }
                        return fail_owner(
                            &mut context,
                            VolumeMakerPaperTaskFailure::InvalidRequest,
                            VolumeMakerPaperTaskError::InvalidRequest,
                        )
                        .await;
                    };
                    context.operation_sequence = next_operation;
                    let request = match build_request(
                        &context.config,
                        &operation,
                        context.operation_sequence,
                    ) {
                        Ok(request) => request,
                        Err(error) => {
                            if let Err(cancel_error) = discard_planned_admission(
                                context.config.account_risk.as_ref(),
                                &context.config.task_id,
                                operation.admission_ticket.as_ref(),
                                observed_at,
                            )
                            .await
                            {
                                let failure = cancel_error.failure_bucket();
                                return fail_owner(&mut context, failure, cancel_error).await;
                            }
                            next.operation_count = context.operation_sequence;
                            context.status_sender.send_replace(next);
                            return fail_owner(
                                &mut context,
                                VolumeMakerPaperTaskFailure::InvalidRequest,
                                error,
                            )
                            .await;
                        }
                    };
                    let recovery_request = request.clone();
                    match run_operation(&mut context, request).await {
                        OperationOutcome::Terminal(Ok(run), stop_requested) => {
                            stop_after_operation = stop_requested;
                            next.operation_count = context.operation_sequence;
                            match consume_run(
                                &operation,
                                &run,
                                &mut position,
                                &mut quote,
                                stats,
                                &mut total_volume,
                            ) {
                                Ok(CycleProgress::None) => {}
                                Ok(CycleProgress::CycleCompleted) => {
                                    next.completed_cycle_count = next
                                        .completed_cycle_count
                                        .checked_add(1)
                                        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                                    next_cycle_at = observed_at
                                        .checked_add_signed(
                                            ChronoDuration::from_std(context.config.cycle_interval)
                                                .unwrap_or(ChronoDuration::MAX),
                                        )
                                        .or(Some(DateTime::<Utc>::MAX_UTC));
                                    if let Some(risk) = context.config.account_risk.as_ref()
                                        && let Err(error) = risk
                                            .record_position_closed(
                                                &context.config.task_id,
                                                observed_at,
                                            )
                                            .await
                                    {
                                        context.status_sender.send_replace(next);
                                        return fail_owner(
                                            &mut context,
                                            VolumeMakerPaperTaskFailure::AccountContract,
                                            VolumeMakerPaperTaskError::AccountRisk(error),
                                        )
                                        .await;
                                    }
                                    if bounds_reached(
                                        &context.config,
                                        next.completed_cycle_count,
                                        total_volume,
                                    ) {
                                        checkpoint(&mut context, &mut next).await?;
                                        return stop_owner(
                                            &mut context,
                                            bucket.take(),
                                            VolumeMakerPaperTaskExit::BoundsReached,
                                        )
                                        .await;
                                    }
                                }
                                Err(error) => {
                                    context.status_sender.send_replace(next);
                                    return fail_owner(
                                        &mut context,
                                        VolumeMakerPaperTaskFailure::InvalidRequest,
                                        error,
                                    )
                                    .await;
                                }
                            }
                        }
                        OperationOutcome::Terminal(Err(error), _) => {
                            let needs_recovery = match retain_cancelled_operation(
                                context.saga.account(),
                                context.config.account_risk.as_ref(),
                                &context.config.task_id,
                                operation.admission_ticket.as_ref(),
                                &recovery_request,
                                observed_at,
                            )
                            .await
                            {
                                Ok(needs_recovery) => needs_recovery,
                                Err(retain_error) => {
                                    let failure = retain_error.failure_bucket();
                                    return fail_owner(&mut context, failure, retain_error).await;
                                }
                            };
                            if needs_recovery {
                                next.operation_count = context.operation_sequence;
                                context.status_sender.send_replace(next);
                                return fail_owner(
                                    &mut context,
                                    VolumeMakerPaperTaskFailure::RecoveryRequired,
                                    VolumeMakerPaperTaskError::RecoveryRequired,
                                )
                                .await;
                            }
                            let (failure, error) = classify_saga_error(error);
                            next.operation_count = context.operation_sequence;
                            context.status_sender.send_replace(next);
                            return fail_owner(&mut context, failure, error).await;
                        }
                        OperationOutcome::Cancelled(request) => {
                            let needs_recovery = match retain_cancelled_operation(
                                context.saga.account(),
                                context.config.account_risk.as_ref(),
                                &context.config.task_id,
                                operation.admission_ticket.as_ref(),
                                &request,
                                observed_at,
                            )
                            .await
                            {
                                Ok(needs_recovery) => needs_recovery,
                                Err(retain_error) => {
                                    let failure = retain_error.failure_bucket();
                                    return fail_owner(&mut context, failure, retain_error).await;
                                }
                            };
                            next.operation_count = context.operation_sequence;
                            context.status_sender.send_replace(next);
                            if needs_recovery {
                                return fail_owner(
                                    &mut context,
                                    VolumeMakerPaperTaskFailure::RecoveryRequired,
                                    VolumeMakerPaperTaskError::RecoveryRequired,
                                )
                                .await;
                            }
                            return stop_owner(
                                &mut context,
                                bucket.take(),
                                VolumeMakerPaperTaskExit::StopRequested,
                            )
                            .await;
                        }
                    }
                }

                checkpoint(&mut context, &mut next).await?;
                if stop_after_operation {
                    return stop_owner(
                        &mut context,
                        bucket.take(),
                        VolumeMakerPaperTaskExit::StopRequested,
                    )
                    .await;
                }
            }
            Selected::Source(Ok(None)) => {
                return stop_owner(
                    &mut context,
                    bucket.take(),
                    VolumeMakerPaperTaskExit::SourceEnded,
                )
                .await;
            }
            Selected::Source(Err(error)) => {
                return fail_owner(
                    &mut context,
                    VolumeMakerPaperTaskFailure::SourceContract,
                    VolumeMakerPaperTaskError::Source(error),
                )
                .await;
            }
        }
    }
}

enum Selected {
    Stop,
    Cancel,
    Source(Result<Option<MarketDataEvent>, MarketSupervisorError>),
}

enum OperationOutcome {
    Terminal(Result<PaperSingleLegRun, PaperSingleLegSagaError>, bool),
    Cancelled(PaperSingleLegRequest),
}

enum CycleProgress {
    None,
    CycleCompleted,
}

async fn run_operation(
    context: &mut OwnerContext,
    request: PaperSingleLegRequest,
) -> OperationOutcome {
    let cancel_request = request.clone();
    let executor = Arc::clone(&context.executor);
    {
        let run = context
            .saga
            .run(request, move |batch| executor.execute(batch));
        tokio::pin!(run);
        let mut stop_requested = false;
        loop {
            tokio::select! {
                biased;
                cancel_result = context.cancel.changed() => {
                    if cancel_result.is_err() || *context.cancel.borrow_and_update() {
                        break OperationOutcome::Cancelled(cancel_request);
                    }
                }
                stop_result = context.stop.changed(), if !stop_requested => {
                    if stop_result.is_err() || *context.stop.borrow_and_update() {
                        stop_requested = true;
                    }
                }
                result = &mut run => {
                    break OperationOutcome::Terminal(result, stop_requested);
                }
            }
        }
    }
}

async fn retain_cancelled_operation(
    account: &PaperAccountAuthority,
    risk: Option<&AccountRiskAuthority>,
    owner_task_id: &str,
    admission_ticket: Option<&AccountRiskAdmissionTicket>,
    request: &PaperSingleLegRequest,
    now: DateTime<Utc>,
) -> Result<bool, VolumeMakerPaperTaskError> {
    retain_cancelled_reservation(
        account,
        risk,
        owner_task_id,
        admission_ticket,
        request.reservation().reservation_id(),
        now,
    )
    .await
    .map_err(VolumeMakerPaperTaskError::from)
}

async fn discard_planned_admission(
    risk: Option<&AccountRiskAuthority>,
    task_id: &str,
    ticket: Option<&AccountRiskAdmissionTicket>,
    now: DateTime<Utc>,
) -> Result<(), VolumeMakerPaperTaskError> {
    discard_shared_admission(risk, task_id, ticket, now)
        .await
        .map_err(VolumeMakerPaperTaskError::from)
}

fn observation_view(
    config: &VolumeMakerPaperTaskConfig,
    event: &MarketDataEvent,
) -> Result<Option<(MarketSnapshot, DateTime<Utc>)>, VolumeMakerPaperTaskError> {
    match event {
        MarketDataEvent::Observation(observation) => {
            if observation.snapshot.exchange() != config.exchange()
                || observation.snapshot.symbol != *config.symbol()
                || observation.snapshot.market_type != config.strategy.config().market_type
            {
                return Err(VolumeMakerPaperTaskError::InvalidSourceBinding);
            }
            Ok(Some((
                observation.snapshot.clone(),
                observation.received_at,
            )))
        }
        MarketDataEvent::SourceGap { .. } | MarketDataEvent::SourceUnavailable { .. } => Ok(None),
    }
}

/// Evaluates durable account-risk close directives at the observed instant.
async fn account_risk_exit(
    context: &mut OwnerContext,
    snapshot: &MarketSnapshot,
    observed_at: DateTime<Utc>,
) -> Result<bool, VolumeMakerPaperTaskError> {
    let Some(risk) = context.config.account_risk.as_ref() else {
        return Ok(false);
    };
    let directives = risk
        .directives(observed_at)
        .await
        .map_err(VolumeMakerPaperTaskError::AccountRisk)?;
    let Some(reason) = account_risk_exit_reason(&directives, &context.config.task_id) else {
        return Ok(false);
    };
    let recorded_at = Utc::now().max(context.last_recorded_at);
    context
        .history
        .append(&account_risk_directive_record(
            &context.config.task_id,
            TASK_KIND,
            context.config.symbol().as_str(),
            &reason,
            &snapshot.mid_price().to_string(),
            recorded_at,
        ))
        .await
        .map_err(VolumeMakerPaperTaskError::Journal)?;
    context.last_recorded_at = recorded_at;
    Ok(true)
}

/// Decides what this observation executes, consuming the strategy plan.
///
/// Limit mode holds one virtual maker quote and only executes an open leg
/// after a later book crosses it; market mode opens on the strategy's
/// imbalance decision; an open position always closes first.
fn plan_operation(
    config: &VolumeMakerPaperTaskConfig,
    position: Option<OpenPosition>,
    quote: &mut Option<StandingQuote>,
    next_cycle_at: Option<DateTime<Utc>>,
    snapshot: &MarketSnapshot,
    observed_at: DateTime<Utc>,
) -> Result<Option<PlannedOperation>, VolumeMakerPaperTaskError> {
    if let Some(open) = position {
        let state = VolumeMakerState::Open {
            side: open.side,
            quantity: open.quantity,
        };
        let intents = config
            .strategy
            .evaluate(&state, snapshot)
            .map_err(VolumeMakerPaperTaskError::Strategy)?;
        let Some(intent) = intents.into_iter().next() else {
            return Ok(None);
        };
        let reference_price = match intent.side {
            Side::Buy => snapshot.ask(),
            Side::Sell => snapshot.bid(),
        };
        return Ok(Some(PlannedOperation {
            intent,
            kind: PlannedKind::Close,
            reference_price,
            admission_ticket: None,
        }));
    }
    if next_cycle_at.is_some_and(|at| observed_at < at) {
        return Ok(None);
    }
    match config.strategy.config().mode {
        VolumeMakerMode::LimitBoth => {
            if let Some(standing) = *quote {
                // Deterministic legacy semantics: exactly one side of the
                // quoted pair fills per cycle, whichever the book crossed
                // first; the other side is implicitly cancelled because the
                // owner keeps no resting orders. Buys are checked first.
                let quantity = config.strategy.config().order_quantity;
                if snapshot.ask() <= standing.bid {
                    let intent = OrderIntent::limit(
                        config.exchange().to_owned(),
                        config.symbol().clone(),
                        config.strategy.config().market_type,
                        Side::Buy,
                        quantity,
                        standing.bid,
                    );
                    return Ok(Some(PlannedOperation {
                        intent,
                        kind: PlannedKind::Open,
                        reference_price: standing.bid,
                        admission_ticket: None,
                    }));
                }
                if snapshot.bid() >= standing.ask {
                    let intent = OrderIntent::limit(
                        config.exchange().to_owned(),
                        config.symbol().clone(),
                        config.strategy.config().market_type,
                        Side::Sell,
                        quantity,
                        standing.ask,
                    );
                    return Ok(Some(PlannedOperation {
                        intent,
                        kind: PlannedKind::Open,
                        reference_price: standing.ask,
                        admission_ticket: None,
                    }));
                }
            }
            // Re-quote at the freshest book, mirroring the legacy
            // cancel-and-requote loop after an unfilled order timeout.
            *quote = Some(standing_quote(config, snapshot)?);
            Ok(None)
        }
        VolumeMakerMode::MarketImbalance => {
            match config.strategy.evaluate(&VolumeMakerState::Flat, snapshot) {
                Ok(intents) => {
                    let Some(intent) = intents.into_iter().next() else {
                        return Ok(None);
                    };
                    let reference_price = match intent.side {
                        Side::Buy => snapshot.ask(),
                        Side::Sell => snapshot.bid(),
                    };
                    Ok(Some(PlannedOperation {
                        intent,
                        kind: PlannedKind::Open,
                        reference_price,
                        admission_ticket: None,
                    }))
                }
                // The legacy service waits for a book with visible depth
                // instead of failing; a depth-free observation skips a cycle.
                Err(StrategyError::MissingMarketData(_)) => Ok(None),
                Err(error) => Err(VolumeMakerPaperTaskError::Strategy(error)),
            }
        }
    }
}

/// Consumes the strategy's flat-plan quote pair into one standing quote.
fn standing_quote(
    config: &VolumeMakerPaperTaskConfig,
    snapshot: &MarketSnapshot,
) -> Result<StandingQuote, VolumeMakerPaperTaskError> {
    let intents = config
        .strategy
        .evaluate(&VolumeMakerState::Flat, snapshot)
        .map_err(VolumeMakerPaperTaskError::Strategy)?;
    let bid = intents
        .iter()
        .find(|intent| intent.side == Side::Buy)
        .and_then(|intent| intent.price);
    let ask = intents
        .iter()
        .find(|intent| intent.side == Side::Sell)
        .and_then(|intent| intent.price);
    match (bid, ask) {
        (Some(bid), Some(ask)) => Ok(StandingQuote { bid, ask }),
        _ => Err(VolumeMakerPaperTaskError::InvalidRequest),
    }
}

/// Admits one open leg through the account-level risk authority.
async fn admit_open(
    context: &OwnerContext,
    operation: &PlannedOperation,
    observed_at: DateTime<Utc>,
) -> Result<Option<AccountRiskAdmissionTicket>, VolumeMakerPaperTaskError> {
    let Some(risk) = context.config.account_risk.as_ref() else {
        return Ok(None);
    };
    let notional = operation
        .reference_price
        .as_decimal()
        .checked_mul(operation.intent.quantity.as_decimal())
        .map(Money::new)
        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
    let candidate = AccountRiskCandidate::new(
        context.config.task_id.clone(),
        context.config.symbol().as_str(),
        notional,
    )
    .map_err(VolumeMakerPaperTaskError::AccountRisk)?;
    match risk
        .admit(&candidate, observed_at)
        .await
        .map_err(VolumeMakerPaperTaskError::AccountRisk)?
    {
        AccountRiskAdmission::Admitted { ticket, .. } => Ok(Some(ticket)),
        AccountRiskAdmission::Rejected(_) => Ok(None),
    }
}

fn build_request(
    config: &VolumeMakerPaperTaskConfig,
    operation: &PlannedOperation,
    operation_sequence: u64,
) -> Result<PaperSingleLegRequest, VolumeMakerPaperTaskError> {
    let intent = operation.intent.clone();
    let batch = ExecutionBatch::planned(vec![intent.clone()])?;
    let reserved_notional = operation
        .reference_price
        .as_decimal()
        .checked_mul(intent.quantity.as_decimal())
        .map(Money::new)
        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
    let task_id = format!("{}/op/{operation_sequence:06}", config.task_id);
    let idempotency_key = format!("volume:{operation_sequence:06}");
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
    PaperSingleLegRequest::new(config.symbol().clone(), batch, reservation)
        .map_err(VolumeMakerPaperTaskError::Saga)
}

/// Applies one terminal saga run to the owner's cycle state and statistics.
fn consume_run(
    operation: &PlannedOperation,
    run: &PaperSingleLegRun,
    position: &mut Option<OpenPosition>,
    quote: &mut Option<StandingQuote>,
    stats: &mut StatisticsBucket,
    total_volume: &mut Decimal,
) -> Result<CycleProgress, VolumeMakerPaperTaskError> {
    match run {
        PaperSingleLegRun::Completed { receipts } => {
            let fill_price = receipt_fill_price(receipts, operation.reference_price);
            match operation.kind {
                PlannedKind::Open => {
                    *position = Some(OpenPosition {
                        side: operation.intent.side,
                        quantity: operation.intent.quantity,
                        open_price: fill_price,
                    });
                    *quote = None;
                    Ok(CycleProgress::None)
                }
                PlannedKind::Close => {
                    let open = position
                        .take()
                        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                    // A bought position realizes close minus open; a sold
                    // position realizes open minus close.
                    let per_unit = match open.side {
                        Side::Buy => fill_price.checked_sub(open.open_price),
                        Side::Sell => open.open_price.checked_sub(fill_price),
                    }
                    .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                    let pnl = per_unit
                        .checked_mul(open.quantity.as_decimal())
                        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                    stats.realized_pnl = stats
                        .realized_pnl
                        .checked_add(pnl)
                        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                    // Legacy semantics: only the open leg's base quantity
                    // counts as generated volume.
                    let volume = open.quantity.as_decimal();
                    match open.side {
                        Side::Buy => {
                            stats.buy_volume = stats
                                .buy_volume
                                .checked_add(volume)
                                .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                        }
                        Side::Sell => {
                            stats.sell_volume = stats
                                .sell_volume
                                .checked_add(volume)
                                .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                        }
                    }
                    *total_volume = total_volume
                        .checked_add(volume)
                        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
                    stats.completed_cycles = stats.completed_cycles.saturating_add(1);
                    Ok(CycleProgress::CycleCompleted)
                }
            }
        }
        // A confirmed cancel (for example zero visible depth) releases the
        // reservation; the cycle simply retries on a later observation.
        PaperSingleLegRun::Cancelled { .. } => {
            if operation.kind == PlannedKind::Open {
                *quote = None;
            }
            Ok(CycleProgress::None)
        }
        PaperSingleLegRun::AlreadyTerminal { .. } => {
            Err(VolumeMakerPaperTaskError::RecoveryRequired)
        }
    }
}

fn receipt_fill_price(receipts: &[TradingReceipt], fallback: Price) -> Decimal {
    receipts
        .first()
        .and_then(|receipt| match receipt {
            TradingReceipt::Submitted { order, .. } => order.average_fill_price,
            TradingReceipt::Cancelled { .. } => None,
        })
        .map_or_else(|| fallback.as_decimal(), Price::as_decimal)
}

fn bounds_reached(
    config: &VolumeMakerPaperTaskConfig,
    completed_cycles: u64,
    total_volume: Decimal,
) -> bool {
    if let Some(max_cycles) = config.max_cycles
        && completed_cycles >= max_cycles
    {
        return true;
    }
    if let Some(target) = config.target_volume
        && total_volume >= target
    {
        return true;
    }
    false
}

fn hour_bucket(at: DateTime<Utc>) -> DateTime<Utc> {
    at.with_nanosecond(0)
        .and_then(|at| at.with_second(0))
        .and_then(|at| at.with_minute(0))
        .unwrap_or(at)
}

async fn checkpoint(
    context: &mut OwnerContext,
    next: &mut VolumeMakerPaperTaskStatus,
) -> Result<(), VolumeMakerPaperTaskError> {
    let recorded_at = Utc::now().max(context.last_recorded_at);
    next.sources = vec![context.source.status()];
    next.last_recorded_at = Some(recorded_at);
    context
        .history
        .append(&status_record(next, "task_checkpointed", recorded_at))
        .await
        .map_err(VolumeMakerPaperTaskError::Journal)?;
    context.last_recorded_at = recorded_at;
    context.status_sender.send_replace(next.clone());
    Ok(())
}

/// One bounded durable statistics fact mirroring the legacy hourly export.
async fn append_statistics(
    context: &mut OwnerContext,
    bucket: &StatisticsBucket,
    reason: &'static str,
) -> Result<(), VolumeMakerPaperTaskError> {
    let total = bucket
        .buy_volume
        .checked_add(bucket.sell_volume)
        .ok_or(VolumeMakerPaperTaskError::InvalidRequest)?;
    let recorded_at = Utc::now().max(context.last_recorded_at);
    context
        .history
        .append(&DecisionRecord {
            timestamp: recorded_at,
            strategy: VOLUME_MAKER_STRATEGY.to_owned(),
            symbol: context.config.symbol().to_string(),
            decision: "volume_maker_statistics".to_owned(),
            details: json!({
                "schema_version": VOLUME_MAKER_STATISTICS_SCHEMA_VERSION,
                "task_id": context.config.task_id,
                "task_kind": TASK_KIND,
                "bucket_start": bucket.hour_start.to_rfc3339(),
                "completed_cycles": bucket.completed_cycles,
                "buy_volume": bucket.buy_volume.to_string(),
                "sell_volume": bucket.sell_volume.to_string(),
                "total_volume": total.to_string(),
                "realized_pnl": bucket.realized_pnl.to_string(),
                "rejected_entries": bucket.rejected_entries,
                "reason": reason,
            }),
        })
        .await
        .map_err(VolumeMakerPaperTaskError::Journal)?;
    context.last_recorded_at = recorded_at;
    Ok(())
}

async fn stop_owner(
    context: &mut OwnerContext,
    bucket: Option<StatisticsBucket>,
    requested_exit: VolumeMakerPaperTaskExit,
) -> TaskResult {
    // Mirror the legacy export-on-stop: the final partial bucket becomes one
    // durable statistics fact before the lifecycle stops.
    if let Some(bucket) = bucket
        && !bucket.is_empty()
    {
        append_statistics(context, &bucket, "stop").await?;
    }
    let stopping_at = Utc::now().max(context.last_recorded_at);
    let mut stopping = context.status_sender.borrow().clone();
    stopping.phase = VolumeMakerPaperTaskPhase::Stopping;
    stopping.sources = vec![context.source.status()];
    stopping.last_recorded_at = Some(stopping_at);
    context
        .history
        .append(&status_record(&stopping, "task_stopping", stopping_at))
        .await
        .map_err(VolumeMakerPaperTaskError::Journal)?;
    context.status_sender.send_replace(stopping);
    context.last_recorded_at = stopping_at;

    let source_exit = context
        .source
        .stop()
        .await
        .map_err(VolumeMakerPaperTaskError::Source)?;
    let exit = if source_exit == MarketSupervisorExit::ShutdownTimedOut {
        VolumeMakerPaperTaskExit::ShutdownTimedOut
    } else {
        requested_exit
    };
    let stopped_at = Utc::now().max(context.last_recorded_at);
    let mut stopped = context.status_sender.borrow().clone();
    stopped.phase = VolumeMakerPaperTaskPhase::Stopped;
    stopped.sources = vec![context.source.status()];
    stopped.last_recorded_at = Some(stopped_at);
    stopped.exit = Some(exit);
    stopped.failure = None;
    context
        .history
        .append(&status_record(&stopped, "task_stopped", stopped_at))
        .await
        .map_err(VolumeMakerPaperTaskError::Journal)?;
    context.status_sender.send_replace(stopped);
    Ok(exit)
}

async fn fail_owner(
    context: &mut OwnerContext,
    failure: VolumeMakerPaperTaskFailure,
    error: VolumeMakerPaperTaskError,
) -> TaskResult {
    let _ = context.source.stop().await;
    let failed_at = Utc::now().max(context.last_recorded_at);
    let mut failed = context.status_sender.borrow().clone();
    failed.phase = VolumeMakerPaperTaskPhase::Failed;
    failed.sources = vec![context.source.status()];
    failed.last_recorded_at = Some(failed_at);
    failed.exit = None;
    failed.failure = Some(failure);
    if let Err(journal_error) = context
        .history
        .append(&status_record(&failed, "task_failed", failed_at))
        .await
    {
        return Err(VolumeMakerPaperTaskError::Journal(journal_error));
    }
    context.status_sender.send_replace(failed);
    Err(error)
}

fn classify_saga_error(
    error: PaperSingleLegSagaError,
) -> (VolumeMakerPaperTaskFailure, VolumeMakerPaperTaskError) {
    let failure = classify_saga_error_ref(&error);
    (failure, VolumeMakerPaperTaskError::Saga(error))
}

const fn classify_saga_error_ref(error: &PaperSingleLegSagaError) -> VolumeMakerPaperTaskFailure {
    match error {
        PaperSingleLegSagaError::RecoveryRequired { .. } => {
            VolumeMakerPaperTaskFailure::RecoveryRequired
        }
        PaperSingleLegSagaError::Account(_) => VolumeMakerPaperTaskFailure::AccountContract,
        PaperSingleLegSagaError::Journal(_) => VolumeMakerPaperTaskFailure::JournalUnavailable,
        PaperSingleLegSagaError::Execution(_) => VolumeMakerPaperTaskFailure::ExecutionFailed,
        PaperSingleLegSagaError::Incomplete(_) => VolumeMakerPaperTaskFailure::ExecutionIncomplete,
        PaperSingleLegSagaError::InvalidRequest(_) => VolumeMakerPaperTaskFailure::InvalidRequest,
    }
}

async fn recovery_preflight(
    task_id: &str,
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
) -> Result<u64, VolumeMakerPaperTaskError> {
    let account_snapshot = account.snapshot().await?;
    if account_snapshot.projection_status != ProjectionStatus::Complete {
        return Err(VolumeMakerPaperTaskError::RecoveryRequired);
    }
    if account_snapshot.reservations.iter().any(|reservation| {
        reservation
            .reconciliation
            .as_ref()
            .is_some_and(|record| record.outcome == PaperReconciliationOutcome::Failed)
    }) {
        return Err(VolumeMakerPaperTaskError::RecoveryRequired);
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
            PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
        ) {
            return Err(VolumeMakerPaperTaskError::RecoveryRequired);
        }
        let suffix = reservation
            .task_id
            .strip_prefix(&prefix)
            .unwrap_or_default();
        let sequence = suffix
            .parse::<u64>()
            .map_err(|_| VolumeMakerPaperTaskError::RecoveryRequired)?;
        last_operation = last_operation.max(sequence);
    }

    if let Some(task) = durable_task_view(account, history.path(), task_id).await?
        && (task.phase != ReadOnlyTaskPhase::Stopped || task.recovery != ReadOnlyTaskRecovery::None)
    {
        return Err(VolumeMakerPaperTaskError::RecoveryRequired);
    }
    Ok(last_operation)
}

async fn durable_task_view(
    account: &PaperAccountAuthority,
    path: &Path,
    task_id: &str,
) -> Result<Option<ReadOnlyTaskView>, VolumeMakerPaperTaskError> {
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
    .map_err(|_| VolumeMakerPaperTaskError::SnapshotTaskFailed)??;
    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot)?;
    if model.projection_status != ProjectionStatus::Complete {
        return Err(VolumeMakerPaperTaskError::RecoveryRequired);
    }
    Ok(model.tasks.into_iter().find(|task| task.task_id == task_id))
}

fn operation_prefix(task_id: &str) -> String {
    format!("{task_id}/op/")
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
    status: &VolumeMakerPaperTaskStatus,
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
            "task_kind": TASK_KIND,
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

const fn task_phase_label(phase: VolumeMakerPaperTaskPhase) -> &'static str {
    match phase {
        VolumeMakerPaperTaskPhase::Running => "running",
        VolumeMakerPaperTaskPhase::Stopping => "stopping",
        VolumeMakerPaperTaskPhase::Stopped => "stopped",
        VolumeMakerPaperTaskPhase::Failed => "failed",
    }
}

const fn task_exit_label(exit: VolumeMakerPaperTaskExit) -> &'static str {
    match exit {
        VolumeMakerPaperTaskExit::StopRequested => "stop_requested",
        VolumeMakerPaperTaskExit::SourceEnded => "source_ended",
        VolumeMakerPaperTaskExit::ShutdownTimedOut => "shutdown_timed_out",
        // The bounded max-cycles/target-volume stop is a completed run in the
        // shared durable task contract.
        VolumeMakerPaperTaskExit::BoundsReached => "completed",
    }
}

const fn task_failure_label(failure: VolumeMakerPaperTaskFailure) -> &'static str {
    match failure {
        VolumeMakerPaperTaskFailure::StartupFailed => "startup_failed",
        VolumeMakerPaperTaskFailure::SourceContract => "source_contract",
        VolumeMakerPaperTaskFailure::JournalUnavailable => "journal_unavailable",
        VolumeMakerPaperTaskFailure::TaskPanicked => "task_panicked",
        VolumeMakerPaperTaskFailure::TaskCancelled => "task_cancelled",
        VolumeMakerPaperTaskFailure::InvalidRequest => "invalid_request",
        VolumeMakerPaperTaskFailure::RecoveryRequired => "recovery_required",
        VolumeMakerPaperTaskFailure::AccountContract => "account_contract",
        VolumeMakerPaperTaskFailure::ExecutionIncomplete => "execution_incomplete",
        VolumeMakerPaperTaskFailure::ExecutionFailed => "execution_failed",
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
pub enum VolumeMakerPaperTaskError {
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
    PreviouslyFailed(VolumeMakerPaperTaskFailure),
}

impl VolumeMakerPaperTaskError {
    const fn failure_bucket(&self) -> VolumeMakerPaperTaskFailure {
        match self {
            Self::InvalidConfig | Self::InvalidSourceBinding | Self::InvalidRequest => {
                VolumeMakerPaperTaskFailure::InvalidRequest
            }
            Self::RecoveryRequired | Self::ShutdownTimedOut => {
                VolumeMakerPaperTaskFailure::RecoveryRequired
            }
            Self::Journal(_) | Self::JournalRead(_) | Self::Projection(_) => {
                VolumeMakerPaperTaskFailure::JournalUnavailable
            }
            Self::Account(_) | Self::AccountRisk(_) => VolumeMakerPaperTaskFailure::AccountContract,
            Self::Source(_) => VolumeMakerPaperTaskFailure::SourceContract,
            Self::Strategy(_) | Self::Runtime(_) => VolumeMakerPaperTaskFailure::InvalidRequest,
            Self::Saga(error) => classify_saga_error_ref(error),
            Self::TaskPanicked => VolumeMakerPaperTaskFailure::TaskPanicked,
            Self::TaskCancelled | Self::SnapshotTaskFailed => {
                VolumeMakerPaperTaskFailure::TaskCancelled
            }
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

impl From<PaperAccountError> for VolumeMakerPaperTaskError {
    fn from(value: PaperAccountError) -> Self {
        Self::Account(value)
    }
}

impl From<PaperAdmissionCompensationError> for VolumeMakerPaperTaskError {
    fn from(value: PaperAdmissionCompensationError) -> Self {
        match value {
            PaperAdmissionCompensationError::Account(error) => Self::Account(error),
            PaperAdmissionCompensationError::AccountRisk(error) => Self::AccountRisk(error),
            PaperAdmissionCompensationError::RecoveryRequired => Self::RecoveryRequired,
        }
    }
}

impl From<JournalReadError> for VolumeMakerPaperTaskError {
    fn from(value: JournalReadError) -> Self {
        Self::JournalRead(value)
    }
}

impl From<ReadModelError> for VolumeMakerPaperTaskError {
    fn from(value: ReadModelError) -> Self {
        Self::Projection(value)
    }
}

impl From<RuntimeError> for VolumeMakerPaperTaskError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl fmt::Display for VolumeMakerPaperTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid volume-maker task configuration"),
            Self::InvalidSourceBinding => {
                formatter.write_str("volume-maker source does not match its exact owner binding")
            }
            Self::InvalidRequest => formatter.write_str("volume-maker operation is invalid"),
            Self::RecoveryRequired => {
                formatter.write_str("volume-maker durable state requires reconciliation")
            }
            Self::ShutdownTimedOut => {
                formatter.write_str("volume-maker shutdown timed out; recovery is required")
            }
            Self::SnapshotTaskFailed => formatter.write_str("volume-maker snapshot worker failed"),
            Self::Journal(error) => error.fmt(formatter),
            Self::JournalRead(error) => error.fmt(formatter),
            Self::Projection(error) => error.fmt(formatter),
            Self::Account(error) => error.fmt(formatter),
            Self::AccountRisk(error) => error.fmt(formatter),
            Self::Source(error) => error.fmt(formatter),
            Self::Strategy(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Saga(error) => error.fmt(formatter),
            Self::TaskPanicked => formatter.write_str("volume-maker task panicked"),
            Self::TaskCancelled => formatter.write_str("volume-maker task was cancelled"),
            Self::PreviouslyFailed(failure) => {
                write!(formatter, "volume-maker task already failed: {failure:?}")
            }
        }
    }
}

impl Error for VolumeMakerPaperTaskError {
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
