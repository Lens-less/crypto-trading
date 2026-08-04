//! Durable, read-only owner for one single-source virtual-grid scanner replay.
//!
//! The public seam deliberately exposes only `start`, `status`, and `stop`.
//! Source supervision, journal ordering, and Tokio lifecycle machinery remain
//! private. The owner holds no exchange execution handle: it accumulates
//! validated replay observations and publishes exactly one durable ranking
//! fact through the existing deterministic scanner before it stops.

use std::{fmt, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use crypto_trading_domain::Price;
use crypto_trading_runtime::{
    DecisionRecord, HistoryError, JsonlHistory, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
    MarketDataEvent, MarketDataEventFuture, MarketDataEventSource, MarketInstrument,
    MarketSupervisor, MarketSupervisorConfig, MarketSupervisorError, MarketSupervisorExit,
    MarketSupervisorHealth, MarketSupervisorPhase, MarketSupervisorStatus,
};
use crypto_trading_strategy::AprEstimateAssumptions;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    sync::{Semaphore, watch},
    task::{JoinError, JoinHandle},
    time::Instant,
};

use crate::scanner::{
    DeterministicVirtualGridScanner, MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS,
    MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS_PER_CANDIDATE, ScannerPriority, VirtualGridScanCandidate,
    VirtualGridScanObservation, VirtualGridScanRequest, VirtualGridScannerError,
};
use crate::task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture};

/// Stable schema version for the process-local task status surface.
pub const CONTINUOUS_SCANNER_TASK_STATUS_SCHEMA_VERSION: u16 = 1;

const TASK_RECORD_SCHEMA_VERSION: u16 = 1;
const MAX_TASK_ID_BYTES: usize = 128;
const TASK_KIND: &str = "scanner";
const TASK_STRATEGY: &str = "read_only_task";
const TASK_SYMBOL: &str = "control-plane";

/// One exact scanner candidate before any replay observation arrives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannerCandidateSpec {
    pub instrument: MarketInstrument,
    pub grid_width_percent: Decimal,
    pub grid_interval_percent: Decimal,
    pub apr_estimate_assumptions: AprEstimateAssumptions,
    pub volume_24h_usdc: Decimal,
    pub price_change_24h_percent: Option<Decimal>,
    pub benchmark: bool,
}

impl ScannerCandidateSpec {
    const fn priority(&self) -> ScannerPriority {
        if self.benchmark {
            ScannerPriority::Benchmark
        } else {
            ScannerPriority::Standard
        }
    }
}

#[derive(Debug)]
struct CandidateState {
    spec: ScannerCandidateSpec,
    last_observed_at: Option<DateTime<Utc>>,
    observations: Vec<VirtualGridScanObservation>,
}

/// Bounded accumulator that turns one exact replay into one durable ranking.
///
/// Construction validates the complete scan plan (identity, bounds, duplicate
/// instruments, and virtual-grid geometry) through the deterministic scanner's
/// own request validation, so a serve host fails closed before any source
/// starts.
#[derive(Debug)]
pub struct ScannerReplayRuntime {
    run_id: String,
    apr_window_seconds: u32,
    apr_estimate_assumptions: AprEstimateAssumptions,
    min_complete_cycles: u64,
    row_limit: usize,
    total_observations: usize,
    candidates: Vec<CandidateState>,
    progress: Arc<Semaphore>,
}

impl ScannerReplayRuntime {
    /// Creates one validated replay-to-ranking plan.
    ///
    /// # Errors
    ///
    /// Returns [`VirtualGridScannerError`] for an invalid run identity, scan
    /// bound, duplicate instrument, or unusable virtual-grid geometry.
    pub fn new(
        run_id: impl Into<String>,
        apr_window_seconds: u32,
        apr_estimate_assumptions: AprEstimateAssumptions,
        min_complete_cycles: u64,
        row_limit: usize,
        specs: Vec<ScannerCandidateSpec>,
    ) -> Result<Self, VirtualGridScannerError> {
        let run_id = run_id.into();
        let mut probe = Vec::with_capacity(specs.len());
        for spec in &specs {
            probe.push(probe_candidate(spec)?);
        }
        let validated = VirtualGridScanRequest::new(
            run_id.clone(),
            DateTime::<Utc>::UNIX_EPOCH,
            apr_window_seconds,
            apr_estimate_assumptions,
            min_complete_cycles,
            row_limit,
            probe,
        )?;
        drop(validated);
        Ok(Self {
            run_id,
            apr_window_seconds,
            apr_estimate_assumptions,
            min_complete_cycles,
            row_limit,
            total_observations: 0,
            candidates: specs
                .into_iter()
                .map(|spec| CandidateState {
                    spec,
                    last_observed_at: None,
                    observations: Vec::new(),
                })
                .collect(),
            progress: Arc::new(Semaphore::new(1)),
        })
    }

    /// Wraps one replay source so it yields the next event only after this
    /// runtime has processed the previous one.
    ///
    /// The market supervisor retains only the latest event (O(1) retention),
    /// so an unpaced finite replay would be compressed into a source gap plus
    /// its final observation whenever the journaling consumer lags. A
    /// deterministic ranking requires every observation, which makes this
    /// pacing part of the scan contract rather than an optimization.
    pub fn pace<S>(&self, source: S) -> PacedReplaySource<S>
    where
        S: MarketDataEventSource,
    {
        PacedReplaySource {
            inner: source,
            permits: Arc::clone(&self.progress),
        }
    }

    fn process(&mut self, event: &MarketDataEvent) -> Result<(), ScannerReplayProcessError> {
        let MarketDataEvent::Observation(observation) = event else {
            self.progress.add_permits(1);
            return Ok(());
        };
        let instrument = MarketInstrument::from_snapshot(&observation.snapshot)
            .map_err(|_| ScannerReplayProcessError::UnknownInstrument)?;
        let Some(candidate) = self
            .candidates
            .iter_mut()
            .find(|candidate| candidate.spec.instrument == instrument)
        else {
            return Err(ScannerReplayProcessError::UnknownInstrument);
        };
        if candidate.observations.len() >= MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS_PER_CANDIDATE
            || self.total_observations >= MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS
        {
            return Err(ScannerReplayProcessError::ObservationBudgetExhausted);
        }
        let observed_at = observation.snapshot.timestamp;
        if candidate
            .last_observed_at
            .is_some_and(|previous| observed_at < previous)
        {
            return Err(ScannerReplayProcessError::NonMonotonicObservation);
        }
        let sequence = u64::try_from(candidate.observations.len())
            .map_err(|_| ScannerReplayProcessError::ObservationBudgetExhausted)?
            .saturating_add(1);
        let price = observation
            .snapshot
            .last
            .unwrap_or_else(|| observation.snapshot.mid_price());
        let observation = VirtualGridScanObservation::new(sequence, price, observed_at)
            .map_err(|_| ScannerReplayProcessError::ObservationBudgetExhausted)?;
        candidate.observations.push(observation);
        candidate.last_observed_at = Some(observed_at);
        self.total_observations = self.total_observations.saturating_add(1);
        self.progress.add_permits(1);
        Ok(())
    }

    /// Ranks every observed candidate and durably records the result.
    ///
    /// Returns `Ok(false)` when the replay produced no observation at all, so
    /// a clean stop never fabricates an empty ranking fact.
    async fn finish(&mut self, history: &JsonlHistory) -> Result<bool, VirtualGridScannerError> {
        let Some(evaluated_at) = self
            .candidates
            .iter()
            .filter_map(|candidate| candidate.last_observed_at)
            .max()
        else {
            return Ok(false);
        };
        let mut candidates = Vec::new();
        for state in self
            .candidates
            .drain(..)
            .filter(|state| !state.observations.is_empty())
        {
            candidates.push(VirtualGridScanCandidate::new(
                state.spec.instrument.clone(),
                state.spec.grid_width_percent,
                state.spec.grid_interval_percent,
                state.spec.volume_24h_usdc,
                state.spec.price_change_24h_percent,
                state.spec.priority(),
                state.observations,
            )?);
        }
        let request = VirtualGridScanRequest::new(
            self.run_id.clone(),
            evaluated_at,
            self.apr_window_seconds,
            self.apr_estimate_assumptions,
            self.min_complete_cycles,
            self.row_limit,
            candidates,
        )?;
        DeterministicVirtualGridScanner::run_and_record(request, history).await?;
        Ok(true)
    }
}

fn probe_candidate(
    spec: &ScannerCandidateSpec,
) -> Result<VirtualGridScanCandidate, VirtualGridScannerError> {
    let probe_price =
        Price::new(Decimal::ONE).map_err(|_| VirtualGridScannerError::InvalidGridProbe)?;
    VirtualGridScanCandidate::new(
        spec.instrument.clone(),
        spec.grid_width_percent,
        spec.grid_interval_percent,
        spec.volume_24h_usdc,
        spec.price_change_24h_percent,
        spec.priority(),
        vec![VirtualGridScanObservation::new(
            1,
            probe_price,
            DateTime::<Utc>::UNIX_EPOCH,
        )?],
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScannerReplayProcessError {
    UnknownInstrument,
    NonMonotonicObservation,
    ObservationBudgetExhausted,
}

/// One replay source gated on the paired [`ScannerReplayRuntime`]'s progress.
///
/// At most one event is ever in flight between the supervisor's O(1) retention
/// slot and the owner, so a finite deterministic replay can never be
/// compressed into a synthesized source gap.
#[derive(Debug)]
pub struct PacedReplaySource<S> {
    inner: S,
    permits: Arc<Semaphore>,
}

impl<S> MarketDataEventSource for PacedReplaySource<S>
where
    S: MarketDataEventSource,
{
    fn source_id(&self) -> &str {
        self.inner.source_id()
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(async move {
            match self.permits.acquire().await {
                Ok(permit) => permit.forget(),
                Err(_) => return Ok(None),
            }
            self.inner.next_event().await
        })
    }
}

/// Validated configuration for one exact single-source scanner owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuousScannerTaskConfig {
    task_id: String,
    source_id: String,
    supervisor: MarketSupervisorConfig,
}

impl ContinuousScannerTaskConfig {
    /// Creates one stable task identity, exact source binding, and bounded
    /// source shutdown policy.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuousScannerTaskError::InvalidConfig`] for an empty,
    /// oversized, or transport-unsafe task or source identity.
    pub fn new(
        task_id: impl Into<String>,
        source_id: impl Into<String>,
        supervisor: MarketSupervisorConfig,
    ) -> Result<Self, ContinuousScannerTaskError> {
        let task_id = task_id.into();
        let task_id = task_id.trim();
        let source_id = source_id.into();
        let source_id = source_id.trim();
        if !safe_identity(task_id) || !safe_identity(source_id) {
            return Err(ContinuousScannerTaskError::InvalidConfig);
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
pub enum ContinuousScannerTaskPhase {
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl ContinuousScannerTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

/// Bounded normal terminal reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousScannerTaskExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Bounded failure bucket suitable for operator status and durable facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContinuousScannerTaskFailure {
    StartupFailed,
    SourceContract,
    ScanContract,
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
pub struct ContinuousScannerTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: ContinuousScannerTaskPhase,
    pub processed_event_count: u64,
    pub sources: Vec<MarketSupervisorStatus>,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub exit: Option<ContinuousScannerTaskExit>,
    pub failure: Option<ContinuousScannerTaskFailure>,
    pub runtime_failure: Option<ContinuousScannerTaskFailure>,
}

impl ContinuousScannerTaskStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

/// Opaque owner of one source supervisor and one replay-accumulation loop.
#[derive(Debug)]
pub struct ContinuousScannerTask {
    stop: watch::Sender<bool>,
    status_sender: watch::Sender<ContinuousScannerTaskStatus>,
    status: watch::Receiver<ContinuousScannerTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<ContinuousScannerTaskExit, ContinuousScannerTaskFailure>>,
    shutdown_grace: Duration,
    history: JsonlHistory,
}

impl ContinuousScannerTask {
    /// Durably registers and starts one exact single-source read-only scanner
    /// owner.
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
        config: ContinuousScannerTaskConfig,
        runtime: ScannerReplayRuntime,
        source: S,
        history: JsonlHistory,
    ) -> Result<Self, ContinuousScannerTaskError>
    where
        S: MarketDataEventSource,
    {
        if source.source_id() != config.source_id {
            return Err(ContinuousScannerTaskError::InvalidSourceBinding);
        }

        let registered_at = Utc::now();
        history
            .append(&registered_record(
                &config.task_id,
                &config.source_id,
                registered_at,
            ))
            .await
            .map_err(ContinuousScannerTaskError::Journal)?;

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
                .map_err(ContinuousScannerTaskError::Journal)?;
            return Err(ContinuousScannerTaskError::SourceContract);
        };

        let running_at = Utc::now().max(registered_at);
        let initial = ContinuousScannerTaskStatus {
            schema_version: CONTINUOUS_SCANNER_TASK_STATUS_SCHEMA_VERSION,
            task_id: config.task_id.clone(),
            phase: ContinuousScannerTaskPhase::Running,
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
            return Err(ContinuousScannerTaskError::Journal(error));
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
            shutdown_grace: config.supervisor.shutdown_grace(),
            history,
        })
    }

    /// Returns the latest status without waiting or exposing the watch channel.
    #[must_use]
    pub fn status(&self) -> ContinuousScannerTaskStatus {
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
    pub async fn stop(&mut self) -> Result<ContinuousScannerTaskExit, ContinuousScannerTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(ContinuousScannerTaskError::PreviouslyFailed);
        }
        let _ = self.stop.send(true);
        let Some(mut join) = self.join.take() else {
            return Err(ContinuousScannerTaskError::TaskCancelled);
        };
        let started_at = Instant::now();
        let owner_deadline = started_at + self.shutdown_grace;
        let shutdown_deadline = started_at + self.shutdown_grace.saturating_mul(2);
        let result = if let Ok(joined) = tokio::time::timeout_at(owner_deadline, &mut join).await {
            self.map_join_result(joined, shutdown_deadline).await
        } else {
            join.abort();
            self.record_forced_exit(
                ContinuousScannerTaskExit::ShutdownTimedOut,
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
    ) -> Result<ContinuousScannerTaskExit, ContinuousScannerTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) if error.is_panic() => {
                self.record_external_failure(ContinuousScannerTaskFailure::TaskPanicked, deadline)
                    .await?;
                Err(ContinuousScannerTaskError::TaskPanicked)
            }
            Err(_) => {
                self.record_external_failure(ContinuousScannerTaskFailure::TaskCancelled, deadline)
                    .await?;
                Err(ContinuousScannerTaskError::TaskCancelled)
            }
        }
    }

    async fn record_forced_exit(
        &mut self,
        exit: ContinuousScannerTaskExit,
        deadline: Instant,
    ) -> Result<ContinuousScannerTaskExit, ContinuousScannerTaskError> {
        let mut status = self.status();
        status.phase = ContinuousScannerTaskPhase::Stopped;
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
                    ContinuousScannerTaskFailure::JournalUnavailable,
                );
                return Err(ContinuousScannerTaskError::Journal(error));
            }
            Err(_) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousScannerTaskFailure::TaskCancelled,
                );
                return Err(ContinuousScannerTaskError::TaskCancelled);
            }
        }
        self.status_sender.send_replace(status);
        Ok(exit)
    }

    async fn record_external_failure(
        &mut self,
        failure: ContinuousScannerTaskFailure,
        deadline: Instant,
    ) -> Result<(), ContinuousScannerTaskError> {
        let mut status = self.status();
        status.phase = ContinuousScannerTaskPhase::Failed;
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
                    ContinuousScannerTaskFailure::JournalUnavailable,
                );
                return Err(ContinuousScannerTaskError::Journal(error));
            }
            Err(_) => {
                publish_runtime_failure(
                    &self.status_sender,
                    ContinuousScannerTaskFailure::TaskCancelled,
                );
                return Err(ContinuousScannerTaskError::TaskCancelled);
            }
        }
        self.status_sender.send_replace(status);
        Ok(())
    }
}

impl Drop for ContinuousScannerTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type TaskResult = Result<ContinuousScannerTaskExit, ContinuousScannerTaskError>;

async fn run_owner(
    mut runtime: ScannerReplayRuntime,
    mut source: MarketSupervisor,
    history: JsonlHistory,
    status_sender: watch::Sender<ContinuousScannerTaskStatus>,
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
            result = source.next_event() => Selected::Source(result),
        };
        match selected {
            Selected::Stop => {
                return stop_owner(
                    &mut runtime,
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousScannerTaskExit::StopRequested,
                )
                .await;
            }
            Selected::Source(Ok(Some(event))) => {
                if let Some(terminal) = apply_event(
                    &mut runtime,
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    &event,
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
                    ContinuousScannerTaskExit::SourceEnded,
                )
                .await;
            }
            Selected::Source(Err(_)) => {
                return fail_owner(
                    &mut source,
                    &history,
                    &status_sender,
                    &mut last_recorded_at,
                    ContinuousScannerTaskFailure::SourceContract,
                )
                .await;
            }
        }
    }
}

enum Selected {
    Stop,
    Source(Result<Option<MarketDataEvent>, MarketSupervisorError>),
}

/// Applies one market event and durably checkpoints the owner status.
///
/// Returns `Some` when the owner reached a terminal outcome.
async fn apply_event(
    runtime: &mut ScannerReplayRuntime,
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousScannerTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    event: &MarketDataEvent,
) -> Option<TaskResult> {
    if runtime.process(event).is_err() {
        return Some(
            fail_owner(
                source,
                history,
                status_sender,
                last_recorded_at,
                ContinuousScannerTaskFailure::ScanContract,
            )
            .await,
        );
    }
    let mut next = status_sender.borrow().clone();
    let Some(processed_event_count) = next.processed_event_count.checked_add(1) else {
        return Some(
            fail_owner(
                source,
                history,
                status_sender,
                last_recorded_at,
                ContinuousScannerTaskFailure::ScanContract,
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
        let _ = source.stop().await;
        publish_runtime_failure(
            status_sender,
            ContinuousScannerTaskFailure::JournalUnavailable,
        );
        return Some(Err(ContinuousScannerTaskError::Journal(error)));
    }
    *last_recorded_at = recorded_at;
    status_sender.send_replace(next);
    None
}

async fn stop_owner(
    runtime: &mut ScannerReplayRuntime,
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousScannerTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    requested_exit: ContinuousScannerTaskExit,
) -> TaskResult {
    let stopping_at = Utc::now().max(*last_recorded_at);
    let mut stopping = status_sender.borrow().clone();
    stopping.phase = ContinuousScannerTaskPhase::Stopping;
    stopping.sources = vec![source.status()];
    stopping.last_recorded_at = Some(stopping_at);
    stopping.runtime_failure = None;
    if let Err(error) = history
        .append(&status_record(&stopping, "task_stopping", stopping_at))
        .await
    {
        let _ = source.stop().await;
        publish_runtime_failure(
            status_sender,
            ContinuousScannerTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousScannerTaskError::Journal(error));
    }
    status_sender.send_replace(stopping);
    *last_recorded_at = stopping_at;

    let Ok(source_exit) = source.stop().await else {
        return fail_owner(
            source,
            history,
            status_sender,
            last_recorded_at,
            ContinuousScannerTaskFailure::SourceContract,
        )
        .await;
    };

    // The ranking fact is durably appended before the terminal lifecycle
    // record, so a projected `stopped` task always implies the scan result
    // (when any observation existed) is already committed.
    match runtime.finish(history).await {
        Ok(_) => {}
        Err(VirtualGridScannerError::Journal(error)) => {
            publish_runtime_failure(
                status_sender,
                ContinuousScannerTaskFailure::JournalUnavailable,
            );
            return Err(ContinuousScannerTaskError::Journal(error));
        }
        Err(_) => {
            return fail_owner(
                source,
                history,
                status_sender,
                last_recorded_at,
                ContinuousScannerTaskFailure::ScanContract,
            )
            .await;
        }
    }
    let aggregate_exit = aggregate_stop_exit(requested_exit, source_exit);

    let stopped_at = Utc::now().max(*last_recorded_at);
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = ContinuousScannerTaskPhase::Stopped;
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
            ContinuousScannerTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousScannerTaskError::Journal(error));
    }
    status_sender.send_replace(stopped);
    Ok(aggregate_exit)
}

async fn fail_owner(
    source: &mut MarketSupervisor,
    history: &JsonlHistory,
    status_sender: &watch::Sender<ContinuousScannerTaskStatus>,
    last_recorded_at: &mut DateTime<Utc>,
    failure: ContinuousScannerTaskFailure,
) -> TaskResult {
    let _ = source.stop().await;
    let failed_at = Utc::now().max(*last_recorded_at);
    let mut failed = status_sender.borrow().clone();
    failed.phase = ContinuousScannerTaskPhase::Failed;
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
            ContinuousScannerTaskFailure::JournalUnavailable,
        );
        return Err(ContinuousScannerTaskError::Journal(journal_error));
    }
    status_sender.send_replace(failed);
    Err(failure.into_error())
}

const fn aggregate_stop_exit(
    requested: ContinuousScannerTaskExit,
    source: MarketSupervisorExit,
) -> ContinuousScannerTaskExit {
    if matches!(source, MarketSupervisorExit::ShutdownTimedOut) {
        ContinuousScannerTaskExit::ShutdownTimedOut
    } else {
        requested
    }
}

fn publish_runtime_failure(
    status_sender: &watch::Sender<ContinuousScannerTaskStatus>,
    failure: ContinuousScannerTaskFailure,
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
    status: &ContinuousScannerTaskStatus,
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
        "task_kind": TASK_KIND,
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

const fn task_phase_label(phase: ContinuousScannerTaskPhase) -> &'static str {
    match phase {
        ContinuousScannerTaskPhase::Running => "running",
        ContinuousScannerTaskPhase::Stopping => "stopping",
        ContinuousScannerTaskPhase::Stopped => "stopped",
        ContinuousScannerTaskPhase::Failed => "failed",
    }
}

impl fmt::Display for ContinuousScannerTaskPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_phase_label(*self))
    }
}

const fn task_exit_label(exit: ContinuousScannerTaskExit) -> &'static str {
    match exit {
        ContinuousScannerTaskExit::StopRequested => "stop_requested",
        ContinuousScannerTaskExit::SourceEnded => "source_ended",
        ContinuousScannerTaskExit::ShutdownTimedOut => "shutdown_timed_out",
    }
}

impl fmt::Display for ContinuousScannerTaskExit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(task_exit_label(*self))
    }
}

/// Durable buckets stay inside the closed task read-model failure vocabulary:
/// a scan-plan contract breach is recorded as `invalid_request`.
const fn task_failure_label(failure: ContinuousScannerTaskFailure) -> &'static str {
    match failure {
        ContinuousScannerTaskFailure::StartupFailed => "startup_failed",
        ContinuousScannerTaskFailure::SourceContract => "source_contract",
        ContinuousScannerTaskFailure::ScanContract => "invalid_request",
        ContinuousScannerTaskFailure::JournalUnavailable => "journal_unavailable",
        ContinuousScannerTaskFailure::TaskPanicked => "task_panicked",
        ContinuousScannerTaskFailure::TaskCancelled => "task_cancelled",
    }
}

impl fmt::Display for ContinuousScannerTaskFailure {
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

impl ContinuousScannerTaskFailure {
    const fn into_error(self) -> ContinuousScannerTaskError {
        match self {
            Self::StartupFailed => ContinuousScannerTaskError::InvalidConfig,
            Self::SourceContract => ContinuousScannerTaskError::SourceContract,
            Self::ScanContract => ContinuousScannerTaskError::ScanContract,
            Self::JournalUnavailable | Self::TaskCancelled => {
                ContinuousScannerTaskError::TaskCancelled
            }
            Self::TaskPanicked => ContinuousScannerTaskError::TaskPanicked,
        }
    }
}

/// Typed construction/runtime failures. Only journal errors retain local
/// diagnostics; durable records contain bounded failure buckets.
#[derive(Debug)]
pub enum ContinuousScannerTaskError {
    InvalidConfig,
    InvalidSourceBinding,
    Journal(HistoryError),
    SourceContract,
    ScanContract,
    TaskPanicked,
    TaskCancelled,
    PreviouslyFailed(ContinuousScannerTaskFailure),
}

impl ContinuousScannerTaskError {
    const fn failure_bucket(&self) -> ContinuousScannerTaskFailure {
        match self {
            Self::InvalidConfig | Self::InvalidSourceBinding => {
                ContinuousScannerTaskFailure::StartupFailed
            }
            Self::Journal(_) => ContinuousScannerTaskFailure::JournalUnavailable,
            Self::SourceContract => ContinuousScannerTaskFailure::SourceContract,
            Self::ScanContract => ContinuousScannerTaskFailure::ScanContract,
            Self::TaskPanicked => ContinuousScannerTaskFailure::TaskPanicked,
            Self::TaskCancelled => ContinuousScannerTaskFailure::TaskCancelled,
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

impl fmt::Display for ContinuousScannerTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid continuous scanner task config"),
            Self::InvalidSourceBinding => {
                formatter.write_str("continuous scanner source does not match its exact binding")
            }
            Self::Journal(source) => {
                write!(formatter, "continuous scanner journal failed: {source}")
            }
            Self::SourceContract => {
                formatter.write_str("continuous scanner source contract failed")
            }
            Self::ScanContract => {
                formatter.write_str("continuous scanner replay-scan contract failed")
            }
            Self::TaskPanicked => formatter.write_str("continuous scanner task panicked"),
            Self::TaskCancelled => {
                formatter.write_str("continuous scanner task was cancelled unexpectedly")
            }
            Self::PreviouslyFailed(failure) => {
                write!(
                    formatter,
                    "continuous scanner task previously failed: {failure:?}"
                )
            }
        }
    }
}

impl std::error::Error for ContinuousScannerTaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(source) => Some(source),
            Self::InvalidConfig
            | Self::InvalidSourceBinding
            | Self::SourceContract
            | Self::ScanContract
            | Self::TaskPanicked
            | Self::TaskCancelled
            | Self::PreviouslyFailed(_) => None,
        }
    }
}

impl TaskHostStatus for ContinuousScannerTaskStatus {
    fn is_terminal(&self) -> bool {
        ContinuousScannerTaskStatus::is_terminal(self)
    }
}

impl TaskHost for ContinuousScannerTask {
    type Status = ContinuousScannerTaskStatus;
    type Exit = ContinuousScannerTaskExit;
    type Error = ContinuousScannerTaskError;

    fn status(&self) -> Self::Status {
        ContinuousScannerTask::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(async move { ContinuousScannerTask::stop(self).await })
    }
}

#[cfg(test)]
mod tests {
    use super::{ContinuousScannerTaskExit, MarketSupervisorExit, aggregate_stop_exit};

    #[test]
    fn source_shutdown_timeout_is_never_downgraded_to_a_normal_task_exit() {
        assert_eq!(
            aggregate_stop_exit(
                ContinuousScannerTaskExit::StopRequested,
                MarketSupervisorExit::ShutdownTimedOut,
            ),
            ContinuousScannerTaskExit::ShutdownTimedOut
        );
        assert_eq!(
            aggregate_stop_exit(
                ContinuousScannerTaskExit::SourceEnded,
                MarketSupervisorExit::SourceEnded,
            ),
            ContinuousScannerTaskExit::SourceEnded
        );
    }
}
