//! Durable Binance Testnet soak owner and offline evidence verifier.
//!
//! The default production mode is read-only. An optional exact lifecycle is
//! Testnet-only and acknowledgement-gated; its mutation/recovery facts remain
//! separate from the bounded observation samples used by this task. Transport
//! errors, response bodies, credentials, and other free-form text never enter
//! the decision journal.

use std::{
    fmt::{self, Write as _},
    future::Future,
    io,
    path::Path,
    pin::Pin,
    thread,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::sha256_digest;
use crypto_trading_runtime::{
    DecisionRecord, HistoryError, HistoryTailRepairOutcome, JournalReadError, JsonlHistory,
    MAX_HISTORY_RECORD_BYTES, read_journal_chain,
};
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};
use uuid::Uuid;

use crate::continuous_testnet::CONTINUOUS_TESTNET_OWNER_SCHEMA_VERSION;
use crate::task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture};

/// Current process-local and durable fact schema.
pub const TESTNET_SOAK_SCHEMA_VERSION: u16 = 2;
/// Durable task kind for the owner-backed Testnet soak host.
pub const TESTNET_SOAK_TASK_KIND: &str = "binance_testnet_owner_soak";
/// Maximum number of physical JSONL records accepted by one evidence read.
pub const MAX_TESTNET_SOAK_EVIDENCE_RECORDS: usize = 131_072;

const MAX_TASK_ID_BYTES: usize = 128;
const MAX_FAILURE_THRESHOLD: u16 = 1_024;
const MAX_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_PROBE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MAX_EVIDENCE_DURATION: Duration = Duration::from_secs(366 * 24 * 60 * 60);
const TASK_STRATEGY: &str = "testnet_soak";
const TASK_SYMBOL: &str = "control-plane";

const STARTED: &str = "testnet_soak_started";
const UNCLEAN_RESTART: &str = "testnet_soak_unclean_restart_detected";
const PROBE_SUCCEEDED: &str = "testnet_soak_probe_succeeded";
const PROBE_FAILED: &str = "testnet_soak_probe_failed";
const STOPPED: &str = "testnet_soak_stopped";
const FAILED: &str = "testnet_soak_failed";
const HISTORY_REPAIR_STRATEGY: &str = "history_repair_audit";
const HISTORY_TAIL_REPAIRED: &str = "history_tail_repaired";
const CONTINUOUS_OWNER_STRATEGY: &str = "binance_testnet_continuous_owner";
const CAMPAIGN_RECOVERY_VERIFIED: &str = "continuous_testnet_campaign_recovery_verified";
const EVIDENCE_INTEGRITY_DOMAIN: &[u8] = b"crypto-trading/testnet-soak-evidence/v1\0";
const EVIDENCE_HASH_BYTES: usize = 32;
const STREAMING_REQUIRED_KIND_COUNT: u64 = 3;

/// Borrowing future returned by an injected Testnet owner probe.
pub type TestnetSoakProbeFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TestnetSoakSample, TestnetSoakProbeFailure>> + Send + 'a>>;
pub type TestnetSoakShutdownFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>>;

/// Async injection seam for one bounded Testnet owner step.
pub trait TestnetSoakProbe: Send + 'static {
    /// Identifies the probe lane before the future starts. Production probes
    /// use this to maintain independent failure streaks; simple fixtures may
    /// return `None` and are conservatively assigned to the spot lane.
    fn planned_sample(&self) -> Option<TestnetSoakSample> {
        None
    }

    /// Production mutation-aware probes return true so timeout/stop observes
    /// the deadline without dropping an in-flight lifecycle or stream-ingest
    /// future. Stateless fixtures may retain the cancellation-safe default.
    fn preserve_in_flight_probe(&self) -> bool {
        false
    }

    fn probe(&mut self) -> TestnetSoakProbeFuture<'_>;

    fn shutdown(&mut self) -> TestnetSoakShutdownFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

/// Closed set of successful read-only observations that may be journaled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakSample {
    SpotBookTicker,
    UsdMBookTicker,
    /// Fresh observation received through the reconnecting public stream.
    MarketStream,
    /// Fresh account/order observation received through the private stream.
    UserDataStream,
    AuthenticatedReconcile,
}

/// Closed, secret-free set of probe failure categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakProbeFailure {
    Transport,
    Timeout,
    RateLimited,
    ClockSkew,
    RemoteRejected,
    Protocol,
    Unavailable,
}

/// Independent consecutive-failure counters for each probe lane.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TestnetSoakSampleFailureCounts {
    pub spot_book_ticker: u16,
    pub usd_m_book_ticker: u16,
    pub market_stream: u16,
    pub user_data_stream: u16,
    pub authenticated_reconcile: u16,
}

impl TestnetSoakSampleFailureCounts {
    fn get(self, sample: TestnetSoakSample) -> u16 {
        match sample {
            TestnetSoakSample::SpotBookTicker => self.spot_book_ticker,
            TestnetSoakSample::UsdMBookTicker => self.usd_m_book_ticker,
            TestnetSoakSample::MarketStream => self.market_stream,
            TestnetSoakSample::UserDataStream => self.user_data_stream,
            TestnetSoakSample::AuthenticatedReconcile => self.authenticated_reconcile,
        }
    }

    fn get_mut(&mut self, sample: TestnetSoakSample) -> &mut u16 {
        match sample {
            TestnetSoakSample::SpotBookTicker => &mut self.spot_book_ticker,
            TestnetSoakSample::UsdMBookTicker => &mut self.usd_m_book_ticker,
            TestnetSoakSample::MarketStream => &mut self.market_stream,
            TestnetSoakSample::UserDataStream => &mut self.user_data_stream,
            TestnetSoakSample::AuthenticatedReconcile => &mut self.authenticated_reconcile,
        }
    }

    fn maximum(self) -> u16 {
        [
            self.spot_book_ticker,
            self.usd_m_book_ticker,
            self.market_stream,
            self.user_data_stream,
            self.authenticated_reconcile,
        ]
        .into_iter()
        .max()
        .unwrap_or_default()
    }
}

/// Validated task timing and failure policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestnetSoakTaskConfig {
    task_id: String,
    interval: Duration,
    probe_timeout: Duration,
    consecutive_failure_threshold: u16,
}

impl TestnetSoakTaskConfig {
    /// Creates a bounded soak-host configuration.
    ///
    /// # Errors
    ///
    /// Returns [`TestnetSoakTaskError::InvalidConfig`] for unsafe identifiers,
    /// zero or excessive durations, or an out-of-range failure threshold.
    pub fn new(
        task_id: impl Into<String>,
        interval: Duration,
        probe_timeout: Duration,
        consecutive_failure_threshold: u16,
    ) -> Result<Self, TestnetSoakTaskError> {
        let task_id = task_id.into();
        let task_id = validate_task_id(&task_id)?;
        if interval.is_zero()
            || interval > MAX_INTERVAL
            || probe_timeout.is_zero()
            || probe_timeout > MAX_PROBE_TIMEOUT
            || consecutive_failure_threshold == 0
            || consecutive_failure_threshold > MAX_FAILURE_THRESHOLD
        {
            return Err(TestnetSoakTaskError::InvalidConfig);
        }
        Ok(Self {
            task_id,
            interval,
            probe_timeout,
            consecutive_failure_threshold,
        })
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

/// Durable task lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakTaskPhase {
    Running,
    Stopped,
    Failed,
}

impl TestnetSoakTaskPhase {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

/// Clean task terminal reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakTaskExit {
    StopRequested,
}

/// Bounded task-level failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakTaskFailure {
    ProbeFailureThreshold,
    CounterOverflow,
    JournalUnavailable,
    TaskPanicked,
    TaskCancelled,
    ProbeShutdown,
    EvidenceIntegrity,
}

/// Latest status, advanced only after the matching durable append succeeds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestnetSoakTaskStatus {
    pub schema_version: u16,
    pub task_id: String,
    pub phase: TestnetSoakTaskPhase,
    pub successful_probe_count: u64,
    pub failed_probe_count: u64,
    pub consecutive_failure_count: u16,
    pub consecutive_failure_counts: TestnetSoakSampleFailureCounts,
    pub unclean_restart_count: u32,
    pub last_sample: Option<TestnetSoakSample>,
    pub last_probe_failure: Option<TestnetSoakProbeFailure>,
    pub last_recorded_at: DateTime<Utc>,
    /// Monotonic elapsed time within the current process segment.
    pub segment_elapsed_milliseconds: u64,
    pub exit: Option<TestnetSoakTaskExit>,
    pub failure: Option<TestnetSoakTaskFailure>,
    /// Process-local fault when a terminal fact could not be made durable.
    pub runtime_failure: Option<TestnetSoakTaskFailure>,
}

impl TestnetSoakTaskStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

impl TaskHostStatus for TestnetSoakTaskStatus {
    fn is_terminal(&self) -> bool {
        Self::is_terminal(self)
    }
}

/// Opaque owner of one bounded Testnet probe loop.
#[derive(Debug)]
pub struct TestnetSoakTask {
    stop: watch::Sender<bool>,
    status_sender: watch::Sender<TestnetSoakTaskStatus>,
    status: watch::Receiver<TestnetSoakTaskStatus>,
    join: Option<JoinHandle<TaskResult>>,
    completion: Option<Result<TestnetSoakTaskExit, TestnetSoakTaskFailure>>,
    history: JsonlHistory,
}

impl TestnetSoakTask {
    /// Reads prior facts, records any observed unclean restart, then starts.
    ///
    /// The first probe is scheduled immediately. Later probes wait for the
    /// configured interval.
    ///
    /// # Errors
    ///
    /// Returns a bounded read, configuration, or durable-journal failure.
    pub async fn start<P>(
        config: TestnetSoakTaskConfig,
        probe: P,
        history: JsonlHistory,
    ) -> Result<Self, TestnetSoakTaskError>
    where
        P: TestnetSoakProbe,
    {
        let startup = prepare_startup(&config, &history).await?;
        let initial = startup.running_status(&config.task_id);
        let started_at = startup.started_at;
        let started_monotonic = startup.started_monotonic;
        let integrity_head = startup.integrity_head;
        let (stop, stop_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial);
        let owner_status = status_sender.clone();
        let owner_history = history.clone();
        let join = tokio::spawn(run_owner(
            probe,
            config,
            owner_history,
            owner_status,
            stop_receiver,
            started_at,
            started_monotonic,
            integrity_head,
        ));

        Ok(Self {
            stop,
            status_sender,
            status,
            join: Some(join),
            completion: None,
            history,
        })
    }

    #[must_use]
    pub fn status(&self) -> TestnetSoakTaskStatus {
        self.status.borrow().clone()
    }

    /// Requests and awaits a clean durable stop.
    ///
    /// # Errors
    ///
    /// Returns a bounded task or journal failure. Repeated calls preserve the
    /// first terminal result.
    pub async fn stop(&mut self) -> Result<TestnetSoakTaskExit, TestnetSoakTaskError> {
        if let Some(completion) = self.completion {
            return completion.map_err(TestnetSoakTaskError::PreviouslyFailed);
        }
        let _ = self.stop.send(true);
        let Some(join) = self.join.take() else {
            return Err(TestnetSoakTaskError::TaskCancelled);
        };
        let result = self.map_join_result(join.await).await;
        self.completion = Some(match &result {
            Ok(exit) => Ok(*exit),
            Err(error) => Err(error.failure_bucket()),
        });
        result
    }

    async fn map_join_result(
        &mut self,
        joined: Result<TaskResult, JoinError>,
    ) -> Result<TestnetSoakTaskExit, TestnetSoakTaskError> {
        match joined {
            Ok(result) => result,
            Err(error) => {
                let failure = if error.is_panic() {
                    TestnetSoakTaskFailure::TaskPanicked
                } else {
                    TestnetSoakTaskFailure::TaskCancelled
                };
                let mut status = self.status();
                status.phase = TestnetSoakTaskPhase::Failed;
                status.exit = None;
                status.failure = Some(failure);
                status.runtime_failure = None;
                status.last_recorded_at = Utc::now().max(status.last_recorded_at);
                let terminal = terminal_failure_record(&status, None);
                if let Err(journal_error) =
                    append_with_current_integrity_head(&self.history, &status.task_id, terminal)
                        .await
                {
                    publish_runtime_failure(
                        &self.status_sender,
                        TestnetSoakTaskFailure::JournalUnavailable,
                    );
                    return Err(journal_error);
                }
                self.status_sender.send_replace(status);
                if matches!(failure, TestnetSoakTaskFailure::TaskPanicked) {
                    Err(TestnetSoakTaskError::TaskPanicked)
                } else {
                    Err(TestnetSoakTaskError::TaskCancelled)
                }
            }
        }
    }
}

impl TaskHost for TestnetSoakTask {
    type Status = TestnetSoakTaskStatus;
    type Exit = TestnetSoakTaskExit;
    type Error = TestnetSoakTaskError;

    fn status(&self) -> Self::Status {
        Self::status(self)
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        Box::pin(Self::stop(self))
    }
}

impl Drop for TestnetSoakTask {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

struct StartupState {
    prior: EvidenceProjection,
    continuing_campaign: bool,
    unclean_restart_count: u32,
    started_at: DateTime<Utc>,
    started_monotonic: Instant,
    integrity_head: [u8; EVIDENCE_HASH_BYTES],
}

impl StartupState {
    fn running_status(&self, task_id: &str) -> TestnetSoakTaskStatus {
        let prior = &self.prior;
        TestnetSoakTaskStatus {
            schema_version: TESTNET_SOAK_SCHEMA_VERSION,
            task_id: task_id.to_owned(),
            phase: TestnetSoakTaskPhase::Running,
            successful_probe_count: self.select(prior.successful_probe_count),
            failed_probe_count: self.select(prior.failed_probe_count),
            consecutive_failure_count: self.select(prior.consecutive_failure_count),
            consecutive_failure_counts: self.select(prior.consecutive_failure_counts),
            unclean_restart_count: self.select(self.unclean_restart_count),
            last_sample: self
                .continuing_campaign
                .then_some(prior.last_sample)
                .flatten(),
            last_probe_failure: self
                .continuing_campaign
                .then_some(prior.last_probe_failure)
                .flatten(),
            last_recorded_at: self.started_at,
            segment_elapsed_milliseconds: 0,
            exit: None,
            failure: None,
            runtime_failure: None,
        }
    }

    fn select<T>(&self, value: T) -> T
    where
        T: Default,
    {
        if self.continuing_campaign {
            value
        } else {
            T::default()
        }
    }
}

async fn prepare_startup(
    config: &TestnetSoakTaskConfig,
    history: &JsonlHistory,
) -> Result<StartupState, TestnetSoakTaskError> {
    repair_soak_history(history, Some(config.task_id.as_str()))
        .await
        .map_err(TestnetSoakTaskError::Journal)?;
    let records = read_records(history.path(), true)?;
    let prior = project_records(&records, &config.task_id)?;
    let unclean_restart = prior.running;
    let continuing_campaign = prior.running || prior.awaiting_restart_start;
    let unclean_restart_count = if unclean_restart {
        prior
            .unclean_restart_count
            .checked_add(1)
            .ok_or(TestnetSoakTaskError::CounterOverflow)?
    } else {
        prior.unclean_restart_count
    };
    let started_at = Utc::now().max(prior.last_recorded_at.unwrap_or_else(Utc::now));
    let started_monotonic = Instant::now();
    let mut startup = StartupState {
        prior,
        continuing_campaign,
        unclean_restart_count,
        started_at,
        started_monotonic,
        integrity_head: [0; EVIDENCE_HASH_BYTES],
    };
    startup.integrity_head = if continuing_campaign {
        startup.prior.integrity_head
    } else {
        [0; EVIDENCE_HASH_BYTES]
    };
    fail_closed_at_inherited_threshold(config, history, &startup).await?;
    let mut records = Vec::with_capacity(2);
    if unclean_restart {
        records.push(unclean_restart_record(&config.task_id, started_at));
    }
    records.push(started_record(&config.task_id, started_at));
    chain_records(&mut records, &mut startup.integrity_head)?;
    history
        .append_batch(&records)
        .await
        .map_err(TestnetSoakTaskError::Journal)?;
    Ok(startup)
}

async fn fail_closed_at_inherited_threshold(
    config: &TestnetSoakTaskConfig,
    history: &JsonlHistory,
    startup: &StartupState,
) -> Result<(), TestnetSoakTaskError> {
    if !startup.continuing_campaign
        || startup.prior.consecutive_failure_counts.maximum() < config.consecutive_failure_threshold
    {
        return Ok(());
    }
    let failure = startup
        .prior
        .last_probe_failure
        .unwrap_or(TestnetSoakProbeFailure::Unavailable);
    let mut failed = startup.running_status(&config.task_id);
    failed.phase = TestnetSoakTaskPhase::Failed;
    failed.failure = Some(TestnetSoakTaskFailure::ProbeFailureThreshold);
    failed.last_probe_failure = Some(failure);
    let mut records = Vec::with_capacity(2);
    if startup.prior.running {
        records.push(unclean_restart_record(&config.task_id, startup.started_at));
    }
    records.push(terminal_failure_record(&failed, Some(failure)));
    let mut integrity_head = startup.integrity_head;
    chain_records(&mut records, &mut integrity_head)?;
    history
        .append_batch(&records)
        .await
        .map_err(TestnetSoakTaskError::Journal)?;
    Err(TestnetSoakTaskError::ProbeFailureThreshold(failure))
}

type TaskResult = Result<TestnetSoakTaskExit, TestnetSoakTaskError>;

// The task loop keeps its durability, status, stop, monotonic-clock, and
// integrity dependencies explicit at the ownership boundary.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_owner<P>(
    mut probe: P,
    config: TestnetSoakTaskConfig,
    history: JsonlHistory,
    status_sender: watch::Sender<TestnetSoakTaskStatus>,
    mut stop: watch::Receiver<bool>,
    mut last_recorded_at: DateTime<Utc>,
    segment_started_monotonic: Instant,
    mut integrity_head: [u8; EVIDENCE_HASH_BYTES],
) -> TaskResult
where
    P: TestnetSoakProbe,
{
    loop {
        let explicitly_planned_sample = probe.planned_sample();
        let preserve_in_flight_probe = probe.preserve_in_flight_probe();
        let planned_sample = explicitly_planned_sample.unwrap_or(TestnetSoakSample::SpotBookTicker);
        let mut probe_future = probe.probe();
        let timeout = tokio::time::sleep(config.probe_timeout);
        tokio::pin!(timeout);
        let mut timed_out = false;
        let mut stop_requested = false;
        let mut threshold_failure = None;
        let probe_result = loop {
            tokio::select! {
                changed = stop.changed(), if !stop_requested => {
                    if changed.is_err() || *stop.borrow_and_update() {
                        stop_requested = true;
                        if !preserve_in_flight_probe {
                            break Err(TestnetSoakProbeFailure::Unavailable);
                        }
                    }
                }
                result = probe_future.as_mut() => break result,
                () = &mut timeout, if threshold_failure.is_none() && !stop_requested => {
                    timed_out = true;
                    let recorded_at = Utc::now().max(last_recorded_at);
                    let elapsed_milliseconds = duration_milliseconds(
                        segment_started_monotonic.elapsed(),
                    )?;
                    if record_probe_failure(
                        &history,
                        &status_sender,
                        &mut integrity_head,
                        planned_sample,
                        TestnetSoakProbeFailure::Timeout,
                        recorded_at,
                        elapsed_milliseconds,
                        config.consecutive_failure_threshold,
                    )
                    .await?
                    {
                        threshold_failure = Some(TestnetSoakProbeFailure::Timeout);
                    }
                    last_recorded_at = recorded_at;
                    if !preserve_in_flight_probe {
                        break Err(TestnetSoakProbeFailure::Timeout);
                    }
                    timeout.as_mut().reset(tokio::time::Instant::now() + config.probe_timeout);
                }
            }
        };
        // The borrowing future is deliberately driven to completion even after
        // a timeout or stop request. Dropping it could lose a received stream
        // item before its durable owner acknowledgement, or strand a lifecycle
        // mutation between query-first phases.
        drop(probe_future);

        if let Some(failure) = threshold_failure {
            shutdown_after_terminal(
                &mut probe,
                &history,
                &status_sender,
                last_recorded_at,
                segment_started_monotonic,
                &mut integrity_head,
                config.probe_timeout,
            )
            .await?;
            return Err(TestnetSoakTaskError::ProbeFailureThreshold(failure));
        }
        if stop_requested {
            return stop_owner(
                &mut probe,
                &history,
                &status_sender,
                last_recorded_at,
                segment_started_monotonic,
                &mut integrity_head,
                config.probe_timeout,
            )
            .await;
        }
        if timed_out {
            // A late result is not fresh evidence and must not reset the
            // independent failure streak for its lane.
        } else {
            let probe_result = match (explicitly_planned_sample, probe_result) {
                (Some(expected), Ok(actual)) if expected != actual => {
                    Err(TestnetSoakProbeFailure::Protocol)
                }
                (_, result) => result,
            };
            let recorded_at = Utc::now().max(last_recorded_at);
            let elapsed_milliseconds = duration_milliseconds(segment_started_monotonic.elapsed())?;
            match probe_result {
                Ok(sample) => {
                    let mut next = status_sender.borrow().clone();
                    next.successful_probe_count = next
                        .successful_probe_count
                        .checked_add(1)
                        .ok_or(TestnetSoakTaskError::CounterOverflow)?;
                    if explicitly_planned_sample.is_some() {
                        *next.consecutive_failure_counts.get_mut(sample) = 0;
                    } else {
                        next.consecutive_failure_counts = TestnetSoakSampleFailureCounts::default();
                    }
                    next.consecutive_failure_count = next.consecutive_failure_counts.maximum();
                    next.last_sample = Some(sample);
                    if next.consecutive_failure_count == 0 {
                        next.last_probe_failure = None;
                    }
                    next.last_recorded_at = recorded_at;
                    next.segment_elapsed_milliseconds = elapsed_milliseconds;
                    let mut record = probe_success_record(&next, sample);
                    chain_record(&mut record, &mut integrity_head)?;
                    if let Err(error) = history.append(&record).await {
                        publish_runtime_failure(
                            &status_sender,
                            TestnetSoakTaskFailure::JournalUnavailable,
                        );
                        return Err(TestnetSoakTaskError::Journal(error));
                    }
                    status_sender.send_replace(next);
                }
                Err(failure) => {
                    if record_probe_failure(
                        &history,
                        &status_sender,
                        &mut integrity_head,
                        planned_sample,
                        failure,
                        recorded_at,
                        elapsed_milliseconds,
                        config.consecutive_failure_threshold,
                    )
                    .await?
                    {
                        shutdown_after_terminal(
                            &mut probe,
                            &history,
                            &status_sender,
                            recorded_at,
                            segment_started_monotonic,
                            &mut integrity_head,
                            config.probe_timeout,
                        )
                        .await?;
                        return Err(TestnetSoakTaskError::ProbeFailureThreshold(failure));
                    }
                }
            }
            last_recorded_at = recorded_at;
        }

        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow_and_update() {
                    return stop_owner(
                        &mut probe,
                        &history,
                        &status_sender,
                        last_recorded_at,
                        segment_started_monotonic,
                        &mut integrity_head,
                        config.probe_timeout,
                    )
                    .await;
                }
            }
            () = tokio::time::sleep(config.interval) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn record_probe_failure(
    history: &JsonlHistory,
    status_sender: &watch::Sender<TestnetSoakTaskStatus>,
    integrity_head: &mut [u8; EVIDENCE_HASH_BYTES],
    sample: TestnetSoakSample,
    failure: TestnetSoakProbeFailure,
    recorded_at: DateTime<Utc>,
    elapsed_milliseconds: u64,
    failure_threshold: u16,
) -> Result<bool, TestnetSoakTaskError> {
    let mut next = status_sender.borrow().clone();
    next.failed_probe_count = next
        .failed_probe_count
        .checked_add(1)
        .ok_or(TestnetSoakTaskError::CounterOverflow)?;
    let sample_failure_count = next
        .consecutive_failure_counts
        .get(sample)
        .checked_add(1)
        .ok_or(TestnetSoakTaskError::CounterOverflow)?;
    *next.consecutive_failure_counts.get_mut(sample) = sample_failure_count;
    next.consecutive_failure_count = next.consecutive_failure_counts.maximum();
    next.last_probe_failure = Some(failure);
    next.last_recorded_at = recorded_at;
    next.segment_elapsed_milliseconds = elapsed_milliseconds;
    let threshold_reached = sample_failure_count >= failure_threshold;
    let mut records = vec![probe_failure_record(&next, sample, failure)];
    if threshold_reached {
        next.phase = TestnetSoakTaskPhase::Failed;
        next.failure = Some(TestnetSoakTaskFailure::ProbeFailureThreshold);
        records.push(terminal_failure_record(&next, Some(failure)));
    }
    chain_records(&mut records, integrity_head)?;
    if let Err(error) = history.append_batch(&records).await {
        publish_runtime_failure(status_sender, TestnetSoakTaskFailure::JournalUnavailable);
        return Err(TestnetSoakTaskError::Journal(error));
    }
    status_sender.send_replace(next);
    Ok(threshold_reached)
}

async fn stop_owner<P>(
    probe: &mut P,
    history: &JsonlHistory,
    status_sender: &watch::Sender<TestnetSoakTaskStatus>,
    last_recorded_at: DateTime<Utc>,
    segment_started_monotonic: Instant,
    integrity_head: &mut [u8; EVIDENCE_HASH_BYTES],
    shutdown_timeout: Duration,
) -> TaskResult
where
    P: TestnetSoakProbe,
{
    if tokio::time::timeout(shutdown_timeout, probe.shutdown())
        .await
        .map_err(|_| ())
        .and_then(|result| result)
        .is_err()
    {
        let mut failed = status_sender.borrow().clone();
        failed.phase = TestnetSoakTaskPhase::Failed;
        failed.last_recorded_at = Utc::now().max(last_recorded_at);
        failed.segment_elapsed_milliseconds =
            duration_milliseconds(segment_started_monotonic.elapsed())?;
        failed.exit = None;
        failed.failure = Some(TestnetSoakTaskFailure::ProbeShutdown);
        failed.runtime_failure = None;
        let mut terminal = terminal_failure_record(&failed, None);
        chain_record(&mut terminal, integrity_head)?;
        if let Err(error) = history.append(&terminal).await {
            publish_runtime_failure(status_sender, TestnetSoakTaskFailure::JournalUnavailable);
            return Err(TestnetSoakTaskError::Journal(error));
        }
        status_sender.send_replace(failed);
        return Err(TestnetSoakTaskError::ProbeShutdown);
    }
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = TestnetSoakTaskPhase::Stopped;
    stopped.last_recorded_at = Utc::now().max(last_recorded_at);
    stopped.segment_elapsed_milliseconds =
        duration_milliseconds(segment_started_monotonic.elapsed())?;
    stopped.exit = Some(TestnetSoakTaskExit::StopRequested);
    stopped.failure = None;
    stopped.runtime_failure = None;
    let mut terminal = stopped_record(&stopped);
    chain_record(&mut terminal, integrity_head)?;
    if let Err(error) = history.append(&terminal).await {
        publish_runtime_failure(status_sender, TestnetSoakTaskFailure::JournalUnavailable);
        return Err(TestnetSoakTaskError::Journal(error));
    }
    status_sender.send_replace(stopped);
    Ok(TestnetSoakTaskExit::StopRequested)
}

async fn shutdown_after_terminal<P>(
    probe: &mut P,
    history: &JsonlHistory,
    status_sender: &watch::Sender<TestnetSoakTaskStatus>,
    last_recorded_at: DateTime<Utc>,
    segment_started_monotonic: Instant,
    integrity_head: &mut [u8; EVIDENCE_HASH_BYTES],
    shutdown_timeout: Duration,
) -> Result<(), TestnetSoakTaskError>
where
    P: TestnetSoakProbe,
{
    if tokio::time::timeout(shutdown_timeout, probe.shutdown())
        .await
        .map_err(|_| ())
        .and_then(|result| result)
        .is_ok()
    {
        return Ok(());
    }
    let mut failed = status_sender.borrow().clone();
    failed.phase = TestnetSoakTaskPhase::Failed;
    failed.last_recorded_at = Utc::now().max(last_recorded_at);
    failed.segment_elapsed_milliseconds =
        duration_milliseconds(segment_started_monotonic.elapsed())?;
    failed.exit = None;
    failed.failure = Some(TestnetSoakTaskFailure::ProbeShutdown);
    failed.runtime_failure = None;
    let mut terminal = terminal_failure_record(&failed, None);
    chain_record(&mut terminal, integrity_head)?;
    if let Err(error) = history.append(&terminal).await {
        publish_runtime_failure(status_sender, TestnetSoakTaskFailure::JournalUnavailable);
        return Err(TestnetSoakTaskError::Journal(error));
    }
    status_sender.send_replace(failed);
    Err(TestnetSoakTaskError::ProbeShutdown)
}

fn publish_runtime_failure(
    status_sender: &watch::Sender<TestnetSoakTaskStatus>,
    failure: TestnetSoakTaskFailure,
) {
    let mut status = status_sender.borrow().clone();
    status.runtime_failure = Some(failure);
    status_sender.send_replace(status);
}

/// Bounded evidence policy used by the offline verifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakSampleCoverageRequirement {
    NotRequired,
    /// Legacy REST probe coverage retained for historical evidence readers.
    AllKinds,
    /// The realtime release gate: public stream, private stream, and an
    /// authoritative REST reconciliation must all be observed.
    StreamingPath,
}

/// Bounded evidence policy used by the offline verifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TestnetSoakEvidenceRequirements {
    minimum_duration: Duration,
    minimum_successful_probes: u64,
    minimum_successes_per_required_kind: u64,
    maximum_required_kind_gap: Option<Duration>,
    require_clean_stop: bool,
    require_unclean_restart: bool,
    require_integrity_chain: bool,
    require_monotonic_elapsed: bool,
    sample_coverage: TestnetSoakSampleCoverageRequirement,
}

impl TestnetSoakEvidenceRequirements {
    /// Builds the production-candidate 24-hour policy.
    ///
    /// A passing artifact must end cleanly and contain an observed unclean
    /// restart in addition to the requested number of successful probes.
    ///
    /// # Errors
    ///
    /// Rejects a success count above the evidence record budget.
    pub fn twenty_four_hour(
        minimum_successful_probes: u64,
    ) -> Result<Self, TestnetSoakEvidenceError> {
        let mut requirements = Self::new(
            Duration::from_secs(24 * 60 * 60),
            minimum_successful_probes,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::StreamingPath,
        )?;
        let minimum_per_kind = minimum_successful_probes
            .div_ceil(STREAMING_REQUIRED_KIND_COUNT)
            .max(1);
        let expected_gap_seconds = requirements
            .minimum_duration
            .as_secs()
            .div_ceil(minimum_per_kind);
        requirements.minimum_successes_per_required_kind = minimum_per_kind;
        requirements.maximum_required_kind_gap =
            Some(Duration::from_secs(expected_gap_seconds.saturating_mul(2)));
        requirements.require_integrity_chain = true;
        requirements.require_monotonic_elapsed = true;
        Ok(requirements)
    }

    /// Creates a bounded evidence policy.
    ///
    /// # Errors
    ///
    /// Rejects durations over one leap year and impossible record counts.
    pub fn new(
        minimum_duration: Duration,
        minimum_successful_probes: u64,
        require_clean_stop: bool,
        require_unclean_restart: bool,
        sample_coverage: TestnetSoakSampleCoverageRequirement,
    ) -> Result<Self, TestnetSoakEvidenceError> {
        if minimum_duration > MAX_EVIDENCE_DURATION
            || minimum_successful_probes
                > u64::try_from(MAX_TESTNET_SOAK_EVIDENCE_RECORDS).unwrap_or(u64::MAX)
        {
            return Err(TestnetSoakEvidenceError::InvalidRequirements);
        }
        Ok(Self {
            minimum_duration,
            minimum_successful_probes,
            minimum_successes_per_required_kind: 0,
            maximum_required_kind_gap: None,
            require_clean_stop,
            require_unclean_restart,
            require_integrity_chain: false,
            require_monotonic_elapsed: false,
            sample_coverage,
        })
    }

    /// Adds explicit per-kind density and maximum-gap gates.
    ///
    /// # Errors
    ///
    /// Rejects a zero density or zero/excessive gap.
    pub fn with_required_kind_density(
        mut self,
        minimum_successes_per_required_kind: u64,
        maximum_required_kind_gap: Duration,
    ) -> Result<Self, TestnetSoakEvidenceError> {
        if minimum_successes_per_required_kind == 0
            || maximum_required_kind_gap.is_zero()
            || maximum_required_kind_gap > MAX_EVIDENCE_DURATION
        {
            return Err(TestnetSoakEvidenceError::InvalidRequirements);
        }
        self.minimum_successes_per_required_kind = minimum_successes_per_required_kind;
        self.maximum_required_kind_gap = Some(maximum_required_kind_gap);
        Ok(self)
    }

    #[must_use]
    pub const fn require_integrity_and_monotonic_elapsed(mut self) -> Self {
        self.require_integrity_chain = true;
        self.require_monotonic_elapsed = true;
        self
    }
}

/// Closed set of unmet evidence requirements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakEvidenceViolation {
    MinimumDuration,
    MinimumSuccessfulProbes,
    CleanStopMissing,
    UncleanRestartMissing,
    SpotBookTickerMissing,
    UsdMBookTickerMissing,
    MarketStreamMissing,
    UserDataStreamMissing,
    AuthenticatedReconcileMissing,
    OwnerCampaignRecoveryMissing,
    MarketStreamDensity,
    UserDataStreamDensity,
    AuthenticatedReconcileDensity,
    MarketStreamGapExceeded,
    UserDataStreamGapExceeded,
    AuthenticatedReconcileGapExceeded,
    IntegrityChainMissing,
    MonotonicElapsedMissing,
}

/// Per-kind successful probe counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TestnetSoakSampleCounts {
    pub spot_book_ticker: u64,
    pub usd_m_book_ticker: u64,
    pub market_stream: u64,
    pub user_data_stream: u64,
    pub authenticated_reconcile: u64,
}

/// Machine-readable copy of the applied evidence policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TestnetSoakEvidencePolicySummary {
    pub minimum_duration_seconds: u64,
    pub minimum_successful_probes: u64,
    pub minimum_successes_per_required_kind: u64,
    pub maximum_required_kind_gap_seconds: Option<u64>,
    pub require_clean_stop: bool,
    pub require_unclean_restart: bool,
    pub require_integrity_chain: bool,
    pub require_monotonic_elapsed: bool,
    pub sample_coverage: TestnetSoakSampleCoverageRequirement,
}

/// Machine-readable, secret-free projection of one task's evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TestnetSoakEvidenceSummary {
    pub schema_version: u16,
    pub task_id: String,
    pub requirements: TestnetSoakEvidencePolicySummary,
    pub observed_duration_seconds: u64,
    pub successful_probe_count: u64,
    pub sample_counts: TestnetSoakSampleCounts,
    pub maximum_sample_gap_seconds: TestnetSoakSampleCounts,
    pub failed_probe_count: u64,
    pub clean_stop_observed: bool,
    pub unclean_restart_count: u32,
    pub owner_campaign_recovery_verified: bool,
    pub integrity_chain_verified: bool,
    pub integrity_chain_head: Option<String>,
    pub source_sha256: String,
    pub monotonic_elapsed_verified: bool,
    pub requirements_met: bool,
    pub violations: Vec<TestnetSoakEvidenceViolation>,
}

impl TestnetSoakEvidenceSummary {
    /// Returns a stable JSON value suitable for CLI output or artifact storage.
    #[must_use]
    pub fn as_json(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "requirements": {
                "minimum_duration_seconds": self.requirements.minimum_duration_seconds,
                "minimum_successful_probes": self.requirements.minimum_successful_probes,
                "minimum_successes_per_required_kind": self.requirements.minimum_successes_per_required_kind,
                "maximum_required_kind_gap_seconds": self.requirements.maximum_required_kind_gap_seconds,
                "require_clean_stop": self.requirements.require_clean_stop,
                "require_unclean_restart": self.requirements.require_unclean_restart,
                "require_integrity_chain": self.requirements.require_integrity_chain,
                "require_monotonic_elapsed": self.requirements.require_monotonic_elapsed,
                "sample_coverage": sample_coverage_label(self.requirements.sample_coverage),
            },
            "observed_duration_seconds": self.observed_duration_seconds,
            "successful_probe_count": self.successful_probe_count,
            "sample_counts": {
                "spot_book_ticker": self.sample_counts.spot_book_ticker,
                "usd_m_book_ticker": self.sample_counts.usd_m_book_ticker,
                "market_stream": self.sample_counts.market_stream,
                "user_data_stream": self.sample_counts.user_data_stream,
                "authenticated_reconcile": self.sample_counts.authenticated_reconcile,
            },
            "maximum_sample_gap_seconds": {
                "spot_book_ticker": self.maximum_sample_gap_seconds.spot_book_ticker,
                "usd_m_book_ticker": self.maximum_sample_gap_seconds.usd_m_book_ticker,
                "market_stream": self.maximum_sample_gap_seconds.market_stream,
                "user_data_stream": self.maximum_sample_gap_seconds.user_data_stream,
                "authenticated_reconcile": self.maximum_sample_gap_seconds.authenticated_reconcile,
            },
            "failed_probe_count": self.failed_probe_count,
            "clean_stop_observed": self.clean_stop_observed,
            "unclean_restart_count": self.unclean_restart_count,
            "owner_campaign_recovery_verified": self.owner_campaign_recovery_verified,
            "integrity_chain_verified": self.integrity_chain_verified,
            "integrity_chain_head": self.integrity_chain_head,
            "source_sha256": self.source_sha256,
            "monotonic_elapsed_verified": self.monotonic_elapsed_verified,
            "requirements_met": self.requirements_met,
            "violations": self
                .violations
                .iter()
                .map(|violation| evidence_violation_label(*violation))
                .collect::<Vec<_>>(),
        })
    }
}

/// Reads and verifies one task's bounded offline evidence.
///
/// # Errors
///
/// Fails closed for invalid requirements or task identifiers, missing,
/// changing, corrupt, partial, oversized, or over-record-budget journals.
#[allow(clippy::too_many_lines)]
pub fn verify_testnet_soak_evidence(
    history_path: &Path,
    task_id: &str,
    requirements: TestnetSoakEvidenceRequirements,
) -> Result<TestnetSoakEvidenceSummary, TestnetSoakEvidenceError> {
    validate_task_id(task_id).map_err(|_| TestnetSoakEvidenceError::InvalidTaskId)?;
    preflight_evidence_source(history_path)?;
    repair_soak_history_blocking(history_path, Some(task_id))?;
    let (records, source_sha256) = read_records_and_hash(history_path, false)?;
    let projection = project_records(&records, task_id)?;
    let observed_duration_seconds = projection.observed_duration_seconds()?;
    let maximum_sample_gap_seconds = projection.maximum_gaps_at_end(observed_duration_seconds);
    let clean_stop_observed = projection.clean_stop_observed();
    let integrity_chain_verified =
        projection.integrity_record_count != 0 && !projection.integrity_missing;
    let monotonic_elapsed_verified = !projection.monotonic_elapsed_missing;
    let mut violations = Vec::with_capacity(4);
    if observed_duration_seconds < requirements.minimum_duration.as_secs() {
        violations.push(TestnetSoakEvidenceViolation::MinimumDuration);
    }
    if projection.successful_probe_count < requirements.minimum_successful_probes {
        violations.push(TestnetSoakEvidenceViolation::MinimumSuccessfulProbes);
    }
    if requirements.require_clean_stop && !clean_stop_observed {
        violations.push(TestnetSoakEvidenceViolation::CleanStopMissing);
    }
    if requirements.require_unclean_restart && projection.unclean_restart_count == 0 {
        violations.push(TestnetSoakEvidenceViolation::UncleanRestartMissing);
    }
    if matches!(
        requirements.sample_coverage,
        TestnetSoakSampleCoverageRequirement::StreamingPath
    ) && requirements.require_unclean_restart
        && !projection.owner_campaign_recovery_verified
    {
        violations.push(TestnetSoakEvidenceViolation::OwnerCampaignRecoveryMissing);
    }
    if matches!(
        requirements.sample_coverage,
        TestnetSoakSampleCoverageRequirement::AllKinds
    ) {
        if projection.sample_counts.spot_book_ticker == 0 {
            violations.push(TestnetSoakEvidenceViolation::SpotBookTickerMissing);
        }
        if projection.sample_counts.usd_m_book_ticker == 0 {
            violations.push(TestnetSoakEvidenceViolation::UsdMBookTickerMissing);
        }
        if projection.sample_counts.authenticated_reconcile == 0 {
            violations.push(TestnetSoakEvidenceViolation::AuthenticatedReconcileMissing);
        }
    }
    if requirements.require_integrity_chain && !integrity_chain_verified {
        violations.push(TestnetSoakEvidenceViolation::IntegrityChainMissing);
    }
    if requirements.require_monotonic_elapsed && !monotonic_elapsed_verified {
        violations.push(TestnetSoakEvidenceViolation::MonotonicElapsedMissing);
    }
    if matches!(
        requirements.sample_coverage,
        TestnetSoakSampleCoverageRequirement::StreamingPath
    ) {
        if projection.sample_counts.market_stream == 0 {
            violations.push(TestnetSoakEvidenceViolation::MarketStreamMissing);
        }
        if projection.sample_counts.user_data_stream == 0 {
            violations.push(TestnetSoakEvidenceViolation::UserDataStreamMissing);
        }
        if projection.sample_counts.authenticated_reconcile == 0 {
            violations.push(TestnetSoakEvidenceViolation::AuthenticatedReconcileMissing);
        }
        let minimum = requirements.minimum_successes_per_required_kind;
        if minimum != 0 {
            if projection.sample_counts.market_stream != 0
                && projection.sample_counts.market_stream < minimum
            {
                violations.push(TestnetSoakEvidenceViolation::MarketStreamDensity);
            }
            if projection.sample_counts.user_data_stream != 0
                && projection.sample_counts.user_data_stream < minimum
            {
                violations.push(TestnetSoakEvidenceViolation::UserDataStreamDensity);
            }
            if projection.sample_counts.authenticated_reconcile != 0
                && projection.sample_counts.authenticated_reconcile < minimum
            {
                violations.push(TestnetSoakEvidenceViolation::AuthenticatedReconcileDensity);
            }
        }
        if let Some(maximum_gap) = requirements.maximum_required_kind_gap {
            let maximum_gap = maximum_gap.as_secs();
            if projection.sample_counts.market_stream != 0
                && maximum_sample_gap_seconds.market_stream > maximum_gap
            {
                violations.push(TestnetSoakEvidenceViolation::MarketStreamGapExceeded);
            }
            if projection.sample_counts.user_data_stream != 0
                && maximum_sample_gap_seconds.user_data_stream > maximum_gap
            {
                violations.push(TestnetSoakEvidenceViolation::UserDataStreamGapExceeded);
            }
            if projection.sample_counts.authenticated_reconcile != 0
                && maximum_sample_gap_seconds.authenticated_reconcile > maximum_gap
            {
                violations.push(TestnetSoakEvidenceViolation::AuthenticatedReconcileGapExceeded);
            }
        }
    }
    Ok(TestnetSoakEvidenceSummary {
        schema_version: TESTNET_SOAK_SCHEMA_VERSION,
        task_id: task_id.to_owned(),
        requirements: TestnetSoakEvidencePolicySummary {
            minimum_duration_seconds: requirements.minimum_duration.as_secs(),
            minimum_successful_probes: requirements.minimum_successful_probes,
            minimum_successes_per_required_kind: requirements.minimum_successes_per_required_kind,
            maximum_required_kind_gap_seconds: requirements
                .maximum_required_kind_gap
                .map(|duration| duration.as_secs()),
            require_clean_stop: requirements.require_clean_stop,
            require_unclean_restart: requirements.require_unclean_restart,
            require_integrity_chain: requirements.require_integrity_chain,
            require_monotonic_elapsed: requirements.require_monotonic_elapsed,
            sample_coverage: requirements.sample_coverage,
        },
        observed_duration_seconds,
        successful_probe_count: projection.successful_probe_count,
        sample_counts: projection.sample_counts,
        maximum_sample_gap_seconds,
        failed_probe_count: projection.failed_probe_count,
        clean_stop_observed,
        unclean_restart_count: projection.unclean_restart_count,
        owner_campaign_recovery_verified: projection.owner_campaign_recovery_verified,
        integrity_chain_verified,
        integrity_chain_head: integrity_chain_verified
            .then(|| encode_hash(&projection.integrity_head)),
        source_sha256,
        monotonic_elapsed_verified,
        requirements_met: violations.is_empty(),
        violations,
    })
}

fn preflight_evidence_source(path: &Path) -> Result<(), TestnetSoakEvidenceError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            TestnetSoakEvidenceError::SourceMissing
        } else {
            TestnetSoakEvidenceError::Io
        }
    })?;
    if !metadata.is_file() {
        return Err(TestnetSoakEvidenceError::NotAFile);
    }
    if metadata.len() > crypto_trading_runtime::MAX_JOURNAL_SOURCE_BYTES {
        return Err(TestnetSoakEvidenceError::SourceTooLarge);
    }
    let bytes = std::fs::read(path).map_err(|_| TestnetSoakEvidenceError::Io)?;
    if bytes.ends_with(b"\n") {
        for raw_line in bytes
            .strip_suffix(b"\n")
            .unwrap_or(&bytes)
            .split(|byte| *byte == b'\n')
        {
            let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            if line.is_empty() {
                return Err(TestnetSoakEvidenceError::EmptyRecord);
            }
            if line.len().saturating_add(1) > MAX_HISTORY_RECORD_BYTES {
                return Err(TestnetSoakEvidenceError::RecordTooLarge);
            }
            serde_json::from_slice::<DecisionRecord>(line)
                .map_err(|_| TestnetSoakEvidenceError::MalformedRecord)?;
        }
        return Ok(());
    }
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        let line = bytes.rsplit(|byte| *byte == b'\n').next().unwrap_or(&bytes);
        if serde_json::from_slice::<DecisionRecord>(line).is_ok() {
            // A complete record missing only its terminator is recoverable.
            return Ok(());
        }
        if line == b"{}" {
            return Err(TestnetSoakEvidenceError::PartialRecord);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct TestnetSoakSampleTimings {
    spot_book_ticker: Option<u64>,
    usd_m_book_ticker: Option<u64>,
    market_stream: Option<u64>,
    user_data_stream: Option<u64>,
    authenticated_reconcile: Option<u64>,
}

impl TestnetSoakSampleTimings {
    fn get(self, sample: TestnetSoakSample) -> Option<u64> {
        match sample {
            TestnetSoakSample::SpotBookTicker => self.spot_book_ticker,
            TestnetSoakSample::UsdMBookTicker => self.usd_m_book_ticker,
            TestnetSoakSample::MarketStream => self.market_stream,
            TestnetSoakSample::UserDataStream => self.user_data_stream,
            TestnetSoakSample::AuthenticatedReconcile => self.authenticated_reconcile,
        }
    }

    fn get_mut(&mut self, sample: TestnetSoakSample) -> &mut Option<u64> {
        match sample {
            TestnetSoakSample::SpotBookTicker => &mut self.spot_book_ticker,
            TestnetSoakSample::UsdMBookTicker => &mut self.usd_m_book_ticker,
            TestnetSoakSample::MarketStream => &mut self.market_stream,
            TestnetSoakSample::UserDataStream => &mut self.user_data_stream,
            TestnetSoakSample::AuthenticatedReconcile => &mut self.authenticated_reconcile,
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug)]
struct EvidenceProjection {
    first_started_at: Option<DateTime<Utc>>,
    last_recorded_at: Option<DateTime<Utc>>,
    segment_started_at: Option<DateTime<Utc>>,
    segment_last_probe_at: Option<DateTime<Utc>>,
    segment_last_elapsed_milliseconds: Option<u64>,
    segment_last_record_elapsed_milliseconds: Option<u64>,
    observed_active_milliseconds: u64,
    successful_probe_count: u64,
    sample_counts: TestnetSoakSampleCounts,
    failed_probe_count: u64,
    consecutive_failure_count: u16,
    consecutive_failure_counts: TestnetSoakSampleFailureCounts,
    unclean_restart_count: u32,
    last_sample: Option<TestnetSoakSample>,
    last_probe_failure: Option<TestnetSoakProbeFailure>,
    running: bool,
    awaiting_restart_start: bool,
    clean_stop: bool,
    owner_campaign_recovery_verified: bool,
    owner_campaign_recovery_candidate: bool,
    last_sample_elapsed_seconds: TestnetSoakSampleTimings,
    maximum_sample_gap_seconds: TestnetSoakSampleCounts,
    integrity_head: [u8; EVIDENCE_HASH_BYTES],
    integrity_record_count: u64,
    integrity_missing: bool,
    monotonic_elapsed_missing: bool,
}

impl Default for EvidenceProjection {
    fn default() -> Self {
        Self {
            first_started_at: None,
            last_recorded_at: None,
            segment_started_at: None,
            segment_last_probe_at: None,
            segment_last_elapsed_milliseconds: None,
            segment_last_record_elapsed_milliseconds: None,
            observed_active_milliseconds: 0,
            successful_probe_count: 0,
            sample_counts: TestnetSoakSampleCounts::default(),
            failed_probe_count: 0,
            consecutive_failure_count: 0,
            consecutive_failure_counts: TestnetSoakSampleFailureCounts::default(),
            unclean_restart_count: 0,
            last_sample: None,
            last_probe_failure: None,
            running: false,
            awaiting_restart_start: false,
            clean_stop: false,
            owner_campaign_recovery_verified: false,
            owner_campaign_recovery_candidate: false,
            last_sample_elapsed_seconds: TestnetSoakSampleTimings::default(),
            maximum_sample_gap_seconds: TestnetSoakSampleCounts::default(),
            integrity_head: [0; EVIDENCE_HASH_BYTES],
            integrity_record_count: 0,
            integrity_missing: false,
            monotonic_elapsed_missing: false,
        }
    }
}

impl EvidenceProjection {
    fn reset_campaign(&mut self) {
        self.first_started_at = None;
        self.segment_started_at = None;
        self.segment_last_probe_at = None;
        self.segment_last_elapsed_milliseconds = None;
        self.segment_last_record_elapsed_milliseconds = None;
        self.observed_active_milliseconds = 0;
        self.successful_probe_count = 0;
        self.sample_counts = TestnetSoakSampleCounts::default();
        self.failed_probe_count = 0;
        self.consecutive_failure_count = 0;
        self.consecutive_failure_counts = TestnetSoakSampleFailureCounts::default();
        self.unclean_restart_count = 0;
        self.last_sample = None;
        self.last_probe_failure = None;
        self.running = false;
        self.awaiting_restart_start = false;
        self.clean_stop = false;
        self.owner_campaign_recovery_verified = false;
        self.owner_campaign_recovery_candidate = false;
        self.last_sample_elapsed_seconds = TestnetSoakSampleTimings::default();
        self.maximum_sample_gap_seconds = TestnetSoakSampleCounts::default();
        self.integrity_head = [0; EVIDENCE_HASH_BYTES];
        self.integrity_record_count = 0;
        self.integrity_missing = false;
        self.monotonic_elapsed_missing = false;
    }

    fn close_segment(&mut self) -> Result<(), TestnetSoakEvidenceError> {
        let Some(started_at) = self.segment_started_at.take() else {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        };
        let last_probe_at = self.segment_last_probe_at.take();
        let last_elapsed = self.segment_last_elapsed_milliseconds.take();
        if let Some(segment_milliseconds) = last_elapsed.or_else(|| {
            last_probe_at.and_then(|last_probe_at| {
                u64::try_from(
                    last_probe_at
                        .signed_duration_since(started_at)
                        .num_milliseconds(),
                )
                .ok()
            })
        }) {
            self.observed_active_milliseconds = self
                .observed_active_milliseconds
                .checked_add(segment_milliseconds)
                .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
        }
        Ok(())
    }

    fn observed_duration_seconds(&self) -> Result<u64, TestnetSoakEvidenceError> {
        let current_segment_milliseconds = match self.segment_last_elapsed_milliseconds {
            Some(elapsed) => elapsed,
            None => match (self.segment_started_at, self.segment_last_probe_at) {
                (Some(started_at), Some(last_probe_at)) => {
                    segment_milliseconds(started_at, last_probe_at)?
                }
                _ => 0,
            },
        };
        self.observed_active_milliseconds
            .checked_add(current_segment_milliseconds)
            .ok_or(TestnetSoakEvidenceError::CounterOverflow)
            .map(|milliseconds| milliseconds / 1_000)
    }

    fn campaign_elapsed_seconds_for(
        &self,
        record: &DecisionRecord,
    ) -> Result<u64, TestnetSoakEvidenceError> {
        let segment_elapsed = record_elapsed_milliseconds(record).unwrap_or_else(|| {
            self.segment_started_at.map_or(0, |started_at| {
                segment_milliseconds(started_at, record.timestamp).unwrap_or(0)
            })
        });
        self.observed_active_milliseconds
            .checked_add(segment_elapsed)
            .ok_or(TestnetSoakEvidenceError::CounterOverflow)
            .map(|milliseconds| milliseconds / 1_000)
    }

    fn maximum_gaps_at_end(&self, observed_duration_seconds: u64) -> TestnetSoakSampleCounts {
        let mut gaps = self.maximum_sample_gap_seconds;
        for sample in all_samples() {
            let trailing = self
                .last_sample_elapsed_seconds
                .get(sample)
                .map_or(observed_duration_seconds, |last| {
                    observed_duration_seconds.saturating_sub(last)
                });
            let gap = sample_count_mut(&mut gaps, sample);
            *gap = (*gap).max(trailing);
        }
        gaps
    }

    const fn clean_stop_observed(&self) -> bool {
        self.clean_stop && !self.running && !self.awaiting_restart_start
    }
}

fn project_records(
    records: &[DecisionRecord],
    task_id: &str,
) -> Result<EvidenceProjection, TestnetSoakEvidenceError> {
    let mut projection = EvidenceProjection::default();
    for record in records {
        if record.strategy == CONTINUOUS_OWNER_STRATEGY {
            project_owner_recovery_fact(&mut projection, record, task_id)?;
            continue;
        }
        if record.strategy != TASK_STRATEGY {
            continue;
        }
        let Some(record_task_id) = record.details.get("task_id").and_then(Value::as_str) else {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        };
        let normalized_task_id = validate_task_id(record_task_id)
            .map_err(|_| TestnetSoakEvidenceError::InvalidSoakRecord)?;
        if normalized_task_id != record_task_id {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        }
        if record_task_id != task_id {
            continue;
        }
        if projection.owner_campaign_recovery_candidate
            && record.decision.as_str() != UNCLEAN_RESTART
        {
            projection.owner_campaign_recovery_candidate = false;
        }
        if record.details.get("task_kind").and_then(Value::as_str) != Some(TESTNET_SOAK_TASK_KIND)
            || record.details.get("schema_version").and_then(Value::as_u64)
                != Some(u64::from(TESTNET_SOAK_SCHEMA_VERSION))
        {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        }
        if record.decision == STARTED
            && projection.first_started_at.is_some()
            && !projection.awaiting_restart_start
            && !projection.running
        {
            projection.reset_campaign();
        }
        if projection
            .last_recorded_at
            .is_some_and(|last| record.timestamp < last)
        {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        }
        projection.last_recorded_at = Some(record.timestamp);
        validate_record_integrity(&mut projection, record)?;
        validate_record_elapsed(&mut projection, record)?;
        apply_projected_record(&mut projection, record)?;
    }
    Ok(projection)
}

fn project_owner_recovery_fact(
    projection: &mut EvidenceProjection,
    record: &DecisionRecord,
    task_id: &str,
) -> Result<(), TestnetSoakEvidenceError> {
    if record.decision != CAMPAIGN_RECOVERY_VERIFIED {
        return Ok(());
    }
    let Some(owner_id) = record.details.get("owner_id").and_then(Value::as_str) else {
        return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
    };
    let normalized_owner_id =
        validate_task_id(owner_id).map_err(|_| TestnetSoakEvidenceError::InvalidSoakRecord)?;
    if normalized_owner_id != owner_id {
        return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
    }
    if owner_id != task_id {
        return Ok(());
    }
    if projection
        .last_recorded_at
        .is_some_and(|last| record.timestamp < last)
        || record
            .details
            .get("campaign_id")
            .and_then(Value::as_str)
            .is_none()
        || record.details.get("schema_version").and_then(Value::as_u64)
            != Some(u64::from(CONTINUOUS_TESTNET_OWNER_SCHEMA_VERSION))
        || record.details.get("phase").and_then(Value::as_str) != Some("campaign_recovered")
        || record
            .details
            .get("observation")
            .and_then(|value| value.get("query_first"))
            .and_then(Value::as_bool)
            != Some(true)
        || record
            .details
            .get("observation")
            .and_then(|value| value.get("query_delta"))
            .and_then(Value::as_u64)
            .is_none_or(|delta| delta == 0)
        || record
            .details
            .get("observation")
            .and_then(|value| value.get("client_order_id"))
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_none()
    {
        return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
    }
    let observation = record.details.get("observation").expect("validated above");
    let before = observation
        .get("query_count_before")
        .and_then(Value::as_u64)
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)?;
    let after = observation
        .get("query_count_after")
        .and_then(Value::as_u64)
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)?;
    let delta = observation
        .get("query_delta")
        .and_then(Value::as_u64)
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)?;
    if after.checked_sub(before) != Some(delta) {
        return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
    }
    projection.last_recorded_at = Some(record.timestamp);
    if projection.running {
        projection.owner_campaign_recovery_candidate = true;
    }
    Ok(())
}

fn apply_projected_record(
    projection: &mut EvidenceProjection,
    record: &DecisionRecord,
) -> Result<(), TestnetSoakEvidenceError> {
    match record.decision.as_str() {
        STARTED => {
            if projection.running {
                return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
            }
            projection.first_started_at.get_or_insert(record.timestamp);
            projection.segment_started_at = Some(record.timestamp);
            projection.segment_last_probe_at = None;
            projection.segment_last_elapsed_milliseconds = None;
            projection.running = true;
            projection.awaiting_restart_start = false;
            projection.clean_stop = false;
        }
        UNCLEAN_RESTART => project_unclean_restart(projection)?,
        PROBE_SUCCEEDED => project_probe_success(projection, record)?,
        PROBE_FAILED => project_probe_failure(projection, record)?,
        STOPPED => {
            require_running(projection)?;
            projection.close_segment()?;
            projection.running = false;
            projection.awaiting_restart_start = false;
            projection.clean_stop = true;
            projection.consecutive_failure_count = 0;
            projection.consecutive_failure_counts = TestnetSoakSampleFailureCounts::default();
        }
        FAILED => {
            if !projection.running && !projection.awaiting_restart_start {
                return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
            }
            if projection.running {
                projection.close_segment()?;
            }
            projection.running = false;
            projection.awaiting_restart_start = false;
            projection.clean_stop = false;
        }
        _ => return Err(TestnetSoakEvidenceError::InvalidSoakRecord),
    }
    Ok(())
}

fn project_unclean_restart(
    projection: &mut EvidenceProjection,
) -> Result<(), TestnetSoakEvidenceError> {
    require_running(projection)?;
    if projection.owner_campaign_recovery_candidate {
        projection.owner_campaign_recovery_verified = true;
        projection.owner_campaign_recovery_candidate = false;
    }
    projection.unclean_restart_count = projection
        .unclean_restart_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    projection.close_segment()?;
    projection.segment_last_record_elapsed_milliseconds = None;
    projection.running = false;
    projection.awaiting_restart_start = true;
    projection.clean_stop = false;
    Ok(())
}

fn project_probe_success(
    projection: &mut EvidenceProjection,
    record: &DecisionRecord,
) -> Result<(), TestnetSoakEvidenceError> {
    require_running(projection)?;
    let sample = observation(record)?
        .get("sample")
        .and_then(Value::as_str)
        .and_then(parse_sample)
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)?;
    let campaign_elapsed_seconds = projection.campaign_elapsed_seconds_for(record)?;
    projection.successful_probe_count = projection
        .successful_probe_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    let sample_count = match sample {
        TestnetSoakSample::SpotBookTicker => &mut projection.sample_counts.spot_book_ticker,
        TestnetSoakSample::UsdMBookTicker => &mut projection.sample_counts.usd_m_book_ticker,
        TestnetSoakSample::MarketStream => &mut projection.sample_counts.market_stream,
        TestnetSoakSample::UserDataStream => &mut projection.sample_counts.user_data_stream,
        TestnetSoakSample::AuthenticatedReconcile => {
            &mut projection.sample_counts.authenticated_reconcile
        }
    };
    *sample_count = sample_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    *projection.consecutive_failure_counts.get_mut(sample) = 0;
    projection.consecutive_failure_count = projection.consecutive_failure_counts.maximum();
    projection.last_sample = Some(sample);
    if projection.consecutive_failure_count == 0 {
        projection.last_probe_failure = None;
    }
    let previous_elapsed = *projection.last_sample_elapsed_seconds.get_mut(sample);
    let gap = previous_elapsed.map_or(campaign_elapsed_seconds, |previous| {
        campaign_elapsed_seconds.saturating_sub(previous)
    });
    *projection.last_sample_elapsed_seconds.get_mut(sample) = Some(campaign_elapsed_seconds);
    let maximum_gap = sample_count_mut(&mut projection.maximum_sample_gap_seconds, sample);
    *maximum_gap = (*maximum_gap).max(gap);
    projection.segment_last_probe_at = Some(record.timestamp);
    projection.segment_last_elapsed_milliseconds =
        record_elapsed_milliseconds(record).or_else(|| {
            projection.segment_started_at.and_then(|started_at| {
                u64::try_from(
                    record
                        .timestamp
                        .signed_duration_since(started_at)
                        .num_milliseconds(),
                )
                .ok()
            })
        });
    Ok(())
}

fn project_probe_failure(
    projection: &mut EvidenceProjection,
    record: &DecisionRecord,
) -> Result<(), TestnetSoakEvidenceError> {
    require_running(projection)?;
    let failure = observation(record)?
        .get("probe_failure")
        .and_then(Value::as_str)
        .and_then(parse_probe_failure)
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)?;
    let sample = observation(record)?
        .get("sample")
        .and_then(Value::as_str)
        .and_then(parse_sample)
        .or(projection.last_sample)
        .unwrap_or(TestnetSoakSample::SpotBookTicker);
    projection.failed_probe_count = projection
        .failed_probe_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    let sample_failures = projection.consecutive_failure_counts.get_mut(sample);
    *sample_failures = sample_failures
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    projection.consecutive_failure_count = projection.consecutive_failure_counts.maximum();
    projection.last_probe_failure = Some(failure);
    projection.segment_last_probe_at = Some(record.timestamp);
    projection.segment_last_elapsed_milliseconds =
        record_elapsed_milliseconds(record).or_else(|| {
            projection.segment_started_at.and_then(|started_at| {
                u64::try_from(
                    record
                        .timestamp
                        .signed_duration_since(started_at)
                        .num_milliseconds(),
                )
                .ok()
            })
        });
    Ok(())
}

fn observation(record: &DecisionRecord) -> Result<&Value, TestnetSoakEvidenceError> {
    record
        .details
        .get("observation")
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)
}

fn validate_record_integrity(
    projection: &mut EvidenceProjection,
    record: &DecisionRecord,
) -> Result<(), TestnetSoakEvidenceError> {
    let Some(integrity) = record.details.get("integrity") else {
        if projection.integrity_record_count != 0 {
            return Err(TestnetSoakEvidenceError::IntegrityMismatch);
        }
        projection.integrity_missing = true;
        return Ok(());
    };
    if integrity.get("algorithm").and_then(Value::as_str) != Some("sha256") {
        return Err(TestnetSoakEvidenceError::IntegrityMismatch);
    }
    let previous = integrity
        .get("previous_hash")
        .and_then(Value::as_str)
        .and_then(decode_hash)
        .ok_or(TestnetSoakEvidenceError::IntegrityMismatch)?;
    let recorded = integrity
        .get("record_hash")
        .and_then(Value::as_str)
        .and_then(decode_hash)
        .ok_or(TestnetSoakEvidenceError::IntegrityMismatch)?;
    if previous != projection.integrity_head
        || (projection.integrity_missing
            && projection.integrity_record_count == 0
            && previous != [0; EVIDENCE_HASH_BYTES])
    {
        return Err(TestnetSoakEvidenceError::IntegrityMismatch);
    }
    let expected = evidence_record_digest(record, &previous)
        .map_err(|()| TestnetSoakEvidenceError::IntegrityMismatch)?;
    if recorded != expected {
        return Err(TestnetSoakEvidenceError::IntegrityMismatch);
    }
    projection.integrity_head = recorded;
    projection.integrity_record_count = projection
        .integrity_record_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    Ok(())
}

fn validate_record_elapsed(
    projection: &mut EvidenceProjection,
    record: &DecisionRecord,
) -> Result<(), TestnetSoakEvidenceError> {
    if record.decision == UNCLEAN_RESTART {
        if record_elapsed_milliseconds(record).is_none() {
            projection.monotonic_elapsed_missing = true;
        }
        return Ok(());
    }
    let Some(elapsed) = record_elapsed_milliseconds(record) else {
        projection.monotonic_elapsed_missing = true;
        return Ok(());
    };
    if record.decision == STARTED {
        if elapsed != 0 {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        }
        projection.segment_last_record_elapsed_milliseconds = Some(0);
        return Ok(());
    }
    if !projection.running {
        if projection.awaiting_restart_start && record.decision == FAILED && elapsed == 0 {
            return Ok(());
        }
        return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
    }
    if projection
        .segment_last_record_elapsed_milliseconds
        .is_some_and(|previous| elapsed < previous)
    {
        return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
    }
    projection.segment_last_record_elapsed_milliseconds = Some(elapsed);
    Ok(())
}

fn record_elapsed_milliseconds(record: &DecisionRecord) -> Option<u64> {
    record
        .details
        .get("elapsed_milliseconds")
        .and_then(Value::as_u64)
}

fn segment_milliseconds(
    started_at: DateTime<Utc>,
    last_probe_at: DateTime<Utc>,
) -> Result<u64, TestnetSoakEvidenceError> {
    u64::try_from(
        last_probe_at
            .signed_duration_since(started_at)
            .num_milliseconds(),
    )
    .map_err(|_| TestnetSoakEvidenceError::InvalidSoakRecord)
}

const fn all_samples() -> [TestnetSoakSample; 5] {
    [
        TestnetSoakSample::SpotBookTicker,
        TestnetSoakSample::UsdMBookTicker,
        TestnetSoakSample::MarketStream,
        TestnetSoakSample::UserDataStream,
        TestnetSoakSample::AuthenticatedReconcile,
    ]
}

fn sample_count_mut(counts: &mut TestnetSoakSampleCounts, sample: TestnetSoakSample) -> &mut u64 {
    match sample {
        TestnetSoakSample::SpotBookTicker => &mut counts.spot_book_ticker,
        TestnetSoakSample::UsdMBookTicker => &mut counts.usd_m_book_ticker,
        TestnetSoakSample::MarketStream => &mut counts.market_stream,
        TestnetSoakSample::UserDataStream => &mut counts.user_data_stream,
        TestnetSoakSample::AuthenticatedReconcile => &mut counts.authenticated_reconcile,
    }
}

fn require_running(projection: &EvidenceProjection) -> Result<(), TestnetSoakEvidenceError> {
    if projection.running {
        Ok(())
    } else {
        Err(TestnetSoakEvidenceError::InvalidSoakRecord)
    }
}

fn read_records(
    path: &Path,
    missing_is_empty: bool,
) -> Result<Vec<DecisionRecord>, TestnetSoakEvidenceError> {
    read_records_and_hash(path, missing_is_empty).map(|(records, _)| records)
}

fn read_records_and_hash(
    path: &Path,
    missing_is_empty: bool,
) -> Result<(Vec<DecisionRecord>, String), TestnetSoakEvidenceError> {
    let bytes = match read_journal_chain(path) {
        Ok(bytes) => bytes,
        Err(JournalReadError::Open(error)) if error.kind() == io::ErrorKind::NotFound => {
            return if missing_is_empty {
                Ok((Vec::new(), encode_hash(&sha256_digest(&[]))))
            } else {
                Err(TestnetSoakEvidenceError::SourceMissing)
            };
        }
        Err(JournalReadError::NotAFile | JournalReadError::SealedSegmentNotAFile { .. }) => {
            return Err(TestnetSoakEvidenceError::NotAFile);
        }
        Err(
            JournalReadError::SourceTooLarge { .. }
            | JournalReadError::ChainTooLarge { .. }
            | JournalReadError::TooManySegments { .. }
            | JournalReadError::SealedSegmentBytes { .. },
        ) => {
            return Err(TestnetSoakEvidenceError::SourceTooLarge);
        }
        Err(JournalReadError::Allocation { .. }) => {
            return Err(TestnetSoakEvidenceError::Allocation);
        }
        Err(JournalReadError::SourceChanged { .. }) => {
            return Err(TestnetSoakEvidenceError::SourceChanged);
        }
        Err(JournalReadError::SealedSegmentPartialTail { .. }) => {
            return Err(TestnetSoakEvidenceError::PartialRecord);
        }
        Err(_) => return Err(TestnetSoakEvidenceError::Io),
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(TestnetSoakEvidenceError::PartialRecord);
    }

    if bytes.is_empty() {
        return Ok((Vec::new(), encode_hash(&sha256_digest(&bytes))));
    }
    let source_sha256 = encode_hash(&sha256_digest(&bytes));
    let complete_bytes = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    let mut records = Vec::new();
    for raw_line in complete_bytes.split(|byte| *byte == b'\n') {
        if records.len() == MAX_TESTNET_SOAK_EVIDENCE_RECORDS {
            return Err(TestnetSoakEvidenceError::TooManyRecords);
        }
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            return Err(TestnetSoakEvidenceError::EmptyRecord);
        }
        if line.len().saturating_add(1) > MAX_HISTORY_RECORD_BYTES {
            return Err(TestnetSoakEvidenceError::RecordTooLarge);
        }
        let record = serde_json::from_slice::<DecisionRecord>(line)
            .map_err(|_| TestnetSoakEvidenceError::MalformedRecord)?;
        records.push(record);
    }
    Ok((records, source_sha256))
}

async fn append_with_current_integrity_head(
    history: &JsonlHistory,
    task_id: &str,
    mut record: DecisionRecord,
) -> Result<(), TestnetSoakTaskError> {
    repair_soak_history(history, Some(task_id))
        .await
        .map_err(TestnetSoakTaskError::Journal)?;
    let records = read_records(history.path(), true)?;
    let projection = project_records(&records, task_id)?;
    let mut integrity_head = projection.integrity_head;
    chain_record(&mut record, &mut integrity_head)?;
    history
        .append(&record)
        .await
        .map_err(TestnetSoakTaskError::Journal)
}

async fn repair_soak_history(
    history: &JsonlHistory,
    task_id: Option<&str>,
) -> Result<(), HistoryError> {
    let observation = match history.repair_recoverable_tail().await? {
        HistoryTailRepairOutcome::Unchanged { .. } => return Ok(()),
        HistoryTailRepairOutcome::Quarantined {
            retained_bytes,
            quarantined_bytes,
            quarantine_path,
            pruned_files,
            pruned_bytes,
        } => json!({
            "repair": "quarantined",
            "retained_bytes": retained_bytes,
            "quarantined_bytes": quarantined_bytes,
            "quarantine_path": quarantine_path,
            "pruned_files": pruned_files,
            "pruned_bytes": pruned_bytes,
        }),
        HistoryTailRepairOutcome::TerminatorRestored { retained_bytes } => json!({
            "repair": "terminator_restored",
            "retained_bytes": retained_bytes,
        }),
    };
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: HISTORY_REPAIR_STRATEGY.to_owned(),
            symbol: TASK_SYMBOL.to_owned(),
            decision: HISTORY_TAIL_REPAIRED.to_owned(),
            details: json!({
                "component": "testnet_soak",
                "task_id": task_id,
                "observation": observation,
            }),
        })
        .await
}

fn repair_soak_history_blocking(
    history_path: &Path,
    task_id: Option<&str>,
) -> Result<(), TestnetSoakEvidenceError> {
    let history_path = history_path.to_owned();
    let task_id = task_id.map(str::to_owned);
    let join = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| TestnetSoakEvidenceError::Io)?;
        runtime.block_on(async {
            repair_soak_history(&JsonlHistory::new(history_path), task_id.as_deref())
                .await
                .map_err(|_| TestnetSoakEvidenceError::Io)
        })
    });
    join.join().unwrap_or(Err(TestnetSoakEvidenceError::Io))
}

fn started_record(task_id: &str, recorded_at: DateTime<Utc>) -> DecisionRecord {
    record(task_id, STARTED, "running", recorded_at, 0, &Value::Null)
}

fn unclean_restart_record(task_id: &str, recorded_at: DateTime<Utc>) -> DecisionRecord {
    record(
        task_id,
        UNCLEAN_RESTART,
        "unclean_restart_detected",
        recorded_at,
        0,
        &Value::Null,
    )
}

fn probe_success_record(
    status: &TestnetSoakTaskStatus,
    sample: TestnetSoakSample,
) -> DecisionRecord {
    record(
        &status.task_id,
        PROBE_SUCCEEDED,
        "running",
        status.last_recorded_at,
        status.segment_elapsed_milliseconds,
        &json!({
            "sample": sample_label(sample),
            "successful_probe_count": status.successful_probe_count,
            "failed_probe_count": status.failed_probe_count,
            "consecutive_failure_count": status.consecutive_failure_count,
            "consecutive_failure_counts": failure_counts_json(status.consecutive_failure_counts),
        }),
    )
}

fn probe_failure_record(
    status: &TestnetSoakTaskStatus,
    sample: TestnetSoakSample,
    failure: TestnetSoakProbeFailure,
) -> DecisionRecord {
    record(
        &status.task_id,
        PROBE_FAILED,
        "running",
        status.last_recorded_at,
        status.segment_elapsed_milliseconds,
        &json!({
            "sample": sample_label(sample),
            "probe_failure": probe_failure_label(failure),
            "successful_probe_count": status.successful_probe_count,
            "failed_probe_count": status.failed_probe_count,
            "consecutive_failure_count": status.consecutive_failure_count,
            "consecutive_failure_counts": failure_counts_json(status.consecutive_failure_counts),
        }),
    )
}

fn failure_counts_json(counts: TestnetSoakSampleFailureCounts) -> Value {
    json!({
        "spot_book_ticker": counts.spot_book_ticker,
        "usd_m_book_ticker": counts.usd_m_book_ticker,
        "market_stream": counts.market_stream,
        "user_data_stream": counts.user_data_stream,
        "authenticated_reconcile": counts.authenticated_reconcile,
    })
}

fn stopped_record(status: &TestnetSoakTaskStatus) -> DecisionRecord {
    record(
        &status.task_id,
        STOPPED,
        "stopped",
        status.last_recorded_at,
        status.segment_elapsed_milliseconds,
        &json!({
            "exit": "stop_requested",
            "successful_probe_count": status.successful_probe_count,
            "failed_probe_count": status.failed_probe_count,
            "unclean_restart_count": status.unclean_restart_count,
        }),
    )
}

fn terminal_failure_record(
    status: &TestnetSoakTaskStatus,
    probe_failure: Option<TestnetSoakProbeFailure>,
) -> DecisionRecord {
    record(
        &status.task_id,
        FAILED,
        "failed",
        status.last_recorded_at,
        status.segment_elapsed_milliseconds,
        &json!({
            "task_failure": status.failure.map(task_failure_label),
            "probe_failure": probe_failure.map(probe_failure_label),
            "successful_probe_count": status.successful_probe_count,
            "failed_probe_count": status.failed_probe_count,
            "unclean_restart_count": status.unclean_restart_count,
        }),
    )
}

fn record(
    task_id: &str,
    decision: &'static str,
    phase: &'static str,
    recorded_at: DateTime<Utc>,
    elapsed_milliseconds: u64,
    observation: &Value,
) -> DecisionRecord {
    DecisionRecord {
        timestamp: recorded_at,
        strategy: TASK_STRATEGY.to_owned(),
        symbol: TASK_SYMBOL.to_owned(),
        decision: decision.to_owned(),
        details: json!({
            "schema_version": TESTNET_SOAK_SCHEMA_VERSION,
            "task_id": task_id,
            "task_kind": TESTNET_SOAK_TASK_KIND,
            "phase": phase,
            "elapsed_milliseconds": elapsed_milliseconds,
            "observation": observation,
        }),
    }
}

fn duration_milliseconds(duration: Duration) -> Result<u64, TestnetSoakTaskError> {
    u64::try_from(duration.as_millis()).map_err(|_| TestnetSoakTaskError::CounterOverflow)
}

fn chain_records(
    records: &mut [DecisionRecord],
    integrity_head: &mut [u8; EVIDENCE_HASH_BYTES],
) -> Result<(), TestnetSoakTaskError> {
    for record in records {
        chain_record(record, integrity_head)?;
    }
    Ok(())
}

fn chain_record(
    record: &mut DecisionRecord,
    integrity_head: &mut [u8; EVIDENCE_HASH_BYTES],
) -> Result<(), TestnetSoakTaskError> {
    let previous = *integrity_head;
    let digest = evidence_record_digest(record, &previous)
        .map_err(|()| TestnetSoakTaskError::EvidenceIntegrity)?;
    let details = record
        .details
        .as_object_mut()
        .ok_or(TestnetSoakTaskError::EvidenceIntegrity)?;
    details.insert(
        "integrity".to_owned(),
        json!({
            "algorithm": "sha256",
            "previous_hash": encode_hash(&previous),
            "record_hash": encode_hash(&digest),
        }),
    );
    *integrity_head = digest;
    Ok(())
}

fn evidence_record_digest(
    record: &DecisionRecord,
    previous: &[u8; EVIDENCE_HASH_BYTES],
) -> Result<[u8; EVIDENCE_HASH_BYTES], ()> {
    let mut unsigned = record.clone();
    unsigned
        .details
        .as_object_mut()
        .ok_or(())?
        .remove("integrity");
    let encoded = serde_json::to_vec(&unsigned).map_err(|_| ())?;
    let mut preimage = Vec::new();
    preimage
        .try_reserve_exact(EVIDENCE_INTEGRITY_DOMAIN.len() + previous.len() + encoded.len())
        .map_err(|_| ())?;
    preimage.extend_from_slice(EVIDENCE_INTEGRITY_DOMAIN);
    preimage.extend_from_slice(previous);
    preimage.extend_from_slice(&encoded);
    Ok(sha256_digest(&preimage))
}

fn encode_hash(hash: &[u8; EVIDENCE_HASH_BYTES]) -> String {
    let mut encoded = String::with_capacity(EVIDENCE_HASH_BYTES * 2);
    for byte in hash {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn decode_hash(value: &str) -> Option<[u8; EVIDENCE_HASH_BYTES]> {
    if value.len() != EVIDENCE_HASH_BYTES * 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    let mut decoded = [0; EVIDENCE_HASH_BYTES];
    for (index, output) in decoded.iter_mut().enumerate() {
        let start = index * 2;
        *output = u8::from_str_radix(&value[start..start + 2], 16).ok()?;
    }
    Some(decoded)
}

fn validate_task_id(task_id: &str) -> Result<String, TestnetSoakTaskError> {
    let normalized = task_id.trim();
    if normalized.is_empty()
        || normalized.len() > MAX_TASK_ID_BYTES
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(TestnetSoakTaskError::InvalidConfig);
    }
    Ok(normalized.to_owned())
}

const fn sample_label(sample: TestnetSoakSample) -> &'static str {
    match sample {
        TestnetSoakSample::SpotBookTicker => "spot_book_ticker",
        TestnetSoakSample::UsdMBookTicker => "usd_m_book_ticker",
        TestnetSoakSample::MarketStream => "market_stream",
        TestnetSoakSample::UserDataStream => "user_data_stream",
        TestnetSoakSample::AuthenticatedReconcile => "authenticated_reconcile",
    }
}

fn parse_sample(value: &str) -> Option<TestnetSoakSample> {
    match value {
        "spot_book_ticker" => Some(TestnetSoakSample::SpotBookTicker),
        "usd_m_book_ticker" => Some(TestnetSoakSample::UsdMBookTicker),
        "market_stream" => Some(TestnetSoakSample::MarketStream),
        "user_data_stream" => Some(TestnetSoakSample::UserDataStream),
        "authenticated_reconcile" => Some(TestnetSoakSample::AuthenticatedReconcile),
        _ => None,
    }
}

const fn sample_coverage_label(requirement: TestnetSoakSampleCoverageRequirement) -> &'static str {
    match requirement {
        TestnetSoakSampleCoverageRequirement::NotRequired => "not_required",
        TestnetSoakSampleCoverageRequirement::AllKinds => "all_kinds",
        TestnetSoakSampleCoverageRequirement::StreamingPath => "streaming_path",
    }
}

const fn probe_failure_label(failure: TestnetSoakProbeFailure) -> &'static str {
    match failure {
        TestnetSoakProbeFailure::Transport => "transport",
        TestnetSoakProbeFailure::Timeout => "timeout",
        TestnetSoakProbeFailure::RateLimited => "rate_limited",
        TestnetSoakProbeFailure::ClockSkew => "clock_skew",
        TestnetSoakProbeFailure::RemoteRejected => "remote_rejected",
        TestnetSoakProbeFailure::Protocol => "protocol",
        TestnetSoakProbeFailure::Unavailable => "unavailable",
    }
}

fn parse_probe_failure(value: &str) -> Option<TestnetSoakProbeFailure> {
    match value {
        "transport" => Some(TestnetSoakProbeFailure::Transport),
        "timeout" => Some(TestnetSoakProbeFailure::Timeout),
        "rate_limited" => Some(TestnetSoakProbeFailure::RateLimited),
        "clock_skew" => Some(TestnetSoakProbeFailure::ClockSkew),
        "remote_rejected" => Some(TestnetSoakProbeFailure::RemoteRejected),
        "protocol" => Some(TestnetSoakProbeFailure::Protocol),
        "unavailable" => Some(TestnetSoakProbeFailure::Unavailable),
        _ => None,
    }
}

const fn task_failure_label(failure: TestnetSoakTaskFailure) -> &'static str {
    match failure {
        TestnetSoakTaskFailure::ProbeFailureThreshold => "probe_failure_threshold",
        TestnetSoakTaskFailure::CounterOverflow => "counter_overflow",
        TestnetSoakTaskFailure::JournalUnavailable => "journal_unavailable",
        TestnetSoakTaskFailure::TaskPanicked => "task_panicked",
        TestnetSoakTaskFailure::TaskCancelled => "task_cancelled",
        TestnetSoakTaskFailure::ProbeShutdown => "probe_shutdown",
        TestnetSoakTaskFailure::EvidenceIntegrity => "evidence_integrity",
    }
}

const fn evidence_violation_label(violation: TestnetSoakEvidenceViolation) -> &'static str {
    match violation {
        TestnetSoakEvidenceViolation::MinimumDuration => "minimum_duration",
        TestnetSoakEvidenceViolation::MinimumSuccessfulProbes => "minimum_successful_probes",
        TestnetSoakEvidenceViolation::CleanStopMissing => "clean_stop_missing",
        TestnetSoakEvidenceViolation::UncleanRestartMissing => "unclean_restart_missing",
        TestnetSoakEvidenceViolation::SpotBookTickerMissing => "spot_book_ticker_missing",
        TestnetSoakEvidenceViolation::UsdMBookTickerMissing => "usd_m_book_ticker_missing",
        TestnetSoakEvidenceViolation::MarketStreamMissing => "market_stream_missing",
        TestnetSoakEvidenceViolation::UserDataStreamMissing => "user_data_stream_missing",
        TestnetSoakEvidenceViolation::AuthenticatedReconcileMissing => {
            "authenticated_reconcile_missing"
        }
        TestnetSoakEvidenceViolation::OwnerCampaignRecoveryMissing => {
            "owner_campaign_recovery_missing"
        }
        TestnetSoakEvidenceViolation::MarketStreamDensity => "market_stream_density",
        TestnetSoakEvidenceViolation::UserDataStreamDensity => "user_data_stream_density",
        TestnetSoakEvidenceViolation::AuthenticatedReconcileDensity => {
            "authenticated_reconcile_density"
        }
        TestnetSoakEvidenceViolation::MarketStreamGapExceeded => "market_stream_gap_exceeded",
        TestnetSoakEvidenceViolation::UserDataStreamGapExceeded => "user_data_stream_gap_exceeded",
        TestnetSoakEvidenceViolation::AuthenticatedReconcileGapExceeded => {
            "authenticated_reconcile_gap_exceeded"
        }
        TestnetSoakEvidenceViolation::IntegrityChainMissing => "integrity_chain_missing",
        TestnetSoakEvidenceViolation::MonotonicElapsedMissing => "monotonic_elapsed_missing",
    }
}

/// Bounded owner error. Error strings never contain probe or response text.
pub enum TestnetSoakTaskError {
    InvalidConfig,
    Evidence(TestnetSoakEvidenceError),
    Journal(HistoryError),
    ProbeFailureThreshold(TestnetSoakProbeFailure),
    CounterOverflow,
    TaskPanicked,
    TaskCancelled,
    ProbeShutdown,
    EvidenceIntegrity,
    PreviouslyFailed(TestnetSoakTaskFailure),
}

impl fmt::Debug for TestnetSoakTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl TestnetSoakTaskError {
    const fn failure_bucket(&self) -> TestnetSoakTaskFailure {
        match self {
            Self::InvalidConfig | Self::Evidence(_) | Self::CounterOverflow => {
                TestnetSoakTaskFailure::CounterOverflow
            }
            Self::Journal(_) => TestnetSoakTaskFailure::JournalUnavailable,
            Self::ProbeFailureThreshold(_) => TestnetSoakTaskFailure::ProbeFailureThreshold,
            Self::TaskPanicked => TestnetSoakTaskFailure::TaskPanicked,
            Self::TaskCancelled => TestnetSoakTaskFailure::TaskCancelled,
            Self::ProbeShutdown => TestnetSoakTaskFailure::ProbeShutdown,
            Self::EvidenceIntegrity => TestnetSoakTaskFailure::EvidenceIntegrity,
            Self::PreviouslyFailed(failure) => *failure,
        }
    }
}

impl From<TestnetSoakEvidenceError> for TestnetSoakTaskError {
    fn from(error: TestnetSoakEvidenceError) -> Self {
        Self::Evidence(error)
    }
}

impl fmt::Display for TestnetSoakTaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig => "invalid testnet soak configuration",
            Self::Evidence(_) => "testnet soak evidence read failed",
            Self::Journal(_) => "testnet soak journal write failed",
            Self::ProbeFailureThreshold(_) => "testnet soak failure threshold reached",
            Self::CounterOverflow => "testnet soak counter overflow",
            Self::TaskPanicked => "testnet soak task panicked",
            Self::TaskCancelled => "testnet soak task was cancelled",
            Self::ProbeShutdown => "testnet soak probe shutdown failed",
            Self::EvidenceIntegrity => "testnet soak evidence integrity failed",
            Self::PreviouslyFailed(_) => "testnet soak task previously failed",
        })
    }
}

impl std::error::Error for TestnetSoakTaskError {}

/// Bounded evidence read or projection failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetSoakEvidenceError {
    InvalidTaskId,
    InvalidRequirements,
    SourceMissing,
    Io,
    NotAFile,
    SourceTooLarge,
    Allocation,
    SourceChanged,
    PartialRecord,
    EmptyRecord,
    RecordTooLarge,
    TooManyRecords,
    MalformedRecord,
    InvalidSoakRecord,
    IntegrityMismatch,
    CounterOverflow,
}

impl fmt::Display for TestnetSoakEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidTaskId => "invalid testnet soak task identifier",
            Self::InvalidRequirements => "invalid testnet soak evidence requirements",
            Self::SourceMissing => "testnet soak evidence source is missing",
            Self::Io => "testnet soak evidence I/O failed",
            Self::NotAFile => "testnet soak evidence source is not a file",
            Self::SourceTooLarge => "testnet soak evidence source exceeds its byte budget",
            Self::Allocation => "testnet soak evidence allocation failed",
            Self::SourceChanged => "testnet soak evidence source changed while reading",
            Self::PartialRecord => "testnet soak evidence has a partial record",
            Self::EmptyRecord => "testnet soak evidence contains an empty record",
            Self::RecordTooLarge => "testnet soak evidence record exceeds its byte budget",
            Self::TooManyRecords => "testnet soak evidence exceeds its record budget",
            Self::MalformedRecord => "testnet soak evidence contains malformed JSON",
            Self::InvalidSoakRecord => "testnet soak evidence violates its fact contract",
            Self::IntegrityMismatch => "testnet soak evidence integrity chain does not verify",
            Self::CounterOverflow => "testnet soak evidence counter overflow",
        })
    }
}

impl std::error::Error for TestnetSoakEvidenceError {}
