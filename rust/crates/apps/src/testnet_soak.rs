//! Durable, read-only Binance testnet soak owner and offline evidence verifier.
//!
//! The task records only bounded result categories and counters. Transport
//! errors, response bodies, credentials, and other free-form text never enter
//! the decision journal.

use std::{fmt, future::Future, io, path::Path, pin::Pin, time::Duration};

use chrono::{DateTime, Utc};
use crypto_trading_runtime::{
    DecisionRecord, HistoryError, JournalReadError, JsonlHistory, MAX_HISTORY_RECORD_BYTES,
    read_journal_chain,
};
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use crate::task_host::{TaskHost, TaskHostStatus, TaskHostStopFuture};

/// Current process-local and durable fact schema.
pub const TESTNET_SOAK_SCHEMA_VERSION: u16 = 1;
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

/// Borrowing future returned by an injected read-only testnet probe.
pub type TestnetSoakProbeFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TestnetSoakSample, TestnetSoakProbeFailure>> + Send + 'a>>;

/// Async injection seam for one bounded, read-only testnet observation.
pub trait TestnetSoakProbe: Send + 'static {
    fn probe(&mut self) -> TestnetSoakProbeFuture<'_>;
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

/// Validated task timing and failure policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestnetSoakTaskConfig {
    task_id: String,
    interval: Duration,
    probe_timeout: Duration,
    consecutive_failure_threshold: u16,
}

impl TestnetSoakTaskConfig {
    /// Creates a bounded read-only soak configuration.
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
    pub unclean_restart_count: u32,
    pub last_sample: Option<TestnetSoakSample>,
    pub last_probe_failure: Option<TestnetSoakProbeFailure>,
    pub last_recorded_at: DateTime<Utc>,
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

/// Opaque owner of one read-only probe loop.
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
                if let Err(journal_error) = self
                    .history
                    .append(&terminal_failure_record(&status, None))
                    .await
                {
                    publish_runtime_failure(
                        &self.status_sender,
                        TestnetSoakTaskFailure::JournalUnavailable,
                    );
                    return Err(TestnetSoakTaskError::Journal(journal_error));
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
    let startup = StartupState {
        prior,
        continuing_campaign,
        unclean_restart_count,
        started_at,
    };
    fail_closed_at_inherited_threshold(config, history, &startup).await?;
    let mut records = Vec::with_capacity(2);
    if unclean_restart {
        records.push(unclean_restart_record(&config.task_id, started_at));
    }
    records.push(started_record(&config.task_id, started_at));
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
        || startup.prior.consecutive_failure_count < config.consecutive_failure_threshold
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
    history
        .append_batch(&records)
        .await
        .map_err(TestnetSoakTaskError::Journal)?;
    Err(TestnetSoakTaskError::ProbeFailureThreshold(failure))
}

type TaskResult = Result<TestnetSoakTaskExit, TestnetSoakTaskError>;

async fn run_owner<P>(
    mut probe: P,
    config: TestnetSoakTaskConfig,
    history: JsonlHistory,
    status_sender: watch::Sender<TestnetSoakTaskStatus>,
    mut stop: watch::Receiver<bool>,
    mut last_recorded_at: DateTime<Utc>,
) -> TaskResult
where
    P: TestnetSoakProbe,
{
    loop {
        let probe_result = tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow_and_update() {
                    return stop_owner(&history, &status_sender, last_recorded_at).await;
                }
                continue;
            }
            result = tokio::time::timeout(config.probe_timeout, probe.probe()) => {
                result.unwrap_or(Err(TestnetSoakProbeFailure::Timeout))
            }
        };
        let recorded_at = Utc::now().max(last_recorded_at);
        match probe_result {
            Ok(sample) => {
                let mut next = status_sender.borrow().clone();
                next.successful_probe_count = next
                    .successful_probe_count
                    .checked_add(1)
                    .ok_or(TestnetSoakTaskError::CounterOverflow)?;
                next.consecutive_failure_count = 0;
                next.last_sample = Some(sample);
                next.last_probe_failure = None;
                next.last_recorded_at = recorded_at;
                if let Err(error) = history.append(&probe_success_record(&next, sample)).await {
                    publish_runtime_failure(
                        &status_sender,
                        TestnetSoakTaskFailure::JournalUnavailable,
                    );
                    return Err(TestnetSoakTaskError::Journal(error));
                }
                status_sender.send_replace(next);
            }
            Err(failure) => {
                let mut next = status_sender.borrow().clone();
                next.failed_probe_count = next
                    .failed_probe_count
                    .checked_add(1)
                    .ok_or(TestnetSoakTaskError::CounterOverflow)?;
                next.consecutive_failure_count = next
                    .consecutive_failure_count
                    .checked_add(1)
                    .ok_or(TestnetSoakTaskError::CounterOverflow)?;
                next.last_probe_failure = Some(failure);
                next.last_recorded_at = recorded_at;
                if next.consecutive_failure_count >= config.consecutive_failure_threshold {
                    next.phase = TestnetSoakTaskPhase::Failed;
                    next.failure = Some(TestnetSoakTaskFailure::ProbeFailureThreshold);
                    let records = [
                        probe_failure_record(&next, failure),
                        terminal_failure_record(&next, Some(failure)),
                    ];
                    if let Err(error) = history.append_batch(&records).await {
                        publish_runtime_failure(
                            &status_sender,
                            TestnetSoakTaskFailure::JournalUnavailable,
                        );
                        return Err(TestnetSoakTaskError::Journal(error));
                    }
                    status_sender.send_replace(next);
                    return Err(TestnetSoakTaskError::ProbeFailureThreshold(failure));
                }
                if let Err(error) = history.append(&probe_failure_record(&next, failure)).await {
                    publish_runtime_failure(
                        &status_sender,
                        TestnetSoakTaskFailure::JournalUnavailable,
                    );
                    return Err(TestnetSoakTaskError::Journal(error));
                }
                status_sender.send_replace(next);
            }
        }
        last_recorded_at = recorded_at;

        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow_and_update() {
                    return stop_owner(&history, &status_sender, last_recorded_at).await;
                }
            }
            () = tokio::time::sleep(config.interval) => {}
        }
    }
}

async fn stop_owner(
    history: &JsonlHistory,
    status_sender: &watch::Sender<TestnetSoakTaskStatus>,
    last_recorded_at: DateTime<Utc>,
) -> TaskResult {
    let mut stopped = status_sender.borrow().clone();
    stopped.phase = TestnetSoakTaskPhase::Stopped;
    stopped.last_recorded_at = Utc::now().max(last_recorded_at);
    stopped.exit = Some(TestnetSoakTaskExit::StopRequested);
    stopped.failure = None;
    stopped.runtime_failure = None;
    if let Err(error) = history.append(&stopped_record(&stopped)).await {
        publish_runtime_failure(status_sender, TestnetSoakTaskFailure::JournalUnavailable);
        return Err(TestnetSoakTaskError::Journal(error));
    }
    status_sender.send_replace(stopped);
    Ok(TestnetSoakTaskExit::StopRequested)
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
pub struct TestnetSoakEvidenceRequirements {
    minimum_duration: Duration,
    minimum_successful_probes: u64,
    require_clean_stop: bool,
    require_unclean_restart: bool,
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
        Self::new(
            Duration::from_secs(24 * 60 * 60),
            minimum_successful_probes,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::StreamingPath,
        )
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
            require_clean_stop,
            require_unclean_restart,
            sample_coverage,
        })
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
pub struct TestnetSoakEvidencePolicySummary {
    pub minimum_duration_seconds: u64,
    pub minimum_successful_probes: u64,
    pub require_clean_stop: bool,
    pub require_unclean_restart: bool,
    pub sample_coverage: TestnetSoakSampleCoverageRequirement,
}

/// Machine-readable, secret-free projection of one task's evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestnetSoakEvidenceSummary {
    pub schema_version: u16,
    pub task_id: String,
    pub requirements: TestnetSoakEvidencePolicySummary,
    pub observed_duration_seconds: u64,
    pub successful_probe_count: u64,
    pub sample_counts: TestnetSoakSampleCounts,
    pub failed_probe_count: u64,
    pub clean_stop_observed: bool,
    pub unclean_restart_count: u32,
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
                "require_clean_stop": self.requirements.require_clean_stop,
                "require_unclean_restart": self.requirements.require_unclean_restart,
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
            "failed_probe_count": self.failed_probe_count,
            "clean_stop_observed": self.clean_stop_observed,
            "unclean_restart_count": self.unclean_restart_count,
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
pub fn verify_testnet_soak_evidence(
    history_path: &Path,
    task_id: &str,
    requirements: TestnetSoakEvidenceRequirements,
) -> Result<TestnetSoakEvidenceSummary, TestnetSoakEvidenceError> {
    validate_task_id(task_id).map_err(|_| TestnetSoakEvidenceError::InvalidTaskId)?;
    let records = read_records(history_path, false)?;
    let projection = project_records(&records, task_id)?;
    let observed_duration_seconds = projection.observed_duration_seconds()?;
    let clean_stop_observed = projection.clean_stop_observed();
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
    }
    Ok(TestnetSoakEvidenceSummary {
        schema_version: TESTNET_SOAK_SCHEMA_VERSION,
        task_id: task_id.to_owned(),
        requirements: TestnetSoakEvidencePolicySummary {
            minimum_duration_seconds: requirements.minimum_duration.as_secs(),
            minimum_successful_probes: requirements.minimum_successful_probes,
            require_clean_stop: requirements.require_clean_stop,
            require_unclean_restart: requirements.require_unclean_restart,
            sample_coverage: requirements.sample_coverage,
        },
        observed_duration_seconds,
        successful_probe_count: projection.successful_probe_count,
        sample_counts: projection.sample_counts,
        failed_probe_count: projection.failed_probe_count,
        clean_stop_observed,
        unclean_restart_count: projection.unclean_restart_count,
        requirements_met: violations.is_empty(),
        violations,
    })
}

#[derive(Clone, Debug, Default)]
struct EvidenceProjection {
    first_started_at: Option<DateTime<Utc>>,
    last_recorded_at: Option<DateTime<Utc>>,
    segment_started_at: Option<DateTime<Utc>>,
    segment_last_probe_at: Option<DateTime<Utc>>,
    observed_active_seconds: u64,
    successful_probe_count: u64,
    sample_counts: TestnetSoakSampleCounts,
    failed_probe_count: u64,
    consecutive_failure_count: u16,
    unclean_restart_count: u32,
    last_sample: Option<TestnetSoakSample>,
    last_probe_failure: Option<TestnetSoakProbeFailure>,
    running: bool,
    awaiting_restart_start: bool,
    clean_stop: bool,
}

impl EvidenceProjection {
    fn reset_campaign(&mut self) {
        self.first_started_at = None;
        self.segment_started_at = None;
        self.segment_last_probe_at = None;
        self.observed_active_seconds = 0;
        self.successful_probe_count = 0;
        self.sample_counts = TestnetSoakSampleCounts::default();
        self.failed_probe_count = 0;
        self.consecutive_failure_count = 0;
        self.unclean_restart_count = 0;
        self.last_sample = None;
        self.last_probe_failure = None;
        self.running = false;
        self.awaiting_restart_start = false;
        self.clean_stop = false;
    }

    fn close_segment(&mut self) -> Result<(), TestnetSoakEvidenceError> {
        let Some(started_at) = self.segment_started_at.take() else {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        };
        let last_probe_at = self.segment_last_probe_at.take();
        if let Some(last_probe_at) = last_probe_at {
            let segment_seconds = segment_seconds(started_at, last_probe_at)?;
            self.observed_active_seconds = self
                .observed_active_seconds
                .checked_add(segment_seconds)
                .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
        }
        Ok(())
    }

    fn observed_duration_seconds(&self) -> Result<u64, TestnetSoakEvidenceError> {
        let current_segment_seconds = match (self.segment_started_at, self.segment_last_probe_at) {
            (Some(started_at), Some(last_probe_at)) => segment_seconds(started_at, last_probe_at)?,
            _ => 0,
        };
        self.observed_active_seconds
            .checked_add(current_segment_seconds)
            .ok_or(TestnetSoakEvidenceError::CounterOverflow)
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
        if record.details.get("schema_version").and_then(Value::as_u64)
            != Some(u64::from(TESTNET_SOAK_SCHEMA_VERSION))
        {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        }
        if projection
            .last_recorded_at
            .is_some_and(|last| record.timestamp < last)
        {
            return Err(TestnetSoakEvidenceError::InvalidSoakRecord);
        }
        projection.last_recorded_at = Some(record.timestamp);
        apply_projected_record(&mut projection, record)?;
    }
    Ok(projection)
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
            if projection.first_started_at.is_some() && !projection.awaiting_restart_start {
                projection.reset_campaign();
            }
            projection.first_started_at.get_or_insert(record.timestamp);
            projection.segment_started_at = Some(record.timestamp);
            projection.segment_last_probe_at = None;
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
    projection.unclean_restart_count = projection
        .unclean_restart_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    projection.close_segment()?;
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
    projection.consecutive_failure_count = 0;
    projection.last_sample = Some(sample);
    projection.last_probe_failure = None;
    projection.segment_last_probe_at = Some(record.timestamp);
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
    projection.failed_probe_count = projection
        .failed_probe_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    projection.consecutive_failure_count = projection
        .consecutive_failure_count
        .checked_add(1)
        .ok_or(TestnetSoakEvidenceError::CounterOverflow)?;
    projection.last_probe_failure = Some(failure);
    projection.segment_last_probe_at = Some(record.timestamp);
    Ok(())
}

fn observation(record: &DecisionRecord) -> Result<&Value, TestnetSoakEvidenceError> {
    record
        .details
        .get("observation")
        .ok_or(TestnetSoakEvidenceError::InvalidSoakRecord)
}

fn segment_seconds(
    started_at: DateTime<Utc>,
    last_probe_at: DateTime<Utc>,
) -> Result<u64, TestnetSoakEvidenceError> {
    u64::try_from(
        last_probe_at
            .signed_duration_since(started_at)
            .num_seconds(),
    )
    .map_err(|_| TestnetSoakEvidenceError::InvalidSoakRecord)
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
    let bytes = match read_journal_chain(path) {
        Ok(bytes) => bytes,
        Err(JournalReadError::Open(error)) if error.kind() == io::ErrorKind::NotFound => {
            return if missing_is_empty {
                Ok(Vec::new())
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
        return Ok(Vec::new());
    }
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
    Ok(records)
}

fn started_record(task_id: &str, recorded_at: DateTime<Utc>) -> DecisionRecord {
    record(task_id, STARTED, "running", recorded_at, &Value::Null)
}

fn unclean_restart_record(task_id: &str, recorded_at: DateTime<Utc>) -> DecisionRecord {
    record(
        task_id,
        UNCLEAN_RESTART,
        "unclean_restart_detected",
        recorded_at,
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
        &json!({
            "sample": sample_label(sample),
            "successful_probe_count": status.successful_probe_count,
            "failed_probe_count": status.failed_probe_count,
            "consecutive_failure_count": status.consecutive_failure_count,
        }),
    )
}

fn probe_failure_record(
    status: &TestnetSoakTaskStatus,
    failure: TestnetSoakProbeFailure,
) -> DecisionRecord {
    record(
        &status.task_id,
        PROBE_FAILED,
        "running",
        status.last_recorded_at,
        &json!({
            "probe_failure": probe_failure_label(failure),
            "successful_probe_count": status.successful_probe_count,
            "failed_probe_count": status.failed_probe_count,
            "consecutive_failure_count": status.consecutive_failure_count,
        }),
    )
}

fn stopped_record(status: &TestnetSoakTaskStatus) -> DecisionRecord {
    record(
        &status.task_id,
        STOPPED,
        "stopped",
        status.last_recorded_at,
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
            "task_kind": "binance_testnet_read_only_soak",
            "phase": phase,
            "observation": observation,
        }),
    }
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
            Self::CounterOverflow => "testnet soak evidence counter overflow",
        })
    }
}

impl std::error::Error for TestnetSoakEvidenceError {}
