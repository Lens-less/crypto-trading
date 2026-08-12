pub mod task_host {
    use std::{future::Future, pin::Pin};

    pub type TaskHostStopFuture<'a, Exit, Error> =
        Pin<Box<dyn Future<Output = Result<Exit, Error>> + Send + 'a>>;

    pub trait TaskHostStatus: Clone + Send + 'static {
        fn is_terminal(&self) -> bool;
    }

    pub trait TaskHost {
        type Status: TaskHostStatus;
        type Exit: Copy + Send + 'static;
        type Error: std::error::Error + Send + Sync + 'static;

        fn status(&self) -> Self::Status;

        fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error>;
    }
}

#[path = "../src/testnet_soak.rs"]
pub mod testnet_soak;

use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use chrono::{Duration as ChronoDuration, Utc};
use crypto_trading_runtime::{
    DecisionRecord, JsonlHistory, MAX_HISTORY_RECORD_BYTES, MAX_JOURNAL_SOURCE_BYTES,
};
use serde_json::{Value, json};
use testnet_soak::{
    TestnetSoakEvidenceError, TestnetSoakEvidenceRequirements, TestnetSoakEvidenceViolation,
    TestnetSoakProbe, TestnetSoakProbeFailure, TestnetSoakProbeFuture, TestnetSoakSample,
    TestnetSoakSampleCoverageRequirement, TestnetSoakTask, TestnetSoakTaskConfig,
    TestnetSoakTaskError, TestnetSoakTaskExit, TestnetSoakTaskFailure, TestnetSoakTaskPhase,
    verify_testnet_soak_evidence,
};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(1);

struct ScriptedProbe {
    results: VecDeque<Result<TestnetSoakSample, TestnetSoakProbeFailure>>,
}

impl ScriptedProbe {
    fn new(
        results: impl IntoIterator<Item = Result<TestnetSoakSample, TestnetSoakProbeFailure>>,
    ) -> Self {
        Self {
            results: results.into_iter().collect(),
        }
    }
}

impl TestnetSoakProbe for ScriptedProbe {
    fn probe(&mut self) -> TestnetSoakProbeFuture<'_> {
        let result = self
            .results
            .pop_front()
            .unwrap_or(Ok(TestnetSoakSample::SpotBookTicker));
        Box::pin(async move { result })
    }
}

struct PendingProbe;

impl TestnetSoakProbe for PendingProbe {
    fn probe(&mut self) -> TestnetSoakProbeFuture<'_> {
        Box::pin(std::future::pending())
    }
}

fn history_path(label: &str) -> PathBuf {
    let id = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "crypto-trading-{label}-{}-{id}.jsonl",
        std::process::id()
    ))
}

async fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(timeout, async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition should become true");
}

#[tokio::test]
async fn successful_probe_and_stop_produce_verifiable_evidence() {
    let path = history_path("soak-success");
    let history = JsonlHistory::new(&path);
    let config = TestnetSoakTaskConfig::new(
        "binance-testnet-24h",
        Duration::from_secs(1),
        Duration::from_millis(50),
        3,
    )
    .unwrap();
    let mut task = TestnetSoakTask::start(
        config,
        ScriptedProbe::new([Ok(TestnetSoakSample::SpotBookTicker)]),
        history,
    )
    .await
    .unwrap();

    wait_until(Duration::from_secs(1), || {
        task.status().successful_probe_count >= 1
    })
    .await;
    assert_eq!(
        task.stop().await.unwrap(),
        TestnetSoakTaskExit::StopRequested
    );

    let summary = verify_testnet_soak_evidence(
        &path,
        "binance-testnet-24h",
        TestnetSoakEvidenceRequirements::new(
            Duration::ZERO,
            1,
            true,
            false,
            TestnetSoakSampleCoverageRequirement::NotRequired,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(summary.requirements_met);
    assert_eq!(summary.successful_probe_count, 1);
    assert!(summary.clean_stop_observed);
    assert_eq!(summary.as_json()["schema_version"], json!(1));
}

#[tokio::test]
async fn a_new_campaign_does_not_inherit_the_previous_campaign_status() {
    let path = history_path("soak-new-campaign");
    let history = JsonlHistory::new(&path);
    let config = TestnetSoakTaskConfig::new(
        "binance-testnet-new-campaign",
        Duration::from_secs(1),
        Duration::from_millis(50),
        3,
    )
    .unwrap();
    let mut first = TestnetSoakTask::start(
        config.clone(),
        ScriptedProbe::new([Ok(TestnetSoakSample::AuthenticatedReconcile)]),
        history.clone(),
    )
    .await
    .unwrap();
    wait_until(Duration::from_secs(1), || {
        first.status().successful_probe_count == 1
    })
    .await;
    first.stop().await.unwrap();

    let mut second = TestnetSoakTask::start(config, PendingProbe, history)
        .await
        .unwrap();
    let status = second.status();
    assert_eq!(status.successful_probe_count, 0);
    assert_eq!(status.failed_probe_count, 0);
    assert_eq!(status.consecutive_failure_count, 0);
    assert_eq!(status.unclean_restart_count, 0);
    assert_eq!(status.last_sample, None);
    assert_eq!(status.last_probe_failure, None);
    second.stop().await.unwrap();
}

#[tokio::test]
async fn transient_failure_recovers_and_resets_the_consecutive_counter() {
    let path = history_path("soak-recover");
    let history = JsonlHistory::new(&path);
    let config = TestnetSoakTaskConfig::new(
        "binance-testnet-recover",
        Duration::from_millis(1),
        Duration::from_millis(50),
        3,
    )
    .unwrap();
    let mut task = TestnetSoakTask::start(
        config,
        ScriptedProbe::new([
            Err(TestnetSoakProbeFailure::Transport),
            Ok(TestnetSoakSample::UsdMBookTicker),
        ]),
        history,
    )
    .await
    .unwrap();

    wait_until(Duration::from_secs(1), || {
        let status = task.status();
        status.failed_probe_count >= 1 && status.successful_probe_count >= 1
    })
    .await;
    let status = task.status();
    assert_eq!(status.phase, TestnetSoakTaskPhase::Running);
    assert_eq!(status.consecutive_failure_count, 0);
    assert_eq!(status.last_probe_failure, None);
    assert_eq!(status.last_sample, Some(TestnetSoakSample::UsdMBookTicker));
    task.stop().await.unwrap();

    let journal = fs::read_to_string(path).unwrap();
    assert!(journal.contains("\"probe_failure\":\"transport\""));
    assert!(!journal.contains("api_key"));
    assert!(!journal.contains("secret"));
}

#[tokio::test]
async fn consecutive_failure_threshold_fails_closed() {
    let path = history_path("soak-threshold");
    let history = JsonlHistory::new(&path);
    let config = TestnetSoakTaskConfig::new(
        "binance-testnet-threshold",
        Duration::from_millis(1),
        Duration::from_millis(50),
        3,
    )
    .unwrap();
    let mut task = TestnetSoakTask::start(
        config,
        ScriptedProbe::new([
            Err(TestnetSoakProbeFailure::RateLimited),
            Err(TestnetSoakProbeFailure::RateLimited),
            Err(TestnetSoakProbeFailure::RateLimited),
        ]),
        history,
    )
    .await
    .unwrap();

    wait_until(Duration::from_secs(1), || task.status().is_terminal()).await;
    let status = task.status();
    assert_eq!(status.phase, TestnetSoakTaskPhase::Failed);
    assert_eq!(
        status.failure,
        Some(TestnetSoakTaskFailure::ProbeFailureThreshold)
    );
    assert!(matches!(
        task.stop().await,
        Err(TestnetSoakTaskError::ProbeFailureThreshold(
            TestnetSoakProbeFailure::RateLimited
        ))
    ));
}

#[tokio::test]
async fn startup_detects_a_prior_running_fact_without_inventing_a_kill() {
    let path = history_path("soak-restart");
    let history = JsonlHistory::new(&path);
    history
        .append(&fact(
            Utc::now() - ChronoDuration::minutes(1),
            "binance-testnet-restart",
            "testnet_soak_started",
            Value::Null,
        ))
        .await
        .unwrap();
    let config = TestnetSoakTaskConfig::new(
        "binance-testnet-restart",
        Duration::from_secs(1),
        Duration::from_millis(50),
        3,
    )
    .unwrap();
    let mut task = TestnetSoakTask::start(
        config,
        ScriptedProbe::new([Ok(TestnetSoakSample::AuthenticatedReconcile)]),
        history,
    )
    .await
    .unwrap();
    assert_eq!(task.status().unclean_restart_count, 1);
    wait_until(Duration::from_secs(1), || {
        task.status().successful_probe_count >= 1
    })
    .await;
    task.stop().await.unwrap();

    let summary = verify_testnet_soak_evidence(
        &path,
        "binance-testnet-restart",
        TestnetSoakEvidenceRequirements::new(
            Duration::ZERO,
            1,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::NotRequired,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(summary.requirements_met);
    assert_eq!(summary.unclean_restart_count, 1);
    let journal = fs::read_to_string(path).unwrap();
    assert!(journal.contains("testnet_soak_unclean_restart_detected"));
    assert!(!journal.contains("\"kill\""));
}

#[tokio::test]
async fn twenty_four_hour_evidence_passes_and_stricter_policy_reports_violations() {
    let path = history_path("soak-24h");
    let history = JsonlHistory::new(&path);
    let task_id = "binance-testnet-evidence";
    let started_at = Utc::now() - ChronoDuration::hours(25);
    history
        .append_batch(&[
            fact(started_at, task_id, "testnet_soak_started", Value::Null),
            fact(
                started_at + ChronoDuration::hours(6),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "spot_book_ticker"}),
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "usd_m_book_ticker"}),
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_unclean_restart_detected",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_started",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "authenticated_reconcile"}),
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_stopped",
                json!({"exit": "stop_requested"}),
            ),
        ])
        .await
        .unwrap();

    let passing = verify_testnet_soak_evidence(
        &path,
        task_id,
        TestnetSoakEvidenceRequirements::new(
            Duration::from_secs(24 * 60 * 60),
            3,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::AllKinds,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(passing.requirements_met);
    assert_eq!(passing.observed_duration_seconds, 25 * 60 * 60);
    assert_eq!(passing.sample_counts.spot_book_ticker, 1);
    assert_eq!(passing.sample_counts.usd_m_book_ticker, 1);
    assert_eq!(passing.sample_counts.authenticated_reconcile, 1);

    let failing = verify_testnet_soak_evidence(
        &path,
        task_id,
        TestnetSoakEvidenceRequirements::new(
            Duration::from_secs(26 * 60 * 60),
            4,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::AllKinds,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(!failing.requirements_met);
    assert_eq!(
        failing.violations,
        vec![
            TestnetSoakEvidenceViolation::MinimumDuration,
            TestnetSoakEvidenceViolation::MinimumSuccessfulProbes,
        ]
    );
}

#[tokio::test]
async fn legacy_rest_samples_do_not_satisfy_the_streaming_policy() {
    let path = history_path("soak-public-only");
    let history = JsonlHistory::new(&path);
    let task_id = "binance-testnet-public-only";
    let started_at = Utc::now() - ChronoDuration::hours(25);
    history
        .append_batch(&[
            fact(started_at, task_id, "testnet_soak_started", Value::Null),
            fact(
                started_at + ChronoDuration::hours(6),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "authenticated_reconcile"}),
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "usd_m_book_ticker"}),
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_unclean_restart_detected",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_started",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "spot_book_ticker"}),
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_stopped",
                json!({"exit": "stop_requested"}),
            ),
        ])
        .await
        .unwrap();

    let summary = verify_testnet_soak_evidence(
        &path,
        task_id,
        TestnetSoakEvidenceRequirements::twenty_four_hour(3).unwrap(),
    )
    .unwrap();
    assert!(!summary.requirements_met);
    assert_eq!(summary.observed_duration_seconds, 25 * 60 * 60);
    assert_eq!(summary.sample_counts.spot_book_ticker, 1);
    assert_eq!(summary.sample_counts.usd_m_book_ticker, 1);
    assert_eq!(summary.sample_counts.authenticated_reconcile, 1);
    assert_eq!(
        summary.violations,
        vec![
            TestnetSoakEvidenceViolation::MarketStreamMissing,
            TestnetSoakEvidenceViolation::UserDataStreamMissing,
        ]
    );
}

#[tokio::test]
async fn a_long_offline_gap_does_not_count_toward_twenty_four_hours() {
    let path = history_path("soak-offline-gap");
    let history = JsonlHistory::new(&path);
    let task_id = "binance-testnet-offline-gap";
    let started_at = Utc::now() - ChronoDuration::hours(26);
    history
        .append_batch(&[
            fact(started_at, task_id, "testnet_soak_started", Value::Null),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_unclean_restart_detected",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_started",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(25) + ChronoDuration::minutes(1),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "spot_book_ticker"}),
            ),
            fact(
                started_at + ChronoDuration::hours(25) + ChronoDuration::minutes(1),
                task_id,
                "testnet_soak_stopped",
                json!({"exit": "stop_requested"}),
            ),
        ])
        .await
        .unwrap();

    let summary = verify_testnet_soak_evidence(
        &path,
        task_id,
        TestnetSoakEvidenceRequirements::new(
            Duration::from_secs(24 * 60 * 60),
            1,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::NotRequired,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(!summary.requirements_met);
    assert_eq!(summary.observed_duration_seconds, 60);
    assert_eq!(
        summary.violations,
        vec![TestnetSoakEvidenceViolation::MinimumDuration]
    );
    assert!(summary.clean_stop_observed);
    assert_eq!(summary.unclean_restart_count, 1);
}

#[tokio::test]
async fn a_clean_stop_from_an_older_campaign_does_not_cover_the_latest_run() {
    let path = history_path("soak-stale-clean-stop");
    let history = JsonlHistory::new(&path);
    let task_id = "binance-testnet-stale-clean-stop";
    let started_at = Utc::now() - ChronoDuration::hours(2);
    history
        .append_batch(&[
            fact(started_at, task_id, "testnet_soak_started", Value::Null),
            fact(
                started_at + ChronoDuration::hours(1),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "spot_book_ticker"}),
            ),
            fact(
                started_at + ChronoDuration::hours(1),
                task_id,
                "testnet_soak_stopped",
                json!({"exit": "stop_requested"}),
            ),
            fact(
                started_at + ChronoDuration::hours(2),
                task_id,
                "testnet_soak_started",
                Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(2),
                task_id,
                "testnet_soak_probe_succeeded",
                json!({"sample": "usd_m_book_ticker"}),
            ),
        ])
        .await
        .unwrap();

    let summary = verify_testnet_soak_evidence(
        &path,
        task_id,
        TestnetSoakEvidenceRequirements::new(
            Duration::ZERO,
            1,
            true,
            false,
            TestnetSoakSampleCoverageRequirement::NotRequired,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(!summary.requirements_met);
    assert!(!summary.clean_stop_observed);
    assert_eq!(
        summary.violations,
        vec![TestnetSoakEvidenceViolation::CleanStopMissing]
    );
}

#[test]
fn corrupt_partial_and_oversized_evidence_fail_closed() {
    let requirements = TestnetSoakEvidenceRequirements::new(
        Duration::ZERO,
        0,
        false,
        false,
        TestnetSoakSampleCoverageRequirement::NotRequired,
    )
    .unwrap();

    let malformed = history_path("soak-malformed");
    fs::write(&malformed, b"{not-json}\n").unwrap();
    assert_eq!(
        verify_testnet_soak_evidence(&malformed, "binance-testnet-corrupt", requirements),
        Err(TestnetSoakEvidenceError::MalformedRecord)
    );

    let partial = history_path("soak-partial");
    fs::write(&partial, b"{}").unwrap();
    assert_eq!(
        verify_testnet_soak_evidence(&partial, "binance-testnet-corrupt", requirements),
        Err(TestnetSoakEvidenceError::PartialRecord)
    );

    let empty_record = history_path("soak-empty-record");
    fs::write(&empty_record, b"\n").unwrap();
    assert_eq!(
        verify_testnet_soak_evidence(&empty_record, "binance-testnet-corrupt", requirements),
        Err(TestnetSoakEvidenceError::EmptyRecord)
    );

    let oversized_record = history_path("soak-record-oversized");
    let mut record = vec![b' '; MAX_HISTORY_RECORD_BYTES];
    record.push(b'\n');
    fs::write(&oversized_record, record).unwrap();
    assert_eq!(
        verify_testnet_soak_evidence(&oversized_record, "binance-testnet-corrupt", requirements),
        Err(TestnetSoakEvidenceError::RecordTooLarge)
    );

    let oversized_source = history_path("soak-source-oversized");
    let file = fs::File::create(&oversized_source).unwrap();
    file.set_len(MAX_JOURNAL_SOURCE_BYTES + 1).unwrap();
    assert_eq!(
        verify_testnet_soak_evidence(&oversized_source, "binance-testnet-corrupt", requirements),
        Err(TestnetSoakEvidenceError::SourceTooLarge)
    );
}

fn fact(
    timestamp: chrono::DateTime<Utc>,
    task_id: &str,
    decision: &str,
    observation: impl Into<Value>,
) -> DecisionRecord {
    let observation = observation.into();
    DecisionRecord {
        timestamp,
        strategy: "testnet_soak".to_owned(),
        symbol: "control-plane".to_owned(),
        decision: decision.to_owned(),
        details: json!({
            "schema_version": 1,
            "task_id": task_id,
            "task_kind": "binance_testnet_read_only_soak",
            "phase": "fixture",
            "observation": observation,
        }),
    }
}
