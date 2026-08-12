use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use crypto_trading_runtime::{DecisionRecord, JsonlHistory};
use serde_json::{Value, json};
use uuid::Uuid;

use crypto_trading_cli::testnet_soak::{
    TestnetSoakEvidenceError, TestnetSoakEvidenceRequirements, TestnetSoakEvidenceViolation,
    TestnetSoakSampleCoverageRequirement, verify_testnet_soak_evidence,
};

#[tokio::test]
async fn verifier_requires_24h_span_restart_and_all_streaming_samples() {
    let path = history_path("streaming-24h");
    let history = JsonlHistory::new(&path);
    let task_id = "binance-testnet-streaming-evidence";
    // Fixed timestamps exercise only the offline evidence projection. This is
    // deliberately not a substitute for the credentialed external 24h run.
    let started_at = Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
    history
        .append_batch(&[
            fact(started_at, task_id, "testnet_soak_started", &Value::Null),
            fact(
                started_at + ChronoDuration::hours(6),
                task_id,
                "testnet_soak_probe_succeeded",
                &json!({"sample": "market_stream"}),
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_probe_succeeded",
                &json!({"sample": "user_data_stream"}),
            ),
            owner_recovery_fact(started_at + ChronoDuration::hours(12), task_id),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_unclean_restart_detected",
                &Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(12),
                task_id,
                "testnet_soak_started",
                &Value::Null,
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_probe_succeeded",
                &json!({"sample": "authenticated_reconcile"}),
            ),
            fact(
                started_at + ChronoDuration::hours(25),
                task_id,
                "testnet_soak_stopped",
                &json!({"exit": "stop_requested"}),
            ),
        ])
        .await
        .unwrap();

    let summary = verify_testnet_soak_evidence(
        &path,
        task_id,
        TestnetSoakEvidenceRequirements::new(
            Duration::from_secs(24 * 60 * 60),
            3,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::StreamingPath,
        )
        .unwrap(),
    )
    .unwrap();

    assert!(summary.requirements_met, "{:?}", summary.violations);
    assert_eq!(summary.sample_counts.market_stream, 1);
    assert_eq!(summary.sample_counts.user_data_stream, 1);
    assert_eq!(summary.sample_counts.authenticated_reconcile, 1);
    assert_eq!(summary.unclean_restart_count, 1);

    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn verifier_rejects_missing_forged_or_detached_owner_recovery() {
    let task_id = "binance-testnet-owner-proof-negative";
    let started_at = Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();

    let missing_path = history_path("missing-owner-recovery");
    write_streaming_campaign(
        &missing_path,
        task_id,
        started_at,
        None,
        false,
        "binance_testnet_owner_soak",
    )
    .await;
    let missing = verify(&missing_path, task_id).unwrap();
    assert!(
        missing
            .violations
            .contains(&TestnetSoakEvidenceViolation::OwnerCampaignRecoveryMissing)
    );

    let forged_path = history_path("forged-owner-recovery");
    let mut forged = owner_recovery_fact(started_at + ChronoDuration::hours(12), task_id);
    forged.details["observation"]["query_delta"] = json!(3);
    forged.details["observation"]["client_order_id"] = json!("not-a-uuid");
    write_streaming_campaign(
        &forged_path,
        task_id,
        started_at,
        Some(forged),
        false,
        "binance_testnet_owner_soak",
    )
    .await;
    assert_eq!(
        verify(&forged_path, task_id).unwrap_err(),
        TestnetSoakEvidenceError::InvalidSoakRecord
    );

    let detached_path = history_path("detached-owner-recovery");
    write_streaming_campaign(
        &detached_path,
        task_id,
        started_at,
        Some(owner_recovery_fact(
            started_at + ChronoDuration::hours(11),
            task_id,
        )),
        true,
        "binance_testnet_owner_soak",
    )
    .await;
    let detached = verify(&detached_path, task_id).unwrap();
    assert!(
        detached
            .violations
            .contains(&TestnetSoakEvidenceViolation::OwnerCampaignRecoveryMissing)
    );

    let legacy_kind_path = history_path("legacy-task-kind");
    write_streaming_campaign(
        &legacy_kind_path,
        task_id,
        started_at,
        Some(owner_recovery_fact(
            started_at + ChronoDuration::hours(12),
            task_id,
        )),
        false,
        "binance_testnet_read_only_soak",
    )
    .await;
    assert_eq!(
        verify(&legacy_kind_path, task_id).unwrap_err(),
        TestnetSoakEvidenceError::InvalidSoakRecord
    );

    for path in [missing_path, forged_path, detached_path, legacy_kind_path] {
        fs::remove_file(path).unwrap();
    }
}

async fn write_streaming_campaign(
    path: &Path,
    task_id: &str,
    started_at: chrono::DateTime<Utc>,
    recovery: Option<DecisionRecord>,
    probe_after_recovery: bool,
    task_kind: &str,
) {
    let history = JsonlHistory::new(path);
    let mut records = vec![
        fact(started_at, task_id, "testnet_soak_started", &Value::Null),
        fact(
            started_at + ChronoDuration::hours(6),
            task_id,
            "testnet_soak_probe_succeeded",
            &json!({"sample": "market_stream"}),
        ),
        fact(
            started_at + ChronoDuration::hours(10),
            task_id,
            "testnet_soak_probe_succeeded",
            &json!({"sample": "user_data_stream"}),
        ),
    ];
    if let Some(recovery) = recovery {
        records.push(recovery);
    }
    if probe_after_recovery {
        records.push(fact(
            started_at + ChronoDuration::hours(11),
            task_id,
            "testnet_soak_probe_succeeded",
            &json!({"sample": "market_stream"}),
        ));
    }
    records.extend([
        fact(
            started_at + ChronoDuration::hours(12),
            task_id,
            "testnet_soak_unclean_restart_detected",
            &Value::Null,
        ),
        fact(
            started_at + ChronoDuration::hours(12),
            task_id,
            "testnet_soak_started",
            &Value::Null,
        ),
        fact(
            started_at + ChronoDuration::hours(25),
            task_id,
            "testnet_soak_probe_succeeded",
            &json!({"sample": "authenticated_reconcile"}),
        ),
        fact(
            started_at + ChronoDuration::hours(25),
            task_id,
            "testnet_soak_stopped",
            &json!({"exit": "stop_requested"}),
        ),
    ]);
    for record in &mut records {
        if record.strategy == "testnet_soak" {
            record.details["task_kind"] = json!(task_kind);
        }
    }
    history.append_batch(&records).await.unwrap();
}

fn verify(
    path: &Path,
    task_id: &str,
) -> Result<crypto_trading_cli::testnet_soak::TestnetSoakEvidenceSummary, TestnetSoakEvidenceError>
{
    verify_testnet_soak_evidence(
        path,
        task_id,
        TestnetSoakEvidenceRequirements::new(
            Duration::from_secs(24 * 60 * 60),
            3,
            true,
            true,
            TestnetSoakSampleCoverageRequirement::StreamingPath,
        )
        .unwrap(),
    )
}

fn owner_recovery_fact(timestamp: chrono::DateTime<Utc>, task_id: &str) -> DecisionRecord {
    DecisionRecord {
        timestamp,
        strategy: "binance_testnet_continuous_owner".to_owned(),
        symbol: "control-plane".to_owned(),
        decision: "continuous_testnet_campaign_recovery_verified".to_owned(),
        details: json!({
            "schema_version": 1,
            "owner_id": task_id,
            "campaign_id": "pending-campaign",
            "phase": "campaign_recovered",
            "kill_switch_latched": false,
            "observation": {
                "query_first": true,
                "query_count_before": 0,
                "query_count_after": 2,
                "query_delta": 2,
                "client_order_id": "0f3c807d-776f-4de4-85d0-93760a82dfcf",
            },
        }),
    }
}

fn history_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("crypto-trading-{label}-{}.jsonl", Uuid::new_v4()))
}

fn fact(
    timestamp: chrono::DateTime<Utc>,
    task_id: &str,
    decision: &str,
    observation: &Value,
) -> DecisionRecord {
    DecisionRecord {
        timestamp,
        strategy: "testnet_soak".to_owned(),
        symbol: "control-plane".to_owned(),
        decision: decision.to_owned(),
        details: json!({
            "schema_version": 2,
            "task_id": task_id,
            "task_kind": "binance_testnet_owner_soak",
            "phase": "running",
            "observation": observation,
        }),
    }
}
