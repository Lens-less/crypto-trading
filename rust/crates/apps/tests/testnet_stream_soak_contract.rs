use std::{fs, path::PathBuf, time::Duration};

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use crypto_trading_runtime::{DecisionRecord, JsonlHistory};
use serde_json::{Value, json};
use uuid::Uuid;

use crypto_trading_cli::testnet_soak::{
    TestnetSoakEvidenceRequirements, TestnetSoakSampleCoverageRequirement,
    verify_testnet_soak_evidence,
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
            "schema_version": 1,
            "task_id": task_id,
            "task_kind": "binance_testnet_read_only_soak",
            "phase": "running",
            "observation": observation,
        }),
    }
}
