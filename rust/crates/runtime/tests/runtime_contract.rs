use std::path::PathBuf;

use chrono::Utc;
use crypto_trading_runtime::{
    DecisionRecord, ExecutionMode, HistoryError, JsonlHistory, LIVE_ACKNOWLEDGEMENT,
    MAX_HISTORY_RECORD_BYTES,
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn live_mode_requires_the_exact_operator_acknowledgement() {
    assert!(ExecutionMode::live(None).is_err());
    assert!(ExecutionMode::live(Some("yes")).is_err());
    assert!(ExecutionMode::live(Some(LIVE_ACKNOWLEDGEMENT)).is_ok());
}

#[tokio::test]
async fn history_is_append_only_jsonl() {
    let path = unique_temp_path("runtime-history");
    let history = JsonlHistory::new(&path);

    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "grid".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "place_order".to_owned(),
            details: json!({"price": "100.1", "quantity": "0.001"}),
        })
        .await
        .unwrap();
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "grid".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "hold".to_owned(),
            details: json!({}),
        })
        .await
        .unwrap();

    let body = tokio::fs::read_to_string(&path).await.unwrap();
    let rows: Vec<serde_json::Value> = body
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["decision"], "place_order");
    assert_eq!(rows[1]["decision"], "hold");

    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn history_appends_one_complete_run_as_a_serialized_synced_batch() {
    let path = unique_temp_path("runtime-history-batch");
    let history = JsonlHistory::new(&path);
    let records = vec![
        DecisionRecord {
            timestamp: Utc::now(),
            strategy: "arbitrage".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "execution_planned".to_owned(),
            details: json!({"batch_id": Uuid::new_v4(), "legs": 2}),
        },
        DecisionRecord {
            timestamp: Utc::now(),
            strategy: "arbitrage".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "execution_completed".to_owned(),
            details: json!({"receipts": 2}),
        },
    ];

    history.append_batch(&records).await.unwrap();

    let body = tokio::fs::read_to_string(&path).await.unwrap();
    let parsed = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0]["decision"], "execution_planned");
    assert_eq!(parsed[1]["decision"], "execution_completed");
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn separately_constructed_histories_do_not_interleave_batches() {
    let path = unique_temp_path("runtime-history-concurrent");
    let mut tasks = Vec::new();
    for index in 0..32 {
        let history = JsonlHistory::new(&path);
        tasks.push(tokio::spawn(async move {
            let batch_id = Uuid::new_v4();
            let records = [
                DecisionRecord {
                    timestamp: Utc::now(),
                    strategy: "arbitrage".to_owned(),
                    symbol: "BTC".to_owned(),
                    decision: "execution_planned".to_owned(),
                    details: json!({"batch_id": batch_id, "index": index}),
                },
                DecisionRecord {
                    timestamp: Utc::now(),
                    strategy: "arbitrage".to_owned(),
                    symbol: "BTC".to_owned(),
                    decision: "execution_completed".to_owned(),
                    details: json!({"batch_id": batch_id, "index": index}),
                },
            ];
            history.append_batch(&records).await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    let body = tokio::fs::read_to_string(&path).await.unwrap();
    let rows = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 64);
    for pair in rows.chunks_exact(2) {
        assert_eq!(pair[0]["decision"], "execution_planned");
        assert_eq!(pair[1]["decision"], "execution_completed");
        assert_eq!(
            pair[0]["details"]["batch_id"],
            pair[1]["details"]["batch_id"]
        );
    }
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn history_rejects_an_oversized_record_before_touching_the_file() {
    let path = unique_temp_path("runtime-history-oversized");
    let history = JsonlHistory::new(&path);
    let error = history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "grid".to_owned(),
            symbol: "BTC".to_owned(),
            decision: "oversized".to_owned(),
            details: json!({"payload": "x".repeat(MAX_HISTORY_RECORD_BYTES + 1)}),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, HistoryError::RecordTooLarge { .. }));
    assert!(!path.exists());
}

fn unique_temp_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}.jsonl", Uuid::new_v4()))
}
