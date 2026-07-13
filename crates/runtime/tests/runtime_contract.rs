use std::path::PathBuf;

use chrono::Utc;
use crypto_trading_runtime::{DecisionRecord, ExecutionMode, JsonlHistory, LIVE_ACKNOWLEDGEMENT};
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

fn unique_temp_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}.jsonl", Uuid::new_v4()))
}
