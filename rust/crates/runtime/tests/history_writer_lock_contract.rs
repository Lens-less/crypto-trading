use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use chrono::Utc;
use crypto_trading_runtime::{DecisionRecord, HistoryError, JsonlHistory};
use serde_json::Value;
use uuid::Uuid;

const HOLD_LEASE_HELPER_TEST_NAME: &str = "hold_cross_process_writer_lease_until_released";
const HISTORY_PATH_ENV: &str = "JSONL_HISTORY_LOCK_PATH";
const READY_PATH_ENV: &str = "JSONL_HISTORY_LOCK_READY_PATH";
const RELEASE_PATH_ENV: &str = "JSONL_HISTORY_LOCK_RELEASE_PATH";

#[test]
fn same_process_alias_handles_share_one_writer_lease() {
    let root = temp_root("history-writer-lease-alias");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("decisions.jsonl");
    let direct = JsonlHistory::new(&path);
    let alias = JsonlHistory::new(root.join("holder").join("..").join("decisions.jsonl"));

    runtime().block_on(async {
        direct.append(&record("first")).await.unwrap();
        alias.append(&record("second")).await.unwrap();
    });

    let rows = std::fs::read_to_string(&path).unwrap().lines().count();
    assert_eq!(rows, 2);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cross_process_second_writer_fails_closed_until_first_exits() {
    let root = temp_root("history-writer-lease-cross-process");
    std::fs::create_dir_all(&root).unwrap();
    let history_path = root.join("decisions.jsonl");
    let ready_path = root.join("holder.ready");
    let release_path = root.join("holder.release");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(HOLD_LEASE_HELPER_TEST_NAME)
        .arg("--nocapture")
        .env(HISTORY_PATH_ENV, &history_path)
        .env(READY_PATH_ENV, &ready_path)
        .env(RELEASE_PATH_ENV, &release_path)
        .env("RUST_TEST_THREADS", "1")
        .spawn()
        .unwrap();

    wait_for_path(&ready_path);

    let blocked = JsonlHistory::new(root.join("writer").join("..").join("decisions.jsonl"));
    let error = runtime()
        .block_on(blocked.append(&record("blocked")))
        .unwrap_err();
    assert!(matches!(
        &error,
        HistoryError::CrossProcessLockBusy { path }
            if path.file_name() == Some(OsStr::new("decisions.jsonl.jsonl.lock"))
    ));

    std::fs::write(&release_path, b"release").unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "holder child failed: {status}");

    let recovered = JsonlHistory::new(&history_path);
    runtime()
        .block_on(recovered.append(&record("recovered")))
        .unwrap();
    let rows = std::fs::read_to_string(&history_path)
        .unwrap()
        .lines()
        .count();
    assert_eq!(rows, 2);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn hold_cross_process_writer_lease_until_released() {
    let Some(history_path) = std::env::var_os(HISTORY_PATH_ENV).map(PathBuf::from) else {
        return;
    };
    let ready_path = PathBuf::from(std::env::var_os(READY_PATH_ENV).unwrap());
    let release_path = PathBuf::from(std::env::var_os(RELEASE_PATH_ENV).unwrap());
    let history = JsonlHistory::new(&history_path);

    runtime()
        .block_on(history.append(&record("holder")))
        .unwrap();
    std::fs::write(&ready_path, b"ready").unwrap();
    wait_for_path(&release_path);
}

fn record(decision: &str) -> DecisionRecord {
    DecisionRecord {
        timestamp: Utc::now(),
        strategy: "writer-lock-contract".to_owned(),
        symbol: "BTC-USDT".to_owned(),
        decision: decision.to_owned(),
        details: Value::Null,
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn temp_root(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
}

fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {}", path.display());
}
