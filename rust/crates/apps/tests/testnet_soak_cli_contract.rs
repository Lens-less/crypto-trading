use std::{
    fmt::Write as _,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{Duration as ChronoDuration, Utc};
use crypto_trading_domain::sha256_digest;
use crypto_trading_runtime::DecisionRecord;
use serde_json::{Value, json};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

fn control_token() -> &'static str {
    "0123456789abcdef0123456789abcdef"
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn temp_path(label: &str, extension: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-{label}-{}-{nonce}.{extension}",
        std::process::id()
    ))
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_with_output(mut child: Child, timeout: Duration) -> Output {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child did not exit in time"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_status(task_id: &str, history: &Path, control_port: u16) -> Output {
    // Every journal append is synced to disk and each poll spawns a status
    // process, so slow CI hosts can take several seconds to reach running.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
            .args([
                "testnet-soak",
                "--mode",
                "status",
                "--task-id",
                task_id,
                "--history-path",
                history.to_str().unwrap(),
                "--control-port",
                &control_port.to_string(),
            ])
            .output()
            .unwrap();
        if output.status.success()
            && String::from_utf8_lossy(&output.stdout).contains("phase=running")
        {
            return output;
        }
        assert!(std::time::Instant::now() < deadline, "{output:?}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_journal_fact(history: &Path, needle: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if fs::read_to_string(history).is_ok_and(|journal| journal.contains(needle)) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "journal never contained {needle:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn write_history(path: &Path, records: &[DecisionRecord]) {
    let body = records
        .iter()
        .map(|record| serde_json::to_string(record).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{body}\n")).unwrap();
}

fn write_production_evidence_history(path: &Path, records: &mut [DecisionRecord]) {
    let mut head = [0_u8; 32];
    let mut segment_started_at = None;
    for record in records.iter_mut() {
        if record.strategy != "testnet_soak" {
            continue;
        }
        let elapsed = match record.decision.as_str() {
            "testnet_soak_started" => {
                segment_started_at = Some(record.timestamp);
                0
            }
            "testnet_soak_unclean_restart_detected" => 0,
            _ => u64::try_from(
                record
                    .timestamp
                    .signed_duration_since(segment_started_at.unwrap())
                    .num_milliseconds(),
            )
            .unwrap(),
        };
        record.details["elapsed_milliseconds"] = json!(elapsed);
        let encoded = serde_json::to_vec(record).unwrap();
        let mut preimage = b"crypto-trading/testnet-soak-evidence/v1\0".to_vec();
        preimage.extend_from_slice(&head);
        preimage.extend_from_slice(&encoded);
        let digest = sha256_digest(&preimage);
        record.details["integrity"] = json!({
            "algorithm": "sha256",
            "previous_hash": hex(&head),
            "record_hash": hex(&digest),
        });
        head = digest;
    }
    write_history(path, records);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing into a String cannot fail");
            output
        },
    )
}

fn fact(
    timestamp: chrono::DateTime<Utc>,
    task_id: &str,
    decision: &str,
    phase: &str,
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
            "phase": phase,
            "observation": observation,
        }),
    }
}

#[test]
fn serve_without_credentials_fails_before_writing_a_started_fact() {
    let history = temp_path("testnet-soak-missing-creds", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "testnet-soak",
            "--mode",
            "serve",
            "--task-id",
            "binance-testnet-soak-missing-creds",
            "--history-path",
            history.to_str().unwrap(),
            "--interval-ms",
            "5",
            "--probe-timeout-ms",
            "50",
            "--failure-threshold",
            "3",
            "--control-port",
            &free_port().to_string(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(
        !history.exists(),
        "history unexpectedly exists at {}",
        history.display()
    );
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

#[test]
fn partial_or_unacknowledged_fresh_config_fails_before_credentials_or_network() {
    let partial_history = temp_path("testnet-soak-partial-recovery", "jsonl");
    let partial = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "testnet-soak",
            "--mode",
            "serve",
            "--task-id",
            "binance-testnet-soak-partial-recovery",
            "--history-path",
            partial_history.to_str().unwrap(),
            "--interval-ms",
            "5",
            "--probe-timeout-ms",
            "50",
            "--failure-threshold",
            "3",
            "--control-port",
            &free_port().to_string(),
            "--recovery-campaign-id",
            "pending-campaign",
        ])
        .output()
        .unwrap();
    assert!(!partial.status.success(), "{partial:?}");
    let stderr = String::from_utf8(partial.stderr).unwrap();
    assert!(stderr.contains("all-or-none"), "{stderr}");
    assert!(!stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(!partial_history.exists());

    let fresh_history = temp_path("testnet-soak-fresh-recovery", "jsonl");
    let fresh = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "testnet-soak",
            "--mode",
            "serve",
            "--task-id",
            "binance-testnet-soak-fresh-recovery",
            "--history-path",
            fresh_history.to_str().unwrap(),
            "--interval-ms",
            "5",
            "--probe-timeout-ms",
            "50",
            "--failure-threshold",
            "3",
            "--control-port",
            &free_port().to_string(),
            "--recovery-campaign-id",
            "pending-campaign",
            "--recovery-client-order-id",
            "0f3c807d-776f-4de4-85d0-93760a82dfcf",
            "--recovery-market",
            "spot",
            "--recovery-side",
            "buy",
            "--recovery-quantity",
            "0.001",
            "--recovery-price",
            "49000.1",
            "--recovery-time-in-force",
            "post-only",
            "--recovery-expected-observation",
            "open",
            "--recovery-reduce-only",
            "false",
            "--recovery-poll-interval-ms",
            "2000",
            "--recovery-maximum-queries",
            "30",
        ])
        .output()
        .unwrap();
    assert!(!fresh.status.success(), "{fresh:?}");
    let stderr = String::from_utf8(fresh.stderr).unwrap();
    assert!(
        stderr.contains("requires --acknowledge-testnet-lifecycle"),
        "{stderr}"
    );
    assert!(!stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(!fresh_history.exists());
}

#[test]
fn serve_with_a_busy_control_port_fails_before_writing_a_started_fact() {
    let history = temp_path("testnet-soak-busy-port", "jsonl");
    let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let control_port = occupied.local_addr().unwrap().port();
    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "testnet-soak",
            "--mode",
            "serve",
            "--task-id",
            "binance-testnet-soak-busy-port",
            "--history-path",
            history.to_str().unwrap(),
            "--interval-ms",
            "100",
            "--probe-timeout-ms",
            "50",
            "--failure-threshold",
            "3",
            "--control-port",
            &control_port.to_string(),
            "--fixture-probe-script",
            "spot",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let journal = fs::read_to_string(&history).unwrap_or_default();
    assert!(
        !journal.contains("\"decision\":\"testnet_soak_started\""),
        "{journal}"
    );
}

#[test]
fn serve_status_and_stop_work_with_the_hidden_fixture_probe() {
    let task_id = format!("binance-testnet-soak-cli-{}", std::process::id());
    let history = temp_path("testnet-soak-serve", "jsonl");
    let control_port = free_port();
    let mut child = ChildGuard(Some(
        Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
            .args([
                "testnet-soak",
                "--mode",
                "serve",
                "--task-id",
                task_id.as_str(),
                "--history-path",
                history.to_str().unwrap(),
                "--interval-ms",
                "5",
                "--probe-timeout-ms",
                "50",
                "--failure-threshold",
                "4",
                "--control-port",
                &control_port.to_string(),
                "--control-poll-interval-ms",
                "10",
                "--fixture-probe-script",
                "spot,usdm,reconcile,transport,spot",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));

    let status = wait_for_status(&task_id, &history, control_port);
    assert!(status.status.success(), "{status:?}");
    let status_stdout = String::from_utf8(status.stdout).unwrap();
    assert!(status_stdout.contains("phase=running"), "{status_stdout}");
    assert!(status_stdout.contains(&format!("task_id={task_id}")));
    wait_for_journal_fact(&history, "\"probe_failure\":\"transport\"");

    // A stop is idempotent, and one transient TCP failure against the control
    // socket must not fail the contract on a loaded CI host.
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let stop = loop {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
            .args([
                "testnet-soak",
                "--mode",
                "stop",
                "--task-id",
                task_id.as_str(),
                "--history-path",
                history.to_str().unwrap(),
                "--control-port",
                &control_port.to_string(),
            ])
            .output()
            .unwrap();
        if output.status.success() || std::time::Instant::now() >= stop_deadline {
            break output;
        }
        thread::sleep(Duration::from_millis(250));
    };
    assert!(stop.status.success(), "{stop:?}");
    let stop_stdout = String::from_utf8(stop.stdout).unwrap();
    assert!(stop_stdout.contains("phase=stopped"), "{stop_stdout}");
    assert!(stop_stdout.contains("exit=stop_requested"), "{stop_stdout}");

    let output = wait_with_output(child.0.take().unwrap(), Duration::from_secs(30));
    assert!(output.status.success(), "{output:?}");
    let serve_stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        serve_stdout.contains("testnet soak task started"),
        "{serve_stdout}"
    );
    assert!(
        serve_stdout.contains("testnet soak task stopped"),
        "{serve_stdout}"
    );
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());

    let journal = fs::read_to_string(&history).unwrap();
    let spot_index = journal.find("\"sample\":\"spot_book_ticker\"").unwrap();
    let usdm_index = journal.find("\"sample\":\"usd_m_book_ticker\"").unwrap();
    let reconcile_index = journal
        .find("\"sample\":\"authenticated_reconcile\"")
        .unwrap();
    assert!(
        spot_index < usdm_index && usdm_index < reconcile_index,
        "{journal}"
    );
    assert!(
        journal.contains("\"probe_failure\":\"transport\""),
        "{journal}"
    );
    assert!(!journal.contains("api_key"), "{journal}");
    assert!(!journal.contains("secret"), "{journal}");
    assert!(!journal.contains("signature"), "{journal}");

    fs::remove_file(history).unwrap();
}

#[test]
fn verify_exits_nonzero_before_the_twenty_four_hour_policy_is_met() {
    let history = temp_path("testnet-soak-verify-short", "jsonl");
    let task_id = "binance-testnet-soak-verify-short";
    let started_at = Utc::now() - ChronoDuration::hours(1);
    write_history(
        &history,
        &[
            fact(
                started_at,
                task_id,
                "testnet_soak_started",
                "running",
                &Value::Null,
            ),
            fact(
                started_at + ChronoDuration::minutes(5),
                task_id,
                "testnet_soak_probe_succeeded",
                "running",
                &json!({"sample": "market_stream", "successful_probe_count": 1, "failed_probe_count": 0, "consecutive_failure_count": 0}),
            ),
            fact(
                started_at + ChronoDuration::minutes(5),
                task_id,
                "testnet_soak_stopped",
                "stopped",
                &json!({"exit": "stop_requested", "successful_probe_count": 1, "failed_probe_count": 0, "unclean_restart_count": 0}),
            ),
        ],
    );

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "testnet-soak",
            "--mode",
            "verify",
            "--task-id",
            task_id,
            "--history-path",
            history.to_str().unwrap(),
            "--minimum-successes",
            "1",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["task_id"], task_id);
    assert_eq!(stdout["requirements_met"], false);
    assert!(
        stdout["violations"]
            .as_array()
            .is_some_and(|violations| violations.iter().any(|value| value == "minimum_duration")),
        "{stdout}"
    );

    fs::remove_file(history).unwrap();
}

#[test]
fn verify_accepts_full_production_sample_coverage() {
    let history = temp_path("testnet-soak-verify-pass", "jsonl");
    let task_id = "binance-testnet-soak-verify-pass";
    let started_at = Utc::now() - ChronoDuration::hours(25);
    let mut records = vec![
        fact(
            started_at,
            task_id,
            "testnet_soak_started",
            "running",
            &Value::Null,
        ),
        fact(
            started_at + ChronoDuration::hours(6),
            task_id,
            "testnet_soak_probe_succeeded",
            "running",
            &json!({"sample": "market_stream", "successful_probe_count": 1, "failed_probe_count": 0, "consecutive_failure_count": 0}),
        ),
        fact(
            started_at + ChronoDuration::hours(12),
            task_id,
            "testnet_soak_probe_succeeded",
            "running",
            &json!({"sample": "user_data_stream", "successful_probe_count": 2, "failed_probe_count": 0, "consecutive_failure_count": 0}),
        ),
        owner_recovery_fact(started_at + ChronoDuration::hours(12), task_id),
        fact(
            started_at + ChronoDuration::hours(12),
            task_id,
            "testnet_soak_unclean_restart_detected",
            "unclean_restart_detected",
            &Value::Null,
        ),
        fact(
            started_at + ChronoDuration::hours(12),
            task_id,
            "testnet_soak_started",
            "running",
            &Value::Null,
        ),
        fact(
            started_at + ChronoDuration::hours(25),
            task_id,
            "testnet_soak_probe_succeeded",
            "running",
            &json!({"sample": "authenticated_reconcile", "successful_probe_count": 3, "failed_probe_count": 0, "consecutive_failure_count": 0}),
        ),
        fact(
            started_at + ChronoDuration::hours(25),
            task_id,
            "testnet_soak_stopped",
            "stopped",
            &json!({"exit": "stop_requested", "successful_probe_count": 3, "failed_probe_count": 0, "unclean_restart_count": 1}),
        ),
    ];
    write_production_evidence_history(&history, &mut records);

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "testnet-soak",
            "--mode",
            "verify",
            "--task-id",
            task_id,
            "--history-path",
            history.to_str().unwrap(),
            "--minimum-successes",
            "3",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["requirements_met"], true);
    assert_eq!(stdout["sample_counts"]["market_stream"], 1);
    assert_eq!(stdout["sample_counts"]["user_data_stream"], 1);
    assert_eq!(stdout["sample_counts"]["authenticated_reconcile"], 1);
    assert_eq!(stdout["unclean_restart_count"], 1);

    fs::remove_file(history).unwrap();
}

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
