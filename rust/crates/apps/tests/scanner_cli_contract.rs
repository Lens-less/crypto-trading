use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crypto_trading_runtime::{
    JournalSnapshot, ProjectionStatus, ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel,
    ReadOnlyTaskRecovery, VirtualGridScannerReadModel,
};

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

/// Every replay event is processed before stop, so the durable ranking fact is
/// a deterministic function of the checked-in fixture and scanner config.
const FIXTURE_EVENT_COUNT: u64 = 6;

#[test]
fn scanner_serve_process_ranks_the_replay_and_stops_through_the_control_host() {
    let task_id = format!("scanner-serve-smoke-{}", std::process::id());
    let history = temp_path("scanner-serve-history", "jsonl");
    let control_port = free_port();
    let mut child = ChildGuard(Some(
        Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
            .args([
                "scanner",
                "--mode",
                "serve",
                "--replay",
                "fixtures/m6-scanner-replay.jsonl",
                "--task-id",
                task_id.as_str(),
                "--history-path",
                history.to_str().unwrap(),
                "--control-port",
                &control_port.to_string(),
                "--control-poll-interval-ms",
                "25",
                // The grace is purely a hang budget: a graceful stop is
                // signal-driven and fast, while a small grace lets CI
                // scheduling jitter force `exit=shutdown_timed_out` because
                // the owner must finish several synced journal appends within
                // it before `stop` reports `exit=stop_requested`.
                "--shutdown-grace-ms",
                "30000",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));

    let status = wait_for_processed_events(&task_id, &history, control_port);
    assert!(status.status.success(), "{status:?}");
    let status_stdout = String::from_utf8(status.stdout).unwrap();
    assert!(
        status_stdout.contains(&format!("task_id={task_id}")),
        "{status_stdout}"
    );
    assert!(status_stdout.contains("phase=running"), "{status_stdout}");

    // A stop is idempotent, and one transient TCP failure against the control
    // socket must not fail the contract on a loaded CI host.
    let stop = retry_until_success(|| run_control("stop", &task_id, &history, control_port));
    assert!(stop.status.success(), "{stop:?}");
    let stop_stdout = String::from_utf8(stop.stdout).unwrap();
    assert!(stop_stdout.contains("phase=stopped"), "{stop_stdout}");
    assert!(stop_stdout.contains("exit=stop_requested"), "{stop_stdout}");

    let output = wait_with_output(child.0.take().unwrap(), Duration::from_secs(30));
    assert!(output.status.success(), "{output:?}");
    let serve_stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        serve_stdout.contains("continuous scanner task started"),
        "{serve_stdout}"
    );
    assert!(
        serve_stdout.contains("continuous scanner task stopped"),
        "{serve_stdout}"
    );
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());

    assert_deterministic_ranking_and_projections(&history, &task_id);

    // With the control endpoint gone, status must degrade to the durable
    // journal projection instead of failing or fabricating liveness.
    let projected = run_control("status", &task_id, &history, control_port);
    assert!(projected.status.success(), "{projected:?}");
    let projected_stdout = String::from_utf8(projected.stdout).unwrap();
    assert!(
        projected_stdout.contains("phase=stopped"),
        "{projected_stdout}"
    );
    assert!(
        projected_stdout.contains("recovery=none"),
        "{projected_stdout}"
    );

    std::fs::remove_file(history).unwrap();
}

#[test]
fn scanner_serve_without_replay_fails_closed() {
    let history = temp_path("scanner-serve-no-replay", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "scanner",
            "--mode",
            "serve",
            "--task-id",
            "scanner-no-replay",
            "--history-path",
            history.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("requires --replay"), "{stderr}");
    assert!(!history.exists());
}

#[test]
fn scanner_default_mode_still_validates_and_now_succeeds() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("scanner")
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("valid: scanner"), "{stdout}");
    assert!(stdout.contains("exchange=binance"), "{stdout}");
    assert!(stdout.contains("enabled=1"), "{stdout}");
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());
}

/// The durable ranking is a deterministic function of the checked-in fixture
/// and scanner config, and both read models must accept the same journal.
fn assert_deterministic_ranking_and_projections(history: &Path, task_id: &str) {
    let journal = std::fs::read_to_string(history).unwrap();
    assert!(journal.contains("\"task_kind\":\"scanner\""), "{journal}");
    for decision in [
        "\"decision\":\"task_registered\"",
        "\"decision\":\"task_running\"",
        "\"decision\":\"task_stopping\"",
        "\"decision\":\"task_stopped\"",
        "\"decision\":\"scanner_ranked\"",
    ] {
        assert!(journal.contains(decision), "{journal}");
    }
    // Two complete 99000<->100000 cycles inside one exact 360-second window.
    for fact in [
        "\"schema_version\":2",
        "\"rating_grade\":\"s\"",
        "\"rating_score\":\"95\"",
        "\"estimated_apr\":\"14016\"",
        "\"estimated_apr_kind\":\"heuristic\"",
        "\"order_notional_usdc\":\"100\"",
        "\"round_trip_fee_percent\":\"0.2\"",
        "\"cycles_per_hour\":\"20\"",
        "\"complete_cycles\":2",
        "\"ranking_policy\":\"explicit_benchmark_then_apr_desc\"",
    ] {
        assert!(journal.contains(fact), "{fact} missing in {journal}");
    }

    let snapshot = JournalSnapshot::new(
        "00000000-0000-0000-0000-0000000000a2".parse().unwrap(),
        std::fs::read(history).unwrap(),
    )
    .unwrap();
    let scanner = VirtualGridScannerReadModel::from_legacy_snapshot(&snapshot).unwrap();
    assert_eq!(scanner.projection_status, ProjectionStatus::Complete);
    let latest = scanner.latest.unwrap();
    assert_eq!(latest.run_id, task_id);
    assert_eq!(latest.rows.len(), 1);
    assert!(latest.rows[0].is_benchmark());
    assert_eq!(latest.estimated_apr_assumptions.order_notional_usdc, "100");
    assert_eq!(
        latest.estimated_apr_assumptions.round_trip_fee_percent,
        "0.2"
    );
    assert_eq!(latest.rows[0].estimated_apr, "14016");
    let tasks = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();
    assert_eq!(tasks.projection_status, ProjectionStatus::Complete);
    assert_eq!(tasks.tasks.len(), 1);
    assert_eq!(tasks.tasks[0].kind, ReadOnlyTaskKind::Scanner);
    assert_eq!(tasks.tasks[0].phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(tasks.tasks[0].recovery, ReadOnlyTaskRecovery::None);
    assert_eq!(tasks.tasks[0].processed_event_count, FIXTURE_EVENT_COUNT);
}

fn retry_until_success(attempt: impl Fn() -> Output) -> Output {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let output = attempt();
        if output.status.success() || std::time::Instant::now() >= deadline {
            return output;
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn run_control(mode: &str, task_id: &str, history: &Path, control_port: u16) -> Output {
    Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "scanner",
            "--mode",
            mode,
            "--task-id",
            task_id,
            "--history-path",
            history.to_str().unwrap(),
            "--control-port",
            &control_port.to_string(),
        ])
        .output()
        .unwrap()
}

fn wait_for_processed_events(task_id: &str, history: &Path, control_port: u16) -> Output {
    let expected = format!("processed_event_count={FIXTURE_EVENT_COUNT}");
    // Every checkpoint append is synced to disk, so slow-filesystem hosts can
    // take several seconds to work through the whole fixture.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = run_control("status", task_id, history, control_port);
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.contains("phase=running") && stdout.contains(&expected) {
                return output;
            }
        }
        assert!(std::time::Instant::now() < deadline, "{output:?}");
        thread::sleep(Duration::from_millis(50));
    }
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

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
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

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
