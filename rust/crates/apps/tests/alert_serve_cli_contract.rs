use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

#[test]
fn price_alert_serve_process_can_start_report_status_and_stop() {
    let task_id = format!("price-alert-serve-smoke-{}", std::process::id());
    let history = temp_path("price-alert-serve-history", "jsonl");
    let control_port = free_port();
    let mut child = ChildGuard(Some(
        Command::new(binary())
            .current_dir(repo_root())
            .args([
                "price-alert",
                "--mode",
                "serve",
                "--replay",
                "fixtures/m6-price-alert-replay.jsonl",
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

    let status = wait_for_status(&task_id, &history, control_port);
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
        serve_stdout.contains("continuous price-alert task started"),
        "{serve_stdout}"
    );
    assert!(
        serve_stdout.contains("continuous price-alert task stopped"),
        "{serve_stdout}"
    );
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());

    let journal = std::fs::read_to_string(&history).unwrap();
    assert!(
        journal.contains("\"task_kind\":\"price_alert\""),
        "{journal}"
    );
    for decision in [
        "\"decision\":\"task_registered\"",
        "\"decision\":\"task_running\"",
        "\"decision\":\"task_stopping\"",
        "\"decision\":\"task_stopped\"",
    ] {
        assert!(journal.contains(decision), "{journal}");
    }

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
fn price_alert_serve_without_replay_fails_closed() {
    let history = temp_path("price-alert-serve-no-replay", "jsonl");
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "price-alert",
            "--mode",
            "serve",
            "--task-id",
            "price-alert-no-replay",
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
fn price_alert_default_mode_still_validates_and_now_succeeds() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .arg("price-alert")
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("valid: price-alert"), "{stdout}");
    assert!(stdout.contains("exchange=binance"), "{stdout}");
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());
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
        .args([
            "price-alert",
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

fn wait_for_status(task_id: &str, history: &Path, control_port: u16) -> Output {
    // Every checkpoint append is synced to disk and each poll spawns a status
    // process, so slow CI hosts can take several seconds to reach running.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = run_control("status", task_id, history, control_port);
        if output.status.success()
            && String::from_utf8_lossy(&output.stdout).contains("phase=running")
        {
            return output;
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
