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
fn monitor_serve_process_can_start_report_status_and_stop() {
    let task_id = format!("monitor-serve-smoke-{}", std::process::id());
    let history = temp_path("monitor-serve-history", "jsonl");
    let spread_history = temp_path("monitor-serve-spread-history", "jsonl");
    let control_port = free_port();
    let mut child = ChildGuard(Some(
        Command::new(binary())
            .current_dir(repo_root())
            .args([
                "monitor",
                "--mode",
                "serve",
                "--config",
                "config/arbitrage/paper-monitor-eth.yaml",
                "--replay",
                "fixtures/m3-monitor-replay.jsonl",
                "--task-id",
                task_id.as_str(),
                "--history-path",
                history.to_str().unwrap(),
                "--spread-history-path",
                spread_history.to_str().unwrap(),
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

    // A stop is idempotent, and a single transient TCP failure against the
    // control socket must not fail the contract on a loaded CI host, so the
    // command retries within a bounded budget until the stop is confirmed.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let stop = loop {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .args([
                "monitor",
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
        if output.status.success() || std::time::Instant::now() >= deadline {
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
        serve_stdout.contains("continuous monitor task started"),
        "{serve_stdout}"
    );
    assert!(
        serve_stdout.contains("continuous monitor task stopped"),
        "{serve_stdout}"
    );
    assert!(String::from_utf8(output.stderr).unwrap().is_empty());

    let journal = std::fs::read_to_string(&history).unwrap();
    for decision in [
        "\"decision\":\"task_registered\"",
        "\"decision\":\"task_running\"",
        "\"decision\":\"task_stopping\"",
        "\"decision\":\"task_stopped\"",
    ] {
        assert!(journal.contains(decision), "{journal}");
    }

    // The replay fixture is stale against the serve-mode system clock, so
    // every outcome is a waiting fact: the dedicated spread-history journal
    // must stay empty rather than recording spreads it never observed.
    // (In-process spread persistence is covered by
    // continuous_monitor_task_contract.rs.)
    let spread_journal = std::fs::read_to_string(&spread_history).unwrap_or_default();
    assert!(
        !spread_journal.contains("\"decision\":\"spread_history_record_v1\""),
        "{spread_journal}"
    );

    std::fs::remove_file(history).unwrap();
    let _ = std::fs::remove_file(spread_history);
}

fn wait_for_status(task_id: &str, history: &Path, control_port: u16) -> Output {
    // Every checkpoint append is synced to disk and each poll spawns a status
    // process, so slow CI hosts can take several seconds to reach running.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .args([
                "monitor",
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
