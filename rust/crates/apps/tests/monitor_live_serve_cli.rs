//! Serve-mode contract for the explicit polling fallback of the `--live`
//! binance+hyperliquid pair.
//!
//! Both venues are stubbed on loopback with fixed fixture responses, so the
//! test proves the deliberately selected polling-source wiring (base-URL
//! overrides included) without any external network dependency. The separate
//! transport contract locks WebSocket as the product default.

use std::{
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const HYPERLIQUID_FIXTURE: &str =
    include_str!("../../exchange/tests/fixtures/hyperliquid_meta_and_asset_ctxs.json");
const BINANCE_BODY: &str = r#"{"symbol":"BTCUSDT","bidPrice":"104550.0","bidQty":"1.5","askPrice":"104551.0","askQty":"2.0"}"#;

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

// One end-to-end serve/status/stop pass over two live stubs does not divide
// without losing the shared process lifecycle it asserts.
#[allow(clippy::too_many_lines)]
#[test]
fn monitor_live_serve_polls_both_loopback_venues_and_stops_cleanly() {
    let task_id = format!("monitor-live-serve-smoke-{}", std::process::id());
    let history = temp_path("monitor-live-history", "jsonl");
    let spread_history = temp_path("monitor-live-spread-history", "jsonl");
    let config = temp_path("monitor-live-config", "yaml");
    std::fs::write(
        &config,
        "exchanges:\n  - binance\n  - hyperliquid\nsymbols:\n  - BTCUSDT\nhealth_check:\n  data_timeout: 30\n",
    )
    .unwrap();
    let control_port = free_port();
    let binance_hits = Arc::new(AtomicUsize::new(0));
    let hyperliquid_hits = Arc::new(AtomicUsize::new(0));
    let binance_url = spawn_stub(
        Arc::clone(&binance_hits),
        false,
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{BINANCE_BODY}",
            BINANCE_BODY.len()
        ),
    );
    let hyperliquid_url = spawn_stub(
        Arc::clone(&hyperliquid_hits),
        true,
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{HYPERLIQUID_FIXTURE}",
            HYPERLIQUID_FIXTURE.len()
        ),
    );

    let mut child = spawn_live_serve(&LiveServeSpec {
        config: &config,
        binance_url: &binance_url,
        hyperliquid_url: &hyperliquid_url,
        task_id: &task_id,
        history: &history,
        spread_history: &spread_history,
        control_port,
    });

    let status = wait_for_status(&task_id, &history, control_port);
    assert!(status.status.success(), "{status:?}");
    let status_stdout = String::from_utf8(status.stdout).unwrap();
    assert!(
        status_stdout.contains(&format!("task_id={task_id}")),
        "{status_stdout}"
    );
    assert!(status_stdout.contains("phase=running"), "{status_stdout}");

    // Both real polling sources must actually have reached their loopback
    // venues; otherwise the live wiring silently fell back to something else.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while binance_hits.load(Ordering::SeqCst) == 0 || hyperliquid_hits.load(Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "loopback venues were never polled: binance={} hyperliquid={}",
            binance_hits.load(Ordering::SeqCst),
            hyperliquid_hits.load(Ordering::SeqCst)
        );
        thread::sleep(Duration::from_millis(25));
    }

    // A stop is idempotent, and one transient TCP failure against the control
    // socket must not fail the contract on a loaded CI host.
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let stop = loop {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
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
        if output.status.success() || std::time::Instant::now() >= stop_deadline {
            break output;
        }
        thread::sleep(Duration::from_millis(250));
    };
    assert!(stop.status.success(), "{stop:?}");
    let stop_stdout = String::from_utf8(stop.stdout).unwrap();
    assert!(stop_stdout.contains("phase=stopped"), "{stop_stdout}");

    let output = wait_with_output(child.0.take().unwrap(), Duration::from_secs(5));
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
        "\"decision\":\"task_stopped\"",
    ] {
        assert!(journal.contains(decision), "{journal}");
    }
    assert!(journal.contains("\"source_id\":\"binance\""), "{journal}");
    assert!(
        journal.contains("\"source_id\":\"hyperliquid\""),
        "{journal}"
    );

    std::fs::remove_file(history).unwrap();
    let _ = std::fs::remove_file(spread_history);
    let _ = std::fs::remove_file(config);
}

#[test]
fn monitor_live_serve_refuses_a_non_binance_hyperliquid_pair() {
    let config = temp_path("monitor-live-bad-config", "yaml");
    std::fs::write(
        &config,
        "exchanges:\n  - binance\n  - lighter\nsymbols:\n  - BTCUSDT\nhealth_check:\n  data_timeout: 30\n",
    )
    .unwrap();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
        .args([
            "monitor",
            "--mode",
            "serve",
            "--config",
            config.to_str().unwrap(),
            "--live",
            "--task-id",
            "monitor-live-refused",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("[binance, hyperliquid]"), "{stderr}");
    let _ = std::fs::remove_file(config);
}

struct LiveServeSpec<'a> {
    config: &'a Path,
    binance_url: &'a str,
    hyperliquid_url: &'a str,
    task_id: &'a str,
    history: &'a Path,
    spread_history: &'a Path,
    control_port: u16,
}

fn spawn_live_serve(spec: &LiveServeSpec<'_>) -> ChildGuard {
    ChildGuard(Some(
        Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
            .args([
                "monitor",
                "--mode",
                "serve",
                "--config",
                spec.config.to_str().unwrap(),
                "--live",
                "--live-transport",
                "polling",
                "--binance-base-url",
                spec.binance_url,
                "--hyperliquid-base-url",
                spec.hyperliquid_url,
                "--poll-interval-ms",
                "25",
                "--task-id",
                spec.task_id,
                "--history-path",
                spec.history.to_str().unwrap(),
                "--spread-history-path",
                spec.spread_history.to_str().unwrap(),
                "--control-port",
                &spec.control_port.to_string(),
                "--control-poll-interval-ms",
                "25",
                "--shutdown-grace-ms",
                "250",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ))
}

/// Serves the fixed response for every connection on a detached thread. The
/// thread parks on `accept` once the test finishes and is reclaimed when the
/// test process exits.
fn spawn_stub(hits: Arc<AtomicUsize>, expect_post: bool, response: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                break;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2_048];
            while let Ok(read) = stream.read(&mut buffer) {
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let complete = if expect_post {
                    request.ends_with(br#"{"type":"metaAndAssetCtxs"}"#)
                } else {
                    request.windows(4).any(|window| window == b"\r\n\r\n")
                };
                if complete {
                    break;
                }
            }
            hits.fetch_add(1, Ordering::SeqCst);
            let _ = stream.write_all(response.as_bytes());
        }
    });
    base_url
}

fn wait_for_status(task_id: &str, history: &Path, control_port: u16) -> Output {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let output = Command::new(binary())
            .current_dir(repo_root())
            .env("CRYPTO_TRADING_TASK_CONTROL_TOKEN", control_token())
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
