use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use crypto_trading_cli::Cli;
use serde_json::{Value, json};

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
fn paper_start_requires_explicit_ids_and_strategy_metadata() {
    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "paper",
            "grid",
            "start",
            "--control-addr",
            "127.0.0.1:41001",
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--principal-id",
            "operator-a",
        ])
        .is_err()
    );

    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "paper",
            "arbitrage",
            "start",
            "--control-addr",
            "127.0.0.1:41001",
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--principal-id",
            "operator-a",
            "--command-id",
            "00000000-0000-0000-0000-000000000111",
            "--idempotency-key",
            "paper-arb-start-1",
            "--task-id",
            "paper-arb-btc",
            "--strategy-id",
            "arb-btc",
        ])
        .is_err()
    );
}

#[test]
fn paper_status_requires_explicit_task_identity() {
    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "paper",
            "grid",
            "status",
            "--control-addr",
            "127.0.0.1:41001",
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
        ])
        .is_err()
    );
}

#[test]
fn paper_commands_reject_non_loopback_control_addresses() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", "0123456789abcdef0123456789abcdef")
        .args([
            "paper",
            "grid",
            "status",
            "--control-addr",
            "1.1.1.1:41001",
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--task-id",
            "paper-grid-btc",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("loopback"), "{stderr}");
}

#[test]
fn paper_commands_require_a_bounded_token_env_var() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .args([
            "paper",
            "grid",
            "status",
            "--control-addr",
            "127.0.0.1:41001",
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--task-id",
            "paper-grid-btc",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("TRUSTED_SUBMIT_TOKEN"), "{stderr}");
}

#[test]
fn paper_commands_require_uppercase_token_env_var_names() {
    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("trusted_submit_token", "0123456789abcdef0123456789abcdef")
        .args([
            "paper",
            "grid",
            "status",
            "--control-addr",
            "127.0.0.1:41001",
            "--token-env-var",
            "trusted_submit_token",
            "--task-id",
            "paper-grid-btc",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("ASCII A-Z"), "{stderr}");
}

#[test]
fn paper_grid_start_posts_the_trusted_submit_envelope() {
    let command_id = "00000000-0000-0000-0000-000000000111";
    let token = "0123456789abcdef0123456789abcdef";
    let server = spawn_fixture_server(response(
        200,
        &json!({
            "schema_version": 1,
            "command_id": command_id,
            "target_task_id": "paper-grid-btc",
            "status": "applied",
            "journal_projection": "submit_command_v1",
            "source": "durable_journal",
        }),
    ));

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", token)
        .args([
            "paper",
            "grid",
            "start",
            "--control-addr",
            &server.address.to_string(),
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--principal-id",
            "operator-a",
            "--command-id",
            command_id,
            "--idempotency-key",
            "paper-grid-start-1",
            "--task-id",
            "paper-grid-btc",
            "--strategy-id",
            "grid-btc-usdc",
            "--strategy-revision",
            "2026-07-25",
        ])
        .output()
        .unwrap();

    let captured = server.finish();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(captured.method, "POST");
    assert_eq!(captured.path, "/api/v1/submit");
    assert_eq!(
        captured.headers.get("authorization").map(String::as_str),
        Some(format!("Bearer {token}").as_str())
    );
    assert_eq!(
        captured.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );

    let body: Value = serde_json::from_slice(&captured.body).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["command_id"], command_id);
    assert_eq!(body["idempotency_key"], "paper-grid-start-1");
    assert_eq!(body["target_task_id"], "paper-grid-btc");
    assert_eq!(body["permission"]["principal_id"], "operator-a");
    assert_eq!(body["permission"]["role"], "paper_operator");
    assert_eq!(body["risk_confirmation"], "paper_only");
    assert_eq!(body["command"]["kind"], "start_paper_grid");
    assert_eq!(body["command"]["strategy_id"], "grid-btc-usdc");
    assert_eq!(body["command"]["strategy_revision"], "2026-07-25");

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains(&format!("command_id={command_id}")),
        "{stdout}"
    );
    assert!(stdout.contains("status=applied"), "{stdout}");
}

#[test]
fn paper_status_reads_the_target_from_the_server_task_projection() {
    let task_id = "paper-arb-btc";
    let token = "fedcba9876543210fedcba9876543210";
    let server = spawn_fixture_server(response(
        200,
        &json!({
            "schema_version": 1,
            "journal_id": "00000000-0000-0000-0000-000000000222",
            "journal_head_sequence": 7,
            "projection_status": "complete",
            "invalid_event_count": 0,
            "tasks": [
                {
                    "task_id": task_id,
                    "kind": "arbitrage_paper",
                    "first_sequence": 1,
                    "last_sequence": 7,
                    "registered_at": "2026-07-25T00:00:00Z",
                    "updated_at": "2026-07-25T00:00:07Z",
                    "phase": "running",
                    "recovery": "investigate",
                    "processed_event_count": 3,
                    "sources": [],
                    "exit": Value::Null,
                    "failure": Value::Null,
                }
            ],
        }),
    ));

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", token)
        .args([
            "paper",
            "arbitrage",
            "status",
            "--control-addr",
            &server.address.to_string(),
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--task-id",
            task_id,
        ])
        .output()
        .unwrap();

    let captured = server.finish();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(captured.method, "GET");
    assert_eq!(captured.path, "/api/v1/tasks");
    assert_eq!(
        captured.headers.get("authorization").map(String::as_str),
        Some(format!("Bearer {token}").as_str())
    );
    assert!(captured.body.is_empty(), "{captured:?}");

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("projection_status=complete"), "{stdout}");
    assert!(stdout.contains("journal_head_sequence=7"), "{stdout}");
    assert!(stdout.contains(&format!("task_id={task_id}")), "{stdout}");
    assert!(stdout.contains("kind=arbitrage_paper"), "{stdout}");
    assert!(stdout.contains("phase=running"), "{stdout}");
    assert!(stdout.contains("recovery=investigate"), "{stdout}");
}

#[test]
fn paper_start_does_not_treat_outcome_unknown_as_applied() {
    let command_id = "00000000-0000-0000-0000-000000000333";
    let server = spawn_fixture_server(response(
        202,
        &json!({
            "schema_version": 1,
            "command_id": command_id,
            "target_task_id": "paper-grid-btc",
            "status": "outcome_unknown",
            "journal_projection": "submit_command_v1",
            "source": "durable_journal",
        }),
    ));

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", "00112233445566778899aabbccddeeff")
        .args([
            "paper",
            "grid",
            "start",
            "--control-addr",
            &server.address.to_string(),
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--principal-id",
            "operator-a",
            "--command-id",
            command_id,
            "--idempotency-key",
            "paper-grid-start-unknown",
            "--task-id",
            "paper-grid-btc",
            "--strategy-id",
            "grid-btc-usdc",
            "--strategy-revision",
            "rev-2",
        ])
        .output()
        .unwrap();

    let _captured = server.finish();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("outcome_unknown"), "{stderr}");
}

#[test]
fn paper_status_rejects_duplicate_response_headers() {
    let body = "{\"tasks\":[]}";
    let server = spawn_fixture_server(raw_response(&format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )));

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", "0123456789abcdef0123456789abcdef")
        .args([
            "paper",
            "grid",
            "status",
            "--control-addr",
            &server.address.to_string(),
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--task-id",
            "paper-grid-btc",
        ])
        .output()
        .unwrap();

    let _captured = server.finish();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("duplicated content-type"), "{stderr}");
}

#[test]
fn paper_status_rejects_non_http_1_responses() {
    let body = serde_json::to_string(&json!({
        "schema_version": 1,
        "journal_id": "00000000-0000-0000-0000-000000000444",
        "journal_head_sequence": 1,
        "projection_status": "complete",
        "invalid_event_count": 0,
        "tasks": [],
    }))
    .unwrap();
    let server = spawn_fixture_server(raw_response(&format!(
        "HTTP/2 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )));

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", "0123456789abcdef0123456789abcdef")
        .args([
            "paper",
            "grid",
            "status",
            "--control-addr",
            &server.address.to_string(),
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--task-id",
            "paper-grid-btc",
        ])
        .output()
        .unwrap();

    let _captured = server.finish();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("HTTP/1.0 or HTTP/1.1"), "{stderr}");
}

#[test]
fn paper_status_uses_one_total_http_deadline() {
    let server = spawn_slow_fixture_server(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
            .to_vec(),
        Duration::from_millis(200),
    );
    let started = std::time::Instant::now();

    let output = Command::new(binary())
        .current_dir(repo_root())
        .env("TRUSTED_SUBMIT_TOKEN", "0123456789abcdef0123456789abcdef")
        .args([
            "paper",
            "grid",
            "status",
            "--control-addr",
            &server.address.to_string(),
            "--token-env-var",
            "TRUSTED_SUBMIT_TOKEN",
            "--task-id",
            "paper-grid-btc",
        ])
        .output()
        .unwrap();

    server.finish();
    assert!(!output.status.success(), "{output:?}");
    assert!(started.elapsed() < Duration::from_secs(7));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("timed out during read"), "{stderr}");
}

#[derive(Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct FixtureServer {
    address: SocketAddr,
    handle: thread::JoinHandle<CapturedRequest>,
}

impl FixtureServer {
    fn finish(self) -> CapturedRequest {
        self.handle.join().unwrap()
    }
}

fn spawn_fixture_server(response: Vec<u8>) -> FixtureServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(&response)
            .and_then(|()| stream.flush())
            .unwrap();
        read_request(stream)
    });
    FixtureServer { address, handle }
}

fn spawn_slow_fixture_server(response: Vec<u8>, delay: Duration) -> FixtureServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        for byte in response {
            if stream.write_all(&[byte]).is_err() {
                break;
            }
            if stream.flush().is_err() {
                break;
            }
            thread::sleep(delay);
        }
        CapturedRequest {
            method: "slow".to_owned(),
            path: "/".to_owned(),
            headers: HashMap::new(),
            body: Vec::new(),
        }
    });
    FixtureServer { address, handle }
}

fn read_request(mut stream: std::net::TcpStream) -> CapturedRequest {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    let mut header_end = None;
    let mut content_length = 0usize;

    loop {
        let read = stream.read(&mut chunk).unwrap();
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if header_end.is_none() {
            header_end = find_headers_end(&buffer);
            if let Some(end) = header_end {
                content_length = parse_content_length(&buffer[..end]);
                if buffer.len() >= end + 4 + content_length {
                    break;
                }
            }
        } else if let Some(end) = header_end
            && buffer.len() >= end + 4 + content_length
        {
            break;
        }
    }

    let end = header_end.expect("request headers missing terminator");
    let head = String::from_utf8(buffer[..end].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap().to_owned();
    let path = request_parts.next().unwrap().to_owned();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect();
    let body = buffer[end + 4..end + 4 + content_length].to_vec();

    CapturedRequest {
        method,
        path,
        headers,
        body,
    }
}

fn find_headers_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &[u8]) -> usize {
    let head = String::from_utf8(headers.to_vec()).unwrap();
    for line in head.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            return value.trim().parse().unwrap();
        }
    }
    0
}

fn response(status: u16, body: &Value) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        _ => "Internal Server Error",
    };
    let body = serde_json::to_vec(body).unwrap();
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body)
    .collect()
}

fn raw_response(response: &str) -> Vec<u8> {
    response.as_bytes().to_vec()
}

#[allow(dead_code)]
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
