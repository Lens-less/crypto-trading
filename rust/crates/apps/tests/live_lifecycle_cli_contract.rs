//! Offline contract for the acknowledged one-shot Binance Spot MAINNET
//! lifecycle command.
//!
//! Every test drives the real binary against loopback stubs or a dead
//! loopback port; nothing here can reach Binance. Owner-level invariants
//! (admission refusals, kill latch semantics, ambiguous-submit recovery)
//! live in `crates/apps/src/live_lifecycle.rs`; this file pins the CLI
//! gate order, credential separation, and the wire-level transport order.

use std::{
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use crypto_trading_domain::{MarketType, OrderIntent, Price, Quantity, Side, Symbol, TimeInForce};
use crypto_trading_runtime::DecisionRecord;
use rust_decimal::Decimal;
use serde_json::json;
use uuid::Uuid;

const MAINNET_BASE_URL_ENV: &str = "CRYPTO_TRADING_BINANCE_MAINNET_SPOT_BASE_URL";
const ACKNOWLEDGEMENT: &str = "I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE";
const CLIENT_ORDER_ID: &str = "0f3c807d-776f-4de4-85d0-93760a82dfcf";

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

/// Command whose mainnet endpoint points at a dead loopback port and whose
/// credential environment is fully scrubbed; tests opt back in per variable.
fn offline_command() -> Command {
    let mut command = Command::new(binary());
    command
        .current_dir(repo_root())
        .env(MAINNET_BASE_URL_ENV, "http://127.0.0.1:9")
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .env_remove("BINANCE_MAINNET_READ_API_KEY")
        .env_remove("BINANCE_MAINNET_READ_API_SECRET")
        .env_remove("BINANCE_MAINNET_TRADE_API_KEY")
        .env_remove("BINANCE_MAINNET_TRADE_API_SECRET");
    command
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn temp_history(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-live-lifecycle-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn cleanup_history(history: &Path) {
    let lock_path = history.with_file_name(format!(
        "{}.jsonl.lock",
        history
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("history.jsonl")
    ));
    let _ = std::fs::remove_file(history);
    let _ = std::fs::remove_file(lock_path);
}

fn base_args(history: &Path, acknowledgement: &str) -> Vec<String> {
    vec![
        "live-lifecycle".to_owned(),
        "--acknowledge-live-lifecycle".to_owned(),
        acknowledgement.to_owned(),
        "--campaign-id".to_owned(),
        "binance-mainnet-open-001".to_owned(),
        "--client-order-id".to_owned(),
        CLIENT_ORDER_ID.to_owned(),
        "--history-path".to_owned(),
        history.display().to_string(),
        "--side".to_owned(),
        "buy".to_owned(),
        "--quantity".to_owned(),
        "0.001".to_owned(),
        "--price".to_owned(),
        "49000.1".to_owned(),
        "--max-notional".to_owned(),
        "100".to_owned(),
    ]
}

fn stub_binance_server(body: &'static str) -> (String, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let mut captured = Vec::new();
        let mut idle_polls_after_first_request = 0usize;
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    idle_polls_after_first_request = 0;
                    let request = read_http_request(&mut stream);
                    captured.push(request);
                    let body = if captured.len() == 1 {
                        body
                    } else {
                        r#"{"code":500,"msg":"unexpected extra request"}"#
                    };
                    let status = if captured.len() == 1 {
                        "200 OK"
                    } else {
                        "500 Internal Server Error"
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).unwrap();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if !captured.is_empty() {
                        idle_polls_after_first_request += 1;
                        if idle_polls_after_first_request >= 40 {
                            break;
                        }
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("stub server accept failed: {error}"),
            }
        }
        captured
    });
    (base_url, server)
}

fn read_http_request(stream: &mut std::net::TcpStream) -> String {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(request).unwrap()
}

#[test]
fn exact_mainnet_acknowledgement_is_checked_before_credentials_or_journal_writes() {
    let history = temp_history("ack");
    let output = offline_command()
        .args(base_args(&history, "I UNDERSTAND"))
        .env("BINANCE_MAINNET_TRADE_API_KEY", "fixture-key")
        .env("BINANCE_MAINNET_TRADE_API_SECRET", "fixture-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(ACKNOWLEDGEMENT), "{stderr}");
    assert!(!stderr.contains("fixture-secret"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn max_notional_is_a_required_argument() {
    let history = temp_history("required-cap");
    let mut args = base_args(&history, ACKNOWLEDGEMENT);
    let cap_flag = args
        .iter()
        .position(|argument| argument == "--max-notional")
        .unwrap();
    args.drain(cap_flag..=cap_flag + 1);
    let output = offline_command().args(args).output().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--max-notional"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn notional_above_the_cap_is_refused_before_credentials_journal_or_network() {
    let history = temp_history("cap-exceeded");
    let mut args = base_args(&history, ACKNOWLEDGEMENT);
    let cap_value = args
        .iter()
        .position(|argument| argument == "--max-notional")
        .unwrap()
        + 1;
    args[cap_value] = "10".to_owned();
    // No credentials at all: the cap must be enforced before they are read.
    let output = offline_command().args(args).output().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("exceeds --max-notional"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn mainnet_trade_credentials_are_required_and_testnet_variables_never_substitute() {
    let history = temp_history("credential-separation");
    let output = offline_command()
        .args(base_args(&history, ACKNOWLEDGEMENT))
        // Testnet credentials must never confer mainnet authority.
        .env("BINANCE_API_KEY", "fixture-testnet-key")
        .env("BINANCE_API_SECRET", "fixture-testnet-secret")
        // Read credentials must never confer trade authority either.
        .env("BINANCE_MAINNET_READ_API_KEY", "fixture-read-key")
        .env("BINANCE_MAINNET_READ_API_SECRET", "fixture-read-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_MAINNET_TRADE_API_KEY"), "{stderr}");
    assert!(!stderr.contains("fixture-testnet-secret"), "{stderr}");
    assert!(!stderr.contains("fixture-read-secret"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn testnet_lifecycle_never_reads_the_mainnet_trade_variables() {
    let history = temp_history("testnet-separation");
    let output = offline_command()
        .env(
            "CRYPTO_TRADING_BINANCE_TESTNET_SPOT_BASE_URL",
            "http://127.0.0.1:9",
        )
        .env(
            "CRYPTO_TRADING_BINANCE_TESTNET_USDM_BASE_URL",
            "http://127.0.0.1:9",
        )
        .args([
            "testnet-lifecycle",
            "--acknowledge-testnet-lifecycle",
            "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE",
            "--campaign-id",
            "binance-spot-open-001",
            "--client-order-id",
            CLIENT_ORDER_ID,
            "--history-path",
            &history.display().to_string(),
            "--side",
            "buy",
            "--quantity",
            "0.001",
            "--price",
            "49000.1",
        ])
        .env("BINANCE_MAINNET_TRADE_API_KEY", "fixture-mainnet-key")
        .env("BINANCE_MAINNET_TRADE_API_SECRET", "fixture-mainnet-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(!stderr.contains("fixture-mainnet-secret"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn latched_kill_fact_blocks_a_fresh_lifecycle_before_credentials_or_network() {
    let history = temp_history("kill-latched");
    let latch = DecisionRecord {
        timestamp: Utc::now(),
        strategy: "binance_live_lifecycle".to_owned(),
        symbol: "BTC-USDT-SPOT".to_owned(),
        decision: "live_lifecycle_kill_switch_engaged".to_owned(),
        details: json!({
            "schema_version": 1,
            "campaign_id": "some-earlier-campaign",
            "client_order_id": "9f3c807d-776f-4de4-85d0-93760a82dfcf",
            "phase": "kill_switch_engaged",
            "failure": "order_terminal_during_cancel",
            "engaged_by": "unsafe_terminal",
        }),
    };
    let mut encoded = serde_json::to_vec(&latch).unwrap();
    encoded.push(b'\n');
    std::fs::write(&history, encoded).unwrap();

    // No credentials on purpose: the latch must fail the run before any
    // credential is read and before any network activity.
    let output = offline_command()
        .args(base_args(&history, ACKNOWLEDGEMENT))
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("kill switch is latched"), "{stderr}");
    assert!(
        !stderr.contains("BINANCE_MAINNET_TRADE_API_KEY"),
        "credentials must not be consulted after the latch: {stderr}"
    );
    cleanup_history(&history);
}

#[test]
fn fresh_lifecycle_journals_planned_before_any_mutating_route() {
    let history = temp_history("transport-order");
    let (base_url, server) = stub_binance_server(
        r#"{
            "symbols": [{
                "symbol":"BTCUSDT",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000000","tickSize":"0.1"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"9000","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5}
                ]
            }]
        }"#,
    );
    let output = offline_command()
        .args(base_args(&history, ACKNOWLEDGEMENT))
        .env("BINANCE_MAINNET_TRADE_API_KEY", "fixture-key")
        .env("BINANCE_MAINNET_TRADE_API_SECRET", "fixture-secret")
        .env(MAINNET_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    // The stub fails the admission balance read, so the run must stop with a
    // durable plan, a durable failure fact, and zero mutating requests.
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("fixture-secret"), "{stderr}");

    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v3/exchangeInfo?symbol=BTCUSDT HTTP/1.1\r\n"),
        "{requests:?}"
    );
    assert!(
        requests[1].starts_with("GET /api/v3/account?"),
        "{requests:?}"
    );
    for request in &requests {
        assert!(
            request.starts_with("GET "),
            "no mutating verb may be dispatched: {request}"
        );
        assert!(!request.contains("fixture-secret"), "{request}");
    }

    let journal = std::fs::read_to_string(&history).unwrap();
    assert!(journal.contains("live_lifecycle_planned"), "{journal}");
    assert!(journal.contains("admission_read_failed"), "{journal}");
    assert!(
        !journal.contains("live_lifecycle_submit_observed"),
        "{journal}"
    );
    assert!(!journal.contains("fixture-secret"), "{journal}");
    cleanup_history(&history);
}

#[test]
fn recovery_after_a_durable_plan_queries_first_and_never_resubmits() {
    let history = temp_history("query-first-recovery");
    let client_order_id = Uuid::parse_str(CLIENT_ORDER_ID).unwrap();
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(Decimal::new(1, 3)).unwrap(),
        Price::new(Decimal::new(490_001, 1)).unwrap(),
    );
    intent.client_order_id = client_order_id;
    intent.time_in_force = TimeInForce::PostOnly;
    let planned = DecisionRecord {
        timestamp: Utc::now(),
        strategy: "binance_live_lifecycle".to_owned(),
        symbol: intent.symbol.to_string(),
        decision: "live_lifecycle_planned".to_owned(),
        details: json!({
            "schema_version": 1,
            "campaign_id": "binance-mainnet-open-001",
            "client_order_id": client_order_id,
            "phase": "planned",
            "intent": intent,
            "wire_symbol": "BTCUSDT",
            "expected_observation": "open",
            "poll_interval_ms": 2_000,
            "maximum_queries": 30,
            "max_notional": "100",
            "allow_foreign_orders": false,
        }),
    };
    let mut encoded = serde_json::to_vec(&planned).unwrap();
    encoded.push(b'\n');
    std::fs::write(&history, encoded).unwrap();

    let (base_url, server) = stub_binance_server(r"{}");
    let mut args = base_args(&history, ACKNOWLEDGEMENT);
    args.extend(["--timeout-ms".to_owned(), "1000".to_owned()]);
    let output = offline_command()
        .args(args)
        .env("BINANCE_MAINNET_TRADE_API_KEY", "fixture-key")
        .env("BINANCE_MAINNET_TRADE_API_SECRET", "fixture-secret")
        .env(MAINNET_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("fixture-secret"), "{stderr}");

    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v3/order?"),
        "recovery must be a single-order query: {requests:?}"
    );
    assert!(requests[0].contains("symbol=BTCUSDT"), "{requests:?}");
    assert!(
        requests[0].contains(&format!("origClientOrderId={client_order_id}")),
        "{requests:?}"
    );
    assert!(!requests[0].contains("exchangeInfo"), "{requests:?}");
    assert!(
        !requests[0].starts_with("POST"),
        "recovery must never resubmit: {requests:?}"
    );

    let journal = std::fs::read_to_string(&history).unwrap();
    assert!(journal.contains("live_lifecycle_resumed"), "{journal}");
    assert!(
        !journal.contains("live_lifecycle_submit_observed"),
        "{journal}"
    );
    cleanup_history(&history);
}
