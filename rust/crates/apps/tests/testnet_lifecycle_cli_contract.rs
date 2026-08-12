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

const BINANCE_TESTNET_SPOT_BASE_URL_ENV: &str = "CRYPTO_TRADING_BINANCE_TESTNET_SPOT_BASE_URL";
const BINANCE_TESTNET_USDM_BASE_URL_ENV: &str = "CRYPTO_TRADING_BINANCE_TESTNET_USDM_BASE_URL";

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

fn offline_command() -> Command {
    let mut command = Command::new(binary());
    command
        .current_dir(repo_root())
        .env(BINANCE_TESTNET_SPOT_BASE_URL_ENV, "http://127.0.0.1:9")
        .env(BINANCE_TESTNET_USDM_BASE_URL_ENV, "http://127.0.0.1:9");
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
        "crypto-trading-testnet-lifecycle-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn base_args(history: &Path, acknowledgement: &str) -> Vec<String> {
    vec![
        "testnet-lifecycle".to_owned(),
        "--acknowledge-testnet-lifecycle".to_owned(),
        acknowledgement.to_owned(),
        "--campaign-id".to_owned(),
        "binance-spot-open-001".to_owned(),
        "--client-order-id".to_owned(),
        "0f3c807d-776f-4de4-85d0-93760a82dfcf".to_owned(),
        "--history-path".to_owned(),
        history.display().to_string(),
        "--side".to_owned(),
        "buy".to_owned(),
        "--quantity".to_owned(),
        "0.001".to_owned(),
        "--price".to_owned(),
        "49000.1".to_owned(),
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
fn exact_testnet_acknowledgement_is_checked_before_credentials_or_journal_writes() {
    let history = temp_history("ack");
    let output = offline_command()
        .args(base_args(&history, "I UNDERSTAND"))
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE"),
        "{stderr}"
    );
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn missing_testnet_credentials_fail_before_creating_a_campaign() {
    let history = temp_history("credentials");
    let output = offline_command()
        .args(base_args(
            &history,
            "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE",
        ))
        .env_remove("BINANCE_API_KEY")
        .env_remove("BINANCE_API_SECRET")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_API_KEY"), "{stderr}");
    assert!(!stderr.contains("fixture-secret"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
}

#[test]
fn invalid_exchange_info_fails_closed_before_any_order_route_or_journal_mutation() {
    let history = temp_history("metadata-gate");
    let (base_url, server) = stub_binance_server(
        r#"{
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "filters": [
                        {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0"},
                        {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                        {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true}
                    ]
                }
            ]
        }"#,
    );
    let output = offline_command()
        .args(base_args(
            &history,
            "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE",
        ))
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .env(BINANCE_TESTNET_SPOT_BASE_URL_ENV, &base_url)
        .env(BINANCE_TESTNET_USDM_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("authoritative Binance testnet instrument metadata"),
        "{stderr}"
    );
    assert!(!stderr.contains("fixture-secret"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v3/exchangeInfo?symbol=BTCUSDT HTTP/1.1\r\n"),
        "{requests:?}"
    );
}

#[test]
fn valid_spot_metadata_is_enforced_by_local_preflight_before_submit() {
    let history = temp_history("spot-preflight");
    let (base_url, server) = stub_binance_server(
        r#"{
            "symbols": [{
                "symbol":"BTCUSDT",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5}
                ]
            }]
        }"#,
    );
    let output = offline_command()
        .args(base_args(
            &history,
            "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE",
        ))
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .env(BINANCE_TESTNET_SPOT_BASE_URL_ENV, &base_url)
        .env(BINANCE_TESTNET_USDM_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("local protocol validation"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v3/exchangeInfo?symbol=BTCUSDT HTTP/1.1\r\n"),
        "{requests:?}"
    );
}

#[test]
fn valid_usdm_metadata_is_loaded_from_the_futures_route_before_preflight() {
    let history = temp_history("usdm-preflight");
    let (base_url, server) = stub_binance_server(
        r#"{
            "symbols": [{
                "symbol":"BTCUSDT",
                "pair":"BTCUSDT",
                "contractType":"PERPETUAL",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","notional":"5"}
                ]
            }]
        }"#,
    );
    let mut args = base_args(&history, "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE");
    args.extend(["--market".to_owned(), "usdm".to_owned()]);
    let output = offline_command()
        .args(args)
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .env(BINANCE_TESTNET_SPOT_BASE_URL_ENV, &base_url)
        .env(BINANCE_TESTNET_USDM_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("local protocol validation"), "{stderr}");
    assert!(
        !history.exists(),
        "unexpected journal {}",
        history.display()
    );
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /fapi/v1/exchangeInfo?symbol=BTCUSDT HTTP/1.1\r\n"),
        "{requests:?}"
    );
}

#[test]
fn recovery_uses_the_durable_wire_symbol_and_skips_exchange_info() {
    let history = temp_history("durable-wire-recovery");
    let client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
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
        strategy: "binance_testnet_lifecycle".to_owned(),
        symbol: intent.symbol.to_string(),
        decision: "testnet_lifecycle_planned".to_owned(),
        details: json!({
            "schema_version": 2,
            "campaign_id": "binance-spot-open-001",
            "client_order_id": client_order_id,
            "phase": "planned",
            "intent": intent,
            "wire_symbol": "BTCUSDT",
            "expected_observation": "open",
            "poll_interval_ms": 2_000,
            "maximum_queries": 30,
        }),
    };
    let mut encoded = serde_json::to_vec(&planned).unwrap();
    encoded.push(b'\n');
    std::fs::write(&history, encoded).unwrap();

    let (base_url, server) = stub_binance_server(r"{}");
    let mut args = base_args(&history, "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE");
    args.extend([
        "--wire-symbol".to_owned(),
        "ETHUSDT".to_owned(),
        "--timeout-ms".to_owned(),
        "1000".to_owned(),
    ]);
    let output = offline_command()
        .args(args)
        .env("BINANCE_API_KEY", "fixture-key")
        .env("BINANCE_API_SECRET", "fixture-secret")
        .env(BINANCE_TESTNET_SPOT_BASE_URL_ENV, &base_url)
        .env(BINANCE_TESTNET_USDM_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!stderr.contains("fixture-secret"), "{stderr}");
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v3/order?"),
        "{requests:?}"
    );
    assert!(requests[0].contains("symbol=BTCUSDT"), "{requests:?}");
    assert!(
        requests[0].contains(&format!("origClientOrderId={client_order_id}")),
        "{requests:?}"
    );
    assert!(!requests[0].contains("exchangeInfo"), "{requests:?}");
    assert!(!requests[0].contains("ETHUSDT"), "{requests:?}");

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
