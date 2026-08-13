//! Offline contract for the read-only Binance Spot MAINNET reconcile command.
//!
//! `live-reconcile` constructs only the read-authority adapter type, which has
//! no submit or cancel surface at the compile-time level. These tests pin the
//! runtime posture: dedicated read credentials, no mutating verbs on the wire,
//! and a redacted report that never echoes a secret.

use std::{
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::Duration,
};

const MAINNET_BASE_URL_ENV: &str = "CRYPTO_TRADING_BINANCE_MAINNET_SPOT_BASE_URL";

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_crypto-trading")
}

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

const ACCOUNT_BODY: &str = r#"{"balances":[
    {"asset":"BTC","free":"0.5","locked":"0"},
    {"asset":"USDT","free":"1000.25","locked":"10"}
]}"#;
const OPEN_ORDERS_BODY: &str = "[]";

/// Stub that answers each request from a fixed script and captures every
/// request line; requests beyond the script get a 500.
fn scripted_stub_server(responses: Vec<&'static str>) -> (String, thread::JoinHandle<Vec<String>>) {
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
                    let (status, body) = responses.get(captured.len() - 1).map_or(
                        (
                            "500 Internal Server Error",
                            r#"{"code":500,"msg":"unexpected extra request"}"#,
                        ),
                        |body| ("200 OK", *body),
                    );
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
fn read_credentials_are_required_and_no_other_variables_substitute() {
    let output = offline_command()
        .args(["live-reconcile"])
        // Testnet and mainnet-trade credentials must never confer read
        // authority for the mainnet report.
        .env("BINANCE_API_KEY", "fixture-testnet-key")
        .env("BINANCE_API_SECRET", "fixture-testnet-secret")
        .env("BINANCE_MAINNET_TRADE_API_KEY", "fixture-trade-key")
        .env("BINANCE_MAINNET_TRADE_API_SECRET", "fixture-trade-secret")
        .output()
        .unwrap();

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("BINANCE_MAINNET_READ_API_KEY"), "{stderr}");
    assert!(!stderr.contains("fixture-testnet-secret"), "{stderr}");
    assert!(!stderr.contains("fixture-trade-secret"), "{stderr}");
}

#[test]
fn report_uses_only_signed_get_routes_and_never_echoes_a_secret() {
    // account + openOrders, sampled twice by the double-sample invariant.
    let (base_url, server) = scripted_stub_server(vec![
        ACCOUNT_BODY,
        OPEN_ORDERS_BODY,
        ACCOUNT_BODY,
        OPEN_ORDERS_BODY,
    ]);
    let output = offline_command()
        .args(["live-reconcile", "--json"])
        .env("BINANCE_MAINNET_READ_API_KEY", "fixture-read-key")
        .env("BINANCE_MAINNET_READ_API_SECRET", "fixture-read-secret")
        .env(MAINNET_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["authority"], "mainnet-read", "{stdout}");
    assert_eq!(report["mutation_authority"], false, "{stdout}");
    assert_eq!(report["balance_count"], 2, "{stdout}");
    assert_eq!(report["owned_order_count"], 0, "{stdout}");
    assert_eq!(report["foreign_order_count"], 0, "{stdout}");
    assert_eq!(
        report["nonzero_balances"][1]["wallet"], "1010.25",
        "{stdout}"
    );
    assert!(!stdout.contains("fixture-read-secret"), "{stdout}");
    assert!(!stdout.contains("fixture-read-key"), "{stdout}");

    assert_eq!(requests.len(), 4, "{requests:?}");
    for request in &requests {
        assert!(
            request.starts_with("GET /api/v3/account?")
                || request.starts_with("GET /api/v3/openOrders?"),
            "only signed read routes may be dispatched: {request}"
        );
        assert!(!request.contains("fixture-read-secret"), "{request}");
    }
}

#[test]
fn optional_exchange_info_is_still_a_read_only_route() {
    let (base_url, server) = scripted_stub_server(vec![
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
        ACCOUNT_BODY,
        OPEN_ORDERS_BODY,
        ACCOUNT_BODY,
        OPEN_ORDERS_BODY,
    ]);
    let output = offline_command()
        .args(["live-reconcile", "--json", "--include-exchange-info"])
        .env("BINANCE_MAINNET_READ_API_KEY", "fixture-read-key")
        .env("BINANCE_MAINNET_READ_API_SECRET", "fixture-read-secret")
        .env(MAINNET_BASE_URL_ENV, &base_url)
        .output()
        .unwrap();
    let requests = server.join().unwrap();

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["exchange_info"]["price_tick"], "0.1", "{stdout}");
    assert_eq!(report["exchange_info"]["min_notional"], "5", "{stdout}");

    assert_eq!(requests.len(), 5, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v3/exchangeInfo?symbol=BTCUSDT HTTP/1.1\r\n"),
        "{requests:?}"
    );
    for request in &requests {
        assert!(
            request.starts_with("GET "),
            "no mutating verb may be dispatched: {request}"
        );
    }
}
