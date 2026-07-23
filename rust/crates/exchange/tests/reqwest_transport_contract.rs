use std::{
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    sync::Arc,
    thread,
    time::Duration,
};

use crypto_trading_domain::{MarketType, Money, Price, Quantity, Symbol};
use crypto_trading_exchange::{
    ExchangeError, HyperliquidAction, HyperliquidAsset, HyperliquidAssetCatalog,
    HyperliquidRequestSigner, HyperliquidSignature, HyperliquidTestnetEndpoint,
    HyperliquidTestnetProtocol, InstrumentRuleCatalog, InstrumentRules, RemoteHttpTransport,
    ReqwestHttpTransport,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

struct Signer;

impl HyperliquidRequestSigner for Signer {
    fn account_address(&self) -> &'static str {
        "0x1111111111111111111111111111111111111111"
    }

    fn sign(
        &self,
        _action: &HyperliquidAction,
        _nonce: u64,
        _vault_address: Option<&str>,
    ) -> Result<HyperliquidSignature, ExchangeError> {
        unreachable!("info requests are unsigned")
    }
}

fn protocol(base_url: &str) -> HyperliquidTestnetProtocol {
    let symbol = Symbol::new("BTC-USDC-PERP").unwrap();
    HyperliquidTestnetProtocol::authenticated(
        HyperliquidTestnetEndpoint::loopback(base_url).unwrap(),
        HyperliquidAssetCatalog::new(vec![
            HyperliquidAsset::new(symbol.clone(), MarketType::Perpetual, 0, "BTC").unwrap(),
        ])
        .unwrap(),
        InstrumentRuleCatalog::new(vec![
            InstrumentRules::new(
                "hyperliquid",
                symbol,
                MarketType::Perpetual,
                Price::new(decimal("0.1")).unwrap(),
                Quantity::new(decimal("0.001")).unwrap(),
                Quantity::new(decimal("0.001")).unwrap(),
                Money::new(decimal("5")),
            )
            .unwrap(),
        ])
        .unwrap(),
        Arc::new(Signer),
        None,
    )
    .unwrap()
}

fn read_http_request(stream: &mut std::net::TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 2_048];
    loop {
        let read = stream.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        let Some(headers_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers_end = headers_end + 4;
        let headers = String::from_utf8_lossy(&request[..headers_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
            })
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        if request.len() >= headers_end + content_length {
            break;
        }
    }
    request
}

#[tokio::test]
async fn reqwest_transport_executes_a_loopback_protocol_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_http_request(&mut stream);
        let body = br#"{"status":"ok"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        request
    });
    let request = protocol(&base_url).build_open_orders_request().unwrap();
    let transport = ReqwestHttpTransport::new(Duration::from_secs(5)).unwrap();

    let response = transport.send(request).await.unwrap();
    let captured = String::from_utf8(server.join().unwrap()).unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.body(), br#"{"status":"ok"}"#);
    assert!(captured.starts_with("POST /info HTTP/1.1\r\n"));
    assert!(
        captured
            .to_ascii_lowercase()
            .contains("content-type: application/json")
    );
    assert!(captured.contains("0x1111111111111111111111111111111111111111"));
}

#[tokio::test]
async fn reqwest_transport_rejects_oversized_content_length_before_body_allocation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_http_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\nConnection: close\r\n\r\n")
            .unwrap();
    });
    let request = protocol(&base_url).build_open_orders_request().unwrap();
    let transport = ReqwestHttpTransport::new(Duration::from_secs(5)).unwrap();

    let error = transport.send(request).await.unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::ResourceLimit {
            resource: "remote HTTP response body",
            ..
        }
    ));
    server.join().unwrap();
}
