use std::{
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    thread,
    time::Duration,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ExchangeOperation,
    HyperliquidFundingRate, HyperliquidPublicEndpoint, HyperliquidPublicExchange,
    MarketSubscription, ReconcileScope, TradingCommand, hyperliquid_usdt_symbol_catalog,
};
use rust_decimal::Decimal;

const FIXTURE: &[u8] = include_bytes!("fixtures/hyperliquid_meta_and_asset_ctxs.json");

/// Reads one full credential-free info request (headers plus JSON body) so the
/// stub never responds while the client is still writing.
fn read_full_info_request(stream: &mut std::net::TcpStream) -> Vec<u8> {
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
        if request.ends_with(br#"{"type":"metaAndAssetCtxs"}"#) {
            break;
        }
    }
    request
}

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("test timestamp must be valid")
        .with_timezone(&Utc)
}

#[test]
fn documented_asset_context_fixture_maps_to_an_exact_perpetual_observation() {
    let received_at = timestamp("2026-07-26T03:04:05Z");

    let observation =
        HyperliquidPublicExchange::parse_meta_and_asset_ctxs(FIXTURE, "BTC", received_at).unwrap();

    assert_eq!(observation.snapshot.exchange(), "hyperliquid");
    assert_eq!(observation.snapshot.symbol.as_str(), "BTC");
    assert_eq!(observation.snapshot.market_type, MarketType::Perpetual);
    assert_eq!(observation.snapshot.bid().as_decimal(), decimal("104559.0"));
    assert_eq!(observation.snapshot.ask().as_decimal(), decimal("104562.0"));
    assert_eq!(
        observation.snapshot.last.map(Price::as_decimal),
        Some(decimal("104560.5"))
    );
    assert_eq!(observation.snapshot.timestamp, received_at);
    assert_eq!(
        observation.funding.map(HyperliquidFundingRate::as_decimal),
        Some(decimal("0.0000125"))
    );

    let negative =
        HyperliquidPublicExchange::parse_meta_and_asset_ctxs(FIXTURE, "ETH", received_at).unwrap();
    assert_eq!(
        negative.funding.map(HyperliquidFundingRate::as_decimal),
        Some(decimal("-0.0000210"))
    );
}

#[test]
fn missing_funding_degrades_to_none_instead_of_failing_the_snapshot() {
    let received_at = timestamp("2026-07-26T03:04:05Z");

    let observation =
        HyperliquidPublicExchange::parse_meta_and_asset_ctxs(FIXTURE, "THIN", received_at).unwrap();

    assert_eq!(observation.snapshot.bid().as_decimal(), decimal("4.9"));
    assert_eq!(observation.snapshot.ask().as_decimal(), decimal("5.1"));
    assert_eq!(observation.snapshot.last, None);
    assert_eq!(observation.funding, None);
}

#[test]
fn malformed_or_ambiguous_payloads_fail_closed() {
    let received_at = timestamp("2026-07-26T03:04:05Z");
    let invalid = |payload: &str, coin: &str| {
        let error = HyperliquidPublicExchange::parse_meta_and_asset_ctxs(
            payload.as_bytes(),
            coin,
            received_at,
        )
        .unwrap_err();
        assert!(
            matches!(error, ExchangeError::InvalidResponse { ref exchange, .. } if exchange == "hyperliquid"),
            "unexpected error: {error:?}"
        );
    };

    // Not the documented two-element [meta, assetCtxs] envelope.
    invalid(r#"{"universe":[]}"#, "BTC");
    invalid(r#"[{"universe":[]}]"#, "BTC");
    invalid(r#"[{"universe":[]},[],[]]"#, "BTC");
    // Universe/context arity mismatch.
    invalid(r#"[{"universe":[{"name":"BTC"}]},[]]"#, "BTC");
    // Unknown coin.
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"impactPxs":["1.0","2.0"]}]]"#,
        "ETH",
    );
    // Duplicated coin is ambiguous.
    invalid(
        r#"[{"universe":[{"name":"BTC"},{"name":"BTC"}]},[{"impactPxs":["1.0","2.0"]},{"impactPxs":["1.0","2.0"]}]]"#,
        "BTC",
    );
    // Delisted coins are refused rather than quoted.
    let delisted =
        HyperliquidPublicExchange::parse_meta_and_asset_ctxs(FIXTURE, "OLDCOIN", received_at)
            .unwrap_err();
    assert!(matches!(
        delisted,
        ExchangeError::InvalidResponse { ref reason, .. } if reason.contains("delisted")
    ));
    // Missing, mis-shaped, crossed, or non-positive impact prices.
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"impactPxs":null}]]"#,
        "BTC",
    );
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"impactPxs":["1.0"]}]]"#,
        "BTC",
    );
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"impactPxs":["2.0","1.0"]}]]"#,
        "BTC",
    );
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"impactPxs":["0","1.0"]}]]"#,
        "BTC",
    );
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"impactPxs":["broken","1.0"]}]]"#,
        "BTC",
    );
    // Implausible or malformed funding values.
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"funding":"2","impactPxs":["1.0","2.0"]}]]"#,
        "BTC",
    );
    invalid(
        r#"[{"universe":[{"name":"BTC"}]},[{"funding":"abc","impactPxs":["1.0","2.0"]}]]"#,
        "BTC",
    );
}

#[test]
fn oversized_universe_hits_the_context_resource_guard() {
    let received_at = timestamp("2026-07-26T03:04:05Z");
    let mut universe = Vec::new();
    let mut contexts = Vec::new();
    for index in 0..10_001 {
        universe.push(format!(r#"{{"name":"C{index}"}}"#));
        contexts.push(r#"{"impactPxs":["1.0","2.0"]}"#.to_owned());
    }
    let payload = format!(
        r#"[{{"universe":[{}]}},[{}]]"#,
        universe.join(","),
        contexts.join(",")
    );

    let error =
        HyperliquidPublicExchange::parse_meta_and_asset_ctxs(payload.as_bytes(), "C0", received_at)
            .unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::ResourceLimit {
            resource: "Hyperliquid asset contexts",
            ..
        }
    ));
}

#[test]
fn endpoint_whitelist_admits_only_official_mainnet_or_literal_loopback() {
    HyperliquidPublicEndpoint::try_official("https://api.hyperliquid.xyz").unwrap();
    for rejected in [
        "http://api.hyperliquid.xyz",
        "https://api.hyperliquid-testnet.xyz",
        "https://api.hyperliquid.xyz:8443",
        "https://api.hyperliquid.xyz/info",
        "https://api.hyperliquid.xyz/?x=1",
        "https://evil.example.com",
    ] {
        assert!(
            HyperliquidPublicEndpoint::try_official(rejected).is_err(),
            "{rejected} must be rejected"
        );
    }

    HyperliquidPublicEndpoint::loopback("http://127.0.0.1:8080").unwrap();
    HyperliquidPublicEndpoint::loopback("http://[::1]:8080").unwrap();
    for rejected in [
        "http://localhost:8080",
        "http://192.168.1.10:8080",
        "http://127.0.0.1:8080/info",
        "ftp://127.0.0.1:8080",
    ] {
        assert!(
            HyperliquidPublicEndpoint::loopback(rejected).is_err(),
            "{rejected} must be rejected"
        );
    }
}

#[test]
fn usdt_symbol_catalog_is_exact_bounded_and_bidirectional() {
    let catalog = hyperliquid_usdt_symbol_catalog(&["BTC", "ETH", "kPEPE"]).unwrap();

    assert_eq!(catalog.len(), 3);
    assert_eq!(
        catalog
            .to_wire(
                "hyperliquid",
                &Symbol::new("BTCUSDT").unwrap(),
                MarketType::Perpetual
            )
            .unwrap(),
        "BTC"
    );
    assert_eq!(
        catalog
            .to_standard("hyperliquid", "kPEPE", MarketType::Perpetual)
            .unwrap()
            .as_str(),
        "kPEPEUSDT"
    );
    assert!(
        catalog
            .to_wire(
                "hyperliquid",
                &Symbol::new("BTCUSDT").unwrap(),
                MarketType::Spot
            )
            .is_err()
    );
    assert!(
        catalog
            .to_standard("hyperliquid", "SOL", MarketType::Perpetual)
            .is_err()
    );

    assert!(hyperliquid_usdt_symbol_catalog(&[]).is_err());
    assert!(hyperliquid_usdt_symbol_catalog(&["BTC", "BTC"]).is_err());
    assert!(hyperliquid_usdt_symbol_catalog(&["@1"]).is_err());
    assert!(hyperliquid_usdt_symbol_catalog(&[""]).is_err());
    let oversized = vec!["BTC"; 1_025];
    assert!(hyperliquid_usdt_symbol_catalog(&oversized).is_err());
}

#[tokio::test]
async fn public_adapter_refuses_every_private_or_trading_operation() {
    let exchange = HyperliquidPublicExchange::new().unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let command = TradingCommand::Submit(OrderIntent::market(
        "hyperliquid",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
    ));

    assert!(matches!(
        exchange.execute(command).await.unwrap_err(),
        ExchangeError::Unsupported {
            operation: ExchangeOperation::SubmitOrder,
            ..
        }
    ));
    assert!(matches!(
        exchange.reconcile(ReconcileScope::All).await.unwrap_err(),
        ExchangeError::Unsupported {
            operation: ExchangeOperation::Reconcile,
            ..
        }
    ));
    assert!(matches!(
        exchange
            .subscribe(MarketSubscription::all_snapshots(Some(
                MarketType::Perpetual
            )))
            .await
            .unwrap_err(),
        ExchangeError::Unsupported {
            operation: ExchangeOperation::Subscribe,
            ..
        }
    ));

    let status = exchange.status().await.unwrap();
    assert_eq!(status.exchange, "hyperliquid");
    assert_eq!(status.mode, ExchangeMode::ReadOnly);
    assert_eq!(status.availability, ExchangeAvailability::Ready);
}

#[tokio::test]
async fn one_shot_fetch_posts_only_the_credential_free_info_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
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
            if request.ends_with(br#"{"type":"metaAndAssetCtxs"}"#) {
                break;
            }
        }

        let body = include_str!("fixtures/hyperliquid_meta_and_asset_ctxs.json");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
        String::from_utf8(request).unwrap()
    });
    let endpoint = HyperliquidPublicEndpoint::loopback(&base_url).unwrap();
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint).unwrap();
    let before = Utc::now();

    let observation = exchange.fetch_observation("BTC").await.unwrap();
    let after = Utc::now();
    let request = server.join().unwrap();

    assert_eq!(observation.snapshot.symbol.as_str(), "BTC");
    assert!(observation.snapshot.timestamp >= before && observation.snapshot.timestamp <= after);
    assert_eq!(
        observation.funding.map(HyperliquidFundingRate::as_decimal),
        Some(decimal("0.0000125"))
    );
    assert!(request.starts_with("POST /info HTTP/1.1\r\n"));
    assert!(request.ends_with(r#"{"type":"metaAndAssetCtxs"}"#));
    let lower_request = request.to_ascii_lowercase();
    assert!(!lower_request.contains("signature"));
    assert!(!lower_request.contains("authorization"));
    assert!(!lower_request.contains("api-key"));
}

#[tokio::test]
async fn non_success_bodies_become_bounded_remote_failures() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_full_info_request(&mut stream);
        let body = "rate limited ".repeat(100);
        let response = format!(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    let endpoint = HyperliquidPublicEndpoint::loopback(&base_url).unwrap();
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint).unwrap();

    let error = exchange.fetch_observation("BTC").await.unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::RemoteFailure {
                exchange,
                status: Some(429),
                reason,
                ..
            } if exchange == "hyperliquid"
                && reason.contains("rate limited")
                && reason.len() <= 256
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}

#[tokio::test]
async fn oversized_content_length_is_rejected_before_reading_the_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_full_info_request(&mut stream);
        let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 10000000\r\nConnection: close\r\n\r\n";
        stream.write_all(response).unwrap();
    });
    let endpoint = HyperliquidPublicEndpoint::loopback(&base_url).unwrap();
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint).unwrap();

    let error = exchange.fetch_observation("BTC").await.unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::ResourceLimit {
                resource: "Hyperliquid response body",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}

#[tokio::test]
async fn oversized_chunked_body_is_stopped_at_the_streaming_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_full_info_request(&mut stream);
        let body = vec![b'x'; 1_048_577];
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&body);
        let _ = stream.write_all(b"\r\n0\r\n\r\n");
    });
    let endpoint = HyperliquidPublicEndpoint::loopback(&base_url).unwrap();
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint).unwrap();

    let error = exchange.fetch_observation("BTC").await.unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::ResourceLimit {
                resource: "Hyperliquid response body",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}
