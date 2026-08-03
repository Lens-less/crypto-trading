use std::{
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    thread,
    time::Duration,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    BinancePublicExchange, ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode,
    ExchangeOperation, MarketSubscription, ReconcileScope, RemoteRetryAfter, TradingCommand,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("test timestamp must be valid")
        .with_timezone(&Utc)
}

#[test]
fn official_book_ticker_fixture_maps_to_an_exact_spot_snapshot() {
    let received_at = timestamp("2026-07-14T03:04:05Z");

    let snapshot = BinancePublicExchange::parse_book_ticker(
        include_bytes!("fixtures/binance_book_ticker.json"),
        received_at,
    )
    .unwrap();

    assert_eq!(snapshot.exchange(), "binance");
    assert_eq!(snapshot.symbol.as_str(), "LTCBTC");
    assert_eq!(snapshot.market_type, MarketType::Spot);
    assert_eq!(snapshot.bid().as_decimal(), decimal("4.00000000"));
    assert_eq!(snapshot.ask().as_decimal(), decimal("4.00000200"));
    assert_eq!(
        snapshot.bid_quantity.unwrap().as_decimal(),
        decimal("431.00000000")
    );
    assert_eq!(
        snapshot.ask_quantity.unwrap().as_decimal(),
        decimal("9.00000000")
    );
    assert_eq!(snapshot.timestamp, received_at);
}

#[test]
fn optional_book_ticker_sequence_aliases_are_parsed_without_claiming_a_venue_event_time() {
    let received_at = timestamp("2026-07-14T03:04:05Z");

    let canonical = BinancePublicExchange::parse_book_ticker_observation(
        br#"{
            "symbol":"LTCBTC",
            "bidPrice":"4.00000000",
            "bidQty":"431.00000000",
            "askPrice":"4.00000200",
            "askQty":"9.00000000",
            "updateId":123456
        }"#,
        received_at,
    )
    .unwrap();
    assert_eq!(canonical.snapshot.timestamp, received_at);
    assert_eq!(canonical.event_time, None);
    assert_eq!(canonical.source_sequence, Some(123_456));

    let alias = BinancePublicExchange::parse_book_ticker_observation(
        br#"{
            "symbol":"LTCBTC",
            "bidPrice":"4.00000000",
            "bidQty":"431.00000000",
            "askPrice":"4.00000200",
            "askQty":"9.00000000",
            "u":123457
        }"#,
        received_at,
    )
    .unwrap();
    assert_eq!(alias.source_sequence, Some(123_457));
    assert_eq!(alias.event_time, None);
}

#[tokio::test]
async fn public_adapter_refuses_every_private_or_trading_operation() {
    let exchange = BinancePublicExchange::new().unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let command = TradingCommand::Submit(OrderIntent::market(
        "binance",
        symbol,
        MarketType::Spot,
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
            .subscribe(MarketSubscription::all_snapshots(Some(MarketType::Spot)))
            .await
            .unwrap_err(),
        ExchangeError::Unsupported {
            operation: ExchangeOperation::Subscribe,
            ..
        }
    ));

    let status = exchange.status().await.unwrap();
    assert_eq!(status.exchange, "binance");
    assert_eq!(status.mode, ExchangeMode::ReadOnly);
    assert_eq!(status.availability, ExchangeAvailability::Ready);
}

#[tokio::test]
async fn one_shot_fetch_uses_only_the_public_book_ticker_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
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

        let body = include_str!("fixtures/binance_book_ticker.json");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
        String::from_utf8(request).unwrap()
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();
    let before = Utc::now();

    let snapshot = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap();
    let after = Utc::now();
    let request = server.join().unwrap();

    assert_eq!(snapshot.symbol.as_str(), "LTCBTC");
    assert!(snapshot.timestamp >= before && snapshot.timestamp <= after);
    assert!(request.starts_with("GET /api/v3/ticker/bookTicker?symbol=LTCBTC HTTP/1.1\r\n"));
    let lower_request = request.to_ascii_lowercase();
    assert!(!lower_request.contains("x-mbx-apikey"));
    assert!(!lower_request.contains("signature"));
}

#[tokio::test]
async fn non_success_binance_json_errors_preserve_http_status_and_exchange_code() {
    for (status_line, status_code, body, expected_code, expected_msg) in [
        (
            "429 Too Many Requests",
            429_u16,
            r#"{"code":-1003,"msg":"Too many requests queued."}"#,
            "-1003",
            "Too many requests queued.",
        ),
        (
            "418 I'm a teapot",
            418_u16,
            r#"{"code":-1003,"msg":"Way too much request weight used; IP banned until 1721323200000."}"#,
            "-1003",
            "Way too much request weight used; IP banned until 1721323200000.",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let body = body.to_owned();
        let status_line = status_line.to_owned();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 2_048];
            let _ = stream.read(&mut request).unwrap();
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

        let error = exchange
            .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                ExchangeError::RemoteFailure {
                    exchange,
                    status: Some(code),
                    reason,
                    ..
                } if exchange == "binance"
                    && *code == status_code
                    && reason.contains(expected_code)
                    && reason.contains(expected_msg)
            ),
            "unexpected error: {error:?}"
        );
        server.join().unwrap();
    }
}

#[tokio::test]
async fn binance_rate_limit_preserves_retry_after_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let body = r#"{"code":-1003,"msg":"too many requests"}"#;
        let response = format!(
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 7\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    let metadata = error.remote_failure_metadata().unwrap();
    assert_eq!(metadata.exchange_code.as_deref(), Some("-1003"));
    assert_eq!(metadata.retry_after, Some(RemoteRetryAfter::Seconds(7)));
    server.join().unwrap();
}

#[tokio::test]
async fn non_json_error_bodies_fall_back_to_a_bounded_text_reason() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let body = "<html><body><h1>temporarily unavailable</h1></body></html>";
        let response = format!(
            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::RemoteFailure {
                exchange,
                status: Some(502),
                reason,
                ..
            } if exchange == "binance"
                && reason.contains("temporarily unavailable")
                && reason.len() < 512
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}

#[tokio::test]
async fn oversized_binance_json_error_msg_is_truncated_safely() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let msg = "\u{4E2D}".repeat(200);
        let body = format!(r#"{{"code":-1003,"msg":"{msg}"}}"#);
        let response = format!(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::RemoteFailure {
                exchange,
                status: Some(429),
                reason,
                ..
            } if exchange == "binance"
                && reason.contains("-1003")
                && reason.ends_with("...")
                && !reason.contains(char::REPLACEMENT_CHARACTER)
                && reason.len() <= 256
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}

#[tokio::test]
async fn non_json_unicode_body_truncation_does_not_emit_replacement_characters() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let mut body = vec![b'a'; 255];
        body.extend_from_slice("\u{4E2D}".as_bytes());
        body.extend_from_slice(b" tail");
        let header = format!(
            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(header.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::RemoteFailure {
                exchange,
                status: Some(502),
                reason,
                ..
            } if exchange == "binance"
                && reason.starts_with("Bad Gateway: ")
                && reason.ends_with("...")
                && !reason.contains(char::REPLACEMENT_CHARACTER)
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
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 10000000\r\nConnection: close\r\n\r\n";
        stream.write_all(response).unwrap();
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::ResourceLimit {
                resource: "Binance response body",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}

#[tokio::test]
async fn oversized_chunked_error_body_is_stopped_at_the_streaming_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let body = vec![b'x'; 1_048_577];
        let header = format!(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&body);
        let _ = stream.write_all(b"\r\n0\r\n\r\n");
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::ResourceLimit {
                resource: "Binance response body",
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
        let mut request = [0_u8; 2_048];
        let _ = stream.read(&mut request).unwrap();
        let body = vec![b'x'; 1_048_577];
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:X}\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&body);
        let _ = stream.write_all(b"\r\n0\r\n\r\n");
    });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();

    let error = exchange
        .fetch_snapshot(&Symbol::new("LTCBTC").unwrap())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            ExchangeError::ResourceLimit {
                resource: "Binance response body",
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
    server.join().unwrap();
}
