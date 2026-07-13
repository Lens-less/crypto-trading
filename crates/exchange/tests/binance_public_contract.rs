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
    ExchangeOperation, MarketSubscription, ReconcileScope, TradingCommand,
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
