use chrono::{DateTime, TimeZone, Utc};
use crypto_trading_domain::{
    MarketType, OrderStatus, OrderType, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    ExchangeError, ForeignOrder, ReconcileReceipt, ReconcileScope, RemoteFailureMetadata,
    RemoteHttpResponse, RemoteRetryAfter,
};
use rust_decimal::Decimal;

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc2822(value)
        .expect("test timestamp must be valid")
        .with_timezone(&Utc)
}

#[test]
fn remote_http_response_preserves_headers_and_parses_backoff_metadata() {
    let response = RemoteHttpResponse::new_with_headers(
        429,
        vec![
            ("Retry-After".to_owned(), "120".to_owned()),
            (
                "Date".to_owned(),
                "Tue, 15 Nov 1994 08:12:31 GMT".to_owned(),
            ),
            ("X-MBX-USED-WEIGHT".to_owned(), "42".to_owned()),
        ],
        br#"{"code":-1003,"msg":"Too many requests"}"#.to_vec(),
    )
    .unwrap();

    assert_eq!(response.header("retry-after"), Some("120"));
    assert_eq!(response.header("x-mbx-used-weight"), Some("42"));
    assert_eq!(response.headers().len(), 3);
    assert!(matches!(
        response.retry_after(),
        Some(RemoteRetryAfter::Seconds(120))
    ));
    assert_eq!(
        response.server_time(),
        Some(timestamp("Tue, 15 Nov 1994 08:12:31 GMT"))
    );
}

#[test]
fn remote_http_response_discards_unrelated_headers_and_bounds_metadata() {
    let response = RemoteHttpResponse::new_with_headers(
        200,
        vec![
            ("Set-Cookie".to_owned(), "secret=must-not-retain".to_owned()),
            ("X-MBX-USED-WEIGHT-1M".to_owned(), "7".to_owned()),
        ],
        Vec::<u8>::new(),
    )
    .unwrap();
    assert_eq!(response.headers().len(), 1);
    assert_eq!(response.header("set-cookie"), None);
    assert_eq!(response.header("x-mbx-used-weight-1m"), Some("7"));

    let error = RemoteHttpResponse::new_with_headers(
        429,
        vec![("Retry-After".to_owned(), "x".repeat(513))],
        Vec::<u8>::new(),
    )
    .unwrap_err();
    assert!(matches!(error, ExchangeError::ResourceLimit { .. }));
}

#[test]
fn remote_failure_metadata_merges_header_and_exchange_code_context() {
    let response = RemoteHttpResponse::new_with_headers(
        429,
        vec![
            ("Retry-After".to_owned(), "120".to_owned()),
            (
                "Date".to_owned(),
                "Tue, 15 Nov 1994 08:12:31 GMT".to_owned(),
            ),
        ],
        Vec::<u8>::new(),
    )
    .unwrap();
    let error = ExchangeError::RemoteFailure {
        exchange: "binance".to_owned(),
        status: Some(429),
        reason: "rate limited".to_owned(),
        metadata: RemoteFailureMetadata::default(),
    }
    .with_remote_metadata(response.remote_failure_metadata())
    .with_exchange_code("-1003");

    let metadata = error.remote_failure_metadata().unwrap();
    assert_eq!(metadata.exchange_code.as_deref(), Some("-1003"));
    assert!(matches!(
        metadata.retry_after,
        Some(RemoteRetryAfter::Seconds(120))
    ));
    assert_eq!(
        metadata.server_time,
        Some(timestamp("Tue, 15 Nov 1994 08:12:31 GMT"))
    );
}

#[test]
fn reconcile_receipt_round_trip_preserves_foreign_manual_orders() {
    let observed_at = Utc.with_ymd_and_hms(2026, 7, 25, 8, 9, 10).unwrap();
    let receipt = ReconcileReceipt {
        scope: ReconcileScope::All,
        orders: Vec::new(),
        foreign_orders: vec![ForeignOrder {
            id: "binance:spot:BTCUSDT:42".to_owned(),
            client_order_id: Some("manual".to_owned()),
            exchange: "binance".to_owned(),
            symbol: Symbol::new("BTC-USDC-SPOT").unwrap(),
            market_type: MarketType::Spot,
            side: Side::Buy,
            order_type: OrderType::Limit,
            quantity: Quantity::new(Decimal::ONE).unwrap(),
            price: Some(Price::new(Decimal::new(50_000, 0)).unwrap()),
            reduce_only: false,
            time_in_force: TimeInForce::Gtc,
            filled_quantity: Quantity::default(),
            average_fill_price: None,
            status: OrderStatus::Open,
            created_at: observed_at,
            updated_at: observed_at,
        }],
        positions: Vec::new(),
        observed_at,
    };

    let value = serde_json::to_value(&receipt).unwrap();
    assert_eq!(value["foreign_orders"][0]["client_order_id"], "manual");

    let round_trip: ReconcileReceipt = serde_json::from_value(value).unwrap();
    assert_eq!(round_trip, receipt);
}
