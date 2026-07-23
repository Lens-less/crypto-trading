use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use chrono::{TimeZone, Utc};
use crypto_trading_domain::{
    MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceProduct, BinanceRequestSigner, BinanceTestnetEndpoints, BinanceTestnetProtocol,
    ExchangeError, ExchangeSymbol, ExchangeSymbolCatalog, InstrumentRuleCatalog, InstrumentRules,
    RemoteHttpMethod,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).unwrap()
}

#[derive(Debug)]
struct CapturingSigner {
    payloads: Mutex<Vec<String>>,
}

impl CapturingSigner {
    fn new() -> Self {
        Self {
            payloads: Mutex::new(Vec::new()),
        }
    }
}

impl BinanceRequestSigner for CapturingSigner {
    fn api_key(&self) -> &'static str {
        "offline-api-key"
    }

    fn sign(&self, payload: &str) -> Result<String, ExchangeError> {
        self.payloads.lock().unwrap().push(payload.to_owned());
        Ok("offline-signature/+".to_owned())
    }
}

fn protocol(signer: Arc<CapturingSigner>) -> BinanceTestnetProtocol {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();
    let symbols = ExchangeSymbolCatalog::new(vec![
        ExchangeSymbol::new("binance", spot.clone(), MarketType::Spot, "BTCUSDT").unwrap(),
        ExchangeSymbol::new(
            "binance",
            perpetual.clone(),
            MarketType::Perpetual,
            "BTCUSDT",
        )
        .unwrap(),
    ])
    .unwrap();
    let rules = InstrumentRuleCatalog::new(vec![
        InstrumentRules::new(
            "binance",
            spot,
            MarketType::Spot,
            price("0.10"),
            quantity("0.0001"),
            quantity("0.0001"),
            Money::new(decimal("5")),
        )
        .unwrap(),
        InstrumentRules::new(
            "binance",
            perpetual,
            MarketType::Perpetual,
            price("0.10"),
            quantity("0.001"),
            quantity("0.001"),
            Money::new(decimal("5")),
        )
        .unwrap(),
    ])
    .unwrap();

    BinanceTestnetProtocol::authenticated(
        BinanceTestnetEndpoints::official(),
        symbols,
        rules,
        signer,
    )
    .unwrap()
}

#[test]
fn spot_limit_order_uses_spot_route_and_signs_the_exact_query() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.10"),
    );
    intent.client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();

    let request = protocol
        .build_order_request(&intent, Some(price("50000.10")), 1_722_000_000_123)
        .unwrap();

    let expected_payload = concat!(
        "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=0.0010&price=50000.10",
        "&timeInForce=GTC&newClientOrderId=0f3c807d-776f-4de4-85d0-93760a82dfcf",
        "&recvWindow=5000&timestamp=1722000000123"
    );
    assert_eq!(request.method(), RemoteHttpMethod::Post);
    assert_eq!(request.url().path(), "/api/v3/order");
    assert_eq!(
        request.url().query().unwrap(),
        format!("{expected_payload}&signature=offline-signature%2F%2B")
    );
    assert_eq!(request.header("X-MBX-APIKEY"), Some("offline-api-key"));
    assert!(request.body().is_empty());
    assert_eq!(
        signer.payloads.lock().unwrap().as_slice(),
        &[expected_payload]
    );
}

#[test]
fn usdm_post_only_reduce_order_uses_futures_semantics() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Sell,
        quantity("0.002"),
        price("50000.20"),
    );
    intent.client_order_id = Uuid::parse_str("feac48e2-9ea4-47f8-8e18-c31285714142").unwrap();
    intent.time_in_force = TimeInForce::PostOnly;
    intent.reduce_only = true;

    let request = protocol
        .build_order_request(&intent, Some(price("50000.20")), 1_722_000_000_456)
        .unwrap();

    let expected_payload = concat!(
        "symbol=BTCUSDT&side=SELL&type=LIMIT&quantity=0.002&price=50000.20",
        "&timeInForce=GTX&reduceOnly=true",
        "&newClientOrderId=feac48e2-9ea4-47f8-8e18-c31285714142",
        "&recvWindow=5000&timestamp=1722000000456"
    );
    assert_eq!(request.method(), RemoteHttpMethod::Post);
    assert_eq!(request.url().path(), "/fapi/v1/order");
    assert_eq!(
        request.url().query().unwrap(),
        format!("{expected_payload}&signature=offline-signature%2F%2B")
    );
    assert_eq!(
        signer.payloads.lock().unwrap().as_slice(),
        &[expected_payload]
    );
}

#[test]
fn request_debug_output_redacts_credentials_signatures_and_parameter_values() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
    );

    let request = protocol
        .build_order_request(&intent, Some(price("50000")), 1_722_000_000_789)
        .unwrap();
    let diagnostic = format!("{request:?}");

    assert!(diagnostic.contains("/api/v3/order"));
    assert!(diagnostic.contains("X-MBX-APIKEY"));
    for secret in [
        "offline-api-key",
        "offline-signature",
        "BTCUSDT",
        "50000",
        "1722000000789",
    ] {
        assert!(
            !diagnostic.contains(secret),
            "{secret:?} leaked through request Debug"
        );
    }
}

#[test]
fn invalid_product_semantics_and_missing_rules_fail_before_signing() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));

    let mut spot_reduce_only = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Sell,
        quantity("0.0010"),
    );
    spot_reduce_only.reduce_only = true;
    assert!(matches!(
        protocol.build_order_request(&spot_reduce_only, Some(price("50000")), 1_722_000_001_000,),
        Err(ExchangeError::InvalidRequest { .. })
    ));

    let unknown = OrderIntent::market(
        "binance",
        Symbol::new("ETH-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.01"),
    );
    assert!(
        protocol
            .build_order_request(&unknown, Some(price("3000")), 1_722_000_001_001)
            .is_err()
    );

    let wrong_exchange = OrderIntent::market(
        "hyperliquid",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.001"),
    );
    assert!(
        protocol
            .build_order_request(&wrong_exchange, Some(price("50000")), 1_722_000_001_002)
            .is_err()
    );

    assert!(signer.payloads.lock().unwrap().is_empty());
}

#[test]
fn one_book_ticker_shape_maps_to_product_specific_domain_instruments() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);
    let received_at = Utc.with_ymd_and_hms(2026, 7, 23, 1, 2, 3).unwrap();
    let body = br#"{
        "symbol":"BTCUSDT",
        "bidPrice":"50000.10",
        "bidQty":"1.25",
        "askPrice":"50000.20",
        "askQty":"2.50"
    }"#;

    let spot = protocol
        .parse_book_ticker(BinanceProduct::Spot, body, received_at)
        .unwrap();
    let perpetual = protocol
        .parse_book_ticker(BinanceProduct::UsdM, body, received_at)
        .unwrap();

    assert_eq!(spot.symbol.as_str(), "BTC-USDC-SPOT");
    assert_eq!(spot.market_type, MarketType::Spot);
    assert_eq!(perpetual.symbol.as_str(), "BTC-USDC-PERP");
    assert_eq!(perpetual.market_type, MarketType::Perpetual);
    assert_eq!(spot.bid().as_decimal(), decimal("50000.10"));
    assert_eq!(spot.ask().as_decimal(), decimal("50000.20"));
    assert_eq!(spot.bid_quantity.unwrap().as_decimal(), decimal("1.25"));
    assert_eq!(spot.ask_quantity.unwrap().as_decimal(), decimal("2.50"));
}

#[test]
fn cancellation_and_reconciliation_routes_remain_product_specific() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();

    let spot_cancel = protocol
        .build_cancel_request(&spot, MarketType::Spot, 42, 1_722_000_002_000)
        .unwrap();
    assert_eq!(spot_cancel.method(), RemoteHttpMethod::Delete);
    assert_eq!(spot_cancel.url().path(), "/api/v3/order");
    assert!(
        spot_cancel
            .url()
            .query()
            .unwrap()
            .starts_with("symbol=BTCUSDT&orderId=42&recvWindow=5000&timestamp=1722000002000")
    );

    let futures_cancel_all = protocol
        .build_cancel_all_request(&perpetual, MarketType::Perpetual, 1_722_000_002_001)
        .unwrap();
    assert_eq!(futures_cancel_all.method(), RemoteHttpMethod::Delete);
    assert_eq!(futures_cancel_all.url().path(), "/fapi/v1/allOpenOrders");

    let spot_orders = protocol
        .build_open_orders_request(MarketType::Spot, None, 1_722_000_002_002)
        .unwrap();
    assert_eq!(spot_orders.method(), RemoteHttpMethod::Get);
    assert_eq!(spot_orders.url().path(), "/api/v3/openOrders");
    assert!(!spot_orders.url().query().unwrap().contains("symbol="));

    let futures_orders = protocol
        .build_open_orders_request(MarketType::Perpetual, Some(&perpetual), 1_722_000_002_003)
        .unwrap();
    assert_eq!(futures_orders.url().path(), "/fapi/v1/openOrders");
    assert!(
        futures_orders
            .url()
            .query()
            .unwrap()
            .starts_with("symbol=BTCUSDT")
    );

    let positions = protocol
        .build_positions_request(Some(&perpetual), 1_722_000_002_004)
        .unwrap();
    assert_eq!(positions.method(), RemoteHttpMethod::Get);
    assert_eq!(positions.url().path(), "/fapi/v2/positionRisk");
    assert!(
        positions
            .url()
            .query()
            .unwrap()
            .starts_with("symbol=BTCUSDT")
    );
}

#[test]
fn book_ticker_requests_are_unsigned_and_select_the_correct_product_route() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));

    let spot = protocol
        .build_book_ticker_request(&Symbol::new("BTC-USDC-SPOT").unwrap(), MarketType::Spot)
        .unwrap();
    let perpetual = protocol
        .build_book_ticker_request(
            &Symbol::new("BTC-USDC-PERP").unwrap(),
            MarketType::Perpetual,
        )
        .unwrap();

    assert_eq!(spot.method(), RemoteHttpMethod::Get);
    assert_eq!(spot.url().path(), "/api/v3/ticker/bookTicker");
    assert_eq!(spot.url().query(), Some("symbol=BTCUSDT"));
    assert_eq!(perpetual.url().path(), "/fapi/v1/ticker/bookTicker");
    assert_eq!(perpetual.url().query(), Some("symbol=BTCUSDT"));
    assert_eq!(spot.header("X-MBX-APIKEY"), None);
    assert_eq!(perpetual.header("X-MBX-APIKEY"), None);
    assert!(signer.payloads.lock().unwrap().is_empty());
}
