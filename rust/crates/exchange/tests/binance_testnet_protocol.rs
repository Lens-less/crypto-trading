use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use chrono::{TimeZone, Utc};
use crypto_trading_domain::{
    MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceHmacSha256Signer, BinanceProduct, BinanceRequestSigner, BinanceServerOrderRef,
    BinanceTestnetEndpoints, BinanceTestnetProtocol, ExchangeError, ExchangeSymbol,
    ExchangeSymbolCatalog, InstrumentRuleCatalog, InstrumentRules, RemoteHttpMethod,
    TradingReceipt,
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

    let client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
    let queried_order = protocol
        .build_query_order_request(
            &perpetual,
            MarketType::Perpetual,
            client_order_id,
            1_722_000_002_004,
        )
        .unwrap();
    assert_eq!(queried_order.method(), RemoteHttpMethod::Get);
    assert_eq!(queried_order.url().path(), "/fapi/v1/order");
    assert!(
        queried_order
            .url()
            .query()
            .unwrap()
            .starts_with("symbol=BTCUSDT&origClientOrderId=0f3c807d-776f-4de4-85d0-93760a82dfcf")
    );

    let positions = protocol
        .build_positions_request(Some(&perpetual), 1_722_000_002_005)
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

    let spot_balances = protocol
        .build_account_balances_request(BinanceProduct::Spot, 1_722_000_002_006)
        .unwrap();
    assert_eq!(spot_balances.method(), RemoteHttpMethod::Get);
    assert_eq!(spot_balances.url().path(), "/api/v3/account");
    assert!(
        spot_balances
            .url()
            .query()
            .unwrap()
            .starts_with("omitZeroBalances=true&recvWindow=5000&timestamp=1722000002006")
    );

    let usdm_balances = protocol
        .build_account_balances_request(BinanceProduct::UsdM, 1_722_000_002_007)
        .unwrap();
    assert_eq!(usdm_balances.method(), RemoteHttpMethod::Get);
    assert_eq!(usdm_balances.url().path(), "/fapi/v3/balance");
    assert!(
        usdm_balances
            .url()
            .query()
            .unwrap()
            .starts_with("recvWindow=5000&timestamp=1722000002007")
    );
}

#[test]
fn account_balances_preserve_product_semantics_and_reject_duplicate_assets() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);

    let spot = protocol
        .parse_account_balances_response(
            BinanceProduct::Spot,
            br#"{
                "balances": [
                    {"asset":"USDT","free":"900.25","locked":"99.75"},
                    {"asset":"BTC","free":"0.010","locked":"0"}
                ]
            }"#,
        )
        .unwrap();
    assert_eq!(spot.len(), 2);
    assert_eq!(spot[0].asset, "BTC");
    assert_eq!(spot[1].asset, "USDT");
    assert_eq!(spot[1].wallet_balance, decimal("1000.00"));
    assert_eq!(spot[1].available_balance, decimal("900.25"));
    assert_eq!(spot[1].locked_balance, Some(decimal("99.75")));

    let usdm = protocol
        .parse_account_balances_response(
            BinanceProduct::UsdM,
            br#"[
                {
                    "asset":"USDT",
                    "balance":"1000.50",
                    "availableBalance":"950.25"
                }
            ]"#,
        )
        .unwrap();
    assert_eq!(usdm.len(), 1);
    assert_eq!(usdm[0].wallet_balance, decimal("1000.50"));
    assert_eq!(usdm[0].available_balance, decimal("950.25"));
    assert_eq!(usdm[0].locked_balance, None);

    assert!(
        protocol
            .parse_account_balances_response(
                BinanceProduct::UsdM,
                br#"[
                    {"asset":"USDT","balance":"1","availableBalance":"1"},
                    {"asset":"USDT","balance":"1","availableBalance":"1"}
                ]"#,
            )
            .is_err()
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

#[test]
fn exchange_info_requests_are_unsigned_and_product_specific() {
    let endpoints = BinanceTestnetEndpoints::official();

    let spot = BinanceTestnetProtocol::build_exchange_info_request(
        &endpoints,
        BinanceProduct::Spot,
        "BTCUSDT",
    )
    .unwrap();
    let perpetual = BinanceTestnetProtocol::build_exchange_info_request(
        &endpoints,
        BinanceProduct::UsdM,
        "BTCUSDT",
    )
    .unwrap();

    assert_eq!(spot.method(), RemoteHttpMethod::Get);
    assert_eq!(spot.url().path(), "/api/v3/exchangeInfo");
    assert_eq!(spot.url().query(), Some("symbol=BTCUSDT"));
    assert_eq!(spot.header("X-MBX-APIKEY"), None);

    assert_eq!(perpetual.method(), RemoteHttpMethod::Get);
    assert_eq!(perpetual.url().path(), "/fapi/v1/exchangeInfo");
    assert_eq!(perpetual.url().query(), Some("symbol=BTCUSDT"));
    assert_eq!(perpetual.header("X-MBX-APIKEY"), None);
}

#[test]
fn spot_exchange_info_maps_notional_and_market_lot_rules() {
    let parsed = BinanceTestnetProtocol::parse_exchange_info_symbol(
        BinanceProduct::Spot,
        include_bytes!("fixtures/binance_spot_exchange_info.json"),
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        "BTCUSDT",
    )
    .unwrap();

    assert_eq!(
        parsed.symbol.standard_symbol(),
        &Symbol::new("BTC-USDT-SPOT").unwrap()
    );
    assert_eq!(parsed.symbol.market_type(), MarketType::Spot);
    assert_eq!(
        parsed.rules.price_tick().as_decimal(),
        decimal("0.01000000")
    );
    assert_eq!(
        parsed.rules.min_price().unwrap().as_decimal(),
        decimal("0.01000000")
    );
    assert_eq!(
        parsed.rules.max_price().unwrap().as_decimal(),
        decimal("1000000.00000000")
    );
    assert_eq!(
        parsed.rules.quantity_step().as_decimal(),
        decimal("0.00010000")
    );
    assert_eq!(
        parsed.rules.min_quantity().as_decimal(),
        decimal("0.00010000")
    );
    assert_eq!(
        parsed.rules.max_quantity().unwrap().as_decimal(),
        decimal("100.00000000")
    );
    assert_eq!(
        parsed.rules.market_quantity_step().unwrap().as_decimal(),
        decimal("0.00100000")
    );
    assert_eq!(
        parsed.rules.market_min_quantity().unwrap().as_decimal(),
        decimal("0.00100000")
    );
    assert_eq!(
        parsed.rules.market_max_quantity().unwrap().as_decimal(),
        decimal("10.00000000")
    );
    assert_eq!(
        parsed.rules.min_notional().as_decimal(),
        decimal("10.00000000")
    );
    assert_eq!(
        parsed.rules.max_notional().unwrap().as_decimal(),
        decimal("100000.00000000")
    );
    assert!(!parsed.rules.apply_min_notional_to_market());
    assert!(parsed.rules.apply_max_notional_to_market());
    assert_eq!(parsed.rules.market_notional_average_minutes(), Some(5));
    assert!(
        parsed
            .rules
            .requires_authoritative_market_notional_reference()
    );

    let catalog = InstrumentRuleCatalog::new(vec![parsed.rules.clone()]).unwrap();
    let market = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.001"),
    );
    let error = catalog
        .validate_order(&market, Some(price("50000")))
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Rejected { .. }));
    assert!(format!("{error}").contains("authoritative market notional reference"));
}

#[test]
fn usdm_exchange_info_requires_perpetual_trading_and_supports_min_notional_alias() {
    let parsed = BinanceTestnetProtocol::parse_exchange_info_symbol(
        BinanceProduct::UsdM,
        include_bytes!("fixtures/binance_usdm_exchange_info.json"),
        Symbol::new("BTC-USDT-PERP").unwrap(),
        "BTCUSDT",
    )
    .unwrap();

    assert_eq!(parsed.symbol.market_type(), MarketType::Perpetual);
    assert_eq!(parsed.rules.price_tick().as_decimal(), decimal("0.1000"));
    assert_eq!(parsed.rules.min_notional().as_decimal(), decimal("5"));
    assert!(parsed.rules.apply_min_notional_to_market());
    assert_eq!(
        parsed.rules.market_quantity_step().unwrap().as_decimal(),
        decimal("0.010")
    );
}

#[test]
fn exchange_info_parser_rejects_missing_or_mismatched_asset_identity() {
    let payload = include_bytes!("fixtures/binance_spot_exchange_info.json");

    assert!(matches!(
        BinanceTestnetProtocol::parse_exchange_info_symbol(
            BinanceProduct::Spot,
            payload,
            Symbol::new("ETH-USDT-SPOT").unwrap(),
            "BTCUSDT",
        ),
        Err(ExchangeError::InvalidResponse { .. })
    ));

    let missing_assets = br#"{
        "symbols": [{
            "symbol":"BTCUSDT",
            "status":"TRADING",
            "filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true}
            ]
        }]
    }"#;
    assert!(matches!(
        BinanceTestnetProtocol::parse_exchange_info_symbol(
            BinanceProduct::Spot,
            missing_assets,
            Symbol::new("BTC-USDT-SPOT").unwrap(),
            "BTCUSDT",
        ),
        Err(ExchangeError::InvalidResponse { .. })
    ));
}

#[test]
fn exchange_info_parser_rejects_unknown_filter_semantics() {
    let payload = br#"{
        "symbols": [{
            "symbol":"BTCUSDT",
            "status":"TRADING",
            "baseAsset":"BTC",
            "quoteAsset":"USDT",
            "filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true},
                {"filterType":"FUTURE_UNKNOWN_SAFETY_FILTER","limit":"1"}
            ]
        }]
    }"#;

    assert!(matches!(
        BinanceTestnetProtocol::parse_exchange_info_symbol(
            BinanceProduct::Spot,
            payload,
            Symbol::new("BTC-USDT-SPOT").unwrap(),
            "BTCUSDT",
        ),
        Err(ExchangeError::InvalidResponse { .. })
    ));
}

#[test]
fn spot_notional_metadata_requires_explicit_market_flags_and_average_window() {
    let missing_variants: [&[u8]; 3] = [
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","avgPriceMins":5}
            ]}]
        }"#,
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"NOTIONAL","minNotional":"5","maxNotional":"100","applyMinToMarket":true,"avgPriceMins":5}
            ]}]
        }"#,
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true}
            ]}]
        }"#,
    ];

    for payload in missing_variants {
        assert!(matches!(
            BinanceTestnetProtocol::parse_exchange_info_symbol(
                BinanceProduct::Spot,
                payload,
                Symbol::new("BTC-USDT-SPOT").unwrap(),
                "BTCUSDT",
            ),
            Err(ExchangeError::InvalidResponse { .. })
        ));
    }
}

#[test]
fn exchange_info_parser_rejects_missing_duplicate_conflicting_and_non_trading_filters() {
    let invalid_payloads: [&[u8]; 4] = [
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5}
            ]}]
        }"#,
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5}
            ]}]
        }"#,
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5},
                {"filterType":"NOTIONAL","minNotional":"5","maxNotional":"100","applyMinToMarket":true,"applyMaxToMarket":true,"avgPriceMins":5}
            ]}]
        }"#,
        br#"{
            "symbols":[{"symbol":"BTCUSDT","status":"HALT","baseAsset":"BTC","quoteAsset":"USDT","filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true,"avgPriceMins":5}
            ]}]
        }"#,
    ];

    for payload in invalid_payloads {
        assert!(matches!(
            BinanceTestnetProtocol::parse_exchange_info_symbol(
                BinanceProduct::Spot,
                payload,
                Symbol::new("BTC-USDT-SPOT").unwrap(),
                "BTCUSDT",
            ),
            Err(ExchangeError::InvalidResponse { .. })
        ));
    }
}

#[test]
fn disabled_market_maximum_remains_optional_without_disabling_market_step_or_minimum() {
    let payload = br#"{
        "symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[
            {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
            {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
            {"filterType":"MARKET_LOT_SIZE","minQty":"0.002","maxQty":"0","stepSize":"0.002"},
            {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":false,"avgPriceMins":5}
        ]}]
    }"#;

    let parsed = BinanceTestnetProtocol::parse_exchange_info_symbol(
        BinanceProduct::Spot,
        payload,
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        "BTCUSDT",
    )
    .unwrap();
    assert_eq!(
        parsed.rules.market_quantity_step().unwrap().as_decimal(),
        decimal("0.002")
    );
    assert_eq!(
        parsed.rules.market_min_quantity().unwrap().as_decimal(),
        decimal("0.002")
    );
    assert_eq!(parsed.rules.market_max_quantity(), None);
    assert!(
        !parsed
            .rules
            .requires_authoritative_market_notional_reference()
    );
}

#[test]
fn exchange_info_parser_fails_closed_on_missing_duplicate_disabled_and_non_perpetual_metadata() {
    let missing = br#"{"symbols":[]}"#;
    let duplicate = br#"{
        "symbols": [
            {
                "symbol":"BTCUSDT",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true}
                ]
            },
            {
                "symbol":"BTCUSDT",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true}
                ]
            }
        ]
    }"#;
    let disabled = br#"{
        "symbols": [
            {
                "symbol":"BTCUSDT",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","minNotional":"5","applyToMarket":true}
                ]
            }
        ]
    }"#;
    let wrong_contract = br#"{
        "symbols": [
            {
                "symbol":"BTCUSDT",
                "pair":"BTCUSDT",
                "contractType":"CURRENT_QUARTER",
                "status":"TRADING",
                "baseAsset":"BTC",
                "quoteAsset":"USDT",
                "filters":[
                    {"filterType":"PRICE_FILTER","minPrice":"0.1","maxPrice":"1000","tickSize":"0.1"},
                    {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"10","stepSize":"0.001"},
                    {"filterType":"MIN_NOTIONAL","notional":"5"}
                ]
            }
        ]
    }"#;

    for payload in [
        missing.as_slice(),
        duplicate.as_slice(),
        disabled.as_slice(),
    ] {
        assert!(matches!(
            BinanceTestnetProtocol::parse_exchange_info_symbol(
                BinanceProduct::Spot,
                payload,
                Symbol::new("BTC-USDT-SPOT").unwrap(),
                "BTCUSDT",
            ),
            Err(ExchangeError::InvalidResponse { .. })
        ));
    }
    assert!(matches!(
        BinanceTestnetProtocol::parse_exchange_info_symbol(
            BinanceProduct::UsdM,
            wrong_contract,
            Symbol::new("BTC-USDT-PERP").unwrap(),
            "BTCUSDT",
        ),
        Err(ExchangeError::InvalidResponse { .. })
    ));
}

#[test]
fn official_hmac_vector_matches_binance_documentation() {
    let signer = BinanceHmacSha256Signer::new(
        "offline-api-key",
        "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
    )
    .unwrap();

    let signature = signer
        .sign(
            "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559",
        )
        .unwrap();

    assert_eq!(
        signature,
        "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
    );
}

#[test]
fn open_orders_reconciliation_preserves_foreign_manual_orders() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);
    let received_at = Utc.with_ymd_and_hms(2026, 7, 25, 8, 9, 10).unwrap();

    let (orders, foreign_orders) = protocol
        .parse_open_orders_response(
            BinanceProduct::Spot,
            br#"[
                {
                    "symbol":"BTCUSDT",
                    "orderId":28,
                    "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                    "transactTime":1722000000123,
                    "price":"50000.10",
                    "origQty":"0.0010",
                    "executedQty":"0.0000",
                    "cummulativeQuoteQty":"0.000000",
                    "status":"NEW",
                    "timeInForce":"GTC",
                    "type":"LIMIT",
                    "side":"BUY"
                },
                {
                    "symbol":"BTCUSDT",
                    "orderId":29,
                    "clientOrderId":"manual-order",
                    "transactTime":1722000000456,
                    "price":"50001.00",
                    "origQty":"0.0020",
                    "executedQty":"0.0010",
                    "cummulativeQuoteQty":"50.001",
                    "status":"PARTIALLY_FILLED",
                    "timeInForce":"GTC",
                    "type":"LIMIT",
                    "side":"SELL"
                }
            ]"#,
            received_at,
        )
        .unwrap();

    assert_eq!(orders.len(), 1);
    assert_eq!(foreign_orders.len(), 1);
    assert_eq!(orders[0].id, "binance:spot:BTCUSDT:28");
    assert_eq!(foreign_orders[0].id, "binance:spot:BTCUSDT:29");
    assert_eq!(
        foreign_orders[0].client_order_id.as_deref(),
        Some("manual-order")
    );
    assert_eq!(
        foreign_orders[0].filled_quantity.as_decimal(),
        decimal("0.0010")
    );
}

#[test]
fn positions_reconciliation_maps_signed_quantities_into_typed_positions() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);
    let received_at = Utc.with_ymd_and_hms(2026, 7, 25, 8, 9, 10).unwrap();

    let positions = protocol
        .parse_positions_response(
            br#"[
                {
                    "symbol":"BTCUSDT",
                    "positionAmt":"0.005",
                    "entryPrice":"50000.1",
                    "markPrice":"50010.1",
                    "updateTime":1722000000456
                },
                {
                    "symbol":"ETHUSDT",
                    "positionAmt":"0"
                }
            ]"#,
            received_at,
        )
        .unwrap();

    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].symbol.as_str(), "BTC-USDC-PERP");
    assert_eq!(positions[0].side, crypto_trading_domain::PositionSide::Long);
    assert_eq!(positions[0].quantity.as_decimal(), decimal("0.005"));

    assert!(
        protocol
            .parse_positions_response(
                br#"[{"symbol":"ETHUSDT","positionAmt":"0.005"}]"#,
                received_at,
            )
            .is_err(),
        "unknown non-zero positions must still fail closed"
    );
}

#[test]
fn server_order_refs_round_trip_through_cancel_identity() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);

    let order_ref = protocol
        .parse_server_order_ref("binance:usdm:BTCUSDT:42")
        .unwrap();

    assert_eq!(
        order_ref,
        BinanceServerOrderRef {
            symbol: Symbol::new("BTC-USDC-PERP").unwrap(),
            market_type: MarketType::Perpetual,
            wire_symbol: "BTCUSDT".to_owned(),
            order_id: 42,
        }
    );
}

#[test]
fn spot_cancel_responses_correlate_on_the_original_client_order_id() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);
    let received_at = Utc.with_ymd_and_hms(2026, 7, 25, 8, 9, 10).unwrap();

    // Binance Spot answers a cancel with the cancelled order's identity in
    // `origClientOrderId` and the cancel request's own generated identity in
    // `clientOrderId`. Correlating on the latter would read every cancelled
    // order as foreign and break the cancel path against the real venue.
    let receipt = protocol
        .parse_order_response(
            BinanceProduct::Spot,
            br#"{
                "symbol":"BTCUSDT",
                "origClientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                "orderId":28,
                "clientOrderId":"cancelMyOrder1",
                "transactTime":1722000000123,
                "price":"50000.10",
                "origQty":"0.0010",
                "executedQty":"0.0000",
                "cummulativeQuoteQty":"0.000000",
                "status":"CANCELED",
                "timeInForce":"GTC",
                "type":"LIMIT",
                "side":"BUY"
            }"#,
            received_at,
        )
        .expect("a cancelled order identified by origClientOrderId stays owned");

    let TradingReceipt::Submitted { order, .. } = receipt else {
        panic!("a parsed single-order response reports the order it describes");
    };
    assert_eq!(
        order.intent.client_order_id,
        Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap(),
    );
}

#[test]
fn cancel_all_responses_keep_foreign_orders_distinguishable() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);
    let received_at = Utc.with_ymd_and_hms(2026, 7, 25, 8, 9, 10).unwrap();

    let (orders, foreign_orders) = protocol
        .parse_open_orders_response(
            BinanceProduct::Spot,
            br#"[
                {
                    "symbol":"BTCUSDT",
                    "origClientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                    "orderId":28,
                    "clientOrderId":"cancelMyOrder1",
                    "transactTime":1722000000123,
                    "price":"50000.10",
                    "origQty":"0.0010",
                    "executedQty":"0.0000",
                    "cummulativeQuoteQty":"0.000000",
                    "status":"CANCELED",
                    "timeInForce":"GTC",
                    "type":"LIMIT",
                    "side":"BUY"
                },
                {
                    "symbol":"BTCUSDT",
                    "origClientOrderId":"manual-order",
                    "orderId":29,
                    "clientOrderId":"cancelMyOrder2",
                    "transactTime":1722000000456,
                    "price":"50001.00",
                    "origQty":"0.0020",
                    "executedQty":"0.0000",
                    "cummulativeQuoteQty":"0.000000",
                    "status":"CANCELED",
                    "timeInForce":"GTC",
                    "type":"LIMIT",
                    "side":"SELL"
                }
            ]"#,
            received_at,
        )
        .expect("a cancel-all batch parses");

    assert_eq!(orders.len(), 1, "the owned UUID order stays owned");
    assert_eq!(
        orders[0].intent.client_order_id,
        Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap(),
    );
    assert_eq!(foreign_orders.len(), 1, "the manual order stays foreign");
    assert_eq!(
        foreign_orders[0].client_order_id.as_deref(),
        Some("manual-order"),
        "a foreign order reports the identity it was submitted under",
    );
}

#[test]
fn usdm_cancel_all_acknowledgements_are_read_from_the_body() {
    BinanceTestnetProtocol::parse_usdm_cancel_all_response(
        br#"{"code":200,"msg":"The operation of cancel all open order is done."}"#,
    )
    .expect("the documented success code is accepted");

    // USD-M reports refusals inside a HTTP 200 body, so trusting the status
    // code alone would report a refused cancellation as a completed one.
    let error = BinanceTestnetProtocol::parse_usdm_cancel_all_response(
        br#"{"code":-1102,"msg":"Mandatory parameter 'symbol' was not sent."}"#,
    )
    .expect_err("an in-body failure code must not read as a completed cancellation");
    assert!(
        error.to_string().contains("-1102"),
        "the reported code stays visible to the operator: {error}"
    );

    assert!(
        BinanceTestnetProtocol::parse_usdm_cancel_all_response(b"not json").is_err(),
        "an unrecognised acknowledgement fails closed"
    );
}
