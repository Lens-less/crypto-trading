use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use crypto_trading_domain::{
    MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    ExchangeError, HyperliquidAction, HyperliquidAsset, HyperliquidAssetCatalog,
    HyperliquidRequestSigner, HyperliquidSignature, HyperliquidTestnetEndpoint,
    HyperliquidTestnetProtocol, InstrumentRuleCatalog, InstrumentRules, RemoteHttpMethod,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
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
    actions: Mutex<Vec<(Value, u64, Option<String>)>>,
}

impl CapturingSigner {
    fn new() -> Self {
        Self {
            actions: Mutex::new(Vec::new()),
        }
    }
}

impl HyperliquidRequestSigner for CapturingSigner {
    fn account_address(&self) -> &'static str {
        "0x1111111111111111111111111111111111111111"
    }

    fn sign(
        &self,
        action: &HyperliquidAction,
        nonce: u64,
        vault_address: Option<&str>,
    ) -> Result<HyperliquidSignature, ExchangeError> {
        self.actions.lock().unwrap().push((
            serde_json::to_value(action).unwrap(),
            nonce,
            vault_address.map(str::to_owned),
        ));
        HyperliquidSignature::new(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            27,
        )
    }
}

fn protocol(signer: Arc<CapturingSigner>) -> HyperliquidTestnetProtocol {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();
    let assets = HyperliquidAssetCatalog::new(vec![
        HyperliquidAsset::new(spot.clone(), MarketType::Spot, 10_001, "BTC/USDC").unwrap(),
        HyperliquidAsset::new(perpetual.clone(), MarketType::Perpetual, 0, "BTC").unwrap(),
    ])
    .unwrap();
    let rules = InstrumentRuleCatalog::new(vec![
        InstrumentRules::new(
            "hyperliquid",
            spot,
            MarketType::Spot,
            price("0.1"),
            quantity("0.0001"),
            quantity("0.0001"),
            Money::new(decimal("10")),
        )
        .unwrap(),
        InstrumentRules::new(
            "hyperliquid",
            perpetual,
            MarketType::Perpetual,
            price("0.1"),
            quantity("0.001"),
            quantity("0.001"),
            Money::new(decimal("10")),
        )
        .unwrap(),
    ])
    .unwrap();
    HyperliquidTestnetProtocol::authenticated(
        HyperliquidTestnetEndpoint::official(),
        assets,
        rules,
        signer,
        None,
    )
    .unwrap()
}

#[test]
fn perpetual_post_only_order_signs_the_typed_action_and_uses_exchange_route() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));
    let mut intent = OrderIntent::limit(
        "hyperliquid",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Sell,
        quantity("0.002"),
        price("50000.2"),
    );
    intent.client_order_id = Uuid::parse_str("feac48e2-9ea4-47f8-8e18-c31285714142").unwrap();
    intent.time_in_force = TimeInForce::PostOnly;
    intent.reduce_only = true;

    let request = protocol
        .build_order_request(&intent, Some(price("50000.2")), 1_722_100_000_123)
        .unwrap();

    let expected_action = json!({
        "type": "order",
        "orders": [{
            "a": 0,
            "b": false,
            "p": "50000.2",
            "s": "0.002",
            "r": true,
            "t": {"limit": {"tif": "Alo"}},
            "c": "0xfeac48e29ea447f88e18c31285714142"
        }],
        "grouping": "na"
    });
    assert_eq!(request.method(), RemoteHttpMethod::Post);
    assert_eq!(request.url().path(), "/exchange");
    assert_eq!(request.header("Content-Type"), Some("application/json"));
    assert_eq!(
        signer.actions.lock().unwrap().as_slice(),
        &[(expected_action.clone(), 1_722_100_000_123, None)]
    );

    let body: Value = serde_json::from_slice(request.body()).unwrap();
    assert_eq!(body["action"], expected_action);
    assert_eq!(body["nonce"], 1_722_100_000_123_u64);
    assert_eq!(body["vaultAddress"], Value::Null);
    assert_eq!(body["signature"]["v"], 27);
    assert_eq!(
        body["signature"]["r"],
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
}

#[test]
fn spot_order_uses_the_explicit_spot_asset_id() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));
    let mut intent = OrderIntent::limit(
        "hyperliquid",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.1"),
    );
    intent.client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
    intent.time_in_force = TimeInForce::Ioc;

    protocol
        .build_order_request(&intent, Some(price("50000.1")), 1_722_100_000_456)
        .unwrap();

    let captured = signer.actions.lock().unwrap();
    assert_eq!(captured[0].0["orders"][0]["a"], 10_001);
    assert_eq!(captured[0].0["orders"][0]["t"]["limit"]["tif"], "Ioc");
    assert_eq!(captured[0].0["orders"][0]["b"], true);
}

#[test]
fn unsupported_order_shapes_fail_before_the_signer_is_called() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(Arc::clone(&signer));

    let market = OrderIntent::market(
        "hyperliquid",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.001"),
    );
    assert!(matches!(
        protocol.build_order_request(&market, Some(price("50000")), 1_722_100_001_000),
        Err(ExchangeError::InvalidRequest { .. })
    ));

    let mut fill_or_kill = OrderIntent::limit(
        "hyperliquid",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.001"),
        price("50000.1"),
    );
    fill_or_kill.time_in_force = TimeInForce::Fok;
    assert!(
        protocol
            .build_order_request(&fill_or_kill, Some(price("50000.1")), 1_722_100_001_001)
            .is_err()
    );

    let mut spot_reduce_only = OrderIntent::limit(
        "hyperliquid",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Sell,
        quantity("0.0010"),
        price("50000.1"),
    );
    spot_reduce_only.reduce_only = true;
    assert!(matches!(
        protocol.build_order_request(&spot_reduce_only, Some(price("50000.1")), 1_722_100_001_002,),
        Err(ExchangeError::InvalidRequest { .. })
    ));

    assert!(signer.actions.lock().unwrap().is_empty());
}

#[test]
fn account_info_requests_are_product_specific_and_secret_safe_in_debug() {
    let signer = Arc::new(CapturingSigner::new());
    let protocol = protocol(signer);

    let open_orders = protocol.build_open_orders_request().unwrap();
    let perpetual_state = protocol.build_perpetual_state_request().unwrap();
    let spot_state = protocol.build_spot_state_request().unwrap();

    for (request, expected_type) in [
        (&open_orders, "openOrders"),
        (&perpetual_state, "clearinghouseState"),
        (&spot_state, "spotClearinghouseState"),
    ] {
        assert_eq!(request.url().path(), "/info");
        let body: Value = serde_json::from_slice(request.body()).unwrap();
        assert_eq!(body["type"], expected_type);
        assert_eq!(body["user"], "0x1111111111111111111111111111111111111111");
        let diagnostic = format!("{request:?}");
        assert!(!diagnostic.contains("1111111111111111111111111111111111111111"));
    }
}

#[test]
fn asset_catalog_rejects_product_id_mismatches_and_ambiguity() {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();

    assert!(HyperliquidAsset::new(spot, MarketType::Spot, 1, "BTC/USDC").is_err());
    assert!(HyperliquidAsset::new(perpetual, MarketType::Perpetual, 10_001, "BTC").is_err());

    let first = HyperliquidAsset::new(
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        0,
        "BTC",
    )
    .unwrap();
    let ambiguous = HyperliquidAsset::new(
        Symbol::new("XBT-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        0,
        "XBT",
    )
    .unwrap();
    assert!(HyperliquidAssetCatalog::new(vec![first, ambiguous]).is_err());
}
