use std::{str::FromStr, sync::Arc};

use chrono::{TimeZone, Utc};
use crypto_trading_domain::{
    MarketType, Money, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceProduct, BinanceRequestSigner, BinanceTestnetEndpoints, BinanceTestnetProtocol,
    ExchangeError, ExchangeSymbol, ExchangeSymbolCatalog, HyperliquidAction, HyperliquidAsset,
    HyperliquidAssetCatalog, HyperliquidRequestSigner, HyperliquidSignature,
    HyperliquidTestnetEndpoint, HyperliquidTestnetProtocol, InstrumentRuleCatalog, InstrumentRules,
    SubmissionDisposition, TradingReceipt,
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

struct BinanceSigner;

impl BinanceRequestSigner for BinanceSigner {
    fn api_key(&self) -> &'static str {
        "key"
    }

    fn sign(&self, _payload: &str) -> Result<String, ExchangeError> {
        Ok("signature".to_owned())
    }
}

struct HyperliquidSigner;

impl HyperliquidRequestSigner for HyperliquidSigner {
    fn account_address(&self) -> &'static str {
        "0x1111111111111111111111111111111111111111"
    }

    fn sign(
        &self,
        _action: &HyperliquidAction,
        _nonce: u64,
        _vault_address: Option<&str>,
    ) -> Result<HyperliquidSignature, ExchangeError> {
        unreachable!("response parsing must not sign")
    }
}

fn binance_protocol() -> BinanceTestnetProtocol {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();
    BinanceTestnetProtocol::authenticated(
        BinanceTestnetEndpoints::official(),
        ExchangeSymbolCatalog::new(vec![
            ExchangeSymbol::new("binance", spot.clone(), MarketType::Spot, "BTCUSDT").unwrap(),
            ExchangeSymbol::new(
                "binance",
                perpetual.clone(),
                MarketType::Perpetual,
                "BTCUSDT",
            )
            .unwrap(),
        ])
        .unwrap(),
        InstrumentRuleCatalog::new(vec![
            InstrumentRules::new(
                "binance",
                spot,
                MarketType::Spot,
                price("0.1"),
                quantity("0.0001"),
                quantity("0.0001"),
                Money::new(decimal("5")),
            )
            .unwrap(),
            InstrumentRules::new(
                "binance",
                perpetual,
                MarketType::Perpetual,
                price("0.1"),
                quantity("0.001"),
                quantity("0.001"),
                Money::new(decimal("5")),
            )
            .unwrap(),
        ])
        .unwrap(),
        Arc::new(BinanceSigner),
    )
    .unwrap()
}

fn hyperliquid_protocol() -> HyperliquidTestnetProtocol {
    let symbol = Symbol::new("BTC-USDC-PERP").unwrap();
    HyperliquidTestnetProtocol::authenticated(
        HyperliquidTestnetEndpoint::official(),
        HyperliquidAssetCatalog::new(vec![
            HyperliquidAsset::new(symbol.clone(), MarketType::Perpetual, 0, "BTC").unwrap(),
        ])
        .unwrap(),
        InstrumentRuleCatalog::new(vec![
            InstrumentRules::new(
                "hyperliquid",
                symbol,
                MarketType::Perpetual,
                price("0.1"),
                quantity("0.001"),
                quantity("0.001"),
                Money::new(decimal("5")),
            )
            .unwrap(),
        ])
        .unwrap(),
        Arc::new(HyperliquidSigner),
        None,
    )
    .unwrap()
}

#[test]
fn binance_spot_and_usdm_order_responses_map_to_typed_receipts() {
    let protocol = binance_protocol();
    let received_at = Utc.with_ymd_and_hms(2026, 7, 23, 4, 5, 6).unwrap();
    let client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();

    let spot = protocol
        .parse_order_response(
            BinanceProduct::Spot,
            br#"{
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
            }"#,
            received_at,
        )
        .unwrap();
    let TradingReceipt::Submitted {
        order: spot_order,
        disposition: spot_disposition,
    } = spot
    else {
        panic!("expected submitted receipt");
    };
    assert_eq!(spot_disposition, SubmissionDisposition::Open);
    assert_eq!(spot_order.id, "binance:spot:BTCUSDT:28");
    assert_eq!(spot_order.intent.client_order_id, client_order_id);
    assert_eq!(spot_order.intent.symbol.as_str(), "BTC-USDC-SPOT");
    assert_eq!(spot_order.intent.time_in_force, TimeInForce::Gtc);
    assert_eq!(spot_order.status, OrderStatus::Open);

    let futures = protocol
        .parse_order_response(
            BinanceProduct::UsdM,
            br#"{
                "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                "executedQty":"0.002",
                "orderId":29,
                "avgPrice":"50000.2",
                "origQty":"0.002",
                "price":"50000.2",
                "reduceOnly":true,
                "side":"SELL",
                "status":"FILLED",
                "symbol":"BTCUSDT",
                "timeInForce":"GTX",
                "type":"LIMIT",
                "updateTime":1722000000456
            }"#,
            received_at,
        )
        .unwrap();
    let TradingReceipt::Submitted {
        order: futures_order,
        disposition: futures_disposition,
    } = futures
    else {
        panic!("expected submitted receipt");
    };
    assert_eq!(futures_disposition, SubmissionDisposition::Filled);
    assert_eq!(futures_order.id, "binance:usdm:BTCUSDT:29");
    assert_eq!(futures_order.intent.symbol.as_str(), "BTC-USDC-PERP");
    assert_eq!(futures_order.intent.time_in_force, TimeInForce::PostOnly);
    assert!(futures_order.intent.reduce_only);
    assert_eq!(
        futures_order.average_fill_price.unwrap().as_decimal(),
        decimal("50000.2")
    );
}

#[test]
fn binance_order_parser_rejects_unowned_client_ids_and_inconsistent_fills() {
    let protocol = binance_protocol();
    let received_at = Utc.with_ymd_and_hms(2026, 7, 23, 4, 5, 6).unwrap();

    for body in [
        br#"{"symbol":"BTCUSDT","orderId":1,"clientOrderId":"manual","price":"1","origQty":"1","executedQty":"0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY"}"#.as_slice(),
        br#"{"symbol":"BTCUSDT","orderId":1,"clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf","price":"1","origQty":"1","executedQty":"2","status":"FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY"}"#.as_slice(),
    ] {
        assert!(matches!(
            protocol
                .parse_order_response(BinanceProduct::Spot, body, received_at)
                .unwrap_err(),
            ExchangeError::InvalidResponse { .. }
        ));
    }
}

#[test]
fn binance_spot_market_fill_derives_average_price_from_cumulative_quote() {
    let protocol = binance_protocol();
    let received_at = Utc.with_ymd_and_hms(2026, 7, 23, 4, 5, 6).unwrap();

    let receipt = protocol
        .parse_order_response(
            BinanceProduct::Spot,
            br#"{
                "symbol":"BTCUSDT",
                "orderId":30,
                "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                "transactTime":1722000000789,
                "price":"0.00000000",
                "origQty":"0.001",
                "executedQty":"0.001",
                "cummulativeQuoteQty":"50.0001",
                "status":"FILLED",
                "timeInForce":"GTC",
                "type":"MARKET",
                "side":"BUY"
            }"#,
            received_at,
        )
        .unwrap();
    let TradingReceipt::Submitted { order, .. } = receipt else {
        panic!("expected submitted receipt");
    };

    assert_eq!(
        order.average_fill_price.unwrap().as_decimal(),
        decimal("50000.1")
    );
}

#[test]
fn hyperliquid_resting_filled_and_rejected_statuses_are_not_conflated() {
    let protocol = hyperliquid_protocol();
    let received_at = Utc.with_ymd_and_hms(2026, 7, 23, 4, 5, 6).unwrap();
    let mut intent = OrderIntent::limit(
        "hyperliquid",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.001"),
        price("50000.1"),
    );
    intent.client_order_id = Uuid::parse_str("feac48e2-9ea4-47f8-8e18-c31285714142").unwrap();

    let resting = protocol
        .parse_order_response(
            &intent,
            br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":31}}]}}}"#,
            received_at,
        )
        .unwrap();
    let TradingReceipt::Submitted { order, disposition } = resting else {
        panic!("expected submitted receipt");
    };
    assert_eq!(disposition, SubmissionDisposition::Open);
    assert_eq!(order.id, "hyperliquid:perpetual:0:31");
    assert_eq!(order.status, OrderStatus::Open);

    let filled = protocol
        .parse_order_response(
            &intent,
            br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.001","avgPx":"50000.1","oid":32}}]}}}"#,
            received_at,
        )
        .unwrap();
    let TradingReceipt::Submitted { order, disposition } = filled else {
        panic!("expected submitted receipt");
    };
    assert_eq!(disposition, SubmissionDisposition::Filled);
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(order.filled_quantity.as_decimal(), decimal("0.001"));

    assert!(matches!(
        protocol
            .parse_order_response(
                &intent,
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Insufficient margin"}]}}}"#,
                received_at,
            )
            .unwrap_err(),
        ExchangeError::Rejected { .. }
    ));
}
