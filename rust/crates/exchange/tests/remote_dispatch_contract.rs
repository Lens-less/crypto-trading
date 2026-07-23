use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use crypto_trading_domain::{MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    BinanceRequestSigner, BinanceTestnetEndpoints, BinanceTestnetProtocol, ExchangeError,
    ExchangeOperation, ExchangeOperationKey, ExchangeSymbol, ExchangeSymbolCatalog,
    HyperliquidAction, HyperliquidAsset, HyperliquidAssetCatalog, HyperliquidRequestSigner,
    HyperliquidSignature, HyperliquidTestnetEndpoint, HyperliquidTestnetProtocol,
    InstrumentRuleCatalog, InstrumentRules, RemoteHttpRequest, RemoteHttpResponse,
    RemoteHttpTransport,
};
use rust_decimal::Decimal;

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
        "offline-key"
    }

    fn sign(&self, _payload: &str) -> Result<String, ExchangeError> {
        Ok("offline-signature".to_owned())
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
        HyperliquidSignature::new(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            27,
        )
    }
}

struct ScriptedTransport {
    result: Mutex<Option<Result<RemoteHttpResponse, ExchangeError>>>,
}

impl ScriptedTransport {
    fn returning(result: Result<RemoteHttpResponse, ExchangeError>) -> Self {
        Self {
            result: Mutex::new(Some(result)),
        }
    }
}

#[async_trait]
impl RemoteHttpTransport for ScriptedTransport {
    async fn send(&self, _request: RemoteHttpRequest) -> Result<RemoteHttpResponse, ExchangeError> {
        self.result.lock().unwrap().take().unwrap()
    }
}

fn binance_protocol() -> BinanceTestnetProtocol {
    let symbol = Symbol::new("BTC-USDC-SPOT").unwrap();
    BinanceTestnetProtocol::authenticated(
        BinanceTestnetEndpoints::official(),
        ExchangeSymbolCatalog::new(vec![
            ExchangeSymbol::new("binance", symbol.clone(), MarketType::Spot, "BTCUSDT").unwrap(),
        ])
        .unwrap(),
        InstrumentRuleCatalog::new(vec![
            InstrumentRules::new(
                "binance",
                symbol,
                MarketType::Spot,
                price("0.1"),
                quantity("0.0001"),
                quantity("0.0001"),
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

#[tokio::test]
async fn transport_failures_after_mutating_dispatch_are_ambiguous_and_secret_safe() {
    let binance = binance_protocol();
    let intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.1"),
    );
    let transport = ScriptedTransport::returning(Err(ExchangeError::unavailable(
        "offline-key offline-signature should never escape",
    )));

    let error = binance
        .dispatch_order(
            &transport,
            &intent,
            Some(price("50000.1")),
            1_722_200_000_001,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        &error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::SubmitOrder,
            client_order_id: Some(id),
            operation_key: Some(ExchangeOperationKey::ClientOrderId { client_order_id }),
            reason,
        } if *id == intent.client_order_id
            && *client_order_id == intent.client_order_id
            && !reason.contains("offline-key")
            && !reason.contains("offline-signature")
    ));
}

#[tokio::test]
async fn server_failures_and_binance_timeout_codes_require_reconciliation() {
    for response in [
        RemoteHttpResponse::new(503, b"maintenance".to_vec()).unwrap(),
        RemoteHttpResponse::new(
            400,
            br#"{"code":-1007,"msg":"Timeout waiting for response from backend server."}"#.to_vec(),
        )
        .unwrap(),
    ] {
        let protocol = binance_protocol();
        let intent = OrderIntent::limit(
            "binance",
            Symbol::new("BTC-USDC-SPOT").unwrap(),
            MarketType::Spot,
            Side::Buy,
            quantity("0.0010"),
            price("50000.1"),
        );
        let transport = ScriptedTransport::returning(Ok(response));

        assert!(matches!(
            protocol
                .dispatch_order(
                    &transport,
                    &intent,
                    Some(price("50000.1")),
                    1_722_200_000_002,
                )
                .await
                .unwrap_err(),
            ExchangeError::AmbiguousOutcome { .. }
        ));
    }
}

#[tokio::test]
async fn definite_client_rejections_remain_non_ambiguous_remote_failures() {
    let protocol = binance_protocol();
    let intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.1"),
    );
    let response = RemoteHttpResponse::new(
        400,
        br#"{"code":-1013,"msg":"Filter failure: MIN_NOTIONAL"}"#.to_vec(),
    )
    .unwrap();
    let transport = ScriptedTransport::returning(Ok(response));

    assert!(matches!(
        protocol
            .dispatch_order(
                &transport,
                &intent,
                Some(price("50000.1")),
                1_722_200_000_003,
            )
            .await
            .unwrap_err(),
        ExchangeError::RemoteFailure {
            status: Some(400),
            ..
        }
    ));
}

#[tokio::test]
async fn hyperliquid_mutating_transport_failures_are_also_ambiguous() {
    let protocol = hyperliquid_protocol();
    let intent = OrderIntent::limit(
        "hyperliquid",
        Symbol::new("BTC-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Sell,
        quantity("0.001"),
        price("50000.1"),
    );
    let transport =
        ScriptedTransport::returning(Err(ExchangeError::unavailable("private key details")));

    let error = protocol
        .dispatch_order(
            &transport,
            &intent,
            Some(price("50000.1")),
            1_722_200_000_004,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::SubmitOrder,
            ..
        }
    ));
}

#[tokio::test]
async fn cancellation_transport_failures_keep_stable_reconciliation_keys() {
    let binance = binance_protocol();
    let transport =
        ScriptedTransport::returning(Err(ExchangeError::unavailable("connection reset")));
    let error = binance
        .dispatch_cancel(
            &transport,
            &Symbol::new("BTC-USDC-SPOT").unwrap(),
            MarketType::Spot,
            42,
            1_722_200_000_005,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::CancelOrder,
            operation_key: Some(ExchangeOperationKey::OrderId(ref key)),
            ..
        } if key == "binance:spot:BTC-USDC-SPOT:42"
    ));

    let hyperliquid = hyperliquid_protocol();
    let transport =
        ScriptedTransport::returning(Err(ExchangeError::unavailable("connection reset")));
    let error = hyperliquid
        .dispatch_cancel(
            &transport,
            &Symbol::new("BTC-USDC-PERP").unwrap(),
            MarketType::Perpetual,
            43,
            1_722_200_000_006,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::CancelOrder,
            operation_key: Some(ExchangeOperationKey::OrderId(ref key)),
            ..
        } if key == "hyperliquid:perpetual:BTC-USDC-PERP:43"
    ));
}

#[test]
fn remote_response_debug_does_not_dump_private_account_payloads() {
    let response =
        RemoteHttpResponse::new(200, br#"{"private_key":"never-log-me"}"#.to_vec()).unwrap();
    let diagnostic = format!("{response:?}");

    assert!(diagnostic.contains("status"));
    assert!(diagnostic.contains("body_bytes"));
    assert!(!diagnostic.contains("never-log-me"));
    assert!(!diagnostic.contains("private_key"));
}
