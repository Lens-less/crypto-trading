use std::{
    collections::VecDeque,
    str::FromStr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use crypto_trading_domain::{MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    BinanceHmacSha256Signer, BinanceTestnetEndpoints, BinanceTestnetExchange,
    BinanceTestnetProtocol, CancellationDisposition, ExchangeError, ExchangeHandle, ExchangeMode,
    ExchangeOperation, ExchangeOperationKey, ExchangeSymbol, ExchangeSymbolCatalog,
    InstrumentRuleCatalog, InstrumentRules, ReconcileScope, RemoteHttpRequest, RemoteHttpResponse,
    RemoteHttpTransport, RemoteRetryAfter, TradingCommand, TradingReceipt,
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

#[derive(Debug)]
struct ScriptedTransport {
    requests: Mutex<Vec<RemoteHttpRequest>>,
    responses: Mutex<VecDeque<Result<RemoteHttpResponse, ExchangeError>>>,
}

impl ScriptedTransport {
    fn from_responses(responses: Vec<Result<RemoteHttpResponse, ExchangeError>>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        }
    }

    fn requests(&self) -> Vec<RemoteHttpRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl RemoteHttpTransport for ScriptedTransport {
    async fn send(&self, request: RemoteHttpRequest) -> Result<RemoteHttpResponse, ExchangeError> {
        self.requests.lock().unwrap().push(request);
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("test transport ran out of scripted responses")
    }
}

fn protocol() -> BinanceTestnetProtocol {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();
    let signer =
        Arc::new(BinanceHmacSha256Signer::new("offline-api-key", "offline-api-secret").unwrap());
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
        signer,
    )
    .unwrap()
}

#[tokio::test]
async fn execute_submit_uses_the_authenticated_protocol_and_parses_partial_fills() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![Ok(
        RemoteHttpResponse::new(
            200,
            br#"{
                "symbol":"BTCUSDT",
                "orderId":28,
                "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                "transactTime":1722000000123,
                "price":"50000.10",
                "origQty":"0.0010",
                "executedQty":"0.0005",
                "cummulativeQuoteQty":"25.00005",
                "status":"PARTIALLY_FILLED",
                "timeInForce":"GTC",
                "type":"LIMIT",
                "side":"BUY"
            }"#,
        )
        .unwrap(),
    )]));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.10"),
    );
    intent.client_order_id = uuid::Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();

    let receipt = exchange
        .execute(TradingCommand::Submit(intent.clone()))
        .await
        .unwrap();

    let TradingReceipt::Submitted { order, .. } = receipt else {
        panic!("expected submitted receipt");
    };
    assert_eq!(order.id, "binance:spot:BTCUSDT:28");
    assert_eq!(order.intent.client_order_id, intent.client_order_id);
    assert_eq!(order.filled_quantity.as_decimal(), decimal("0.0005"));
    assert_eq!(transport.requests()[0].url().path(), "/api/v3/order");
}

#[tokio::test]
async fn execute_cancel_decodes_server_order_refs_without_hidden_lookup_state() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![Ok(
        RemoteHttpResponse::new(
            200,
            br#"{
                "symbol":"BTCUSDT",
                "orderId":29,
                "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                "updateTime":1722000000456,
                "price":"50000.20",
                "origQty":"0.002",
                "executedQty":"0.0000",
                "status":"CANCELED",
                "timeInForce":"GTX",
                "type":"LIMIT",
                "side":"SELL",
                "reduceOnly":true
            }"#,
        )
        .unwrap(),
    )]));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });

    let receipt = exchange
        .execute(TradingCommand::Cancel {
            order_id: "binance:usdm:BTCUSDT:29".to_owned(),
        })
        .await
        .unwrap();

    let TradingReceipt::Cancelled {
        orders,
        disposition,
    } = receipt
    else {
        panic!("expected cancellation receipt");
    };
    assert_eq!(disposition, CancellationDisposition::Cancelled);
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].id, "binance:usdm:BTCUSDT:29");
    assert_eq!(transport.requests()[0].url().path(), "/fapi/v1/order");
}

#[tokio::test]
async fn status_never_mislabels_testnet_authority_as_live() {
    let transport = Arc::new(ScriptedTransport::from_responses(Vec::new()));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport, || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });

    let status = exchange.status().await.unwrap();

    assert_eq!(status.mode, ExchangeMode::Testnet);
}

#[tokio::test]
async fn cancel_all_transport_failures_are_ambiguous_and_keep_the_exact_scope() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![Err(
        ExchangeError::unavailable("connection closed after dispatch"),
    )]));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport, || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });
    let symbol = Symbol::new("BTC-USDC-SPOT").unwrap();

    let error = exchange
        .execute(TradingCommand::CancelAll {
            symbol: Some(symbol.clone()),
            market_type: Some(MarketType::Spot),
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::CancelAll,
            operation_key: Some(ExchangeOperationKey::CancelAll {
                symbol: Some(ref actual_symbol),
                market_type: Some(MarketType::Spot),
            }),
            ..
        } if actual_symbol == &symbol
    ));
}

#[tokio::test]
async fn reconcile_all_merges_owned_foreign_orders_and_positions() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![
        Ok(RemoteHttpResponse::new(
            200,
            br#"[{
                "symbol":"BTCUSDT",
                "orderId":28,
                "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                "transactTime":1722000000123,
                "price":"50000.10",
                "origQty":"0.0010",
                "executedQty":"0.0000",
                "status":"NEW",
                "timeInForce":"GTC",
                "type":"LIMIT",
                "side":"BUY"
            }]"#,
        )
        .unwrap()),
        Ok(RemoteHttpResponse::new(
            200,
            br#"[{
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
                "side":"SELL",
                "reduceOnly":true
            }]"#,
        )
        .unwrap()),
        Ok(RemoteHttpResponse::new(
            200,
            br#"[{
                "symbol":"BTCUSDT",
                "positionAmt":"0.005",
                "entryPrice":"50000.1",
                "markPrice":"50010.1",
                "updateTime":1722000000456
            }]"#,
        )
        .unwrap()),
    ]));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });

    let receipt = exchange.reconcile(ReconcileScope::All).await.unwrap();

    assert_eq!(receipt.orders.len(), 1);
    assert_eq!(receipt.foreign_orders.len(), 1);
    assert_eq!(receipt.positions.len(), 1);
    let requests = transport.requests();
    assert_eq!(requests[0].url().path(), "/api/v3/openOrders");
    assert_eq!(requests[1].url().path(), "/fapi/v1/openOrders");
    assert_eq!(requests[2].url().path(), "/fapi/v2/positionRisk");
}

#[tokio::test]
async fn execute_submit_retries_once_after_clock_skew_and_time_sync() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![
        Ok(RemoteHttpResponse::new(
            400,
            br#"{"code":-1021,"msg":"Timestamp for this request was outside of the recvWindow."}"#,
        )
        .unwrap()),
        Ok(RemoteHttpResponse::new(200, br#"{"serverTime":1722000005000}"#).unwrap()),
        Ok(RemoteHttpResponse::new(
            200,
            br#"{
                    "symbol":"BTCUSDT",
                    "orderId":30,
                    "clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf",
                    "transactTime":1722000005123,
                    "price":"50000.10",
                    "origQty":"0.0010",
                    "executedQty":"0.0010",
                    "cummulativeQuoteQty":"50.0001",
                    "status":"FILLED",
                    "timeInForce":"GTC",
                    "type":"LIMIT",
                    "side":"BUY"
                }"#,
        )
        .unwrap()),
    ]));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2024, 7, 25, 9, 10, 11).unwrap()
    });
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDC-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.10"),
    );
    intent.client_order_id = uuid::Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();

    let receipt = exchange
        .execute(TradingCommand::Submit(intent))
        .await
        .unwrap();

    let TradingReceipt::Submitted { order, .. } = receipt else {
        panic!("expected submitted receipt");
    };
    assert_eq!(order.id, "binance:spot:BTCUSDT:30");
    let requests = transport.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].url().path(), "/api/v3/order");
    assert_eq!(requests[1].url().path(), "/api/v3/time");
    assert_eq!(requests[2].url().path(), "/api/v3/order");
}

#[tokio::test]
async fn reconcile_orders_rejects_an_explicit_unsupported_symbol_without_expanding_scope() {
    let transport = Arc::new(ScriptedTransport::from_responses(Vec::new()));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });

    let error = exchange
        .reconcile(ReconcileScope::Orders {
            symbol: Some(Symbol::new("ETH-USDC-SPOT").unwrap()),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, ExchangeError::InvalidRequest { .. }));
    assert!(transport.requests().is_empty());
}

#[tokio::test]
async fn reconcile_positions_rejects_an_explicit_wrong_market_symbol_without_expanding_scope() {
    let transport = Arc::new(ScriptedTransport::from_responses(Vec::new()));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });

    let error = exchange
        .reconcile(ReconcileScope::Positions {
            symbol: Some(Symbol::new("BTC-USDC-SPOT").unwrap()),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, ExchangeError::InvalidRequest { .. }));
    assert!(transport.requests().is_empty());
}

#[tokio::test]
async fn reconcile_preserves_retry_after_metadata_on_binance_rate_limits() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![Ok(
        RemoteHttpResponse::new_with_headers(
            429,
            vec![("Retry-After".to_owned(), "120".to_owned())],
            br#"{"code":-1003,"msg":"Too many requests"}"#,
        )
        .unwrap(),
    )]));
    let exchange = BinanceTestnetExchange::with_clock(protocol(), transport.clone(), || {
        Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap()
    });

    let error = exchange
        .reconcile(ReconcileScope::Orders {
            symbol: Some(Symbol::new("BTC-USDC-SPOT").unwrap()),
        })
        .await
        .unwrap_err();

    let metadata = error.remote_failure_metadata().unwrap();
    assert_eq!(metadata.exchange_code.as_deref(), Some("-1003"));
    assert!(matches!(
        metadata.retry_after,
        Some(RemoteRetryAfter::Seconds(120))
    ));
    assert_eq!(transport.requests().len(), 1);
}
