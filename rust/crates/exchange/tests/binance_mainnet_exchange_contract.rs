//! Offline contract for the authority-typed Binance Spot MAINNET adapters.
//!
//! All venues are scripted transports; no test touches the network. The read
//! adapter type ([`BinanceMainnetSpotReadExchange`]) exposes no submit,
//! cancel, or cancel-all surface at all — it does not implement
//! `ExchangeHandle` — so mutation-freedom is enforced by the compiler rather
//! than asserted here. These tests pin the remaining behavioral invariants:
//! the mainnet REST host, Spot-only authority, the one-shot lifecycle wire
//! protocol, and fail-closed handling of redirects and wide reconcile scopes.

use std::{
    collections::VecDeque,
    str::FromStr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use crypto_trading_domain::{MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    BinanceHmacSha256Signer, BinanceMainnetReadEndpoints, BinanceMainnetSpotExchange,
    BinanceMainnetSpotReadExchange, BinanceMainnetTradeEndpoints, ExchangeError, ExchangeHandle,
    ExchangeMode, ExchangeSymbol, ExchangeSymbolCatalog, InstrumentRuleCatalog, InstrumentRules,
    ReconcileScope, RemoteHttpMethod, RemoteHttpRequest, RemoteHttpResponse, RemoteHttpTransport,
    TradingCommand, TradingReceipt,
};
use rust_decimal::Decimal;
use uuid::Uuid;

const CLIENT_ORDER_ID: &str = "0f3c807d-776f-4de4-85d0-93760a82dfcf";

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

fn symbols() -> ExchangeSymbolCatalog {
    ExchangeSymbolCatalog::new(vec![
        ExchangeSymbol::new(
            "binance",
            Symbol::new("BTC-USDT-SPOT").unwrap(),
            MarketType::Spot,
            "BTCUSDT",
        )
        .unwrap(),
    ])
    .unwrap()
}

fn rules() -> InstrumentRuleCatalog {
    InstrumentRuleCatalog::new(vec![
        InstrumentRules::new(
            "binance",
            Symbol::new("BTC-USDT-SPOT").unwrap(),
            MarketType::Spot,
            price("0.1"),
            quantity("0.0001"),
            quantity("0.0001"),
            Money::new(decimal("5")),
        )
        .unwrap(),
    ])
    .unwrap()
}

fn signer() -> Arc<BinanceHmacSha256Signer> {
    Arc::new(BinanceHmacSha256Signer::new("offline-api-key", "offline-api-secret").unwrap())
}

fn read_exchange(transport: Arc<ScriptedTransport>) -> BinanceMainnetSpotReadExchange {
    BinanceMainnetSpotReadExchange::with_clock(
        BinanceMainnetReadEndpoints::official(),
        symbols(),
        rules(),
        signer(),
        transport,
        || Utc.with_ymd_and_hms(2026, 8, 13, 9, 10, 11).unwrap(),
    )
    .unwrap()
}

fn trade_exchange(transport: Arc<ScriptedTransport>) -> BinanceMainnetSpotExchange {
    BinanceMainnetSpotExchange::with_clock(
        BinanceMainnetTradeEndpoints::official(),
        symbols(),
        rules(),
        signer(),
        transport,
        || Utc.with_ymd_and_hms(2026, 8, 13, 9, 10, 11).unwrap(),
    )
    .unwrap()
}

fn spot_intent() -> OrderIntent {
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0010"),
        price("50000.1"),
    );
    intent.client_order_id = Uuid::parse_str(CLIENT_ORDER_ID).unwrap();
    intent
}

fn account_body() -> RemoteHttpResponse {
    RemoteHttpResponse::new(
        200,
        br#"{"balances":[
            {"asset":"BTC","free":"0.5","locked":"0"},
            {"asset":"USDT","free":"1000.25","locked":"10"}
        ]}"#,
    )
    .unwrap()
}

fn open_orders_body() -> RemoteHttpResponse {
    RemoteHttpResponse::new(200, br"[]").unwrap()
}

fn order_body(status: &str) -> RemoteHttpResponse {
    let body = format!(
        r#"{{
            "symbol":"BTCUSDT",
            "orderId":31,
            "clientOrderId":"{CLIENT_ORDER_ID}",
            "transactTime":1722000000123,
            "price":"50000.10",
            "origQty":"0.0010",
            "executedQty":"0.0000",
            "status":"{status}",
            "timeInForce":"GTC",
            "type":"LIMIT",
            "side":"BUY"
        }}"#
    );
    RemoteHttpResponse::new(200, body.as_bytes()).unwrap()
}

#[tokio::test]
async fn read_adapter_samples_spot_account_truth_with_signed_reads_only() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![
        Ok(account_body()),
        Ok(open_orders_body()),
        Ok(account_body()),
        Ok(open_orders_body()),
    ]));
    let exchange = read_exchange(Arc::clone(&transport));

    let snapshot = exchange.account_snapshot().await.unwrap();

    assert_eq!(snapshot.balances.len(), 2);
    assert_eq!(snapshot.balances[0].asset, "BTC");
    assert_eq!(snapshot.balances[0].available_balance, decimal("0.5"));
    assert_eq!(snapshot.balances[1].wallet_balance, decimal("1010.25"));
    assert!(snapshot.orders.is_empty());
    assert!(snapshot.foreign_orders.is_empty());

    let requests = transport.requests();
    assert_eq!(requests.len(), 4, "double sample must issue two read pairs");
    let paths = requests
        .iter()
        .map(|request| request.url().path())
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        [
            "/api/v3/account",
            "/api/v3/openOrders",
            "/api/v3/account",
            "/api/v3/openOrders"
        ]
    );
    for request in &requests {
        assert_eq!(request.method(), RemoteHttpMethod::Get);
        assert_eq!(request.url().host_str(), Some("api.binance.com"));
        assert_eq!(request.url().scheme(), "https");
    }

    let status = exchange.status().await.unwrap();
    assert_eq!(status.mode, ExchangeMode::ReadOnly);
}

#[tokio::test]
async fn trade_adapter_runs_the_one_shot_wire_protocol_on_the_pinned_mainnet_host() {
    let transport = Arc::new(ScriptedTransport::from_responses(vec![
        Ok(order_body("NEW")),
        Ok(order_body("NEW")),
        Ok(order_body("CANCELED")),
    ]));
    let exchange = trade_exchange(Arc::clone(&transport));
    let intent = spot_intent();

    let receipt = exchange
        .execute(TradingCommand::Submit(intent.clone()))
        .await
        .unwrap();
    let TradingReceipt::Submitted { order, .. } = receipt else {
        panic!("expected submitted receipt");
    };
    assert_eq!(order.id, "binance:spot:BTCUSDT:31");

    let queried = exchange
        .query_order(&intent.symbol, intent.client_order_id)
        .await
        .unwrap();
    assert_eq!(queried.intent.client_order_id, intent.client_order_id);

    exchange
        .execute(TradingCommand::Cancel {
            order_id: order.id.clone(),
        })
        .await
        .unwrap();

    let requests = transport.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method(), RemoteHttpMethod::Post);
    assert_eq!(requests[0].url().path(), "/api/v3/order");
    assert_eq!(requests[1].method(), RemoteHttpMethod::Get);
    assert!(
        requests[1]
            .url()
            .query()
            .unwrap()
            .contains(&format!("origClientOrderId={CLIENT_ORDER_ID}")),
        "query must be by the durable client order id"
    );
    assert_eq!(requests[2].method(), RemoteHttpMethod::Delete);
    for request in &requests {
        assert_eq!(request.url().host_str(), Some("api.binance.com"));
        assert_eq!(request.url().scheme(), "https");
    }

    let status = exchange.status().await.unwrap();
    assert_eq!(status.mode, ExchangeMode::Live);
}

#[tokio::test]
async fn mainnet_trade_authority_is_spot_only_and_never_dispatches_refused_commands() {
    let transport = Arc::new(ScriptedTransport::from_responses(Vec::new()));
    let exchange = trade_exchange(Arc::clone(&transport));

    let mut perpetual = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDT-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.001"),
        price("50000.1"),
    );
    perpetual.client_order_id = Uuid::parse_str(CLIENT_ORDER_ID).unwrap();
    assert!(matches!(
        exchange
            .execute(TradingCommand::Submit(perpetual))
            .await
            .unwrap_err(),
        ExchangeError::InvalidRequest { .. }
    ));

    assert!(matches!(
        exchange
            .execute(TradingCommand::CancelAll {
                symbol: None,
                market_type: None,
            })
            .await
            .unwrap_err(),
        ExchangeError::InvalidRequest { .. }
    ));

    for scope in [
        ReconcileScope::All,
        ReconcileScope::Orders { symbol: None },
        ReconcileScope::Positions {
            symbol: Some(Symbol::new("BTC-USDT-PERP").unwrap()),
        },
    ] {
        assert!(matches!(
            exchange.reconcile(scope).await.unwrap_err(),
            ExchangeError::InvalidRequest { .. }
        ));
    }

    assert!(
        transport.requests().is_empty(),
        "refused authority must never reach the transport"
    );
}

#[tokio::test]
async fn redirect_responses_fail_closed_instead_of_widening_the_host_pin() {
    // The production transport refuses to follow redirects
    // (reqwest::redirect::Policy::none in remote.rs), so a 3xx surfaces here
    // as a plain non-success response; the adapter must fail closed on it.
    let transport = Arc::new(ScriptedTransport::from_responses(vec![Ok(
        RemoteHttpResponse::new_with_headers(
            307,
            vec![(
                "Location".to_owned(),
                "https://evil.example.com/api/v3/account".to_owned(),
            )],
            br"redirect".to_vec(),
        )
        .unwrap(),
    )]));
    let exchange = read_exchange(Arc::clone(&transport));

    let error = exchange.account_snapshot().await.unwrap_err();

    assert!(matches!(
        error,
        ExchangeError::RemoteFailure {
            status: Some(307),
            ..
        }
    ));
    assert_eq!(
        transport.requests().len(),
        1,
        "a redirect must not trigger a follow-up request"
    );
}
