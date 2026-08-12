use chrono::{DateTime, Utc};
use crypto_trading_domain::MarketType;
use crypto_trading_exchange::{
    BinancePublicExchange, BinanceTestnetProtocol, BinanceUserDataEvent,
};
use rust_decimal::Decimal;
use std::str::FromStr;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("test timestamp must be valid")
        .with_timezone(&Utc)
}

#[test]
fn websocket_book_ticker_supports_raw_and_combined_stream_payloads() {
    let received_at = timestamp("2026-08-12T00:00:00Z");

    let raw = BinancePublicExchange::parse_book_ticker_stream_observation(
        br#"{
            "u":400900217,
            "s":"BNBUSDT",
            "b":"25.3519",
            "B":"31.21",
            "a":"25.3652",
            "A":"40.66"
        }"#,
        received_at,
    )
    .unwrap();
    assert_eq!(raw.snapshot.exchange(), "binance");
    assert_eq!(raw.snapshot.symbol.as_str(), "BNBUSDT");
    assert_eq!(raw.snapshot.market_type, MarketType::Spot);
    assert_eq!(raw.snapshot.timestamp, received_at);
    assert_eq!(raw.snapshot.bid().as_decimal(), decimal("25.3519"));
    assert_eq!(raw.snapshot.ask().as_decimal(), decimal("25.3652"));
    assert_eq!(
        raw.snapshot.bid_quantity.unwrap().as_decimal(),
        decimal("31.21")
    );
    assert_eq!(
        raw.snapshot.ask_quantity.unwrap().as_decimal(),
        decimal("40.66")
    );
    assert_eq!(raw.source_sequence, Some(400_900_217));

    let combined = BinancePublicExchange::parse_book_ticker_stream_observation(
        br#"{
            "stream":"bnbusdt@bookTicker",
            "data":{
                "u":400900218,
                "s":"BNBUSDT",
                "b":"25.4519",
                "B":"32.21",
                "a":"25.4652",
                "A":"41.66"
            }
        }"#,
        received_at,
    )
    .unwrap();
    assert_eq!(combined.snapshot.symbol.as_str(), "BNBUSDT");
    assert_eq!(combined.snapshot.timestamp, received_at);
    assert_eq!(combined.source_sequence, Some(400_900_218));
}

#[test]
fn websocket_book_ticker_rejects_unknown_payload_shapes() {
    let error = BinancePublicExchange::parse_book_ticker_stream_observation(
        br#"{"stream":"bnbusdt@bookTicker","data":{"s":"BNBUSDT"}}"#,
        timestamp("2026-08-12T00:00:00Z"),
    )
    .unwrap_err();

    assert!(error.to_string().contains("binance"));
}

#[test]
fn user_data_parser_supports_wrapped_execution_reports_and_account_updates() {
    let execution = BinanceTestnetProtocol::parse_user_data_event(
        br#"{
            "subscriptionId":0,
            "event":{
                "e":"executionReport",
                "E":1723422222000,
                "s":"ETHBTC",
                "c":"order-7",
                "S":"BUY",
                "o":"LIMIT",
                "f":"GTC",
                "q":"1.00000000",
                "p":"0.10264410",
                "x":"TRADE",
                "X":"PARTIALLY_FILLED",
                "i":4293153,
                "l":"0.10000000",
                "z":"0.10000000",
                "L":"0.10264410",
                "T":1723422221999,
                "I":8641984
            }
        }"#,
    )
    .unwrap();
    let BinanceUserDataEvent::ExecutionReport(execution) = execution else {
        panic!("expected execution report");
    };
    assert_eq!(execution.event_time, timestamp("2024-08-12T00:23:42Z"));
    assert_eq!(
        execution.transaction_time,
        timestamp("2024-08-12T00:23:41.999Z")
    );
    assert_eq!(execution.symbol.as_str(), "ETHBTC");
    assert_eq!(execution.order_id, 4_293_153);
    assert_eq!(execution.execution_id, Some(8_641_984));
    assert_eq!(
        execution.cumulative_filled_quantity.as_decimal(),
        decimal("0.10000000")
    );
    assert_eq!(
        execution.last_executed_quantity.unwrap().as_decimal(),
        decimal("0.10000000")
    );
    assert_eq!(execution.price.unwrap().as_decimal(), decimal("0.10264410"));

    let account = BinanceTestnetProtocol::parse_user_data_event(
        br#"{
            "subscriptionId":0,
            "event":{
                "e":"outboundAccountPosition",
                "E":1564034571105,
                "u":1564034571073,
                "B":[
                    {"a":"ETH","f":"10000.000000","l":"0.000000"},
                    {"a":"BTC","f":"0.000000","l":"12.500000"}
                ]
            }
        }"#,
    )
    .unwrap();
    let BinanceUserDataEvent::AccountUpdate(account) = account else {
        panic!("expected account update");
    };
    assert_eq!(account.event_time, timestamp("2019-07-25T06:02:51.105Z"));
    assert_eq!(
        account.account_update_time,
        timestamp("2019-07-25T06:02:51.073Z")
    );
    assert_eq!(account.balances.len(), 2);
    assert_eq!(account.balances[0].asset, "BTC");
    assert_eq!(account.balances[0].free, decimal("0.000000"));
    assert_eq!(account.balances[0].locked, decimal("12.500000"));
    assert_eq!(account.balances[1].asset, "ETH");
    assert_eq!(account.balances[1].free, decimal("10000.000000"));
}

#[test]
fn user_data_parser_accepts_market_execution_reports_with_zero_order_price() {
    let execution = BinanceTestnetProtocol::parse_user_data_event(
        br#"{
            "subscriptionId":0,
            "event":{
                "e":"executionReport",
                "E":1723422222000,
                "s":"ETHBTC",
                "c":"order-8",
                "S":"BUY",
                "o":"MARKET",
                "f":"GTC",
                "q":"1.00000000",
                "p":"0.00000000",
                "x":"TRADE",
                "X":"FILLED",
                "i":4293154,
                "l":"1.00000000",
                "z":"1.00000000",
                "L":"0.10264410",
                "T":1723422221999,
                "I":8641985
            }
        }"#,
    )
    .unwrap();
    let BinanceUserDataEvent::ExecutionReport(execution) = execution else {
        panic!("expected execution report");
    };
    assert!(execution.price.is_none());
    assert_eq!(
        execution.last_executed_price.unwrap().as_decimal(),
        decimal("0.10264410")
    );
}
