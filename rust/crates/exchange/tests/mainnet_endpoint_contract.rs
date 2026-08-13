//! Pinning contract for the authority-typed Binance Spot MAINNET endpoints.
//!
//! Mirrors `testnet_endpoint_contract.rs` in the opposite direction: mainnet
//! constructors must reject testnet hosts and every non-official origin, and
//! the read and trade endpoint types stay distinct so a generic URL string can
//! never confer mainnet authority.

use crypto_trading_exchange::{
    BinanceMainnetReadEndpoints, BinanceMainnetSpotMarketStreamEndpoint,
    BinanceMainnetSpotUserDataStreamEndpoint, BinanceMainnetTradeEndpoints, ExchangeError,
};

#[test]
fn official_mainnet_profiles_pin_the_exact_binance_spot_host() {
    let read = BinanceMainnetReadEndpoints::official();
    assert_eq!(
        read.rest_url("/api/v3/account").unwrap().as_str(),
        "https://api.binance.com/api/v3/account"
    );

    let trade = BinanceMainnetTradeEndpoints::official();
    assert_eq!(
        trade.rest_url("/api/v3/order").unwrap().as_str(),
        "https://api.binance.com/api/v3/order"
    );
}

#[test]
fn mainnet_constructors_reject_testnet_hosts_and_any_non_official_origin() {
    for rejected in [
        "https://testnet.binance.vision",
        "https://demo-fapi.binance.com",
        "https://fapi.binance.com",
        "https://api.binance.example.com",
        "https://example.com",
        "http://api.binance.com",
        "https://api.binance.com:444",
        "https://user@api.binance.com",
        "https://api.binance.com/base",
        "https://api.binance.com/?feature=1",
    ] {
        assert!(
            matches!(
                BinanceMainnetReadEndpoints::try_official(rejected),
                Err(ExchangeError::InvalidRequest { .. })
            ),
            "read endpoints must reject {rejected}"
        );
        assert!(
            matches!(
                BinanceMainnetTradeEndpoints::try_official(rejected),
                Err(ExchangeError::InvalidRequest { .. })
            ),
            "trade endpoints must reject {rejected}"
        );
    }
}

#[test]
fn testnet_constructors_keep_rejecting_the_mainnet_host() {
    assert!(matches!(
        crypto_trading_exchange::BinanceTestnetEndpoints::try_official(
            "https://api.binance.com",
            "https://demo-fapi.binance.com",
        ),
        Err(ExchangeError::InvalidRequest { .. })
    ));
}

#[test]
fn mainnet_offline_profiles_accept_only_literal_loopback_hosts() {
    let read = BinanceMainnetReadEndpoints::loopback("http://127.0.0.1:41011").unwrap();
    assert_eq!(
        read.rest_url("/api/v3/account").unwrap().as_str(),
        "http://127.0.0.1:41011/api/v3/account"
    );
    let trade = BinanceMainnetTradeEndpoints::loopback("http://[::1]:41012").unwrap();
    assert_eq!(
        trade.rest_url("/api/v3/order").unwrap().as_str(),
        "http://[::1]:41012/api/v3/order"
    );

    for disallowed in [
        "http://localhost:41011",
        "http://example.com:41011",
        "http://127.0.0.1:41011/base",
        "http://user@127.0.0.1:41011",
        "https://api.binance.com",
    ] {
        assert!(
            BinanceMainnetReadEndpoints::loopback(disallowed).is_err(),
            "{disallowed} must not be accepted as a mainnet offline endpoint"
        );
        assert!(
            BinanceMainnetTradeEndpoints::loopback(disallowed).is_err(),
            "{disallowed} must not be accepted as a mainnet offline endpoint"
        );
    }
}

#[test]
fn mainnet_endpoint_joining_cannot_escape_the_selected_origin() {
    let read = BinanceMainnetReadEndpoints::official();
    let trade = BinanceMainnetTradeEndpoints::official();

    for invalid_path in [
        "https://testnet.binance.vision/api/v3/order",
        "//testnet.binance.vision/api/v3/order",
        "/../api/v3/order",
        "api/v3/order",
        "/api/v3/order?symbol=BTCUSDT",
    ] {
        assert!(
            read.rest_url(invalid_path).is_err(),
            "{invalid_path} must not escape the mainnet read origin"
        );
        assert!(
            trade.rest_url(invalid_path).is_err(),
            "{invalid_path} must not escape the mainnet trade origin"
        );
    }
}

#[test]
fn mainnet_websocket_profiles_use_exact_official_hosts_and_paths() {
    let market = BinanceMainnetSpotMarketStreamEndpoint::official();
    assert_eq!(
        market.stream_url("btcusdt@bookTicker").unwrap().as_str(),
        "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
    );

    let user_data = BinanceMainnetSpotUserDataStreamEndpoint::official();
    assert_eq!(
        user_data.websocket_url().unwrap().as_str(),
        "wss://ws-api.binance.com/ws-api/v3"
    );
}

#[test]
fn mainnet_websocket_profiles_reject_testnet_hosts_and_wrong_ports() {
    assert!(matches!(
        BinanceMainnetSpotMarketStreamEndpoint::try_official(
            "wss://stream.testnet.binance.vision:9443"
        ),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        BinanceMainnetSpotMarketStreamEndpoint::try_official("wss://stream.binance.com"),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        BinanceMainnetSpotMarketStreamEndpoint::try_official("wss://stream.binance.com:443"),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        BinanceMainnetSpotUserDataStreamEndpoint::try_official(
            "wss://ws-api.testnet.binance.vision"
        ),
        Err(ExchangeError::InvalidRequest { .. })
    ));

    let market = BinanceMainnetSpotMarketStreamEndpoint::official();
    assert!(market.stream_url("../btcusdt@bookTicker").is_err());
    assert!(market.stream_url("btcusdt@bookTicker?foo=bar").is_err());
}
