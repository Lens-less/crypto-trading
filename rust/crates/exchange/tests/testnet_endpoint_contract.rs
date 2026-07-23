use crypto_trading_exchange::{
    BinanceProduct, BinanceTestnetEndpoints, ExchangeError, HyperliquidTestnetEndpoint,
};

#[test]
fn official_testnet_profiles_use_product_specific_hosts() {
    let binance = BinanceTestnetEndpoints::official();
    assert_eq!(
        binance
            .rest_url(BinanceProduct::Spot, "/api/v3/order")
            .unwrap()
            .as_str(),
        "https://testnet.binance.vision/api/v3/order"
    );
    assert_eq!(
        binance
            .rest_url(BinanceProduct::UsdM, "/fapi/v1/order")
            .unwrap()
            .as_str(),
        "https://demo-fapi.binance.com/fapi/v1/order"
    );

    let hyperliquid = HyperliquidTestnetEndpoint::official();
    assert_eq!(
        hyperliquid.rest_url("/info").unwrap().as_str(),
        "https://api.hyperliquid-testnet.xyz/info"
    );
    assert_eq!(
        hyperliquid.rest_url("/exchange").unwrap().as_str(),
        "https://api.hyperliquid-testnet.xyz/exchange"
    );
}

#[test]
fn custom_testnet_profiles_reject_mainnet_and_mixed_hosts() {
    assert!(matches!(
        BinanceTestnetEndpoints::try_official(
            "https://api.binance.com",
            "https://demo-fapi.binance.com",
        ),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        BinanceTestnetEndpoints::try_official(
            "https://testnet.binance.vision",
            "https://fapi.binance.com",
        ),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        HyperliquidTestnetEndpoint::try_official("https://api.hyperliquid.xyz"),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        BinanceTestnetEndpoints::try_official(
            "https://testnet.binance.vision:444",
            "https://demo-fapi.binance.com",
        ),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        HyperliquidTestnetEndpoint::try_official("https://api.hyperliquid-testnet.xyz:444"),
        Err(ExchangeError::InvalidRequest { .. })
    ));
}

#[test]
fn offline_profiles_accept_only_literal_loopback_hosts() {
    let binance =
        BinanceTestnetEndpoints::loopback("http://127.0.0.1:41001", "http://[::1]:41002").unwrap();
    assert_eq!(
        binance
            .rest_url(BinanceProduct::Spot, "/api/v3/order")
            .unwrap()
            .as_str(),
        "http://127.0.0.1:41001/api/v3/order"
    );
    assert_eq!(
        binance
            .rest_url(BinanceProduct::UsdM, "/fapi/v1/order")
            .unwrap()
            .as_str(),
        "http://[::1]:41002/fapi/v1/order"
    );

    let hyperliquid = HyperliquidTestnetEndpoint::loopback("http://127.0.0.1:41003").unwrap();
    assert_eq!(
        hyperliquid.rest_url("/info").unwrap().as_str(),
        "http://127.0.0.1:41003/info"
    );

    for disallowed in [
        "http://localhost:41001",
        "http://example.com:41001",
        "http://127.0.0.1:41001/base",
        "http://user@127.0.0.1:41001",
    ] {
        assert!(
            BinanceTestnetEndpoints::loopback(disallowed, "http://127.0.0.1:41002").is_err(),
            "{disallowed} must not be accepted as an offline endpoint"
        );
        assert!(
            HyperliquidTestnetEndpoint::loopback(disallowed).is_err(),
            "{disallowed} must not be accepted as an offline endpoint"
        );
    }
}

#[test]
fn endpoint_joining_cannot_escape_the_selected_origin() {
    let endpoints = BinanceTestnetEndpoints::official();

    for invalid_path in [
        "https://api.binance.com/api/v3/order",
        "//api.binance.com/api/v3/order",
        "/../api/v3/order",
        "api/v3/order",
    ] {
        assert!(
            endpoints
                .rest_url(BinanceProduct::Spot, invalid_path)
                .is_err(),
            "{invalid_path} must not escape or weaken the endpoint profile"
        );
    }
}
