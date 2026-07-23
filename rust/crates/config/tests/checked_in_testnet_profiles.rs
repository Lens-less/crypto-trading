use serde_yaml::Value;

fn profile(relative: &str) -> Value {
    serde_yaml::from_str(match relative {
        "binance" => include_str!("../../../config/exchanges/binance_testnet.yaml"),
        "hyperliquid" => include_str!("../../../config/exchanges/hyperliquid_testnet.yaml"),
        _ => unreachable!("test profile must be known"),
    })
    .unwrap()
}

#[test]
fn checked_in_remote_profiles_are_testnet_only_and_secret_free() {
    let binance = profile("binance");
    assert_eq!(binance["binance"]["environment"], "testnet");
    assert_eq!(
        binance["binance"]["api"]["spot_rest_url"],
        "https://testnet.binance.vision"
    );
    assert_eq!(
        binance["binance"]["api"]["usdm_rest_url"],
        "https://demo-fapi.binance.com"
    );
    assert_eq!(binance["binance"]["authentication"]["api_key"], "");
    assert_eq!(binance["binance"]["authentication"]["api_secret"], "");

    let hyperliquid = profile("hyperliquid");
    assert_eq!(hyperliquid["hyperliquid"]["environment"], "testnet");
    assert_eq!(
        hyperliquid["hyperliquid"]["api"]["rest_url"],
        "https://api.hyperliquid-testnet.xyz"
    );
    assert_eq!(
        hyperliquid["hyperliquid"]["authentication"]["private_key"],
        ""
    );
    assert_eq!(
        hyperliquid["hyperliquid"]["authentication"]["wallet_address"],
        ""
    );
}
