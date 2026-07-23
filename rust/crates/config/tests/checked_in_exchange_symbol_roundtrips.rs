use crypto_trading_config::load_symbol_conversions_from_str;
use crypto_trading_domain::MarketType;

#[test]
fn checked_in_binance_and_hyperliquid_mappings_round_trip_known_symbols() {
    let mappings =
        load_symbol_conversions_from_str(include_str!("../../../config/symbol_conversion.yaml"))
            .unwrap();

    for (exchange, standard, market_type, wire) in [
        ("binance", "BTC-USDC-SPOT", MarketType::Spot, "BTCUSDT"),
        ("binance", "BTC-USDC-PERP", MarketType::Perpetual, "BTCUSDT"),
        ("binance", "ETH-USDC-PERP", MarketType::Perpetual, "ETHUSDT"),
        (
            "hyperliquid",
            "BTC-USDC-PERP",
            MarketType::Perpetual,
            "BTC/USDC:USDC",
        ),
        ("hyperliquid", "ETH-USDC-SPOT", MarketType::Spot, "ETH/USDC"),
    ] {
        assert_eq!(
            mappings.resolve(exchange, standard, market_type).as_deref(),
            Some(wire),
            "{exchange}/{standard} forward mapping"
        );
        assert_eq!(
            mappings.to_standard(exchange, wire, market_type).as_deref(),
            Some(standard),
            "{exchange}/{wire} reverse mapping"
        );
    }
}
