use crypto_trading_config::{SymbolConversions, load_symbol_conversions_from_str};

#[test]
fn standard_symbols_map_to_exchange_formats() {
    let mappings = SymbolConversions::default();

    assert_eq!(
        mappings.resolve("backpack", "BTC-USDC-PERP"),
        "BTC_USDC_PERP"
    );
    assert_eq!(mappings.resolve("lighter", "BTC-USDC-PERP"), "BTC");
    assert_eq!(mappings.resolve("edgex", "BTC-USDC-PERP"), "BTCUSD");
    assert_eq!(mappings.resolve("paradex", "BTC-USDC-PERP"), "BTC-USD-PERP");
}

#[test]
fn explicit_yaml_mapping_overrides_the_standard_rule() {
    let mappings = load_symbol_conversions_from_str(
        r"
conversions:
  BTC-USDC-PERP:
    lighter: XBT
symbol_mappings:
  paradex:
    ETH-USDC-PERP: ETH-CUSTOM-PERP
unknown_extension: true
",
    )
    .unwrap();

    assert_eq!(mappings.resolve("lighter", "BTC-USDC-PERP"), "XBT");
    assert_eq!(
        mappings.resolve("paradex", "ETH-USDC-PERP"),
        "ETH-CUSTOM-PERP"
    );
}

#[test]
fn checked_in_symbol_mapping_loads_and_keeps_explicit_precedence() {
    let mappings =
        load_symbol_conversions_from_str(include_str!("../../../config/symbol_conversion.yaml"))
            .unwrap();

    assert_eq!(mappings.resolve("lighter", "PAXG-USD-PERP"), "PAXG");
    assert_eq!(
        mappings.to_standard("lighter", "BTC"),
        Some("BTC-USDC-PERP".into())
    );
}
