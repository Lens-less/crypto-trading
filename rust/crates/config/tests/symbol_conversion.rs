use std::fmt::Write as _;

use crypto_trading_config::{SymbolConversions, load_symbol_conversions_from_str};
use crypto_trading_domain::MarketType;

#[test]
fn uncatalogued_symbols_fail_closed_in_both_directions() {
    let mappings = SymbolConversions::default();

    assert_eq!(
        mappings.resolve("backpack", "BTC-USDC-PERP", MarketType::Perpetual),
        None
    );
    assert_eq!(
        mappings.to_standard("binance", "BTCUSDT", MarketType::Spot),
        None
    );
}

#[test]
fn explicit_yaml_mapping_is_the_only_resolution_source() {
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

    assert_eq!(
        mappings.resolve("lighter", "BTC-USDC-PERP", MarketType::Perpetual),
        Some("XBT".into())
    );
    assert_eq!(
        mappings.resolve("paradex", "ETH-USDC-PERP", MarketType::Perpetual),
        Some("ETH-CUSTOM-PERP".into())
    );
    assert_eq!(
        mappings.resolve("lighter", "ETH-USDC-PERP", MarketType::Perpetual),
        None
    );
}

#[test]
fn checked_in_symbol_mapping_loads_and_keeps_explicit_precedence() {
    let mappings =
        load_symbol_conversions_from_str(include_str!("../../../config/symbol_conversion.yaml"))
            .unwrap();

    assert_eq!(
        mappings.resolve("lighter", "PAXG-USD-PERP", MarketType::Perpetual),
        Some("PAXG".into())
    );
    assert_eq!(
        mappings.to_standard("lighter", "BTC", MarketType::Perpetual),
        Some("BTC-USDC-PERP".into())
    );
}

#[test]
fn exchange_to_standard_lookups_are_case_insensitive_for_explicit_entries() {
    let mappings = load_symbol_conversions_from_str(
        r"
symbol_mappings:
  exchange_to_standard:
    lighter:
      xbt: BTC-USDC-PERP
    backpack:
      sol_usdc: SOL-USDC-SPOT
",
    )
    .unwrap();

    assert_eq!(
        mappings.to_standard("lighter", "XBT", MarketType::Perpetual),
        Some("BTC-USDC-PERP".into())
    );
    assert_eq!(
        mappings.to_standard("lighter", "xbt", MarketType::Perpetual),
        Some("BTC-USDC-PERP".into())
    );
    assert_eq!(
        mappings.to_standard("backpack", "SoL_UsDc", MarketType::Spot),
        Some("SOL-USDC-SPOT".into())
    );
    assert_eq!(
        mappings.to_standard("lighter", "btc", MarketType::Perpetual),
        None
    );
    assert_eq!(
        mappings.to_standard("backpack", "btc_usdc_perp", MarketType::Perpetual),
        None
    );
    assert_eq!(
        mappings.to_standard("paradex", "btc-usd-perp", MarketType::Perpetual),
        None
    );
}

#[test]
fn one_wire_symbol_can_be_catalogued_for_spot_and_perpetual() {
    let mappings = load_symbol_conversions_from_str(
        r"
symbol_mappings:
  standard_to_exchange:
    binance:
      BTC-USDC-SPOT: BTCUSDT
      BTC-USDC-PERP: BTCUSDT
",
    )
    .unwrap();

    assert_eq!(
        mappings.resolve("binance", "BTC-USDC-SPOT", MarketType::Spot),
        Some("BTCUSDT".into())
    );
    assert_eq!(
        mappings.resolve("binance", "BTC-USDC-PERP", MarketType::Perpetual),
        Some("BTCUSDT".into())
    );
    assert_eq!(
        mappings.to_standard("binance", "BTCUSDT", MarketType::Spot),
        Some("BTC-USDC-SPOT".into())
    );
    assert_eq!(
        mappings.to_standard("binance", "BTCUSDT", MarketType::Perpetual),
        Some("BTC-USDC-PERP".into())
    );
}

#[test]
fn requested_market_must_match_the_catalogued_standard_symbol() {
    let mappings = load_symbol_conversions_from_str(
        r"
conversions:
  BTC-USDC-SPOT:
    binance: BTCUSDT
",
    )
    .unwrap();

    assert_eq!(
        mappings.resolve("binance", "BTC-USDC-SPOT", MarketType::Perpetual),
        None
    );
    assert_eq!(
        mappings.to_standard("binance", "BTCUSDT", MarketType::Perpetual),
        None
    );
}

#[test]
fn conflicting_reverse_mapping_is_rejected() {
    let error = load_symbol_conversions_from_str(
        r"
symbol_mappings:
  standard_to_exchange:
    binance:
      BTC-USDC-SPOT: BTCUSDT
  exchange_to_standard:
    binance:
      BTCUSDT: ETH-USDC-SPOT
",
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("ambiguous reverse mapping"),
        "{error}"
    );
}

#[test]
fn conversion_catalog_rejects_more_than_ten_thousand_entries() {
    let mut yaml = String::from("symbol_mappings:\n  standard_to_exchange:\n    binance:\n");
    for index in 0..=10_000 {
        writeln!(yaml, "      A{index}-USDC-PERP: A{index}USDT").unwrap();
    }

    let error = load_symbol_conversions_from_str(&yaml).unwrap_err();

    assert!(error.to_string().contains("exceeds 10000"), "{error}");
}
