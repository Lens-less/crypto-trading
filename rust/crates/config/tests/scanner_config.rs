use std::fmt::Write as _;

use crypto_trading_config::{
    ConfigError, MAX_SCANNER_CONFIG_SYMBOLS, load_scanner_config_from_str,
};
use crypto_trading_domain::MarketType;
use rust_decimal::Decimal;

#[test]
fn checked_in_scanner_document_preserves_grid_geometry_and_enablement() {
    let config =
        load_scanner_config_from_str(include_str!("../../../config/scanner/binance_scanner.yaml"))
            .unwrap();

    assert_eq!(config.exchange, "binance");
    assert_eq!(config.apr_window_seconds, 360);
    assert_eq!(config.apr_estimate.order_notional_usdc, Decimal::from(100));
    assert_eq!(
        config.apr_estimate.round_trip_fee_percent,
        Decimal::new(2, 1)
    );
    assert_eq!(config.min_complete_cycles, 0);
    assert_eq!(config.row_limit, 50);
    assert_eq!(config.symbols.len(), 2);
    assert_eq!(config.enabled_symbols().count(), 1);

    let bitcoin = &config.symbols[0];
    assert_eq!(bitcoin.symbol.as_str(), "BTC/USDT");
    assert_eq!(bitcoin.market_type, MarketType::Spot);
    assert!(bitcoin.enabled);
    assert!(bitcoin.benchmark);
    assert_eq!(bitcoin.grid_width_percent, Decimal::TEN);
    assert_eq!(bitcoin.grid_interval_percent, Decimal::ONE);
    assert_eq!(bitcoin.volume_24h_usdc, Decimal::from(1_000_000));
    assert_eq!(bitcoin.price_change_24h_percent, Some(Decimal::new(25, 1)));

    let ether = &config.symbols[1];
    assert!(!ether.enabled);
    assert!(!ether.benchmark);
    assert_eq!(ether.price_change_24h_percent, None);
}

#[test]
fn scanner_defaults_stay_bounded_and_explicit() {
    let config = load_scanner_config_from_str(
        r#"
scanner:
  exchange: binance
  scan:
    apr_estimate:
      order_notional_usdc: 100
      round_trip_fee_percent: 0.2
  symbols:
    - symbol: "BTC/USDT"
      grid:
        width_percent: 10
        interval_percent: 1
"#,
    )
    .unwrap();

    assert_eq!(config.apr_window_seconds, 300);
    assert_eq!(config.apr_estimate.order_notional_usdc, Decimal::from(100));
    assert_eq!(
        config.apr_estimate.round_trip_fee_percent,
        Decimal::new(2, 1)
    );
    assert_eq!(config.min_complete_cycles, 0);
    assert_eq!(config.row_limit, 50);
    let symbol = &config.symbols[0];
    assert!(symbol.enabled);
    assert!(!symbol.benchmark);
    // The scanner schema inherits the domain-wide perpetual-first default.
    assert_eq!(symbol.market_type, MarketType::Perpetual);
    assert_eq!(symbol.volume_24h_usdc, Decimal::ZERO);
}

#[test]
fn scanner_requires_explicit_apr_estimate_assumptions() {
    let error = load_scanner_config_from_str(
        r#"
scanner:
  exchange: binance
  scan: {}
  symbols:
    - symbol: "BTC/USDT"
      grid:
        width_percent: 10
        interval_percent: 1
"#,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ConfigError::MissingRequiredField {
            path: "scanner.scan.apr_estimate"
        }
    ));
}

#[test]
fn scanner_schema_fails_closed_for_invalid_documents() {
    let cases: &[(&str, &str)] = &[
        (
            "scanner:\n  exchange: \"   \"\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n",
            "exchange must not be empty",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols: []\n",
            "at least one symbol",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      enabled: false\n      grid: {width_percent: 10, interval_percent: 1}\n",
            "at least one enabled symbol",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n",
            "must not repeat",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 0, interval_percent: 1}\n",
            "width_percent must be positive",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 0}\n",
            "interval_percent must be positive",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 1, interval_percent: 1}\n",
            "fit twice inside",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n      volume_24h_usdc: -1\n",
            "must not be negative",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {apr_window_seconds: 0, apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n",
            "apr_window_seconds",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {row_limit: 0, apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n",
            "row_limit",
        ),
        (
            "scanner:\n  exchange: binance\n  scan: {row_limit: 129, apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n    - symbol: BTC/USDT\n      grid: {width_percent: 10, interval_percent: 1}\n",
            "row_limit",
        ),
    ];

    for (yaml, expected) in cases {
        let error = load_scanner_config_from_str(yaml).unwrap_err();
        assert!(
            matches!(&error, ConfigError::Validation(message) if message.contains(expected)),
            "{yaml}: {error}"
        );
    }
}

#[test]
fn scanner_symbol_universe_is_bounded() {
    let mut yaml = String::from(
        "scanner:\n  exchange: binance\n  scan: {apr_estimate: {order_notional_usdc: 100, round_trip_fee_percent: 0.2}}\n  symbols:\n",
    );
    for index in 0..=MAX_SCANNER_CONFIG_SYMBOLS {
        writeln!(
            yaml,
            "    - symbol: SYM{index}/USDT\n      grid: {{width_percent: 10, interval_percent: 1}}"
        )
        .unwrap();
    }

    let error = load_scanner_config_from_str(&yaml).unwrap_err();
    assert!(error.to_string().contains("at most 128 symbols"), "{error}");
}
