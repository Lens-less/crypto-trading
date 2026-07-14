use std::str::FromStr;

use crypto_trading_config::{load_price_alert_config_from_str, load_volume_maker_config_from_str};
use rust_decimal::Decimal;

#[test]
fn existing_price_alert_document_preserves_per_symbol_thresholds() {
    let config = load_price_alert_config_from_str(include_str!(
        "../../../config/price_alert/binance_alert.yaml"
    ))
    .unwrap();

    assert_eq!(config.exchange, "binance");
    assert_eq!(config.cooldown_seconds, 30);
    let bitcoin = &config.symbols[0];
    assert_eq!(bitcoin.symbol.as_str(), "BTC/USDT");
    assert_eq!(
        bitcoin.price_alert.upper_price.unwrap().as_decimal(),
        Decimal::from(120_000)
    );
}

#[test]
fn existing_volume_document_uses_cycle_interval_when_legacy_delay_is_also_present() {
    let config = load_volume_maker_config_from_str(include_str!(
        "../../../config/volume_maker/lighter_volume_maker.yaml"
    ))
    .unwrap();

    assert_eq!(config.exchange, "lighter");
    assert_eq!(config.order_mode, "market");
    assert!(!config.reverse_trading);
    assert!(!config.use_post_only);
    assert_eq!(
        config.order_quantity.as_decimal(),
        Decimal::from_str("0.005").unwrap()
    );
    assert_eq!(config.interval_seconds, Decimal::TEN);
}

#[test]
fn volume_maker_rejects_invalid_mode_and_negative_interval() {
    for yaml in [
        r"
volume_maker:
  exchange: paper
  symbol: BTC-USDC-PERP
  order_quantity: 1
  order_mode: definitely-not-a-mode
",
        r"
volume_maker:
  exchange: paper
  symbol: BTC-USDC-PERP
  order_quantity: 1
  order_mode: limit
  interval_seconds: -1
",
    ] {
        assert!(load_volume_maker_config_from_str(yaml).is_err(), "{yaml}");
    }
}

#[test]
fn volume_maker_emergency_stop_blocks_runtime_execution() {
    let config = load_volume_maker_config_from_str(
        r"
volume_maker:
  exchange: paper
  symbol: BTC-USDC-PERP
  order_quantity: 1
  order_mode: limit
  interval_seconds: 1
  emergency_stop: true
",
    )
    .unwrap();

    let error = config.validate_execution_controls().unwrap_err();
    assert!(error.to_string().contains("emergency stop"));
}

#[test]
fn volume_maker_missing_emergency_stop_fails_closed() {
    let config = load_volume_maker_config_from_str(
        r"
volume_maker:
  exchange: paper
  symbol: BTC-USDC-PERP
  order_quantity: 1
  order_mode: limit
  interval_seconds: 1
",
    )
    .unwrap();

    assert!(config.emergency_stop);
    assert!(config.validate_execution_controls().is_err());
}
