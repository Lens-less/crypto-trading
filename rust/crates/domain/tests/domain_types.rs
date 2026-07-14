use std::str::FromStr;

use chrono::Utc;
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Quantity, Symbol};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

#[test]
fn financial_types_preserve_decimal_text_without_binary_rounding() {
    let price: Price = serde_json::from_str(r#""0.10000001""#).unwrap();
    let quantity: Quantity = serde_json::from_str("0.00000001").unwrap();

    assert_eq!(price.as_decimal(), decimal("0.10000001"));
    assert_eq!(quantity.as_decimal(), decimal("0.00000001"));
    assert_eq!(serde_json::to_string(&price).unwrap(), r#""0.10000001""#);
}

#[test]
fn price_rejects_zero_at_every_domain_boundary() {
    assert!(Price::new(Decimal::ZERO).is_err());
    assert!(serde_json::from_str::<Price>(r#""0""#).is_err());
    assert!(serde_yaml::from_str::<Price>("0\n").is_err());

    // Zero remains a valid quantity representation for flat positions and
    // unfilled orders; order validation is responsible for requiring > 0.
    assert!(Quantity::new(Decimal::ZERO).is_ok());
}

#[test]
fn market_snapshot_reports_exact_spread_and_rejects_crossed_quotes() {
    let snapshot = MarketSnapshot::new(
        "lighter",
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("100.10")).unwrap(),
        Price::new(decimal("100.30")).unwrap(),
        Utc::now(),
    )
    .unwrap();

    assert_eq!(snapshot.spread(), decimal("0.20"));
    assert_eq!(snapshot.mid_price().as_decimal(), decimal("100.20"));

    let crossed = MarketSnapshot::new(
        "lighter",
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("100.30")).unwrap(),
        Price::new(decimal("100.10")).unwrap(),
        Utc::now(),
    );
    assert!(crossed.is_err());
}

#[test]
fn midpoint_does_not_overflow_for_equal_maximum_decimal_quotes() {
    let maximum = Price::new(Decimal::MAX).unwrap();
    let snapshot = MarketSnapshot::new(
        "paper",
        Symbol::new("MAX").unwrap(),
        MarketType::Perpetual,
        maximum,
        maximum,
        Utc::now(),
    )
    .unwrap();

    assert_eq!(snapshot.mid_price(), maximum);
}

#[test]
fn market_snapshot_deserialization_enforces_constructor_invariants() {
    let crossed = r#"{
        "exchange": "lighter",
        "symbol": "BTC",
        "market_type": "perpetual",
        "bid": "100.30",
        "ask": "100.10",
        "timestamp": "2026-07-14T00:00:00Z"
    }"#;
    assert!(serde_json::from_str::<MarketSnapshot>(crossed).is_err());

    let empty_exchange = r#"{
        "exchange": "   ",
        "symbol": "BTC",
        "market_type": "perpetual",
        "bid": "100.10",
        "ask": "100.30",
        "timestamp": "2026-07-14T00:00:00Z"
    }"#;
    assert!(serde_json::from_str::<MarketSnapshot>(empty_exchange).is_err());
}

#[test]
fn numeric_json_decimals_keep_all_supported_digits() {
    let price: Price = serde_json::from_str("0.1000000000000000000000000001").unwrap();

    assert_eq!(
        price.as_decimal(),
        decimal("0.1000000000000000000000000001")
    );
}

#[test]
fn legacy_yaml_numeric_decimals_remain_accepted() {
    let price: Price = serde_yaml::from_str("0.125\n").unwrap();

    assert_eq!(price.as_decimal(), decimal("0.125"));
}

#[test]
fn market_type_accepts_legacy_perpetual_aliases() {
    for alias in ["perp", "perpetual", "future", "futures"] {
        let yaml = format!("{alias}\n");
        let market_type: MarketType = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(market_type, MarketType::Perpetual);
    }
}
