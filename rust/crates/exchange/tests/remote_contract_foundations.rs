use std::str::FromStr;

use crypto_trading_domain::{MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    ExchangeError, ExchangeSymbol, ExchangeSymbolCatalog, InstrumentRuleCatalog, InstrumentRules,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).unwrap()
}

fn money(value: &str) -> Money {
    Money::new(decimal(value))
}

#[test]
fn symbol_catalog_disambiguates_identical_wire_symbols_by_market_type() {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let perpetual = Symbol::new("BTC-USDC-PERP").unwrap();
    let catalog = ExchangeSymbolCatalog::new(vec![
        ExchangeSymbol::new("binance", spot.clone(), MarketType::Spot, "BTCUSDT").unwrap(),
        ExchangeSymbol::new(
            "binance",
            perpetual.clone(),
            MarketType::Perpetual,
            "BTCUSDT",
        )
        .unwrap(),
    ])
    .unwrap();

    assert_eq!(
        catalog.to_wire("BINANCE", &spot, MarketType::Spot).unwrap(),
        "BTCUSDT"
    );
    assert_eq!(
        catalog
            .to_standard("binance", "BTCUSDT", MarketType::Spot)
            .unwrap(),
        &spot
    );
    assert_eq!(
        catalog
            .to_standard("binance", "BTCUSDT", MarketType::Perpetual)
            .unwrap(),
        &perpetual
    );
}

#[test]
fn symbol_catalog_rejects_ambiguous_and_unbounded_inputs() {
    let standard = Symbol::new("BTC-USDC-PERP").unwrap();
    let mapping = ExchangeSymbol::new(
        "binance",
        standard.clone(),
        MarketType::Perpetual,
        "BTCUSDT",
    )
    .unwrap();
    let duplicate_wire = ExchangeSymbol::new(
        "binance",
        Symbol::new("XBT-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        "BTCUSDT",
    )
    .unwrap();

    assert!(matches!(
        ExchangeSymbolCatalog::new(vec![mapping.clone(), mapping]),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(matches!(
        ExchangeSymbolCatalog::new(vec![
            ExchangeSymbol::new("binance", standard, MarketType::Perpetual, "BTCUSDT").unwrap(),
            duplicate_wire,
        ]),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(
        ExchangeSymbol::new(
            "binance",
            Symbol::new("BTC").unwrap(),
            MarketType::Spot,
            " "
        )
        .is_err()
    );
}

#[test]
fn missing_reverse_symbol_mapping_is_an_invalid_remote_response() {
    let catalog = ExchangeSymbolCatalog::default();

    assert!(matches!(
        catalog
            .to_standard("binance", "UNKNOWN", MarketType::Spot)
            .unwrap_err(),
        ExchangeError::InvalidResponse { .. }
    ));
}

#[test]
fn strict_catalog_validation_reuses_exact_instrument_rules() {
    let symbol = Symbol::new("BTC-USDC-PERP").unwrap();
    let rules = InstrumentRules::new(
        "binance",
        symbol.clone(),
        MarketType::Perpetual,
        price("0.10"),
        quantity("0.001"),
        quantity("0.001"),
        money("5"),
    )
    .unwrap();
    let catalog = InstrumentRuleCatalog::new(vec![rules]).unwrap();

    let valid = OrderIntent::limit(
        "binance",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.002"),
        price("50000.10"),
    );
    catalog
        .validate_order(&valid, Some(price("50000.10")))
        .unwrap();

    let misaligned = OrderIntent::limit(
        "binance",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.0025"),
        price("50000.10"),
    );
    assert!(matches!(
        catalog
            .validate_order(&misaligned, Some(price("50000.10")))
            .unwrap_err(),
        ExchangeError::Rejected { .. }
    ));

    let missing = OrderIntent::market(
        "binance",
        Symbol::new("ETH-USDC-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.01"),
    );
    assert!(matches!(
        catalog
            .validate_order(&missing, Some(price("3000")))
            .unwrap_err(),
        ExchangeError::Rejected { .. }
    ));
}
