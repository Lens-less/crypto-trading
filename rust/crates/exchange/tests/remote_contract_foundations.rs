use std::str::FromStr;

use crypto_trading_domain::{MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol};
use crypto_trading_exchange::{
    ExchangeError, ExchangeSymbol, ExchangeSymbolCatalog, InstrumentRuleCatalog,
    InstrumentRuleOptions, InstrumentRules,
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

#[test]
fn market_orders_use_market_lot_size_instead_of_limit_lot_size() {
    let symbol = Symbol::new("BTC-USDC-SPOT").unwrap();
    let rules = InstrumentRules::with_options(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        price("0.01"),
        quantity("0.0001"),
        quantity("0.0001"),
        InstrumentRuleOptions {
            market_quantity_step: Some(quantity("0.001")),
            market_min_quantity: Some(quantity("0.001")),
            market_max_quantity: Some(quantity("1")),
            ..InstrumentRuleOptions::new(money("10"))
        },
    )
    .unwrap();
    let catalog = InstrumentRuleCatalog::new(vec![rules]).unwrap();

    let limit = OrderIntent::limit(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        quantity("0.0005"),
        price("50000.01"),
    );
    catalog
        .validate_order(&limit, Some(price("50000.01")))
        .unwrap();

    let market = OrderIntent::market(
        "binance",
        symbol,
        MarketType::Spot,
        Side::Buy,
        quantity("0.0015"),
    );
    let error = catalog
        .validate_order(&market, Some(price("50000.01")))
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Rejected { .. }));
    assert!(
        error.to_string().contains("aligned to step 0.001"),
        "{error}"
    );
}

#[test]
fn notional_flags_can_skip_market_minimum_but_keep_market_maximum() {
    let symbol = Symbol::new("BTC-USDC-SPOT").unwrap();
    let rules = InstrumentRules::with_options(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        price("0.01"),
        quantity("0.001"),
        quantity("0.001"),
        InstrumentRuleOptions {
            max_notional: Some(money("20")),
            apply_min_notional_to_market: false,
            apply_max_notional_to_market: true,
            ..InstrumentRuleOptions::new(money("10"))
        },
    )
    .unwrap();
    let catalog = InstrumentRuleCatalog::new(vec![rules]).unwrap();

    let market_below_min = OrderIntent::market(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        quantity("1"),
    );
    catalog
        .validate_order(&market_below_min, Some(price("5")))
        .unwrap();

    let limit_below_min = OrderIntent::limit(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        quantity("1"),
        price("5"),
    );
    assert!(matches!(
        catalog
            .validate_order(&limit_below_min, Some(price("5")))
            .unwrap_err(),
        ExchangeError::Rejected { .. }
    ));

    let market_above_max = OrderIntent::market(
        "binance",
        symbol,
        MarketType::Spot,
        Side::Buy,
        quantity("5"),
    );
    let error = catalog
        .validate_order(&market_above_max, Some(price("5")))
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Rejected { .. }));
    assert!(error.to_string().contains("exceeds maximum"), "{error}");
}
