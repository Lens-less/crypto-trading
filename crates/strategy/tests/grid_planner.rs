use std::str::FromStr;

use crypto_trading_domain::{MarketType, Price, Quantity, Side, Symbol};
use crypto_trading_strategy::{GridDirection, GridPlanConfig, GridPlanner, GridRange};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).expect("test price must be positive")
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).expect("test quantity must be positive")
}

#[test]
fn fixed_long_grid_derives_legacy_levels_and_martingale_quantities() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Fixed {
            lower: price("100"),
            upper: price("104"),
        },
        interval: decimal("1"),
        quantity: quantity("1"),
        martingale_increment: Some(decimal("0.1")),
    })
    .unwrap();

    let levels = planner.fixed_levels().unwrap();
    let prices: Vec<_> = levels
        .iter()
        .map(|level| level.price.as_decimal())
        .collect();
    let quantities: Vec<_> = levels
        .iter()
        .map(|level| level.quantity.as_decimal())
        .collect();

    assert_eq!(
        prices,
        [
            decimal("100"),
            decimal("101"),
            decimal("102"),
            decimal("103")
        ]
    );
    assert_eq!(
        quantities,
        [decimal("1.3"), decimal("1.2"), decimal("1.1"), decimal("1")]
    );
    assert!(levels.iter().all(|level| level.side == Side::Buy));
}
