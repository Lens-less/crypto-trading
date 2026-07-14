use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Quantity, Side, Symbol};
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

fn snapshot(market_type: MarketType) -> MarketSnapshot {
    MarketSnapshot::new(
        "paper",
        Symbol::new("BTC").unwrap(),
        market_type,
        price("100"),
        price("101"),
        Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap(),
    )
    .unwrap()
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

#[test]
fn grid_rejects_a_snapshot_for_another_market_type() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Follow {
            level_count: 2,
            price_offset_levels: 0,
        },
        interval: decimal("1"),
        quantity: quantity("1"),
        martingale_increment: None,
    })
    .unwrap();

    assert!(planner.intents(&snapshot(MarketType::Spot)).is_err());
}

#[test]
fn grid_rejects_level_counts_above_the_business_limit() {
    let config = GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Follow {
            level_count: 10_001,
            price_offset_levels: 0,
        },
        interval: Decimal::ONE,
        quantity: quantity("1"),
        martingale_increment: None,
    };

    assert!(GridPlanner::new(config).is_err());
}

#[test]
fn grid_returns_an_error_when_follow_bounds_overflow() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Follow {
            level_count: 2,
            price_offset_levels: 0,
        },
        interval: Decimal::MAX,
        quantity: quantity("1"),
        martingale_increment: None,
    })
    .unwrap();

    assert!(planner.levels(&snapshot(MarketType::Perpetual)).is_err());
}

#[test]
fn grid_returns_an_error_when_martingale_quantity_overflows() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Fixed {
            lower: price("100"),
            upper: price("102"),
        },
        interval: Decimal::ONE,
        quantity: Quantity::new(Decimal::MAX).unwrap(),
        martingale_increment: Some(Decimal::MAX),
    })
    .unwrap();

    assert!(planner.fixed_levels().is_err());
}

#[test]
fn follow_grid_handles_extreme_but_valid_mid_prices_without_panicking() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Follow {
            level_count: 2,
            price_offset_levels: 0,
        },
        interval: Decimal::ONE,
        quantity: quantity("1"),
        martingale_increment: None,
    })
    .unwrap();
    let extreme = MarketSnapshot::new(
        "paper",
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        price("0.0000000000000000000000000001"),
        Price::new(Decimal::MAX).unwrap(),
        Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap(),
    )
    .unwrap();

    assert!(planner.levels(&extreme).is_ok());
}
