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

// Golden vector extracted from the frozen Python engine
// (`archive/python-legacy/core/services/grid/models/grid_config.py:557-564`):
// long martingale quantity per level is
// `order_amount + (grid_count - grid_index) * martingale_increment`, with
// Grid 1 at the lower bound (`grid_config.py:307-310`) buying the most.
#[test]
fn martingale_long_matches_the_legacy_ten_level_quantity_ladder() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Long,
        range: GridRange::Fixed {
            lower: price("100"),
            upper: price("110"),
        },
        interval: decimal("1"),
        quantity: quantity("0.5"),
        martingale_increment: Some(decimal("0.05")),
    })
    .unwrap();

    let levels = planner.fixed_levels().unwrap();
    let expected: Vec<(Decimal, Decimal)> = [
        ("100", "0.95"),
        ("101", "0.90"),
        ("102", "0.85"),
        ("103", "0.80"),
        ("104", "0.75"),
        ("105", "0.70"),
        ("106", "0.65"),
        ("107", "0.60"),
        ("108", "0.55"),
        ("109", "0.50"),
    ]
    .iter()
    .map(|(price, quantity)| (decimal(price), decimal(quantity)))
    .collect();

    assert_eq!(
        levels
            .iter()
            .map(|level| (level.price.as_decimal(), level.quantity.as_decimal()))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(levels.iter().all(|level| level.side == Side::Buy));
}

// Golden vector for the legacy engine's DOCUMENTED short-martingale intent
// (`archive/python-legacy/core/services/grid/models/grid_config.py:565-569`:
// "价格越高（grid_index 越大），数量越多" — higher price sells more). The
// legacy formula `order_amount + (grid_index - 1) * martingale_increment`
// delivered the opposite because Grid 1 is the highest short price
// (`grid_config.py:311-314`); this port deliberately deviates from that
// buggy behavior so the largest quantity sits at the most adverse price,
// mirroring the long ladder.
#[test]
fn martingale_short_sizes_largest_at_the_most_adverse_price() {
    let planner = GridPlanner::new(GridPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        direction: GridDirection::Short,
        range: GridRange::Fixed {
            lower: price("100"),
            upper: price("110"),
        },
        interval: decimal("1"),
        quantity: quantity("0.5"),
        martingale_increment: Some(decimal("0.05")),
    })
    .unwrap();

    let levels = planner.fixed_levels().unwrap();
    let expected: Vec<(Decimal, Decimal)> = [
        ("110", "0.95"),
        ("109", "0.90"),
        ("108", "0.85"),
        ("107", "0.80"),
        ("106", "0.75"),
        ("105", "0.70"),
        ("104", "0.65"),
        ("103", "0.60"),
        ("102", "0.55"),
        ("101", "0.50"),
    ]
    .iter()
    .map(|(price, quantity)| (decimal(price), decimal(quantity)))
    .collect();

    assert_eq!(
        levels
            .iter()
            .map(|level| (level.price.as_decimal(), level.quantity.as_decimal()))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(levels.iter().all(|level| level.side == Side::Sell));
}

#[test]
fn grid_rejects_a_zero_martingale_increment() {
    let config = GridPlanConfig {
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
        martingale_increment: Some(Decimal::ZERO),
    };

    assert!(GridPlanner::new(config).is_err());
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
