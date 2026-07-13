use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_domain::{Price, Symbol};
use crypto_trading_strategy::{
    AprCalculator, GridFill, Rating, RatingGrade, VirtualGrid, VirtualGridConfig,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

#[test]
fn virtual_grid_matches_legacy_two_sided_cycle_golden_path() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let mut grid = VirtualGrid::new(
        VirtualGridConfig {
            symbol: Symbol::new("BTC").unwrap(),
            initial_price: price("100"),
            grid_width_percent: decimal("10"),
            grid_interval_percent: decimal("1"),
        },
        started_at,
    )
    .unwrap();

    assert_eq!(grid.lower_price().as_decimal(), decimal("95"));
    assert_eq!(grid.upper_price().as_decimal(), decimal("105"));
    assert_eq!(grid.grid_count(), 10);
    assert_eq!(grid.pending_buy_price().as_decimal(), decimal("99"));
    assert_eq!(grid.pending_sell_price().as_decimal(), decimal("101"));

    assert_eq!(
        grid.update_price_at(price("99"), started_at + Duration::seconds(10))
            .unwrap(),
        Some(GridFill::Buy)
    );
    assert_eq!(grid.pending_sell_price().as_decimal(), decimal("100"));
    assert_eq!(
        grid.update_price_at(price("100"), started_at + Duration::seconds(20))
            .unwrap(),
        Some(GridFill::Sell)
    );
    assert_eq!(grid.buy_crosses(), 1);
    assert_eq!(grid.sell_crosses(), 1);
    assert_eq!(grid.complete_cycles(), 1);
}

#[test]
fn apr_and_rating_match_legacy_golden_values() {
    let apr = AprCalculator::annualized(decimal("0.5"), decimal("10"), decimal("10")).unwrap();
    assert_eq!(apr, decimal("2172.4800"));
    assert_eq!(
        AprCalculator::total_capital(decimal("10"), decimal("0.5")).unwrap(),
        decimal("200")
    );
    assert_eq!(
        AprCalculator::profit_per_cycle(decimal("0.5")),
        decimal("0.04960")
    );

    let thresholds = [
        ("500", RatingGrade::S),
        ("300", RatingGrade::A),
        ("150", RatingGrade::B),
        ("50", RatingGrade::C),
        ("49.999", RatingGrade::D),
    ];
    for (value, grade) in thresholds {
        assert_eq!(
            Rating::calculate(decimal(value), decimal("10"), decimal("1000000")).grade,
            grade
        );
    }

    let best = Rating::calculate(decimal("500"), decimal("51"), decimal("10000000"));
    assert_eq!(best.grade, RatingGrade::S);
    assert_eq!(best.score, decimal("100"));
    let weak = Rating::calculate(decimal("49"), decimal("4"), decimal("499999"));
    assert_eq!(weak.grade, RatingGrade::D);
    assert_eq!(weak.score, decimal("20"));
}
