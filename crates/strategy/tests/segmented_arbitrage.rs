use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Quantity, Side, Symbol};
use crypto_trading_strategy::{
    ArbitrageDecisionKind, ArbitrageState, ArbitrageStrategy, PairStrategyMachine,
    SegmentedArbitrageConfig, SpreadCalculator,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).unwrap()
}

fn snapshot(exchange: &str, bid: &str, ask: &str) -> MarketSnapshot {
    MarketSnapshot::new(
        exchange,
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        price(bid),
        price(ask),
        Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap(),
    )
    .unwrap()
}

fn config() -> SegmentedArbitrageConfig {
    SegmentedArbitrageConfig {
        initial_spread_percent: decimal("0.5"),
        grid_step_percent: decimal("0.5"),
        max_segments: 3,
        base_quantity: quantity("2"),
        first_close_ratio: decimal("0.4"),
    }
}

#[test]
fn cross_exchange_spread_uses_buy_ask_and_sell_bid_exactly() {
    let left = snapshot("left", "99", "100");
    let right = snapshot("right", "101", "102");

    let directions = SpreadCalculator::directions(&left, &right).unwrap();

    assert_eq!(directions[0].buy_exchange, "left");
    assert_eq!(directions[0].sell_exchange, "right");
    assert_eq!(directions[0].absolute, decimal("1"));
    assert_eq!(directions[0].percent, decimal("1"));
    assert_eq!(directions[1].absolute, decimal("-3"));
}

#[test]
fn segmented_decision_opens_to_target_then_reduces_with_hysteresis() {
    let strategy = ArbitrageStrategy::new(config()).unwrap();
    let left = snapshot("left", "99", "100");
    let right = snapshot("right", "102", "103");

    let opened = strategy
        .evaluate_pair(&ArbitrageState::default(), &left, &right)
        .unwrap();

    assert_eq!(opened.kind, ArbitrageDecisionKind::Open);
    assert_eq!(opened.segment, 3);
    assert_eq!(opened.target_quantity, decimal("6"));
    assert_eq!(opened.intents.len(), 2);
    assert_eq!(opened.intents[0].side, Side::Buy);
    assert_eq!(opened.intents[1].side, Side::Sell);
    assert_eq!(opened.intents[0].quantity.as_decimal(), decimal("6"));

    let state = ArbitrageState {
        position_quantity: decimal("6"),
        direction: Some(opened.direction.clone().unwrap()),
    };
    let contracted_left = snapshot("left", "99.5", "100");
    let contracted_right = snapshot("right", "100.75", "101");
    let reduced = strategy
        .evaluate_pair(&state, &contracted_left, &contracted_right)
        .unwrap();

    assert_eq!(reduced.kind, ArbitrageDecisionKind::Reduce);
    assert_eq!(reduced.segment, 2);
    assert_eq!(reduced.target_quantity, decimal("4"));
    assert_eq!(reduced.delta_quantity, decimal("2"));
    assert!(reduced.intents.iter().all(|intent| intent.reduce_only));
    assert_eq!(reduced.intents[0].exchange, "right");
    assert_eq!(reduced.intents[0].side, Side::Buy);
    assert_eq!(reduced.intents[1].exchange, "left");
    assert_eq!(reduced.intents[1].side, Side::Sell);
}
