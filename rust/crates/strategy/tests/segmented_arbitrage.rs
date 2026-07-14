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

fn snapshot_for_market(
    exchange: &str,
    market_type: MarketType,
    bid: &str,
    ask: &str,
) -> MarketSnapshot {
    MarketSnapshot::new(
        exchange,
        Symbol::new("BTC").unwrap(),
        market_type,
        price(bid),
        price(ask),
        Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap(),
    )
    .unwrap()
}

fn snapshot(exchange: &str, bid: &str, ask: &str) -> MarketSnapshot {
    snapshot_for_market(exchange, MarketType::Perpetual, bid, ask)
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

#[test]
fn locked_direction_rejects_snapshots_for_different_market_types() {
    let strategy = ArbitrageStrategy::new(config()).unwrap();
    let left = snapshot("left", "99", "100");
    let right = snapshot("right", "102", "103");
    let opened = strategy
        .evaluate_pair(&ArbitrageState::default(), &left, &right)
        .unwrap();
    let direction = opened.direction.clone().unwrap();
    assert_eq!(direction.buy_market_type, MarketType::Perpetual);
    assert_eq!(direction.sell_market_type, MarketType::Perpetual);

    let state = ArbitrageState {
        position_quantity: opened.target_quantity,
        direction: Some(direction),
    };
    let spot_left = snapshot_for_market("left", MarketType::Spot, "99.5", "100");
    let spot_right = snapshot_for_market("right", MarketType::Spot, "100.1", "100.2");

    assert!(
        strategy
            .evaluate_pair(&state, &spot_left, &spot_right)
            .is_err()
    );
}

#[test]
fn flat_hold_and_complete_close_do_not_keep_a_direction_lock() {
    let strategy = ArbitrageStrategy::new(config()).unwrap();
    let flat_left = snapshot("left", "99", "100");
    let flat_right = snapshot("right", "99", "100");

    let held = strategy
        .evaluate_pair(&ArbitrageState::default(), &flat_left, &flat_right)
        .unwrap();
    assert_eq!(held.kind, ArbitrageDecisionKind::Hold);
    assert!(held.direction.is_none());

    let open_left = snapshot("left", "99", "100");
    let open_right = snapshot("right", "102", "103");
    let opened = strategy
        .evaluate_pair(&ArbitrageState::default(), &open_left, &open_right)
        .unwrap();
    let state = ArbitrageState {
        position_quantity: config().base_quantity.as_decimal(),
        direction: opened.direction,
    };
    let closed = strategy
        .evaluate_pair(&state, &flat_left, &flat_right)
        .unwrap();
    assert_eq!(closed.kind, ArbitrageDecisionKind::Reduce);
    assert_eq!(closed.target_quantity, Decimal::ZERO);
    assert!(closed.direction.is_none());
}

#[test]
fn flat_state_ignores_a_stale_direction_when_selecting_a_new_trade() {
    let strategy = ArbitrageStrategy::new(config()).unwrap();
    let original_left = snapshot("left", "99", "100");
    let original_right = snapshot("right", "102", "103");
    let original = strategy
        .evaluate_pair(&ArbitrageState::default(), &original_left, &original_right)
        .unwrap();
    let stale_direction = original.direction.unwrap();
    assert_eq!(stale_direction.buy_exchange, "left");

    let reversed_left = snapshot("left", "102", "103");
    let reversed_right = snapshot("right", "99", "100");
    let reopened = strategy
        .evaluate_pair(
            &ArbitrageState {
                position_quantity: Decimal::ZERO,
                direction: Some(stale_direction),
            },
            &reversed_left,
            &reversed_right,
        )
        .unwrap();

    assert_eq!(reopened.kind, ArbitrageDecisionKind::Open);
    let direction = reopened.direction.unwrap();
    assert_eq!(direction.buy_exchange, "right");
    assert_eq!(direction.sell_exchange, "left");
}

#[test]
fn arbitrage_rejects_segment_counts_above_the_business_limit() {
    let mut oversized = config();
    oversized.max_segments = 10_001;

    assert!(ArbitrageStrategy::new(oversized).is_err());
}

#[test]
fn arbitrage_returns_an_error_when_threshold_generation_overflows() {
    let mut overflowing = config();
    overflowing.initial_spread_percent = Decimal::MAX;
    overflowing.grid_step_percent = Decimal::ONE;
    overflowing.max_segments = 2;

    assert!(ArbitrageStrategy::new(overflowing).is_err());
}

#[test]
fn spread_calculation_returns_an_error_for_unrepresentable_percentages() {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let tiny = price("0.0000000000000000000000000001");
    let maximum = Price::new(Decimal::MAX).unwrap();
    let left = MarketSnapshot::new(
        "left",
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        tiny,
        tiny,
        now,
    )
    .unwrap();
    let right = MarketSnapshot::new(
        "right",
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        maximum,
        maximum,
        now,
    )
    .unwrap();

    assert!(SpreadCalculator::directions(&left, &right).is_err());
}

#[test]
fn arbitrage_returns_an_error_when_target_quantity_overflows() {
    let mut overflowing = config();
    overflowing.base_quantity = Quantity::new(Decimal::MAX).unwrap();
    overflowing.max_segments = 2;
    let strategy = ArbitrageStrategy::new(overflowing).unwrap();
    let left = snapshot("left", "99", "100");
    let right = snapshot("right", "102", "103");

    assert!(
        strategy
            .evaluate_pair(&ArbitrageState::default(), &left, &right)
            .is_err()
    );
}
