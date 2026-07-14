use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_config::load_volume_maker_config_from_str;
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_strategy::{
    StrategyMachine, VolumeMakerMode, VolumeMakerPlanConfig, VolumeMakerState, VolumeMakerStrategy,
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

fn snapshot() -> MarketSnapshot {
    let mut snapshot = MarketSnapshot::new(
        "paper",
        Symbol::new("BTC").unwrap(),
        MarketType::Perpetual,
        price("99"),
        price("101"),
        Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap(),
    )
    .unwrap();
    snapshot.bid_quantity = Some(quantity("10"));
    snapshot.ask_quantity = Some(quantity("2"));
    snapshot
}

fn config(mode: VolumeMakerMode, reverse_trading: bool) -> VolumeMakerPlanConfig {
    VolumeMakerPlanConfig {
        exchange: "paper".to_owned(),
        symbol: Symbol::new("BTC").unwrap(),
        market_type: MarketType::Perpetual,
        mode,
        order_quantity: quantity("0.5"),
        reverse_trading,
        post_only: true,
    }
}

#[test]
fn limit_mode_quotes_both_sides_at_top_of_book() {
    let strategy = VolumeMakerStrategy::new(config(VolumeMakerMode::LimitBoth, false)).unwrap();
    let intents = strategy
        .evaluate(&VolumeMakerState::Flat, &snapshot())
        .unwrap();

    assert_eq!(intents.len(), 2);
    assert_eq!(intents[0].side, Side::Buy);
    assert_eq!(intents[0].price.unwrap().as_decimal(), decimal("99"));
    assert_eq!(intents[1].side, Side::Sell);
    assert_eq!(intents[1].price.unwrap().as_decimal(), decimal("101"));
    assert!(
        intents
            .iter()
            .all(|intent| intent.time_in_force == TimeInForce::PostOnly)
    );
}

#[test]
fn market_mode_follows_book_imbalance_and_open_state_closes_reduce_only() {
    let strategy =
        VolumeMakerStrategy::new(config(VolumeMakerMode::MarketImbalance, false)).unwrap();
    let opened = strategy
        .evaluate(&VolumeMakerState::Flat, &snapshot())
        .unwrap();
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].side, Side::Buy);

    let closed = strategy
        .evaluate(
            &VolumeMakerState::Open {
                side: Side::Buy,
                quantity: quantity("0.5"),
            },
            &snapshot(),
        )
        .unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].side, Side::Sell);
    assert!(closed[0].reduce_only);

    let reverse = VolumeMakerStrategy::new(config(VolumeMakerMode::MarketImbalance, true)).unwrap();
    assert_eq!(
        reverse
            .evaluate(&VolumeMakerState::Flat, &snapshot())
            .unwrap()[0]
            .side,
        Side::Sell
    );
}

#[test]
fn volume_maker_rejects_a_snapshot_for_another_market_type() {
    let strategy = VolumeMakerStrategy::new(config(VolumeMakerMode::LimitBoth, false)).unwrap();
    let mut spot = snapshot();
    spot.market_type = MarketType::Spot;

    assert!(strategy.evaluate(&VolumeMakerState::Flat, &spot).is_err());
}

#[test]
fn public_volume_maker_constructor_enforces_emergency_stop() {
    let config = load_volume_maker_config_from_str(
        r"
volume_maker:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  order_mode: limit
  order_size: 0.5
  emergency_stop: true
",
    )
    .unwrap();

    let error = VolumeMakerStrategy::try_from(&config).unwrap_err();
    let plan_error = VolumeMakerPlanConfig::try_from(&config).unwrap_err();

    assert!(error.to_string().contains("emergency stop"), "{error}");
    assert!(
        plan_error.to_string().contains("emergency stop"),
        "{plan_error}"
    );
}
