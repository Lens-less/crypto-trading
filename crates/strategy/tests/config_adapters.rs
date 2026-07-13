use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_config::{
    load_arbitrage_config_from_str, load_grid_config_from_str, load_price_alert_config_from_str,
    load_volume_maker_config_from_str,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_strategy::{
    AlertKind, AlertState, AlertStrategy, ArbitrageStrategy, GridDirection, GridPlanner,
    VolumeMakerMode, VolumeMakerPlanConfig,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).expect("test price must be positive")
}

#[test]
fn grid_and_arbitrage_yaml_feed_pure_planners_without_lossy_numbers() {
    let grid_config = load_grid_config_from_str(
        r"
exchange: paper
symbol: BTCUSDT
market_type: perpetual
mode: martingale_long
grid_interval: 1
order_amount: 1
lower_price: 100
upper_price: 104
martingale_increment: 0.1
",
    )
    .unwrap();
    let planner = GridPlanner::try_from(&grid_config).unwrap();
    let levels = planner.fixed_levels().unwrap();

    assert_eq!(planner.config().direction, GridDirection::Long);
    assert_eq!(levels[0].quantity.as_decimal(), decimal("1.3"));
    assert_eq!(levels[3].price.as_decimal(), decimal("103"));

    let arbitrage_config = load_arbitrage_config_from_str(
        r"
default_config:
  grid_config:
    initial_spread_threshold: 0.5
    grid_step: 0.2
    max_segments: 3
    first_close_ratio: 0.4
  quantity_config:
    base_quantity: 2
",
    )
    .unwrap();
    let arbitrage = ArbitrageStrategy::try_from(&arbitrage_config).unwrap();

    assert_eq!(
        arbitrage.open_thresholds(),
        [decimal("0.5"), decimal("0.7"), decimal("0.9")]
    );
    assert_eq!(
        arbitrage.close_thresholds(),
        [decimal("0.20"), decimal("0.5"), decimal("0.7")]
    );
    assert_eq!(arbitrage.config().base_quantity.as_decimal(), decimal("2"));
}

#[test]
fn alert_and_volume_yaml_construct_symbol_scoped_strategies() {
    let alert_config = load_price_alert_config_from_str(
        r"
price_alert:
  exchange: paper
  symbols:
    - symbol: BTCUSDT
      market_type: perpetual
      volatility_alert:
        enabled: true
        time_window: 60
        threshold_percent: 5
      price_alert:
        enabled: true
        upper_limit: 110
        lower_limit: 90
  alert:
    cooldown_seconds: 30
",
    )
    .unwrap();
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let alert_strategy = AlertStrategy::from_config(&alert_config, &symbol).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 1, 0, 0).unwrap();
    let mut snapshot = MarketSnapshot::new(
        "paper",
        symbol,
        MarketType::Perpetual,
        price("110"),
        price("111"),
        now,
    )
    .unwrap();
    snapshot.last = Some(price("110"));
    let alerts = alert_strategy
        .evaluate(&AlertState::default(), &snapshot)
        .unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].kind, AlertKind::UpperLimit);

    let volume_config = load_volume_maker_config_from_str(
        r"
volume_maker:
  exchange: paper
  symbol: BTCUSDT
  market_type: perpetual
  order_quantity: 0.25
  order_mode: market
  reverse_trading: true
  advanced:
    use_post_only: true
",
    )
    .unwrap();
    let plan = VolumeMakerPlanConfig::try_from(&volume_config).unwrap();

    assert_eq!(plan.mode, VolumeMakerMode::MarketImbalance);
    assert_eq!(plan.order_quantity.as_decimal(), decimal("0.25"));
    assert!(plan.reverse_trading);
    assert!(plan.post_only);
}
