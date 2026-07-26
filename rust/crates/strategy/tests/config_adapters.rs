use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_config::{
    load_arbitrage_config_from_str, load_grid_config_from_str, load_price_alert_config_from_str,
    load_volume_maker_config_from_str,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_strategy::{
    AlertKind, AlertState, AlertStrategy, ArbitrageState, ArbitrageStrategy, GridDirection,
    GridPlanner, PairStrategyMachine, VolumeMakerMode, VolumeMakerPlanConfig,
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
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [BTC]
default_config:
  grid_config:
    initial_spread_threshold: 0.5
    grid_step: 0.2
    max_segments: 3
    first_close_ratio: 0.4
  quantity_config:
    base_quantity: 2
  risk_config:
    max_position_value: 100
symbol_configs:
  BTC:
    enabled: true
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
fn martingale_mode_without_a_positive_increment_is_rejected_end_to_end() {
    // The loader already fails closed; the pure planner conversion must also
    // refuse a hand-mutated config so martingale semantics cannot silently
    // degrade to a flat grid (`grid_config.py:352-367` treats a missing or
    // zero increment as "not martingale").
    let yaml = r"
exchange: paper
symbol: BTCUSDT
market_type: perpetual
mode: martingale_long
grid_interval: 1
order_amount: 1
lower_price: 100
upper_price: 104
";
    assert!(load_grid_config_from_str(yaml).is_err());

    let mut config =
        load_grid_config_from_str(&format!("{yaml}martingale_increment: 0.1\n")).unwrap();
    config.martingale_increment = None;
    assert!(GridPlanner::try_from(&config).is_err());
}

#[test]
fn arbitrage_public_conversion_enforces_operator_controls_mode_and_allowlists() {
    for yaml in [
        r"
mode: segmented
enabled: false
system_mode:
  monitor_only: false
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
",
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: true
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
",
        r"
mode: unified
enabled: true
system_mode:
  monitor_only: false
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
",
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: []
symbols: [BTC]
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
",
    ] {
        let config = load_arbitrage_config_from_str(yaml).unwrap();
        assert!(ArbitrageStrategy::try_from(&config).is_err(), "{yaml}");
    }
}

#[test]
fn arbitrage_public_conversion_requires_executable_symbol_scope_and_risk_limits() {
    let base = load_arbitrage_config_from_str(
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [BTC]
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
max_position_value: 10
symbol_configs:
  BTC:
    enabled: true
",
    )
    .unwrap();

    let mut no_enabled_strategy = base.clone();
    no_enabled_strategy
        .symbol_configs
        .get_mut(&Symbol::new("BTC").unwrap())
        .unwrap()
        .enabled = false;
    assert!(ArbitrageStrategy::try_from(&no_enabled_strategy).is_err());

    let mut non_positive_risk_limit = base;
    non_positive_risk_limit.max_position_value = Some(Decimal::ZERO);
    assert!(ArbitrageStrategy::try_from(&non_positive_risk_limit).is_err());
}

#[test]
fn arbitrage_public_conversion_rejects_missing_unresolved_risk_cap() {
    let config = load_arbitrage_config_from_str(
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [BTC]
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
symbol_configs:
  BTC:
    enabled: true
",
    )
    .unwrap();

    let error = ArbitrageStrategy::try_from(&config).unwrap_err();
    assert!(error.to_string().contains("execution controls"), "{error}");
}

#[test]
fn arbitrage_config_conversion_enforces_snapshot_allowlists() {
    let config = load_arbitrage_config_from_str(
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [BTC]
min_spread_pct: 0.5
grid_step: 0.2
max_segments: 3
base_quantity: 2
max_position_value: 100
symbol_configs:
  BTC:
    enabled: true
",
    )
    .unwrap();
    let strategy = ArbitrageStrategy::try_from(&config).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 2, 0, 0).unwrap();
    let snapshot = |exchange: &str, symbol: &str, bid: &str, ask: &str| {
        MarketSnapshot::new(
            exchange,
            Symbol::new(symbol).unwrap(),
            MarketType::Perpetual,
            price(bid),
            price(ask),
            now,
        )
        .unwrap()
    };
    let left = snapshot("left", "BTC", "99", "100");
    let right = snapshot("right", "BTC", "102", "103");

    strategy
        .evaluate_pair(&ArbitrageState::default(), &left, &right)
        .unwrap();

    let unknown_exchange = snapshot("third", "BTC", "99", "100");
    let error = strategy
        .evaluate_pair(&ArbitrageState::default(), &unknown_exchange, &right)
        .unwrap_err();
    assert!(error.to_string().contains("exchange third"), "{error}");

    let unknown_symbol = snapshot("left", "ETH", "99", "100");
    let error = strategy
        .evaluate_pair(&ArbitrageState::default(), &unknown_symbol, &right)
        .unwrap_err();
    assert!(error.to_string().contains("symbol ETH"), "{error}");
}

#[test]
fn arbitrage_resolved_selector_preserves_leg_allowlist_and_override_cap() {
    let mut config = load_arbitrage_config_from_str(
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [left, right]
symbols: [AAA-PERP, BBB-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.5
    grid_step: 0.2
    max_segments: 3
  quantity_config:
    base_quantity: 2
symbol_configs:
  CROSS_PAIR:
    enabled: true
    grid_config:
      initial_spread_threshold: 0.5
      grid_step: 0.2
      max_segments: 3
    quantity_config:
      base_quantity: 2
    risk_config:
      max_position_value: 25
",
    )
    .unwrap();
    let selector = Symbol::new("CROSS_PAIR").unwrap();
    config.max_position_value = Some(Decimal::ZERO);

    let error = ArbitrageStrategy::try_from(&config).unwrap_err();
    assert!(error.to_string().contains("execution controls"), "{error}");

    let effective = config.resolve_for_strategy(&selector).unwrap();
    assert_eq!(
        effective.symbols,
        vec![
            Symbol::new("AAA-PERP").unwrap(),
            Symbol::new("BBB-PERP").unwrap()
        ]
    );
    assert_eq!(effective.max_position_value, Some(decimal("25")));

    let strategy = ArbitrageStrategy::try_from(&effective).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 2, 30, 0).unwrap();
    let snapshot = |exchange: &str, symbol: &str, bid: &str, ask: &str| {
        MarketSnapshot::new(
            exchange,
            Symbol::new(symbol).unwrap(),
            MarketType::Perpetual,
            price(bid),
            price(ask),
            now,
        )
        .unwrap()
    };

    let left = snapshot("left", "AAA-PERP", "99", "100");
    let right = snapshot("right", "BBB-PERP", "102", "103");
    strategy
        .evaluate_pair(&ArbitrageState::default(), &left, &right)
        .unwrap();
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
  emergency_stop: false
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
