use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_strategy::{
    AlertConfig, AlertKind, AlertState, AlertStrategy, VolatilityAlertConfig,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

#[test]
fn alert_strategy_emits_exact_limit_and_window_volatility_events_with_cooldowns() {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 1, 0).unwrap();
    let mut snapshot = MarketSnapshot::new(
        "binance",
        Symbol::new("BTCUSDT").unwrap(),
        MarketType::Spot,
        price("104.9"),
        price("105.1"),
        now,
    )
    .unwrap();
    snapshot.last = Some(price("105"));

    let strategy = AlertStrategy::new(AlertConfig {
        upper_limit: Some(price("104")),
        lower_limit: Some(price("96")),
        volatility: Some(VolatilityAlertConfig {
            window: Duration::seconds(60),
            threshold_percent: decimal("4"),
        }),
        cooldown: Duration::seconds(30),
    })
    .unwrap();
    let mut state = AlertState::default();
    state
        .record_price(now - Duration::seconds(60), price("100"))
        .unwrap();

    let alerts = strategy.evaluate(&state, &snapshot).unwrap();
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[0].kind, AlertKind::VolatilityUp);
    assert_eq!(alerts[0].change_percent, Some(decimal("5")));
    assert_eq!(alerts[1].kind, AlertKind::UpperLimit);

    state.record_alert(AlertKind::VolatilityUp, now - Duration::seconds(10));
    state.record_alert(AlertKind::UpperLimit, now - Duration::seconds(10));
    assert!(strategy.evaluate(&state, &snapshot).unwrap().is_empty());

    state.record_alert(AlertKind::UpperLimit, now - Duration::seconds(31));
    let after_cooldown = strategy.evaluate(&state, &snapshot).unwrap();
    assert_eq!(after_cooldown.len(), 1);
    assert_eq!(after_cooldown[0].kind, AlertKind::UpperLimit);
}

#[test]
fn volatility_cooldown_is_shared_across_direction_changes() {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 1, 0).unwrap();
    let snapshot = MarketSnapshot::new(
        "binance",
        Symbol::new("BTCUSDT").unwrap(),
        MarketType::Spot,
        price("94.9"),
        price("95.1"),
        now,
    )
    .unwrap();
    let strategy = AlertStrategy::new(AlertConfig {
        upper_limit: None,
        lower_limit: None,
        volatility: Some(VolatilityAlertConfig {
            window: Duration::seconds(60),
            threshold_percent: decimal("4"),
        }),
        cooldown: Duration::seconds(30),
    })
    .unwrap();
    let mut state = AlertState::default();
    state
        .record_price(now - Duration::seconds(60), price("100"))
        .unwrap();
    state.record_alert(AlertKind::VolatilityUp, now - Duration::seconds(10));

    assert!(strategy.evaluate(&state, &snapshot).unwrap().is_empty());
}
