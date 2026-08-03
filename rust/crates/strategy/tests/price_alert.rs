use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_config::{
    PriceAlertConfig, PriceAlertSymbolConfig, PriceThresholdConfig,
    VolatilityAlertConfig as SourceVolatilityAlertConfig,
};
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
fn volatility_baseline_falls_back_to_the_oldest_sample_inside_the_window() {
    // Regression: the runtime prunes history to exactly the volatility window,
    // so with real millisecond timestamps no retained sample sits at or before
    // the window boundary. The strategy must measure against the oldest
    // retained in-window sample instead of never firing.
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
        upper_limit: None,
        lower_limit: None,
        volatility: Some(VolatilityAlertConfig {
            window: Duration::seconds(60),
            threshold_percent: decimal("4"),
        }),
        cooldown: Duration::zero(),
    })
    .unwrap();
    let mut state = AlertState::default();
    state
        .record_price(now - Duration::milliseconds(59_700), price("100"))
        .unwrap();
    state
        .record_price(now - Duration::milliseconds(29_300), price("102"))
        .unwrap();

    let alerts = strategy.evaluate(&state, &snapshot).unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].kind, AlertKind::VolatilityUp);
    assert_eq!(alerts[0].change_percent, Some(decimal("5")));

    // An empty history still yields no volatility baseline.
    assert!(
        strategy
            .evaluate(&AlertState::default(), &snapshot)
            .unwrap()
            .is_empty()
    );
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

#[test]
fn alert_config_rejects_unbounded_source_durations_without_panicking() {
    let symbol = Symbol::new("BTCUSDT").unwrap();
    let mut source = PriceAlertConfig {
        exchange: "paper".to_owned(),
        symbols: vec![PriceAlertSymbolConfig {
            symbol: symbol.clone(),
            market_type: MarketType::Perpetual,
            enabled: true,
            volatility_alert: SourceVolatilityAlertConfig {
                enabled: true,
                time_window_seconds: u64::MAX,
                threshold_percent: Decimal::ONE,
            },
            price_alert: PriceThresholdConfig {
                enabled: false,
                upper_price: None,
                lower_price: None,
            },
        }],
        refresh_interval_seconds: Decimal::ONE,
        cooldown_seconds: 0,
    };

    assert!(AlertStrategy::from_config(&source, &symbol).is_err());

    source.symbols[0].volatility_alert.time_window_seconds = 60;
    source.cooldown_seconds = u64::MAX;
    assert!(AlertStrategy::from_config(&source, &symbol).is_err());
}

#[test]
fn alert_evaluation_returns_an_error_when_volatility_math_overflows() {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 1, 0).unwrap();
    let maximum = Price::new(Decimal::MAX).unwrap();
    let snapshot = MarketSnapshot::new(
        "binance",
        Symbol::new("BTCUSDT").unwrap(),
        MarketType::Spot,
        maximum,
        maximum,
        now,
    )
    .unwrap();
    let strategy = AlertStrategy::new(AlertConfig {
        upper_limit: None,
        lower_limit: None,
        volatility: Some(VolatilityAlertConfig {
            window: Duration::seconds(60),
            threshold_percent: Decimal::ONE,
        }),
        cooldown: Duration::zero(),
    })
    .unwrap();
    let mut state = AlertState::default();
    state
        .record_price(
            now - Duration::seconds(60),
            price("0.0000000000000000000000000001"),
        )
        .unwrap();

    assert!(strategy.evaluate(&state, &snapshot).is_err());
}

#[test]
fn alert_history_has_a_bounded_capacity() {
    let started_at = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let mut state = AlertState::default();
    for offset in 0..100_000 {
        state
            .record_price(started_at + Duration::milliseconds(offset), price("100"))
            .unwrap();
    }

    assert!(
        state
            .record_price(started_at + Duration::milliseconds(100_000), price("100"))
            .is_err()
    );
}

#[test]
fn alert_handles_extreme_but_valid_mid_prices_without_panicking() {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 1, 0).unwrap();
    let snapshot = MarketSnapshot::new(
        "binance",
        Symbol::new("BTCUSDT").unwrap(),
        MarketType::Spot,
        price("0.0000000000000000000000000001"),
        Price::new(Decimal::MAX).unwrap(),
        now,
    )
    .unwrap();
    let strategy = AlertStrategy::new(AlertConfig {
        upper_limit: Some(price("100")),
        lower_limit: None,
        volatility: None,
        cooldown: Duration::zero(),
    })
    .unwrap();

    assert!(strategy.evaluate(&AlertState::default(), &snapshot).is_ok());
}
