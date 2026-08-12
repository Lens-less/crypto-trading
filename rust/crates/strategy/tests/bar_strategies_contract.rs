use chrono::{Duration, TimeZone, Utc};
use crypto_trading_domain::Price;
use crypto_trading_strategy::{
    Bar, BarStrategy, BarStrategyContext, BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy,
    LongOnlyDonchian, SlowTimeSeriesMomentum, TargetExposure,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn exposure(value: &str) -> TargetExposure {
    TargetExposure::new(decimal(value)).unwrap()
}

fn bar(day: i64, close: &str) -> Bar {
    let open_time = Utc.timestamp_opt(day * 86_400, 0).unwrap();
    Bar::new(
        open_time,
        open_time + Duration::days(1) - Duration::milliseconds(1),
        price(close),
        price(close),
        price(close),
        price(close),
        Decimal::ONE,
        decimal("100"),
        1,
    )
    .unwrap()
}

fn decide<S: BarStrategy>(
    strategy: &mut S,
    bars: &[Bar],
    current_target: TargetExposure,
) -> Decimal {
    strategy
        .target_exposure(&BarStrategyContext {
            history: bars,
            decided_at: bars.last().unwrap().close_time,
            bar_index: bars.len() - 1,
            current_target: current_target.as_decimal(),
        })
        .unwrap()
        .as_decimal()
}

#[test]
fn mandatory_cash_and_buy_hold_baselines_are_bounded_and_deterministic() {
    let bars = vec![bar(0, "100")];
    assert_eq!(
        decide(&mut CashStrategy, &bars, exposure("0.7")),
        Decimal::ZERO
    );
    let mut buy_and_hold = BuyAndHoldStrategy::default();
    assert_eq!(
        decide(&mut buy_and_hold, &bars, exposure("0")),
        Decimal::ONE
    );
    assert_eq!(
        decide(&mut buy_and_hold, &bars, exposure("0.99999999")),
        decimal("0.99999999")
    );
}

#[test]
fn slow_time_series_momentum_uses_only_the_completed_trailing_return() {
    let rising = vec![bar(0, "100"), bar(1, "90"), bar(2, "110")];
    let falling = vec![bar(0, "100"), bar(1, "110"), bar(2, "90")];

    assert_eq!(
        decide(
            &mut SlowTimeSeriesMomentum::new(2, 1).unwrap(),
            &rising,
            exposure("0"),
        ),
        Decimal::ONE
    );
    assert_eq!(
        decide(
            &mut SlowTimeSeriesMomentum::new(2, 1).unwrap(),
            &falling,
            exposure("1"),
        ),
        Decimal::ZERO
    );
}

#[test]
fn donchian_entry_compares_the_current_close_only_with_prior_completed_closes() {
    let breakout = vec![bar(0, "100"), bar(1, "105"), bar(2, "110")];
    let equality = vec![bar(0, "100"), bar(1, "110"), bar(2, "110")];

    assert_eq!(
        decide(
            &mut LongOnlyDonchian::new(2).unwrap(),
            &breakout,
            exposure("0"),
        ),
        Decimal::ONE
    );
    assert_eq!(
        decide(
            &mut LongOnlyDonchian::new(2).unwrap(),
            &equality,
            exposure("0"),
        ),
        Decimal::ZERO
    );
}

#[test]
fn volatility_target_never_leverages_and_treats_zero_variance_as_cash() {
    let flat = vec![bar(0, "100"), bar(1, "100"), bar(2, "100")];
    let variable = vec![bar(0, "100"), bar(1, "120"), bar(2, "90")];
    let mut strategy = CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1).unwrap();

    assert_eq!(decide(&mut strategy, &flat, exposure("0")), Decimal::ZERO);
    let target = decide(&mut strategy, &variable, exposure("0"));
    assert!(target > Decimal::ZERO);
    assert!(target <= Decimal::ONE);
}

#[test]
fn volatility_target_skips_rebalancing_when_bar_index_is_off_cadence() {
    let bars = vec![bar(0, "100"), bar(1, "120"), bar(2, "90"), bar(3, "200")];
    let current_target = exposure("0.42");

    let cadence_locked = decide(
        &mut CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 2).unwrap(),
        &bars,
        current_target,
    );
    let recomputed = decide(
        &mut CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1).unwrap(),
        &bars,
        current_target,
    );

    assert_eq!(cadence_locked, current_target.as_decimal());
    assert_ne!(recomputed, current_target.as_decimal());
}

#[test]
fn volatility_target_supports_explicit_annualization_for_other_cadences() {
    let bars = vec![bar(0, "100"), bar(1, "120"), bar(2, "90"), bar(3, "150")];
    let daily = decide(
        &mut CappedVolatilityTarget::new_with_periods_per_year(
            2,
            decimal("0.15"),
            Decimal::ZERO,
            1,
            decimal("365"),
        )
        .unwrap(),
        &bars,
        exposure("0"),
    );
    let hourly = decide(
        &mut CappedVolatilityTarget::new_with_periods_per_year(
            2,
            decimal("0.15"),
            Decimal::ZERO,
            1,
            decimal("8760"),
        )
        .unwrap(),
        &bars,
        exposure("0"),
    );

    assert!(hourly < daily);
}
