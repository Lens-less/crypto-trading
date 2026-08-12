use chrono::{Duration, TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestError, BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy, LongOnlyDonchian,
    SlowTimeSeriesMomentum, SpotBar, SpotDecisionContext, TargetExposureStrategy,
};
use crypto_trading_domain::Price;
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn bar(day: i64, close: &str) -> SpotBar {
    let open_time = Utc.timestamp_opt(day * 86_400, 0).unwrap();
    SpotBar::new(
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

fn decide<S: TargetExposureStrategy>(
    strategy: &mut S,
    bars: &[SpotBar],
    current_target: Decimal,
) -> Result<Decimal, BacktestError> {
    strategy.target_exposure(&SpotDecisionContext {
        bar_index: bars.len() - 1,
        decided_at: bars.last().unwrap().close_time,
        history: bars,
        current_target,
    })
}

#[test]
fn mandatory_cash_and_buy_hold_baselines_are_bounded_and_deterministic() {
    let bars = vec![bar(0, "100")];
    assert_eq!(
        decide(&mut CashStrategy, &bars, decimal("0.7")).unwrap(),
        Decimal::ZERO
    );
    let mut buy_and_hold = BuyAndHoldStrategy::default();
    assert_eq!(
        decide(&mut buy_and_hold, &bars, Decimal::ZERO).unwrap(),
        Decimal::ONE
    );
    assert_eq!(
        decide(&mut buy_and_hold, &bars, decimal("0.99999999")).unwrap(),
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
            Decimal::ZERO,
        )
        .unwrap(),
        Decimal::ONE
    );
    assert_eq!(
        decide(
            &mut SlowTimeSeriesMomentum::new(2, 1).unwrap(),
            &falling,
            Decimal::ONE,
        )
        .unwrap(),
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
            Decimal::ZERO,
        )
        .unwrap(),
        Decimal::ONE
    );
    assert_eq!(
        decide(
            &mut LongOnlyDonchian::new(2).unwrap(),
            &equality,
            Decimal::ZERO,
        )
        .unwrap(),
        Decimal::ZERO
    );
}

#[test]
fn volatility_target_never_leverages_and_treats_zero_variance_as_cash() {
    let flat = vec![bar(0, "100"), bar(1, "100"), bar(2, "100")];
    let variable = vec![bar(0, "100"), bar(1, "120"), bar(2, "90")];
    let mut strategy = CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1).unwrap();

    assert_eq!(
        decide(&mut strategy, &flat, Decimal::ZERO).unwrap(),
        Decimal::ZERO
    );
    let target = decide(&mut strategy, &variable, Decimal::ZERO).unwrap();
    assert!(target > Decimal::ZERO);
    assert!(target <= Decimal::ONE);
}

#[test]
fn volatility_target_skips_rebalancing_when_bar_index_is_off_cadence() {
    let bars = vec![bar(0, "100"), bar(1, "120"), bar(2, "90"), bar(3, "200")];
    let current_target = decimal("0.42");

    let cadence_locked = decide(
        &mut CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 2).unwrap(),
        &bars,
        current_target,
    )
    .unwrap();
    let recomputed = decide(
        &mut CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1).unwrap(),
        &bars,
        current_target,
    )
    .unwrap();

    assert_eq!(cadence_locked, current_target);
    assert_ne!(recomputed, current_target);
}

#[test]
fn candidate_parameters_fail_closed_before_any_evaluation() {
    assert!(SlowTimeSeriesMomentum::new(0, 1).is_err());
    assert!(LongOnlyDonchian::new(0).is_err());
    assert!(CappedVolatilityTarget::new(1, decimal("1.01"), Decimal::ZERO, 1).is_err());
    assert!(CappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 0).is_err());
}
