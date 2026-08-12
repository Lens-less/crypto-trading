use chrono::{Duration, TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestError, CappedVolatilityTarget as BacktestCappedVolatilityTarget,
    LongOnlyDonchian as BacktestLongOnlyDonchian, SlowTimeSeriesMomentum as BacktestMomentum,
    SpotBar, SpotDecisionContext, SpotStrategyConfig, TargetExposureStrategy,
};
use crypto_trading_domain::Price;
use crypto_trading_strategy::{
    BarStrategy, BarStrategyContext, BuyAndHoldStrategy as SharedBuyAndHoldStrategy,
    CappedVolatilityTarget as SharedCappedVolatilityTarget, CashStrategy as SharedCashStrategy,
    LongOnlyDonchian as SharedLongOnlyDonchian,
    SlowTimeSeriesMomentum as SharedSlowTimeSeriesMomentum,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn spot_bar(day: i64, close: &str) -> SpotBar {
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

fn decide_backtest<S: TargetExposureStrategy>(
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

fn decide_shared<S: BarStrategy>(
    strategy: &mut S,
    bars: &[SpotBar],
    current_target: Decimal,
) -> Decimal {
    strategy
        .target_exposure(&BarStrategyContext {
            history: bars,
            decided_at: bars.last().unwrap().close_time,
            bar_index: bars.len() - 1,
            current_target,
        })
        .unwrap()
        .as_decimal()
}

#[test]
fn registered_candidate_families_match_the_shared_bar_contract() {
    let spot_bars = vec![
        spot_bar(0, "100"),
        spot_bar(1, "105"),
        spot_bar(2, "95"),
        spot_bar(3, "110"),
    ];

    let mut cash = SpotStrategyConfig::Cash.build().unwrap();
    assert_eq!(
        decide_backtest(&mut cash, &spot_bars[..1], decimal("0.25")).unwrap(),
        decide_shared(&mut SharedCashStrategy, &spot_bars[..1], decimal("0.25"))
    );

    let mut buy_and_hold = SpotStrategyConfig::BuyAndHold.build().unwrap();
    assert_eq!(
        decide_backtest(&mut buy_and_hold, &spot_bars[..1], Decimal::ZERO).unwrap(),
        decide_shared(
            &mut SharedBuyAndHoldStrategy::default(),
            &spot_bars[..1],
            Decimal::ZERO,
        )
    );

    let mut momentum = SpotStrategyConfig::SlowTimeSeriesMomentum {
        lookback_bars: 2,
        rebalance_every_bars: 1,
    }
    .build()
    .unwrap();
    assert_eq!(
        decide_backtest(&mut momentum, &spot_bars[..3], Decimal::ZERO).unwrap(),
        decide_shared(
            &mut SharedSlowTimeSeriesMomentum::new(2, 1).unwrap(),
            &spot_bars[..3],
            Decimal::ZERO,
        )
    );

    let mut donchian = SpotStrategyConfig::LongOnlyDonchian { lookback_bars: 2 }
        .build()
        .unwrap();
    assert_eq!(
        decide_backtest(&mut donchian, &spot_bars[..3], Decimal::ZERO).unwrap(),
        decide_shared(
            &mut SharedLongOnlyDonchian::new(2).unwrap(),
            &spot_bars[..3],
            Decimal::ZERO,
        )
    );

    let mut volatility = SpotStrategyConfig::CappedVolatilityTarget {
        lookback_returns: 2,
        annual_target: decimal("0.15"),
        rebalance_band: Decimal::ZERO,
        rebalance_every_bars: 1,
    }
    .build()
    .unwrap();
    assert_eq!(
        decide_backtest(&mut volatility, &spot_bars, decimal("0.4")).unwrap(),
        decide_shared(
            &mut SharedCappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1).unwrap(),
            &spot_bars,
            decimal("0.4"),
        )
    );
}

#[test]
fn legacy_backtest_types_remain_constructible_after_strategy_extraction() {
    assert_eq!(
        BacktestMomentum::new(2, 1),
        Ok(BacktestMomentum::new(2, 1).unwrap())
    );
    assert_eq!(
        BacktestLongOnlyDonchian::new(2),
        Ok(BacktestLongOnlyDonchian::new(2).unwrap())
    );
    assert_eq!(
        BacktestCappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1),
        Ok(BacktestCappedVolatilityTarget::new(2, decimal("0.15"), Decimal::ZERO, 1).unwrap())
    );
}
