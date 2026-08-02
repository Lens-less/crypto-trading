use std::str::FromStr;

use crypto_trading_indicators::{
    Atr, Ema, EwmaRealizedVolatility, RatioConfig, RollingZScore, max_drawdown, profit_factor,
    sharpe_ratio, sortino_ratio, summarize_performance, win_rate,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must parse")
}

fn assert_close(actual: Decimal, expected: &str) {
    let tolerance = decimal("0.000000000000001");
    let expected = decimal(expected);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn ema_matches_incremental_golden_vector() {
    let mut ema = Ema::new(3).unwrap();

    assert_eq!(ema.update(decimal("10")).unwrap(), decimal("10"));
    assert_eq!(ema.update(decimal("12")).unwrap(), decimal("11"));
    assert_eq!(ema.update(decimal("11")).unwrap(), decimal("11"));
    assert_eq!(ema.update(decimal("13")).unwrap(), decimal("12"));
}

#[test]
fn ema_rejects_a_period_that_cannot_form_its_smoothing_denominator() {
    assert_eq!(
        Ema::new(u32::MAX),
        Err(crypto_trading_indicators::IndicatorError::ArithmeticOverflow)
    );
}

#[test]
fn atr_matches_wilder_golden_vector() {
    let mut atr = Atr::new(3).unwrap();

    assert_eq!(
        atr.update(decimal("10"), decimal("8"), decimal("9"))
            .unwrap(),
        decimal("2")
    );
    assert_close(
        atr.update(decimal("11"), decimal("8"), decimal("10"))
            .unwrap(),
        "2.333333333333333333",
    );
    assert_close(
        atr.update(decimal("13"), decimal("9"), decimal("12"))
            .unwrap(),
        "2.888888888888888889",
    );
}

#[test]
fn ewma_realized_volatility_matches_incremental_golden_vector() {
    let mut volatility = EwmaRealizedVolatility::new(decimal("0.5")).unwrap();

    assert_eq!(volatility.update(decimal("100")).unwrap(), None);
    assert_eq!(
        volatility.update(decimal("110")).unwrap(),
        Some(decimal("0.1"))
    );
    assert_close(
        volatility.update(decimal("110")).unwrap().unwrap(),
        "0.070710678118654752",
    );
    assert_eq!(volatility.variance(), Some(decimal("0.005")));
}

#[test]
fn ewma_update_is_atomic_when_decimal_arithmetic_overflows() {
    let mut volatility = EwmaRealizedVolatility::new(decimal("0.5")).unwrap();
    assert_eq!(volatility.update(Decimal::ONE).unwrap(), None);
    let before = volatility.clone();

    assert_eq!(
        volatility.update(Decimal::MAX),
        Err(crypto_trading_indicators::IndicatorError::ArithmeticOverflow)
    );
    assert_eq!(volatility, before);
}

#[test]
fn rolling_zscore_matches_full_window_golden_vector() {
    let mut zscore = RollingZScore::new(2).unwrap();

    assert_eq!(zscore.update(decimal("1")).unwrap(), None);
    assert_eq!(zscore.update(decimal("3")).unwrap(), Some(decimal("1")));
    assert_eq!(zscore.update(decimal("5")).unwrap(), Some(decimal("1")));
}

#[test]
fn rolling_zscore_update_is_atomic_when_decimal_arithmetic_overflows() {
    let mut zscore = RollingZScore::new(2).unwrap();
    let before = zscore.clone();

    assert_eq!(
        zscore.update(Decimal::MAX),
        Err(crypto_trading_indicators::IndicatorError::ArithmeticOverflow)
    );
    assert_eq!(zscore, before);
}

#[test]
fn rolling_zscore_rejects_capacity_overflow_as_a_typed_window_error() {
    assert_eq!(
        RollingZScore::new(usize::MAX),
        Err(crypto_trading_indicators::IndicatorError::InvalidWindow)
    );
}

#[test]
fn performance_metrics_match_golden_vectors() {
    let equity_curve = [decimal("100"), decimal("120"), decimal("90"), decimal("95")];
    let trade_pnls = [decimal("10"), decimal("-5"), Decimal::ZERO, decimal("15")];
    let returns = [decimal("1"), decimal("1"), decimal("-1")];
    let ratios = RatioConfig::default();

    let drawdown = max_drawdown(&equity_curve).unwrap();
    assert_eq!(drawdown.amount, decimal("30"));
    assert_eq!(drawdown.ratio, decimal("0.25"));
    assert_eq!(win_rate(&trade_pnls).unwrap(), decimal("0.5"));
    assert_eq!(profit_factor(&trade_pnls).unwrap(), decimal("5"));
    assert_eq!(
        sharpe_ratio(&[decimal("1"), decimal("2"), decimal("3")], ratios)
            .unwrap()
            .unwrap(),
        decimal("2")
    );
    assert_close(
        sortino_ratio(&returns, ratios).unwrap().unwrap(),
        "0.577350269189625765",
    );

    let summary = summarize_performance(&equity_curve, &trade_pnls, &returns, ratios).unwrap();
    assert_eq!(summary.max_drawdown.unwrap().amount, decimal("30"));
}

#[test]
fn max_drawdown_returns_none_instead_of_panicking_on_unrepresentable_distance() {
    assert_eq!(max_drawdown(&[Decimal::MAX, Decimal::MIN]), None);
}
