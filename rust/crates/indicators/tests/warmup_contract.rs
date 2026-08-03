use std::str::FromStr;

use crypto_trading_indicators::{Atr, Ema, IndicatorError};
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
fn ema_returns_none_until_the_period_is_fully_warmed_up() {
    let mut ema = Ema::new(3).unwrap();

    assert_eq!(ema.update(decimal("10")).unwrap(), None);
    assert_eq!(ema.value(), None);

    assert_eq!(ema.update(decimal("12")).unwrap(), None);
    assert_eq!(ema.value(), None);

    assert_eq!(ema.update(decimal("11")).unwrap(), Some(decimal("11")));
    assert_eq!(ema.value(), Some(decimal("11")));

    assert_eq!(ema.update(decimal("13")).unwrap(), Some(decimal("12")));
    assert_eq!(ema.value(), Some(decimal("12")));
}

#[test]
fn ema_update_is_atomic_when_warmup_arithmetic_overflows() {
    let mut ema = Ema::new(2).unwrap();

    assert_eq!(ema.update(Decimal::MAX).unwrap(), None);
    let before = ema.clone();

    assert_eq!(
        ema.update(Decimal::ONE),
        Err(IndicatorError::ArithmeticOverflow)
    );
    assert_eq!(ema, before);
}

#[test]
fn atr_returns_none_until_the_period_is_fully_warmed_up() {
    let mut atr = Atr::new(3).unwrap();

    assert_eq!(
        atr.update(decimal("10"), decimal("8"), decimal("9"))
            .unwrap(),
        None
    );
    assert_eq!(atr.value(), None);

    assert_eq!(
        atr.update(decimal("11"), decimal("8"), decimal("10"))
            .unwrap(),
        None
    );
    assert_eq!(atr.value(), None);

    assert_eq!(
        atr.update(decimal("13"), decimal("9"), decimal("12"))
            .unwrap(),
        Some(decimal("3"))
    );
    assert_eq!(atr.value(), Some(decimal("3")));

    let next = atr
        .update(decimal("14"), decimal("10"), decimal("13"))
        .unwrap()
        .expect("ATR must be ready after the warm-up window");
    assert_close(next, "3.333333333333333333");
    assert_eq!(atr.value(), Some(next));
}

#[test]
fn atr_update_is_atomic_when_warmup_arithmetic_overflows() {
    let mut atr = Atr::new(2).unwrap();

    assert_eq!(
        atr.update(Decimal::MAX, Decimal::ONE, Decimal::ONE)
            .unwrap(),
        None
    );
    let before = atr.clone();

    assert_eq!(
        atr.update(Decimal::MAX, Decimal::ONE, Decimal::ONE),
        Err(IndicatorError::ArithmeticOverflow)
    );
    assert_eq!(atr, before);
}
