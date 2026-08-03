use std::str::FromStr;

use crypto_trading_indicators::RollingZScore;
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must parse")
}

#[test]
fn rolling_zscore_preserves_small_variance_around_a_large_price() {
    let mut zscore = RollingZScore::new(3).unwrap();

    assert_eq!(zscore.update(decimal("100000000.0000")).unwrap(), None);
    assert_eq!(zscore.update(decimal("100000000.0001")).unwrap(), None);
    let first = zscore
        .update(decimal("100000000.0002"))
        .unwrap()
        .expect("non-zero variance must produce a z-score");
    let second = zscore
        .update(decimal("100000000.0003"))
        .unwrap()
        .expect("rolling non-zero variance must remain measurable");

    let expected = decimal("1.224744871391589049");
    let tolerance = decimal("0.00000000000001");
    assert!((first - expected).abs() <= tolerance, "got {first}");
    assert!((second - expected).abs() <= tolerance, "got {second}");
}
