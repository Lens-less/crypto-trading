use rust_decimal::Decimal;

use crate::IndicatorError;

const SQRT_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 18);
const SQRT_ROUND_DP: u32 = 18;
const SQRT_MAX_ITERATIONS: usize = 64;

pub(crate) fn checked_sqrt(value: Decimal) -> Result<Decimal, IndicatorError> {
    if value < Decimal::ZERO {
        return Err(IndicatorError::NegativeSquareRoot);
    }
    if value.is_zero() {
        return Ok(Decimal::ZERO);
    }

    let two = Decimal::from(2_u32);
    let mut guess = if value > Decimal::ONE {
        value
            .checked_div(two)
            .ok_or(IndicatorError::ArithmeticOverflow)?
    } else {
        Decimal::ONE
    };

    for _ in 0..SQRT_MAX_ITERATIONS {
        let quotient = value
            .checked_div(guess)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let next = guess
            .checked_add(quotient)
            .ok_or(IndicatorError::ArithmeticOverflow)?
            .checked_div(two)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let delta = next
            .checked_sub(guess)
            .map(|difference| difference.abs())
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        if delta <= SQRT_TOLERANCE {
            return Ok(next.round_dp(SQRT_ROUND_DP));
        }
        guess = next;
    }

    Ok(guess.round_dp(SQRT_ROUND_DP))
}
