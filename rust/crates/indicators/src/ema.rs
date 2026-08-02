use rust_decimal::Decimal;

use crate::IndicatorError;

/// Incremental exponential moving average seeded with the first sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ema {
    alpha: Decimal,
    value: Option<Decimal>,
}

impl Ema {
    /// Creates a new EMA using `2 / (period + 1)` smoothing.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::InvalidPeriod`] when `period == 0`.
    /// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
    pub fn new(period: u32) -> Result<Self, IndicatorError> {
        if period == 0 {
            return Err(IndicatorError::InvalidPeriod);
        }

        let denominator = period
            .checked_add(1)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let alpha = Decimal::from(2_u32)
            .checked_div(Decimal::from(denominator))
            .ok_or(IndicatorError::ArithmeticOverflow)?;

        Ok(Self { alpha, value: None })
    }

    /// Updates the EMA with `sample` and returns the new value.
    ///
    /// The first sample seeds the EMA exactly.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
    pub fn update(&mut self, sample: Decimal) -> Result<Decimal, IndicatorError> {
        let next = match self.value {
            None => sample,
            Some(previous) => {
                let delta = sample
                    .checked_sub(previous)
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                let adjustment = self
                    .alpha
                    .checked_mul(delta)
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                previous
                    .checked_add(adjustment)
                    .ok_or(IndicatorError::ArithmeticOverflow)?
            }
        };

        self.value = Some(next);
        Ok(next)
    }

    /// Returns the last EMA value if one has been observed.
    #[must_use]
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}
