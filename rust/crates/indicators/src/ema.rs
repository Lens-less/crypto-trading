use rust_decimal::Decimal;

use crate::IndicatorError;

/// Incremental exponential moving average with an explicit warm-up window.
///
/// The first `period` samples are accumulated into an arithmetic mean. The
/// first `period - 1` updates produce `None`; the `period`th returns that mean
/// as the first ready value. Later updates apply the standard
/// `ema = ema + alpha * (sample - ema)` recurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ema {
    period: Decimal,
    alpha: Decimal,
    pending_samples: u32,
    warmup_sum: Decimal,
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

        let pending_samples = period;
        let period = Decimal::from(period);
        let denominator = period
            .checked_add(Decimal::ONE)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let alpha = Decimal::from(2_u32)
            .checked_div(denominator)
            .ok_or(IndicatorError::ArithmeticOverflow)?;

        Ok(Self {
            period,
            alpha,
            pending_samples,
            warmup_sum: Decimal::ZERO,
            value: None,
        })
    }

    /// Updates the EMA with `sample`.
    ///
    /// Returns `None` until `period` samples have been observed. The `period`th
    /// sample returns the arithmetic mean of the warm-up window, and later
    /// samples return the smoothed EMA value.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
    pub fn update(&mut self, sample: Decimal) -> Result<Option<Decimal>, IndicatorError> {
        if let Some(previous) = self.value {
            let delta = sample
                .checked_sub(previous)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let adjustment = self
                .alpha
                .checked_mul(delta)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let next = previous
                .checked_add(adjustment)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            self.value = Some(next);
            return Ok(Some(next));
        }

        let pending_samples = self
            .pending_samples
            .checked_sub(1)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let warmup_sum = self
            .warmup_sum
            .checked_add(sample)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        if pending_samples > 0 {
            self.pending_samples = pending_samples;
            self.warmup_sum = warmup_sum;
            return Ok(None);
        }

        let next = warmup_sum
            .checked_div(self.period)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        self.pending_samples = 0;
        self.warmup_sum = warmup_sum;
        self.value = Some(next);
        Ok(Some(next))
    }

    /// Returns the latest ready EMA value.
    ///
    /// This remains `None` until the full warm-up window has been observed.
    #[must_use]
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}
