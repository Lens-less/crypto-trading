use rust_decimal::Decimal;

use crate::IndicatorError;

/// Incremental Average True Range with an explicit warm-up window.
///
/// The first `period` true ranges are accumulated into their arithmetic mean.
/// The first `period - 1` updates produce `None`; the `period`th returns that
/// average as the first ready value. Later updates apply Wilder smoothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Atr {
    period: Decimal,
    pending_bars: u32,
    warmup_true_range_sum: Decimal,
    value: Option<Decimal>,
    previous_close: Option<Decimal>,
}

impl Atr {
    /// Creates a new ATR with Wilder smoothing (`1 / period`).
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::InvalidPeriod`] when `period == 0`.
    pub fn new(period: u32) -> Result<Self, IndicatorError> {
        if period == 0 {
            return Err(IndicatorError::InvalidPeriod);
        }

        Ok(Self {
            period: Decimal::from(period),
            pending_bars: period,
            warmup_true_range_sum: Decimal::ZERO,
            value: None,
            previous_close: None,
        })
    }

    /// Updates the ATR with a new OHLC bar.
    ///
    /// Returns `None` until `period` valid bars have been observed. The
    /// `period`th bar returns the arithmetic mean of the warm-up true ranges,
    /// and later bars return Wilder-smoothed ATR values.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::NonPositivePrice`] when any price is not
    /// strictly positive.
    /// Returns [`IndicatorError::CrossedRange`] when `high < low`.
    /// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
    pub fn update(
        &mut self,
        high: Decimal,
        low: Decimal,
        close: Decimal,
    ) -> Result<Option<Decimal>, IndicatorError> {
        if high <= Decimal::ZERO || low <= Decimal::ZERO || close <= Decimal::ZERO {
            return Err(IndicatorError::NonPositivePrice);
        }
        if high < low {
            return Err(IndicatorError::CrossedRange);
        }

        let range = high
            .checked_sub(low)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let true_range = match self.previous_close {
            None => range,
            Some(previous_close) => {
                let high_gap = high
                    .checked_sub(previous_close)
                    .map(|gap| gap.abs())
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                let low_gap = low
                    .checked_sub(previous_close)
                    .map(|gap| gap.abs())
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                range.max(high_gap).max(low_gap)
            }
        };

        if let Some(previous_atr) = self.value {
            let retained = previous_atr
                .checked_mul(
                    self.period
                        .checked_sub(Decimal::ONE)
                        .ok_or(IndicatorError::ArithmeticOverflow)?,
                )
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let next = retained
                .checked_add(true_range)
                .ok_or(IndicatorError::ArithmeticOverflow)?
                .checked_div(self.period)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            self.previous_close = Some(close);
            self.value = Some(next);
            return Ok(Some(next));
        }

        let pending_bars = self
            .pending_bars
            .checked_sub(1)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let warmup_true_range_sum = self
            .warmup_true_range_sum
            .checked_add(true_range)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        if pending_bars > 0 {
            self.pending_bars = pending_bars;
            self.warmup_true_range_sum = warmup_true_range_sum;
            self.previous_close = Some(close);
            return Ok(None);
        }

        let next = warmup_true_range_sum
            .checked_div(self.period)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        self.pending_bars = 0;
        self.warmup_true_range_sum = warmup_true_range_sum;
        self.previous_close = Some(close);
        self.value = Some(next);
        Ok(Some(next))
    }

    /// Returns the latest ready ATR value.
    ///
    /// This remains `None` until the full warm-up window has been observed.
    #[must_use]
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}
