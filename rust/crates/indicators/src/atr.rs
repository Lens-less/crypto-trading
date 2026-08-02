use rust_decimal::Decimal;

use crate::IndicatorError;

/// Incremental Average True Range using Wilder smoothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Atr {
    period: Decimal,
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
            value: None,
            previous_close: None,
        })
    }

    /// Updates the ATR with a new OHLC bar and returns the new ATR value.
    ///
    /// The first bar seeds ATR with its true range.
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
    ) -> Result<Decimal, IndicatorError> {
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

        let next = match self.value {
            None => true_range,
            Some(previous_atr) => {
                let retained = previous_atr
                    .checked_mul(
                        self.period
                            .checked_sub(Decimal::ONE)
                            .ok_or(IndicatorError::ArithmeticOverflow)?,
                    )
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                retained
                    .checked_add(true_range)
                    .ok_or(IndicatorError::ArithmeticOverflow)?
                    .checked_div(self.period)
                    .ok_or(IndicatorError::ArithmeticOverflow)?
            }
        };

        self.previous_close = Some(close);
        self.value = Some(next);
        Ok(next)
    }

    /// Returns the latest ATR value if any bars have been processed.
    #[must_use]
    pub const fn value(&self) -> Option<Decimal> {
        self.value
    }
}
