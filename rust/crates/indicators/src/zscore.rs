use std::collections::VecDeque;

use rust_decimal::Decimal;

use crate::{IndicatorError, math::checked_sqrt};

const VARIANCE_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 18);

/// Rolling z-score over a fixed-size window using population variance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingZScore {
    window: usize,
    samples: VecDeque<Decimal>,
    sum: Decimal,
    sum_of_squares: Decimal,
}

impl RollingZScore {
    /// Creates a rolling z-score indicator.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::InvalidWindow`] when `window < 2`.
    pub fn new(window: usize) -> Result<Self, IndicatorError> {
        if window < 2 {
            return Err(IndicatorError::InvalidWindow);
        }

        Ok(Self {
            window,
            samples: VecDeque::with_capacity(window),
            sum: Decimal::ZERO,
            sum_of_squares: Decimal::ZERO,
        })
    }

    /// Adds a sample and returns the latest z-score once the window is full.
    ///
    /// Returns `None` until enough observations are available or when the
    /// window variance is zero.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
    pub fn update(&mut self, sample: Decimal) -> Result<Option<Decimal>, IndicatorError> {
        let removed = if self.samples.len() == self.window {
            Some(
                self.samples
                    .front()
                    .copied()
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
        } else {
            None
        };
        let retained_sum = match removed {
            Some(value) => self
                .sum
                .checked_sub(value)
                .ok_or(IndicatorError::ArithmeticOverflow)?,
            None => self.sum,
        };
        let retained_sum_of_squares = match removed {
            Some(value) => self
                .sum_of_squares
                .checked_sub(
                    value
                        .checked_mul(value)
                        .ok_or(IndicatorError::ArithmeticOverflow)?,
                )
                .ok_or(IndicatorError::ArithmeticOverflow)?,
            None => self.sum_of_squares,
        };
        let next_sum = retained_sum
            .checked_add(sample)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let next_sum_of_squares = retained_sum_of_squares
            .checked_add(
                sample
                    .checked_mul(sample)
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
            .ok_or(IndicatorError::ArithmeticOverflow)?;

        let next_len = if removed.is_some() {
            self.window
        } else {
            self.samples.len() + 1
        };
        let result = if next_len < self.window {
            None
        } else {
            let count = Decimal::from(
                u64::try_from(self.window).map_err(|_| IndicatorError::ArithmeticOverflow)?,
            );
            let mean = next_sum
                .checked_div(count)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let mean_square = mean
                .checked_mul(mean)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let average_square = next_sum_of_squares
                .checked_div(count)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let variance = average_square
                .checked_sub(mean_square)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            if variance.is_zero()
                || (variance < Decimal::ZERO && variance.abs() <= VARIANCE_TOLERANCE)
            {
                None
            } else {
                if variance < Decimal::ZERO {
                    return Err(IndicatorError::ArithmeticOverflow);
                }
                let standard_deviation = checked_sqrt(variance)?;
                if standard_deviation.is_zero() {
                    None
                } else {
                    Some(
                        sample
                            .checked_sub(mean)
                            .ok_or(IndicatorError::ArithmeticOverflow)?
                            .checked_div(standard_deviation)
                            .ok_or(IndicatorError::ArithmeticOverflow)?,
                    )
                }
            }
        };

        if removed.is_some() {
            let _ = self.samples.pop_front();
        }
        self.samples.push_back(sample);
        self.sum = next_sum;
        self.sum_of_squares = next_sum_of_squares;
        Ok(result)
    }
}
