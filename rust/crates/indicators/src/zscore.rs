use std::collections::VecDeque;

use rust_decimal::Decimal;

use crate::{IndicatorError, math::checked_sqrt};

const VARIANCE_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 18);
const MAX_ROLLING_WINDOW: usize = 1_000_000;

/// Rolling z-score over a fixed-size window using population variance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingZScore {
    window: usize,
    samples: VecDeque<Decimal>,
    mean: Decimal,
    squared_deviation_sum: Decimal,
}

impl RollingZScore {
    /// Creates a rolling z-score indicator.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::InvalidWindow`] when `window` is outside the
    /// supported `2..=1_000_000` range or its bounded sample buffer cannot be
    /// reserved.
    pub fn new(window: usize) -> Result<Self, IndicatorError> {
        if !(2..=MAX_ROLLING_WINDOW).contains(&window) {
            return Err(IndicatorError::InvalidWindow);
        }
        let mut samples = VecDeque::new();
        samples
            .try_reserve(window)
            .map_err(|_| IndicatorError::InvalidWindow)?;

        Ok(Self {
            window,
            samples,
            mean: Decimal::ZERO,
            squared_deviation_sum: Decimal::ZERO,
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
        let mut retained_len = self.samples.len();
        let mut retained_mean = self.mean;
        let mut retained_squared_deviation_sum = self.squared_deviation_sum;
        if let Some(value) = removed {
            let retained_count = retained_len
                .checked_sub(1)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let count = decimal_count(retained_len)?;
            let retained_count_decimal = decimal_count(retained_count)?;
            retained_mean = self
                .mean
                .checked_mul(count)
                .and_then(|weighted| weighted.checked_sub(value))
                .and_then(|weighted| weighted.checked_div(retained_count_decimal))
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let removed_from_old_mean = value
                .checked_sub(self.mean)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            let removed_from_retained_mean = value
                .checked_sub(retained_mean)
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            retained_squared_deviation_sum = self
                .squared_deviation_sum
                .checked_sub(
                    removed_from_old_mean
                        .checked_mul(removed_from_retained_mean)
                        .ok_or(IndicatorError::ArithmeticOverflow)?,
                )
                .ok_or(IndicatorError::ArithmeticOverflow)?;
            retained_len = retained_count;
        }

        let next_len = retained_len
            .checked_add(1)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let next_count = decimal_count(next_len)?;
        let delta = sample
            .checked_sub(retained_mean)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let next_mean = retained_mean
            .checked_add(
                delta
                    .checked_div(next_count)
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let delta_from_next_mean = sample
            .checked_sub(next_mean)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let next_squared_deviation_sum = retained_squared_deviation_sum
            .checked_add(
                delta
                    .checked_mul(delta_from_next_mean)
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
            .ok_or(IndicatorError::ArithmeticOverflow)?;

        let result = if next_len < self.window {
            None
        } else {
            let variance = next_squared_deviation_sum
                .checked_div(next_count)
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
                            .checked_sub(next_mean)
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
        self.mean = next_mean;
        self.squared_deviation_sum = next_squared_deviation_sum;
        Ok(result)
    }
}

fn decimal_count(count: usize) -> Result<Decimal, IndicatorError> {
    u64::try_from(count)
        .map(Decimal::from)
        .map_err(|_| IndicatorError::ArithmeticOverflow)
}
