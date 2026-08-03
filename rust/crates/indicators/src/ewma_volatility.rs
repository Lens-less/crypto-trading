use rust_decimal::Decimal;

use crate::{IndicatorError, math::checked_sqrt};

/// Incremental EWMA realized volatility over simple price returns.
///
/// The first price seeds the return clock and produces no value. The first
/// return seeds variance with its squared value; later samples apply
/// `variance = decay * variance + (1 - decay) * return^2`. The result is the
/// per-observation standard deviation. Callers choose any annualization
/// separately so replay and live code share identical raw arithmetic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EwmaRealizedVolatility {
    decay: Decimal,
    previous_price: Option<Decimal>,
    variance: Option<Decimal>,
}

impl EwmaRealizedVolatility {
    /// Creates an EWMA volatility estimator with `0 <= decay < 1`.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::InvalidDecay`] outside the accepted interval.
    pub fn new(decay: Decimal) -> Result<Self, IndicatorError> {
        if decay < Decimal::ZERO || decay >= Decimal::ONE {
            return Err(IndicatorError::InvalidDecay);
        }
        Ok(Self {
            decay,
            previous_price: None,
            variance: None,
        })
    }

    /// Updates the estimator with a strictly positive price.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::NonPositivePrice`] for an invalid price and
    /// [`IndicatorError::ArithmeticOverflow`] when Decimal arithmetic cannot
    /// represent an intermediate result.
    pub fn update(&mut self, price: Decimal) -> Result<Option<Decimal>, IndicatorError> {
        if price <= Decimal::ZERO {
            return Err(IndicatorError::NonPositivePrice);
        }
        let Some(previous_price) = self.previous_price else {
            self.previous_price = Some(price);
            return Ok(None);
        };
        let price_delta = price
            .checked_sub(previous_price)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let simple_return = price_delta
            .checked_div(previous_price)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let squared_return = simple_return
            .checked_mul(simple_return)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let next_variance = match self.variance {
            None => squared_return,
            Some(previous_variance) => {
                let retained = self
                    .decay
                    .checked_mul(previous_variance)
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                let innovation_weight = Decimal::ONE
                    .checked_sub(self.decay)
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                let innovation = innovation_weight
                    .checked_mul(squared_return)
                    .ok_or(IndicatorError::ArithmeticOverflow)?;
                retained
                    .checked_add(innovation)
                    .ok_or(IndicatorError::ArithmeticOverflow)?
            }
        };
        let volatility = checked_sqrt(next_variance)?;
        self.previous_price = Some(price);
        self.variance = Some(next_variance);
        Ok(Some(volatility))
    }

    /// Returns the latest per-observation variance.
    #[must_use]
    pub const fn variance(&self) -> Option<Decimal> {
        self.variance
    }
}
