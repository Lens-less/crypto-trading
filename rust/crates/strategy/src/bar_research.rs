use rust_decimal::Decimal;

use crate::{BarStrategy, BarStrategyContext, StrategyError, TargetExposure};

/// Mandatory abstention baseline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CashStrategy;

impl BarStrategy for CashStrategy {
    fn target_exposure(
        &mut self,
        _context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, StrategyError> {
        TargetExposure::new(Decimal::ZERO)
    }
}

/// Mandatory cost-matched passive spot baseline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BuyAndHoldStrategy {
    entered: bool,
}

impl BarStrategy for BuyAndHoldStrategy {
    fn target_exposure(
        &mut self,
        context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, StrategyError> {
        if self.entered {
            TargetExposure::new(context.current_target)
        } else {
            self.entered = true;
            TargetExposure::new(Decimal::ONE)
        }
    }
}

/// Long-or-cash sign of a completed trailing return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlowTimeSeriesMomentum {
    lookback_bars: usize,
    rebalance_every_bars: usize,
}

impl SlowTimeSeriesMomentum {
    /// Creates a bounded trailing-return strategy.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] when either count is zero.
    pub fn new(lookback_bars: usize, rebalance_every_bars: usize) -> Result<Self, StrategyError> {
        if lookback_bars == 0 || rebalance_every_bars == 0 {
            return Err(StrategyError::InvalidConfig(
                "bar research windows must be strictly positive",
            ));
        }
        Ok(Self {
            lookback_bars,
            rebalance_every_bars,
        })
    }
}

impl BarStrategy for SlowTimeSeriesMomentum {
    fn target_exposure(
        &mut self,
        context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, StrategyError> {
        if context.history.len() <= self.lookback_bars {
            return TargetExposure::new(Decimal::ZERO);
        }
        if !context.bar_index.is_multiple_of(self.rebalance_every_bars) {
            return TargetExposure::new(context.current_target);
        }

        let current = context
            .history
            .last()
            .ok_or(StrategyError::InvalidFinancialValue("bar history"))?
            .close
            .as_decimal();
        let trailing = context.history[context.history.len() - self.lookback_bars - 1]
            .close
            .as_decimal();
        TargetExposure::new(if current > trailing {
            Decimal::ONE
        } else {
            Decimal::ZERO
        })
    }
}

/// Long-only close-channel breakout with a non-decreasing midpoint exit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LongOnlyDonchian {
    lookback_bars: usize,
    trailing_exit: Option<Decimal>,
}

impl LongOnlyDonchian {
    /// Creates a fixed-window channel adapter.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a zero lookback.
    pub const fn new(lookback_bars: usize) -> Result<Self, StrategyError> {
        if lookback_bars == 0 {
            return Err(StrategyError::InvalidConfig(
                "donchian lookback must be strictly positive",
            ));
        }
        Ok(Self {
            lookback_bars,
            trailing_exit: None,
        })
    }
}

impl BarStrategy for LongOnlyDonchian {
    fn target_exposure(
        &mut self,
        context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, StrategyError> {
        if context.history.len() <= self.lookback_bars {
            self.trailing_exit = None;
            return TargetExposure::new(Decimal::ZERO);
        }

        let current = context
            .history
            .last()
            .ok_or(StrategyError::InvalidFinancialValue("bar history"))?
            .close
            .as_decimal();
        if context.current_target.is_zero() {
            self.trailing_exit = None;
            let prior = &context.history
                [context.history.len() - self.lookback_bars - 1..context.history.len() - 1];
            let prior_high = prior
                .iter()
                .map(|bar| bar.close.as_decimal())
                .max()
                .ok_or(StrategyError::InvalidFinancialValue("bar history"))?;
            return TargetExposure::new(if current > prior_high {
                Decimal::ONE
            } else {
                Decimal::ZERO
            });
        }

        let channel = &context.history[context.history.len() - self.lookback_bars..];
        let high = channel
            .iter()
            .map(|bar| bar.close.as_decimal())
            .max()
            .ok_or(StrategyError::InvalidFinancialValue("bar history"))?;
        let low = channel
            .iter()
            .map(|bar| bar.close.as_decimal())
            .min()
            .ok_or(StrategyError::InvalidFinancialValue("bar history"))?;
        let midpoint = high
            .checked_add(low)
            .and_then(|sum| sum.checked_div(Decimal::from(2_u32)))
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
        let trailing_exit = self
            .trailing_exit
            .map_or(midpoint, |previous| previous.max(midpoint));
        self.trailing_exit = Some(trailing_exit);
        if current < trailing_exit {
            self.trailing_exit = None;
            TargetExposure::new(Decimal::ZERO)
        } else {
            TargetExposure::new(Decimal::ONE)
        }
    }
}

/// Long-only exposure capped by completed-close realized volatility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CappedVolatilityTarget {
    lookback_returns: usize,
    annual_target: Decimal,
    rebalance_band: Decimal,
    rebalance_every_bars: usize,
    periods_per_year: Decimal,
}

impl CappedVolatilityTarget {
    /// Creates a rolling volatility target.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] unless the lookback has at
    /// least two returns, the target is in `(0, 1]`, and the rebalance band is
    /// in `[0, 1]`.
    pub fn new(
        lookback_returns: usize,
        annual_target: Decimal,
        rebalance_band: Decimal,
        rebalance_every_bars: usize,
    ) -> Result<Self, StrategyError> {
        Self::new_with_periods_per_year(
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
            Decimal::from(365_u32),
        )
    }

    /// Creates a rolling volatility target with explicit annualization.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] unless the lookback has at
    /// least two returns, the target is in `(0, 1]`, the rebalance band is in
    /// `[0, 1]`, and `periods_per_year > 0`.
    pub fn new_with_periods_per_year(
        lookback_returns: usize,
        annual_target: Decimal,
        rebalance_band: Decimal,
        rebalance_every_bars: usize,
        periods_per_year: Decimal,
    ) -> Result<Self, StrategyError> {
        if lookback_returns < 2
            || annual_target <= Decimal::ZERO
            || annual_target > Decimal::ONE
            || rebalance_band < Decimal::ZERO
            || rebalance_band > Decimal::ONE
            || rebalance_every_bars == 0
            || periods_per_year <= Decimal::ZERO
        {
            return Err(StrategyError::InvalidConfig(
                "volatility target parameters are outside the bounded domain",
            ));
        }
        Ok(Self {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
            periods_per_year,
        })
    }
}

impl BarStrategy for CappedVolatilityTarget {
    fn target_exposure(
        &mut self,
        context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, StrategyError> {
        if context.history.len() <= self.lookback_returns {
            return TargetExposure::new(Decimal::ZERO);
        }
        if !context.bar_index.is_multiple_of(self.rebalance_every_bars) {
            return TargetExposure::new(context.current_target);
        }
        let start = context.history.len() - self.lookback_returns - 1;
        let closes = &context.history[start..];
        let returns = closes
            .windows(2)
            .map(|pair| {
                pair[1]
                    .close
                    .as_decimal()
                    .checked_sub(pair[0].close.as_decimal())
                    .and_then(|change| change.checked_div(pair[0].close.as_decimal()))
                    .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = Decimal::from(
            u64::try_from(returns.len())
                .map_err(|_| StrategyError::InvalidFinancialValue("arithmetic"))?,
        );
        let mean = returns
            .iter()
            .try_fold(Decimal::ZERO, |sum, value| {
                sum.checked_add(*value)
                    .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))
            })?
            .checked_div(count)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
        let squared_deviations = returns.iter().try_fold(Decimal::ZERO, |sum, value| {
            let deviation = value
                .checked_sub(mean)
                .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
            sum.checked_add(
                deviation
                    .checked_mul(deviation)
                    .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?,
            )
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))
        })?;
        let denominator = count
            .checked_sub(Decimal::ONE)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
        let annual_variance = squared_deviations
            .checked_div(denominator)
            .and_then(|variance| variance.checked_mul(self.periods_per_year))
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
        if annual_variance.is_zero() {
            return TargetExposure::new(Decimal::ZERO);
        }
        let annual_volatility = checked_sqrt(annual_variance)?;
        let desired = self
            .annual_target
            .checked_div(annual_volatility)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?
            .min(Decimal::ONE);
        let band = context
            .current_target
            .checked_mul(self.rebalance_band)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
        let difference = desired
            .checked_sub(context.current_target)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?
            .abs();
        TargetExposure::new(if difference <= band {
            context.current_target
        } else {
            desired
        })
    }
}

fn checked_sqrt(value: Decimal) -> Result<Decimal, StrategyError> {
    if value < Decimal::ZERO {
        return Err(StrategyError::InvalidFinancialValue("arithmetic"));
    }
    if value.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let two = Decimal::from(2_u32);
    let mut guess = if value > Decimal::ONE {
        value
            .checked_div(two)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?
    } else {
        Decimal::ONE
    };
    let tolerance = Decimal::from_parts(1, 0, 0, false, 18);
    for _ in 0..64 {
        let next = guess
            .checked_add(
                value
                    .checked_div(guess)
                    .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?,
            )
            .and_then(|sum| sum.checked_div(two))
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?;
        if next
            .checked_sub(guess)
            .ok_or(StrategyError::InvalidFinancialValue("arithmetic"))?
            .abs()
            <= tolerance
        {
            return Ok(next.round_dp(18));
        }
        guess = next;
    }
    Ok(guess.round_dp(18))
}
