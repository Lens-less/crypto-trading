use rust_decimal::Decimal;

use crate::{BacktestError, SpotDecisionContext, TargetExposureStrategy};

/// Concrete, pre-registered Spot-only research configurations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpotStrategyConfig {
    Cash,
    BuyAndHold,
    SlowTimeSeriesMomentum {
        lookback_bars: usize,
        rebalance_every_bars: usize,
    },
    LongOnlyDonchian {
        lookback_bars: usize,
    },
    CappedVolatilityTarget {
        lookback_returns: usize,
        annual_target: Decimal,
        rebalance_band: Decimal,
        rebalance_every_bars: usize,
    },
}

impl SpotStrategyConfig {
    /// Returns the stable family label used for bounded search accounting.
    #[must_use]
    pub const fn family(self) -> &'static str {
        match self {
            Self::Cash => "cash",
            Self::BuyAndHold => "buy_and_hold",
            Self::SlowTimeSeriesMomentum { .. } => "slow_time_series_momentum",
            Self::LongOnlyDonchian { .. } => "long_only_donchian",
            Self::CappedVolatilityTarget { .. } => "capped_volatility_target",
        }
    }

    /// Builds the corresponding bounded Spot strategy.
    ///
    /// # Errors
    ///
    /// Propagates [`BacktestError::InvalidStrategyConfiguration`] from the
    /// underlying bounded constructors.
    pub fn build(self) -> Result<BoundedSpotStrategy, BacktestError> {
        match self {
            Self::Cash => Ok(BoundedSpotStrategy::Cash(CashStrategy)),
            Self::BuyAndHold => Ok(BoundedSpotStrategy::BuyAndHold(
                BuyAndHoldStrategy::default(),
            )),
            Self::SlowTimeSeriesMomentum {
                lookback_bars,
                rebalance_every_bars,
            } => Ok(BoundedSpotStrategy::SlowTimeSeriesMomentum(
                SlowTimeSeriesMomentum::new(lookback_bars, rebalance_every_bars)?,
            )),
            Self::LongOnlyDonchian { lookback_bars } => Ok(BoundedSpotStrategy::LongOnlyDonchian(
                LongOnlyDonchian::new(lookback_bars)?,
            )),
            Self::CappedVolatilityTarget {
                lookback_returns,
                annual_target,
                rebalance_band,
                rebalance_every_bars,
            } => Ok(BoundedSpotStrategy::CappedVolatilityTarget(
                CappedVolatilityTarget::new(
                    lookback_returns,
                    annual_target,
                    rebalance_band,
                    rebalance_every_bars,
                )?,
            )),
        }
    }
}

/// Dispatch wrapper for every bounded Spot research family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedSpotStrategy {
    Cash(CashStrategy),
    BuyAndHold(BuyAndHoldStrategy),
    SlowTimeSeriesMomentum(SlowTimeSeriesMomentum),
    LongOnlyDonchian(LongOnlyDonchian),
    CappedVolatilityTarget(CappedVolatilityTarget),
}

impl TargetExposureStrategy for BoundedSpotStrategy {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        match self {
            Self::Cash(strategy) => strategy.target_exposure(context),
            Self::BuyAndHold(strategy) => strategy.target_exposure(context),
            Self::SlowTimeSeriesMomentum(strategy) => strategy.target_exposure(context),
            Self::LongOnlyDonchian(strategy) => strategy.target_exposure(context),
            Self::CappedVolatilityTarget(strategy) => strategy.target_exposure(context),
        }
    }
}

/// Mandatory abstention baseline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CashStrategy;

impl TargetExposureStrategy for CashStrategy {
    fn target_exposure(
        &mut self,
        _context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        Ok(Decimal::ZERO)
    }
}

/// Mandatory cost-matched passive Spot baseline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BuyAndHoldStrategy {
    entered: bool,
}

impl TargetExposureStrategy for BuyAndHoldStrategy {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        if self.entered {
            Ok(context.current_target)
        } else {
            self.entered = true;
            Ok(Decimal::ONE)
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
    /// Returns [`BacktestError::InvalidStrategyConfiguration`] when either
    /// count is zero.
    pub fn new(lookback_bars: usize, rebalance_every_bars: usize) -> Result<Self, BacktestError> {
        if lookback_bars == 0 || rebalance_every_bars == 0 {
            return Err(BacktestError::InvalidStrategyConfiguration);
        }
        Ok(Self {
            lookback_bars,
            rebalance_every_bars,
        })
    }
}

impl TargetExposureStrategy for SlowTimeSeriesMomentum {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        if context.history.len() <= self.lookback_bars {
            return Ok(Decimal::ZERO);
        }
        if !context.bar_index.is_multiple_of(self.rebalance_every_bars) {
            return Ok(context.current_target);
        }

        let current = context
            .history
            .last()
            .ok_or(BacktestError::InvalidEvaluationRange)?
            .close
            .as_decimal();
        let trailing = context.history[context.history.len() - self.lookback_bars - 1]
            .close
            .as_decimal();
        Ok(if current > trailing {
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
    /// Returns [`BacktestError::InvalidStrategyConfiguration`] for a zero
    /// lookback.
    pub const fn new(lookback_bars: usize) -> Result<Self, BacktestError> {
        if lookback_bars == 0 {
            return Err(BacktestError::InvalidStrategyConfiguration);
        }
        Ok(Self {
            lookback_bars,
            trailing_exit: None,
        })
    }
}

impl TargetExposureStrategy for LongOnlyDonchian {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        if context.history.len() <= self.lookback_bars {
            self.trailing_exit = None;
            return Ok(Decimal::ZERO);
        }

        let current = context
            .history
            .last()
            .ok_or(BacktestError::InvalidEvaluationRange)?
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
                .ok_or(BacktestError::InvalidEvaluationRange)?;
            return Ok(if current > prior_high {
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
            .ok_or(BacktestError::InvalidEvaluationRange)?;
        let low = channel
            .iter()
            .map(|bar| bar.close.as_decimal())
            .min()
            .ok_or(BacktestError::InvalidEvaluationRange)?;
        let midpoint = high
            .checked_add(low)
            .and_then(|sum| sum.checked_div(Decimal::from(2_u32)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let trailing_exit = self
            .trailing_exit
            .map_or(midpoint, |previous| previous.max(midpoint));
        self.trailing_exit = Some(trailing_exit);
        if current < trailing_exit {
            self.trailing_exit = None;
            Ok(Decimal::ZERO)
        } else {
            Ok(Decimal::ONE)
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
}

impl CappedVolatilityTarget {
    /// Creates a rolling volatility target.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidStrategyConfiguration`] unless the
    /// lookback has at least two returns, the target is in `(0, 1]`, and the
    /// rebalance band is in `[0, 1]`.
    pub fn new(
        lookback_returns: usize,
        annual_target: Decimal,
        rebalance_band: Decimal,
        rebalance_every_bars: usize,
    ) -> Result<Self, BacktestError> {
        if lookback_returns < 2
            || annual_target <= Decimal::ZERO
            || annual_target > Decimal::ONE
            || rebalance_band < Decimal::ZERO
            || rebalance_band > Decimal::ONE
            || rebalance_every_bars == 0
        {
            return Err(BacktestError::InvalidStrategyConfiguration);
        }
        Ok(Self {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
        })
    }
}

impl TargetExposureStrategy for CappedVolatilityTarget {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        if context.history.len() <= self.lookback_returns {
            return Ok(Decimal::ZERO);
        }
        if !context.bar_index.is_multiple_of(self.rebalance_every_bars) {
            return Ok(context.current_target);
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
                    .ok_or(BacktestError::ArithmeticOverflow)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = Decimal::from(
            u64::try_from(returns.len()).map_err(|_| BacktestError::ArithmeticOverflow)?,
        );
        let mean = returns
            .iter()
            .try_fold(Decimal::ZERO, |sum, value| {
                sum.checked_add(*value)
                    .ok_or(BacktestError::ArithmeticOverflow)
            })?
            .checked_div(count)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let squared_deviations = returns.iter().try_fold(Decimal::ZERO, |sum, value| {
            let deviation = value
                .checked_sub(mean)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            sum.checked_add(
                deviation
                    .checked_mul(deviation)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .ok_or(BacktestError::ArithmeticOverflow)
        })?;
        let denominator = count
            .checked_sub(Decimal::ONE)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let annual_variance = squared_deviations
            .checked_div(denominator)
            .and_then(|variance| variance.checked_mul(Decimal::from(365_u32)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if annual_variance.is_zero() {
            return Ok(Decimal::ZERO);
        }
        let annual_volatility = checked_sqrt(annual_variance)?;
        let desired = self
            .annual_target
            .checked_div(annual_volatility)
            .ok_or(BacktestError::ArithmeticOverflow)?
            .min(Decimal::ONE);
        let band = context
            .current_target
            .checked_mul(self.rebalance_band)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let difference = desired
            .checked_sub(context.current_target)
            .ok_or(BacktestError::ArithmeticOverflow)?
            .abs();
        Ok(if difference <= band {
            context.current_target
        } else {
            desired
        })
    }
}

fn checked_sqrt(value: Decimal) -> Result<Decimal, BacktestError> {
    if value < Decimal::ZERO {
        return Err(BacktestError::ArithmeticOverflow);
    }
    if value.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let two = Decimal::from(2_u32);
    let mut guess = if value > Decimal::ONE {
        value
            .checked_div(two)
            .ok_or(BacktestError::ArithmeticOverflow)?
    } else {
        Decimal::ONE
    };
    let tolerance = Decimal::from_parts(1, 0, 0, false, 18);
    for _ in 0..64 {
        let next = guess
            .checked_add(
                value
                    .checked_div(guess)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .and_then(|sum| sum.checked_div(two))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if next
            .checked_sub(guess)
            .ok_or(BacktestError::ArithmeticOverflow)?
            .abs()
            <= tolerance
        {
            return Ok(next.round_dp(18));
        }
        guess = next;
    }
    Ok(guess.round_dp(18))
}

#[cfg(test)]
mod tests {
    use super::{
        BoundedSpotStrategy, BuyAndHoldStrategy, CashStrategy, LongOnlyDonchian,
        SlowTimeSeriesMomentum, SpotStrategyConfig,
    };
    use crate::{BacktestError, CappedVolatilityTarget};
    use rust_decimal::Decimal;

    fn decimal(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    #[test]
    fn spot_strategy_config_families_are_stable() {
        assert_eq!(SpotStrategyConfig::Cash.family(), "cash");
        assert_eq!(SpotStrategyConfig::BuyAndHold.family(), "buy_and_hold");
        assert_eq!(
            SpotStrategyConfig::SlowTimeSeriesMomentum {
                lookback_bars: 12,
                rebalance_every_bars: 1,
            }
            .family(),
            "slow_time_series_momentum"
        );
        assert_eq!(
            SpotStrategyConfig::LongOnlyDonchian { lookback_bars: 20 }.family(),
            "long_only_donchian"
        );
        assert_eq!(
            SpotStrategyConfig::CappedVolatilityTarget {
                lookback_returns: 20,
                annual_target: decimal("0.15"),
                rebalance_band: decimal("0.20"),
                rebalance_every_bars: 5,
            }
            .family(),
            "capped_volatility_target"
        );
    }

    #[test]
    fn build_dispatches_to_existing_bounded_constructors() {
        assert_eq!(
            SpotStrategyConfig::Cash.build().unwrap(),
            BoundedSpotStrategy::Cash(CashStrategy)
        );
        assert_eq!(
            SpotStrategyConfig::BuyAndHold.build().unwrap(),
            BoundedSpotStrategy::BuyAndHold(BuyAndHoldStrategy::default())
        );
        assert_eq!(
            SpotStrategyConfig::SlowTimeSeriesMomentum {
                lookback_bars: 12,
                rebalance_every_bars: 4,
            }
            .build()
            .unwrap(),
            BoundedSpotStrategy::SlowTimeSeriesMomentum(
                SlowTimeSeriesMomentum::new(12, 4).unwrap()
            )
        );
        assert_eq!(
            SpotStrategyConfig::LongOnlyDonchian { lookback_bars: 60 }
                .build()
                .unwrap(),
            BoundedSpotStrategy::LongOnlyDonchian(LongOnlyDonchian::new(60).unwrap())
        );
        assert_eq!(
            SpotStrategyConfig::CappedVolatilityTarget {
                lookback_returns: 20,
                annual_target: decimal("0.15"),
                rebalance_band: decimal("0.20"),
                rebalance_every_bars: 5,
            }
            .build()
            .unwrap(),
            BoundedSpotStrategy::CappedVolatilityTarget(
                CappedVolatilityTarget::new(20, decimal("0.15"), decimal("0.20"), 5).unwrap()
            )
        );
    }

    #[test]
    fn invalid_registered_configurations_fail_closed() {
        assert_eq!(
            SpotStrategyConfig::SlowTimeSeriesMomentum {
                lookback_bars: 0,
                rebalance_every_bars: 1,
            }
            .build(),
            Err(BacktestError::InvalidStrategyConfiguration)
        );
        assert_eq!(
            SpotStrategyConfig::LongOnlyDonchian { lookback_bars: 0 }.build(),
            Err(BacktestError::InvalidStrategyConfiguration)
        );
        assert_eq!(
            SpotStrategyConfig::CappedVolatilityTarget {
                lookback_returns: 1,
                annual_target: Decimal::ONE,
                rebalance_band: Decimal::ZERO,
                rebalance_every_bars: 1,
            }
            .build(),
            Err(BacktestError::InvalidStrategyConfiguration)
        );
        assert_eq!(
            SpotStrategyConfig::CappedVolatilityTarget {
                lookback_returns: 2,
                annual_target: Decimal::ONE,
                rebalance_band: Decimal::ZERO,
                rebalance_every_bars: 0,
            }
            .build(),
            Err(BacktestError::InvalidStrategyConfiguration)
        );
    }
}
