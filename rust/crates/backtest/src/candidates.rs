use crypto_trading_strategy::{
    BarStrategy, BarStrategyContext, BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy,
    LongOnlyDonchian, SlowTimeSeriesMomentum, StrategyError,
};
use rust_decimal::Decimal;

use crate::{BacktestError, SpotDecisionContext, TargetExposureStrategy};

/// Concrete, pre-registered spot-only research configurations.
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
    CappedVolatilityTargetExplicitAnnualization {
        lookback_returns: usize,
        annual_target: Decimal,
        rebalance_band: Decimal,
        rebalance_every_bars: usize,
        periods_per_year: Decimal,
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
            Self::CappedVolatilityTarget { .. }
            | Self::CappedVolatilityTargetExplicitAnnualization { .. } => {
                "capped_volatility_target"
            }
        }
    }

    /// Builds the corresponding bounded spot strategy.
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
                SlowTimeSeriesMomentum::new(lookback_bars, rebalance_every_bars)
                    .map_err(map_strategy_error)?,
            )),
            Self::LongOnlyDonchian { lookback_bars } => Ok(BoundedSpotStrategy::LongOnlyDonchian(
                LongOnlyDonchian::new(lookback_bars).map_err(map_strategy_error)?,
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
                )
                .map_err(map_strategy_error)?,
            )),
            Self::CappedVolatilityTargetExplicitAnnualization {
                lookback_returns,
                annual_target,
                rebalance_band,
                rebalance_every_bars,
                periods_per_year,
            } => Ok(BoundedSpotStrategy::CappedVolatilityTarget(
                CappedVolatilityTarget::new_with_periods_per_year(
                    lookback_returns,
                    annual_target,
                    rebalance_band,
                    rebalance_every_bars,
                    periods_per_year,
                )
                .map_err(map_strategy_error)?,
            )),
        }
    }
}

/// Dispatch wrapper for every bounded spot research family.
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
            Self::Cash(strategy) => TargetExposureStrategy::target_exposure(strategy, context),
            Self::BuyAndHold(strategy) => {
                TargetExposureStrategy::target_exposure(strategy, context)
            }
            Self::SlowTimeSeriesMomentum(strategy) => {
                TargetExposureStrategy::target_exposure(strategy, context)
            }
            Self::LongOnlyDonchian(strategy) => {
                TargetExposureStrategy::target_exposure(strategy, context)
            }
            Self::CappedVolatilityTarget(strategy) => {
                TargetExposureStrategy::target_exposure(strategy, context)
            }
        }
    }
}

impl TargetExposureStrategy for CashStrategy {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        decide_shared(self, context)
    }
}

impl TargetExposureStrategy for BuyAndHoldStrategy {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        decide_shared(self, context)
    }
}

impl TargetExposureStrategy for SlowTimeSeriesMomentum {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        decide_shared(self, context)
    }
}

impl TargetExposureStrategy for LongOnlyDonchian {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        decide_shared(self, context)
    }
}

impl TargetExposureStrategy for CappedVolatilityTarget {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        decide_shared(self, context)
    }
}

fn decide_shared<S: BarStrategy>(
    strategy: &mut S,
    context: &SpotDecisionContext<'_>,
) -> Result<Decimal, BacktestError> {
    let context = BarStrategyContext {
        history: context.history,
        decided_at: context.decided_at,
        bar_index: context.bar_index,
        current_target: context.current_target,
    };
    strategy
        .target_exposure(&context)
        .map(crypto_trading_strategy::TargetExposure::as_decimal)
        .map_err(map_strategy_error)
}

#[allow(clippy::needless_pass_by_value)]
fn map_strategy_error(error: StrategyError) -> BacktestError {
    match error {
        StrategyError::InvalidConfig(_) => BacktestError::InvalidStrategyConfiguration,
        StrategyError::InvalidFinancialValue("target exposure") => {
            BacktestError::InvalidTargetExposure
        }
        StrategyError::InvalidFinancialValue("bar") => BacktestError::InvalidBarSequence,
        StrategyError::InvalidFinancialValue(_) => BacktestError::ArithmeticOverflow,
        StrategyError::SnapshotMismatch(_) | StrategyError::MissingMarketData(_) => {
            BacktestError::InvalidEvaluationRange
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedSpotStrategy, SpotStrategyConfig};
    use crate::{
        BacktestError, BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy, LongOnlyDonchian,
        SlowTimeSeriesMomentum,
    };
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

    #[test]
    fn spot_bar_history_is_shared_bar_history_without_rebuilding() {
        fn shared_history_len(history: &[crypto_trading_strategy::Bar]) -> usize {
            history.len()
        }

        let history: &[crate::SpotBar] = &[];
        assert_eq!(shared_history_len(history), 0);
    }
}
