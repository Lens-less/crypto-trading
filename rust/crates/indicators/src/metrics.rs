use rust_decimal::Decimal;

use crate::{IndicatorError, math::checked_sqrt};

/// Annualization configuration for Sharpe and Sortino ratios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RatioConfig {
    /// Number of observations per year.
    pub periods_per_year: Decimal,
    /// Risk-free rate expressed per observation period.
    pub risk_free_rate_per_period: Decimal,
}

impl RatioConfig {
    /// Creates a new ratio configuration.
    ///
    /// # Errors
    ///
    /// Returns [`IndicatorError::InvalidAnnualization`] when
    /// `periods_per_year <= 0`.
    pub fn new(
        periods_per_year: Decimal,
        risk_free_rate_per_period: Decimal,
    ) -> Result<Self, IndicatorError> {
        if periods_per_year <= Decimal::ZERO {
            return Err(IndicatorError::InvalidAnnualization);
        }

        Ok(Self {
            periods_per_year,
            risk_free_rate_per_period,
        })
    }
}

impl Default for RatioConfig {
    fn default() -> Self {
        Self {
            periods_per_year: Decimal::ONE,
            risk_free_rate_per_period: Decimal::ZERO,
        }
    }
}

/// Maximum drawdown measured from the highest equity peak to the deepest
/// subsequent trough.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drawdown {
    pub peak: Decimal,
    pub trough: Decimal,
    pub amount: Decimal,
    pub ratio: Decimal,
}

/// Aggregate performance summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerformanceMetrics {
    pub max_drawdown: Option<Drawdown>,
    /// Winning trades divided by total trades. Flat trades are counted in the
    /// denominator and not in the numerator. Returns `None` for an empty input.
    pub win_rate: Option<Decimal>,
    /// Gross profits divided by gross losses. Returns `None` when losses sum to
    /// zero because the denominator is undefined.
    pub profit_factor: Option<Decimal>,
    /// Mean excess return divided by sample standard deviation of excess
    /// returns, annualized by `sqrt(periods_per_year)`. Returns `None` when the
    /// sample has fewer than two observations or the denominator is zero.
    pub sharpe_ratio: Option<Decimal>,
    /// Mean excess return divided by downside deviation, annualized by
    /// `sqrt(periods_per_year)`. Downside deviation uses all observations in
    /// the denominator and only negative excess returns in the squared loss
    /// term. Returns `None` when the downside deviation is zero or no
    /// observations are supplied.
    pub sortino_ratio: Option<Decimal>,
}

/// Computes the maximum drawdown of an equity curve.
///
/// An empty curve is a valid unavailable metric (`Ok(None)`). Arithmetic
/// failure is reported separately and is never presented as missing data.
///
/// # Errors
///
/// Returns [`IndicatorError::ArithmeticOverflow`] when a drawdown amount or
/// ratio cannot be represented by [`Decimal`].
pub fn max_drawdown(equity_curve: &[Decimal]) -> Result<Option<Drawdown>, IndicatorError> {
    let Some((&first, rest)) = equity_curve.split_first() else {
        return Ok(None);
    };
    let mut peak = first;
    let mut best = Drawdown {
        peak,
        trough: first,
        amount: Decimal::ZERO,
        ratio: Decimal::ZERO,
    };

    for &equity in rest {
        if equity > peak {
            peak = equity;
            continue;
        }

        let amount = peak
            .checked_sub(equity)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let ratio = if peak.is_zero() {
            Decimal::ZERO
        } else {
            amount
                .checked_div(peak)
                .ok_or(IndicatorError::ArithmeticOverflow)?
        };

        if amount > best.amount {
            best = Drawdown {
                peak,
                trough: equity,
                amount,
                ratio,
            };
        }
    }

    Ok(Some(best))
}

/// Computes winning trades divided by total trades.
///
/// An empty sample is a valid unavailable metric (`Ok(None)`). Arithmetic
/// failure is reported separately.
///
/// # Errors
///
/// Returns [`IndicatorError::ArithmeticOverflow`] when the sample counts or
/// ratio cannot be represented by [`Decimal`].
pub fn win_rate(trade_pnls: &[Decimal]) -> Result<Option<Decimal>, IndicatorError> {
    if trade_pnls.is_empty() {
        return Ok(None);
    }

    let wins = trade_pnls
        .iter()
        .filter(|value| **value > Decimal::ZERO)
        .count();
    let wins = u64::try_from(wins).map_err(|_| IndicatorError::ArithmeticOverflow)?;
    let observations =
        u64::try_from(trade_pnls.len()).map_err(|_| IndicatorError::ArithmeticOverflow)?;
    Decimal::from(wins)
        .checked_div(Decimal::from(observations))
        .ok_or(IndicatorError::ArithmeticOverflow)
        .map(Some)
}

/// Computes profit factor from trade `PnL` samples.
///
/// An empty sample or a sample without any loss has no finite profit factor
/// and returns `Ok(None)`. Arithmetic failure is reported separately.
///
/// # Errors
///
/// Returns [`IndicatorError::ArithmeticOverflow`] when an aggregate or ratio
/// cannot be represented by [`Decimal`].
pub fn profit_factor(trade_pnls: &[Decimal]) -> Result<Option<Decimal>, IndicatorError> {
    if trade_pnls.is_empty() {
        return Ok(None);
    }
    let gross_profit = trade_pnls
        .iter()
        .copied()
        .filter(|value| *value > Decimal::ZERO)
        .try_fold(Decimal::ZERO, Decimal::checked_add)
        .ok_or(IndicatorError::ArithmeticOverflow)?;
    let gross_loss = trade_pnls
        .iter()
        .copied()
        .filter(|value| *value < Decimal::ZERO)
        .map(|value| value.abs())
        .try_fold(Decimal::ZERO, Decimal::checked_add)
        .ok_or(IndicatorError::ArithmeticOverflow)?;

    if gross_loss.is_zero() {
        return Ok(None);
    }

    gross_profit
        .checked_div(gross_loss)
        .ok_or(IndicatorError::ArithmeticOverflow)
        .map(Some)
}

/// Computes the annualized Sharpe ratio for periodic returns.
///
/// # Errors
///
/// Returns [`IndicatorError::InvalidAnnualization`] when
/// `config.periods_per_year <= 0`.
/// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
pub fn sharpe_ratio(
    returns: &[Decimal],
    config: RatioConfig,
) -> Result<Option<Decimal>, IndicatorError> {
    if config.periods_per_year <= Decimal::ZERO {
        return Err(IndicatorError::InvalidAnnualization);
    }
    if returns.len() < 2 {
        return Ok(None);
    }

    let excess_returns = excess_returns(returns, config.risk_free_rate_per_period)?;
    let mean = arithmetic_mean(&excess_returns).ok_or(IndicatorError::ArithmeticOverflow)?;
    let variance = sample_variance(&excess_returns)?.unwrap_or(Decimal::ZERO);
    if variance.is_zero() {
        return Ok(None);
    }

    let annualization = checked_sqrt(config.periods_per_year)?;
    let denominator = checked_sqrt(variance)?;

    mean.checked_div(denominator)
        .and_then(|ratio| ratio.checked_mul(annualization))
        .ok_or(IndicatorError::ArithmeticOverflow)
        .map(Some)
}

/// Computes the annualized Sortino ratio for periodic returns.
///
/// # Errors
///
/// Returns [`IndicatorError::InvalidAnnualization`] when
/// `config.periods_per_year <= 0`.
/// Returns [`IndicatorError::ArithmeticOverflow`] on Decimal overflow.
pub fn sortino_ratio(
    returns: &[Decimal],
    config: RatioConfig,
) -> Result<Option<Decimal>, IndicatorError> {
    if config.periods_per_year <= Decimal::ZERO {
        return Err(IndicatorError::InvalidAnnualization);
    }
    if returns.is_empty() {
        return Ok(None);
    }

    let excess_returns = excess_returns(returns, config.risk_free_rate_per_period)?;
    let mean = arithmetic_mean(&excess_returns).ok_or(IndicatorError::ArithmeticOverflow)?;

    let downside_sum = excess_returns
        .iter()
        .try_fold(Decimal::ZERO, |acc, value| {
            if *value >= Decimal::ZERO {
                Ok(acc)
            } else {
                acc.checked_add(
                    value
                        .checked_mul(*value)
                        .ok_or(IndicatorError::ArithmeticOverflow)?,
                )
                .ok_or(IndicatorError::ArithmeticOverflow)
            }
        })?;
    if downside_sum.is_zero() {
        return Ok(None);
    }

    let observations = Decimal::from(
        u64::try_from(excess_returns.len()).map_err(|_| IndicatorError::ArithmeticOverflow)?,
    );
    let downside_variance = downside_sum
        .checked_div(observations)
        .ok_or(IndicatorError::ArithmeticOverflow)?;
    if downside_variance.is_zero() {
        return Ok(None);
    }

    let annualization = checked_sqrt(config.periods_per_year)?;
    let downside_deviation = checked_sqrt(downside_variance)?;

    mean.checked_div(downside_deviation)
        .and_then(|ratio| ratio.checked_mul(annualization))
        .ok_or(IndicatorError::ArithmeticOverflow)
        .map(Some)
}

/// Computes a full performance summary from equity, trade `PnL`, and periodic
/// return samples.
///
/// # Errors
///
/// Propagates errors from ratio computations.
pub fn summarize_performance(
    equity_curve: &[Decimal],
    trade_pnls: &[Decimal],
    returns: &[Decimal],
    ratio_config: RatioConfig,
) -> Result<PerformanceMetrics, IndicatorError> {
    Ok(PerformanceMetrics {
        max_drawdown: max_drawdown(equity_curve)?,
        win_rate: win_rate(trade_pnls)?,
        profit_factor: profit_factor(trade_pnls)?,
        sharpe_ratio: sharpe_ratio(returns, ratio_config)?,
        sortino_ratio: sortino_ratio(returns, ratio_config)?,
    })
}

fn excess_returns(
    returns: &[Decimal],
    risk_free_rate_per_period: Decimal,
) -> Result<Vec<Decimal>, IndicatorError> {
    returns
        .iter()
        .copied()
        .map(|value| {
            value
                .checked_sub(risk_free_rate_per_period)
                .ok_or(IndicatorError::ArithmeticOverflow)
        })
        .collect()
}

fn arithmetic_mean(values: &[Decimal]) -> Option<Decimal> {
    let mut mean = Decimal::ZERO;
    for (index, value) in values.iter().enumerate() {
        let count = Decimal::from(u64::try_from(index.checked_add(1)?).ok()?);
        mean = mean.checked_add(value.checked_sub(mean)?.checked_div(count)?)?;
    }
    Some(mean)
}

fn sample_variance(values: &[Decimal]) -> Result<Option<Decimal>, IndicatorError> {
    if values.len() < 2 {
        return Ok(None);
    }

    let mut mean = Decimal::ZERO;
    let mut squared_distance_sum = Decimal::ZERO;
    for (index, value) in values.iter().enumerate() {
        let count = Decimal::from(
            u64::try_from(
                index
                    .checked_add(1)
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
            .map_err(|_| IndicatorError::ArithmeticOverflow)?,
        );
        let delta = value
            .checked_sub(mean)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        mean = mean
            .checked_add(
                delta
                    .checked_div(count)
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        let adjusted_delta = value
            .checked_sub(mean)
            .ok_or(IndicatorError::ArithmeticOverflow)?;
        squared_distance_sum = squared_distance_sum
            .checked_add(
                delta
                    .checked_mul(adjusted_delta)
                    .ok_or(IndicatorError::ArithmeticOverflow)?,
            )
            .ok_or(IndicatorError::ArithmeticOverflow)?;
    }
    let denominator = Decimal::from(
        u64::try_from(values.len().saturating_sub(1))
            .map_err(|_| IndicatorError::ArithmeticOverflow)?,
    );

    squared_distance_sum
        .checked_div(denominator)
        .ok_or(IndicatorError::ArithmeticOverflow)
        .map(Some)
}
