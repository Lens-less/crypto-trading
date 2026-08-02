//! Deterministic Decimal-based indicators and performance metrics.
//!
//! The public API intentionally stays incremental and allocation-light so it
//! can be used in both backtests and future live projections without changing
//! arithmetic semantics.

mod atr;
mod ema;
mod ewma_volatility;
mod math;
mod metrics;
mod zscore;

pub use atr::Atr;
pub use ema::Ema;
pub use ewma_volatility::EwmaRealizedVolatility;
pub use metrics::{
    Drawdown, PerformanceMetrics, RatioConfig, max_drawdown, profit_factor, sharpe_ratio,
    sortino_ratio, summarize_performance, win_rate,
};
pub use zscore::RollingZScore;

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IndicatorError {
    #[error("indicator period must be positive")]
    InvalidPeriod,
    #[error("window size must be within the supported bounded range")]
    InvalidWindow,
    #[error("price inputs must be strictly positive")]
    NonPositivePrice,
    #[error("high price must be greater than or equal to low price")]
    CrossedRange,
    #[error("risk metric annualization must be strictly positive")]
    InvalidAnnualization,
    #[error("EWMA decay must be in the half-open interval [0, 1)")]
    InvalidDecay,
    #[error("decimal arithmetic overflow")]
    ArithmeticOverflow,
    #[error("square root input must be non-negative")]
    NegativeSquareRoot,
}
