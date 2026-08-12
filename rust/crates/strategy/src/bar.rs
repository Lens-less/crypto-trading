use chrono::{DateTime, Utc};
use crypto_trading_domain::Price;
use rust_decimal::Decimal;

use crate::StrategyError;

/// One closed OHLCV bar consumed by pure bar-driven research strategies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bar {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub trade_count: u64,
}

impl Bar {
    /// Builds one validated closed bar.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidFinancialValue`] when the timestamp or
    /// OHLCV shape is invalid.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        open_time: DateTime<Utc>,
        close_time: DateTime<Utc>,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Decimal,
        quote_volume: Decimal,
        trade_count: u64,
    ) -> Result<Self, StrategyError> {
        if close_time < open_time
            || high < low
            || high < open
            || high < close
            || low > open
            || low > close
            || volume.is_sign_negative()
            || quote_volume.is_sign_negative()
        {
            return Err(StrategyError::InvalidFinancialValue("bar"));
        }

        Ok(Self {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
        })
    }
}

/// Bounded long-only target exposure in `[0, 1]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetExposure(Decimal);

impl TargetExposure {
    /// Flat target exposure.
    pub const ZERO: Self = Self(Decimal::ZERO);

    /// Fully invested long target exposure.
    pub const ONE: Self = Self(Decimal::ONE);

    /// Validates and wraps a target exposure.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidFinancialValue`] when the value is not
    /// in `[0, 1]`.
    pub fn new(value: Decimal) -> Result<Self, StrategyError> {
        if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
            return Err(StrategyError::InvalidFinancialValue("target exposure"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn as_decimal(self) -> Decimal {
        self.0
    }
}

/// Immutable decision context for one completed bar close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarStrategyContext<'a> {
    pub history: &'a [Bar],
    pub decided_at: DateTime<Utc>,
    pub bar_index: usize,
    pub current_target: Decimal,
}

/// Pure research strategy that only consumes completed bars.
pub trait BarStrategy {
    /// Returns the next bounded long-only target exposure.
    ///
    /// # Errors
    ///
    /// Returns a typed pure strategy error when the target cannot be formed.
    fn target_exposure(
        &mut self,
        context: &BarStrategyContext<'_>,
    ) -> Result<TargetExposure, StrategyError>;
}
