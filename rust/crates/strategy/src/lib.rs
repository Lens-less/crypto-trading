//! Pure strategy machines for the Rust trading runtime.
//!
//! The modules in this crate do not perform I/O. They translate validated
//! configuration, explicit state, and market snapshots into deterministic
//! decisions that the runtime may execute or record.

mod alert;
mod arbitrage;
mod grid;
mod risk;
mod virtual_grid;
mod volume_maker;

use crypto_trading_domain::{MarketSnapshot, OrderIntent};
use thiserror::Error;

pub use alert::{
    AlertConfig, AlertKind, AlertState, AlertStrategy, PriceAlert, PricePoint,
    VolatilityAlertConfig,
};
pub use arbitrage::{
    ArbitrageDecision, ArbitrageDecisionKind, ArbitrageDirection, ArbitrageState,
    ArbitrageStrategy, PairStrategyMachine, SegmentedArbitrageConfig, SpreadCalculator,
    SpreadQuote,
};
pub use grid::{
    GridDirection, GridLevel, GridPlanConfig, GridPlanner, GridRange, GridState, GridStrategy,
};
pub use risk::{AccountRiskSnapshot, RiskDecision, RiskEngine, RiskLimits, RiskRejection};
pub use virtual_grid::{
    AprCalculator, GridFill, Rating, RatingGrade, VirtualGrid, VirtualGridConfig,
};
pub use volume_maker::{
    VolumeMakerMode, VolumeMakerPlanConfig, VolumeMakerState, VolumeMakerStrategy,
};

/// Common seam for strategies driven by one market snapshot.
pub trait StrategyMachine {
    type State;

    /// Produces executable order intents for the supplied state and snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when the snapshot does not match the strategy
    /// or required market data is missing.
    fn evaluate(
        &self,
        state: &Self::State,
        snapshot: &MarketSnapshot,
    ) -> Result<Vec<OrderIntent>, StrategyError>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StrategyError {
    #[error("invalid strategy configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("snapshot does not match strategy: {0}")]
    SnapshotMismatch(String),
    #[error("market snapshot is missing {0}")]
    MissingMarketData(&'static str),
    #[error("financial value is outside the domain: {0}")]
    InvalidFinancialValue(&'static str),
}
