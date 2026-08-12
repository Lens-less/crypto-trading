//! Pure strategy machines for the Rust trading runtime.
//!
//! The modules in this crate do not perform I/O. They translate validated
//! configuration, explicit state, and market snapshots into deterministic
//! decisions that the runtime may execute or record.

mod account_risk;
mod alert;
mod arbitrage;
mod arbitrage_history;
mod bar;
mod bar_research;
mod grid;
mod grid_protection;
mod risk;
mod virtual_grid;
mod volume_maker;

use crypto_trading_domain::{MarketSnapshot, OrderIntent};
use thiserror::Error;

pub use account_risk::{
    AccountRiskDecision, AccountRiskInput, AccountRiskLimits, AccountRiskOpenPosition,
    AccountRiskPolicy, AccountRiskRejection, AccountRiskTimeout, AccountRiskWarning,
    MAX_ACCOUNT_RISK_OPEN_POSITIONS, MAX_ACCOUNT_RISK_POSITION_DURATION_SECONDS,
    MAX_ACCOUNT_RISK_SYMBOLS,
};
pub use alert::{
    AlertConfig, AlertKind, AlertState, AlertStrategy, PriceAlert, PricePoint,
    VolatilityAlertConfig,
};
pub use arbitrage::{
    ArbitrageDecision, ArbitrageDecisionKind, ArbitrageDirection, ArbitrageState,
    ArbitrageStrategy, PairStrategyMachine, SegmentedArbitrageConfig, SpreadCalculator,
    SpreadQuote,
};
pub use arbitrage_history::{
    FUNDING_INTERVALS_PER_YEAR, HistoryArbitrageConfig, HistoryDecision, HistoryDecisionKind,
    HistoryDecisionMachine, MAX_HISTORY_WINDOW_SECONDS, MAX_SPREAD_SAMPLES,
    NaturalSpreadCalculator, SpreadSample,
};
pub use bar::{Bar, BarStrategy, BarStrategyContext, TargetExposure};
pub use bar_research::{
    BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy, LongOnlyDonchian,
    SlowTimeSeriesMomentum,
};
pub use grid::{
    GridDirection, GridLevel, GridPlanConfig, GridPlanner, GridRange, GridState, GridStrategy,
};
pub use grid_protection::{
    CapitalProtectionPolicy, CapitalProtectionPolicyConfig, GridDirective, GridProtectionGeometry,
    GridProtectionMachine, GridProtectionObservation, GridProtectionPolicies, GridProtectionReason,
    PriceLockPolicy, PriceLockPolicyConfig, ScalpingPolicy, ScalpingPolicyConfig, StopLossPolicy,
    StopLossPolicyConfig, TakeProfitPolicy, TakeProfitPolicyConfig,
};
pub use risk::{AccountRiskSnapshot, RiskDecision, RiskEngine, RiskLimits, RiskRejection};
pub use virtual_grid::{
    AprCalculator, AprEstimateAssumptions, GridFill, Rating, RatingGrade, VirtualGrid,
    VirtualGridConfig, VirtualGridCross,
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
