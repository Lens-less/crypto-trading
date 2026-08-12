//! Deterministic event-tape backtesting primitives.
//!
//! The crate models a minimal single-instrument simulation loop with explicit
//! fill assumptions, a mark-to-market ledger, and a walk-forward splitter that
//! exposes only out-of-sample windows.
//!
//! The bounded kernel supports immediate taker market requests only. Limit
//! orders, resting-maker execution, and production `StrategyMachine` adapters
//! remain explicit unsupported boundaries rather than optimistic fills.
//! Identified perpetual production-snapshot tapes also fail closed until the
//! crate models margin, liquidation, and funding semantics explicitly.

mod candidates;
mod engine;
mod evaluation;
mod experiment;
mod ledger;
mod sha256;
mod spot_data;
mod walk_forward;

pub use candidates::{
    BoundedSpotStrategy, BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy,
    LongOnlyDonchian, SlowTimeSeriesMomentum, SpotStrategyConfig,
};
pub use crypto_trading_domain::Side;
pub use engine::{
    BacktestEngine, BacktestMetrics, BacktestResult, EquityPoint, EventTape, FillModel, Liquidity,
    MarketEvent, MarketEventPrice, OrderRequest, SimClock, Strategy, StrategyContext,
    TapeInstrument, Trade, TradeFill, adapt_order_intents,
};
pub use evaluation::{
    CausalSpotEvaluation, CausalSpotEvaluator, CausalSpotMetrics, CausalTradeRecord, CostBreakdown,
    CostSchedule, CostSensitivityEvaluation, EvaluationPlan, EvaluationProtocol,
    EvaluationSplitConfig, EvaluationWindow, FinalHoldoutPhase, FrozenSelection,
    RegisteredConfiguration, SelectionPhase, SpotDecisionContext, TargetExposureStrategy,
    VerifiedEvaluationSample,
};
pub use experiment::{
    AggregateSelectionMetrics, BootstrapConfig, BootstrapInterval, CompletedExperiment,
    ConfigurationSelectionSummary, CostScheduleSpec, EvaluationProtocolSpec, ExperimentError,
    ExperimentPlan, ExperimentSplitSpec, FamilySelection, FinalHoldoutOutcome, FinalHoldoutRunner,
    PersistedSelection, PromisingCondition, PromisingDecision, PromotionThresholds,
    SelectedExperiment, SelectionSummary, SelectionWindowResult,
};
pub use ledger::LedgerSnapshot;
pub use spot_data::{DatasetManifest, Sha256Digest, SpotBar, SpotKlineDataset, TimestampUnit};
pub use walk_forward::{
    OutOfSampleWindow, WalkForwardConfig, WalkForwardResult, WalkForwardRunner,
    WalkForwardSplitter, WalkForwardWindowResult,
};

use crypto_trading_domain::{DomainError, Money, Quantity};
use crypto_trading_indicators::IndicatorError;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BacktestError {
    #[error("initial cash must be non-negative")]
    InvalidInitialCash,
    #[error("order quantity must be strictly positive")]
    InvalidQuantity,
    #[error("basis points inputs must be non-negative")]
    NegativeBasisPoints,
    #[error("slippage basis points must be less than 10,000")]
    InvalidSlippageBasisPoints,
    #[error("event tape timestamps must not move backwards")]
    NonMonotonicTape,
    #[error("event tape must contain exactly one exchange, symbol, and market type")]
    MixedInstrumentTape,
    #[error("the current backtest fill model supports market intents only")]
    UnsupportedOrderIntent,
    #[error("maker liquidity requires a resting-order model and is not supported")]
    UnsupportedMakerLiquidity,
    #[error("order instrument does not match the event tape instrument")]
    OrderInstrumentMismatch,
    #[error(
        "identified perpetual tapes require an explicit derivatives margin/liquidation/funding model"
    )]
    UnsupportedDerivativesMarginModel,
    #[error("spot order requires {required} cash but only {available} is available")]
    InsufficientBuyingPower { required: Money, available: Money },
    #[error("spot sell requires {required} inventory but only {available} is available")]
    InsufficientSpotInventory {
        required: Quantity,
        available: Quantity,
    },
    #[error("walk-forward train, test, and step sizes must be positive")]
    InvalidWalkForwardConfig,
    #[error("walk-forward index arithmetic overflowed")]
    WalkForwardIndexOverflow,
    #[error("SHA-256 checksums must contain exactly 64 hexadecimal characters")]
    InvalidChecksumFormat,
    #[error("observed archive or content checksum does not match the frozen manifest")]
    ChecksumMismatch,
    #[error("historical bars or their provenance are malformed, ambiguous, or non-contiguous")]
    InvalidBarSequence,
    #[error("historical dataset expected {expected} bars but parsed {actual}")]
    IncompleteDataset { expected: usize, actual: usize },
    #[error("historical dataset contains a bar that was not closed at the seal time")]
    StillOpenBar,
    #[error("evaluation target exposure must be between zero and one inclusive")]
    InvalidTargetExposure,
    #[error("evaluation search exceeds five families or twenty configurations per family")]
    SearchBudgetExceeded,
    #[error("evaluation range must be non-empty and contained in the supplied bar slice")]
    InvalidEvaluationRange,
    #[error("embargo, test, and final holdout sizes leave no complete out-of-sample window")]
    InsufficientEvaluationData,
    #[error("strategy parameters are outside the pre-registered bounded domain")]
    InvalidStrategyConfiguration,
    #[error("final holdout evaluation requested a configuration that was not frozen")]
    UnregisteredHoldoutConfiguration,
    #[error("decimal arithmetic overflow")]
    ArithmeticOverflow,
    #[error(transparent)]
    Indicator(#[from] IndicatorError),
    #[error(transparent)]
    Domain(#[from] DomainError),
}
