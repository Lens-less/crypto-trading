//! Deterministic event-tape backtesting primitives.
//!
//! The crate models a minimal single-instrument simulation loop with explicit
//! fill assumptions, a mark-to-market ledger, and a walk-forward splitter that
//! exposes only out-of-sample windows.
//!
//! The bounded kernel supports immediate taker market requests only. Limit
//! orders, resting-maker execution, and production `StrategyMachine` adapters
//! remain explicit unsupported boundaries rather than optimistic fills.

mod engine;
mod ledger;
mod walk_forward;

pub use crypto_trading_domain::Side;
pub use engine::{
    BacktestEngine, BacktestMetrics, BacktestResult, EquityPoint, EventTape, FillModel, Liquidity,
    MarketEvent, MarketEventPrice, OrderRequest, SimClock, Strategy, StrategyContext,
    TapeInstrument, Trade, TradeFill, adapt_order_intents,
};
pub use ledger::LedgerSnapshot;
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
    #[error("decimal arithmetic overflow")]
    ArithmeticOverflow,
    #[error(transparent)]
    Indicator(#[from] IndicatorError),
    #[error(transparent)]
    Domain(#[from] DomainError),
}
