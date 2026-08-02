//! Deterministic event-tape backtesting primitives.
//!
//! The crate models a minimal single-instrument simulation loop with explicit
//! fill assumptions, a mark-to-market ledger, and a walk-forward splitter that
//! exposes only out-of-sample windows.

mod engine;
mod ledger;
mod walk_forward;

pub use engine::{
    BacktestEngine, BacktestMetrics, BacktestResult, EquityPoint, EventTape, FillModel, Liquidity,
    MarketEvent, OrderRequest, Side, SimClock, Strategy, StrategyContext, Trade, TradeFill,
};
pub use ledger::LedgerSnapshot;
pub use walk_forward::{OutOfSampleWindow, WalkForwardConfig, WalkForwardSplitter};

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BacktestError {
    #[error("initial cash must be non-negative")]
    InvalidInitialCash,
    #[error("event price must be strictly positive")]
    NonPositivePrice,
    #[error("order quantity must be strictly positive")]
    InvalidQuantity,
    #[error("basis points inputs must be non-negative")]
    NegativeBasisPoints,
    #[error("slippage basis points must be less than 10,000")]
    InvalidSlippageBasisPoints,
    #[error("event tape timestamps must be monotonic")]
    NonMonotonicTape,
    #[error("walk-forward train, test, and step sizes must be positive")]
    InvalidWalkForwardConfig,
    #[error("decimal arithmetic overflow")]
    ArithmeticOverflow,
}
