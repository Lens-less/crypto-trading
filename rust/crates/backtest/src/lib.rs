//! Deterministic event-tape backtesting primitives.
//!
//! The crate models a minimal single-instrument simulation loop with explicit
//! fill assumptions, a mark-to-market ledger, and a walk-forward splitter that
//! exposes only out-of-sample windows.

mod engine;
mod ledger;
mod walk_forward;

pub use crypto_trading_domain::Side;
pub use engine::{
    BacktestEngine, BacktestMetrics, BacktestResult, EquityPoint, EventTape, FillModel, Liquidity,
    MarketEvent, MarketEventPrice, OrderRequest, SimClock, Strategy, StrategyContext, Trade,
    TradeFill, adapt_order_intents,
};
pub use ledger::LedgerSnapshot;
pub use walk_forward::{OutOfSampleWindow, WalkForwardConfig, WalkForwardSplitter};

use crypto_trading_domain::DomainError;
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
    #[error("event tape timestamps must be monotonic")]
    NonMonotonicTape,
    #[error("the current backtest fill model supports market intents only")]
    UnsupportedOrderIntent,
    #[error("walk-forward train, test, and step sizes must be positive")]
    InvalidWalkForwardConfig,
    #[error("decimal arithmetic overflow")]
    ArithmeticOverflow,
    #[error(transparent)]
    Domain(#[from] DomainError),
}
