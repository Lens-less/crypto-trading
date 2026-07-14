//! Supervised runtime and process-local synchronized decision history.

mod execution;
mod history;
mod mode;

pub use execution::{
    ExchangeRouter, ExecutionBatch, ExecutionClock, ExecutionPolicy, IntentExecutor,
    MAX_EXECUTION_BATCH_ORDERS, MAX_EXECUTION_POLICY_SNAPSHOTS, ReconciliationObservation,
    RuntimeError,
};
pub use history::{
    DecisionRecord, HistoryError, JsonlHistory, MAX_HISTORY_BATCH_BYTES, MAX_HISTORY_RECORD_BYTES,
};
pub use mode::{ExecutionMode, LIVE_ACKNOWLEDGEMENT, ModeError};
