//! Unified command-line surface for the Rust runtime.

pub mod alert;
pub mod cli;
pub mod command;
pub mod continuous_monitor;
pub mod monitor;
pub mod paper_arbitrage_saga;
pub mod paper_grid_task;
pub mod paper_single_leg_saga;
pub mod scanner;
pub mod task_host;

pub use cli::{Cli, Command, ExchangeChoice, LogLevel};
pub use command::run;
pub use paper_arbitrage_saga::{
    DurablePaperArbitrageSaga, PaperArbitragePreservedOutcome, PaperArbitrageRecoveryStage,
    PaperArbitrageRequest, PaperArbitrageRun, PaperArbitrageSagaError,
};
pub use paper_grid_task::{
    GRID_PAPER_TASK_STATUS_SCHEMA_VERSION, GridPaperExecutionFuture, GridPaperExecutor,
    GridPaperTask, GridPaperTaskConfig, GridPaperTaskError, GridPaperTaskExit,
    GridPaperTaskFailure, GridPaperTaskPhase, GridPaperTaskStatus,
};
pub use paper_single_leg_saga::{
    DurablePaperSingleLegSaga, PaperSingleLegRequest, PaperSingleLegRun, PaperSingleLegSagaError,
};
