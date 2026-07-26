//! Unified command-line surface for the Rust runtime.

pub mod alert;
pub mod cli;
pub mod command;
pub mod continuous_alert;
pub mod continuous_monitor;
pub mod monitor;
pub mod paper_arbitrage_saga;
pub mod paper_arbitrage_task;
pub mod paper_grid_task;
pub mod paper_profile;
pub mod paper_single_leg_saga;
pub mod scanner;
pub mod task_host;
pub mod testnet_lifecycle;
pub mod testnet_reconciliation;
pub mod testnet_soak;

pub use cli::{Cli, Command, ExchangeChoice, LogLevel};
pub use command::run;
pub use paper_arbitrage_saga::{
    DurablePaperArbitrageSaga, PaperArbitragePreservedOutcome, PaperArbitrageRecoveryStage,
    PaperArbitrageRequest, PaperArbitrageRun, PaperArbitrageSagaError,
};
pub use paper_arbitrage_task::{
    ARBITRAGE_PAPER_TASK_STATUS_SCHEMA_VERSION, ArbitragePaperExecutionFuture,
    ArbitragePaperExecutor, ArbitragePaperTask, ArbitragePaperTaskConfig, ArbitragePaperTaskError,
    ArbitragePaperTaskExit, ArbitragePaperTaskFailure, ArbitragePaperTaskPhase,
    ArbitragePaperTaskStatus,
};
pub use paper_grid_task::{
    GRID_PAPER_TASK_STATUS_SCHEMA_VERSION, GridPaperExecutionFuture, GridPaperExecutor,
    GridPaperTask, GridPaperTaskConfig, GridPaperTaskError, GridPaperTaskExit,
    GridPaperTaskFailure, GridPaperTaskPhase, GridPaperTaskStatus,
};
pub use paper_profile::{
    ArbitragePaperProfileInput, GridPaperProfileInput, PaperProfileCatalog,
    PaperProfileCatalogInput, PaperProfileError, StartedPaperTask,
};
pub use paper_single_leg_saga::{
    DurablePaperSingleLegSaga, PaperSingleLegRequest, PaperSingleLegRun, PaperSingleLegSagaError,
};
pub use testnet_lifecycle::{
    TESTNET_LIFECYCLE_ACKNOWLEDGEMENT, TESTNET_LIFECYCLE_SCHEMA_VERSION, TestnetLifecycleConfig,
    TestnetLifecycleError, TestnetLifecycleObservation, TestnetLifecycleReport,
    TestnetLifecycleVenue, TestnetLifecycleVenueFuture, run_testnet_lifecycle,
};
pub use testnet_reconciliation::{
    TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT, TESTNET_RECONCILIATION_SCHEMA_VERSION,
    TestnetReconciliationConfig, TestnetReconciliationMismatch, TestnetReconciliationPlan,
    TestnetReconciliationReport,
};
