//! Unified command-line surface for the Rust runtime.

pub mod cli;
pub mod command;
pub mod continuous_monitor;
pub mod continuous_testnet;
pub mod monitor;
mod paper_admission;
pub mod paper_arbitrage_saga;
pub mod paper_arbitrage_task;
pub mod paper_bar_task;
pub mod paper_grid_task;
pub mod paper_profile;
pub mod paper_single_leg_saga;
pub mod shutdown;
pub mod task_host;
pub mod testnet_lifecycle;
pub mod testnet_reconciliation;
pub mod testnet_soak;

pub use cli::{Cli, Command};
pub use command::run;
pub use paper_arbitrage_saga::{
    DurablePaperArbitrageSaga, PaperArbitragePreservedOutcome, PaperArbitrageRecoveryStage,
    PaperArbitrageRequest, PaperArbitrageRun, PaperArbitrageSagaError,
};
pub use paper_arbitrage_task::{
    ARBITRAGE_PAPER_TASK_STATUS_SCHEMA_VERSION, ArbitragePaperExecutionFuture,
    ArbitragePaperExecutor, ArbitragePaperMarketEventFuture, ArbitragePaperTask,
    ArbitragePaperTaskConfig, ArbitragePaperTaskError, ArbitragePaperTaskExit,
    ArbitragePaperTaskFailure, ArbitragePaperTaskPhase, ArbitragePaperTaskStatus,
};
pub use paper_bar_task::{
    PaperBarAction, PaperBarDecision, PaperBarTask, PaperBarTaskError, PaperBarTaskState,
};
pub use paper_grid_task::{
    GRID_PAPER_TASK_STATUS_SCHEMA_VERSION, GridPaperExecutionFuture, GridPaperExecutor,
    GridPaperObservationFuture, GridPaperTask, GridPaperTaskConfig, GridPaperTaskError,
    GridPaperTaskExit, GridPaperTaskFailure, GridPaperTaskPhase, GridPaperTaskStatus,
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
    TestnetLifecycleError, TestnetLifecycleObservation, TestnetLifecycleRecoveryState,
    TestnetLifecycleReport, TestnetLifecycleVenue, TestnetLifecycleVenueFuture,
    run_testnet_lifecycle, testnet_lifecycle_recovery_state, testnet_lifecycle_requires_submission,
    testnet_lifecycle_wire_symbol,
};
pub use testnet_reconciliation::{
    TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT, TESTNET_RECONCILIATION_SCHEMA_VERSION,
    TestnetReconciliationConfig, TestnetReconciliationMismatch, TestnetReconciliationPlan,
    TestnetReconciliationReport,
};
