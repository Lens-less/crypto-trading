//! Supervised runtime and process-local synchronized decision history.

mod capability;
mod execution;
mod history;
mod journal;
mod journal_reader;
mod market_data;
mod market_polling;
mod market_supervisor;
mod mode;
mod monitor_read_model;
mod read_model;

pub use capability::{
    AdapterFacet, AdapterFacetSupport, AdapterSupport, AdapterSupportLevel,
    CAPABILITY_SCHEMA_VERSION, Capability, CapabilityAccess, CapabilityArea, CapabilityEnvironment,
    CapabilityError, CapabilityLevel, CapabilityManifest, CapabilityScope, ReleaseStage,
    current_capability_manifest,
};
pub use execution::{
    ExchangeRouter, ExecutionBatch, ExecutionClock, ExecutionPolicy, IntentExecutor,
    MAX_EXECUTION_BATCH_ORDERS, MAX_EXECUTION_POLICY_SNAPSHOTS, ReconciliationObservation,
    RuntimeError,
};
pub use history::{
    DecisionRecord, HistoryError, JsonlHistory, MAX_HISTORY_BATCH_BYTES, MAX_HISTORY_RECORD_BYTES,
};
pub use journal::{
    AggregateRef, CursorError, EventContractError, JOURNAL_CURSOR_SCHEMA_VERSION, JournalCursor,
    MAX_JOURNAL_CURSOR_BYTES, MAX_OPERATION_EVENT_BYTES, MAX_OPERATION_EVENT_PAYLOAD_BYTES,
    OPERATION_EVENT_SCHEMA_VERSION, OperationEventEnvelope,
};
pub use journal_reader::{
    FileJournalSnapshotSource, JournalPage, JournalPageBoundary, JournalReadError, JournalSnapshot,
    JournalSnapshotSource, LegacyJsonlJournalReader, MAX_CURSOR_ANCHOR_SCAN_BYTES,
    MAX_JOURNAL_PAGE_BYTES, MAX_JOURNAL_PAGE_EVENTS, MAX_JOURNAL_SOURCE_BYTES,
    MemoryJournalSnapshotSource,
};
pub use market_data::{
    DeterministicMarketDataAdapter, MARKET_DATA_VIEW_SCHEMA_VERSION, MAX_MARKET_DATA_EVENTS,
    MAX_MARKET_DATA_TARGETS, MarketContinuity, MarketDataBook, MarketDataClock, MarketDataError,
    MarketDataEvent, MarketDataFreshness, MarketDataObservation, MarketDataSourceFailure,
    MarketDataUpdate, MarketDataView, MarketFreshnessPolicy, MarketInstrument,
    MarketInstrumentView, MarketUniverse, ObservedMarketPair, SubscriptionMarketDataAdapter,
    SystemMarketDataClock,
};
pub use market_polling::{BinancePollingRoute, BinancePublicPollingSource, MarketPollingPolicy};
pub use market_supervisor::{
    MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION, MarketDataEventFuture, MarketDataEventSource,
    MarketSupervisor, MarketSupervisorConfig, MarketSupervisorError, MarketSupervisorExit,
    MarketSupervisorHealth, MarketSupervisorPhase, MarketSupervisorStatus,
};
pub use mode::{ExecutionMode, LIVE_ACKNOWLEDGEMENT, ModeError};
pub use monitor_read_model::{
    ARBITRAGE_MONITOR_READ_MODEL_SCHEMA_VERSION, ArbitrageMonitorProjection,
    ArbitrageMonitorReadModel, ArbitrageMonitorView, MonitorContinuityState, MonitorFreshnessState,
    MonitorLegView, MonitorProjectionState,
};
pub use read_model::{
    ExecutionBatchState, ExecutionBatchView, ExecutionPhase, MAX_OPERATOR_READ_MODEL_BATCHES,
    MAX_OPERATOR_READ_MODEL_WARNINGS, OPERATOR_READ_MODEL_SCHEMA_VERSION, OperatorReadModel,
    ProjectionStatus, ReadModelError, ReadModelWarning, ReadModelWarningCode, RecoveryDirective,
};
