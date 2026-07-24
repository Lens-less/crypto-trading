//! Supervised runtime and process-local synchronized decision history.

mod capability;
mod execution;
mod history;
mod journal;
mod mode;

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
pub use mode::{ExecutionMode, LIVE_ACKNOWLEDGEMENT, ModeError};
