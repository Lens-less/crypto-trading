//! Core trading types with decimal-safe financial values.

mod error;
mod hash;
mod market;
mod operational_metrics;
mod order;
mod value;

pub use error::DomainError;
pub use hash::sha256_digest;
pub use market::{MarketSnapshot, MarketType, Symbol};
pub use operational_metrics::{
    OperationalJournalMetricsSnapshot, OperationalMetricsSnapshot, OperationalOwnerPhase,
    OperationalRestMetricsSnapshot, OperationalRestObservation, OperationalStreamKind,
    OperationalStreamMetricsSnapshot, operational_metrics_snapshot, record_journal_append,
    record_operational_clock_skew_milliseconds, record_operational_rest_response,
    record_operational_rest_transport_error, record_stream_frame, record_stream_gap,
    record_stream_queue_drop, record_stream_reconnect, render_prometheus_metrics,
    set_operational_owner_phase,
};
pub use order::{
    Order, OrderIntent, OrderStatus, OrderType, Position, PositionSide, Side, TimeInForce,
};
pub use value::{Money, Price, Quantity};
