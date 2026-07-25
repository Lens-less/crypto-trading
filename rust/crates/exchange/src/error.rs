use chrono::{DateTime, Utc};
use uuid::Uuid;

use thiserror::Error;

use crate::model::ExchangeOperationKey;

/// Operations whose failure semantics matter to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeOperation {
    Subscribe,
    SubmitOrder,
    CancelOrder,
    CancelAll,
    Reconcile,
    Status,
    PublishSnapshot,
}

impl std::fmt::Display for ExchangeOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Subscribe => "subscribe",
            Self::SubmitOrder => "submit_order",
            Self::CancelOrder => "cancel_order",
            Self::CancelAll => "cancel_all",
            Self::Reconcile => "reconcile",
            Self::Status => "status",
            Self::PublishSnapshot => "publish_snapshot",
        };
        formatter.write_str(value)
    }
}

/// Structured backoff data carried from a remote failure response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteRetryAfter {
    Seconds(u64),
    At(DateTime<Utc>),
}

/// Optional exchange-specific metadata attached to a remote failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteFailureMetadata {
    pub exchange_code: Option<String>,
    pub retry_after: Option<RemoteRetryAfter>,
    pub server_time: Option<DateTime<Utc>>,
}

impl RemoteFailureMetadata {
    fn merge_from(&mut self, other: Self) {
        if other.exchange_code.is_some() {
            self.exchange_code = other.exchange_code;
        }
        if other.retry_after.is_some() {
            self.retry_after = other.retry_after;
        }
        if other.server_time.is_some() {
            self.server_time = other.server_time;
        }
    }
}

/// Failures at the exchange boundary.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExchangeError {
    #[error("invalid exchange request: {reason}")]
    InvalidRequest { reason: String },

    #[error("exchange rejected the request: {reason}")]
    Rejected { reason: String },

    #[error("{operation} is unsupported by exchange {exchange}")]
    Unsupported {
        exchange: String,
        operation: ExchangeOperation,
    },

    #[error("exchange request queue is full (capacity {capacity})")]
    Backpressure { capacity: usize },

    #[error("{resource} exceeds hard limit {limit} (requested {requested})")]
    ResourceLimit {
        resource: &'static str,
        limit: usize,
        requested: usize,
    },

    #[error(
        "outcome of {operation} is ambiguous for operation key {operation_key:?} (client order {client_order_id:?}): {reason}"
    )]
    AmbiguousOutcome {
        operation: ExchangeOperation,
        client_order_id: Option<Uuid>,
        operation_key: Option<ExchangeOperationKey>,
        reason: String,
    },

    #[error("exchange is unavailable: {reason}")]
    Unavailable { reason: String },

    #[error("invalid response from exchange {exchange}: {reason}")]
    InvalidResponse { exchange: String, reason: String },

    #[error("remote exchange {exchange} failed with HTTP status {status:?}: {reason}")]
    RemoteFailure {
        exchange: String,
        status: Option<u16>,
        reason: String,
        metadata: RemoteFailureMetadata,
    },

    #[error("market-data subscriber lagged and skipped {skipped} snapshots")]
    SubscriptionLagged { skipped: u64 },

    #[error("paper exchange invariant failed: {reason}")]
    InvariantViolation { reason: String },
}

impl ExchangeError {
    pub fn invalid(reason: impl Into<String>) -> Self {
        Self::InvalidRequest {
            reason: reason.into(),
        }
    }

    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
        }
    }

    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }

    pub(crate) const fn resource_limit(
        resource: &'static str,
        limit: usize,
        requested: usize,
    ) -> Self {
        Self::ResourceLimit {
            resource,
            limit,
            requested,
        }
    }

    pub(crate) fn invariant(reason: impl Into<String>) -> Self {
        Self::InvariantViolation {
            reason: reason.into(),
        }
    }

    pub(crate) fn invalid_response(exchange: &str, reason: impl Into<String>) -> Self {
        Self::InvalidResponse {
            exchange: exchange.to_owned(),
            reason: reason.into(),
        }
    }

    pub(crate) fn remote_failure(
        exchange: &str,
        status: Option<u16>,
        reason: impl Into<String>,
    ) -> Self {
        Self::RemoteFailure {
            exchange: exchange.to_owned(),
            status,
            reason: reason.into(),
            metadata: RemoteFailureMetadata::default(),
        }
    }

    #[must_use]
    pub fn with_remote_metadata(mut self, metadata: RemoteFailureMetadata) -> Self {
        if let Self::RemoteFailure {
            metadata: current, ..
        } = &mut self
        {
            current.merge_from(metadata);
        }
        self
    }

    #[must_use]
    pub fn with_exchange_code(self, exchange_code: impl Into<String>) -> Self {
        self.with_remote_metadata(RemoteFailureMetadata {
            exchange_code: Some(exchange_code.into()),
            ..RemoteFailureMetadata::default()
        })
    }

    #[must_use]
    pub fn with_retry_after(self, retry_after: RemoteRetryAfter) -> Self {
        self.with_remote_metadata(RemoteFailureMetadata {
            retry_after: Some(retry_after),
            ..RemoteFailureMetadata::default()
        })
    }

    #[must_use]
    pub fn with_server_time(self, server_time: DateTime<Utc>) -> Self {
        self.with_remote_metadata(RemoteFailureMetadata {
            server_time: Some(server_time),
            ..RemoteFailureMetadata::default()
        })
    }

    #[must_use]
    pub fn remote_failure_metadata(&self) -> Option<&RemoteFailureMetadata> {
        match self {
            Self::RemoteFailure { metadata, .. } => Some(metadata),
            _ => None,
        }
    }
}
