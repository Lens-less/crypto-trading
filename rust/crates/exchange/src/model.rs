use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Order, OrderIntent, Position, Symbol};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::{ExchangeError, ExchangeOperation};

/// A command with explicit order and cancellation semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TradingCommand {
    Submit(OrderIntent),
    Cancel {
        order_id: String,
    },
    CancelAll {
        symbol: Option<Symbol>,
        market_type: Option<MarketType>,
    },
}

impl TradingCommand {
    pub(crate) const fn operation(&self) -> ExchangeOperation {
        match self {
            Self::Submit(_) => ExchangeOperation::SubmitOrder,
            Self::Cancel { .. } => ExchangeOperation::CancelOrder,
            Self::CancelAll { .. } => ExchangeOperation::CancelAll,
        }
    }

    pub(crate) fn client_order_id(&self) -> Option<Uuid> {
        match self {
            Self::Submit(intent) => Some(intent.client_order_id),
            Self::Cancel { .. } | Self::CancelAll { .. } => None,
        }
    }
}

/// Certain outcome of a paper order submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionDisposition {
    Open,
    Filled,
    Cancelled,
    AlreadyProcessed,
}

/// Certain outcome of a cancellation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationDisposition {
    Cancelled,
    AlreadyCancelled,
    NoMatchingOrders,
}

/// Typed receipt returned only when the command outcome is known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TradingReceipt {
    Submitted {
        order: Order,
        disposition: SubmissionDisposition,
    },
    Cancelled {
        orders: Vec<Order>,
        disposition: CancellationDisposition,
    },
}

impl TradingReceipt {
    pub const fn submission_disposition(&self) -> Option<SubmissionDisposition> {
        match self {
            Self::Submitted { disposition, .. } => Some(*disposition),
            Self::Cancelled { .. } => None,
        }
    }
}

/// Scope of authoritative state requested from an adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReconcileScope {
    All,
    Orders { symbol: Option<Symbol> },
    Positions { symbol: Option<Symbol> },
}

/// Authoritative state observed at one logical adapter time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileReceipt {
    pub scope: ReconcileScope,
    pub orders: Vec<Order>,
    pub positions: Vec<Position>,
    pub observed_at: DateTime<Utc>,
}

/// Filter for a stream of top-of-book snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketSubscription {
    AllSnapshots {
        market_type: Option<MarketType>,
    },
    Snapshots {
        symbols: Vec<Symbol>,
        market_type: Option<MarketType>,
    },
}

impl MarketSubscription {
    /// Builds a deduplicated selected-symbol subscription.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when `symbols` is empty.
    pub fn snapshots(
        mut symbols: Vec<Symbol>,
        market_type: Option<MarketType>,
    ) -> Result<Self, ExchangeError> {
        if symbols.is_empty() {
            return Err(ExchangeError::invalid(
                "a selected snapshot subscription needs at least one symbol",
            ));
        }
        symbols.sort();
        symbols.dedup();
        Ok(Self::Snapshots {
            symbols,
            market_type,
        })
    }

    pub const fn all_snapshots(market_type: Option<MarketType>) -> Self {
        Self::AllSnapshots { market_type }
    }

    pub(crate) fn matches(&self, snapshot: &MarketSnapshot) -> bool {
        match self {
            Self::AllSnapshots { market_type } => {
                market_type.is_none_or(|kind| kind == snapshot.market_type)
            }
            Self::Snapshots {
                symbols,
                market_type,
            } => {
                symbols.binary_search(&snapshot.symbol).is_ok()
                    && market_type.is_none_or(|kind| kind == snapshot.market_type)
            }
        }
    }
}

/// Bounded market-data stream. Lag is surfaced rather than silently dropping data.
#[derive(Debug)]
pub struct SubscriptionReceipt {
    id: String,
    subscription: MarketSubscription,
    receiver: broadcast::Receiver<MarketSnapshot>,
}

impl SubscriptionReceipt {
    pub(crate) const fn new(
        id: String,
        subscription: MarketSubscription,
        receiver: broadcast::Receiver<MarketSnapshot>,
    ) -> Self {
        Self {
            id,
            subscription,
            receiver,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub const fn subscription(&self) -> &MarketSubscription {
        &self.subscription
    }

    /// Waits for the next matching snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::SubscriptionLagged`] if the bounded stream
    /// overwrote unread snapshots, or [`ExchangeError::Unavailable`] if closed.
    pub async fn recv(&mut self) -> Result<MarketSnapshot, ExchangeError> {
        loop {
            match self.receiver.recv().await {
                Ok(snapshot) if self.subscription.matches(&snapshot) => return Ok(snapshot),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(ExchangeError::SubscriptionLagged { skipped });
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(ExchangeError::unavailable(
                        "market-data subscription closed",
                    ));
                }
            }
        }
    }
}

/// Safety mode advertised by an adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeMode {
    Paper,
    ReadOnly,
    Live,
}

/// Operational availability independent of trading mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeAvailability {
    Ready,
    Unavailable,
}

/// Small readiness surface for orchestration and health reporting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeStatus {
    pub exchange: String,
    pub mode: ExchangeMode,
    pub availability: ExchangeAvailability,
    pub latest_market_timestamp: Option<DateTime<Utc>>,
    pub open_orders: usize,
}
