use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{MarketType, Money, Price, Quantity, Symbol};

/// Direction of an order.
///
/// The `long` and `short` aliases are accepted when deserializing so
/// configuration written for the predecessor Python system still loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// Acquire the base asset, or increase a long position.
    #[serde(alias = "long")]
    Buy,
    /// Dispose of the base asset, or increase a short position.
    #[serde(alias = "short")]
    Sell,
}

/// How an order's execution price is determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Execute immediately against the book at whatever price is available.
    Market,
    /// Execute only at the stated price or better.
    Limit,
}

/// How long an order remains executable, and what happens to the remainder.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Good till cancelled: rest on the book until filled or cancelled.
    #[default]
    Gtc,
    /// Immediate or cancel: fill what is available now, cancel the rest.
    Ioc,
    /// Fill or kill: fill completely and immediately, or not at all.
    Fok,
    /// Rest on the book only if doing so does not take liquidity. An order
    /// that would cross the spread is rejected rather than executed.
    PostOnly,
}

/// Lifecycle state of a submitted order.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Constructed locally; not yet acknowledged by a venue.
    #[default]
    Pending,
    /// Acknowledged and resting on the book with nothing filled.
    Open,
    /// Some quantity has filled; a cancellable remainder is still open.
    PartiallyFilled,
    /// The full quantity has filled. Terminal.
    Filled,
    /// Cancelled before the full quantity filled. Terminal.
    Cancelled,
    /// Refused by the venue or by a local guard. Terminal.
    Rejected,
}

/// What the caller intends to submit, before any venue has seen it.
///
/// An intent carries a locally generated
/// [`client_order_id`](Self::client_order_id) that is written to the journal
/// *before* submission. That identity is what makes recovery possible: after
/// an ambiguous submit, the runtime queries the venue by this UUID rather than
/// guessing whether resubmitting is safe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    /// Caller-owned idempotency key, durable across restarts.
    pub client_order_id: Uuid,
    /// Venue this intent is addressed to.
    pub exchange: String,
    /// Instrument in this project's canonical `BASE-QUOTE-PRODUCT` form.
    pub symbol: Symbol,
    /// Product the instrument trades as.
    #[serde(default)]
    pub market_type: MarketType,
    /// Direction of the order.
    pub side: Side,
    /// Whether the price is taken from the book or stated explicitly.
    pub order_type: OrderType,
    /// Base-asset quantity to trade.
    pub quantity: Quantity,
    /// Limit price. Always `Some` for [`OrderType::Limit`].
    #[serde(default)]
    pub price: Option<Price>,
    /// Whether the order may only reduce an existing position, never open or
    /// flip one. Not meaningful on spot.
    #[serde(default)]
    pub reduce_only: bool,
    /// Resting and remainder policy.
    #[serde(default)]
    pub time_in_force: TimeInForce,
}

impl OrderIntent {
    /// Builds a market order with a fresh client order ID.
    ///
    /// The result is not risk-checked. Every submission path separately
    /// validates product identity, market-data freshness, book depth, and risk
    /// limits before an intent reaches a venue.
    #[must_use]
    pub fn market(
        exchange: impl Into<String>,
        symbol: Symbol,
        market_type: MarketType,
        side: Side,
        quantity: Quantity,
    ) -> Self {
        Self {
            client_order_id: Uuid::new_v4(),
            exchange: exchange.into(),
            symbol,
            market_type,
            side,
            order_type: OrderType::Market,
            quantity,
            price: None,
            reduce_only: false,
            time_in_force: TimeInForce::Gtc,
        }
    }

    /// Builds a limit order with a fresh client order ID.
    ///
    /// Carries the same caveat as [`Self::market`]: construction performs no
    /// risk check.
    #[must_use]
    pub fn limit(
        exchange: impl Into<String>,
        symbol: Symbol,
        market_type: MarketType,
        side: Side,
        quantity: Quantity,
        price: Price,
    ) -> Self {
        Self {
            price: Some(price),
            order_type: OrderType::Limit,
            ..Self::market(exchange, symbol, market_type, side, quantity)
        }
    }
}

/// An order as a venue currently reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    /// Venue-assigned identifier, in this project's stable
    /// `exchange:product:wire_symbol:id` form.
    pub id: String,
    /// The intent this order was created from, including the client order ID
    /// used to correlate it during recovery.
    pub intent: OrderIntent,
    /// Quantity filled so far. Never exceeds `intent.quantity`.
    #[serde(default)]
    pub filled_quantity: Quantity,
    /// Volume-weighted average price of the filled quantity. `None` while
    /// nothing has filled.
    #[serde(default)]
    pub average_fill_price: Option<Price>,
    /// Current lifecycle state.
    #[serde(default)]
    pub status: OrderStatus,
    /// When the venue accepted the order.
    pub created_at: DateTime<Utc>,
    /// When the venue last changed the order.
    pub updated_at: DateTime<Utc>,
}

/// Direction of an open position.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    /// Net long exposure.
    Long,
    /// Net short exposure.
    Short,
    /// No exposure. This is the default, so an absent side is never read as
    /// exposure that was not reported.
    #[default]
    Flat,
}

/// Exposure in one instrument on one venue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    /// Venue holding the position.
    pub exchange: String,
    /// Instrument in canonical form.
    pub symbol: Symbol,
    /// Product the instrument trades as.
    #[serde(default)]
    pub market_type: MarketType,
    /// Direction of the exposure. [`PositionSide::Flat`] when `quantity` is
    /// zero.
    #[serde(default)]
    pub side: PositionSide,
    /// Absolute size of the exposure. Direction is carried by `side`, so this
    /// is never negative.
    #[serde(default)]
    pub quantity: Quantity,
    /// Average price the position was opened at, when the venue reports one.
    #[serde(default)]
    pub entry_price: Option<Price>,
    /// Price the venue currently marks the position at.
    #[serde(default)]
    pub mark_price: Option<Price>,
    /// Unrealised profit and loss as the venue reports it. This is the
    /// venue's figure, not a locally recomputed one.
    #[serde(default)]
    pub unrealized_pnl: Money,
    /// When the venue last changed the position.
    pub updated_at: DateTime<Utc>,
}
