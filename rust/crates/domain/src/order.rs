use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{MarketType, Money, Price, Quantity, Symbol};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    #[serde(alias = "long")]
    Buy,
    #[serde(alias = "short")]
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    #[default]
    Gtc,
    Ioc,
    Fok,
    PostOnly,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    #[default]
    Pending,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub client_order_id: Uuid,
    pub exchange: String,
    pub symbol: Symbol,
    #[serde(default)]
    pub market_type: MarketType,
    pub side: Side,
    pub order_type: OrderType,
    pub quantity: Quantity,
    #[serde(default)]
    pub price: Option<Price>,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub time_in_force: TimeInForce,
}

impl OrderIntent {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub intent: OrderIntent,
    #[serde(default)]
    pub filled_quantity: Quantity,
    #[serde(default)]
    pub average_fill_price: Option<Price>,
    #[serde(default)]
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
    #[default]
    Flat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub exchange: String,
    pub symbol: Symbol,
    #[serde(default)]
    pub market_type: MarketType,
    #[serde(default)]
    pub side: PositionSide,
    #[serde(default)]
    pub quantity: Quantity,
    #[serde(default)]
    pub entry_price: Option<Price>,
    #[serde(default)]
    pub mark_price: Option<Price>,
    #[serde(default)]
    pub unrealized_pnl: Money,
    pub updated_at: DateTime<Utc>,
}
