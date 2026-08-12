//! Core trading types with decimal-safe financial values.

mod error;
mod hash;
mod market;
mod order;
mod value;

pub use error::DomainError;
pub use hash::sha256_digest;
pub use market::{MarketSnapshot, MarketType, Symbol};
pub use order::{
    Order, OrderIntent, OrderStatus, OrderType, Position, PositionSide, Side, TimeInForce,
};
pub use value::{Money, Price, Quantity};
