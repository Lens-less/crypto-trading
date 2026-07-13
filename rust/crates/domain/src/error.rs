use rust_decimal::Decimal;
use thiserror::Error;

/// Validation failures at the domain boundary.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("symbol must not be empty")]
    EmptySymbol,
    #[error("exchange must not be empty")]
    EmptyExchange,
    #[error("price must not be negative: {0}")]
    NegativePrice(Decimal),
    #[error("quantity must not be negative: {0}")]
    NegativeQuantity(Decimal),
    #[error("invalid decimal value: {0}")]
    InvalidDecimal(String),
    #[error("ask price {ask} is below bid price {bid}")]
    CrossedMarket { bid: Decimal, ask: Decimal },
}
