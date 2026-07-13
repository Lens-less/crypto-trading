use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{DomainError, Price, Quantity};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Symbol(String);

impl Symbol {
    /// Creates a non-empty symbol, trimming surrounding whitespace.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::EmptySymbol`] if the value is empty after trimming.
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(DomainError::EmptySymbol);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for Symbol {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    Spot,
    #[default]
    #[serde(alias = "perp", alias = "future", alias = "futures")]
    Perpetual,
}

/// A coherent top-of-book snapshot used by pure strategy modules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketSnapshot {
    exchange: String,
    pub symbol: Symbol,
    #[serde(default)]
    pub market_type: MarketType,
    bid: Price,
    ask: Price,
    #[serde(default)]
    pub last: Option<Price>,
    #[serde(default)]
    pub bid_quantity: Option<Quantity>,
    #[serde(default)]
    pub ask_quantity: Option<Quantity>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
struct UncheckedMarketSnapshot {
    exchange: String,
    symbol: Symbol,
    #[serde(default)]
    market_type: MarketType,
    bid: Price,
    ask: Price,
    #[serde(default)]
    last: Option<Price>,
    #[serde(default)]
    bid_quantity: Option<Quantity>,
    #[serde(default)]
    ask_quantity: Option<Quantity>,
    timestamp: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for MarketSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = UncheckedMarketSnapshot::deserialize(deserializer)?;
        let mut snapshot = Self::new(
            raw.exchange,
            raw.symbol,
            raw.market_type,
            raw.bid,
            raw.ask,
            raw.timestamp,
        )
        .map_err(serde::de::Error::custom)?;
        snapshot.last = raw.last;
        snapshot.bid_quantity = raw.bid_quantity;
        snapshot.ask_quantity = raw.ask_quantity;
        Ok(snapshot)
    }
}

impl MarketSnapshot {
    /// Creates a validated top-of-book snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty exchange or when the ask is below the bid.
    pub fn new(
        exchange: impl Into<String>,
        symbol: Symbol,
        market_type: MarketType,
        bid: Price,
        ask: Price,
        timestamp: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        let exchange = exchange.into();
        if exchange.trim().is_empty() {
            return Err(DomainError::EmptyExchange);
        }
        if ask < bid {
            return Err(DomainError::CrossedMarket {
                bid: bid.as_decimal(),
                ask: ask.as_decimal(),
            });
        }

        Ok(Self {
            exchange,
            symbol,
            market_type,
            bid,
            ask,
            last: None,
            bid_quantity: None,
            ask_quantity: None,
            timestamp,
        })
    }

    pub fn mid_price(&self) -> Price {
        let bid = self.bid.as_decimal();
        let midpoint = bid + (self.ask.as_decimal() - bid) / Decimal::TWO;
        Price(midpoint)
    }

    /// Returns the exchange identifier associated with this quote.
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Returns the validated best bid.
    pub const fn bid(&self) -> Price {
        self.bid
    }

    /// Returns the validated best ask.
    pub const fn ask(&self) -> Price {
        self.ask
    }

    pub fn spread(&self) -> Decimal {
        self.ask.as_decimal() - self.bid.as_decimal()
    }

    pub fn spread_bps(&self) -> Decimal {
        let midpoint = self.mid_price().as_decimal();
        if midpoint.is_zero() {
            Decimal::ZERO
        } else {
            self.spread() / midpoint * Decimal::from(10_000)
        }
    }
}
