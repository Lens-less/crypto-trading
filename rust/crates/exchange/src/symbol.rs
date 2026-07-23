use std::collections::HashMap;

use crypto_trading_domain::{MarketType, Symbol};

use crate::ExchangeError;

const MAX_EXCHANGE_SYMBOLS: usize = 10_000;
const MAX_EXCHANGE_ID_BYTES: usize = 64;
const MAX_WIRE_SYMBOL_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StandardKey {
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WireKey {
    exchange: String,
    wire_symbol: String,
    market_type: MarketType,
}

/// One explicit, product-aware mapping between a domain and wire symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeSymbol {
    standard_key: StandardKey,
    wire_key: WireKey,
}

impl ExchangeSymbol {
    /// Builds one validated mapping.
    ///
    /// Exchange identifiers are normalized to lowercase. Wire symbols remain
    /// case-sensitive because they are remote protocol values.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for blank, oversized, or
    /// whitespace/control-containing identifiers.
    pub fn new(
        exchange: impl Into<String>,
        standard_symbol: Symbol,
        market_type: MarketType,
        wire_symbol: impl Into<String>,
    ) -> Result<Self, ExchangeError> {
        let exchange = exchange.into();
        let exchange = normalized_exchange(&exchange)?;
        let wire_symbol = wire_symbol.into();
        if wire_symbol.is_empty()
            || wire_symbol.len() > MAX_WIRE_SYMBOL_BYTES
            || wire_symbol.chars().any(char::is_whitespace)
            || wire_symbol.chars().any(char::is_control)
        {
            return Err(ExchangeError::invalid(format!(
                "wire symbol must contain 1..={MAX_WIRE_SYMBOL_BYTES} non-whitespace bytes"
            )));
        }

        Ok(Self {
            standard_key: StandardKey {
                exchange: exchange.clone(),
                symbol: standard_symbol,
                market_type,
            },
            wire_key: WireKey {
                exchange,
                wire_symbol,
                market_type,
            },
        })
    }

    pub fn exchange(&self) -> &str {
        &self.standard_key.exchange
    }

    pub const fn standard_symbol(&self) -> &Symbol {
        &self.standard_key.symbol
    }

    pub const fn market_type(&self) -> MarketType {
        self.standard_key.market_type
    }

    pub fn wire_symbol(&self) -> &str {
        &self.wire_key.wire_symbol
    }
}

/// Bounded, exact, bidirectional exchange-symbol catalog.
///
/// Market type is part of both keys so exchanges may legitimately reuse one
/// wire symbol for Spot and perpetual products without ambiguity.
#[derive(Debug, Clone, Default)]
pub struct ExchangeSymbolCatalog {
    standard_to_wire: HashMap<StandardKey, ExchangeSymbol>,
    wire_to_standard: HashMap<WireKey, StandardKey>,
}

impl ExchangeSymbolCatalog {
    /// Builds a bounded catalog and rejects either-direction ambiguity.
    ///
    /// # Errors
    ///
    /// Returns an error for oversized, duplicate, ambiguous, or unreservable
    /// input.
    pub fn new(symbols: Vec<ExchangeSymbol>) -> Result<Self, ExchangeError> {
        if symbols.len() > MAX_EXCHANGE_SYMBOLS {
            return Err(ExchangeError::resource_limit(
                "exchange symbol catalog",
                MAX_EXCHANGE_SYMBOLS,
                symbols.len(),
            ));
        }
        let mut standard_to_wire = HashMap::new();
        let mut wire_to_standard = HashMap::new();
        standard_to_wire.try_reserve(symbols.len()).map_err(|_| {
            ExchangeError::unavailable("unable to reserve bounded exchange symbol catalog")
        })?;
        wire_to_standard.try_reserve(symbols.len()).map_err(|_| {
            ExchangeError::unavailable("unable to reserve bounded reverse symbol catalog")
        })?;

        for mapping in symbols {
            let standard_key = mapping.standard_key.clone();
            let wire_key = mapping.wire_key.clone();
            if standard_to_wire
                .insert(standard_key.clone(), mapping)
                .is_some()
            {
                return Err(ExchangeError::invalid(
                    "exchange symbol catalog contains a duplicate standard key",
                ));
            }
            if wire_to_standard.insert(wire_key, standard_key).is_some() {
                return Err(ExchangeError::invalid(
                    "exchange symbol catalog contains an ambiguous wire key",
                ));
            }
        }

        Ok(Self {
            standard_to_wire,
            wire_to_standard,
        })
    }

    pub fn len(&self) -> usize {
        self.standard_to_wire.len()
    }

    pub fn is_empty(&self) -> bool {
        self.standard_to_wire.is_empty()
    }

    /// Resolves one exact domain instrument to its wire symbol.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when no exact mapping exists
    /// or the exchange identifier is malformed.
    pub fn to_wire(
        &self,
        exchange: &str,
        symbol: &Symbol,
        market_type: MarketType,
    ) -> Result<&str, ExchangeError> {
        let exchange = normalized_exchange(exchange)?;
        self.standard_to_wire
            .get(&StandardKey {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                market_type,
            })
            .map(ExchangeSymbol::wire_symbol)
            .ok_or_else(|| {
                ExchangeError::invalid(format!(
                    "missing exact wire-symbol mapping for {exchange}/{symbol}/{market_type:?}"
                ))
            })
    }

    /// Resolves one exact remote instrument to its domain symbol.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidResponse`] when the remote symbol is not
    /// in the explicit catalog, and [`ExchangeError::InvalidRequest`] for a
    /// malformed exchange identifier.
    pub fn to_standard(
        &self,
        exchange: &str,
        wire_symbol: &str,
        market_type: MarketType,
    ) -> Result<&Symbol, ExchangeError> {
        let exchange = normalized_exchange(exchange)?;
        self.wire_to_standard
            .get(&WireKey {
                exchange: exchange.clone(),
                wire_symbol: wire_symbol.to_owned(),
                market_type,
            })
            .map(|key| &key.symbol)
            .ok_or_else(|| {
                ExchangeError::invalid_response(
                    &exchange,
                    format!(
                        "wire symbol {wire_symbol:?} has no exact {market_type:?} catalog mapping"
                    ),
                )
            })
    }
}

fn normalized_exchange(exchange: &str) -> Result<String, ExchangeError> {
    let exchange = exchange.trim();
    if exchange.is_empty()
        || exchange.len() > MAX_EXCHANGE_ID_BYTES
        || exchange.chars().any(char::is_whitespace)
        || exchange.chars().any(char::is_control)
    {
        return Err(ExchangeError::invalid(format!(
            "exchange identifier must contain 1..={MAX_EXCHANGE_ID_BYTES} non-whitespace bytes"
        )));
    }
    Ok(exchange.to_ascii_lowercase())
}
