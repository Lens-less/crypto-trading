use std::collections::HashMap;

use crypto_trading_domain::{MarketType, Money, OrderIntent, Price, Quantity, Symbol};
use rust_decimal::Decimal;

use crate::ExchangeError;

const MAX_INSTRUMENT_RULES: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InstrumentKey {
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
}

/// Exact adapter-owned trading constraints for one exchange instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentRules {
    key: InstrumentKey,
    price_tick: Price,
    quantity_step: Quantity,
    min_quantity: Quantity,
    min_notional: Money,
}

impl InstrumentRules {
    /// Builds validated rules for one exact exchange, symbol, and market type.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for empty or non-positive constraints.
    pub fn new(
        exchange: impl Into<String>,
        symbol: Symbol,
        market_type: MarketType,
        price_tick: Price,
        quantity_step: Quantity,
        min_quantity: Quantity,
        min_notional: Money,
    ) -> Result<Self, ExchangeError> {
        let exchange = exchange.into();
        let exchange = exchange.trim();
        if exchange.is_empty() {
            return Err(ExchangeError::invalid(
                "instrument rules exchange must not be empty",
            ));
        }
        if quantity_step.as_decimal() <= Decimal::ZERO {
            return Err(ExchangeError::invalid(
                "instrument quantity step must be greater than zero",
            ));
        }
        if min_quantity.as_decimal() <= Decimal::ZERO {
            return Err(ExchangeError::invalid(
                "instrument minimum quantity must be greater than zero",
            ));
        }
        if min_notional.as_decimal() <= Decimal::ZERO {
            return Err(ExchangeError::invalid(
                "instrument minimum notional must be greater than zero",
            ));
        }
        Ok(Self {
            key: InstrumentKey {
                exchange: exchange.to_owned(),
                symbol,
                market_type,
            },
            price_tick,
            quantity_step,
            min_quantity,
            min_notional,
        })
    }

    pub fn exchange(&self) -> &str {
        &self.key.exchange
    }

    pub const fn symbol(&self) -> &Symbol {
        &self.key.symbol
    }

    pub const fn market_type(&self) -> MarketType {
        self.key.market_type
    }

    pub const fn price_tick(&self) -> Price {
        self.price_tick
    }

    pub const fn quantity_step(&self) -> Quantity {
        self.quantity_step
    }

    pub const fn min_quantity(&self) -> Quantity {
        self.min_quantity
    }

    pub const fn min_notional(&self) -> Money {
        self.min_notional
    }

    pub(crate) fn validate(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Price>,
    ) -> Result<(), ExchangeError> {
        let quantity = intent.quantity.as_decimal();
        if quantity < self.min_quantity.as_decimal() {
            return Err(ExchangeError::rejected(format!(
                "order quantity {quantity} is below minimum {}",
                self.min_quantity
            )));
        }
        let quantity_remainder = quantity
            .checked_rem(self.quantity_step.as_decimal())
            .ok_or_else(|| ExchangeError::rejected("unable to validate order quantity step"))?;
        if !quantity_remainder.is_zero() {
            return Err(ExchangeError::rejected(format!(
                "order quantity {quantity} is not aligned to step {}",
                self.quantity_step
            )));
        }
        if let Some(limit) = intent.price {
            let price_remainder = limit
                .as_decimal()
                .checked_rem(self.price_tick.as_decimal())
                .ok_or_else(|| ExchangeError::rejected("unable to validate order price tick"))?;
            if !price_remainder.is_zero() {
                return Err(ExchangeError::rejected(format!(
                    "order price {limit} is not aligned to tick {}",
                    self.price_tick
                )));
            }
        }
        let price = reference_price.ok_or_else(|| {
            ExchangeError::rejected("instrument minimum notional needs a reference price")
        })?;
        let notional = price
            .as_decimal()
            .checked_mul(quantity)
            .ok_or_else(|| ExchangeError::rejected("order notional overflowed"))?;
        if notional < self.min_notional.as_decimal() {
            return Err(ExchangeError::rejected(format!(
                "order notional {notional} is below minimum {}",
                self.min_notional
            )));
        }
        Ok(())
    }
}

/// Missing-rule behavior for paper execution.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentRulesMode {
    /// Missing rules are accepted for compatibility; present rules are still enforced.
    #[default]
    Permissive,
    /// Every order must have an exact catalog match.
    Strict,
}

/// Read-only public view of the active paper rules policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentRulesStatus {
    pub mode: InstrumentRulesMode,
    pub rule_count: usize,
}

/// Bounded exact-match catalog owned by an exchange adapter.
#[derive(Debug, Clone, Default)]
pub struct InstrumentRuleCatalog {
    rules: HashMap<InstrumentKey, InstrumentRules>,
}

impl InstrumentRuleCatalog {
    /// Builds a bounded catalog and rejects duplicate exact keys.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog is too large, duplicated, or cannot reserve storage.
    pub fn new(rules: Vec<InstrumentRules>) -> Result<Self, ExchangeError> {
        if rules.len() > MAX_INSTRUMENT_RULES {
            return Err(ExchangeError::resource_limit(
                "instrument rule catalog",
                MAX_INSTRUMENT_RULES,
                rules.len(),
            ));
        }
        let mut catalog = HashMap::new();
        catalog.try_reserve(rules.len()).map_err(|_| {
            ExchangeError::unavailable("unable to reserve bounded instrument rule catalog")
        })?;
        for rule in rules {
            let key = rule.key.clone();
            if catalog.insert(key, rule).is_some() {
                return Err(ExchangeError::invalid(
                    "instrument rule catalog contains a duplicate exact key",
                ));
            }
        }
        Ok(Self { rules: catalog })
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Validates an order against an exact exchange, symbol, and market rule.
    ///
    /// Remote adapters use this fail-closed surface before constructing an
    /// authenticated request. A missing exact rule is rejected instead of
    /// silently accepting exchange defaults.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::Rejected`] when no exact rule exists or when
    /// quantity, price, or notional constraints are violated.
    pub fn validate_order(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Price>,
    ) -> Result<(), ExchangeError> {
        let rules = self
            .find(&intent.exchange, &intent.symbol, intent.market_type)
            .ok_or_else(|| {
                ExchangeError::rejected(format!(
                    "missing exact instrument rules for {}/{}/{:?}",
                    intent.exchange, intent.symbol, intent.market_type
                ))
            })?;
        rules.validate(intent, reference_price.or(intent.price))
    }

    pub(crate) fn find(
        &self,
        exchange: &str,
        symbol: &Symbol,
        market_type: MarketType,
    ) -> Option<&InstrumentRules> {
        self.rules.get(&InstrumentKey {
            exchange: exchange.to_owned(),
            symbol: symbol.clone(),
            market_type,
        })
    }
}
