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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentRuleOptions {
    pub min_notional: Money,
    pub min_price: Option<Price>,
    pub max_price: Option<Price>,
    pub max_quantity: Option<Quantity>,
    pub market_quantity_step: Option<Quantity>,
    pub market_min_quantity: Option<Quantity>,
    pub market_max_quantity: Option<Quantity>,
    pub max_notional: Option<Money>,
    pub apply_min_notional_to_market: bool,
    pub apply_max_notional_to_market: bool,
    pub market_notional_average_minutes: Option<u32>,
    pub requires_authoritative_market_notional_reference: bool,
}

impl InstrumentRuleOptions {
    #[must_use]
    pub const fn new(min_notional: Money) -> Self {
        Self {
            min_notional,
            min_price: None,
            max_price: None,
            max_quantity: None,
            market_quantity_step: None,
            market_min_quantity: None,
            market_max_quantity: None,
            max_notional: None,
            apply_min_notional_to_market: true,
            apply_max_notional_to_market: true,
            market_notional_average_minutes: None,
            requires_authoritative_market_notional_reference: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentRules {
    key: InstrumentKey,
    price_tick: Price,
    min_price: Option<Price>,
    max_price: Option<Price>,
    quantity_step: Quantity,
    min_quantity: Quantity,
    max_quantity: Option<Quantity>,
    market_quantity_step: Option<Quantity>,
    market_min_quantity: Option<Quantity>,
    market_max_quantity: Option<Quantity>,
    min_notional: Money,
    max_notional: Option<Money>,
    apply_min_notional_to_market: bool,
    apply_max_notional_to_market: bool,
    market_notional_average_minutes: Option<u32>,
    requires_authoritative_market_notional_reference: bool,
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
        Self::with_options(
            exchange,
            symbol,
            market_type,
            price_tick,
            quantity_step,
            min_quantity,
            InstrumentRuleOptions::new(min_notional),
        )
    }

    /// Builds validated rules with optional product-specific bounds.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for empty, non-positive, or
    /// internally inconsistent constraints.
    pub fn with_options(
        exchange: impl Into<String>,
        symbol: Symbol,
        market_type: MarketType,
        price_tick: Price,
        quantity_step: Quantity,
        min_quantity: Quantity,
        options: InstrumentRuleOptions,
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
        if options.min_notional.as_decimal() <= Decimal::ZERO {
            return Err(ExchangeError::invalid(
                "instrument minimum notional must be greater than zero",
            ));
        }
        validate_optional_price("instrument minimum price", options.min_price)?;
        validate_optional_price("instrument maximum price", options.max_price)?;
        validate_optional_quantity("instrument maximum quantity", options.max_quantity)?;
        validate_optional_quantity(
            "instrument market quantity step",
            options.market_quantity_step,
        )?;
        validate_optional_quantity(
            "instrument market minimum quantity",
            options.market_min_quantity,
        )?;
        validate_optional_quantity(
            "instrument market maximum quantity",
            options.market_max_quantity,
        )?;
        validate_optional_money("instrument maximum notional", options.max_notional)?;
        validate_price_range(options.min_price, options.max_price)?;
        validate_quantity_range("instrument quantity", min_quantity, options.max_quantity)?;
        validate_quantity_range(
            "instrument market quantity",
            options.market_min_quantity.unwrap_or(min_quantity),
            options.market_max_quantity.or(options.max_quantity),
        )?;
        validate_notional_range(options.min_notional, options.max_notional)?;
        Ok(Self {
            key: InstrumentKey {
                exchange: exchange.to_owned(),
                symbol,
                market_type,
            },
            price_tick,
            min_price: options.min_price,
            max_price: options.max_price,
            quantity_step,
            min_quantity,
            max_quantity: options.max_quantity,
            market_quantity_step: options.market_quantity_step,
            market_min_quantity: options.market_min_quantity,
            market_max_quantity: options.market_max_quantity,
            min_notional: options.min_notional,
            max_notional: options.max_notional,
            apply_min_notional_to_market: options.apply_min_notional_to_market,
            apply_max_notional_to_market: options.apply_max_notional_to_market,
            market_notional_average_minutes: options.market_notional_average_minutes,
            requires_authoritative_market_notional_reference: options
                .requires_authoritative_market_notional_reference,
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

    pub const fn min_price(&self) -> Option<Price> {
        self.min_price
    }

    pub const fn max_price(&self) -> Option<Price> {
        self.max_price
    }

    pub const fn quantity_step(&self) -> Quantity {
        self.quantity_step
    }

    pub const fn min_quantity(&self) -> Quantity {
        self.min_quantity
    }

    pub const fn max_quantity(&self) -> Option<Quantity> {
        self.max_quantity
    }

    pub const fn market_quantity_step(&self) -> Option<Quantity> {
        self.market_quantity_step
    }

    pub const fn market_min_quantity(&self) -> Option<Quantity> {
        self.market_min_quantity
    }

    pub const fn market_max_quantity(&self) -> Option<Quantity> {
        self.market_max_quantity
    }

    pub const fn min_notional(&self) -> Money {
        self.min_notional
    }

    pub const fn max_notional(&self) -> Option<Money> {
        self.max_notional
    }

    pub const fn apply_min_notional_to_market(&self) -> bool {
        self.apply_min_notional_to_market
    }

    pub const fn apply_max_notional_to_market(&self) -> bool {
        self.apply_max_notional_to_market
    }

    pub const fn market_notional_average_minutes(&self) -> Option<u32> {
        self.market_notional_average_minutes
    }

    pub const fn requires_authoritative_market_notional_reference(&self) -> bool {
        self.requires_authoritative_market_notional_reference
    }

    pub(crate) fn validate(
        &self,
        intent: &OrderIntent,
        reference_price: Option<Price>,
    ) -> Result<(), ExchangeError> {
        let (min_quantity, max_quantity, quantity_step) = self.quantity_limits(intent.order_type);
        let quantity = intent.quantity.as_decimal();
        if quantity < min_quantity.as_decimal() {
            return Err(ExchangeError::rejected(format!(
                "order quantity {quantity} is below minimum {min_quantity}"
            )));
        }
        if let Some(max_quantity) = max_quantity
            && quantity > max_quantity.as_decimal()
        {
            return Err(ExchangeError::rejected(format!(
                "order quantity {quantity} exceeds maximum {max_quantity}",
            )));
        }
        let quantity_remainder = quantity
            .checked_rem(quantity_step.as_decimal())
            .ok_or_else(|| ExchangeError::rejected("unable to validate order quantity step"))?;
        if !quantity_remainder.is_zero() {
            return Err(ExchangeError::rejected(format!(
                "order quantity {quantity} is not aligned to step {quantity_step}"
            )));
        }
        if intent.order_type == crypto_trading_domain::OrderType::Market
            && self.requires_authoritative_market_notional_reference
        {
            return Err(ExchangeError::rejected(
                "market order requires an authoritative market notional reference",
            ));
        }
        if let Some(limit) = intent.price {
            if let Some(min_price) = self.min_price
                && limit.as_decimal() < min_price.as_decimal()
            {
                return Err(ExchangeError::rejected(format!(
                    "order price {limit} is below minimum {min_price}",
                )));
            }
            if let Some(max_price) = self.max_price
                && limit.as_decimal() > max_price.as_decimal()
            {
                return Err(ExchangeError::rejected(format!(
                    "order price {limit} exceeds maximum {max_price}",
                )));
            }
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
        let apply_min_notional = intent.order_type == crypto_trading_domain::OrderType::Limit
            || self.apply_min_notional_to_market;
        let apply_max_notional = self.max_notional.is_some()
            && (intent.order_type == crypto_trading_domain::OrderType::Limit
                || self.apply_max_notional_to_market);
        if !apply_min_notional && !apply_max_notional {
            return Ok(());
        }
        let price = intent.price.or(reference_price).ok_or_else(|| {
            ExchangeError::rejected("instrument notional validation needs a reference price")
        })?;
        let notional = price
            .as_decimal()
            .checked_mul(quantity)
            .ok_or_else(|| ExchangeError::rejected("order notional overflowed"))?;
        if apply_min_notional && notional < self.min_notional.as_decimal() {
            return Err(ExchangeError::rejected(format!(
                "order notional {notional} is below minimum {}",
                self.min_notional
            )));
        }
        if let Some(max_notional) = self.max_notional
            && apply_max_notional
            && notional > max_notional.as_decimal()
        {
            return Err(ExchangeError::rejected(format!(
                "order notional {notional} exceeds maximum {max_notional}",
            )));
        }
        Ok(())
    }

    fn quantity_limits(
        &self,
        order_type: crypto_trading_domain::OrderType,
    ) -> (Quantity, Option<Quantity>, Quantity) {
        if order_type == crypto_trading_domain::OrderType::Market {
            return (
                self.market_min_quantity.unwrap_or(self.min_quantity),
                self.market_max_quantity.or(self.max_quantity),
                self.market_quantity_step.unwrap_or(self.quantity_step),
            );
        }
        (self.min_quantity, self.max_quantity, self.quantity_step)
    }
}

fn validate_optional_price(label: &str, value: Option<Price>) -> Result<(), ExchangeError> {
    if value.is_some_and(|value| value.as_decimal() <= Decimal::ZERO) {
        return Err(ExchangeError::invalid(format!(
            "{label} must be greater than zero when present",
        )));
    }
    Ok(())
}

fn validate_optional_quantity(label: &str, value: Option<Quantity>) -> Result<(), ExchangeError> {
    if value.is_some_and(|value| value.as_decimal() <= Decimal::ZERO) {
        return Err(ExchangeError::invalid(format!(
            "{label} must be greater than zero when present",
        )));
    }
    Ok(())
}

fn validate_optional_money(label: &str, value: Option<Money>) -> Result<(), ExchangeError> {
    if value.is_some_and(|value| value.as_decimal() <= Decimal::ZERO) {
        return Err(ExchangeError::invalid(format!(
            "{label} must be greater than zero when present",
        )));
    }
    Ok(())
}

fn validate_price_range(min: Option<Price>, max: Option<Price>) -> Result<(), ExchangeError> {
    if let Some((min, max)) = min.zip(max)
        && max.as_decimal() < min.as_decimal()
    {
        return Err(ExchangeError::invalid(
            "instrument maximum price must be greater than or equal to minimum price",
        ));
    }
    Ok(())
}

fn validate_quantity_range(
    label: &str,
    min: Quantity,
    max: Option<Quantity>,
) -> Result<(), ExchangeError> {
    if let Some(max) = max
        && max.as_decimal() < min.as_decimal()
    {
        return Err(ExchangeError::invalid(format!(
            "{label} maximum must be greater than or equal to the minimum",
        )));
    }
    Ok(())
}

fn validate_notional_range(min: Money, max: Option<Money>) -> Result<(), ExchangeError> {
    if let Some(max) = max
        && max.as_decimal() < min.as_decimal()
    {
        return Err(ExchangeError::invalid(
            "instrument maximum notional must be greater than or equal to minimum notional",
        ));
    }
    Ok(())
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
