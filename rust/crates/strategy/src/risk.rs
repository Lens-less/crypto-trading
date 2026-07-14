use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, OrderType, Position, PositionSide, Side, Symbol,
};
use rust_decimal::Decimal;

use crate::StrategyError;

const MAX_RISK_BATCH_INTENTS: usize = 10_000;
const MAX_RISK_POSITIONS: usize = 10_000;
const MAX_RISK_MARKETS: usize = 10_000;
const MAX_RISK_SNAPSHOT_AGE_SECONDS: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskLimits {
    pub max_position_value: Decimal,
    pub max_snapshot_age: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRiskSnapshot {
    pub equity: Money,
    pub available_balance: Money,
    pub kill_switch: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskRejection {
    KillSwitchActive,
    StaleAccountData,
    StaleMarketData,
    StalePositionData,
    MarketMismatch,
    InvalidQuantity,
    ReduceOnlyWouldIncrease,
    ArithmeticOverflow,
    InputLimitExceeded {
        input: &'static str,
        count: usize,
        limit: usize,
    },
    MaxPositionValue {
        projected: Decimal,
        limit: Decimal,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskDecision {
    Authorized,
    Rejected(RiskRejection),
}

#[derive(Debug, Clone)]
pub struct RiskEngine {
    limits: RiskLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InstrumentKey {
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
}

impl InstrumentKey {
    fn from_intent(intent: &OrderIntent) -> Self {
        Self {
            exchange: intent.exchange.clone(),
            symbol: intent.symbol.clone(),
            market_type: intent.market_type,
        }
    }

    fn from_position(position: &Position) -> Self {
        Self {
            exchange: position.exchange.clone(),
            symbol: position.symbol.clone(),
            market_type: position.market_type,
        }
    }

    fn from_market(market: &MarketSnapshot) -> Self {
        Self {
            exchange: market.exchange().to_owned(),
            symbol: market.symbol.clone(),
            market_type: market.market_type,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectedPosition {
    signed_quantity: Decimal,
    stale: bool,
}

impl RiskEngine {
    /// Validates limits and constructs the centralized risk engine.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] if the position limit is not
    /// positive or the permitted snapshot age is negative.
    pub fn new(limits: RiskLimits) -> Result<Self, StrategyError> {
        if limits.max_position_value <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "maximum position value must be positive",
            ));
        }
        if limits.max_snapshot_age < Duration::zero() {
            return Err(StrategyError::InvalidConfig(
                "maximum snapshot age must not be negative",
            ));
        }
        if limits.max_snapshot_age > Duration::seconds(MAX_RISK_SNAPSHOT_AGE_SECONDS) {
            return Err(StrategyError::InvalidConfig(
                "maximum snapshot age exceeds the business limit",
            ));
        }
        Ok(Self { limits })
    }

    pub const fn limits(&self) -> &RiskLimits {
        &self.limits
    }

    pub fn authorize(
        &self,
        intent: &OrderIntent,
        account: &AccountRiskSnapshot,
        positions: &[Position],
        market: &MarketSnapshot,
        now: DateTime<Utc>,
    ) -> RiskDecision {
        if let Err(rejection) = self.validate_account(account, now) {
            return RiskDecision::Rejected(rejection);
        }
        if positions.len() > MAX_RISK_POSITIONS {
            return RiskDecision::Rejected(RiskRejection::InputLimitExceeded {
                input: "positions",
                count: positions.len(),
                limit: MAX_RISK_POSITIONS,
            });
        }
        let current = match self.aggregate_position(intent, positions, now) {
            Ok(current) => current,
            Err(rejection) => return RiskDecision::Rejected(rejection),
        };
        match self.authorize_projected(intent, market, current, now) {
            Ok(_) => RiskDecision::Authorized,
            Err(rejection) => RiskDecision::Rejected(rejection),
        }
    }

    /// Authorizes a complete batch against one immutable account/position/market view.
    ///
    /// Intents are projected in order, so every successful earlier intent reserves
    /// exposure for later intents in the same batch. The function is pure; a runtime
    /// can call it while holding its reservation lock and commit the reservation only
    /// after receiving [`RiskDecision::Authorized`].
    pub fn authorize_batch(
        &self,
        intents: &[OrderIntent],
        account: &AccountRiskSnapshot,
        positions: &[Position],
        markets: &[MarketSnapshot],
        now: DateTime<Utc>,
    ) -> RiskDecision {
        if let Err(rejection) = self.validate_account(account, now) {
            return RiskDecision::Rejected(rejection);
        }
        for (input, count, limit) in [
            ("intents", intents.len(), MAX_RISK_BATCH_INTENTS),
            ("positions", positions.len(), MAX_RISK_POSITIONS),
            ("markets", markets.len(), MAX_RISK_MARKETS),
        ] {
            if count > limit {
                return RiskDecision::Rejected(RiskRejection::InputLimitExceeded {
                    input,
                    count,
                    limit,
                });
            }
        }

        let Some(projected_capacity) = positions.len().checked_add(intents.len()) else {
            return RiskDecision::Rejected(RiskRejection::ArithmeticOverflow);
        };
        let mut projected = HashMap::<InstrumentKey, ProjectedPosition>::new();
        if projected.try_reserve(projected_capacity).is_err() {
            return RiskDecision::Rejected(RiskRejection::ArithmeticOverflow);
        }
        for position in positions {
            let key = InstrumentKey::from_position(position);
            let signed = match Self::signed_position(position) {
                Ok(signed) => signed,
                Err(rejection) => return RiskDecision::Rejected(rejection),
            };
            let entry = projected.entry(key).or_insert(ProjectedPosition {
                signed_quantity: Decimal::ZERO,
                stale: false,
            });
            let Some(total) = entry.signed_quantity.checked_add(signed) else {
                return RiskDecision::Rejected(RiskRejection::ArithmeticOverflow);
            };
            entry.signed_quantity = total;
            entry.stale |= self.is_stale(position.updated_at, now);
        }

        let mut market_by_key = HashMap::<InstrumentKey, &MarketSnapshot>::new();
        if market_by_key.try_reserve(markets.len()).is_err() {
            return RiskDecision::Rejected(RiskRejection::ArithmeticOverflow);
        }
        for market in markets {
            if market_by_key
                .insert(InstrumentKey::from_market(market), market)
                .is_some()
            {
                return RiskDecision::Rejected(RiskRejection::MarketMismatch);
            }
        }

        for intent in intents {
            let key = InstrumentKey::from_intent(intent);
            let Some(market) = market_by_key.get(&key).copied() else {
                return RiskDecision::Rejected(RiskRejection::MarketMismatch);
            };
            let current = projected.get(&key).copied().unwrap_or(ProjectedPosition {
                signed_quantity: Decimal::ZERO,
                stale: false,
            });
            let next = match self.authorize_projected(intent, market, current, now) {
                Ok(next) => next,
                Err(rejection) => return RiskDecision::Rejected(rejection),
            };
            projected.insert(
                key,
                ProjectedPosition {
                    signed_quantity: next,
                    stale: false,
                },
            );
        }

        RiskDecision::Authorized
    }

    fn is_stale(&self, timestamp: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        timestamp > now || now.signed_duration_since(timestamp) > self.limits.max_snapshot_age
    }

    fn validate_account(
        &self,
        account: &AccountRiskSnapshot,
        now: DateTime<Utc>,
    ) -> Result<(), RiskRejection> {
        if account.kill_switch {
            return Err(RiskRejection::KillSwitchActive);
        }
        if self.is_stale(account.timestamp, now) {
            return Err(RiskRejection::StaleAccountData);
        }
        Ok(())
    }

    fn aggregate_position(
        &self,
        intent: &OrderIntent,
        positions: &[Position],
        now: DateTime<Utc>,
    ) -> Result<ProjectedPosition, RiskRejection> {
        let mut current = ProjectedPosition {
            signed_quantity: Decimal::ZERO,
            stale: false,
        };
        for position in positions.iter().filter(|position| {
            position.exchange == intent.exchange
                && position.symbol == intent.symbol
                && position.market_type == intent.market_type
        }) {
            current.stale |= self.is_stale(position.updated_at, now);
            current.signed_quantity = current
                .signed_quantity
                .checked_add(Self::signed_position(position)?)
                .ok_or(RiskRejection::ArithmeticOverflow)?;
        }
        Ok(current)
    }

    fn signed_position(position: &Position) -> Result<Decimal, RiskRejection> {
        let quantity = position.quantity.as_decimal();
        match position.side {
            PositionSide::Long => Ok(quantity),
            PositionSide::Short => Decimal::ZERO
                .checked_sub(quantity)
                .ok_or(RiskRejection::ArithmeticOverflow),
            PositionSide::Flat => Ok(Decimal::ZERO),
        }
    }

    fn authorize_projected(
        &self,
        intent: &OrderIntent,
        market: &MarketSnapshot,
        current: ProjectedPosition,
        now: DateTime<Utc>,
    ) -> Result<Decimal, RiskRejection> {
        if self.is_stale(market.timestamp, now) {
            return Err(RiskRejection::StaleMarketData);
        }
        if current.stale {
            return Err(RiskRejection::StalePositionData);
        }
        if intent.exchange != market.exchange()
            || intent.symbol != market.symbol
            || intent.market_type != market.market_type
        {
            return Err(RiskRejection::MarketMismatch);
        }
        if intent.quantity.as_decimal() <= Decimal::ZERO {
            return Err(RiskRejection::InvalidQuantity);
        }

        let order_quantity = match intent.side {
            Side::Buy => Some(intent.quantity.as_decimal()),
            Side::Sell => Decimal::ZERO.checked_sub(intent.quantity.as_decimal()),
        }
        .ok_or(RiskRejection::ArithmeticOverflow)?;
        let projected_quantity = current
            .signed_quantity
            .checked_add(order_quantity)
            .ok_or(RiskRejection::ArithmeticOverflow)?;
        let crosses_flat = !current.signed_quantity.is_zero()
            && !projected_quantity.is_zero()
            && current.signed_quantity.is_sign_positive() != projected_quantity.is_sign_positive();
        let strictly_reduces = !current.signed_quantity.is_zero()
            && projected_quantity.abs() < current.signed_quantity.abs()
            && !crosses_flat;
        if intent.reduce_only && !strictly_reduces {
            return Err(RiskRejection::ReduceOnlyWouldIncrease);
        }
        if intent.reduce_only {
            return Ok(projected_quantity);
        }

        let executable_price = match intent.side {
            Side::Buy => market.ask(),
            Side::Sell => market.bid(),
        };
        let valuation_price = if intent.order_type == OrderType::Limit {
            intent
                .price
                .unwrap_or(executable_price)
                .max(executable_price)
        } else {
            executable_price
        };
        let projected_value = projected_quantity
            .abs()
            .checked_mul(valuation_price.as_decimal())
            .ok_or(RiskRejection::ArithmeticOverflow)?;
        if projected_value > self.limits.max_position_value {
            return Err(RiskRejection::MaxPositionValue {
                projected: projected_value,
                limit: self.limits.max_position_value,
            });
        }
        Ok(projected_quantity)
    }
}
