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
    InvalidAccountState {
        field: &'static str,
        value: Decimal,
    },
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
    OpeningNotionalExceedsBuyingPower {
        required: Decimal,
        buying_power: Decimal,
    },
    SpotShortOpenUnsupported {
        opening: Decimal,
        projected: Decimal,
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
    observed_side: Option<DirectionalPositionSide>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProjectionCheck {
    projected_quantity: Decimal,
    opening_notional: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectionalPositionSide {
    Long,
    Short,
}

impl DirectionalPositionSide {
    const fn from_position_side(side: PositionSide) -> Option<Self> {
        match side {
            PositionSide::Long => Some(Self::Long),
            PositionSide::Short => Some(Self::Short),
            PositionSide::Flat => None,
        }
    }
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
            Ok(check) => {
                match Self::ensure_buying_power(
                    check.opening_notional,
                    Self::opening_buying_power(account),
                ) {
                    Ok(()) => RiskDecision::Authorized,
                    Err(rejection) => RiskDecision::Rejected(rejection),
                }
            }
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
            let entry = projected.entry(key).or_insert(ProjectedPosition {
                signed_quantity: Decimal::ZERO,
                stale: false,
                observed_side: None,
            });
            if let Err(rejection) = self.accumulate_position(entry, position, now) {
                return RiskDecision::Rejected(rejection);
            }
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

        let buying_power = Self::opening_buying_power(account);
        let mut required_opening_notional = Decimal::ZERO;
        for intent in intents {
            let key = InstrumentKey::from_intent(intent);
            let Some(market) = market_by_key.get(&key).copied() else {
                return RiskDecision::Rejected(RiskRejection::MarketMismatch);
            };
            let current = projected.get(&key).copied().unwrap_or(ProjectedPosition {
                signed_quantity: Decimal::ZERO,
                stale: false,
                observed_side: None,
            });
            let check = match self.authorize_projected(intent, market, current, now) {
                Ok(check) => check,
                Err(rejection) => return RiskDecision::Rejected(rejection),
            };
            required_opening_notional =
                match required_opening_notional.checked_add(check.opening_notional) {
                    Some(total) => total,
                    None => return RiskDecision::Rejected(RiskRejection::ArithmeticOverflow),
                };
            if let Err(rejection) =
                Self::ensure_buying_power(required_opening_notional, buying_power)
            {
                return RiskDecision::Rejected(rejection);
            }
            projected.insert(
                key,
                ProjectedPosition {
                    signed_quantity: check.projected_quantity,
                    stale: false,
                    observed_side: None,
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
        for (field, value) in [
            ("equity", account.equity.as_decimal()),
            ("available_balance", account.available_balance.as_decimal()),
        ] {
            if value < Decimal::ZERO {
                return Err(RiskRejection::InvalidAccountState { field, value });
            }
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
            observed_side: None,
        };
        for position in positions.iter().filter(|position| {
            position.exchange == intent.exchange
                && position.symbol == intent.symbol
                && position.market_type == intent.market_type
        }) {
            self.accumulate_position(&mut current, position, now)?;
        }
        Ok(current)
    }

    fn accumulate_position(
        &self,
        current: &mut ProjectedPosition,
        position: &Position,
        now: DateTime<Utc>,
    ) -> Result<(), RiskRejection> {
        current.stale |= self.is_stale(position.updated_at, now);
        let signed_quantity = Self::signed_position(position)?;
        if let Some(side) = DirectionalPositionSide::from_position_side(position.side) {
            match current.observed_side {
                Some(existing) if existing != side => return Err(RiskRejection::InvalidQuantity),
                Some(_) => {}
                None => current.observed_side = Some(side),
            }
        }
        current.signed_quantity = current
            .signed_quantity
            .checked_add(signed_quantity)
            .ok_or(RiskRejection::ArithmeticOverflow)?;
        Ok(())
    }

    fn signed_position(position: &Position) -> Result<Decimal, RiskRejection> {
        let quantity = position.quantity.as_decimal();
        match position.side {
            PositionSide::Long if quantity > Decimal::ZERO => Ok(quantity),
            PositionSide::Short if quantity > Decimal::ZERO => Decimal::ZERO
                .checked_sub(quantity)
                .ok_or(RiskRejection::ArithmeticOverflow),
            PositionSide::Flat if quantity.is_zero() => Ok(Decimal::ZERO),
            _ => Err(RiskRejection::InvalidQuantity),
        }
    }

    fn opening_buying_power(account: &AccountRiskSnapshot) -> Decimal {
        account
            .equity
            .as_decimal()
            .max(Decimal::ZERO)
            .min(account.available_balance.as_decimal().max(Decimal::ZERO))
    }

    fn ensure_buying_power(
        required_opening_notional: Decimal,
        buying_power: Decimal,
    ) -> Result<(), RiskRejection> {
        if required_opening_notional > buying_power {
            return Err(RiskRejection::OpeningNotionalExceedsBuyingPower {
                required: required_opening_notional,
                buying_power,
            });
        }
        Ok(())
    }

    fn opening_quantity(
        current_quantity: Decimal,
        order_quantity: Decimal,
    ) -> Result<Decimal, RiskRejection> {
        if current_quantity.is_zero()
            || current_quantity.is_sign_positive() == order_quantity.is_sign_positive()
        {
            return Ok(order_quantity.abs());
        }
        order_quantity
            .abs()
            .checked_sub(current_quantity.abs())
            .map(|quantity| quantity.max(Decimal::ZERO))
            .ok_or(RiskRejection::ArithmeticOverflow)
    }

    fn authorize_projected(
        &self,
        intent: &OrderIntent,
        market: &MarketSnapshot,
        current: ProjectedPosition,
        now: DateTime<Utc>,
    ) -> Result<ProjectionCheck, RiskRejection> {
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
            return Ok(ProjectionCheck {
                projected_quantity,
                opening_notional: Decimal::ZERO,
            });
        }
        if strictly_reduces {
            return Ok(ProjectionCheck {
                projected_quantity,
                opening_notional: Decimal::ZERO,
            });
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
        let opening_quantity = Self::opening_quantity(current.signed_quantity, order_quantity)?;
        if intent.market_type == MarketType::Spot
            && projected_quantity.is_sign_negative()
            && opening_quantity > Decimal::ZERO
        {
            return Err(RiskRejection::SpotShortOpenUnsupported {
                opening: opening_quantity,
                projected: projected_quantity.abs(),
            });
        }
        let opening_notional = opening_quantity
            .checked_mul(valuation_price.as_decimal())
            .ok_or(RiskRejection::ArithmeticOverflow)?;
        Ok(ProjectionCheck {
            projected_quantity,
            opening_notional,
        })
    }
}
