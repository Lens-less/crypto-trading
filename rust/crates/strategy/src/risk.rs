use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{MarketSnapshot, Money, OrderIntent, Position, PositionSide, Side};
use rust_decimal::Decimal;

use crate::StrategyError;

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
    MaxPositionValue { projected: Decimal, limit: Decimal },
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
        if account.kill_switch {
            return RiskDecision::Rejected(RiskRejection::KillSwitchActive);
        }
        if self.is_stale(account.timestamp, now) {
            return RiskDecision::Rejected(RiskRejection::StaleAccountData);
        }
        if self.is_stale(market.timestamp, now) {
            return RiskDecision::Rejected(RiskRejection::StaleMarketData);
        }
        if intent.exchange != market.exchange()
            || intent.symbol != market.symbol
            || intent.market_type != market.market_type
        {
            return RiskDecision::Rejected(RiskRejection::MarketMismatch);
        }
        if intent.quantity.as_decimal() <= Decimal::ZERO {
            return RiskDecision::Rejected(RiskRejection::InvalidQuantity);
        }

        let matching_position = positions.iter().find(|position| {
            position.exchange == intent.exchange
                && position.symbol == intent.symbol
                && position.market_type == intent.market_type
        });
        if matching_position.is_some_and(|position| self.is_stale(position.updated_at, now)) {
            return RiskDecision::Rejected(RiskRejection::StalePositionData);
        }

        let current_quantity = matching_position.map_or(Decimal::ZERO, |position| {
            let quantity = position.quantity.as_decimal();
            match position.side {
                PositionSide::Long => quantity,
                PositionSide::Short => -quantity,
                PositionSide::Flat => Decimal::ZERO,
            }
        });
        let order_quantity = match intent.side {
            Side::Buy => intent.quantity.as_decimal(),
            Side::Sell => -intent.quantity.as_decimal(),
        };
        let projected_quantity = current_quantity + order_quantity;
        let crosses_flat = !current_quantity.is_zero()
            && !projected_quantity.is_zero()
            && current_quantity.is_sign_positive() != projected_quantity.is_sign_positive();
        if intent.reduce_only
            && (current_quantity.is_zero()
                || projected_quantity.abs() > current_quantity.abs()
                || crosses_flat)
        {
            return RiskDecision::Rejected(RiskRejection::ReduceOnlyWouldIncrease);
        }

        let execution_price = intent.price.unwrap_or_else(|| match intent.side {
            Side::Buy => market.ask(),
            Side::Sell => market.bid(),
        });
        let execution_price = execution_price.as_decimal();
        let projected_value = projected_quantity.abs() * execution_price;
        if projected_value > self.limits.max_position_value {
            return RiskDecision::Rejected(RiskRejection::MaxPositionValue {
                projected: projected_value,
                limit: self.limits.max_position_value,
            });
        }

        RiskDecision::Authorized
    }

    fn is_stale(&self, timestamp: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        timestamp > now || now - timestamp > self.limits.max_snapshot_age
    }
}
