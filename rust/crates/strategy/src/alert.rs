use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use rust_decimal::Decimal;

use crate::StrategyError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolatilityAlertConfig {
    pub window: Duration,
    pub threshold_percent: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertConfig {
    pub upper_limit: Option<Price>,
    pub lower_limit: Option<Price>,
    pub volatility: Option<VolatilityAlertConfig>,
    pub cooldown: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlertKind {
    VolatilityUp,
    VolatilityDown,
    UpperLimit,
    LowerLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricePoint {
    pub timestamp: DateTime<Utc>,
    pub price: Price,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlertState {
    history: VecDeque<PricePoint>,
    last_alerts: BTreeMap<AlertKind, DateTime<Utc>>,
}

impl AlertState {
    /// Appends a chronological observation to the alert window.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] if the timestamp precedes the
    /// latest recorded observation.
    pub fn record_price(
        &mut self,
        timestamp: DateTime<Utc>,
        price: Price,
    ) -> Result<(), StrategyError> {
        if self
            .history
            .back()
            .is_some_and(|point| timestamp < point.timestamp)
        {
            return Err(StrategyError::InvalidConfig(
                "alert price history must be chronological",
            ));
        }
        self.history.push_back(PricePoint { timestamp, price });
        Ok(())
    }

    pub fn record_alert(&mut self, kind: AlertKind, timestamp: DateTime<Utc>) {
        self.last_alerts.insert(kind, timestamp);
    }

    pub fn price_history(&self) -> &VecDeque<PricePoint> {
        &self.history
    }

    pub fn last_alert_at(&self, kind: AlertKind) -> Option<DateTime<Utc>> {
        match kind {
            AlertKind::VolatilityUp | AlertKind::VolatilityDown => {
                [AlertKind::VolatilityUp, AlertKind::VolatilityDown]
                    .into_iter()
                    .filter_map(|volatility_kind| self.last_alerts.get(&volatility_kind).copied())
                    .max()
            }
            AlertKind::UpperLimit | AlertKind::LowerLimit => self.last_alerts.get(&kind).copied(),
        }
    }

    pub fn prune_before(&mut self, timestamp: DateTime<Utc>) {
        while self
            .history
            .front()
            .is_some_and(|point| point.timestamp < timestamp)
        {
            self.history.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceAlert {
    pub exchange: String,
    pub symbol: Symbol,
    pub kind: AlertKind,
    pub price: Price,
    pub change_percent: Option<Decimal>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AlertStrategy {
    config: AlertConfig,
    scope: Option<AlertScope>,
}

#[derive(Debug, Clone)]
struct AlertScope {
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
}

impl AlertStrategy {
    /// Validates alert thresholds and constructs the strategy.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for invalid limits, windows,
    /// thresholds, or cooldowns.
    pub fn new(config: AlertConfig) -> Result<Self, StrategyError> {
        if config
            .upper_limit
            .is_some_and(|price| price.as_decimal() <= Decimal::ZERO)
            || config
                .lower_limit
                .is_some_and(|price| price.as_decimal() <= Decimal::ZERO)
        {
            return Err(StrategyError::InvalidConfig(
                "configured alert limits must be positive",
            ));
        }
        if config
            .upper_limit
            .zip(config.lower_limit)
            .is_some_and(|(upper, lower)| lower >= upper)
        {
            return Err(StrategyError::InvalidConfig(
                "lower alert limit must be below upper limit",
            ));
        }
        if config.cooldown < Duration::zero() {
            return Err(StrategyError::InvalidConfig(
                "alert cooldown must not be negative",
            ));
        }
        if config.volatility.as_ref().is_some_and(|volatility| {
            volatility.window <= Duration::zero() || volatility.threshold_percent <= Decimal::ZERO
        }) {
            return Err(StrategyError::InvalidConfig(
                "volatility window and threshold must be positive",
            ));
        }
        Ok(Self {
            config,
            scope: None,
        })
    }

    /// Constructs a symbol-scoped strategy from the compatibility configuration.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] when the symbol is missing or
    /// disabled, or when its thresholds are invalid.
    pub fn from_config(
        config: &crypto_trading_config::PriceAlertConfig,
        symbol: &Symbol,
    ) -> Result<Self, StrategyError> {
        let symbol_config = config
            .symbols
            .iter()
            .find(|candidate| candidate.symbol == *symbol)
            .ok_or(StrategyError::InvalidConfig(
                "price-alert symbol is not configured",
            ))?;
        if !symbol_config.enabled {
            return Err(StrategyError::InvalidConfig(
                "price-alert symbol is disabled",
            ));
        }
        let volatility = symbol_config
            .volatility_alert
            .enabled
            .then(|| VolatilityAlertConfig {
                window: Duration::seconds(
                    i64::try_from(symbol_config.volatility_alert.time_window_seconds)
                        .unwrap_or(i64::MAX),
                ),
                threshold_percent: symbol_config.volatility_alert.threshold_percent,
            });
        let (upper_limit, lower_limit) = if symbol_config.price_alert.enabled {
            (
                symbol_config.price_alert.upper_price,
                symbol_config.price_alert.lower_price,
            )
        } else {
            (None, None)
        };
        let cooldown =
            Duration::seconds(i64::try_from(config.cooldown_seconds).unwrap_or(i64::MAX));
        let mut strategy = Self::new(AlertConfig {
            upper_limit,
            lower_limit,
            volatility,
            cooldown,
        })?;
        strategy.scope = Some(AlertScope {
            exchange: config.exchange.clone(),
            symbol: symbol_config.symbol.clone(),
            market_type: symbol_config.market_type,
        });
        Ok(strategy)
    }

    pub const fn config(&self) -> &AlertConfig {
        &self.config
    }

    /// Evaluates volatility and absolute-price thresholds at snapshot time.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidFinancialValue`] when the current price
    /// is zero.
    pub fn evaluate(
        &self,
        state: &AlertState,
        snapshot: &MarketSnapshot,
    ) -> Result<Vec<PriceAlert>, StrategyError> {
        if self.scope.as_ref().is_some_and(|scope| {
            scope.exchange != snapshot.exchange()
                || scope.symbol != snapshot.symbol
                || scope.market_type != snapshot.market_type
        }) {
            return Err(StrategyError::SnapshotMismatch(
                "snapshot does not match configured alert symbol".to_owned(),
            ));
        }
        let current = snapshot.last.unwrap_or_else(|| snapshot.mid_price());
        if current.as_decimal() <= Decimal::ZERO {
            return Err(StrategyError::InvalidFinancialValue("alert current price"));
        }
        let mut alerts = Vec::new();

        if let Some(volatility) = &self.config.volatility {
            let window_start = snapshot.timestamp - volatility.window;
            let baseline = state
                .history
                .iter()
                .rev()
                .find(|point| point.timestamp <= window_start);
            if let Some(baseline) =
                baseline.filter(|point| point.price.as_decimal() > Decimal::ZERO)
            {
                let change = (current.as_decimal() - baseline.price.as_decimal())
                    / baseline.price.as_decimal()
                    * Decimal::ONE_HUNDRED;
                if change.abs() >= volatility.threshold_percent {
                    let kind = if change.is_sign_positive() {
                        AlertKind::VolatilityUp
                    } else {
                        AlertKind::VolatilityDown
                    };
                    if self.can_emit(state, kind, snapshot.timestamp) {
                        alerts.push(Self::alert(snapshot, current, kind, Some(change)));
                    }
                }
            }
        }

        if self
            .config
            .upper_limit
            .is_some_and(|limit| current >= limit)
            && self.can_emit(state, AlertKind::UpperLimit, snapshot.timestamp)
        {
            alerts.push(Self::alert(snapshot, current, AlertKind::UpperLimit, None));
        }
        if self
            .config
            .lower_limit
            .is_some_and(|limit| current <= limit)
            && self.can_emit(state, AlertKind::LowerLimit, snapshot.timestamp)
        {
            alerts.push(Self::alert(snapshot, current, AlertKind::LowerLimit, None));
        }

        Ok(alerts)
    }

    fn can_emit(&self, state: &AlertState, kind: AlertKind, now: DateTime<Utc>) -> bool {
        state
            .last_alert_at(kind)
            .is_none_or(|last| now - last >= self.config.cooldown)
    }

    fn alert(
        snapshot: &MarketSnapshot,
        price: Price,
        kind: AlertKind,
        change_percent: Option<Decimal>,
    ) -> PriceAlert {
        PriceAlert {
            exchange: snapshot.exchange().to_owned(),
            symbol: snapshot.symbol.clone(),
            kind,
            price,
            change_percent,
            timestamp: snapshot.timestamp,
        }
    }
}
