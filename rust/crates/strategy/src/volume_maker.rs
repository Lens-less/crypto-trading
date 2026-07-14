use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, Quantity, Side, Symbol, TimeInForce,
};

use crate::{StrategyError, StrategyMachine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeMakerMode {
    LimitBoth,
    MarketImbalance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMakerPlanConfig {
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub mode: VolumeMakerMode,
    pub order_quantity: Quantity,
    pub reverse_trading: bool,
    pub post_only: bool,
}

impl TryFrom<&crypto_trading_config::VolumeMakerConfig> for VolumeMakerPlanConfig {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::VolumeMakerConfig) -> Result<Self, Self::Error> {
        config
            .validate_execution_controls()
            .map_err(|_| StrategyError::InvalidConfig("volume maker emergency stop is active"))?;
        let mode = match config.order_mode.trim().to_ascii_lowercase().as_str() {
            "limit" => VolumeMakerMode::LimitBoth,
            "market" => VolumeMakerMode::MarketImbalance,
            _ => {
                return Err(StrategyError::InvalidConfig(
                    "volume maker order mode must be limit or market",
                ));
            }
        };
        Ok(Self {
            exchange: config.exchange.clone(),
            symbol: config.symbol.clone(),
            market_type: config.market_type,
            mode,
            order_quantity: config.order_quantity,
            reverse_trading: config.reverse_trading,
            post_only: config.use_post_only,
        })
    }
}

impl TryFrom<&crypto_trading_config::VolumeMakerConfig> for VolumeMakerStrategy {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::VolumeMakerConfig) -> Result<Self, Self::Error> {
        config
            .validate_execution_controls()
            .map_err(|_| StrategyError::InvalidConfig("volume maker emergency stop is active"))?;
        Self::new(VolumeMakerPlanConfig::try_from(config)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VolumeMakerState {
    #[default]
    Flat,
    Open {
        side: Side,
        quantity: Quantity,
    },
}

#[derive(Debug, Clone)]
pub struct VolumeMakerStrategy {
    config: VolumeMakerPlanConfig,
}

impl VolumeMakerStrategy {
    /// Validates a volume-making plan and constructs its strategy.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for an empty exchange or zero
    /// order quantity.
    pub fn new(config: VolumeMakerPlanConfig) -> Result<Self, StrategyError> {
        if config.exchange.trim().is_empty() {
            return Err(StrategyError::InvalidConfig(
                "volume maker exchange must not be empty",
            ));
        }
        if config.order_quantity.as_decimal().is_zero() {
            return Err(StrategyError::InvalidConfig(
                "volume maker quantity must be positive",
            ));
        }
        Ok(Self { config })
    }

    pub const fn config(&self) -> &VolumeMakerPlanConfig {
        &self.config
    }

    fn validate_snapshot(&self, snapshot: &MarketSnapshot) -> Result<(), StrategyError> {
        if snapshot.exchange() != self.config.exchange
            || snapshot.symbol != self.config.symbol
            || snapshot.market_type != self.config.market_type
        {
            return Err(StrategyError::SnapshotMismatch(format!(
                "expected {}/{}/{:?}, got {}/{}/{:?}",
                self.config.exchange,
                self.config.symbol,
                self.config.market_type,
                snapshot.exchange(),
                snapshot.symbol,
                snapshot.market_type
            )));
        }
        Ok(())
    }

    const fn opposite(side: Side) -> Side {
        match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl StrategyMachine for VolumeMakerStrategy {
    type State = VolumeMakerState;

    fn evaluate(
        &self,
        state: &Self::State,
        snapshot: &MarketSnapshot,
    ) -> Result<Vec<OrderIntent>, StrategyError> {
        self.validate_snapshot(snapshot)?;

        if let VolumeMakerState::Open { side, quantity } = state {
            if quantity.as_decimal().is_zero() {
                return Ok(Vec::new());
            }
            let mut intent = OrderIntent::market(
                self.config.exchange.clone(),
                self.config.symbol.clone(),
                self.config.market_type,
                Self::opposite(*side),
                *quantity,
            );
            intent.reduce_only = true;
            return Ok(vec![intent]);
        }

        match self.config.mode {
            VolumeMakerMode::LimitBoth => {
                let mut buy = OrderIntent::limit(
                    self.config.exchange.clone(),
                    self.config.symbol.clone(),
                    self.config.market_type,
                    Side::Buy,
                    self.config.order_quantity,
                    snapshot.bid(),
                );
                let mut sell_order = OrderIntent::limit(
                    self.config.exchange.clone(),
                    self.config.symbol.clone(),
                    self.config.market_type,
                    Side::Sell,
                    self.config.order_quantity,
                    snapshot.ask(),
                );
                if self.config.post_only {
                    buy.time_in_force = TimeInForce::PostOnly;
                    sell_order.time_in_force = TimeInForce::PostOnly;
                }
                Ok(vec![buy, sell_order])
            }
            VolumeMakerMode::MarketImbalance => {
                let bid_quantity = snapshot
                    .bid_quantity
                    .ok_or(StrategyError::MissingMarketData("bid quantity"))?;
                let ask_quantity = snapshot
                    .ask_quantity
                    .ok_or(StrategyError::MissingMarketData("ask quantity"))?;
                let natural_side = if bid_quantity > ask_quantity {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let side = if self.config.reverse_trading {
                    Self::opposite(natural_side)
                } else {
                    natural_side
                };
                Ok(vec![OrderIntent::market(
                    self.config.exchange.clone(),
                    self.config.symbol.clone(),
                    self.config.market_type,
                    side,
                    self.config.order_quantity,
                )])
            }
        }
    }
}
