use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, Price, Quantity, Side, Symbol,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::{StrategyError, StrategyMachine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GridDirection {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridRange {
    Fixed {
        lower: Price,
        upper: Price,
    },
    Follow {
        level_count: u32,
        price_offset_levels: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GridPlanConfig {
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub direction: GridDirection,
    pub range: GridRange,
    pub interval: Decimal,
    pub quantity: Quantity,
    pub martingale_increment: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GridLevel {
    pub index: u32,
    pub price: Price,
    pub quantity: Quantity,
    pub side: Side,
}

#[derive(Debug, Clone)]
pub struct GridPlanner {
    config: GridPlanConfig,
}

impl GridPlanner {
    /// Validates a grid plan and constructs its pure planner.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for empty exchanges, invalid
    /// ranges, non-positive intervals, or negative martingale increments.
    pub fn new(config: GridPlanConfig) -> Result<Self, StrategyError> {
        if config.exchange.trim().is_empty() {
            return Err(StrategyError::InvalidConfig(
                "grid exchange must not be empty",
            ));
        }
        if config.interval <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "grid interval must be positive",
            ));
        }
        if config
            .martingale_increment
            .is_some_and(|increment| increment < Decimal::ZERO)
        {
            return Err(StrategyError::InvalidConfig(
                "martingale increment must not be negative",
            ));
        }

        match config.range {
            GridRange::Fixed { lower, upper } => {
                if lower.as_decimal() >= upper.as_decimal() {
                    return Err(StrategyError::InvalidConfig(
                        "fixed grid lower price must be below upper price",
                    ));
                }
                let count = ((upper.as_decimal() - lower.as_decimal()) / config.interval)
                    .floor()
                    .to_u32()
                    .unwrap_or(0);
                if count == 0 {
                    return Err(StrategyError::InvalidConfig(
                        "fixed grid range must contain at least one level",
                    ));
                }
            }
            GridRange::Follow { level_count: 0, .. } => {
                return Err(StrategyError::InvalidConfig(
                    "follow grid must contain at least one level",
                ));
            }
            GridRange::Follow { .. } => {}
        }

        Ok(Self { config })
    }

    pub const fn config(&self) -> &GridPlanConfig {
        &self.config
    }

    /// Builds levels for a fixed grid.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::MissingMarketData`] for a follow grid or a
    /// financial/configuration error if a level cannot be represented.
    pub fn fixed_levels(&self) -> Result<Vec<GridLevel>, StrategyError> {
        match self.config.range {
            GridRange::Fixed { lower, upper } => {
                self.build_levels(lower.as_decimal(), upper.as_decimal())
            }
            GridRange::Follow { .. } => Err(StrategyError::MissingMarketData(
                "snapshot required for follow grid",
            )),
        }
    }

    /// Builds grid levels, deriving follow-grid bounds from `snapshot`.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] when the snapshot identity differs from the
    /// plan or derived prices/quantities are invalid.
    pub fn levels(&self, snapshot: &MarketSnapshot) -> Result<Vec<GridLevel>, StrategyError> {
        self.validate_snapshot(snapshot)?;

        match self.config.range {
            GridRange::Fixed { lower, upper } => {
                self.build_levels(lower.as_decimal(), upper.as_decimal())
            }
            GridRange::Follow {
                level_count,
                price_offset_levels,
            } => {
                let anchor = snapshot
                    .last
                    .unwrap_or_else(|| snapshot.mid_price())
                    .as_decimal();
                let width = self.config.interval * Decimal::from(level_count);
                let offset = self.config.interval * Decimal::from(price_offset_levels);
                let (lower, upper) = match self.config.direction {
                    GridDirection::Long => {
                        let upper = anchor + offset;
                        (upper - width, upper)
                    }
                    GridDirection::Short => {
                        let lower = anchor - offset;
                        (lower, lower + width)
                    }
                };
                if lower <= Decimal::ZERO {
                    return Err(StrategyError::InvalidFinancialValue(
                        "follow grid lower price must be positive",
                    ));
                }
                self.build_levels(lower, upper)
            }
        }
    }

    /// Converts every planned level into a limit-order intent.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`Self::levels`].
    pub fn intents(&self, snapshot: &MarketSnapshot) -> Result<Vec<OrderIntent>, StrategyError> {
        self.levels(snapshot)?
            .into_iter()
            .map(|level| {
                Ok(OrderIntent::limit(
                    self.config.exchange.clone(),
                    self.config.symbol.clone(),
                    self.config.market_type,
                    level.side,
                    level.quantity,
                    level.price,
                ))
            })
            .collect()
    }

    fn validate_snapshot(&self, snapshot: &MarketSnapshot) -> Result<(), StrategyError> {
        if snapshot.exchange() != self.config.exchange {
            return Err(StrategyError::SnapshotMismatch(format!(
                "expected exchange {}, got {}",
                self.config.exchange,
                snapshot.exchange()
            )));
        }
        if snapshot.symbol != self.config.symbol {
            return Err(StrategyError::SnapshotMismatch(format!(
                "expected symbol {}, got {}",
                self.config.symbol, snapshot.symbol
            )));
        }
        Ok(())
    }

    fn build_levels(
        &self,
        lower: Decimal,
        upper: Decimal,
    ) -> Result<Vec<GridLevel>, StrategyError> {
        let count = ((upper - lower) / self.config.interval)
            .floor()
            .to_u32()
            .ok_or(StrategyError::InvalidConfig(
                "grid level count is too large",
            ))?;
        if count == 0 {
            return Err(StrategyError::InvalidConfig(
                "grid range must contain at least one level",
            ));
        }

        (1..=count)
            .map(|index| {
                let price_value = match self.config.direction {
                    GridDirection::Long => lower + self.config.interval * Decimal::from(index - 1),
                    GridDirection::Short => upper - self.config.interval * Decimal::from(index - 1),
                };
                let increment_count = match self.config.direction {
                    GridDirection::Long => count - index,
                    GridDirection::Short => index - 1,
                };
                let quantity_value = self.config.quantity.as_decimal()
                    + self.config.martingale_increment.unwrap_or_default()
                        * Decimal::from(increment_count);
                let price = Price::new(price_value)
                    .map_err(|_| StrategyError::InvalidFinancialValue("grid level price"))?;
                let quantity = Quantity::new(quantity_value)
                    .map_err(|_| StrategyError::InvalidFinancialValue("grid level quantity"))?;

                Ok(GridLevel {
                    index,
                    price,
                    quantity,
                    side: match self.config.direction {
                        GridDirection::Long => Side::Buy,
                        GridDirection::Short => Side::Sell,
                    },
                })
            })
            .collect()
    }
}

impl TryFrom<&crypto_trading_config::GridConfig> for GridPlanConfig {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::GridConfig) -> Result<Self, Self::Error> {
        let direction = if config.mode.is_short() {
            GridDirection::Short
        } else {
            GridDirection::Long
        };
        let range = if config.mode.is_follow() {
            let level_count = config
                .follow_grid_count
                .ok_or(StrategyError::InvalidConfig(
                    "follow grid count is required",
                ))?;
            let price_offset_levels = u32::try_from(config.price_offset_grids).map_err(|_| {
                StrategyError::InvalidConfig("grid price offset must not be negative")
            })?;
            GridRange::Follow {
                level_count,
                price_offset_levels,
            }
        } else {
            GridRange::Fixed {
                lower: config.lower_price.ok_or(StrategyError::InvalidConfig(
                    "fixed grid lower price is required",
                ))?,
                upper: config.upper_price.ok_or(StrategyError::InvalidConfig(
                    "fixed grid upper price is required",
                ))?,
            }
        };

        Ok(Self {
            exchange: config.exchange.clone(),
            symbol: config.symbol.clone(),
            market_type: config.market_type,
            direction,
            range,
            interval: config.grid_interval.as_decimal(),
            quantity: config.order_amount,
            martingale_increment: config.martingale_increment.map(Quantity::as_decimal),
        })
    }
}

impl TryFrom<&crypto_trading_config::GridConfig> for GridPlanner {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::GridConfig) -> Result<Self, Self::Error> {
        Self::new(GridPlanConfig::try_from(config)?)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GridState {
    pub initialized: bool,
}

#[derive(Debug, Clone)]
pub struct GridStrategy {
    planner: GridPlanner,
}

impl GridStrategy {
    pub const fn new(planner: GridPlanner) -> Self {
        Self { planner }
    }

    pub const fn planner(&self) -> &GridPlanner {
        &self.planner
    }
}

impl StrategyMachine for GridStrategy {
    type State = GridState;

    fn evaluate(
        &self,
        state: &Self::State,
        snapshot: &MarketSnapshot,
    ) -> Result<Vec<OrderIntent>, StrategyError> {
        if state.initialized {
            return Ok(Vec::new());
        }
        self.planner.intents(snapshot)
    }
}
