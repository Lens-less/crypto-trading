use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, Price, Quantity, Side, Symbol,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::StrategyError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpreadQuote {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_symbol: Symbol,
    pub sell_symbol: Symbol,
    pub buy_market_type: MarketType,
    pub sell_market_type: MarketType,
    pub buy_price: Price,
    pub sell_price: Price,
    pub absolute: Decimal,
    pub percent: Decimal,
}

impl SpreadQuote {
    pub fn direction(&self) -> ArbitrageDirection {
        ArbitrageDirection {
            buy_exchange: self.buy_exchange.clone(),
            sell_exchange: self.sell_exchange.clone(),
            buy_symbol: self.buy_symbol.clone(),
            sell_symbol: self.sell_symbol.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SpreadCalculator;

impl SpreadCalculator {
    /// Calculates both executable cross-market directions.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidFinancialValue`] when either executable
    /// buy price is zero.
    pub fn directions(
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<[SpreadQuote; 2], StrategyError> {
        if left.ask().as_decimal() <= Decimal::ZERO || right.ask().as_decimal() <= Decimal::ZERO {
            return Err(StrategyError::InvalidFinancialValue(
                "arbitrage buy price must be positive",
            ));
        }

        Ok([Self::quote(left, right), Self::quote(right, left)])
    }

    /// Returns the direction with the greatest spread, including negative spreads.
    ///
    /// # Errors
    ///
    /// Propagates validation errors from [`Self::directions`].
    pub fn best(
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<SpreadQuote, StrategyError> {
        let [first, second] = Self::directions(left, right)?;
        if first.percent >= second.percent {
            Ok(first)
        } else {
            Ok(second)
        }
    }

    /// Returns the best direction only when its spread is positive.
    ///
    /// # Errors
    ///
    /// Propagates validation errors from [`Self::best`].
    pub fn best_positive(
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<Option<SpreadQuote>, StrategyError> {
        let best = Self::best(left, right)?;
        Ok((best.percent > Decimal::ZERO).then_some(best))
    }

    fn quote(buy: &MarketSnapshot, sell: &MarketSnapshot) -> SpreadQuote {
        let absolute = sell.bid().as_decimal() - buy.ask().as_decimal();
        let percent = absolute / buy.ask().as_decimal() * Decimal::ONE_HUNDRED;
        SpreadQuote {
            buy_exchange: buy.exchange().to_owned(),
            sell_exchange: sell.exchange().to_owned(),
            buy_symbol: buy.symbol.clone(),
            sell_symbol: sell.symbol.clone(),
            buy_market_type: buy.market_type,
            sell_market_type: sell.market_type,
            buy_price: buy.ask(),
            sell_price: sell.bid(),
            absolute,
            percent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentedArbitrageConfig {
    pub initial_spread_percent: Decimal,
    pub grid_step_percent: Decimal,
    pub max_segments: u32,
    pub base_quantity: Quantity,
    /// Ratio of T1 used as T0, the first segment's close threshold.
    pub first_close_ratio: Decimal,
}

impl From<&crypto_trading_config::ArbitrageConfig> for SegmentedArbitrageConfig {
    fn from(config: &crypto_trading_config::ArbitrageConfig) -> Self {
        Self {
            initial_spread_percent: config.min_spread_pct,
            grid_step_percent: config.grid_step_pct,
            max_segments: config.max_segments,
            base_quantity: config.base_quantity,
            first_close_ratio: config.first_close_ratio,
        }
    }
}

impl TryFrom<&crypto_trading_config::ArbitrageConfig> for ArbitrageStrategy {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::ArbitrageConfig) -> Result<Self, Self::Error> {
        Self::new(SegmentedArbitrageConfig::from(config))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbitrageDirection {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_symbol: Symbol,
    pub sell_symbol: Symbol,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArbitrageState {
    pub position_quantity: Decimal,
    pub direction: Option<ArbitrageDirection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbitrageDecisionKind {
    Hold,
    Open,
    Increase,
    Reduce,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbitrageDecision {
    pub kind: ArbitrageDecisionKind,
    pub segment: u32,
    pub target_quantity: Decimal,
    pub delta_quantity: Decimal,
    pub spread: SpreadQuote,
    pub direction: Option<ArbitrageDirection>,
    pub intents: Vec<OrderIntent>,
}

pub trait PairStrategyMachine {
    type State;
    type Decision;

    /// Evaluates a strategy that requires two coherent market snapshots.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for invalid state, missing legs, or financial
    /// values outside the domain.
    fn evaluate_pair(
        &self,
        state: &Self::State,
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<Self::Decision, StrategyError>;
}

#[derive(Debug, Clone)]
pub struct ArbitrageStrategy {
    config: SegmentedArbitrageConfig,
    open_thresholds: Vec<Decimal>,
    close_thresholds: Vec<Decimal>,
}

impl ArbitrageStrategy {
    /// Validates segmented thresholds and constructs an arbitrage strategy.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for invalid thresholds,
    /// segment counts, quantities, or close ratios.
    pub fn new(config: SegmentedArbitrageConfig) -> Result<Self, StrategyError> {
        if config.initial_spread_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "initial arbitrage spread must be positive",
            ));
        }
        if config.grid_step_percent <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "arbitrage grid step must be positive",
            ));
        }
        if config.max_segments == 0 {
            return Err(StrategyError::InvalidConfig(
                "arbitrage must allow at least one segment",
            ));
        }
        if config.base_quantity.as_decimal() <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "arbitrage base quantity must be positive",
            ));
        }
        if config.first_close_ratio < Decimal::ZERO || config.first_close_ratio >= Decimal::ONE {
            return Err(StrategyError::InvalidConfig(
                "first close ratio must be in [0, 1)",
            ));
        }

        let open_thresholds: Vec<_> = (0..config.max_segments)
            .map(|offset| {
                config.initial_spread_percent + config.grid_step_percent * Decimal::from(offset)
            })
            .collect();
        let close_thresholds =
            std::iter::once(config.initial_spread_percent * config.first_close_ratio)
                .chain(
                    open_thresholds
                        .iter()
                        .copied()
                        .take(open_thresholds.len() - 1),
                )
                .collect();

        Ok(Self {
            config,
            open_thresholds,
            close_thresholds,
        })
    }

    pub const fn config(&self) -> &SegmentedArbitrageConfig {
        &self.config
    }

    pub fn open_thresholds(&self) -> &[Decimal] {
        &self.open_thresholds
    }

    pub fn close_thresholds(&self) -> &[Decimal] {
        &self.close_thresholds
    }

    pub fn segment_for_spread(&self, spread_percent: Decimal) -> u32 {
        Self::count_thresholds(spread_percent, &self.open_thresholds)
    }

    fn count_thresholds(value: Decimal, thresholds: &[Decimal]) -> u32 {
        thresholds
            .iter()
            .rposition(|threshold| value >= *threshold)
            .map_or(0, |index| u32::try_from(index + 1).unwrap_or(u32::MAX))
    }

    fn current_segments(&self, position_quantity: Decimal) -> u32 {
        if position_quantity <= Decimal::ZERO {
            return 0;
        }
        (position_quantity / self.config.base_quantity.as_decimal())
            .ceil()
            .to_u32()
            .unwrap_or(self.config.max_segments)
            .min(self.config.max_segments)
    }

    fn quote_for_direction(
        direction: &ArbitrageDirection,
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<SpreadQuote, StrategyError> {
        SpreadCalculator::directions(left, right)?
            .into_iter()
            .find(|quote| {
                quote.buy_exchange == direction.buy_exchange
                    && quote.sell_exchange == direction.sell_exchange
                    && quote.buy_symbol == direction.buy_symbol
                    && quote.sell_symbol == direction.sell_symbol
            })
            .ok_or_else(|| {
                StrategyError::SnapshotMismatch(
                    "snapshot pair does not contain the locked arbitrage direction".to_owned(),
                )
            })
    }

    fn opening_intents(quote: &SpreadQuote, quantity: Quantity) -> Vec<OrderIntent> {
        vec![
            OrderIntent::limit(
                quote.buy_exchange.clone(),
                quote.buy_symbol.clone(),
                quote.buy_market_type,
                Side::Buy,
                quantity,
                quote.buy_price,
            ),
            OrderIntent::limit(
                quote.sell_exchange.clone(),
                quote.sell_symbol.clone(),
                quote.sell_market_type,
                Side::Sell,
                quantity,
                quote.sell_price,
            ),
        ]
    }

    fn closing_intents(
        quote: &SpreadQuote,
        quantity: Quantity,
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<Vec<OrderIntent>, StrategyError> {
        let original_buy =
            Self::matching_snapshot(&quote.buy_exchange, &quote.buy_symbol, left, right)?;
        let original_sell =
            Self::matching_snapshot(&quote.sell_exchange, &quote.sell_symbol, left, right)?;

        let mut buy_to_cover = OrderIntent::limit(
            original_sell.exchange().to_owned(),
            original_sell.symbol.clone(),
            original_sell.market_type,
            Side::Buy,
            quantity,
            original_sell.ask(),
        );
        buy_to_cover.reduce_only = true;
        let mut sell_long = OrderIntent::limit(
            original_buy.exchange().to_owned(),
            original_buy.symbol.clone(),
            original_buy.market_type,
            Side::Sell,
            quantity,
            original_buy.bid(),
        );
        sell_long.reduce_only = true;
        Ok(vec![buy_to_cover, sell_long])
    }

    fn matching_snapshot<'a>(
        exchange: &str,
        symbol: &Symbol,
        left: &'a MarketSnapshot,
        right: &'a MarketSnapshot,
    ) -> Result<&'a MarketSnapshot, StrategyError> {
        [left, right]
            .into_iter()
            .find(|snapshot| snapshot.exchange() == exchange && snapshot.symbol == *symbol)
            .ok_or_else(|| {
                StrategyError::SnapshotMismatch(format!(
                    "missing {exchange}/{symbol} from arbitrage pair"
                ))
            })
    }
}

impl PairStrategyMachine for ArbitrageStrategy {
    type State = ArbitrageState;
    type Decision = ArbitrageDecision;

    fn evaluate_pair(
        &self,
        state: &Self::State,
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<Self::Decision, StrategyError> {
        if state.position_quantity < Decimal::ZERO {
            return Err(StrategyError::InvalidFinancialValue(
                "arbitrage position quantity",
            ));
        }
        if state.position_quantity > Decimal::ZERO && state.direction.is_none() {
            return Err(StrategyError::InvalidConfig(
                "non-flat arbitrage state requires a locked direction",
            ));
        }

        let quote = match &state.direction {
            Some(direction) => Self::quote_for_direction(direction, left, right)?,
            None => SpreadCalculator::best(left, right)?,
        };
        let direction = state.direction.clone().or_else(|| Some(quote.direction()));
        let current_segments = self.current_segments(state.position_quantity);
        let open_segments = Self::count_thresholds(quote.percent, &self.open_thresholds);
        let keep_segments =
            Self::count_thresholds(quote.percent, &self.close_thresholds).min(current_segments);
        let target_segments = if open_segments > current_segments {
            open_segments
        } else {
            keep_segments
        };
        let target_quantity =
            self.config.base_quantity.as_decimal() * Decimal::from(target_segments);

        let (kind, delta_quantity, intents) = match target_quantity.cmp(&state.position_quantity) {
            std::cmp::Ordering::Greater => {
                let delta = target_quantity - state.position_quantity;
                let quantity = Quantity::new(delta).map_err(|_| {
                    StrategyError::InvalidFinancialValue("arbitrage opening quantity")
                })?;
                let kind = if state.position_quantity.is_zero() {
                    ArbitrageDecisionKind::Open
                } else {
                    ArbitrageDecisionKind::Increase
                };
                (kind, delta, Self::opening_intents(&quote, quantity))
            }
            std::cmp::Ordering::Less => {
                let delta = state.position_quantity - target_quantity;
                let quantity = Quantity::new(delta).map_err(|_| {
                    StrategyError::InvalidFinancialValue("arbitrage closing quantity")
                })?;
                (
                    ArbitrageDecisionKind::Reduce,
                    delta,
                    Self::closing_intents(&quote, quantity, left, right)?,
                )
            }
            std::cmp::Ordering::Equal => (ArbitrageDecisionKind::Hold, Decimal::ZERO, Vec::new()),
        };

        Ok(ArbitrageDecision {
            kind,
            segment: target_segments,
            target_quantity,
            delta_quantity,
            spread: quote,
            direction,
            intents,
        })
    }
}
