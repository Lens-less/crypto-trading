use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, Price, Quantity, Side, Symbol,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::StrategyError;

const MAX_ARBITRAGE_SEGMENTS: u32 = 10_000;

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
            buy_market_type: self.buy_market_type,
            sell_market_type: self.sell_market_type,
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

        Ok([Self::quote(left, right)?, Self::quote(right, left)?])
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

    fn quote(buy: &MarketSnapshot, sell: &MarketSnapshot) -> Result<SpreadQuote, StrategyError> {
        let absolute = sell
            .bid()
            .as_decimal()
            .checked_sub(buy.ask().as_decimal())
            .ok_or(StrategyError::InvalidFinancialValue(
                "arbitrage absolute spread",
            ))?;
        let percent = absolute
            .checked_div(buy.ask().as_decimal())
            .and_then(|value| value.checked_mul(Decimal::ONE_HUNDRED))
            .ok_or(StrategyError::InvalidFinancialValue(
                "arbitrage percentage spread",
            ))?;
        Ok(SpreadQuote {
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
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Numeric controls for the pure segmented-arbitrage state machine.
///
/// This type intentionally carries no operator exchange or symbol scope.
/// Runtime/configuration callers must construct [`ArbitrageStrategy`] through
/// its checked `TryFrom<&crypto_trading_config::ArbitrageConfig>` conversion.
pub struct SegmentedArbitrageConfig {
    pub initial_spread_percent: Decimal,
    pub grid_step_percent: Decimal,
    pub max_segments: u32,
    pub base_quantity: Quantity,
    /// Ratio of T1 used as T0, the first segment's close threshold.
    pub first_close_ratio: Decimal,
}

/// Constructs an operator-scoped strategy after validating every execution
/// control on the source configuration.
impl TryFrom<&crypto_trading_config::ArbitrageConfig> for ArbitrageStrategy {
    type Error = StrategyError;

    fn try_from(config: &crypto_trading_config::ArbitrageConfig) -> Result<Self, Self::Error> {
        config.validate_execution_controls().map_err(|_| {
            StrategyError::InvalidConfig("arbitrage execution controls deny strategy construction")
        })?;

        let numeric_config = SegmentedArbitrageConfig {
            initial_spread_percent: config.min_spread_pct,
            grid_step_percent: config.grid_step_pct,
            max_segments: config.max_segments,
            base_quantity: config.base_quantity,
            first_close_ratio: config.first_close_ratio,
        };
        let mut strategy = Self::new(numeric_config)?;
        strategy.operator_scope = Some(ArbitrageOperatorScope {
            exchanges: config.exchanges.clone(),
            symbols: config.symbols.clone(),
        });
        Ok(strategy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbitrageDirection {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_symbol: Symbol,
    pub sell_symbol: Symbol,
    pub buy_market_type: MarketType,
    pub sell_market_type: MarketType,
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
    operator_scope: Option<ArbitrageOperatorScope>,
}

#[derive(Debug, Clone)]
struct ArbitrageOperatorScope {
    exchanges: Vec<String>,
    symbols: Vec<Symbol>,
}

impl ArbitrageStrategy {
    pub fn symbols_share_hedge_identity(left: &Symbol, right: &Symbol) -> bool {
        if left == right {
            return true;
        }

        match (
            canonical_hedge_symbol_parts(left),
            canonical_hedge_symbol_parts(right),
        ) {
            (
                Some((left_base, left_quote, left_product)),
                Some((right_base, right_quote, right_product)),
            ) => {
                left_base == right_base
                    && left_quote == right_quote
                    && left_product != right_product
            }
            _ => false,
        }
    }

    /// Validates segmented thresholds and constructs a scope-free pure
    /// arbitrage strategy.
    ///
    /// This constructor is intended for deterministic strategy evaluation and
    /// does not enforce operator exchange or symbol allowlists. Callers using
    /// an [`crypto_trading_config::ArbitrageConfig`] must use the checked
    /// `TryFrom` conversion instead.
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
        if config.max_segments > MAX_ARBITRAGE_SEGMENTS {
            return Err(StrategyError::InvalidConfig(
                "arbitrage segment count exceeds the business limit",
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

        let segment_count = config.max_segments as usize;
        let mut open_thresholds = Vec::new();
        open_thresholds
            .try_reserve_exact(segment_count)
            .map_err(|_| StrategyError::InvalidConfig("arbitrage threshold allocation failed"))?;
        for offset in 0..config.max_segments {
            let increment = config
                .grid_step_percent
                .checked_mul(Decimal::from(offset))
                .ok_or(StrategyError::InvalidFinancialValue(
                    "arbitrage open threshold increment",
                ))?;
            let threshold = config.initial_spread_percent.checked_add(increment).ok_or(
                StrategyError::InvalidFinancialValue("arbitrage open threshold"),
            )?;
            open_thresholds.push(threshold);
        }
        let mut close_thresholds = Vec::new();
        close_thresholds
            .try_reserve_exact(segment_count)
            .map_err(|_| StrategyError::InvalidConfig("arbitrage threshold allocation failed"))?;
        close_thresholds.push(
            config
                .initial_spread_percent
                .checked_mul(config.first_close_ratio)
                .ok_or(StrategyError::InvalidFinancialValue(
                    "arbitrage first close threshold",
                ))?,
        );
        close_thresholds.extend_from_slice(&open_thresholds[..segment_count - 1]);

        Ok(Self {
            config,
            open_thresholds,
            close_thresholds,
            operator_scope: None,
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

    fn validate_operator_scope(
        &self,
        left: &MarketSnapshot,
        right: &MarketSnapshot,
    ) -> Result<(), StrategyError> {
        for snapshot in [left, right] {
            if !symbol_market_type_matches_suffix(&snapshot.symbol, snapshot.market_type) {
                return Err(StrategyError::SnapshotMismatch(format!(
                    "symbol {} does not match market type {:?}",
                    snapshot.symbol, snapshot.market_type
                )));
            }
        }
        if !Self::symbols_share_hedge_identity(&left.symbol, &right.symbol) {
            return Err(StrategyError::SnapshotMismatch(
                "arbitrage legs do not share a hedge identity".to_owned(),
            ));
        }

        let Some(scope) = &self.operator_scope else {
            return Ok(());
        };

        for snapshot in [left, right] {
            if !scope
                .exchanges
                .iter()
                .any(|exchange| exchange == snapshot.exchange())
            {
                return Err(StrategyError::SnapshotMismatch(format!(
                    "exchange {} is outside the configured arbitrage allowlist",
                    snapshot.exchange()
                )));
            }
            if !scope.symbols.contains(&snapshot.symbol) {
                return Err(StrategyError::SnapshotMismatch(format!(
                    "symbol {} is outside the configured arbitrage allowlist",
                    snapshot.symbol
                )));
            }
        }
        Ok(())
    }

    fn count_thresholds(value: Decimal, thresholds: &[Decimal]) -> u32 {
        thresholds
            .iter()
            .rposition(|threshold| value >= *threshold)
            .map_or(0, |index| u32::try_from(index + 1).unwrap_or(u32::MAX))
    }

    fn current_segments(&self, position_quantity: Decimal) -> Result<u32, StrategyError> {
        if position_quantity <= Decimal::ZERO {
            return Ok(0);
        }
        let ratio = position_quantity
            .checked_div(self.config.base_quantity.as_decimal())
            .ok_or(StrategyError::InvalidFinancialValue(
                "arbitrage current segment count",
            ))?;
        Ok(ratio
            .ceil()
            .to_u32()
            .unwrap_or(self.config.max_segments)
            .min(self.config.max_segments))
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
                    && quote.buy_market_type == direction.buy_market_type
                    && quote.sell_market_type == direction.sell_market_type
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
        let original_buy = Self::matching_snapshot(
            &quote.buy_exchange,
            &quote.buy_symbol,
            quote.buy_market_type,
            left,
            right,
        )?;
        let original_sell = Self::matching_snapshot(
            &quote.sell_exchange,
            &quote.sell_symbol,
            quote.sell_market_type,
            left,
            right,
        )?;

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
        market_type: MarketType,
        left: &'a MarketSnapshot,
        right: &'a MarketSnapshot,
    ) -> Result<&'a MarketSnapshot, StrategyError> {
        [left, right]
            .into_iter()
            .find(|snapshot| {
                snapshot.exchange() == exchange
                    && snapshot.symbol == *symbol
                    && snapshot.market_type == market_type
            })
            .ok_or_else(|| {
                StrategyError::SnapshotMismatch(format!(
                    "missing {exchange}/{symbol}/{market_type:?} from arbitrage pair"
                ))
            })
    }
}

fn canonical_hedge_symbol_parts(symbol: &Symbol) -> Option<(&str, &str, &str)> {
    let mut parts = symbol.as_str().split('-');
    let (Some(base), Some(quote), Some(product), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    if base.is_empty() || quote.is_empty() || !matches!(product, "SPOT" | "PERP") {
        return None;
    }
    Some((base, quote, product))
}

fn symbol_market_type_matches_suffix(symbol: &Symbol, market_type: MarketType) -> bool {
    match canonical_hedge_symbol_parts(symbol) {
        Some((_, _, "SPOT")) => market_type == MarketType::Spot,
        Some((_, _, "PERP")) => market_type == MarketType::Perpetual,
        Some(_) | None => true,
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
        self.validate_operator_scope(left, right)?;

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

        let quote = match (state.position_quantity.is_zero(), &state.direction) {
            (false, Some(direction)) => Self::quote_for_direction(direction, left, right)?,
            (true, _) | (false, None) => SpreadCalculator::best(left, right)?,
        };
        let current_segments = self.current_segments(state.position_quantity)?;
        let open_segments = Self::count_thresholds(quote.percent, &self.open_thresholds);
        let keep_segments =
            Self::count_thresholds(quote.percent, &self.close_thresholds).min(current_segments);
        let target_segments = if open_segments > current_segments {
            open_segments
        } else {
            keep_segments
        };
        let target_quantity = self
            .config
            .base_quantity
            .as_decimal()
            .checked_mul(Decimal::from(target_segments))
            .ok_or(StrategyError::InvalidFinancialValue(
                "arbitrage target quantity",
            ))?;

        let (kind, delta_quantity, intents) = match target_quantity.cmp(&state.position_quantity) {
            std::cmp::Ordering::Greater => {
                let delta = target_quantity.checked_sub(state.position_quantity).ok_or(
                    StrategyError::InvalidFinancialValue("arbitrage opening quantity"),
                )?;
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
                let delta = state.position_quantity.checked_sub(target_quantity).ok_or(
                    StrategyError::InvalidFinancialValue("arbitrage closing quantity"),
                )?;
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
        let direction = if target_quantity.is_zero() {
            None
        } else if state.position_quantity.is_zero() {
            Some(quote.direction())
        } else {
            state.direction.clone().or_else(|| Some(quote.direction()))
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
