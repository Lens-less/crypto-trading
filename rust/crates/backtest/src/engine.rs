use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketSnapshot, Money, OrderIntent, OrderType, Price, Quantity, Side};
use crypto_trading_indicators::{PerformanceMetrics, RatioConfig, summarize_performance};
use rust_decimal::Decimal;

use crate::{BacktestError, ledger::Ledger};

/// Which production quote field should become the deterministic backtest mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketEventPrice {
    LastOrMid,
    Mid,
    Bid,
    Ask,
}

impl Default for MarketEventPrice {
    fn default() -> Self {
        Self::LastOrMid
    }
}

/// Deterministic market data event consumed by a backtest tape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketEvent {
    pub occurred_at: DateTime<Utc>,
    pub price: Price,
}

impl MarketEvent {
    #[must_use]
    pub const fn new(occurred_at: DateTime<Utc>, price: Price) -> Self {
        Self { occurred_at, price }
    }

    #[must_use]
    pub fn from_snapshot(snapshot: &MarketSnapshot, price_source: MarketEventPrice) -> Self {
        Self {
            occurred_at: snapshot.timestamp,
            price: snapshot_price(snapshot, price_source),
        }
    }
}

/// Immutable sequence of ordered market events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTape {
    events: Vec<MarketEvent>,
}

impl EventTape {
    /// Creates a tape and validates monotonic timestamps.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::NonMonotonicTape`] when timestamps move
    /// backwards.
    pub fn new(events: Vec<MarketEvent>) -> Result<Self, BacktestError> {
        if events
            .windows(2)
            .any(|pair| pair[1].occurred_at < pair[0].occurred_at)
        {
            return Err(BacktestError::NonMonotonicTape);
        }

        Ok(Self { events })
    }

    /// Adapts production snapshots into a validated deterministic tape.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::NonMonotonicTape`] when timestamps move
    /// backwards.
    pub fn from_market_snapshots(
        snapshots: &[MarketSnapshot],
        price_source: MarketEventPrice,
    ) -> Result<Self, BacktestError> {
        Self::new(
            snapshots
                .iter()
                .map(|snapshot| MarketEvent::from_snapshot(snapshot, price_source))
                .collect(),
        )
    }

    /// Returns all tape events.
    #[must_use]
    pub fn events(&self) -> &[MarketEvent] {
        &self.events
    }
}

/// Deterministic simulation clock that can only advance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimClock {
    now: Option<DateTime<Utc>>,
}

impl SimClock {
    /// Advances the clock to `timestamp`.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::NonMonotonicTape`] when `timestamp` goes
    /// backwards.
    pub fn advance_to(&mut self, timestamp: DateTime<Utc>) -> Result<(), BacktestError> {
        if self.now.is_some_and(|current| timestamp < current) {
            return Err(BacktestError::NonMonotonicTape);
        }

        self.now = Some(timestamp);
        Ok(())
    }

    /// Returns the current simulation time.
    #[must_use]
    pub const fn now(&self) -> Option<DateTime<Utc>> {
        self.now
    }
}

/// Liquidity assumption applied by the fill model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liquidity {
    Maker,
    Taker,
}

/// Pure order request emitted by a strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequest {
    pub side: Side,
    pub quantity: Quantity,
    pub liquidity: Liquidity,
}

impl OrderRequest {
    /// Creates an order request.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidQuantity`] when `quantity <= 0`.
    pub fn new(
        side: Side,
        quantity: Quantity,
        liquidity: Liquidity,
    ) -> Result<Self, BacktestError> {
        if quantity.as_decimal() <= Decimal::ZERO {
            return Err(BacktestError::InvalidQuantity);
        }

        Ok(Self {
            side,
            quantity,
            liquidity,
        })
    }

    /// Adapts one production order intent into the deterministic execution
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::UnsupportedOrderIntent`] for non-market
    /// intents because the current fill model does not simulate resting-book
    /// behavior.
    pub fn from_order_intent(
        intent: &OrderIntent,
        liquidity: Liquidity,
    ) -> Result<Self, BacktestError> {
        if intent.order_type != OrderType::Market {
            return Err(BacktestError::UnsupportedOrderIntent);
        }

        Self::new(intent.side, intent.quantity, liquidity)
    }
}

/// Adapts a batch of production order intents into deterministic requests.
///
/// # Errors
///
/// Returns [`BacktestError::UnsupportedOrderIntent`] when any intent is not a
/// market order.
pub fn adapt_order_intents(
    intents: &[OrderIntent],
    liquidity: Liquidity,
) -> Result<Vec<OrderRequest>, BacktestError> {
    intents
        .iter()
        .map(|intent| OrderRequest::from_order_intent(intent, liquidity))
        .collect()
}

/// Explicit maker/taker fill assumptions expressed in basis points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillModel {
    maker_fee: Decimal,
    taker_fee: Decimal,
    maker_slippage: Decimal,
    taker_slippage: Decimal,
}

impl FillModel {
    /// Creates a fill model.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::NegativeBasisPoints`] when any input is
    /// negative, or [`BacktestError::InvalidSlippageBasisPoints`] when sell
    /// slippage could produce a non-positive fill price.
    pub fn new(
        maker_fee_bps: Decimal,
        taker_fee_bps: Decimal,
        maker_slippage_bps: Decimal,
        taker_slippage_bps: Decimal,
    ) -> Result<Self, BacktestError> {
        for value in [
            maker_fee_bps,
            taker_fee_bps,
            maker_slippage_bps,
            taker_slippage_bps,
        ] {
            if value < Decimal::ZERO {
                return Err(BacktestError::NegativeBasisPoints);
            }
        }
        if maker_slippage_bps >= decimal_bps_denominator()
            || taker_slippage_bps >= decimal_bps_denominator()
        {
            return Err(BacktestError::InvalidSlippageBasisPoints);
        }

        Ok(Self {
            maker_fee: maker_fee_bps,
            taker_fee: taker_fee_bps,
            maker_slippage: maker_slippage_bps,
            taker_slippage: taker_slippage_bps,
        })
    }

    /// Applies fill assumptions to a strategy order at the current event price.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] on Decimal overflow.
    pub fn fill(
        &self,
        event: &MarketEvent,
        order: &OrderRequest,
    ) -> Result<TradeFill, BacktestError> {
        let (fee_bps, slippage_bps) = match order.liquidity {
            Liquidity::Maker => (self.maker_fee, self.maker_slippage),
            Liquidity::Taker => (self.taker_fee, self.taker_slippage),
        };
        let slippage_factor = slippage_bps
            .checked_div(decimal_bps_denominator())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let fill_price = match order.side {
            Side::Buy => event
                .price
                .as_decimal()
                .checked_mul(
                    Decimal::ONE
                        .checked_add(slippage_factor)
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
            Side::Sell => event
                .price
                .as_decimal()
                .checked_mul(
                    Decimal::ONE
                        .checked_sub(slippage_factor)
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
        };
        let fill_price = Price::new(fill_price)?;
        let notional = notional(fill_price, order.quantity)?;
        let fee = Money::new(
            notional
                .as_decimal()
                .checked_mul(
                    fee_bps
                        .checked_div(decimal_bps_denominator())
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );

        Ok(TradeFill {
            occurred_at: event.occurred_at,
            side: order.side,
            quantity: order.quantity,
            liquidity: order.liquidity,
            reference_price: event.price,
            fill_price,
            fee,
        })
    }
}

/// Fill details before the ledger attaches `PnL` state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeFill {
    pub occurred_at: DateTime<Utc>,
    pub side: Side,
    pub quantity: Quantity,
    pub liquidity: Liquidity,
    pub reference_price: Price,
    pub fill_price: Price,
    pub fee: Money,
}

/// Executed trade plus the resulting ledger state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub fill: TradeFill,
    pub realized_pnl_delta: Money,
    pub cumulative_realized_pnl: Money,
    pub position_qty: Decimal,
    pub equity: Money,
}

/// Point on the mark-to-market equity curve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EquityPoint {
    pub occurred_at: DateTime<Utc>,
    pub price: Price,
    pub equity: Money,
}

/// Read-only strategy context for the current event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyContext {
    pub now: DateTime<Utc>,
    pub event: MarketEvent,
    pub ledger: crate::LedgerSnapshot,
}

/// Pure strategy seam.
pub trait Strategy {
    fn on_event(&mut self, context: &StrategyContext) -> Vec<OrderRequest>;
}

/// Summary metrics attached to a finished backtest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestMetrics {
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub ending_equity: Money,
    pub performance: PerformanceMetrics,
}

/// Deterministic backtest output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestResult {
    pub trades: Vec<Trade>,
    pub equity_curve: Vec<EquityPoint>,
    pub metrics: BacktestMetrics,
}

/// Minimal single-instrument backtest engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestEngine {
    initial_cash: Money,
    fill_model: FillModel,
    ratio_config: RatioConfig,
}

impl BacktestEngine {
    /// Creates a backtest engine.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidInitialCash`] when `initial_cash < 0`.
    pub fn new(initial_cash: Money, fill_model: FillModel) -> Result<Self, BacktestError> {
        if initial_cash.as_decimal() < Decimal::ZERO {
            return Err(BacktestError::InvalidInitialCash);
        }

        Ok(Self {
            initial_cash,
            fill_model,
            ratio_config: RatioConfig::default(),
        })
    }

    /// Overrides ratio annualization assumptions for performance metrics.
    #[must_use]
    pub const fn with_ratio_config(mut self, ratio_config: RatioConfig) -> Self {
        self.ratio_config = ratio_config;
        self
    }

    /// Runs a strategy over a validated event tape.
    ///
    /// # Errors
    ///
    /// Returns arithmetic and validation errors propagated from the fill model
    /// or ledger.
    pub fn run<S: Strategy>(
        &self,
        tape: &EventTape,
        strategy: &mut S,
    ) -> Result<BacktestResult, BacktestError> {
        let mut clock = SimClock::default();
        let mut ledger = Ledger::new(self.initial_cash)?;
        let mut trades = Vec::new();
        let mut equity_curve = Vec::with_capacity(tape.events().len());

        for event in tape.events() {
            clock.advance_to(event.occurred_at)?;
            let context = StrategyContext {
                now: event.occurred_at,
                event: event.clone(),
                ledger: ledger.snapshot(event.price)?,
            };
            for order in strategy.on_event(&context) {
                let fill = self.fill_model.fill(event, &order)?;
                let applied = ledger.apply_fill(&fill)?;
                let marked = ledger.snapshot(event.price)?;
                trades.push(Trade {
                    fill,
                    realized_pnl_delta: applied.realized_pnl_delta,
                    cumulative_realized_pnl: marked.realized_pnl,
                    position_qty: marked.position_qty,
                    equity: marked.equity,
                });
            }

            let marked = ledger.snapshot(event.price)?;
            equity_curve.push(EquityPoint {
                occurred_at: event.occurred_at,
                price: event.price,
                equity: marked.equity,
            });
        }

        let terminal = match tape.events().last() {
            Some(event) => ledger.snapshot(event.price)?,
            None => ledger.snapshot(Price::new(Decimal::ONE)?)?,
        };
        let trade_pnls: Vec<Decimal> = trades
            .iter()
            .map(|trade| trade.realized_pnl_delta.as_decimal())
            .collect();
        let equity_values: Vec<Decimal> = equity_curve
            .iter()
            .map(|point| point.equity.as_decimal())
            .collect();
        let returns = equity_returns(&equity_values)?;
        let performance =
            summarize_performance(&equity_values, &trade_pnls, &returns, self.ratio_config)
                .map_err(|_| BacktestError::ArithmeticOverflow)?;

        Ok(BacktestResult {
            trades,
            equity_curve,
            metrics: BacktestMetrics {
                realized_pnl: terminal.realized_pnl,
                unrealized_pnl: terminal.unrealized_pnl,
                ending_equity: terminal.equity,
                performance,
            },
        })
    }
}

fn snapshot_price(snapshot: &MarketSnapshot, price_source: MarketEventPrice) -> Price {
    match price_source {
        MarketEventPrice::LastOrMid => snapshot.last.unwrap_or_else(|| snapshot.mid_price()),
        MarketEventPrice::Mid => snapshot.mid_price(),
        MarketEventPrice::Bid => snapshot.bid(),
        MarketEventPrice::Ask => snapshot.ask(),
    }
}

fn notional(price: Price, quantity: Quantity) -> Result<Money, BacktestError> {
    Ok(Money::new(
        price
            .as_decimal()
            .checked_mul(quantity.as_decimal())
            .ok_or(BacktestError::ArithmeticOverflow)?,
    ))
}

fn equity_returns(equity_curve: &[Decimal]) -> Result<Vec<Decimal>, BacktestError> {
    equity_curve
        .windows(2)
        .filter(|pair| !pair[0].is_zero())
        .map(|pair| {
            pair[1]
                .checked_sub(pair[0])
                .ok_or(BacktestError::ArithmeticOverflow)?
                .checked_div(pair[0])
                .ok_or(BacktestError::ArithmeticOverflow)
        })
        .collect()
}

const fn decimal_bps_denominator() -> Decimal {
    Decimal::from_parts(10_000, 0, 0, false, 0)
}
