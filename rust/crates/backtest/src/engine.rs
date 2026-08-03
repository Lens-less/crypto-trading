use chrono::{DateTime, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, OrderType, Price, Quantity, Side, Symbol,
};
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
    /// Mark used for equity valuation. Execution uses the opposing top of book.
    pub price: Price,
    pub bid: Price,
    pub ask: Price,
    instrument: Option<TapeInstrument>,
}

impl MarketEvent {
    #[must_use]
    pub const fn new(occurred_at: DateTime<Utc>, price: Price) -> Self {
        Self {
            occurred_at,
            price,
            bid: price,
            ask: price,
            instrument: None,
        }
    }

    #[must_use]
    pub fn from_snapshot(snapshot: &MarketSnapshot, price_source: MarketEventPrice) -> Self {
        Self {
            occurred_at: snapshot.timestamp,
            price: snapshot_price(snapshot, price_source),
            bid: snapshot.bid(),
            ask: snapshot.ask(),
            instrument: Some(TapeInstrument::from_snapshot(snapshot)),
        }
    }

    /// Returns the exact venue instrument carried by production snapshots.
    #[must_use]
    pub const fn instrument(&self) -> Option<&TapeInstrument> {
        self.instrument.as_ref()
    }
}

/// Exact identity shared by every event in a production-snapshot tape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapeInstrument {
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
}

impl TapeInstrument {
    fn from_snapshot(snapshot: &MarketSnapshot) -> Self {
        Self {
            exchange: snapshot.exchange().to_owned(),
            symbol: snapshot.symbol.clone(),
            market_type: snapshot.market_type,
        }
    }
}

/// Immutable sequence of ordered market events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTape {
    events: Vec<MarketEvent>,
    instrument: Option<TapeInstrument>,
}

impl EventTape {
    /// Creates a tape and validates non-decreasing timestamps.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::NonMonotonicTape`] when timestamps move
    /// backwards, or [`BacktestError::MixedInstrumentTape`] when event
    /// identities differ.
    pub fn new(events: Vec<MarketEvent>) -> Result<Self, BacktestError> {
        if events
            .windows(2)
            .any(|pair| pair[1].occurred_at < pair[0].occurred_at)
        {
            return Err(BacktestError::NonMonotonicTape);
        }

        let instrument = events.first().and_then(|event| event.instrument().cloned());
        if events
            .iter()
            .any(|event| event.instrument() != instrument.as_ref())
        {
            return Err(BacktestError::MixedInstrumentTape);
        }

        Ok(Self { events, instrument })
    }

    /// Adapts production snapshots into a validated deterministic tape.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::NonMonotonicTape`] when timestamps move
    /// backwards, or [`BacktestError::MixedInstrumentTape`] when snapshot
    /// identities differ.
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

    /// Returns the exact instrument for a tape built from production snapshots.
    #[must_use]
    pub const fn instrument(&self) -> Option<&TapeInstrument> {
        self.instrument.as_ref()
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
    pub instrument: Option<TapeInstrument>,
}

impl OrderRequest {
    /// Creates an order request.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidQuantity`] when `quantity <= 0`, or
    /// [`BacktestError::UnsupportedMakerLiquidity`] until a resting-order
    /// model exists.
    pub fn new(
        side: Side,
        quantity: Quantity,
        liquidity: Liquidity,
    ) -> Result<Self, BacktestError> {
        if quantity.as_decimal() <= Decimal::ZERO {
            return Err(BacktestError::InvalidQuantity);
        }
        if liquidity == Liquidity::Maker {
            return Err(BacktestError::UnsupportedMakerLiquidity);
        }

        Ok(Self {
            side,
            quantity,
            liquidity,
            instrument: None,
        })
    }

    /// Adapts one production order intent into the deterministic execution
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::UnsupportedOrderIntent`] for non-market
    /// intents, or [`BacktestError::UnsupportedMakerLiquidity`] for maker
    /// requests, because the current fill model does not simulate
    /// resting-book behavior.
    pub fn from_order_intent(
        intent: &OrderIntent,
        liquidity: Liquidity,
    ) -> Result<Self, BacktestError> {
        if intent.order_type != OrderType::Market {
            return Err(BacktestError::UnsupportedOrderIntent);
        }

        let mut request = Self::new(intent.side, intent.quantity, liquidity)?;
        request.instrument = Some(TapeInstrument {
            exchange: intent.exchange.clone(),
            symbol: intent.symbol.clone(),
            market_type: intent.market_type,
        });
        Ok(request)
    }
}

/// Adapts a batch of production order intents into deterministic requests.
///
/// # Errors
///
/// Returns [`BacktestError::UnsupportedOrderIntent`] when any intent is not a
/// market order, or [`BacktestError::UnsupportedMakerLiquidity`] when maker
/// liquidity is requested.
pub fn adapt_order_intents(
    intents: &[OrderIntent],
    liquidity: Liquidity,
) -> Result<Vec<OrderRequest>, BacktestError> {
    intents
        .iter()
        .map(|intent| OrderRequest::from_order_intent(intent, liquidity))
        .collect()
}

/// Explicit fee and slippage assumptions expressed in basis points.
///
/// Maker inputs remain part of the configuration contract, but maker requests
/// fail closed until the engine has a resting-order model.
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
    /// Returns [`BacktestError::UnsupportedMakerLiquidity`] for maker
    /// requests, [`BacktestError::OrderInstrumentMismatch`] when an identified
    /// order and event disagree, and arithmetic or domain errors when the fill
    /// price or fee cannot be represented.
    pub fn fill(
        &self,
        event: &MarketEvent,
        order: &OrderRequest,
    ) -> Result<TradeFill, BacktestError> {
        if order.liquidity == Liquidity::Maker {
            return Err(BacktestError::UnsupportedMakerLiquidity);
        }
        if let (Some(event_instrument), Some(order_instrument)) =
            (event.instrument(), order.instrument.as_ref())
            && event_instrument != order_instrument
        {
            return Err(BacktestError::OrderInstrumentMismatch);
        }
        let (fee_bps, slippage_bps) = match order.liquidity {
            Liquidity::Maker => (self.maker_fee, self.maker_slippage),
            Liquidity::Taker => (self.taker_fee, self.taker_slippage),
        };
        let slippage_factor = slippage_bps
            .checked_div(decimal_bps_denominator())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let reference_price = match order.side {
            Side::Buy => event.ask,
            Side::Sell => event.bid,
        };
        let fill_price = match order.side {
            Side::Buy => reference_price
                .as_decimal()
                .checked_mul(
                    Decimal::ONE
                        .checked_add(slippage_factor)
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
            Side::Sell => reference_price
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
            reference_price,
            fill_price,
            fee,
            instrument: event
                .instrument()
                .cloned()
                .or_else(|| order.instrument.clone()),
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
    pub instrument: Option<TapeInstrument>,
}

/// Executed trade plus the resulting ledger state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub fill: TradeFill,
    pub realized_pnl_delta: Money,
    /// Net `PnL` for quantity closed by this fill, including its proportional
    /// entry and exit fees. Opening-only fills are `None`.
    pub closed_trade_pnl: Option<Money>,
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

/// Pure research-only strategy seam.
///
/// This is not the production `crypto_trading_strategy::StrategyMachine`
/// interface. Production parity remains unsupported until both drivers share
/// one state and execution-feedback contract.
pub trait Strategy {
    fn on_event(&mut self, context: &StrategyContext) -> Vec<OrderRequest>;
}

/// Summary metrics attached to a finished backtest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestMetrics {
    pub realized_pnl: Money,
    pub unrealized_pnl: Money,
    pub ending_equity: Money,
    /// Observation frequency used to annualize risk ratios. `None` when the
    /// tape is too short to derive a period.
    pub periods_per_year: Option<Decimal>,
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
    ratio_config: Option<RatioConfig>,
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
            ratio_config: None,
        })
    }

    /// Overrides ratio annualization assumptions for performance metrics.
    #[must_use]
    pub const fn with_ratio_config(mut self, ratio_config: RatioConfig) -> Self {
        self.ratio_config = Some(ratio_config);
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
        let mut closed_trade_pnls = Vec::new();
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
                if let Some(closed_trade_pnl) = applied.closed_trade_pnl {
                    closed_trade_pnls.push(closed_trade_pnl.as_decimal());
                }
                trades.push(Trade {
                    fill,
                    realized_pnl_delta: applied.realized_pnl_delta,
                    closed_trade_pnl: applied.closed_trade_pnl,
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
        let equity_values: Vec<Decimal> = equity_curve
            .iter()
            .map(|point| point.equity.as_decimal())
            .collect();
        let returns = equity_returns(&equity_values)?;
        let (ratio_config, periods_per_year) = self.ratio_config_for(tape)?;
        let ratio_returns = if periods_per_year.is_some() {
            returns.as_deref().unwrap_or_default()
        } else {
            &[]
        };
        let performance = summarize_performance(
            &equity_values,
            &closed_trade_pnls,
            ratio_returns,
            ratio_config,
        )?;

        Ok(BacktestResult {
            trades,
            equity_curve,
            metrics: BacktestMetrics {
                realized_pnl: terminal.realized_pnl,
                unrealized_pnl: terminal.unrealized_pnl,
                ending_equity: terminal.equity,
                periods_per_year,
                performance,
            },
        })
    }

    fn ratio_config_for(
        &self,
        tape: &EventTape,
    ) -> Result<(RatioConfig, Option<Decimal>), BacktestError> {
        if let Some(config) = self.ratio_config {
            return Ok((config, Some(config.periods_per_year)));
        }
        let Some((first, last)) = tape.events().first().zip(tape.events().last()) else {
            return Ok((RatioConfig::new(Decimal::ONE, Decimal::ZERO)?, None));
        };
        let periods = tape.events().len().saturating_sub(1);
        if periods == 0 {
            return Ok((RatioConfig::new(Decimal::ONE, Decimal::ZERO)?, None));
        }
        let elapsed_nanos = last
            .occurred_at
            .signed_duration_since(first.occurred_at)
            .num_nanoseconds()
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if elapsed_nanos < 0 {
            return Err(BacktestError::NonMonotonicTape);
        }
        if elapsed_nanos == 0 {
            return Ok((RatioConfig::new(Decimal::ONE, Decimal::ZERO)?, None));
        }
        let periods =
            Decimal::from(u64::try_from(periods).map_err(|_| BacktestError::ArithmeticOverflow)?);
        let nanos_per_year = Decimal::from(31_536_000_000_000_000_i64);
        let periods_per_year = periods
            .checked_mul(nanos_per_year)
            .and_then(|value| value.checked_div(Decimal::from(elapsed_nanos)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let config = RatioConfig::new(periods_per_year, Decimal::ZERO)?;
        Ok((config, Some(periods_per_year)))
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

fn equity_returns(equity_curve: &[Decimal]) -> Result<Option<Vec<Decimal>>, BacktestError> {
    if equity_curve.windows(2).any(|pair| pair[0] <= Decimal::ZERO) {
        return Ok(None);
    }
    equity_curve
        .windows(2)
        .map(|pair| {
            pair[1]
                .checked_sub(pair[0])
                .ok_or(BacktestError::ArithmeticOverflow)?
                .checked_div(pair[0])
                .ok_or(BacktestError::ArithmeticOverflow)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

const fn decimal_bps_denominator() -> Decimal {
    Decimal::from_parts(10_000, 0, 0, false, 0)
}
