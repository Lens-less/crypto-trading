use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex as StdMutex},
    time::Instant,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, Order, OrderIntent, OrderStatus, OrderType, Position,
    PositionSide, Price, Quantity, Side, Symbol, TimeInForce,
};
use rust_decimal::Decimal;
use tokio::sync::{Mutex, broadcast};

use crate::{
    BoundedExchangeHandle, CancellationDisposition, ExchangeAvailability, ExchangeError,
    ExchangeHandle, ExchangeStatus, InstrumentRuleCatalog, InstrumentRulesMode,
    InstrumentRulesStatus, MarketSubscription, ReconcileReceipt, ReconcileScope,
    SubmissionDisposition, SubscriptionReceipt, TradingCommand, TradingReceipt,
};

const DEFAULT_MAX_MARKET_AGE: chrono::TimeDelta = chrono::TimeDelta::seconds(30);
const DEFAULT_MAX_FUTURE_SKEW: chrono::TimeDelta = chrono::TimeDelta::seconds(1);
const MAX_EVENT_CAPACITY: usize = 65_536;
const MAX_PAPER_SNAPSHOTS: usize = 10_000;
const MAX_PAPER_ORDERS: usize = 100_000;
const MAX_PAPER_POSITIONS: usize = 10_000;

type PaperClock = dyn Fn() -> DateTime<Utc> + Send + Sync;
type PositionIndex = HashMap<(Symbol, MarketType), usize>;

#[derive(Clone, Copy)]
struct PaperFill {
    quantity: Decimal,
    price: Price,
    mark: Price,
    at: DateTime<Utc>,
}

fn advance_clock_high_water(
    observed: &mut DateTime<Utc>,
    monotonic_candidate: DateTime<Utc>,
    wall_candidate: DateTime<Utc>,
) -> DateTime<Utc> {
    *observed = (*observed).max(monotonic_candidate).max(wall_candidate);
    *observed
}

fn anchored_paper_clock() -> impl Fn() -> DateTime<Utc> + Send + Sync + 'static {
    let utc_anchor = Utc::now();
    let monotonic_anchor = Instant::now();
    let high_water = StdMutex::new(utc_anchor);
    move || {
        let monotonic_candidate = match chrono::TimeDelta::from_std(monotonic_anchor.elapsed()) {
            Ok(elapsed) => utc_anchor
                .checked_add_signed(elapsed)
                .unwrap_or(DateTime::<Utc>::MAX_UTC),
            Err(_) => DateTime::<Utc>::MAX_UTC,
        };
        // Wall time accounts for system suspend on platforms whose monotonic
        // clock pauses; taking the high-water mark also fails closed on wall
        // clock rollback.
        let wall_candidate = Utc::now();
        let mut observed = high_water
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        advance_clock_high_water(&mut observed, monotonic_candidate, wall_candidate)
    }
}

/// Conservative, caller-lowerable caps for the in-memory paper ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaperLedgerLimits {
    snapshots: usize,
    orders: usize,
    positions: usize,
}

impl PaperLedgerLimits {
    /// Builds caller-lowered paper ledger caps.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] when a cap exceeds its hard maximum.
    pub fn new(
        max_snapshots: NonZeroUsize,
        max_orders: NonZeroUsize,
        max_positions: NonZeroUsize,
    ) -> Result<Self, ExchangeError> {
        for (resource, requested, hard_limit) in [
            (
                "paper snapshot ledger",
                max_snapshots.get(),
                MAX_PAPER_SNAPSHOTS,
            ),
            ("paper order ledger", max_orders.get(), MAX_PAPER_ORDERS),
            (
                "paper position ledger",
                max_positions.get(),
                MAX_PAPER_POSITIONS,
            ),
        ] {
            if requested > hard_limit {
                return Err(ExchangeError::resource_limit(
                    resource, hard_limit, requested,
                ));
            }
        }
        Ok(Self {
            snapshots: max_snapshots.get(),
            orders: max_orders.get(),
            positions: max_positions.get(),
        })
    }

    pub const fn max_snapshots(self) -> usize {
        self.snapshots
    }

    pub const fn max_orders(self) -> usize {
        self.orders
    }

    pub const fn max_positions(self) -> usize {
        self.positions
    }
}

impl Default for PaperLedgerLimits {
    fn default() -> Self {
        Self {
            snapshots: MAX_PAPER_SNAPSHOTS,
            orders: MAX_PAPER_ORDERS,
            positions: MAX_PAPER_POSITIONS,
        }
    }
}

/// Deterministic in-memory exchange used for paper execution and contract tests.
#[derive(Clone)]
pub struct PaperExchange {
    exchange: Arc<str>,
    state: Arc<Mutex<PaperState>>,
    market_sender: broadcast::Sender<MarketSnapshot>,
    clock: Arc<PaperClock>,
    max_market_age: chrono::TimeDelta,
    max_future_skew: chrono::TimeDelta,
    instrument_rules: Arc<InstrumentRuleCatalog>,
    instrument_rules_mode: InstrumentRulesMode,
    ledger_limits: PaperLedgerLimits,
}

#[derive(Clone)]
struct PaperState {
    snapshots: Vec<MarketSnapshot>,
    orders: Vec<Order>,
    positions: Vec<Position>,
    next_order_id: u64,
    next_subscription_id: u64,
    observed_at: DateTime<Utc>,
}

impl PaperState {
    fn new(observed_at: DateTime<Utc>) -> Self {
        Self {
            snapshots: Vec::new(),
            orders: Vec::new(),
            positions: Vec::new(),
            next_order_id: 1,
            next_subscription_id: 1,
            observed_at,
        }
    }
}

impl PaperExchange {
    /// Creates a paper adapter whose event stream retains at most `event_capacity` snapshots.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when the exchange name is empty.
    pub fn new(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
    ) -> Result<Self, ExchangeError> {
        Self::with_clock_and_freshness(
            exchange,
            event_capacity,
            anchored_paper_clock(),
            DEFAULT_MAX_MARKET_AGE,
            DEFAULT_MAX_FUTURE_SKEW,
        )
    }

    /// Creates a paper adapter with an injected clock and safe default quote freshness.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid exchange, freshness, or capacity configuration.
    pub fn with_clock<F>(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
        clock: F,
    ) -> Result<Self, ExchangeError>
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        Self::with_clock_and_freshness(
            exchange,
            event_capacity,
            clock,
            DEFAULT_MAX_MARKET_AGE,
            DEFAULT_MAX_FUTURE_SKEW,
        )
    }

    /// Creates a paper adapter with an injected clock and explicit quote freshness bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid exchange, freshness, or capacity configuration.
    pub fn with_clock_and_freshness<F>(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
        clock: F,
        max_market_age: chrono::TimeDelta,
        max_future_skew: chrono::TimeDelta,
    ) -> Result<Self, ExchangeError>
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        Self::with_clock_freshness_and_rules(
            exchange,
            event_capacity,
            clock,
            max_market_age,
            max_future_skew,
            InstrumentRulesMode::Permissive,
            InstrumentRuleCatalog::default(),
        )
    }

    /// Creates a paper adapter with an adapter-owned instrument-rule catalog.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid exchange or capacity configuration.
    pub fn with_instrument_rules(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
        instrument_rules_mode: InstrumentRulesMode,
        instrument_rules: InstrumentRuleCatalog,
    ) -> Result<Self, ExchangeError> {
        Self::with_clock_freshness_and_rules(
            exchange,
            event_capacity,
            anchored_paper_clock(),
            DEFAULT_MAX_MARKET_AGE,
            DEFAULT_MAX_FUTURE_SKEW,
            instrument_rules_mode,
            instrument_rules,
        )
    }

    /// Creates a fully configurable paper adapter for deterministic simulation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid exchange, freshness, or capacity configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn with_clock_freshness_and_rules<F>(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
        clock: F,
        max_market_age: chrono::TimeDelta,
        max_future_skew: chrono::TimeDelta,
        instrument_rules_mode: InstrumentRulesMode,
        instrument_rules: InstrumentRuleCatalog,
    ) -> Result<Self, ExchangeError>
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        Self::with_clock_freshness_rules_and_limits(
            exchange,
            event_capacity,
            clock,
            max_market_age,
            max_future_skew,
            instrument_rules_mode,
            instrument_rules,
            PaperLedgerLimits::default(),
        )
    }

    /// Creates a paper adapter with lower custom ledger caps.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid exchange or capacity configuration.
    pub fn with_ledger_limits(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
        ledger_limits: PaperLedgerLimits,
    ) -> Result<Self, ExchangeError> {
        Self::with_clock_freshness_rules_and_limits(
            exchange,
            event_capacity,
            anchored_paper_clock(),
            DEFAULT_MAX_MARKET_AGE,
            DEFAULT_MAX_FUTURE_SKEW,
            InstrumentRulesMode::Permissive,
            InstrumentRuleCatalog::default(),
            ledger_limits,
        )
    }

    /// Creates a fully configurable paper adapter including lower ledger caps.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid exchange, freshness, or capacity configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn with_clock_freshness_rules_and_limits<F>(
        exchange: impl Into<String>,
        event_capacity: NonZeroUsize,
        clock: F,
        max_market_age: chrono::TimeDelta,
        max_future_skew: chrono::TimeDelta,
        instrument_rules_mode: InstrumentRulesMode,
        instrument_rules: InstrumentRuleCatalog,
        ledger_limits: PaperLedgerLimits,
    ) -> Result<Self, ExchangeError>
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        let exchange = exchange.into();
        let exchange = exchange.trim();
        if exchange.is_empty() {
            return Err(ExchangeError::invalid("exchange name must not be empty"));
        }
        if max_market_age < chrono::TimeDelta::zero() {
            return Err(ExchangeError::invalid(
                "maximum market age must not be negative",
            ));
        }
        if max_future_skew < chrono::TimeDelta::zero() {
            return Err(ExchangeError::invalid(
                "maximum future market skew must not be negative",
            ));
        }
        if event_capacity.get() > MAX_EVENT_CAPACITY {
            return Err(ExchangeError::resource_limit(
                "paper event capacity",
                MAX_EVENT_CAPACITY,
                event_capacity.get(),
            ));
        }
        let clock: Arc<PaperClock> = Arc::new(clock);
        let observed_at = clock();
        let (market_sender, _) = broadcast::channel(event_capacity.get());
        Ok(Self {
            exchange: Arc::from(exchange),
            state: Arc::new(Mutex::new(PaperState::new(observed_at))),
            market_sender,
            clock,
            max_market_age,
            max_future_skew,
            instrument_rules: Arc::new(instrument_rules),
            instrument_rules_mode,
            ledger_limits,
        })
    }

    /// Reports whether missing instrument rules fail closed or use simulator compatibility.
    pub fn instrument_rules_status(&self) -> InstrumentRulesStatus {
        InstrumentRulesStatus {
            mode: self.instrument_rules_mode,
            rule_count: self.instrument_rules.len(),
        }
    }

    pub const fn ledger_limits(&self) -> PaperLedgerLimits {
        self.ledger_limits
    }

    /// Wraps this adapter in a bounded actor handle suitable for runtime use.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] when command capacity exceeds its hard cap.
    pub fn bounded(
        &self,
        command_capacity: NonZeroUsize,
    ) -> Result<BoundedExchangeHandle, ExchangeError> {
        BoundedExchangeHandle::spawn(Arc::new(self.clone()), command_capacity)
    }

    /// Injects one authoritative snapshot and deterministically crosses resting orders.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot belongs to another exchange, is stale,
    /// or would violate an internal paper-ledger invariant.
    #[allow(clippy::too_many_lines)]
    pub async fn publish_snapshot(&self, snapshot: MarketSnapshot) -> Result<(), ExchangeError> {
        self.ensure_exchange(snapshot.exchange())?;
        let mut state = self.state.lock().await;
        let now = self.effective_now(&state);
        if let Some(reason) = self.freshness_violation(&snapshot, now) {
            return Err(ExchangeError::invalid(reason));
        }
        let is_new_snapshot = !state.snapshots.iter().any(|existing| {
            existing.symbol == snapshot.symbol && existing.market_type == snapshot.market_type
        });
        if is_new_snapshot && state.snapshots.len() >= self.ledger_limits.snapshots {
            return Err(ExchangeError::resource_limit(
                "paper snapshot ledger",
                self.ledger_limits.snapshots,
                state.snapshots.len().saturating_add(1),
            ));
        }
        if let Some(existing) = state.snapshots.iter().find(|existing| {
            existing.symbol == snapshot.symbol && existing.market_type == snapshot.market_type
        }) && snapshot.timestamp <= existing.timestamp
        {
            return Err(ExchangeError::invalid(format!(
                "stale or duplicate snapshot for {}: {} is not after {}",
                snapshot.symbol, snapshot.timestamp, existing.timestamp
            )));
        }

        let mut candidate = state.clone();
        let snapshot_index = if let Some(index) = candidate.snapshots.iter().position(|existing| {
            existing.symbol == snapshot.symbol && existing.market_type == snapshot.market_type
        }) {
            candidate.snapshots[index] = snapshot.clone();
            index
        } else {
            candidate.snapshots.push(snapshot.clone());
            candidate.snapshots.len() - 1
        };
        candidate.observed_at = now;
        let mut position_indexes = build_position_index(&candidate.positions)?;

        for index in 0..candidate.orders.len() {
            let order = &candidate.orders[index];
            if !matches!(
                order.status,
                OrderStatus::Open | OrderStatus::PartiallyFilled
            ) || order.intent.symbol != snapshot.symbol
                || order.intent.market_type != snapshot.market_type
            {
                continue;
            }
            let intent = candidate.orders[index].intent.clone();
            let Some(fill_price) = crossing_price(&intent, &candidate.snapshots[snapshot_index])?
            else {
                continue;
            };
            let remaining = remaining_quantity(&candidate.orders[index])?;
            let available = if intent.reduce_only {
                available_reduce_only_quantity(
                    &candidate,
                    &intent,
                    Some(candidate.orders[index].id.as_str()),
                    Some(&position_indexes),
                )?
            } else if intent.market_type == MarketType::Spot && intent.side == Side::Sell {
                available_spot_inventory(
                    &candidate,
                    &intent,
                    Some(candidate.orders[index].id.as_str()),
                    Some(&position_indexes),
                )?
            } else {
                remaining
            };
            if available.is_zero() {
                let order = &mut candidate.orders[index];
                order.status = OrderStatus::Cancelled;
                order.updated_at = now;
                continue;
            }
            let fill_quantity = executable_quantity(
                &candidate,
                &intent,
                remaining.min(available),
                &candidate.snapshots[snapshot_index],
                Some(&position_indexes),
            );
            if fill_quantity.is_zero() {
                continue;
            }
            record_order_fill(&mut candidate.orders[index], fill_price, fill_quantity, now)?;
            consume_depth(
                &mut candidate.snapshots[snapshot_index],
                intent.side,
                fill_quantity,
            )?;
            apply_fill(
                &mut candidate,
                &intent,
                PaperFill {
                    quantity: fill_quantity,
                    price: fill_price,
                    mark: mark_price(&snapshot),
                    at: now,
                },
                self.ledger_limits.positions,
                Some(&mut position_indexes),
            )?;
            let order = &mut candidate.orders[index];
            order.status = if order.filled_quantity == order.intent.quantity {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
        }
        refresh_mark(&mut candidate, &snapshot, now)?;
        *state = candidate;
        drop(state);

        let _ignored_no_subscribers = self.market_sender.send(snapshot);
        Ok(())
    }

    /// Returns the latest snapshot for a symbol and market type.
    pub async fn snapshot(
        &self,
        symbol: &Symbol,
        market_type: MarketType,
    ) -> Option<MarketSnapshot> {
        self.state
            .lock()
            .await
            .snapshots
            .iter()
            .find(|snapshot| snapshot.symbol == *symbol && snapshot.market_type == market_type)
            .cloned()
    }

    /// Returns all accepted paper orders in deterministic insertion order.
    pub async fn orders(&self) -> Vec<Order> {
        self.state.lock().await.orders.clone()
    }

    /// Returns all paper positions in deterministic insertion order.
    pub async fn positions(&self) -> Vec<Position> {
        self.state.lock().await.positions.clone()
    }

    fn ensure_exchange(&self, candidate: &str) -> Result<(), ExchangeError> {
        if candidate == self.exchange.as_ref() {
            Ok(())
        } else {
            Err(ExchangeError::rejected(format!(
                "request exchange {candidate} does not match adapter {}",
                self.exchange
            )))
        }
    }

    fn now(&self) -> DateTime<Utc> {
        (self.clock)()
    }

    fn effective_now(&self, state: &PaperState) -> DateTime<Utc> {
        self.now().max(state.observed_at)
    }

    fn freshness_violation(&self, snapshot: &MarketSnapshot, now: DateTime<Utc>) -> Option<String> {
        let age = now.signed_duration_since(snapshot.timestamp);
        if age > self.max_market_age {
            return Some(format!(
                "snapshot for {} is too old: age {} ms exceeds {} ms",
                snapshot.symbol,
                age.num_milliseconds(),
                self.max_market_age.num_milliseconds()
            ));
        }
        let future_skew = snapshot.timestamp.signed_duration_since(now);
        if future_skew > self.max_future_skew {
            return Some(format!(
                "snapshot for {} is too far in the future: skew {} ms exceeds {} ms",
                snapshot.symbol,
                future_skew.num_milliseconds(),
                self.max_future_skew.num_milliseconds()
            ));
        }
        None
    }

    fn validate_instrument_rules(
        &self,
        intent: &OrderIntent,
        snapshot: Option<&MarketSnapshot>,
    ) -> Result<(), ExchangeError> {
        let Some(rules) =
            self.instrument_rules
                .find(&intent.exchange, &intent.symbol, intent.market_type)
        else {
            return match self.instrument_rules_mode {
                InstrumentRulesMode::Permissive => Ok(()),
                InstrumentRulesMode::Strict => Err(ExchangeError::rejected(format!(
                    "strict instrument rules are missing for {}:{}:{:?}",
                    intent.exchange, intent.symbol, intent.market_type
                ))),
            };
        };
        let reference_price = intent.price.or_else(|| {
            snapshot.map(|snapshot| match intent.side {
                Side::Buy => snapshot.ask(),
                Side::Sell => snapshot.bid(),
            })
        });
        rules.validate(intent, reference_price)
    }

    #[allow(clippy::too_many_lines)]
    async fn submit(&self, intent: OrderIntent) -> Result<TradingReceipt, ExchangeError> {
        self.ensure_exchange(&intent.exchange)?;
        validate_intent(&intent)?;
        let mut state = self.state.lock().await;
        let now = self.effective_now(&state);
        if let Some(existing) = state
            .orders
            .iter()
            .find(|order| order.intent.client_order_id == intent.client_order_id)
        {
            if existing.intent != intent {
                return Err(ExchangeError::rejected(format!(
                    "client_order_id {} was reused with a different intent",
                    intent.client_order_id
                )));
            }
            return Ok(TradingReceipt::Submitted {
                order: existing.clone(),
                disposition: SubmissionDisposition::AlreadyProcessed,
            });
        }
        if state.orders.len() >= self.ledger_limits.orders {
            return Err(ExchangeError::resource_limit(
                "paper order ledger",
                self.ledger_limits.orders,
                state.orders.len().saturating_add(1),
            ));
        }
        if intent.reduce_only
            && available_reduce_only_quantity(&state, &intent, None, None)?
                < intent.quantity.as_decimal()
        {
            return Err(ExchangeError::rejected(
                "reduce-only order would increase or reverse the position",
            ));
        }
        if intent.market_type == MarketType::Spot
            && intent.side == Side::Sell
            && available_spot_inventory(&state, &intent, None, None)? < intent.quantity.as_decimal()
        {
            return Err(ExchangeError::rejected(
                "spot sell requires available long inventory",
            ));
        }

        let snapshot_index = state.snapshots.iter().position(|snapshot| {
            snapshot.symbol == intent.symbol && snapshot.market_type == intent.market_type
        });
        if let Some(snapshot) = snapshot_index.map(|index| &state.snapshots[index])
            && let Some(reason) = self.freshness_violation(snapshot, now)
        {
            return Err(ExchangeError::rejected(reason));
        }
        self.validate_instrument_rules(
            &intent,
            snapshot_index.map(|index| &state.snapshots[index]),
        )?;
        if intent.order_type == OrderType::Market && snapshot_index.is_none() {
            return Err(ExchangeError::rejected(format!(
                "no market snapshot is available for {}",
                intent.symbol
            )));
        }
        let immediate_fill_price = snapshot_index
            .map(|index| crossing_price(&intent, &state.snapshots[index]))
            .transpose()?
            .flatten();
        if intent.time_in_force == TimeInForce::PostOnly && immediate_fill_price.is_some() {
            return Err(ExchangeError::rejected(
                "post-only order would take liquidity",
            ));
        }

        let mut candidate = state.clone();
        let sequence = candidate.next_order_id;
        candidate.next_order_id = sequence
            .checked_add(1)
            .ok_or_else(|| ExchangeError::invariant("paper order sequence overflowed"))?;
        let mut order = Order {
            id: format!("{}-{sequence:016}", self.exchange),
            intent: intent.clone(),
            filled_quantity: Quantity::default(),
            average_fill_price: None,
            status: OrderStatus::Open,
            created_at: now,
            updated_at: now,
        };
        let mut disposition = SubmissionDisposition::Open;
        if let Some(snapshot_index) = snapshot_index {
            let snapshot = candidate.snapshots[snapshot_index].clone();
            if let Some(fill_price) = immediate_fill_price {
                let requested = intent.quantity.as_decimal();
                let fill_quantity =
                    executable_quantity(&candidate, &intent, requested, &snapshot, None);
                if !fill_quantity.is_zero() {
                    record_order_fill(&mut order, fill_price, fill_quantity, now)?;
                    consume_depth(
                        &mut candidate.snapshots[snapshot_index],
                        intent.side,
                        fill_quantity,
                    )?;
                    apply_fill(
                        &mut candidate,
                        &intent,
                        PaperFill {
                            quantity: fill_quantity,
                            price: fill_price,
                            mark: mark_price(&snapshot),
                            at: now,
                        },
                        self.ledger_limits.positions,
                        None,
                    )?;
                }
                if fill_quantity == requested {
                    order.status = OrderStatus::Filled;
                    disposition = SubmissionDisposition::Filled;
                } else if intent.order_type == OrderType::Market
                    || matches!(intent.time_in_force, TimeInForce::Ioc | TimeInForce::Fok)
                {
                    order.status = OrderStatus::Cancelled;
                    disposition = SubmissionDisposition::Cancelled;
                } else if fill_quantity.is_zero() {
                    order.status = OrderStatus::Open;
                } else {
                    order.status = OrderStatus::PartiallyFilled;
                }
            } else if matches!(intent.time_in_force, TimeInForce::Ioc | TimeInForce::Fok) {
                order.status = OrderStatus::Cancelled;
                disposition = SubmissionDisposition::Cancelled;
            }
        } else if matches!(intent.time_in_force, TimeInForce::Ioc | TimeInForce::Fok) {
            order.status = OrderStatus::Cancelled;
            disposition = SubmissionDisposition::Cancelled;
        }
        candidate.orders.push(order.clone());
        candidate.observed_at = now;
        *state = candidate;

        Ok(TradingReceipt::Submitted { order, disposition })
    }

    async fn cancel(&self, order_id: &str) -> Result<TradingReceipt, ExchangeError> {
        let mut state = self.state.lock().await;
        let now = self.effective_now(&state);
        let order = state
            .orders
            .iter_mut()
            .find(|order| order.id == order_id)
            .ok_or_else(|| ExchangeError::rejected(format!("unknown order {order_id}")))?;

        let disposition = match order.status {
            OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled => {
                order.status = OrderStatus::Cancelled;
                order.updated_at = now;
                CancellationDisposition::Cancelled
            }
            OrderStatus::Cancelled => CancellationDisposition::AlreadyCancelled,
            OrderStatus::Filled | OrderStatus::Rejected => {
                return Err(ExchangeError::rejected(format!(
                    "order {order_id} is already terminal with status {:?}",
                    order.status
                )));
            }
        };
        let order = order.clone();
        state.observed_at = now;
        Ok(TradingReceipt::Cancelled {
            orders: vec![order],
            disposition,
        })
    }

    async fn cancel_all(
        &self,
        symbol: Option<&Symbol>,
        market_type: Option<MarketType>,
    ) -> Result<TradingReceipt, ExchangeError> {
        let mut state = self.state.lock().await;
        let now = self.effective_now(&state);
        let mut cancelled = Vec::new();
        for order in &mut state.orders {
            let active = matches!(
                order.status,
                OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
            );
            let symbol_matches = symbol.is_none_or(|candidate| order.intent.symbol == *candidate);
            let market_matches =
                market_type.is_none_or(|candidate| order.intent.market_type == candidate);
            if active && symbol_matches && market_matches {
                order.status = OrderStatus::Cancelled;
                order.updated_at = now;
                cancelled.push(order.clone());
            }
        }
        let disposition = if cancelled.is_empty() {
            CancellationDisposition::NoMatchingOrders
        } else {
            CancellationDisposition::Cancelled
        };
        state.observed_at = now;
        Ok(TradingReceipt::Cancelled {
            orders: cancelled,
            disposition,
        })
    }
}

#[async_trait]
impl ExchangeHandle for PaperExchange {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        match command {
            TradingCommand::Submit(intent) => self.submit(intent).await,
            TradingCommand::Cancel { order_id } => self.cancel(&order_id).await,
            TradingCommand::CancelAll {
                symbol,
                market_type,
            } => self.cancel_all(symbol.as_ref(), market_type).await,
        }
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        let mut state = self.state.lock().await;
        let observed_at = self.effective_now(&state);
        state.observed_at = observed_at;
        let (orders, positions) = match &scope {
            ReconcileScope::All => (state.orders.clone(), state.positions.clone()),
            ReconcileScope::Orders { symbol } => (
                state
                    .orders
                    .iter()
                    .filter(|order| {
                        symbol
                            .as_ref()
                            .is_none_or(|candidate| order.intent.symbol == *candidate)
                    })
                    .cloned()
                    .collect(),
                Vec::new(),
            ),
            ReconcileScope::Positions { symbol } => (
                Vec::new(),
                state
                    .positions
                    .iter()
                    .filter(|position| {
                        symbol
                            .as_ref()
                            .is_none_or(|candidate| position.symbol == *candidate)
                    })
                    .cloned()
                    .collect(),
            ),
        };
        Ok(ReconcileReceipt {
            scope,
            orders,
            foreign_orders: Vec::new(),
            positions,
            observed_at,
        })
    }

    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        let mut state = self.state.lock().await;
        let sequence = state.next_subscription_id;
        state.next_subscription_id = sequence
            .checked_add(1)
            .ok_or_else(|| ExchangeError::invariant("paper subscription sequence overflowed"))?;
        Ok(SubscriptionReceipt::new(
            format!("{}-subscription-{sequence:016}", self.exchange),
            subscription,
            self.market_sender.subscribe(),
        ))
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        let state = self.state.lock().await;
        Ok(ExchangeStatus {
            exchange: self.exchange.to_string(),
            mode: crate::ExchangeMode::Paper,
            availability: ExchangeAvailability::Ready,
            latest_market_timestamp: state.snapshots.iter().map(|item| item.timestamp).max(),
            open_orders: state
                .orders
                .iter()
                .filter(|order| {
                    matches!(
                        order.status,
                        OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
                    )
                })
                .count(),
        })
    }
}

fn validate_intent(intent: &OrderIntent) -> Result<(), ExchangeError> {
    if intent.quantity.as_decimal().is_zero() {
        return Err(ExchangeError::invalid(
            "order quantity must be greater than zero",
        ));
    }
    match (intent.order_type, intent.price) {
        (OrderType::Market, None) | (OrderType::Limit, Some(_)) => {}
        (OrderType::Market, Some(_)) => {
            return Err(ExchangeError::invalid(
                "market orders must not include a limit price",
            ));
        }
        (OrderType::Limit, None) => {
            return Err(ExchangeError::invalid("limit orders require a limit price"));
        }
    }
    if intent.order_type == OrderType::Market && intent.time_in_force == TimeInForce::PostOnly {
        return Err(ExchangeError::invalid("market orders cannot be post-only"));
    }
    Ok(())
}

fn crossing_price(
    intent: &OrderIntent,
    snapshot: &MarketSnapshot,
) -> Result<Option<Price>, ExchangeError> {
    match intent.order_type {
        OrderType::Market => Ok(Some(match intent.side {
            Side::Buy => snapshot.ask(),
            Side::Sell => snapshot.bid(),
        })),
        OrderType::Limit => {
            let limit = intent
                .price
                .ok_or_else(|| ExchangeError::invalid("limit order is missing its price"))?;
            let crosses = match intent.side {
                Side::Buy => limit >= snapshot.ask(),
                Side::Sell => limit <= snapshot.bid(),
            };
            Ok(crosses.then_some(match intent.side {
                Side::Buy => snapshot.ask(),
                Side::Sell => snapshot.bid(),
            }))
        }
    }
}

fn remaining_quantity(order: &Order) -> Result<Decimal, ExchangeError> {
    order
        .intent
        .quantity
        .as_decimal()
        .checked_sub(order.filled_quantity.as_decimal())
        .ok_or_else(|| ExchangeError::invariant("paper order filled quantity exceeds requested"))
}

fn is_active_order(order: &Order) -> bool {
    matches!(
        order.status,
        OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
    )
}

fn remaining_reservation_capacity(total: Decimal, reserved: Decimal) -> Decimal {
    if reserved >= total {
        Decimal::ZERO
    } else {
        total - reserved
    }
}

fn reserved_quantity_by<F>(
    state: &PaperState,
    current_order_id: Option<&str>,
    mut predicate: F,
) -> Result<Decimal, ExchangeError>
where
    F: FnMut(&Order) -> bool,
{
    let mut reserved = Decimal::ZERO;
    let mut found_current = current_order_id.is_none();
    for order in &state.orders {
        if current_order_id.is_some_and(|order_id| order.id == order_id) {
            found_current = true;
            break;
        }
        if !is_active_order(order) {
            continue;
        }
        if !predicate(order) {
            continue;
        }
        reserved = reserved
            .checked_add(remaining_quantity(order)?)
            .ok_or_else(|| ExchangeError::invariant("paper reservation quantity overflowed"))?;
    }
    if !found_current {
        return Err(ExchangeError::invariant(format!(
            "paper reservation lookup could not find current order {current_order_id:?}"
        )));
    }
    Ok(reserved)
}

fn available_spot_inventory(
    state: &PaperState,
    intent: &OrderIntent,
    current_order_id: Option<&str>,
    position_indexes: Option<&PositionIndex>,
) -> Result<Decimal, ExchangeError> {
    let inventory = spot_inventory_quantity(state, intent, position_indexes);
    let reserved = reserved_quantity_by(state, current_order_id, |order| {
        order.intent.market_type == MarketType::Spot
            && order.intent.side == Side::Sell
            && order.intent.symbol == intent.symbol
    })?;
    Ok(remaining_reservation_capacity(inventory, reserved))
}

fn available_reduce_only_quantity(
    state: &PaperState,
    intent: &OrderIntent,
    current_order_id: Option<&str>,
    position_indexes: Option<&PositionIndex>,
) -> Result<Decimal, ExchangeError> {
    let Some(position) = position_for_intent(state, intent, position_indexes) else {
        return Ok(Decimal::ZERO);
    };
    let signed = signed_quantity(position);
    let total = match intent.side {
        Side::Buy if signed.is_sign_negative() => signed.abs(),
        Side::Sell if signed.is_sign_positive() => signed.abs(),
        _ => return Ok(Decimal::ZERO),
    };
    let reserved = reserved_quantity_by(state, current_order_id, |order| {
        order.intent.reduce_only
            && order.intent.side == intent.side
            && order.intent.symbol == intent.symbol
            && order.intent.market_type == intent.market_type
    })?;
    Ok(remaining_reservation_capacity(total, reserved))
}

fn executable_quantity(
    state: &PaperState,
    intent: &OrderIntent,
    requested: Decimal,
    snapshot: &MarketSnapshot,
    position_indexes: Option<&PositionIndex>,
) -> Decimal {
    let depth = match intent.side {
        Side::Buy => snapshot.ask_quantity,
        Side::Sell => snapshot.bid_quantity,
    }
    .map_or(Decimal::ZERO, Quantity::as_decimal);
    let available = if intent.market_type == MarketType::Spot && intent.side == Side::Sell {
        depth.min(spot_inventory_quantity(state, intent, position_indexes))
    } else {
        depth
    };
    if intent.time_in_force == TimeInForce::Fok && available < requested {
        Decimal::ZERO
    } else {
        requested.min(available)
    }
}

fn consume_depth(
    snapshot: &mut MarketSnapshot,
    side: Side,
    filled: Decimal,
) -> Result<(), ExchangeError> {
    let depth = match side {
        Side::Buy => &mut snapshot.ask_quantity,
        Side::Sell => &mut snapshot.bid_quantity,
    };
    let Some(current) = *depth else {
        return Ok(());
    };
    let remaining = current
        .as_decimal()
        .checked_sub(filled)
        .ok_or_else(|| ExchangeError::invariant("paper top-of-book depth was over-consumed"))?;
    *depth = Some(
        Quantity::new(remaining).map_err(|error| ExchangeError::invariant(error.to_string()))?,
    );
    Ok(())
}

fn record_order_fill(
    order: &mut Order,
    fill_price: Price,
    filled: Decimal,
    at: DateTime<Utc>,
) -> Result<(), ExchangeError> {
    let previous = order.filled_quantity.as_decimal();
    let total = previous
        .checked_add(filled)
        .ok_or_else(|| ExchangeError::invariant("paper order filled quantity overflowed"))?;
    if total > order.intent.quantity.as_decimal() {
        return Err(ExchangeError::invariant(
            "paper order filled quantity exceeds requested quantity",
        ));
    }
    let average = if previous.is_zero() {
        fill_price
    } else {
        let old_average = order.average_fill_price.ok_or_else(|| {
            ExchangeError::invariant("partially filled order is missing an average fill price")
        })?;
        let old_weight = old_average
            .as_decimal()
            .checked_mul(previous)
            .ok_or_else(|| ExchangeError::invariant("paper order fill value overflowed"))?;
        let new_weight = fill_price
            .as_decimal()
            .checked_mul(filled)
            .ok_or_else(|| ExchangeError::invariant("paper order fill value overflowed"))?;
        let weighted = old_weight
            .checked_add(new_weight)
            .and_then(|value| value.checked_div(total))
            .ok_or_else(|| ExchangeError::invariant("paper average fill price overflowed"))?;
        Price::new(weighted).map_err(|error| ExchangeError::invariant(error.to_string()))?
    };
    order.filled_quantity =
        Quantity::new(total).map_err(|error| ExchangeError::invariant(error.to_string()))?;
    order.average_fill_price = Some(average);
    order.updated_at = at;
    Ok(())
}

fn spot_inventory_quantity(
    state: &PaperState,
    intent: &OrderIntent,
    position_indexes: Option<&PositionIndex>,
) -> Decimal {
    position_for_intent(state, intent, position_indexes)
        .filter(|position| {
            position.market_type == MarketType::Spot && position.side == PositionSide::Long
        })
        .map_or(Decimal::ZERO, |position| position.quantity.as_decimal())
}

fn build_position_index(positions: &[Position]) -> Result<PositionIndex, ExchangeError> {
    let mut indexes = PositionIndex::new();
    indexes
        .try_reserve(positions.len())
        .map_err(|_| ExchangeError::unavailable("failed to allocate paper position index"))?;
    for (index, position) in positions.iter().enumerate() {
        let key = (position.symbol.clone(), position.market_type);
        if indexes.insert(key, index).is_some() {
            return Err(ExchangeError::invariant(format!(
                "duplicate paper position for {} {:?}",
                position.symbol, position.market_type
            )));
        }
    }
    Ok(indexes)
}

fn position_for_intent<'a>(
    state: &'a PaperState,
    intent: &OrderIntent,
    position_indexes: Option<&PositionIndex>,
) -> Option<&'a Position> {
    if let Some(indexes) = position_indexes {
        return indexes
            .get(&(intent.symbol.clone(), intent.market_type))
            .and_then(|index| state.positions.get(*index));
    }
    state.positions.iter().find(|position| {
        position.symbol == intent.symbol && position.market_type == intent.market_type
    })
}

fn signed_quantity(position: &Position) -> Decimal {
    match position.side {
        PositionSide::Long => position.quantity.as_decimal(),
        PositionSide::Short => -position.quantity.as_decimal(),
        PositionSide::Flat => Decimal::ZERO,
    }
}

fn remove_position(
    state: &mut PaperState,
    index: usize,
    position_indexes: Option<&mut PositionIndex>,
) -> Result<(), ExchangeError> {
    let removed = state.positions.swap_remove(index);
    if let Some(indexes) = position_indexes {
        let removed_key = (removed.symbol, removed.market_type);
        if indexes.remove(&removed_key).is_none() {
            return Err(ExchangeError::invariant(
                "paper position index is missing a removed entry",
            ));
        }
        if index < state.positions.len() {
            let swapped = &state.positions[index];
            let swapped_key = (swapped.symbol.clone(), swapped.market_type);
            indexes.insert(swapped_key, index);
        }
    }
    Ok(())
}

fn apply_fill(
    state: &mut PaperState,
    intent: &OrderIntent,
    fill: PaperFill,
    max_positions: usize,
    position_indexes: Option<&mut PositionIndex>,
) -> Result<(), ExchangeError> {
    let PaperFill {
        quantity: fill_quantity,
        price: fill_price,
        mark,
        at,
    } = fill;
    let position_key = (intent.symbol.clone(), intent.market_type);
    let position_index = if let Some(indexes) = position_indexes.as_ref() {
        indexes.get(&position_key).copied()
    } else {
        state.positions.iter().position(|position| {
            position.symbol == intent.symbol && position.market_type == intent.market_type
        })
    };
    if position_index.is_none() && state.positions.len() >= max_positions {
        return Err(ExchangeError::resource_limit(
            "paper position ledger",
            max_positions,
            state.positions.len().saturating_add(1),
        ));
    }
    let existing = position_index.map(|index| state.positions[index].clone());
    let old_signed = existing.as_ref().map_or(Decimal::ZERO, signed_quantity);
    let delta = match intent.side {
        Side::Buy => fill_quantity,
        Side::Sell => -fill_quantity,
    };
    let new_signed = old_signed
        .checked_add(delta)
        .ok_or_else(|| ExchangeError::invariant("paper position quantity overflowed"))?;
    if new_signed.is_zero() {
        if let Some(index) = position_index {
            remove_position(state, index, position_indexes)?;
        }
        return Ok(());
    }
    let old_entry = existing.as_ref().and_then(|position| position.entry_price);
    let entry = calculate_entry(old_signed, old_entry, delta, fill_price, new_signed)?;
    let side = if new_signed.is_sign_positive() {
        PositionSide::Long
    } else {
        PositionSide::Short
    };
    let mark = Some(mark);
    let position = Position {
        exchange: intent.exchange.clone(),
        symbol: intent.symbol.clone(),
        market_type: intent.market_type,
        side,
        quantity: Quantity::new(new_signed.abs())
            .map_err(|error| ExchangeError::invariant(error.to_string()))?,
        entry_price: entry,
        mark_price: mark,
        unrealized_pnl: unrealized_pnl(side, new_signed.abs(), entry, mark)?,
        updated_at: at,
    };
    if let Some(index) = position_index {
        state.positions[index] = position;
    } else {
        let new_position_index = state.positions.len();
        state.positions.push(position);
        if let Some(indexes) = position_indexes
            && indexes.insert(position_key, new_position_index).is_some()
        {
            return Err(ExchangeError::invariant(
                "paper position index unexpectedly replaced an entry",
            ));
        }
    }
    Ok(())
}

fn calculate_entry(
    old_signed: Decimal,
    old_entry: Option<Price>,
    delta: Decimal,
    fill_price: Price,
    new_signed: Decimal,
) -> Result<Option<Price>, ExchangeError> {
    if new_signed.is_zero() {
        return Ok(None);
    }
    if old_signed.is_zero() || old_signed.is_sign_negative() != new_signed.is_sign_negative() {
        return Ok(Some(fill_price));
    }
    if old_signed.is_sign_negative() != delta.is_sign_negative() {
        return Ok(old_entry);
    }
    let old_entry = old_entry
        .ok_or_else(|| ExchangeError::invariant("non-flat position is missing an entry price"))?;
    let old_weight = old_entry
        .as_decimal()
        .checked_mul(old_signed.abs())
        .ok_or_else(|| ExchangeError::invariant("paper weighted entry value overflowed"))?;
    let fill_weight = fill_price
        .as_decimal()
        .checked_mul(delta.abs())
        .ok_or_else(|| ExchangeError::invariant("paper weighted fill value overflowed"))?;
    let total = old_signed
        .abs()
        .checked_add(delta.abs())
        .ok_or_else(|| ExchangeError::invariant("paper weighted quantity overflowed"))?;
    let weighted = old_weight
        .checked_add(fill_weight)
        .and_then(|value| value.checked_div(total))
        .ok_or_else(|| ExchangeError::invariant("paper weighted entry calculation overflowed"))?;
    Price::new(weighted)
        .map(Some)
        .map_err(|error| ExchangeError::invariant(error.to_string()))
}

fn mark_price(snapshot: &MarketSnapshot) -> Price {
    snapshot.last.unwrap_or_else(|| snapshot.mid_price())
}

fn refresh_mark(
    state: &mut PaperState,
    snapshot: &MarketSnapshot,
    at: DateTime<Utc>,
) -> Result<(), ExchangeError> {
    let mark = mark_price(snapshot);
    for position in &mut state.positions {
        if position.symbol == snapshot.symbol && position.market_type == snapshot.market_type {
            position.mark_price = Some(mark);
            position.unrealized_pnl = unrealized_pnl(
                position.side,
                position.quantity.as_decimal(),
                position.entry_price,
                position.mark_price,
            )?;
            position.updated_at = at;
        }
    }
    Ok(())
}

fn unrealized_pnl(
    side: PositionSide,
    quantity: Decimal,
    entry: Option<Price>,
    mark: Option<Price>,
) -> Result<Money, ExchangeError> {
    let Some((entry, mark)) = entry.zip(mark) else {
        return Ok(Money::default());
    };
    let pnl = match side {
        PositionSide::Long => mark
            .as_decimal()
            .checked_sub(entry.as_decimal())
            .and_then(|difference| difference.checked_mul(quantity)),
        PositionSide::Short => entry
            .as_decimal()
            .checked_sub(mark.as_decimal())
            .and_then(|difference| difference.checked_mul(quantity)),
        PositionSide::Flat => Some(Decimal::ZERO),
    }
    .ok_or_else(|| ExchangeError::invariant("paper unrealized PnL overflowed"))?;
    Ok(Money::new(pnl))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::advance_clock_high_water;

    fn timestamp(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("test timestamp must be valid")
            .with_timezone(&Utc)
    }

    #[test]
    fn clock_high_water_uses_wall_progress_after_suspend_and_never_rewinds() {
        let mut observed = timestamp("2026-07-14T00:00:00Z");
        assert_eq!(
            advance_clock_high_water(
                &mut observed,
                timestamp("2026-07-14T00:00:10Z"),
                timestamp("2026-07-14T01:00:00Z"),
            ),
            timestamp("2026-07-14T01:00:00Z")
        );
        assert_eq!(
            advance_clock_high_water(
                &mut observed,
                timestamp("2026-07-14T00:00:20Z"),
                timestamp("2026-07-14T00:30:00Z"),
            ),
            timestamp("2026-07-14T01:00:00Z")
        );
    }
}
