use std::{num::NonZeroUsize, sync::Arc};

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
    ExchangeHandle, ExchangeStatus, MarketSubscription, ReconcileReceipt, ReconcileScope,
    SubmissionDisposition, SubscriptionReceipt, TradingCommand, TradingReceipt,
};

/// Deterministic in-memory exchange used for paper execution and contract tests.
#[derive(Clone)]
pub struct PaperExchange {
    exchange: Arc<str>,
    state: Arc<Mutex<PaperState>>,
    market_sender: broadcast::Sender<MarketSnapshot>,
}

struct PaperState {
    snapshots: Vec<MarketSnapshot>,
    orders: Vec<Order>,
    positions: Vec<Position>,
    next_order_id: u64,
    next_subscription_id: u64,
    observed_at: DateTime<Utc>,
}

impl Default for PaperState {
    fn default() -> Self {
        Self {
            snapshots: Vec::new(),
            orders: Vec::new(),
            positions: Vec::new(),
            next_order_id: 1,
            next_subscription_id: 1,
            observed_at: DateTime::<Utc>::UNIX_EPOCH,
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
        let exchange = exchange.into();
        let exchange = exchange.trim();
        if exchange.is_empty() {
            return Err(ExchangeError::invalid("exchange name must not be empty"));
        }
        let (market_sender, _) = broadcast::channel(event_capacity.get());
        Ok(Self {
            exchange: Arc::from(exchange),
            state: Arc::new(Mutex::new(PaperState::default())),
            market_sender,
        })
    }

    /// Wraps this adapter in a bounded actor handle suitable for runtime use.
    pub fn bounded(&self, command_capacity: NonZeroUsize) -> BoundedExchangeHandle {
        BoundedExchangeHandle::spawn(Arc::new(self.clone()), command_capacity)
    }

    /// Injects one authoritative snapshot and deterministically crosses resting orders.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot belongs to another exchange, is stale,
    /// or would violate an internal paper-ledger invariant.
    pub async fn publish_snapshot(&self, snapshot: MarketSnapshot) -> Result<(), ExchangeError> {
        self.ensure_exchange(snapshot.exchange())?;

        let mut state = self.state.lock().await;
        if let Some(existing) = state.snapshots.iter().find(|existing| {
            existing.symbol == snapshot.symbol && existing.market_type == snapshot.market_type
        }) {
            if snapshot.timestamp < existing.timestamp {
                return Err(ExchangeError::invalid(format!(
                    "stale snapshot for {}: {} is before {}",
                    snapshot.symbol, snapshot.timestamp, existing.timestamp
                )));
            }
        }

        if let Some(existing) = state.snapshots.iter_mut().find(|existing| {
            existing.symbol == snapshot.symbol && existing.market_type == snapshot.market_type
        }) {
            *existing = snapshot.clone();
        } else {
            state.snapshots.push(snapshot.clone());
        }
        state.observed_at = state.observed_at.max(snapshot.timestamp);

        let mut candidates = Vec::new();
        for (index, order) in state.orders.iter().enumerate() {
            if order.status != OrderStatus::Open
                || order.intent.symbol != snapshot.symbol
                || order.intent.market_type != snapshot.market_type
            {
                continue;
            }
            if let Some(fill_price) = crossing_price(&order.intent, &snapshot)? {
                candidates.push((index, fill_price));
            }
        }
        for (index, fill_price) in candidates {
            let intent = state.orders[index].intent.clone();
            if intent.reduce_only && !reduces_position(&state, &intent) {
                let order = &mut state.orders[index];
                order.status = OrderStatus::Cancelled;
                order.updated_at = snapshot.timestamp;
            } else {
                let order = &mut state.orders[index];
                order.status = OrderStatus::Filled;
                order.filled_quantity = order.intent.quantity;
                order.average_fill_price = Some(fill_price);
                order.updated_at = snapshot.timestamp;
                apply_fill(&mut state, &intent, fill_price, snapshot.timestamp)?;
            }
        }
        refresh_mark(&mut state, &snapshot);
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

    async fn submit(&self, intent: OrderIntent) -> Result<TradingReceipt, ExchangeError> {
        self.ensure_exchange(&intent.exchange)?;
        validate_intent(&intent)?;

        let mut state = self.state.lock().await;
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
        if intent.reduce_only && !reduces_position(&state, &intent) {
            return Err(ExchangeError::rejected(
                "reduce-only order would increase or reverse the position",
            ));
        }

        let latest_snapshot = state
            .snapshots
            .iter()
            .find(|snapshot| {
                snapshot.symbol == intent.symbol && snapshot.market_type == intent.market_type
            })
            .cloned();
        let (status, fill_price, disposition, at) =
            submission_outcome(&intent, latest_snapshot.as_ref(), state.observed_at)?;

        let sequence = state.next_order_id;
        state.next_order_id = sequence
            .checked_add(1)
            .ok_or_else(|| ExchangeError::invariant("paper order sequence overflowed"))?;
        let order = Order {
            id: format!("{}-{sequence:016}", self.exchange),
            intent: intent.clone(),
            filled_quantity: if status == OrderStatus::Filled {
                intent.quantity
            } else {
                Quantity::default()
            },
            average_fill_price: fill_price,
            status,
            created_at: at,
            updated_at: at,
        };
        state.orders.push(order.clone());
        if let Some(fill_price) = fill_price {
            apply_fill(&mut state, &intent, fill_price, at)?;
        }

        Ok(TradingReceipt::Submitted { order, disposition })
    }

    async fn cancel(&self, order_id: &str) -> Result<TradingReceipt, ExchangeError> {
        let mut state = self.state.lock().await;
        let observed_at = state.observed_at;
        let order = state
            .orders
            .iter_mut()
            .find(|order| order.id == order_id)
            .ok_or_else(|| ExchangeError::rejected(format!("unknown order {order_id}")))?;

        let disposition = match order.status {
            OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled => {
                order.status = OrderStatus::Cancelled;
                order.updated_at = observed_at;
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
        Ok(TradingReceipt::Cancelled {
            orders: vec![order.clone()],
            disposition,
        })
    }

    async fn cancel_all(
        &self,
        symbol: Option<&Symbol>,
        market_type: Option<MarketType>,
    ) -> Result<TradingReceipt, ExchangeError> {
        let mut state = self.state.lock().await;
        let observed_at = state.observed_at;
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
                order.updated_at = observed_at;
                cancelled.push(order.clone());
            }
        }
        let disposition = if cancelled.is_empty() {
            CancellationDisposition::NoMatchingOrders
        } else {
            CancellationDisposition::Cancelled
        };
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
        let state = self.state.lock().await;
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
            positions,
            observed_at: state.observed_at,
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

fn submission_outcome(
    intent: &OrderIntent,
    snapshot: Option<&MarketSnapshot>,
    fallback_time: DateTime<Utc>,
) -> Result<
    (
        OrderStatus,
        Option<Price>,
        SubmissionDisposition,
        DateTime<Utc>,
    ),
    ExchangeError,
> {
    let at = snapshot.map_or(fallback_time, |snapshot| snapshot.timestamp);
    if intent.order_type == OrderType::Market {
        let snapshot = snapshot.ok_or_else(|| {
            ExchangeError::rejected(format!(
                "no market snapshot is available for {}",
                intent.symbol
            ))
        })?;
        let fill_price = crossing_price(intent, snapshot)?
            .ok_or_else(|| ExchangeError::invariant("market order did not produce a fill price"))?;
        return Ok((
            OrderStatus::Filled,
            Some(fill_price),
            SubmissionDisposition::Filled,
            at,
        ));
    }

    let fill_price = snapshot
        .and_then(|snapshot| crossing_price(intent, snapshot).transpose())
        .transpose()?;
    if let Some(fill_price) = fill_price {
        if intent.time_in_force == TimeInForce::PostOnly {
            return Err(ExchangeError::rejected(
                "post-only order would take liquidity",
            ));
        }
        return Ok((
            OrderStatus::Filled,
            Some(fill_price),
            SubmissionDisposition::Filled,
            at,
        ));
    }
    if matches!(intent.time_in_force, TimeInForce::Ioc | TimeInForce::Fok) {
        Ok((
            OrderStatus::Cancelled,
            None,
            SubmissionDisposition::Cancelled,
            at,
        ))
    } else {
        Ok((OrderStatus::Open, None, SubmissionDisposition::Open, at))
    }
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

fn reduces_position(state: &PaperState, intent: &OrderIntent) -> bool {
    let Some(position) = state.positions.iter().find(|position| {
        position.symbol == intent.symbol && position.market_type == intent.market_type
    }) else {
        return false;
    };
    let signed = signed_quantity(position);
    let requested = intent.quantity.as_decimal();
    match intent.side {
        Side::Buy => signed.is_sign_negative() && requested <= signed.abs(),
        Side::Sell => signed.is_sign_positive() && requested <= signed.abs(),
    }
}

fn signed_quantity(position: &Position) -> Decimal {
    match position.side {
        PositionSide::Long => position.quantity.as_decimal(),
        PositionSide::Short => -position.quantity.as_decimal(),
        PositionSide::Flat => Decimal::ZERO,
    }
}

fn apply_fill(
    state: &mut PaperState,
    intent: &OrderIntent,
    fill_price: Price,
    at: DateTime<Utc>,
) -> Result<(), ExchangeError> {
    let position_index = state.positions.iter().position(|position| {
        position.symbol == intent.symbol && position.market_type == intent.market_type
    });
    let existing = position_index.map(|index| state.positions[index].clone());
    let old_signed = existing.as_ref().map_or(Decimal::ZERO, signed_quantity);
    let delta = match intent.side {
        Side::Buy => intent.quantity.as_decimal(),
        Side::Sell => -intent.quantity.as_decimal(),
    };
    let new_signed = old_signed + delta;
    let old_entry = existing.as_ref().and_then(|position| position.entry_price);
    let entry = calculate_entry(old_signed, old_entry, delta, fill_price, new_signed)?;
    let side = if new_signed.is_zero() {
        PositionSide::Flat
    } else if new_signed.is_sign_positive() {
        PositionSide::Long
    } else {
        PositionSide::Short
    };
    let mark = state
        .snapshots
        .iter()
        .find(|snapshot| {
            snapshot.symbol == intent.symbol && snapshot.market_type == intent.market_type
        })
        .map(mark_price)
        .or(Some(fill_price));
    let position = Position {
        exchange: intent.exchange.clone(),
        symbol: intent.symbol.clone(),
        market_type: intent.market_type,
        side,
        quantity: Quantity::new(new_signed.abs())
            .map_err(|error| ExchangeError::invariant(error.to_string()))?,
        entry_price: entry,
        mark_price: mark,
        unrealized_pnl: unrealized_pnl(side, new_signed.abs(), entry, mark),
        updated_at: at,
    };
    if let Some(index) = position_index {
        state.positions[index] = position;
    } else {
        state.positions.push(position);
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
    let total = old_signed.abs() + delta.abs();
    let weighted =
        (old_entry.as_decimal() * old_signed.abs() + fill_price.as_decimal() * delta.abs()) / total;
    Price::new(weighted)
        .map(Some)
        .map_err(|error| ExchangeError::invariant(error.to_string()))
}

fn mark_price(snapshot: &MarketSnapshot) -> Price {
    snapshot.last.unwrap_or_else(|| snapshot.mid_price())
}

fn refresh_mark(state: &mut PaperState, snapshot: &MarketSnapshot) {
    let mark = mark_price(snapshot);
    for position in &mut state.positions {
        if position.symbol == snapshot.symbol && position.market_type == snapshot.market_type {
            position.mark_price = Some(mark);
            position.unrealized_pnl = unrealized_pnl(
                position.side,
                position.quantity.as_decimal(),
                position.entry_price,
                position.mark_price,
            );
            position.updated_at = snapshot.timestamp;
        }
    }
}

fn unrealized_pnl(
    side: PositionSide,
    quantity: Decimal,
    entry: Option<Price>,
    mark: Option<Price>,
) -> Money {
    let Some((entry, mark)) = entry.zip(mark) else {
        return Money::default();
    };
    let pnl = match side {
        PositionSide::Long => (mark.as_decimal() - entry.as_decimal()) * quantity,
        PositionSide::Short => (entry.as_decimal() - mark.as_decimal()) * quantity,
        PositionSide::Flat => Decimal::ZERO,
    };
    Money::new(pnl)
}
