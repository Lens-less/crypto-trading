use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::Quantity;
use crypto_trading_exchange::{
    BinanceAccountUpdateEvent, BinanceExecutionReportEvent, BinanceUserDataBalance,
    BinanceUserDataEvent,
};
use rust_decimal::Decimal;

use crate::{
    market_data::{
        MarketDataClock, MarketDataError, MarketDataSourceFailure, classify_exchange_failure,
    },
    market_stream::{
        MarketStreamJitter, MarketStreamReconnectPolicy, MarketStreamSleeper,
        TextWebSocketConnector, TextWebSocketEvent, WebSocketCloseKind,
    },
};

type ExecutionFingerprint = (u64, Option<u64>, DateTime<Utc>, DateTime<Utc>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEnvelope<T> {
    pub connection_generation: u64,
    pub local_sequence: u64,
    pub observed_at: DateTime<Utc>,
    pub payload: T,
}

impl<T> StreamEnvelope<T> {
    /// Builds one validated stream envelope.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InvalidRevision`] when either sequence axis
    /// is zero.
    pub fn new(
        connection_generation: u64,
        local_sequence: u64,
        observed_at: DateTime<Utc>,
        payload: T,
    ) -> Result<Self, MarketDataError> {
        if connection_generation == 0 || local_sequence == 0 {
            return Err(MarketDataError::InvalidRevision);
        }
        Ok(Self {
            connection_generation,
            local_sequence,
            observed_at,
            payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceUserDataReconcileReason {
    ConnectionRestart,
    TransportGap,
    StreamExpired,
    EventTimeRegression,
    ExecutionRegression,
    LocalSequenceRegression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceUserDataApply {
    AppliedExecution,
    AppliedAccountUpdate,
    Duplicate,
    IgnoredUnsupported,
    ReconcileRequired(BinanceUserDataReconcileReason),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinanceUserDataStreamItem {
    Subscribed {
        subscription_id: u64,
        observed_at: DateTime<Utc>,
    },
    Heartbeat {
        observed_at: DateTime<Utc>,
    },
    Event(StreamEnvelope<BinanceUserDataEvent>),
    TransportGap {
        skipped: u64,
        observed_at: DateTime<Utc>,
    },
    StreamExpired {
        observed_at: DateTime<Utc>,
    },
    SourceUnavailable {
        failure: MarketDataSourceFailure,
        observed_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUserDataOrderState {
    pub cumulative_filled_quantity: Quantity,
    last_execution_id: Option<u64>,
    last_transaction_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUserDataBalanceState {
    pub free: Decimal,
    pub locked: Decimal,
}

#[derive(Debug, Default)]
pub struct BinanceUserDataState {
    current_generation: Option<u64>,
    last_local_sequence: u64,
    last_event_time: Option<DateTime<Utc>>,
    last_account_update_time: Option<DateTime<Utc>>,
    last_account_update_event_time: Option<DateTime<Utc>>,
    seen_execution_fingerprints: HashSet<ExecutionFingerprint>,
    orders: HashMap<u64, BinanceUserDataOrderState>,
    balances: HashMap<String, BinanceUserDataBalanceState>,
    reconcile_required: Option<BinanceUserDataReconcileReason>,
}

#[derive(Debug)]
pub struct BinanceUserDataStreamSource {
    connector: Arc<dyn TextWebSocketConnector>,
    clock: Arc<dyn MarketDataClock>,
    sleeper: Arc<dyn MarketStreamSleeper>,
    jitter: Arc<dyn MarketStreamJitter>,
    reconnect_policy: MarketStreamReconnectPolicy,
    session: Option<Box<dyn crate::market_stream::TextWebSocketSession>>,
    pending_items: VecDeque<BinanceUserDataStreamItem>,
    connection_generation: u64,
    local_sequence: u64,
    consecutive_failures: u32,
    pending_retry: Option<std::time::Duration>,
    exhausted: bool,
}

impl BinanceUserDataStreamSource {
    pub fn new<C>(
        connector: Arc<dyn TextWebSocketConnector>,
        reconnect_policy: MarketStreamReconnectPolicy,
        clock: Arc<C>,
        sleeper: Arc<dyn MarketStreamSleeper>,
        jitter: Arc<dyn MarketStreamJitter>,
    ) -> Self
    where
        C: MarketDataClock + 'static,
    {
        Self {
            connector,
            clock,
            sleeper,
            jitter,
            reconnect_policy,
            session: None,
            pending_items: VecDeque::new(),
            connection_generation: 0,
            local_sequence: 0,
            consecutive_failures: 0,
            pending_retry: None,
            exhausted: false,
        }
    }

    /// Produces the next user-data stream item, or `None` once reconnects are
    /// exhausted.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] only for internal contract violations such
    /// as revision overflow or a missing active session after a successful
    /// connect.
    pub async fn next_item(
        &mut self,
    ) -> Result<Option<BinanceUserDataStreamItem>, MarketDataError> {
        loop {
            if let Some(item) = self.pending_items.pop_front() {
                return Ok(Some(item));
            }
            if self.exhausted {
                return Ok(None);
            }
            if let Some(delay) = self.pending_retry.take() {
                self.sleeper.sleep(delay).await;
            }
            if self.session.is_none() {
                match self.connector.connect().await {
                    Ok(session) => {
                        self.session = Some(session);
                        self.connection_generation = self.connection_generation.saturating_add(1);
                        self.local_sequence = 0;
                    }
                    Err(error) => {
                        return Ok(Some(self.schedule_reconnect(&error, false)));
                    }
                }
            }
            let observed_at = self.clock.now();
            let Some(session) = self.session.as_mut() else {
                return Err(MarketDataError::SourceIdentityMismatch {
                    expected: "binance".to_owned(),
                    actual: "user-data websocket session disappeared".to_owned(),
                });
            };
            let event = session.next_event().await.map_err(|error| {
                MarketDataError::SourceIdentityMismatch {
                    expected: "binance".to_owned(),
                    actual: error.to_string(),
                }
            })?;
            match event {
                TextWebSocketEvent::Text(text) => {
                    if let Some(item) = self.handle_text(&text, observed_at)? {
                        return Ok(Some(item));
                    }
                }
                TextWebSocketEvent::Heartbeat => {
                    self.consecutive_failures = 0;
                    self.exhausted = false;
                    return Ok(Some(BinanceUserDataStreamItem::Heartbeat { observed_at }));
                }
                TextWebSocketEvent::Lagged { skipped } => {
                    return Ok(Some(BinanceUserDataStreamItem::TransportGap {
                        skipped,
                        observed_at,
                    }));
                }
                TextWebSocketEvent::Closed { kind } => {
                    return Ok(Some(self.handle_closed(kind, observed_at)));
                }
            }
        }
    }

    fn handle_text(
        &mut self,
        text: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<Option<BinanceUserDataStreamItem>, MarketDataError> {
        if let Some(item) = parse_subscription_ack(text, observed_at)? {
            self.consecutive_failures = 0;
            self.exhausted = false;
            return Ok(Some(item));
        }
        let payload =
            crypto_trading_exchange::BinanceTestnetProtocol::parse_user_data_event(text.as_bytes())
                .map_err(|error| MarketDataError::SourceIdentityMismatch {
                    expected: "binance".to_owned(),
                    actual: error.to_string(),
                })?;
        self.local_sequence = self
            .local_sequence
            .checked_add(1)
            .ok_or(MarketDataError::RevisionExhausted)?;
        if matches!(payload, BinanceUserDataEvent::StreamTerminated(_)) {
            self.session = None;
            self.pending_retry = Some(self.reconnect_policy.retry_delay(1, self.jitter.as_ref()));
            return Ok(Some(BinanceUserDataStreamItem::StreamExpired {
                observed_at,
            }));
        }
        self.consecutive_failures = 0;
        self.exhausted = false;
        Ok(Some(BinanceUserDataStreamItem::Event(StreamEnvelope::new(
            self.connection_generation,
            self.local_sequence,
            observed_at,
            payload,
        )?)))
    }

    fn handle_closed(
        &mut self,
        kind: WebSocketCloseKind,
        observed_at: DateTime<Utc>,
    ) -> BinanceUserDataStreamItem {
        self.session = None;
        match kind {
            WebSocketCloseKind::Expired | WebSocketCloseKind::ServerShutdown => {
                self.pending_retry =
                    Some(self.reconnect_policy.retry_delay(1, self.jitter.as_ref()));
                BinanceUserDataStreamItem::StreamExpired { observed_at }
            }
            WebSocketCloseKind::Remote | WebSocketCloseKind::Protocol => self.schedule_reconnect(
                &crypto_trading_exchange::ExchangeError::unavailable("user-data websocket closed"),
                true,
            ),
        }
    }

    fn schedule_reconnect(
        &mut self,
        error: &crypto_trading_exchange::ExchangeError,
        transport_gap: bool,
    ) -> BinanceUserDataStreamItem {
        self.session = None;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let observed_at = self.clock.now();
        if transport_gap {
            self.pending_items
                .push_back(BinanceUserDataStreamItem::TransportGap {
                    skipped: 1,
                    observed_at,
                });
        }
        if self.reconnect_policy.exhausted(self.consecutive_failures) {
            self.exhausted = true;
        } else {
            self.pending_retry = Some(
                self.reconnect_policy
                    .retry_delay(self.consecutive_failures, self.jitter.as_ref()),
            );
        }
        BinanceUserDataStreamItem::SourceUnavailable {
            failure: classify_exchange_failure(error),
            observed_at,
        }
    }
}

fn parse_subscription_ack(
    text: &str,
    observed_at: DateTime<Utc>,
) -> Result<Option<BinanceUserDataStreamItem>, MarketDataError> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| MarketDataError::SourceIdentityMismatch {
            expected: "binance".to_owned(),
            actual: error.to_string(),
        })?;
    if value.get("event").is_some() || value.get("e").is_some() {
        return Ok(None);
    }
    let Some(status) = value.get("status").and_then(serde_json::Value::as_u64) else {
        return Ok(None);
    };
    if status != 200 {
        return Err(MarketDataError::SourceIdentityMismatch {
            expected: "binance".to_owned(),
            actual: "Binance user-data subscription was rejected".to_owned(),
        });
    }
    let subscription_id = value
        .get("result")
        .and_then(|result| result.get("subscriptionId"))
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| MarketDataError::SourceIdentityMismatch {
            expected: "binance".to_owned(),
            actual: "Binance user-data subscription ack is missing subscriptionId".to_owned(),
        })?;
    Ok(Some(BinanceUserDataStreamItem::Subscribed {
        subscription_id,
        observed_at,
    }))
}

impl BinanceUserDataState {
    pub fn apply(
        &mut self,
        envelope: StreamEnvelope<BinanceUserDataEvent>,
    ) -> BinanceUserDataApply {
        if let Some(reason) = self.reconcile_required.clone() {
            return BinanceUserDataApply::ReconcileRequired(reason);
        }
        if let Some(current_generation) = self.current_generation {
            if envelope.connection_generation != current_generation {
                return self.fail(BinanceUserDataReconcileReason::ConnectionRestart);
            }
            if envelope.local_sequence <= self.last_local_sequence {
                return self.fail(BinanceUserDataReconcileReason::LocalSequenceRegression);
            }
        } else {
            self.current_generation = Some(envelope.connection_generation);
        }
        self.last_local_sequence = envelope.local_sequence;
        match envelope.payload {
            BinanceUserDataEvent::ExecutionReport(event) => self.apply_execution_report(event),
            BinanceUserDataEvent::AccountUpdate(event) => self.apply_account_update(event),
            BinanceUserDataEvent::StreamTerminated(_) => {
                self.fail(BinanceUserDataReconcileReason::StreamExpired)
            }
            BinanceUserDataEvent::Unsupported(_) => BinanceUserDataApply::IgnoredUnsupported,
        }
    }

    pub fn note_transport_gap(
        &mut self,
        _skipped: u64,
        _observed_at: DateTime<Utc>,
    ) -> BinanceUserDataApply {
        self.fail(BinanceUserDataReconcileReason::TransportGap)
    }

    pub fn note_stream_expired(&mut self, _observed_at: DateTime<Utc>) -> BinanceUserDataApply {
        self.fail(BinanceUserDataReconcileReason::StreamExpired)
    }

    pub fn order(&self, order_id: u64) -> Option<&BinanceUserDataOrderState> {
        self.orders.get(&order_id)
    }

    pub fn balance(&self, asset: &str) -> Option<&BinanceUserDataBalanceState> {
        self.balances.get(asset)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn apply_execution_report(
        &mut self,
        event: BinanceExecutionReportEvent,
    ) -> BinanceUserDataApply {
        if self
            .last_event_time
            .is_some_and(|last_event_time| event.event_time < last_event_time)
        {
            return self.fail(BinanceUserDataReconcileReason::EventTimeRegression);
        }
        let fingerprint = (
            event.order_id,
            event.execution_id,
            event.event_time,
            event.transaction_time,
        );
        if !self.seen_execution_fingerprints.insert(fingerprint) {
            return BinanceUserDataApply::Duplicate;
        }
        let order = self
            .orders
            .entry(event.order_id)
            .or_insert(BinanceUserDataOrderState {
                cumulative_filled_quantity: event.cumulative_filled_quantity,
                last_execution_id: event.execution_id,
                last_transaction_time: event.transaction_time,
            });
        if event.cumulative_filled_quantity.as_decimal()
            < order.cumulative_filled_quantity.as_decimal()
            || event
                .execution_id
                .zip(order.last_execution_id)
                .is_some_and(|(current, last)| current < last)
            || event.transaction_time < order.last_transaction_time
        {
            return self.fail(BinanceUserDataReconcileReason::ExecutionRegression);
        }
        order.cumulative_filled_quantity = event.cumulative_filled_quantity;
        order.last_execution_id = event.execution_id;
        order.last_transaction_time = event.transaction_time;
        self.last_event_time = Some(
            self.last_event_time
                .map_or(event.event_time, |last| last.max(event.event_time)),
        );
        BinanceUserDataApply::AppliedExecution
    }

    fn apply_account_update(&mut self, event: BinanceAccountUpdateEvent) -> BinanceUserDataApply {
        if self
            .last_event_time
            .is_some_and(|last_event_time| event.event_time < last_event_time)
        {
            return self.fail(BinanceUserDataReconcileReason::EventTimeRegression);
        }
        if self
            .last_account_update_time
            .is_some_and(|last_update_time| event.account_update_time < last_update_time)
        {
            return self.fail(BinanceUserDataReconcileReason::EventTimeRegression);
        }
        if self.last_account_update_time == Some(event.account_update_time)
            && self.last_account_update_event_time == Some(event.event_time)
        {
            return BinanceUserDataApply::Duplicate;
        }
        for balance in event.balances {
            self.apply_balance(balance);
        }
        self.last_account_update_time = Some(event.account_update_time);
        self.last_account_update_event_time = Some(event.event_time);
        self.last_event_time = Some(
            self.last_event_time
                .map_or(event.event_time, |last| last.max(event.event_time)),
        );
        BinanceUserDataApply::AppliedAccountUpdate
    }

    fn apply_balance(&mut self, balance: BinanceUserDataBalance) {
        self.balances.insert(
            balance.asset,
            BinanceUserDataBalanceState {
                free: balance.free,
                locked: balance.locked,
            },
        );
    }

    fn fail(&mut self, reason: BinanceUserDataReconcileReason) -> BinanceUserDataApply {
        self.reconcile_required = Some(reason.clone());
        BinanceUserDataApply::ReconcileRequired(reason)
    }
}
