use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant, SystemTime},
};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, OrderType, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    ExchangeAvailability, ExchangeError, ExchangeHandle, ExchangeMode, ReconcileReceipt,
    ReconcileScope, TradingCommand, TradingReceipt,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor},
};
use thiserror::Error;
use tokio::time::Instant as TokioInstant;
use uuid::Uuid;

use crate::ExecutionMode;

/// Hard resource limits at the order execution boundary.
pub const MAX_EXECUTION_BATCH_ORDERS: usize = 256;
pub const MAX_EXECUTION_POLICY_SNAPSHOTS: usize = 4_096;

/// Supplies logical UTC time for execution freshness checks.
///
/// Implementations used for replay or tests should advance only when their
/// owning scenario advances. The default policy clock anchors the caller's
/// logical `now` to a nondecreasing high-water mark over monotonic and wall
/// elapsed time, so suspend and wall-clock rollback both fail closed.
pub trait ExecutionClock: Debug + Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

trait ElapsedTimeSource: Debug + Send + Sync {
    fn monotonic_elapsed(&self) -> StdDuration;
    fn wall_elapsed(&self) -> Option<StdDuration>;
}

#[derive(Debug)]
struct SystemElapsedTimeSource {
    monotonic_anchor: Instant,
    wall_anchor: SystemTime,
}

impl SystemElapsedTimeSource {
    fn new() -> Self {
        Self {
            monotonic_anchor: Instant::now(),
            wall_anchor: SystemTime::now(),
        }
    }
}

impl ElapsedTimeSource for SystemElapsedTimeSource {
    fn monotonic_elapsed(&self) -> StdDuration {
        self.monotonic_anchor.elapsed()
    }

    fn wall_elapsed(&self) -> Option<StdDuration> {
        self.wall_anchor.elapsed().ok()
    }
}

#[derive(Debug)]
struct AnchoredExecutionClock<S = SystemElapsedTimeSource> {
    logical_now: DateTime<Utc>,
    elapsed_source: S,
    elapsed_high_water: Mutex<StdDuration>,
}

impl AnchoredExecutionClock<SystemElapsedTimeSource> {
    fn new(logical_now: DateTime<Utc>) -> Self {
        Self::with_source(logical_now, SystemElapsedTimeSource::new())
    }
}

impl<S> AnchoredExecutionClock<S> {
    fn with_source(logical_now: DateTime<Utc>, elapsed_source: S) -> Self {
        Self {
            logical_now,
            elapsed_source,
            elapsed_high_water: Mutex::new(StdDuration::ZERO),
        }
    }
}

impl<S> ExecutionClock for AnchoredExecutionClock<S>
where
    S: ElapsedTimeSource,
{
    fn now(&self) -> DateTime<Utc> {
        let monotonic_elapsed = self.elapsed_source.monotonic_elapsed();
        let wall_elapsed = self.elapsed_source.wall_elapsed().unwrap_or_default();
        let observed_elapsed = monotonic_elapsed.max(wall_elapsed);
        let mut high_water = self
            .elapsed_high_water
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *high_water = (*high_water).max(observed_elapsed);
        let elapsed = Duration::from_std(*high_water).unwrap_or(Duration::MAX);
        self.logical_now
            .checked_add_signed(elapsed)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }
}

/// A stable logical execution unit. The caller must persist this identifier and
/// the contained client order identifiers before crossing a live order seam.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionBatch {
    id: Uuid,
    intents: Vec<OrderIntent>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionBatchRecoveryPayload {
    id: Uuid,
    #[serde(deserialize_with = "deserialize_execution_intents")]
    intents: Vec<RecoveryOrderIntent>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RecoveryPrice {
    Value(Price),
    Null(()),
}

impl RecoveryPrice {
    const fn into_option(self) -> Option<Price> {
        match self {
            Self::Value(price) => Some(price),
            Self::Null(()) => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryOrderIntent {
    client_order_id: Uuid,
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
    side: Side,
    order_type: OrderType,
    quantity: Quantity,
    price: RecoveryPrice,
    reduce_only: bool,
    time_in_force: TimeInForce,
}

impl From<RecoveryOrderIntent> for OrderIntent {
    fn from(intent: RecoveryOrderIntent) -> Self {
        Self {
            client_order_id: intent.client_order_id,
            exchange: intent.exchange,
            symbol: intent.symbol,
            market_type: intent.market_type,
            side: intent.side,
            order_type: intent.order_type,
            quantity: intent.quantity,
            price: intent.price.into_option(),
            reduce_only: intent.reduce_only,
            time_in_force: intent.time_in_force,
        }
    }
}

fn deserialize_execution_intents<'de, D>(
    deserializer: D,
) -> Result<Vec<RecoveryOrderIntent>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ExecutionIntentsVisitor;

    impl<'de> Visitor<'de> for ExecutionIntentsVisitor {
        type Value = Vec<RecoveryOrderIntent>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                formatter,
                "at most {MAX_EXECUTION_BATCH_ORDERS} execution intents"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or(0);
            validate_execution_batch_order_count(capacity).map_err(serde::de::Error::custom)?;
            let mut intents = Vec::with_capacity(capacity);
            while let Some(intent) = sequence.next_element()? {
                let count = intents.len().saturating_add(1);
                validate_execution_batch_order_count(count).map_err(serde::de::Error::custom)?;
                intents.push(intent);
            }
            Ok(intents)
        }
    }

    deserializer.deserialize_seq(ExecutionIntentsVisitor)
}

impl<'de> Deserialize<'de> for ExecutionBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let payload = ExecutionBatchRecoveryPayload::deserialize(deserializer)?;
        let intents = payload.intents.into_iter().map(OrderIntent::from).collect();
        Self::new(payload.id, intents).map_err(serde::de::Error::custom)
    }
}

impl ExecutionBatch {
    /// Plans a new independently identified execution batch.
    ///
    /// The generated batch identifier is always non-nil. Existing client order
    /// identifiers are preserved and validated exactly as supplied.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for an oversized batch, an invalid intent, a
    /// nil client ID, or a duplicate client ID.
    pub fn planned(intents: Vec<OrderIntent>) -> Result<Self, RuntimeError> {
        let id = loop {
            let candidate = Uuid::new_v4();
            if !candidate.is_nil() {
                break candidate;
            }
        };
        Self::new(id, intents)
    }

    /// Builds a bounded batch with unique client order identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for a nil batch ID, an oversized batch, an
    /// invalid intent, a nil client ID, or a duplicate client ID.
    pub fn new(id: Uuid, intents: Vec<OrderIntent>) -> Result<Self, RuntimeError> {
        if id.is_nil() {
            return Err(RuntimeError::InvalidExecutionBatchId);
        }
        validate_execution_batch_order_count(intents.len())?;
        let mut client_ids = HashSet::with_capacity(intents.len());
        for (index, intent) in intents.iter().enumerate() {
            if intent.client_order_id.is_nil() {
                return Err(RuntimeError::InvalidClientOrderId);
            }
            if !client_ids.insert(intent.client_order_id) {
                return Err(RuntimeError::DuplicateClientOrderId(intent.client_order_id));
            }
            validate_execution_intent(index, intent)?;
        }
        Ok(Self { id, intents })
    }

    pub const fn id(&self) -> Uuid {
        self.id
    }

    pub fn intents(&self) -> &[OrderIntent] {
        &self.intents
    }
}

fn validate_execution_intent(index: usize, intent: &OrderIntent) -> Result<(), RuntimeError> {
    let invalid = |reason| RuntimeError::InvalidExecutionIntent { index, reason };
    if intent.exchange.trim().is_empty() {
        return Err(invalid("exchange must not be empty"));
    }
    if intent.quantity.as_decimal().is_zero() {
        return Err(invalid("quantity must be greater than zero"));
    }
    match (intent.order_type, intent.price) {
        (OrderType::Market, None) | (OrderType::Limit, Some(_)) => {}
        (OrderType::Market, Some(_)) => {
            return Err(invalid("market orders must not include a limit price"));
        }
        (OrderType::Limit, None) => {
            return Err(invalid("limit orders require a limit price"));
        }
    }
    if intent.order_type == OrderType::Market && intent.time_in_force == TimeInForce::PostOnly {
        return Err(invalid("market orders cannot be post-only"));
    }
    Ok(())
}

fn validate_execution_batch_order_count(count: usize) -> Result<(), RuntimeError> {
    if count > MAX_EXECUTION_BATCH_ORDERS {
        return Err(RuntimeError::BatchTooLarge {
            count,
            limit: MAX_EXECUTION_BATCH_ORDERS,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InstrumentKey {
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
}

impl InstrumentKey {
    fn from_snapshot(snapshot: &MarketSnapshot) -> Self {
        Self {
            exchange: snapshot.exchange().to_owned(),
            symbol: snapshot.symbol.clone(),
            market_type: snapshot.market_type,
        }
    }

    fn from_intent(intent: &OrderIntent) -> Self {
        Self {
            exchange: intent.exchange.clone(),
            symbol: intent.symbol.clone(),
            market_type: intent.market_type,
        }
    }
}

/// Fail-closed execution controls and per-instrument market freshness.
///
/// This is intentionally separate from exchange readiness: a fresh snapshot
/// for one symbol must never authorize an order for another symbol or product.
#[derive(Debug, Clone)]
pub struct ExecutionPolicy {
    enabled: bool,
    monitor_only: bool,
    now: DateTime<Utc>,
    clock: Arc<dyn ExecutionClock>,
    max_market_age: Duration,
    snapshots: HashMap<InstrumentKey, MarketSnapshot>,
}

impl ExecutionPolicy {
    /// Creates an immutable policy for one logical evaluation instant.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid bounds or duplicate instruments.
    pub fn new(
        enabled: bool,
        monitor_only: bool,
        now: DateTime<Utc>,
        max_market_age: Duration,
        snapshots: Vec<MarketSnapshot>,
    ) -> Result<Self, RuntimeError> {
        let clock = Arc::new(AnchoredExecutionClock::new(now));
        if max_market_age < Duration::zero() {
            return Err(RuntimeError::InvalidExecutionPolicy(
                "maximum market age must not be negative",
            ));
        }
        if snapshots.len() > MAX_EXECUTION_POLICY_SNAPSHOTS {
            return Err(RuntimeError::TooManyPolicySnapshots {
                count: snapshots.len(),
                limit: MAX_EXECUTION_POLICY_SNAPSHOTS,
            });
        }

        let mut by_instrument = HashMap::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let key = InstrumentKey::from_snapshot(&snapshot);
            if by_instrument.insert(key, snapshot).is_some() {
                return Err(RuntimeError::InvalidExecutionPolicy(
                    "duplicate market snapshot for one instrument",
                ));
            }
        }
        Ok(Self {
            enabled,
            monitor_only,
            now,
            clock,
            max_market_age,
            snapshots: by_instrument,
        })
    }

    /// Replaces the default monotonic clock with an injected logical clock.
    ///
    /// This is intended for deterministic replay and tests. Initial batch
    /// validation remains pinned to the constructor's logical `now`; the
    /// injected clock controls the freshness recheck immediately before each
    /// leg is submitted.
    #[must_use]
    pub fn with_clock<C>(mut self, clock: Arc<C>) -> Self
    where
        C: ExecutionClock + 'static,
    {
        self.clock = clock;
        self
    }

    /// Validates operator controls and every target instrument before any order
    /// in the batch is submitted.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed [`RuntimeError`] for disabled execution, missing
    /// target data, or stale/future market data.
    pub fn validate(&self, intents: &[OrderIntent]) -> Result<(), RuntimeError> {
        self.validate_at(intents, self.now)
    }

    fn validate_at(&self, intents: &[OrderIntent], now: DateTime<Utc>) -> Result<(), RuntimeError> {
        if !self.enabled {
            return Err(RuntimeError::ExecutionDisabled);
        }
        if self.monitor_only {
            return Err(RuntimeError::MonitorOnlyPolicy);
        }
        if intents.len() > MAX_EXECUTION_BATCH_ORDERS {
            return Err(RuntimeError::BatchTooLarge {
                count: intents.len(),
                limit: MAX_EXECUTION_BATCH_ORDERS,
            });
        }

        for intent in intents {
            let snapshot = self
                .snapshots
                .get(&InstrumentKey::from_intent(intent))
                .ok_or_else(|| RuntimeError::MissingMarketData {
                    exchange: intent.exchange.clone(),
                    symbol: intent.symbol.clone(),
                    market_type: intent.market_type,
                })?;
            if snapshot.timestamp > now {
                return Err(RuntimeError::FutureMarketData {
                    exchange: intent.exchange.clone(),
                    symbol: intent.symbol.clone(),
                    market_type: intent.market_type,
                    observed_at: snapshot.timestamp,
                    now,
                });
            }
            let age = now.signed_duration_since(snapshot.timestamp);
            if age > self.max_market_age {
                return Err(RuntimeError::StaleMarketData {
                    exchange: intent.exchange.clone(),
                    symbol: intent.symbol.clone(),
                    market_type: intent.market_type,
                    age,
                    limit: self.max_market_age,
                });
            }
        }
        Ok(())
    }

    fn current_time(&self) -> DateTime<Utc> {
        self.clock.now()
    }

    fn dispatch_deadline(&self, intent: &OrderIntent) -> Result<TokioInstant, RuntimeError> {
        let monotonic_now = TokioInstant::now();
        let logical_now = self.current_time();
        self.validate_at(std::slice::from_ref(intent), logical_now)?;

        let snapshot = self
            .snapshots
            .get(&InstrumentKey::from_intent(intent))
            .ok_or_else(|| RuntimeError::MissingMarketData {
                exchange: intent.exchange.clone(),
                symbol: intent.symbol.clone(),
                market_type: intent.market_type,
            })?;
        let maximum_age = self.max_market_age.to_std().map_err(|_| {
            RuntimeError::InvalidExecutionPolicy(
                "maximum market age cannot be represented by the monotonic clock",
            )
        })?;
        let current_age = logical_now
            .signed_duration_since(snapshot.timestamp)
            .to_std()
            .map_err(|_| {
                RuntimeError::InvalidExecutionPolicy(
                    "market-data age cannot be represented by the monotonic clock",
                )
            })?;
        let remaining =
            maximum_age
                .checked_sub(current_age)
                .ok_or_else(|| RuntimeError::StaleMarketData {
                    exchange: intent.exchange.clone(),
                    symbol: intent.symbol.clone(),
                    market_type: intent.market_type,
                    age: logical_now.signed_duration_since(snapshot.timestamp),
                    limit: self.max_market_age,
                })?;
        monotonic_now
            .checked_add(remaining)
            .ok_or(RuntimeError::InvalidExecutionPolicy(
                "market freshness deadline exceeds the monotonic clock range",
            ))
    }
}

/// Authoritative state collected immediately after a partial outcome.
#[derive(Debug)]
pub struct ReconciliationObservation {
    pub exchange: String,
    pub result: Result<ReconcileReceipt, ExchangeError>,
}

/// Crosses the exchange seam only after runtime authority, operator controls,
/// per-instrument freshness, and adapter mode all agree.
pub struct IntentExecutor<E> {
    exchange: Arc<E>,
    mode: ExecutionMode,
    policy: ExecutionPolicy,
}

impl<E> IntentExecutor<E>
where
    E: ExchangeHandle,
{
    pub const fn new(exchange: Arc<E>, mode: ExecutionMode, policy: ExecutionPolicy) -> Self {
        Self {
            exchange,
            mode,
            policy,
        }
    }

    /// Executes a stable batch in order after validating every precondition.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when authority or policy is absent, the adapter
    /// is not ready, or an exchange operation fails. Partial errors preserve
    /// the failed index, unattempted intents, and a reconciliation observation.
    pub async fn execute_batch(
        &self,
        batch: ExecutionBatch,
    ) -> Result<Vec<TradingReceipt>, RuntimeError> {
        let expected = execution_mode(self.mode)?;
        self.policy.validate(&batch.intents)?;
        if batch.intents.is_empty() {
            return Ok(Vec::new());
        }
        let expected_exchange = single_exchange_name(&batch.intents)?.to_owned();
        require_ready(self.exchange.as_ref(), expected, &expected_exchange).await?;

        let batch_id = batch.id;
        let mut receipts = Vec::with_capacity(batch.intents.len());
        let mut pending = batch.intents.into_iter().enumerate();
        while let Some((index, intent)) = pending.next() {
            let result = match self.policy.dispatch_deadline(&intent) {
                Err(source) => Err(source),
                Ok(deadline) => self
                    .exchange
                    .execute_before(TradingCommand::Submit(intent.clone()), deadline)
                    .await
                    .map_err(RuntimeError::from),
            };
            match result {
                Ok(receipt) => receipts.push(receipt),
                Err(source) => {
                    let unattempted = pending.map(|(_, item)| item).collect();
                    let reconciliation = vec![ReconciliationObservation {
                        exchange: intent.exchange.clone(),
                        result: self.exchange.reconcile(ReconcileScope::All).await,
                    }];
                    return Err(RuntimeError::PartialExecution {
                        batch_id,
                        failed_index: index,
                        completed: receipts,
                        failed_intent: Box::new(intent),
                        unattempted,
                        reconciliation,
                        source: Box::new(source),
                    });
                }
            }
        }
        Ok(receipts)
    }

    /// Compatibility wrapper for callers that do not need crash recovery.
    /// Recovery-sensitive code should create and persist an [`ExecutionBatch`]
    /// before calling [`Self::execute_batch`].
    ///
    /// # Errors
    ///
    /// Returns the same validation, adapter, or partial-execution errors as
    /// [`Self::execute_batch`].
    pub async fn execute_all(
        &self,
        intents: Vec<OrderIntent>,
    ) -> Result<Vec<TradingReceipt>, RuntimeError> {
        self.execute_batch(ExecutionBatch::planned(intents)?).await
    }
}

/// Routes strategy intents to named exchange adapters through one small seam.
pub struct ExchangeRouter {
    exchanges: HashMap<String, Arc<dyn ExchangeHandle>>,
    mode: ExecutionMode,
    policy: ExecutionPolicy,
}

impl ExchangeRouter {
    pub fn new(mode: ExecutionMode, policy: ExecutionPolicy) -> Self {
        Self {
            exchanges: HashMap::new(),
            mode,
            policy,
        }
    }

    pub fn register<E>(&mut self, name: impl Into<String>, exchange: Arc<E>)
    where
        E: ExchangeHandle + 'static,
    {
        self.exchanges.insert(name.into(), exchange);
    }

    /// Routes and executes one stable batch through its named adapters.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when policy, routing, readiness, or execution
    /// fails. Every adapter in a partial batch is reconciled before return.
    pub async fn execute_batch(
        &self,
        batch: ExecutionBatch,
    ) -> Result<Vec<TradingReceipt>, RuntimeError> {
        let expected = execution_mode(self.mode)?;
        self.policy.validate(&batch.intents)?;
        if batch.intents.is_empty() {
            return Ok(Vec::new());
        }
        let mut routed = Vec::with_capacity(batch.intents.len());
        for (index, intent) in batch.intents.into_iter().enumerate() {
            let exchange_name = intent.exchange.clone();
            let exchange = self
                .exchanges
                .get(&exchange_name)
                .ok_or_else(|| RuntimeError::UnknownExchange(exchange_name.clone()))?;
            routed.push((index, intent, Arc::clone(exchange)));
        }

        let mut validated = HashSet::new();
        for (_, intent, exchange) in &routed {
            if validated.insert(intent.exchange.clone()) {
                require_ready(exchange.as_ref(), expected, &intent.exchange).await?;
            }
        }
        let mut exchange_names = validated.into_iter().collect::<Vec<_>>();
        exchange_names.sort();

        let batch_id = batch.id;
        let mut receipts = Vec::with_capacity(routed.len());
        let mut pending = routed.into_iter();
        while let Some((index, intent, exchange)) = pending.next() {
            let result = match self.policy.dispatch_deadline(&intent) {
                Err(source) => Err(source),
                Ok(deadline) => exchange
                    .execute_before(TradingCommand::Submit(intent.clone()), deadline)
                    .await
                    .map_err(RuntimeError::from),
            };
            match result {
                Ok(receipt) => receipts.push(receipt),
                Err(source) => {
                    let unattempted = pending.map(|(_, item, _)| item).collect();
                    let mut reconciliation = Vec::with_capacity(exchange_names.len());
                    for name in &exchange_names {
                        let Some(adapter) = self.exchanges.get(name) else {
                            reconciliation.push(ReconciliationObservation {
                                exchange: name.clone(),
                                result: Err(ExchangeError::unavailable(
                                    "validated exchange registration disappeared",
                                )),
                            });
                            continue;
                        };
                        reconciliation.push(ReconciliationObservation {
                            exchange: name.clone(),
                            result: adapter.reconcile(ReconcileScope::All).await,
                        });
                    }
                    return Err(RuntimeError::PartialExecution {
                        batch_id,
                        failed_index: index,
                        completed: receipts,
                        failed_intent: Box::new(intent),
                        unattempted,
                        reconciliation,
                        source: Box::new(source),
                    });
                }
            }
        }
        Ok(receipts)
    }

    /// Compatibility wrapper for non-recoverable paper callers.
    ///
    /// # Errors
    ///
    /// Returns the same validation, routing, adapter, or partial-execution
    /// errors as [`Self::execute_batch`].
    pub async fn execute_all(
        &self,
        intents: Vec<OrderIntent>,
    ) -> Result<Vec<TradingReceipt>, RuntimeError> {
        self.execute_batch(ExecutionBatch::planned(intents)?).await
    }
}

fn single_exchange_name(intents: &[OrderIntent]) -> Result<&str, RuntimeError> {
    let Some(first) = intents.first() else {
        return Err(RuntimeError::InvalidExecutionPolicy(
            "single-adapter execution requires a non-empty batch",
        ));
    };
    for (index, intent) in intents.iter().enumerate().skip(1) {
        if intent.exchange != first.exchange {
            return Err(RuntimeError::MixedExchangeBatch {
                index,
                expected: first.exchange.clone(),
                actual: intent.exchange.clone(),
            });
        }
    }
    Ok(&first.exchange)
}

fn execution_mode(mode: ExecutionMode) -> Result<ExchangeMode, RuntimeError> {
    match mode {
        ExecutionMode::Paper => Ok(ExchangeMode::Paper),
        ExecutionMode::Monitor => Err(RuntimeError::ModeDisallowsOrders),
        ExecutionMode::Live(_) => Err(RuntimeError::LiveExecutionUnavailable),
    }
}

async fn require_ready(
    exchange: &dyn ExchangeHandle,
    expected: ExchangeMode,
    expected_exchange: &str,
) -> Result<(), RuntimeError> {
    let status = exchange.status().await?;
    if status.exchange != expected_exchange {
        return Err(RuntimeError::AdapterIdentityMismatch {
            expected: expected_exchange.to_owned(),
            actual: status.exchange,
        });
    }
    if status.mode != expected {
        return Err(RuntimeError::AdapterModeMismatch {
            expected,
            actual: status.mode,
        });
    }
    if status.availability != ExchangeAvailability::Ready {
        return Err(RuntimeError::AdapterUnavailable {
            exchange: status.exchange,
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("monitor mode cannot submit or cancel orders")]
    ModeDisallowsOrders,
    #[error("execution is disabled by operator policy")]
    ExecutionDisabled,
    #[error("monitor-only policy cannot submit orders")]
    MonitorOnlyPolicy,
    #[error(
        "autonomous strategy live execution stays closed until the strategy promotion gate \
         passes; the only mainnet order authority is the acknowledged one-shot `live-lifecycle` \
         command"
    )]
    LiveExecutionUnavailable,
    #[error("invalid execution policy: {0}")]
    InvalidExecutionPolicy(&'static str),
    #[error("execution batch has {count} orders; maximum is {limit}")]
    BatchTooLarge { count: usize, limit: usize },
    #[error("execution batch id must not be nil")]
    InvalidExecutionBatchId,
    #[error("execution policy has {count} snapshots; maximum is {limit}")]
    TooManyPolicySnapshots { count: usize, limit: usize },
    #[error("client order id {0} appears more than once in one execution batch")]
    DuplicateClientOrderId(Uuid),
    #[error("client order id must not be nil")]
    InvalidClientOrderId,
    #[error("execution intent {index} is invalid: {reason}")]
    InvalidExecutionIntent { index: usize, reason: &'static str },
    #[error(
        "single-adapter execution intent {index} targets {actual}, but the batch targets {expected}"
    )]
    MixedExchangeBatch {
        index: usize,
        expected: String,
        actual: String,
    },
    #[error("no exchange adapter is registered for {0}")]
    UnknownExchange(String),
    #[error("runtime expected exchange adapter {expected}, but adapter reports {actual}")]
    AdapterIdentityMismatch { expected: String, actual: String },
    #[error("runtime expected a {expected:?} adapter but received {actual:?}")]
    AdapterModeMismatch {
        expected: ExchangeMode,
        actual: ExchangeMode,
    },
    #[error("exchange adapter {exchange} is not ready")]
    AdapterUnavailable { exchange: String },
    #[error("no market snapshot authorizes {exchange}/{symbol}/{market_type:?}")]
    MissingMarketData {
        exchange: String,
        symbol: Symbol,
        market_type: MarketType,
    },
    #[error(
        "market snapshot for {exchange}/{symbol}/{market_type:?} is from the future ({observed_at} > {now})"
    )]
    FutureMarketData {
        exchange: String,
        symbol: Symbol,
        market_type: MarketType,
        observed_at: DateTime<Utc>,
        now: DateTime<Utc>,
    },
    #[error(
        "market snapshot for {exchange}/{symbol}/{market_type:?} is stale ({age:?} > {limit:?})"
    )]
    StaleMarketData {
        exchange: String,
        symbol: Symbol,
        market_type: MarketType,
        age: Duration,
        limit: Duration,
    },
    #[error(
        "batch {batch_id} stopped at leg {failed_index}; preserve completed receipts and reconciliation before retrying: {source}"
    )]
    PartialExecution {
        batch_id: Uuid,
        failed_index: usize,
        completed: Vec<TradingReceipt>,
        failed_intent: Box<OrderIntent>,
        unattempted: Vec<OrderIntent>,
        reconciliation: Vec<ReconciliationObservation>,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error(transparent)]
    Exchange(Box<ExchangeError>),
}

impl From<ExchangeError> for RuntimeError {
    fn from(source: ExchangeError) -> Self {
        Self::Exchange(Box::new(source))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration as StdDuration,
    };

    use chrono::{Duration, TimeZone, Utc};

    use super::{AnchoredExecutionClock, ElapsedTimeSource, ExecutionClock};

    #[derive(Clone, Debug)]
    struct FakeElapsedTimeSource {
        sample: Arc<Mutex<(StdDuration, Option<StdDuration>)>>,
    }

    impl FakeElapsedTimeSource {
        fn new() -> Self {
            Self {
                sample: Arc::new(Mutex::new((StdDuration::ZERO, Some(StdDuration::ZERO)))),
            }
        }

        fn set(&self, monotonic: StdDuration, wall: Option<StdDuration>) {
            *self.sample.lock().unwrap() = (monotonic, wall);
        }
    }

    impl ElapsedTimeSource for FakeElapsedTimeSource {
        fn monotonic_elapsed(&self) -> StdDuration {
            self.sample.lock().unwrap().0
        }

        fn wall_elapsed(&self) -> Option<StdDuration> {
            self.sample.lock().unwrap().1
        }
    }

    #[test]
    fn anchored_clock_counts_suspend_and_never_rewinds_after_wall_clock_changes() {
        let logical_now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        let source = FakeElapsedTimeSource::new();
        let clock = AnchoredExecutionClock::with_source(logical_now, source.clone());
        assert_eq!(clock.now(), logical_now);

        source.set(
            StdDuration::from_secs(5),
            Some(StdDuration::from_secs(2 * 60 * 60)),
        );
        assert_eq!(clock.now(), logical_now + Duration::hours(2));

        source.set(StdDuration::from_secs(10), None);
        assert_eq!(clock.now(), logical_now + Duration::hours(2));

        source.set(
            StdDuration::from_secs(2 * 60 * 60 + 1),
            Some(StdDuration::from_secs(60)),
        );
        assert_eq!(
            clock.now(),
            logical_now + Duration::hours(2) + Duration::seconds(1)
        );
    }
}
