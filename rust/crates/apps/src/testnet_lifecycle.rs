use std::{fmt, future::Future, io, path::Path, pin::Pin, time::Duration};

use chrono::Utc;
use crypto_trading_domain::{
    MarketType, Order, OrderIntent, OrderStatus, OrderType, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceTestnetExchange, ExchangeError, ExchangeHandle, TradingCommand, TradingReceipt,
};
use crypto_trading_runtime::{
    DecisionRecord, FileJournalSnapshotSource, HistoryError, JournalPageBoundary, JournalReadError,
    JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader,
};
use serde_json::{Value, json};
use tokio::time::sleep;
use uuid::Uuid;

pub const TESTNET_LIFECYCLE_SCHEMA_VERSION: u16 = 1;
pub const TESTNET_LIFECYCLE_ACKNOWLEDGEMENT: &str = "I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE";

const STRATEGY: &str = "binance_testnet_lifecycle";
const PLANNED: &str = "testnet_lifecycle_planned";
const RESUMED: &str = "testnet_lifecycle_resumed";
const QUERY_PLANNED: &str = "testnet_lifecycle_query_planned";
const SUBMIT_OBSERVED: &str = "testnet_lifecycle_submit_observed";
const QUERY_OBSERVED: &str = "testnet_lifecycle_query_observed";
const CANCEL_PLANNED: &str = "testnet_lifecycle_cancel_planned";
const CANCEL_OBSERVED: &str = "testnet_lifecycle_cancel_observed";
const OUTCOME_UNKNOWN: &str = "testnet_lifecycle_outcome_unknown";
const COMPLETED: &str = "testnet_lifecycle_completed";
const FAILED: &str = "testnet_lifecycle_failed";
const MAX_CAMPAIGN_ID_BYTES: usize = 128;
const MAX_QUERY_ATTEMPTS_PER_CAMPAIGN: u16 = 1_000;
const MIN_QUERY_ATTEMPTS_PER_CAMPAIGN: u16 = 3;
const CLEANUP_QUERY_RESERVE: u32 = 2;
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(60);
const PROJECTION_JOURNAL_ID: Uuid = Uuid::from_u128(0x7e57_1f3c_6c79_4ec8_9ec0_98a7_bbe7_a101);

pub type TestnetLifecycleVenueFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ExchangeError>> + Send + 'a>>;

/// Narrow mutation/query seam used by the lifecycle owner and offline tests.
pub trait TestnetLifecycleVenue: Send + Sync {
    fn submit(&self, intent: OrderIntent) -> TestnetLifecycleVenueFuture<'_, Order>;

    fn query(
        &self,
        symbol: Symbol,
        market_type: MarketType,
        client_order_id: Uuid,
    ) -> TestnetLifecycleVenueFuture<'_, Order>;

    fn cancel(&self, order_id: String) -> TestnetLifecycleVenueFuture<'_, Order>;
}

impl TestnetLifecycleVenue for BinanceTestnetExchange {
    fn submit(&self, intent: OrderIntent) -> TestnetLifecycleVenueFuture<'_, Order> {
        Box::pin(async move {
            let receipt = self.execute(TradingCommand::Submit(intent)).await?;
            submitted_order(receipt)
        })
    }

    fn query(
        &self,
        symbol: Symbol,
        market_type: MarketType,
        client_order_id: Uuid,
    ) -> TestnetLifecycleVenueFuture<'_, Order> {
        Box::pin(async move {
            BinanceTestnetExchange::query_order(self, &symbol, market_type, client_order_id).await
        })
    }

    fn cancel(&self, order_id: String) -> TestnetLifecycleVenueFuture<'_, Order> {
        Box::pin(async move {
            let receipt = self.execute(TradingCommand::Cancel { order_id }).await?;
            cancelled_order(receipt)
        })
    }
}

/// Observation that must be proven by a single-order query before cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TestnetLifecycleObservation {
    Open,
    PartiallyFilled,
}

impl TestnetLifecycleObservation {
    const fn label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::PartiallyFilled => "partially_filled",
        }
    }

    const fn matches(self, status: OrderStatus) -> bool {
        matches!(
            (self, status),
            (Self::Open, OrderStatus::Open) | (Self::PartiallyFilled, OrderStatus::PartiallyFilled)
        )
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "partially_filled" => Some(Self::PartiallyFilled),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancelReason {
    ObservationSatisfied,
    ObservationNotReached,
}

impl CancelReason {
    const fn label(self) -> &'static str {
        match self {
            Self::ObservationSatisfied => "observation_satisfied",
            Self::ObservationNotReached => "observation_not_reached",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "observation_satisfied" => Some(Self::ObservationSatisfied),
            "observation_not_reached" => Some(Self::ObservationNotReached),
            _ => None,
        }
    }
}

/// Immutable identity and bounded polling policy for one real Testnet round trip.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestnetLifecycleConfig {
    campaign_id: String,
    intent: OrderIntent,
    expected_observation: TestnetLifecycleObservation,
    poll_interval: Duration,
    maximum_queries: u16,
}

impl TestnetLifecycleConfig {
    /// Builds one recovery-safe lifecycle configuration.
    ///
    /// # Errors
    ///
    /// Returns [`TestnetLifecycleError::InvalidConfig`] unless the campaign
    /// identity, UUID-backed Binance limit order, and polling bounds are safe.
    pub fn new(
        campaign_id: impl Into<String>,
        intent: OrderIntent,
        expected_observation: TestnetLifecycleObservation,
        poll_interval: Duration,
        maximum_queries: u16,
    ) -> Result<Self, TestnetLifecycleError> {
        let campaign_id = campaign_id.into();
        if !valid_campaign_id(&campaign_id)
            || !intent.exchange.eq_ignore_ascii_case("binance")
            || intent.client_order_id.is_nil()
            || intent.order_type != OrderType::Limit
            || intent.price.is_none()
            || intent.quantity.as_decimal().is_zero()
            || !matches!(
                intent.time_in_force,
                TimeInForce::Gtc | TimeInForce::PostOnly
            )
            || poll_interval.is_zero()
            || poll_interval > MAX_POLL_INTERVAL
            || !(MIN_QUERY_ATTEMPTS_PER_CAMPAIGN..=MAX_QUERY_ATTEMPTS_PER_CAMPAIGN)
                .contains(&maximum_queries)
        {
            return Err(TestnetLifecycleError::InvalidConfig);
        }
        Ok(Self {
            campaign_id,
            intent,
            expected_observation,
            poll_interval,
            maximum_queries,
        })
    }

    #[must_use]
    pub fn campaign_id(&self) -> &str {
        &self.campaign_id
    }

    #[must_use]
    pub const fn intent(&self) -> &OrderIntent {
        &self.intent
    }

    #[must_use]
    pub const fn expected_observation(&self) -> TestnetLifecycleObservation {
        self.expected_observation
    }
}

/// Durable proof returned only after a final query observes cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestnetLifecycleReport {
    pub campaign_id: String,
    pub client_order_id: Uuid,
    pub server_order_id: String,
    pub expected_observation: TestnetLifecycleObservation,
    pub final_status: OrderStatus,
    pub query_count: u32,
    pub recovered: bool,
}

/// Runs or recovers one submit-query-cancel-query Testnet lifecycle.
///
/// A fresh run appends `planned` before submission. Any later run that sees
/// that fact queries by the same client ID first and never resubmits.
///
/// # Errors
///
/// Returns a bounded failure after recording the safest durable fact possible.
/// Mutating ambiguity is never reported as a definite failure.
pub async fn run_testnet_lifecycle<V>(
    config: &TestnetLifecycleConfig,
    venue: &V,
    history: &JsonlHistory,
) -> Result<TestnetLifecycleReport, TestnetLifecycleError>
where
    V: TestnetLifecycleVenue + ?Sized,
{
    let projected = project_campaign(history.path(), config)?;
    if let Some(order) = projected.completed {
        return Ok(report(config, &order, projected.query_count, true));
    }
    if projected.failed {
        return Err(TestnetLifecycleError::PreviouslyFailed);
    }
    let active = establish_active_lifecycle(config, venue, history, projected).await?;
    finish_active_lifecycle(config, venue, history, active).await
}

async fn establish_active_lifecycle<V>(
    config: &TestnetLifecycleConfig,
    venue: &V,
    history: &JsonlHistory,
    projected: ProjectedCampaign,
) -> Result<ActiveLifecycle, TestnetLifecycleError>
where
    V: TestnetLifecycleVenue + ?Sized,
{
    let recovered = projected.planned;
    let mut query_count = projected.query_count;
    let cancel_reason = projected.cancel_reason;
    let mut observation_satisfied = projected.observation_satisfied;
    let order = if recovered {
        history
            .append(&lifecycle_record(
                config,
                RESUMED,
                "recovery_started",
                json!({}),
            ))
            .await?;
        let order =
            query_and_record(venue, config, history, &mut query_count, "recovery_query").await?;
        observation_satisfied |= config.expected_observation.matches(order.status);
        order
    } else {
        history
            .append(&lifecycle_record(
                config,
                PLANNED,
                "planned",
                json!({
                    "intent": config.intent,
                    "expected_observation": config.expected_observation.label(),
                    "poll_interval_ms": poll_interval_millis(config)?,
                    "maximum_queries": config.maximum_queries,
                }),
            ))
            .await?;
        match venue.submit(config.intent.clone()).await {
            Ok(order) => {
                validate_order(config, &order, None)?;
                history
                    .append(&order_record(config, SUBMIT_OBSERVED, "submitted", &order))
                    .await?;
                order
            }
            Err(error @ ExchangeError::AmbiguousOutcome { .. }) => {
                append_outcome_unknown(config, history, "submit_dispatch", &error).await?;
                let order = query_and_record(
                    venue,
                    config,
                    history,
                    &mut query_count,
                    "submit_recovery_query",
                )
                .await?;
                observation_satisfied |= config.expected_observation.matches(order.status);
                order
            }
            Err(error) => {
                append_failed(config, history, "submit_rejected", None, Some(&error)).await?;
                return Err(TestnetLifecycleError::Exchange(error));
            }
        }
    };
    Ok(ActiveLifecycle {
        recovered,
        query_count,
        cancel_reason,
        observation_satisfied,
        order,
    })
}

async fn finish_active_lifecycle<V>(
    config: &TestnetLifecycleConfig,
    venue: &V,
    history: &JsonlHistory,
    active: ActiveLifecycle,
) -> Result<TestnetLifecycleReport, TestnetLifecycleError>
where
    V: TestnetLifecycleVenue + ?Sized,
{
    let ActiveLifecycle {
        recovered,
        mut query_count,
        cancel_reason,
        mut observation_satisfied,
        mut order,
    } = active;
    if let Some(reason) = cancel_reason {
        let final_order = cancel_and_confirm(
            venue,
            config,
            history,
            order,
            &mut query_count,
            true,
            reason,
        )
        .await?;
        return finish_planned_cleanup(
            config,
            history,
            &final_order,
            query_count,
            recovered,
            reason,
        )
        .await;
    }

    if !recovered && query_count == 0 {
        order = query_and_record(
            venue,
            config,
            history,
            &mut query_count,
            "post_submit_query",
        )
        .await?;
        observation_satisfied |= config.expected_observation.matches(order.status);
    }

    while !observation_satisfied
        && is_active_status(order.status)
        && has_observation_query_budget(config, query_count)
    {
        sleep(config.poll_interval).await;
        order = query_and_record(
            venue,
            config,
            history,
            &mut query_count,
            "observation_query",
        )
        .await?;
        observation_satisfied |= config.expected_observation.matches(order.status);
    }
    if !observation_satisfied {
        return fail_missed_observation(venue, config, history, order, &mut query_count).await;
    }
    let final_order = cancel_and_confirm(
        venue,
        config,
        history,
        order,
        &mut query_count,
        false,
        CancelReason::ObservationSatisfied,
    )
    .await?;
    append_completed(config, history, &final_order).await?;
    Ok(report(config, &final_order, query_count, recovered))
}

async fn finish_planned_cleanup(
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    final_order: &Order,
    query_count: u32,
    recovered: bool,
    reason: CancelReason,
) -> Result<TestnetLifecycleReport, TestnetLifecycleError> {
    match reason {
        CancelReason::ObservationSatisfied => {
            append_completed(config, history, final_order).await?;
            Ok(report(config, final_order, query_count, recovered))
        }
        CancelReason::ObservationNotReached => {
            append_failed(
                config,
                history,
                "expected_observation_not_reached",
                Some(final_order),
                None,
            )
            .await?;
            Err(TestnetLifecycleError::ObservationNotReached)
        }
    }
}

async fn fail_missed_observation<V>(
    venue: &V,
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    order: Order,
    query_count: &mut u32,
) -> Result<TestnetLifecycleReport, TestnetLifecycleError>
where
    V: TestnetLifecycleVenue + ?Sized,
{
    if is_active_status(order.status) {
        let final_order = cancel_and_confirm(
            venue,
            config,
            history,
            order,
            query_count,
            false,
            CancelReason::ObservationNotReached,
        )
        .await?;
        append_failed(
            config,
            history,
            "expected_observation_not_reached",
            Some(&final_order),
            None,
        )
        .await?;
        return Err(TestnetLifecycleError::ObservationNotReached);
    }
    append_failed(
        config,
        history,
        "unexpected_terminal_order",
        Some(&order),
        None,
    )
    .await?;
    Err(TestnetLifecycleError::UnsafeTerminal(order.status))
}

async fn query_and_record<V>(
    venue: &V,
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    query_count: &mut u32,
    phase: &str,
) -> Result<Order, TestnetLifecycleError>
where
    V: TestnetLifecycleVenue + ?Sized,
{
    let maximum_queries = u32::from(config.maximum_queries);
    if *query_count >= maximum_queries {
        append_query_budget_exhausted(config, history, phase, *query_count).await?;
        return Err(TestnetLifecycleError::QueryBudgetExhausted);
    }
    let query_sequence = query_count
        .checked_add(1)
        .ok_or(TestnetLifecycleError::CounterOverflow)?;
    history
        .append(&lifecycle_record(
            config,
            QUERY_PLANNED,
            phase,
            json!({ "query_sequence": query_sequence }),
        ))
        .await?;
    *query_count = query_sequence;

    let result = venue
        .query(
            config.intent.symbol.clone(),
            config.intent.market_type,
            config.intent.client_order_id,
        )
        .await;
    let order = match result {
        Ok(order) => order,
        Err(error) => {
            append_outcome_unknown(config, history, phase, &error).await?;
            return Err(TestnetLifecycleError::OutcomeUnknown);
        }
    };
    validate_order(config, &order, None)?;
    history
        .append(&query_order_record(config, phase, &order, query_sequence))
        .await?;
    Ok(order)
}

async fn cancel_and_confirm<V>(
    venue: &V,
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    current: Order,
    query_count: &mut u32,
    cancel_already_planned: bool,
    cancel_reason: CancelReason,
) -> Result<Order, TestnetLifecycleError>
where
    V: TestnetLifecycleVenue + ?Sized,
{
    validate_order(config, &current, None)?;
    if current.status == OrderStatus::Cancelled {
        if cancel_already_planned {
            return Ok(current);
        }
        append_failed(
            config,
            history,
            "order_cancelled_before_owner_cleanup",
            Some(&current),
            None,
        )
        .await?;
        return Err(TestnetLifecycleError::UnsafeTerminal(current.status));
    }
    if !is_active_status(current.status) {
        append_failed(
            config,
            history,
            "order_terminal_before_cancel",
            Some(&current),
            None,
        )
        .await?;
        return Err(TestnetLifecycleError::UnsafeTerminal(current.status));
    }
    if *query_count >= u32::from(config.maximum_queries) {
        append_query_budget_exhausted(config, history, "cancel_confirmation", *query_count).await?;
        return Err(TestnetLifecycleError::QueryBudgetExhausted);
    }
    if !cancel_already_planned {
        history
            .append(&lifecycle_record(
                config,
                CANCEL_PLANNED,
                "cancel_planned",
                json!({
                    "server_order_id": current.id,
                    "reason": cancel_reason.label(),
                }),
            ))
            .await?;
    }

    let cancel_result = venue.cancel(current.id.clone()).await;
    match cancel_result {
        Ok(cancelled) => {
            validate_order(config, &cancelled, Some(&current.id))?;
            history
                .append(&order_record(
                    config,
                    CANCEL_OBSERVED,
                    "cancel_observed",
                    &cancelled,
                ))
                .await?;
        }
        Err(error) => {
            append_outcome_unknown(config, history, "cancel_dispatch", &error).await?;
        }
    }

    let final_order =
        query_and_record(venue, config, history, query_count, "post_cancel_query").await?;
    if final_order.id != current.id {
        return Err(TestnetLifecycleError::ProtocolViolation);
    }
    match final_order.status {
        OrderStatus::Cancelled => Ok(final_order),
        OrderStatus::Filled | OrderStatus::Rejected => {
            append_failed(
                config,
                history,
                "order_terminal_during_cancel",
                Some(&final_order),
                None,
            )
            .await?;
            Err(TestnetLifecycleError::UnsafeTerminal(final_order.status))
        }
        OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled => {
            history
                .append(&lifecycle_record(
                    config,
                    OUTCOME_UNKNOWN,
                    "cancel_not_terminal",
                    json!({
                        "server_order_id": final_order.id,
                        "order_status": order_status_label(final_order.status),
                    }),
                ))
                .await?;
            Err(TestnetLifecycleError::OutcomeUnknown)
        }
    }
}

async fn append_completed(
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    order: &Order,
) -> Result<(), TestnetLifecycleError> {
    if order.status != OrderStatus::Cancelled {
        return Err(TestnetLifecycleError::ProtocolViolation);
    }
    history
        .append(&order_record(config, COMPLETED, "completed", order))
        .await?;
    Ok(())
}

async fn append_outcome_unknown(
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    phase: &str,
    error: &ExchangeError,
) -> Result<(), TestnetLifecycleError> {
    history
        .append(&lifecycle_record(
            config,
            OUTCOME_UNKNOWN,
            phase,
            json!({ "failure": exchange_failure_bucket(error) }),
        ))
        .await?;
    Ok(())
}

async fn append_query_budget_exhausted(
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    phase: &str,
    query_count: u32,
) -> Result<(), TestnetLifecycleError> {
    history
        .append(&lifecycle_record(
            config,
            OUTCOME_UNKNOWN,
            phase,
            json!({
                "failure": "query_budget_exhausted",
                "query_count": query_count,
                "maximum_queries": config.maximum_queries,
            }),
        ))
        .await?;
    Ok(())
}

async fn append_failed(
    config: &TestnetLifecycleConfig,
    history: &JsonlHistory,
    failure: &str,
    order: Option<&Order>,
    exchange_error: Option<&ExchangeError>,
) -> Result<(), TestnetLifecycleError> {
    let mut details = json!({ "failure": failure });
    let fields = details
        .as_object_mut()
        .ok_or(TestnetLifecycleError::ProtocolViolation)?;
    if let Some(order) = order {
        fields.insert("order".to_owned(), json!(order));
    }
    if let Some(error) = exchange_error {
        fields.insert(
            "exchange_failure".to_owned(),
            json!(exchange_failure_bucket(error)),
        );
    }
    history
        .append(&lifecycle_record(config, FAILED, "failed", details))
        .await?;
    Ok(())
}

fn lifecycle_record(
    config: &TestnetLifecycleConfig,
    decision: &str,
    phase: &str,
    extra: Value,
) -> DecisionRecord {
    let mut details = json!({
        "schema_version": TESTNET_LIFECYCLE_SCHEMA_VERSION,
        "campaign_id": config.campaign_id,
        "client_order_id": config.intent.client_order_id,
        "phase": phase,
    });
    if let (Some(fields), Value::Object(extra)) = (details.as_object_mut(), extra) {
        fields.extend(extra);
    }
    DecisionRecord {
        timestamp: Utc::now(),
        strategy: STRATEGY.to_owned(),
        symbol: config.intent.symbol.to_string(),
        decision: decision.to_owned(),
        details,
    }
}

fn order_record(
    config: &TestnetLifecycleConfig,
    decision: &str,
    phase: &str,
    order: &Order,
) -> DecisionRecord {
    lifecycle_record(
        config,
        decision,
        phase,
        json!({
            "order": order,
            "server_order_id": order.id,
            "order_status": order_status_label(order.status),
            "filled_quantity": order.filled_quantity,
        }),
    )
}

fn query_order_record(
    config: &TestnetLifecycleConfig,
    phase: &str,
    order: &Order,
    query_sequence: u32,
) -> DecisionRecord {
    lifecycle_record(
        config,
        QUERY_OBSERVED,
        phase,
        json!({
            "order": order,
            "server_order_id": order.id,
            "order_status": order_status_label(order.status),
            "filled_quantity": order.filled_quantity,
            "query_sequence": query_sequence,
        }),
    )
}

fn project_campaign(
    path: &Path,
    config: &TestnetLifecycleConfig,
) -> Result<ProjectedCampaign, TestnetLifecycleError> {
    if !path
        .try_exists()
        .map_err(TestnetLifecycleError::HistoryProbe)?
    {
        return Ok(ProjectedCampaign::default());
    }
    let source = FileJournalSnapshotSource::new(PROJECTION_JOURNAL_ID, path)?;
    let snapshot = source.snapshot()?;
    let mut projected = ProjectedCampaign::default();
    let mut cursor = None;
    loop {
        let page = LegacyJsonlJournalReader::read_page(&snapshot, cursor.as_ref())?;
        for event in page.events() {
            project_event(config, event.payload(), &mut projected)?;
        }
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => break,
            JournalPageBoundary::PageLimit => {
                cursor = page.next_cursor().cloned();
                if cursor.is_none() {
                    return Err(TestnetLifecycleError::CorruptHistory);
                }
            }
            JournalPageBoundary::PartialTail { .. } => {
                return Err(TestnetLifecycleError::CorruptHistory);
            }
        }
    }
    Ok(projected)
}

fn project_event(
    config: &TestnetLifecycleConfig,
    payload: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), TestnetLifecycleError> {
    if payload.get("strategy").and_then(Value::as_str) != Some(STRATEGY) {
        return Ok(());
    }
    let Some(details) = payload.get("details") else {
        return Err(TestnetLifecycleError::CorruptHistory);
    };
    if details.get("campaign_id").and_then(Value::as_str) != Some(&config.campaign_id) {
        return Ok(());
    }
    if details.get("schema_version").and_then(Value::as_u64)
        != Some(u64::from(TESTNET_LIFECYCLE_SCHEMA_VERSION))
        || details
            .get("client_order_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            != Some(config.intent.client_order_id)
    {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    let decision = payload
        .get("decision")
        .and_then(Value::as_str)
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    match decision {
        PLANNED => project_planned_event(config, details, projected)?,
        QUERY_PLANNED => project_query_plan(config, details, projected)?,
        SUBMIT_OBSERVED | QUERY_OBSERVED | CANCEL_OBSERVED | COMPLETED => {
            project_order_event(config, decision, details, projected)?;
        }
        CANCEL_PLANNED => project_cancel_plan(details, projected)?,
        RESUMED | OUTCOME_UNKNOWN => {
            if !projected.planned || projected.completed.is_some() || projected.failed {
                return Err(TestnetLifecycleError::CorruptHistory);
            }
        }
        FAILED => {
            if !projected.planned || projected.failed || projected.completed.is_some() {
                return Err(TestnetLifecycleError::CorruptHistory);
            }
            projected.failed = true;
        }
        _ => return Err(TestnetLifecycleError::CorruptHistory),
    }
    Ok(())
}

fn project_planned_event(
    config: &TestnetLifecycleConfig,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), TestnetLifecycleError> {
    if projected.planned {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    let intent = serde_json::from_value::<OrderIntent>(
        details
            .get("intent")
            .cloned()
            .ok_or(TestnetLifecycleError::CorruptHistory)?,
    )
    .map_err(|_| TestnetLifecycleError::CorruptHistory)?;
    let observation = details
        .get("expected_observation")
        .and_then(Value::as_str)
        .and_then(TestnetLifecycleObservation::parse)
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    let poll_interval_ms = details
        .get("poll_interval_ms")
        .and_then(Value::as_u64)
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    let maximum_queries = details
        .get("maximum_queries")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    if intent != config.intent
        || observation != config.expected_observation
        || poll_interval_ms != poll_interval_millis(config)?
        || maximum_queries != config.maximum_queries
    {
        return Err(TestnetLifecycleError::ConfigConflict);
    }
    projected.planned = true;
    Ok(())
}

fn project_query_plan(
    config: &TestnetLifecycleConfig,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), TestnetLifecycleError> {
    if !projected.planned || projected.completed.is_some() || projected.failed {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    let query_sequence = details
        .get("query_sequence")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    let expected_sequence = projected
        .query_count
        .checked_add(1)
        .ok_or(TestnetLifecycleError::CounterOverflow)?;
    if query_sequence != expected_sequence || query_sequence > u32::from(config.maximum_queries) {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    projected.query_count = query_sequence;
    Ok(())
}

fn project_cancel_plan(
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), TestnetLifecycleError> {
    if !projected.planned || projected.cancel_reason.is_some() {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    let server_order_id = details
        .get("server_order_id")
        .and_then(Value::as_str)
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    if projected.server_order_id.as_deref() != Some(server_order_id) {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    let reason = details
        .get("reason")
        .and_then(Value::as_str)
        .and_then(CancelReason::parse)
        .ok_or(TestnetLifecycleError::CorruptHistory)?;
    if (reason == CancelReason::ObservationSatisfied) != projected.observation_satisfied {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    projected.cancel_reason = Some(reason);
    Ok(())
}

fn project_order_event(
    config: &TestnetLifecycleConfig,
    decision: &str,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), TestnetLifecycleError> {
    if !projected.planned
        || (matches!(decision, CANCEL_OBSERVED | COMPLETED) && projected.cancel_reason.is_none())
        || projected.completed.is_some()
        || projected.failed
    {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    let order = projected_order(config, details)?;
    if let Some(existing_id) = projected.server_order_id.as_deref()
        && existing_id != order.id
    {
        return Err(TestnetLifecycleError::CorruptHistory);
    }
    projected.server_order_id = Some(order.id.clone());
    if decision == QUERY_OBSERVED {
        let query_sequence = details
            .get("query_sequence")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(TestnetLifecycleError::CorruptHistory)?;
        if query_sequence == 0
            || query_sequence > projected.query_count
            || projected
                .last_observed_query_sequence
                .is_some_and(|previous| query_sequence <= previous)
        {
            return Err(TestnetLifecycleError::CorruptHistory);
        }
        projected.last_observed_query_sequence = Some(query_sequence);
        if config.expected_observation.matches(order.status) {
            projected.observation_satisfied = true;
        }
    }
    if decision == COMPLETED {
        if order.status != OrderStatus::Cancelled
            || projected.cancel_reason != Some(CancelReason::ObservationSatisfied)
            || !projected.observation_satisfied
        {
            return Err(TestnetLifecycleError::CorruptHistory);
        }
        projected.completed = Some(order);
    }
    Ok(())
}

fn projected_order(
    config: &TestnetLifecycleConfig,
    details: &Value,
) -> Result<Order, TestnetLifecycleError> {
    let order = serde_json::from_value::<Order>(
        details
            .get("order")
            .cloned()
            .ok_or(TestnetLifecycleError::CorruptHistory)?,
    )
    .map_err(|_| TestnetLifecycleError::CorruptHistory)?;
    validate_order(config, &order, None)?;
    Ok(order)
}

fn validate_order(
    config: &TestnetLifecycleConfig,
    order: &Order,
    expected_server_id: Option<&str>,
) -> Result<(), TestnetLifecycleError> {
    if order.id.is_empty()
        || order.intent != config.intent
        || expected_server_id.is_some_and(|expected| expected != order.id)
    {
        return Err(TestnetLifecycleError::ProtocolViolation);
    }
    Ok(())
}

fn submitted_order(receipt: TradingReceipt) -> Result<Order, ExchangeError> {
    let TradingReceipt::Submitted { order, .. } = receipt else {
        return Err(ExchangeError::invalid(
            "Binance lifecycle submit returned a non-order receipt",
        ));
    };
    Ok(order)
}

fn cancelled_order(receipt: TradingReceipt) -> Result<Order, ExchangeError> {
    let TradingReceipt::Cancelled { mut orders, .. } = receipt else {
        return Err(ExchangeError::invalid(
            "Binance lifecycle cancel returned a non-cancellation receipt",
        ));
    };
    if orders.len() != 1 {
        return Err(ExchangeError::invalid(
            "Binance lifecycle single cancel must return exactly one order",
        ));
    }
    orders
        .pop()
        .ok_or_else(|| ExchangeError::invalid("Binance lifecycle cancel omitted its order"))
}

fn report(
    config: &TestnetLifecycleConfig,
    order: &Order,
    query_count: u32,
    recovered: bool,
) -> TestnetLifecycleReport {
    TestnetLifecycleReport {
        campaign_id: config.campaign_id.clone(),
        client_order_id: config.intent.client_order_id,
        server_order_id: order.id.clone(),
        expected_observation: config.expected_observation,
        final_status: order.status,
        query_count,
        recovered,
    }
}

const fn is_active_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Pending | OrderStatus::Open | OrderStatus::PartiallyFilled
    )
}

fn has_observation_query_budget(config: &TestnetLifecycleConfig, query_count: u32) -> bool {
    query_count.saturating_add(CLEANUP_QUERY_RESERVE) < u32::from(config.maximum_queries)
}

fn poll_interval_millis(config: &TestnetLifecycleConfig) -> Result<u64, TestnetLifecycleError> {
    u64::try_from(config.poll_interval.as_millis())
        .map_err(|_| TestnetLifecycleError::InvalidConfig)
}

const fn order_status_label(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Pending => "pending",
        OrderStatus::Open => "open",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
    }
}

fn valid_campaign_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CAMPAIGN_ID_BYTES
        && value.trim() == value
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

const fn exchange_failure_bucket(error: &ExchangeError) -> &'static str {
    match error {
        ExchangeError::InvalidRequest { .. } => "invalid_request",
        ExchangeError::Rejected { .. } => "rejected",
        ExchangeError::Unsupported { .. } => "unsupported",
        ExchangeError::Backpressure { .. } => "backpressure",
        ExchangeError::ResourceLimit { .. } => "resource_limit",
        ExchangeError::AmbiguousOutcome { .. } => "ambiguous_outcome",
        ExchangeError::Unavailable { .. } => "unavailable",
        ExchangeError::InvalidResponse { .. } => "invalid_response",
        ExchangeError::RemoteFailure { .. } => "remote_failure",
        ExchangeError::SubscriptionLagged { .. } => "subscription_lagged",
        ExchangeError::InvariantViolation { .. } => "invariant_violation",
    }
}

#[derive(Default)]
struct ProjectedCampaign {
    planned: bool,
    cancel_reason: Option<CancelReason>,
    observation_satisfied: bool,
    failed: bool,
    completed: Option<Order>,
    server_order_id: Option<String>,
    query_count: u32,
    last_observed_query_sequence: Option<u32>,
}

struct ActiveLifecycle {
    recovered: bool,
    query_count: u32,
    cancel_reason: Option<CancelReason>,
    observation_satisfied: bool,
    order: Order,
}

/// Bounded lifecycle failure. Display text intentionally excludes remote bodies
/// and credentials.
#[derive(Debug)]
pub enum TestnetLifecycleError {
    InvalidConfig,
    ConfigConflict,
    HistoryProbe(io::Error),
    HistoryRead(JournalReadError),
    HistoryWrite(HistoryError),
    CorruptHistory,
    Exchange(ExchangeError),
    ObservationNotReached,
    UnsafeTerminal(OrderStatus),
    OutcomeUnknown,
    QueryBudgetExhausted,
    PreviouslyFailed,
    ProtocolViolation,
    CounterOverflow,
}

impl fmt::Display for TestnetLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig => "invalid Binance testnet lifecycle configuration",
            Self::ConfigConflict => {
                "the durable testnet lifecycle identity conflicts with this configuration"
            }
            Self::HistoryProbe(_) => "unable to inspect the testnet lifecycle journal",
            Self::HistoryRead(_) => "unable to read bounded testnet lifecycle evidence",
            Self::HistoryWrite(_) => "unable to make the testnet lifecycle fact durable",
            Self::CorruptHistory => "testnet lifecycle evidence violates its fact contract",
            Self::Exchange(_) => "Binance testnet returned a definite lifecycle failure",
            Self::ObservationNotReached => {
                "the expected testnet order observation was not reached before cleanup"
            }
            Self::UnsafeTerminal(_) => {
                "the testnet order reached an unexpected terminal state"
            }
            Self::OutcomeUnknown => {
                "the testnet lifecycle outcome is unresolved; rerun the same campaign to query first"
            }
            Self::QueryBudgetExhausted => {
                "the durable testnet lifecycle query budget is exhausted; reconcile manually"
            }
            Self::PreviouslyFailed => "the testnet lifecycle campaign is already failed",
            Self::ProtocolViolation => "the testnet lifecycle response violated its identity contract",
            Self::CounterOverflow => "the testnet lifecycle evidence counter overflowed",
        })
    }
}

impl std::error::Error for TestnetLifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::HistoryProbe(error) => Some(error),
            Self::HistoryRead(error) => Some(error),
            Self::HistoryWrite(error) => Some(error),
            Self::Exchange(error) => Some(error),
            Self::InvalidConfig
            | Self::ConfigConflict
            | Self::CorruptHistory
            | Self::ObservationNotReached
            | Self::UnsafeTerminal(_)
            | Self::OutcomeUnknown
            | Self::QueryBudgetExhausted
            | Self::PreviouslyFailed
            | Self::ProtocolViolation
            | Self::CounterOverflow => None,
        }
    }
}

impl From<JournalReadError> for TestnetLifecycleError {
    fn from(error: JournalReadError) -> Self {
        Self::HistoryRead(error)
    }
}

impl From<HistoryError> for TestnetLifecycleError {
    fn from(error: HistoryError) -> Self {
        Self::HistoryWrite(error)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        str::FromStr,
        sync::{Mutex, MutexGuard},
    };

    use chrono::{TimeZone, Utc};
    use crypto_trading_domain::{
        MarketType, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol, TimeInForce,
    };
    use crypto_trading_exchange::{ExchangeOperation, ExchangeOperationKey};
    use rust_decimal::Decimal;

    use super::*;

    struct FakeVenue {
        submit: Mutex<VecDeque<Result<Order, ExchangeError>>>,
        query: Mutex<VecDeque<Result<Order, ExchangeError>>>,
        cancel: Mutex<VecDeque<Result<Order, ExchangeError>>>,
        calls: Mutex<Vec<&'static str>>,
    }

    impl FakeVenue {
        fn new(
            submit: Vec<Result<Order, ExchangeError>>,
            query: Vec<Result<Order, ExchangeError>>,
            cancel: Vec<Result<Order, ExchangeError>>,
        ) -> Self {
            Self {
                submit: Mutex::new(submit.into()),
                query: Mutex::new(query.into()),
                cancel: Mutex::new(cancel.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<&'static str> {
            lock(&self.calls).clone()
        }
    }

    impl TestnetLifecycleVenue for FakeVenue {
        fn submit(&self, _intent: OrderIntent) -> TestnetLifecycleVenueFuture<'_, Order> {
            lock(&self.calls).push("submit");
            let result = lock(&self.submit)
                .pop_front()
                .expect("missing submit fixture");
            Box::pin(async move { result })
        }

        fn query(
            &self,
            _symbol: Symbol,
            _market_type: MarketType,
            _client_order_id: Uuid,
        ) -> TestnetLifecycleVenueFuture<'_, Order> {
            lock(&self.calls).push("query");
            let result = lock(&self.query)
                .pop_front()
                .expect("missing query fixture");
            Box::pin(async move { result })
        }

        fn cancel(&self, _order_id: String) -> TestnetLifecycleVenueFuture<'_, Order> {
            lock(&self.calls).push("cancel");
            let result = lock(&self.cancel)
                .pop_front()
                .expect("missing cancel fixture");
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn fresh_campaign_proves_query_and_cancel_before_completion() {
        let config = config(TestnetLifecycleObservation::Open, "fresh");
        let history = test_history("fresh");
        let venue = FakeVenue::new(
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        );

        let report = run_testnet_lifecycle(&config, &venue, &history)
            .await
            .unwrap();

        assert_eq!(report.final_status, OrderStatus::Cancelled);
        assert_eq!(report.query_count, 2);
        assert!(!report.recovered);
        assert_eq!(venue.calls(), vec!["submit", "query", "cancel", "query"]);
        cleanup_history(history);
    }

    #[tokio::test]
    async fn partial_fill_is_observed_by_query_before_cleanup() {
        let config = config(TestnetLifecycleObservation::PartiallyFilled, "partial");
        let history = test_history("partial");
        let venue = FakeVenue::new(
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::PartiallyFilled, "0.0005")),
                Ok(order(&config, OrderStatus::Cancelled, "0.0005")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0.0005"))],
        );

        let report = run_testnet_lifecycle(&config, &venue, &history)
            .await
            .unwrap();

        assert_eq!(report.query_count, 3);
        assert_eq!(
            venue.calls(),
            vec!["submit", "query", "query", "cancel", "query"]
        );
        cleanup_history(history);
    }

    #[tokio::test]
    async fn restart_after_ambiguous_submit_queries_first_and_never_resubmits() {
        let config = config(TestnetLifecycleObservation::Open, "recover");
        let history = test_history("recover");
        let interrupted = FakeVenue::new(
            vec![Err(ambiguous_submit(&config))],
            vec![Err(ExchangeError::unavailable("fixture query unavailable"))],
            Vec::new(),
        );
        let first = run_testnet_lifecycle(&config, &interrupted, &history)
            .await
            .unwrap_err();
        assert!(matches!(first, TestnetLifecycleError::OutcomeUnknown));

        let recovered = FakeVenue::new(
            Vec::new(),
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        );
        let report = run_testnet_lifecycle(&config, &recovered, &history)
            .await
            .unwrap();

        assert!(report.recovered);
        assert_eq!(recovered.calls(), vec!["query", "cancel", "query"]);
        cleanup_history(history);
    }

    #[tokio::test]
    async fn recovery_cannot_turn_failed_observation_cleanup_into_completion() {
        let config = config(
            TestnetLifecycleObservation::PartiallyFilled,
            "failed-cleanup",
        );
        let history = test_history("failed-cleanup");
        let interrupted = FakeVenue::new(
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Open, "0")),
                Err(ExchangeError::unavailable(
                    "fixture cancel confirmation unavailable",
                )),
            ],
            vec![Err(ambiguous_cancel(&config))],
        );

        let first = run_testnet_lifecycle(&config, &interrupted, &history)
            .await
            .unwrap_err();
        assert!(matches!(first, TestnetLifecycleError::OutcomeUnknown));

        let recovered = FakeVenue::new(
            Vec::new(),
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
            Vec::new(),
        );
        let second = run_testnet_lifecycle(&config, &recovered, &history)
            .await
            .unwrap_err();

        assert!(matches!(
            second,
            TestnetLifecycleError::ObservationNotReached
        ));
        assert_eq!(recovered.calls(), vec!["query"]);
        let body = std::fs::read_to_string(history.path()).unwrap();
        assert!(body.contains(FAILED));
        assert!(!body.contains(COMPLETED));
        cleanup_history(history);
    }

    #[tokio::test]
    async fn durable_campaign_rejects_polling_policy_drift_before_remote_calls() {
        let config = config(TestnetLifecycleObservation::Open, "policy");
        let history = test_history("policy");
        let interrupted = FakeVenue::new(
            vec![Err(ambiguous_submit(&config))],
            vec![Err(ExchangeError::unavailable("fixture query unavailable"))],
            Vec::new(),
        );
        let first = run_testnet_lifecycle(&config, &interrupted, &history)
            .await
            .unwrap_err();
        assert!(matches!(first, TestnetLifecycleError::OutcomeUnknown));

        let mut changed = config.clone();
        changed.poll_interval = Duration::from_millis(2);
        let no_remote = FakeVenue::new(Vec::new(), Vec::new(), Vec::new());
        let second = run_testnet_lifecycle(&changed, &no_remote, &history)
            .await
            .unwrap_err();

        assert!(matches!(second, TestnetLifecycleError::ConfigConflict));
        assert!(no_remote.calls().is_empty());
        cleanup_history(history);
    }

    #[tokio::test]
    async fn query_attempt_budget_is_cumulative_across_restarts() {
        let mut config = config(TestnetLifecycleObservation::Open, "query-budget");
        config.maximum_queries = 3;
        let history = test_history("query-budget");
        let first_venue = FakeVenue::new(
            vec![Err(ambiguous_submit(&config))],
            vec![Err(ExchangeError::unavailable("fixture query one"))],
            Vec::new(),
        );
        let first = run_testnet_lifecycle(&config, &first_venue, &history)
            .await
            .unwrap_err();
        assert!(matches!(first, TestnetLifecycleError::OutcomeUnknown));

        for failure in ["fixture query two", "fixture query three"] {
            let venue = FakeVenue::new(
                Vec::new(),
                vec![Err(ExchangeError::unavailable(failure))],
                Vec::new(),
            );
            let error = run_testnet_lifecycle(&config, &venue, &history)
                .await
                .unwrap_err();
            assert!(matches!(error, TestnetLifecycleError::OutcomeUnknown));
            assert_eq!(venue.calls(), vec!["query"]);
        }

        let exhausted = FakeVenue::new(Vec::new(), Vec::new(), Vec::new());
        let error = run_testnet_lifecycle(&config, &exhausted, &history)
            .await
            .unwrap_err();
        assert!(matches!(error, TestnetLifecycleError::QueryBudgetExhausted));
        assert!(exhausted.calls().is_empty());
        cleanup_history(history);
    }

    #[tokio::test]
    async fn ambiguous_cancel_can_complete_from_the_authoritative_final_query() {
        let config = config(TestnetLifecycleObservation::Open, "cancel-query");
        let history = test_history("cancel-query");
        let venue = FakeVenue::new(
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Err(ambiguous_cancel(&config))],
        );

        let report = run_testnet_lifecycle(&config, &venue, &history)
            .await
            .unwrap();

        assert_eq!(report.final_status, OrderStatus::Cancelled);
        let body = std::fs::read_to_string(history.path()).unwrap();
        assert!(body.contains(OUTCOME_UNKNOWN));
        assert!(!body.contains(CANCEL_OBSERVED));
        assert!(body.contains(COMPLETED));
        cleanup_history(history);
    }

    #[tokio::test]
    async fn completed_campaign_is_idempotent_and_performs_no_remote_calls() {
        let config = config(TestnetLifecycleObservation::Open, "idempotent");
        let history = test_history("idempotent");
        let first_venue = FakeVenue::new(
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        );
        run_testnet_lifecycle(&config, &first_venue, &history)
            .await
            .unwrap();

        let replay = FakeVenue::new(Vec::new(), Vec::new(), Vec::new());
        let report = run_testnet_lifecycle(&config, &replay, &history)
            .await
            .unwrap();

        assert!(report.recovered);
        assert!(replay.calls().is_empty());
        cleanup_history(history);
    }

    fn ambiguous_submit(config: &TestnetLifecycleConfig) -> ExchangeError {
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::SubmitOrder,
            client_order_id: Some(config.intent.client_order_id),
            operation_key: Some(ExchangeOperationKey::ClientOrderId {
                client_order_id: config.intent.client_order_id,
            }),
            reason: "fixture disconnect".to_owned(),
        }
    }

    fn ambiguous_cancel(config: &TestnetLifecycleConfig) -> ExchangeError {
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::CancelOrder,
            client_order_id: Some(config.intent.client_order_id),
            operation_key: Some(ExchangeOperationKey::OrderId(
                "binance:spot:BTCUSDT:31".to_owned(),
            )),
            reason: "fixture disconnect".to_owned(),
        }
    }

    fn config(
        expected_observation: TestnetLifecycleObservation,
        suffix: &str,
    ) -> TestnetLifecycleConfig {
        let mut intent = OrderIntent::limit(
            "binance",
            Symbol::new("BTC-USDC-SPOT").unwrap(),
            MarketType::Spot,
            Side::Buy,
            Quantity::new(Decimal::from_str("0.001").unwrap()).unwrap(),
            Price::new(Decimal::from_str("49000.1").unwrap()).unwrap(),
        );
        intent.client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
        intent.time_in_force = TimeInForce::PostOnly;
        TestnetLifecycleConfig::new(
            format!("campaign-{suffix}"),
            intent,
            expected_observation,
            Duration::from_millis(1),
            4,
        )
        .unwrap()
    }

    fn order(config: &TestnetLifecycleConfig, status: OrderStatus, filled: &str) -> Order {
        Order {
            id: "binance:spot:BTCUSDT:31".to_owned(),
            intent: config.intent.clone(),
            filled_quantity: Quantity::new(Decimal::from_str(filled).unwrap()).unwrap(),
            average_fill_price: (!Decimal::from_str(filled).unwrap().is_zero())
                .then(|| Price::new(Decimal::from_str("49000.1").unwrap()).unwrap()),
            status,
            created_at: Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 11).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 7, 25, 9, 10, 12).unwrap(),
        }
    }

    fn test_history(label: &str) -> JsonlHistory {
        JsonlHistory::new(std::env::temp_dir().join(format!(
            "crypto-trading-testnet-lifecycle-{label}-{}.jsonl",
            Uuid::new_v4()
        )))
    }

    fn cleanup_history(history: JsonlHistory) {
        let path = history.path().to_owned();
        let lock_path = path.with_file_name(format!(
            "{}.jsonl.lock",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("history.jsonl")
        ));
        drop(history);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(lock_path);
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
