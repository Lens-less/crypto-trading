//! Durable one-shot Binance Spot MAINNET order lifecycle owner.
//!
//! Modeled directly on [`crate::testnet_lifecycle`] with the same journal-first
//! invariants: `planned` is durable before any network mutation, recovery is
//! query-first by the durable UUID client order id with no blind resubmit, and
//! ambiguous outcomes are never success and never trigger a new order.
//!
//! Mainnet additions on top of the testnet owner:
//!
//! * The configuration is Spot LIMIT only and carries a required
//!   `max_notional` cap in quote units; a plan whose `price * quantity`
//!   exceeds the cap is rejected at construction, before any journal write.
//! * A fresh campaign performs venue-truth admission after `planned` and
//!   before submit: exchangeInfo instrument filters, spot-no-short balance
//!   checks against the signed account snapshot, and a foreign-open-order
//!   refusal unless explicitly allowed.
//! * A latched kill fact in the journal blocks every new lifecycle. Pending
//!   campaigns may still be recovered query-first for cleanup, which never
//!   creates submit authority. Unsafe terminal outcomes latch the kill switch
//!   automatically so a human must review before any new mainnet order.

use std::{fmt, future::Future, io, path::Path, pin::Pin, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{
    MarketType, Order, OrderIntent, OrderStatus, OrderType, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceMainnetSpotExchange, ExchangeError, ExchangeHandle, InstrumentRuleCatalog,
    RemoteRetryAfter, TradingCommand, TradingReceipt,
};
use crypto_trading_runtime::{
    DecisionRecord, FileJournalSnapshotSource, HistoryError, JournalPageBoundary, JournalReadError,
    JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::time::sleep;
use uuid::Uuid;

pub const LIVE_LIFECYCLE_SCHEMA_VERSION: u16 = 1;
pub const LIVE_LIFECYCLE_ACKNOWLEDGEMENT: &str = "I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE";

const STRATEGY: &str = "binance_live_lifecycle";
const PLANNED: &str = "live_lifecycle_planned";
const RESUMED: &str = "live_lifecycle_resumed";
const ADMISSION_OBSERVED: &str = "live_lifecycle_admission_observed";
const QUERY_PLANNED: &str = "live_lifecycle_query_planned";
const SUBMIT_OBSERVED: &str = "live_lifecycle_submit_observed";
const QUERY_OBSERVED: &str = "live_lifecycle_query_observed";
const CANCEL_PLANNED: &str = "live_lifecycle_cancel_planned";
const CANCEL_OBSERVED: &str = "live_lifecycle_cancel_observed";
const OUTCOME_UNKNOWN: &str = "live_lifecycle_outcome_unknown";
const COMPLETED: &str = "live_lifecycle_completed";
const FAILED: &str = "live_lifecycle_failed";
const KILL_SWITCH_ENGAGED: &str = "live_lifecycle_kill_switch_engaged";
const MAX_CAMPAIGN_ID_BYTES: usize = 128;
const MAX_WIRE_SYMBOL_BYTES: usize = 64;
const MAX_QUERY_ATTEMPTS_PER_CAMPAIGN: u16 = 1_000;
const MIN_QUERY_ATTEMPTS_PER_CAMPAIGN: u16 = 3;
const CLEANUP_QUERY_RESERVE: u32 = 2;
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(60);
const PROJECTION_JOURNAL_ID: Uuid = Uuid::from_u128(0x11fe_51fe_c1c1_4e2e_8f10_2b6a_66d1_a201);

pub type LiveLifecycleVenueFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ExchangeError>> + Send + 'a>>;

/// Venue-truth inputs required before a mainnet submit is admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveAdmissionTruth {
    /// Free (available) base-asset balance from the signed account snapshot.
    pub free_base_quantity: Decimal,
    /// Free (available) quote-asset balance from the signed account snapshot.
    pub free_quote_amount: Decimal,
    /// Client order ids of every open order on the campaign symbol; `None`
    /// entries are venue orders whose client id could not be read.
    pub open_order_client_ids: Vec<Option<String>>,
}

/// Narrow mutation/query seam used by the live lifecycle owner and offline
/// tests.
pub trait LiveLifecycleVenue: Send + Sync {
    /// Samples the signed venue truth used by pre-submit admission.
    fn admission(&self) -> LiveLifecycleVenueFuture<'_, LiveAdmissionTruth>;

    fn submit(&self, intent: OrderIntent) -> LiveLifecycleVenueFuture<'_, Order>;

    fn query(&self, symbol: Symbol, client_order_id: Uuid) -> LiveLifecycleVenueFuture<'_, Order>;

    fn cancel(&self, order_id: String) -> LiveLifecycleVenueFuture<'_, Order>;
}

/// Executable venue backed by the authority-typed mainnet Spot trade adapter.
pub struct LiveLifecycleExchangeVenue {
    exchange: BinanceMainnetSpotExchange,
    symbol: Symbol,
    base_asset: String,
    quote_asset: String,
}

impl LiveLifecycleExchangeVenue {
    /// Binds the venue to one exact Spot symbol whose base and quote assets
    /// drive the no-short balance checks.
    ///
    /// # Errors
    ///
    /// Returns [`LiveLifecycleError::InvalidConfig`] unless the symbol has the
    /// canonical `BASE-QUOTE-SPOT` shape.
    pub fn new(
        exchange: BinanceMainnetSpotExchange,
        symbol: Symbol,
    ) -> Result<Self, LiveLifecycleError> {
        let (base_asset, quote_asset) = spot_symbol_assets(&symbol)?;
        Ok(Self {
            exchange,
            symbol,
            base_asset,
            quote_asset,
        })
    }
}

impl LiveLifecycleVenue for LiveLifecycleExchangeVenue {
    fn admission(&self) -> LiveLifecycleVenueFuture<'_, LiveAdmissionTruth> {
        Box::pin(async move {
            let snapshot = self.exchange.account_snapshot().await?;
            let free_balance = |asset: &str| {
                snapshot
                    .balances
                    .iter()
                    .find(|balance| balance.asset.eq_ignore_ascii_case(asset))
                    .map_or(Decimal::ZERO, |balance| balance.available_balance)
            };
            let mut open_order_client_ids = Vec::new();
            for order in &snapshot.orders {
                if order.intent.symbol == self.symbol {
                    open_order_client_ids.push(Some(order.intent.client_order_id.to_string()));
                }
            }
            for foreign in &snapshot.foreign_orders {
                if foreign.symbol == self.symbol {
                    open_order_client_ids.push(foreign.client_order_id.clone());
                }
            }
            Ok(LiveAdmissionTruth {
                free_base_quantity: free_balance(&self.base_asset),
                free_quote_amount: free_balance(&self.quote_asset),
                open_order_client_ids,
            })
        })
    }

    fn submit(&self, intent: OrderIntent) -> LiveLifecycleVenueFuture<'_, Order> {
        Box::pin(async move {
            let receipt = self
                .exchange
                .execute(TradingCommand::Submit(intent))
                .await?;
            submitted_order(receipt)
        })
    }

    fn query(&self, symbol: Symbol, client_order_id: Uuid) -> LiveLifecycleVenueFuture<'_, Order> {
        Box::pin(async move { self.exchange.query_order(&symbol, client_order_id).await })
    }

    fn cancel(&self, order_id: String) -> LiveLifecycleVenueFuture<'_, Order> {
        Box::pin(async move {
            let receipt = self
                .exchange
                .execute(TradingCommand::Cancel { order_id })
                .await?;
            cancelled_order(receipt)
        })
    }
}

/// Observation that must be proven by a single-order query before cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveLifecycleObservation {
    Open,
    PartiallyFilled,
}

impl LiveLifecycleObservation {
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

/// Specific pre-submit refusal recorded as a durable failed fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveAdmissionRefusal {
    FiltersViolated,
    InsufficientBaseBalance,
    InsufficientQuoteBalance,
    ForeignOpenOrders,
}

impl LiveAdmissionRefusal {
    const fn label(self) -> &'static str {
        match self {
            Self::FiltersViolated => "admission_filters_violated",
            Self::InsufficientBaseBalance => "admission_sell_exceeds_free_base_balance",
            Self::InsufficientQuoteBalance => "admission_buy_exceeds_free_quote_balance",
            Self::ForeignOpenOrders => "admission_foreign_open_orders",
        }
    }
}

/// Immutable identity, admission caps, and bounded polling policy for one
/// human-authorized mainnet Spot round trip.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveLifecycleConfig {
    campaign_id: String,
    intent: OrderIntent,
    wire_symbol: String,
    expected_observation: LiveLifecycleObservation,
    poll_interval: Duration,
    maximum_queries: u16,
    max_notional: Decimal,
    allow_foreign_orders: bool,
}

impl LiveLifecycleConfig {
    /// Builds one recovery-safe mainnet lifecycle configuration.
    ///
    /// # Errors
    ///
    /// Returns [`LiveLifecycleError::InvalidConfig`] unless the campaign
    /// identity, UUID-backed Binance Spot limit order, cap, and polling
    /// bounds are safe, and [`LiveLifecycleError::NotionalExceedsCap`] when
    /// `price * quantity` exceeds `max_notional`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        campaign_id: impl Into<String>,
        intent: OrderIntent,
        wire_symbol: impl Into<String>,
        expected_observation: LiveLifecycleObservation,
        poll_interval: Duration,
        maximum_queries: u16,
        max_notional: Decimal,
        allow_foreign_orders: bool,
    ) -> Result<Self, LiveLifecycleError> {
        let campaign_id = campaign_id.into();
        let wire_symbol = wire_symbol.into();
        if !valid_campaign_id(&campaign_id)
            || !valid_wire_symbol(&wire_symbol)
            || !intent.exchange.eq_ignore_ascii_case("binance")
            || intent.market_type != MarketType::Spot
            || intent.reduce_only
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
            || max_notional <= Decimal::ZERO
        {
            return Err(LiveLifecycleError::InvalidConfig);
        }
        let notional = intent_notional(&intent)?;
        if notional > max_notional {
            return Err(LiveLifecycleError::NotionalExceedsCap {
                notional,
                max_notional,
            });
        }
        Ok(Self {
            campaign_id,
            intent,
            wire_symbol,
            expected_observation,
            poll_interval,
            maximum_queries,
            max_notional,
            allow_foreign_orders,
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
    pub fn wire_symbol(&self) -> &str {
        &self.wire_symbol
    }

    #[must_use]
    pub const fn max_notional(&self) -> Decimal {
        self.max_notional
    }

    /// Rebinds a caller-supplied mapping to the exact durable recovery symbol.
    ///
    /// # Errors
    ///
    /// Returns [`LiveLifecycleError::InvalidConfig`] for an invalid Binance
    /// wire symbol.
    pub fn with_wire_symbol(
        mut self,
        wire_symbol: impl Into<String>,
    ) -> Result<Self, LiveLifecycleError> {
        let wire_symbol = wire_symbol.into();
        if !valid_wire_symbol(&wire_symbol) {
            return Err(LiveLifecycleError::InvalidConfig);
        }
        self.wire_symbol = wire_symbol;
        Ok(self)
    }

    #[must_use]
    pub const fn expected_observation(&self) -> LiveLifecycleObservation {
        self.expected_observation
    }
}

/// Durable proof returned only after a final query observes cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveLifecycleReport {
    pub campaign_id: String,
    pub client_order_id: Uuid,
    pub server_order_id: String,
    pub expected_observation: LiveLifecycleObservation,
    pub final_status: OrderStatus,
    pub query_count: u32,
    pub recovered: bool,
}

/// Reports whether the durable campaign can still enter its one submit branch.
///
/// Once `planned` is durable, callers must construct only query/cancel
/// recovery authority; current metadata availability must not block cleanup.
///
/// # Errors
///
/// Returns an error when the bounded journal cannot be projected safely.
pub fn live_lifecycle_requires_submission(
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
) -> Result<bool, LiveLifecycleError> {
    Ok(!project_campaign(history.path(), config)?.planned)
}

/// Returns the exact wire symbol from the durable submit plan, or the
/// validated caller mapping for a fresh campaign.
///
/// # Errors
///
/// Returns an error when the bounded journal cannot be projected safely.
pub fn live_lifecycle_wire_symbol(
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
) -> Result<String, LiveLifecycleError> {
    Ok(project_campaign(history.path(), config)?
        .wire_symbol
        .unwrap_or_else(|| config.wire_symbol.clone()))
}

/// Reports whether any durable kill fact latches this journal.
///
/// The latch is journal-scoped, not campaign-scoped: one unsafe terminal
/// outcome blocks every new lifecycle recorded in the same account journal
/// until a human starts a fresh journal after review. There is deliberately
/// no disengage transition.
///
/// # Errors
///
/// Returns an error when the bounded journal cannot be projected safely.
pub fn live_lifecycle_kill_switch_latched(
    history: &JsonlHistory,
) -> Result<bool, LiveLifecycleError> {
    let path = history.path();
    if !path
        .try_exists()
        .map_err(LiveLifecycleError::HistoryProbe)?
    {
        return Ok(false);
    }
    let source = FileJournalSnapshotSource::new(PROJECTION_JOURNAL_ID, path)?;
    let snapshot = source.snapshot()?;
    let mut cursor = None;
    loop {
        let page = LegacyJsonlJournalReader::read_page(&snapshot, cursor.as_ref())?;
        for event in page.events() {
            let payload = event.payload();
            if payload.get("strategy").and_then(Value::as_str) == Some(STRATEGY)
                && payload.get("decision").and_then(Value::as_str) == Some(KILL_SWITCH_ENGAGED)
            {
                return Ok(true);
            }
        }
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => break,
            JournalPageBoundary::PageLimit => {
                cursor = page.next_cursor().cloned();
                if cursor.is_none() {
                    return Err(LiveLifecycleError::CorruptHistory);
                }
            }
            JournalPageBoundary::PartialTail { .. } => {
                return Err(LiveLifecycleError::CorruptHistory);
            }
        }
    }
    Ok(false)
}

/// Runs or recovers one submit-query-cancel-query mainnet Spot lifecycle.
///
/// A fresh run refuses when the journal kill fact is latched, appends
/// `planned` before any network mutation, then performs venue-truth admission
/// against `rules` and the venue's signed account truth before its single
/// submit. Any later run that sees the durable plan queries by the same
/// client ID first and never resubmits; recovery needs no admission because
/// it holds no submit authority.
///
/// # Errors
///
/// Returns a bounded failure after recording the safest durable fact
/// possible. Mutating ambiguity is never reported as a definite failure.
pub async fn run_live_lifecycle<V>(
    config: &LiveLifecycleConfig,
    venue: &V,
    rules: &InstrumentRuleCatalog,
    history: &JsonlHistory,
) -> Result<LiveLifecycleReport, LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
{
    let projected = project_campaign(history.path(), config)?;
    if let Some(order) = projected.completed {
        return Ok(report(config, &order, projected.query_count, true));
    }
    if projected.failed {
        return Err(LiveLifecycleError::PreviouslyFailed);
    }
    if projected
        .wire_symbol
        .as_deref()
        .is_some_and(|wire_symbol| wire_symbol != config.wire_symbol)
    {
        return Err(LiveLifecycleError::ConfigConflict);
    }
    if let Some(not_before) = projected.retry_not_before
        && not_before > Utc::now()
    {
        return Err(LiveLifecycleError::RetryDeferred { not_before });
    }
    if !projected.planned && live_lifecycle_kill_switch_latched(history)? {
        return Err(LiveLifecycleError::KillSwitchLatched);
    }
    let active = establish_active_lifecycle(config, venue, rules, history, projected).await?;
    finish_active_lifecycle(config, venue, history, active).await
}

async fn establish_active_lifecycle<V>(
    config: &LiveLifecycleConfig,
    venue: &V,
    rules: &InstrumentRuleCatalog,
    history: &JsonlHistory,
    projected: ProjectedCampaign,
) -> Result<ActiveLifecycle, LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
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
                    "wire_symbol": config.wire_symbol,
                    "expected_observation": config.expected_observation.label(),
                    "poll_interval_ms": poll_interval_millis(config)?,
                    "maximum_queries": config.maximum_queries,
                    "max_notional": config.max_notional.to_string(),
                    "allow_foreign_orders": config.allow_foreign_orders,
                }),
            ))
            .await?;
        admit_fresh_submission(config, venue, rules, history).await?;
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
                return Err(LiveLifecycleError::Exchange(error));
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

/// Venue-truth admission for the one fresh submit branch.
///
/// Runs after `planned` is durable and before any mutation. Every refusal is
/// recorded as a durable failed fact so the campaign cannot silently retry.
async fn admit_fresh_submission<V>(
    config: &LiveLifecycleConfig,
    venue: &V,
    rules: &InstrumentRuleCatalog,
    history: &JsonlHistory,
) -> Result<(), LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
{
    let truth = match venue.admission().await {
        Ok(truth) => truth,
        Err(error) => {
            append_failed(config, history, "admission_read_failed", None, Some(&error)).await?;
            return Err(LiveLifecycleError::Exchange(error));
        }
    };
    let notional = intent_notional(&config.intent)?;
    let own_client_id = config.intent.client_order_id.to_string();
    let foreign_open_orders = truth
        .open_order_client_ids
        .iter()
        .filter(|id| id.as_deref() != Some(own_client_id.as_str()))
        .count();
    history
        .append(&lifecycle_record(
            config,
            ADMISSION_OBSERVED,
            "admission",
            json!({
                "notional": notional.to_string(),
                "max_notional": config.max_notional.to_string(),
                "free_base_quantity": truth.free_base_quantity.to_string(),
                "free_quote_amount": truth.free_quote_amount.to_string(),
                "open_orders_on_symbol": truth.open_order_client_ids.len(),
                "foreign_open_orders": foreign_open_orders,
                "allow_foreign_orders": config.allow_foreign_orders,
            }),
        ))
        .await?;
    if let Err(error) = rules.validate_order(&config.intent, None) {
        append_failed(
            config,
            history,
            LiveAdmissionRefusal::FiltersViolated.label(),
            None,
            Some(&error),
        )
        .await?;
        return Err(LiveLifecycleError::AdmissionRefused(
            LiveAdmissionRefusal::FiltersViolated,
        ));
    }
    let refusal = match config.intent.side {
        Side::Sell if config.intent.quantity.as_decimal() > truth.free_base_quantity => {
            Some(LiveAdmissionRefusal::InsufficientBaseBalance)
        }
        Side::Buy if notional > truth.free_quote_amount => {
            Some(LiveAdmissionRefusal::InsufficientQuoteBalance)
        }
        Side::Buy | Side::Sell => None,
    };
    let refusal = refusal.or_else(|| {
        (foreign_open_orders > 0 && !config.allow_foreign_orders)
            .then_some(LiveAdmissionRefusal::ForeignOpenOrders)
    });
    if let Some(refusal) = refusal {
        append_failed(config, history, refusal.label(), None, None).await?;
        return Err(LiveLifecycleError::AdmissionRefused(refusal));
    }
    Ok(())
}

async fn finish_active_lifecycle<V>(
    config: &LiveLifecycleConfig,
    venue: &V,
    history: &JsonlHistory,
    active: ActiveLifecycle,
) -> Result<LiveLifecycleReport, LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
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
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    final_order: &Order,
    query_count: u32,
    recovered: bool,
    reason: CancelReason,
) -> Result<LiveLifecycleReport, LiveLifecycleError> {
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
            Err(LiveLifecycleError::ObservationNotReached)
        }
    }
}

async fn fail_missed_observation<V>(
    venue: &V,
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    order: Order,
    query_count: &mut u32,
) -> Result<LiveLifecycleReport, LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
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
        return Err(LiveLifecycleError::ObservationNotReached);
    }
    append_failed_unsafe_terminal(config, history, "unexpected_terminal_order", &order).await?;
    Err(LiveLifecycleError::UnsafeTerminal(order.status))
}

async fn query_and_record<V>(
    venue: &V,
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    query_count: &mut u32,
    phase: &str,
) -> Result<Order, LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
{
    let maximum_queries = u32::from(config.maximum_queries);
    if *query_count >= maximum_queries {
        append_query_budget_exhausted(config, history, phase, *query_count).await?;
        return Err(LiveLifecycleError::QueryBudgetExhausted);
    }
    let query_sequence = query_count
        .checked_add(1)
        .ok_or(LiveLifecycleError::CounterOverflow)?;
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
        .query(config.intent.symbol.clone(), config.intent.client_order_id)
        .await;
    let order = match result {
        Ok(order) => order,
        Err(error) => {
            append_outcome_unknown(config, history, phase, &error).await?;
            return Err(LiveLifecycleError::OutcomeUnknown);
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
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    current: Order,
    query_count: &mut u32,
    cancel_already_planned: bool,
    cancel_reason: CancelReason,
) -> Result<Order, LiveLifecycleError>
where
    V: LiveLifecycleVenue + ?Sized,
{
    validate_order(config, &current, None)?;
    if current.status == OrderStatus::Cancelled {
        if cancel_already_planned {
            return Ok(current);
        }
        append_failed_unsafe_terminal(
            config,
            history,
            "order_cancelled_before_owner_cleanup",
            &current,
        )
        .await?;
        return Err(LiveLifecycleError::UnsafeTerminal(current.status));
    }
    if !is_active_status(current.status) {
        append_failed_unsafe_terminal(config, history, "order_terminal_before_cancel", &current)
            .await?;
        return Err(LiveLifecycleError::UnsafeTerminal(current.status));
    }
    if *query_count >= u32::from(config.maximum_queries) {
        append_query_budget_exhausted(config, history, "cancel_confirmation", *query_count).await?;
        return Err(LiveLifecycleError::QueryBudgetExhausted);
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
            if should_defer_cancel_confirmation(&error) {
                return Err(LiveLifecycleError::OutcomeUnknown);
            }
        }
    }

    let final_order =
        query_and_record(venue, config, history, query_count, "post_cancel_query").await?;
    if final_order.id != current.id {
        return Err(LiveLifecycleError::ProtocolViolation);
    }
    match final_order.status {
        OrderStatus::Cancelled => Ok(final_order),
        OrderStatus::Filled | OrderStatus::Rejected => {
            append_failed_unsafe_terminal(
                config,
                history,
                "order_terminal_during_cancel",
                &final_order,
            )
            .await?;
            Err(LiveLifecycleError::UnsafeTerminal(final_order.status))
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
            Err(LiveLifecycleError::OutcomeUnknown)
        }
    }
}

async fn append_completed(
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    order: &Order,
) -> Result<(), LiveLifecycleError> {
    if order.status != OrderStatus::Cancelled {
        return Err(LiveLifecycleError::ProtocolViolation);
    }
    history
        .append(&order_record(config, COMPLETED, "completed", order))
        .await?;
    Ok(())
}

async fn append_outcome_unknown(
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    phase: &str,
    error: &ExchangeError,
) -> Result<(), LiveLifecycleError> {
    history
        .append(&lifecycle_record(
            config,
            OUTCOME_UNKNOWN,
            phase,
            outcome_unknown_details(error),
        ))
        .await?;
    Ok(())
}

async fn append_query_budget_exhausted(
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    phase: &str,
    query_count: u32,
) -> Result<(), LiveLifecycleError> {
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
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    failure: &str,
    order: Option<&Order>,
    exchange_error: Option<&ExchangeError>,
) -> Result<(), LiveLifecycleError> {
    let mut details = json!({ "failure": failure });
    let fields = details
        .as_object_mut()
        .ok_or(LiveLifecycleError::ProtocolViolation)?;
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

/// Records the failure and latches the journal kill fact in the same pass.
///
/// An unexpected terminal state on a live account means venue truth diverged
/// from the owner's expectations; every new lifecycle is blocked until a
/// human reviews the account and starts a fresh journal.
async fn append_failed_unsafe_terminal(
    config: &LiveLifecycleConfig,
    history: &JsonlHistory,
    failure: &str,
    order: &Order,
) -> Result<(), LiveLifecycleError> {
    append_failed(config, history, failure, Some(order), None).await?;
    history
        .append(&lifecycle_record(
            config,
            KILL_SWITCH_ENGAGED,
            "kill_switch_engaged",
            json!({
                "failure": failure,
                "engaged_by": "unsafe_terminal",
            }),
        ))
        .await?;
    Ok(())
}

fn lifecycle_record(
    config: &LiveLifecycleConfig,
    decision: &str,
    phase: &str,
    extra: Value,
) -> DecisionRecord {
    let mut details = json!({
        "schema_version": LIVE_LIFECYCLE_SCHEMA_VERSION,
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
    config: &LiveLifecycleConfig,
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
    config: &LiveLifecycleConfig,
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
    config: &LiveLifecycleConfig,
) -> Result<ProjectedCampaign, LiveLifecycleError> {
    if !path
        .try_exists()
        .map_err(LiveLifecycleError::HistoryProbe)?
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
            project_event(config, event.recorded_at(), event.payload(), &mut projected)?;
        }
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => break,
            JournalPageBoundary::PageLimit => {
                cursor = page.next_cursor().cloned();
                if cursor.is_none() {
                    return Err(LiveLifecycleError::CorruptHistory);
                }
            }
            JournalPageBoundary::PartialTail { .. } => {
                return Err(LiveLifecycleError::CorruptHistory);
            }
        }
    }
    Ok(projected)
}

fn project_event(
    config: &LiveLifecycleConfig,
    recorded_at: DateTime<Utc>,
    payload: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), LiveLifecycleError> {
    if payload.get("strategy").and_then(Value::as_str) != Some(STRATEGY) {
        return Ok(());
    }
    let Some(details) = payload.get("details") else {
        return Err(LiveLifecycleError::CorruptHistory);
    };
    if details.get("campaign_id").and_then(Value::as_str) != Some(&config.campaign_id) {
        return Ok(());
    }
    if details.get("schema_version").and_then(Value::as_u64)
        != Some(u64::from(LIVE_LIFECYCLE_SCHEMA_VERSION))
        || details
            .get("client_order_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            != Some(config.intent.client_order_id)
    {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let decision = payload
        .get("decision")
        .and_then(Value::as_str)
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    match decision {
        PLANNED => project_planned_event(config, details, projected)?,
        QUERY_PLANNED => project_query_plan(config, details, projected)?,
        SUBMIT_OBSERVED | QUERY_OBSERVED | CANCEL_OBSERVED | COMPLETED => {
            project_order_event(config, decision, details, projected)?;
        }
        CANCEL_PLANNED => project_cancel_plan(details, projected)?,
        RESUMED | ADMISSION_OBSERVED => {
            if !projected.planned || projected.completed.is_some() || projected.failed {
                return Err(LiveLifecycleError::CorruptHistory);
            }
        }
        OUTCOME_UNKNOWN => project_outcome_unknown(recorded_at, details, projected)?,
        FAILED => {
            if !projected.planned || projected.failed || projected.completed.is_some() {
                return Err(LiveLifecycleError::CorruptHistory);
            }
            projected.failed = true;
        }
        KILL_SWITCH_ENGAGED => {
            // The latch itself is journal-scoped and read by the dedicated
            // scan; within one campaign it must only follow the durable plan.
            if !projected.planned {
                return Err(LiveLifecycleError::CorruptHistory);
            }
        }
        _ => return Err(LiveLifecycleError::CorruptHistory),
    }
    Ok(())
}

fn project_planned_event(
    config: &LiveLifecycleConfig,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), LiveLifecycleError> {
    if projected.planned {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let intent = serde_json::from_value::<OrderIntent>(
        details
            .get("intent")
            .cloned()
            .ok_or(LiveLifecycleError::CorruptHistory)?,
    )
    .map_err(|_| LiveLifecycleError::CorruptHistory)?;
    let observation = details
        .get("expected_observation")
        .and_then(Value::as_str)
        .and_then(LiveLifecycleObservation::parse)
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    let wire_symbol = details
        .get("wire_symbol")
        .and_then(Value::as_str)
        .filter(|value| valid_wire_symbol(value))
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    let poll_interval_ms = details
        .get("poll_interval_ms")
        .and_then(Value::as_u64)
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    let maximum_queries = details
        .get("maximum_queries")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    let max_notional = details
        .get("max_notional")
        .and_then(Value::as_str)
        .and_then(|value| Decimal::from_str(value).ok())
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    let allow_foreign_orders = details
        .get("allow_foreign_orders")
        .and_then(Value::as_bool)
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    if intent != config.intent
        || observation != config.expected_observation
        || poll_interval_ms != poll_interval_millis(config)?
        || maximum_queries != config.maximum_queries
        || max_notional != config.max_notional
        || allow_foreign_orders != config.allow_foreign_orders
    {
        return Err(LiveLifecycleError::ConfigConflict);
    }
    projected.planned = true;
    projected.wire_symbol = Some(wire_symbol.to_owned());
    Ok(())
}

fn project_outcome_unknown(
    recorded_at: DateTime<Utc>,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), LiveLifecycleError> {
    if !projected.planned || projected.completed.is_some() || projected.failed {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let Some(retry_after) = details.get("retry_after") else {
        return Ok(());
    };
    let not_before = match retry_after.get("kind").and_then(Value::as_str) {
        Some("seconds") => {
            let seconds = retry_after
                .get("seconds")
                .and_then(Value::as_u64)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or(LiveLifecycleError::CorruptHistory)?;
            recorded_at
                .checked_add_signed(chrono::Duration::seconds(seconds))
                .ok_or(LiveLifecycleError::CorruptHistory)?
        }
        Some("at") => serde_json::from_value::<DateTime<Utc>>(
            retry_after
                .get("at")
                .cloned()
                .ok_or(LiveLifecycleError::CorruptHistory)?,
        )
        .map_err(|_| LiveLifecycleError::CorruptHistory)?,
        _ => return Err(LiveLifecycleError::CorruptHistory),
    };
    projected.retry_not_before = Some(
        projected
            .retry_not_before
            .map_or(not_before, |existing| existing.max(not_before)),
    );
    Ok(())
}

fn project_query_plan(
    config: &LiveLifecycleConfig,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), LiveLifecycleError> {
    if !projected.planned || projected.completed.is_some() || projected.failed {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let query_sequence = details
        .get("query_sequence")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    let expected_sequence = projected
        .query_count
        .checked_add(1)
        .ok_or(LiveLifecycleError::CounterOverflow)?;
    if query_sequence != expected_sequence || query_sequence > u32::from(config.maximum_queries) {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    projected.query_count = query_sequence;
    Ok(())
}

fn project_cancel_plan(
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), LiveLifecycleError> {
    if !projected.planned || projected.cancel_reason.is_some() {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let server_order_id = details
        .get("server_order_id")
        .and_then(Value::as_str)
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    if projected.server_order_id.as_deref() != Some(server_order_id) {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let reason = details
        .get("reason")
        .and_then(Value::as_str)
        .and_then(CancelReason::parse)
        .ok_or(LiveLifecycleError::CorruptHistory)?;
    if (reason == CancelReason::ObservationSatisfied) != projected.observation_satisfied {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    projected.cancel_reason = Some(reason);
    Ok(())
}

fn project_order_event(
    config: &LiveLifecycleConfig,
    decision: &str,
    details: &Value,
    projected: &mut ProjectedCampaign,
) -> Result<(), LiveLifecycleError> {
    if !projected.planned
        || (matches!(decision, CANCEL_OBSERVED | COMPLETED) && projected.cancel_reason.is_none())
        || projected.completed.is_some()
        || projected.failed
    {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    let order = projected_order(config, details)?;
    projected.retry_not_before = None;
    if let Some(existing_id) = projected.server_order_id.as_deref()
        && existing_id != order.id
    {
        return Err(LiveLifecycleError::CorruptHistory);
    }
    projected.server_order_id = Some(order.id.clone());
    if decision == QUERY_OBSERVED {
        let query_sequence = details
            .get("query_sequence")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(LiveLifecycleError::CorruptHistory)?;
        if query_sequence == 0
            || query_sequence > projected.query_count
            || projected
                .last_observed_query_sequence
                .is_some_and(|previous| query_sequence <= previous)
        {
            return Err(LiveLifecycleError::CorruptHistory);
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
            return Err(LiveLifecycleError::CorruptHistory);
        }
        projected.completed = Some(order);
    }
    Ok(())
}

fn projected_order(
    config: &LiveLifecycleConfig,
    details: &Value,
) -> Result<Order, LiveLifecycleError> {
    let order = serde_json::from_value::<Order>(
        details
            .get("order")
            .cloned()
            .ok_or(LiveLifecycleError::CorruptHistory)?,
    )
    .map_err(|_| LiveLifecycleError::CorruptHistory)?;
    validate_order(config, &order, None)?;
    Ok(order)
}

fn validate_order(
    config: &LiveLifecycleConfig,
    order: &Order,
    expected_server_id: Option<&str>,
) -> Result<(), LiveLifecycleError> {
    if order.id.is_empty()
        || order.intent != config.intent
        || expected_server_id.is_some_and(|expected| expected != order.id)
    {
        return Err(LiveLifecycleError::ProtocolViolation);
    }
    Ok(())
}

fn submitted_order(receipt: TradingReceipt) -> Result<Order, ExchangeError> {
    let TradingReceipt::Submitted { order, .. } = receipt else {
        return Err(ExchangeError::invalid(
            "Binance live lifecycle submit returned a non-order receipt",
        ));
    };
    Ok(order)
}

fn cancelled_order(receipt: TradingReceipt) -> Result<Order, ExchangeError> {
    let TradingReceipt::Cancelled { mut orders, .. } = receipt else {
        return Err(ExchangeError::invalid(
            "Binance live lifecycle cancel returned a non-cancellation receipt",
        ));
    };
    if orders.len() != 1 {
        return Err(ExchangeError::invalid(
            "Binance live lifecycle single cancel must return exactly one order",
        ));
    }
    orders
        .pop()
        .ok_or_else(|| ExchangeError::invalid("Binance live lifecycle cancel omitted its order"))
}

fn report(
    config: &LiveLifecycleConfig,
    order: &Order,
    query_count: u32,
    recovered: bool,
) -> LiveLifecycleReport {
    LiveLifecycleReport {
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

fn has_observation_query_budget(config: &LiveLifecycleConfig, query_count: u32) -> bool {
    query_count.saturating_add(CLEANUP_QUERY_RESERVE) < u32::from(config.maximum_queries)
}

fn poll_interval_millis(config: &LiveLifecycleConfig) -> Result<u64, LiveLifecycleError> {
    u64::try_from(config.poll_interval.as_millis()).map_err(|_| LiveLifecycleError::InvalidConfig)
}

fn intent_notional(intent: &OrderIntent) -> Result<Decimal, LiveLifecycleError> {
    let price = intent.price.ok_or(LiveLifecycleError::InvalidConfig)?;
    price
        .as_decimal()
        .checked_mul(intent.quantity.as_decimal())
        .ok_or(LiveLifecycleError::CounterOverflow)
}

fn spot_symbol_assets(symbol: &Symbol) -> Result<(String, String), LiveLifecycleError> {
    let mut segments = symbol.as_str().split('-');
    let (Some(base), Some(quote), Some("SPOT"), None) = (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) else {
        return Err(LiveLifecycleError::InvalidConfig);
    };
    if base.is_empty() || quote.is_empty() {
        return Err(LiveLifecycleError::InvalidConfig);
    }
    Ok((base.to_owned(), quote.to_owned()))
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

fn valid_wire_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_WIRE_SYMBOL_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
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

fn outcome_unknown_details(error: &ExchangeError) -> Value {
    let mut details = json!({ "failure": exchange_failure_bucket(error) });
    let Some(fields) = details.as_object_mut() else {
        return details;
    };
    if let ExchangeError::RemoteFailure {
        status, metadata, ..
    } = error
    {
        if let Some(http_status) = status {
            fields.insert("http_status".to_owned(), json!(http_status));
        }
        if let Some(exchange_code) = metadata.exchange_code.as_deref() {
            fields.insert("exchange_code".to_owned(), json!(exchange_code));
        }
        if let Some(retry_after) = remote_retry_after_value(metadata.retry_after.as_ref()) {
            fields.insert("retry_after".to_owned(), retry_after);
        }
        if let Some(server_time) = metadata.server_time {
            fields.insert("server_time".to_owned(), json!(server_time.to_rfc3339()));
        }
    }
    details
}

fn remote_retry_after_value(retry_after: Option<&RemoteRetryAfter>) -> Option<Value> {
    match retry_after? {
        RemoteRetryAfter::Seconds(seconds) => {
            Some(json!({ "kind": "seconds", "seconds": seconds }))
        }
        RemoteRetryAfter::At(deadline) => {
            Some(json!({ "kind": "at", "at": deadline.to_rfc3339() }))
        }
    }
}

fn should_defer_cancel_confirmation(error: &ExchangeError) -> bool {
    matches!(
        error,
        ExchangeError::RemoteFailure {
            status,
            metadata,
            ..
        } if metadata.retry_after.is_some() || matches!(status, Some(418 | 429))
    )
}

#[derive(Default)]
struct ProjectedCampaign {
    planned: bool,
    wire_symbol: Option<String>,
    cancel_reason: Option<CancelReason>,
    observation_satisfied: bool,
    failed: bool,
    completed: Option<Order>,
    server_order_id: Option<String>,
    query_count: u32,
    last_observed_query_sequence: Option<u32>,
    retry_not_before: Option<DateTime<Utc>>,
}

struct ActiveLifecycle {
    recovered: bool,
    query_count: u32,
    cancel_reason: Option<CancelReason>,
    observation_satisfied: bool,
    order: Order,
}

/// Bounded lifecycle failure. Display text intentionally excludes remote
/// bodies and credentials.
#[derive(Debug)]
pub enum LiveLifecycleError {
    InvalidConfig,
    NotionalExceedsCap {
        notional: Decimal,
        max_notional: Decimal,
    },
    AdmissionRefused(LiveAdmissionRefusal),
    KillSwitchLatched,
    ConfigConflict,
    HistoryProbe(io::Error),
    HistoryRead(JournalReadError),
    HistoryWrite(HistoryError),
    CorruptHistory,
    Exchange(ExchangeError),
    ObservationNotReached,
    UnsafeTerminal(OrderStatus),
    OutcomeUnknown,
    RetryDeferred {
        not_before: DateTime<Utc>,
    },
    QueryBudgetExhausted,
    PreviouslyFailed,
    ProtocolViolation,
    CounterOverflow,
}

impl fmt::Display for LiveLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotionalExceedsCap {
                notional,
                max_notional,
            } => {
                return write!(
                    formatter,
                    "the planned live order notional {notional} exceeds --max-notional {max_notional}"
                );
            }
            Self::AdmissionRefused(refusal) => {
                return write!(
                    formatter,
                    "mainnet venue-truth admission refused the live order before submit: {}",
                    refusal.label()
                );
            }
            _ => {}
        }
        formatter.write_str(match self {
            Self::InvalidConfig => "invalid Binance live lifecycle configuration",
            Self::NotionalExceedsCap { .. } | Self::AdmissionRefused(_) => unreachable!(),
            Self::KillSwitchLatched => {
                "the live lifecycle kill switch is latched in this journal; review the account and start a fresh journal before any new lifecycle"
            }
            Self::ConfigConflict => {
                "the durable live lifecycle identity conflicts with this configuration"
            }
            Self::HistoryProbe(_) => "unable to inspect the live lifecycle journal",
            Self::HistoryRead(_) => "unable to read bounded live lifecycle evidence",
            Self::HistoryWrite(_) => "unable to make the live lifecycle fact durable",
            Self::CorruptHistory => "live lifecycle evidence violates its fact contract",
            Self::Exchange(_) => "Binance mainnet returned a definite lifecycle failure",
            Self::ObservationNotReached => {
                "the expected live order observation was not reached before cleanup"
            }
            Self::UnsafeTerminal(_) => {
                "the live order reached an unexpected terminal state; the kill switch is now latched"
            }
            Self::OutcomeUnknown => {
                "the live lifecycle outcome is unresolved; rerun the same campaign to query first"
            }
            Self::RetryDeferred { .. } => {
                "the durable Binance retry-after deadline has not elapsed"
            }
            Self::QueryBudgetExhausted => {
                "the durable live lifecycle query budget is exhausted; reconcile manually"
            }
            Self::PreviouslyFailed => "the live lifecycle campaign is already failed",
            Self::ProtocolViolation => {
                "the live lifecycle response violated its identity contract"
            }
            Self::CounterOverflow => "the live lifecycle evidence counter overflowed",
        })
    }
}

impl std::error::Error for LiveLifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::HistoryProbe(error) => Some(error),
            Self::HistoryRead(error) => Some(error),
            Self::HistoryWrite(error) => Some(error),
            Self::Exchange(error) => Some(error),
            Self::InvalidConfig
            | Self::NotionalExceedsCap { .. }
            | Self::AdmissionRefused(_)
            | Self::KillSwitchLatched
            | Self::ConfigConflict
            | Self::CorruptHistory
            | Self::ObservationNotReached
            | Self::UnsafeTerminal(_)
            | Self::OutcomeUnknown
            | Self::RetryDeferred { .. }
            | Self::QueryBudgetExhausted
            | Self::PreviouslyFailed
            | Self::ProtocolViolation
            | Self::CounterOverflow => None,
        }
    }
}

impl From<JournalReadError> for LiveLifecycleError {
    fn from(error: JournalReadError) -> Self {
        Self::HistoryRead(error)
    }
}

impl From<HistoryError> for LiveLifecycleError {
    fn from(error: HistoryError) -> Self {
        Self::HistoryWrite(error)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Mutex, MutexGuard},
    };

    use chrono::{TimeZone, Utc};
    use crypto_trading_domain::{
        MarketType, Money, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol, TimeInForce,
    };
    use crypto_trading_exchange::{
        ExchangeOperation, ExchangeOperationKey, InstrumentRules, RemoteFailureMetadata,
    };
    use rust_decimal::Decimal;

    use super::*;

    struct FakeVenue {
        admission: Mutex<VecDeque<Result<LiveAdmissionTruth, ExchangeError>>>,
        submit: Mutex<VecDeque<Result<Order, ExchangeError>>>,
        query: Mutex<VecDeque<Result<Order, ExchangeError>>>,
        cancel: Mutex<VecDeque<Result<Order, ExchangeError>>>,
        calls: Mutex<Vec<&'static str>>,
        journal_path: Mutex<Option<std::path::PathBuf>>,
        planned_visible_before_mutation: Mutex<Vec<bool>>,
    }

    impl FakeVenue {
        fn new(
            admission: Vec<Result<LiveAdmissionTruth, ExchangeError>>,
            submit: Vec<Result<Order, ExchangeError>>,
            query: Vec<Result<Order, ExchangeError>>,
            cancel: Vec<Result<Order, ExchangeError>>,
        ) -> Self {
            Self {
                admission: Mutex::new(admission.into()),
                submit: Mutex::new(submit.into()),
                query: Mutex::new(query.into()),
                cancel: Mutex::new(cancel.into()),
                calls: Mutex::new(Vec::new()),
                journal_path: Mutex::new(None),
                planned_visible_before_mutation: Mutex::new(Vec::new()),
            }
        }

        fn watch_journal(self, path: &std::path::Path) -> Self {
            *lock(&self.journal_path) = Some(path.to_owned());
            self
        }

        fn calls(&self) -> Vec<&'static str> {
            lock(&self.calls).clone()
        }

        fn planned_probes(&self) -> Vec<bool> {
            lock(&self.planned_visible_before_mutation).clone()
        }

        fn probe_planned(&self) {
            if let Some(path) = lock(&self.journal_path).clone() {
                let body = std::fs::read_to_string(path).unwrap_or_default();
                lock(&self.planned_visible_before_mutation).push(body.contains(PLANNED));
            }
        }
    }

    impl LiveLifecycleVenue for FakeVenue {
        fn admission(&self) -> LiveLifecycleVenueFuture<'_, LiveAdmissionTruth> {
            lock(&self.calls).push("admission");
            self.probe_planned();
            let result = lock(&self.admission)
                .pop_front()
                .expect("missing admission fixture");
            Box::pin(async move { result })
        }

        fn submit(&self, _intent: OrderIntent) -> LiveLifecycleVenueFuture<'_, Order> {
            lock(&self.calls).push("submit");
            self.probe_planned();
            let result = lock(&self.submit)
                .pop_front()
                .expect("missing submit fixture");
            Box::pin(async move { result })
        }

        fn query(
            &self,
            _symbol: Symbol,
            _client_order_id: Uuid,
        ) -> LiveLifecycleVenueFuture<'_, Order> {
            lock(&self.calls).push("query");
            let result = lock(&self.query)
                .pop_front()
                .expect("missing query fixture");
            Box::pin(async move { result })
        }

        fn cancel(&self, _order_id: String) -> LiveLifecycleVenueFuture<'_, Order> {
            lock(&self.calls).push("cancel");
            let result = lock(&self.cancel)
                .pop_front()
                .expect("missing cancel fixture");
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn fresh_campaign_admits_then_proves_query_and_cancel_before_completion() {
        let config = config("fresh");
        let history = test_history("fresh");
        let venue = FakeVenue::new(
            vec![Ok(passing_admission())],
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        )
        .watch_journal(history.path());

        let report = run_live_lifecycle(&config, &venue, &passing_rules(&config), &history)
            .await
            .unwrap();

        assert_eq!(report.final_status, OrderStatus::Cancelled);
        assert_eq!(report.query_count, 2);
        assert!(!report.recovered);
        assert_eq!(
            venue.calls(),
            vec!["admission", "submit", "query", "cancel", "query"]
        );
        // The durable plan precedes both the admission read and the submit.
        assert_eq!(venue.planned_probes(), vec![true, true]);
        cleanup_history(history);
    }

    #[test]
    fn notional_above_the_required_cap_is_rejected_at_construction() {
        let intent = intent();
        let error = LiveLifecycleConfig::new(
            "campaign-cap",
            intent,
            "BTCUSDT",
            LiveLifecycleObservation::Open,
            Duration::from_millis(1),
            4,
            Decimal::ONE,
            false,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LiveLifecycleError::NotionalExceedsCap { .. }
        ));
    }

    #[test]
    fn non_spot_and_non_positive_cap_configurations_fail_closed() {
        let mut perpetual = intent();
        perpetual.market_type = MarketType::Perpetual;
        assert!(matches!(
            LiveLifecycleConfig::new(
                "campaign-perp",
                perpetual,
                "BTCUSDT",
                LiveLifecycleObservation::Open,
                Duration::from_millis(1),
                4,
                Decimal::from(100),
                false,
            ),
            Err(LiveLifecycleError::InvalidConfig)
        ));
        assert!(matches!(
            LiveLifecycleConfig::new(
                "campaign-zero-cap",
                intent(),
                "BTCUSDT",
                LiveLifecycleObservation::Open,
                Duration::from_millis(1),
                4,
                Decimal::ZERO,
                false,
            ),
            Err(LiveLifecycleError::InvalidConfig)
        ));
    }

    #[tokio::test]
    async fn filter_violations_are_refused_after_the_plan_and_before_submit() {
        let config = config("filters");
        let history = test_history("filters");
        let venue = FakeVenue::new(
            vec![Ok(passing_admission())],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        // Tick 1000 cannot admit price 49000.1.
        let rules = InstrumentRuleCatalog::new(vec![
            InstrumentRules::new(
                "binance",
                config.intent.symbol.clone(),
                MarketType::Spot,
                Price::new(Decimal::from(1_000)).unwrap(),
                Quantity::new(Decimal::new(1, 3)).unwrap(),
                Quantity::new(Decimal::new(1, 3)).unwrap(),
                Money::new(Decimal::from(5)),
            )
            .unwrap(),
        ])
        .unwrap();

        let error = run_live_lifecycle(&config, &venue, &rules, &history)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            LiveLifecycleError::AdmissionRefused(LiveAdmissionRefusal::FiltersViolated)
        ));
        assert_eq!(venue.calls(), vec!["admission"]);
        let body = std::fs::read_to_string(history.path()).unwrap();
        assert!(body.contains("admission_filters_violated"), "{body}");
        assert!(!body.contains(SUBMIT_OBSERVED), "{body}");
        cleanup_history(history);
    }

    #[tokio::test]
    async fn selling_more_than_the_free_base_balance_is_refused_before_submit() {
        let mut sell_intent = intent();
        sell_intent.side = Side::Sell;
        let config = LiveLifecycleConfig::new(
            "campaign-no-short",
            sell_intent,
            "BTCUSDT",
            LiveLifecycleObservation::Open,
            Duration::from_millis(1),
            4,
            Decimal::from(100),
            false,
        )
        .unwrap();
        let history = test_history("no-short");
        let truth = LiveAdmissionTruth {
            free_base_quantity: Decimal::ZERO,
            free_quote_amount: Decimal::from(1_000_000),
            open_order_client_ids: Vec::new(),
        };
        let venue = FakeVenue::new(vec![Ok(truth)], Vec::new(), Vec::new(), Vec::new());

        let error = run_live_lifecycle(&config, &venue, &passing_rules(&config), &history)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            LiveLifecycleError::AdmissionRefused(LiveAdmissionRefusal::InsufficientBaseBalance)
        ));
        assert_eq!(venue.calls(), vec!["admission"]);
        cleanup_history(history);
    }

    #[tokio::test]
    async fn buying_beyond_the_free_quote_balance_is_refused_before_submit() {
        let config = config("no-overspend");
        let history = test_history("no-overspend");
        let truth = LiveAdmissionTruth {
            free_base_quantity: Decimal::ONE,
            free_quote_amount: Decimal::ONE,
            open_order_client_ids: Vec::new(),
        };
        let venue = FakeVenue::new(vec![Ok(truth)], Vec::new(), Vec::new(), Vec::new());

        let error = run_live_lifecycle(&config, &venue, &passing_rules(&config), &history)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            LiveLifecycleError::AdmissionRefused(LiveAdmissionRefusal::InsufficientQuoteBalance)
        ));
        cleanup_history(history);
    }

    #[tokio::test]
    async fn foreign_open_orders_are_refused_unless_explicitly_allowed() {
        let config = config("foreign");
        let history = test_history("foreign");
        let truth = LiveAdmissionTruth {
            free_base_quantity: Decimal::ONE,
            free_quote_amount: Decimal::from(1_000_000),
            open_order_client_ids: vec![Some("someone-else".to_owned()), None],
        };
        let venue = FakeVenue::new(vec![Ok(truth.clone())], Vec::new(), Vec::new(), Vec::new());
        let error = run_live_lifecycle(&config, &venue, &passing_rules(&config), &history)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            LiveLifecycleError::AdmissionRefused(LiveAdmissionRefusal::ForeignOpenOrders)
        ));
        cleanup_history(history);

        let mut allow = config_with("foreign-allowed", intent(), Decimal::from(100), true);
        allow.maximum_queries = 4;
        let history = test_history("foreign-allowed");
        let venue = FakeVenue::new(
            vec![Ok(truth)],
            vec![Ok(order(&allow, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&allow, OrderStatus::Open, "0")),
                Ok(order(&allow, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&allow, OrderStatus::Cancelled, "0"))],
        );
        let report = run_live_lifecycle(&allow, &venue, &passing_rules(&allow), &history)
            .await
            .unwrap();
        assert_eq!(report.final_status, OrderStatus::Cancelled);
        cleanup_history(history);
    }

    #[tokio::test]
    async fn latched_kill_fact_blocks_a_fresh_campaign_before_any_venue_call() {
        let config = config("kill-blocked");
        let history = test_history("kill-blocked");
        history
            .append(&lifecycle_record(
                &config,
                KILL_SWITCH_ENGAGED,
                "kill_switch_engaged",
                json!({ "failure": "fixture", "engaged_by": "unsafe_terminal" }),
            ))
            .await
            .unwrap();
        assert!(live_lifecycle_kill_switch_latched(&history).unwrap());

        let fresh = config_with("kill-blocked-next", intent(), Decimal::from(100), false);
        let venue = FakeVenue::new(Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let error = run_live_lifecycle(&fresh, &venue, &passing_rules(&fresh), &history)
            .await
            .unwrap_err();

        assert!(matches!(error, LiveLifecycleError::KillSwitchLatched));
        assert!(venue.calls().is_empty());
        let body = std::fs::read_to_string(history.path()).unwrap();
        assert!(!body.contains("kill-blocked-next"), "{body}");
        cleanup_history(history);
    }

    #[tokio::test]
    async fn unsafe_terminal_outcomes_latch_the_kill_switch_for_later_campaigns() {
        let config = config("kill-latch");
        let history = test_history("kill-latch");
        let venue = FakeVenue::new(
            vec![Ok(passing_admission())],
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![Ok(order(&config, OrderStatus::Filled, "0.001"))],
            Vec::new(),
        );

        let error = run_live_lifecycle(&config, &venue, &passing_rules(&config), &history)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            LiveLifecycleError::UnsafeTerminal(OrderStatus::Filled)
        ));
        assert!(live_lifecycle_kill_switch_latched(&history).unwrap());

        let next = config_with("kill-latch-next", intent(), Decimal::from(100), false);
        let no_remote = FakeVenue::new(Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let blocked = run_live_lifecycle(&next, &no_remote, &passing_rules(&next), &history)
            .await
            .unwrap_err();
        assert!(matches!(blocked, LiveLifecycleError::KillSwitchLatched));
        assert!(no_remote.calls().is_empty());
        cleanup_history(history);
    }

    #[tokio::test]
    async fn pending_recovery_stays_query_first_even_with_a_latched_kill_fact() {
        let config = config("kill-recovery");
        let history = test_history("kill-recovery");
        let interrupted = FakeVenue::new(
            vec![Ok(passing_admission())],
            vec![Err(ambiguous_submit(&config))],
            vec![Err(ExchangeError::unavailable("fixture query unavailable"))],
            Vec::new(),
        );
        let first = run_live_lifecycle(&config, &interrupted, &passing_rules(&config), &history)
            .await
            .unwrap_err();
        assert!(matches!(first, LiveLifecycleError::OutcomeUnknown));

        history
            .append(&lifecycle_record(
                &config,
                KILL_SWITCH_ENGAGED,
                "kill_switch_engaged",
                json!({ "failure": "fixture", "engaged_by": "unsafe_terminal" }),
            ))
            .await
            .unwrap();
        assert!(live_lifecycle_kill_switch_latched(&history).unwrap());

        let recovered = FakeVenue::new(
            Vec::new(),
            Vec::new(),
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        );
        let report = run_live_lifecycle(&config, &recovered, &passing_rules(&config), &history)
            .await
            .unwrap();
        assert!(report.recovered);
        assert_eq!(recovered.calls(), vec!["query", "cancel", "query"]);
        cleanup_history(history);
    }

    #[tokio::test]
    async fn restart_after_ambiguous_submit_queries_first_and_never_resubmits() {
        let config = config("recover");
        let history = test_history("recover");
        let interrupted = FakeVenue::new(
            vec![Ok(passing_admission())],
            vec![Err(ambiguous_submit(&config))],
            vec![Err(ExchangeError::unavailable("fixture query unavailable"))],
            Vec::new(),
        );
        let first = run_live_lifecycle(&config, &interrupted, &passing_rules(&config), &history)
            .await
            .unwrap_err();
        assert!(matches!(first, LiveLifecycleError::OutcomeUnknown));
        assert!(!live_lifecycle_requires_submission(&config, &history).unwrap());

        let recovered = FakeVenue::new(
            Vec::new(),
            Vec::new(),
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        );
        let report = run_live_lifecycle(&config, &recovered, &passing_rules(&config), &history)
            .await
            .unwrap();

        assert!(report.recovered);
        assert_eq!(recovered.calls(), vec!["query", "cancel", "query"]);
        cleanup_history(history);
    }

    #[tokio::test]
    async fn completed_campaign_is_idempotent_and_performs_no_remote_calls() {
        let config = config("idempotent");
        let history = test_history("idempotent");
        let first_venue = FakeVenue::new(
            vec![Ok(passing_admission())],
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![
                Ok(order(&config, OrderStatus::Open, "0")),
                Ok(order(&config, OrderStatus::Cancelled, "0")),
            ],
            vec![Ok(order(&config, OrderStatus::Cancelled, "0"))],
        );
        run_live_lifecycle(&config, &first_venue, &passing_rules(&config), &history)
            .await
            .unwrap();

        let replay = FakeVenue::new(Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let report = run_live_lifecycle(&config, &replay, &passing_rules(&config), &history)
            .await
            .unwrap();

        assert!(report.recovered);
        assert!(replay.calls().is_empty());
        cleanup_history(history);
    }

    #[tokio::test]
    async fn persisted_retry_after_blocks_immediate_restart_before_remote_calls() {
        let config = config("retry-deferred");
        let history = test_history("retry-deferred");
        let interrupted = FakeVenue::new(
            vec![Ok(passing_admission())],
            vec![Ok(order(&config, OrderStatus::Open, "0"))],
            vec![Err(rate_limited_query(3_600))],
            Vec::new(),
        );
        let first = run_live_lifecycle(&config, &interrupted, &passing_rules(&config), &history)
            .await
            .unwrap_err();
        assert!(matches!(first, LiveLifecycleError::OutcomeUnknown));

        let no_remote = FakeVenue::new(Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let second = run_live_lifecycle(&config, &no_remote, &passing_rules(&config), &history)
            .await
            .unwrap_err();
        assert!(matches!(second, LiveLifecycleError::RetryDeferred { .. }));
        assert!(no_remote.calls().is_empty());
        cleanup_history(history);
    }

    fn ambiguous_submit(config: &LiveLifecycleConfig) -> ExchangeError {
        ExchangeError::AmbiguousOutcome {
            operation: ExchangeOperation::SubmitOrder,
            client_order_id: Some(config.intent.client_order_id),
            operation_key: Some(ExchangeOperationKey::ClientOrderId {
                client_order_id: config.intent.client_order_id,
            }),
            reason: "fixture disconnect".to_owned(),
        }
    }

    fn rate_limited_query(retry_after_seconds: u64) -> ExchangeError {
        ExchangeError::RemoteFailure {
            exchange: "binance".to_owned(),
            status: Some(429),
            reason: "query rate limited".to_owned(),
            metadata: RemoteFailureMetadata::default(),
        }
        .with_retry_after(RemoteRetryAfter::Seconds(retry_after_seconds))
    }

    fn intent() -> OrderIntent {
        let mut intent = OrderIntent::limit(
            "binance",
            Symbol::new("BTC-USDT-SPOT").unwrap(),
            MarketType::Spot,
            Side::Buy,
            Quantity::new(Decimal::new(1, 3)).unwrap(),
            Price::new(Decimal::from_str("49000.1").unwrap()).unwrap(),
        );
        intent.client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
        intent.time_in_force = TimeInForce::PostOnly;
        intent
    }

    fn config(suffix: &str) -> LiveLifecycleConfig {
        config_with(
            &format!("campaign-{suffix}"),
            intent(),
            Decimal::from(100),
            false,
        )
    }

    fn config_with(
        campaign_id: &str,
        intent: OrderIntent,
        max_notional: Decimal,
        allow_foreign_orders: bool,
    ) -> LiveLifecycleConfig {
        LiveLifecycleConfig::new(
            campaign_id,
            intent,
            "BTCUSDT",
            LiveLifecycleObservation::Open,
            Duration::from_millis(1),
            4,
            max_notional,
            allow_foreign_orders,
        )
        .unwrap()
    }

    fn passing_admission() -> LiveAdmissionTruth {
        LiveAdmissionTruth {
            free_base_quantity: Decimal::ONE,
            free_quote_amount: Decimal::from(1_000_000),
            open_order_client_ids: Vec::new(),
        }
    }

    fn passing_rules(config: &LiveLifecycleConfig) -> InstrumentRuleCatalog {
        InstrumentRuleCatalog::new(vec![
            InstrumentRules::new(
                "binance",
                config.intent.symbol.clone(),
                MarketType::Spot,
                Price::new(Decimal::new(1, 1)).unwrap(),
                Quantity::new(Decimal::new(1, 3)).unwrap(),
                Quantity::new(Decimal::new(1, 3)).unwrap(),
                Money::new(Decimal::from(5)),
            )
            .unwrap(),
        ])
        .unwrap()
    }

    fn order(config: &LiveLifecycleConfig, status: OrderStatus, filled: &str) -> Order {
        Order {
            id: "binance:spot:BTCUSDT:31".to_owned(),
            intent: config.intent.clone(),
            filled_quantity: Quantity::new(Decimal::from_str(filled).unwrap()).unwrap(),
            average_fill_price: (!Decimal::from_str(filled).unwrap().is_zero())
                .then(|| Price::new(Decimal::from_str("49000.1").unwrap()).unwrap()),
            status,
            created_at: Utc.with_ymd_and_hms(2026, 8, 13, 9, 10, 11).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 8, 13, 9, 10, 12).unwrap(),
        }
    }

    fn test_history(label: &str) -> JsonlHistory {
        JsonlHistory::new(std::env::temp_dir().join(format!(
            "crypto-trading-live-lifecycle-{label}-{}.jsonl",
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
