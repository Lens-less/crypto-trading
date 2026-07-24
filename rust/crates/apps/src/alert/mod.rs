//! Durable, read-only price-alert evaluation and bounded local notification.
//!
//! The service consumes only the stable market-data plane and pure alert
//! strategy. It owns no exchange handle, execution mode, order router, or live
//! authority. Alert facts are journaled before any adapter is enqueued.

mod journal;
mod notification;

use std::{fmt, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_config::PriceAlertConfig;
use crypto_trading_domain::Price;
use crypto_trading_runtime::{
    HistoryError, JournalPageBoundary, JournalSnapshot, JsonlHistory, LegacyJsonlJournalReader,
    MarketDataBook, MarketDataClock, MarketDataError, MarketDataEvent, MarketDataUpdate,
    MarketFreshnessPolicy, MarketInstrument, MarketUniverse, ReadModelError,
};
use crypto_trading_strategy::{AlertState, AlertStrategy, PriceAlert, StrategyError};
use rust_decimal::Decimal;

use journal::{
    RecoveredAlertFact, acknowledgement_record, occurrence_record, parse_alert_fact,
    pending_record, sample_record,
};

pub use journal::PRICE_ALERT_EVENT_SCHEMA_VERSION;
use notification::NotificationDispatcher;
pub use notification::{
    AlertDeliveryMode, AlertNotification, AlertNotificationAdapter, AlertNotificationFuture,
    AlertOccurrenceId, DeterministicNotificationAdapter, DeterministicNotificationProbe,
    LocalNoticeNotificationAdapter, LocalNoticeReceiver, NotificationConfigError,
    NotificationDispatcherConfig, NotificationDispatcherExit, NotificationDispatcherStatus,
    NotificationEnqueue, NotificationEnqueueState, NotificationFailure,
};

/// Maximum recent occurrences retained for in-process acknowledgement.
pub const MAX_RECENT_ALERT_OCCURRENCES: usize = 256;

const MAX_RECOVERABLE_ALERT_SAMPLES: u64 = 100_000;

/// Runtime mode, queue budgets, and recent acknowledgement window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceAlertRuntimeConfig {
    delivery_mode: AlertDeliveryMode,
    dispatcher: NotificationDispatcherConfig,
    recent_occurrence_limit: usize,
}

impl PriceAlertRuntimeConfig {
    /// Creates a bounded runtime configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PriceAlertRuntimeError::InvalidConfig`] for a zero or
    /// oversized recent occurrence window.
    pub fn new(
        delivery_mode: AlertDeliveryMode,
        dispatcher: NotificationDispatcherConfig,
        recent_occurrence_limit: usize,
    ) -> Result<Self, PriceAlertRuntimeError> {
        if recent_occurrence_limit == 0 || recent_occurrence_limit > MAX_RECENT_ALERT_OCCURRENCES {
            return Err(PriceAlertRuntimeError::InvalidConfig(
                "invalid recent alert occurrence limit",
            ));
        }
        Ok(Self {
            delivery_mode,
            dispatcher,
            recent_occurrence_limit,
        })
    }
}

#[derive(Debug, Clone)]
struct BoundAlertRule {
    instrument: MarketInstrument,
    strategy: AlertStrategy,
    state: AlertState,
    maximum_window: Option<Duration>,
}

/// One stable alert occurrence committed to the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertOccurrence {
    pub id: AlertOccurrenceId,
    pub kind: crypto_trading_strategy::AlertKind,
    pub price: Price,
    pub change_percent: Option<Decimal>,
    pub occurred_at: DateTime<Utc>,
}

impl AlertOccurrence {
    fn notification(&self) -> AlertNotification {
        AlertNotification {
            occurrence_id: self.id.clone(),
            kind: self.kind,
            price: self.price,
            change_percent: self.change_percent,
            occurred_at: self.occurred_at,
        }
    }
}

/// Recent bounded acknowledgement projection owned by the local service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertOccurrenceStatus {
    pub occurrence: AlertOccurrence,
    pub acknowledged_at: Option<DateTime<Utc>>,
}

/// Idempotent acknowledgement outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertAcknowledgementOutcome {
    Recorded,
    AlreadyAcknowledged,
}

/// Result of applying one market event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceAlertProcessReport {
    pub market_update: MarketDataUpdate,
    pub occurrences: Vec<AlertOccurrence>,
    pub notification_enqueues: Vec<NotificationEnqueue>,
    pub notification_status_persistence_failures: u64,
}

struct PreparedAlertUpdate {
    market_update: MarketDataUpdate,
    rule_index: Option<usize>,
    next_state: Option<AlertState>,
    next_occurrence_sequence: u64,
    records: Vec<crypto_trading_runtime::DecisionRecord>,
    occurrences: Vec<AlertOccurrence>,
}

/// Multi-symbol, journal-first, read-only price-alert application service.
///
/// `process` may await durable journal I/O, but it never awaits a notification
/// adapter. Every adapter is isolated behind a fixed-capacity `try_send` queue
/// and a hard delivery timeout.
#[derive(Debug)]
pub struct PriceAlertRuntime {
    book: MarketDataBook,
    rules: Vec<BoundAlertRule>,
    history: JsonlHistory,
    notifications: NotificationDispatcher,
    occurrence_sequence: u64,
    recent_occurrences: Vec<AlertOccurrenceStatus>,
    recent_occurrence_limit: usize,
    recovered: bool,
    failed: bool,
}

impl PriceAlertRuntime {
    /// Builds a read-only runtime from the compatibility configuration.
    ///
    /// The constructor validates a non-empty, duplicate-free enabled universe,
    /// notification-safe identities, positive sampling cadence, and enough
    /// bounded strategy history capacity for every configured volatility
    /// window.
    ///
    /// # Errors
    ///
    /// Returns [`PriceAlertRuntimeError`] for unsafe config, strategy, market,
    /// or notification bounds.
    pub fn new<C>(
        config: &PriceAlertConfig,
        freshness: MarketFreshnessPolicy,
        clock: Arc<C>,
        history: JsonlHistory,
        runtime: PriceAlertRuntimeConfig,
        adapters: Vec<Box<dyn AlertNotificationAdapter>>,
    ) -> Result<Self, PriceAlertRuntimeError>
    where
        C: MarketDataClock + 'static,
    {
        validate_sampling_budget(config)?;
        validate_notification_text(&config.exchange)?;
        let mut rules = config
            .symbols
            .iter()
            .filter(|symbol| symbol.enabled)
            .map(|symbol| {
                validate_notification_text(symbol.symbol.as_str())?;
                let instrument = MarketInstrument::new(
                    &config.exchange,
                    symbol.symbol.clone(),
                    symbol.market_type,
                )?;
                let strategy = AlertStrategy::from_config(config, &symbol.symbol)?;
                let maximum_window = strategy
                    .config()
                    .volatility
                    .as_ref()
                    .map(|volatility| volatility.window);
                Ok(BoundAlertRule {
                    instrument,
                    strategy,
                    state: AlertState::default(),
                    maximum_window,
                })
            })
            .collect::<Result<Vec<_>, PriceAlertRuntimeError>>()?;
        rules.sort_by(|left, right| left.instrument.cmp(&right.instrument));
        if rules.is_empty() {
            return Err(PriceAlertRuntimeError::InvalidConfig(
                "price alert requires at least one enabled symbol",
            ));
        }
        if rules
            .windows(2)
            .any(|pair| pair[0].instrument == pair[1].instrument)
        {
            return Err(PriceAlertRuntimeError::InvalidConfig(
                "price alert instruments must be unique",
            ));
        }
        let universe =
            MarketUniverse::new(rules.iter().map(|rule| rule.instrument.clone()).collect())?;
        let notification_clock: Arc<dyn MarketDataClock> = clock.clone();
        let notifications = NotificationDispatcher::start(
            runtime.delivery_mode,
            runtime.dispatcher,
            &history,
            notification_clock,
            adapters,
        )?;
        Ok(Self {
            book: MarketDataBook::new(universe, freshness, clock),
            rules,
            history,
            notifications,
            occurrence_sequence: 0,
            recent_occurrences: Vec::new(),
            recent_occurrence_limit: runtime.recent_occurrence_limit,
            recovered: false,
            failed: false,
        })
    }

    /// Applies one stable market event, journals samples and occurrences, and
    /// only then attempts non-blocking adapter enqueue.
    ///
    /// # Errors
    ///
    /// Returns [`PriceAlertRuntimeError`] for market/strategy contract failure
    /// or occurrence journal failure. A delivery failure is returned in status
    /// and never fails the market-processing path.
    pub async fn process(
        &mut self,
        event: MarketDataEvent,
    ) -> Result<PriceAlertProcessReport, PriceAlertRuntimeError> {
        if self.failed {
            return Err(PriceAlertRuntimeError::RuntimeFailed);
        }
        let mut prepared = match self.prepare(event) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };

        let adapter_ids = self
            .notifications
            .adapter_ids()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for occurrence in &prepared.occurrences {
            let notification = occurrence.notification();
            prepared.records.extend(
                adapter_ids
                    .iter()
                    .map(|adapter_id| pending_record(&notification, adapter_id)),
            );
        }
        if let Err(error) = self.history.append_batch(&prepared.records).await {
            self.failed = true;
            return Err(error.into());
        }

        if let (Some(rule_index), Some(next_state)) =
            (prepared.rule_index, prepared.next_state.take())
        {
            self.rules[rule_index].state = next_state;
        }
        self.occurrence_sequence = prepared.next_occurrence_sequence;
        for occurrence in &prepared.occurrences {
            self.push_recent(occurrence.clone(), None);
        }

        let mut enqueues = Vec::new();
        let mut persistence_failures = 0u64;
        for occurrence in &prepared.occurrences {
            let notification = occurrence.notification();
            let occurrence_enqueues = self.notifications.enqueue(&notification);
            persistence_failures = persistence_failures.saturating_add(
                self.notifications
                    .persist_enqueue_failures(&self.history, &notification, &occurrence_enqueues)
                    .await,
            );
            enqueues.extend(occurrence_enqueues);
        }

        Ok(PriceAlertProcessReport {
            market_update: prepared.market_update,
            occurrences: prepared.occurrences,
            notification_enqueues: enqueues,
            notification_status_persistence_failures: persistence_failures,
        })
    }

    /// Restores price history, cooldowns, recent occurrences, and
    /// acknowledgements from one immutable journal generation.
    ///
    /// Delivery attempts are validated but never replayed automatically.
    ///
    /// # Errors
    ///
    /// Returns [`PriceAlertRuntimeError`] for malformed, partial, out-of-order,
    /// or scope-inconsistent alert history.
    pub fn recover(&mut self, snapshot: &JournalSnapshot) -> Result<(), PriceAlertRuntimeError> {
        if self.recovered
            || self.book.view().generation != 0
            || self.occurrence_sequence != 0
            || !self.recent_occurrences.is_empty()
        {
            return Err(PriceAlertRuntimeError::Recovery(
                "price alert recovery must run before market processing",
            ));
        }

        let original_rules = self.rules.clone();
        let result = self.recover_snapshot(snapshot);
        if let Err(error) = result {
            self.rules = original_rules;
            self.occurrence_sequence = 0;
            self.recent_occurrences.clear();
            return Err(error);
        }
        self.recovered = true;
        Ok(())
    }

    /// Persists an exact, local-only acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`PriceAlertRuntimeError::UnknownOccurrence`] for an occurrence
    /// outside the bounded recent window or a timestamp preceding occurrence.
    pub async fn acknowledge(
        &mut self,
        occurrence_id: &AlertOccurrenceId,
        acknowledged_at: DateTime<Utc>,
    ) -> Result<AlertAcknowledgementOutcome, PriceAlertRuntimeError> {
        let Some(index) = self
            .recent_occurrences
            .iter()
            .position(|status| status.occurrence.id == *occurrence_id)
        else {
            return Err(PriceAlertRuntimeError::UnknownOccurrence);
        };
        let status = &self.recent_occurrences[index];
        if acknowledged_at < status.occurrence.occurred_at {
            return Err(PriceAlertRuntimeError::UnknownOccurrence);
        }
        if status.acknowledged_at.is_some() {
            return Ok(AlertAcknowledgementOutcome::AlreadyAcknowledged);
        }
        self.history
            .append(&acknowledgement_record(occurrence_id, acknowledged_at))
            .await?;
        self.recent_occurrences[index].acknowledged_at = Some(acknowledged_at);
        Ok(AlertAcknowledgementOutcome::Recorded)
    }

    pub fn recent_occurrences(&self) -> &[AlertOccurrenceStatus] {
        &self.recent_occurrences
    }

    pub fn notification_status(&self) -> NotificationDispatcherStatus {
        self.notifications.status()
    }

    /// Stops all adapter workers within the configured grace period.
    pub async fn stop(&mut self) -> NotificationDispatcherExit {
        self.notifications.stop().await
    }

    fn recover_snapshot(
        &mut self,
        snapshot: &JournalSnapshot,
    ) -> Result<(), PriceAlertRuntimeError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())
                .map_err(ReadModelError::Journal)?;
            for event in page.events() {
                let fact = parse_alert_fact(event)
                    .map_err(|()| PriceAlertRuntimeError::Recovery("invalid price alert event"))?;
                if let Some(fact) = fact {
                    self.apply_recovered_fact(fact)?;
                }
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => return Ok(()),
                JournalPageBoundary::PartialTail { .. } => {
                    return Err(PriceAlertRuntimeError::Recovery(
                        "price alert journal has a partial tail",
                    ));
                }
                JournalPageBoundary::PageLimit => {
                    let next = page
                        .next_cursor()
                        .cloned()
                        .ok_or(ReadModelError::NonAdvancingPage)?;
                    if cursor.as_ref().is_some_and(|previous| {
                        previous.next_offset() == next.next_offset()
                            && previous.after_sequence() == next.after_sequence()
                    }) {
                        return Err(ReadModelError::NonAdvancingPage.into());
                    }
                    cursor = Some(next);
                }
            }
        }
    }

    fn prepare(
        &mut self,
        event: MarketDataEvent,
    ) -> Result<PreparedAlertUpdate, PriceAlertRuntimeError> {
        let observation_instrument = match &event {
            MarketDataEvent::Observation(observation) => {
                Some(MarketInstrument::from_snapshot(&observation.snapshot)?)
            }
            MarketDataEvent::SourceGap { .. } | MarketDataEvent::SourceUnavailable { .. } => None,
        };
        let market_update = self.book.apply(event)?;
        if market_update != MarketDataUpdate::Accepted {
            return Ok(PreparedAlertUpdate {
                market_update,
                rule_index: None,
                next_state: None,
                next_occurrence_sequence: self.occurrence_sequence,
                records: Vec::new(),
                occurrences: Vec::new(),
            });
        }
        let instrument = observation_instrument.ok_or(PriceAlertRuntimeError::Recovery(
            "accepted event did not contain an observation",
        ))?;
        let view = self.book.view();
        let row = view.instrument(&instrument).ok_or_else(|| {
            MarketDataError::InstrumentOutsideUniverse {
                instrument: instrument.clone(),
            }
        })?;
        if !row.is_ready() {
            return Ok(PreparedAlertUpdate {
                market_update,
                rule_index: None,
                next_state: None,
                next_occurrence_sequence: self.occurrence_sequence,
                records: Vec::new(),
                occurrences: Vec::new(),
            });
        }
        let snapshot = row
            .snapshot
            .as_ref()
            .ok_or(PriceAlertRuntimeError::Recovery(
                "ready alert row omitted its snapshot",
            ))?;
        let revision = row.revision.ok_or(PriceAlertRuntimeError::Recovery(
            "ready alert row omitted its revision",
        ))?;
        let rule_index = self
            .rules
            .binary_search_by(|rule| rule.instrument.cmp(&instrument))
            .map_err(|_| PriceAlertRuntimeError::Recovery("alert rule is missing"))?;
        let rule = &self.rules[rule_index];
        let mut next_state = rule.state.clone();
        if let Some(window) = rule.maximum_window {
            let cutoff = snapshot.timestamp.checked_sub_signed(window).ok_or(
                PriceAlertRuntimeError::Recovery("alert history cutoff is not representable"),
            )?;
            next_state.prune_before(cutoff);
        }
        let alerts = rule.strategy.evaluate(&next_state, snapshot)?;
        for alert in &alerts {
            next_state.record_alert(alert.kind, alert.timestamp);
        }
        let current_price = snapshot.last.unwrap_or_else(|| snapshot.mid_price());
        next_state.record_price(snapshot.timestamp, current_price)?;

        let mut sequence = self.occurrence_sequence;
        let mut occurrences = Vec::with_capacity(alerts.len());
        for alert in &alerts {
            sequence = sequence
                .checked_add(1)
                .ok_or(PriceAlertRuntimeError::OccurrenceSequenceExhausted)?;
            occurrences.push(alert_occurrence(instrument.clone(), sequence, alert));
        }
        let mut records = Vec::with_capacity(occurrences.len().saturating_add(1));
        records.push(sample_record(
            &instrument,
            snapshot.timestamp,
            revision,
            view.generation,
            current_price,
        ));
        records.extend(occurrences.iter().map(|occurrence| {
            occurrence_record(&occurrence.notification(), revision, view.generation)
        }));
        Ok(PreparedAlertUpdate {
            market_update,
            rule_index: Some(rule_index),
            next_state: Some(next_state),
            next_occurrence_sequence: sequence,
            records,
            occurrences,
        })
    }

    fn apply_recovered_fact(
        &mut self,
        fact: RecoveredAlertFact,
    ) -> Result<(), PriceAlertRuntimeError> {
        match fact {
            RecoveredAlertFact::Sample {
                instrument,
                recorded_at,
                price,
            } => {
                let rule = self.rule_mut(&instrument)?;
                if let Some(window) = rule.maximum_window {
                    let cutoff = recorded_at.checked_sub_signed(window).ok_or(
                        PriceAlertRuntimeError::Recovery(
                            "recovered alert cutoff is not representable",
                        ),
                    )?;
                    rule.state.prune_before(cutoff);
                }
                rule.state.record_price(recorded_at, price)?;
            }
            RecoveredAlertFact::Occurrence { notification } => {
                let expected = self
                    .occurrence_sequence
                    .checked_add(1)
                    .ok_or(PriceAlertRuntimeError::OccurrenceSequenceExhausted)?;
                if notification.occurrence_id.sequence != expected {
                    return Err(PriceAlertRuntimeError::Recovery(
                        "price alert occurrence sequence is not contiguous",
                    ));
                }
                let rule = self.rule_mut(&notification.occurrence_id.instrument)?;
                rule.state
                    .record_alert(notification.kind, notification.occurred_at);
                self.occurrence_sequence = expected;
                self.push_recent(alert_occurrence_from_notification(notification), None);
            }
            RecoveredAlertFact::Acknowledged {
                occurrence_id,
                acknowledged_at,
            } => {
                if let Some(status) = self
                    .recent_occurrences
                    .iter_mut()
                    .find(|status| status.occurrence.id == occurrence_id)
                {
                    if acknowledged_at < status.occurrence.occurred_at
                        || status.acknowledged_at.is_some()
                    {
                        return Err(PriceAlertRuntimeError::Recovery(
                            "invalid or duplicate price alert acknowledgement",
                        ));
                    }
                    status.acknowledged_at = Some(acknowledged_at);
                } else {
                    return Err(PriceAlertRuntimeError::Recovery(
                        "acknowledgement references an unknown occurrence",
                    ));
                }
            }
            RecoveredAlertFact::Delivery { occurrence_id } => {
                let _ = self.rule_mut(&occurrence_id.instrument)?;
                if occurrence_id.sequence > self.occurrence_sequence {
                    return Err(PriceAlertRuntimeError::Recovery(
                        "delivery references a future occurrence",
                    ));
                }
                let retained = self
                    .recent_occurrences
                    .iter()
                    .find(|status| status.occurrence.id.sequence == occurrence_id.sequence);
                if let Some(status) = retained {
                    if status.occurrence.id != occurrence_id {
                        return Err(PriceAlertRuntimeError::Recovery(
                            "delivery occurrence identity is inconsistent",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn rule_mut(
        &mut self,
        instrument: &MarketInstrument,
    ) -> Result<&mut BoundAlertRule, PriceAlertRuntimeError> {
        let index = self
            .rules
            .binary_search_by(|rule| rule.instrument.cmp(instrument))
            .map_err(|_| {
                PriceAlertRuntimeError::Recovery("alert event is outside runtime scope")
            })?;
        Ok(&mut self.rules[index])
    }

    fn push_recent(&mut self, occurrence: AlertOccurrence, acknowledged_at: Option<DateTime<Utc>>) {
        if self.recent_occurrences.len() >= self.recent_occurrence_limit {
            self.recent_occurrences.remove(0);
        }
        self.recent_occurrences.push(AlertOccurrenceStatus {
            occurrence,
            acknowledged_at,
        });
    }
}

fn alert_occurrence(
    instrument: MarketInstrument,
    sequence: u64,
    alert: &PriceAlert,
) -> AlertOccurrence {
    AlertOccurrence {
        id: AlertOccurrenceId {
            instrument,
            sequence,
        },
        kind: alert.kind,
        price: alert.price,
        change_percent: alert.change_percent,
        occurred_at: alert.timestamp,
    }
}

fn alert_occurrence_from_notification(notification: AlertNotification) -> AlertOccurrence {
    AlertOccurrence {
        id: notification.occurrence_id,
        kind: notification.kind,
        price: notification.price,
        change_percent: notification.change_percent,
        occurred_at: notification.occurred_at,
    }
}

fn validate_sampling_budget(config: &PriceAlertConfig) -> Result<(), PriceAlertRuntimeError> {
    if config.refresh_interval_seconds <= Decimal::ZERO {
        return Err(PriceAlertRuntimeError::InvalidConfig(
            "price alert refresh interval must be positive",
        ));
    }
    let recoverable_coverage = config
        .refresh_interval_seconds
        .checked_mul(Decimal::from(
            MAX_RECOVERABLE_ALERT_SAMPLES.saturating_sub(1),
        ))
        .ok_or(PriceAlertRuntimeError::InvalidConfig(
            "price alert sampling budget overflowed",
        ))?;
    if config.symbols.iter().any(|symbol| {
        symbol.enabled
            && symbol.volatility_alert.enabled
            && Decimal::from(symbol.volatility_alert.time_window_seconds) > recoverable_coverage
    }) {
        return Err(PriceAlertRuntimeError::InvalidConfig(
            "price alert volatility window exceeds bounded sample capacity",
        ));
    }
    Ok(())
}

fn validate_notification_text(value: &str) -> Result<(), PriceAlertRuntimeError> {
    if value.chars().any(char::is_control) {
        return Err(PriceAlertRuntimeError::InvalidConfig(
            "notification identity contains control characters",
        ));
    }
    Ok(())
}

/// Fatal evaluator/persistence errors and local acknowledgement rejection.
#[derive(Debug)]
pub enum PriceAlertRuntimeError {
    InvalidConfig(&'static str),
    RuntimeFailed,
    OccurrenceSequenceExhausted,
    UnknownOccurrence,
    Recovery(&'static str),
    Market(MarketDataError),
    Strategy(StrategyError),
    History(HistoryError),
    Journal(ReadModelError),
    NotificationConfig(NotificationConfigError),
}

impl fmt::Display for PriceAlertRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(detail) => {
                write!(formatter, "invalid price alert config: {detail}")
            }
            Self::RuntimeFailed => formatter.write_str("price alert runtime is already failed"),
            Self::OccurrenceSequenceExhausted => {
                formatter.write_str("price alert occurrence sequence is exhausted")
            }
            Self::UnknownOccurrence => formatter.write_str("unknown price alert occurrence"),
            Self::Recovery(detail) => write!(formatter, "price alert recovery failed: {detail}"),
            Self::Market(error) => write!(formatter, "price alert market failure: {error}"),
            Self::Strategy(error) => write!(formatter, "price alert strategy failure: {error}"),
            Self::History(error) => write!(formatter, "price alert history failure: {error}"),
            Self::Journal(error) => write!(formatter, "price alert journal failure: {error}"),
            Self::NotificationConfig(error) => {
                write!(
                    formatter,
                    "price alert notification config failure: {error}"
                )
            }
        }
    }
}

impl std::error::Error for PriceAlertRuntimeError {}

impl From<MarketDataError> for PriceAlertRuntimeError {
    fn from(error: MarketDataError) -> Self {
        Self::Market(error)
    }
}

impl From<StrategyError> for PriceAlertRuntimeError {
    fn from(error: StrategyError) -> Self {
        Self::Strategy(error)
    }
}

impl From<HistoryError> for PriceAlertRuntimeError {
    fn from(error: HistoryError) -> Self {
        Self::History(error)
    }
}

impl From<ReadModelError> for PriceAlertRuntimeError {
    fn from(error: ReadModelError) -> Self {
        Self::Journal(error)
    }
}

impl From<NotificationConfigError> for PriceAlertRuntimeError {
    fn from(error: NotificationConfigError) -> Self {
        Self::NotificationConfig(error)
    }
}
