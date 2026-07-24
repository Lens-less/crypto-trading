use std::collections::HashMap;

use chrono::{DateTime, Utc};
use crypto_trading_domain::MarketType;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    JournalPageBoundary, JournalSnapshot, LegacyJsonlJournalReader, OperationEventEnvelope,
    ProjectionStatus, ReadModelError,
};

pub const PRICE_ALERT_READ_MODEL_SCHEMA_VERSION: u16 = 1;
pub const MAX_ALERT_READ_MODEL_OCCURRENCES: usize = 256;

const PRICE_ALERT_STRATEGY: &str = "price_alert";
const PRICE_ALERT_SAMPLED: &str = "price_alert_sampled";
const PRICE_ALERT_OCCURRED: &str = "price_alert_occurred";
const PRICE_ALERT_DELIVERY_PENDING: &str = "price_alert_delivery_pending";
const PRICE_ALERT_DELIVERY_DROPPED: &str = "price_alert_delivery_dropped";
const PRICE_ALERT_DELIVERY_SUCCEEDED: &str = "price_alert_delivery_succeeded";
const PRICE_ALERT_DELIVERY_FAILED: &str = "price_alert_delivery_failed";
const PRICE_ALERT_DELIVERY_TIMED_OUT: &str = "price_alert_delivery_timed_out";
const PRICE_ALERT_ACKNOWLEDGED: &str = "price_alert_acknowledged";

const MAX_ALERT_TEXT_BYTES: usize = 128;
const MAX_DECIMAL_TEXT_BYTES: usize = 64;
const MAX_ADAPTER_ID_BYTES: usize = 64;
const MAX_ALERT_NOTIFICATION_ADAPTERS: usize = 8;
const MAX_EVICTED_PENDING_DELIVERIES: usize = 65_536;
const MAX_ALERT_SCOPES: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceAlertReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub journal_head_sequence: Option<u64>,
    pub boundary: JournalPageBoundary,
    pub projection_status: ProjectionStatus,
    pub occurrences: Vec<AlertOccurrenceView>,
    pub occurrences_truncated: bool,
    pub invalid_event_count: u64,
}

impl PriceAlertReadModel {
    /// Projects alert facts from one immutable legacy journal snapshot.
    ///
    /// Non-alert records are ignored. Any malformed alert fact, contradictory
    /// reference, or partial tail degrades the projection and clears retained
    /// occurrences to avoid surfacing an untrustworthy latest view.
    ///
    /// # Errors
    ///
    /// Returns [`ReadModelError`] for journal failures or a non-advancing page.
    pub fn from_legacy_snapshot(snapshot: &JournalSnapshot) -> Result<Self, ReadModelError> {
        ProjectionBuilder::new(snapshot.journal_id()).project(snapshot)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertOccurrenceKind {
    VolatilityUp,
    VolatilityDown,
    UpperLimit,
    LowerLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryStatus {
    Pending,
    Dropped,
    Succeeded,
    Failed,
    TimedOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertDeliveryFailure {
    Backpressure,
    AdapterClosed,
    DeviceUnavailable,
    Rejected,
    WorkerFailed,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertDeliveryView {
    pub adapter_id: String,
    pub status: AlertDeliveryStatus,
    pub failure: Option<AlertDeliveryFailure>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertOccurrenceView {
    pub source_sequence: u64,
    pub event_id: Uuid,
    pub alert_sequence: u64,
    pub recorded_at: DateTime<Utc>,
    pub exchange: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub kind: AlertOccurrenceKind,
    pub price: String,
    pub change_percent: Option<String>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub deliveries: Vec<AlertDeliveryView>,
}

struct ProjectionBuilder {
    journal_id: Uuid,
    head_sequence: Option<u64>,
    boundary: JournalPageBoundary,
    projection_status: ProjectionStatus,
    occurrences: Vec<AlertOccurrenceView>,
    truncated: bool,
    invalid_event_count: u64,
    last_alert_sequence: Option<u64>,
    last_occurrence_at_by_scope: HashMap<AlertScope, DateTime<Utc>>,
    evicted_pending: HashMap<u64, EvictedOccurrence>,
    evicted_pending_delivery_count: usize,
}

impl ProjectionBuilder {
    fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            head_sequence: None,
            boundary: JournalPageBoundary::SnapshotEnd,
            projection_status: ProjectionStatus::Complete,
            occurrences: Vec::new(),
            truncated: false,
            invalid_event_count: 0,
            last_alert_sequence: None,
            last_occurrence_at_by_scope: HashMap::new(),
            evicted_pending: HashMap::new(),
            evicted_pending_delivery_count: 0,
        }
    }

    fn project(
        mut self,
        snapshot: &JournalSnapshot,
    ) -> Result<PriceAlertReadModel, ReadModelError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            if let Some(event) = page.events().last() {
                self.head_sequence = Some(event.sequence());
            }
            for event in page.events() {
                self.apply_event(event);
            }
            self.boundary = page.boundary().clone();
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    self.record_invalid();
                    break;
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
                        return Err(ReadModelError::NonAdvancingPage);
                    }
                    cursor = Some(next);
                }
            }
        }
        Ok(PriceAlertReadModel {
            schema_version: PRICE_ALERT_READ_MODEL_SCHEMA_VERSION,
            journal_id: self.journal_id,
            journal_head_sequence: self.head_sequence,
            boundary: self.boundary,
            projection_status: self.projection_status,
            occurrences: self.occurrences,
            occurrences_truncated: self.truncated,
            invalid_event_count: self.invalid_event_count,
        })
    }

    fn apply_event(&mut self, event: &OperationEventEnvelope) {
        match parse_alert_event(event) {
            Ok(Some(fact)) if self.projection_status != ProjectionStatus::Degraded => {
                if self.apply_fact(fact).is_err() {
                    self.record_invalid();
                }
            }
            Ok(Some(_) | None) => {}
            Err(()) => self.record_invalid(),
        }
    }

    fn apply_fact(&mut self, fact: AlertFact) -> Result<(), ()> {
        match fact {
            AlertFact::Sample => Ok(()),
            AlertFact::Occurrence(occurrence) => self.apply_occurrence(occurrence),
            AlertFact::Delivery(delivery) => self.apply_delivery(delivery),
            AlertFact::Acknowledged(acknowledgement) => {
                self.apply_acknowledgement(&acknowledgement)
            }
        }
    }

    fn apply_occurrence(&mut self, occurrence: AlertOccurrenceView) -> Result<(), ()> {
        let expected_sequence = match self.last_alert_sequence {
            Some(last) => last.checked_add(1).ok_or(())?,
            None => 1,
        };
        if occurrence.alert_sequence != expected_sequence {
            return Err(());
        }

        let scope = AlertScope::from(&occurrence);
        if let Some(previous) = self.last_occurrence_at_by_scope.get_mut(&scope) {
            if occurrence.recorded_at < *previous {
                return Err(());
            }
            *previous = occurrence.recorded_at;
        } else {
            if self.last_occurrence_at_by_scope.len() >= MAX_ALERT_SCOPES {
                return Err(());
            }
            self.last_occurrence_at_by_scope
                .insert(scope, occurrence.recorded_at);
        }

        if self.occurrences.len() >= MAX_ALERT_READ_MODEL_OCCURRENCES {
            let evicted = self.occurrences.remove(0);
            self.retain_evicted_pending(evicted)?;
            self.truncated = true;
        }
        self.last_alert_sequence = Some(occurrence.alert_sequence);
        self.occurrences.push(occurrence);
        self.refresh_projection_status();
        Ok(())
    }

    fn apply_delivery(&mut self, delivery: DeliveryFact) -> Result<(), ()> {
        if let Some(occurrence) = self
            .occurrences
            .iter_mut()
            .find(|occurrence| occurrence.alert_sequence == delivery.alert_sequence)
        {
            return apply_retained_delivery(occurrence, delivery);
        }

        self.apply_evicted_delivery(&delivery)
    }

    fn retain_evicted_pending(&mut self, occurrence: AlertOccurrenceView) -> Result<(), ()> {
        let pending_deliveries = occurrence
            .deliveries
            .into_iter()
            .filter(|delivery| delivery.status == AlertDeliveryStatus::Pending)
            .collect::<Vec<_>>();
        if pending_deliveries.is_empty() {
            return Ok(());
        }
        let next_count = self
            .evicted_pending_delivery_count
            .checked_add(pending_deliveries.len())
            .ok_or(())?;
        if next_count > MAX_EVICTED_PENDING_DELIVERIES
            || self
                .evicted_pending
                .contains_key(&occurrence.alert_sequence)
        {
            return Err(());
        }
        self.evicted_pending.insert(
            occurrence.alert_sequence,
            EvictedOccurrence {
                exchange: occurrence.exchange,
                symbol: occurrence.symbol,
                market_type: occurrence.market_type,
                pending_deliveries,
            },
        );
        self.evicted_pending_delivery_count = next_count;
        Ok(())
    }

    fn apply_evicted_delivery(&mut self, delivery: &DeliveryFact) -> Result<(), ()> {
        let occurrence = self
            .evicted_pending
            .get_mut(&delivery.alert_sequence)
            .ok_or(())?;
        if occurrence.exchange != delivery.exchange
            || occurrence.symbol != delivery.symbol
            || occurrence.market_type != delivery.market_type
            || delivery.status == AlertDeliveryStatus::Pending
        {
            return Err(());
        }
        let index = occurrence
            .pending_deliveries
            .iter()
            .position(|existing| existing.adapter_id == delivery.adapter_id)
            .ok_or(())?;
        if delivery.recorded_at < occurrence.pending_deliveries[index].updated_at {
            return Err(());
        }
        occurrence.pending_deliveries.remove(index);
        self.evicted_pending_delivery_count = self
            .evicted_pending_delivery_count
            .checked_sub(1)
            .ok_or(())?;
        if occurrence.pending_deliveries.is_empty() {
            self.evicted_pending.remove(&delivery.alert_sequence);
        }
        Ok(())
    }

    fn apply_acknowledgement(&mut self, acknowledgement: &AcknowledgementFact) -> Result<(), ()> {
        let occurrence = self
            .occurrences
            .iter_mut()
            .find(|occurrence| occurrence.alert_sequence == acknowledgement.alert_sequence)
            .ok_or(())?;
        if occurrence.exchange != acknowledgement.exchange
            || occurrence.symbol != acknowledgement.symbol
            || occurrence.market_type != acknowledgement.market_type
            || acknowledgement.acknowledged_at < occurrence.recorded_at
            || occurrence.acknowledged_at.is_some()
        {
            return Err(());
        }
        occurrence.acknowledged_at = Some(acknowledgement.acknowledged_at);
        Ok(())
    }

    fn record_invalid(&mut self) {
        self.projection_status = ProjectionStatus::Degraded;
        self.invalid_event_count = self.invalid_event_count.saturating_add(1);
        self.occurrences.clear();
        self.last_alert_sequence = None;
        self.last_occurrence_at_by_scope.clear();
        self.evicted_pending.clear();
        self.evicted_pending_delivery_count = 0;
        self.truncated = false;
    }

    fn refresh_projection_status(&mut self) {
        if self.projection_status == ProjectionStatus::Degraded {
            return;
        }
        self.projection_status = if self.truncated {
            ProjectionStatus::Windowed
        } else {
            ProjectionStatus::Complete
        };
    }
}

fn apply_retained_delivery(
    occurrence: &mut AlertOccurrenceView,
    delivery: DeliveryFact,
) -> Result<(), ()> {
    if occurrence.exchange != delivery.exchange
        || occurrence.symbol != delivery.symbol
        || occurrence.market_type != delivery.market_type
        || delivery.recorded_at < occurrence.recorded_at
    {
        return Err(());
    }
    if let Some(existing) = occurrence
        .deliveries
        .iter_mut()
        .find(|existing| existing.adapter_id == delivery.adapter_id)
    {
        if existing.status != AlertDeliveryStatus::Pending
            || delivery.status == AlertDeliveryStatus::Pending
            || delivery.recorded_at < existing.updated_at
        {
            return Err(());
        }
        existing.status = delivery.status;
        existing.failure = delivery.failure;
        existing.updated_at = delivery.recorded_at;
    } else {
        if delivery.status != AlertDeliveryStatus::Pending
            || occurrence.deliveries.len() >= MAX_ALERT_NOTIFICATION_ADAPTERS
        {
            return Err(());
        }
        occurrence.deliveries.push(AlertDeliveryView {
            adapter_id: delivery.adapter_id,
            status: delivery.status,
            failure: delivery.failure,
            updated_at: delivery.recorded_at,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AlertScope {
    exchange: String,
    symbol: String,
    market_type: MarketType,
}

impl From<&AlertOccurrenceView> for AlertScope {
    fn from(occurrence: &AlertOccurrenceView) -> Self {
        Self {
            exchange: occurrence.exchange.clone(),
            symbol: occurrence.symbol.clone(),
            market_type: occurrence.market_type,
        }
    }
}

struct EvictedOccurrence {
    exchange: String,
    symbol: String,
    market_type: MarketType,
    pending_deliveries: Vec<AlertDeliveryView>,
}

enum AlertFact {
    Sample,
    Occurrence(AlertOccurrenceView),
    Delivery(DeliveryFact),
    Acknowledged(AcknowledgementFact),
}

struct DeliveryFact {
    alert_sequence: u64,
    exchange: String,
    symbol: String,
    market_type: MarketType,
    adapter_id: String,
    status: AlertDeliveryStatus,
    failure: Option<AlertDeliveryFailure>,
    recorded_at: DateTime<Utc>,
}

struct AcknowledgementFact {
    alert_sequence: u64,
    exchange: String,
    symbol: String,
    market_type: MarketType,
    acknowledged_at: DateTime<Utc>,
}

fn parse_alert_event(event: &OperationEventEnvelope) -> Result<Option<AlertFact>, ()> {
    let payload = object(event.payload())?;
    require_exact_keys(payload, &["strategy", "symbol", "decision", "details"])?;
    let strategy = required_text(payload, "strategy")?;
    if strategy != PRICE_ALERT_STRATEGY {
        return Ok(None);
    }
    let symbol = required_text(payload, "symbol")?;
    let decision = required_text(payload, "decision")?;
    let details = object(required(payload, "details")?)?;
    require_schema(details)?;

    match decision.as_str() {
        PRICE_ALERT_SAMPLED => parse_sample(details, &symbol).map(Some),
        PRICE_ALERT_OCCURRED => parse_occurrence(event, details, &symbol).map(Some),
        PRICE_ALERT_DELIVERY_PENDING
        | PRICE_ALERT_DELIVERY_DROPPED
        | PRICE_ALERT_DELIVERY_SUCCEEDED
        | PRICE_ALERT_DELIVERY_FAILED
        | PRICE_ALERT_DELIVERY_TIMED_OUT => {
            parse_delivery(event, details, &symbol, &decision).map(Some)
        }
        PRICE_ALERT_ACKNOWLEDGED => parse_acknowledgement(event, details, &symbol).map(Some),
        _ => Err(()),
    }
}

fn parse_sample(details: &Map<String, Value>, symbol: &str) -> Result<AlertFact, ()> {
    require_exact_keys(
        details,
        &[
            "schema_version",
            "exchange",
            "market_type",
            "revision",
            "market_generation",
            "price",
        ],
    )?;
    if required_u64(details, "revision")? == 0 || required_u64(details, "market_generation")? == 0 {
        return Err(());
    }
    let _ = required_exchange(details)?;
    let _ = required_market_type(details)?;
    let _ = required_symbol_text(symbol)?;
    let _ = required_decimal_text(details, "price")?;
    Ok(AlertFact::Sample)
}

fn parse_occurrence(
    event: &OperationEventEnvelope,
    details: &Map<String, Value>,
    symbol: &str,
) -> Result<AlertFact, ()> {
    require_exact_keys(
        details,
        &[
            "schema_version",
            "sequence",
            "exchange",
            "market_type",
            "kind",
            "price",
            "change_percent",
            "market_revision",
            "market_generation",
        ],
    )?;
    let alert_sequence = required_u64(details, "sequence")?;
    if alert_sequence == 0
        || required_u64(details, "market_revision")? == 0
        || required_u64(details, "market_generation")? == 0
    {
        return Err(());
    }
    let exchange = required_exchange(details)?;
    let symbol = required_symbol_text(symbol)?;
    let market_type = required_market_type(details)?;
    let price = required_decimal_text(details, "price")?;
    let change_percent = optional_decimal_text(details, "change_percent")?;
    let kind = parse_kind(&required_text(details, "kind")?)?;
    Ok(AlertFact::Occurrence(AlertOccurrenceView {
        source_sequence: event.sequence(),
        event_id: event.event_id(),
        alert_sequence,
        recorded_at: event.recorded_at(),
        exchange,
        symbol,
        market_type,
        kind,
        price,
        change_percent,
        acknowledged_at: None,
        deliveries: Vec::new(),
    }))
}

fn parse_delivery(
    event: &OperationEventEnvelope,
    details: &Map<String, Value>,
    symbol: &str,
    decision: &str,
) -> Result<AlertFact, ()> {
    require_exact_keys(
        details,
        &[
            "schema_version",
            "sequence",
            "exchange",
            "market_type",
            "adapter_id",
            "failure",
        ],
    )?;
    let alert_sequence = required_u64(details, "sequence")?;
    if alert_sequence == 0 {
        return Err(());
    }
    let exchange = required_exchange(details)?;
    let symbol = required_symbol_text(symbol)?;
    let market_type = required_market_type(details)?;
    let adapter_id = required_adapter_id(details)?;
    let (status, failure) = parse_delivery_outcome(decision, required(details, "failure")?)?;
    Ok(AlertFact::Delivery(DeliveryFact {
        alert_sequence,
        exchange,
        symbol,
        market_type,
        adapter_id,
        status,
        failure,
        recorded_at: event.recorded_at(),
    }))
}

fn parse_acknowledgement(
    event: &OperationEventEnvelope,
    details: &Map<String, Value>,
    symbol: &str,
) -> Result<AlertFact, ()> {
    require_exact_keys(
        details,
        &["schema_version", "sequence", "exchange", "market_type"],
    )?;
    let alert_sequence = required_u64(details, "sequence")?;
    if alert_sequence == 0 {
        return Err(());
    }
    Ok(AlertFact::Acknowledged(AcknowledgementFact {
        alert_sequence,
        exchange: required_exchange(details)?,
        symbol: required_symbol_text(symbol)?,
        market_type: required_market_type(details)?,
        acknowledged_at: event.recorded_at(),
    }))
}

fn parse_delivery_outcome(
    decision: &str,
    failure: &Value,
) -> Result<(AlertDeliveryStatus, Option<AlertDeliveryFailure>), ()> {
    match (decision, failure.as_str()) {
        (PRICE_ALERT_DELIVERY_PENDING, None) if failure.is_null() => {
            Ok((AlertDeliveryStatus::Pending, None))
        }
        (PRICE_ALERT_DELIVERY_SUCCEEDED, None) if failure.is_null() => {
            Ok((AlertDeliveryStatus::Succeeded, None))
        }
        (PRICE_ALERT_DELIVERY_DROPPED, Some("backpressure")) => Ok((
            AlertDeliveryStatus::Dropped,
            Some(AlertDeliveryFailure::Backpressure),
        )),
        (PRICE_ALERT_DELIVERY_DROPPED, Some("adapter_closed")) => Ok((
            AlertDeliveryStatus::Dropped,
            Some(AlertDeliveryFailure::AdapterClosed),
        )),
        (PRICE_ALERT_DELIVERY_FAILED, Some("device_unavailable")) => Ok((
            AlertDeliveryStatus::Failed,
            Some(AlertDeliveryFailure::DeviceUnavailable),
        )),
        (PRICE_ALERT_DELIVERY_FAILED, Some("backpressure")) => Ok((
            AlertDeliveryStatus::Failed,
            Some(AlertDeliveryFailure::Backpressure),
        )),
        (PRICE_ALERT_DELIVERY_FAILED, Some("rejected")) => Ok((
            AlertDeliveryStatus::Failed,
            Some(AlertDeliveryFailure::Rejected),
        )),
        (PRICE_ALERT_DELIVERY_FAILED, Some("worker_failed")) => Ok((
            AlertDeliveryStatus::Failed,
            Some(AlertDeliveryFailure::WorkerFailed),
        )),
        (PRICE_ALERT_DELIVERY_TIMED_OUT, Some("timeout")) => Ok((
            AlertDeliveryStatus::TimedOut,
            Some(AlertDeliveryFailure::Timeout),
        )),
        _ => Err(()),
    }
}

fn parse_kind(value: &str) -> Result<AlertOccurrenceKind, ()> {
    match value {
        "volatility_up" => Ok(AlertOccurrenceKind::VolatilityUp),
        "volatility_down" => Ok(AlertOccurrenceKind::VolatilityDown),
        "upper_limit" => Ok(AlertOccurrenceKind::UpperLimit),
        "lower_limit" => Ok(AlertOccurrenceKind::LowerLimit),
        _ => Err(()),
    }
}

fn require_schema(details: &Map<String, Value>) -> Result<(), ()> {
    if required_u64(details, "schema_version")? != u64::from(PRICE_ALERT_READ_MODEL_SCHEMA_VERSION)
    {
        return Err(());
    }
    Ok(())
}

fn require_exact_keys(object: &Map<String, Value>, expected: &[&str]) -> Result<(), ()> {
    if object.len() != expected.len() || object.keys().any(|key| !expected.contains(&key.as_str()))
    {
        return Err(());
    }
    Ok(())
}

fn required_exchange(details: &Map<String, Value>) -> Result<String, ()> {
    required_text(details, "exchange")
}

fn required_symbol_text(symbol: &str) -> Result<String, ()> {
    let trimmed = symbol.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_ALERT_TEXT_BYTES
        || trimmed.chars().any(char::is_control)
    {
        return Err(());
    }
    Ok(trimmed.to_owned())
}

fn required_market_type(details: &Map<String, Value>) -> Result<MarketType, ()> {
    match required_text(details, "market_type")?.as_str() {
        "spot" => Ok(MarketType::Spot),
        "perpetual" => Ok(MarketType::Perpetual),
        _ => Err(()),
    }
}

fn required_adapter_id(details: &Map<String, Value>) -> Result<String, ()> {
    let adapter_id = required_text(details, "adapter_id")?;
    if adapter_id.len() > MAX_ADAPTER_ID_BYTES
        || !adapter_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.".contains(&byte)
        })
    {
        return Err(());
    }
    Ok(adapter_id)
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ()> {
    object.get(key).ok_or(())
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Result<u64, ()> {
    required(object, key)?.as_u64().ok_or(())
}

fn required_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?;
    if value.is_empty()
        || value.len() > MAX_ALERT_TEXT_BYTES
        || value != value.trim()
        || value.chars().any(char::is_control)
    {
        return Err(());
    }
    Ok(value.to_owned())
}

fn required_decimal_text(object: &Map<String, Value>, key: &str) -> Result<String, ()> {
    let value = required(object, key)?.as_str().ok_or(())?;
    if value.is_empty()
        || value.len() > MAX_DECIMAL_TEXT_BYTES
        || !valid_decimal_text(value.as_bytes())
    {
        return Err(());
    }
    Ok(value.to_owned())
}

fn optional_decimal_text(object: &Map<String, Value>, key: &str) -> Result<Option<String>, ()> {
    let value = required(object, key)?;
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(required_decimal_text(object, key)?))
}

fn valid_decimal_text(value: &[u8]) -> bool {
    let mut digits = 0usize;
    let mut decimal_points = 0usize;
    for (index, byte) in value.iter().enumerate() {
        if byte.is_ascii_digit() {
            digits = digits.saturating_add(1);
        } else if *byte == b'.' {
            decimal_points = decimal_points.saturating_add(1);
        } else if *byte != b'-' || index != 0 {
            return false;
        }
    }
    digits > 0 && decimal_points <= 1
}

fn object(value: &Value) -> Result<&Map<String, Value>, ()> {
    value.as_object().ok_or(())
}
