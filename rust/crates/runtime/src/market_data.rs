use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Symbol};
use crypto_trading_exchange::{ExchangeError, SubscriptionReceipt};
use serde::Serialize;
use thiserror::Error;

/// Version of the stable read-only market-data view.
pub const MARKET_DATA_VIEW_SCHEMA_VERSION: u16 = 1;
/// Maximum number of exact instruments in one market-data universe.
pub const MAX_MARKET_DATA_TARGETS: usize = 4_096;
/// Maximum number of deterministic source events retained in memory.
pub const MAX_MARKET_DATA_EVENTS: usize = 8_192;

const MAX_EXCHANGE_NAME_BYTES: usize = 128;
pub(crate) const MAX_MARKET_SYMBOL_BYTES: usize = 128;

/// Exact market identity used by the read-only market-data plane.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct MarketInstrument {
    exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
}

impl MarketInstrument {
    /// Builds an exact, validated market identity.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] when the exchange name is empty or oversized.
    pub fn new(
        exchange: impl Into<String>,
        symbol: Symbol,
        market_type: MarketType,
    ) -> Result<Self, MarketDataError> {
        let exchange = validated_exchange(exchange)?;
        if symbol.as_str().len() > MAX_MARKET_SYMBOL_BYTES {
            return Err(MarketDataError::SymbolTooLong {
                bytes: symbol.as_str().len(),
                limit: MAX_MARKET_SYMBOL_BYTES,
            });
        }
        Ok(Self {
            exchange,
            symbol,
            market_type,
        })
    }

    /// Derives the exact identity carried by a validated domain snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] if the snapshot exchange identifier exceeds
    /// the read-plane bound.
    pub fn from_snapshot(snapshot: &MarketSnapshot) -> Result<Self, MarketDataError> {
        Self::new(
            snapshot.exchange(),
            snapshot.symbol.clone(),
            snapshot.market_type,
        )
    }

    pub fn exchange(&self) -> &str {
        &self.exchange
    }
}

impl PartialOrd for MarketInstrument {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MarketInstrument {
    fn cmp(&self, other: &Self) -> Ordering {
        self.exchange
            .cmp(&other.exchange)
            .then_with(|| self.symbol.cmp(&other.symbol))
            .then_with(|| {
                market_type_rank(self.market_type).cmp(&market_type_rank(other.market_type))
            })
    }
}

/// A non-empty, bounded set of exact market instruments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketUniverse {
    instruments: Vec<MarketInstrument>,
}

impl MarketUniverse {
    /// Sorts and deduplicates a caller-provided exact universe.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for an empty or oversized universe.
    pub fn new(mut instruments: Vec<MarketInstrument>) -> Result<Self, MarketDataError> {
        instruments.sort();
        instruments.dedup();
        if instruments.is_empty() {
            return Err(MarketDataError::EmptyUniverse);
        }
        if instruments.len() > MAX_MARKET_DATA_TARGETS {
            return Err(MarketDataError::UniverseTooLarge {
                count: instruments.len(),
                limit: MAX_MARKET_DATA_TARGETS,
            });
        }
        Ok(Self { instruments })
    }

    pub fn instruments(&self) -> &[MarketInstrument] {
        &self.instruments
    }

    pub fn contains(&self, instrument: &MarketInstrument) -> bool {
        self.instruments.binary_search(instrument).is_ok()
    }

    fn contains_exchange(&self, exchange: &str) -> bool {
        self.instruments
            .iter()
            .any(|instrument| instrument.exchange() == exchange)
    }
}

/// Freshness bounds applied at read time rather than at ingest time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketFreshnessPolicy {
    maximum_age: Duration,
    future_tolerance: Duration,
    pair_skew_bound: Duration,
}

impl MarketFreshnessPolicy {
    /// Creates non-negative freshness bounds.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] if either bound is negative.
    pub fn new(max_age: Duration, max_future_skew: Duration) -> Result<Self, MarketDataError> {
        if max_age < Duration::zero() {
            return Err(MarketDataError::InvalidFreshnessPolicy(
                "maximum market age must not be negative",
            ));
        }
        if max_future_skew < Duration::zero() {
            return Err(MarketDataError::InvalidFreshnessPolicy(
                "maximum future skew must not be negative",
            ));
        }
        Ok(Self {
            maximum_age: max_age,
            future_tolerance: max_future_skew,
            pair_skew_bound: max_future_skew,
        })
    }

    /// Sets the independent cross-venue event-time skew bound.
    ///
    /// The two-argument constructor keeps backward-compatible behavior by
    /// defaulting this bound to `max_future_skew`; callers that compare venues
    /// should set it explicitly.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] when `max_pair_skew` is negative.
    pub fn with_max_pair_skew(mut self, max_pair_skew: Duration) -> Result<Self, MarketDataError> {
        if max_pair_skew < Duration::zero() {
            return Err(MarketDataError::InvalidFreshnessPolicy(
                "maximum market pair skew must not be negative",
            ));
        }
        self.pair_skew_bound = max_pair_skew;
        Ok(self)
    }

    pub const fn max_age(self) -> Duration {
        self.maximum_age
    }

    pub const fn max_future_skew(self) -> Duration {
        self.future_tolerance
    }

    pub const fn max_pair_skew(self) -> Duration {
        self.pair_skew_bound
    }

    fn classify(self, snapshot_at: DateTime<Utc>, now: DateTime<Utc>) -> MarketDataFreshness {
        if snapshot_at > now {
            let skew = snapshot_at.signed_duration_since(now);
            return MarketDataFreshness::Future {
                skew_millis: skew.num_milliseconds(),
                tolerance_millis: self.future_tolerance.num_milliseconds(),
                within_tolerance: skew <= self.future_tolerance,
            };
        }
        let age = now.signed_duration_since(snapshot_at);
        if age > self.maximum_age {
            MarketDataFreshness::Stale {
                age_millis: age.num_milliseconds(),
                limit_millis: self.maximum_age.num_milliseconds(),
            }
        } else {
            MarketDataFreshness::Fresh {
                age_millis: age.num_milliseconds(),
            }
        }
    }
}

/// Supplies logical time for freshness classification and adapter observations.
pub trait MarketDataClock: Debug + Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Production wall-clock implementation. Replay callers should inject a
/// deterministic [`MarketDataClock`] instead.
#[derive(Debug, Default)]
pub struct SystemMarketDataClock;

impl MarketDataClock for SystemMarketDataClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Freshness of the latest accepted snapshot at one coherent read instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MarketDataFreshness {
    Missing,
    Fresh {
        age_millis: i64,
    },
    Stale {
        age_millis: i64,
        limit_millis: i64,
    },
    Future {
        skew_millis: i64,
        tolerance_millis: i64,
        within_tolerance: bool,
    },
    PairSkew {
        skew_millis: i64,
        tolerance_millis: i64,
    },
}

impl MarketDataFreshness {
    pub const fn is_fresh(&self) -> bool {
        matches!(
            self,
            Self::Fresh { .. }
                | Self::Future {
                    within_tolerance: true,
                    ..
                }
        )
    }
}

/// Whether one snapshot timestamp came from the venue event itself or from the
/// runtime's local receive clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketTimestampProvenance {
    Missing,
    VenueEventTime,
    LocalReceipt,
}

/// Bounded, non-secret classification of a market-data source failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataSourceFailure {
    Disconnected,
    TimedOut,
    Backpressure,
    InvalidPayload,
    Rejected,
    Unknown,
}

/// Continuity of the latest accepted per-instrument state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MarketContinuity {
    Missing,
    Continuous,
    Gap {
        expected_revision: u64,
        observed_revision: u64,
    },
    Duplicate {
        revision: u64,
    },
    OutOfOrder {
        last_revision: u64,
        observed_revision: u64,
    },
    DuplicateTimestamp {
        timestamp: DateTime<Utc>,
    },
    OutOfOrderTimestamp {
        last_timestamp: DateTime<Utc>,
        observed_timestamp: DateTime<Utc>,
    },
    OutOfOrderReceipt {
        last_received_at: DateTime<Utc>,
        observed_received_at: DateTime<Utc>,
    },
    SourceGap {
        skipped: u64,
    },
    Unavailable {
        failure: MarketDataSourceFailure,
    },
}

/// One source observation with explicit receive order and time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketDataObservation {
    pub snapshot: MarketSnapshot,
    pub revision: u64,
    pub received_at: DateTime<Utc>,
    pub timestamp_provenance: MarketTimestampProvenance,
    pub source_sequence: Option<u64>,
}

impl MarketDataObservation {
    /// Creates one ordered source observation.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InvalidRevision`] for revision zero.
    pub fn new(
        snapshot: MarketSnapshot,
        revision: u64,
        received_at: DateTime<Utc>,
    ) -> Result<Self, MarketDataError> {
        Self::with_source_metadata(
            snapshot,
            revision,
            received_at,
            MarketTimestampProvenance::VenueEventTime,
            None,
        )
    }

    /// Creates one ordered source observation with explicit timestamp
    /// provenance and optional source sequence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InvalidRevision`] for revision zero and
    /// [`MarketDataError::InvalidTimestampProvenance`] when callers omit the
    /// accepted timestamp provenance.
    pub fn with_source_metadata(
        snapshot: MarketSnapshot,
        revision: u64,
        received_at: DateTime<Utc>,
        timestamp_provenance: MarketTimestampProvenance,
        source_sequence: Option<u64>,
    ) -> Result<Self, MarketDataError> {
        if revision == 0 {
            return Err(MarketDataError::InvalidRevision);
        }
        if timestamp_provenance == MarketTimestampProvenance::Missing {
            return Err(MarketDataError::InvalidTimestampProvenance);
        }
        Ok(Self {
            snapshot,
            revision,
            received_at,
            timestamp_provenance,
            source_sequence,
        })
    }
}

/// Adapter-neutral input to the bounded market-data book.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketDataEvent {
    Observation(MarketDataObservation),
    SourceGap {
        exchange: String,
        skipped: u64,
        observed_at: DateTime<Utc>,
    },
    SourceUnavailable {
        exchange: String,
        failure: MarketDataSourceFailure,
        observed_at: DateTime<Utc>,
    },
}

impl MarketDataEvent {
    /// Returns the stable source identity carried by this event.
    pub fn exchange(&self) -> &str {
        match self {
            Self::Observation(observation) => observation.snapshot.exchange(),
            Self::SourceGap { exchange, .. } | Self::SourceUnavailable { exchange, .. } => exchange,
        }
    }

    /// Returns when this event was observed by the runtime.
    pub fn observed_at(&self) -> DateTime<Utc> {
        match self {
            Self::Observation(observation) => observation.received_at,
            Self::SourceGap { observed_at, .. } | Self::SourceUnavailable { observed_at, .. } => {
                *observed_at
            }
        }
    }

    /// Creates an explicit source-wide continuity gap.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for an invalid exchange or zero skipped count.
    pub fn source_gap(
        exchange: impl Into<String>,
        skipped: u64,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, MarketDataError> {
        if skipped == 0 {
            return Err(MarketDataError::InvalidGapCount);
        }
        Ok(Self::SourceGap {
            exchange: validated_exchange(exchange)?,
            skipped,
            observed_at,
        })
    }

    /// Creates an explicit source-wide unavailability event.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for an invalid exchange identifier.
    pub fn source_unavailable(
        exchange: impl Into<String>,
        failure: MarketDataSourceFailure,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, MarketDataError> {
        Ok(Self::SourceUnavailable {
            exchange: validated_exchange(exchange)?,
            failure,
            observed_at,
        })
    }
}

/// Result of applying one source event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataUpdate {
    Accepted,
    AcceptedWithGap,
    IgnoredDuplicate,
    IgnoredOutOfOrder,
    IgnoredDuplicateTimestamp,
    IgnoredOutOfOrderTimestamp,
    IgnoredOutOfOrderReceipt,
    SourceDegraded,
}

#[derive(Debug, Clone)]
struct MarketDataEntry {
    instrument: MarketInstrument,
    last: Option<MarketDataObservation>,
    continuity: MarketContinuity,
    status_changed_at: Option<DateTime<Utc>>,
}

impl MarketDataEntry {
    fn new(instrument: MarketInstrument) -> Self {
        Self {
            instrument,
            last: None,
            continuity: MarketContinuity::Missing,
            status_changed_at: None,
        }
    }
}

/// Mutable, bounded latest-value book. It owns no networking or trading
/// authority; trusted adapters translate their input into [`MarketDataEvent`].
#[derive(Debug)]
pub struct MarketDataBook {
    universe: MarketUniverse,
    policy: MarketFreshnessPolicy,
    clock: Arc<dyn MarketDataClock>,
    generation: u64,
    entries: Vec<MarketDataEntry>,
}

impl MarketDataBook {
    pub fn new<C>(universe: MarketUniverse, policy: MarketFreshnessPolicy, clock: Arc<C>) -> Self
    where
        C: MarketDataClock + 'static,
    {
        let entries = universe
            .instruments()
            .iter()
            .cloned()
            .map(MarketDataEntry::new)
            .collect();
        Self {
            universe,
            policy,
            clock,
            generation: 0,
            entries,
        }
    }

    pub const fn universe(&self) -> &MarketUniverse {
        &self.universe
    }

    /// Applies one adapter-neutral event atomically.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for events outside the bound universe,
    /// invalid revisions, revision exhaustion, or source-time rollback.
    pub fn apply(&mut self, event: MarketDataEvent) -> Result<MarketDataUpdate, MarketDataError> {
        match event {
            MarketDataEvent::Observation(observation) => self.apply_observation(observation),
            MarketDataEvent::SourceGap {
                exchange,
                skipped,
                observed_at,
            } => {
                if skipped == 0 {
                    return Err(MarketDataError::InvalidGapCount);
                }
                let exchange = validated_exchange(exchange)?;
                let continuity = MarketContinuity::SourceGap { skipped };
                self.apply_source_state(&exchange, &continuity, observed_at)
            }
            MarketDataEvent::SourceUnavailable {
                exchange,
                failure,
                observed_at,
            } => {
                let exchange = validated_exchange(exchange)?;
                let continuity = MarketContinuity::Unavailable { failure };
                self.apply_source_state(&exchange, &continuity, observed_at)
            }
        }
    }

    /// Produces one coherent, deterministic view at the injected clock time.
    pub fn view(&self) -> MarketDataView {
        let now = self.clock.now();
        let instruments = self
            .entries
            .iter()
            .map(|entry| {
                let freshness = entry
                    .last
                    .as_ref()
                    .map_or(MarketDataFreshness::Missing, |last| {
                        self.policy.classify(last.snapshot.timestamp, now)
                    });
                MarketInstrumentView {
                    instrument: entry.instrument.clone(),
                    snapshot: entry.last.as_ref().map(|last| last.snapshot.clone()),
                    revision: entry.last.as_ref().map(|last| last.revision),
                    received_at: entry.last.as_ref().map(|last| last.received_at),
                    timestamp_provenance: entry
                        .last
                        .as_ref()
                        .map_or(MarketTimestampProvenance::Missing, |last| {
                            last.timestamp_provenance
                        }),
                    source_latency_millis: entry.last.as_ref().and_then(|last| {
                        (last.timestamp_provenance == MarketTimestampProvenance::VenueEventTime)
                            .then(|| {
                                last.received_at
                                    .signed_duration_since(last.snapshot.timestamp)
                                    .num_milliseconds()
                            })
                    }),
                    status_changed_at: entry.status_changed_at,
                    freshness,
                    continuity: entry.continuity.clone(),
                }
            })
            .collect();
        MarketDataView {
            schema_version: MARKET_DATA_VIEW_SCHEMA_VERSION,
            observed_at: now,
            generation: self.generation,
            pair_skew_tolerance_millis: self.policy.max_pair_skew().num_milliseconds(),
            instruments,
        }
    }

    /// Reads a fresh, continuous pair from one coherent generation.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InstrumentOutsideUniverse`] for unknown legs
    /// and [`MarketDataError::InstrumentNotReady`] for missing, stale, future,
    /// or continuity-degraded legs.
    pub fn current_pair(
        &self,
        left: &MarketInstrument,
        right: &MarketInstrument,
    ) -> Result<ObservedMarketPair, MarketDataError> {
        self.view().current_pair(left, right)
    }
}

impl MarketDataView {
    /// Reads a fresh, continuous pair from this already-captured generation.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InstrumentOutsideUniverse`] for unknown legs
    /// and [`MarketDataError::InstrumentNotReady`] for degraded rows.
    pub fn current_pair(
        &self,
        left: &MarketInstrument,
        right: &MarketInstrument,
    ) -> Result<ObservedMarketPair, MarketDataError> {
        let left_snapshot = ready_snapshot(self, left)?;
        let right_snapshot = ready_snapshot(self, right)?;
        let skew_millis = left_snapshot
            .timestamp
            .signed_duration_since(right_snapshot.timestamp)
            .num_milliseconds()
            .abs();
        if skew_millis > self.pair_skew_tolerance_millis {
            return Err(MarketDataError::PairSkew {
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
                skew_millis,
                tolerance_millis: self.pair_skew_tolerance_millis,
            });
        }
        Ok(ObservedMarketPair {
            left: left_snapshot,
            right: right_snapshot,
            observed_at: self.observed_at,
            generation: self.generation,
            pair_skew_millis: skew_millis,
        })
    }
}

impl MarketDataBook {
    fn apply_observation(
        &mut self,
        observation: MarketDataObservation,
    ) -> Result<MarketDataUpdate, MarketDataError> {
        if observation.revision == 0 {
            return Err(MarketDataError::InvalidRevision);
        }
        let instrument = MarketInstrument::from_snapshot(&observation.snapshot)?;
        let index = self
            .entries
            .binary_search_by(|entry| entry.instrument.cmp(&instrument))
            .map_err(|_| MarketDataError::InstrumentOutsideUniverse {
                instrument: instrument.clone(),
            })?;
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or(MarketDataError::GenerationExhausted)?;

        let entry = &mut self.entries[index];
        if let Some(last_received_at) = entry.status_changed_at
            && observation.received_at < last_received_at
        {
            entry.continuity = MarketContinuity::OutOfOrderReceipt {
                last_received_at,
                observed_received_at: observation.received_at,
            };
            self.generation = next_generation;
            return Ok(MarketDataUpdate::IgnoredOutOfOrderReceipt);
        }
        let mut accepted_continuity = MarketContinuity::Continuous;
        if let Some(last) = &entry.last {
            if observation.revision == last.revision {
                entry.continuity = MarketContinuity::Duplicate {
                    revision: observation.revision,
                };
                entry.status_changed_at = Some(observation.received_at);
                self.generation = next_generation;
                return Ok(MarketDataUpdate::IgnoredDuplicate);
            }
            if observation.revision < last.revision {
                entry.continuity = MarketContinuity::OutOfOrder {
                    last_revision: last.revision,
                    observed_revision: observation.revision,
                };
                entry.status_changed_at = Some(observation.received_at);
                self.generation = next_generation;
                return Ok(MarketDataUpdate::IgnoredOutOfOrder);
            }
            let authoritative_timestamps = observation.timestamp_provenance
                == MarketTimestampProvenance::VenueEventTime
                && last.timestamp_provenance == MarketTimestampProvenance::VenueEventTime;
            if authoritative_timestamps
                && observation.snapshot.timestamp == last.snapshot.timestamp
                && observation.source_sequence.is_none()
            {
                entry.continuity = MarketContinuity::DuplicateTimestamp {
                    timestamp: observation.snapshot.timestamp,
                };
                entry.status_changed_at = Some(observation.received_at);
                self.generation = next_generation;
                return Ok(MarketDataUpdate::IgnoredDuplicateTimestamp);
            }
            if authoritative_timestamps && observation.snapshot.timestamp < last.snapshot.timestamp
            {
                entry.continuity = MarketContinuity::OutOfOrderTimestamp {
                    last_timestamp: last.snapshot.timestamp,
                    observed_timestamp: observation.snapshot.timestamp,
                };
                entry.status_changed_at = Some(observation.received_at);
                self.generation = next_generation;
                return Ok(MarketDataUpdate::IgnoredOutOfOrderTimestamp);
            }
            let expected_revision = last
                .revision
                .checked_add(1)
                .ok_or(MarketDataError::RevisionExhausted)?;
            if observation.revision > expected_revision {
                accepted_continuity = MarketContinuity::Gap {
                    expected_revision,
                    observed_revision: observation.revision,
                };
            }
        }

        let update = if matches!(accepted_continuity, MarketContinuity::Gap { .. }) {
            MarketDataUpdate::AcceptedWithGap
        } else {
            MarketDataUpdate::Accepted
        };
        entry.status_changed_at = Some(observation.received_at);
        entry.last = Some(observation);
        entry.continuity = accepted_continuity;
        self.generation = next_generation;
        Ok(update)
    }

    fn apply_source_state(
        &mut self,
        exchange: &str,
        continuity: &MarketContinuity,
        observed_at: DateTime<Utc>,
    ) -> Result<MarketDataUpdate, MarketDataError> {
        if !self.universe.contains_exchange(exchange) {
            return Err(MarketDataError::UnknownSource {
                exchange: exchange.to_owned(),
            });
        }
        if self.entries.iter().any(|entry| {
            entry.instrument.exchange() == exchange
                && entry
                    .status_changed_at
                    .is_some_and(|last| observed_at < last)
        }) {
            return Err(MarketDataError::SourceEventTimeRollback {
                exchange: exchange.to_owned(),
            });
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or(MarketDataError::GenerationExhausted)?;
        for entry in self
            .entries
            .iter_mut()
            .filter(|entry| entry.instrument.exchange() == exchange)
        {
            entry.continuity.clone_from(continuity);
            entry.status_changed_at = Some(observed_at);
        }
        self.generation = next_generation;
        Ok(MarketDataUpdate::SourceDegraded)
    }
}

/// One deterministic row in a [`MarketDataView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketInstrumentView {
    pub instrument: MarketInstrument,
    pub snapshot: Option<MarketSnapshot>,
    pub revision: Option<u64>,
    pub received_at: Option<DateTime<Utc>>,
    pub timestamp_provenance: MarketTimestampProvenance,
    pub source_latency_millis: Option<i64>,
    pub status_changed_at: Option<DateTime<Utc>>,
    pub freshness: MarketDataFreshness,
    pub continuity: MarketContinuity,
}

impl MarketInstrumentView {
    /// A strategy-ready row must be both fresh and continuity-safe.
    pub fn is_ready(&self) -> bool {
        self.snapshot.is_some()
            && self.freshness.is_fresh()
            && self.continuity == MarketContinuity::Continuous
    }
}

/// Coherent, stable-order projection of one bound market universe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketDataView {
    pub schema_version: u16,
    pub observed_at: DateTime<Utc>,
    pub generation: u64,
    pub pair_skew_tolerance_millis: i64,
    instruments: Vec<MarketInstrumentView>,
}

impl MarketDataView {
    pub fn instruments(&self) -> &[MarketInstrumentView] {
        &self.instruments
    }

    pub fn instrument(&self, instrument: &MarketInstrument) -> Option<&MarketInstrumentView> {
        self.instruments
            .binary_search_by(|row| row.instrument.cmp(instrument))
            .ok()
            .map(|index| &self.instruments[index])
    }
}

/// Fresh, continuous pair sampled from one book generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedMarketPair {
    pub left: MarketSnapshot,
    pub right: MarketSnapshot,
    pub observed_at: DateTime<Utc>,
    pub generation: u64,
    pub pair_skew_millis: i64,
}

fn ready_snapshot(
    view: &MarketDataView,
    instrument: &MarketInstrument,
) -> Result<MarketSnapshot, MarketDataError> {
    let row =
        view.instrument(instrument)
            .ok_or_else(|| MarketDataError::InstrumentOutsideUniverse {
                instrument: instrument.clone(),
            })?;
    if !row.is_ready() {
        return Err(MarketDataError::InstrumentNotReady {
            instrument: instrument.clone(),
            freshness: row.freshness.clone(),
            continuity: row.continuity.clone(),
        });
    }
    row.snapshot
        .clone()
        .ok_or_else(|| MarketDataError::InstrumentNotReady {
            instrument: instrument.clone(),
            freshness: row.freshness.clone(),
            continuity: row.continuity.clone(),
        })
}

/// Finite in-process adapter used by deterministic replay and contract tests.
#[derive(Debug)]
pub struct DeterministicMarketDataAdapter {
    events: VecDeque<MarketDataEvent>,
}

impl DeterministicMarketDataAdapter {
    /// Creates a bounded finite source.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::EventBufferTooLarge`] above the hard limit.
    pub fn new(events: Vec<MarketDataEvent>) -> Result<Self, MarketDataError> {
        if events.len() > MAX_MARKET_DATA_EVENTS {
            return Err(MarketDataError::EventBufferTooLarge {
                count: events.len(),
                limit: MAX_MARKET_DATA_EVENTS,
            });
        }
        Ok(Self {
            events: events.into(),
        })
    }

    /// Returns `None` explicitly after the finite replay is exhausted.
    pub fn next_event(&mut self) -> Option<MarketDataEvent> {
        self.events.pop_front()
    }
}

/// Read-only bridge from an existing bounded exchange subscription into the
/// adapter-neutral market-data event contract.
#[derive(Debug)]
pub struct SubscriptionMarketDataAdapter {
    exchange: String,
    receipt: SubscriptionReceipt,
    universe: MarketUniverse,
    clock: Arc<dyn MarketDataClock>,
    revisions: HashMap<MarketInstrument, u64>,
}

impl SubscriptionMarketDataAdapter {
    /// Binds a subscription to one exact source identity and bounded universe.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for an invalid or unconfigured source.
    pub fn new<C>(
        exchange: impl Into<String>,
        receipt: SubscriptionReceipt,
        universe: MarketUniverse,
        clock: Arc<C>,
    ) -> Result<Self, MarketDataError>
    where
        C: MarketDataClock + 'static,
    {
        let exchange = validated_exchange(exchange)?;
        if !universe.contains_exchange(&exchange) {
            return Err(MarketDataError::UnknownSource { exchange });
        }
        Ok(Self {
            exchange,
            receipt,
            universe,
            clock,
            revisions: HashMap::new(),
        })
    }

    /// Returns the exact source identity bound during construction.
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Waits for and translates the next bounded subscription outcome.
    ///
    /// Lag and disconnect errors become explicit events instead of being
    /// swallowed or converted into fabricated snapshots.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for source identity drift, observations
    /// outside the bound universe, or revision exhaustion.
    pub async fn next_event(&mut self) -> Result<MarketDataEvent, MarketDataError> {
        match self.receipt.recv().await {
            Ok(snapshot) => {
                let instrument = MarketInstrument::from_snapshot(&snapshot)?;
                if instrument.exchange() != self.exchange {
                    return Err(MarketDataError::SourceIdentityMismatch {
                        expected: self.exchange.clone(),
                        actual: instrument.exchange().to_owned(),
                    });
                }
                if !self.universe.contains(&instrument) {
                    return Err(MarketDataError::InstrumentOutsideUniverse { instrument });
                }
                let revision = self.revisions.entry(instrument).or_insert(0);
                *revision = revision
                    .checked_add(1)
                    .ok_or(MarketDataError::RevisionExhausted)?;
                Ok(MarketDataEvent::Observation(MarketDataObservation::new(
                    snapshot,
                    *revision,
                    self.clock.now(),
                )?))
            }
            Err(ExchangeError::SubscriptionLagged { skipped }) => Ok(MarketDataEvent::SourceGap {
                exchange: self.exchange.clone(),
                skipped,
                observed_at: self.clock.now(),
            }),
            Err(error) => Ok(MarketDataEvent::SourceUnavailable {
                exchange: self.exchange.clone(),
                failure: classify_exchange_failure(&error),
                observed_at: self.clock.now(),
            }),
        }
    }
}

pub(crate) fn classify_exchange_failure(error: &ExchangeError) -> MarketDataSourceFailure {
    match error {
        ExchangeError::Unavailable { .. } | ExchangeError::RemoteFailure { .. } => {
            MarketDataSourceFailure::Disconnected
        }
        ExchangeError::Backpressure { .. } => MarketDataSourceFailure::Backpressure,
        ExchangeError::InvalidResponse { .. } => MarketDataSourceFailure::InvalidPayload,
        ExchangeError::Rejected { .. } | ExchangeError::InvalidRequest { .. } => {
            MarketDataSourceFailure::Rejected
        }
        _ => MarketDataSourceFailure::Unknown,
    }
}

pub(crate) fn validated_exchange(exchange: impl Into<String>) -> Result<String, MarketDataError> {
    let exchange = exchange.into();
    let trimmed = exchange.trim();
    if trimmed.is_empty() {
        return Err(MarketDataError::EmptyExchange);
    }
    if trimmed.len() > MAX_EXCHANGE_NAME_BYTES {
        return Err(MarketDataError::ExchangeNameTooLong {
            bytes: trimmed.len(),
            limit: MAX_EXCHANGE_NAME_BYTES,
        });
    }
    Ok(trimmed.to_owned())
}

const fn market_type_rank(market_type: MarketType) -> u8 {
    match market_type {
        MarketType::Spot => 0,
        MarketType::Perpetual => 1,
    }
}

/// Contract and operational failures at the read-only market-data seam.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MarketDataError {
    #[error("market-data exchange name must not be empty")]
    EmptyExchange,
    #[error("market-data exchange name is {bytes} bytes; maximum is {limit}")]
    ExchangeNameTooLong { bytes: usize, limit: usize },
    #[error("market-data symbol is {bytes} bytes; maximum is {limit}")]
    SymbolTooLong { bytes: usize, limit: usize },
    #[error("market-data universe must contain at least one exact instrument")]
    EmptyUniverse,
    #[error("market-data universe has {count} instruments; maximum is {limit}")]
    UniverseTooLarge { count: usize, limit: usize },
    #[error("invalid market-data freshness policy: {0}")]
    InvalidFreshnessPolicy(&'static str),
    #[error("market-data revision must be greater than zero")]
    InvalidRevision,
    #[error("market-data source gap must skip at least one observation")]
    InvalidGapCount,
    #[error("market-data timestamp provenance must be explicit for accepted observations")]
    InvalidTimestampProvenance,
    #[error("market-data revision counter is exhausted")]
    RevisionExhausted,
    #[error("market-data view generation counter is exhausted")]
    GenerationExhausted,
    #[error("market-data event buffer has {count} entries; maximum is {limit}")]
    EventBufferTooLarge { count: usize, limit: usize },
    #[error("invalid market-data polling policy: {0}")]
    InvalidPollingPolicy(&'static str),
    #[error("a bounded market-data polling task terminated unexpectedly")]
    PollingTaskFailed,
    #[error("polling is unsupported for instrument {instrument:?}")]
    UnsupportedPollingInstrument { instrument: MarketInstrument },
    #[error("polling route for instrument {instrument:?} is duplicated")]
    DuplicatePollingInstrument { instrument: MarketInstrument },
    #[error("polling wire symbol {symbol} is mapped more than once")]
    DuplicatePollingWireSymbol { symbol: Symbol },
    #[error("instrument {instrument:?} is outside the bound market-data universe")]
    InstrumentOutsideUniverse { instrument: MarketInstrument },
    #[error("market-data source {exchange} has no instruments in the bound universe")]
    UnknownSource { exchange: String },
    #[error("market-data source identity changed from {expected} to {actual}")]
    SourceIdentityMismatch { expected: String, actual: String },
    #[error("market-data source event time moved backwards for {exchange}")]
    SourceEventTimeRollback { exchange: String },
    #[error(
        "observed pair skew between {left:?} and {right:?} is {skew_millis}ms, exceeding the {tolerance_millis}ms tolerance"
    )]
    PairSkew {
        left: Box<MarketInstrument>,
        right: Box<MarketInstrument>,
        skew_millis: i64,
        tolerance_millis: i64,
    },
    #[error(
        "instrument {instrument:?} is not strategy-ready: freshness={freshness:?}, continuity={continuity:?}"
    )]
    InstrumentNotReady {
        instrument: MarketInstrument,
        freshness: MarketDataFreshness,
        continuity: MarketContinuity,
    },
}
