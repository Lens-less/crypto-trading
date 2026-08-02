use std::{
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_exchange::{ExchangeHandle, MarketSubscription, PaperExchange};
use crypto_trading_runtime::{
    DeterministicMarketDataAdapter, MARKET_DATA_VIEW_SCHEMA_VERSION, MAX_MARKET_DATA_EVENTS,
    MAX_MARKET_DATA_TARGETS, MarketContinuity, MarketDataBook, MarketDataClock, MarketDataError,
    MarketDataEvent, MarketDataFreshness, MarketDataObservation, MarketDataSourceFailure,
    MarketDataUpdate, MarketFreshnessPolicy, MarketInstrument, MarketTimestampProvenance,
    MarketUniverse, SubscriptionMarketDataAdapter,
};
use rust_decimal::Decimal;

#[derive(Clone)]
struct TestClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl TestClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    fn set(&self, now: DateTime<Utc>) {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
    }
}

impl fmt::Debug for TestClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TestClock").finish_non_exhaustive()
    }
}

impl MarketDataClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[test]
fn universe_is_exact_sorted_deduplicated_and_bounded() {
    let btc = instrument("paper-b", "BTC-USDT");
    let eth = instrument("paper-a", "ETH-USDT");
    let universe = MarketUniverse::new(vec![btc.clone(), eth.clone(), btc]).unwrap();

    assert_eq!(
        universe.instruments(),
        &[eth, instrument("paper-b", "BTC-USDT")]
    );
    assert!(matches!(
        MarketUniverse::new(Vec::new()).unwrap_err(),
        MarketDataError::EmptyUniverse
    ));

    let over_limit = (0..=MAX_MARKET_DATA_TARGETS)
        .map(|index| instrument("paper", &format!("ASSET-{index}")))
        .collect();
    assert!(matches!(
        MarketUniverse::new(over_limit).unwrap_err(),
        MarketDataError::UniverseTooLarge {
            count,
            limit: MAX_MARKET_DATA_TARGETS
        } if count == MAX_MARKET_DATA_TARGETS + 1
    ));

    let oversized_symbol = Symbol::new("S".repeat(129)).unwrap();
    assert!(matches!(
        MarketInstrument::new("paper", oversized_symbol, MarketType::Perpetual).unwrap_err(),
        MarketDataError::SymbolTooLong {
            bytes: 129,
            limit: 128
        }
    ));
}

#[test]
fn empty_book_reports_every_configured_instrument_as_missing() {
    let now = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let book = book(vec![key.clone()], now, Duration::seconds(10));

    let view = book.view();

    assert_eq!(view.schema_version, MARKET_DATA_VIEW_SCHEMA_VERSION);
    assert_eq!(view.observed_at, now);
    assert_eq!(view.generation, 0);
    assert_eq!(view.instruments().len(), 1);
    let row = view.instrument(&key).unwrap();
    assert_eq!(row.freshness, MarketDataFreshness::Missing);
    assert_eq!(row.continuity, MarketContinuity::Missing);
    assert!(row.snapshot.is_none());
}

#[test]
fn freshness_is_recomputed_from_the_injected_clock() {
    let base = timestamp(0);
    let clock = Arc::new(TestClock::new(base + Duration::seconds(5)));
    let key = instrument("paper", "BTC-USDT");
    let mut book = MarketDataBook::new(
        MarketUniverse::new(vec![key.clone()]).unwrap(),
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        Arc::clone(&clock),
    );
    book.apply(observation_event(
        snapshot("paper", "BTC-USDT", base),
        1,
        base,
    ))
    .unwrap();

    assert!(matches!(
        book.view().instrument(&key).unwrap().freshness,
        MarketDataFreshness::Fresh { age_millis: 5_000 }
    ));

    clock.set(base + Duration::seconds(11));
    assert!(matches!(
        book.view().instrument(&key).unwrap().freshness,
        MarketDataFreshness::Stale {
            age_millis: 11_000,
            limit_millis: 10_000
        }
    ));

    clock.set(base - Duration::seconds(2));
    assert!(matches!(
        book.view().instrument(&key).unwrap().freshness,
        MarketDataFreshness::Future {
            skew_millis: 2_000,
            tolerance_millis: 1_000,
            within_tolerance: false
        }
    ));
}

#[test]
fn future_within_tolerance_is_strategy_ready() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(vec![key.clone()], base, Duration::seconds(10));

    book.apply(observation_event(
        snapshot("paper", "BTC-USDT", base + Duration::milliseconds(750)),
        1,
        base,
    ))
    .unwrap();

    let row = book.view().instrument(&key).unwrap().clone();
    assert!(matches!(
        row.freshness,
        MarketDataFreshness::Future {
            skew_millis: 750,
            tolerance_millis: 1_000,
            within_tolerance: true
        }
    ));
    assert!(row.is_ready());
}

#[test]
fn observations_outside_the_bound_universe_are_rejected_without_growth() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(vec![key.clone()], base, Duration::seconds(10));

    let error = book
        .apply(observation_event(
            snapshot("paper", "ETH-USDT", base),
            1,
            base,
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        MarketDataError::InstrumentOutsideUniverse { .. }
    ));
    assert_eq!(book.view().instruments().len(), 1);
    assert!(book.view().instrument(&key).unwrap().snapshot.is_none());
}

#[test]
fn duplicate_and_out_of_order_revisions_never_replace_the_last_good_snapshot() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    let first = snapshot("paper", "BTC-USDT", base);
    book.apply(observation_event(first.clone(), 3, base))
        .unwrap();

    let duplicate = book
        .apply(observation_event(
            snapshot("paper", "BTC-USDT", base + Duration::seconds(1)),
            3,
            base + Duration::seconds(1),
        ))
        .unwrap();
    assert_eq!(duplicate, MarketDataUpdate::IgnoredDuplicate);
    let duplicate_row = book.view().instrument(&key).unwrap().clone();
    assert_eq!(duplicate_row.snapshot, Some(first.clone()));
    assert_eq!(
        duplicate_row.continuity,
        MarketContinuity::Duplicate { revision: 3 }
    );

    let out_of_order = book
        .apply(observation_event(
            snapshot("paper", "BTC-USDT", base + Duration::seconds(2)),
            2,
            base + Duration::seconds(2),
        ))
        .unwrap();
    assert_eq!(out_of_order, MarketDataUpdate::IgnoredOutOfOrder);
    let row = book.view().instrument(&key).unwrap().clone();
    assert_eq!(row.snapshot, Some(first));
    assert_eq!(
        row.continuity,
        MarketContinuity::OutOfOrder {
            last_revision: 3,
            observed_revision: 2
        }
    );
}

#[test]
fn revision_gap_is_explicit_while_the_newest_snapshot_remains_available() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    book.apply(observation_event(
        snapshot("paper", "BTC-USDT", base),
        1,
        base,
    ))
    .unwrap();
    let newest = snapshot("paper", "BTC-USDT", base + Duration::seconds(2));

    let update = book
        .apply(observation_event(
            newest.clone(),
            4,
            base + Duration::seconds(2),
        ))
        .unwrap();

    assert_eq!(update, MarketDataUpdate::AcceptedWithGap);
    let row = book.view().instrument(&key).unwrap().clone();
    assert_eq!(row.snapshot, Some(newest));
    assert_eq!(
        row.continuity,
        MarketContinuity::Gap {
            expected_revision: 2,
            observed_revision: 4
        }
    );
}

#[test]
fn duplicate_and_rollback_timestamps_are_ignored_even_when_revisions_advance() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    let first = snapshot("paper", "BTC-USDT", base);
    book.apply(observation_event(first.clone(), 1, base))
        .unwrap();

    assert_eq!(
        book.apply(observation_event(
            first.clone(),
            2,
            base + Duration::seconds(1)
        ))
        .unwrap(),
        MarketDataUpdate::IgnoredDuplicateTimestamp
    );
    assert!(matches!(
        book.view().instrument(&key).unwrap().continuity,
        MarketContinuity::DuplicateTimestamp { .. }
    ));

    assert_eq!(
        book.apply(observation_event(
            snapshot("paper", "BTC-USDT", base - Duration::seconds(1)),
            3,
            base + Duration::seconds(2),
        ))
        .unwrap(),
        MarketDataUpdate::IgnoredOutOfOrderTimestamp
    );
    let view = book.view();
    let row = view.instrument(&key).unwrap();
    assert_eq!(row.snapshot, Some(first));
    assert!(matches!(
        row.continuity,
        MarketContinuity::OutOfOrderTimestamp { .. }
    ));
}

#[test]
fn equal_event_timestamps_with_larger_source_sequences_remain_usable() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    let first = snapshot("paper", "BTC-USDT", base);
    book.apply(observation_event_with_metadata(
        first.clone(),
        17,
        base + Duration::milliseconds(100),
        MarketTimestampProvenance::VenueEventTime,
        Some(17),
    ))
    .unwrap();

    let next = snapshot("paper", "BTC-USDT", base);
    let update = book
        .apply(observation_event_with_metadata(
            next.clone(),
            18,
            base + Duration::milliseconds(200),
            MarketTimestampProvenance::VenueEventTime,
            Some(18),
        ))
        .unwrap();

    assert_eq!(update, MarketDataUpdate::Accepted);
    let view = book.view();
    let row = view.instrument(&key).unwrap();
    assert_eq!(row.snapshot, Some(next));
    assert_eq!(row.revision, Some(18));
    assert_eq!(row.continuity, MarketContinuity::Continuous);
}

#[test]
fn local_receipt_timestamps_do_not_trigger_duplicate_timestamp_continuity() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    let first = snapshot("paper", "BTC-USDT", base);
    book.apply(observation_event_with_metadata(
        first.clone(),
        1,
        base,
        MarketTimestampProvenance::LocalReceipt,
        None,
    ))
    .unwrap();

    let next = snapshot("paper", "BTC-USDT", base);
    let update = book
        .apply(observation_event_with_metadata(
            next.clone(),
            2,
            base + Duration::seconds(1),
            MarketTimestampProvenance::LocalReceipt,
            None,
        ))
        .unwrap();

    assert_eq!(update, MarketDataUpdate::Accepted);
    let view = book.view();
    let row = view.instrument(&key).unwrap();
    assert_eq!(row.snapshot, Some(next));
    assert_eq!(row.revision, Some(2));
    assert_eq!(row.continuity, MarketContinuity::Continuous);
}

#[test]
fn source_disconnect_is_visible_and_does_not_fabricate_or_clear_quotes() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(1),
        Duration::seconds(10),
    );
    let accepted = snapshot("paper", "BTC-USDT", base);
    book.apply(observation_event(accepted.clone(), 1, base))
        .unwrap();

    book.apply(
        MarketDataEvent::source_unavailable(
            "paper",
            MarketDataSourceFailure::Disconnected,
            base + Duration::seconds(1),
        )
        .unwrap(),
    )
    .unwrap();

    let view = book.view();
    let row = view.instrument(&key).unwrap();
    assert_eq!(row.snapshot, Some(accepted));
    assert_eq!(
        row.continuity,
        MarketContinuity::Unavailable {
            failure: MarketDataSourceFailure::Disconnected
        }
    );
    assert!(!row.is_ready());
}

#[test]
fn source_variants_are_revalidated_and_delayed_recovery_cannot_clear_a_gap() {
    let base = timestamp(0);
    let key = instrument("paper", "BTC-USDT");
    let mut book = book(
        vec![key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    book.apply(observation_event(
        snapshot("paper", "BTC-USDT", base),
        1,
        base,
    ))
    .unwrap();
    book.apply(MarketDataEvent::source_gap("paper", 2, base + Duration::seconds(2)).unwrap())
        .unwrap();

    let delayed = book
        .apply(observation_event(
            snapshot("paper", "BTC-USDT", base + Duration::seconds(1)),
            2,
            base + Duration::seconds(1),
        ))
        .unwrap();
    assert_eq!(delayed, MarketDataUpdate::IgnoredOutOfOrderReceipt);
    assert!(matches!(
        book.view().instrument(&key).unwrap().continuity,
        MarketContinuity::OutOfOrderReceipt { .. }
    ));

    assert!(matches!(
        book.apply(MarketDataEvent::SourceGap {
            exchange: "paper".to_owned(),
            skipped: 0,
            observed_at: base + Duration::seconds(3),
        })
        .unwrap_err(),
        MarketDataError::InvalidGapCount
    ));
}

#[test]
fn pair_read_is_one_generation_and_fails_closed_on_degraded_legs() {
    let base = timestamp(0);
    let left = instrument("left", "BTC-USDT");
    let right = instrument("right", "BTC-USDT");
    let mut book = book(
        vec![left.clone(), right.clone()],
        base + Duration::seconds(1),
        Duration::seconds(10),
    );
    book.apply(observation_event(
        snapshot("left", "BTC-USDT", base),
        1,
        base,
    ))
    .unwrap();

    assert!(matches!(
        book.current_pair(&left, &right).unwrap_err(),
        MarketDataError::InstrumentNotReady { instrument, .. } if instrument == right
    ));

    book.apply(observation_event(
        snapshot("right", "BTC-USDT", base),
        1,
        base,
    ))
    .unwrap();
    let pair = book.current_pair(&left, &right).unwrap();
    assert_eq!(pair.generation, 2);
    assert_eq!(pair.observed_at, base + Duration::seconds(1));
    assert_eq!(pair.left.exchange(), "left");
    assert_eq!(pair.right.exchange(), "right");

    book.apply(MarketDataEvent::source_gap("right", 4, base + Duration::seconds(1)).unwrap())
        .unwrap();
    assert!(matches!(
        book.current_pair(&left, &right).unwrap_err(),
        MarketDataError::InstrumentNotReady { instrument, .. } if instrument == right
    ));
}

#[test]
fn pair_skew_is_reported_and_rejected_beyond_tolerance() {
    let base = timestamp(0);
    let left = instrument("left", "BTC-USDT");
    let right = instrument("right", "BTC-USDT");
    let policy = MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1))
        .unwrap()
        .with_max_pair_skew(Duration::milliseconds(500))
        .unwrap();
    let mut book = MarketDataBook::new(
        MarketUniverse::new(vec![left.clone(), right.clone()]).unwrap(),
        policy,
        Arc::new(TestClock::new(base + Duration::seconds(3))),
    );
    book.apply(observation_event_with_metadata(
        snapshot("left", "BTC-USDT", base),
        1,
        base + Duration::seconds(1),
        MarketTimestampProvenance::VenueEventTime,
        Some(1),
    ))
    .unwrap();
    book.apply(observation_event_with_metadata(
        snapshot("right", "BTC-USDT", base + Duration::seconds(2)),
        1,
        base + Duration::seconds(2),
        MarketTimestampProvenance::VenueEventTime,
        Some(1),
    ))
    .unwrap();

    let error = book.current_pair(&left, &right).unwrap_err();
    assert!(matches!(
        error,
        MarketDataError::PairSkew {
            skew_millis: 2_000,
            tolerance_millis: 500,
            ..
        }
    ));
}

#[test]
fn timestamp_provenance_and_source_latency_are_observable() {
    let base = timestamp(0);
    let event_key = instrument("event", "BTC-USDT");
    let local_key = instrument("local", "BTC-USDT");
    let mut book = book(
        vec![event_key.clone(), local_key.clone()],
        base + Duration::seconds(3),
        Duration::seconds(10),
    );
    book.apply(observation_event_with_metadata(
        snapshot("event", "BTC-USDT", base),
        7,
        base + Duration::seconds(2),
        MarketTimestampProvenance::VenueEventTime,
        Some(7),
    ))
    .unwrap();
    book.apply(observation_event_with_metadata(
        snapshot("local", "BTC-USDT", base + Duration::seconds(2)),
        1,
        base + Duration::seconds(2),
        MarketTimestampProvenance::LocalReceipt,
        None,
    ))
    .unwrap();

    let view = book.view();
    let event_row = view.instrument(&event_key).unwrap();
    assert_eq!(
        event_row.timestamp_provenance,
        MarketTimestampProvenance::VenueEventTime
    );
    assert_eq!(event_row.source_latency_millis, Some(2_000));

    let local_row = view.instrument(&local_key).unwrap();
    assert_eq!(
        local_row.timestamp_provenance,
        MarketTimestampProvenance::LocalReceipt
    );
    assert_eq!(local_row.source_latency_millis, None);
}

#[test]
fn deterministic_adapter_is_bounded_and_exhaustion_is_explicit() {
    let base = timestamp(0);
    let event = observation_event(snapshot("paper", "BTC-USDT", base), 1, base);
    let mut adapter = DeterministicMarketDataAdapter::new(vec![event.clone()]).unwrap();

    assert_eq!(adapter.next_event(), Some(event));
    assert_eq!(adapter.next_event(), None);

    let events =
        vec![MarketDataEvent::source_gap("paper", 1, base).unwrap(); MAX_MARKET_DATA_EVENTS + 1];
    assert!(matches!(
        DeterministicMarketDataAdapter::new(events).unwrap_err(),
        MarketDataError::EventBufferTooLarge {
            count,
            limit: MAX_MARKET_DATA_EVENTS
        } if count == MAX_MARKET_DATA_EVENTS + 1
    ));
}

#[tokio::test]
async fn slow_subscription_consumer_gets_a_gap_before_the_latest_snapshot() {
    let base = timestamp(0);
    let clock = Arc::new(TestClock::new(base + Duration::seconds(2)));
    let paper_clock = Arc::clone(&clock);
    let paper = PaperExchange::with_clock_and_freshness(
        "paper",
        NonZeroUsize::new(1).unwrap(),
        move || paper_clock.now(),
        Duration::days(1),
        Duration::days(1),
    )
    .unwrap();
    let subscription = paper
        .subscribe(MarketSubscription::all_snapshots(None))
        .await
        .unwrap();
    let key = instrument("paper", "BTC-USDT");
    let universe = MarketUniverse::new(vec![key.clone()]).unwrap();
    let mut adapter = SubscriptionMarketDataAdapter::new(
        "paper",
        subscription,
        universe.clone(),
        Arc::clone(&clock),
    )
    .unwrap();
    let mut book = MarketDataBook::new(
        universe,
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        clock,
    );

    paper
        .publish_snapshot(snapshot("paper", "BTC-USDT", base))
        .await
        .unwrap();
    let latest = snapshot("paper", "BTC-USDT", base + Duration::seconds(1));
    paper.publish_snapshot(latest.clone()).await.unwrap();

    let gap = adapter.next_event().await.unwrap();
    assert!(matches!(gap, MarketDataEvent::SourceGap { skipped: 1, .. }));
    book.apply(gap).unwrap();
    assert_eq!(
        book.view().instrument(&key).unwrap().continuity,
        MarketContinuity::SourceGap { skipped: 1 }
    );

    let observation = adapter.next_event().await.unwrap();
    book.apply(observation).unwrap();
    let view = book.view();
    let row = view.instrument(&key).unwrap();
    assert_eq!(row.snapshot, Some(latest));
    assert_eq!(row.continuity, MarketContinuity::Continuous);
}

fn book(
    instruments: Vec<MarketInstrument>,
    now: DateTime<Utc>,
    max_age: Duration,
) -> MarketDataBook {
    MarketDataBook::new(
        MarketUniverse::new(instruments).unwrap(),
        MarketFreshnessPolicy::new(max_age, Duration::seconds(1)).unwrap(),
        Arc::new(TestClock::new(now)),
    )
}

fn instrument(exchange: &str, symbol: &str) -> MarketInstrument {
    MarketInstrument::new(
        exchange,
        Symbol::new(symbol).unwrap(),
        MarketType::Perpetual,
    )
    .unwrap()
}

fn observation_event(
    snapshot: MarketSnapshot,
    revision: u64,
    received_at: DateTime<Utc>,
) -> MarketDataEvent {
    MarketDataEvent::Observation(
        MarketDataObservation::new(snapshot, revision, received_at).unwrap(),
    )
}

fn observation_event_with_metadata(
    snapshot: MarketSnapshot,
    revision: u64,
    received_at: DateTime<Utc>,
    timestamp_provenance: MarketTimestampProvenance,
    source_sequence: Option<u64>,
) -> MarketDataEvent {
    MarketDataEvent::Observation(
        MarketDataObservation::with_source_metadata(
            snapshot,
            revision,
            received_at,
            timestamp_provenance,
            source_sequence,
        )
        .unwrap(),
    )
}

fn snapshot(exchange: &str, symbol: &str, at: DateTime<Utc>) -> MarketSnapshot {
    MarketSnapshot::new(
        exchange,
        Symbol::new(symbol).unwrap(),
        MarketType::Perpetual,
        Price::new(Decimal::from(99)).unwrap(),
        Price::new(Decimal::from(101)).unwrap(),
        at,
    )
    .unwrap()
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}
