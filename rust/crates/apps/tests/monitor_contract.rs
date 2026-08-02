use std::{
    fmt,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_cli::monitor::{
    ARBITRAGE_MONITOR_EVENT_SCHEMA_VERSION, ArbitrageMonitorOutcome, ReadOnlyArbitrageMonitor,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_runtime::{
    MarketContinuity, MarketDataBook, MarketDataClock, MarketDataEvent, MarketDataFreshness,
    MarketDataObservation, MarketFreshnessPolicy, MarketInstrument, MarketUniverse,
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
fn monitor_emits_waiting_then_opportunity_without_order_authority() {
    let base = timestamp(0);
    let (mut monitor, left, right, _) = monitor(base);

    let waiting = monitor
        .process(observation(snapshot("left", "99", "100", base), 1, base))
        .unwrap();
    assert_eq!(
        waiting.schema_version,
        ARBITRAGE_MONITOR_EVENT_SCHEMA_VERSION
    );
    assert_eq!(waiting.sequence, 1);
    assert!(matches!(
        waiting.outcome,
        ArbitrageMonitorOutcome::Waiting {
            instrument,
            continuity: MarketContinuity::Missing,
            ..
        } if instrument == right
    ));

    let opportunity = monitor
        .process(observation(snapshot("right", "102", "103", base), 1, base))
        .unwrap();
    assert_eq!(opportunity.sequence, 2);
    assert_eq!(opportunity.market_generation, 2);
    assert!(matches!(
        opportunity.outcome,
        ArbitrageMonitorOutcome::Opportunity {
            ref buy_exchange,
            ref sell_exchange,
            spread_percent,
            ..
        } if buy_exchange == "left"
            && sell_exchange == "right"
            && spread_percent == Decimal::from(2)
    ));

    let record = opportunity.to_record();
    assert_eq!(record.strategy, "arbitrage_monitor");
    assert_eq!(record.decision, "monitor_opportunity");
    assert_eq!(record.details["left"]["exchange"], left.exchange());
    assert!(record.details.get("intents").is_none());
    assert!(record.details.get("orders").is_none());
}

#[test]
fn below_threshold_and_stale_market_are_distinct_read_only_outcomes() {
    let base = timestamp(0);
    let (mut monitor, _, _, clock) = monitor(base);
    monitor
        .process(observation(snapshot("left", "99", "100", base), 1, base))
        .unwrap();
    let no_opportunity = monitor
        .process(observation(snapshot("right", "100", "101", base), 1, base))
        .unwrap();

    assert!(matches!(
        no_opportunity.outcome,
        ArbitrageMonitorOutcome::NoOpportunity {
            spread_percent,
            threshold_percent,
            ..
        } if spread_percent == Decimal::ZERO
            && threshold_percent == Decimal::new(5, 1)
    ));
    assert_eq!(
        no_opportunity.to_record().decision,
        "monitor_no_opportunity"
    );

    clock.set(base + Duration::seconds(11));
    let stale = monitor
        .process(observation(
            snapshot("left", "100", "101", base + Duration::seconds(1)),
            2,
            base + Duration::seconds(1),
        ))
        .unwrap();
    assert!(matches!(
        stale.outcome,
        ArbitrageMonitorOutcome::Waiting { .. }
    ));
    assert_eq!(stale.to_record().decision, "monitor_waiting");
}

#[test]
fn pair_skew_waits_for_the_lagging_leg_without_terminating_the_monitor() {
    let base = timestamp(0);
    let (mut monitor, _, _, clock) = monitor(base);
    monitor
        .process(observation(snapshot("left", "99", "100", base), 1, base))
        .unwrap();
    clock.set(base + Duration::seconds(2));

    let waiting = monitor
        .process(observation(
            snapshot("right", "102", "103", base + Duration::seconds(2)),
            1,
            base + Duration::seconds(2),
        ))
        .unwrap();

    assert!(matches!(
        waiting.outcome,
        ArbitrageMonitorOutcome::Waiting {
            ref instrument,
            freshness: MarketDataFreshness::PairSkew {
                skew_millis: 2_000,
                tolerance_millis: 1_000,
            },
            continuity: MarketContinuity::Continuous,
        } if instrument.exchange() == "left"
    ));
    assert_eq!(waiting.to_record().decision, "monitor_waiting");
}

#[test]
fn duplicate_gap_and_recovery_are_events_not_silent_state_changes() {
    let base = timestamp(0);
    let (mut monitor, _, right, clock) = monitor(base);
    monitor
        .process(observation(snapshot("left", "99", "100", base), 1, base))
        .unwrap();
    monitor
        .process(observation(snapshot("right", "102", "103", base), 1, base))
        .unwrap();

    let duplicate = monitor
        .process(observation(
            snapshot("right", "103", "104", base + Duration::seconds(1)),
            1,
            base + Duration::seconds(1),
        ))
        .unwrap();
    assert!(matches!(
        duplicate.outcome,
        ArbitrageMonitorOutcome::Waiting {
            instrument,
            continuity: MarketContinuity::Duplicate { revision: 1 },
            ..
        } if instrument == right
    ));

    // Keep the sibling leg inside the independent pair-skew bound while this
    // test focuses on right-source gap recovery.
    monitor
        .process(observation(
            snapshot("left", "99", "100", base + Duration::seconds(1)),
            2,
            base + Duration::seconds(1),
        ))
        .unwrap();

    let gap = monitor
        .process(MarketDataEvent::source_gap("right", 3, base + Duration::seconds(2)).unwrap())
        .unwrap();
    assert!(matches!(
        gap.outcome,
        ArbitrageMonitorOutcome::Waiting {
            continuity: MarketContinuity::SourceGap { skipped: 3 },
            ..
        }
    ));

    clock.set(base + Duration::seconds(2));
    let recovered = monitor
        .process(observation(
            snapshot("right", "102", "103", base + Duration::seconds(2)),
            2,
            base + Duration::seconds(2),
        ))
        .unwrap();
    assert!(matches!(
        recovered.outcome,
        ArbitrageMonitorOutcome::Opportunity { .. }
    ));
}

#[test]
fn invalid_financial_input_is_isolated_as_an_analysis_rejection() {
    let base = timestamp(0);
    let (mut monitor, _, _, _) = monitor(base);
    monitor
        .process(observation(
            snapshot(
                "left",
                "0.0000000000000000000000000001",
                "0.0000000000000000000000000001",
                base,
            ),
            1,
            base,
        ))
        .unwrap();

    let rejected = monitor
        .process(observation(
            snapshot(
                "right",
                "79228162514264337593543950335",
                "79228162514264337593543950335",
                base,
            ),
            1,
            base,
        ))
        .unwrap();

    assert!(matches!(
        rejected.outcome,
        ArbitrageMonitorOutcome::AnalysisRejected { .. }
    ));
    assert_eq!(rejected.to_record().decision, "monitor_analysis_rejected");
}

fn monitor(
    now: DateTime<Utc>,
) -> (
    ReadOnlyArbitrageMonitor,
    MarketInstrument,
    MarketInstrument,
    Arc<TestClock>,
) {
    let left = instrument("left");
    let right = instrument("right");
    let universe = MarketUniverse::new(vec![left.clone(), right.clone()]).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let book = MarketDataBook::new(
        universe,
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        Arc::clone(&clock),
    );
    let monitor =
        ReadOnlyArbitrageMonitor::new(book, left.clone(), right.clone(), Decimal::new(5, 1))
            .unwrap();
    (monitor, left, right, clock)
}

fn instrument(exchange: &str) -> MarketInstrument {
    MarketInstrument::new(
        exchange,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
    )
    .unwrap()
}

fn observation(
    snapshot: MarketSnapshot,
    revision: u64,
    received_at: DateTime<Utc>,
) -> MarketDataEvent {
    MarketDataEvent::Observation(
        MarketDataObservation::new(snapshot, revision, received_at).unwrap(),
    )
}

fn snapshot(exchange: &str, bid: &str, ask: &str, at: DateTime<Utc>) -> MarketSnapshot {
    MarketSnapshot::new(
        exchange,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(bid.parse().unwrap()).unwrap(),
        Price::new(ask.parse().unwrap()).unwrap(),
        at,
    )
    .unwrap()
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}
