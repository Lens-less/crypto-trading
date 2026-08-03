use std::{
    collections::VecDeque,
    future,
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration as StdDuration, Instant as StdInstant},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_exchange::BinancePublicExchange;
use crypto_trading_runtime::{
    BinancePollingRoute, BinancePublicPollingSource, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
    MAX_MARKET_SUPERVISOR_BUFFERED_EVENTS, MarketContinuity, MarketDataBook, MarketDataClock,
    MarketDataError, MarketDataEvent, MarketDataEventFuture, MarketDataEventSource,
    MarketDataFreshness, MarketDataObservation, MarketDataSourceFailure, MarketFreshnessPolicy,
    MarketInstrument, MarketPollingPolicy, MarketSupervisor, MarketSupervisorConfig,
    MarketSupervisorExit, MarketSupervisorHealth, MarketSupervisorPhase, MarketUniverse,
};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug)]
struct FixedClock {
    now: DateTime<Utc>,
}

impl MarketDataClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

#[derive(Debug)]
struct ScriptedSource {
    source_id: String,
    events: VecDeque<Result<Option<MarketDataEvent>, MarketDataError>>,
}

impl ScriptedSource {
    fn new(source_id: &str, events: Vec<MarketDataEvent>) -> Self {
        let mut events = events
            .into_iter()
            .map(|event| Ok(Some(event)))
            .collect::<VecDeque<_>>();
        events.push_back(Ok(None));
        Self {
            source_id: source_id.to_owned(),
            events,
        }
    }
}

impl MarketDataEventSource for ScriptedSource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(async move { self.events.pop_front().unwrap_or(Ok(None)) })
    }
}

#[derive(Debug)]
struct PendingSource;

impl MarketDataEventSource for PendingSource {
    fn source_id(&self) -> &'static str {
        "binance"
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(future::pending())
    }
}

#[test]
fn binance_polling_routes_are_exact_spot_only_and_unambiguous() {
    let clock = Arc::new(FixedClock { now: timestamp(0) });
    let policy = polling_policy(StdDuration::from_millis(1));
    let exchange = BinancePublicExchange::new().unwrap();

    let other = BinancePollingRoute::new(
        instrument("other", "BTC-USDT", MarketType::Spot),
        Symbol::new("BTCUSDT").unwrap(),
    )
    .unwrap();
    assert!(matches!(
        BinancePublicPollingSource::new(exchange.clone(), vec![other], policy, Arc::clone(&clock))
            .unwrap_err(),
        MarketDataError::UnsupportedPollingInstrument { .. }
    ));

    let perpetual = BinancePollingRoute::new(
        instrument("binance", "BTC-USDT", MarketType::Perpetual),
        Symbol::new("BTCUSDT").unwrap(),
    )
    .unwrap();
    assert!(matches!(
        BinancePublicPollingSource::new(
            exchange.clone(),
            vec![perpetual],
            policy,
            Arc::clone(&clock)
        )
        .unwrap_err(),
        MarketDataError::UnsupportedPollingInstrument { .. }
    ));

    let duplicate_route = route("BTC-USDT", "BTCUSDT");
    assert!(matches!(
        BinancePublicPollingSource::new(
            exchange.clone(),
            vec![duplicate_route.clone(), duplicate_route],
            policy,
            Arc::clone(&clock)
        )
        .unwrap_err(),
        MarketDataError::DuplicatePollingInstrument { .. }
    ));

    assert!(matches!(
        BinancePublicPollingSource::new(
            exchange,
            vec![route("BTC-USDT", "BTCUSDT"), route("BTC-USDC", "BTCUSDT")],
            policy,
            clock
        )
        .unwrap_err(),
        MarketDataError::DuplicatePollingWireSymbol { .. }
    ));

    for policy in [
        MarketPollingPolicy::new(
            StdDuration::ZERO,
            StdDuration::from_secs(1),
            StdDuration::from_secs(1),
        ),
        MarketPollingPolicy::new(
            StdDuration::from_secs(1),
            StdDuration::from_secs(2),
            StdDuration::from_secs(1),
        ),
        MarketPollingPolicy::new(
            StdDuration::from_secs(1),
            StdDuration::from_secs(1),
            StdDuration::from_secs(3_601),
        ),
    ] {
        assert!(matches!(
            policy.unwrap_err(),
            MarketDataError::InvalidPollingPolicy(_)
        ));
    }
}

#[test]
fn polling_policy_backoff_is_deterministic_exponential_and_hard_capped() {
    let policy = MarketPollingPolicy::new(
        StdDuration::from_secs(1),
        StdDuration::from_secs(1),
        StdDuration::from_secs(10),
    )
    .unwrap();

    assert_eq!(policy.retry_delay_after(0), StdDuration::ZERO);
    assert_eq!(policy.retry_delay_after(1), StdDuration::from_secs(1));
    assert_eq!(policy.retry_delay_after(2), StdDuration::from_secs(2));
    assert_eq!(policy.retry_delay_after(3), StdDuration::from_secs(4));
    assert_eq!(policy.retry_delay_after(4), StdDuration::from_secs(8));
    assert_eq!(policy.retry_delay_after(5), StdDuration::from_secs(10));
    assert_eq!(
        policy.retry_delay_after(u32::MAX),
        StdDuration::from_secs(10)
    );

    let hard_limit = MarketPollingPolicy::new(
        StdDuration::from_secs(1),
        StdDuration::from_secs(15 * 60),
        StdDuration::from_secs(60 * 60),
    )
    .unwrap();
    assert_eq!(
        hard_limit.retry_delay_after(3),
        StdDuration::from_secs(60 * 60)
    );
    assert_eq!(
        hard_limit.retry_delay_after(u32::MAX),
        StdDuration::from_secs(60 * 60)
    );
}

#[tokio::test]
async fn binance_polling_reconnects_after_failure_without_fabricating_a_quote() {
    let responses = vec![
        http_response(
            "502 Bad Gateway",
            r#"{"code":-1000,"msg":"temporarily unavailable"}"#,
        ),
        http_response("200 OK", r#"{"symbol":"LTCBTC","bidPrice":"broken"}"#),
        http_response(
            "200 OK",
            include_str!("../../exchange/tests/fixtures/binance_book_ticker.json"),
        ),
        http_response(
            "200 OK",
            include_str!("../../exchange/tests/fixtures/binance_book_ticker.json"),
        ),
    ];
    let (base_url, server) = stub_server(responses);
    let now = Utc::now() + Duration::seconds(1);
    let clock = Arc::new(FixedClock { now });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();
    let mut source = BinancePublicPollingSource::new(
        exchange,
        vec![route("LTC-BTC-SPOT", "LTCBTC")],
        polling_policy(StdDuration::from_millis(1)),
        clock,
    )
    .unwrap();

    let unavailable = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        unavailable,
        MarketDataEvent::SourceUnavailable {
            exchange,
            failure: MarketDataSourceFailure::Disconnected,
            ..
        } if exchange == "binance"
    ));

    let invalid = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        invalid,
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::InvalidPayload,
            ..
        }
    ));

    let recovered = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        recovered,
        MarketDataEvent::Observation(MarketDataObservation {
            snapshot,
            revision: 1,
            received_at,
            ..
        }) if snapshot.exchange() == "binance"
            && snapshot.symbol.as_str() == "LTC-BTC-SPOT"
            && snapshot.market_type == MarketType::Spot
            && received_at == now
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation { revision: 2, .. })
    ));
    server.join().unwrap();
}

#[tokio::test]
async fn binance_polling_keeps_wire_sequence_separate_from_poll_continuity() {
    let (base_url, server) = stub_server(vec![
        http_response(
            "200 OK",
            r#"{
            "symbol":"LTCBTC",
            "bidPrice":"4.00000000",
            "bidQty":"431.00000000",
            "askPrice":"4.00000200",
            "askQty":"9.00000000",
            "u":41
        }"#,
        ),
        http_response(
            "200 OK",
            r#"{
            "symbol":"LTCBTC",
            "bidPrice":"4.10000000",
            "bidQty":"431.00000000",
            "askPrice":"4.10000200",
            "askQty":"9.00000000",
            "u":47
        }"#,
        ),
    ]);
    let now = timestamp(1);
    let clock = Arc::new(FixedClock { now });
    let exchange = BinancePublicExchange::with_base_url(&base_url).unwrap();
    let key = instrument("binance", "LTC-BTC-SPOT", MarketType::Spot);
    let mut source = BinancePublicPollingSource::new(
        exchange,
        vec![BinancePollingRoute::new(key.clone(), Symbol::new("LTCBTC").unwrap()).unwrap()],
        polling_policy(StdDuration::from_millis(1)),
        Arc::clone(&clock),
    )
    .unwrap();

    let first = source.next_event().await.unwrap().unwrap();
    let second = source.next_event().await.unwrap().unwrap();

    assert!(matches!(
        &first,
        MarketDataEvent::Observation(MarketDataObservation {
            snapshot,
            revision: 1,
            received_at,
            timestamp_provenance,
            source_sequence,
        }) if snapshot.symbol.as_str() == "LTC-BTC-SPOT"
            && *received_at == now
            && *timestamp_provenance == crypto_trading_runtime::MarketTimestampProvenance::LocalReceipt
            && *source_sequence == Some(41)
    ));
    assert!(matches!(
        &second,
        MarketDataEvent::Observation(MarketDataObservation {
            revision: 2,
            source_sequence: Some(47),
            ..
        })
    ));

    let mut book = MarketDataBook::new(
        MarketUniverse::new(vec![key.clone()]).unwrap(),
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        clock,
    );
    book.apply(first).unwrap();
    book.apply(second).unwrap();
    assert_eq!(
        book.view().instrument(&key).unwrap().continuity,
        MarketContinuity::Continuous
    );
    server.join().unwrap();
}

#[tokio::test]
async fn binance_due_targets_are_fetched_concurrently_without_dropping_results() {
    let request_count = 3;
    let response_delay = StdDuration::from_millis(250);
    let (base_url, server) = delayed_binance_stub_server(request_count, response_delay);
    let clock = Arc::new(FixedClock { now: timestamp(1) });
    let mut source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![
            route("CCC-USDT", "CCCUSDT"),
            route("AAA-USDT", "AAAUSDT"),
            route("BBB-USDT", "BBBUSDT"),
        ],
        polling_policy(StdDuration::from_millis(1)),
        clock,
    )
    .unwrap();

    let started = StdInstant::now();
    let mut symbols = Vec::new();
    for _ in 0..request_count {
        let event = source.next_event().await.unwrap().unwrap();
        let MarketDataEvent::Observation(observation) = event else {
            panic!("delayed fixture must return a valid observation");
        };
        symbols.push(observation.snapshot.symbol.as_str().to_owned());
    }
    let elapsed = started.elapsed();

    symbols.sort();
    assert_eq!(symbols, ["AAA-USDT", "BBB-USDT", "CCC-USDT"]);
    assert!(
        elapsed < StdDuration::from_millis(650),
        "one due-target round took {elapsed:?}; serial polling would take at least {:?}",
        response_delay * u32::try_from(request_count).unwrap()
    );
    server.join().unwrap();
}

#[tokio::test]
async fn concurrent_binance_routes_emit_in_completion_order() {
    let (base_url, server) = completion_order_binance_stub_server();
    let clock = Arc::new(FixedClock { now: timestamp(1) });
    let aaa = instrument("binance", "AAA-USDT", MarketType::Spot);
    let bbb = instrument("binance", "BBB-USDT", MarketType::Spot);
    let mut source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![route("AAA-USDT", "AAAUSDT"), route("BBB-USDT", "BBBUSDT")],
        polling_policy(StdDuration::from_millis(1)),
        Arc::clone(&clock),
    )
    .unwrap();

    let first = source.next_event().await.unwrap().unwrap();
    let second = source.next_event().await.unwrap().unwrap();

    assert!(matches!(
        &first,
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::Backpressure,
            ..
        }
    ));
    assert!(matches!(
        &second,
        MarketDataEvent::Observation(observation)
            if observation.snapshot.symbol.as_str() == "AAA-USDT"
    ));

    let mut book = MarketDataBook::new(
        MarketUniverse::new(vec![aaa, bbb]).unwrap(),
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        clock,
    );
    book.apply(first).unwrap();
    book.apply(second).unwrap();
    server.join().unwrap();
}

#[tokio::test]
async fn binance_poll_interval_starts_after_request_completion() {
    let response_delay = StdDuration::from_millis(180);
    let poll_interval = StdDuration::from_millis(120);
    let (base_url, server) = cooldown_binance_stub_server(response_delay);
    let clock = Arc::new(FixedClock { now: timestamp(1) });
    let mut source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![
            BinancePollingRoute::new(
                instrument("binance", "LTC-BTC-SPOT", MarketType::Spot),
                Symbol::new("LTCBTC").unwrap(),
            )
            .unwrap(),
        ],
        MarketPollingPolicy::new(poll_interval, poll_interval, poll_interval).unwrap(),
        clock,
    )
    .unwrap();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(_)
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(_)
    ));

    let (first_response_completed, second_request_accepted) = server.join().unwrap();
    assert!(
        second_request_accepted.duration_since(first_response_completed)
            >= StdDuration::from_millis(100),
        "next request started before the post-completion cooldown elapsed"
    );
}

#[tokio::test]
async fn supervisor_retains_one_concurrent_poll_round_without_synthetic_gaps() {
    let request_count = 3;
    let (base_url, server) =
        delayed_binance_stub_server(request_count, StdDuration::from_millis(25));
    let clock = Arc::new(FixedClock { now: timestamp(1) });
    let source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![
            route("CCC-USDT", "CCCUSDT"),
            route("AAA-USDT", "AAAUSDT"),
            route("BBB-USDT", "BBBUSDT"),
        ],
        MarketPollingPolicy::new(
            StdDuration::from_secs(30),
            StdDuration::from_secs(30),
            StdDuration::from_secs(30),
        )
        .unwrap(),
        clock,
    )
    .unwrap();
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(46),
        source,
        supervisor_config(StdDuration::from_secs(5)),
    )
    .unwrap();
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while supervisor.status().event_sequence < u64::try_from(request_count).unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let mut symbols = Vec::new();
    for _ in 0..request_count {
        let event = supervisor.next_event().await.unwrap().unwrap();
        let MarketDataEvent::Observation(observation) = event else {
            panic!("one bounded poll round must not synthesize a source gap");
        };
        symbols.push(observation.snapshot.symbol.as_str().to_owned());
    }

    symbols.sort();
    assert_eq!(symbols, ["AAA-USDT", "BBB-USDT", "CCC-USDT"]);
    assert_eq!(
        supervisor.stop().await.unwrap(),
        MarketSupervisorExit::StopRequested
    );
    server.join().unwrap();
}

#[tokio::test]
async fn supervisor_lag_preserves_retained_window_and_counts_overwrites() {
    let base = timestamp(0);
    let event_count = u64::try_from(MAX_MARKET_SUPERVISOR_BUFFERED_EVENTS).unwrap() + 3;
    let source = ScriptedSource::new(
        "binance",
        (1..=event_count)
            .map(|revision| observation("binance", "BTCUSDT", revision, base))
            .collect(),
    );
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(41),
        source,
        supervisor_config(StdDuration::from_millis(100)),
    )
    .unwrap();

    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if supervisor.status().event_sequence == event_count {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(matches!(
        supervisor.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap {
            exchange,
            skipped,
            observed_at,
        } if exchange == "binance" && skipped == 3 && observed_at == base
    ));
    for expected_revision in 4..=event_count {
        assert!(matches!(
            supervisor.next_event().await.unwrap().unwrap(),
            MarketDataEvent::Observation(MarketDataObservation { revision, .. })
                if revision == expected_revision
        ));
    }
    assert!(supervisor.next_event().await.unwrap().is_none());

    let status = supervisor.status();
    assert_eq!(
        status.schema_version,
        MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION
    );
    assert_eq!(status.task_id, Uuid::from_u128(41));
    assert_eq!(status.phase, MarketSupervisorPhase::Stopped);
    assert_eq!(status.health, MarketSupervisorHealth::Healthy);
    assert_eq!(status.dropped_event_count, 3);
    assert_eq!(status.exit, Some(MarketSupervisorExit::SourceEnded));
}

#[tokio::test]
async fn supervisor_last_event_time_is_a_non_regressing_projection_high_water() {
    let base = timestamp(0);
    let source = ScriptedSource::new(
        "binance",
        vec![
            observation("binance", "BTCUSDT", 1, base + Duration::seconds(2)),
            MarketDataEvent::source_unavailable(
                "binance",
                MarketDataSourceFailure::Disconnected,
                base + Duration::seconds(1),
            )
            .unwrap(),
        ],
    );
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(47),
        source,
        supervisor_config(StdDuration::from_millis(100)),
    )
    .unwrap();

    assert!(supervisor.next_event().await.unwrap().is_some());
    assert!(supervisor.next_event().await.unwrap().is_some());
    assert!(supervisor.next_event().await.unwrap().is_none());

    assert_eq!(
        supervisor.status().last_event_at,
        Some(base + Duration::seconds(2))
    );
}

#[tokio::test]
async fn supervisor_stop_cancels_an_in_flight_source_within_the_grace_period() {
    // The grace only bounds a stop that would otherwise hang: a cancelled
    // in-flight source must report `StopRequested` well before it elapses. A
    // generous grace keeps CI scheduling jitter from forcing the
    // `ShutdownTimedOut` fallback while still failing fast if cancellation
    // ever regresses.
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(42),
        PendingSource,
        supervisor_config(StdDuration::from_secs(5)),
    )
    .unwrap();

    let exit = tokio::time::timeout(StdDuration::from_secs(30), supervisor.stop())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(exit, MarketSupervisorExit::StopRequested);
    assert_eq!(supervisor.status().phase, MarketSupervisorPhase::Stopped);
    assert_eq!(
        supervisor.status().exit,
        Some(MarketSupervisorExit::StopRequested)
    );
}

#[tokio::test]
async fn supervisor_rejects_source_identity_drift_and_fails_closed() {
    let base = timestamp(0);
    let source = ScriptedSource::new("binance", vec![observation("other", "BTCUSDT", 1, base)]);
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(45),
        source,
        supervisor_config(StdDuration::from_millis(100)),
    )
    .unwrap();

    assert!(matches!(
        supervisor.next_event().await.unwrap_err(),
        crypto_trading_runtime::MarketSupervisorError::SourceContract(
            MarketDataError::SourceIdentityMismatch { expected, actual }
        ) if expected == "binance" && actual == "other"
    ));
    assert_eq!(supervisor.status().phase, MarketSupervisorPhase::Failed);
    assert_eq!(supervisor.status().event_sequence, 0);
}

#[tokio::test]
async fn supervisor_cancels_a_long_polling_backoff_without_waiting_for_the_timer() {
    let (base_url, server) = stub_server(vec![http_response(
        "502 Bad Gateway",
        r#"{"code":-1000,"msg":"temporarily unavailable"}"#,
    )]);
    let clock = Arc::new(FixedClock {
        now: Utc::now() + Duration::seconds(1),
    });
    let source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![route("LTC-BTC-SPOT", "LTCBTC")],
        MarketPollingPolicy::new(
            StdDuration::from_secs(30),
            StdDuration::from_secs(30),
            StdDuration::from_secs(30),
        )
        .unwrap(),
        clock,
    )
    .unwrap();
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(43),
        source,
        supervisor_config(StdDuration::from_secs(5)),
    )
    .unwrap();

    assert!(matches!(
        supervisor.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable { .. }
    ));
    // Ten seconds is a CI-jitter margin that still proves the semantic point:
    // the stop never waits out the thirty-second polling backoff timer.
    let exit = tokio::time::timeout(StdDuration::from_secs(10), supervisor.stop())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit, MarketSupervisorExit::StopRequested);
    server.join().unwrap();
}

#[tokio::test]
async fn external_failure_degrades_the_book_and_recovery_restores_a_fresh_quote() {
    let responses = vec![
        http_response_with_headers(
            "429 Too Many Requests",
            &["Retry-After: 1"],
            r#"{"code":-1003,"msg":"too many requests"}"#,
        ),
        http_response(
            "200 OK",
            include_str!("../../exchange/tests/fixtures/binance_book_ticker.json"),
        ),
    ];
    let (base_url, server) = stub_server(responses);
    let now = Utc::now() + Duration::seconds(1);
    let clock = Arc::new(FixedClock { now });
    let key = instrument("binance", "LTC-BTC-SPOT", MarketType::Spot);
    let source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![BinancePollingRoute::new(key.clone(), Symbol::new("LTCBTC").unwrap()).unwrap()],
        polling_policy(StdDuration::from_millis(1)),
        Arc::clone(&clock),
    )
    .unwrap();
    let mut supervisor = MarketSupervisor::start(
        Uuid::from_u128(44),
        source,
        supervisor_config(StdDuration::from_millis(100)),
    )
    .unwrap();
    let mut book = MarketDataBook::new(
        MarketUniverse::new(vec![key.clone()]).unwrap(),
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        clock,
    );

    book.apply(supervisor.next_event().await.unwrap().unwrap())
        .unwrap();
    let degraded = book.view();
    let degraded_row = degraded.instrument(&key).unwrap();
    assert_eq!(
        degraded_row.continuity,
        MarketContinuity::Unavailable {
            failure: MarketDataSourceFailure::Backpressure
        }
    );
    assert_eq!(degraded_row.freshness, MarketDataFreshness::Missing);

    let retry_started = StdInstant::now();
    book.apply(supervisor.next_event().await.unwrap().unwrap())
        .unwrap();
    assert!(retry_started.elapsed() >= StdDuration::from_millis(900));
    let recovered = book.view();
    let recovered_row = recovered.instrument(&key).unwrap();
    assert_eq!(recovered_row.continuity, MarketContinuity::Continuous);
    assert!(recovered_row.freshness.is_fresh());
    assert_eq!(
        recovered_row
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.symbol.as_str()),
        Some("LTC-BTC-SPOT")
    );

    supervisor.stop().await.unwrap();
    server.join().unwrap();
}

fn polling_policy(retry: StdDuration) -> MarketPollingPolicy {
    MarketPollingPolicy::new(StdDuration::from_millis(1), retry, retry).unwrap()
}

fn supervisor_config(shutdown_grace: StdDuration) -> MarketSupervisorConfig {
    MarketSupervisorConfig::new(shutdown_grace).unwrap()
}

fn route(canonical_symbol: &str, wire_symbol: &str) -> BinancePollingRoute {
    BinancePollingRoute::new(
        instrument("binance", canonical_symbol, MarketType::Spot),
        Symbol::new(wire_symbol).unwrap(),
    )
    .unwrap()
}

fn instrument(exchange: &str, symbol: &str, market_type: MarketType) -> MarketInstrument {
    MarketInstrument::new(exchange, Symbol::new(symbol).unwrap(), market_type).unwrap()
}

fn observation(exchange: &str, symbol: &str, revision: u64, at: DateTime<Utc>) -> MarketDataEvent {
    let snapshot = MarketSnapshot::new(
        exchange,
        Symbol::new(symbol).unwrap(),
        MarketType::Spot,
        Price::new(Decimal::from_str("99").unwrap()).unwrap(),
        Price::new(Decimal::from_str("101").unwrap()).unwrap(),
        at,
    )
    .unwrap();
    MarketDataEvent::Observation(MarketDataObservation::new(snapshot, revision, at).unwrap())
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}

fn stub_server(responses: Vec<String>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1_024];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            assert!(
                String::from_utf8(request)
                    .unwrap()
                    .starts_with("GET /api/v3/ticker/bookTicker?symbol=LTCBTC HTTP/1.1\r\n")
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (base_url, server)
}

fn delayed_binance_stub_server(
    request_count: usize,
    response_delay: StdDuration,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let mut handlers = Vec::with_capacity(request_count);
        for _ in 0..request_count {
            let (mut stream, _) = listener.accept().unwrap();
            handlers.push(thread::spawn(move || {
                stream
                    .set_read_timeout(Some(StdDuration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1_024];
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                let symbol = request
                    .split("symbol=")
                    .nth(1)
                    .and_then(|suffix| suffix.split_whitespace().next())
                    .expect("request must carry an exact wire symbol");
                thread::sleep(response_delay);
                let body = format!(
                    r#"{{"symbol":"{symbol}","bidPrice":"99","bidQty":"1","askPrice":"101","askQty":"1","u":1}}"#
                );
                stream
                    .write_all(http_response("200 OK", &body).as_bytes())
                    .unwrap();
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });
    (base_url, server)
}

fn completion_order_binance_stub_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let mut handlers = Vec::with_capacity(2);
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            handlers.push(thread::spawn(move || {
                stream
                    .set_read_timeout(Some(StdDuration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1_024];
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    request.extend_from_slice(&buffer[..read]);
                    if read == 0 || request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                let symbol = request
                    .split("symbol=")
                    .nth(1)
                    .and_then(|suffix| suffix.split_whitespace().next())
                    .expect("request must carry an exact wire symbol");
                let response = if symbol == "AAAUSDT" {
                    thread::sleep(StdDuration::from_millis(250));
                    let body =
                        r#"{"symbol":"AAAUSDT","bidPrice":"99","bidQty":"1","askPrice":"101","askQty":"1","u":1}"#;
                    http_response("200 OK", body)
                } else {
                    thread::sleep(StdDuration::from_millis(20));
                    http_response(
                        "429 Too Many Requests",
                        r#"{"code":-1003,"msg":"too many requests"}"#,
                    )
                };
                stream.write_all(response.as_bytes()).unwrap();
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });
    (base_url, server)
}

fn cooldown_binance_stub_server(
    first_response_delay: StdDuration,
) -> (String, thread::JoinHandle<(StdInstant, StdInstant)>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let body = include_str!("../../exchange/tests/fixtures/binance_book_ticker.json");
        let mut first = listener.accept().unwrap().0;
        read_http_headers(&mut first);
        thread::sleep(first_response_delay);
        first
            .write_all(http_response("200 OK", body).as_bytes())
            .unwrap();
        let first_response_completed = StdInstant::now();

        let mut second = listener.accept().unwrap().0;
        let second_request_accepted = StdInstant::now();
        read_http_headers(&mut second);
        second
            .write_all(http_response("200 OK", body).as_bytes())
            .unwrap();
        (first_response_completed, second_request_accepted)
    });
    (base_url, server)
}

fn read_http_headers(stream: &mut std::net::TcpStream) {
    stream
        .set_read_timeout(Some(StdDuration::from_secs(5)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1_024];
    loop {
        let read = stream.read(&mut buffer).unwrap();
        request.extend_from_slice(&buffer[..read]);
        if read == 0 || request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
}

fn http_response(status: &str, body: &str) -> String {
    http_response_with_headers(status, &[], body)
}

fn http_response_with_headers(status: &str, headers: &[&str], body: &str) -> String {
    let headers = if headers.is_empty() {
        String::new()
    } else {
        format!("{}\r\n", headers.join("\r\n"))
    };
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
