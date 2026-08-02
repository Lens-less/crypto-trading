use std::{
    collections::VecDeque,
    future,
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    sync::Arc,
    thread,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_exchange::BinancePublicExchange;
use crypto_trading_runtime::{
    BinancePollingRoute, BinancePublicPollingSource, MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
    MarketContinuity, MarketDataBook, MarketDataClock, MarketDataError, MarketDataEvent,
    MarketDataEventFuture, MarketDataEventSource, MarketDataFreshness, MarketDataObservation,
    MarketDataSourceFailure, MarketFreshnessPolicy, MarketInstrument, MarketPollingPolicy,
    MarketSupervisor, MarketSupervisorConfig, MarketSupervisorExit, MarketSupervisorHealth,
    MarketSupervisorPhase, MarketUniverse,
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
async fn supervisor_slow_consumer_gets_gap_then_latest_event_with_constant_memory() {
    let base = timestamp(0);
    let source = ScriptedSource::new(
        "binance",
        (1..=3)
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
            if supervisor.status().event_sequence == 3 {
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
            skipped: 2,
            observed_at,
        } if exchange == "binance" && observed_at == base
    ));
    assert!(matches!(
        supervisor.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation { revision: 3, .. })
    ));
    assert!(supervisor.next_event().await.unwrap().is_none());

    let status = supervisor.status();
    assert_eq!(
        status.schema_version,
        MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION
    );
    assert_eq!(status.task_id, Uuid::from_u128(41));
    assert_eq!(status.phase, MarketSupervisorPhase::Stopped);
    assert_eq!(status.health, MarketSupervisorHealth::Healthy);
    assert_eq!(status.exit, Some(MarketSupervisorExit::SourceEnded));
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
        http_response(
            "429 Too Many Requests",
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
            failure: MarketDataSourceFailure::Disconnected
        }
    );
    assert_eq!(degraded_row.freshness, MarketDataFreshness::Missing);

    book.apply(supervisor.next_event().await.unwrap().unwrap())
        .unwrap();
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

fn http_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
