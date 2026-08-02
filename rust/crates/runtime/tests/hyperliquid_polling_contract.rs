use std::{
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    sync::Arc,
    thread,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::{MarketType, Symbol};
use crypto_trading_exchange::{
    BinancePublicExchange, HyperliquidPublicEndpoint, HyperliquidPublicExchange,
};
use crypto_trading_runtime::{
    BinancePollingRoute, BinancePublicPollingSource, HyperliquidPollingRoute,
    HyperliquidPublicPollingSource, MarketDataBook, MarketDataClock, MarketDataError,
    MarketDataEvent, MarketDataEventSource, MarketDataObservation, MarketDataSourceFailure,
    MarketFreshnessPolicy, MarketInstrument, MarketPollingPolicy, MarketSupervisor,
    MarketSupervisorConfig, MarketUniverse,
};
use rust_decimal::Decimal;
use uuid::Uuid;

const FIXTURE: &str =
    include_str!("../../exchange/tests/fixtures/hyperliquid_meta_and_asset_ctxs.json");

#[derive(Debug)]
struct FixedClock {
    now: DateTime<Utc>,
}

impl MarketDataClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

#[test]
fn hyperliquid_polling_routes_are_exact_perpetual_only_and_unambiguous() {
    let clock = Arc::new(FixedClock { now: Utc::now() });
    let policy = polling_policy(StdDuration::from_millis(1));
    let exchange = HyperliquidPublicExchange::new().unwrap();

    let other_exchange = HyperliquidPollingRoute::new(
        instrument("binance", "BTCUSDT", MarketType::Perpetual),
        Symbol::new("BTC").unwrap(),
    )
    .unwrap();
    assert!(matches!(
        HyperliquidPublicPollingSource::new(
            exchange.clone(),
            vec![other_exchange],
            policy,
            Arc::clone(&clock)
        )
        .unwrap_err(),
        MarketDataError::UnsupportedPollingInstrument { .. }
    ));

    let spot = HyperliquidPollingRoute::new(
        instrument("hyperliquid", "BTCUSDT", MarketType::Spot),
        Symbol::new("BTC").unwrap(),
    )
    .unwrap();
    assert!(matches!(
        HyperliquidPublicPollingSource::new(
            exchange.clone(),
            vec![spot],
            policy,
            Arc::clone(&clock)
        )
        .unwrap_err(),
        MarketDataError::UnsupportedPollingInstrument { .. }
    ));

    let duplicate = route("BTCUSDT", "BTC");
    assert!(matches!(
        HyperliquidPublicPollingSource::new(
            exchange.clone(),
            vec![duplicate.clone(), duplicate],
            policy,
            Arc::clone(&clock)
        )
        .unwrap_err(),
        MarketDataError::DuplicatePollingInstrument { .. }
    ));

    assert!(matches!(
        HyperliquidPublicPollingSource::new(
            exchange.clone(),
            vec![route("BTCUSDT", "BTC"), route("BTCUSDC", "BTC")],
            policy,
            Arc::clone(&clock)
        )
        .unwrap_err(),
        MarketDataError::DuplicatePollingWireSymbol { .. }
    ));

    assert!(matches!(
        HyperliquidPublicPollingSource::new(exchange, Vec::new(), policy, clock).unwrap_err(),
        MarketDataError::EmptyUniverse
    ));
}

#[tokio::test]
async fn hyperliquid_polling_recovers_after_failure_and_publishes_funding_sideband() {
    let responses = vec![
        http_response("500 Internal Server Error", "upstream exploded"),
        http_response("200 OK", r#"{"not":"the documented shape"}"#),
        http_response("200 OK", FIXTURE),
        http_response("200 OK", FIXTURE),
    ];
    let (base_url, server) = stub_server(responses);
    let now = Utc::now() + Duration::seconds(1);
    let clock = Arc::new(FixedClock { now });
    let endpoint = HyperliquidPublicEndpoint::loopback(&base_url).unwrap();
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint).unwrap();
    let key = instrument("hyperliquid", "BTCUSDT", MarketType::Perpetual);
    let mut source = HyperliquidPublicPollingSource::new(
        exchange,
        vec![HyperliquidPollingRoute::new(key.clone(), Symbol::new("BTC").unwrap()).unwrap()],
        polling_policy(StdDuration::from_millis(1)),
        clock,
    )
    .unwrap();
    let funding = source.funding_feed();

    let unavailable = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        unavailable,
        MarketDataEvent::SourceUnavailable {
            exchange,
            failure: MarketDataSourceFailure::Disconnected,
            ..
        } if exchange == "hyperliquid"
    ));
    assert_eq!(funding.latest(&key), None);

    let invalid = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        invalid,
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::InvalidPayload,
            ..
        }
    ));
    assert_eq!(funding.latest(&key), None);

    let recovered = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        recovered,
        MarketDataEvent::Observation(MarketDataObservation {
            snapshot,
            revision: 1,
            received_at,
            ..
        }) if snapshot.exchange() == "hyperliquid"
            && snapshot.symbol.as_str() == "BTCUSDT"
            && snapshot.market_type == MarketType::Perpetual
            && received_at == now
    ));
    let sample = funding.latest(&key).unwrap();
    assert_eq!(
        sample.rate.as_decimal(),
        Decimal::from_str("0.0000125").unwrap()
    );
    assert_eq!(sample.revision, 1);
    assert_eq!(sample.observed_at, now);

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation { revision: 2, .. })
    ));
    assert_eq!(funding.latest(&key).unwrap().revision, 2);
    server.join().unwrap();
}

#[tokio::test]
async fn absent_venue_funding_stays_absent_instead_of_being_fabricated() {
    let (base_url, server) = stub_server(vec![http_response("200 OK", FIXTURE)]);
    let clock = Arc::new(FixedClock {
        now: Utc::now() + Duration::seconds(1),
    });
    let endpoint = HyperliquidPublicEndpoint::loopback(&base_url).unwrap();
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint).unwrap();
    let key = instrument("hyperliquid", "THINUSDT", MarketType::Perpetual);
    let mut source = HyperliquidPublicPollingSource::new(
        exchange,
        vec![HyperliquidPollingRoute::new(key.clone(), Symbol::new("THIN").unwrap()).unwrap()],
        polling_policy(StdDuration::from_millis(1)),
        clock,
    )
    .unwrap();
    let funding = source.funding_feed();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation { revision: 1, .. })
    ));
    assert_eq!(funding.latest(&key), None);
    server.join().unwrap();
}

#[tokio::test]
async fn two_real_polling_venues_compose_into_one_ready_exact_pair() {
    let (binance_url, binance_server) = binance_stub_server(vec![http_response(
        "200 OK",
        include_str!("../../exchange/tests/fixtures/binance_book_ticker.json"),
    )]);
    let (hyperliquid_url, hyperliquid_server) = stub_server(vec![http_response("200 OK", FIXTURE)]);
    let now = Utc::now() + Duration::seconds(1);
    let clock = Arc::new(FixedClock { now });
    let left = instrument("binance", "LTC-BTC-SPOT", MarketType::Spot);
    let right = instrument("hyperliquid", "BTCUSDT", MarketType::Perpetual);

    let binance_source = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&binance_url).unwrap(),
        vec![BinancePollingRoute::new(left.clone(), Symbol::new("LTCBTC").unwrap()).unwrap()],
        polling_policy(StdDuration::from_millis(1)),
        Arc::clone(&clock),
    )
    .unwrap();
    let hyperliquid_source = HyperliquidPublicPollingSource::new(
        HyperliquidPublicExchange::with_endpoint(
            &HyperliquidPublicEndpoint::loopback(&hyperliquid_url).unwrap(),
        )
        .unwrap(),
        vec![HyperliquidPollingRoute::new(right.clone(), Symbol::new("BTC").unwrap()).unwrap()],
        polling_policy(StdDuration::from_millis(1)),
        Arc::clone(&clock),
    )
    .unwrap();
    let funding = hyperliquid_source.funding_feed();

    let mut left_supervisor = MarketSupervisor::start(
        Uuid::from_u128(61),
        binance_source,
        MarketSupervisorConfig::new(StdDuration::from_millis(100)).unwrap(),
    )
    .unwrap();
    let mut right_supervisor = MarketSupervisor::start(
        Uuid::from_u128(62),
        hyperliquid_source,
        MarketSupervisorConfig::new(StdDuration::from_millis(100)).unwrap(),
    )
    .unwrap();
    let mut book = MarketDataBook::new(
        MarketUniverse::new(vec![left.clone(), right.clone()]).unwrap(),
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        clock,
    );

    book.apply(left_supervisor.next_event().await.unwrap().unwrap())
        .unwrap();
    book.apply(right_supervisor.next_event().await.unwrap().unwrap())
        .unwrap();

    let pair = book.current_pair(&left, &right).unwrap();
    assert_eq!(pair.left.exchange(), "binance");
    assert_eq!(pair.left.symbol.as_str(), "LTC-BTC-SPOT");
    assert_eq!(pair.right.exchange(), "hyperliquid");
    assert_eq!(pair.right.symbol.as_str(), "BTCUSDT");
    assert_eq!(pair.right.market_type, MarketType::Perpetual);
    assert_eq!(
        funding.latest(&right).unwrap().rate.as_decimal(),
        Decimal::from_str("0.0000125").unwrap()
    );

    left_supervisor.stop().await.unwrap();
    right_supervisor.stop().await.unwrap();
    binance_server.join().unwrap();
    hyperliquid_server.join().unwrap();
}

fn polling_policy(retry: StdDuration) -> MarketPollingPolicy {
    MarketPollingPolicy::new(StdDuration::from_millis(1), retry, retry).unwrap()
}

fn route(canonical_symbol: &str, wire_coin: &str) -> HyperliquidPollingRoute {
    HyperliquidPollingRoute::new(
        instrument("hyperliquid", canonical_symbol, MarketType::Perpetual),
        Symbol::new(wire_coin).unwrap(),
    )
    .unwrap()
}

fn instrument(exchange: &str, symbol: &str, market_type: MarketType) -> MarketInstrument {
    MarketInstrument::new(exchange, Symbol::new(symbol).unwrap(), market_type).unwrap()
}

fn stub_server(responses: Vec<String>) -> (String, thread::JoinHandle<()>) {
    serve(responses, |request| {
        assert!(request.starts_with("POST /info HTTP/1.1\r\n"), "{request}");
        assert!(
            request.ends_with(r#"{"type":"metaAndAssetCtxs"}"#),
            "{request}"
        );
    })
}

fn binance_stub_server(responses: Vec<String>) -> (String, thread::JoinHandle<()>) {
    serve(responses, |request| {
        assert!(
            request.starts_with("GET /api/v3/ticker/bookTicker?symbol=LTCBTC HTTP/1.1\r\n"),
            "{request}"
        );
    })
}

fn serve<F>(responses: Vec<String>, verify: F) -> (String, thread::JoinHandle<()>)
where
    F: Fn(&str) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2_048];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request);
                if text.starts_with("GET ") && text.contains("\r\n\r\n") {
                    break;
                }
                if text.ends_with(r#"{"type":"metaAndAssetCtxs"}"#) {
                    break;
                }
            }
            verify(&String::from_utf8(request).unwrap());
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
