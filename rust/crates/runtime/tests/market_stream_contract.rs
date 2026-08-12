use std::{
    collections::VecDeque,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Symbol};
use crypto_trading_exchange::{
    BinancePublicExchange, BinanceSpotMarketStreamEndpoint, ExchangeError,
};
use crypto_trading_runtime::{
    BinanceBookTickerStreamSource, BinancePollingRoute, FixedMarketStreamJitter, MarketDataClock,
    MarketDataEvent, MarketDataEventSource, MarketDataObservation, MarketDataSourceFailure,
    MarketStreamReconnectPolicy, MarketStreamSleeper, MarketSupervisor, MarketSupervisorConfig,
    MarketSupervisorExit, TextWebSocketConnector, TextWebSocketEvent, TextWebSocketSession,
    TokioTextWebSocketConnector, WebSocketCloseKind,
};

#[derive(Clone)]
struct FixedClock {
    now: DateTime<Utc>,
}

impl fmt::Debug for FixedClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("FixedClock").finish_non_exhaustive()
    }
}

impl MarketDataClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

#[derive(Debug, Default)]
struct RecordingSleeper {
    durations: Mutex<Vec<StdDuration>>,
}

#[async_trait]
impl MarketStreamSleeper for RecordingSleeper {
    async fn sleep(&self, duration: StdDuration) {
        self.durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(duration);
        tokio::task::yield_now().await;
    }
}

#[derive(Debug)]
struct ScriptedSession {
    events: VecDeque<Result<TextWebSocketEvent, ExchangeError>>,
}

#[async_trait]
impl TextWebSocketSession for ScriptedSession {
    async fn next_event(&mut self) -> Result<TextWebSocketEvent, ExchangeError> {
        self.events.pop_front().unwrap_or_else(|| {
            Ok(TextWebSocketEvent::Closed {
                kind: WebSocketCloseKind::Remote,
            })
        })
    }
}

#[derive(Debug)]
struct ScriptedConnector {
    connects: Mutex<VecDeque<Result<ScriptedSession, ExchangeError>>>,
}

impl ScriptedConnector {
    fn new(connects: Vec<Result<ScriptedSession, ExchangeError>>) -> Self {
        Self {
            connects: Mutex::new(connects.into()),
        }
    }
}

#[async_trait]
impl TextWebSocketConnector for ScriptedConnector {
    async fn connect(&self) -> Result<Box<dyn TextWebSocketSession>, ExchangeError> {
        let next = self
            .connects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or_else(|| Err(ExchangeError::unavailable("no scripted connection")));
        next.map(|session| Box::new(session) as Box<dyn TextWebSocketSession>)
    }
}

#[tokio::test]
async fn stream_source_reconnects_fail_closed_and_marks_the_transport_gap() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let connector = Arc::new(ScriptedConnector::new(vec![
        Ok(ScriptedSession {
            events: vec![
                Ok(TextWebSocketEvent::Text(
                    r#"{"u":7,"s":"BNBUSDT","b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#.to_owned(),
                )),
                Ok(TextWebSocketEvent::Closed {
                    kind: WebSocketCloseKind::Remote,
                }),
            ]
            .into(),
        }),
        Ok(ScriptedSession {
            events: vec![Ok(TextWebSocketEvent::Text(
                r#"{"u":11,"s":"BNBUSDT","b":"1.2","B":"2.2","a":"1.3","A":"3.3"}"#.to_owned(),
            ))]
            .into(),
        }),
    ]));
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        connector,
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        sleeper,
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    let first = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        first,
        MarketDataEvent::Observation(MarketDataObservation {
            revision: 1,
            source_sequence: Some(7),
            source_generation: Some(1),
            ..
        })
    ));

    let disconnected = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        disconnected,
        MarketDataEvent::SourceUnavailable {
            exchange,
            failure: MarketDataSourceFailure::Disconnected,
            ..
        } if exchange == "binance"
    ));

    let gap = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        gap,
        MarketDataEvent::SourceGap {
            exchange,
            skipped,
            ..
        } if exchange == "binance" && skipped == 1
    ));

    let resumed = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        resumed,
        MarketDataEvent::Observation(MarketDataObservation {
            revision: 2,
            source_sequence: Some(11),
            source_generation: Some(2),
            ..
        })
    ));
}

#[tokio::test]
async fn stream_source_converts_transport_lag_into_an_explicit_gap() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![Ok(ScriptedSession {
            events: vec![Ok(TextWebSocketEvent::Lagged { skipped: 4 })].into(),
        })])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(RecordingSleeper::default()),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    let gap = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        gap,
        MarketDataEvent::SourceGap {
            exchange,
            skipped,
            observed_at,
        } if exchange == "binance" && skipped == 4 && observed_at == clock.now()
    ));
}

#[tokio::test]
async fn stream_source_applies_the_jittered_backoff_before_retrying() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![
            Err(ExchangeError::unavailable("first dial failed")),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"u":12,"s":"BNBUSDT","b":"1.2","B":"2.2","a":"1.3","A":"3.3"}"#.to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(200), StdDuration::from_secs(1))
            .unwrap(),
        clock,
        sleeper.clone(),
        Arc::new(FixedMarketStreamJitter::new(12_500).unwrap()),
    )
    .unwrap();

    let unavailable = source.next_event().await.unwrap().unwrap();
    assert!(matches!(
        unavailable,
        MarketDataEvent::SourceUnavailable { .. }
    ));

    let _ = source.next_event().await.unwrap().unwrap();
    let recorded = sleeper
        .durations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(recorded, vec![StdDuration::from_millis(250)]);
}

#[test]
fn book_ticker_connector_rejects_multiple_routes_on_raw_ws_endpoint() {
    let error = TokioTextWebSocketConnector::for_binance_book_ticker(
        BinanceSpotMarketStreamEndpoint::official(),
        &[route("BNB-USDT", "BNBUSDT"), route("BTC-USDT", "BTCUSDT")],
        NonZeroUsize::new(8).unwrap(),
        StdDuration::from_secs(1),
    )
    .unwrap_err();

    assert!(error.to_string().contains("exactly one route"));
}

#[tokio::test]
async fn stream_source_keeps_exponential_backoff_when_connections_close_before_data() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Closed {
                    kind: WebSocketCloseKind::Remote,
                })]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Closed {
                    kind: WebSocketCloseKind::Remote,
                })]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"u":12,"s":"BNBUSDT","b":"1.2","B":"2.2","a":"1.3","A":"3.3"}"#.to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap()
            .with_max_reconnect_attempts(3),
        Arc::clone(&clock),
        sleeper.clone(),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable { .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap { skipped: 1, .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable { .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap { skipped: 1, .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation { revision: 1, .. })
    ));

    let recorded = sleeper
        .durations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(
        recorded,
        vec![StdDuration::from_millis(50), StdDuration::from_millis(100)]
    );
}

#[tokio::test]
async fn stream_source_surfaces_server_shutdown_as_fail_closed_gap_and_unavailable() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![Ok(ScriptedSession {
            events: vec![Ok(TextWebSocketEvent::Closed {
                kind: WebSocketCloseKind::ServerShutdown,
            })]
            .into(),
        })])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(RecordingSleeper::default()),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::Disconnected,
            ..
        }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap { skipped: 1, .. }
    ));
}

#[tokio::test]
async fn stream_source_rejects_frames_without_source_sequence() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![
                    Ok(TextWebSocketEvent::Text(
                        r#"{"u":7,"s":"BNBUSDT","b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#
                            .to_owned(),
                    )),
                    Ok(TextWebSocketEvent::Text(
                        r#"{"s":"BNBUSDT","b":"1.2","B":"2.2","a":"1.3","A":"3.3"}"#.to_owned(),
                    )),
                ]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"u":8,"s":"BNBUSDT","b":"1.4","B":"2.4","a":"1.5","A":"3.5"}"#.to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        sleeper.clone(),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation {
            source_sequence: Some(7),
            source_generation: Some(1),
            ..
        })
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::InvalidPayload,
            ..
        }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap { skipped: 1, .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation {
            revision: 2,
            source_sequence: Some(8),
            source_generation: Some(2),
            ..
        })
    ));
    assert_eq!(
        sleeper
            .durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![StdDuration::from_millis(50)]
    );
}

#[tokio::test]
async fn stream_source_malformed_frame_reconnects_instead_of_failing_the_supervisor_contract() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text("{not-json".to_owned()))].into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"u":8,"s":"BNBUSDT","b":"1.4","B":"2.4","a":"1.5","A":"3.5"}"#.to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(RecordingSleeper::default()),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();
    let mut supervisor =
        MarketSupervisor::start_new(source, MarketSupervisorConfig::default()).unwrap();

    assert!(matches!(
        supervisor.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::InvalidPayload,
            ..
        }
    ));
    assert_eq!(
        supervisor.status().phase,
        crypto_trading_runtime::MarketSupervisorPhase::Running
    );
    assert_eq!(
        supervisor.stop().await.unwrap(),
        MarketSupervisorExit::StopRequested
    );
}

#[tokio::test]
async fn stream_source_unknown_symbol_uses_counted_backoff_instead_of_busy_reconnect_loop() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"u":9,"s":"BTCUSDT","b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#.to_owned(),
                ))]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"u":10,"s":"BNBUSDT","b":"1.2","B":"2.2","a":"1.3","A":"3.3"}"#.to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        sleeper.clone(),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::InvalidPayload,
            ..
        }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap { skipped: 1, .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation {
            source_generation: Some(2),
            ..
        })
    ));
    assert_eq!(
        sleeper
            .durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![StdDuration::from_millis(50)]
    );
}

#[tokio::test]
async fn stream_source_sequence_regression_uses_failure_budget_and_resets_on_new_connection() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![
                    Ok(TextWebSocketEvent::Text(
                        r#"{"u":7,"s":"BNBUSDT","b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#
                            .to_owned(),
                    )),
                    Ok(TextWebSocketEvent::Text(
                        r#"{"u":6,"s":"BNBUSDT","b":"1.2","B":"2.2","a":"1.3","A":"3.3"}"#
                            .to_owned(),
                    )),
                ]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![
                    Ok(TextWebSocketEvent::Text(
                        r#"{"u":6,"s":"BNBUSDT","b":"1.4","B":"2.4","a":"1.5","A":"3.5"}"#
                            .to_owned(),
                    )),
                    Ok(TextWebSocketEvent::Text(
                        r#"{"u":9,"s":"BNBUSDT","b":"1.6","B":"2.6","a":"1.7","A":"3.7"}"#
                            .to_owned(),
                    )),
                ]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap()
            .with_max_reconnect_attempts(3),
        Arc::clone(&clock),
        sleeper.clone(),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();

    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation {
            source_sequence: Some(7),
            source_generation: Some(1),
            ..
        })
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::InvalidPayload,
            ..
        }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceGap { skipped: 1, .. }
    ));
    assert!(matches!(
        source.next_event().await.unwrap().unwrap(),
        MarketDataEvent::Observation(MarketDataObservation {
            revision: 2,
            source_sequence: Some(6),
            source_generation: Some(2),
            ..
        })
    ));
    assert_eq!(
        sleeper
            .durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![StdDuration::from_millis(50)]
    );
}

#[tokio::test]
async fn supervisor_distinguishes_reconnect_exhaustion_from_normal_source_end() {
    let clock = Arc::new(FixedClock {
        now: timestamp("2026-08-12T00:00:00Z"),
    });
    let source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::new().unwrap(),
        vec![route("BNB-USDT", "BNBUSDT")],
        Arc::new(ScriptedConnector::new(vec![Ok(ScriptedSession {
            events: vec![Ok(TextWebSocketEvent::Closed {
                kind: WebSocketCloseKind::Remote,
            })]
            .into(),
        })])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap()
            .with_max_reconnect_attempts(1),
        Arc::clone(&clock),
        Arc::new(RecordingSleeper::default()),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    )
    .unwrap();
    let mut supervisor =
        MarketSupervisor::start_new(source, MarketSupervisorConfig::default()).unwrap();

    assert!(matches!(
        supervisor.next_event().await.unwrap().unwrap(),
        MarketDataEvent::SourceUnavailable {
            failure: MarketDataSourceFailure::Disconnected,
            ..
        }
    ));
    assert!(supervisor.next_event().await.unwrap().is_none());
    assert_eq!(
        supervisor.status().exit,
        Some(MarketSupervisorExit::ReconnectExhausted)
    );
}

#[test]
fn fixed_market_stream_jitter_rejects_zero_bps() {
    assert!(FixedMarketStreamJitter::new(0).is_err());
}

fn route(canonical_symbol: &str, wire_symbol: &str) -> BinancePollingRoute {
    BinancePollingRoute::new(
        crypto_trading_runtime::MarketInstrument::new(
            "binance",
            Symbol::new(canonical_symbol).unwrap(),
            MarketType::Spot,
        )
        .unwrap(),
        Symbol::new(wire_symbol).unwrap(),
    )
    .unwrap()
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("timestamp must be valid")
        .with_timezone(&Utc)
}
