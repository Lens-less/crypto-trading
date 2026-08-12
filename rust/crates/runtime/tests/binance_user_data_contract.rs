#![allow(clippy::unreadable_literal)]

use std::{
    collections::VecDeque,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use crypto_trading_domain::{MarketType, Money, Price, Quantity, Symbol};
use crypto_trading_exchange::{
    BinanceHmacSha256Signer, BinanceSpotUserDataStreamEndpoint, BinanceTestnetEndpoints,
    BinanceTestnetProtocol, BinanceUserDataEvent, ExchangeError, ExchangeSymbol,
    ExchangeSymbolCatalog, InstrumentRuleCatalog, InstrumentRules,
};
use crypto_trading_runtime::{
    BinanceUserDataApply, BinanceUserDataReconcileReason, BinanceUserDataState,
    BinanceUserDataStreamItem, BinanceUserDataStreamSource, FixedMarketStreamJitter,
    MarketDataClock, MarketStreamReconnectPolicy, MarketStreamSleeper, StreamEnvelope,
    TextWebSocketConnector, TextWebSocketEvent, TextWebSocketSession, TokioTextWebSocketConnector,
    WebSocketCloseKind,
};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

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
struct NoopSleeper;

#[async_trait]
impl MarketStreamSleeper for NoopSleeper {
    async fn sleep(&self, _duration: StdDuration) {}
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
async fn user_data_stream_source_surfaces_subscribed_heartbeat_and_expiry() {
    let clock = Arc::new(FixedClock {
        now: timestamp(1_723_422_226_000),
    });
    let mut source = BinanceUserDataStreamSource::new(
        Arc::new(ScriptedConnector::new(vec![Ok(ScriptedSession {
            events: vec![
                Ok(TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":7}}"#
                        .to_owned(),
                )),
                Ok(TextWebSocketEvent::Heartbeat),
                Ok(TextWebSocketEvent::Text(
                    r#"{"subscriptionId":7,"event":{"e":"eventStreamTerminated","E":1723422226000}}"#
                        .to_owned(),
                )),
            ]
            .into(),
        })])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(NoopSleeper),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    );

    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed {
            subscription_id: 7,
            observed_at,
        } if observed_at == clock.now()
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Heartbeat { observed_at } if observed_at == clock.now()
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::StreamExpired { observed_at } if observed_at == clock.now()
    ));
}

#[tokio::test]
async fn user_data_stream_source_requires_the_expected_subscription_ack_id() {
    let clock = Arc::new(FixedClock {
        now: timestamp(1_723_422_226_000),
    });
    let mut source = BinanceUserDataStreamSource::new(
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"id":"wrong-id","status":200,"result":{"subscriptionId":7}}"#.to_owned(),
                ))]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":9}}"#
                        .to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(NoopSleeper),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    );

    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::SourceUnavailable {
            failure: crypto_trading_runtime::MarketDataSourceFailure::InvalidPayload,
            observed_at,
        } if observed_at == clock.now()
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::TransportGap {
            skipped: 1,
            observed_at,
        } if observed_at == clock.now()
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed {
            subscription_id: 9,
            observed_at,
        } if observed_at == clock.now()
    ));
}

#[tokio::test]
async fn production_user_data_connector_sends_signed_subscription_init_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let expected_init = Arc::new(Mutex::new(None::<String>));
    let observed_init = Arc::clone(&expected_init);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        let Message::Text(text) = message else {
            panic!("expected initial text subscription");
        };
        *observed_init
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(text.to_string());
        websocket
            .send(Message::Text(
                r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":7}}"#.into(),
            ))
            .await
            .unwrap();
        websocket.send(Message::Close(None)).await.unwrap();
    });

    let connector =
        TokioTextWebSocketConnector::for_binance_user_data_stream_with_timestamp_provider(
            BinanceSpotUserDataStreamEndpoint::loopback(&format!("ws://{address}")).unwrap(),
            Arc::new(test_protocol()),
            Some(5_000),
            NonZeroUsize::new(8).unwrap(),
            StdDuration::from_secs(30),
            Arc::new(|| 1_723_422_222_000),
        )
        .unwrap();
    let mut session = connector.connect().await.unwrap();
    assert!(matches!(
        session.next_event().await.unwrap(),
        TextWebSocketEvent::Text(_)
    ));
    server.await.unwrap();

    let init = expected_init
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .unwrap();
    assert_eq!(
        init,
        r#"{"id":"user-data-subscribe","method":"userDataStream.subscribe.signature","params":{"apiKey":"test-key","recvWindow":5000,"signature":"367a8cdda33212532850d80c9aa734aa8ca2a10698baf569412ba9a0390121dd","timestamp":1723422222000}}"#
    );
}

#[tokio::test]
async fn user_data_stream_source_reconnects_after_lagged_queue_loss_and_resubscribes() {
    let clock = Arc::new(FixedClock {
        now: timestamp(1_723_422_226_000),
    });
    let mut source = BinanceUserDataStreamSource::new(
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Lagged { skipped: 3 })].into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":7}}"#
                        .to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(NoopSleeper),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    );

    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::SourceUnavailable { .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::TransportGap { skipped: 3, .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed {
            subscription_id: 7,
            ..
        }
    ));
}

#[tokio::test]
async fn user_data_stream_heartbeat_does_not_reset_the_reconnect_budget() {
    let clock = Arc::new(FixedClock {
        now: timestamp(1_723_422_226_000),
    });
    let sleeper = Arc::new(RecordingSleeper::default());
    let mut source = BinanceUserDataStreamSource::new(
        Arc::new(ScriptedConnector::new(vec![
            Err(ExchangeError::unavailable("first dial failed")),
            Ok(ScriptedSession {
                events: vec![
                    Ok(TextWebSocketEvent::Heartbeat),
                    Ok(TextWebSocketEvent::Closed {
                        kind: WebSocketCloseKind::Remote,
                    }),
                ]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":7}}"#
                        .to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        sleeper.clone(),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    );

    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::SourceUnavailable { .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Heartbeat { .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::SourceUnavailable { .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::TransportGap { skipped: 1, .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed { .. }
    ));
    assert_eq!(
        sleeper
            .durations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![StdDuration::from_millis(50), StdDuration::from_millis(100)]
    );
}

#[tokio::test]
async fn stream_terminated_events_retry_but_resubscribe_resets_the_budget() {
    let clock = Arc::new(FixedClock {
        now: timestamp(1_723_422_226_000),
    });
    let mut source = BinanceUserDataStreamSource::new(
        Arc::new(ScriptedConnector::new(vec![
            Ok(ScriptedSession {
                events: vec![
                    Ok(TextWebSocketEvent::Text(
                        r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":7}}"#
                            .to_owned(),
                    )),
                    Ok(TextWebSocketEvent::Text(
                        r#"{"subscriptionId":7,"event":{"e":"eventStreamTerminated","E":1723422226000}}"#
                            .to_owned(),
                    )),
                ]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![
                    Ok(TextWebSocketEvent::Text(
                        r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":8}}"#
                            .to_owned(),
                    )),
                    Ok(TextWebSocketEvent::Text(
                        r#"{"subscriptionId":8,"event":{"e":"eventStreamTerminated","E":1723422227000}}"#
                            .to_owned(),
                    )),
                ]
                .into(),
            }),
            Ok(ScriptedSession {
                events: vec![Ok(TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":9}}"#
                        .to_owned(),
                ))]
                .into(),
            }),
        ])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap()
            .with_max_reconnect_attempts(2),
        Arc::clone(&clock),
        Arc::new(NoopSleeper),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    );

    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed {
            subscription_id: 7,
            ..
        }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::StreamExpired { .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed {
            subscription_id: 8,
            ..
        }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::StreamExpired { .. }
    ));
    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed {
            subscription_id: 9,
            ..
        }
    ));
}

#[test]
fn user_data_state_deduplicates_reports_and_accumulates_fills_monotonically() {
    let mut state = BinanceUserDataState::default();

    let first = envelope(
        1,
        1,
        execution_report(
            1723422222000,
            1723422221999,
            4_293_153,
            8_641_984,
            "0.10000000",
        ),
    );
    assert_eq!(
        state.apply(first.clone()),
        BinanceUserDataApply::AppliedExecution
    );
    assert_eq!(
        state
            .order(4_293_153)
            .unwrap()
            .cumulative_filled_quantity
            .to_string(),
        "0.10000000"
    );

    let duplicate = envelope(
        1,
        2,
        execution_report(
            1723422222000,
            1723422221999,
            4_293_153,
            8_641_984,
            "0.10000000",
        ),
    );
    assert_eq!(state.apply(duplicate), BinanceUserDataApply::Duplicate);

    let next = envelope(
        1,
        3,
        execution_report(
            1723422223000,
            1723422222999,
            4_293_153,
            8_641_985,
            "0.40000000",
        ),
    );
    assert_eq!(state.apply(next), BinanceUserDataApply::AppliedExecution);
    assert_eq!(
        state
            .order(4_293_153)
            .unwrap()
            .cumulative_filled_quantity
            .to_string(),
        "0.40000000"
    );
}

#[tokio::test]
async fn user_data_stream_uses_trade_id_and_cumulative_fill_to_avoid_fail_open_dedup() {
    let clock = Arc::new(FixedClock {
        now: timestamp(1_723_422_226_000),
    });
    let mut source = BinanceUserDataStreamSource::new(
        Arc::new(ScriptedConnector::new(vec![Ok(ScriptedSession {
            events: vec![
                Ok(TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":7}}"#
                        .to_owned(),
                )),
                Ok(TextWebSocketEvent::Text(raw_execution_report(
                    1723422222000,
                    1723422221999,
                    4_293_153,
                    Some(991),
                    Some(8_641_984),
                    "0.10000000",
                ))),
                Ok(TextWebSocketEvent::Text(raw_execution_report(
                    1723422222000,
                    1723422221999,
                    4_293_153,
                    Some(992),
                    Some(8_641_984),
                    "0.40000000",
                ))),
            ]
            .into(),
        })])),
        MarketStreamReconnectPolicy::new(StdDuration::from_millis(50), StdDuration::from_secs(1))
            .unwrap(),
        Arc::clone(&clock),
        Arc::new(NoopSleeper),
        Arc::new(FixedMarketStreamJitter::new(10_000).unwrap()),
    );
    let mut state = BinanceUserDataState::default();

    assert!(matches!(
        source.next_item().await.unwrap().unwrap(),
        BinanceUserDataStreamItem::Subscribed { .. }
    ));
    let BinanceUserDataStreamItem::Event(first) = source.next_item().await.unwrap().unwrap() else {
        panic!("expected first execution event");
    };
    let BinanceUserDataStreamItem::Event(second) = source.next_item().await.unwrap().unwrap()
    else {
        panic!("expected second execution event");
    };

    assert_eq!(state.apply(first), BinanceUserDataApply::AppliedExecution);
    assert_eq!(state.apply(second), BinanceUserDataApply::AppliedExecution);
    assert_eq!(
        state
            .order(4_293_153)
            .unwrap()
            .cumulative_filled_quantity
            .to_string(),
        "0.40000000"
    );
}

#[test]
fn user_data_state_flags_same_trade_fingerprint_with_different_cumulative_fill_as_regression() {
    let mut state = BinanceUserDataState::default();
    assert_eq!(
        state.apply(envelope(
            1,
            1,
            execution_report_with_trade_id(
                1723422222000,
                1723422221999,
                4_293_153,
                Some(991),
                Some(8_641_984),
                "0.10000000",
            ),
        )),
        BinanceUserDataApply::AppliedExecution
    );
    assert_eq!(
        state.apply(envelope(
            1,
            2,
            execution_report_with_trade_id(
                1723422222000,
                1723422221999,
                4_293_153,
                Some(991),
                Some(8_641_984),
                "0.40000000",
            ),
        )),
        BinanceUserDataApply::ReconcileRequired(
            BinanceUserDataReconcileReason::ExecutionRegression
        )
    );
}

#[test]
fn user_data_state_fails_closed_on_fill_regressions_and_stream_restarts() {
    let mut state = BinanceUserDataState::default();
    assert_eq!(
        state.apply(envelope(
            1,
            1,
            execution_report(
                1723422222000,
                1723422221999,
                4_293_153,
                8_641_984,
                "0.50000000",
            ),
        )),
        BinanceUserDataApply::AppliedExecution
    );

    let regression = state.apply(envelope(
        1,
        2,
        execution_report(
            1723422223000,
            1723422222999,
            4_293_153,
            8_641_985,
            "0.40000000",
        ),
    ));
    assert_eq!(
        regression,
        BinanceUserDataApply::ReconcileRequired(
            BinanceUserDataReconcileReason::ExecutionRegression
        )
    );

    let mut restarted = BinanceUserDataState::default();
    assert_eq!(
        restarted.apply(envelope(
            3,
            1,
            execution_report(
                1723422222000,
                1723422221999,
                4_293_153,
                8_641_984,
                "0.10000000",
            ),
        )),
        BinanceUserDataApply::AppliedExecution
    );
    assert_eq!(
        restarted.apply(envelope(4, 1, account_update(1723422224000, 1723422223999),)),
        BinanceUserDataApply::ReconcileRequired(BinanceUserDataReconcileReason::ConnectionRestart)
    );
}

#[test]
fn user_data_state_rejects_event_time_and_account_update_regressions() {
    let mut state = BinanceUserDataState::default();
    assert_eq!(
        state.apply(envelope(1, 1, account_update(1723422224000, 1723422223999))),
        BinanceUserDataApply::AppliedAccountUpdate
    );
    assert_eq!(
        state.balance("BTC").unwrap().locked.to_string(),
        "12.500000"
    );

    let duplicate = state.apply(envelope(1, 2, account_update(1723422224000, 1723422223999)));
    assert_eq!(duplicate, BinanceUserDataApply::Duplicate);

    let older_event_time = state.apply(envelope(
        1,
        3,
        execution_report(
            1723422223000,
            1723422222999,
            4_293_153,
            8_641_984,
            "0.10000000",
        ),
    ));
    assert_eq!(
        older_event_time,
        BinanceUserDataApply::ReconcileRequired(
            BinanceUserDataReconcileReason::EventTimeRegression
        )
    );
}

#[test]
fn user_data_state_requires_reconciliation_after_transport_gaps_and_expiry() {
    let mut gap = BinanceUserDataState::default();
    assert_eq!(
        gap.note_transport_gap(2, timestamp(1723422225000)),
        BinanceUserDataApply::ReconcileRequired(BinanceUserDataReconcileReason::TransportGap)
    );

    let mut expired = BinanceUserDataState::default();
    assert_eq!(
        expired.note_stream_expired(timestamp(1723422225000)),
        BinanceUserDataApply::ReconcileRequired(BinanceUserDataReconcileReason::StreamExpired)
    );
}

fn execution_report(
    event_time_ms: i64,
    transaction_time_ms: i64,
    order_id: u64,
    execution_id: u64,
    cumulative_filled_quantity: &str,
) -> BinanceUserDataEvent {
    execution_report_with_trade_id(
        event_time_ms,
        transaction_time_ms,
        order_id,
        Some(execution_id),
        Some(execution_id),
        cumulative_filled_quantity,
    )
}

fn execution_report_with_trade_id(
    event_time_ms: i64,
    transaction_time_ms: i64,
    order_id: u64,
    trade_id: Option<u64>,
    execution_id: Option<u64>,
    cumulative_filled_quantity: &str,
) -> BinanceUserDataEvent {
    BinanceTestnetProtocol::parse_user_data_event(
        raw_execution_report(
            event_time_ms,
            transaction_time_ms,
            order_id,
            trade_id,
            execution_id,
            cumulative_filled_quantity,
        )
        .as_bytes(),
    )
    .unwrap()
}

fn raw_execution_report(
    event_time_ms: i64,
    transaction_time_ms: i64,
    order_id: u64,
    trade_id: Option<u64>,
    execution_id: Option<u64>,
    cumulative_filled_quantity: &str,
) -> String {
    let trade_id_field = trade_id.map_or(String::new(), |value| format!(r#","t":{value}"#));
    let execution_id_field = execution_id.map_or(String::new(), |value| format!(r#","I":{value}"#));
    format!(
        r#"{{
                "subscriptionId":0,
                "event":{{
                    "e":"executionReport",
                    "E":{event_time_ms},
                    "s":"ETHBTC",
                    "c":"order-7",
                    "S":"BUY",
                    "o":"LIMIT",
                    "f":"GTC",
                    "q":"1.00000000",
                    "p":"0.10264410",
                    "x":"TRADE",
                    "X":"PARTIALLY_FILLED",
                    "i":{order_id},
                    "l":"0.10000000",
                    "z":"{cumulative_filled_quantity}",
                    "L":"0.10264410",
                    "T":{transaction_time_ms}{trade_id_field}{execution_id_field}
                }}
            }}"#
    )
}

fn account_update(event_time_ms: i64, account_update_time_ms: i64) -> BinanceUserDataEvent {
    BinanceTestnetProtocol::parse_user_data_event(
        format!(
            r#"{{
                "subscriptionId":0,
                "event":{{
                    "e":"outboundAccountPosition",
                    "E":{event_time_ms},
                    "u":{account_update_time_ms},
                    "B":[
                        {{"a":"ETH","f":"10000.000000","l":"0.000000"}},
                        {{"a":"BTC","f":"0.000000","l":"12.500000"}}
                    ]
                }}
            }}"#
        )
        .as_bytes(),
    )
    .unwrap()
}

fn envelope(
    connection_generation: u64,
    local_sequence: u64,
    payload: BinanceUserDataEvent,
) -> StreamEnvelope<BinanceUserDataEvent> {
    StreamEnvelope::new(
        connection_generation,
        local_sequence,
        timestamp(1723422226000),
        payload,
    )
    .unwrap()
}

fn timestamp(value: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(value).single().unwrap()
}

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn price(value: &str) -> Price {
    value.parse().unwrap()
}

fn quantity(value: &str) -> Quantity {
    value.parse().unwrap()
}

fn test_protocol() -> BinanceTestnetProtocol {
    let spot = Symbol::new("BTC-USDC-SPOT").unwrap();
    let symbols = ExchangeSymbolCatalog::new(vec![
        ExchangeSymbol::new("binance", spot.clone(), MarketType::Spot, "BTCUSDT").unwrap(),
    ])
    .unwrap();
    let rules = InstrumentRuleCatalog::new(vec![
        InstrumentRules::new(
            "binance",
            spot,
            MarketType::Spot,
            price("0.10"),
            quantity("0.0001"),
            quantity("0.0001"),
            Money::new(decimal("5")),
        )
        .unwrap(),
    ])
    .unwrap();

    BinanceTestnetProtocol::authenticated(
        BinanceTestnetEndpoints::official(),
        symbols,
        rules,
        Arc::new(BinanceHmacSha256Signer::new("test-key", "test-secret").unwrap()),
    )
    .unwrap()
}
