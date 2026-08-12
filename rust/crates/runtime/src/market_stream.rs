use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
    time::SystemTime,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_exchange::{
    BinancePublicExchange, BinanceSpotMarketStreamEndpoint, BinanceSpotUserDataStreamEndpoint,
    BinanceTestnetProtocol, ExchangeError,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    market_data::{
        MarketDataClock, MarketDataError, MarketDataEvent, MarketDataObservation,
        MarketDataSourceFailure, MarketTimestampProvenance, classify_exchange_failure,
    },
    market_polling::BinancePollingRoute,
    market_supervisor::{MarketDataEventFuture, MarketDataEventSource},
};

const BINANCE_EXCHANGE: &str = "binance";
const MAX_STREAM_TARGETS: usize = 1_024;
type InitMessageFactory = Arc<dyn Fn() -> Result<Vec<String>, ExchangeError> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSocketCloseKind {
    Remote,
    ServerShutdown,
    Expired,
    Protocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextWebSocketEvent {
    Text(String),
    Heartbeat,
    Lagged { skipped: u64 },
    Closed { kind: WebSocketCloseKind },
}

#[async_trait]
pub trait TextWebSocketSession: Debug + Send {
    async fn next_event(&mut self) -> Result<TextWebSocketEvent, ExchangeError>;
}

#[async_trait]
pub trait TextWebSocketConnector: Debug + Send + Sync {
    async fn connect(&self) -> Result<Box<dyn TextWebSocketSession>, ExchangeError>;
}

#[async_trait]
pub trait MarketStreamSleeper: Debug + Send + Sync {
    async fn sleep(&self, duration: Duration);
}

#[derive(Debug, Default)]
pub struct TokioMarketStreamSleeper;

#[async_trait]
impl MarketStreamSleeper for TokioMarketStreamSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

pub trait MarketStreamJitter: Debug + Send + Sync {
    fn multiplier_bps(&self) -> u16;
}

#[derive(Debug, Clone, Copy)]
pub struct FixedMarketStreamJitter {
    multiplier_bps: u16,
}

impl FixedMarketStreamJitter {
    pub fn new(multiplier_bps: u16) -> Self {
        Self { multiplier_bps }
    }
}

impl MarketStreamJitter for FixedMarketStreamJitter {
    fn multiplier_bps(&self) -> u16 {
        self.multiplier_bps
    }
}

#[derive(Debug)]
pub struct ProductionMarketStreamJitter {
    state: AtomicU64,
    min_bps: u16,
    max_bps: u16,
}

impl ProductionMarketStreamJitter {
    /// Builds a production jitter source bounded in basis points.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when the bounds are zero or
    /// `min_bps` exceeds `max_bps`.
    pub fn new(min_bps: u16, max_bps: u16) -> Result<Self, ExchangeError> {
        if min_bps == 0 || min_bps > max_bps {
            return Err(ExchangeError::invalid(
                "market-stream jitter bounds must satisfy 1..=max",
            ));
        }
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0_u64, |duration| {
                u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
            })
            ^ 0x9e37_79b9_7f4a_7c15;
        Ok(Self {
            state: AtomicU64::new(seed.max(1)),
            min_bps,
            max_bps,
        })
    }

    fn next_u64(&self) -> u64 {
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            let mut next = current;
            next ^= next << 13;
            next ^= next >> 7;
            next ^= next << 17;
            if let Err(previous) =
                self.state
                    .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                current = previous;
                continue;
            }
            return next;
        }
    }
}

impl MarketStreamJitter for ProductionMarketStreamJitter {
    fn multiplier_bps(&self) -> u16 {
        if self.min_bps == self.max_bps {
            return self.min_bps;
        }
        let span = u64::from(self.max_bps - self.min_bps);
        let offset = self.next_u64() % (span + 1);
        self.min_bps + u16::try_from(offset).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketStreamReconnectPolicy {
    initial_retry_delay: Duration,
    max_retry_delay: Duration,
    max_reconnect_attempts: Option<u32>,
}

impl MarketStreamReconnectPolicy {
    /// Builds a bounded exponential reconnect policy.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::InvalidPollingPolicy`] when either delay is
    /// zero or the initial delay exceeds the maximum.
    pub fn new(
        initial_retry_delay: Duration,
        max_retry_delay: Duration,
    ) -> Result<Self, MarketDataError> {
        if initial_retry_delay.is_zero() || max_retry_delay.is_zero() {
            return Err(MarketDataError::InvalidPollingPolicy(
                "stream retry delays must be positive",
            ));
        }
        if initial_retry_delay > max_retry_delay {
            return Err(MarketDataError::InvalidPollingPolicy(
                "stream initial retry delay must not exceed the maximum",
            ));
        }
        Ok(Self {
            initial_retry_delay,
            max_retry_delay,
            max_reconnect_attempts: None,
        })
    }

    #[must_use]
    pub const fn with_max_reconnect_attempts(mut self, max_reconnect_attempts: u32) -> Self {
        self.max_reconnect_attempts = Some(max_reconnect_attempts);
        self
    }

    pub(crate) fn retry_delay(
        self,
        consecutive_failures: u32,
        jitter: &dyn MarketStreamJitter,
    ) -> Duration {
        let exponent = consecutive_failures.saturating_sub(1).min(31);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let base = self
            .initial_retry_delay
            .checked_mul(multiplier)
            .unwrap_or(self.max_retry_delay)
            .min(self.max_retry_delay);
        let scaled_millis = base
            .as_millis()
            .saturating_mul(u128::from(jitter.multiplier_bps()))
            / 10_000_u128;
        Duration::from_millis(u64::try_from(scaled_millis).unwrap_or(u64::MAX))
    }

    pub(crate) fn exhausted(self, consecutive_failures: u32) -> bool {
        self.max_reconnect_attempts
            .is_some_and(|limit| consecutive_failures >= limit)
    }
}

#[derive(Debug)]
struct BroadcastTextWebSocketSession {
    receiver: broadcast::Receiver<TextWebSocketEvent>,
}

#[async_trait]
impl TextWebSocketSession for BroadcastTextWebSocketSession {
    async fn next_event(&mut self) -> Result<TextWebSocketEvent, ExchangeError> {
        match self.receiver.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Ok(TextWebSocketEvent::Lagged { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => Ok(TextWebSocketEvent::Closed {
                kind: WebSocketCloseKind::Remote,
            }),
        }
    }
}

pub struct TokioTextWebSocketConnector {
    url: reqwest::Url,
    queue_capacity: usize,
    ping_interval: Duration,
    init_message_factory: Option<InitMessageFactory>,
}

impl Debug for TokioTextWebSocketConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TokioTextWebSocketConnector")
            .field("url", &self.url)
            .field("queue_capacity", &self.queue_capacity)
            .field("ping_interval", &self.ping_interval)
            .finish_non_exhaustive()
    }
}

impl Clone for TokioTextWebSocketConnector {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            queue_capacity: self.queue_capacity,
            ping_interval: self.ping_interval,
            init_message_factory: self.init_message_factory.clone(),
        }
    }
}

impl TokioTextWebSocketConnector {
    /// Builds one websocket connector with bounded in-process buffering.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when the ping interval is
    /// zero.
    pub fn new(
        url: reqwest::Url,
        queue_capacity: NonZeroUsize,
        ping_interval: Duration,
    ) -> Result<Self, ExchangeError> {
        if ping_interval.is_zero() {
            return Err(ExchangeError::invalid(
                "websocket ping interval must be positive",
            ));
        }
        Ok(Self {
            url,
            queue_capacity: queue_capacity.get(),
            ping_interval,
            init_message_factory: None,
        })
    }

    /// Configures one fixed set of text frames sent immediately after connect.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when any init message is
    /// empty.
    pub fn with_init_messages(mut self, init_messages: Vec<String>) -> Result<Self, ExchangeError> {
        for message in &init_messages {
            if message.is_empty() {
                return Err(ExchangeError::invalid(
                    "websocket init messages must not be empty",
                ));
            }
        }
        self.init_message_factory = Some(Arc::new(move || Ok(init_messages.clone())));
        Ok(self)
    }

    /// Builds a Binance Spot raw `bookTicker` connector for exactly one route.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when more than one route is
    /// supplied or the endpoint cannot build a safe websocket URL.
    #[allow(clippy::needless_pass_by_value)]
    pub fn for_binance_book_ticker(
        endpoint: BinanceSpotMarketStreamEndpoint,
        routes: &[BinancePollingRoute],
        queue_capacity: NonZeroUsize,
        ping_interval: Duration,
    ) -> Result<Self, ExchangeError> {
        if routes.len() != 1 {
            return Err(ExchangeError::invalid(
                "Binance bookTicker websocket currently supports exactly one route",
            ));
        }
        let stream_name = routes
            .iter()
            .map(|route| {
                format!(
                    "{}@bookTicker",
                    route.wire_symbol().as_str().to_ascii_lowercase()
                )
            })
            .collect::<Vec<_>>()
            .join("/");
        Self::new(
            endpoint.stream_url(&stream_name)?,
            queue_capacity,
            ping_interval,
        )
    }

    /// Builds a Binance Spot websocket-API connector that signs each connect
    /// attempt with a fresh timestamp.
    ///
    /// # Errors
    ///
    /// Returns any endpoint-validation or subscription-signing error surfaced
    /// by the underlying protocol and connector builders.
    pub fn for_binance_user_data_stream(
        endpoint: BinanceSpotUserDataStreamEndpoint,
        protocol: Arc<BinanceTestnetProtocol>,
        recv_window_ms: Option<u64>,
        queue_capacity: NonZeroUsize,
        ping_interval: Duration,
    ) -> Result<Self, ExchangeError> {
        Self::for_binance_user_data_stream_with_timestamp_provider(
            endpoint,
            protocol,
            recv_window_ms,
            queue_capacity,
            ping_interval,
            Arc::new(current_timestamp_ms),
        )
    }

    /// Builds a Binance Spot websocket-API connector with an injected
    /// timestamp source for deterministic tests.
    ///
    /// # Errors
    ///
    /// Returns any endpoint-validation or subscription-signing error surfaced
    /// by the underlying protocol and connector builders.
    #[allow(clippy::needless_pass_by_value)]
    pub fn for_binance_user_data_stream_with_timestamp_provider(
        endpoint: BinanceSpotUserDataStreamEndpoint,
        protocol: Arc<BinanceTestnetProtocol>,
        recv_window_ms: Option<u64>,
        queue_capacity: NonZeroUsize,
        ping_interval: Duration,
        timestamp_provider: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self, ExchangeError> {
        let endpoint_url = endpoint.websocket_url()?;
        let connector = Self::new(endpoint_url, queue_capacity, ping_interval)?;
        Ok(Self {
            init_message_factory: Some(Arc::new(move || {
                let subscription = protocol.build_user_data_stream_subscribe_signature(
                    timestamp_provider(),
                    recv_window_ms,
                )?;
                let mut params = serde_json::Map::new();
                params.insert(
                    "apiKey".to_owned(),
                    serde_json::Value::String(subscription.api_key),
                );
                if let Some(recv_window_ms) = subscription.recv_window_ms {
                    params.insert(
                        "recvWindow".to_owned(),
                        serde_json::Value::Number(recv_window_ms.into()),
                    );
                }
                params.insert(
                    "signature".to_owned(),
                    serde_json::Value::String(subscription.signature),
                );
                params.insert(
                    "timestamp".to_owned(),
                    serde_json::Value::Number(subscription.timestamp_ms.into()),
                );
                Ok(vec![
                    serde_json::json!({
                        "id": "user-data-subscribe",
                        "method": "userDataStream.subscribe.signature",
                        "params": params,
                    })
                    .to_string(),
                ])
            })),
            ..connector
        })
    }
}

#[async_trait]
impl TextWebSocketConnector for TokioTextWebSocketConnector {
    async fn connect(&self) -> Result<Box<dyn TextWebSocketSession>, ExchangeError> {
        let (mut socket, _) = tokio_tungstenite::connect_async(self.url.as_str())
            .await
            .map_err(|error| ExchangeError::unavailable(error.to_string()))?;
        let init_messages = self
            .init_message_factory
            .as_ref()
            .map_or_else(|| Ok(Vec::new()), |factory| factory())?;
        for message in &init_messages {
            socket
                .send(Message::Text(message.clone().into()))
                .await
                .map_err(|error| ExchangeError::unavailable(error.to_string()))?;
        }
        let (sender, receiver) = broadcast::channel(self.queue_capacity);
        let ping_interval = self.ping_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + ping_interval,
                ping_interval,
            );
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if sender.receiver_count() == 0 {
                            break;
                        }
                        if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                            let _ = sender.send(TextWebSocketEvent::Closed { kind: WebSocketCloseKind::Protocol });
                            break;
                        }
                    }
                    message = socket.next() => {
                        let Some(message) = message else {
                            let _ = sender.send(TextWebSocketEvent::Closed { kind: WebSocketCloseKind::Remote });
                            break;
                        };
                        match message {
                            Ok(Message::Text(text)) => {
                                if sender.send(TextWebSocketEvent::Text(text.to_string())).is_err() {
                                    break;
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                if socket.send(Message::Pong(payload)).await.is_err() {
                                    let _ = sender.send(TextWebSocketEvent::Closed { kind: WebSocketCloseKind::Protocol });
                                    break;
                                }
                            }
                            Ok(Message::Pong(_)) => {
                                if sender.send(TextWebSocketEvent::Heartbeat).is_err() {
                                    break;
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                let kind = classify_close_kind(frame.as_ref().map(|close| close.reason.as_ref()));
                                let _ = sender.send(TextWebSocketEvent::Closed { kind });
                                break;
                            }
                            Ok(Message::Binary(_) | Message::Frame(_)) => {
                                let _ = sender.send(TextWebSocketEvent::Closed { kind: WebSocketCloseKind::Protocol });
                                break;
                            }
                            Err(_) => {
                                let _ = sender.send(TextWebSocketEvent::Closed { kind: WebSocketCloseKind::Remote });
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(Box::new(BroadcastTextWebSocketSession { receiver }))
    }
}

#[derive(Debug, Clone)]
struct StreamRouteState {
    route: BinancePollingRoute,
    revision: u64,
    last_source_sequence: Option<u64>,
}

#[derive(Debug)]
pub struct BinanceBookTickerStreamSource {
    _exchange: BinancePublicExchange,
    routes: HashMap<String, StreamRouteState>,
    connector: Arc<dyn TextWebSocketConnector>,
    session: Option<Box<dyn TextWebSocketSession>>,
    pending_events: VecDeque<MarketDataEvent>,
    reconnect_policy: MarketStreamReconnectPolicy,
    clock: Arc<dyn MarketDataClock>,
    sleeper: Arc<dyn MarketStreamSleeper>,
    jitter: Arc<dyn MarketStreamJitter>,
    consecutive_failures: u32,
    pending_retry: Option<Duration>,
    connection_generation: u64,
    exhausted: bool,
}

impl BinanceBookTickerStreamSource {
    #[allow(clippy::needless_pass_by_value)]
    /// Builds one single-route Binance `bookTicker` market-data source.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] when the route set is empty, oversized,
    /// duplicated, non-spot, or not exactly one symbol.
    pub fn new<C>(
        exchange: BinancePublicExchange,
        routes: Vec<BinancePollingRoute>,
        connector: Arc<dyn TextWebSocketConnector>,
        reconnect_policy: MarketStreamReconnectPolicy,
        clock: Arc<C>,
        sleeper: Arc<dyn MarketStreamSleeper>,
        jitter: Arc<dyn MarketStreamJitter>,
    ) -> Result<Self, MarketDataError>
    where
        C: MarketDataClock + 'static,
    {
        if routes.is_empty() {
            return Err(MarketDataError::EmptyUniverse);
        }
        if routes.len() != 1 {
            return Err(MarketDataError::InvalidPollingPolicy(
                "Binance bookTicker websocket source currently supports exactly one route",
            ));
        }
        if routes.len() > MAX_STREAM_TARGETS {
            return Err(MarketDataError::UniverseTooLarge {
                count: routes.len(),
                limit: MAX_STREAM_TARGETS,
            });
        }
        let mut route_map = HashMap::with_capacity(routes.len());
        for route in routes {
            let duplicate_symbol = route.wire_symbol().clone();
            let wire_symbol = duplicate_symbol.as_str().to_owned();
            if route.instrument().exchange() != BINANCE_EXCHANGE {
                return Err(MarketDataError::UnsupportedPollingInstrument {
                    instrument: route.instrument().clone(),
                });
            }
            if route.instrument().market_type != crypto_trading_domain::MarketType::Spot {
                return Err(MarketDataError::UnsupportedPollingInstrument {
                    instrument: route.instrument().clone(),
                });
            }
            if route_map
                .insert(
                    wire_symbol,
                    StreamRouteState {
                        route,
                        revision: 0,
                        last_source_sequence: None,
                    },
                )
                .is_some()
            {
                return Err(MarketDataError::DuplicatePollingWireSymbol {
                    symbol: duplicate_symbol,
                });
            }
        }
        Ok(Self {
            _exchange: exchange,
            routes: route_map,
            connector,
            session: None,
            pending_events: VecDeque::new(),
            reconnect_policy,
            clock,
            sleeper,
            jitter,
            consecutive_failures: 0,
            pending_retry: None,
            connection_generation: 0,
            exhausted: false,
        })
    }

    async fn next_stream_event(&mut self) -> Result<Option<MarketDataEvent>, MarketDataError> {
        loop {
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
            if self.exhausted {
                return Ok(None);
            }
            if let Some(delay) = self.pending_retry.take() {
                self.sleeper.sleep(delay).await;
            }
            if self.session.is_none() {
                match self.connector.connect().await {
                    Ok(session) => {
                        self.session = Some(session);
                        self.connection_generation = self
                            .connection_generation
                            .checked_add(1)
                            .ok_or(MarketDataError::GenerationExhausted)?;
                    }
                    Err(error) => {
                        return Ok(Some(self.schedule_reconnect(&error, false)?));
                    }
                }
            }
            let observed_at = self.clock.now();
            let Some(session) = self.session.as_mut() else {
                return Err(MarketDataError::SourceIdentityMismatch {
                    expected: BINANCE_EXCHANGE.to_owned(),
                    actual: "market websocket session disappeared".to_owned(),
                });
            };
            let event = session.next_event().await.map_err(|error| {
                MarketDataError::SourceIdentityMismatch {
                    expected: BINANCE_EXCHANGE.to_owned(),
                    actual: error.to_string(),
                }
            })?;
            match event {
                TextWebSocketEvent::Text(text) => return self.handle_text(&text, observed_at),
                TextWebSocketEvent::Heartbeat => {}
                TextWebSocketEvent::Lagged { skipped } => {
                    return Ok(Some(MarketDataEvent::source_gap(
                        BINANCE_EXCHANGE,
                        skipped,
                        observed_at,
                    )?));
                }
                TextWebSocketEvent::Closed { kind } => return self.handle_closed(kind),
            }
        }
    }

    fn handle_text(
        &mut self,
        text: &str,
        observed_at: DateTime<Utc>,
    ) -> Result<Option<MarketDataEvent>, MarketDataError> {
        let observation = BinancePublicExchange::parse_book_ticker_stream_observation(
            text.as_bytes(),
            observed_at,
        )
        .map_err(|error| MarketDataError::SourceIdentityMismatch {
            expected: BINANCE_EXCHANGE.to_owned(),
            actual: error.to_string(),
        })?;
        let wire_symbol = observation.snapshot.symbol.as_str().to_owned();
        let Some(route) = self.routes.get_mut(&wire_symbol) else {
            self.pending_events.push_back(MarketDataEvent::source_gap(
                BINANCE_EXCHANGE,
                1,
                observed_at,
            )?);
            self.session = None;
            return Ok(Some(MarketDataEvent::source_unavailable(
                BINANCE_EXCHANGE,
                MarketDataSourceFailure::InvalidPayload,
                observed_at,
            )?));
        };
        if let Some(source_sequence) = observation.source_sequence
            && route
                .last_source_sequence
                .is_some_and(|last_sequence| source_sequence <= last_sequence)
        {
            self.session = None;
            self.pending_events.push_back(MarketDataEvent::source_gap(
                BINANCE_EXCHANGE,
                1,
                observed_at,
            )?);
            self.pending_retry = Some(self.reconnect_policy.retry_delay(1, self.jitter.as_ref()));
            return Ok(Some(MarketDataEvent::source_unavailable(
                BINANCE_EXCHANGE,
                MarketDataSourceFailure::InvalidPayload,
                observed_at,
            )?));
        }
        self.consecutive_failures = 0;
        self.exhausted = false;
        route.revision = route
            .revision
            .checked_add(1)
            .ok_or(MarketDataError::RevisionExhausted)?;
        route.last_source_sequence = observation.source_sequence;
        let mut snapshot = observation.snapshot;
        snapshot.symbol = route.route.instrument().symbol.clone();
        snapshot.market_type = route.route.instrument().market_type;
        Ok(Some(MarketDataEvent::Observation(
            MarketDataObservation::with_source_metadata_and_generation(
                snapshot,
                route.revision,
                observed_at,
                MarketTimestampProvenance::LocalReceipt,
                observation.source_sequence,
                Some(self.connection_generation),
            )?,
        )))
    }

    fn handle_closed(
        &mut self,
        kind: WebSocketCloseKind,
    ) -> Result<Option<MarketDataEvent>, MarketDataError> {
        let message = match kind {
            WebSocketCloseKind::Remote => "market websocket closed",
            WebSocketCloseKind::ServerShutdown => "market websocket serverShutdown",
            WebSocketCloseKind::Expired => "market websocket expired",
            WebSocketCloseKind::Protocol => "market websocket protocol failure",
        };
        Ok(Some(self.schedule_reconnect(
            &ExchangeError::unavailable(message),
            true,
        )?))
    }

    fn schedule_reconnect(
        &mut self,
        error: &ExchangeError,
        transport_gap: bool,
    ) -> Result<MarketDataEvent, MarketDataError> {
        self.session = None;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let observed_at = self.clock.now();
        if transport_gap {
            self.pending_events.push_back(MarketDataEvent::source_gap(
                BINANCE_EXCHANGE,
                1,
                observed_at,
            )?);
        }
        if self.reconnect_policy.exhausted(self.consecutive_failures) {
            self.exhausted = true;
        } else {
            self.pending_retry = Some(
                self.reconnect_policy
                    .retry_delay(self.consecutive_failures, self.jitter.as_ref()),
            );
        }
        MarketDataEvent::source_unavailable(
            BINANCE_EXCHANGE,
            classify_exchange_failure(error),
            observed_at,
        )
    }
}

impl MarketDataEventSource for BinanceBookTickerStreamSource {
    fn source_id(&self) -> &str {
        BINANCE_EXCHANGE
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(self.next_stream_event())
    }
}

fn classify_close_kind(reason: Option<&str>) -> WebSocketCloseKind {
    let Some(reason) = reason else {
        return WebSocketCloseKind::Remote;
    };
    if reason.contains("serverShutdown") {
        WebSocketCloseKind::ServerShutdown
    } else if reason.contains("expired") || reason.contains("24h") {
        WebSocketCloseKind::Expired
    } else {
        WebSocketCloseKind::Remote
    }
}

fn current_timestamp_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0)
}
