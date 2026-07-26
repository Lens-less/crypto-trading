use std::{
    io::{Read, Write},
    net::TcpListener,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_cli::{
    continuous_monitor::{
        CONTINUOUS_MONITOR_TASK_STATUS_SCHEMA_VERSION, ContinuousMonitorTask,
        ContinuousMonitorTaskConfig, ContinuousMonitorTaskError, ContinuousMonitorTaskExit,
        ContinuousMonitorTaskFailure, ContinuousMonitorTaskPhase,
    },
    monitor::ReadOnlyArbitrageMonitor,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_exchange::BinancePublicExchange;
use crypto_trading_runtime::{
    ArbitrageMonitorReadModel, BinancePollingRoute, BinancePublicPollingSource,
    FileJournalSnapshotSource, JournalSnapshotSource, JsonlHistory, MarketDataBook,
    MarketDataClock, MarketDataError, MarketDataEvent, MarketDataEventFuture,
    MarketDataEventSource, MarketDataObservation, MarketDataSourceFailure, MarketFreshnessPolicy,
    MarketInstrument, MarketPollingPolicy, MarketSupervisorConfig, MarketSupervisorHealth,
    MarketUniverse, ProjectionStatus, ReadOnlyTaskFailure, ReadOnlyTaskPhase,
    ReadOnlyTaskReadModel, ReadOnlyTaskRecovery, SpreadHistoryReadModel, SpreadHistoryWriter,
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct ChannelSource {
    source_id: String,
    receiver: mpsc::Receiver<Result<Option<MarketDataEvent>, MarketDataError>>,
}

impl ChannelSource {
    fn new(
        source_id: &str,
    ) -> (
        Self,
        mpsc::Sender<Result<Option<MarketDataEvent>, MarketDataError>>,
    ) {
        let (sender, receiver) = mpsc::channel(8);
        (
            Self {
                source_id: source_id.to_owned(),
                receiver,
            },
            sender,
        )
    }
}

impl MarketDataEventSource for ChannelSource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(async move { self.receiver.recv().await.unwrap_or(Ok(None)) })
    }
}

#[derive(Debug)]
struct FixedClock(DateTime<Utc>);

impl MarketDataClock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[tokio::test]
async fn exact_pair_is_journal_first_and_projects_independent_source_health() {
    let path = temp_path("continuous-monitor-happy");
    let base = timestamp(0);
    let monitor = monitor(base);
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let config = config("arb-btc-usdt");

    let mut task = ContinuousMonitorTask::start(
        config,
        monitor,
        left_source,
        right_source,
        JsonlHistory::new(&path),
    )
    .await
    .unwrap();

    assert_eq!(
        task.status().schema_version,
        CONTINUOUS_MONITOR_TASK_STATUS_SCHEMA_VERSION
    );
    assert_eq!(task.status().phase, ContinuousMonitorTaskPhase::Running);
    assert_eq!(task.status().processed_event_count, 0);
    assert_eq!(task.status().sources.len(), 2);

    left_sender
        .send(Ok(Some(observation("left", "99", "100", 1, base))))
        .await
        .unwrap();
    right_sender
        .send(Ok(Some(
            MarketDataEvent::source_unavailable(
                "right",
                MarketDataSourceFailure::Disconnected,
                base + Duration::seconds(1),
            )
            .unwrap(),
        )))
        .await
        .unwrap();
    wait_for_processed(&task, 2).await;

    let running = task.status();
    assert_eq!(running.phase, ContinuousMonitorTaskPhase::Running);
    assert_eq!(running.processed_event_count, 2);
    assert_eq!(running.sources[0].source_id, "left");
    assert_eq!(running.sources[0].health, MarketSupervisorHealth::Healthy);
    assert_eq!(running.sources[1].source_id, "right");
    assert_eq!(running.sources[1].health, MarketSupervisorHealth::Degraded);

    let exit = task.stop().await.unwrap();
    assert_eq!(exit, ContinuousMonitorTaskExit::StopRequested);
    assert_eq!(task.status().phase, ContinuousMonitorTaskPhase::Stopped);
    assert_eq!(
        task.stop().await.unwrap(),
        ContinuousMonitorTaskExit::StopRequested
    );

    let records = read_records(&path);
    let decisions = records
        .iter()
        .map(|record| record["decision"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        decisions,
        vec![
            "task_registered",
            "task_running",
            "monitor_waiting",
            "task_checkpointed",
            "monitor_waiting",
            "task_checkpointed",
            "task_stopping",
            "task_stopped",
        ]
    );
    for record in &records {
        let encoded = record.to_string();
        for forbidden in ["orders", "intents", "api_key", "authorization", "panic"] {
            assert!(
                !encoded.contains(forbidden),
                "{forbidden} leaked in {encoded}"
            );
        }
    }

    let snapshot = snapshot(&path);
    let monitor_model = ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot).unwrap();
    assert_eq!(monitor_model.projection_status, ProjectionStatus::Complete);
    assert_eq!(monitor_model.latest.unwrap().monitor_sequence, 2);
    let task_model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot).unwrap();
    assert_eq!(task_model.projection_status, ProjectionStatus::Complete);
    assert_eq!(task_model.tasks.len(), 1);
    assert_eq!(task_model.tasks[0].phase, ReadOnlyTaskPhase::Stopped);
    assert_eq!(task_model.tasks[0].recovery, ReadOnlyTaskRecovery::None);
    assert_eq!(task_model.tasks[0].processed_event_count, 2);

    remove_file(&path);
}

#[tokio::test]
async fn source_contract_failure_stops_the_sibling_and_persists_a_bounded_terminal() {
    let path = temp_path("continuous-monitor-source-failure");
    let base = timestamp(0);
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, _right_sender) = ChannelSource::new("right");
    let mut task = ContinuousMonitorTask::start(
        config("arb-source-failure"),
        monitor(base),
        left_source,
        right_source,
        JsonlHistory::new(&path),
    )
    .await
    .unwrap();

    left_sender
        .send(Ok(Some(observation("drift", "99", "100", 1, base))))
        .await
        .unwrap();
    wait_for_phase(&task, ContinuousMonitorTaskPhase::Failed).await;

    let status = task.status();
    assert_eq!(
        status.failure,
        Some(ContinuousMonitorTaskFailure::SourceContract)
    );
    assert!(matches!(
        task.stop().await.unwrap_err(),
        ContinuousMonitorTaskError::SourceContract
    ));

    let task_model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(&path)).unwrap();
    assert_eq!(task_model.projection_status, ProjectionStatus::Complete);
    assert_eq!(task_model.tasks.len(), 1);
    assert_eq!(task_model.tasks[0].phase, ReadOnlyTaskPhase::Failed);
    assert_eq!(
        task_model.tasks[0].failure,
        Some(ReadOnlyTaskFailure::SourceContract)
    );
    assert_eq!(
        task_model.tasks[0].recovery,
        ReadOnlyTaskRecovery::Investigate
    );
    let encoded = std::fs::read_to_string(&path).unwrap();
    assert!(!encoded.contains("drift"));
    assert!(!encoded.contains("identity mismatch"));

    remove_file(&path);
}

#[tokio::test]
async fn journal_failure_keeps_the_last_durable_checkpoint_visible() {
    let path = temp_path("continuous-monitor-journal-failure");
    let backup = path.with_extension("before-failure.jsonl");
    let base = timestamp(0);
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, _right_sender) = ChannelSource::new("right");
    let mut task = ContinuousMonitorTask::start(
        config("arb-journal-failure"),
        monitor(base),
        left_source,
        right_source,
        JsonlHistory::new(&path),
    )
    .await
    .unwrap();
    let durable = task.status();
    assert_eq!(durable.processed_event_count, 0);
    assert_eq!(durable.runtime_failure, None);

    std::fs::rename(&path, &backup).unwrap();
    std::fs::create_dir(&path).unwrap();
    left_sender
        .send(Ok(Some(observation("left", "99", "100", 1, base))))
        .await
        .unwrap();
    wait_for_runtime_failure(&task, ContinuousMonitorTaskFailure::JournalUnavailable).await;

    let mut expected = durable;
    expected.runtime_failure = Some(ContinuousMonitorTaskFailure::JournalUnavailable);
    assert_eq!(task.status(), expected);
    assert!(matches!(
        task.stop().await.unwrap_err(),
        ContinuousMonitorTaskError::Journal(_)
    ));

    let records = read_records(&backup);
    assert_eq!(
        records
            .iter()
            .map(|record| record["decision"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["task_registered", "task_running"]
    );
    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(&backup)).unwrap();
    assert_eq!(model.tasks[0].phase, ReadOnlyTaskPhase::Running);
    assert_eq!(model.tasks[0].processed_event_count, 0);
    assert_eq!(model.tasks[0].recovery, ReadOnlyTaskRecovery::Investigate);

    std::fs::remove_dir(&path).unwrap();
    remove_file(&backup);
}

#[tokio::test]
async fn invalid_source_binding_fails_before_creating_a_durable_registration() {
    let path = temp_path("continuous-monitor-invalid-binding");
    let (wrong_left, _left_sender) = ChannelSource::new("right");
    let (right, _right_sender) = ChannelSource::new("right");

    let error = ContinuousMonitorTask::start(
        config("arb-invalid-binding"),
        monitor(timestamp(0)),
        wrong_left,
        right,
        JsonlHistory::new(&path),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ContinuousMonitorTaskError::InvalidSourceBinding
    ));
    assert!(!path.exists());
}

#[tokio::test]
async fn credential_free_binance_polling_composes_with_a_second_source_and_the_same_journal() {
    let path = temp_path("continuous-monitor-binance-source");
    let base = timestamp(0);
    let (base_url, server) = stub_server(vec![http_response(
        "200 OK",
        include_str!("../../exchange/tests/fixtures/binance_book_ticker.json"),
    )]);
    let binance_instrument = instrument("binance");
    let binance = BinancePublicPollingSource::new(
        BinancePublicExchange::with_base_url(&base_url).unwrap(),
        vec![
            BinancePollingRoute::new(binance_instrument.clone(), Symbol::new("LTCBTC").unwrap())
                .unwrap(),
        ],
        MarketPollingPolicy::new(
            StdDuration::from_secs(30),
            StdDuration::from_secs(30),
            StdDuration::from_secs(30),
        )
        .unwrap(),
        Arc::new(FixedClock(base)),
    )
    .unwrap();
    let (other, other_sender) = ChannelSource::new("right");
    let mut task = ContinuousMonitorTask::start(
        config("arb-binance-right"),
        monitor_for(base, binance_instrument, instrument("right")),
        binance,
        other,
        JsonlHistory::new(&path),
    )
    .await
    .unwrap();

    wait_for_processed(&task, 1).await;
    other_sender
        .send(Ok(Some(observation("right", "102", "103", 1, base))))
        .await
        .unwrap();
    wait_for_processed(&task, 2).await;
    task.stop().await.unwrap();

    let model = ReadOnlyTaskReadModel::from_legacy_snapshot(&snapshot(&path)).unwrap();
    assert_eq!(model.tasks[0].sources[0].source_id, "binance");
    assert_eq!(model.tasks[0].sources[0].event_sequence, 1);
    assert_eq!(model.tasks[0].sources[1].source_id, "right");
    assert_eq!(model.tasks[0].processed_event_count, 2);
    assert_eq!(
        ArbitrageMonitorReadModel::from_legacy_snapshot(&snapshot(&path))
            .unwrap()
            .latest
            .unwrap()
            .monitor_sequence,
        2
    );

    server.join().unwrap();
    remove_file(&path);
}

#[tokio::test]
async fn spread_observations_are_mirrored_into_the_dedicated_spread_history_journal() {
    let path = temp_path("continuous-monitor-spread-history");
    let spread_path = temp_path("continuous-monitor-spread-history-spread");
    let base = timestamp(0);
    let (left_source, left_sender) = ChannelSource::new("left");
    let (right_source, right_sender) = ChannelSource::new("right");
    let mut task = ContinuousMonitorTask::start_with_spread_history(
        config("arb-spread-history"),
        monitor(base),
        left_source,
        right_source,
        JsonlHistory::new(&path),
        Some(SpreadHistoryWriter::new(&spread_path)),
    )
    .await
    .unwrap();

    // First event: only one leg is fresh, so the outcome is waiting and no
    // spread record may be persisted.
    left_sender
        .send(Ok(Some(observation("left", "99", "100", 1, base))))
        .await
        .unwrap();
    wait_for_processed(&task, 1).await;
    assert!(
        !spread_path.exists(),
        "waiting outcomes must not append spread history"
    );

    // Second event completes the pair: 102 bid vs 100 ask is a 2% spread,
    // recorded as 200 bps.
    right_sender
        .send(Ok(Some(observation(
            "right",
            "102",
            "103",
            1,
            base + Duration::seconds(1),
        ))))
        .await
        .unwrap();
    wait_for_processed(&task, 2).await;
    task.stop().await.unwrap();

    let spread_snapshot = crypto_trading_runtime::JournalSnapshot::new(
        "00000000-0000-0000-0000-000000000901".parse().unwrap(),
        crypto_trading_runtime::read_journal_chain(&spread_path).unwrap(),
    )
    .unwrap();
    let model = SpreadHistoryReadModel::from_legacy_snapshot(&spread_snapshot).unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.samples.len(), 1);
    let sample = &model.samples[0];
    assert_eq!(sample.symbol, "BTC-USDT");
    assert_eq!(sample.exchange_buy, "left");
    assert_eq!(sample.exchange_sell, "right");
    assert_eq!(sample.price_buy, "100");
    assert_eq!(sample.price_sell, "102");
    assert_eq!(sample.spread_bps, "200.00");
    assert_eq!(sample.funding_rate_buy, None, "no source publishes funding");

    remove_file(&path);
    remove_file(&spread_path);
}

fn config(task_id: &str) -> ContinuousMonitorTaskConfig {
    ContinuousMonitorTaskConfig::new(
        task_id,
        MarketSupervisorConfig::new(StdDuration::from_secs(30)).unwrap(),
    )
    .unwrap()
}

fn monitor(now: DateTime<Utc>) -> ReadOnlyArbitrageMonitor {
    monitor_for(now, instrument("left"), instrument("right"))
}

fn monitor_for(
    now: DateTime<Utc>,
    left: MarketInstrument,
    right: MarketInstrument,
) -> ReadOnlyArbitrageMonitor {
    let universe = MarketUniverse::new(vec![left.clone(), right.clone()]).unwrap();
    let book = MarketDataBook::new(
        universe,
        MarketFreshnessPolicy::new(Duration::seconds(10), Duration::seconds(1)).unwrap(),
        Arc::new(FixedClock(now + Duration::seconds(2))),
    );
    ReadOnlyArbitrageMonitor::new(book, left, right, Decimal::new(5, 1)).unwrap()
}

fn instrument(exchange: &str) -> MarketInstrument {
    MarketInstrument::new(exchange, Symbol::new("BTC-USDT").unwrap(), MarketType::Spot).unwrap()
}

fn observation(
    exchange: &str,
    bid: &str,
    ask: &str,
    revision: u64,
    at: DateTime<Utc>,
) -> MarketDataEvent {
    let snapshot = MarketSnapshot::new(
        exchange,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Spot,
        Price::new(Decimal::from_str(bid).unwrap()).unwrap(),
        Price::new(Decimal::from_str(ask).unwrap()).unwrap(),
        at,
    )
    .unwrap();
    MarketDataEvent::Observation(MarketDataObservation::new(snapshot, revision, at).unwrap())
}

async fn wait_for_processed(task: &ContinuousMonitorTask, expected: u64) {
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if task.status().processed_event_count >= expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_phase(task: &ContinuousMonitorTask, expected: ContinuousMonitorTaskPhase) {
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if task.status().phase == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_runtime_failure(
    task: &ContinuousMonitorTask,
    expected: ContinuousMonitorTaskFailure,
) {
    tokio::time::timeout(StdDuration::from_secs(10), async {
        loop {
            if task.status().runtime_failure == Some(expected) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn snapshot(path: &std::path::Path) -> crypto_trading_runtime::JournalSnapshot {
    FileJournalSnapshotSource::new(
        "00000000-0000-0000-0000-000000000900".parse().unwrap(),
        path,
    )
    .unwrap()
    .snapshot()
    .unwrap()
}

fn read_records(path: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
}

fn temp_path(label: &str) -> std::path::PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "crypto-trading-{label}-{}-{sequence}.jsonl",
        std::process::id()
    ))
}

fn remove_file(path: &std::path::Path) {
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
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
