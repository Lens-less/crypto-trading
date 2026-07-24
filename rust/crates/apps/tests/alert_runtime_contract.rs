use std::{
    collections::VecDeque,
    future,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_cli::alert::{
    AlertAcknowledgementOutcome, AlertDeliveryMode, AlertNotification, AlertNotificationAdapter,
    AlertNotificationFuture, AlertOccurrenceId, DeterministicNotificationAdapter,
    LocalNoticeNotificationAdapter, NotificationConfigError, NotificationDispatcherConfig,
    NotificationDispatcherExit, NotificationEnqueueState, NotificationFailure, PriceAlertRuntime,
    PriceAlertRuntimeConfig, PriceAlertRuntimeError,
};
use crypto_trading_config::{
    PriceAlertConfig, PriceAlertSymbolConfig, PriceThresholdConfig, VolatilityAlertConfig,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Symbol};
use crypto_trading_runtime::{
    FileJournalSnapshotSource, JournalSnapshotSource, JsonlHistory, MarketDataClock,
    MarketDataEvent, MarketDataObservation, MarketFreshnessPolicy,
};
use rust_decimal::Decimal;
use tokio::sync::Notify;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct TestClock {
    now: Mutex<DateTime<Utc>>,
}

impl TestClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(now),
        }
    }

    fn set(&self, now: DateTime<Utc>) {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = now;
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

#[derive(Debug)]
struct PendingAdapter {
    entered: Arc<Notify>,
}

impl AlertNotificationAdapter for PendingAdapter {
    fn adapter_id(&self) -> &'static str {
        "pending"
    }

    fn deliver(&mut self, _notification: AlertNotification) -> AlertNotificationFuture<'_> {
        let entered = Arc::clone(&self.entered);
        Box::pin(async move {
            entered.notify_one();
            future::pending().await
        })
    }
}

#[derive(Debug)]
struct PanickingAdapter {
    entered: Arc<Notify>,
}

impl AlertNotificationAdapter for PanickingAdapter {
    fn adapter_id(&self) -> &'static str {
        "panicking"
    }

    fn deliver(&mut self, _notification: AlertNotification) -> AlertNotificationFuture<'_> {
        let entered = Arc::clone(&self.entered);
        Box::pin(async move {
            entered.notify_one();
            panic!("scripted adapter panic");
        })
    }
}

#[test]
fn dispatch_construction_without_tokio_runtime_fails_closed() {
    let path = temp_path("alert-no-runtime");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let (adapter, _) =
        DeterministicNotificationAdapter::new("deterministic", VecDeque::new(), 1).unwrap();
    let error = PriceAlertRuntime::new(
        &threshold_config(0),
        MarketFreshnessPolicy::new(Duration::seconds(300), Duration::seconds(1)).unwrap(),
        clock,
        JsonlHistory::new(&path),
        PriceAlertRuntimeConfig::new(AlertDeliveryMode::Dispatch, dispatcher_config(1), 1).unwrap(),
        vec![Box::new(adapter)],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        PriceAlertRuntimeError::NotificationConfig(NotificationConfigError::MissingRuntime)
    ));
    remove_file(&path);
}

#[test]
fn runtime_rejects_unsafe_identity_and_unrecoverable_sampling_budget() {
    let path = temp_path("alert-invalid-config");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let runtime_config =
        PriceAlertRuntimeConfig::new(AlertDeliveryMode::JournalOnly, dispatcher_config(1), 1)
            .unwrap();

    let mut unsafe_identity = threshold_config(0);
    unsafe_identity.exchange = "binance\u{1b}[2J".to_owned();
    let error = PriceAlertRuntime::new(
        &unsafe_identity,
        MarketFreshnessPolicy::new(Duration::seconds(300), Duration::seconds(1)).unwrap(),
        Arc::clone(&clock),
        JsonlHistory::new(&path),
        runtime_config,
        Vec::new(),
    )
    .unwrap_err();
    assert!(matches!(error, PriceAlertRuntimeError::InvalidConfig(_)));

    let mut oversized_window = volatility_config(0);
    oversized_window.symbols[0]
        .volatility_alert
        .time_window_seconds = 1_000_000;
    let error = PriceAlertRuntime::new(
        &oversized_window,
        MarketFreshnessPolicy::new(Duration::seconds(300), Duration::seconds(1)).unwrap(),
        clock,
        JsonlHistory::new(&path),
        runtime_config,
        Vec::new(),
    )
    .unwrap_err();
    assert!(matches!(error, PriceAlertRuntimeError::InvalidConfig(_)));
    remove_file(&path);
}

#[tokio::test]
async fn ready_samples_persist_occurrences_and_cooldown_suppresses_duplicates() {
    let path = temp_path("alert-cooldown");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let (adapter, probe) =
        DeterministicNotificationAdapter::new("deterministic", VecDeque::from([Ok(()), Ok(())]), 8)
            .unwrap();
    let mut runtime = runtime(
        &threshold_config(30),
        Arc::clone(&clock),
        &path,
        vec![Box::new(adapter)],
        dispatcher_config(4),
    );

    let first = runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    assert_eq!(first.occurrences.len(), 1);

    let duplicate = runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    assert!(duplicate.occurrences.is_empty());

    clock.set(timestamp(10));
    let cooling_down = runtime
        .process(observation(2, timestamp(10), "106"))
        .await
        .unwrap();
    assert!(cooling_down.occurrences.is_empty());

    let unavailable = runtime
        .process(
            MarketDataEvent::source_unavailable(
                "binance",
                crypto_trading_runtime::MarketDataSourceFailure::Disconnected,
                timestamp(11),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(unavailable.occurrences.is_empty());

    clock.set(timestamp(31));
    let after_cooldown = runtime
        .process(observation(3, timestamp(31), "107"))
        .await
        .unwrap();
    assert_eq!(after_cooldown.occurrences.len(), 1);

    assert_eq!(runtime.stop().await, NotificationDispatcherExit::Drained);
    assert_eq!(probe.deliveries().len(), 2);
    let records = records(&path);
    assert_eq!(decision_count(&records, "price_alert_sampled"), 3);
    assert_eq!(decision_count(&records, "price_alert_occurred"), 2);
    assert_eq!(
        decision_count(&records, "price_alert_delivery_succeeded"),
        2
    );
    remove_file(&path);
}

#[tokio::test]
async fn recovery_restores_price_window_cooldown_and_acknowledgement_without_replaying_delivery() {
    let path = temp_path("alert-recovery");
    let journal_id = "00000000-0000-0000-0000-000000000384".parse().unwrap();
    let clock = Arc::new(TestClock::new(timestamp(-70)));
    let mut first = journal_only_runtime(&volatility_config(30), Arc::clone(&clock), &path);

    for (revision, offset) in [-70, -60, -50, -40, -30]
        .into_iter()
        .enumerate()
        .map(|(index, offset)| (u64::try_from(index).unwrap() + 1, offset))
    {
        clock.set(timestamp(offset));
        let report = first
            .process(observation(revision, timestamp(offset), "100"))
            .await
            .unwrap();
        assert!(report.occurrences.is_empty());
    }

    clock.set(timestamp(0));
    let triggered = first
        .process(observation(6, timestamp(0), "105"))
        .await
        .unwrap();
    let occurrence = triggered.occurrences[0].id.clone();
    assert_eq!(
        first.acknowledge(&occurrence, timestamp(1)).await.unwrap(),
        AlertAcknowledgementOutcome::Recorded
    );
    first.stop().await;

    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let snapshot = source.snapshot().unwrap();
    let (adapter, probe) =
        DeterministicNotificationAdapter::new("deterministic", VecDeque::new(), 4).unwrap();
    let mut recovered = runtime(
        &volatility_config(30),
        Arc::clone(&clock),
        &path,
        vec![Box::new(adapter)],
        dispatcher_config(4),
    );
    recovered.recover(&snapshot).unwrap();

    assert_eq!(
        recovered
            .acknowledge(&occurrence, timestamp(2))
            .await
            .unwrap(),
        AlertAcknowledgementOutcome::AlreadyAcknowledged
    );
    clock.set(timestamp(10));
    let cooling_down = recovered
        .process(observation(7, timestamp(10), "106"))
        .await
        .unwrap();
    assert!(cooling_down.occurrences.is_empty());

    recovered.stop().await;
    assert!(probe.deliveries().is_empty());
    assert_eq!(
        decision_count(&records(&path), "price_alert_acknowledged"),
        1
    );
    remove_file(&path);
}

#[tokio::test]
async fn full_or_stuck_notification_adapter_never_blocks_the_monitor_loop() {
    let path = temp_path("alert-backpressure");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let entered = Arc::new(Notify::new());
    let adapter = PendingAdapter {
        entered: Arc::clone(&entered),
    };
    let mut runtime = runtime(
        &threshold_config(0),
        Arc::clone(&clock),
        &path,
        vec![Box::new(adapter)],
        NotificationDispatcherConfig::new(
            1,
            StdDuration::from_secs(30),
            StdDuration::from_millis(20),
        )
        .unwrap(),
    );

    runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    entered.notified().await;
    for revision in 2..=3 {
        clock.set(timestamp(i64::try_from(revision).unwrap()));
        let report = tokio::time::timeout(
            StdDuration::from_millis(50),
            runtime.process(observation(
                revision,
                timestamp(i64::try_from(revision).unwrap()),
                "105",
            )),
        )
        .await
        .expect("notification delivery must not block market processing")
        .unwrap();
        if revision == 3 {
            assert!(report.notification_enqueues.iter().any(|enqueue| {
                enqueue.adapter_id == "pending"
                    && enqueue.state == NotificationEnqueueState::Backpressure
            }));
        }
    }

    assert_eq!(runtime.recent_occurrences().len(), 3);
    assert_eq!(
        decision_count(&records(&path), "price_alert_delivery_dropped"),
        1
    );
    assert_eq!(
        runtime.stop().await,
        NotificationDispatcherExit::AbortedAfterGrace
    );
    remove_file(&path);
}

#[tokio::test]
async fn local_and_deterministic_adapters_expose_typed_notices_and_bounded_failures() {
    let path = temp_path("alert-adapters");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let (local, mut local_notices) = LocalNoticeNotificationAdapter::channel(2).unwrap();
    let (deterministic, probe) = DeterministicNotificationAdapter::new(
        "deterministic",
        VecDeque::from([
            Err(NotificationFailure::DeviceUnavailable),
            Err(NotificationFailure::Rejected),
        ]),
        4,
    )
    .unwrap();
    let mut runtime = runtime(
        &threshold_config(0),
        Arc::clone(&clock),
        &path,
        vec![Box::new(local), Box::new(deterministic)],
        dispatcher_config(4),
    );

    let first = runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    let notice = tokio::time::timeout(StdDuration::from_millis(100), local_notices.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(notice.occurrence_id, first.occurrences[0].id);
    assert_eq!(notice.occurrence_id.instrument.symbol.as_str(), "BTC-USDT");

    clock.set(timestamp(1));
    runtime
        .process(observation(2, timestamp(1), "106"))
        .await
        .unwrap();
    runtime.stop().await;

    assert_eq!(probe.deliveries().len(), 2);
    let status = runtime.notification_status();
    assert_eq!(status.failed, 2);
    assert_eq!(status.delivered, 2);
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(!body.contains("command"));
    assert!(!body.contains("powershell"));
    assert!(!body.contains("device path"));
    assert_eq!(
        decision_count(&records(&path), "price_alert_delivery_failed"),
        2
    );
    remove_file(&path);
}

#[tokio::test]
async fn one_panicking_adapter_is_isolated_from_the_evaluator_and_other_adapters() {
    let path = temp_path("alert-adapter-panic");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let entered = Arc::new(Notify::new());
    let panicking = PanickingAdapter {
        entered: Arc::clone(&entered),
    };
    let (healthy, probe) =
        DeterministicNotificationAdapter::new("healthy", VecDeque::new(), 4).unwrap();
    let mut runtime = runtime(
        &threshold_config(0),
        Arc::clone(&clock),
        &path,
        vec![Box::new(panicking), Box::new(healthy)],
        dispatcher_config(4),
    );

    runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    entered.notified().await;
    clock.set(timestamp(1));
    let report = runtime
        .process(observation(2, timestamp(1), "106"))
        .await
        .unwrap();
    assert!(report.notification_enqueues.iter().any(|enqueue| {
        enqueue.adapter_id == "panicking"
            && enqueue.state == NotificationEnqueueState::AdapterClosed
    }));

    runtime.stop().await;
    assert_eq!(probe.deliveries().len(), 2);
    assert_eq!(runtime.notification_status().worker_failures, 1);
    assert_eq!(
        decision_count(&records(&path), "price_alert_delivery_dropped"),
        1
    );
    remove_file(&path);
}

#[tokio::test]
async fn acknowledgement_is_exact_idempotent_and_cannot_cross_scope() {
    let path = temp_path("alert-ack");
    let clock = Arc::new(TestClock::new(timestamp(0)));
    let mut runtime = journal_only_runtime(&threshold_config(0), clock, &path);
    let report = runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    let occurrence = report.occurrences[0].id.clone();

    assert_eq!(
        runtime
            .acknowledge(&occurrence, timestamp(1))
            .await
            .unwrap(),
        AlertAcknowledgementOutcome::Recorded
    );
    assert_eq!(
        runtime
            .acknowledge(&occurrence, timestamp(2))
            .await
            .unwrap(),
        AlertAcknowledgementOutcome::AlreadyAcknowledged
    );
    let unknown = AlertOccurrenceId {
        instrument: crypto_trading_runtime::MarketInstrument::new(
            "binance",
            Symbol::new("ETH-USDT").unwrap(),
            MarketType::Spot,
        )
        .unwrap(),
        sequence: occurrence.sequence,
    };
    assert!(runtime.acknowledge(&unknown, timestamp(2)).await.is_err());

    runtime.stop().await;
    assert_eq!(
        decision_count(&records(&path), "price_alert_acknowledged"),
        1
    );
    remove_file(&path);
}

#[tokio::test]
async fn delivery_facts_use_the_injected_clock_and_invalid_status_cannot_recover() {
    let path = temp_path("alert-delivery-clock");
    let journal_id = "00000000-0000-0000-0000-000000000385".parse().unwrap();
    let clock = Arc::new(TestClock::new(timestamp(42)));
    let (adapter, _) =
        DeterministicNotificationAdapter::new("deterministic", VecDeque::from([Ok(())]), 2)
            .unwrap();
    let mut runtime = runtime(
        &threshold_config(0),
        Arc::clone(&clock),
        &path,
        vec![Box::new(adapter)],
        dispatcher_config(2),
    );

    runtime
        .process(observation(1, timestamp(0), "105"))
        .await
        .unwrap();
    runtime.stop().await;

    let mut records = records(&path);
    let delivered = records
        .iter_mut()
        .find(|record| record["decision"] == "price_alert_delivery_succeeded")
        .unwrap();
    let delivered_at = DateTime::parse_from_rfc3339(delivered["timestamp"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(delivered_at, timestamp(42));
    delivered["details"]["failure"] = serde_json::Value::String("timeout".to_owned());
    let body = records
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");
    std::fs::write(&path, format!("{body}\n")).unwrap();

    let source = FileJournalSnapshotSource::new(journal_id, &path).unwrap();
    let snapshot = source.snapshot().unwrap();
    let mut recovered = journal_only_runtime(&threshold_config(0), clock, &path);
    assert!(recovered.recover(&snapshot).is_err());
    assert!(recovered.recent_occurrences().is_empty());

    let delivered = records
        .iter_mut()
        .find(|record| record["decision"] == "price_alert_delivery_succeeded")
        .unwrap();
    delivered["details"]["failure"] = serde_json::Value::Null;
    let body = records
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");
    std::fs::write(&path, format!("{body}\n")).unwrap();
    let valid_snapshot = source.snapshot().unwrap();
    recovered.recover(&valid_snapshot).unwrap();
    assert_eq!(recovered.recent_occurrences().len(), 1);
    recovered.stop().await;
    remove_file(&path);
}

fn runtime(
    config: &PriceAlertConfig,
    clock: Arc<TestClock>,
    path: &std::path::Path,
    adapters: Vec<Box<dyn AlertNotificationAdapter>>,
    dispatcher: NotificationDispatcherConfig,
) -> PriceAlertRuntime {
    PriceAlertRuntime::new(
        config,
        MarketFreshnessPolicy::new(Duration::seconds(300), Duration::seconds(1)).unwrap(),
        clock,
        JsonlHistory::new(path),
        PriceAlertRuntimeConfig::new(AlertDeliveryMode::Dispatch, dispatcher, 32).unwrap(),
        adapters,
    )
    .unwrap()
}

fn journal_only_runtime(
    config: &PriceAlertConfig,
    clock: Arc<TestClock>,
    path: &std::path::Path,
) -> PriceAlertRuntime {
    PriceAlertRuntime::new(
        config,
        MarketFreshnessPolicy::new(Duration::seconds(300), Duration::seconds(1)).unwrap(),
        clock,
        JsonlHistory::new(path),
        PriceAlertRuntimeConfig::new(AlertDeliveryMode::JournalOnly, dispatcher_config(1), 32)
            .unwrap(),
        Vec::new(),
    )
    .unwrap()
}

fn dispatcher_config(capacity: usize) -> NotificationDispatcherConfig {
    NotificationDispatcherConfig::new(
        capacity,
        StdDuration::from_millis(100),
        StdDuration::from_millis(200),
    )
    .unwrap()
}

fn threshold_config(cooldown_seconds: u64) -> PriceAlertConfig {
    PriceAlertConfig {
        exchange: "binance".to_owned(),
        symbols: vec![PriceAlertSymbolConfig {
            symbol: Symbol::new("BTC-USDT").unwrap(),
            market_type: MarketType::Spot,
            enabled: true,
            volatility_alert: VolatilityAlertConfig {
                enabled: false,
                time_window_seconds: 60,
                threshold_percent: Decimal::ONE,
            },
            price_alert: PriceThresholdConfig {
                enabled: true,
                upper_price: Some(price("100")),
                lower_price: None,
            },
        }],
        refresh_interval_seconds: Decimal::ONE,
        cooldown_seconds,
    }
}

fn volatility_config(cooldown_seconds: u64) -> PriceAlertConfig {
    PriceAlertConfig {
        exchange: "binance".to_owned(),
        symbols: vec![PriceAlertSymbolConfig {
            symbol: Symbol::new("BTC-USDT").unwrap(),
            market_type: MarketType::Spot,
            enabled: true,
            volatility_alert: VolatilityAlertConfig {
                enabled: true,
                time_window_seconds: 60,
                threshold_percent: Decimal::from(4),
            },
            price_alert: PriceThresholdConfig {
                enabled: false,
                upper_price: None,
                lower_price: None,
            },
        }],
        refresh_interval_seconds: Decimal::from(10),
        cooldown_seconds,
    }
}

fn observation(revision: u64, at: DateTime<Utc>, value: &str) -> MarketDataEvent {
    let value = price(value);
    let mut snapshot = MarketSnapshot::new(
        "binance",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Spot,
        value,
        value,
        at,
    )
    .unwrap();
    snapshot.last = Some(value);
    MarketDataEvent::Observation(MarketDataObservation::new(snapshot, revision, at).unwrap())
}

fn records(path: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn decision_count(records: &[serde_json::Value], decision: &str) -> usize {
    records
        .iter()
        .filter(|record| record["decision"] == decision)
        .count()
}

fn price(value: &str) -> Price {
    Price::new(Decimal::from_str(value).unwrap()).unwrap()
}

fn timestamp(offset_seconds: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 24, 0, 0, 0).single().unwrap() + Duration::seconds(offset_seconds)
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
