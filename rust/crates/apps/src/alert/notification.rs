use std::{
    collections::{HashSet, VecDeque},
    fmt::{self, Debug},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::Price;
use crypto_trading_runtime::{JsonlHistory, MarketDataClock, MarketInstrument};
use crypto_trading_strategy::AlertKind;
use rust_decimal::Decimal;
use tokio::{
    runtime::Handle,
    sync::mpsc,
    task::{JoinError, JoinHandle},
    time::Instant,
};

use super::journal::{delivery_record, dropped_record};

const MAX_NOTIFICATION_ADAPTERS: usize = 8;
const MAX_NOTIFICATION_ADAPTER_ID_BYTES: usize = 64;
const MAX_NOTIFICATION_QUEUE_CAPACITY: usize = 4_096;
const MAX_NOTIFICATION_DURATION: Duration = Duration::from_secs(60);
const MAX_DETERMINISTIC_DELIVERIES: usize = 4_096;

/// Exact, stream-local identity of one durable alert occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertOccurrenceId {
    pub instrument: MarketInstrument,
    pub sequence: u64,
}

/// Safe typed fact presented to a notification adapter.
///
/// It deliberately contains no rendered text, command, path, credential, or
/// raw adapter payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertNotification {
    pub occurrence_id: AlertOccurrenceId,
    pub kind: AlertKind,
    pub price: Price,
    pub change_percent: Option<Decimal>,
    pub occurred_at: DateTime<Utc>,
}

/// Boxed adapter future used without an async-trait dependency.
pub type AlertNotificationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), NotificationFailure>> + Send + 'a>>;

/// Least-authority notification seam.
///
/// Implementations receive typed alert facts only. They have no market source,
/// order router, execution mode, or arbitrary command surface.
pub trait AlertNotificationAdapter: Debug + Send + 'static {
    fn adapter_id(&self) -> &str;
    fn deliver(&mut self, notification: AlertNotification) -> AlertNotificationFuture<'_>;
}

/// Stable, bounded delivery failure classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationFailure {
    DeviceUnavailable,
    Backpressure,
    Rejected,
}

impl NotificationFailure {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DeviceUnavailable => "device_unavailable",
            Self::Backpressure => "backpressure",
            Self::Rejected => "rejected",
        }
    }
}

/// Whether a notification is intentionally disabled or sent to bounded
/// adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertDeliveryMode {
    JournalOnly,
    Dispatch,
}

/// Fixed queue, timeout, and shutdown budgets for all adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotificationDispatcherConfig {
    queue_capacity: usize,
    delivery_timeout: Duration,
    shutdown_grace: Duration,
}

impl NotificationDispatcherConfig {
    /// Creates bounded non-zero dispatcher budgets.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationConfigError`] for zero or oversized values.
    pub fn new(
        queue_capacity: usize,
        delivery_timeout: Duration,
        shutdown_grace: Duration,
    ) -> Result<Self, NotificationConfigError> {
        if queue_capacity == 0 || queue_capacity > MAX_NOTIFICATION_QUEUE_CAPACITY {
            return Err(NotificationConfigError::InvalidQueueCapacity);
        }
        if delivery_timeout.is_zero() || delivery_timeout > MAX_NOTIFICATION_DURATION {
            return Err(NotificationConfigError::InvalidDeliveryTimeout);
        }
        if shutdown_grace.is_zero() || shutdown_grace > MAX_NOTIFICATION_DURATION {
            return Err(NotificationConfigError::InvalidShutdownGrace);
        }
        Ok(Self {
            queue_capacity,
            delivery_timeout,
            shutdown_grace,
        })
    }

    pub const fn queue_capacity(self) -> usize {
        self.queue_capacity
    }

    pub const fn delivery_timeout(self) -> Duration {
        self.delivery_timeout
    }

    pub const fn shutdown_grace(self) -> Duration {
        self.shutdown_grace
    }
}

/// Immediate result of enqueueing one occurrence for one adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationEnqueueState {
    Queued,
    Backpressure,
    AdapterClosed,
}

/// Adapter-specific enqueue fact returned to the evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationEnqueue {
    pub occurrence_id: AlertOccurrenceId,
    pub adapter_id: String,
    pub state: NotificationEnqueueState,
}

/// Bounded aggregate status; adapter error strings are never retained.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotificationDispatcherStatus {
    pub queued: u64,
    pub delivered: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub dropped: u64,
    pub adapter_closed: u64,
    pub status_persistence_failures: u64,
    pub worker_failures: u64,
}

/// Normal dispatcher shutdown result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationDispatcherExit {
    Drained,
    AbortedAfterGrace,
}

#[derive(Debug)]
struct NotificationWorker {
    adapter_id: String,
    sender: Option<mpsc::Sender<AlertNotification>>,
    join: Option<JoinHandle<()>>,
}

/// Owns one bounded sequential worker per notification adapter.
#[derive(Debug)]
pub(crate) struct NotificationDispatcher {
    mode: AlertDeliveryMode,
    config: NotificationDispatcherConfig,
    workers: Vec<NotificationWorker>,
    status: Arc<Mutex<NotificationDispatcherStatus>>,
    clock: Arc<dyn MarketDataClock>,
}

impl NotificationDispatcher {
    pub(crate) fn start(
        mode: AlertDeliveryMode,
        config: NotificationDispatcherConfig,
        history: &JsonlHistory,
        clock: Arc<dyn MarketDataClock>,
        adapters: Vec<Box<dyn AlertNotificationAdapter>>,
    ) -> Result<Self, NotificationConfigError> {
        match mode {
            AlertDeliveryMode::JournalOnly if !adapters.is_empty() => {
                return Err(NotificationConfigError::AdaptersForbiddenInJournalOnly);
            }
            AlertDeliveryMode::Dispatch if adapters.is_empty() => {
                return Err(NotificationConfigError::MissingAdapters);
            }
            _ => {}
        }
        if adapters.len() > MAX_NOTIFICATION_ADAPTERS {
            return Err(NotificationConfigError::TooManyAdapters);
        }
        let runtime_handle = if adapters.is_empty() {
            None
        } else {
            Some(Handle::try_current().map_err(|_| NotificationConfigError::MissingRuntime)?)
        };

        let mut adapter_ids = HashSet::with_capacity(adapters.len());
        for adapter in &adapters {
            validate_adapter_id(adapter.adapter_id())?;
            if !adapter_ids.insert(adapter.adapter_id().to_owned()) {
                return Err(NotificationConfigError::DuplicateAdapterId);
            }
        }

        let status = Arc::new(Mutex::new(NotificationDispatcherStatus::default()));
        let workers = match runtime_handle.as_ref() {
            Some(runtime_handle) => adapters
                .into_iter()
                .map(|adapter| {
                    start_worker(
                        adapter,
                        config,
                        history.clone(),
                        Arc::clone(&status),
                        Arc::clone(&clock),
                        runtime_handle,
                    )
                })
                .collect(),
            None => Vec::new(),
        };
        Ok(Self {
            mode,
            config,
            workers,
            status,
            clock,
        })
    }

    pub(crate) fn adapter_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.workers.iter().map(|worker| worker.adapter_id.as_str())
    }

    pub(crate) fn enqueue(&self, notification: &AlertNotification) -> Vec<NotificationEnqueue> {
        if self.mode == AlertDeliveryMode::JournalOnly {
            return Vec::new();
        }
        self.workers
            .iter()
            .map(|worker| {
                let state = match worker.sender.as_ref() {
                    Some(sender) => match sender.try_send(notification.clone()) {
                        Ok(()) => {
                            update_status(&self.status, |status| {
                                status.queued = status.queued.saturating_add(1);
                            });
                            NotificationEnqueueState::Queued
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            update_status(&self.status, |status| {
                                status.dropped = status.dropped.saturating_add(1);
                            });
                            NotificationEnqueueState::Backpressure
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            update_status(&self.status, |status| {
                                status.adapter_closed = status.adapter_closed.saturating_add(1);
                            });
                            NotificationEnqueueState::AdapterClosed
                        }
                    },
                    None => NotificationEnqueueState::AdapterClosed,
                };
                NotificationEnqueue {
                    occurrence_id: notification.occurrence_id.clone(),
                    adapter_id: worker.adapter_id.clone(),
                    state,
                }
            })
            .collect()
    }

    pub(crate) async fn persist_enqueue_failures(
        &self,
        history: &JsonlHistory,
        notification: &AlertNotification,
        enqueues: &[NotificationEnqueue],
    ) -> u64 {
        let records = enqueues
            .iter()
            .filter_map(|enqueue| match enqueue.state {
                NotificationEnqueueState::Queued => None,
                NotificationEnqueueState::Backpressure => Some(dropped_record(
                    notification,
                    &enqueue.adapter_id,
                    "backpressure",
                    self.clock.now(),
                )),
                NotificationEnqueueState::AdapterClosed => Some(dropped_record(
                    notification,
                    &enqueue.adapter_id,
                    "adapter_closed",
                    self.clock.now(),
                )),
            })
            .collect::<Vec<_>>();
        if records.is_empty() {
            return 0;
        }
        if history.append_batch(&records).await.is_ok() {
            0
        } else {
            let failures = u64::try_from(records.len()).unwrap_or(u64::MAX);
            update_status(&self.status, |status| {
                status.status_persistence_failures =
                    status.status_persistence_failures.saturating_add(failures);
            });
            failures
        }
    }

    pub(crate) fn status(&self) -> NotificationDispatcherStatus {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) async fn stop(&mut self) -> NotificationDispatcherExit {
        for worker in &mut self.workers {
            worker.sender.take();
        }
        let deadline = Instant::now() + self.config.shutdown_grace;
        let mut aborted = false;
        for worker in &mut self.workers {
            let Some(mut join) = worker.join.take() else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                join.abort();
                let _ = join.await;
                aborted = true;
                continue;
            }
            if let Ok(result) = tokio::time::timeout(remaining, &mut join).await {
                record_join_result(&self.status, &result);
            } else {
                join.abort();
                let _ = join.await;
                aborted = true;
            }
        }
        if aborted {
            NotificationDispatcherExit::AbortedAfterGrace
        } else {
            NotificationDispatcherExit::Drained
        }
    }
}

impl Drop for NotificationDispatcher {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            worker.sender.take();
            if let Some(join) = worker.join.take() {
                join.abort();
            }
        }
    }
}

fn start_worker(
    mut adapter: Box<dyn AlertNotificationAdapter>,
    config: NotificationDispatcherConfig,
    history: JsonlHistory,
    status: Arc<Mutex<NotificationDispatcherStatus>>,
    clock: Arc<dyn MarketDataClock>,
    runtime_handle: &Handle,
) -> NotificationWorker {
    let adapter_id = adapter.adapter_id().to_owned();
    let worker_adapter_id = adapter_id.clone();
    let (sender, mut receiver) = mpsc::channel::<AlertNotification>(config.queue_capacity);
    let join = runtime_handle.spawn(async move {
        while let Some(notification) = receiver.recv().await {
            let result = tokio::time::timeout(
                config.delivery_timeout,
                adapter.deliver(notification.clone()),
            )
            .await;
            let (decision, failure) = match result {
                Ok(Ok(())) => {
                    update_status(&status, |status| {
                        status.delivered = status.delivered.saturating_add(1);
                    });
                    ("price_alert_delivery_succeeded", None)
                }
                Ok(Err(error)) => {
                    update_status(&status, |status| {
                        status.failed = status.failed.saturating_add(1);
                    });
                    ("price_alert_delivery_failed", Some(error.as_str()))
                }
                Err(_) => {
                    update_status(&status, |status| {
                        status.timed_out = status.timed_out.saturating_add(1);
                    });
                    ("price_alert_delivery_timed_out", Some("timeout"))
                }
            };
            let record = delivery_record(
                &notification,
                &worker_adapter_id,
                decision,
                failure,
                clock.now(),
            );
            if history.append(&record).await.is_err() {
                update_status(&status, |status| {
                    status.status_persistence_failures =
                        status.status_persistence_failures.saturating_add(1);
                });
            }
        }
    });
    NotificationWorker {
        adapter_id,
        sender: Some(sender),
        join: Some(join),
    }
}

fn update_status(
    status: &Arc<Mutex<NotificationDispatcherStatus>>,
    update: impl FnOnce(&mut NotificationDispatcherStatus),
) {
    let mut status = status
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    update(&mut status);
}

fn record_join_result(
    status: &Arc<Mutex<NotificationDispatcherStatus>>,
    result: &Result<(), JoinError>,
) {
    if result.is_err() {
        update_status(status, |status| {
            status.worker_failures = status.worker_failures.saturating_add(1);
        });
    }
}

pub(crate) fn validate_adapter_id(adapter_id: &str) -> Result<(), NotificationConfigError> {
    if adapter_id.is_empty()
        || adapter_id.len() > MAX_NOTIFICATION_ADAPTER_ID_BYTES
        || !adapter_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-.".contains(&byte)
        })
    {
        return Err(NotificationConfigError::InvalidAdapterId);
    }
    Ok(())
}

/// A bounded in-process local notification adapter.
///
/// Consumers receive typed facts through [`LocalNoticeReceiver`]. No terminal
/// escape, shell, subprocess, file path, or rendered text crosses this seam.
#[derive(Debug)]
pub struct LocalNoticeNotificationAdapter {
    sender: mpsc::Sender<AlertNotification>,
}

impl LocalNoticeNotificationAdapter {
    /// Creates one bounded local notice channel.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationConfigError`] for an invalid capacity.
    pub fn channel(
        capacity: usize,
    ) -> Result<(Self, LocalNoticeReceiver), NotificationConfigError> {
        if capacity == 0 || capacity > MAX_NOTIFICATION_QUEUE_CAPACITY {
            return Err(NotificationConfigError::InvalidQueueCapacity);
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((Self { sender }, LocalNoticeReceiver { receiver }))
    }
}

impl AlertNotificationAdapter for LocalNoticeNotificationAdapter {
    fn adapter_id(&self) -> &'static str {
        "local_notice"
    }

    fn deliver(&mut self, notification: AlertNotification) -> AlertNotificationFuture<'_> {
        Box::pin(async move {
            self.sender
                .try_send(notification)
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => NotificationFailure::Backpressure,
                    mpsc::error::TrySendError::Closed(_) => NotificationFailure::DeviceUnavailable,
                })
        })
    }
}

/// Receiving half of [`LocalNoticeNotificationAdapter`].
#[derive(Debug)]
pub struct LocalNoticeReceiver {
    receiver: mpsc::Receiver<AlertNotification>,
}

impl LocalNoticeReceiver {
    pub async fn recv(&mut self) -> Option<AlertNotification> {
        self.receiver.recv().await
    }
}

/// Read-only handle for deterministic delivery assertions.
#[derive(Debug, Clone)]
pub struct DeterministicNotificationProbe {
    deliveries: Arc<Mutex<Vec<AlertNotification>>>,
}

impl DeterministicNotificationProbe {
    pub fn deliveries(&self) -> Vec<AlertNotification> {
        self.deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Scripted bounded adapter for deterministic/offline use.
#[derive(Debug)]
pub struct DeterministicNotificationAdapter {
    adapter_id: String,
    outcomes: VecDeque<Result<(), NotificationFailure>>,
    deliveries: Arc<Mutex<Vec<AlertNotification>>>,
    capacity: usize,
}

impl DeterministicNotificationAdapter {
    /// Creates a deterministic adapter and its read-only probe.
    ///
    /// # Errors
    ///
    /// Returns [`NotificationConfigError`] for an invalid adapter ID or
    /// delivery capacity.
    pub fn new(
        adapter_id: impl Into<String>,
        outcomes: VecDeque<Result<(), NotificationFailure>>,
        capacity: usize,
    ) -> Result<(Self, DeterministicNotificationProbe), NotificationConfigError> {
        let adapter_id = adapter_id.into();
        validate_adapter_id(&adapter_id)?;
        if capacity == 0 || capacity > MAX_DETERMINISTIC_DELIVERIES {
            return Err(NotificationConfigError::InvalidDeterministicCapacity);
        }
        let deliveries = Arc::new(Mutex::new(Vec::new()));
        Ok((
            Self {
                adapter_id,
                outcomes,
                deliveries: Arc::clone(&deliveries),
                capacity,
            },
            DeterministicNotificationProbe { deliveries },
        ))
    }
}

impl AlertNotificationAdapter for DeterministicNotificationAdapter {
    fn adapter_id(&self) -> &str {
        &self.adapter_id
    }

    fn deliver(&mut self, notification: AlertNotification) -> AlertNotificationFuture<'_> {
        Box::pin(async move {
            {
                let mut deliveries = self
                    .deliveries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if deliveries.len() >= self.capacity {
                    return Err(NotificationFailure::Backpressure);
                }
                deliveries.push(notification);
            }
            self.outcomes.pop_front().unwrap_or(Ok(()))
        })
    }
}

/// Construction errors for bounded notification surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationConfigError {
    InvalidQueueCapacity,
    InvalidDeliveryTimeout,
    InvalidShutdownGrace,
    MissingRuntime,
    TooManyAdapters,
    MissingAdapters,
    AdaptersForbiddenInJournalOnly,
    InvalidAdapterId,
    DuplicateAdapterId,
    InvalidDeterministicCapacity,
}

impl fmt::Display for NotificationConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidQueueCapacity => "invalid notification queue capacity",
            Self::InvalidDeliveryTimeout => "invalid notification delivery timeout",
            Self::InvalidShutdownGrace => "invalid notification shutdown grace",
            Self::MissingRuntime => "notification dispatch requires a Tokio runtime",
            Self::TooManyAdapters => "too many notification adapters",
            Self::MissingAdapters => "dispatch mode requires at least one notification adapter",
            Self::AdaptersForbiddenInJournalOnly => {
                "journal-only mode must not receive notification adapters"
            }
            Self::InvalidAdapterId => "invalid notification adapter identifier",
            Self::DuplicateAdapterId => "duplicate notification adapter identifier",
            Self::InvalidDeterministicCapacity => {
                "invalid deterministic notification delivery capacity"
            }
        })
    }
}

impl std::error::Error for NotificationConfigError {}
