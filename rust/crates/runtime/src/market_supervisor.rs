use std::{fmt::Debug, future::Future, pin::Pin, time::Duration};

use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};
use uuid::Uuid;

use crate::market_data::{
    MarketDataError, MarketDataEvent, SubscriptionMarketDataAdapter, validated_exchange,
};

/// Version of the stable, non-secret supervisor status view.
pub const MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION: u16 = 1;

const MAX_SHUTDOWN_GRACE: Duration = Duration::from_secs(60);

/// Boxed future returned by one read-only market source.
pub type MarketDataEventFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<MarketDataEvent>, MarketDataError>> + Send + 'a>>;

/// Source-neutral seam consumed by the long-lived read-only supervisor.
///
/// Implementations translate remote or in-process source outcomes into the
/// stable [`MarketDataEvent`] contract. Recoverable venue failures are events;
/// only contract violations are returned as errors.
pub trait MarketDataEventSource: Debug + Send + 'static {
    /// Stable source identity used for continuity gaps.
    fn source_id(&self) -> &str;

    /// Produces the next event, or `None` when a finite source is exhausted.
    fn next_event(&mut self) -> MarketDataEventFuture<'_>;
}

impl MarketDataEventSource for SubscriptionMarketDataAdapter {
    fn source_id(&self) -> &str {
        self.exchange()
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        Box::pin(async move {
            SubscriptionMarketDataAdapter::next_event(self)
                .await
                .map(Some)
        })
    }
}

/// Lifecycle phase of one supervised read-only task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSupervisorPhase {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

/// Latest source health inferred from delivered market events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSupervisorHealth {
    Unknown,
    Healthy,
    Degraded,
}

/// Normal terminal reason for a supervised read-only task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSupervisorExit {
    StopRequested,
    SourceEnded,
    ShutdownTimedOut,
}

/// Stable, non-secret supervisor status suitable for a future operator read
/// model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketSupervisorStatus {
    pub schema_version: u16,
    pub task_id: Uuid,
    pub source_id: String,
    pub phase: MarketSupervisorPhase,
    pub health: MarketSupervisorHealth,
    pub event_sequence: u64,
    pub consecutive_source_failures: u32,
    pub last_event_at: Option<DateTime<Utc>>,
    pub exit: Option<MarketSupervisorExit>,
}

impl MarketSupervisorStatus {
    fn starting(task_id: Uuid, source_id: String) -> Self {
        Self {
            schema_version: MARKET_SUPERVISOR_STATUS_SCHEMA_VERSION,
            task_id,
            source_id,
            phase: MarketSupervisorPhase::Starting,
            health: MarketSupervisorHealth::Unknown,
            event_sequence: 0,
            consecutive_source_failures: 0,
            last_event_at: None,
            exit: None,
        }
    }

    fn observe(&mut self, event: &MarketDataEvent) {
        self.last_event_at = Some(event.observed_at());
        match event {
            MarketDataEvent::Observation(_) => {
                self.health = MarketSupervisorHealth::Healthy;
                self.consecutive_source_failures = 0;
            }
            MarketDataEvent::SourceGap { .. } => {
                self.health = MarketSupervisorHealth::Degraded;
            }
            MarketDataEvent::SourceUnavailable { .. } => {
                self.health = MarketSupervisorHealth::Degraded;
                self.consecutive_source_failures =
                    self.consecutive_source_failures.saturating_add(1);
            }
        }
    }
}

/// Bounded shutdown behavior for one supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSupervisorConfig {
    shutdown_grace: Duration,
}

impl MarketSupervisorConfig {
    /// Creates a positive shutdown grace no longer than one minute.
    ///
    /// # Errors
    ///
    /// Returns [`MarketSupervisorError::InvalidConfig`] for an invalid grace.
    pub fn new(shutdown_grace: Duration) -> Result<Self, MarketSupervisorError> {
        if shutdown_grace.is_zero() {
            return Err(MarketSupervisorError::InvalidConfig(
                "shutdown grace must be positive",
            ));
        }
        if shutdown_grace > MAX_SHUTDOWN_GRACE {
            return Err(MarketSupervisorError::InvalidConfig(
                "shutdown grace exceeds the one-minute hard limit",
            ));
        }
        Ok(Self { shutdown_grace })
    }

    pub const fn shutdown_grace(self) -> Duration {
        self.shutdown_grace
    }
}

impl Default for MarketSupervisorConfig {
    fn default() -> Self {
        Self {
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone)]
struct SequencedMarketEvent {
    sequence: u64,
    event: MarketDataEvent,
}

type SupervisorTaskResult = Result<MarketSupervisorExit, MarketSupervisorError>;

/// Single-owner interface for a bounded, cancellable, read-only source task.
///
/// Event retention is O(1): a slow consumer first receives an explicit source
/// gap and then the latest retained event. Dropping the supervisor requests
/// cancellation; [`Self::stop`] additionally waits for graceful completion.
#[derive(Debug)]
pub struct MarketSupervisor {
    source_id: String,
    config: MarketSupervisorConfig,
    stop: watch::Sender<bool>,
    status_sender: watch::Sender<MarketSupervisorStatus>,
    status: watch::Receiver<MarketSupervisorStatus>,
    events: watch::Receiver<Option<SequencedMarketEvent>>,
    join: Option<JoinHandle<SupervisorTaskResult>>,
    last_event_sequence: u64,
    pending_event: Option<MarketDataEvent>,
    completion: Option<SupervisorTaskResult>,
}

impl MarketSupervisor {
    /// Starts one source task on the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`MarketSupervisorError::SourceContract`] if the source identity
    /// is invalid.
    pub fn start<S>(
        task_id: Uuid,
        mut source: S,
        config: MarketSupervisorConfig,
    ) -> Result<Self, MarketSupervisorError>
    where
        S: MarketDataEventSource,
    {
        let source_id = validated_exchange(source.source_id())
            .map_err(MarketSupervisorError::SourceContract)?;
        let initial = MarketSupervisorStatus::starting(task_id, source_id.clone());
        let (stop, mut stop_receiver) = watch::channel(false);
        let (status_sender, status) = watch::channel(initial.clone());
        let task_status_sender = status_sender.clone();
        let (event_sender, events) = watch::channel(None);
        let task_source_id = source_id.clone();
        let join = tokio::spawn(async move {
            let mut current = initial;
            current.phase = MarketSupervisorPhase::Running;
            task_status_sender.send_replace(current.clone());

            loop {
                tokio::select! {
                    biased;
                    stop_result = stop_receiver.changed() => {
                        if stop_result.is_err() || *stop_receiver.borrow_and_update() {
                            return Ok(finish_supervisor(
                                &task_status_sender,
                                current,
                                MarketSupervisorExit::StopRequested,
                            ));
                        }
                    }
                    source_result = source.next_event() => {
                        match source_result {
                            Ok(Some(event)) => {
                                if event.exchange() != task_source_id {
                                    current.phase = MarketSupervisorPhase::Failed;
                                    task_status_sender.send_replace(current);
                                    return Err(MarketSupervisorError::SourceContract(
                                        MarketDataError::SourceIdentityMismatch {
                                            expected: task_source_id.clone(),
                                            actual: event.exchange().to_owned(),
                                        },
                                    ));
                                }
                                let Some(sequence) = current.event_sequence.checked_add(1) else {
                                    current.phase = MarketSupervisorPhase::Failed;
                                    task_status_sender.send_replace(current);
                                    return Err(MarketSupervisorError::EventSequenceExhausted);
                                };
                                current.event_sequence = sequence;
                                current.observe(&event);
                                event_sender.send_replace(Some(SequencedMarketEvent {
                                    sequence: current.event_sequence,
                                    event,
                                }));
                                task_status_sender.send_replace(current.clone());
                            }
                            Ok(None) => {
                                return Ok(finish_supervisor(
                                    &task_status_sender,
                                    current,
                                    MarketSupervisorExit::SourceEnded,
                                ));
                            }
                            Err(error) => {
                                current.phase = MarketSupervisorPhase::Failed;
                                task_status_sender.send_replace(current);
                                return Err(MarketSupervisorError::SourceContract(error));
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            source_id,
            config,
            stop,
            status_sender,
            status,
            events,
            join: Some(join),
            last_event_sequence: 0,
            pending_event: None,
            completion: None,
        })
    }

    /// Returns the latest stable task status without waiting.
    pub fn status(&self) -> MarketSupervisorStatus {
        self.status.borrow().clone()
    }

    /// Waits for the next source event.
    ///
    /// A slow caller receives a source-gap event before the latest retained
    /// source event. `None` means the source ended normally.
    ///
    /// # Errors
    ///
    /// Returns [`MarketSupervisorError`] if the source violated its contract or
    /// the task panicked/cancelled.
    pub async fn next_event(&mut self) -> Result<Option<MarketDataEvent>, MarketSupervisorError> {
        if let Some(event) = self.pending_event.take() {
            return Ok(Some(event));
        }
        loop {
            if self.events.changed().await.is_err() {
                return match self.settle_join().await? {
                    MarketSupervisorExit::SourceEnded
                    | MarketSupervisorExit::StopRequested
                    | MarketSupervisorExit::ShutdownTimedOut => Ok(None),
                };
            }
            let Some(envelope) = self.events.borrow_and_update().clone() else {
                continue;
            };
            if envelope.sequence <= self.last_event_sequence {
                continue;
            }
            let skipped = envelope
                .sequence
                .saturating_sub(self.last_event_sequence)
                .saturating_sub(1);
            self.last_event_sequence = envelope.sequence;
            if skipped > 0 {
                let observed_at = envelope.event.observed_at();
                self.pending_event = Some(envelope.event);
                return MarketDataEvent::source_gap(&self.source_id, skipped, observed_at)
                    .map(Some)
                    .map_err(MarketSupervisorError::SourceContract);
            }
            return Ok(Some(envelope.event));
        }
    }

    /// Requests cancellation and waits for bounded graceful shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`MarketSupervisorError`] if the task panicked or its source
    /// contract failed before shutdown.
    pub async fn stop(&mut self) -> Result<MarketSupervisorExit, MarketSupervisorError> {
        if let Some(completion) = &self.completion {
            return completion.clone();
        }
        let _ = self.stop.send(true);
        let Some(mut join) = self.join.take() else {
            return self
                .completion
                .clone()
                .unwrap_or(Err(MarketSupervisorError::TaskCancelled));
        };
        if let Ok(result) = tokio::time::timeout(self.config.shutdown_grace, &mut join).await {
            self.store_join_result(result)
        } else {
            join.abort();
            let _ = join.await;
            let exit = MarketSupervisorExit::ShutdownTimedOut;
            let mut status = self.status();
            status.phase = MarketSupervisorPhase::Stopped;
            status.exit = Some(exit);
            self.status_sender.send_replace(status);
            self.completion = Some(Ok(exit));
            Ok(exit)
        }
    }

    async fn settle_join(&mut self) -> Result<MarketSupervisorExit, MarketSupervisorError> {
        if let Some(completion) = &self.completion {
            return completion.clone();
        }
        let Some(join) = self.join.take() else {
            return Err(MarketSupervisorError::TaskCancelled);
        };
        self.store_join_result(join.await)
    }

    fn store_join_result(
        &mut self,
        result: Result<SupervisorTaskResult, JoinError>,
    ) -> Result<MarketSupervisorExit, MarketSupervisorError> {
        let completion = match result {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(MarketSupervisorError::TaskPanicked),
            Err(_) => Err(MarketSupervisorError::TaskCancelled),
        };
        self.completion = Some(completion.clone());
        completion
    }
}

impl Drop for MarketSupervisor {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

fn finish_supervisor(
    status_sender: &watch::Sender<MarketSupervisorStatus>,
    mut status: MarketSupervisorStatus,
    exit: MarketSupervisorExit,
) -> MarketSupervisorExit {
    status.phase = MarketSupervisorPhase::Stopping;
    status_sender.send_replace(status.clone());
    status.phase = MarketSupervisorPhase::Stopped;
    status.exit = Some(exit);
    status_sender.send_replace(status);
    exit
}

/// Construction, source-contract, and task-lifecycle failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MarketSupervisorError {
    #[error("invalid market supervisor configuration: {0}")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    SourceContract(#[from] MarketDataError),
    #[error("market supervisor event sequence is exhausted")]
    EventSequenceExhausted,
    #[error("market supervisor task panicked")]
    TaskPanicked,
    #[error("market supervisor task was cancelled unexpectedly")]
    TaskCancelled,
}
