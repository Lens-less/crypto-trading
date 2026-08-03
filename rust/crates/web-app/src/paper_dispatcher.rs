use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use chrono::Utc;
use crypto_trading_cli::{
    ArbitragePaperTaskError, GridPaperTaskError, PaperProfileCatalog, PaperProfileError,
    StartedPaperTask,
};
use crypto_trading_control_plane::{
    SubmitCommand, SubmitDispatchFuture, SubmitDispatchOutcome, SubmitDispatcher, SubmitEnvelope,
};
use crypto_trading_runtime::{AccountRiskAuthority, AccountRiskError, JsonlHistory};
use tokio::{
    sync::{Mutex, Notify, RwLock, watch},
    task::JoinSet,
};
use uuid::Uuid;

#[derive(Clone)]
pub struct TrustedPaperSubmitDispatcher {
    journal_id: Uuid,
    history_path: Arc<PathBuf>,
    catalog: Arc<PaperProfileCatalog>,
    registry: Arc<PaperTaskRegistry>,
    account_risk: Option<Arc<AccountRiskAuthority>>,
    degraded_logged: Arc<AtomicBool>,
}

impl TrustedPaperSubmitDispatcher {
    #[must_use]
    pub fn new(journal_id: Uuid, history_path: PathBuf, catalog: PaperProfileCatalog) -> Self {
        let registry = PaperTaskRegistry::new(catalog.task_ids());
        let degraded_logged = Arc::new(AtomicBool::new(false));
        let account_risk = if let Ok(authority) = AccountRiskAuthority::new(
            journal_id,
            JsonlHistory::new(&history_path),
            PaperProfileCatalog::account_risk_scope(),
            catalog.account_risk_policy().clone(),
        ) {
            Some(Arc::new(authority))
        } else {
            degraded_logged.store(true, Ordering::Release);
            tracing::error!(
                event = "account_risk_authority_unavailable",
                "paper account-risk authority failed closed during startup"
            );
            None
        };
        Self {
            journal_id,
            history_path: Arc::new(history_path),
            catalog: Arc::new(catalog),
            registry: Arc::new(registry),
            account_risk,
            degraded_logged,
        }
    }

    async fn dispatch_command(&self, envelope: SubmitEnvelope) -> SubmitDispatchOutcome {
        let command = command_kind(envelope.command());
        let task_id = envelope.target_task_id().to_owned();
        let outcome = match envelope.command() {
            SubmitCommand::StartPaperGrid { .. } | SubmitCommand::StartPaperArbitrage { .. } => {
                self.dispatch_start(envelope).await
            }
            SubmitCommand::StopTask => {
                self.registry
                    .control(envelope.target_task_id(), TaskControl::Stop)
                    .await
            }
            SubmitCommand::CancelTask => {
                self.registry
                    .control(envelope.target_task_id(), TaskControl::Cancel)
                    .await
            }
            SubmitCommand::PauseAccountRisk { reason } => {
                let reason = reason.clone();
                self.dispatch_account_risk(move |authority| async move {
                    authority.pause(&reason, Utc::now()).await.map(|_| ())
                })
                .await
            }
            SubmitCommand::ResumeAccountRisk => {
                self.dispatch_account_risk(|authority| async move {
                    authority.resume(Utc::now()).await.map(|_| ())
                })
                .await
            }
            SubmitCommand::EngageAccountKillSwitch { reason } => {
                let reason = reason.clone();
                self.dispatch_account_risk(move |authority| async move {
                    authority
                        .engage_kill_switch(&reason, Utc::now())
                        .await
                        .map(|_| ())
                })
                .await
            }
            SubmitCommand::ReconcileRelease { .. }
            | SubmitCommand::RecordReconcileFailure { .. } => SubmitDispatchOutcome::Rejected,
        };
        tracing::info!(
            event = "paper_command_completed",
            command,
            task_id,
            outcome = dispatch_outcome_name(outcome),
            "trusted paper command reached a bounded outcome"
        );
        outcome
    }

    async fn dispatch_account_risk<F, Fut>(&self, action: F) -> SubmitDispatchOutcome
    where
        F: FnOnce(Arc<AccountRiskAuthority>) -> Fut,
        Fut: Future<Output = Result<(), AccountRiskError>>,
    {
        let Some(authority) = self.account_risk.clone() else {
            self.log_degraded_once("account_risk_authority_unavailable");
            return SubmitDispatchOutcome::Rejected;
        };
        match action(authority).await {
            Ok(()) => SubmitDispatchOutcome::Applied,
            Err(AccountRiskError::DegradedState) => {
                self.log_degraded_once("account_risk_degraded");
                SubmitDispatchOutcome::Rejected
            }
            Err(AccountRiskError::InvalidConfig(_) | AccountRiskError::InvalidRequest(_)) => {
                SubmitDispatchOutcome::Rejected
            }
            Err(_) => {
                self.log_degraded_once("account_risk_outcome_unknown");
                SubmitDispatchOutcome::OutcomeUnknown
            }
        }
    }

    fn log_degraded_once(&self, component: &'static str) {
        if !self.degraded_logged.swap(true, Ordering::AcqRel) {
            tracing::error!(
                event = "paper_authority_degraded",
                component,
                "paper authority entered a fail-closed degraded state"
            );
        }
    }

    async fn dispatch_start(&self, envelope: SubmitEnvelope) -> SubmitDispatchOutcome {
        let catalog = Arc::clone(&self.catalog);
        let history_path = Arc::clone(&self.history_path);
        let journal_id = self.journal_id;
        let task_id = envelope.target_task_id().to_owned();
        self.registry
            .start(&task_id, move || async move {
                catalog
                    .start_matching(journal_id, history_path.as_path(), &envelope)
                    .await
            })
            .await
    }

    /// Quiesces command admission and durably stops every running paper owner.
    pub async fn shutdown(&self) -> SubmitDispatchOutcome {
        self.registry.shutdown().await
    }
}

impl SubmitDispatcher for TrustedPaperSubmitDispatcher {
    fn dispatch(&self, envelope: SubmitEnvelope) -> SubmitDispatchFuture {
        let dispatcher = self.clone();
        Box::pin(async move { dispatcher.dispatch_command(envelope).await })
    }
}

#[derive(Clone, Copy)]
enum TaskControl {
    Stop,
    Cancel,
}

struct PaperTaskRegistry {
    slots: HashMap<String, Arc<PaperTaskSlot>>,
    accepting_commands: RwLock<bool>,
    shutdown_result: watch::Sender<Option<SubmitDispatchOutcome>>,
}

struct PaperTaskSlot {
    state: Mutex<TaskSlot>,
    changed: Arc<Notify>,
}

impl PaperTaskRegistry {
    fn new(task_ids: Vec<String>) -> Self {
        let slots = task_ids
            .into_iter()
            .map(|task_id| {
                (
                    task_id,
                    Arc::new(PaperTaskSlot {
                        state: Mutex::new(TaskSlot::Vacant(None)),
                        changed: Arc::new(Notify::new()),
                    }),
                )
            })
            .collect();
        let (shutdown_result, _) = watch::channel(None);
        Self {
            slots,
            accepting_commands: RwLock::new(true),
            shutdown_result,
        }
    }

    async fn start<F, Fut>(&self, task_id: &str, starter: F) -> SubmitDispatchOutcome
    where
        F: FnOnce() -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<StartedPaperTask, PaperProfileError>> + Send + 'static,
    {
        let accepting_commands = self.accepting_commands.read().await;
        if !*accepting_commands {
            return SubmitDispatchOutcome::Rejected;
        }
        let Some(slot) = self.slots.get(task_id).cloned() else {
            return SubmitDispatchOutcome::Rejected;
        };
        {
            let mut state = slot.state.lock().await;
            match &*state {
                TaskSlot::Vacant(_) => *state = TaskSlot::Starting,
                TaskSlot::Running(task) if is_terminal(task) => *state = TaskSlot::Starting,
                TaskSlot::Starting | TaskSlot::Stopping | TaskSlot::Running(_) => {
                    return SubmitDispatchOutcome::Rejected;
                }
            }
        }
        drop(accepting_commands);

        let (result_sender, result_receiver) = watch::channel(None);
        spawn_detached(async move {
            let attempted = tokio::spawn(async move { starter().await }).await;
            let (next_state, outcome) = match attempted {
                Ok(Ok(task)) => (
                    TaskSlot::Running(Box::new(task)),
                    SubmitDispatchOutcome::Applied,
                ),
                Ok(Err(error)) if error.is_rejected() => {
                    (TaskSlot::Vacant(None), SubmitDispatchOutcome::Rejected)
                }
                Ok(Err(_)) | Err(_) => (
                    TaskSlot::Vacant(None),
                    SubmitDispatchOutcome::OutcomeUnknown,
                ),
            };
            {
                let mut state = slot.state.lock().await;
                *state = next_state;
            }
            result_sender.send_replace(Some(outcome));
            slot.changed.notify_one();
        });
        wait_for_dispatch_outcome(result_receiver).await
    }

    async fn control(&self, task_id: &str, control: TaskControl) -> SubmitDispatchOutcome {
        let accepting_commands = self.accepting_commands.read().await;
        if !*accepting_commands {
            return SubmitDispatchOutcome::Rejected;
        }
        let Some(slot) = self.slots.get(task_id).cloned() else {
            return SubmitDispatchOutcome::Rejected;
        };
        let task = {
            let mut state = slot.state.lock().await;
            match &*state {
                TaskSlot::Vacant(_) | TaskSlot::Starting | TaskSlot::Stopping => {
                    return SubmitDispatchOutcome::Rejected;
                }
                TaskSlot::Running(task) if is_terminal(task) => {
                    *state = TaskSlot::Vacant(None);
                    return SubmitDispatchOutcome::Rejected;
                }
                TaskSlot::Running(_) => {}
            }
            let TaskSlot::Running(task) = std::mem::replace(&mut *state, TaskSlot::Stopping) else {
                unreachable!();
            };
            task
        };
        drop(accepting_commands);

        let (result_sender, result_receiver) = watch::channel(None);
        spawn_detached(async move {
            let attempted = tokio::spawn(async move {
                match control {
                    TaskControl::Stop => stop_task(*task).await,
                    TaskControl::Cancel => cancel_task(*task).await,
                }
            })
            .await;
            let outcome = attempted.unwrap_or(SubmitDispatchOutcome::OutcomeUnknown);
            {
                let mut state = slot.state.lock().await;
                *state = TaskSlot::Vacant(Some(outcome));
            }
            result_sender.send_replace(Some(outcome));
            slot.changed.notify_one();
        });
        wait_for_dispatch_outcome(result_receiver).await
    }

    async fn shutdown(self: &Arc<Self>) -> SubmitDispatchOutcome {
        let mut result = self.shutdown_result.subscribe();
        let should_launch = {
            let mut accepting_commands = self.accepting_commands.write().await;
            if *accepting_commands {
                *accepting_commands = false;
                true
            } else {
                false
            }
        };
        if should_launch {
            tracing::info!(
                event = "paper_registry_quiescing",
                owner_count = self.slots.len(),
                "paper command admission is closed and owner shutdown has begun"
            );
            let registry = Arc::clone(self);
            spawn_detached(async move {
                let worker = Arc::clone(&registry);
                let outcome = tokio::spawn(async move { worker.stop_all().await })
                    .await
                    .unwrap_or(SubmitDispatchOutcome::OutcomeUnknown);
                registry.shutdown_result.send_replace(Some(outcome));
                tracing::info!(
                    event = "paper_registry_shutdown_completed",
                    outcome = dispatch_outcome_name(outcome),
                    "paper owner shutdown reached a bounded outcome"
                );
            });
        }

        loop {
            if let Some(outcome) = *result.borrow() {
                return outcome;
            }
            if result.changed().await.is_err() {
                return SubmitDispatchOutcome::OutcomeUnknown;
            }
        }
    }

    async fn stop_all(&self) -> SubmitDispatchOutcome {
        let mut overall = SubmitDispatchOutcome::Applied;
        loop {
            let mut stop_jobs = JoinSet::new();
            let mut transition_waiters = JoinSet::new();
            for slot in self.slots.values() {
                let mut state = slot.state.lock().await;
                match &mut *state {
                    TaskSlot::Vacant(previous_control) => {
                        if let Some(outcome) = previous_control.take() {
                            overall = combine_shutdown_outcomes(overall, outcome);
                        }
                        continue;
                    }
                    TaskSlot::Running(task) if is_terminal(task) => {
                        *state = TaskSlot::Vacant(None);
                        slot.changed.notify_one();
                        continue;
                    }
                    TaskSlot::Running(_) => {}
                    TaskSlot::Starting | TaskSlot::Stopping => {
                        let notified = Arc::clone(&slot.changed).notified_owned();
                        transition_waiters.spawn(notified);
                        continue;
                    }
                }
                let TaskSlot::Running(task) = std::mem::replace(&mut *state, TaskSlot::Stopping)
                else {
                    unreachable!();
                };
                let slot = Arc::clone(slot);
                stop_jobs.spawn(async move {
                    let outcome = tokio::spawn(async move { stop_task(*task).await })
                        .await
                        .unwrap_or(SubmitDispatchOutcome::OutcomeUnknown);
                    {
                        let mut state = slot.state.lock().await;
                        *state = TaskSlot::Vacant(None);
                    }
                    slot.changed.notify_one();
                    outcome
                });
            }

            while let Some(joined) = stop_jobs.join_next().await {
                let outcome = joined.unwrap_or(SubmitDispatchOutcome::OutcomeUnknown);
                overall = combine_shutdown_outcomes(overall, outcome);
            }
            if transition_waiters.is_empty() {
                return overall;
            }
            let _ = transition_waiters.join_next().await;
            transition_waiters.abort_all();
        }
    }
}

fn spawn_detached(future: impl Future<Output = ()> + Send + 'static) {
    std::mem::drop(tokio::spawn(future));
}

async fn wait_for_dispatch_outcome(
    mut result: watch::Receiver<Option<SubmitDispatchOutcome>>,
) -> SubmitDispatchOutcome {
    loop {
        if let Some(outcome) = *result.borrow() {
            return outcome;
        }
        if result.changed().await.is_err() {
            return SubmitDispatchOutcome::OutcomeUnknown;
        }
    }
}

enum TaskSlot {
    Vacant(Option<SubmitDispatchOutcome>),
    Starting,
    Running(Box<StartedPaperTask>),
    Stopping,
}

fn is_terminal(task: &StartedPaperTask) -> bool {
    match task {
        StartedPaperTask::Grid(task) => task.status().phase.is_terminal(),
        StartedPaperTask::Arbitrage(task) => task.status().phase.is_terminal(),
    }
}

const fn command_kind(command: &SubmitCommand) -> &'static str {
    match command {
        SubmitCommand::StartPaperGrid { .. } => "start_paper_grid",
        SubmitCommand::StartPaperArbitrage { .. } => "start_paper_arbitrage",
        SubmitCommand::StopTask => "stop_task",
        SubmitCommand::CancelTask => "cancel_task",
        SubmitCommand::PauseAccountRisk { .. } => "pause_account_risk",
        SubmitCommand::ResumeAccountRisk => "resume_account_risk",
        SubmitCommand::EngageAccountKillSwitch { .. } => "engage_account_kill_switch",
        SubmitCommand::ReconcileRelease { .. } => "reconcile_release",
        SubmitCommand::RecordReconcileFailure { .. } => "record_reconcile_failure",
    }
}

const fn dispatch_outcome_name(outcome: SubmitDispatchOutcome) -> &'static str {
    match outcome {
        SubmitDispatchOutcome::Applied => "applied",
        SubmitDispatchOutcome::Rejected => "rejected",
        SubmitDispatchOutcome::OutcomeUnknown => "outcome_unknown",
    }
}

async fn stop_task(task: StartedPaperTask) -> SubmitDispatchOutcome {
    match task {
        StartedPaperTask::Grid(mut task) => match task.stop().await {
            Ok(_) => SubmitDispatchOutcome::Applied,
            Err(error) => map_grid_control_error(&error),
        },
        StartedPaperTask::Arbitrage(mut task) => match task.stop().await {
            Ok(_) => SubmitDispatchOutcome::Applied,
            Err(error) => map_arbitrage_control_error(&error),
        },
    }
}

async fn cancel_task(task: StartedPaperTask) -> SubmitDispatchOutcome {
    match task {
        StartedPaperTask::Grid(mut task) => match task.cancel().await {
            Ok(_) => SubmitDispatchOutcome::Applied,
            Err(error) => map_grid_control_error(&error),
        },
        StartedPaperTask::Arbitrage(mut task) => match task.cancel().await {
            Ok(_) => SubmitDispatchOutcome::Applied,
            Err(error) => map_arbitrage_control_error(&error),
        },
    }
}

const fn combine_shutdown_outcomes(
    left: SubmitDispatchOutcome,
    right: SubmitDispatchOutcome,
) -> SubmitDispatchOutcome {
    match (left, right) {
        (SubmitDispatchOutcome::OutcomeUnknown, _) | (_, SubmitDispatchOutcome::OutcomeUnknown) => {
            SubmitDispatchOutcome::OutcomeUnknown
        }
        (SubmitDispatchOutcome::Rejected, _) | (_, SubmitDispatchOutcome::Rejected) => {
            SubmitDispatchOutcome::Rejected
        }
        (SubmitDispatchOutcome::Applied, SubmitDispatchOutcome::Applied) => {
            SubmitDispatchOutcome::Applied
        }
    }
}

fn map_grid_control_error(error: &GridPaperTaskError) -> SubmitDispatchOutcome {
    match error {
        GridPaperTaskError::Journal(_)
        | GridPaperTaskError::JournalRead(_)
        | GridPaperTaskError::Projection(_)
        | GridPaperTaskError::SnapshotTaskFailed
        | GridPaperTaskError::TaskPanicked => SubmitDispatchOutcome::OutcomeUnknown,
        GridPaperTaskError::InvalidConfig
        | GridPaperTaskError::InvalidSourceBinding
        | GridPaperTaskError::InvalidRequest
        | GridPaperTaskError::RecoveryRequired
        | GridPaperTaskError::ShutdownTimedOut
        | GridPaperTaskError::Account(_)
        | GridPaperTaskError::AccountRisk(_)
        | GridPaperTaskError::Source(_)
        | GridPaperTaskError::Strategy(_)
        | GridPaperTaskError::Runtime(_)
        | GridPaperTaskError::Saga(_)
        | GridPaperTaskError::TaskCancelled
        | GridPaperTaskError::PreviouslyFailed(_) => SubmitDispatchOutcome::Rejected,
    }
}

fn map_arbitrage_control_error(error: &ArbitragePaperTaskError) -> SubmitDispatchOutcome {
    match error {
        ArbitragePaperTaskError::Journal(_)
        | ArbitragePaperTaskError::JournalRead(_)
        | ArbitragePaperTaskError::Projection(_)
        | ArbitragePaperTaskError::SnapshotTaskFailed
        | ArbitragePaperTaskError::TaskPanicked => SubmitDispatchOutcome::OutcomeUnknown,
        ArbitragePaperTaskError::InvalidConfig
        | ArbitragePaperTaskError::InvalidSourceBinding
        | ArbitragePaperTaskError::InvalidRequest
        | ArbitragePaperTaskError::LiquidityRejected
        | ArbitragePaperTaskError::RiskRejected(_)
        | ArbitragePaperTaskError::RecoveryRequired
        | ArbitragePaperTaskError::ShutdownTimedOut
        | ArbitragePaperTaskError::Account(_)
        | ArbitragePaperTaskError::AccountRisk(_)
        | ArbitragePaperTaskError::Source(_)
        | ArbitragePaperTaskError::SourceContract
        | ArbitragePaperTaskError::Monitor(_)
        | ArbitragePaperTaskError::Market(_)
        | ArbitragePaperTaskError::Strategy(_)
        | ArbitragePaperTaskError::Runtime(_)
        | ArbitragePaperTaskError::Saga(_)
        | ArbitragePaperTaskError::TaskCancelled
        | ArbitragePaperTaskError::PreviouslyFailed(_) => SubmitDispatchOutcome::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn shutdown_survives_caller_cancellation_and_reuses_the_final_outcome() {
        let registry = Arc::new(PaperTaskRegistry::new(vec!["paper-owner".to_owned()]));
        let slot = registry.slots.get("paper-owner").unwrap();
        *slot.state.lock().await = TaskSlot::Starting;

        let first_registry = Arc::clone(&registry);
        let first = tokio::spawn(async move { first_registry.shutdown().await });
        for _ in 0..100 {
            if !*registry.accepting_commands.read().await {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!*registry.accepting_commands.read().await);
        first.abort();
        let _ = first.await;

        let second_registry = Arc::clone(&registry);
        let mut second = tokio::spawn(async move { second_registry.shutdown().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut second)
                .await
                .is_err(),
            "a concurrent caller must wait for the active quiesce operation"
        );

        *slot.state.lock().await = TaskSlot::Vacant(None);
        slot.changed.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second)
                .await
                .expect("shutdown worker remained stranded")
                .unwrap(),
            SubmitDispatchOutcome::Applied
        );
        assert_eq!(
            registry.shutdown().await,
            SubmitDispatchOutcome::Applied,
            "later callers must observe the stored final outcome"
        );
    }

    #[tokio::test]
    async fn shutdown_preserves_a_concurrent_control_failure() {
        let registry = Arc::new(PaperTaskRegistry::new(vec!["paper-owner".to_owned()]));
        let slot = registry.slots.get("paper-owner").unwrap();
        *slot.state.lock().await = TaskSlot::Vacant(Some(SubmitDispatchOutcome::OutcomeUnknown));

        assert_eq!(
            registry.shutdown().await,
            SubmitDispatchOutcome::OutcomeUnknown
        );
        assert_eq!(
            registry.shutdown().await,
            SubmitDispatchOutcome::OutcomeUnknown,
            "idempotent callers must receive the same failed cleanup outcome"
        );
    }
}
