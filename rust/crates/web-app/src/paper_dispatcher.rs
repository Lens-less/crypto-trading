use std::{collections::HashMap, future::Future, path::PathBuf, sync::Arc};

use chrono::Utc;
use crypto_trading_cli::{
    ArbitragePaperTaskError, GridPaperTaskError, PaperProfileCatalog, PaperProfileError,
    StartedPaperTask,
};
use crypto_trading_control_plane::{
    SubmitCommand, SubmitDispatchFuture, SubmitDispatchOutcome, SubmitDispatcher, SubmitEnvelope,
};
use crypto_trading_runtime::{AccountRiskAuthority, AccountRiskError, JsonlHistory};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

#[derive(Clone)]
pub struct TrustedPaperSubmitDispatcher {
    journal_id: Uuid,
    history_path: Arc<PathBuf>,
    catalog: Arc<PaperProfileCatalog>,
    registry: Arc<PaperTaskRegistry>,
    account_risk: Option<Arc<AccountRiskAuthority>>,
}

impl TrustedPaperSubmitDispatcher {
    #[must_use]
    pub fn new(journal_id: Uuid, history_path: PathBuf, catalog: PaperProfileCatalog) -> Self {
        let registry = PaperTaskRegistry::new(catalog.task_ids());
        let account_risk = AccountRiskAuthority::new(
            journal_id,
            JsonlHistory::new(&history_path),
            PaperProfileCatalog::account_risk_scope(),
            catalog.account_risk_policy().clone(),
        )
        .ok()
        .map(Arc::new);
        Self {
            journal_id,
            history_path: Arc::new(history_path),
            catalog: Arc::new(catalog),
            registry: Arc::new(registry),
            account_risk,
        }
    }

    async fn dispatch_command(&self, envelope: SubmitEnvelope) -> SubmitDispatchOutcome {
        match envelope.command() {
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
        }
    }

    async fn dispatch_account_risk<F, Fut>(&self, action: F) -> SubmitDispatchOutcome
    where
        F: FnOnce(Arc<AccountRiskAuthority>) -> Fut,
        Fut: Future<Output = Result<(), AccountRiskError>>,
    {
        let Some(authority) = self.account_risk.clone() else {
            return SubmitDispatchOutcome::Rejected;
        };
        match action(authority).await {
            Ok(()) => SubmitDispatchOutcome::Applied,
            Err(
                AccountRiskError::InvalidConfig(_)
                | AccountRiskError::InvalidRequest(_)
                | AccountRiskError::DegradedState,
            ) => SubmitDispatchOutcome::Rejected,
            Err(_) => SubmitDispatchOutcome::OutcomeUnknown,
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
    slots: HashMap<String, Arc<Mutex<TaskSlot>>>,
    accepting_commands: RwLock<bool>,
}

impl PaperTaskRegistry {
    fn new(task_ids: Vec<String>) -> Self {
        let slots = task_ids
            .into_iter()
            .map(|task_id| (task_id, Arc::new(Mutex::new(TaskSlot::Vacant))))
            .collect();
        Self {
            slots,
            accepting_commands: RwLock::new(true),
        }
    }

    async fn start<F, Fut>(&self, task_id: &str, starter: F) -> SubmitDispatchOutcome
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<StartedPaperTask, PaperProfileError>> + Send,
    {
        let accepting_commands = self.accepting_commands.read().await;
        if !*accepting_commands {
            return SubmitDispatchOutcome::Rejected;
        }
        let Some(slot) = self.slots.get(task_id).cloned() else {
            return SubmitDispatchOutcome::Rejected;
        };
        {
            let mut state = slot.lock().await;
            match &*state {
                TaskSlot::Vacant => *state = TaskSlot::Starting,
                TaskSlot::Running(task) if is_terminal(task) => *state = TaskSlot::Starting,
                TaskSlot::Starting | TaskSlot::Stopping | TaskSlot::Running(_) => {
                    return SubmitDispatchOutcome::Rejected;
                }
            }
        }

        match starter().await {
            Ok(task) => {
                let mut state = slot.lock().await;
                *state = TaskSlot::Running(Box::new(task));
                SubmitDispatchOutcome::Applied
            }
            Err(error) => {
                let mut state = slot.lock().await;
                *state = TaskSlot::Vacant;
                if error.is_rejected() {
                    SubmitDispatchOutcome::Rejected
                } else {
                    SubmitDispatchOutcome::OutcomeUnknown
                }
            }
        }
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
            let mut state = slot.lock().await;
            match &*state {
                TaskSlot::Vacant | TaskSlot::Starting | TaskSlot::Stopping => {
                    return SubmitDispatchOutcome::Rejected;
                }
                TaskSlot::Running(task) if is_terminal(task) => {
                    *state = TaskSlot::Vacant;
                    return SubmitDispatchOutcome::Rejected;
                }
                TaskSlot::Running(_) => {}
            }
            let TaskSlot::Running(task) = std::mem::replace(&mut *state, TaskSlot::Stopping) else {
                unreachable!();
            };
            task
        };
        let outcome = match control {
            TaskControl::Stop => stop_task(*task).await,
            TaskControl::Cancel => cancel_task(*task).await,
        };
        let mut state = slot.lock().await;
        *state = TaskSlot::Vacant;
        outcome
    }

    async fn shutdown(&self) -> SubmitDispatchOutcome {
        let mut accepting_commands = self.accepting_commands.write().await;
        if !*accepting_commands {
            return SubmitDispatchOutcome::Applied;
        }
        *accepting_commands = false;

        let mut overall = SubmitDispatchOutcome::Applied;
        for slot in self.slots.values() {
            let task = {
                let mut state = slot.lock().await;
                match &*state {
                    TaskSlot::Vacant => continue,
                    TaskSlot::Running(task) if is_terminal(task) => {
                        *state = TaskSlot::Vacant;
                        continue;
                    }
                    TaskSlot::Running(_) => {}
                    TaskSlot::Starting | TaskSlot::Stopping => {
                        unreachable!(
                            "the write gate excludes concurrent start/control transitions"
                        );
                    }
                }
                let TaskSlot::Running(task) = std::mem::replace(&mut *state, TaskSlot::Stopping)
                else {
                    unreachable!();
                };
                task
            };
            let outcome = stop_task(*task).await;
            let mut state = slot.lock().await;
            *state = TaskSlot::Vacant;
            overall = combine_shutdown_outcomes(overall, outcome);
        }
        overall
    }
}

enum TaskSlot {
    Vacant,
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
