use std::{
    future::{Future, pending},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crypto_trading_control_plane::{
    SubmitCommand, SubmitDispatchFuture, SubmitDispatchOutcome, SubmitDispatcher, SubmitEnvelope,
    SubmitPermission, SubmitRiskConfirmation, SubmitRole, SubmitService, SubmitStatus,
};
use uuid::Uuid;

#[derive(Clone)]
struct RecordingDispatcher {
    calls: Arc<AtomicUsize>,
    accepted_was_durable: Arc<AtomicBool>,
    journal_path: PathBuf,
    outcome: SubmitDispatchOutcome,
}

impl SubmitDispatcher for RecordingDispatcher {
    fn dispatch(&self, _envelope: SubmitEnvelope) -> SubmitDispatchFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.accepted_was_durable.store(
            std::fs::read_to_string(&self.journal_path)
                .is_ok_and(|journal| journal.contains("\"decision\":\"submit_accepted\"")),
            Ordering::SeqCst,
        );
        let outcome = self.outcome;
        Box::pin(async move { outcome })
    }
}

#[derive(Clone)]
struct PendingDispatcher {
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct TerminalWriteBreakingDispatcher {
    journal_path: PathBuf,
}

impl SubmitDispatcher for TerminalWriteBreakingDispatcher {
    fn dispatch(&self, _envelope: SubmitEnvelope) -> SubmitDispatchFuture {
        std::fs::remove_file(&self.journal_path).unwrap();
        std::fs::create_dir(&self.journal_path).unwrap();
        Box::pin(async { SubmitDispatchOutcome::Applied })
    }
}

impl SubmitDispatcher for PendingDispatcher {
    fn dispatch(&self, _envelope: SubmitEnvelope) -> SubmitDispatchFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        Box::pin(pending::<SubmitDispatchOutcome>())
    }
}

fn paper_stop(command_id: Uuid, key: &str, target: &str) -> SubmitEnvelope {
    SubmitEnvelope::new(
        command_id,
        key,
        target,
        SubmitPermission::new("operator-a", SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        SubmitCommand::StopTask,
    )
    .unwrap()
}

fn temporary_journal(label: &str) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("crypto-trading-{label}-{}.jsonl", Uuid::new_v4()));
    std::fs::write(&path, []).unwrap();
    path
}

fn recording_service(
    path: &PathBuf,
    journal_id: Uuid,
    calls: Arc<AtomicUsize>,
    outcome: SubmitDispatchOutcome,
) -> (SubmitService, Arc<AtomicBool>) {
    let accepted_was_durable = Arc::new(AtomicBool::new(false));
    let service = SubmitService::new(
        journal_id,
        path,
        Arc::new(RecordingDispatcher {
            calls,
            accepted_was_durable: accepted_was_durable.clone(),
            journal_path: path.clone(),
            outcome,
        }),
    )
    .unwrap();
    (service, accepted_was_durable)
}

#[tokio::test]
async fn accepted_is_durable_before_dispatch_and_terminal_replay_never_redispatches() {
    let path = temporary_journal("submit-replay");
    let journal_id = Uuid::new_v4();
    let calls = Arc::new(AtomicUsize::new(0));
    let envelope = paper_stop(Uuid::new_v4(), "stop-0001", "paper-grid-btc-usdt");
    let (service, accepted_was_durable) = recording_service(
        &path,
        journal_id,
        calls.clone(),
        SubmitDispatchOutcome::Applied,
    );

    let first = service.submit(envelope.clone()).await.unwrap();
    let replay = service.submit(envelope.clone()).await.unwrap();

    assert_eq!(first, replay);
    assert_eq!(first.status(), SubmitStatus::Applied);
    assert_eq!(first.command_id(), envelope.command_id());
    assert_eq!(first.target_task_id(), envelope.target_task_id());
    assert_eq!(first.journal_projection(), "submit_command_v1");
    assert_eq!(first.source(), "durable_journal");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(accepted_was_durable.load(Ordering::SeqCst));

    drop(service);
    let restart_calls = Arc::new(AtomicUsize::new(0));
    let (restarted, _) = recording_service(
        &path,
        journal_id,
        restart_calls.clone(),
        SubmitDispatchOutcome::Applied,
    );
    assert_eq!(restarted.submit(envelope).await.unwrap(), first);
    assert_eq!(restart_calls.load(Ordering::SeqCst), 0);

    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn either_idempotency_identifier_conflict_fails_closed() {
    let path = temporary_journal("submit-conflict");
    let journal_id = Uuid::new_v4();
    let calls = Arc::new(AtomicUsize::new(0));
    let command_id = Uuid::new_v4();
    let original = paper_stop(command_id, "stop-0001", "paper-grid-btc-usdt");
    let (service, _) = recording_service(
        &path,
        journal_id,
        calls.clone(),
        SubmitDispatchOutcome::Applied,
    );
    service.submit(original).await.unwrap();

    let same_key_different_command = paper_stop(Uuid::new_v4(), "stop-0001", "paper-grid-eth-usdt");
    let same_command_different_key = paper_stop(command_id, "stop-0002", "paper-grid-btc-usdt");

    assert!(
        service
            .submit(same_key_different_command)
            .await
            .unwrap_err()
            .is_conflict()
    );
    assert!(
        service
            .submit(same_command_different_key)
            .await
            .unwrap_err()
            .is_conflict()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    drop(service);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn accepted_without_terminal_recovers_as_unknown_and_never_redispatches() {
    let path = temporary_journal("submit-accepted-only");
    let journal_id = Uuid::new_v4();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let envelope = paper_stop(Uuid::new_v4(), "stop-0003", "paper-grid-sol-usdt");
    let service = SubmitService::new(
        journal_id,
        &path,
        Arc::new(PendingDispatcher {
            calls: calls.clone(),
            entered: entered.clone(),
        }),
    )
    .unwrap();

    let running_service = service.clone();
    let running_envelope = envelope.clone();
    let handle =
        tokio::spawn(async move { running_service.submit(running_envelope).await.unwrap() });
    entered.notified().await;
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    drop(service);

    let restart_calls = Arc::new(AtomicUsize::new(0));
    let (restarted, _) = recording_service(
        &path,
        journal_id,
        restart_calls.clone(),
        SubmitDispatchOutcome::Applied,
    );
    let recovered = restarted.submit(envelope).await.unwrap();

    assert_eq!(recovered.status(), SubmitStatus::OutcomeUnknown);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(restart_calls.load(Ordering::SeqCst), 0);

    drop(restarted);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn terminal_write_failure_is_reported_as_outcome_unknown() {
    let path = temporary_journal("submit-terminal-write");
    let journal_id = Uuid::new_v4();
    let envelope = paper_stop(
        Uuid::new_v4(),
        "stop-terminal-write",
        "paper-grid-doge-usdt",
    );
    let service = SubmitService::new(
        journal_id,
        &path,
        Arc::new(TerminalWriteBreakingDispatcher {
            journal_path: path.clone(),
        }),
    )
    .unwrap();

    let receipt = service.submit(envelope).await.unwrap();

    assert_eq!(receipt.status(), SubmitStatus::OutcomeUnknown);
    drop(service);
    std::fs::remove_dir(path).unwrap();
}

#[test]
fn dispatcher_future_is_owned_and_transport_independent() {
    fn assert_send_future(
        future: SubmitDispatchFuture,
    ) -> Pin<Box<dyn Future<Output = SubmitDispatchOutcome> + Send + 'static>> {
        future
    }

    let future = assert_send_future(Box::pin(async { SubmitDispatchOutcome::Applied }));
    drop(future);
}
