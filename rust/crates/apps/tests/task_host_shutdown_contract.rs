#![allow(dead_code)]

#[path = "../src/shutdown.rs"]
mod shutdown;
#[path = "../src/task_host.rs"]
mod task_host;

use std::{
    io,
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use shutdown::{ShutdownSignal, ShutdownSignalError, ShutdownSignalStage};
use task_host::{
    TaskHost, TaskHostServeError, TaskHostServeOutcome, TaskHostStatus, TaskHostStopFuture,
};
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
struct MockStatus {
    terminal: bool,
}

impl TaskHostStatus for MockStatus {
    fn is_terminal(&self) -> bool {
        self.terminal
    }
}

#[derive(Clone)]
struct MockHost {
    stopped: Arc<AtomicBool>,
}

impl MockHost {
    fn new() -> Self {
        Self {
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl TaskHost for MockHost {
    type Status = MockStatus;
    type Exit = &'static str;
    type Error = io::Error;

    fn status(&self) -> Self::Status {
        MockStatus { terminal: false }
    }

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
        self.stopped.store(true, Ordering::SeqCst);
        Box::pin(async { Ok("stopped") })
    }
}

#[tokio::test]
async fn injected_shutdown_signal_requests_a_graceful_stop() {
    let mut host = MockHost::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

    let outcome = task_host::serve_host_with_shutdown(
        &mut host,
        listener,
        Duration::from_secs(60),
        |_| "status\n".to_owned(),
        |_, exit| format!("exit={exit}\n"),
        Ok(Box::pin(async { Ok(ShutdownSignal::CtrlC) })),
    )
    .await
    .unwrap();

    assert!(matches!(
        outcome,
        TaskHostServeOutcome::StopRequested("stopped")
    ));
    assert!(host.stopped.load(Ordering::SeqCst));
}

#[cfg(unix)]
#[tokio::test]
async fn injected_sigterm_uses_the_same_graceful_stop_path() {
    let mut host = MockHost::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

    let outcome = task_host::serve_host_with_shutdown(
        &mut host,
        listener,
        Duration::from_secs(60),
        |_| "status\n".to_owned(),
        |_, exit| format!("exit={exit}\n"),
        Ok(Box::pin(async { Ok(ShutdownSignal::Sigterm) })),
    )
    .await
    .unwrap();

    assert!(matches!(
        outcome,
        TaskHostServeOutcome::StopRequested("stopped")
    ));
    assert!(host.stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn shutdown_registration_failure_is_observable_and_fail_closed() {
    let mut host = MockHost::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

    let error = task_host::serve_host_with_shutdown(
        &mut host,
        listener,
        Duration::from_secs(60),
        |_| "status\n".to_owned(),
        |_, exit| format!("exit={exit}\n"),
        Err(TaskHostServeError::Shutdown(
            ShutdownSignalError::synthetic(
                "SIGTERM",
                ShutdownSignalStage::Register,
                "synthetic failure",
            ),
        )),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, TaskHostServeError::Shutdown(_)));
    assert!(!host.stopped.load(Ordering::SeqCst));
}
