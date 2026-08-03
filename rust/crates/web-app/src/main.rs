use anyhow::anyhow;
use clap::Parser;
use crypto_trading_cli::shutdown::install_shutdown_signal;
use crypto_trading_web_app::{Cli, run};
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

const DEFAULT_LOG_FILTER: &str = concat!(
    "warn,",
    "crypto_trading_web_app=info,",
    "crypto_trading_web=info,",
    "crypto_trading_cli=info,",
    "crypto_trading_control_plane=info,",
    "crypto_trading_runtime=info,",
    "crypto_trading_exchange=info"
);

#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        eprintln!("warning: invalid RUST_LOG; using the scoped operational default");
        EnvFilter::new(DEFAULT_LOG_FILTER)
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
    tracing::info!(
        event = "web_process_starting",
        "control-plane process initialization began"
    );

    let shutdown = match install_shutdown_signal() {
        Ok(shutdown) => shutdown,
        Err(error) => {
            tracing::error!(
                event = "shutdown_signal_registration_failed",
                "process cannot guarantee graceful termination"
            );
            eprintln!("error: {error:#}");
            std::process::exit(1);
        }
    };
    let (shutdown_result_sender, shutdown_result_receiver) = oneshot::channel();
    let server_result = run(Cli::parse(), async move {
        let outcome = shutdown
            .await
            .map(|signal| {
                tracing::info!(
                    event = "shutdown_signal_received",
                    signal = ?signal,
                    "operating-system shutdown signal received"
                );
            })
            .map_err(anyhow::Error::new);
        let _ = shutdown_result_sender.send(outcome);
    })
    .await;
    let shutdown_result = shutdown_result_receiver.await;

    if let Err(error) = server_result {
        tracing::error!(
            event = "web_process_failed",
            failure_class = "server_or_cleanup",
            "control-plane process exited with a failure"
        );
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
    match shutdown_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::error!(
                event = "web_process_failed",
                failure_class = "shutdown_signal",
                "shutdown signal observer failed"
            );
            eprintln!("error: {error:#}");
            std::process::exit(1);
        }
        Err(_) => {
            tracing::error!(
                event = "web_process_failed",
                failure_class = "shutdown_observer",
                "shutdown observer did not report an outcome"
            );
            eprintln!(
                "error: {:#}",
                anyhow!("shutdown observer dropped before reporting an outcome")
            );
            std::process::exit(1);
        }
    }
    tracing::info!(
        event = "web_process_stopped",
        "control-plane process completed graceful shutdown"
    );
}
