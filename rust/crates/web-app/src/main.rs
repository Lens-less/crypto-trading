use anyhow::anyhow;
use clap::Parser;
use crypto_trading_cli::shutdown::install_shutdown_signal;
use crypto_trading_web_app::{Cli, run};
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let shutdown = match install_shutdown_signal() {
        Ok(shutdown) => shutdown,
        Err(error) => {
            eprintln!("error: {error:#}");
            std::process::exit(1);
        }
    };
    let (shutdown_result_sender, shutdown_result_receiver) = oneshot::channel();
    let server_result = run(Cli::parse(), async move {
        let outcome = shutdown.await.map(|_| ()).map_err(anyhow::Error::new);
        let _ = shutdown_result_sender.send(outcome);
    })
    .await;
    let shutdown_result = shutdown_result_receiver.await;

    if let Err(error) = server_result {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
    match shutdown_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("error: {error:#}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!(
                "error: {:#}",
                anyhow!("shutdown observer dropped before reporting an outcome")
            );
            std::process::exit(1);
        }
    }
}
