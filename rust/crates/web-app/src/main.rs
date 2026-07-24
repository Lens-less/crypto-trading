use clap::Parser;
use crypto_trading_web_app::{Cli, run};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let shutdown = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    if let Err(error) = run(Cli::parse(), shutdown).await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
