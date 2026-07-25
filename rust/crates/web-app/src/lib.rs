//! Trusted composition root for the local read-only Web control plane.
//!
//! This crate is the only production binary that receives a journal path and
//! generation ID. The HTTP adapter receives an already-constructed
//! [`ReadControlPlane`] and therefore cannot discover files or construct
//! execution authority.

use std::{env, future::Future, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::Parser;
use crypto_trading_runtime::{FileJournalSnapshotSource, JournalSnapshotSource};
use crypto_trading_web::{
    ReadControlPlane, WebAccessPolicy, WebServerConfig, app_router, serve_with_shutdown,
};
use uuid::Uuid;

mod submit;

pub use submit::{
    MAX_TRUSTED_SUBMIT_BODY_BYTES, TrustedSubmitApplication, TrustedSubmitIdentity,
    bind_trusted_submit_app,
};

/// Local read-only control-plane server.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "crypto-trading-web",
    version,
    about = "本地只读交易运行与审计控制面"
)]
pub struct Cli {
    /// Existing execution JSONL journal to project.
    #[arg(long, value_name = "PATH")]
    pub history_path: PathBuf,

    /// Durable generation UUID; change it when the journal is replaced or rotated.
    #[arg(long, value_name = "UUID")]
    pub journal_id: Uuid,

    /// Loopback TCP port. Use 0 only for tests that need an ephemeral port.
    #[arg(long, default_value_t = crypto_trading_web::DEFAULT_WEB_PORT)]
    pub port: u16,

    /// Optional environment variable containing a 32-512 byte bearer token.
    ///
    /// The token itself is never accepted as a command-line argument.
    #[arg(long, value_name = "ENV_NAME")]
    pub bearer_token_env: Option<String>,
}

/// Runs the local read-only application until `shutdown` resolves.
///
/// # Errors
///
/// Returns an error when the journal cannot be frozen and projected safely,
/// access policy validation fails, the loopback listener cannot bind, or the
/// HTTP server terminates with an I/O failure.
pub async fn run<F>(cli: Cli, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (listener, router, address) = prepare(&cli).await?;
    println!("control-plane web: http://{address}/overview");
    println!("authority: paper-only; live-trading=false; access=loopback");
    serve_with_shutdown(listener, router, shutdown)
        .await
        .context("read-only Web server stopped with an I/O error")
}

async fn prepare(cli: &Cli) -> Result<(tokio::net::TcpListener, axum::Router, SocketAddr)> {
    let source = FileJournalSnapshotSource::new(cli.journal_id, &cli.history_path)
        .context("failed to construct the bounded journal source")?;

    // Fail at startup instead of exposing a shell whose first API read is
    // already known to be unsafe or unavailable.
    source
        .snapshot()
        .context("failed to freeze the configured execution journal")?;

    let control_plane =
        ReadControlPlane::new(Arc::new(source)).context("the capability manifest failed closed")?;
    control_plane
        .snapshot()
        .context("failed to build the initial operator projection")?;

    let access = access_policy(cli)?;
    let listener = WebServerConfig::loopback(cli.port)
        .bind()
        .await
        .context("failed to bind the loopback Web listener")?;
    let address = listener
        .local_addr()
        .context("failed to inspect the bound loopback address")?;
    let router = app_router(Arc::new(control_plane), access);
    Ok((listener, router, address))
}

fn access_policy(cli: &Cli) -> Result<WebAccessPolicy> {
    let Some(name) = cli.bearer_token_env.as_deref() else {
        return Ok(WebAccessPolicy::loopback_open());
    };
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!(
            "--bearer-token-env must name a 1-128 byte ASCII environment variable using A-Z, 0-9, and _"
        );
    }
    let token = env::var(name)
        .with_context(|| format!("bearer token environment variable {name} is unavailable"))?;
    WebAccessPolicy::bearer(token).context("bearer token policy rejected the supplied secret")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{Cli, access_policy, prepare};
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use clap::Parser;
    use tower::ServiceExt;
    use uuid::Uuid;

    const JOURNAL_ID: &str = "00000000-0000-4000-8000-000000000123";

    #[test]
    fn cli_requires_an_explicit_path_and_durable_generation() {
        let cli = Cli::try_parse_from([
            "crypto-trading-web",
            "--history-path",
            "fixture.jsonl",
            "--journal-id",
            JOURNAL_ID,
            "--port",
            "0",
        ])
        .unwrap();

        assert_eq!(cli.history_path.to_string_lossy(), "fixture.jsonl");
        assert_eq!(cli.journal_id.to_string(), JOURNAL_ID);
        assert_eq!(cli.port, 0);
        assert!(cli.bearer_token_env.is_none());
    }

    #[test]
    fn command_line_never_accepts_the_bearer_secret_itself() {
        assert!(
            Cli::try_parse_from([
                "crypto-trading-web",
                "--history-path",
                "fixture.jsonl",
                "--journal-id",
                JOURNAL_ID,
                "--bearer-token",
                "0123456789abcdef0123456789abcdef",
            ])
            .is_err()
        );
    }

    #[test]
    fn access_policy_rejects_ambiguous_environment_names() {
        let mut cli = Cli::try_parse_from([
            "crypto-trading-web",
            "--history-path",
            "fixture.jsonl",
            "--journal-id",
            JOURNAL_ID,
        ])
        .unwrap();
        cli.bearer_token_env = Some("mixed-Case".to_owned());

        let error = access_policy(&cli).unwrap_err().to_string();
        assert!(error.contains("ASCII environment variable"));
    }

    #[tokio::test]
    async fn offline_fixture_builds_the_loopback_application() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/m2-operator-journal.jsonl");
        let cli = Cli {
            history_path: fixture,
            journal_id: Uuid::parse_str(JOURNAL_ID).unwrap(),
            port: 0,
            bearer_token_env: None,
        };

        let (listener, router, address) = prepare(&cli).await.unwrap();
        assert!(address.ip().is_loopback());
        assert_ne!(address.port(), 0);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/system")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(listener);
    }

    #[tokio::test]
    async fn recovery_fixture_exposes_bounded_reconciliation_without_raw_failure_text() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/m2-recovery-journal.jsonl");
        let cli = Cli {
            history_path: fixture,
            journal_id: Uuid::parse_str(JOURNAL_ID).unwrap(),
            port: 0,
            bearer_token_env: None,
        };

        let (listener, router, _) = prepare(&cli).await.unwrap();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/executions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("\"state\":\"partial\""));
        assert!(body.contains("\"recovery\":\"reconcile_required\""));
        assert!(!body.contains("deterministic offline reconciliation failure"));
        assert!(!body.contains("deterministic offline partial execution"));
        drop(listener);
    }
}
