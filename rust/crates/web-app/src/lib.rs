//! Trusted composition root for the local Web control plane.
//!
//! This crate is the only production binary that receives a journal path and
//! generation ID. The HTTP adapter receives an already-constructed
//! [`ReadControlPlane`] and therefore cannot discover files or construct
//! execution authority.

use std::{env, future::Future, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser};
use crypto_trading_cli::{
    ArbitragePaperProfileInput, GridPaperProfileInput, PaperProfileCatalog,
    PaperProfileCatalogInput,
};
use crypto_trading_control_plane::{SubmitDispatchOutcome, SubmitDispatcher, SubmitService};
use crypto_trading_runtime::{FileJournalSnapshotSource, JournalSnapshotSource};
use crypto_trading_web::{
    CredentialConfiguration, CredentialSettings, NotificationEvidence, PaperProfileKind,
    PaperProfileSettings, ReadControlPlane, RuntimeLogSink, SettingsResponse, WebAccessPolicy,
    WebRequestRateLimiter, WebServerConfig, app_router_with_settings, serve_with_shutdown,
};
use uuid::Uuid;

mod paper_dispatcher;
mod submit;

const PAPER_WRITE_PRINCIPAL_ID: &str = "local-paper-operator";

pub use paper_dispatcher::TrustedPaperSubmitDispatcher;
pub use submit::{
    MAX_TRUSTED_SUBMIT_BODY_BYTES, TrustedSubmitApplication, TrustedSubmitIdentity,
    bind_trusted_submit_app, bind_trusted_submit_app_with_settings,
};

/// Local control-plane server with an explicit opt-in paper-only write mode.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "crypto-trading-web",
    version,
    about = "本地交易运行与审计控制面（默认只读，可显式启用 Paper 任务控制）"
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

    #[command(flatten)]
    pub paper_write: PaperWriteArgs,
}

#[derive(Clone, Debug, Default, Args)]
pub struct PaperWriteArgs {
    /// Explicitly enable the loopback-only trusted submit route.
    #[arg(long, default_value_t = false)]
    pub enable_paper_writes: bool,

    /// Stable task identity for the one configured replay-backed paper grid owner.
    #[arg(long, value_name = "TASK_ID")]
    pub paper_grid_task_id: Option<String>,
    /// Stable strategy identity accepted by the paper grid owner.
    #[arg(long, value_name = "STRATEGY_ID")]
    pub paper_grid_strategy_id: Option<String>,
    /// Exact strategy revision accepted by the paper grid owner.
    #[arg(long, value_name = "REVISION")]
    pub paper_grid_strategy_revision: Option<String>,
    /// Bounded YAML grid config used to derive the virtual grid owner.
    #[arg(long, value_name = "PATH")]
    pub paper_grid_config: Option<PathBuf>,
    /// Strict JSONL replay fixture mirrored into the process-local paper exchange.
    #[arg(long, value_name = "PATH")]
    pub paper_grid_replay: Option<PathBuf>,
    /// Optional shutdown grace override for the configured paper grid owner.
    #[arg(long, value_name = "MILLISECONDS")]
    pub paper_grid_shutdown_grace_ms: Option<u64>,

    /// Stable task identity for the one configured replay-backed paper arbitrage owner.
    #[arg(long, value_name = "TASK_ID")]
    pub paper_arbitrage_task_id: Option<String>,
    /// Stable strategy identity accepted by the paper arbitrage owner.
    #[arg(long, value_name = "STRATEGY_ID")]
    pub paper_arbitrage_strategy_id: Option<String>,
    /// Exact strategy revision accepted by the paper arbitrage owner.
    #[arg(long, value_name = "REVISION")]
    pub paper_arbitrage_strategy_revision: Option<String>,
    /// Bounded YAML arbitrage config used to derive the exact-pair paper owner.
    #[arg(long, value_name = "PATH")]
    pub paper_arbitrage_config: Option<PathBuf>,
    /// Bounded YAML monitor config used to derive the exact-pair read-only monitor.
    #[arg(long, value_name = "PATH")]
    pub paper_arbitrage_monitor_config: Option<PathBuf>,
    /// Strict JSONL replay fixture mirrored into the exact-pair paper exchanges.
    #[arg(long, value_name = "PATH")]
    pub paper_arbitrage_replay: Option<PathBuf>,
    /// Optional shutdown grace override for the configured paper arbitrage owner.
    #[arg(long, value_name = "MILLISECONDS")]
    pub paper_arbitrage_shutdown_grace_ms: Option<u64>,

    /// Optional bounded YAML account-level risk limits shared by every
    /// configured paper owner; absent limits stay disabled while durable
    /// pause/kill-switch facts still gate admissions.
    #[arg(long, value_name = "PATH")]
    pub paper_account_risk_config: Option<PathBuf>,
}

struct WriteModeConfig {
    bearer_token: String,
    identity: TrustedSubmitIdentity,
    catalog: PaperProfileCatalog,
}

/// Runs the local application until `shutdown` resolves.
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
    let write_mode = write_mode_config(&cli)?;
    let (listener, router, address, dispatcher) = prepare(&cli, write_mode.as_ref()).await?;
    println!("control-plane web: http://{address}/overview");
    if write_mode.is_some() {
        println!(
            "authority: paper-only; live-trading=false; access=loopback; trusted-submit=enabled"
        );
    } else {
        println!("authority: paper-only; live-trading=false; access=loopback");
    }
    let shutdown_dispatcher = dispatcher.clone();
    let (shutdown_result_sender, shutdown_result_receiver) = tokio::sync::oneshot::channel();
    let server_result = serve_with_shutdown(listener, router, async move {
        shutdown.await;
        let outcome = match shutdown_dispatcher {
            Some(dispatcher) => dispatcher.shutdown().await,
            None => SubmitDispatchOutcome::Applied,
        };
        let _ = shutdown_result_sender.send(outcome);
    })
    .await;
    let shutdown_outcome = match shutdown_result_receiver.await {
        Ok(outcome) => outcome,
        Err(_) => match dispatcher {
            Some(dispatcher) => dispatcher.shutdown().await,
            None => SubmitDispatchOutcome::Applied,
        },
    };
    match shutdown_outcome {
        SubmitDispatchOutcome::Applied => {}
        SubmitDispatchOutcome::Rejected => {
            bail!("one or more paper owners failed during graceful shutdown")
        }
        SubmitDispatchOutcome::OutcomeUnknown => {
            bail!("paper owner shutdown outcome is unknown; inspect the durable task projection")
        }
    }
    server_result.context("Web control-plane server stopped with an I/O error")
}

async fn prepare(
    cli: &Cli,
    write_mode: Option<&WriteModeConfig>,
) -> Result<(
    tokio::net::TcpListener,
    axum::Router,
    SocketAddr,
    Option<Arc<TrustedPaperSubmitDispatcher>>,
)> {
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

    let control_plane = Arc::new(control_plane);
    let settings = runtime_settings(cli, write_mode.is_some());
    if let Some(write_mode) = write_mode {
        let dispatcher = Arc::new(TrustedPaperSubmitDispatcher::new(
            cli.journal_id,
            cli.history_path.clone(),
            write_mode.catalog.clone(),
        ));
        let submit_dispatcher: Arc<dyn SubmitDispatcher> = dispatcher.clone();
        let submit = Arc::new(
            SubmitService::new(cli.journal_id, &cli.history_path, submit_dispatcher)
                .context("failed to construct the trusted submit service")?,
        );
        let application = bind_trusted_submit_app_with_settings(
            cli.port,
            control_plane,
            submit,
            write_mode.bearer_token.clone(),
            write_mode.identity.clone(),
            settings,
        )
        .await?;
        let address = application.address();
        let (listener, router) = application.into_parts();
        return Ok((listener, router, address, Some(dispatcher)));
    }

    let access = access_policy(cli)?;
    let listener = WebServerConfig::loopback(cli.port)
        .bind()
        .await
        .context("failed to bind the loopback Web listener")?;
    let address = listener
        .local_addr()
        .context("failed to inspect the bound loopback address")?;
    let router = app_router_with_settings(
        control_plane,
        access,
        settings,
        WebRequestRateLimiter::default(),
    );
    Ok((listener, router, address, None))
}

fn runtime_settings(cli: &Cli, paper_writes_enabled: bool) -> SettingsResponse {
    let mut paper_profiles = Vec::new();
    if paper_writes_enabled {
        if let (
            Some(task_id),
            Some(strategy_id),
            Some(strategy_revision),
            Some(config),
            Some(replay),
        ) = (
            &cli.paper_write.paper_grid_task_id,
            &cli.paper_write.paper_grid_strategy_id,
            &cli.paper_write.paper_grid_strategy_revision,
            &cli.paper_write.paper_grid_config,
            &cli.paper_write.paper_grid_replay,
        ) {
            paper_profiles.push(PaperProfileSettings {
                kind: PaperProfileKind::Grid,
                task_id: task_id.clone(),
                strategy_id: strategy_id.clone(),
                strategy_revision: strategy_revision.clone(),
                configuration_files: vec![config.display().to_string()],
                replay_file: replay.display().to_string(),
            });
        }
        if let (
            Some(task_id),
            Some(strategy_id),
            Some(strategy_revision),
            Some(arbitrage_config),
            Some(monitor_config),
            Some(replay),
        ) = (
            &cli.paper_write.paper_arbitrage_task_id,
            &cli.paper_write.paper_arbitrage_strategy_id,
            &cli.paper_write.paper_arbitrage_strategy_revision,
            &cli.paper_write.paper_arbitrage_config,
            &cli.paper_write.paper_arbitrage_monitor_config,
            &cli.paper_write.paper_arbitrage_replay,
        ) {
            paper_profiles.push(PaperProfileSettings {
                kind: PaperProfileKind::Arbitrage,
                task_id: task_id.clone(),
                strategy_id: strategy_id.clone(),
                strategy_revision: strategy_revision.clone(),
                configuration_files: vec![
                    arbitrage_config.display().to_string(),
                    monitor_config.display().to_string(),
                ],
                replay_file: replay.display().to_string(),
            });
        }
    }
    let data_directory = cli
        .history_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."))
        .display()
        .to_string();
    SettingsResponse {
        data_directory: Some(data_directory),
        journal_path: Some(cli.history_path.display().to_string()),
        log_sink: RuntimeLogSink::StdoutStderr,
        notification_evidence: NotificationEvidence::JournalProjection,
        credentials: CredentialSettings {
            web_bearer: if cli.bearer_token_env.is_some() {
                CredentialConfiguration::Configured
            } else {
                CredentialConfiguration::NotConfigured
            },
            binance_testnet: credential_pair_status("BINANCE_API_KEY", "BINANCE_API_SECRET"),
            mainnet: CredentialConfiguration::NotAccepted,
        },
        paper_principal_id: paper_writes_enabled.then(|| PAPER_WRITE_PRINCIPAL_ID.to_owned()),
        paper_profiles,
        ..SettingsResponse::default()
    }
}

fn credential_pair_status(key_name: &str, secret_name: &str) -> CredentialConfiguration {
    let present =
        |name: &str| env::var_os(name).is_some_and(|value| !value.as_encoded_bytes().is_empty());
    match (present(key_name), present(secret_name)) {
        (true, true) => CredentialConfiguration::Configured,
        (false, false) => CredentialConfiguration::NotConfigured,
        _ => CredentialConfiguration::Partial,
    }
}

fn access_policy(cli: &Cli) -> Result<WebAccessPolicy> {
    let Some(name) = cli.bearer_token_env.as_deref() else {
        return Ok(WebAccessPolicy::loopback_open());
    };
    validate_env_name(name)?;
    let token = env::var(name)
        .with_context(|| format!("bearer token environment variable {name} is unavailable"))?;
    WebAccessPolicy::bearer(token).context("bearer token policy rejected the supplied secret")
}

fn write_mode_config(cli: &Cli) -> Result<Option<WriteModeConfig>> {
    let has_profile_args = cli.paper_write.paper_grid_task_id.is_some()
        || cli.paper_write.paper_grid_strategy_id.is_some()
        || cli.paper_write.paper_grid_strategy_revision.is_some()
        || cli.paper_write.paper_grid_config.is_some()
        || cli.paper_write.paper_grid_replay.is_some()
        || cli.paper_write.paper_arbitrage_task_id.is_some()
        || cli.paper_write.paper_arbitrage_strategy_id.is_some()
        || cli.paper_write.paper_arbitrage_strategy_revision.is_some()
        || cli.paper_write.paper_arbitrage_config.is_some()
        || cli.paper_write.paper_arbitrage_monitor_config.is_some()
        || cli.paper_write.paper_arbitrage_replay.is_some();
    if !cli.paper_write.enable_paper_writes {
        if has_profile_args {
            bail!("paper task profiles require --enable-paper-writes");
        }
        return Ok(None);
    }
    let bearer_env = cli
        .bearer_token_env
        .as_deref()
        .context("trusted paper writes require --bearer-token-env")?;
    let bearer_token = bearer_token_from_env(bearer_env)?;
    let identity = TrustedSubmitIdentity::paper_operator(PAPER_WRITE_PRINCIPAL_ID)
        .context("trusted paper write principal is invalid")?;
    let catalog = PaperProfileCatalog::new(PaperProfileCatalogInput {
        grid: grid_profile_input(&cli.paper_write)?,
        arbitrage: arbitrage_profile_input(&cli.paper_write)?,
        account_risk_config_path: cli.paper_write.paper_account_risk_config.clone(),
    })
    .context("failed to construct the replay-backed paper profile catalog")?;
    if catalog.is_empty() {
        bail!("trusted paper writes require at least one configured paper profile");
    }
    Ok(Some(WriteModeConfig {
        bearer_token,
        identity,
        catalog,
    }))
}

fn grid_profile_input(write: &PaperWriteArgs) -> Result<Option<GridPaperProfileInput>> {
    let fields = [
        write.paper_grid_task_id.is_some(),
        write.paper_grid_strategy_id.is_some(),
        write.paper_grid_strategy_revision.is_some(),
        write.paper_grid_config.is_some(),
        write.paper_grid_replay.is_some(),
    ];
    if !fields.iter().any(|present| *present) {
        return Ok(None);
    }
    if fields.iter().any(|present| !present) {
        bail!(
            "paper grid write mode requires --paper-grid-task-id, --paper-grid-strategy-id, --paper-grid-strategy-revision, --paper-grid-config, and --paper-grid-replay"
        );
    }
    Ok(Some(GridPaperProfileInput {
        task_id: write.paper_grid_task_id.clone().unwrap_or_default(),
        strategy_id: write.paper_grid_strategy_id.clone().unwrap_or_default(),
        strategy_revision: write
            .paper_grid_strategy_revision
            .clone()
            .unwrap_or_default(),
        config_path: write.paper_grid_config.clone().unwrap_or_default(),
        replay_path: write.paper_grid_replay.clone().unwrap_or_default(),
        shutdown_grace: shutdown_grace(write.paper_grid_shutdown_grace_ms)?,
    }))
}

fn arbitrage_profile_input(write: &PaperWriteArgs) -> Result<Option<ArbitragePaperProfileInput>> {
    let fields = [
        write.paper_arbitrage_task_id.is_some(),
        write.paper_arbitrage_strategy_id.is_some(),
        write.paper_arbitrage_strategy_revision.is_some(),
        write.paper_arbitrage_config.is_some(),
        write.paper_arbitrage_monitor_config.is_some(),
        write.paper_arbitrage_replay.is_some(),
    ];
    if !fields.iter().any(|present| *present) {
        return Ok(None);
    }
    if fields.iter().any(|present| !present) {
        bail!(
            "paper arbitrage write mode requires --paper-arbitrage-task-id, --paper-arbitrage-strategy-id, --paper-arbitrage-strategy-revision, --paper-arbitrage-config, --paper-arbitrage-monitor-config, and --paper-arbitrage-replay"
        );
    }
    Ok(Some(ArbitragePaperProfileInput {
        task_id: write.paper_arbitrage_task_id.clone().unwrap_or_default(),
        strategy_id: write
            .paper_arbitrage_strategy_id
            .clone()
            .unwrap_or_default(),
        strategy_revision: write
            .paper_arbitrage_strategy_revision
            .clone()
            .unwrap_or_default(),
        arbitrage_config_path: write.paper_arbitrage_config.clone().unwrap_or_default(),
        monitor_config_path: write
            .paper_arbitrage_monitor_config
            .clone()
            .unwrap_or_default(),
        replay_path: write.paper_arbitrage_replay.clone().unwrap_or_default(),
        shutdown_grace: shutdown_grace(write.paper_arbitrage_shutdown_grace_ms)?,
    }))
}

fn shutdown_grace(value: Option<u64>) -> Result<std::time::Duration> {
    let milliseconds = value.unwrap_or(250);
    if milliseconds == 0 {
        bail!("paper shutdown grace must be positive");
    }
    Ok(std::time::Duration::from_millis(milliseconds))
}

fn validate_env_name(name: &str) -> Result<()> {
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
    Ok(())
}

fn bearer_token_from_env(name: &str) -> Result<String> {
    validate_env_name(name)?;
    env::var(name)
        .with_context(|| format!("bearer token environment variable {name} is unavailable"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        Cli, PAPER_WRITE_PRINCIPAL_ID, PaperWriteArgs, access_policy, prepare, runtime_settings,
        write_mode_config,
    };
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
        assert_eq!(PAPER_WRITE_PRINCIPAL_ID, "local-paper-operator");
    }

    #[test]
    fn command_line_cannot_override_the_browser_paper_principal() {
        assert!(
            Cli::try_parse_from([
                "crypto-trading-web",
                "--history-path",
                "fixture.jsonl",
                "--journal-id",
                JOURNAL_ID,
                "--principal-id",
                "operator-b",
            ])
            .is_err()
        );
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

    #[test]
    fn write_mode_requires_a_bearer_env_name() {
        let cli = Cli::try_parse_from([
            "crypto-trading-web",
            "--history-path",
            "fixture.jsonl",
            "--journal-id",
            JOURNAL_ID,
            "--enable-paper-writes",
            "--paper-grid-task-id",
            "paper-grid-owner",
            "--paper-grid-strategy-id",
            "grid.strategy",
            "--paper-grid-strategy-revision",
            "grid.v1",
            "--paper-grid-config",
            "grid.yaml",
            "--paper-grid-replay",
            "grid.jsonl",
        ])
        .unwrap();

        let error = match write_mode_config(&cli) {
            Ok(_) => panic!("write mode without a bearer env name must fail"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("require --bearer-token-env"));
    }

    #[test]
    fn configured_paper_profiles_are_visible_without_exposing_secrets() {
        let cli = Cli::try_parse_from([
            "crypto-trading-web",
            "--history-path",
            "var/history/operations.jsonl",
            "--journal-id",
            JOURNAL_ID,
            "--bearer-token-env",
            "CRYPTO_TRADING_WEB_TOKEN",
            "--enable-paper-writes",
            "--paper-grid-task-id",
            "paper-grid-owner",
            "--paper-grid-strategy-id",
            "grid.strategy",
            "--paper-grid-strategy-revision",
            "grid.v1",
            "--paper-grid-config",
            "config/grid.yaml",
            "--paper-grid-replay",
            "fixtures/grid.jsonl",
        ])
        .unwrap();

        let settings = runtime_settings(&cli, true);
        assert_eq!(
            settings.paper_principal_id.as_deref(),
            Some(PAPER_WRITE_PRINCIPAL_ID)
        );
        assert_eq!(settings.paper_profiles.len(), 1);
        assert_eq!(settings.paper_profiles[0].task_id, "paper-grid-owner");
        assert_eq!(
            settings.paper_profiles[0].configuration_files,
            vec!["config/grid.yaml"]
        );
        let encoded = serde_json::to_string(&settings).unwrap();
        assert!(!encoded.contains("CRYPTO_TRADING_WEB_TOKEN"));
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
            paper_write: PaperWriteArgs::default(),
        };

        let (listener, router, address, dispatcher) = prepare(&cli, None).await.unwrap();
        assert!(address.ip().is_loopback());
        assert!(dispatcher.is_none());
        assert_ne!(address.port(), 0);

        let settings_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(settings_response.status(), StatusCode::OK);
        let settings_body = to_bytes(settings_response.into_body(), 65_536)
            .await
            .unwrap();
        let settings: serde_json::Value = serde_json::from_slice(&settings_body).unwrap();
        assert_eq!(
            settings["journal_path"],
            cli.history_path.display().to_string()
        );
        assert_eq!(settings["credentials"]["mainnet"], "not_accepted");
        assert!(settings["paper_principal_id"].is_null());

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
            paper_write: PaperWriteArgs::default(),
        };

        let (listener, router, _, dispatcher) = prepare(&cli, None).await.unwrap();
        assert!(dispatcher.is_none());
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
