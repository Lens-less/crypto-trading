use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    future::Future,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::{Context, Result, bail};
use chrono::{Duration, Utc};
use crypto_trading_config::{
    ArbitrageConfig, GridConfig, MonitorConfig, PriceAlertConfig, ScannerConfig,
    load_arbitrage_config_from_str, load_exchange_auth_from_str, load_grid_config_from_str,
    load_monitor_config_from_str, load_price_alert_config_from_str, load_scanner_config_from_str,
    load_symbol_conversions_from_str, load_volume_maker_config_from_str, read_bounded_config,
};
use crypto_trading_control_plane::{
    SubmitCommand, SubmitEnvelope, SubmitPermission, SubmitReceipt, SubmitRiskConfirmation,
    SubmitRole, SubmitStatus,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, OrderType, Price, Quantity, Side, Symbol,
    TimeInForce,
};
use crypto_trading_exchange::{
    BinanceHmacSha256Signer, BinanceProduct, BinancePublicExchange, BinanceRequestSigner,
    BinanceTestnetEndpoints, BinanceTestnetExchange, BinanceTestnetProtocol, ExchangeError,
    ExchangeHandle, ExchangeSymbol, ExchangeSymbolCatalog, HyperliquidPublicEndpoint,
    HyperliquidPublicExchange, InstrumentRuleCatalog, InstrumentRules, PaperExchange,
    ReconcileScope, RemoteHttpTransport, ReqwestHttpTransport, SubmissionDisposition,
    TradingReceipt, hyperliquid_usdt_symbol_catalog,
};
use crypto_trading_runtime::{
    BinancePollingRoute, BinancePublicPollingSource, DecisionRecord,
    DeterministicMarketDataAdapter, ExchangeRouter, ExecutionBatch, ExecutionMode, ExecutionPolicy,
    HistoryError, HyperliquidPollingRoute, HyperliquidPublicPollingSource, IntentExecutor,
    JournalReadError, JsonlHistory, MAX_HISTORY_RECORD_BYTES, MarketDataBook, MarketDataError,
    MarketDataEvent, MarketDataEventFuture, MarketDataEventSource, MarketFreshnessPolicy,
    MarketInstrument, MarketPollingPolicy, MarketSupervisorConfig, MarketUniverse,
    PaperAccountAuthority, PaperAccountConfig, ReadOnlyTaskExit, ReadOnlyTaskFailure,
    ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel, ReadOnlyTaskRecovery, RuntimeError,
    SpreadHistoryWriter, SystemMarketDataClock, current_capability_manifest, read_journal_chain,
};
use crypto_trading_strategy::{
    AccountRiskSnapshot, ArbitrageDecision, ArbitrageState, ArbitrageStrategy, GridPlanner,
    GridState, GridStrategy, PairStrategyMachine, RiskDecision, RiskEngine, RiskLimits,
    StrategyMachine, VolumeMakerStrategy,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout_at},
};

use crate::alert::{
    AlertDeliveryMode, MAX_RECENT_ALERT_OCCURRENCES, NotificationDispatcherConfig,
    PriceAlertRuntime, PriceAlertRuntimeConfig,
};
use crate::cli::{
    ArbitrageArgs, CapabilitiesArgs, Cli, Command, ConfigCheckArgs, GridArgs, MonitorArgs,
    MonitorMode, PaperCommand, PaperMutationArgs, PaperOperation, PaperStartArgs, PaperStatusArgs,
    PaperTaskArgs, PriceAlertArgs, PriceAlertMode, ScannerArgs, ScannerMode, TestnetLifecycleArgs,
    TestnetLifecycleExpected, TestnetLifecycleMarket, TestnetLifecycleSide,
    TestnetLifecycleTimeInForce, TestnetReconciliationArgs, TestnetSmokeArgs, TestnetSoakArgs,
    TestnetSoakMode, VolumeMakerArgs,
};
use crate::continuous_alert::{
    ContinuousAlertTask, ContinuousAlertTaskConfig, ContinuousAlertTaskExit,
    ContinuousAlertTaskStatus,
};
use crate::continuous_monitor::{
    ContinuousMonitorTask, ContinuousMonitorTaskConfig, ContinuousMonitorTaskExit,
    ContinuousMonitorTaskStatus,
};
use crate::continuous_scanner::{
    ContinuousScannerTask, ContinuousScannerTaskConfig, ContinuousScannerTaskExit,
    ContinuousScannerTaskStatus, ScannerCandidateSpec, ScannerReplayRuntime,
};
use crate::monitor::{
    ArbitrageMonitorOutcome, ReadOnlyArbitrageMonitor, ReplayMarketDataClock,
    freshness_policy_from_monitor_config, load_market_snapshot_replay,
};
use crate::shutdown::{ShutdownSignalFuture, install_shutdown_signal};
use crate::task_host::{
    TaskHostControlCommand, TaskHostServeOutcome, control_addr, query_control,
    serve_host_with_shutdown,
};
use crate::testnet_lifecycle::{
    TESTNET_LIFECYCLE_ACKNOWLEDGEMENT, TestnetLifecycleConfig, TestnetLifecycleObservation,
    run_testnet_lifecycle,
};
use crate::testnet_reconciliation::{
    TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT, TestnetReconciliationConfig,
    TestnetReconciliationPlan, TestnetReconciliationReport, product_label,
};
use crate::testnet_soak::{
    MAX_TESTNET_SOAK_EVIDENCE_RECORDS, TESTNET_SOAK_SCHEMA_VERSION,
    TestnetSoakEvidenceRequirements, TestnetSoakProbe, TestnetSoakProbeFailure,
    TestnetSoakProbeFuture, TestnetSoakSample, TestnetSoakTask, TestnetSoakTaskConfig,
    TestnetSoakTaskExit, TestnetSoakTaskFailure, TestnetSoakTaskStatus,
    verify_testnet_soak_evidence,
};
// Volume-maker task-host imports are grouped here so the four-mode CLI wiring
// stays one coherent seam.
use crate::cli::VolumeMakerRunMode;
use crate::paper_volume_maker_task::{
    VolumeMakerPaperExecutionFuture, VolumeMakerPaperExecutor, VolumeMakerPaperTask,
    VolumeMakerPaperTaskConfig, VolumeMakerPaperTaskExit, VolumeMakerPaperTaskStatus,
};
use crypto_trading_config::VolumeMakerConfig;
use crypto_trading_runtime::{AccountRiskAuthority, PaperCostModel};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy};
use rust_decimal::prelude::ToPrimitive;

/// Runs one parsed CLI command.
///
/// # Errors
///
/// Returns an error when configuration, authority validation, strategy
/// evaluation, or paper execution fails.
pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Capabilities(args) => run_capabilities(&args),
        Command::TestnetSmoke(args) => run_testnet_smoke(&args).await,
        Command::TestnetLifecycle(args) => run_testnet_lifecycle_command(&args).await,
        Command::TestnetReconcile(args) => run_testnet_reconciliation_command(&args).await,
        Command::TestnetSoak(args) => run_testnet_soak(&args).await,
        Command::ConfigCheck(args) => check_configs(&args),
        Command::Grid(args) => run_grid(args).await,
        Command::Arbitrage(args) => run_arbitrage(&args).await,
        Command::Monitor(args) => run_monitor(&args).await,
        Command::VolumeMaker(args) => run_volume_maker(&args).await,
        Command::PriceAlert(args) => run_price_alert(&args).await,
        Command::Scanner(args) => run_scanner(&args).await,
        Command::Paper(args) => run_paper(args).await,
    }
}

fn run_capabilities(args: &CapabilitiesArgs) -> Result<()> {
    let manifest = current_capability_manifest();
    manifest.validate()?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&manifest)?);
        return Ok(());
    }

    println!(
        "capabilities schema={} version={} release={} live-trading={}",
        manifest.schema_version,
        manifest.product_version,
        manifest.release_stage,
        manifest.live_trading_enabled
    );
    println!("adapter\tpublic-data\ttestnet-protocol\tauthenticated\treconcile\tlive");
    for adapter in &manifest.adapters {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            adapter.id,
            adapter.public_data.level,
            adapter.testnet_protocol.level,
            adapter.authenticated.level,
            adapter.reconcile.level,
            adapter.live.level
        );
    }
    println!("capability\tarea\tlevel\taccess\tenvironments\tsummary");
    for capability in manifest.capabilities {
        let environments = capability
            .scope
            .environments
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            capability.id,
            capability.area,
            capability.level,
            capability.scope.access,
            environments,
            capability.summary
        );
    }
    Ok(())
}

const MIN_TRUSTED_BEARER_TOKEN_BYTES: usize = 32;
const MAX_TRUSTED_BEARER_TOKEN_BYTES: usize = 512;
const MAX_TRUSTED_ENV_VAR_BYTES: usize = 128;
const MAX_TRUSTED_HTTP_REQUEST_BODY_BYTES: usize = 32 * 1024;
const MAX_TRUSTED_HTTP_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
const MAX_TRUSTED_HTTP_RESPONSE_BODY_BYTES: usize = 256 * 1024;
const TRUSTED_HTTP_TIMEOUT: StdDuration = StdDuration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PaperTaskKind {
    Grid,
    Arbitrage,
}

impl PaperTaskKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Grid => "grid",
            Self::Arbitrage => "arbitrage",
        }
    }

    const fn task_kind(self) -> ReadOnlyTaskKind {
        match self {
            Self::Grid => ReadOnlyTaskKind::GridPaper,
            Self::Arbitrage => ReadOnlyTaskKind::ArbitragePaper,
        }
    }

    fn start_command(self, args: &PaperStartArgs) -> SubmitCommand {
        match self {
            Self::Grid => SubmitCommand::StartPaperGrid {
                strategy_id: args.strategy_id.clone(),
                strategy_revision: args.strategy_revision.clone(),
            },
            Self::Arbitrage => SubmitCommand::StartPaperArbitrage {
                strategy_id: args.strategy_id.clone(),
                strategy_revision: args.strategy_revision.clone(),
            },
        }
    }
}

struct TrustedControlContext {
    control_addr: std::net::SocketAddr,
    bearer_token: String,
}

struct TrustedHttpResponse {
    status_code: u16,
    body: Vec<u8>,
}

/// Exact operator acknowledgement required to engage the latching account
/// kill switch through the CLI. Any other phrase fails closed locally.
pub const ACCOUNT_KILL_SWITCH_ACKNOWLEDGEMENT: &str =
    "I ACKNOWLEDGE THE LATCHING ACCOUNT KILL SWITCH";

async fn run_paper(command: PaperCommand) -> Result<()> {
    match command {
        PaperCommand::Grid(args) => run_paper_task(PaperTaskKind::Grid, args).await,
        PaperCommand::Arbitrage(args) => run_paper_task(PaperTaskKind::Arbitrage, args).await,
        PaperCommand::Risk(args) => run_paper_risk(args).await,
    }
}

async fn run_paper_risk(args: crate::cli::PaperRiskArgs) -> Result<()> {
    use crate::cli::PaperRiskOperation;
    let (operation, command, confirmation, mutation) = match args.operation {
        PaperRiskOperation::Pause(args) => (
            "risk-pause",
            SubmitCommand::PauseAccountRisk {
                reason: args.reason,
            },
            SubmitRiskConfirmation::PaperOnly,
            args.mutation,
        ),
        PaperRiskOperation::Resume(mutation) => (
            "risk-resume",
            SubmitCommand::ResumeAccountRisk,
            SubmitRiskConfirmation::PaperOnly,
            mutation,
        ),
        PaperRiskOperation::KillSwitch(args) => {
            if args.acknowledge != ACCOUNT_KILL_SWITCH_ACKNOWLEDGEMENT {
                bail!(
                    "account kill switch requires the exact acknowledgement phrase: {ACCOUNT_KILL_SWITCH_ACKNOWLEDGEMENT:?}"
                );
            }
            (
                "risk-kill-switch",
                SubmitCommand::EngageAccountKillSwitch {
                    reason: args.reason,
                },
                SubmitRiskConfirmation::AccountKillSwitchArmed,
                args.mutation,
            )
        }
    };
    let permission =
        SubmitPermission::new(mutation.principal_id.clone(), SubmitRole::PaperOperator)
            .context("invalid paper trusted-submit principal")?;
    let envelope = SubmitEnvelope::new(
        mutation.command_id,
        mutation.idempotency_key.clone(),
        mutation.task_id.clone(),
        permission,
        confirmation,
        command,
    )
    .context("invalid trusted submit envelope")?;
    submit_paper_envelope("risk", operation, &mutation.control, envelope).await
}

async fn run_paper_task(kind: PaperTaskKind, args: PaperTaskArgs) -> Result<()> {
    match args.operation {
        PaperOperation::Start(args) => run_paper_start(kind, &args).await,
        PaperOperation::Status(args) => run_paper_status(kind, &args).await,
        PaperOperation::Stop(args) => {
            run_paper_mutation(kind, "stop", SubmitCommand::StopTask, &args).await
        }
        PaperOperation::Cancel(args) => {
            run_paper_mutation(kind, "cancel", SubmitCommand::CancelTask, &args).await
        }
    }
}

async fn run_paper_start(kind: PaperTaskKind, args: &PaperStartArgs) -> Result<()> {
    let permission = SubmitPermission::new(
        args.mutation.principal_id.clone(),
        SubmitRole::PaperOperator,
    )
    .context("invalid paper trusted-submit principal")?;
    let envelope = SubmitEnvelope::new(
        args.mutation.command_id,
        args.mutation.idempotency_key.clone(),
        args.mutation.task_id.clone(),
        permission,
        SubmitRiskConfirmation::PaperOnly,
        kind.start_command(args),
    )
    .context("invalid trusted submit envelope")?;
    submit_paper_command(kind, "start", &args.mutation.control, envelope).await
}

async fn run_paper_mutation(
    kind: PaperTaskKind,
    operation: &str,
    command: SubmitCommand,
    args: &PaperMutationArgs,
) -> Result<()> {
    let permission = SubmitPermission::new(args.principal_id.clone(), SubmitRole::PaperOperator)
        .context("invalid paper trusted-submit principal")?;
    let envelope = SubmitEnvelope::new(
        args.command_id,
        args.idempotency_key.clone(),
        args.task_id.clone(),
        permission,
        SubmitRiskConfirmation::PaperOnly,
        command,
    )
    .context("invalid trusted submit envelope")?;
    submit_paper_command(kind, operation, &args.control, envelope).await
}

async fn submit_paper_command(
    kind: PaperTaskKind,
    operation: &str,
    control: &crate::cli::TrustedControlArgs,
    envelope: SubmitEnvelope,
) -> Result<()> {
    submit_paper_envelope(kind.label(), operation, control, envelope).await
}

async fn submit_paper_envelope(
    label: &'static str,
    operation: &str,
    control: &crate::cli::TrustedControlArgs,
    envelope: SubmitEnvelope,
) -> Result<()> {
    let control = trusted_control_context(control.control_addr, &control.token_env_var)?;
    let body =
        serde_json::to_vec(&envelope).context("failed to serialize trusted submit envelope")?;
    if body.len() > MAX_TRUSTED_HTTP_REQUEST_BODY_BYTES {
        bail!("trusted submit envelope exceeded the bounded request body limit");
    }
    let response = trusted_http_json_request(
        "POST",
        control.control_addr,
        "/api/v1/submit",
        &control.bearer_token,
        Some(&body),
    )
    .await?;

    match response.status_code {
        200 | 202 | 422 => {
            let receipt: SubmitReceipt = serde_json::from_slice(&response.body)
                .context("trusted submit response did not match SubmitReceipt")?;
            render_submit_receipt(label, operation, &receipt);
            match receipt.status() {
                SubmitStatus::Applied => Ok(()),
                SubmitStatus::Rejected => {
                    bail!("{label} paper {operation} rejected by trusted submit")
                }
                SubmitStatus::OutcomeUnknown => bail!(
                    "{label} paper {operation} returned outcome_unknown and is not confirmed applied"
                ),
            }
        }
        _ => bail!(
            "trusted submit {} failed with HTTP {}: {}",
            operation,
            response.status_code,
            bounded_http_error(&response.body)
        ),
    }
}

async fn run_paper_status(kind: PaperTaskKind, args: &PaperStatusArgs) -> Result<()> {
    let control = trusted_control_context(args.control.control_addr, &args.control.token_env_var)?;
    let response = trusted_http_json_request(
        "GET",
        control.control_addr,
        "/api/v1/tasks",
        &control.bearer_token,
        None,
    )
    .await?;
    if response.status_code != 200 {
        bail!(
            "trusted task status failed with HTTP {}: {}",
            response.status_code,
            bounded_http_error(&response.body)
        );
    }
    let model: ReadOnlyTaskReadModel = serde_json::from_slice(&response.body)
        .context("task status response did not match ReadOnlyTaskReadModel")?;
    let task = model
        .tasks
        .iter()
        .find(|task| task.task_id == args.task_id)
        .with_context(|| format!("task {} not found in /api/v1/tasks", args.task_id))?;
    if task.kind != kind.task_kind() {
        bail!(
            "task {} is kind={}, expected kind={} for paper {} status",
            task.task_id,
            task_kind_name(task.kind),
            task_kind_name(kind.task_kind()),
            kind.label()
        );
    }

    print!(
        "projection_status={}\njournal_head_sequence={}\ninvalid_event_count={}\ntask_id={}\nkind={}\nphase={}\nrecovery={}\nprocessed_event_count={}\nupdated_at={}\nexit={}\nfailure={}\n",
        projection_status_name(model.projection_status),
        model
            .journal_head_sequence
            .map_or_else(|| "none".to_owned(), |sequence| sequence.to_string()),
        model.invalid_event_count,
        task.task_id,
        task_kind_name(task.kind),
        task_phase_name(task.phase),
        task_recovery_name(task.recovery),
        task.processed_event_count,
        task.updated_at.to_rfc3339(),
        task_exit_name(task.exit),
        task_failure_name(task.failure),
    );
    Ok(())
}

fn trusted_control_context(
    control_addr: std::net::SocketAddr,
    token_env_var: &str,
) -> Result<TrustedControlContext> {
    if !control_addr.ip().is_loopback() {
        bail!("trusted paper control address must stay on loopback: {control_addr}");
    }
    validate_env_var_name(token_env_var)?;
    let bearer_token = std::env::var(token_env_var)
        .with_context(|| format!("trusted bearer token env var {token_env_var} is not set"))?;
    if !(MIN_TRUSTED_BEARER_TOKEN_BYTES..=MAX_TRUSTED_BEARER_TOKEN_BYTES)
        .contains(&bearer_token.len())
    {
        bail!(
            "trusted bearer token from {token_env_var} has {} bytes; expected {}..={}",
            bearer_token.len(),
            MIN_TRUSTED_BEARER_TOKEN_BYTES,
            MAX_TRUSTED_BEARER_TOKEN_BYTES
        );
    }
    if bearer_token.chars().any(char::is_control) {
        bail!("trusted bearer token from {token_env_var} must not contain control characters");
    }
    Ok(TrustedControlContext {
        control_addr,
        bearer_token,
    })
}

fn validate_env_var_name(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("trusted bearer token env var name must not be empty");
    }
    if value.len() > MAX_TRUSTED_ENV_VAR_BYTES {
        bail!("trusted bearer token env var name exceeds {MAX_TRUSTED_ENV_VAR_BYTES} bytes");
    }
    if value.trim() != value {
        bail!("trusted bearer token env var name must not have surrounding whitespace");
    }
    if value.chars().any(char::is_control) {
        bail!("trusted bearer token env var name must not contain control characters");
    }
    if value.chars().any(|character| {
        !(character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_')
    }) {
        bail!("trusted bearer token env var name must use only ASCII A-Z, digits, or _");
    }
    Ok(())
}

async fn trusted_http_json_request(
    method: &str,
    address: std::net::SocketAddr,
    path: &str,
    bearer_token: &str,
    body: Option<&[u8]>,
) -> Result<TrustedHttpResponse> {
    let request = build_trusted_http_request(method, address, path, bearer_token, body)?;
    let deadline = Instant::now() + TRUSTED_HTTP_TIMEOUT;
    let mut stream = timeout_at(deadline, TcpStream::connect(address))
        .await
        .context("trusted HTTP transaction timed out during connect")?
        .with_context(|| format!("failed to connect to trusted paper endpoint {address}"))?;
    timeout_at(deadline, stream.write_all(&request))
        .await
        .context("trusted HTTP transaction timed out during write")?
        .context("failed to write trusted HTTP request")?;
    timeout_at(deadline, stream.shutdown())
        .await
        .context("trusted HTTP transaction timed out during shutdown")?
        .context("failed to finish trusted HTTP request")?;

    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = timeout_at(deadline, stream.read(&mut buffer))
            .await
            .context("trusted HTTP transaction timed out during read")?
            .context("failed to read trusted HTTP response")?;
        if read == 0 {
            break;
        }
        response.extend_from_slice(&buffer[..read]);
        if response.len()
            > MAX_TRUSTED_HTTP_RESPONSE_HEADER_BYTES + MAX_TRUSTED_HTTP_RESPONSE_BODY_BYTES + 4
        {
            bail!("trusted HTTP response exceeded the bounded header/body limit");
        }
    }

    parse_trusted_http_response(&response)
}

fn build_trusted_http_request(
    method: &str,
    address: std::net::SocketAddr,
    path: &str,
    bearer_token: &str,
    body: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let body = body.unwrap_or_default();
    if body.len() > MAX_TRUSTED_HTTP_REQUEST_BODY_BYTES {
        bail!("trusted HTTP request body exceeded the bounded limit");
    }
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {bearer_token}\r\nAccept: application/json\r\nConnection: close\r\n"
    )
    .into_bytes();
    if body.is_empty() {
        request.extend_from_slice(b"Content-Length: 0\r\n");
    } else {
        request.extend_from_slice(
            format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            )
            .as_bytes(),
        );
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    Ok(request)
}

fn parse_trusted_http_response(response: &[u8]) -> Result<TrustedHttpResponse> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        bail!("trusted HTTP response headers were incomplete");
    };
    if header_end > MAX_TRUSTED_HTTP_RESPONSE_HEADER_BYTES {
        bail!("trusted HTTP response headers exceeded the bounded limit");
    }
    let headers = std::str::from_utf8(&response[..header_end])
        .context("trusted HTTP response headers were not valid UTF-8")?;
    let mut lines = headers.split("\r\n");
    let status_line = lines
        .next()
        .context("trusted HTTP response was missing a status line")?;
    let mut status_parts = status_line.split_whitespace();
    let version = status_parts
        .next()
        .context("trusted HTTP response status line was malformed")?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        bail!("trusted HTTP response must use HTTP/1.0 or HTTP/1.1");
    }
    let status_code = status_parts
        .next()
        .context("trusted HTTP response status line was malformed")?
        .parse::<u16>()
        .context("trusted HTTP response status code was invalid")?;

    let mut content_length = None;
    let mut content_type = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            bail!("trusted HTTP response header line was malformed");
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                bail!("trusted HTTP response duplicated content-length");
            }
            let parsed = value
                .parse::<usize>()
                .context("trusted HTTP response content-length was invalid")?;
            if parsed > MAX_TRUSTED_HTTP_RESPONSE_BODY_BYTES {
                bail!("trusted HTTP response body exceeded the bounded limit");
            }
            content_length = Some(parsed);
        } else if name.eq_ignore_ascii_case("content-type") {
            if content_type.is_some() {
                bail!("trusted HTTP response duplicated content-type");
            }
            content_type = Some(value.to_owned());
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            bail!("trusted HTTP response transfer-encoding is unsupported");
        }
    }
    let content_length = content_length.context("trusted HTTP response omitted content-length")?;
    let body_start = header_end + 4;
    if response.len() != body_start + content_length {
        bail!("trusted HTTP response body length did not match content-length");
    }
    if content_length > 0
        && !content_type
            .as_deref()
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        bail!("trusted HTTP response content type must be application/json");
    }

    Ok(TrustedHttpResponse {
        status_code,
        body: response[body_start..].to_vec(),
    })
}

fn render_submit_receipt(label: &'static str, operation: &str, receipt: &SubmitReceipt) {
    println!(
        "paper={}\noperation={}\ncommand_id={}\ntask_id={}\nstatus={}\njournal_projection={}\nsource={}",
        label,
        operation,
        receipt.command_id(),
        receipt.target_task_id(),
        submit_status_name(receipt.status()),
        receipt.journal_projection(),
        receipt.source(),
    );
}

fn bounded_http_error(body: &[u8]) -> String {
    if body.is_empty() {
        return "empty response body".to_owned();
    }
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(message) = value
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| value.get("error").and_then(Value::as_str))
        {
            return message.to_owned();
        }
        return bounded_text(
            &value.to_string(),
            MAX_TRUSTED_HTTP_RESPONSE_BODY_BYTES.min(512),
        );
    }
    bounded_text(
        &String::from_utf8_lossy(body),
        MAX_TRUSTED_HTTP_RESPONSE_BODY_BYTES.min(512),
    )
}

const fn submit_status_name(status: SubmitStatus) -> &'static str {
    match status {
        SubmitStatus::Applied => "applied",
        SubmitStatus::Rejected => "rejected",
        SubmitStatus::OutcomeUnknown => "outcome_unknown",
    }
}

const fn projection_status_name(status: crypto_trading_runtime::ProjectionStatus) -> &'static str {
    match status {
        crypto_trading_runtime::ProjectionStatus::Complete => "complete",
        crypto_trading_runtime::ProjectionStatus::Windowed => "windowed",
        crypto_trading_runtime::ProjectionStatus::Degraded => "degraded",
    }
}

const fn task_kind_name(kind: ReadOnlyTaskKind) -> &'static str {
    match kind {
        ReadOnlyTaskKind::ArbitrageMonitor => "arbitrage_monitor",
        ReadOnlyTaskKind::ArbitragePaper => "arbitrage_paper",
        ReadOnlyTaskKind::GridPaper => "grid_paper",
        ReadOnlyTaskKind::PriceAlert => "price_alert",
        ReadOnlyTaskKind::Scanner => "scanner",
        ReadOnlyTaskKind::VolumeMaker => "volume_maker",
    }
}

const fn task_phase_name(phase: ReadOnlyTaskPhase) -> &'static str {
    match phase {
        ReadOnlyTaskPhase::Registered => "registered",
        ReadOnlyTaskPhase::Running => "running",
        ReadOnlyTaskPhase::Stopping => "stopping",
        ReadOnlyTaskPhase::Stopped => "stopped",
        ReadOnlyTaskPhase::Failed => "failed",
    }
}

const fn task_recovery_name(recovery: ReadOnlyTaskRecovery) -> &'static str {
    match recovery {
        ReadOnlyTaskRecovery::None => "none",
        ReadOnlyTaskRecovery::Investigate => "investigate",
    }
}

const fn task_exit_name(exit: Option<ReadOnlyTaskExit>) -> &'static str {
    match exit {
        Some(ReadOnlyTaskExit::StopRequested) => "stop_requested",
        Some(ReadOnlyTaskExit::SourceEnded) => "source_ended",
        Some(ReadOnlyTaskExit::ShutdownTimedOut) => "shutdown_timed_out",
        Some(ReadOnlyTaskExit::Completed) => "completed",
        None => "none",
    }
}

const fn task_failure_name(failure: Option<ReadOnlyTaskFailure>) -> &'static str {
    match failure {
        Some(ReadOnlyTaskFailure::StartupFailed) => "startup_failed",
        Some(ReadOnlyTaskFailure::SourceContract) => "source_contract",
        Some(ReadOnlyTaskFailure::MonitorContract) => "monitor_contract",
        Some(ReadOnlyTaskFailure::JournalUnavailable) => "journal_unavailable",
        Some(ReadOnlyTaskFailure::TaskPanicked) => "task_panicked",
        Some(ReadOnlyTaskFailure::TaskCancelled) => "task_cancelled",
        Some(ReadOnlyTaskFailure::InvalidRequest) => "invalid_request",
        Some(ReadOnlyTaskFailure::RecoveryRequired) => "recovery_required",
        Some(ReadOnlyTaskFailure::AccountContract) => "account_contract",
        Some(ReadOnlyTaskFailure::ExecutionIncomplete) => "execution_incomplete",
        Some(ReadOnlyTaskFailure::ExecutionFailed) => "execution_failed",
        None => "none",
    }
}

struct BinanceSmokeSymbols {
    spot: Symbol,
    perpetual: Symbol,
    wire_symbol: String,
}

async fn run_testnet_smoke(args: &TestnetSmokeArgs) -> Result<()> {
    if !args.call_book_ticker && !args.call_reconcile {
        bail!(
            "testnet-smoke is inert unless --call-book-ticker and/or --call-reconcile is selected"
        );
    }
    if args.timeout_ms == 0 {
        bail!("--timeout-ms must be greater than zero");
    }

    let symbols = BinanceSmokeSymbols {
        spot: Symbol::new(args.spot_symbol.clone()).context("invalid --spot-symbol")?,
        perpetual: Symbol::new(args.perpetual_symbol.clone())
            .context("invalid --perpetual-symbol")?,
        wire_symbol: args.wire_symbol.clone(),
    };
    let transport = Arc::new(ReqwestHttpTransport::new(StdDuration::from_millis(
        args.timeout_ms,
    ))?);

    let mut checks = Vec::new();

    if args.call_book_ticker {
        checks.push(run_book_ticker_check(&transport, &symbols).await?);
    }

    if args.call_reconcile {
        checks.push(run_reconcile_check(&transport, &symbols).await?);
    }

    if args.json {
        let report = json!({
            "exchange": "binance",
            "timeout_ms": args.timeout_ms,
            "checks": checks,
        });
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_testnet_smoke_checks(&checks);
    Ok(())
}

async fn run_testnet_lifecycle_command(args: &TestnetLifecycleArgs) -> Result<()> {
    if args.acknowledge_testnet_lifecycle != TESTNET_LIFECYCLE_ACKNOWLEDGEMENT {
        bail!(
            "testnet-lifecycle requires --acknowledge-testnet-lifecycle \"{TESTNET_LIFECYCLE_ACKNOWLEDGEMENT}\""
        );
    }
    if args.timeout_ms == 0 {
        bail!("testnet-lifecycle requires --timeout-ms > 0");
    }
    if args.reduce_only && args.market == TestnetLifecycleMarket::Spot {
        bail!("testnet-lifecycle --reduce-only is only valid with --market usdm");
    }

    let symbols = BinanceSmokeSymbols {
        spot: Symbol::new(args.spot_symbol.clone()).context("invalid --spot-symbol")?,
        perpetual: Symbol::new(args.perpetual_symbol.clone())
            .context("invalid --perpetual-symbol")?,
        wire_symbol: args.wire_symbol.clone(),
    };
    let (symbol, market_type) = match args.market {
        TestnetLifecycleMarket::Spot => (symbols.spot.clone(), MarketType::Spot),
        TestnetLifecycleMarket::Usdm => (symbols.perpetual.clone(), MarketType::Perpetual),
    };
    let side = match args.side {
        TestnetLifecycleSide::Buy => Side::Buy,
        TestnetLifecycleSide::Sell => Side::Sell,
    };
    let time_in_force = match args.time_in_force {
        TestnetLifecycleTimeInForce::Gtc => TimeInForce::Gtc,
        TestnetLifecycleTimeInForce::PostOnly => TimeInForce::PostOnly,
    };
    let expected_observation = match args.expected_observation {
        TestnetLifecycleExpected::Open => TestnetLifecycleObservation::Open,
        TestnetLifecycleExpected::PartiallyFilled => TestnetLifecycleObservation::PartiallyFilled,
    };
    let quantity = Quantity::new(args.quantity).context("invalid --quantity")?;
    let price = Price::new(args.price).context("invalid --price")?;
    let mut intent = OrderIntent::limit("binance", symbol, market_type, side, quantity, price);
    intent.client_order_id = args.client_order_id;
    intent.time_in_force = time_in_force;
    intent.reduce_only = args.reduce_only;
    let config = TestnetLifecycleConfig::new(
        args.campaign_id.clone(),
        intent.clone(),
        expected_observation,
        StdDuration::from_millis(args.poll_interval_ms),
        args.maximum_queries,
    )?;

    let (api_key, api_secret) = load_binance_testnet_credentials()?;
    let signer = Arc::new(BinanceHmacSha256Signer::new(api_key, api_secret)?);
    let protocol = build_binance_testnet_protocol(signer, &symbols)?;
    let preflight_timestamp = u64::try_from(Utc::now().timestamp_millis())
        .context("current timestamp is outside the Binance millisecond range")?;
    protocol
        .build_order_request(&intent, Some(price), preflight_timestamp)
        .context("testnet lifecycle order failed local protocol validation")?;
    let transport: Arc<dyn RemoteHttpTransport> = Arc::new(ReqwestHttpTransport::new(
        StdDuration::from_millis(args.timeout_ms),
    )?);
    let exchange = BinanceTestnetExchange::new(protocol, transport);
    let history = JsonlHistory::new(&args.history_path);
    let report = run_testnet_lifecycle(&config, &exchange, &history).await?;

    let expected = lifecycle_observation_label(report.expected_observation);
    let final_status = lifecycle_order_status_label(report.final_status);
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "exchange": "binance",
                "authority": "testnet",
                "mainnet_enabled": false,
                "campaign_id": report.campaign_id,
                "client_order_id": report.client_order_id,
                "server_order_id": report.server_order_id,
                "expected_observation": expected,
                "final_status": final_status,
                "query_count": report.query_count,
                "recovered": report.recovered,
                "evidence_path": args.history_path,
            }))?
        );
        return Ok(());
    }
    println!(
        "exchange=binance\nauthority=testnet\nmainnet_enabled=false\ncampaign_id={}\nclient_order_id={}\nserver_order_id={}\nexpected_observation={expected}\nfinal_status={final_status}\nquery_count={}\nrecovered={}\nevidence_path={}",
        report.campaign_id,
        report.client_order_id,
        report.server_order_id,
        report.query_count,
        report.recovered,
        args.history_path.display(),
    );
    Ok(())
}

const fn lifecycle_observation_label(observation: TestnetLifecycleObservation) -> &'static str {
    match observation {
        TestnetLifecycleObservation::Open => "open",
        TestnetLifecycleObservation::PartiallyFilled => "partially_filled",
    }
}

const fn lifecycle_order_status_label(status: crypto_trading_domain::OrderStatus) -> &'static str {
    match status {
        crypto_trading_domain::OrderStatus::Pending => "pending",
        crypto_trading_domain::OrderStatus::Open => "open",
        crypto_trading_domain::OrderStatus::PartiallyFilled => "partially_filled",
        crypto_trading_domain::OrderStatus::Filled => "filled",
        crypto_trading_domain::OrderStatus::Cancelled => "cancelled",
        crypto_trading_domain::OrderStatus::Rejected => "rejected",
    }
}

async fn run_testnet_reconciliation_command(args: &TestnetReconciliationArgs) -> Result<()> {
    if let Some(acknowledgement) = args.apply_reconciliation.as_deref()
        && acknowledgement != TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT
    {
        bail!(
            "testnet-reconcile --apply-reconciliation requires \"{TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT}\""
        );
    }
    if args.timeout_ms == 0 {
        bail!("testnet-reconcile requires --timeout-ms > 0");
    }

    let symbols = BinanceSmokeSymbols {
        spot: Symbol::new(args.spot_symbol.clone()).context("invalid --spot-symbol")?,
        perpetual: Symbol::new(args.perpetual_symbol.clone())
            .context("invalid --perpetual-symbol")?,
        wire_symbol: args.wire_symbol.clone(),
    };
    let (product, symbol) = match args.market {
        TestnetLifecycleMarket::Spot => (BinanceProduct::Spot, symbols.spot.clone()),
        TestnetLifecycleMarket::Usdm => (BinanceProduct::UsdM, symbols.perpetual.clone()),
    };
    let reconciliation_config = TestnetReconciliationConfig::new(
        product,
        args.settlement_asset.clone(),
        symbol,
        args.reservation_id,
    )?;
    let account_config =
        PaperAccountConfig::new(args.account_id.clone(), Money::new(args.initial_available))
            .context("invalid Paper account reconciliation configuration")?;
    let history = JsonlHistory::new(&args.history_path);
    let authority = PaperAccountAuthority::new(args.journal_id, history, account_config)
        .context("failed to open the Paper account reconciliation authority")?;
    let account = authority
        .snapshot()
        .await
        .context("failed to load the Paper account reconciliation snapshot")?;
    let plan = TestnetReconciliationPlan::new(reconciliation_config, account)?;

    let (api_key, api_secret) = load_binance_testnet_credentials()?;
    let signer = Arc::new(BinanceHmacSha256Signer::new(api_key, api_secret)?);
    let protocol = build_binance_testnet_protocol(signer, &symbols)?;
    let transport: Arc<dyn RemoteHttpTransport> = Arc::new(ReqwestHttpTransport::new(
        StdDuration::from_millis(args.timeout_ms),
    )?);
    let exchange = BinanceTestnetExchange::new(protocol, transport);
    let remote = exchange
        .account_snapshot(product)
        .await
        .context("failed to sample complete Binance Testnet account truth")?;
    let report = plan.compare(&remote, Utc::now())?;
    let applied_outcome =
        apply_testnet_reconciliation(&authority, &report, args.apply_reconciliation.is_some())
            .await?;
    print_testnet_reconciliation(args, &report, applied_outcome)?;
    let mismatch_codes = report
        .mismatches
        .iter()
        .map(|mismatch| mismatch.code())
        .collect::<Vec<_>>();
    if !report.matches() {
        bail!(
            "Binance Testnet account truth did not match the Paper release gate: {}",
            mismatch_codes.join(",")
        );
    }
    Ok(())
}

async fn apply_testnet_reconciliation(
    authority: &PaperAccountAuthority,
    report: &TestnetReconciliationReport,
    apply: bool,
) -> Result<Option<&'static str>> {
    if !apply {
        return Ok(None);
    }
    if report.matches() {
        authority
            .reconcile_release(report.proof.clone())
            .await
            .context("failed to apply the verified Paper reconciliation release")?;
        return Ok(Some("released"));
    }
    authority
        .record_reconciliation_failure(report.proof.clone())
        .await
        .context("failed to record the Paper reconciliation failure")?;
    Ok(Some("failure_recorded"))
}

fn print_testnet_reconciliation(
    args: &TestnetReconciliationArgs,
    report: &TestnetReconciliationReport,
    applied_outcome: Option<&str>,
) -> Result<()> {
    let mismatch_codes = report
        .mismatches
        .iter()
        .map(|mismatch| mismatch.code())
        .collect::<Vec<_>>();
    let expected_available = report.expected_available.normalize().to_string();
    let observed_wallet = report
        .observed_wallet
        .map(|value| value.normalize().to_string());
    let observed_available = report
        .observed_available
        .map(|value| value.normalize().to_string());
    let observed_locked = report
        .observed_locked
        .map(|value| value.normalize().to_string());
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "schema_version": report.schema_version,
                "exchange": "binance",
                "authority": "testnet",
                "mainnet_enabled": false,
                "scope": "clean_account_release_gate",
                "product": product_label(report.product),
                "settlement_asset": &report.settlement_asset,
                "account_id": &report.account_id,
                "reservation_id": report.reservation_id,
                "batch_id": report.batch_id,
                "matches": report.matches(),
                "expected_available": &expected_available,
                "observed_wallet": &observed_wallet,
                "observed_available": &observed_available,
                "observed_locked": &observed_locked,
                "owned_order_count": report.owned_order_count,
                "foreign_order_count": report.foreign_order_count,
                "position_count": report.position_count,
                "observed_at": report.observed_at,
                "captured_at": report.captured_at,
                "mismatches": &mismatch_codes,
                "proof": &report.proof,
                "mutation_requested": args.apply_reconciliation.is_some(),
                "applied_outcome": applied_outcome,
                "evidence_path": &args.history_path,
            }))?
        );
        return Ok(());
    }
    println!(
        "exchange=binance\nauthority=testnet\nmainnet_enabled=false\nscope=clean_account_release_gate\nproduct={}\nsettlement_asset={}\naccount_id={}\nreservation_id={}\nmatches={}\nexpected_available={expected_available}\nobserved_available={}\nowned_order_count={}\nforeign_order_count={}\nposition_count={}\nmismatches={}\napplied_outcome={}\nevidence_path={}",
        product_label(report.product),
        report.settlement_asset,
        report.account_id,
        report.reservation_id,
        report.matches(),
        observed_available.as_deref().unwrap_or("missing"),
        report.owned_order_count,
        report.foreign_order_count,
        report.position_count,
        mismatch_codes.join(","),
        applied_outcome.unwrap_or("none"),
        args.history_path.display(),
    );
    Ok(())
}

async fn run_book_ticker_check(
    transport: &Arc<ReqwestHttpTransport>,
    symbols: &BinanceSmokeSymbols,
) -> Result<Value> {
    let signer = Arc::new(BinanceHmacSha256Signer::new(
        "offline-api-key",
        "offline-api-secret",
    )?);
    let protocol = build_binance_testnet_protocol(signer, symbols)?;
    let spot =
        fetch_binance_book_ticker(&protocol, &**transport, &symbols.spot, MarketType::Spot).await?;
    let perpetual = fetch_binance_book_ticker(
        &protocol,
        &**transport,
        &symbols.perpetual,
        MarketType::Perpetual,
    )
    .await?;
    Ok(json!({
        "name": "book-ticker",
        "spot": spot,
        "perpetual": perpetual,
    }))
}

async fn run_reconcile_check(
    transport: &Arc<ReqwestHttpTransport>,
    symbols: &BinanceSmokeSymbols,
) -> Result<Value> {
    let (api_key, api_secret) = load_binance_testnet_credentials()?;
    let signer = Arc::new(BinanceHmacSha256Signer::new(api_key, api_secret)?);
    let protocol = build_binance_testnet_protocol(signer, symbols)?;
    let exchange = BinanceTestnetExchange::new(protocol, transport.clone());
    let spot_orders = exchange
        .reconcile(ReconcileScope::Orders {
            symbol: Some(symbols.spot.clone()),
        })
        .await?;
    let perpetual_orders = exchange
        .reconcile(ReconcileScope::Orders {
            symbol: Some(symbols.perpetual.clone()),
        })
        .await?;
    let positions = exchange
        .reconcile(ReconcileScope::Positions {
            symbol: Some(symbols.perpetual.clone()),
        })
        .await?;
    Ok(json!({
        "name": "reconcile",
        "spot_orders": summarize_reconcile_receipt(&spot_orders),
        "perpetual_orders": summarize_reconcile_receipt(&perpetual_orders),
        "positions": summarize_reconcile_receipt(&positions),
    }))
}

fn print_testnet_smoke_checks(checks: &[Value]) {
    println!(
        "binance testnet smoke completed: checks={}",
        checks
            .iter()
            .map(|check| check["name"].as_str().unwrap_or("unknown"))
            .collect::<Vec<_>>()
            .join(",")
    );
    for check in checks {
        match check["name"].as_str() {
            Some("book-ticker") => {
                println!(
                    "book-ticker spot={} bid={} ask={} | perpetual={} bid={} ask={}",
                    check["spot"]["symbol"].as_str().unwrap_or("?"),
                    check["spot"]["bid"].as_str().unwrap_or("?"),
                    check["spot"]["ask"].as_str().unwrap_or("?"),
                    check["perpetual"]["symbol"].as_str().unwrap_or("?"),
                    check["perpetual"]["bid"].as_str().unwrap_or("?"),
                    check["perpetual"]["ask"].as_str().unwrap_or("?"),
                );
            }
            Some("reconcile") => {
                println!(
                    "reconcile spot_orders={} spot_foreign={} perpetual_orders={} perpetual_foreign={} positions={}",
                    check["spot_orders"]["orders"].as_u64().unwrap_or(0),
                    check["spot_orders"]["foreign_orders"].as_u64().unwrap_or(0),
                    check["perpetual_orders"]["orders"].as_u64().unwrap_or(0),
                    check["perpetual_orders"]["foreign_orders"]
                        .as_u64()
                        .unwrap_or(0),
                    check["positions"]["positions"].as_u64().unwrap_or(0),
                );
            }
            _ => {}
        }
    }
}

fn load_binance_testnet_credentials() -> Result<(String, String)> {
    let auth =
        load_exchange_auth_from_str("binance", "binance:\n  api_key: \"\"\n  api_secret: \"\"\n")
            .context("failed to load Binance credential overrides from the environment")?;
    let api_key = auth
        .api_key
        .expose_secret()
        .context("authenticated Binance Testnet commands require BINANCE_API_KEY")?
        .to_owned();
    let api_secret = auth
        .api_secret
        .expose_secret()
        .context("authenticated Binance Testnet commands require BINANCE_API_SECRET")?
        .to_owned();
    Ok((api_key, api_secret))
}

fn build_binance_testnet_protocol<S>(
    signer: Arc<S>,
    symbols: &BinanceSmokeSymbols,
) -> Result<BinanceTestnetProtocol>
where
    S: BinanceRequestSigner + 'static,
{
    let tick_size = Price::new(Decimal::new(1, 1)).expect("0.1 must be valid");
    let spot_quantity = Quantity::new(Decimal::new(1, 4)).expect("0.0001 must be valid");
    let perpetual_quantity = Quantity::new(Decimal::new(1, 3)).expect("0.001 must be valid");
    let min_notional = Money::new(Decimal::new(5, 0));
    let catalog = ExchangeSymbolCatalog::new(vec![
        ExchangeSymbol::new(
            "binance",
            symbols.spot.clone(),
            MarketType::Spot,
            &symbols.wire_symbol,
        )?,
        ExchangeSymbol::new(
            "binance",
            symbols.perpetual.clone(),
            MarketType::Perpetual,
            &symbols.wire_symbol,
        )?,
    ])?;
    let rules = InstrumentRuleCatalog::new(vec![
        InstrumentRules::new(
            "binance",
            symbols.spot.clone(),
            MarketType::Spot,
            tick_size,
            spot_quantity,
            spot_quantity,
            min_notional,
        )?,
        InstrumentRules::new(
            "binance",
            symbols.perpetual.clone(),
            MarketType::Perpetual,
            tick_size,
            perpetual_quantity,
            perpetual_quantity,
            min_notional,
        )?,
    ])?;
    BinanceTestnetProtocol::authenticated(
        BinanceTestnetEndpoints::official(),
        catalog,
        rules,
        signer,
    )
    .context("failed to build Binance testnet smoke protocol")
}

async fn fetch_binance_book_ticker(
    protocol: &BinanceTestnetProtocol,
    transport: &(dyn RemoteHttpTransport + Send + Sync),
    symbol: &Symbol,
    market_type: MarketType,
) -> std::result::Result<MarketSnapshot, ExchangeError> {
    let request = protocol.build_book_ticker_request(symbol, market_type)?;
    let product = match market_type {
        MarketType::Spot => BinanceProduct::Spot,
        MarketType::Perpetual => BinanceProduct::UsdM,
    };
    let response = transport.send(request).await?;
    if !response.is_success() {
        return Err(BinanceTestnetProtocol::remote_failure_from_response(
            &response,
        ));
    }
    let received_at = response.server_time().unwrap_or_else(Utc::now);
    protocol.parse_book_ticker(product, response.body(), received_at)
}

fn summarize_reconcile_receipt(receipt: &crypto_trading_exchange::ReconcileReceipt) -> Value {
    json!({
        "orders": receipt.orders.len(),
        "foreign_orders": receipt.foreign_orders.len(),
        "positions": receipt.positions.len(),
        "observed_at": receipt.observed_at,
    })
}

struct ProductionBinanceTestnetSoakProbe {
    protocol: BinanceTestnetProtocol,
    exchange: BinanceTestnetExchange,
    transport: Arc<dyn RemoteHttpTransport>,
    symbols: BinanceSmokeSymbols,
    next_step: usize,
}

impl ProductionBinanceTestnetSoakProbe {
    fn new(
        transport: Arc<dyn RemoteHttpTransport>,
        symbols: BinanceSmokeSymbols,
        api_key: String,
        api_secret: String,
    ) -> Result<Self> {
        let signer = Arc::new(BinanceHmacSha256Signer::new(api_key, api_secret)?);
        let protocol = build_binance_testnet_protocol(Arc::clone(&signer), &symbols)?;
        let exchange_protocol = build_binance_testnet_protocol(signer, &symbols)?;
        let exchange = BinanceTestnetExchange::new(exchange_protocol, Arc::clone(&transport));
        Ok(Self {
            protocol,
            exchange,
            transport,
            symbols,
            next_step: 0,
        })
    }

    async fn next_probe(&mut self) -> Result<TestnetSoakSample, TestnetSoakProbeFailure> {
        let step = self.next_step;
        self.next_step = (self.next_step + 1) % 3;
        match step {
            0 => fetch_binance_book_ticker(
                &self.protocol,
                &*self.transport,
                &self.symbols.spot,
                MarketType::Spot,
            )
            .await
            .map(|_| TestnetSoakSample::SpotBookTicker)
            .map_err(|error| classify_testnet_soak_probe_failure(&error)),
            1 => fetch_binance_book_ticker(
                &self.protocol,
                &*self.transport,
                &self.symbols.perpetual,
                MarketType::Perpetual,
            )
            .await
            .map(|_| TestnetSoakSample::UsdMBookTicker)
            .map_err(|error| classify_testnet_soak_probe_failure(&error)),
            _ => self
                .exchange
                .reconcile(ReconcileScope::All)
                .await
                .map(|_| TestnetSoakSample::AuthenticatedReconcile)
                .map_err(|error| classify_testnet_soak_probe_failure(&error)),
        }
    }
}

impl TestnetSoakProbe for ProductionBinanceTestnetSoakProbe {
    fn probe(&mut self) -> TestnetSoakProbeFuture<'_> {
        Box::pin(async move { self.next_probe().await })
    }
}

struct ScriptedTestnetSoakProbe {
    results: VecDeque<Result<TestnetSoakSample, TestnetSoakProbeFailure>>,
}

impl ScriptedTestnetSoakProbe {
    fn parse(script: &str) -> Result<Self> {
        let mut results = VecDeque::new();
        for token in script.split(',') {
            let token = token.trim();
            if token.is_empty() {
                bail!("fixture probe script contains an empty step");
            }
            results.push_back(parse_fixture_probe_step(token)?);
        }
        if results.is_empty() {
            bail!("fixture probe script must contain at least one step");
        }
        Ok(Self { results })
    }
}

impl TestnetSoakProbe for ScriptedTestnetSoakProbe {
    fn probe(&mut self) -> TestnetSoakProbeFuture<'_> {
        let result = self
            .results
            .pop_front()
            .unwrap_or(Ok(TestnetSoakSample::SpotBookTicker));
        Box::pin(async move { result })
    }
}

#[derive(Debug)]
struct ProjectedTestnetSoakStatus {
    task_id: String,
    phase: String,
    recovery: String,
    successful_probe_count: u64,
    failed_probe_count: u64,
    consecutive_failure_count: u16,
    unclean_restart_count: u32,
    last_sample: String,
    last_probe_failure: String,
    updated_at: String,
    exit: String,
    failure: String,
    runtime_failure: String,
}

async fn run_testnet_soak(args: &TestnetSoakArgs) -> Result<()> {
    match args.mode {
        TestnetSoakMode::Serve => run_testnet_soak_serve(args).await,
        TestnetSoakMode::Status => run_testnet_soak_status(args).await,
        TestnetSoakMode::Stop => run_testnet_soak_stop(args).await,
        TestnetSoakMode::Verify => run_testnet_soak_verify(args),
    }
}

async fn run_testnet_soak_serve(args: &TestnetSoakArgs) -> Result<()> {
    if args.timeout_ms == 0 {
        bail!("testnet-soak serve requires --timeout-ms > 0");
    }
    let control_port = args
        .control_port
        .context("testnet-soak serve requires --control-port")?;
    if control_port == 0 {
        bail!("testnet-soak serve requires a nonzero --control-port");
    }
    let interval_ms = args
        .interval_ms
        .context("testnet-soak serve requires --interval-ms")?;
    let probe_timeout_ms = args
        .probe_timeout_ms
        .context("testnet-soak serve requires --probe-timeout-ms")?;
    let failure_threshold = args
        .failure_threshold
        .context("testnet-soak serve requires --failure-threshold")?;
    let config = TestnetSoakTaskConfig::new(
        args.task_id.clone(),
        StdDuration::from_millis(interval_ms),
        StdDuration::from_millis(probe_timeout_ms),
        failure_threshold,
    )?;

    if let Some(script) = &args.fixture_probe_script {
        let probe = ScriptedTestnetSoakProbe::parse(script)?;
        return serve_testnet_soak_task(args, control_port, config, probe).await;
    }

    let symbols = testnet_soak_symbols(args)?;
    let (api_key, api_secret) = load_binance_testnet_credentials()?;
    let transport: Arc<dyn RemoteHttpTransport> = Arc::new(ReqwestHttpTransport::new(
        StdDuration::from_millis(args.timeout_ms),
    )?);
    let probe = ProductionBinanceTestnetSoakProbe::new(transport, symbols, api_key, api_secret)?;
    serve_testnet_soak_task(args, control_port, config, probe).await
}

fn register_task_host_shutdown() -> Result<ShutdownSignalFuture> {
    install_shutdown_signal()
        .map_err(anyhow::Error::new)
        .context("failed to pre-register task-host shutdown signals")
}

async fn start_after_shutdown_registration<T, Register, Start, StartFuture>(
    register: Register,
    start: Start,
) -> Result<(ShutdownSignalFuture, T)>
where
    Register: FnOnce() -> Result<ShutdownSignalFuture>,
    Start: FnOnce() -> StartFuture,
    StartFuture: Future<Output = Result<T>>,
{
    let shutdown = register()?;
    let task = start().await?;
    Ok((shutdown, task))
}

async fn serve_testnet_soak_task<P>(
    args: &TestnetSoakArgs,
    control_port: u16,
    config: TestnetSoakTaskConfig,
    probe: P,
) -> Result<()>
where
    P: TestnetSoakProbe,
{
    let task_id = args.task_id.as_str();
    let address = control_addr(task_id, &args.history_path, Some(control_port));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind testnet soak control socket on {address}"))?;
    let (shutdown, mut task) =
        start_after_shutdown_registration(register_task_host_shutdown, || async move {
            TestnetSoakTask::start(config, probe, JsonlHistory::new(&args.history_path))
                .await
                .context("failed to start testnet soak task")
        })
        .await?;

    println!(
        "testnet soak task started: task_id={} control={} history={}",
        task_id,
        address,
        args.history_path.display()
    );

    let outcome = serve_host_with_shutdown(
        &mut task,
        listener,
        StdDuration::from_millis(args.control_poll_interval_ms.max(1)),
        render_live_testnet_soak_status,
        render_live_testnet_soak_stop,
        Ok(shutdown),
    )
    .await
    .map_err(|error| anyhow::Error::new(error).context("testnet soak control host failed"))?;

    match outcome {
        TaskHostServeOutcome::StopRequested(exit) => {
            println!(
                "testnet soak task stopped: task_id={task_id} exit={}",
                testnet_soak_exit_name(exit)
            );
        }
        TaskHostServeOutcome::Terminal(status) => {
            println!(
                "testnet soak task terminated: task_id={} phase={} successful_probe_count={} failed_probe_count={}",
                status.task_id,
                testnet_soak_phase_name(status.phase),
                status.successful_probe_count,
                status.failed_probe_count
            );
        }
    }
    Ok(())
}

async fn run_testnet_soak_status(args: &TestnetSoakArgs) -> Result<()> {
    let address = control_addr(&args.task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Status).await {
        print!("{response}");
        return Ok(());
    }
    print!(
        "{}",
        render_projected_testnet_soak_status(&project_testnet_soak_status(
            &args.history_path,
            &args.task_id,
        )?)
    );
    Ok(())
}

async fn run_testnet_soak_stop(args: &TestnetSoakArgs) -> Result<()> {
    let address = control_addr(&args.task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Stop).await {
        print!("{response}");
        return Ok(());
    }
    let projected = project_testnet_soak_status(&args.history_path, &args.task_id)?;
    if matches!(projected.phase.as_str(), "stopped" | "failed") {
        print!("{}", render_projected_testnet_soak_status(&projected));
        return Ok(());
    }
    bail!(
        "testnet soak control endpoint is unavailable at {address}; the task is not confirmed stopped"
    );
}

fn run_testnet_soak_verify(args: &TestnetSoakArgs) -> Result<()> {
    let minimum_successes = args
        .minimum_successes
        .context("testnet-soak verify requires --minimum-successes")?;
    let summary = verify_testnet_soak_evidence(
        &args.history_path,
        &args.task_id,
        TestnetSoakEvidenceRequirements::twenty_four_hour(minimum_successes)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&summary.as_json())?);
    if summary.requirements_met {
        return Ok(());
    }
    bail!("testnet soak evidence does not satisfy the 24-hour production policy")
}

fn testnet_soak_symbols(args: &TestnetSoakArgs) -> Result<BinanceSmokeSymbols> {
    Ok(BinanceSmokeSymbols {
        spot: Symbol::new(args.spot_symbol.clone()).context("invalid --spot-symbol")?,
        perpetual: Symbol::new(args.perpetual_symbol.clone())
            .context("invalid --perpetual-symbol")?,
        wire_symbol: args.wire_symbol.clone(),
    })
}

fn parse_fixture_probe_step(
    token: &str,
) -> Result<Result<TestnetSoakSample, TestnetSoakProbeFailure>> {
    Ok(match token {
        "spot" | "spot_book_ticker" => Ok(TestnetSoakSample::SpotBookTicker),
        "usdm" | "usd_m_book_ticker" => Ok(TestnetSoakSample::UsdMBookTicker),
        "reconcile" | "authenticated_reconcile" => Ok(TestnetSoakSample::AuthenticatedReconcile),
        "transport" => Err(TestnetSoakProbeFailure::Transport),
        "timeout" => Err(TestnetSoakProbeFailure::Timeout),
        "rate_limited" => Err(TestnetSoakProbeFailure::RateLimited),
        "clock_skew" => Err(TestnetSoakProbeFailure::ClockSkew),
        "remote_rejected" => Err(TestnetSoakProbeFailure::RemoteRejected),
        "protocol" => Err(TestnetSoakProbeFailure::Protocol),
        "unavailable" => Err(TestnetSoakProbeFailure::Unavailable),
        _ => bail!("unknown fixture probe step {token:?}"),
    })
}

fn classify_testnet_soak_probe_failure(error: &ExchangeError) -> TestnetSoakProbeFailure {
    match error {
        ExchangeError::Unavailable { reason } => {
            if reason.contains("timed out") {
                TestnetSoakProbeFailure::Timeout
            } else {
                TestnetSoakProbeFailure::Transport
            }
        }
        ExchangeError::Rejected { .. } => TestnetSoakProbeFailure::RemoteRejected,
        ExchangeError::RemoteFailure {
            status, metadata, ..
        } => {
            if metadata.exchange_code.as_deref() == Some("-1021") {
                TestnetSoakProbeFailure::ClockSkew
            } else if metadata.retry_after.is_some() || matches!(status, Some(418 | 429)) {
                TestnetSoakProbeFailure::RateLimited
            } else if status.is_some_and(|value| value >= 500) {
                TestnetSoakProbeFailure::Unavailable
            } else {
                TestnetSoakProbeFailure::RemoteRejected
            }
        }
        ExchangeError::InvalidResponse { .. } | ExchangeError::InvariantViolation { .. } => {
            TestnetSoakProbeFailure::Protocol
        }
        ExchangeError::AmbiguousOutcome { .. } => TestnetSoakProbeFailure::Transport,
        ExchangeError::InvalidRequest { .. }
        | ExchangeError::Unsupported { .. }
        | ExchangeError::Backpressure { .. }
        | ExchangeError::ResourceLimit { .. }
        | ExchangeError::SubscriptionLagged { .. } => TestnetSoakProbeFailure::Protocol,
    }
}

fn overwrite_string(target: &mut String, value: &str) {
    value.clone_into(target);
}

#[allow(clippy::too_many_lines)]
fn project_testnet_soak_status(
    history_path: &Path,
    task_id: &str,
) -> Result<ProjectedTestnetSoakStatus> {
    let mut projected = ProjectedTestnetSoakStatus {
        task_id: task_id.to_owned(),
        phase: "unknown".to_owned(),
        recovery: "investigate".to_owned(),
        successful_probe_count: 0,
        failed_probe_count: 0,
        consecutive_failure_count: 0,
        unclean_restart_count: 0,
        last_sample: "none".to_owned(),
        last_probe_failure: "none".to_owned(),
        updated_at: "unknown".to_owned(),
        exit: "none".to_owned(),
        failure: "none".to_owned(),
        runtime_failure: "none".to_owned(),
    };
    let mut running = false;
    let mut awaiting_restart_start = false;
    let mut saw_record = false;
    let mut saw_started = false;

    for record in read_bounded_testnet_soak_records(history_path)? {
        if record.strategy != "testnet_soak" {
            continue;
        }
        if record.details["task_kind"].as_str() != Some("binance_testnet_read_only_soak") {
            continue;
        }
        if record.details["task_id"].as_str() != Some(task_id) {
            continue;
        }
        if record.details["schema_version"].as_u64() != Some(u64::from(TESTNET_SOAK_SCHEMA_VERSION))
        {
            bail!("testnet soak status failed: unsupported schema for task {task_id}");
        }
        saw_record = true;
        projected.updated_at = record.timestamp.to_rfc3339();
        let observation = &record.details["observation"];
        match record.decision.as_str() {
            "testnet_soak_started" => {
                if saw_started && !awaiting_restart_start {
                    projected.successful_probe_count = 0;
                    projected.failed_probe_count = 0;
                    projected.consecutive_failure_count = 0;
                    projected.unclean_restart_count = 0;
                    overwrite_string(&mut projected.last_sample, "none");
                    overwrite_string(&mut projected.last_probe_failure, "none");
                }
                saw_started = true;
                running = true;
                awaiting_restart_start = false;
                overwrite_string(&mut projected.phase, "running");
                overwrite_string(&mut projected.exit, "none");
                overwrite_string(&mut projected.failure, "none");
            }
            "testnet_soak_unclean_restart_detected" => {
                projected.unclean_restart_count = projected.unclean_restart_count.saturating_add(1);
                running = false;
                awaiting_restart_start = true;
                overwrite_string(&mut projected.phase, "restarting");
                overwrite_string(&mut projected.exit, "none");
                overwrite_string(&mut projected.failure, "none");
            }
            "testnet_soak_probe_succeeded" => {
                projected.successful_probe_count =
                    projected.successful_probe_count.saturating_add(1);
                projected.consecutive_failure_count = 0;
                overwrite_string(&mut projected.last_probe_failure, "none");
                overwrite_string(
                    &mut projected.last_sample,
                    observation["sample"].as_str().unwrap_or("none"),
                );
                running = true;
                overwrite_string(&mut projected.phase, "running");
                overwrite_string(&mut projected.exit, "none");
                overwrite_string(&mut projected.failure, "none");
            }
            "testnet_soak_probe_failed" => {
                projected.failed_probe_count = projected.failed_probe_count.saturating_add(1);
                projected.consecutive_failure_count =
                    projected.consecutive_failure_count.saturating_add(1);
                overwrite_string(
                    &mut projected.last_probe_failure,
                    observation["probe_failure"].as_str().unwrap_or("none"),
                );
                running = true;
                overwrite_string(&mut projected.phase, "running");
                overwrite_string(&mut projected.exit, "none");
                overwrite_string(&mut projected.failure, "none");
            }
            "testnet_soak_stopped" => {
                running = false;
                awaiting_restart_start = false;
                overwrite_string(&mut projected.phase, "stopped");
                overwrite_string(
                    &mut projected.exit,
                    observation["exit"].as_str().unwrap_or("stop_requested"),
                );
                overwrite_string(&mut projected.failure, "none");
                projected.consecutive_failure_count = 0;
            }
            "testnet_soak_failed" => {
                running = false;
                awaiting_restart_start = false;
                overwrite_string(&mut projected.phase, "failed");
                overwrite_string(&mut projected.exit, "none");
                overwrite_string(
                    &mut projected.failure,
                    observation["task_failure"].as_str().unwrap_or("none"),
                );
                if let Some(probe_failure) = observation["probe_failure"].as_str() {
                    overwrite_string(&mut projected.last_probe_failure, probe_failure);
                }
            }
            _ => bail!("testnet soak status failed: unsupported fact for task {task_id}"),
        }
    }

    if !saw_record {
        bail!("testnet soak task not found: {task_id}");
    }
    projected.recovery = if !running && projected.phase == "stopped" && projected.failure == "none"
    {
        "none".to_owned()
    } else {
        "investigate".to_owned()
    };
    Ok(projected)
}

fn read_bounded_testnet_soak_records(history_path: &Path) -> Result<Vec<DecisionRecord>> {
    let bytes = read_journal_chain(history_path).with_context(|| {
        format!(
            "testnet soak status failed to read history chain {}",
            history_path.display()
        )
    })?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bail!(
            "testnet soak status failed: history source {} has a partial trailing record",
            history_path.display()
        );
    }
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let complete = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    let mut records = Vec::new();
    for (index, raw_line) in complete.split(|byte| *byte == b'\n').enumerate() {
        if records.len() == MAX_TESTNET_SOAK_EVIDENCE_RECORDS {
            bail!(
                "testnet soak status failed: history source {} exceeds {} records",
                history_path.display(),
                MAX_TESTNET_SOAK_EVIDENCE_RECORDS
            );
        }
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            bail!(
                "testnet soak status failed: history source {} contains an empty record",
                history_path.display()
            );
        }
        if line.len().saturating_add(1) > MAX_HISTORY_RECORD_BYTES {
            bail!(
                "testnet soak status failed: history record {} exceeds {} bytes",
                index + 1,
                MAX_HISTORY_RECORD_BYTES
            );
        }
        records.push(
            serde_json::from_slice::<DecisionRecord>(line).with_context(|| {
                format!(
                    "failed to parse testnet soak history record {} from {}",
                    index + 1,
                    history_path.display()
                )
            })?,
        );
    }
    Ok(records)
}

fn render_live_testnet_soak_status(status: &TestnetSoakTaskStatus) -> String {
    format_testnet_soak_status(
        &status.task_id,
        Cow::Owned(testnet_soak_phase_name(status.phase)),
        Cow::Borrowed("none"),
        status.successful_probe_count,
        status.failed_probe_count,
        status.consecutive_failure_count,
        status.unclean_restart_count,
        Cow::Owned(
            status
                .last_sample
                .map_or("none".to_owned(), testnet_soak_sample_name),
        ),
        Cow::Owned(
            status
                .last_probe_failure
                .map_or("none".to_owned(), testnet_soak_probe_failure_name),
        ),
        Cow::Owned(status.last_recorded_at.to_rfc3339()),
        Cow::Owned(
            status
                .exit
                .map_or("none".to_owned(), testnet_soak_exit_name),
        ),
        Cow::Owned(
            status
                .failure
                .map_or("none".to_owned(), testnet_soak_task_failure_name),
        ),
        Cow::Owned(
            status
                .runtime_failure
                .map_or("none".to_owned(), testnet_soak_task_failure_name),
        ),
    )
}

fn render_live_testnet_soak_stop(
    status: &TestnetSoakTaskStatus,
    _exit: TestnetSoakTaskExit,
) -> String {
    render_live_testnet_soak_status(status)
}

fn render_projected_testnet_soak_status(status: &ProjectedTestnetSoakStatus) -> String {
    format_testnet_soak_status(
        &status.task_id,
        Cow::Borrowed(&status.phase),
        Cow::Borrowed(&status.recovery),
        status.successful_probe_count,
        status.failed_probe_count,
        status.consecutive_failure_count,
        status.unclean_restart_count,
        Cow::Borrowed(&status.last_sample),
        Cow::Borrowed(&status.last_probe_failure),
        Cow::Borrowed(&status.updated_at),
        Cow::Borrowed(&status.exit),
        Cow::Borrowed(&status.failure),
        Cow::Borrowed(&status.runtime_failure),
    )
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_pass_by_value)]
fn format_testnet_soak_status(
    task_id: &str,
    phase: Cow<'_, str>,
    recovery: Cow<'_, str>,
    successful_probe_count: u64,
    failed_probe_count: u64,
    consecutive_failure_count: u16,
    unclean_restart_count: u32,
    last_sample: Cow<'_, str>,
    last_probe_failure: Cow<'_, str>,
    updated_at: Cow<'_, str>,
    exit: Cow<'_, str>,
    failure: Cow<'_, str>,
    runtime_failure: Cow<'_, str>,
) -> String {
    format!(
        "task_id={task_id}\nphase={phase}\nrecovery={recovery}\nsuccessful_probe_count={successful_probe_count}\nfailed_probe_count={failed_probe_count}\nconsecutive_failure_count={consecutive_failure_count}\nunclean_restart_count={unclean_restart_count}\nlast_sample={last_sample}\nlast_probe_failure={last_probe_failure}\nupdated_at={updated_at}\nexit={exit}\nfailure={failure}\nruntime_failure={runtime_failure}\n"
    )
}

fn testnet_soak_sample_name(sample: TestnetSoakSample) -> String {
    match sample {
        TestnetSoakSample::SpotBookTicker => "spot_book_ticker",
        TestnetSoakSample::UsdMBookTicker => "usd_m_book_ticker",
        TestnetSoakSample::AuthenticatedReconcile => "authenticated_reconcile",
    }
    .to_owned()
}

fn testnet_soak_phase_name(phase: crate::testnet_soak::TestnetSoakTaskPhase) -> String {
    match phase {
        crate::testnet_soak::TestnetSoakTaskPhase::Running => "running",
        crate::testnet_soak::TestnetSoakTaskPhase::Stopped => "stopped",
        crate::testnet_soak::TestnetSoakTaskPhase::Failed => "failed",
    }
    .to_owned()
}

fn testnet_soak_probe_failure_name(failure: TestnetSoakProbeFailure) -> String {
    match failure {
        TestnetSoakProbeFailure::Transport => "transport",
        TestnetSoakProbeFailure::Timeout => "timeout",
        TestnetSoakProbeFailure::RateLimited => "rate_limited",
        TestnetSoakProbeFailure::ClockSkew => "clock_skew",
        TestnetSoakProbeFailure::RemoteRejected => "remote_rejected",
        TestnetSoakProbeFailure::Protocol => "protocol",
        TestnetSoakProbeFailure::Unavailable => "unavailable",
    }
    .to_owned()
}

fn testnet_soak_exit_name(exit: TestnetSoakTaskExit) -> String {
    match exit {
        TestnetSoakTaskExit::StopRequested => "stop_requested",
    }
    .to_owned()
}

fn testnet_soak_task_failure_name(failure: TestnetSoakTaskFailure) -> String {
    match failure {
        TestnetSoakTaskFailure::ProbeFailureThreshold => "probe_failure_threshold",
        TestnetSoakTaskFailure::CounterOverflow => "counter_overflow",
        TestnetSoakTaskFailure::JournalUnavailable => "journal_unavailable",
        TestnetSoakTaskFailure::TaskPanicked => "task_panicked",
        TestnetSoakTaskFailure::TaskCancelled => "task_cancelled",
    }
    .to_owned()
}

#[derive(Debug)]
struct PaperExecution {
    receipts: Vec<TradingReceipt>,
}

async fn run_grid(args: GridArgs) -> Result<()> {
    if args.authority.live {
        ExecutionMode::live(args.authority.acknowledge_risk.as_deref())?;
        bail!(
            "live grid execution is unavailable until its exchange adapter passes signing and testnet verification"
        );
    }
    let body = if args.once {
        validated_paper_runtime_body(&args.config, PaperRuntimeSchema::Grid)?
    } else {
        read_bounded_config(&args.config).map_err(anyhow::Error::msg)?
    };
    let config = load_grid_config_from_str(&body)
        .with_context(|| format!("failed to load grid config {}", args.config.display()))?;
    let valid_message = format!(
        "valid: grid {} exchange={} symbol={} mode={:?} market={:?}",
        args.config.display(),
        config.exchange,
        config.symbol,
        config.mode,
        config.market_type
    );
    if args.once {
        let price = args
            .price
            .context("--once requires --price so the paper run has an explicit snapshot")?;
        let execution = execute_grid_paper(&config, price, &args.history_path).await?;
        println!("{valid_message}");
        println!(
            "paper placement simulated: {} orders at snapshot price={price}; history={}",
            execution.receipts.len(),
            args.history_path.display()
        );
    } else {
        println!("{valid_message}");
    }
    Ok(())
}

async fn run_arbitrage(args: &ArbitrageArgs) -> Result<()> {
    if args.authority.live {
        ExecutionMode::live(args.authority.acknowledge_risk.as_deref())?;
        bail!(
            "live arbitrage execution is unavailable until both exchange adapters pass reconcile verification"
        );
    }
    let config_body = if args.behavior.once {
        validated_paper_runtime_body(&args.config, PaperRuntimeSchema::Arbitrage)?
    } else {
        read_bounded_config(&args.config).map_err(anyhow::Error::msg)?
    };
    let monitor_body = if args.behavior.once {
        validated_paper_runtime_body(&args.monitor_config, PaperRuntimeSchema::Monitor)?
    } else {
        read_bounded_config(&args.monitor_config).map_err(anyhow::Error::msg)?
    };
    let config = load_arbitrage_config_from_str(&config_body)
        .with_context(|| format!("failed to load arbitrage config {}", args.config.display()))?;
    let monitor = load_monitor_config_from_str(&monitor_body).with_context(|| {
        format!(
            "failed to load monitor config {}",
            args.monitor_config.display()
        )
    })?;
    let valid_message = format!(
        "valid: arbitrage {} monitor={} exchanges={} symbols={} mode=paper",
        args.config.display(),
        args.monitor_config.display(),
        monitor.exchanges.len(),
        monitor.symbols.len()
    );
    if !args.behavior.once {
        bail!(
            "continuous arbitrage runtime is unavailable; use --once with explicit paper snapshots"
        );
    }
    let snapshots = resolve_arbitrage_snapshots(args, &monitor)?;
    let (effective_config, policy) = resolve_arbitrage_policy(args, &config, &monitor, &snapshots)?;
    let (decision, execution) = execute_arbitrage_paper(
        &effective_config,
        &config,
        &policy,
        snapshots,
        &args.history_path,
    )
    .await?;
    println!("{valid_message}");
    println!(
        "paper executed: decision={:?} segment={} receipts={}; history={}",
        decision.kind,
        decision.segment,
        execution.receipts.len(),
        args.history_path.display()
    );
    Ok(())
}

async fn run_monitor(args: &MonitorArgs) -> Result<()> {
    match args.mode {
        MonitorMode::Replay => run_monitor_replay(args).await,
        MonitorMode::Serve => run_monitor_serve(args).await,
        MonitorMode::Status => run_monitor_status(args).await,
        MonitorMode::Stop => run_monitor_stop(args).await,
    }
}

async fn run_monitor_replay(args: &MonitorArgs) -> Result<()> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let monitor = load_monitor_config_from_str(&body)
        .with_context(|| format!("failed to load monitor config {}", args.config.display()))?;
    let replay_path = args
        .replay
        .as_ref()
        .context(
            "monitor replay mode requires --replay with a strict JSONL snapshot fixture; continuous external monitor sources remain unavailable",
        )?;
    validate_monitor_pair(&monitor)?;
    let symbol = selected_monitor_symbol(args, &monitor)?;

    let mut instruments = Vec::new();
    for exchange in &monitor.exchanges {
        for configured_symbol in &monitor.symbols {
            instruments.push(MarketInstrument::new(
                exchange,
                configured_symbol.clone(),
                MarketType::Perpetual,
            )?);
        }
    }
    let universe = MarketUniverse::new(instruments)?;
    let left = MarketInstrument::new(&monitor.exchanges[0], symbol.clone(), MarketType::Perpetual)?;
    let right = MarketInstrument::new(&monitor.exchanges[1], symbol, MarketType::Perpetual)?;
    let events = load_market_snapshot_replay(replay_path)?;
    let first_at = match events.first() {
        Some(MarketDataEvent::Observation(observation)) => observation.received_at,
        Some(
            MarketDataEvent::SourceGap { observed_at, .. }
            | MarketDataEvent::SourceUnavailable { observed_at, .. },
        ) => *observed_at,
        None => bail!("monitor replay must contain at least one event"),
    };
    let clock = Arc::new(ReplayMarketDataClock::new(first_at));
    let book = MarketDataBook::new(
        universe,
        freshness_policy_from_monitor_config(&monitor)?,
        Arc::clone(&clock),
    );
    let mut read_monitor =
        ReadOnlyArbitrageMonitor::new(book, left, right, monitor.min_spread_pct)?;
    let mut adapter = DeterministicMarketDataAdapter::new(events)?;
    let mut records = Vec::new();
    let mut opportunities = 0usize;
    let mut waiting = 0usize;
    while let Some(event) = adapter.next_event() {
        match &event {
            MarketDataEvent::Observation(observation) => clock.advance(observation.received_at),
            MarketDataEvent::SourceGap { observed_at, .. }
            | MarketDataEvent::SourceUnavailable { observed_at, .. } => {
                clock.advance(*observed_at);
            }
        }
        let monitor_event = read_monitor.process(event)?;
        match &monitor_event.outcome {
            ArbitrageMonitorOutcome::Opportunity { .. } => {
                opportunities = opportunities.saturating_add(1);
            }
            ArbitrageMonitorOutcome::Waiting { .. } => {
                waiting = waiting.saturating_add(1);
            }
            ArbitrageMonitorOutcome::NoOpportunity { .. }
            | ArbitrageMonitorOutcome::AnalysisRejected { .. } => {}
        }
        records.push(monitor_event.to_record());
    }
    JsonlHistory::new(&args.history_path)
        .append_batch(&records)
        .await
        .context("failed to persist the read-only monitor replay")?;
    println!(
        "read-only monitor replay: events={} opportunities={} waiting={} history={}",
        records.len(),
        opportunities,
        waiting,
        args.history_path.display()
    );
    Ok(())
}

async fn run_monitor_serve(args: &MonitorArgs) -> Result<()> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let monitor = load_monitor_config_from_str(&body)
        .with_context(|| format!("failed to load monitor config {}", args.config.display()))?;
    let task_id = args
        .task_id
        .as_deref()
        .context("monitor serve mode requires --task-id")?;
    validate_monitor_pair(&monitor)?;
    let symbol = selected_monitor_symbol(args, &monitor)?;
    if args.live {
        let (read_monitor, left_source, right_source) =
            build_live_monitor_pair(args, &monitor, &symbol)?;
        return serve_monitor_task(args, task_id, read_monitor, left_source, right_source).await;
    }
    let replay_path = args.replay.as_ref().context(
        "monitor serve requires --replay unless --live opts into the credential-free binance+hyperliquid polling pair",
    )?;
    let market_type = serve_market_type(&symbol);
    let left = MarketInstrument::new(&monitor.exchanges[0], symbol.clone(), market_type)?;
    let right = MarketInstrument::new(&monitor.exchanges[1], symbol.clone(), market_type)?;
    let read_monitor = build_exact_pair_monitor(&monitor, left, right)?;
    let (left_source, right_source) = build_serve_replay_sources(
        replay_path,
        &monitor.exchanges[0],
        &monitor.exchanges[1],
        &symbol,
    )?;
    serve_monitor_task(args, task_id, read_monitor, left_source, right_source).await
}

/// Builds the exact-pair composer (bounded book plus read-only monitor) shared
/// by the replay-backed and live-polling serve bootstraps.
fn build_exact_pair_monitor(
    monitor: &MonitorConfig,
    left: MarketInstrument,
    right: MarketInstrument,
) -> Result<ReadOnlyArbitrageMonitor> {
    let universe = MarketUniverse::new(vec![left.clone(), right.clone()])?;
    let book = MarketDataBook::new(
        universe,
        freshness_policy_from_monitor_config(monitor)?,
        Arc::new(SystemMarketDataClock),
    );
    Ok(ReadOnlyArbitrageMonitor::new(
        book,
        left,
        right,
        monitor.min_spread_pct,
    )?)
}

/// Builds the explicit live pair: a Binance Spot polling leg and a Hyperliquid
/// perpetual polling leg, both credential-free and read-only.
///
/// The Hyperliquid leg's funding-rate side feed is not consumed here yet: the
/// spread-history journal keeps recording funding fields as absent, so
/// history-mode decisions stay explicitly funding-degraded.
fn build_live_monitor_pair(
    args: &MonitorArgs,
    monitor: &MonitorConfig,
    symbol: &Symbol,
) -> Result<(
    ReadOnlyArbitrageMonitor,
    BinancePublicPollingSource,
    HyperliquidPublicPollingSource,
)> {
    if monitor.exchanges.len() != 2
        || monitor.exchanges[0] != "binance"
        || monitor.exchanges[1] != "hyperliquid"
    {
        bail!(
            "monitor --live currently supports exactly the configured exchange pair [binance, hyperliquid] in that order"
        );
    }
    let Some(coin) = symbol
        .as_str()
        .strip_suffix("USDT")
        .filter(|coin| !coin.is_empty())
    else {
        bail!("monitor --live requires a USDT-quoted symbol such as BTCUSDT; got {symbol}");
    };
    let catalog = hyperliquid_usdt_symbol_catalog(&[coin])?;
    let wire_coin = catalog
        .to_wire("hyperliquid", symbol, MarketType::Perpetual)?
        .to_owned();
    let left = MarketInstrument::new("binance", symbol.clone(), MarketType::Spot)?;
    let right = MarketInstrument::new("hyperliquid", symbol.clone(), MarketType::Perpetual)?;
    let read_monitor = build_exact_pair_monitor(monitor, left.clone(), right.clone())?;
    let poll_interval = StdDuration::from_millis(args.poll_interval_ms.max(1));
    let policy = MarketPollingPolicy::new(
        poll_interval,
        poll_interval,
        poll_interval.max(StdDuration::from_secs(60)),
    )?;
    let binance = match args.binance_base_url.as_deref() {
        Some(base_url) => BinancePublicExchange::with_base_url(base_url)?,
        None => BinancePublicExchange::new()?,
    };
    let hyperliquid_endpoint = match args.hyperliquid_base_url.as_deref() {
        Some(base_url) => HyperliquidPublicEndpoint::loopback(base_url)?,
        None => HyperliquidPublicEndpoint::official(),
    };
    let hyperliquid = HyperliquidPublicExchange::with_endpoint(&hyperliquid_endpoint)?;
    let left_source = BinancePublicPollingSource::new(
        binance,
        vec![BinancePollingRoute::new(
            left,
            Symbol::new(symbol.as_str())?,
        )?],
        policy,
        Arc::new(SystemMarketDataClock),
    )?;
    let right_source = HyperliquidPublicPollingSource::new(
        hyperliquid,
        vec![HyperliquidPollingRoute::new(
            right,
            Symbol::new(wire_coin)?,
        )?],
        policy,
        Arc::new(SystemMarketDataClock),
    )?;
    Ok((read_monitor, left_source, right_source))
}

/// Starts one continuous monitor owner over the given exact sources and hosts
/// its loopback control endpoint until it stops or terminates.
async fn serve_monitor_task<L, R>(
    args: &MonitorArgs,
    task_id: &str,
    read_monitor: ReadOnlyArbitrageMonitor,
    left_source: L,
    right_source: R,
) -> Result<()>
where
    L: MarketDataEventSource,
    R: MarketDataEventSource,
{
    let task_config =
        ContinuousMonitorTaskConfig::new(task_id, supervisor_config(args.shutdown_grace_ms)?)?;
    let (shutdown, mut task) =
        start_after_shutdown_registration(register_task_host_shutdown, || async move {
            ContinuousMonitorTask::start_with_spread_history(
                task_config,
                read_monitor,
                left_source,
                right_source,
                JsonlHistory::new(&args.history_path),
                Some(SpreadHistoryWriter::new(&args.spread_history_path)),
            )
            .await
            .context("failed to start continuous monitor task")
        })
        .await?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind monitor control socket on {address}"))?;

    println!(
        "continuous monitor task started: task_id={} control={} history={} spread_history={}",
        task_id,
        address,
        args.history_path.display(),
        args.spread_history_path.display()
    );

    let outcome = serve_host_with_shutdown(
        &mut task,
        listener,
        StdDuration::from_millis(args.control_poll_interval_ms.max(1)),
        render_live_monitor_status,
        render_live_monitor_stop,
        Ok(shutdown),
    )
    .await
    .map_err(|error| anyhow::Error::new(error).context("monitor control host failed"))?;

    match outcome {
        TaskHostServeOutcome::StopRequested(exit) => {
            println!("continuous monitor task stopped: task_id={task_id} exit={exit}");
        }
        TaskHostServeOutcome::Terminal(status) => {
            println!(
                "continuous monitor task terminated: task_id={} phase={} processed_event_count={}",
                status.task_id, status.phase, status.processed_event_count
            );
        }
    }
    Ok(())
}

async fn run_monitor_status(args: &MonitorArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("monitor status mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Status).await {
        print!("{response}");
        return Ok(());
    }
    print!(
        "{}",
        render_projected_task_status(&project_task_status(
            &args.history_path,
            task_id,
            MONITOR_TASK_PROJECTION,
        )?)
    );
    Ok(())
}

async fn run_monitor_stop(args: &MonitorArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("monitor stop mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Stop).await {
        print!("{response}");
        return Ok(());
    }
    let projected = project_task_status(&args.history_path, task_id, MONITOR_TASK_PROJECTION)?;
    if projected.phase == "stopped" || projected.phase == "failed" {
        print!("{}", render_projected_task_status(&projected));
        return Ok(());
    }
    bail!(
        "monitor task control endpoint is unavailable at {address}; the task is not confirmed stopped"
    );
}

fn selected_monitor_symbol(args: &MonitorArgs, monitor: &MonitorConfig) -> Result<Symbol> {
    if args.symbols.len() > 1 {
        bail!("the first monitor tracer accepts at most one --symbols value");
    }
    if let Some(value) = args.symbols.first() {
        let candidate = Symbol::new(value.clone()).context("invalid monitor symbol filter")?;
        if !monitor.symbols.contains(&candidate) {
            bail!("monitor symbol {candidate} is outside the configured allowlist");
        }
        return Ok(candidate);
    }
    monitor
        .symbols
        .first()
        .cloned()
        .context("monitor configuration has no symbols")
}

fn validate_monitor_pair(monitor: &MonitorConfig) -> Result<()> {
    if monitor.exchanges.len() != 2 {
        bail!(
            "the first read-only monitor tracer requires exactly two configured exchanges; found {}",
            monitor.exchanges.len()
        );
    }
    if monitor.exchanges[0] == monitor.exchanges[1] {
        bail!("read-only arbitrage monitor needs two distinct configured exchanges");
    }
    if monitor.symbols.is_empty() {
        bail!("monitor configuration has no symbols");
    }
    Ok(())
}

fn serve_market_type(symbol: &Symbol) -> MarketType {
    if symbol.as_str().ends_with("-SPOT") {
        MarketType::Spot
    } else {
        MarketType::Perpetual
    }
}

fn supervisor_config(shutdown_grace_ms: Option<u64>) -> Result<MarketSupervisorConfig> {
    match shutdown_grace_ms {
        Some(milliseconds) => MarketSupervisorConfig::new(StdDuration::from_millis(milliseconds))
            .map_err(anyhow::Error::msg)
            .context("invalid task shutdown grace override"),
        None => Ok(MarketSupervisorConfig::default()),
    }
}

#[derive(Debug)]
struct ServeReplaySource {
    source_id: String,
    events: VecDeque<MarketDataEvent>,
}

impl MarketDataEventSource for ServeReplaySource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        if let Some(event) = self.events.pop_front() {
            return Box::pin(async move { Ok(Some(event)) });
        }
        Box::pin(async move {
            std::future::pending::<Result<Option<MarketDataEvent>, MarketDataError>>().await
        })
    }
}

fn build_serve_replay_sources(
    replay_path: &Path,
    left_source_id: &str,
    right_source_id: &str,
    symbol: &Symbol,
) -> Result<(ServeReplaySource, ServeReplaySource)> {
    let events = load_market_snapshot_replay(replay_path)?;
    Ok((
        ServeReplaySource {
            source_id: left_source_id.to_owned(),
            events: filter_serve_replay_events(&events, left_source_id, symbol),
        },
        ServeReplaySource {
            source_id: right_source_id.to_owned(),
            events: filter_serve_replay_events(&events, right_source_id, symbol),
        },
    ))
}

fn filter_serve_replay_events(
    events: &[MarketDataEvent],
    source_id: &str,
    symbol: &Symbol,
) -> VecDeque<MarketDataEvent> {
    events
        .iter()
        .filter(|event| match event {
            MarketDataEvent::Observation(observation) => {
                observation.snapshot.exchange() == source_id
                    && observation.snapshot.symbol == *symbol
            }
            MarketDataEvent::SourceGap { exchange, .. }
            | MarketDataEvent::SourceUnavailable { exchange, .. } => exchange == source_id,
        })
        .cloned()
        .collect()
}

#[derive(Debug)]
struct ProjectedTaskStatus {
    task_id: String,
    phase: String,
    recovery: String,
    failure: String,
    processed_event_count: u64,
    updated_at: String,
    exit: String,
    runtime_failure: String,
}

/// Durable `task_kind` filter and operator-facing label for one journal-backed
/// task projection.
#[derive(Clone, Copy, Debug)]
struct TaskProjectionScope {
    task_kind: &'static str,
    label: &'static str,
}

const MONITOR_TASK_PROJECTION: TaskProjectionScope = TaskProjectionScope {
    task_kind: "arbitrage_monitor",
    label: "monitor",
};

const PRICE_ALERT_TASK_PROJECTION: TaskProjectionScope = TaskProjectionScope {
    task_kind: "price_alert",
    label: "price-alert",
};

const SCANNER_TASK_PROJECTION: TaskProjectionScope = TaskProjectionScope {
    task_kind: "scanner",
    label: "scanner",
};

fn project_task_status(
    history_path: &Path,
    task_id: &str,
    scope: TaskProjectionScope,
) -> Result<ProjectedTaskStatus> {
    let bytes = match read_journal_chain(history_path) {
        Ok(bytes) => bytes,
        Err(JournalReadError::Open(source)) if source.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "{} status failed: history file {} does not exist",
                scope.label,
                history_path.display()
            );
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read {}", history_path.display()));
        }
    };
    let text = String::from_utf8(bytes)
        .with_context(|| format!("failed to read {}", history_path.display()))?;

    let mut projected = None;
    for (index, line) in text.lines().enumerate() {
        let record: Value = serde_json::from_str(line).with_context(|| {
            format!(
                "failed to parse {} task record {} from {}",
                scope.label,
                index + 1,
                history_path.display()
            )
        })?;
        if record["strategy"].as_str() != Some("read_only_task") {
            continue;
        }
        if record["details"]["task_kind"].as_str() != Some(scope.task_kind) {
            continue;
        }
        if record["details"]["task_id"].as_str() != Some(task_id) {
            continue;
        }
        let phase = record["details"]["phase"]
            .as_str()
            .with_context(|| format!("{} task status record is missing phase", scope.label))?
            .to_owned();
        let failure = record["details"]["failure"]
            .as_str()
            .unwrap_or("none")
            .to_owned();
        let exit = record["details"]["exit"]
            .as_str()
            .unwrap_or("none")
            .to_owned();
        let recovery = if phase == "stopped" && failure == "none" {
            "none"
        } else {
            "investigate"
        }
        .to_owned();
        projected = Some(ProjectedTaskStatus {
            task_id: task_id.to_owned(),
            phase,
            recovery,
            failure,
            processed_event_count: record["details"]["processed_event_count"]
                .as_u64()
                .unwrap_or(0),
            updated_at: record["timestamp"].as_str().unwrap_or("unknown").to_owned(),
            exit,
            runtime_failure: "none".to_owned(),
        });
    }

    projected.context(format!("{} task not found: {task_id}", scope.label))
}

fn render_live_monitor_status(status: &ContinuousMonitorTaskStatus) -> String {
    format_task_status(&TaskStatusRender {
        task_id: &status.task_id,
        phase: Cow::Owned(status.phase.to_string()),
        recovery: Cow::Borrowed("none"),
        failure: Cow::Owned(
            status
                .failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
        processed_event_count: status.processed_event_count,
        updated_at: Cow::Owned(
            status
                .last_recorded_at
                .map_or_else(|| "none".to_owned(), |recorded_at| recorded_at.to_rfc3339()),
        ),
        exit: Cow::Owned(
            status
                .exit
                .map_or("none".to_owned(), |exit| exit.to_string()),
        ),
        runtime_failure: Cow::Owned(
            status
                .runtime_failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
    })
}

fn render_live_monitor_stop(
    status: &ContinuousMonitorTaskStatus,
    _exit: ContinuousMonitorTaskExit,
) -> String {
    render_live_monitor_status(status)
}

fn render_projected_task_status(status: &ProjectedTaskStatus) -> String {
    format_task_status(&TaskStatusRender {
        task_id: &status.task_id,
        phase: Cow::Borrowed(&status.phase),
        recovery: Cow::Borrowed(&status.recovery),
        failure: Cow::Borrowed(&status.failure),
        processed_event_count: status.processed_event_count,
        updated_at: Cow::Borrowed(&status.updated_at),
        exit: Cow::Borrowed(&status.exit),
        runtime_failure: Cow::Borrowed(&status.runtime_failure),
    })
}

struct TaskStatusRender<'a> {
    task_id: &'a str,
    phase: Cow<'a, str>,
    recovery: Cow<'a, str>,
    failure: Cow<'a, str>,
    processed_event_count: u64,
    updated_at: Cow<'a, str>,
    exit: Cow<'a, str>,
    runtime_failure: Cow<'a, str>,
}

fn format_task_status(status: &TaskStatusRender<'_>) -> String {
    format!(
        "task_id={task_id}\nphase={phase}\nrecovery={recovery}\nfailure={}\nprocessed_event_count={processed_event_count}\nupdated_at={}\nexit={}\nruntime_failure={}\n",
        status.failure,
        status.updated_at,
        status.exit,
        status.runtime_failure,
        task_id = status.task_id,
        phase = status.phase,
        recovery = status.recovery,
        processed_event_count = status.processed_event_count,
    )
}

/// Fixed cost model and account bounds for the CLI paper volume-maker host.
const VOLUME_MAKER_INITIAL_AVAILABLE: i64 = 10_000;
const VOLUME_MAKER_COST_FEE_BPS: u32 = 10;
const VOLUME_MAKER_COST_FUNDING_BUFFER_BPS: u32 = 5;
const VOLUME_MAKER_COST_SLIPPAGE_BPS: u32 = 15;
const VOLUME_MAKER_EVENT_CAPACITY: usize = 256;
const VOLUME_MAKER_MAX_MARKET_AGE_SECONDS: i64 = 30;
/// One shared account-risk scope mirroring the paper profile owners.
const VOLUME_MAKER_ACCOUNT_RISK_SCOPE: &str = "paper";

const VOLUME_MAKER_TASK_PROJECTION: TaskProjectionScope = TaskProjectionScope {
    task_kind: "volume_maker",
    label: "volume-maker",
};

async fn run_volume_maker(args: &VolumeMakerArgs) -> Result<()> {
    match args.mode {
        VolumeMakerRunMode::Validate => {
            run_volume_maker_validate(args)?;
            Ok(())
        }
        VolumeMakerRunMode::Serve => run_volume_maker_serve(args).await,
        VolumeMakerRunMode::Status => run_volume_maker_status(args).await,
        VolumeMakerRunMode::Stop => run_volume_maker_stop(args).await,
    }
}

fn run_volume_maker_validate(args: &VolumeMakerArgs) -> Result<VolumeMakerConfig> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let config = load_volume_maker_config_from_str(&body).with_context(|| {
        format!(
            "failed to load volume-maker config {}",
            args.config.display()
        )
    })?;
    config
        .validate_execution_controls()
        .context("volume-maker execution controls rejected the configuration")?;
    VolumeMakerStrategy::try_from(&config)
        .context("volume-maker strategy rejected the configuration")?;
    println!(
        "valid: volume-maker {} exchange={} symbol={} mode={} market={:?}",
        args.config.display(),
        config.exchange,
        config.symbol,
        config.order_mode.trim().to_ascii_lowercase(),
        config.market_type
    );
    Ok(config)
}

#[allow(clippy::too_many_lines)]
async fn run_volume_maker_serve(args: &VolumeMakerArgs) -> Result<()> {
    let config = run_volume_maker_validate(args)?;
    let strategy = VolumeMakerStrategy::try_from(&config)
        .map_err(anyhow::Error::new)
        .context("volume-maker strategy rejected the configuration")?;
    let replay_path = args.replay.as_ref().context(
        "volume-maker serve currently requires --replay until external continuous market sources are wired into this task host",
    )?;
    let task_id = args
        .task_id
        .as_deref()
        .context("volume-maker serve mode requires --task-id")?;
    let history = JsonlHistory::new(&args.history_path);

    let instrument =
        MarketInstrument::new(&config.exchange, config.symbol.clone(), config.market_type)?;
    let events = build_exact_universe_replay_source(
        replay_path,
        &config.exchange,
        std::slice::from_ref(&instrument),
        "volume-maker",
    )?
    .events;
    let start = match events.front() {
        Some(MarketDataEvent::Observation(observation)) => observation.received_at,
        Some(
            MarketDataEvent::SourceGap { observed_at, .. }
            | MarketDataEvent::SourceUnavailable { observed_at, .. },
        ) => *observed_at,
        None => bail!("volume-maker replay has no events inside the configured universe"),
    };
    let clock = Arc::new(ReplayMarketDataClock::new(start));
    let exchange_clock = Arc::clone(&clock);
    let paper_exchange = Arc::new(
        PaperExchange::with_clock(
            config.exchange.clone(),
            NonZeroUsize::new(VOLUME_MAKER_EVENT_CAPACITY)
                .context("volume-maker paper event capacity must be non-zero")?,
            move || crypto_trading_runtime::MarketDataClock::now(exchange_clock.as_ref()),
        )
        .map_err(anyhow::Error::new)
        .context("failed to build the volume-maker paper exchange")?,
    );
    let latest: Arc<std::sync::Mutex<Option<MarketSnapshot>>> =
        Arc::new(std::sync::Mutex::new(None));
    let source = VolumeMakerReplaySource {
        source_id: config.exchange.clone(),
        events,
        clock: Arc::clone(&clock),
        latest: Arc::clone(&latest),
        exchange: Arc::clone(&paper_exchange),
    };
    let executor = Arc::new(VolumeMakerReplayExecutor {
        exchange: paper_exchange,
        exchange_name: config.exchange.clone(),
        clock,
        latest,
    });

    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new(
            format!("paper-volume-maker:{task_id}"),
            Money::new(Decimal::from(VOLUME_MAKER_INITIAL_AVAILABLE)),
        )
        .map_err(anyhow::Error::new)?,
    )
    .map_err(anyhow::Error::new)
    .context("failed to plan the volume-maker paper account")?;
    let account_risk = AccountRiskAuthority::new(
        account.journal_id(),
        history.clone(),
        VOLUME_MAKER_ACCOUNT_RISK_SCOPE,
        AccountRiskPolicy::new(AccountRiskLimits::default()).map_err(anyhow::Error::new)?,
    )
    .map_err(anyhow::Error::new)
    .context("failed to build the volume-maker account-risk authority")?;

    let mut task_config = VolumeMakerPaperTaskConfig::new(
        task_id,
        strategy,
        PaperCostModel::v1(
            VOLUME_MAKER_COST_FEE_BPS,
            VOLUME_MAKER_COST_FUNDING_BUFFER_BPS,
            VOLUME_MAKER_COST_SLIPPAGE_BPS,
        )
        .map_err(anyhow::Error::new)?,
        supervisor_config(args.shutdown_grace_ms)?,
    )
    .map_err(anyhow::Error::new)
    .context("invalid volume-maker task configuration")?
    .with_account_risk(account_risk)
    .with_cycle_interval(volume_maker_cycle_interval(&config)?);
    if let Some(max_cycles) = config.max_cycles {
        task_config = task_config.with_max_cycles(max_cycles);
    }
    if let Some(target_volume) = config.target_volume {
        task_config = task_config.with_target_volume(target_volume);
    }

    let (shutdown, mut task) =
        start_after_shutdown_registration(register_task_host_shutdown, || async move {
            VolumeMakerPaperTask::start(task_config, source, account, history, executor)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to start continuous volume-maker task")
        })
        .await?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind volume-maker control socket on {address}"))?;

    println!(
        "continuous volume-maker task started: task_id={} control={} history={}",
        task_id,
        address,
        args.history_path.display()
    );

    let outcome = serve_host_with_shutdown(
        &mut task,
        listener,
        StdDuration::from_millis(args.control_poll_interval_ms.max(1)),
        render_live_volume_maker_status,
        render_live_volume_maker_stop,
        Ok(shutdown),
    )
    .await
    .map_err(|error| anyhow::Error::new(error).context("volume-maker control host failed"))?;

    match outcome {
        TaskHostServeOutcome::StopRequested(exit) => {
            println!("continuous volume-maker task stopped: task_id={task_id} exit={exit}");
        }
        TaskHostServeOutcome::Terminal(status) => {
            println!(
                "continuous volume-maker task terminated: task_id={} phase={} processed_event_count={} completed_cycles={}",
                status.task_id,
                status.phase,
                status.processed_event_count,
                status.completed_cycle_count
            );
        }
    }
    Ok(())
}

async fn run_volume_maker_status(args: &VolumeMakerArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("volume-maker status mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Status).await {
        print!("{response}");
        return Ok(());
    }
    print!(
        "{}",
        render_projected_task_status(&project_task_status(
            &args.history_path,
            task_id,
            VOLUME_MAKER_TASK_PROJECTION,
        )?)
    );
    Ok(())
}

async fn run_volume_maker_stop(args: &VolumeMakerArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("volume-maker stop mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Stop).await {
        print!("{response}");
        return Ok(());
    }
    let projected = project_task_status(&args.history_path, task_id, VOLUME_MAKER_TASK_PROJECTION)?;
    if projected.phase == "stopped" || projected.phase == "failed" {
        print!("{}", render_projected_task_status(&projected));
        return Ok(());
    }
    bail!(
        "volume-maker task control endpoint is unavailable at {address}; the task is not confirmed stopped"
    );
}

/// Converts the validated legacy `cycle_interval` seconds into event-time
/// pacing for the paper owner.
fn volume_maker_cycle_interval(config: &VolumeMakerConfig) -> Result<StdDuration> {
    let milliseconds = config
        .interval_seconds
        .checked_mul(Decimal::ONE_THOUSAND)
        .and_then(|value| value.to_u64())
        .context("volume-maker cycle interval is out of range")?;
    Ok(StdDuration::from_millis(milliseconds))
}

/// Finite replay source that mirrors every observation into the paper
/// exchange book before the owner consumes it.
struct VolumeMakerReplaySource {
    source_id: String,
    events: VecDeque<MarketDataEvent>,
    clock: Arc<ReplayMarketDataClock>,
    latest: Arc<std::sync::Mutex<Option<MarketSnapshot>>>,
    exchange: Arc<PaperExchange>,
}

impl fmt::Debug for VolumeMakerReplaySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VolumeMakerReplaySource")
            .field("source_id", &self.source_id)
            .field("remaining_events", &self.events.len())
            .finish_non_exhaustive()
    }
}

impl MarketDataEventSource for VolumeMakerReplaySource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        let Some(event) = self.events.pop_front() else {
            return Box::pin(async move { Ok(None) });
        };
        let clock = Arc::clone(&self.clock);
        let latest = Arc::clone(&self.latest);
        let exchange = Arc::clone(&self.exchange);
        let source_id = self.source_id.clone();
        Box::pin(async move {
            match &event {
                MarketDataEvent::Observation(observation) => {
                    clock.advance(observation.received_at);
                    *latest
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(observation.snapshot.clone());
                    exchange
                        .publish_snapshot(observation.snapshot.clone())
                        .await
                        .map_err(|_| MarketDataError::SourceEventTimeRollback {
                            exchange: source_id.clone(),
                        })?;
                }
                MarketDataEvent::SourceGap { observed_at, .. }
                | MarketDataEvent::SourceUnavailable { observed_at, .. } => {
                    clock.advance(*observed_at);
                }
            }
            Ok(Some(event))
        })
    }
}

/// Paper-exchange execution seam for the replay-backed volume-maker host.
struct VolumeMakerReplayExecutor {
    exchange: Arc<PaperExchange>,
    exchange_name: String,
    clock: Arc<ReplayMarketDataClock>,
    latest: Arc<std::sync::Mutex<Option<MarketSnapshot>>>,
}

impl VolumeMakerPaperExecutor for VolumeMakerReplayExecutor {
    fn execute(&self, batch: ExecutionBatch) -> VolumeMakerPaperExecutionFuture {
        let exchange = Arc::clone(&self.exchange);
        let exchange_name = self.exchange_name.clone();
        let clock = Arc::clone(&self.clock);
        let latest = Arc::clone(&self.latest);
        Box::pin(async move {
            let Some(intent) = batch.intents().first().cloned() else {
                return Err(RuntimeError::InvalidExecutionPolicy(
                    "volume-maker batch must contain exactly one intent",
                ));
            };
            if intent.exchange != exchange_name {
                return Err(RuntimeError::UnknownExchange(exchange_name));
            }
            let snapshot = latest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .ok_or_else(|| RuntimeError::MissingMarketData {
                    exchange: intent.exchange.clone(),
                    symbol: intent.symbol.clone(),
                    market_type: intent.market_type,
                })?;
            let policy = ExecutionPolicy::new(
                true,
                false,
                crypto_trading_runtime::ExecutionClock::now(clock.as_ref()),
                Duration::seconds(VOLUME_MAKER_MAX_MARKET_AGE_SECONDS),
                vec![snapshot],
            )?
            .with_clock(clock);
            let executor = IntentExecutor::new(exchange, ExecutionMode::Paper, policy);
            executor.execute_batch(batch).await
        })
    }
}

fn render_live_volume_maker_status(status: &VolumeMakerPaperTaskStatus) -> String {
    format_task_status(&TaskStatusRender {
        task_id: &status.task_id,
        phase: Cow::Owned(status.phase.to_string()),
        recovery: Cow::Borrowed("none"),
        failure: Cow::Owned(
            status
                .failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
        processed_event_count: status.processed_event_count,
        updated_at: Cow::Owned(
            status
                .last_recorded_at
                .map_or_else(|| "none".to_owned(), |recorded_at| recorded_at.to_rfc3339()),
        ),
        exit: Cow::Owned(
            status
                .exit
                .map_or("none".to_owned(), |exit| exit.to_string()),
        ),
        runtime_failure: Cow::Owned(
            status
                .runtime_failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
    })
}

fn render_live_volume_maker_stop(
    status: &VolumeMakerPaperTaskStatus,
    _exit: VolumeMakerPaperTaskExit,
) -> String {
    render_live_volume_maker_status(status)
}

/// Bounded staleness horizon for replay-backed price-alert readiness checks.
const PRICE_ALERT_MAX_SNAPSHOT_AGE_SECONDS: i64 = 300;
/// Fixed local notification budgets for the journal-only CLI task host.
const PRICE_ALERT_NOTIFICATION_QUEUE_CAPACITY: usize = 64;
const PRICE_ALERT_NOTIFICATION_TIMEOUT: StdDuration = StdDuration::from_millis(500);

async fn run_price_alert(args: &PriceAlertArgs) -> Result<()> {
    match args.mode {
        PriceAlertMode::Validate => {
            run_price_alert_validate(args)?;
            Ok(())
        }
        PriceAlertMode::Serve => run_price_alert_serve(args).await,
        PriceAlertMode::Status => run_price_alert_status(args).await,
        PriceAlertMode::Stop => run_price_alert_stop(args).await,
    }
}

fn run_price_alert_validate(args: &PriceAlertArgs) -> Result<PriceAlertConfig> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let config = load_price_alert_config_from_str(&body).with_context(|| {
        format!(
            "failed to load price-alert config {}",
            args.config.display()
        )
    })?;
    println!(
        "valid: price-alert {} exchange={} symbols={} enabled={}",
        args.config.display(),
        config.exchange,
        config.symbols.len(),
        config
            .symbols
            .iter()
            .filter(|symbol| symbol.enabled)
            .count()
    );
    Ok(config)
}

async fn run_price_alert_serve(args: &PriceAlertArgs) -> Result<()> {
    let config = run_price_alert_validate(args)?;
    let replay_path = args.replay.as_ref().context(
        "price-alert serve currently requires --replay until external continuous source adapters are wired into this task host",
    )?;
    let task_id = args
        .task_id
        .as_deref()
        .context("price-alert serve mode requires --task-id")?;
    let history = JsonlHistory::new(&args.history_path);
    let dispatcher = NotificationDispatcherConfig::new(
        PRICE_ALERT_NOTIFICATION_QUEUE_CAPACITY,
        PRICE_ALERT_NOTIFICATION_TIMEOUT,
        PRICE_ALERT_NOTIFICATION_TIMEOUT,
    )
    .map_err(anyhow::Error::new)
    .context("invalid price-alert notification budgets")?;
    let runtime_config = PriceAlertRuntimeConfig::new(
        AlertDeliveryMode::JournalOnly,
        dispatcher,
        MAX_RECENT_ALERT_OCCURRENCES,
    )
    .map_err(anyhow::Error::new)
    .context("invalid price-alert runtime bounds")?;
    let runtime = PriceAlertRuntime::new(
        &config,
        MarketFreshnessPolicy::new(
            Duration::seconds(PRICE_ALERT_MAX_SNAPSHOT_AGE_SECONDS),
            Duration::seconds(1),
        )?,
        Arc::new(SystemMarketDataClock),
        history.clone(),
        runtime_config,
        Vec::new(),
    )
    .map_err(anyhow::Error::new)
    .context("failed to build the price-alert runtime")?;
    let source = build_alert_serve_replay_source(replay_path, &config)?;
    let task_config = ContinuousAlertTaskConfig::new(
        task_id,
        &config.exchange,
        supervisor_config(args.shutdown_grace_ms)?,
    )
    .map_err(anyhow::Error::new)?;
    let (shutdown, mut task) =
        start_after_shutdown_registration(register_task_host_shutdown, || async move {
            ContinuousAlertTask::start(task_config, runtime, source, history)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to start continuous price-alert task")
        })
        .await?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind price-alert control socket on {address}"))?;

    println!(
        "continuous price-alert task started: task_id={} control={} history={}",
        task_id,
        address,
        args.history_path.display()
    );

    let outcome = serve_host_with_shutdown(
        &mut task,
        listener,
        StdDuration::from_millis(args.control_poll_interval_ms.max(1)),
        render_live_alert_status,
        render_live_alert_stop,
        Ok(shutdown),
    )
    .await
    .map_err(|error| anyhow::Error::new(error).context("price-alert control host failed"))?;

    match outcome {
        TaskHostServeOutcome::StopRequested(exit) => {
            println!("continuous price-alert task stopped: task_id={task_id} exit={exit}");
        }
        TaskHostServeOutcome::Terminal(status) => {
            println!(
                "continuous price-alert task terminated: task_id={} phase={} processed_event_count={}",
                status.task_id, status.phase, status.processed_event_count
            );
        }
    }
    Ok(())
}

async fn run_price_alert_status(args: &PriceAlertArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("price-alert status mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Status).await {
        print!("{response}");
        return Ok(());
    }
    print!(
        "{}",
        render_projected_task_status(&project_task_status(
            &args.history_path,
            task_id,
            PRICE_ALERT_TASK_PROJECTION,
        )?)
    );
    Ok(())
}

async fn run_price_alert_stop(args: &PriceAlertArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("price-alert stop mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Stop).await {
        print!("{response}");
        return Ok(());
    }
    let projected = project_task_status(&args.history_path, task_id, PRICE_ALERT_TASK_PROJECTION)?;
    if projected.phase == "stopped" || projected.phase == "failed" {
        print!("{}", render_projected_task_status(&projected));
        return Ok(());
    }
    bail!(
        "price-alert task control endpoint is unavailable at {address}; the task is not confirmed stopped"
    );
}

/// One replay-backed source scoped to the exact configured alert universe, so
/// out-of-universe records never reach the fail-closed market book.
fn build_alert_serve_replay_source(
    replay_path: &Path,
    config: &PriceAlertConfig,
) -> Result<ServeReplaySource> {
    let mut instruments = Vec::new();
    for symbol in config.symbols.iter().filter(|symbol| symbol.enabled) {
        instruments.push(MarketInstrument::new(
            &config.exchange,
            symbol.symbol.clone(),
            symbol.market_type,
        )?);
    }
    build_exact_universe_replay_source(replay_path, &config.exchange, &instruments, "price-alert")
}

/// One replay-backed single source restricted to an exact instrument universe.
fn build_exact_universe_replay_source(
    replay_path: &Path,
    exchange: &str,
    instruments: &[MarketInstrument],
    label: &str,
) -> Result<ServeReplaySource> {
    let events = load_market_snapshot_replay(replay_path)?
        .into_iter()
        .filter(|event| match event {
            MarketDataEvent::Observation(observation) => {
                MarketInstrument::from_snapshot(&observation.snapshot)
                    .is_ok_and(|instrument| instruments.contains(&instrument))
            }
            MarketDataEvent::SourceGap {
                exchange: event_exchange,
                ..
            }
            | MarketDataEvent::SourceUnavailable {
                exchange: event_exchange,
                ..
            } => event_exchange == exchange,
        })
        .collect::<VecDeque<_>>();
    if events.is_empty() {
        bail!(
            "{label} replay {} has no events inside the configured {label} universe",
            replay_path.display()
        );
    }
    Ok(ServeReplaySource {
        source_id: exchange.to_owned(),
        events,
    })
}

fn render_live_alert_status(status: &ContinuousAlertTaskStatus) -> String {
    format_task_status(&TaskStatusRender {
        task_id: &status.task_id,
        phase: Cow::Owned(status.phase.to_string()),
        recovery: Cow::Borrowed("none"),
        failure: Cow::Owned(
            status
                .failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
        processed_event_count: status.processed_event_count,
        updated_at: Cow::Owned(
            status
                .last_recorded_at
                .map_or_else(|| "none".to_owned(), |recorded_at| recorded_at.to_rfc3339()),
        ),
        exit: Cow::Owned(
            status
                .exit
                .map_or("none".to_owned(), |exit| exit.to_string()),
        ),
        runtime_failure: Cow::Owned(
            status
                .runtime_failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
    })
}

fn render_live_alert_stop(
    status: &ContinuousAlertTaskStatus,
    _exit: ContinuousAlertTaskExit,
) -> String {
    render_live_alert_status(status)
}

async fn run_scanner(args: &ScannerArgs) -> Result<()> {
    match args.mode {
        ScannerMode::Validate => {
            run_scanner_validate(args)?;
            Ok(())
        }
        ScannerMode::Serve => run_scanner_serve(args).await,
        ScannerMode::Status => run_scanner_status(args).await,
        ScannerMode::Stop => run_scanner_stop(args).await,
    }
}

fn run_scanner_validate(args: &ScannerArgs) -> Result<ScannerConfig> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let config = load_scanner_config_from_str(&body)
        .with_context(|| format!("failed to load scanner config {}", args.config.display()))?;
    println!(
        "valid: scanner {} exchange={} symbols={} enabled={}",
        args.config.display(),
        config.exchange,
        config.symbols.len(),
        config.enabled_symbols().count()
    );
    Ok(config)
}

async fn run_scanner_serve(args: &ScannerArgs) -> Result<()> {
    let config = run_scanner_validate(args)?;
    let replay_path = args.replay.as_ref().context(
        "scanner serve currently requires --replay until external market-discovery adapters are wired into this task host",
    )?;
    let task_id = args
        .task_id
        .as_deref()
        .context("scanner serve mode requires --task-id")?;
    let history = JsonlHistory::new(&args.history_path);
    let specs = scanner_candidate_specs(&config)?;
    let instruments = specs
        .iter()
        .map(|spec| spec.instrument.clone())
        .collect::<Vec<_>>();
    let runtime = ScannerReplayRuntime::new(
        task_id,
        config.apr_window_seconds,
        config.min_complete_cycles,
        config.row_limit,
        specs,
    )
    .map_err(anyhow::Error::new)
    .context("scanner runtime rejected the validated configuration")?;
    // Pacing keeps every replay observation intact across the supervisor's
    // O(1) event retention, which the deterministic ranking depends on.
    let source = runtime.pace(build_exact_universe_replay_source(
        replay_path,
        &config.exchange,
        &instruments,
        "scanner",
    )?);
    let task_config = ContinuousScannerTaskConfig::new(
        task_id,
        &config.exchange,
        supervisor_config(args.shutdown_grace_ms)?,
    )
    .map_err(anyhow::Error::new)?;
    let (shutdown, mut task) =
        start_after_shutdown_registration(register_task_host_shutdown, || async move {
            ContinuousScannerTask::start(task_config, runtime, source, history)
                .await
                .map_err(anyhow::Error::new)
                .context("failed to start continuous scanner task")
        })
        .await?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind scanner control socket on {address}"))?;

    println!(
        "continuous scanner task started: task_id={} control={} history={}",
        task_id,
        address,
        args.history_path.display()
    );

    let outcome = serve_host_with_shutdown(
        &mut task,
        listener,
        StdDuration::from_millis(args.control_poll_interval_ms.max(1)),
        render_live_scanner_status,
        render_live_scanner_stop,
        Ok(shutdown),
    )
    .await
    .map_err(|error| anyhow::Error::new(error).context("scanner control host failed"))?;

    match outcome {
        TaskHostServeOutcome::StopRequested(exit) => {
            println!("continuous scanner task stopped: task_id={task_id} exit={exit}");
        }
        TaskHostServeOutcome::Terminal(status) => {
            println!(
                "continuous scanner task terminated: task_id={} phase={} processed_event_count={}",
                status.task_id, status.phase, status.processed_event_count
            );
        }
    }
    Ok(())
}

async fn run_scanner_status(args: &ScannerArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("scanner status mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Status).await {
        print!("{response}");
        return Ok(());
    }
    print!(
        "{}",
        render_projected_task_status(&project_task_status(
            &args.history_path,
            task_id,
            SCANNER_TASK_PROJECTION,
        )?)
    );
    Ok(())
}

async fn run_scanner_stop(args: &ScannerArgs) -> Result<()> {
    let task_id = args
        .task_id
        .as_deref()
        .context("scanner stop mode requires --task-id")?;
    let address = control_addr(task_id, &args.history_path, args.control_port);
    if let Ok(response) = query_control(address, TaskHostControlCommand::Stop).await {
        print!("{response}");
        return Ok(());
    }
    let projected = project_task_status(&args.history_path, task_id, SCANNER_TASK_PROJECTION)?;
    if projected.phase == "stopped" || projected.phase == "failed" {
        print!("{}", render_projected_task_status(&projected));
        return Ok(());
    }
    bail!(
        "scanner task control endpoint is unavailable at {address}; the task is not confirmed stopped"
    );
}

/// Exact enabled scan candidates derived from one validated scanner config.
fn scanner_candidate_specs(config: &ScannerConfig) -> Result<Vec<ScannerCandidateSpec>> {
    let mut specs = Vec::new();
    for symbol in config.enabled_symbols() {
        specs.push(ScannerCandidateSpec {
            instrument: MarketInstrument::new(
                &config.exchange,
                symbol.symbol.clone(),
                symbol.market_type,
            )?,
            grid_width_percent: symbol.grid_width_percent,
            grid_interval_percent: symbol.grid_interval_percent,
            volume_24h_usdc: symbol.volume_24h_usdc,
            price_change_24h_percent: symbol.price_change_24h_percent,
            benchmark: symbol.benchmark,
        });
    }
    Ok(specs)
}

fn render_live_scanner_status(status: &ContinuousScannerTaskStatus) -> String {
    format_task_status(&TaskStatusRender {
        task_id: &status.task_id,
        phase: Cow::Owned(status.phase.to_string()),
        recovery: Cow::Borrowed("none"),
        failure: Cow::Owned(
            status
                .failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
        processed_event_count: status.processed_event_count,
        updated_at: Cow::Owned(
            status
                .last_recorded_at
                .map_or_else(|| "none".to_owned(), |recorded_at| recorded_at.to_rfc3339()),
        ),
        exit: Cow::Owned(
            status
                .exit
                .map_or("none".to_owned(), |exit| exit.to_string()),
        ),
        runtime_failure: Cow::Owned(
            status
                .runtime_failure
                .map_or("none".to_owned(), |failure| failure.to_string()),
        ),
    })
}

fn render_live_scanner_stop(
    status: &ContinuousScannerTaskStatus,
    _exit: ContinuousScannerTaskExit,
) -> String {
    render_live_scanner_status(status)
}

async fn execute_grid_paper(
    config: &GridConfig,
    value: Decimal,
    history_path: &Path,
) -> Result<PaperExecution> {
    let (snapshot, intents) = plan_grid_intents(config, value)?;
    let intent_count = intents.len();
    let batch = execution_batch(intents)?;
    let batch_id = batch.id().to_string();
    let runtime_policy = ExecutionPolicy::new(
        true,
        false,
        Utc::now(),
        Duration::seconds(5),
        vec![snapshot.clone()],
    )?;
    let paper = Arc::new(PaperExchange::new(
        config.exchange.clone(),
        event_capacity(),
    )?);
    paper.publish_snapshot(snapshot).await?;
    let history = JsonlHistory::new(history_path);
    append_execution_planned(
        &history,
        "grid",
        config.symbol.as_str(),
        &batch,
        json!({
            "snapshot_price": value,
            "intent_count": intent_count,
        }),
    )
    .await?;
    let executor = IntentExecutor::new(paper, ExecutionMode::Paper, runtime_policy);
    let receipts = finish_execution(
        &history,
        "grid",
        config.symbol.as_str(),
        &batch_id,
        executor.execute_batch(batch).await,
    )
    .await?;
    Ok(PaperExecution { receipts })
}

fn plan_grid_intents(
    config: &GridConfig,
    value: Decimal,
) -> Result<(MarketSnapshot, Vec<OrderIntent>)> {
    let price = Price::new(value).context("paper snapshot price must be greater than zero")?;
    let planner = GridPlanner::try_from(config)?;
    let snapshot = MarketSnapshot::new(
        config.exchange.clone(),
        config.symbol.clone(),
        config.market_type,
        price,
        price,
        Utc::now(),
    )?;
    let strategy = GridStrategy::new(planner);
    let intents = strategy.evaluate(&GridState::default(), &snapshot)?;
    Ok((snapshot, intents))
}

fn resolve_arbitrage_snapshots(
    args: &ArbitrageArgs,
    monitor: &MonitorConfig,
) -> Result<[MarketSnapshot; 2]> {
    let left_exchange = args
        .market
        .left_exchange
        .as_deref()
        .or_else(|| monitor.exchanges.first().map(String::as_str))
        .context("--once needs --left-exchange or a first exchange in monitor config")?;
    let right_exchange = args
        .market
        .right_exchange
        .as_deref()
        .or_else(|| monitor.exchanges.get(1).map(String::as_str))
        .context("--once needs --right-exchange or a second exchange in monitor config")?;
    if left_exchange == right_exchange {
        bail!("one-shot arbitrage requires two distinct exchanges");
    }
    let left_symbol = if let Some(symbol) = args.market.left_symbol.as_deref() {
        Symbol::new(symbol)?
    } else {
        monitor
            .symbols
            .first()
            .cloned()
            .context("--once needs a symbol in monitor config or --left-symbol")?
    };
    let right_symbol = args
        .market
        .right_symbol
        .as_deref()
        .map(Symbol::new)
        .transpose()?
        .unwrap_or_else(|| left_symbol.clone());

    Ok([
        market_snapshot(
            left_exchange,
            left_symbol,
            args.market.left_bid.context("--once requires --left-bid")?,
            args.market.left_ask.context("--once requires --left-ask")?,
            args.market
                .left_bid_quantity
                .context("--once requires --left-bid-quantity")?,
            args.market
                .left_ask_quantity
                .context("--once requires --left-ask-quantity")?,
        )?,
        market_snapshot(
            right_exchange,
            right_symbol,
            args.market
                .right_bid
                .context("--once requires --right-bid")?,
            args.market
                .right_ask
                .context("--once requires --right-ask")?,
            args.market
                .right_bid_quantity
                .context("--once requires --right-bid-quantity")?,
            args.market
                .right_ask_quantity
                .context("--once requires --right-ask-quantity")?,
        )?,
    ])
}

fn market_snapshot(
    exchange: &str,
    symbol: Symbol,
    bid: Decimal,
    ask: Decimal,
    bid_quantity: Decimal,
    ask_quantity: Decimal,
) -> Result<MarketSnapshot> {
    let mut snapshot = MarketSnapshot::new(
        exchange,
        symbol,
        MarketType::Perpetual,
        Price::new(bid).context("paper bid must be greater than zero")?,
        Price::new(ask).context("paper ask must be greater than zero")?,
        Utc::now(),
    )?;
    snapshot.bid_quantity =
        Some(Quantity::new(bid_quantity).context("paper bid quantity must not be negative")?);
    snapshot.ask_quantity =
        Some(Quantity::new(ask_quantity).context("paper ask quantity must not be negative")?);
    Ok(snapshot)
}

#[derive(Debug)]
struct ArbitrageExecutionPolicy {
    strategy_key: Symbol,
    data_timeout_seconds: u64,
    monitor_exchanges: Vec<String>,
    monitor_symbols: Vec<Symbol>,
    configured_exchanges: Vec<String>,
    configured_symbols: Vec<Symbol>,
    leg_markets: Vec<(String, Symbol)>,
}

fn resolve_arbitrage_policy(
    args: &ArbitrageArgs,
    config: &ArbitrageConfig,
    monitor: &MonitorConfig,
    snapshots: &[MarketSnapshot; 2],
) -> Result<(ArbitrageConfig, ArbitrageExecutionPolicy)> {
    if !config.enabled {
        bail!("arbitrage execution is disabled by configuration");
    }
    if config.monitor_only {
        bail!("arbitrage execution is blocked by monitor-only mode");
    }

    let strategy_key = if let Some(value) = args.market.strategy_key.as_deref() {
        Symbol::new(value).context("--strategy-key must not be empty")?
    } else if snapshots[0].symbol == snapshots[1].symbol {
        snapshots[0].symbol.clone()
    } else {
        bail!("--strategy-key is required when arbitrage leg symbols differ");
    };

    let policy = ArbitrageExecutionPolicy {
        strategy_key: strategy_key.clone(),
        data_timeout_seconds: monitor.data_timeout_seconds,
        monitor_exchanges: monitor.exchanges.clone(),
        monitor_symbols: monitor.symbols.clone(),
        configured_exchanges: config.exchanges.clone(),
        configured_symbols: config.symbols.clone(),
        leg_markets: snapshots
            .iter()
            .map(|snapshot| (snapshot.exchange().to_owned(), snapshot.symbol.clone()))
            .collect(),
    };
    policy.validate_snapshots(args, snapshots)?;
    let effective = config.resolve_for_strategy(&strategy_key)?;
    Ok((effective, policy))
}

impl ArbitrageExecutionPolicy {
    fn validate_snapshots(
        &self,
        args: &ArbitrageArgs,
        snapshots: &[MarketSnapshot; 2],
    ) -> Result<()> {
        for snapshot in snapshots {
            if !self
                .monitor_exchanges
                .iter()
                .any(|exchange| exchange == snapshot.exchange())
            {
                bail!(
                    "{} is outside the monitor exchange allowlist",
                    snapshot.exchange()
                );
            }
            if !self.monitor_symbols.contains(&snapshot.symbol) {
                bail!(
                    "{} is outside the monitor symbol allowlist",
                    snapshot.symbol
                );
            }
            if !self.configured_exchanges.is_empty()
                && !self
                    .configured_exchanges
                    .iter()
                    .any(|exchange| exchange == snapshot.exchange())
            {
                bail!(
                    "{} is outside the arbitrage exchange allowlist",
                    snapshot.exchange()
                );
            }
            if !self.configured_symbols.is_empty()
                && !self.configured_symbols.contains(&snapshot.symbol)
            {
                bail!(
                    "{} is outside the arbitrage symbol allowlist",
                    snapshot.symbol
                );
            }
            if !args.symbols.is_empty()
                && !args
                    .symbols
                    .iter()
                    .any(|symbol| symbol == snapshot.symbol.as_str())
            {
                bail!("{} is outside the CLI symbol filter", snapshot.symbol);
            }
        }
        Ok(())
    }

    fn validate_submission(
        &self,
        source_config: &ArbitrageConfig,
        intents: &[OrderIntent],
    ) -> Result<()> {
        if !source_config.enabled || source_config.monitor_only {
            bail!("arbitrage operator controls changed before submission");
        }
        source_config.resolve_for_strategy(&self.strategy_key)?;
        for intent in intents {
            if !self
                .leg_markets
                .iter()
                .any(|(exchange, symbol)| exchange == &intent.exchange && symbol == &intent.symbol)
            {
                bail!(
                    "intent {}/{} is outside the authorized arbitrage legs",
                    intent.exchange,
                    intent.symbol
                );
            }
            if !self
                .monitor_exchanges
                .iter()
                .any(|exchange| exchange == &intent.exchange)
                || !self.monitor_symbols.contains(&intent.symbol)
            {
                bail!(
                    "intent {}/{} failed the monitor allowlist recheck",
                    intent.exchange,
                    intent.symbol
                );
            }
        }
        Ok(())
    }
}

async fn execute_arbitrage_paper(
    config: &ArbitrageConfig,
    source_config: &ArbitrageConfig,
    policy: &ArbitrageExecutionPolicy,
    [left, right]: [MarketSnapshot; 2],
    history_path: &Path,
) -> Result<(ArbitrageDecision, PaperExecution)> {
    let strategy = ArbitrageStrategy::try_from(config)?;
    let decision = strategy.evaluate_pair(&ArbitrageState::default(), &left, &right)?;
    let intent_count = decision.intents.len();
    policy.validate_submission(source_config, &decision.intents)?;
    let max_market_age = Duration::try_seconds(
        i64::try_from(policy.data_timeout_seconds)
            .context("monitor data timeout does not fit the runtime clock")?,
    )
    .context("monitor data timeout is outside chrono's supported range")?;
    let now = Utc::now();
    authorize_arbitrage_risk(
        config,
        &decision.intents,
        [&left, &right],
        now,
        max_market_age,
    )?;
    validate_arbitrage_liquidity(&decision.intents, [&left, &right])?;
    let batch = execution_batch(decision.intents.clone())?;
    let batch_id = batch.id().to_string();
    let runtime_policy = ExecutionPolicy::new(
        source_config.enabled,
        source_config.monitor_only,
        now,
        max_market_age,
        vec![left.clone(), right.clone()],
    )?;

    let left_paper = Arc::new(PaperExchange::new(
        left.exchange().to_owned(),
        event_capacity(),
    )?);
    let right_paper = Arc::new(PaperExchange::new(
        right.exchange().to_owned(),
        event_capacity(),
    )?);
    left_paper.publish_snapshot(left.clone()).await?;
    right_paper.publish_snapshot(right.clone()).await?;

    let history = JsonlHistory::new(history_path);
    append_execution_planned(
        &history,
        "arbitrage",
        policy.strategy_key.as_str(),
        &batch,
        json!({
            "kind": format!("{:?}", decision.kind).to_ascii_lowercase(),
            "segment": decision.segment,
            "spread_percent": decision.spread.percent,
            "target_quantity": decision.target_quantity,
            "intent_count": intent_count,
            "strategy_key": policy.strategy_key,
            "buy_exchange": decision.spread.buy_exchange,
            "sell_exchange": decision.spread.sell_exchange,
        }),
    )
    .await?;

    let mut router = ExchangeRouter::new(ExecutionMode::Paper, runtime_policy);
    router.register(left.exchange().to_owned(), left_paper);
    router.register(right.exchange().to_owned(), right_paper);
    let receipts = finish_arbitrage_execution(
        &history,
        policy.strategy_key.as_str(),
        &batch_id,
        intent_count,
        router.execute_batch(batch).await,
    )
    .await?;

    Ok((decision, PaperExecution { receipts }))
}

fn authorize_arbitrage_risk(
    config: &ArbitrageConfig,
    intents: &[OrderIntent],
    markets: [&MarketSnapshot; 2],
    now: chrono::DateTime<Utc>,
    max_snapshot_age: Duration,
) -> Result<()> {
    let max_position_value = config
        .max_position_value
        .context("arbitrage max_position_value is required for paper execution")?;
    let engine = RiskEngine::new(RiskLimits {
        max_position_value,
        max_snapshot_age,
    })?;
    let account = AccountRiskSnapshot {
        equity: Money::default(),
        available_balance: Money::default(),
        kill_switch: false,
        timestamp: now,
    };
    let markets = markets.into_iter().cloned().collect::<Vec<_>>();
    match engine.authorize_batch(intents, &account, &[], &markets, now) {
        RiskDecision::Authorized => Ok(()),
        RiskDecision::Rejected(rejection) => {
            bail!("arbitrage risk rejected the batch: {rejection:?}")
        }
    }
}

fn validate_arbitrage_liquidity(
    intents: &[OrderIntent],
    markets: [&MarketSnapshot; 2],
) -> Result<()> {
    let mut required = HashMap::<(String, Symbol, MarketType, Side), Decimal>::new();
    for intent in intents {
        let market = markets
            .iter()
            .copied()
            .find(|market| {
                market.exchange() == intent.exchange
                    && market.symbol == intent.symbol
                    && market.market_type == intent.market_type
            })
            .with_context(|| {
                format!(
                    "paper liquidity snapshot is missing for {}/{}/{:?}",
                    intent.exchange, intent.symbol, intent.market_type
                )
            })?;
        let immediately_executable = match intent.order_type {
            OrderType::Market => true,
            OrderType::Limit => {
                let price = intent
                    .price
                    .context("arbitrage limit intent is missing its price")?;
                match intent.side {
                    Side::Buy => price >= market.ask(),
                    Side::Sell => price <= market.bid(),
                }
            }
        };
        if !immediately_executable {
            bail!(
                "arbitrage paper intent {}/{}/{:?} is not immediately executable",
                intent.exchange,
                intent.symbol,
                intent.side
            );
        }

        let key = (
            intent.exchange.clone(),
            intent.symbol.clone(),
            intent.market_type,
            intent.side,
        );
        let total = required.entry(key).or_default();
        *total = total
            .checked_add(intent.quantity.as_decimal())
            .context("arbitrage paper depth requirement overflowed")?;
    }

    for ((exchange, symbol, market_type, side), needed) in required {
        let market = markets
            .iter()
            .copied()
            .find(|market| {
                market.exchange() == exchange
                    && market.symbol == symbol
                    && market.market_type == market_type
            })
            .context("validated arbitrage market disappeared")?;
        let available = match side {
            Side::Buy => market.ask_quantity,
            Side::Sell => market.bid_quantity,
        }
        .context("paper top-of-book depth is required for arbitrage execution")?
        .as_decimal();
        if available < needed {
            bail!(
                "insufficient paper top-of-book depth for {exchange}/{symbol}/{market_type:?}/{side:?}: need {needed}, available {available}"
            );
        }
    }
    Ok(())
}

async fn finish_arbitrage_execution(
    history: &JsonlHistory,
    symbol: &str,
    batch_id: &str,
    expected_receipts: usize,
    result: std::result::Result<Vec<TradingReceipt>, RuntimeError>,
) -> Result<Vec<TradingReceipt>> {
    match result {
        Ok(receipts)
            if receipts.len() == expected_receipts
                && receipts.iter().all(|receipt| {
                    receipt.submission_disposition() == Some(SubmissionDisposition::Filled)
                }) =>
        {
            finish_execution(history, "arbitrage", symbol, batch_id, Ok(receipts)).await
        }
        Ok(receipts) => {
            let mut details = receipt_summary(&receipts);
            details["batch_id"] = json!(batch_id);
            details["expected_receipt_count"] = json!(expected_receipts);
            if let Err(journal) = append_execution_outcome(
                history,
                "arbitrage",
                symbol,
                "execution_incomplete",
                details,
            )
            .await
            {
                return Err(ExecutionOutcomeJournalError {
                    outcome: PreservedExecutionOutcome::Incomplete(receipts),
                    journal,
                }
                .into());
            }
            bail!("arbitrage paper batch did not fill every leg; reconcile before another attempt")
        }
        Err(error) => finish_execution(history, "arbitrage", symbol, batch_id, Err(error)).await,
    }
}

const fn event_capacity() -> NonZeroUsize {
    NonZeroUsize::new(256).expect("paper event capacity is non-zero")
}

fn execution_batch(intents: Vec<OrderIntent>) -> Result<ExecutionBatch> {
    ExecutionBatch::planned(intents).map_err(Into::into)
}

async fn append_execution_planned(
    history: &JsonlHistory,
    strategy: &str,
    symbol: &str,
    batch: &ExecutionBatch,
    context: Value,
) -> Result<()> {
    let legs = batch
        .intents()
        .iter()
        .enumerate()
        .map(|(index, intent)| intent_summary(index, intent))
        .collect::<Vec<_>>();
    history
        .append_batch(&[DecisionRecord {
            timestamp: Utc::now(),
            strategy: strategy.to_owned(),
            symbol: symbol.to_owned(),
            decision: "execution_planned".to_owned(),
            details: json!({
                "batch_id": batch.id(),
                "legs": legs,
                "recovery_batch": batch,
                "context": context,
            }),
        }])
        .await?;
    Ok(())
}

async fn finish_execution(
    history: &JsonlHistory,
    strategy: &str,
    symbol: &str,
    batch_id: &str,
    result: std::result::Result<Vec<TradingReceipt>, RuntimeError>,
) -> Result<Vec<TradingReceipt>> {
    match result {
        Ok(receipts) => {
            let mut details = receipt_summary(&receipts);
            details["batch_id"] = json!(batch_id);
            if let Err(journal) =
                append_execution_outcome(history, strategy, symbol, "execution_completed", details)
                    .await
            {
                return Err(ExecutionOutcomeJournalError {
                    outcome: PreservedExecutionOutcome::Completed(receipts),
                    journal,
                }
                .into());
            }
            Ok(receipts)
        }
        Err(error) => {
            let (decision, details) = execution_error_summary(&error, batch_id);
            if let Err(journal) =
                append_execution_outcome(history, strategy, symbol, decision, details).await
            {
                return Err(ExecutionOutcomeJournalError {
                    outcome: PreservedExecutionOutcome::Failed(error),
                    journal,
                }
                .into());
            }
            Err(error.into())
        }
    }
}

#[derive(Debug)]
enum PreservedExecutionOutcome {
    Completed(Vec<TradingReceipt>),
    Incomplete(Vec<TradingReceipt>),
    Failed(RuntimeError),
}

#[derive(Debug)]
struct ExecutionOutcomeJournalError {
    outcome: PreservedExecutionOutcome,
    journal: HistoryError,
}

impl fmt::Display for ExecutionOutcomeJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.outcome {
            PreservedExecutionOutcome::Completed(receipts) => write!(
                formatter,
                "execution completed with {} receipt(s), but the outcome journal failed: {}",
                receipts.len(),
                self.journal
            ),
            PreservedExecutionOutcome::Incomplete(receipts) => write!(
                formatter,
                "execution returned {} incomplete receipt(s), but the outcome journal failed: {}",
                receipts.len(),
                self.journal
            ),
            PreservedExecutionOutcome::Failed(error) => write!(
                formatter,
                "execution failed ({error}), and the outcome journal also failed: {}",
                self.journal
            ),
        }
    }
}

impl Error for ExecutionOutcomeJournalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.journal)
    }
}

async fn append_execution_outcome(
    history: &JsonlHistory,
    strategy: &str,
    symbol: &str,
    decision: &str,
    details: Value,
) -> std::result::Result<(), HistoryError> {
    history
        .append_batch(&[DecisionRecord {
            timestamp: Utc::now(),
            strategy: strategy.to_owned(),
            symbol: symbol.to_owned(),
            decision: decision.to_owned(),
            details,
        }])
        .await?;
    Ok(())
}

fn intent_summary(index: usize, intent: &OrderIntent) -> Value {
    json!({
        "index": index,
        "client_order_id": intent.client_order_id,
        "exchange": intent.exchange,
        "symbol": intent.symbol,
        "market_type": intent.market_type,
        "side": intent.side,
        "order_type": intent.order_type,
        "quantity": intent.quantity,
        "price": intent.price,
        "reduce_only": intent.reduce_only,
        "time_in_force": intent.time_in_force,
    })
}

fn execution_error_summary(error: &RuntimeError, expected_batch_id: &str) -> (&'static str, Value) {
    if let RuntimeError::PartialExecution {
        batch_id,
        failed_index,
        completed,
        failed_intent,
        unattempted,
        reconciliation,
        source,
    } = error
    {
        let reconciliation = reconciliation
            .iter()
            .map(|observation| match &observation.result {
                Ok(receipt) => {
                    let orders = receipt
                        .orders
                        .iter()
                        .take(MAX_RECONCILIATION_SUMMARY_ORDERS)
                        .collect::<Vec<_>>();
                    let foreign_orders = receipt
                        .foreign_orders
                        .iter()
                        .take(MAX_RECONCILIATION_SUMMARY_ORDERS)
                        .collect::<Vec<_>>();
                    let positions = receipt
                        .positions
                        .iter()
                        .take(MAX_RECONCILIATION_SUMMARY_POSITIONS)
                        .collect::<Vec<_>>();
                    json!({
                    "exchange": observation.exchange,
                    "status": "ok",
                    "scope": receipt.scope,
                    "observed_at": receipt.observed_at,
                    "orders": orders,
                    "orders_total": receipt.orders.len(),
                    "orders_truncated": receipt.orders.len() > MAX_RECONCILIATION_SUMMARY_ORDERS,
                    "foreign_orders": foreign_orders,
                    "foreign_orders_total": receipt.foreign_orders.len(),
                    "foreign_orders_truncated": receipt.foreign_orders.len() > MAX_RECONCILIATION_SUMMARY_ORDERS,
                    "positions": positions,
                    "positions_total": receipt.positions.len(),
                    "positions_truncated": receipt.positions.len() > MAX_RECONCILIATION_SUMMARY_POSITIONS,
                })
                }
                Err(error) => json!({
                    "exchange": observation.exchange,
                    "status": "error",
                    "error": error.to_string(),
                }),
            })
            .collect::<Vec<_>>();
        let unattempted = unattempted
            .iter()
            .enumerate()
            .map(|(index, intent)| intent_summary(failed_index + index + 1, intent))
            .collect::<Vec<_>>();
        return (
            "execution_partial",
            json!({
                "batch_id": batch_id,
                "expected_batch_id": expected_batch_id,
                "failed_index": failed_index,
                "completed": receipt_summary(completed),
                "failed_intent": intent_summary(*failed_index, failed_intent),
                "unattempted": unattempted,
                "reconciliation": reconciliation,
                "source": source.to_string(),
            }),
        );
    }

    (
        "execution_failed",
        json!({
            "batch_id": expected_batch_id,
            "error": error.to_string(),
        }),
    )
}

const MAX_RECONCILIATION_SUMMARY_ORDERS: usize = 64;
const MAX_RECONCILIATION_SUMMARY_POSITIONS: usize = 64;

fn receipt_summary(receipts: &[TradingReceipt]) -> Value {
    let mut open = 0;
    let mut filled = 0;
    let mut cancelled = 0;
    let mut already_processed = 0;
    for receipt in receipts {
        match receipt.submission_disposition() {
            Some(SubmissionDisposition::Open) => open += 1,
            Some(SubmissionDisposition::Filled) => filled += 1,
            Some(SubmissionDisposition::Cancelled) | None => cancelled += 1,
            Some(SubmissionDisposition::AlreadyProcessed) => already_processed += 1,
        }
    }
    json!({
        "receipt_count": receipts.len(),
        "receipts": receipts
            .iter()
            .take(MAX_RECEIPT_SUMMARY_RECEIPTS)
            .collect::<Vec<_>>(),
        "receipts_truncated": receipts.len() > MAX_RECEIPT_SUMMARY_RECEIPTS,
        "open": open,
        "filled": filled,
        "cancelled": cancelled,
        "already_processed": already_processed,
    })
}

const MAX_RECEIPT_SUMMARY_RECEIPTS: usize = 64;

fn check_configs(args: &ConfigCheckArgs) -> Result<()> {
    let report = collect_config_report(&args.paths)?;

    let failure_count = report
        .summaries
        .iter()
        .filter(|summary| summary["status"] == "error")
        .count();

    if args.json {
        let output = serde_json::to_string_pretty(&report.summaries)?;
        if output.len().saturating_add(1) > MAX_CONFIG_CHECK_OUTPUT_BYTES {
            bail!("configuration check JSON output exceeded its byte budget");
        }
        println!("{output}");
    } else {
        let mut output = String::with_capacity(report.text_bytes);
        for summary in &report.summaries {
            output.push_str(&render_config_summary(summary));
            output.push('\n');
        }
        if output.len() > MAX_CONFIG_CHECK_OUTPUT_BYTES {
            bail!("configuration check text output exceeded its byte budget");
        }
        print!("{output}");
    }
    if failure_count > 0 {
        bail!("configuration check failed for {failure_count} path(s)");
    }
    Ok(())
}

fn collect_config_report(inputs: &[PathBuf]) -> Result<ConfigCheckReport> {
    let (mut paths, discovery_errors) = expand_config_paths(inputs);
    paths.sort();
    paths.dedup();
    let mut report = ConfigCheckReport::default();
    for summary in discovery_errors {
        if !report.try_push(summary)? {
            report.push_budget_error(None)?;
            break;
        }
    }
    if !report.stopped {
        for path in &paths {
            if !report.try_push(inspect_config(path))? {
                report.push_budget_error(Some(path))?;
                break;
            }
        }
    }
    Ok(report)
}

const MAX_CONFIG_CHECK_ENTRIES: usize = 4_096;
const MAX_CONFIG_CHECK_ERRORS: usize = 128;
const MAX_CONFIG_CHECK_DEPTH: usize = 32;
const MAX_CONFIG_CHECK_SUMMARIES: usize = 512;
const MAX_CONFIG_CHECK_OUTPUT_BYTES: usize = 1_048_576;
const MAX_CONFIG_CHECK_TERMINAL_RESERVE_BYTES: usize = 16_384;
const MAX_CONFIG_PATH_BYTES: usize = 1_024;
const MAX_CONFIG_MESSAGE_BYTES: usize = 2_048;
const MAX_CONFIG_DETAIL_BYTES: usize = 8_192;
const MAX_CONFIG_SCHEMA_ISSUES: usize = 64;
const MAX_CONFIG_SCHEMA_ISSUE_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigInspectionOutcome {
    Invalid,
    Unknown,
}

type ConfigInspectionFailure = (&'static str, String, ConfigInspectionOutcome);

#[derive(Debug)]
struct ConfigCheckReport {
    summaries: Vec<Value>,
    json_bytes: usize,
    text_bytes: usize,
    stopped: bool,
}

impl Default for ConfigCheckReport {
    fn default() -> Self {
        Self {
            summaries: Vec::new(),
            // JSON array delimiters plus the trailing newline printed by check_configs.
            json_bytes: 3,
            text_bytes: 0,
            stopped: false,
        }
    }
}

impl ConfigCheckReport {
    fn try_push(&mut self, summary: Value) -> Result<bool> {
        if self.stopped || self.summaries.len() >= MAX_CONFIG_CHECK_SUMMARIES.saturating_sub(1) {
            return Ok(false);
        }
        let json_delta = pretty_json_summary_delta(&summary)?;
        let text_delta = render_config_summary(&summary).len().saturating_add(1);
        let usable_bytes =
            MAX_CONFIG_CHECK_OUTPUT_BYTES.saturating_sub(MAX_CONFIG_CHECK_TERMINAL_RESERVE_BYTES);
        if self.json_bytes.saturating_add(json_delta) > usable_bytes
            || self.text_bytes.saturating_add(text_delta) > usable_bytes
        {
            return Ok(false);
        }
        self.json_bytes += json_delta;
        self.text_bytes += text_delta;
        self.summaries.push(summary);
        Ok(true)
    }

    fn push_budget_error(&mut self, path: Option<&Path>) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        let summary = config_error_summary(
            path.unwrap_or_else(|| Path::new("")),
            "configuration",
            ConfigInspectionOutcome::Unknown,
            "configuration check stopped before inspecting all paths because the summary count or output byte budget was exhausted",
        );
        let json_delta = pretty_json_summary_delta(&summary)?;
        let text_delta = render_config_summary(&summary).len().saturating_add(1);
        if self.summaries.len() >= MAX_CONFIG_CHECK_SUMMARIES
            || self.json_bytes.saturating_add(json_delta) > MAX_CONFIG_CHECK_OUTPUT_BYTES
            || self.text_bytes.saturating_add(text_delta) > MAX_CONFIG_CHECK_OUTPUT_BYTES
        {
            bail!("configuration check could not fit its terminal budget error");
        }
        self.json_bytes += json_delta;
        self.text_bytes += text_delta;
        self.summaries.push(summary);
        self.stopped = true;
        Ok(())
    }
}

fn pretty_json_summary_delta(summary: &Value) -> Result<usize> {
    let serialized = serde_json::to_string_pretty(summary)?;
    // Each line gains two spaces when nested in the report array. The final
    // two bytes account for either the first array newline or a `,\n` separator.
    Ok(serialized
        .len()
        .saturating_add(serialized.lines().count().saturating_mul(2))
        .saturating_add(2))
}

fn render_config_summary(summary: &Value) -> String {
    let classification = summary["classification"].as_str().unwrap_or("unsupported");
    let kind = summary["kind"].as_str().unwrap_or("configuration");
    let path = summary["path"].as_str().unwrap_or_default();
    if summary["status"] == "error" {
        format!(
            "{classification}: {kind} {path}: {}",
            summary["error"].as_str().unwrap_or("unknown error")
        )
    } else if let Some(detail) = summary["detail"].as_str() {
        format!("{classification}: {kind} {path}: {detail}")
    } else {
        format!("{classification}: {kind} {path}")
    }
}

fn expand_config_paths(inputs: &[PathBuf]) -> (Vec<PathBuf>, Vec<Value>) {
    let mut discovery = ConfigDiscovery::default();
    for path in inputs {
        discovery.visit(path, 0, true);
        if discovery.entry_limit_reached {
            break;
        }
    }
    if discovery.paths.is_empty() && discovery.errors.is_empty() {
        discovery.record_error(
            inputs
                .first()
                .map_or_else(|| Path::new("."), PathBuf::as_path),
            "no supported configuration files were discovered",
        );
    }
    discovery.paths.sort();
    discovery.errors.sort_by(|left, right| {
        left["path"]
            .as_str()
            .cmp(&right["path"].as_str())
            .then_with(|| left["error"].as_str().cmp(&right["error"].as_str()))
    });
    (discovery.paths, discovery.errors)
}

#[derive(Debug)]
struct ConfigDiscovery {
    paths: Vec<PathBuf>,
    errors: Vec<Value>,
    visited_entries: usize,
    entry_limit_reached: bool,
    error_limit_reached: bool,
    max_entries: usize,
    max_errors: usize,
}

impl Default for ConfigDiscovery {
    fn default() -> Self {
        Self::with_limits(MAX_CONFIG_CHECK_ENTRIES, MAX_CONFIG_CHECK_ERRORS)
    }
}

#[derive(Debug)]
enum DirectoryCandidate {
    Path(PathBuf),
    Error(String),
}

impl ConfigDiscovery {
    fn with_limits(max_entries: usize, max_errors: usize) -> Self {
        Self {
            paths: Vec::new(),
            errors: Vec::new(),
            visited_entries: 0,
            entry_limit_reached: false,
            error_limit_reached: false,
            max_entries,
            max_errors,
        }
    }
}

impl ConfigDiscovery {
    fn visit(&mut self, path: &Path, depth: usize, explicit: bool) {
        if self.entry_limit_reached {
            return;
        }
        if self.visited_entries >= self.max_entries {
            self.entry_limit_reached = true;
            self.record_error(
                path,
                "configuration discovery exceeded its visited-entry limit",
            );
            return;
        }
        self.visited_entries += 1;

        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            if explicit {
                // Preserve an explicit missing/unreadable input so inspection
                // reports the precise I/O error.
                self.paths.push(path.to_path_buf());
            } else {
                self.record_error(path, "failed to inspect configuration directory entry");
            }
            return;
        };
        if metadata.file_type().is_symlink() {
            if path.is_file() && (explicit || is_config_file(path)) {
                self.paths.push(path.to_path_buf());
            } else {
                self.record_error(path, "directory symlinks are not traversed");
            }
            return;
        }
        if metadata.is_file() {
            if explicit || is_config_file(path) {
                self.paths.push(path.to_path_buf());
            }
            return;
        }
        if !metadata.is_dir() {
            self.record_error(path, "path is not a regular file or directory");
            return;
        }
        if depth >= MAX_CONFIG_CHECK_DEPTH {
            self.record_error(
                path,
                "configuration discovery exceeded its directory depth limit",
            );
            return;
        }

        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) => {
                self.record_error(
                    path,
                    &format!("failed to read configuration directory: {error}"),
                );
                return;
            }
        };
        let remaining = self.max_entries.saturating_sub(self.visited_entries);
        let mut candidates = Vec::with_capacity(remaining.min(256));
        for entry in entries {
            if candidates.len() >= remaining {
                self.visited_entries = self.max_entries;
                self.entry_limit_reached = true;
                self.record_error(
                    path,
                    "configuration discovery exceeded its visited-entry limit",
                );
                return;
            }
            candidates.push(match entry {
                Ok(entry) => DirectoryCandidate::Path(entry.path()),
                Err(error) => DirectoryCandidate::Error(error.to_string()),
            });
        }
        candidates.sort_by(|left, right| match (left, right) {
            (DirectoryCandidate::Path(left), DirectoryCandidate::Path(right)) => left.cmp(right),
            (DirectoryCandidate::Path(_), DirectoryCandidate::Error(_)) => std::cmp::Ordering::Less,
            (DirectoryCandidate::Error(_), DirectoryCandidate::Path(_)) => {
                std::cmp::Ordering::Greater
            }
            (DirectoryCandidate::Error(left), DirectoryCandidate::Error(right)) => left.cmp(right),
        });
        for candidate in candidates {
            match candidate {
                DirectoryCandidate::Path(entry) => self.visit(&entry, depth + 1, false),
                DirectoryCandidate::Error(error) => {
                    self.visited_entries += 1;
                    self.record_error(
                        path,
                        &format!("failed to read configuration directory entry: {error}"),
                    );
                }
            }
        }
    }

    fn record_error(&mut self, path: &Path, error: &str) {
        if self.error_limit_reached {
            return;
        }
        if self.errors.len() < self.max_errors.saturating_sub(1) {
            self.errors.push(discovery_error(path, error));
        } else if self.max_errors > 0 {
            self.error_limit_reached = true;
            self.errors.push(discovery_error(
                path,
                "configuration discovery exceeded its error-report limit",
            ));
        }
    }
}

fn is_config_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            ["yaml", "yml", "json"]
                .iter()
                .any(|candidate| extension.eq_ignore_ascii_case(candidate))
        })
}

fn discovery_error(path: &Path, error: &str) -> Value {
    config_error_summary(
        path,
        "configuration",
        ConfigInspectionOutcome::Unknown,
        error,
    )
}

fn config_error_summary(
    path: &Path,
    kind: &'static str,
    outcome: ConfigInspectionOutcome,
    error: &str,
) -> Value {
    let (parseable, consumed_fields) = match outcome {
        ConfigInspectionOutcome::Invalid => (Value::from(false), "none"),
        ConfigInspectionOutcome::Unknown => (Value::Null, "unknown"),
    };
    json!({
        "path": bounded_path(path),
        "kind": kind,
        "classification": "unsupported",
        "status": "error",
        "parseable": parseable,
        "executable": false,
        "consumed_fields": consumed_fields,
        "runtime": "unavailable",
        "error": bounded_text(error, MAX_CONFIG_MESSAGE_BYTES),
    })
}

fn inspect_config(path: &Path) -> Value {
    match inspect_config_inner(path) {
        Ok(summary) => summary,
        Err((kind, error, outcome)) => config_error_summary(path, kind, outcome, &error),
    }
}

fn bounded_text(value: &str, max_bytes: usize) -> String {
    const SUFFIX: &str = "...[truncated]";
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    if max_bytes <= SUFFIX.len() {
        return SUFFIX[..max_bytes].to_owned();
    }
    let mut end = max_bytes.saturating_sub(SUFFIX.len()).min(value.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut output = String::with_capacity(max_bytes);
    output.push_str(&value[..end]);
    output.push_str(SUFFIX);
    output
}

fn bounded_path(path: &Path) -> String {
    bounded_text(&path.display().to_string(), MAX_CONFIG_PATH_BYTES)
}

fn bounded_issue_detail(prefix: &str, issues: &[String]) -> String {
    bounded_text(
        &format!("{prefix}{}", issues.join(", ")),
        MAX_CONFIG_DETAIL_BYTES,
    )
}

fn mark_summary_error(mut summary: Value, error: &str) -> Value {
    summary["status"] = Value::from("error");
    summary["classification"] = Value::from("unsupported");
    summary["executable"] = Value::from(false);
    summary["runtime"] = Value::from("unavailable");
    summary["error"] = Value::from(bounded_text(error, MAX_CONFIG_MESSAGE_BYTES));
    summary
}

fn invalid_config_error(
    kind: &'static str,
    error: &(impl ToString + ?Sized),
) -> ConfigInspectionFailure {
    (kind, error.to_string(), ConfigInspectionOutcome::Invalid)
}

fn unknown_config_error(
    kind: &'static str,
    error: &(impl ToString + ?Sized),
) -> ConfigInspectionFailure {
    (kind, error.to_string(), ConfigInspectionOutcome::Unknown)
}

fn inspect_config_inner(path: &Path) -> Result<Value, ConfigInspectionFailure> {
    let body =
        read_bounded_config(path).map_err(|error| unknown_config_error("configuration", &error))?;
    let document: serde_yaml::Value = serde_yaml::from_str(&body).map_err(|error| {
        invalid_config_error("configuration", &format!("invalid YAML: {error}"))
    })?;
    let auxiliary_kind = auxiliary_config_filename_kind(path);
    let mapping = document.as_mapping().ok_or_else(|| {
        invalid_config_error("configuration", "configuration must contain a YAML mapping")
    })?;

    let has = |key: &str| mapping.contains_key(serde_yaml::Value::from(key));
    let summary = if has("grid_system") || has("grid") || is_bare_grid(mapping) {
        inspect_grid_config(path, &body, &document)
    } else if has("volume_maker") {
        let config = load_volume_maker_config_from_str(&body)
            .map_err(|error| invalid_config_error("volume-maker", &error))?;
        let detail = if let Err(error) = config.validate_execution_controls() {
            error.to_string()
        } else if let Err(error) = VolumeMakerStrategy::try_from(&config) {
            error.to_string()
        } else {
            "runtime command is unavailable".to_owned()
        };
        Ok(config_summary(
            path,
            "volume-maker",
            ConfigSupport::ParseOnly,
            Some(&detail),
        ))
    } else if has("price_alert") {
        inspect_price_alert_config(path, &body)
    } else if has("scanner") {
        inspect_scanner_config(path, &body)
    } else if is_arbitrage(mapping) {
        inspect_arbitrage_config(path, &body, &document)
    } else if has("exchanges") && has("symbols") {
        load_monitor_config_from_str(&body)
            .map_err(|error| invalid_config_error("monitor", &error))?;
        let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Monitor, &document);
        if issues.is_empty() {
            Ok(config_summary(
                path,
                "monitor",
                ConfigSupport::PaperCompanion,
                Some("arbitrage paper companion; standalone monitor runtime unavailable"),
            ))
        } else {
            let detail = bounded_issue_detail(
                "paper one-shot rejects ignored or unknown companion keys: ",
                &issues,
            );
            Ok(config_summary(
                path,
                "monitor",
                ConfigSupport::LegacyPartial,
                Some(&detail),
            ))
        }
    } else if has("symbol_mappings") || has("conversions") {
        load_symbol_conversions_from_str(&body)
            .map_err(|error| invalid_config_error("symbol-conversion", &error))?;
        Ok(config_summary(
            path,
            "symbol-conversion",
            ConfigSupport::AuxiliaryParsed,
            None,
        ))
    } else if let Some(exchange) = exchange_auth_name(mapping) {
        load_exchange_auth_from_str(exchange, &body)
            .map_err(|error| invalid_config_error("exchange-auth", &error))?;
        Ok(config_summary(
            path,
            "exchange-auth",
            ConfigSupport::ParseOnly,
            Some("private live adapters are unavailable"),
        ))
    } else if let Some(kind) = auxiliary_config_kind(path, &document) {
        Ok(config_summary(
            path,
            kind,
            ConfigSupport::AuxiliaryOnly,
            None,
        ))
    } else {
        Err(invalid_config_error(
            "configuration",
            "unsupported configuration schema",
        ))
    }?;

    Ok(reject_auxiliary_filename_mismatch(summary, auxiliary_kind))
}

fn inspect_price_alert_config(path: &Path, body: &str) -> Result<Value, ConfigInspectionFailure> {
    load_price_alert_config_from_str(body)
        .map_err(|error| invalid_config_error("price-alert", &error))?;
    Ok(config_summary(
        path,
        "price-alert",
        ConfigSupport::ParseOnly,
        Some("runtime command is unavailable"),
    ))
}

fn inspect_scanner_config(path: &Path, body: &str) -> Result<Value, ConfigInspectionFailure> {
    load_scanner_config_from_str(body).map_err(|error| invalid_config_error("scanner", &error))?;
    Ok(config_summary(
        path,
        "scanner",
        ConfigSupport::ParseOnly,
        Some("replay-backed serve/status/stop only; real-time discovery runtime unavailable"),
    ))
}

fn inspect_grid_config(
    path: &Path,
    body: &str,
    document: &serde_yaml::Value,
) -> Result<Value, ConfigInspectionFailure> {
    let config =
        load_grid_config_from_str(body).map_err(|error| invalid_config_error("grid", &error))?;
    let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Grid, document);
    if !issues.is_empty() {
        let detail = bounded_issue_detail(
            "paper one-shot rejects ignored or unknown runtime keys: ",
            &issues,
        );
        return Ok(config_summary(
            path,
            "grid",
            ConfigSupport::LegacyPartial,
            Some(&detail),
        ));
    }
    if let Err(error) = GridPlanner::try_from(&config) {
        return Ok(config_summary(
            path,
            "grid",
            ConfigSupport::ParseOnly,
            Some(&error.to_string()),
        ));
    }
    Ok(config_summary(path, "grid", ConfigSupport::PaperOnce, None))
}

fn reject_auxiliary_filename_mismatch(
    summary: Value,
    expected_kind: Option<&'static str>,
) -> Value {
    let Some(expected_kind) = expected_kind else {
        return summary;
    };
    if summary["classification"] == "auxiliary" && summary["kind"] == expected_kind {
        return summary;
    }
    let actual_kind = summary["kind"].as_str().unwrap_or("configuration");
    let error = format!(
        "filename is reserved for {expected_kind} auxiliary configuration, but the content matches {actual_kind}"
    );
    mark_summary_error(summary, &error)
}

fn inspect_arbitrage_config(
    path: &Path,
    body: &str,
    document: &serde_yaml::Value,
) -> Result<Value, ConfigInspectionFailure> {
    let config = load_arbitrage_config_from_str(body)
        .map_err(|error| invalid_config_error("arbitrage", &error))?;
    let enabled_keys = config
        .symbol_configs
        .iter()
        .filter(|(_, profile)| profile.enabled)
        .map(|(key, _)| key.to_string())
        .collect::<Vec<_>>();
    let schema_issues = paper_runtime_schema_issues(PaperRuntimeSchema::Arbitrage, document);
    if !schema_issues.is_empty() {
        let detail = bounded_issue_detail(
            "paper one-shot rejects ignored or unknown runtime keys: ",
            &schema_issues,
        );
        return Ok(config_summary(
            path,
            "arbitrage",
            ConfigSupport::LegacyPartial,
            Some(&detail),
        ));
    }
    if let Err(error) = config.validate_execution_controls() {
        return Ok(config_summary(
            path,
            "arbitrage",
            ConfigSupport::ParseOnly,
            Some(&error.to_string()),
        ));
    }
    if enabled_keys.is_empty() {
        return Ok(config_summary(
            path,
            "arbitrage",
            ConfigSupport::ParseOnly,
            Some("no enabled symbol_configs strategy key"),
        ));
    }

    let mut missing_position_limit_keys = Vec::new();
    for (key, profile) in &config.symbol_configs {
        if profile.enabled {
            let effective = match config.resolve_for_strategy(key) {
                Ok(effective) => effective,
                Err(error) => {
                    return Ok(config_summary(
                        path,
                        "arbitrage",
                        ConfigSupport::ParseOnly,
                        Some(&error.to_string()),
                    ));
                }
            };
            if effective.max_position_value.is_none() {
                missing_position_limit_keys.push(key.to_string());
            }
        }
    }
    if !missing_position_limit_keys.is_empty() {
        let detail = format!(
            "enabled strategy keys require max_position_value: {}",
            missing_position_limit_keys.join(", ")
        );
        return Ok(config_summary(
            path,
            "arbitrage",
            ConfigSupport::ParseOnly,
            Some(&detail),
        ));
    }

    let detail = format!(
        "requires a strict monitor companion and explicit strategy key; enabled keys: {}",
        enabled_keys.join(", ")
    );
    Ok(config_summary(
        path,
        "arbitrage",
        ConfigSupport::PaperOnce,
        Some(&detail),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigSupport {
    PaperOnce,
    PaperCompanion,
    LegacyPartial,
    ParseOnly,
    AuxiliaryParsed,
    AuxiliaryOnly,
}

impl ConfigSupport {
    const fn classification(self) -> &'static str {
        match self {
            Self::PaperOnce => "runtime-executable",
            Self::LegacyPartial | Self::ParseOnly => "legacy-parseable",
            Self::PaperCompanion | Self::AuxiliaryParsed | Self::AuxiliaryOnly => "auxiliary",
        }
    }

    const fn executable(self) -> bool {
        matches!(self, Self::PaperOnce)
    }

    const fn consumed_fields(self) -> &'static str {
        match self {
            Self::PaperOnce | Self::PaperCompanion => "strict",
            Self::LegacyPartial => "partial",
            Self::ParseOnly | Self::AuxiliaryParsed => "parse-only",
            Self::AuxiliaryOnly => "auxiliary-only",
        }
    }

    const fn runtime(self) -> &'static str {
        match self {
            Self::PaperOnce => "paper-once",
            Self::PaperCompanion => "paper-companion",
            Self::LegacyPartial | Self::ParseOnly => "unavailable",
            Self::AuxiliaryParsed | Self::AuxiliaryOnly => "not-wired",
        }
    }
}

fn config_summary(
    path: &Path,
    kind: &'static str,
    support: ConfigSupport,
    detail: Option<&str>,
) -> Value {
    json!({
        "path": bounded_path(path),
        "kind": kind,
        "classification": support.classification(),
        "status": "ok",
        "parseable": true,
        "executable": support.executable(),
        "consumed_fields": support.consumed_fields(),
        "runtime": support.runtime(),
        "detail": detail.map(|value| bounded_text(value, MAX_CONFIG_DETAIL_BYTES)),
    })
}

fn auxiliary_config_filename_kind(path: &Path) -> Option<&'static str> {
    let file_name = path.file_name()?.to_str()?.to_ascii_lowercase();
    if file_name == "logging.yaml" || file_name.contains("logging") {
        return Some("logging");
    }
    if file_name == "extra_symbols.yaml" {
        return Some("extra-symbols");
    }
    if file_name == "multi_leg_pairs.yaml" {
        return Some("multi-leg-pairs");
    }
    if file_name == "segment_symbol_filters.yaml" {
        return Some("segment-symbol-filters");
    }
    if file_name.ends_with("_markets.json") {
        return Some("market-metadata");
    }
    None
}

fn auxiliary_config_kind(path: &Path, document: &serde_yaml::Value) -> Option<&'static str> {
    let kind = auxiliary_config_filename_kind(path)?;
    let mapping = document.as_mapping()?;
    let value = |key: &str| mapping.get(serde_yaml::Value::from(key));
    let valid = match kind {
        "logging" => {
            value("handlers").is_some_and(serde_yaml::Value::is_mapping)
                || value("logging").is_some_and(serde_yaml::Value::is_mapping)
        }
        "extra-symbols" => value("extra_symbols").is_some_and(serde_yaml::Value::is_sequence),
        "multi-leg-pairs" => value("pairs").is_some_and(serde_yaml::Value::is_sequence),
        "segment-symbol-filters" => {
            [
                "enabled_symbols",
                "disabled_symbols",
                "enabled_exchanges",
                "disabled_exchanges",
            ]
            .iter()
            .any(|key| value(key).is_some_and(serde_yaml::Value::is_sequence))
                || value("allow_single_exchange").is_some_and(serde_yaml::Value::is_bool)
        }
        "market-metadata" => ["markets", "overlapping_markets"]
            .iter()
            .any(|key| value(key).is_some_and(is_yaml_collection)),
        _ => false,
    };
    if valid {
        return Some(kind);
    }
    None
}

fn is_yaml_collection(value: &serde_yaml::Value) -> bool {
    value.is_sequence() || value.is_mapping()
}

#[derive(Debug, Clone, Copy)]
enum PaperRuntimeSchema {
    Grid,
    Arbitrage,
    Monitor,
}

impl PaperRuntimeSchema {
    const fn label(self) -> &'static str {
        match self {
            Self::Grid => "grid",
            Self::Arbitrage => "arbitrage",
            Self::Monitor => "arbitrage monitor companion",
        }
    }
}

fn validated_paper_runtime_body(path: &Path, schema: PaperRuntimeSchema) -> Result<String> {
    let body = read_bounded_config(path).map_err(anyhow::Error::msg)?;
    let path_text = bounded_path(path);
    let document: serde_yaml::Value = serde_yaml::from_str(&body).map_err(|error| {
        anyhow::Error::msg(bounded_text(
            &format!("invalid YAML in {path_text}: {error}"),
            MAX_CONFIG_MESSAGE_BYTES,
        ))
    })?;
    let issues = paper_runtime_schema_issues(schema, &document);
    if !issues.is_empty() {
        let detail = bounded_issue_detail("", &issues);
        let error = format!(
            "{} paper one-shot rejects ignored or unknown runtime config keys in {path_text}: {detail}; run `crypto-trading config-check {path_text}` for classification",
            schema.label(),
        );
        return Err(anyhow::Error::msg(bounded_text(
            &error,
            MAX_CONFIG_MESSAGE_BYTES,
        )));
    }
    Ok(body)
}

fn paper_runtime_schema_issues(
    schema: PaperRuntimeSchema,
    document: &serde_yaml::Value,
) -> Vec<String> {
    let mut issues = SchemaIssues::default();
    match schema {
        PaperRuntimeSchema::Grid => grid_schema_issues(document, &mut issues),
        PaperRuntimeSchema::Arbitrage => arbitrage_schema_issues(document, &mut issues),
        PaperRuntimeSchema::Monitor => monitor_schema_issues(document, &mut issues),
    }
    issues.into_values()
}

#[derive(Debug, Default)]
struct SchemaIssues {
    values: Vec<String>,
    truncated: bool,
}

impl SchemaIssues {
    fn push(&mut self, issue: impl Into<String>) {
        let issue = bounded_text(&issue.into(), MAX_CONFIG_SCHEMA_ISSUE_BYTES);
        if self.values.iter().any(|existing| existing == &issue) {
            return;
        }
        if self.values.len() < MAX_CONFIG_SCHEMA_ISSUES.saturating_sub(1) {
            self.values.push(issue);
        } else {
            self.truncated = true;
        }
    }

    fn into_values(mut self) -> Vec<String> {
        self.values.sort();
        if self.truncated {
            self.values
                .push("... additional schema issues omitted".to_owned());
        }
        self.values
    }
}

fn grid_schema_issues(document: &serde_yaml::Value, issues: &mut SchemaIssues) {
    const KEYS: &[&str] = &[
        "exchange",
        "symbol",
        "market_type",
        "mode",
        "grid_interval",
        "order_amount",
        "lower_price",
        "upper_price",
        "follow_grid_count",
        "price_offset_grids",
        "martingale_increment",
    ];

    let Some(root) = document.as_mapping() else {
        issues.push("<root: expected mapping>".to_owned());
        return;
    };
    if let Some(content) = root.get(serde_yaml::Value::from("grid_system")) {
        unknown_keys(root, &["grid_system"], "", issues);
        mapping_with_keys(content, KEYS, "grid_system", issues);
    } else if let Some(content) = root.get(serde_yaml::Value::from("grid")) {
        issues.push("grid (legacy wrapper; use grid_system)".to_owned());
        unknown_keys(root, &["grid"], "", issues);
        mapping_with_keys(content, KEYS, "grid", issues);
    } else {
        issues.push("<root> (legacy bare grid schema; use grid_system)".to_owned());
        unknown_keys(root, KEYS, "", issues);
    }
}

fn arbitrage_schema_issues(document: &serde_yaml::Value, issues: &mut SchemaIssues) {
    const TOP: &[&str] = &[
        "mode",
        "enabled",
        "system_mode",
        "exchanges",
        "symbols",
        "min_spread_pct",
        "base_quantity",
        "grid_step",
        "max_segments",
        "first_close_ratio",
        "max_position_value",
        "default_config",
        "symbol_configs",
    ];
    const DEFAULT_GRID: &[&str] = &[
        "initial_spread_threshold",
        "grid_step",
        "max_segments",
        "first_close_ratio",
    ];
    const SYMBOL_GRID: &[&str] = &["initial_spread_threshold", "grid_step", "max_segments"];
    const QUANTITY: &[&str] = &["base_quantity"];
    const RISK: &[&str] = &["max_position_value"];

    let Some(root) = document.as_mapping() else {
        issues.push("<root: expected mapping>".to_owned());
        return;
    };
    unknown_keys(root, TOP, "", issues);
    if !root.contains_key(serde_yaml::Value::from("enabled")) {
        issues.push("enabled (required explicit paper execution control)".to_owned());
    }
    require_non_empty_sequence(root, "exchanges", issues);
    require_non_empty_sequence(root, "symbols", issues);
    match root
        .get(serde_yaml::Value::from("mode"))
        .and_then(serde_yaml::Value::as_str)
    {
        Some("segmented") => {}
        Some(mode) => issues.push(format!(
            "mode={mode} (paper one-shot currently supports only segmented)"
        )),
        None => issues.push("mode (required value: segmented)".to_owned()),
    }
    if let Some(value) = root.get(serde_yaml::Value::from("system_mode")) {
        mapping_with_keys(value, &["monitor_only"], "system_mode", issues);
    }
    if let Some(value) = root.get(serde_yaml::Value::from("default_config")) {
        mapping_with_keys(
            value,
            &["grid_config", "quantity_config", "risk_config"],
            "default_config",
            issues,
        );
        if let Some(mapping) = value.as_mapping() {
            nested_mapping_with_keys(
                mapping,
                "grid_config",
                DEFAULT_GRID,
                "default_config",
                issues,
            );
            nested_mapping_with_keys(
                mapping,
                "quantity_config",
                QUANTITY,
                "default_config",
                issues,
            );
            nested_mapping_with_keys(mapping, "risk_config", RISK, "default_config", issues);
        }
    }
    reject_conflicting_arbitrage_aliases(document, issues);
    if let Some(value) = root.get(serde_yaml::Value::from("symbol_configs")) {
        let Some(symbols) = value.as_mapping() else {
            issues.push("symbol_configs: expected mapping".to_owned());
            return;
        };
        for (symbol, profile) in symbols {
            let symbol = symbol.as_str().unwrap_or("<non-string-key>");
            let prefix = format!("symbol_configs.{symbol}");
            mapping_with_keys(
                profile,
                &["enabled", "grid_config", "quantity_config", "risk_config"],
                &prefix,
                issues,
            );
            if let Some(mapping) = profile.as_mapping() {
                nested_mapping_with_keys(mapping, "grid_config", SYMBOL_GRID, &prefix, issues);
                nested_mapping_with_keys(mapping, "quantity_config", QUANTITY, &prefix, issues);
                nested_mapping_with_keys(mapping, "risk_config", RISK, &prefix, issues);
            }
        }
    }
}

fn reject_conflicting_arbitrage_aliases(document: &serde_yaml::Value, issues: &mut SchemaIssues) {
    reject_conflicting_decimal_alias(
        document,
        &["min_spread_pct"],
        &["default_config", "grid_config", "initial_spread_threshold"],
        issues,
    );
    reject_conflicting_decimal_alias(
        document,
        &["base_quantity"],
        &["default_config", "quantity_config", "base_quantity"],
        issues,
    );
    reject_conflicting_decimal_alias(
        document,
        &["grid_step"],
        &["default_config", "grid_config", "grid_step"],
        issues,
    );
    reject_conflicting_u32_alias(
        document,
        &["max_segments"],
        &["default_config", "grid_config", "max_segments"],
        issues,
    );
    reject_conflicting_decimal_alias(
        document,
        &["first_close_ratio"],
        &["default_config", "grid_config", "first_close_ratio"],
        issues,
    );
    reject_conflicting_decimal_alias(
        document,
        &["max_position_value"],
        &["default_config", "risk_config", "max_position_value"],
        issues,
    );
}

fn reject_conflicting_decimal_alias(
    document: &serde_yaml::Value,
    flat_path: &[&str],
    nested_path: &[&str],
    issues: &mut SchemaIssues,
) {
    reject_conflicting_alias(
        document,
        flat_path,
        nested_path,
        "decimal",
        schema_decimal,
        issues,
    );
}

fn reject_conflicting_u32_alias(
    document: &serde_yaml::Value,
    flat_path: &[&str],
    nested_path: &[&str],
    issues: &mut SchemaIssues,
) {
    reject_conflicting_alias(
        document,
        flat_path,
        nested_path,
        "unsigned integer",
        schema_u32,
        issues,
    );
}

fn reject_conflicting_alias<T: PartialEq>(
    document: &serde_yaml::Value,
    flat_path: &[&str],
    nested_path: &[&str],
    value_kind: &str,
    parse: impl Fn(&serde_yaml::Value) -> Option<T>,
    issues: &mut SchemaIssues,
) {
    let Some(flat) = schema_value_at(document, flat_path).filter(|value| !value.is_null()) else {
        return;
    };
    let Some(nested) = schema_value_at(document, nested_path).filter(|value| !value.is_null())
    else {
        return;
    };
    let flat_label = flat_path.join(".");
    let nested_label = nested_path.join(".");
    match (parse(flat), parse(nested)) {
        (Some(flat), Some(nested)) if flat == nested => {}
        (Some(_), Some(_)) => issues.push(format!(
            "{flat_label} conflicts with {nested_label} (strict aliases must be equal)"
        )),
        _ => issues.push(format!(
            "{flat_label} and {nested_label} must both be valid {value_kind} values when both strict aliases are present"
        )),
    }
}

fn schema_value_at<'a>(
    document: &'a serde_yaml::Value,
    path: &[&str],
) -> Option<&'a serde_yaml::Value> {
    path.iter().try_fold(document, |current, key| {
        current.as_mapping()?.get(serde_yaml::Value::from(*key))
    })
}

fn schema_decimal(value: &serde_yaml::Value) -> Option<Decimal> {
    let text = match value {
        serde_yaml::Value::String(value) => value.clone(),
        serde_yaml::Value::Number(value) => value.to_string(),
        _ => return None,
    };
    text.parse().ok()
}

fn schema_u32(value: &serde_yaml::Value) -> Option<u32> {
    value.as_u64()?.try_into().ok()
}

fn require_non_empty_sequence(mapping: &serde_yaml::Mapping, key: &str, issues: &mut SchemaIssues) {
    match mapping
        .get(serde_yaml::Value::from(key))
        .and_then(serde_yaml::Value::as_sequence)
    {
        Some(values) if !values.is_empty() => {}
        Some(_) => issues.push(format!("{key} (must be a non-empty list)")),
        None => issues.push(format!("{key} (required non-empty list)")),
    }
}

fn monitor_schema_issues(document: &serde_yaml::Value, issues: &mut SchemaIssues) {
    let Some(root) = document.as_mapping() else {
        issues.push("<root: expected mapping>".to_owned());
        return;
    };
    unknown_keys(root, &["exchanges", "symbols", "health_check"], "", issues);
    match root.get(serde_yaml::Value::from("health_check")) {
        Some(value) => {
            mapping_with_keys(
                value,
                &["data_timeout", "max_pair_skew_ms"],
                "health_check",
                issues,
            );
            if value.as_mapping().is_some_and(|mapping| {
                !mapping.contains_key(serde_yaml::Value::from("data_timeout"))
            }) {
                issues.push("health_check.data_timeout (required freshness limit)".to_owned());
            }
        }
        None => issues.push("health_check.data_timeout (required freshness limit)".to_owned()),
    }
}

fn nested_mapping_with_keys(
    parent: &serde_yaml::Mapping,
    key: &str,
    allowed: &[&str],
    parent_prefix: &str,
    issues: &mut SchemaIssues,
) {
    if let Some(value) = parent.get(serde_yaml::Value::from(key)) {
        let prefix = if parent_prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{parent_prefix}.{key}")
        };
        mapping_with_keys(value, allowed, &prefix, issues);
    }
}

fn mapping_with_keys(
    value: &serde_yaml::Value,
    allowed: &[&str],
    prefix: &str,
    issues: &mut SchemaIssues,
) {
    let Some(mapping) = value.as_mapping() else {
        issues.push(format!("{prefix}: expected mapping"));
        return;
    };
    unknown_keys(mapping, allowed, prefix, issues);
}

fn unknown_keys(
    mapping: &serde_yaml::Mapping,
    allowed: &[&str],
    prefix: &str,
    issues: &mut SchemaIssues,
) {
    for key in mapping.keys() {
        let Some(key) = key.as_str() else {
            issues.push(if prefix.is_empty() {
                "<non-string-key>".to_owned()
            } else {
                format!("{prefix}.<non-string-key>")
            });
            continue;
        };
        if !allowed.contains(&key) {
            issues.push(if prefix.is_empty() {
                key.to_owned()
            } else {
                format!("{prefix}.{key}")
            });
        }
    }
}

fn is_bare_grid(mapping: &serde_yaml::Mapping) -> bool {
    let has = |key: &str| mapping.contains_key(serde_yaml::Value::from(key));
    (has("exchange") || has("exchange_name"))
        && (has("symbol") || has("pair") || has("trading_pair"))
        && (has("mode") || has("grid_type") || has("strategy"))
        && (has("grid_interval") || has("grid_spacing") || has("spacing"))
        && (has("order_amount") || has("order_quantity") || has("quantity"))
}

fn is_arbitrage(mapping: &serde_yaml::Mapping) -> bool {
    let has = |key: &str| mapping.contains_key(serde_yaml::Value::from(key));
    has("system_mode")
        || has("default_config")
        || has("symbol_configs")
        || has("arbitrage_decision")
        || has("arbitrage_execution")
        || (has("mode")
            && has("symbols")
            && (has("min_spread_pct") || has("center_exchange") || has("counter_exchanges")))
}

fn exchange_auth_name(mapping: &serde_yaml::Mapping) -> Option<&str> {
    if let Some(exchange) = exchange_identity(mapping) {
        return Some(exchange);
    }
    if has_auth_schema(mapping) {
        return mapping
            .get(serde_yaml::Value::from("exchange"))
            .and_then(serde_yaml::Value::as_str);
    }

    mapping.iter().find_map(|(key, value)| {
        let root_name = key.as_str()?;
        let exchange_config = value.as_mapping()?;
        exchange_identity(exchange_config)
            .or_else(|| has_auth_schema(exchange_config).then_some(root_name))
    })
}

fn exchange_identity(mapping: &serde_yaml::Mapping) -> Option<&str> {
    ["exchange_id", "exchange_name"]
        .into_iter()
        .find_map(|key| mapping.get(serde_yaml::Value::from(key))?.as_str())
}

fn has_auth_schema(mapping: &serde_yaml::Mapping) -> bool {
    has_auth_fields(mapping)
        || ["authentication", "auth", "extra_params"]
            .into_iter()
            .any(|key| nested_mapping(mapping, key).is_some_and(has_auth_fields))
        || nested_mapping(mapping, "api_config")
            .and_then(|api_config| nested_mapping(api_config, "auth"))
            .is_some_and(has_auth_fields)
}

fn nested_mapping<'a>(
    mapping: &'a serde_yaml::Mapping,
    key: &str,
) -> Option<&'a serde_yaml::Mapping> {
    mapping
        .get(serde_yaml::Value::from(key))
        .and_then(serde_yaml::Value::as_mapping)
}

fn has_auth_fields(mapping: &serde_yaml::Mapping) -> bool {
    const AUTH_FIELDS: [&str; 13] = [
        "api_key",
        "api_secret",
        "api_passphrase",
        "private_key",
        "jwt_token",
        "api_key_private_key",
        "stark_private_key",
        "wallet_address",
        "sub_account_id",
        "l2_address",
        "account_id",
        "account_index",
        "api_key_index",
    ];
    AUTH_FIELDS
        .into_iter()
        .any(|key| mapping.contains_key(serde_yaml::Value::from(key)))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use chrono::{TimeZone, Utc};
    use crypto_trading_config::load_grid_config_from_str;
    use crypto_trading_domain::{
        MarketType, Money, Order, OrderIntent, OrderStatus, OrderType, Position, PositionSide,
        Price, Quantity, Side, Symbol, TimeInForce,
    };
    use crypto_trading_exchange::{
        ExchangeError, ForeignOrder, ReconcileReceipt, ReconcileScope, SubmissionDisposition,
        TradingReceipt,
    };
    use crypto_trading_runtime::{
        ExecutionBatch, JsonlHistory, ReconciliationObservation, RuntimeError,
    };
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::{
        ConfigCheckReport, ConfigDiscovery, ExecutionOutcomeJournalError, MAX_CONFIG_CHECK_ENTRIES,
        MAX_CONFIG_CHECK_ERRORS, MAX_CONFIG_CHECK_OUTPUT_BYTES, MAX_CONFIG_CHECK_SUMMARIES,
        MAX_CONFIG_DETAIL_BYTES, MAX_CONFIG_SCHEMA_ISSUE_BYTES, MAX_CONFIG_SCHEMA_ISSUES,
        MAX_RECEIPT_SUMMARY_RECEIPTS, MAX_RECONCILIATION_SUMMARY_ORDERS,
        MAX_RECONCILIATION_SUMMARY_POSITIONS, PaperRuntimeSchema, PreservedExecutionOutcome,
        append_execution_planned, auxiliary_config_kind, bounded_issue_detail,
        collect_config_report, config_summary, execution_batch, execution_error_summary,
        finish_arbitrage_execution, finish_execution, inspect_config, paper_runtime_schema_issues,
        plan_grid_intents, receipt_summary, render_config_summary,
        start_after_shutdown_registration,
    };
    use crypto_trading_config::reject_yaml_anchors_and_aliases;

    fn temp_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "crypto-trading-command-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn task_host_signal_registration_precedes_task_start() {
        let steps = Arc::new(Mutex::new(Vec::new()));
        let registration_steps = Arc::clone(&steps);
        let start_steps = Arc::clone(&steps);

        let (shutdown, started) = start_after_shutdown_registration(
            move || {
                registration_steps.lock().unwrap().push("registered");
                Ok::<_, anyhow::Error>(Box::pin(async {
                    Ok(crate::shutdown::ShutdownSignal::CtrlC)
                }) as crate::shutdown::ShutdownSignalFuture)
            },
            move || async move {
                start_steps.lock().unwrap().push("started");
                Ok::<_, anyhow::Error>(true)
            },
        )
        .await
        .unwrap();

        assert!(started);
        assert_eq!(*steps.lock().unwrap(), ["registered", "started"]);
        drop(shutdown);
    }

    fn test_intent(exchange: &str) -> OrderIntent {
        OrderIntent::market(
            exchange,
            Symbol::new("BTC-USDC-PERP").unwrap(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(Decimal::ONE).unwrap(),
        )
    }

    fn test_order(index: usize) -> Order {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        Order {
            id: format!("order-{index}"),
            intent: test_intent("paper"),
            filled_quantity: Quantity::default(),
            average_fill_price: None,
            status: OrderStatus::Open,
            created_at: now,
            updated_at: now,
        }
    }

    fn test_foreign_order(index: usize) -> ForeignOrder {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        ForeignOrder {
            id: format!("foreign-order-{index}"),
            client_order_id: Some(format!("manual-{index}")),
            exchange: "paper".to_owned(),
            symbol: Symbol::new("BTC-USDC-PERP").unwrap(),
            market_type: MarketType::Perpetual,
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity: Quantity::new(Decimal::ONE).unwrap(),
            price: Some(Price::new(Decimal::new(50_000, 0)).unwrap()),
            reduce_only: false,
            time_in_force: TimeInForce::Gtc,
            filled_quantity: Quantity::default(),
            average_fill_price: None,
            status: OrderStatus::Open,
            created_at: now,
            updated_at: now,
        }
    }

    fn test_receipt() -> TradingReceipt {
        TradingReceipt::Submitted {
            order: test_order(0),
            disposition: SubmissionDisposition::Open,
        }
    }

    #[test]
    fn grid_plan_preserves_martingale_quantity_increments() {
        let config = load_grid_config_from_str(
            r"
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: martingale
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 140
  martingale_increment: 0.5
",
        )
        .unwrap();

        let (_, intents) = plan_grid_intents(&config, Decimal::from(120)).unwrap();
        let quantities = intents
            .into_iter()
            .map(|intent| intent.quantity.as_decimal())
            .collect::<Vec<_>>();

        assert_eq!(
            quantities,
            vec![
                Decimal::new(25, 1),
                Decimal::new(20, 1),
                Decimal::new(15, 1),
                Decimal::new(10, 1),
            ]
        );
    }

    #[test]
    fn partial_execution_summary_preserves_batch_and_recovery_context() {
        let failed_intent = OrderIntent::market(
            "paper-left",
            Symbol::new("BTC-USDC-PERP").unwrap(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(Decimal::ONE).unwrap(),
        );
        let unattempted = OrderIntent::market(
            "paper-right",
            Symbol::new("BTC-USDC-PERP").unwrap(),
            MarketType::Perpetual,
            Side::Sell,
            Quantity::new(Decimal::ONE).unwrap(),
        );
        let batch_id = failed_intent.client_order_id;
        let error = RuntimeError::PartialExecution {
            batch_id,
            failed_index: 0,
            completed: Vec::new(),
            failed_intent: Box::new(failed_intent),
            unattempted: vec![unattempted],
            reconciliation: Vec::new(),
            source: Box::new(ExchangeError::rejected("injected partial outcome").into()),
        };

        let (phase, details) = execution_error_summary(&error, &batch_id.to_string());

        assert_eq!(phase, "execution_partial");
        assert_eq!(details["batch_id"], batch_id.to_string());
        assert_eq!(details["failed_index"], 0);
        assert_eq!(details["unattempted"].as_array().unwrap().len(), 1);
        assert!(details["source"].as_str().unwrap().contains("injected"));
    }

    #[test]
    fn partial_execution_summary_persists_bounded_authoritative_reconciliation() {
        let failed_intent = test_intent("paper");
        let batch_id = failed_intent.client_order_id;
        let observed_at = Utc.with_ymd_and_hms(2026, 7, 14, 1, 2, 3).unwrap();
        let orders = (0..=MAX_RECONCILIATION_SUMMARY_ORDERS)
            .map(test_order)
            .collect::<Vec<_>>();
        let foreign_orders = vec![test_foreign_order(0)];
        let positions = (0..=MAX_RECONCILIATION_SUMMARY_POSITIONS)
            .map(|index| Position {
                exchange: "paper".to_owned(),
                symbol: Symbol::new(format!("ASSET-{index}")).unwrap(),
                market_type: MarketType::Perpetual,
                side: PositionSide::Long,
                quantity: Quantity::new(Decimal::ONE).unwrap(),
                entry_price: None,
                mark_price: None,
                unrealized_pnl: Money::default(),
                updated_at: observed_at,
            })
            .collect::<Vec<_>>();
        let error = RuntimeError::PartialExecution {
            batch_id,
            failed_index: 0,
            completed: Vec::new(),
            failed_intent: Box::new(failed_intent),
            unattempted: Vec::new(),
            reconciliation: vec![ReconciliationObservation {
                exchange: "paper".to_owned(),
                result: Ok(ReconcileReceipt {
                    scope: ReconcileScope::All,
                    orders,
                    foreign_orders,
                    positions,
                    observed_at,
                }),
            }],
            source: Box::new(ExchangeError::rejected("injected partial outcome").into()),
        };

        let (_, details) = execution_error_summary(&error, &batch_id.to_string());
        let observation = &details["reconciliation"][0];

        assert_eq!(observation["scope"]["type"], "all");
        assert_eq!(observation["observed_at"], json!(observed_at));
        assert_eq!(
            observation["orders_total"].as_u64(),
            Some(u64::try_from(MAX_RECONCILIATION_SUMMARY_ORDERS + 1).unwrap())
        );
        assert_eq!(
            observation["orders"].as_array().unwrap().len(),
            MAX_RECONCILIATION_SUMMARY_ORDERS
        );
        assert_eq!(observation["orders_truncated"], true);
        assert_eq!(observation["foreign_orders_total"].as_u64(), Some(1));
        assert_eq!(
            observation["foreign_orders"][0]["client_order_id"],
            json!("manual-0")
        );
        assert_eq!(observation["foreign_orders_truncated"], false);
        assert_eq!(
            observation["positions_total"].as_u64(),
            Some(u64::try_from(MAX_RECONCILIATION_SUMMARY_POSITIONS + 1).unwrap())
        );
        assert_eq!(
            observation["positions"].as_array().unwrap().len(),
            MAX_RECONCILIATION_SUMMARY_POSITIONS
        );
        assert_eq!(observation["positions_truncated"], true);
    }

    #[tokio::test]
    async fn outcome_journal_failure_preserves_successful_receipts() {
        let path = temp_path("success-journal");
        let history = JsonlHistory::new(&path);
        let batch = execution_batch(vec![test_intent("paper")]).unwrap();
        let batch_id = batch.id().to_string();
        append_execution_planned(&history, "grid", "BTC", &batch, json!({}))
            .await
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        let error = finish_execution(&history, "grid", "BTC", &batch_id, Ok(vec![test_receipt()]))
            .await
            .unwrap_err();
        let composite = error
            .downcast_ref::<ExecutionOutcomeJournalError>()
            .unwrap();

        assert!(matches!(
            &composite.outcome,
            PreservedExecutionOutcome::Completed(receipts) if receipts.len() == 1
        ));
        std::fs::remove_dir(&path).unwrap();
    }

    #[tokio::test]
    async fn outcome_journal_failure_preserves_partial_execution_error() {
        let path = temp_path("partial-journal");
        let history = JsonlHistory::new(&path);
        let batch = ExecutionBatch::planned(vec![test_intent("paper")]).unwrap();
        let batch_id = batch.id().to_string();
        append_execution_planned(&history, "grid", "BTC", &batch, json!({}))
            .await
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let failed_intent = test_intent("paper");
        let runtime_error = RuntimeError::PartialExecution {
            batch_id: batch.id(),
            failed_index: 0,
            completed: vec![test_receipt()],
            failed_intent: Box::new(failed_intent),
            unattempted: Vec::new(),
            reconciliation: Vec::new(),
            source: Box::new(ExchangeError::rejected("injected partial outcome").into()),
        };

        let error = finish_execution(&history, "grid", "BTC", &batch_id, Err(runtime_error))
            .await
            .unwrap_err();
        let composite = error
            .downcast_ref::<ExecutionOutcomeJournalError>()
            .unwrap();

        assert!(matches!(
            &composite.outcome,
            PreservedExecutionOutcome::Failed(RuntimeError::PartialExecution { completed, .. })
                if completed.len() == 1
        ));
        std::fs::remove_dir(&path).unwrap();
    }

    #[tokio::test]
    async fn outcome_journal_failure_preserves_incomplete_receipts() {
        let path = temp_path("incomplete-journal");
        let history = JsonlHistory::new(&path);
        let batch = ExecutionBatch::planned(vec![test_intent("paper")]).unwrap();
        let batch_id = batch.id().to_string();
        append_execution_planned(&history, "arbitrage", "BTC", &batch, json!({}))
            .await
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        let error =
            finish_arbitrage_execution(&history, "BTC", &batch_id, 2, Ok(vec![test_receipt()]))
                .await
                .unwrap_err();
        let composite = error
            .downcast_ref::<ExecutionOutcomeJournalError>()
            .unwrap();

        assert!(matches!(
            &composite.outcome,
            PreservedExecutionOutcome::Incomplete(receipts) if receipts.len() == 1
        ));
        std::fs::remove_dir(&path).unwrap();
    }

    #[test]
    fn receipt_summary_persists_a_bounded_recovery_sample() {
        let receipts = (0..=MAX_RECEIPT_SUMMARY_RECEIPTS)
            .map(|_| test_receipt())
            .collect::<Vec<_>>();

        let summary = receipt_summary(&receipts);

        assert_eq!(
            summary["receipt_count"].as_u64(),
            Some(u64::try_from(MAX_RECEIPT_SUMMARY_RECEIPTS + 1).unwrap())
        );
        assert_eq!(
            summary["receipts"].as_array().unwrap().len(),
            MAX_RECEIPT_SUMMARY_RECEIPTS
        );
        assert_eq!(summary["receipts_truncated"], true);
        assert_eq!(summary["receipts"][0]["type"], "submitted");
    }

    #[test]
    fn discovery_limits_errors_and_counts_irrelevant_entries() {
        let root = temp_path("discovery");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("README.md"), "not config").unwrap();
        let mut discovery = ConfigDiscovery::default();
        discovery.visit(&root, 0, true);
        assert_eq!(discovery.visited_entries, 2);
        assert!(discovery.paths.is_empty());

        for index in 0..(MAX_CONFIG_CHECK_ERRORS + 10) {
            discovery.record_error(&root, &format!("error-{index}"));
        }
        assert_eq!(discovery.errors.len(), MAX_CONFIG_CHECK_ERRORS);
        assert_eq!(
            discovery.errors.last().unwrap()["error"],
            "configuration discovery exceeded its error-report limit"
        );

        discovery.visited_entries = MAX_CONFIG_CHECK_ENTRIES;
        discovery.visit(&root, 0, true);
        assert!(discovery.entry_limit_reached);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discovery_sorts_within_its_bound_and_fails_deterministically_on_overflow() {
        let sorted_root = temp_path("discovery-sorted");
        std::fs::create_dir(&sorted_root).unwrap();
        for name in ["z.yaml", "README.md", "a.yaml"] {
            std::fs::write(sorted_root.join(name), "unknown: true\n").unwrap();
        }
        let mut discovery = ConfigDiscovery::with_limits(4, 8);
        discovery.visit(&sorted_root, 0, true);
        assert_eq!(
            discovery
                .paths
                .iter()
                .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            vec!["a.yaml", "z.yaml"]
        );

        let overflow_root = temp_path("discovery-overflow");
        std::fs::create_dir(&overflow_root).unwrap();
        for name in ["z.yaml", "a.yaml", "m.yaml"] {
            std::fs::write(overflow_root.join(name), "unknown: true\n").unwrap();
        }
        let mut overflow = ConfigDiscovery::with_limits(3, 8);
        overflow.visit(&overflow_root, 0, true);
        assert!(overflow.entry_limit_reached);
        assert!(overflow.paths.is_empty());
        assert_eq!(overflow.errors.len(), 1);
        assert_eq!(
            overflow.errors[0]["path"],
            overflow_root.display().to_string()
        );
        assert!(
            overflow.errors[0]["error"]
                .as_str()
                .unwrap()
                .contains("visited-entry limit")
        );

        std::fs::remove_dir_all(sorted_root).unwrap();
        std::fs::remove_dir_all(overflow_root).unwrap();
    }

    #[test]
    fn config_inspection_rejects_a_file_over_the_byte_limit() {
        let path = temp_path("oversized.yaml");
        std::fs::write(&path, vec![b' '; 1_048_577]).unwrap();

        let summary = inspect_config(&path);

        assert_eq!(summary["status"], "error");
        assert!(summary["error"].as_str().unwrap().contains("maximum"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn auxiliary_filenames_cannot_hide_trading_schemas_or_safety_keys() {
        let cases = [
            (
                "logging.yaml",
                "grid",
                r"
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
",
            ),
            (
                "extra_symbols.yaml",
                "arbitrage",
                r"
mode: segmented
enabled: true
exchanges: [paper-left, paper-right]
symbols: [BTC-USDC-PERP]
min_spread_pct: 0.1
base_quantity: 1
grid_step: 0.03
max_segments: 5
first_close_ratio: 0.4
max_position_value: 5000
",
            ),
            (
                "multi_leg_pairs.yaml",
                "grid",
                r"
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
  take_profit_enabled: true
",
            ),
        ];

        for (file_name, expected_kind, body) in cases {
            let root = temp_path(expected_kind);
            std::fs::create_dir(&root).unwrap();
            let path = root.join(file_name);
            std::fs::write(&path, body).unwrap();

            let summary = inspect_config(&path);

            assert_eq!(summary["kind"], expected_kind, "{summary:?}");
            assert_eq!(summary["status"], "error", "{summary:?}");
            assert!(
                summary["error"]
                    .as_str()
                    .unwrap()
                    .contains("filename is reserved"),
                "{summary:?}"
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn auxiliary_filenames_require_their_minimum_content_shape() {
        let invalid_cases = [
            "logging.yaml",
            "extra_symbols.yaml",
            "multi_leg_pairs.yaml",
            "segment_symbol_filters.yaml",
            "example_markets.json",
        ];
        for file_name in invalid_cases {
            let document: serde_yaml::Value = serde_yaml::from_str("unknown: true\n").unwrap();
            assert_eq!(
                auxiliary_config_kind(std::path::Path::new(file_name), &document),
                None,
                "{file_name}"
            );
        }

        let valid_cases = [
            ("logging.yaml", "handlers: {}\n", "logging"),
            (
                "extra_symbols.yaml",
                "extra_symbols: [BTC-USDC-PERP]\n",
                "extra-symbols",
            ),
            (
                "multi_leg_pairs.yaml",
                "pairs: [BTC-USDC-PERP]\n",
                "multi-leg-pairs",
            ),
            (
                "segment_symbol_filters.yaml",
                "allow_single_exchange: true\n",
                "segment-symbol-filters",
            ),
            (
                "example_markets.json",
                "{\"overlapping_markets\": {\"BTC\": {}}}\n",
                "market-metadata",
            ),
        ];
        for (file_name, body, expected_kind) in valid_cases {
            let document: serde_yaml::Value = serde_yaml::from_str(body).unwrap();
            assert_eq!(
                auxiliary_config_kind(std::path::Path::new(file_name), &document),
                Some(expected_kind),
                "{file_name}"
            );
        }
    }

    #[test]
    fn schema_issue_count_and_detail_bytes_are_hard_bounded() {
        let mut grid = serde_yaml::Mapping::new();
        for index in 0..1_000 {
            grid.insert(
                serde_yaml::Value::from(format!("unknown_{index:04}")),
                serde_yaml::Value::Bool(true),
            );
        }
        grid.insert(
            serde_yaml::Value::from("x".repeat(10_000)),
            serde_yaml::Value::Bool(true),
        );
        let mut root = serde_yaml::Mapping::new();
        root.insert(
            serde_yaml::Value::from("grid_system"),
            serde_yaml::Value::Mapping(grid),
        );

        let issues = paper_runtime_schema_issues(
            PaperRuntimeSchema::Grid,
            &serde_yaml::Value::Mapping(root),
        );
        let detail = bounded_issue_detail("schema issues: ", &issues);

        assert!(issues.len() <= MAX_CONFIG_SCHEMA_ISSUES, "{}", issues.len());
        assert!(
            issues
                .iter()
                .all(|issue| issue.len() <= MAX_CONFIG_SCHEMA_ISSUE_BYTES)
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("additional schema issues omitted"))
        );
        assert!(detail.len() <= MAX_CONFIG_DETAIL_BYTES, "{}", detail.len());
    }

    #[test]
    fn yaml_anchor_guard_ignores_quoted_globs_and_comments() {
        let accepted = r#"
double: "*_PERP"
single: '*SPOT*'
literal: "&not-an-anchor"
url: https://example.invalid/a&b
# * comment bullet
"#;
        assert!(reject_yaml_anchors_and_aliases(accepted).is_ok());

        for rejected in [
            "defaults: &defaults\n",
            "copy: *defaults\n",
            "items: [*defaults]\n",
            "items: [https://example.invalid/#fragment, *defaults]\n",
        ] {
            assert!(
                reject_yaml_anchors_and_aliases(rejected).is_err(),
                "{rejected}"
            );
        }
    }

    #[test]
    fn config_inspection_rejects_yaml_anchors_before_deserialization() {
        let path = temp_path("yaml-anchor.yaml");
        std::fs::write(
            &path,
            "defaults: &defaults\n  enabled: true\ncopy: *defaults\n",
        )
        .unwrap();

        let summary = inspect_config(&path);

        assert_eq!(summary["status"], "error");
        assert!(
            summary["error"]
                .as_str()
                .unwrap()
                .contains("YAML anchor tokens")
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn config_inspection_allows_literal_tokens_inside_block_scalars() {
        let path = temp_path("yaml-block-scalar.yaml");
        std::fs::write(
            &path,
            r"
notes: >2+
  *literal
  &literal

grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
",
        )
        .unwrap();

        let summary = inspect_config(&path);

        assert_eq!(summary["status"], "ok", "{summary:?}");
        assert_eq!(summary["kind"], "grid", "{summary:?}");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn config_report_stops_a_large_file_batch_with_a_terminal_error() {
        let root = temp_path("report-batch");
        std::fs::create_dir(&root).unwrap();
        for index in 0..(MAX_CONFIG_CHECK_SUMMARIES + 8) {
            std::fs::write(root.join(format!("{index:04}.yaml")), "unknown: true\n").unwrap();
        }

        let report = collect_config_report(std::slice::from_ref(&root)).unwrap();

        assert!(report.stopped);
        assert!(report.summaries.len() <= MAX_CONFIG_CHECK_SUMMARIES);
        assert!(
            report.summaries.last().unwrap()["error"]
                .as_str()
                .unwrap()
                .contains("budget was exhausted")
        );
        let json = serde_json::to_string_pretty(&report.summaries).unwrap();
        let text_bytes = report
            .summaries
            .iter()
            .map(|summary| render_config_summary(summary).len() + 1)
            .sum::<usize>();
        assert!(json.len() < MAX_CONFIG_CHECK_OUTPUT_BYTES);
        assert!(text_bytes <= MAX_CONFIG_CHECK_OUTPUT_BYTES);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn config_report_stops_before_oversized_json_or_text_output() {
        let mut report = ConfigCheckReport::default();
        let detail = "x".repeat(MAX_CONFIG_DETAIL_BYTES);
        for _ in 0..MAX_CONFIG_CHECK_SUMMARIES {
            let summary = config_summary(
                std::path::Path::new("large.yaml"),
                "grid",
                super::ConfigSupport::LegacyPartial,
                Some(&detail),
            );
            if !report.try_push(summary).unwrap() {
                report
                    .push_budget_error(Some(std::path::Path::new("large.yaml")))
                    .unwrap();
                break;
            }
        }

        assert!(report.stopped);
        assert!(report.summaries.len() < MAX_CONFIG_CHECK_SUMMARIES);
        assert!(
            serde_json::to_string_pretty(&report.summaries)
                .unwrap()
                .len()
                < MAX_CONFIG_CHECK_OUTPUT_BYTES
        );
        assert!(report.text_bytes <= MAX_CONFIG_CHECK_OUTPUT_BYTES);
    }

    fn strict_arbitrage_document(flat: &str, nested: &str) -> serde_yaml::Value {
        serde_yaml::from_str(&format!(
            r"mode: segmented
enabled: true
exchanges: [paper-left, paper-right]
symbols: [BTC-USDC-PERP]
{flat}
default_config:
{nested}
"
        ))
        .unwrap()
    }

    #[test]
    fn strict_arbitrage_rejects_unconsumed_symbol_first_close_ratio() {
        let document = serde_yaml::from_str(
            r"
mode: segmented
enabled: true
exchanges: [paper-left, paper-right]
symbols: [BTC-USDC-PERP]
symbol_configs:
  BTC-USDC-PERP:
    enabled: true
    grid_config:
      first_close_ratio: 0.99
",
        )
        .unwrap();

        let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Arbitrage, &document);

        assert_eq!(
            issues,
            ["symbol_configs.BTC-USDC-PERP.grid_config.first_close_ratio"]
        );
    }

    #[test]
    fn strict_arbitrage_allows_semantically_equal_flat_and_nested_aliases() {
        let document = strict_arbitrage_document(
            r#"min_spread_pct: "0.10"
base_quantity: 1.0
grid_step: 0.030
max_segments: 5
first_close_ratio: 0.40
max_position_value: 5000.00"#,
            r#"  grid_config:
    initial_spread_threshold: 0.1
    grid_step: "0.03"
    max_segments: 5
    first_close_ratio: 0.4
  quantity_config:
    base_quantity: 1
  risk_config:
    max_position_value: 5000"#,
        );

        let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Arbitrage, &document);

        assert!(issues.is_empty(), "{issues:?}");
    }

    #[test]
    fn strict_arbitrage_rejects_every_conflicting_numeric_alias_family() {
        let cases = [
            (
                "min_spread_pct: 0.1",
                "  grid_config:\n    initial_spread_threshold: 0.2",
                "min_spread_pct",
            ),
            (
                "base_quantity: 1",
                "  quantity_config:\n    base_quantity: 2",
                "base_quantity",
            ),
            (
                "grid_step: 0.03",
                "  grid_config:\n    grid_step: 0.04",
                "grid_step",
            ),
            (
                "max_segments: 5",
                "  grid_config:\n    max_segments: 6",
                "max_segments",
            ),
            (
                "first_close_ratio: 0.4",
                "  grid_config:\n    first_close_ratio: 0.5",
                "first_close_ratio",
            ),
            (
                "max_position_value: 5000",
                "  risk_config:\n    max_position_value: 50000",
                "max_position_value",
            ),
            (
                "max_position_value: 50000",
                "  risk_config:\n    max_position_value: 5000",
                "max_position_value",
            ),
        ];

        for (flat, nested, label) in cases {
            let document = strict_arbitrage_document(flat, nested);
            let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Arbitrage, &document);
            assert!(
                issues
                    .iter()
                    .any(|issue| issue.contains(label) && issue.contains("conflicts")),
                "{flat} / {nested}: {issues:?}"
            );
        }
    }
}
