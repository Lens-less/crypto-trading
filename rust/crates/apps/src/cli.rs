use std::{net::SocketAddr, path::PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};
use rust_decimal::Decimal;
use uuid::Uuid;

/// Rust-first command surface replacing the legacy Python launch scripts.
#[derive(Debug, Parser)]
#[command(name = "crypto-trading", version, about = "多交易所策略自动化系统")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Report the runtime's machine-checkable capability and authority manifest.
    Capabilities(CapabilitiesArgs),
    /// Run bounded Binance testnet connectivity and reconcile smoke checks.
    #[command(name = "testnet-smoke")]
    TestnetSmoke(TestnetSmokeArgs),
    /// Run, inspect, stop, or verify a durable Binance testnet soak campaign.
    #[command(name = "testnet-soak")]
    TestnetSoak(TestnetSoakArgs),
    /// Run or inspect a grid strategy.
    Grid(GridArgs),
    /// Run the segmented arbitrage engine.
    Arbitrage(Box<ArbitrageArgs>),
    /// Run the read-only arbitrage monitor.
    Monitor(MonitorArgs),
    /// Run a maker-volume strategy.
    #[command(name = "volume-maker")]
    VolumeMaker(VolumeMakerArgs),
    /// Run a price alert strategy.
    #[command(name = "price-alert")]
    PriceAlert(PriceAlertArgs),
    /// Rank symbols with the virtual-grid scanner.
    Scanner(ScannerArgs),
    /// Parse and validate existing YAML configuration files.
    #[command(name = "config-check")]
    ConfigCheck(ConfigCheckArgs),
    /// Submit or inspect trusted paper task lifecycle commands.
    #[command(subcommand)]
    Paper(PaperCommand),
}

#[derive(Debug, Args)]
pub struct CapabilitiesArgs {
    /// Emit the stable versioned JSON manifest.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct TestnetSmokeArgs {
    /// Emit a machine-readable summary.
    #[arg(long)]
    pub json: bool,
    /// Call Binance Spot and USD-M testnet `bookTicker` once each.
    #[arg(long)]
    pub call_book_ticker: bool,
    /// Call authenticated Binance testnet reconcile routes with env credentials.
    #[arg(long)]
    pub call_reconcile: bool,
    /// Standard spot symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDC-SPOT")]
    pub spot_symbol: String,
    /// Standard perpetual symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDC-PERP")]
    pub perpetual_symbol: String,
    /// Exact Binance wire symbol used for both spot and perpetual probes.
    #[arg(long, default_value = "BTCUSDT")]
    pub wire_symbol: String,
    /// Total HTTP timeout for each remote call in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub timeout_ms: u64,
}

#[derive(Debug, Args)]
pub struct TestnetSoakArgs {
    /// Task host mode for the durable soak owner.
    #[arg(long, value_enum)]
    pub mode: TestnetSoakMode,
    /// Stable durable task identity.
    #[arg(long)]
    pub task_id: String,
    /// Append read-only soak facts to this JSONL journal.
    #[arg(long)]
    pub history_path: PathBuf,
    /// Spot symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDC-SPOT")]
    pub spot_symbol: String,
    /// Perpetual symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDC-PERP")]
    pub perpetual_symbol: String,
    /// Exact Binance wire symbol used for both spot and perpetual probes.
    #[arg(long, default_value = "BTCUSDT")]
    pub wire_symbol: String,
    /// Bounded probe cadence in milliseconds; required for `serve`.
    #[arg(long)]
    pub interval_ms: Option<u64>,
    /// Per-probe timeout in milliseconds; required for `serve`.
    #[arg(long)]
    pub probe_timeout_ms: Option<u64>,
    /// Consecutive probe failures tolerated before the task fails closed; required for `serve`.
    #[arg(long)]
    pub failure_threshold: Option<u16>,
    /// Minimum successful probes required by 24-hour verification; required for `verify`.
    #[arg(long)]
    pub minimum_successes: Option<u64>,
    /// Loopback control port override. `serve` requires an explicit value.
    #[arg(long)]
    pub control_port: Option<u16>,
    /// Total HTTP timeout for each remote request in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub timeout_ms: u64,
    /// Serve-loop polling interval used for bounded local tests.
    #[arg(long, hide = true, default_value_t = 100)]
    pub control_poll_interval_ms: u64,
    /// Comma-separated fixture probe script for bounded local tests.
    #[arg(long, hide = true)]
    pub fixture_probe_script: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum TestnetSoakMode {
    Serve,
    Status,
    Stop,
    Verify,
}

#[derive(Debug, Args)]
pub struct LiveArgs {
    /// Request a live runtime (unsupported adapters fail closed).
    #[arg(long, requires = "acknowledge_risk")]
    pub live: bool,
    /// Exact acknowledgement required by the runtime for live mode.
    #[arg(long, value_name = "PHRASE")]
    pub acknowledge_risk: Option<String>,
}

#[derive(Debug, Args)]
pub struct GridArgs {
    /// Existing `grid_system` YAML file.
    pub config: PathBuf,
    #[arg(long)]
    pub debug: bool,
    /// Evaluate one paper-mode tick at this exact decimal price.
    #[arg(long, requires = "once")]
    pub price: Option<Decimal>,
    /// Process one snapshot and exit.
    #[arg(long, requires = "price")]
    pub once: bool,
    /// Append paper decisions and receipt summaries to this JSONL file.
    #[arg(long, default_value = "var/history/grid-paper.jsonl")]
    pub history_path: PathBuf,
    #[command(flatten)]
    pub authority: LiveArgs,
}

#[derive(Debug, Args)]
pub struct ArbitrageArgs {
    #[arg(long, default_value = "config/arbitrage/arbitrage_segmented.yaml")]
    pub config: PathBuf,
    #[arg(long, default_value = "config/arbitrage/monitor_v2.yaml")]
    pub monitor_config: PathBuf,
    #[command(flatten)]
    pub diagnostics: DiagnosticArgs,
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,
    #[command(flatten)]
    pub behavior: ArbitrageBehaviorArgs,
    #[command(flatten)]
    pub market: ArbitrageMarketArgs,
    /// Append paper decisions and receipt summaries to this JSONL file.
    #[arg(long, default_value = "var/history/arbitrage-paper.jsonl")]
    pub history_path: PathBuf,
    #[command(flatten)]
    pub authority: LiveArgs,
}

#[derive(Debug, Args)]
pub struct DiagnosticArgs {
    #[arg(long)]
    pub debug: bool,
    #[arg(long)]
    pub debug_detail: bool,
}

#[derive(Debug, Args)]
pub struct ArbitrageBehaviorArgs {
    #[arg(long)]
    pub no_ui: bool,
    #[arg(
        long,
        requires_all = [
            "left_bid",
            "left_ask",
            "left_bid_quantity",
            "left_ask_quantity",
            "right_bid",
            "right_ask",
            "right_bid_quantity",
            "right_ask_quantity"
        ]
    )]
    pub once: bool,
}

#[derive(Debug, Args)]
pub struct ArbitrageMarketArgs {
    /// `symbol_configs` key used when the two legs have different symbols.
    #[arg(long, requires = "once")]
    pub strategy_key: Option<String>,
    /// Left exchange identity; defaults to the monitor config's first exchange.
    #[arg(long, requires = "once")]
    pub left_exchange: Option<String>,
    /// Left standard symbol; defaults to the monitor config's first symbol.
    #[arg(long, requires = "once")]
    pub left_symbol: Option<String>,
    /// Explicit left best bid for the one-shot paper snapshot.
    #[arg(long, requires = "once")]
    pub left_bid: Option<Decimal>,
    /// Explicit left best ask for the one-shot paper snapshot.
    #[arg(long, requires = "once")]
    pub left_ask: Option<Decimal>,
    /// Explicit quantity available at the left best bid.
    #[arg(long, requires = "once", allow_hyphen_values = true)]
    pub left_bid_quantity: Option<Decimal>,
    /// Explicit quantity available at the left best ask.
    #[arg(long, requires = "once", allow_hyphen_values = true)]
    pub left_ask_quantity: Option<Decimal>,
    /// Right exchange identity; defaults to the monitor config's second exchange.
    #[arg(long, requires = "once")]
    pub right_exchange: Option<String>,
    /// Right standard symbol; defaults to the left symbol.
    #[arg(long, requires = "once")]
    pub right_symbol: Option<String>,
    /// Explicit right best bid for the one-shot paper snapshot.
    #[arg(long, requires = "once")]
    pub right_bid: Option<Decimal>,
    /// Explicit right best ask for the one-shot paper snapshot.
    #[arg(long, requires = "once")]
    pub right_ask: Option<Decimal>,
    /// Explicit quantity available at the right best bid.
    #[arg(long, requires = "once", allow_hyphen_values = true)]
    pub right_bid_quantity: Option<Decimal>,
    /// Explicit quantity available at the right best ask.
    #[arg(long, requires = "once", allow_hyphen_values = true)]
    pub right_ask_quantity: Option<Decimal>,
}

#[derive(Debug, Args)]
pub struct MonitorArgs {
    #[arg(long, default_value_t = MonitorMode::Replay, value_enum)]
    pub mode: MonitorMode,
    #[arg(long, default_value = "config/arbitrage/monitor_v2.yaml")]
    pub config: PathBuf,
    /// Finite JSONL replay of validated top-of-book snapshots.
    #[arg(long, value_name = "PATH")]
    pub replay: Option<PathBuf>,
    /// Service task identity for long-running monitor operations.
    #[arg(long)]
    pub task_id: Option<String>,
    /// Append read-only monitor outcomes to this JSONL journal.
    #[arg(long, default_value = "var/history/arbitrage-monitor.jsonl")]
    pub history_path: PathBuf,
    #[arg(long)]
    pub debug: bool,
    #[arg(long)]
    pub debug_detail: bool,
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,
    #[arg(long)]
    pub no_ui: bool,
    /// Local loopback control port override used by monitor serve/status/stop.
    #[arg(long, hide = true)]
    pub control_port: Option<u16>,
    /// Serve-loop status polling interval used for bounded local tests.
    #[arg(long, hide = true, default_value_t = 100)]
    pub control_poll_interval_ms: u64,
    /// Supervisor shutdown grace override for bounded local tests.
    #[arg(long, hide = true)]
    pub shutdown_grace_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, ValueEnum)]
pub enum MonitorMode {
    #[default]
    Replay,
    Serve,
    Status,
    Stop,
}

#[derive(Debug, Args)]
pub struct VolumeMakerArgs {
    #[arg(default_value = "config/volume_maker/backpack_btc_volume_maker.yaml")]
    pub config: PathBuf,
    #[arg(long)]
    pub debug: bool,
}

#[derive(Debug, Args)]
pub struct PriceAlertArgs {
    #[arg(default_value = "config/price_alert/binance_alert.yaml")]
    pub config: PathBuf,
    #[arg(long)]
    pub debug: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum ExchangeChoice {
    #[default]
    Lighter,
    Hyperliquid,
    Backpack,
    Binance,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum LogLevel {
    #[value(name = "DEBUG")]
    Debug,
    #[default]
    #[value(name = "INFO")]
    Info,
    #[value(name = "WARNING")]
    Warning,
    #[value(name = "ERROR")]
    Error,
}

#[derive(Debug, Args)]
pub struct ScannerArgs {
    #[arg(long, value_enum, default_value_t)]
    pub exchange: ExchangeChoice,
    #[arg(long)]
    pub duration: Option<u64>,
    #[arg(long)]
    pub config: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t)]
    pub log_level: LogLevel,
}

#[derive(Debug, Args)]
pub struct ConfigCheckArgs {
    /// One or more legacy YAML/JSON files.
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,
    /// Emit machine-readable JSON summaries.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum PaperCommand {
    /// Control one continuous paper grid task through the trusted submit seam.
    Grid(PaperTaskArgs),
    /// Control one continuous paper arbitrage task through the trusted submit seam.
    Arbitrage(PaperTaskArgs),
}

#[derive(Debug, Args)]
pub struct PaperTaskArgs {
    #[command(subcommand)]
    pub operation: PaperOperation,
}

#[derive(Debug, Subcommand)]
pub enum PaperOperation {
    /// Start one paper task with an explicit trusted-submit command identity.
    Start(PaperStartArgs),
    /// Read one task projection from the server-side read model.
    Status(PaperStatusArgs),
    /// Request a graceful stop through the trusted submit seam.
    Stop(PaperMutationArgs),
    /// Request durable cancellation through the trusted submit seam.
    Cancel(PaperMutationArgs),
}

#[derive(Debug, Args)]
pub struct TrustedControlArgs {
    /// Loopback-only trusted submit socket in `IP:PORT` form.
    #[arg(long, value_name = "IP:PORT")]
    pub control_addr: SocketAddr,
    /// Environment variable name that stores the bearer token.
    #[arg(long, value_name = "ENV_VAR")]
    pub token_env_var: String,
}

#[derive(Debug, Args)]
pub struct PaperMutationArgs {
    #[command(flatten)]
    pub control: TrustedControlArgs,
    /// Principal identity that must exactly match the server's paper-operator identity.
    #[arg(long)]
    pub principal_id: String,
    /// Stable UUID assigned by the caller to this trusted submit command.
    #[arg(long)]
    pub command_id: Uuid,
    /// Stable idempotency key assigned by the caller.
    #[arg(long)]
    pub idempotency_key: String,
    /// Stable durable task identity targeted by the command.
    #[arg(long)]
    pub task_id: String,
}

#[derive(Debug, Args)]
pub struct PaperStartArgs {
    #[command(flatten)]
    pub mutation: PaperMutationArgs,
    /// Stable strategy identity referenced by the task owner.
    #[arg(long)]
    pub strategy_id: String,
    /// Explicit strategy revision that the trusted task must run.
    #[arg(long)]
    pub strategy_revision: String,
}

#[derive(Debug, Args)]
pub struct PaperStatusArgs {
    #[command(flatten)]
    pub control: TrustedControlArgs,
    /// Stable durable task identity projected by `/api/v1/tasks`.
    #[arg(long)]
    pub task_id: String,
}
