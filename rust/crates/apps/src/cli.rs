use std::{net::SocketAddr, path::PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};
use rust_decimal::Decimal;
use uuid::Uuid;

/// Rust-first command surface replacing the legacy Python launch scripts.
#[derive(Debug, Parser)]
#[command(
    name = "crypto-trading",
    version,
    about = "Multi-exchange strategy kernel with deterministic paper execution and an auditable journal.",
    long_about = "Multi-exchange strategy kernel with deterministic paper execution and an \
                  auditable JSONL journal.\n\n\
                  Mainnet trading is disabled and fails closed. The only path with order \
                  authority is Binance Testnet, and it requires an exact acknowledgement \
                  phrase. Run `capabilities --json` for the authoritative statement of what \
                  this build is permitted to do."
)]
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
    /// Run or recover one durable Binance testnet order lifecycle.
    #[command(name = "testnet-lifecycle")]
    TestnetLifecycle(TestnetLifecycleArgs),
    /// Compare signed Binance Testnet account truth to one committed Paper reservation.
    #[command(name = "testnet-reconcile")]
    TestnetReconcile(TestnetReconciliationArgs),
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
    /// Run one deterministic bar-driven paper owner over closed spot bars.
    #[command(name = "paper-bar")]
    PaperBar(PaperBarArgs),
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
    #[arg(long, default_value = "BTC-USDT-SPOT")]
    pub spot_symbol: String,
    /// Standard perpetual symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDT-PERP")]
    pub perpetual_symbol: String,
    /// Exact Binance wire symbol used for both spot and perpetual probes.
    #[arg(long, default_value = "BTCUSDT")]
    pub wire_symbol: String,
    /// Total HTTP timeout for each remote call in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub timeout_ms: u64,
}

#[derive(Debug, Args)]
pub struct TestnetLifecycleArgs {
    /// Emit the final durable proof as JSON.
    #[arg(long)]
    pub json: bool,
    /// Exact acknowledgement required before any Testnet mutation.
    #[arg(long, value_name = "PHRASE")]
    pub acknowledge_testnet_lifecycle: String,
    /// Stable campaign identity reused for recovery.
    #[arg(long)]
    pub campaign_id: String,
    /// Stable UUID persisted before submission and reused for every query.
    #[arg(long)]
    pub client_order_id: Uuid,
    /// Dedicated append-only JSONL evidence path.
    #[arg(long)]
    pub history_path: PathBuf,
    /// Binance product selected for this Testnet order.
    #[arg(long, value_enum, default_value_t = TestnetLifecycleMarket::Spot)]
    pub market: TestnetLifecycleMarket,
    /// Order side.
    #[arg(long, value_enum)]
    pub side: TestnetLifecycleSide,
    /// Positive base-asset quantity that satisfies the configured instrument rule.
    #[arg(long)]
    pub quantity: Decimal,
    /// Positive limit price that satisfies the configured instrument rule.
    #[arg(long)]
    pub price: Decimal,
    /// GTC supports manual partial-fill evidence; post-only is the safer open-order default.
    #[arg(long, value_enum, default_value_t = TestnetLifecycleTimeInForce::PostOnly)]
    pub time_in_force: TestnetLifecycleTimeInForce,
    /// Order state that a signed single-order query must prove before cancellation.
    #[arg(long, value_enum, default_value_t = TestnetLifecycleExpected::Open)]
    pub expected_observation: TestnetLifecycleExpected,
    /// Permit reduce-only semantics for a USD-M order.
    #[arg(long)]
    pub reduce_only: bool,
    /// Standard spot symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDT-SPOT")]
    pub spot_symbol: String,
    /// Standard perpetual symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDT-PERP")]
    pub perpetual_symbol: String,
    /// Exact Binance wire symbol used by the Testnet request.
    #[arg(long, default_value = "BTCUSDT")]
    pub wire_symbol: String,
    /// Delay between authoritative single-order queries.
    #[arg(long, default_value_t = 2_000)]
    pub poll_interval_ms: u64,
    /// Maximum signed single-order query attempts across the durable campaign.
    #[arg(long, default_value_t = 30)]
    pub maximum_queries: u16,
    /// Total HTTP timeout for each remote request in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub timeout_ms: u64,
}

#[derive(Debug, Args)]
pub struct TestnetReconciliationArgs {
    /// Emit the reconciliation verdict and proof as JSON.
    #[arg(long)]
    pub json: bool,
    /// Paper account journal containing the committed reservation.
    #[arg(long)]
    pub history_path: PathBuf,
    /// Exact journal generation UUID used by the Paper account authority.
    #[arg(long)]
    pub journal_id: Uuid,
    /// Exact Paper account identity.
    #[arg(long)]
    pub account_id: String,
    /// Original Paper account starting quote capacity.
    #[arg(long)]
    pub initial_available: Decimal,
    /// Exact committed Paper reservation to reconcile.
    #[arg(long)]
    pub reservation_id: Uuid,
    /// Binance product selected for this account comparison.
    #[arg(long, value_enum, default_value_t = TestnetLifecycleMarket::Spot)]
    pub market: TestnetLifecycleMarket,
    /// Settlement asset whose available balance must match local post-release capacity.
    #[arg(long, default_value = "USDT")]
    pub settlement_asset: String,
    /// Standard spot symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDT-SPOT")]
    pub spot_symbol: String,
    /// Standard perpetual symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDT-PERP")]
    pub perpetual_symbol: String,
    /// Exact Binance wire symbol used to parse account order and position truth.
    #[arg(long, default_value = "BTCUSDT")]
    pub wire_symbol: String,
    /// Total HTTP timeout for each signed remote request in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub timeout_ms: u64,
    /// Apply the verified release/failure transition using the exact acknowledgement phrase.
    #[arg(long, value_name = "PHRASE")]
    pub apply_reconciliation: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum TestnetLifecycleMarket {
    Spot,
    Usdm,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum TestnetLifecycleSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum TestnetLifecycleTimeInForce {
    Gtc,
    PostOnly,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum TestnetLifecycleExpected {
    Open,
    PartiallyFilled,
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
    #[arg(long, default_value = "BTC-USDT-SPOT")]
    pub spot_symbol: String,
    /// Perpetual symbol mapped to the selected wire symbol.
    #[arg(long, default_value = "BTC-USDT-PERP")]
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
    /// Optional exact lifecycle configuration. A fresh campaign additionally
    /// requires the Testnet acknowledgement; a pending campaign is recovered
    /// query-first without new submit authority.
    #[command(flatten)]
    pub lifecycle_recovery: TestnetSoakLifecycleRecoveryArgs,
}

/// All-or-none identity and intent for one continuous Testnet lifecycle.
/// Fresh submit authority is acknowledgement-gated, while a durable pending
/// plan can only enter the exact-ID recovery branch.
#[derive(Debug, Args, Default)]
pub struct TestnetSoakLifecycleRecoveryArgs {
    /// Exact acknowledgement required only when this exact configuration is
    /// still eligible for its first Testnet submit.
    #[arg(long, value_name = "PHRASE")]
    pub acknowledge_testnet_lifecycle: Option<String>,
    /// Campaign identity already present in the lifecycle journal.
    #[arg(long)]
    pub recovery_campaign_id: Option<String>,
    /// Exact UUID-backed client order ID from the durable plan.
    #[arg(long)]
    pub recovery_client_order_id: Option<Uuid>,
    /// Product from the durable plan.
    #[arg(long, value_enum)]
    pub recovery_market: Option<TestnetLifecycleMarket>,
    /// Side from the durable plan.
    #[arg(long, value_enum)]
    pub recovery_side: Option<TestnetLifecycleSide>,
    /// Exact quantity from the durable plan.
    #[arg(long)]
    pub recovery_quantity: Option<Decimal>,
    /// Exact price from the durable plan.
    #[arg(long)]
    pub recovery_price: Option<Decimal>,
    /// Exact time-in-force from the durable plan.
    #[arg(long, value_enum)]
    pub recovery_time_in_force: Option<TestnetLifecycleTimeInForce>,
    /// Exact expected observation from the durable plan.
    #[arg(long, value_enum)]
    pub recovery_expected_observation: Option<TestnetLifecycleExpected>,
    /// Exact reduce-only bit from the durable plan (`true` or `false`).
    #[arg(long, value_name = "BOOL")]
    pub recovery_reduce_only: Option<bool>,
    /// Exact query cadence from the durable plan.
    #[arg(long)]
    pub recovery_poll_interval_ms: Option<u64>,
    /// Exact query budget from the durable plan.
    #[arg(long)]
    pub recovery_maximum_queries: Option<u16>,
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

#[allow(
    clippy::struct_excessive_bools,
    reason = "operator CLI flags are independent toggles, not an encoded state machine"
)]
#[derive(Debug, Args)]
pub struct MonitorArgs {
    #[arg(
        long,
        default_value_t = MonitorMode::Replay,
        default_value_if("live", "true", "serve"),
        value_enum
    )]
    pub mode: MonitorMode,
    #[arg(
        long,
        default_value = "config/arbitrage/monitor_v2.yaml",
        default_value_if("live", "true", "config/arbitrage/monitor-live-testnet.yaml")
    )]
    pub config: PathBuf,
    /// Finite JSONL replay of validated top-of-book snapshots.
    #[arg(long, value_name = "PATH")]
    pub replay: Option<PathBuf>,
    /// Consume the real credential-free Binance Spot stream plus the
    /// Hyperliquid public leg instead of a --replay fixture. Read-only.
    #[arg(long, conflicts_with = "replay")]
    pub live: bool,
    /// Binance live transport. Streaming is the default; REST polling is an
    /// explicit degraded fallback for incident handling.
    #[arg(
        long,
        value_enum,
        default_value_t = MonitorLiveTransport::Stream,
        requires = "live"
    )]
    pub live_transport: MonitorLiveTransport,
    /// Loopback Binance websocket-root override for bounded local tests.
    #[arg(long, hide = true)]
    pub binance_ws_base_url: Option<String>,
    /// Loopback Binance base-URL override for bounded local tests.
    #[arg(long, hide = true)]
    pub binance_base_url: Option<String>,
    /// Loopback Hyperliquid base-URL override for bounded local tests.
    #[arg(long, hide = true)]
    pub hyperliquid_base_url: Option<String>,
    /// Live polling interval in milliseconds, used only by the explicit
    /// `--live-transport polling` degradation path.
    #[arg(long, hide = true, default_value_t = 1_000)]
    pub poll_interval_ms: u64,
    /// Service task identity for long-running monitor operations.
    #[arg(long)]
    pub task_id: Option<String>,
    /// Append read-only monitor outcomes to this JSONL journal.
    #[arg(long, default_value = "var/history/arbitrage-monitor.jsonl")]
    pub history_path: PathBuf,
    /// Append serve-mode spread observations to this dedicated JSONL journal.
    #[arg(long, default_value = "var/history/spread-history.jsonl")]
    pub spread_history_path: PathBuf,
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

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, ValueEnum)]
pub enum MonitorLiveTransport {
    #[default]
    Stream,
    Polling,
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
    #[arg(long, default_value_t = VolumeMakerRunMode::Validate, value_enum)]
    pub mode: VolumeMakerRunMode,
    #[arg(default_value = "config/volume_maker/backpack_btc_volume_maker.yaml")]
    pub config: PathBuf,
    /// Finite JSONL replay of validated top-of-book snapshots.
    #[arg(long, value_name = "PATH")]
    pub replay: Option<PathBuf>,
    /// Service task identity for long-running volume-maker operations.
    #[arg(long)]
    pub task_id: Option<String>,
    /// Append paper volume-maker facts to this JSONL journal.
    #[arg(long, default_value = "var/history/volume-maker.jsonl")]
    pub history_path: PathBuf,
    /// Bounded account-level limits required by paper volume-maker serve mode.
    #[arg(long, value_name = "PATH")]
    pub paper_account_risk_config: Option<PathBuf>,
    #[arg(long)]
    pub debug: bool,
    /// Local loopback control port override used by volume-maker serve/status/stop.
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
pub enum VolumeMakerRunMode {
    #[default]
    Validate,
    Serve,
    Status,
    Stop,
}

#[derive(Debug, Args)]
pub struct PriceAlertArgs {
    #[arg(long, default_value_t = PriceAlertMode::Validate, value_enum)]
    pub mode: PriceAlertMode,
    #[arg(default_value = "config/price_alert/binance_alert.yaml")]
    pub config: PathBuf,
    /// Finite JSONL replay of validated top-of-book snapshots.
    #[arg(long, value_name = "PATH")]
    pub replay: Option<PathBuf>,
    /// Service task identity for long-running price-alert operations.
    #[arg(long)]
    pub task_id: Option<String>,
    /// Append read-only price-alert outcomes to this JSONL journal.
    #[arg(long, default_value = "var/history/price-alert.jsonl")]
    pub history_path: PathBuf,
    #[arg(long)]
    pub debug: bool,
    /// Local loopback control port override used by price-alert serve/status/stop.
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
pub enum PriceAlertMode {
    #[default]
    Validate,
    Serve,
    Status,
    Stop,
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
    #[arg(long, default_value_t = ScannerMode::Validate, value_enum)]
    pub mode: ScannerMode,
    /// Legacy exchange selector kept for CLI compatibility; the validated
    /// scanner config file is the runtime source of truth.
    #[arg(long, value_enum, default_value_t)]
    pub exchange: ExchangeChoice,
    /// Legacy duration hint kept for CLI compatibility; serve runs until stop.
    #[arg(long)]
    pub duration: Option<u64>,
    /// Scanner YAML configuration validated by every mode that starts work.
    #[arg(long, default_value = "config/scanner/binance_scanner.yaml")]
    pub config: PathBuf,
    #[arg(long, value_enum, default_value_t)]
    pub log_level: LogLevel,
    /// Finite JSONL replay of validated top-of-book snapshots.
    #[arg(long, value_name = "PATH")]
    pub replay: Option<PathBuf>,
    /// Service task identity for long-running scanner operations.
    #[arg(long)]
    pub task_id: Option<String>,
    /// Append read-only scanner outcomes to this JSONL journal.
    #[arg(long, default_value = "var/history/scanner.jsonl")]
    pub history_path: PathBuf,
    /// Local loopback control port override used by scanner serve/status/stop.
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
pub enum ScannerMode {
    #[default]
    Validate,
    Serve,
    Status,
    Stop,
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

#[derive(Debug, Args)]
pub struct PaperBarArgs {
    /// Stable owner identity recorded on journaled reservations.
    #[arg(long)]
    pub task_id: String,
    /// Optional closed-bar history replayed for indicator warmup only.
    #[arg(long, value_name = "PATH")]
    pub warmup_bars_csv: Option<PathBuf>,
    /// Headerless Binance-style kline CSV with closed bars only.
    #[arg(long, value_name = "PATH")]
    pub bars_csv: PathBuf,
    /// Canonical spot symbol traded by this owner.
    #[arg(long, default_value = "BTC-USDT-SPOT")]
    pub symbol: String,
    /// Paper exchange identity written onto intents and receipts.
    #[arg(long, default_value = "paper")]
    pub exchange: String,
    /// Append paper-bar decisions and execution facts to this JSONL journal.
    #[arg(long, default_value = "var/history/paper-bar.jsonl")]
    pub history_path: PathBuf,
    /// Starting quote balance for the dedicated paper account.
    #[arg(long, default_value = "1000")]
    pub initial_available: Decimal,
    /// Optional bounded account-risk limits for entry-side admissions.
    #[arg(long, value_name = "PATH")]
    pub paper_account_risk_config: Option<PathBuf>,
    /// Absolute dataset index of the first supplied bar.
    #[arg(long, default_value_t = 0)]
    pub start_bar_index: usize,
    /// Per-fill fee used by both reservation budgeting and exact settlement.
    #[arg(long, default_value_t = 0)]
    pub fee_bps: u32,
    /// Conservative paper-only funding buffer reserved but not settled.
    #[arg(long, default_value_t = 0)]
    pub funding_buffer_bps: u32,
    /// Conservative paper-only slippage buffer reserved and applied to fills.
    #[arg(long, default_value_t = 0)]
    pub slippage_bps: u32,
    /// Half-spread component applied to synthetic paper fills.
    #[arg(long, default_value_t = 0)]
    pub half_spread_bps: u32,
    /// Latency impact component applied to synthetic paper fills.
    #[arg(long, default_value_t = 0)]
    pub latency_bps: u32,
    #[command(subcommand)]
    pub strategy: PaperBarStrategyArgs,
}

#[derive(Debug, Subcommand)]
pub enum PaperBarStrategyArgs {
    Cash,
    #[command(name = "buy-and-hold")]
    BuyAndHold,
    #[command(name = "slow-time-series-momentum")]
    SlowTimeSeriesMomentum {
        #[arg(long)]
        lookback_bars: usize,
        #[arg(long)]
        rebalance_every_bars: usize,
    },
    #[command(name = "long-only-donchian")]
    LongOnlyDonchian {
        #[arg(long)]
        lookback_bars: usize,
    },
    #[command(name = "capped-volatility-target")]
    CappedVolatilityTarget {
        #[arg(long)]
        lookback_returns: usize,
        #[arg(long)]
        annual_target: Decimal,
        #[arg(long)]
        rebalance_band: Decimal,
        #[arg(long)]
        rebalance_every_bars: usize,
        /// Explicit annualization for non-daily bars.
        #[arg(long)]
        periods_per_year: Option<Decimal>,
    },
}

#[derive(Debug, Subcommand)]
pub enum PaperCommand {
    /// Control one continuous paper grid task through the trusted submit seam.
    Grid(PaperTaskArgs),
    /// Control one continuous paper arbitrage task through the trusted submit seam.
    Arbitrage(PaperTaskArgs),
    /// Control the shared account-level risk authority through the trusted submit seam.
    Risk(PaperRiskArgs),
}

#[derive(Debug, Args)]
pub struct PaperRiskArgs {
    #[command(subcommand)]
    pub operation: PaperRiskOperation,
}

#[derive(Debug, Subcommand)]
pub enum PaperRiskOperation {
    /// Durably pause new account-risk admissions with an explicit reason.
    Pause(PaperRiskReasonArgs),
    /// Durably resume admissions after a pause; never clears the kill switch.
    Resume(PaperMutationArgs),
    /// Durably engage the latching account kill switch.
    KillSwitch(PaperRiskKillSwitchArgs),
}

#[derive(Debug, Args)]
pub struct PaperRiskReasonArgs {
    #[command(flatten)]
    pub mutation: PaperMutationArgs,
    /// Bounded operator-facing reason recorded as a durable fact.
    #[arg(long)]
    pub reason: String,
}

#[derive(Debug, Args)]
pub struct PaperRiskKillSwitchArgs {
    #[command(flatten)]
    pub mutation: PaperMutationArgs,
    /// Bounded operator-facing reason recorded as a durable fact.
    #[arg(long)]
    pub reason: String,
    /// Exact acknowledgement phrase; any other value fails closed before any
    /// request is sent.
    #[arg(long, value_name = "PHRASE")]
    pub acknowledge: String,
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
