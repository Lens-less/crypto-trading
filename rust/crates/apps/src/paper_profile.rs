use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::pending,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use crypto_trading_config::{
    ArbitrageConfig, GridConfig, GridMode, MonitorConfig, load_arbitrage_config_from_str,
    load_grid_config_from_str, load_monitor_config_from_str, read_bounded_config,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, Money, OrderIntent, Price, Symbol};
use crypto_trading_exchange::PaperExchange;
use crypto_trading_runtime::{
    ExchangeRouter, ExecutionBatch, ExecutionClock, ExecutionMode, ExecutionPolicy, IntentExecutor,
    JsonlHistory, MarketDataBook, MarketDataClock, MarketDataError, MarketDataEvent,
    MarketDataEventFuture, MarketDataEventSource, MarketDataObservation, MarketFreshnessPolicy,
    MarketInstrument, MarketUniverse, PaperAccountAuthority, PaperAccountConfig, PaperCostModel,
    RuntimeError,
};
use crypto_trading_strategy::{VirtualGrid, VirtualGridConfig};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::{
    ArbitragePaperExecutionFuture, ArbitragePaperExecutor, ArbitragePaperTask,
    ArbitragePaperTaskConfig, ArbitragePaperTaskError, GridPaperExecutionFuture, GridPaperExecutor,
    GridPaperTask, GridPaperTaskConfig, GridPaperTaskError,
    monitor::{ReadOnlyArbitrageMonitor, ReplayMarketDataClock, load_market_snapshot_replay},
};

const DEFAULT_COST_FEE_BPS: u32 = 10;
const DEFAULT_COST_FUNDING_BUFFER_BPS: u32 = 5;
const DEFAULT_COST_SLIPPAGE_BPS: u32 = 15;
const DEFAULT_GRID_INITIAL_AVAILABLE: i64 = 10_000;
const DEFAULT_ARBITRAGE_INITIAL_AVAILABLE: i64 = 100_000;
const DEFAULT_EVENT_CAPACITY: usize = 256;
const DEFAULT_GRID_MAX_MARKET_AGE_SECONDS: i64 = 30;

#[derive(Clone, Debug)]
pub struct PaperProfileCatalogInput {
    pub grid: Option<GridPaperProfileInput>,
    pub arbitrage: Option<ArbitragePaperProfileInput>,
}

#[derive(Clone, Debug)]
pub struct GridPaperProfileInput {
    pub task_id: String,
    pub strategy_id: String,
    pub strategy_revision: String,
    pub config_path: PathBuf,
    pub replay_path: PathBuf,
    pub shutdown_grace: StdDuration,
}

#[derive(Clone, Debug)]
pub struct ArbitragePaperProfileInput {
    pub task_id: String,
    pub strategy_id: String,
    pub strategy_revision: String,
    pub arbitrage_config_path: PathBuf,
    pub monitor_config_path: PathBuf,
    pub replay_path: PathBuf,
    pub shutdown_grace: StdDuration,
}

#[derive(Clone, Debug)]
pub struct PaperProfileCatalog {
    grid: Option<GridPaperProfile>,
    arbitrage: Option<ArbitragePaperProfile>,
}

#[derive(Debug)]
pub enum StartedPaperTask {
    Grid(GridPaperTask),
    Arbitrage(ArbitragePaperTask),
}

#[derive(Debug)]
pub enum PaperProfileError {
    UnknownTask,
    UnsupportedCommand,
    StrategyMismatch,
    GridStart(GridPaperTaskError),
    ArbitrageStart(ArbitragePaperTaskError),
}

impl PaperProfileCatalog {
    /// Loads and validates the configured replay-backed paper task profiles.
    ///
    /// # Errors
    ///
    /// Returns an error when a profile is incomplete, internally inconsistent,
    /// or references a config/replay file that cannot be loaded safely.
    pub fn new(input: PaperProfileCatalogInput) -> Result<Self> {
        let catalog = Self {
            grid: input.grid.map(load_grid_profile).transpose()?,
            arbitrage: input.arbitrage.map(load_arbitrage_profile).transpose()?,
        };
        if matches!(
            (&catalog.grid, &catalog.arbitrage),
            (Some(grid), Some(arbitrage)) if grid.task_id == arbitrage.task_id
        ) {
            bail!("grid and arbitrage paper profiles must use distinct task identities");
        }
        Ok(catalog)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.grid.is_none() && self.arbitrage.is_none()
    }

    #[must_use]
    pub fn task_ids(&self) -> Vec<String> {
        let mut task_ids = Vec::new();
        if let Some(grid) = &self.grid {
            task_ids.push(grid.task_id.clone());
        }
        if let Some(arbitrage) = &self.arbitrage {
            task_ids.push(arbitrage.task_id.clone());
        }
        task_ids
    }

    /// Starts the one configured paper owner that exactly matches `envelope`.
    ///
    /// # Errors
    ///
    /// Returns a typed rejection for an unknown task, unsupported command, or
    /// strategy mismatch, and a typed owner error when startup cannot complete.
    pub async fn start_matching(
        &self,
        journal_id: Uuid,
        history_path: &Path,
        envelope: &crypto_trading_control_plane::SubmitEnvelope,
    ) -> Result<StartedPaperTask, PaperProfileError> {
        match envelope.command() {
            crypto_trading_control_plane::SubmitCommand::StartPaperGrid {
                strategy_id,
                strategy_revision,
            } => {
                let profile = self.grid.as_ref().ok_or(PaperProfileError::UnknownTask)?;
                if envelope.target_task_id() != profile.task_id {
                    return Err(PaperProfileError::UnknownTask);
                }
                if strategy_id != &profile.strategy_id
                    || strategy_revision != &profile.strategy_revision
                {
                    return Err(PaperProfileError::StrategyMismatch);
                }
                profile
                    .start(journal_id, history_path)
                    .await
                    .map(StartedPaperTask::Grid)
                    .map_err(PaperProfileError::GridStart)
            }
            crypto_trading_control_plane::SubmitCommand::StartPaperArbitrage {
                strategy_id,
                strategy_revision,
            } => {
                let profile = self
                    .arbitrage
                    .as_ref()
                    .ok_or(PaperProfileError::UnknownTask)?;
                if envelope.target_task_id() != profile.task_id {
                    return Err(PaperProfileError::UnknownTask);
                }
                if strategy_id != &profile.strategy_id
                    || strategy_revision != &profile.strategy_revision
                {
                    return Err(PaperProfileError::StrategyMismatch);
                }
                profile
                    .start(journal_id, history_path)
                    .await
                    .map(StartedPaperTask::Arbitrage)
                    .map_err(PaperProfileError::ArbitrageStart)
            }
            _ => Err(PaperProfileError::UnsupportedCommand),
        }
    }
}

impl PaperProfileError {
    #[must_use]
    pub const fn is_rejected(&self) -> bool {
        match self {
            Self::UnknownTask | Self::UnsupportedCommand | Self::StrategyMismatch => true,
            Self::GridStart(error) => matches!(
                error,
                GridPaperTaskError::InvalidConfig
                    | GridPaperTaskError::InvalidSourceBinding
                    | GridPaperTaskError::InvalidRequest
                    | GridPaperTaskError::RecoveryRequired
                    | GridPaperTaskError::ShutdownTimedOut
                    | GridPaperTaskError::Account(_)
                    | GridPaperTaskError::Source(_)
                    | GridPaperTaskError::Strategy(_)
                    | GridPaperTaskError::Runtime(_)
                    | GridPaperTaskError::Saga(_)
                    | GridPaperTaskError::TaskCancelled
                    | GridPaperTaskError::PreviouslyFailed(_)
            ),
            Self::ArbitrageStart(error) => matches!(
                error,
                ArbitragePaperTaskError::InvalidConfig
                    | ArbitragePaperTaskError::InvalidSourceBinding
                    | ArbitragePaperTaskError::InvalidRequest
                    | ArbitragePaperTaskError::LiquidityRejected
                    | ArbitragePaperTaskError::RiskRejected(_)
                    | ArbitragePaperTaskError::RecoveryRequired
                    | ArbitragePaperTaskError::ShutdownTimedOut
                    | ArbitragePaperTaskError::Account(_)
                    | ArbitragePaperTaskError::Source(_)
                    | ArbitragePaperTaskError::SourceContract
                    | ArbitragePaperTaskError::Monitor(_)
                    | ArbitragePaperTaskError::Market(_)
                    | ArbitragePaperTaskError::Strategy(_)
                    | ArbitragePaperTaskError::Runtime(_)
                    | ArbitragePaperTaskError::Saga(_)
                    | ArbitragePaperTaskError::TaskCancelled
                    | ArbitragePaperTaskError::PreviouslyFailed(_)
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct GridPaperProfile {
    task_id: String,
    strategy_id: String,
    strategy_revision: String,
    config: GridConfig,
    replay_events: Vec<MarketDataEvent>,
    shutdown_grace: StdDuration,
}

impl GridPaperProfile {
    async fn start(
        &self,
        journal_id: Uuid,
        history_path: &Path,
    ) -> Result<GridPaperTask, GridPaperTaskError> {
        let first_snapshot =
            first_observation(&self.replay_events).ok_or(GridPaperTaskError::InvalidConfig)?;
        let clock = Arc::new(ReplayMarketDataClock::new(first_snapshot.timestamp));
        let latest = Arc::new(MirroredReplayState::default());
        let exchange = Arc::new(
            paper_exchange(self.config.exchange.clone(), &clock)
                .map_err(|_| GridPaperTaskError::InvalidConfig)?,
        );
        let source = MirroredReplaySource::new(
            self.config.exchange.clone(),
            self.replay_events.clone(),
            Arc::clone(&clock),
            Arc::clone(&latest),
            vec![Arc::clone(&exchange)],
        );
        let history = JsonlHistory::new(history_path);
        let account = PaperAccountAuthority::new(
            journal_id,
            history.clone(),
            PaperAccountConfig::new(
                format!("paper-grid:{}", self.task_id),
                Money::new(Decimal::from(DEFAULT_GRID_INITIAL_AVAILABLE)),
            )
            .map_err(GridPaperTaskError::Account)?,
        )
        .map_err(GridPaperTaskError::Account)?;
        let grid = build_virtual_grid(&self.config, first_snapshot.timestamp)
            .map_err(GridPaperTaskError::Strategy)?;
        let executor = Arc::new(GridReplayExecutor {
            exchange,
            exchange_name: self.config.exchange.clone(),
            clock,
            latest,
        });
        GridPaperTask::start(
            GridPaperTaskConfig::new(
                self.task_id.clone(),
                self.config.exchange.clone(),
                self.config.market_type,
                self.config.order_amount,
                default_cost_model().map_err(GridPaperTaskError::Account)?,
                crypto_trading_runtime::MarketSupervisorConfig::new(self.shutdown_grace)
                    .map_err(GridPaperTaskError::Source)?,
            )?,
            grid,
            source,
            account,
            history,
            executor,
        )
        .await
    }
}

#[derive(Clone, Debug)]
struct ArbitragePaperProfile {
    task_id: String,
    strategy_id: String,
    strategy_revision: String,
    strategy: ArbitrageConfig,
    monitor: MonitorConfig,
    symbol: Symbol,
    left_exchange: String,
    right_exchange: String,
    replay_events: Vec<MarketDataEvent>,
    shutdown_grace: StdDuration,
}

impl ArbitragePaperProfile {
    async fn start(
        &self,
        journal_id: Uuid,
        history_path: &Path,
    ) -> Result<ArbitragePaperTask, ArbitragePaperTaskError> {
        let first_snapshot =
            first_observation(&self.replay_events).ok_or(ArbitragePaperTaskError::InvalidConfig)?;
        let clock = Arc::new(ReplayMarketDataClock::new(first_snapshot.timestamp));
        let latest = Arc::new(MirroredReplayState::default());
        let left_exchange = Arc::new(
            paper_exchange(self.left_exchange.clone(), &clock)
                .map_err(|_| ArbitragePaperTaskError::InvalidConfig)?,
        );
        let right_exchange = Arc::new(
            paper_exchange(self.right_exchange.clone(), &clock)
                .map_err(|_| ArbitragePaperTaskError::InvalidConfig)?,
        );
        let history = JsonlHistory::new(history_path);
        let account = PaperAccountAuthority::new(
            journal_id,
            history.clone(),
            PaperAccountConfig::new(
                format!("paper-arbitrage:{}", self.task_id),
                Money::new(Decimal::from(DEFAULT_ARBITRAGE_INITIAL_AVAILABLE)),
            )
            .map_err(ArbitragePaperTaskError::Account)?,
        )
        .map_err(ArbitragePaperTaskError::Account)?;
        let monitor = build_monitor(
            &self.monitor,
            &self.left_exchange,
            &self.right_exchange,
            self.symbol.clone(),
            Arc::clone(&clock),
        )
        .map_err(ArbitragePaperTaskError::Monitor)?;
        let left_source = MirroredReplaySource::new(
            self.left_exchange.clone(),
            filter_events(&self.replay_events, &self.left_exchange, &self.symbol),
            Arc::clone(&clock),
            Arc::clone(&latest),
            vec![Arc::clone(&left_exchange)],
        );
        let right_source = MirroredReplaySource::new(
            self.right_exchange.clone(),
            filter_events(&self.replay_events, &self.right_exchange, &self.symbol),
            Arc::clone(&clock),
            Arc::clone(&latest),
            vec![Arc::clone(&right_exchange)],
        );
        let executor = Arc::new(ArbitrageReplayExecutor {
            left: left_exchange,
            left_exchange: self.left_exchange.clone(),
            right: right_exchange,
            right_exchange: self.right_exchange.clone(),
            clock,
            latest,
        });
        ArbitragePaperTask::start(
            ArbitragePaperTaskConfig::new(
                self.task_id.clone(),
                &self.strategy,
                Duration::seconds(
                    i64::try_from(self.monitor.data_timeout_seconds).unwrap_or(i64::MAX),
                ),
                default_cost_model().map_err(ArbitragePaperTaskError::Account)?,
                crypto_trading_runtime::MarketSupervisorConfig::new(self.shutdown_grace)
                    .map_err(ArbitragePaperTaskError::Source)?,
            )?,
            monitor,
            left_source,
            right_source,
            account,
            history,
            executor,
        )
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SnapshotKey {
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
}

impl SnapshotKey {
    fn from_snapshot(snapshot: &MarketSnapshot) -> Self {
        Self {
            exchange: snapshot.exchange().to_owned(),
            symbol: snapshot.symbol.clone(),
            market_type: snapshot.market_type,
        }
    }

    fn from_intent(intent: &OrderIntent) -> Self {
        Self {
            exchange: intent.exchange.clone(),
            symbol: intent.symbol.clone(),
            market_type: intent.market_type,
        }
    }
}

#[derive(Debug, Default)]
struct MirroredReplayState {
    latest: Mutex<HashMap<SnapshotKey, MarketSnapshot>>,
}

impl MirroredReplayState {
    fn update(&self, snapshot: MarketSnapshot) {
        let mut latest = self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        latest.insert(SnapshotKey::from_snapshot(&snapshot), snapshot);
    }

    fn snapshots_for_batch(
        &self,
        intents: &[OrderIntent],
    ) -> Result<Vec<MarketSnapshot>, RuntimeError> {
        let latest = self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut selected = HashMap::<SnapshotKey, MarketSnapshot>::new();
        for intent in intents {
            let key = SnapshotKey::from_intent(intent);
            let snapshot = latest
                .get(&key)
                .ok_or_else(|| RuntimeError::MissingMarketData {
                    exchange: intent.exchange.clone(),
                    symbol: intent.symbol.clone(),
                    market_type: intent.market_type,
                })?;
            selected.entry(key).or_insert_with(|| snapshot.clone());
        }
        Ok(selected.into_values().collect())
    }
}

struct MirroredReplaySource {
    source_id: String,
    events: VecDeque<MarketDataEvent>,
    clock: Arc<ReplayMarketDataClock>,
    latest: Arc<MirroredReplayState>,
    exchanges: Vec<Arc<PaperExchange>>,
}

impl fmt::Debug for MirroredReplaySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MirroredReplaySource")
            .field("source_id", &self.source_id)
            .field("remaining_events", &self.events.len())
            .finish_non_exhaustive()
    }
}

impl MirroredReplaySource {
    fn new(
        source_id: String,
        events: Vec<MarketDataEvent>,
        clock: Arc<ReplayMarketDataClock>,
        latest: Arc<MirroredReplayState>,
        exchanges: Vec<Arc<PaperExchange>>,
    ) -> Self {
        Self {
            source_id,
            events: events.into(),
            clock,
            latest,
            exchanges,
        }
    }
}

impl MarketDataEventSource for MirroredReplaySource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn next_event(&mut self) -> MarketDataEventFuture<'_> {
        let Some(event) = self.events.pop_front() else {
            return Box::pin(async move { pending().await });
        };
        let clock = Arc::clone(&self.clock);
        let latest = Arc::clone(&self.latest);
        let exchanges = self.exchanges.clone();
        let source_id = self.source_id.clone();
        Box::pin(async move {
            match &event {
                MarketDataEvent::Observation(observation) => {
                    clock.advance(observation.received_at);
                    latest.update(observation.snapshot.clone());
                    for exchange in exchanges {
                        exchange
                            .publish_snapshot(observation.snapshot.clone())
                            .await
                            .map_err(|_| MarketDataError::SourceEventTimeRollback {
                                exchange: source_id.clone(),
                            })?;
                    }
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

impl ExecutionClock for ReplayMarketDataClock {
    fn now(&self) -> DateTime<Utc> {
        MarketDataClock::now(self)
    }
}

struct GridReplayExecutor {
    exchange: Arc<PaperExchange>,
    exchange_name: String,
    clock: Arc<ReplayMarketDataClock>,
    latest: Arc<MirroredReplayState>,
}

impl GridPaperExecutor for GridReplayExecutor {
    fn execute(&self, batch: ExecutionBatch) -> GridPaperExecutionFuture {
        let exchange = Arc::clone(&self.exchange);
        let exchange_name = self.exchange_name.clone();
        let clock = Arc::clone(&self.clock);
        let latest = Arc::clone(&self.latest);
        Box::pin(async move {
            let snapshots = latest.snapshots_for_batch(batch.intents())?;
            let policy = ExecutionPolicy::new(
                true,
                false,
                ExecutionClock::now(clock.as_ref()),
                Duration::seconds(DEFAULT_GRID_MAX_MARKET_AGE_SECONDS),
                snapshots,
            )?
            .with_clock(clock);
            let executor = IntentExecutor::new(exchange, ExecutionMode::Paper, policy);
            if batch
                .intents()
                .iter()
                .any(|intent| intent.exchange != exchange_name)
            {
                return Err(RuntimeError::UnknownExchange(exchange_name));
            }
            executor.execute_batch(batch).await
        })
    }
}

struct ArbitrageReplayExecutor {
    left: Arc<PaperExchange>,
    left_exchange: String,
    right: Arc<PaperExchange>,
    right_exchange: String,
    clock: Arc<ReplayMarketDataClock>,
    latest: Arc<MirroredReplayState>,
}

impl ArbitragePaperExecutor for ArbitrageReplayExecutor {
    fn execute(&self, batch: ExecutionBatch) -> ArbitragePaperExecutionFuture {
        let left = Arc::clone(&self.left);
        let left_exchange = self.left_exchange.clone();
        let right = Arc::clone(&self.right);
        let right_exchange = self.right_exchange.clone();
        let clock = Arc::clone(&self.clock);
        let latest = Arc::clone(&self.latest);
        Box::pin(async move {
            let snapshots = latest.snapshots_for_batch(batch.intents())?;
            let policy = ExecutionPolicy::new(
                true,
                false,
                ExecutionClock::now(clock.as_ref()),
                Duration::seconds(30),
                snapshots,
            )?
            .with_clock(clock);
            let mut router = ExchangeRouter::new(ExecutionMode::Paper, policy);
            router.register(left_exchange, left);
            router.register(right_exchange, right);
            router.execute_batch(batch).await
        })
    }
}

fn load_grid_profile(input: GridPaperProfileInput) -> Result<GridPaperProfile> {
    validate_profile_identity(&input.task_id, "grid task_id")?;
    validate_profile_identity(&input.strategy_id, "grid strategy_id")?;
    validate_profile_identity(&input.strategy_revision, "grid strategy_revision")?;
    let config =
        load_grid_config_from_str(&read_bounded_config(&input.config_path).with_context(|| {
            format!("failed to read grid config {}", input.config_path.display())
        })?)
        .with_context(|| format!("failed to load grid config {}", input.config_path.display()))?;
    if config.mode != GridMode::FixedLong {
        bail!("grid paper write mode currently supports only fixed long configs");
    }
    let events = load_market_snapshot_replay(&input.replay_path)
        .with_context(|| format!("failed to load grid replay {}", input.replay_path.display()))?;
    validate_grid_replay(
        &events,
        &config.exchange,
        &config.symbol,
        config.market_type,
    )?;
    Ok(GridPaperProfile {
        task_id: input.task_id,
        strategy_id: input.strategy_id,
        strategy_revision: input.strategy_revision,
        config,
        replay_events: events,
        shutdown_grace: input.shutdown_grace,
    })
}

fn load_arbitrage_profile(input: ArbitragePaperProfileInput) -> Result<ArbitragePaperProfile> {
    validate_profile_identity(&input.task_id, "arbitrage task_id")?;
    validate_profile_identity(&input.strategy_id, "arbitrage strategy_id")?;
    validate_profile_identity(&input.strategy_revision, "arbitrage strategy_revision")?;
    let strategy = load_arbitrage_config_from_str(
        &read_bounded_config(&input.arbitrage_config_path).with_context(|| {
            format!(
                "failed to read arbitrage config {}",
                input.arbitrage_config_path.display()
            )
        })?,
    )
    .with_context(|| {
        format!(
            "failed to load arbitrage config {}",
            input.arbitrage_config_path.display()
        )
    })?;
    let monitor = load_monitor_config_from_str(
        &read_bounded_config(&input.monitor_config_path).with_context(|| {
            format!(
                "failed to read monitor config {}",
                input.monitor_config_path.display()
            )
        })?,
    )
    .with_context(|| {
        format!(
            "failed to load monitor config {}",
            input.monitor_config_path.display()
        )
    })?;
    strategy.validate_execution_controls()?;
    let Some(symbol) = strategy.symbols.first().cloned() else {
        bail!("arbitrage profile requires one configured symbol");
    };
    if strategy.symbols.len() != 1 || monitor.symbols.len() != 1 || monitor.symbols[0] != symbol {
        bail!("arbitrage paper write mode requires exactly one shared symbol");
    }
    if strategy.exchanges.len() != 2 || monitor.exchanges.len() != 2 {
        bail!("arbitrage paper write mode requires exactly two configured exchanges");
    }
    if strategy.exchanges != monitor.exchanges {
        bail!("arbitrage strategy and monitor must use the same ordered exact-pair exchanges");
    }
    if strategy.exchanges[0] == strategy.exchanges[1]
        || monitor.exchanges[0] == monitor.exchanges[1]
    {
        bail!("arbitrage paper write mode requires two distinct exchanges");
    }
    let events = load_market_snapshot_replay(&input.replay_path).with_context(|| {
        format!(
            "failed to load arbitrage replay {}",
            input.replay_path.display()
        )
    })?;
    let market_type = market_type_for_symbol(&symbol);
    validate_arbitrage_replay(&events, &monitor.exchanges, &symbol, market_type)?;
    let strategy = strategy
        .resolve_for_strategy(&symbol)
        .with_context(|| format!("arbitrage symbol {symbol} is not executable"))?;
    let left_exchange = strategy.exchanges[0].clone();
    let right_exchange = strategy.exchanges[1].clone();
    Ok(ArbitragePaperProfile {
        task_id: input.task_id,
        strategy_id: input.strategy_id,
        strategy_revision: input.strategy_revision,
        strategy,
        monitor,
        symbol,
        left_exchange,
        right_exchange,
        replay_events: events,
        shutdown_grace: input.shutdown_grace,
    })
}

fn default_cost_model() -> Result<PaperCostModel, crypto_trading_runtime::PaperAccountError> {
    PaperCostModel::v1(
        DEFAULT_COST_FEE_BPS,
        DEFAULT_COST_FUNDING_BUFFER_BPS,
        DEFAULT_COST_SLIPPAGE_BPS,
    )
}

fn paper_exchange(
    exchange: String,
    clock: &Arc<ReplayMarketDataClock>,
) -> Result<PaperExchange, crypto_trading_exchange::ExchangeError> {
    let exchange_clock = Arc::clone(clock);
    PaperExchange::with_clock(
        exchange,
        NonZeroUsize::new(DEFAULT_EVENT_CAPACITY).unwrap(),
        move || MarketDataClock::now(exchange_clock.as_ref()),
    )
}

fn build_virtual_grid(
    config: &GridConfig,
    initialized_at: DateTime<Utc>,
) -> Result<VirtualGrid, crypto_trading_strategy::StrategyError> {
    let lower = config.lower_price.ok_or({
        crypto_trading_strategy::StrategyError::InvalidConfig(
            "grid paper profile requires lower_price",
        )
    })?;
    let upper = config.upper_price.ok_or({
        crypto_trading_strategy::StrategyError::InvalidConfig(
            "grid paper profile requires upper_price",
        )
    })?;
    let midpoint = midpoint_price(lower, upper)?;
    let grid_width_percent =
        (upper.as_decimal() - lower.as_decimal()) / midpoint.as_decimal() * Decimal::from(100_u32);
    let grid_interval_percent =
        config.grid_interval.as_decimal() / midpoint.as_decimal() * Decimal::from(100_u32);
    VirtualGrid::new(
        VirtualGridConfig {
            symbol: config.symbol.clone(),
            initial_price: midpoint,
            grid_width_percent,
            grid_interval_percent,
        },
        initialized_at,
    )
}

fn midpoint_price(
    lower: Price,
    upper: Price,
) -> Result<Price, crypto_trading_strategy::StrategyError> {
    let midpoint = (lower.as_decimal() + upper.as_decimal()) / Decimal::from(2_u32);
    Price::new(midpoint).map_err(|_| {
        crypto_trading_strategy::StrategyError::InvalidConfig("grid midpoint must be positive")
    })
}

fn build_monitor(
    monitor: &MonitorConfig,
    left_exchange: &str,
    right_exchange: &str,
    symbol: Symbol,
    clock: Arc<ReplayMarketDataClock>,
) -> Result<ReadOnlyArbitrageMonitor, crate::monitor::ArbitrageMonitorError> {
    let market_type = market_type_for_symbol(&symbol);
    let left = MarketInstrument::new(left_exchange, symbol.clone(), market_type)?;
    let right = MarketInstrument::new(right_exchange, symbol, market_type)?;
    let universe = MarketUniverse::new(vec![left.clone(), right.clone()])?;
    let freshness = MarketFreshnessPolicy::new(
        Duration::seconds(i64::try_from(monitor.data_timeout_seconds).unwrap_or(i64::MAX)),
        Duration::seconds(1),
    )?;
    let book = MarketDataBook::new(universe, freshness, clock);
    ReadOnlyArbitrageMonitor::new(book, left, right, monitor.min_spread_pct)
}

fn market_type_for_symbol(symbol: &Symbol) -> MarketType {
    if symbol.as_str().ends_with("-SPOT") {
        MarketType::Spot
    } else {
        MarketType::Perpetual
    }
}

fn validate_profile_identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        bail!("{label} must be a non-empty, trimmed, control-free identity of at most 128 bytes");
    }
    Ok(())
}

fn validate_grid_replay(
    events: &[MarketDataEvent],
    exchange: &str,
    symbol: &Symbol,
    market_type: MarketType,
) -> Result<()> {
    if events.is_empty() {
        bail!("grid replay must contain at least one event");
    }
    for event in events {
        let MarketDataEvent::Observation(MarketDataObservation { snapshot, .. }) = event else {
            bail!("grid replay must contain only observation events");
        };
        if snapshot.exchange() != exchange
            || snapshot.symbol != *symbol
            || snapshot.market_type != market_type
        {
            bail!("grid replay source identity drifted from {exchange}/{symbol}/{market_type:?}");
        }
    }
    Ok(())
}

fn validate_arbitrage_replay(
    events: &[MarketDataEvent],
    exchanges: &[String],
    symbol: &Symbol,
    market_type: MarketType,
) -> Result<()> {
    if events.is_empty() {
        bail!("arbitrage replay must contain at least one event");
    }
    let allowed = [exchanges[0].as_str(), exchanges[1].as_str()];
    let mut seen = [false; 2];
    for event in events {
        let MarketDataEvent::Observation(MarketDataObservation { snapshot, .. }) = event else {
            bail!("arbitrage replay must contain only observation events");
        };
        let Some(exchange_index) = allowed
            .iter()
            .position(|exchange| snapshot.exchange() == *exchange)
        else {
            bail!("arbitrage replay source identity drifted outside the configured exact pair");
        };
        if snapshot.symbol != *symbol || snapshot.market_type != market_type {
            bail!("arbitrage replay source identity drifted outside the configured exact pair");
        }
        seen[exchange_index] = true;
    }
    if !seen.into_iter().all(|present| present) {
        bail!("arbitrage replay must contain observations for both exact-pair exchanges");
    }
    Ok(())
}

fn first_observation(events: &[MarketDataEvent]) -> Option<MarketSnapshot> {
    events.iter().find_map(|event| match event {
        MarketDataEvent::Observation(observation) => Some(observation.snapshot.clone()),
        MarketDataEvent::SourceGap { .. } | MarketDataEvent::SourceUnavailable { .. } => None,
    })
}

fn filter_events(
    events: &[MarketDataEvent],
    exchange: &str,
    symbol: &Symbol,
) -> Vec<MarketDataEvent> {
    events
        .iter()
        .filter(|event| match event {
            MarketDataEvent::Observation(observation) => {
                observation.snapshot.exchange() == exchange
                    && observation.snapshot.symbol == *symbol
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
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{paper_exchange, validate_grid_replay, validate_profile_identity};
    use crate::monitor::ReplayMarketDataClock;
    use chrono::{TimeZone, Utc};
    use crypto_trading_domain::{MarketType, Symbol};
    use crypto_trading_exchange::{ExchangeHandle, ExchangeMode};
    use crypto_trading_runtime::{MarketDataEvent, MarketDataObservation};
    use serde_json::json;
    use std::sync::Arc;

    #[tokio::test]
    async fn mirror_factory_is_always_paper_mode_even_for_live_named_exchanges() {
        let clock = Arc::new(ReplayMarketDataClock::new(
            Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap(),
        ));
        let exchange = paper_exchange("binance".to_owned(), &clock).unwrap();
        let status = exchange.status().await.unwrap();
        assert_eq!(status.mode, ExchangeMode::Paper);
    }

    #[test]
    fn grid_replay_rejects_source_identity_drift() {
        let symbol = Symbol::new("BTC-USDC-PERP").unwrap();
        let snapshot = serde_json::from_value(json!({
            "exchange": "paper-grid-alt",
            "symbol": "BTC-USDC-PERP",
            "market_type": "perpetual",
            "bid": "99",
            "ask": "100",
            "timestamp": "2026-07-25T00:00:00Z",
        }))
        .unwrap();
        let event = MarketDataEvent::Observation(
            MarketDataObservation::new(
                snapshot,
                1,
                Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap(),
            )
            .unwrap(),
        );
        let error = validate_grid_replay(&[event], "paper-grid", &symbol, MarketType::Perpetual)
            .unwrap_err();
        assert!(error.to_string().contains("source identity drifted"));
    }

    #[test]
    fn profile_identity_rejects_whitespace_controls_and_oversized_values() {
        for invalid in ["", " leading", "trailing ", "line\nbreak"] {
            assert!(validate_profile_identity(invalid, "identity").is_err());
        }
        assert!(validate_profile_identity(&"x".repeat(129), "identity").is_err());
        validate_profile_identity("paper-grid-btc", "identity").unwrap();
    }
}
