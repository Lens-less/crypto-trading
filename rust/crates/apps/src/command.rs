use std::{
    collections::HashMap,
    error::Error,
    fmt,
    fs::File,
    io::Read,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use chrono::{Duration, Utc};
use crypto_trading_config::{
    ArbitrageConfig, GridConfig, MonitorConfig, load_arbitrage_config_from_str,
    load_exchange_auth_from_str, load_grid_config_from_str, load_monitor_config_from_str,
    load_price_alert_config_from_str, load_symbol_conversions_from_str,
    load_volume_maker_config_from_str, reject_yaml_anchors_and_aliases,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, OrderType, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{PaperExchange, SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    DecisionRecord, ExchangeRouter, ExecutionBatch, ExecutionMode, ExecutionPolicy, HistoryError,
    IntentExecutor, JsonlHistory, RuntimeError,
};
use crypto_trading_strategy::{
    AccountRiskSnapshot, ArbitrageDecision, ArbitrageState, ArbitrageStrategy, GridPlanner,
    GridState, GridStrategy, PairStrategyMachine, RiskDecision, RiskEngine, RiskLimits,
    StrategyMachine, VolumeMakerStrategy,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::cli::{
    ArbitrageArgs, Cli, Command, ConfigCheckArgs, GridArgs, MonitorArgs, PriceAlertArgs,
    ScannerArgs, VolumeMakerArgs,
};

/// Runs one parsed CLI command.
///
/// # Errors
///
/// Returns an error when configuration, authority validation, strategy
/// evaluation, or paper execution fails.
pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::ConfigCheck(args) => check_configs(&args),
        Command::Grid(args) => run_grid(args).await,
        Command::Arbitrage(args) => run_arbitrage(&args).await,
        Command::Monitor(args) => run_monitor(&args),
        Command::VolumeMaker(args) => run_volume_maker(&args),
        Command::PriceAlert(args) => run_price_alert(&args),
        Command::Scanner(args) => run_scanner(&args),
    }
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
    println!(
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
        println!(
            "paper placement simulated: {} orders at snapshot price={price}; history={}",
            execution.receipts.len(),
            args.history_path.display()
        );
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
    println!(
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
    println!(
        "paper executed: decision={:?} segment={} receipts={}; history={}",
        decision.kind,
        decision.segment,
        execution.receipts.len(),
        args.history_path.display()
    );
    Ok(())
}

fn run_monitor(args: &MonitorArgs) -> Result<()> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let monitor = load_monitor_config_from_str(&body)
        .with_context(|| format!("failed to load monitor config {}", args.config.display()))?;
    bail!(
        "continuous monitor runtime is unavailable (validated {} exchanges and {} symbols from {})",
        monitor.exchanges.len(),
        monitor.symbols.len(),
        args.config.display()
    )
}

fn run_volume_maker(args: &VolumeMakerArgs) -> Result<()> {
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
    bail!(
        "volume-maker runtime is unavailable (validated {}/{} from {})",
        config.exchange,
        config.symbol,
        args.config.display()
    )
}

fn run_price_alert(args: &PriceAlertArgs) -> Result<()> {
    let body = read_bounded_config(&args.config).map_err(anyhow::Error::msg)?;
    let config = load_price_alert_config_from_str(&body).with_context(|| {
        format!(
            "failed to load price-alert config {}",
            args.config.display()
        )
    })?;
    bail!(
        "price-alert runtime is unavailable (validated exchange={} symbols={} from {})",
        config.exchange,
        config.symbols.len(),
        args.config.display()
    )
}

fn run_scanner(args: &ScannerArgs) -> Result<()> {
    if let Some(path) = &args.config {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("failed to inspect scanner config {}", path.display()))?;
        if !metadata.is_file() {
            bail!("scanner config {} must be a file", path.display());
        }
    }
    bail!(
        "scanner runtime is unavailable (requested exchange={:?} duration={:?} log={:?})",
        args.exchange,
        args.duration,
        args.log_level
    )
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
const MAX_CONFIG_FILE_BYTES: usize = 1_048_576;
const MAX_CONFIG_CHECK_SUMMARIES: usize = 512;
const MAX_CONFIG_CHECK_OUTPUT_BYTES: usize = 1_048_576;
const MAX_CONFIG_CHECK_TERMINAL_RESERVE_BYTES: usize = 16_384;
const MAX_CONFIG_PATH_BYTES: usize = 1_024;
const MAX_CONFIG_MESSAGE_BYTES: usize = 2_048;
const MAX_CONFIG_DETAIL_BYTES: usize = 8_192;
const MAX_CONFIG_SCHEMA_ISSUES: usize = 64;
const MAX_CONFIG_SCHEMA_ISSUE_BYTES: usize = 512;

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
        let summary = json!({
            "path": path.map_or_else(String::new, bounded_path),
            "kind": "configuration",
            "classification": "unsupported",
            "status": "error",
            "error": "configuration check stopped before inspecting all paths because the summary count or output byte budget was exhausted",
        });
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
    json!({
        "path": bounded_path(path),
        "kind": "configuration",
        "classification": "unsupported",
        "status": "error",
        "error": bounded_text(error, MAX_CONFIG_MESSAGE_BYTES),
    })
}

fn inspect_config(path: &Path) -> Value {
    match inspect_config_inner(path) {
        Ok(summary) => summary,
        Err((kind, error)) => json!({
            "path": bounded_path(path),
            "kind": kind,
            "classification": "unsupported",
            "status": "error",
            "error": bounded_text(&error, MAX_CONFIG_MESSAGE_BYTES),
        }),
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
    summary["error"] = Value::from(bounded_text(error, MAX_CONFIG_MESSAGE_BYTES));
    summary
}

fn inspect_config_inner(path: &Path) -> Result<Value, (&'static str, String)> {
    let body = read_bounded_config(path).map_err(|error| ("configuration", error))?;
    let document: serde_yaml::Value = serde_yaml::from_str(&body)
        .map_err(|error| ("configuration", format!("invalid YAML: {error}")))?;
    let auxiliary_kind = auxiliary_config_filename_kind(path);
    let mapping = document.as_mapping().ok_or((
        "configuration",
        "configuration must contain a YAML mapping".to_owned(),
    ))?;

    let has = |key: &str| mapping.contains_key(serde_yaml::Value::from(key));
    let summary = if has("grid_system") || has("grid") || is_bare_grid(mapping) {
        let config =
            load_grid_config_from_str(&body).map_err(|error| ("grid", error.to_string()))?;
        GridPlanner::try_from(&config).map_err(|error| ("grid", error.to_string()))?;
        let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Grid, &document);
        if issues.is_empty() {
            Ok(config_summary(path, "grid", "runtime-executable", None))
        } else {
            let detail = bounded_issue_detail(
                "paper one-shot rejects ignored or unknown runtime keys: ",
                &issues,
            );
            Ok(config_summary(
                path,
                "grid",
                "legacy-parseable",
                Some(&detail),
            ))
        }
    } else if has("volume_maker") {
        let config = load_volume_maker_config_from_str(&body)
            .map_err(|error| ("volume-maker", error.to_string()))?;
        let detail = if let Err(error) = config.validate_execution_controls() {
            error.to_string()
        } else {
            VolumeMakerStrategy::try_from(&config)
                .map_err(|error| ("volume-maker", error.to_string()))?;
            "runtime command is unavailable".to_owned()
        };
        Ok(config_summary(
            path,
            "volume-maker",
            "legacy-parseable",
            Some(&detail),
        ))
    } else if has("price_alert") {
        load_price_alert_config_from_str(&body)
            .map_err(|error| ("price-alert", error.to_string()))?;
        Ok(config_summary(
            path,
            "price-alert",
            "legacy-parseable",
            Some("runtime command is unavailable"),
        ))
    } else if is_arbitrage(mapping) {
        inspect_arbitrage_config(path, &body, &document)
    } else if has("exchanges") && has("symbols") {
        load_monitor_config_from_str(&body).map_err(|error| ("monitor", error.to_string()))?;
        let issues = paper_runtime_schema_issues(PaperRuntimeSchema::Monitor, &document);
        if issues.is_empty() {
            Ok(config_summary(
                path,
                "monitor",
                "auxiliary",
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
                "legacy-parseable",
                Some(&detail),
            ))
        }
    } else if has("symbol_mappings") || has("conversions") {
        load_symbol_conversions_from_str(&body)
            .map_err(|error| ("symbol-conversion", error.to_string()))?;
        Ok(config_summary(path, "symbol-conversion", "auxiliary", None))
    } else if let Some(exchange) = exchange_auth_name(mapping) {
        load_exchange_auth_from_str(exchange, &body)
            .map_err(|error| ("exchange-auth", error.to_string()))?;
        Ok(config_summary(
            path,
            "exchange-auth",
            "legacy-parseable",
            Some("private live adapters are unavailable"),
        ))
    } else if let Some(kind) = auxiliary_config_kind(path, &document) {
        Ok(config_summary(path, kind, "auxiliary", None))
    } else {
        Err((
            "configuration",
            String::from("unsupported configuration schema"),
        ))
    }?;

    Ok(reject_auxiliary_filename_mismatch(summary, auxiliary_kind))
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
) -> Result<Value, (&'static str, String)> {
    let config =
        load_arbitrage_config_from_str(body).map_err(|error| ("arbitrage", error.to_string()))?;
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
            "legacy-parseable",
            Some(&detail),
        ));
    }
    if let Err(error) = config.validate_execution_controls() {
        return Ok(config_summary(
            path,
            "arbitrage",
            "legacy-parseable",
            Some(&error.to_string()),
        ));
    }
    if enabled_keys.is_empty() {
        return Ok(config_summary(
            path,
            "arbitrage",
            "legacy-parseable",
            Some("no enabled symbol_configs strategy key"),
        ));
    }

    let mut missing_position_limit_keys = Vec::new();
    for (key, profile) in &config.symbol_configs {
        if profile.enabled {
            let effective = config
                .resolve_for_strategy(key)
                .map_err(|error| ("arbitrage", error.to_string()))?;
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
            "legacy-parseable",
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
        "runtime-executable",
        Some(&detail),
    ))
}

fn config_summary(
    path: &Path,
    kind: &'static str,
    classification: &'static str,
    detail: Option<&str>,
) -> Value {
    json!({
        "path": bounded_path(path),
        "kind": kind,
        "classification": classification,
        "status": "ok",
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

fn read_bounded_config(path: &Path) -> std::result::Result<String, String> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        bounded_text(
            &format!("failed to inspect: {error}"),
            MAX_CONFIG_MESSAGE_BYTES,
        )
    })?;
    if metadata.len() > u64::try_from(MAX_CONFIG_FILE_BYTES).unwrap_or(u64::MAX) {
        return Err(format!(
            "configuration file has {} bytes; maximum is {MAX_CONFIG_FILE_BYTES}",
            metadata.len()
        ));
    }

    let file = File::open(path).map_err(|error| {
        bounded_text(
            &format!("failed to read: {error}"),
            MAX_CONFIG_MESSAGE_BYTES,
        )
    })?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(MAX_CONFIG_FILE_BYTES)
            .min(MAX_CONFIG_FILE_BYTES)
            .saturating_add(1),
    );
    file.take(u64::try_from(MAX_CONFIG_FILE_BYTES).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            bounded_text(
                &format!("failed to read: {error}"),
                MAX_CONFIG_MESSAGE_BYTES,
            )
        })?;
    if bytes.len() > MAX_CONFIG_FILE_BYTES {
        return Err(format!(
            "configuration file exceeded the {MAX_CONFIG_FILE_BYTES}-byte read limit"
        ));
    }
    let body = String::from_utf8(bytes).map_err(|error| {
        bounded_text(
            &format!("configuration is not valid UTF-8: {error}"),
            MAX_CONFIG_MESSAGE_BYTES,
        )
    })?;
    reject_yaml_anchors_and_aliases(&body)
        .map_err(|error| bounded_text(&error.to_string(), MAX_CONFIG_MESSAGE_BYTES))?;
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
            mapping_with_keys(value, &["data_timeout"], "health_check", issues);
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::{TimeZone, Utc};
    use crypto_trading_config::load_grid_config_from_str;
    use crypto_trading_domain::{
        MarketType, Money, Order, OrderIntent, OrderStatus, Position, PositionSide, Quantity, Side,
        Symbol,
    };
    use crypto_trading_exchange::{
        ExchangeError, ReconcileReceipt, ReconcileScope, SubmissionDisposition, TradingReceipt,
    };
    use crypto_trading_runtime::{
        ExecutionBatch, JsonlHistory, ReconciliationObservation, RuntimeError,
    };
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::{
        ConfigCheckReport, ConfigDiscovery, ExecutionOutcomeJournalError, MAX_CONFIG_CHECK_ENTRIES,
        MAX_CONFIG_CHECK_ERRORS, MAX_CONFIG_CHECK_OUTPUT_BYTES, MAX_CONFIG_CHECK_SUMMARIES,
        MAX_CONFIG_DETAIL_BYTES, MAX_CONFIG_FILE_BYTES, MAX_CONFIG_SCHEMA_ISSUE_BYTES,
        MAX_CONFIG_SCHEMA_ISSUES, MAX_RECEIPT_SUMMARY_RECEIPTS, MAX_RECONCILIATION_SUMMARY_ORDERS,
        MAX_RECONCILIATION_SUMMARY_POSITIONS, PaperRuntimeSchema, PreservedExecutionOutcome,
        append_execution_planned, auxiliary_config_kind, bounded_issue_detail,
        collect_config_report, config_summary, execution_batch, execution_error_summary,
        finish_arbitrage_execution, finish_execution, inspect_config, paper_runtime_schema_issues,
        plan_grid_intents, receipt_summary, reject_yaml_anchors_and_aliases, render_config_summary,
    };

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
        std::fs::write(&path, vec![b' '; MAX_CONFIG_FILE_BYTES + 1]).unwrap();

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
                "legacy-parseable",
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
