use std::{num::NonZeroUsize, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use crypto_trading_config::{
    GridConfig, MonitorConfig, load_arbitrage_config, load_exchange_auth, load_grid_config,
    load_monitor_config, load_price_alert_config, load_symbol_conversions,
    load_volume_maker_config,
};
use crypto_trading_domain::{MarketSnapshot, MarketType, OrderIntent, Price, Symbol};
use crypto_trading_exchange::{PaperExchange, SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    DecisionRecord, ExchangeRouter, ExecutionMode, IntentExecutor, JsonlHistory,
};
use crypto_trading_strategy::{
    ArbitrageDecision, ArbitrageState, ArbitrageStrategy, GridPlanner, GridState, GridStrategy,
    PairStrategyMachine, StrategyMachine,
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
        Command::Scanner(args) => {
            run_scanner(&args);
            Ok(())
        }
    }
}

#[derive(Debug)]
struct PaperExecution {
    intent_count: usize,
    receipts: Vec<TradingReceipt>,
}

async fn run_grid(args: GridArgs) -> Result<()> {
    if args.authority.live {
        ExecutionMode::live(args.authority.acknowledge_risk.as_deref())?;
        bail!(
            "live grid execution is unavailable until its exchange adapter passes signing and testnet verification"
        );
    }
    let config = load_grid_config(&args.config)
        .with_context(|| format!("failed to load grid config {}", args.config.display()))?;
    println!(
        "valid: grid {} exchange={} symbol={} mode={:?} market={:?}",
        args.config.display(),
        config.exchange,
        config.symbol,
        config.mode,
        config.market_type
    );
    if args.once && args.price.is_none() {
        bail!("--once requires --price so the paper run has an explicit snapshot");
    }
    if let Some(price) = args.price {
        let execution = execute_grid_paper(&config, price).await?;
        append_history(
            &args.history_path,
            "grid",
            config.symbol.as_str(),
            json!({
                "snapshot_price": price,
                "intent_count": execution.intent_count,
            }),
            &execution.receipts,
        )
        .await?;
        println!(
            "paper executed: {} orders at snapshot price={price}; history={}",
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
    let config = load_arbitrage_config(&args.config)
        .with_context(|| format!("failed to load arbitrage config {}", args.config.display()))?;
    let monitor = load_monitor_config(&args.monitor_config).with_context(|| {
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
    if args.behavior.once {
        let snapshots = resolve_arbitrage_snapshots(args, &monitor)?;
        let (decision, execution) = execute_arbitrage_paper(&config, snapshots).await?;
        append_history(
            &args.history_path,
            "arbitrage",
            decision.spread.buy_symbol.as_str(),
            json!({
                "kind": format!("{:?}", decision.kind).to_ascii_lowercase(),
                "segment": decision.segment,
                "spread_percent": decision.spread.percent,
                "target_quantity": decision.target_quantity,
                "intent_count": execution.intent_count,
                "buy_exchange": decision.spread.buy_exchange,
                "sell_exchange": decision.spread.sell_exchange,
            }),
            &execution.receipts,
        )
        .await?;
        println!(
            "paper executed: decision={:?} segment={} receipts={}; history={}",
            decision.kind,
            decision.segment,
            execution.receipts.len(),
            args.history_path.display()
        );
    }
    Ok(())
}

fn run_monitor(args: &MonitorArgs) -> Result<()> {
    let monitor = load_monitor_config(&args.config)
        .with_context(|| format!("failed to load monitor config {}", args.config.display()))?;
    println!(
        "valid: monitor {} exchanges={} symbols={}",
        args.config.display(),
        monitor.exchanges.len(),
        monitor.symbols.len()
    );
    Ok(())
}

fn run_volume_maker(args: &VolumeMakerArgs) -> Result<()> {
    let config = load_volume_maker_config(&args.config).with_context(|| {
        format!(
            "failed to load volume-maker config {}",
            args.config.display()
        )
    })?;
    println!(
        "valid: volume-maker {} exchange={} symbol={} mode=paper",
        args.config.display(),
        config.exchange,
        config.symbol
    );
    Ok(())
}

fn run_price_alert(args: &PriceAlertArgs) -> Result<()> {
    let config = load_price_alert_config(&args.config).with_context(|| {
        format!(
            "failed to load price-alert config {}",
            args.config.display()
        )
    })?;
    println!(
        "valid: price-alert {} exchange={} symbols={}",
        args.config.display(),
        config.exchange,
        config.symbols.len()
    );
    Ok(())
}

fn run_scanner(args: &ScannerArgs) {
    println!(
        "scanner configured: exchange={:?} duration={:?} log={:?}; live market adapter is read-only and not enabled in this build",
        args.exchange, args.duration, args.log_level
    );
}

async fn execute_grid_paper(config: &GridConfig, value: Decimal) -> Result<PaperExecution> {
    let (snapshot, intents) = plan_grid_intents(config, value)?;
    let intent_count = intents.len();
    let paper = Arc::new(PaperExchange::new(
        config.exchange.clone(),
        event_capacity(),
    )?);
    paper.publish_snapshot(snapshot).await?;
    let executor = IntentExecutor::new(paper, ExecutionMode::Paper);
    let receipts = executor.execute_all(intents).await?;
    Ok(PaperExecution {
        intent_count,
        receipts,
    })
}

fn plan_grid_intents(
    config: &GridConfig,
    value: Decimal,
) -> Result<(MarketSnapshot, Vec<OrderIntent>)> {
    let price = Price::new(value).context("paper snapshot price must not be negative")?;
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
        )?,
    ])
}

fn market_snapshot(
    exchange: &str,
    symbol: Symbol,
    bid: Decimal,
    ask: Decimal,
) -> Result<MarketSnapshot> {
    MarketSnapshot::new(
        exchange,
        symbol,
        MarketType::Perpetual,
        Price::new(bid).context("paper bid must not be negative")?,
        Price::new(ask).context("paper ask must not be negative")?,
        Utc::now(),
    )
    .map_err(Into::into)
}

async fn execute_arbitrage_paper(
    config: &crypto_trading_config::ArbitrageConfig,
    [left, right]: [MarketSnapshot; 2],
) -> Result<(ArbitrageDecision, PaperExecution)> {
    let strategy = ArbitrageStrategy::try_from(config)?;
    let decision = strategy.evaluate_pair(&ArbitrageState::default(), &left, &right)?;
    let intent_count = decision.intents.len();

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

    let mut router = ExchangeRouter::new(ExecutionMode::Paper);
    router.register(left.exchange().to_owned(), left_paper);
    router.register(right.exchange().to_owned(), right_paper);
    let receipts = router.execute_all(decision.intents.clone()).await?;

    Ok((
        decision,
        PaperExecution {
            intent_count,
            receipts,
        },
    ))
}

const fn event_capacity() -> NonZeroUsize {
    NonZeroUsize::new(256).expect("paper event capacity is non-zero")
}

async fn append_history(
    path: &Path,
    strategy: &str,
    symbol: &str,
    decision_details: Value,
    receipts: &[TradingReceipt],
) -> Result<()> {
    let history = JsonlHistory::new(path);
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: strategy.to_owned(),
            symbol: symbol.to_owned(),
            decision: "decision".to_owned(),
            details: decision_details,
        })
        .await?;
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: strategy.to_owned(),
            symbol: symbol.to_owned(),
            decision: "receipt".to_owned(),
            details: receipt_summary(receipts),
        })
        .await?;
    Ok(())
}

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
        "open": open,
        "filled": filled,
        "cancelled": cancelled,
        "already_processed": already_processed,
    })
}

fn check_configs(args: &ConfigCheckArgs) -> Result<()> {
    let mut summaries = Vec::with_capacity(args.paths.len());
    for path in &args.paths {
        let kind = detect_config_kind(path)?;
        summaries.push(serde_json::json!({
            "path": path,
            "kind": kind,
            "status": "valid",
        }));
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&summaries)?);
    } else {
        for summary in summaries {
            println!(
                "valid: {} {}",
                summary["kind"].as_str().unwrap_or("configuration"),
                summary["path"].as_str().unwrap_or_default()
            );
        }
    }
    Ok(())
}

fn detect_config_kind(path: &Path) -> Result<&'static str> {
    let body = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let document: serde_yaml::Value = serde_yaml::from_str(&body)
        .with_context(|| format!("invalid YAML in {}", path.display()))?;
    let mapping = document
        .as_mapping()
        .with_context(|| format!("{} must contain a YAML mapping", path.display()))?;

    let has = |key: &str| mapping.contains_key(serde_yaml::Value::from(key));
    if has("grid_system") || has("grid") || is_bare_grid(mapping) {
        load_grid_config(path)?;
        Ok("grid")
    } else if has("volume_maker") {
        load_volume_maker_config(path)?;
        Ok("volume-maker")
    } else if has("price_alert") {
        load_price_alert_config(path)?;
        Ok("price-alert")
    } else if is_arbitrage(mapping) {
        load_arbitrage_config(path)?;
        Ok("arbitrage")
    } else if has("exchanges") && has("symbols") {
        load_monitor_config(path)?;
        Ok("monitor")
    } else if has("symbol_mappings") || has("conversions") {
        load_symbol_conversions(path)?;
        Ok("symbol-conversion")
    } else if let Some(exchange) = exchange_auth_name(mapping) {
        load_exchange_auth(path, exchange)?;
        Ok("exchange-auth")
    } else {
        bail!("unsupported configuration schema in {}", path.display())
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
    use crypto_trading_config::load_grid_config_from_str;
    use rust_decimal::Decimal;

    use super::plan_grid_intents;

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
}
