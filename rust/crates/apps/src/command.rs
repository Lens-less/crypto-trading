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
    ArbitrageConfig, GridConfig, MonitorConfig, load_account_risk_config,
    load_account_risk_config_from_str, load_arbitrage_config_from_str, load_exchange_auth_from_str,
    load_grid_config_from_str, load_monitor_config_from_str, load_symbol_conversions_from_str,
    read_bounded_config,
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
    BinanceExchangeInfoSymbol, BinanceHmacSha256Signer, BinanceProduct, BinancePublicExchange,
    BinanceRequestSigner, BinanceSpotMarketStreamEndpoint, BinanceSpotUserDataStreamEndpoint,
    BinanceTestnetEndpoints, BinanceTestnetExchange, BinanceTestnetProtocol, ExchangeError,
    ExchangeHandle, ExchangeSymbol, ExchangeSymbolCatalog, HyperliquidPublicEndpoint,
    HyperliquidPublicExchange, InstrumentRuleCatalog, PaperExchange, ReconcileScope,
    RemoteHttpTransport, ReqwestHttpTransport, SubmissionDisposition, TradingReceipt,
    hyperliquid_usdt_symbol_catalog,
};
use crypto_trading_runtime::{
    BinanceBookTickerStreamSource, BinancePollingRoute, BinancePublicPollingSource,
    BinanceUserDataStreamItem, BinanceUserDataStreamSource, DecisionRecord,
    DeterministicMarketDataAdapter, ExchangeRouter, ExecutionBatch, ExecutionMode, ExecutionPolicy,
    HistoryError, HyperliquidPollingRoute, HyperliquidPublicPollingSource, IntentExecutor,
    JournalReadError, JsonlHistory, MAX_HISTORY_RECORD_BYTES,
    MAX_MARKET_SUPERVISOR_BUFFERED_EVENTS, MarketDataBook, MarketDataError, MarketDataEvent,
    MarketDataEventFuture, MarketDataEventSource, MarketDataSourceFailure, MarketInstrument,
    MarketPollingPolicy, MarketStreamReconnectPolicy, MarketSupervisorConfig, MarketUniverse,
    PaperAccountAuthority, PaperAccountConfig, ProductionMarketStreamJitter, ReadOnlyTaskExit,
    ReadOnlyTaskFailure, ReadOnlyTaskKind, ReadOnlyTaskPhase, ReadOnlyTaskReadModel,
    ReadOnlyTaskRecovery, RuntimeError, SpreadHistoryWriter, SystemMarketDataClock,
    TokioMarketStreamSleeper, TokioTextWebSocketConnector, current_capability_manifest,
    read_journal_chain,
};
use crypto_trading_strategy::{
    AccountRiskSnapshot, ArbitrageDecision, ArbitrageState, ArbitrageStrategy, GridPlanner,
    GridState, GridStrategy, PairStrategyMachine, RiskDecision, RiskEngine, RiskLimits,
    StrategyMachine,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, timeout_at},
};

use crate::cli::{
    ArbitrageArgs, CapabilitiesArgs, Cli, Command, ConfigCheckArgs, GridArgs, MonitorArgs,
    MonitorLiveTransport, MonitorMode, PaperBarArgs, PaperBarStrategyArgs, PaperCommand,
    PaperMutationArgs, PaperOperation, PaperStartArgs, PaperStatusArgs, PaperTaskArgs,
    TestnetLifecycleArgs, TestnetLifecycleExpected, TestnetLifecycleMarket, TestnetLifecycleSide,
    TestnetLifecycleTimeInForce, TestnetReconciliationArgs, TestnetSmokeArgs, TestnetSoakArgs,
    TestnetSoakMode,
};
use crate::continuous_monitor::{
    ContinuousMonitorTask, ContinuousMonitorTaskConfig, ContinuousMonitorTaskExit,
    ContinuousMonitorTaskStatus,
};
use crate::continuous_testnet::{
    ContinuousTestnetOwner, ContinuousTestnetOwnerError, ContinuousTestnetOwnerPhase,
    ContinuousTestnetUserDataOutcome,
};
use crate::monitor::{
    ArbitrageMonitorOutcome, ReadOnlyArbitrageMonitor, ReplayMarketDataClock,
    freshness_policy_from_monitor_config, load_market_snapshot_replay,
};
use crate::paper_bar_task::{PaperBarAction, PaperBarTask, PaperBarTaskState};
use crate::shutdown::{ShutdownSignalFuture, install_shutdown_signal};
use crate::task_host::{
    TaskHostControlCommand, TaskHostControlError, TaskHostServeError, TaskHostServeOutcome,
    control_addr, ensure_control_token_configured, query_control, serve_host_with_shutdown,
};
use crate::testnet_lifecycle::{
    TESTNET_LIFECYCLE_ACKNOWLEDGEMENT, TestnetLifecycleConfig, TestnetLifecycleObservation,
    TestnetLifecycleRecoveryState, TestnetLifecycleReport, run_testnet_lifecycle,
    testnet_lifecycle_recovery_state, testnet_lifecycle_requires_submission,
    testnet_lifecycle_wire_symbol,
};
use crate::testnet_reconciliation::{
    TESTNET_RECONCILIATION_APPLY_ACKNOWLEDGEMENT, TestnetReconciliationConfig,
    TestnetReconciliationPlan, TestnetReconciliationReport, product_label,
};
use crate::testnet_soak::{
    MAX_TESTNET_SOAK_EVIDENCE_RECORDS, TESTNET_SOAK_SCHEMA_VERSION, TESTNET_SOAK_TASK_KIND,
    TestnetSoakEvidenceRequirements, TestnetSoakProbe, TestnetSoakProbeFailure,
    TestnetSoakProbeFuture, TestnetSoakSample, TestnetSoakTask, TestnetSoakTaskConfig,
    TestnetSoakTaskError, TestnetSoakTaskExit, TestnetSoakTaskFailure, TestnetSoakTaskStatus,
    verify_testnet_soak_evidence,
};
use crypto_trading_runtime::{
    AccountRiskAdmission, AccountRiskAuthority, AccountRiskCandidate, PaperCostModel,
    PaperReservationLeg, PaperReservationRequest,
};
use crypto_trading_strategy::{
    AccountRiskPolicy, Bar, BarStrategy, BuyAndHoldStrategy, CappedVolatilityTarget, CashStrategy,
    LongOnlyDonchian, SlowTimeSeriesMomentum, TargetExposure,
};

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
        Command::PaperBar(args) => run_paper_bar(&args).await,
        Command::Paper(args) => run_paper(args).await,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnedPaperBarStrategy {
    Cash(CashStrategy),
    BuyAndHold(BuyAndHoldStrategy),
    SlowTimeSeriesMomentum(SlowTimeSeriesMomentum),
    LongOnlyDonchian(LongOnlyDonchian),
    CappedVolatilityTarget(CappedVolatilityTarget),
}

impl BarStrategy for OwnedPaperBarStrategy {
    fn target_exposure(
        &mut self,
        context: &crypto_trading_strategy::BarStrategyContext<'_>,
    ) -> std::result::Result<TargetExposure, crypto_trading_strategy::StrategyError> {
        match self {
            Self::Cash(strategy) => strategy.target_exposure(context),
            Self::BuyAndHold(strategy) => strategy.target_exposure(context),
            Self::SlowTimeSeriesMomentum(strategy) => strategy.target_exposure(context),
            Self::LongOnlyDonchian(strategy) => strategy.target_exposure(context),
            Self::CappedVolatilityTarget(strategy) => strategy.target_exposure(context),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PaperBarFillModel {
    cost_model: PaperCostModel,
    impact_bps: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct PaperBarPosition {
    current_target: TargetExposure,
    position_quantity: Decimal,
    equity: Money,
}

#[allow(clippy::too_many_lines)]
async fn run_paper_bar(args: &PaperBarArgs) -> Result<()> {
    ensure_paper_bar_history_is_fresh(&args.history_path)?;
    let warmup_bars = load_paper_bar_bars(args.warmup_bars_csv.as_deref())?;
    let bars = load_paper_bar_bars(Some(&args.bars_csv))?;
    let symbol =
        Symbol::new(&args.symbol).with_context(|| format!("invalid symbol {}", args.symbol))?;
    if market_type_for_one_shot_symbol(&symbol) != MarketType::Spot {
        bail!("paper-bar currently supports spot symbols only");
    }
    let strategy = build_paper_bar_strategy(&args.strategy)
        .context("paper-bar strategy parameters are invalid")?;
    let fill_model = build_paper_bar_fill_model(args)?;
    let history = JsonlHistory::new(&args.history_path);
    let account = PaperAccountAuthority::planned(
        history.clone(),
        PaperAccountConfig::new(
            format!("paper-bar:{}", args.task_id),
            Money::new(args.initial_available),
        )
        .map_err(anyhow::Error::new)?,
    )
    .map_err(anyhow::Error::new)
    .context("failed to plan the paper-bar account")?;
    let account_risk = build_paper_bar_account_risk(args, &account, &history)?;
    account
        .ensure_initialized()
        .await
        .map_err(anyhow::Error::new)
        .context("failed to initialize the paper-bar account")?;

    let warmup_start_bar_index = args
        .start_bar_index
        .checked_sub(warmup_bars.len())
        .context("paper-bar warmup bars exceed the configured start_bar_index")?;
    let mut task = PaperBarTask::with_state(
        strategy,
        PaperBarTaskState {
            next_bar_index: warmup_start_bar_index,
            current_target: TargetExposure::ZERO,
        },
    );
    let mut pending_target =
        warmup_paper_bar_task(&mut task, &warmup_bars).context("paper-bar warmup failed")?;
    // Match the causal evaluator exactly: `current_target` is the achieved
    // exposure immediately after the most recent rebalance. It is not
    // re-marked on every later bar unless another rebalance is attempted.
    let mut current_target = TargetExposure::ZERO;
    let mut execution_count = 0_u64;

    for bar in &bars {
        if let Some(target) = pending_target.take()
            && target != current_target
        {
            if execute_paper_bar_rebalance(
                &args.task_id,
                &symbol,
                args.exchange.as_str(),
                &account,
                account_risk.as_ref(),
                &history,
                fill_model,
                bar.open,
                bar.open_time,
                target,
            )
            .await?
            {
                execution_count = execution_count
                    .checked_add(1)
                    .context("paper-bar execution count overflowed")?;
            }
            current_target = paper_bar_position(&account, bar.open).await?.current_target;
        }
        let decision = task
            .on_bar_with_current_target(bar.clone(), current_target)
            .map_err(anyhow::Error::new)
            .context("paper-bar strategy evaluation failed")?;
        history
            .append(&DecisionRecord {
                timestamp: decision.decided_at,
                strategy: "paper_bar".to_owned(),
                symbol: symbol.to_string(),
                decision: "paper_bar_decided".to_owned(),
                details: json!({
                    "schema_version": 1,
                    "task_id": args.task_id,
                    "bar_index": decision.bar_index,
                    "decided_at": decision.decided_at,
                    "close": bar.close,
                    "current_target": current_target.as_decimal(),
                    "target": decision.target.as_decimal(),
                    "action": paper_bar_action_json(&decision.action),
                }),
            })
            .await
            .context("failed to append paper-bar decision")?;
        pending_target = Some(decision.target);
    }

    let terminal_bar = bars.last().context("paper-bar requires at least one bar")?;
    if execute_paper_bar_rebalance(
        &args.task_id,
        &symbol,
        args.exchange.as_str(),
        &account,
        account_risk.as_ref(),
        &history,
        fill_model,
        terminal_bar.close,
        terminal_bar.close_time,
        TargetExposure::ZERO,
    )
    .await?
    {
        execution_count = execution_count
            .checked_add(1)
            .context("paper-bar execution count overflowed")?;
    }

    let final_position = paper_bar_position(&account, terminal_bar.close).await?;
    let snapshot = account
        .decision_snapshot()
        .await
        .map_err(anyhow::Error::new)
        .context("failed to load final paper-bar account snapshot")?;
    println!(
        "paper-bar complete: task_id={} bars={} executions={} final_target={} available={} settled_equity_base={} committed_exposure={}",
        args.task_id,
        bars.len(),
        execution_count,
        final_position.current_target.as_decimal(),
        snapshot.available,
        snapshot.settled_equity_base,
        snapshot.committed_exposure,
    );
    Ok(())
}

fn build_paper_bar_strategy(args: &PaperBarStrategyArgs) -> Result<OwnedPaperBarStrategy> {
    Ok(match args {
        PaperBarStrategyArgs::Cash => OwnedPaperBarStrategy::Cash(CashStrategy),
        PaperBarStrategyArgs::BuyAndHold => {
            OwnedPaperBarStrategy::BuyAndHold(BuyAndHoldStrategy::default())
        }
        PaperBarStrategyArgs::SlowTimeSeriesMomentum {
            lookback_bars,
            rebalance_every_bars,
        } => OwnedPaperBarStrategy::SlowTimeSeriesMomentum(
            SlowTimeSeriesMomentum::new(*lookback_bars, *rebalance_every_bars)
                .map_err(anyhow::Error::new)?,
        ),
        PaperBarStrategyArgs::LongOnlyDonchian { lookback_bars } => {
            OwnedPaperBarStrategy::LongOnlyDonchian(
                LongOnlyDonchian::new(*lookback_bars).map_err(anyhow::Error::new)?,
            )
        }
        PaperBarStrategyArgs::CappedVolatilityTarget {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
            periods_per_year,
        } => OwnedPaperBarStrategy::CappedVolatilityTarget(
            match periods_per_year {
                Some(periods_per_year) => CappedVolatilityTarget::new_with_periods_per_year(
                    *lookback_returns,
                    *annual_target,
                    *rebalance_band,
                    *rebalance_every_bars,
                    *periods_per_year,
                ),
                None => CappedVolatilityTarget::new(
                    *lookback_returns,
                    *annual_target,
                    *rebalance_band,
                    *rebalance_every_bars,
                ),
            }
            .map_err(anyhow::Error::new)?,
        ),
    })
}

fn build_paper_bar_fill_model(args: &PaperBarArgs) -> Result<PaperBarFillModel> {
    let impact_bps = Decimal::from(args.half_spread_bps)
        .checked_add(Decimal::from(args.slippage_bps))
        .and_then(|value| value.checked_add(Decimal::from(args.latency_bps)))
        .context("paper-bar impact basis points overflowed")?;
    Ok(PaperBarFillModel {
        cost_model: PaperCostModel::v1(args.fee_bps, args.funding_buffer_bps, args.slippage_bps)
            .map_err(anyhow::Error::new)?,
        impact_bps,
    })
}

fn build_paper_bar_account_risk(
    args: &PaperBarArgs,
    account: &PaperAccountAuthority,
    history: &JsonlHistory,
) -> Result<Option<AccountRiskAuthority>> {
    let Some(path) = args.paper_account_risk_config.as_ref() else {
        return Ok(None);
    };
    let config = load_account_risk_config(path).with_context(|| {
        format!(
            "failed to load paper-bar account risk config {}",
            path.display()
        )
    })?;
    let policy = AccountRiskPolicy::try_from(&config).with_context(|| {
        format!(
            "failed to validate paper-bar account risk config {}",
            path.display()
        )
    })?;
    AccountRiskAuthority::new(account.journal_id(), history.clone(), "paper", policy)
        .map(Some)
        .map_err(anyhow::Error::new)
}

fn ensure_paper_bar_history_is_fresh(history_path: &Path) -> Result<()> {
    match std::fs::metadata(history_path) {
        Ok(metadata) if metadata.len() > 0 => {
            bail!(
                "paper-bar does not support journal recovery; refuse to reuse non-empty history {}",
                history_path.display()
            );
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect paper-bar history path {}",
                history_path.display()
            )
        }),
    }
}

fn load_paper_bar_bars(path: Option<&Path>) -> Result<Vec<Bar>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let body = read_bounded_config(path)
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("failed to read bar CSV {}", path.display()))?;
    parse_paper_bar_csv(&body).with_context(|| format!("failed to parse {}", path.display()))
}

fn warmup_paper_bar_task<S>(
    task: &mut PaperBarTask<S>,
    warmup_bars: &[Bar],
) -> Result<Option<TargetExposure>>
where
    S: BarStrategy,
{
    let mut pending_target = None;
    for bar in warmup_bars {
        let decision = task
            .on_bar_with_current_target(bar.clone(), TargetExposure::ZERO)
            .map_err(anyhow::Error::new)?;
        pending_target = Some(decision.target);
    }
    Ok(pending_target)
}

fn parse_paper_bar_csv(body: &str) -> Result<Vec<Bar>> {
    let mut bars = Vec::new();
    for (line_number, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if line_number == 0
            && fields
                .first()
                .is_some_and(|value| value.parse::<i64>().is_err())
        {
            continue;
        }
        if fields.len() < 9 {
            bail!(
                "bar CSV line {} has {} columns; expected at least 9",
                line_number + 1,
                fields.len()
            );
        }
        bars.push(Bar::new(
            parse_bar_timestamp(fields[0])
                .with_context(|| format!("line {} has an invalid open_time", line_number + 1))?,
            parse_bar_timestamp(fields[6])
                .with_context(|| format!("line {} has an invalid close_time", line_number + 1))?,
            Price::new(fields[1].parse::<Decimal>()?)?,
            Price::new(fields[2].parse::<Decimal>()?)?,
            Price::new(fields[3].parse::<Decimal>()?)?,
            Price::new(fields[4].parse::<Decimal>()?)?,
            fields[5].parse::<Decimal>()?,
            fields[7].parse::<Decimal>()?,
            fields[8].parse::<u64>()?,
        )?);
    }
    if bars.is_empty() {
        bail!("bar CSV did not contain any closed bars");
    }
    Ok(bars)
}

fn parse_bar_timestamp(value: &str) -> Result<chrono::DateTime<Utc>> {
    let raw = value.parse::<i64>()?;
    if let Some(timestamp) = chrono::DateTime::<Utc>::from_timestamp_millis(raw) {
        return Ok(timestamp);
    }
    chrono::DateTime::<Utc>::from_timestamp_micros(raw)
        .context("timestamp is outside chrono's supported range")
}

async fn paper_bar_position(
    account: &PaperAccountAuthority,
    mark: Price,
) -> Result<PaperBarPosition> {
    let snapshot = account
        .decision_snapshot()
        .await
        .map_err(anyhow::Error::new)
        .context("failed to read paper-bar account state")?;
    let position_quantity = snapshot
        .open_lots
        .iter()
        .try_fold(Decimal::ZERO, |total, lot| {
            total.checked_add(lot.remaining_quantity.as_decimal())
        })
        .context("paper-bar position quantity overflowed")?;
    let marked_notional = position_quantity
        .checked_mul(mark.as_decimal())
        .map(Money::new)
        .context("paper-bar marked notional overflowed")?;
    let equity = snapshot
        .available
        .as_decimal()
        .checked_add(marked_notional.as_decimal())
        .map(Money::new)
        .context("paper-bar equity overflowed")?;
    let current_target =
        if equity.as_decimal() <= Decimal::ZERO || marked_notional <= Money::default() {
            TargetExposure::ZERO
        } else {
            TargetExposure::new(
                marked_notional
                    .as_decimal()
                    .checked_div(equity.as_decimal())
                    .context("paper-bar target ratio overflowed")?,
            )
            .map_err(anyhow::Error::new)?
        };
    Ok(PaperBarPosition {
        current_target,
        position_quantity,
        equity,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn execute_paper_bar_rebalance(
    owner_task_id: &str,
    symbol: &Symbol,
    exchange: &str,
    account: &PaperAccountAuthority,
    account_risk: Option<&AccountRiskAuthority>,
    history: &JsonlHistory,
    fill_model: PaperBarFillModel,
    reference_price: Price,
    occurred_at: chrono::DateTime<Utc>,
    target_exposure: TargetExposure,
) -> Result<bool> {
    let position = paper_bar_position(account, reference_price).await?;
    let target_notional = position
        .equity
        .as_decimal()
        .max(Decimal::ZERO)
        .checked_mul(target_exposure.as_decimal())
        .context("paper-bar target notional overflowed")?;
    let target_quantity = target_notional
        .checked_div(reference_price.as_decimal())
        .context("paper-bar target quantity overflowed")?;
    let delta = target_quantity
        .checked_sub(position.position_quantity)
        .context("paper-bar quantity delta overflowed")?;
    if delta.is_zero() {
        return Ok(false);
    }

    let (side, quantity) = if delta > Decimal::ZERO {
        let affordable = maximum_affordable_quantity(
            account,
            reference_price,
            fill_model.impact_bps,
            fill_model.cost_model.fee_bps(),
        )
        .await?;
        let quantity = delta.min(affordable);
        if quantity <= Decimal::ZERO {
            return Ok(false);
        }
        (Side::Buy, Quantity::new(quantity)?)
    } else {
        (Side::Sell, Quantity::new(delta.abs())?)
    };
    let fill_price = synthetic_fill_price(reference_price, side, fill_model.impact_bps)?;
    let fill_notional = fill_price
        .as_decimal()
        .checked_mul(quantity.as_decimal())
        .map(Money::new)
        .context("paper-bar fill notional overflowed")?;
    let reference_notional = reference_price
        .as_decimal()
        .checked_mul(quantity.as_decimal())
        .map(Money::new)
        .context("paper-bar reference notional overflowed")?;
    let reserved_notional = if fill_notional > reference_notional {
        fill_notional
    } else {
        reference_notional
    };

    let admission_ticket = if side == Side::Buy {
        admit_paper_bar_entry(
            account_risk,
            owner_task_id,
            symbol.as_str(),
            reserved_notional,
            occurred_at,
        )
        .await?
    } else {
        None
    };
    if side == Side::Buy && account_risk.is_some() && admission_ticket.is_none() {
        history
            .append(&DecisionRecord {
                timestamp: occurred_at,
                strategy: "paper_bar".to_owned(),
                symbol: symbol.to_string(),
                decision: "paper_bar_risk_rejected".to_owned(),
                details: json!({
                    "schema_version": 1,
                    "task_id": owner_task_id,
                    "target": target_exposure.as_decimal(),
                    "reference_price": reference_price,
                    "reserved_notional": reserved_notional,
                }),
            })
            .await
            .context("failed to append paper-bar risk rejection")?;
        return Ok(false);
    }

    let mut intent = OrderIntent::market(
        exchange.to_owned(),
        symbol.clone(),
        MarketType::Spot,
        side,
        quantity,
    );
    if side == Side::Sell {
        intent.reduce_only = true;
    }
    let batch = ExecutionBatch::planned(vec![intent.clone()])?;
    let operation_task_id = format!("{owner_task_id}/op/{}", occurred_at.timestamp_millis());
    let idempotency_key = format!(
        "paper-bar:{}:{}:{}",
        occurred_at.timestamp_millis(),
        side_name(side),
        target_exposure.as_decimal(),
    );
    let mut reservation = PaperReservationRequest::planned(
        operation_task_id,
        idempotency_key,
        batch.id(),
        fill_model.cost_model,
        vec![
            PaperReservationLeg::from_intent(0, &intent, reserved_notional)
                .map_err(anyhow::Error::new)?,
        ],
    )
    .map_err(anyhow::Error::new)?;
    if let Some((risk, ticket)) = account_risk.zip(admission_ticket.as_ref()) {
        reservation = reservation
            .with_account_risk_admission(risk.scope_id(), ticket)
            .map_err(anyhow::Error::new)?;
    }
    let request = crate::paper_single_leg_saga::PaperSingleLegRequest::new(
        symbol.clone(),
        batch.clone(),
        reservation,
    )
    .map_err(anyhow::Error::new)?;
    let saga = crate::paper_single_leg_saga::DurablePaperSingleLegSaga::new(
        account.clone(),
        history.clone(),
    )
    .map_err(anyhow::Error::new)?
    .with_strategy_label("paper_bar");
    let receipt = synthetic_receipt(&intent, occurred_at, fill_price);
    match saga
        .run(request, |_| async move { Ok(vec![receipt]) })
        .await
        .map_err(anyhow::Error::new)?
    {
        crate::paper_single_leg_saga::PaperSingleLegRun::Completed { .. } => {}
        crate::paper_single_leg_saga::PaperSingleLegRun::Cancelled { .. } => {
            bail!("paper-bar synthetic execution cancelled unexpectedly");
        }
        crate::paper_single_leg_saga::PaperSingleLegRun::AlreadyTerminal { .. } => {
            bail!("paper-bar synthetic execution hit an existing terminal reservation");
        }
    }

    let updated = paper_bar_position(account, reference_price).await?;
    if side == Side::Sell
        && updated.position_quantity.is_zero()
        && let Some(risk) = account_risk
    {
        risk.record_position_closed(owner_task_id, occurred_at)
            .await
            .map_err(anyhow::Error::new)
            .context("failed to record paper-bar position close")?;
    }
    Ok(true)
}

async fn admit_paper_bar_entry(
    account_risk: Option<&AccountRiskAuthority>,
    task_id: &str,
    symbol: &str,
    notional: Money,
    observed_at: chrono::DateTime<Utc>,
) -> Result<Option<crypto_trading_runtime::AccountRiskAdmissionTicket>> {
    let Some(risk) = account_risk else {
        return Ok(None);
    };
    let candidate =
        AccountRiskCandidate::new(task_id, symbol, notional).map_err(anyhow::Error::new)?;
    match risk
        .admit(&candidate, observed_at)
        .await
        .map_err(anyhow::Error::new)?
    {
        AccountRiskAdmission::Admitted { ticket, .. } => Ok(Some(ticket)),
        AccountRiskAdmission::Rejected(_) => Ok(None),
    }
}

async fn maximum_affordable_quantity(
    account: &PaperAccountAuthority,
    reference_price: Price,
    impact_bps: Decimal,
    fee_bps: u32,
) -> Result<Decimal> {
    let snapshot = account
        .decision_snapshot()
        .await
        .map_err(anyhow::Error::new)
        .context("failed to read paper-bar buying power")?;
    let impact = impact_bps
        .checked_div(Decimal::from(10_000_u32))
        .context("paper-bar impact division overflowed")?;
    let fill_price = reference_price
        .as_decimal()
        .checked_mul(
            Decimal::ONE
                .checked_add(impact)
                .context("paper-bar fill impact overflowed")?,
        )
        .context("paper-bar fill price overflowed")?;
    let fee_rate = Decimal::from(fee_bps)
        .checked_div(Decimal::from(10_000_u32))
        .context("paper-bar fee division overflowed")?;
    let cost_per_unit = fill_price
        .checked_mul(
            Decimal::ONE
                .checked_add(fee_rate)
                .context("paper-bar fee rate overflowed")?,
        )
        .context("paper-bar cost per unit overflowed")?;
    let mut quantity = snapshot
        .available
        .as_decimal()
        .checked_div(cost_per_unit)
        .context("paper-bar affordable quantity overflowed")?;
    let representable_step = Decimal::new(1, quantity.scale());
    for _ in 0..=2 {
        let required = required_buying_power(fill_price, fee_rate, quantity)?;
        if required <= snapshot.available.as_decimal() {
            return Ok(quantity.max(Decimal::ZERO));
        }
        quantity = quantity
            .checked_sub(representable_step)
            .context("paper-bar affordable quantity underflowed")?;
    }
    bail!("paper-bar buying power could not be represented safely")
}

fn required_buying_power(
    fill_price: Decimal,
    fee_rate: Decimal,
    quantity: Decimal,
) -> Result<Decimal> {
    let notional = fill_price
        .checked_mul(quantity)
        .context("paper-bar buy notional overflowed")?;
    notional
        .checked_add(
            notional
                .checked_mul(fee_rate)
                .context("paper-bar fee notional overflowed")?,
        )
        .context("paper-bar required buying power overflowed")
}

fn synthetic_fill_price(reference_price: Price, side: Side, impact_bps: Decimal) -> Result<Price> {
    let impact = impact_bps
        .checked_div(Decimal::from(10_000_u32))
        .context("paper-bar impact division overflowed")?;
    let price = match side {
        Side::Buy => reference_price.as_decimal().checked_mul(
            Decimal::ONE
                .checked_add(impact)
                .context("paper-bar buy impact overflowed")?,
        ),
        Side::Sell => reference_price.as_decimal().checked_mul(
            Decimal::ONE
                .checked_sub(impact)
                .context("paper-bar sell impact overflowed")?,
        ),
    }
    .context("paper-bar synthetic fill price overflowed")?;
    Price::new(price).map_err(Into::into)
}

fn synthetic_receipt(
    intent: &OrderIntent,
    occurred_at: chrono::DateTime<Utc>,
    fill_price: Price,
) -> TradingReceipt {
    TradingReceipt::Submitted {
        order: crypto_trading_domain::Order {
            id: format!(
                "{}:{}:{}",
                intent.exchange, intent.symbol, intent.client_order_id
            ),
            intent: intent.clone(),
            filled_quantity: intent.quantity,
            average_fill_price: Some(fill_price),
            status: crypto_trading_domain::OrderStatus::Filled,
            created_at: occurred_at,
            updated_at: occurred_at,
        },
        disposition: SubmissionDisposition::Filled,
    }
}

fn paper_bar_action_json(action: &PaperBarAction) -> Value {
    match action {
        PaperBarAction::Hold => json!({"kind": "hold"}),
        PaperBarAction::Rebalance { side, target } => json!({
            "kind": "rebalance",
            "side": side,
            "target": target.as_decimal(),
        }),
    }
}

const fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn control_host_unavailable(error: &TaskHostControlError) -> bool {
    matches!(error, TaskHostControlError::Io(_))
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

const BINANCE_TESTNET_SPOT_BASE_URL_ENV: &str = "CRYPTO_TRADING_BINANCE_TESTNET_SPOT_BASE_URL";
const BINANCE_TESTNET_USDM_BASE_URL_ENV: &str = "CRYPTO_TRADING_BINANCE_TESTNET_USDM_BASE_URL";

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

    let mut symbols = BinanceSmokeSymbols {
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
        args.wire_symbol.clone(),
        expected_observation,
        StdDuration::from_millis(args.poll_interval_ms),
        args.maximum_queries,
    )?;

    let history = JsonlHistory::new(&args.history_path);
    let durable_wire_symbol = testnet_lifecycle_wire_symbol(&config, &history)
        .context("failed to recover the durable Binance lifecycle wire symbol")?;
    let config = config.with_wire_symbol(durable_wire_symbol.clone())?;
    symbols.wire_symbol = durable_wire_symbol;
    let transport: Arc<dyn RemoteHttpTransport> = Arc::new(ReqwestHttpTransport::new(
        StdDuration::from_millis(args.timeout_ms),
    )?);
    let (api_key, api_secret) = load_binance_testnet_credentials()?;
    let signer = Arc::new(BinanceHmacSha256Signer::new(api_key, api_secret)?);
    let requires_submission = testnet_lifecycle_requires_submission(&config, &history)
        .context("failed to inspect durable Binance testnet lifecycle state")?;
    let protocol = if requires_submission {
        let protocol = build_binance_mutation_protocol(
            &*transport,
            signer,
            intent.symbol.clone(),
            intent.market_type,
            config.wire_symbol(),
        )
        .await
        .context("failed to fetch authoritative Binance testnet instrument metadata")?;
        let preflight_timestamp = u64::try_from(Utc::now().timestamp_millis())
            .context("current timestamp is outside the Binance millisecond range")?;
        protocol
            .build_order_request(&intent, Some(price), preflight_timestamp)
            .context("testnet lifecycle order failed local protocol validation")?;
        protocol
    } else {
        build_binance_read_only_protocol(signer, &symbols)
            .context("failed to build query-first Binance testnet recovery protocol")?
    };
    let exchange = BinanceTestnetExchange::new(protocol, transport);
    let report = run_testnet_lifecycle(&config, &exchange, &history).await?;

    print_testnet_lifecycle_report(args, &report)
}

fn print_testnet_lifecycle_report(
    args: &TestnetLifecycleArgs,
    report: &TestnetLifecycleReport,
) -> Result<()> {
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
    let protocol = build_binance_read_only_protocol(signer, &symbols)?;
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
    let protocol = build_binance_read_only_protocol(signer, symbols)?;
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
    let protocol = build_binance_read_only_protocol(signer, symbols)?;
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

fn build_binance_symbol_catalog(symbols: &BinanceSmokeSymbols) -> Result<ExchangeSymbolCatalog> {
    Ok(ExchangeSymbolCatalog::new(vec![
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
    ])?)
}

fn build_binance_read_only_protocol<S>(
    signer: Arc<S>,
    symbols: &BinanceSmokeSymbols,
) -> Result<BinanceTestnetProtocol>
where
    S: BinanceRequestSigner + 'static,
{
    let catalog = build_binance_symbol_catalog(symbols)?;
    BinanceTestnetProtocol::authenticated(
        binance_testnet_endpoints()?,
        catalog,
        InstrumentRuleCatalog::default(),
        signer,
    )
    .context("failed to build Binance testnet read-only protocol")
}

async fn build_binance_mutation_protocol<S>(
    transport: &(dyn RemoteHttpTransport + Send + Sync),
    signer: Arc<S>,
    symbol: Symbol,
    market_type: MarketType,
    wire_symbol: &str,
) -> Result<BinanceTestnetProtocol>
where
    S: BinanceRequestSigner + 'static,
{
    let endpoints = binance_testnet_endpoints()?;
    let BinanceExchangeInfoSymbol {
        symbol: exchange_symbol,
        rules,
    } = fetch_binance_authoritative_symbol(transport, &endpoints, symbol, market_type, wire_symbol)
        .await?;
    BinanceTestnetProtocol::authenticated(
        endpoints,
        ExchangeSymbolCatalog::new(vec![exchange_symbol])?,
        InstrumentRuleCatalog::new(vec![rules])?,
        signer,
    )
    .context("failed to build Binance testnet mutation protocol")
}

async fn build_binance_soak_lifecycle_protocol<S>(
    transport: &(dyn RemoteHttpTransport + Send + Sync),
    signer: Arc<S>,
    symbols: &BinanceSmokeSymbols,
    config: &TestnetLifecycleConfig,
) -> Result<BinanceTestnetProtocol>
where
    S: BinanceRequestSigner + 'static,
{
    let endpoints = binance_testnet_endpoints()?;
    let BinanceExchangeInfoSymbol { rules, .. } = fetch_binance_authoritative_symbol(
        transport,
        &endpoints,
        config.intent().symbol.clone(),
        config.intent().market_type,
        config.wire_symbol(),
    )
    .await?;
    BinanceTestnetProtocol::authenticated(
        endpoints,
        build_binance_symbol_catalog(symbols)?,
        InstrumentRuleCatalog::new(vec![rules])?,
        signer,
    )
    .context("failed to build owner-backed Binance testnet lifecycle protocol")
}

async fn fetch_binance_authoritative_symbol(
    transport: &(dyn RemoteHttpTransport + Send + Sync),
    endpoints: &BinanceTestnetEndpoints,
    symbol: Symbol,
    market_type: MarketType,
    wire_symbol: &str,
) -> Result<BinanceExchangeInfoSymbol> {
    let product = match market_type {
        MarketType::Spot => BinanceProduct::Spot,
        MarketType::Perpetual => BinanceProduct::UsdM,
    };
    let request =
        BinanceTestnetProtocol::build_exchange_info_request(endpoints, product, wire_symbol)?;
    let response = transport.send(request).await?;
    if !response.is_success() {
        return Err(BinanceTestnetProtocol::remote_failure_from_response(&response).into());
    }
    BinanceTestnetProtocol::parse_exchange_info_symbol(
        product,
        response.body(),
        symbol,
        wire_symbol,
    )
    .map_err(anyhow::Error::from)
}

fn binance_testnet_endpoints() -> Result<BinanceTestnetEndpoints> {
    let spot = testnet_env_value(BINANCE_TESTNET_SPOT_BASE_URL_ENV)?;
    let usdm = testnet_env_value(BINANCE_TESTNET_USDM_BASE_URL_ENV)?;
    match (spot, usdm) {
        (None, None) => Ok(BinanceTestnetEndpoints::official()),
        (Some(spot), Some(usdm)) => BinanceTestnetEndpoints::loopback(&spot, &usdm)
            .context("invalid Binance testnet loopback endpoint override"),
        _ => bail!(
            "{BINANCE_TESTNET_SPOT_BASE_URL_ENV} and {BINANCE_TESTNET_USDM_BASE_URL_ENV} must be set together"
        ),
    }
}

fn testnet_env_value(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => bail!("{name} must not be blank"),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => bail!("{name} must be valid UTF-8"),
    }
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
    market_stream: BinanceBookTickerStreamSource,
    user_stream: BinanceUserDataStreamSource,
    owner: ContinuousTestnetOwner<BinanceTestnetExchange>,
    lifecycle_run_pending: bool,
    pending_user_item: Option<BinanceUserDataStreamItem>,
    next_step: usize,
}

#[derive(Clone, Debug)]
enum TestnetSoakLifecycleOwnerMode {
    ReadOnly,
    Fresh(TestnetLifecycleConfig),
    Recovery(TestnetLifecycleConfig),
}

impl ProductionBinanceTestnetSoakProbe {
    async fn new(
        transport: &Arc<dyn RemoteHttpTransport>,
        symbols: &BinanceSmokeSymbols,
        api_key: String,
        api_secret: String,
        owner_id: &str,
        history: JsonlHistory,
        lifecycle: TestnetSoakLifecycleOwnerMode,
    ) -> Result<Self> {
        let signer = Arc::new(BinanceHmacSha256Signer::new(api_key, api_secret)?);
        let user_protocol = Arc::new(build_binance_read_only_protocol(
            Arc::clone(&signer),
            symbols,
        )?);
        let exchange_protocol = match &lifecycle {
            TestnetSoakLifecycleOwnerMode::Fresh(config) => {
                build_binance_soak_lifecycle_protocol(
                    &**transport,
                    Arc::clone(&signer),
                    symbols,
                    config,
                )
                .await?
            }
            TestnetSoakLifecycleOwnerMode::ReadOnly
            | TestnetSoakLifecycleOwnerMode::Recovery(_) => {
                build_binance_read_only_protocol(Arc::clone(&signer), symbols)?
            }
        };
        let exchange = Arc::new(BinanceTestnetExchange::new(
            exchange_protocol,
            Arc::clone(transport),
        ));

        let market_route = BinancePollingRoute::new(
            MarketInstrument::new("binance", symbols.spot.clone(), MarketType::Spot)?,
            Symbol::new(&symbols.wire_symbol)?,
        )?;
        let queue_capacity = NonZeroUsize::new(MAX_MARKET_SUPERVISOR_BUFFERED_EVENTS)
            .context("market supervisor queue capacity must be nonzero")?;
        let market_connector = Arc::new(TokioTextWebSocketConnector::for_binance_book_ticker(
            BinanceSpotMarketStreamEndpoint::official(),
            std::slice::from_ref(&market_route),
            queue_capacity,
            StdDuration::from_secs(20),
        )?);
        let reconnect_policy = MarketStreamReconnectPolicy::new(
            StdDuration::from_secs(1),
            StdDuration::from_secs(60),
        )?
        .with_max_reconnect_attempts(10);
        let market_stream = BinanceBookTickerStreamSource::new(
            BinancePublicExchange::with_base_url("https://testnet.binance.vision")?,
            vec![market_route],
            market_connector,
            reconnect_policy,
            Arc::new(SystemMarketDataClock),
            Arc::new(TokioMarketStreamSleeper),
            Arc::new(ProductionMarketStreamJitter::new(7_500, 12_500)?),
        )?;
        let user_connector = Arc::new(TokioTextWebSocketConnector::for_binance_user_data_stream(
            BinanceSpotUserDataStreamEndpoint::official(),
            user_protocol,
            None,
            queue_capacity,
            StdDuration::from_secs(20),
        )?);
        let user_stream = BinanceUserDataStreamSource::new(
            user_connector,
            reconnect_policy,
            Arc::new(SystemMarketDataClock),
            Arc::new(TokioMarketStreamSleeper),
            Arc::new(ProductionMarketStreamJitter::new(7_500, 12_500)?),
        );
        Self::from_parts(
            market_stream,
            user_stream,
            exchange,
            owner_id,
            history,
            lifecycle,
        )
        .await
    }

    async fn from_parts(
        market_stream: BinanceBookTickerStreamSource,
        user_stream: BinanceUserDataStreamSource,
        exchange: Arc<BinanceTestnetExchange>,
        owner_id: &str,
        history: JsonlHistory,
        lifecycle: TestnetSoakLifecycleOwnerMode,
    ) -> Result<Self> {
        let lifecycle_run_pending = matches!(lifecycle, TestnetSoakLifecycleOwnerMode::Fresh(_));
        let owner = match lifecycle {
            TestnetSoakLifecycleOwnerMode::Fresh(config) => {
                ContinuousTestnetOwner::start(owner_id, config, Arc::clone(&exchange), history)
                    .await
            }
            TestnetSoakLifecycleOwnerMode::Recovery(config) => {
                ContinuousTestnetOwner::start_recovery_only(
                    owner_id,
                    config,
                    Arc::clone(&exchange),
                    history,
                )
                .await
            }
            TestnetSoakLifecycleOwnerMode::ReadOnly => {
                ContinuousTestnetOwner::start_read_only(owner_id, Arc::clone(&exchange), history)
                    .await
            }
        }
        .context("failed to start owner-backed Binance Testnet soak")?;
        if owner.status().phase != ContinuousTestnetOwnerPhase::AwaitingUserStream {
            bail!("owner-backed Binance Testnet soak requires recovery before serving");
        }
        Ok(Self {
            market_stream,
            user_stream,
            owner,
            lifecycle_run_pending,
            pending_user_item: None,
            next_step: 0,
        })
    }

    async fn next_probe(&mut self) -> Result<TestnetSoakSample, TestnetSoakProbeFailure> {
        let step = self.next_step;
        let result = match step {
            0 => self.next_market_stream_sample().await,
            1 => self.next_user_stream_sample().await,
            _ => {
                self.owner
                    .verify_stable_reconcile()
                    .await
                    .map_err(classify_continuous_testnet_owner_failure)?;
                Ok(TestnetSoakSample::AuthenticatedReconcile)
            }
        };
        // Advancing after the awaited work is the cancellation-safety seam:
        // a dropped timeout future retries the same lane instead of silently
        // skipping it in the three-way rotation.
        self.next_step = (self.next_step + 1) % 3;
        result
    }

    async fn next_market_stream_sample(
        &mut self,
    ) -> Result<TestnetSoakSample, TestnetSoakProbeFailure> {
        match self
            .market_stream
            .next_event()
            .await
            .map_err(|_| TestnetSoakProbeFailure::Protocol)?
        {
            Some(MarketDataEvent::Observation(_)) => Ok(TestnetSoakSample::MarketStream),
            Some(MarketDataEvent::SourceGap { .. }) => Err(TestnetSoakProbeFailure::Transport),
            Some(MarketDataEvent::SourceUnavailable { failure, .. }) => {
                Err(classify_market_stream_failure(failure))
            }
            None => Err(TestnetSoakProbeFailure::Unavailable),
        }
    }

    async fn next_user_stream_sample(
        &mut self,
    ) -> Result<TestnetSoakSample, TestnetSoakProbeFailure> {
        if matches!(
            self.owner.status().phase,
            ContinuousTestnetOwnerPhase::CampaignRunning
                | ContinuousTestnetOwnerPhase::Reconciling
                | ContinuousTestnetOwnerPhase::RecoveryRequired
        ) {
            self.owner
                .resume_interrupted_work()
                .await
                .map_err(classify_continuous_testnet_owner_failure)?;
            // Recovery completes the durable lifecycle that was interrupted by
            // the bounded probe timeout. Do not submit it a second time after
            // the cached user-stream item is acknowledged below.
            self.lifecycle_run_pending = false;
        }
        if self.pending_user_item.is_none() {
            self.pending_user_item = Some(
                self.user_stream
                    .next_item()
                    .await
                    .map_err(|_| TestnetSoakProbeFailure::Protocol)?
                    .ok_or(TestnetSoakProbeFailure::Unavailable)?,
            );
        }
        let item = self
            .pending_user_item
            .clone()
            .ok_or(TestnetSoakProbeFailure::Unavailable)?;
        let recovery_failure = match &item {
            BinanceUserDataStreamItem::TransportGap { .. } => TestnetSoakProbeFailure::Transport,
            BinanceUserDataStreamItem::StreamExpired { .. } => TestnetSoakProbeFailure::Unavailable,
            BinanceUserDataStreamItem::SourceUnavailable { failure, .. } => {
                classify_market_stream_failure(*failure)
            }
            _ => TestnetSoakProbeFailure::Protocol,
        };
        let outcome = self
            .owner
            .ingest_user_data_item(item)
            .await
            .map_err(classify_continuous_testnet_owner_failure)?;
        match outcome {
            ContinuousTestnetUserDataOutcome::Subscribed
            | ContinuousTestnetUserDataOutcome::Heartbeat
            | ContinuousTestnetUserDataOutcome::Applied(_) => {
                if self.lifecycle_run_pending {
                    self.owner
                        .run_lifecycle()
                        .await
                        .map_err(classify_continuous_testnet_owner_failure)?;
                    self.lifecycle_run_pending = false;
                }
                self.pending_user_item = None;
                Ok(TestnetSoakSample::UserDataStream)
            }
            ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(_) => {
                self.pending_user_item = None;
                Err(recovery_failure)
            }
        }
    }
}

fn classify_continuous_testnet_owner_failure(
    error: ContinuousTestnetOwnerError,
) -> TestnetSoakProbeFailure {
    match error {
        ContinuousTestnetOwnerError::Exchange(error) => classify_testnet_soak_probe_failure(&error),
        ContinuousTestnetOwnerError::ForeignActivity
        | ContinuousTestnetOwnerError::UnstableReconciliation
        | ContinuousTestnetOwnerError::InvalidJournal
        | ContinuousTestnetOwnerError::RecoveryPlanMissing
        | ContinuousTestnetOwnerError::RecoveryQueryMissing => TestnetSoakProbeFailure::Protocol,
        ContinuousTestnetOwnerError::Lifecycle(_) => TestnetSoakProbeFailure::RemoteRejected,
        ContinuousTestnetOwnerError::History(_)
        | ContinuousTestnetOwnerError::JournalRead(_)
        | ContinuousTestnetOwnerError::InvalidConfig
        | ContinuousTestnetOwnerError::OwnerBusy
        | ContinuousTestnetOwnerError::NotReady
        | ContinuousTestnetOwnerError::KillSwitchLatched
        | ContinuousTestnetOwnerError::LifecycleAuthorityUnavailable => {
            TestnetSoakProbeFailure::Unavailable
        }
    }
}

const fn classify_market_stream_failure(
    failure: MarketDataSourceFailure,
) -> TestnetSoakProbeFailure {
    match failure {
        MarketDataSourceFailure::Disconnected => TestnetSoakProbeFailure::Transport,
        MarketDataSourceFailure::TimedOut => TestnetSoakProbeFailure::Timeout,
        MarketDataSourceFailure::Backpressure => TestnetSoakProbeFailure::RateLimited,
        MarketDataSourceFailure::InvalidPayload => TestnetSoakProbeFailure::Protocol,
        MarketDataSourceFailure::Rejected => TestnetSoakProbeFailure::RemoteRejected,
        MarketDataSourceFailure::Unknown => TestnetSoakProbeFailure::Unavailable,
    }
}

impl TestnetSoakProbe for ProductionBinanceTestnetSoakProbe {
    fn planned_sample(&self) -> Option<TestnetSoakSample> {
        Some(match self.next_step {
            0 => TestnetSoakSample::MarketStream,
            1 => TestnetSoakSample::UserDataStream,
            _ => TestnetSoakSample::AuthenticatedReconcile,
        })
    }

    fn probe(&mut self) -> TestnetSoakProbeFuture<'_> {
        Box::pin(async move { self.next_probe().await })
    }

    fn shutdown(&mut self) -> crate::testnet_soak::TestnetSoakShutdownFuture<'_> {
        Box::pin(async move {
            // A pending lifecycle must be recovered and cancelled before the
            // soak can claim a clean stop. Read-only, unsubmitted, and already
            // completed sessions retain restartability through a final stable
            // owner-backed reconciliation without latching the kill switch.
            self.owner.shutdown_cleanly().await.map_err(|_| ())
        })
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

    // Validate the all-or-none recovery group and durable campaign state
    // before credentials, sockets, or any other remote-capable dependency is
    // constructed.
    let recovery = testnet_soak_recovery_config(args)?;
    ensure_control_token_configured()
        .map_err(anyhow::Error::new)
        .context("testnet-soak serve requires a valid loopback control token")?;
    let task_id = args.task_id.as_str();
    let address = control_addr(task_id, &args.history_path, Some(control_port));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind testnet soak control socket on {address}"))?;
    let shutdown = register_task_host_shutdown()?;

    if let Some(script) = &args.fixture_probe_script {
        if !matches!(recovery, TestnetSoakLifecycleOwnerMode::ReadOnly) {
            bail!("fixture testnet-soak probes cannot claim lifecycle recovery evidence");
        }
        let probe = ScriptedTestnetSoakProbe::parse(script)?;
        return serve_testnet_soak_task(args, address, listener, shutdown, config, probe).await;
    }

    let symbols = testnet_soak_symbols(args)?;
    let (api_key, api_secret) = load_binance_testnet_credentials()?;
    let transport: Arc<dyn RemoteHttpTransport> = Arc::new(ReqwestHttpTransport::new(
        StdDuration::from_millis(args.timeout_ms),
    )?);
    let probe = ProductionBinanceTestnetSoakProbe::new(
        &transport,
        &symbols,
        api_key,
        api_secret,
        &args.task_id,
        JsonlHistory::new(&args.history_path),
        recovery,
    )
    .await?;
    serve_testnet_soak_task(args, address, listener, shutdown, config, probe).await
}

#[allow(clippy::too_many_lines)]
fn testnet_soak_recovery_config(args: &TestnetSoakArgs) -> Result<TestnetSoakLifecycleOwnerMode> {
    let recovery = &args.lifecycle_recovery;
    let supplied = [
        recovery.recovery_campaign_id.is_some(),
        recovery.recovery_client_order_id.is_some(),
        recovery.recovery_market.is_some(),
        recovery.recovery_side.is_some(),
        recovery.recovery_quantity.is_some(),
        recovery.recovery_price.is_some(),
        recovery.recovery_time_in_force.is_some(),
        recovery.recovery_expected_observation.is_some(),
        recovery.recovery_reduce_only.is_some(),
        recovery.recovery_poll_interval_ms.is_some(),
        recovery.recovery_maximum_queries.is_some(),
    ];
    if supplied.iter().all(|supplied| !supplied) {
        if recovery.acknowledge_testnet_lifecycle.is_some() {
            bail!("testnet-soak lifecycle acknowledgement requires the full exact configuration");
        }
        return Ok(TestnetSoakLifecycleOwnerMode::ReadOnly);
    }
    if !supplied.iter().all(|supplied| *supplied) {
        bail!("testnet-soak lifecycle recovery options must be supplied all-or-none");
    }

    let market = recovery
        .recovery_market
        .context("missing recovery market")?;
    let reduce_only = recovery
        .recovery_reduce_only
        .context("missing recovery reduce-only bit")?;
    if reduce_only && market == TestnetLifecycleMarket::Spot {
        bail!("testnet-soak recovery --recovery-reduce-only is invalid for spot");
    }
    let (symbol, market_type) = match market {
        TestnetLifecycleMarket::Spot => (
            Symbol::new(args.spot_symbol.clone()).context("invalid --spot-symbol")?,
            MarketType::Spot,
        ),
        TestnetLifecycleMarket::Usdm => (
            Symbol::new(args.perpetual_symbol.clone()).context("invalid --perpetual-symbol")?,
            MarketType::Perpetual,
        ),
    };
    let side = match recovery.recovery_side.context("missing recovery side")? {
        TestnetLifecycleSide::Buy => Side::Buy,
        TestnetLifecycleSide::Sell => Side::Sell,
    };
    let time_in_force = match recovery
        .recovery_time_in_force
        .context("missing recovery time-in-force")?
    {
        TestnetLifecycleTimeInForce::Gtc => TimeInForce::Gtc,
        TestnetLifecycleTimeInForce::PostOnly => TimeInForce::PostOnly,
    };
    let expected = match recovery
        .recovery_expected_observation
        .context("missing recovery expected observation")?
    {
        TestnetLifecycleExpected::Open => TestnetLifecycleObservation::Open,
        TestnetLifecycleExpected::PartiallyFilled => TestnetLifecycleObservation::PartiallyFilled,
    };
    let quantity = Quantity::new(
        recovery
            .recovery_quantity
            .context("missing recovery quantity")?,
    )
    .context("invalid recovery quantity")?;
    let price = Price::new(recovery.recovery_price.context("missing recovery price")?)
        .context("invalid recovery price")?;
    let mut intent = OrderIntent::limit("binance", symbol, market_type, side, quantity, price);
    intent.client_order_id = recovery
        .recovery_client_order_id
        .context("missing recovery client order ID")?;
    intent.time_in_force = time_in_force;
    intent.reduce_only = reduce_only;
    let config = TestnetLifecycleConfig::new(
        recovery
            .recovery_campaign_id
            .clone()
            .context("missing recovery campaign ID")?,
        intent,
        args.wire_symbol.clone(),
        expected,
        StdDuration::from_millis(
            recovery
                .recovery_poll_interval_ms
                .context("missing recovery poll interval")?,
        ),
        recovery
            .recovery_maximum_queries
            .context("missing recovery query budget")?,
    )?;
    let history = JsonlHistory::new(&args.history_path);
    match testnet_lifecycle_recovery_state(&config, &history)
        .context("failed to inspect exact durable lifecycle recovery state")?
    {
        TestnetLifecycleRecoveryState::Pending { .. } => {
            if recovery
                .acknowledge_testnet_lifecycle
                .as_deref()
                .is_some_and(|ack| ack != TESTNET_LIFECYCLE_ACKNOWLEDGEMENT)
            {
                bail!("invalid Testnet lifecycle acknowledgement");
            }
            Ok(TestnetSoakLifecycleOwnerMode::Recovery(config))
        }
        TestnetLifecycleRecoveryState::Fresh => {
            if recovery.acknowledge_testnet_lifecycle.as_deref()
                != Some(TESTNET_LIFECYCLE_ACKNOWLEDGEMENT)
            {
                bail!(
                    "first-submit eligible testnet-soak lifecycle requires --acknowledge-testnet-lifecycle \"{TESTNET_LIFECYCLE_ACKNOWLEDGEMENT}\""
                );
            }
            Ok(TestnetSoakLifecycleOwnerMode::Fresh(config))
        }
        TestnetLifecycleRecoveryState::Completed | TestnetLifecycleRecoveryState::Failed => {
            bail!("testnet-soak recovery requires a pending non-terminal lifecycle")
        }
    }
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
    address: std::net::SocketAddr,
    listener: tokio::net::TcpListener,
    shutdown: ShutdownSignalFuture,
    config: TestnetSoakTaskConfig,
    probe: P,
) -> Result<()>
where
    P: TestnetSoakProbe,
{
    let task_id = args.task_id.as_str();
    let mut task = TestnetSoakTask::start(config, probe, JsonlHistory::new(&args.history_path))
        .await
        .context("failed to start testnet soak task")?;

    println!(
        "testnet soak task started: task_id={} control={} history={}",
        task_id,
        address,
        args.history_path.display()
    );

    let outcome = match serve_host_with_shutdown(
        &mut task,
        listener,
        StdDuration::from_millis(args.control_poll_interval_ms.max(1)),
        render_live_testnet_soak_status,
        render_live_testnet_soak_stop,
        Ok(shutdown),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => return Err(stop_testnet_soak_task_after_serve_error(&mut task, error).await),
    };

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

async fn stop_testnet_soak_task_after_serve_error(
    task: &mut TestnetSoakTask,
    error: TaskHostServeError<TestnetSoakTaskError>,
) -> anyhow::Error {
    let requires_cleanup =
        !matches!(error, TaskHostServeError::Task(_)) && !task.status().is_terminal();
    let serve_error = anyhow::Error::new(error).context("testnet soak control host failed");
    if !requires_cleanup {
        return serve_error;
    }
    match task.stop().await {
        Ok(_) => serve_error,
        Err(stop_error) => serve_error.context(format!(
            "failed to stop testnet soak task after control host failure: {stop_error}"
        )),
    }
}

async fn run_testnet_soak_status(args: &TestnetSoakArgs) -> Result<()> {
    let address = control_addr(&args.task_id, &args.history_path, args.control_port);
    match query_control(address, TaskHostControlCommand::Status).await {
        Ok(response) => {
            print!("{response}");
            return Ok(());
        }
        Err(error) if !control_host_unavailable(&error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("testnet soak control request failed for {address}")));
        }
        Err(_) => {}
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
    match query_control(address, TaskHostControlCommand::Stop).await {
        Ok(response) => {
            print!("{response}");
            return Ok(());
        }
        Err(error) if !control_host_unavailable(&error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("testnet soak control request failed for {address}")));
        }
        Err(_) => {}
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
        "market_stream" => Ok(TestnetSoakSample::MarketStream),
        "user_data_stream" => Ok(TestnetSoakSample::UserDataStream),
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
        if record.details["task_kind"].as_str() != Some(TESTNET_SOAK_TASK_KIND) {
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
                projected.consecutive_failure_count = observation["consecutive_failure_count"]
                    .as_u64()
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or(0);
                if projected.consecutive_failure_count == 0 {
                    overwrite_string(&mut projected.last_probe_failure, "none");
                }
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
                projected.consecutive_failure_count = observation["consecutive_failure_count"]
                    .as_u64()
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or_else(|| projected.consecutive_failure_count.saturating_add(1));
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
        TestnetSoakSample::MarketStream => "market_stream",
        TestnetSoakSample::UserDataStream => "user_data_stream",
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
        TestnetSoakTaskFailure::ProbeShutdown => "probe_shutdown",
        TestnetSoakTaskFailure::EvidenceIntegrity => "evidence_integrity",
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
        return match args.live_transport {
            MonitorLiveTransport::Stream => {
                let (read_monitor, left_source, right_source) =
                    build_live_stream_monitor_pair(args, &monitor, &symbol)?;
                serve_monitor_task(args, task_id, read_monitor, left_source, right_source).await
            }
            MonitorLiveTransport::Polling => {
                let (read_monitor, left_source, right_source) =
                    build_live_polling_monitor_pair(args, &monitor, &symbol)?;
                serve_monitor_task(args, task_id, read_monitor, left_source, right_source).await
            }
        };
    }
    let replay_path = args.replay.as_ref().context(
        "monitor serve requires --replay unless --live opts into the credential-free binance+hyperliquid pair",
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

/// Builds the default live pair: a Binance Spot Testnet websocket leg and a
/// Hyperliquid perpetual polling leg, both credential-free and read-only.
fn build_live_stream_monitor_pair(
    args: &MonitorArgs,
    monitor: &MonitorConfig,
    symbol: &Symbol,
) -> Result<(
    ReadOnlyArbitrageMonitor,
    BinanceBookTickerStreamSource,
    HyperliquidPublicPollingSource,
)> {
    let (read_monitor, left, right, wire_coin) = live_monitor_context(monitor, symbol)?;
    let routes = vec![BinancePollingRoute::new(
        left,
        Symbol::new(symbol.as_str())?,
    )?];
    let endpoint = match args.binance_ws_base_url.as_deref() {
        Some(base_url) => BinanceSpotMarketStreamEndpoint::loopback(base_url)?,
        None => BinanceSpotMarketStreamEndpoint::official(),
    };
    let queue_capacity = NonZeroUsize::new(MAX_MARKET_SUPERVISOR_BUFFERED_EVENTS)
        .context("market supervisor queue capacity must be nonzero")?;
    let connector = Arc::new(TokioTextWebSocketConnector::for_binance_book_ticker(
        endpoint,
        &routes,
        queue_capacity,
        StdDuration::from_secs(monitor.ws_ping_interval),
    )?);
    let initial_retry_delay = StdDuration::from_secs(monitor.ws_reconnect_delay);
    let max_retry_delay = initial_retry_delay
        .checked_mul(32)
        .unwrap_or(StdDuration::from_secs(300))
        .min(StdDuration::from_secs(300));
    let reconnect_policy = MarketStreamReconnectPolicy::new(initial_retry_delay, max_retry_delay)?
        .with_max_reconnect_attempts(monitor.ws_max_reconnect_attempts);
    let left_source = BinanceBookTickerStreamSource::new(
        BinancePublicExchange::with_base_url("https://testnet.binance.vision")?,
        routes,
        connector,
        reconnect_policy,
        Arc::new(SystemMarketDataClock),
        Arc::new(TokioMarketStreamSleeper),
        Arc::new(ProductionMarketStreamJitter::new(7_500, 12_500)?),
    )?;
    let right_source = build_hyperliquid_live_source(args, right, wire_coin)?;
    Ok((read_monitor, left_source, right_source))
}

/// Builds the explicit degraded live pair: Binance Spot and Hyperliquid both
/// use REST polling. Operators must select this path deliberately.
///
/// The Hyperliquid leg's funding-rate side feed is not consumed here yet: the
/// spread-history journal keeps recording funding fields as absent, so
/// history-mode decisions stay explicitly funding-degraded.
fn build_live_polling_monitor_pair(
    args: &MonitorArgs,
    monitor: &MonitorConfig,
    symbol: &Symbol,
) -> Result<(
    ReadOnlyArbitrageMonitor,
    BinancePublicPollingSource,
    HyperliquidPublicPollingSource,
)> {
    let (read_monitor, left, right, wire_coin) = live_monitor_context(monitor, symbol)?;
    let poll_interval = StdDuration::from_millis(args.poll_interval_ms.max(1));
    let policy = MarketPollingPolicy::new(
        poll_interval,
        poll_interval,
        poll_interval.max(StdDuration::from_secs(60)),
    )?;
    let binance = match args.binance_base_url.as_deref() {
        Some(base_url) => BinancePublicExchange::with_base_url(base_url)?,
        None => BinancePublicExchange::with_base_url("https://testnet.binance.vision")?,
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

fn live_monitor_context(
    monitor: &MonitorConfig,
    symbol: &Symbol,
) -> Result<(
    ReadOnlyArbitrageMonitor,
    MarketInstrument,
    MarketInstrument,
    String,
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
    Ok((read_monitor, left, right, wire_coin))
}

fn build_hyperliquid_live_source(
    args: &MonitorArgs,
    right: MarketInstrument,
    wire_coin: String,
) -> Result<HyperliquidPublicPollingSource> {
    let poll_interval = StdDuration::from_millis(args.poll_interval_ms.max(1));
    let policy = MarketPollingPolicy::new(
        poll_interval,
        poll_interval,
        poll_interval.max(StdDuration::from_secs(60)),
    )?;
    let endpoint = match args.hyperliquid_base_url.as_deref() {
        Some(base_url) => HyperliquidPublicEndpoint::loopback(base_url)?,
        None => HyperliquidPublicEndpoint::official(),
    };
    let exchange = HyperliquidPublicExchange::with_endpoint(&endpoint)?;
    Ok(HyperliquidPublicPollingSource::new(
        exchange,
        vec![HyperliquidPollingRoute::new(
            right,
            Symbol::new(wire_coin)?,
        )?],
        policy,
        Arc::new(SystemMarketDataClock),
    )?)
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
    ensure_control_token_configured()
        .map_err(anyhow::Error::new)
        .context("monitor serve requires a valid loopback control token")?;
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
    match query_control(address, TaskHostControlCommand::Status).await {
        Ok(response) => {
            print!("{response}");
            return Ok(());
        }
        Err(error) if !control_host_unavailable(&error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("monitor control request failed for {address}")));
        }
        Err(_) => {}
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
    match query_control(address, TaskHostControlCommand::Stop).await {
        Ok(response) => {
            print!("{response}");
            return Ok(());
        }
        Err(error) if !control_host_unavailable(&error) => {
            return Err(anyhow::Error::new(error)
                .context(format!("monitor control request failed for {address}")));
        }
        Err(_) => {}
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
            &left_symbol,
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
            &right_symbol,
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
    symbol: &Symbol,
    bid: Decimal,
    ask: Decimal,
    bid_quantity: Decimal,
    ask_quantity: Decimal,
) -> Result<MarketSnapshot> {
    let mut snapshot = MarketSnapshot::new(
        exchange,
        symbol.clone(),
        market_type_for_one_shot_symbol(symbol),
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

fn market_type_for_one_shot_symbol(symbol: &Symbol) -> MarketType {
    match symbol.as_str().rsplit_once('-') {
        Some((_, "SPOT")) => MarketType::Spot,
        _ => MarketType::Perpetual,
    }
}

#[derive(Debug)]
struct ArbitrageExecutionPolicy {
    strategy_key: Symbol,
    data_timeout_seconds: u64,
    monitor_exchanges: Vec<String>,
    monitor_symbols: Vec<Symbol>,
    configured_exchanges: Vec<String>,
    configured_symbols: Vec<Symbol>,
    leg_markets: Vec<(String, Symbol, MarketType)>,
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
    if snapshots[0].symbol != snapshots[1].symbol {
        bail!(
            "one-shot arbitrage execution requires identical leg symbols until multi-symbol admission and replay are supported"
        );
    }

    let strategy_key = if let Some(value) = args.market.strategy_key.as_deref() {
        Symbol::new(value).context("--strategy-key must not be empty")?
    } else {
        snapshots[0].symbol.clone()
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
            .map(|snapshot| {
                (
                    snapshot.exchange().to_owned(),
                    snapshot.symbol.clone(),
                    snapshot.market_type,
                )
            })
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
        if !ArbitrageStrategy::symbols_share_hedge_identity(
            &snapshots[0].symbol,
            &snapshots[1].symbol,
        ) {
            bail!("arbitrage legs do not share a hedge identity");
        }

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
                .any(|(exchange, symbol, market_type)| {
                    exchange == &intent.exchange
                        && symbol == &intent.symbol
                        && market_type == &intent.market_type
                })
            {
                bail!(
                    "intent {}/{}/{:?} is outside the authorized arbitrage legs",
                    intent.exchange,
                    intent.symbol,
                    intent.market_type
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
    // This one-shot paper helper has no durable account truth. Until the
    // shipped paper account authority is threaded into this path, reuse the
    // explicit paper-only `max_position_value` as a synthetic full-notional
    // opening budget. Mainnet order authority remains unavailable elsewhere.
    let account = AccountRiskSnapshot {
        equity: Money::new(max_position_value),
        available_balance: Money::new(max_position_value),
        kill_switch: false,
        timestamp: now,
    };
    let markets = markets.into_iter().cloned().collect::<Vec<_>>();
    match engine.authorize_batch(intents, &account, &[], &markets, now) {
        RiskDecision::Authorized => Ok(()),
        RiskDecision::Rejected(rejection) => {
            bail!(
                "arbitrage risk rejected the batch: {rejection:?}; paper-only once execution uses max_position_value as a synthetic full-notional account budget"
            )
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
    } else if is_account_risk_config(path, mapping) {
        load_account_risk_config_from_str(&body)
            .map_err(|error| invalid_config_error("account-risk", &error))?;
        Ok(config_summary(
            path,
            "account-risk",
            ConfigSupport::PaperCompanion,
            Some("shared limits consumed by replay-backed paper owners"),
        ))
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

fn is_account_risk_config(path: &Path, mapping: &serde_yaml::Mapping) -> bool {
    let named_for_account_risk =
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                let name = name.to_ascii_lowercase();
                name.contains("account-risk") || name.contains("account_risk")
            });
    named_for_account_risk
        || [
            "max_symbol_exposure",
            "max_total_exposure",
            "min_balance_warning",
            "min_balance_close_position",
            "max_position_duration_seconds",
            "max_daily_trades",
        ]
        .iter()
        .any(|key| mapping.contains_key(serde_yaml::Value::from(*key)))
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
        collections::VecDeque,
        fmt,
        sync::{Arc, Mutex},
        time::Duration as StdDuration,
        time::{SystemTime, UNIX_EPOCH},
    };

    use async_trait::async_trait;
    use chrono::{Duration, TimeZone, Utc};
    use crypto_trading_config::{
        ArbitrageConfig, load_arbitrage_config_from_str, load_grid_config_from_str,
    };
    use crypto_trading_domain::{
        MarketSnapshot, MarketType, Money, Order, OrderIntent, OrderStatus, OrderType, Position,
        PositionSide, Price, Quantity, Side, Symbol, TimeInForce,
    };
    use crypto_trading_exchange::{
        BinanceHmacSha256Signer, BinancePublicExchange, BinanceTestnetEndpoints,
        BinanceTestnetExchange, BinanceTestnetProtocol, ExchangeError, ExchangeSymbol,
        ExchangeSymbolCatalog, ForeignOrder, InstrumentRuleCatalog, InstrumentRules,
        ReconcileReceipt, ReconcileScope, RemoteHttpMethod, RemoteHttpRequest, RemoteHttpResponse,
        RemoteHttpTransport, SubmissionDisposition, TradingReceipt,
    };
    use crypto_trading_runtime::{
        BinanceBookTickerStreamSource, BinancePollingRoute, BinanceUserDataStreamSource,
        ExecutionBatch, FixedMarketStreamJitter, JsonlHistory, MarketDataClock, MarketInstrument,
        MarketStreamReconnectPolicy, MarketStreamSleeper, ReconciliationObservation, RuntimeError,
        TextWebSocketConnector, TextWebSocketEvent, TextWebSocketSession, WebSocketCloseKind,
    };
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::{
        BinanceSmokeSymbols, ConfigCheckReport, ConfigDiscovery, ExecutionOutcomeJournalError,
        MAX_CONFIG_CHECK_ENTRIES, MAX_CONFIG_CHECK_ERRORS, MAX_CONFIG_CHECK_OUTPUT_BYTES,
        MAX_CONFIG_CHECK_SUMMARIES, MAX_CONFIG_DETAIL_BYTES, MAX_CONFIG_SCHEMA_ISSUE_BYTES,
        MAX_CONFIG_SCHEMA_ISSUES, MAX_RECEIPT_SUMMARY_RECEIPTS, MAX_RECONCILIATION_SUMMARY_ORDERS,
        MAX_RECONCILIATION_SUMMARY_POSITIONS, PaperRuntimeSchema, PreservedExecutionOutcome,
        ProductionBinanceTestnetSoakProbe, TestnetSoakLifecycleOwnerMode, append_execution_planned,
        auxiliary_config_kind, bounded_issue_detail, build_binance_read_only_protocol,
        collect_config_report, config_summary, execution_batch, execution_error_summary,
        finish_arbitrage_execution, finish_execution, inspect_config, paper_runtime_schema_issues,
        plan_grid_intents, receipt_summary, render_config_summary,
        start_after_shutdown_registration, stop_testnet_soak_task_after_serve_error,
    };
    use crate::task_host::{TaskHostControlTokenError, TaskHostServeError};
    use crate::testnet_lifecycle::{TestnetLifecycleConfig, TestnetLifecycleObservation};
    use crate::testnet_soak::{
        TestnetSoakProbe, TestnetSoakProbeFuture, TestnetSoakSample, TestnetSoakTask,
        TestnetSoakTaskConfig, TestnetSoakTaskPhase,
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

    #[derive(Clone)]
    struct FixedStreamClock {
        now: chrono::DateTime<Utc>,
    }

    impl fmt::Debug for FixedStreamClock {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("FixedStreamClock")
                .finish_non_exhaustive()
        }
    }

    impl MarketDataClock for FixedStreamClock {
        fn now(&self) -> chrono::DateTime<Utc> {
            self.now
        }
    }

    #[derive(Debug, Default)]
    struct NoopStreamSleeper;

    #[async_trait]
    impl MarketStreamSleeper for NoopStreamSleeper {
        async fn sleep(&self, _duration: StdDuration) {}
    }

    struct PendingSoakProbe;

    impl TestnetSoakProbe for PendingSoakProbe {
        fn probe(&mut self) -> TestnetSoakProbeFuture<'_> {
            Box::pin(std::future::pending())
        }
    }

    #[derive(Debug)]
    struct ScriptedWebSocketSession {
        events: VecDeque<Result<TextWebSocketEvent, ExchangeError>>,
    }

    #[async_trait]
    impl TextWebSocketSession for ScriptedWebSocketSession {
        async fn next_event(&mut self) -> Result<TextWebSocketEvent, ExchangeError> {
            self.events.pop_front().unwrap_or_else(|| {
                Ok(TextWebSocketEvent::Closed {
                    kind: WebSocketCloseKind::Remote,
                })
            })
        }
    }

    #[derive(Debug)]
    struct ScriptedWebSocketConnector {
        sessions: Mutex<VecDeque<ScriptedWebSocketSession>>,
    }

    impl ScriptedWebSocketConnector {
        fn one(events: Vec<TextWebSocketEvent>) -> Self {
            Self {
                sessions: Mutex::new(
                    vec![ScriptedWebSocketSession {
                        events: events.into_iter().map(Ok).collect(),
                    }]
                    .into(),
                ),
            }
        }
    }

    #[async_trait]
    impl TextWebSocketConnector for ScriptedWebSocketConnector {
        async fn connect(&self) -> Result<Box<dyn TextWebSocketSession>, ExchangeError> {
            self.sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .map(|session| Box::new(session) as Box<dyn TextWebSocketSession>)
                .ok_or_else(|| ExchangeError::unavailable("no scripted websocket session"))
        }
    }

    #[derive(Debug)]
    struct ScriptedHttpTransport {
        requests: Mutex<Vec<RemoteHttpRequest>>,
        responses: Mutex<VecDeque<Result<RemoteHttpResponse, ExchangeError>>>,
    }

    #[derive(Debug)]
    enum CancellationSafeHttpAction {
        Response(Result<RemoteHttpResponse, ExchangeError>),
        Pending,
    }

    #[derive(Debug)]
    struct CancellationSafeHttpTransport {
        requests: Mutex<Vec<RemoteHttpRequest>>,
        actions: Mutex<VecDeque<CancellationSafeHttpAction>>,
    }

    #[async_trait]
    impl RemoteHttpTransport for CancellationSafeHttpTransport {
        async fn send(
            &self,
            request: RemoteHttpRequest,
        ) -> Result<RemoteHttpResponse, ExchangeError> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            let action = self
                .actions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted cancellation-safe transport ran out of actions");
            match action {
                CancellationSafeHttpAction::Response(response) => response,
                CancellationSafeHttpAction::Pending => std::future::pending().await,
            }
        }
    }

    #[async_trait]
    impl RemoteHttpTransport for ScriptedHttpTransport {
        async fn send(
            &self,
            request: RemoteHttpRequest,
        ) -> Result<RemoteHttpResponse, ExchangeError> {
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            self.responses
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted HTTP transport ran out of responses")
        }
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn production_testnet_soak_probe_emits_all_three_streaming_samples() {
        let symbols = BinanceSmokeSymbols {
            spot: Symbol::new("BTC-USDT-SPOT").unwrap(),
            perpetual: Symbol::new("BTC-USDT-PERP").unwrap(),
            wire_symbol: "BTCUSDT".to_owned(),
        };
        let clock = Arc::new(FixedStreamClock {
            now: Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap(),
        });
        let sleeper: Arc<dyn MarketStreamSleeper> = Arc::new(NoopStreamSleeper);
        let jitter = Arc::new(
            FixedMarketStreamJitter::new(10_000).expect("non-zero fixed jitter must be valid"),
        );
        let reconnect = MarketStreamReconnectPolicy::new(
            StdDuration::from_millis(1),
            StdDuration::from_secs(1),
        )
        .unwrap();
        let route = BinancePollingRoute::new(
            MarketInstrument::new("binance", symbols.spot.clone(), MarketType::Spot).unwrap(),
            Symbol::new("BTCUSDT").unwrap(),
        )
        .unwrap();
        let market_connector: Arc<dyn TextWebSocketConnector> =
            Arc::new(ScriptedWebSocketConnector::one(vec![
                TextWebSocketEvent::Text(
                    r#"{"u":7,"s":"BTCUSDT","b":"50000.0","B":"1.0","a":"50000.1","A":"2.0"}"#
                        .to_owned(),
                ),
            ]));
        let market_stream = BinanceBookTickerStreamSource::new(
            BinancePublicExchange::new().unwrap(),
            vec![route],
            market_connector,
            reconnect,
            Arc::clone(&clock),
            Arc::clone(&sleeper),
            jitter.clone(),
        )
        .unwrap();
        let user_connector: Arc<dyn TextWebSocketConnector> =
            Arc::new(ScriptedWebSocketConnector::one(vec![
                TextWebSocketEvent::Text(
                    r#"{"id":"user-data-subscribe","status":200,"result":{"subscriptionId":17}}"#
                        .to_owned(),
                ),
            ]));
        let user_stream =
            BinanceUserDataStreamSource::new(user_connector, reconnect, clock, sleeper, jitter);
        let http = Arc::new(ScriptedHttpTransport {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(
                (0..12)
                    .map(|_| Ok(RemoteHttpResponse::new(200, br"[]").unwrap()))
                    .collect(),
            ),
        });
        let transport: Arc<dyn RemoteHttpTransport> = http.clone();
        let signer = Arc::new(
            BinanceHmacSha256Signer::new("offline-api-key", "offline-api-secret").unwrap(),
        );
        let exchange = Arc::new(BinanceTestnetExchange::new(
            build_binance_read_only_protocol(signer, &symbols).unwrap(),
            transport,
        ));
        let history = JsonlHistory::new(temp_path("production-soak-owner"));
        let mut probe = ProductionBinanceTestnetSoakProbe::from_parts(
            market_stream,
            user_stream,
            exchange,
            "production-soak-owner",
            history.clone(),
            TestnetSoakLifecycleOwnerMode::ReadOnly,
        )
        .await
        .unwrap();

        assert_eq!(
            probe.next_probe().await.unwrap(),
            TestnetSoakSample::MarketStream
        );
        assert_eq!(
            probe.next_probe().await.unwrap(),
            TestnetSoakSample::UserDataStream
        );
        assert_eq!(
            probe.next_probe().await.unwrap(),
            TestnetSoakSample::AuthenticatedReconcile
        );
        let request_paths = http
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|request| request.url().path().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            request_paths,
            [
                "/api/v3/openOrders",
                "/fapi/v1/openOrders",
                "/fapi/v2/positionRisk",
                "/api/v3/openOrders",
                "/fapi/v1/openOrders",
                "/fapi/v2/positionRisk",
                "/api/v3/openOrders",
                "/fapi/v1/openOrders",
                "/fapi/v2/positionRisk",
                "/api/v3/openOrders",
                "/fapi/v1/openOrders",
                "/fapi/v2/positionRisk",
            ]
        );
        let journal = std::fs::read_to_string(history.path()).unwrap();
        assert!(journal.contains("continuous_testnet_user_stream_subscribed"));
        assert!(journal.contains("continuous_testnet_reconcile_verified"));
        assert!(!journal.contains("continuous_testnet_campaign_recovery_verified"));
        let path = history.path().to_owned();
        drop(probe);
        drop(history);
        let _ = std::fs::remove_file(path);
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn production_soak_retries_a_cancelled_lifecycle_query_first_without_losing_the_ack() {
        let symbols = BinanceSmokeSymbols {
            spot: Symbol::new("BTC-USDT-SPOT").unwrap(),
            perpetual: Symbol::new("BTC-USDT-PERP").unwrap(),
            wire_symbol: "BTCUSDT".to_owned(),
        };
        let config = soak_lifecycle_config(&symbols);
        let open = binance_order_response("NEW");
        let cancelled = binance_order_response("CANCELED");
        let mut actions = (0..6)
            .map(|_| {
                CancellationSafeHttpAction::Response(Ok(
                    RemoteHttpResponse::new(200, br"[]").unwrap()
                ))
            })
            .collect::<VecDeque<_>>();
        actions.push_back(CancellationSafeHttpAction::Pending);
        actions.extend([
            CancellationSafeHttpAction::Response(Ok(
                RemoteHttpResponse::new(200, open.as_bytes()).unwrap()
            )),
            CancellationSafeHttpAction::Response(Ok(RemoteHttpResponse::new(
                200,
                cancelled.as_bytes(),
            )
            .unwrap())),
            CancellationSafeHttpAction::Response(Ok(RemoteHttpResponse::new(
                200,
                cancelled.as_bytes(),
            )
            .unwrap())),
        ]);
        actions.extend((0..6).map(|_| {
            CancellationSafeHttpAction::Response(Ok(RemoteHttpResponse::new(200, br"[]").unwrap()))
        }));
        let http = Arc::new(CancellationSafeHttpTransport {
            requests: Mutex::new(Vec::new()),
            actions: Mutex::new(actions),
        });
        let transport: Arc<dyn RemoteHttpTransport> = http.clone();
        let (market, user) = scripted_soak_streams(&symbols, 91);
        let history = JsonlHistory::new(temp_path("production-soak-cancel-resume"));
        let mut probe = ProductionBinanceTestnetSoakProbe::from_parts(
            market,
            user,
            mutation_exchange(&symbols, transport),
            "production-soak-cancel-resume",
            history.clone(),
            TestnetSoakLifecycleOwnerMode::Fresh(config),
        )
        .await
        .unwrap();

        assert!(
            tokio::time::timeout(StdDuration::from_secs(1), probe.next_user_stream_sample())
                .await
                .is_err()
        );
        assert!(probe.pending_user_item.is_some());
        assert_eq!(
            http.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|request| request.method() == RemoteHttpMethod::Post)
                .count(),
            1,
            "the cancelled attempt must reach the durable planned -> submit boundary"
        );
        assert_eq!(
            tokio::time::timeout(StdDuration::from_secs(2), probe.next_user_stream_sample())
                .await
                .expect("query-first recovery must be bounded")
                .unwrap(),
            TestnetSoakSample::UserDataStream
        );
        assert!(probe.pending_user_item.is_none());

        let requests = http
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut mutating = requests
            .iter()
            .filter(|request| {
                matches!(
                    request.method(),
                    RemoteHttpMethod::Post | RemoteHttpMethod::Delete
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            mutating
                .iter()
                .filter(|request| request.method() == RemoteHttpMethod::Post)
                .count(),
            1
        );
        assert_eq!(mutating.remove(0).method(), RemoteHttpMethod::Post);
        assert_eq!(mutating.remove(0).method(), RemoteHttpMethod::Delete);
        drop(requests);
        let journal = std::fs::read_to_string(history.path()).unwrap();
        assert!(journal.contains("continuous_testnet_campaign_recovery_verified"));
        assert!(journal.contains("continuous_testnet_user_stream_subscribed"));
        let path = history.path().to_owned();
        drop(probe);
        drop(history);
        let _ = std::fs::remove_file(path);
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn production_soak_waits_for_subscription_then_recovers_pending_campaign_query_first() {
        let symbols = BinanceSmokeSymbols {
            spot: Symbol::new("BTC-USDT-SPOT").unwrap(),
            perpetual: Symbol::new("BTC-USDT-PERP").unwrap(),
            wire_symbol: "BTCUSDT".to_owned(),
        };
        let config = soak_lifecycle_config(&symbols);
        let history = JsonlHistory::new(temp_path("production-soak-restart"));
        let first_http = Arc::new(ScriptedHttpTransport {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(
                (0..6)
                    .map(|_| Ok(RemoteHttpResponse::new(200, br"[]").unwrap()))
                    .chain([
                        Err(ExchangeError::unavailable("fixture submit disconnect")),
                        Err(ExchangeError::unavailable("fixture query disconnect")),
                    ])
                    .collect(),
            ),
        });
        let first_transport: Arc<dyn RemoteHttpTransport> = first_http.clone();
        let (market, user) = scripted_soak_streams(&symbols, 81);
        let mut first = ProductionBinanceTestnetSoakProbe::from_parts(
            market,
            user,
            mutation_exchange(&symbols, first_transport),
            "production-soak-restart",
            history.clone(),
            TestnetSoakLifecycleOwnerMode::Fresh(config.clone()),
        )
        .await
        .unwrap();

        assert!(
            first_http
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| { request.method() != RemoteHttpMethod::Post })
        );
        assert!(first.next_user_stream_sample().await.is_err());
        assert!(
            first_http
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| { request.method() == RemoteHttpMethod::Post })
        );
        let first_journal = std::fs::read_to_string(history.path()).unwrap();
        assert!(first_journal.contains("testnet_lifecycle_planned"));
        assert!(!first_journal.contains("continuous_testnet_campaign_recovery_verified"));
        drop(first); // deterministic process-kill boundary: no graceful shutdown.

        let open = binance_order_response("NEW");
        let cancelled = binance_order_response("CANCELED");
        let recovery_http = Arc::new(ScriptedHttpTransport {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(
                [
                    Ok(RemoteHttpResponse::new(200, open.as_bytes()).unwrap()),
                    Ok(RemoteHttpResponse::new(200, cancelled.as_bytes()).unwrap()),
                    Ok(RemoteHttpResponse::new(200, cancelled.as_bytes()).unwrap()),
                ]
                .into_iter()
                .chain((0..6).map(|_| Ok(RemoteHttpResponse::new(200, br"[]").unwrap())))
                .collect(),
            ),
        });
        let recovery_transport: Arc<dyn RemoteHttpTransport> = recovery_http.clone();
        let (market, user) = scripted_soak_streams(&symbols, 82);
        let mut recovered = ProductionBinanceTestnetSoakProbe::from_parts(
            market,
            user,
            read_only_exchange(&symbols, recovery_transport),
            "production-soak-restart",
            history.clone(),
            TestnetSoakLifecycleOwnerMode::Recovery(config),
        )
        .await
        .unwrap();
        {
            let requests = recovery_http.requests.lock().unwrap();
            assert_eq!(requests[0].method(), RemoteHttpMethod::Get);
            assert_eq!(requests[0].url().path(), "/api/v3/order");
            assert!(
                requests[0]
                    .url()
                    .query()
                    .unwrap()
                    .contains("origClientOrderId=0f3c807d-776f-4de4-85d0-93760a82dfcf")
            );
            assert_eq!(requests[1].method(), RemoteHttpMethod::Delete);
            assert_eq!(requests[2].method(), RemoteHttpMethod::Get);
            assert!(
                requests
                    .iter()
                    .all(|request| request.method() != RemoteHttpMethod::Post)
            );
        }
        assert_eq!(
            recovered.next_user_stream_sample().await.unwrap(),
            TestnetSoakSample::UserDataStream
        );
        let journal = std::fs::read_to_string(history.path()).unwrap();
        assert!(journal.contains("continuous_testnet_campaign_recovery_verified"));
        assert!(journal.contains("\"query_delta\":2"));
        assert!(journal.contains("continuous_testnet_user_stream_subscribed"));

        let path = history.path().to_owned();
        drop(recovered);
        drop(history);
        let _ = std::fs::remove_file(path);
    }

    fn scripted_soak_streams(
        symbols: &BinanceSmokeSymbols,
        subscription_id: u64,
    ) -> (BinanceBookTickerStreamSource, BinanceUserDataStreamSource) {
        let clock = Arc::new(FixedStreamClock {
            now: Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap(),
        });
        let sleeper: Arc<dyn MarketStreamSleeper> = Arc::new(NoopStreamSleeper);
        let jitter = Arc::new(
            FixedMarketStreamJitter::new(10_000).expect("non-zero fixed jitter must be valid"),
        );
        let reconnect = MarketStreamReconnectPolicy::new(
            StdDuration::from_millis(1),
            StdDuration::from_secs(1),
        )
        .unwrap();
        let route = BinancePollingRoute::new(
            MarketInstrument::new("binance", symbols.spot.clone(), MarketType::Spot).unwrap(),
            Symbol::new("BTCUSDT").unwrap(),
        )
        .unwrap();
        let market = BinanceBookTickerStreamSource::new(
            BinancePublicExchange::new().unwrap(),
            vec![route],
            Arc::new(ScriptedWebSocketConnector::one(Vec::new())),
            reconnect,
            Arc::clone(&clock),
            Arc::clone(&sleeper),
            jitter.clone(),
        )
        .unwrap();
        let user = BinanceUserDataStreamSource::new(
            Arc::new(ScriptedWebSocketConnector::one(vec![
                TextWebSocketEvent::Text(format!(
                    r#"{{"id":"user-data-subscribe","status":200,"result":{{"subscriptionId":{subscription_id}}}}}"#
                )),
            ])),
            reconnect,
            clock,
            sleeper,
            jitter,
        );
        (market, user)
    }

    fn soak_lifecycle_config(symbols: &BinanceSmokeSymbols) -> TestnetLifecycleConfig {
        let mut intent = OrderIntent::limit(
            "binance",
            symbols.spot.clone(),
            MarketType::Spot,
            Side::Buy,
            Quantity::new(Decimal::new(1, 3)).unwrap(),
            Price::new(Decimal::new(490_001, 1)).unwrap(),
        );
        intent.client_order_id =
            uuid::Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
        intent.time_in_force = TimeInForce::PostOnly;
        TestnetLifecycleConfig::new(
            "production-soak-campaign",
            intent,
            "BTCUSDT",
            TestnetLifecycleObservation::Open,
            StdDuration::from_millis(1),
            4,
        )
        .unwrap()
    }

    fn read_only_exchange(
        symbols: &BinanceSmokeSymbols,
        transport: Arc<dyn RemoteHttpTransport>,
    ) -> Arc<BinanceTestnetExchange> {
        let signer = Arc::new(
            BinanceHmacSha256Signer::new("offline-api-key", "offline-api-secret").unwrap(),
        );
        Arc::new(BinanceTestnetExchange::new(
            build_binance_read_only_protocol(signer, symbols).unwrap(),
            transport,
        ))
    }

    fn mutation_exchange(
        symbols: &BinanceSmokeSymbols,
        transport: Arc<dyn RemoteHttpTransport>,
    ) -> Arc<BinanceTestnetExchange> {
        let signer = Arc::new(
            BinanceHmacSha256Signer::new("offline-api-key", "offline-api-secret").unwrap(),
        );
        let protocol = BinanceTestnetProtocol::authenticated(
            BinanceTestnetEndpoints::official(),
            ExchangeSymbolCatalog::new(vec![
                ExchangeSymbol::new("binance", symbols.spot.clone(), MarketType::Spot, "BTCUSDT")
                    .unwrap(),
            ])
            .unwrap(),
            InstrumentRuleCatalog::new(vec![
                InstrumentRules::new(
                    "binance",
                    symbols.spot.clone(),
                    MarketType::Spot,
                    Price::new(Decimal::new(1, 1)).unwrap(),
                    Quantity::new(Decimal::new(1, 4)).unwrap(),
                    Quantity::new(Decimal::new(1, 4)).unwrap(),
                    Money::new(Decimal::from(5)),
                )
                .unwrap(),
            ])
            .unwrap(),
            signer,
        )
        .unwrap();
        Arc::new(BinanceTestnetExchange::new(protocol, transport))
    }

    fn binance_order_response(status: &str) -> String {
        format!(
            r#"{{"symbol":"BTCUSDT","orderId":28,"clientOrderId":"0f3c807d-776f-4de4-85d0-93760a82dfcf","price":"49000.1","origQty":"0.001","executedQty":"0","status":"{status}","timeInForce":"GTC","type":"LIMIT_MAKER","side":"BUY"}}"#
        )
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

    #[tokio::test]
    async fn serve_error_cleanup_stops_the_testnet_soak_task() {
        let path = temp_path("serve-error-cleanup.jsonl");
        let mut task = TestnetSoakTask::start(
            TestnetSoakTaskConfig::new(
                "serve-error-cleanup",
                StdDuration::from_secs(1),
                StdDuration::from_secs(1),
                3,
            )
            .unwrap(),
            PendingSoakProbe,
            JsonlHistory::new(&path),
        )
        .await
        .unwrap();

        let error = stop_testnet_soak_task_after_serve_error(
            &mut task,
            TaskHostServeError::ControlToken(TaskHostControlTokenError::Missing("fixture")),
        )
        .await;

        assert!(
            error
                .to_string()
                .contains("testnet soak control host failed")
        );
        assert_eq!(task.status().phase, TestnetSoakTaskPhase::Stopped);
        let journal = std::fs::read_to_string(&path).unwrap();
        assert!(journal.contains("testnet_soak_stopped"), "{journal}");
        let _ = std::fs::remove_file(path);
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

    fn test_arbitrage_config(max_position_value: &str) -> ArbitrageConfig {
        load_arbitrage_config_from_str(&format!(
            r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [paper-left, paper-right]
symbols: [ETH-USDC-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.1
    grid_step: 0.1
    max_segments: 1
  quantity_config:
    base_quantity: 1
  risk_config:
    max_position_value: {max_position_value}
symbol_configs:
  ETH-USDC-PERP:
    enabled: true
"
        ))
        .unwrap()
    }

    fn test_arbitrage_markets(now: chrono::DateTime<Utc>) -> [MarketSnapshot; 2] {
        [
            MarketSnapshot::new(
                "paper-left",
                Symbol::new("ETH-USDC-PERP").unwrap(),
                MarketType::Perpetual,
                Price::new(Decimal::new(99, 0)).unwrap(),
                Price::new(Decimal::new(100, 0)).unwrap(),
                now,
            )
            .unwrap(),
            MarketSnapshot::new(
                "paper-right",
                Symbol::new("ETH-USDC-PERP").unwrap(),
                MarketType::Perpetual,
                Price::new(Decimal::new(101, 0)).unwrap(),
                Price::new(Decimal::new(102, 0)).unwrap(),
                now,
            )
            .unwrap(),
        ]
    }

    fn test_arbitrage_intents() -> [OrderIntent; 2] {
        [
            OrderIntent::market(
                "paper-left",
                Symbol::new("ETH-USDC-PERP").unwrap(),
                MarketType::Perpetual,
                Side::Buy,
                Quantity::new(Decimal::ONE).unwrap(),
            ),
            OrderIntent::market(
                "paper-right",
                Symbol::new("ETH-USDC-PERP").unwrap(),
                MarketType::Perpetual,
                Side::Sell,
                Quantity::new(Decimal::ONE).unwrap(),
            ),
        ]
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
    fn paper_once_arbitrage_uses_max_position_value_as_synthetic_buying_power_budget() {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        let config = test_arbitrage_config("201");
        let markets = test_arbitrage_markets(now);
        let intents = test_arbitrage_intents();

        let result = super::authorize_arbitrage_risk(
            &config,
            &intents,
            [&markets[0], &markets[1]],
            now,
            Duration::seconds(5),
        );

        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn paper_once_arbitrage_rejects_when_the_synthetic_budget_is_too_small() {
        let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
        let config = test_arbitrage_config("200");
        let markets = test_arbitrage_markets(now);
        let intents = test_arbitrage_intents();

        let error = super::authorize_arbitrage_risk(
            &config,
            &intents,
            [&markets[0], &markets[1]],
            now,
            Duration::seconds(5),
        )
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("OpeningNotionalExceedsBuyingPower"),
            "{error}"
        );
        assert!(
            error.contains("synthetic full-notional account budget"),
            "{error}"
        );
    }

    #[test]
    fn paper_once_snapshot_derives_market_type_from_symbol_suffix() {
        let spot = super::market_snapshot(
            "paper-left",
            &Symbol::new("ETH-USDC-SPOT").unwrap(),
            Decimal::new(99, 0),
            Decimal::new(100, 0),
            Decimal::ONE,
            Decimal::ONE,
        )
        .unwrap();
        let perp = super::market_snapshot(
            "paper-right",
            &Symbol::new("ETH-USDC-PERP").unwrap(),
            Decimal::new(101, 0),
            Decimal::new(102, 0),
            Decimal::ONE,
            Decimal::ONE,
        )
        .unwrap();
        let legacy = super::market_snapshot(
            "paper-legacy",
            &Symbol::new("ETH-USDC").unwrap(),
            Decimal::new(101, 0),
            Decimal::new(102, 0),
            Decimal::ONE,
            Decimal::ONE,
        )
        .unwrap();

        assert_eq!(spot.market_type, MarketType::Spot);
        assert_eq!(perp.market_type, MarketType::Perpetual);
        assert_eq!(legacy.market_type, MarketType::Perpetual);
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
