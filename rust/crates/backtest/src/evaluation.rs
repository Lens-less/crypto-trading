use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Money, Price, Quantity, Side, Symbol};
use crypto_trading_indicators::{PerformanceMetrics, RatioConfig, summarize_performance};
use rust_decimal::Decimal;

use crate::{
    BacktestError, DatasetManifest, EquityPoint, Liquidity, SpotBar, SpotKlineDataset,
    SpotStrategyConfig, TapeInstrument, Trade, TradeFill, ledger::Ledger,
};

/// Fixed-size rolling selection windows, embargo, and terminal holdout.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationSplitConfig {
    training_len: usize,
    test_len: usize,
    step_len: usize,
    embargo_len: usize,
    final_holdout_len: usize,
}

impl EvaluationSplitConfig {
    /// Creates a split configuration without inspecting any bar values.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidWalkForwardConfig`] when a required
    /// component is zero.
    pub fn new(
        training_len: usize,
        test_len: usize,
        step_len: usize,
        embargo_len: usize,
        final_holdout_len: usize,
    ) -> Result<Self, BacktestError> {
        if training_len == 0 || test_len == 0 || step_len == 0 || final_holdout_len == 0 {
            return Err(BacktestError::InvalidWalkForwardConfig);
        }

        Ok(Self {
            training_len,
            test_len,
            step_len,
            embargo_len,
            final_holdout_len,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationWindow {
    pub training_range: Range<usize>,
    pub embargo_range: Range<usize>,
    pub test_range: Range<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationPlan {
    windows: Vec<EvaluationWindow>,
    final_holdout_range: Range<usize>,
}

impl EvaluationPlan {
    /// Builds complete selection windows ending before one terminal holdout.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, overflow, or insufficient-data error
    /// instead of truncating a partial split.
    pub fn new(total_bars: usize, config: EvaluationSplitConfig) -> Result<Self, BacktestError> {
        let final_holdout_start = total_bars
            .checked_sub(config.final_holdout_len)
            .ok_or(BacktestError::InvalidWalkForwardConfig)?;
        let selection_end = final_holdout_start;
        let window_span = config
            .training_len
            .checked_add(config.embargo_len)
            .and_then(|value| value.checked_add(config.test_len))
            .ok_or(BacktestError::WalkForwardIndexOverflow)?;
        let mut windows = Vec::new();
        let mut start = 0usize;

        while start
            .checked_add(window_span)
            .is_some_and(|end| end <= selection_end)
        {
            let training_end = start
                .checked_add(config.training_len)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
            let embargo_end = training_end
                .checked_add(config.embargo_len)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
            let test_end = embargo_end
                .checked_add(config.test_len)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
            windows.push(EvaluationWindow {
                training_range: start..training_end,
                embargo_range: training_end..embargo_end,
                test_range: embargo_end..test_end,
            });
            start = start
                .checked_add(config.step_len)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
        }

        if windows.is_empty() {
            return Err(BacktestError::InsufficientEvaluationData);
        }

        Ok(Self {
            windows,
            final_holdout_range: final_holdout_start..total_bars,
        })
    }

    #[must_use]
    pub fn windows(&self) -> &[EvaluationWindow] {
        &self.windows
    }

    #[must_use]
    pub fn final_holdout_range(&self) -> Range<usize> {
        self.final_holdout_range.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredConfiguration {
    identifier: String,
    strategy: SpotStrategyConfig,
}

impl RegisteredConfiguration {
    /// Registers one family/configuration identifier before holdout access.
    ///
    /// # Errors
    ///
    /// Returns a typed error for a blank identifier or invalid bounded strategy
    /// parameters.
    pub fn new(
        identifier: impl Into<String>,
        strategy: SpotStrategyConfig,
    ) -> Result<Self, BacktestError> {
        let identifier = identifier.into().trim().to_owned();
        if identifier.is_empty() {
            return Err(BacktestError::InvalidWalkForwardConfig);
        }
        strategy.build()?;

        Ok(Self {
            identifier,
            strategy,
        })
    }

    #[must_use]
    pub fn family(&self) -> &str {
        self.strategy.family()
    }

    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Returns the exact bounded strategy parameters frozen for this id.
    #[must_use]
    pub const fn strategy(&self) -> SpotStrategyConfig {
        self.strategy
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SelectionPhase<'a> {
    dataset: &'a SpotKlineDataset,
    plan: EvaluationPlan,
}

impl<'a> SelectionPhase<'a> {
    /// Creates the selection-only view of a bar slice.
    ///
    /// # Errors
    ///
    /// Propagates split planning errors when no complete OOS window and final
    /// holdout fit.
    pub fn new(
        dataset: &'a SpotKlineDataset,
        config: EvaluationSplitConfig,
    ) -> Result<Self, BacktestError> {
        Ok(Self {
            dataset,
            plan: EvaluationPlan::new(dataset.bars().len(), config)?,
        })
    }

    #[must_use]
    pub fn selection_bars(&self) -> &'a [SpotBar] {
        &self.dataset.bars()[..self.plan.final_holdout_range.start]
    }

    /// Creates a provenance-bound sample wholly inside the selection region.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidEvaluationRange`] when the requested
    /// range is empty or touches the final holdout.
    pub fn sample(
        &self,
        range: Range<usize>,
    ) -> Result<VerifiedEvaluationSample<'a>, BacktestError> {
        if range.start >= range.end || range.end > self.plan.final_holdout_range.start {
            return Err(BacktestError::InvalidEvaluationRange);
        }
        Ok(VerifiedEvaluationSample {
            dataset: self.dataset,
            range,
        })
    }

    /// Returns one provenance-bound OOS test sample by window index.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidEvaluationRange`] for an unknown window.
    pub fn window_sample(
        &self,
        window_index: usize,
    ) -> Result<VerifiedEvaluationSample<'a>, BacktestError> {
        let range = self
            .plan
            .windows
            .get(window_index)
            .ok_or(BacktestError::InvalidEvaluationRange)?
            .test_range
            .clone();
        self.sample(range)
    }

    /// Returns the value-free split plan used by this selection phase.
    #[must_use]
    pub const fn plan(&self) -> &EvaluationPlan {
        &self.plan
    }

    /// Consumes selection and freezes the complete bounded search registry.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::SearchBudgetExceeded`] for an empty registry,
    /// duplicates, more than five families, or more than twenty configurations
    /// in one family.
    pub fn freeze(
        self,
        registered_configurations: Vec<RegisteredConfiguration>,
        protocol: EvaluationProtocol,
    ) -> Result<FrozenSelection<'a>, BacktestError> {
        enforce_search_budget(&registered_configurations)?;
        Ok(FrozenSelection {
            dataset: self.dataset,
            plan: self.plan,
            registered_configurations,
            protocol,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FrozenSelection<'a> {
    dataset: &'a SpotKlineDataset,
    plan: EvaluationPlan,
    registered_configurations: Vec<RegisteredConfiguration>,
    protocol: EvaluationProtocol,
}

impl<'a> FrozenSelection<'a> {
    #[must_use]
    pub fn registered_configurations(&self) -> &[RegisteredConfiguration] {
        &self.registered_configurations
    }

    #[must_use]
    pub fn open_final_holdout(self) -> FinalHoldoutPhase<'a> {
        FinalHoldoutPhase {
            dataset: self.dataset,
            range: self.plan.final_holdout_range,
            registered_configurations: self.registered_configurations,
            protocol: self.protocol,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FinalHoldoutPhase<'a> {
    dataset: &'a SpotKlineDataset,
    range: Range<usize>,
    registered_configurations: Vec<RegisteredConfiguration>,
    protocol: EvaluationProtocol,
}

impl FinalHoldoutPhase<'_> {
    /// Returns the frozen registry without exposing holdout bar values.
    #[must_use]
    pub fn registered_configurations(&self) -> &[RegisteredConfiguration] {
        &self.registered_configurations
    }

    /// Evaluates exactly one frozen configuration without exposing raw holdout
    /// bars or accepting a caller-supplied strategy object.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::UnregisteredHoldoutConfiguration`] for an
    /// unknown id and otherwise propagates bounded strategy/evaluation errors.
    pub fn evaluate_registered(
        &self,
        identifier: &str,
    ) -> Result<CostSensitivityEvaluation, BacktestError> {
        let registered = self
            .registered_configurations
            .iter()
            .find(|configuration| configuration.identifier() == identifier)
            .ok_or(BacktestError::UnregisteredHoldoutConfiguration)?;
        self.protocol.evaluate(
            &VerifiedEvaluationSample {
                dataset: self.dataset,
                range: self.range.clone(),
            },
            registered.strategy(),
        )
    }
}

/// Provenance-bound range that cannot be created from arbitrary raw bars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedEvaluationSample<'a> {
    dataset: &'a SpotKlineDataset,
    range: Range<usize>,
}

impl VerifiedEvaluationSample<'_> {
    /// Returns the frozen manifest carried into every evaluation result path.
    #[must_use]
    pub fn manifest(&self) -> &DatasetManifest {
        self.dataset.manifest()
    }

    /// Returns every ordered archive manifest backing this verified sample.
    #[must_use]
    pub fn manifests(&self) -> &[DatasetManifest] {
        self.dataset.manifests()
    }

    /// Returns value-free absolute sample boundaries.
    #[must_use]
    pub fn range(&self) -> Range<usize> {
        self.range.clone()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CostBreakdown {
    pub fee: Money,
    pub half_spread: Money,
    pub slippage: Money,
    pub latency: Money,
    pub total: Money,
}

impl CostBreakdown {
    fn add(self, other: Self) -> Result<Self, BacktestError> {
        let fee = add_money(self.fee, other.fee)?;
        let half_spread = add_money(self.half_spread, other.half_spread)?;
        let slippage = add_money(self.slippage, other.slippage)?;
        let latency = add_money(self.latency, other.latency)?;
        let total = add_money(self.total, other.total)?;
        Ok(Self {
            fee,
            half_spread,
            slippage,
            latency,
            total,
        })
    }
}

/// Per-side taker cost assumptions expressed in basis points.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostSchedule {
    fee_bps: Decimal,
    half_spread_bps: Decimal,
    slippage_bps: Decimal,
    latency_bps: Decimal,
}

impl CostSchedule {
    /// Creates a separated adverse taker-cost schedule.
    ///
    /// # Errors
    ///
    /// Returns a typed basis-point or arithmetic error for negative values or
    /// price impact that could make a sell fill non-positive.
    pub fn new(
        fee_bps: Decimal,
        half_spread_bps: Decimal,
        slippage_bps: Decimal,
        latency_bps: Decimal,
    ) -> Result<Self, BacktestError> {
        for value in [fee_bps, half_spread_bps, slippage_bps, latency_bps] {
            if value < Decimal::ZERO {
                return Err(BacktestError::NegativeBasisPoints);
            }
        }
        if half_spread_bps
            .checked_add(slippage_bps)
            .and_then(|value| value.checked_add(latency_bps))
            .ok_or(BacktestError::ArithmeticOverflow)?
            >= bps_denominator()
        {
            return Err(BacktestError::InvalidSlippageBasisPoints);
        }

        Ok(Self {
            fee_bps,
            half_spread_bps,
            slippage_bps,
            latency_bps,
        })
    }

    /// Doubles every component without refitting or changing its definition.
    ///
    /// # Errors
    ///
    /// Returns an arithmetic or invalid-impact error when the doubled schedule
    /// cannot be represented safely.
    pub fn doubled(self) -> Result<Self, BacktestError> {
        Self::new(
            checked_double(self.fee_bps)?,
            checked_double(self.half_spread_bps)?,
            checked_double(self.slippage_bps)?,
            checked_double(self.latency_bps)?,
        )
    }

    fn impact_bps(self) -> Result<Decimal, BacktestError> {
        self.half_spread_bps
            .checked_add(self.slippage_bps)
            .and_then(|value| value.checked_add(self.latency_bps))
            .ok_or(BacktestError::ArithmeticOverflow)
    }

    fn costs_for(
        self,
        reference_price: Price,
        quantity: Quantity,
        fill_price: Price,
    ) -> Result<CostBreakdown, BacktestError> {
        let reference_notional = notional(reference_price, quantity)?;
        let fee = notional(fill_price, quantity)?
            .as_decimal()
            .checked_mul(
                self.fee_bps
                    .checked_div(bps_denominator())
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .map(Money::new)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let half_spread = component_cost(reference_notional, self.half_spread_bps)?;
        let slippage = component_cost(reference_notional, self.slippage_bps)?;
        let latency = component_cost(reference_notional, self.latency_bps)?;
        let total = add_money(add_money(fee, half_spread)?, add_money(slippage, latency)?)?;
        Ok(CostBreakdown {
            fee,
            half_spread,
            slippage,
            latency,
            total,
        })
    }
}

/// Immutable cash and cost assumptions applied to both sensitivity runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationProtocol {
    initial_cash: Money,
    one_x_costs: CostSchedule,
}

impl EvaluationProtocol {
    /// Freezes initial cash and the one-times cost schedule before holdout access.
    ///
    /// # Errors
    ///
    /// Returns a typed evaluator or doubled-cost validation error when either
    /// the one-times or two-times protocol cannot be executed safely.
    pub fn new(initial_cash: Money, one_x_costs: CostSchedule) -> Result<Self, BacktestError> {
        CausalSpotEvaluator::new(initial_cash, one_x_costs)?;
        one_x_costs.doubled()?;
        Ok(Self {
            initial_cash,
            one_x_costs,
        })
    }

    /// Evaluates one frozen strategy at exactly one-times and two-times costs.
    ///
    /// # Errors
    ///
    /// Propagates bounded-strategy construction, cost-doubling, and causal
    /// evaluation errors. Both runs use fresh strategy state.
    pub fn evaluate(
        &self,
        sample: &VerifiedEvaluationSample<'_>,
        strategy: SpotStrategyConfig,
    ) -> Result<CostSensitivityEvaluation, BacktestError> {
        let mut one_x_strategy = strategy.build()?;
        let one_x = CausalSpotEvaluator::new(self.initial_cash, self.one_x_costs)?
            .run(sample, &mut one_x_strategy)?;

        let mut two_x_strategy = strategy.build()?;
        let two_x = CausalSpotEvaluator::new(self.initial_cash, self.one_x_costs.doubled()?)?
            .run(sample, &mut two_x_strategy)?;

        Ok(CostSensitivityEvaluation { one_x, two_x })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotDecisionContext<'a> {
    pub history: &'a [SpotBar],
    pub decided_at: DateTime<Utc>,
    pub bar_index: usize,
    pub current_target: Decimal,
}

pub trait TargetExposureStrategy {
    /// Returns the next target using completed history only.
    ///
    /// # Errors
    ///
    /// Returns a typed evaluation error when the strategy cannot form a valid
    /// causal decision.
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalTradeRecord {
    pub trade: Trade,
    pub costs: CostBreakdown,
    pub target_exposure: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalSpotMetrics {
    pub total_costs: CostBreakdown,
    pub ending_equity: Money,
    pub net_return: Decimal,
    pub turnover: Decimal,
    pub trade_count: usize,
    pub average_exposure: Decimal,
    pub periods_per_year: Option<Decimal>,
    pub annualized_volatility: Option<Decimal>,
    pub performance: PerformanceMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalSpotEvaluation {
    pub trades: Vec<CausalTradeRecord>,
    pub equity_curve: Vec<EquityPoint>,
    pub metrics: CausalSpotMetrics,
}

/// Paired results under the frozen one-times and two-times cost schedules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostSensitivityEvaluation {
    pub one_x: CausalSpotEvaluation,
    pub two_x: CausalSpotEvaluation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalSpotEvaluator {
    initial_cash: Money,
    costs: CostSchedule,
    instrument: TapeInstrument,
}

impl CausalSpotEvaluator {
    /// Creates a Spot-only evaluator with strictly positive initial cash.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidInitialCash`] for non-positive cash or
    /// a domain error if the fixed research instrument cannot be represented.
    pub fn new(initial_cash: Money, costs: CostSchedule) -> Result<Self, BacktestError> {
        if initial_cash.as_decimal() <= Decimal::ZERO {
            return Err(BacktestError::InvalidInitialCash);
        }

        Ok(Self {
            initial_cash,
            costs,
            instrument: TapeInstrument {
                exchange: "binance".to_owned(),
                symbol: Symbol::new("BTC-USDT-SPOT")?,
                market_type: MarketType::Spot,
            },
        })
    }

    /// Evaluates close decisions at the next eligible open and liquidates any
    /// residual long exposure at the common terminal close convention.
    ///
    /// # Errors
    ///
    /// Returns typed range, bar, strategy, exposure, arithmetic, buying-power,
    /// inventory, or metric errors; unsupported states never receive a fill.
    #[allow(clippy::too_many_lines)]
    pub fn run<S: TargetExposureStrategy>(
        &self,
        sample: &VerifiedEvaluationSample<'_>,
        strategy: &mut S,
    ) -> Result<CausalSpotEvaluation, BacktestError> {
        let bars = sample.dataset.bars();
        let range = sample.range.clone();
        if range.start >= range.end || range.end > bars.len() {
            return Err(BacktestError::InvalidEvaluationRange);
        }
        validate_evaluation_bars(&bars[..range.end])?;

        let mut ledger = Ledger::new(self.initial_cash)?;
        let mut trades = Vec::new();
        let mut equity_curve = Vec::with_capacity(range.len());
        let mut closed_trade_pnls = Vec::new();
        let mut total_costs = CostBreakdown::default();
        let mut turnover_notional = Decimal::ZERO;
        let mut exposure_sum = Decimal::ZERO;
        let mut exposure_count = 0usize;
        let mut current_target = Decimal::ZERO;
        let mut pending_target = if range.start == 0 {
            None
        } else {
            let prior_index = range.start - 1;
            let context = SpotDecisionContext {
                history: &bars[..range.start],
                decided_at: bars[prior_index].close_time,
                bar_index: prior_index,
                current_target,
            };
            let target = strategy.target_exposure(&context)?;
            validate_target_exposure(target)?;
            Some(target)
        };

        for index in range.clone() {
            let bar = &bars[index];

            if let Some(target_exposure) = pending_target.take()
                && target_exposure != current_target
            {
                let pre_open = ledger.snapshot(bar.open)?;
                if let Some(record) = self.execute_rebalance(
                    &mut ledger,
                    &pre_open,
                    bar.open,
                    bar.open_time,
                    target_exposure,
                )? {
                    turnover_notional = turnover_notional
                        .checked_add(
                            record
                                .trade
                                .fill
                                .reference_price
                                .as_decimal()
                                .checked_mul(record.trade.fill.quantity.as_decimal())
                                .ok_or(BacktestError::ArithmeticOverflow)?,
                        )
                        .ok_or(BacktestError::ArithmeticOverflow)?;
                    total_costs = total_costs.add(record.costs)?;
                    if let Some(pnl) = record.trade.closed_trade_pnl {
                        closed_trade_pnls.push(pnl.as_decimal());
                    }
                    trades.push(record);
                }
                current_target = exposure_ratio(&ledger.snapshot(bar.open)?, bar.open)?;
            }

            let close_snapshot = ledger.snapshot(bar.close)?;
            exposure_sum = exposure_sum
                .checked_add(exposure_ratio(&close_snapshot, bar.close)?)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            exposure_count = exposure_count
                .checked_add(1)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            equity_curve.push(EquityPoint {
                occurred_at: bar.close_time,
                price: bar.close,
                equity: close_snapshot.equity,
            });

            let context = SpotDecisionContext {
                history: &bars[..=index],
                decided_at: bar.close_time,
                bar_index: index,
                current_target,
            };
            let next_target = strategy.target_exposure(&context)?;
            validate_target_exposure(next_target)?;
            pending_target = Some(next_target);
        }

        let terminal_bar = &bars[range.end - 1];
        let terminal_snapshot = ledger.snapshot(terminal_bar.close)?;
        if terminal_snapshot.position_qty > Decimal::ZERO
            && let Some(record) = self.execute_rebalance(
                &mut ledger,
                &terminal_snapshot,
                terminal_bar.close,
                terminal_bar.close_time,
                Decimal::ZERO,
            )?
        {
            turnover_notional = turnover_notional
                .checked_add(
                    record
                        .trade
                        .fill
                        .reference_price
                        .as_decimal()
                        .checked_mul(record.trade.fill.quantity.as_decimal())
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?;
            total_costs = total_costs.add(record.costs)?;
            if let Some(pnl) = record.trade.closed_trade_pnl {
                closed_trade_pnls.push(pnl.as_decimal());
            }
            trades.push(record);
            let post_terminal = ledger.snapshot(terminal_bar.close)?;
            if let Some(last_point) = equity_curve.last_mut() {
                last_point.equity = post_terminal.equity;
            }
        }

        let ending_equity = equity_curve
            .last()
            .map_or(self.initial_cash, |point| point.equity);
        let net_return = ending_equity
            .as_decimal()
            .checked_sub(self.initial_cash.as_decimal())
            .and_then(|value| value.checked_div(self.initial_cash.as_decimal()))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let turnover = turnover_notional
            .checked_div(self.initial_cash.as_decimal())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let average_exposure = if exposure_count == 0 {
            Decimal::ZERO
        } else {
            exposure_sum
                .checked_div(Decimal::from(
                    u64::try_from(exposure_count).map_err(|_| BacktestError::ArithmeticOverflow)?,
                ))
                .ok_or(BacktestError::ArithmeticOverflow)?
        };
        let equity_values: Vec<Decimal> = equity_curve
            .iter()
            .map(|point| point.equity.as_decimal())
            .collect();
        let returns = equity_returns(&equity_values)?;
        let (ratio_config, periods_per_year) = ratio_config_for_bars(&bars[range.clone()])?;
        let annualized_volatility = annualized_volatility(&returns, periods_per_year)?;
        let performance =
            summarize_performance(&equity_values, &closed_trade_pnls, &returns, ratio_config)?;
        let trade_count = trades.len();

        Ok(CausalSpotEvaluation {
            trades,
            equity_curve,
            metrics: CausalSpotMetrics {
                total_costs,
                ending_equity,
                net_return,
                turnover,
                trade_count,
                average_exposure,
                periods_per_year,
                annualized_volatility,
                performance,
            },
        })
    }

    fn execute_rebalance(
        &self,
        ledger: &mut Ledger,
        current: &crate::LedgerSnapshot,
        reference_price: Price,
        occurred_at: DateTime<Utc>,
        target_exposure: Decimal,
    ) -> Result<Option<CausalTradeRecord>, BacktestError> {
        let current_quantity = current.position_qty.max(Decimal::ZERO);
        let target_notional = current
            .equity
            .as_decimal()
            .max(Decimal::ZERO)
            .checked_mul(target_exposure)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let target_quantity = target_notional
            .checked_div(reference_price.as_decimal())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let delta = target_quantity
            .checked_sub(current_quantity)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if delta.is_zero() {
            return Ok(None);
        }

        let (side, quantity) = if delta > Decimal::ZERO {
            let affordable = self.maximum_affordable_quantity(current.cash, reference_price)?;
            let quantity = delta.min(affordable);
            if quantity <= Decimal::ZERO {
                return Ok(None);
            }
            (Side::Buy, Quantity::new(quantity)?)
        } else {
            (
                Side::Sell,
                Quantity::new(delta.abs()).map_err(BacktestError::from)?,
            )
        };
        let fill = self.synthetic_fill(reference_price, occurred_at, side, quantity)?;
        let costs = self
            .costs
            .costs_for(fill.reference_price, fill.quantity, fill.fill_price)?;
        let applied = ledger.apply_fill(&fill)?;
        let post_fill = ledger.snapshot(reference_price)?;

        Ok(Some(CausalTradeRecord {
            trade: Trade {
                fill,
                realized_pnl_delta: applied.realized_pnl_delta,
                closed_trade_pnl: applied.closed_trade_pnl,
                cumulative_realized_pnl: post_fill.realized_pnl,
                position_qty: post_fill.position_qty,
                equity: post_fill.equity,
            },
            costs,
            target_exposure,
        }))
    }

    fn synthetic_fill(
        &self,
        reference_price: Price,
        occurred_at: DateTime<Utc>,
        side: Side,
        quantity: Quantity,
    ) -> Result<TradeFill, BacktestError> {
        let impact = self
            .costs
            .impact_bps()?
            .checked_div(bps_denominator())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let fill_price = match side {
            Side::Buy => reference_price
                .as_decimal()
                .checked_mul(
                    Decimal::ONE
                        .checked_add(impact)
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
            Side::Sell => reference_price
                .as_decimal()
                .checked_mul(
                    Decimal::ONE
                        .checked_sub(impact)
                        .ok_or(BacktestError::ArithmeticOverflow)?,
                )
                .ok_or(BacktestError::ArithmeticOverflow)?,
        };
        let fill_price = Price::new(fill_price)?;
        let fee = notional(fill_price, quantity)?
            .as_decimal()
            .checked_mul(
                self.costs
                    .fee_bps
                    .checked_div(bps_denominator())
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .map(Money::new)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        Ok(TradeFill {
            occurred_at,
            side,
            quantity,
            liquidity: Liquidity::Taker,
            reference_price,
            fill_price,
            fee,
            instrument: Some(self.instrument.clone()),
        })
    }

    fn maximum_affordable_quantity(
        &self,
        cash: Money,
        reference_price: Price,
    ) -> Result<Decimal, BacktestError> {
        let impact = self
            .costs
            .impact_bps()?
            .checked_div(bps_denominator())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let fill_price = reference_price
            .as_decimal()
            .checked_mul(
                Decimal::ONE
                    .checked_add(impact)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let fee_rate = self
            .costs
            .fee_bps
            .checked_div(bps_denominator())
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let cost_per_unit = fill_price
            .checked_mul(
                Decimal::ONE
                    .checked_add(fee_rate)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let mut quantity = cash
            .as_decimal()
            .checked_div(cost_per_unit)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let representable_step = Decimal::new(1, quantity.scale());

        // Division and the ledger's notional-plus-fee calculation round in a
        // different order. Move down by exact representable units until the
        // quantity is affordable under the same arithmetic the ledger uses.
        for _ in 0..=2 {
            if required_buying_power(fill_price, fee_rate, quantity)? <= cash.as_decimal() {
                return Ok(quantity.max(Decimal::ZERO));
            }
            quantity = quantity
                .checked_sub(representable_step)
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }

        Err(BacktestError::ArithmeticOverflow)
    }
}

fn required_buying_power(
    fill_price: Decimal,
    fee_rate: Decimal,
    quantity: Decimal,
) -> Result<Decimal, BacktestError> {
    let notional = fill_price
        .checked_mul(quantity)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    notional
        .checked_add(
            notional
                .checked_mul(fee_rate)
                .ok_or(BacktestError::ArithmeticOverflow)?,
        )
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn enforce_search_budget(
    registered_configurations: &[RegisteredConfiguration],
) -> Result<(), BacktestError> {
    if registered_configurations.is_empty() {
        return Err(BacktestError::SearchBudgetExceeded);
    }
    let mut families = BTreeMap::<&str, usize>::new();
    let mut unique = BTreeSet::new();
    let mut strategies = Vec::new();
    for configuration in registered_configurations {
        if !unique.insert((configuration.family(), configuration.identifier()))
            || strategies.contains(&configuration.strategy())
        {
            return Err(BacktestError::SearchBudgetExceeded);
        }
        strategies.push(configuration.strategy());
        let counter = families.entry(configuration.family()).or_default();
        *counter = counter
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if *counter > 20 {
            return Err(BacktestError::SearchBudgetExceeded);
        }
    }
    if families.len() > 5 {
        return Err(BacktestError::SearchBudgetExceeded);
    }
    Ok(())
}

fn validate_target_exposure(value: Decimal) -> Result<(), BacktestError> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
        return Err(BacktestError::InvalidTargetExposure);
    }
    Ok(())
}

fn validate_evaluation_bars(bars: &[SpotBar]) -> Result<(), BacktestError> {
    if bars.is_empty()
        || bars.iter().any(|bar| bar.close_time < bar.open_time)
        || bars.windows(2).any(|pair| {
            pair[1].open_time <= pair[0].open_time || pair[1].open_time <= pair[0].close_time
        })
    {
        return Err(BacktestError::InvalidBarSequence);
    }
    Ok(())
}

fn exposure_ratio(snapshot: &crate::LedgerSnapshot, mark: Price) -> Result<Decimal, BacktestError> {
    if snapshot.equity.as_decimal() <= Decimal::ZERO {
        return Ok(Decimal::ZERO);
    }
    let exposure = snapshot
        .position_qty
        .checked_mul(mark.as_decimal())
        .and_then(|value| value.checked_div(snapshot.equity.as_decimal()))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Ok(exposure.max(Decimal::ZERO).min(Decimal::ONE))
}

fn component_cost(notional: Money, bps: Decimal) -> Result<Money, BacktestError> {
    notional
        .as_decimal()
        .checked_mul(
            bps.checked_div(bps_denominator())
                .ok_or(BacktestError::ArithmeticOverflow)?,
        )
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn notional(price: Price, quantity: Quantity) -> Result<Money, BacktestError> {
    price
        .as_decimal()
        .checked_mul(quantity.as_decimal())
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn add_money(left: Money, right: Money) -> Result<Money, BacktestError> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .map(Money::new)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn checked_double(value: Decimal) -> Result<Decimal, BacktestError> {
    value
        .checked_mul(Decimal::TWO)
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn equity_returns(equity_curve: &[Decimal]) -> Result<Vec<Decimal>, BacktestError> {
    if equity_curve.windows(2).any(|pair| pair[0] <= Decimal::ZERO) {
        return Ok(Vec::new());
    }

    equity_curve
        .windows(2)
        .map(|pair| {
            pair[1]
                .checked_sub(pair[0])
                .and_then(|delta| delta.checked_div(pair[0]))
                .ok_or(BacktestError::ArithmeticOverflow)
        })
        .collect()
}

fn ratio_config_for_bars(
    bars: &[SpotBar],
) -> Result<(RatioConfig, Option<Decimal>), BacktestError> {
    let Some((first, last)) = bars.first().zip(bars.last()) else {
        return Ok((RatioConfig::default(), None));
    };
    let periods = bars.len().saturating_sub(1);
    if periods == 0 {
        return Ok((RatioConfig::default(), None));
    }
    let elapsed_nanos = last
        .close_time
        .signed_duration_since(first.close_time)
        .num_nanoseconds()
        .ok_or(BacktestError::ArithmeticOverflow)?;
    if elapsed_nanos <= 0 {
        return Ok((RatioConfig::default(), None));
    }
    let periods =
        Decimal::from(u64::try_from(periods).map_err(|_| BacktestError::ArithmeticOverflow)?);
    let periods_per_year = periods
        .checked_mul(Decimal::from(31_536_000_000_000_000_i64))
        .and_then(|value| value.checked_div(Decimal::from(elapsed_nanos)))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Ok((
        RatioConfig::new(periods_per_year, Decimal::ZERO)?,
        Some(periods_per_year),
    ))
}

fn annualized_volatility(
    returns: &[Decimal],
    periods_per_year: Option<Decimal>,
) -> Result<Option<Decimal>, BacktestError> {
    if returns.len() < 2 {
        return Ok(None);
    }
    let Some(periods_per_year) = periods_per_year else {
        return Ok(None);
    };
    let count =
        Decimal::from(u64::try_from(returns.len()).map_err(|_| BacktestError::ArithmeticOverflow)?);
    let mean = returns
        .iter()
        .try_fold(Decimal::ZERO, |sum, value| {
            sum.checked_add(*value)
                .ok_or(BacktestError::ArithmeticOverflow)
        })?
        .checked_div(count)
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let sum_squared = returns.iter().try_fold(Decimal::ZERO, |sum, value| {
        let deviation = value
            .checked_sub(mean)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        sum.checked_add(
            deviation
                .checked_mul(deviation)
                .ok_or(BacktestError::ArithmeticOverflow)?,
        )
        .ok_or(BacktestError::ArithmeticOverflow)
    })?;
    let variance = sum_squared
        .checked_div(
            count
                .checked_sub(Decimal::ONE)
                .ok_or(BacktestError::ArithmeticOverflow)?,
        )
        .and_then(|value| value.checked_mul(periods_per_year))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    Ok(Some(checked_sqrt(variance)?))
}

fn checked_sqrt(value: Decimal) -> Result<Decimal, BacktestError> {
    if value < Decimal::ZERO {
        return Err(BacktestError::ArithmeticOverflow);
    }
    if value.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let two = Decimal::TWO;
    let mut guess = if value > Decimal::ONE {
        value
            .checked_div(two)
            .ok_or(BacktestError::ArithmeticOverflow)?
    } else {
        Decimal::ONE
    };
    let tolerance = Decimal::from_parts(1, 0, 0, false, 18);
    for _ in 0..64 {
        let next = guess
            .checked_add(
                value
                    .checked_div(guess)
                    .ok_or(BacktestError::ArithmeticOverflow)?,
            )
            .and_then(|sum| sum.checked_div(two))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if next
            .checked_sub(guess)
            .ok_or(BacktestError::ArithmeticOverflow)?
            .abs()
            <= tolerance
        {
            return Ok(next.round_dp(18));
        }
        guess = next;
    }
    Ok(guess.round_dp(18))
}

const fn bps_denominator() -> Decimal {
    Decimal::from_parts(10_000, 0, 0, false, 0)
}
