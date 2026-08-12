use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    ops::Range,
};

use chrono::SecondsFormat;
use crypto_trading_domain::{MarketType, Money};
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    BacktestError, CostSchedule, CostSensitivityEvaluation, DatasetManifest, EvaluationPlan,
    EvaluationProtocol, EvaluationSplitConfig, FinalHoldoutPhase, FrozenSelection,
    RegisteredConfiguration, SelectionPhase, Sha256Digest, SpotKlineDataset, SpotStrategyConfig,
    TimestampUnit,
};

const CASH_BASELINE_ID: &str = "cash";
const BUY_AND_HOLD_BASELINE_ID: &str = "buy-and-hold";
const MIN_AVAILABLE_SHARPE_OBSERVATIONS: usize = 6;
const REQUIRED_REGISTRY_LEN: usize = 22;
const REQUIRED_FAMILY_COUNTS: [(&str, usize); 5] = [
    ("cash", 1),
    ("buy_and_hold", 1),
    ("slow_time_series_momentum", 5),
    ("long_only_donchian", 3),
    ("capped_volatility_target", 12),
];

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ExperimentError {
    #[error("experiment runner version must not be blank")]
    BlankRunnerVersion,
    #[error("bootstrap replicates must be strictly positive")]
    InvalidBootstrapReplicates,
    #[error("promotion thresholds must stay within their bounded ranges")]
    InvalidPromotionThresholds,
    #[error("experiment dataset must be Binance Spot provenance")]
    NonSpotDataset,
    #[error("frozen registry must contain exactly {expected} configurations, found {actual}")]
    InvalidRegistryBudget { expected: usize, actual: usize },
    #[error("frozen registry family counts do not match the pre-registered protocol budget")]
    InvalidFamilyBudget,
    #[error(
        "frozen registry does not exactly match the pre-registered protocol identifiers, parameters, and order"
    )]
    RegistryDoesNotMatchPreregistration,
    #[error("frozen registry identifiers must be unique")]
    DuplicateIdentifier,
    #[error("frozen registry strategies must be unique")]
    DuplicateStrategy,
    #[error("registry must contain baseline `{0}` with the matching strategy")]
    MissingBaseline(&'static str),
    #[error("selection phase was built from a different split geometry")]
    SelectionPlanMismatch,
    #[error("selection phase dataset provenance does not match the frozen experiment plan")]
    DatasetFingerprintMismatch,
    #[error("selection summary is missing baseline `{0}`")]
    MissingSelectionBaseline(&'static str),
    #[error("selection summary is missing registered configuration `{0}`")]
    MissingSelectionConfiguration(String),
    #[error("holdout identifier `{0}` is not part of the sealed registry")]
    UnknownHoldoutIdentifier(String),
    #[error("holdout identifier `{0}` has already been evaluated")]
    DuplicateHoldoutEvaluation(String),
    #[error("holdout is incomplete; {remaining} identifiers remain")]
    IncompleteHoldout { remaining: usize },
    #[error("selection artifact persistence failed: {0}")]
    SelectionArtifactPersistenceFailed(String),
    #[error(transparent)]
    Backtest(#[from] BacktestError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExperimentSplitSpec {
    pub training_len: usize,
    pub test_len: usize,
    pub step_len: usize,
    pub embargo_len: usize,
    pub final_holdout_len: usize,
}

impl ExperimentSplitSpec {
    /// Builds the validated evaluator split configuration.
    ///
    /// # Errors
    ///
    /// Returns a typed backtest error when any required split component is zero.
    pub fn build(self) -> Result<EvaluationSplitConfig, BacktestError> {
        EvaluationSplitConfig::new(
            self.training_len,
            self.test_len,
            self.step_len,
            self.embargo_len,
            self.final_holdout_len,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostScheduleSpec {
    pub fee_bps: Decimal,
    pub half_spread_bps: Decimal,
    pub slippage_bps: Decimal,
    pub latency_bps: Decimal,
}

impl CostScheduleSpec {
    /// Builds the validated one-times component cost schedule.
    ///
    /// # Errors
    ///
    /// Returns a typed backtest error when a component cost is negative.
    pub fn build(self) -> Result<CostSchedule, BacktestError> {
        CostSchedule::new(
            self.fee_bps,
            self.half_spread_bps,
            self.slippage_bps,
            self.latency_bps,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationProtocolSpec {
    pub initial_cash: Money,
    pub one_x_costs: CostScheduleSpec,
}

impl EvaluationProtocolSpec {
    /// Builds the immutable cash and one-times/two-times cost protocol.
    ///
    /// # Errors
    ///
    /// Returns a typed backtest error for invalid cash or component costs.
    pub fn build(self) -> Result<EvaluationProtocol, BacktestError> {
        EvaluationProtocol::new(self.initial_cash, self.one_x_costs.build()?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub replicates: usize,
    pub base_seed: u64,
}

impl BootstrapConfig {
    /// Validates the deterministic bootstrap budget.
    ///
    /// # Errors
    ///
    /// Returns [`ExperimentError::InvalidBootstrapReplicates`] for a zero budget.
    pub fn validate(self) -> Result<Self, ExperimentError> {
        if self.replicates == 0 {
            return Err(ExperimentError::InvalidBootstrapReplicates);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromotionThresholds {
    pub selection_median_sharpe_min: Decimal,
    pub holdout_profit_factor_min: Decimal,
    pub holdout_max_drawdown_max: Decimal,
    pub selection_positive_window_ratio_min: Decimal,
}

impl PromotionThresholds {
    /// Validates all conjunctive promotion thresholds.
    ///
    /// # Errors
    ///
    /// Returns [`ExperimentError::InvalidPromotionThresholds`] for non-positive
    /// score thresholds or ratios outside the closed unit interval.
    pub fn validate(self) -> Result<Self, ExperimentError> {
        let ratios_in_bounds = self.holdout_max_drawdown_max >= Decimal::ZERO
            && self.holdout_max_drawdown_max <= Decimal::ONE
            && self.selection_positive_window_ratio_min >= Decimal::ZERO
            && self.selection_positive_window_ratio_min <= Decimal::ONE;
        let positive_thresholds = self.selection_median_sharpe_min > Decimal::ZERO
            && self.holdout_profit_factor_min > Decimal::ZERO;
        if !ratios_in_bounds || !positive_thresholds {
            return Err(ExperimentError::InvalidPromotionThresholds);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperimentPlan {
    split: EvaluationSplitConfig,
    split_spec: ExperimentSplitSpec,
    protocol: EvaluationProtocol,
    protocol_spec: EvaluationProtocolSpec,
    registered_configurations: Vec<RegisteredConfiguration>,
    promotion_thresholds: PromotionThresholds,
    bootstrap: BootstrapConfig,
    runner_version: String,
    dataset_provenance_fingerprint: Sha256Digest,
    plan_fingerprint: Sha256Digest,
}

impl ExperimentPlan {
    /// Freezes dataset provenance, split, protocol, registry, thresholds, and bootstrap budget.
    ///
    /// # Errors
    ///
    /// Returns a typed experiment or backtest error when any frozen input is
    /// invalid, the dataset is not Spot, or required baselines are absent.
    pub fn new(
        dataset: &SpotKlineDataset,
        split_spec: ExperimentSplitSpec,
        protocol_spec: EvaluationProtocolSpec,
        registered_configurations: Vec<RegisteredConfiguration>,
        promotion_thresholds: PromotionThresholds,
        bootstrap: BootstrapConfig,
        runner_version: impl Into<String>,
    ) -> Result<Self, ExperimentError> {
        if dataset.manifest().product != MarketType::Spot {
            return Err(ExperimentError::NonSpotDataset);
        }
        let runner_version = runner_version.into().trim().to_owned();
        if runner_version.is_empty() {
            return Err(ExperimentError::BlankRunnerVersion);
        }
        bootstrap.validate()?;
        promotion_thresholds.validate()?;
        validate_registry(&registered_configurations)?;

        let split = split_spec.build()?;
        let protocol = protocol_spec.build()?;
        let dataset_provenance_fingerprint =
            Sha256Digest::from_bytes(&dataset_provenance_bytes(dataset));
        let plan_fingerprint = Sha256Digest::from_bytes(
            canonical_plan_bytes(
                split_spec,
                protocol_spec,
                &registered_configurations,
                promotion_thresholds,
                bootstrap,
                &runner_version,
                &dataset_provenance_fingerprint,
            )
            .as_bytes(),
        );

        Ok(Self {
            split,
            split_spec,
            protocol,
            protocol_spec,
            registered_configurations,
            promotion_thresholds,
            bootstrap,
            runner_version,
            dataset_provenance_fingerprint,
            plan_fingerprint,
        })
    }

    pub const fn split(&self) -> &EvaluationSplitConfig {
        &self.split
    }

    pub const fn protocol(&self) -> &EvaluationProtocol {
        &self.protocol
    }

    pub fn registered_configurations(&self) -> &[RegisteredConfiguration] {
        &self.registered_configurations
    }

    pub const fn promotion_thresholds(&self) -> PromotionThresholds {
        self.promotion_thresholds
    }

    pub const fn bootstrap(&self) -> BootstrapConfig {
        self.bootstrap
    }

    pub fn runner_version(&self) -> &str {
        &self.runner_version
    }

    pub fn dataset_provenance_fingerprint(&self) -> &Sha256Digest {
        &self.dataset_provenance_fingerprint
    }

    pub fn plan_fingerprint(&self) -> &Sha256Digest {
        &self.plan_fingerprint
    }

    /// Evaluates every preregistered configuration on selection windows only.
    ///
    /// # Errors
    ///
    /// Returns a typed error for plan/provenance mismatches or any failed
    /// causal evaluation or deterministic aggregation.
    pub fn run_selection(
        self,
        selection: SelectionPhase<'_>,
    ) -> Result<SelectedExperiment<'_>, ExperimentError> {
        let expected = EvaluationPlan::new(selection.plan().final_holdout_range().end, self.split)?;
        if selection.plan() != &expected {
            return Err(ExperimentError::SelectionPlanMismatch);
        }

        let provenance_fingerprint = Sha256Digest::from_bytes(
            canonical_manifests_bytes(selection.window_sample(0)?.manifests()).as_bytes(),
        );
        if provenance_fingerprint != self.dataset_provenance_fingerprint {
            return Err(ExperimentError::DatasetFingerprintMismatch);
        }

        let mut window_ranges = Vec::new();
        let mut window_evaluations = BTreeMap::<String, Vec<SelectionWindowResult>>::new();
        for (window_index, _) in selection.plan().windows().iter().enumerate() {
            let sample = selection.window_sample(window_index)?;
            window_ranges.push(sample.range());
            for configuration in &self.registered_configurations {
                let evaluation = self.protocol.evaluate(&sample, configuration.strategy())?;
                window_evaluations
                    .entry(configuration.identifier().to_owned())
                    .or_default()
                    .push(SelectionWindowResult {
                        window_index,
                        range: sample.range(),
                        evaluation,
                        one_x_delta_vs_cash: Decimal::ZERO,
                        one_x_delta_vs_buy_and_hold: Decimal::ZERO,
                    });
            }
        }

        let cash_window_returns = one_x_net_returns(
            window_evaluations
                .get(CASH_BASELINE_ID)
                .ok_or(ExperimentError::MissingSelectionBaseline(CASH_BASELINE_ID))?,
        );
        let buy_and_hold_window_returns =
            one_x_net_returns(window_evaluations.get(BUY_AND_HOLD_BASELINE_ID).ok_or(
                ExperimentError::MissingSelectionBaseline(BUY_AND_HOLD_BASELINE_ID),
            )?);

        let mut configurations = Vec::with_capacity(self.registered_configurations.len());
        for configuration in &self.registered_configurations {
            let mut window_results = window_evaluations
                .remove(configuration.identifier())
                .ok_or_else(|| {
                    ExperimentError::MissingSelectionConfiguration(
                        configuration.identifier().to_owned(),
                    )
                })?;
            for (result, (cash_return, buy_hold_return)) in window_results.iter_mut().zip(
                cash_window_returns
                    .iter()
                    .copied()
                    .zip(buy_and_hold_window_returns.iter().copied()),
            ) {
                result.one_x_delta_vs_cash = result
                    .evaluation
                    .one_x
                    .metrics
                    .net_return
                    .checked_sub(cash_return)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
                result.one_x_delta_vs_buy_and_hold = result
                    .evaluation
                    .one_x
                    .metrics
                    .net_return
                    .checked_sub(buy_hold_return)
                    .ok_or(BacktestError::ArithmeticOverflow)?;
            }
            configurations.push(summarize_configuration(
                configuration,
                window_results,
                self.bootstrap,
            )?);
        }

        let family_selections = select_family_winners(&configurations);
        let sealed_identifiers =
            sealed_identifiers(&self.registered_configurations, &family_selections);

        for summary in &mut configurations {
            summary.selected_for_holdout = sealed_identifiers.contains(summary.identifier.as_str());
        }

        let frozen = selection.freeze(
            self.registered_configurations
                .iter()
                .filter(|configuration| sealed_identifiers.contains(configuration.identifier()))
                .cloned()
                .collect(),
            self.protocol,
        )?;

        Ok(SelectedExperiment {
            plan: self,
            selection: SelectionSummary {
                window_ranges,
                configurations,
                family_selections,
                sealed_identifiers: sealed_identifiers.into_iter().collect(),
            },
            frozen,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapInterval {
    pub lower: Decimal,
    pub upper: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSelectionMetrics {
    pub median_net_return: Decimal,
    pub worst_net_return: Decimal,
    pub median_sharpe: Option<Decimal>,
    pub sharpe_bootstrap_95: Option<BootstrapInterval>,
    pub median_sortino: Option<Decimal>,
    pub sortino_bootstrap_95: Option<BootstrapInterval>,
    pub positive_window_ratio: Decimal,
    pub worst_drawdown: Option<Decimal>,
    pub median_turnover: Decimal,
    pub median_trade_count: Decimal,
    pub median_exposure: Decimal,
    pub median_delta_vs_cash: Decimal,
    pub median_delta_vs_buy_and_hold: Decimal,
    pub median_two_x_net_return: Decimal,
    pub available_sharpe_observations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionWindowResult {
    pub window_index: usize,
    pub range: Range<usize>,
    pub evaluation: CostSensitivityEvaluation,
    pub one_x_delta_vs_cash: Decimal,
    pub one_x_delta_vs_buy_and_hold: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationSelectionSummary {
    pub identifier: String,
    pub family: String,
    pub window_results: Vec<SelectionWindowResult>,
    pub aggregates: AggregateSelectionMetrics,
    pub family_winner_eligible: bool,
    pub selected_for_holdout: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilySelection {
    pub family: String,
    pub winner_identifier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionSummary {
    pub window_ranges: Vec<Range<usize>>,
    pub configurations: Vec<ConfigurationSelectionSummary>,
    pub family_selections: Vec<FamilySelection>,
    pub sealed_identifiers: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SelectedExperiment<'a> {
    plan: ExperimentPlan,
    selection: SelectionSummary,
    frozen: FrozenSelection<'a>,
}

impl<'a> SelectedExperiment<'a> {
    pub fn plan(&self) -> &ExperimentPlan {
        &self.plan
    }

    pub fn selection(&self) -> &SelectionSummary {
        &self.selection
    }

    /// Persists the sealed selection summary before any holdout access is possible.
    ///
    /// The callback receives the exact frozen plan and complete selection
    /// summary. A failed callback consumes no holdout state and returns its
    /// typed error unchanged.
    ///
    /// # Errors
    ///
    /// Returns the callback's error when durable selection persistence fails.
    pub fn persist_selection_with(
        self,
        persist: impl FnOnce(&ExperimentPlan, &SelectionSummary) -> Result<(), ExperimentError>,
    ) -> Result<PersistedSelection<'a>, ExperimentError> {
        persist(&self.plan, &self.selection)?;
        Ok(PersistedSelection {
            plan: self.plan,
            selection: self.selection,
            frozen: self.frozen,
        })
    }
}

/// Selection state whose complete artifact persistence callback succeeded.
#[derive(Debug, PartialEq, Eq)]
pub struct PersistedSelection<'a> {
    plan: ExperimentPlan,
    selection: SelectionSummary,
    frozen: FrozenSelection<'a>,
}

impl<'a> PersistedSelection<'a> {
    /// Consumes persisted selection state and makes the sealed holdout cohort accessible.
    pub fn open_final_holdout(self) -> FinalHoldoutRunner<'a> {
        let pending = self
            .selection
            .sealed_identifiers
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        FinalHoldoutRunner {
            promotion_thresholds: self.plan.promotion_thresholds,
            plan_fingerprint: self.plan.plan_fingerprint,
            selection: self.selection,
            phase: self.frozen.open_final_holdout(),
            pending,
            outcomes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromisingCondition {
    pub name: &'static str,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromisingDecision {
    pub passed: bool,
    pub conditions: [PromisingCondition; 6],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalHoldoutOutcome {
    pub identifier: String,
    pub family: String,
    pub evaluation: CostSensitivityEvaluation,
    pub promising: Option<PromisingDecision>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FinalHoldoutRunner<'a> {
    promotion_thresholds: PromotionThresholds,
    plan_fingerprint: Sha256Digest,
    selection: SelectionSummary,
    phase: FinalHoldoutPhase<'a>,
    pending: BTreeSet<String>,
    outcomes: Vec<FinalHoldoutOutcome>,
}

impl FinalHoldoutRunner<'_> {
    pub fn pending_identifiers(&self) -> Vec<&str> {
        self.pending.iter().map(String::as_str).collect()
    }

    pub fn outcomes(&self) -> &[FinalHoldoutOutcome] {
        &self.outcomes
    }

    pub fn last_outcome(&self) -> Option<&FinalHoldoutOutcome> {
        self.outcomes.last()
    }

    /// Evaluates one still-pending identifier from the sealed holdout registry.
    ///
    /// # Errors
    ///
    /// Returns a typed error for unknown, duplicate, missing, or failed
    /// registered evaluations.
    pub fn evaluate_registered(mut self, identifier: &str) -> Result<Self, ExperimentError> {
        if !self
            .selection
            .sealed_identifiers
            .iter()
            .any(|sealed| sealed == identifier)
        {
            return Err(ExperimentError::UnknownHoldoutIdentifier(
                identifier.to_owned(),
            ));
        }
        if !self.pending.remove(identifier) {
            return Err(ExperimentError::DuplicateHoldoutEvaluation(
                identifier.to_owned(),
            ));
        }

        let selection_summary = self
            .selection
            .configurations
            .iter()
            .find(|summary| summary.identifier == identifier)
            .ok_or_else(|| ExperimentError::MissingSelectionConfiguration(identifier.to_owned()))?;
        let evaluation = self.phase.evaluate_registered(identifier)?;
        let promising = if is_baseline(identifier) {
            None
        } else {
            Some(promotion_decision(
                &selection_summary.aggregates,
                &evaluation,
                self.promotion_thresholds,
            ))
        };
        self.outcomes.push(FinalHoldoutOutcome {
            identifier: identifier.to_owned(),
            family: selection_summary.family.clone(),
            evaluation,
            promising,
        });
        Ok(self)
    }

    /// Completes the experiment after every sealed identifier ran exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`ExperimentError::IncompleteHoldout`] while any identifier is pending.
    pub fn finish(self) -> Result<CompletedExperiment, ExperimentError> {
        if !self.pending.is_empty() {
            return Err(ExperimentError::IncompleteHoldout {
                remaining: self.pending.len(),
            });
        }
        let any_promising = self
            .outcomes
            .iter()
            .filter_map(|outcome| outcome.promising.as_ref())
            .any(|decision| decision.passed);
        Ok(CompletedExperiment {
            plan_fingerprint: self.plan_fingerprint,
            selection: self.selection,
            outcomes: self.outcomes,
            any_promising,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedExperiment {
    pub plan_fingerprint: Sha256Digest,
    pub selection: SelectionSummary,
    pub outcomes: Vec<FinalHoldoutOutcome>,
    pub any_promising: bool,
}

fn validate_registry(
    registered_configurations: &[RegisteredConfiguration],
) -> Result<(), ExperimentError> {
    if registered_configurations.len() != REQUIRED_REGISTRY_LEN {
        return Err(ExperimentError::InvalidRegistryBudget {
            expected: REQUIRED_REGISTRY_LEN,
            actual: registered_configurations.len(),
        });
    }

    let mut identifiers = BTreeSet::new();
    let mut strategies = Vec::new();
    let mut family_counts = BTreeMap::<&str, usize>::new();
    let mut cash_found = false;
    let mut buy_and_hold_found = false;

    for configuration in registered_configurations {
        if !identifiers.insert(configuration.identifier()) {
            return Err(ExperimentError::DuplicateIdentifier);
        }
        if strategies.contains(&configuration.strategy()) {
            return Err(ExperimentError::DuplicateStrategy);
        }
        strategies.push(configuration.strategy());

        let counter = family_counts.entry(configuration.family()).or_default();
        *counter = counter
            .checked_add(1)
            .ok_or(BacktestError::ArithmeticOverflow)?;

        match (configuration.identifier(), configuration.strategy()) {
            (CASH_BASELINE_ID, SpotStrategyConfig::Cash) => cash_found = true,
            (BUY_AND_HOLD_BASELINE_ID, SpotStrategyConfig::BuyAndHold) => {
                buy_and_hold_found = true;
            }
            _ => {}
        }
    }

    if !cash_found {
        return Err(ExperimentError::MissingBaseline(CASH_BASELINE_ID));
    }
    if !buy_and_hold_found {
        return Err(ExperimentError::MissingBaseline(BUY_AND_HOLD_BASELINE_ID));
    }

    if REQUIRED_FAMILY_COUNTS
        .iter()
        .any(|(family, expected)| family_counts.get(family).copied() != Some(*expected))
        || family_counts.len() != REQUIRED_FAMILY_COUNTS.len()
    {
        return Err(ExperimentError::InvalidFamilyBudget);
    }
    if !registry_matches_preregistration(registered_configurations) {
        return Err(ExperimentError::RegistryDoesNotMatchPreregistration);
    }

    Ok(())
}

fn registry_matches_preregistration(registered_configurations: &[RegisteredConfiguration]) -> bool {
    registered_configurations
        .iter()
        .zip(expected_daily_preregistration())
        .all(|(actual, (identifier, strategy))| {
            actual.identifier() == identifier && actual.strategy() == strategy
        })
        || registered_configurations
            .iter()
            .zip(expected_hourly_preregistration())
            .all(|(actual, (identifier, strategy))| {
                actual.identifier() == identifier && actual.strategy() == strategy
            })
}

fn expected_daily_preregistration() -> Vec<(String, SpotStrategyConfig)> {
    let mut expected = vec![
        ("cash".to_owned(), SpotStrategyConfig::Cash),
        ("buy-and-hold".to_owned(), SpotStrategyConfig::BuyAndHold),
    ];
    for lookback_bars in [28, 56, 84, 112, 168] {
        expected.push((
            format!("tsm-lb{lookback_bars:03}-rb007"),
            SpotStrategyConfig::SlowTimeSeriesMomentum {
                lookback_bars,
                rebalance_every_bars: 7,
            },
        ));
    }
    for lookback_bars in [20, 60, 120] {
        expected.push((
            format!("donchian-lb{lookback_bars:03}"),
            SpotStrategyConfig::LongOnlyDonchian { lookback_bars },
        ));
    }
    for lookback_returns in [20, 60] {
        for (target_code, annual_target) in [("10", 10), ("15", 15), ("20", 20)] {
            for (band_code, rebalance_band) in [("00", Decimal::ZERO), ("20", Decimal::new(20, 2))]
            {
                expected.push((
                    format!("vol-lb{lookback_returns:03}-t{target_code}-b{band_code}-rb007"),
                    SpotStrategyConfig::CappedVolatilityTarget {
                        lookback_returns,
                        annual_target: Decimal::new(annual_target, 2),
                        rebalance_band,
                        rebalance_every_bars: 7,
                    },
                ));
            }
        }
    }
    expected
}

fn expected_hourly_preregistration() -> Vec<(String, SpotStrategyConfig)> {
    let mut expected = vec![
        ("cash".to_owned(), SpotStrategyConfig::Cash),
        ("buy-and-hold".to_owned(), SpotStrategyConfig::BuyAndHold),
    ];
    for lookback_bars in [672, 1_344, 2_016, 2_688, 4_032] {
        expected.push((
            format!("tsm-lb{lookback_bars:03}-rb168"),
            SpotStrategyConfig::SlowTimeSeriesMomentum {
                lookback_bars,
                rebalance_every_bars: 168,
            },
        ));
    }
    for lookback_bars in [480, 1_440, 2_880] {
        expected.push((
            format!("donchian-lb{lookback_bars:03}"),
            SpotStrategyConfig::LongOnlyDonchian { lookback_bars },
        ));
    }
    for lookback_returns in [480, 1_440] {
        for (target_code, annual_target) in [("10", 10), ("15", 15), ("20", 20)] {
            for (band_code, rebalance_band) in [("00", Decimal::ZERO), ("20", Decimal::new(20, 2))]
            {
                expected.push((
                    format!("vol-lb{lookback_returns:03}-t{target_code}-b{band_code}-rb168"),
                    SpotStrategyConfig::CappedVolatilityTargetExplicitAnnualization {
                        lookback_returns,
                        annual_target: Decimal::new(annual_target, 2),
                        rebalance_band,
                        rebalance_every_bars: 168,
                        periods_per_year: Decimal::from(8_760_u32),
                    },
                ));
            }
        }
    }
    expected
}

fn summarize_configuration(
    configuration: &RegisteredConfiguration,
    window_results: Vec<SelectionWindowResult>,
    bootstrap: BootstrapConfig,
) -> Result<ConfigurationSelectionSummary, ExperimentError> {
    let one_x_returns = one_x_net_returns(&window_results);
    let two_x_returns = two_x_net_returns(&window_results);
    let one_x_sharpes = one_x_sharpes(&window_results);
    let one_x_sortinos = one_x_sortinos(&window_results);
    let drawdowns = one_x_drawdowns(&window_results);
    let turnovers = one_x_turnovers(&window_results);
    let trade_counts = one_x_trade_counts(&window_results)?;
    let exposures = one_x_exposures(&window_results);
    let delta_vs_cash = delta_vs_cash(&window_results);
    let delta_vs_buy_and_hold = delta_vs_buy_and_hold(&window_results);

    let median_net_return = median_decimal(&one_x_returns)?;
    let median_two_x_net_return = median_decimal(&two_x_returns)?;
    let median_sharpe = median_optional_decimal(&one_x_sharpes)?;
    let median_sortino = median_optional_decimal(&one_x_sortinos)?;
    let aggregates = AggregateSelectionMetrics {
        median_net_return,
        worst_net_return: *one_x_returns
            .iter()
            .min()
            .ok_or(BacktestError::ArithmeticOverflow)?,
        median_sharpe,
        sharpe_bootstrap_95: bootstrap_interval(
            &one_x_sharpes,
            bootstrap,
            bootstrap_seed(configuration.identifier(), "sharpe", bootstrap.base_seed),
        )?,
        median_sortino,
        sortino_bootstrap_95: bootstrap_interval(
            &one_x_sortinos,
            bootstrap,
            bootstrap_seed(configuration.identifier(), "sortino", bootstrap.base_seed),
        )?,
        positive_window_ratio: ratio_of_positive_windows(&one_x_returns)?,
        worst_drawdown: worst_optional_decimal(&drawdowns),
        median_turnover: median_decimal(&turnovers)?,
        median_trade_count: median_decimal(&trade_counts)?,
        median_exposure: median_decimal(&exposures)?,
        median_delta_vs_cash: median_decimal(&delta_vs_cash)?,
        median_delta_vs_buy_and_hold: median_decimal(&delta_vs_buy_and_hold)?,
        median_two_x_net_return,
        available_sharpe_observations: one_x_sharpes.len(),
    };
    let family_winner_eligible = !is_baseline(configuration.identifier())
        && aggregates.available_sharpe_observations >= MIN_AVAILABLE_SHARPE_OBSERVATIONS
        && aggregates.median_net_return > Decimal::ZERO
        && aggregates.median_two_x_net_return > Decimal::ZERO
        && aggregates.median_sharpe.is_some();

    Ok(ConfigurationSelectionSummary {
        identifier: configuration.identifier().to_owned(),
        family: configuration.family().to_owned(),
        window_results,
        aggregates,
        family_winner_eligible,
        selected_for_holdout: false,
    })
}

fn select_family_winners(configurations: &[ConfigurationSelectionSummary]) -> Vec<FamilySelection> {
    let mut families = BTreeMap::<&str, Vec<&ConfigurationSelectionSummary>>::new();
    for configuration in configurations {
        if is_baseline(&configuration.identifier) {
            continue;
        }
        families
            .entry(&configuration.family)
            .or_default()
            .push(configuration);
    }

    families
        .into_iter()
        .map(|(family, candidates)| {
            let mut eligible = candidates
                .into_iter()
                .filter(|candidate| candidate.family_winner_eligible)
                .collect::<Vec<_>>();
            eligible.sort_by(|left, right| compare_family_winner(left, right));
            FamilySelection {
                family: family.to_owned(),
                winner_identifier: eligible.first().map(|winner| winner.identifier.clone()),
            }
        })
        .collect()
}

fn compare_family_winner(
    left: &ConfigurationSelectionSummary,
    right: &ConfigurationSelectionSummary,
) -> Ordering {
    right
        .aggregates
        .median_sharpe
        .cmp(&left.aggregates.median_sharpe)
        .then_with(|| {
            right
                .aggregates
                .positive_window_ratio
                .cmp(&left.aggregates.positive_window_ratio)
        })
        .then_with(|| {
            right
                .aggregates
                .median_delta_vs_buy_and_hold
                .cmp(&left.aggregates.median_delta_vs_buy_and_hold)
        })
        .then_with(|| {
            left.aggregates
                .worst_drawdown
                .cmp(&right.aggregates.worst_drawdown)
        })
        .then_with(|| {
            left.aggregates
                .median_turnover
                .cmp(&right.aggregates.median_turnover)
        })
        .then_with(|| left.identifier.cmp(&right.identifier))
}

fn sealed_identifiers(
    registered_configurations: &[RegisteredConfiguration],
    family_selections: &[FamilySelection],
) -> BTreeSet<String> {
    let mut sealed = BTreeSet::from([
        CASH_BASELINE_ID.to_owned(),
        BUY_AND_HOLD_BASELINE_ID.to_owned(),
    ]);
    for selection in family_selections {
        if let Some(identifier) = &selection.winner_identifier {
            sealed.insert(identifier.clone());
        }
    }

    registered_configurations
        .iter()
        .filter(|configuration| sealed.contains(configuration.identifier()))
        .map(|configuration| configuration.identifier().to_owned())
        .collect()
}

fn promotion_decision(
    selection: &AggregateSelectionMetrics,
    holdout: &CostSensitivityEvaluation,
    thresholds: PromotionThresholds,
) -> PromisingDecision {
    let conditions = [
        PromisingCondition {
            name: "holdout_one_x_net_return_positive",
            passed: holdout.one_x.metrics.net_return > Decimal::ZERO,
        },
        PromisingCondition {
            name: "selection_median_sharpe_threshold",
            passed: selection
                .median_sharpe
                .is_some_and(|value| value >= thresholds.selection_median_sharpe_min),
        },
        PromisingCondition {
            name: "holdout_profit_factor_threshold",
            passed: holdout
                .one_x
                .metrics
                .performance
                .profit_factor
                .is_some_and(|value| value >= thresholds.holdout_profit_factor_min),
        },
        PromisingCondition {
            name: "holdout_max_drawdown_threshold",
            passed: holdout
                .one_x
                .metrics
                .performance
                .max_drawdown
                .as_ref()
                .is_some_and(|drawdown| drawdown.ratio <= thresholds.holdout_max_drawdown_max),
        },
        PromisingCondition {
            name: "selection_positive_window_ratio_threshold",
            passed: selection.positive_window_ratio
                >= thresholds.selection_positive_window_ratio_min,
        },
        PromisingCondition {
            name: "holdout_two_x_net_return_positive",
            passed: holdout.two_x.metrics.net_return > Decimal::ZERO,
        },
    ];

    PromisingDecision {
        passed: conditions.iter().all(|condition| condition.passed),
        conditions,
    }
}

fn one_x_net_returns(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .map(|result| result.evaluation.one_x.metrics.net_return)
        .collect()
}

fn two_x_net_returns(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .map(|result| result.evaluation.two_x.metrics.net_return)
        .collect()
}

fn one_x_sharpes(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .filter_map(|result| result.evaluation.one_x.metrics.performance.sharpe_ratio)
        .collect()
}

fn one_x_sortinos(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .filter_map(|result| result.evaluation.one_x.metrics.performance.sortino_ratio)
        .collect()
}

fn one_x_drawdowns(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .filter_map(|result| {
            result
                .evaluation
                .one_x
                .metrics
                .performance
                .max_drawdown
                .as_ref()
                .map(|drawdown| drawdown.ratio)
        })
        .collect()
}

fn one_x_turnovers(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .map(|result| result.evaluation.one_x.metrics.turnover)
        .collect()
}

fn one_x_trade_counts(results: &[SelectionWindowResult]) -> Result<Vec<Decimal>, BacktestError> {
    results
        .iter()
        .map(|result| {
            Ok(Decimal::from(
                u64::try_from(result.evaluation.one_x.metrics.trade_count)
                    .map_err(|_| BacktestError::ArithmeticOverflow)?,
            ))
        })
        .collect()
}

fn one_x_exposures(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .map(|result| result.evaluation.one_x.metrics.average_exposure)
        .collect()
}

fn delta_vs_cash(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .map(|result| result.one_x_delta_vs_cash)
        .collect()
}

fn delta_vs_buy_and_hold(results: &[SelectionWindowResult]) -> Vec<Decimal> {
    results
        .iter()
        .map(|result| result.one_x_delta_vs_buy_and_hold)
        .collect()
}

fn median_optional_decimal(values: &[Decimal]) -> Result<Option<Decimal>, BacktestError> {
    if values.is_empty() {
        return Ok(None);
    }
    Ok(Some(median_decimal(values)?))
}

fn median_decimal(values: &[Decimal]) -> Result<Decimal, BacktestError> {
    let mut sorted = values.to_vec();
    sorted.sort();
    let midpoint = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        return Ok(sorted[midpoint]);
    }

    sorted[midpoint - 1]
        .checked_add(sorted[midpoint])
        .and_then(|sum| sum.checked_div(Decimal::TWO))
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn ratio_of_positive_windows(values: &[Decimal]) -> Result<Decimal, BacktestError> {
    let positives = values
        .iter()
        .filter(|value| **value > Decimal::ZERO)
        .count();
    Decimal::from(u64::try_from(positives).map_err(|_| BacktestError::ArithmeticOverflow)?)
        .checked_div(Decimal::from(
            u64::try_from(values.len()).map_err(|_| BacktestError::ArithmeticOverflow)?,
        ))
        .ok_or(BacktestError::ArithmeticOverflow)
}

fn worst_optional_decimal(values: &[Decimal]) -> Option<Decimal> {
    values.iter().copied().max()
}

fn bootstrap_interval(
    values: &[Decimal],
    bootstrap: BootstrapConfig,
    seed: u64,
) -> Result<Option<BootstrapInterval>, BacktestError> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut replicates = bootstrap_replicate_medians(values, bootstrap, seed)?;
    replicates.sort();

    let lower_index = bootstrap.replicates.saturating_sub(1) * 25 / 1000;
    let upper_index = bootstrap.replicates.saturating_sub(1) * 975 / 1000;
    Ok(Some(BootstrapInterval {
        lower: replicates[lower_index],
        upper: replicates[upper_index],
    }))
}

pub(crate) fn bootstrap_replicate_medians(
    values: &[Decimal],
    bootstrap: BootstrapConfig,
    seed: u64,
) -> Result<Vec<Decimal>, BacktestError> {
    let mut rng = SplitMix64::new(seed);
    let mut replicates = Vec::with_capacity(bootstrap.replicates);
    for _ in 0..bootstrap.replicates {
        let mut sample = Vec::with_capacity(values.len());
        for _ in 0..values.len() {
            sample.push(values[rng.next_index(values.len())]);
        }
        replicates.push(median_decimal(&sample)?);
    }
    Ok(replicates)
}

pub(crate) fn bootstrap_seed(identifier: &str, metric: &str, base_seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    hash ^= base_seed;
    hash = hash.wrapping_mul(0x0100_0000_01b3);
    for byte in identifier.bytes().chain([b':']).chain(metric.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

fn dataset_provenance_bytes(dataset: &SpotKlineDataset) -> Vec<u8> {
    canonical_manifests_bytes(dataset.manifests()).into_bytes()
}

fn canonical_plan_bytes(
    split_spec: ExperimentSplitSpec,
    protocol_spec: EvaluationProtocolSpec,
    registered_configurations: &[RegisteredConfiguration],
    promotion_thresholds: PromotionThresholds,
    bootstrap: BootstrapConfig,
    runner_version: &str,
    dataset_provenance_fingerprint: &Sha256Digest,
) -> String {
    let mut output = String::new();
    let _ = write!(
        output,
        "dataset_provenance_fingerprint={}\ntraining_len={}\ntest_len={}\nstep_len={}\nembargo_len={}\nfinal_holdout_len={}\ninitial_cash={}\nfee_bps={}\nhalf_spread_bps={}\nslippage_bps={}\nlatency_bps={}\nselection_median_sharpe_min={}\nholdout_profit_factor_min={}\nholdout_max_drawdown_max={}\nselection_positive_window_ratio_min={}\nbootstrap_replicates={}\nbootstrap_base_seed={:#018x}\nrunner_version={}\n",
        dataset_provenance_fingerprint.as_str(),
        split_spec.training_len,
        split_spec.test_len,
        split_spec.step_len,
        split_spec.embargo_len,
        split_spec.final_holdout_len,
        decimal_string(protocol_spec.initial_cash.as_decimal()),
        decimal_string(protocol_spec.one_x_costs.fee_bps),
        decimal_string(protocol_spec.one_x_costs.half_spread_bps),
        decimal_string(protocol_spec.one_x_costs.slippage_bps),
        decimal_string(protocol_spec.one_x_costs.latency_bps),
        decimal_string(promotion_thresholds.selection_median_sharpe_min),
        decimal_string(promotion_thresholds.holdout_profit_factor_min),
        decimal_string(promotion_thresholds.holdout_max_drawdown_max),
        decimal_string(promotion_thresholds.selection_positive_window_ratio_min),
        bootstrap.replicates,
        bootstrap.base_seed,
        runner_version,
    );
    let _ = writeln!(
        output,
        "family_winner_min_available_sharpe_observations={MIN_AVAILABLE_SHARPE_OBSERVATIONS}"
    );
    output.push_str(
        "family_winner_eligibility=median_one_x_return_positive_and_median_two_x_return_positive_and_median_sharpe_available\n\
family_winner_rank=median_sharpe_desc,positive_window_ratio_desc,median_delta_vs_buy_and_hold_desc,worst_drawdown_asc,median_turnover_asc,identifier_asc\n\
uncertainty=window_bootstrap_with_replacement,statistic_median,percentile_2.5_97.5\n\
holdout_registry=cash,buy_and_hold,at_most_one_preselected_winner_per_candidate_family\n\
promotion=holdout_one_x_return_positive_and_selection_median_sharpe_threshold_and_holdout_profit_factor_threshold_and_holdout_max_drawdown_threshold_and_selection_positive_window_ratio_threshold_and_holdout_two_x_return_positive\n",
    );
    for configuration in registered_configurations {
        let _ = writeln!(
            output,
            "configuration={}||family={}||strategy={}",
            configuration.identifier(),
            configuration.family(),
            canonical_strategy_spec(configuration.strategy()),
        );
    }
    output
}

fn canonical_manifest_bytes(manifest: &DatasetManifest) -> String {
    let mut output = String::new();
    let product = match manifest.product {
        MarketType::Spot => "spot",
        MarketType::Perpetual => "perpetual",
    };
    let timestamp_unit = match manifest.timestamp_unit {
        TimestampUnit::Milliseconds => "milliseconds",
        TimestampUnit::Microseconds => "microseconds",
    };
    let _ = write!(
        output,
        "source_url={}\nretrieved_at={}\nvenue={}\nproduct={}\nsymbol={}\ninterval_micros={}\ntimezone={}\ntimestamp_unit={}\narchive_sha256={}\ncontent_sha256={}\nparser_version={}\nexpected_first_open={}\nexpected_last_close={}\nexpected_bar_count={}\n",
        manifest.source_url,
        manifest
            .retrieved_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
        manifest.venue,
        product,
        manifest.symbol.as_str(),
        manifest.interval_micros,
        manifest.timezone,
        timestamp_unit,
        manifest.archive_sha256.as_str(),
        manifest.content_sha256.as_str(),
        manifest.parser_version,
        manifest
            .expected_first_open
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
        manifest
            .expected_last_close
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
        manifest.expected_bar_count,
    );
    output
}

fn canonical_manifests_bytes(manifests: &[DatasetManifest]) -> String {
    let mut output = String::new();
    for (index, manifest) in manifests.iter().enumerate() {
        let _ = writeln!(output, "manifest_index={index}");
        output.push_str(&canonical_manifest_bytes(manifest));
    }
    output
}

fn canonical_strategy_spec(strategy: SpotStrategyConfig) -> String {
    match strategy {
        SpotStrategyConfig::Cash => "cash".to_owned(),
        SpotStrategyConfig::BuyAndHold => "buy_and_hold".to_owned(),
        SpotStrategyConfig::SlowTimeSeriesMomentum {
            lookback_bars,
            rebalance_every_bars,
        } => format!("slow_time_series_momentum:{lookback_bars}:{rebalance_every_bars}"),
        SpotStrategyConfig::LongOnlyDonchian { lookback_bars } => {
            format!("long_only_donchian:{lookback_bars}")
        }
        SpotStrategyConfig::CappedVolatilityTarget {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
        } => format!(
            "capped_volatility_target:{}:{}:{}:{}",
            lookback_returns,
            decimal_string(annual_target),
            decimal_string(rebalance_band),
            rebalance_every_bars,
        ),
        SpotStrategyConfig::CappedVolatilityTargetExplicitAnnualization {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
            periods_per_year,
        } => format!(
            "capped_volatility_target:{}:{}:{}:{}:{}",
            lookback_returns,
            decimal_string(annual_target),
            decimal_string(rebalance_band),
            rebalance_every_bars,
            decimal_string(periods_per_year),
        ),
    }
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn is_baseline(identifier: &str) -> bool {
    matches!(identifier, CASH_BASELINE_ID | BUY_AND_HOLD_BASELINE_ID)
}

#[derive(Debug, Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn next_index(&mut self, upper: usize) -> usize {
        usize::try_from(
            self.next_u64() % u64::try_from(upper).expect("bootstrap upper bound fits u64"),
        )
        .expect("sampled bootstrap index fits usize")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BootstrapConfig, bootstrap_replicate_medians, bootstrap_seed, canonical_manifests_bytes,
        dataset_provenance_bytes,
    };
    use crate::{DatasetManifest, Sha256Digest, SpotBar, SpotKlineDataset, TimestampUnit};
    use chrono::{Duration, TimeZone, Utc};
    use crypto_trading_domain::{MarketType, Price, Symbol};
    use rust_decimal::Decimal;

    fn decimal(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    fn price(value: &str) -> Price {
        Price::new(decimal(value)).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        Sha256Digest::new(&character.to_string().repeat(64)).unwrap()
    }

    fn day_bar(day: i64, close: &str) -> SpotBar {
        let open_time = Utc.timestamp_opt(day * 86_400, 0).unwrap();
        SpotBar::new(
            open_time,
            open_time + Duration::days(1) - Duration::milliseconds(1),
            price(close),
            price(close),
            price(close),
            price(close),
            Decimal::ONE,
            decimal("100"),
            1,
        )
        .unwrap()
    }

    fn dataset_for_day(day: i64, close: &str, suffix: &str, digest_char: char) -> SpotKlineDataset {
        let bar = day_bar(day, close);
        let csv = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}\n",
            bar.open_time.timestamp_millis(),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.close_time.timestamp_millis(),
            bar.quote_volume,
            bar.trade_count,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
        );
        let sealed_at = bar.close_time + Duration::milliseconds(1);
        SpotKlineDataset::parse_csv(
            DatasetManifest {
                source_url: format!(
                    "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1d/{suffix}.zip"
                ),
                retrieved_at: sealed_at,
                venue: "binance".to_owned(),
                product: MarketType::Spot,
                symbol: Symbol::new("BTCUSDT").unwrap(),
                interval_micros: 86_400_000_000,
                timezone: "UTC".to_owned(),
                timestamp_unit: TimestampUnit::Milliseconds,
                archive_sha256: digest(digest_char),
                content_sha256: Sha256Digest::from_bytes(csv.as_bytes()),
                parser_version: "binance-spot-kline-v1".to_owned(),
                expected_first_open: bar.open_time,
                expected_last_close: bar.close_time,
                expected_bar_count: 1,
            },
            &csv,
            &digest(digest_char),
            sealed_at,
        )
        .unwrap()
    }

    #[test]
    fn dataset_provenance_bytes_cover_all_ordered_manifests() {
        let first = dataset_for_day(0, "100", "part-1", 'a');
        let second = dataset_for_day(1, "101", "part-2", 'b');
        let merged = SpotKlineDataset::merge_verified(vec![first.clone(), second]).unwrap();

        assert_ne!(
            dataset_provenance_bytes(&first),
            dataset_provenance_bytes(&merged)
        );
        assert_eq!(
            String::from_utf8(dataset_provenance_bytes(&merged)).unwrap(),
            canonical_manifests_bytes(merged.manifests())
        );
    }

    #[test]
    fn bootstrap_seed_and_replicates_change_with_base_seed() {
        let bootstrap = BootstrapConfig {
            replicates: 64,
            base_seed: 0x1111,
        };
        let values = vec![
            decimal("1"),
            decimal("2"),
            decimal("3"),
            decimal("5"),
            decimal("8"),
            decimal("13"),
            decimal("21"),
        ];

        let first_seed = bootstrap_seed("candidate", "sharpe", bootstrap.base_seed);
        let same_seed = bootstrap_seed("candidate", "sharpe", bootstrap.base_seed);
        let other_seed = bootstrap_seed("candidate", "sharpe", 0x2222);

        assert_eq!(first_seed, same_seed);
        assert_ne!(first_seed, other_seed);
        assert_eq!(
            bootstrap_replicate_medians(&values, bootstrap, first_seed).unwrap(),
            bootstrap_replicate_medians(&values, bootstrap, same_seed).unwrap()
        );
        assert_ne!(
            bootstrap_replicate_medians(&values, bootstrap, first_seed).unwrap(),
            bootstrap_replicate_medians(&values, bootstrap, other_seed).unwrap()
        );
    }
}
