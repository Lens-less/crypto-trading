use std::fmt::Write as _;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_backtest::{
    BootstrapConfig, CompletedExperiment, CostScheduleSpec, DatasetManifest,
    EvaluationProtocolSpec, ExperimentError, ExperimentPlan, ExperimentSplitSpec,
    PromotionThresholds, RegisteredConfiguration, SelectedExperiment, SelectionPhase, Sha256Digest,
    SpotBar, SpotKlineDataset, SpotStrategyConfig, TimestampUnit,
};
use crypto_trading_domain::{MarketType, Money, Price, Symbol};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn money(value: &str) -> Money {
    Money::new(decimal(value))
}

fn price(value: &(impl ToString + ?Sized)) -> Price {
    Price::new(decimal(&value.to_string())).unwrap()
}

fn digest(character: char) -> Sha256Digest {
    Sha256Digest::new(&character.to_string().repeat(64)).unwrap()
}

fn runner_version() -> &'static str {
    "g005-btcusdt-spot-20260812-v1"
}

fn protocol_spec() -> EvaluationProtocolSpec {
    EvaluationProtocolSpec {
        initial_cash: money("10000"),
        one_x_costs: CostScheduleSpec {
            fee_bps: decimal("10"),
            half_spread_bps: decimal("2"),
            slippage_bps: decimal("4"),
            latency_bps: decimal("4"),
        },
    }
}

fn zero_cost_protocol_spec() -> EvaluationProtocolSpec {
    EvaluationProtocolSpec {
        initial_cash: money("10000"),
        one_x_costs: CostScheduleSpec {
            fee_bps: Decimal::ZERO,
            half_spread_bps: Decimal::ZERO,
            slippage_bps: Decimal::ZERO,
            latency_bps: Decimal::ZERO,
        },
    }
}

fn split_spec() -> ExperimentSplitSpec {
    ExperimentSplitSpec {
        training_len: 1095,
        test_len: 182,
        step_len: 182,
        embargo_len: 1,
        final_holdout_len: 365,
    }
}

fn bootstrap_config() -> BootstrapConfig {
    BootstrapConfig {
        replicates: 10_000,
        base_seed: 0x4750_3035_2026_0812,
    }
}

fn promotion_thresholds() -> PromotionThresholds {
    PromotionThresholds {
        selection_median_sharpe_min: decimal("1.0"),
        holdout_profit_factor_min: decimal("1.2"),
        holdout_max_drawdown_max: decimal("0.20"),
        selection_positive_window_ratio_min: decimal("0.60"),
    }
}

fn registered_configurations() -> Vec<RegisteredConfiguration> {
    let mut registry = vec![
        RegisteredConfiguration::new("cash", SpotStrategyConfig::Cash).unwrap(),
        RegisteredConfiguration::new("buy-and-hold", SpotStrategyConfig::BuyAndHold).unwrap(),
    ];
    for lookback in [28, 56, 84, 112, 168] {
        registry.push(
            RegisteredConfiguration::new(
                format!("tsm-lb{lookback:03}-rb007"),
                SpotStrategyConfig::SlowTimeSeriesMomentum {
                    lookback_bars: lookback,
                    rebalance_every_bars: 7,
                },
            )
            .unwrap(),
        );
    }
    for lookback in [20, 60, 120] {
        registry.push(
            RegisteredConfiguration::new(
                format!("donchian-lb{lookback:03}"),
                SpotStrategyConfig::LongOnlyDonchian {
                    lookback_bars: lookback,
                },
            )
            .unwrap(),
        );
    }
    for lookback in [20, 60] {
        for annual_target in ["0.10", "0.15", "0.20"] {
            for rebalance_band in ["0.00", "0.20"] {
                let band_code = if rebalance_band == "0.00" { "00" } else { "20" };
                let target_code = &annual_target[2..];
                registry.push(
                    RegisteredConfiguration::new(
                        format!("vol-lb{lookback:03}-t{target_code}-b{band_code}-rb007"),
                        SpotStrategyConfig::CappedVolatilityTarget {
                            lookback_returns: lookback,
                            annual_target: decimal(annual_target),
                            rebalance_band: decimal(rebalance_band),
                            rebalance_every_bars: 7,
                        },
                    )
                    .unwrap(),
                );
            }
        }
    }
    registry
}

fn verified_dataset_with_source(
    bars: &[SpotBar],
    source_url: &str,
    archive_digest: char,
) -> SpotKlineDataset {
    let mut csv = String::new();
    for bar in bars {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
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
        )
        .unwrap();
    }
    let first = bars.first().unwrap();
    let last = bars.last().unwrap();
    let sealed_at = last.close_time + Duration::milliseconds(1);
    SpotKlineDataset::parse_csv(
        DatasetManifest {
            source_url: source_url.to_owned(),
            retrieved_at: sealed_at,
            venue: "binance".to_owned(),
            product: MarketType::Spot,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval_micros: 86_400_000_000,
            timezone: "UTC".to_owned(),
            timestamp_unit: TimestampUnit::Milliseconds,
            archive_sha256: digest(archive_digest),
            content_sha256: Sha256Digest::from_bytes(csv.as_bytes()),
            parser_version: "binance-spot-kline-v1".to_owned(),
            expected_first_open: first.open_time,
            expected_last_close: last.close_time,
            expected_bar_count: bars.len(),
        },
        &csv,
        &digest(archive_digest),
        sealed_at,
    )
    .unwrap()
}

fn verified_dataset(bars: &[SpotBar]) -> SpotKlineDataset {
    verified_dataset_with_source(
        bars,
        "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1d/synthetic.zip",
        'e',
    )
}

fn synthetic_dataset(bar_count: usize) -> SpotKlineDataset {
    let mut bars = Vec::with_capacity(bar_count);
    let mut previous_close = decimal("1000");
    for day in 0..bar_count {
        let open_time = Utc
            .timestamp_opt(i64::try_from(day).unwrap() * 86_400, 0)
            .unwrap();
        let increment = match day % 48 {
            0 => decimal("-1"),
            1..=16 => decimal("2"),
            17..=32 => decimal("1"),
            _ => decimal("3"),
        };
        let open = previous_close;
        let close = previous_close + increment;
        let high = open.max(close) + decimal("1");
        let low = open.min(close) - decimal("1");
        previous_close = close;
        bars.push(
            SpotBar::new(
                open_time,
                open_time + Duration::days(1) - Duration::milliseconds(1),
                price(&open),
                price(&high),
                price(&low),
                price(&close),
                Decimal::ONE,
                decimal("1000"),
                1,
            )
            .unwrap(),
        );
    }
    verified_dataset(&bars)
}

fn experiment_plan(dataset: &SpotKlineDataset) -> ExperimentPlan {
    ExperimentPlan::new(
        dataset,
        split_spec(),
        protocol_spec(),
        registered_configurations(),
        promotion_thresholds(),
        bootstrap_config(),
        runner_version(),
    )
    .unwrap()
}

fn selection_plan(dataset: &SpotKlineDataset) -> ExperimentPlan {
    ExperimentPlan::new(
        dataset,
        split_spec(),
        zero_cost_protocol_spec(),
        registered_configurations(),
        promotion_thresholds(),
        bootstrap_config(),
        runner_version(),
    )
    .unwrap()
}

fn selection_phase(dataset: &SpotKlineDataset) -> SelectionPhase<'_> {
    SelectionPhase::new(dataset, split_spec().build().unwrap()).unwrap()
}

fn complete_holdout(selected: SelectedExperiment<'_>, sealed: &[String]) -> CompletedExperiment {
    let mut runner = selected
        .persist_selection_with(|_, _| Ok(()))
        .unwrap()
        .open_final_holdout();
    for identifier in sealed {
        runner = runner.evaluate_registered(identifier).unwrap();
        let outcome = runner.last_outcome().unwrap();
        assert_eq!(outcome.identifier, *identifier);
        if identifier == "cash" || identifier == "buy-and-hold" {
            assert!(outcome.promising.is_none());
        } else {
            assert!(outcome.promising.is_some());
        }
    }
    runner.finish().unwrap()
}

#[test]
fn plan_fingerprint_is_stable_and_changes_when_frozen_inputs_change() {
    let dataset = synthetic_dataset(3134);
    let first = experiment_plan(&dataset);
    let second = experiment_plan(&dataset);
    let changed_runner = ExperimentPlan::new(
        &dataset,
        split_spec(),
        protocol_spec(),
        registered_configurations(),
        promotion_thresholds(),
        bootstrap_config(),
        "g005-btcusdt-spot-20260812-v2",
    )
    .unwrap();
    let changed_bootstrap = ExperimentPlan::new(
        &dataset,
        split_spec(),
        protocol_spec(),
        registered_configurations(),
        promotion_thresholds(),
        BootstrapConfig {
            replicates: bootstrap_config().replicates,
            base_seed: bootstrap_config().base_seed.wrapping_add(1),
        },
        runner_version(),
    )
    .unwrap();

    assert_eq!(first.plan_fingerprint(), second.plan_fingerprint());
    assert_eq!(
        first.dataset_provenance_fingerprint(),
        second.dataset_provenance_fingerprint()
    );
    assert_ne!(first.plan_fingerprint(), changed_runner.plan_fingerprint());
    assert_ne!(
        first.plan_fingerprint(),
        changed_bootstrap.plan_fingerprint()
    );
}

#[test]
fn plan_rejects_missing_preregistered_baselines() {
    let dataset = synthetic_dataset(3134);
    let mut registry = registered_configurations();
    registry[1] = RegisteredConfiguration::new("buy_hold", SpotStrategyConfig::BuyAndHold).unwrap();

    assert_eq!(
        ExperimentPlan::new(
            &dataset,
            split_spec(),
            protocol_spec(),
            registry,
            promotion_thresholds(),
            bootstrap_config(),
            runner_version(),
        ),
        Err(ExperimentError::MissingBaseline("buy-and-hold"))
    );
}

#[test]
fn plan_rejects_same_budget_with_unregistered_parameters_or_order() {
    let dataset = synthetic_dataset(3134);
    let mut changed_parameter = registered_configurations();
    changed_parameter[2] = RegisteredConfiguration::new(
        "tsm-lb029-rb007",
        SpotStrategyConfig::SlowTimeSeriesMomentum {
            lookback_bars: 29,
            rebalance_every_bars: 7,
        },
    )
    .unwrap();
    let mut reordered = registered_configurations();
    reordered.swap(2, 3);

    for registry in [changed_parameter, reordered] {
        assert!(
            ExperimentPlan::new(
                &dataset,
                split_spec(),
                protocol_spec(),
                registry,
                promotion_thresholds(),
                bootstrap_config(),
                runner_version(),
            )
            .is_err()
        );
    }
}

#[test]
fn dataset_provenance_fingerprint_changes_when_ordered_manifests_expand() {
    let first_open = Utc.timestamp_opt(0, 0).unwrap();
    let second_open = Utc.timestamp_opt(86_400, 0).unwrap();
    let first_dataset = verified_dataset_with_source(
        &[SpotBar::new(
            first_open,
            first_open + Duration::days(1) - Duration::milliseconds(1),
            price("100"),
            price("101"),
            price("99"),
            price("100"),
            Decimal::ONE,
            decimal("100"),
            1,
        )
        .unwrap()],
        "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1d/part-1.zip",
        'a',
    );
    let second_dataset = verified_dataset_with_source(
        &[SpotBar::new(
            second_open,
            second_open + Duration::days(1) - Duration::milliseconds(1),
            price("101"),
            price("102"),
            price("100"),
            price("101"),
            Decimal::ONE,
            decimal("100"),
            1,
        )
        .unwrap()],
        "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1d/part-2.zip",
        'b',
    );
    let merged =
        SpotKlineDataset::merge_verified(vec![first_dataset.clone(), second_dataset]).unwrap();

    assert_ne!(
        experiment_plan(&first_dataset).dataset_provenance_fingerprint(),
        experiment_plan(&merged).dataset_provenance_fingerprint()
    );
}

#[test]
fn selection_and_holdout_are_deterministic_and_holdout_is_single_use() {
    let dataset = synthetic_dataset(3134);
    let selection = selection_phase(&dataset);
    assert_eq!(selection.plan().windows().len(), 9);
    assert_eq!(selection.plan().final_holdout_range(), 2769..3134);

    let first = selection_plan(&dataset)
        .run_selection(selection_phase(&dataset))
        .unwrap();
    let second = selection_plan(&dataset)
        .run_selection(selection_phase(&dataset))
        .unwrap();

    assert_eq!(first.selection(), second.selection());
    assert_eq!(first.selection().family_selections.len(), 3);
    assert!(first.selection().sealed_identifiers.len() >= 2);
    assert!(first.selection().sealed_identifiers.len() <= 5);
    assert!(
        first
            .selection()
            .sealed_identifiers
            .contains(&"cash".to_owned())
    );
    assert!(
        first
            .selection()
            .sealed_identifiers
            .contains(&"buy-and-hold".to_owned())
    );
    assert!(
        first
            .selection()
            .configurations
            .iter()
            .all(|summary| summary.window_results.len() == 9)
    );
    assert!(
        first
            .selection()
            .configurations
            .iter()
            .any(|summary| summary.aggregates.sharpe_bootstrap_95.is_some())
    );

    let sealed = first.selection().sealed_identifiers.clone();
    let pending = sealed.iter().map(String::as_str).collect::<Vec<_>>();
    let incomplete_runner = selection_plan(&dataset)
        .run_selection(selection_phase(&dataset))
        .unwrap()
        .persist_selection_with(|_, _| Ok(()))
        .unwrap()
        .open_final_holdout();
    assert_eq!(incomplete_runner.pending_identifiers(), pending);
    assert_eq!(
        incomplete_runner.finish(),
        Err(ExperimentError::IncompleteHoldout {
            remaining: sealed.len(),
        })
    );

    let duplicate_error = second
        .persist_selection_with(|_, _| Ok(()))
        .unwrap()
        .open_final_holdout()
        .evaluate_registered(&sealed[0])
        .unwrap()
        .evaluate_registered(&sealed[0])
        .unwrap_err();
    assert_eq!(
        duplicate_error,
        ExperimentError::DuplicateHoldoutEvaluation(sealed[0].clone())
    );

    let completed = complete_holdout(first, &sealed);
    let replay_selected = selection_plan(&dataset)
        .run_selection(selection_phase(&dataset))
        .unwrap();
    let replay_completed = complete_holdout(replay_selected, &sealed);

    assert_eq!(completed, replay_completed);
    assert_eq!(completed.outcomes.len(), sealed.len());
    assert_eq!(
        completed.any_promising,
        completed
            .outcomes
            .iter()
            .filter_map(|outcome| outcome.promising.as_ref())
            .any(|decision| decision.passed)
    );
}

#[test]
fn failed_selection_persistence_does_not_unlock_the_holdout() {
    let dataset = synthetic_dataset(3134);
    let selected = selection_plan(&dataset)
        .run_selection(selection_phase(&dataset))
        .unwrap();

    assert_eq!(
        selected.persist_selection_with(|_, _| {
            Err(ExperimentError::SelectionArtifactPersistenceFailed(
                "simulated write failure".to_owned(),
            ))
        }),
        Err(ExperimentError::SelectionArtifactPersistenceFailed(
            "simulated write failure".to_owned(),
        ))
    );
}
