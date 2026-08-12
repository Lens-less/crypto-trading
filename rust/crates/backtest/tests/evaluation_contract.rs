use std::{collections::VecDeque, fmt::Write as _, ops::Range};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestError, BuyAndHoldStrategy, CausalSpotEvaluator, CostSchedule, DatasetManifest,
    EvaluationPlan, EvaluationProtocol, EvaluationSplitConfig, RegisteredConfiguration,
    SelectionPhase, Sha256Digest, SpotBar, SpotDecisionContext, SpotKlineDataset,
    SpotStrategyConfig, TargetExposureStrategy, TimestampUnit, VerifiedEvaluationSample,
};
use crypto_trading_domain::{MarketType, Money, Price, Quantity, Side, Symbol};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn money(value: &str) -> Money {
    Money::new(decimal(value))
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn bar(day: i64, open: &str, close: &str) -> SpotBar {
    let open_time = Utc.timestamp_opt(day * 86_400, 0).unwrap();
    SpotBar::new(
        open_time,
        open_time + Duration::days(1) - Duration::milliseconds(1),
        price(open),
        price(&decimal(open).max(decimal(close)).to_string()),
        price(&decimal(open).min(decimal(close)).to_string()),
        price(close),
        decimal("1"),
        decimal("100"),
        1,
    )
    .unwrap()
}

fn digest(character: char) -> Sha256Digest {
    Sha256Digest::new(&character.to_string().repeat(64)).unwrap()
}

fn verified_dataset(bars: &[SpotBar]) -> SpotKlineDataset {
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
            source_url: "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/test.zip"
                .to_owned(),
            retrieved_at: sealed_at,
            venue: "binance".to_owned(),
            product: MarketType::Spot,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            interval_micros: 86_400_000_000,
            timezone: "UTC".to_owned(),
            timestamp_unit: TimestampUnit::Milliseconds,
            archive_sha256: digest('a'),
            content_sha256: Sha256Digest::from_bytes(csv.as_bytes()),
            parser_version: "binance-spot-kline-v1".to_owned(),
            expected_first_open: first.open_time,
            expected_last_close: last.close_time,
            expected_bar_count: bars.len(),
        },
        &csv,
        &digest('a'),
        sealed_at,
    )
    .unwrap()
}

fn dataset_with_holdout(mut bars: Vec<SpotBar>) -> SpotKlineDataset {
    let last = bars.last().unwrap();
    let next_open = last.open_time + Duration::days(1);
    bars.push(
        SpotBar::new(
            next_open,
            next_open + Duration::days(1) - Duration::milliseconds(1),
            last.close,
            last.close,
            last.close,
            last.close,
            Decimal::ONE,
            decimal("100"),
            1,
        )
        .unwrap(),
    );
    verified_dataset(&bars)
}

fn sample(dataset: &SpotKlineDataset, range: Range<usize>) -> VerifiedEvaluationSample<'_> {
    SelectionPhase::new(dataset, EvaluationSplitConfig::new(1, 1, 1, 0, 1).unwrap())
        .unwrap()
        .sample(range)
        .unwrap()
}

#[derive(Debug)]
struct ScriptedTargets {
    targets: VecDeque<Decimal>,
}

impl TargetExposureStrategy for ScriptedTargets {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        assert_eq!(
            context.history.last().unwrap().close_time,
            context.decided_at
        );
        Ok(self.targets.pop_front().unwrap_or(context.current_target))
    }
}

#[test]
fn close_signal_executes_at_the_next_bar_open_and_never_at_the_same_close() {
    let bars = vec![
        bar(0, "90", "150"),
        bar(1, "120", "121"),
        bar(2, "122", "122"),
    ];
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut strategy = ScriptedTargets {
        targets: [decimal("0.12"), Decimal::ZERO, Decimal::ZERO].into(),
    };
    let dataset = dataset_with_holdout(bars.clone());
    let evaluation_sample = sample(&dataset, 0..bars.len());

    let result = evaluator.run(&evaluation_sample, &mut strategy).unwrap();

    assert_eq!(result.trades.len(), 2);
    assert_eq!(result.trades[0].trade.fill.occurred_at, bars[1].open_time);
    assert_eq!(result.trades[0].trade.fill.reference_price, price("120"));
    assert_ne!(result.trades[0].trade.fill.reference_price, bars[0].close);
    assert_eq!(result.trades[0].trade.fill.side, Side::Buy);
    assert_eq!(result.trades[1].trade.fill.occurred_at, bars[2].open_time);
    assert_eq!(result.trades[1].trade.fill.side, Side::Sell);
}

#[test]
fn unchanged_target_is_a_noop_until_terminal_liquidation() {
    let bars = vec![
        bar(0, "100", "100"),
        bar(1, "100", "110"),
        bar(2, "120", "120"),
        bar(3, "130", "130"),
    ];
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut strategy = ScriptedTargets {
        targets: [decimal("0.5")].into(),
    };
    let dataset = dataset_with_holdout(bars.clone());

    let result = evaluator
        .run(&sample(&dataset, 0..bars.len()), &mut strategy)
        .unwrap();

    assert_eq!(result.trades.len(), 2);
    assert_eq!(result.trades[0].trade.fill.side, Side::Buy);
    assert_eq!(result.trades[0].trade.fill.occurred_at, bars[1].open_time);
    assert_eq!(result.trades[1].trade.fill.side, Side::Sell);
    assert_eq!(result.trades[1].trade.fill.occurred_at, bars[3].close_time);
}

#[test]
fn embargoed_windows_stop_before_the_terminal_holdout() {
    let plan = EvaluationPlan::new(10, EvaluationSplitConfig::new(3, 2, 2, 1, 2).unwrap()).unwrap();

    assert_eq!(plan.final_holdout_range(), 8..10);
    assert_eq!(plan.windows().len(), 2);
    assert_eq!(plan.windows()[0].training_range, 0..3);
    assert_eq!(plan.windows()[0].embargo_range, 3..4);
    assert_eq!(plan.windows()[0].test_range, 4..6);
    assert_eq!(plan.windows()[1].training_range, 2..5);
    assert_eq!(plan.windows()[1].embargo_range, 5..6);
    assert_eq!(plan.windows()[1].test_range, 6..8);
}

#[test]
fn split_plan_fails_closed_when_embargo_and_holdout_leave_no_oos_window() {
    assert_eq!(
        EvaluationPlan::new(5, EvaluationSplitConfig::new(3, 2, 1, 1, 2).unwrap()),
        Err(BacktestError::InsufficientEvaluationData)
    );
}

#[test]
fn final_holdout_data_is_unavailable_until_the_configuration_set_is_frozen() {
    let bars = (0..10)
        .map(|day| bar(day, &(100 + day).to_string(), &(100 + day).to_string()))
        .collect::<Vec<_>>();
    let dataset = verified_dataset(&bars);
    let selection =
        SelectionPhase::new(&dataset, EvaluationSplitConfig::new(3, 2, 2, 1, 2).unwrap()).unwrap();

    assert_eq!(selection.selection_bars(), &bars[..8]);
    let frozen = selection
        .freeze(
            vec![
                RegisteredConfiguration::new("cash", SpotStrategyConfig::Cash).unwrap(),
                RegisteredConfiguration::new(
                    "lookback-12w",
                    SpotStrategyConfig::SlowTimeSeriesMomentum {
                        lookback_bars: 84,
                        rebalance_every_bars: 7,
                    },
                )
                .unwrap(),
            ],
            EvaluationProtocol::new(
                money("1000"),
                CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
                    .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(frozen.registered_configurations().len(), 2);
    let opened = frozen.open_final_holdout();
    assert_eq!(opened.registered_configurations().len(), 2);
    assert_eq!(
        opened
            .evaluate_registered("cash")
            .unwrap()
            .one_x
            .metrics
            .trade_count,
        0
    );
    assert_eq!(
        opened.evaluate_registered("post-hoc-config"),
        Err(BacktestError::UnregisteredHoldoutConfiguration)
    );
}

#[test]
fn search_budget_is_enforced_before_the_holdout_can_be_opened() {
    let bars = (0..10)
        .map(|day| bar(day, "100", "100"))
        .collect::<Vec<_>>();
    let dataset = verified_dataset(&bars);
    let selection =
        SelectionPhase::new(&dataset, EvaluationSplitConfig::new(3, 2, 2, 1, 2).unwrap()).unwrap();
    let too_many = (0..21)
        .map(|index| {
            RegisteredConfiguration::new(
                format!("config-{index}"),
                SpotStrategyConfig::LongOnlyDonchian {
                    lookback_bars: index + 1,
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(
        selection.freeze(
            too_many,
            EvaluationProtocol::new(
                money("1000"),
                CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
                    .unwrap(),
            )
            .unwrap(),
        ),
        Err(BacktestError::SearchBudgetExceeded)
    );
}

#[test]
fn duplicate_registered_configuration_cannot_open_the_holdout() {
    let bars = (0..10)
        .map(|day| bar(day, "100", "100"))
        .collect::<Vec<_>>();
    let dataset = verified_dataset(&bars);
    let selection =
        SelectionPhase::new(&dataset, EvaluationSplitConfig::new(3, 2, 2, 1, 2).unwrap()).unwrap();
    let duplicate = RegisteredConfiguration::new(
        "lookback-12w",
        SpotStrategyConfig::SlowTimeSeriesMomentum {
            lookback_bars: 84,
            rebalance_every_bars: 7,
        },
    )
    .unwrap();

    assert_eq!(
        selection.freeze(
            vec![duplicate.clone(), duplicate],
            EvaluationProtocol::new(
                money("1000"),
                CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
                    .unwrap(),
            )
            .unwrap(),
        ),
        Err(BacktestError::SearchBudgetExceeded)
    );
}

#[derive(Debug, Default)]
struct PriorCloseSignal;

impl TargetExposureStrategy for PriorCloseSignal {
    fn target_exposure(
        &mut self,
        context: &SpotDecisionContext<'_>,
    ) -> Result<Decimal, BacktestError> {
        Ok(if context.history.last().unwrap().close > price("100") {
            decimal("0.1")
        } else {
            Decimal::ZERO
        })
    }
}

#[test]
fn oos_window_can_use_prior_completed_history_and_fill_its_first_open() {
    let bars = vec![
        bar(0, "90", "90"),
        bar(1, "90", "110"),
        bar(2, "120", "121"),
        bar(3, "122", "123"),
    ];
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let dataset = dataset_with_holdout(bars.clone());
    let evaluation_sample = sample(&dataset, 2..4);

    let result = evaluator
        .run(&evaluation_sample, &mut PriorCloseSignal)
        .unwrap();

    assert_eq!(result.trades[0].trade.fill.occurred_at, bars[2].open_time);
    assert_eq!(result.trades[0].trade.fill.reference_price, bars[2].open);
}

#[test]
fn one_x_and_two_x_costs_are_componentwise_and_the_round_trip_ledger_is_exact() {
    let one_x =
        CostSchedule::new(decimal("10"), decimal("5"), decimal("10"), decimal("5")).unwrap();
    assert_eq!(
        one_x.doubled().unwrap(),
        CostSchedule::new(decimal("20"), decimal("10"), decimal("20"), decimal("10"),).unwrap()
    );

    let bars = vec![
        bar(0, "90", "100"),
        bar(1, "100", "110"),
        bar(2, "110", "110"),
    ];
    let evaluator = CausalSpotEvaluator::new(money("1000"), one_x).unwrap();
    let mut strategy = ScriptedTargets {
        targets: [decimal("0.1"), Decimal::ZERO, Decimal::ZERO].into(),
    };
    let dataset = dataset_with_holdout(bars.clone());
    let evaluation_sample = sample(&dataset, 0..3);
    let result = evaluator.run(&evaluation_sample, &mut strategy).unwrap();

    assert_eq!(result.trades.len(), 2);
    assert_eq!(
        result.trades[0].trade.fill.quantity,
        Quantity::new(Decimal::ONE).unwrap()
    );
    assert_eq!(result.trades[0].trade.fill.fill_price, price("100.2"));
    assert_eq!(result.trades[0].costs.fee, money("0.1002"));
    assert_eq!(result.trades[0].costs.half_spread, money("0.05"));
    assert_eq!(result.trades[0].costs.slippage, money("0.1"));
    assert_eq!(result.trades[0].costs.latency, money("0.05"));
    assert_eq!(result.trades[1].trade.fill.fill_price, price("109.78"));
    assert_eq!(result.metrics.total_costs.fee, money("0.20998"));
    assert_eq!(result.metrics.total_costs.half_spread, money("0.105"));
    assert_eq!(result.metrics.total_costs.slippage, money("0.21"));
    assert_eq!(result.metrics.total_costs.latency, money("0.105"));
    assert_eq!(result.metrics.total_costs.total, money("0.62998"));
    assert_eq!(result.metrics.ending_equity, money("1009.37002"));
    assert_eq!(result.metrics.net_return, decimal("0.00937002"));
    assert_eq!(result.metrics.turnover, decimal("0.21"));
    assert_eq!(result.metrics.trade_count, 2);
    assert_eq!(result.metrics.periods_per_year, Some(decimal("365")));
    assert!(result.metrics.annualized_volatility.is_some());
    assert_eq!(result.metrics.performance.profit_factor, None);
    assert!(result.metrics.average_exposure >= Decimal::ZERO);
    assert!(result.metrics.average_exposure <= Decimal::ONE);
}

#[test]
fn passive_baseline_is_affordable_at_one_x_and_pays_a_terminal_exit() {
    let bars = vec![
        bar(0, "100", "100"),
        bar(1, "100", "110"),
        bar(2, "110", "120"),
    ];
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(decimal("10"), decimal("5"), decimal("10"), decimal("5")).unwrap(),
    )
    .unwrap();
    let dataset = dataset_with_holdout(bars.clone());
    let evaluation_sample = sample(&dataset, 0..bars.len());

    let result = evaluator
        .run(&evaluation_sample, &mut BuyAndHoldStrategy::default())
        .unwrap();

    assert_eq!(result.trades.len(), 2);
    assert_eq!(result.trades[0].trade.fill.side, Side::Buy);
    assert_eq!(result.trades[0].trade.fill.occurred_at, bars[1].open_time);
    assert_eq!(result.trades[1].trade.fill.side, Side::Sell);
    assert_eq!(result.trades[1].trade.fill.occurred_at, bars[2].close_time);
    assert_eq!(result.trades[1].trade.position_qty, Decimal::ZERO);
    assert!(result.metrics.total_costs.total > result.trades[0].costs.total);
}

#[test]
fn identical_causal_inputs_produce_identical_evaluation_results() {
    let bars = vec![
        bar(0, "90", "100"),
        bar(1, "100", "110"),
        bar(2, "110", "110"),
    ];
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let strategy = || ScriptedTargets {
        targets: [decimal("0.1"), Decimal::ZERO, Decimal::ZERO].into(),
    };
    let dataset = dataset_with_holdout(bars.clone());
    let evaluation_sample = sample(&dataset, 0..bars.len());

    assert_eq!(
        evaluator.run(&evaluation_sample, &mut strategy()).unwrap(),
        evaluator.run(&evaluation_sample, &mut strategy()).unwrap()
    );
}

#[test]
fn invalid_exposure_fails_closed() {
    let bars = vec![bar(0, "100", "100"), bar(1, "100", "100")];
    let evaluator = CausalSpotEvaluator::new(
        money("1000"),
        CostSchedule::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut leveraged = ScriptedTargets {
        targets: [decimal("1.01")].into(),
    };
    let dataset = dataset_with_holdout(bars);
    let evaluation_sample = sample(&dataset, 0..2);

    assert_eq!(
        evaluator.run(&evaluation_sample, &mut leveraged),
        Err(BacktestError::InvalidTargetExposure)
    );
}
