//! Golden-vector contract for the history ("natural spread") decision mode.
//!
//! Expected values are derived line-by-line from the frozen Python reference
//! in `archive/python-legacy/core/services/arbitrage_monitor_v2`:
//! - median semantics: `statistics.median` at `history/history_calculator.py:415`
//! - minimum data points: `history/history_calculator.py:407`
//! - negative natural spread clamped to zero: `decision/arbitrage_decision.py:334`
//! - real arbitrage space and threshold: `decision/arbitrage_decision.py:337,345`
//! - funding gate against the annualized threshold: `decision/arbitrage_decision.py:395-401`
//! - missing funding data degrades instead of blocking: `decision/arbitrage_decision.py:403-410`
//! - annualization `diff x 1095 x 100`: `core/orchestrator.py:436`

use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_config::ArbitrageHistoryDecisionConfig;
use crypto_trading_strategy::{
    HistoryArbitrageConfig, HistoryDecisionKind, HistoryDecisionMachine, MAX_SPREAD_SAMPLES,
    NaturalSpreadCalculator, SpreadSample, StrategyError,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap()
}

fn sample(offset_seconds: i64, spread_bps: &str) -> SpreadSample {
    SpreadSample {
        timestamp: base_time() + Duration::seconds(offset_seconds),
        buy_exchange: "left".to_owned(),
        sell_exchange: "right".to_owned(),
        buy_price: decimal("100"),
        sell_price: decimal("101"),
        spread_bps: decimal(spread_bps),
        funding_rate_buy: None,
        funding_rate_sell: None,
    }
}

fn funded_sample(
    offset_seconds: i64,
    spread_bps: &str,
    funding_buy: &str,
    funding_sell: &str,
) -> SpreadSample {
    let mut sample = sample(offset_seconds, spread_bps);
    sample.funding_rate_buy = Some(decimal(funding_buy));
    sample.funding_rate_sell = Some(decimal(funding_sell));
    sample
}

fn machine(min_samples: usize, threshold_bps: &str) -> HistoryDecisionMachine {
    HistoryDecisionMachine::new(HistoryArbitrageConfig {
        window: Duration::hours(1),
        min_samples,
        deviation_threshold_bps: decimal(threshold_bps),
        funding_rate_annual_threshold_pct: decimal("10"),
    })
    .unwrap()
}

#[test]
fn median_matches_python_statistics_median_for_odd_and_even_counts() {
    // statistics.median([1, 3, 2]) == 2 (history_calculator.py:415).
    assert_eq!(
        NaturalSpreadCalculator::median(&[decimal("1"), decimal("3"), decimal("2")]).unwrap(),
        Some(decimal("2"))
    );
    // statistics.median([1, 2, 3, 4]) == 2.5.
    assert_eq!(
        NaturalSpreadCalculator::median(&[decimal("4"), decimal("1"), decimal("3"), decimal("2"),])
            .unwrap(),
        Some(decimal("2.5"))
    );
    // Sign-preserving: statistics.median([-3, -1, -2]) == -2.
    assert_eq!(
        NaturalSpreadCalculator::median(&[decimal("-3"), decimal("-1"), decimal("-2")]).unwrap(),
        Some(decimal("-2"))
    );
    assert_eq!(NaturalSpreadCalculator::median(&[]).unwrap(), None);
}

#[test]
fn insufficient_history_refuses_to_judge_and_never_opens() {
    // Fewer than min_data_points refuses to open
    // (arbitrage_decision.py:296-312, history_calculator.py:407).
    let mut machine = machine(3, "10");
    machine.observe(sample(0, "150")).unwrap();
    machine.observe(sample(60, "160")).unwrap();

    let decision = machine.evaluate(&sample(120, "400")).unwrap();
    assert_eq!(decision.kind, HistoryDecisionKind::InsufficientHistory);
    assert_eq!(decision.segment, 0);
    assert_eq!(decision.natural_spread_bps, None);
    assert_eq!(decision.real_arbitrage_space_bps, None);
    assert_eq!(decision.window_sample_count, 2);
    assert!(decision.funding_degraded);
}

#[test]
fn positive_natural_spread_is_deducted_before_the_threshold_check() {
    // Python vector: spread history [100, 110, 120] -> natural = 110
    // (statistics.median, history_calculator.py:415).
    // real_arbitrage_space = 130 - max(110, 0) = 20 (arbitrage_decision.py:334,337).
    // 20 >= threshold 10 -> opportunity (arbitrage_decision.py:345).
    let mut machine = machine(3, "10");
    machine.observe(sample(0, "100")).unwrap();
    machine.observe(sample(60, "110")).unwrap();
    machine.observe(sample(120, "120")).unwrap();

    let decision = machine.evaluate(&sample(180, "130")).unwrap();
    assert_eq!(decision.kind, HistoryDecisionKind::Open);
    assert_eq!(decision.natural_spread_bps, Some(decimal("110")));
    assert_eq!(decision.real_arbitrage_space_bps, Some(decimal("20")));
    assert_eq!(decision.segment, 2);
    assert_eq!(decision.buy_exchange, "left");
    assert_eq!(decision.sell_exchange, "right");
    assert!(decision.funding_degraded);

    // real space 5 < threshold 10 -> hold (arbitrage_decision.py:345).
    let hold = machine.evaluate(&sample(180, "115")).unwrap();
    assert_eq!(hold.kind, HistoryDecisionKind::Hold);
    assert_eq!(hold.real_arbitrage_space_bps, Some(decimal("5")));
    assert_eq!(hold.segment, 0);
}

#[test]
fn negative_natural_spread_is_clamped_to_zero() {
    // Python vector: history [-60, -50, -40] -> natural = -50, treated as 0
    // (arbitrage_decision.py:333-334), so real space equals the raw spread.
    let mut machine = machine(3, "10");
    machine.observe(sample(0, "-60")).unwrap();
    machine.observe(sample(60, "-50")).unwrap();
    machine.observe(sample(120, "-40")).unwrap();

    let decision = machine.evaluate(&sample(180, "30")).unwrap();
    assert_eq!(decision.kind, HistoryDecisionKind::Open);
    assert_eq!(decision.natural_spread_bps, Some(decimal("-50")));
    assert_eq!(decision.real_arbitrage_space_bps, Some(decimal("30")));
    assert_eq!(decision.segment, 3);
}

#[test]
fn missing_funding_data_degrades_the_funding_term_without_blocking() {
    // arbitrage_decision.py:403-410: no funding data still allows the spread
    // path; the port surfaces this as funding_degraded=true.
    let mut machine = machine(2, "10");
    machine.observe(sample(0, "100")).unwrap();
    machine.observe(sample(60, "100")).unwrap();

    let decision = machine.evaluate(&sample(120, "150")).unwrap();
    assert_eq!(decision.kind, HistoryDecisionKind::Open);
    assert!(decision.funding_degraded);
    assert_eq!(decision.funding_rate_diff_annual_pct, None);
    assert_eq!(decision.natural_funding_rate_diff, None);
}

#[test]
fn funding_annualization_and_threshold_follow_the_python_formula() {
    let mut machine = machine(2, "10");
    machine
        .observe(funded_sample(0, "100", "0.0001", "0.0002"))
        .unwrap();
    machine
        .observe(funded_sample(60, "100", "0.0001", "0.0003"))
        .unwrap();

    // Collecting funding (diff >= 0) always passes (arbitrage_decision.py:395).
    // diff = 0.0002 - 0.0001 = 0.0001; annual = 0.0001 x 1095 x 100 = 10.95%
    // (orchestrator.py:436, spread_pipeline.py:529).
    let favourable = machine
        .evaluate(&funded_sample(120, "150", "0.0001", "0.0002"))
        .unwrap();
    assert_eq!(favourable.kind, HistoryDecisionKind::Open);
    assert!(!favourable.funding_degraded);
    assert_eq!(
        favourable.funding_rate_diff_annual_pct,
        Some(decimal("10.950000"))
    );
    // Natural funding diff: median of |0.0001|, |0.0002| = 0.00015
    // (history_calculator.py:423-428).
    assert_eq!(
        favourable.natural_funding_rate_diff,
        Some(decimal("0.00015"))
    );

    // Paying funding with |annual| >= threshold blocks the opportunity.
    // diff = -0.0001 -> annual = -10.95%; |annual| >= 10 -> hold. Python
    // checks this bound (arbitrage_decision.py:398-401) and this port
    // enforces it fail-closed.
    let blocked = machine
        .evaluate(&funded_sample(120, "150", "0.0002", "0.0001"))
        .unwrap();
    assert_eq!(blocked.kind, HistoryDecisionKind::Hold);
    assert!(!blocked.funding_degraded);
    assert_eq!(
        blocked.funding_rate_diff_annual_pct,
        Some(decimal("-10.950000"))
    );

    // Paying funding under the annual threshold passes
    // (arbitrage_decision.py:399-401): diff = -0.00005 -> annual = -5.475%.
    let allowed = machine
        .evaluate(&funded_sample(120, "150", "0.00015", "0.0001"))
        .unwrap();
    assert_eq!(allowed.kind, HistoryDecisionKind::Open);
    assert_eq!(
        allowed.funding_rate_diff_annual_pct,
        Some(decimal("-5.475000"))
    );
}

#[test]
fn window_and_direction_filters_bound_the_judgement() {
    let mut machine = machine(2, "10");
    // Two samples that will fall outside the one-hour window of the
    // evaluated sample.
    machine.observe(sample(0, "100")).unwrap();
    machine.observe(sample(10, "100")).unwrap();
    // One in-window sample of the opposite direction.
    let mut reversed = sample(7_000, "100");
    reversed.buy_exchange = "right".to_owned();
    reversed.sell_exchange = "left".to_owned();
    machine.observe(reversed).unwrap();
    // One in-window sample of the evaluated direction.
    machine.observe(sample(7_010, "100")).unwrap();

    let decision = machine.evaluate(&sample(7_020, "150")).unwrap();
    assert_eq!(decision.kind, HistoryDecisionKind::InsufficientHistory);
    assert_eq!(decision.window_sample_count, 1);
}

#[test]
fn ring_capacity_evicts_the_oldest_samples() {
    let mut machine = HistoryDecisionMachine::new(HistoryArbitrageConfig {
        window: Duration::hours(24),
        min_samples: 1,
        deviation_threshold_bps: decimal("10"),
        funding_rate_annual_threshold_pct: decimal("10"),
    })
    .unwrap();
    let total = i64::try_from(MAX_SPREAD_SAMPLES).unwrap() + 8;
    for index in 0..total {
        machine.observe(sample(index, "100")).unwrap();
    }
    assert_eq!(machine.sample_count(), MAX_SPREAD_SAMPLES);
}

#[test]
fn window_eviction_drops_stale_samples_on_observe() {
    let mut machine = machine(1, "10");
    machine.observe(sample(0, "100")).unwrap();
    machine.observe(sample(30, "100")).unwrap();
    // One hour plus later: both earlier samples leave the retention window.
    machine.observe(sample(3_700, "100")).unwrap();
    assert_eq!(machine.sample_count(), 1);
}

#[test]
fn observe_rejects_regressed_timestamps_and_invalid_samples() {
    let mut machine = machine(1, "10");
    machine.observe(sample(60, "100")).unwrap();
    assert!(matches!(
        machine.observe(sample(0, "100")),
        Err(StrategyError::SnapshotMismatch(_))
    ));

    let mut duplicate = sample(120, "100");
    duplicate.sell_exchange = "left".to_owned();
    assert!(matches!(
        machine.observe(duplicate),
        Err(StrategyError::InvalidConfig(_))
    ));

    let mut broken_price = sample(120, "100");
    broken_price.buy_price = Decimal::ZERO;
    assert!(matches!(
        machine.observe(broken_price),
        Err(StrategyError::InvalidFinancialValue(_))
    ));
}

#[test]
fn machine_configuration_is_validated_fail_closed() {
    for (window, min_samples, threshold, funding) in [
        (Duration::zero(), 1usize, "10", "10"),
        (Duration::seconds(86_401), 1, "10", "10"),
        (Duration::hours(1), 0, "10", "10"),
        (Duration::hours(1), MAX_SPREAD_SAMPLES + 1, "10", "10"),
        (Duration::hours(1), 1, "0", "10"),
        (Duration::hours(1), 1, "10", "-1"),
    ] {
        assert!(
            HistoryDecisionMachine::new(HistoryArbitrageConfig {
                window,
                min_samples,
                deviation_threshold_bps: decimal(threshold),
                funding_rate_annual_threshold_pct: decimal(funding),
            })
            .is_err()
        );
    }
}

#[test]
fn config_adapter_requires_the_mode_to_be_enabled() {
    let disabled = ArbitrageHistoryDecisionConfig {
        enabled: false,
        window_seconds: 3_600,
        min_samples: 10,
        deviation_threshold_bps: decimal("10"),
        funding_rate_annual_threshold_pct: decimal("10"),
        spread_history_path: None,
    };
    assert!(HistoryDecisionMachine::try_from(&disabled).is_err());

    let enabled = ArbitrageHistoryDecisionConfig {
        enabled: true,
        ..disabled
    };
    let machine = HistoryDecisionMachine::try_from(&enabled).unwrap();
    assert_eq!(machine.config().min_samples, 10);
    assert_eq!(machine.config().window, Duration::seconds(3_600));
}
