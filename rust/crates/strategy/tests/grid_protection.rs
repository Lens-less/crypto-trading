//! Golden-vector and contract tests for the pure grid-protection policies.
//!
//! Every trigger condition and expected value below is extracted from the
//! frozen Python subsystem under
//! `archive/python-legacy/core/services/grid/` with the source file and line
//! numbers cited next to each scenario.

use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_domain::{Price, Side};
use crypto_trading_strategy::{
    CapitalProtectionPolicy, CapitalProtectionPolicyConfig, GridDirection, GridDirective,
    GridProtectionGeometry, GridProtectionMachine, GridProtectionObservation,
    GridProtectionPolicies, GridProtectionReason, PriceLockPolicy, PriceLockPolicyConfig,
    ScalpingPolicy, ScalpingPolicyConfig, StopLossPolicy, StopLossPolicyConfig, TakeProfitPolicy,
    TakeProfitPolicyConfig,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).expect("test price must be positive")
}

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap()
}

/// Long grid 100..200 with 100 levels of interval 1, the shape used by every
/// golden vector below.
fn long_geometry() -> GridProtectionGeometry {
    GridProtectionGeometry::new(GridDirection::Long, price("100"), price("200"), 100).unwrap()
}

fn short_geometry() -> GridProtectionGeometry {
    GridProtectionGeometry::new(GridDirection::Short, price("100"), price("200"), 100).unwrap()
}

fn observation(
    price_text: &str,
    seconds: i64,
    position: &str,
    collateral: &str,
    cycles_per_hour: &str,
) -> GridProtectionObservation {
    GridProtectionObservation {
        price: price(price_text),
        observed_at: base_time() + Duration::seconds(seconds),
        position_quantity: decimal(position),
        current_collateral: decimal(collateral),
        cycles_per_hour: decimal(cycles_per_hour),
    }
}

#[test]
fn geometry_rejects_inverted_ranges_and_unbounded_level_counts() {
    assert!(
        GridProtectionGeometry::new(GridDirection::Long, price("200"), price("100"), 10).is_err()
    );
    assert!(
        GridProtectionGeometry::new(GridDirection::Long, price("100"), price("200"), 0).is_err()
    );
    assert!(
        GridProtectionGeometry::new(GridDirection::Long, price("100"), price("200"), 10_001)
            .is_err()
    );
}

#[test]
fn policy_configs_reject_out_of_range_parameters() {
    assert!(ScalpingPolicyConfig::new(0, 2).is_err());
    assert!(ScalpingPolicyConfig::new(101, 2).is_err());
    assert!(ScalpingPolicyConfig::new(80, 0).is_err());
    assert!(CapitalProtectionPolicyConfig::new(0).is_err());
    assert!(CapitalProtectionPolicyConfig::new(101).is_err());
    assert!(TakeProfitPolicyConfig::new(Decimal::ZERO).is_err());
    assert!(TakeProfitPolicyConfig::new(decimal("11")).is_err());
    assert!(StopLossPolicyConfig::new(Decimal::ZERO, 300, decimal("50")).is_err());
    assert!(StopLossPolicyConfig::new(decimal("101"), 300, decimal("50")).is_err());
    assert!(StopLossPolicyConfig::new(decimal("100"), 0, decimal("50")).is_err());
    assert!(StopLossPolicyConfig::new(decimal("100"), 300, decimal("-1")).is_err());
}

// Golden vector: `grid_config.py:603-632` derives the trigger level as
// `grid_count - floor(grid_count * trigger_percent / 100)`, so an 80% trigger
// on 100 levels fires at level 20 and below; `scalping_manager.py:123-144`
// activates when the current level index is at or below that trigger.
#[test]
fn scalping_triggers_at_the_legacy_progress_level_and_not_above_it() {
    let geometry = long_geometry();
    let mut policy = ScalpingPolicy::new(ScalpingPolicyConfig::new(80, 2).unwrap());

    // Level index for $120.4 rounds to 21 (`grid_config.py:334-338`).
    let above = policy
        .evaluate(
            &geometry,
            &observation("120.4", 0, "0", "1000", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(above, GridDirective::Continue);
    assert!(!policy.is_active());

    // Level index for $119 is 20, exactly the trigger level.
    policy
        .evaluate(
            &geometry,
            &observation("119", 1, "0", "1000", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert!(policy.is_active());

    // Rebound above the trigger level exits scalping
    // (`scalping_manager.py:170-178`).
    let exited = policy
        .evaluate(
            &geometry,
            &observation("130", 2, "0", "1000", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(exited, GridDirective::Continue);
    assert!(!policy.is_active());
}

// Golden vector: `scalping_manager.py:263-314` computes
// breakeven = price + (initial - collateral) / |position| and the take-profit
// level as `min(grid_count, conservative(breakeven) + take_profit_grids)`.
// With price=119, pnl=-50, position=10: breakeven=124, level 25, take-profit
// level 27, price $126 via `grid_config.py:307-310`.
#[test]
fn scalping_long_derives_the_breakeven_plus_two_take_profit_order() {
    let geometry = long_geometry();
    let mut policy = ScalpingPolicy::new(ScalpingPolicyConfig::new(80, 2).unwrap());

    let directive = policy
        .evaluate(
            &geometry,
            &observation("119", 0, "10", "950", "0"),
            decimal("1000"),
        )
        .unwrap();

    assert_eq!(
        directive,
        GridDirective::Scalp {
            reason: GridProtectionReason::ScalpingActive,
            side: Side::Sell,
            quantity: decimal("10").try_into().unwrap(),
            take_profit_price: price("126"),
        }
    );
}

// Golden vector: the short leg computes breakeven = price - required move and
// the take-profit level `min(grid_count, conservative(breakeven) +
// take_profit_grids)` — in short index space a larger index is a lower price,
// so adding levels moves the buy-to-cover past breakeven into profit. This
// deliberately deviates from the buggy legacy `scalping_manager.py:316-340`,
// which subtracted the levels and exited on the loss side of breakeven. With
// price=181, pnl=-50, position=-10: breakeven=176, level 25, take-profit
// level 27, price $174 via `grid_config.py:311-314`.
#[test]
fn scalping_short_derives_the_breakeven_plus_two_take_profit_order() {
    let geometry = short_geometry();
    let mut policy = ScalpingPolicy::new(ScalpingPolicyConfig::new(80, 2).unwrap());

    let directive = policy
        .evaluate(
            &geometry,
            &observation("181", 0, "-10", "950", "0"),
            decimal("1000"),
        )
        .unwrap();

    assert_eq!(
        directive,
        GridDirective::Scalp {
            reason: GridProtectionReason::ScalpingActive,
            side: Side::Buy,
            quantity: decimal("10").try_into().unwrap(),
            take_profit_price: price("174"),
        }
    );
}

#[test]
fn scalping_without_position_or_principal_stays_passive_while_active() {
    let geometry = long_geometry();
    let mut policy = ScalpingPolicy::new(ScalpingPolicyConfig::new(80, 2).unwrap());

    // Active but no position (`scalping_manager.py:253-260`).
    let directive = policy
        .evaluate(
            &geometry,
            &observation("119", 0, "0", "950", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
    assert!(policy.is_active());

    // Active with a position but no recorded principal.
    let directive = policy
        .evaluate(
            &geometry,
            &observation("119", 1, "10", "950", "0"),
            Decimal::ZERO,
        )
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
}

// Golden vector: `grid_config.py:674-703` places a 50% capital-protection
// trigger on 100 levels at level 50; `capital_protection_manager.py:104-125`
// activates at or below it and `:136-174` resets once
// collateral - principal >= -$0.01.
#[test]
fn capital_protection_activates_then_resets_only_after_recovery() {
    let geometry = long_geometry();
    let mut policy = CapitalProtectionPolicy::new(CapitalProtectionPolicyConfig::new(50).unwrap());

    // Level 51 does not activate.
    let directive = policy
        .evaluate(
            &geometry,
            &observation("150", 0, "10", "900", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
    assert!(!policy.is_active());

    // Level 50 activates but the account is still under water.
    let directive = policy
        .evaluate(
            &geometry,
            &observation("149", 1, "10", "900", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
    assert!(policy.is_active());

    // Down $0.015 stays outside the $0.01 tolerance
    // (`capital_protection_manager.py:154-157`).
    let directive = policy
        .evaluate(
            &geometry,
            &observation("160", 2, "10", "999.985", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);

    // Down $0.01 exactly is recovered within tolerance.
    let directive = policy
        .evaluate(
            &geometry,
            &observation("160", 3, "10", "999.99", "0"),
            decimal("1000"),
        )
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::ResetGrid {
            reason: GridProtectionReason::CapitalRecovered,
        }
    );
    assert!(!policy.is_active());
}

// Golden vector: `take_profit_manager.py:101-119` fires when
// (collateral - principal) / principal reaches the configured rate.
#[test]
fn take_profit_fires_exactly_at_the_configured_equity_rate() {
    let mut policy = TakeProfitPolicy::new(TakeProfitPolicyConfig::new(decimal("0.01")).unwrap());

    let below = policy
        .evaluate(&observation("150", 0, "0", "1009.99", "0"), decimal("1000"))
        .unwrap();
    assert_eq!(below, GridDirective::Continue);

    let at = policy
        .evaluate(&observation("150", 1, "0", "1010", "0"), decimal("1000"))
        .unwrap();
    assert_eq!(
        at,
        GridDirective::ResetGrid {
            reason: GridProtectionReason::TakeProfitTarget,
        }
    );
}

// Golden vector: `price_lock_manager.py:59-97` locks only on a favourable
// escape that reaches the threshold, and `:110-142` unlocks once price
// returns inside the grid range.
#[test]
fn price_lock_freezes_on_favourable_escape_and_unlocks_inside_the_range() {
    let geometry = long_geometry();
    let mut policy = PriceLockPolicy::new(PriceLockPolicyConfig::new(price("210")));

    // Escaped above the grid but below the threshold: no lock.
    let directive = policy
        .evaluate(&geometry, &observation("205", 0, "0", "1000", "0"))
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
    assert!(!policy.is_locked());

    // Threshold reached: freeze entries without closing anything.
    let directive = policy
        .evaluate(&geometry, &observation("210", 1, "0", "1000", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::FreezeEntries {
            reason: GridProtectionReason::PriceLockActive,
        }
    );
    assert!(policy.is_locked());

    // Still outside the range: the lock holds.
    let directive = policy
        .evaluate(&geometry, &observation("205", 2, "0", "1000", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::FreezeEntries {
            reason: GridProtectionReason::PriceLockActive,
        }
    );

    // Back inside the range: unlock.
    let directive = policy
        .evaluate(&geometry, &observation("200", 3, "0", "1000", "0"))
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
    assert!(!policy.is_locked());
}

#[test]
fn price_lock_short_locks_below_the_grid_and_threshold() {
    let geometry = short_geometry();
    let mut policy = PriceLockPolicy::new(PriceLockPolicyConfig::new(price("90")));

    let directive = policy
        .evaluate(&geometry, &observation("95", 0, "0", "1000", "0"))
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);

    let directive = policy
        .evaluate(&geometry, &observation("90", 1, "0", "1000", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::FreezeEntries {
            reason: GridProtectionReason::PriceLockActive,
        }
    );
}

// Golden vector: `stop_loss_monitor.py:209-256` puts a 90% long trigger at
// upper - 0.9 * range = $110, `:166-184` requires the escape to persist for
// the timeout, and `:334-350` gates reset-vs-exit on the realtime APR.
#[test]
fn stop_loss_requires_sustained_adverse_escape_before_deciding() {
    let geometry = long_geometry();
    let mut policy =
        StopLossPolicy::new(StopLossPolicyConfig::new(decimal("90"), 300, decimal("50")).unwrap());

    // $111 sits above the $110 trigger: nothing starts.
    let directive = policy
        .evaluate(&geometry, &observation("111", 0, "0", "1000", "0"))
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);
    assert!(!policy.is_escaped());

    // Adverse escape starts the timer.
    policy
        .evaluate(&geometry, &observation("109", 10, "0", "1000", "0"))
        .unwrap();
    assert!(policy.is_escaped());

    // Recovery clears the timer (`stop_loss_monitor.py:157-165`).
    policy
        .evaluate(&geometry, &observation("111", 100, "0", "1000", "0"))
        .unwrap();
    assert!(!policy.is_escaped());

    // A fresh escape must persist the full timeout again.
    policy
        .evaluate(&geometry, &observation("109", 200, "0", "1000", "0"))
        .unwrap();
    let directive = policy
        .evaluate(&geometry, &observation("109", 499, "0", "1000", "0"))
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);

    // Timeout with zero realized cycles means APR 0 < threshold: exit
    // (`stop_loss_monitor.py:346-350`).
    let directive = policy
        .evaluate(&geometry, &observation("109", 500, "0", "1000", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::ExitAll {
            reason: GridProtectionReason::StopLossAprBelowThreshold,
        }
    );
}

#[test]
fn stop_loss_resets_the_grid_when_realtime_apr_meets_the_threshold() {
    let geometry = long_geometry();
    let mut policy =
        StopLossPolicy::new(StopLossPolicyConfig::new(decimal("90"), 300, decimal("50")).unwrap());

    policy
        .evaluate(&geometry, &observation("109", 0, "0", "1000", "0"))
        .unwrap();
    // One completed cycle per hour annualizes above the 50% threshold for this
    // geometry (interval 2/3%, width 200/3%), matching
    // `stop_loss_monitor.py:334-345`.
    let directive = policy
        .evaluate(&geometry, &observation("109", 300, "0", "1000", "1"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::ResetGrid {
            reason: GridProtectionReason::StopLossAprRecovered,
        }
    );
    assert!(!policy.is_escaped());
}

#[test]
fn machine_requires_at_least_one_policy() {
    assert!(
        GridProtectionMachine::new(long_geometry(), GridProtectionPolicies::default()).is_err()
    );
}

// Arbitration priority: stop-loss > capital protection > price lock > take
// profit > scalping (`grid_config.py:196`).
#[test]
fn machine_prefers_stop_loss_over_every_other_triggered_policy() {
    let mut machine = GridProtectionMachine::new(
        long_geometry(),
        GridProtectionPolicies {
            stop_loss: Some(StopLossPolicyConfig::new(decimal("90"), 300, decimal("50")).unwrap()),
            capital_protection: Some(CapitalProtectionPolicyConfig::new(50).unwrap()),
            price_lock: None,
            take_profit: None,
            scalping: Some(ScalpingPolicyConfig::new(80, 2).unwrap()),
        },
    )
    .unwrap();

    // Baseline capital records on the first neutral observation.
    let directive = machine
        .observe(&observation("150", 0, "0", "1000", "0"))
        .unwrap();
    assert_eq!(directive, GridDirective::Continue);

    // Deep drop: stop-loss timer starts, capital protection activates without
    // recovery, scalping is active with a position, so scalping wins for now.
    let directive = machine
        .observe(&observation("109", 10, "10", "900", "0"))
        .unwrap();
    assert_eq!(directive.label(), "scalp");

    // Once the stop-loss timeout elapses, its exit outranks the scalp.
    let directive = machine
        .observe(&observation("109", 310, "10", "900", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::ExitAll {
            reason: GridProtectionReason::StopLossAprBelowThreshold,
        }
    );
}

#[test]
fn machine_prefers_capital_protection_over_scalping_and_rebases_on_reset() {
    let mut machine = GridProtectionMachine::new(
        long_geometry(),
        GridProtectionPolicies {
            stop_loss: None,
            capital_protection: Some(CapitalProtectionPolicyConfig::new(50).unwrap()),
            price_lock: None,
            take_profit: None,
            scalping: Some(ScalpingPolicyConfig::new(80, 2).unwrap()),
        },
    )
    .unwrap();

    machine
        .observe(&observation("150", 0, "0", "1000", "0"))
        .unwrap();

    // Both capital protection (recovered immediately) and scalping trigger at
    // $119; capital protection outranks scalping.
    let directive = machine
        .observe(&observation("119", 10, "10", "1000", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::ResetGrid {
            reason: GridProtectionReason::CapitalRecovered,
        }
    );

    // The reset re-based the principal to $1000 and cleared policy state, so
    // an under-water account no longer counts as recovered.
    let directive = machine
        .observe(&observation("119", 20, "10", "900", "0"))
        .unwrap();
    assert_eq!(directive.label(), "scalp");
}

#[test]
fn machine_prefers_price_lock_over_take_profit() {
    let mut machine = GridProtectionMachine::new(
        long_geometry(),
        GridProtectionPolicies {
            stop_loss: None,
            capital_protection: None,
            price_lock: Some(PriceLockPolicyConfig::new(price("210"))),
            take_profit: Some(TakeProfitPolicyConfig::new(decimal("0.01")).unwrap()),
            scalping: None,
        },
    )
    .unwrap();

    machine
        .observe(&observation("150", 0, "0", "1000", "0"))
        .unwrap();

    // Price escaped to the lock threshold while equity is also above the
    // take-profit rate: the lock wins and nothing is closed.
    let directive = machine
        .observe(&observation("210", 10, "0", "1020", "0"))
        .unwrap();
    assert_eq!(
        directive,
        GridDirective::FreezeEntries {
            reason: GridProtectionReason::PriceLockActive,
        }
    );
}
