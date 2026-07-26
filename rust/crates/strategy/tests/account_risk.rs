//! Contract tests for the pure account-level risk policy.

use std::collections::BTreeSet;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_config::load_account_risk_config_from_str;
use crypto_trading_domain::Money;
use crypto_trading_strategy::{
    AccountRiskDecision, AccountRiskInput, AccountRiskLimits, AccountRiskOpenPosition,
    AccountRiskPolicy, AccountRiskRejection, AccountRiskWarning,
};
use rust_decimal::Decimal;

fn money(value: i64) -> Money {
    Money::new(Decimal::from(value))
}

fn baseline_input() -> AccountRiskInput {
    AccountRiskInput {
        candidate_symbol: "BTC-USDC-PERP".to_owned(),
        candidate_notional: money(100),
        symbol_exposure: money(0),
        total_exposure: money(0),
        total_balance: money(10_000),
        daily_trade_count: 0,
        paused_reason: None,
        kill_switch_reason: None,
    }
}

fn policy(limits: AccountRiskLimits) -> AccountRiskPolicy {
    AccountRiskPolicy::new(limits).unwrap()
}

#[test]
fn default_policy_admits_without_warnings() {
    let policy = policy(AccountRiskLimits::default());
    assert_eq!(
        policy.evaluate(&baseline_input()),
        AccountRiskDecision::Admitted {
            warnings: Vec::new()
        }
    );
}

#[test]
fn kill_switch_dominates_every_other_check() {
    let policy = policy(AccountRiskLimits::default());
    let mut input = baseline_input();
    input.kill_switch_reason = Some("operator".to_owned());
    input.paused_reason = Some("network".to_owned());
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::KillSwitchEngaged {
            reason: "operator".to_owned()
        })
    );
}

#[test]
fn pause_rejects_new_admissions_with_the_durable_reason() {
    let policy = policy(AccountRiskLimits::default());
    let mut input = baseline_input();
    input.paused_reason = Some("exchange maintenance".to_owned());
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::Paused {
            reason: "exchange maintenance".to_owned()
        })
    );
}

#[test]
fn disabled_symbols_reject_case_insensitively() {
    let policy = policy(AccountRiskLimits {
        disabled_symbols: BTreeSet::from(["BTC-USDC-PERP".to_owned()]),
        ..AccountRiskLimits::default()
    });
    let mut input = baseline_input();
    input.candidate_symbol = "btc-usdc-perp".to_owned();
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::SymbolDisabled {
            symbol: "BTC-USDC-PERP".to_owned()
        })
    );
}

#[test]
fn balance_below_close_threshold_rejects_before_daily_and_exposure_checks() {
    let policy = policy(AccountRiskLimits {
        min_balance_close: Some(money(50)),
        max_daily_trades: Some(1),
        ..AccountRiskLimits::default()
    });
    let mut input = baseline_input();
    input.total_balance = money(49);
    input.daily_trade_count = 5;
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::BalanceBelowCloseThreshold {
            total_balance: money(49),
            limit: money(50),
        })
    );
}

#[test]
fn daily_trade_limit_rejects_at_the_cap() {
    let policy = policy(AccountRiskLimits {
        max_daily_trades: Some(3),
        ..AccountRiskLimits::default()
    });
    let mut input = baseline_input();
    input.daily_trade_count = 3;
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::DailyTradeLimitReached {
            count: 3,
            limit: 3
        })
    );
    input.daily_trade_count = 2;
    assert!(matches!(
        policy.evaluate(&input),
        AccountRiskDecision::Admitted { .. }
    ));
}

#[test]
fn symbol_exposure_cap_uses_projected_exposure_and_admits_at_the_boundary() {
    let policy = policy(AccountRiskLimits {
        max_symbol_exposure: Some(money(500)),
        ..AccountRiskLimits::default()
    });
    let mut input = baseline_input();
    input.symbol_exposure = money(400);
    input.candidate_notional = money(100);
    assert!(matches!(
        policy.evaluate(&input),
        AccountRiskDecision::Admitted { .. }
    ));
    input.candidate_notional = money(101);
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::SymbolExposureExceeded {
            symbol: "BTC-USDC-PERP".to_owned(),
            projected: money(501),
            limit: money(500),
        })
    );
}

#[test]
fn total_exposure_cap_rejects_across_symbols() {
    let policy = policy(AccountRiskLimits {
        max_total_exposure: Some(money(1_000)),
        ..AccountRiskLimits::default()
    });
    let mut input = baseline_input();
    input.total_exposure = money(950);
    input.candidate_notional = money(51);
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::TotalExposureExceeded {
            projected: money(1_001),
            limit: money(1_000),
        })
    );
}

#[test]
fn low_balance_and_high_risk_symbols_admit_with_warnings() {
    let policy = policy(AccountRiskLimits {
        min_balance_warning: Some(money(100)),
        min_balance_close: Some(money(50)),
        high_risk_symbols: BTreeSet::from(["BTC-USDC-PERP".to_owned()]),
        ..AccountRiskLimits::default()
    });
    let mut input = baseline_input();
    input.total_balance = money(75);
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Admitted {
            warnings: vec![
                AccountRiskWarning::LowBalance {
                    total_balance: money(75),
                    limit: money(100),
                },
                AccountRiskWarning::HighRiskSymbol {
                    symbol: "BTC-USDC-PERP".to_owned()
                },
            ]
        }
    );
}

#[test]
fn invalid_candidates_reject_deterministically() {
    let policy = policy(AccountRiskLimits::default());
    let mut input = baseline_input();
    input.candidate_notional = money(0);
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::InvalidCandidate)
    );
    let mut input = baseline_input();
    input.candidate_symbol = " padded".to_owned();
    assert_eq!(
        policy.evaluate(&input),
        AccountRiskDecision::Rejected(AccountRiskRejection::InvalidCandidate)
    );
}

#[test]
fn expired_positions_report_only_past_duration_holdings() {
    let policy = policy(AccountRiskLimits {
        max_position_duration: Some(Duration::seconds(3_600)),
        ..AccountRiskLimits::default()
    });
    let opened_at = Utc.with_ymd_and_hms(2026, 7, 26, 0, 0, 0).unwrap();
    let positions = vec![
        AccountRiskOpenPosition {
            task_id: "old".to_owned(),
            symbol: "BTC-USDC-PERP".to_owned(),
            opened_at,
        },
        AccountRiskOpenPosition {
            task_id: "fresh".to_owned(),
            symbol: "ETH-USDC-PERP".to_owned(),
            opened_at: opened_at + Duration::seconds(3_000),
        },
    ];
    let now = opened_at + Duration::seconds(3_601);
    let expired = policy.expired_positions(&positions, now).unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].task_id, "old");

    let unlimited = AccountRiskPolicy::new(AccountRiskLimits::default()).unwrap();
    assert!(
        unlimited
            .expired_positions(&positions, now)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn policy_validation_rejects_inverted_thresholds_and_unsafe_symbols() {
    assert!(
        AccountRiskPolicy::new(AccountRiskLimits {
            min_balance_warning: Some(money(10)),
            min_balance_close: Some(money(20)),
            ..AccountRiskLimits::default()
        })
        .is_err()
    );
    assert!(
        AccountRiskPolicy::new(AccountRiskLimits {
            max_daily_trades: Some(0),
            ..AccountRiskLimits::default()
        })
        .is_err()
    );
    assert!(
        AccountRiskPolicy::new(AccountRiskLimits {
            max_position_duration: Some(Duration::seconds(0)),
            ..AccountRiskLimits::default()
        })
        .is_err()
    );
    assert!(
        AccountRiskPolicy::new(AccountRiskLimits {
            disabled_symbols: BTreeSet::from(["bad\nsymbol".to_owned()]),
            ..AccountRiskLimits::default()
        })
        .is_err()
    );
}

#[test]
fn config_adapter_normalizes_symbols_and_maps_legacy_thresholds() {
    let config = load_account_risk_config_from_str(
        "min_balance_warning: 100\nmin_balance_close_position: 50\nmax_daily_trades: 20\nmax_position_duration_seconds: 3600\ndisabled_symbols:\n  - luna-usdc-perp\n",
    )
    .unwrap();
    let policy = AccountRiskPolicy::try_from(&config).unwrap();
    assert_eq!(policy.limits().min_balance_warning, Some(money(100)));
    assert_eq!(policy.limits().min_balance_close, Some(money(50)));
    assert_eq!(policy.limits().max_daily_trades, Some(20));
    assert_eq!(
        policy.limits().max_position_duration,
        Some(Duration::seconds(3_600))
    );
    assert!(policy.limits().disabled_symbols.contains("LUNA-USDC-PERP"));
    assert!(!policy.is_high_risk_symbol("LUNA-USDC-PERP"));
}
