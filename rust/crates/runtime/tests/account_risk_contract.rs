//! Contract tests for the durable paper account-level risk authority.

use std::collections::BTreeSet;
use std::str::FromStr;

use chrono::{DateTime, Duration, TimeZone, Utc};
use crypto_trading_domain::{
    MarketType, Money, Order, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    AccountRiskAdmission, AccountRiskAdmissionTicket, AccountRiskAuthority, AccountRiskCandidate,
    AccountRiskDirective, JsonlHistory, PaperAccountAuthority, PaperAccountConfig, PaperCostModel,
    PaperReservationLeg, PaperReservationRequest,
};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy, AccountRiskRejection};
use rust_decimal::Decimal;
use uuid::Uuid;

fn money(value: &str) -> Money {
    Money::new(Decimal::from_str(value).unwrap())
}

fn temp_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "crypto-trading-account-risk-{label}-{}.jsonl",
        Uuid::new_v4()
    ))
}

fn at(hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 26, hour, minute, 0).unwrap()
}

fn authority(
    journal_id: Uuid,
    path: &std::path::Path,
    limits: AccountRiskLimits,
) -> AccountRiskAuthority {
    AccountRiskAuthority::new(
        journal_id,
        JsonlHistory::new(path),
        "paper",
        AccountRiskPolicy::new(limits).unwrap(),
    )
    .unwrap()
}

fn candidate(task_id: &str, symbol: &str, notional: &str) -> AccountRiskCandidate {
    AccountRiskCandidate::new(task_id, symbol, money(notional)).unwrap()
}

fn admitted_ticket(admission: AccountRiskAdmission) -> AccountRiskAdmissionTicket {
    match admission {
        AccountRiskAdmission::Admitted { ticket, .. } => ticket,
        AccountRiskAdmission::Rejected(rejection) => {
            panic!("expected admitted ticket, got rejection: {rejection:?}")
        }
    }
}

async fn reserve_paper_exposure(journal_id: Uuid, path: &std::path::Path, account_id: &str) {
    let account = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(path),
        PaperAccountConfig::new(account_id, money("1000")).unwrap(),
    )
    .unwrap();
    let left = OrderIntent::market(
        "paper-left",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let right = OrderIntent::market(
        "paper-right",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Sell,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let request = PaperReservationRequest::new(
        Uuid::new_v4(),
        format!("{account_id}/op/000001"),
        "risk-fixture:000001",
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![
            PaperReservationLeg::from_intent(0, &left, money("100")).unwrap(),
            PaperReservationLeg::from_intent(1, &right, money("100")).unwrap(),
        ],
    )
    .unwrap();
    account.reserve(request).await.unwrap();
}

/// Reserves one buy leg under the documented per-operation identity
/// `<owner>/op/<sequence>` so the admitted notional of `owner_task_id` is
/// settled the way real owners settle it.
async fn reserve_owner_leg(
    journal_id: Uuid,
    path: &std::path::Path,
    account_id: &str,
    owner_task_id: &str,
    notional: &str,
) {
    let account = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(path),
        PaperAccountConfig::new(account_id, money("1000")).unwrap(),
    )
    .unwrap();
    let intent = OrderIntent::market(
        "paper-left",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let request = PaperReservationRequest::new(
        Uuid::new_v4(),
        format!("{owner_task_id}/op/000001"),
        format!("risk-gap:{owner_task_id}"),
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &intent, money(notional)).unwrap()],
    )
    .unwrap();
    account.reserve(request).await.unwrap();
}

#[tokio::test]
async fn daily_trade_cap_counts_admissions_and_resets_at_utc_midnight() {
    let path = temp_path("daily-cap");
    let journal_id = Uuid::new_v4();
    let risk = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            max_daily_trades: Some(2),
            ..AccountRiskLimits::default()
        },
    );

    for _ in 0..2 {
        assert!(matches!(
            risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(10, 0))
                .await
                .unwrap(),
            AccountRiskAdmission::Admitted { .. }
        ));
    }
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(11, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::DailyTradeLimitReached {
            count: 2,
            limit: 2
        })
    ));

    // A restarted authority reconstructs the same durable counts.
    let restarted = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            max_daily_trades: Some(2),
            ..AccountRiskLimits::default()
        },
    );
    let state = restarted.state().await.unwrap();
    assert_eq!(state.daily_trade_count_at(at(12, 0)), 2);
    assert_eq!(state.rejected_count, 1);
    assert_eq!(
        state.last_rejection.as_deref(),
        Some("daily_trade_limit_reached")
    );

    // The next UTC day starts a fresh count without any explicit reset fact.
    let next_day = at(12, 0) + Duration::days(1);
    assert!(matches!(
        restarted
            .admit(&candidate("owner", "BTC-USDC-PERP", "10"), next_day)
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    let state = restarted.state().await.unwrap();
    assert_eq!(state.daily_trade_count_at(next_day), 1);
}

#[tokio::test]
async fn daily_trade_cap_ignores_backwards_date_changes() {
    let path = temp_path("daily-cap-regress");
    let journal_id = Uuid::new_v4();
    let limits = || AccountRiskLimits {
        max_daily_trades: Some(2),
        ..AccountRiskLimits::default()
    };
    let risk = authority(journal_id, &path, limits());

    let day_one = at(10, 0);
    let day_two = day_one + Duration::days(1);
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), day_one)
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), day_two)
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));

    // Owners feed replay-driven clocks that can regress across UTC midnight;
    // a backdated admission must count against the latched later day instead
    // of resetting the cap on every date flip.
    assert!(matches!(
        risk.admit(
            &candidate("owner", "BTC-USDC-PERP", "10"),
            day_one + Duration::hours(1)
        )
        .await
        .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(matches!(
        risk.admit(
            &candidate("owner", "BTC-USDC-PERP", "10"),
            day_one + Duration::hours(2)
        )
        .await
        .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::DailyTradeLimitReached {
            count: 2,
            limit: 2
        })
    ));
    assert!(matches!(
        risk.admit(
            &candidate("owner", "BTC-USDC-PERP", "10"),
            day_two + Duration::hours(1)
        )
        .await
        .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::DailyTradeLimitReached {
            count: 2,
            limit: 2
        })
    ));

    let state = risk.state().await.unwrap();
    // Regressed instants report the latched count (fail closed), a future day
    // reports zero until its first admission rolls the date forward.
    assert_eq!(state.daily_trade_count_at(day_one), 2);
    assert_eq!(state.daily_trade_count_at(day_two), 2);
    assert_eq!(state.daily_trade_count_at(day_two + Duration::days(1)), 0);
}

#[tokio::test]
async fn admitted_but_unreserved_notional_counts_toward_exposure_caps() {
    let path = temp_path("admit-reserve-gap");
    let journal_id = Uuid::new_v4();
    let limits = || AccountRiskLimits {
        max_symbol_exposure: Some(money("150")),
        max_total_exposure: Some(money("150")),
        ..AccountRiskLimits::default()
    };
    let risk = authority(journal_id, &path, limits());

    // Owner A is admitted but has not reserved yet: its in-flight notional
    // must already occupy the caps for concurrent owners.
    assert!(matches!(
        risk.admit(&candidate("owner-a", "BTC-USDT", "100"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(matches!(
        risk.admit(&candidate("owner-b", "BTC-USDT", "60"), at(9, 1))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::SymbolExposureExceeded { .. })
    ));

    // Once owner A's reservation lands, the admitted notional moves into the
    // reservation projection and is not double counted.
    reserve_owner_leg(journal_id, &path, "paper-main", "owner-a", "100").await;
    assert!(matches!(
        risk.admit(&candidate("owner-b", "BTC-USDT", "40"), at(9, 2))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));

    // Closing an owner's position clock clears its leftover admitted
    // notional, so a stalled owner cannot occupy the caps forever.
    risk.record_position_closed("owner-b", at(9, 3))
        .await
        .unwrap();
    assert!(matches!(
        risk.admit(&candidate("owner-d", "BTC-USDT", "40"), at(9, 4))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
}

#[tokio::test]
async fn cancelling_pending_admission_frees_capacity_and_clears_open_clock() {
    let path = temp_path("admission-cancel");
    let journal_id = Uuid::new_v4();
    let limits = || AccountRiskLimits {
        max_symbol_exposure: Some(money("100")),
        max_total_exposure: Some(money("100")),
        ..AccountRiskLimits::default()
    };
    let risk = authority(journal_id, &path, limits());

    let ticket = admitted_ticket(
        risk.admit(&candidate("owner-a", "BTC-USDT", "100"), at(9, 0))
            .await
            .unwrap(),
    );
    assert!(matches!(
        risk.admit(&candidate("owner-b", "BTC-USDT", "1"), at(9, 1))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::SymbolExposureExceeded { .. })
    ));

    assert!(
        risk.cancel_admission("owner-a", &ticket, at(9, 2))
            .await
            .unwrap()
    );
    let state = risk.state().await.unwrap();
    assert!(state.open_positions.is_empty());

    assert!(matches!(
        risk.admit(&candidate("owner-b", "BTC-USDT", "100"), at(9, 3))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
}

#[tokio::test]
async fn wrong_ticket_cannot_cancel_another_pending_admission() {
    let path = temp_path("admission-wrong-ticket");
    let journal_id = Uuid::new_v4();
    let limits = || AccountRiskLimits {
        max_symbol_exposure: Some(money("80")),
        max_total_exposure: Some(money("80")),
        ..AccountRiskLimits::default()
    };
    let risk = authority(journal_id, &path, limits());

    let first_owner_ticket = admitted_ticket(
        risk.admit(&candidate("owner-a", "BTC-USDT", "40"), at(9, 0))
            .await
            .unwrap(),
    );
    let second_owner_ticket = admitted_ticket(
        risk.admit(&candidate("owner-b", "BTC-USDT", "40"), at(9, 1))
            .await
            .unwrap(),
    );

    assert!(
        !risk
            .cancel_admission("owner-a", &second_owner_ticket, at(9, 2))
            .await
            .unwrap()
    );
    assert!(matches!(
        risk.admit(&candidate("owner-c", "BTC-USDT", "1"), at(9, 3))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::SymbolExposureExceeded { .. })
    ));

    assert!(
        risk.cancel_admission("owner-a", &first_owner_ticket, at(9, 4))
            .await
            .unwrap()
    );
    assert!(matches!(
        risk.admit(&candidate("owner-c", "BTC-USDT", "1"), at(9, 5))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
}

#[tokio::test]
async fn cancelling_an_increase_keeps_the_existing_position_clock() {
    let path = temp_path("admission-increase-cancel");
    let journal_id = Uuid::new_v4();
    let risk = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            max_position_duration: Some(Duration::seconds(60)),
            ..AccountRiskLimits::default()
        },
    );

    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDT", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    reserve_owner_leg(journal_id, &path, "paper-main", "owner", "10").await;
    let increase_ticket = admitted_ticket(
        risk.admit(&candidate("owner", "BTC-USDT", "5"), at(9, 1))
            .await
            .unwrap(),
    );

    assert!(
        risk.cancel_admission("owner", &increase_ticket, at(9, 2))
            .await
            .unwrap()
    );
    let state = risk.state().await.unwrap();
    assert_eq!(state.open_positions.len(), 1);
    assert_eq!(state.open_positions[0].task_id, "owner");
    assert_eq!(state.open_positions[0].opened_at, at(9, 0));
    assert_eq!(
        risk.directives(at(9, 2)).await.unwrap(),
        vec![AccountRiskDirective::ClosePosition {
            task_id: "owner".to_owned(),
            symbol: "BTC-USDT".to_owned(),
        }]
    );
}

#[tokio::test]
async fn cancelling_same_timestamp_increase_does_not_clear_the_opening_clock() {
    let path = temp_path("admission-same-timestamp-cancel");
    let journal_id = Uuid::new_v4();
    let risk = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            max_position_duration: Some(Duration::seconds(60)),
            ..AccountRiskLimits::default()
        },
    );

    let opened_at = at(9, 0);
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDT", "10"), opened_at)
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    reserve_owner_leg(journal_id, &path, "paper-main", "owner", "10").await;
    let increase_ticket = admitted_ticket(
        risk.admit(&candidate("owner", "BTC-USDT", "5"), opened_at)
            .await
            .unwrap(),
    );

    assert!(
        risk.cancel_admission("owner", &increase_ticket, at(9, 1))
            .await
            .unwrap()
    );
    let state = risk.state().await.unwrap();
    assert_eq!(state.open_positions.len(), 1);
    assert_eq!(state.open_positions[0].opened_at, opened_at);
}

#[tokio::test]
async fn exposure_caps_compose_the_paper_reservation_projection() {
    let path = temp_path("exposure-caps");
    let journal_id = Uuid::new_v4();
    reserve_paper_exposure(journal_id, &path, "paper-main").await;

    // Two non-released 100-notional BTC-USDT legs exist, so symbol exposure
    // is 200 and total pending exposure carries the 30 bps cost buffer.
    let symbol_capped = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            max_symbol_exposure: Some(money("250")),
            ..AccountRiskLimits::default()
        },
    );
    assert!(matches!(
        symbol_capped
            .admit(&candidate("owner", "BTC-USDT", "50"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(matches!(
        symbol_capped
            .admit(&candidate("owner-b", "BTC-USDT", "51"), at(9, 1))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::SymbolExposureExceeded { .. })
    ));
    // Another symbol is not constrained by the BTC exposure.
    assert!(matches!(
        symbol_capped
            .admit(&candidate("owner-c", "ETH-USDT", "51"), at(9, 2))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));

    let total_capped = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            max_total_exposure: Some(money("250")),
            ..AccountRiskLimits::default()
        },
    );
    assert!(matches!(
        total_capped
            .admit(&candidate("owner-d", "ETH-USDT", "100"), at(9, 3))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::TotalExposureExceeded { .. })
    ));
}

#[tokio::test]
async fn balance_thresholds_use_total_balance_not_available() {
    let path = temp_path("balance-total");
    let journal_id = Uuid::new_v4();
    reserve_paper_exposure(journal_id, &path, "paper-main").await;

    // Available dropped to 799.40 but total stays 1000: the close threshold
    // mirrors the legacy controller and must judge the total.
    let close_at_900 = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            min_balance_close: Some(money("900")),
            ..AccountRiskLimits::default()
        },
    );
    assert!(matches!(
        close_at_900
            .admit(&candidate("owner", "BTC-USDT", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(close_at_900.directives(at(9, 0)).await.unwrap().is_empty());

    let close_at_1500 = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            min_balance_warning: Some(money("2000")),
            min_balance_close: Some(money("1500")),
            ..AccountRiskLimits::default()
        },
    );
    assert!(matches!(
        close_at_1500
            .admit(&candidate("owner-b", "BTC-USDT", "10"), at(9, 1))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::BalanceBelowCloseThreshold { .. })
    ));
    let directives = close_at_1500.directives(at(9, 1)).await.unwrap();
    assert!(directives.iter().any(|directive| matches!(
        directive,
        AccountRiskDirective::CloseAllPositions { reason }
            if reason == "balance_below_close_threshold"
    )));
}

#[tokio::test]
async fn exact_execution_fees_lower_the_balance_used_by_close_directives() {
    let path = temp_path("exact-fee-balance");
    let journal_id = Uuid::new_v4();
    let account = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();
    let intent = OrderIntent::market(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let request = PaperReservationRequest::new(
        Uuid::new_v4(),
        "grid:btc/op/fee",
        "grid-fee",
        Uuid::new_v4(),
        PaperCostModel::v1(10, 0, 0).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &intent, money("100")).unwrap()],
    )
    .unwrap();
    let reservation_id = request.reservation_id();
    account.reserve(request).await.unwrap();
    account
        .settle_execution(
            reservation_id,
            &[TradingReceipt::Submitted {
                order: Order {
                    id: "paper-grid:BTC-USDT:fee".to_owned(),
                    intent,
                    filled_quantity: Quantity::new(Decimal::ONE).unwrap(),
                    average_fill_price: Some(Price::new(Decimal::from(100_u32)).unwrap()),
                    status: OrderStatus::Filled,
                    created_at: at(10, 0),
                    updated_at: at(10, 0),
                },
                disposition: SubmissionDisposition::Filled,
            }],
        )
        .await
        .unwrap();

    let risk = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            min_balance_close: Some(money("999.95")),
            ..AccountRiskLimits::default()
        },
    );
    assert!(
        risk.directives(at(10, 1))
            .await
            .unwrap()
            .iter()
            .any(|directive| matches!(
                directive,
                AccountRiskDirective::CloseAllPositions { reason }
                    if reason == "balance_below_close_threshold"
            ))
    );
}

#[tokio::test]
async fn disabled_symbol_rejections_are_durable_facts() {
    let path = temp_path("disabled-symbol");
    let journal_id = Uuid::new_v4();
    let risk = authority(
        journal_id,
        &path,
        AccountRiskLimits {
            disabled_symbols: BTreeSet::from(["LUNA-USDC-PERP".to_owned()]),
            ..AccountRiskLimits::default()
        },
    );
    assert!(matches!(
        risk.admit(&candidate("owner", "luna-usdc-perp", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::SymbolDisabled { .. })
    ));
    let state = risk.state().await.unwrap();
    assert_eq!(state.rejected_count, 1);
    assert_eq!(state.last_rejection.as_deref(), Some("symbol_disabled"));
    assert!(state.open_positions.is_empty());
}

#[tokio::test]
async fn pause_resume_and_kill_switch_replay_from_durable_facts() {
    let path = temp_path("pause-kill");
    let journal_id = Uuid::new_v4();
    let limits = AccountRiskLimits::default;
    let risk = authority(journal_id, &path, limits());

    risk.pause("exchange maintenance", at(9, 0)).await.unwrap();
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 1))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::Paused { .. })
    ));
    // Pause with the same reason is idempotent, and resume restores admission.
    risk.pause("exchange maintenance", at(9, 2)).await.unwrap();
    let resumed = risk.resume(at(9, 3)).await.unwrap();
    assert!(!resumed.paused);
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 4))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));

    let engaged = risk
        .engage_kill_switch("operator drill", at(9, 5))
        .await
        .unwrap();
    assert!(engaged.kill_switch_engaged);
    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 6))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::KillSwitchEngaged { .. })
    ));

    // The kill switch is latching: a restart replays it and resume never
    // clears it.
    let restarted = authority(journal_id, &path, limits());
    let state = restarted.resume(at(9, 7)).await.unwrap();
    assert!(state.kill_switch_engaged);
    assert!(matches!(
        restarted
            .admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 8))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::KillSwitchEngaged { .. })
    ));
    let directives = restarted.directives(at(9, 9)).await.unwrap();
    assert!(directives.iter().any(|directive| matches!(
        directive,
        AccountRiskDirective::CloseAllPositions { reason } if reason.starts_with("kill_switch:")
    )));
}

#[tokio::test]
async fn position_clocks_raise_timeout_directives_until_closed() {
    let path = temp_path("position-clock");
    let journal_id = Uuid::new_v4();
    let limits = || AccountRiskLimits {
        max_position_duration: Some(Duration::seconds(60)),
        ..AccountRiskLimits::default()
    };
    let risk = authority(journal_id, &path, limits());

    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(risk.directives(at(9, 0)).await.unwrap().is_empty());

    // The clock is durable: a restarted authority still times the position.
    let restarted = authority(journal_id, &path, limits());
    let directives = restarted.directives(at(9, 2)).await.unwrap();
    assert_eq!(
        directives,
        vec![AccountRiskDirective::ClosePosition {
            task_id: "owner".to_owned(),
            symbol: "BTC-USDC-PERP".to_owned(),
        }]
    );

    restarted
        .record_position_closed("owner", at(9, 3))
        .await
        .unwrap();
    assert!(restarted.directives(at(9, 4)).await.unwrap().is_empty());
    // Closing an unknown owner is a durable no-op.
    restarted
        .record_position_closed("owner", at(9, 5))
        .await
        .unwrap();
    let state = restarted.state().await.unwrap();
    assert!(state.open_positions.is_empty());
    assert_eq!(state.admitted_count, 1);
}

#[tokio::test]
async fn sealed_chain_without_active_file_replays_the_latched_kill_switch() {
    let path = temp_path("sealed-chain");
    let journal_id = Uuid::new_v4();
    let risk = authority(journal_id, &path, AccountRiskLimits::default());

    assert!(matches!(
        risk.admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    risk.engage_kill_switch("operator drill", at(9, 1))
        .await
        .unwrap();

    // Crash point between sealing the active file and recreating it: the
    // sealed chain `<path>.1` alone is the complete durable record and must
    // replay every risk fact, not silently read as an empty journal.
    let sealed = {
        let mut sealed = path.clone().into_os_string();
        sealed.push(".1");
        std::path::PathBuf::from(sealed)
    };
    std::fs::rename(&path, &sealed).unwrap();

    let restarted = authority(journal_id, &path, AccountRiskLimits::default());
    let state = restarted.state().await.unwrap();
    assert!(state.kill_switch_engaged);
    assert_eq!(state.admitted_count, 1);
    assert!(matches!(
        restarted
            .admit(&candidate("owner", "BTC-USDC-PERP", "10"), at(9, 2))
            .await
            .unwrap(),
        AccountRiskAdmission::Rejected(AccountRiskRejection::KillSwitchEngaged { .. })
    ));

    // A journal with neither an active file nor sealed segments still loads
    // as a fresh empty scope.
    let fresh = authority(
        Uuid::new_v4(),
        &temp_path("sealed-chain-fresh"),
        AccountRiskLimits::default(),
    );
    let state = fresh.state().await.unwrap();
    assert!(!state.kill_switch_engaged);
    assert_eq!(state.admitted_count, 0);
}
