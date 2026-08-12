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
    AccountRiskDirective, AccountRiskError, DecisionRecord, JsonlHistory, PaperAccountAuthority,
    PaperAccountConfig, PaperCostModel, PaperReservationLeg, PaperReservationRequest,
};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy, AccountRiskRejection};
use rust_decimal::Decimal;
use serde_json::json;
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
    authority_for_scope(journal_id, path, "paper", limits)
}

fn authority_for_scope(
    journal_id: Uuid,
    path: &std::path::Path,
    scope_id: &str,
    limits: AccountRiskLimits,
) -> AccountRiskAuthority {
    AccountRiskAuthority::new(
        journal_id,
        JsonlHistory::new(path),
        scope_id,
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
    reserve_task_leg(
        journal_id,
        path,
        account_id,
        &format!("{owner_task_id}/op/000001"),
        notional,
    )
    .await;
}

async fn reserve_task_leg(
    journal_id: Uuid,
    path: &std::path::Path,
    account_id: &str,
    reservation_task_id: &str,
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
        reservation_task_id,
        format!("risk-gap:{reservation_task_id}"),
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &intent, money(notional)).unwrap()],
    )
    .unwrap();
    account.reserve(request).await.unwrap();
}

async fn append_legacy_reserved_fact(
    journal_id: Uuid,
    path: &std::path::Path,
    account_id: &str,
    request: PaperReservationRequest,
) {
    let reserved_exposure = request.gross_notional().unwrap();
    JsonlHistory::new(path)
        .append(&DecisionRecord {
            timestamp: at(9, 1),
            strategy: "paper_account".to_owned(),
            symbol: account_id.to_owned(),
            decision: "paper_account_reserved".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": account_id,
                "initial_available": money("1000"),
                "request": request,
                "reserved_exposure": reserved_exposure,
            }),
        })
        .await
        .unwrap();
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
async fn sixty_fifth_live_admission_is_refused_before_it_reaches_the_journal() {
    let path = temp_path("live-admission-cap");
    let journal_id = Uuid::new_v4();
    let risk = authority(journal_id, &path, AccountRiskLimits::default());

    for index in 0..64 {
        let admission = risk
            .admit(
                &candidate(&format!("owner-{index}"), "BTC-USDT", "1"),
                at(9, 0),
            )
            .await
            .unwrap();
        assert!(matches!(admission, AccountRiskAdmission::Admitted { .. }));
    }

    assert!(
        risk.admit(&candidate("owner-64", "BTC-USDT", "1"), at(9, 1))
            .await
            .is_err(),
        "the live capacity guard must run before an admitted fact is durable"
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 64);
}

#[tokio::test]
async fn expired_admission_is_compensated_once_and_restart_recovers() {
    let path = temp_path("admission-lease");
    let journal_id = Uuid::new_v4();
    let risk = authority(journal_id, &path, AccountRiskLimits::default());
    let ticket = admitted_ticket(
        risk.admit(&candidate("owner-a", "BTC-USDT", "10"), at(9, 0))
            .await
            .unwrap(),
    );

    let first: DecisionRecord = serde_json::from_str(
        std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first.details["recorded_at"], json!(at(9, 0)));
    let lease_expires_at: DateTime<Utc> =
        serde_json::from_value(first.details["lease_expires_at"].clone()).unwrap();
    assert_eq!(lease_expires_at, first.timestamp + Duration::seconds(300));

    let recovery_now = Utc::now() + Duration::minutes(10);
    assert_eq!(
        risk.recover_expired_admissions(recovery_now).await.unwrap(),
        1
    );
    assert_eq!(
        risk.recover_expired_admissions(recovery_now).await.unwrap(),
        0
    );
    assert!(risk.state().await.unwrap().open_positions.is_empty());

    let records = std::fs::read_to_string(&path).unwrap();
    assert_eq!(records.lines().count(), 2);
    let expired: serde_json::Value = serde_json::from_str(records.lines().nth(1).unwrap()).unwrap();
    assert_eq!(expired["decision"], "account_risk_admission_expired");
    assert_eq!(expired["details"]["ticket_id"], ticket.as_str());

    let restarted = authority(journal_id, &path, AccountRiskLimits::default());
    assert!(restarted.state().await.unwrap().open_positions.is_empty());
    assert_eq!(
        restarted
            .recover_expired_admissions(recovery_now)
            .await
            .unwrap(),
        0
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 2);
}

#[tokio::test]
async fn v1_ticketless_admission_restarts_and_expires_with_a_stable_derived_ticket() {
    let path = temp_path("v1-ticketless-expiry");
    let journal_id = Uuid::new_v4();
    let wall_time = Utc::now() - Duration::minutes(10);
    JsonlHistory::new(&path)
        .append(&DecisionRecord {
            timestamp: wall_time,
            strategy: "account_risk".to_owned(),
            symbol: "paper".to_owned(),
            decision: "account_risk_admitted".to_owned(),
            details: json!({
                "kind": "admitted",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "task_id": "legacy-owner",
                "symbol": "BTC-USDT",
                "notional": money("10"),
                "utc_date": "2026-07-26",
                "recorded_at": at(9, 0),
                "warnings": [],
            }),
        })
        .await
        .unwrap();

    let restarted = authority(journal_id, &path, AccountRiskLimits::default());
    assert_eq!(restarted.state().await.unwrap().open_positions.len(), 1);
    assert_eq!(
        restarted
            .recover_expired_admissions(Utc::now())
            .await
            .unwrap(),
        1
    );
    let records = std::fs::read_to_string(&path).unwrap();
    let expired: serde_json::Value = serde_json::from_str(records.lines().nth(1).unwrap()).unwrap();
    assert_eq!(expired["details"]["schema_version"], 2);
    assert!(
        Uuid::parse_str(expired["details"]["ticket_id"].as_str().unwrap())
            .is_ok_and(|ticket| !ticket.is_nil())
    );

    let replayed = authority(journal_id, &path, AccountRiskLimits::default());
    assert!(replayed.state().await.unwrap().open_positions.is_empty());
    assert_eq!(
        replayed
            .recover_expired_admissions(Utc::now())
            .await
            .unwrap(),
        0
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 2);
}

#[tokio::test]
async fn oversized_pre_guard_pending_set_is_replayable_and_durably_recoverable() {
    let path = temp_path("legacy-pending-recovery");
    let journal_id = Uuid::new_v4();
    let history = JsonlHistory::new(&path);
    let wall_time = Utc::now() - Duration::minutes(10);
    let lease_expires_at = wall_time + Duration::minutes(5);
    let records = (0..65)
        .map(|index| DecisionRecord {
            timestamp: wall_time,
            strategy: "account_risk".to_owned(),
            symbol: "paper".to_owned(),
            decision: "account_risk_admitted".to_owned(),
            details: json!({
                "kind": "admitted",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "task_id": format!("legacy-owner-{index}"),
                "symbol": "BTC-USDT",
                "ticket_id": Uuid::new_v4().to_string(),
                "notional": money("1"),
                "utc_date": "2026-07-26",
                "recorded_at": at(9, 0),
                "lease_expires_at": lease_expires_at,
                "warnings": [],
            }),
        })
        .collect::<Vec<_>>();
    history.append_batch(&records).await.unwrap();

    let risk = authority(journal_id, &path, AccountRiskLimits::default());
    assert_eq!(risk.state().await.unwrap().open_positions.len(), 65);
    assert_eq!(
        risk.recover_expired_admissions(Utc::now()).await.unwrap(),
        65
    );

    let restarted = authority(journal_id, &path, AccountRiskLimits::default());
    assert!(restarted.state().await.unwrap().open_positions.is_empty());
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 130);
}

#[tokio::test]
async fn legacy_unbound_reservation_that_exceeds_pending_notional_degrades_replay() {
    let path = temp_path("legacy-overconsume-degrades");
    let journal_id = Uuid::new_v4();
    let risk = authority(journal_id, &path, AccountRiskLimits::default());

    assert!(matches!(
        risk.admit(&candidate("owner-gap", "BTC-USDT", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    let intent = OrderIntent::market(
        "paper-gap",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let request = PaperReservationRequest::new(
        Uuid::new_v4(),
        "owner-gap/op/000001",
        "legacy-overconsume",
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &intent, money("11")).unwrap()],
    )
    .unwrap();
    append_legacy_reserved_fact(journal_id, &path, "paper-main", request).await;

    let restarted = authority(journal_id, &path, AccountRiskLimits::default());
    assert!(
        matches!(
            restarted.state().await,
            Err(AccountRiskError::DegradedState)
        ),
        "legacy fallback must fail closed when reserved_notional exceeds the matching admission"
    );
}

#[tokio::test]
async fn legacy_fallback_ignores_reduce_only_legs_like_bound_replay() {
    let path = temp_path("legacy-reduce-only-ignored");
    let journal_id = Uuid::new_v4();
    let risk = authority(journal_id, &path, AccountRiskLimits::default());
    let account = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(&path),
        PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
    )
    .unwrap();

    assert!(matches!(
        risk.admit(&candidate("owner-legacy", "BTC-USDT", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    assert!(matches!(
        risk.admit(&candidate("owner-legacy", "BTC-USDT", "10"), at(9, 0))
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));

    let inventory_intent = OrderIntent::market(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let inventory_request = PaperReservationRequest::new(
        Uuid::new_v4(),
        "inventory-owner/op/000001",
        "legacy-inventory",
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![PaperReservationLeg::from_intent(0, &inventory_intent, money("10")).unwrap()],
    )
    .unwrap();
    let inventory_reservation_id = inventory_request.reservation_id();
    account.reserve(inventory_request).await.unwrap();
    account
        .settle_execution(
            inventory_reservation_id,
            &[TradingReceipt::Submitted {
                order: Order {
                    id: "inventory-open".to_owned(),
                    intent: inventory_intent.clone(),
                    filled_quantity: Quantity::new(Decimal::ONE).unwrap(),
                    average_fill_price: Some(Price::new(Decimal::from(10_u32)).unwrap()),
                    status: OrderStatus::Filled,
                    created_at: at(9, 0),
                    updated_at: at(9, 0),
                },
                disposition: SubmissionDisposition::Filled,
            }],
        )
        .await
        .unwrap();

    let opening = OrderIntent::market(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let mut reduce_only = OrderIntent::market(
        "paper-grid",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Sell,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    reduce_only.reduce_only = true;
    let request = PaperReservationRequest::new(
        Uuid::new_v4(),
        "owner-legacy/op/000001",
        "legacy-reduce-only",
        Uuid::new_v4(),
        PaperCostModel::v1(10, 5, 15).unwrap(),
        vec![
            PaperReservationLeg::from_intent(0, &opening, money("10")).unwrap(),
            PaperReservationLeg::from_intent(1, &reduce_only, money("10")).unwrap(),
        ],
    )
    .unwrap();
    account.reserve(request).await.unwrap();

    let restarted = authority(journal_id, &path, AccountRiskLimits::default());
    assert_eq!(restarted.state().await.unwrap().open_positions.len(), 1);
}

#[tokio::test]
async fn expiring_the_opening_ticket_promotes_the_next_live_ticket_clock() {
    let path = temp_path("parallel-ticket-expiry");
    let journal_id = Uuid::new_v4();
    let history = JsonlHistory::new(&path);
    let old_wall_time = Utc::now() - Duration::minutes(10);
    let live_wall_time = Utc::now();
    let admitted = |timestamp: DateTime<Utc>, recorded_at: DateTime<Utc>| DecisionRecord {
        timestamp,
        strategy: "account_risk".to_owned(),
        symbol: "paper".to_owned(),
        decision: "account_risk_admitted".to_owned(),
        details: json!({
            "kind": "admitted",
            "schema_version": 1,
            "journal_id": journal_id,
            "scope_id": "paper",
            "task_id": "owner-a",
            "symbol": "BTC-USDT",
            "ticket_id": Uuid::new_v4().to_string(),
            "notional": money("1"),
            "utc_date": recorded_at.format("%Y-%m-%d").to_string(),
            "recorded_at": recorded_at,
            "lease_expires_at": timestamp + Duration::minutes(5),
            "warnings": [],
        }),
    };
    history
        .append_batch(&[
            admitted(old_wall_time, at(9, 0)),
            admitted(live_wall_time, at(9, 1)),
        ])
        .await
        .unwrap();

    let risk = authority(journal_id, &path, AccountRiskLimits::default());
    assert_eq!(
        risk.recover_expired_admissions(Utc::now()).await.unwrap(),
        1
    );
    let state = risk.state().await.unwrap();
    assert_eq!(state.open_positions.len(), 1);
    assert_eq!(state.open_positions[0].opened_at, at(9, 1));
}

#[tokio::test]
async fn malformed_risk_fact_identity_and_money_fields_fail_closed() {
    for case in [
        "journal_id",
        "strategy",
        "scope",
        "ticket",
        "ticket_missing",
        "lease_missing",
        "notional",
    ] {
        let path = temp_path(&format!("invalid-{case}"));
        let journal_id = Uuid::new_v4();
        let wall_time = Utc::now();
        let mut record = DecisionRecord {
            timestamp: wall_time,
            strategy: "account_risk".to_owned(),
            symbol: "paper".to_owned(),
            decision: "account_risk_admitted".to_owned(),
            details: json!({
                "kind": "admitted",
                "schema_version": 2,
                "journal_id": journal_id,
                "scope_id": "paper",
                "task_id": "owner-a",
                "symbol": "BTC-USDT",
                "ticket_id": Uuid::new_v4().to_string(),
                "notional": money("10"),
                "utc_date": "2026-07-26",
                "recorded_at": at(9, 0),
                "lease_expires_at": wall_time + Duration::minutes(5),
                "warnings": [],
            }),
        };
        match case {
            "journal_id" => record.details["journal_id"] = json!(Uuid::new_v4()),
            "strategy" => record.strategy = "lookalike_risk".to_owned(),
            "scope" => record.symbol = "another-scope".to_owned(),
            "ticket" => record.details["ticket_id"] = json!(Uuid::nil().to_string()),
            "ticket_missing" => {
                record.details.as_object_mut().unwrap().remove("ticket_id");
            }
            "lease_missing" => {
                record
                    .details
                    .as_object_mut()
                    .unwrap()
                    .remove("lease_expires_at");
            }
            "notional" => record.details["notional"] = json!(money("0")),
            _ => unreachable!(),
        }
        JsonlHistory::new(&path).append(&record).await.unwrap();

        let risk = authority(journal_id, &path, AccountRiskLimits::default());
        assert!(
            matches!(risk.state().await, Err(AccountRiskError::DegradedState)),
            "{case} mismatch must degrade the decision projection"
        );
    }
}

#[tokio::test]
async fn reservation_consumption_and_cancellation_preserve_risk_scope_isolation() {
    let path = temp_path("scope-isolation");
    let journal_id = Uuid::new_v4();
    let risk_a = authority_for_scope(journal_id, &path, "scope-a", AccountRiskLimits::default());
    let risk_b = authority_for_scope(journal_id, &path, "scope-b", AccountRiskLimits::default());
    let owner = candidate("owner/a", "BTC-USDT", "10");
    let ticket_a = admitted_ticket(risk_a.admit(&owner, at(9, 0)).await.unwrap());
    let ticket_b = admitted_ticket(risk_b.admit(&owner, at(9, 1)).await.unwrap());

    assert!(
        risk_a
            .cancel_admission("owner/a", &ticket_a, at(9, 2))
            .await
            .unwrap()
    );
    assert!(risk_a.state().await.unwrap().open_positions.is_empty());
    assert_eq!(risk_b.state().await.unwrap().open_positions.len(), 1);
    assert!(
        !risk_b
            .cancel_admission("owner/a", &ticket_a, at(9, 3))
            .await
            .unwrap()
    );

    risk_a
        .record_position_closed("owner/a", at(9, 4))
        .await
        .unwrap();
    assert_eq!(risk_b.state().await.unwrap().open_positions.len(), 1);

    reserve_owner_leg(journal_id, &path, "paper-main", "owner/a", "10").await;
    assert!(
        !risk_b
            .cancel_admission("owner/a", &ticket_b, at(9, 5))
            .await
            .unwrap(),
        "the matching paper reservation must consume only scope-b's remaining ticket"
    );
}

#[tokio::test]
async fn legacy_reservation_fallback_accepts_only_a_numeric_operation_suffix() {
    let path = temp_path("strict-operation-suffix");
    let journal_id = Uuid::new_v4();
    let risk = authority(journal_id, &path, AccountRiskLimits::default());

    for (owner, forged_task) in [
        ("owner-fee", "owner-fee/op/fee"),
        ("owner-nested", "owner-nested/op/000001/extra"),
    ] {
        let ticket = admitted_ticket(
            risk.admit(&candidate(owner, "BTC-USDT", "10"), at(9, 0))
                .await
                .unwrap(),
        );
        reserve_task_leg(journal_id, &path, "paper-main", forged_task, "10").await;
        assert!(
            risk.cancel_admission(owner, &ticket, at(9, 1))
                .await
                .unwrap(),
            "forged suffix {forged_task} must not consume the admission"
        );
    }

    let ticket = admitted_ticket(
        risk.admit(&candidate("owner-valid", "BTC-USDT", "10"), at(9, 2))
            .await
            .unwrap(),
    );
    reserve_task_leg(
        journal_id,
        &path,
        "paper-main",
        "owner-valid/op/000001",
        "10",
    )
    .await;
    assert!(
        !risk
            .cancel_admission("owner-valid", &ticket, at(9, 3))
            .await
            .unwrap(),
        "the numeric legacy operation suffix must remain compatible"
    );
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
