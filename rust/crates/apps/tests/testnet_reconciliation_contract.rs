use std::{
    path::PathBuf,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{TimeZone, Utc};
use crypto_trading_cli::{
    TestnetReconciliationConfig, TestnetReconciliationMismatch, TestnetReconciliationPlan,
};
use crypto_trading_domain::{
    MarketType, Money, Order, OrderIntent, OrderStatus, OrderType, Position, PositionSide, Price,
    Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceProduct, BinanceTestnetAccountSnapshot, BinanceTestnetBalance, ForeignOrder,
};
use crypto_trading_runtime::{
    JsonlHistory, PAPER_ACCOUNT_SCHEMA_VERSION, PaperAccountAuthority, PaperAccountConfig,
    PaperAccountSnapshot, PaperCostModel, PaperExecutionLedgerKind, PaperReconciliationOutcome,
    PaperReservationLeg, PaperReservationPhase, PaperReservationRequest, PaperReservationView,
    ProjectionStatus,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn ids() -> (Uuid, Uuid, Uuid) {
    (
        Uuid::parse_str("85ad0b40-5930-4ac8-9857-f3d2ec679394").unwrap(),
        Uuid::parse_str("5252fd91-cd35-4bff-9cfa-fe8634c38cc3").unwrap(),
        Uuid::parse_str("aa2ce047-b50a-48b4-b5b8-b68c1a78d5fb").unwrap(),
    )
}

fn temp_history(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-testnet-reconcile-transition-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn account(symbol: &Symbol) -> PaperAccountSnapshot {
    let (journal_id, reservation_id, batch_id) = ids();
    let intent = OrderIntent::market(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(decimal("0.001")).unwrap(),
    );
    let leg = PaperReservationLeg::from_intent(0, &intent, Money::new(decimal("100"))).unwrap();
    PaperAccountSnapshot {
        schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
        journal_id,
        projection_status: ProjectionStatus::Complete,
        invalid_event_count: 0,
        account_id: "paper-main".to_owned(),
        initial_available: Money::new(decimal("1000")),
        available: Money::new(decimal("900")),
        pending_reserved: Money::default(),
        uncertain_reserved: Money::default(),
        committed_exposure: Money::new(decimal("100")),
        ledger_kind: PaperExecutionLedgerKind::LegacyReservationOnly,
        cumulative_fees: Money::default(),
        realized_pnl: Money::default(),
        settled_equity_base: Money::new(decimal("1000")),
        open_lots: Vec::new(),
        reservations: vec![PaperReservationView {
            reservation_id,
            task_id: "grid-btc".to_owned(),
            idempotency_key: "grid-btc-001".to_owned(),
            batch_id,
            cost_model: PaperCostModel::v1(0, 0, 0).unwrap(),
            legs: vec![leg],
            reserved_exposure: Money::new(decimal("100")),
            held_exposure: Money::new(decimal("100")),
            phase: PaperReservationPhase::Committed,
            first_sequence: 1,
            last_sequence: 2,
            reconciliation: None,
            ledger_kind: PaperExecutionLedgerKind::LegacyReservationOnly,
            settlement: None,
        }],
    }
}

fn remote(available: &str) -> BinanceTestnetAccountSnapshot {
    BinanceTestnetAccountSnapshot {
        product: BinanceProduct::Spot,
        balances: vec![BinanceTestnetBalance {
            asset: "USDT".to_owned(),
            wallet_balance: decimal(available),
            available_balance: decimal(available),
            locked_balance: Some(Decimal::ZERO),
        }],
        orders: Vec::new(),
        foreign_orders: Vec::new(),
        positions: Vec::new(),
        observed_at: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap(),
    }
}

fn remote_balance(
    asset: &str,
    wallet_balance: &str,
    available_balance: &str,
    locked_balance: Option<&str>,
) -> BinanceTestnetBalance {
    BinanceTestnetBalance {
        asset: asset.to_owned(),
        wallet_balance: decimal(wallet_balance),
        available_balance: decimal(available_balance),
        locked_balance: locked_balance.map(decimal),
    }
}

fn order(symbol: &Symbol, status: OrderStatus, filled_quantity: &str) -> Order {
    let mut intent = OrderIntent::limit(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(decimal("0.001")).unwrap(),
        Price::new(decimal("100000")).unwrap(),
    );
    intent.time_in_force = TimeInForce::Gtc;
    Order {
        id: format!("binance:spot:{}:owned-order", symbol.as_str()),
        intent,
        filled_quantity: Quantity::new(decimal(filled_quantity)).unwrap(),
        average_fill_price: (!decimal(filled_quantity).is_zero())
            .then(|| Price::new(decimal("100000")).unwrap()),
        status,
        created_at: Utc.with_ymd_and_hms(2026, 7, 25, 11, 59, 0).unwrap(),
        updated_at: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap(),
    }
}

fn foreign_order(symbol: &Symbol, id: &str, filled_quantity: &str) -> ForeignOrder {
    ForeignOrder {
        id: id.to_owned(),
        client_order_id: Some(format!("manual-{id}")),
        exchange: "binance".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Spot,
        side: Side::Sell,
        order_type: OrderType::Limit,
        quantity: Quantity::new(decimal("0.001")).unwrap(),
        price: Some(Price::new(decimal("99999")).unwrap()),
        reduce_only: false,
        time_in_force: TimeInForce::Gtc,
        filled_quantity: Quantity::new(decimal(filled_quantity)).unwrap(),
        average_fill_price: (!decimal(filled_quantity).is_zero())
            .then(|| Price::new(decimal("99999")).unwrap()),
        status: if decimal(filled_quantity).is_zero() {
            OrderStatus::Open
        } else {
            OrderStatus::PartiallyFilled
        },
        created_at: Utc.with_ymd_and_hms(2026, 7, 25, 11, 58, 0).unwrap(),
        updated_at: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap(),
    }
}

fn position(symbol: &Symbol, side: PositionSide, quantity: &str) -> Position {
    Position {
        exchange: "binance".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Spot,
        side,
        quantity: Quantity::new(decimal(quantity)).unwrap(),
        entry_price: Some(Price::new(decimal("100000")).unwrap()),
        mark_price: Some(Price::new(decimal("100001")).unwrap()),
        unrealized_pnl: Money::new(decimal("0.1")),
        updated_at: Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap(),
    }
}

#[test]
fn clean_account_truth_produces_a_deterministic_release_proof() {
    let (_, reservation_id, batch_id) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        account(&symbol),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();

    let report = plan.compare(&remote("1000"), captured_at).unwrap();
    let repeated = plan.compare(&remote("1000.0"), captured_at).unwrap();

    assert!(report.matches());
    assert_eq!(report.expected_available, decimal("1000"));
    assert_eq!(report.proof.account_id(), "paper-main");
    assert_eq!(report.proof.reservation_id(), reservation_id);
    assert_eq!(report.proof.batch_id(), batch_id);
    assert_eq!(report.proof.digest().len(), 16);
    assert_eq!(report.proof, repeated.proof);
}

#[test]
fn balance_mismatch_and_missing_balance_fail_closed_without_losing_proof() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        account(&symbol),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();

    let mismatch = plan.compare(&remote("999"), captured_at).unwrap();
    assert!(!mismatch.matches());
    assert_eq!(
        mismatch.mismatches,
        vec![TestnetReconciliationMismatch::AvailableBalanceMismatch]
    );

    let mut missing = remote("1000");
    missing.balances.clear();
    let missing = plan.compare(&missing, captured_at).unwrap();
    assert_eq!(
        missing.mismatches,
        vec![TestnetReconciliationMismatch::MissingSettlementBalance]
    );
    assert_ne!(mismatch.proof.digest(), missing.proof.digest());

    let mut untracked = remote("1000");
    untracked.balances.push(BinanceTestnetBalance {
        asset: "BTC".to_owned(),
        wallet_balance: decimal("0.01"),
        available_balance: decimal("0.01"),
        locked_balance: Some(Decimal::ZERO),
    });
    let untracked = plan.compare(&untracked, captured_at).unwrap();
    assert_eq!(
        untracked.mismatches,
        vec![TestnetReconciliationMismatch::UntrackedAssetBalance]
    );
}

#[test]
fn locked_balance_non_zero_fails_closed() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(BinanceProduct::Spot, "USDT", symbol, reservation_id)
            .unwrap(),
        account(&Symbol::new("BTC-USDT-SPOT").unwrap()),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();
    let mut snapshot = remote("1000");
    snapshot.balances[0].locked_balance = Some(decimal("0.01"));

    let report = plan.compare(&snapshot, captured_at).unwrap();

    assert_eq!(
        report.mismatches,
        vec![TestnetReconciliationMismatch::LockedBalanceNonZero]
    );
}

#[test]
fn wallet_available_divergence_fails_closed_even_when_available_matches() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(BinanceProduct::Spot, "USDT", symbol, reservation_id)
            .unwrap(),
        account(&Symbol::new("BTC-USDT-SPOT").unwrap()),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();
    let mut snapshot = remote("1000");
    snapshot.balances[0].wallet_balance = decimal("1000.01");

    let report = plan.compare(&snapshot, captured_at).unwrap();

    assert_eq!(
        report.mismatches,
        vec![TestnetReconciliationMismatch::WalletAvailableDivergence]
    );
}

#[test]
fn open_owned_orders_fail_closed_for_partial_fills() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        account(&symbol),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();
    let mut snapshot = remote("1000");
    snapshot.orders.push(order(
        &Symbol::new("BTC-USDT-SPOT").unwrap(),
        OrderStatus::PartiallyFilled,
        "0.0005",
    ));

    let report = plan.compare(&snapshot, captured_at).unwrap();

    assert_eq!(
        report.mismatches,
        vec![TestnetReconciliationMismatch::OpenOwnedOrders]
    );
}

#[test]
fn open_foreign_orders_fail_closed_even_when_balances_match() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        account(&symbol),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();
    let mut snapshot = remote("1000");
    snapshot
        .foreign_orders
        .push(foreign_order(&symbol, "binance:spot:BTCUSDT:29", "0"));

    let report = plan.compare(&snapshot, captured_at).unwrap();

    assert_eq!(
        report.mismatches,
        vec![TestnetReconciliationMismatch::OpenForeignOrders]
    );
}

#[test]
fn non_flat_positions_fail_closed() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        account(&symbol),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();
    let mut snapshot = remote("1000");
    snapshot
        .positions
        .push(position(&symbol, PositionSide::Long, "0.001"));

    let report = plan.compare(&snapshot, captured_at).unwrap();

    assert_eq!(
        report.mismatches,
        vec![TestnetReconciliationMismatch::NonFlatPositions]
    );
}

#[test]
fn simultaneous_mismatches_preserve_stable_order_and_proof_digest() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        account(&symbol),
    )
    .unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();
    let expected_mismatches = vec![
        TestnetReconciliationMismatch::UntrackedAssetBalance,
        TestnetReconciliationMismatch::AvailableBalanceMismatch,
        TestnetReconciliationMismatch::LockedBalanceNonZero,
        TestnetReconciliationMismatch::WalletAvailableDivergence,
        TestnetReconciliationMismatch::OpenOwnedOrders,
        TestnetReconciliationMismatch::OpenForeignOrders,
        TestnetReconciliationMismatch::NonFlatPositions,
    ];
    let mut owned_open = order(&symbol, OrderStatus::Open, "0");
    owned_open.id = "binance:spot:BTC-USDT-SPOT:owned-open".to_owned();
    let mut owned_partial = order(&symbol, OrderStatus::PartiallyFilled, "0.0005");
    owned_partial.id = "binance:spot:BTC-USDT-SPOT:owned-partial".to_owned();
    let foreign_open = foreign_order(&symbol, "binance:spot:BTCUSDT:01", "0");
    let foreign_partial = foreign_order(&symbol, "binance:spot:BTCUSDT:99", "0.0001");
    let long_position = position(&symbol, PositionSide::Long, "0.001");
    let short_position = position(&symbol, PositionSide::Short, "0.002");

    let mut left = remote("999");
    left.balances = vec![
        remote_balance("USDT", "1001", "999", Some("0.01")),
        remote_balance("BTC", "0.01", "0.01", Some("0")),
    ];
    left.orders = vec![owned_partial.clone(), owned_open.clone()];
    left.foreign_orders = vec![foreign_partial.clone(), foreign_open.clone()];
    left.positions = vec![short_position.clone(), long_position.clone()];

    let mut right = remote("999");
    right.balances = vec![
        remote_balance("BTC", "0.01", "0.01", Some("0")),
        remote_balance("USDT", "1001", "999", Some("0.01")),
    ];
    right.orders = vec![owned_open, owned_partial];
    right.foreign_orders = vec![foreign_open, foreign_partial];
    right.positions = vec![long_position, short_position];

    let left_report = plan.compare(&left, captured_at).unwrap();
    let right_report = plan.compare(&right, captured_at).unwrap();

    assert_eq!(left_report.mismatches, expected_mismatches);
    assert_eq!(right_report.mismatches, expected_mismatches);
    assert_eq!(left_report.proof.digest(), right_report.proof.digest());
    assert_eq!(left_report.proof, right_report.proof);
}

#[test]
fn mixed_product_or_exchange_reservations_are_rejected_before_remote_io() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let mut account = account(&symbol);
    let intent = OrderIntent::market(
        "other",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(decimal("0.001")).unwrap(),
    );
    account.reservations[0].legs[0] =
        PaperReservationLeg::from_intent(0, &intent, Money::new(decimal("100"))).unwrap();

    let result = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(BinanceProduct::Spot, "USDT", symbol, reservation_id)
            .unwrap(),
        account,
    );

    assert!(result.is_err());
}

#[test]
fn reconciliation_scope_rejects_symbol_product_or_settlement_drift() {
    let (_, reservation_id, _) = ids();

    assert!(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            Symbol::new("BTC-USDC-SPOT").unwrap(),
            reservation_id,
        )
        .is_err()
    );
    assert!(
        TestnetReconciliationConfig::new(
            BinanceProduct::UsdM,
            "USDT",
            Symbol::new("BTC-USDT-SPOT").unwrap(),
            reservation_id,
        )
        .is_err()
    );
}

async fn committed_authority(path: &PathBuf) -> PaperAccountAuthority {
    let (journal_id, reservation_id, batch_id) = ids();
    let authority = PaperAccountAuthority::new(
        journal_id,
        JsonlHistory::new(path),
        PaperAccountConfig::new("paper-main", Money::new(decimal("1000"))).unwrap(),
    )
    .unwrap();
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(decimal("0.001")).unwrap(),
    );
    authority
        .reserve(
            PaperReservationRequest::new(
                reservation_id,
                "grid-btc",
                "grid-btc-001",
                batch_id,
                PaperCostModel::v1(0, 0, 0).unwrap(),
                vec![
                    PaperReservationLeg::from_intent(0, &intent, Money::new(decimal("100")))
                        .unwrap(),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    authority
        .commit(reservation_id, Money::new(decimal("100")))
        .await
        .unwrap();
    authority
}

#[tokio::test]
async fn comparator_proofs_drive_release_and_failure_transitions() {
    let (_, reservation_id, _) = ids();
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let captured_at = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 1).unwrap();

    let release_history = temp_history("release");
    let release_authority = committed_authority(&release_history).await;
    let release_plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(
            BinanceProduct::Spot,
            "USDT",
            symbol.clone(),
            reservation_id,
        )
        .unwrap(),
        release_authority.snapshot().await.unwrap(),
    )
    .unwrap();
    let release_report = release_plan.compare(&remote("1000"), captured_at).unwrap();
    let release_proof = release_report.proof.clone();
    let released_view = release_authority
        .reconcile_release(release_report.proof)
        .await
        .unwrap();
    assert_eq!(released_view.phase, PaperReservationPhase::Released);
    assert_eq!(
        released_view.reconciliation.as_ref().unwrap().outcome,
        PaperReconciliationOutcome::Released
    );
    assert_eq!(
        release_authority
            .reconcile_release(release_proof)
            .await
            .unwrap(),
        released_view
    );
    let released = release_authority.snapshot().await.unwrap();
    assert!(
        released.reservations.is_empty(),
        "released reservations must not consume the bounded live snapshot"
    );

    let failure_history = temp_history("failure");
    let failure_authority = committed_authority(&failure_history).await;
    let failure_plan = TestnetReconciliationPlan::new(
        TestnetReconciliationConfig::new(BinanceProduct::Spot, "USDT", symbol, reservation_id)
            .unwrap(),
        failure_authority.snapshot().await.unwrap(),
    )
    .unwrap();
    let failure_report = failure_plan.compare(&remote("999"), captured_at).unwrap();
    failure_authority
        .record_reconciliation_failure(failure_report.proof)
        .await
        .unwrap();
    let failed = failure_authority.snapshot().await.unwrap();
    assert_eq!(
        failed.reservations[0].phase,
        PaperReservationPhase::Committed
    );
    assert_eq!(
        failed.reservations[0]
            .reconciliation
            .as_ref()
            .unwrap()
            .outcome,
        PaperReconciliationOutcome::Failed
    );
}
