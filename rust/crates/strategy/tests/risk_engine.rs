use std::str::FromStr;

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, Position, PositionSide, Price, Quantity, Side,
    Symbol,
};
use crypto_trading_strategy::{
    AccountRiskSnapshot, RiskDecision, RiskEngine, RiskLimits, RiskRejection,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).unwrap()
}

fn setup() -> (
    chrono::DateTime<Utc>,
    Symbol,
    MarketSnapshot,
    AccountRiskSnapshot,
) {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let symbol = Symbol::new("BTC").unwrap();
    let market = MarketSnapshot::new(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        price("99"),
        price("101"),
        now,
    )
    .unwrap();
    let account = AccountRiskSnapshot {
        equity: Money::new(decimal("1000")),
        available_balance: Money::new(decimal("500")),
        kill_switch: false,
        timestamp: now,
    };
    (now, symbol, market, account)
}

fn assert_rejects_single_and_batch(
    engine: &RiskEngine,
    intent: &OrderIntent,
    account: &AccountRiskSnapshot,
    positions: &[Position],
    market: &MarketSnapshot,
    now: chrono::DateTime<Utc>,
    rejection: &RiskRejection,
) {
    let expected = RiskDecision::Rejected(rejection.clone());
    assert_eq!(
        engine.authorize(intent, account, positions, market, now),
        expected
    );
    assert_eq!(
        engine.authorize_batch(
            std::slice::from_ref(intent),
            account,
            positions,
            std::slice::from_ref(market),
            now,
        ),
        expected
    );
}

#[test]
fn kill_switch_rejects_before_all_other_checks() {
    let (now, symbol, market, mut account) = setup();
    account.kill_switch = true;
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Rejected(RiskRejection::KillSwitchActive)
    );
}

#[test]
fn stale_market_data_is_rejected() {
    let (now, symbol, mut market, account) = setup();
    market.timestamp = now - Duration::seconds(6);
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Rejected(RiskRejection::StaleMarketData)
    );
}

#[test]
fn future_account_data_is_rejected_as_stale() {
    let (now, symbol, market, mut account) = setup();
    account.timestamp = now + Duration::milliseconds(1);
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Rejected(RiskRejection::StaleAccountData)
    );
}

#[test]
fn future_market_data_is_rejected_as_stale() {
    let (now, symbol, mut market, account) = setup();
    market.timestamp = now + Duration::milliseconds(1);
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Rejected(RiskRejection::StaleMarketData)
    );
}

#[test]
fn future_position_data_is_rejected_as_stale() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now + Duration::milliseconds(1),
    };
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[position], &market, now),
        RiskDecision::Rejected(RiskRejection::StalePositionData)
    );
}

#[test]
fn conflicting_long_and_short_positions_fail_closed() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let positions = [
        Position {
            exchange: "paper".to_owned(),
            symbol: symbol.clone(),
            market_type: MarketType::Perpetual,
            side: PositionSide::Long,
            quantity: quantity("1"),
            entry_price: Some(price("100")),
            mark_price: Some(price("100")),
            unrealized_pnl: Money::default(),
            updated_at: now,
        },
        Position {
            exchange: "paper".to_owned(),
            symbol: symbol.clone(),
            market_type: MarketType::Perpetual,
            side: PositionSide::Short,
            quantity: quantity("1"),
            entry_price: Some(price("100")),
            mark_price: Some(price("100")),
            unrealized_pnl: Money::default(),
            updated_at: now,
        },
    ];
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.1"),
    );

    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &account,
        &positions,
        &market,
        now,
        &RiskRejection::InvalidQuantity,
    );
}

#[test]
fn flat_position_with_non_zero_quantity_is_rejected() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let positions = [Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Flat,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    }];
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.1"),
    );

    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &account,
        &positions,
        &market,
        now,
        &RiskRejection::InvalidQuantity,
    );
}

#[test]
fn non_flat_position_with_zero_quantity_is_rejected() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let positions = [Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("0"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    }];
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("0.1"),
    );

    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &account,
        &positions,
        &market,
        now,
        &RiskRejection::InvalidQuantity,
    );
}

#[test]
fn market_buy_is_valued_at_the_ask() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("100"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Rejected(RiskRejection::MaxPositionValue {
            projected: decimal("101"),
            limit: decimal("100"),
        })
    );
}

#[test]
fn market_sell_at_the_bid_limit_is_authorized() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("99"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Authorized
    );
}

#[test]
fn opening_notional_is_bounded_by_the_lower_of_equity_and_available_balance() {
    let (now, symbol, market, mut account) = setup();
    account.equity = Money::new(decimal("1000"));
    account.available_balance = Money::new(decimal("100"));
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &account,
        &[],
        &market,
        now,
        &RiskRejection::OpeningNotionalExceedsBuyingPower {
            required: decimal("101"),
            buying_power: decimal("100"),
        },
    );
}

#[test]
fn aggressive_buy_limit_uses_limit_price_for_opening_notional() {
    let (now, symbol, market, mut account) = setup();
    account.equity = Money::new(decimal("149.99"));
    account.available_balance = Money::new(decimal("149.99"));
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::limit(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
        price("150"),
    );

    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &account,
        &[],
        &market,
        now,
        &RiskRejection::OpeningNotionalExceedsBuyingPower {
            required: decimal("150"),
            buying_power: decimal("149.99"),
        },
    );
}

#[test]
fn opening_is_rejected_when_equity_is_zero_even_if_available_is_positive() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.5"),
    );

    let mut constrained = account.clone();
    constrained.equity = Money::new(decimal("0"));
    constrained.available_balance = Money::new(decimal("1000"));
    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &constrained,
        &[],
        &market,
        now,
        &RiskRejection::OpeningNotionalExceedsBuyingPower {
            required: decimal("50.5"),
            buying_power: Decimal::ZERO,
        },
    );
}

#[test]
fn negative_account_truth_is_rejected_even_for_reductions() {
    let (now, symbol, market, mut account) = setup();
    account.equity = Money::new(decimal("1000"));
    account.available_balance = Money::new(decimal("1000"));
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let reduce = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
    );

    account.equity = Money::new(decimal("-0.01"));
    assert_rejects_single_and_batch(
        &engine,
        &reduce,
        &account,
        std::slice::from_ref(&position),
        &market,
        now,
        &RiskRejection::InvalidAccountState {
            field: "equity",
            value: decimal("-0.01"),
        },
    );

    account.equity = Money::new(decimal("1000"));
    account.available_balance = Money::new(decimal("-0.01"));
    assert_rejects_single_and_batch(
        &engine,
        &reduce,
        &account,
        &[position],
        &market,
        now,
        &RiskRejection::InvalidAccountState {
            field: "available_balance",
            value: decimal("-0.01"),
        },
    );
}

#[test]
fn projected_position_value_is_bounded_while_reduction_is_authorized() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("150"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let increase = OrderIntent::limit(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
        price("100"),
    );
    assert_eq!(
        engine.authorize(
            &increase,
            &account,
            std::slice::from_ref(&position),
            &market,
            now
        ),
        RiskDecision::Rejected(RiskRejection::MaxPositionValue {
            projected: decimal("202"),
            limit: decimal("150"),
        })
    );

    let mut reduce = OrderIntent::limit(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
        price("100"),
    );
    reduce.reduce_only = true;
    assert_eq!(
        engine.authorize(&reduce, &account, &[position], &market, now),
        RiskDecision::Authorized
    );
}

#[test]
fn reductions_remain_authorized_when_buying_power_is_exhausted() {
    let (now, symbol, market, mut account) = setup();
    account.equity = Money::default();
    account.available_balance = Money::default();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let reduce = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(
            &reduce,
            &account,
            std::slice::from_ref(&position),
            &market,
            now
        ),
        RiskDecision::Authorized
    );
    assert_eq!(
        engine.authorize_batch(&[reduce], &account, &[position], &[market], now),
        RiskDecision::Authorized
    );
}

#[test]
fn reduce_only_order_cannot_cross_through_flat_into_the_opposite_side() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let mut overshoot = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1.5"),
    );
    overshoot.reduce_only = true;

    assert_eq!(
        engine.authorize(&overshoot, &account, &[position], &market, now),
        RiskDecision::Rejected(RiskRejection::ReduceOnlyWouldIncrease)
    );
}

#[test]
fn crossing_through_flat_only_charges_the_opening_excess() {
    let (now, symbol, market, mut account) = setup();
    account.equity = Money::new(decimal("49.5"));
    account.available_balance = Money::new(decimal("49.5"));
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("1"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let cross = OrderIntent::market(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Sell,
        quantity("1.5"),
    );

    assert_eq!(
        engine.authorize(
            &cross,
            &account,
            std::slice::from_ref(&position),
            &market,
            now
        ),
        RiskDecision::Authorized
    );
    assert_eq!(
        engine.authorize_batch(
            std::slice::from_ref(&cross),
            &account,
            std::slice::from_ref(&position),
            std::slice::from_ref(&market),
            now
        ),
        RiskDecision::Authorized
    );

    account.equity = Money::new(decimal("49.4"));
    account.available_balance = Money::new(decimal("49.4"));
    assert_rejects_single_and_batch(
        &engine,
        &cross,
        &account,
        &[position],
        &market,
        now,
        &RiskRejection::OpeningNotionalExceedsBuyingPower {
            required: decimal("49.5"),
            buying_power: decimal("49.4"),
        },
    );
}

#[test]
fn underpriced_marketable_sell_limit_uses_the_executable_bid_for_risk() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("500"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::limit(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("100"),
        price("0.01"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &[], &market, now),
        RiskDecision::Rejected(RiskRejection::MaxPositionValue {
            projected: decimal("9900"),
            limit: decimal("500"),
        })
    );
}

#[test]
fn strict_reduce_only_exit_is_allowed_when_existing_exposure_is_over_limit() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("500"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("10"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let mut reduce = OrderIntent::limit(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
        price("100"),
    );
    reduce.reduce_only = true;

    assert_eq!(
        engine.authorize(&reduce, &account, &[position], &market, now),
        RiskDecision::Authorized
    );
}

#[test]
fn partial_reduction_without_reduce_only_is_allowed_when_existing_exposure_is_over_limit() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("500"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let position = Position {
        exchange: "paper".to_owned(),
        symbol: symbol.clone(),
        market_type: MarketType::Perpetual,
        side: PositionSide::Long,
        quantity: quantity("10"),
        entry_price: Some(price("100")),
        mark_price: Some(price("100")),
        unrealized_pnl: Money::default(),
        updated_at: now,
    };
    let reduce = OrderIntent::limit(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
        price("100"),
    );

    assert_eq!(
        engine.authorize(
            &reduce,
            &account,
            std::slice::from_ref(&position),
            &market,
            now
        ),
        RiskDecision::Authorized
    );
    assert_eq!(
        engine.authorize_batch(&[reduce], &account, &[position], &[market], now),
        RiskDecision::Authorized
    );
}

#[test]
fn batch_opening_notional_accumulates_across_legs() {
    let (now, symbol, market, mut account) = setup();
    account.equity = Money::new(decimal("201"));
    account.available_balance = Money::new(decimal("201"));
    let other_symbol = Symbol::new("ETH").unwrap();
    let other_market = MarketSnapshot::new(
        "paper-right",
        other_symbol.clone(),
        MarketType::Perpetual,
        price("101"),
        price("103"),
        now,
    )
    .unwrap();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let left = OrderIntent::market(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );
    let right = OrderIntent::market(
        "paper-right",
        other_symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&left, &account, &[], &market, now),
        RiskDecision::Authorized
    );
    assert_eq!(
        engine.authorize(&right, &account, &[], &other_market, now),
        RiskDecision::Authorized
    );
    assert_eq!(
        engine.authorize_batch(&[left, right], &account, &[], &[market, other_market], now),
        RiskDecision::Rejected(RiskRejection::OpeningNotionalExceedsBuyingPower {
            required: decimal("202"),
            buying_power: decimal("201"),
        })
    );
}

#[test]
fn spot_sell_cannot_open_short_inventory() {
    let now = Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, 0).unwrap();
    let symbol = Symbol::new("BTC-USDC").unwrap();
    let market = MarketSnapshot::new(
        "paper",
        symbol.clone(),
        MarketType::Spot,
        price("99"),
        price("101"),
        now,
    )
    .unwrap();
    let account = AccountRiskSnapshot {
        equity: Money::new(decimal("1000")),
        available_balance: Money::new(decimal("1000")),
        kill_switch: false,
        timestamp: now,
    };
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market("paper", symbol, MarketType::Spot, Side::Sell, quantity("1"));

    assert_rejects_single_and_batch(
        &engine,
        &intent,
        &account,
        &[],
        &market,
        now,
        &RiskRejection::SpotShortOpenUnsupported {
            opening: Decimal::ONE,
            projected: Decimal::ONE,
        },
    );
}

#[test]
fn all_matching_positions_are_aggregated_before_authorization() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("800"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let positions: Vec<_> = ["3", "4"]
        .into_iter()
        .map(|value| Position {
            exchange: "paper".to_owned(),
            symbol: symbol.clone(),
            market_type: MarketType::Perpetual,
            side: PositionSide::Long,
            quantity: quantity(value),
            entry_price: Some(price("100")),
            mark_price: Some(price("100")),
            unrealized_pnl: Money::default(),
            updated_at: now,
        })
        .collect();
    let increase = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&increase, &account, &positions, &market, now),
        RiskDecision::Rejected(RiskRejection::MaxPositionValue {
            projected: decimal("808"),
            limit: decimal("800"),
        })
    );
}

#[test]
fn batch_authorization_accounts_for_every_intent_before_reserving() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("100"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intents = vec![
        OrderIntent::market(
            "paper",
            symbol.clone(),
            MarketType::Perpetual,
            Side::Buy,
            quantity("0.6"),
        ),
        OrderIntent::market(
            "paper",
            symbol,
            MarketType::Perpetual,
            Side::Buy,
            quantity("0.6"),
        ),
    ];
    assert!(intents.iter().all(
        |intent| engine.authorize(intent, &account, &[], &market, now) == RiskDecision::Authorized
    ));

    assert_eq!(
        engine.authorize_batch(&intents, &account, &[], &[market], now),
        RiskDecision::Rejected(RiskRejection::MaxPositionValue {
            projected: decimal("121.2"),
            limit: decimal("100"),
        })
    );
}

#[test]
fn risk_limits_reject_unbounded_snapshot_ages() {
    assert!(
        RiskEngine::new(RiskLimits {
            max_position_value: decimal("1000"),
            max_snapshot_age: Duration::hours(25),
        })
        .is_err()
    );
}

#[test]
fn position_aggregation_returns_a_rejection_on_decimal_overflow() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: Decimal::MAX,
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let positions: Vec<_> = [Decimal::MAX, Decimal::ONE]
        .into_iter()
        .map(|position_quantity| Position {
            exchange: "paper".to_owned(),
            symbol: symbol.clone(),
            market_type: MarketType::Perpetual,
            side: PositionSide::Long,
            quantity: Quantity::new(position_quantity).unwrap(),
            entry_price: Some(price("100")),
            mark_price: Some(price("100")),
            unrealized_pnl: Money::default(),
            updated_at: now,
        })
        .collect();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );

    assert_eq!(
        engine.authorize(&intent, &account, &positions, &market, now),
        RiskDecision::Rejected(RiskRejection::ArithmeticOverflow)
    );
}

#[test]
fn risk_batch_rejects_intent_counts_above_the_business_limit() {
    let (now, symbol, market, account) = setup();
    let engine = RiskEngine::new(RiskLimits {
        max_position_value: decimal("1000"),
        max_snapshot_age: Duration::seconds(5),
    })
    .unwrap();
    let intent = OrderIntent::market(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        quantity("0.01"),
    );
    let intents = vec![intent; 10_001];

    assert_eq!(
        engine.authorize_batch(&intents, &account, &[], &[market], now),
        RiskDecision::Rejected(RiskRejection::InputLimitExceeded {
            input: "intents",
            count: 10_001,
            limit: 10_000,
        })
    );
}
