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
            projected: decimal("200"),
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
