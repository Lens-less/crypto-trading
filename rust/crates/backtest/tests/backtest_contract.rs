use std::{collections::VecDeque, str::FromStr};

use chrono::{TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestEngine, BacktestError, EventTape, FillModel, Liquidity, MarketEvent, MarketEventPrice,
    OrderRequest, Strategy, StrategyContext, WalkForwardConfig, WalkForwardSplitter,
    adapt_order_intents,
};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, Price, Quantity, Side, Symbol,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must parse")
}

fn price(value: &str) -> Price {
    Price::from_str(value).expect("test price must parse")
}

fn quantity(value: &str) -> Quantity {
    Quantity::from_str(value).expect("test quantity must parse")
}

fn money(value: &str) -> Money {
    Money::from_str(value).expect("test money must parse")
}

fn event(second: i64, last: &str) -> MarketEvent {
    MarketEvent::new(Utc.timestamp_opt(second, 0).unwrap(), price(last))
}

fn snapshot(second: i64, bid: &str, ask: &str, last: Option<&str>) -> MarketSnapshot {
    let mut snapshot = MarketSnapshot::new(
        "binance",
        Symbol::new("BTC-USDT-PERP").unwrap(),
        MarketType::Perpetual,
        price(bid),
        price(ask),
        Utc.timestamp_opt(second, 0).unwrap(),
    )
    .unwrap();
    snapshot.last = last.map(price);
    snapshot
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThresholdStrategy {
    bought: bool,
    sold: bool,
}

impl Strategy for ThresholdStrategy {
    fn on_event(&mut self, context: &StrategyContext) -> Vec<OrderRequest> {
        if !self.bought && context.event.price <= price("100") {
            self.bought = true;
            return vec![OrderRequest::new(Side::Buy, quantity("1"), Liquidity::Taker).unwrap()];
        }
        if self.bought && !self.sold && context.event.price >= price("110") {
            self.sold = true;
            return vec![OrderRequest::new(Side::Sell, quantity("1"), Liquidity::Taker).unwrap()];
        }

        Vec::new()
    }
}

#[test]
fn identical_tape_and_strategy_produce_identical_results() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "105"), event(2, "110")]).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    let first = engine.run(
        &tape,
        &mut ThresholdStrategy {
            bought: false,
            sold: false,
        },
    );
    let second = engine.run(
        &tape,
        &mut ThresholdStrategy {
            bought: false,
            sold: false,
        },
    );

    assert_eq!(first.unwrap(), second.unwrap());
}

#[derive(Debug, Clone)]
struct IntentScript {
    frames: VecDeque<Vec<OrderRequest>>,
}

impl Strategy for IntentScript {
    fn on_event(&mut self, _context: &StrategyContext) -> Vec<OrderRequest> {
        self.frames.pop_front().unwrap_or_default()
    }
}

#[test]
fn market_intent_adapter_matches_hand_worked_fee_and_pnl_vector() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "110")]).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, decimal("10"), Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let symbol = Symbol::new("BTC-USDT-PERP").unwrap();
    let buy = OrderIntent::market(
        "binance",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );
    let sell = OrderIntent::market(
        "binance",
        symbol,
        MarketType::Perpetual,
        Side::Sell,
        quantity("1"),
    );
    let mut strategy = IntentScript {
        frames: [vec![buy], vec![sell]]
            .into_iter()
            .map(|frame| adapt_order_intents(&frame, Liquidity::Taker).unwrap())
            .collect(),
    };

    let result = engine.run(&tape, &mut strategy).unwrap();

    assert_eq!(result.trades.len(), 2);
    assert_eq!(result.trades[0].fill.fee, money("0.1"));
    assert_eq!(result.trades[0].realized_pnl_delta, money("-0.1"));
    assert_eq!(result.trades[1].fill.fee, money("0.11"));
    assert_eq!(result.trades[1].realized_pnl_delta, money("9.89"));
    assert_eq!(result.metrics.realized_pnl, money("9.79"));
    assert_eq!(result.metrics.ending_equity, money("1009.79"));
}

#[test]
fn limit_order_intents_are_rejected_instead_of_being_silently_mispriced() {
    let intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDT-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
        price("100"),
    );

    assert_eq!(
        OrderRequest::from_order_intent(&intent, Liquidity::Taker),
        Err(BacktestError::UnsupportedOrderIntent)
    );
}

#[test]
fn tape_adapts_market_snapshots_through_the_domain_seam() {
    let tape = EventTape::from_market_snapshots(
        &[
            snapshot(0, "99", "101", Some("100")),
            snapshot(1, "109", "111", None),
        ],
        MarketEventPrice::LastOrMid,
    )
    .unwrap();

    assert_eq!(tape.events().len(), 2);
    assert_eq!(tape.events()[0].price, price("100"));
    assert_eq!(tape.events()[1].price, price("110"));
}

#[test]
fn fill_model_rejects_sell_slippage_that_would_make_a_non_positive_price() {
    assert_eq!(
        FillModel::new(
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            decimal("10000"),
        ),
        Err(BacktestError::InvalidSlippageBasisPoints)
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuyAndHoldOnce {
    entered: bool,
}

impl Strategy for BuyAndHoldOnce {
    fn on_event(&mut self, _context: &StrategyContext) -> Vec<OrderRequest> {
        if self.entered {
            Vec::new()
        } else {
            self.entered = true;
            vec![OrderRequest::new(Side::Buy, quantity("1"), Liquidity::Taker).unwrap()]
        }
    }
}

#[test]
fn backtest_reports_drawdown_from_equity_curve() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "80"), event(2, "95")]).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    let result = engine
        .run(&tape, &mut BuyAndHoldOnce { entered: false })
        .unwrap();

    let drawdown = result.metrics.performance.max_drawdown.unwrap();
    assert_eq!(drawdown.amount, decimal("20"));
    assert_eq!(drawdown.ratio, decimal("0.02"));
}

#[test]
fn walk_forward_reports_only_out_of_sample_windows() {
    let splitter = WalkForwardSplitter::new(WalkForwardConfig::new(3, 2, 2).unwrap());
    let windows = splitter.out_of_sample_windows(8);

    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].range, 3..5);
    assert_eq!(windows[1].range, 5..7);
}

#[test]
fn walk_forward_rejects_every_zero_sized_component() {
    for config in [
        WalkForwardConfig::new(0, 1, 1),
        WalkForwardConfig::new(1, 0, 1),
        WalkForwardConfig::new(1, 1, 0),
    ] {
        assert_eq!(config, Err(BacktestError::InvalidWalkForwardConfig));
    }
}

#[test]
fn walk_forward_stops_cleanly_when_the_next_step_would_overflow() {
    let splitter = WalkForwardSplitter::new(
        WalkForwardConfig::new(1, 1, usize::MAX.checked_sub(2).unwrap()).unwrap(),
    );

    let windows = splitter.out_of_sample_windows(usize::MAX);

    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].range, 1..2);
    assert_eq!(windows[1].range, usize::MAX - 1..usize::MAX);
}
