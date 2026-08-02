use std::str::FromStr;

use chrono::{TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestEngine, BacktestError, EventTape, FillModel, Liquidity, MarketEvent, OrderRequest,
    Side, Strategy, StrategyContext, WalkForwardConfig, WalkForwardSplitter,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must parse")
}

fn event(second: i64, price: &str) -> MarketEvent {
    MarketEvent::new(Utc.timestamp_opt(second, 0).unwrap(), decimal(price)).unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThresholdStrategy {
    bought: bool,
    sold: bool,
}

impl Strategy for ThresholdStrategy {
    fn on_event(&mut self, context: &StrategyContext) -> Vec<OrderRequest> {
        if !self.bought && context.event.price <= decimal("100") {
            self.bought = true;
            return vec![OrderRequest::new(Side::Buy, Decimal::ONE, Liquidity::Taker).unwrap()];
        }
        if self.bought && !self.sold && context.event.price >= decimal("110") {
            self.sold = true;
            return vec![OrderRequest::new(Side::Sell, Decimal::ONE, Liquidity::Taker).unwrap()];
        }

        Vec::new()
    }
}

#[test]
fn identical_tape_and_strategy_produce_identical_results() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "105"), event(2, "110")]).unwrap();
    let engine = BacktestEngine::new(
        decimal("1000"),
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

#[test]
fn backtest_matches_hand_worked_fee_and_pnl_vector() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "110")]).unwrap();
    let engine = BacktestEngine::new(
        decimal("1000"),
        FillModel::new(Decimal::ZERO, decimal("10"), Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    let result = engine
        .run(
            &tape,
            &mut ThresholdStrategy {
                bought: false,
                sold: false,
            },
        )
        .unwrap();

    assert_eq!(result.trades.len(), 2);
    assert_eq!(result.trades[0].fill.fee, decimal("0.1"));
    assert_eq!(result.trades[0].realized_pnl_delta, decimal("-0.1"));
    assert_eq!(result.trades[1].fill.fee, decimal("0.11"));
    assert_eq!(result.trades[1].realized_pnl_delta, decimal("9.89"));
    assert_eq!(result.metrics.realized_pnl, decimal("9.79"));
    assert_eq!(result.metrics.ending_equity, decimal("1009.79"));
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
            vec![OrderRequest::new(Side::Buy, Decimal::ONE, Liquidity::Taker).unwrap()]
        }
    }
}

#[test]
fn backtest_reports_drawdown_from_equity_curve() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "80"), event(2, "95")]).unwrap();
    let engine = BacktestEngine::new(
        decimal("1000"),
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
