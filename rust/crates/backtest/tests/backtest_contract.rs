use std::{collections::VecDeque, str::FromStr};

use chrono::{TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestEngine, BacktestError, EventTape, FillModel, Liquidity, MarketEvent, MarketEventPrice,
    OrderRequest, Strategy, StrategyContext, WalkForwardConfig, WalkForwardRunner,
    WalkForwardSplitter, adapt_order_intents,
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
    snapshot_for(
        second,
        "binance",
        "BTC-USDT-PERP",
        MarketType::Perpetual,
        bid,
        ask,
        last,
    )
}

fn snapshot_for(
    second: i64,
    exchange: &str,
    symbol: &str,
    market_type: MarketType,
    bid: &str,
    ask: &str,
    last: Option<&str>,
) -> MarketSnapshot {
    let mut snapshot = MarketSnapshot::new(
        exchange,
        Symbol::new(symbol).unwrap(),
        market_type,
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
    let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
    let buy = OrderIntent::market(
        "binance",
        symbol.clone(),
        MarketType::Spot,
        Side::Buy,
        quantity("1"),
    );
    let sell = OrderIntent::market(
        "binance",
        symbol,
        MarketType::Spot,
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
    assert_eq!(result.trades[0].closed_trade_pnl, None);
    assert_eq!(result.trades[1].fill.fee, money("0.11"));
    assert_eq!(result.trades[1].realized_pnl_delta, money("9.89"));
    assert_eq!(result.trades[1].closed_trade_pnl, Some(money("9.79")));
    assert_eq!(result.metrics.realized_pnl, money("9.79"));
    assert_eq!(result.metrics.ending_equity, money("1009.79"));
    assert_eq!(result.metrics.performance.win_rate, Some(Decimal::ONE));
    assert_eq!(result.metrics.performance.profit_factor, None);
    assert_eq!(result.metrics.periods_per_year, Some(decimal("31536000")));
}

#[test]
fn anonymous_event_tape_rejects_identified_perpetual_order_intents() {
    let tape = EventTape::new(vec![event(0, "100")]).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDT-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );
    let mut strategy = IntentScript {
        frames: [adapt_order_intents(&[intent], Liquidity::Taker).unwrap()].into(),
    };

    assert_eq!(
        engine.run(&tape, &mut strategy),
        Err(BacktestError::UnsupportedDerivativesMarginModel)
    );
}

#[test]
fn partial_closes_allocate_entry_and_exit_fees_to_closed_trade_metrics() {
    let tape = EventTape::new(vec![event(0, "100"), event(1, "110"), event(2, "120")]).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, decimal("10"), Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut strategy = IntentScript {
        frames: [
            vec![OrderRequest::new(Side::Buy, quantity("2"), Liquidity::Taker).unwrap()],
            vec![OrderRequest::new(Side::Sell, quantity("1"), Liquidity::Taker).unwrap()],
            vec![OrderRequest::new(Side::Sell, quantity("1"), Liquidity::Taker).unwrap()],
        ]
        .into(),
    };

    let result = engine.run(&tape, &mut strategy).unwrap();

    assert_eq!(result.trades[0].closed_trade_pnl, None);
    assert_eq!(result.trades[1].closed_trade_pnl, Some(money("9.79")));
    assert_eq!(result.trades[2].closed_trade_pnl, Some(money("19.78")));
    assert_eq!(result.metrics.realized_pnl, money("29.57"));
    assert_eq!(result.metrics.performance.win_rate, Some(Decimal::ONE));
}

#[test]
fn tape_annualization_uses_observation_count_and_elapsed_time() {
    let tape = EventTape::new(vec![event(0, "100"), event(2, "105"), event(4, "110")]).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
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

    assert_eq!(result.metrics.periods_per_year, Some(decimal("15768000")));
}

#[test]
fn tape_rejects_timestamps_that_move_backwards() {
    assert_eq!(
        EventTape::new(vec![event(1, "100"), event(0, "101")]),
        Err(BacktestError::NonMonotonicTape)
    );
}

#[test]
fn tape_preserves_equal_timestamp_events_and_marks_ratios_unavailable() {
    let events = vec![event(0, "100"), event(0, "90"), event(0, "110")];
    let tape = EventTape::new(events.clone()).unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut strategy = IntentScript {
        frames: [
            vec![OrderRequest::new(Side::Buy, quantity("1"), Liquidity::Taker).unwrap()],
            Vec::new(),
            vec![OrderRequest::new(Side::Sell, quantity("1"), Liquidity::Taker).unwrap()],
        ]
        .into(),
    };

    assert_eq!(tape.events(), events);
    let result = engine.run(&tape, &mut strategy).unwrap();
    assert_eq!(result.trades.len(), 2);
    assert_eq!(result.metrics.periods_per_year, None);
    assert_eq!(result.metrics.performance.sharpe_ratio, None);
    assert_eq!(result.metrics.performance.sortino_ratio, None);
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
    assert_eq!(tape.events()[0].bid, price("99"));
    assert_eq!(tape.events()[0].ask, price("101"));
    assert_eq!(tape.events()[1].price, price("110"));
    assert_eq!(tape.events()[1].bid, price("109"));
    assert_eq!(tape.events()[1].ask, price("111"));
}

#[test]
fn snapshot_tape_executes_taker_buys_at_ask_and_sells_at_bid() {
    let tape = EventTape::from_market_snapshots(
        &[
            snapshot_for(
                0,
                "binance",
                "BTC-USDT-SPOT",
                MarketType::Spot,
                "99",
                "101",
                Some("100"),
            ),
            snapshot_for(
                1,
                "binance",
                "BTC-USDT-SPOT",
                MarketType::Spot,
                "109",
                "111",
                Some("110"),
            ),
        ],
        MarketEventPrice::LastOrMid,
    )
    .unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut strategy = IntentScript {
        frames: [
            vec![OrderRequest::new(Side::Buy, quantity("1"), Liquidity::Taker).unwrap()],
            vec![OrderRequest::new(Side::Sell, quantity("1"), Liquidity::Taker).unwrap()],
        ]
        .into(),
    };

    let result = engine.run(&tape, &mut strategy).unwrap();

    assert_eq!(result.trades[0].fill.reference_price, price("101"));
    assert_eq!(result.trades[0].fill.fill_price, price("101"));
    assert_eq!(result.trades[1].fill.reference_price, price("109"));
    assert_eq!(result.trades[1].fill.fill_price, price("109"));
    assert_eq!(result.metrics.realized_pnl, money("8"));
}

#[test]
fn identified_perpetual_snapshot_tapes_fail_closed_without_a_margin_model() {
    let tape = EventTape::from_market_snapshots(
        &[
            snapshot(0, "99", "101", Some("100")),
            snapshot(1, "109", "111", Some("110")),
        ],
        MarketEventPrice::LastOrMid,
    )
    .unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    assert_eq!(
        engine.run(&tape, &mut BuyAndHoldOnce { entered: false }),
        Err(BacktestError::UnsupportedDerivativesMarginModel)
    );
}

#[test]
fn snapshot_tape_rejects_mixed_instrument_identity() {
    for mismatched in [
        snapshot_for(
            1,
            "coinbase",
            "BTC-USDT-PERP",
            MarketType::Perpetual,
            "109",
            "111",
            Some("110"),
        ),
        snapshot_for(
            1,
            "binance",
            "ETH-USDT-PERP",
            MarketType::Perpetual,
            "109",
            "111",
            Some("110"),
        ),
        snapshot_for(
            1,
            "binance",
            "BTC-USDT-PERP",
            MarketType::Spot,
            "109",
            "111",
            Some("110"),
        ),
    ] {
        assert_eq!(
            EventTape::from_market_snapshots(
                &[snapshot(0, "99", "101", Some("100")), mismatched],
                MarketEventPrice::LastOrMid,
            ),
            Err(BacktestError::MixedInstrumentTape)
        );
    }
}

#[test]
fn spot_buy_that_exceeds_cash_is_rejected_explicitly() {
    let tape = EventTape::from_market_snapshots(
        &[snapshot_for(
            0,
            "binance",
            "BTC-USDT-SPOT",
            MarketType::Spot,
            "99",
            "101",
            Some("100"),
        )],
        MarketEventPrice::LastOrMid,
    )
    .unwrap();
    let engine = BacktestEngine::new(
        money("100"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    assert_eq!(
        engine.run(&tape, &mut BuyAndHoldOnce { entered: false }),
        Err(BacktestError::InsufficientBuyingPower {
            required: money("101"),
            available: money("100"),
        })
    );
}

#[test]
fn spot_sell_that_exceeds_inventory_is_rejected_explicitly() {
    let tape = EventTape::from_market_snapshots(
        &[snapshot_for(
            0,
            "binance",
            "BTC-USDT-SPOT",
            MarketType::Spot,
            "99",
            "101",
            Some("100"),
        )],
        MarketEventPrice::LastOrMid,
    )
    .unwrap();
    let engine = BacktestEngine::new(
        money("100"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let mut strategy = IntentScript {
        frames: [vec![
            OrderRequest::new(Side::Sell, quantity("1"), Liquidity::Taker).unwrap(),
        ]]
        .into(),
    };

    assert_eq!(
        engine.run(&tape, &mut strategy),
        Err(BacktestError::InsufficientSpotInventory {
            required: quantity("1"),
            available: Quantity::default(),
        })
    );
}

#[test]
fn market_order_identity_must_match_snapshot_tape() {
    let tape = EventTape::from_market_snapshots(
        &[snapshot_for(
            0,
            "binance",
            "BTC-USDT-SPOT",
            MarketType::Spot,
            "99",
            "101",
            Some("100"),
        )],
        MarketEventPrice::LastOrMid,
    )
    .unwrap();
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("ETH-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        quantity("1"),
    );
    let mut strategy = IntentScript {
        frames: [adapt_order_intents(&[intent], Liquidity::Taker).unwrap()].into(),
    };
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    assert_eq!(
        engine.run(&tape, &mut strategy),
        Err(BacktestError::OrderInstrumentMismatch)
    );
}

#[test]
fn maker_liquidity_is_rejected_at_the_request_boundary_and_fill_defensively() {
    assert_eq!(
        OrderRequest::new(Side::Buy, quantity("1"), Liquidity::Maker),
        Err(BacktestError::UnsupportedMakerLiquidity)
    );
    let intent = OrderIntent::market(
        "binance",
        Symbol::new("BTC-USDT-PERP").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        quantity("1"),
    );
    assert_eq!(
        OrderRequest::from_order_intent(&intent, Liquidity::Maker),
        Err(BacktestError::UnsupportedMakerLiquidity)
    );

    let tape = EventTape::new(vec![event(0, "100")]).unwrap();
    let mut strategy = IntentScript {
        frames: [vec![OrderRequest {
            side: Side::Buy,
            quantity: quantity("1"),
            liquidity: Liquidity::Maker,
            instrument: None,
        }]]
        .into(),
    };
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();

    assert_eq!(
        engine.run(&tape, &mut strategy),
        Err(BacktestError::UnsupportedMakerLiquidity)
    );
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
    let windows = splitter.out_of_sample_windows(8).unwrap();

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
fn walk_forward_reports_index_overflow_instead_of_truncating_results() {
    let splitter = WalkForwardSplitter::new(
        WalkForwardConfig::new(1, 1, usize::MAX.checked_sub(2).unwrap()).unwrap(),
    );

    assert_eq!(
        splitter.out_of_sample_windows(usize::MAX),
        Err(BacktestError::WalkForwardIndexOverflow)
    );
}

#[test]
fn walk_forward_runner_selects_fresh_strategies_from_train_and_reports_only_test_results() {
    let tape = EventTape::new(
        (0_i64..8)
            .map(|second| event(second, &(100 + second).to_string()))
            .collect(),
    )
    .unwrap();
    let engine = BacktestEngine::new(
        money("1000"),
        FillModel::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO).unwrap(),
    )
    .unwrap();
    let runner = WalkForwardRunner::new(
        engine,
        WalkForwardSplitter::new(WalkForwardConfig::new(3, 2, 2).unwrap()),
    );
    let mut training_prices = Vec::new();

    let result = runner
        .run(&tape, |window_index, training_events| {
            training_prices.push((
                window_index,
                training_events
                    .iter()
                    .map(|event| event.price)
                    .collect::<Vec<_>>(),
            ));
            Ok(BuyAndHoldOnce { entered: false })
        })
        .unwrap();

    assert_eq!(training_prices.len(), 2);
    assert_eq!(
        training_prices[0].1,
        vec![price("100"), price("101"), price("102")]
    );
    assert_eq!(
        training_prices[1].1,
        vec![price("102"), price("103"), price("104")]
    );
    assert_eq!(result.windows.len(), 2);
    assert_eq!(result.windows[0].range, 3..5);
    assert_eq!(
        result.windows[0]
            .result
            .equity_curve
            .iter()
            .map(|point| point.price)
            .collect::<Vec<_>>(),
        vec![price("103"), price("104")]
    );
    assert_eq!(result.windows[1].range, 5..7);
    assert_eq!(
        result.windows[1]
            .result
            .equity_curve
            .iter()
            .map(|point| point.price)
            .collect::<Vec<_>>(),
        vec![price("105"), price("106")]
    );
}
