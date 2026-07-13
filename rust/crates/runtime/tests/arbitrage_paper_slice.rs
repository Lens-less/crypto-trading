use std::{num::NonZeroUsize, str::FromStr, sync::Arc};

use chrono::Utc;
use crypto_trading_domain::{MarketSnapshot, MarketType, Price, Quantity, Symbol};
use crypto_trading_exchange::{ExchangeHandle, PaperExchange, ReconcileScope};
use crypto_trading_runtime::{ExchangeRouter, ExecutionMode, RuntimeError};
use crypto_trading_strategy::{
    ArbitrageState, ArbitrageStrategy, PairStrategyMachine, SegmentedArbitrageConfig,
};
use rust_decimal::Decimal;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn snapshot(exchange: &str, bid: &str, ask: &str) -> MarketSnapshot {
    MarketSnapshot::new(
        exchange,
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal(bid)).unwrap(),
        Price::new(decimal(ask)).unwrap(),
        Utc::now(),
    )
    .unwrap()
}

#[tokio::test]
async fn segmented_arbitrage_routes_both_legs_to_paper_adapters() {
    let left = Arc::new(PaperExchange::new("left", NonZeroUsize::new(8).unwrap()).unwrap());
    let right = Arc::new(PaperExchange::new("right", NonZeroUsize::new(8).unwrap()).unwrap());
    let left_quote = snapshot("left", "100", "101");
    let right_quote = snapshot("right", "103", "104");
    left.publish_snapshot(left_quote.clone()).await.unwrap();
    right.publish_snapshot(right_quote.clone()).await.unwrap();

    let strategy = ArbitrageStrategy::new(SegmentedArbitrageConfig {
        initial_spread_percent: decimal("1"),
        grid_step_percent: decimal("1"),
        max_segments: 3,
        base_quantity: Quantity::new(decimal("0.1")).unwrap(),
        first_close_ratio: decimal("0.5"),
    })
    .unwrap();
    let decision = strategy
        .evaluate_pair(&ArbitrageState::default(), &left_quote, &right_quote)
        .unwrap();
    assert_eq!(decision.intents.len(), 2);

    let mut router = ExchangeRouter::new(ExecutionMode::Paper);
    router.register("left", left.clone());
    router.register("right", right.clone());
    let receipts = router.execute_all(decision.intents).await.unwrap();
    assert_eq!(receipts.len(), 2);

    for exchange in [left, right] {
        let account = exchange.reconcile(ReconcileScope::All).await.unwrap();
        assert_eq!(account.orders.len(), 1);
    }
}

#[tokio::test]
async fn router_preflights_every_leg_before_submitting_the_first_order() {
    let left = Arc::new(PaperExchange::new("left", NonZeroUsize::new(8).unwrap()).unwrap());
    let left_quote = snapshot("left", "100", "101");
    let right_quote = snapshot("right", "103", "104");
    left.publish_snapshot(left_quote.clone()).await.unwrap();

    let strategy = ArbitrageStrategy::new(SegmentedArbitrageConfig {
        initial_spread_percent: decimal("1"),
        grid_step_percent: decimal("1"),
        max_segments: 3,
        base_quantity: Quantity::new(decimal("0.1")).unwrap(),
        first_close_ratio: decimal("0.5"),
    })
    .unwrap();
    let decision = strategy
        .evaluate_pair(&ArbitrageState::default(), &left_quote, &right_quote)
        .unwrap();

    let mut router = ExchangeRouter::new(ExecutionMode::Paper);
    router.register("left", Arc::clone(&left));
    let error = router.execute_all(decision.intents).await.unwrap_err();
    assert!(matches!(error, RuntimeError::UnknownExchange(name) if name == "right"));

    let account = left.reconcile(ReconcileScope::All).await.unwrap();
    assert!(account.orders.is_empty());
}
