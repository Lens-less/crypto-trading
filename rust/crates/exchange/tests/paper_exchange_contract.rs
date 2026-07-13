use std::{num::NonZeroUsize, str::FromStr};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, OrderIntent, OrderStatus, PositionSide, Price, Quantity, Side,
    Symbol,
};
use crypto_trading_exchange::{
    CancellationDisposition, ExchangeError, ExchangeHandle, ExchangeMode, MarketSubscription,
    PaperExchange, ReconcileScope, SubmissionDisposition, TradingCommand, TradingReceipt,
    UnsupportedLiveExchange,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("test decimal must be valid")
}

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("test timestamp must be valid")
        .with_timezone(&Utc)
}

fn snapshot(bid: &str, ask: &str, at: &str) -> MarketSnapshot {
    MarketSnapshot::new(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal(bid)).unwrap(),
        Price::new(decimal(ask)).unwrap(),
        timestamp(at),
    )
    .unwrap()
}

fn market_buy() -> OrderIntent {
    let mut intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("2")).unwrap(),
    );
    intent.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10318").unwrap();
    intent
}

#[tokio::test]
async fn market_submission_is_deterministic_and_idempotent() {
    let paper = PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap();
    paper
        .publish_snapshot(snapshot("100.75", "101.25", "2026-07-14T01:02:03Z"))
        .await
        .unwrap();

    let command = TradingCommand::Submit(market_buy());
    let first = paper.execute(command.clone()).await.unwrap();
    let repeated = paper.execute(command).await.unwrap();

    let TradingReceipt::Submitted { order, disposition } = first else {
        panic!("submission must return a submission receipt");
    };
    assert_eq!(order.id, "paper-0000000000000001");
    assert_eq!(order.status, OrderStatus::Filled);
    assert_eq!(order.filled_quantity.as_decimal(), decimal("2"));
    assert_eq!(
        order.average_fill_price.unwrap().as_decimal(),
        decimal("101.25")
    );
    assert_eq!(order.created_at, timestamp("2026-07-14T01:02:03Z"));
    assert_eq!(disposition, SubmissionDisposition::Filled);

    let TradingReceipt::Submitted {
        order: repeated_order,
        disposition: repeated_disposition,
    } = repeated
    else {
        panic!("duplicate submission must return its original receipt");
    };
    assert_eq!(repeated_order.id, order.id);
    assert_eq!(
        repeated_disposition,
        SubmissionDisposition::AlreadyProcessed
    );

    let reconciled = paper
        .reconcile(ReconcileScope::Orders {
            symbol: Some(Symbol::new("BTC-USDT").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(reconciled.orders, vec![order]);

    let positions = paper
        .reconcile(ReconcileScope::Positions {
            symbol: Some(Symbol::new("BTC-USDT").unwrap()),
        })
        .await
        .unwrap();
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].side, PositionSide::Long);
    assert_eq!(positions.positions[0].quantity.as_decimal(), decimal("2"));
    assert_eq!(
        positions.positions[0].entry_price.unwrap().as_decimal(),
        decimal("101.25")
    );
}

#[tokio::test]
async fn resting_limit_fills_from_a_later_snapshot_and_subscription_observes_it() {
    let paper = PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap();
    let symbol = Symbol::new("BTC-USDT").unwrap();
    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:00:00Z"))
        .await
        .unwrap();
    let mut subscription = paper
        .subscribe(
            MarketSubscription::snapshots(vec![symbol.clone()], Some(MarketType::Perpetual))
                .unwrap(),
        )
        .await
        .unwrap();

    let mut intent = OrderIntent::limit(
        "paper",
        symbol.clone(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1.5")).unwrap(),
        Price::new(decimal("100")).unwrap(),
    );
    intent.client_order_id = Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap();

    let placed = paper.execute(TradingCommand::Submit(intent)).await.unwrap();
    assert_eq!(
        placed.submission_disposition(),
        Some(SubmissionDisposition::Open)
    );

    let next = snapshot("98.5", "99.5", "2026-07-14T01:01:00Z");
    paper.publish_snapshot(next.clone()).await.unwrap();
    assert_eq!(subscription.recv().await.unwrap(), next);

    let orders = paper.orders().await;
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].status, OrderStatus::Filled);
    assert_eq!(
        orders[0].average_fill_price.unwrap().as_decimal(),
        decimal("99.5")
    );
    assert_eq!(orders[0].updated_at, timestamp("2026-07-14T01:01:00Z"));
}

#[tokio::test]
async fn cancelling_a_resting_order_is_certain_and_idempotent() {
    let paper = PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap();
    let symbol = Symbol::new("BTC-USDT").unwrap();
    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:00:00Z"))
        .await
        .unwrap();
    let mut intent = OrderIntent::limit(
        "paper",
        symbol,
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("1")).unwrap(),
        Price::new(decimal("90")).unwrap(),
    );
    intent.client_order_id = Uuid::parse_str("8f14e45f-ea3f-4bb7-9f69-44973ea5a52f").unwrap();

    let placed = paper.execute(TradingCommand::Submit(intent)).await.unwrap();
    let TradingReceipt::Submitted { order, .. } = placed else {
        panic!("limit placement must return a submission receipt");
    };
    let first = paper
        .execute(TradingCommand::Cancel {
            order_id: order.id.clone(),
        })
        .await
        .unwrap();
    let repeated = paper
        .execute(TradingCommand::Cancel { order_id: order.id })
        .await
        .unwrap();

    let TradingReceipt::Cancelled {
        orders,
        disposition,
    } = first
    else {
        panic!("cancellation must return a cancellation receipt");
    };
    assert_eq!(disposition, CancellationDisposition::Cancelled);
    assert_eq!(orders[0].status, OrderStatus::Cancelled);
    let TradingReceipt::Cancelled { disposition, .. } = repeated else {
        panic!("repeated cancellation must return a cancellation receipt");
    };
    assert_eq!(disposition, CancellationDisposition::AlreadyCancelled);
}

#[tokio::test]
async fn live_execution_is_explicitly_unsupported_instead_of_faking_success() {
    let live = UnsupportedLiveExchange::new("binance").unwrap();

    let error = live
        .execute(TradingCommand::Submit(market_buy()))
        .await
        .unwrap_err();

    assert!(matches!(error, ExchangeError::Unsupported { .. }));
    assert_eq!(live.status().await.unwrap().mode, ExchangeMode::Live);
}

#[tokio::test]
async fn lagging_market_subscriber_gets_an_explicit_error() {
    let paper = PaperExchange::new("paper", NonZeroUsize::new(1).unwrap()).unwrap();
    let mut subscription = paper
        .subscribe(MarketSubscription::all_snapshots(None))
        .await
        .unwrap();

    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:00:00Z"))
        .await
        .unwrap();
    let latest = snapshot("100", "102", "2026-07-14T01:01:00Z");
    paper.publish_snapshot(latest.clone()).await.unwrap();

    assert!(matches!(
        subscription.recv().await.unwrap_err(),
        ExchangeError::SubscriptionLagged { skipped: 1 }
    ));
    assert_eq!(subscription.recv().await.unwrap(), latest);
}

#[tokio::test]
async fn concurrent_reduce_only_orders_cannot_reverse_a_position() {
    let paper = PaperExchange::new("paper", NonZeroUsize::new(8).unwrap()).unwrap();
    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:00:00Z"))
        .await
        .unwrap();

    let mut opening = market_buy();
    opening.quantity = Quantity::new(decimal("1")).unwrap();
    paper
        .execute(TradingCommand::Submit(opening))
        .await
        .unwrap();

    for client_order_id in [
        "d9428888-122b-11e1-b85c-61cd3cbb3210",
        "d9428888-122b-11e1-b85c-61cd3cbb3211",
    ] {
        let mut close = OrderIntent::limit(
            "paper",
            Symbol::new("BTC-USDT").unwrap(),
            MarketType::Perpetual,
            Side::Sell,
            Quantity::new(decimal("1")).unwrap(),
            Price::new(decimal("110")).unwrap(),
        );
        close.client_order_id = Uuid::parse_str(client_order_id).unwrap();
        close.reduce_only = true;
        paper.execute(TradingCommand::Submit(close)).await.unwrap();
    }

    paper
        .publish_snapshot(snapshot("110", "111", "2026-07-14T01:01:00Z"))
        .await
        .unwrap();

    let orders = paper.orders().await;
    assert_eq!(orders[1].status, OrderStatus::Filled);
    assert_eq!(orders[2].status, OrderStatus::Cancelled);
    let positions = paper.positions().await;
    assert_eq!(positions[0].side, PositionSide::Flat);
    assert_eq!(positions[0].quantity.as_decimal(), Decimal::ZERO);
}
