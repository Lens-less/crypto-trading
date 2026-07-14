use std::{
    num::NonZeroUsize,
    str::FromStr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use crypto_trading_domain::{
    MarketSnapshot, MarketType, Money, OrderIntent, OrderStatus, PositionSide, Price, Quantity,
    Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    CancellationDisposition, ExchangeError, ExchangeHandle, ExchangeMode, InstrumentRuleCatalog,
    InstrumentRules, InstrumentRulesMode, MarketSubscription, PaperExchange, PaperLedgerLimits,
    ReconcileScope, SubmissionDisposition, TradingCommand, TradingReceipt, UnsupportedLiveExchange,
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

#[derive(Clone)]
struct TestClock(Arc<StdMutex<DateTime<Utc>>>);

impl TestClock {
    fn new(at: &str) -> Self {
        Self(Arc::new(StdMutex::new(timestamp(at))))
    }

    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("test clock lock must not be poisoned")
    }

    fn set(&self, at: &str) {
        *self.0.lock().expect("test clock lock must not be poisoned") = timestamp(at);
    }
}

fn paper_at(at: &str) -> PaperExchange {
    let clock = TestClock::new(at);
    PaperExchange::with_clock_and_freshness(
        "paper",
        NonZeroUsize::new(8).unwrap(),
        move || clock.now(),
        TimeDelta::days(1),
        TimeDelta::days(1),
    )
    .unwrap()
}

fn instrument_rules(exchange: &str, symbol: &str, market_type: MarketType) -> InstrumentRules {
    InstrumentRules::new(
        exchange,
        Symbol::new(symbol).unwrap(),
        market_type,
        Price::new(decimal("0.5")).unwrap(),
        Quantity::new(decimal("0.1")).unwrap(),
        Quantity::new(decimal("0.2")).unwrap(),
        Money::new(decimal("10")),
    )
    .unwrap()
}

fn paper_with_rules_at(
    at: &str,
    mode: InstrumentRulesMode,
    rules: Vec<InstrumentRules>,
) -> PaperExchange {
    let clock = TestClock::new(at);
    PaperExchange::with_clock_freshness_and_rules(
        "paper",
        NonZeroUsize::new(8).unwrap(),
        move || clock.now(),
        TimeDelta::days(1),
        TimeDelta::days(1),
        mode,
        InstrumentRuleCatalog::new(rules).unwrap(),
    )
    .unwrap()
}

fn paper_with_limits_at(at: &str, ledger_limits: PaperLedgerLimits) -> PaperExchange {
    let clock = TestClock::new(at);
    PaperExchange::with_clock_freshness_rules_and_limits(
        "paper",
        NonZeroUsize::new(8).unwrap(),
        move || clock.now(),
        TimeDelta::days(1),
        TimeDelta::days(1),
        InstrumentRulesMode::Permissive,
        InstrumentRuleCatalog::default(),
        ledger_limits,
    )
    .unwrap()
}

fn snapshot_without_depth(bid: &str, ask: &str, at: &str) -> MarketSnapshot {
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

fn snapshot(bid: &str, ask: &str, at: &str) -> MarketSnapshot {
    let mut snapshot = snapshot_without_depth(bid, ask, at);
    snapshot.bid_quantity = Some(Quantity::new(decimal("1000")).unwrap());
    snapshot.ask_quantity = Some(Quantity::new(decimal("1000")).unwrap());
    snapshot
}

fn snapshot_with_depth(
    bid: &str,
    bid_quantity: &str,
    ask: &str,
    ask_quantity: &str,
    at: &str,
) -> MarketSnapshot {
    let mut snapshot = snapshot(bid, ask, at);
    snapshot.bid_quantity = Some(Quantity::new(decimal(bid_quantity)).unwrap());
    snapshot.ask_quantity = Some(Quantity::new(decimal(ask_quantity)).unwrap());
    snapshot
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
    let paper = paper_at("2026-07-14T01:02:03Z");
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
async fn missing_top_of_book_depth_never_implies_infinite_liquidity() {
    let paper = paper_at("2026-07-14T01:02:03Z");
    paper
        .publish_snapshot(snapshot_without_depth(
            "100.75",
            "101.25",
            "2026-07-14T01:02:03Z",
        ))
        .await
        .unwrap();

    let receipt = paper
        .execute(TradingCommand::Submit(market_buy()))
        .await
        .unwrap();
    let TradingReceipt::Submitted { order, disposition } = receipt else {
        panic!("market submission must return a submission receipt");
    };
    assert_eq!(disposition, SubmissionDisposition::Cancelled);
    assert_eq!(order.status, OrderStatus::Cancelled);
    assert_eq!(order.filled_quantity.as_decimal(), Decimal::ZERO);
    assert!(paper.positions().await.is_empty());
}

#[tokio::test]
async fn resting_limit_fills_from_a_later_snapshot_and_subscription_observes_it() {
    let paper = paper_at("2026-07-14T01:01:00Z");
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
async fn snapshot_crossing_indexes_multiple_positions_and_updates_new_entries() {
    let paper = paper_at("2026-07-14T01:03:00Z");
    for (symbol, client_order_id) in [
        ("ETH-USDT", "f10c8001-0b7f-4b38-a1b4-0df95ad38401"),
        ("SOL-USDT", "f10c8002-0b7f-4b38-a1b4-0df95ad38402"),
    ] {
        let symbol = Symbol::new(symbol).unwrap();
        let mut book = MarketSnapshot::new(
            "paper",
            symbol.clone(),
            MarketType::Perpetual,
            Price::new(decimal("49")).unwrap(),
            Price::new(decimal("51")).unwrap(),
            timestamp("2026-07-14T01:00:00Z"),
        )
        .unwrap();
        book.bid_quantity = Some(Quantity::new(decimal("10")).unwrap());
        book.ask_quantity = Some(Quantity::new(decimal("10")).unwrap());
        paper.publish_snapshot(book).await.unwrap();

        let mut buy = OrderIntent::market(
            "paper",
            symbol,
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(Decimal::ONE).unwrap(),
        );
        buy.client_order_id = Uuid::parse_str(client_order_id).unwrap();
        paper.execute(TradingCommand::Submit(buy)).await.unwrap();
    }

    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:01:00Z"))
        .await
        .unwrap();
    for (side, client_order_id) in [
        (Side::Buy, "f10c8003-0b7f-4b38-a1b4-0df95ad38403"),
        (Side::Sell, "f10c8004-0b7f-4b38-a1b4-0df95ad38404"),
    ] {
        let mut intent = OrderIntent::limit(
            "paper",
            Symbol::new("BTC-USDT").unwrap(),
            MarketType::Perpetual,
            side,
            Quantity::new(Decimal::ONE).unwrap(),
            Price::new(decimal("100")).unwrap(),
        );
        intent.client_order_id = Uuid::parse_str(client_order_id).unwrap();
        let receipt = paper.execute(TradingCommand::Submit(intent)).await.unwrap();
        assert_eq!(
            receipt.submission_disposition(),
            Some(SubmissionDisposition::Open)
        );
    }

    paper
        .publish_snapshot(snapshot_with_depth(
            "100",
            "2",
            "100",
            "2",
            "2026-07-14T01:02:00Z",
        ))
        .await
        .unwrap();

    let orders = paper.orders().await;
    assert_eq!(orders.len(), 4);
    assert!(
        orders
            .iter()
            .all(|order| order.status == OrderStatus::Filled)
    );
    let positions = paper.positions().await;
    assert_eq!(positions.len(), 3);
    let btc_positions = positions
        .iter()
        .filter(|position| position.symbol == Symbol::new("BTC-USDT").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(btc_positions.len(), 1);
    assert_eq!(btc_positions[0].side, PositionSide::Flat);
    assert_eq!(btc_positions[0].quantity.as_decimal(), Decimal::ZERO);
}

#[tokio::test]
async fn cancelling_a_resting_order_is_certain_and_idempotent() {
    let paper = paper_at("2026-07-14T01:00:00Z");
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
    let clock = TestClock::new("2026-07-14T01:01:00Z");
    let paper = PaperExchange::with_clock_and_freshness(
        "paper",
        NonZeroUsize::new(1).unwrap(),
        move || clock.now(),
        TimeDelta::days(1),
        TimeDelta::days(1),
    )
    .unwrap();
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
    let paper = paper_at("2026-07-14T01:01:00Z");
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

#[tokio::test]
async fn overflowing_immediate_fill_returns_an_error_without_partial_ledger_state() {
    let paper = paper_at("2026-07-14T01:00:00Z");
    let maximum = Decimal::MAX.to_string();
    paper
        .publish_snapshot(snapshot_with_depth(
            "99",
            &maximum,
            "101",
            &maximum,
            "2026-07-14T01:00:00Z",
        ))
        .await
        .unwrap();

    let mut first = market_buy();
    first.quantity = Quantity::new(Decimal::MAX).unwrap();
    paper.execute(TradingCommand::Submit(first)).await.unwrap();
    paper
        .publish_snapshot(snapshot_with_depth(
            "99",
            &maximum,
            "101",
            &maximum,
            "2026-07-14T01:00:01Z",
        ))
        .await
        .unwrap();

    let mut overflowing = market_buy();
    overflowing.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10319").unwrap();
    overflowing.quantity = Quantity::new(Decimal::MAX).unwrap();
    let task = tokio::spawn({
        let paper = paper.clone();
        async move { paper.execute(TradingCommand::Submit(overflowing)).await }
    });

    let result = task.await.expect("paper arithmetic must not panic");
    assert!(matches!(
        result,
        Err(ExchangeError::InvariantViolation { .. })
    ));
    let orders = paper.orders().await;
    assert_eq!(orders.len(), 1, "failed order must not enter the ledger");
    assert_eq!(orders[0].status, OrderStatus::Filled);
    let positions = paper.positions().await;
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity.as_decimal(), Decimal::MAX);
}

#[tokio::test]
async fn overflowing_delayed_fills_roll_back_the_entire_snapshot_transition() {
    let paper = paper_at("2026-07-14T01:01:00Z");
    let maximum = Decimal::MAX.to_string();
    let initial = snapshot_with_depth("99", &maximum, "101", &maximum, "2026-07-14T01:00:00Z");
    paper.publish_snapshot(initial).await.unwrap();

    let mut existing = market_buy();
    existing.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10320").unwrap();
    existing.quantity = Quantity::new(Decimal::MAX).unwrap();
    paper
        .execute(TradingCommand::Submit(existing))
        .await
        .unwrap();
    let mut resting = OrderIntent::limit(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
        Price::new(decimal("100")).unwrap(),
    );
    resting.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10321").unwrap();
    paper
        .execute(TradingCommand::Submit(resting))
        .await
        .unwrap();
    let before_transition = paper
        .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
        .await;

    let task = tokio::spawn({
        let paper = paper.clone();
        async move {
            paper
                .publish_snapshot(snapshot_with_depth(
                    "98",
                    "1",
                    "99",
                    "1",
                    "2026-07-14T01:01:00Z",
                ))
                .await
        }
    });
    let result = task.await.expect("paper arithmetic must not panic");

    assert!(matches!(
        result,
        Err(ExchangeError::InvariantViolation { .. })
    ));
    let orders = paper.orders().await;
    assert_eq!(orders[0].status, OrderStatus::Filled);
    assert_eq!(orders[1].status, OrderStatus::Open);
    assert_eq!(
        paper.positions().await[0].quantity.as_decimal(),
        Decimal::MAX
    );
    assert_eq!(
        paper
            .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
            .await,
        before_transition
    );
}

#[tokio::test]
async fn default_freshness_rejects_stale_and_future_snapshots() {
    let clock = TestClock::new("2026-07-14T02:00:00Z");
    let paper = PaperExchange::with_clock("paper", NonZeroUsize::new(8).unwrap(), {
        let clock = clock.clone();
        move || clock.now()
    })
    .unwrap();

    let stale = paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:59:29Z"))
        .await
        .unwrap_err();
    assert!(matches!(stale, ExchangeError::InvalidRequest { .. }));
    let future = paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T02:00:02Z"))
        .await
        .unwrap_err();
    assert!(matches!(future, ExchangeError::InvalidRequest { .. }));
    assert!(
        paper
            .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
            .await
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn freshness_is_checked_after_the_paper_state_lock_is_acquired() {
    let clock = TestClock::new("2026-07-14T02:00:00Z");
    let calls = Arc::new(AtomicUsize::new(0));
    let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
    let (release_sender, release_receiver) = mpsc::sync_channel(1);
    let entered_sender = Arc::new(StdMutex::new(Some(entered_sender)));
    let release_receiver = Arc::new(StdMutex::new(release_receiver));
    let paper = PaperExchange::with_clock("paper", NonZeroUsize::new(8).unwrap(), {
        let clock = clock.clone();
        let calls = Arc::clone(&calls);
        let entered_sender = Arc::clone(&entered_sender);
        let release_receiver = Arc::clone(&release_receiver);
        move || {
            if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                entered_sender
                    .lock()
                    .expect("entered sender lock must not be poisoned")
                    .take()
                    .expect("clock must only block once")
                    .send(())
                    .expect("test must still be waiting for the clock");
                release_receiver
                    .lock()
                    .expect("release receiver lock must not be poisoned")
                    .recv_timeout(Duration::from_secs(2))
                    .expect("test must release the clock");
            }
            clock.now()
        }
    })
    .unwrap();

    let publish = tokio::spawn({
        let paper = paper.clone();
        async move {
            paper
                .publish_snapshot(snapshot("99", "101", "2026-07-14T02:00:00Z"))
                .await
        }
    });
    entered_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("publish must reach the injected clock");
    clock.set("2026-07-14T02:01:00Z");

    let reconcile_waited_for_the_lock = tokio::time::timeout(
        Duration::from_millis(50),
        paper.reconcile(ReconcileScope::All),
    )
    .await
    .is_err();
    release_sender
        .send(())
        .expect("blocked clock must still be waiting");

    assert!(
        reconcile_waited_for_the_lock,
        "the state lock must be held before the execution clock is sampled"
    );
    assert!(matches!(
        publish.await.unwrap(),
        Err(ExchangeError::InvalidRequest { .. })
    ));
    assert!(paper.orders().await.is_empty());
}

#[tokio::test]
async fn custom_clock_rollback_never_moves_paper_state_or_order_time_backwards() {
    let clock = TestClock::new("2026-07-14T02:00:00Z");
    let paper = PaperExchange::with_clock_and_freshness(
        "paper",
        NonZeroUsize::new(8).unwrap(),
        {
            let clock = clock.clone();
            move || clock.now()
        },
        TimeDelta::days(1),
        TimeDelta::days(1),
    )
    .unwrap();
    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T02:00:00Z"))
        .await
        .unwrap();

    clock.set("2026-07-14T02:01:00Z");
    let mut resting = OrderIntent::limit(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
        Price::new(decimal("90")).unwrap(),
    );
    resting.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10355").unwrap();
    let TradingReceipt::Submitted { order, .. } = paper
        .execute(TradingCommand::Submit(resting))
        .await
        .unwrap()
    else {
        panic!("limit placement must return an order")
    };
    assert_eq!(order.updated_at, timestamp("2026-07-14T02:01:00Z"));

    clock.set("2026-07-14T01:59:00Z");
    let TradingReceipt::Cancelled { orders, .. } = paper
        .execute(TradingCommand::Cancel { order_id: order.id })
        .await
        .unwrap()
    else {
        panic!("cancellation must return an order")
    };
    assert_eq!(orders[0].updated_at, timestamp("2026-07-14T02:01:00Z"));

    clock.set("2026-07-14T02:02:00Z");
    assert_eq!(
        paper
            .reconcile(ReconcileScope::All)
            .await
            .unwrap()
            .observed_at,
        timestamp("2026-07-14T02:02:00Z")
    );
    clock.set("2026-07-14T01:58:00Z");
    assert_eq!(
        paper
            .reconcile(ReconcileScope::All)
            .await
            .unwrap()
            .observed_at,
        timestamp("2026-07-14T02:02:00Z")
    );
}

#[tokio::test]
async fn duplicate_snapshot_timestamp_cannot_replenish_consumed_depth() {
    let paper = paper_at("2026-07-14T03:00:00Z");
    let authoritative = snapshot_with_depth("99", "1", "101", "1", "2026-07-14T03:00:00Z");
    paper.publish_snapshot(authoritative.clone()).await.unwrap();
    let mut first = market_buy();
    first.quantity = Quantity::new(Decimal::ONE).unwrap();
    paper.execute(TradingCommand::Submit(first)).await.unwrap();

    let error = paper.publish_snapshot(authoritative).await.unwrap_err();
    assert!(matches!(error, ExchangeError::InvalidRequest { .. }));
    assert!(error.to_string().contains("duplicate"));
    assert_eq!(paper.orders().await.len(), 1);
    assert_eq!(
        paper.positions().await[0].quantity.as_decimal(),
        Decimal::ONE
    );
    assert_eq!(
        paper
            .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
            .await
            .unwrap()
            .ask_quantity
            .unwrap()
            .as_decimal(),
        Decimal::ZERO
    );
}

#[tokio::test]
async fn command_times_use_the_injected_clock_and_aged_quotes_cannot_drive_orders() {
    let clock = TestClock::new("2026-07-14T02:00:00Z");
    let paper = PaperExchange::with_clock("paper", NonZeroUsize::new(8).unwrap(), {
        let clock = clock.clone();
        move || clock.now()
    })
    .unwrap();
    paper
        .publish_snapshot(snapshot("99", "101", "2026-07-14T01:59:50Z"))
        .await
        .unwrap();

    let submitted = paper
        .execute(TradingCommand::Submit(market_buy()))
        .await
        .unwrap();
    let TradingReceipt::Submitted { order, .. } = submitted else {
        panic!("market submission must return an order")
    };
    assert_eq!(order.created_at, timestamp("2026-07-14T02:00:00Z"));
    assert_eq!(order.updated_at, timestamp("2026-07-14T02:00:00Z"));

    clock.set("2026-07-14T02:00:21Z");
    let mut aged = market_buy();
    aged.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10322").unwrap();
    let error = paper
        .execute(TradingCommand::Submit(aged))
        .await
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Rejected { .. }));
    assert_eq!(paper.orders().await.len(), 1);
}

#[tokio::test]
async fn competing_market_orders_consume_top_of_book_depth_once() {
    let paper = paper_at("2026-07-14T03:00:00Z");
    paper
        .publish_snapshot(snapshot_with_depth(
            "99",
            "10",
            "101",
            "3",
            "2026-07-14T03:00:00Z",
        ))
        .await
        .unwrap();

    let first = paper
        .execute(TradingCommand::Submit(market_buy()))
        .await
        .unwrap();
    assert_eq!(
        first.submission_disposition(),
        Some(SubmissionDisposition::Filled)
    );

    let mut second = market_buy();
    second.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10323").unwrap();
    let second = paper.execute(TradingCommand::Submit(second)).await.unwrap();
    let TradingReceipt::Submitted { order, disposition } = second else {
        panic!("market submission must return an order")
    };
    assert_eq!(disposition, SubmissionDisposition::Cancelled);
    assert_eq!(order.status, OrderStatus::Cancelled);
    assert_eq!(order.filled_quantity.as_decimal(), Decimal::ONE);
    assert_eq!(
        paper
            .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
            .await
            .unwrap()
            .ask_quantity
            .unwrap()
            .as_decimal(),
        Decimal::ZERO
    );
    assert_eq!(
        paper.positions().await[0].quantity.as_decimal(),
        decimal("3")
    );
}

#[tokio::test]
async fn gtc_limit_accumulates_partial_fills_across_snapshots() {
    let paper = paper_at("2026-07-14T03:02:00Z");
    paper
        .publish_snapshot(snapshot_with_depth(
            "99",
            "10",
            "101",
            "10",
            "2026-07-14T03:00:00Z",
        ))
        .await
        .unwrap();
    let mut intent = OrderIntent::limit(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("3")).unwrap(),
        Price::new(decimal("100")).unwrap(),
    );
    intent.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10324").unwrap();
    paper.execute(TradingCommand::Submit(intent)).await.unwrap();

    paper
        .publish_snapshot(snapshot_with_depth(
            "98",
            "10",
            "99",
            "1",
            "2026-07-14T03:01:00Z",
        ))
        .await
        .unwrap();
    let partial = paper.orders().await.remove(0);
    assert_eq!(partial.status, OrderStatus::PartiallyFilled);
    assert_eq!(partial.filled_quantity.as_decimal(), Decimal::ONE);
    assert_eq!(
        paper.positions().await[0].quantity.as_decimal(),
        Decimal::ONE
    );

    paper
        .publish_snapshot(snapshot_with_depth(
            "98",
            "10",
            "99",
            "2",
            "2026-07-14T03:02:00Z",
        ))
        .await
        .unwrap();
    let filled = paper.orders().await.remove(0);
    assert_eq!(filled.status, OrderStatus::Filled);
    assert_eq!(filled.filled_quantity.as_decimal(), decimal("3"));
    assert_eq!(
        paper.positions().await[0].quantity.as_decimal(),
        decimal("3")
    );
}

#[tokio::test]
async fn ioc_partially_fills_then_cancels_while_fok_is_all_or_nothing() {
    let paper = paper_at("2026-07-14T04:00:00Z");
    paper
        .publish_snapshot(snapshot_with_depth(
            "99",
            "10",
            "101",
            "1",
            "2026-07-14T04:00:00Z",
        ))
        .await
        .unwrap();
    let mut ioc = OrderIntent::limit(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("2")).unwrap(),
        Price::new(decimal("102")).unwrap(),
    );
    ioc.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10325").unwrap();
    ioc.time_in_force = TimeInForce::Ioc;
    let TradingReceipt::Submitted { order, disposition } =
        paper.execute(TradingCommand::Submit(ioc)).await.unwrap()
    else {
        panic!("IOC submission must return an order")
    };
    assert_eq!(disposition, SubmissionDisposition::Cancelled);
    assert_eq!(order.status, OrderStatus::Cancelled);
    assert_eq!(order.filled_quantity.as_decimal(), Decimal::ONE);

    paper
        .publish_snapshot(snapshot_with_depth(
            "99",
            "10",
            "101",
            "1",
            "2026-07-14T04:00:01Z",
        ))
        .await
        .unwrap();
    let mut fok = OrderIntent::limit(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("2")).unwrap(),
        Price::new(decimal("102")).unwrap(),
    );
    fok.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10326").unwrap();
    fok.time_in_force = TimeInForce::Fok;
    let TradingReceipt::Submitted { order, disposition } =
        paper.execute(TradingCommand::Submit(fok)).await.unwrap()
    else {
        panic!("FOK submission must return an order")
    };
    assert_eq!(disposition, SubmissionDisposition::Cancelled);
    assert_eq!(order.status, OrderStatus::Cancelled);
    assert_eq!(order.filled_quantity.as_decimal(), Decimal::ZERO);
    assert_eq!(
        paper.positions().await[0].quantity.as_decimal(),
        Decimal::ONE
    );
}

#[tokio::test]
async fn spot_sell_without_inventory_is_rejected() {
    let paper = paper_at("2026-07-14T05:00:00Z");
    let mut spot = MarketSnapshot::new(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Spot,
        Price::new(decimal("99")).unwrap(),
        Price::new(decimal("101")).unwrap(),
        timestamp("2026-07-14T05:00:00Z"),
    )
    .unwrap();
    spot.bid_quantity = Some(Quantity::new(decimal("10")).unwrap());
    spot.ask_quantity = Some(Quantity::new(decimal("10")).unwrap());
    paper.publish_snapshot(spot).await.unwrap();

    let intent = OrderIntent::market(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Spot,
        Side::Sell,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    let error = paper
        .execute(TradingCommand::Submit(intent))
        .await
        .unwrap_err();
    assert!(matches!(error, ExchangeError::Rejected { .. }));
    assert!(paper.orders().await.is_empty());
    assert!(paper.positions().await.is_empty());
}

#[test]
fn oversized_event_capacity_is_rejected_before_broadcast_allocation() {
    let result = PaperExchange::new("paper", NonZeroUsize::new(usize::MAX).unwrap());

    assert!(matches!(
        result,
        Err(ExchangeError::ResourceLimit {
            resource: "paper event capacity",
            ..
        })
    ));
}

#[tokio::test]
async fn default_paper_mode_is_explicitly_permissive_when_no_rules_exist() {
    let paper = paper_at("2026-07-14T06:00:00Z");
    assert_eq!(
        paper.instrument_rules_status().mode,
        InstrumentRulesMode::Permissive
    );
    assert_eq!(paper.instrument_rules_status().rule_count, 0);

    let intent = OrderIntent::limit(
        "paper",
        Symbol::new("BTC-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(decimal("0.253")).unwrap(),
        Price::new(decimal("100.23")).unwrap(),
    );
    let receipt = paper.execute(TradingCommand::Submit(intent)).await.unwrap();
    assert_eq!(
        receipt.submission_disposition(),
        Some(SubmissionDisposition::Open)
    );
}

#[tokio::test]
async fn strict_rules_reject_off_tick_off_step_and_minimum_violations_before_mutation() {
    let paper = paper_with_rules_at(
        "2026-07-14T06:00:00Z",
        InstrumentRulesMode::Strict,
        vec![instrument_rules("paper", "BTC-USDT", MarketType::Perpetual)],
    );
    let initial = snapshot_with_depth("99", "10", "101", "10", "2026-07-14T06:00:00Z");
    paper.publish_snapshot(initial.clone()).await.unwrap();

    for (client_order_id, quantity, price, expected) in [
        (
            "4d36e96e-e325-11ce-bfc1-08002be10330",
            "1",
            "100.25",
            "tick",
        ),
        (
            "4d36e96e-e325-11ce-bfc1-08002be10331",
            "0.25",
            "100",
            "step",
        ),
        (
            "4d36e96e-e325-11ce-bfc1-08002be10332",
            "0.1",
            "100",
            "minimum",
        ),
        (
            "4d36e96e-e325-11ce-bfc1-08002be10333",
            "0.2",
            "20",
            "notional",
        ),
    ] {
        let mut intent = OrderIntent::limit(
            "paper",
            Symbol::new("BTC-USDT").unwrap(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(decimal(quantity)).unwrap(),
            Price::new(decimal(price)).unwrap(),
        );
        intent.client_order_id = Uuid::parse_str(client_order_id).unwrap();
        let error = paper
            .execute(TradingCommand::Submit(intent))
            .await
            .unwrap_err();
        assert!(matches!(error, ExchangeError::Rejected { .. }));
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    assert!(paper.orders().await.is_empty());
    assert!(paper.positions().await.is_empty());
    assert_eq!(
        paper
            .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
            .await,
        Some(initial)
    );
}

#[tokio::test]
async fn strict_catalog_match_includes_exchange_symbol_and_market_type() {
    let paper = paper_with_rules_at(
        "2026-07-14T06:00:00Z",
        InstrumentRulesMode::Strict,
        vec![
            instrument_rules("other", "BTC-USDT", MarketType::Perpetual),
            instrument_rules("paper", "ETH-USDT", MarketType::Perpetual),
            instrument_rules("paper", "BTC-USDT", MarketType::Spot),
        ],
    );
    assert_eq!(paper.instrument_rules_status().rule_count, 3);

    let error = paper
        .execute(TradingCommand::Submit(OrderIntent::limit(
            "paper",
            Symbol::new("BTC-USDT").unwrap(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(Decimal::ONE).unwrap(),
            Price::new(decimal("100")).unwrap(),
        )))
        .await
        .unwrap_err();

    assert!(matches!(error, ExchangeError::Rejected { .. }));
    assert!(error.to_string().contains("missing"));
    assert!(paper.orders().await.is_empty());
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn paper_order_snapshot_and_position_ledgers_fail_fast_at_configured_lower_caps() {
    let order_limited = paper_with_limits_at(
        "2026-07-14T07:00:00Z",
        PaperLedgerLimits::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
        .unwrap(),
    );
    for (client_order_id, symbol) in [
        ("4d36e96e-e325-11ce-bfc1-08002be10340", "BTC-USDT"),
        ("4d36e96e-e325-11ce-bfc1-08002be10341", "ETH-USDT"),
    ] {
        let mut intent = OrderIntent::limit(
            "paper",
            Symbol::new(symbol).unwrap(),
            MarketType::Perpetual,
            Side::Buy,
            Quantity::new(Decimal::ONE).unwrap(),
            Price::new(decimal("90")).unwrap(),
        );
        intent.client_order_id = Uuid::parse_str(client_order_id).unwrap();
        let result = order_limited.execute(TradingCommand::Submit(intent)).await;
        if symbol == "BTC-USDT" {
            result.unwrap();
        } else {
            assert!(matches!(
                result,
                Err(ExchangeError::ResourceLimit {
                    resource: "paper order ledger",
                    ..
                })
            ));
        }
    }
    assert_eq!(order_limited.orders().await.len(), 1);

    let snapshot_limited = paper_with_limits_at(
        "2026-07-14T07:00:00Z",
        PaperLedgerLimits::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
        .unwrap(),
    );
    let btc = snapshot("99", "101", "2026-07-14T07:00:00Z");
    snapshot_limited
        .publish_snapshot(btc.clone())
        .await
        .unwrap();
    let eth = MarketSnapshot::new(
        "paper",
        Symbol::new("ETH-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("49")).unwrap(),
        Price::new(decimal("51")).unwrap(),
        timestamp("2026-07-14T07:00:00Z"),
    )
    .unwrap();
    assert!(matches!(
        snapshot_limited.publish_snapshot(eth).await,
        Err(ExchangeError::ResourceLimit {
            resource: "paper snapshot ledger",
            ..
        })
    ));
    assert_eq!(
        snapshot_limited
            .snapshot(&Symbol::new("BTC-USDT").unwrap(), MarketType::Perpetual)
            .await,
        Some(btc)
    );

    let position_limited = paper_with_limits_at(
        "2026-07-14T07:00:00Z",
        PaperLedgerLimits::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap(),
    );
    position_limited
        .publish_snapshot(snapshot("99", "101", "2026-07-14T07:00:00Z"))
        .await
        .unwrap();
    let mut eth = MarketSnapshot::new(
        "paper",
        Symbol::new("ETH-USDT").unwrap(),
        MarketType::Perpetual,
        Price::new(decimal("49")).unwrap(),
        Price::new(decimal("51")).unwrap(),
        timestamp("2026-07-14T07:00:00Z"),
    )
    .unwrap();
    eth.bid_quantity = Some(Quantity::new(decimal("10")).unwrap());
    eth.ask_quantity = Some(Quantity::new(decimal("10")).unwrap());
    position_limited.publish_snapshot(eth).await.unwrap();
    let mut btc_buy = market_buy();
    btc_buy.quantity = Quantity::new(Decimal::ONE).unwrap();
    position_limited
        .execute(TradingCommand::Submit(btc_buy))
        .await
        .unwrap();
    let mut eth_buy = OrderIntent::market(
        "paper",
        Symbol::new("ETH-USDT").unwrap(),
        MarketType::Perpetual,
        Side::Buy,
        Quantity::new(Decimal::ONE).unwrap(),
    );
    eth_buy.client_order_id = Uuid::parse_str("4d36e96e-e325-11ce-bfc1-08002be10342").unwrap();
    assert!(matches!(
        position_limited
            .execute(TradingCommand::Submit(eth_buy))
            .await,
        Err(ExchangeError::ResourceLimit {
            resource: "paper position ledger",
            ..
        })
    ));
    assert_eq!(position_limited.orders().await.len(), 1);
    assert_eq!(position_limited.positions().await.len(), 1);
}
