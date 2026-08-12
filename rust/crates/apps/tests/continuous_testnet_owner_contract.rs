use std::{
    collections::VecDeque,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use crypto_trading_cli::{
    continuous_testnet::{
        ContinuousTestnetOwner, ContinuousTestnetOwnerError, ContinuousTestnetOwnerPhase,
        ContinuousTestnetUserDataOutcome,
    },
    testnet_lifecycle::{
        TestnetLifecycleConfig, TestnetLifecycleObservation, TestnetLifecycleVenue,
        TestnetLifecycleVenueFuture, run_testnet_lifecycle,
    },
};
use crypto_trading_domain::{
    MarketType, Order, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol, TimeInForce,
};
use crypto_trading_exchange::{
    BinanceAccountUpdateEvent, BinanceUserDataBalance, BinanceUserDataEvent, ExchangeAvailability,
    ExchangeError, ExchangeHandle, ExchangeMode, ExchangeOperation, ExchangeOperationKey,
    ExchangeStatus, MarketSubscription, ReconcileReceipt, ReconcileScope, SubscriptionReceipt,
    TradingCommand, TradingReceipt,
};
use crypto_trading_runtime::{
    BinanceUserDataApply, BinanceUserDataStreamItem, JsonlHistory, StreamEnvelope,
};
use rust_decimal::Decimal;
use uuid::Uuid;

#[tokio::test]
async fn restarted_continuous_owner_reconciles_then_recovers_query_first() {
    let config = lifecycle_config();
    let history = history();
    let interrupted = FixtureVenue::new(
        vec![Err(ambiguous_submit(&config))],
        vec![Err(ExchangeError::unavailable("fixture query unavailable"))],
        Vec::new(),
    );
    let first = run_testnet_lifecycle(&config, &interrupted, &history)
        .await
        .unwrap_err();
    assert!(first.to_string().contains("outcome"));

    let venue = Arc::new(FixtureVenue::new(
        Vec::new(),
        vec![
            Ok(order(&config, OrderStatus::Open)),
            Ok(order(&config, OrderStatus::Cancelled)),
        ],
        vec![Ok(order(&config, OrderStatus::Cancelled))],
    ));
    let mut owner = ContinuousTestnetOwner::start_recovery_only(
        "testnet-owner-recovery",
        config,
        Arc::clone(&venue),
        history.clone(),
    )
    .await
    .unwrap();

    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::AwaitingUserStream
    );
    assert_eq!(
        venue.calls(),
        vec!["query", "cancel", "query", "reconcile", "reconcile"]
    );
    assert_eq!(
        owner
            .ingest_user_data_item(BinanceUserDataStreamItem::Subscribed {
                subscription_id: 41,
                observed_at: observed_at(2),
            })
            .await
            .unwrap(),
        ContinuousTestnetUserDataOutcome::Subscribed
    );
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::ReadyUnarmed
    );
    let report = owner.run_lifecycle().await.unwrap();
    assert!(report.recovered);
    assert_eq!(
        venue.calls(),
        vec!["query", "cancel", "query", "reconcile", "reconcile"]
    );
    assert!(!venue.calls().contains(&"submit"));

    owner.engage_kill_switch().await.unwrap();
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::KilledClean
    );
    assert!(owner.status().kill_switch_latched);
    assert!(owner.run_lifecycle().await.is_err());

    let body = std::fs::read_to_string(history.path()).unwrap();
    assert!(body.contains("continuous_testnet_bootstrap_planned"));
    assert!(body.contains("continuous_testnet_campaign_recovery_verified"));
    assert!(body.contains("continuous_testnet_kill_switch_engaged"));
    assert!(body.contains("continuous_testnet_killed_clean"));
    cleanup(history);
}

#[tokio::test]
async fn read_only_owner_projects_streams_without_claiming_campaign_recovery() {
    let history = history();
    let venue = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));
    let mut owner = ContinuousTestnetOwner::start_read_only(
        "testnet-owner-read-only",
        Arc::clone(&venue),
        history.clone(),
    )
    .await
    .unwrap();

    assert_eq!(owner.status().campaign_id, None);
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::AwaitingUserStream
    );
    subscribe(&mut owner, 71, 1).await;
    assert!(owner.run_lifecycle().await.is_err());
    owner.verify_stable_reconcile().await.unwrap();

    let body = std::fs::read_to_string(history.path()).unwrap();
    assert!(body.contains("continuous_testnet_user_stream_subscribed"));
    assert!(body.contains("continuous_testnet_reconcile_verified"));
    assert!(!body.contains("continuous_testnet_campaign_recovery_verified"));
    assert!(!venue.calls().contains(&"submit"));
    cleanup(history);
}

#[tokio::test]
async fn recovery_only_owner_rejects_first_submit_eligible_campaign_before_io() {
    let config = lifecycle_config();
    let history = history();
    let venue = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));

    let error = ContinuousTestnetOwner::start_recovery_only(
        "testnet-owner-unplanned",
        config,
        Arc::clone(&venue),
        history.clone(),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("durable lifecycle plan"));
    assert!(venue.calls().is_empty());
    assert!(!history.path().exists());
    cleanup(history);
}

#[tokio::test]
async fn recovery_only_owner_rejects_completed_and_failed_campaigns_before_io() {
    let completed_config = lifecycle_config();
    let completed_history = history();
    let completed_venue = FixtureVenue::new(
        vec![Ok(order(&completed_config, OrderStatus::Open))],
        vec![
            Ok(order(&completed_config, OrderStatus::Open)),
            Ok(order(&completed_config, OrderStatus::Cancelled)),
        ],
        vec![Ok(order(&completed_config, OrderStatus::Cancelled))],
    );
    run_testnet_lifecycle(&completed_config, &completed_venue, &completed_history)
        .await
        .unwrap();
    let unused = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));
    assert!(
        ContinuousTestnetOwner::start_recovery_only(
            "completed-owner",
            completed_config,
            Arc::clone(&unused),
            completed_history.clone(),
        )
        .await
        .is_err()
    );
    assert!(unused.calls().is_empty());

    let failed_config = lifecycle_config();
    let failed_history = history();
    let failed_venue = FixtureVenue::new(
        vec![Err(ExchangeError::unavailable("fixture reject"))],
        Vec::new(),
        Vec::new(),
    );
    assert!(
        run_testnet_lifecycle(&failed_config, &failed_venue, &failed_history)
            .await
            .is_err()
    );
    let unused = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));
    assert!(
        ContinuousTestnetOwner::start_recovery_only(
            "failed-owner",
            failed_config,
            Arc::clone(&unused),
            failed_history.clone(),
        )
        .await
        .is_err()
    );
    assert!(unused.calls().is_empty());

    cleanup(completed_history);
    cleanup(failed_history);
}

#[tokio::test]
async fn clean_shutdown_recovers_and_cancels_an_interrupted_fresh_lifecycle() {
    let config = lifecycle_config();
    let history = history();
    let venue = Arc::new(FixtureVenue::new(
        vec![Err(ambiguous_submit(&config))],
        vec![
            Err(ExchangeError::unavailable("fixture query interrupted")),
            Ok(order(&config, OrderStatus::Open)),
            Ok(order(&config, OrderStatus::Cancelled)),
        ],
        vec![Ok(order(&config, OrderStatus::Cancelled))],
    ));
    let mut owner = ContinuousTestnetOwner::start(
        "testnet-owner-clean-shutdown",
        config,
        Arc::clone(&venue),
        history.clone(),
    )
    .await
    .unwrap();
    subscribe(&mut owner, 91, 1).await;

    assert!(owner.run_lifecycle().await.is_err());
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::RecoveryRequired
    );

    owner.shutdown_cleanly().await.unwrap();
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::KilledClean
    );
    assert!(owner.status().kill_switch_latched);
    assert_eq!(
        venue.calls(),
        vec![
            "reconcile",
            "reconcile",
            "submit",
            "query",
            "query",
            "cancel",
            "query",
            "reconcile",
            "reconcile",
        ]
    );

    let body = std::fs::read_to_string(history.path()).unwrap();
    assert!(body.contains("continuous_testnet_campaign_recovery_verified"));
    assert!(body.contains("continuous_testnet_kill_switch_engaged"));
    assert!(body.contains("continuous_testnet_killed_clean"));
    cleanup(history);
}

#[tokio::test]
async fn restarted_owner_finishes_pending_kill_switch_cleanup() {
    let history = history();
    let first_venue = Arc::new(FixtureVenue::with_reconcile(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![
            Ok(empty_reconcile_receipt()),
            Ok(empty_reconcile_receipt()),
            Err(ExchangeError::unavailable("fixture cleanup interrupted")),
        ],
    ));
    let owner_id = "testnet-owner-kill-restart";
    let mut first = ContinuousTestnetOwner::start_read_only(
        owner_id,
        Arc::clone(&first_venue),
        history.clone(),
    )
    .await
    .unwrap();
    subscribe(&mut first, 17, 1).await;
    assert!(first.engage_kill_switch().await.is_err());
    assert_eq!(
        first.status().phase,
        ContinuousTestnetOwnerPhase::RecoveryRequired
    );
    drop(first);

    let venue = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));
    let restarted =
        ContinuousTestnetOwner::start_read_only(owner_id, Arc::clone(&venue), history.clone())
            .await
            .unwrap();
    assert_eq!(
        restarted.status().phase,
        ContinuousTestnetOwnerPhase::KilledClean
    );
    assert!(restarted.status().kill_switch_latched);

    let body = std::fs::read_to_string(history.path()).unwrap();
    assert!(body.contains("continuous_testnet_killed_clean"));
    assert!(body.contains("\"recovered_from_kill_switch_engaged\":true"));
    assert!(body.contains("\"spot_balance_authority\":\"unavailable_in_reconcile_receipt\""));
    cleanup(history);
}

#[tokio::test]
async fn kill_switch_fails_closed_when_owned_open_orders_remain() {
    let history = history();
    let config = lifecycle_config();
    let venue = Arc::new(FixtureVenue::with_reconcile(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![
            Ok(empty_reconcile_receipt()),
            Ok(empty_reconcile_receipt()),
            Ok(reconcile_receipt_with_orders(vec![order(
                &config,
                OrderStatus::Open,
            )])),
            Ok(reconcile_receipt_with_orders(vec![order(
                &config,
                OrderStatus::Open,
            )])),
        ],
    ));
    let mut owner = ContinuousTestnetOwner::start(
        "testnet-owner-kill-open-orders",
        config,
        Arc::clone(&venue),
        history.clone(),
    )
    .await
    .unwrap();
    subscribe(&mut owner, 19, 1).await;

    let error = owner.engage_kill_switch().await.unwrap_err();
    assert!(matches!(
        error,
        ContinuousTestnetOwnerError::UnstableReconciliation
    ));
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::RecoveryRequired
    );

    let body = std::fs::read_to_string(history.path()).unwrap();
    assert!(body.contains("continuous_testnet_kill_switch_engaged"));
    assert!(body.contains("continuous_testnet_recovery_required"));
    assert!(!body.contains("continuous_testnet_killed_clean"));
    cleanup(history);
}

#[cfg(windows)]
#[tokio::test]
async fn windows_case_aliases_share_one_owner_lane() {
    let path = std::env::temp_dir().join(format!(
        "crypto-trading-continuous-testnet-case-{}-\u{00C4}.jsonl",
        Uuid::new_v4()
    ));
    let upper_history = JsonlHistory::new(path.to_string_lossy().to_ascii_uppercase());
    let lower_history = JsonlHistory::new(path.to_string_lossy().to_lowercase());
    let venue = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));
    let owner = ContinuousTestnetOwner::start_read_only(
        "testnet-owner-case-fold",
        Arc::clone(&venue),
        upper_history.clone(),
    )
    .await
    .unwrap();

    let second =
        ContinuousTestnetOwner::start_read_only("testnet-owner-case-fold", venue, lower_history)
            .await;

    assert!(matches!(
        second,
        Err(ContinuousTestnetOwnerError::OwnerBusy)
    ));
    drop(owner);
    cleanup(upper_history);
}

#[tokio::test]
async fn user_stream_faults_reconcile_and_require_a_fresh_subscription() {
    let config = lifecycle_config();
    let history = history();
    let venue = Arc::new(FixtureVenue::new(Vec::new(), Vec::new(), Vec::new()));
    let mut owner = ContinuousTestnetOwner::start(
        "testnet-owner-user-stream",
        config,
        Arc::clone(&venue),
        history.clone(),
    )
    .await
    .unwrap();

    subscribe(&mut owner, 1, 1).await;
    let applied = owner
        .ingest_user_data_item(account_event(1, 1, 2, 2))
        .await
        .unwrap();
    assert_eq!(
        applied,
        ContinuousTestnetUserDataOutcome::Applied(BinanceUserDataApply::AppliedAccountUpdate)
    );
    assert!(owner.status().balance_projection_observed);

    let restarted = owner
        .ingest_user_data_item(account_event(2, 1, 3, 3))
        .await
        .unwrap();
    assert!(matches!(
        restarted,
        ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(_)
    ));
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::AwaitingUserStream
    );
    assert!(!owner.status().balance_projection_observed);

    subscribe(&mut owner, 2, 4).await;
    let gap = owner
        .ingest_user_data_item(BinanceUserDataStreamItem::TransportGap {
            skipped: 2,
            observed_at: observed_at(5),
        })
        .await
        .unwrap();
    assert!(matches!(
        gap,
        ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(_)
    ));

    subscribe(&mut owner, 3, 6).await;
    let expired = owner
        .ingest_user_data_item(BinanceUserDataStreamItem::StreamExpired {
            observed_at: observed_at(7),
        })
        .await
        .unwrap();
    assert!(matches!(
        expired,
        ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(_)
    ));

    subscribe(&mut owner, 4, 8).await;
    owner
        .ingest_user_data_item(account_event(1, 1, 10, 10))
        .await
        .unwrap();
    let regression = owner
        .ingest_user_data_item(account_event(1, 2, 9, 11))
        .await
        .unwrap();
    assert!(matches!(
        regression,
        ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(_)
    ));

    assert_eq!(venue.calls(), vec!["reconcile"; 10]);
    let body = std::fs::read_to_string(history.path()).unwrap();
    assert!(body.contains("continuous_testnet_user_stream_subscribed"));
    assert!(body.contains("continuous_testnet_user_data_applied"));
    assert!(body.contains("connection_restart"));
    assert!(body.contains("transport_gap"));
    assert!(body.contains("stream_expired"));
    assert!(body.contains("event_time_regression"));
    assert!(body.contains("orders_positions_only"));
    assert!(body.contains("awaiting_fresh_user_stream"));
    cleanup(history);
}

async fn subscribe(
    owner: &mut ContinuousTestnetOwner<FixtureVenue>,
    subscription_id: u64,
    second: u32,
) {
    let outcome = owner
        .ingest_user_data_item(BinanceUserDataStreamItem::Subscribed {
            subscription_id,
            observed_at: observed_at(second),
        })
        .await
        .unwrap();
    assert_eq!(outcome, ContinuousTestnetUserDataOutcome::Subscribed);
    assert_eq!(
        owner.status().phase,
        ContinuousTestnetOwnerPhase::ReadyUnarmed
    );
}

fn account_event(
    generation: u64,
    sequence: u64,
    event_second: u32,
    observed_second: u32,
) -> BinanceUserDataStreamItem {
    BinanceUserDataStreamItem::Event(
        StreamEnvelope::new(
            generation,
            sequence,
            observed_at(observed_second),
            BinanceUserDataEvent::AccountUpdate(BinanceAccountUpdateEvent {
                event_time: observed_at(event_second),
                account_update_time: observed_at(event_second),
                balances: vec![BinanceUserDataBalance {
                    asset: "USDT".to_owned(),
                    free: Decimal::from(1_000),
                    locked: Decimal::ZERO,
                }],
            }),
        )
        .unwrap(),
    )
}

fn observed_at(second: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, second).unwrap()
}

struct FixtureVenue {
    submit: Mutex<VecDeque<Result<Order, ExchangeError>>>,
    query: Mutex<VecDeque<Result<Order, ExchangeError>>>,
    cancel: Mutex<VecDeque<Result<Order, ExchangeError>>>,
    reconcile: Mutex<VecDeque<Result<ReconcileReceipt, ExchangeError>>>,
    calls: Mutex<Vec<&'static str>>,
}

impl FixtureVenue {
    fn new(
        submit: Vec<Result<Order, ExchangeError>>,
        query: Vec<Result<Order, ExchangeError>>,
        cancel: Vec<Result<Order, ExchangeError>>,
    ) -> Self {
        Self::with_reconcile(submit, query, cancel, Vec::new())
    }

    fn with_reconcile(
        submit: Vec<Result<Order, ExchangeError>>,
        query: Vec<Result<Order, ExchangeError>>,
        cancel: Vec<Result<Order, ExchangeError>>,
        reconcile: Vec<Result<ReconcileReceipt, ExchangeError>>,
    ) -> Self {
        Self {
            submit: Mutex::new(submit.into()),
            query: Mutex::new(query.into()),
            cancel: Mutex::new(cancel.into()),
            reconcile: Mutex::new(reconcile.into()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }
}

impl TestnetLifecycleVenue for FixtureVenue {
    fn submit(&self, _intent: OrderIntent) -> TestnetLifecycleVenueFuture<'_, Order> {
        self.calls.lock().unwrap().push("submit");
        let result = self.submit.lock().unwrap().pop_front().unwrap();
        Box::pin(async move { result })
    }

    fn query(
        &self,
        _symbol: Symbol,
        _market_type: MarketType,
        _client_order_id: Uuid,
    ) -> TestnetLifecycleVenueFuture<'_, Order> {
        self.calls.lock().unwrap().push("query");
        let result = self.query.lock().unwrap().pop_front().unwrap();
        Box::pin(async move { result })
    }

    fn cancel(&self, _order_id: String) -> TestnetLifecycleVenueFuture<'_, Order> {
        self.calls.lock().unwrap().push("cancel");
        let result = self.cancel.lock().unwrap().pop_front().unwrap();
        Box::pin(async move { result })
    }
}

#[async_trait]
impl ExchangeHandle for FixtureVenue {
    async fn execute(&self, _command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        panic!("continuous owner must use the narrow lifecycle venue seam")
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        self.calls.lock().unwrap().push("reconcile");
        self.reconcile
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(empty_reconcile_receipt_for_scope(scope)))
    }

    async fn subscribe(
        &self,
        _subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        panic!("not used")
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        Ok(ExchangeStatus {
            exchange: "binance".to_owned(),
            mode: ExchangeMode::Testnet,
            availability: ExchangeAvailability::Ready,
            latest_market_timestamp: None,
            open_orders: 0,
        })
    }
}

fn lifecycle_config() -> TestnetLifecycleConfig {
    let mut intent = OrderIntent::limit(
        "binance",
        Symbol::new("BTC-USDT-SPOT").unwrap(),
        MarketType::Spot,
        Side::Buy,
        Quantity::new(Decimal::from_str("0.001").unwrap()).unwrap(),
        Price::new(Decimal::from_str("49000.1").unwrap()).unwrap(),
    );
    intent.client_order_id = Uuid::parse_str("0f3c807d-776f-4de4-85d0-93760a82dfcf").unwrap();
    intent.time_in_force = TimeInForce::PostOnly;
    TestnetLifecycleConfig::new(
        "continuous-owner-campaign",
        intent,
        "BTCUSDT",
        TestnetLifecycleObservation::Open,
        Duration::from_millis(1),
        4,
    )
    .unwrap()
}

fn order(config: &TestnetLifecycleConfig, status: OrderStatus) -> Order {
    Order {
        id: "binance:spot:BTCUSDT:31".to_owned(),
        intent: config.intent().clone(),
        filled_quantity: Quantity::new(Decimal::ZERO).unwrap(),
        average_fill_price: None,
        status,
        created_at: Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap(),
        updated_at: Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 1).unwrap(),
    }
}

fn empty_reconcile_receipt() -> ReconcileReceipt {
    empty_reconcile_receipt_for_scope(ReconcileScope::All)
}

fn empty_reconcile_receipt_for_scope(scope: ReconcileScope) -> ReconcileReceipt {
    ReconcileReceipt {
        scope,
        orders: Vec::new(),
        foreign_orders: Vec::new(),
        positions: Vec::new(),
        observed_at: Utc.with_ymd_and_hms(2026, 8, 12, 0, 0, 0).unwrap(),
    }
}

fn reconcile_receipt_with_orders(orders: Vec<Order>) -> ReconcileReceipt {
    ReconcileReceipt {
        orders,
        ..empty_reconcile_receipt()
    }
}

fn ambiguous_submit(config: &TestnetLifecycleConfig) -> ExchangeError {
    ExchangeError::AmbiguousOutcome {
        operation: ExchangeOperation::SubmitOrder,
        client_order_id: Some(config.intent().client_order_id),
        operation_key: Some(ExchangeOperationKey::ClientOrderId {
            client_order_id: config.intent().client_order_id,
        }),
        reason: "fixture disconnect".to_owned(),
    }
}

fn history() -> JsonlHistory {
    JsonlHistory::new(std::env::temp_dir().join(format!(
        "crypto-trading-continuous-testnet-{}.jsonl",
        Uuid::new_v4()
    )))
}

fn cleanup(history: JsonlHistory) {
    let path = history.path().to_owned();
    let lock_path = path.with_file_name(format!(
        "{}.lock",
        path.file_name().and_then(|name| name.to_str()).unwrap()
    ));
    drop(history);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(lock_path);
}
