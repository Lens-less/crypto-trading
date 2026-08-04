use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{
        Request, Response, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER},
    },
};
use crypto_trading_control_plane::ReadControlPlane;
use crypto_trading_runtime::MemoryJournalSnapshotSource;
use crypto_trading_web::{
    WebAccessPolicy, WebServerConfig, WebServerConfigError, api_router, api_router_with_shutdown,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::time::{Duration, timeout};
use tower::ServiceExt;
use uuid::Uuid;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn capabilities_and_system_expose_fail_closed_truth_with_security_headers() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());

    let capabilities = app
        .clone()
        .oneshot(get("/api/v1/capabilities"))
        .await
        .unwrap();
    assert_eq!(capabilities.status(), StatusCode::OK);
    assert_security_headers(&capabilities);
    let capabilities = response_json(capabilities).await;
    assert_eq!(capabilities["live_trading_enabled"], false);
    assert_eq!(capabilities["release_stage"], "paper-only");
    let web = capabilities["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "control-plane.web")
        .unwrap();
    assert_eq!(web["level"], "available");
    assert_eq!(web["scope"]["environments"], json!(["offline", "paper"]));
    assert_eq!(web["scope"]["access"], "paper-trading");
    assert!(web["blockers"].as_array().unwrap().iter().any(|blocker| {
        blocker
            .as_str()
            .is_some_and(|text| text.contains("mainnet authority are not exposed"))
    }));
    for capability_id in ["research.backtest", "research.indicators"] {
        let capability = capabilities["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == capability_id)
            .unwrap_or_else(|| panic!("missing capability {capability_id}"));
        assert_eq!(capability["level"], "unavailable", "{capability_id}");
        assert!(
            capability["blockers"]
                .as_array()
                .is_some_and(|blockers| !blockers.is_empty()),
            "{capability_id} must explain its library-only boundary"
        );
    }

    let system = app.oneshot(get("/api/v1/system")).await.unwrap();
    assert_eq!(system.status(), StatusCode::OK);
    assert_security_headers(&system);
    let system = response_json(system).await;
    assert_eq!(system["live_trading_enabled"], false);
    assert_eq!(system["access_scope"], "loopback");
    assert_eq!(system["authentication_required"], false);
    assert_eq!(system["projection_status"], "complete");
    assert_eq!(system["kill_switch"], "not_available");
    assert_eq!(system["market_data_freshness"], "not_available");
    assert_eq!(system["adapter_health"], "not_available");
}

#[tokio::test]
async fn system_endpoint_projects_operational_signals_from_risk_and_task_health() {
    let journal_id = fixed_uuid(1);
    let bytes = jsonl(&[
        json!({
            "timestamp": "2026-07-25T00:00:00Z",
            "strategy": "read_only_task",
            "symbol": "control-plane",
            "decision": "task_registered",
            "details": {
                "schema_version": 1,
                "task_id": "arb-btc-usdt",
                "task_kind": "arbitrage_monitor",
                "phase": "registered",
                "processed_event_count": 0,
                "sources": [
                    task_source(&Value::Null, "left", "starting", "unknown", 0),
                    task_source(&Value::Null, "right", "starting", "unknown", 0),
                ],
                "exit": Value::Null,
                "failure": Value::Null,
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:01:00Z",
            "strategy": "read_only_task",
            "symbol": "control-plane",
            "decision": "task_running",
            "details": {
                "schema_version": 1,
                "task_id": "arb-btc-usdt",
                "task_kind": "arbitrage_monitor",
                "phase": "running",
                "processed_event_count": 2,
                "sources": [
                    task_source(&json!("00000000-0000-0000-0000-000000000301"), "left", "running", "healthy", 1),
                    task_source(&json!("00000000-0000-0000-0000-000000000302"), "right", "running", "healthy", 1),
                ],
                "exit": Value::Null,
                "failure": Value::Null,
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:02:00Z",
            "strategy": "account_risk",
            "symbol": "paper",
            "decision": "account_risk_admitted",
            "details": {
                "kind": "admitted",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "task_id": "paper-grid-btc",
                "symbol": "BTC-USDT",
                "notional": "100",
                "utc_date": "2026-07-25",
                "recorded_at": "2026-07-25T00:02:00Z",
                "warnings": []
            }
        }),
    ]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.oneshot(get("/api/v1/system")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let system = response_json(response).await;
    assert_eq!(system["kill_switch"], "normal");
    assert_eq!(system["market_data_freshness"], "not_available");
    assert_eq!(system["adapter_health"], "normal");
}

#[tokio::test]
async fn system_endpoint_fails_closed_when_risk_or_task_projection_is_degraded() {
    let journal_id = fixed_uuid(1);
    let bytes = jsonl(&[
        json!({
            "timestamp": "2026-07-25T00:00:00Z",
            "strategy": "read_only_task",
            "symbol": "control-plane",
            "decision": "task_registered",
            "details": {
                "schema_version": 1,
                "task_id": "arb-btc-usdt",
                "task_kind": "arbitrage_monitor",
                "phase": "registered",
                "processed_event_count": 0,
                "sources": [
                    task_source(&Value::Null, "left", "starting", "unknown", 0),
                    task_source(&Value::Null, "right", "starting", "unknown", 0),
                ],
                "exit": Value::Null,
                "failure": Value::Null,
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:01:00Z",
            "strategy": "read_only_task",
            "symbol": "control-plane",
            "decision": "task_running",
            "details": {
                "schema_version": 1,
                "task_id": "arb-btc-usdt",
                "task_kind": "arbitrage_monitor",
                "phase": "running",
                "processed_event_count": 2,
                "sources": [
                    task_source(&json!("00000000-0000-0000-0000-000000000301"), "left", "running", "healthy", 1),
                    task_source(&json!("00000000-0000-0000-0000-000000000302"), "right", "running", "degraded", 1),
                ],
                "exit": Value::Null,
                "failure": Value::Null,
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:02:00Z",
            "strategy": "account_risk",
            "symbol": "paper",
            "decision": "account_risk_kill_switch_engaged",
            "details": {
                "kind": "kill_switch_engaged",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "reason": "operator drill",
                "recorded_at": "2026-07-25T00:02:00Z"
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:03:00Z",
            "strategy": "account_risk",
            "symbol": "paper",
            "decision": "account_risk_paused",
            "details": {
                "kind": "paused",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "reason": 7,
                "recorded_at": "2026-07-25T00:03:00Z"
            }
        }),
    ]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.oneshot(get("/api/v1/system")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let system = response_json(response).await;
    assert_eq!(system["kill_switch"], "degraded");
    assert_eq!(system["market_data_freshness"], "not_available");
    assert_eq!(system["adapter_health"], "degraded");
}

#[tokio::test]
async fn monitor_endpoint_exposes_only_the_bounded_read_model() {
    let bytes = jsonl(&[json!({
        "timestamp": "2026-07-24T00:00:00Z",
        "strategy": "arbitrage_monitor",
        "symbol": "BTC-USDT",
        "decision": "monitor_opportunity",
        "details": {
            "schema_version": 1,
            "sequence": 2,
            "market_generation": 2,
            "market_update": "accepted",
            "left": {
                "exchange": "left",
                "symbol": "BTC-USDT",
                "market_type": "perpetual",
            },
            "right": {
                "exchange": "right",
                "symbol": "BTC-USDT",
                "market_type": "perpetual",
            },
            "outcome": {
                "type": "opportunity",
                "buy_exchange": "left",
                "sell_exchange": "right",
                "buy_price": "100",
                "sell_price": "102",
                "absolute_spread": "2",
                "spread_percent": "2",
                "threshold_percent": "0.5",
                "api_key": "must-not-leak",
                "intents": ["must-not-leak"],
            },
        },
    })]);
    let response = fixture_app(bytes, WebAccessPolicy::loopback_open())
        .oneshot(get("/api/v1/monitor"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    let monitor = response_json(response).await;
    assert_eq!(monitor["latest"]["state"], "opportunity");
    assert_eq!(monitor["latest"]["projection"]["spread_percent"], "2");
    let encoded = serde_json::to_string(&monitor).unwrap();
    assert!(!encoded.contains("api_key"));
    assert!(!encoded.contains("intents"));
    assert!(!encoded.contains("must-not-leak"));
}

#[tokio::test]
async fn alerts_endpoint_exposes_only_the_bounded_read_model_and_no_write_authority() {
    let bytes = jsonl(&[
        json!({
            "timestamp": "2026-07-24T00:00:00Z",
            "strategy": "price_alert",
            "symbol": "BTC-USDT",
            "decision": "price_alert_occurred",
            "details": {
                "schema_version": 1,
                "sequence": 1,
                "exchange": "binance",
                "market_type": "spot",
                "kind": "upper_limit",
                "price": "101.25",
                "change_percent": null,
                "market_revision": 7,
                "market_generation": 3,
            },
        }),
        json!({
            "timestamp": "2026-07-24T00:00:01Z",
            "strategy": "unrelated",
            "symbol": "BTC-USDT",
            "decision": "hold",
            "details": {
                "api_key": "must-not-leak",
                "message": "must-not-leak",
            },
        }),
    ]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.clone().oneshot(get("/api/v1/alerts")).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    let alerts = response_json(response).await;
    assert_eq!(alerts["projection_status"], "complete");
    assert_eq!(alerts["occurrences"][0]["kind"], "upper_limit");
    assert_eq!(alerts["occurrences"][0]["price"], "101.25");
    let encoded = serde_json::to_string(&alerts).unwrap();
    assert!(!encoded.contains("api_key"));
    assert!(!encoded.contains("must-not-leak"));

    let write = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn tasks_endpoint_exposes_durable_lifecycle_without_control_authority() {
    let bytes = jsonl(&[
        json!({
            "timestamp": "2026-07-25T00:00:00Z",
            "strategy": "read_only_task",
            "symbol": "control-plane",
            "decision": "task_registered",
            "details": {
                "schema_version": 1,
                "task_id": "arb-btc-usdt",
                "task_kind": "arbitrage_monitor",
                "phase": "registered",
                "processed_event_count": 0,
                "sources": [
                    task_source(&Value::Null, "left", "starting", "unknown", 0),
                    task_source(&Value::Null, "right", "starting", "unknown", 0),
                ],
                "exit": Value::Null,
                "failure": Value::Null,
            },
        }),
        json!({
            "timestamp": "2026-07-25T00:00:01Z",
            "strategy": "read_only_task",
            "symbol": "control-plane",
            "decision": "task_running",
            "details": {
                "schema_version": 1,
                "task_id": "arb-btc-usdt",
                "task_kind": "arbitrage_monitor",
                "phase": "running",
                "processed_event_count": 0,
                "sources": [
                    task_source(
                        &json!("00000000-0000-0000-0000-000000000301"),
                        "left",
                        "running",
                        "healthy",
                        1,
                    ),
                    task_source(
                        &json!("00000000-0000-0000-0000-000000000302"),
                        "right",
                        "running",
                        "degraded",
                        1,
                    ),
                ],
                "exit": Value::Null,
                "failure": Value::Null,
            },
        }),
    ]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.clone().oneshot(get("/api/v1/tasks")).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    let tasks = response_json(response).await;
    assert_eq!(tasks["projection_status"], "complete");
    assert_eq!(tasks["tasks"][0]["task_id"], "arb-btc-usdt");
    assert_eq!(tasks["tasks"][0]["phase"], "running");
    assert_eq!(tasks["tasks"][0]["recovery"], "investigate");
    assert_eq!(tasks["tasks"][0]["sources"][0]["health"], "healthy");
    assert_eq!(tasks["tasks"][0]["sources"][1]["health"], "degraded");
    let encoded = serde_json::to_string(&tasks).unwrap();
    for forbidden in ["orders", "intents", "api_key", "authorization", "raw_error"] {
        assert!(
            !encoded.contains(forbidden),
            "{forbidden} leaked in {encoded}"
        );
    }

    let write = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/tasks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn scanner_endpoint_exposes_only_the_last_bounded_historical_ranking() {
    let scanner_row = json!({
        "rank": 1,
        "activity": "active",
        "priority": "benchmark",
        "instrument": {
            "exchange": "fixture",
            "symbol": "BTC-USDC",
            "market_type": "spot",
        },
        "started_at": "2026-07-25T00:00:00Z",
        "last_observed_at": "2026-07-25T00:04:00Z",
        "observation_count": 5,
        "last_observation_sequence": 5,
        "current_price": "100",
        "lower_price": "95",
        "upper_price": "105",
        "pending_buy_price": "99",
        "pending_sell_price": "101",
        "grid_width_percent": "10",
        "grid_interval_percent": "1",
        "grid_count": 10,
        "running_seconds": 300,
        "buy_crosses": 2,
        "sell_crosses": 2,
        "total_crosses": 4,
        "complete_cycles": 2,
        "recent_five_minute_cycles": 2,
        "cycles_per_hour": "10",
        "estimated_apr": "500",
        "estimated_apr_kind": "heuristic",
        "volume_24h_usdc": "1000000",
        "price_change_24h_percent": null,
        "rating_grade": "s",
        "rating_score": "95",
    });
    let bytes = jsonl(&[json!({
        "timestamp": "2026-07-25T00:05:00Z",
        "strategy": "virtual_grid_scanner",
        "symbol": "control-plane",
        "decision": "scanner_ranked",
        "details": {
            "schema_version": 2,
            "run_id": "scan-web",
            "ranking_policy": "explicit_benchmark_then_apr_desc",
            "apr_window_seconds": 300,
            "estimated_apr_kind": "heuristic",
            "estimated_apr_assumptions": {
                "order_notional_usdc": "100",
                "round_trip_fee_percent": "0.2",
            },
            "min_complete_cycles": 0,
            "row_limit": 50,
            "candidate_count": 1,
            "eligible_count": 1,
            "filtered_by_cycles_count": 0,
            "truncated": false,
            "rows": [scanner_row],
        },
    })]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.clone().oneshot(get("/api/v1/scanner")).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    let scanner = response_json(response).await;
    assert_eq!(scanner["projection_status"], "complete");
    assert_eq!(scanner["latest"]["run_id"], "scan-web");
    assert_eq!(scanner["latest"]["estimated_apr_kind"], "heuristic");
    assert_eq!(
        scanner["latest"]["estimated_apr_assumptions"]["round_trip_fee_percent"],
        "0.2"
    );
    assert_eq!(
        scanner["latest"]["rows"][0]["instrument"]["symbol"],
        "BTC-USDC"
    );
    assert_eq!(scanner["latest"]["rows"][0]["estimated_apr"], "500");
    assert_eq!(
        scanner["latest"]["rows"][0]["estimated_apr_kind"],
        "heuristic"
    );
    let encoded = serde_json::to_string(&scanner).unwrap();
    for forbidden in ["orders", "intents", "api_key", "authorization", "secret"] {
        assert!(
            !encoded.contains(forbidden),
            "{forbidden} leaked in {encoded}"
        );
    }

    let write = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/scanner")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn scanner_endpoint_preserves_legacy_v1_unknown_apr_semantics() {
    let scanner_row = json!({
        "rank": 1,
        "activity": "active",
        "priority": "benchmark",
        "instrument": {
            "exchange": "fixture",
            "symbol": "BTC-USDC",
            "market_type": "spot",
        },
        "started_at": "2026-07-25T00:00:00Z",
        "last_observed_at": "2026-07-25T00:04:00Z",
        "observation_count": 5,
        "last_observation_sequence": 5,
        "current_price": "100",
        "lower_price": "95",
        "upper_price": "105",
        "pending_buy_price": "99",
        "pending_sell_price": "101",
        "grid_width_percent": "10",
        "grid_interval_percent": "1",
        "grid_count": 10,
        "running_seconds": 300,
        "buy_crosses": 2,
        "sell_crosses": 2,
        "total_crosses": 4,
        "complete_cycles": 2,
        "recent_five_minute_cycles": 2,
        "cycles_per_hour": "10",
        "estimated_apr": "500",
        "volume_24h_usdc": "1000000",
        "price_change_24h_percent": null,
        "rating_grade": "s",
        "rating_score": "95",
    });
    let bytes = jsonl(&[json!({
        "timestamp": "2026-07-25T00:05:00Z",
        "strategy": "virtual_grid_scanner",
        "symbol": "control-plane",
        "decision": "scanner_ranked",
        "details": {
            "schema_version": 1,
            "run_id": "scan-web-v1",
            "ranking_policy": "explicit_benchmark_then_apr_desc",
            "apr_window_seconds": 300,
            "min_complete_cycles": 0,
            "row_limit": 50,
            "candidate_count": 1,
            "eligible_count": 1,
            "filtered_by_cycles_count": 0,
            "truncated": false,
            "rows": [scanner_row],
        },
    })]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.clone().oneshot(get("/api/v1/scanner")).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let scanner = response_json(response).await;
    assert_eq!(scanner["projection_status"], "complete");
    assert_eq!(scanner["latest"]["run_id"], "scan-web-v1");
    assert_eq!(scanner["latest"]["estimated_apr_kind"], "unknown");
    assert_eq!(
        scanner["latest"]["estimated_apr_assumptions"]["order_notional_usdc"],
        "10"
    );
    assert_eq!(
        scanner["latest"]["estimated_apr_assumptions"]["round_trip_fee_percent"],
        "0.004"
    );
    assert_eq!(
        scanner["latest"]["rows"][0]["estimated_apr_kind"],
        "unknown"
    );
}

#[tokio::test]
async fn risk_endpoint_exposes_paper_reservations_and_account_exposure_read_only() {
    let journal_id = fixed_uuid(1);
    let reservation_id = fixed_uuid(31);
    let batch_id = fixed_uuid(32);
    let bytes = jsonl(&[json!({
        "timestamp": "2026-07-25T00:00:00Z",
        "strategy": "paper_account",
        "symbol": "paper-main",
        "decision": "paper_account_reserved",
        "details": {
            "schema_version": 1,
            "journal_id": journal_id,
            "account_id": "paper-main",
            "initial_available": "1000",
            "request": {
                "reservation_id": reservation_id,
                "task_id": "paper-grid-btc",
                "idempotency_key": "grid/open/0001",
                "batch_id": batch_id,
                "cost_model": {
                    "version": 1,
                    "fee_bps": 10,
                    "funding_buffer_bps": 5,
                    "slippage_bps": 15
                },
                "legs": [{
                    "index": 0,
                    "exchange": "paper-binance",
                    "symbol": "BTC-USDT",
                    "market_type": "spot",
                    "side": "buy",
                    "reserved_notional": "100"
                }]
            },
            "reserved_exposure": "100.30"
        }
    })]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.clone().oneshot(get("/api/v1/risk")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    let risk = response_json(response).await;
    assert_eq!(risk["schema_version"], 1);
    let accounts = &risk["paper_accounts"];
    assert_eq!(accounts["projection_status"], "complete");
    assert_eq!(accounts["accounts"][0]["account_id"], "paper-main");
    assert_eq!(accounts["accounts"][0]["available"], "899.70");
    assert_eq!(accounts["accounts"][0]["pending_reserved"], "100.30");
    assert_eq!(
        accounts["accounts"][0]["reservations"][0]["reservation_id"],
        reservation_id.to_string()
    );
    // Without account-risk facts the projection is present and empty.
    assert_eq!(risk["account_risk"]["projection_status"], "complete");
    assert_eq!(risk["account_risk"]["scopes"], json!([]));

    let write = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/risk")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn risk_endpoint_projects_durable_account_risk_state() {
    let journal_id = fixed_uuid(1);
    let bytes = jsonl(&[
        json!({
            "timestamp": "2026-07-25T00:01:00Z",
            "strategy": "account_risk",
            "symbol": "paper",
            "decision": "account_risk_admitted",
            "details": {
                "kind": "admitted",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "task_id": "paper-grid-btc",
                "symbol": "BTC-USDT",
                "notional": "100",
                "utc_date": "2026-07-25",
                "recorded_at": "2026-07-25T00:01:00Z",
                "warnings": ["low_balance"]
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:02:00Z",
            "strategy": "account_risk",
            "symbol": "paper",
            "decision": "account_risk_paused",
            "details": {
                "kind": "paused",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "reason": "exchange maintenance",
                "recorded_at": "2026-07-25T00:02:00Z"
            }
        }),
        json!({
            "timestamp": "2026-07-25T00:03:00Z",
            "strategy": "account_risk",
            "symbol": "paper",
            "decision": "account_risk_kill_switch_engaged",
            "details": {
                "kind": "kill_switch_engaged",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "reason": "operator drill",
                "recorded_at": "2026-07-25T00:03:00Z"
            }
        }),
    ]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.oneshot(get("/api/v1/risk")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let risk = response_json(response).await;
    // The durable account-level risk state is projected alongside exposure:
    // pause and kill-switch facts, UTC-day counts, and open position clocks.
    let account_risk = &risk["account_risk"];
    assert_eq!(account_risk["projection_status"], "complete");
    let scope = &account_risk["scopes"][0];
    assert_eq!(scope["scope_id"], "paper");
    assert_eq!(scope["paused"], true);
    assert_eq!(scope["pause_reason"], "exchange maintenance");
    assert_eq!(scope["kill_switch_engaged"], true);
    assert_eq!(scope["kill_switch_reason"], "operator drill");
    assert_eq!(scope["trade_date_utc"], "2026-07-25");
    assert_eq!(scope["daily_trade_count"], 1);
    assert_eq!(scope["admitted_count"], 1);
    assert_eq!(scope["open_positions"][0]["task_id"], "paper-grid-btc");
}

#[tokio::test]
async fn settings_are_bounded_read_only_metadata_without_credential_values() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());
    let response = app.clone().oneshot(get("/api/v1/settings")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    let settings = response_json(response).await;
    assert_eq!(settings["log_sink"], "stdout_stderr");
    assert_eq!(settings["notification_evidence"], "journal_projection");
    assert_eq!(settings["credentials"]["mainnet"], "not_accepted");
    assert_eq!(
        settings["request_limit"]["maximum_requests"],
        crypto_trading_web::WEB_REQUEST_LIMIT_PER_MINUTE
    );
    assert_eq!(settings["request_limit"]["window_seconds"], 60);
    let encoded = serde_json::to_string(&settings).unwrap();
    for forbidden in ["api_key", "api_secret", "bearer_token"] {
        assert!(!encoded.contains(forbidden));
    }

    let write = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/settings")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn authenticated_api_requests_fail_with_retry_after_when_the_window_is_exhausted() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());
    for _ in 0..crypto_trading_web::WEB_REQUEST_LIMIT_PER_MINUTE {
        let response = app
            .clone()
            .oneshot(get("/api/v1/capabilities"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let limited = app.oneshot(get("/api/v1/capabilities")).await.unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after = limited.headers()[RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!((1..=60).contains(&retry_after));
    assert_eq!(
        response_json(limited).await["error"]["code"],
        "rate_limited"
    );
}

#[tokio::test]
async fn executions_use_cursor_as_a_change_watermark_without_exposing_payloads() {
    let bytes = jsonl(&[decision_record(&json!({
        "api_key": "super-secret",
        "authorization": "Bearer should-not-leak",
    }))]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let first = app
        .clone()
        .oneshot(get("/api/v1/executions"))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = response_json(first).await;
    assert_eq!(first["operator"]["batches"], json!([]));
    assert_eq!(first["changes"]["events"][0]["kind"], "legacy_decision");
    let encoded = serde_json::to_string(&first).unwrap();
    for secret in [
        "api_key",
        "super-secret",
        "authorization",
        "should-not-leak",
    ] {
        assert!(!encoded.contains(secret), "{secret} leaked in {encoded}");
    }

    let cursor = first["changes"]["next_cursor"].as_str().unwrap();
    let resumed = app
        .oneshot(get(&format!("/api/v1/executions?cursor={cursor}")))
        .await
        .unwrap();
    assert_eq!(resumed.status(), StatusCode::OK);
    let resumed = response_json(resumed).await;
    assert_eq!(resumed["changes"]["events"], json!([]));
    assert_eq!(resumed["changes"]["boundary"]["kind"], "snapshot_end");
}

#[tokio::test]
async fn invalid_queries_and_cursors_return_bounded_json_errors() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());

    let invalid_query = app
        .clone()
        .oneshot(get("/api/v1/executions?unexpected=true"))
        .await
        .unwrap();
    assert_eq!(invalid_query.status(), StatusCode::BAD_REQUEST);
    assert_security_headers(&invalid_query);
    assert_eq!(
        response_json(invalid_query).await["error"]["code"],
        "invalid_query"
    );

    let invalid_cursor = app
        .oneshot(get("/api/v1/executions?cursor=not-a-cursor"))
        .await
        .unwrap();
    assert_eq!(invalid_cursor.status(), StatusCode::BAD_REQUEST);
    assert_security_headers(&invalid_cursor);
    let body = response_json(invalid_cursor).await;
    assert_eq!(body["error"]["code"], "invalid_cursor");
    assert!(!serde_json::to_string(&body).unwrap().contains("checksum"));
}

#[tokio::test]
async fn optional_auth_never_prints_the_token_and_protects_every_route() {
    let access = WebAccessPolicy::bearer(TOKEN).unwrap();
    let debug = format!("{access:?}");
    assert!(debug.contains("authentication_required: true"));
    assert!(!debug.contains(TOKEN));
    let app = fixture_app(Vec::new(), access);

    let health = app.clone().oneshot(get("/api/v1/health")).await.unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let health = response_json(health).await;
    assert_eq!(health["status"], "ready");
    assert_eq!(health["live_trading_enabled"], false);

    let unauthorized = app.clone().oneshot(get("/api/v1/system")).await.unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_security_headers(&unauthorized);
    assert_eq!(unauthorized.headers()["www-authenticate"], "Bearer");
    assert_eq!(
        response_json(unauthorized).await["error"]["code"],
        "authentication_required"
    );

    let authorized = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/system")
                .header(AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);
    assert_eq!(
        response_json(authorized).await["authentication_required"],
        true
    );
}

#[tokio::test]
async fn sse_emits_atomic_pages_and_resumes_from_last_event_id_without_payloads() {
    let bytes = jsonl(&[
        decision_record(&json!({"api_key": "first-secret"})),
        decision_record(&json!({"authorization": "Bearer second-secret"})),
    ]);
    let app = fixture_app(bytes, WebAccessPolicy::loopback_open());

    let response = app.clone().oneshot(get("/api/v1/events")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_security_headers(&response);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
    let first_event = first_sse_event(response).await;
    assert_eq!(first_event.matches("event: operation_page").count(), 1);
    assert_eq!(first_event.matches("id: ").count(), 1);
    let page = sse_json_data(&first_event);
    assert_eq!(page["events"].as_array().unwrap().len(), 2);
    assert_eq!(page["events"][0]["sequence"], 1);
    assert_eq!(page["events"][1]["sequence"], 2);
    for secret in ["api_key", "first-secret", "authorization", "second-secret"] {
        assert!(!first_event.contains(secret));
    }
    let cursor = sse_field(&first_event, "id").unwrap();

    let resumed = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/events")
                .header("last-event-id", cursor)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let resumed_page = sse_json_data(&first_sse_event(resumed).await);
    assert_eq!(resumed_page["events"], json!([]));
    assert_eq!(resumed_page["boundary"]["kind"], "snapshot_end");
}

#[tokio::test]
async fn sse_terminates_when_application_shutdown_begins() {
    let source = MemoryJournalSnapshotSource::new(fixed_uuid(1), Vec::new()).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::watch::channel(false);
    let app = api_router_with_shutdown(
        Arc::new(control_plane),
        WebAccessPolicy::loopback_open(),
        shutdown_receiver,
    );

    let response = app.oneshot(get("/api/v1/events")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let first = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("initial SSE event timed out")
        .expect("SSE stream ended before its initial page")
        .expect("initial SSE body failed");
    assert!(String::from_utf8_lossy(&first).contains("event: operation_page"));

    shutdown_sender.send_replace(true);
    let next = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("SSE stream ignored application shutdown");
    assert!(next.is_none(), "SSE body remained open after shutdown");
}

#[tokio::test]
async fn sse_rejects_ambiguous_resume_positions_before_streaming() {
    let response = fixture_app(Vec::new(), WebAccessPolicy::loopback_open())
        .oneshot(
            Request::builder()
                .uri("/api/v1/events?cursor=query-cursor")
                .header("last-event-id", "different-header-cursor")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_security_headers(&response);
    assert_eq!(
        response_json(response).await["error"]["code"],
        "invalid_query"
    );
}

#[tokio::test]
async fn listener_configuration_is_loopback_only_and_supports_ephemeral_tests() {
    let default = WebServerConfig::default();
    assert!(default.bind_addr().ip().is_loopback());

    let wildcard = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8787);
    assert_eq!(
        WebServerConfig::try_new(wildcard).unwrap_err(),
        WebServerConfigError::NonLoopbackDenied
    );

    let listener = WebServerConfig::loopback(0).bind().await.unwrap();
    assert!(listener.local_addr().unwrap().ip().is_loopback());
}

#[tokio::test]
async fn fallback_errors_receive_the_same_security_policy() {
    let response = fixture_app(Vec::new(), WebAccessPolicy::loopback_open())
        .oneshot(get("/does-not-exist"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_security_headers(&response);
    assert_eq!(response_json(response).await["error"]["code"], "not_found");
}

fn fixture_app(bytes: Vec<u8>, access: WebAccessPolicy) -> Router {
    let source = MemoryJournalSnapshotSource::new(fixed_uuid(1), bytes).unwrap();
    let control_plane = ReadControlPlane::new(Arc::new(source)).unwrap();
    api_router(Arc::new(control_plane), access)
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

fn assert_security_headers(response: &Response<Body>) {
    let headers = response.headers();
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(headers["referrer-policy"], "no-referrer");
    assert_eq!(headers["x-frame-options"], "DENY");
    assert!(headers["content-security-policy"].to_str().is_ok());
    assert!(headers["permissions-policy"].to_str().is_ok());
}

async fn response_json(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn first_sse_event(response: Response<Body>) -> String {
    let mut stream = response.into_body().into_data_stream();
    let bytes = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("SSE event timed out")
        .expect("SSE stream ended")
        .expect("SSE body failed");
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn sse_json_data(event: &str) -> Value {
    serde_json::from_str(sse_field(event, "data").unwrap()).unwrap()
}

fn sse_field<'a>(event: &'a str, name: &str) -> Option<&'a str> {
    event
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
}

fn decision_record(details: &Value) -> Value {
    json!({
        "timestamp": "2026-07-24T00:00:00Z",
        "strategy": "web-test",
        "symbol": "BTC-USDT",
        "decision": "hold",
        "details": details,
    })
}

fn task_source(
    task_id: &Value,
    source_id: &str,
    phase: &str,
    health: &str,
    event_sequence: u64,
) -> Value {
    json!({
        "schema_version": 1,
        "task_id": task_id,
        "source_id": source_id,
        "phase": phase,
        "health": health,
        "event_sequence": event_sequence,
        "consecutive_source_failures": u32::from(health == "degraded"),
        "last_event_at": if event_sequence == 0 {
            Value::Null
        } else {
            json!("2026-07-25T00:00:01Z")
        },
        "exit": Value::Null,
    })
}

fn jsonl(records: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(&serde_json::to_vec(record).unwrap());
        bytes.push(b'\n');
    }
    bytes
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}

#[tokio::test]
async fn readiness_probe_stays_unauthenticated_but_reads_no_operational_data() {
    // A corrupt journal fails every projection. The readiness probe must still
    // answer, which proves it is served from the static capability manifest
    // rather than from a journal projection an unauthenticated caller could
    // force the process to run on every request.
    let app = fixture_app(
        b"not a journal\n".to_vec(),
        WebAccessPolicy::loopback_open(),
    );

    let health = app.clone().oneshot(get("/api/v1/health")).await.unwrap();

    assert_eq!(health.status(), StatusCode::OK);
    assert_security_headers(&health);
    let body = response_json(health).await;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["live_trading_enabled"], false);
    assert_eq!(
        body.as_object().unwrap().len(),
        3,
        "the probe reports schema, status, and authority only: {body}"
    );

    // The authenticated projections over the same journal do fail, confirming
    // the probe is not simply reading a healthy snapshot.
    let system = app.oneshot(get("/api/v1/system")).await.unwrap();
    assert_eq!(system.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn readiness_probe_is_rate_limited_even_without_a_token() {
    let app = fixture_app(Vec::new(), WebAccessPolicy::loopback_open());

    let mut limited = None;
    for _ in 0..=crypto_trading_web::WEB_REQUEST_LIMIT_PER_MINUTE {
        let response = app.clone().oneshot(get("/api/v1/health")).await.unwrap();
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            limited = Some(response);
            break;
        }
    }

    let limited = limited.expect("the unauthenticated probe must not be an unbounded route");
    assert!(
        limited.headers().contains_key(RETRY_AFTER),
        "backpressure tells the caller when to return"
    );
}
