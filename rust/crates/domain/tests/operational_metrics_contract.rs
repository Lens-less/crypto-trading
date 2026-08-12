use crypto_trading_domain::{
    OperationalOwnerPhase, OperationalRestObservation, OperationalStreamKind,
    operational_metrics_snapshot, record_journal_append,
    record_operational_clock_skew_milliseconds, record_operational_rest_response,
    record_operational_rest_transport_error, record_stream_frame, record_stream_gap,
    record_stream_queue_drop, record_stream_reconnect, render_prometheus_metrics,
    set_operational_owner_phase,
};
use std::sync::Mutex;

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn operational_metrics_are_monotonic_and_render_only_fixed_labels() {
    let _guard = TEST_LOCK.lock().unwrap();
    let before = operational_metrics_snapshot();

    record_stream_frame(OperationalStreamKind::Market, 7, 1_800_000_000);
    record_stream_frame(OperationalStreamKind::Market, 6, 1_799_999_999);
    record_stream_reconnect(OperationalStreamKind::Market);
    record_stream_gap(OperationalStreamKind::Market);
    record_stream_queue_drop(OperationalStreamKind::Market, 3);
    record_operational_rest_response(OperationalRestObservation {
        latency_micros: 17_000,
        status: 429,
        used_weight: Some(1_100),
        order_count: Some(9),
        retry_after_unix_seconds: Some(1_800_000_030),
    });
    record_operational_rest_transport_error(19_000);
    record_operational_clock_skew_milliseconds(-321);
    record_journal_append(2_500, false);
    set_operational_owner_phase(OperationalOwnerPhase::RecoveryRequired);

    let snapshot = operational_metrics_snapshot();
    assert!(snapshot.market_stream.reconnect_total > before.market_stream.reconnect_total);
    assert!(snapshot.market_stream.gap_total > before.market_stream.gap_total);
    assert!(snapshot.market_stream.queue_drop_total >= before.market_stream.queue_drop_total + 3);
    assert_eq!(snapshot.market_stream.generation, 7);
    assert_eq!(
        snapshot.market_stream.last_frame_unix_seconds,
        1_800_000_000
    );
    assert!(snapshot.rest.status_429_total > before.rest.status_429_total);
    assert!(snapshot.rest.transport_error_total > before.rest.transport_error_total);
    assert_eq!(snapshot.rest.used_weight, 1_100);
    assert_eq!(snapshot.rest.order_count, 9);
    assert_eq!(
        snapshot.owner_phase,
        OperationalOwnerPhase::RecoveryRequired
    );

    let rendered = render_prometheus_metrics(&snapshot, 1_800_000_010);
    for expected in [
        "crypto_trading_process_up 1",
        "crypto_trading_stream_generation{stream=\"market\"} 7",
        "crypto_trading_stream_age_seconds{stream=\"market\"} 10",
        "crypto_trading_stream_reconnect_total{stream=\"market\"}",
        "crypto_trading_rest_status_total{class=\"429\"}",
        "crypto_trading_binance_used_weight 1100",
        "crypto_trading_clock_skew_milliseconds -321",
        "crypto_trading_owner_phase{phase=\"recovery_required\"} 1",
        "crypto_trading_journal_append_failure_total",
    ] {
        assert!(
            rendered.contains(expected),
            "missing metric: {expected}\n{rendered}"
        );
    }
    assert!(!rendered.contains("api_key"));
    assert!(!rendered.contains("account_id"));
}

#[test]
fn owner_recovery_counter_only_advances_on_entry() {
    let _guard = TEST_LOCK.lock().unwrap();
    set_operational_owner_phase(OperationalOwnerPhase::ReadyUnarmed);
    let before = operational_metrics_snapshot().owner_recovery_required_total;
    set_operational_owner_phase(OperationalOwnerPhase::RecoveryRequired);
    set_operational_owner_phase(OperationalOwnerPhase::RecoveryRequired);
    let after = operational_metrics_snapshot().owner_recovery_required_total;
    assert_eq!(after, before + 1);
}
