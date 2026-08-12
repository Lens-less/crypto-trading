use serde_yaml::Value;

#[test]
fn checked_in_alerts_cover_the_minimum_live_failure_modes() {
    let rules: Value =
        serde_yaml::from_str(include_str!("../../../../deploy/prometheus-alerts.yml"))
            .expect("checked-in Prometheus alert rules must be valid YAML");

    let checked_in_rules = rules["groups"][0]["rules"]
        .as_sequence()
        .expect("the safety group must contain alert rules");
    let expected = [
        (
            "CryptoTradingProcessDown",
            "absent(crypto_trading_process_up) == 1 or crypto_trading_process_up == 0",
        ),
        (
            "CryptoTradingMarketStreamStale",
            "crypto_trading_stream_observed{stream=\"market\"} == 0 or (time() - crypto_trading_stream_last_frame_timestamp_seconds{stream=\"market\"}) > 5",
        ),
        (
            "CryptoTradingUserDataStreamStale",
            "crypto_trading_stream_observed{stream=\"user_data\"} == 0 or (time() - crypto_trading_stream_last_frame_timestamp_seconds{stream=\"user_data\"}) > 60",
        ),
        (
            "CryptoTradingRecoveryRequired",
            "crypto_trading_owner_phase{phase=\"recovery_required\"} == 1",
        ),
        (
            "CryptoTradingJournalAppendFailure",
            "increase(crypto_trading_journal_append_failure_total[5m]) > 0",
        ),
        (
            "CryptoTradingClockSkew",
            "abs(crypto_trading_clock_skew_milliseconds) > 1000",
        ),
        (
            "CryptoTradingRateLimited",
            "increase(crypto_trading_rest_status_total{class=\"429\"}[5m]) > 0",
        ),
    ];

    assert_eq!(
        checked_in_rules.len(),
        expected.len(),
        "unexpected alert-policy drift; update the authoritative YAML and its operator documentation together"
    );
    for (alert, expression) in expected {
        let rule = checked_in_rules
            .iter()
            .find(|rule| rule["alert"].as_str() == Some(alert))
            .unwrap_or_else(|| panic!("missing required alert {alert}"));
        assert_eq!(
            rule["expr"].as_str(),
            Some(expression),
            "expression drift for {alert}"
        );
    }
}
