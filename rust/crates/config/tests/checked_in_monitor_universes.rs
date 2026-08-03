use std::{fs, path::PathBuf};

use crypto_trading_config::load_monitor_config;

fn repository_config(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("config")
        .join(relative)
}

#[test]
fn checked_in_monitor_profiles_declare_pair_skew_explicitly() {
    for relative in [
        "arbitrage/monitor.yaml",
        "arbitrage/monitor_lighter_eth_spot.yaml",
        "arbitrage/monitor_lighter_gold.yaml",
        "arbitrage/monitor_lighter_multi_btc.yaml",
        "arbitrage/monitor_paradex_lighter_btc.yaml",
        "arbitrage/monitor_v2.yaml",
        "arbitrage/paper-monitor-eth.yaml",
    ] {
        let path = repository_config(relative);
        let yaml = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"));
        assert!(
            yaml.lines()
                .any(|line| line.trim_start().starts_with("max_pair_skew_ms:")),
            "{relative} must declare its cross-venue pair-skew policy"
        );
        assert!(
            load_monitor_config(&path).is_ok(),
            "{relative} must remain loadable"
        );
    }
}

#[test]
fn specialized_monitor_configs_have_explicit_non_empty_universes() {
    for relative in [
        "arbitrage/monitor_lighter_eth_spot.yaml",
        "arbitrage/monitor_lighter_gold.yaml",
    ] {
        let path = repository_config(relative);
        let config = load_monitor_config(&path)
            .unwrap_or_else(|error| panic!("{relative} must be executable monitor input: {error}"));

        assert!(
            !config.symbols.is_empty(),
            "{relative} must not depend on an implicit companion-file merge"
        );
    }
}
