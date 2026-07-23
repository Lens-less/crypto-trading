use std::path::PathBuf;

use crypto_trading_config::load_monitor_config;

fn repository_config(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("config")
        .join(relative)
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
