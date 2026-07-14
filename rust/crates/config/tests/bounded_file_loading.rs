use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crypto_trading_config::{
    ConfigError, ConfigResult, EnvProvider, load_arbitrage_config, load_exchange_auth,
    load_exchange_auth_with_env, load_grid_config, load_monitor_config, load_price_alert_config,
    load_symbol_conversions, load_volume_maker_config,
};

const MAX_CONFIG_FILE_BYTES: usize = 1_048_576;

fn temp_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-config-{label}-{}-{nonce}.yaml",
        std::process::id()
    ))
}

#[test]
fn every_public_path_loader_rejects_files_over_one_mebibyte() {
    let path = temp_path("oversized");
    fs::write(&path, vec![b' '; MAX_CONFIG_FILE_BYTES + 1]).unwrap();

    let errors = [
        load_arbitrage_config(&path).unwrap_err(),
        load_exchange_auth(&path, "paper").unwrap_err(),
        load_grid_config(&path).unwrap_err(),
        load_monitor_config(&path).unwrap_err(),
        load_price_alert_config(&path).unwrap_err(),
        load_symbol_conversions(&path).unwrap_err(),
        load_volume_maker_config(&path).unwrap_err(),
    ];

    for error in errors {
        let message = error.to_string();
        assert!(message.contains("maximum is 1048576"), "{message}");
    }

    fs::remove_file(path).unwrap();
}

#[test]
fn every_public_path_loader_rejects_yaml_anchors_before_schema_parsing() {
    let path = temp_path("anchor");
    fs::write(
        &path,
        "defaults: &defaults\n  enabled: true\ncopy: *defaults\n",
    )
    .unwrap();

    let errors = [
        load_arbitrage_config(&path).unwrap_err(),
        load_exchange_auth(&path, "paper").unwrap_err(),
        load_grid_config(&path).unwrap_err(),
        load_monitor_config(&path).unwrap_err(),
        load_price_alert_config(&path).unwrap_err(),
        load_symbol_conversions(&path).unwrap_err(),
        load_volume_maker_config(&path).unwrap_err(),
    ];

    for error in errors {
        assert!(matches!(&error, ConfigError::Validation(_)), "{error:?}");
        assert!(error.to_string().contains("YAML anchor tokens"), "{error}");
    }

    fs::remove_file(path).unwrap();
}

#[derive(Debug, Clone, Copy)]
struct EmptyEnvironment;

impl EnvProvider for EmptyEnvironment {
    fn get(&self, _key: &str) -> Option<String> {
        None
    }
}

fn assert_path_loader_allows_block_scalar<T>(
    label: &str,
    fixture: &str,
    loader: impl FnOnce(&Path) -> ConfigResult<T>,
) {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(fixture);
    let mut yaml = fs::read_to_string(&fixture_path).unwrap();
    yaml.push_str("\n_guard_literal: >2-\n  *literal\n  &literal\n");
    let path = temp_path(label);
    fs::write(&path, yaml).unwrap();

    let result = loader(&path).map(|_| ());

    fs::remove_file(path).unwrap();
    assert!(result.is_ok(), "{fixture}: {result:?}");
}

#[test]
fn every_public_path_loader_allows_literal_tokens_inside_block_scalars() {
    assert_path_loader_allows_block_scalar(
        "arbitrage-block",
        "config/arbitrage/arbitrage_segmented.yaml",
        |path| load_arbitrage_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "auth-block",
        "config/exchanges/lighter_config.yaml",
        |path| load_exchange_auth_with_env(path, "lighter", &EmptyEnvironment),
    );
    assert_path_loader_allows_block_scalar(
        "grid-block",
        "config/grid/lighter-long-perp-btc.yaml",
        |path| load_grid_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "monitor-block",
        "config/arbitrage/monitor.yaml",
        |path| load_monitor_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "price-alert-block",
        "config/price_alert/binance_alert.yaml",
        |path| load_price_alert_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "symbol-conversion-block",
        "config/symbol_conversion.yaml",
        |path| load_symbol_conversions(path),
    );
    assert_path_loader_allows_block_scalar(
        "volume-maker-block",
        "config/volume_maker/lighter_volume_maker.yaml",
        |path| load_volume_maker_config(path),
    );
}
