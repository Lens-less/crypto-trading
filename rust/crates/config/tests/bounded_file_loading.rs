use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crypto_trading_config::{
    ConfigError, ConfigResult, EnvProvider, load_arbitrage_config, load_arbitrage_config_from_str,
    load_exchange_auth, load_exchange_auth_from_str, load_exchange_auth_from_str_with_env,
    load_exchange_auth_with_env, load_grid_config, load_grid_config_from_str, load_monitor_config,
    load_monitor_config_from_str, load_symbol_conversions, load_symbol_conversions_from_str,
    read_bounded_config,
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
        read_bounded_config(&path).unwrap_err(),
        load_symbol_conversions(&path).unwrap_err(),
    ];

    for error in errors {
        let message = error.to_string();
        assert!(message.contains("maximum is 1048576"), "{message}");
    }

    fs::remove_file(path).unwrap();
}

fn pad_yaml_to_size(mut yaml: String, size: usize) -> String {
    assert!(yaml.len() <= size, "fixture larger than target size");
    yaml.push_str(&" ".repeat(size - yaml.len()));
    yaml
}

fn assert_path_and_from_str_loader_boundary_behavior(
    label: &str,
    fixture: &str,
    path_loader: impl Fn(&Path) -> ConfigResult<()>,
    from_str_loader: impl Fn(&str) -> ConfigResult<()>,
) {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(fixture);
    let fixture_yaml = fs::read_to_string(&fixture_path).unwrap();
    let exact = pad_yaml_to_size(fixture_yaml, MAX_CONFIG_FILE_BYTES);
    assert_eq!(exact.len(), MAX_CONFIG_FILE_BYTES);

    let path = temp_path(label);
    fs::write(&path, &exact).unwrap();
    assert!(
        path_loader(&path).is_ok(),
        "{fixture} path loader rejected exact boundary"
    );
    assert!(
        from_str_loader(&exact).is_ok(),
        "{fixture} from_str loader rejected exact boundary"
    );

    let oversized = format!("{exact} ");
    let path_error = path_loader(&path_with_contents(&path, &oversized)).unwrap_err();
    let from_str_error = from_str_loader(&oversized).unwrap_err();

    for error in [path_error, from_str_error] {
        let message = error.to_string();
        assert!(message.contains("maximum is 1048576"), "{message}");
    }

    fs::remove_file(path).unwrap();
}

fn path_with_contents(path: &Path, contents: &str) -> PathBuf {
    fs::write(path, contents).unwrap();
    path.to_path_buf()
}

#[test]
fn raw_bounded_reader_accepts_the_exact_byte_limit() {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("config/grid/hyperliquid-long-perp-btc.yaml");
    let exact = pad_yaml_to_size(
        fs::read_to_string(&fixture_path).unwrap(),
        MAX_CONFIG_FILE_BYTES,
    );
    let path = temp_path("raw-reader-boundary");
    fs::write(&path, &exact).unwrap();

    let result = read_bounded_config(&path).unwrap();

    assert_eq!(result.len(), MAX_CONFIG_FILE_BYTES);
    fs::remove_file(path).unwrap();
}

#[test]
fn every_public_from_str_loader_matches_path_loader_size_boundaries() {
    assert_path_and_from_str_loader_boundary_behavior(
        "arbitrage-from-str-boundary",
        "config/arbitrage/arbitrage_segmented.yaml",
        |path| load_arbitrage_config(path).map(|_| ()),
        |yaml| load_arbitrage_config_from_str(yaml).map(|_| ()),
    );
    assert_path_and_from_str_loader_boundary_behavior(
        "auth-from-str-boundary",
        "config/exchanges/hyperliquid_config.yaml",
        |path| load_exchange_auth(path, "hyperliquid").map(|_| ()),
        |yaml| load_exchange_auth_from_str("hyperliquid", yaml).map(|_| ()),
    );
    assert_path_and_from_str_loader_boundary_behavior(
        "auth-from-str-with-env-boundary",
        "config/exchanges/hyperliquid_config.yaml",
        |path| load_exchange_auth_with_env(path, "hyperliquid", &EmptyEnvironment).map(|_| ()),
        |yaml| {
            load_exchange_auth_from_str_with_env("hyperliquid", yaml, &EmptyEnvironment).map(|_| ())
        },
    );
    assert_path_and_from_str_loader_boundary_behavior(
        "grid-from-str-boundary",
        "config/grid/hyperliquid-long-perp-btc.yaml",
        |path| load_grid_config(path).map(|_| ()),
        |yaml| load_grid_config_from_str(yaml).map(|_| ()),
    );
    assert_path_and_from_str_loader_boundary_behavior(
        "monitor-from-str-boundary",
        "config/arbitrage/monitor_v2.yaml",
        |path| load_monitor_config(path).map(|_| ()),
        |yaml| load_monitor_config_from_str(yaml).map(|_| ()),
    );
    assert_path_and_from_str_loader_boundary_behavior(
        "symbol-conversion-from-str-boundary",
        "config/symbol_conversion.yaml",
        |path| load_symbol_conversions(path).map(|_| ()),
        |yaml| load_symbol_conversions_from_str(yaml).map(|_| ()),
    );
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
        read_bounded_config(&path).unwrap_err(),
        load_symbol_conversions(&path).unwrap_err(),
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
        "config/exchanges/hyperliquid_config.yaml",
        |path| load_exchange_auth_with_env(path, "hyperliquid", &EmptyEnvironment),
    );
    assert_path_loader_allows_block_scalar(
        "grid-block",
        "config/grid/hyperliquid-long-perp-btc.yaml",
        |path| load_grid_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "monitor-block",
        "config/arbitrage/monitor_v2.yaml",
        |path| load_monitor_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "raw-reader-block",
        "config/grid/hyperliquid-long-perp-btc.yaml",
        |path| read_bounded_config(path),
    );
    assert_path_loader_allows_block_scalar(
        "symbol-conversion-block",
        "config/symbol_conversion.yaml",
        |path| load_symbol_conversions(path),
    );
}
