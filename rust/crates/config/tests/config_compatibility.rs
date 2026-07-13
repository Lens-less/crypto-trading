use std::{collections::HashMap, fs, path::PathBuf, str::FromStr};

use crypto_trading_config::{
    EnvProvider, GridMode, load_arbitrage_config_from_str, load_exchange_auth_from_str_with_env,
    load_grid_config_from_str, load_monitor_config_from_str,
};
use crypto_trading_domain::MarketType;
use rust_decimal::Decimal;

#[derive(Default)]
struct TestEnv(HashMap<String, String>);

impl EnvProvider for TestEnv {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

#[test]
fn existing_follow_grid_yaml_loads_and_ignores_unknown_fields() {
    let yaml = include_str!("../../../config/grid/lighter-long-perp-eth.yaml");
    let config = load_grid_config_from_str(yaml).unwrap();

    assert_eq!(config.exchange, "lighter");
    assert_eq!(config.market_type, MarketType::Perpetual);
    assert_eq!(config.mode, GridMode::FollowLong);
    assert!(config.follow_grid_count.unwrap() > 0);
    assert_eq!(
        config.grid_interval.as_decimal(),
        Decimal::from_str("0.89").unwrap()
    );
}

#[test]
fn grid_loader_accepts_fixed_mode_and_legacy_field_aliases() {
    let yaml = r#"
grid:
  exchange_name: backpack
  pair: BTC_USDC_PERP
  strategy: fixed
  market: perpetual
  grid_spacing: "100.25"
  order_quantity: 0.001
  price_range:
    lower_price: 90000
    upper_price: 110000
  future_field: safely ignored
"#;

    let config = load_grid_config_from_str(yaml).unwrap();
    assert_eq!(config.mode, GridMode::FixedLong);
    assert_eq!(config.market_type, MarketType::Perpetual);
    assert_eq!(
        config.lower_price.unwrap().as_decimal(),
        Decimal::from(90_000)
    );
    assert_eq!(
        config.upper_price.unwrap().as_decimal(),
        Decimal::from(110_000)
    );
}

#[test]
fn monitor_and_arbitrage_top_level_documents_keep_their_operator_controls() {
    let monitor = load_monitor_config_from_str(
        r#"
exchanges: [lighter, backpack]
symbols: [BTC-USDC-PERP]
thresholds:
  min_spread_pct: 0.125
  min_funding_rate_diff: "0.01"
future_monitor_section: { enabled: true }
"#,
    )
    .unwrap();
    assert_eq!(monitor.exchanges, ["lighter", "backpack"]);
    assert_eq!(monitor.min_spread_pct, Decimal::from_str("0.125").unwrap());

    let arbitrage = load_arbitrage_config_from_str(
        r"
system_mode:
  monitor_only: true
arbitrage_decision:
  thresholds:
    spread_arbitrage_threshold: 0.02
arbitrage_execution:
  quantity_config:
    default:
      single_order_quantity: 0.10
new_risk_knob: enabled
",
    )
    .unwrap();
    assert!(arbitrage.monitor_only);
    assert_eq!(arbitrage.min_spread_pct, Decimal::from_str("0.02").unwrap());
    assert_eq!(arbitrage.grid_step_pct, Decimal::from_str("0.02").unwrap());
    assert_eq!(arbitrage.max_segments, 1);
    assert_eq!(
        arbitrage.base_quantity.as_decimal(),
        Decimal::from_str("0.10").unwrap()
    );
}

#[test]
fn exchange_auth_reads_flat_or_nested_yaml_and_environment_wins() {
    let mut env = TestEnv::default();
    env.0
        .insert("LIGHTER_API_KEY_PRIVATE_KEY".into(), "env-secret".into());
    env.0.insert("LIGHTER_ACCOUNT_INDEX".into(), "17".into());

    let nested = r"
exchange_id: lighter
api_config:
  auth:
    api_key_private_key: yaml-secret
    account_index: 2
    api_key_index: 3
";
    let auth = load_exchange_auth_from_str_with_env("lighter", nested, &env).unwrap();
    assert_eq!(auth.api_key_private_key.expose_secret(), Some("env-secret"));
    assert_eq!(auth.account_index, Some(17));
    assert_eq!(auth.api_key_index, Some(3));

    let flat = r"
paradex:
  api_key: yaml-api-key
  extra_params:
    jwt_token: yaml-jwt
    l2_address: yaml-address
";
    let auth = load_exchange_auth_from_str_with_env("paradex", flat, &TestEnv::default()).unwrap();
    assert_eq!(auth.api_key.expose_secret(), Some("yaml-api-key"));
    assert_eq!(auth.jwt_token.expose_secret(), Some("yaml-jwt"));
    assert_eq!(auth.l2_address.as_deref(), Some("yaml-address"));
    assert!(!format!("{auth:?}").contains("yaml-api-key"));
}

#[test]
fn every_checked_in_grid_configuration_loads() {
    let directory = repo_root().join("config/grid");
    let mut loaded = 0;
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("yaml") {
            continue;
        }
        let yaml = fs::read_to_string(&path).unwrap();
        load_grid_config_from_str(&yaml)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        loaded += 1;
    }
    assert!(loaded >= 10, "representative grid fixtures disappeared");
}

#[test]
fn checked_in_monitor_and_arbitrage_documents_load() {
    let root = repo_root();
    for relative in [
        "config/arbitrage/monitor.yaml",
        "config/arbitrage/monitor_v2.yaml",
        "config/arbitrage/monitor_lighter_gold.yaml",
    ] {
        let yaml = fs::read_to_string(root.join(relative)).unwrap();
        load_monitor_config_from_str(&yaml).unwrap_or_else(|error| panic!("{relative}: {error}"));
    }
    for relative in [
        "config/arbitrage/arbitrage_unified.yaml",
        "config/arbitrage/arbitrage_segmented.yaml",
    ] {
        let yaml = fs::read_to_string(root.join(relative)).unwrap();
        let config = load_arbitrage_config_from_str(&yaml)
            .unwrap_or_else(|error| panic!("{relative}: {error}"));
        if relative.ends_with("segmented.yaml") {
            assert_eq!(config.grid_step_pct, Decimal::from_str("0.03").unwrap());
            assert_eq!(config.max_segments, 5);
        }
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
