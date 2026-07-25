use std::{collections::HashMap, fs, path::PathBuf, str::FromStr};

use crypto_trading_config::{
    ConfigError, EnvProvider, GridMode, load_arbitrage_config_from_str,
    load_exchange_auth_from_str_with_env, load_grid_config_from_str, load_monitor_config_from_str,
};
use crypto_trading_domain::{MarketType, Symbol};
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
fn exchange_auth_env_unsigned_integer_overrides_reject_invalid_numbers() {
    let yaml = r"
exchange_id: lighter
api_config:
  auth:
    api_key_private_key: yaml-secret
    account_index: 2
    api_key_index: 3
";

    for (key, value) in [
        ("LIGHTER_ACCOUNT_INDEX", "-1"),
        ("LIGHTER_API_KEY_INDEX", "18446744073709551616"),
    ] {
        let mut env = TestEnv::default();
        env.0.insert(key.into(), value.into());

        let error = load_exchange_auth_from_str_with_env("lighter", yaml, &env).unwrap_err();
        assert!(matches!(
            error,
            ConfigError::InvalidEnvironmentNumber { key: ref actual_key } if actual_key == key
        ));
    }
}

#[test]
fn exchange_auth_blank_environment_values_do_not_override_yaml() {
    let yaml = r"
exchange_id: lighter
api_config:
  auth:
    api_key_private_key: yaml-secret
    account_index: 2
    api_key_index: 3
";

    let mut env = TestEnv::default();
    env.0
        .insert("LIGHTER_API_KEY_PRIVATE_KEY".into(), "   ".into());
    env.0.insert("LIGHTER_ACCOUNT_INDEX".into(), "\t".into());
    env.0.insert("LIGHTER_API_KEY_INDEX".into(), "\n".into());

    let auth = load_exchange_auth_from_str_with_env("lighter", yaml, &env).unwrap();
    assert_eq!(
        auth.api_key_private_key.expose_secret(),
        Some("yaml-secret")
    );
    assert_eq!(auth.account_index, Some(2));
    assert_eq!(auth.api_key_index, Some(3));
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
        "config/arbitrage/monitor_lighter_eth_spot.yaml",
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

#[test]
fn arbitrage_rejects_mistyped_operator_controls() {
    let error = load_arbitrage_config_from_str(
        r#"
arbitrage_decision:
  enabled: "false"
system_mode:
  monitor_only: false
default_config:
  grid_config:
    initial_spread_threshold: 0.1
    grid_step: 0.1
    max_segments: 2
  quantity_config:
    base_quantity: 1
"#,
    )
    .unwrap_err();

    assert!(error.to_string().contains("enabled must be a boolean"));

    let error = load_arbitrage_config_from_str(
        r"
system_mode:
  monitor_only: false
default_config:
  grid_config:
    initial_spread_threshold: 0.1
    grid_step: 0.1
    max_segments: 2
  quantity_config:
    base_quantity: 1
symbol_configs:
  BTC-USDC-PERP:
    grid_config:
      grid_step: 0.2
",
    )
    .unwrap_err();
    assert!(error.to_string().contains("enabled is required"));
}

#[test]
fn arbitrage_execution_controls_are_explicit_and_fail_closed() {
    for (yaml, expected) in [
        (
            r"
mode: segmented
system_mode:
  monitor_only: false
",
            "disabled",
        ),
        (
            r"
mode: segmented
enabled: false
system_mode:
  monitor_only: false
",
            "disabled",
        ),
        (
            r"
mode: segmented
enabled: true
system_mode:
  monitor_only: true
",
            "monitor-only",
        ),
        (
            r"
mode: unified
enabled: true
system_mode:
  monitor_only: false
",
            "expected segmented",
        ),
    ] {
        let config = load_arbitrage_config_from_str(yaml).unwrap();
        let error = config.validate_execution_controls().unwrap_err();
        assert!(error.to_string().contains(expected), "{error}: {yaml}");
    }
}

#[test]
fn arbitrage_execution_controls_require_non_empty_operator_allowlists() {
    for (yaml, expected) in [
        (
            r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: []
symbols: [BTC-USDC-PERP]
",
            "exchange allowlist must not be empty",
        ),
        (
            r#"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [lighter, "   "]
symbols: [BTC-USDC-PERP]
"#,
            "exchange allowlist entries must not be blank",
        ),
        (
            r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [lighter]
symbols: []
",
            "symbol allowlist must not be empty",
        ),
    ] {
        let config = load_arbitrage_config_from_str(yaml).unwrap();
        let error = config.validate_execution_controls().unwrap_err();
        assert!(error.to_string().contains(expected), "{error}: {yaml}");
    }
}

#[test]
fn arbitrage_execution_controls_require_enabled_symbol_scope_and_positive_risk_limits() {
    let config = load_arbitrage_config_from_str(
        r"
mode: segmented
enabled: true
system_mode:
  monitor_only: false
exchanges: [lighter, paradex]
symbols: [BTC-USDC-PERP]
symbol_configs:
  BTC-USDC-PERP:
    enabled: true
",
    )
    .unwrap();

    let error = config.validate_execution_controls().unwrap_err();
    assert!(error.to_string().contains("max_position_value"), "{error}");

    let mut config = config;
    config.max_position_value = Some(Decimal::from(10));
    config
        .symbol_configs
        .get_mut(&Symbol::new("BTC-USDC-PERP").unwrap())
        .unwrap()
        .enabled = false;

    let error = config.validate_execution_controls().unwrap_err();
    assert!(
        error.to_string().contains("enabled symbol strategy"),
        "{error}"
    );

    config
        .symbol_configs
        .get_mut(&Symbol::new("BTC-USDC-PERP").unwrap())
        .unwrap()
        .enabled = true;
    config.max_position_value = Some(Decimal::ZERO);

    let error = config.validate_execution_controls().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("max_position_value must be positive"),
        "{error}"
    );
}

#[test]
fn symbol_resolution_cannot_bypass_disabled_top_level_controls() {
    let config = load_arbitrage_config_from_str(
        r"
mode: segmented
enabled: false
system_mode:
  monitor_only: false
symbol_configs:
  BTC-USDC-PERP:
    enabled: true
",
    )
    .unwrap();

    let error = config
        .resolve_for_strategy(&Symbol::new("BTC-USDC-PERP").unwrap())
        .unwrap_err();

    assert!(error.to_string().contains("disabled"), "{error}");
}

#[test]
fn arbitrage_resolves_enabled_symbol_overrides_for_the_strategy() {
    let config = load_arbitrage_config_from_str(
        r"
enabled: true
system_mode:
  monitor_only: false
exchanges: [lighter, paradex]
symbols: [PAXG-USD-PERP, BTC-USDC-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.10
    grid_step: 0.10
    max_segments: 2
  quantity_config:
    base_quantity: 1
  risk_config:
    max_position_value: 1000
symbol_configs:
  PAXG-USD-PERP:
    enabled: true
    grid_config:
      initial_spread_threshold: 0.03
      grid_step: 0.04
      max_segments: 5
    quantity_config:
      base_quantity: 0.04
    risk_config:
      max_position_value: 250
",
    )
    .unwrap();
    let key = Symbol::new("PAXG-USD-PERP").unwrap();

    let effective = config.resolve_for_strategy(&key).unwrap();

    assert_eq!(effective.min_spread_pct, Decimal::from_str("0.03").unwrap());
    assert_eq!(effective.grid_step_pct, Decimal::from_str("0.04").unwrap());
    assert_eq!(effective.max_segments, 5);
    assert_eq!(
        effective.base_quantity.as_decimal(),
        Decimal::from_str("0.04").unwrap()
    );
    assert_eq!(effective.max_position_value, Some(Decimal::from(250)));
}

#[test]
fn arbitrage_resolve_allows_symbol_risk_override_without_top_level_cap() {
    let mut config = load_arbitrage_config_from_str(
        r"
enabled: true
system_mode:
  monitor_only: false
exchanges: [lighter, paradex]
symbols: [AAA-PERP, BBB-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.10
    grid_step: 0.10
    max_segments: 2
  quantity_config:
    base_quantity: 1
symbol_configs:
  CROSS_PAIR:
    enabled: true
    risk_config:
      max_position_value: 250
",
    )
    .unwrap();
    let key = Symbol::new("CROSS_PAIR").unwrap();

    config.max_position_value = Some(Decimal::ZERO);
    let error = config.validate_execution_controls().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("max_position_value must be positive"),
        "{error}"
    );

    let effective = config.resolve_for_strategy(&key).unwrap();

    assert_eq!(
        effective.symbols,
        vec![
            Symbol::new("AAA-PERP").unwrap(),
            Symbol::new("BBB-PERP").unwrap()
        ]
    );
    assert_eq!(effective.max_position_value, Some(Decimal::from(250)));
}

#[test]
fn arbitrage_rejects_non_positive_position_value_limits() {
    for value in ["0", "-1"] {
        let yaml = format!(
            r"
default_config:
  risk_config:
    max_position_value: {value}
"
        );
        let error = load_arbitrage_config_from_str(&yaml).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("max_position_value must be positive"),
            "{error}"
        );
    }
}

#[test]
fn arbitrage_rejects_missing_and_disabled_strategy_keys() {
    let config = load_arbitrage_config_from_str(
        r"
enabled: true
system_mode:
  monitor_only: false
exchanges: [lighter, paradex]
symbols: [PAXG-USD-PERP]
default_config:
  grid_config:
    initial_spread_threshold: 0.10
    grid_step: 0.10
    max_segments: 2
  quantity_config:
    base_quantity: 1
symbol_configs:
  PAXG-USD-PERP:
    enabled: false
  BTC-USDC-PERP:
    enabled: true
",
    )
    .unwrap();

    let disabled = config
        .resolve_for_strategy(&Symbol::new("PAXG-USD-PERP").unwrap())
        .unwrap_err();
    assert!(disabled.to_string().contains("disabled"));

    let missing = config
        .resolve_for_strategy(&Symbol::new("ETH-USDC-PERP").unwrap())
        .unwrap_err();
    assert!(missing.to_string().contains("not configured"));
}

#[test]
fn monitor_rejects_empty_universes_and_zero_intervals() {
    for yaml in [
        "exchanges: []\nsymbols: [BTC-USDC-PERP]\n",
        "exchanges: [lighter]\nsymbols: []\n",
        "exchanges: [lighter]\nsymbols: [BTC-USDC-PERP]\nperformance:\n  analysis_interval_ms: 0\n",
    ] {
        assert!(load_monitor_config_from_str(yaml).is_err(), "{yaml}");
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
