use std::{collections::HashMap, fs, path::PathBuf, str::FromStr};

use crypto_trading_config::{
    EnvProvider, GridMode, load_arbitrage_config_from_str, load_exchange_auth_from_str_with_env,
    load_grid_config_from_str, load_monitor_config_from_str,
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
    let yaml = include_str!("../../../config/grid/hyperliquid-long-perp-btc.yaml");
    let config = load_grid_config_from_str(yaml).unwrap();

    assert_eq!(config.exchange, "hyperliquid");
    assert_eq!(config.market_type, MarketType::Perpetual);
    assert_eq!(config.mode, GridMode::FollowLong);
    assert!(config.follow_grid_count.unwrap() > 0);
    assert_eq!(config.grid_interval.as_decimal(), Decimal::from(100));
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
fn grid_loader_requires_a_positive_increment_for_martingale_modes() {
    let base = r"
grid:
  exchange: backpack
  symbol: BTC_USDC_PERP
  grid_type: martingale_long
  grid_interval: 1
  order_amount: 0.5
  lower_price: 100
  upper_price: 110
";

    let missing = load_grid_config_from_str(base).unwrap_err();
    assert!(
        missing.to_string().contains("martingale_increment"),
        "{missing}"
    );

    let zero =
        load_grid_config_from_str(&format!("{base}  martingale_increment: 0\n")).unwrap_err();
    assert!(zero.to_string().contains("martingale_increment"), "{zero}");

    let config =
        load_grid_config_from_str(&format!("{base}  martingale_increment: 0.05\n")).unwrap();
    assert_eq!(config.mode, GridMode::MartingaleLong);
    assert_eq!(
        config.martingale_increment.unwrap().as_decimal(),
        Decimal::from_str("0.05").unwrap()
    );
}

#[test]
fn grid_protection_fields_stay_disabled_unless_their_legacy_flag_is_set() {
    // Values without enable flags mirror the checked-in legacy configs where a
    // subsystem is described but switched off.
    let yaml = r"
grid:
  exchange: backpack
  symbol: BTC_USDC_PERP
  grid_type: long
  grid_interval: 1
  order_amount: 0.5
  lower_price: 100
  upper_price: 110
  scalping_enabled: false
  scalping_trigger_percent: 40
  capital_protection_enabled: false
  capital_protection_trigger_percent: 30
  take_profit_enabled: false
  take_profit_percentage: 0.002
  price_lock_enabled: false
  price_lock_threshold: 125000.0
  stop_loss_protection_enabled: false
  stop_loss_trigger_percent: 95.0
  stop_loss_escape_timeout: 600
  stop_loss_apr_threshold: 45.0
";
    let config = load_grid_config_from_str(yaml).unwrap();
    assert_eq!(config.scalping_trigger_percent, None);
    assert_eq!(config.scalping_take_profit_grids, None);
    assert_eq!(config.capital_protection_trigger_percent, None);
    assert_eq!(config.take_profit_percentage, None);
    assert_eq!(config.price_lock_threshold, None);
    assert_eq!(config.stop_loss_trigger_percent, None);
    assert_eq!(config.stop_loss_escape_timeout, None);
    assert_eq!(config.stop_loss_apr_threshold, None);
}

#[test]
fn grid_protection_fields_load_with_legacy_defaults_when_enabled() {
    let yaml = r"
grid:
  exchange: backpack
  symbol: BTC_USDC_PERP
  grid_type: long
  grid_interval: 1
  order_amount: 0.5
  lower_price: 100
  upper_price: 110
  scalping_enabled: true
  scalping_trigger_percent: 40
  capital_protection_enabled: true
  take_profit_enabled: true
  take_profit_percentage: 0.002
  price_lock_enabled: true
  price_lock_threshold: 125
  stop_loss_protection_enabled: true
  stop_loss_trigger_percent: 95.0
  stop_loss_escape_timeout: 600
  stop_loss_apr_threshold: 45.0
";
    let config = load_grid_config_from_str(yaml).unwrap();
    assert_eq!(config.scalping_trigger_percent, Some(40));
    // Take-profit distance defaults to the legacy two grids.
    assert_eq!(config.scalping_take_profit_grids, Some(2));
    // Capital protection trigger defaults to the legacy 50%.
    assert_eq!(config.capital_protection_trigger_percent, Some(50));
    assert_eq!(
        config.take_profit_percentage,
        Some(Decimal::from_str("0.002").unwrap())
    );
    assert_eq!(
        config.price_lock_threshold.unwrap().as_decimal(),
        Decimal::from(125)
    );
    assert_eq!(
        config.stop_loss_trigger_percent,
        Some(Decimal::from_str("95.0").unwrap())
    );
    assert_eq!(config.stop_loss_escape_timeout, Some(600));
    assert_eq!(
        config.stop_loss_apr_threshold,
        Some(Decimal::from_str("45.0").unwrap())
    );
}

#[test]
fn grid_protection_validation_fails_closed_on_out_of_range_values() {
    let base = r"
grid:
  exchange: backpack
  symbol: BTC_USDC_PERP
  grid_type: long
  grid_interval: 1
  order_amount: 0.5
  lower_price: 100
  upper_price: 110
";
    for (extra, needle) in [
        (
            "  scalping_enabled: true\n  scalping_trigger_percent: 0\n",
            "scalping_trigger_percent",
        ),
        (
            "  scalping_enabled: true\n  scalping_trigger_percent: 101\n",
            "scalping_trigger_percent",
        ),
        (
            "  scalping_enabled: true\n  scalping_take_profit_grids: 0\n",
            "scalping_take_profit_grids",
        ),
        (
            "  capital_protection_enabled: true\n  capital_protection_trigger_percent: 101\n",
            "capital_protection_trigger_percent",
        ),
        (
            "  take_profit_enabled: true\n  take_profit_percentage: 0\n",
            "take_profit_percentage",
        ),
        ("  price_lock_enabled: true\n", "price_lock_threshold"),
        (
            "  stop_loss_protection_enabled: true\n  stop_loss_trigger_percent: 0\n",
            "stop_loss_trigger_percent",
        ),
        (
            "  stop_loss_protection_enabled: true\n  stop_loss_escape_timeout: 0\n",
            "stop_loss_escape_timeout",
        ),
        (
            "  stop_loss_protection_enabled: true\n  stop_loss_apr_threshold: -1\n",
            "stop_loss_apr_threshold",
        ),
    ] {
        let error = load_grid_config_from_str(&format!("{base}{extra}")).unwrap_err();
        assert!(error.to_string().contains(needle), "{extra}: {error}");
    }
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
health_check:
  data_timeout: 30
  max_pair_skew_ms: 250
future_monitor_section: { enabled: true }
"#,
    )
    .unwrap();
    assert_eq!(monitor.exchanges, ["lighter", "backpack"]);
    assert_eq!(monitor.min_spread_pct, Decimal::from_str("0.125").unwrap());
    assert_eq!(monitor.max_pair_skew_ms, 250);

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
        .insert("HYPERLIQUID_PRIVATE_KEY".into(), "env-secret".into());

    let nested = r"
exchange_id: hyperliquid
api_config:
  auth:
    private_key: yaml-secret
    wallet_address: yaml-wallet
";
    let auth = load_exchange_auth_from_str_with_env("hyperliquid", nested, &env).unwrap();
    assert_eq!(auth.private_key.expose_secret(), Some("env-secret"));
    assert_eq!(auth.wallet_address.as_deref(), Some("yaml-wallet"));
    // Hyperliquid derives API credentials from the wallet private key.
    assert_eq!(auth.api_key.expose_secret(), Some("env-secret"));
    assert_eq!(auth.api_secret.expose_secret(), Some("env-secret"));

    let flat = r"
binance:
  api_key: yaml-api-key
  extra_params:
    api_secret: yaml-api-secret
";
    let auth = load_exchange_auth_from_str_with_env("binance", flat, &TestEnv::default()).unwrap();
    assert_eq!(auth.api_key.expose_secret(), Some("yaml-api-key"));
    assert_eq!(auth.api_secret.expose_secret(), Some("yaml-api-secret"));
    assert!(!format!("{auth:?}").contains("yaml-api-key"));
}

#[test]
fn exchange_auth_blank_environment_values_do_not_override_yaml() {
    let yaml = r"
exchange_id: hyperliquid
api_config:
  auth:
    private_key: yaml-secret
    wallet_address: yaml-wallet
";

    let mut env = TestEnv::default();
    env.0.insert("HYPERLIQUID_PRIVATE_KEY".into(), "   ".into());
    env.0
        .insert("HYPERLIQUID_WALLET_ADDRESS".into(), "\t".into());

    let auth = load_exchange_auth_from_str_with_env("hyperliquid", yaml, &env).unwrap();
    assert_eq!(auth.private_key.expose_secret(), Some("yaml-secret"));
    assert_eq!(auth.wallet_address.as_deref(), Some("yaml-wallet"));
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
    assert!(loaded >= 4, "representative grid fixtures disappeared");
}

#[test]
fn checked_in_monitor_and_arbitrage_documents_load() {
    let root = repo_root();
    for relative in [
        "config/arbitrage/monitor_v2.yaml",
        "config/arbitrage/monitor-live-testnet.yaml",
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
fn arbitrage_history_decision_defaults_to_absent_and_parses_explicit_controls() {
    let absent = load_arbitrage_config_from_str(
        r"
mode: segmented
system_mode:
  monitor_only: true
",
    )
    .unwrap();
    assert_eq!(absent.history_decision, None);

    let config = load_arbitrage_config_from_str(
        r"
mode: segmented
system_mode:
  monitor_only: true
history_decision:
  enabled: true
  window_seconds: 7200
  min_samples: 12
  deviation_threshold_bps: 15
  funding_rate_annual_threshold_pct: 20
  spread_history_path: var/history/spread-history.jsonl
",
    )
    .unwrap();
    let history = config.history_decision.unwrap();
    assert!(history.enabled);
    assert_eq!(history.window_seconds, 7_200);
    assert_eq!(history.min_samples, 12);
    assert_eq!(history.deviation_threshold_bps.to_string(), "15");
    assert_eq!(history.funding_rate_annual_threshold_pct.to_string(), "20");
    assert_eq!(
        history.spread_history_path.as_deref(),
        Some("var/history/spread-history.jsonl")
    );

    // A bare mapping stays disabled and takes the Python-derived defaults
    // (min_data_points=10, funding annual threshold 10 %/year).
    let defaults = load_arbitrage_config_from_str(
        r"
mode: segmented
system_mode:
  monitor_only: true
history_decision:
  window_seconds: 600
",
    )
    .unwrap()
    .history_decision
    .unwrap();
    assert!(!defaults.enabled);
    assert_eq!(defaults.min_samples, 10);
    assert_eq!(defaults.deviation_threshold_bps.to_string(), "10");
    assert_eq!(defaults.funding_rate_annual_threshold_pct.to_string(), "10");
    assert_eq!(defaults.spread_history_path, None);
}

#[test]
fn arbitrage_history_decision_bounds_are_fail_closed() {
    for (yaml, expected) in [
        (
            r"
mode: segmented
history_decision:
  window_seconds: 0
",
            "window_seconds must be in 1..=86400",
        ),
        (
            r"
mode: segmented
history_decision:
  window_seconds: 86401
",
            "window_seconds must be in 1..=86400",
        ),
        (
            r"
mode: segmented
history_decision:
  min_samples: 0
",
            "min_samples must be in 1..=4096",
        ),
        (
            r"
mode: segmented
history_decision:
  min_samples: 4097
",
            "min_samples must be in 1..=4096",
        ),
        (
            r"
mode: segmented
history_decision:
  deviation_threshold_bps: 0
",
            "deviation_threshold_bps must be positive",
        ),
        (
            r"
mode: segmented
history_decision:
  funding_rate_annual_threshold_pct: -1
",
            "funding_rate_annual_threshold_pct must not be negative",
        ),
        (
            r#"
mode: segmented
history_decision:
  spread_history_path: "   "
"#,
            "spread_history_path must not be blank",
        ),
        (
            r"
mode: segmented
history_decision: 7
",
            "history_decision must be a mapping",
        ),
        (
            r#"
mode: segmented
history_decision:
  window_seconds: "soon"
"#,
            "window_seconds must be an unsigned integer",
        ),
    ] {
        let error = load_arbitrage_config_from_str(yaml).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}: {yaml}");
    }
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
        "exchanges: [lighter]\nsymbols: [BTC-USDC-PERP]\nhealth_check:\n  max_pair_skew_ms: 0\n",
        "exchanges: [lighter]\nsymbols: [BTC-USDC-PERP]\nhealth_check:\n  data_timeout: 1\n  max_pair_skew_ms: 1001\n",
        "exchanges: [lighter]\nsymbols: [BTC-USDC-PERP]\nhealth_check:\n  data_timeout: 120\n  max_pair_skew_ms: 60001\n",
    ] {
        assert!(load_monitor_config_from_str(yaml).is_err(), "{yaml}");
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
