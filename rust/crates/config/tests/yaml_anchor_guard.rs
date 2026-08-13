use crypto_trading_config::{
    ConfigError, ConfigResult, EnvProvider, load_arbitrage_config_from_str,
    load_exchange_auth_from_str, load_exchange_auth_from_str_with_env, load_grid_config_from_str,
    load_monitor_config_from_str, load_symbol_conversions_from_str,
    reject_yaml_anchors_and_aliases,
};

#[derive(Debug, Clone, Copy)]
struct EmptyEnvironment;

impl EnvProvider for EmptyEnvironment {
    fn get(&self, _key: &str) -> Option<String> {
        None
    }
}

fn assert_anchor_rejected<T>(result: ConfigResult<T>) {
    let error = result.err().expect("anchor input must be rejected");
    assert!(matches!(&error, ConfigError::Validation(_)), "{error:?}");
    assert!(error.to_string().contains("YAML anchor tokens"), "{error}");
}

#[test]
fn every_public_yaml_from_str_loader_rejects_anchors_before_deserialization() {
    let yaml = "defaults: &defaults\n  enabled: true\ncopy: *defaults\n";

    assert_anchor_rejected(load_arbitrage_config_from_str(yaml));
    assert_anchor_rejected(load_exchange_auth_from_str("paper", yaml));
    assert_anchor_rejected(load_exchange_auth_from_str_with_env(
        "paper",
        yaml,
        &EmptyEnvironment,
    ));
    assert_anchor_rejected(load_grid_config_from_str(yaml));
    assert_anchor_rejected(load_monitor_config_from_str(yaml));
    assert_anchor_rejected(load_symbol_conversions_from_str(yaml));
}

#[test]
fn public_guard_and_from_str_loader_allow_literal_block_scalar_tokens() {
    let yaml = r"
notes: |-
  *literal
  &literal
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
";

    reject_yaml_anchors_and_aliases(yaml).unwrap();
    load_grid_config_from_str(yaml).unwrap();
}

#[test]
fn from_str_loader_resumes_alias_rejection_after_block_scalar_dedent() {
    let yaml = r"
notes: >+
  *literal
  &literal

copy: *actual_alias
grid_system:
  exchange: paper
  symbol: BTC-USDC-PERP
  market_type: perpetual
  mode: fixed
  grid_interval: 10
  order_amount: 1
  lower_price: 100
  upper_price: 120
";

    let error = load_grid_config_from_str(yaml).unwrap_err();

    assert!(error.to_string().contains("YAML alias tokens"), "{error}");
}
