//! Backward-compatible configuration loading for the legacy YAML files.

mod arbitrage;
mod auth;
mod error;
mod grid;
mod input;
mod monitor;
mod scanner;
mod supporting;
mod symbol_conversion;

pub use arbitrage::{
    ArbitrageConfig, ArbitrageHistoryDecisionConfig, ArbitrageSymbolConfig,
    MAX_ARBITRAGE_HISTORY_MIN_SAMPLES, MAX_ARBITRAGE_HISTORY_WINDOW_SECONDS, load_arbitrage_config,
    load_arbitrage_config_from_str,
};
pub use auth::{
    EnvProvider, ExchangeAuth, ProcessEnvironment, Secret, load_exchange_auth,
    load_exchange_auth_from_str, load_exchange_auth_from_str_with_env, load_exchange_auth_with_env,
};
pub use error::{ConfigError, ConfigResult};
pub use grid::{GridConfig, GridMode, load_grid_config, load_grid_config_from_str};
pub use input::{read_bounded_config, reject_yaml_anchors_and_aliases};
pub use monitor::{MonitorConfig, load_monitor_config, load_monitor_config_from_str};
pub use scanner::{
    MAX_SCANNER_CONFIG_APR_WINDOW_SECONDS, MAX_SCANNER_CONFIG_ROW_LIMIT,
    MAX_SCANNER_CONFIG_SYMBOLS, ScannerConfig, ScannerSymbolConfig, load_scanner_config,
    load_scanner_config_from_str,
};
pub use supporting::{
    PriceAlertConfig, PriceAlertSymbolConfig, PriceThresholdConfig, VolatilityAlertConfig,
    VolumeMakerConfig, load_price_alert_config, load_price_alert_config_from_str,
    load_volume_maker_config, load_volume_maker_config_from_str,
};
pub use symbol_conversion::{
    SymbolConversions, load_symbol_conversions, load_symbol_conversions_from_str,
};
