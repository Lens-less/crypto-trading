use std::path::Path;

use crypto_trading_domain::Symbol;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorConfig {
    pub exchanges: Vec<String>,
    pub symbols: Vec<Symbol>,
    pub min_spread_pct: Decimal,
    pub min_funding_rate_diff: Decimal,
    pub ws_ping_interval: u64,
    pub ws_reconnect_delay: u64,
    pub ws_max_reconnect_attempts: u32,
    pub analysis_interval_ms: u64,
    pub ui_refresh_interval_ms: u64,
    pub health_check_interval: u64,
    pub data_timeout_seconds: u64,
}

#[derive(Debug, Default, Deserialize)]
struct RawMonitorConfig {
    #[serde(default)]
    exchanges: Vec<String>,
    #[serde(default)]
    symbols: Vec<Symbol>,
    #[serde(default)]
    thresholds: Thresholds,
    #[serde(default)]
    websocket: Websocket,
    #[serde(default)]
    performance: Performance,
    #[serde(default)]
    health_check: HealthCheck,
}

#[derive(Debug, Deserialize)]
struct Thresholds {
    #[serde(default = "default_min_spread")]
    min_spread_pct: Decimal,
    #[serde(default = "default_min_funding")]
    min_funding_rate_diff: Decimal,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            min_spread_pct: default_min_spread(),
            min_funding_rate_diff: default_min_funding(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Websocket {
    #[serde(default = "default_ping")]
    ping_interval: u64,
    #[serde(default = "default_reconnect_delay")]
    reconnect_delay: u64,
    #[serde(default = "default_reconnect_attempts")]
    max_reconnect_attempts: u32,
}

impl Default for Websocket {
    fn default() -> Self {
        Self {
            ping_interval: default_ping(),
            reconnect_delay: default_reconnect_delay(),
            max_reconnect_attempts: default_reconnect_attempts(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Performance {
    #[serde(default = "default_analysis_interval")]
    analysis_interval_ms: u64,
    #[serde(default = "default_ui_interval")]
    ui_refresh_interval_ms: u64,
}

impl Default for Performance {
    fn default() -> Self {
        Self {
            analysis_interval_ms: default_analysis_interval(),
            ui_refresh_interval_ms: default_ui_interval(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct HealthCheck {
    #[serde(default = "default_health_interval", alias = "health_check_interval")]
    interval: u64,
    #[serde(default = "default_data_timeout", alias = "data_timeout_seconds")]
    data_timeout: u64,
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self {
            interval: default_health_interval(),
            data_timeout: default_data_timeout(),
        }
    }
}

fn default_min_spread() -> Decimal {
    Decimal::new(1, 1)
}

fn default_min_funding() -> Decimal {
    Decimal::new(1, 2)
}

const fn default_ping() -> u64 {
    30
}

const fn default_reconnect_delay() -> u64 {
    5
}

const fn default_reconnect_attempts() -> u32 {
    5
}

const fn default_analysis_interval() -> u64 {
    10
}

const fn default_ui_interval() -> u64 {
    1_000
}

const fn default_health_interval() -> u64 {
    10
}

const fn default_data_timeout() -> u64 {
    30
}

/// Loads an arbitrage-monitor configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_monitor_config(path: impl AsRef<Path>) -> ConfigResult<MonitorConfig> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_monitor_config_from_str(&yaml)
}

/// Parses an arbitrage-monitor configuration document.
///
/// # Errors
///
/// Returns an error if YAML values are malformed or fail validation.
pub fn load_monitor_config_from_str(yaml: &str) -> ConfigResult<MonitorConfig> {
    let raw: RawMonitorConfig = parse_yaml(yaml)?;
    if raw.exchanges.is_empty() {
        return Err(ConfigError::Validation(
            "monitor must configure at least one exchange".to_owned(),
        ));
    }
    if raw
        .exchanges
        .iter()
        .any(|exchange| exchange.trim().is_empty())
    {
        return Err(ConfigError::Validation(
            "monitor exchange names must not be empty".to_owned(),
        ));
    }
    if raw.symbols.is_empty() {
        return Err(ConfigError::Validation(
            "monitor must configure at least one symbol".to_owned(),
        ));
    }
    if raw.thresholds.min_spread_pct < Decimal::ZERO
        || raw.thresholds.min_funding_rate_diff < Decimal::ZERO
    {
        return Err(ConfigError::Validation(
            "monitor thresholds must not be negative".to_owned(),
        ));
    }
    for (name, value) in [
        ("websocket.ping_interval", raw.websocket.ping_interval),
        ("websocket.reconnect_delay", raw.websocket.reconnect_delay),
        (
            "performance.analysis_interval_ms",
            raw.performance.analysis_interval_ms,
        ),
        (
            "performance.ui_refresh_interval_ms",
            raw.performance.ui_refresh_interval_ms,
        ),
        ("health_check.interval", raw.health_check.interval),
        ("health_check.data_timeout", raw.health_check.data_timeout),
    ] {
        if value == 0 {
            return Err(ConfigError::Validation(format!(
                "monitor {name} must be positive"
            )));
        }
    }

    Ok(MonitorConfig {
        exchanges: raw.exchanges,
        symbols: raw.symbols,
        min_spread_pct: raw.thresholds.min_spread_pct,
        min_funding_rate_diff: raw.thresholds.min_funding_rate_diff,
        ws_ping_interval: raw.websocket.ping_interval,
        ws_reconnect_delay: raw.websocket.reconnect_delay,
        ws_max_reconnect_attempts: raw.websocket.max_reconnect_attempts,
        analysis_interval_ms: raw.performance.analysis_interval_ms,
        ui_refresh_interval_ms: raw.performance.ui_refresh_interval_ms,
        health_check_interval: raw.health_check.interval,
        data_timeout_seconds: raw.health_check.data_timeout,
    })
}
