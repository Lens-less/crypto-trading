use std::{fs, path::Path};

use crypto_trading_domain::{MarketType, Price, Quantity, Symbol};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{ConfigError, ConfigResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceAlertConfig {
    pub exchange: String,
    pub symbols: Vec<PriceAlertSymbolConfig>,
    pub refresh_interval_seconds: Decimal,
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceAlertSymbolConfig {
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub enabled: bool,
    pub volatility_alert: VolatilityAlertConfig,
    pub price_alert: PriceThresholdConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolatilityAlertConfig {
    pub enabled: bool,
    pub time_window_seconds: u64,
    pub threshold_percent: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceThresholdConfig {
    pub enabled: bool,
    pub upper_price: Option<Price>,
    pub lower_price: Option<Price>,
}

#[derive(Debug, Deserialize)]
struct RawPriceAlertDocument {
    #[serde(alias = "alert_system")]
    price_alert: RawPriceAlert,
}

#[derive(Debug, Deserialize)]
struct RawPriceAlert {
    exchange: String,
    #[serde(default)]
    symbols: Vec<RawPriceAlertSymbol>,
    #[serde(default)]
    display: RawAlertDisplay,
    #[serde(default)]
    alert: RawAlertControls,
}

#[derive(Debug, Deserialize)]
struct RawPriceAlertSymbol {
    symbol: Symbol,
    #[serde(default)]
    market_type: MarketType,
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default)]
    volatility_alert: RawVolatilityAlert,
    #[serde(default)]
    price_alert: RawPriceThreshold,
}

#[derive(Debug, Deserialize)]
struct RawVolatilityAlert {
    #[serde(default)]
    enabled: bool,
    #[serde(default = "default_volatility_window", alias = "time_window_seconds")]
    time_window: u64,
    #[serde(default)]
    threshold_percent: Decimal,
}

impl Default for RawVolatilityAlert {
    fn default() -> Self {
        Self {
            enabled: false,
            time_window: default_volatility_window(),
            threshold_percent: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct RawPriceThreshold {
    #[serde(default)]
    enabled: bool,
    #[serde(default, alias = "upper_price")]
    upper_limit: Option<Price>,
    #[serde(default, alias = "lower_price")]
    lower_limit: Option<Price>,
}

#[derive(Debug, Deserialize)]
struct RawAlertDisplay {
    #[serde(default = "default_refresh_interval")]
    refresh_interval: Decimal,
}

impl Default for RawAlertDisplay {
    fn default() -> Self {
        Self {
            refresh_interval: default_refresh_interval(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawAlertControls {
    #[serde(default = "default_cooldown")]
    cooldown_seconds: u64,
}

impl Default for RawAlertControls {
    fn default() -> Self {
        Self {
            cooldown_seconds: default_cooldown(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMakerConfig {
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub signal_exchange: Option<String>,
    pub signal_symbol: Option<Symbol>,
    pub order_quantity: Quantity,
    pub min_quantity: Option<Quantity>,
    pub max_quantity: Option<Quantity>,
    pub target_volume: Option<Quantity>,
    pub interval_seconds: Decimal,
    pub max_cycles: Option<u64>,
    pub emergency_stop: bool,
    pub order_mode: String,
    pub reverse_trading: bool,
    pub use_post_only: bool,
}

#[derive(Debug, Deserialize)]
struct RawVolumeDocument {
    #[serde(alias = "volume_system")]
    volume_maker: RawVolumeMaker,
}

#[derive(Debug, Deserialize)]
struct RawVolumeMaker {
    exchange: String,
    symbol: Symbol,
    #[serde(default)]
    market_type: MarketType,
    #[serde(default)]
    signal_exchange: Option<String>,
    #[serde(default)]
    signal_symbol: Option<Symbol>,
    #[serde(alias = "order_size", alias = "quantity")]
    order_quantity: Quantity,
    #[serde(default, alias = "min_size")]
    min_quantity: Option<Quantity>,
    #[serde(default, alias = "max_size")]
    max_quantity: Option<Quantity>,
    #[serde(default)]
    target_volume: Option<Quantity>,
    #[serde(default)]
    interval_seconds: Option<Decimal>,
    #[serde(default)]
    cycle_interval: Option<Decimal>,
    #[serde(default)]
    check_interval: Option<Decimal>,
    #[serde(default)]
    post_trade_delay: Option<Decimal>,
    #[serde(default)]
    max_cycles: Option<u64>,
    #[serde(default)]
    emergency_stop: bool,
    #[serde(default = "default_order_mode")]
    order_mode: String,
    #[serde(default)]
    reverse_trading: bool,
    #[serde(default)]
    advanced: RawVolumeAdvanced,
}

#[derive(Debug, Default, Deserialize)]
struct RawVolumeAdvanced {
    #[serde(default)]
    use_post_only: bool,
}

const fn yes() -> bool {
    true
}

const fn default_volatility_window() -> u64 {
    300
}

fn default_refresh_interval() -> Decimal {
    Decimal::ONE
}

const fn default_cooldown() -> u64 {
    30
}

fn default_order_mode() -> String {
    "limit".to_owned()
}

/// Loads a price-alert configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_price_alert_config(path: impl AsRef<Path>) -> ConfigResult<PriceAlertConfig> {
    let path = path.as_ref();
    let yaml = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    load_price_alert_config_from_str(&yaml)
}

/// Parses a price-alert configuration document.
///
/// # Errors
///
/// Returns an error if the YAML shape or a typed value is invalid.
pub fn load_price_alert_config_from_str(yaml: &str) -> ConfigResult<PriceAlertConfig> {
    let raw: RawPriceAlertDocument = serde_yaml::from_str(yaml)?;
    let symbols = raw
        .price_alert
        .symbols
        .into_iter()
        .map(|symbol| PriceAlertSymbolConfig {
            symbol: symbol.symbol,
            market_type: symbol.market_type,
            enabled: symbol.enabled,
            volatility_alert: VolatilityAlertConfig {
                enabled: symbol.volatility_alert.enabled,
                time_window_seconds: symbol.volatility_alert.time_window,
                threshold_percent: symbol.volatility_alert.threshold_percent,
            },
            price_alert: PriceThresholdConfig {
                enabled: symbol.price_alert.enabled,
                upper_price: nonzero_price(symbol.price_alert.upper_limit),
                lower_price: nonzero_price(symbol.price_alert.lower_limit),
            },
        })
        .collect();

    Ok(PriceAlertConfig {
        exchange: raw.price_alert.exchange,
        symbols,
        refresh_interval_seconds: raw.price_alert.display.refresh_interval,
        cooldown_seconds: raw.price_alert.alert.cooldown_seconds,
    })
}

/// Loads a volume-maker configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_volume_maker_config(path: impl AsRef<Path>) -> ConfigResult<VolumeMakerConfig> {
    let path = path.as_ref();
    let yaml = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    load_volume_maker_config_from_str(&yaml)
}

/// Parses and validates a volume-maker configuration document.
///
/// # Errors
///
/// Returns an error for invalid YAML or a non-positive order quantity.
pub fn load_volume_maker_config_from_str(yaml: &str) -> ConfigResult<VolumeMakerConfig> {
    let raw: RawVolumeDocument = serde_yaml::from_str(yaml)?;
    if raw.volume_maker.order_quantity.as_decimal().is_zero() {
        return Err(ConfigError::Validation(
            "volume maker order quantity must be positive".to_owned(),
        ));
    }
    let interval_seconds = raw
        .volume_maker
        .interval_seconds
        .or(raw.volume_maker.cycle_interval)
        .or(raw.volume_maker.check_interval)
        .or(raw.volume_maker.post_trade_delay)
        .unwrap_or(Decimal::ZERO);
    Ok(VolumeMakerConfig {
        exchange: raw.volume_maker.exchange,
        symbol: raw.volume_maker.symbol,
        market_type: raw.volume_maker.market_type,
        signal_exchange: raw.volume_maker.signal_exchange,
        signal_symbol: raw.volume_maker.signal_symbol,
        order_quantity: raw.volume_maker.order_quantity,
        min_quantity: raw.volume_maker.min_quantity,
        max_quantity: raw.volume_maker.max_quantity,
        target_volume: raw.volume_maker.target_volume,
        interval_seconds,
        max_cycles: raw.volume_maker.max_cycles,
        emergency_stop: raw.volume_maker.emergency_stop,
        order_mode: raw.volume_maker.order_mode,
        reverse_trading: raw.volume_maker.reverse_trading,
        use_post_only: raw.volume_maker.advanced.use_post_only,
    })
}

fn nonzero_price(price: Option<Price>) -> Option<Price> {
    price.filter(|price| !price.as_decimal().is_zero())
}
