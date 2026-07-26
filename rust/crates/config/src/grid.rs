use std::path::Path;

use crypto_trading_domain::{MarketType, Price, Quantity, Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GridMode {
    FixedLong,
    FixedShort,
    FollowLong,
    FollowShort,
    MartingaleLong,
    MartingaleShort,
}

impl GridMode {
    pub const fn is_follow(self) -> bool {
        matches!(self, Self::FollowLong | Self::FollowShort)
    }

    pub const fn is_short(self) -> bool {
        matches!(
            self,
            Self::FixedShort | Self::FollowShort | Self::MartingaleShort
        )
    }

    pub const fn is_martingale(self) -> bool {
        matches!(self, Self::MartingaleLong | Self::MartingaleShort)
    }
}

impl<'de> Deserialize<'de> for GridMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.trim().to_ascii_lowercase().as_str() {
            "fixed" | "long" | "fixed_long" => Ok(Self::FixedLong),
            "short" | "fixed_short" => Ok(Self::FixedShort),
            "follow" | "follow_long" | "moving" | "moving_long" => Ok(Self::FollowLong),
            "follow_short" | "moving_short" => Ok(Self::FollowShort),
            "martingale" | "martingale_long" => Ok(Self::MartingaleLong),
            "martingale_short" => Ok(Self::MartingaleShort),
            _ => Err(serde::de::Error::custom(format!(
                "unsupported grid mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GridConfig {
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub mode: GridMode,
    pub grid_interval: Price,
    pub order_amount: Quantity,
    pub lower_price: Option<Price>,
    pub upper_price: Option<Price>,
    pub follow_grid_count: Option<u32>,
    pub follow_timeout: u64,
    pub follow_distance: u32,
    pub price_offset_grids: i32,
    pub quantity_precision: u32,
    pub price_decimals: u32,
    pub margin_mode: String,
    pub leverage: u32,
    pub fee_rate: Decimal,
    pub martingale_increment: Option<Quantity>,
    /// Scalping trigger progress percent; `None` disables scalping.
    pub scalping_trigger_percent: Option<u32>,
    /// Scalping take-profit distance in grid levels; set with the trigger.
    pub scalping_take_profit_grids: Option<u32>,
    /// Capital protection trigger progress percent; `None` disables it.
    pub capital_protection_trigger_percent: Option<u32>,
    /// Take-profit equity rate as a fraction (0.01 = 1%); `None` disables it.
    pub take_profit_percentage: Option<Decimal>,
    /// Price-lock threshold; `None` disables price locking.
    pub price_lock_threshold: Option<Price>,
    /// Stop-loss trigger percent of the grid height; set when stop-loss is on.
    pub stop_loss_trigger_percent: Option<Decimal>,
    /// Stop-loss adverse escape timeout in seconds; `None` disables stop-loss.
    pub stop_loss_escape_timeout: Option<u64>,
    /// Stop-loss realtime APR threshold percent; set when stop-loss is on.
    pub stop_loss_apr_threshold: Option<Decimal>,
}

// The five protection enable switches mirror the frozen Python configuration
// model one-to-one; collapsing them into enums would break the checked-in
// legacy YAML surface.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Deserialize)]
struct RawGridConfig {
    #[serde(alias = "exchange_name")]
    exchange: String,
    #[serde(alias = "pair", alias = "trading_pair")]
    symbol: Symbol,
    #[serde(default, alias = "market")]
    market_type: MarketType,
    #[serde(alias = "grid_type", alias = "strategy")]
    mode: GridMode,
    #[serde(alias = "grid_spacing", alias = "spacing")]
    grid_interval: Price,
    #[serde(alias = "order_quantity", alias = "quantity")]
    order_amount: Quantity,
    #[serde(default)]
    lower_price: Option<Price>,
    #[serde(default)]
    upper_price: Option<Price>,
    #[serde(default)]
    price_range: Option<PriceRange>,
    #[serde(default, alias = "follow_grids", alias = "grid_count")]
    follow_grid_count: Option<u32>,
    #[serde(default = "default_follow_timeout")]
    follow_timeout: u64,
    #[serde(default = "default_follow_distance")]
    follow_distance: u32,
    #[serde(default)]
    price_offset_grids: i32,
    #[serde(default = "default_quantity_precision")]
    quantity_precision: u32,
    #[serde(default = "default_price_decimals")]
    price_decimals: u32,
    #[serde(default = "default_margin_mode")]
    margin_mode: String,
    #[serde(default = "default_leverage")]
    leverage: u32,
    #[serde(default = "default_fee_rate")]
    fee_rate: Decimal,
    #[serde(default)]
    martingale_increment: Option<Quantity>,
    #[serde(default)]
    scalping_enabled: bool,
    #[serde(default = "default_scalping_trigger_percent")]
    scalping_trigger_percent: u32,
    #[serde(default = "default_scalping_take_profit_grids")]
    scalping_take_profit_grids: u32,
    #[serde(default)]
    capital_protection_enabled: bool,
    #[serde(default = "default_capital_protection_trigger_percent")]
    capital_protection_trigger_percent: u32,
    #[serde(default)]
    take_profit_enabled: bool,
    #[serde(default = "default_take_profit_percentage")]
    take_profit_percentage: Decimal,
    #[serde(default)]
    price_lock_enabled: bool,
    #[serde(default)]
    price_lock_threshold: Option<Price>,
    #[serde(default, alias = "stop_loss_enabled")]
    stop_loss_protection_enabled: bool,
    #[serde(default = "default_stop_loss_trigger_percent")]
    stop_loss_trigger_percent: Decimal,
    #[serde(default = "default_stop_loss_escape_timeout")]
    stop_loss_escape_timeout: u64,
    #[serde(default = "default_stop_loss_apr_threshold")]
    stop_loss_apr_threshold: Decimal,
}

#[derive(Debug, Deserialize)]
struct PriceRange {
    #[serde(alias = "min_price")]
    lower_price: Price,
    #[serde(alias = "max_price")]
    upper_price: Price,
}

const fn default_follow_timeout() -> u64 {
    300
}

const fn default_follow_distance() -> u32 {
    1
}

const fn default_quantity_precision() -> u32 {
    3
}

const fn default_price_decimals() -> u32 {
    2
}

fn default_margin_mode() -> String {
    "isolated".to_owned()
}

const fn default_leverage() -> u32 {
    10
}

fn default_fee_rate() -> Decimal {
    Decimal::new(1, 4)
}

// Legacy protection defaults mirror the frozen Python configuration model
// (`archive/python-legacy/core/services/grid/models/grid_config.py:143-197`).
const fn default_scalping_trigger_percent() -> u32 {
    80
}

const fn default_scalping_take_profit_grids() -> u32 {
    2
}

const fn default_capital_protection_trigger_percent() -> u32 {
    50
}

fn default_take_profit_percentage() -> Decimal {
    Decimal::new(1, 2)
}

fn default_stop_loss_trigger_percent() -> Decimal {
    Decimal::ONE_HUNDRED
}

const fn default_stop_loss_escape_timeout() -> u64 {
    300
}

fn default_stop_loss_apr_threshold() -> Decimal {
    Decimal::from_parts(50, 0, 0, false, 0)
}

const MAX_STOP_LOSS_ESCAPE_TIMEOUT_SECONDS: u64 = 31_536_000;

/// Loads and validates a grid configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or validated.
pub fn load_grid_config(path: impl AsRef<Path>) -> ConfigResult<GridConfig> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_grid_config_from_str(&yaml)
}

/// Parses and validates a grid configuration document.
///
/// # Errors
///
/// Returns an error for invalid YAML, missing fields, or inconsistent grid bounds.
pub fn load_grid_config_from_str(yaml: &str) -> ConfigResult<GridConfig> {
    let document: serde_yaml::Value = parse_yaml(yaml)?;
    let content = mapping_value(&document, "grid_system")
        .or_else(|| mapping_value(&document, "grid"))
        .unwrap_or(&document)
        .clone();
    let raw: RawGridConfig = serde_yaml::from_value(content)?;

    if raw.exchange.trim().is_empty() {
        return Err(ConfigError::Validation(
            "grid exchange must not be empty".to_owned(),
        ));
    }
    if raw.grid_interval.as_decimal().is_zero() {
        return Err(ConfigError::Validation(
            "grid interval must be positive".to_owned(),
        ));
    }
    if raw.order_amount.as_decimal().is_zero() {
        return Err(ConfigError::Validation(
            "grid order amount must be positive".to_owned(),
        ));
    }

    let lower_price = raw
        .lower_price
        .or_else(|| raw.price_range.as_ref().map(|range| range.lower_price));
    let upper_price = raw
        .upper_price
        .or_else(|| raw.price_range.as_ref().map(|range| range.upper_price));

    if raw.mode.is_martingale()
        && raw
            .martingale_increment
            .is_none_or(|increment| increment.as_decimal() <= Decimal::ZERO)
    {
        return Err(ConfigError::Validation(
            "martingale grid requires a positive martingale_increment".to_owned(),
        ));
    }
    let protection = validate_protection(&raw)?;

    if raw.mode.is_follow() {
        if raw.follow_grid_count == Some(0) || raw.follow_grid_count.is_none() {
            return Err(ConfigError::Validation(
                "follow grid requires a positive follow_grid_count".to_owned(),
            ));
        }
    } else {
        let (Some(lower), Some(upper)) = (lower_price, upper_price) else {
            return Err(ConfigError::Validation(
                "fixed grid requires lower and upper prices".to_owned(),
            ));
        };
        if lower >= upper {
            return Err(ConfigError::Validation(
                "grid lower price must be below upper price".to_owned(),
            ));
        }
    }

    Ok(GridConfig {
        exchange: raw.exchange,
        symbol: raw.symbol,
        market_type: raw.market_type,
        mode: raw.mode,
        grid_interval: raw.grid_interval,
        order_amount: raw.order_amount,
        lower_price,
        upper_price,
        follow_grid_count: raw.follow_grid_count,
        follow_timeout: raw.follow_timeout,
        follow_distance: raw.follow_distance,
        price_offset_grids: raw.price_offset_grids,
        quantity_precision: raw.quantity_precision,
        price_decimals: raw.price_decimals,
        margin_mode: raw.margin_mode,
        leverage: raw.leverage,
        fee_rate: raw.fee_rate,
        martingale_increment: raw.martingale_increment,
        scalping_trigger_percent: protection.scalping_trigger_percent,
        scalping_take_profit_grids: protection.scalping_take_profit_grids,
        capital_protection_trigger_percent: protection.capital_protection_trigger_percent,
        take_profit_percentage: protection.take_profit_percentage,
        price_lock_threshold: protection.price_lock_threshold,
        stop_loss_trigger_percent: protection.stop_loss_trigger_percent,
        stop_loss_escape_timeout: protection.stop_loss_escape_timeout,
        stop_loss_apr_threshold: protection.stop_loss_apr_threshold,
    })
}

struct ProtectionFields {
    scalping_trigger_percent: Option<u32>,
    scalping_take_profit_grids: Option<u32>,
    capital_protection_trigger_percent: Option<u32>,
    take_profit_percentage: Option<Decimal>,
    price_lock_threshold: Option<Price>,
    stop_loss_trigger_percent: Option<Decimal>,
    stop_loss_escape_timeout: Option<u64>,
    stop_loss_apr_threshold: Option<Decimal>,
}

/// Validates the optional protection subsystem fields. Each subsystem stays
/// disabled (`None`) unless its legacy enable flag is set, mirroring the
/// frozen Python configuration model (`grid_config.py:143-197`).
fn validate_protection(raw: &RawGridConfig) -> ConfigResult<ProtectionFields> {
    let mut fields = ProtectionFields {
        scalping_trigger_percent: None,
        scalping_take_profit_grids: None,
        capital_protection_trigger_percent: None,
        take_profit_percentage: None,
        price_lock_threshold: None,
        stop_loss_trigger_percent: None,
        stop_loss_escape_timeout: None,
        stop_loss_apr_threshold: None,
    };
    if raw.scalping_enabled {
        if raw.scalping_trigger_percent == 0 || raw.scalping_trigger_percent > 100 {
            return Err(ConfigError::Validation(
                "scalping_trigger_percent must be within 1..=100".to_owned(),
            ));
        }
        if raw.scalping_take_profit_grids == 0 {
            return Err(ConfigError::Validation(
                "scalping_take_profit_grids must be positive".to_owned(),
            ));
        }
        fields.scalping_trigger_percent = Some(raw.scalping_trigger_percent);
        fields.scalping_take_profit_grids = Some(raw.scalping_take_profit_grids);
    }
    if raw.capital_protection_enabled {
        if raw.capital_protection_trigger_percent == 0
            || raw.capital_protection_trigger_percent > 100
        {
            return Err(ConfigError::Validation(
                "capital_protection_trigger_percent must be within 1..=100".to_owned(),
            ));
        }
        fields.capital_protection_trigger_percent = Some(raw.capital_protection_trigger_percent);
    }
    if raw.take_profit_enabled {
        if raw.take_profit_percentage <= Decimal::ZERO || raw.take_profit_percentage > Decimal::TEN
        {
            return Err(ConfigError::Validation(
                "take_profit_percentage must be a positive fraction of at most 10".to_owned(),
            ));
        }
        fields.take_profit_percentage = Some(raw.take_profit_percentage);
    }
    if raw.price_lock_enabled {
        // Enabling price lock without a threshold fails closed
        // (`price_lock_manager.py:35-36`).
        let Some(threshold) = raw.price_lock_threshold else {
            return Err(ConfigError::Validation(
                "price_lock requires a price_lock_threshold".to_owned(),
            ));
        };
        fields.price_lock_threshold = Some(threshold);
    }
    if raw.stop_loss_protection_enabled {
        if raw.stop_loss_trigger_percent <= Decimal::ZERO
            || raw.stop_loss_trigger_percent > Decimal::ONE_HUNDRED
        {
            return Err(ConfigError::Validation(
                "stop_loss_trigger_percent must be within (0, 100]".to_owned(),
            ));
        }
        if raw.stop_loss_escape_timeout == 0
            || raw.stop_loss_escape_timeout > MAX_STOP_LOSS_ESCAPE_TIMEOUT_SECONDS
        {
            return Err(ConfigError::Validation(
                "stop_loss_escape_timeout must be within 1..=31536000 seconds".to_owned(),
            ));
        }
        if raw.stop_loss_apr_threshold < Decimal::ZERO {
            return Err(ConfigError::Validation(
                "stop_loss_apr_threshold must not be negative".to_owned(),
            ));
        }
        fields.stop_loss_trigger_percent = Some(raw.stop_loss_trigger_percent);
        fields.stop_loss_escape_timeout = Some(raw.stop_loss_escape_timeout);
        fields.stop_loss_apr_threshold = Some(raw.stop_loss_apr_threshold);
    }
    Ok(fields)
}

fn mapping_value<'a>(document: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    document.as_mapping()?.get(serde_yaml::Value::from(key))
}
