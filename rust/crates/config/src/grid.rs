use std::{fs, path::Path};

use crypto_trading_domain::{MarketType, Price, Quantity, Symbol};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{ConfigError, ConfigResult};

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
}

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

/// Loads and validates a grid configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or validated.
pub fn load_grid_config(path: impl AsRef<Path>) -> ConfigResult<GridConfig> {
    let path = path.as_ref();
    let yaml = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    load_grid_config_from_str(&yaml)
}

/// Parses and validates a grid configuration document.
///
/// # Errors
///
/// Returns an error for invalid YAML, missing fields, or inconsistent grid bounds.
pub fn load_grid_config_from_str(yaml: &str) -> ConfigResult<GridConfig> {
    let document: serde_yaml::Value = serde_yaml::from_str(yaml)?;
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
    })
}

fn mapping_value<'a>(document: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    document.as_mapping()?.get(serde_yaml::Value::from(key))
}
