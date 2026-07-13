use std::{fs, path::Path, str::FromStr};

use crypto_trading_domain::{Quantity, Symbol};
use rust_decimal::Decimal;

use crate::{ConfigError, ConfigResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbitrageConfig {
    pub mode: String,
    pub monitor_only: bool,
    pub enabled: bool,
    pub exchanges: Vec<String>,
    pub symbols: Vec<Symbol>,
    pub min_spread_pct: Decimal,
    pub base_quantity: Quantity,
    pub grid_step_pct: Decimal,
    pub max_segments: u32,
    pub first_close_ratio: Decimal,
}

/// Loads an arbitrage configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or its YAML values are invalid.
pub fn load_arbitrage_config(path: impl AsRef<Path>) -> ConfigResult<ArbitrageConfig> {
    let path = path.as_ref();
    let yaml = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    load_arbitrage_config_from_str(&yaml)
}

/// Parses an arbitrage configuration document.
///
/// # Errors
///
/// Returns an error if the YAML shape or a typed value is invalid.
pub fn load_arbitrage_config_from_str(yaml: &str) -> ConfigResult<ArbitrageConfig> {
    let document: serde_yaml::Value = serde_yaml::from_str(yaml)?;

    let mode = string_at(&document, &["mode"]).map_or_else(
        || {
            if value_at(&document, &["default_config"]).is_some() {
                "segmented".to_owned()
            } else {
                "unified".to_owned()
            }
        },
        ToOwned::to_owned,
    );
    let monitor_only = bool_at(&document, &["system_mode", "monitor_only"]).unwrap_or(true);
    let enabled = bool_at(&document, &["arbitrage_decision", "enabled"])
        .or_else(|| bool_at(&document, &["enabled"]))
        .unwrap_or(true);
    let exchanges = strings_at(&document, &["exchanges"])?;
    let symbols = strings_at(&document, &["symbols"])?
        .into_iter()
        .map(Symbol::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ConfigError::Validation(error.to_string()))?;

    let min_spread_pct = decimal_at_any(
        &document,
        &[
            &[
                "arbitrage_decision",
                "thresholds",
                "spread_arbitrage_threshold",
            ],
            &["default_config", "grid_config", "initial_spread_threshold"],
            &["min_spread_pct"],
        ],
    )?
    .unwrap_or_else(|| Decimal::new(1, 1));

    let base_quantity = decimal_at_any(
        &document,
        &[
            &[
                "arbitrage_execution",
                "quantity_config",
                "default",
                "single_order_quantity",
            ],
            &["default_config", "quantity_config", "base_quantity"],
            &["base_quantity"],
        ],
    )?
    .unwrap_or(Decimal::ZERO);
    let base_quantity =
        Quantity::new(base_quantity).map_err(|error| ConfigError::Validation(error.to_string()))?;
    let grid_step_pct = decimal_at_any(
        &document,
        &[
            &["default_config", "grid_config", "grid_step"],
            &["grid_step"],
        ],
    )?
    .unwrap_or(min_spread_pct);
    let max_segments = u32_at(
        &document,
        &["default_config", "grid_config", "max_segments"],
    )
    .or_else(|| u32_at(&document, &["max_segments"]))
    .unwrap_or(1);
    let first_close_ratio = decimal_at_any(
        &document,
        &[
            &["default_config", "grid_config", "first_close_ratio"],
            &["first_close_ratio"],
        ],
    )?
    .unwrap_or_else(|| Decimal::new(4, 1));

    Ok(ArbitrageConfig {
        mode,
        monitor_only,
        enabled,
        exchanges,
        symbols,
        min_spread_pct,
        base_quantity,
        grid_step_pct,
        max_segments,
        first_close_ratio,
    })
}

fn value_at<'a>(document: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a serde_yaml::Value> {
    path.iter().try_fold(document, |current, key| {
        current.as_mapping()?.get(serde_yaml::Value::from(*key))
    })
}

fn string_at<'a>(document: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a str> {
    value_at(document, path)?.as_str()
}

fn bool_at(document: &serde_yaml::Value, path: &[&str]) -> Option<bool> {
    value_at(document, path)?.as_bool()
}

fn u32_at(document: &serde_yaml::Value, path: &[&str]) -> Option<u32> {
    value_at(document, path)?.as_u64()?.try_into().ok()
}

fn strings_at(document: &serde_yaml::Value, path: &[&str]) -> ConfigResult<Vec<String>> {
    let Some(value) = value_at(document, path) else {
        return Ok(Vec::new());
    };
    let Some(sequence) = value.as_sequence() else {
        return Err(ConfigError::Validation(format!(
            "{} must be a list",
            path.join(".")
        )));
    };
    sequence
        .iter()
        .map(|value| {
            value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                ConfigError::Validation(format!("{} entries must be strings", path.join(".")))
            })
        })
        .collect()
}

fn decimal_at_any(
    document: &serde_yaml::Value,
    paths: &[&[&str]],
) -> ConfigResult<Option<Decimal>> {
    for path in paths {
        if let Some(value) = value_at(document, path) {
            return decimal_value(value).map(Some);
        }
    }
    Ok(None)
}

fn decimal_value(value: &serde_yaml::Value) -> ConfigResult<Decimal> {
    let text = match value {
        serde_yaml::Value::String(value) => value.clone(),
        serde_yaml::Value::Number(value) => value.to_string(),
        _ => {
            return Err(ConfigError::Validation(
                "decimal configuration value must be a string or number".to_owned(),
            ));
        }
    };
    Decimal::from_str(&text)
        .map_err(|_| ConfigError::Validation(format!("invalid decimal value {text}")))
}
