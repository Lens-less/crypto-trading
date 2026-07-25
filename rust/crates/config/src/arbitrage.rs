use std::{collections::BTreeMap, path::Path, str::FromStr};

use crypto_trading_domain::{Quantity, Symbol};
use rust_decimal::Decimal;

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

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
    pub max_position_value: Option<Decimal>,
    pub symbol_configs: BTreeMap<Symbol, ArbitrageSymbolConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArbitrageSymbolConfig {
    pub enabled: bool,
    pub min_spread_pct: Option<Decimal>,
    pub grid_step_pct: Option<Decimal>,
    pub max_segments: Option<u32>,
    pub base_quantity: Option<Quantity>,
    pub max_position_value: Option<Decimal>,
}

impl ArbitrageConfig {
    /// Verifies the operator controls required before constructing an
    /// executable arbitrage strategy.
    ///
    /// Legacy documents remain parseable for inventory and migration, but
    /// only explicitly enabled, non-monitoring segmented profiles with
    /// non-empty exchange and symbol allowlists, at least one enabled symbol
    /// strategy, and a positive resolved risk limit may cross the strategy
    /// boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when arbitrage is disabled, monitor-only mode is
    /// active, the configured mode is not implemented by the Rust runtime, or
    /// an operator allowlist is empty or contains a blank exchange name, the
    /// resolved position limit is missing or non-positive, or no symbol
    /// strategy is enabled for execution.
    pub fn validate_execution_controls(&self) -> ConfigResult<()> {
        self.validate_strategy_selection_controls()?;
        self.validate_execution_risk_limit()
    }

    fn validate_strategy_selection_controls(&self) -> ConfigResult<()> {
        if !self.enabled {
            return Err(ConfigError::Validation(
                "arbitrage execution is disabled".to_owned(),
            ));
        }
        if self.monitor_only {
            return Err(ConfigError::Validation(
                "arbitrage monitor-only mode forbids execution".to_owned(),
            ));
        }
        if !self.mode.trim().eq_ignore_ascii_case("segmented") {
            return Err(ConfigError::Validation(format!(
                "arbitrage mode {} is not executable; expected segmented",
                self.mode
            )));
        }
        if self.exchanges.is_empty() {
            return Err(ConfigError::Validation(
                "arbitrage exchange allowlist must not be empty".to_owned(),
            ));
        }
        if self
            .exchanges
            .iter()
            .any(|exchange| exchange.trim().is_empty())
        {
            return Err(ConfigError::Validation(
                "arbitrage exchange allowlist entries must not be blank".to_owned(),
            ));
        }
        if self.symbols.is_empty() {
            return Err(ConfigError::Validation(
                "arbitrage symbol allowlist must not be empty".to_owned(),
            ));
        }
        if !self.symbol_configs.values().any(|config| config.enabled) {
            return Err(ConfigError::Validation(
                "arbitrage executable config requires at least one enabled symbol strategy"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_execution_risk_limit(&self) -> ConfigResult<()> {
        match self.max_position_value {
            Some(value) if value > Decimal::ZERO => Ok(()),
            Some(_) => Err(ConfigError::Validation(
                "arbitrage max_position_value must be positive".to_owned(),
            )),
            None => Err(ConfigError::Validation(
                "arbitrage max_position_value is required for execution".to_owned(),
            )),
        }
    }

    /// Resolves the effective strategy configuration for one explicitly named
    /// `symbol_configs` entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is absent, disabled, or resolves to an
    /// invalid runtime strategy configuration.
    pub fn resolve_for_strategy(&self, strategy_key: &Symbol) -> ConfigResult<Self> {
        self.validate_strategy_selection_controls()?;
        let symbol_config = self.symbol_configs.get(strategy_key).ok_or_else(|| {
            ConfigError::Validation(format!(
                "arbitrage strategy key {strategy_key} is not configured"
            ))
        })?;
        if !symbol_config.enabled {
            return Err(ConfigError::Validation(format!(
                "arbitrage strategy key {strategy_key} is disabled"
            )));
        }

        let mut effective = self.clone();
        if let Some(value) = symbol_config.min_spread_pct {
            effective.min_spread_pct = value;
        }
        if let Some(value) = symbol_config.grid_step_pct {
            effective.grid_step_pct = value;
        }
        if let Some(value) = symbol_config.max_segments {
            effective.max_segments = value;
        }
        if let Some(value) = symbol_config.base_quantity {
            effective.base_quantity = value;
        }
        if let Some(value) = symbol_config.max_position_value {
            effective.max_position_value = Some(value);
        }
        effective.validate_execution_controls()?;
        effective.validate_strategy_values()?;
        Ok(effective)
    }

    fn validate_strategy_values(&self) -> ConfigResult<()> {
        if self.min_spread_pct <= Decimal::ZERO {
            return Err(ConfigError::Validation(
                "arbitrage spread threshold must be positive".to_owned(),
            ));
        }
        if self.grid_step_pct <= Decimal::ZERO {
            return Err(ConfigError::Validation(
                "arbitrage grid step must be positive".to_owned(),
            ));
        }
        if self.max_segments == 0 {
            return Err(ConfigError::Validation(
                "arbitrage max_segments must be positive".to_owned(),
            ));
        }
        if self.base_quantity.as_decimal() <= Decimal::ZERO {
            return Err(ConfigError::Validation(
                "arbitrage base quantity must be positive".to_owned(),
            ));
        }
        if self.first_close_ratio < Decimal::ZERO || self.first_close_ratio >= Decimal::ONE {
            return Err(ConfigError::Validation(
                "arbitrage first_close_ratio must be in [0, 1)".to_owned(),
            ));
        }
        if self
            .max_position_value
            .is_some_and(|value| value <= Decimal::ZERO)
        {
            return Err(ConfigError::Validation(
                "arbitrage max_position_value must be positive".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Loads an arbitrage configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or its YAML values are invalid.
pub fn load_arbitrage_config(path: impl AsRef<Path>) -> ConfigResult<ArbitrageConfig> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_arbitrage_config_from_str(&yaml)
}

/// Parses an arbitrage configuration document.
///
/// # Errors
///
/// Returns an error if the YAML shape or a typed value is invalid.
pub fn load_arbitrage_config_from_str(yaml: &str) -> ConfigResult<ArbitrageConfig> {
    let document: serde_yaml::Value = parse_yaml(yaml)?;

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
    let monitor_only = bool_at(&document, &["system_mode", "monitor_only"])?.unwrap_or(true);
    let enabled = bool_at(&document, &["arbitrage_decision", "enabled"])?
        .or(bool_at(&document, &["enabled"])?)
        .unwrap_or(false);
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
    )?
    .or(u32_at(&document, &["max_segments"])?)
    .unwrap_or(1);
    let first_close_ratio = decimal_at_any(
        &document,
        &[
            &["default_config", "grid_config", "first_close_ratio"],
            &["first_close_ratio"],
        ],
    )?
    .unwrap_or_else(|| Decimal::new(4, 1));
    let max_position_value = positive_decimal_at_any(
        &document,
        &[
            &["default_config", "risk_config", "max_position_value"],
            &["risk_config", "max_position_value"],
            &["risk_control", "max_position_value"],
            &["max_position_value"],
        ],
        "arbitrage max_position_value",
    )?;

    let symbol_configs = symbol_configs_at(&document)?;

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
        max_position_value,
        symbol_configs,
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

fn bool_at(document: &serde_yaml::Value, path: &[&str]) -> ConfigResult<Option<bool>> {
    let Some(value) = value_at(document, path) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| ConfigError::Validation(format!("{} must be a boolean", path.join("."))))
}

fn u32_at(document: &serde_yaml::Value, path: &[&str]) -> ConfigResult<Option<u32>> {
    let Some(value) = value_at(document, path) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let value = value.as_u64().ok_or_else(|| {
        ConfigError::Validation(format!("{} must be an unsigned integer", path.join(".")))
    })?;
    value
        .try_into()
        .map(Some)
        .map_err(|_| ConfigError::Validation(format!("{} is larger than u32", path.join("."))))
}

fn symbol_configs_at(
    document: &serde_yaml::Value,
) -> ConfigResult<BTreeMap<Symbol, ArbitrageSymbolConfig>> {
    let Some(value) = value_at(document, &["symbol_configs"]) else {
        return Ok(BTreeMap::new());
    };
    let mapping = value
        .as_mapping()
        .ok_or_else(|| ConfigError::Validation("symbol_configs must be a mapping".to_owned()))?;
    mapping
        .iter()
        .map(|(raw_key, raw_config)| {
            let raw_key = raw_key.as_str().ok_or_else(|| {
                ConfigError::Validation("symbol_configs keys must be strings".to_owned())
            })?;
            let key =
                Symbol::new(raw_key).map_err(|error| ConfigError::Validation(error.to_string()))?;
            if !raw_config.is_mapping() {
                return Err(ConfigError::Validation(format!(
                    "symbol_configs.{key} must be a mapping"
                )));
            }
            let enabled = bool_at(raw_config, &["enabled"])?.ok_or_else(|| {
                ConfigError::Validation(format!("symbol_configs.{key}.enabled is required"))
            })?;
            let min_spread_pct =
                decimal_at_any(raw_config, &[&["grid_config", "initial_spread_threshold"]])?;
            let grid_step_pct = decimal_at_any(raw_config, &[&["grid_config", "grid_step"]])?;
            let max_segments = u32_at(raw_config, &["grid_config", "max_segments"])?;
            let base_quantity = decimal_at_any(
                raw_config,
                &[&["quantity_config", "base_quantity"]],
            )?
            .map(|value| {
                if value <= Decimal::ZERO {
                    return Err(ConfigError::Validation(format!(
                        "symbol_configs.{key}.quantity_config.base_quantity must be positive"
                    )));
                }
                Quantity::new(value).map_err(|error| ConfigError::Validation(error.to_string()))
            })
            .transpose()?;
            let max_position_value = positive_decimal_at_any(
                raw_config,
                &[&["risk_config", "max_position_value"]],
                &format!("symbol_configs.{key}.risk_config.max_position_value"),
            )?;
            Ok((
                key,
                ArbitrageSymbolConfig {
                    enabled,
                    min_spread_pct,
                    grid_step_pct,
                    max_segments,
                    base_quantity,
                    max_position_value,
                },
            ))
        })
        .collect()
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
            if value.is_null() {
                continue;
            }
            return decimal_value(value).map(Some);
        }
    }
    Ok(None)
}

fn positive_decimal_at_any(
    document: &serde_yaml::Value,
    paths: &[&[&str]],
    label: &str,
) -> ConfigResult<Option<Decimal>> {
    let value = decimal_at_any(document, paths)?;
    if value.is_some_and(|candidate| candidate <= Decimal::ZERO) {
        return Err(ConfigError::Validation(format!("{label} must be positive")));
    }
    Ok(value)
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
