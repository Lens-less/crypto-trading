use std::{collections::BTreeMap, path::Path};

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SymbolConversions {
    standard_to_exchange: BTreeMap<String, BTreeMap<String, String>>,
    exchange_to_standard: BTreeMap<String, BTreeMap<String, String>>,
}

impl SymbolConversions {
    pub fn resolve(&self, exchange: &str, standard_symbol: &str) -> String {
        let exchange = exchange.to_ascii_lowercase();
        let standard = standard_symbol.to_ascii_uppercase();
        if let Some(mapped) = self
            .standard_to_exchange
            .get(&exchange)
            .and_then(|mappings| mappings.get(&standard))
        {
            return mapped.clone();
        }
        standard_rule(&exchange, &standard).unwrap_or(standard)
    }

    pub fn to_standard(&self, exchange: &str, exchange_symbol: &str) -> Option<String> {
        let exchange = exchange.to_ascii_lowercase();
        if let Some(mapped) = self
            .exchange_to_standard
            .get(&exchange)
            .and_then(|mappings| mappings.get(exchange_symbol))
        {
            return Some(mapped.clone());
        }
        reverse_rule(&exchange, exchange_symbol)
    }
}

/// Loads symbol-conversion overrides from a YAML file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or its mappings are malformed.
pub fn load_symbol_conversions(path: impl AsRef<Path>) -> ConfigResult<SymbolConversions> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_symbol_conversions_from_str(&yaml)
}

/// Parses symbol-conversion overrides from YAML.
///
/// # Errors
///
/// Returns an error if YAML mappings do not contain string pairs.
pub fn load_symbol_conversions_from_str(yaml: &str) -> ConfigResult<SymbolConversions> {
    let document: serde_yaml::Value = parse_yaml(yaml)?;
    let mut conversions = SymbolConversions::default();

    if let Some(value) = at(&document, &["symbol_mappings", "standard_to_exchange"]) {
        merge_exchange_first(value, &mut conversions.standard_to_exchange)?;
    }
    if let Some(value) = at(&document, &["symbol_mappings", "exchange_to_standard"]) {
        merge_exchange_first(value, &mut conversions.exchange_to_standard)?;
    }

    if let Some(value) = at(&document, &["conversions"]) {
        merge_symbol_first(value, &mut conversions.standard_to_exchange)?;
    }

    if let Some(value) = at(&document, &["symbol_mappings"]) {
        let Some(mapping) = value.as_mapping() else {
            return Err(ConfigError::Validation(
                "symbol_mappings must be a mapping".to_owned(),
            ));
        };
        for (exchange, entries) in mapping {
            let Some(exchange) = exchange.as_str() else {
                continue;
            };
            if matches!(exchange, "standard_to_exchange" | "exchange_to_standard") {
                continue;
            }
            merge_one_exchange(exchange, entries, &mut conversions.standard_to_exchange)?;
        }
    }

    Ok(conversions)
}

fn standard_rule(exchange: &str, standard: &str) -> Option<String> {
    let (base, quote, market_type) = split_standard(standard)?;
    let output = match exchange {
        "backpack" => match market_type {
            "SPOT" => format!("{base}_{quote}"),
            _ => format!("{base}_{quote}_PERP"),
        },
        "lighter" => match market_type {
            "SPOT" => format!("{base}/{quote}"),
            _ => base.to_owned(),
        },
        "edgex" => {
            let quote = if matches!(quote, "USDC" | "USDT") {
                "USD"
            } else {
                quote
            };
            format!("{base}{quote}")
        }
        "paradex" => {
            let quote = if matches!(quote, "USDC" | "USDT") {
                "USD"
            } else {
                quote
            };
            match market_type {
                "SPOT" => format!("{base}-{quote}"),
                _ => format!("{base}-{quote}-PERP"),
            }
        }
        "hyperliquid" => match market_type {
            "SPOT" => format!("{base}/{quote}"),
            _ => format!("{base}/{quote}:{quote}"),
        },
        "binance" => {
            let quote = if quote == "USDC" { "USDT" } else { quote };
            format!("{base}{quote}")
        }
        _ => return None,
    };
    Some(output)
}

fn reverse_rule(exchange: &str, symbol: &str) -> Option<String> {
    match exchange {
        "backpack" => {
            let parts: Vec<_> = symbol.split('_').collect();
            match parts.as_slice() {
                [base, quote, "PERP"] => Some(format!("{base}-{quote}-PERP")),
                [base, quote] => Some(format!("{base}-{quote}-SPOT")),
                _ => None,
            }
        }
        "lighter" if !symbol.contains(['-', '/']) => Some(format!("{symbol}-USDC-PERP")),
        "lighter" if symbol.contains('/') => {
            let (base, quote) = symbol.split_once('/')?;
            Some(format!("{base}-{quote}-SPOT"))
        }
        "paradex" => {
            let mut parts = symbol.rsplitn(3, '-').collect::<Vec<_>>();
            parts.reverse();
            match parts.as_slice() {
                [base, "USD", "PERP"] => Some(format!("{base}-USDC-PERP")),
                _ => None,
            }
        }
        _ => None,
    }
}

fn split_standard(symbol: &str) -> Option<(&str, &str, &str)> {
    let mut parts = symbol.rsplitn(3, '-');
    let market_type = parts.next()?;
    let quote = parts.next()?;
    let base = parts.next()?;
    Some((base, quote, market_type))
}

fn at<'a>(value: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a serde_yaml::Value> {
    path.iter().try_fold(value, |current, key| {
        current.as_mapping()?.get(serde_yaml::Value::from(*key))
    })
}

fn merge_exchange_first(
    value: &serde_yaml::Value,
    target: &mut BTreeMap<String, BTreeMap<String, String>>,
) -> ConfigResult<()> {
    let mapping = value.as_mapping().ok_or_else(|| {
        ConfigError::Validation("symbol conversion section must be a mapping".to_owned())
    })?;
    for (exchange, entries) in mapping {
        let exchange = exchange.as_str().ok_or_else(|| {
            ConfigError::Validation("exchange mapping keys must be strings".to_owned())
        })?;
        merge_one_exchange(exchange, entries, target)?;
    }
    Ok(())
}

fn merge_one_exchange(
    exchange: &str,
    entries: &serde_yaml::Value,
    target: &mut BTreeMap<String, BTreeMap<String, String>>,
) -> ConfigResult<()> {
    let entries = entries.as_mapping().ok_or_else(|| {
        ConfigError::Validation(format!("symbol mappings for {exchange} must be a mapping"))
    })?;
    let target = target.entry(exchange.to_ascii_lowercase()).or_default();
    for (source, destination) in entries {
        let (Some(source), Some(destination)) = (source.as_str(), destination.as_str()) else {
            return Err(ConfigError::Validation(format!(
                "symbol mappings for {exchange} must contain string pairs"
            )));
        };
        target.insert(source.to_ascii_uppercase(), destination.to_owned());
    }
    Ok(())
}

fn merge_symbol_first(
    value: &serde_yaml::Value,
    target: &mut BTreeMap<String, BTreeMap<String, String>>,
) -> ConfigResult<()> {
    let mapping = value
        .as_mapping()
        .ok_or_else(|| ConfigError::Validation("conversions must be a mapping".to_owned()))?;
    for (source, entries) in mapping {
        let source = source.as_str().ok_or_else(|| {
            ConfigError::Validation("conversion source symbols must be strings".to_owned())
        })?;
        let entries = entries.as_mapping().ok_or_else(|| {
            ConfigError::Validation(format!("conversion entries for {source} must be a mapping"))
        })?;
        for (exchange, destination) in entries {
            let (Some(exchange), Some(destination)) = (exchange.as_str(), destination.as_str())
            else {
                return Err(ConfigError::Validation(format!(
                    "conversion entries for {source} must contain string pairs"
                )));
            };
            target
                .entry(exchange.to_ascii_lowercase())
                .or_default()
                .insert(source.to_ascii_uppercase(), destination.to_owned());
        }
    }
    Ok(())
}
