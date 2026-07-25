use std::{collections::BTreeMap, path::Path};

use crypto_trading_domain::MarketType;

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

const MAX_SYMBOL_MAPPINGS_PER_DIRECTION: usize = 10_000;
const MAX_EXCHANGE_ID_BYTES: usize = 64;
const MAX_SYMBOL_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CatalogMarket {
    Spot,
    Perpetual,
}

impl CatalogMarket {
    fn from_standard_suffix(suffix: &str) -> Option<Self> {
        match suffix {
            "SPOT" => Some(Self::Spot),
            "PERP" | "PERPETUAL" | "FUTURES" => Some(Self::Perpetual),
            _ => None,
        }
    }
}

impl From<MarketType> for CatalogMarket {
    fn from(value: MarketType) -> Self {
        match value {
            MarketType::Spot => Self::Spot,
            MarketType::Perpetual => Self::Perpetual,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StandardKey {
    exchange: String,
    market_type: CatalogMarket,
    symbol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WireKey {
    exchange: String,
    market_type: CatalogMarket,
    symbol: String,
}

/// Explicit, bounded, product-aware symbol mappings loaded from configuration.
///
/// Every configured pair is indexed in both directions. Resolution never
/// synthesizes an exchange symbol from a naming convention.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SymbolConversions {
    standard_to_exchange: BTreeMap<StandardKey, String>,
    exchange_to_standard: BTreeMap<WireKey, String>,
}

impl SymbolConversions {
    /// Resolves an exact standard symbol for one exchange product.
    ///
    /// Returns `None` when the identifier is malformed, the market argument
    /// disagrees with the standard-symbol suffix, or the mapping is absent.
    pub fn resolve(
        &self,
        exchange: &str,
        standard_symbol: &str,
        market_type: MarketType,
    ) -> Option<String> {
        let exchange = normalize_exchange(exchange)?;
        let (standard, embedded_market) = normalize_standard_symbol(standard_symbol).ok()?;
        let market_type = CatalogMarket::from(market_type);
        if embedded_market != market_type {
            return None;
        }
        self.standard_to_exchange
            .get(&StandardKey {
                exchange,
                market_type,
                symbol: standard,
            })
            .cloned()
    }

    /// Resolves an exact exchange wire symbol for one product.
    ///
    /// Market type is part of the lookup key, allowing Spot and perpetual
    /// products to reuse wire symbols such as `BTCUSDT` without ambiguity.
    pub fn to_standard(
        &self,
        exchange: &str,
        exchange_symbol: &str,
        market_type: MarketType,
    ) -> Option<String> {
        let exchange = normalize_exchange(exchange)?;
        let exchange_symbol = normalize_wire_symbol(exchange_symbol)?;
        self.exchange_to_standard
            .get(&WireKey {
                exchange,
                market_type: market_type.into(),
                symbol: exchange_symbol,
            })
            .cloned()
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
        merge_exchange_first(value, MappingDirection::Forward, &mut conversions)?;
    }
    if let Some(value) = at(&document, &["symbol_mappings", "exchange_to_standard"]) {
        merge_exchange_first(value, MappingDirection::Reverse, &mut conversions)?;
    }

    if let Some(value) = at(&document, &["conversions"]) {
        merge_symbol_first(value, &mut conversions)?;
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
            merge_one_exchange(
                exchange,
                entries,
                MappingDirection::Forward,
                &mut conversions,
            )?;
        }
    }

    Ok(conversions)
}

fn split_standard(symbol: &str) -> Option<(&str, &str, &str)> {
    let mut parts = symbol.rsplitn(3, '-');
    let market_type = parts.next()?;
    let quote = parts.next()?;
    let base = parts.next()?;
    if base.is_empty() || quote.is_empty() || market_type.is_empty() {
        return None;
    }
    Some((base, quote, market_type))
}

fn at<'a>(value: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a serde_yaml::Value> {
    path.iter().try_fold(value, |current, key| {
        current.as_mapping()?.get(serde_yaml::Value::from(*key))
    })
}

#[derive(Debug, Clone, Copy)]
enum MappingDirection {
    Forward,
    Reverse,
}

fn merge_exchange_first(
    value: &serde_yaml::Value,
    direction: MappingDirection,
    conversions: &mut SymbolConversions,
) -> ConfigResult<()> {
    let mapping = value.as_mapping().ok_or_else(|| {
        ConfigError::Validation("symbol conversion section must be a mapping".to_owned())
    })?;
    for (exchange, entries) in mapping {
        let exchange = exchange.as_str().ok_or_else(|| {
            ConfigError::Validation("exchange mapping keys must be strings".to_owned())
        })?;
        merge_one_exchange(exchange, entries, direction, conversions)?;
    }
    Ok(())
}

fn merge_one_exchange(
    exchange: &str,
    entries: &serde_yaml::Value,
    direction: MappingDirection,
    conversions: &mut SymbolConversions,
) -> ConfigResult<()> {
    let entries = entries.as_mapping().ok_or_else(|| {
        ConfigError::Validation(format!("symbol mappings for {exchange} must be a mapping"))
    })?;
    for (source, destination) in entries {
        let Some(source) = source.as_str() else {
            return Err(ConfigError::Validation(format!(
                "symbol mappings for {exchange} must use string keys"
            )));
        };

        if let Some(destination) = destination.as_str() {
            insert_pair(conversions, exchange, source, destination, direction, None)?;
            continue;
        }

        let expected_market = CatalogMarket::from_standard_suffix(&source.to_ascii_uppercase())
            .ok_or_else(|| {
                ConfigError::Validation(format!(
                    "symbol mappings for {exchange} must contain string pairs or market sections"
                ))
            })?;
        let nested = destination.as_mapping().ok_or_else(|| {
            ConfigError::Validation(format!(
                "symbol market section {exchange}/{source} must be a mapping"
            ))
        })?;
        for (nested_source, nested_destination) in nested {
            let (Some(nested_source), Some(nested_destination)) =
                (nested_source.as_str(), nested_destination.as_str())
            else {
                return Err(ConfigError::Validation(format!(
                    "symbol market section {exchange}/{source} must contain string pairs"
                )));
            };
            insert_pair(
                conversions,
                exchange,
                nested_source,
                nested_destination,
                direction,
                Some(expected_market),
            )?;
        }
    }
    Ok(())
}

fn merge_symbol_first(
    value: &serde_yaml::Value,
    conversions: &mut SymbolConversions,
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
            insert_forward(conversions, exchange, source, destination, None)?;
        }
    }
    Ok(())
}

fn insert_pair(
    conversions: &mut SymbolConversions,
    exchange: &str,
    source: &str,
    destination: &str,
    direction: MappingDirection,
    expected_market: Option<CatalogMarket>,
) -> ConfigResult<()> {
    match direction {
        MappingDirection::Forward => {
            insert_forward(conversions, exchange, source, destination, expected_market)
        }
        MappingDirection::Reverse => {
            insert_reverse(conversions, exchange, source, destination, expected_market)
        }
    }
}

fn insert_forward(
    conversions: &mut SymbolConversions,
    exchange: &str,
    standard_symbol: &str,
    wire_symbol: &str,
    expected_market: Option<CatalogMarket>,
) -> ConfigResult<()> {
    let exchange = normalize_exchange_for_config(exchange)?;
    let (standard_symbol, market_type) = normalize_standard_symbol(standard_symbol)?;
    ensure_expected_market(&exchange, &standard_symbol, market_type, expected_market)?;
    let wire_symbol = normalize_wire_symbol_for_config(wire_symbol)?;

    let standard_key = StandardKey {
        exchange: exchange.clone(),
        market_type,
        symbol: standard_symbol.clone(),
    };
    let wire_key = WireKey {
        exchange: exchange.clone(),
        market_type,
        symbol: wire_symbol.clone(),
    };

    if let Some(existing) = conversions.standard_to_exchange.get(&standard_key)
        && existing != &wire_symbol
    {
        return Err(ConfigError::Validation(format!(
            "conflicting forward mapping for {exchange}/{standard_symbol}"
        )));
    }
    if let Some(existing) = conversions.exchange_to_standard.get(&wire_key)
        && existing != &standard_symbol
    {
        return Err(ConfigError::Validation(format!(
            "ambiguous reverse mapping for {exchange}/{wire_symbol}"
        )));
    }

    ensure_room(
        conversions.standard_to_exchange.len(),
        conversions.standard_to_exchange.contains_key(&standard_key),
    )?;
    ensure_room(
        conversions.exchange_to_standard.len(),
        conversions.exchange_to_standard.contains_key(&wire_key),
    )?;
    conversions
        .standard_to_exchange
        .insert(standard_key, wire_symbol);
    conversions
        .exchange_to_standard
        .insert(wire_key, standard_symbol);
    Ok(())
}

fn insert_reverse(
    conversions: &mut SymbolConversions,
    exchange: &str,
    wire_symbol: &str,
    standard_symbol: &str,
    expected_market: Option<CatalogMarket>,
) -> ConfigResult<()> {
    let exchange = normalize_exchange_for_config(exchange)?;
    let wire_symbol = normalize_wire_symbol_for_config(wire_symbol)?;
    let (standard_symbol, market_type) = normalize_standard_symbol(standard_symbol)?;
    ensure_expected_market(&exchange, &standard_symbol, market_type, expected_market)?;

    let wire_key = WireKey {
        exchange: exchange.clone(),
        market_type,
        symbol: wire_symbol.clone(),
    };
    if let Some(existing) = conversions.exchange_to_standard.get(&wire_key)
        && existing != &standard_symbol
    {
        return Err(ConfigError::Validation(format!(
            "ambiguous reverse mapping for {exchange}/{wire_symbol}"
        )));
    }

    let standard_key = StandardKey {
        exchange,
        market_type,
        symbol: standard_symbol.clone(),
    };
    ensure_room(
        conversions.exchange_to_standard.len(),
        conversions.exchange_to_standard.contains_key(&wire_key),
    )?;
    conversions
        .exchange_to_standard
        .insert(wire_key, standard_symbol);

    if !conversions.standard_to_exchange.contains_key(&standard_key) {
        ensure_room(conversions.standard_to_exchange.len(), false)?;
        conversions
            .standard_to_exchange
            .insert(standard_key, wire_symbol);
    }
    Ok(())
}

fn normalize_exchange(exchange: &str) -> Option<String> {
    let exchange = exchange.trim();
    if exchange.is_empty()
        || exchange.len() > MAX_EXCHANGE_ID_BYTES
        || exchange.chars().any(char::is_whitespace)
        || exchange.chars().any(char::is_control)
    {
        return None;
    }
    Some(exchange.to_ascii_lowercase())
}

fn normalize_exchange_for_config(exchange: &str) -> ConfigResult<String> {
    normalize_exchange(exchange).ok_or_else(|| {
        ConfigError::Validation(format!(
            "exchange identifier must contain 1..={MAX_EXCHANGE_ID_BYTES} non-whitespace bytes"
        ))
    })
}

fn normalize_standard_symbol(symbol: &str) -> ConfigResult<(String, CatalogMarket)> {
    let symbol = normalize_symbol(symbol, "standard symbol")?;
    let (_, _, suffix) = split_standard(&symbol).ok_or_else(|| {
        ConfigError::Validation(format!(
            "standard symbol {symbol:?} must use BASE-QUOTE-TYPE format"
        ))
    })?;
    let market_type = CatalogMarket::from_standard_suffix(suffix).ok_or_else(|| {
        ConfigError::Validation(format!(
            "standard symbol {symbol:?} has unsupported market type {suffix:?}"
        ))
    })?;
    Ok((symbol, market_type))
}

fn normalize_wire_symbol(symbol: &str) -> Option<String> {
    normalize_symbol(symbol, "wire symbol").ok()
}

fn normalize_wire_symbol_for_config(symbol: &str) -> ConfigResult<String> {
    normalize_symbol(symbol, "wire symbol")
}

fn normalize_symbol(symbol: &str, label: &str) -> ConfigResult<String> {
    let symbol = symbol.trim();
    if symbol.is_empty()
        || symbol.len() > MAX_SYMBOL_BYTES
        || symbol.chars().any(char::is_whitespace)
        || symbol.chars().any(char::is_control)
    {
        return Err(ConfigError::Validation(format!(
            "{label} must contain 1..={MAX_SYMBOL_BYTES} non-whitespace bytes"
        )));
    }
    Ok(symbol.to_ascii_uppercase())
}

fn ensure_expected_market(
    exchange: &str,
    standard_symbol: &str,
    actual: CatalogMarket,
    expected: Option<CatalogMarket>,
) -> ConfigResult<()> {
    if expected.is_some_and(|expected| expected != actual) {
        return Err(ConfigError::Validation(format!(
            "market section disagrees with standard symbol {exchange}/{standard_symbol}"
        )));
    }
    Ok(())
}

fn ensure_room(current_len: usize, already_present: bool) -> ConfigResult<()> {
    if !already_present && current_len >= MAX_SYMBOL_MAPPINGS_PER_DIRECTION {
        return Err(ConfigError::Validation(format!(
            "symbol conversion catalog exceeds {MAX_SYMBOL_MAPPINGS_PER_DIRECTION} mappings per direction"
        )));
    }
    Ok(())
}
