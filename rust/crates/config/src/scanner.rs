use std::path::Path;

use crypto_trading_domain::{MarketType, Symbol};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

/// Hard bound on symbols accepted by one scanner configuration. Mirrors the
/// runtime scan candidate limit so a valid config can never overrun one scan.
pub const MAX_SCANNER_CONFIG_SYMBOLS: usize = 128;
/// Maximum APR window accepted by the scanner schema, in seconds.
pub const MAX_SCANNER_CONFIG_APR_WINDOW_SECONDS: u32 = 366 * 24 * 60 * 60;
/// Maximum ranked rows one configured scan may persist.
pub const MAX_SCANNER_CONFIG_ROW_LIMIT: usize = 128;

const MAX_SCANNER_CONFIG_EXCHANGE_BYTES: usize = 128;

/// Validated virtual-grid scanner configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannerConfig {
    pub exchange: String,
    pub apr_window_seconds: u32,
    pub apr_estimate: ScannerAprEstimateConfig,
    pub min_complete_cycles: u64,
    pub row_limit: usize,
    pub symbols: Vec<ScannerSymbolConfig>,
}

impl ScannerConfig {
    /// Returns the symbols currently enabled for scanning.
    pub fn enabled_symbols(&self) -> impl Iterator<Item = &ScannerSymbolConfig> {
        self.symbols.iter().filter(|symbol| symbol.enabled)
    }
}

/// Explicit assumptions used only for the scanner's heuristic APR estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScannerAprEstimateConfig {
    pub order_notional_usdc: Decimal,
    pub round_trip_fee_percent: Decimal,
}

/// One exact scanner candidate: market identity plus virtual-grid geometry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannerSymbolConfig {
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub enabled: bool,
    pub benchmark: bool,
    pub grid_width_percent: Decimal,
    pub grid_interval_percent: Decimal,
    pub volume_24h_usdc: Decimal,
    pub price_change_24h_percent: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
struct RawScannerDocument {
    scanner: RawScanner,
}

#[derive(Debug, Deserialize)]
struct RawScanner {
    exchange: String,
    #[serde(default)]
    scan: RawScanControls,
    #[serde(default)]
    symbols: Vec<RawScannerSymbol>,
}

#[derive(Debug, Deserialize)]
struct RawScanControls {
    #[serde(default = "default_apr_window_seconds")]
    apr_window_seconds: u32,
    #[serde(default)]
    apr_estimate: Option<RawScannerAprEstimate>,
    #[serde(default)]
    min_complete_cycles: u64,
    #[serde(default = "default_row_limit")]
    row_limit: usize,
}

impl Default for RawScanControls {
    fn default() -> Self {
        Self {
            apr_window_seconds: default_apr_window_seconds(),
            apr_estimate: None,
            min_complete_cycles: 0,
            row_limit: default_row_limit(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawScannerSymbol {
    symbol: Symbol,
    #[serde(default)]
    market_type: MarketType,
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default)]
    benchmark: bool,
    grid: RawScannerGrid,
    #[serde(default)]
    volume_24h_usdc: Decimal,
    #[serde(default)]
    price_change_24h_percent: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
struct RawScannerGrid {
    width_percent: Decimal,
    interval_percent: Decimal,
}

#[derive(Debug, Deserialize)]
struct RawScannerAprEstimate {
    order_notional_usdc: Decimal,
    round_trip_fee_percent: Decimal,
}

const fn yes() -> bool {
    true
}

const fn default_apr_window_seconds() -> u32 {
    300
}

const fn default_row_limit() -> usize {
    50
}

/// Loads a scanner configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_scanner_config(path: impl AsRef<Path>) -> ConfigResult<ScannerConfig> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_scanner_config_from_str(&yaml)
}

/// Parses and validates a scanner configuration document.
///
/// The schema fails closed: it rejects an empty or oversized symbol universe,
/// duplicate market identities, unusable virtual-grid geometry, negative
/// volume, and out-of-range scan bounds.
///
/// # Errors
///
/// Returns an error if the YAML shape or a typed value is invalid.
pub fn load_scanner_config_from_str(yaml: &str) -> ConfigResult<ScannerConfig> {
    let raw: RawScannerDocument = parse_yaml(yaml)?;
    let exchange = raw.scanner.exchange.trim().to_owned();
    if exchange.is_empty() {
        return Err(ConfigError::Validation(
            "scanner exchange must not be empty".to_owned(),
        ));
    }
    if exchange.len() > MAX_SCANNER_CONFIG_EXCHANGE_BYTES {
        return Err(ConfigError::Validation(format!(
            "scanner exchange exceeds {MAX_SCANNER_CONFIG_EXCHANGE_BYTES} bytes"
        )));
    }
    let scan = raw.scanner.scan;
    if scan.apr_window_seconds == 0
        || scan.apr_window_seconds > MAX_SCANNER_CONFIG_APR_WINDOW_SECONDS
    {
        return Err(ConfigError::Validation(format!(
            "scanner apr_window_seconds must be within 1..={MAX_SCANNER_CONFIG_APR_WINDOW_SECONDS}"
        )));
    }
    if scan.row_limit == 0 || scan.row_limit > MAX_SCANNER_CONFIG_ROW_LIMIT {
        return Err(ConfigError::Validation(format!(
            "scanner row_limit must be within 1..={MAX_SCANNER_CONFIG_ROW_LIMIT}"
        )));
    }
    let apr_estimate = scan.apr_estimate.ok_or(ConfigError::MissingRequiredField {
        path: "scanner.scan.apr_estimate",
    })?;
    if apr_estimate.order_notional_usdc <= Decimal::ZERO {
        return Err(ConfigError::Validation(
            "scanner scan.apr_estimate.order_notional_usdc must be positive".to_owned(),
        ));
    }
    if apr_estimate.round_trip_fee_percent < Decimal::ZERO {
        return Err(ConfigError::Validation(
            "scanner scan.apr_estimate.round_trip_fee_percent must not be negative".to_owned(),
        ));
    }
    if raw.scanner.symbols.is_empty() {
        return Err(ConfigError::Validation(
            "scanner requires at least one symbol".to_owned(),
        ));
    }
    if raw.scanner.symbols.len() > MAX_SCANNER_CONFIG_SYMBOLS {
        return Err(ConfigError::Validation(format!(
            "scanner accepts at most {MAX_SCANNER_CONFIG_SYMBOLS} symbols"
        )));
    }

    let mut symbols = Vec::with_capacity(raw.scanner.symbols.len());
    for symbol in raw.scanner.symbols {
        symbols.push(validated_symbol(symbol)?);
    }
    let mut identities = symbols
        .iter()
        .map(|symbol: &ScannerSymbolConfig| {
            (symbol.symbol.as_str(), market_type_rank(symbol.market_type))
        })
        .collect::<Vec<_>>();
    identities.sort_unstable();
    if identities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConfigError::Validation(
            "scanner symbols must not repeat one exact market identity".to_owned(),
        ));
    }
    if !symbols.iter().any(|symbol| symbol.enabled) {
        return Err(ConfigError::Validation(
            "scanner requires at least one enabled symbol".to_owned(),
        ));
    }

    Ok(ScannerConfig {
        exchange,
        apr_window_seconds: scan.apr_window_seconds,
        apr_estimate: ScannerAprEstimateConfig {
            order_notional_usdc: apr_estimate.order_notional_usdc,
            round_trip_fee_percent: apr_estimate.round_trip_fee_percent,
        },
        min_complete_cycles: scan.min_complete_cycles,
        row_limit: scan.row_limit,
        symbols,
    })
}

const fn market_type_rank(market_type: MarketType) -> u8 {
    match market_type {
        MarketType::Spot => 0,
        MarketType::Perpetual => 1,
    }
}

fn validated_symbol(raw: RawScannerSymbol) -> ConfigResult<ScannerSymbolConfig> {
    let symbol_text = raw.symbol.as_str().to_owned();
    let invalid =
        |message: &str| ConfigError::Validation(format!("scanner symbol {symbol_text}: {message}"));
    if raw.grid.width_percent <= Decimal::ZERO {
        return Err(invalid("grid width_percent must be positive"));
    }
    if raw.grid.interval_percent <= Decimal::ZERO {
        return Err(invalid("grid interval_percent must be positive"));
    }
    let interval_span = raw
        .grid
        .interval_percent
        .checked_mul(Decimal::TWO)
        .ok_or_else(|| invalid("grid interval_percent is not representable"))?;
    if interval_span > raw.grid.width_percent {
        return Err(invalid(
            "grid interval_percent must fit twice inside width_percent",
        ));
    }
    if raw.volume_24h_usdc < Decimal::ZERO {
        return Err(invalid("volume_24h_usdc must not be negative"));
    }
    Ok(ScannerSymbolConfig {
        symbol: raw.symbol,
        market_type: raw.market_type,
        enabled: raw.enabled,
        benchmark: raw.benchmark,
        grid_width_percent: raw.grid.width_percent,
        grid_interval_percent: raw.grid.interval_percent,
        volume_24h_usdc: raw.volume_24h_usdc,
        price_change_24h_percent: raw.price_change_24h_percent,
    })
}
