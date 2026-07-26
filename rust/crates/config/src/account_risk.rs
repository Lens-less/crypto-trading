use std::path::Path;

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

/// Hard bound for configured account-risk symbol lists.
pub const MAX_ACCOUNT_RISK_CONFIG_SYMBOLS: usize = 256;
/// Hard bound for the optional maximum position duration.
pub const MAX_ACCOUNT_RISK_CONFIG_DURATION_SECONDS: u64 = 30 * 24 * 60 * 60;

/// Account-level risk configuration mirroring the legacy global risk
/// controller. Every limit is optional and disabled by default.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AccountRiskConfig {
    /// Maximum non-released exposure attributable to one symbol.
    pub max_symbol_exposure: Option<Decimal>,
    /// Maximum non-released exposure across every symbol and account.
    pub max_total_exposure: Option<Decimal>,
    /// Total-balance threshold below which admissions carry a warning.
    pub min_balance_warning: Option<Decimal>,
    /// Total-balance threshold below which new admissions are refused and a
    /// close-all directive is raised.
    pub min_balance_close_position: Option<Decimal>,
    /// Maximum holding duration before a close directive is raised.
    pub max_position_duration_seconds: Option<u64>,
    /// Maximum admitted trades per UTC day (midnight reset).
    pub max_daily_trades: Option<u32>,
    /// Symbols refused outright.
    pub disabled_symbols: Vec<String>,
    /// Symbols admitted with an explicit high-risk annotation.
    pub high_risk_symbols: Vec<String>,
}

/// Loads an account-risk configuration file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_account_risk_config(path: impl AsRef<Path>) -> ConfigResult<AccountRiskConfig> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_account_risk_config_from_str(&yaml)
}

/// Parses an account-risk configuration document.
///
/// # Errors
///
/// Returns an error if YAML values are malformed or fail validation.
pub fn load_account_risk_config_from_str(yaml: &str) -> ConfigResult<AccountRiskConfig> {
    let config: AccountRiskConfig = parse_yaml(yaml)?;
    validate_account_risk_config(&config)?;
    Ok(config)
}

fn validate_account_risk_config(config: &AccountRiskConfig) -> ConfigResult<()> {
    for (name, value) in [
        ("max_symbol_exposure", config.max_symbol_exposure),
        ("max_total_exposure", config.max_total_exposure),
        ("min_balance_warning", config.min_balance_warning),
        (
            "min_balance_close_position",
            config.min_balance_close_position,
        ),
    ] {
        if value.is_some_and(|value| value <= Decimal::ZERO) {
            return Err(ConfigError::Validation(format!(
                "account risk {name} must be positive"
            )));
        }
    }
    if let (Some(warning), Some(close)) = (
        config.min_balance_warning,
        config.min_balance_close_position,
    ) && warning < close
    {
        return Err(ConfigError::Validation(
            "account risk min_balance_warning must not be below min_balance_close_position"
                .to_owned(),
        ));
    }
    if let Some(duration) = config.max_position_duration_seconds
        && (duration == 0 || duration > MAX_ACCOUNT_RISK_CONFIG_DURATION_SECONDS)
    {
        return Err(ConfigError::Validation(
            "account risk max_position_duration_seconds is outside the supported bound".to_owned(),
        ));
    }
    if config.max_daily_trades == Some(0) {
        return Err(ConfigError::Validation(
            "account risk max_daily_trades must be positive".to_owned(),
        ));
    }
    for (name, symbols) in [
        ("disabled_symbols", &config.disabled_symbols),
        ("high_risk_symbols", &config.high_risk_symbols),
    ] {
        if symbols.len() > MAX_ACCOUNT_RISK_CONFIG_SYMBOLS {
            return Err(ConfigError::Validation(format!(
                "account risk {name} exceeds {MAX_ACCOUNT_RISK_CONFIG_SYMBOLS} entries"
            )));
        }
        for symbol in symbols {
            if symbol.is_empty()
                || symbol.len() > 64
                || symbol.trim() != symbol
                || symbol.chars().any(char::is_control)
            {
                return Err(ConfigError::Validation(format!(
                    "account risk {name} entry is unsafe"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AccountRiskConfig, load_account_risk_config_from_str};
    use rust_decimal::Decimal;

    #[test]
    fn empty_document_disables_every_limit() {
        let config = load_account_risk_config_from_str("{}").unwrap();
        assert_eq!(config, AccountRiskConfig::default());
    }

    #[test]
    fn legacy_defaults_parse_with_total_balance_thresholds() {
        let config = load_account_risk_config_from_str(
            "min_balance_warning: 100\nmin_balance_close_position: 50\nmax_daily_trades: 20\nmax_position_duration_seconds: 86400\ndisabled_symbols:\n  - LUNA-USDC-PERP\nhigh_risk_symbols:\n  - DOGE-USDC-PERP\n",
        )
        .unwrap();
        assert_eq!(config.min_balance_warning, Some(Decimal::from(100)));
        assert_eq!(config.min_balance_close_position, Some(Decimal::from(50)));
        assert_eq!(config.max_daily_trades, Some(20));
        assert_eq!(config.max_position_duration_seconds, Some(86_400));
        assert_eq!(config.disabled_symbols, vec!["LUNA-USDC-PERP".to_owned()]);
    }

    #[test]
    fn validation_rejects_non_positive_inverted_or_unsafe_values() {
        for invalid in [
            "max_symbol_exposure: 0",
            "max_total_exposure: -1",
            "min_balance_warning: 10\nmin_balance_close_position: 20",
            "max_position_duration_seconds: 0",
            "max_daily_trades: 0",
            "disabled_symbols:\n  - ' padded'",
            "unknown_field: 1",
        ] {
            assert!(
                load_account_risk_config_from_str(invalid).is_err(),
                "{invalid} must fail validation"
            );
        }
    }
}
