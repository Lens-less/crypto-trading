use std::{fmt, path::Path};

use crate::{
    ConfigError, ConfigResult,
    input::{parse_yaml, read_config_file},
};

pub trait EnvProvider {
    fn get(&self, key: &str) -> Option<String>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessEnvironment;

impl EnvProvider for ProcessEnvironment {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

#[derive(Default, Clone, PartialEq, Eq)]
pub struct Secret(Option<String>);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        if value.trim().is_empty() {
            Self::default()
        } else {
            Self(Some(value))
        }
    }

    pub fn expose_secret(&self) -> Option<&str> {
        self.0.as_deref()
    }

    pub fn is_configured(&self) -> bool {
        self.0.is_some()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_configured() {
            formatter.write_str("<redacted>")
        } else {
            formatter.write_str("<unset>")
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq)]
pub struct ExchangeAuth {
    pub api_key: Secret,
    pub api_secret: Secret,
    pub api_passphrase: Secret,
    pub private_key: Secret,
    pub jwt_token: Secret,
    pub api_key_private_key: Secret,
    pub stark_private_key: Secret,
    pub wallet_address: Option<String>,
    pub sub_account_id: Option<String>,
    pub l2_address: Option<String>,
    pub account_id: Option<String>,
    pub account_index: Option<u64>,
    pub api_key_index: Option<u64>,
}

impl fmt::Debug for ExchangeAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExchangeAuth")
            .field("api_key", &self.api_key)
            .field("api_secret", &self.api_secret)
            .field("api_passphrase", &self.api_passphrase)
            .field("private_key", &self.private_key)
            .field("jwt_token", &self.jwt_token)
            .field("api_key_private_key", &self.api_key_private_key)
            .field("stark_private_key", &self.stark_private_key)
            .field("wallet_address", &self.wallet_address)
            .field("sub_account_id", &self.sub_account_id)
            .field("l2_address", &self.l2_address)
            .field("account_id", &self.account_id)
            .field("account_index", &self.account_index)
            .field("api_key_index", &self.api_key_index)
            .finish()
    }
}

/// Loads exchange credentials, applying process-environment overrides.
///
/// # Errors
///
/// Returns an error if the file, YAML, or numeric environment values are invalid.
pub fn load_exchange_auth(path: impl AsRef<Path>, exchange: &str) -> ConfigResult<ExchangeAuth> {
    load_exchange_auth_with_env(path, exchange, &ProcessEnvironment)
}

/// Loads exchange credentials with an injected environment source.
///
/// # Errors
///
/// Returns an error if the file, YAML, or numeric environment values are invalid.
pub fn load_exchange_auth_with_env(
    path: impl AsRef<Path>,
    exchange: &str,
    env: &impl EnvProvider,
) -> ConfigResult<ExchangeAuth> {
    let path = path.as_ref();
    let yaml = read_config_file(path)?;
    load_exchange_auth_from_str_with_env(exchange, &yaml, env)
}

/// Parses exchange credentials and applies process-environment overrides.
///
/// # Errors
///
/// Returns an error if the YAML or numeric environment values are invalid.
pub fn load_exchange_auth_from_str(exchange: &str, yaml: &str) -> ConfigResult<ExchangeAuth> {
    load_exchange_auth_from_str_with_env(exchange, yaml, &ProcessEnvironment)
}

/// Parses exchange credentials with an injected environment source.
///
/// # Errors
///
/// Returns an error if the YAML or numeric environment values are invalid.
pub fn load_exchange_auth_from_str_with_env(
    exchange: &str,
    yaml: &str,
    env: &impl EnvProvider,
) -> ConfigResult<ExchangeAuth> {
    let document: serde_yaml::Value = parse_yaml(yaml)?;
    let exchange_key = exchange.to_ascii_lowercase();
    let root = child(&document, &exchange_key).unwrap_or(&document);
    let prefix = exchange.to_ascii_uppercase().replace('-', "_");

    let mut auth = ExchangeAuth {
        api_key: Secret::new(yaml_string(root, "api_key").unwrap_or_default()),
        api_secret: Secret::new(yaml_string(root, "api_secret").unwrap_or_default()),
        api_passphrase: Secret::new(yaml_string(root, "api_passphrase").unwrap_or_default()),
        private_key: Secret::new(yaml_string(root, "private_key").unwrap_or_default()),
        jwt_token: Secret::new(yaml_string(root, "jwt_token").unwrap_or_default()),
        api_key_private_key: Secret::new(
            yaml_string(root, "api_key_private_key").unwrap_or_default(),
        ),
        stark_private_key: Secret::new(yaml_string(root, "stark_private_key").unwrap_or_default()),
        wallet_address: yaml_string(root, "wallet_address"),
        sub_account_id: yaml_string(root, "sub_account_id"),
        l2_address: yaml_string(root, "l2_address"),
        account_id: yaml_string(root, "account_id"),
        account_index: yaml_u64(root, "account_index")?,
        api_key_index: yaml_u64(root, "api_key_index")?,
    };

    overlay_secret(env, &format!("{prefix}_API_KEY"), &mut auth.api_key);
    overlay_secret(env, &format!("{prefix}_API_SECRET"), &mut auth.api_secret);
    overlay_secret(
        env,
        &format!("{prefix}_API_PASSPHRASE"),
        &mut auth.api_passphrase,
    );
    overlay_secret(env, &format!("{prefix}_PRIVATE_KEY"), &mut auth.private_key);
    overlay_secret(env, &format!("{prefix}_JWT_TOKEN"), &mut auth.jwt_token);
    overlay_secret(
        env,
        &format!("{prefix}_API_KEY_PRIVATE_KEY"),
        &mut auth.api_key_private_key,
    );
    overlay_secret(
        env,
        &format!("{prefix}_STARK_PRIVATE_KEY"),
        &mut auth.stark_private_key,
    );
    overlay_string(
        env,
        &format!("{prefix}_WALLET_ADDRESS"),
        &mut auth.wallet_address,
    );
    overlay_string(
        env,
        &format!("{prefix}_SUB_ACCOUNT_ID"),
        &mut auth.sub_account_id,
    );
    overlay_string(env, &format!("{prefix}_L2_ADDRESS"), &mut auth.l2_address);
    overlay_string(env, &format!("{prefix}_ACCOUNT_ID"), &mut auth.account_id);
    overlay_u64(
        env,
        &format!("{prefix}_ACCOUNT_INDEX"),
        &mut auth.account_index,
    )?;
    overlay_u64(
        env,
        &format!("{prefix}_API_KEY_INDEX"),
        &mut auth.api_key_index,
    )?;

    match exchange_key.as_str() {
        "hyperliquid" if auth.private_key.is_configured() => {
            if !auth.api_key.is_configured() {
                auth.api_key = auth.private_key.clone();
            }
            if !auth.api_secret.is_configured() {
                auth.api_secret = auth.private_key.clone();
            }
        }
        "lighter" if auth.api_key_private_key.is_configured() => {
            if !auth.api_key.is_configured() {
                auth.api_key = auth.api_key_private_key.clone();
            }
            if !auth.api_secret.is_configured() {
                auth.api_secret = auth.api_key_private_key.clone();
            }
        }
        "edgex" if auth.stark_private_key.is_configured() && !auth.api_key.is_configured() => {
            auth.api_key = auth.stark_private_key.clone();
        }
        _ => {}
    }

    Ok(auth)
}

fn child<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    value.as_mapping()?.get(serde_yaml::Value::from(key))
}

fn yaml_string(root: &serde_yaml::Value, field: &str) -> Option<String> {
    let candidates: &[&[&str]] = &[
        &[field],
        &["authentication", field],
        &["auth", field],
        &["api_config", "auth", field],
        &["extra_params", field],
    ];
    candidates.iter().find_map(|path| {
        nested(root, path)
            .and_then(serde_yaml::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
    })
}

fn yaml_u64(root: &serde_yaml::Value, field: &str) -> ConfigResult<Option<u64>> {
    let candidates: &[&[&str]] = &[
        &[field],
        &["authentication", field],
        &["auth", field],
        &["api_config", "auth", field],
        &["extra_params", field],
    ];
    for path in candidates {
        if let Some(value) = nested(root, path) {
            if let Some(number) = value.as_u64() {
                return Ok(Some(number));
            }
            if let Some(text) = value.as_str() {
                return text.parse().map(Some).map_err(|_| {
                    ConfigError::Validation(format!("{field} must be an unsigned integer"))
                });
            }
        }
    }
    Ok(None)
}

fn nested<'a>(root: &'a serde_yaml::Value, path: &[&str]) -> Option<&'a serde_yaml::Value> {
    path.iter().try_fold(root, |value, key| child(value, key))
}

fn overlay_secret(env: &impl EnvProvider, key: &str, target: &mut Secret) {
    if let Some(value) = env.get(key).filter(|value| !value.trim().is_empty()) {
        *target = Secret::new(value);
    }
}

fn overlay_string(env: &impl EnvProvider, key: &str, target: &mut Option<String>) {
    if let Some(value) = env.get(key).filter(|value| !value.trim().is_empty()) {
        *target = Some(value);
    }
}

fn overlay_u64(env: &impl EnvProvider, key: &str, target: &mut Option<u64>) -> ConfigResult<()> {
    if let Some(value) = env.get(key).filter(|value| !value.trim().is_empty()) {
        *target = Some(
            value
                .parse()
                .map_err(|_| ConfigError::InvalidEnvironmentNumber {
                    key: key.to_owned(),
                })?,
        );
    }
    Ok(())
}
