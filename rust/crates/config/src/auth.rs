use std::{fmt, path::Path};

use crate::{
    ConfigResult,
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
    pub private_key: Secret,
    pub wallet_address: Option<String>,
}

impl fmt::Debug for ExchangeAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExchangeAuth")
            .field("api_key", &self.api_key)
            .field("api_secret", &self.api_secret)
            .field("private_key", &self.private_key)
            .field("wallet_address", &self.wallet_address)
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
        private_key: Secret::new(yaml_string(root, "private_key").unwrap_or_default()),
        wallet_address: yaml_string(root, "wallet_address"),
    };

    overlay_secret(env, &format!("{prefix}_API_KEY"), &mut auth.api_key);
    overlay_secret(env, &format!("{prefix}_API_SECRET"), &mut auth.api_secret);
    overlay_secret(env, &format!("{prefix}_PRIVATE_KEY"), &mut auth.private_key);
    overlay_string(
        env,
        &format!("{prefix}_WALLET_ADDRESS"),
        &mut auth.wallet_address,
    );

    if exchange_key == "hyperliquid" && auth.private_key.is_configured() {
        if !auth.api_key.is_configured() {
            auth.api_key = auth.private_key.clone();
        }
        if !auth.api_secret.is_configured() {
            auth.api_secret = auth.private_key.clone();
        }
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
