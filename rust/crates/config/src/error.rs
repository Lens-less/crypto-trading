use std::{io, path::PathBuf};

use thiserror::Error;

pub type ConfigResult<T> = Result<T, ConfigError>;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid YAML configuration: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid configuration: {0}")]
    Validation(String),
    #[error("environment variable {key} must be an unsigned integer")]
    InvalidEnvironmentNumber { key: String },
}
