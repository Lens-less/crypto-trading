use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DecisionRecord {
    pub timestamp: DateTime<Utc>,
    pub strategy: String,
    pub symbol: String,
    pub decision: String,
    pub details: Value,
}

/// Append-only JSONL sink. A complete record is written with one file append.
#[derive(Clone, Debug)]
pub struct JsonlHistory {
    path: PathBuf,
}

impl JsonlHistory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one complete JSON record and flushes it to the file handle.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] when directory creation, serialization, file
    /// opening, writing, or flushing fails.
    pub async fn append(&self, record: &DecisionRecord) -> Result<(), HistoryError> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(HistoryError::CreateDirectory)?;
            }
        }

        let mut encoded = serde_json::to_vec(record).map_err(HistoryError::Serialize)?;
        encoded.push(b'\n');

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(HistoryError::Open)?;
        file.write_all(&encoded)
            .await
            .map_err(HistoryError::Write)?;
        file.flush().await.map_err(HistoryError::Flush)
    }
}

#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("failed to create history directory: {0}")]
    CreateDirectory(std::io::Error),
    #[error("failed to serialize decision record: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to open history file: {0}")]
    Open(std::io::Error),
    #[error("failed to append history record: {0}")]
    Write(std::io::Error),
    #[error("failed to flush history file: {0}")]
    Flush(std::io::Error),
}
