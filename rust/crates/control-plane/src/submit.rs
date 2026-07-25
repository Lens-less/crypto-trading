//! Versioned, transport-independent command schema for trusted paper-task hosts.
//!
//! This module describes commands; it does not grant execution authority.
//! Transport adapters must hand validated envelopes to a trusted submit host
//! and read outcomes back from the durable journal projections.

use crypto_trading_runtime::{PaperAccountError, PaperReconciliationProof};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const SUBMIT_SCHEMA_VERSION: u16 = 1;

const MAX_IDENTITY_BYTES: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitEnvelope {
    schema_version: u16,
    command_id: Uuid,
    idempotency_key: String,
    target_task_id: String,
    permission: SubmitPermission,
    risk_confirmation: SubmitRiskConfirmation,
    command: SubmitCommand,
}

impl SubmitEnvelope {
    /// Creates and validates one fail-closed submit envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitValidationError`] when any identity is unsafe, when the
    /// schema or command id is invalid, or when permission and risk context do
    /// not match the command.
    pub fn new(
        command_id: Uuid,
        idempotency_key: impl Into<String>,
        target_task_id: impl Into<String>,
        permission: SubmitPermission,
        risk_confirmation: SubmitRiskConfirmation,
        command: SubmitCommand,
    ) -> Result<Self, SubmitValidationError> {
        let envelope = Self {
            schema_version: SUBMIT_SCHEMA_VERSION,
            command_id,
            idempotency_key: idempotency_key.into(),
            target_task_id: target_task_id.into(),
            permission,
            risk_confirmation,
            command,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Revalidates a deserialized envelope before it crosses the trusted seam.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitValidationError`] for any unsupported, unbounded, or
    /// internally inconsistent input.
    pub fn validate(&self) -> Result<(), SubmitValidationError> {
        if self.schema_version != SUBMIT_SCHEMA_VERSION {
            return Err(SubmitValidationError::UnsupportedSchema(
                self.schema_version,
            ));
        }
        if self.command_id.is_nil() {
            return Err(SubmitValidationError::NilCommandId);
        }
        validate_identity(&self.idempotency_key, "idempotency key")?;
        validate_identity(&self.target_task_id, "target task id")?;
        self.permission.validate()?;
        self.command.validate()?;

        let expected_role = self.command.required_role();
        if self.permission.role != expected_role {
            return Err(SubmitValidationError::PermissionMismatch {
                expected: expected_role,
                actual: self.permission.role,
            });
        }
        let expected_confirmation = self.command.required_risk_confirmation();
        if self.risk_confirmation != expected_confirmation {
            return Err(SubmitValidationError::RiskConfirmationMismatch {
                expected: expected_confirmation,
                actual: self.risk_confirmation,
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    #[must_use]
    pub const fn command_id(&self) -> Uuid {
        self.command_id
    }

    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    #[must_use]
    pub fn target_task_id(&self) -> &str {
        &self.target_task_id
    }

    #[must_use]
    pub const fn permission(&self) -> &SubmitPermission {
        &self.permission
    }

    #[must_use]
    pub const fn risk_confirmation(&self) -> SubmitRiskConfirmation {
        self.risk_confirmation
    }

    #[must_use]
    pub const fn command(&self) -> &SubmitCommand {
        &self.command
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitPermission {
    principal_id: String,
    role: SubmitRole,
}

impl SubmitPermission {
    /// Creates a bounded permission context.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitValidationError`] for an empty, padded, controlled, or
    /// overlong principal identity.
    pub fn new(
        principal_id: impl Into<String>,
        role: SubmitRole,
    ) -> Result<Self, SubmitValidationError> {
        let permission = Self {
            principal_id: principal_id.into(),
            role,
        };
        permission.validate()?;
        Ok(permission)
    }

    fn validate(&self) -> Result<(), SubmitValidationError> {
        validate_identity(&self.principal_id, "principal id")
    }

    #[must_use]
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    #[must_use]
    pub const fn role(&self) -> SubmitRole {
        self.role
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmitRole {
    PaperOperator,
    Reconciler,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmitRiskConfirmation {
    PaperOnly,
    ReconciliationEvidenceVerified,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmitCommand {
    StartPaperArbitrage {
        strategy_id: String,
        strategy_revision: String,
    },
    StartPaperGrid {
        strategy_id: String,
        strategy_revision: String,
    },
    StopTask,
    CancelTask,
    ReconcileRelease {
        proof: PaperReconciliationProof,
    },
    RecordReconcileFailure {
        proof: PaperReconciliationProof,
    },
}

impl SubmitCommand {
    fn validate(&self) -> Result<(), SubmitValidationError> {
        match self {
            Self::StartPaperArbitrage {
                strategy_id,
                strategy_revision,
            }
            | Self::StartPaperGrid {
                strategy_id,
                strategy_revision,
            } => {
                validate_identity(strategy_id, "strategy id")?;
                validate_identity(strategy_revision, "strategy revision")
            }
            Self::StopTask | Self::CancelTask => Ok(()),
            Self::ReconcileRelease { proof } | Self::RecordReconcileFailure { proof } => {
                validate_reconciliation_proof(proof)
            }
        }
    }

    const fn required_role(&self) -> SubmitRole {
        match self {
            Self::StartPaperArbitrage { .. }
            | Self::StartPaperGrid { .. }
            | Self::StopTask
            | Self::CancelTask => SubmitRole::PaperOperator,
            Self::ReconcileRelease { .. } | Self::RecordReconcileFailure { .. } => {
                SubmitRole::Reconciler
            }
        }
    }

    const fn required_risk_confirmation(&self) -> SubmitRiskConfirmation {
        match self {
            Self::StartPaperArbitrage { .. }
            | Self::StartPaperGrid { .. }
            | Self::StopTask
            | Self::CancelTask => SubmitRiskConfirmation::PaperOnly,
            Self::ReconcileRelease { .. } | Self::RecordReconcileFailure { .. } => {
                SubmitRiskConfirmation::ReconciliationEvidenceVerified
            }
        }
    }
}

fn validate_reconciliation_proof(
    proof: &PaperReconciliationProof,
) -> Result<(), SubmitValidationError> {
    PaperReconciliationProof::new(
        proof.account_id(),
        proof.reservation_id(),
        proof.batch_id(),
        proof.snapshot_id(),
        proof.snapshot_sequence(),
        proof.digest_algorithm(),
        proof.digest(),
    )
    .map(|_| ())
    .map_err(SubmitValidationError::InvalidReconciliationProof)
}

fn validate_identity(value: &str, label: &'static str) -> Result<(), SubmitValidationError> {
    if value.is_empty() {
        return Err(SubmitValidationError::InvalidIdentity {
            label,
            reason: "must not be empty",
        });
    }
    if value.len() > MAX_IDENTITY_BYTES {
        return Err(SubmitValidationError::InvalidIdentity {
            label,
            reason: "exceeds 128 bytes",
        });
    }
    if value.trim() != value {
        return Err(SubmitValidationError::InvalidIdentity {
            label,
            reason: "must not have surrounding whitespace",
        });
    }
    if value.chars().any(char::is_control) {
        return Err(SubmitValidationError::InvalidIdentity {
            label,
            reason: "must not contain control characters",
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SubmitValidationError {
    #[error("unsupported submit schema version {0}")]
    UnsupportedSchema(u16),
    #[error("submit command id must not be nil")]
    NilCommandId,
    #[error("invalid {label}: {reason}")]
    InvalidIdentity {
        label: &'static str,
        reason: &'static str,
    },
    #[error("submit permission mismatch: expected {expected:?}, got {actual:?}")]
    PermissionMismatch {
        expected: SubmitRole,
        actual: SubmitRole,
    },
    #[error("submit risk confirmation mismatch: expected {expected:?}, got {actual:?}")]
    RiskConfirmationMismatch {
        expected: SubmitRiskConfirmation,
        actual: SubmitRiskConfirmation,
    },
    #[error("invalid reconciliation proof: {0}")]
    InvalidReconciliationProof(PaperAccountError),
}
