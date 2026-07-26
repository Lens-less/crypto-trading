//! Versioned, transport-independent command schema for trusted paper-task hosts.
//!
//! This module describes commands; it does not grant execution authority.
//! Transport adapters must hand validated envelopes to a trusted submit host
//! and read outcomes back from the durable journal projections.

use std::{
    collections::HashMap,
    future::Future,
    io::ErrorKind,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use chrono::Utc;
use crypto_trading_runtime::{
    DecisionRecord, FileJournalSnapshotSource, HistoryError, JournalPageBoundary, JournalReadError,
    JournalSnapshot, JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader,
    PaperAccountError, PaperReconciliationProof,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const SUBMIT_SCHEMA_VERSION: u16 = 1;
pub const SUBMIT_JOURNAL_PROJECTION: &str = "submit_command_v1";
pub const SUBMIT_JOURNAL_SOURCE: &str = "durable_journal";

const MAX_IDENTITY_BYTES: usize = 128;
const SUBMIT_ACCEPTED_DECISION: &str = "submit_accepted";
const SUBMIT_TERMINAL_DECISION: &str = "submit_terminal";
const SUBMIT_JOURNAL_STRATEGY: &str = "trusted-submit";
const MIN_SUBMIT_GATE_CLEANUP_SIZE: usize = 64;
static SUBMIT_GATES: OnceLock<StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();

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
    /// Dedicated stronger confirmation for the latching account kill switch:
    /// the caller explicitly acknowledges that every later admission is
    /// refused and open paper positions must be closed.
    AccountKillSwitchArmed,
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
    PauseAccountRisk {
        reason: String,
    },
    ResumeAccountRisk,
    EngageAccountKillSwitch {
        reason: String,
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
            Self::StopTask | Self::CancelTask | Self::ResumeAccountRisk => Ok(()),
            Self::ReconcileRelease { proof } | Self::RecordReconcileFailure { proof } => {
                validate_reconciliation_proof(proof)
            }
            Self::PauseAccountRisk { reason } | Self::EngageAccountKillSwitch { reason } => {
                validate_identity(reason, "risk reason")
            }
        }
    }

    const fn required_role(&self) -> SubmitRole {
        match self {
            Self::StartPaperArbitrage { .. }
            | Self::StartPaperGrid { .. }
            | Self::StopTask
            | Self::CancelTask
            | Self::PauseAccountRisk { .. }
            | Self::ResumeAccountRisk
            | Self::EngageAccountKillSwitch { .. } => SubmitRole::PaperOperator,
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
            | Self::CancelTask
            | Self::PauseAccountRisk { .. }
            | Self::ResumeAccountRisk => SubmitRiskConfirmation::PaperOnly,
            Self::ReconcileRelease { .. } | Self::RecordReconcileFailure { .. } => {
                SubmitRiskConfirmation::ReconciliationEvidenceVerified
            }
            Self::EngageAccountKillSwitch { .. } => SubmitRiskConfirmation::AccountKillSwitchArmed,
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

/// Owned async result returned by a trusted command dispatcher.
pub type SubmitDispatchFuture =
    Pin<Box<dyn Future<Output = SubmitDispatchOutcome> + Send + 'static>>;

/// Execution seam injected by the trusted composition root.
///
/// The control plane owns validation, durability, and idempotency. A
/// dispatcher owns only the command-specific side effect and returns a bounded
/// classification without transport or secret-bearing error text.
pub trait SubmitDispatcher: Send + Sync {
    fn dispatch(&self, envelope: SubmitEnvelope) -> SubmitDispatchFuture;
}

/// Bounded outcome reported by a trusted command dispatcher.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitDispatchOutcome {
    Applied,
    Rejected,
    OutcomeUnknown,
}

/// Durable command state exposed to trusted transports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmitStatus {
    Applied,
    Rejected,
    OutcomeUnknown,
}

impl From<SubmitDispatchOutcome> for SubmitStatus {
    fn from(outcome: SubmitDispatchOutcome) -> Self {
        match outcome {
            SubmitDispatchOutcome::Applied => Self::Applied,
            SubmitDispatchOutcome::Rejected => Self::Rejected,
            SubmitDispatchOutcome::OutcomeUnknown => Self::OutcomeUnknown,
        }
    }
}

/// Journal-derived response for a submitted command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitReceipt {
    schema_version: u16,
    command_id: Uuid,
    target_task_id: String,
    status: SubmitStatus,
    journal_projection: String,
    source: String,
}

impl SubmitReceipt {
    fn from_envelope(envelope: &SubmitEnvelope, status: SubmitStatus) -> Self {
        Self {
            schema_version: SUBMIT_SCHEMA_VERSION,
            command_id: envelope.command_id(),
            target_task_id: envelope.target_task_id().to_owned(),
            status,
            journal_projection: SUBMIT_JOURNAL_PROJECTION.to_owned(),
            source: SUBMIT_JOURNAL_SOURCE.to_owned(),
        }
    }

    fn validate(&self) -> Result<(), SubmitServiceError> {
        if self.schema_version != SUBMIT_SCHEMA_VERSION
            || self.command_id.is_nil()
            || self.journal_projection != SUBMIT_JOURNAL_PROJECTION
            || self.source != SUBMIT_JOURNAL_SOURCE
        {
            return Err(SubmitServiceError::InvalidJournal);
        }
        validate_identity(&self.target_task_id, "target task id")
            .map_err(|_| SubmitServiceError::InvalidJournal)
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
    pub fn target_task_id(&self) -> &str {
        &self.target_task_id
    }

    #[must_use]
    pub const fn status(&self) -> SubmitStatus {
        self.status
    }

    #[must_use]
    pub fn journal_projection(&self) -> &str {
        &self.journal_projection
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// Durable, transport-independent trusted submit service.
///
/// Each new command is written as an accepted fact before dispatch. A second
/// terminal fact records the bounded dispatcher outcome. On restart, an
/// accepted command without a terminal fact is reported as
/// [`SubmitStatus::OutcomeUnknown`] and is never dispatched again.
#[derive(Clone)]
pub struct SubmitService {
    journal_id: Uuid,
    history: JsonlHistory,
    source: FileJournalSnapshotSource,
    dispatcher: Arc<dyn SubmitDispatcher>,
    submit_gate: Arc<Mutex<()>>,
}

impl SubmitService {
    /// Creates one trusted submit authority for a durable journal generation.
    ///
    /// # Errors
    ///
    /// Returns [`JournalReadError`] when the journal identity or path cannot be
    /// represented by the bounded journal reader.
    pub fn new(
        journal_id: Uuid,
        history_path: impl Into<PathBuf>,
        dispatcher: Arc<dyn SubmitDispatcher>,
    ) -> Result<Self, JournalReadError> {
        let source = FileJournalSnapshotSource::new(journal_id, history_path.into())?;
        let history = JsonlHistory::new(source.path());
        let submit_gate = shared_submit_gate(source.path());
        Ok(Self {
            journal_id,
            history,
            source,
            dispatcher,
            submit_gate,
        })
    }

    /// Returns the durable journal generation bound to this submit authority.
    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    /// Validates, durably binds, and dispatches one command at most once.
    ///
    /// Replays return the durable terminal receipt. If the journal contains
    /// only the accepted fact, the receipt is `outcome_unknown`; dispatch is
    /// deliberately not retried because the prior side effect cannot be
    /// disproved.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitServiceError`] when validation fails, either idempotency
    /// identifier is already bound to a different envelope, or the journal
    /// cannot establish a safe pre-dispatch state.
    pub async fn submit(
        &self,
        envelope: SubmitEnvelope,
    ) -> Result<SubmitReceipt, SubmitServiceError> {
        envelope.validate()?;
        let _guard = self.submit_gate.lock().await;

        match self.project_binding(&envelope).await? {
            ExistingBinding::Terminal(receipt) => return Ok(receipt),
            ExistingBinding::AcceptedOnly => {
                return Ok(SubmitReceipt::from_envelope(
                    &envelope,
                    SubmitStatus::OutcomeUnknown,
                ));
            }
            ExistingBinding::Absent => {}
        }

        self.append_fact(
            SUBMIT_ACCEPTED_DECISION,
            &SubmitJournalFact::Accepted {
                schema_version: SUBMIT_SCHEMA_VERSION,
                envelope: Box::new(envelope.clone()),
            },
            envelope.target_task_id(),
        )
        .await?;

        let outcome = self.dispatcher.dispatch(envelope.clone()).await;
        let receipt = SubmitReceipt::from_envelope(&envelope, outcome.into());
        let terminal = SubmitJournalFact::Terminal {
            schema_version: SUBMIT_SCHEMA_VERSION,
            idempotency_key: envelope.idempotency_key().to_owned(),
            receipt: receipt.clone(),
        };
        if self
            .append_fact(
                SUBMIT_TERMINAL_DECISION,
                &terminal,
                envelope.target_task_id(),
            )
            .await
            .is_err()
        {
            return Ok(SubmitReceipt::from_envelope(
                &envelope,
                SubmitStatus::OutcomeUnknown,
            ));
        }
        match self.project_binding(&envelope).await {
            Ok(ExistingBinding::Terminal(durable_receipt)) => Ok(durable_receipt),
            Ok(ExistingBinding::Absent | ExistingBinding::AcceptedOnly) | Err(_) => Ok(
                SubmitReceipt::from_envelope(&envelope, SubmitStatus::OutcomeUnknown),
            ),
        }
    }

    async fn project_binding(
        &self,
        envelope: &SubmitEnvelope,
    ) -> Result<ExistingBinding, SubmitServiceError> {
        let source = self.source.clone();
        let journal_id = self.journal_id;
        let source_path = source.path().to_owned();
        let snapshot = tokio::task::spawn_blocking(move || match std::fs::metadata(&source_path) {
            Ok(_) => source.snapshot(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                JournalSnapshot::new(journal_id, Vec::new())
            }
            Err(error) => Err(JournalReadError::Metadata(error)),
        })
        .await
        .map_err(|_| SubmitServiceError::SnapshotTaskFailed)??;
        project_binding_from_snapshot(&snapshot, envelope)
    }

    async fn append_fact(
        &self,
        decision: &'static str,
        fact: &SubmitJournalFact,
        target_task_id: &str,
    ) -> Result<(), SubmitServiceError> {
        let fact = serde_json::to_value(fact).map_err(|_| SubmitServiceError::InvalidJournal)?;
        self.history
            .append(&DecisionRecord {
                timestamp: Utc::now(),
                strategy: SUBMIT_JOURNAL_STRATEGY.to_owned(),
                symbol: target_task_id.to_owned(),
                decision: decision.to_owned(),
                details: serde_json::json!({ "submit": fact }),
            })
            .await?;
        Ok(())
    }
}

fn shared_submit_gate(path: &Path) -> Arc<Mutex<()>> {
    let gates = SUBMIT_GATES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut gates = gates
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(gate) = gates.get(path).and_then(Weak::upgrade) {
        return gate;
    }
    if gates.len() >= MIN_SUBMIT_GATE_CLEANUP_SIZE {
        gates.retain(|_, gate| gate.strong_count() > 0);
    }
    let gate = Arc::new(Mutex::new(()));
    gates.insert(path.to_owned(), Arc::downgrade(&gate));
    gate
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
enum SubmitJournalFact {
    Accepted {
        schema_version: u16,
        envelope: Box<SubmitEnvelope>,
    },
    Terminal {
        schema_version: u16,
        idempotency_key: String,
        receipt: SubmitReceipt,
    },
}

enum ExistingBinding {
    Absent,
    AcceptedOnly,
    Terminal(SubmitReceipt),
}

fn project_binding_from_snapshot(
    snapshot: &JournalSnapshot,
    incoming: &SubmitEnvelope,
) -> Result<ExistingBinding, SubmitServiceError> {
    let mut cursor = None;
    let mut accepted = false;
    let mut terminal = None;
    loop {
        let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
        for event in page.events() {
            let Some(fact) = submit_fact(event.payload())? else {
                continue;
            };
            match fact {
                SubmitJournalFact::Accepted {
                    schema_version,
                    envelope,
                } => {
                    if schema_version != SUBMIT_SCHEMA_VERSION || envelope.validate().is_err() {
                        return Err(SubmitServiceError::InvalidJournal);
                    }
                    if identifiers_collide(&envelope, incoming) {
                        if envelope.as_ref() != incoming {
                            return Err(SubmitServiceError::Conflict);
                        }
                        if accepted {
                            return Err(SubmitServiceError::InvalidJournal);
                        }
                        accepted = true;
                    }
                }
                SubmitJournalFact::Terminal {
                    schema_version,
                    idempotency_key,
                    receipt,
                } => {
                    if schema_version != SUBMIT_SCHEMA_VERSION
                        || validate_identity(&idempotency_key, "idempotency key").is_err()
                        || receipt.validate().is_err()
                    {
                        return Err(SubmitServiceError::InvalidJournal);
                    }
                    let collides = receipt.command_id == incoming.command_id()
                        || idempotency_key == incoming.idempotency_key();
                    if !collides {
                        continue;
                    }
                    if receipt.command_id != incoming.command_id()
                        || idempotency_key != incoming.idempotency_key()
                        || receipt.target_task_id != incoming.target_task_id()
                    {
                        return Err(SubmitServiceError::Conflict);
                    }
                    if !accepted || terminal.replace(receipt).is_some() {
                        return Err(SubmitServiceError::InvalidJournal);
                    }
                }
            }
        }
        cursor = page.next_cursor().cloned();
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => break,
            JournalPageBoundary::PartialTail { .. } => {
                return Err(SubmitServiceError::IncompleteJournal);
            }
            JournalPageBoundary::PageLimit => {
                if cursor.is_none() {
                    return Err(SubmitServiceError::InvalidJournal);
                }
            }
        }
    }

    if let Some(receipt) = terminal {
        Ok(ExistingBinding::Terminal(receipt))
    } else if accepted {
        Ok(ExistingBinding::AcceptedOnly)
    } else {
        Ok(ExistingBinding::Absent)
    }
}

fn identifiers_collide(left: &SubmitEnvelope, right: &SubmitEnvelope) -> bool {
    left.command_id() == right.command_id() || left.idempotency_key() == right.idempotency_key()
}

fn submit_fact(payload: &Value) -> Result<Option<SubmitJournalFact>, SubmitServiceError> {
    let Some(decision) = payload.get("decision").and_then(Value::as_str) else {
        return Err(SubmitServiceError::InvalidJournal);
    };
    if !matches!(
        decision,
        SUBMIT_ACCEPTED_DECISION | SUBMIT_TERMINAL_DECISION
    ) {
        return Ok(None);
    }
    let fact = payload
        .get("details")
        .and_then(|details| details.get("submit"))
        .cloned()
        .ok_or(SubmitServiceError::InvalidJournal)
        .and_then(|value| {
            serde_json::from_value::<SubmitJournalFact>(value)
                .map_err(|_| SubmitServiceError::InvalidJournal)
        })?;
    if matches!(
        (decision, &fact),
        (SUBMIT_ACCEPTED_DECISION, SubmitJournalFact::Accepted { .. })
            | (SUBMIT_TERMINAL_DECISION, SubmitJournalFact::Terminal { .. })
    ) {
        Ok(Some(fact))
    } else {
        Err(SubmitServiceError::InvalidJournal)
    }
}

/// Stable class for transport-level fail-closed error mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitFailureKind {
    InvalidEnvelope,
    Conflict,
    JournalUnavailable,
    InvalidJournal,
}

#[derive(Debug, Error)]
pub enum SubmitServiceError {
    #[error(transparent)]
    Validation(#[from] SubmitValidationError),
    #[error("submit identifiers are already bound to a different envelope")]
    Conflict,
    #[error("submit journal contains an invalid command projection")]
    InvalidJournal,
    #[error("submit journal ended with an incomplete record")]
    IncompleteJournal,
    #[error("submit journal snapshot task failed")]
    SnapshotTaskFailed,
    #[error(transparent)]
    JournalRead(#[from] JournalReadError),
    #[error(transparent)]
    JournalWrite(#[from] HistoryError),
}

impl SubmitServiceError {
    #[must_use]
    pub const fn kind(&self) -> SubmitFailureKind {
        match self {
            Self::Validation(_) => SubmitFailureKind::InvalidEnvelope,
            Self::Conflict => SubmitFailureKind::Conflict,
            Self::InvalidJournal | Self::IncompleteJournal => SubmitFailureKind::InvalidJournal,
            Self::SnapshotTaskFailed | Self::JournalRead(_) | Self::JournalWrite(_) => {
                SubmitFailureKind::JournalUnavailable
            }
        }
    }

    #[must_use]
    pub const fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict)
    }
}
