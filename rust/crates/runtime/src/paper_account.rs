//! Durable, paper-only pending-reservation authority.
//!
//! This module deliberately separates account reservation truth from execution
//! outcome truth. Authorities cold-replay the synchronized JSONL journal once,
//! then refresh one shared in-memory projection from durable append receipts
//! before another mutation is admitted. The shared [`JsonlHistory`] writer
//! owns a sibling lock-file lease, so competing processes fail closed before
//! appending to the same normalized journal path.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use chrono::Utc;
use crypto_trading_domain::{
    MarketType, Money, OrderIntent, OrderStatus, Price, Quantity, Side, Symbol,
};
use crypto_trading_exchange::TradingReceipt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

use crate::authority_state::{AuthorityStateCache, AuthorityStateError};
use crate::{
    DecisionRecord, HistoryError, JournalPageBoundary, JournalReadError, JournalSnapshot,
    JsonlHistory, LegacyJsonlJournalReader, OperationEventEnvelope, ProjectionStatus,
};

pub const PAPER_ACCOUNT_SCHEMA_VERSION: u16 = 1;
pub const PAPER_COST_MODEL_VERSION: u16 = 1;
pub const MAX_PAPER_ACCOUNT_RESERVATIONS: usize = 1_024;

const MAX_PAPER_ACCOUNTS: usize = 16;
const MAX_RESERVATION_LEGS: usize = 256;
const MAX_LABEL_BYTES: usize = 128;
const MAX_REASON_BYTES: usize = 128;
const RECONCILIATION_DIGEST_HEX_BYTES: usize = 16;
const MAX_RECONCILIATION_MISMATCHES: usize = 16;
const MAX_RECONCILIATION_EVIDENCE_BYTES: usize = 32 * 1_024;
const MAX_COST_BPS: u32 = 10_000;
const PAPER_ACCOUNT_STRATEGY: &str = "paper_account";
const PAPER_ACCOUNT_INITIALIZED: &str = "paper_account_initialized";
pub(crate) const PAPER_ACCOUNT_RESERVED: &str = "paper_account_reserved";
const PAPER_ACCOUNT_UNCERTAIN: &str = "paper_account_uncertain";
const PAPER_ACCOUNT_COMMITTED: &str = "paper_account_committed";
const PAPER_ACCOUNT_RELEASED: &str = "paper_account_released";
const PAPER_ACCOUNT_RECONCILE_FAILED: &str = "paper_account_reconcile_failed";
const PAPER_ACCOUNT_EXECUTION_SETTLED: &str = "paper_account_execution_settled";

pub(crate) type AuthorityLock = AsyncMutex<()>;
static AUTHORITY_LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Weak<AuthorityLock>>>> = OnceLock::new();
pub(crate) type OperationLock = AsyncMutex<()>;
static OPERATION_LOCKS: OnceLock<StdMutex<HashMap<OperationLockKey, Weak<OperationLock>>>> =
    OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperationLockKey {
    path: PathBuf,
    account_id: String,
}

/// Process-local exclusive lease for one journal-backed paper operation lane.
///
/// Hold this guard from the first decision snapshot through reserve,
/// execution, and settlement when one owner must ensure no sibling authority
/// instance can interleave another account operation on the same normalized
/// journal path.
#[must_use = "dropping the lease releases exclusive paper-account operation access"]
pub struct PaperAccountOperationLease {
    inner: Arc<PaperAccountOperationLeaseInner>,
}

struct PaperAccountOperationLeaseInner {
    _guard: OwnedMutexGuard<()>,
}

impl Clone for PaperAccountOperationLease {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for PaperAccountOperationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaperAccountOperationLease")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaperAccountConfig {
    account_id: String,
    initial_available: Money,
}

impl PaperAccountConfig {
    /// Creates one bounded paper account identity and starting quote capacity.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidConfig`] for an unsafe identity or a
    /// non-positive initial capacity.
    pub fn new(
        account_id: impl Into<String>,
        initial_available: Money,
    ) -> Result<Self, PaperAccountError> {
        let account_id = account_id.into();
        let account_id = bounded_identity(&account_id, "account id")
            .map_err(PaperAccountError::InvalidConfig)?;
        if initial_available <= Money::default() {
            return Err(PaperAccountError::InvalidConfig(
                "initial paper availability must be positive",
            ));
        }
        Ok(Self {
            account_id,
            initial_available,
        })
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub const fn initial_available(&self) -> Money {
        self.initial_available
    }
}

/// Explicit conservative paper cost buffers. Version 1 reserves each buffer
/// against gross leg notional; it does not claim exchange-realistic pricing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperCostModel {
    version: u16,
    fee_bps: u32,
    funding_buffer_bps: u32,
    slippage_bps: u32,
}

impl PaperCostModel {
    /// Builds the only cost model supported by this schema.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidRequest`] when any independent
    /// buffer exceeds 100%.
    pub fn v1(
        fee_bps: u32,
        funding_buffer_bps: u32,
        slippage_bps: u32,
    ) -> Result<Self, PaperAccountError> {
        let model = Self {
            version: PAPER_COST_MODEL_VERSION,
            fee_bps,
            funding_buffer_bps,
            slippage_bps,
        };
        model.validate()?;
        Ok(model)
    }

    #[must_use]
    pub const fn version(self) -> u16 {
        self.version
    }

    #[must_use]
    pub const fn fee_bps(self) -> u32 {
        self.fee_bps
    }

    #[must_use]
    pub const fn funding_buffer_bps(self) -> u32 {
        self.funding_buffer_bps
    }

    #[must_use]
    pub const fn slippage_bps(self) -> u32 {
        self.slippage_bps
    }

    fn validate(self) -> Result<(), PaperAccountError> {
        if self.version != PAPER_COST_MODEL_VERSION {
            return Err(PaperAccountError::InvalidRequest(
                "unsupported paper cost model version",
            ));
        }
        if [self.fee_bps, self.funding_buffer_bps, self.slippage_bps]
            .into_iter()
            .any(|value| value > MAX_COST_BPS)
        {
            return Err(PaperAccountError::InvalidRequest(
                "paper cost buffers must not exceed 10000 bps",
            ));
        }
        self.total_bps()
            .ok_or(PaperAccountError::ArithmeticOverflow)?;
        Ok(())
    }

    fn total_bps(self) -> Option<u32> {
        self.fee_bps
            .checked_add(self.funding_buffer_bps)?
            .checked_add(self.slippage_bps)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperReservationLeg {
    index: usize,
    exchange: String,
    symbol: Symbol,
    market_type: MarketType,
    side: Side,
    #[serde(default)]
    client_order_id: Option<Uuid>,
    #[serde(default)]
    expected_quantity: Option<Quantity>,
    #[serde(default)]
    reduce_only: bool,
    reserved_notional: Money,
}

impl PaperReservationLeg {
    /// Captures the bounded account reservation identity for one execution leg.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidRequest`] for an unsafe exchange
    /// identity or non-positive reserved notional.
    pub fn from_intent(
        index: usize,
        intent: &OrderIntent,
        reserved_notional: Money,
    ) -> Result<Self, PaperAccountError> {
        let exchange = bounded_identity(&intent.exchange, "exchange")
            .map_err(PaperAccountError::InvalidRequest)?;
        if reserved_notional <= Money::default() {
            return Err(PaperAccountError::InvalidRequest(
                "reserved leg notional must be positive",
            ));
        }
        Ok(Self {
            index,
            exchange,
            symbol: intent.symbol.clone(),
            market_type: intent.market_type,
            side: intent.side,
            client_order_id: Some(intent.client_order_id),
            expected_quantity: Some(intent.quantity),
            reduce_only: intent.reduce_only,
            reserved_notional,
        })
    }

    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub const fn market_type(&self) -> MarketType {
        self.market_type
    }

    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    #[must_use]
    pub const fn client_order_id(&self) -> Option<Uuid> {
        self.client_order_id
    }

    #[must_use]
    pub const fn expected_quantity(&self) -> Option<Quantity> {
        self.expected_quantity
    }

    #[must_use]
    pub const fn reduce_only(&self) -> bool {
        self.reduce_only
    }

    #[must_use]
    pub const fn reserved_notional(&self) -> Money {
        self.reserved_notional
    }

    fn validate(&self, expected_index: usize) -> Result<(), PaperAccountError> {
        if self.index != expected_index {
            return Err(PaperAccountError::InvalidRequest(
                "paper reservation leg indexes must be contiguous",
            ));
        }
        bounded_identity(&self.exchange, "exchange").map_err(PaperAccountError::InvalidRequest)?;
        bounded_identity(self.symbol.as_str(), "symbol")
            .map_err(PaperAccountError::InvalidRequest)?;
        if self
            .expected_quantity
            .is_some_and(|quantity| quantity == Quantity::default())
        {
            return Err(PaperAccountError::InvalidRequest(
                "expected fill quantity must be positive when present",
            ));
        }
        if self.reduce_only && self.expected_quantity.is_none() {
            return Err(PaperAccountError::InvalidRequest(
                "reduce-only reservation legs require an exact quantity",
            ));
        }
        if self.reserved_notional <= Money::default() {
            return Err(PaperAccountError::InvalidRequest(
                "reserved leg notional must be positive",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperReservationRequest {
    reservation_id: Uuid,
    task_id: String,
    idempotency_key: String,
    batch_id: Uuid,
    cost_model: PaperCostModel,
    legs: Vec<PaperReservationLeg>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    risk_scope_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    risk_admission_ticket_id: Option<String>,
}

impl PaperReservationRequest {
    /// Creates a request with a new non-nil reservation identifier while
    /// retaining the caller-provided durable batch identifier.
    ///
    /// # Errors
    ///
    /// Returns the same validation failures as [`Self::new`].
    pub fn planned(
        task_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        batch_id: Uuid,
        cost_model: PaperCostModel,
        legs: Vec<PaperReservationLeg>,
    ) -> Result<Self, PaperAccountError> {
        let reservation_id = loop {
            let candidate = Uuid::new_v4();
            if !candidate.is_nil() {
                break candidate;
            }
        };
        Self::new(
            reservation_id,
            task_id,
            idempotency_key,
            batch_id,
            cost_model,
            legs,
        )
    }

    /// Creates a stable paper reservation request.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidRequest`] for nil identifiers,
    /// unsafe labels, empty/oversized legs, or incoherent leg indexes.
    pub fn new(
        reservation_id: Uuid,
        task_id: impl Into<String>,
        idempotency_key: impl Into<String>,
        batch_id: Uuid,
        cost_model: PaperCostModel,
        legs: Vec<PaperReservationLeg>,
    ) -> Result<Self, PaperAccountError> {
        let request = Self {
            reservation_id,
            task_id: task_id.into(),
            idempotency_key: idempotency_key.into(),
            batch_id,
            cost_model,
            legs,
            risk_scope_id: None,
            risk_admission_ticket_id: None,
        };
        request.validate()?;
        Ok(request)
    }

    #[must_use]
    pub const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    #[must_use]
    pub const fn batch_id(&self) -> Uuid {
        self.batch_id
    }

    #[must_use]
    pub const fn cost_model(&self) -> PaperCostModel {
        self.cost_model
    }

    #[must_use]
    pub fn legs(&self) -> &[PaperReservationLeg] {
        &self.legs
    }

    /// Binds this reservation to one exact live account-risk admission.
    ///
    /// The paper authority checks the ticket while holding the same journal
    /// serialization lock used by the risk authority. Consequently either a
    /// reservation consumes the ticket or expiry does; neither path can race
    /// past the other.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidRequest`] for an unsafe scope.
    pub fn with_account_risk_admission(
        mut self,
        scope_id: impl Into<String>,
        ticket: &crate::AccountRiskAdmissionTicket,
    ) -> Result<Self, PaperAccountError> {
        let scope_id = bounded_identity(&scope_id.into(), "scope id")
            .map_err(PaperAccountError::InvalidRequest)?;
        self.risk_scope_id = Some(scope_id);
        self.risk_admission_ticket_id = Some(ticket.as_str().to_owned());
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn risk_admission_binding(&self) -> Option<(&str, &str)> {
        self.risk_scope_id
            .as_deref()
            .zip(self.risk_admission_ticket_id.as_deref())
    }

    /// Returns gross reserved leg notional before the versioned paper cost
    /// buffers are applied.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::ArithmeticOverflow`] when the bounded
    /// decimal sum cannot be represented.
    pub fn gross_notional(&self) -> Result<Money, PaperAccountError> {
        self.legs.iter().try_fold(Money::default(), |total, leg| {
            checked_add_money(total, leg.reserved_notional)
                .ok_or(PaperAccountError::ArithmeticOverflow)
        })
    }

    fn validate(&self) -> Result<(), PaperAccountError> {
        if self.reservation_id.is_nil() || self.batch_id.is_nil() {
            return Err(PaperAccountError::InvalidRequest(
                "paper reservation and batch ids must not be nil",
            ));
        }
        bounded_identity(&self.task_id, "task id").map_err(PaperAccountError::InvalidRequest)?;
        bounded_identity(&self.idempotency_key, "idempotency key")
            .map_err(PaperAccountError::InvalidRequest)?;
        match (
            self.risk_scope_id.as_deref(),
            self.risk_admission_ticket_id.as_deref(),
        ) {
            (None, None) => {}
            (Some(scope_id), Some(ticket_id)) => {
                bounded_identity(scope_id, "scope id")
                    .map_err(PaperAccountError::InvalidRequest)?;
                if !super::account_risk::valid_ticket(ticket_id) {
                    return Err(PaperAccountError::InvalidRequest(
                        "paper risk admission ticket must be a non-nil UUID",
                    ));
                }
            }
            _ => {
                return Err(PaperAccountError::InvalidRequest(
                    "paper risk admission binding must include scope and ticket",
                ));
            }
        }
        if self.legs.is_empty() || self.legs.len() > MAX_RESERVATION_LEGS {
            return Err(PaperAccountError::InvalidRequest(
                "paper reservation leg count is outside the supported bound",
            ));
        }
        self.cost_model.validate()?;
        for (index, leg) in self.legs.iter().enumerate() {
            leg.validate(index)?;
        }
        let _ = reserved_exposure(self)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperReservationPhase {
    Pending,
    Uncertain,
    Committed,
    Released,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperReconciliationDigestAlgorithm {
    Fnv1a64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperReconciliationVerdict {
    Match,
    Mismatch,
}

/// Canonical bounded facts that a reconciliation proof commits to.
///
/// The authority revalidates these facts against the current local account
/// before releasing committed exposure. This keeps the journal proof
/// replayable instead of accepting an opaque caller-supplied digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperReconciliationEvidence {
    schema_version: u16,
    source: String,
    source_state_digest: String,
    mainnet_enabled: bool,
    account_id: String,
    reservation_id: Uuid,
    batch_id: Uuid,
    snapshot_id: String,
    snapshot_sequence: u64,
    stable_sample_count: u8,
    expected_available: Money,
    observed_wallet: Option<Money>,
    observed_available: Option<Money>,
    observed_locked: Option<Money>,
    owned_order_count: u32,
    foreign_order_count: u32,
    position_count: u32,
    untracked_asset_count: u32,
    verdict: PaperReconciliationVerdict,
    mismatches: Vec<String>,
}

impl PaperReconciliationEvidence {
    /// Creates a clean two-sample balance match for a committed reservation.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as [`Self::new`].
    #[allow(clippy::too_many_arguments)]
    pub fn clean_match(
        source: impl Into<String>,
        source_state_digest: impl Into<String>,
        account_id: impl Into<String>,
        reservation_id: Uuid,
        batch_id: Uuid,
        snapshot_id: impl Into<String>,
        snapshot_sequence: u64,
        expected_available: Money,
    ) -> Result<Self, PaperAccountError> {
        Self::new(
            source,
            source_state_digest,
            account_id,
            reservation_id,
            batch_id,
            snapshot_id,
            snapshot_sequence,
            2,
            expected_available,
            Some(expected_available),
            Some(expected_available),
            Some(Money::default()),
            0,
            0,
            0,
            0,
            Vec::new(),
        )
    }

    /// Creates a two-sample mismatch fact for a committed reservation.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as [`Self::new`].
    #[allow(clippy::too_many_arguments)]
    pub fn mismatch(
        source: impl Into<String>,
        source_state_digest: impl Into<String>,
        account_id: impl Into<String>,
        reservation_id: Uuid,
        batch_id: Uuid,
        snapshot_id: impl Into<String>,
        snapshot_sequence: u64,
        expected_available: Money,
        mismatch: impl Into<String>,
    ) -> Result<Self, PaperAccountError> {
        Self::new(
            source,
            source_state_digest,
            account_id,
            reservation_id,
            batch_id,
            snapshot_id,
            snapshot_sequence,
            2,
            expected_available,
            None,
            None,
            None,
            0,
            0,
            0,
            0,
            vec![mismatch.into()],
        )
    }

    /// Creates canonical reconciliation evidence from one bounded verifier.
    ///
    /// Empty mismatches encode a match verdict; non-empty mismatches encode a
    /// failure verdict. Mainnet is fixed off.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidRequest`] for unsafe identities,
    /// unbounded mismatch labels, invalid sample metadata, or contradictory
    /// match facts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: impl Into<String>,
        source_state_digest: impl Into<String>,
        account_id: impl Into<String>,
        reservation_id: Uuid,
        batch_id: Uuid,
        snapshot_id: impl Into<String>,
        snapshot_sequence: u64,
        stable_sample_count: u8,
        expected_available: Money,
        observed_wallet: Option<Money>,
        observed_available: Option<Money>,
        observed_locked: Option<Money>,
        owned_order_count: u32,
        foreign_order_count: u32,
        position_count: u32,
        untracked_asset_count: u32,
        mut mismatches: Vec<String>,
    ) -> Result<Self, PaperAccountError> {
        mismatches.sort();
        mismatches.dedup();
        let verdict = if mismatches.is_empty() {
            PaperReconciliationVerdict::Match
        } else {
            PaperReconciliationVerdict::Mismatch
        };
        let evidence = Self {
            schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
            source: source.into(),
            source_state_digest: source_state_digest.into(),
            mainnet_enabled: false,
            account_id: account_id.into(),
            reservation_id,
            batch_id,
            snapshot_id: snapshot_id.into(),
            snapshot_sequence,
            stable_sample_count,
            expected_available: normalized_money(expected_available),
            observed_wallet: observed_wallet.map(normalized_money),
            observed_available: observed_available.map(normalized_money),
            observed_locked: observed_locked.map(normalized_money),
            owned_order_count,
            foreign_order_count,
            position_count,
            untracked_asset_count,
            verdict,
            mismatches,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub const fn verdict(&self) -> PaperReconciliationVerdict {
        self.verdict
    }

    #[must_use]
    pub fn mismatches(&self) -> &[String] {
        &self.mismatches
    }

    fn validate(&self) -> Result<(), PaperAccountError> {
        if self.schema_version != PAPER_ACCOUNT_SCHEMA_VERSION
            || self.mainnet_enabled
            || self.reservation_id.is_nil()
            || self.batch_id.is_nil()
            || self.snapshot_sequence == 0
            || !(2..=16).contains(&self.stable_sample_count)
        {
            return Err(PaperAccountError::InvalidRequest(
                "paper reconciliation evidence metadata is invalid",
            ));
        }
        bounded_identity(&self.source, "reconciliation source")
            .map_err(PaperAccountError::InvalidRequest)?;
        bounded_digest(
            PaperReconciliationDigestAlgorithm::Fnv1a64,
            &self.source_state_digest,
        )?;
        bounded_identity(&self.account_id, "account id")
            .map_err(PaperAccountError::InvalidRequest)?;
        bounded_identity(&self.snapshot_id, "snapshot id")
            .map_err(PaperAccountError::InvalidRequest)?;
        if self.mismatches.len() > MAX_RECONCILIATION_MISMATCHES {
            return Err(PaperAccountError::InvalidRequest(
                "paper reconciliation mismatch count exceeds the supported bound",
            ));
        }
        let mut previous = None;
        for mismatch in &self.mismatches {
            bounded_reason(mismatch)?;
            if previous.is_some_and(|label| label >= mismatch.as_str()) {
                return Err(PaperAccountError::InvalidRequest(
                    "paper reconciliation mismatch labels must be unique and sorted",
                ));
            }
            previous = Some(mismatch.as_str());
        }
        let counts_are_zero = self.owned_order_count == 0
            && self.foreign_order_count == 0
            && self.position_count == 0
            && self.untracked_asset_count == 0;
        match self.verdict {
            PaperReconciliationVerdict::Match => {
                if !self.mismatches.is_empty()
                    || !counts_are_zero
                    || self.observed_available != Some(self.expected_available)
                    || self.observed_wallet != self.observed_available
                    || self
                        .observed_locked
                        .is_some_and(|locked| locked != Money::default())
                {
                    return Err(PaperAccountError::InvalidRequest(
                        "paper reconciliation match evidence is contradictory",
                    ));
                }
            }
            PaperReconciliationVerdict::Mismatch if self.mismatches.is_empty() => {
                return Err(PaperAccountError::InvalidRequest(
                    "paper reconciliation mismatch evidence needs a reason",
                ));
            }
            PaperReconciliationVerdict::Mismatch => {}
        }
        let encoded = serde_json::to_vec(self).map_err(PaperAccountError::Serialize)?;
        if encoded.len() > MAX_RECONCILIATION_EVIDENCE_BYTES {
            return Err(PaperAccountError::InvalidRequest(
                "paper reconciliation evidence exceeds the supported byte bound",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperReconciliationProof {
    account_id: String,
    reservation_id: Uuid,
    batch_id: Uuid,
    snapshot_id: String,
    snapshot_sequence: u64,
    digest_algorithm: PaperReconciliationDigestAlgorithm,
    digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    evidence: Option<PaperReconciliationEvidence>,
}

impl PaperReconciliationProof {
    /// Creates one bounded, replayable reconciliation proof.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidRequest`] for unsafe identities,
    /// nil identifiers, zero snapshot sequences, or malformed digests.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_id: impl Into<String>,
        reservation_id: Uuid,
        batch_id: Uuid,
        snapshot_id: impl Into<String>,
        snapshot_sequence: u64,
        digest_algorithm: PaperReconciliationDigestAlgorithm,
        digest: impl Into<String>,
    ) -> Result<Self, PaperAccountError> {
        let proof = Self {
            account_id: bounded_identity(&account_id.into(), "account id")
                .map_err(PaperAccountError::InvalidRequest)?,
            reservation_id,
            batch_id,
            snapshot_id: bounded_identity(&snapshot_id.into(), "snapshot id")
                .map_err(PaperAccountError::InvalidRequest)?,
            snapshot_sequence,
            digest_algorithm,
            digest: bounded_digest(digest_algorithm, &digest.into())?,
            evidence: None,
        };
        proof.validate()?;
        Ok(proof)
    }

    /// Creates a proof whose digest is derived from canonical bounded
    /// reconciliation evidence.
    ///
    /// This is the only proof shape accepted by account release/failure
    /// transitions. [`Self::new`] remains available for decoding legacy
    /// transport contracts, but its opaque digest is not verified evidence.
    ///
    /// # Errors
    ///
    /// Returns an error when the evidence is invalid or cannot be serialized.
    pub fn from_evidence(evidence: PaperReconciliationEvidence) -> Result<Self, PaperAccountError> {
        evidence.validate()?;
        let encoded = serde_json::to_vec(&evidence).map_err(PaperAccountError::Serialize)?;
        let digest_algorithm = PaperReconciliationDigestAlgorithm::Fnv1a64;
        let proof = Self {
            account_id: evidence.account_id.clone(),
            reservation_id: evidence.reservation_id,
            batch_id: evidence.batch_id,
            snapshot_id: evidence.snapshot_id.clone(),
            snapshot_sequence: evidence.snapshot_sequence,
            digest_algorithm,
            digest: format!("{:016x}", fnv1a64(&encoded)),
            evidence: Some(evidence),
        };
        proof.validate()?;
        Ok(proof)
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub const fn reservation_id(&self) -> Uuid {
        self.reservation_id
    }

    #[must_use]
    pub const fn batch_id(&self) -> Uuid {
        self.batch_id
    }

    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    #[must_use]
    pub const fn snapshot_sequence(&self) -> u64 {
        self.snapshot_sequence
    }

    #[must_use]
    pub const fn digest_algorithm(&self) -> PaperReconciliationDigestAlgorithm {
        self.digest_algorithm
    }

    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    #[must_use]
    pub const fn evidence(&self) -> Option<&PaperReconciliationEvidence> {
        self.evidence.as_ref()
    }

    fn validate(&self) -> Result<(), PaperAccountError> {
        bounded_identity(&self.account_id, "account id")
            .map_err(PaperAccountError::InvalidRequest)?;
        bounded_identity(&self.snapshot_id, "snapshot id")
            .map_err(PaperAccountError::InvalidRequest)?;
        if self.reservation_id.is_nil() || self.batch_id.is_nil() {
            return Err(PaperAccountError::InvalidRequest(
                "paper reconciliation proof ids must not be nil",
            ));
        }
        if self.snapshot_sequence == 0 {
            return Err(PaperAccountError::InvalidRequest(
                "paper reconciliation snapshot sequence must be positive",
            ));
        }
        let _ = bounded_digest(self.digest_algorithm, &self.digest)?;
        if let Some(evidence) = &self.evidence {
            evidence.validate()?;
            if evidence.account_id != self.account_id
                || evidence.reservation_id != self.reservation_id
                || evidence.batch_id != self.batch_id
                || evidence.snapshot_id != self.snapshot_id
                || evidence.snapshot_sequence != self.snapshot_sequence
            {
                return Err(PaperAccountError::InvalidRequest(
                    "paper reconciliation proof conflicts with canonical evidence",
                ));
            }
            let encoded = serde_json::to_vec(evidence).map_err(PaperAccountError::Serialize)?;
            let expected = match self.digest_algorithm {
                PaperReconciliationDigestAlgorithm::Fnv1a64 => {
                    format!("{:016x}", fnv1a64(&encoded))
                }
            };
            if self.digest != expected {
                return Err(PaperAccountError::InvalidRequest(
                    "paper reconciliation evidence digest does not match",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperReconciliationOutcome {
    Released,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperReconciliationRecord {
    pub outcome: PaperReconciliationOutcome,
    pub proof: PaperReconciliationProof,
    pub evidence_sequence: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperExecutionLedgerKind {
    #[default]
    LegacyReservationOnly,
    ExactExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperExecutionLiquidity {
    Taker,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperExecutedFill {
    pub leg_index: usize,
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub side: Side,
    pub quantity: Quantity,
    pub average_fill_price: Price,
    pub notional: Money,
    pub fee: Money,
    pub liquidity: PaperExecutionLiquidity,
    #[serde(default)]
    pub reduce_only: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperExecutionSettlementView {
    pub fills: Vec<PaperExecutedFill>,
    pub fees_paid: Money,
    pub realized_pnl_delta: Money,
    pub settled_equity_base_after: Money,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperOpenLotView {
    pub source_reservation_id: Uuid,
    pub exchange: String,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub side: Side,
    pub remaining_quantity: Quantity,
    pub entry_price: Price,
    pub remaining_notional: Money,
    pub remaining_entry_fee: Money,
    pub held_exposure: Money,
    pub opened_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperReservationView {
    pub reservation_id: Uuid,
    pub task_id: String,
    pub idempotency_key: String,
    pub batch_id: Uuid,
    pub cost_model: PaperCostModel,
    pub legs: Vec<PaperReservationLeg>,
    pub reserved_exposure: Money,
    pub held_exposure: Money,
    pub phase: PaperReservationPhase,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub reconciliation: Option<PaperReconciliationRecord>,
    #[serde(default)]
    pub ledger_kind: PaperExecutionLedgerKind,
    #[serde(default)]
    pub settlement: Option<PaperExecutionSettlementView>,
}

impl PaperReservationView {
    pub(crate) fn matches(&self, request: &PaperReservationRequest) -> bool {
        self.reservation_id == request.reservation_id
            && self.task_id == request.task_id
            && self.idempotency_key == request.idempotency_key
            && self.batch_id == request.batch_id
            && self.cost_model == request.cost_model
            && self.legs == request.legs
    }

    const fn is_active(&self) -> bool {
        !matches!(self.phase, PaperReservationPhase::Released)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperAccountSnapshot {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub projection_status: ProjectionStatus,
    pub invalid_event_count: u64,
    pub account_id: String,
    pub initial_available: Money,
    pub available: Money,
    pub pending_reserved: Money,
    pub uncertain_reserved: Money,
    pub committed_exposure: Money,
    #[serde(default)]
    pub ledger_kind: PaperExecutionLedgerKind,
    #[serde(default)]
    pub cumulative_fees: Money,
    #[serde(default)]
    pub realized_pnl: Money,
    #[serde(default)]
    pub settled_equity_base: Money,
    #[serde(default)]
    pub open_lots: Vec<PaperOpenLotView>,
    pub reservations: Vec<PaperReservationView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperAccountReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub projection_status: ProjectionStatus,
    pub invalid_event_count: u64,
    pub accounts: Vec<PaperAccountSnapshot>,
}

impl PaperAccountReadModel {
    /// Reconstructs bounded paper account facts from one immutable journal.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountProjectionError`] for journal failures, pagination
    /// that cannot advance, or hard account/reservation resource exhaustion.
    pub fn from_legacy_snapshot(
        snapshot: &JournalSnapshot,
    ) -> Result<Self, PaperAccountProjectionError> {
        ProjectionBuilder::new(snapshot.journal_id()).project(snapshot)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaperReservationAdmission {
    Reserved(PaperReservationView),
    Existing(PaperReservationView),
}

#[derive(Clone, Debug)]
pub struct PaperAccountAuthority {
    journal_id: Uuid,
    history: JsonlHistory,
    config: PaperAccountConfig,
    authority_lock: Arc<AuthorityLock>,
    operation_lock: Arc<OperationLock>,
    state_cache: AuthorityStateCache,
}

impl PaperAccountAuthority {
    /// Creates a new paper journal generation for a fresh local authority.
    ///
    /// # Errors
    ///
    /// Returns the same configuration failures as [`Self::new`].
    pub fn planned(
        history: JsonlHistory,
        config: PaperAccountConfig,
    ) -> Result<Self, PaperAccountError> {
        let journal_id = loop {
            let candidate = Uuid::new_v4();
            if !candidate.is_nil() {
                break candidate;
            }
        };
        Self::new(journal_id, history, config)
    }

    /// Creates a process-local authority over one journal-backed paper account.
    ///
    /// Every paper-account fact is bound to `journal_id`; reopening a history
    /// under another generation degrades the projection and closes writes.
    /// The supplied [`JsonlHistory`] retains its cross-process writer lease for
    /// the authority lifetime. A competing process targeting the same
    /// normalized path fails closed on its first journal operation.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::InvalidConfig`] for a nil journal ID.
    pub fn new(
        journal_id: Uuid,
        history: JsonlHistory,
        config: PaperAccountConfig,
    ) -> Result<Self, PaperAccountError> {
        if journal_id.is_nil() {
            return Err(PaperAccountError::InvalidConfig(
                "paper journal id must not be nil",
            ));
        }
        // The account authority and the journal writer must serialize on the
        // same key. Keying this on the raw path while the writer keys on the
        // normalized one lets two spellings of one journal hold two different
        // authority locks, so their read-modify-write of available capacity
        // would interleave and could commit the same capacity twice.
        let authority_lock =
            shared_authority_lock(&crate::history::normalized_lock_key(history.path()));
        let operation_lock = shared_operation_lock(
            &crate::history::normalized_lock_key(history.path()),
            config.account_id(),
        );
        let state_cache = AuthorityStateCache::new(journal_id, &history);
        Ok(Self {
            journal_id,
            history,
            config,
            authority_lock,
            operation_lock,
            state_cache,
        })
    }

    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    #[must_use]
    pub fn history_path(&self) -> &Path {
        self.history.path()
    }

    /// Acquires the process-local paper operation lane for this journal.
    ///
    /// Owners can hold the returned lease across decision snapshots, reserve,
    /// execute, and settle so no sibling in-process authority instance can
    /// interleave another account operation on the same normalized journal
    /// path. This lease is intentionally independent from the internal
    /// authority lock, so callers can acquire it before invoking authority
    /// methods without self-deadlocking.
    pub async fn acquire_operation_lease(&self) -> PaperAccountOperationLease {
        PaperAccountOperationLease {
            inner: Arc::new(PaperAccountOperationLeaseInner {
                _guard: Arc::clone(&self.operation_lock).lock_owned().await,
            }),
        }
    }

    /// Cold-replays the current frozen journal head and verifies that it is
    /// equivalent to the process-local authority projection.
    ///
    /// A detected mismatch permanently degrades every in-process authority
    /// sharing this journal generation. Repeated calls are idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::DurableStateDegraded`] when a cached
    /// projection cannot be proven equivalent, including malformed durable
    /// bytes, or after that failure has been latched. Initial projection
    /// loading can still return its corresponding journal or projection error.
    pub async fn verify_durable_state(&self) -> Result<(), PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        self.state_cache
            .verify_durable_state(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)
    }

    #[must_use]
    pub const fn config(&self) -> &PaperAccountConfig {
        &self.config
    }

    /// Durably materializes this account's configured starting capacity.
    ///
    /// Paper-account projections historically appeared only after the first
    /// reservation. Account-wide risk must evaluate balance thresholds before
    /// that reservation, so risk-enabled owners call this idempotent method at
    /// startup to make the configured balance part of the shared journal
    /// truth first.
    ///
    /// # Errors
    ///
    /// Fails closed when the journal is degraded, the configured balance
    /// conflicts with an existing account, the account limit is exhausted, or
    /// the initialization fact cannot be written and replayed.
    pub async fn ensure_initialized(&self) -> Result<PaperAccountSnapshot, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)?;
        let snapshot = projection
            .paper_snapshot(self.config.account_id(), self.config.initial_available)
            .map_err(map_authority_state_error_to_paper)?;
        require_writable(&snapshot)?;
        if projection
            .paper_live
            .accounts
            .iter()
            .any(|account| account.account_id == self.config.account_id)
        {
            return Ok(snapshot);
        }
        if projection.paper_live.accounts.len() >= MAX_PAPER_ACCOUNTS {
            return Err(PaperAccountError::InvalidConfig(
                "paper account limit was reached",
            ));
        }
        self.append_fact(
            PAPER_ACCOUNT_INITIALIZED,
            &InitializedFact {
                schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                account_id: self.config.account_id.clone(),
                initial_available: self.config.initial_available,
            },
        )
        .await?;
        let initialized = self.load_account_snapshot().await?;
        require_writable(&initialized)?;
        Ok(initialized)
    }

    /// Returns one frozen durable account projection.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError`] for snapshot, journal, projection, or
    /// configured-starting-balance conflicts.
    pub async fn snapshot(&self) -> Result<PaperAccountSnapshot, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        self.load_account_snapshot().await
    }

    /// Returns a projection that is safe to use for a control decision.
    ///
    /// [`Self::snapshot`] intentionally remains diagnostic and can expose the
    /// last valid state together with a degraded status. Callers deciding
    /// whether to reserve, compensate, or release capacity must use this
    /// method so degraded input fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`PaperAccountError::DurableStateDegraded`] when replay found a
    /// partial or invalid fact, in addition to snapshot/journal errors.
    pub async fn decision_snapshot(&self) -> Result<PaperAccountSnapshot, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        Ok(snapshot)
    }

    /// Durably reserves account capacity before any execution plan or adapter
    /// side effect is allowed.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state, identity conflicts, an active
    /// reservation for the same task, insufficient capacity, or journal I/O.
    pub async fn reserve(
        &self,
        request: PaperReservationRequest,
    ) -> Result<PaperReservationAdmission, PaperAccountError> {
        request.validate()?;
        let _guard = self.authority_lock.lock().await;
        let authority_now = Utc::now();
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;

        if let Some(existing) = snapshot.reservations.iter().find(|reservation| {
            reservation.task_id == request.task_id
                && reservation.idempotency_key == request.idempotency_key
        }) {
            return if existing.matches(&request) {
                Ok(PaperReservationAdmission::Existing(existing.clone()))
            } else {
                Err(PaperAccountError::IdempotencyConflict)
            };
        }
        if snapshot.reservations.iter().any(|reservation| {
            reservation.reservation_id == request.reservation_id
                || reservation.batch_id == request.batch_id
        }) {
            return Err(PaperAccountError::ReservationIdentityConflict);
        }
        if snapshot.reservations.iter().any(|reservation| {
            reservation.task_id == request.task_id
                && reservation.phase != PaperReservationPhase::Released
        }) {
            return Err(PaperAccountError::ActiveTaskReservation);
        }
        if active_reservation_count(&snapshot.reservations) >= MAX_PAPER_ACCOUNT_RESERVATIONS {
            return Err(PaperAccountError::ReservationLimitExceeded {
                limit: MAX_PAPER_ACCOUNT_RESERVATIONS,
            });
        }
        if let Some(existing) = self.find_historical_reservation(&request).await? {
            return Ok(PaperReservationAdmission::Existing(existing));
        }
        self.validate_bound_risk_admission(&request, authority_now)
            .await?;
        let reserved_exposure = reserved_exposure(&request)?;
        let required =
            additional_reservation_exposure(&snapshot.open_lots, &snapshot.reservations, &request)?;
        if snapshot.available < required {
            return Err(PaperAccountError::InsufficientAvailable {
                required,
                available: snapshot.available,
            });
        }

        let reservation_id = request.reservation_id;
        self.append_fact_at(
            authority_now,
            PAPER_ACCOUNT_RESERVED,
            &ReservedFact {
                schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                account_id: self.config.account_id.clone(),
                initial_available: self.config.initial_available,
                request,
                reserved_exposure,
            },
        )
        .await?;
        let updated = self.load_account_snapshot().await?;
        require_writable(&updated)?;
        let reservation = updated
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == reservation_id)
            .cloned()
            .ok_or(PaperAccountError::DurableStateConflict)?;
        Ok(PaperReservationAdmission::Reserved(reservation))
    }

    /// Marks a reservation outcome as uncertain without releasing capacity.
    ///
    /// # Errors
    ///
    /// Fails closed if the reservation is absent, already released/committed,
    /// or the journal cannot be advanced safely.
    pub async fn mark_uncertain(
        &self,
        reservation_id: Uuid,
    ) -> Result<PaperReservationView, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        let reservation = self
            .find_transition_reservation(&snapshot, reservation_id)
            .await?;
        match reservation.phase {
            PaperReservationPhase::Uncertain => return Ok(reservation.clone()),
            PaperReservationPhase::Pending => {}
            PaperReservationPhase::Committed | PaperReservationPhase::Released => {
                return Err(PaperAccountError::InvalidTransition);
            }
        }
        self.append_transition(PAPER_ACCOUNT_UNCERTAIN, &reservation, None, None, None)
            .await?;
        self.reloaded_reservation(reservation_id).await
    }

    /// Moves an uncertain/pending reservation into confirmed committed
    /// exposure, releasing only the unused conservative buffer.
    ///
    /// # Errors
    ///
    /// Fails closed when confirmed exposure is non-positive, exceeds the
    /// original reservation, or conflicts with an existing terminal state.
    pub async fn commit(
        &self,
        reservation_id: Uuid,
        confirmed_exposure: Money,
    ) -> Result<PaperReservationView, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        let reservation = self
            .find_transition_reservation(&snapshot, reservation_id)
            .await?;
        if reservation.phase == PaperReservationPhase::Committed {
            return if reservation.held_exposure == confirmed_exposure {
                Ok(reservation.clone())
            } else {
                Err(PaperAccountError::InvalidTransition)
            };
        }
        if !matches!(
            reservation.phase,
            PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
        ) || confirmed_exposure <= Money::default()
            || confirmed_exposure > reservation.reserved_exposure
        {
            return Err(PaperAccountError::InvalidTransition);
        }
        self.append_transition(
            PAPER_ACCOUNT_COMMITTED,
            &reservation,
            Some(confirmed_exposure),
            None,
            None,
        )
        .await?;
        self.reloaded_reservation(reservation_id).await
    }

    /// Settles one fully filled synchronous paper execution into exact durable
    /// inventory, fees, and realized `PnL` without writing any mark-to-market.
    ///
    /// # Errors
    ///
    /// Fails closed when receipts are not exactly and fully filled, when they
    /// do not match the reserved legs, or when bounded FIFO settlement would
    /// overflow or conflict with durable state.
    pub async fn settle_execution(
        &self,
        reservation_id: Uuid,
        receipts: &[TradingReceipt],
    ) -> Result<PaperReservationView, PaperAccountError> {
        if receipts.is_empty() || receipts.len() > MAX_RESERVATION_LEGS {
            return Err(PaperAccountError::InvalidTransition);
        }
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        let reservation = self
            .find_transition_reservation(&snapshot, reservation_id)
            .await?;
        if !matches!(
            reservation.phase,
            PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
        ) {
            return Err(PaperAccountError::InvalidTransition);
        }
        let fact = execution_settlement_fact(
            self.journal_id,
            self.config.account_id(),
            &reservation,
            receipts,
        )?;
        validate_reduce_only_settlement(&snapshot.open_lots, &fact.fills)?;
        self.append_fact(PAPER_ACCOUNT_EXECUTION_SETTLED, &fact)
            .await?;
        self.reloaded_reservation(reservation_id).await
    }

    /// Releases pending or uncertain exposure.
    ///
    /// Committed exposure cannot be released by a caller-supplied reason. A
    /// later reconciliation tracer must introduce a verified, durable proof
    /// before that transition can become available.
    ///
    /// # Errors
    ///
    /// Fails closed for an unsafe reason, unknown reservation, degraded
    /// journal, or invalid durable transition.
    pub async fn release(
        &self,
        reservation_id: Uuid,
        reason: impl Into<String>,
    ) -> Result<PaperReservationView, PaperAccountError> {
        let reason = reason.into();
        let reason = bounded_reason(&reason)?;
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        let reservation = self
            .find_transition_reservation(&snapshot, reservation_id)
            .await?;
        if reservation.phase == PaperReservationPhase::Released {
            return Ok(reservation.clone());
        }
        if reservation.phase == PaperReservationPhase::Committed {
            return Err(PaperAccountError::InvalidTransition);
        }
        self.append_transition(
            PAPER_ACCOUNT_RELEASED,
            &reservation,
            None,
            Some(reason),
            None,
        )
        .await?;
        self.reloaded_reservation(reservation_id).await
    }

    /// Releases opaque committed exposure only when caller presents durable,
    /// replayable reconciliation proof bound to this account and batch.
    /// Exact execution inventory is released exclusively by local fill
    /// settlement so its exposure cannot diverge from its FIFO open lots.
    ///
    /// # Errors
    ///
    /// Fails closed on missing/mismatched proof, degraded durable state, or
    /// conflicting reconciliation evidence.
    pub async fn reconcile_release(
        &self,
        proof: PaperReconciliationProof,
    ) -> Result<PaperReservationView, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        let reservation = self
            .find_transition_reservation(&snapshot, proof.reservation_id())
            .await?;
        ensure_reconciliation_proof_matches(
            &proof,
            self.config.account_id(),
            reservation.reservation_id,
            reservation.batch_id,
        )?;
        proof.validate()?;
        if reservation.phase == PaperReservationPhase::Released {
            return if matches_reconciliation(
                reservation.reconciliation.as_ref(),
                PaperReconciliationOutcome::Released,
                &proof,
            ) {
                Ok(reservation.clone())
            } else {
                Err(PaperAccountError::InvalidTransition)
            };
        }
        if reservation.phase != PaperReservationPhase::Committed {
            return Err(PaperAccountError::InvalidTransition);
        }
        if reservation.ledger_kind != PaperExecutionLedgerKind::LegacyReservationOnly {
            return Err(PaperAccountError::InvalidTransition);
        }
        validate_reconciliation_evidence(
            &proof,
            &snapshot,
            &reservation,
            PaperReconciliationVerdict::Match,
        )?;
        validate_reconciliation_progress(
            reservation.reconciliation.as_ref(),
            PaperReconciliationOutcome::Released,
            &proof,
        )?;
        self.append_transition(
            PAPER_ACCOUNT_RELEASED,
            &reservation,
            None,
            None,
            Some(proof),
        )
        .await?;
        self.reloaded_reservation(reservation.reservation_id).await
    }

    /// Durably records a failed committed reconciliation without releasing
    /// committed exposure.
    ///
    /// # Errors
    ///
    /// Fails closed on missing/mismatched proof, degraded durable state, or
    /// conflicting reconciliation evidence.
    pub async fn record_reconciliation_failure(
        &self,
        proof: PaperReconciliationProof,
    ) -> Result<PaperReservationView, PaperAccountError> {
        let _guard = self.authority_lock.lock().await;
        let snapshot = self.load_account_snapshot().await?;
        require_writable(&snapshot)?;
        let reservation = self
            .find_transition_reservation(&snapshot, proof.reservation_id())
            .await?;
        ensure_reconciliation_proof_matches(
            &proof,
            self.config.account_id(),
            reservation.reservation_id,
            reservation.batch_id,
        )?;
        proof.validate()?;
        if reservation.phase != PaperReservationPhase::Committed {
            return Err(PaperAccountError::InvalidTransition);
        }
        if matches_reconciliation(
            reservation.reconciliation.as_ref(),
            PaperReconciliationOutcome::Failed,
            &proof,
        ) {
            return Ok(reservation.clone());
        }
        validate_reconciliation_evidence(
            &proof,
            &snapshot,
            &reservation,
            PaperReconciliationVerdict::Mismatch,
        )?;
        validate_reconciliation_progress(
            reservation.reconciliation.as_ref(),
            PaperReconciliationOutcome::Failed,
            &proof,
        )?;
        self.append_transition(
            PAPER_ACCOUNT_RECONCILE_FAILED,
            &reservation,
            None,
            None,
            Some(proof),
        )
        .await?;
        self.reloaded_reservation(reservation.reservation_id).await
    }

    async fn append_transition(
        &self,
        decision: &'static str,
        reservation: &PaperReservationView,
        confirmed_exposure: Option<Money>,
        reason: Option<String>,
        proof: Option<PaperReconciliationProof>,
    ) -> Result<(), PaperAccountError> {
        self.append_fact(
            decision,
            &TransitionFact {
                schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                account_id: self.config.account_id.clone(),
                reservation_id: reservation.reservation_id,
                batch_id: reservation.batch_id,
                confirmed_exposure,
                reason,
                proof,
            },
        )
        .await
    }

    async fn append_fact<T: Serialize>(
        &self,
        decision: &'static str,
        fact: &T,
    ) -> Result<(), PaperAccountError> {
        self.append_fact_at(Utc::now(), decision, fact).await
    }

    async fn append_fact_at<T: Serialize>(
        &self,
        timestamp: chrono::DateTime<Utc>,
        decision: &'static str,
        fact: &T,
    ) -> Result<(), PaperAccountError> {
        let details = serde_json::to_value(fact).map_err(PaperAccountError::Serialize)?;
        self.history
            .append(&DecisionRecord {
                timestamp,
                strategy: PAPER_ACCOUNT_STRATEGY.to_owned(),
                symbol: self.config.account_id.clone(),
                decision: decision.to_owned(),
                details,
            })
            .await
            .map_err(PaperAccountError::JournalWrite)
    }

    async fn reloaded_reservation(
        &self,
        reservation_id: Uuid,
    ) -> Result<PaperReservationView, PaperAccountError> {
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)?;
        let updated = projection
            .paper_snapshot(self.config.account_id(), self.config.initial_available)
            .map_err(map_authority_state_error_to_paper)?;
        require_writable(&updated)?;
        if let Some(reservation) = updated
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == reservation_id)
        {
            return Ok(reservation.clone());
        }
        projection
            .historical_reservation_by_id(self.config.account_id(), reservation_id)?
            .ok_or(PaperAccountError::UnknownReservation)
    }

    async fn load_account_snapshot(&self) -> Result<PaperAccountSnapshot, PaperAccountError> {
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)?;
        projection
            .paper_snapshot(self.config.account_id(), self.config.initial_available)
            .map_err(map_authority_state_error_to_paper)
    }

    async fn find_historical_reservation(
        &self,
        request: &PaperReservationRequest,
    ) -> Result<Option<PaperReservationView>, PaperAccountError> {
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)?;
        projection.historical_reservation(self.config.account_id(), request)
    }

    async fn validate_bound_risk_admission(
        &self,
        request: &PaperReservationRequest,
        authority_now: chrono::DateTime<Utc>,
    ) -> Result<(), PaperAccountError> {
        let Some((scope_id, ticket_id)) = request.risk_admission_binding() else {
            return Ok(());
        };
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)?;
        let admission = projection
            .bound_open_admission(scope_id, ticket_id)
            .map_err(map_authority_state_error_to_paper)?
            .ok_or(PaperAccountError::RiskAdmissionUnavailable)?;
        if admission.lease_expires_at <= authority_now {
            return Err(PaperAccountError::RiskAdmissionExpired);
        }
        if !crate::account_risk::bound_admission_matches_reservation(&admission, request)
            .map_err(|_| PaperAccountError::ArithmeticOverflow)?
        {
            return Err(PaperAccountError::RiskAdmissionMismatch);
        }
        Ok(())
    }

    async fn find_transition_reservation(
        &self,
        snapshot: &PaperAccountSnapshot,
        reservation_id: Uuid,
    ) -> Result<PaperReservationView, PaperAccountError> {
        if let Some(reservation) = snapshot
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == reservation_id)
        {
            return Ok(reservation.clone());
        }
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_paper)?;
        projection
            .historical_reservation_by_id(self.config.account_id(), reservation_id)?
            .ok_or(PaperAccountError::UnknownReservation)
    }
}

fn map_authority_state_error_to_paper(error: AuthorityStateError) -> PaperAccountError {
    match error {
        AuthorityStateError::History => PaperAccountError::DurableStateDegraded,
        AuthorityStateError::Journal(error) => PaperAccountError::JournalRead(error),
        AuthorityStateError::Paper(error) => PaperAccountError::Projection(error),
        AuthorityStateError::Risk(_) | AuthorityStateError::Degraded => {
            PaperAccountError::DurableStateDegraded
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReservedFact {
    schema_version: u16,
    journal_id: Uuid,
    account_id: String,
    initial_available: Money,
    request: PaperReservationRequest,
    reserved_exposure: Money,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializedFact {
    schema_version: u16,
    journal_id: Uuid,
    account_id: String,
    initial_available: Money,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransitionFact {
    schema_version: u16,
    journal_id: Uuid,
    account_id: String,
    reservation_id: Uuid,
    batch_id: Uuid,
    confirmed_exposure: Option<Money>,
    reason: Option<String>,
    proof: Option<PaperReconciliationProof>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSettledFact {
    schema_version: u16,
    journal_id: Uuid,
    account_id: String,
    reservation_id: Uuid,
    batch_id: Uuid,
    fills: Vec<PaperExecutedFill>,
}

pub(crate) struct ProjectionBuilder {
    journal_id: Uuid,
    projection_status: ProjectionStatus,
    invalid_event_count: u64,
    accounts: Vec<AccountAccumulator>,
}

pub(crate) struct TerminalReservationUpdate {
    pub(crate) account_id: String,
    pub(crate) reservation: PaperReservationView,
}

impl ProjectionBuilder {
    pub(crate) const fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            projection_status: ProjectionStatus::Complete,
            invalid_event_count: 0,
            accounts: Vec::new(),
        }
    }

    pub(crate) fn from_model(model: PaperAccountReadModel) -> Self {
        Self {
            journal_id: model.journal_id,
            projection_status: model.projection_status,
            invalid_event_count: model.invalid_event_count,
            accounts: model
                .accounts
                .into_iter()
                .map(AccountAccumulator::from_snapshot)
                .collect(),
        }
    }

    fn project(
        mut self,
        snapshot: &JournalSnapshot,
    ) -> Result<PaperAccountReadModel, PaperAccountProjectionError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            for event in page.events() {
                self.observe_event(event);
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    self.mark_partial_tail();
                    break;
                }
                JournalPageBoundary::PageLimit => {
                    let next = page.next_cursor().cloned();
                    if next == cursor {
                        return Err(PaperAccountProjectionError::NonAdvancingPage);
                    }
                    cursor = next;
                }
            }
        }

        self.finish()
    }

    pub(crate) fn observe_event(&mut self, event: &OperationEventEnvelope) {
        let _ = self.observe_event_with_terminal_updates(event);
    }

    pub(crate) fn observe_event_with_terminal_updates(
        &mut self,
        event: &OperationEventEnvelope,
    ) -> Vec<TerminalReservationUpdate> {
        self.apply_event(event.sequence(), event.payload())
    }

    pub(crate) fn mark_partial_tail(&mut self) {
        self.projection_status = ProjectionStatus::Degraded;
    }

    pub(crate) fn finish(self) -> Result<PaperAccountReadModel, PaperAccountProjectionError> {
        let mut accounts = self
            .accounts
            .into_iter()
            .map(|account| {
                account.finish(
                    self.journal_id,
                    self.projection_status,
                    self.invalid_event_count,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        accounts.sort_by(|left, right| left.account_id.cmp(&right.account_id));
        Ok(PaperAccountReadModel {
            schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
            journal_id: self.journal_id,
            projection_status: self.projection_status,
            invalid_event_count: self.invalid_event_count,
            accounts,
        })
    }

    fn apply_event(&mut self, sequence: u64, payload: &Value) -> Vec<TerminalReservationUpdate> {
        let Some(decision) = payload.get("decision").and_then(Value::as_str) else {
            return Vec::new();
        };
        if !matches!(
            decision,
            PAPER_ACCOUNT_INITIALIZED
                | PAPER_ACCOUNT_RESERVED
                | PAPER_ACCOUNT_UNCERTAIN
                | PAPER_ACCOUNT_COMMITTED
                | PAPER_ACCOUNT_RELEASED
                | PAPER_ACCOUNT_RECONCILE_FAILED
                | PAPER_ACCOUNT_EXECUTION_SETTLED
        ) {
            return Vec::new();
        }
        if let Ok(updates) = self.try_apply_event(sequence, decision, payload) {
            updates
        } else {
            self.invalid_event_count = self.invalid_event_count.saturating_add(1);
            self.projection_status = ProjectionStatus::Degraded;
            Vec::new()
        }
    }

    fn try_apply_event(
        &mut self,
        sequence: u64,
        decision: &str,
        payload: &Value,
    ) -> Result<Vec<TerminalReservationUpdate>, ()> {
        let payload = exact_object(payload, &["decision", "details", "strategy", "symbol"])?;
        if text(payload.get("decision"))? != decision
            || text(payload.get("strategy"))? != PAPER_ACCOUNT_STRATEGY
        {
            return Err(());
        }
        let symbol = text(payload.get("symbol"))?;
        let details = payload.get("details").ok_or(())?.clone();
        match decision {
            PAPER_ACCOUNT_INITIALIZED => {
                require_money_strings_for_initialized(&details)?;
                let fact: InitializedFact = serde_json::from_value(details).map_err(|_| ())?;
                validate_initialized_fact(&fact, symbol, self.journal_id)?;
                if self.accounts.len() >= MAX_PAPER_ACCOUNTS
                    || self
                        .accounts
                        .iter()
                        .any(|account| account.account_id == fact.account_id)
                {
                    return Err(());
                }
                self.accounts.push(AccountAccumulator::new(
                    fact.account_id,
                    fact.initial_available,
                ));
                Ok(Vec::new())
            }
            PAPER_ACCOUNT_RESERVED => {
                require_money_strings_for_reserved(&details)?;
                let fact: ReservedFact = serde_json::from_value(details).map_err(|_| ())?;
                validate_reserved_fact(&fact, symbol, self.journal_id)?;
                let account_index = if let Some(index) = self
                    .accounts
                    .iter()
                    .position(|account| account.account_id == fact.account_id)
                {
                    index
                } else {
                    if self.accounts.len() >= MAX_PAPER_ACCOUNTS {
                        return Err(());
                    }
                    self.accounts.push(AccountAccumulator::new(
                        fact.account_id.clone(),
                        fact.initial_available,
                    ));
                    self.accounts.len().saturating_sub(1)
                };
                self.accounts[account_index].reserve(sequence, fact)?;
                Ok(Vec::new())
            }
            PAPER_ACCOUNT_UNCERTAIN | PAPER_ACCOUNT_COMMITTED | PAPER_ACCOUNT_RELEASED => {
                require_money_strings_for_transition(&details)?;
                let fact: TransitionFact = serde_json::from_value(details).map_err(|_| ())?;
                validate_transition_fact(&fact, symbol, decision, self.journal_id)?;
                let account = self
                    .accounts
                    .iter_mut()
                    .find(|account| account.account_id == fact.account_id)
                    .ok_or(())?;
                let account_id = fact.account_id.clone();
                account
                    .transition(sequence, decision, &fact)
                    .map(|reservations| terminal_updates(&account_id, reservations))
            }
            PAPER_ACCOUNT_EXECUTION_SETTLED => {
                require_money_strings_for_execution_settled(&details)?;
                let fact: ExecutionSettledFact = serde_json::from_value(details).map_err(|_| ())?;
                validate_execution_settled_fact(&fact, symbol, self.journal_id)?;
                let account = self
                    .accounts
                    .iter_mut()
                    .find(|account| account.account_id == fact.account_id)
                    .ok_or(())?;
                let account_id = fact.account_id.clone();
                account
                    .settle_execution(sequence, fact)
                    .map(|reservations| terminal_updates(&account_id, reservations))
            }
            PAPER_ACCOUNT_RECONCILE_FAILED => {
                require_money_strings_for_transition(&details)?;
                let fact: TransitionFact = serde_json::from_value(details).map_err(|_| ())?;
                validate_transition_fact(&fact, symbol, decision, self.journal_id)?;
                let account = self
                    .accounts
                    .iter_mut()
                    .find(|account| account.account_id == fact.account_id)
                    .ok_or(())?;
                let account_id = fact.account_id.clone();
                account
                    .transition(sequence, decision, &fact)
                    .map(|reservations| terminal_updates(&account_id, reservations))
            }
            _ => Err(()),
        }
    }
}

fn terminal_updates(
    account_id: &str,
    reservations: Vec<PaperReservationView>,
) -> Vec<TerminalReservationUpdate> {
    reservations
        .into_iter()
        .map(|reservation| TerminalReservationUpdate {
            account_id: account_id.to_owned(),
            reservation,
        })
        .collect()
}

#[derive(Clone)]
struct AccountAccumulator {
    account_id: String,
    initial_available: Money,
    ledger_kind: PaperExecutionLedgerKind,
    cumulative_fees: Money,
    realized_pnl: Money,
    settled_equity_base: Money,
    open_lots: Vec<PaperOpenLotView>,
    reservations: Vec<PaperReservationView>,
}

#[derive(Default)]
struct AppliedPaperFill {
    gross_pnl: Money,
    net_pnl: Money,
    touched_reservations: Vec<Uuid>,
}

struct ClosedPaperLot {
    source_reservation_id: Uuid,
    gross_pnl: Money,
    net_pnl: Money,
}

struct RemainingPaperFill {
    quantity: Quantity,
    notional: Money,
    fee: Money,
}

impl RemainingPaperFill {
    const fn new(fill: &PaperExecutedFill) -> Self {
        Self {
            quantity: fill.quantity,
            notional: fill.notional,
            fee: fill.fee,
        }
    }

    fn allocate_notional(&self, matched: Quantity) -> Result<Money, ()> {
        if matched == self.quantity {
            Ok(self.notional)
        } else {
            proportional_money(self.notional, matched, self.quantity)
        }
    }

    fn allocate_fee(&self, matched: Quantity) -> Result<Money, ()> {
        if matched == self.quantity {
            Ok(self.fee)
        } else {
            proportional_money(self.fee, matched, self.quantity)
        }
    }

    fn consume(
        &mut self,
        matched: Quantity,
        matched_notional: Money,
        matched_fee: Money,
    ) -> Result<(), ()> {
        self.quantity = checked_sub_quantity(self.quantity, matched).ok_or(())?;
        self.notional =
            checked_sub_money_allow_negative(self.notional, matched_notional).ok_or(())?;
        self.fee = checked_sub_money_allow_negative(self.fee, matched_fee).ok_or(())?;
        Ok(())
    }

    fn into_lot(
        self,
        reservation_id: Uuid,
        sequence: u64,
        fill: &PaperExecutedFill,
    ) -> PaperOpenLotView {
        PaperOpenLotView {
            source_reservation_id: reservation_id,
            exchange: fill.exchange.clone(),
            symbol: fill.symbol.clone(),
            market_type: fill.market_type,
            side: fill.side,
            remaining_quantity: self.quantity,
            entry_price: fill.average_fill_price,
            remaining_notional: self.notional,
            remaining_entry_fee: self.fee,
            held_exposure: self.notional,
            opened_sequence: sequence,
        }
    }
}

fn fill_matches_leg((fill, leg): (&PaperExecutedFill, &PaperReservationLeg)) -> bool {
    fill.leg_index == leg.index
        && fill.exchange == leg.exchange
        && fill.symbol == leg.symbol
        && fill.market_type == leg.market_type
        && fill.side == leg.side
        && fill.reduce_only == leg.reduce_only
        && leg
            .expected_quantity
            .is_none_or(|quantity| fill.quantity == quantity)
        && fill.notional <= leg.reserved_notional
}

impl AccountAccumulator {
    fn new(account_id: String, initial_available: Money) -> Self {
        Self {
            account_id,
            initial_available,
            ledger_kind: PaperExecutionLedgerKind::LegacyReservationOnly,
            cumulative_fees: Money::default(),
            realized_pnl: Money::default(),
            settled_equity_base: initial_available,
            open_lots: Vec::new(),
            reservations: Vec::new(),
        }
    }

    pub(crate) fn from_snapshot(snapshot: PaperAccountSnapshot) -> Self {
        Self {
            account_id: snapshot.account_id,
            initial_available: snapshot.initial_available,
            ledger_kind: snapshot.ledger_kind,
            cumulative_fees: snapshot.cumulative_fees,
            realized_pnl: snapshot.realized_pnl,
            settled_equity_base: snapshot.settled_equity_base,
            open_lots: snapshot.open_lots,
            reservations: snapshot.reservations,
        }
    }

    fn reserve(&mut self, sequence: u64, fact: ReservedFact) -> Result<(), ()> {
        if self.initial_available != fact.initial_available
            || active_reservation_count(&self.reservations) >= MAX_PAPER_ACCOUNT_RESERVATIONS
            || self.reservations.iter().any(|reservation| {
                reservation.reservation_id == fact.request.reservation_id
                    || reservation.batch_id == fact.request.batch_id
                    || (reservation.task_id == fact.request.task_id
                        && reservation.idempotency_key == fact.request.idempotency_key)
                    || (reservation.task_id == fact.request.task_id
                        && reservation.phase != PaperReservationPhase::Released)
            })
        {
            return Err(());
        }
        let reservation_hold =
            additional_reservation_exposure(&self.open_lots, &self.reservations, &fact.request)
                .map_err(|_| ())?;
        let held = self.held_total().ok_or(())?;
        let available = available_after_holds(self.settled_equity_base, held).ok_or(())?;
        if available < reservation_hold {
            return Err(());
        }
        self.reservations.push(PaperReservationView {
            reservation_id: fact.request.reservation_id,
            task_id: fact.request.task_id,
            idempotency_key: fact.request.idempotency_key,
            batch_id: fact.request.batch_id,
            cost_model: fact.request.cost_model,
            legs: fact.request.legs,
            reserved_exposure: fact.reserved_exposure,
            held_exposure: reservation_hold,
            phase: PaperReservationPhase::Pending,
            first_sequence: sequence,
            last_sequence: sequence,
            reconciliation: None,
            ledger_kind: PaperExecutionLedgerKind::LegacyReservationOnly,
            settlement: None,
        });
        Ok(())
    }

    fn transition(
        &mut self,
        sequence: u64,
        decision: &str,
        fact: &TransitionFact,
    ) -> Result<Vec<PaperReservationView>, ()> {
        let reservation_index = self
            .reservations
            .iter()
            .position(|reservation| reservation.reservation_id == fact.reservation_id)
            .ok_or(())?;
        let held = self.held_total().ok_or(())?;
        let available = available_after_holds(self.settled_equity_base, held).ok_or(())?;
        let expected_post_release_available = checked_add_money(
            available,
            self.reservations[reservation_index].held_exposure,
        )
        .ok_or(())?;
        let reservation = self.reservations.get_mut(reservation_index).ok_or(())?;
        if reservation.batch_id != fact.batch_id {
            return Err(());
        }
        match decision {
            PAPER_ACCOUNT_UNCERTAIN => {
                if reservation.phase != PaperReservationPhase::Pending
                    || fact.confirmed_exposure.is_some()
                    || fact.reason.is_some()
                    || fact.proof.is_some()
                {
                    return Err(());
                }
                reservation.phase = PaperReservationPhase::Uncertain;
            }
            PAPER_ACCOUNT_COMMITTED => {
                if !matches!(
                    reservation.phase,
                    PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
                ) || fact.reason.is_some()
                    || fact.proof.is_some()
                {
                    return Err(());
                }
                let confirmed = fact.confirmed_exposure.ok_or(())?;
                if confirmed <= Money::default() || confirmed > reservation.reserved_exposure {
                    return Err(());
                }
                reservation.phase = PaperReservationPhase::Committed;
                reservation.held_exposure = confirmed;
            }
            PAPER_ACCOUNT_RELEASED => {
                apply_projected_release(
                    reservation,
                    sequence,
                    fact,
                    &self.account_id,
                    expected_post_release_available,
                )?;
            }
            PAPER_ACCOUNT_RECONCILE_FAILED => {
                apply_projected_reconciliation_failure(
                    reservation,
                    sequence,
                    fact,
                    &self.account_id,
                    expected_post_release_available,
                )?;
            }
            _ => return Err(()),
        }
        reservation.last_sequence = sequence;
        Ok(self.prune_terminal_reservations())
    }

    fn settle_execution(
        &mut self,
        sequence: u64,
        fact: ExecutionSettledFact,
    ) -> Result<Vec<PaperReservationView>, ()> {
        let mut candidate = self.clone();
        let terminal = candidate.apply_execution_settlement(sequence, fact)?;
        *self = candidate;
        Ok(terminal)
    }

    fn apply_execution_settlement(
        &mut self,
        sequence: u64,
        fact: ExecutionSettledFact,
    ) -> Result<Vec<PaperReservationView>, ()> {
        let (reservation_index, maximum_held) = self.validate_settlement_reservation(&fact)?;

        self.ledger_kind = PaperExecutionLedgerKind::ExactExecution;
        let mut gross_realized_delta = Money::default();
        let mut realized_delta = Money::default();
        let mut fees_paid = Money::default();
        let mut touched_reservations = vec![fact.reservation_id];
        for fill in &fact.fills {
            fees_paid = checked_add_money(fees_paid, fill.fee).ok_or(())?;
            self.cumulative_fees = checked_add_money(self.cumulative_fees, fill.fee).ok_or(())?;
            let applied = self.apply_fill_to_lots(fact.reservation_id, sequence, fill)?;
            gross_realized_delta =
                checked_add_money(gross_realized_delta, applied.gross_pnl).ok_or(())?;
            realized_delta = checked_add_money(realized_delta, applied.net_pnl).ok_or(())?;
            touched_reservations.extend(applied.touched_reservations);
        }

        self.realized_pnl = checked_add_money(self.realized_pnl, realized_delta).ok_or(())?;
        self.settled_equity_base = checked_add_money(
            checked_sub_money_allow_negative(self.settled_equity_base, fees_paid).ok_or(())?,
            gross_realized_delta,
        )
        .ok_or(())?;

        let settlement = PaperExecutionSettlementView {
            fills: fact.fills,
            fees_paid,
            realized_pnl_delta: realized_delta,
            settled_equity_base_after: self.settled_equity_base,
        };
        let current_held = self.held_for_reservation(fact.reservation_id)?;
        if current_held > maximum_held {
            return Err(());
        }
        let reservation = self.reservations.get_mut(reservation_index).ok_or(())?;
        reservation.ledger_kind = PaperExecutionLedgerKind::ExactExecution;
        reservation.settlement = Some(settlement);

        self.sync_exact_reservation_holds(sequence, &touched_reservations)?;
        Ok(self.prune_terminal_reservations())
    }

    fn validate_settlement_reservation(
        &self,
        fact: &ExecutionSettledFact,
    ) -> Result<(usize, Money), ()> {
        let index = self
            .reservations
            .iter()
            .position(|reservation| reservation.reservation_id == fact.reservation_id)
            .ok_or(())?;
        let reservation = self.reservations.get(index).ok_or(())?;
        let valid_phase = matches!(
            reservation.phase,
            PaperReservationPhase::Pending | PaperReservationPhase::Uncertain
        );
        let fills_match = fact.fills.len() == reservation.legs.len()
            && fact
                .fills
                .iter()
                .zip(reservation.legs.iter())
                .all(fill_matches_leg);
        if reservation.batch_id != fact.batch_id
            || !valid_phase
            || reservation.settlement.is_some()
            || !fills_match
        {
            return Err(());
        }
        Ok((index, reservation.reserved_exposure))
    }

    fn apply_fill_to_lots(
        &mut self,
        reservation_id: Uuid,
        sequence: u64,
        fill: &PaperExecutedFill,
    ) -> Result<AppliedPaperFill, ()> {
        let mut remaining = RemainingPaperFill::new(fill);
        let mut applied = AppliedPaperFill::default();
        while remaining.quantity > Quantity::default() {
            let Some(lot_index) = self.open_lots.iter().position(|lot| {
                lot.exchange == fill.exchange
                    && lot.symbol == fill.symbol
                    && lot.market_type == fill.market_type
                    && lot.side != fill.side
            }) else {
                break;
            };
            let closed = self.consume_matching_lot(lot_index, &mut remaining)?;
            applied.gross_pnl = checked_add_money(applied.gross_pnl, closed.gross_pnl).ok_or(())?;
            applied.net_pnl = checked_add_money(applied.net_pnl, closed.net_pnl).ok_or(())?;
            applied
                .touched_reservations
                .push(closed.source_reservation_id);
            if self.open_lots[lot_index].remaining_quantity == Quantity::default() {
                self.open_lots.remove(lot_index);
            }
        }
        if remaining.quantity > Quantity::default() && fill.reduce_only {
            return Err(());
        }
        if remaining.quantity > Quantity::default() {
            self.open_lots
                .push(remaining.into_lot(reservation_id, sequence, fill));
        }
        Ok(applied)
    }

    fn consume_matching_lot(
        &mut self,
        lot_index: usize,
        remaining: &mut RemainingPaperFill,
    ) -> Result<ClosedPaperLot, ()> {
        let lot = self.open_lots.get_mut(lot_index).ok_or(())?;
        let matched = min_quantity(remaining.quantity, lot.remaining_quantity)?;
        let exit_notional = remaining.allocate_notional(matched)?;
        let close_fee = remaining.allocate_fee(matched)?;
        let entry_notional =
            consume_lot_money(&mut lot.remaining_notional, matched, lot.remaining_quantity)?;
        let entry_fee = consume_lot_money(
            &mut lot.remaining_entry_fee,
            matched,
            lot.remaining_quantity,
        )?;
        let closed = ClosedPaperLot {
            source_reservation_id: lot.source_reservation_id,
            gross_pnl: gross_close_delta(lot.side, entry_notional, exit_notional)?,
            net_pnl: realized_close_delta(
                lot.side,
                entry_notional,
                exit_notional,
                entry_fee,
                close_fee,
            )?,
        };
        lot.remaining_quantity = checked_sub_quantity(lot.remaining_quantity, matched).ok_or(())?;
        lot.held_exposure = lot.remaining_notional;
        remaining.consume(matched, exit_notional, close_fee)?;
        Ok(closed)
    }

    fn held_total(&self) -> Option<Money> {
        self.reservations
            .iter()
            .try_fold(Money::default(), |total, reservation| {
                checked_add_money(total, reservation.held_exposure)
            })
    }

    fn held_for_reservation(&self, reservation_id: Uuid) -> Result<Money, ()> {
        self.open_lots
            .iter()
            .filter(|lot| lot.source_reservation_id == reservation_id)
            .try_fold(Money::default(), |total, lot| {
                checked_add_money(total, lot.held_exposure).ok_or(())
            })
    }

    fn sync_exact_reservation_holds(
        &mut self,
        sequence: u64,
        touched_reservations: &[Uuid],
    ) -> Result<(), ()> {
        for reservation in &mut self.reservations {
            if reservation.ledger_kind != PaperExecutionLedgerKind::ExactExecution {
                continue;
            }
            let held = self
                .open_lots
                .iter()
                .filter(|lot| lot.source_reservation_id == reservation.reservation_id)
                .try_fold(Money::default(), |total, lot| {
                    checked_add_money(total, lot.held_exposure).ok_or(())
                })?;
            reservation.held_exposure = held;
            reservation.phase = if held > Money::default() {
                PaperReservationPhase::Committed
            } else {
                PaperReservationPhase::Released
            };
            if touched_reservations.contains(&reservation.reservation_id) {
                reservation.last_sequence = sequence;
            }
        }
        Ok(())
    }

    fn prune_terminal_reservations(&mut self) -> Vec<PaperReservationView> {
        let mut terminal = Vec::new();
        self.reservations.retain(|reservation| {
            if reservation.phase == PaperReservationPhase::Released {
                terminal.push(reservation.clone());
                false
            } else {
                true
            }
        });
        terminal
    }

    fn finish(
        self,
        journal_id: Uuid,
        projection_status: ProjectionStatus,
        invalid_event_count: u64,
    ) -> Result<PaperAccountSnapshot, PaperAccountProjectionError> {
        let mut pending_reserved = Money::default();
        let mut uncertain_reserved = Money::default();
        let mut committed_exposure = Money::default();
        for reservation in &self.reservations {
            match reservation.phase {
                PaperReservationPhase::Pending => {
                    pending_reserved =
                        checked_add_money(pending_reserved, reservation.held_exposure)
                            .ok_or(PaperAccountProjectionError::ArithmeticOverflow)?;
                }
                PaperReservationPhase::Uncertain => {
                    uncertain_reserved =
                        checked_add_money(uncertain_reserved, reservation.held_exposure)
                            .ok_or(PaperAccountProjectionError::ArithmeticOverflow)?;
                }
                PaperReservationPhase::Committed => {
                    committed_exposure =
                        checked_add_money(committed_exposure, reservation.held_exposure)
                            .ok_or(PaperAccountProjectionError::ArithmeticOverflow)?;
                }
                PaperReservationPhase::Released => {}
            }
        }
        let held = checked_add_money(pending_reserved, uncertain_reserved)
            .and_then(|value| checked_add_money(value, committed_exposure))
            .ok_or(PaperAccountProjectionError::ArithmeticOverflow)?;
        let available = available_after_holds(self.settled_equity_base, held)
            .ok_or(PaperAccountProjectionError::ArithmeticOverflow)?;
        let mut open_lots = self.open_lots;
        open_lots.sort_by(|left, right| {
            left.opened_sequence
                .cmp(&right.opened_sequence)
                .then_with(|| left.source_reservation_id.cmp(&right.source_reservation_id))
                .then_with(|| left.exchange.cmp(&right.exchange))
                .then_with(|| left.symbol.cmp(&right.symbol))
        });
        Ok(PaperAccountSnapshot {
            schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
            journal_id,
            projection_status,
            invalid_event_count,
            account_id: self.account_id,
            initial_available: self.initial_available,
            available,
            pending_reserved,
            uncertain_reserved,
            committed_exposure,
            ledger_kind: self.ledger_kind,
            cumulative_fees: self.cumulative_fees,
            realized_pnl: self.realized_pnl,
            settled_equity_base: self.settled_equity_base,
            open_lots,
            reservations: self.reservations,
        })
    }
}

fn apply_projected_release(
    reservation: &mut PaperReservationView,
    sequence: u64,
    fact: &TransitionFact,
    account_id: &str,
    expected_available: Money,
) -> Result<(), ()> {
    if fact.confirmed_exposure.is_some() {
        return Err(());
    }
    match (&fact.reason, &fact.proof) {
        (Some(reason), None) => {
            if matches!(
                reservation.phase,
                PaperReservationPhase::Committed | PaperReservationPhase::Released
            ) {
                return Err(());
            }
            bounded_reason(reason).map_err(|_| ())?;
        }
        (None, Some(proof)) => {
            if reservation.phase != PaperReservationPhase::Committed
                || reservation.ledger_kind != PaperExecutionLedgerKind::LegacyReservationOnly
            {
                return Err(());
            }
            validate_projected_reconciliation_proof(
                proof,
                account_id,
                reservation,
                expected_available,
                PaperReconciliationVerdict::Match,
            )?;
            apply_reconciliation_record(
                &mut reservation.reconciliation,
                PaperReconciliationOutcome::Released,
                proof,
                sequence,
            )?;
        }
        _ => return Err(()),
    }
    reservation.phase = PaperReservationPhase::Released;
    reservation.held_exposure = Money::default();
    Ok(())
}

fn apply_projected_reconciliation_failure(
    reservation: &mut PaperReservationView,
    sequence: u64,
    fact: &TransitionFact,
    account_id: &str,
    expected_available: Money,
) -> Result<(), ()> {
    if reservation.phase != PaperReservationPhase::Committed
        || fact.confirmed_exposure.is_some()
        || fact.reason.is_some()
    {
        return Err(());
    }
    let proof = fact.proof.as_ref().ok_or(())?;
    validate_projected_reconciliation_proof(
        proof,
        account_id,
        reservation,
        expected_available,
        PaperReconciliationVerdict::Mismatch,
    )?;
    apply_reconciliation_record(
        &mut reservation.reconciliation,
        PaperReconciliationOutcome::Failed,
        proof,
        sequence,
    )
}

fn validate_projected_reconciliation_proof(
    proof: &PaperReconciliationProof,
    account_id: &str,
    reservation: &PaperReservationView,
    expected_available: Money,
    verdict: PaperReconciliationVerdict,
) -> Result<(), ()> {
    ensure_reconciliation_proof_matches(
        proof,
        account_id,
        reservation.reservation_id,
        reservation.batch_id,
    )
    .map_err(|_| ())?;
    validate_reconciliation_evidence_values(proof, expected_available, verdict).map_err(|_| ())
}

fn validate_reserved_fact(fact: &ReservedFact, symbol: &str, journal_id: Uuid) -> Result<(), ()> {
    if fact.schema_version != PAPER_ACCOUNT_SCHEMA_VERSION
        || fact.journal_id != journal_id
        || fact.account_id != symbol
        || bounded_identity(&fact.account_id, "account id").is_err()
        || fact.initial_available <= Money::default()
        || fact.request.validate().is_err()
        || reserved_exposure(&fact.request).map_err(|_| ())? != fact.reserved_exposure
    {
        return Err(());
    }
    Ok(())
}

fn validate_initialized_fact(
    fact: &InitializedFact,
    symbol: &str,
    journal_id: Uuid,
) -> Result<(), ()> {
    if fact.schema_version != PAPER_ACCOUNT_SCHEMA_VERSION
        || fact.journal_id != journal_id
        || fact.account_id != symbol
        || bounded_identity(&fact.account_id, "account id").is_err()
        || fact.initial_available <= Money::default()
    {
        return Err(());
    }
    Ok(())
}

fn validate_transition_fact(
    fact: &TransitionFact,
    symbol: &str,
    decision: &str,
    journal_id: Uuid,
) -> Result<(), ()> {
    if fact.schema_version != PAPER_ACCOUNT_SCHEMA_VERSION
        || fact.journal_id != journal_id
        || fact.account_id != symbol
        || bounded_identity(&fact.account_id, "account id").is_err()
        || fact.reservation_id.is_nil()
        || fact.batch_id.is_nil()
    {
        return Err(());
    }
    match decision {
        PAPER_ACCOUNT_UNCERTAIN => {
            if fact.confirmed_exposure.is_some() || fact.reason.is_some() || fact.proof.is_some() {
                return Err(());
            }
        }
        PAPER_ACCOUNT_COMMITTED => {
            if fact.confirmed_exposure.is_none() || fact.reason.is_some() || fact.proof.is_some() {
                return Err(());
            }
        }
        PAPER_ACCOUNT_RELEASED => {
            if fact.confirmed_exposure.is_some() {
                return Err(());
            }
            match (&fact.reason, &fact.proof) {
                (Some(reason), None) => {
                    if bounded_reason(reason).is_err() {
                        return Err(());
                    }
                }
                (None, Some(proof)) => {
                    if proof.validate().is_err()
                        || proof.evidence().is_none_or(|evidence| {
                            evidence.verdict != PaperReconciliationVerdict::Match
                        })
                    {
                        return Err(());
                    }
                }
                _ => return Err(()),
            }
        }
        PAPER_ACCOUNT_RECONCILE_FAILED => {
            if fact.confirmed_exposure.is_some() || fact.reason.is_some() {
                return Err(());
            }
            if fact.proof.as_ref().is_none_or(|proof| {
                proof.validate().is_err()
                    || proof.evidence().is_none_or(|evidence| {
                        evidence.verdict != PaperReconciliationVerdict::Mismatch
                    })
            }) {
                return Err(());
            }
        }
        _ => return Err(()),
    }
    Ok(())
}

fn validate_execution_settled_fact(
    fact: &ExecutionSettledFact,
    symbol: &str,
    journal_id: Uuid,
) -> Result<(), ()> {
    if fact.schema_version != PAPER_ACCOUNT_SCHEMA_VERSION
        || fact.journal_id != journal_id
        || fact.account_id != symbol
        || bounded_identity(&fact.account_id, "account id").is_err()
        || fact.reservation_id.is_nil()
        || fact.batch_id.is_nil()
        || fact.fills.is_empty()
        || fact.fills.len() > MAX_RESERVATION_LEGS
    {
        return Err(());
    }
    for (index, fill) in fact.fills.iter().enumerate() {
        bounded_identity(&fill.exchange, "exchange").map_err(|_| ())?;
        bounded_identity(fill.symbol.as_str(), "symbol").map_err(|_| ())?;
        if fill.quantity == Quantity::default()
            || fill.average_fill_price.as_decimal() <= Money::default().as_decimal()
            || fill.notional <= Money::default()
            || fill.fee < Money::default()
            || fill.liquidity != PaperExecutionLiquidity::Taker
            || fill.leg_index != index
        {
            return Err(());
        }
        let expected = notional(fill.average_fill_price, fill.quantity).ok_or(())?;
        if expected != fill.notional {
            return Err(());
        }
    }
    Ok(())
}

fn execution_settlement_fact(
    journal_id: Uuid,
    account_id: &str,
    reservation: &PaperReservationView,
    receipts: &[TradingReceipt],
) -> Result<ExecutionSettledFact, PaperAccountError> {
    if receipts.len() != reservation.legs.len() {
        return Err(PaperAccountError::InvalidTransition);
    }
    let mut unused = reservation.legs.clone();
    let mut fills = Vec::with_capacity(receipts.len());
    for receipt in receipts {
        let TradingReceipt::Submitted { order, disposition } = receipt else {
            return Err(PaperAccountError::InvalidTransition);
        };
        if *disposition != crypto_trading_exchange::SubmissionDisposition::Filled
            || order.status != OrderStatus::Filled
            || order.filled_quantity != order.intent.quantity
            || order.filled_quantity == Quantity::default()
        {
            return Err(PaperAccountError::InvalidTransition);
        }
        let average_fill_price = order
            .average_fill_price
            .ok_or(PaperAccountError::InvalidTransition)?;
        let matched_leg = unused
            .iter()
            .position(|leg| {
                let identity_matches = leg.exchange() == order.intent.exchange
                    && leg.symbol() == &order.intent.symbol
                    && leg.market_type() == order.intent.market_type
                    && leg.side() == order.intent.side;
                let id_matches = leg
                    .client_order_id()
                    .is_none_or(|client_order_id| client_order_id == order.intent.client_order_id);
                let quantity_matches = leg
                    .expected_quantity()
                    .is_none_or(|quantity| quantity == order.filled_quantity);
                identity_matches && id_matches && quantity_matches
            })
            .ok_or(PaperAccountError::InvalidTransition)?;
        let matched_leg = unused.remove(matched_leg);
        let exchange = bounded_identity(&order.intent.exchange, "exchange")
            .map_err(|_| PaperAccountError::InvalidTransition)?;
        let fill_notional = notional(average_fill_price, order.filled_quantity)
            .ok_or(PaperAccountError::ArithmeticOverflow)?;
        if fill_notional > matched_leg.reserved_notional() {
            return Err(PaperAccountError::InvalidTransition);
        }
        let fee = exact_fee(fill_notional, reservation.cost_model.fee_bps())?;
        if fee > reservation.reserved_exposure || fill_notional > reservation.reserved_exposure {
            return Err(PaperAccountError::InvalidTransition);
        }
        fills.push(PaperExecutedFill {
            leg_index: matched_leg.index,
            exchange,
            symbol: order.intent.symbol.clone(),
            market_type: order.intent.market_type,
            side: order.intent.side,
            quantity: order.filled_quantity,
            average_fill_price,
            notional: fill_notional,
            fee,
            liquidity: PaperExecutionLiquidity::Taker,
            reduce_only: matched_leg.reduce_only,
        });
    }
    fills.sort_by_key(|fill| fill.leg_index);
    Ok(ExecutionSettledFact {
        schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
        journal_id,
        account_id: account_id.to_owned(),
        reservation_id: reservation.reservation_id,
        batch_id: reservation.batch_id,
        fills,
    })
}

fn reserved_exposure(request: &PaperReservationRequest) -> Result<Money, PaperAccountError> {
    exposure_with_cost(gross_exposure(request.legs.iter())?, request.cost_model)
}

fn additional_reservation_exposure(
    open_lots: &[PaperOpenLotView],
    reservations: &[PaperReservationView],
    request: &PaperReservationRequest,
) -> Result<Money, PaperAccountError> {
    for (index, leg) in request.legs.iter().enumerate() {
        if !leg.reduce_only
            || request.legs[..index]
                .iter()
                .any(|earlier| same_reduction_target(earlier, leg))
        {
            continue;
        }
        let requested = request
            .legs
            .iter()
            .filter(|candidate| same_reduction_target(candidate, leg))
            .try_fold(Quantity::default(), |total, candidate| {
                checked_add_quantity(
                    total,
                    candidate
                        .expected_quantity
                        .ok_or(PaperAccountError::ReduceOnlyCapacityExceeded)?,
                )
                .ok_or(PaperAccountError::ArithmeticOverflow)
            })?;
        let already_reserved = reservations
            .iter()
            .filter(|reservation| reservation.phase != PaperReservationPhase::Released)
            .flat_map(|reservation| reservation.legs.iter())
            .filter(|candidate| same_reduction_target(candidate, leg))
            .try_fold(Quantity::default(), |total, candidate| {
                checked_add_quantity(
                    total,
                    candidate
                        .expected_quantity
                        .ok_or(PaperAccountError::ReduceOnlyCapacityExceeded)?,
                )
                .ok_or(PaperAccountError::ArithmeticOverflow)
            })?;
        let available = open_lots
            .iter()
            .filter(|lot| lot_matches_reduction(lot, leg))
            .try_fold(Quantity::default(), |total, lot| {
                checked_add_quantity(total, lot.remaining_quantity)
                    .ok_or(PaperAccountError::ArithmeticOverflow)
            })?;
        let required = checked_add_quantity(requested, already_reserved)
            .ok_or(PaperAccountError::ArithmeticOverflow)?;
        if required > available {
            return Err(PaperAccountError::ReduceOnlyCapacityExceeded);
        }
    }

    let opening_gross = gross_exposure(request.legs.iter().filter(|leg| !leg.reduce_only))?;
    exposure_with_cost(opening_gross, request.cost_model)
}

fn gross_exposure<'a>(
    mut legs: impl Iterator<Item = &'a PaperReservationLeg>,
) -> Result<Money, PaperAccountError> {
    legs.try_fold(Money::default(), |total, leg| {
        checked_add_money(total, leg.reserved_notional).ok_or(PaperAccountError::ArithmeticOverflow)
    })
}

fn exposure_with_cost(
    gross: Money,
    cost_model: PaperCostModel,
) -> Result<Money, PaperAccountError> {
    let total_bps = cost_model
        .total_bps()
        .ok_or(PaperAccountError::ArithmeticOverflow)?;
    let bps = Money::from_str(&total_bps.to_string())
        .map_err(|_| PaperAccountError::ArithmeticOverflow)?
        .as_decimal();
    let divisor = Money::from_str("10000")
        .map_err(|_| PaperAccountError::ArithmeticOverflow)?
        .as_decimal();
    let buffer = gross
        .as_decimal()
        .checked_mul(bps)
        .and_then(|value| value.checked_div(divisor))
        .map(Money::new)
        .ok_or(PaperAccountError::ArithmeticOverflow)?;
    checked_add_money(gross, buffer).ok_or(PaperAccountError::ArithmeticOverflow)
}

fn same_reduction_target(left: &PaperReservationLeg, right: &PaperReservationLeg) -> bool {
    left.reduce_only
        && right.reduce_only
        && left.exchange == right.exchange
        && left.symbol == right.symbol
        && left.market_type == right.market_type
        && left.side == right.side
}

fn lot_matches_reduction(lot: &PaperOpenLotView, leg: &PaperReservationLeg) -> bool {
    lot.exchange == leg.exchange
        && lot.symbol == leg.symbol
        && lot.market_type == leg.market_type
        && lot.side != leg.side
}

fn validate_reduce_only_settlement(
    open_lots: &[PaperOpenLotView],
    fills: &[PaperExecutedFill],
) -> Result<(), PaperAccountError> {
    let mut remaining_lots = open_lots.to_vec();
    for fill in fills {
        let mut remaining = fill.quantity;
        while remaining > Quantity::default() {
            let Some(index) = remaining_lots.iter().position(|lot| {
                lot.exchange == fill.exchange
                    && lot.symbol == fill.symbol
                    && lot.market_type == fill.market_type
                    && lot.side != fill.side
            }) else {
                break;
            };
            let matched = min_quantity(remaining, remaining_lots[index].remaining_quantity)
                .map_err(|()| PaperAccountError::ArithmeticOverflow)?;
            remaining = checked_sub_quantity(remaining, matched)
                .ok_or(PaperAccountError::ArithmeticOverflow)?;
            remaining_lots[index].remaining_quantity =
                checked_sub_quantity(remaining_lots[index].remaining_quantity, matched)
                    .ok_or(PaperAccountError::ArithmeticOverflow)?;
            if remaining_lots[index].remaining_quantity == Quantity::default() {
                remaining_lots.remove(index);
            }
        }
        if remaining > Quantity::default() {
            if fill.reduce_only {
                return Err(PaperAccountError::ReduceOnlyCapacityExceeded);
            }
            remaining_lots.push(PaperOpenLotView {
                source_reservation_id: Uuid::nil(),
                exchange: fill.exchange.clone(),
                symbol: fill.symbol.clone(),
                market_type: fill.market_type,
                side: fill.side,
                remaining_quantity: remaining,
                entry_price: fill.average_fill_price,
                remaining_notional: fill.notional,
                remaining_entry_fee: fill.fee,
                held_exposure: fill.notional,
                opened_sequence: 0,
            });
        }
    }
    Ok(())
}

fn checked_add_money(left: Money, right: Money) -> Option<Money> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .map(Money::new)
}

fn available_after_holds(equity: Money, held: Money) -> Option<Money> {
    equity
        .as_decimal()
        .checked_sub(held.as_decimal())
        .map(|value| Money::new(value.max(Money::default().as_decimal())))
}

fn checked_sub_money_allow_negative(left: Money, right: Money) -> Option<Money> {
    left.as_decimal()
        .checked_sub(right.as_decimal())
        .map(Money::new)
}

fn checked_sub_quantity(left: Quantity, right: Quantity) -> Option<Quantity> {
    left.as_decimal()
        .checked_sub(right.as_decimal())
        .and_then(|value| Quantity::new(value).ok())
}

fn checked_add_quantity(left: Quantity, right: Quantity) -> Option<Quantity> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .and_then(|value| Quantity::new(value).ok())
}

fn notional(price: Price, quantity: Quantity) -> Option<Money> {
    price
        .as_decimal()
        .checked_mul(quantity.as_decimal())
        .map(Money::new)
}

fn exact_fee(notional: Money, fee_bps: u32) -> Result<Money, PaperAccountError> {
    let bps = Money::from_str(&fee_bps.to_string())
        .map_err(|_| PaperAccountError::ArithmeticOverflow)?
        .as_decimal();
    let divisor = Money::from_str("10000")
        .map_err(|_| PaperAccountError::ArithmeticOverflow)?
        .as_decimal();
    notional
        .as_decimal()
        .checked_mul(bps)
        .and_then(|value| value.checked_div(divisor))
        .map(Money::new)
        .ok_or(PaperAccountError::ArithmeticOverflow)
}

fn min_quantity(left: Quantity, right: Quantity) -> Result<Quantity, ()> {
    Quantity::new(left.as_decimal().min(right.as_decimal())).map_err(|_| ())
}

fn proportional_money(total: Money, part: Quantity, whole: Quantity) -> Result<Money, ()> {
    if whole == Quantity::default() {
        return Err(());
    }
    total
        .as_decimal()
        .checked_mul(part.as_decimal())
        .and_then(|value| value.checked_div(whole.as_decimal()))
        .map(Money::new)
        .ok_or(())
}

fn consume_lot_money(
    remaining_value: &mut Money,
    consumed_quantity: Quantity,
    original_quantity: Quantity,
) -> Result<Money, ()> {
    if consumed_quantity > original_quantity {
        return Err(());
    }
    let consumed = if consumed_quantity == original_quantity {
        *remaining_value
    } else {
        proportional_money(*remaining_value, consumed_quantity, original_quantity)?
    };
    *remaining_value = checked_sub_money_allow_negative(*remaining_value, consumed).ok_or(())?;
    Ok(consumed)
}

fn realized_close_delta(
    opened_side: Side,
    entry_notional: Money,
    exit_notional: Money,
    entry_fee: Money,
    close_fee: Money,
) -> Result<Money, ()> {
    let gross = gross_close_delta(opened_side, entry_notional, exit_notional)?;
    checked_sub_money_allow_negative(
        checked_sub_money_allow_negative(gross, entry_fee).ok_or(())?,
        close_fee,
    )
    .ok_or(())
}

fn gross_close_delta(
    opened_side: Side,
    entry_notional: Money,
    exit_notional: Money,
) -> Result<Money, ()> {
    match opened_side {
        Side::Buy => checked_sub_money_allow_negative(exit_notional, entry_notional).ok_or(()),
        Side::Sell => checked_sub_money_allow_negative(entry_notional, exit_notional).ok_or(()),
    }
}

fn active_reservation_count(reservations: &[PaperReservationView]) -> usize {
    reservations
        .iter()
        .filter(|reservation| reservation.is_active())
        .count()
}

fn require_writable(snapshot: &PaperAccountSnapshot) -> Result<(), PaperAccountError> {
    if snapshot.projection_status != ProjectionStatus::Complete || snapshot.invalid_event_count != 0
    {
        return Err(PaperAccountError::DurableStateDegraded);
    }
    Ok(())
}

pub(crate) fn shared_authority_lock(path: &Path) -> Arc<AuthorityLock> {
    let registry = AUTHORITY_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = registry.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(AsyncMutex::new(()));
    registry.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

pub(crate) fn shared_operation_lock(path: &Path, account_id: &str) -> Arc<OperationLock> {
    let registry = OPERATION_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, lock| lock.strong_count() > 0);
    let key = OperationLockKey {
        path: path.to_path_buf(),
        account_id: account_id.to_owned(),
    };
    if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(AsyncMutex::new(()));
    registry.insert(key, Arc::downgrade(&lock));
    lock
}

pub(crate) fn bounded_identity(value: &str, field: &'static str) -> Result<String, &'static str> {
    let normalized = value.trim();
    if normalized.is_empty()
        || normalized.len() > MAX_LABEL_BYTES
        || !normalized.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
    {
        return Err(match field {
            "account id" => "paper account id is empty, oversized, or transport-unsafe",
            "task id" => "paper task id is empty, oversized, or transport-unsafe",
            "idempotency key" => "paper idempotency key is empty, oversized, or transport-unsafe",
            "exchange" => "paper exchange id is empty, oversized, or transport-unsafe",
            "symbol" => "paper symbol is empty, oversized, or transport-unsafe",
            _ => "paper identity is empty, oversized, or transport-unsafe",
        });
    }
    Ok(normalized.to_owned())
}

fn bounded_reason(value: &str) -> Result<String, PaperAccountError> {
    let normalized = value.trim();
    if normalized.is_empty()
        || normalized.len() > MAX_REASON_BYTES
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(PaperAccountError::InvalidRequest(
            "paper release reason is empty, oversized, or transport-unsafe",
        ));
    }
    Ok(normalized.to_owned())
}

fn bounded_digest(
    algorithm: PaperReconciliationDigestAlgorithm,
    value: &str,
) -> Result<String, PaperAccountError> {
    let normalized = value.trim().to_ascii_lowercase();
    let expected_len = match algorithm {
        PaperReconciliationDigestAlgorithm::Fnv1a64 => RECONCILIATION_DIGEST_HEX_BYTES,
    };
    if normalized.len() != expected_len || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PaperAccountError::InvalidRequest(
            "paper reconciliation digest is malformed",
        ));
    }
    Ok(normalized)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn normalized_money(value: Money) -> Money {
    Money::new(value.as_decimal().normalize())
}

fn ensure_reconciliation_proof_matches(
    proof: &PaperReconciliationProof,
    account_id: &str,
    reservation_id: Uuid,
    batch_id: Uuid,
) -> Result<(), PaperAccountError> {
    if proof.account_id() != account_id
        || proof.reservation_id() != reservation_id
        || proof.batch_id() != batch_id
    {
        return Err(PaperAccountError::InvalidTransition);
    }
    Ok(())
}

fn validate_reconciliation_evidence(
    proof: &PaperReconciliationProof,
    snapshot: &PaperAccountSnapshot,
    reservation: &PaperReservationView,
    expected_verdict: PaperReconciliationVerdict,
) -> Result<(), PaperAccountError> {
    let expected_available = checked_add_money(snapshot.available, reservation.held_exposure)
        .ok_or(PaperAccountError::ArithmeticOverflow)?;
    validate_reconciliation_evidence_values(proof, expected_available, expected_verdict)
}

fn validate_reconciliation_evidence_values(
    proof: &PaperReconciliationProof,
    expected_available: Money,
    expected_verdict: PaperReconciliationVerdict,
) -> Result<(), PaperAccountError> {
    let evidence = proof
        .evidence()
        .ok_or(PaperAccountError::InvalidTransition)?;
    if evidence.verdict != expected_verdict {
        return Err(PaperAccountError::InvalidTransition);
    }
    if evidence.expected_available != expected_available {
        return Err(PaperAccountError::InvalidTransition);
    }
    Ok(())
}

fn validate_reconciliation_progress(
    current: Option<&PaperReconciliationRecord>,
    outcome: PaperReconciliationOutcome,
    proof: &PaperReconciliationProof,
) -> Result<(), PaperAccountError> {
    if let Some(current) = current
        && (current.proof != *proof || outcome != current.outcome)
    {
        return Err(PaperAccountError::InvalidTransition);
    }
    Ok(())
}

fn matches_reconciliation(
    current: Option<&PaperReconciliationRecord>,
    outcome: PaperReconciliationOutcome,
    proof: &PaperReconciliationProof,
) -> bool {
    current.is_some_and(|current| current.outcome == outcome && current.proof == *proof)
}

fn apply_reconciliation_record(
    current: &mut Option<PaperReconciliationRecord>,
    outcome: PaperReconciliationOutcome,
    proof: &PaperReconciliationProof,
    evidence_sequence: u64,
) -> Result<(), ()> {
    if let Some(existing) = current
        && (existing.proof != *proof || outcome != existing.outcome)
    {
        return Err(());
    }
    *current = Some(PaperReconciliationRecord {
        outcome,
        proof: proof.clone(),
        evidence_sequence,
    });
    Ok(())
}

fn exact_object<'a>(value: &'a Value, expected: &[&str]) -> Result<&'a Map<String, Value>, ()> {
    let object = value.as_object().ok_or(())?;
    if object.len() != expected.len() || expected.iter().any(|field| !object.contains_key(*field)) {
        return Err(());
    }
    Ok(object)
}

fn text(value: Option<&Value>) -> Result<&str, ()> {
    value.and_then(Value::as_str).ok_or(())
}

fn require_money_strings_for_reserved(details: &Value) -> Result<(), ()> {
    let details = details.as_object().ok_or(())?;
    require_money_string(details.get("initial_available"))?;
    require_money_string(details.get("reserved_exposure"))?;
    let request = details
        .get("request")
        .and_then(Value::as_object)
        .ok_or(())?;
    let legs = request.get("legs").and_then(Value::as_array).ok_or(())?;
    for leg in legs {
        require_money_string(leg.as_object().and_then(|leg| leg.get("reserved_notional")))?;
    }
    Ok(())
}

fn require_money_strings_for_initialized(details: &Value) -> Result<(), ()> {
    let details = details.as_object().ok_or(())?;
    require_money_string(details.get("initial_available"))
}

fn require_money_strings_for_transition(details: &Value) -> Result<(), ()> {
    let details = details.as_object().ok_or(())?;
    if let Some(value) = details.get("confirmed_exposure")
        && !value.is_null()
    {
        require_money_string(Some(value))?;
    }
    Ok(())
}

fn require_money_strings_for_execution_settled(details: &Value) -> Result<(), ()> {
    let details = details.as_object().ok_or(())?;
    let fills = details.get("fills").and_then(Value::as_array).ok_or(())?;
    for fill in fills {
        let fill = fill.as_object().ok_or(())?;
        require_money_string(fill.get("notional"))?;
        require_money_string(fill.get("fee"))?;
    }
    Ok(())
}

fn require_money_string(value: Option<&Value>) -> Result<(), ()> {
    let value = value.and_then(Value::as_str).ok_or(())?;
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'-' | b'.'))
        || Money::from_str(value).is_err()
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_thousand_round_trip_flips_do_not_exhaust_projected_balance() {
        let journal_id = Uuid::from_u128(1);
        let mut account = AccountAccumulator::new("paper-main".to_owned(), money("1000"));
        let mut sequence = 1_u64;

        apply_fill_pair(&mut account, journal_id, sequence, 1, Side::Buy, "1", "0.1");
        sequence += 2;

        let mut side = Side::Sell;
        for index in 0..1_000_u32 {
            apply_fill_pair(
                &mut account,
                journal_id,
                sequence,
                u128::from(index) + 2,
                side,
                "2",
                "0.2",
            );
            sequence += 2;
            side = match side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
        }

        let snapshot = account
            .finish(journal_id, ProjectionStatus::Complete, 0)
            .unwrap();
        assert_eq!(snapshot.available, money("999.6999"));
        assert_eq!(snapshot.committed_exposure, money("0.1"));
        assert_eq!(snapshot.cumulative_fees, money("0.2001"));
        assert_eq!(snapshot.realized_pnl, money("-0.2"));
        assert_eq!(snapshot.settled_equity_base, money("999.7999"));
        assert_eq!(snapshot.open_lots.len(), 1);
    }

    #[test]
    fn released_reservations_do_not_consume_active_capacity_slots() {
        let journal_id = Uuid::from_u128(9);
        let mut account = AccountAccumulator::new("paper-main".to_owned(), money("1000"));
        let mut sequence = 1_u64;

        for identity in 0..(MAX_PAPER_ACCOUNT_RESERVATIONS as u128 + 8) {
            let request = reservation_request(identity, Side::Buy, "0.5");
            let reservation_id = request.reservation_id();
            let batch_id = request.batch_id();
            let reserved = reserved_exposure(&request).unwrap();
            account
                .reserve(
                    sequence,
                    ReservedFact {
                        schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                        journal_id,
                        account_id: "paper-main".to_owned(),
                        initial_available: money("1000"),
                        request,
                        reserved_exposure: reserved,
                    },
                )
                .unwrap();
            account
                .transition(
                    sequence + 1,
                    PAPER_ACCOUNT_RELEASED,
                    &TransitionFact {
                        schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                        journal_id,
                        account_id: "paper-main".to_owned(),
                        reservation_id,
                        batch_id,
                        confirmed_exposure: None,
                        reason: Some("capacity_released".to_owned()),
                        proof: None,
                    },
                )
                .unwrap();
            assert!(
                account.reservations.is_empty(),
                "released reservations should be pruned from the live projection"
            );
            sequence += 2;
        }

        let next =
            reservation_request(MAX_PAPER_ACCOUNT_RESERVATIONS as u128 + 42, Side::Sell, "1");
        let next_reserved = reserved_exposure(&next).unwrap();
        account
            .reserve(
                sequence,
                ReservedFact {
                    schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                    journal_id,
                    account_id: "paper-main".to_owned(),
                    initial_available: money("1000"),
                    request: next,
                    reserved_exposure: next_reserved,
                },
            )
            .unwrap();

        let snapshot = account
            .finish(journal_id, ProjectionStatus::Complete, 0)
            .unwrap();
        assert_eq!(snapshot.reservations.len(), 1);
        assert_eq!(snapshot.pending_reserved, money("1.001"));
    }

    fn apply_fill_pair(
        account: &mut AccountAccumulator,
        journal_id: Uuid,
        sequence: u64,
        identity: u128,
        side: Side,
        quantity_value: &str,
        notional_value: &str,
    ) {
        let reservation_id = Uuid::from_u128(identity.checked_mul(2).unwrap());
        let batch_id = Uuid::from_u128(identity.checked_mul(2).unwrap() + 1);
        let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
        let quantity = Quantity::from_str(quantity_value).unwrap();
        let notional = money(notional_value);
        let intent = OrderIntent::market(
            "paper-grid",
            symbol.clone(),
            MarketType::Spot,
            side,
            quantity,
        );
        let request = PaperReservationRequest::new(
            reservation_id,
            format!("flip/{identity}"),
            format!("flip-key/{identity}"),
            batch_id,
            PaperCostModel::v1(10, 0, 0).unwrap(),
            vec![PaperReservationLeg::from_intent(0, &intent, notional).unwrap()],
        )
        .unwrap();
        let reserved = reserved_exposure(&request).unwrap();
        account
            .reserve(
                sequence,
                ReservedFact {
                    schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                    journal_id,
                    account_id: "paper-main".to_owned(),
                    initial_available: money("1000"),
                    request,
                    reserved_exposure: reserved,
                },
            )
            .unwrap();
        account
            .settle_execution(
                sequence + 1,
                ExecutionSettledFact {
                    schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                    journal_id,
                    account_id: "paper-main".to_owned(),
                    reservation_id,
                    batch_id,
                    fills: vec![PaperExecutedFill {
                        leg_index: 0,
                        exchange: "paper-grid".to_owned(),
                        symbol,
                        market_type: MarketType::Spot,
                        side,
                        quantity,
                        average_fill_price: Price::from_str("0.1").unwrap(),
                        notional,
                        fee: exact_fee(notional, 10).unwrap(),
                        liquidity: PaperExecutionLiquidity::Taker,
                        reduce_only: false,
                    }],
                },
            )
            .unwrap();
    }

    fn reservation_request(
        identity: u128,
        side: Side,
        reserved_notional: &str,
    ) -> PaperReservationRequest {
        let symbol = Symbol::new("BTC-USDT-SPOT").unwrap();
        let quantity = Quantity::from_str("1").unwrap();
        let notional = money(reserved_notional);
        let intent = OrderIntent::market("paper-grid", symbol, MarketType::Spot, side, quantity);
        PaperReservationRequest::new(
            Uuid::from_u128(identity.checked_mul(2).unwrap_or(identity + 1) + 10_000),
            format!("capacity/{identity}"),
            format!("capacity-key/{identity}"),
            Uuid::from_u128(identity.checked_mul(2).unwrap_or(identity + 1) + 10_001),
            PaperCostModel::v1(10, 0, 0).unwrap(),
            vec![PaperReservationLeg::from_intent(0, &intent, notional).unwrap()],
        )
        .unwrap()
    }

    fn money(value: &str) -> Money {
        Money::from_str(value).unwrap()
    }
}

#[derive(Debug, Error)]
pub enum PaperAccountProjectionError {
    #[error(transparent)]
    Journal(#[from] JournalReadError),
    #[error("paper account journal pagination did not advance")]
    NonAdvancingPage,
    #[error("paper account projection arithmetic overflowed")]
    ArithmeticOverflow,
}

#[derive(Debug, Error)]
pub enum PaperAccountError {
    #[error("invalid paper account configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid paper account request: {0}")]
    InvalidRequest(&'static str),
    #[error("paper account arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("paper account durable state is degraded; reconcile or repair before writing")]
    DurableStateDegraded,
    #[error("paper account durable state conflicts with the configured initial capacity")]
    AccountConfigConflict,
    #[error("paper reservation idempotency key conflicts with durable identity")]
    IdempotencyConflict,
    #[error("paper reservation or batch id already belongs to another request")]
    ReservationIdentityConflict,
    #[error("paper task already has an active reservation")]
    ActiveTaskReservation,
    #[error("paper reservation's bound account-risk admission is unavailable")]
    RiskAdmissionUnavailable,
    #[error("paper reservation's bound account-risk admission lease has expired")]
    RiskAdmissionExpired,
    #[error("paper reservation does not match its bound account-risk admission")]
    RiskAdmissionMismatch,
    #[error("paper account has only {available} available but reservation needs {required}")]
    InsufficientAvailable { required: Money, available: Money },
    #[error("paper reduce-only reservation exceeds exact open-lot capacity")]
    ReduceOnlyCapacityExceeded,
    #[error("paper account reservation limit {limit} was reached")]
    ReservationLimitExceeded { limit: usize },
    #[error("paper reservation is unknown")]
    UnknownReservation,
    #[error("paper reservation transition is invalid")]
    InvalidTransition,
    #[error("paper account fact did not reappear after a synchronized append")]
    DurableStateConflict,
    #[error("paper account snapshot worker failed")]
    SnapshotTaskFailed,
    #[error(transparent)]
    JournalRead(#[from] JournalReadError),
    #[error(transparent)]
    Projection(#[from] PaperAccountProjectionError),
    #[error("paper account fact serialization failed: {0}")]
    Serialize(serde_json::Error),
    #[error(transparent)]
    JournalWrite(HistoryError),
}
