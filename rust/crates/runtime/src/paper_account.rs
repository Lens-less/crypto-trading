//! Durable, paper-only pending-reservation authority.
//!
//! This module deliberately separates account reservation truth from execution
//! outcome truth. Every mutation is reconstructed from the synchronized JSONL
//! journal before another mutation is admitted. The shared [`JsonlHistory`]
//! writer owns a sibling lock-file lease, so competing processes fail closed
//! before appending to the same normalized journal path.

use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use chrono::Utc;
use crypto_trading_domain::{MarketType, Money, OrderIntent, Side, Symbol};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::{
    DecisionRecord, FileJournalSnapshotSource, HistoryError, JournalPageBoundary, JournalReadError,
    JournalSnapshot, JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader,
    ProjectionStatus,
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
const PAPER_ACCOUNT_RESERVED: &str = "paper_account_reserved";
const PAPER_ACCOUNT_UNCERTAIN: &str = "paper_account_uncertain";
const PAPER_ACCOUNT_COMMITTED: &str = "paper_account_committed";
const PAPER_ACCOUNT_RELEASED: &str = "paper_account_released";
const PAPER_ACCOUNT_RECONCILE_FAILED: &str = "paper_account_reconcile_failed";

pub(crate) type AuthorityLock = AsyncMutex<()>;
static AUTHORITY_LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Weak<AuthorityLock>>>> = OnceLock::new();

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
}

impl PaperReservationView {
    fn matches(&self, request: &PaperReservationRequest) -> bool {
        self.reservation_id == request.reservation_id
            && self.task_id == request.task_id
            && self.idempotency_key == request.idempotency_key
            && self.batch_id == request.batch_id
            && self.cost_model == request.cost_model
            && self.legs == request.legs
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
        Ok(Self {
            journal_id,
            history,
            config,
            authority_lock,
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

    #[must_use]
    pub const fn config(&self) -> &PaperAccountConfig {
        &self.config
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
        if snapshot.reservations.len() >= MAX_PAPER_ACCOUNT_RESERVATIONS {
            return Err(PaperAccountError::ReservationLimitExceeded {
                limit: MAX_PAPER_ACCOUNT_RESERVATIONS,
            });
        }
        let required = reserved_exposure(&request)?;
        if snapshot.available < required {
            return Err(PaperAccountError::InsufficientAvailable {
                required,
                available: snapshot.available,
            });
        }

        self.append_fact(
            PAPER_ACCOUNT_RESERVED,
            &ReservedFact {
                schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                account_id: self.config.account_id.clone(),
                initial_available: self.config.initial_available,
                request,
                reserved_exposure: required,
            },
        )
        .await?;
        let updated = self.load_account_snapshot().await?;
        require_writable(&updated)?;
        let reservation = updated
            .reservations
            .last()
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
        let reservation = find_reservation(&snapshot, reservation_id)?;
        match reservation.phase {
            PaperReservationPhase::Uncertain => return Ok(reservation.clone()),
            PaperReservationPhase::Pending => {}
            PaperReservationPhase::Committed | PaperReservationPhase::Released => {
                return Err(PaperAccountError::InvalidTransition);
            }
        }
        self.append_transition(PAPER_ACCOUNT_UNCERTAIN, reservation, None, None, None)
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
        let reservation = find_reservation(&snapshot, reservation_id)?;
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
            reservation,
            Some(confirmed_exposure),
            None,
            None,
        )
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
        let reservation = find_reservation(&snapshot, reservation_id)?;
        if reservation.phase == PaperReservationPhase::Released {
            return Ok(reservation.clone());
        }
        if reservation.phase == PaperReservationPhase::Committed {
            return Err(PaperAccountError::InvalidTransition);
        }
        self.append_transition(
            PAPER_ACCOUNT_RELEASED,
            reservation,
            None,
            Some(reason),
            None,
        )
        .await?;
        self.reloaded_reservation(reservation_id).await
    }

    /// Releases committed exposure only when caller presents durable,
    /// replayable reconciliation proof bound to this account and batch.
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
        let reservation = find_reservation(&snapshot, proof.reservation_id())?;
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
        validate_reconciliation_evidence(
            &proof,
            &snapshot,
            reservation,
            PaperReconciliationVerdict::Match,
        )?;
        validate_reconciliation_progress(
            reservation.reconciliation.as_ref(),
            PaperReconciliationOutcome::Released,
            &proof,
        )?;
        self.append_transition(PAPER_ACCOUNT_RELEASED, reservation, None, None, Some(proof))
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
        let reservation = find_reservation(&snapshot, proof.reservation_id())?;
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
        validate_reconciliation_evidence(
            &proof,
            &snapshot,
            reservation,
            PaperReconciliationVerdict::Mismatch,
        )?;
        if matches_reconciliation(
            reservation.reconciliation.as_ref(),
            PaperReconciliationOutcome::Failed,
            &proof,
        ) {
            return Ok(reservation.clone());
        }
        validate_reconciliation_progress(
            reservation.reconciliation.as_ref(),
            PaperReconciliationOutcome::Failed,
            &proof,
        )?;
        self.append_transition(
            PAPER_ACCOUNT_RECONCILE_FAILED,
            reservation,
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
        let details = serde_json::to_value(fact).map_err(PaperAccountError::Serialize)?;
        self.history
            .append(&DecisionRecord {
                timestamp: Utc::now(),
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
        let updated = self.load_account_snapshot().await?;
        require_writable(&updated)?;
        find_reservation(&updated, reservation_id).cloned()
    }

    async fn load_account_snapshot(&self) -> Result<PaperAccountSnapshot, PaperAccountError> {
        let source = FileJournalSnapshotSource::new(self.journal_id, self.history.path())?;
        let source_path = source.path().to_owned();
        let journal_id = self.journal_id;
        let journal = tokio::task::spawn_blocking(move || match std::fs::metadata(&source_path) {
            Ok(_) => source.snapshot(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                JournalSnapshot::new(journal_id, Vec::new())
            }
            Err(error) => Err(JournalReadError::Metadata(error)),
        })
        .await
        .map_err(|_| PaperAccountError::SnapshotTaskFailed)??;
        let model = PaperAccountReadModel::from_legacy_snapshot(&journal)?;
        if let Some(account) = model
            .accounts
            .iter()
            .find(|account| account.account_id == self.config.account_id)
        {
            if account.initial_available != self.config.initial_available {
                return Err(PaperAccountError::AccountConfigConflict);
            }
            return Ok(account.clone());
        }
        Ok(PaperAccountSnapshot {
            schema_version: PAPER_ACCOUNT_SCHEMA_VERSION,
            journal_id: self.journal_id,
            projection_status: model.projection_status,
            invalid_event_count: model.invalid_event_count,
            account_id: self.config.account_id.clone(),
            initial_available: self.config.initial_available,
            available: self.config.initial_available,
            pending_reserved: Money::default(),
            uncertain_reserved: Money::default(),
            committed_exposure: Money::default(),
            reservations: Vec::new(),
        })
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

struct ProjectionBuilder {
    journal_id: Uuid,
    projection_status: ProjectionStatus,
    invalid_event_count: u64,
    accounts: Vec<AccountAccumulator>,
}

impl ProjectionBuilder {
    const fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            projection_status: ProjectionStatus::Complete,
            invalid_event_count: 0,
            accounts: Vec::new(),
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
                self.apply_event(event.sequence(), event.payload());
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    self.projection_status = ProjectionStatus::Degraded;
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

    fn apply_event(&mut self, sequence: u64, payload: &Value) {
        let Some(decision) = payload.get("decision").and_then(Value::as_str) else {
            return;
        };
        if !matches!(
            decision,
            PAPER_ACCOUNT_RESERVED
                | PAPER_ACCOUNT_UNCERTAIN
                | PAPER_ACCOUNT_COMMITTED
                | PAPER_ACCOUNT_RELEASED
                | PAPER_ACCOUNT_RECONCILE_FAILED
        ) {
            return;
        }
        if self.try_apply_event(sequence, decision, payload).is_err() {
            self.invalid_event_count = self.invalid_event_count.saturating_add(1);
            self.projection_status = ProjectionStatus::Degraded;
        }
    }

    fn try_apply_event(
        &mut self,
        sequence: u64,
        decision: &str,
        payload: &Value,
    ) -> Result<(), ()> {
        let payload = exact_object(payload, &["decision", "details", "strategy", "symbol"])?;
        if text(payload.get("decision"))? != decision
            || text(payload.get("strategy"))? != PAPER_ACCOUNT_STRATEGY
        {
            return Err(());
        }
        let symbol = text(payload.get("symbol"))?;
        let details = payload.get("details").ok_or(())?.clone();
        match decision {
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
                self.accounts[account_index].reserve(sequence, fact)
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
                account.transition(sequence, decision, &fact)
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
                account.transition(sequence, decision, &fact)
            }
            _ => Err(()),
        }
    }
}

struct AccountAccumulator {
    account_id: String,
    initial_available: Money,
    reservations: Vec<PaperReservationView>,
}

impl AccountAccumulator {
    const fn new(account_id: String, initial_available: Money) -> Self {
        Self {
            account_id,
            initial_available,
            reservations: Vec::new(),
        }
    }

    fn reserve(&mut self, sequence: u64, fact: ReservedFact) -> Result<(), ()> {
        if self.initial_available != fact.initial_available
            || self.reservations.len() >= MAX_PAPER_ACCOUNT_RESERVATIONS
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
        let held = self.held_total().ok_or(())?;
        let available = checked_sub_money(self.initial_available, held).ok_or(())?;
        if available < fact.reserved_exposure {
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
            held_exposure: fact.reserved_exposure,
            phase: PaperReservationPhase::Pending,
            first_sequence: sequence,
            last_sequence: sequence,
            reconciliation: None,
        });
        Ok(())
    }

    fn transition(
        &mut self,
        sequence: u64,
        decision: &str,
        fact: &TransitionFact,
    ) -> Result<(), ()> {
        let reservation_index = self
            .reservations
            .iter()
            .position(|reservation| reservation.reservation_id == fact.reservation_id)
            .ok_or(())?;
        let held = self.held_total().ok_or(())?;
        let available = checked_sub_money(self.initial_available, held).ok_or(())?;
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
        Ok(())
    }

    fn held_total(&self) -> Option<Money> {
        self.reservations
            .iter()
            .try_fold(Money::default(), |total, reservation| {
                checked_add_money(total, reservation.held_exposure)
            })
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
        let available = checked_sub_money(self.initial_available, held)
            .ok_or(PaperAccountProjectionError::ArithmeticOverflow)?;
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
            if reservation.phase != PaperReservationPhase::Committed {
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

fn reserved_exposure(request: &PaperReservationRequest) -> Result<Money, PaperAccountError> {
    let gross = request
        .legs
        .iter()
        .try_fold(Money::default(), |total, leg| {
            checked_add_money(total, leg.reserved_notional)
                .ok_or(PaperAccountError::ArithmeticOverflow)
        })?;
    let total_bps = request
        .cost_model
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

fn checked_add_money(left: Money, right: Money) -> Option<Money> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .map(Money::new)
}

fn checked_sub_money(left: Money, right: Money) -> Option<Money> {
    left.as_decimal()
        .checked_sub(right.as_decimal())
        .filter(|value| !value.is_sign_negative())
        .map(Money::new)
}

fn find_reservation(
    snapshot: &PaperAccountSnapshot,
    reservation_id: Uuid,
) -> Result<&PaperReservationView, PaperAccountError> {
    snapshot
        .reservations
        .iter()
        .find(|reservation| reservation.reservation_id == reservation_id)
        .ok_or(PaperAccountError::UnknownReservation)
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
    if let Some(current) = current {
        if proof.snapshot_sequence() < current.proof.snapshot_sequence() {
            return Err(PaperAccountError::InvalidTransition);
        }
        if proof.snapshot_sequence() == current.proof.snapshot_sequence()
            && (current.proof != *proof || outcome != current.outcome)
        {
            return Err(PaperAccountError::InvalidTransition);
        }
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
    if let Some(existing) = current {
        if proof.snapshot_sequence() < existing.proof.snapshot_sequence() {
            return Err(());
        }
        if proof.snapshot_sequence() == existing.proof.snapshot_sequence()
            && (existing.proof != *proof || outcome != existing.outcome)
        {
            return Err(());
        }
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

fn require_money_strings_for_transition(details: &Value) -> Result<(), ()> {
    let details = details.as_object().ok_or(())?;
    if let Some(value) = details.get("confirmed_exposure")
        && !value.is_null()
    {
        require_money_string(Some(value))?;
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
    #[error("paper account has only {available} available but reservation needs {required}")]
    InsufficientAvailable { required: Money, available: Money },
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
