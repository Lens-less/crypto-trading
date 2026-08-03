//! Durable, paper-only account-level risk authority.
//!
//! The authority owns account-scoped admission truth: daily trade counts with
//! forward-only UTC-midnight reset (admission timestamps that regress across
//! midnight keep counting against the latest observed day), owner-level
//! open-position clocks, pause/resume facts, and a latching kill switch.
//! Authorities cold-replay the shared operations journal once, then refresh a
//! shared incremental projection before another mutation is admitted; every
//! deterministic rejection is itself a durable fact. Exposure and balance
//! observations come from the
//! paper-account projection of the same journal generation, plus each owner's
//! admitted-but-not-yet-reserved notional replayed from this scope's own
//! admission facts, so risk state can never be newer than account state and
//! concurrent owners cannot double-spend the exposure caps between admission
//! and reservation.
//!
//! Facts join the shared operations journal (the same journal that owns
//! paper-account reservation facts) instead of a dedicated file: the control
//! plane projects every read model from one frozen journal generation, these
//! control facts are low-volume, and a separate chain would let risk state and
//! account state drift across generations.

use std::{path::Path, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::Money;
use crypto_trading_strategy::{
    AccountRiskDecision, AccountRiskInput, AccountRiskOpenPosition, AccountRiskPolicy,
    AccountRiskRejection, AccountRiskWarning, StrategyError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::authority_state::{AuthorityStateCache, AuthorityStateError};
use crate::{
    DecisionRecord, HistoryError, JournalPageBoundary, JournalReadError, JournalSnapshot,
    JsonlHistory, LegacyJsonlJournalReader, PaperAccountProjectionError, PaperAccountReadModel,
    PaperExecutionLedgerKind, PaperReservationPhase, PaperReservationRequest, ProjectionStatus,
    paper_account::{
        AuthorityLock, PAPER_ACCOUNT_RESERVED, bounded_identity, shared_authority_lock,
    },
};

/// Stable version of the public account-risk read model.
///
/// Durable facts have their own version because adding ticket leases must not
/// silently change the control-plane response schema.
pub const ACCOUNT_RISK_SCHEMA_VERSION: u16 = 1;
const LEGACY_ACCOUNT_RISK_SCHEMA_VERSION: u16 = 1;
const ACCOUNT_RISK_FACT_SCHEMA_VERSION: u16 = 2;
/// Hard bound for distinct risk scopes reconstructed from one journal.
pub const MAX_ACCOUNT_RISK_SCOPES: usize = 16;
/// Hard bound for concurrently open owner-level position clocks per scope.
pub const MAX_ACCOUNT_RISK_SCOPE_POSITIONS: usize = 64;
/// Wall-clock lease granted to an admitted-but-not-yet-reserved ticket.
///
/// The market observation time supplied to [`AccountRiskAuthority::admit`]
/// may be historical during replay, so it must never drive this lease.
pub const ACCOUNT_RISK_ADMISSION_LEASE_SECONDS: i64 = 300;
/// Defensive replay bound. This is deliberately larger than the live
/// admission limit so a journal written by an older buggy process remains
/// loadable long enough for durable compensation to repair it.
const MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS: usize = 4_096;

const ACCOUNT_RISK_STRATEGY: &str = "account_risk";
const ACCOUNT_RISK_ADMITTED: &str = "account_risk_admitted";
const ACCOUNT_RISK_ADMISSION_CANCELLED: &str = "account_risk_admission_cancelled";
const ACCOUNT_RISK_ADMISSION_EXPIRED: &str = "account_risk_admission_expired";
const ACCOUNT_RISK_REJECTED: &str = "account_risk_rejected";
const ACCOUNT_RISK_POSITION_CLOSED: &str = "account_risk_position_closed";
const ACCOUNT_RISK_PAUSED: &str = "account_risk_paused";
const ACCOUNT_RISK_RESUMED: &str = "account_risk_resumed";
const ACCOUNT_RISK_KILL_SWITCH: &str = "account_risk_kill_switch_engaged";
const MAX_RISK_REASON_BYTES: usize = 128;
const MAX_RISK_WARNINGS: usize = 8;
const UTC_DATE_FORMAT: &str = "%Y-%m-%d";

/// One admission candidate judged before any account reservation is created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRiskCandidate {
    task_id: String,
    symbol: String,
    notional: Money,
}

impl AccountRiskCandidate {
    /// Creates one bounded admission candidate for an owner-level entry.
    ///
    /// # Errors
    ///
    /// Returns [`AccountRiskError::InvalidRequest`] for unsafe identities or a
    /// non-positive notional.
    pub fn new(
        task_id: impl Into<String>,
        symbol: impl Into<String>,
        notional: Money,
    ) -> Result<Self, AccountRiskError> {
        let task_id = bounded_identity(&task_id.into(), "task id")
            .map_err(AccountRiskError::InvalidRequest)?;
        let symbol =
            bounded_identity(&symbol.into(), "symbol").map_err(AccountRiskError::InvalidRequest)?;
        if notional <= Money::default() {
            return Err(AccountRiskError::InvalidRequest(
                "candidate notional must be positive",
            ));
        }
        Ok(Self {
            task_id,
            symbol: symbol.to_ascii_uppercase(),
            notional,
        })
    }

    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub const fn notional(&self) -> Money {
        self.notional
    }
}

/// Durable admission outcome. Rejections are facts, not errors: the caller
/// must skip the candidate and keep its owner alive unless a directive says
/// otherwise.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountRiskAdmissionTicket(String);

impl AccountRiskAdmissionTicket {
    fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Reconstructs a validated durable ticket identifier.
    ///
    /// # Errors
    ///
    /// Returns [`AccountRiskError::InvalidRequest`] for a malformed or nil
    /// UUID.
    pub fn parse(value: impl Into<String>) -> Result<Self, AccountRiskError> {
        let value = value.into();
        if !valid_ticket(&value) {
            return Err(AccountRiskError::InvalidRequest(
                "account risk admission ticket must be a non-nil UUID",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountRiskAdmission {
    Admitted {
        ticket: AccountRiskAdmissionTicket,
        warnings: Vec<AccountRiskWarning>,
    },
    Rejected(AccountRiskRejection),
}

/// Consumer-facing closure demand derived from durable risk state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountRiskDirective {
    /// Kill switch or a critically low total balance: close everything and
    /// stop admitting. The consumer decides the bounded action.
    CloseAllPositions { reason: String },
    /// One owner-level position exceeded the maximum holding duration.
    ClosePosition { task_id: String, symbol: String },
}

/// One open owner-level position clock reconstructed from admission facts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRiskOpenPositionView {
    pub task_id: String,
    pub symbol: String,
    pub opened_at: DateTime<Utc>,
}

/// Deterministic per-scope risk state reconstructed from the journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRiskStateView {
    pub schema_version: u16,
    pub scope_id: String,
    pub paused: bool,
    pub pause_reason: Option<String>,
    pub kill_switch_engaged: bool,
    pub kill_switch_reason: Option<String>,
    /// Latest UTC date (YYYY-MM-DD) observed on an admission fact. The count
    /// resets only when this date rolls forward; backdated admissions keep
    /// counting against it.
    pub trade_date_utc: Option<String>,
    pub daily_trade_count: u32,
    pub open_positions: Vec<AccountRiskOpenPositionView>,
    #[serde(skip)]
    open_position_tickets: Vec<Option<String>>,
    #[serde(skip)]
    admission_clocks: Vec<AccountRiskAdmissionClock>,
    pub admitted_count: u64,
    pub rejected_count: u64,
    pub last_rejection: Option<String>,
    pub last_recorded_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AccountRiskAdmissionClock {
    task_id: String,
    symbol: String,
    ticket_id: String,
    recorded_at: DateTime<Utc>,
}

impl AccountRiskStateView {
    pub(crate) fn empty(scope_id: String) -> Self {
        Self {
            schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
            scope_id,
            paused: false,
            pause_reason: None,
            kill_switch_engaged: false,
            kill_switch_reason: None,
            trade_date_utc: None,
            daily_trade_count: 0,
            open_positions: Vec::new(),
            open_position_tickets: Vec::new(),
            admission_clocks: Vec::new(),
            admitted_count: 0,
            rejected_count: 0,
            last_rejection: None,
            last_recorded_at: None,
        }
    }

    /// Admitted trades already counted at the supplied instant's UTC day.
    ///
    /// `YYYY-MM-DD` orders lexicographically as it does chronologically. An
    /// instant before the latched trade date means admission timestamps
    /// regressed across UTC midnight (owners feed replay-driven clocks), so
    /// the latched count is reported as-is instead of a fresh zero: the cap
    /// must fail closed rather than reset on a backwards date change.
    #[must_use]
    pub fn daily_trade_count_at(&self, now: DateTime<Utc>) -> u32 {
        if self.trade_date_utc.as_deref() >= Some(utc_date(now).as_str()) {
            self.daily_trade_count
        } else {
            0
        }
    }
}

/// Bounded account-risk projection over one immutable journal generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRiskReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub projection_status: ProjectionStatus,
    pub invalid_event_count: u64,
    pub scopes: Vec<AccountRiskStateView>,
}

impl AccountRiskReadModel {
    /// Reconstructs bounded account-risk facts from one immutable journal.
    ///
    /// # Errors
    ///
    /// Returns [`AccountRiskProjectionError`] for journal failures, pagination
    /// that cannot advance, or hard scope/position resource exhaustion.
    pub fn from_legacy_snapshot(
        snapshot: &JournalSnapshot,
    ) -> Result<Self, AccountRiskProjectionError> {
        let mut projection = ProjectionBuilder::new(snapshot.journal_id());
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            for event in page.events() {
                projection.observe_event(event)?;
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    projection.mark_partial_tail();
                    break;
                }
                JournalPageBoundary::PageLimit => {
                    let next = page.next_cursor().cloned();
                    if next == cursor {
                        return Err(AccountRiskProjectionError::NonAdvancingPage);
                    }
                    cursor = next;
                }
            }
        }
        Ok(projection.finish())
    }

    /// Returns the reconstructed state for one scope, if any fact exists.
    #[must_use]
    pub fn scope(&self, scope_id: &str) -> Option<&AccountRiskStateView> {
        self.scopes.iter().find(|scope| scope.scope_id == scope_id)
    }

    fn apply_event(
        &mut self,
        event: &crate::OperationEventEnvelope,
    ) -> Result<(), AccountRiskProjectionError> {
        let fact = match validated_account_risk_fact(event, self.journal_id) {
            Ok(Some(fact)) => fact,
            Ok(None) => return Ok(()),
            Err(()) => {
                self.invalid_event_count = self.invalid_event_count.saturating_add(1);
                self.projection_status = ProjectionStatus::Degraded;
                return Ok(());
            }
        };
        let fact = fact.fact;
        let scope = self.scope_mut(fact.scope_id())?;
        scope.last_recorded_at = Some(match scope.last_recorded_at {
            Some(previous) => previous.max(fact.recorded_at()),
            None => fact.recorded_at(),
        });
        apply_fact_to_scope(scope, fact)
    }

    fn scope_mut(
        &mut self,
        scope_id: &str,
    ) -> Result<&mut AccountRiskStateView, AccountRiskProjectionError> {
        if let Some(index) = self
            .scopes
            .iter()
            .position(|scope| scope.scope_id == scope_id)
        {
            return Ok(&mut self.scopes[index]);
        }
        if self.scopes.len() >= MAX_ACCOUNT_RISK_SCOPES {
            return Err(AccountRiskProjectionError::ScopeLimitExceeded {
                limit: MAX_ACCOUNT_RISK_SCOPES,
            });
        }
        self.scopes
            .push(AccountRiskStateView::empty(scope_id.to_owned()));
        let index = self.scopes.len() - 1;
        Ok(&mut self.scopes[index])
    }
}

fn apply_fact_to_scope(
    scope: &mut AccountRiskStateView,
    fact: AccountRiskFact,
) -> Result<(), AccountRiskProjectionError> {
    match fact {
        AccountRiskFact::Admitted {
            task_id,
            symbol,
            ticket_id,
            utc_date,
            recorded_at,
            ..
        } => {
            let ticket = ticket_id
                .clone()
                .ok_or(AccountRiskProjectionError::InvalidAdmissionTicket)?;
            // Reset only on a forward UTC-date roll: a fact dated before the
            // latched day keeps counting instead of zeroing the daily cap.
            if scope.trade_date_utc.as_deref() < Some(utc_date.as_str()) {
                scope.trade_date_utc = Some(utc_date);
                scope.daily_trade_count = 0;
            }
            scope.daily_trade_count = scope.daily_trade_count.saturating_add(1);
            scope.admitted_count = scope.admitted_count.saturating_add(1);
            if scope.admission_clocks.len() >= MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS {
                return Err(AccountRiskProjectionError::OpenPositionLimitExceeded {
                    limit: MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS,
                });
            }
            scope.admission_clocks.push(AccountRiskAdmissionClock {
                task_id: task_id.clone(),
                symbol: symbol.clone(),
                ticket_id: ticket,
                recorded_at,
            });
            if !scope
                .open_positions
                .iter()
                .any(|position| position.task_id == task_id)
            {
                if scope.open_positions.len() >= MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS {
                    return Err(AccountRiskProjectionError::OpenPositionLimitExceeded {
                        limit: MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS,
                    });
                }
                scope.open_positions.push(AccountRiskOpenPositionView {
                    task_id,
                    symbol,
                    opened_at: recorded_at,
                });
                scope.open_position_tickets.push(ticket_id);
            }
        }
        AccountRiskFact::AdmissionCancelled {
            task_id, ticket_id, ..
        }
        | AccountRiskFact::AdmissionExpired {
            task_id, ticket_id, ..
        } => {
            if let Some(index) = scope
                .admission_clocks
                .iter()
                .position(|clock| clock.task_id == task_id && clock.ticket_id == ticket_id)
            {
                scope.admission_clocks.remove(index);
                reconcile_open_position_clock(scope, &task_id)?;
            }
        }
        AccountRiskFact::PositionClosed { task_id, .. } => {
            scope
                .admission_clocks
                .retain(|clock| clock.task_id != task_id);
            if let Some(index) = scope
                .open_positions
                .iter()
                .position(|position| position.task_id == task_id)
            {
                remove_open_position(scope, index);
            }
        }
        AccountRiskFact::Rejected { rejection, .. } => {
            scope.rejected_count = scope.rejected_count.saturating_add(1);
            scope.last_rejection = Some(rejection);
        }
        AccountRiskFact::Paused { reason, .. } => {
            scope.paused = true;
            scope.pause_reason = reason;
        }
        AccountRiskFact::Resumed { .. } => {
            scope.paused = false;
            scope.pause_reason = None;
        }
        AccountRiskFact::KillSwitchEngaged { reason, .. } => {
            scope.kill_switch_engaged = true;
            scope.kill_switch_reason = reason;
        }
    }
    Ok(())
}

fn remove_open_position(scope: &mut AccountRiskStateView, index: usize) {
    scope.open_positions.remove(index);
    scope.open_position_tickets.remove(index);
}

fn reconcile_open_position_clock(
    scope: &mut AccountRiskStateView,
    task_id: &str,
) -> Result<(), AccountRiskProjectionError> {
    let position_index = scope
        .open_positions
        .iter()
        .position(|position| position.task_id == task_id);
    let next_clock = scope
        .admission_clocks
        .iter()
        .find(|clock| clock.task_id == task_id)
        .cloned();
    match (position_index, next_clock) {
        (Some(index), Some(clock)) => {
            scope.open_positions[index].symbol = clock.symbol;
            scope.open_positions[index].opened_at = clock.recorded_at;
            scope.open_position_tickets[index] = Some(clock.ticket_id);
        }
        (Some(index), None) => remove_open_position(scope, index),
        (None, Some(clock)) => {
            if scope.open_positions.len() >= MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS {
                return Err(AccountRiskProjectionError::OpenPositionLimitExceeded {
                    limit: MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS,
                });
            }
            scope.open_positions.push(AccountRiskOpenPositionView {
                task_id: clock.task_id,
                symbol: clock.symbol,
                opened_at: clock.recorded_at,
            });
            scope.open_position_tickets.push(Some(clock.ticket_id));
        }
        (None, None) => {}
    }
    Ok(())
}

/// Incremental account-risk interpreter shared by standalone and composite
/// journal projections. Keeping the state machine here prevents control-plane
/// fan-out optimization from growing a second interpretation of money facts.
pub(crate) struct ProjectionBuilder {
    model: AccountRiskReadModel,
}

impl ProjectionBuilder {
    pub(crate) const fn new(journal_id: Uuid) -> Self {
        Self {
            model: AccountRiskReadModel {
                schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
                journal_id,
                projection_status: ProjectionStatus::Complete,
                invalid_event_count: 0,
                scopes: Vec::new(),
            },
        }
    }

    pub(crate) fn from_model(model: AccountRiskReadModel) -> Self {
        Self { model }
    }

    pub(crate) fn observe_event(
        &mut self,
        event: &crate::OperationEventEnvelope,
    ) -> Result<(), AccountRiskProjectionError> {
        self.model.apply_event(event)
    }

    pub(crate) fn mark_partial_tail(&mut self) {
        self.model.projection_status = ProjectionStatus::Degraded;
    }

    pub(crate) fn finish(mut self) -> AccountRiskReadModel {
        self.model
            .scopes
            .sort_by(|left, right| left.scope_id.cmp(&right.scope_id));
        self.model
    }
}

/// Process-local authority over one journal-backed account-risk scope.
///
/// The authority serializes on the same per-journal lock as
/// [`crate::PaperAccountAuthority`], so a risk admission can never interleave
/// with an account reservation's read-modify-write on the same journal.
#[derive(Clone, Debug)]
pub struct AccountRiskAuthority {
    journal_id: Uuid,
    history: JsonlHistory,
    scope_id: String,
    policy: AccountRiskPolicy,
    authority_lock: Arc<AuthorityLock>,
    state_cache: AuthorityStateCache,
}

impl AccountRiskAuthority {
    /// Creates one account-risk authority bound to a durable journal
    /// generation and a validated pure policy.
    ///
    /// # Errors
    ///
    /// Returns [`AccountRiskError::InvalidConfig`] for a nil journal ID or an
    /// unsafe scope identity.
    pub fn new(
        journal_id: Uuid,
        history: JsonlHistory,
        scope_id: impl Into<String>,
        policy: AccountRiskPolicy,
    ) -> Result<Self, AccountRiskError> {
        if journal_id.is_nil() {
            return Err(AccountRiskError::InvalidConfig(
                "account risk journal id must not be nil",
            ));
        }
        let scope_id = bounded_identity(&scope_id.into(), "scope id")
            .map_err(AccountRiskError::InvalidConfig)?;
        let authority_lock =
            shared_authority_lock(&crate::history::normalized_lock_key(history.path()));
        let state_cache = AuthorityStateCache::new(journal_id, &history);
        Ok(Self {
            journal_id,
            history,
            scope_id,
            policy,
            authority_lock,
            state_cache,
        })
    }

    #[must_use]
    pub const fn journal_id(&self) -> Uuid {
        self.journal_id
    }

    #[must_use]
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    #[must_use]
    pub const fn policy(&self) -> &AccountRiskPolicy {
        &self.policy
    }

    #[must_use]
    pub fn history_path(&self) -> &Path {
        self.history.path()
    }

    /// Cold-replays the current frozen journal head and verifies that it is
    /// equivalent to the process-local authority projection.
    ///
    /// A detected mismatch permanently degrades every in-process authority
    /// sharing this journal generation. Repeated calls are idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`AccountRiskError::DegradedState`] when a cached projection
    /// cannot be proven equivalent, including malformed durable bytes, or
    /// after that failure has been latched. Initial projection loading can
    /// still return its corresponding journal or projection error.
    pub async fn verify_durable_state(&self) -> Result<(), AccountRiskError> {
        let _guard = self.authority_lock.lock().await;
        self.state_cache
            .verify_durable_state(&self.history)
            .await
            .map_err(map_authority_state_error_to_risk)
    }

    /// Returns the reconstructed durable state for this scope.
    ///
    /// # Errors
    ///
    /// Fails closed on journal, projection, or degraded-state conflicts.
    pub async fn state(&self) -> Result<AccountRiskStateView, AccountRiskError> {
        let _guard = self.authority_lock.lock().await;
        Ok(self.load().await?.0)
    }

    /// Evaluates one candidate and durably records the outcome.
    ///
    /// A deterministic policy rejection is written as a rejection fact and
    /// returned as [`AccountRiskAdmission::Rejected`]; the caller must skip
    /// the candidate. Admission writes an admitted fact that increments the
    /// UTC-day trade count and opens the owner-level position clock.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state, journal I/O, or arithmetic
    /// that cannot be represented.
    pub async fn admit(
        &self,
        candidate: &AccountRiskCandidate,
        now: DateTime<Utc>,
    ) -> Result<AccountRiskAdmission, AccountRiskError> {
        let _guard = self.authority_lock.lock().await;
        let authority_now = Utc::now();
        self.recover_expired_admissions_locked(authority_now)
            .await?;
        let (state, accounts, open_admissions) = self.load().await?;
        if open_admissions.len() >= MAX_ACCOUNT_RISK_SCOPE_POSITIONS
            || (state.open_positions.len() >= MAX_ACCOUNT_RISK_SCOPE_POSITIONS
                && !state
                    .open_positions
                    .iter()
                    .any(|position| position.task_id == candidate.task_id()))
        {
            return Err(AccountRiskError::AdmissionCapacityExceeded {
                limit: MAX_ACCOUNT_RISK_SCOPE_POSITIONS,
            });
        }
        let (mut symbol_exposure, mut total_exposure, total_balance) =
            exposures(&accounts, candidate.symbol())?;
        // Owners hold the shared lock for admission only, not for the later
        // reservation, so admitted-but-not-yet-reserved notional is invisible
        // to the reservation-based exposures. Replaying it here closes the
        // gap: a concurrent owner admitted between this owner's admission and
        // reservation still observes the in-flight notional.
        for admission in open_admissions {
            total_exposure = checked_add(total_exposure, admission.notional)?;
            if admission.symbol.eq_ignore_ascii_case(candidate.symbol()) {
                symbol_exposure = checked_add(symbol_exposure, admission.notional)?;
            }
        }
        let input = AccountRiskInput {
            candidate_symbol: candidate.symbol().to_owned(),
            candidate_notional: candidate.notional(),
            symbol_exposure,
            total_exposure,
            total_balance,
            daily_trade_count: state.daily_trade_count_at(now),
            paused_reason: state
                .paused
                .then(|| state.pause_reason.clone().unwrap_or_default()),
            kill_switch_reason: state
                .kill_switch_engaged
                .then(|| state.kill_switch_reason.clone().unwrap_or_default()),
        };
        match self.policy.evaluate(&input) {
            AccountRiskDecision::Admitted { warnings } => {
                let ticket = AccountRiskAdmissionTicket::new();
                let lease_expires_at = authority_now
                    .checked_add_signed(Duration::seconds(ACCOUNT_RISK_ADMISSION_LEASE_SECONDS))
                    .ok_or(AccountRiskError::ArithmeticOverflow)?;
                let labels = warnings
                    .iter()
                    .map(|warning| warning.label().to_owned())
                    .take(MAX_RISK_WARNINGS)
                    .collect();
                self.append_fact_at(
                    authority_now,
                    ACCOUNT_RISK_ADMITTED,
                    &AccountRiskFact::Admitted {
                        schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                        journal_id: self.journal_id,
                        scope_id: self.scope_id.clone(),
                        task_id: candidate.task_id().to_owned(),
                        symbol: candidate.symbol().to_owned(),
                        ticket_id: Some(ticket.as_str().to_owned()),
                        notional: candidate.notional(),
                        utc_date: utc_date(now),
                        recorded_at: now,
                        lease_expires_at: Some(lease_expires_at),
                        warnings: labels,
                    },
                )
                .await?;
                Ok(AccountRiskAdmission::Admitted { ticket, warnings })
            }
            AccountRiskDecision::Rejected(rejection) => {
                self.append_fact(
                    ACCOUNT_RISK_REJECTED,
                    &AccountRiskFact::Rejected {
                        schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                        journal_id: self.journal_id,
                        scope_id: self.scope_id.clone(),
                        task_id: candidate.task_id().to_owned(),
                        symbol: candidate.symbol().to_owned(),
                        rejection: rejection.label().to_owned(),
                        recorded_at: now,
                    },
                )
                .await?;
                Ok(AccountRiskAdmission::Rejected(rejection))
            }
        }
    }

    /// Durably expires every still-pending admission whose authority
    /// wall-clock lease has elapsed.
    ///
    /// Replaying an expiry removes the exact ticket only. Calling this method
    /// again, including after restart, therefore appends no duplicate
    /// compensation facts. [`Self::admit`] performs the same recovery before
    /// evaluating a new candidate.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state, serialization, or journal I/O.
    pub async fn recover_expired_admissions(
        &self,
        now: DateTime<Utc>,
    ) -> Result<usize, AccountRiskError> {
        let _guard = self.authority_lock.lock().await;
        self.recover_expired_admissions_locked(now).await
    }

    /// Durably cancels one still-pending admission ticket.
    ///
    /// Returns `Ok(false)` when the ticket does not belong to `task_id` or was
    /// already settled/cancelled. A successful cancel releases only that
    /// pending admission; daily trade counts remain durable facts.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state or journal I/O.
    pub async fn cancel_admission(
        &self,
        task_id: &str,
        ticket: &AccountRiskAdmissionTicket,
        now: DateTime<Utc>,
    ) -> Result<bool, AccountRiskError> {
        let task_id =
            bounded_identity(task_id, "task id").map_err(AccountRiskError::InvalidRequest)?;
        let _ = bounded_identity(ticket.as_str(), "admission ticket")
            .map_err(AccountRiskError::InvalidRequest)?;
        let _guard = self.authority_lock.lock().await;
        let (_, _, open_admissions) = self.load().await?;
        let Some(admission) = open_admissions.into_iter().find(|admission| {
            admission.task_id == task_id && admission.ticket_id.as_deref() == Some(ticket.as_str())
        }) else {
            return Ok(false);
        };
        self.append_fact(
            ACCOUNT_RISK_ADMISSION_CANCELLED,
            &AccountRiskFact::AdmissionCancelled {
                schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                scope_id: self.scope_id.clone(),
                task_id,
                ticket_id: ticket.as_str().to_owned(),
                admitted_at: admission.recorded_at,
                recorded_at: now,
            },
        )
        .await?;
        Ok(true)
    }

    /// Durably closes the owner-level position clock; unknown owners no-op.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state or journal I/O.
    pub async fn record_position_closed(
        &self,
        task_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AccountRiskError> {
        let task_id =
            bounded_identity(task_id, "task id").map_err(AccountRiskError::InvalidRequest)?;
        let _guard = self.authority_lock.lock().await;
        let (state, _, _) = self.load().await?;
        if !state
            .open_positions
            .iter()
            .any(|position| position.task_id == task_id)
        {
            return Ok(());
        }
        self.append_fact(
            ACCOUNT_RISK_POSITION_CLOSED,
            &AccountRiskFact::PositionClosed {
                schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                scope_id: self.scope_id.clone(),
                task_id,
                recorded_at: now,
            },
        )
        .await
    }

    /// Durably suspends new admissions; repeated pauses replace the reason
    /// only when it changed.
    ///
    /// # Errors
    ///
    /// Fails closed on an unsafe reason, degraded state, or journal I/O.
    pub async fn pause(
        &self,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<AccountRiskStateView, AccountRiskError> {
        let reason = bounded_reason(reason)?;
        let _guard = self.authority_lock.lock().await;
        let (state, _, _) = self.load().await?;
        if state.paused && state.pause_reason.as_deref() == Some(reason.as_str()) {
            return Ok(state);
        }
        self.append_fact(
            ACCOUNT_RISK_PAUSED,
            &AccountRiskFact::Paused {
                schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                scope_id: self.scope_id.clone(),
                reason: Some(reason),
                recorded_at: now,
            },
        )
        .await?;
        Ok(self.load().await?.0)
    }

    /// Durably resumes admissions after a pause. The kill switch is latching
    /// and is deliberately not cleared by resume.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state or journal I/O.
    pub async fn resume(
        &self,
        now: DateTime<Utc>,
    ) -> Result<AccountRiskStateView, AccountRiskError> {
        let _guard = self.authority_lock.lock().await;
        let (state, _, _) = self.load().await?;
        if !state.paused {
            return Ok(state);
        }
        self.append_fact(
            ACCOUNT_RISK_RESUMED,
            &AccountRiskFact::Resumed {
                schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                scope_id: self.scope_id.clone(),
                recorded_at: now,
            },
        )
        .await?;
        Ok(self.load().await?.0)
    }

    /// Durably engages the latching kill switch: every later admission is
    /// refused and [`Self::directives`] demands closing all positions. There
    /// is deliberately no disengage transition in this schema.
    ///
    /// # Errors
    ///
    /// Fails closed on an unsafe reason, degraded state, or journal I/O.
    pub async fn engage_kill_switch(
        &self,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<AccountRiskStateView, AccountRiskError> {
        let reason = bounded_reason(reason)?;
        let _guard = self.authority_lock.lock().await;
        let (state, _, _) = self.load().await?;
        if state.kill_switch_engaged {
            return Ok(state);
        }
        self.append_fact(
            ACCOUNT_RISK_KILL_SWITCH,
            &AccountRiskFact::KillSwitchEngaged {
                schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                journal_id: self.journal_id,
                scope_id: self.scope_id.clone(),
                reason: Some(reason),
                recorded_at: now,
            },
        )
        .await?;
        Ok(self.load().await?.0)
    }

    /// Derives the closure demands active at `now`: kill switch, critically
    /// low total balance, and expired owner-level position clocks.
    ///
    /// # Errors
    ///
    /// Fails closed on degraded durable state or journal failures.
    pub async fn directives(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<AccountRiskDirective>, AccountRiskError> {
        let _guard = self.authority_lock.lock().await;
        let (state, accounts, _) = self.load().await?;
        let mut directives = Vec::new();
        if state.kill_switch_engaged {
            directives.push(AccountRiskDirective::CloseAllPositions {
                reason: format!(
                    "kill_switch:{}",
                    state.kill_switch_reason.clone().unwrap_or_default()
                ),
            });
        }
        if let Some(limit) = self.policy.limits().min_balance_close {
            let (_, _, total_balance) = exposures(&accounts, "")?;
            if total_balance < limit {
                directives.push(AccountRiskDirective::CloseAllPositions {
                    reason: "balance_below_close_threshold".to_owned(),
                });
            }
        }
        let observations = state
            .open_positions
            .iter()
            .map(|position| AccountRiskOpenPosition {
                task_id: position.task_id.clone(),
                symbol: position.symbol.clone(),
                opened_at: position.opened_at,
            })
            .collect::<Vec<_>>();
        for expired in self
            .policy
            .expired_positions(&observations, now)
            .map_err(AccountRiskError::Strategy)?
        {
            directives.push(AccountRiskDirective::ClosePosition {
                task_id: expired.task_id,
                symbol: expired.symbol,
            });
        }
        Ok(directives)
    }

    async fn load(
        &self,
    ) -> Result<
        (
            AccountRiskStateView,
            PaperAccountReadModel,
            Vec<OpenAdmission>,
        ),
        AccountRiskError,
    > {
        let projection = self
            .state_cache
            .refresh(&self.history)
            .await
            .map_err(map_authority_state_error_to_risk)?;
        let state = projection
            .risk_state(&self.scope_id)
            .map_err(map_authority_state_error_to_risk)?;
        Ok((
            state,
            projection.paper_live.clone(),
            projection.open_admissions_for_scope(&self.scope_id),
        ))
    }

    async fn recover_expired_admissions_locked(
        &self,
        now: DateTime<Utc>,
    ) -> Result<usize, AccountRiskError> {
        let (_, _, open_admissions) = self.load().await?;
        let expired = open_admissions
            .into_iter()
            .filter(|admission| admission.lease_expires_at <= now)
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return Ok(0);
        }

        let mut records = Vec::with_capacity(expired.len());
        for admission in &expired {
            let ticket_id = admission
                .ticket_id
                .as_deref()
                .ok_or(AccountRiskError::DegradedState)?;
            records.push(self.fact_record(
                now,
                ACCOUNT_RISK_ADMISSION_EXPIRED,
                &AccountRiskFact::AdmissionExpired {
                    schema_version: ACCOUNT_RISK_FACT_SCHEMA_VERSION,
                    journal_id: self.journal_id,
                    scope_id: self.scope_id.clone(),
                    task_id: admission.task_id.clone(),
                    ticket_id: ticket_id.to_owned(),
                    admitted_at: admission.recorded_at,
                    lease_expires_at: admission.lease_expires_at,
                    expired_at: now,
                },
            )?);
        }
        self.history
            .append_batch(&records)
            .await
            .map_err(AccountRiskError::JournalWrite)?;
        Ok(expired.len())
    }

    async fn append_fact(
        &self,
        decision: &'static str,
        fact: &AccountRiskFact,
    ) -> Result<(), AccountRiskError> {
        self.append_fact_at(Utc::now(), decision, fact).await
    }

    async fn append_fact_at(
        &self,
        timestamp: DateTime<Utc>,
        decision: &'static str,
        fact: &AccountRiskFact,
    ) -> Result<(), AccountRiskError> {
        let record = self.fact_record(timestamp, decision, fact)?;
        self.history
            .append(&record)
            .await
            .map_err(AccountRiskError::JournalWrite)
    }

    fn fact_record(
        &self,
        timestamp: DateTime<Utc>,
        decision: &'static str,
        fact: &AccountRiskFact,
    ) -> Result<DecisionRecord, AccountRiskError> {
        let details = serde_json::to_value(fact).map_err(AccountRiskError::Serialize)?;
        Ok(DecisionRecord {
            timestamp,
            strategy: ACCOUNT_RISK_STRATEGY.to_owned(),
            symbol: self.scope_id.clone(),
            decision: decision.to_owned(),
            details,
        })
    }
}

fn map_authority_state_error_to_risk(error: AuthorityStateError) -> AccountRiskError {
    match error {
        AuthorityStateError::History | AuthorityStateError::Degraded => {
            AccountRiskError::DegradedState
        }
        AuthorityStateError::Journal(error) => AccountRiskError::JournalRead(error),
        AuthorityStateError::Paper(error) => AccountRiskError::PaperProjection(error),
        AuthorityStateError::Risk(error) => AccountRiskError::Projection(error),
    }
}

/// One owner's admitted-but-not-yet-reserved notional for one symbol.
#[derive(Clone, Debug)]
pub(crate) struct OpenAdmission {
    pub(crate) scope_id: String,
    pub(crate) task_id: String,
    pub(crate) symbol: String,
    pub(crate) ticket_id: Option<String>,
    pub(crate) recorded_at: DateTime<Utc>,
    pub(crate) lease_expires_at: DateTime<Utc>,
    pub(crate) notional: Money,
}

pub(crate) fn apply_open_admission_event(
    pending: &mut Vec<OpenAdmission>,
    event: &crate::OperationEventEnvelope,
) -> Result<(), AccountRiskError> {
    let payload = event.payload();
    let Some(decision) = payload.get("decision").and_then(Value::as_str) else {
        return Ok(());
    };
    match decision {
        ACCOUNT_RISK_ADMITTED
        | ACCOUNT_RISK_ADMISSION_CANCELLED
        | ACCOUNT_RISK_ADMISSION_EXPIRED
        | ACCOUNT_RISK_POSITION_CLOSED => apply_account_risk_open_event(pending, event)?,
        PAPER_ACCOUNT_RESERVED => apply_paper_reservation_open_event(pending, event)?,
        _ => {}
    }
    Ok(())
}

fn apply_account_risk_open_event(
    pending: &mut Vec<OpenAdmission>,
    event: &crate::OperationEventEnvelope,
) -> Result<(), AccountRiskError> {
    // Malformed facts are skipped here rather than counted: the main
    // projection already degrades on them, failing admission closed.
    let Ok(Some(validated)) = validated_account_risk_fact(event, event.journal_id()) else {
        return Ok(());
    };
    match validated.fact {
        AccountRiskFact::Admitted {
            scope_id,
            task_id,
            symbol,
            ticket_id,
            recorded_at,
            notional,
            ..
        } => record_open_admission(
            pending,
            OpenAdmission {
                scope_id,
                task_id,
                symbol,
                ticket_id,
                recorded_at,
                lease_expires_at: validated
                    .admitted_lease_expires_at
                    .ok_or(AccountRiskError::DegradedState)?,
                notional,
            },
        )?,
        AccountRiskFact::AdmissionCancelled {
            scope_id,
            ticket_id,
            ..
        }
        | AccountRiskFact::AdmissionExpired {
            scope_id,
            ticket_id,
            ..
        } => pending.retain(|entry| {
            entry.scope_id != scope_id || entry.ticket_id.as_deref() != Some(ticket_id.as_str())
        }),
        AccountRiskFact::PositionClosed {
            scope_id, task_id, ..
        } => pending.retain(|entry| entry.scope_id != scope_id || entry.task_id != task_id),
        _ => {}
    }
    Ok(())
}

fn apply_paper_reservation_open_event(
    pending: &mut Vec<OpenAdmission>,
    event: &crate::OperationEventEnvelope,
) -> Result<(), AccountRiskError> {
    let request = event
        .payload()
        .get("details")
        .and_then(|details| details.get("request"))
        .cloned()
        .map(serde_json::from_value::<PaperReservationRequest>);
    let Some(Ok(request)) = request else {
        return Ok(());
    };
    if let Some((scope_id, ticket_id)) = request.risk_admission_binding() {
        let Some(index) = pending.iter().position(|admission| {
            admission.scope_id == scope_id && admission.ticket_id.as_deref() == Some(ticket_id)
        }) else {
            return Err(AccountRiskError::DegradedState);
        };
        if event.recorded_at() >= pending[index].lease_expires_at
            || !bound_admission_matches_reservation(&pending[index], &request)?
        {
            return Err(AccountRiskError::DegradedState);
        }
        pending.remove(index);
        return Ok(());
    }
    settle_legacy_reservation_admissions(pending, &request)
}

fn settle_legacy_reservation_admissions(
    pending: &mut Vec<OpenAdmission>,
    request: &PaperReservationRequest,
) -> Result<(), AccountRiskError> {
    for leg in request.legs() {
        let mut matching_scopes = Vec::new();
        for admission in pending.iter().filter(|admission| {
            admission_matches_reservation(admission, request.task_id(), leg.symbol().as_str())
        }) {
            if !matching_scopes.contains(&admission.scope_id) {
                matching_scopes.push(admission.scope_id.clone());
            }
        }
        for scope_id in matching_scopes {
            settle_open_admission(
                pending,
                &scope_id,
                request.task_id(),
                leg.symbol().as_str(),
                leg.reserved_notional(),
            )?;
        }
    }
    Ok(())
}

fn record_open_admission(
    pending: &mut Vec<OpenAdmission>,
    admission: OpenAdmission,
) -> Result<(), AccountRiskError> {
    if pending
        .iter()
        .filter(|entry| entry.scope_id == admission.scope_id)
        .count()
        >= MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS
    {
        return Err(AccountRiskProjectionError::OpenAdmissionLimitExceeded {
            limit: MAX_ACCOUNT_RISK_RECOVERY_SCOPE_POSITIONS,
        }
        .into());
    }
    pending.push(admission);
    Ok(())
}

/// Consumes reserved notional from the owner's pending admission, floored at
/// zero. Owners admit under their lifecycle identity but reserve under the
/// documented per-operation identity `<owner>/op/<sequence>`, so ownership is
/// matched by that prefix (or an exact identity match).
fn settle_open_admission(
    pending: &mut Vec<OpenAdmission>,
    scope_id: &str,
    reservation_task_id: &str,
    symbol: &str,
    reserved: Money,
) -> Result<(), AccountRiskError> {
    let mut remaining = reserved;
    while remaining > Money::default() {
        let Some(index) = pending.iter().position(|entry| {
            entry.scope_id == scope_id
                && admission_matches_reservation(entry, reservation_task_id, symbol)
        }) else {
            break;
        };
        if pending[index].notional > remaining {
            pending[index].notional = checked_sub(pending[index].notional, remaining)?;
            break;
        }
        remaining = checked_sub(remaining, pending[index].notional)?;
        pending.remove(index);
    }
    Ok(())
}

fn admission_matches_reservation(
    admission: &OpenAdmission,
    reservation_task_id: &str,
    symbol: &str,
) -> bool {
    (admission.task_id == reservation_task_id
        || numeric_operation_suffix(&admission.task_id, reservation_task_id))
        && admission.symbol.eq_ignore_ascii_case(symbol)
}

fn numeric_operation_suffix(owner_task_id: &str, reservation_task_id: &str) -> bool {
    reservation_task_id
        .strip_prefix(owner_task_id)
        .and_then(|suffix| suffix.strip_prefix("/op/"))
        .is_some_and(|operation| {
            !operation.is_empty() && operation.bytes().all(|byte| byte.is_ascii_digit())
        })
}

pub(crate) fn bound_admission_matches_reservation(
    admission: &OpenAdmission,
    request: &PaperReservationRequest,
) -> Result<bool, AccountRiskError> {
    if !admission_matches_reservation(admission, request.task_id(), &admission.symbol) {
        return Ok(false);
    }
    let mut opening_notional = Money::default();
    let mut opening_legs = 0_usize;
    for leg in request.legs().iter().filter(|leg| !leg.reduce_only()) {
        if !leg
            .symbol()
            .as_str()
            .eq_ignore_ascii_case(&admission.symbol)
        {
            return Ok(false);
        }
        opening_notional = checked_add(opening_notional, leg.reserved_notional())?;
        opening_legs = opening_legs.saturating_add(1);
    }
    Ok(opening_legs > 0 && opening_notional <= admission.notional)
}

/// Symbol, global, and total-balance observations derived from the active
/// paper ledger. Legacy reservations retain their conservative original-leg
/// semantics; exact settlements use remaining FIFO lots and settled equity.
fn exposures(
    accounts: &PaperAccountReadModel,
    symbol: &str,
) -> Result<(Money, Money, Money), AccountRiskError> {
    let mut symbol_exposure = Money::default();
    let mut total_exposure = Money::default();
    let mut total_balance = Money::default();
    for account in &accounts.accounts {
        for value in [
            account.pending_reserved,
            account.uncertain_reserved,
            account.committed_exposure,
        ] {
            total_exposure = checked_add(total_exposure, value)?;
        }
        total_balance = checked_add(total_balance, account.settled_equity_base)?;
        for reservation in &account.reservations {
            if reservation.phase == PaperReservationPhase::Released
                || reservation.ledger_kind == PaperExecutionLedgerKind::ExactExecution
            {
                continue;
            }
            for leg in &reservation.legs {
                if !symbol.is_empty() && leg.symbol().as_str().eq_ignore_ascii_case(symbol) {
                    symbol_exposure = checked_add(symbol_exposure, leg.reserved_notional())?;
                }
            }
        }
        for lot in &account.open_lots {
            if !symbol.is_empty() && lot.symbol.as_str().eq_ignore_ascii_case(symbol) {
                symbol_exposure = checked_add(symbol_exposure, lot.held_exposure)?;
            }
        }
    }
    Ok((symbol_exposure, total_exposure, total_balance))
}

fn checked_add(left: Money, right: Money) -> Result<Money, AccountRiskError> {
    left.as_decimal()
        .checked_add(right.as_decimal())
        .map(Money::new)
        .ok_or(AccountRiskError::ArithmeticOverflow)
}

fn checked_sub(left: Money, right: Money) -> Result<Money, AccountRiskError> {
    left.as_decimal()
        .checked_sub(right.as_decimal())
        .map(Money::new)
        .ok_or(AccountRiskError::ArithmeticOverflow)
}

fn bounded_reason(value: &str) -> Result<String, AccountRiskError> {
    let normalized = value.trim();
    if normalized.is_empty()
        || normalized.len() > MAX_RISK_REASON_BYTES
        || normalized.chars().any(char::is_control)
    {
        return Err(AccountRiskError::InvalidRequest(
            "account risk reason must be a bounded, control-free label",
        ));
    }
    Ok(normalized.to_owned())
}

fn utc_date(now: DateTime<Utc>) -> String {
    now.format(UTC_DATE_FORMAT).to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AccountRiskFact {
    Admitted {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        task_id: String,
        symbol: String,
        #[serde(default)]
        ticket_id: Option<String>,
        notional: Money,
        utc_date: String,
        recorded_at: DateTime<Utc>,
        #[serde(default)]
        lease_expires_at: Option<DateTime<Utc>>,
        warnings: Vec<String>,
    },
    AdmissionCancelled {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        task_id: String,
        ticket_id: String,
        admitted_at: DateTime<Utc>,
        recorded_at: DateTime<Utc>,
    },
    AdmissionExpired {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        task_id: String,
        ticket_id: String,
        admitted_at: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        expired_at: DateTime<Utc>,
    },
    Rejected {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        task_id: String,
        symbol: String,
        rejection: String,
        recorded_at: DateTime<Utc>,
    },
    PositionClosed {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        task_id: String,
        recorded_at: DateTime<Utc>,
    },
    Paused {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        reason: Option<String>,
        recorded_at: DateTime<Utc>,
    },
    Resumed {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        recorded_at: DateTime<Utc>,
    },
    KillSwitchEngaged {
        schema_version: u16,
        journal_id: Uuid,
        scope_id: String,
        reason: Option<String>,
        recorded_at: DateTime<Utc>,
    },
}

struct ValidatedAccountRiskFact {
    fact: AccountRiskFact,
    admitted_lease_expires_at: Option<DateTime<Utc>>,
}

fn validated_account_risk_fact(
    event: &crate::OperationEventEnvelope,
    expected_journal_id: Uuid,
) -> Result<Option<ValidatedAccountRiskFact>, ()> {
    let payload = event.payload();
    let Some(decision) = payload.get("decision").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !matches!(
        decision,
        ACCOUNT_RISK_ADMITTED
            | ACCOUNT_RISK_ADMISSION_CANCELLED
            | ACCOUNT_RISK_ADMISSION_EXPIRED
            | ACCOUNT_RISK_REJECTED
            | ACCOUNT_RISK_POSITION_CLOSED
            | ACCOUNT_RISK_PAUSED
            | ACCOUNT_RISK_RESUMED
            | ACCOUNT_RISK_KILL_SWITCH
    ) {
        return Ok(None);
    }

    let object = payload.as_object().ok_or(())?;
    if object.len() != 4
        || !["decision", "strategy", "symbol", "details"]
            .iter()
            .all(|key| object.contains_key(*key))
        || event.journal_id() != expected_journal_id
        || payload.get("strategy").and_then(Value::as_str) != Some(ACCOUNT_RISK_STRATEGY)
    {
        return Err(());
    }
    let outer_scope = payload.get("symbol").and_then(Value::as_str).ok_or(())?;
    let mut fact =
        serde_json::from_value::<AccountRiskFact>(payload.get("details").cloned().ok_or(())?)
            .map_err(|_| ())?;
    if !fact.has_supported_schema()
        || fact.journal_id() != expected_journal_id
        || fact.scope_id() != outer_scope
        || fact.matches_decision(decision).is_none()
        || !valid_identity(fact.scope_id(), "scope id")
    {
        return Err(());
    }

    let admitted_lease_expires_at = fact.validate_fields(event)?;
    fact.normalize_legacy_admission(event, admitted_lease_expires_at)?;
    Ok(Some(ValidatedAccountRiskFact {
        fact,
        admitted_lease_expires_at,
    }))
}

impl AccountRiskFact {
    const fn schema_version(&self) -> u16 {
        match self {
            Self::Admitted { schema_version, .. }
            | Self::AdmissionCancelled { schema_version, .. }
            | Self::AdmissionExpired { schema_version, .. }
            | Self::Rejected { schema_version, .. }
            | Self::PositionClosed { schema_version, .. }
            | Self::Paused { schema_version, .. }
            | Self::Resumed { schema_version, .. }
            | Self::KillSwitchEngaged { schema_version, .. } => *schema_version,
        }
    }

    const fn journal_id(&self) -> Uuid {
        match self {
            Self::Admitted { journal_id, .. }
            | Self::AdmissionCancelled { journal_id, .. }
            | Self::AdmissionExpired { journal_id, .. }
            | Self::Rejected { journal_id, .. }
            | Self::PositionClosed { journal_id, .. }
            | Self::Paused { journal_id, .. }
            | Self::Resumed { journal_id, .. }
            | Self::KillSwitchEngaged { journal_id, .. } => *journal_id,
        }
    }

    const fn has_supported_schema(&self) -> bool {
        match self {
            Self::AdmissionExpired { schema_version, .. } => {
                *schema_version == ACCOUNT_RISK_FACT_SCHEMA_VERSION
            }
            _ => matches!(
                self.schema_version(),
                LEGACY_ACCOUNT_RISK_SCHEMA_VERSION | ACCOUNT_RISK_FACT_SCHEMA_VERSION
            ),
        }
    }

    fn scope_id(&self) -> &str {
        match self {
            Self::Admitted { scope_id, .. }
            | Self::AdmissionCancelled { scope_id, .. }
            | Self::AdmissionExpired { scope_id, .. }
            | Self::Rejected { scope_id, .. }
            | Self::PositionClosed { scope_id, .. }
            | Self::Paused { scope_id, .. }
            | Self::Resumed { scope_id, .. }
            | Self::KillSwitchEngaged { scope_id, .. } => scope_id,
        }
    }

    const fn recorded_at(&self) -> DateTime<Utc> {
        match self {
            Self::Admitted { recorded_at, .. }
            | Self::AdmissionCancelled { recorded_at, .. }
            | Self::Rejected { recorded_at, .. }
            | Self::PositionClosed { recorded_at, .. }
            | Self::Paused { recorded_at, .. }
            | Self::Resumed { recorded_at, .. }
            | Self::KillSwitchEngaged { recorded_at, .. } => *recorded_at,
            Self::AdmissionExpired { expired_at, .. } => *expired_at,
        }
    }

    fn matches_decision(&self, decision: &str) -> Option<()> {
        let expected: &str = match self {
            Self::Admitted { .. } => ACCOUNT_RISK_ADMITTED,
            Self::AdmissionCancelled { .. } => ACCOUNT_RISK_ADMISSION_CANCELLED,
            Self::AdmissionExpired { .. } => ACCOUNT_RISK_ADMISSION_EXPIRED,
            Self::Rejected { .. } => ACCOUNT_RISK_REJECTED,
            Self::PositionClosed { .. } => ACCOUNT_RISK_POSITION_CLOSED,
            Self::Paused { .. } => ACCOUNT_RISK_PAUSED,
            Self::Resumed { .. } => ACCOUNT_RISK_RESUMED,
            Self::KillSwitchEngaged { .. } => ACCOUNT_RISK_KILL_SWITCH,
        };
        (expected == decision).then_some(())
    }

    fn validate_fields(
        &self,
        event: &crate::OperationEventEnvelope,
    ) -> Result<Option<DateTime<Utc>>, ()> {
        match self {
            Self::Admitted { .. } => self.validate_admitted_fields(event),
            Self::AdmissionCancelled {
                task_id, ticket_id, ..
            } => {
                if !valid_identity(task_id, "task id") || !valid_ticket(ticket_id) {
                    return Err(());
                }
                Ok(None)
            }
            Self::AdmissionExpired {
                task_id,
                ticket_id,
                lease_expires_at,
                expired_at,
                ..
            } => {
                if !valid_identity(task_id, "task id")
                    || !valid_ticket(ticket_id)
                    || expired_at < lease_expires_at
                    || *expired_at != event.recorded_at()
                {
                    return Err(());
                }
                Ok(None)
            }
            Self::Rejected {
                schema_version,
                task_id,
                symbol,
                rejection,
                ..
            } => {
                let valid_rejection = if *schema_version == LEGACY_ACCOUNT_RISK_SCHEMA_VERSION {
                    valid_reason(rejection)
                } else {
                    matches!(
                        rejection.as_str(),
                        "kill_switch_engaged"
                            | "paused"
                            | "symbol_disabled"
                            | "balance_below_close_threshold"
                            | "daily_trade_limit_reached"
                            | "symbol_exposure_exceeded"
                            | "total_exposure_exceeded"
                            | "invalid_candidate"
                            | "arithmetic_overflow"
                    )
                };
                if !valid_identity(task_id, "task id") || !valid_symbol(symbol) || !valid_rejection
                {
                    return Err(());
                }
                Ok(None)
            }
            Self::PositionClosed { task_id, .. } => {
                if !valid_identity(task_id, "task id") {
                    return Err(());
                }
                Ok(None)
            }
            Self::Paused { reason, .. } | Self::KillSwitchEngaged { reason, .. } => {
                if reason
                    .as_deref()
                    .is_some_and(|reason| !valid_reason(reason))
                {
                    return Err(());
                }
                Ok(None)
            }
            Self::Resumed { .. } => Ok(None),
        }
    }

    fn normalize_legacy_admission(
        &mut self,
        event: &crate::OperationEventEnvelope,
        admitted_lease_expires_at: Option<DateTime<Utc>>,
    ) -> Result<(), ()> {
        let Self::Admitted {
            schema_version,
            ticket_id,
            lease_expires_at,
            ..
        } = self
        else {
            return Ok(());
        };
        if *schema_version == LEGACY_ACCOUNT_RISK_SCHEMA_VERSION {
            ticket_id.get_or_insert_with(|| event.event_id().to_string());
            *lease_expires_at = admitted_lease_expires_at;
        }
        if ticket_id.is_none() || lease_expires_at.is_none() {
            return Err(());
        }
        Ok(())
    }

    fn validate_admitted_fields(
        &self,
        event: &crate::OperationEventEnvelope,
    ) -> Result<Option<DateTime<Utc>>, ()> {
        let Self::Admitted {
            schema_version,
            task_id,
            symbol,
            ticket_id,
            notional,
            utc_date: fact_utc_date,
            recorded_at,
            lease_expires_at,
            warnings,
            ..
        } = self
        else {
            return Err(());
        };
        if !valid_identity(task_id, "task id")
            || !valid_symbol(symbol)
            || ticket_id
                .as_deref()
                .is_some_and(|ticket| !valid_ticket(ticket))
            || (*schema_version == ACCOUNT_RISK_FACT_SCHEMA_VERSION && ticket_id.is_none())
            || *notional <= Money::default()
            || fact_utc_date != &utc_date(*recorded_at)
            || warnings.len() > MAX_RISK_WARNINGS
            || warnings.iter().any(|warning| {
                if *schema_version == LEGACY_ACCOUNT_RISK_SCHEMA_VERSION {
                    !valid_reason(warning)
                } else {
                    !matches!(warning.as_str(), "low_balance" | "high_risk_symbol")
                }
            })
        {
            return Err(());
        }
        let expected_lease = event
            .recorded_at()
            .checked_add_signed(Duration::seconds(ACCOUNT_RISK_ADMISSION_LEASE_SECONDS))
            .ok_or(())?;
        if lease_expires_at.is_some_and(|lease| lease != expected_lease) {
            return Err(());
        }
        if *schema_version == ACCOUNT_RISK_FACT_SCHEMA_VERSION && lease_expires_at.is_none() {
            return Err(());
        }
        Ok(Some(lease_expires_at.unwrap_or(expected_lease)))
    }
}

fn valid_identity(value: &str, field: &'static str) -> bool {
    bounded_identity(value, field).is_ok_and(|normalized| normalized == value)
}

fn valid_symbol(value: &str) -> bool {
    valid_identity(value, "symbol") && value == value.to_ascii_uppercase()
}

pub(crate) fn valid_ticket(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|ticket| !ticket.is_nil())
}

fn valid_reason(value: &str) -> bool {
    bounded_reason(value).is_ok_and(|normalized| normalized == value)
}

#[derive(Debug, Error)]
pub enum AccountRiskProjectionError {
    #[error(transparent)]
    Journal(#[from] JournalReadError),
    #[error("account risk projection could not advance past a page boundary")]
    NonAdvancingPage,
    #[error("account risk admitted fact is missing its exact ticket")]
    InvalidAdmissionTicket,
    #[error("account risk projection exceeds the {limit}-scope bound")]
    ScopeLimitExceeded { limit: usize },
    #[error("account risk projection exceeds the {limit}-open-position bound")]
    OpenPositionLimitExceeded { limit: usize },
    #[error("account risk projection exceeds the {limit}-open-admission bound")]
    OpenAdmissionLimitExceeded { limit: usize },
}

#[derive(Debug, Error)]
pub enum AccountRiskError {
    #[error("invalid account risk configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid account risk request: {0}")]
    InvalidRequest(&'static str),
    #[error("account risk durable state is degraded; recovery is required")]
    DegradedState,
    #[error("account risk live admission capacity exceeds the {limit}-admission bound")]
    AdmissionCapacityExceeded { limit: usize },
    #[error("account risk fact serialization failed: {0}")]
    Serialize(serde_json::Error),
    #[error(transparent)]
    JournalWrite(#[from] HistoryError),
    #[error(transparent)]
    JournalRead(#[from] JournalReadError),
    #[error(transparent)]
    Projection(#[from] AccountRiskProjectionError),
    #[error(transparent)]
    PaperProjection(#[from] PaperAccountProjectionError),
    #[error("account risk journal snapshot task failed")]
    SnapshotTaskFailed,
    #[error("account risk arithmetic overflow")]
    ArithmeticOverflow,
    #[error(transparent)]
    Strategy(StrategyError),
}
