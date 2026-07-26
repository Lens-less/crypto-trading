//! Durable, paper-only account-level risk authority.
//!
//! The authority owns account-scoped admission truth: daily trade counts with
//! UTC-midnight reset, owner-level open-position clocks, pause/resume facts,
//! and a latching kill switch. Every mutation is reconstructed from the shared
//! operations journal before another mutation is admitted, and every
//! deterministic rejection is itself a durable fact. Exposure and balance
//! observations come from the paper-account projection of the same journal
//! generation, so risk state can never be newer than account state.
//!
//! Facts join the shared operations journal (the same journal that owns
//! paper-account reservation facts) instead of a dedicated file: the control
//! plane projects every read model from one frozen journal generation, these
//! control facts are low-volume, and a separate chain would let risk state and
//! account state drift across generations.

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::Money;
use crypto_trading_strategy::{
    AccountRiskDecision, AccountRiskInput, AccountRiskOpenPosition, AccountRiskPolicy,
    AccountRiskRejection, AccountRiskWarning, StrategyError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    DecisionRecord, FileJournalSnapshotSource, HistoryError, JournalPageBoundary, JournalReadError,
    JournalSnapshot, JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader,
    PaperAccountProjectionError, PaperAccountReadModel, PaperReservationPhase, ProjectionStatus,
    paper_account::{AuthorityLock, bounded_identity, shared_authority_lock},
};

/// Stable version of every durable account-risk fact and view.
pub const ACCOUNT_RISK_SCHEMA_VERSION: u16 = 1;
/// Hard bound for distinct risk scopes reconstructed from one journal.
pub const MAX_ACCOUNT_RISK_SCOPES: usize = 16;
/// Hard bound for concurrently open owner-level position clocks per scope.
pub const MAX_ACCOUNT_RISK_SCOPE_POSITIONS: usize = 64;

const ACCOUNT_RISK_STRATEGY: &str = "account_risk";
const ACCOUNT_RISK_ADMITTED: &str = "account_risk_admitted";
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountRiskAdmission {
    Admitted { warnings: Vec<AccountRiskWarning> },
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
    /// UTC date (YYYY-MM-DD) that `daily_trade_count` belongs to.
    pub trade_date_utc: Option<String>,
    pub daily_trade_count: u32,
    pub open_positions: Vec<AccountRiskOpenPositionView>,
    pub admitted_count: u64,
    pub rejected_count: u64,
    pub last_rejection: Option<String>,
    pub last_recorded_at: Option<DateTime<Utc>>,
}

impl AccountRiskStateView {
    fn empty(scope_id: String) -> Self {
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
            admitted_count: 0,
            rejected_count: 0,
            last_rejection: None,
            last_recorded_at: None,
        }
    }

    /// Admitted trades already counted for the supplied instant's UTC day.
    #[must_use]
    pub fn daily_trade_count_at(&self, now: DateTime<Utc>) -> u32 {
        if self.trade_date_utc.as_deref() == Some(utc_date(now).as_str()) {
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
        let mut model = Self {
            schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
            journal_id: snapshot.journal_id(),
            projection_status: ProjectionStatus::Complete,
            invalid_event_count: 0,
            scopes: Vec::new(),
        };
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            for event in page.events() {
                model.apply_event(event.payload())?;
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    model.projection_status = ProjectionStatus::Degraded;
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
        model
            .scopes
            .sort_by(|left, right| left.scope_id.cmp(&right.scope_id));
        Ok(model)
    }

    /// Returns the reconstructed state for one scope, if any fact exists.
    #[must_use]
    pub fn scope(&self, scope_id: &str) -> Option<&AccountRiskStateView> {
        self.scopes.iter().find(|scope| scope.scope_id == scope_id)
    }

    fn apply_event(&mut self, payload: &Value) -> Result<(), AccountRiskProjectionError> {
        let Some(decision) = payload.get("decision").and_then(Value::as_str) else {
            return Ok(());
        };
        if !matches!(
            decision,
            ACCOUNT_RISK_ADMITTED
                | ACCOUNT_RISK_REJECTED
                | ACCOUNT_RISK_POSITION_CLOSED
                | ACCOUNT_RISK_PAUSED
                | ACCOUNT_RISK_RESUMED
                | ACCOUNT_RISK_KILL_SWITCH
        ) {
            return Ok(());
        }
        let fact = payload
            .get("details")
            .cloned()
            .map(serde_json::from_value::<AccountRiskFact>);
        let Some(Ok(fact)) = fact else {
            self.invalid_event_count = self.invalid_event_count.saturating_add(1);
            self.projection_status = ProjectionStatus::Degraded;
            return Ok(());
        };
        if fact.schema_version() != ACCOUNT_RISK_SCHEMA_VERSION
            || fact.matches_decision(decision).is_none()
        {
            self.invalid_event_count = self.invalid_event_count.saturating_add(1);
            self.projection_status = ProjectionStatus::Degraded;
            return Ok(());
        }
        let scope = self.scope_mut(fact.scope_id())?;
        scope.last_recorded_at = Some(match scope.last_recorded_at {
            Some(previous) => previous.max(fact.recorded_at()),
            None => fact.recorded_at(),
        });
        match fact {
            AccountRiskFact::Admitted {
                task_id,
                symbol,
                utc_date,
                recorded_at,
                ..
            } => {
                if scope.trade_date_utc.as_deref() != Some(utc_date.as_str()) {
                    scope.trade_date_utc = Some(utc_date);
                    scope.daily_trade_count = 0;
                }
                scope.daily_trade_count = scope.daily_trade_count.saturating_add(1);
                scope.admitted_count = scope.admitted_count.saturating_add(1);
                if !scope
                    .open_positions
                    .iter()
                    .any(|position| position.task_id == task_id)
                {
                    if scope.open_positions.len() >= MAX_ACCOUNT_RISK_SCOPE_POSITIONS {
                        return Err(AccountRiskProjectionError::OpenPositionLimitExceeded {
                            limit: MAX_ACCOUNT_RISK_SCOPE_POSITIONS,
                        });
                    }
                    scope.open_positions.push(AccountRiskOpenPositionView {
                        task_id,
                        symbol,
                        opened_at: recorded_at,
                    });
                }
            }
            AccountRiskFact::Rejected { rejection, .. } => {
                scope.rejected_count = scope.rejected_count.saturating_add(1);
                scope.last_rejection = Some(rejection);
            }
            AccountRiskFact::PositionClosed { task_id, .. } => {
                scope
                    .open_positions
                    .retain(|position| position.task_id != task_id);
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
        Ok(Self {
            journal_id,
            history,
            scope_id,
            policy,
            authority_lock,
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
        let (state, accounts) = self.load().await?;
        let (symbol_exposure, total_exposure, total_balance) =
            exposures(&accounts, candidate.symbol())?;
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
                let labels = warnings
                    .iter()
                    .map(|warning| warning.label().to_owned())
                    .take(MAX_RISK_WARNINGS)
                    .collect();
                self.append_fact(
                    ACCOUNT_RISK_ADMITTED,
                    &AccountRiskFact::Admitted {
                        schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
                        journal_id: self.journal_id,
                        scope_id: self.scope_id.clone(),
                        task_id: candidate.task_id().to_owned(),
                        symbol: candidate.symbol().to_owned(),
                        notional: candidate.notional(),
                        utc_date: utc_date(now),
                        recorded_at: now,
                        warnings: labels,
                    },
                )
                .await?;
                Ok(AccountRiskAdmission::Admitted { warnings })
            }
            AccountRiskDecision::Rejected(rejection) => {
                self.append_fact(
                    ACCOUNT_RISK_REJECTED,
                    &AccountRiskFact::Rejected {
                        schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
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
        let (state, _) = self.load().await?;
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
                schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
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
        let (state, _) = self.load().await?;
        if state.paused && state.pause_reason.as_deref() == Some(reason.as_str()) {
            return Ok(state);
        }
        self.append_fact(
            ACCOUNT_RISK_PAUSED,
            &AccountRiskFact::Paused {
                schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
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
        let (state, _) = self.load().await?;
        if !state.paused {
            return Ok(state);
        }
        self.append_fact(
            ACCOUNT_RISK_RESUMED,
            &AccountRiskFact::Resumed {
                schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
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
        let (state, _) = self.load().await?;
        if state.kill_switch_engaged {
            return Ok(state);
        }
        self.append_fact(
            ACCOUNT_RISK_KILL_SWITCH,
            &AccountRiskFact::KillSwitchEngaged {
                schema_version: ACCOUNT_RISK_SCHEMA_VERSION,
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
        let (state, accounts) = self.load().await?;
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
    ) -> Result<(AccountRiskStateView, PaperAccountReadModel), AccountRiskError> {
        let snapshot = load_journal_snapshot(self.journal_id, self.history.path()).await?;
        let risk = AccountRiskReadModel::from_legacy_snapshot(&snapshot)?;
        let accounts = PaperAccountReadModel::from_legacy_snapshot(&snapshot)?;
        if risk.projection_status != ProjectionStatus::Complete
            || accounts.projection_status != ProjectionStatus::Complete
        {
            return Err(AccountRiskError::DegradedState);
        }
        let state = risk
            .scope(&self.scope_id)
            .cloned()
            .unwrap_or_else(|| AccountRiskStateView::empty(self.scope_id.clone()));
        Ok((state, accounts))
    }

    async fn append_fact(
        &self,
        decision: &'static str,
        fact: &AccountRiskFact,
    ) -> Result<(), AccountRiskError> {
        let details = serde_json::to_value(fact).map_err(AccountRiskError::Serialize)?;
        self.history
            .append(&DecisionRecord {
                timestamp: Utc::now(),
                strategy: ACCOUNT_RISK_STRATEGY.to_owned(),
                symbol: self.scope_id.clone(),
                decision: decision.to_owned(),
                details,
            })
            .await
            .map_err(AccountRiskError::JournalWrite)
    }
}

async fn load_journal_snapshot(
    journal_id: Uuid,
    path: &Path,
) -> Result<JournalSnapshot, AccountRiskError> {
    let source = FileJournalSnapshotSource::new(journal_id, path)?;
    let source_path: PathBuf = source.path().to_owned();
    tokio::task::spawn_blocking(move || match std::fs::metadata(&source_path) {
        Ok(_) => source.snapshot(),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            JournalSnapshot::new(journal_id, Vec::new())
        }
        Err(error) => Err(JournalReadError::Metadata(error)),
    })
    .await
    .map_err(|_| AccountRiskError::SnapshotTaskFailed)?
    .map_err(AccountRiskError::JournalRead)
}

/// Symbol, global, and total-balance observations derived conservatively from
/// non-released paper reservations: committed exposure uses the original
/// reserved leg notional, never a smaller confirmed value.
fn exposures(
    accounts: &PaperAccountReadModel,
    symbol: &str,
) -> Result<(Money, Money, Money), AccountRiskError> {
    let mut symbol_exposure = Money::default();
    let mut total_exposure = Money::default();
    let mut available = Money::default();
    for account in &accounts.accounts {
        for value in [
            account.pending_reserved,
            account.uncertain_reserved,
            account.committed_exposure,
        ] {
            total_exposure = checked_add(total_exposure, value)?;
        }
        available = checked_add(available, account.available)?;
        for reservation in &account.reservations {
            if reservation.phase == PaperReservationPhase::Released {
                continue;
            }
            for leg in &reservation.legs {
                if !symbol.is_empty() && leg.symbol().as_str().eq_ignore_ascii_case(symbol) {
                    symbol_exposure = checked_add(symbol_exposure, leg.reserved_notional())?;
                }
            }
        }
    }
    // Total balance mirrors the legacy controller's use of *total* rather
    // than available funds: available plus every non-released reservation.
    let total_balance = checked_add(available, total_exposure)?;
    Ok((symbol_exposure, total_exposure, total_balance))
}

fn checked_add(left: Money, right: Money) -> Result<Money, AccountRiskError> {
    left.as_decimal()
        .checked_add(right.as_decimal())
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
        notional: Money,
        utc_date: String,
        recorded_at: DateTime<Utc>,
        warnings: Vec<String>,
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

impl AccountRiskFact {
    const fn schema_version(&self) -> u16 {
        match self {
            Self::Admitted { schema_version, .. }
            | Self::Rejected { schema_version, .. }
            | Self::PositionClosed { schema_version, .. }
            | Self::Paused { schema_version, .. }
            | Self::Resumed { schema_version, .. }
            | Self::KillSwitchEngaged { schema_version, .. } => *schema_version,
        }
    }

    fn scope_id(&self) -> &str {
        match self {
            Self::Admitted { scope_id, .. }
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
            | Self::Rejected { recorded_at, .. }
            | Self::PositionClosed { recorded_at, .. }
            | Self::Paused { recorded_at, .. }
            | Self::Resumed { recorded_at, .. }
            | Self::KillSwitchEngaged { recorded_at, .. } => *recorded_at,
        }
    }

    fn matches_decision(&self, decision: &str) -> Option<()> {
        let expected: &str = match self {
            Self::Admitted { .. } => ACCOUNT_RISK_ADMITTED,
            Self::Rejected { .. } => ACCOUNT_RISK_REJECTED,
            Self::PositionClosed { .. } => ACCOUNT_RISK_POSITION_CLOSED,
            Self::Paused { .. } => ACCOUNT_RISK_PAUSED,
            Self::Resumed { .. } => ACCOUNT_RISK_RESUMED,
            Self::KillSwitchEngaged { .. } => ACCOUNT_RISK_KILL_SWITCH,
        };
        (expected == decision).then_some(())
    }
}

#[derive(Debug, Error)]
pub enum AccountRiskProjectionError {
    #[error(transparent)]
    Journal(#[from] JournalReadError),
    #[error("account risk projection could not advance past a page boundary")]
    NonAdvancingPage,
    #[error("account risk projection exceeds the {limit}-scope bound")]
    ScopeLimitExceeded { limit: usize },
    #[error("account risk projection exceeds the {limit}-open-position bound")]
    OpenPositionLimitExceeded { limit: usize },
}

#[derive(Debug, Error)]
pub enum AccountRiskError {
    #[error("invalid account risk configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid account risk request: {0}")]
    InvalidRequest(&'static str),
    #[error("account risk durable state is degraded; recovery is required")]
    DegradedState,
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
