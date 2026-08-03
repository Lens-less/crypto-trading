use std::{
    collections::HashMap,
    io,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::Money;
use uuid::Uuid;

use crate::account_risk::OpenAdmission;
use crate::history::{HistoryChainHead, HistoryDelta, HistoryError};
use crate::journal_reader::event_from_decision_record;
use crate::{
    AccountRiskProjectionError, AccountRiskReadModel, AccountRiskStateView,
    FileJournalSnapshotSource, JournalPageBoundary, JournalReadError, JournalSnapshot,
    JournalSnapshotSource, JsonlHistory, LegacyJsonlJournalReader, PaperAccountProjectionError,
    PaperAccountReadModel, PaperAccountSnapshot, PaperReservationRequest, PaperReservationView,
    ProjectionStatus, account_risk, paper_account,
};

const MAX_AUTHORITY_SNAPSHOT_ATTEMPTS: usize = 3;
/// A full durability cross-check follows every 64 cache-backed refreshes.
/// Cold replay already reads and projects the complete frozen journal, so it
/// resets this counter without immediately repeating the same work.
const AUTHORITY_DURABILITY_VERIFY_REFRESH_INTERVAL: u32 = 64;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AuthorityStateKey {
    journal_id: Uuid,
    path: PathBuf,
}

#[derive(Debug, Default)]
struct AuthorityStateCell {
    cached: Option<Arc<AuthorityProjection>>,
    durability_degraded: bool,
    cache_refreshes_since_verification: u32,
}

static AUTHORITY_STATE_CELLS: OnceLock<
    StdMutex<HashMap<AuthorityStateKey, Weak<StdMutex<AuthorityStateCell>>>>,
> = OnceLock::new();

#[derive(Clone, Debug)]
pub(crate) struct AuthorityStateCache {
    key: AuthorityStateKey,
    cell: Arc<StdMutex<AuthorityStateCell>>,
}

#[derive(Clone, Debug)]
pub(crate) struct HistoricalReservationIndex {
    // Paper and risk authorities serialize every cache access on the shared
    // journal lock. Sharing this append-only identity table therefore keeps a
    // delta O(active state + changed reservations) without exposing a mutable
    // public snapshot or cloning all terminal history.
    maps: Arc<StdMutex<HistoricalReservationMaps>>,
}

#[derive(Debug, Default)]
struct HistoricalReservationMaps {
    by_task_key: HashMap<(String, String, String), Arc<PaperReservationView>>,
    reservation_ids: HashMap<(String, Uuid), Arc<PaperReservationView>>,
    batch_ids: HashMap<(String, Uuid), Arc<PaperReservationView>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalAuthorityProjection {
    head: HistoryChainHead,
    last_sequence: u64,
    paper_live: PaperAccountReadModel,
    risk: AccountRiskReadModel,
    open_admissions: Vec<CanonicalOpenAdmission>,
    historical_reservations: CanonicalHistoricalReservationIndex,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalOpenAdmission {
    scope_id: String,
    task_id: String,
    symbol: String,
    ticket_id: Option<String>,
    recorded_at: DateTime<Utc>,
    lease_expires_at: DateTime<Utc>,
    notional: Money,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalHistoricalReservationIndex {
    by_task_key: Vec<((String, String, String), PaperReservationView)>,
    reservation_ids: Vec<((String, Uuid), PaperReservationView)>,
    batch_ids: Vec<((String, Uuid), PaperReservationView)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableVerification {
    Match,
    Mismatch,
    Superseded,
}

struct ProjectionRefresh {
    projection: Arc<AuthorityProjection>,
    cold_replayed: bool,
}

impl HistoricalReservationIndex {
    fn empty() -> Self {
        Self {
            maps: Arc::new(StdMutex::new(HistoricalReservationMaps::default())),
        }
    }

    fn apply_terminal_updates(
        &self,
        updates: &[paper_account::TerminalReservationUpdate],
    ) -> Result<(), AuthorityStateError> {
        let mut maps = self
            .maps
            .lock()
            .map_err(|_| AuthorityStateError::Degraded)?;
        for update in updates {
            let account_id = update.account_id.clone();
            let reservation = Arc::new(update.reservation.clone());
            maps.by_task_key.insert(
                (
                    account_id.clone(),
                    reservation.task_id.clone(),
                    reservation.idempotency_key.clone(),
                ),
                Arc::clone(&reservation),
            );
            maps.reservation_ids.insert(
                (account_id.clone(), reservation.reservation_id),
                Arc::clone(&reservation),
            );
            maps.batch_ids
                .insert((account_id, reservation.batch_id), reservation);
        }
        Ok(())
    }

    fn canonical(&self) -> Result<CanonicalHistoricalReservationIndex, AuthorityStateError> {
        let maps = self
            .maps
            .lock()
            .map_err(|_| AuthorityStateError::Degraded)?;
        let mut by_task_key = maps
            .by_task_key
            .iter()
            .map(|(key, reservation)| (key.clone(), reservation.as_ref().clone()))
            .collect::<Vec<_>>();
        by_task_key.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut reservation_ids = maps
            .reservation_ids
            .iter()
            .map(|(key, reservation)| (key.clone(), reservation.as_ref().clone()))
            .collect::<Vec<_>>();
        reservation_ids.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut batch_ids = maps
            .batch_ids
            .iter()
            .map(|(key, reservation)| (key.clone(), reservation.as_ref().clone()))
            .collect::<Vec<_>>();
        batch_ids.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(CanonicalHistoricalReservationIndex {
            by_task_key,
            reservation_ids,
            batch_ids,
        })
    }

    #[cfg(test)]
    fn storage_identity(&self) -> *const () {
        Arc::as_ptr(&self.maps).cast()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AuthorityProjection {
    pub(crate) head: HistoryChainHead,
    pub(crate) last_sequence: u64,
    pub(crate) paper_live: PaperAccountReadModel,
    pub(crate) risk: AccountRiskReadModel,
    open_admissions: Vec<OpenAdmission>,
    historical_reservations: HistoricalReservationIndex,
}

#[derive(Debug)]
pub(crate) enum AuthorityStateError {
    History,
    Journal(JournalReadError),
    Paper(PaperAccountProjectionError),
    Risk(AccountRiskProjectionError),
    Degraded,
}

impl From<HistoryError> for AuthorityStateError {
    fn from(_: HistoryError) -> Self {
        Self::History
    }
}

impl From<JournalReadError> for AuthorityStateError {
    fn from(value: JournalReadError) -> Self {
        Self::Journal(value)
    }
}

impl From<PaperAccountProjectionError> for AuthorityStateError {
    fn from(value: PaperAccountProjectionError) -> Self {
        Self::Paper(value)
    }
}

impl From<AccountRiskProjectionError> for AuthorityStateError {
    fn from(value: AccountRiskProjectionError) -> Self {
        Self::Risk(value)
    }
}

impl From<crate::AccountRiskError> for AuthorityStateError {
    fn from(_: crate::AccountRiskError) -> Self {
        Self::Degraded
    }
}

impl AuthorityStateCache {
    pub(crate) fn new(journal_id: Uuid, history: &JsonlHistory) -> Self {
        let key = AuthorityStateKey {
            journal_id,
            path: crate::history::normalized_lock_key(history.path()),
        };
        let registry = AUTHORITY_STATE_CELLS.get_or_init(|| StdMutex::new(HashMap::new()));
        let mut registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|_, cell| cell.strong_count() > 0);
        let cell = registry
            .get(&key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let cell = Arc::new(StdMutex::new(AuthorityStateCell::default()));
                registry.insert(key.clone(), Arc::downgrade(&cell));
                cell
            });
        Self { key, cell }
    }

    pub(crate) async fn refresh(
        &self,
        history: &JsonlHistory,
    ) -> Result<Arc<AuthorityProjection>, AuthorityStateError> {
        let refreshed = self.refresh_projection(history).await?;
        if self.should_verify_after_refresh(&refreshed)? {
            let verification = verify_projection_against_durable_history(
                self.key.journal_id,
                history,
                Arc::clone(&refreshed.projection),
            )
            .await;
            match self.require_verification_result(&verification)? {
                DurableVerification::Match => {
                    self.mark_durability_verified(&refreshed.projection)?;
                }
                DurableVerification::Mismatch => {
                    self.latch_durability_degraded();
                    return Err(AuthorityStateError::Degraded);
                }
                DurableVerification::Superseded => {}
            }
        }
        Ok(refreshed.projection)
    }

    async fn refresh_projection(
        &self,
        history: &JsonlHistory,
    ) -> Result<ProjectionRefresh, AuthorityStateError> {
        let cached = {
            let cell = self
                .cell
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cell.durability_degraded {
                return Err(AuthorityStateError::Degraded);
            }
            cell.cached.clone()
        };
        let mut current_head = history.inspect_chain_head().await?;
        if let Some(cached) = cached.as_ref() {
            match classify_head_progression(&cached.head, &current_head) {
                HeadProgression::Same => {
                    return Ok(ProjectionRefresh {
                        projection: Arc::clone(cached),
                        cold_replayed: false,
                    });
                }
                HeadProgression::RegressedOrDiscontinuous => {
                    return Err(AuthorityStateError::Degraded);
                }
                HeadProgression::Forward => {
                    if let Some(delta) = history.same_process_delta_since(&cached.head)
                        && delta.head_after == current_head
                    {
                        let updated = Arc::new(apply_delta(cached.as_ref(), delta)?);
                        let mut cell = self
                            .cell
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if cell.durability_degraded {
                            return Err(AuthorityStateError::Degraded);
                        }
                        cell.cached = Some(updated.clone());
                        return Ok(ProjectionRefresh {
                            projection: updated,
                            cold_replayed: false,
                        });
                    }
                }
            }
        }
        history.repair_recoverable_tail().await?;
        current_head = history.inspect_chain_head().await?;
        if let Some(cached) = cached {
            match classify_head_progression(&cached.head, &current_head) {
                HeadProgression::Same => {
                    return Ok(ProjectionRefresh {
                        projection: cached,
                        cold_replayed: false,
                    });
                }
                HeadProgression::Forward => {}
                HeadProgression::RegressedOrDiscontinuous => {
                    return Err(AuthorityStateError::Degraded);
                }
            }
        }
        let rebuilt = Arc::new(cold_replay(self.key.journal_id, history, current_head).await?);
        let mut cell = self
            .cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cell.durability_degraded {
            return Err(AuthorityStateError::Degraded);
        }
        cell.cached = Some(rebuilt.clone());
        Ok(ProjectionRefresh {
            projection: rebuilt,
            cold_replayed: true,
        })
    }

    pub(crate) async fn verify_durable_state(
        &self,
        history: &JsonlHistory,
    ) -> Result<(), AuthorityStateError> {
        for _ in 0..MAX_AUTHORITY_SNAPSHOT_ATTEMPTS {
            let refreshed = self.refresh_projection(history).await?;
            let verification = verify_projection_against_durable_history(
                self.key.journal_id,
                history,
                Arc::clone(&refreshed.projection),
            )
            .await;
            match self.require_verification_result(&verification)? {
                DurableVerification::Match => {
                    self.mark_durability_verified(&refreshed.projection)?;
                    return Ok(());
                }
                DurableVerification::Mismatch => {
                    self.latch_durability_degraded();
                    return Err(AuthorityStateError::Degraded);
                }
                DurableVerification::Superseded => {}
            }
        }
        Err(AuthorityStateError::Degraded)
    }

    fn require_verification_result(
        &self,
        verification: &Result<DurableVerification, AuthorityStateError>,
    ) -> Result<DurableVerification, AuthorityStateError> {
        if let Ok(result) = verification {
            return Ok(*result);
        }
        // Once a frozen head cannot be proven equivalent to the process-local
        // projection, returning the old cache would put memory ahead of
        // durable truth. Treat every verification failure as an integrity
        // failure and require operator intervention, even if the bytes are
        // later restored.
        self.latch_durability_degraded();
        Err(AuthorityStateError::Degraded)
    }

    fn should_verify_after_refresh(
        &self,
        refreshed: &ProjectionRefresh,
    ) -> Result<bool, AuthorityStateError> {
        let mut cell = self
            .cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cell.durability_degraded {
            return Err(AuthorityStateError::Degraded);
        }
        if refreshed.cold_replayed {
            cell.cache_refreshes_since_verification = 0;
            return Ok(false);
        }
        cell.cache_refreshes_since_verification =
            cell.cache_refreshes_since_verification.saturating_add(1);
        Ok(cell.cache_refreshes_since_verification >= AUTHORITY_DURABILITY_VERIFY_REFRESH_INTERVAL)
    }

    fn mark_durability_verified(
        &self,
        verified: &AuthorityProjection,
    ) -> Result<(), AuthorityStateError> {
        let mut cell = self
            .cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cell.durability_degraded {
            return Err(AuthorityStateError::Degraded);
        }
        if cell
            .cached
            .as_ref()
            .is_some_and(|cached| cached.head == verified.head)
        {
            cell.cache_refreshes_since_verification = 0;
        }
        Ok(())
    }

    fn latch_durability_degraded(&self) {
        self.cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .durability_degraded = true;
    }
}

impl AuthorityProjection {
    fn canonical(&self) -> Result<CanonicalAuthorityProjection, AuthorityStateError> {
        let mut open_admissions = self
            .open_admissions
            .iter()
            .map(|admission| CanonicalOpenAdmission {
                scope_id: admission.scope_id.clone(),
                task_id: admission.task_id.clone(),
                symbol: admission.symbol.clone(),
                ticket_id: admission.ticket_id.clone(),
                recorded_at: admission.recorded_at,
                lease_expires_at: admission.lease_expires_at,
                notional: admission.notional,
            })
            .collect::<Vec<_>>();
        open_admissions.sort();
        Ok(CanonicalAuthorityProjection {
            head: self.head.clone(),
            last_sequence: self.last_sequence,
            paper_live: self.paper_live.clone(),
            risk: self.risk.clone(),
            open_admissions,
            historical_reservations: self.historical_reservations.canonical()?,
        })
    }

    pub(crate) fn paper_snapshot(
        &self,
        account_id: &str,
        initial_available: Money,
    ) -> Result<PaperAccountSnapshot, AuthorityStateError> {
        let model = &self.paper_live;
        if let Some(account) = model
            .accounts
            .iter()
            .find(|account| account.account_id == account_id)
        {
            if account.initial_available != initial_available {
                return Err(AuthorityStateError::Degraded);
            }
            return Ok(account.clone());
        }
        Ok(PaperAccountSnapshot {
            schema_version: crate::PAPER_ACCOUNT_SCHEMA_VERSION,
            journal_id: self.paper_live.journal_id,
            projection_status: model.projection_status,
            invalid_event_count: model.invalid_event_count,
            account_id: account_id.to_owned(),
            initial_available,
            available: initial_available,
            pending_reserved: Money::default(),
            uncertain_reserved: Money::default(),
            committed_exposure: Money::default(),
            ledger_kind: crate::PaperExecutionLedgerKind::LegacyReservationOnly,
            cumulative_fees: Money::default(),
            realized_pnl: Money::default(),
            settled_equity_base: initial_available,
            open_lots: Vec::new(),
            reservations: Vec::new(),
        })
    }

    pub(crate) fn risk_state(
        &self,
        scope_id: &str,
    ) -> Result<AccountRiskStateView, AuthorityStateError> {
        if self.risk.projection_status != ProjectionStatus::Complete
            || self.paper_live.projection_status != ProjectionStatus::Complete
        {
            return Err(AuthorityStateError::Degraded);
        }
        Ok(self
            .risk
            .scope(scope_id)
            .cloned()
            .unwrap_or_else(|| crate::AccountRiskStateView::empty(scope_id.to_owned())))
    }

    pub(crate) fn open_admissions_for_scope(&self, scope_id: &str) -> Vec<OpenAdmission> {
        self.open_admissions
            .iter()
            .filter(|admission| admission.scope_id == scope_id)
            .cloned()
            .collect()
    }

    pub(crate) fn bound_open_admission(
        &self,
        scope_id: &str,
        ticket_id: &str,
    ) -> Result<Option<OpenAdmission>, AuthorityStateError> {
        if self.risk.projection_status != ProjectionStatus::Complete {
            return Err(AuthorityStateError::Degraded);
        }
        Ok(self
            .open_admissions
            .iter()
            .find(|admission| {
                admission.scope_id == scope_id && admission.ticket_id.as_deref() == Some(ticket_id)
            })
            .cloned())
    }

    pub(crate) fn historical_reservation(
        &self,
        account_id: &str,
        request: &PaperReservationRequest,
    ) -> Result<Option<PaperReservationView>, crate::PaperAccountError> {
        let maps = self
            .historical_reservations
            .maps
            .lock()
            .map_err(|_| crate::PaperAccountError::DurableStateDegraded)?;
        if let Some(existing) = maps.by_task_key.get(&(
            account_id.to_owned(),
            request.task_id().to_owned(),
            request.idempotency_key().to_owned(),
        )) {
            return if existing.matches(request) {
                Ok(Some(existing.as_ref().clone()))
            } else {
                Err(crate::PaperAccountError::IdempotencyConflict)
            };
        }
        if maps
            .reservation_ids
            .contains_key(&(account_id.to_owned(), request.reservation_id()))
            || maps
                .batch_ids
                .contains_key(&(account_id.to_owned(), request.batch_id()))
        {
            return Err(crate::PaperAccountError::ReservationIdentityConflict);
        }
        Ok(None)
    }

    pub(crate) fn historical_reservation_by_id(
        &self,
        account_id: &str,
        reservation_id: Uuid,
    ) -> Result<Option<PaperReservationView>, crate::PaperAccountError> {
        let maps = self
            .historical_reservations
            .maps
            .lock()
            .map_err(|_| crate::PaperAccountError::DurableStateDegraded)?;
        Ok(maps
            .reservation_ids
            .get(&(account_id.to_owned(), reservation_id))
            .map(|reservation| reservation.as_ref().clone()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadProgression {
    Same,
    Forward,
    RegressedOrDiscontinuous,
}

fn classify_head_progression(
    previous: &HistoryChainHead,
    current: &HistoryChainHead,
) -> HeadProgression {
    if previous == current {
        return HeadProgression::Same;
    }
    let previous_segments = &previous.sealed_segment_bytes;
    let current_segments = &current.sealed_segment_bytes;
    if current_segments.len() < previous_segments.len()
        || !current_segments.starts_with(previous_segments)
    {
        return HeadProgression::RegressedOrDiscontinuous;
    }
    if current_segments.len() == previous_segments.len() {
        return if current.active_bytes >= previous.active_bytes {
            HeadProgression::Forward
        } else {
            HeadProgression::RegressedOrDiscontinuous
        };
    }
    if current_segments[previous_segments.len()] != previous.active_bytes {
        return HeadProgression::RegressedOrDiscontinuous;
    }
    if current_segments
        .iter()
        .skip(previous_segments.len() + 1)
        .any(|bytes| *bytes != crate::MAX_HISTORY_FILE_BYTES)
    {
        return HeadProgression::RegressedOrDiscontinuous;
    }
    HeadProgression::Forward
}

async fn cold_replay(
    journal_id: Uuid,
    history: &JsonlHistory,
    mut expected_head: HistoryChainHead,
) -> Result<AuthorityProjection, AuthorityStateError> {
    for _ in 0..MAX_AUTHORITY_SNAPSHOT_ATTEMPTS {
        let snapshot = load_journal_snapshot(journal_id, history.path()).await?;
        let observed_head = history.inspect_chain_head().await?;
        let snapshot_bytes = u64::try_from(snapshot.len()).unwrap_or(u64::MAX);
        if observed_head == expected_head
            && history_head_bytes(&observed_head) == Some(snapshot_bytes)
        {
            return project_snapshot(&snapshot, observed_head);
        }
        expected_head = observed_head;
    }
    Err(AuthorityStateError::Degraded)
}

async fn load_journal_snapshot(
    journal_id: Uuid,
    path: &Path,
) -> Result<JournalSnapshot, JournalReadError> {
    let source = FileJournalSnapshotSource::new(journal_id, path)?;
    tokio::task::spawn_blocking(move || match source.snapshot() {
        Err(JournalReadError::Open(error)) if error.kind() == ErrorKind::NotFound => {
            JournalSnapshot::new(journal_id, Vec::new())
        }
        result => result,
    })
    .await
    .map_err(|_| JournalReadError::Read(io::Error::other("authority snapshot task failed")))?
}

async fn verify_projection_against_durable_history(
    journal_id: Uuid,
    history: &JsonlHistory,
    cached: Arc<AuthorityProjection>,
) -> Result<DurableVerification, AuthorityStateError> {
    let frozen_head = cached.head.clone();
    if history.inspect_chain_head().await? != frozen_head {
        return Ok(DurableVerification::Superseded);
    }
    let snapshot = load_journal_snapshot(journal_id, history.path()).await?;
    let observed_head = history.inspect_chain_head().await?;
    let snapshot_bytes = u64::try_from(snapshot.len()).unwrap_or(u64::MAX);
    if observed_head != frozen_head || history_head_bytes(&observed_head) != Some(snapshot_bytes) {
        return Ok(DurableVerification::Superseded);
    }

    let replay_head = frozen_head.clone();
    let replayed = tokio::task::spawn_blocking(move || project_snapshot(&snapshot, replay_head))
        .await
        .map_err(|_| authority_verification_task_failed("projection"))??;
    let projections_match = tokio::task::spawn_blocking(move || {
        Ok::<_, AuthorityStateError>(cached.canonical()? == replayed.canonical()?)
    })
    .await
    .map_err(|_| authority_verification_task_failed("comparison"))??;

    if history.inspect_chain_head().await? != frozen_head {
        return Ok(DurableVerification::Superseded);
    }
    if projections_match {
        Ok(DurableVerification::Match)
    } else {
        Ok(DurableVerification::Mismatch)
    }
}

fn authority_verification_task_failed(stage: &str) -> AuthorityStateError {
    AuthorityStateError::Journal(JournalReadError::Read(io::Error::other(format!(
        "authority durability verification {stage} task failed"
    ))))
}

fn project_snapshot(
    snapshot: &JournalSnapshot,
    head: HistoryChainHead,
) -> Result<AuthorityProjection, AuthorityStateError> {
    let mut risk = account_risk::ProjectionBuilder::new(snapshot.journal_id());
    let mut paper_live = paper_account::ProjectionBuilder::new(snapshot.journal_id());
    let historical_reservations = HistoricalReservationIndex::empty();
    let mut open_admissions = Vec::new();
    let mut cursor = None;
    let mut last_sequence = 0_u64;
    loop {
        let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
        let mut terminal_updates = Vec::new();
        for event in page.events() {
            last_sequence = event.sequence();
            risk.observe_event(event)?;
            terminal_updates.extend(paper_live.observe_event_with_terminal_updates(event));
            account_risk::apply_open_admission_event(&mut open_admissions, event)
                .map_err(|_| AuthorityStateError::Degraded)?;
        }
        historical_reservations.apply_terminal_updates(&terminal_updates)?;
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => break,
            JournalPageBoundary::PartialTail { .. } => {
                risk.mark_partial_tail();
                paper_live.mark_partial_tail();
                break;
            }
            JournalPageBoundary::PageLimit => {
                let next = page.next_cursor().cloned();
                if next == cursor {
                    return Err(AuthorityStateError::Paper(
                        PaperAccountProjectionError::NonAdvancingPage,
                    ));
                }
                cursor = next;
            }
        }
    }
    let risk = risk.finish();
    let paper_live = paper_live.finish()?;
    Ok(AuthorityProjection {
        head,
        last_sequence,
        paper_live,
        risk,
        open_admissions,
        historical_reservations,
    })
}

fn apply_delta(
    cached: &AuthorityProjection,
    delta: HistoryDelta,
) -> Result<AuthorityProjection, AuthorityStateError> {
    if delta.head_before != cached.head
        || classify_head_progression(&delta.head_before, &delta.head_after)
            != HeadProgression::Forward
    {
        return Err(AuthorityStateError::Degraded);
    }
    let expected_delta_bytes = history_head_bytes(&delta.head_after)
        .and_then(|after| {
            history_head_bytes(&delta.head_before).and_then(|before| after.checked_sub(before))
        })
        .ok_or(AuthorityStateError::Degraded)?;
    let actual_delta_bytes = delta.records.iter().try_fold(0_u64, |total, record| {
        let bytes = serde_json::to_vec(record)
            .map_err(|_| AuthorityStateError::Degraded)?
            .len()
            .checked_add(1)
            .ok_or(AuthorityStateError::Degraded)?;
        total
            .checked_add(u64::try_from(bytes).unwrap_or(u64::MAX))
            .ok_or(AuthorityStateError::Degraded)
    })?;
    if actual_delta_bytes != expected_delta_bytes {
        return Err(AuthorityStateError::Degraded);
    }

    let mut risk = account_risk::ProjectionBuilder::from_model(cached.risk.clone());
    let mut paper_live = paper_account::ProjectionBuilder::from_model(cached.paper_live.clone());
    let mut terminal_updates = Vec::new();
    let mut open_admissions = cached.open_admissions.clone();
    let mut sequence = cached.last_sequence;
    for record in delta.records {
        sequence = sequence
            .checked_add(1)
            .ok_or(AuthorityStateError::Degraded)?;
        let event = event_from_decision_record(cached.paper_live.journal_id, sequence, &record)?;
        risk.observe_event(&event)?;
        terminal_updates.extend(paper_live.observe_event_with_terminal_updates(&event));
        account_risk::apply_open_admission_event(&mut open_admissions, &event)
            .map_err(|_| AuthorityStateError::Degraded)?;
    }
    let risk = risk.finish();
    let paper_live = paper_live.finish()?;
    cached
        .historical_reservations
        .apply_terminal_updates(&terminal_updates)?;
    Ok(AuthorityProjection {
        head: delta.head_after,
        last_sequence: sequence,
        paper_live,
        risk,
        open_admissions,
        historical_reservations: cached.historical_reservations.clone(),
    })
}

fn history_head_bytes(head: &HistoryChainHead) -> Option<u64> {
    head.sealed_segment_bytes
        .iter()
        .try_fold(head.active_bytes, |total, bytes| total.checked_add(*bytes))
}

#[cfg(test)]
mod tests {
    use super::HistoricalReservationIndex;

    #[test]
    fn historical_index_clones_share_the_cold_history_storage() {
        let index = HistoricalReservationIndex::empty();
        let cloned = index.clone();

        assert_eq!(index.storage_identity(), cloned.storage_identity());
    }
}
