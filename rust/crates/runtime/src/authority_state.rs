use std::{
    collections::HashMap,
    io,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AuthorityStateKey {
    journal_id: Uuid,
    path: PathBuf,
}

#[derive(Debug, Default)]
struct AuthorityStateCell {
    cached: Option<Arc<AuthorityProjection>>,
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

impl HistoricalReservationIndex {
    fn empty() -> Self {
        Self {
            maps: Arc::new(StdMutex::new(HistoricalReservationMaps::default())),
        }
    }

    fn apply_model_updates(
        &self,
        model: &PaperAccountReadModel,
        previous_last_sequence: u64,
    ) -> Result<(), AuthorityStateError> {
        let updates = model.accounts.iter().flat_map(|account| {
            account
                .reservations
                .iter()
                .filter(move |reservation| reservation.last_sequence > previous_last_sequence)
                .map(|reservation| (account.account_id.clone(), Arc::new(reservation.clone())))
        });
        let mut maps = self
            .maps
            .lock()
            .map_err(|_| AuthorityStateError::Degraded)?;
        for (account_id, reservation) in updates {
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
        let mut current_head = history.inspect_chain_head().await?;
        let cached = self
            .cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cached
            .clone();
        if let Some(cached) = cached.as_ref() {
            match classify_head_progression(&cached.head, &current_head) {
                HeadProgression::Same => return Ok(Arc::clone(cached)),
                HeadProgression::RegressedOrDiscontinuous => {
                    return Err(AuthorityStateError::Degraded);
                }
                HeadProgression::Forward => {
                    if let Some(delta) = history.same_process_delta_since(&cached.head)
                        && delta.head_after == current_head
                    {
                        let updated = Arc::new(apply_delta(cached.as_ref(), delta)?);
                        self.cell
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .cached = Some(updated.clone());
                        return Ok(updated);
                    }
                }
            }
        }
        history.repair_recoverable_tail().await?;
        current_head = history.inspect_chain_head().await?;
        if let Some(cached) = cached {
            match classify_head_progression(&cached.head, &current_head) {
                HeadProgression::Same => return Ok(cached),
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
        cell.cached = Some(rebuilt.clone());
        Ok(rebuilt)
    }
}

impl AuthorityProjection {
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

fn project_snapshot(
    snapshot: &JournalSnapshot,
    head: HistoryChainHead,
) -> Result<AuthorityProjection, AuthorityStateError> {
    let mut risk = account_risk::ProjectionBuilder::new(snapshot.journal_id());
    let mut paper_live = paper_account::ProjectionBuilder::new(snapshot.journal_id());
    let mut paper_all = paper_account::ProjectionBuilder::new(snapshot.journal_id())
        .retain_terminal_reservations(true);
    let mut open_admissions = Vec::new();
    let mut cursor = None;
    let mut last_sequence = 0_u64;
    loop {
        let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
        for event in page.events() {
            last_sequence = event.sequence();
            risk.observe_event(event)?;
            paper_live.observe_event(event);
            paper_all.observe_event(event);
            account_risk::apply_open_admission_event(&mut open_admissions, event.payload())
                .map_err(|_| AuthorityStateError::Degraded)?;
        }
        match page.boundary() {
            JournalPageBoundary::SnapshotEnd => break,
            JournalPageBoundary::PartialTail { .. } => {
                risk.mark_partial_tail();
                paper_live.mark_partial_tail();
                paper_all.mark_partial_tail();
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
    let paper_all = paper_all.finish()?;
    Ok(AuthorityProjection {
        head,
        last_sequence,
        historical_reservations: build_historical_reservation_index(&paper_all)?,
        paper_live,
        risk,
        open_admissions,
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
    let mut paper_live =
        paper_account::ProjectionBuilder::from_model(cached.paper_live.clone(), false);
    let mut paper_updates =
        paper_account::ProjectionBuilder::from_model(cached.paper_live.clone(), true);
    let mut open_admissions = cached.open_admissions.clone();
    let mut sequence = cached.last_sequence;
    for record in delta.records {
        sequence = sequence
            .checked_add(1)
            .ok_or(AuthorityStateError::Degraded)?;
        let event = event_from_decision_record(cached.paper_live.journal_id, sequence, &record)?;
        risk.observe_event(&event)?;
        paper_live.observe_event(&event);
        paper_updates.observe_event(&event);
        account_risk::apply_open_admission_event(&mut open_admissions, event.payload())
            .map_err(|_| AuthorityStateError::Degraded)?;
    }
    let risk = risk.finish();
    let paper_live = paper_live.finish()?;
    let paper_updates = paper_updates.finish()?;
    cached
        .historical_reservations
        .apply_model_updates(&paper_updates, cached.last_sequence)?;
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

fn build_historical_reservation_index(
    model: &PaperAccountReadModel,
) -> Result<HistoricalReservationIndex, AuthorityStateError> {
    let index = HistoricalReservationIndex::empty();
    index.apply_model_updates(model, 0)?;
    Ok(index)
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
