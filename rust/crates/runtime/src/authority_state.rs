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
    replay_count: usize,
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
    by_task_key: HashMap<(String, String, String), PaperReservationView>,
    reservation_ids: HashMap<(String, Uuid), PaperReservationView>,
    batch_ids: HashMap<(String, Uuid), PaperReservationView>,
}

#[derive(Clone, Debug)]
pub(crate) struct AuthorityProjection {
    pub(crate) head: HistoryChainHead,
    pub(crate) last_sequence: u64,
    pub(crate) paper_live: PaperAccountReadModel,
    pub(crate) paper_all: PaperAccountReadModel,
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
        cell.replay_count = cell.replay_count.saturating_add(1);
        cell.cached = Some(rebuilt.clone());
        Ok(rebuilt)
    }

    #[cfg(test)]
    pub(crate) fn replay_count(&self) -> usize {
        self.cell
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replay_count
    }
}

impl AuthorityProjection {
    pub(crate) fn paper_snapshot(
        &self,
        account_id: &str,
        initial_available: Money,
        retain_terminal_reservations: bool,
    ) -> Result<PaperAccountSnapshot, AuthorityStateError> {
        let model = if retain_terminal_reservations {
            &self.paper_all
        } else {
            &self.paper_live
        };
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
        if let Some(existing) = self.historical_reservations.by_task_key.get(&(
            account_id.to_owned(),
            request.task_id().to_owned(),
            request.idempotency_key().to_owned(),
        )) {
            return if existing.matches(request) {
                Ok(Some(existing.clone()))
            } else {
                Err(crate::PaperAccountError::IdempotencyConflict)
            };
        }
        if self
            .historical_reservations
            .reservation_ids
            .contains_key(&(account_id.to_owned(), request.reservation_id()))
            || self
                .historical_reservations
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
    ) -> Option<PaperReservationView> {
        self.historical_reservations
            .reservation_ids
            .get(&(account_id.to_owned(), reservation_id))
            .cloned()
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
    finish_projection(
        head,
        last_sequence,
        Ok(risk.finish()),
        paper_live.finish(),
        paper_all.finish(),
        open_admissions,
    )
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
    let mut paper_all =
        paper_account::ProjectionBuilder::from_model(cached.paper_all.clone(), true);
    let mut open_admissions = cached.open_admissions.clone();
    let mut sequence = cached.last_sequence;
    for record in delta.records {
        sequence = sequence
            .checked_add(1)
            .ok_or(AuthorityStateError::Degraded)?;
        let event = event_from_decision_record(cached.paper_live.journal_id, sequence, &record)?;
        risk.observe_event(&event)?;
        paper_live.observe_event(&event);
        paper_all.observe_event(&event);
        account_risk::apply_open_admission_event(&mut open_admissions, event.payload())
            .map_err(|_| AuthorityStateError::Degraded)?;
    }
    finish_projection(
        delta.head_after,
        sequence,
        Ok(risk.finish()),
        paper_live.finish(),
        paper_all.finish(),
        open_admissions,
    )
}

fn history_head_bytes(head: &HistoryChainHead) -> Option<u64> {
    head.sealed_segment_bytes
        .iter()
        .try_fold(head.active_bytes, |total, bytes| total.checked_add(*bytes))
}

fn finish_projection(
    head: HistoryChainHead,
    last_sequence: u64,
    risk: Result<AccountRiskReadModel, AccountRiskProjectionError>,
    paper_live: Result<PaperAccountReadModel, PaperAccountProjectionError>,
    paper_all: Result<PaperAccountReadModel, PaperAccountProjectionError>,
    open_admissions: Vec<OpenAdmission>,
) -> Result<AuthorityProjection, AuthorityStateError> {
    let risk = risk?;
    let paper_live = paper_live?;
    let paper_all = paper_all?;
    Ok(AuthorityProjection {
        head,
        last_sequence,
        historical_reservations: build_historical_reservation_index(&paper_all),
        paper_live,
        paper_all,
        risk,
        open_admissions,
    })
}

fn build_historical_reservation_index(model: &PaperAccountReadModel) -> HistoricalReservationIndex {
    let mut by_task_key = HashMap::new();
    let mut reservation_ids = HashMap::new();
    let mut batch_ids = HashMap::new();
    for account in &model.accounts {
        for reservation in &account.reservations {
            by_task_key.insert(
                (
                    account.account_id.clone(),
                    reservation.task_id.clone(),
                    reservation.idempotency_key.clone(),
                ),
                reservation.clone(),
            );
            reservation_ids.insert(
                (account.account_id.clone(), reservation.reservation_id),
                reservation.clone(),
            );
            batch_ids.insert(
                (account.account_id.clone(), reservation.batch_id),
                reservation.clone(),
            );
        }
    }
    HistoricalReservationIndex {
        by_task_key,
        reservation_ids,
        batch_ids,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AccountRiskAdmission, AccountRiskAuthority, AccountRiskCandidate, JsonlHistory,
        PaperAccountAuthority, PaperAccountConfig, PaperCostModel, PaperReservationAdmission,
        PaperReservationLeg, PaperReservationRequest,
    };
    use chrono::{TimeZone, Utc};
    use crypto_trading_domain::{MarketType, Quantity, Side, Symbol};
    use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy};
    use rust_decimal::Decimal;

    fn money(value: &str) -> Money {
        Money::new(Decimal::from_str_exact(value).unwrap())
    }

    fn temp_case(label: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("authority-state-{label}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("decisions.jsonl");
        (root, path)
    }

    fn paper_authority(journal_id: Uuid, path: &Path) -> PaperAccountAuthority {
        PaperAccountAuthority::new(
            journal_id,
            JsonlHistory::new(path),
            PaperAccountConfig::new("paper-main", money("1000")).unwrap(),
        )
        .unwrap()
    }

    fn risk_authority(journal_id: Uuid, path: &Path) -> AccountRiskAuthority {
        AccountRiskAuthority::new(
            journal_id,
            JsonlHistory::new(path),
            "paper",
            AccountRiskPolicy::new(AccountRiskLimits::default()).unwrap(),
        )
        .unwrap()
    }

    fn request(
        task_id: &str,
        idempotency: &str,
        reservation_id: Uuid,
        batch_id: Uuid,
    ) -> PaperReservationRequest {
        let intent = crypto_trading_domain::OrderIntent::market(
            "paper-grid",
            Symbol::new("BTC-USDT").unwrap(),
            MarketType::Spot,
            Side::Buy,
            Quantity::new(Decimal::ONE).unwrap(),
        );
        PaperReservationRequest::new(
            reservation_id,
            task_id,
            idempotency,
            batch_id,
            PaperCostModel::v1(10, 0, 0).unwrap(),
            vec![PaperReservationLeg::from_intent(0, &intent, money("10")).unwrap()],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn many_mutations_share_one_cold_replay_and_incremental_refresh() {
        let (root, path) = temp_case("incremental");
        let journal_id = Uuid::new_v4();
        let authority = paper_authority(journal_id, &path);
        let cache = AuthorityStateCache::new(journal_id, &JsonlHistory::new(&path));

        assert_eq!(authority.snapshot().await.unwrap().reservations.len(), 0);
        assert_eq!(cache.replay_count(), 1);

        for index in 0..8_u128 {
            let reserved = authority
                .reserve(request(
                    &format!("task/{index}"),
                    &format!("idem/{index}"),
                    Uuid::from_u128(index + 1),
                    Uuid::from_u128(index + 101),
                ))
                .await
                .unwrap();
            let reservation = match reserved {
                PaperReservationAdmission::Reserved(reservation) => reservation,
                PaperReservationAdmission::Existing(_) => panic!("expected fresh reservation"),
            };
            authority
                .release(reservation.reservation_id, "cycle_done")
                .await
                .unwrap();
        }

        assert_eq!(cache.replay_count(), 1);
        assert!(authority.snapshot().await.unwrap().reservations.is_empty());
        drop(cache);
        drop(authority);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn same_process_other_handle_append_is_observed_incrementally() {
        let (root, path) = temp_case("other-handle");
        let journal_id = Uuid::new_v4();
        let first = paper_authority(journal_id, &path);
        let second = paper_authority(journal_id, &path);
        let cache = AuthorityStateCache::new(journal_id, &JsonlHistory::new(&path));

        first.snapshot().await.unwrap();
        let admission = second
            .reserve(request(
                "task/a",
                "idem/a",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
            ))
            .await
            .unwrap();
        match admission {
            PaperReservationAdmission::Reserved(_) => {}
            PaperReservationAdmission::Existing(_) => panic!("expected fresh reservation"),
        }

        let snapshot = first.snapshot().await.unwrap();
        assert_eq!(snapshot.reservations.len(), 1);
        assert_eq!(cache.replay_count(), 1);
        drop(cache);
        drop(first);
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restart_recovers_by_cold_replaying_again() {
        let (root, path) = temp_case("restart");
        let journal_id = Uuid::new_v4();
        {
            let authority = paper_authority(journal_id, &path);
            authority.snapshot().await.unwrap();
            authority
                .reserve(request(
                    "task/a",
                    "idem/a",
                    Uuid::from_u128(1),
                    Uuid::from_u128(2),
                ))
                .await
                .unwrap();
            let cache = AuthorityStateCache::new(journal_id, &JsonlHistory::new(&path));
            assert_eq!(cache.replay_count(), 1);
        }

        let restarted = paper_authority(journal_id, &path);
        let cache = AuthorityStateCache::new(journal_id, &JsonlHistory::new(&path));
        let snapshot = restarted.snapshot().await.unwrap();
        assert_eq!(snapshot.reservations.len(), 1);
        assert_eq!(cache.replay_count(), 1);
        drop(cache);
        drop(restarted);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn tampered_head_regression_fails_closed() {
        let (root, path) = temp_case("tamper");
        let journal_id = Uuid::new_v4();
        let authority = paper_authority(journal_id, &path);
        authority
            .reserve(request(
                "task/a",
                "idem/a",
                Uuid::from_u128(1),
                Uuid::from_u128(2),
            ))
            .await
            .unwrap();
        authority.snapshot().await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();

        let error = authority.snapshot().await.unwrap_err();
        assert!(matches!(
            error,
            crate::PaperAccountError::DurableStateDegraded
        ));
        drop(authority);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn risk_incremental_state_matches_cold_replay() {
        let (root, path) = temp_case("risk");
        let journal_id = Uuid::new_v4();
        let first = risk_authority(journal_id, &path);
        let second = risk_authority(journal_id, &path);
        let now = Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap();
        let admission = second
            .admit(
                &AccountRiskCandidate::new("owner/a", "BTC-USDT", money("10")).unwrap(),
                now,
            )
            .await
            .unwrap();
        assert!(matches!(admission, AccountRiskAdmission::Admitted { .. }));

        let warm = first.state().await.unwrap();
        drop(first);
        drop(second);

        let restarted = risk_authority(journal_id, &path);
        let cold = restarted.state().await.unwrap();
        assert_eq!(warm, cold);
        drop(restarted);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn historical_idempotency_is_scoped_to_one_paper_account() {
        let (root, path) = temp_case("account-scope");
        let journal_id = Uuid::new_v4();
        let first = paper_authority(journal_id, &path);
        let second = PaperAccountAuthority::new(
            journal_id,
            JsonlHistory::new(&path),
            PaperAccountConfig::new("paper-secondary", money("1000")).unwrap(),
        )
        .unwrap();
        let first_request = request(
            "shared-task",
            "shared-idempotency",
            Uuid::from_u128(1),
            Uuid::from_u128(2),
        );
        let first_reservation = match first.reserve(first_request).await.unwrap() {
            PaperReservationAdmission::Reserved(reservation) => reservation,
            PaperReservationAdmission::Existing(_) => panic!("expected first reservation"),
        };
        first
            .release(first_reservation.reservation_id, "account_scope_complete")
            .await
            .unwrap();

        let second_admission = second
            .reserve(request(
                "shared-task",
                "shared-idempotency",
                Uuid::from_u128(3),
                Uuid::from_u128(4),
            ))
            .await
            .unwrap();

        assert!(matches!(
            second_admission,
            PaperReservationAdmission::Reserved(_)
        ));
        drop(first);
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restart_quarantines_an_anchored_partial_tail_before_replay() {
        let (root, path) = temp_case("partial-restart");
        let journal_id = Uuid::new_v4();
        {
            let authority = paper_authority(journal_id, &path);
            authority
                .reserve(request(
                    "task/a",
                    "idem/a",
                    Uuid::from_u128(1),
                    Uuid::from_u128(2),
                ))
                .await
                .unwrap();
        }
        let partial = br#"{"timestamp":"2026-08-03T00:00:00Z","strategy":"crash""#;
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(partial);
        std::fs::write(&path, bytes).unwrap();

        let restarted = paper_authority(journal_id, &path);
        let snapshot = restarted.snapshot().await.unwrap();
        assert_eq!(snapshot.reservations.len(), 1);
        assert!(std::fs::read(&path).unwrap().ends_with(b"\n"));
        let quarantines = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|candidate| {
                candidate
                    .extension()
                    .is_some_and(|extension| extension == "quarantine")
            })
            .collect::<Vec<_>>();
        assert_eq!(quarantines.len(), 1);
        assert_eq!(std::fs::read(&quarantines[0]).unwrap(), partial);

        drop(restarted);
        std::fs::remove_dir_all(root).unwrap();
    }
}
