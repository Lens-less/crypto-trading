//! Testnet-only owner that binds durable lifecycle recovery to authoritative
//! reconciliation and a latching local kill switch.
//!
//! The owner deliberately exposes no mainnet authority and no autonomous
//! strategy loop. It keeps one journal writer lease for its lifetime, performs
//! two stable REST reconciliations before becoming ready, delegates the exact
//! submit/query/cancel state machine to [`crate::testnet_lifecycle`], and
//! refuses every later campaign after the kill switch is durable.

use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};

use chrono::{DateTime, Utc};
use crypto_trading_domain::{OperationalOwnerPhase, set_operational_owner_phase};
use crypto_trading_exchange::{ExchangeError, ExchangeHandle, ReconcileReceipt, ReconcileScope};
use crypto_trading_runtime::{
    BinanceUserDataApply, BinanceUserDataReconcileReason, BinanceUserDataState,
    BinanceUserDataStreamItem, DecisionRecord, HistoryError, HistoryTailRepairOutcome,
    JournalReadError, JsonlHistory, normalized_lock_key, read_journal_chain,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::testnet_lifecycle::{
    TestnetLifecycleConfig, TestnetLifecycleError, TestnetLifecycleRecoveryState,
    TestnetLifecycleReport, TestnetLifecycleVenue, run_testnet_lifecycle,
    testnet_lifecycle_recovery_state,
};

pub const CONTINUOUS_TESTNET_OWNER_SCHEMA_VERSION: u16 = 1;

const OWNER_STRATEGY: &str = "binance_testnet_continuous_owner";
const OWNER_SYMBOL: &str = "control-plane";
const BOOTSTRAP_PLANNED: &str = "continuous_testnet_bootstrap_planned";
const USER_STREAM_AWAITED: &str = "continuous_testnet_user_stream_awaited";
const USER_STREAM_SUBSCRIBED: &str = "continuous_testnet_user_stream_subscribed";
const USER_STREAM_HEARTBEAT: &str = "continuous_testnet_user_stream_heartbeat";
const USER_DATA_APPLIED: &str = "continuous_testnet_user_data_applied";
const USER_STREAM_RECOVERED: &str = "continuous_testnet_user_stream_recovered";
const RECONCILE_VERIFIED: &str = "continuous_testnet_reconcile_verified";
const CAMPAIGN_PLANNED: &str = "continuous_testnet_campaign_planned";
const CAMPAIGN_RECOVERY_VERIFIED: &str = "continuous_testnet_campaign_recovery_verified";
const CAMPAIGN_COMPLETED: &str = "continuous_testnet_campaign_completed";
const RECOVERY_REQUIRED: &str = "continuous_testnet_recovery_required";
const KILL_SWITCH_ENGAGED: &str = "continuous_testnet_kill_switch_engaged";
const KILLED_CLEAN: &str = "continuous_testnet_killed_clean";
const HISTORY_REPAIR_STRATEGY: &str = "history_repair_audit";
const HISTORY_TAIL_REPAIRED: &str = "history_tail_repaired";
const MAX_OWNER_ID_BYTES: usize = 128;

type OwnerLockRegistry = StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>>;
static OWNER_LOCKS: OnceLock<OwnerLockRegistry> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuousTestnetOwnerPhase {
    Reconciling,
    AwaitingUserStream,
    ReadyUnarmed,
    CampaignRunning,
    RecoveryRequired,
    KilledClean,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuousTestnetOwnerStatus {
    pub owner_id: String,
    /// Present only when this owner was given the exact durable lifecycle
    /// identity. Read-only stream owners never manufacture a campaign ID.
    pub campaign_id: Option<String>,
    pub phase: ContinuousTestnetOwnerPhase,
    pub kill_switch_latched: bool,
    /// True only after a subscription acknowledgement on the current stream.
    pub user_stream_active: bool,
    /// Observational only. Generic [`ReconcileReceipt`] does not carry
    /// balances, so REST recovery deliberately clears this bit until a fresh
    /// account update arrives on the authenticated stream.
    pub balance_projection_observed: bool,
    pub last_recorded_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuousTestnetRecoveryReason {
    ConnectionRestart,
    TransportGap,
    StreamExpired,
    EventTimeRegression,
    ExecutionRegression,
    LocalSequenceRegression,
    SourceUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContinuousTestnetUserDataOutcome {
    Subscribed,
    Heartbeat,
    Applied(BinanceUserDataApply),
    ReconciledAwaitingSubscription(ContinuousTestnetRecoveryReason),
}

/// One Testnet lifecycle authority. The held guard prevents sibling owners in
/// this process; [`JsonlHistory`] provides the matching cross-process lease.
pub struct ContinuousTestnetOwner<V> {
    owner_id: String,
    config: Option<TestnetLifecycleConfig>,
    venue: Arc<V>,
    history: JsonlHistory,
    status: ContinuousTestnetOwnerStatus,
    user_data: BinanceUserDataState,
    _owner_lease: OwnedMutexGuard<()>,
}

impl<V> fmt::Debug for ContinuousTestnetOwner<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContinuousTestnetOwner")
            .field("status", &self.status)
            .field("history", &self.history.path())
            .finish_non_exhaustive()
    }
}

impl<V> ContinuousTestnetOwner<V>
where
    V: TestnetLifecycleVenue + ExchangeHandle + 'static,
{
    /// Acquires the single-writer lane, journals bootstrap intent, and proves
    /// two stable authoritative account snapshots before returning ready.
    ///
    /// # Errors
    ///
    /// Invalid identity/journal state and a busy writer lane return an error.
    /// Remote lifecycle or reconciliation failures remain inspectable: they
    /// are journaled and returned as an owner in [`ContinuousTestnetOwnerPhase::RecoveryRequired`].
    pub async fn start(
        owner_id: impl Into<String>,
        config: TestnetLifecycleConfig,
        venue: Arc<V>,
        history: JsonlHistory,
    ) -> Result<Self, ContinuousTestnetOwnerError> {
        Self::start_inner(owner_id.into(), Some(config), venue, history, false).await
    }

    /// Starts an owner for authenticated stream projection and stable account
    /// reconciliation only. This mode has no lifecycle submit/query/cancel
    /// authority and therefore cannot claim campaign-recovery evidence.
    ///
    /// # Errors
    ///
    /// Returns a bounded owner error for invalid identity/journal state, a
    /// busy writer lane, or failed bootstrap reconciliation.
    pub async fn start_read_only(
        owner_id: impl Into<String>,
        venue: Arc<V>,
        history: JsonlHistory,
    ) -> Result<Self, ContinuousTestnetOwnerError> {
        Self::start_inner(owner_id.into(), None, venue, history, false).await
    }

    /// Starts an owner with recovery-only authority for an exact lifecycle
    /// that is already durably planned. Fresh/first-submit eligible campaigns
    /// are rejected before any remote I/O.
    ///
    /// # Errors
    ///
    /// Returns a bounded owner error unless the lifecycle is pending and its
    /// exact durable identity can be recovered safely.
    pub async fn start_recovery_only(
        owner_id: impl Into<String>,
        config: TestnetLifecycleConfig,
        venue: Arc<V>,
        history: JsonlHistory,
    ) -> Result<Self, ContinuousTestnetOwnerError> {
        Self::start_inner(owner_id.into(), Some(config), venue, history, true).await
    }

    async fn start_inner(
        owner_id: String,
        config: Option<TestnetLifecycleConfig>,
        venue: Arc<V>,
        history: JsonlHistory,
        recovery_only: bool,
    ) -> Result<Self, ContinuousTestnetOwnerError> {
        let owner_id = validate_owner_id(&owner_id)?;
        let owner_lock = shared_owner_lock(history.path());
        let owner_lease = owner_lock
            .try_lock_owned()
            .map_err(|_| ContinuousTestnetOwnerError::OwnerBusy)?;
        let campaign_id = config.as_ref().map(TestnetLifecycleConfig::campaign_id);
        repair_owner_history(&history, &owner_id, campaign_id).await?;
        if recovery_only
            && !matches!(
                config
                    .as_ref()
                    .map(|config| testnet_lifecycle_recovery_state(config, &history))
                    .transpose()?,
                Some(TestnetLifecycleRecoveryState::Pending { .. })
            )
        {
            return Err(ContinuousTestnetOwnerError::RecoveryPlanMissing);
        }
        let projected = project_account_owner(history.path())?;
        let now = Utc::now();
        let mut owner = Self {
            status: ContinuousTestnetOwnerStatus {
                owner_id: owner_id.clone(),
                campaign_id: campaign_id.map(str::to_owned),
                phase: if projected.kill_switch_latched {
                    ContinuousTestnetOwnerPhase::KilledClean
                } else {
                    ContinuousTestnetOwnerPhase::Reconciling
                },
                kill_switch_latched: projected.kill_switch_latched
                    || projected.kill_switch_cleanup_pending,
                user_stream_active: false,
                balance_projection_observed: false,
                last_recorded_at: projected.last_recorded_at.unwrap_or(now),
            },
            owner_id,
            config,
            venue,
            history,
            user_data: BinanceUserDataState::default(),
            _owner_lease: owner_lease,
        };
        owner.set_phase(owner.status.phase);
        if owner.status.kill_switch_latched {
            if projected.kill_switch_cleanup_pending {
                let residue = owner.stable_reconcile_clean_account().await?;
                owner.set_phase(ContinuousTestnetOwnerPhase::KilledClean);
                owner
                    .append(
                        KILLED_CLEAN,
                        "killed_clean",
                        json!({
                            "recovered_from_kill_switch_engaged": true,
                            "residue": residue,
                        }),
                    )
                    .await?;
            }
            return Ok(owner);
        }

        owner
            .append(BOOTSTRAP_PLANNED, "reconciling", json!({}))
            .await?;
        // A durable lifecycle plan removes submit authority. Recovery of the
        // exact client order ID must therefore happen before a broad account
        // snapshot, so restart can never hide an ambiguous submit behind a
        // later aggregate reconcile.
        if matches!(
            owner.lifecycle_recovery_state()?,
            Some(TestnetLifecycleRecoveryState::Pending { .. })
        ) && owner.run_lifecycle_inner().await.is_err()
        {
            return Ok(owner);
        }
        owner.set_phase(ContinuousTestnetOwnerPhase::Reconciling);
        match owner.stable_reconcile().await {
            Ok(()) => {
                owner.set_phase(ContinuousTestnetOwnerPhase::AwaitingUserStream);
                owner
                    .append(
                        USER_STREAM_AWAITED,
                        "awaiting_user_stream",
                        reconciliation_scope_observation(),
                    )
                    .await?;
            }
            Err(error) => {
                owner.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
                owner
                    .append(
                        RECOVERY_REQUIRED,
                        "recovery_required",
                        json!({"reason": error.reason_label()}),
                    )
                    .await?;
            }
        }
        Ok(owner)
    }

    #[must_use]
    pub const fn status(&self) -> &ContinuousTestnetOwnerStatus {
        &self.status
    }

    /// Applies one item from the authenticated Binance Testnet user stream.
    /// Any discontinuity is journaled before network recovery, resolves an
    /// already-planned lifecycle by exact client ID first, then requires two
    /// stable REST account snapshots. The in-memory projection is rebuilt and
    /// a fresh subscription acknowledgement is required before new work.
    ///
    /// # Errors
    ///
    /// Fails when the kill switch is latched, stream events arrive outside a
    /// ready subscription, or durable/query-first reconciliation cannot be
    /// completed safely.
    pub async fn ingest_user_data_item(
        &mut self,
        item: BinanceUserDataStreamItem,
    ) -> Result<ContinuousTestnetUserDataOutcome, ContinuousTestnetOwnerError> {
        if self.status.kill_switch_latched {
            return Err(ContinuousTestnetOwnerError::KillSwitchLatched);
        }
        match item {
            BinanceUserDataStreamItem::Subscribed {
                subscription_id,
                observed_at,
            } => {
                if self.status.phase == ContinuousTestnetOwnerPhase::RecoveryRequired {
                    return Err(ContinuousTestnetOwnerError::NotReady);
                }
                if self.status.phase == ContinuousTestnetOwnerPhase::ReadyUnarmed {
                    self.recover_user_stream(ContinuousTestnetRecoveryReason::ConnectionRestart)
                        .await?;
                }
                if self.status.phase != ContinuousTestnetOwnerPhase::AwaitingUserStream {
                    return Err(ContinuousTestnetOwnerError::NotReady);
                }
                self.user_data = BinanceUserDataState::default();
                self.status.user_stream_active = true;
                self.set_phase(ContinuousTestnetOwnerPhase::ReadyUnarmed);
                self.append(
                    USER_STREAM_SUBSCRIBED,
                    "ready_unarmed",
                    json!({
                        "subscription_id": subscription_id,
                        "observed_at": observed_at,
                        "balance_projection": "awaiting_fresh_user_stream",
                    }),
                )
                .await?;
                Ok(ContinuousTestnetUserDataOutcome::Subscribed)
            }
            BinanceUserDataStreamItem::Event(envelope) => {
                if self.status.phase != ContinuousTestnetOwnerPhase::ReadyUnarmed
                    || !self.status.user_stream_active
                {
                    return Err(ContinuousTestnetOwnerError::NotReady);
                }
                let apply = self.user_data.apply(envelope);
                if let BinanceUserDataApply::ReconcileRequired(reason) = apply {
                    let reason = ContinuousTestnetRecoveryReason::from(reason);
                    self.recover_user_stream(reason).await?;
                    return Ok(
                        ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(reason),
                    );
                }
                if apply == BinanceUserDataApply::AppliedAccountUpdate {
                    self.status.balance_projection_observed = true;
                }
                self.append(
                    USER_DATA_APPLIED,
                    "ready_unarmed",
                    json!({"result": user_data_apply_label(&apply)}),
                )
                .await?;
                Ok(ContinuousTestnetUserDataOutcome::Applied(apply))
            }
            BinanceUserDataStreamItem::Heartbeat { observed_at } => {
                if self.status.phase != ContinuousTestnetOwnerPhase::ReadyUnarmed
                    || !self.status.user_stream_active
                {
                    return Err(ContinuousTestnetOwnerError::NotReady);
                }
                self.append(
                    USER_STREAM_HEARTBEAT,
                    "ready_unarmed",
                    json!({"observed_at": observed_at}),
                )
                .await?;
                Ok(ContinuousTestnetUserDataOutcome::Heartbeat)
            }
            BinanceUserDataStreamItem::TransportGap {
                skipped,
                observed_at,
            } => {
                let _ = self.user_data.note_transport_gap(skipped, observed_at);
                self.recover_and_report(ContinuousTestnetRecoveryReason::TransportGap)
                    .await
            }
            BinanceUserDataStreamItem::StreamExpired { observed_at } => {
                let _ = self.user_data.note_stream_expired(observed_at);
                self.recover_and_report(ContinuousTestnetRecoveryReason::StreamExpired)
                    .await
            }
            BinanceUserDataStreamItem::SourceUnavailable { .. } => {
                self.recover_and_report(ContinuousTestnetRecoveryReason::SourceUnavailable)
                    .await
            }
        }
    }

    async fn recover_and_report(
        &mut self,
        reason: ContinuousTestnetRecoveryReason,
    ) -> Result<ContinuousTestnetUserDataOutcome, ContinuousTestnetOwnerError> {
        self.recover_user_stream(reason).await?;
        Ok(ContinuousTestnetUserDataOutcome::ReconciledAwaitingSubscription(reason))
    }

    async fn recover_user_stream(
        &mut self,
        reason: ContinuousTestnetRecoveryReason,
    ) -> Result<(), ContinuousTestnetOwnerError> {
        self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
        self.status.user_stream_active = false;
        self.status.balance_projection_observed = false;
        self.append(
            RECOVERY_REQUIRED,
            "recovery_required",
            json!({"reason": recovery_reason_label(reason)}),
        )
        .await?;

        if matches!(
            self.lifecycle_recovery_state()?,
            Some(TestnetLifecycleRecoveryState::Pending { .. })
        ) {
            self.run_lifecycle_inner().await?;
        }
        self.set_phase(ContinuousTestnetOwnerPhase::Reconciling);
        if let Err(error) = self.stable_reconcile().await {
            self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
            return Err(error);
        }
        self.user_data = BinanceUserDataState::default();
        self.set_phase(ContinuousTestnetOwnerPhase::AwaitingUserStream);
        self.append(
            USER_STREAM_RECOVERED,
            "awaiting_user_stream",
            json!({
                "reason": recovery_reason_label(reason),
                "authoritative_scope": "orders_positions_only",
                "balance_projection": "awaiting_fresh_user_stream",
            }),
        )
        .await
    }

    /// Runs or resumes the exact durable Testnet lifecycle. Restart recovery
    /// is explicitly marked before the shared lifecycle seam performs its
    /// first query; a durable plan can never return to the submit branch.
    ///
    /// # Errors
    ///
    /// Fails when the owner is not ready, the kill switch is latched, or the
    /// shared durable lifecycle cannot complete safely.
    pub async fn run_lifecycle(
        &mut self,
    ) -> Result<TestnetLifecycleReport, ContinuousTestnetOwnerError> {
        if self.status.kill_switch_latched {
            return Err(ContinuousTestnetOwnerError::KillSwitchLatched);
        }
        if !matches!(
            self.status.phase,
            ContinuousTestnetOwnerPhase::ReadyUnarmed
                | ContinuousTestnetOwnerPhase::CampaignRunning
                | ContinuousTestnetOwnerPhase::RecoveryRequired
        ) {
            return Err(ContinuousTestnetOwnerError::NotReady);
        }
        if self.config.is_none() {
            return Err(ContinuousTestnetOwnerError::LifecycleAuthorityUnavailable);
        }
        self.run_lifecycle_inner().await
    }

    /// Resumes a probe future that was cancelled by its outer deadline. Every
    /// mutation is rediscovered from the durable lifecycle before remote I/O;
    /// account state is then reconciled twice and a fresh subscription ACK is
    /// required. This makes cancellation a retry boundary rather than an
    /// in-memory wedge.
    ///
    /// # Errors
    ///
    /// Returns a bounded owner error when durable lifecycle recovery or stable
    /// reconciliation cannot be completed.
    pub async fn resume_interrupted_work(&mut self) -> Result<(), ContinuousTestnetOwnerError> {
        if self.status.kill_switch_latched {
            return Err(ContinuousTestnetOwnerError::KillSwitchLatched);
        }
        if matches!(
            self.status.phase,
            ContinuousTestnetOwnerPhase::ReadyUnarmed
                | ContinuousTestnetOwnerPhase::AwaitingUserStream
        ) {
            return Ok(());
        }
        if self.config.is_some()
            && matches!(
                self.status.phase,
                ContinuousTestnetOwnerPhase::CampaignRunning
                    | ContinuousTestnetOwnerPhase::RecoveryRequired
            )
        {
            self.run_lifecycle_inner().await?;
        }
        self.set_phase(ContinuousTestnetOwnerPhase::Reconciling);
        if let Err(error) = self.stable_reconcile().await {
            self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
            self.append(
                RECOVERY_REQUIRED,
                "recovery_required",
                json!({"reason": error.reason_label()}),
            )
            .await?;
            return Err(error);
        }
        self.set_phase(ContinuousTestnetOwnerPhase::AwaitingUserStream);
        self.status.user_stream_active = false;
        self.status.balance_projection_observed = false;
        self.user_data = BinanceUserDataState::default();
        self.append(
            USER_STREAM_AWAITED,
            "awaiting_user_stream",
            json!({
                "reason": "interrupted_probe_resumed",
                "reconciliation": reconciliation_scope_observation(),
            }),
        )
        .await
    }

    async fn run_lifecycle_inner(
        &mut self,
    ) -> Result<TestnetLifecycleReport, ContinuousTestnetOwnerError> {
        let config = self
            .config
            .clone()
            .ok_or(ContinuousTestnetOwnerError::LifecycleAuthorityUnavailable)?;
        let recovery_state = testnet_lifecycle_recovery_state(&config, &self.history)?;
        let query_count_before = match recovery_state {
            TestnetLifecycleRecoveryState::Pending { query_count } => Some(query_count),
            _ => None,
        };
        self.set_phase(ContinuousTestnetOwnerPhase::CampaignRunning);
        if matches!(recovery_state, TestnetLifecycleRecoveryState::Fresh) {
            self.append(
                CAMPAIGN_PLANNED,
                "campaign_running",
                json!({"query_first": false}),
            )
            .await?;
        }
        match run_testnet_lifecycle(&config, &*self.venue, &self.history).await {
            Ok(report) => {
                if let Some(query_count_before) = query_count_before {
                    let query_delta = report
                        .query_count
                        .checked_sub(query_count_before)
                        .ok_or(ContinuousTestnetOwnerError::RecoveryQueryMissing)?;
                    if query_delta == 0 {
                        self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
                        return Err(ContinuousTestnetOwnerError::RecoveryQueryMissing);
                    }
                    self.append(
                        CAMPAIGN_RECOVERY_VERIFIED,
                        "campaign_recovered",
                        json!({
                            "query_first": true,
                            "query_count_before": query_count_before,
                            "query_count_after": report.query_count,
                            "query_delta": query_delta,
                            "client_order_id": config.intent().client_order_id,
                        }),
                    )
                    .await?;
                }
                self.set_phase(ContinuousTestnetOwnerPhase::ReadyUnarmed);
                self.append(
                    CAMPAIGN_COMPLETED,
                    "ready_unarmed",
                    json!({
                        "recovered": report.recovered,
                        "query_count": report.query_count,
                        "final_status": format!("{:?}", report.final_status).to_ascii_lowercase(),
                    }),
                )
                .await?;
                Ok(report)
            }
            Err(error) => {
                self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
                self.append(
                    RECOVERY_REQUIRED,
                    "recovery_required",
                    json!({"reason": "lifecycle"}),
                )
                .await?;
                Err(error.into())
            }
        }
    }

    /// Durably latches the kill switch before remote I/O. Any already planned
    /// campaign is then recovered through the query-first lifecycle seam; two
    /// stable account snapshots are required before `killed_clean` is durable.
    ///
    /// # Errors
    ///
    /// Fails if the kill fact cannot be journaled or pending lifecycle/account
    /// state cannot be reconciled to a stable clean snapshot.
    pub async fn engage_kill_switch(&mut self) -> Result<(), ContinuousTestnetOwnerError> {
        if self.status.phase == ContinuousTestnetOwnerPhase::KilledClean {
            return Ok(());
        }
        self.status.kill_switch_latched = true;
        self.status.user_stream_active = false;
        self.append(
            KILL_SWITCH_ENGAGED,
            "kill_switch_engaged",
            json!({"scope": "account_journal"}),
        )
        .await?;

        if matches!(
            self.lifecycle_recovery_state()?,
            Some(TestnetLifecycleRecoveryState::Pending { .. })
        ) {
            self.run_lifecycle_inner().await?;
            self.status.kill_switch_latched = true;
        }
        self.set_phase(ContinuousTestnetOwnerPhase::Reconciling);
        let residue = match self.stable_reconcile_clean_account().await {
            Ok(residue) => residue,
            Err(error) => {
                self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
                self.append(
                    RECOVERY_REQUIRED,
                    "recovery_required",
                    json!({"reason": error.reason_label()}),
                )
                .await?;
                return Err(error);
            }
        };
        self.set_phase(ContinuousTestnetOwnerPhase::KilledClean);
        self.append(
            KILLED_CLEAN,
            "killed_clean",
            json!({
                "recovered_from_kill_switch_engaged": false,
                "residue": residue,
            }),
        )
        .await
    }

    fn set_phase(&mut self, phase: ContinuousTestnetOwnerPhase) {
        self.status.phase = phase;
        set_operational_owner_phase(match phase {
            ContinuousTestnetOwnerPhase::Reconciling => OperationalOwnerPhase::Reconciling,
            ContinuousTestnetOwnerPhase::AwaitingUserStream => {
                OperationalOwnerPhase::AwaitingStreams
            }
            ContinuousTestnetOwnerPhase::ReadyUnarmed => OperationalOwnerPhase::ReadyUnarmed,
            ContinuousTestnetOwnerPhase::CampaignRunning => OperationalOwnerPhase::CampaignRunning,
            ContinuousTestnetOwnerPhase::RecoveryRequired => {
                OperationalOwnerPhase::RecoveryRequired
            }
            ContinuousTestnetOwnerPhase::KilledClean => OperationalOwnerPhase::KilledClean,
        });
    }

    async fn stable_reconcile_receipts(
        &self,
    ) -> Result<(ReconcileReceipt, ReconcileReceipt), ContinuousTestnetOwnerError> {
        let first = self.venue.reconcile(ReconcileScope::All).await?;
        let second = self.venue.reconcile(ReconcileScope::All).await?;
        if !first.foreign_orders.is_empty() || !second.foreign_orders.is_empty() {
            return Err(ContinuousTestnetOwnerError::ForeignActivity);
        }
        if !same_authoritative_state(&first, &second) {
            return Err(ContinuousTestnetOwnerError::UnstableReconciliation);
        }
        Ok((first, second))
    }

    async fn stable_reconcile_clean_account(&self) -> Result<Value, ContinuousTestnetOwnerError> {
        let (first, second) = self.stable_reconcile_receipts().await?;
        if !first.orders.is_empty()
            || !second.orders.is_empty()
            || !first.positions.is_empty()
            || !second.positions.is_empty()
        {
            return Err(ContinuousTestnetOwnerError::UnstableReconciliation);
        }
        Ok(json!({
            "owned_open_orders_first": first.orders.len(),
            "owned_open_orders_second": second.orders.len(),
            "foreign_open_orders_first": first.foreign_orders.len(),
            "foreign_open_orders_second": second.foreign_orders.len(),
            "positions_first": first.positions.len(),
            "positions_second": second.positions.len(),
            "balance_projection_observed": self.status.balance_projection_observed,
            "spot_balance_authority": "unavailable_in_reconcile_receipt",
        }))
    }

    async fn stable_reconcile(&self) -> Result<(), ContinuousTestnetOwnerError> {
        self.stable_reconcile_receipts().await.map(|_| ())
    }

    /// Proves two identical authoritative REST snapshots through the same
    /// owner that projects the authenticated stream. This is the reconcile
    /// sample used by the production soak path.
    ///
    /// # Errors
    ///
    /// Returns a bounded owner error for a latched/not-ready owner or an
    /// unstable, foreign, or unavailable authoritative snapshot.
    pub async fn verify_stable_reconcile(&mut self) -> Result<(), ContinuousTestnetOwnerError> {
        if self.status.kill_switch_latched {
            return Err(ContinuousTestnetOwnerError::KillSwitchLatched);
        }
        if self.status.phase == ContinuousTestnetOwnerPhase::RecoveryRequired {
            return Err(ContinuousTestnetOwnerError::NotReady);
        }
        if let Err(error) = self.stable_reconcile().await {
            self.set_phase(ContinuousTestnetOwnerPhase::RecoveryRequired);
            self.append(
                RECOVERY_REQUIRED,
                "recovery_required",
                json!({"reason": error.reason_label()}),
            )
            .await?;
            return Err(error);
        }
        self.append(
            RECONCILE_VERIFIED,
            "stable_reconcile_verified",
            reconciliation_scope_observation(),
        )
        .await
    }

    /// Ends an owner-hosted session without abandoning an in-flight durable
    /// lifecycle. A pending plan is resolved through the latching kill switch
    /// and exact-ID recovery; read-only, fresh-unsubmitted, and completed
    /// sessions require only a final stable authoritative reconciliation.
    ///
    /// # Errors
    ///
    /// Returns a bounded owner error when pending lifecycle cleanup or the
    /// final stable reconciliation cannot be proven durably.
    pub async fn shutdown_cleanly(&mut self) -> Result<(), ContinuousTestnetOwnerError> {
        let must_latch_kill_switch = matches!(
            self.status.phase,
            ContinuousTestnetOwnerPhase::CampaignRunning
                | ContinuousTestnetOwnerPhase::RecoveryRequired
        ) || matches!(
            self.lifecycle_recovery_state()?,
            Some(TestnetLifecycleRecoveryState::Pending { .. })
        );
        if must_latch_kill_switch {
            return self.engage_kill_switch().await;
        }
        if matches!(
            self.status.phase,
            ContinuousTestnetOwnerPhase::CampaignRunning
                | ContinuousTestnetOwnerPhase::Reconciling
                | ContinuousTestnetOwnerPhase::RecoveryRequired
        ) {
            self.resume_interrupted_work().await?;
        }
        self.verify_stable_reconcile().await
    }

    fn lifecycle_recovery_state(
        &self,
    ) -> Result<Option<TestnetLifecycleRecoveryState>, ContinuousTestnetOwnerError> {
        self.config
            .as_ref()
            .map(|config| testnet_lifecycle_recovery_state(config, &self.history))
            .transpose()
            .map_err(Into::into)
    }

    async fn append(
        &mut self,
        decision: &'static str,
        phase: &'static str,
        observation: Value,
    ) -> Result<(), ContinuousTestnetOwnerError> {
        let recorded_at = Utc::now().max(self.status.last_recorded_at);
        self.history
            .append(&DecisionRecord {
                timestamp: recorded_at,
                strategy: OWNER_STRATEGY.to_owned(),
                symbol: OWNER_SYMBOL.to_owned(),
                decision: decision.to_owned(),
                details: json!({
                    "schema_version": CONTINUOUS_TESTNET_OWNER_SCHEMA_VERSION,
                    "owner_id": self.owner_id,
                    "campaign_id": self.config.as_ref().map(TestnetLifecycleConfig::campaign_id),
                    "phase": phase,
                    "kill_switch_latched": self.status.kill_switch_latched,
                    "observation": observation,
                }),
            })
            .await?;
        self.status.last_recorded_at = recorded_at;
        Ok(())
    }
}

impl From<BinanceUserDataReconcileReason> for ContinuousTestnetRecoveryReason {
    fn from(reason: BinanceUserDataReconcileReason) -> Self {
        match reason {
            BinanceUserDataReconcileReason::ConnectionRestart => Self::ConnectionRestart,
            BinanceUserDataReconcileReason::TransportGap => Self::TransportGap,
            BinanceUserDataReconcileReason::StreamExpired => Self::StreamExpired,
            BinanceUserDataReconcileReason::EventTimeRegression => Self::EventTimeRegression,
            BinanceUserDataReconcileReason::ExecutionRegression => Self::ExecutionRegression,
            BinanceUserDataReconcileReason::LocalSequenceRegression => {
                Self::LocalSequenceRegression
            }
        }
    }
}

const fn recovery_reason_label(reason: ContinuousTestnetRecoveryReason) -> &'static str {
    match reason {
        ContinuousTestnetRecoveryReason::ConnectionRestart => "connection_restart",
        ContinuousTestnetRecoveryReason::TransportGap => "transport_gap",
        ContinuousTestnetRecoveryReason::StreamExpired => "stream_expired",
        ContinuousTestnetRecoveryReason::EventTimeRegression => "event_time_regression",
        ContinuousTestnetRecoveryReason::ExecutionRegression => "execution_regression",
        ContinuousTestnetRecoveryReason::LocalSequenceRegression => "local_sequence_regression",
        ContinuousTestnetRecoveryReason::SourceUnavailable => "source_unavailable",
    }
}

const fn user_data_apply_label(apply: &BinanceUserDataApply) -> &'static str {
    match apply {
        BinanceUserDataApply::AppliedExecution => "applied_execution",
        BinanceUserDataApply::AppliedAccountUpdate => "applied_account_update",
        BinanceUserDataApply::Duplicate => "duplicate",
        BinanceUserDataApply::IgnoredUnsupported => "ignored_unsupported",
        BinanceUserDataApply::ReconcileRequired(_) => "reconcile_required",
    }
}

fn reconciliation_scope_observation() -> Value {
    json!({
        "authoritative_scope": "orders_positions_only",
        "balance_projection": "awaiting_fresh_user_stream",
    })
}

fn same_authoritative_state(first: &ReconcileReceipt, second: &ReconcileReceipt) -> bool {
    let mut first_orders = first.orders.clone();
    let mut second_orders = second.orders.clone();
    first_orders.sort_by(|left, right| left.id.cmp(&right.id));
    second_orders.sort_by(|left, right| left.id.cmp(&right.id));
    let mut first_foreign_orders = first.foreign_orders.clone();
    let mut second_foreign_orders = second.foreign_orders.clone();
    first_foreign_orders.sort_by(|left, right| left.id.cmp(&right.id));
    second_foreign_orders.sort_by(|left, right| left.id.cmp(&right.id));
    let mut first_positions = first.positions.clone();
    let mut second_positions = second.positions.clone();
    let compare_positions = |left: &crypto_trading_domain::Position,
                             right: &crypto_trading_domain::Position| {
        left.exchange
            .cmp(&right.exchange)
            .then_with(|| left.symbol.as_str().cmp(right.symbol.as_str()))
            .then_with(|| {
                market_type_rank(left.market_type).cmp(&market_type_rank(right.market_type))
            })
    };
    first_positions.sort_by(compare_positions);
    second_positions.sort_by(compare_positions);

    first.scope == second.scope
        && first_orders == second_orders
        && first_foreign_orders == second_foreign_orders
        && first_positions == second_positions
}

const fn market_type_rank(market_type: crypto_trading_domain::MarketType) -> u8 {
    match market_type {
        crypto_trading_domain::MarketType::Spot => 0,
        crypto_trading_domain::MarketType::Perpetual => 1,
    }
}

#[derive(Default)]
struct ProjectedOwner {
    kill_switch_latched: bool,
    kill_switch_cleanup_pending: bool,
    last_recorded_at: Option<DateTime<Utc>>,
}

fn project_account_owner(path: &Path) -> Result<ProjectedOwner, ContinuousTestnetOwnerError> {
    if !path.exists() {
        return Ok(ProjectedOwner::default());
    }
    let bytes = read_journal_chain(path)?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(ContinuousTestnetOwnerError::InvalidJournal);
    }
    let mut projected = ProjectedOwner::default();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: DecisionRecord = serde_json::from_slice(line)
            .map_err(|_| ContinuousTestnetOwnerError::InvalidJournal)?;
        if record.strategy != OWNER_STRATEGY {
            continue;
        }
        // The journal is the single-writer account boundary. A kill fact must
        // therefore survive changes to task/owner and campaign identity;
        // filtering it to an exact pair would let a restart regain submit
        // authority simply by choosing new identifiers.
        if record.details.get("schema_version").and_then(Value::as_u64)
            != Some(u64::from(CONTINUOUS_TESTNET_OWNER_SCHEMA_VERSION))
            || projected
                .last_recorded_at
                .is_some_and(|last| record.timestamp < last)
        {
            return Err(ContinuousTestnetOwnerError::InvalidJournal);
        }
        projected.last_recorded_at = Some(record.timestamp);
        match record.decision.as_str() {
            KILL_SWITCH_ENGAGED => projected.kill_switch_cleanup_pending = true,
            KILLED_CLEAN => {
                projected.kill_switch_latched = true;
                projected.kill_switch_cleanup_pending = false;
            }
            _ => {}
        }
    }
    Ok(projected)
}

async fn repair_owner_history(
    history: &JsonlHistory,
    owner_id: &str,
    campaign_id: Option<&str>,
) -> Result<(), ContinuousTestnetOwnerError> {
    let observation = match history.repair_recoverable_tail().await? {
        HistoryTailRepairOutcome::Unchanged { .. } => return Ok(()),
        HistoryTailRepairOutcome::Quarantined {
            retained_bytes,
            quarantined_bytes,
            quarantine_path,
            pruned_files,
            pruned_bytes,
        } => json!({
            "repair": "quarantined",
            "retained_bytes": retained_bytes,
            "quarantined_bytes": quarantined_bytes,
            "quarantine_path": quarantine_path,
            "pruned_files": pruned_files,
            "pruned_bytes": pruned_bytes,
        }),
        HistoryTailRepairOutcome::TerminatorRestored { retained_bytes } => json!({
            "repair": "terminator_restored",
            "retained_bytes": retained_bytes,
        }),
    };
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: HISTORY_REPAIR_STRATEGY.to_owned(),
            symbol: OWNER_SYMBOL.to_owned(),
            decision: HISTORY_TAIL_REPAIRED.to_owned(),
            details: json!({
                "component": "continuous_testnet_owner",
                "owner_id": owner_id,
                "campaign_id": campaign_id,
                "observation": observation,
            }),
        })
        .await?;
    Ok(())
}

fn validate_owner_id(owner_id: &str) -> Result<String, ContinuousTestnetOwnerError> {
    let normalized = owner_id.trim();
    if normalized.is_empty()
        || normalized.len() > MAX_OWNER_ID_BYTES
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(ContinuousTestnetOwnerError::InvalidConfig);
    }
    Ok(normalized.to_owned())
}

fn shared_owner_lock(path: &Path) -> Arc<Mutex<()>> {
    let key = normalized_lock_key(path);
    let registry = OWNER_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, lock| lock.strong_count() > 0);
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    let lock = Arc::new(Mutex::new(()));
    registry.insert(key, Arc::downgrade(&lock));
    lock
}

#[derive(Debug)]
pub enum ContinuousTestnetOwnerError {
    InvalidConfig,
    OwnerBusy,
    NotReady,
    KillSwitchLatched,
    LifecycleAuthorityUnavailable,
    RecoveryPlanMissing,
    RecoveryQueryMissing,
    ForeignActivity,
    UnstableReconciliation,
    InvalidJournal,
    History(HistoryError),
    JournalRead(JournalReadError),
    Exchange(ExchangeError),
    Lifecycle(TestnetLifecycleError),
}

impl ContinuousTestnetOwnerError {
    const fn reason_label(&self) -> &'static str {
        match self {
            Self::ForeignActivity => "foreign_activity",
            Self::UnstableReconciliation => "unstable_reconciliation",
            Self::Exchange(_) => "exchange",
            _ => "owner_contract",
        }
    }
}

impl fmt::Display for ContinuousTestnetOwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig => "invalid continuous Testnet owner configuration",
            Self::OwnerBusy => "continuous Testnet owner lane is already held",
            Self::NotReady => "continuous Testnet owner is not ready and unarmed",
            Self::KillSwitchLatched => "continuous Testnet owner kill switch is latched",
            Self::LifecycleAuthorityUnavailable => {
                "continuous Testnet owner has no lifecycle authority"
            }
            Self::RecoveryPlanMissing => {
                "continuous Testnet recovery requires an exact pending durable lifecycle plan"
            }
            Self::RecoveryQueryMissing => {
                "continuous Testnet recovery did not perform a fresh exact-ID query"
            }
            Self::ForeignActivity => "foreign Testnet account activity requires recovery",
            Self::UnstableReconciliation => "Testnet account reconciliation was not stable",
            Self::InvalidJournal => "continuous Testnet owner journal is invalid",
            Self::History(_) => "continuous Testnet owner journal write failed",
            Self::JournalRead(_) => "continuous Testnet owner journal replay failed",
            Self::Exchange(_) => "continuous Testnet owner reconciliation failed",
            Self::Lifecycle(_) => "continuous Testnet lifecycle failed",
        })
    }
}

impl std::error::Error for ContinuousTestnetOwnerError {}

impl From<HistoryError> for ContinuousTestnetOwnerError {
    fn from(error: HistoryError) -> Self {
        Self::History(error)
    }
}

impl From<JournalReadError> for ContinuousTestnetOwnerError {
    fn from(error: JournalReadError) -> Self {
        Self::JournalRead(error)
    }
}

impl From<ExchangeError> for ContinuousTestnetOwnerError {
    fn from(error: ExchangeError) -> Self {
        Self::Exchange(error)
    }
}

impl From<TestnetLifecycleError> for ContinuousTestnetOwnerError {
    fn from(error: TestnetLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}
