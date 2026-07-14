use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crypto_trading_domain::{MarketType, Symbol};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    time::{Instant, timeout_at},
};

use crate::{
    ExchangeError, ExchangeHandle, ExchangeOperationKey, ExchangeStatus, MarketSubscription,
    ReconcileReceipt, ReconcileScope, SubscriptionReceipt, TradingCommand, TradingReceipt,
};

/// Cloneable actor handle that caps normal work and reserves one bounded lane
/// each for cancel-one, cancel-all, and authoritative reconciliation traffic.
#[derive(Clone)]
pub struct BoundedExchangeHandle {
    normal_sender: mpsc::Sender<Request>,
    priority_sender: mpsc::Sender<Request>,
    barrier_sender: mpsc::Sender<Request>,
    reconciliation_sender: mpsc::Sender<Request>,
    normal_in_flight: Arc<Semaphore>,
    priority_in_flight: Arc<Semaphore>,
    barrier_in_flight: Arc<Semaphore>,
    reconciliation_in_flight: Arc<Semaphore>,
    admission_state: Arc<StdMutex<AdmissionState>>,
    capacity: usize,
    operation_timeout: Duration,
}

const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const CANCELLATION_CAPACITY: usize = 1;
const CANCEL_ALL_CAPACITY: usize = 1;
const RECONCILIATION_CAPACITY: usize = 1;
const MAX_COMMAND_CAPACITY: usize = 4_096;

#[derive(Clone, Copy)]
enum AdmissionLane {
    Normal,
    Cancellation,
    CancelAll,
    Reconciliation,
}

#[derive(Clone)]
struct Admission(Arc<StdMutex<Option<OwnedSemaphorePermit>>>);

impl Admission {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self(Arc::new(StdMutex::new(Some(permit))))
    }

    fn release(&self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

struct PendingSubmission {
    symbol: Symbol,
    market_type: MarketType,
    cancelled_by_barrier: bool,
}

struct Quarantine {
    operation_key: ExchangeOperationKey,
    required_scope: ReconcileScope,
    minimum_adapter_watermark: Option<DateTime<Utc>>,
    minimum_reconcile_generation: u64,
}

struct AdmissionState {
    next_submission_id: u64,
    pending_submissions: HashMap<u64, PendingSubmission>,
    capacity: usize,
    quarantine: Option<Quarantine>,
    last_successful_reconcile_observed_at: Option<DateTime<Utc>>,
    reconcile_generation: u64,
}

impl AdmissionState {
    fn new(capacity: usize) -> Result<Self, ExchangeError> {
        let mut pending_submissions = HashMap::new();
        pending_submissions.try_reserve(capacity).map_err(|_| {
            ExchangeError::unavailable("unable to reserve bounded submit admission metadata")
        })?;
        Ok(Self {
            next_submission_id: 1,
            pending_submissions,
            capacity,
            quarantine: None,
            last_successful_reconcile_observed_at: None,
            reconcile_generation: 0,
        })
    }

    fn register(&mut self, submission: PendingSubmission) -> Result<u64, ExchangeError> {
        if self.pending_submissions.len() >= self.capacity {
            return Err(ExchangeError::resource_limit(
                "pending submit admission metadata",
                self.capacity,
                self.pending_submissions.len().saturating_add(1),
            ));
        }
        let id = self.next_submission_id;
        self.next_submission_id = id
            .checked_add(1)
            .ok_or_else(|| ExchangeError::invariant("submit admission sequence overflowed"))?;
        if self.pending_submissions.insert(id, submission).is_some() {
            return Err(ExchangeError::invariant(
                "submit admission sequence was reused",
            ));
        }
        Ok(id)
    }

    fn mark_cancelled(&mut self, symbol: Option<&Symbol>, market_type: Option<MarketType>) {
        for pending in self.pending_submissions.values_mut() {
            let symbol_matches = symbol.is_none_or(|candidate| pending.symbol == *candidate);
            let market_matches =
                market_type.is_none_or(|candidate| pending.market_type == candidate);
            if symbol_matches && market_matches {
                pending.cancelled_by_barrier = true;
            }
        }
    }

    fn record_ambiguity(
        &mut self,
        operation_key: ExchangeOperationKey,
        required_scope: ReconcileScope,
    ) {
        let minimum_adapter_watermark = self.last_successful_reconcile_observed_at;
        let minimum_reconcile_generation = self.reconcile_generation;
        match &mut self.quarantine {
            Some(quarantine) => {
                quarantine.required_scope =
                    merge_reconcile_scopes(&quarantine.required_scope, &required_scope);
                quarantine.minimum_adapter_watermark = match (
                    quarantine.minimum_adapter_watermark,
                    minimum_adapter_watermark,
                ) {
                    (Some(existing), Some(candidate)) => Some(existing.max(candidate)),
                    (existing @ Some(_), None) => existing,
                    (None, candidate) => candidate,
                };
                quarantine.minimum_reconcile_generation = quarantine
                    .minimum_reconcile_generation
                    .max(minimum_reconcile_generation);
            }
            None => {
                self.quarantine = Some(Quarantine {
                    operation_key,
                    required_scope,
                    minimum_adapter_watermark,
                    minimum_reconcile_generation,
                });
            }
        }
    }

    fn record_reconciliation(
        &mut self,
        requested_scope: &ReconcileScope,
        receipt: &ReconcileReceipt,
    ) -> Result<(), ExchangeError> {
        if &receipt.scope != requested_scope {
            return Err(ExchangeError::invalid_response(
                "bounded adapter",
                format!(
                    "reconciliation returned scope {:?} for request {:?}",
                    receipt.scope, requested_scope
                ),
            ));
        }
        if self
            .last_successful_reconcile_observed_at
            .is_some_and(|last| receipt.observed_at < last)
        {
            return Err(ExchangeError::invalid_response(
                "bounded adapter",
                format!(
                    "reconciliation watermark {} regressed below the last successful watermark {:?}",
                    receipt.observed_at, self.last_successful_reconcile_observed_at
                ),
            ));
        }
        if self.quarantine.as_ref().is_some_and(|quarantine| {
            quarantine
                .minimum_adapter_watermark
                .is_some_and(|minimum| receipt.observed_at < minimum)
        }) {
            return Err(ExchangeError::invalid_response(
                "bounded adapter",
                "reconciliation watermark regressed below the quarantined adapter watermark",
            ));
        }

        let next_generation = self
            .reconcile_generation
            .checked_add(1)
            .ok_or_else(|| ExchangeError::invariant("reconciliation generation overflowed"))?;
        let clears_quarantine = self.quarantine.as_ref().is_some_and(|quarantine| {
            reconcile_scope_covers(&receipt.scope, &quarantine.required_scope)
                && next_generation > quarantine.minimum_reconcile_generation
                && quarantine
                    .minimum_adapter_watermark
                    .is_none_or(|minimum| receipt.observed_at >= minimum)
        });

        self.last_successful_reconcile_observed_at = Some(receipt.observed_at);
        self.reconcile_generation = next_generation;
        if clears_quarantine {
            self.quarantine = None;
        }
        Ok(())
    }

    fn quarantine_error(&self) -> Option<ExchangeError> {
        self.quarantine.as_ref().map(|quarantine| {
            ExchangeError::rejected(format!(
                "submit is quarantined until authoritative reconciliation covers {:?} without regressing adapter watermark {:?} after local generation {} for ambiguous operation {:?}",
                quarantine.required_scope,
                quarantine.minimum_adapter_watermark,
                quarantine.minimum_reconcile_generation,
                quarantine.operation_key
            ))
        })
    }
}

fn command_reconcile_scope(command: &TradingCommand) -> ReconcileScope {
    match command {
        TradingCommand::Submit(_) => ReconcileScope::All,
        TradingCommand::Cancel { .. } => ReconcileScope::Orders { symbol: None },
        TradingCommand::CancelAll { symbol, .. } => ReconcileScope::Orders {
            symbol: symbol.clone(),
        },
    }
}

fn reconcile_scope_covers(actual: &ReconcileScope, required: &ReconcileScope) -> bool {
    match (actual, required) {
        (ReconcileScope::All, _)
        | (ReconcileScope::Orders { symbol: None }, ReconcileScope::Orders { .. })
        | (ReconcileScope::Positions { symbol: None }, ReconcileScope::Positions { .. }) => true,
        (
            ReconcileScope::Orders {
                symbol: Some(actual),
            },
            ReconcileScope::Orders {
                symbol: Some(required),
            },
        )
        | (
            ReconcileScope::Positions {
                symbol: Some(actual),
            },
            ReconcileScope::Positions {
                symbol: Some(required),
            },
        ) => actual == required,
        _ => false,
    }
}

fn merge_reconcile_scopes(left: &ReconcileScope, right: &ReconcileScope) -> ReconcileScope {
    if reconcile_scope_covers(left, right) {
        return left.clone();
    }
    if reconcile_scope_covers(right, left) {
        return right.clone();
    }
    match (left, right) {
        (ReconcileScope::Orders { .. }, ReconcileScope::Orders { .. }) => {
            ReconcileScope::Orders { symbol: None }
        }
        (ReconcileScope::Positions { .. }, ReconcileScope::Positions { .. }) => {
            ReconcileScope::Positions { symbol: None }
        }
        _ => ReconcileScope::All,
    }
}

struct AdmissionStateCleanup(Arc<StdMutex<AdmissionState>>);

impl Drop for AdmissionStateCleanup {
    fn drop(&mut self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending_submissions
            .clear();
    }
}

enum ExecuteAdmission {
    Submit(PendingSubmission),
    CancelAll {
        symbol: Option<Symbol>,
        market_type: Option<MarketType>,
    },
    CancelOne,
}

#[derive(Clone, Copy)]
struct ExecuteDeadlines {
    operation_deadline: Instant,
    dispatch_before: Instant,
}

enum Request {
    Execute {
        command: TradingCommand,
        pending_submit_id: Option<u64>,
        deadlines: ExecuteDeadlines,
        response: oneshot::Sender<Result<TradingReceipt, ExchangeError>>,
        _admission: Admission,
    },
    Reconcile {
        scope: ReconcileScope,
        deadline: Instant,
        response: oneshot::Sender<Result<ReconcileReceipt, ExchangeError>>,
        _admission: Admission,
    },
    Subscribe {
        subscription: MarketSubscription,
        deadline: Instant,
        response: oneshot::Sender<Result<SubscriptionReceipt, ExchangeError>>,
        _admission: Admission,
    },
    Status {
        deadline: Instant,
        response: oneshot::Sender<Result<ExchangeStatus, ExchangeError>>,
        _admission: Admission,
    },
}

async fn run_actor(
    inner: Arc<dyn ExchangeHandle>,
    mut normal_receiver: mpsc::Receiver<Request>,
    mut priority_receiver: mpsc::Receiver<Request>,
    mut barrier_receiver: mpsc::Receiver<Request>,
    mut reconciliation_receiver: mpsc::Receiver<Request>,
    admission_state: Arc<StdMutex<AdmissionState>>,
) {
    let _admission_cleanup = AdmissionStateCleanup(Arc::clone(&admission_state));
    loop {
        let request = tokio::select! {
            biased;
            request = barrier_receiver.recv() => request,
            request = reconciliation_receiver.recv() => request,
            request = priority_receiver.recv() => request,
            request = normal_receiver.recv() => request,
        };
        let Some(request) = request else {
            if barrier_receiver.is_closed()
                && reconciliation_receiver.is_closed()
                && priority_receiver.is_closed()
                && normal_receiver.is_closed()
            {
                break;
            }
            continue;
        };
        dispatch_request(&inner, &admission_state, request).await;
    }
}

async fn dispatch_request(
    inner: &Arc<dyn ExchangeHandle>,
    admission_state: &Arc<StdMutex<AdmissionState>>,
    request: Request,
) {
    match request {
        Request::Execute {
            command,
            pending_submit_id,
            deadlines,
            response,
            _admission: admission,
        } => {
            dispatch_execute(
                inner,
                admission_state,
                command,
                pending_submit_id,
                deadlines,
                response,
                admission,
            )
            .await;
        }
        Request::Reconcile {
            scope,
            deadline,
            response,
            _admission: admission,
        } => {
            dispatch_reconcile(inner, admission_state, scope, deadline, response, admission).await;
        }
        Request::Subscribe {
            subscription,
            deadline,
            response,
            _admission: admission,
        } => dispatch_subscribe(inner, subscription, deadline, response, admission).await,
        Request::Status {
            deadline,
            response,
            _admission: admission,
        } => dispatch_status(inner, deadline, response, admission).await,
    }
}

async fn dispatch_execute(
    inner: &Arc<dyn ExchangeHandle>,
    admission_state: &Arc<StdMutex<AdmissionState>>,
    command: TradingCommand,
    pending_submit_id: Option<u64>,
    deadlines: ExecuteDeadlines,
    response: oneshot::Sender<Result<TradingReceipt, ExchangeError>>,
    _admission: Admission,
) {
    let expired_before_dispatch = Instant::now() >= deadlines.dispatch_before;
    let operation = command.operation();
    let client_order_id = command.client_order_id();
    let operation_key = command.operation_key();
    let required_scope = command_reconcile_scope(&command);
    if let Some(pending_submit_id) = pending_submit_id {
        let pending = admission_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending_submissions
            .remove(&pending_submit_id);
        let Some(pending) = pending else {
            let _response_was_dropped = response.send(Err(ExchangeError::invariant(
                "pending submit admission metadata is missing",
            )));
            return;
        };
        if expired_before_dispatch {
            let _response_was_dropped = response.send(Err(ExchangeError::rejected(format!(
                "{operation} request expired before adapter dispatch"
            ))));
            return;
        }
        if pending.cancelled_by_barrier {
            let client_order_id =
                client_order_id.map_or_else(|| "unknown".to_owned(), |value| value.to_string());
            let _response_was_dropped = response.send(Err(ExchangeError::rejected(format!(
                "submit {client_order_id} was cancelled by cancel_all before adapter dispatch"
            ))));
            return;
        }
    }
    if expired_before_dispatch {
        let _response_was_dropped = response.send(Err(ExchangeError::rejected(format!(
            "{operation} request expired before adapter dispatch"
        ))));
        return;
    }
    let quarantine_error = matches!(&command, TradingCommand::Submit(_)).then(|| {
        admission_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .quarantine_error()
    });
    if let Some(error) = quarantine_error.flatten() {
        let _response_was_dropped = response.send(Err(error));
        return;
    }
    if Instant::now() >= deadlines.operation_deadline {
        let _response_was_dropped = response.send(Err(ExchangeError::rejected(format!(
            "{operation} request expired before adapter dispatch"
        ))));
        return;
    }
    let result = match timeout_at(
        deadlines.operation_deadline,
        inner.execute_before(command, deadlines.dispatch_before),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ExchangeError::AmbiguousOutcome {
            operation,
            client_order_id,
            operation_key: Some(operation_key.clone()),
            reason: "adapter operation timed out after local admission".to_owned(),
        }),
    };
    let result_is_ambiguous = matches!(result, Err(ExchangeError::AmbiguousOutcome { .. }));
    if result_is_ambiguous {
        admission_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_ambiguity(operation_key.clone(), required_scope.clone());
    }
    if response.send(result).is_err() && !result_is_ambiguous {
        admission_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_ambiguity(operation_key, required_scope);
    }
}

async fn dispatch_reconcile(
    inner: &Arc<dyn ExchangeHandle>,
    admission_state: &Arc<StdMutex<AdmissionState>>,
    scope: ReconcileScope,
    deadline: Instant,
    response: oneshot::Sender<Result<ReconcileReceipt, ExchangeError>>,
    _admission: Admission,
) {
    if Instant::now() >= deadline {
        let _response_was_dropped = response.send(Err(ExchangeError::unavailable(
            "exchange reconciliation expired before adapter dispatch",
        )));
        return;
    }
    let requested_scope = scope.clone();
    let result = timeout_at(deadline, inner.reconcile(scope))
        .await
        .unwrap_or_else(|_| {
            Err(ExchangeError::unavailable(
                "exchange reconciliation timed out",
            ))
        });
    let result = result.and_then(|receipt| {
        admission_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_reconciliation(&requested_scope, &receipt)?;
        Ok(receipt)
    });
    let _response_was_dropped = response.send(result);
}

async fn dispatch_subscribe(
    inner: &Arc<dyn ExchangeHandle>,
    subscription: MarketSubscription,
    deadline: Instant,
    response: oneshot::Sender<Result<SubscriptionReceipt, ExchangeError>>,
    _admission: Admission,
) {
    if Instant::now() >= deadline {
        let _response_was_dropped = response.send(Err(ExchangeError::unavailable(
            "exchange subscription expired before adapter dispatch",
        )));
        return;
    }
    let result = timeout_at(deadline, inner.subscribe(subscription))
        .await
        .unwrap_or_else(|_| {
            Err(ExchangeError::unavailable(
                "exchange subscription timed out",
            ))
        });
    let _response_was_dropped = response.send(result);
}

async fn dispatch_status(
    inner: &Arc<dyn ExchangeHandle>,
    deadline: Instant,
    response: oneshot::Sender<Result<ExchangeStatus, ExchangeError>>,
    _admission: Admission,
) {
    if Instant::now() >= deadline {
        let _response_was_dropped = response.send(Err(ExchangeError::unavailable(
            "exchange status expired before adapter dispatch",
        )));
        return;
    }
    let result = timeout_at(deadline, inner.status())
        .await
        .unwrap_or_else(|_| Err(ExchangeError::unavailable("exchange status timed out")));
    let _response_was_dropped = response.send(result);
}

impl BoundedExchangeHandle {
    /// Starts a single-owner adapter actor on the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] when `capacity` exceeds the hard cap.
    pub fn spawn<H>(inner: Arc<H>, capacity: NonZeroUsize) -> Result<Self, ExchangeError>
    where
        H: ExchangeHandle + 'static,
    {
        let inner: Arc<dyn ExchangeHandle> = inner;
        Self::spawn_dyn_with_timeout(inner, capacity, DEFAULT_OPERATION_TIMEOUT)
    }

    /// Starts a single-owner adapter actor with an explicit operation timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] when `capacity` exceeds the hard cap.
    pub fn spawn_with_timeout<H>(
        inner: Arc<H>,
        capacity: NonZeroUsize,
        operation_timeout: Duration,
    ) -> Result<Self, ExchangeError>
    where
        H: ExchangeHandle + 'static,
    {
        let inner: Arc<dyn ExchangeHandle> = inner;
        Self::spawn_dyn_with_timeout(inner, capacity, operation_timeout)
    }

    /// Starts a bounded actor from an object-safe exchange implementation.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] when `capacity` exceeds the hard cap.
    pub fn spawn_dyn(
        inner: Arc<dyn ExchangeHandle>,
        capacity: NonZeroUsize,
    ) -> Result<Self, ExchangeError> {
        Self::spawn_dyn_with_timeout(inner, capacity, DEFAULT_OPERATION_TIMEOUT)
    }

    /// Starts a bounded actor from an object-safe adapter with an explicit timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::ResourceLimit`] when `capacity` exceeds the hard cap.
    pub fn spawn_dyn_with_timeout(
        inner: Arc<dyn ExchangeHandle>,
        capacity: NonZeroUsize,
        operation_timeout: Duration,
    ) -> Result<Self, ExchangeError> {
        let capacity = capacity.get();
        if capacity > MAX_COMMAND_CAPACITY {
            return Err(ExchangeError::resource_limit(
                "exchange command capacity",
                MAX_COMMAND_CAPACITY,
                capacity,
            ));
        }
        let admission_state = Arc::new(StdMutex::new(AdmissionState::new(capacity)?));
        let (normal_sender, normal_receiver) = mpsc::channel(capacity);
        let (priority_sender, priority_receiver) = mpsc::channel(CANCELLATION_CAPACITY);
        let (barrier_sender, barrier_receiver) = mpsc::channel(CANCEL_ALL_CAPACITY);
        let (reconciliation_sender, reconciliation_receiver) =
            mpsc::channel(RECONCILIATION_CAPACITY);
        let actor_admission_state = Arc::clone(&admission_state);
        tokio::spawn(run_actor(
            inner,
            normal_receiver,
            priority_receiver,
            barrier_receiver,
            reconciliation_receiver,
            actor_admission_state,
        ));
        Ok(Self {
            normal_sender,
            priority_sender,
            barrier_sender,
            reconciliation_sender,
            normal_in_flight: Arc::new(Semaphore::new(capacity)),
            priority_in_flight: Arc::new(Semaphore::new(CANCELLATION_CAPACITY)),
            barrier_in_flight: Arc::new(Semaphore::new(CANCEL_ALL_CAPACITY)),
            reconciliation_in_flight: Arc::new(Semaphore::new(RECONCILIATION_CAPACITY)),
            admission_state,
            capacity,
            operation_timeout,
        })
    }

    fn try_admit(&self, lane: AdmissionLane) -> Result<Admission, ExchangeError> {
        let (in_flight, capacity) = match lane {
            AdmissionLane::Normal => (&self.normal_in_flight, self.capacity),
            AdmissionLane::Cancellation => (&self.priority_in_flight, CANCELLATION_CAPACITY),
            AdmissionLane::CancelAll => (&self.barrier_in_flight, CANCEL_ALL_CAPACITY),
            AdmissionLane::Reconciliation => {
                (&self.reconciliation_in_flight, RECONCILIATION_CAPACITY)
            }
        };
        Arc::clone(in_flight)
            .try_acquire_owned()
            .map(Admission::new)
            .map_err(|_| ExchangeError::Backpressure { capacity })
    }

    fn send(&self, request: Request, lane: AdmissionLane) -> Result<(), ExchangeError> {
        let (sender, capacity) = match lane {
            AdmissionLane::Normal => (&self.normal_sender, self.capacity),
            AdmissionLane::Cancellation => (&self.priority_sender, CANCELLATION_CAPACITY),
            AdmissionLane::CancelAll => (&self.barrier_sender, CANCEL_ALL_CAPACITY),
            AdmissionLane::Reconciliation => (&self.reconciliation_sender, RECONCILIATION_CAPACITY),
        };
        sender.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => ExchangeError::Backpressure { capacity },
            mpsc::error::TrySendError::Closed(_) => {
                ExchangeError::unavailable("exchange command actor is closed")
            }
        })
    }

    fn send_execute(
        &self,
        command: TradingCommand,
        deadlines: ExecuteDeadlines,
        response: oneshot::Sender<Result<TradingReceipt, ExchangeError>>,
        admission: Admission,
    ) -> Result<(), ExchangeError> {
        let execute_admission = match &command {
            TradingCommand::Submit(intent) => ExecuteAdmission::Submit(PendingSubmission {
                symbol: intent.symbol.clone(),
                market_type: intent.market_type,
                cancelled_by_barrier: false,
            }),
            TradingCommand::CancelAll {
                symbol,
                market_type,
            } => ExecuteAdmission::CancelAll {
                symbol: symbol.clone(),
                market_type: *market_type,
            },
            TradingCommand::Cancel { .. } => ExecuteAdmission::CancelOne,
        };

        match execute_admission {
            ExecuteAdmission::Submit(pending) => {
                let mut state = self
                    .admission_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(error) = state.quarantine_error() {
                    return Err(error);
                }
                let pending_submit_id = state.register(pending)?;
                let result = self.send(
                    Request::Execute {
                        command,
                        pending_submit_id: Some(pending_submit_id),
                        deadlines,
                        response,
                        _admission: admission,
                    },
                    AdmissionLane::Normal,
                );
                if result.is_err() {
                    state.pending_submissions.remove(&pending_submit_id);
                }
                result
            }
            ExecuteAdmission::CancelAll {
                symbol,
                market_type,
            } => {
                let mut state = self
                    .admission_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let result = self.send(
                    Request::Execute {
                        command,
                        pending_submit_id: None,
                        deadlines,
                        response,
                        _admission: admission,
                    },
                    AdmissionLane::CancelAll,
                );
                if result.is_ok() {
                    state.mark_cancelled(symbol.as_ref(), market_type);
                }
                result
            }
            ExecuteAdmission::CancelOne => self.send(
                Request::Execute {
                    command,
                    pending_submit_id: None,
                    deadlines,
                    response,
                    _admission: admission,
                },
                AdmissionLane::Cancellation,
            ),
        }
    }

    async fn execute_with_deadlines(
        &self,
        command: TradingCommand,
        operation_deadline: Instant,
        dispatch_before: Instant,
    ) -> Result<TradingReceipt, ExchangeError> {
        let lane = match &command {
            TradingCommand::Submit(_) => AdmissionLane::Normal,
            TradingCommand::Cancel { .. } => AdmissionLane::Cancellation,
            TradingCommand::CancelAll { .. } => AdmissionLane::CancelAll,
        };
        let permit = self.try_admit(lane)?;
        let operation = command.operation();
        let client_order_id = command.client_order_id();
        let operation_key = Some(command.operation_key());
        let (response, receiver) = oneshot::channel();
        self.send_execute(
            command,
            ExecuteDeadlines {
                operation_deadline,
                dispatch_before,
            },
            response,
            permit.clone(),
        )?;
        match timeout_at(operation_deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ExchangeError::AmbiguousOutcome {
                operation,
                client_order_id,
                operation_key: operation_key.clone(),
                reason: "request was accepted locally but the adapter response was lost".to_owned(),
            }),
            Err(_) => {
                permit.release();
                Err(ExchangeError::AmbiguousOutcome {
                    operation,
                    client_order_id,
                    operation_key,
                    reason: "adapter operation exceeded its admission deadline".to_owned(),
                })
            }
        }
    }

    fn deadline(&self) -> Result<Instant, ExchangeError> {
        Instant::now()
            .checked_add(self.operation_timeout)
            .ok_or_else(|| ExchangeError::invalid("exchange operation timeout is too large"))
    }
}

#[async_trait]
impl ExchangeHandle for BoundedExchangeHandle {
    async fn execute(&self, command: TradingCommand) -> Result<TradingReceipt, ExchangeError> {
        let operation_deadline = self.deadline()?;
        self.execute_with_deadlines(command, operation_deadline, operation_deadline)
            .await
    }

    async fn execute_before(
        &self,
        command: TradingCommand,
        dispatch_before: Instant,
    ) -> Result<TradingReceipt, ExchangeError> {
        if Instant::now() >= dispatch_before {
            return Err(ExchangeError::rejected(
                "trading command expired before adapter dispatch",
            ));
        }
        let operation_deadline = self.deadline()?;
        self.execute_with_deadlines(command, operation_deadline, dispatch_before)
            .await
    }

    async fn reconcile(&self, scope: ReconcileScope) -> Result<ReconcileReceipt, ExchangeError> {
        let permit = self.try_admit(AdmissionLane::Reconciliation)?;
        let deadline = self.deadline()?;
        let (response, receiver) = oneshot::channel();
        self.send(
            Request::Reconcile {
                scope,
                deadline,
                response,
                _admission: permit.clone(),
            },
            AdmissionLane::Reconciliation,
        )?;
        match timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ExchangeError::unavailable(
                "reconciliation response was lost",
            )),
            Err(_) => {
                permit.release();
                Err(ExchangeError::unavailable(
                    "exchange reconciliation exceeded its admission deadline",
                ))
            }
        }
    }

    async fn subscribe(
        &self,
        subscription: MarketSubscription,
    ) -> Result<SubscriptionReceipt, ExchangeError> {
        let permit = self.try_admit(AdmissionLane::Normal)?;
        let deadline = self.deadline()?;
        let (response, receiver) = oneshot::channel();
        self.send(
            Request::Subscribe {
                subscription,
                deadline,
                response,
                _admission: permit.clone(),
            },
            AdmissionLane::Normal,
        )?;
        match timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ExchangeError::unavailable("subscription response was lost")),
            Err(_) => {
                permit.release();
                Err(ExchangeError::unavailable(
                    "exchange subscription exceeded its admission deadline",
                ))
            }
        }
    }

    async fn status(&self) -> Result<ExchangeStatus, ExchangeError> {
        let permit = self.try_admit(AdmissionLane::Normal)?;
        let deadline = self.deadline()?;
        let (response, receiver) = oneshot::channel();
        self.send(
            Request::Status {
                deadline,
                response,
                _admission: permit.clone(),
            },
            AdmissionLane::Normal,
        )?;
        match timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ExchangeError::unavailable("status response was lost")),
            Err(_) => {
                permit.release();
                Err(ExchangeError::unavailable(
                    "exchange status exceeded its admission deadline",
                ))
            }
        }
    }
}
