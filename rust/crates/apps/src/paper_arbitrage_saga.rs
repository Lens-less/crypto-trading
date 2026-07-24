//! Exact-two-leg durable paper arbitrage execution saga.
//!
//! The saga composes the account reservation authority with the existing
//! execution journal contract. It deliberately performs one command only:
//! restart classification never submits, cancels, hedges, or retries.

use std::{error::Error, fmt, future::Future, path::Path};

use chrono::Utc;
use crypto_trading_domain::{OrderIntent, OrderStatus, Quantity, Side, Symbol};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    DecisionRecord, ExecutionBatch, ExecutionBatchState, FileJournalSnapshotSource, HistoryError,
    JournalReadError, JournalSnapshotSource, JsonlHistory, OperatorReadModel,
    PaperAccountAuthority, PaperAccountError, PaperReservationAdmission, PaperReservationPhase,
    PaperReservationRequest, PaperReservationView, ProjectionStatus, ReadModelError, RuntimeError,
};
use serde_json::{Value, json};

pub const PAPER_ARBITRAGE_SAGA_SCHEMA_VERSION: u16 = 1;

const EXACT_LEG_COUNT: usize = 2;
const MAX_ERROR_SUMMARY_BYTES: usize = 2_048;
const MAX_RECEIPT_SUMMARY_RECEIPTS: usize = 64;
const MAX_RECONCILIATION_SUMMARY_ORDERS: usize = 64;
const MAX_RECONCILIATION_SUMMARY_POSITIONS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaperArbitrageRequest {
    symbol: Symbol,
    batch: ExecutionBatch,
    reservation: PaperReservationRequest,
}

impl PaperArbitrageRequest {
    /// Creates one exact cross-exchange buy/sell pair bound to one reservation.
    ///
    /// # Errors
    ///
    /// Returns [`PaperArbitrageSagaError::InvalidRequest`] before any journal
    /// or account authority is selected when the pair is not exact or coherent.
    pub fn new(
        symbol: Symbol,
        batch: ExecutionBatch,
        reservation: PaperReservationRequest,
    ) -> Result<Self, PaperArbitrageSagaError> {
        let request = Self {
            symbol,
            batch,
            reservation,
        };
        request.validate()?;
        Ok(request)
    }

    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    #[must_use]
    pub const fn batch(&self) -> &ExecutionBatch {
        &self.batch
    }

    #[must_use]
    pub const fn reservation(&self) -> &PaperReservationRequest {
        &self.reservation
    }

    fn validate(&self) -> Result<(), PaperArbitrageSagaError> {
        let intents = self.batch.intents();
        let legs = self.reservation.legs();
        if intents.len() != EXACT_LEG_COUNT || legs.len() != EXACT_LEG_COUNT {
            return Err(PaperArbitrageSagaError::InvalidRequest(
                "paper arbitrage saga requires exactly two legs",
            ));
        }
        if self.reservation.batch_id() != self.batch.id() {
            return Err(PaperArbitrageSagaError::InvalidRequest(
                "paper reservation batch id does not match execution batch",
            ));
        }
        if intents[0].exchange == intents[1].exchange {
            return Err(PaperArbitrageSagaError::InvalidRequest(
                "paper arbitrage legs must target distinct exchanges",
            ));
        }
        if !matches!(
            (intents[0].side, intents[1].side),
            (Side::Buy, Side::Sell) | (Side::Sell, Side::Buy)
        ) {
            return Err(PaperArbitrageSagaError::InvalidRequest(
                "paper arbitrage legs must contain one buy and one sell",
            ));
        }
        for (index, (intent, leg)) in intents.iter().zip(legs).enumerate() {
            if intent.symbol != self.symbol
                || leg.index() != index
                || leg.exchange() != intent.exchange
                || leg.symbol() != &intent.symbol
                || leg.market_type() != intent.market_type
                || leg.side() != intent.side
            {
                return Err(PaperArbitrageSagaError::InvalidRequest(
                    "paper reservation legs do not match execution intents",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DurablePaperArbitrageSaga {
    account: PaperAccountAuthority,
    history: JsonlHistory,
}

impl DurablePaperArbitrageSaga {
    /// Binds one account authority and execution journal.
    ///
    /// # Errors
    ///
    /// Returns [`PaperArbitrageSagaError::InvalidRequest`] when the paths do
    /// not identify the same process-local journal.
    pub fn new(
        account: PaperAccountAuthority,
        history: JsonlHistory,
    ) -> Result<Self, PaperArbitrageSagaError> {
        if account.history_path() != history.path() {
            return Err(PaperArbitrageSagaError::InvalidRequest(
                "paper account and execution saga must share one journal",
            ));
        }
        Ok(Self { account, history })
    }

    #[must_use]
    pub const fn account(&self) -> &PaperAccountAuthority {
        &self.account
    }

    #[must_use]
    pub fn history_path(&self) -> &Path {
        self.history.path()
    }

    /// Runs one journal-first exact-pair paper command.
    ///
    /// The execution callback is invoked only for a newly durable reservation
    /// after `execution_planned` is synchronized. Existing requests are
    /// classified from durable facts and never execute automatically.
    ///
    /// # Errors
    ///
    /// Returns a structured durable-state, journal, account, incomplete, or
    /// execution error while retaining any confirmed receipts.
    pub async fn run<F, Fut>(
        &self,
        request: PaperArbitrageRequest,
        execute: F,
    ) -> Result<PaperArbitrageRun, PaperArbitrageSagaError>
    where
        F: FnOnce(ExecutionBatch) -> Fut,
        Fut: Future<Output = Result<Vec<TradingReceipt>, RuntimeError>>,
    {
        request.validate()?;
        let admission = self.account.reserve(request.reservation.clone()).await?;
        let reservation = match admission {
            PaperReservationAdmission::Reserved(reservation) => reservation,
            PaperReservationAdmission::Existing(reservation) => {
                return self.classify_existing(&reservation).await;
            }
        };

        append_execution_planned(
            &self.history,
            &request,
            &reservation,
            self.account.config().account_id(),
        )
        .await?;
        let result = execute(request.batch.clone()).await;
        self.finish_new(request, reservation, result).await
    }

    async fn classify_existing(
        &self,
        reservation: &PaperReservationView,
    ) -> Result<PaperArbitrageRun, PaperArbitrageSagaError> {
        let source =
            FileJournalSnapshotSource::new(self.account.journal_id(), self.history.path())?;
        let snapshot = tokio::task::spawn_blocking(move || source.snapshot())
            .await
            .map_err(|_| PaperArbitrageSagaError::SnapshotTaskFailed)??;
        let model = OperatorReadModel::from_legacy_snapshot(&snapshot)
            .map_err(PaperArbitrageSagaError::Projection)?;
        if model.projection_status != ProjectionStatus::Complete {
            return Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::Conflict,
            ));
        }
        let Some(batch) = model
            .batches
            .iter()
            .find(|batch| batch.batch_id == reservation.batch_id)
        else {
            return Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::ReservedOnly,
            ));
        };
        match batch.state {
            ExecutionBatchState::Completed => {
                if matches!(
                    reservation.phase,
                    PaperReservationPhase::Committed | PaperReservationPhase::Released
                ) {
                    Ok(PaperArbitrageRun::AlreadyCompleted {
                        batch_id: reservation.batch_id.to_string(),
                    })
                } else {
                    Err(recovery_error(
                        reservation,
                        PaperArbitrageRecoveryStage::AccountFinalizationRequired,
                    ))
                }
            }
            ExecutionBatchState::OutcomeUnknown => Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::OutcomeUnknown,
            )),
            ExecutionBatchState::Partial => Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::Partial,
            )),
            ExecutionBatchState::Incomplete => Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::Incomplete,
            )),
            ExecutionBatchState::Failed => Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::Failed,
            )),
            ExecutionBatchState::Conflict => Err(recovery_error(
                reservation,
                PaperArbitrageRecoveryStage::Conflict,
            )),
        }
    }

    async fn finish_new(
        &self,
        request: PaperArbitrageRequest,
        reservation: PaperReservationView,
        result: Result<Vec<TradingReceipt>, RuntimeError>,
    ) -> Result<PaperArbitrageRun, PaperArbitrageSagaError> {
        match result {
            Ok(receipts)
                if receipts_confirm_batch(
                    request.batch(),
                    &receipts,
                    SubmissionDisposition::Filled,
                ) =>
            {
                let batch_id = request.batch.id().to_string();
                let details = completed_details(&batch_id, &receipts);
                if let Err(source) = append_execution_outcome(
                    &self.history,
                    request.symbol(),
                    "execution_completed",
                    details,
                )
                .await
                {
                    return Err(PaperArbitrageSagaError::OutcomeJournal {
                        outcome: PaperArbitragePreservedOutcome::Completed(receipts),
                        source,
                    });
                }

                let committed = request.reservation.gross_notional()?;
                let finalization = self
                    .account
                    .commit(reservation.reservation_id, committed)
                    .await;
                if let Err(source) = finalization {
                    return Err(PaperArbitrageSagaError::AccountFinalization {
                        outcome: PaperArbitragePreservedOutcome::Completed(receipts),
                        source,
                    });
                }
                Ok(PaperArbitrageRun::Completed { receipts })
            }
            Ok(receipts) => {
                let mut details = receipt_summary(&receipts);
                details["batch_id"] = json!(request.batch.id());
                details["expected_receipt_count"] = json!(EXACT_LEG_COUNT);
                if let Err(source) = append_execution_outcome(
                    &self.history,
                    request.symbol(),
                    "execution_incomplete",
                    details,
                )
                .await
                {
                    return Err(PaperArbitrageSagaError::OutcomeJournal {
                        outcome: PaperArbitragePreservedOutcome::Incomplete(receipts),
                        source,
                    });
                }
                if let Err(source) = self
                    .finalize_incomplete_account(&request, &reservation, &receipts)
                    .await
                {
                    return Err(PaperArbitrageSagaError::AccountFinalization {
                        outcome: PaperArbitragePreservedOutcome::Incomplete(receipts),
                        source,
                    });
                }
                Err(PaperArbitrageSagaError::Incomplete(receipts))
            }
            Err(error) => {
                let (decision, details) =
                    execution_error_summary(&error, &request.batch.id().to_string());
                if let Err(source) =
                    append_execution_outcome(&self.history, request.symbol(), decision, details)
                        .await
                {
                    return Err(PaperArbitrageSagaError::OutcomeJournal {
                        outcome: PaperArbitragePreservedOutcome::Failed(error),
                        source,
                    });
                }
                if let Err(source) = self
                    .account
                    .mark_uncertain(reservation.reservation_id)
                    .await
                {
                    return Err(PaperArbitrageSagaError::AccountFinalization {
                        outcome: PaperArbitragePreservedOutcome::Failed(error),
                        source,
                    });
                }
                Err(PaperArbitrageSagaError::Execution(error))
            }
        }
    }

    async fn finalize_incomplete_account(
        &self,
        request: &PaperArbitrageRequest,
        reservation: &PaperReservationView,
        receipts: &[TradingReceipt],
    ) -> Result<PaperReservationView, PaperAccountError> {
        if receipts_confirm_batch(request.batch(), receipts, SubmissionDisposition::Cancelled) {
            self.account
                .release(reservation.reservation_id, "all_legs_cancelled")
                .await
        } else {
            self.account
                .mark_uncertain(reservation.reservation_id)
                .await
        }
    }
}

async fn append_execution_planned(
    history: &JsonlHistory,
    request: &PaperArbitrageRequest,
    reservation: &PaperReservationView,
    account_id: &str,
) -> Result<(), PaperArbitrageSagaError> {
    let legs = request
        .batch
        .intents()
        .iter()
        .enumerate()
        .map(|(index, intent)| intent_summary(index, intent))
        .collect::<Vec<_>>();
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "arbitrage".to_owned(),
            symbol: request.symbol.to_string(),
            decision: "execution_planned".to_owned(),
            details: json!({
                "batch_id": request.batch.id(),
                "legs": legs,
                "recovery_batch": request.batch,
                "context": {
                    "paper_arbitrage_saga": {
                        "schema_version": PAPER_ARBITRAGE_SAGA_SCHEMA_VERSION,
                        "account_id": account_id,
                        "reservation_id": reservation.reservation_id,
                        "task_id": reservation.task_id,
                        "idempotency_key": reservation.idempotency_key,
                        "cost_model_version": reservation.cost_model.version(),
                    }
                },
            }),
        })
        .await
        .map_err(PaperArbitrageSagaError::JournalWrite)
}

async fn append_execution_outcome(
    history: &JsonlHistory,
    symbol: &Symbol,
    decision: &str,
    details: Value,
) -> Result<(), HistoryError> {
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "arbitrage".to_owned(),
            symbol: symbol.to_string(),
            decision: decision.to_owned(),
            details,
        })
        .await
}

fn completed_details(batch_id: &str, receipts: &[TradingReceipt]) -> Value {
    let mut details = receipt_summary(receipts);
    details["batch_id"] = json!(batch_id);
    details
}

fn receipts_confirm_batch(
    batch: &ExecutionBatch,
    receipts: &[TradingReceipt],
    expected_disposition: SubmissionDisposition,
) -> bool {
    if receipts.len() != EXACT_LEG_COUNT || batch.intents().len() != EXACT_LEG_COUNT {
        return false;
    }
    let mut matched = [false; EXACT_LEG_COUNT];
    for receipt in receipts {
        let TradingReceipt::Submitted { order, disposition } = receipt else {
            return false;
        };
        if *disposition != expected_disposition {
            return false;
        }
        let Some((index, intent)) = batch
            .intents()
            .iter()
            .enumerate()
            .find(|(_, intent)| *intent == &order.intent)
        else {
            return false;
        };
        if matched[index] {
            return false;
        }
        let confirmed = match expected_disposition {
            SubmissionDisposition::Filled => {
                order.status == OrderStatus::Filled && order.filled_quantity == intent.quantity
            }
            SubmissionDisposition::Cancelled => {
                order.status == OrderStatus::Cancelled
                    && order.filled_quantity == Quantity::default()
            }
            SubmissionDisposition::Open | SubmissionDisposition::AlreadyProcessed => false,
        };
        if !confirmed {
            return false;
        }
        matched[index] = true;
    }
    matched.into_iter().all(std::convert::identity)
}

fn intent_summary(index: usize, intent: &OrderIntent) -> Value {
    json!({
        "index": index,
        "client_order_id": intent.client_order_id,
        "exchange": intent.exchange,
        "symbol": intent.symbol,
        "market_type": intent.market_type,
        "side": intent.side,
        "order_type": intent.order_type,
        "quantity": intent.quantity,
        "price": intent.price,
        "reduce_only": intent.reduce_only,
        "time_in_force": intent.time_in_force,
    })
}

fn execution_error_summary(error: &RuntimeError, expected_batch_id: &str) -> (&'static str, Value) {
    if let RuntimeError::PartialExecution {
        batch_id,
        failed_index,
        completed,
        failed_intent,
        unattempted,
        reconciliation,
        source,
    } = error
    {
        let reconciliation = reconciliation
            .iter()
            .map(|observation| match &observation.result {
                Ok(receipt) => {
                    let orders = receipt
                        .orders
                        .iter()
                        .take(MAX_RECONCILIATION_SUMMARY_ORDERS)
                        .collect::<Vec<_>>();
                    let positions = receipt
                        .positions
                        .iter()
                        .take(MAX_RECONCILIATION_SUMMARY_POSITIONS)
                        .collect::<Vec<_>>();
                    json!({
                        "exchange": observation.exchange,
                        "status": "ok",
                        "scope": receipt.scope,
                        "observed_at": receipt.observed_at,
                        "orders": orders,
                        "orders_total": receipt.orders.len(),
                        "orders_truncated": receipt.orders.len() > MAX_RECONCILIATION_SUMMARY_ORDERS,
                        "positions": positions,
                        "positions_total": receipt.positions.len(),
                        "positions_truncated": receipt.positions.len() > MAX_RECONCILIATION_SUMMARY_POSITIONS,
                    })
                }
                Err(error) => json!({
                    "exchange": observation.exchange,
                    "status": "error",
                    "error": bounded_error(error.to_string()),
                }),
            })
            .collect::<Vec<_>>();
        let unattempted = unattempted
            .iter()
            .enumerate()
            .map(|(index, intent)| intent_summary(failed_index + index + 1, intent))
            .collect::<Vec<_>>();
        return (
            "execution_partial",
            json!({
                "batch_id": batch_id,
                "expected_batch_id": expected_batch_id,
                "failed_index": failed_index,
                "completed": receipt_summary(completed),
                "failed_intent": intent_summary(*failed_index, failed_intent),
                "unattempted": unattempted,
                "reconciliation": reconciliation,
                "source": bounded_error(source.to_string()),
            }),
        );
    }
    (
        "execution_failed",
        json!({
            "batch_id": expected_batch_id,
            "error": bounded_error(error.to_string()),
        }),
    )
}

fn receipt_summary(receipts: &[TradingReceipt]) -> Value {
    let mut open = 0usize;
    let mut filled = 0usize;
    let mut cancelled = 0usize;
    let mut already_processed = 0usize;
    for receipt in receipts {
        match receipt.submission_disposition() {
            Some(SubmissionDisposition::Open) => open += 1,
            Some(SubmissionDisposition::Filled) => filled += 1,
            Some(SubmissionDisposition::Cancelled) | None => cancelled += 1,
            Some(SubmissionDisposition::AlreadyProcessed) => already_processed += 1,
        }
    }
    json!({
        "receipt_count": receipts.len(),
        "receipts": receipts
            .iter()
            .take(MAX_RECEIPT_SUMMARY_RECEIPTS)
            .collect::<Vec<_>>(),
        "receipts_truncated": receipts.len() > MAX_RECEIPT_SUMMARY_RECEIPTS,
        "open": open,
        "filled": filled,
        "cancelled": cancelled,
        "already_processed": already_processed,
    })
}

fn bounded_error(mut value: String) -> String {
    if value.len() <= MAX_ERROR_SUMMARY_BYTES {
        return value;
    }
    let mut boundary = MAX_ERROR_SUMMARY_BYTES;
    while !value.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    value.truncate(boundary);
    value
}

fn recovery_error(
    reservation: &PaperReservationView,
    stage: PaperArbitrageRecoveryStage,
) -> PaperArbitrageSagaError {
    PaperArbitrageSagaError::RecoveryRequired {
        batch_id: reservation.batch_id.to_string(),
        stage,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaperArbitrageRecoveryStage {
    ReservedOnly,
    OutcomeUnknown,
    Partial,
    Incomplete,
    Failed,
    AccountFinalizationRequired,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaperArbitrageRun {
    Completed { receipts: Vec<TradingReceipt> },
    AlreadyCompleted { batch_id: String },
}

#[derive(Debug)]
pub enum PaperArbitragePreservedOutcome {
    Completed(Vec<TradingReceipt>),
    Incomplete(Vec<TradingReceipt>),
    Failed(RuntimeError),
}

impl PaperArbitragePreservedOutcome {
    fn safe_summary(&self) -> String {
        match self {
            Self::Completed(receipts) => {
                format!("completed outcome with {} receipt(s)", receipts.len())
            }
            Self::Incomplete(receipts) => {
                format!("incomplete outcome with {} receipt(s)", receipts.len())
            }
            Self::Failed(_) => "failed outcome".to_owned(),
        }
    }
}

#[derive(Debug)]
pub enum PaperArbitrageSagaError {
    InvalidRequest(&'static str),
    RecoveryRequired {
        batch_id: String,
        stage: PaperArbitrageRecoveryStage,
    },
    Account(PaperAccountError),
    JournalRead(JournalReadError),
    Projection(ReadModelError),
    JournalWrite(HistoryError),
    Execution(RuntimeError),
    Incomplete(Vec<TradingReceipt>),
    OutcomeJournal {
        outcome: PaperArbitragePreservedOutcome,
        source: HistoryError,
    },
    AccountFinalization {
        outcome: PaperArbitragePreservedOutcome,
        source: PaperAccountError,
    },
    SnapshotTaskFailed,
}

impl From<PaperAccountError> for PaperArbitrageSagaError {
    fn from(value: PaperAccountError) -> Self {
        Self::Account(value)
    }
}

impl From<JournalReadError> for PaperArbitrageSagaError {
    fn from(value: JournalReadError) -> Self {
        Self::JournalRead(value)
    }
}

impl fmt::Display for PaperArbitrageSagaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(reason) => {
                write!(formatter, "invalid paper arbitrage request: {reason}")
            }
            Self::RecoveryRequired { batch_id, stage } => write!(
                formatter,
                "paper batch {batch_id} requires {stage:?} recovery; automatic retry is disabled"
            ),
            Self::Account(error) => error.fmt(formatter),
            Self::JournalRead(error) => error.fmt(formatter),
            Self::Projection(error) => error.fmt(formatter),
            Self::JournalWrite(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
            Self::Incomplete(receipts) => write!(
                formatter,
                "paper arbitrage returned {} receipt(s) for two legs; reconcile before any action",
                receipts.len()
            ),
            Self::OutcomeJournal { outcome, source } => write!(
                formatter,
                "paper execution produced {}, but its outcome journal failed: {source}",
                outcome.safe_summary()
            ),
            Self::AccountFinalization { outcome, source } => write!(
                formatter,
                "paper execution produced {}, but account finalization failed: {source}",
                outcome.safe_summary()
            ),
            Self::SnapshotTaskFailed => {
                formatter.write_str("paper recovery snapshot worker failed")
            }
        }
    }
}

impl Error for PaperArbitrageSagaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Account(error) | Self::AccountFinalization { source: error, .. } => Some(error),
            Self::JournalRead(error) => Some(error),
            Self::Projection(error) => Some(error),
            Self::JournalWrite(error) | Self::OutcomeJournal { source: error, .. } => Some(error),
            Self::Execution(error) => Some(error),
            Self::InvalidRequest(_)
            | Self::RecoveryRequired { .. }
            | Self::Incomplete(_)
            | Self::SnapshotTaskFailed => None,
        }
    }
}
