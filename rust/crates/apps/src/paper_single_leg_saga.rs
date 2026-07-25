//! Durable single-leg paper execution saga used by continuous grid owners.
//!
//! This is intentionally separate from the exact-two-leg arbitrage saga. A
//! grid crossing has one reservation, one intent, and one independently
//! recoverable outcome. Unknown or incomplete outcomes retain account
//! capacity until reconciliation proves that it is safe to release.

use std::{error::Error, fmt, future::Future, path::Path};

use chrono::Utc;
use crypto_trading_domain::{OrderIntent, OrderStatus, Quantity, Symbol};
use crypto_trading_exchange::{SubmissionDisposition, TradingReceipt};
use crypto_trading_runtime::{
    DecisionRecord, ExecutionBatch, HistoryError, JsonlHistory, PaperAccountAuthority,
    PaperAccountError, PaperReservationAdmission, PaperReservationPhase, PaperReservationRequest,
    PaperReservationView, RuntimeError,
};
use serde_json::{Value, json};

/// Stable version of the single-leg recovery context written to the journal.
pub const PAPER_SINGLE_LEG_SAGA_SCHEMA_VERSION: u16 = 1;

const SINGLE_LEG_COUNT: usize = 1;
const MAX_ERROR_SUMMARY_BYTES: usize = 2_048;

/// One independently identified single-leg paper operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaperSingleLegRequest {
    symbol: Symbol,
    batch: ExecutionBatch,
    reservation: PaperReservationRequest,
}

impl PaperSingleLegRequest {
    /// Creates one coherent single-leg execution and reservation pair.
    ///
    /// # Errors
    ///
    /// Returns [`PaperSingleLegSagaError::InvalidRequest`] before any durable
    /// write when the batch is not exactly one leg or identities disagree.
    pub fn new(
        symbol: Symbol,
        batch: ExecutionBatch,
        reservation: PaperReservationRequest,
    ) -> Result<Self, PaperSingleLegSagaError> {
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

    fn validate(&self) -> Result<(), PaperSingleLegSagaError> {
        let intents = self.batch.intents();
        let legs = self.reservation.legs();
        if intents.len() != SINGLE_LEG_COUNT || legs.len() != SINGLE_LEG_COUNT {
            return Err(PaperSingleLegSagaError::InvalidRequest(
                "single-leg paper saga requires exactly one leg",
            ));
        }
        if self.reservation.batch_id() != self.batch.id() {
            return Err(PaperSingleLegSagaError::InvalidRequest(
                "paper reservation batch id does not match execution batch",
            ));
        }
        let intent = &intents[0];
        let leg = &legs[0];
        if intent.symbol != self.symbol
            || leg.index() != 0
            || leg.exchange() != intent.exchange
            || leg.symbol() != &intent.symbol
            || leg.market_type() != intent.market_type
            || leg.side() != intent.side
        {
            return Err(PaperSingleLegSagaError::InvalidRequest(
                "paper reservation leg does not match execution intent",
            ));
        }
        Ok(())
    }
}

/// Journal-first owner for one single-leg paper operation.
#[derive(Clone, Debug)]
pub struct DurablePaperSingleLegSaga {
    account: PaperAccountAuthority,
    history: JsonlHistory,
}

impl DurablePaperSingleLegSaga {
    /// Binds the account authority and execution saga to the same journal.
    ///
    /// # Errors
    ///
    /// Returns [`PaperSingleLegSagaError::InvalidRequest`] for mismatched
    /// journal paths.
    pub fn new(
        account: PaperAccountAuthority,
        history: JsonlHistory,
    ) -> Result<Self, PaperSingleLegSagaError> {
        if account.history_path() != history.path() {
            return Err(PaperSingleLegSagaError::InvalidRequest(
                "paper account and single-leg saga must share one journal",
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

    /// Runs one new operation or classifies an existing durable reservation.
    ///
    /// Existing nonterminal reservations never cross the execution seam
    /// automatically.
    ///
    /// # Errors
    ///
    /// Returns a typed account, journal, execution, or recovery error while
    /// preserving uncertain capacity.
    pub async fn run<F, Fut>(
        &self,
        request: PaperSingleLegRequest,
        execute: F,
    ) -> Result<PaperSingleLegRun, PaperSingleLegSagaError>
    where
        F: FnOnce(ExecutionBatch) -> Fut,
        Fut: Future<Output = Result<Vec<TradingReceipt>, RuntimeError>>,
    {
        request.validate()?;
        let admission = self.account.reserve(request.reservation.clone()).await?;
        let reservation = match admission {
            PaperReservationAdmission::Reserved(reservation) => reservation,
            PaperReservationAdmission::Existing(reservation) => {
                return classify_existing(&reservation);
            }
        };

        append_planned(
            &self.history,
            &request,
            &reservation,
            self.account.config().account_id(),
        )
        .await?;
        let result = execute(request.batch.clone()).await;
        self.finish_new(&request, &reservation, result).await
    }

    async fn finish_new(
        &self,
        request: &PaperSingleLegRequest,
        reservation: &PaperReservationView,
        result: Result<Vec<TradingReceipt>, RuntimeError>,
    ) -> Result<PaperSingleLegRun, PaperSingleLegSagaError> {
        match result {
            Ok(receipts)
                if receipts_confirm_batch(
                    request.batch(),
                    &receipts,
                    SubmissionDisposition::Filled,
                ) =>
            {
                append_outcome(
                    &self.history,
                    request.symbol(),
                    "execution_completed",
                    outcome_details(request.batch(), &receipts),
                )
                .await?;
                let confirmed = request.reservation().gross_notional()?;
                self.account
                    .commit(reservation.reservation_id, confirmed)
                    .await?;
                Ok(PaperSingleLegRun::Completed { receipts })
            }
            Ok(receipts)
                if receipts_confirm_batch(
                    request.batch(),
                    &receipts,
                    SubmissionDisposition::Cancelled,
                ) =>
            {
                append_outcome(
                    &self.history,
                    request.symbol(),
                    "execution_incomplete",
                    outcome_details(request.batch(), &receipts),
                )
                .await?;
                self.account
                    .release(reservation.reservation_id, "single_leg_cancelled")
                    .await?;
                Ok(PaperSingleLegRun::Cancelled { receipts })
            }
            Ok(receipts) => {
                append_outcome(
                    &self.history,
                    request.symbol(),
                    "execution_incomplete",
                    outcome_details(request.batch(), &receipts),
                )
                .await?;
                self.account
                    .mark_uncertain(reservation.reservation_id)
                    .await?;
                Err(PaperSingleLegSagaError::Incomplete(receipts))
            }
            Err(error) => {
                append_outcome(
                    &self.history,
                    request.symbol(),
                    "execution_failed",
                    json!({
                        "batch_id": request.batch().id(),
                        "error": bounded_error(error.to_string()),
                    }),
                )
                .await?;
                self.account
                    .mark_uncertain(reservation.reservation_id)
                    .await?;
                Err(PaperSingleLegSagaError::Execution(error))
            }
        }
    }
}

fn classify_existing(
    reservation: &PaperReservationView,
) -> Result<PaperSingleLegRun, PaperSingleLegSagaError> {
    match reservation.phase {
        PaperReservationPhase::Committed | PaperReservationPhase::Released => {
            Ok(PaperSingleLegRun::AlreadyTerminal {
                batch_id: reservation.batch_id.to_string(),
                phase: reservation.phase,
            })
        }
        PaperReservationPhase::Pending | PaperReservationPhase::Uncertain => {
            Err(PaperSingleLegSagaError::RecoveryRequired {
                batch_id: reservation.batch_id.to_string(),
                phase: reservation.phase,
            })
        }
    }
}

async fn append_planned(
    history: &JsonlHistory,
    request: &PaperSingleLegRequest,
    reservation: &PaperReservationView,
    account_id: &str,
) -> Result<(), PaperSingleLegSagaError> {
    let intent = &request.batch.intents()[0];
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "grid".to_owned(),
            symbol: request.symbol.to_string(),
            decision: "execution_planned".to_owned(),
            details: json!({
                "batch_id": request.batch.id(),
                "legs": [intent_summary(0, intent)],
                "recovery_batch": request.batch,
                "context": {
                    "paper_single_leg_saga": {
                        "schema_version": PAPER_SINGLE_LEG_SAGA_SCHEMA_VERSION,
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
        .map_err(PaperSingleLegSagaError::Journal)
}

async fn append_outcome(
    history: &JsonlHistory,
    symbol: &Symbol,
    decision: &'static str,
    details: Value,
) -> Result<(), PaperSingleLegSagaError> {
    history
        .append(&DecisionRecord {
            timestamp: Utc::now(),
            strategy: "grid".to_owned(),
            symbol: symbol.to_string(),
            decision: decision.to_owned(),
            details,
        })
        .await
        .map_err(PaperSingleLegSagaError::Journal)
}

fn receipts_confirm_batch(
    batch: &ExecutionBatch,
    receipts: &[TradingReceipt],
    expected_disposition: SubmissionDisposition,
) -> bool {
    if batch.intents().len() != SINGLE_LEG_COUNT || receipts.len() != SINGLE_LEG_COUNT {
        return false;
    }
    let TradingReceipt::Submitted { order, disposition } = &receipts[0] else {
        return false;
    };
    let intent = &batch.intents()[0];
    if order.intent != *intent || *disposition != expected_disposition {
        return false;
    }
    match expected_disposition {
        SubmissionDisposition::Filled => {
            order.status == OrderStatus::Filled && order.filled_quantity == intent.quantity
        }
        SubmissionDisposition::Cancelled => {
            order.status == OrderStatus::Cancelled && order.filled_quantity == Quantity::default()
        }
        SubmissionDisposition::Open | SubmissionDisposition::AlreadyProcessed => false,
    }
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

fn outcome_details(batch: &ExecutionBatch, receipts: &[TradingReceipt]) -> Value {
    json!({
        "batch_id": batch.id(),
        "receipt_count": receipts.len(),
        "receipts": receipts,
        "receipts_truncated": false,
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

/// Terminal result of one single-leg saga run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaperSingleLegRun {
    Completed {
        receipts: Vec<TradingReceipt>,
    },
    Cancelled {
        receipts: Vec<TradingReceipt>,
    },
    AlreadyTerminal {
        batch_id: String,
        phase: PaperReservationPhase,
    },
}

/// Fail-closed errors for single-leg paper execution.
#[derive(Debug)]
pub enum PaperSingleLegSagaError {
    InvalidRequest(&'static str),
    RecoveryRequired {
        batch_id: String,
        phase: PaperReservationPhase,
    },
    Account(PaperAccountError),
    Journal(HistoryError),
    Execution(RuntimeError),
    Incomplete(Vec<TradingReceipt>),
}

impl From<PaperAccountError> for PaperSingleLegSagaError {
    fn from(value: PaperAccountError) -> Self {
        Self::Account(value)
    }
}

impl fmt::Display for PaperSingleLegSagaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(reason) => {
                write!(formatter, "invalid single-leg paper request: {reason}")
            }
            Self::RecoveryRequired { batch_id, phase } => write!(
                formatter,
                "single-leg paper batch {batch_id} is {phase:?}; reconciliation is required"
            ),
            Self::Account(error) => error.fmt(formatter),
            Self::Journal(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
            Self::Incomplete(receipts) => write!(
                formatter,
                "single-leg paper execution returned {} unproven receipt(s)",
                receipts.len()
            ),
        }
    }
}

impl Error for PaperSingleLegSagaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Account(error) => Some(error),
            Self::Journal(error) => Some(error),
            Self::Execution(error) => Some(error),
            Self::InvalidRequest(_) | Self::RecoveryRequired { .. } | Self::Incomplete(_) => None,
        }
    }
}
