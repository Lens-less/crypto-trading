use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    ExecutionBatch, JournalPageBoundary, JournalReadError, JournalSnapshot,
    LegacyJsonlJournalReader, OperationEventEnvelope,
};

pub const OPERATOR_READ_MODEL_SCHEMA_VERSION: u16 = 1;
pub const MAX_OPERATOR_READ_MODEL_BATCHES: usize = 256;
pub const MAX_OPERATOR_READ_MODEL_WARNINGS: usize = 256;

const MAX_VIEW_TEXT_BYTES: usize = 256;
const MAX_WARNING_DETAIL_BYTES: usize = 512;
const EXECUTION_AGGREGATE_KIND: &str = "execution_batch";
const LEGACY_EVENT_PRODUCER: &str = "legacy_jsonl";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionStatus {
    Complete,
    Windowed,
    Degraded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBatchState {
    OutcomeUnknown,
    Completed,
    Partial,
    Incomplete,
    Failed,
    Conflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDirective {
    None,
    ReconcileRequired,
    Investigate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPhase {
    Planned,
    Completed,
    Partial,
    Incomplete,
    Failed,
}

impl ExecutionPhase {
    fn from_kind(kind: &str) -> Option<Self> {
        match kind {
            "execution_planned" => Some(Self::Planned),
            "execution_completed" => Some(Self::Completed),
            "execution_partial" => Some(Self::Partial),
            "execution_incomplete" => Some(Self::Incomplete),
            "execution_failed" => Some(Self::Failed),
            _ => None,
        }
    }

    const fn is_terminal(self) -> bool {
        !matches!(self, Self::Planned)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadModelWarningCode {
    ConflictingDuplicate,
    DuplicateIgnored,
    InvalidExecutionEvent,
    MetadataConflict,
    OrphanOutcome,
    OutOfOrderPlanned,
    PartialTail,
    ResolvedBatchEvicted,
    TerminalConflict,
    TimestampRegressed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadModelWarning {
    pub code: ReadModelWarningCode,
    pub sequence: Option<u64>,
    pub event_id: Option<Uuid>,
    pub batch_id: Option<Uuid>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBatchView {
    pub batch_id: Uuid,
    pub strategy: String,
    pub symbol: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub first_seen_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub planned_at: Option<DateTime<Utc>>,
    pub outcome_at: Option<DateTime<Utc>>,
    pub state: ExecutionBatchState,
    pub recovery: RecoveryDirective,
    pub status_summary: String,
    pub leg_count: Option<usize>,
    pub receipt_count: Option<usize>,
    pub expected_receipt_count: Option<usize>,
    pub failed_index: Option<usize>,
    pub unattempted_count: Option<usize>,
    pub reconciliation_observation_count: Option<usize>,
    pub reconciliation_error_count: Option<usize>,
    pub failure_recorded: bool,
    pub phases: Vec<ExecutionPhase>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub head_sequence: Option<u64>,
    pub head_event_id: Option<Uuid>,
    pub projection_status: ProjectionStatus,
    pub batches: Vec<ExecutionBatchView>,
    pub batches_truncated: bool,
    pub warnings: Vec<ReadModelWarning>,
    pub warnings_truncated: bool,
}

impl OperatorReadModel {
    /// Builds a bounded operator snapshot from one immutable legacy snapshot.
    ///
    /// Non-execution decisions are ignored. A final partial line produces a
    /// degraded snapshot at the last complete event; malformed middle records
    /// remain hard reader errors and are never skipped.
    ///
    /// # Errors
    ///
    /// Returns [`ReadModelError`] for journal failures, non-advancing pages, or
    /// a batch cardinality that cannot be represented without dropping facts.
    pub fn from_legacy_snapshot(snapshot: &JournalSnapshot) -> Result<Self, ReadModelError> {
        ProjectionBuilder::new(snapshot.journal_id()).project(snapshot)
    }
}

struct ProjectionBuilder {
    journal_id: Uuid,
    head_sequence: Option<u64>,
    head_event_id: Option<Uuid>,
    projection_status: ProjectionStatus,
    batches: Vec<BatchAccumulator>,
    warnings: Vec<ReadModelWarning>,
    batches_truncated: bool,
    warnings_truncated: bool,
}

impl ProjectionBuilder {
    fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            head_sequence: None,
            head_event_id: None,
            projection_status: ProjectionStatus::Complete,
            batches: Vec::new(),
            warnings: Vec::new(),
            batches_truncated: false,
            warnings_truncated: false,
        }
    }

    fn project(mut self, snapshot: &JournalSnapshot) -> Result<OperatorReadModel, ReadModelError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            if let Some(event) = page.events().last() {
                self.head_sequence = Some(event.sequence());
                self.head_event_id = Some(event.event_id());
            }
            for event in page.events() {
                self.apply_event(event)?;
            }

            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { offset, bytes } => {
                    self.projection_status = ProjectionStatus::Degraded;
                    self.push_warning(ReadModelWarning {
                        code: ReadModelWarningCode::PartialTail,
                        sequence: self.head_sequence.map(|value| value.saturating_add(1)),
                        event_id: None,
                        batch_id: None,
                        detail: bounded_detail(format!(
                            "ignored {bytes} trailing byte(s) at offset {offset}; projection stops at the last complete record"
                        )),
                    });
                    break;
                }
                JournalPageBoundary::PageLimit => {
                    let next = page
                        .next_cursor()
                        .cloned()
                        .ok_or(ReadModelError::NonAdvancingPage)?;
                    if cursor.as_ref().is_some_and(|previous| {
                        previous.next_offset() == next.next_offset()
                            && previous.after_sequence() == next.after_sequence()
                    }) {
                        return Err(ReadModelError::NonAdvancingPage);
                    }
                    cursor = Some(next);
                }
            }
        }

        Ok(OperatorReadModel {
            schema_version: OPERATOR_READ_MODEL_SCHEMA_VERSION,
            journal_id: self.journal_id,
            head_sequence: self.head_sequence,
            head_event_id: self.head_event_id,
            projection_status: self.projection_status,
            batches: self
                .batches
                .into_iter()
                .map(|accumulator| accumulator.view)
                .collect(),
            batches_truncated: self.batches_truncated,
            warnings: self.warnings,
            warnings_truncated: self.warnings_truncated,
        })
    }

    fn apply_event(&mut self, event: &OperationEventEnvelope) -> Result<(), ReadModelError> {
        let Some(phase) = ExecutionPhase::from_kind(event.kind()) else {
            return Ok(());
        };
        match ParsedExecutionEvent::parse(event, phase) {
            Ok(parsed) => self.apply_parsed(parsed),
            Err(detail) => self.record_invalid_event(event, detail),
        }
    }

    fn apply_parsed(&mut self, parsed: ParsedExecutionEvent) -> Result<(), ReadModelError> {
        if let Some(index) = self
            .batches
            .iter()
            .position(|batch| batch.view.batch_id == parsed.batch_id)
        {
            let warnings = self.batches[index].apply(parsed);
            for warning in warnings {
                self.push_warning(warning);
            }
            return Ok(());
        }
        self.make_room_for_batch()?;
        let orphan = parsed.phase.is_terminal();
        if orphan {
            self.push_warning(parsed.warning(
                ReadModelWarningCode::OrphanOutcome,
                "terminal execution outcome has no preceding planned record; investigate before any action",
            ));
        }
        self.batches.push(BatchAccumulator::new(parsed));
        Ok(())
    }

    fn record_invalid_event(
        &mut self,
        event: &OperationEventEnvelope,
        detail: String,
    ) -> Result<(), ReadModelError> {
        self.projection_status = ProjectionStatus::Degraded;
        let batch_id = candidate_batch_id(event);
        if let Some(batch_id) = batch_id {
            if let Some(index) = self
                .batches
                .iter()
                .position(|batch| batch.view.batch_id == batch_id)
            {
                self.batches[index].touch_invalid(event);
            } else {
                self.make_room_for_batch()?;
                self.batches.push(BatchAccumulator::invalid(
                    batch_id,
                    candidate_text(event.payload(), "strategy"),
                    candidate_text(event.payload(), "symbol"),
                    event,
                ));
            }
        }
        self.push_warning(ReadModelWarning {
            code: ReadModelWarningCode::InvalidExecutionEvent,
            sequence: Some(event.sequence()),
            event_id: Some(event.event_id()),
            batch_id,
            detail: bounded_detail(detail),
        });
        Ok(())
    }

    fn make_room_for_batch(&mut self) -> Result<(), ReadModelError> {
        if self.batches.len() < MAX_OPERATOR_READ_MODEL_BATCHES {
            return Ok(());
        }
        let Some(index) = self
            .batches
            .iter()
            .enumerate()
            .filter(|(_, batch)| {
                batch.view.state == ExecutionBatchState::Completed
                    && batch.view.recovery == RecoveryDirective::None
            })
            .filter_map(|(index, batch)| {
                batch
                    .terminal_sequence
                    .map(|terminal_sequence| (index, terminal_sequence))
            })
            .min_by_key(|(_, terminal_sequence)| *terminal_sequence)
            .map(|(index, _)| index)
        else {
            return Err(ReadModelError::BatchLimitExceeded {
                limit: MAX_OPERATOR_READ_MODEL_BATCHES,
            });
        };
        let dropped = self.batches.remove(index);
        self.batches_truncated = true;
        if self.projection_status == ProjectionStatus::Complete {
            self.projection_status = ProjectionStatus::Windowed;
        }
        self.push_warning(ReadModelWarning {
            code: ReadModelWarningCode::ResolvedBatchEvicted,
            sequence: Some(dropped.view.last_sequence),
            event_id: None,
            batch_id: Some(dropped.view.batch_id),
            detail: "evicted the oldest durably completed batch to preserve the bounded recent window and every unresolved batch"
                .to_owned(),
        });
        Ok(())
    }

    fn push_warning(&mut self, warning: ReadModelWarning) {
        if self.warnings.len() < MAX_OPERATOR_READ_MODEL_WARNINGS {
            self.warnings.push(warning);
        } else {
            self.warnings_truncated = true;
            self.projection_status = ProjectionStatus::Degraded;
        }
    }
}

struct BatchAccumulator {
    view: ExecutionBatchView,
    planned_seen: bool,
    terminal_seen: Option<ExecutionPhase>,
    terminal_sequence: Option<u64>,
    fingerprints: Vec<PhaseFingerprint>,
}

impl BatchAccumulator {
    fn new(parsed: ParsedExecutionEvent) -> Self {
        let is_planned = parsed.phase == ExecutionPhase::Planned;
        let (state, recovery, summary) = if is_planned {
            (
                ExecutionBatchState::OutcomeUnknown,
                RecoveryDirective::ReconcileRequired,
                "outcome is not durably recorded; reconcile before any further action",
            )
        } else {
            (
                ExecutionBatchState::Conflict,
                RecoveryDirective::Investigate,
                "terminal outcome is orphaned from its execution plan; investigate before any action",
            )
        };
        let mut view = ExecutionBatchView {
            batch_id: parsed.batch_id,
            strategy: parsed.strategy,
            symbol: parsed.symbol,
            first_sequence: parsed.sequence,
            last_sequence: parsed.sequence,
            first_seen_at: parsed.recorded_at,
            updated_at: parsed.recorded_at,
            planned_at: is_planned.then_some(parsed.recorded_at),
            outcome_at: parsed.phase.is_terminal().then_some(parsed.recorded_at),
            state,
            recovery,
            status_summary: summary.to_owned(),
            leg_count: None,
            receipt_count: None,
            expected_receipt_count: None,
            failed_index: None,
            unattempted_count: None,
            reconciliation_observation_count: None,
            reconciliation_error_count: None,
            failure_recorded: false,
            phases: vec![parsed.phase],
        };
        apply_fact_fields(&mut view, &parsed.facts);
        Self {
            view,
            planned_seen: is_planned,
            terminal_seen: parsed.phase.is_terminal().then_some(parsed.phase),
            terminal_sequence: parsed.phase.is_terminal().then_some(parsed.sequence),
            fingerprints: vec![PhaseFingerprint {
                phase: parsed.phase,
                bytes: parsed.fingerprint,
            }],
        }
    }

    fn invalid(
        batch_id: Uuid,
        strategy: Option<String>,
        symbol: Option<String>,
        event: &OperationEventEnvelope,
    ) -> Self {
        Self {
            view: ExecutionBatchView {
                batch_id,
                strategy: strategy.unwrap_or_else(|| "<invalid>".to_owned()),
                symbol: symbol.unwrap_or_else(|| "<invalid>".to_owned()),
                first_sequence: event.sequence(),
                last_sequence: event.sequence(),
                first_seen_at: event.recorded_at(),
                updated_at: event.recorded_at(),
                planned_at: None,
                outcome_at: None,
                state: ExecutionBatchState::Conflict,
                recovery: RecoveryDirective::Investigate,
                status_summary: "execution history is invalid; investigate before any action"
                    .to_owned(),
                leg_count: None,
                receipt_count: None,
                expected_receipt_count: None,
                failed_index: None,
                unattempted_count: None,
                reconciliation_observation_count: None,
                reconciliation_error_count: None,
                failure_recorded: false,
                phases: Vec::new(),
            },
            planned_seen: false,
            terminal_seen: None,
            terminal_sequence: None,
            fingerprints: Vec::new(),
        }
    }

    fn apply(&mut self, parsed: ParsedExecutionEvent) -> Vec<ReadModelWarning> {
        let mut warnings = Vec::new();
        self.touch(
            parsed.sequence,
            parsed.recorded_at,
            parsed.event_id,
            &mut warnings,
        );

        if let Some(existing) = self
            .fingerprints
            .iter()
            .find(|existing| existing.phase == parsed.phase)
        {
            if existing.bytes == parsed.fingerprint {
                warnings.push(parsed.warning(
                    ReadModelWarningCode::DuplicateIgnored,
                    "ignored an exact duplicate execution phase",
                ));
            } else {
                self.mark_conflict(
                    "the same execution phase carries different durable content; investigate before any action",
                );
                warnings.push(parsed.warning(
                    ReadModelWarningCode::ConflictingDuplicate,
                    "the same execution phase was recorded with different content",
                ));
            }
            return warnings;
        }

        if self.view.strategy != parsed.strategy || self.view.symbol != parsed.symbol {
            self.mark_conflict(
                "execution metadata changed within one batch; investigate before any action",
            );
            warnings.push(parsed.warning(
                ReadModelWarningCode::MetadataConflict,
                "strategy or symbol changed within one execution batch",
            ));
            if parsed.phase.is_terminal() && self.terminal_seen.is_none() {
                self.terminal_seen = Some(parsed.phase);
            }
            self.record_phase(parsed, false);
            return warnings;
        }

        if parsed.phase == ExecutionPhase::Planned {
            self.planned_seen = true;
            if self.terminal_seen.is_some() || !self.view.phases.is_empty() {
                self.mark_conflict(
                    "an execution plan appeared after another durable batch fact; investigate before any action",
                );
                warnings.push(parsed.warning(
                    ReadModelWarningCode::OutOfOrderPlanned,
                    "execution_planned appeared after an earlier batch event",
                ));
            }
            self.record_phase(parsed, false);
            return warnings;
        }

        if !self.planned_seen {
            self.terminal_seen = Some(parsed.phase);
            self.mark_conflict(
                "terminal outcome has no preceding durable plan; investigate before any action",
            );
            warnings.push(parsed.warning(
                ReadModelWarningCode::OrphanOutcome,
                "terminal execution outcome has no preceding planned record",
            ));
            self.record_phase(parsed, false);
            return warnings;
        }
        if self.terminal_seen.is_some() {
            self.mark_conflict(
                "multiple terminal outcomes were recorded; investigate before any action",
            );
            warnings.push(parsed.warning(
                ReadModelWarningCode::TerminalConflict,
                "multiple terminal execution outcomes were recorded",
            ));
            self.record_phase(parsed, false);
            return warnings;
        }

        self.terminal_seen = Some(parsed.phase);
        if let Err(detail) = validate_terminal_against_plan(&self.view, &parsed.facts) {
            self.mark_conflict(
                "terminal outcome does not match the durable plan; investigate before any action",
            );
            warnings.push(parsed.warning(ReadModelWarningCode::TerminalConflict, detail));
            self.record_phase(parsed, false);
            return warnings;
        }

        if self.view.state != ExecutionBatchState::Conflict {
            let (state, recovery, summary) = terminal_state(parsed.phase);
            self.view.state = state;
            self.view.recovery = recovery;
            summary.clone_into(&mut self.view.status_summary);
        }
        self.record_phase(parsed, true);
        warnings
    }

    fn touch(
        &mut self,
        sequence: u64,
        recorded_at: DateTime<Utc>,
        event_id: Uuid,
        warnings: &mut Vec<ReadModelWarning>,
    ) {
        self.view.last_sequence = sequence;
        if recorded_at < self.view.updated_at {
            warnings.push(ReadModelWarning {
                code: ReadModelWarningCode::TimestampRegressed,
                sequence: Some(sequence),
                event_id: Some(event_id),
                batch_id: Some(self.view.batch_id),
                detail:
                    "event timestamp regressed; physical journal sequence remains authoritative"
                        .to_owned(),
            });
        } else {
            self.view.updated_at = recorded_at;
        }
    }

    fn touch_invalid(&mut self, event: &OperationEventEnvelope) {
        self.view.last_sequence = event.sequence();
        if event.recorded_at() > self.view.updated_at {
            self.view.updated_at = event.recorded_at();
        }
        self.mark_conflict("execution history is invalid; investigate before any action");
    }

    fn record_phase(&mut self, parsed: ParsedExecutionEvent, apply_facts: bool) {
        if apply_facts {
            if parsed.phase == ExecutionPhase::Planned && self.view.planned_at.is_none() {
                self.view.planned_at = Some(parsed.recorded_at);
            }
            if parsed.phase.is_terminal() && self.view.outcome_at.is_none() {
                self.view.outcome_at = Some(parsed.recorded_at);
                self.terminal_sequence = Some(parsed.sequence);
            }
            apply_fact_fields(&mut self.view, &parsed.facts);
        }
        self.view.phases.push(parsed.phase);
        self.fingerprints.push(PhaseFingerprint {
            phase: parsed.phase,
            bytes: parsed.fingerprint,
        });
    }

    fn mark_conflict(&mut self, summary: &str) {
        self.view.state = ExecutionBatchState::Conflict;
        self.view.recovery = RecoveryDirective::Investigate;
        summary.clone_into(&mut self.view.status_summary);
    }
}

struct PhaseFingerprint {
    phase: ExecutionPhase,
    bytes: Vec<u8>,
}

struct ParsedExecutionEvent {
    batch_id: Uuid,
    strategy: String,
    symbol: String,
    sequence: u64,
    event_id: Uuid,
    recorded_at: DateTime<Utc>,
    phase: ExecutionPhase,
    facts: PhaseFacts,
    fingerprint: Vec<u8>,
}

impl ParsedExecutionEvent {
    fn parse(event: &OperationEventEnvelope, phase: ExecutionPhase) -> Result<Self, String> {
        if event.producer() != LEGACY_EVENT_PRODUCER {
            return Err("unsupported execution event producer".to_owned());
        }
        let payload = require_object(event.payload(), "event payload")?;
        ensure_exact_fields(
            payload,
            &["decision", "details", "strategy", "symbol"],
            "event payload",
        )?;
        let decision = require_text(payload.get("decision"), "payload.decision")?;
        if decision != event.kind() {
            return Err("payload.decision does not match the event kind".to_owned());
        }
        let strategy = require_bounded_text(payload.get("strategy"), "payload.strategy")?;
        let symbol = require_bounded_text(payload.get("symbol"), "payload.symbol")?;
        let details = require_object(
            payload
                .get("details")
                .ok_or_else(|| "payload.details is missing".to_owned())?,
            "payload.details",
        )?;
        let batch_id = require_uuid(details.get("batch_id"), "details.batch_id")?;
        if event.aggregate().kind() != EXECUTION_AGGREGATE_KIND
            || event.aggregate().id() != batch_id
        {
            return Err("event aggregate does not match details.batch_id".to_owned());
        }
        let facts = PhaseFacts::parse(phase, details, batch_id)?;
        let fingerprint = serde_json::to_vec(&(event.recorded_at(), event.kind(), event.payload()))
            .map_err(|error| format!("failed to fingerprint execution event: {error}"))?;
        Ok(Self {
            batch_id,
            strategy,
            symbol,
            sequence: event.sequence(),
            event_id: event.event_id(),
            recorded_at: event.recorded_at(),
            phase,
            facts,
            fingerprint,
        })
    }

    fn warning(&self, code: ReadModelWarningCode, detail: impl Into<String>) -> ReadModelWarning {
        ReadModelWarning {
            code,
            sequence: Some(self.sequence),
            event_id: Some(self.event_id),
            batch_id: Some(self.batch_id),
            detail: bounded_detail(detail.into()),
        }
    }
}

enum PhaseFacts {
    Planned {
        leg_count: usize,
    },
    Completed {
        receipt_count: usize,
    },
    Partial {
        receipt_count: usize,
        failed_index: usize,
        unattempted_count: usize,
        reconciliation_observation_count: usize,
        reconciliation_error_count: usize,
    },
    Incomplete {
        receipt_count: usize,
        expected_receipt_count: usize,
    },
    Failed,
}

impl PhaseFacts {
    fn parse(
        phase: ExecutionPhase,
        details: &Map<String, Value>,
        batch_id: Uuid,
    ) -> Result<Self, String> {
        match phase {
            ExecutionPhase::Planned => parse_planned(details, batch_id),
            ExecutionPhase::Completed => parse_completed(details),
            ExecutionPhase::Partial => parse_partial(details, batch_id),
            ExecutionPhase::Incomplete => parse_incomplete(details),
            ExecutionPhase::Failed => parse_failed(details),
        }
    }
}

fn parse_planned(details: &Map<String, Value>, batch_id: Uuid) -> Result<PhaseFacts, String> {
    ensure_exact_fields(
        details,
        &["batch_id", "context", "legs", "recovery_batch"],
        "execution_planned details",
    )?;
    let legs = require_array(details.get("legs"), "details.legs")?;
    let recovery: ExecutionBatch = serde_json::from_value(
        details
            .get("recovery_batch")
            .ok_or_else(|| "details.recovery_batch is missing".to_owned())?
            .clone(),
    )
    .map_err(|error| format!("details.recovery_batch is invalid: {error}"))?;
    if recovery.id() != batch_id {
        return Err("recovery batch ID does not match details.batch_id".to_owned());
    }
    if recovery.intents().len() != legs.len() {
        return Err("details.legs length does not match recovery batch intents".to_owned());
    }
    if !details.contains_key("context") {
        return Err("details.context is missing".to_owned());
    }
    for (index, (leg, intent)) in legs.iter().zip(recovery.intents()).enumerate() {
        parse_intent_summary(leg, index, Some(intent.client_order_id))?;
    }
    Ok(PhaseFacts::Planned {
        leg_count: legs.len(),
    })
}

fn parse_completed(details: &Map<String, Value>) -> Result<PhaseFacts, String> {
    ensure_exact_fields(
        details,
        &[
            "already_processed",
            "batch_id",
            "cancelled",
            "filled",
            "open",
            "receipt_count",
            "receipts",
            "receipts_truncated",
        ],
        "execution_completed details",
    )?;
    Ok(PhaseFacts::Completed {
        receipt_count: parse_receipt_summary(details)?,
    })
}

fn parse_partial(details: &Map<String, Value>, batch_id: Uuid) -> Result<PhaseFacts, String> {
    ensure_exact_fields(
        details,
        &[
            "batch_id",
            "completed",
            "expected_batch_id",
            "failed_index",
            "failed_intent",
            "reconciliation",
            "source",
            "unattempted",
        ],
        "execution_partial details",
    )?;
    let expected_batch_id = require_uuid(
        details.get("expected_batch_id"),
        "details.expected_batch_id",
    )?;
    if expected_batch_id != batch_id {
        return Err("details.expected_batch_id does not match details.batch_id".to_owned());
    }
    let failed_index = require_usize(details.get("failed_index"), "details.failed_index")?;
    parse_intent_summary(
        details
            .get("failed_intent")
            .ok_or_else(|| "details.failed_intent is missing".to_owned())?,
        failed_index,
        None,
    )?;
    let unattempted = require_array(details.get("unattempted"), "details.unattempted")?;
    for (offset, intent) in unattempted.iter().enumerate() {
        let index = failed_index
            .checked_add(offset)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| "unattempted intent index overflowed".to_owned())?;
        parse_intent_summary(intent, index, None)?;
    }
    let reconciliation = require_array(details.get("reconciliation"), "details.reconciliation")?;
    let reconciliation_error_count =
        reconciliation
            .iter()
            .try_fold(0usize, |count, observation| {
                parse_reconciliation_status(observation).and_then(|is_error| {
                    count
                        .checked_add(usize::from(is_error))
                        .ok_or_else(|| "reconciliation error count overflowed".to_owned())
                })
            })?;
    require_nonempty_text(details.get("source"), "details.source")?;
    let completed = require_object(
        details
            .get("completed")
            .ok_or_else(|| "details.completed is missing".to_owned())?,
        "details.completed",
    )?;
    ensure_exact_fields(
        completed,
        &[
            "already_processed",
            "cancelled",
            "filled",
            "open",
            "receipt_count",
            "receipts",
            "receipts_truncated",
        ],
        "details.completed",
    )?;
    Ok(PhaseFacts::Partial {
        receipt_count: parse_receipt_summary(completed)?,
        failed_index,
        unattempted_count: unattempted.len(),
        reconciliation_observation_count: reconciliation.len(),
        reconciliation_error_count,
    })
}

fn parse_reconciliation_status(value: &Value) -> Result<bool, String> {
    let observation = require_object(value, "reconciliation observation")?;
    let exchange = require_nonempty_text(
        observation.get("exchange"),
        "reconciliation observation.exchange",
    )?;
    if exchange.len() > MAX_VIEW_TEXT_BYTES {
        return Err("reconciliation observation.exchange is too long".to_owned());
    }
    match require_text(
        observation.get("status"),
        "reconciliation observation.status",
    )? {
        "ok" => Ok(false),
        "error" => Ok(true),
        _ => Err("reconciliation observation.status must be ok or error".to_owned()),
    }
}

fn parse_incomplete(details: &Map<String, Value>) -> Result<PhaseFacts, String> {
    ensure_exact_fields(
        details,
        &[
            "already_processed",
            "batch_id",
            "cancelled",
            "expected_receipt_count",
            "filled",
            "open",
            "receipt_count",
            "receipts",
            "receipts_truncated",
        ],
        "execution_incomplete details",
    )?;
    Ok(PhaseFacts::Incomplete {
        receipt_count: parse_receipt_summary(details)?,
        expected_receipt_count: require_usize(
            details.get("expected_receipt_count"),
            "details.expected_receipt_count",
        )?,
    })
}

fn parse_failed(details: &Map<String, Value>) -> Result<PhaseFacts, String> {
    ensure_exact_fields(details, &["batch_id", "error"], "execution_failed details")?;
    require_nonempty_text(details.get("error"), "details.error")?;
    Ok(PhaseFacts::Failed)
}

fn parse_receipt_summary(summary: &Map<String, Value>) -> Result<usize, String> {
    let receipt_count = require_usize(summary.get("receipt_count"), "receipt_count")?;
    let receipts = require_array(summary.get("receipts"), "receipts")?;
    let truncated = require_bool(summary.get("receipts_truncated"), "receipts_truncated")?;
    let open = require_usize(summary.get("open"), "open")?;
    let filled = require_usize(summary.get("filled"), "filled")?;
    let cancelled = require_usize(summary.get("cancelled"), "cancelled")?;
    let already_processed = require_usize(summary.get("already_processed"), "already_processed")?;
    let counted = open
        .checked_add(filled)
        .and_then(|value| value.checked_add(cancelled))
        .and_then(|value| value.checked_add(already_processed))
        .ok_or_else(|| "receipt counters overflowed".to_owned())?;
    if counted != receipt_count {
        return Err("receipt counters do not sum to receipt_count".to_owned());
    }
    if receipts.len() > receipt_count {
        return Err("stored receipts exceed receipt_count".to_owned());
    }
    if (!truncated && receipts.len() != receipt_count)
        || (truncated && receipts.len() >= receipt_count)
    {
        return Err("receipts_truncated does not match stored receipt cardinality".to_owned());
    }
    Ok(receipt_count)
}

fn parse_intent_summary(
    value: &Value,
    expected_index: usize,
    expected_client_id: Option<Uuid>,
) -> Result<(), String> {
    let intent = require_object(value, "intent summary")?;
    ensure_exact_fields(
        intent,
        &[
            "client_order_id",
            "exchange",
            "index",
            "market_type",
            "order_type",
            "price",
            "quantity",
            "reduce_only",
            "side",
            "symbol",
            "time_in_force",
        ],
        "intent summary",
    )?;
    let index = require_usize(intent.get("index"), "intent.index")?;
    if index != expected_index {
        return Err("intent summary index is not contiguous".to_owned());
    }
    let client_id = require_uuid(intent.get("client_order_id"), "intent.client_order_id")?;
    if expected_client_id.is_some_and(|expected| expected != client_id) {
        return Err("intent client_order_id does not match recovery batch".to_owned());
    }
    Ok(())
}

fn validate_terminal_against_plan(
    view: &ExecutionBatchView,
    facts: &PhaseFacts,
) -> Result<(), String> {
    let leg_count = view
        .leg_count
        .ok_or_else(|| "terminal outcome has no validated planned leg count".to_owned())?;
    match facts {
        PhaseFacts::Completed { receipt_count } => {
            if *receipt_count != leg_count {
                return Err("completed receipt_count does not match planned leg count".to_owned());
            }
        }
        PhaseFacts::Partial {
            receipt_count,
            failed_index,
            unattempted_count,
            ..
        } => {
            if failed_index != receipt_count {
                return Err(
                    "partial failed_index does not match completed receipt count".to_owned(),
                );
            }
            let accounted = receipt_count
                .checked_add(1)
                .and_then(|value| value.checked_add(*unattempted_count))
                .ok_or_else(|| "partial leg accounting overflowed".to_owned())?;
            if accounted != leg_count {
                return Err("partial leg accounting does not match planned leg count".to_owned());
            }
        }
        PhaseFacts::Incomplete {
            receipt_count,
            expected_receipt_count,
        } => {
            if *expected_receipt_count != leg_count || receipt_count > expected_receipt_count {
                return Err(
                    "incomplete receipt counts do not match the planned leg count".to_owned(),
                );
            }
        }
        PhaseFacts::Failed => {}
        PhaseFacts::Planned { .. } => {
            return Err("planned facts cannot be used as a terminal outcome".to_owned());
        }
    }
    Ok(())
}

fn terminal_state(phase: ExecutionPhase) -> (ExecutionBatchState, RecoveryDirective, &'static str) {
    match phase {
        ExecutionPhase::Completed => (
            ExecutionBatchState::Completed,
            RecoveryDirective::None,
            "execution outcome is durably completed",
        ),
        ExecutionPhase::Partial => (
            ExecutionBatchState::Partial,
            RecoveryDirective::ReconcileRequired,
            "partial execution requires reconciliation before any further action",
        ),
        ExecutionPhase::Incomplete => (
            ExecutionBatchState::Incomplete,
            RecoveryDirective::ReconcileRequired,
            "incomplete execution requires reconciliation before any further action",
        ),
        ExecutionPhase::Failed => (
            ExecutionBatchState::Failed,
            RecoveryDirective::Investigate,
            "execution failure may have unknown side effects; investigate before any further action",
        ),
        ExecutionPhase::Planned => (
            ExecutionBatchState::OutcomeUnknown,
            RecoveryDirective::ReconcileRequired,
            "outcome is not durably recorded; reconcile before any further action",
        ),
    }
}

fn apply_fact_fields(view: &mut ExecutionBatchView, facts: &PhaseFacts) {
    match facts {
        PhaseFacts::Planned { leg_count } => view.leg_count = Some(*leg_count),
        PhaseFacts::Completed { receipt_count } => {
            view.receipt_count = Some(*receipt_count);
        }
        PhaseFacts::Partial {
            receipt_count,
            failed_index,
            unattempted_count,
            reconciliation_observation_count,
            reconciliation_error_count,
        } => {
            view.receipt_count = Some(*receipt_count);
            view.failed_index = Some(*failed_index);
            view.unattempted_count = Some(*unattempted_count);
            view.reconciliation_observation_count = Some(*reconciliation_observation_count);
            view.reconciliation_error_count = Some(*reconciliation_error_count);
            view.failure_recorded = true;
        }
        PhaseFacts::Incomplete {
            receipt_count,
            expected_receipt_count,
        } => {
            view.receipt_count = Some(*receipt_count);
            view.expected_receipt_count = Some(*expected_receipt_count);
        }
        PhaseFacts::Failed => view.failure_recorded = true,
    }
}

fn candidate_batch_id(event: &OperationEventEnvelope) -> Option<Uuid> {
    event
        .payload()
        .as_object()
        .and_then(|payload| payload.get("details"))
        .and_then(Value::as_object)
        .and_then(|details| details.get("batch_id"))
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .or_else(|| {
            (event.aggregate().kind() == EXECUTION_AGGREGATE_KIND)
                .then_some(event.aggregate().id())
                .filter(|value| !value.is_nil())
        })
}

fn candidate_text(payload: &Value, key: &str) -> Option<String> {
    payload
        .as_object()
        .and_then(|payload| payload.get(key))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= MAX_VIEW_TEXT_BYTES)
        .map(ToOwned::to_owned)
}

fn require_object<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{field} must be an object"))
}

fn require_array<'a>(
    value: Option<&'a Value>,
    field: &'static str,
) -> Result<&'a Vec<Value>, String> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{field} must be an array"))
}

fn require_text<'a>(value: Option<&'a Value>, field: &'static str) -> Result<&'a str, String> {
    value
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{field} must be a string"))
}

fn require_nonempty_text<'a>(
    value: Option<&'a Value>,
    field: &'static str,
) -> Result<&'a str, String> {
    let value = require_text(value, field)?;
    if value.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(value)
}

fn require_bounded_text(value: Option<&Value>, field: &'static str) -> Result<String, String> {
    let value = require_nonempty_text(value, field)?;
    if value.len() > MAX_VIEW_TEXT_BYTES {
        return Err(format!(
            "{field} has {} bytes; maximum is {MAX_VIEW_TEXT_BYTES}",
            value.len()
        ));
    }
    Ok(value.to_owned())
}

fn require_uuid(value: Option<&Value>, field: &'static str) -> Result<Uuid, String> {
    let value = require_text(value, field)?;
    let id = Uuid::parse_str(value).map_err(|_| format!("{field} must be a UUID"))?;
    if id.is_nil() {
        return Err(format!("{field} must not be nil"));
    }
    Ok(id)
}

fn require_usize(value: Option<&Value>, field: &'static str) -> Result<usize, String> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| format!("{field} must be a non-negative integer within platform bounds"))
}

fn require_bool(value: Option<&Value>, field: &'static str) -> Result<bool, String> {
    value
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("{field} must be a boolean"))
}

fn ensure_exact_fields(
    object: &Map<String, Value>,
    expected: &[&str],
    field: &'static str,
) -> Result<(), String> {
    if object.len() != expected.len() || !object.keys().all(|key| expected.contains(&key.as_str()))
    {
        return Err(format!("{field} has missing or unknown fields"));
    }
    Ok(())
}

fn bounded_detail(detail: String) -> String {
    if detail.len() <= MAX_WARNING_DETAIL_BYTES {
        return detail;
    }
    let mut end = MAX_WARNING_DETAIL_BYTES;
    while !detail.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}…", &detail[..end])
}

#[derive(Debug, Error)]
pub enum ReadModelError {
    #[error(transparent)]
    Journal(#[from] JournalReadError),
    #[error("operator read model cannot represent more than {limit} execution batches")]
    BatchLimitExceeded { limit: usize },
    #[error("task read model cannot represent more than {limit} distinct task identities")]
    TaskLimitExceeded { limit: usize },
    #[error("journal reader returned a non-advancing page")]
    NonAdvancingPage,
}
