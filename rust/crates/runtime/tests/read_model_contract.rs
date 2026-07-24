use chrono::{TimeZone, Utc};
use crypto_trading_domain::{MarketType, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    ExecutionBatch, ExecutionBatchState, JournalReadError, JournalSnapshot,
    MAX_OPERATOR_READ_MODEL_BATCHES, OPERATOR_READ_MODEL_SCHEMA_VERSION, OperatorReadModel,
    ProjectionStatus, ReadModelError, ReadModelWarningCode, RecoveryDirective,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn lf_and_crlf_real_execution_fixtures_produce_the_same_snapshot() {
    let journal_id = fixed_uuid(1);
    let batch = test_batch(fixed_uuid(11), 0);
    let records = vec![
        execution_record(
            "execution_planned",
            &batch,
            fixed_time(0),
            planned_details(&batch, json!({"source": "test"})),
        ),
        execution_record(
            "execution_completed",
            &batch,
            fixed_time(1),
            completed_details(batch.id(), 0),
        ),
        json!({
            "timestamp": fixed_time(2),
            "strategy": "grid",
            "symbol": "BTC-USDT",
            "decision": "hold",
            "details": {},
        }),
    ];
    let lf = JournalSnapshot::new(journal_id, jsonl(&records, LineEnding::Lf)).unwrap();
    let crlf = JournalSnapshot::new(journal_id, jsonl(&records, LineEnding::CrLf)).unwrap();

    let lf_model = OperatorReadModel::from_legacy_snapshot(&lf).unwrap();
    let crlf_model = OperatorReadModel::from_legacy_snapshot(&crlf).unwrap();

    assert_eq!(lf_model, crlf_model);
    assert_eq!(lf_model.schema_version, OPERATOR_READ_MODEL_SCHEMA_VERSION);
    assert_eq!(lf_model.projection_status, ProjectionStatus::Complete);
    assert!(!lf_model.batches_truncated);
    assert_eq!(lf_model.head_sequence, Some(3));
    assert_eq!(lf_model.batches.len(), 1);
    assert_eq!(lf_model.batches[0].state, ExecutionBatchState::Completed);
    assert_eq!(lf_model.batches[0].recovery, RecoveryDirective::None);
    assert_eq!(lf_model.batches[0].leg_count, Some(0));
    assert_eq!(lf_model.batches[0].receipt_count, Some(0));
}

#[test]
fn planned_only_is_outcome_unknown_and_never_direct_retry_guidance() {
    let batch = test_batch(fixed_uuid(12), 1);
    let snapshot = snapshot(&[execution_record(
        "execution_planned",
        &batch,
        fixed_time(0),
        planned_details(&batch, json!({})),
    )]);

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot).unwrap();
    let view = &model.batches[0];

    assert_eq!(view.state, ExecutionBatchState::OutcomeUnknown);
    assert_eq!(view.recovery, RecoveryDirective::ReconcileRequired);
    assert!(!view.status_summary.to_ascii_lowercase().contains("retry"));
}

#[test]
fn partial_and_incomplete_outcomes_require_reconciliation() {
    let partial_batch = test_batch(fixed_uuid(21), 2);
    let incomplete_batch = test_batch(fixed_uuid(22), 2);
    let records = vec![
        execution_record(
            "execution_planned",
            &partial_batch,
            fixed_time(0),
            planned_details(&partial_batch, json!({})),
        ),
        execution_record(
            "execution_partial",
            &partial_batch,
            fixed_time(1),
            partial_details(&partial_batch, 0),
        ),
        execution_record(
            "execution_planned",
            &incomplete_batch,
            fixed_time(2),
            planned_details(&incomplete_batch, json!({})),
        ),
        execution_record(
            "execution_incomplete",
            &incomplete_batch,
            fixed_time(3),
            incomplete_details(incomplete_batch.id(), 1, 2),
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    let partial = batch_view(&model, partial_batch.id());
    assert_eq!(partial.state, ExecutionBatchState::Partial);
    assert_eq!(partial.recovery, RecoveryDirective::ReconcileRequired);
    assert_eq!(partial.receipt_count, Some(0));
    assert_eq!(partial.failed_index, Some(0));
    assert_eq!(partial.unattempted_count, Some(1));
    assert_eq!(partial.reconciliation_observation_count, Some(1));
    assert_eq!(partial.reconciliation_error_count, Some(1));
    assert!(partial.failure_recorded);
    assert!(
        !partial
            .status_summary
            .to_ascii_lowercase()
            .contains("retry")
    );

    let incomplete = batch_view(&model, incomplete_batch.id());
    assert_eq!(incomplete.state, ExecutionBatchState::Incomplete);
    assert_eq!(incomplete.recovery, RecoveryDirective::ReconcileRequired);
    assert_eq!(incomplete.receipt_count, Some(1));
    assert_eq!(incomplete.expected_receipt_count, Some(2));
    assert!(
        !incomplete
            .status_summary
            .to_ascii_lowercase()
            .contains("retry")
    );
}

#[test]
fn failed_outcome_is_visible_without_claiming_it_is_safe_to_retry() {
    let batch = test_batch(fixed_uuid(23), 1);
    let records = vec![
        execution_record(
            "execution_planned",
            &batch,
            fixed_time(0),
            planned_details(&batch, json!({})),
        ),
        execution_record(
            "execution_failed",
            &batch,
            fixed_time(1),
            failed_details(batch.id()),
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    let view = batch_view(&model, batch.id());

    assert_eq!(view.state, ExecutionBatchState::Failed);
    assert_eq!(view.recovery, RecoveryDirective::Investigate);
    assert!(view.failure_recorded);
    assert!(!view.status_summary.to_ascii_lowercase().contains("retry"));
}

#[test]
fn orphan_outcome_and_conflicting_same_phase_are_investigation_states() {
    let orphan = test_batch(fixed_uuid(31), 0);
    let duplicate = test_batch(fixed_uuid(32), 0);
    let records = vec![
        execution_record(
            "execution_completed",
            &orphan,
            fixed_time(0),
            completed_details(orphan.id(), 0),
        ),
        execution_record(
            "execution_planned",
            &duplicate,
            fixed_time(1),
            planned_details(&duplicate, json!({"attempt": 1})),
        ),
        execution_record(
            "execution_planned",
            &duplicate,
            fixed_time(2),
            planned_details(&duplicate, json!({"attempt": 2})),
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    let orphan = batch_view(&model, orphan.id());
    assert_eq!(orphan.state, ExecutionBatchState::Conflict);
    assert_eq!(orphan.recovery, RecoveryDirective::Investigate);
    let duplicate = batch_view(&model, duplicate.id());
    assert_eq!(duplicate.state, ExecutionBatchState::Conflict);
    assert_eq!(duplicate.recovery, RecoveryDirective::Investigate);
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::OrphanOutcome
            && warning.batch_id == Some(orphan.batch_id)
    }));
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::ConflictingDuplicate
            && warning.batch_id == Some(duplicate.batch_id)
    }));
}

#[test]
fn exact_duplicate_phase_is_ignored_without_hiding_the_terminal_outcome() {
    let batch = test_batch(fixed_uuid(41), 1);
    let planned = execution_record(
        "execution_planned",
        &batch,
        fixed_time(0),
        planned_details(&batch, json!({})),
    );
    let records = vec![
        planned.clone(),
        planned,
        execution_record(
            "execution_completed",
            &batch,
            fixed_time(1),
            completed_details(batch.id(), 1),
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    let view = batch_view(&model, batch.id());
    assert_eq!(view.state, ExecutionBatchState::Completed);
    assert_eq!(view.phases.len(), 2);
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::DuplicateIgnored
            && warning.batch_id == Some(batch.id())
    }));
}

#[test]
fn terminal_conflicts_and_plans_after_outcomes_remain_investigation_states() {
    let terminal_conflict = test_batch(fixed_uuid(45), 2);
    let out_of_order = test_batch(fixed_uuid(46), 0);
    let records = vec![
        execution_record(
            "execution_planned",
            &terminal_conflict,
            fixed_time(0),
            planned_details(&terminal_conflict, json!({})),
        ),
        execution_record(
            "execution_incomplete",
            &terminal_conflict,
            fixed_time(1),
            incomplete_details(terminal_conflict.id(), 1, 2),
        ),
        execution_record(
            "execution_completed",
            &terminal_conflict,
            fixed_time(2),
            completed_details(terminal_conflict.id(), 2),
        ),
        execution_record(
            "execution_completed",
            &out_of_order,
            fixed_time(3),
            completed_details(out_of_order.id(), 0),
        ),
        execution_record(
            "execution_planned",
            &out_of_order,
            fixed_time(4),
            planned_details(&out_of_order, json!({})),
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    for batch_id in [terminal_conflict.id(), out_of_order.id()] {
        let view = batch_view(&model, batch_id);
        assert_eq!(view.state, ExecutionBatchState::Conflict);
        assert_eq!(view.recovery, RecoveryDirective::Investigate);
    }
    let terminal_conflict_view = batch_view(&model, terminal_conflict.id());
    assert_eq!(terminal_conflict_view.receipt_count, Some(1));
    assert_eq!(terminal_conflict_view.expected_receipt_count, Some(2));
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::TerminalConflict
            && warning.batch_id == Some(terminal_conflict.id())
    }));
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::OutOfOrderPlanned
            && warning.batch_id == Some(out_of_order.id())
    }));
}

#[test]
fn missing_required_counts_degrade_and_conflict_instead_of_defaulting_to_zero() {
    let batch = test_batch(fixed_uuid(51), 1);
    let mut invalid_outcome = completed_details(batch.id(), 1);
    invalid_outcome
        .as_object_mut()
        .unwrap()
        .remove("receipt_count");
    let records = vec![
        execution_record(
            "execution_planned",
            &batch,
            fixed_time(0),
            planned_details(&batch, json!({})),
        ),
        execution_record(
            "execution_completed",
            &batch,
            fixed_time(1),
            invalid_outcome,
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    let view = batch_view(&model, batch.id());
    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(view.state, ExecutionBatchState::Conflict);
    assert_eq!(view.recovery, RecoveryDirective::Investigate);
    assert_eq!(view.receipt_count, None);
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::InvalidExecutionEvent
            && warning.batch_id == Some(batch.id())
    }));
}

#[test]
fn physical_sequence_wins_when_timestamps_regress() {
    let batch = test_batch(fixed_uuid(61), 0);
    let planned_at = fixed_time(2);
    let records = vec![
        execution_record(
            "execution_planned",
            &batch,
            planned_at,
            planned_details(&batch, json!({})),
        ),
        execution_record(
            "execution_completed",
            &batch,
            fixed_time(1),
            completed_details(batch.id(), 0),
        ),
    ];

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();
    let view = batch_view(&model, batch.id());
    assert_eq!(view.state, ExecutionBatchState::Completed);
    assert_eq!(view.updated_at, planned_at);
    assert_eq!(view.last_sequence, 2);
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::TimestampRegressed
            && warning.batch_id == Some(batch.id())
    }));
}

#[test]
fn partial_tail_returns_last_good_projection_as_degraded() {
    let batch = test_batch(fixed_uuid(71), 1);
    let mut bytes = jsonl(
        &[execution_record(
            "execution_planned",
            &batch,
            fixed_time(0),
            planned_details(&batch, json!({})),
        )],
        LineEnding::Lf,
    );
    bytes.extend_from_slice(br#"{"timestamp":"2026-07-24T00:00:01Z""#);
    let snapshot = JournalSnapshot::new(fixed_uuid(9), bytes).unwrap();

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot).unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Degraded);
    assert_eq!(model.batches[0].state, ExecutionBatchState::OutcomeUnknown);
    assert!(
        model
            .warnings
            .iter()
            .any(|warning| { warning.code == ReadModelWarningCode::PartialTail })
    );
}

#[test]
fn malformed_middle_record_is_a_hard_projection_error() {
    let batch = test_batch(fixed_uuid(81), 0);
    let mut bytes = jsonl(
        &[execution_record(
            "execution_planned",
            &batch,
            fixed_time(0),
            planned_details(&batch, json!({})),
        )],
        LineEnding::Lf,
    );
    bytes.extend_from_slice(b"{not-json}\n");
    bytes.extend_from_slice(&jsonl(
        &[execution_record(
            "execution_completed",
            &batch,
            fixed_time(1),
            completed_details(batch.id(), 0),
        )],
        LineEnding::Lf,
    ));
    let snapshot = JournalSnapshot::new(fixed_uuid(10), bytes).unwrap();

    assert!(matches!(
        OperatorReadModel::from_legacy_snapshot(&snapshot).unwrap_err(),
        ReadModelError::Journal(JournalReadError::MalformedRecord { sequence: 2, .. })
    ));
}

#[test]
fn batch_limit_fails_without_evicting_unresolved_execution_facts() {
    let records = (0..=MAX_OPERATOR_READ_MODEL_BATCHES)
        .map(|index| {
            let batch = test_batch(Uuid::from_u128(1_000 + index as u128), 0);
            execution_record(
                "execution_planned",
                &batch,
                fixed_time(i64::try_from(index).unwrap()),
                planned_details(&batch, json!({})),
            )
        })
        .collect::<Vec<_>>();
    let snapshot = snapshot(&records);

    assert!(matches!(
        OperatorReadModel::from_legacy_snapshot(&snapshot).unwrap_err(),
        ReadModelError::BatchLimitExceeded {
            limit: MAX_OPERATOR_READ_MODEL_BATCHES
        }
    ));
}

#[test]
fn resolved_batches_are_windowed_before_unresolved_execution_facts_are_lost() {
    let mut records = Vec::new();
    let first_planned = test_batch(Uuid::from_u128(10_000), 0);
    let first_completed = test_batch(Uuid::from_u128(10_001), 0);
    for batch in [&first_planned, &first_completed] {
        records.push(execution_record(
            "execution_planned",
            batch,
            fixed_time(i64::try_from(records.len()).unwrap()),
            planned_details(batch, json!({})),
        ));
    }
    records.push(execution_record(
        "execution_completed",
        &first_completed,
        fixed_time(i64::try_from(records.len()).unwrap()),
        completed_details(first_completed.id(), 0),
    ));
    records.push(execution_record(
        "execution_completed",
        &first_planned,
        fixed_time(i64::try_from(records.len()).unwrap()),
        completed_details(first_planned.id(), 0),
    ));

    for index in 2..=MAX_OPERATOR_READ_MODEL_BATCHES {
        let batch_id = Uuid::from_u128(10_000 + index as u128);
        let batch = test_batch(batch_id, 0);
        records.push(execution_record(
            "execution_planned",
            &batch,
            fixed_time(i64::try_from(records.len()).unwrap()),
            planned_details(&batch, json!({})),
        ));
        records.push(execution_record(
            "execution_completed",
            &batch,
            fixed_time(i64::try_from(records.len()).unwrap()),
            completed_details(batch.id(), 0),
        ));
    }

    let model = OperatorReadModel::from_legacy_snapshot(&snapshot(&records)).unwrap();

    assert_eq!(model.projection_status, ProjectionStatus::Windowed);
    assert!(model.batches_truncated);
    assert_eq!(model.batches.len(), MAX_OPERATOR_READ_MODEL_BATCHES);
    assert!(
        model
            .batches
            .iter()
            .all(|batch| batch.batch_id != first_completed.id())
    );
    assert!(
        model
            .batches
            .iter()
            .any(|batch| batch.batch_id == first_planned.id())
    );
    assert!(model.batches.iter().any(|batch| {
        batch.batch_id == Uuid::from_u128(10_000 + MAX_OPERATOR_READ_MODEL_BATCHES as u128)
    }));
    assert!(model.warnings.iter().any(|warning| {
        warning.code == ReadModelWarningCode::ResolvedBatchEvicted
            && warning.batch_id == Some(first_completed.id())
    }));
}

fn batch_view(
    model: &OperatorReadModel,
    batch_id: Uuid,
) -> &crypto_trading_runtime::ExecutionBatchView {
    model
        .batches
        .iter()
        .find(|batch| batch.batch_id == batch_id)
        .unwrap()
}

fn snapshot(records: &[Value]) -> JournalSnapshot {
    JournalSnapshot::new(fixed_uuid(8), jsonl(records, LineEnding::Lf)).unwrap()
}

fn execution_record(
    decision: &str,
    batch: &ExecutionBatch,
    timestamp: chrono::DateTime<Utc>,
    details: Value,
) -> Value {
    debug_assert_eq!(details["batch_id"], json!(batch.id()));
    let record = json!({
        "timestamp": timestamp,
        "strategy": "grid",
        "symbol": "BTC-USDT",
        "decision": decision,
        "details": details,
    });
    drop(details);
    record
}

fn planned_details(batch: &ExecutionBatch, context: Value) -> Value {
    let legs = batch
        .intents()
        .iter()
        .enumerate()
        .map(|(index, intent)| intent_summary(index, intent))
        .collect::<Vec<_>>();
    let details = json!({
        "batch_id": batch.id(),
        "legs": legs,
        "recovery_batch": batch,
        "context": context,
    });
    drop(context);
    details
}

fn completed_details(batch_id: Uuid, receipt_count: usize) -> Value {
    let mut summary = receipt_summary(receipt_count);
    summary["batch_id"] = json!(batch_id);
    summary
}

fn incomplete_details(
    batch_id: Uuid,
    receipt_count: usize,
    expected_receipt_count: usize,
) -> Value {
    let mut summary = completed_details(batch_id, receipt_count);
    summary["expected_receipt_count"] = json!(expected_receipt_count);
    summary
}

fn failed_details(batch_id: Uuid) -> Value {
    json!({
        "batch_id": batch_id,
        "error": "injected test failure",
    })
}

fn partial_details(batch: &ExecutionBatch, completed_count: usize) -> Value {
    let failed_index = completed_count;
    let unattempted = batch
        .intents()
        .iter()
        .enumerate()
        .skip(failed_index + 1)
        .map(|(index, intent)| intent_summary(index, intent))
        .collect::<Vec<_>>();
    json!({
        "batch_id": batch.id(),
        "expected_batch_id": batch.id(),
        "failed_index": failed_index,
        "completed": receipt_summary(completed_count),
        "failed_intent": intent_summary(failed_index, &batch.intents()[failed_index]),
        "unattempted": unattempted,
        "reconciliation": [{
            "exchange": "paper",
            "status": "error",
            "error": "injected reconciliation failure",
        }],
        "source": "injected test failure",
    })
}

fn receipt_summary(receipt_count: usize) -> Value {
    json!({
        "receipt_count": receipt_count,
        "receipts": (0..receipt_count)
            .map(|index| json!({"index": index}))
            .collect::<Vec<_>>(),
        "receipts_truncated": false,
        "open": 0,
        "filled": receipt_count,
        "cancelled": 0,
        "already_processed": 0,
    })
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

fn test_batch(id: Uuid, leg_count: usize) -> ExecutionBatch {
    let intents = (0..leg_count)
        .map(|index| {
            let mut intent = OrderIntent::market(
                "paper",
                Symbol::new("BTC-USDT").unwrap(),
                MarketType::Spot,
                if index % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                Quantity::new(Decimal::ONE).unwrap(),
            );
            intent.client_order_id = Uuid::from_u128(10_000 + index as u128);
            intent
        })
        .collect();
    ExecutionBatch::new(id, intents).unwrap()
}

#[derive(Clone, Copy)]
enum LineEnding {
    Lf,
    CrLf,
}

fn jsonl(records: &[Value], line_ending: LineEnding) -> Vec<u8> {
    let delimiter = match line_ending {
        LineEnding::Lf => b"\n".as_slice(),
        LineEnding::CrLf => b"\r\n".as_slice(),
    };
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend_from_slice(&serde_json::to_vec(record).unwrap());
        bytes.extend_from_slice(delimiter);
    }
    bytes
}

fn fixed_time(offset_seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_785_400_000 + offset_seconds, 0)
        .single()
        .unwrap()
}

fn fixed_uuid(value: u8) -> Uuid {
    Uuid::from_bytes([value; 16])
}
