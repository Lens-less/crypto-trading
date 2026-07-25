use crypto_trading_control_plane::{
    SUBMIT_SCHEMA_VERSION, SubmitCommand, SubmitEnvelope, SubmitPermission, SubmitRiskConfirmation,
    SubmitRole,
};
use crypto_trading_runtime::{PaperReconciliationDigestAlgorithm, PaperReconciliationProof};
use serde_json::json;
use uuid::Uuid;

fn paper_operator(command: SubmitCommand) -> SubmitEnvelope {
    SubmitEnvelope::new(
        Uuid::from_u128(1),
        "request-0001",
        "paper-arbitrage-btc-usdt",
        SubmitPermission::new("operator-a", SubmitRole::PaperOperator).unwrap(),
        SubmitRiskConfirmation::PaperOnly,
        command,
    )
    .unwrap()
}

fn reconciliation_proof() -> PaperReconciliationProof {
    PaperReconciliationProof::new(
        "paper-account",
        Uuid::from_u128(2),
        Uuid::from_u128(3),
        "binance-testnet-open-orders-42",
        42,
        PaperReconciliationDigestAlgorithm::Fnv1a64,
        "0123456789abcdef",
    )
    .unwrap()
}

#[test]
fn paper_start_round_trips_deterministically() {
    let envelope = paper_operator(SubmitCommand::StartPaperArbitrage {
        strategy_id: "cross-venue-btc".to_owned(),
        strategy_revision: "2026-07-25".to_owned(),
    });

    let encoded = serde_json::to_string(&envelope).unwrap();
    let decoded: SubmitEnvelope = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, envelope);
    assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);
    assert_eq!(decoded.schema_version(), SUBMIT_SCHEMA_VERSION);
}

#[test]
fn deserialized_envelopes_still_require_fail_closed_validation() {
    let malformed = json!({
        "schema_version": 99,
        "command_id": Uuid::from_u128(1),
        "idempotency_key": "request-0001",
        "target_task_id": "paper-grid-btc-usdt",
        "permission": {
            "principal_id": "operator-a",
            "role": "paper_operator"
        },
        "risk_confirmation": "paper_only",
        "command": {
            "kind": "start_paper_grid",
            "strategy_id": "grid-btc",
            "strategy_revision": "2026-07-25"
        }
    });

    let decoded: SubmitEnvelope = serde_json::from_value(malformed).unwrap();
    assert!(decoded.validate().is_err());
}

#[test]
fn command_permissions_and_risk_confirmation_cannot_be_cross_wired() {
    let proof = reconciliation_proof();
    let operator = SubmitPermission::new("operator-a", SubmitRole::PaperOperator).unwrap();
    let reconciler = SubmitPermission::new("reconciler-a", SubmitRole::Reconciler).unwrap();

    let wrong_role = SubmitEnvelope::new(
        Uuid::from_u128(1),
        "reconcile-0001",
        "paper-arbitrage-btc-usdt",
        operator,
        SubmitRiskConfirmation::ReconciliationEvidenceVerified,
        SubmitCommand::ReconcileRelease {
            proof: proof.clone(),
        },
    );
    assert!(wrong_role.is_err());

    let wrong_confirmation = SubmitEnvelope::new(
        Uuid::from_u128(1),
        "reconcile-0001",
        "paper-arbitrage-btc-usdt",
        reconciler,
        SubmitRiskConfirmation::PaperOnly,
        SubmitCommand::ReconcileRelease { proof },
    );
    assert!(wrong_confirmation.is_err());
}

#[test]
fn unknown_fields_and_unbounded_identifiers_are_rejected() {
    let mut value = serde_json::to_value(paper_operator(SubmitCommand::StopTask)).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("live".to_owned(), json!(true));
    assert!(serde_json::from_value::<SubmitEnvelope>(value).is_err());

    assert!(
        SubmitEnvelope::new(
            Uuid::from_u128(1),
            "x".repeat(129),
            "paper-grid-btc-usdt",
            SubmitPermission::new("operator-a", SubmitRole::PaperOperator).unwrap(),
            SubmitRiskConfirmation::PaperOnly,
            SubmitCommand::StopTask,
        )
        .is_err()
    );
}

#[test]
fn nil_command_ids_and_blank_strategy_fields_fail_closed() {
    assert!(
        SubmitEnvelope::new(
            Uuid::nil(),
            "request-0001",
            "paper-grid-btc-usdt",
            SubmitPermission::new("operator-a", SubmitRole::PaperOperator).unwrap(),
            SubmitRiskConfirmation::PaperOnly,
            SubmitCommand::StartPaperGrid {
                strategy_id: " ".to_owned(),
                strategy_revision: "2026-07-25".to_owned(),
            },
        )
        .is_err()
    );
}
