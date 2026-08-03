//! Shared fail-closed compensation for the risk-admission/reservation seam.

use chrono::{DateTime, Utc};
use crypto_trading_runtime::{
    AccountRiskAdmissionTicket, AccountRiskAuthority, AccountRiskError, PaperAccountAuthority,
    PaperAccountError, PaperReservationPhase,
};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum PaperAdmissionCompensationError {
    Account(PaperAccountError),
    AccountRisk(AccountRiskError),
    RecoveryRequired,
}

/// Cancels an admitted ticket that never reached reservation.
pub(crate) async fn cancel_unreserved_admission(
    risk: &AccountRiskAuthority,
    task_id: &str,
    ticket: &AccountRiskAdmissionTicket,
    now: DateTime<Utc>,
) -> Result<bool, PaperAdmissionCompensationError> {
    risk.cancel_admission(task_id, ticket, now)
        .await
        .map_err(PaperAdmissionCompensationError::AccountRisk)
}

/// Retains uncertain capacity when cancellation races with durable reservation.
///
/// `Ok(false)` proves that no reservation exists and a pending admission, when
/// present, was cancelled. `Ok(true)` means durable reservation capacity was
/// found and retained. An unprovable race fails closed as recovery-required.
pub(crate) async fn retain_cancelled_reservation(
    account: &PaperAccountAuthority,
    risk: Option<&AccountRiskAuthority>,
    owner_task_id: &str,
    admission_ticket: Option<&AccountRiskAdmissionTicket>,
    reservation_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, PaperAdmissionCompensationError> {
    let snapshot = account
        .decision_snapshot()
        .await
        .map_err(PaperAdmissionCompensationError::Account)?;
    if let Some(reservation) = snapshot
        .reservations
        .iter()
        .find(|reservation| reservation.reservation_id == reservation_id)
    {
        retain_pending(account, reservation.phase, reservation_id).await?;
        return Ok(true);
    }

    let (Some(risk), Some(ticket)) = (risk, admission_ticket) else {
        return Ok(false);
    };
    let cancelled = cancel_unreserved_admission(risk, owner_task_id, ticket, now).await?;
    if cancelled {
        return Ok(false);
    }

    // A false cancellation can mean that reserve consumed the admission
    // between the first snapshot and the cancel. Re-read while the caller's
    // owner is stopping and retain the raced reservation as uncertain.
    let retry_snapshot = account
        .decision_snapshot()
        .await
        .map_err(PaperAdmissionCompensationError::Account)?;
    let retry_reservation = retry_snapshot
        .reservations
        .iter()
        .find(|reservation| reservation.reservation_id == reservation_id)
        .ok_or(PaperAdmissionCompensationError::RecoveryRequired)?;
    retain_pending(account, retry_reservation.phase, reservation_id).await?;
    Ok(true)
}

/// Cancels an admitted ticket that was discarded before reservation.
pub(crate) async fn discard_planned_admission(
    risk: Option<&AccountRiskAuthority>,
    task_id: &str,
    ticket: Option<&AccountRiskAdmissionTicket>,
    now: DateTime<Utc>,
) -> Result<(), PaperAdmissionCompensationError> {
    let (Some(risk), Some(ticket)) = (risk, ticket) else {
        return Ok(());
    };
    match cancel_unreserved_admission(risk, task_id, ticket, now).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(PaperAdmissionCompensationError::RecoveryRequired),
        Err(error) => Err(error),
    }
}

async fn retain_pending(
    account: &PaperAccountAuthority,
    phase: PaperReservationPhase,
    reservation_id: Uuid,
) -> Result<(), PaperAdmissionCompensationError> {
    if phase == PaperReservationPhase::Pending {
        account
            .mark_uncertain(reservation_id)
            .await
            .map_err(PaperAdmissionCompensationError::Account)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crypto_trading_domain::Money;
    use crypto_trading_runtime::{
        DecisionRecord, JsonlHistory, PaperAccountConfig, PaperAccountError,
    };
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn compensation_refuses_to_decide_from_a_degraded_diagnostic_snapshot() {
        let path =
            std::env::temp_dir().join(format!("paper-admission-degraded-{}.jsonl", Uuid::new_v4()));
        let journal_id = Uuid::new_v4();
        let history = JsonlHistory::new(&path);
        history
            .append(&DecisionRecord {
                timestamp: Utc::now(),
                strategy: "paper_account".to_owned(),
                symbol: "paper-main".to_owned(),
                decision: "paper_account_reserved".to_owned(),
                details: json!({"schema_version": 1}),
            })
            .await
            .unwrap();
        let account = PaperAccountAuthority::new(
            journal_id,
            history,
            PaperAccountConfig::new("paper-main", Money::new(Decimal::from_str("1000").unwrap()))
                .unwrap(),
        )
        .unwrap();

        assert!(matches!(
            retain_cancelled_reservation(
                &account,
                None,
                "owner/a",
                None,
                Uuid::new_v4(),
                Utc::now(),
            )
            .await,
            Err(PaperAdmissionCompensationError::Account(
                PaperAccountError::DurableStateDegraded
            ))
        ));
    }
}
