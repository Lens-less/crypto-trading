//! Contract tests for the shared incremental authority projection.

use std::path::{Path, PathBuf};

use chrono::{TimeZone, Utc};
use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    AccountRiskAdmission, AccountRiskAuthority, AccountRiskCandidate, JsonlHistory,
    PaperAccountAuthority, PaperAccountConfig, PaperAccountError, PaperCostModel,
    PaperReservationAdmission, PaperReservationLeg, PaperReservationRequest,
};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy};
use rust_decimal::Decimal;
use uuid::Uuid;

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
    let intent = OrderIntent::market(
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
async fn repeated_mutations_keep_the_live_projection_consistent() {
    let (root, path) = temp_case("incremental");
    let journal_id = Uuid::new_v4();
    let authority = paper_authority(journal_id, &path);

    assert!(authority.snapshot().await.unwrap().reservations.is_empty());

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

    assert!(authority.snapshot().await.unwrap().reservations.is_empty());
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 16);
    drop(authority);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn another_handle_observes_appended_authority_facts() {
    let (root, path) = temp_case("other-handle");
    let journal_id = Uuid::new_v4();
    let first = paper_authority(journal_id, &path);
    let second = paper_authority(journal_id, &path);

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
    assert!(matches!(admission, PaperReservationAdmission::Reserved(_)));

    assert_eq!(first.snapshot().await.unwrap().reservations.len(), 1);
    drop(first);
    drop(second);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_rebuilds_the_authority_projection_from_the_journal() {
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
    }

    let restarted = paper_authority(journal_id, &path);
    assert_eq!(restarted.snapshot().await.unwrap().reservations.len(), 1);
    drop(restarted);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn regressed_journal_head_fails_closed() {
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

    assert!(matches!(
        authority.snapshot().await.unwrap_err(),
        PaperAccountError::DurableStateDegraded
    ));
    drop(authority);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn incremental_risk_state_matches_a_restart_replay() {
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

    let incremental = first.state().await.unwrap();
    drop(first);
    drop(second);

    let restarted = risk_authority(journal_id, &path);
    assert_eq!(incremental, restarted.state().await.unwrap());
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
    let first_reservation = match first
        .reserve(request(
            "shared-task",
            "shared-idempotency",
            Uuid::from_u128(1),
            Uuid::from_u128(2),
        ))
        .await
        .unwrap()
    {
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
    assert_eq!(restarted.snapshot().await.unwrap().reservations.len(), 1);
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

#[tokio::test]
async fn restart_quarantines_an_unanchored_first_record_before_replay() {
    let (root, path) = temp_case("unanchored-partial-restart");
    let partial = br#"{"timestamp":"2026-08-03T00:00:00Z","strategy":"crash""#;
    std::fs::write(&path, partial).unwrap();

    let authority = paper_authority(Uuid::new_v4(), &path);
    assert!(authority.snapshot().await.unwrap().reservations.is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), b"");
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

    drop(authority);
    std::fs::remove_dir_all(root).unwrap();
}
