//! Contract tests for the shared incremental authority projection.

use std::path::{Path, PathBuf};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_domain::{MarketType, Money, OrderIntent, Quantity, Side, Symbol};
use crypto_trading_runtime::{
    AccountRiskAdmission, AccountRiskAdmissionTicket, AccountRiskAuthority, AccountRiskCandidate,
    AccountRiskError, DecisionRecord, JsonlHistory, MAX_PAPER_ACCOUNT_RESERVATIONS,
    PaperAccountAuthority, PaperAccountConfig, PaperAccountError, PaperCostModel,
    PaperReservationAdmission, PaperReservationLeg, PaperReservationRequest,
};
use crypto_trading_strategy::{AccountRiskLimits, AccountRiskPolicy};
use rust_decimal::Decimal;
use serde_json::{Value, json};
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

fn rewrite_journal_line(path: &Path, line_index: usize, mutate: impl FnOnce(&mut Value)) {
    let original = std::fs::read(path).unwrap();
    let mut rows = std::str::from_utf8(&original)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    mutate(rows.get_mut(line_index).unwrap());
    let mut rewritten = Vec::with_capacity(original.len());
    for row in rows {
        serde_json::to_writer(&mut rewritten, &row).unwrap();
        rewritten.push(b'\n');
    }
    assert_eq!(rewritten.len(), original.len(), "tamper must preserve head");
    std::fs::write(path, rewritten).unwrap();
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
async fn cold_replay_indexes_long_terminal_history_in_one_pass_without_losing_idempotency() {
    let (root, path) = temp_case("linear-terminal-history");
    let journal_id = Uuid::new_v4();
    let timestamp = Utc.with_ymd_and_hms(2026, 8, 2, 1, 0, 0).unwrap();
    let cycles = MAX_PAPER_ACCOUNT_RESERVATIONS + 8;
    let mut records = Vec::with_capacity(cycles * 2);
    let mut first_request = None;
    let mut last_request = None;
    for index in 0..cycles {
        let request = request(
            &format!("terminal/{index}"),
            &format!("terminal-idem/{index}"),
            Uuid::from_u128(u128::try_from(index).unwrap() + 1),
            Uuid::from_u128(u128::try_from(index).unwrap() + 10_000),
        );
        first_request.get_or_insert_with(|| request.clone());
        last_request = Some(request.clone());
        records.push(DecisionRecord {
            timestamp,
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_reserved".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": "paper-main",
                "initial_available": money("1000"),
                "request": request,
                "reserved_exposure": money("10.01"),
            }),
        });
        records.push(DecisionRecord {
            timestamp,
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_released".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": "paper-main",
                "reservation_id": request.reservation_id(),
                "batch_id": request.batch_id(),
                "confirmed_exposure": null,
                "reason": "cycle_done",
                "proof": null,
            }),
        });
    }
    JsonlHistory::new(&path)
        .append_batch(&records)
        .await
        .unwrap();

    let restarted = paper_authority(journal_id, &path);
    assert!(restarted.snapshot().await.unwrap().reservations.is_empty());
    for request in [first_request.unwrap(), last_request.unwrap()] {
        assert!(matches!(
            restarted.reserve(request).await.unwrap(),
            PaperReservationAdmission::Existing(_)
        ));
    }
    assert_eq!(
        std::fs::read_to_string(&path).unwrap().lines().count(),
        cycles * 2
    );

    drop(restarted);
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
async fn explicit_verification_latches_equal_length_live_state_tampering() {
    let (root, path) = temp_case("equal-length-live-tamper");
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
    assert_eq!(authority.snapshot().await.unwrap().reservations.len(), 1);
    rewrite_journal_line(&path, 0, |row| {
        assert_eq!(row["details"]["request"]["task_id"], "task/a");
        row["details"]["request"]["task_id"] = json!("task/b");
    });

    assert!(matches!(
        authority.verify_durable_state().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));
    assert!(matches!(
        authority.verify_durable_state().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));
    assert!(matches!(
        authority.snapshot().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));

    drop(authority);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn malformed_equal_length_replay_latches_degraded_even_after_bytes_are_restored() {
    let (root, path) = temp_case("equal-length-malformed-tamper");
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
    let original = std::fs::read(&path).unwrap();
    let mut corrupted = original.clone();
    corrupted[0] = b'!';
    std::fs::write(&path, corrupted).unwrap();

    assert!(matches!(
        authority.verify_durable_state().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));
    std::fs::write(&path, original).unwrap();
    assert!(matches!(
        authority.verify_durable_state().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));
    assert!(matches!(
        authority.snapshot().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));

    drop(authority);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn cached_refreshes_verify_durability_on_the_sixty_fourth_hit() {
    let (root, path) = temp_case("automatic-equal-length-tamper");
    let journal_id = Uuid::new_v4();
    let request = request("task/a", "idem/a", Uuid::from_u128(1), Uuid::from_u128(2));
    JsonlHistory::new(&path)
        .append(&DecisionRecord {
            timestamp: Utc.with_ymd_and_hms(2026, 8, 2, 1, 0, 0).unwrap(),
            strategy: "paper_account".to_owned(),
            symbol: "paper-main".to_owned(),
            decision: "paper_account_reserved".to_owned(),
            details: json!({
                "schema_version": 1,
                "journal_id": journal_id,
                "account_id": "paper-main",
                "initial_available": money("1000"),
                "request": request,
                "reserved_exposure": money("10.01"),
            }),
        })
        .await
        .unwrap();
    let authority = paper_authority(journal_id, &path);
    assert_eq!(authority.snapshot().await.unwrap().reservations.len(), 1);
    rewrite_journal_line(&path, 0, |row| {
        row["details"]["request"]["task_id"] = json!("task/b");
    });

    for hit in 1..64 {
        assert!(
            authority.snapshot().await.is_ok(),
            "verification fired before the documented interval at hit {hit}"
        );
    }
    assert!(matches!(
        authority.snapshot().await,
        Err(PaperAccountError::DurableStateDegraded)
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
async fn explicit_verification_compares_open_admissions_not_just_risk_state() {
    let (root, path) = temp_case("equal-length-open-admission-tamper");
    let journal_id = Uuid::new_v4();
    let authority = risk_authority(journal_id, &path);
    let now = Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap();
    assert!(matches!(
        authority
            .admit(
                &AccountRiskCandidate::new("task/a", "BTC-USDT", money("10")).unwrap(),
                now,
            )
            .await
            .unwrap(),
        AccountRiskAdmission::Admitted { .. }
    ));
    authority.state().await.unwrap();
    rewrite_journal_line(&path, 0, |row| {
        assert_eq!(row["details"]["notional"], json!(money("10")));
        row["details"]["notional"] = json!(money("11"));
    });

    assert!(matches!(
        authority.verify_durable_state().await,
        Err(AccountRiskError::DegradedState)
    ));
    assert!(matches!(
        authority.state().await,
        Err(AccountRiskError::DegradedState)
    ));

    drop(authority);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn paper_reservation_consumes_the_exact_bound_live_risk_ticket() {
    let (root, path) = temp_case("bound-risk-ticket");
    let journal_id = Uuid::new_v4();
    let risk = risk_authority(journal_id, &path);
    let paper = paper_authority(journal_id, &path);
    let now = Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap();
    let AccountRiskAdmission::Admitted { ticket, .. } = risk
        .admit(
            &AccountRiskCandidate::new("task/a", "BTC-USDT", money("10")).unwrap(),
            now,
        )
        .await
        .unwrap()
    else {
        panic!("expected an admission ticket");
    };
    let bound = request(
        "task/a/op/000001",
        "bound/1",
        Uuid::from_u128(501),
        Uuid::from_u128(502),
    )
    .with_account_risk_admission("paper", &ticket)
    .unwrap();

    assert!(matches!(
        paper.reserve(bound).await.unwrap(),
        PaperReservationAdmission::Reserved(_)
    ));
    assert!(
        !risk.cancel_admission("task/a", &ticket, now).await.unwrap(),
        "the reservation fact must consume this exact ticket"
    );

    drop(paper);
    drop(risk);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn paper_reservation_rejects_a_bound_ticket_after_its_wall_clock_lease() {
    let (root, path) = temp_case("expired-bound-risk-ticket");
    let journal_id = Uuid::new_v4();
    let ticket_id = Uuid::new_v4();
    let wall_time = Utc::now() - Duration::minutes(10);
    JsonlHistory::new(&path)
        .append(&DecisionRecord {
            timestamp: wall_time,
            strategy: "account_risk".to_owned(),
            symbol: "paper".to_owned(),
            decision: "account_risk_admitted".to_owned(),
            details: json!({
                "kind": "admitted",
                "schema_version": 1,
                "journal_id": journal_id,
                "scope_id": "paper",
                "task_id": "task/a",
                "symbol": "BTC-USDT",
                "ticket_id": ticket_id.to_string(),
                "notional": money("10"),
                "utc_date": "2026-08-02",
                "recorded_at": Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap(),
                "lease_expires_at": wall_time + Duration::minutes(5),
                "warnings": [],
            }),
        })
        .await
        .unwrap();
    let ticket = AccountRiskAdmissionTicket::parse(ticket_id.to_string()).unwrap();
    let bound = request(
        "task/a/op/000001",
        "bound/expired",
        Uuid::from_u128(601),
        Uuid::from_u128(602),
    )
    .with_account_risk_admission("paper", &ticket)
    .unwrap();

    let paper = paper_authority(journal_id, &path);
    assert!(matches!(
        paper.reserve(bound).await,
        Err(PaperAccountError::RiskAdmissionExpired)
    ));
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);

    drop(paper);
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
async fn explicit_verification_compares_released_reservation_identity_history() {
    let (root, path) = temp_case("equal-length-released-identity-tamper");
    let journal_id = Uuid::new_v4();
    let authority = paper_authority(journal_id, &path);
    let original = request("task/a", "idem/a", Uuid::from_u128(1), Uuid::from_u128(2));
    let reservation = match authority.reserve(original.clone()).await.unwrap() {
        PaperReservationAdmission::Reserved(reservation) => reservation,
        PaperReservationAdmission::Existing(_) => panic!("expected fresh reservation"),
    };
    authority
        .release(reservation.reservation_id, "cycle_done")
        .await
        .unwrap();
    assert!(authority.snapshot().await.unwrap().reservations.is_empty());
    rewrite_journal_line(&path, 0, |row| {
        assert_eq!(row["details"]["request"]["idempotency_key"], "idem/a");
        row["details"]["request"]["idempotency_key"] = json!("idem/b");
    });

    assert!(matches!(
        authority.verify_durable_state().await,
        Err(PaperAccountError::DurableStateDegraded)
    ));
    assert!(matches!(
        authority.reserve(original).await,
        Err(PaperAccountError::DurableStateDegraded)
    ));

    drop(authority);
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
    let quarantine_dir = root.join(format!(
        "{}.quarantine",
        path.file_name().unwrap().to_string_lossy()
    ));
    let quarantines = std::fs::read_dir(&quarantine_dir)
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
    let quarantine_dir = root.join(format!(
        "{}.quarantine",
        path.file_name().unwrap().to_string_lossy()
    ));
    let quarantines = std::fs::read_dir(&quarantine_dir)
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
