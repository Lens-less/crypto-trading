//! Contract for the dedicated spread-history journal: bounded validated
//! writes, chain replay across sealed segments, the bounded recent-window
//! query used for cold-start backfill, and fail-closed corruption semantics.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{Duration, TimeZone, Utc};
use crypto_trading_runtime::{
    JournalSnapshot, MAX_SPREAD_HISTORY_BATCH_RECORDS, MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES,
    ProjectionStatus, ReadModelError, SPREAD_HISTORY_READ_MODEL_SCHEMA_VERSION, SpreadHistoryError,
    SpreadHistoryReadModel, SpreadHistoryRecord, SpreadHistoryWriter, read_journal_chain,
};
use uuid::Uuid;

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap()
}

fn record(offset_seconds: i64, spread_bps: &str) -> SpreadHistoryRecord {
    SpreadHistoryRecord {
        timestamp: base_time() + Duration::seconds(offset_seconds),
        symbol: "BTC-USDT".to_owned(),
        exchange_buy: "left".to_owned(),
        exchange_sell: "right".to_owned(),
        price_buy: "100.5".to_owned(),
        price_sell: "101.25".to_owned(),
        spread_bps: spread_bps.to_owned(),
        funding_rate_buy: None,
        funding_rate_sell: None,
        funding_rate_diff: None,
        funding_rate_diff_annual_pct: None,
    }
}

fn funded_record(offset_seconds: i64, spread_bps: &str) -> SpreadHistoryRecord {
    let mut record = record(offset_seconds, spread_bps);
    record.funding_rate_buy = Some("0.0001".to_owned());
    record.funding_rate_sell = Some("0.0002".to_owned());
    record.funding_rate_diff = Some("0.0001".to_owned());
    record.funding_rate_diff_annual_pct = Some("10.95".to_owned());
    record
}

fn temp_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crypto-trading-spread-history-{label}-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn snapshot_of(path: &std::path::Path) -> JournalSnapshot {
    JournalSnapshot::new(Uuid::new_v4(), read_journal_chain(path).unwrap()).unwrap()
}

#[tokio::test]
async fn write_read_roundtrip_preserves_the_versioned_record_shape() {
    let path = temp_path("roundtrip");
    let writer = SpreadHistoryWriter::new(&path);

    writer
        .append_batch(&[record(0, "150"), funded_record(60, "-25.5")])
        .await
        .unwrap();

    let model = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap();
    assert_eq!(
        model.schema_version,
        SPREAD_HISTORY_READ_MODEL_SCHEMA_VERSION
    );
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(model.invalid_record_count, 0);
    assert_eq!(model.samples.len(), 2);

    let plain = &model.samples[0];
    assert_eq!(plain.source_sequence, 1);
    assert_eq!(plain.symbol, "BTC-USDT");
    assert_eq!(plain.exchange_buy, "left");
    assert_eq!(plain.exchange_sell, "right");
    assert_eq!(plain.spread_bps, "150");
    assert_eq!(plain.funding_rate_buy, None);
    assert_eq!(plain.funding_rate_diff_annual_pct, None);

    let funded = &model.samples[1];
    assert_eq!(funded.spread_bps, "-25.5");
    assert_eq!(funded.funding_rate_buy.as_deref(), Some("0.0001"));
    assert_eq!(funded.funding_rate_sell.as_deref(), Some("0.0002"));
    assert_eq!(funded.funding_rate_diff.as_deref(), Some("0.0001"));
    assert_eq!(
        funded.funding_rate_diff_annual_pct.as_deref(),
        Some("10.95")
    );

    let roundtrip = SpreadHistoryReadModel::record_of(funded);
    assert_eq!(roundtrip, funded_record(60, "-25.5"));

    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn replay_spans_sealed_segments_in_order() {
    let path = temp_path("segments");
    let writer = SpreadHistoryWriter::new(&path);
    writer.append(&record(0, "100")).await.unwrap();

    // Seal the active file as segment 1 exactly the way rotation does, then
    // continue the chain with a fresh active file.
    let sealed = path.with_file_name(format!("{}.1", path.file_name().unwrap().to_str().unwrap()));
    std::fs::rename(&path, &sealed).unwrap();
    writer.append(&record(60, "110")).await.unwrap();
    writer.append(&record(120, "120")).await.unwrap();

    let model = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap();
    assert_eq!(model.projection_status, ProjectionStatus::Complete);
    assert_eq!(
        model
            .samples
            .iter()
            .map(|sample| (sample.source_sequence, sample.spread_bps.as_str()))
            .collect::<Vec<_>>(),
        [(1, "100"), (2, "110"), (3, "120")]
    );

    std::fs::remove_file(path).unwrap();
    std::fs::remove_file(sealed).unwrap();
}

#[tokio::test]
async fn recent_window_bounds_the_cold_start_backfill() {
    let path = temp_path("window");
    let writer = SpreadHistoryWriter::new(&path);
    writer
        .append_batch(&[
            record(0, "100"),
            record(1_800, "110"),
            record(3_600, "120"),
            record(5_400, "130"),
        ])
        .await
        .unwrap();

    let model = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap();
    let end = base_time() + Duration::seconds(5_400);
    let window = model.recent_window(end, Duration::hours(1));
    assert_eq!(
        window
            .iter()
            .map(|sample| sample.spread_bps.as_str())
            .collect::<Vec<_>>(),
        ["110", "120", "130"],
        "only samples inside [end - window, end] participate"
    );
    assert!(model.recent_window(end, Duration::seconds(1)).len() <= 1);

    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn read_model_retains_only_the_bounded_most_recent_samples() {
    let path = temp_path("bounded");
    let writer = SpreadHistoryWriter::new(&path);
    let total = MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES + 32;
    let mut written = 0usize;
    while written < total {
        let batch: Vec<SpreadHistoryRecord> = (written
            ..(written + MAX_SPREAD_HISTORY_BATCH_RECORDS).min(total))
            .map(|index| record(i64::try_from(index).unwrap(), "100"))
            .collect();
        written += batch.len();
        writer.append_batch(&batch).await.unwrap();
    }

    let model = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap();
    assert_eq!(model.samples.len(), MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES);
    assert_eq!(
        model.samples.first().unwrap().source_sequence,
        u64::try_from(total - MAX_SPREAD_HISTORY_READ_MODEL_SAMPLES + 1).unwrap(),
        "older samples are dropped, never the newest"
    );

    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn writer_validates_records_and_batch_bounds_without_writing() {
    let path = temp_path("validation");
    let writer = SpreadHistoryWriter::new(&path);

    let mut duplicate_exchanges = record(0, "100");
    duplicate_exchanges.exchange_sell = "left".to_owned();
    assert!(matches!(
        writer.append(&duplicate_exchanges).await,
        Err(SpreadHistoryError::InvalidRecord {
            field: "exchange_sell"
        })
    ));

    let mut broken_decimal = record(0, "100");
    broken_decimal.spread_bps = "1.2.3".to_owned();
    assert!(matches!(
        writer.append(&broken_decimal).await,
        Err(SpreadHistoryError::InvalidRecord {
            field: "spread_bps"
        })
    ));

    let mut broken_funding = funded_record(0, "100");
    broken_funding.funding_rate_diff = Some("not-a-number".to_owned());
    assert!(matches!(
        writer.append(&broken_funding).await,
        Err(SpreadHistoryError::InvalidRecord {
            field: "funding_rate_diff"
        })
    ));

    let oversized = vec![record(0, "100"); MAX_SPREAD_HISTORY_BATCH_RECORDS + 1];
    assert!(matches!(
        writer.append_batch(&oversized).await,
        Err(SpreadHistoryError::TooManyRecords { .. })
    ));

    assert!(!path.exists(), "rejected writes must not touch the journal");
}

#[tokio::test]
async fn corrupted_payloads_degrade_and_torn_lines_fail_closed() {
    let path = temp_path("corruption");
    let writer = SpreadHistoryWriter::new(&path);
    writer.append(&record(0, "100")).await.unwrap();

    // A structurally valid decision record with a broken spread payload
    // degrades the projection and is counted.
    let mut body = std::fs::read_to_string(&path).unwrap();
    body.push_str(
        "{\"timestamp\":\"2026-07-25T12:01:00Z\",\"strategy\":\"spread_history\",\
         \"symbol\":\"BTC-USDT\",\"decision\":\"spread_history_record_v1\",\
         \"details\":{\"schema_version\":1,\"exchange_buy\":\"left\",\
         \"exchange_sell\":\"left\",\"price_buy\":\"1\",\"price_sell\":\"1\",\
         \"spread_bps\":\"1\",\"funding_rate_buy\":null,\"funding_rate_sell\":null,\
         \"funding_rate_diff\":null,\"funding_rate_diff_annual_pct\":null}}\n",
    );
    std::fs::write(&path, &body).unwrap();
    let degraded = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap();
    assert_eq!(degraded.projection_status, ProjectionStatus::Degraded);
    assert_eq!(degraded.invalid_record_count, 1);
    assert_eq!(degraded.samples.len(), 1);

    // Records from other strategies are ignored, not counted as invalid.
    body.push_str(
        "{\"timestamp\":\"2026-07-25T12:02:00Z\",\"strategy\":\"grid\",\
         \"symbol\":\"BTC-USDT\",\"decision\":\"hold\",\"details\":null}\n",
    );
    std::fs::write(&path, &body).unwrap();
    let mixed = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap();
    assert_eq!(mixed.invalid_record_count, 1);
    assert_eq!(mixed.samples.len(), 1);

    // Physically malformed JSONL is a hard journal error.
    body.push_str("this-is-not-json\n");
    std::fs::write(&path, &body).unwrap();
    let error = SpreadHistoryReadModel::from_legacy_snapshot(&snapshot_of(&path)).unwrap_err();
    assert!(matches!(error, ReadModelError::Journal(_)));

    std::fs::remove_file(path).unwrap();
}
