use chrono::{DateTime, TimeZone, Utc};
use crypto_trading_backtest::{
    BacktestError, DatasetManifest, Sha256Digest, SpotKlineDataset, TimestampUnit,
};
use crypto_trading_domain::{MarketType, Symbol};

const DAY_MICROS: i64 = 86_400_000_000;
const CSV: &str = concat!(
    "1704067200000,100,110,90,105,1,1704153599999,100,10,0.5,50,0\n",
    "1704153600000,105,115,95,110,2,1704239999999,210,11,1,105,0\n",
    "1704240000000,110,120,100,115,3,1704326399999,330,12,1.5,165,0\n",
);

fn digest(character: char) -> Sha256Digest {
    Sha256Digest::new(&character.to_string().repeat(64)).unwrap()
}

fn instant(milliseconds: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(milliseconds).unwrap()
}

fn instant_micros(micros: i64) -> DateTime<Utc> {
    let seconds = micros.div_euclid(1_000_000);
    let micros = u32::try_from(micros.rem_euclid(1_000_000)).unwrap();
    Utc.timestamp_opt(seconds, micros * 1_000).unwrap()
}

fn manifest() -> DatasetManifest {
    DatasetManifest {
        source_url: "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/example.zip"
            .to_owned(),
        retrieved_at: instant(1_704_326_400_001),
        venue: "binance".to_owned(),
        product: MarketType::Spot,
        symbol: Symbol::new("BTCUSDT").unwrap(),
        interval_micros: 86_400_000_000,
        timezone: "UTC".to_owned(),
        timestamp_unit: TimestampUnit::Milliseconds,
        archive_sha256: digest('a'),
        content_sha256: Sha256Digest::from_bytes(CSV.as_bytes()),
        parser_version: "binance-spot-kline-v1".to_owned(),
        expected_first_open: instant(1_704_067_200_000),
        expected_last_close: instant(1_704_326_399_999),
        expected_bar_count: 3,
    }
}

fn source_url(symbol: &str, archive_name: &str) -> String {
    format!("https://data.binance.vision/data/spot/daily/klines/{symbol}/1d/{archive_name}.zip")
}

struct ArchiveSpec<'a> {
    symbol: &'a str,
    interval_micros: i64,
    timestamp_unit: TimestampUnit,
    first_open: DateTime<Utc>,
    last_close: DateTime<Utc>,
    expected_bar_count: usize,
}

fn manifest_for(archive_name: &str, csv: &str, spec: &ArchiveSpec<'_>) -> DatasetManifest {
    DatasetManifest {
        source_url: source_url(spec.symbol, archive_name),
        retrieved_at: instant(1_704_326_400_001),
        venue: "binance".to_owned(),
        product: MarketType::Spot,
        symbol: Symbol::new(spec.symbol).unwrap(),
        interval_micros: spec.interval_micros,
        timezone: "UTC".to_owned(),
        timestamp_unit: spec.timestamp_unit,
        archive_sha256: digest('d'),
        content_sha256: Sha256Digest::from_bytes(csv.as_bytes()),
        parser_version: "binance-spot-kline-v1".to_owned(),
        expected_first_open: spec.first_open,
        expected_last_close: spec.last_close,
        expected_bar_count: spec.expected_bar_count,
    }
}

fn encode_timestamp(timestamp_unit: TimestampUnit, timestamp_micros: i64) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => timestamp_micros / 1_000,
        TimestampUnit::Microseconds => timestamp_micros,
    }
}

fn close_offset(timestamp_unit: TimestampUnit) -> i64 {
    match timestamp_unit {
        TimestampUnit::Milliseconds => 1_000,
        TimestampUnit::Microseconds => 1,
    }
}

fn single_bar_csv(timestamp_unit: TimestampUnit, open_micros: i64) -> String {
    let close_micros = open_micros + DAY_MICROS - close_offset(timestamp_unit);
    format!(
        "{},{},{},{},{},{},{},{},{},0,0,0\n",
        encode_timestamp(timestamp_unit, open_micros),
        100,
        110,
        90,
        105,
        1,
        encode_timestamp(timestamp_unit, close_micros),
        100,
        10
    )
}

fn parse_dataset(
    manifest: DatasetManifest,
    csv: &str,
    sealed_at: DateTime<Utc>,
) -> SpotKlineDataset {
    let archive_sha256 = manifest.archive_sha256.clone();
    SpotKlineDataset::parse_csv(manifest, csv, &archive_sha256, sealed_at).unwrap()
}

fn daily_spec(
    symbol: &str,
    timestamp_unit: TimestampUnit,
    open_micros: i64,
    archive_name: &str,
    csv: &str,
) -> DatasetManifest {
    manifest_for(
        archive_name,
        csv,
        &ArchiveSpec {
            symbol,
            interval_micros: DAY_MICROS,
            timestamp_unit,
            first_open: instant_micros(open_micros),
            last_close: instant_micros(open_micros + DAY_MICROS - close_offset(timestamp_unit)),
            expected_bar_count: 1,
        },
    )
}

#[test]
fn verified_binance_spot_csv_records_provenance_and_closed_contiguous_bars() {
    let dataset =
        SpotKlineDataset::parse_csv(manifest(), CSV, &digest('a'), instant(1_704_326_400_000))
            .unwrap();

    assert_eq!(dataset.bars().len(), 3);
    assert_eq!(dataset.manifest(), &manifest());
    assert_eq!(dataset.manifests(), &[manifest()]);
    assert_eq!(dataset.bars()[0].open_time, instant(1_704_067_200_000));
    assert_eq!(dataset.bars()[2].close_time, instant(1_704_326_399_999));
}

#[test]
fn checksum_evidence_must_match_the_frozen_manifest() {
    assert_eq!(
        SpotKlineDataset::parse_csv(manifest(), CSV, &digest('c'), instant(1_704_326_400_000),),
        Err(BacktestError::ChecksumMismatch)
    );
    assert_eq!(
        SpotKlineDataset::parse_csv(
            manifest(),
            &CSV.replace("115,3", "116,3"),
            &digest('a'),
            instant(1_704_326_400_000),
        ),
        Err(BacktestError::ChecksumMismatch)
    );
    assert_eq!(
        Sha256Digest::from_bytes(b"abc").as_str(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        Sha256Digest::new(""),
        Err(BacktestError::InvalidChecksumFormat)
    );
}

#[test]
fn missing_intervals_and_incomplete_counts_fail_closed() {
    let gapped = concat!(
        "1704067200000,100,110,90,105,1,1704153599999,100,10,0.5,50,0\n",
        "1704240000000,110,120,100,115,3,1704326399999,330,12,1.5,165,0\n",
    );
    let mut gapped_manifest = manifest();
    gapped_manifest.expected_bar_count = 2;
    gapped_manifest.content_sha256 = Sha256Digest::from_bytes(gapped.as_bytes());

    assert_eq!(
        SpotKlineDataset::parse_csv(
            gapped_manifest,
            gapped,
            &digest('a'),
            instant(1_704_326_400_000),
        ),
        Err(BacktestError::InvalidBarSequence)
    );

    let mut incomplete_manifest = manifest();
    incomplete_manifest.expected_bar_count = 4;
    assert_eq!(
        SpotKlineDataset::parse_csv(
            incomplete_manifest,
            CSV,
            &digest('a'),
            instant(1_704_326_400_000),
        ),
        Err(BacktestError::IncompleteDataset {
            expected: 4,
            actual: 3,
        })
    );
}

#[test]
fn timestamp_scale_still_open_bars_and_non_spot_products_fail_closed() {
    let mut wrong_scale = manifest();
    wrong_scale.timestamp_unit = TimestampUnit::Microseconds;
    assert_eq!(
        SpotKlineDataset::parse_csv(wrong_scale, CSV, &digest('a'), instant(1_704_326_400_000),),
        Err(BacktestError::InvalidBarSequence)
    );

    assert_eq!(
        SpotKlineDataset::parse_csv(manifest(), CSV, &digest('a'), instant(1_704_326_399_999),),
        Err(BacktestError::StillOpenBar)
    );

    let mut perpetual = manifest();
    perpetual.product = MarketType::Perpetual;
    assert_eq!(
        SpotKlineDataset::parse_csv(perpetual, CSV, &digest('a'), instant(1_704_326_400_000),),
        Err(BacktestError::UnsupportedDerivativesMarginModel)
    );
}

#[test]
fn manifest_metadata_and_csv_shape_must_match_the_official_spot_contract() {
    let mut wrong_url = manifest();
    wrong_url.source_url = "https://example.com/data.csv".to_owned();
    assert_eq!(
        SpotKlineDataset::parse_csv(wrong_url, CSV, &digest('a'), instant(1_704_326_400_000),),
        Err(BacktestError::InvalidBarSequence)
    );

    let mut wrong_timezone = manifest();
    wrong_timezone.timezone = "Asia/Shanghai".to_owned();
    assert_eq!(
        SpotKlineDataset::parse_csv(
            wrong_timezone,
            CSV,
            &digest('a'),
            instant(1_704_326_400_000),
        ),
        Err(BacktestError::InvalidBarSequence)
    );

    let mut wrong_parser = manifest();
    wrong_parser.parser_version = "binance-spot-kline-v0".to_owned();
    assert_eq!(
        SpotKlineDataset::parse_csv(wrong_parser, CSV, &digest('a'), instant(1_704_326_400_000),),
        Err(BacktestError::InvalidBarSequence)
    );

    let malformed = "1704067200000,100,110,90,105,1,1704153599999,100,10,0.5,50\n";
    let mut malformed_manifest = manifest();
    malformed_manifest.content_sha256 = Sha256Digest::from_bytes(malformed.as_bytes());
    assert_eq!(
        SpotKlineDataset::parse_csv(
            malformed_manifest,
            malformed,
            &digest('a'),
            instant(1_704_326_400_000),
        ),
        Err(BacktestError::InvalidBarSequence)
    );
}

#[test]
fn merge_verified_flattens_composites_and_accepts_adjacent_ms_to_us_archives() {
    let day_one_open = 1_704_067_200_000_000;
    let day_two_open = day_one_open + DAY_MICROS;
    let day_three_open = day_two_open + DAY_MICROS;

    let day_one_csv = single_bar_csv(TimestampUnit::Milliseconds, day_one_open);
    let day_two_csv = single_bar_csv(TimestampUnit::Microseconds, day_two_open);
    let day_three_csv = single_bar_csv(TimestampUnit::Microseconds, day_three_open);

    let day_one_manifest = daily_spec(
        "BTCUSDT",
        TimestampUnit::Milliseconds,
        day_one_open,
        "archive-1",
        &day_one_csv,
    );
    let day_two_manifest = daily_spec(
        "BTCUSDT",
        TimestampUnit::Microseconds,
        day_two_open,
        "archive-2",
        &day_two_csv,
    );
    let day_three_manifest = daily_spec(
        "BTCUSDT",
        TimestampUnit::Microseconds,
        day_three_open,
        "archive-3",
        &day_three_csv,
    );

    let day_one = parse_dataset(
        day_one_manifest.clone(),
        &day_one_csv,
        instant_micros(day_two_open),
    );
    let day_two = parse_dataset(
        day_two_manifest.clone(),
        &day_two_csv,
        instant_micros(day_three_open),
    );
    let day_three = parse_dataset(
        day_three_manifest.clone(),
        &day_three_csv,
        instant_micros(day_three_open + DAY_MICROS),
    );

    let first_two =
        SpotKlineDataset::merge_verified(vec![day_one.clone(), day_two.clone()]).unwrap();
    let merged = SpotKlineDataset::merge_verified(vec![first_two, day_three]).unwrap();

    assert_eq!(merged.manifest(), &day_one_manifest);
    assert_eq!(
        merged.manifests(),
        &[
            day_one_manifest.clone(),
            day_two_manifest.clone(),
            day_three_manifest.clone()
        ]
    );
    assert_eq!(merged.bars().len(), 3);
    assert_eq!(
        merged.bars()[0].close_time,
        instant_micros(day_two_open - 1_000)
    );
    assert_eq!(merged.bars()[1].open_time, instant_micros(day_two_open));
    assert_eq!(
        merged.bars()[1].close_time,
        instant_micros(day_three_open - 1)
    );
    assert_eq!(merged.bars()[2].open_time, instant_micros(day_three_open));
}

#[test]
fn merge_verified_rejects_empty_input() {
    assert_eq!(
        SpotKlineDataset::merge_verified(Vec::new()),
        Err(BacktestError::InvalidBarSequence)
    );
}

#[test]
fn merge_verified_rejects_duplicate_source_urls() {
    let day_one_open = 1_704_067_200_000_000;
    let day_two_open = day_one_open + DAY_MICROS;

    let day_one_csv = single_bar_csv(TimestampUnit::Milliseconds, day_one_open);
    let day_two_csv = single_bar_csv(TimestampUnit::Milliseconds, day_two_open);

    let duplicate_one = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_one_open,
            "duplicate",
            &day_one_csv,
        ),
        &day_one_csv,
        instant_micros(day_two_open),
    );
    let duplicate_two = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_two_open,
            "duplicate",
            &day_two_csv,
        ),
        &day_two_csv,
        instant_micros(day_two_open + DAY_MICROS),
    );

    assert_eq!(
        SpotKlineDataset::merge_verified(vec![duplicate_one, duplicate_two]),
        Err(BacktestError::InvalidBarSequence)
    );
}

#[test]
fn merge_verified_rejects_mixed_symbols_and_intervals() {
    let day_one_open = 1_704_067_200_000_000;
    let day_two_open = day_one_open + DAY_MICROS;
    let day_three_open = day_two_open + DAY_MICROS;

    let day_one_csv = single_bar_csv(TimestampUnit::Milliseconds, day_one_open);
    let day_two_csv = single_bar_csv(TimestampUnit::Milliseconds, day_two_open);

    let day_one = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_one_open,
            "base-1",
            &day_one_csv,
        ),
        &day_one_csv,
        instant_micros(day_two_open),
    );

    let mixed_symbol = parse_dataset(
        daily_spec(
            "ETHUSDT",
            TimestampUnit::Milliseconds,
            day_two_open,
            "eth-archive",
            &day_two_csv,
        ),
        &day_two_csv,
        instant_micros(day_three_open),
    );
    assert_eq!(
        SpotKlineDataset::merge_verified(vec![day_one.clone(), mixed_symbol]),
        Err(BacktestError::InvalidBarSequence)
    );

    let half_day_micros = DAY_MICROS / 2;
    let half_day_csv = format!(
        "{},{},{},{},{},{},{},{},{},0,0,0\n",
        encode_timestamp(TimestampUnit::Milliseconds, day_two_open),
        100,
        110,
        90,
        105,
        1,
        encode_timestamp(
            TimestampUnit::Milliseconds,
            day_two_open + half_day_micros - 1_000
        ),
        100,
        10
    );
    let mixed_interval = parse_dataset(
        manifest_for(
            "half-day",
            &half_day_csv,
            &ArchiveSpec {
                symbol: "BTCUSDT",
                interval_micros: half_day_micros,
                timestamp_unit: TimestampUnit::Milliseconds,
                first_open: instant_micros(day_two_open),
                last_close: instant_micros(day_two_open + half_day_micros - 1_000),
                expected_bar_count: 1,
            },
        ),
        &half_day_csv,
        instant_micros(day_two_open + half_day_micros),
    );
    assert_eq!(
        SpotKlineDataset::merge_verified(vec![day_one.clone(), mixed_interval]),
        Err(BacktestError::InvalidBarSequence)
    );
}

#[test]
fn merge_verified_rejects_gapped_and_overlapping_boundaries() {
    let day_one_open = 1_704_067_200_000_000;
    let day_two_open = day_one_open + DAY_MICROS;
    let day_three_open = day_two_open + DAY_MICROS;

    let day_one_csv = single_bar_csv(TimestampUnit::Milliseconds, day_one_open);
    let day_three_csv = single_bar_csv(TimestampUnit::Milliseconds, day_three_open);

    let day_one = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_one_open,
            "base-1",
            &day_one_csv,
        ),
        &day_one_csv,
        instant_micros(day_two_open),
    );
    let gapped = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_three_open,
            "gap",
            &day_three_csv,
        ),
        &day_three_csv,
        instant_micros(day_three_open + DAY_MICROS),
    );
    assert_eq!(
        SpotKlineDataset::merge_verified(vec![day_one.clone(), gapped]),
        Err(BacktestError::InvalidBarSequence)
    );

    let overlapping = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_one_open,
            "overlap",
            &day_one_csv,
        ),
        &day_one_csv,
        instant_micros(day_two_open),
    );
    assert_eq!(
        SpotKlineDataset::merge_verified(vec![day_one.clone(), overlapping]),
        Err(BacktestError::InvalidBarSequence)
    );
}

#[test]
fn merge_verified_rejects_out_of_order_components() {
    let day_one_open = 1_704_067_200_000_000;
    let day_two_open = day_one_open + DAY_MICROS;
    let day_three_open = day_two_open + DAY_MICROS;

    let day_one_csv = single_bar_csv(TimestampUnit::Milliseconds, day_one_open);
    let day_two_csv = single_bar_csv(TimestampUnit::Milliseconds, day_two_open);

    let day_one = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_one_open,
            "base-1",
            &day_one_csv,
        ),
        &day_one_csv,
        instant_micros(day_two_open),
    );
    let day_two = parse_dataset(
        daily_spec(
            "BTCUSDT",
            TimestampUnit::Milliseconds,
            day_two_open,
            "base-2",
            &day_two_csv,
        ),
        &day_two_csv,
        instant_micros(day_three_open),
    );

    assert_eq!(
        SpotKlineDataset::merge_verified(vec![day_two, day_one]),
        Err(BacktestError::InvalidBarSequence)
    );
}
