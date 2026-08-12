//! Shared offline runner for frozen research protocols.

use std::{
    env,
    error::Error,
    ffi::OsString,
    fmt::Write as _,
    fs::{self, File},
    io::{self, Write as _},
    ops::Range,
    path::{Path, PathBuf},
    process,
    str::FromStr,
};

use chrono::{DateTime, Duration, SecondsFormat, TimeZone, Utc};
use crypto_trading_backtest::{
    AggregateSelectionMetrics, BootstrapConfig, BootstrapInterval, CausalSpotEvaluation,
    CompletedExperiment, ConfigurationSelectionSummary, CostBreakdown, CostScheduleSpec,
    CostSensitivityEvaluation, DatasetManifest, EvaluationProtocolSpec, ExperimentError,
    ExperimentPlan, ExperimentSplitSpec, FinalHoldoutOutcome, PromotionThresholds,
    RegisteredConfiguration, SelectionPhase, SelectionSummary, SelectionWindowResult, Sha256Digest,
    SpotKlineDataset, SpotStrategyConfig, TimestampUnit,
};
use crypto_trading_domain::{MarketType, Money, Symbol};
use rust_decimal::Decimal;

const SCHEMA_VERSION: usize = 1;
const PARSER_VERSION: &str = "binance-spot-kline-v1";
const G005_PROTOCOL_ID: &str = "g005-btcusdt-spot-20260812-v1";
const G005_PROVENANCE_LOCK_SHA256: &str =
    "5eb95ab4efeddc2656c6cd2863a48a50c685758ef458a5102bcc64c5047c2d3f";
const G005_FROZEN_RETRIEVED_AT: &str = "2026-08-11T18:50:46.7040611Z";
const W1_PROTOCOL_ID: &str = "w1-btcusdt-spot-1h-20260812-v1";

const TSV_HEADER: [&str; 10] = [
    "source_url",
    "retrieved_at",
    "archive_sha256",
    "observed_archive_sha256",
    "content_sha256",
    "timestamp_unit",
    "expected_first_open",
    "expected_last_close",
    "expected_bar_count",
    "csv_file",
];

#[allow(dead_code)]
pub fn run_legacy_g005_from_env() -> Result<(), Box<dyn Error>> {
    let Some(arguments) = RunnerArguments::parse_legacy_from_env()? else {
        return Ok(());
    };
    run(&arguments)
}

#[allow(dead_code)]
pub fn run_product_cli_from_env() -> Result<(), Box<dyn Error>> {
    let Some(arguments) = RunnerArguments::parse_product_from_env()? else {
        return Ok(());
    };
    run(&arguments)
}

fn run(arguments: &RunnerArguments) -> Result<(), Box<dyn Error>> {
    let protocol = validate_supported_prereg(arguments)?;

    let dataset = load_verified_dataset(&arguments.cache, &arguments.provenance_lock, protocol)?;
    validate_aggregate_dataset(&dataset, protocol)?;

    let split = split_spec(protocol);
    let phase = SelectionPhase::new(&dataset, split.build()?)?;
    validate_split_geometry(&phase, protocol)?;

    let registry = registered_configurations(protocol)?;
    let plan = ExperimentPlan::new(
        &dataset,
        split,
        protocol_spec()?,
        registry.clone(),
        promotion_thresholds()?,
        bootstrap_config(),
        protocol.id,
    )?;
    if plan.runner_version() != arguments.protocol {
        return Err(invalid_data("experiment plan protocol id was not frozen").into());
    }
    let dataset_provenance_fingerprint = plan.dataset_provenance_fingerprint().clone();

    fs::create_dir_all(&arguments.output_dir)?;
    let selection_path = arguments
        .output_dir
        .join(format!("{}-selection.json", arguments.protocol));
    let selected = plan.run_selection(phase)?;
    let sealed_identifiers = selected.selection().sealed_identifiers.clone();
    let mut persisted_selection_digest = None;
    let persisted = selected.persist_selection_with(|frozen_plan, selection| {
        let selection_json =
            render_selection_artifact(&dataset, &registry, frozen_plan, selection, protocol)
                .map_err(|error| selection_persistence_error(&error))?;
        write_synced(&selection_path, selection_json.as_bytes())
            .map_err(|error| selection_persistence_error(&error))?;
        persisted_selection_digest = Some(Sha256Digest::from_bytes(selection_json.as_bytes()));
        Ok(())
    })?;
    let selection_artifact_sha256 = persisted_selection_digest.ok_or_else(|| {
        ExperimentError::SelectionArtifactPersistenceFailed(
            "successful callback did not retain the artifact digest".to_owned(),
        )
    })?;

    // This consuming transition is type-gated by the successful durable write above.
    let mut holdout = persisted.open_final_holdout();
    if holdout.pending_identifiers()
        != sealed_identifiers
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(invalid_data("sealed and pending holdout registries differ").into());
    }
    for identifier in &sealed_identifiers {
        holdout = holdout.evaluate_registered(identifier)?;
    }
    let completed = holdout.finish()?;

    let final_json = render_final_artifact(
        &dataset,
        &registry,
        &completed,
        &selection_artifact_sha256,
        &dataset_provenance_fingerprint,
        protocol,
    )?;
    let final_path = arguments
        .output_dir
        .join(format!("{}-results.json", arguments.protocol));
    write_synced(&final_path, final_json.as_bytes())?;

    let report =
        render_markdown_report(&dataset, &completed, &selection_artifact_sha256, protocol)?;
    let report_path = arguments
        .output_dir
        .join(format!("{}-report.md", arguments.protocol));
    write_synced(&report_path, report.as_bytes())?;

    println!("protocol={}", arguments.protocol);
    println!("cadence={}", arguments.cadence.label());
    println!("plan_fingerprint={}", completed.plan_fingerprint.as_str());
    println!(
        "selection_artifact_sha256={}",
        selection_artifact_sha256.as_str()
    );
    println!("selection_artifact={}", selection_path.display());
    println!("results_artifact={}", final_path.display());
    println!("report_artifact={}", report_path.display());
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunnerArguments {
    protocol: String,
    cadence: ResearchCadence,
    cache: PathBuf,
    provenance_lock: PathBuf,
    output_dir: PathBuf,
}

impl RunnerArguments {
    #[allow(dead_code)]
    fn parse_legacy_from_env() -> Result<Option<Self>, Box<dyn Error>> {
        let mut parsed = Self::parse_arguments(
            env::args().skip(1),
            ParseMode::LegacyG005,
            "Usage: g005_evaluation --cache <CSV cache> --provenance-lock <TSV lock> \
             --output-dir <artifact directory>",
        )?;
        if let Some(arguments) = &mut parsed {
            G005_PROTOCOL_ID.clone_into(&mut arguments.protocol);
            arguments.cadence = ResearchCadence::OneDay;
        }
        Ok(parsed)
    }

    #[allow(dead_code)]
    fn parse_product_from_env() -> Result<Option<Self>, Box<dyn Error>> {
        Self::parse_arguments(
            env::args().skip(1),
            ParseMode::Product,
            "Usage: crypto-trading-research --protocol <protocol id> --cadence <1d|1h> \
             --cache <CSV cache> --provenance-lock <TSV lock> \
             --output-dir <artifact directory>",
        )
    }

    fn parse_arguments(
        mut arguments: impl Iterator<Item = String>,
        mode: ParseMode,
        usage: &str,
    ) -> Result<Option<Self>, Box<dyn Error>> {
        let mut protocol = None;
        let mut cadence = None;
        let mut cache = None;
        let mut provenance_lock = None;
        let mut output_dir = None;

        while let Some(flag) = arguments.next() {
            if matches!(flag.as_str(), "--help" | "-h") {
                println!("{usage}");
                return Ok(None);
            }
            let value = arguments
                .next()
                .ok_or_else(|| invalid_input(format!("missing value for `{flag}`")))?;
            match flag.as_str() {
                "--protocol" => {
                    if mode == ParseMode::LegacyG005 {
                        return Err(invalid_input(
                            "legacy G-005 wrapper does not accept `--protocol`",
                        )
                        .into());
                    }
                    if protocol.replace(value).is_some() {
                        return Err(invalid_input("duplicate argument `--protocol`").into());
                    }
                }
                "--cadence" => {
                    if mode == ParseMode::LegacyG005 {
                        return Err(invalid_input(
                            "legacy G-005 wrapper does not accept `--cadence`",
                        )
                        .into());
                    }
                    if cadence.replace(value).is_some() {
                        return Err(invalid_input("duplicate argument `--cadence`").into());
                    }
                }
                "--cache" | "--cache-dir" => {
                    if cache.replace(PathBuf::from(value)).is_some() {
                        return Err(invalid_input(format!("duplicate argument `{flag}`")).into());
                    }
                }
                "--provenance-lock" => {
                    if provenance_lock.replace(PathBuf::from(value)).is_some() {
                        return Err(invalid_input(format!("duplicate argument `{flag}`")).into());
                    }
                }
                "--output-dir" => {
                    if output_dir.replace(PathBuf::from(value)).is_some() {
                        return Err(invalid_input(format!("duplicate argument `{flag}`")).into());
                    }
                }
                _ => return Err(invalid_input(format!("unknown argument `{flag}`")).into()),
            }
        }

        Ok(Some(Self {
            protocol: match mode {
                ParseMode::LegacyG005 => String::new(),
                ParseMode::Product => {
                    protocol.ok_or_else(|| invalid_input("missing required `--protocol`"))?
                }
            },
            cadence: match mode {
                ParseMode::LegacyG005 => ResearchCadence::OneDay,
                ParseMode::Product => cadence
                    .as_deref()
                    .ok_or_else(|| invalid_input("missing required `--cadence`"))
                    .and_then(ResearchCadence::parse)?,
            },
            cache: cache.ok_or_else(|| invalid_input("missing required `--cache`"))?,
            provenance_lock: provenance_lock
                .ok_or_else(|| invalid_input("missing required `--provenance-lock`"))?,
            output_dir: output_dir
                .ok_or_else(|| invalid_input("missing required `--output-dir`"))?,
        }))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseMode {
    LegacyG005,
    Product,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResearchCadence {
    OneHour,
    OneDay,
}

impl ResearchCadence {
    fn parse(value: &str) -> Result<Self, io::Error> {
        match value {
            "1h" => Ok(Self::OneHour),
            "1d" => Ok(Self::OneDay),
            _ => Err(invalid_input(format!(
                "unsupported cadence `{value}`; expected `1d` or `1h`"
            ))),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::OneHour => "1h",
            Self::OneDay => "1d",
        }
    }
}

fn validate_supported_prereg(
    arguments: &RunnerArguments,
) -> Result<FrozenResearchProtocol, io::Error> {
    match (arguments.protocol.as_str(), arguments.cadence) {
        (G005_PROTOCOL_ID, ResearchCadence::OneDay) => Ok(g005_protocol()),
        (W1_PROTOCOL_ID, ResearchCadence::OneHour) => Err(invalid_input(
            "protocol w1-btcusdt-spot-1h-20260812-v1 is terminally data-admission-aborted and must not be executed",
        )),
        (G005_PROTOCOL_ID, ResearchCadence::OneHour) => Err(invalid_input(
            "protocol g005-btcusdt-spot-20260812-v1 is frozen for `1d` only",
        )),
        (W1_PROTOCOL_ID, ResearchCadence::OneDay) => Err(invalid_input(
            "protocol w1-btcusdt-spot-1h-20260812-v1 is frozen for `1h` only",
        )),
        _ => Err(invalid_input(format!(
            "unsupported protocol/cadence pair `{}` / `{}`",
            arguments.protocol,
            arguments.cadence.label()
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrozenResearchProtocol {
    id: &'static str,
    cadence: ResearchCadence,
    interval_micros: i64,
    source_prefix: &'static str,
    expected_archive_count: usize,
    expected_bar_count: usize,
    expected_window_count: usize,
    expected_final_holdout_start: usize,
    expected_last_selection_test_end: usize,
    fixed_retrieved_at: Option<&'static str>,
    provenance_lock_sha256: Option<&'static str>,
    split: ExperimentSplitSpec,
}

const fn g005_protocol() -> FrozenResearchProtocol {
    FrozenResearchProtocol {
        id: G005_PROTOCOL_ID,
        cadence: ResearchCadence::OneDay,
        interval_micros: 86_400_000_000,
        source_prefix: "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1d",
        expected_archive_count: 103,
        expected_bar_count: 3_134,
        expected_window_count: 9,
        expected_final_holdout_start: 2_769,
        expected_last_selection_test_end: 2_734,
        fixed_retrieved_at: Some(G005_FROZEN_RETRIEVED_AT),
        provenance_lock_sha256: Some(G005_PROVENANCE_LOCK_SHA256),
        split: ExperimentSplitSpec {
            training_len: 1_095,
            test_len: 182,
            step_len: 182,
            embargo_len: 1,
            final_holdout_len: 365,
        },
    }
}

#[cfg_attr(not(test), allow(dead_code))]
const fn w1_protocol() -> FrozenResearchProtocol {
    FrozenResearchProtocol {
        id: W1_PROTOCOL_ID,
        cadence: ResearchCadence::OneHour,
        interval_micros: 3_600_000_000,
        source_prefix: "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h",
        expected_archive_count: 103,
        expected_bar_count: 75_216,
        expected_window_count: 9,
        expected_final_holdout_start: 66_456,
        expected_last_selection_test_end: 65_616,
        fixed_retrieved_at: None,
        provenance_lock_sha256: None,
        split: ExperimentSplitSpec {
            training_len: 26_280,
            test_len: 4_368,
            step_len: 4_368,
            embargo_len: 24,
            final_holdout_len: 8_760,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProvenanceRow {
    source_url: String,
    retrieved_at: String,
    archive_sha256: String,
    observed_archive_sha256: String,
    content_sha256: String,
    timestamp_unit: String,
    expected_first_open: String,
    expected_last_close: String,
    expected_bar_count: String,
    csv_file: String,
}

impl ProvenanceRow {
    fn from_fields(fields: Vec<String>) -> Result<Self, io::Error> {
        let [
            source_url,
            retrieved_at,
            archive_sha256,
            observed_archive_sha256,
            content_sha256,
            timestamp_unit,
            expected_first_open,
            expected_last_close,
            expected_bar_count,
            csv_file,
        ]: [String; 10] = fields
            .try_into()
            .map_err(|_| invalid_data("provenance row must contain exactly ten fields"))?;
        Ok(Self {
            source_url,
            retrieved_at,
            archive_sha256,
            observed_archive_sha256,
            content_sha256,
            timestamp_unit,
            expected_first_open,
            expected_last_close,
            expected_bar_count,
            csv_file,
        })
    }
}

fn load_verified_dataset(
    cache: &Path,
    provenance_lock: &Path,
    protocol: FrozenResearchProtocol,
) -> Result<SpotKlineDataset, Box<dyn Error>> {
    let lock_bytes = fs::read(provenance_lock)?;
    if let Some(expected_sha256) = protocol.provenance_lock_sha256 {
        let expected_lock_digest = Sha256Digest::new(expected_sha256)?;
        if Sha256Digest::from_bytes(&lock_bytes) != expected_lock_digest {
            return Err(
                invalid_data("provenance lock SHA-256 differs from the frozen protocol").into(),
            );
        }
    } else {
        return Err(invalid_data("protocol provenance lock SHA-256 is not frozen").into());
    }
    let lock_text = String::from_utf8(lock_bytes)?;
    let rows = parse_provenance_lock(&lock_text)?;
    if rows.len() != protocol.expected_archive_count {
        return Err(invalid_data(format!(
            "expected {} provenance rows, found {}",
            protocol.expected_archive_count,
            rows.len()
        ))
        .into());
    }

    let frozen_retrieved_at = protocol
        .fixed_retrieved_at
        .map(parse_datetime)
        .transpose()?;
    let mut components = Vec::with_capacity(protocol.expected_archive_count);
    for (index, row) in rows.iter().enumerate() {
        let calendar = expected_month(index, protocol)?;
        let retrieved_at = validate_provenance_row(row, &calendar, protocol, frozen_retrieved_at)?;

        let archive_sha256 = Sha256Digest::new(&row.archive_sha256)?;
        let observed_archive_sha256 = Sha256Digest::new(&row.observed_archive_sha256)?;
        if archive_sha256 != observed_archive_sha256 {
            return Err(invalid_data(format!(
                "recorded archive checksum mismatch for {}",
                row.source_url
            ))
            .into());
        }
        let content_sha256 = Sha256Digest::new(&row.content_sha256)?;
        let csv_path = cache.join(&row.csv_file);
        let csv_bytes = fs::read(&csv_path)?;
        if Sha256Digest::from_bytes(&csv_bytes) != content_sha256 {
            return Err(
                invalid_data(format!("cached CSV checksum mismatch for {}", row.csv_file)).into(),
            );
        }
        let csv = String::from_utf8(csv_bytes)?;
        let manifest = DatasetManifest {
            source_url: row.source_url.clone(),
            retrieved_at,
            venue: "binance".to_owned(),
            product: MarketType::Spot,
            symbol: Symbol::new("BTCUSDT")?,
            interval_micros: protocol.interval_micros,
            timezone: "UTC".to_owned(),
            timestamp_unit: calendar.timestamp_unit,
            archive_sha256: archive_sha256.clone(),
            content_sha256,
            parser_version: PARSER_VERSION.to_owned(),
            expected_first_open: calendar.first_open,
            expected_last_close: calendar.last_close,
            expected_bar_count: calendar.bar_count,
        };
        components.push(SpotKlineDataset::parse_csv(
            manifest,
            &csv,
            &archive_sha256,
            retrieved_at,
        )?);
    }

    Ok(SpotKlineDataset::merge_verified(components)?)
}

fn parse_provenance_lock(lock: &str) -> Result<Vec<ProvenanceRow>, io::Error> {
    let mut lines = lock.lines();
    let header = lines
        .next()
        .ok_or_else(|| invalid_data("provenance lock is empty"))?;
    let mut parsed_header = parse_tsv_line(header.trim_end_matches('\r'))?;
    if let Some(first) = parsed_header.first_mut() {
        *first = first.trim_start_matches('\u{feff}').to_owned();
    }
    if parsed_header != TSV_HEADER {
        return Err(invalid_data("provenance lock header is not canonical"));
    }

    lines
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            parse_tsv_line(line.trim_end_matches('\r')).and_then(ProvenanceRow::from_fields)
        })
        .collect()
}

fn parse_tsv_line(line: &str) -> Result<Vec<String>, io::Error> {
    let mut characters = line.chars().peekable();
    let mut fields = Vec::new();
    loop {
        if characters.next() != Some('"') {
            return Err(invalid_data("TSV fields must use canonical CSV quoting"));
        }
        let mut field = String::new();
        loop {
            match characters.next() {
                Some('"') if characters.peek() == Some(&'"') => {
                    characters.next();
                    field.push('"');
                }
                Some('"') => break,
                Some(character) => field.push(character),
                None => return Err(invalid_data("unterminated quoted TSV field")),
            }
        }
        fields.push(field);
        match characters.next() {
            Some('\t') => {}
            None => break,
            _ => return Err(invalid_data("unexpected data after quoted TSV field")),
        }
    }
    Ok(fields)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpectedMonth {
    first_open: DateTime<Utc>,
    last_close: DateTime<Utc>,
    bar_count: usize,
    timestamp_unit: TimestampUnit,
    year: i32,
    month: u32,
}

fn expected_month(
    index: usize,
    protocol: FrozenResearchProtocol,
) -> Result<ExpectedMonth, io::Error> {
    let absolute_month = 2018_i32
        .checked_mul(12)
        .and_then(|value| value.checked_add(i32::try_from(index).ok()?))
        .ok_or_else(|| invalid_data("archive month index overflow"))?;
    let year = absolute_month.div_euclid(12);
    let month = u32::try_from(absolute_month.rem_euclid(12) + 1)
        .map_err(|_| invalid_data("archive month is invalid"))?;
    let first_open = utc_month_start(year, month)?;
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let next_open = utc_month_start(next_year, next_month)?;
    let timestamp_unit = if year < 2025 {
        TimestampUnit::Milliseconds
    } else {
        TimestampUnit::Microseconds
    };
    let last_close = match timestamp_unit {
        TimestampUnit::Milliseconds => next_open - Duration::milliseconds(1),
        TimestampUnit::Microseconds => next_open - Duration::microseconds(1),
    };
    let interval_hours = protocol.interval_micros / 3_600_000_000;
    let bar_count = if interval_hours == 24 {
        usize::try_from((next_open - first_open).num_days())
    } else {
        usize::try_from((next_open - first_open).num_hours())
    }
    .map_err(|_| invalid_data("calendar month bar count is invalid"))?;
    Ok(ExpectedMonth {
        first_open,
        last_close,
        bar_count,
        timestamp_unit,
        year,
        month,
    })
}

fn utc_month_start(year: i32, month: u32) -> Result<DateTime<Utc>, io::Error> {
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| invalid_data("calendar month is outside the supported UTC range"))
}

fn validate_provenance_row(
    row: &ProvenanceRow,
    calendar: &ExpectedMonth,
    protocol: FrozenResearchProtocol,
    frozen_retrieved_at: Option<DateTime<Utc>>,
) -> Result<DateTime<Utc>, Box<dyn Error>> {
    let month_label = format!("{:04}-{:02}", calendar.year, calendar.month);
    let cadence = protocol.cadence.label();
    let expected_csv = format!("BTCUSDT-{cadence}-{month_label}.csv");
    let expected_url = format!(
        "{}/BTCUSDT-{cadence}-{month_label}.zip",
        protocol.source_prefix
    );
    let expected_unit = match calendar.timestamp_unit {
        TimestampUnit::Milliseconds => "milliseconds",
        TimestampUnit::Microseconds => "microseconds",
    };
    let expected_count = row.expected_bar_count.parse::<usize>()?;
    let first_open = parse_datetime(&row.expected_first_open)?;
    let last_close = parse_datetime(&row.expected_last_close)?;
    let retrieved_at = parse_datetime(&row.retrieved_at)?;

    if row.csv_file != expected_csv
        || row.source_url != expected_url
        || row.timestamp_unit != expected_unit
        || expected_count != calendar.bar_count
        || first_open != calendar.first_open
        || last_close != calendar.last_close
    {
        return Err(invalid_data(format!(
            "provenance row violates frozen calendar/source contract for {month_label}"
        ))
        .into());
    }
    if frozen_retrieved_at.is_some_and(|frozen| retrieved_at != frozen) {
        return Err(invalid_data(format!(
            "provenance row retrieval time violates the frozen lock for {month_label}"
        ))
        .into());
    }
    Ok(retrieved_at)
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|timestamp| timestamp.with_timezone(&Utc))
}

fn validate_aggregate_dataset(
    dataset: &SpotKlineDataset,
    protocol: FrozenResearchProtocol,
) -> Result<(), Box<dyn Error>> {
    if dataset.manifests().len() != protocol.expected_archive_count
        || dataset.bars().len() != protocol.expected_bar_count
    {
        return Err(invalid_data("merged dataset size differs from the frozen protocol").into());
    }
    let first_manifest = dataset
        .manifests()
        .first()
        .ok_or_else(|| invalid_data("merged dataset has no first manifest"))?;
    let last_manifest = dataset
        .manifests()
        .last()
        .ok_or_else(|| invalid_data("merged dataset has no last manifest"))?;
    let cadence = protocol.cadence.label();
    if first_manifest.source_url
        != format!("{}/BTCUSDT-{cadence}-2018-01.zip", protocol.source_prefix)
        || last_manifest.source_url
            != format!("{}/BTCUSDT-{cadence}-2026-07.zip", protocol.source_prefix)
        || first_manifest.expected_first_open != utc_month_start(2018, 1)?
        || last_manifest.expected_last_close
            != expected_month(protocol.expected_archive_count - 1, protocol)?.last_close
    {
        return Err(invalid_data("merged dataset endpoint sources are not frozen").into());
    }
    Ok(())
}

fn validate_split_geometry(
    phase: &SelectionPhase<'_>,
    protocol: FrozenResearchProtocol,
) -> Result<(), io::Error> {
    let windows = phase.plan().windows();
    let final_holdout = phase.plan().final_holdout_range();
    if windows.len() != protocol.expected_window_count
        || final_holdout != (protocol.expected_final_holdout_start..protocol.expected_bar_count)
        || windows.last().map(|window| window.test_range.end)
            != Some(protocol.expected_last_selection_test_end)
    {
        return Err(invalid_data(
            "evaluation split differs from the frozen geometry",
        ));
    }
    Ok(())
}

fn split_spec(protocol: FrozenResearchProtocol) -> ExperimentSplitSpec {
    protocol.split
}

fn protocol_spec() -> Result<EvaluationProtocolSpec, rust_decimal::Error> {
    Ok(EvaluationProtocolSpec {
        initial_cash: Money::new(decimal("10000")?),
        one_x_costs: CostScheduleSpec {
            fee_bps: decimal("10")?,
            half_spread_bps: decimal("2")?,
            slippage_bps: decimal("4")?,
            latency_bps: decimal("4")?,
        },
    })
}

fn bootstrap_config() -> BootstrapConfig {
    BootstrapConfig {
        replicates: 10_000,
        base_seed: 0x4750_3035_2026_0812,
    }
}

fn promotion_thresholds() -> Result<PromotionThresholds, rust_decimal::Error> {
    Ok(PromotionThresholds {
        selection_median_sharpe_min: decimal("1.0")?,
        holdout_profit_factor_min: decimal("1.2")?,
        holdout_max_drawdown_max: decimal("0.20")?,
        selection_positive_window_ratio_min: decimal("0.60")?,
    })
}

fn registered_configurations(
    protocol: FrozenResearchProtocol,
) -> Result<Vec<RegisteredConfiguration>, Box<dyn Error>> {
    let mut registry = vec![
        RegisteredConfiguration::new("cash", SpotStrategyConfig::Cash)?,
        RegisteredConfiguration::new("buy-and-hold", SpotStrategyConfig::BuyAndHold)?,
    ];
    let (momentum_lookbacks, momentum_rebalance, donchian_lookbacks, vol_lookbacks, vol_rebalance) =
        match protocol.cadence {
            ResearchCadence::OneDay => (
                vec![28, 56, 84, 112, 168],
                7,
                vec![20, 60, 120],
                vec![20, 60],
                7,
            ),
            ResearchCadence::OneHour => (
                vec![672, 1_344, 2_016, 2_688, 4_032],
                168,
                vec![480, 1_440, 2_880],
                vec![480, 1_440],
                168,
            ),
        };
    for lookback in momentum_lookbacks {
        registry.push(RegisteredConfiguration::new(
            format!("tsm-lb{lookback:03}-rb{momentum_rebalance:03}"),
            SpotStrategyConfig::SlowTimeSeriesMomentum {
                lookback_bars: lookback,
                rebalance_every_bars: momentum_rebalance,
            },
        )?);
    }
    for lookback in donchian_lookbacks {
        registry.push(RegisteredConfiguration::new(
            format!("donchian-lb{lookback:03}"),
            SpotStrategyConfig::LongOnlyDonchian {
                lookback_bars: lookback,
            },
        )?);
    }
    for lookback in vol_lookbacks {
        for (annual_target, target_code) in [("0.10", "10"), ("0.15", "15"), ("0.20", "20")] {
            for (rebalance_band, band_code) in [("0.00", "00"), ("0.20", "20")] {
                let strategy = match protocol.cadence {
                    ResearchCadence::OneDay => SpotStrategyConfig::CappedVolatilityTarget {
                        lookback_returns: lookback,
                        annual_target: decimal(annual_target)?,
                        rebalance_band: decimal(rebalance_band)?,
                        rebalance_every_bars: vol_rebalance,
                    },
                    ResearchCadence::OneHour => {
                        SpotStrategyConfig::CappedVolatilityTargetExplicitAnnualization {
                            lookback_returns: lookback,
                            annual_target: decimal(annual_target)?,
                            rebalance_band: decimal(rebalance_band)?,
                            rebalance_every_bars: vol_rebalance,
                            periods_per_year: Decimal::from(8_760_u32),
                        }
                    }
                };
                registry.push(RegisteredConfiguration::new(
                    format!("vol-lb{lookback:03}-t{target_code}-b{band_code}-rb{vol_rebalance:03}"),
                    strategy,
                )?);
            }
        }
    }
    Ok(registry)
}

fn decimal(value: &str) -> Result<Decimal, rust_decimal::Error> {
    Decimal::from_str(value)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temp_path = temporary_write_path(path)?;

    let write_result = (|| -> Result<(), io::Error> {
        let mut file = File::options()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        atomic_replace(&temp_path, path)?;
        sync_parent_dir(parent)
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn temporary_write_path(path: &Path) -> Result<PathBuf, io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(OsString::from)
        .ok_or_else(|| invalid_input("artifact path must include a file name"))?;
    for attempt in 0..1_000_u32 {
        let mut candidate = file_name.clone();
        candidate.push(format!(".tmp-{}-{attempt}", process::id()));
        let candidate = parent.join(candidate);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(invalid_input(format!(
        "could not allocate a temporary artifact path for {}",
        path.display()
    )))
}

fn atomic_replace(temp_path: &Path, destination: &Path) -> Result<(), io::Error> {
    fs::rename(temp_path, destination)
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<(), io::Error> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(path: &Path) -> Result<(), io::Error> {
    // Windows does not expose the Unix directory-fsync primitive through
    // `std`, but still validate that the replacement's parent remains
    // reachable before reporting the artifact write as complete.
    fs::metadata(path).map(|_| ())
}

fn render_selection_artifact(
    dataset: &SpotKlineDataset,
    registry: &[RegisteredConfiguration],
    plan: &ExperimentPlan,
    selection: &SelectionSummary,
    protocol: FrozenResearchProtocol,
) -> Result<String, io::Error> {
    let mut json = JsonWriter::new();
    json.start_object(None)?;
    write_artifact_header(
        &mut json,
        "selection",
        "selection_complete_holdout_unopened",
        plan.plan_fingerprint(),
        plan.dataset_provenance_fingerprint(),
        protocol,
    )?;
    write_frozen_protocol(&mut json, protocol)?;
    write_dataset_provenance(&mut json, dataset, protocol)?;
    write_registry(&mut json, registry)?;
    write_selection_summary(&mut json, "selection", selection)?;
    json.end_object()?;
    json.finish()
}

fn render_final_artifact(
    dataset: &SpotKlineDataset,
    registry: &[RegisteredConfiguration],
    completed: &CompletedExperiment,
    selection_artifact_sha256: &Sha256Digest,
    dataset_fingerprint: &Sha256Digest,
    protocol: FrozenResearchProtocol,
) -> Result<String, io::Error> {
    let mut json = JsonWriter::new();
    json.start_object(None)?;
    write_artifact_header(
        &mut json,
        "final_results",
        "single_holdout_complete",
        &completed.plan_fingerprint,
        dataset_fingerprint,
        protocol,
    )?;
    json.string(
        Some("selection_artifact_sha256"),
        selection_artifact_sha256.as_str(),
    )?;
    write_frozen_protocol(&mut json, protocol)?;
    write_dataset_provenance(&mut json, dataset, protocol)?;
    write_registry(&mut json, registry)?;
    write_selection_summary(&mut json, "selection", &completed.selection)?;
    write_holdout_outcomes(&mut json, completed)?;
    json.end_object()?;
    json.finish()
}

fn write_artifact_header(
    json: &mut JsonWriter,
    artifact_type: &str,
    status: &str,
    plan_fingerprint: &Sha256Digest,
    dataset_fingerprint: &Sha256Digest,
    protocol: FrozenResearchProtocol,
) -> Result<(), io::Error> {
    json.number(Some("schema_version"), SCHEMA_VERSION)?;
    json.string(Some("protocol_id"), protocol.id)?;
    json.string(Some("runner_version"), protocol.id)?;
    json.string(Some("artifact_type"), artifact_type)?;
    json.string(Some("status"), status)?;
    json.string(Some("plan_fingerprint"), plan_fingerprint.as_str())?;
    json.string(
        Some("dataset_provenance_fingerprint"),
        dataset_fingerprint.as_str(),
    )
}

fn write_frozen_protocol(
    json: &mut JsonWriter,
    protocol: FrozenResearchProtocol,
) -> Result<(), io::Error> {
    json.start_object(Some("frozen_protocol"))?;
    json.string(Some("market"), "binance_spot")?;
    json.string(Some("symbol"), "BTCUSDT")?;
    json.string(Some("interval"), protocol.cadence.label())?;
    json.string(Some("timezone"), "UTC")?;
    json.decimal(Some("initial_cash_usdt"), decimal_infallible("10000"))?;

    json.start_object(Some("one_x_cost_bps"))?;
    write_cost_schedule(json, 10, 2, 4, 4)?;
    json.end_object()?;
    json.start_object(Some("two_x_cost_bps"))?;
    write_cost_schedule(json, 20, 4, 8, 8)?;
    json.end_object()?;

    let split = split_spec(protocol);
    json.start_object(Some("split"))?;
    json.number(Some("training_len"), split.training_len)?;
    json.number(Some("test_len"), split.test_len)?;
    json.number(Some("step_len"), split.step_len)?;
    json.number(Some("embargo_len"), split.embargo_len)?;
    json.number(Some("final_holdout_len"), split.final_holdout_len)?;
    json.end_object()?;

    let bootstrap = bootstrap_config();
    json.start_object(Some("bootstrap"))?;
    json.number(Some("replicates"), bootstrap.replicates)?;
    json.string(
        Some("base_seed_hex"),
        &format!("0x{:016x}", bootstrap.base_seed),
    )?;
    json.string(Some("method"), "window_level_percentile_median_95")?;
    json.end_object()?;

    json.start_object(Some("promotion_thresholds"))?;
    json.decimal(
        Some("selection_median_sharpe_min"),
        decimal_infallible("1.0"),
    )?;
    json.decimal(Some("holdout_profit_factor_min"), decimal_infallible("1.2"))?;
    json.decimal(Some("holdout_max_drawdown_max"), decimal_infallible("0.20"))?;
    json.decimal(
        Some("selection_positive_window_ratio_min"),
        decimal_infallible("0.60"),
    )?;
    json.end_object()?;

    json.start_object(Some("selection_rule"))?;
    json.number(Some("minimum_available_sharpe_observations"), 6)?;
    json.string(
        Some("eligibility"),
        "strictly_positive_median_one_x_and_two_x_net_return_with_available_sharpe",
    )?;
    json.start_array(Some("rank_order"))?;
    for field in [
        "median_one_x_sharpe_desc",
        "positive_window_ratio_desc",
        "median_delta_vs_buy_and_hold_desc",
        "worst_drawdown_asc",
        "median_turnover_asc",
        "identifier_asc",
    ] {
        json.string(None, field)?;
    }
    json.end_array()?;
    json.end_object()?;
    json.end_object()
}

fn write_cost_schedule(
    json: &mut JsonWriter,
    fee: usize,
    half_spread: usize,
    slippage: usize,
    latency: usize,
) -> Result<(), io::Error> {
    json.number(Some("fee"), fee)?;
    json.number(Some("half_spread"), half_spread)?;
    json.number(Some("slippage"), slippage)?;
    json.number(Some("latency"), latency)
}

fn write_dataset_provenance(
    json: &mut JsonWriter,
    dataset: &SpotKlineDataset,
    protocol: FrozenResearchProtocol,
) -> Result<(), io::Error> {
    json.start_object(Some("dataset"))?;
    if let Some(lock_sha256) = protocol.provenance_lock_sha256 {
        json.string(Some("provenance_lock_sha256"), lock_sha256)?;
    } else {
        json.null(Some("provenance_lock_sha256"))?;
    }
    json.number(Some("manifest_count"), dataset.manifests().len())?;
    json.number(Some("bar_count"), dataset.bars().len())?;
    json.start_array(Some("manifests"))?;
    for manifest in dataset.manifests() {
        json.start_object(None)?;
        json.string(Some("source_url"), &manifest.source_url)?;
        json.string(
            Some("retrieved_at"),
            &manifest
                .retrieved_at
                .to_rfc3339_opts(SecondsFormat::Nanos, true),
        )?;
        json.string(Some("venue"), &manifest.venue)?;
        json.string(Some("product"), "spot")?;
        json.string(Some("symbol"), manifest.symbol.as_str())?;
        json.signed_number(Some("interval_micros"), manifest.interval_micros)?;
        json.string(Some("timezone"), &manifest.timezone)?;
        json.string(
            Some("timestamp_unit"),
            match manifest.timestamp_unit {
                TimestampUnit::Milliseconds => "milliseconds",
                TimestampUnit::Microseconds => "microseconds",
            },
        )?;
        json.string(Some("archive_sha256"), manifest.archive_sha256.as_str())?;
        json.string(Some("content_sha256"), manifest.content_sha256.as_str())?;
        json.string(Some("parser_version"), &manifest.parser_version)?;
        json.string(
            Some("expected_first_open"),
            &manifest
                .expected_first_open
                .to_rfc3339_opts(SecondsFormat::Nanos, true),
        )?;
        json.string(
            Some("expected_last_close"),
            &manifest
                .expected_last_close
                .to_rfc3339_opts(SecondsFormat::Nanos, true),
        )?;
        json.number(Some("expected_bar_count"), manifest.expected_bar_count)?;
        json.end_object()?;
    }
    json.end_array()?;
    json.end_object()
}

fn write_registry(
    json: &mut JsonWriter,
    registry: &[RegisteredConfiguration],
) -> Result<(), io::Error> {
    json.start_array(Some("registry"))?;
    for configuration in registry {
        json.start_object(None)?;
        json.string(Some("identifier"), configuration.identifier())?;
        json.string(Some("family"), configuration.family())?;
        write_strategy(json, configuration.strategy())?;
        json.end_object()?;
    }
    json.end_array()
}

fn write_strategy(json: &mut JsonWriter, strategy: SpotStrategyConfig) -> Result<(), io::Error> {
    json.start_object(Some("strategy"))?;
    match strategy {
        SpotStrategyConfig::Cash => json.string(Some("kind"), "cash")?,
        SpotStrategyConfig::BuyAndHold => json.string(Some("kind"), "buy_and_hold")?,
        SpotStrategyConfig::SlowTimeSeriesMomentum {
            lookback_bars,
            rebalance_every_bars,
        } => {
            json.string(Some("kind"), "slow_time_series_momentum")?;
            json.number(Some("lookback_bars"), lookback_bars)?;
            json.number(Some("rebalance_every_bars"), rebalance_every_bars)?;
        }
        SpotStrategyConfig::LongOnlyDonchian { lookback_bars } => {
            json.string(Some("kind"), "long_only_donchian")?;
            json.number(Some("lookback_bars"), lookback_bars)?;
        }
        SpotStrategyConfig::CappedVolatilityTarget {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
        } => {
            json.string(Some("kind"), "capped_volatility_target")?;
            json.number(Some("lookback_returns"), lookback_returns)?;
            json.decimal(Some("annual_target"), annual_target)?;
            json.decimal(Some("rebalance_band"), rebalance_band)?;
            json.number(Some("rebalance_every_bars"), rebalance_every_bars)?;
        }
        SpotStrategyConfig::CappedVolatilityTargetExplicitAnnualization {
            lookback_returns,
            annual_target,
            rebalance_band,
            rebalance_every_bars,
            periods_per_year,
        } => {
            json.string(Some("kind"), "capped_volatility_target")?;
            json.number(Some("lookback_returns"), lookback_returns)?;
            json.decimal(Some("annual_target"), annual_target)?;
            json.decimal(Some("rebalance_band"), rebalance_band)?;
            json.number(Some("rebalance_every_bars"), rebalance_every_bars)?;
            json.decimal(Some("periods_per_year"), periods_per_year)?;
        }
    }
    json.end_object()
}

fn write_selection_summary(
    json: &mut JsonWriter,
    key: &str,
    selection: &SelectionSummary,
) -> Result<(), io::Error> {
    json.start_object(Some(key))?;
    json.start_array(Some("window_ranges"))?;
    for range in &selection.window_ranges {
        write_range(json, None, range)?;
    }
    json.end_array()?;

    json.start_array(Some("family_selections"))?;
    for family in &selection.family_selections {
        json.start_object(None)?;
        json.string(Some("family"), &family.family)?;
        json.optional_string(
            Some("winner_identifier"),
            family.winner_identifier.as_deref(),
        )?;
        json.end_object()?;
    }
    json.end_array()?;

    json.start_array(Some("sealed_identifiers"))?;
    for identifier in &selection.sealed_identifiers {
        json.string(None, identifier)?;
    }
    json.end_array()?;

    json.start_array(Some("configurations"))?;
    for configuration in &selection.configurations {
        write_configuration_summary(json, configuration)?;
    }
    json.end_array()?;
    json.end_object()
}

fn write_configuration_summary(
    json: &mut JsonWriter,
    summary: &ConfigurationSelectionSummary,
) -> Result<(), io::Error> {
    json.start_object(None)?;
    json.string(Some("identifier"), &summary.identifier)?;
    json.string(Some("family"), &summary.family)?;
    json.boolean(
        Some("family_winner_eligible"),
        summary.family_winner_eligible,
    )?;
    json.boolean(Some("selected_for_holdout"), summary.selected_for_holdout)?;
    write_aggregate_metrics(json, &summary.aggregates)?;
    json.start_array(Some("window_results"))?;
    for window in &summary.window_results {
        write_window_result(json, window)?;
    }
    json.end_array()?;
    json.end_object()
}

fn write_aggregate_metrics(
    json: &mut JsonWriter,
    metrics: &AggregateSelectionMetrics,
) -> Result<(), io::Error> {
    json.start_object(Some("aggregates"))?;
    json.decimal(Some("median_net_return"), metrics.median_net_return)?;
    json.decimal(Some("worst_net_return"), metrics.worst_net_return)?;
    json.optional_decimal(Some("median_sharpe"), metrics.median_sharpe)?;
    write_bootstrap_interval(
        json,
        "sharpe_bootstrap_95",
        metrics.sharpe_bootstrap_95.as_ref(),
    )?;
    json.optional_decimal(Some("median_sortino"), metrics.median_sortino)?;
    write_bootstrap_interval(
        json,
        "sortino_bootstrap_95",
        metrics.sortino_bootstrap_95.as_ref(),
    )?;
    json.decimal(Some("positive_window_ratio"), metrics.positive_window_ratio)?;
    json.optional_decimal(Some("worst_drawdown"), metrics.worst_drawdown)?;
    json.decimal(Some("median_turnover"), metrics.median_turnover)?;
    json.decimal(Some("median_trade_count"), metrics.median_trade_count)?;
    json.decimal(Some("median_exposure"), metrics.median_exposure)?;
    json.decimal(Some("median_delta_vs_cash"), metrics.median_delta_vs_cash)?;
    json.decimal(
        Some("median_delta_vs_buy_and_hold"),
        metrics.median_delta_vs_buy_and_hold,
    )?;
    json.decimal(
        Some("median_two_x_net_return"),
        metrics.median_two_x_net_return,
    )?;
    json.number(
        Some("available_sharpe_observations"),
        metrics.available_sharpe_observations,
    )?;
    json.end_object()
}

fn write_bootstrap_interval(
    json: &mut JsonWriter,
    key: &str,
    interval: Option<&BootstrapInterval>,
) -> Result<(), io::Error> {
    let Some(interval) = interval else {
        return json.null(Some(key));
    };
    json.start_object(Some(key))?;
    json.decimal(Some("lower"), interval.lower)?;
    json.decimal(Some("upper"), interval.upper)?;
    json.end_object()
}

fn write_window_result(
    json: &mut JsonWriter,
    window: &SelectionWindowResult,
) -> Result<(), io::Error> {
    json.start_object(None)?;
    json.number(Some("window_index"), window.window_index)?;
    write_range(json, Some("range"), &window.range)?;
    json.decimal(Some("one_x_delta_vs_cash"), window.one_x_delta_vs_cash)?;
    json.decimal(
        Some("one_x_delta_vs_buy_and_hold"),
        window.one_x_delta_vs_buy_and_hold,
    )?;
    write_cost_sensitivity(json, "evaluation", &window.evaluation)?;
    json.end_object()
}

fn write_range(
    json: &mut JsonWriter,
    key: Option<&str>,
    range: &Range<usize>,
) -> Result<(), io::Error> {
    json.start_object(key)?;
    json.number(Some("start"), range.start)?;
    json.number(Some("end_exclusive"), range.end)?;
    json.end_object()
}

fn write_cost_sensitivity(
    json: &mut JsonWriter,
    key: &str,
    evaluation: &CostSensitivityEvaluation,
) -> Result<(), io::Error> {
    json.start_object(Some(key))?;
    write_causal_evaluation(json, "one_x", &evaluation.one_x)?;
    write_causal_evaluation(json, "two_x", &evaluation.two_x)?;
    json.end_object()
}

fn write_causal_evaluation(
    json: &mut JsonWriter,
    key: &str,
    evaluation: &CausalSpotEvaluation,
) -> Result<(), io::Error> {
    let metrics = &evaluation.metrics;
    json.start_object(Some(key))?;
    json.decimal(Some("ending_equity"), metrics.ending_equity.as_decimal())?;
    json.decimal(Some("net_return"), metrics.net_return)?;
    json.decimal(Some("turnover"), metrics.turnover)?;
    json.number(Some("trade_count"), metrics.trade_count)?;
    json.decimal(Some("average_exposure"), metrics.average_exposure)?;
    json.optional_decimal(Some("periods_per_year"), metrics.periods_per_year)?;
    json.optional_decimal(Some("annualized_volatility"), metrics.annualized_volatility)?;
    write_cost_breakdown(json, &metrics.total_costs)?;

    json.start_object(Some("performance"))?;
    let performance = &metrics.performance;
    if let Some(drawdown) = &performance.max_drawdown {
        json.start_object(Some("max_drawdown"))?;
        json.decimal(Some("peak"), drawdown.peak)?;
        json.decimal(Some("trough"), drawdown.trough)?;
        json.decimal(Some("amount"), drawdown.amount)?;
        json.decimal(Some("ratio"), drawdown.ratio)?;
        json.end_object()?;
    } else {
        json.null(Some("max_drawdown"))?;
    }
    json.optional_decimal(Some("win_rate"), performance.win_rate)?;
    json.optional_decimal(Some("profit_factor"), performance.profit_factor)?;
    json.optional_decimal(Some("sharpe_ratio"), performance.sharpe_ratio)?;
    json.optional_decimal(Some("sortino_ratio"), performance.sortino_ratio)?;
    json.end_object()?;
    json.end_object()
}

fn write_cost_breakdown(json: &mut JsonWriter, costs: &CostBreakdown) -> Result<(), io::Error> {
    json.start_object(Some("total_costs"))?;
    json.decimal(Some("fee"), costs.fee.as_decimal())?;
    json.decimal(Some("half_spread"), costs.half_spread.as_decimal())?;
    json.decimal(Some("slippage"), costs.slippage.as_decimal())?;
    json.decimal(Some("latency"), costs.latency.as_decimal())?;
    json.decimal(Some("total"), costs.total.as_decimal())?;
    json.end_object()
}

fn write_holdout_outcomes(
    json: &mut JsonWriter,
    completed: &CompletedExperiment,
) -> Result<(), io::Error> {
    json.start_object(Some("final_holdout"))?;
    json.boolean(Some("consumed_once"), true)?;
    json.boolean(Some("any_promising"), completed.any_promising)?;
    json.boolean(Some("no_candidate_passed"), !completed.any_promising)?;
    json.start_array(Some("promising_identifiers"))?;
    for outcome in completed.outcomes.iter().filter(|outcome| {
        outcome
            .promising
            .as_ref()
            .is_some_and(|decision| decision.passed)
    }) {
        json.string(None, &outcome.identifier)?;
    }
    json.end_array()?;
    json.start_array(Some("outcomes"))?;
    for outcome in &completed.outcomes {
        write_holdout_outcome(json, outcome)?;
    }
    json.end_array()?;
    json.end_object()
}

fn write_holdout_outcome(
    json: &mut JsonWriter,
    outcome: &FinalHoldoutOutcome,
) -> Result<(), io::Error> {
    json.start_object(None)?;
    json.string(Some("identifier"), &outcome.identifier)?;
    json.string(Some("family"), &outcome.family)?;
    write_cost_sensitivity(json, "evaluation", &outcome.evaluation)?;
    if let Some(promising) = &outcome.promising {
        json.start_object(Some("promising"))?;
        json.boolean(Some("passed"), promising.passed)?;
        json.start_array(Some("conditions"))?;
        for condition in &promising.conditions {
            json.start_object(None)?;
            json.string(Some("name"), condition.name)?;
            json.boolean(Some("passed"), condition.passed)?;
            json.end_object()?;
        }
        json.end_array()?;
        json.end_object()?;
    } else {
        json.null(Some("promising"))?;
    }
    json.end_object()
}

fn render_markdown_report(
    dataset: &SpotKlineDataset,
    completed: &CompletedExperiment,
    selection_artifact_sha256: &Sha256Digest,
    protocol: FrozenResearchProtocol,
) -> Result<String, std::fmt::Error> {
    let mut report = String::new();
    let protocol_label = if protocol.id == G005_PROTOCOL_ID {
        "G-005".to_owned()
    } else {
        format!("W1 {}", protocol.cadence.label())
    };
    let selection_window_label = if protocol.expected_window_count == 9 {
        "nine".to_owned()
    } else {
        protocol.expected_window_count.to_string()
    };
    writeln!(report, "# {protocol_label} BTCUSDT Spot Offline Evaluation")?;
    writeln!(report)?;
    writeln!(report, "- Protocol: `{}`", protocol.id)?;
    writeln!(
        report,
        "- Plan fingerprint: `{}`",
        completed.plan_fingerprint.as_str()
    )?;
    writeln!(
        report,
        "- Selection artifact SHA-256: `{}`",
        selection_artifact_sha256.as_str()
    )?;
    writeln!(
        report,
        "- Verified dataset: {} ordered manifests / {} {} bars",
        dataset.manifests().len(),
        dataset.bars().len(),
        if protocol.cadence == ResearchCadence::OneDay {
            "daily"
        } else {
            "hourly"
        }
    )?;
    writeln!(
        report,
        "- Evaluation: {} selection OOS windows, one consuming {}-bar final holdout, 1x/2x costs",
        selection_window_label, protocol.split.final_holdout_len
    )?;
    writeln!(report)?;
    writeln!(
        report,
        "This is a deterministic offline research artifact. It contains aggregate metrics and provenance only; raw price rows and credentials are omitted."
    )?;
    writeln!(report)?;

    write_selection_markdown(&mut report, &completed.selection)?;
    write_holdout_markdown(&mut report, &completed.outcomes)?;
    write_conclusion_markdown(&mut report, completed)?;
    Ok(report)
}

fn write_selection_markdown(
    report: &mut String,
    selection: &SelectionSummary,
) -> Result<(), std::fmt::Error> {
    writeln!(report, "## Selection")?;
    writeln!(report)?;
    writeln!(
        report,
        "| Configuration | Family | Median 1x return | Median Sharpe | Median 2x return | Positive windows | Eligible | Holdout |"
    )?;
    writeln!(
        report,
        "| --- | --- | ---: | ---: | ---: | ---: | --- | --- |"
    )?;
    for configuration in &selection.configurations {
        let metrics = &configuration.aggregates;
        writeln!(
            report,
            "| `{}` | `{}` | {} | {} | {} | {} | {} | {} |",
            configuration.identifier,
            configuration.family,
            decimal_string(metrics.median_net_return),
            optional_decimal_string(metrics.median_sharpe),
            decimal_string(metrics.median_two_x_net_return),
            decimal_string(metrics.positive_window_ratio),
            yes_no(configuration.family_winner_eligible),
            yes_no(configuration.selected_for_holdout),
        )?;
    }
    writeln!(report)?;

    writeln!(report, "### Frozen family decisions")?;
    writeln!(report)?;
    for family in &selection.family_selections {
        writeln!(
            report,
            "- `{}`: {}",
            family.family,
            family
                .winner_identifier
                .as_deref()
                .map_or("rejected before holdout", |winner| winner)
        )?;
    }
    writeln!(report)
}

fn write_holdout_markdown(
    report: &mut String,
    outcomes: &[FinalHoldoutOutcome],
) -> Result<(), std::fmt::Error> {
    writeln!(report, "## Single final holdout")?;
    writeln!(report)?;
    writeln!(
        report,
        "| Configuration | 1x net return | 2x net return | 1x profit factor | 1x max drawdown | Promising |"
    )?;
    writeln!(report, "| --- | ---: | ---: | ---: | ---: | --- |")?;
    for outcome in outcomes {
        let one_x = &outcome.evaluation.one_x.metrics;
        let two_x = &outcome.evaluation.two_x.metrics;
        writeln!(
            report,
            "| `{}` | {} | {} | {} | {} | {} |",
            outcome.identifier,
            decimal_string(one_x.net_return),
            decimal_string(two_x.net_return),
            optional_decimal_string(one_x.performance.profit_factor),
            one_x.performance.max_drawdown.as_ref().map_or_else(
                || "N/A".to_owned(),
                |drawdown| decimal_string(drawdown.ratio)
            ),
            outcome
                .promising
                .as_ref()
                .map_or("baseline", |decision| yes_no(decision.passed)),
        )?;
    }
    writeln!(report)
}

fn write_conclusion_markdown(
    report: &mut String,
    completed: &CompletedExperiment,
) -> Result<(), std::fmt::Error> {
    writeln!(report, "## Conclusion")?;
    writeln!(report)?;
    if completed.any_promising {
        let identifiers = completed
            .outcomes
            .iter()
            .filter(|outcome| {
                outcome
                    .promising
                    .as_ref()
                    .is_some_and(|decision| decision.passed)
            })
            .map(|outcome| format!("`{}`", outcome.identifier))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(report, "Promising under the frozen rule: {identifiers}.")?;
    } else {
        writeln!(report, "No candidate passed.")?;
    }
    Ok(())
}

fn decimal_infallible(value: &str) -> Decimal {
    Decimal::from_str(value).expect("hard-coded frozen decimal is valid")
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn optional_decimal_string(value: Option<Decimal>) -> String {
    value.map_or_else(|| "N/A".to_owned(), decimal_string)
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonContainerKind {
    Object,
    Array,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JsonFrame {
    kind: JsonContainerKind,
    first: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct JsonWriter {
    output: String,
    frames: Vec<JsonFrame>,
}

impl JsonWriter {
    const fn new() -> Self {
        Self {
            output: String::new(),
            frames: Vec::new(),
        }
    }

    fn start_object(&mut self, key: Option<&str>) -> Result<(), io::Error> {
        self.begin_value(key)?;
        self.output.push('{');
        self.frames.push(JsonFrame {
            kind: JsonContainerKind::Object,
            first: true,
        });
        Ok(())
    }

    fn end_object(&mut self) -> Result<(), io::Error> {
        self.end_container(JsonContainerKind::Object, '}')
    }

    fn start_array(&mut self, key: Option<&str>) -> Result<(), io::Error> {
        self.begin_value(key)?;
        self.output.push('[');
        self.frames.push(JsonFrame {
            kind: JsonContainerKind::Array,
            first: true,
        });
        Ok(())
    }

    fn end_array(&mut self) -> Result<(), io::Error> {
        self.end_container(JsonContainerKind::Array, ']')
    }

    fn string(&mut self, key: Option<&str>, value: &str) -> Result<(), io::Error> {
        self.begin_value(key)?;
        push_json_string(&mut self.output, value);
        Ok(())
    }

    fn optional_string(&mut self, key: Option<&str>, value: Option<&str>) -> Result<(), io::Error> {
        match value {
            Some(value) => self.string(key, value),
            None => self.null(key),
        }
    }

    fn decimal(&mut self, key: Option<&str>, value: Decimal) -> Result<(), io::Error> {
        self.string(key, &decimal_string(value))
    }

    fn optional_decimal(
        &mut self,
        key: Option<&str>,
        value: Option<Decimal>,
    ) -> Result<(), io::Error> {
        match value {
            Some(value) => self.decimal(key, value),
            None => self.null(key),
        }
    }

    fn number(&mut self, key: Option<&str>, value: usize) -> Result<(), io::Error> {
        self.begin_value(key)?;
        self.output.push_str(&value.to_string());
        Ok(())
    }

    fn signed_number(&mut self, key: Option<&str>, value: i64) -> Result<(), io::Error> {
        self.begin_value(key)?;
        self.output.push_str(&value.to_string());
        Ok(())
    }

    fn boolean(&mut self, key: Option<&str>, value: bool) -> Result<(), io::Error> {
        self.begin_value(key)?;
        self.output.push_str(if value { "true" } else { "false" });
        Ok(())
    }

    fn null(&mut self, key: Option<&str>) -> Result<(), io::Error> {
        self.begin_value(key)?;
        self.output.push_str("null");
        Ok(())
    }

    fn begin_value(&mut self, key: Option<&str>) -> Result<(), io::Error> {
        let Some(frame) = self.frames.last_mut() else {
            if key.is_some() || !self.output.is_empty() {
                return Err(invalid_data("invalid JSON root value"));
            }
            return Ok(());
        };
        let kind = frame.kind;
        let add_comma = !frame.first;
        frame.first = false;

        match (kind, key) {
            (JsonContainerKind::Object, None) => {
                return Err(invalid_data("JSON object value is missing its key"));
            }
            (JsonContainerKind::Array, Some(_)) => {
                return Err(invalid_data("JSON array value unexpectedly has a key"));
            }
            _ => {}
        }
        if add_comma {
            self.output.push(',');
        }
        self.output.push('\n');
        push_indent(&mut self.output, self.frames.len());
        if let Some(key) = key {
            push_json_string(&mut self.output, key);
            self.output.push_str(": ");
        }
        Ok(())
    }

    fn end_container(
        &mut self,
        expected: JsonContainerKind,
        delimiter: char,
    ) -> Result<(), io::Error> {
        let frame = self
            .frames
            .pop()
            .ok_or_else(|| invalid_data("JSON container stack underflow"))?;
        if frame.kind != expected {
            return Err(invalid_data("JSON container kind mismatch"));
        }
        if !frame.first {
            self.output.push('\n');
            push_indent(&mut self.output, self.frames.len());
        }
        self.output.push(delimiter);
        Ok(())
    }

    fn finish(mut self) -> Result<String, io::Error> {
        if !self.frames.is_empty() || self.output.is_empty() {
            return Err(invalid_data("JSON document is incomplete"));
        }
        self.output.push('\n');
        Ok(self.output)
    }
}

fn push_indent(output: &mut String, depth: usize) {
    for _ in 0..depth {
        output.push_str("  ");
    }
}

fn push_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                write!(output, "\\u{:04x}", u32::from(character))
                    .expect("writing to String cannot fail");
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn selection_persistence_error(error: &io::Error) -> ExperimentError {
    ExperimentError::SelectionArtifactPersistenceFailed(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn quoted_tsv_parser_is_strict_and_handles_escaped_quotes() {
        assert_eq!(
            parse_tsv_line("\"alpha\"\t\"b\"\"eta\"").unwrap(),
            vec!["alpha".to_owned(), "b\"eta".to_owned()]
        );
        assert!(parse_tsv_line("alpha\tbeta").is_err());
        assert!(parse_tsv_line("\"alpha\" trailing").is_err());
    }

    #[test]
    fn frozen_registry_has_exact_ids_and_budget() {
        let registry = registered_configurations(g005_protocol()).unwrap();
        assert_eq!(registry.len(), 22);
        assert_eq!(registry[0].identifier(), "cash");
        assert_eq!(registry[1].identifier(), "buy-and-hold");
        assert_eq!(registry[21].identifier(), "vol-lb060-t20-b20-rb007");
        assert_eq!(
            registry
                .iter()
                .filter(|configuration| configuration.family() == "capped_volatility_target")
                .count(),
            12
        );
    }

    #[test]
    fn json_writer_is_deterministic_and_escapes_strings() {
        let mut first = JsonWriter::new();
        first.start_object(None).unwrap();
        first.string(Some("message"), "line\n\"quoted\"").unwrap();
        first.start_array(Some("values")).unwrap();
        first.number(None, 7).unwrap();
        first.null(None).unwrap();
        first.end_array().unwrap();
        first.end_object().unwrap();
        let first = first.finish().unwrap();

        let mut second = JsonWriter::new();
        second.start_object(None).unwrap();
        second.string(Some("message"), "line\n\"quoted\"").unwrap();
        second.start_array(Some("values")).unwrap();
        second.number(None, 7).unwrap();
        second.null(None).unwrap();
        second.end_array().unwrap();
        second.end_object().unwrap();
        assert_eq!(first, second.finish().unwrap());
        assert!(first.contains("line\\n\\\"quoted\\\""));
    }

    #[test]
    fn frozen_month_geometry_covers_exact_source_range() {
        let protocol = g005_protocol();
        let first = expected_month(0, protocol).unwrap();
        let last = expected_month(protocol.expected_archive_count - 1, protocol).unwrap();
        assert_eq!((first.year, first.month, first.bar_count), (2018, 1, 31));
        assert_eq!((last.year, last.month, last.bar_count), (2026, 7, 31));
        assert_eq!(first.timestamp_unit, TimestampUnit::Milliseconds);
        assert_eq!(last.timestamp_unit, TimestampUnit::Microseconds);
    }

    #[test]
    fn embedded_g005_calendar_matches_the_real_frozen_lock_first_row() {
        let lock = fs::read_to_string(g005_lock_path()).unwrap();
        let first_row = parse_provenance_lock(&lock).unwrap().remove(0);
        let protocol = g005_protocol();
        let calendar = expected_month(0, protocol).unwrap();
        let retrieved_at = validate_provenance_row(
            &first_row,
            &calendar,
            protocol,
            Some(parse_datetime(G005_FROZEN_RETRIEVED_AT).unwrap()),
        )
        .unwrap();

        assert_eq!(
            retrieved_at,
            parse_datetime(G005_FROZEN_RETRIEVED_AT).unwrap()
        );
        assert_eq!(
            calendar
                .last_close
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            "2018-01-31T23:59:59.999Z"
        );
    }

    #[test]
    fn write_synced_replaces_existing_artifacts_without_leaving_temp_files() {
        let temp_dir = unique_test_directory("atomic-write");
        fs::create_dir_all(&temp_dir).unwrap();
        let artifact = temp_dir.join("artifact.json");
        fs::write(&artifact, b"old").unwrap();

        write_synced(&artifact, b"new").unwrap();

        assert_eq!(fs::read(&artifact).unwrap(), b"new");
        let leftover_temps = fs::read_dir(&temp_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .count();
        assert_eq!(leftover_temps, 0);

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[test]
    fn product_cli_requires_explicit_protocol_and_cadence() {
        let missing_protocol = RunnerArguments::parse_arguments(
            [
                "--cadence".to_owned(),
                "1d".to_owned(),
                "--cache".to_owned(),
                "cache".to_owned(),
                "--provenance-lock".to_owned(),
                "lock.tsv".to_owned(),
                "--output-dir".to_owned(),
                "out".to_owned(),
            ]
            .into_iter(),
            ParseMode::Product,
            "usage",
        );
        assert!(missing_protocol.is_err());

        let missing_cadence = RunnerArguments::parse_arguments(
            [
                "--protocol".to_owned(),
                G005_PROTOCOL_ID.to_owned(),
                "--cache".to_owned(),
                "cache".to_owned(),
                "--provenance-lock".to_owned(),
                "lock.tsv".to_owned(),
                "--output-dir".to_owned(),
                "out".to_owned(),
            ]
            .into_iter(),
            ParseMode::Product,
            "usage",
        );
        assert!(missing_cadence.is_err());
    }

    #[test]
    fn protocol_table_rejects_terminally_aborted_hourly_w1_pair() {
        let arguments = RunnerArguments {
            protocol: W1_PROTOCOL_ID.to_owned(),
            cadence: ResearchCadence::OneHour,
            cache: PathBuf::from("cache"),
            provenance_lock: PathBuf::from("lock.tsv"),
            output_dir: PathBuf::from("out"),
        };
        let error = validate_supported_prereg(&arguments).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error
                .to_string()
                .contains("terminally data-admission-aborted")
        );
    }

    #[test]
    fn hourly_w1_run_aborts_before_any_dataset_io() {
        let arguments = RunnerArguments {
            protocol: W1_PROTOCOL_ID.to_owned(),
            cadence: ResearchCadence::OneHour,
            cache: PathBuf::from("does-not-need-to-exist"),
            provenance_lock: PathBuf::from("does-not-need-to-exist.tsv"),
            output_dir: PathBuf::from("does-not-need-to-exist-out"),
        };
        let error = run(&arguments).unwrap_err();
        let error = error.downcast::<io::Error>().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error
                .to_string()
                .contains("terminally data-admission-aborted")
        );
    }

    #[test]
    fn hourly_registry_uses_24x_geometry_and_8760_annualization() {
        let registry = registered_configurations(w1_protocol()).unwrap();
        assert_eq!(registry.len(), 22);
        assert_eq!(registry[2].identifier(), "tsm-lb672-rb168");
        assert_eq!(registry[10].identifier(), "vol-lb480-t10-b00-rb168");
        assert_eq!(
            registry[10].strategy(),
            SpotStrategyConfig::CappedVolatilityTargetExplicitAnnualization {
                lookback_returns: 480,
                annual_target: Decimal::new(10, 2),
                rebalance_band: Decimal::ZERO,
                rebalance_every_bars: 168,
                periods_per_year: Decimal::from(8_760_u32),
            }
        );
    }

    #[test]
    fn product_cli_rejects_caller_supplied_provenance_lock_hash() {
        let error = RunnerArguments::parse_arguments(
            [
                "--protocol".to_owned(),
                W1_PROTOCOL_ID.to_owned(),
                "--cadence".to_owned(),
                "1h".to_owned(),
                "--cache".to_owned(),
                "cache".to_owned(),
                "--provenance-lock".to_owned(),
                "lock.tsv".to_owned(),
                "--provenance-lock-sha256".to_owned(),
                "00".repeat(32),
                "--output-dir".to_owned(),
                "out".to_owned(),
            ]
            .into_iter(),
            ParseMode::Product,
            "usage",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unknown argument `--provenance-lock-sha256`")
        );
    }

    fn g005_lock_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../artifacts/strategy-evaluation/g005-btcusdt-provenance.tsv")
    }

    fn unique_test_directory(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!(
            "crypto-trading-backtest-{label}-{}-{nonce}",
            process::id()
        ))
    }
}
