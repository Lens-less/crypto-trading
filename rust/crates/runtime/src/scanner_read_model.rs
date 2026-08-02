use std::{cmp::Ordering, collections::HashSet, fmt};

use chrono::{DateTime, Utc};
use crypto_trading_domain::MarketType;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    JournalPageBoundary, JournalSnapshot, LegacyJsonlJournalReader, OperationEventEnvelope,
    ProjectionStatus, ReadModelError,
};

/// Stable schema version for the bounded virtual-grid scanner projection.
pub const VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION: u16 = 1;
/// Maximum rows retained from one complete ranking fact.
pub const MAX_VIRTUAL_GRID_SCANNER_ROWS: usize = 128;

const SCANNER_STRATEGY: &str = "virtual_grid_scanner";
const SCANNER_SYMBOL: &str = "control-plane";
const SCANNER_DECISION: &str = "scanner_ranked";
const RANKING_POLICY: &str = "explicit_benchmark_then_apr_desc";
const MAX_SCANNER_CANDIDATES: usize = 128;
const MAX_SCANNER_OBSERVATIONS_PER_CANDIDATE: usize = 8_192;
const MAX_SCANNER_WINDOW_SECONDS: u64 = 366 * 24 * 60 * 60;
const MAX_SCANNER_TEXT_BYTES: usize = 128;
const MAX_DECIMAL_TEXT_BYTES: usize = 64;
const MAX_GRID_COUNT: u64 = 10_000;

const DETAILS_FIELDS: &[&str] = &[
    "schema_version",
    "run_id",
    "ranking_policy",
    "apr_window_seconds",
    "min_complete_cycles",
    "row_limit",
    "candidate_count",
    "eligible_count",
    "filtered_by_cycles_count",
    "truncated",
    "rows",
];
const ROW_FIELDS: &[&str] = &[
    "rank",
    "activity",
    "priority",
    "instrument",
    "started_at",
    "last_observed_at",
    "observation_count",
    "last_observation_sequence",
    "current_price",
    "lower_price",
    "upper_price",
    "pending_buy_price",
    "pending_sell_price",
    "grid_width_percent",
    "grid_interval_percent",
    "grid_count",
    "running_seconds",
    "buy_crosses",
    "sell_crosses",
    "total_crosses",
    "complete_cycles",
    "recent_five_minute_cycles",
    "cycles_per_hour",
    "estimated_apr",
    "volume_24h_usdc",
    "price_change_24h_percent",
    "rating_grade",
    "rating_score",
];
const INSTRUMENT_FIELDS: &[&str] = &["exchange", "symbol", "market_type"];

/// Latest complete, strictly validated scanner ranking.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualGridScannerReadModel {
    pub schema_version: u16,
    pub journal_id: Uuid,
    pub journal_head_sequence: Option<u64>,
    pub projection_status: ProjectionStatus,
    pub latest: Option<VirtualGridScanView>,
    pub invalid_event_count: u64,
}

impl VirtualGridScannerReadModel {
    /// Projects scanner facts from one immutable legacy journal snapshot.
    ///
    /// Non-scanner records are ignored. A malformed scanner fact degrades this
    /// projection while retaining the last valid ranking. A physical partial
    /// tail also degrades the projection without inventing a scanner event.
    ///
    /// # Errors
    ///
    /// Returns [`ReadModelError`] for journal failures or a non-advancing page.
    pub fn from_legacy_snapshot(snapshot: &JournalSnapshot) -> Result<Self, ReadModelError> {
        ScannerProjectionBuilder::new(snapshot.journal_id()).project(snapshot)
    }
}

/// Explicit priority retained from the durable ranking policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerPriorityView {
    Benchmark,
    Standard,
}

impl ScannerPriorityView {
    const fn rank(self) -> u8 {
        match self {
            Self::Benchmark => 0,
            Self::Standard => 1,
        }
    }
}

/// Safe scanner rating grade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScannerRatingGradeView {
    S,
    A,
    B,
    C,
    D,
}

impl fmt::Display for ScannerRatingGradeView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::S => "s",
            Self::A => "a",
            Self::B => "b",
            Self::C => "c",
            Self::D => "d",
        })
    }
}

/// Exact market identity copied from a validated scanner row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScannerInstrumentView {
    pub exchange: String,
    pub symbol: String,
    pub market_type: MarketType,
}

/// One bounded active scanner result. Decimal values remain canonical strings
/// so the read surface never introduces JSON floating-point loss.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualGridScanRowView {
    pub rank: usize,
    pub priority: ScannerPriorityView,
    pub instrument: ScannerInstrumentView,
    pub started_at: DateTime<Utc>,
    pub last_observed_at: DateTime<Utc>,
    pub observation_count: usize,
    pub last_observation_sequence: u64,
    pub current_price: String,
    pub lower_price: String,
    pub upper_price: String,
    pub pending_buy_price: String,
    pub pending_sell_price: String,
    pub grid_width_percent: String,
    pub grid_interval_percent: String,
    pub grid_count: u32,
    pub running_seconds: i64,
    pub buy_crosses: u64,
    pub sell_crosses: u64,
    pub total_crosses: u64,
    pub complete_cycles: u64,
    pub recent_five_minute_cycles: usize,
    pub cycles_per_hour: String,
    pub estimated_apr: String,
    pub volume_24h_usdc: String,
    pub price_change_24h_percent: Option<String>,
    pub rating_grade: ScannerRatingGradeView,
    pub rating_score: String,
}

impl VirtualGridScanRowView {
    pub const fn is_benchmark(&self) -> bool {
        matches!(self.priority, ScannerPriorityView::Benchmark)
    }
}

/// Last valid ranking with its durable journal identity and bounded counts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualGridScanView {
    pub source_sequence: u64,
    pub event_id: Uuid,
    pub recorded_at: DateTime<Utc>,
    pub run_id: String,
    pub ranking_policy: String,
    pub apr_window_seconds: u32,
    pub min_complete_cycles: u64,
    pub row_limit: usize,
    pub candidate_count: usize,
    pub eligible_count: usize,
    pub filtered_by_cycles_count: usize,
    pub truncated: bool,
    pub rows: Vec<VirtualGridScanRowView>,
}

pub(crate) struct ScannerProjectionBuilder {
    journal_id: Uuid,
    journal_head_sequence: Option<u64>,
    projection_status: ProjectionStatus,
    latest: Option<VirtualGridScanView>,
    invalid_event_count: u64,
}

impl ScannerProjectionBuilder {
    pub(crate) const fn new(journal_id: Uuid) -> Self {
        Self {
            journal_id,
            journal_head_sequence: None,
            projection_status: ProjectionStatus::Complete,
            latest: None,
            invalid_event_count: 0,
        }
    }

    fn project(
        mut self,
        snapshot: &JournalSnapshot,
    ) -> Result<VirtualGridScannerReadModel, ReadModelError> {
        let mut cursor = None;
        loop {
            let page = LegacyJsonlJournalReader::read_page(snapshot, cursor.as_ref())?;
            for event in page.events() {
                self.observe_event(event);
            }
            match page.boundary() {
                JournalPageBoundary::SnapshotEnd => break,
                JournalPageBoundary::PartialTail { .. } => {
                    self.mark_partial_tail();
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
        Ok(self.finish())
    }

    pub(crate) fn observe_event(&mut self, event: &OperationEventEnvelope) {
        self.journal_head_sequence = Some(event.sequence());
        self.apply_event(event);
    }

    pub(crate) fn mark_partial_tail(&mut self) {
        self.projection_status = ProjectionStatus::Degraded;
    }

    pub(crate) fn finish(self) -> VirtualGridScannerReadModel {
        VirtualGridScannerReadModel {
            schema_version: VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION,
            journal_id: self.journal_id,
            journal_head_sequence: self.journal_head_sequence,
            projection_status: self.projection_status,
            latest: self.latest,
            invalid_event_count: self.invalid_event_count,
        }
    }

    fn apply_event(&mut self, event: &OperationEventEnvelope) {
        match parse_scanner_event(event) {
            Ok(Some(view)) => self.latest = Some(view),
            Ok(None) => {}
            Err(()) => {
                self.projection_status = ProjectionStatus::Degraded;
                self.invalid_event_count = self.invalid_event_count.saturating_add(1);
            }
        }
    }
}

fn parse_scanner_event(event: &OperationEventEnvelope) -> Result<Option<VirtualGridScanView>, ()> {
    let payload = object(event.payload())?;
    let strategy = required_text(payload, "strategy")?;
    if strategy != SCANNER_STRATEGY {
        return Ok(None);
    }
    exact_fields(payload, &["strategy", "symbol", "decision", "details"])?;
    if required_text(payload, "symbol")? != SCANNER_SYMBOL
        || required_text(payload, "decision")? != SCANNER_DECISION
    {
        return Err(());
    }
    let details = object(required(payload, "details")?)?;
    let header = parse_scan_header(details)?;
    let rows = required(details, "rows")?.as_array().ok_or(())?;
    if rows.len() > MAX_VIRTUAL_GRID_SCANNER_ROWS
        || rows.len() != header.eligible_count.min(header.row_limit)
    {
        return Err(());
    }

    let mut projected_rows = Vec::new();
    projected_rows
        .try_reserve_exact(rows.len())
        .map_err(|_| ())?;
    let mut identities = HashSet::with_capacity(rows.len());
    for (index, value) in rows.iter().enumerate() {
        let row = parse_row(
            value,
            index.saturating_add(1),
            event.recorded_at(),
            header.min_complete_cycles,
        )?;
        let identity = (
            row.instrument.exchange.clone(),
            row.instrument.symbol.clone(),
            market_type_rank(row.instrument.market_type),
        );
        if !identities.insert(identity) {
            return Err(());
        }
        if let Some(previous) = projected_rows.last()
            && compare_rows(previous, &row)? == Ordering::Greater
        {
            return Err(());
        }
        projected_rows.push(row);
    }

    Ok(Some(VirtualGridScanView {
        source_sequence: event.sequence(),
        event_id: event.event_id(),
        recorded_at: event.recorded_at(),
        run_id: header.run_id,
        ranking_policy: RANKING_POLICY.to_owned(),
        apr_window_seconds: header.apr_window_seconds,
        min_complete_cycles: header.min_complete_cycles,
        row_limit: header.row_limit,
        candidate_count: header.candidate_count,
        eligible_count: header.eligible_count,
        filtered_by_cycles_count: header.filtered_by_cycles_count,
        truncated: header.truncated,
        rows: projected_rows,
    }))
}

struct ParsedScanHeader {
    run_id: String,
    apr_window_seconds: u32,
    min_complete_cycles: u64,
    row_limit: usize,
    candidate_count: usize,
    eligible_count: usize,
    filtered_by_cycles_count: usize,
    truncated: bool,
}

fn parse_scan_header(details: &Map<String, Value>) -> Result<ParsedScanHeader, ()> {
    exact_fields(details, DETAILS_FIELDS)?;
    if required_u64(details, "schema_version")?
        != u64::from(VIRTUAL_GRID_SCANNER_READ_MODEL_SCHEMA_VERSION)
    {
        return Err(());
    }
    let run_id = required_text(details, "run_id")?;
    if !valid_identifier(&run_id) || required_text(details, "ranking_policy")? != RANKING_POLICY {
        return Err(());
    }
    let apr_window_seconds = required_u64(details, "apr_window_seconds")?;
    if apr_window_seconds == 0 || apr_window_seconds > MAX_SCANNER_WINDOW_SECONDS {
        return Err(());
    }
    let row_limit = bounded_usize(
        required_u64(details, "row_limit")?,
        1,
        MAX_VIRTUAL_GRID_SCANNER_ROWS,
    )?;
    let candidate_count = bounded_usize(
        required_u64(details, "candidate_count")?,
        1,
        MAX_SCANNER_CANDIDATES,
    )?;
    let eligible_count =
        usize::try_from(required_u64(details, "eligible_count")?).map_err(|_| ())?;
    let filtered_by_cycles_count =
        usize::try_from(required_u64(details, "filtered_by_cycles_count")?).map_err(|_| ())?;
    if eligible_count > candidate_count
        || filtered_by_cycles_count != candidate_count.saturating_sub(eligible_count)
    {
        return Err(());
    }
    let truncated = required_bool(details, "truncated")?;
    if truncated != (eligible_count > row_limit) {
        return Err(());
    }
    Ok(ParsedScanHeader {
        run_id,
        apr_window_seconds: u32::try_from(apr_window_seconds).map_err(|_| ())?,
        min_complete_cycles: required_u64(details, "min_complete_cycles")?,
        row_limit,
        candidate_count,
        eligible_count,
        filtered_by_cycles_count,
        truncated,
    })
}

fn parse_row(
    value: &Value,
    expected_rank: usize,
    recorded_at: DateTime<Utc>,
    min_complete_cycles: u64,
) -> Result<VirtualGridScanRowView, ()> {
    let row = object(value)?;
    exact_fields(row, ROW_FIELDS)?;
    if required_text(row, "activity")? != "active" {
        return Err(());
    }
    let rank = usize::try_from(required_u64(row, "rank")?).map_err(|_| ())?;
    if rank != expected_rank {
        return Err(());
    }
    let priority = match required_text(row, "priority")?.as_str() {
        "benchmark" => ScannerPriorityView::Benchmark,
        "standard" => ScannerPriorityView::Standard,
        _ => return Err(()),
    };
    let instrument = parse_instrument(required(row, "instrument")?)?;
    let timing = parse_row_timing(row, recorded_at)?;

    let current_price = positive_decimal(row, "current_price")?;
    let lower_price = positive_decimal(row, "lower_price")?;
    let upper_price = positive_decimal(row, "upper_price")?;
    let pending_buy_price = positive_decimal(row, "pending_buy_price")?;
    let pending_sell_price = positive_decimal(row, "pending_sell_price")?;
    if lower_price.number()?.cmp(&upper_price.number()?) != Ordering::Less {
        return Err(());
    }
    let grid_width_percent = positive_decimal(row, "grid_width_percent")?;
    let grid_interval_percent = positive_decimal(row, "grid_interval_percent")?;
    let grid_count = required_u64(row, "grid_count")?;
    if grid_count == 0 || grid_count > MAX_GRID_COUNT {
        return Err(());
    }
    let grid_count = u32::try_from(grid_count).map_err(|_| ())?;

    let buy_crosses = required_u64(row, "buy_crosses")?;
    let sell_crosses = required_u64(row, "sell_crosses")?;
    let total_crosses = required_u64(row, "total_crosses")?;
    if buy_crosses.checked_add(sell_crosses) != Some(total_crosses) {
        return Err(());
    }
    let complete_cycles = required_u64(row, "complete_cycles")?;
    if complete_cycles != buy_crosses.min(sell_crosses)
        || (priority == ScannerPriorityView::Standard && complete_cycles < min_complete_cycles)
    {
        return Err(());
    }
    let recent_five_minute_cycles =
        usize::try_from(required_u64(row, "recent_five_minute_cycles")?).map_err(|_| ())?;
    if u64::try_from(recent_five_minute_cycles).unwrap_or(u64::MAX) > complete_cycles {
        return Err(());
    }

    let cycles_per_hour = nonnegative_decimal(row, "cycles_per_hour")?;
    let estimated_apr = nonnegative_decimal(row, "estimated_apr")?;
    let volume_24h_usdc = nonnegative_decimal(row, "volume_24h_usdc")?;
    let price_change_24h_percent = optional_decimal(row, "price_change_24h_percent")?;
    let rating_grade = parse_grade(&required_text(row, "rating_grade")?)?;
    let rating_score = nonnegative_decimal(row, "rating_score")?;
    validate_rating(
        &estimated_apr.number()?,
        &cycles_per_hour.number()?,
        &volume_24h_usdc.number()?,
        rating_grade,
        &rating_score.number()?,
    )?;

    Ok(VirtualGridScanRowView {
        rank,
        priority,
        instrument,
        started_at: timing.started_at,
        last_observed_at: timing.last_observed_at,
        observation_count: timing.observation_count,
        last_observation_sequence: timing.last_observation_sequence,
        current_price: current_price.text,
        lower_price: lower_price.text,
        upper_price: upper_price.text,
        pending_buy_price: pending_buy_price.text,
        pending_sell_price: pending_sell_price.text,
        grid_width_percent: grid_width_percent.text,
        grid_interval_percent: grid_interval_percent.text,
        grid_count,
        running_seconds: timing.running_seconds,
        buy_crosses,
        sell_crosses,
        total_crosses,
        complete_cycles,
        recent_five_minute_cycles,
        cycles_per_hour: cycles_per_hour.text,
        estimated_apr: estimated_apr.text,
        volume_24h_usdc: volume_24h_usdc.text,
        price_change_24h_percent,
        rating_grade,
        rating_score: rating_score.text,
    })
}

struct ParsedRowTiming {
    started_at: DateTime<Utc>,
    last_observed_at: DateTime<Utc>,
    observation_count: usize,
    last_observation_sequence: u64,
    running_seconds: i64,
}

fn parse_row_timing(
    row: &Map<String, Value>,
    recorded_at: DateTime<Utc>,
) -> Result<ParsedRowTiming, ()> {
    let started_at = required_timestamp(row, "started_at")?;
    let last_observed_at = required_timestamp(row, "last_observed_at")?;
    if started_at > last_observed_at || last_observed_at > recorded_at {
        return Err(());
    }
    let observation_count = bounded_usize(
        required_u64(row, "observation_count")?,
        1,
        MAX_SCANNER_OBSERVATIONS_PER_CANDIDATE,
    )?;
    let last_observation_sequence = required_u64(row, "last_observation_sequence")?;
    if usize::try_from(last_observation_sequence).map_err(|_| ())? != observation_count {
        return Err(());
    }
    let running_seconds = required_i64(row, "running_seconds")?;
    if running_seconds < 0
        || running_seconds != recorded_at.signed_duration_since(started_at).num_seconds()
    {
        return Err(());
    }
    Ok(ParsedRowTiming {
        started_at,
        last_observed_at,
        observation_count,
        last_observation_sequence,
        running_seconds,
    })
}

fn parse_instrument(value: &Value) -> Result<ScannerInstrumentView, ()> {
    let instrument = object(value)?;
    exact_fields(instrument, INSTRUMENT_FIELDS)?;
    let exchange = required_text(instrument, "exchange")?;
    let symbol = required_text(instrument, "symbol")?;
    if !valid_market_identity_text(&exchange) || !valid_market_identity_text(&symbol) {
        return Err(());
    }
    let market_type = match required_text(instrument, "market_type")?.as_str() {
        "spot" => MarketType::Spot,
        "perpetual" => MarketType::Perpetual,
        _ => return Err(()),
    };
    Ok(ScannerInstrumentView {
        exchange,
        symbol,
        market_type,
    })
}

fn compare_rows(
    left: &VirtualGridScanRowView,
    right: &VirtualGridScanRowView,
) -> Result<Ordering, ()> {
    let left_apr = parse_decimal_text(&left.estimated_apr)?;
    let right_apr = parse_decimal_text(&right.estimated_apr)?;
    Ok(left
        .priority
        .rank()
        .cmp(&right.priority.rank())
        .then_with(|| right_apr.cmp(&left_apr))
        .then_with(|| left.instrument.exchange.cmp(&right.instrument.exchange))
        .then_with(|| left.instrument.symbol.cmp(&right.instrument.symbol))
        .then_with(|| {
            market_type_rank(left.instrument.market_type)
                .cmp(&market_type_rank(right.instrument.market_type))
        }))
}

fn validate_rating(
    apr: &CanonicalDecimal<'_>,
    cycles_per_hour: &CanonicalDecimal<'_>,
    volume: &CanonicalDecimal<'_>,
    observed_grade: ScannerRatingGradeView,
    observed_score: &CanonicalDecimal<'_>,
) -> Result<(), ()> {
    let (expected_grade, mut score) = if apr.cmp_nonnegative_integer("500") != Ordering::Less {
        (ScannerRatingGradeView::S, 95_i16)
    } else if apr.cmp_nonnegative_integer("300") != Ordering::Less {
        (ScannerRatingGradeView::A, 85)
    } else if apr.cmp_nonnegative_integer("150") != Ordering::Less {
        (ScannerRatingGradeView::B, 75)
    } else if apr.cmp_nonnegative_integer("50") != Ordering::Less {
        (ScannerRatingGradeView::C, 60)
    } else {
        (ScannerRatingGradeView::D, 40)
    };
    if cycles_per_hour.cmp_nonnegative_integer("50") == Ordering::Greater {
        score += 5;
    } else if cycles_per_hour.cmp_nonnegative_integer("5") == Ordering::Less {
        score -= 10;
    }
    if volume.cmp_nonnegative_integer("10000000") != Ordering::Less {
        score += 5;
    } else if volume.cmp_nonnegative_integer("500000") == Ordering::Less {
        score -= 10;
    }
    score = score.clamp(0, 100);
    if observed_grade != expected_grade
        || !observed_score.equals_nonnegative_integer(&score.to_string())
    {
        return Err(());
    }
    Ok(())
}

fn parse_grade(value: &str) -> Result<ScannerRatingGradeView, ()> {
    match value {
        "s" => Ok(ScannerRatingGradeView::S),
        "a" => Ok(ScannerRatingGradeView::A),
        "b" => Ok(ScannerRatingGradeView::B),
        "c" => Ok(ScannerRatingGradeView::C),
        "d" => Ok(ScannerRatingGradeView::D),
        _ => Err(()),
    }
}

struct ParsedDecimal {
    text: String,
}

impl ParsedDecimal {
    fn number(&self) -> Result<CanonicalDecimal<'_>, ()> {
        parse_decimal_text(&self.text)
    }
}

fn positive_decimal(row: &Map<String, Value>, field: &str) -> Result<ParsedDecimal, ()> {
    let text = required_text(row, field)?;
    let number = parse_decimal_text(&text)?;
    if !number.is_positive() {
        return Err(());
    }
    Ok(ParsedDecimal { text })
}

fn nonnegative_decimal(row: &Map<String, Value>, field: &str) -> Result<ParsedDecimal, ()> {
    let text = required_text(row, field)?;
    let number = parse_decimal_text(&text)?;
    if number.negative {
        return Err(());
    }
    Ok(ParsedDecimal { text })
}

fn optional_decimal(row: &Map<String, Value>, field: &str) -> Result<Option<String>, ()> {
    let value = required(row, field)?;
    if value.is_null() {
        return Ok(None);
    }
    let text = value.as_str().ok_or(())?.to_owned();
    parse_decimal_text(&text)?;
    Ok(Some(text))
}

#[derive(Clone, Copy)]
struct CanonicalDecimal<'a> {
    negative: bool,
    integer: &'a str,
    fraction: &'a str,
}

impl CanonicalDecimal<'_> {
    fn is_positive(self) -> bool {
        !(self.negative || self.integer == "0" && self.fraction.is_empty())
    }

    fn cmp_nonnegative_integer(self, value: &str) -> Ordering {
        debug_assert!(!self.negative);
        compare_magnitude(self.integer, self.fraction, value, "")
    }

    fn equals_nonnegative_integer(self, value: &str) -> bool {
        !self.negative && self.integer == value && self.fraction.is_empty()
    }
}

impl Ord for CanonicalDecimal<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => {
                compare_magnitude(self.integer, self.fraction, other.integer, other.fraction)
            }
            (true, true) => {
                compare_magnitude(other.integer, other.fraction, self.integer, self.fraction)
            }
        }
    }
}

impl PartialOrd for CanonicalDecimal<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CanonicalDecimal<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for CanonicalDecimal<'_> {}

fn parse_decimal_text(value: &str) -> Result<CanonicalDecimal<'_>, ()> {
    if value.is_empty() || value.len() > MAX_DECIMAL_TEXT_BYTES || !value.is_ascii() {
        return Err(());
    }
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    if unsigned.is_empty() || unsigned.starts_with('+') {
        return Err(());
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next().ok_or(())?;
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || (unsigned.contains('.') && fraction.is_empty())
        || (integer.len() > 1 && integer.starts_with('0'))
        || fraction.ends_with('0')
    {
        return Err(());
    }
    let zero = integer == "0" && fraction.bytes().all(|byte| byte == b'0');
    if negative && zero {
        return Err(());
    }
    Ok(CanonicalDecimal {
        negative,
        integer,
        fraction,
    })
}

fn compare_magnitude(
    left_integer: &str,
    left_fraction: &str,
    right_integer: &str,
    right_fraction: &str,
) -> Ordering {
    left_integer
        .len()
        .cmp(&right_integer.len())
        .then_with(|| left_integer.cmp(right_integer))
        .then_with(|| {
            let width = left_fraction.len().max(right_fraction.len());
            for index in 0..width {
                let left = left_fraction.as_bytes().get(index).copied().unwrap_or(b'0');
                let right = right_fraction
                    .as_bytes()
                    .get(index)
                    .copied()
                    .unwrap_or(b'0');
                match left.cmp(&right) {
                    Ordering::Equal => {}
                    ordering => return ordering,
                }
            }
            Ordering::Equal
        })
}

fn object(value: &Value) -> Result<&Map<String, Value>, ()> {
    value.as_object().ok_or(())
}

fn exact_fields(object: &Map<String, Value>, expected: &[&str]) -> Result<(), ()> {
    if object.len() != expected.len()
        || object
            .keys()
            .any(|field| !expected.contains(&field.as_str()))
    {
        return Err(());
    }
    Ok(())
}

fn required<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a Value, ()> {
    object.get(field).ok_or(())
}

fn required_text(object: &Map<String, Value>, field: &str) -> Result<String, ()> {
    let value = required(object, field)?.as_str().ok_or(())?;
    if value.len() > MAX_SCANNER_TEXT_BYTES {
        return Err(());
    }
    Ok(value.to_owned())
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, ()> {
    required(object, field)?.as_u64().ok_or(())
}

fn required_i64(object: &Map<String, Value>, field: &str) -> Result<i64, ()> {
    required(object, field)?.as_i64().ok_or(())
}

fn required_bool(object: &Map<String, Value>, field: &str) -> Result<bool, ()> {
    required(object, field)?.as_bool().ok_or(())
}

fn required_timestamp(object: &Map<String, Value>, field: &str) -> Result<DateTime<Utc>, ()> {
    let text = required_text(object, field)?;
    DateTime::parse_from_rfc3339(&text)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ())
}

fn bounded_usize(value: u64, min: usize, max: usize) -> Result<usize, ()> {
    let value = usize::try_from(value).map_err(|_| ())?;
    if !(min..=max).contains(&value) {
        return Err(());
    }
    Ok(value)
}

fn valid_identifier(value: &str) -> bool {
    valid_text(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_SCANNER_TEXT_BYTES
}

fn valid_market_identity_text(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SCANNER_TEXT_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
}

const fn market_type_rank(market_type: MarketType) -> u8 {
    match market_type {
        MarketType::Spot => 0,
        MarketType::Perpetual => 1,
    }
}
