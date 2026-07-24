//! Deterministic, offline virtual-grid ranking.
//!
//! This module owns one bounded replay-to-journal operation. It accepts
//! validated historical price observations, evaluates the existing pure
//! virtual-grid strategy, ranks the resulting read-only facts, and returns a
//! report only after the complete ranking has been synced to history. It has
//! no exchange handle, order intent, execution policy, or network adapter.

use std::{cmp::Ordering, fmt};

use chrono::{DateTime, Duration, Utc};
use crypto_trading_domain::Price;
use crypto_trading_runtime::{DecisionRecord, HistoryError, JsonlHistory, MarketInstrument};
use crypto_trading_strategy::{Rating, RatingGrade, StrategyError, VirtualGrid, VirtualGridConfig};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// Stable schema version for one durable scanner ranking fact.
pub const VIRTUAL_GRID_SCAN_RECORD_SCHEMA_VERSION: u16 = 1;
/// Maximum exact instruments evaluated by one bounded scan.
pub const MAX_VIRTUAL_GRID_SCAN_CANDIDATES: usize = 128;
/// Maximum price observations accepted for one instrument.
pub const MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS_PER_CANDIDATE: usize = 8_192;
/// Maximum price observations accepted across one scan.
pub const MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS: usize = 65_536;
/// Maximum ranked rows persisted by one scan.
pub const MAX_VIRTUAL_GRID_SCAN_ROWS: usize = 128;
/// Maximum configured APR window in seconds.
pub const MAX_VIRTUAL_GRID_SCAN_WINDOW_SECONDS: u32 = 366 * 24 * 60 * 60;

const MAX_SCAN_RUN_ID_BYTES: usize = 128;
const SCANNER_STRATEGY: &str = "virtual_grid_scanner";
const SCANNER_SYMBOL: &str = "control-plane";
const SCANNER_DECISION: &str = "scanner_ranked";
const RANKING_POLICY: &str = "explicit_benchmark_then_apr_desc";
const RECENT_WINDOW_SECONDS: i64 = 5 * 60;

/// Explicit display priority replacing the legacy symbol-substring heuristic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerPriority {
    Benchmark,
    Standard,
}

impl ScannerPriority {
    const fn rank(self) -> u8 {
        match self {
            Self::Benchmark => 0,
            Self::Standard => 1,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Benchmark => "benchmark",
            Self::Standard => "standard",
        }
    }
}

/// Whether one projected row was evaluated from observed prices.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerActivity {
    Active,
}

/// Stable, presentation-safe rating grade.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerRatingGrade {
    S,
    A,
    B,
    C,
    D,
}

impl fmt::Display for ScannerRatingGrade {
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

impl From<RatingGrade> for ScannerRatingGrade {
    fn from(value: RatingGrade) -> Self {
        match value {
            RatingGrade::S => Self::S,
            RatingGrade::A => Self::A,
            RatingGrade::B => Self::B,
            RatingGrade::C => Self::C,
            RatingGrade::D => Self::D,
        }
    }
}

/// One explicit, ordered historical price observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualGridScanObservation {
    sequence: u64,
    price: Price,
    observed_at: DateTime<Utc>,
}

impl VirtualGridScanObservation {
    /// Creates one nonzero source observation.
    ///
    /// # Errors
    ///
    /// Returns [`VirtualGridScannerError::InvalidObservationSequence`] when
    /// `sequence` is zero.
    pub const fn new(
        sequence: u64,
        price: Price,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, VirtualGridScannerError> {
        if sequence == 0 {
            return Err(VirtualGridScannerError::InvalidObservationSequence {
                expected: 1,
                observed: sequence,
            });
        }
        Ok(Self {
            sequence,
            price,
            observed_at,
        })
    }
}

/// One exact scanner candidate and its complete deterministic replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualGridScanCandidate {
    instrument: MarketInstrument,
    grid_width_percent: Decimal,
    grid_interval_percent: Decimal,
    volume_24h_usdc: Decimal,
    price_change_24h_percent: Option<Decimal>,
    priority: ScannerPriority,
    observations: Vec<VirtualGridScanObservation>,
}

impl VirtualGridScanCandidate {
    /// Validates one candidate before it can enter a scan request.
    ///
    /// Observations must be nonempty, contiguous from sequence one, bounded,
    /// and monotonic by explicit event time.
    ///
    /// # Errors
    ///
    /// Returns [`VirtualGridScannerError`] for an invalid grid, negative
    /// volume, missing/oversized observations, sequence gaps, or time rollback.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instrument: MarketInstrument,
        grid_width_percent: Decimal,
        grid_interval_percent: Decimal,
        volume_24h_usdc: Decimal,
        price_change_24h_percent: Option<Decimal>,
        priority: ScannerPriority,
        observations: Vec<VirtualGridScanObservation>,
    ) -> Result<Self, VirtualGridScannerError> {
        if !valid_market_identity_text(instrument.exchange())
            || !valid_market_identity_text(instrument.symbol.as_str())
        {
            return Err(VirtualGridScannerError::InvalidInstrumentIdentity);
        }
        if volume_24h_usdc < Decimal::ZERO {
            return Err(VirtualGridScannerError::NegativeVolume {
                instrument: instrument.clone(),
            });
        }
        if observations.is_empty() {
            return Err(VirtualGridScannerError::MissingObservations {
                instrument: instrument.clone(),
            });
        }
        if observations.len() > MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS_PER_CANDIDATE {
            return Err(VirtualGridScannerError::TooManyCandidateObservations {
                instrument: instrument.clone(),
                count: observations.len(),
                limit: MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS_PER_CANDIDATE,
            });
        }
        for (index, observation) in observations.iter().enumerate() {
            let expected = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
            if observation.sequence != expected {
                return Err(VirtualGridScannerError::InvalidObservationSequence {
                    expected,
                    observed: observation.sequence,
                });
            }
            if index > 0 && observation.observed_at < observations[index - 1].observed_at {
                return Err(VirtualGridScannerError::NonMonotonicObservationTime {
                    instrument: instrument.clone(),
                    sequence: observation.sequence,
                });
            }
        }

        // A unit-price probe validates grid geometry even when the caller's
        // concrete first price would otherwise hide a percentage-only defect.
        VirtualGrid::new(
            VirtualGridConfig {
                symbol: instrument.symbol.clone(),
                initial_price: Price::new(Decimal::ONE)
                    .map_err(|_| VirtualGridScannerError::InvalidGridProbe)?,
                grid_width_percent,
                grid_interval_percent,
            },
            DateTime::<Utc>::UNIX_EPOCH,
        )
        .map_err(VirtualGridScannerError::Strategy)?;

        Ok(Self {
            instrument,
            grid_width_percent,
            grid_interval_percent,
            volume_24h_usdc,
            price_change_24h_percent,
            priority,
            observations,
        })
    }
}

/// Complete bounded input for one deterministic ranking fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualGridScanRequest {
    run_id: String,
    evaluated_at: DateTime<Utc>,
    apr_window_seconds: u32,
    min_complete_cycles: u64,
    row_limit: usize,
    candidates: Vec<VirtualGridScanCandidate>,
}

impl VirtualGridScanRequest {
    /// Creates a complete scan request with no ambient clock or hidden input.
    ///
    /// # Errors
    ///
    /// Returns [`VirtualGridScannerError`] for an invalid run identity,
    /// resource bound, duplicate instrument, or evaluation time that precedes
    /// a candidate's final observation.
    pub fn new(
        run_id: impl Into<String>,
        evaluated_at: DateTime<Utc>,
        apr_window_seconds: u32,
        min_complete_cycles: u64,
        row_limit: usize,
        candidates: Vec<VirtualGridScanCandidate>,
    ) -> Result<Self, VirtualGridScannerError> {
        let run_id = run_id.into();
        let normalized_run_id = run_id.trim();
        if normalized_run_id.is_empty()
            || normalized_run_id.len() > MAX_SCAN_RUN_ID_BYTES
            || !normalized_run_id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(VirtualGridScannerError::InvalidRunId);
        }
        if apr_window_seconds == 0 || apr_window_seconds > MAX_VIRTUAL_GRID_SCAN_WINDOW_SECONDS {
            return Err(VirtualGridScannerError::InvalidAprWindow {
                seconds: apr_window_seconds,
            });
        }
        if !(1..=MAX_VIRTUAL_GRID_SCAN_ROWS).contains(&row_limit) {
            return Err(VirtualGridScannerError::InvalidRowLimit {
                rows: row_limit,
                limit: MAX_VIRTUAL_GRID_SCAN_ROWS,
            });
        }
        if candidates.is_empty() {
            return Err(VirtualGridScannerError::MissingCandidates);
        }
        if candidates.len() > MAX_VIRTUAL_GRID_SCAN_CANDIDATES {
            return Err(VirtualGridScannerError::TooManyCandidates {
                count: candidates.len(),
                limit: MAX_VIRTUAL_GRID_SCAN_CANDIDATES,
            });
        }

        let mut identities = candidates
            .iter()
            .map(|candidate| candidate.instrument.clone())
            .collect::<Vec<_>>();
        identities.sort();
        if let Some(duplicate) = identities
            .windows(2)
            .find(|pair| pair[0] == pair[1])
            .map(|pair| pair[0].clone())
        {
            return Err(VirtualGridScannerError::DuplicateInstrument {
                instrument: duplicate,
            });
        }

        let mut observation_count = 0_usize;
        for candidate in &candidates {
            observation_count = observation_count
                .checked_add(candidate.observations.len())
                .ok_or(VirtualGridScannerError::TooManyObservations {
                    count: usize::MAX,
                    limit: MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS,
                })?;
            let last = candidate.observations.last().ok_or_else(|| {
                VirtualGridScannerError::MissingObservations {
                    instrument: candidate.instrument.clone(),
                }
            })?;
            if evaluated_at < last.observed_at {
                return Err(VirtualGridScannerError::EvaluationPrecedesObservation {
                    instrument: candidate.instrument.clone(),
                });
            }
        }
        if observation_count > MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS {
            return Err(VirtualGridScannerError::TooManyObservations {
                count: observation_count,
                limit: MAX_VIRTUAL_GRID_SCAN_OBSERVATIONS,
            });
        }

        Ok(Self {
            run_id: normalized_run_id.to_owned(),
            evaluated_at,
            apr_window_seconds,
            min_complete_cycles,
            row_limit,
            candidates,
        })
    }
}

/// One fully evaluated and ranked read-only candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualGridScanRow {
    rank: usize,
    instrument: MarketInstrument,
    priority: ScannerPriority,
    started_at: DateTime<Utc>,
    last_observed_at: DateTime<Utc>,
    evaluated_at: DateTime<Utc>,
    observation_count: usize,
    last_observation_sequence: u64,
    current_price: Price,
    lower_price: Price,
    upper_price: Price,
    pending_buy_price: Price,
    pending_sell_price: Price,
    grid_width_percent: Decimal,
    grid_interval_percent: Decimal,
    grid_count: u32,
    running_seconds: i64,
    buy_crosses: u64,
    sell_crosses: u64,
    total_crosses: u64,
    complete_cycles: u64,
    recent_five_minute_cycles: usize,
    cycles_per_hour: Decimal,
    estimated_apr: Decimal,
    volume_24h_usdc: Decimal,
    price_change_24h_percent: Option<Decimal>,
    rating_grade: ScannerRatingGrade,
    rating_score: Decimal,
}

impl VirtualGridScanRow {
    pub const fn rank(&self) -> usize {
        self.rank
    }

    pub const fn activity(&self) -> ScannerActivity {
        ScannerActivity::Active
    }

    pub const fn instrument(&self) -> &MarketInstrument {
        &self.instrument
    }

    pub const fn is_benchmark(&self) -> bool {
        matches!(self.priority, ScannerPriority::Benchmark)
    }

    pub const fn estimated_apr(&self) -> Decimal {
        self.estimated_apr
    }

    pub const fn buy_crosses(&self) -> u64 {
        self.buy_crosses
    }

    pub const fn sell_crosses(&self) -> u64 {
        self.sell_crosses
    }

    pub const fn total_crosses(&self) -> u64 {
        self.total_crosses
    }

    pub const fn complete_cycles(&self) -> u64 {
        self.complete_cycles
    }

    pub const fn recent_five_minute_cycles(&self) -> usize {
        self.recent_five_minute_cycles
    }

    pub const fn cycles_per_hour(&self) -> Decimal {
        self.cycles_per_hour
    }

    pub const fn rating_grade(&self) -> ScannerRatingGrade {
        self.rating_grade
    }

    pub const fn rating_score(&self) -> Decimal {
        self.rating_score
    }

    pub const fn last_observation_sequence(&self) -> u64 {
        self.last_observation_sequence
    }

    pub const fn last_observed_at(&self) -> DateTime<Utc> {
        self.last_observed_at
    }

    pub const fn evaluated_at(&self) -> DateTime<Utc> {
        self.evaluated_at
    }
}

/// Durable report returned only after its full ranking record is synced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualGridScanReport {
    pub run_id: String,
    pub evaluated_at: DateTime<Utc>,
    pub apr_window_seconds: u32,
    pub min_complete_cycles: u64,
    pub row_limit: usize,
    pub candidate_count: usize,
    pub eligible_count: usize,
    pub filtered_by_cycles_count: usize,
    pub truncated: bool,
    pub rows: Vec<VirtualGridScanRow>,
}

impl VirtualGridScanReport {
    fn to_record(&self) -> DecisionRecord {
        DecisionRecord {
            timestamp: self.evaluated_at,
            strategy: SCANNER_STRATEGY.to_owned(),
            symbol: SCANNER_SYMBOL.to_owned(),
            decision: SCANNER_DECISION.to_owned(),
            details: json!({
                "schema_version": VIRTUAL_GRID_SCAN_RECORD_SCHEMA_VERSION,
                "run_id": self.run_id,
                "ranking_policy": RANKING_POLICY,
                "apr_window_seconds": self.apr_window_seconds,
                "min_complete_cycles": self.min_complete_cycles,
                "row_limit": self.row_limit,
                "candidate_count": self.candidate_count,
                "eligible_count": self.eligible_count,
                "filtered_by_cycles_count": self.filtered_by_cycles_count,
                "truncated": self.truncated,
                "rows": self.rows.iter().map(row_value).collect::<Vec<_>>(),
            }),
        }
    }
}

/// Stateless owner of the replay, ranking, and journal-first publication.
#[derive(Clone, Copy, Debug, Default)]
pub struct DeterministicVirtualGridScanner;

impl DeterministicVirtualGridScanner {
    /// Evaluates, ranks, and durably records one complete scan.
    ///
    /// The returned report is never observable before the corresponding
    /// history record has been flushed and synced.
    ///
    /// # Errors
    ///
    /// Returns [`VirtualGridScannerError`] for strategy/count failures or when
    /// the complete ranking cannot be durably appended.
    pub async fn run_and_record(
        request: VirtualGridScanRequest,
        history: &JsonlHistory,
    ) -> Result<VirtualGridScanReport, VirtualGridScannerError> {
        let candidate_count = request.candidates.len();
        let mut rows = Vec::with_capacity(candidate_count);
        let mut filtered_by_cycles_count = 0_usize;
        for candidate in request.candidates {
            let row =
                evaluate_candidate(candidate, request.evaluated_at, request.apr_window_seconds)?;
            if row.complete_cycles < request.min_complete_cycles && !row.is_benchmark() {
                filtered_by_cycles_count = filtered_by_cycles_count.saturating_add(1);
            } else {
                rows.push(row);
            }
        }
        rows.sort_by(compare_rows);
        let eligible_count = rows.len();
        let truncated = eligible_count > request.row_limit;
        rows.truncate(request.row_limit);
        for (index, row) in rows.iter_mut().enumerate() {
            row.rank = index.saturating_add(1);
        }

        let report = VirtualGridScanReport {
            run_id: request.run_id,
            evaluated_at: request.evaluated_at,
            apr_window_seconds: request.apr_window_seconds,
            min_complete_cycles: request.min_complete_cycles,
            row_limit: request.row_limit,
            candidate_count,
            eligible_count,
            filtered_by_cycles_count,
            truncated,
            rows,
        };
        history
            .append(&report.to_record())
            .await
            .map_err(VirtualGridScannerError::Journal)?;
        Ok(report)
    }
}

fn evaluate_candidate(
    candidate: VirtualGridScanCandidate,
    evaluated_at: DateTime<Utc>,
    apr_window_seconds: u32,
) -> Result<VirtualGridScanRow, VirtualGridScannerError> {
    let first = candidate.observations.first().ok_or_else(|| {
        VirtualGridScannerError::MissingObservations {
            instrument: candidate.instrument.clone(),
        }
    })?;
    let started_at = first.observed_at;
    let mut grid = VirtualGrid::new(
        VirtualGridConfig {
            symbol: candidate.instrument.symbol.clone(),
            initial_price: first.price,
            grid_width_percent: candidate.grid_width_percent,
            grid_interval_percent: candidate.grid_interval_percent,
        },
        started_at,
    )
    .map_err(VirtualGridScannerError::Strategy)?;
    for observation in candidate.observations.iter().skip(1) {
        grid.update_price_at(observation.price, observation.observed_at)
            .map_err(VirtualGridScannerError::Strategy)?;
    }
    grid.calculate_apr_at(
        evaluated_at,
        Duration::seconds(i64::from(apr_window_seconds)),
    )
    .map_err(VirtualGridScannerError::Strategy)?;
    let last = candidate.observations.last().ok_or_else(|| {
        VirtualGridScannerError::MissingObservations {
            instrument: candidate.instrument.clone(),
        }
    })?;
    let rating = Rating::calculate(
        grid.estimated_apr(),
        grid.cycles_per_hour(),
        candidate.volume_24h_usdc,
    );
    let running_seconds = evaluated_at.signed_duration_since(started_at).num_seconds();
    let observation_count = candidate.observations.len();
    let total_crosses = grid
        .buy_crosses()
        .checked_add(grid.sell_crosses())
        .ok_or(VirtualGridScannerError::CountOverflow)?;

    Ok(VirtualGridScanRow {
        rank: 0,
        instrument: candidate.instrument,
        priority: candidate.priority,
        started_at,
        last_observed_at: last.observed_at,
        evaluated_at,
        observation_count,
        last_observation_sequence: last.sequence,
        current_price: grid.current_price(),
        lower_price: grid.lower_price(),
        upper_price: grid.upper_price(),
        pending_buy_price: grid.pending_buy_price(),
        pending_sell_price: grid.pending_sell_price(),
        grid_width_percent: candidate.grid_width_percent,
        grid_interval_percent: candidate.grid_interval_percent,
        grid_count: grid.grid_count(),
        running_seconds,
        buy_crosses: grid.buy_crosses(),
        sell_crosses: grid.sell_crosses(),
        total_crosses,
        complete_cycles: grid.complete_cycles(),
        recent_five_minute_cycles: grid
            .recent_cycles_at(evaluated_at, Duration::seconds(RECENT_WINDOW_SECONDS)),
        cycles_per_hour: grid.cycles_per_hour(),
        estimated_apr: grid.estimated_apr(),
        volume_24h_usdc: candidate.volume_24h_usdc,
        price_change_24h_percent: candidate.price_change_24h_percent,
        rating_grade: rating.grade.into(),
        rating_score: rating.score,
    })
}

fn compare_rows(left: &VirtualGridScanRow, right: &VirtualGridScanRow) -> Ordering {
    left.priority
        .rank()
        .cmp(&right.priority.rank())
        .then_with(|| right.estimated_apr.cmp(&left.estimated_apr))
        .then_with(|| left.instrument.cmp(&right.instrument))
}

fn row_value(row: &VirtualGridScanRow) -> Value {
    json!({
        "rank": row.rank,
        "activity": "active",
        "priority": row.priority.as_str(),
        "instrument": {
            "exchange": row.instrument.exchange(),
            "symbol": row.instrument.symbol.as_str(),
            "market_type": row.instrument.market_type,
        },
        "started_at": row.started_at,
        "last_observed_at": row.last_observed_at,
        "observation_count": row.observation_count,
        "last_observation_sequence": row.last_observation_sequence,
        "current_price": decimal_text(row.current_price.as_decimal()),
        "lower_price": decimal_text(row.lower_price.as_decimal()),
        "upper_price": decimal_text(row.upper_price.as_decimal()),
        "pending_buy_price": decimal_text(row.pending_buy_price.as_decimal()),
        "pending_sell_price": decimal_text(row.pending_sell_price.as_decimal()),
        "grid_width_percent": decimal_text(row.grid_width_percent),
        "grid_interval_percent": decimal_text(row.grid_interval_percent),
        "grid_count": row.grid_count,
        "running_seconds": row.running_seconds,
        "buy_crosses": row.buy_crosses,
        "sell_crosses": row.sell_crosses,
        "total_crosses": row.total_crosses,
        "complete_cycles": row.complete_cycles,
        "recent_five_minute_cycles": row.recent_five_minute_cycles,
        "cycles_per_hour": decimal_text(row.cycles_per_hour),
        "estimated_apr": decimal_text(row.estimated_apr),
        "volume_24h_usdc": decimal_text(row.volume_24h_usdc),
        "price_change_24h_percent": row.price_change_24h_percent.map(decimal_text),
        "rating_grade": row.rating_grade.to_string(),
        "rating_score": decimal_text(row.rating_score),
    })
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn valid_market_identity_text(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SCAN_RUN_ID_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
}

/// Bounded failure surface for deterministic scanner callers.
#[derive(Debug)]
pub enum VirtualGridScannerError {
    InvalidRunId,
    InvalidInstrumentIdentity,
    InvalidAprWindow {
        seconds: u32,
    },
    InvalidRowLimit {
        rows: usize,
        limit: usize,
    },
    MissingCandidates,
    TooManyCandidates {
        count: usize,
        limit: usize,
    },
    MissingObservations {
        instrument: MarketInstrument,
    },
    TooManyCandidateObservations {
        instrument: MarketInstrument,
        count: usize,
        limit: usize,
    },
    TooManyObservations {
        count: usize,
        limit: usize,
    },
    InvalidObservationSequence {
        expected: u64,
        observed: u64,
    },
    NonMonotonicObservationTime {
        instrument: MarketInstrument,
        sequence: u64,
    },
    EvaluationPrecedesObservation {
        instrument: MarketInstrument,
    },
    DuplicateInstrument {
        instrument: MarketInstrument,
    },
    NegativeVolume {
        instrument: MarketInstrument,
    },
    InvalidGridProbe,
    CountOverflow,
    Strategy(StrategyError),
    Journal(HistoryError),
}

impl fmt::Display for VirtualGridScannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRunId => formatter.write_str("scanner run id is invalid"),
            Self::InvalidInstrumentIdentity => {
                formatter.write_str("scanner instrument identity is not canonical ASCII")
            }
            Self::InvalidAprWindow { seconds } => {
                write!(formatter, "scanner APR window {seconds}s is invalid")
            }
            Self::InvalidRowLimit { rows, limit } => {
                write!(formatter, "scanner row limit {rows} exceeds 1..={limit}")
            }
            Self::MissingCandidates => {
                formatter.write_str("scanner requires at least one candidate")
            }
            Self::TooManyCandidates { count, limit } => {
                write!(
                    formatter,
                    "scanner has {count} candidates; maximum is {limit}"
                )
            }
            Self::MissingObservations { instrument } => {
                write!(
                    formatter,
                    "scanner candidate {instrument:?} has no observations"
                )
            }
            Self::TooManyCandidateObservations {
                instrument,
                count,
                limit,
            } => write!(
                formatter,
                "scanner candidate {instrument:?} has {count} observations; maximum is {limit}"
            ),
            Self::TooManyObservations { count, limit } => {
                write!(
                    formatter,
                    "scanner has {count} observations; maximum is {limit}"
                )
            }
            Self::InvalidObservationSequence { expected, observed } => write!(
                formatter,
                "scanner observation sequence {observed} does not match expected {expected}"
            ),
            Self::NonMonotonicObservationTime {
                instrument,
                sequence,
            } => write!(
                formatter,
                "scanner candidate {instrument:?} rolls time back at sequence {sequence}"
            ),
            Self::EvaluationPrecedesObservation { instrument } => write!(
                formatter,
                "scanner evaluation time precedes the last observation for {instrument:?}"
            ),
            Self::DuplicateInstrument { instrument } => {
                write!(formatter, "scanner instrument {instrument:?} is duplicated")
            }
            Self::NegativeVolume { instrument } => {
                write!(formatter, "scanner volume for {instrument:?} is negative")
            }
            Self::InvalidGridProbe => formatter.write_str("scanner grid probe price is invalid"),
            Self::CountOverflow => formatter.write_str("scanner count overflowed"),
            Self::Strategy(source) => write!(formatter, "scanner strategy failed: {source}"),
            Self::Journal(source) => write!(formatter, "scanner journal failed: {source}"),
        }
    }
}

impl std::error::Error for VirtualGridScannerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Strategy(source) => Some(source),
            Self::Journal(source) => Some(source),
            _ => None,
        }
    }
}
