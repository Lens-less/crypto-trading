//! History-based ("natural spread") arbitrage decision machine.
//!
//! This module ports the historical decision mode of the frozen Python
//! implementation into a pure, bounded state machine. The authoritative
//! semantics live in `archive/python-legacy/core/services/arbitrage_monitor_v2`:
//!
//! - Natural spread is the sign-preserving median of the recorded spread
//!   history (`history/history_calculator.py:415`, using `statistics.median`,
//!   so an even sample count averages the two middle values).
//! - A direction with fewer than the minimum number of samples has no natural
//!   spread and must not open (`history/history_calculator.py:407`,
//!   `decision/arbitrage_decision.py:296-312`).
//! - A negative natural spread is treated as zero
//!   (`decision/arbitrage_decision.py:333-334`).
//! - The real arbitrage space is the current spread minus the effective
//!   natural spread, and an opportunity requires that space to reach the
//!   configured threshold (`decision/arbitrage_decision.py:337` and `:345`).
//! - The natural funding-rate difference is the median of the recorded
//!   absolute 8-hour funding-rate differences, and it also requires the
//!   minimum sample count (`history/history_calculator.py:423-428`).
//! - The annualized funding-rate difference multiplies the 8-hour difference
//!   by 1095 (three intervals per day for 365 days) and converts the fraction
//!   to percent (`core/orchestrator.py:436`, `core/spread_pipeline.py:529`).
//! - Python checks an unfavourable funding difference against the annualized
//!   threshold (`decision/arbitrage_decision.py:398-401`) but then falls
//!   through to a permissive warning; this port enforces that check
//!   fail-closed instead of logging and allowing.
//! - Missing funding data degrades the funding term instead of blocking the
//!   spread decision (`decision/arbitrage_decision.py:403-410`); the decision
//!   reports this as `funding_degraded: true`.
//!
//! No I/O happens here: samples arrive through [`HistoryDecisionMachine::observe`]
//! and decisions are produced by the pure [`HistoryDecisionMachine::evaluate`].

use std::collections::VecDeque;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::StrategyError;

/// Hard capacity of the bounded in-memory sample ring.
pub const MAX_SPREAD_SAMPLES: usize = 4_096;
/// Maximum evaluation window, mirroring the Python 24-hour history ceiling
/// (`history/history_calculator.py:19` and `:74`).
pub const MAX_HISTORY_WINDOW_SECONDS: i64 = 86_400;
/// Funding intervals per year: three 8-hour intervals per day for 365 days
/// (`core/spread_pipeline.py:529`).
pub const FUNDING_INTERVALS_PER_YEAR: u32 = 1_095;
/// Mirrors the segmented-arbitrage business limit on suggested segments.
const MAX_HISTORY_SEGMENTS: u32 = 10_000;
const MAX_SAMPLE_EXCHANGE_BYTES: usize = 128;

/// One observed cross-exchange spread with optional funding rates.
///
/// Funding rates are 8-hour rates expressed as decimal fractions
/// (`0.0001 == 0.01%`). They are `None` whenever the market-data source does
/// not publish funding data; the decision then degrades instead of guessing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpreadSample {
    pub timestamp: DateTime<Utc>,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: Decimal,
    pub sell_price: Decimal,
    /// Spread of this direction in basis points (percent x 100).
    pub spread_bps: Decimal,
    pub funding_rate_buy: Option<Decimal>,
    pub funding_rate_sell: Option<Decimal>,
}

impl SpreadSample {
    /// Signed 8-hour funding-rate difference earned by the short (sell) leg.
    ///
    /// Positive values mean the position collects funding; negative values
    /// mean it pays. Requires both legs to publish a funding rate.
    #[must_use]
    pub fn funding_rate_diff(&self) -> Option<Decimal> {
        self.funding_rate_sell?.checked_sub(self.funding_rate_buy?)
    }

    /// Annualized funding-rate difference in percent per year:
    /// `diff x 1095 x 100` (`core/orchestrator.py:436`).
    #[must_use]
    pub fn funding_rate_diff_annual_pct(&self) -> Option<Decimal> {
        self.funding_rate_diff()?
            .checked_mul(Decimal::from(FUNDING_INTERVALS_PER_YEAR))?
            .checked_mul(Decimal::ONE_HUNDRED)
    }

    fn validate(&self) -> Result<(), StrategyError> {
        for exchange in [&self.buy_exchange, &self.sell_exchange] {
            if exchange.trim().is_empty() || exchange.len() > MAX_SAMPLE_EXCHANGE_BYTES {
                return Err(StrategyError::InvalidConfig(
                    "spread sample exchange identity is empty or oversized",
                ));
            }
        }
        if self.buy_exchange == self.sell_exchange {
            return Err(StrategyError::InvalidConfig(
                "spread sample requires two distinct exchanges",
            ));
        }
        if self.buy_price <= Decimal::ZERO || self.sell_price <= Decimal::ZERO {
            return Err(StrategyError::InvalidFinancialValue(
                "spread sample prices must be positive",
            ));
        }
        Ok(())
    }

    fn direction_matches(&self, other: &Self) -> bool {
        self.buy_exchange == other.buy_exchange && self.sell_exchange == other.sell_exchange
    }
}

/// Pure median calculator matching Python `statistics.median`
/// (`history/history_calculator.py:415`): the middle value for an odd count,
/// the mean of the two middle values for an even count.
#[derive(Clone, Copy, Debug, Default)]
pub struct NaturalSpreadCalculator;

impl NaturalSpreadCalculator {
    /// Returns the sign-preserving median of `values`, or `None` when empty.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidFinancialValue`] when the even-count
    /// midpoint cannot be represented.
    pub fn median(values: &[Decimal]) -> Result<Option<Decimal>, StrategyError> {
        if values.is_empty() {
            return Ok(None);
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let middle = sorted.len() / 2;
        if sorted.len() % 2 == 1 {
            return Ok(Some(sorted[middle]));
        }
        let sum = sorted[middle - 1].checked_add(sorted[middle]).ok_or(
            StrategyError::InvalidFinancialValue("natural spread median midpoint"),
        )?;
        let two = Decimal::from(2u8);
        sum.checked_div(two)
            .map(Some)
            .ok_or(StrategyError::InvalidFinancialValue(
                "natural spread median division",
            ))
    }
}

/// Validated numeric controls for the history decision machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryArbitrageConfig {
    /// Look-back window applied relative to the evaluated sample's timestamp.
    pub window: Duration,
    /// Minimum in-window, same-direction samples before any judgement
    /// (`history/history_calculator.py:407`).
    pub min_samples: usize,
    /// Required real arbitrage space in basis points
    /// (`decision/arbitrage_decision.py:345`).
    pub deviation_threshold_bps: Decimal,
    /// Annualized funding-rate threshold in percent per year
    /// (`decision/arbitrage_decision.py:398-401`,
    /// default 10.0 in `config/arbitrage_config.py:65`).
    pub funding_rate_annual_threshold_pct: Decimal,
}

/// Bounded, explicit outcome kinds of one history evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryDecisionKind {
    /// Fewer than `min_samples` in-window, same-direction samples: the
    /// machine refuses to judge (fail closed, no order).
    InsufficientHistory,
    /// Enough history, but the deviation from the natural spread (or the
    /// funding gate) does not justify opening.
    Hold,
    /// The current spread deviates from the natural spread by at least the
    /// configured threshold; opening in this direction is suggested.
    Open,
}

/// One pure history decision, aligned with the segmented-arbitrage vocabulary
/// (direction as buy/sell exchanges, suggested position size as a segment).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryDecision {
    pub kind: HistoryDecisionKind,
    /// Suggested segment count: whole multiples of the deviation threshold
    /// covered by the real arbitrage space. Zero unless `kind` is `Open`.
    pub segment: u32,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub current_spread_bps: Decimal,
    /// Sign-preserving natural spread; `None` for `InsufficientHistory`.
    pub natural_spread_bps: Option<Decimal>,
    /// `current - max(natural, 0)`; `None` for `InsufficientHistory`.
    pub real_arbitrage_space_bps: Option<Decimal>,
    /// Median absolute 8-hour funding-rate difference of the window, present
    /// only when at least `min_samples` samples carried funding data.
    pub natural_funding_rate_diff: Option<Decimal>,
    /// Annualized funding difference of the evaluated sample, in percent.
    pub funding_rate_diff_annual_pct: Option<Decimal>,
    /// True when the evaluated sample carries no funding data, so the funding
    /// term was skipped instead of enforced.
    pub funding_degraded: bool,
    /// In-window, same-direction samples the judgement was based on.
    pub window_sample_count: usize,
}

/// Bounded ring of spread samples plus the pure natural-spread judgement.
#[derive(Clone, Debug)]
pub struct HistoryDecisionMachine {
    config: HistoryArbitrageConfig,
    samples: VecDeque<SpreadSample>,
}

impl HistoryDecisionMachine {
    /// Validates the numeric controls and creates an empty machine.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError::InvalidConfig`] for a non-positive or
    /// oversized window, a zero or over-capacity minimum sample count, a
    /// non-positive deviation threshold, or a negative funding threshold.
    pub fn new(config: HistoryArbitrageConfig) -> Result<Self, StrategyError> {
        if config.window <= Duration::zero()
            || config.window > Duration::seconds(MAX_HISTORY_WINDOW_SECONDS)
        {
            return Err(StrategyError::InvalidConfig(
                "history window must be positive and at most 24 hours",
            ));
        }
        if config.min_samples == 0 || config.min_samples > MAX_SPREAD_SAMPLES {
            return Err(StrategyError::InvalidConfig(
                "history minimum sample count must be within the ring capacity",
            ));
        }
        if config.deviation_threshold_bps <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "history deviation threshold must be positive",
            ));
        }
        if config.funding_rate_annual_threshold_pct < Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "history funding threshold must not be negative",
            ));
        }
        Ok(Self {
            config,
            samples: VecDeque::new(),
        })
    }

    #[must_use]
    pub const fn config(&self) -> &HistoryArbitrageConfig {
        &self.config
    }

    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Records one sample, evicting anything past the ring capacity or older
    /// than the window relative to the newest timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for an invalid sample or a timestamp that
    /// regresses behind the newest recorded sample.
    pub fn observe(&mut self, sample: SpreadSample) -> Result<(), StrategyError> {
        sample.validate()?;
        if let Some(latest) = self.samples.back()
            && sample.timestamp < latest.timestamp
        {
            return Err(StrategyError::SnapshotMismatch(
                "history sample timestamp regressed behind recorded history".to_owned(),
            ));
        }
        self.samples.push_back(sample);
        if self.samples.len() > MAX_SPREAD_SAMPLES {
            self.samples.pop_front();
        }
        let newest = self
            .samples
            .back()
            .map(|sample| sample.timestamp)
            .unwrap_or_default();
        let horizon = newest - self.config.window;
        while let Some(front) = self.samples.front() {
            if front.timestamp >= horizon {
                break;
            }
            self.samples.pop_front();
        }
        Ok(())
    }

    /// Judges `current` against the recorded natural spread of its direction.
    ///
    /// The window is anchored at `current.timestamp`; only recorded samples of
    /// the same direction inside `[current.timestamp - window, current.timestamp]`
    /// participate. Fewer than `min_samples` such samples yield
    /// [`HistoryDecisionKind::InsufficientHistory`] and never an opportunity.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] for an invalid sample or a financial value
    /// outside the domain.
    pub fn evaluate(&self, current: &SpreadSample) -> Result<HistoryDecision, StrategyError> {
        current.validate()?;
        let horizon = current.timestamp - self.config.window;
        let window: Vec<&SpreadSample> = self
            .samples
            .iter()
            .filter(|sample| {
                sample.direction_matches(current)
                    && sample.timestamp >= horizon
                    && sample.timestamp <= current.timestamp
            })
            .collect();
        let funding_rate_diff_annual_pct = current.funding_rate_diff_annual_pct();
        let funding_degraded = current.funding_rate_diff().is_none();

        if window.len() < self.config.min_samples {
            // decision/arbitrage_decision.py:296-312: insufficient natural
            // spread data refuses to open (fail closed).
            return Ok(HistoryDecision {
                kind: HistoryDecisionKind::InsufficientHistory,
                segment: 0,
                buy_exchange: current.buy_exchange.clone(),
                sell_exchange: current.sell_exchange.clone(),
                current_spread_bps: current.spread_bps,
                natural_spread_bps: None,
                real_arbitrage_space_bps: None,
                natural_funding_rate_diff: None,
                funding_rate_diff_annual_pct,
                funding_degraded,
                window_sample_count: window.len(),
            });
        }

        let spreads: Vec<Decimal> = window.iter().map(|sample| sample.spread_bps).collect();
        let natural_spread_bps = NaturalSpreadCalculator::median(&spreads)?.ok_or(
            StrategyError::InvalidFinancialValue("natural spread median of a non-empty window"),
        )?;
        // decision/arbitrage_decision.py:334: a negative natural spread is
        // treated as zero.
        let effective_natural = natural_spread_bps.max(Decimal::ZERO);
        // decision/arbitrage_decision.py:337.
        let real_space = current.spread_bps.checked_sub(effective_natural).ok_or(
            StrategyError::InvalidFinancialValue("history real arbitrage space"),
        )?;

        let mut funding_diffs: Vec<Decimal> = Vec::new();
        for sample in &window {
            if let Some(diff) = sample.funding_rate_diff() {
                // history/history_calculator.py:423-425: the stored natural
                // funding difference is an absolute-value difference.
                funding_diffs.push(diff.abs());
            }
        }
        let natural_funding_rate_diff = if funding_diffs.len() >= self.config.min_samples {
            // history/history_calculator.py:426-428.
            NaturalSpreadCalculator::median(&funding_diffs)?
        } else {
            None
        };

        let funding_allows = match (current.funding_rate_diff(), funding_rate_diff_annual_pct) {
            // decision/arbitrage_decision.py:395: collecting funding passes.
            (Some(diff), _) if diff >= Decimal::ZERO => true,
            // decision/arbitrage_decision.py:398-401: paying funding passes
            // only while the annualized cost stays under the threshold. The
            // Python code then falls through permissively; this port enforces
            // the check fail-closed.
            (Some(_), Some(annual)) => annual.abs() < self.config.funding_rate_annual_threshold_pct,
            (Some(_), None) => false,
            // decision/arbitrage_decision.py:403-410: missing funding data
            // degrades the funding term instead of blocking the spread path.
            (None, _) => true,
        };

        // decision/arbitrage_decision.py:345.
        let open = real_space >= self.config.deviation_threshold_bps && funding_allows;
        let segment = if open {
            segment_for_space(real_space, self.config.deviation_threshold_bps)?
        } else {
            0
        };
        Ok(HistoryDecision {
            kind: if open {
                HistoryDecisionKind::Open
            } else {
                HistoryDecisionKind::Hold
            },
            segment,
            buy_exchange: current.buy_exchange.clone(),
            sell_exchange: current.sell_exchange.clone(),
            current_spread_bps: current.spread_bps,
            natural_spread_bps: Some(natural_spread_bps),
            real_arbitrage_space_bps: Some(real_space),
            natural_funding_rate_diff,
            funding_rate_diff_annual_pct,
            funding_degraded,
            window_sample_count: window.len(),
        })
    }
}

/// Counts whole deviation thresholds covered by the real arbitrage space,
/// mirroring how segmented arbitrage counts crossed open thresholds.
fn segment_for_space(real_space: Decimal, threshold: Decimal) -> Result<u32, StrategyError> {
    let ratio = real_space
        .checked_div(threshold)
        .ok_or(StrategyError::InvalidFinancialValue(
            "history segment ratio",
        ))?;
    Ok(ratio
        .floor()
        .to_u32()
        .unwrap_or(MAX_HISTORY_SEGMENTS)
        .clamp(1, MAX_HISTORY_SEGMENTS))
}

impl TryFrom<&crypto_trading_config::ArbitrageHistoryDecisionConfig> for HistoryDecisionMachine {
    type Error = StrategyError;

    fn try_from(
        config: &crypto_trading_config::ArbitrageHistoryDecisionConfig,
    ) -> Result<Self, Self::Error> {
        if !config.enabled {
            return Err(StrategyError::InvalidConfig(
                "arbitrage history decision mode is disabled",
            ));
        }
        let window_seconds = i64::try_from(config.window_seconds).map_err(|_| {
            StrategyError::InvalidConfig("history window exceeds the supported duration")
        })?;
        let min_samples = usize::try_from(config.min_samples).map_err(|_| {
            StrategyError::InvalidConfig("history minimum sample count exceeds the address space")
        })?;
        Self::new(HistoryArbitrageConfig {
            window: Duration::seconds(window_seconds),
            min_samples,
            deviation_threshold_bps: config.deviation_threshold_bps,
            funding_rate_annual_threshold_pct: config.funding_rate_annual_threshold_pct,
        })
    }
}
