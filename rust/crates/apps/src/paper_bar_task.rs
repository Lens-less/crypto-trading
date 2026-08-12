use crypto_trading_domain::Side;
use crypto_trading_strategy::{Bar, BarStrategy, StrategyError, TargetExposure};
use std::cmp::Ordering;

/// Pure rebalance action derived from one bar-driven target transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaperBarAction {
    Hold,
    Rebalance { side: Side, target: TargetExposure },
}

/// Deterministic paper-owner decision for one completed bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperBarDecision {
    pub bar_index: usize,
    pub decided_at: chrono::DateTime<chrono::Utc>,
    pub target: TargetExposure,
    pub action: PaperBarAction,
}

/// Minimal bar-owner failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaperBarTaskError {
    InvalidBarSequence,
    Strategy(StrategyError),
}

/// Resume state for one bar-driven paper owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaperBarTaskState {
    pub next_bar_index: usize,
    pub current_target: TargetExposure,
}

/// Minimal in-memory owner that consumes one shared pure bar strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperBarTask<S> {
    strategy: S,
    history: Vec<Bar>,
    next_bar_index: usize,
    current_target: TargetExposure,
}

impl<S> PaperBarTask<S>
where
    S: BarStrategy,
{
    #[must_use]
    pub fn new(strategy: S) -> Self {
        Self::with_state(
            strategy,
            PaperBarTaskState {
                next_bar_index: 0,
                current_target: TargetExposure::ZERO,
            },
        )
    }

    #[must_use]
    pub fn with_state(strategy: S, state: PaperBarTaskState) -> Self {
        Self {
            strategy,
            history: Vec::new(),
            next_bar_index: state.next_bar_index,
            current_target: state.current_target,
        }
    }

    #[must_use]
    pub const fn state(&self) -> PaperBarTaskState {
        PaperBarTaskState {
            next_bar_index: self.next_bar_index,
            current_target: self.current_target,
        }
    }

    /// Feeds one completed bar through the shared strategy contract.
    ///
    /// # Errors
    ///
    /// Propagates pure strategy failures without introducing I/O.
    pub fn on_bar(&mut self, bar: Bar) -> Result<PaperBarDecision, PaperBarTaskError> {
        let decision = self.evaluate_bar(bar, self.current_target)?;
        // The convenience API assumes the requested target is achieved before
        // the next bar. Real owners use `on_bar_with_current_target` instead.
        self.current_target = decision.target;
        Ok(decision)
    }

    /// Feeds one completed bar through the shared strategy contract while the
    /// caller supplies the currently achieved target.
    ///
    /// # Errors
    ///
    /// Propagates pure strategy failures without introducing I/O.
    pub fn on_bar_with_current_target(
        &mut self,
        bar: Bar,
        current_target: TargetExposure,
    ) -> Result<PaperBarDecision, PaperBarTaskError> {
        let decision = self.evaluate_bar(bar, current_target)?;
        self.current_target = current_target;
        Ok(decision)
    }

    fn evaluate_bar(
        &mut self,
        bar: Bar,
        current_target: TargetExposure,
    ) -> Result<PaperBarDecision, PaperBarTaskError> {
        if self.history.last().is_some_and(|previous| {
            previous.open_time >= bar.open_time || previous.close_time >= bar.open_time
        }) {
            return Err(PaperBarTaskError::InvalidBarSequence);
        }
        let bar_index = self.next_bar_index;
        self.history.push(bar);
        let decided_at = self
            .history
            .last()
            .expect("history contains the pushed bar")
            .close_time;
        let next_target =
            self.strategy
                .target_exposure(&crypto_trading_strategy::BarStrategyContext {
                    history: &self.history,
                    decided_at,
                    bar_index,
                    current_target: current_target.as_decimal(),
                })?;
        let action = match next_target.as_decimal().cmp(&current_target.as_decimal()) {
            Ordering::Greater => PaperBarAction::Rebalance {
                side: Side::Buy,
                target: next_target,
            },
            Ordering::Less => PaperBarAction::Rebalance {
                side: Side::Sell,
                target: next_target,
            },
            Ordering::Equal => PaperBarAction::Hold,
        };
        self.next_bar_index =
            self.next_bar_index
                .checked_add(1)
                .ok_or(PaperBarTaskError::Strategy(
                    StrategyError::InvalidFinancialValue("bar"),
                ))?;
        Ok(PaperBarDecision {
            bar_index,
            decided_at,
            target: next_target,
            action,
        })
    }
}

impl From<StrategyError> for PaperBarTaskError {
    fn from(value: StrategyError) -> Self {
        Self::Strategy(value)
    }
}

impl std::fmt::Display for PaperBarTaskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBarSequence => {
                formatter.write_str("paper bar task requires strictly increasing completed bars")
            }
            Self::Strategy(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for PaperBarTaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidBarSequence => None,
            Self::Strategy(error) => Some(error),
        }
    }
}
