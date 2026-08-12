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

/// Minimal in-memory owner that consumes one shared pure bar strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperBarTask<S> {
    strategy: S,
    history: Vec<Bar>,
    current_target: TargetExposure,
}

impl<S> PaperBarTask<S>
where
    S: BarStrategy,
{
    #[must_use]
    pub fn new(strategy: S) -> Self {
        Self {
            strategy,
            history: Vec::new(),
            current_target: TargetExposure::ZERO,
        }
    }

    /// Feeds one completed bar through the shared strategy contract.
    ///
    /// # Errors
    ///
    /// Propagates pure strategy failures without introducing I/O.
    pub fn on_bar(&mut self, bar: Bar) -> Result<PaperBarDecision, PaperBarTaskError> {
        if self.history.last().is_some_and(|previous| {
            previous.open_time >= bar.open_time || previous.close_time >= bar.open_time
        }) {
            return Err(PaperBarTaskError::InvalidBarSequence);
        }
        self.history.push(bar);
        let bar_index = self.history.len() - 1;
        let decided_at = self.history[bar_index].close_time;
        let next_target =
            self.strategy
                .target_exposure(&crypto_trading_strategy::BarStrategyContext {
                    history: &self.history,
                    decided_at,
                    bar_index,
                    current_target: self.current_target.as_decimal(),
                })?;
        let action = match next_target
            .as_decimal()
            .cmp(&self.current_target.as_decimal())
        {
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
        self.current_target = next_target;
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
