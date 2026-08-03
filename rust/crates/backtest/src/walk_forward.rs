use std::ops::Range;

use crate::{
    BacktestError,
    engine::{BacktestEngine, BacktestResult, EventTape, MarketEvent, Strategy},
};

/// Walk-forward sizing parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkForwardConfig {
    train: usize,
    test: usize,
    step: usize,
}

impl WalkForwardConfig {
    /// Creates a walk-forward configuration.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidWalkForwardConfig`] when any size is
    /// zero.
    pub fn new(
        train_size: usize,
        test_size: usize,
        step_size: usize,
    ) -> Result<Self, BacktestError> {
        if train_size == 0 || test_size == 0 || step_size == 0 {
            return Err(BacktestError::InvalidWalkForwardConfig);
        }

        Ok(Self {
            train: train_size,
            test: test_size,
            step: step_size,
        })
    }

    /// Returns the number of observations in each training window.
    #[must_use]
    pub const fn train_size(self) -> usize {
        self.train
    }

    /// Returns the number of observations in each out-of-sample window.
    #[must_use]
    pub const fn test_size(self) -> usize {
        self.test
    }

    /// Returns the number of observations advanced between windows.
    #[must_use]
    pub const fn step_size(self) -> usize {
        self.step
    }
}

/// Public out-of-sample window report. Training ranges stay internal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutOfSampleWindow {
    pub window_index: usize,
    pub range: Range<usize>,
    training_range: Range<usize>,
}

/// Deterministic walk-forward splitter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkForwardSplitter {
    config: WalkForwardConfig,
}

impl WalkForwardSplitter {
    #[must_use]
    pub const fn new(config: WalkForwardConfig) -> Self {
        Self { config }
    }

    /// Returns only out-of-sample report windows. Training ranges stay private
    /// and are consumed only by [`WalkForwardRunner`] for strategy selection.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::WalkForwardIndexOverflow`] rather than
    /// truncating a valid prefix when index arithmetic overflows.
    pub fn out_of_sample_windows(
        &self,
        total_len: usize,
    ) -> Result<Vec<OutOfSampleWindow>, BacktestError> {
        let mut windows = Vec::new();
        let mut train_start = 0_usize;
        let mut window_index = 0_usize;

        loop {
            if train_start >= total_len {
                break;
            }
            let test_start = train_start
                .checked_add(self.config.train)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
            let test_end = test_start
                .checked_add(self.config.test)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
            if test_end > total_len {
                break;
            }
            windows.push(OutOfSampleWindow {
                window_index,
                range: test_start..test_end,
                training_range: train_start..test_start,
            });
            train_start = train_start
                .checked_add(self.config.step)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
            window_index = window_index
                .checked_add(1)
                .ok_or(BacktestError::WalkForwardIndexOverflow)?;
        }

        Ok(windows)
    }
}

/// One independently selected out-of-sample backtest result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkForwardWindowResult {
    pub window_index: usize,
    pub range: Range<usize>,
    pub result: BacktestResult,
}

/// Walk-forward output containing only out-of-sample backtest results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkForwardResult {
    pub windows: Vec<WalkForwardWindowResult>,
}

/// Runs independent strategy selection and out-of-sample evaluation for each
/// complete walk-forward window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkForwardRunner {
    engine: BacktestEngine,
    splitter: WalkForwardSplitter,
}

impl WalkForwardRunner {
    #[must_use]
    pub const fn new(engine: BacktestEngine, splitter: WalkForwardSplitter) -> Self {
        Self { engine, splitter }
    }

    /// Selects a fresh strategy from each training slice, then runs it only on
    /// that window's out-of-sample slice. Training results are never returned.
    ///
    /// # Errors
    ///
    /// Propagates typed splitter, strategy-selection, tape, execution, and
    /// metric failures.
    pub fn run<S, F>(
        &self,
        tape: &EventTape,
        mut select_strategy: F,
    ) -> Result<WalkForwardResult, BacktestError>
    where
        S: Strategy,
        F: FnMut(usize, &[MarketEvent]) -> Result<S, BacktestError>,
    {
        let windows = self.splitter.out_of_sample_windows(tape.events().len())?;
        let mut results = Vec::new();
        results
            .try_reserve_exact(windows.len())
            .map_err(|_| BacktestError::ArithmeticOverflow)?;

        for window in windows {
            let mut strategy = select_strategy(
                window.window_index,
                &tape.events()[window.training_range.clone()],
            )?;
            let test_tape = EventTape::new(tape.events()[window.range.clone()].to_vec())?;
            let result = self.engine.run(&test_tape, &mut strategy)?;
            results.push(WalkForwardWindowResult {
                window_index: window.window_index,
                range: window.range,
                result,
            });
        }

        Ok(WalkForwardResult { windows: results })
    }
}
