use std::ops::Range;

use crate::BacktestError;

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

    /// Returns only out-of-sample windows. Training windows remain implicit so
    /// downstream reports cannot accidentally include in-sample data.
    #[must_use]
    pub fn out_of_sample_windows(&self, total_len: usize) -> Vec<OutOfSampleWindow> {
        let mut windows = Vec::new();
        let mut train_start = 0_usize;
        let mut window_index = 0_usize;

        loop {
            let Some(test_start) = train_start.checked_add(self.config.train) else {
                break;
            };
            let Some(test_end) = test_start
                .checked_add(self.config.test)
                .filter(|end| *end <= total_len)
            else {
                break;
            };
            windows.push(OutOfSampleWindow {
                window_index,
                range: test_start..test_end,
            });
            let Some(next_train_start) = train_start.checked_add(self.config.step) else {
                break;
            };
            let Some(next_window_index) = window_index.checked_add(1) else {
                break;
            };
            train_start = next_train_start;
            window_index = next_window_index;
        }

        windows
    }
}
