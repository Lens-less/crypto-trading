use std::ops::Range;

use crate::BacktestError;

/// Walk-forward sizing parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkForwardConfig {
    pub train_size: usize,
    pub test_size: usize,
    pub step_size: usize,
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
            train_size,
            test_size,
            step_size,
        })
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

        while train_start
            .checked_add(self.config.train_size)
            .and_then(|value| value.checked_add(self.config.test_size))
            .is_some_and(|end| end <= total_len)
        {
            let test_start = train_start + self.config.train_size;
            let test_end = test_start + self.config.test_size;
            windows.push(OutOfSampleWindow {
                window_index,
                range: test_start..test_end,
            });
            train_start += self.config.step_size;
            window_index += 1;
        }

        windows
    }
}
