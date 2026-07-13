use thiserror::Error;

/// Deliberately awkward phrase required before constructing live mode.
pub const LIVE_ACKNOWLEDGEMENT: &str = "I_UNDERSTAND_LIVE_TRADING";

/// Runtime authority. Paper and monitor modes cannot place live orders.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMode {
    Paper,
    Monitor,
    Live(LivePermit),
}

impl ExecutionMode {
    /// Constructs live authority only when the exact acknowledgement is present.
    ///
    /// # Errors
    ///
    /// Returns [`ModeError::AcknowledgementRequired`] unless the caller supplies
    /// [`LIVE_ACKNOWLEDGEMENT`] exactly.
    pub fn live(acknowledgement: Option<&str>) -> Result<Self, ModeError> {
        if acknowledgement == Some(LIVE_ACKNOWLEDGEMENT) {
            Ok(Self::Live(LivePermit(())))
        } else {
            Err(ModeError::AcknowledgementRequired)
        }
    }

    #[must_use]
    pub const fn allows_live_orders(self) -> bool {
        matches!(self, Self::Live(_))
    }
}

/// The private field prevents callers from forging live authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LivePermit(());

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ModeError {
    #[error("live mode requires --acknowledge-risk {LIVE_ACKNOWLEDGEMENT}")]
    AcknowledgementRequired,
}
