use std::{future::Future, io, pin::Pin};

pub type ShutdownSignalFuture =
    Pin<Box<dyn Future<Output = Result<ShutdownSignal, ShutdownSignalError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSignal {
    CtrlC,
    #[cfg(unix)]
    Sigterm,
    #[cfg(windows)]
    CtrlBreak,
    #[cfg(windows)]
    CtrlClose,
    #[cfg(windows)]
    CtrlShutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSignalStage {
    Register,
    Receive,
}

#[derive(Debug)]
pub struct ShutdownSignalError {
    signal: &'static str,
    stage: ShutdownSignalStage,
    source: io::Error,
}

impl ShutdownSignalError {
    pub(crate) fn register(signal: &'static str, source: io::Error) -> Self {
        Self {
            signal,
            stage: ShutdownSignalStage::Register,
            source,
        }
    }

    pub(crate) fn receive(signal: &'static str, source: io::Error) -> Self {
        Self {
            signal,
            stage: ShutdownSignalStage::Receive,
            source,
        }
    }

    /// Returns the operating-system signal whose handling failed.
    #[must_use]
    pub const fn signal(&self) -> &'static str {
        self.signal
    }

    /// Returns whether handler registration or signal receipt failed.
    #[must_use]
    pub const fn stage(&self) -> ShutdownSignalStage {
        self.stage
    }
}

impl std::fmt::Display for ShutdownSignalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let action = match self.stage {
            ShutdownSignalStage::Register => "registration",
            ShutdownSignalStage::Receive => "wait",
        };
        write!(
            formatter,
            "shutdown signal {action} failed for {}: {}",
            self.signal, self.source
        )
    }
}

impl std::error::Error for ShutdownSignalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Installs handlers for the graceful-shutdown signals supported by this platform.
///
/// # Errors
///
/// Returns an error when an operating-system signal handler cannot be registered.
pub fn install_shutdown_signal() -> Result<ShutdownSignalFuture, ShutdownSignalError> {
    platform_shutdown_signal()
}

#[cfg(unix)]
fn platform_shutdown_signal() -> Result<ShutdownSignalFuture, ShutdownSignalError> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|source| ShutdownSignalError::register("SIGTERM", source))?;
    Ok(Box::pin(async move {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result
                    .map(|()| ShutdownSignal::CtrlC)
                    .map_err(|source| ShutdownSignalError::receive("Ctrl-C", source))
            }
            result = sigterm.recv() => result
                .map(|()| ShutdownSignal::Sigterm)
                .ok_or_else(|| ShutdownSignalError::receive("SIGTERM", io::Error::other("signal stream ended unexpectedly"))),
        }
    }))
}

#[cfg(windows)]
fn platform_shutdown_signal() -> Result<ShutdownSignalFuture, ShutdownSignalError> {
    use tokio::signal::windows;

    let mut ctrl_c =
        windows::ctrl_c().map_err(|source| ShutdownSignalError::register("Ctrl-C", source))?;
    let mut ctrl_break = windows::ctrl_break()
        .map_err(|source| ShutdownSignalError::register("Ctrl-Break", source))?;
    let mut ctrl_close = windows::ctrl_close()
        .map_err(|source| ShutdownSignalError::register("Ctrl-Close", source))?;
    let mut ctrl_shutdown = windows::ctrl_shutdown()
        .map_err(|source| ShutdownSignalError::register("Ctrl-Shutdown", source))?;
    Ok(Box::pin(async move {
        tokio::select! {
            result = ctrl_c.recv() => result
                .map(|()| ShutdownSignal::CtrlC)
                .ok_or_else(|| ShutdownSignalError::receive("Ctrl-C", io::Error::other("signal stream ended unexpectedly"))),
            result = ctrl_break.recv() => result
                .map(|()| ShutdownSignal::CtrlBreak)
                .ok_or_else(|| ShutdownSignalError::receive("Ctrl-Break", io::Error::other("signal stream ended unexpectedly"))),
            result = ctrl_close.recv() => result
                .map(|()| ShutdownSignal::CtrlClose)
                .ok_or_else(|| ShutdownSignalError::receive("Ctrl-Close", io::Error::other("signal stream ended unexpectedly"))),
            result = ctrl_shutdown.recv() => result
                .map(|()| ShutdownSignal::CtrlShutdown)
                .ok_or_else(|| ShutdownSignalError::receive("Ctrl-Shutdown", io::Error::other("signal stream ended unexpectedly"))),
        }
    }))
}

#[cfg(not(any(unix, windows)))]
fn platform_shutdown_signal() -> Result<ShutdownSignalFuture, ShutdownSignalError> {
    Ok(Box::pin(async move {
        tokio::signal::ctrl_c()
            .await
            .map(|()| ShutdownSignal::CtrlC)
            .map_err(|source| ShutdownSignalError::receive("Ctrl-C", source))
    }))
}

#[cfg(test)]
mod tests {
    use super::{ShutdownSignalError, ShutdownSignalStage};
    use std::io;

    #[test]
    fn signal_error_exposes_typed_identity_and_stage() {
        let error = ShutdownSignalError::register(
            "SIGTERM",
            io::Error::from(io::ErrorKind::PermissionDenied),
        );

        assert_eq!(error.signal(), "SIGTERM");
        assert_eq!(error.stage(), ShutdownSignalStage::Register);
        assert_eq!(error.source.kind(), io::ErrorKind::PermissionDenied);
    }
}
