use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use axum::Router;
use thiserror::Error;
use tokio::net::TcpListener;

pub const DEFAULT_WEB_PORT: u16 = 8787;

/// Validated listener settings for the read-only Web adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WebServerConfig {
    bind_addr: SocketAddr,
}

impl WebServerConfig {
    #[must_use]
    pub const fn loopback(port: u16) -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        }
    }

    /// Accepts an explicit address while preserving the M2 loopback boundary.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerConfigError::NonLoopbackDenied`] for any wildcard or
    /// non-loopback address.
    pub fn try_new(bind_addr: SocketAddr) -> Result<Self, WebServerConfigError> {
        if !bind_addr.ip().is_loopback() {
            return Err(WebServerConfigError::NonLoopbackDenied);
        }
        Ok(Self { bind_addr })
    }

    #[must_use]
    pub const fn bind_addr(self) -> SocketAddr {
        self.bind_addr
    }

    /// Binds the validated listener.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the loopback address cannot be bound.
    pub async fn bind(self) -> std::io::Result<TcpListener> {
        TcpListener::bind(self.bind_addr).await
    }
}

impl Default for WebServerConfig {
    fn default() -> Self {
        Self::loopback(DEFAULT_WEB_PORT)
    }
}

/// Serves a fully constructed router until the supplied shutdown signal fires.
///
/// # Errors
///
/// Returns an I/O error when the listener fails while accepting connections.
pub async fn serve_with_shutdown<F>(
    listener: TcpListener,
    router: Router,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum WebServerConfigError {
    #[error("M2 Web server binding is restricted to loopback addresses")]
    NonLoopbackDenied,
}
