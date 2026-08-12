use std::net::IpAddr;

use reqwest::Url;

use crate::ExchangeError;

const BINANCE_SPOT_TESTNET_HOST: &str = "testnet.binance.vision";
const BINANCE_USDM_TESTNET_HOST: &str = "demo-fapi.binance.com";
const BINANCE_SPOT_TESTNET_STREAM_HOST: &str = "stream.testnet.binance.vision";
const BINANCE_SPOT_TESTNET_WS_API_HOST: &str = "ws-api.testnet.binance.vision";
const HYPERLIQUID_TESTNET_HOST: &str = "api.hyperliquid-testnet.xyz";
const HYPERLIQUID_MAINNET_HOST: &str = "api.hyperliquid.xyz";

/// Binance product families with distinct REST hosts and route namespaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinanceProduct {
    Spot,
    /// Binance USDⓈ-M futures (USDT/USDC-margined perpetuals).
    UsdM,
}

/// Testnet-only Binance endpoint selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceTestnetEndpoints {
    spot: Url,
    usdm: Url,
}

impl BinanceTestnetEndpoints {
    /// Returns the official Binance Spot testnet and USDⓈ-M demo endpoints.
    pub fn official() -> Self {
        match Self::try_official(
            "https://testnet.binance.vision",
            "https://demo-fapi.binance.com",
        ) {
            Ok(endpoints) => endpoints,
            Err(_) => unreachable!("hard-coded Binance testnet endpoints must be valid"),
        }
    }

    /// Validates caller-supplied URLs against the exact official testnet hosts.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for mainnet hosts, mixed
    /// products, credentials, query strings, fragments, or non-root paths.
    pub fn try_official(spot: &str, usdm: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            spot: official_root(spot, BINANCE_SPOT_TESTNET_HOST, "Binance Spot testnet")?,
            usdm: official_root(usdm, BINANCE_USDM_TESTNET_HOST, "Binance USD-M testnet")?,
        })
    }

    /// Builds an explicitly offline endpoint set.
    ///
    /// Literal loopback IPs are required; hostnames are rejected to prevent DNS
    /// rebinding or an accidental non-local target.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] unless both URLs are root
    /// `http`/`https` URLs on literal loopback IP addresses.
    pub fn loopback(spot: &str, usdm: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            spot: loopback_root(spot, "Binance Spot offline endpoint")?,
            usdm: loopback_root(usdm, "Binance USD-M offline endpoint")?,
        })
    }

    /// Resolves a fixed API path without allowing origin escape.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for a relative, absolute-URL,
    /// traversal, query-bearing, or fragment-bearing path.
    pub fn rest_url(&self, product: BinanceProduct, path: &str) -> Result<Url, ExchangeError> {
        let base = match product {
            BinanceProduct::Spot => &self.spot,
            BinanceProduct::UsdM => &self.usdm,
        };
        fixed_api_url(base, path)
    }
}

/// Testnet-only Binance Spot public websocket stream endpoint selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceSpotMarketStreamEndpoint {
    base: Url,
}

impl BinanceSpotMarketStreamEndpoint {
    #[must_use]
    pub fn official() -> Self {
        match Self::try_official("wss://stream.testnet.binance.vision") {
            Ok(endpoint) => endpoint,
            Err(_) => unreachable!("hard-coded Binance Spot stream endpoint must be valid"),
        }
    }

    /// Validates a caller-supplied Binance Spot public-stream websocket origin.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] unless `base` is the exact
    /// official testnet `wss` origin.
    pub fn try_official(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: official_websocket_root(
                base,
                BINANCE_SPOT_TESTNET_STREAM_HOST,
                "Binance Spot stream testnet",
            )?,
        })
    }

    /// Builds an explicitly offline public-stream websocket origin.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] unless `base` is a root
    /// `ws`/`wss` URL on a literal loopback IP address.
    pub fn loopback(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: loopback_websocket_root(base, "Binance Spot stream offline endpoint")?,
        })
    }

    /// Resolves one fixed raw-stream path on the selected websocket origin.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] when `stream_name` would
    /// escape the selected origin or introduce query/fragment components.
    pub fn stream_url(&self, stream_name: &str) -> Result<Url, ExchangeError> {
        fixed_websocket_path(&self.base, &format!("/ws/{stream_name}"))
    }
}

/// Testnet-only Binance Spot websocket API endpoint selection for authenticated
/// user-data subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceSpotUserDataStreamEndpoint {
    base: Url,
}

impl BinanceSpotUserDataStreamEndpoint {
    #[must_use]
    pub fn official() -> Self {
        match Self::try_official("wss://ws-api.testnet.binance.vision") {
            Ok(endpoint) => endpoint,
            Err(_) => unreachable!("hard-coded Binance Spot WS API endpoint must be valid"),
        }
    }

    /// Validates a caller-supplied Binance Spot websocket-API origin.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] unless `base` is the exact
    /// official testnet `wss` origin.
    pub fn try_official(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: official_websocket_root(
                base,
                BINANCE_SPOT_TESTNET_WS_API_HOST,
                "Binance Spot WS API testnet",
            )?,
        })
    }

    /// Builds an explicitly offline websocket-API origin.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] unless `base` is a root
    /// `ws`/`wss` URL on a literal loopback IP address.
    pub fn loopback(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: loopback_websocket_root(base, "Binance Spot WS API offline endpoint")?,
        })
    }

    /// Resolves the fixed Spot websocket-API route on the selected origin.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] if the selected origin is not
    /// structurally safe for the fixed `/ws-api/v3` path.
    pub fn websocket_url(&self) -> Result<Url, ExchangeError> {
        fixed_websocket_path(&self.base, "/ws-api/v3")
    }
}

/// Testnet-only Hyperliquid endpoint selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidTestnetEndpoint {
    base: Url,
}

impl HyperliquidTestnetEndpoint {
    /// Returns the official Hyperliquid testnet HTTP endpoint.
    pub fn official() -> Self {
        match Self::try_official("https://api.hyperliquid-testnet.xyz") {
            Ok(endpoint) => endpoint,
            Err(_) => unreachable!("hard-coded Hyperliquid testnet endpoint must be valid"),
        }
    }

    /// Validates a URL against the exact official Hyperliquid testnet host.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for any non-testnet or
    /// structurally unsafe endpoint.
    pub fn try_official(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: official_root(base, HYPERLIQUID_TESTNET_HOST, "Hyperliquid testnet")?,
        })
    }

    /// Builds an explicitly offline endpoint on a literal loopback address.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for hostnames, non-loopback
    /// addresses, credentials, query strings, fragments, or non-root paths.
    pub fn loopback(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: loopback_root(base, "Hyperliquid offline endpoint")?,
        })
    }

    /// Resolves a fixed API path without allowing origin escape.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for unsafe paths.
    pub fn rest_url(&self, path: &str) -> Result<Url, ExchangeError> {
        fixed_api_url(&self.base, path)
    }
}

/// Public, credential-free Hyperliquid market-data endpoint selection.
///
/// This whitelist admits only the official mainnet info origin or an explicit
/// literal-loopback origin for deterministic offline contract tests. It never
/// selects the authenticated `/exchange` surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidPublicEndpoint {
    base: Url,
}

impl HyperliquidPublicEndpoint {
    /// Returns the official Hyperliquid mainnet public-info endpoint.
    #[must_use]
    pub fn official() -> Self {
        match Self::try_official("https://api.hyperliquid.xyz") {
            Ok(endpoint) => endpoint,
            Err(_) => unreachable!("hard-coded Hyperliquid mainnet endpoint must be valid"),
        }
    }

    /// Validates a URL against the exact official Hyperliquid mainnet host.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for any non-official or
    /// structurally unsafe endpoint.
    pub fn try_official(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: official_root(
                base,
                HYPERLIQUID_MAINNET_HOST,
                "Hyperliquid public endpoint",
            )?,
        })
    }

    /// Builds an explicitly offline endpoint on a literal loopback address.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for hostnames, non-loopback
    /// addresses, credentials, query strings, fragments, or non-root paths.
    pub fn loopback(base: &str) -> Result<Self, ExchangeError> {
        Ok(Self {
            base: loopback_root(base, "Hyperliquid public offline endpoint")?,
        })
    }

    /// Resolves a fixed API path without allowing origin escape.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::InvalidRequest`] for unsafe paths.
    pub fn rest_url(&self, path: &str) -> Result<Url, ExchangeError> {
        fixed_api_url(&self.base, path)
    }
}

fn official_root(value: &str, expected_host: &str, label: &str) -> Result<Url, ExchangeError> {
    let url = parse_root(value, label)?;
    if url.scheme() != "https"
        || url.port().is_some()
        || !url
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case(expected_host))
    {
        return Err(ExchangeError::invalid(format!(
            "{label} must use https://{expected_host}"
        )));
    }
    Ok(url)
}

fn official_websocket_root(
    value: &str,
    expected_host: &str,
    label: &str,
) -> Result<Url, ExchangeError> {
    let url = parse_root(value, label)?;
    if url.scheme() != "wss"
        || url.port().is_some()
        || !url
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case(expected_host))
    {
        return Err(ExchangeError::invalid(format!(
            "{label} must use wss://{expected_host}"
        )));
    }
    Ok(url)
}

fn loopback_root(value: &str, label: &str) -> Result<Url, ExchangeError> {
    let url = parse_root(value, label)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ExchangeError::invalid(format!(
            "{label} must use http or https"
        )));
    }
    let address = url
        .host_str()
        .and_then(|host| {
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .ok()
        })
        .ok_or_else(|| {
            ExchangeError::invalid(format!("{label} must use a literal loopback IP address"))
        })?;
    if !address.is_loopback() {
        return Err(ExchangeError::invalid(format!(
            "{label} must use a loopback IP address"
        )));
    }
    Ok(url)
}

fn loopback_websocket_root(value: &str, label: &str) -> Result<Url, ExchangeError> {
    let url = parse_root(value, label)?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(ExchangeError::invalid(format!(
            "{label} must use ws or wss"
        )));
    }
    let address = url
        .host_str()
        .and_then(|host| {
            host.trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .ok()
        })
        .ok_or_else(|| {
            ExchangeError::invalid(format!("{label} must use a literal loopback IP address"))
        })?;
    if !address.is_loopback() {
        return Err(ExchangeError::invalid(format!(
            "{label} must use a loopback IP address"
        )));
    }
    Ok(url)
}

fn parse_root(value: &str, label: &str) -> Result<Url, ExchangeError> {
    let url = Url::parse(value).map_err(|error| {
        ExchangeError::invalid(format!("{label} is not a valid absolute URL: {error}"))
    })?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(ExchangeError::invalid(format!(
            "{label} must be an origin-only URL without credentials, path, query, or fragment"
        )));
    }
    Ok(url)
}

fn fixed_api_url(base: &Url, path: &str) -> Result<Url, ExchangeError> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['?', '#', '\\'])
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
        || !path.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '-')
        })
    {
        return Err(ExchangeError::invalid(
            "exchange API path must be a fixed absolute path on the selected origin",
        ));
    }
    let mut url = base.clone();
    url.set_path(path);
    Ok(url)
}

fn fixed_websocket_path(base: &Url, path: &str) -> Result<Url, ExchangeError> {
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['?', '#', '\\'])
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
        || !path.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '-' | '@' | '.')
        })
    {
        return Err(ExchangeError::invalid(
            "exchange websocket path must stay on the selected origin",
        ));
    }
    let mut url = base.clone();
    url.set_path(path);
    Ok(url)
}
