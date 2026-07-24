use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CAPABILITY_SCHEMA_VERSION: u16 = 1;
const MAX_CAPABILITIES: usize = 64;
const MAX_CAPABILITY_TEXT_BYTES: usize = 512;

/// Product release stage used by capability consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseStage {
    PaperOnly,
}

impl fmt::Display for ReleaseStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PaperOnly => "paper-only",
        })
    }
}

/// Product area that owns one operator-visible capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityArea {
    Config,
    ControlPlane,
    Exchange,
    History,
    Risk,
    Runtime,
    Strategy,
}

impl fmt::Display for CapabilityArea {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Config => "config",
            Self::ControlPlane => "control-plane",
            Self::Exchange => "exchange",
            Self::History => "history",
            Self::Risk => "risk",
            Self::Runtime => "runtime",
            Self::Strategy => "strategy",
        })
    }
}

/// How much of a capability is currently authorized and implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityLevel {
    Available,
    ReadOnly,
    PaperOnce,
    ValidateOnly,
    ContractOnly,
    Unavailable,
}

impl fmt::Display for CapabilityLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Available => "available",
            Self::ReadOnly => "read-only",
            Self::PaperOnce => "paper-once",
            Self::ValidateOnly => "validate-only",
            Self::ContractOnly => "contract-only",
            Self::Unavailable => "unavailable",
        })
    }
}

/// Environment in which a capability can be evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityEnvironment {
    Offline,
    Paper,
    Testnet,
    Mainnet,
}

impl fmt::Display for CapabilityEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Offline => "offline",
            Self::Paper => "paper",
            Self::Testnet => "testnet",
            Self::Mainnet => "mainnet",
        })
    }
}

/// Highest authority a capability targets, separate from its network environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityAccess {
    Local,
    MarketData,
    PaperTrading,
    TestnetTrading,
    MainnetTrading,
}

impl fmt::Display for CapabilityAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Local => "local",
            Self::MarketData => "market-data",
            Self::PaperTrading => "paper-trading",
            Self::TestnetTrading => "testnet-trading",
            Self::MainnetTrading => "mainnet-trading",
        })
    }
}

/// Network environment and authority target, kept separate from availability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityScope {
    pub environments: Vec<CapabilityEnvironment>,
    pub access: CapabilityAccess,
}

/// One stable capability record consumed by CLI and future control-plane adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    pub id: String,
    pub area: CapabilityArea,
    pub level: CapabilityLevel,
    pub scope: CapabilityScope,
    pub summary: String,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
}

/// Versioned, deterministic snapshot of the product's actual authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub schema_version: u16,
    pub product_version: String,
    pub release_stage: ReleaseStage,
    pub live_trading_enabled: bool,
    pub capabilities: Vec<Capability>,
}

impl CapabilityManifest {
    /// Finds a capability by its stable ID.
    pub fn capability(&self, id: &str) -> Option<&Capability> {
        self.capabilities
            .binary_search_by_key(&id, |capability| capability.id.as_str())
            .ok()
            .map(|index| &self.capabilities[index])
    }

    /// Validates ordering, resource bounds, evidence, and fail-closed live authority.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityError`] when the manifest would be ambiguous,
    /// unbounded, or advertise authority that this release does not possess.
    pub fn validate(&self) -> Result<(), CapabilityError> {
        if self.schema_version != CAPABILITY_SCHEMA_VERSION {
            return Err(CapabilityError::UnsupportedSchema(self.schema_version));
        }
        validate_text("product_version", &self.product_version)?;
        if self.release_stage == ReleaseStage::PaperOnly && self.live_trading_enabled {
            return Err(CapabilityError::InconsistentReleaseStage);
        }
        if self.capabilities.is_empty() || self.capabilities.len() > MAX_CAPABILITIES {
            return Err(CapabilityError::InvalidCount(self.capabilities.len()));
        }

        let mut previous_id: Option<&str> = None;
        for capability in &self.capabilities {
            validate_capability_id(&capability.id)?;
            if previous_id.is_some_and(|previous| previous >= capability.id.as_str()) {
                return Err(CapabilityError::UnstableOrdering(capability.id.clone()));
            }
            previous_id = Some(&capability.id);

            validate_text("summary", &capability.summary)?;
            if capability.evidence.is_empty() {
                return Err(CapabilityError::MissingEvidence(capability.id.clone()));
            }
            for evidence in &capability.evidence {
                validate_text("evidence", evidence)?;
            }
            for blocker in &capability.blockers {
                validate_text("blocker", blocker)?;
            }
            if capability.level == CapabilityLevel::Unavailable && capability.blockers.is_empty() {
                return Err(CapabilityError::MissingBlocker(capability.id.clone()));
            }
            validate_environments(capability)?;
            if !self.live_trading_enabled
                && capability.scope.access == CapabilityAccess::MainnetTrading
                && capability.level != CapabilityLevel::Unavailable
            {
                return Err(CapabilityError::LiveAuthorityAdvertised(
                    capability.id.clone(),
                ));
            }
        }
        Ok(())
    }
}

/// Returns the single operator-facing capability source for this build.
#[must_use]
pub fn current_capability_manifest() -> CapabilityManifest {
    let capabilities = [
        foundation_capabilities(),
        exchange_capabilities(),
        state_and_risk_capabilities(),
        runtime_execution_capabilities(),
        runtime_validation_capabilities(),
        strategy_capabilities(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let manifest = CapabilityManifest {
        schema_version: CAPABILITY_SCHEMA_VERSION,
        product_version: env!("CARGO_PKG_VERSION").to_owned(),
        release_stage: ReleaseStage::PaperOnly,
        live_trading_enabled: false,
        capabilities,
    };
    debug_assert!(manifest.validate().is_ok());
    manifest
}

fn foundation_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "config.compatibility",
            CapabilityArea::Config,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Bounded YAML and JSON configuration parsing with executable classification.",
            &[],
            &["rust/crates/config", "rust/crates/apps/src/command.rs"],
        ),
        capability(
            "control-plane.web",
            CapabilityArea::ControlPlane,
            CapabilityLevel::Unavailable,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Operator-facing HTTP, event-stream, and web control plane.",
            &["No control-plane interface or web adapter is implemented."],
            &["docs/plans/2026-07-24-project-alignment-web-goal-plan.md"],
        ),
    ]
}

fn exchange_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "exchange.binance-public",
            CapabilityArea::Exchange,
            CapabilityLevel::ReadOnly,
            scope(
                &[CapabilityEnvironment::Mainnet],
                CapabilityAccess::MarketData,
            ),
            "One-shot Binance Spot public book-ticker snapshots.",
            &[],
            &[
                "rust/crates/exchange/src/binance.rs",
                "rust/crates/exchange/tests/binance_public_contract.rs",
            ],
        ),
        capability(
            "exchange.binance-testnet-protocol",
            CapabilityArea::Exchange,
            CapabilityLevel::ContractOnly,
            scope(
                &[CapabilityEnvironment::Testnet],
                CapabilityAccess::TestnetTrading,
            ),
            "Injectable-signer Binance Spot and USD-M testnet request contracts.",
            &["Real signing and credentialed testnet lifecycle are not verified."],
            &[
                "rust/crates/exchange/src/binance_testnet.rs",
                "rust/crates/exchange/tests/binance_testnet_protocol.rs",
            ],
        ),
        capability(
            "exchange.hyperliquid-testnet-protocol",
            CapabilityArea::Exchange,
            CapabilityLevel::ContractOnly,
            scope(
                &[CapabilityEnvironment::Testnet],
                CapabilityAccess::TestnetTrading,
            ),
            "Injectable-signer Hyperliquid Spot and perpetual testnet request contracts.",
            &["Real signing and credentialed testnet lifecycle are not verified."],
            &[
                "rust/crates/exchange/src/hyperliquid_testnet.rs",
                "rust/crates/exchange/tests/hyperliquid_testnet_protocol.rs",
            ],
        ),
        capability(
            "exchange.paper",
            CapabilityArea::Exchange,
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Deterministic process-local order, fill, reservation, cancel, and reconcile model.",
            &[],
            &[
                "rust/crates/exchange/src/paper.rs",
                "rust/crates/exchange/tests/paper_exchange_contract.rs",
            ],
        ),
        capability(
            "exchange.private-live",
            CapabilityArea::Exchange,
            CapabilityLevel::Unavailable,
            scope(
                &[CapabilityEnvironment::Mainnet],
                CapabilityAccess::MainnetTrading,
            ),
            "Authenticated private exchange market, account, and trading adapters.",
            &["No private adapter has passed signing, testnet, account, and reconcile gates."],
            &["rust/crates/exchange/src/unsupported.rs"],
        ),
    ]
}

fn state_and_risk_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "history.execution-jsonl",
            CapabilityArea::History,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Paper], CapabilityAccess::Local),
            "Bounded planned, completed, partial, and incomplete execution JSONL records.",
            &[],
            &[
                "rust/crates/runtime/src/history.rs",
                "rust/crates/runtime/tests/execution_contract.rs",
            ],
        ),
        capability(
            "risk.account-authority",
            CapabilityArea::Risk,
            CapabilityLevel::Unavailable,
            scope(
                &[
                    CapabilityEnvironment::Paper,
                    CapabilityEnvironment::Testnet,
                    CapabilityEnvironment::Mainnet,
                ],
                CapabilityAccess::Local,
            ),
            "Authoritative account equity, margin, global exposure, and pending-order reservations.",
            &["Current risk is product-scoped and does not own an authoritative account ledger."],
            &[
                "rust/crates/strategy/src/risk.rs",
                "rust/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md",
            ],
        ),
    ]
}

fn runtime_execution_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "runtime.arbitrage",
            CapabilityArea::Runtime,
            CapabilityLevel::PaperOnce,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Two-leg segmented arbitrage evaluation and paper execution from explicit snapshots.",
            &[],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/runtime/tests/arbitrage_paper_slice.rs",
            ],
        ),
        capability(
            "runtime.continuous",
            CapabilityArea::Runtime,
            CapabilityLevel::Unavailable,
            scope(
                &[
                    CapabilityEnvironment::Paper,
                    CapabilityEnvironment::Testnet,
                    CapabilityEnvironment::Mainnet,
                ],
                CapabilityAccess::MainnetTrading,
            ),
            "Supervised continuous strategy lifecycle with cancellation and restart recovery.",
            &["No continuous runtime supervisor is implemented."],
            &["rust/crates/apps/src/command.rs", "rust/README.md"],
        ),
        capability(
            "runtime.grid",
            CapabilityArea::Runtime,
            CapabilityLevel::PaperOnce,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Fixed-snapshot grid planning and resting-order paper placement.",
            &[],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/tests/command_smoke.rs",
            ],
        ),
        capability(
            "runtime.live",
            CapabilityArea::Runtime,
            CapabilityLevel::Unavailable,
            scope(
                &[CapabilityEnvironment::Mainnet],
                CapabilityAccess::MainnetTrading,
            ),
            "Mainnet order authority.",
            &[
                "Mandatory account risk, private adapters, reconciliation, and recovery gates are incomplete.",
            ],
            &[
                "rust/crates/runtime/src/mode.rs",
                "rust/crates/runtime/src/execution.rs",
            ],
        ),
    ]
}

fn runtime_validation_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "runtime.monitor",
            CapabilityArea::Runtime,
            CapabilityLevel::ValidateOnly,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Arbitrage monitor configuration loading and validation.",
            &["Continuous monitoring and event production are not implemented."],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/config/src/monitor.rs",
            ],
        ),
        capability(
            "runtime.price-alert",
            CapabilityArea::Runtime,
            CapabilityLevel::ValidateOnly,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Price-alert configuration loading and validation.",
            &[
                "Continuous evaluation, cooldown persistence, and notifications are not implemented.",
            ],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/strategy/src/alert.rs",
            ],
        ),
        capability(
            "runtime.scanner",
            CapabilityArea::Runtime,
            CapabilityLevel::ValidateOnly,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Bounded scanner configuration file access checks.",
            &["Scanner schema validation and runtime ranking are not implemented."],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/strategy/src/virtual_grid.rs",
            ],
        ),
        capability(
            "runtime.volume-maker",
            CapabilityArea::Runtime,
            CapabilityLevel::ValidateOnly,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Volume-maker configuration and emergency-stop validation.",
            &["Continuous paper or live volume-maker execution is not implemented."],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/strategy/src/volume_maker.rs",
            ],
        ),
    ]
}

fn strategy_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "strategy.arbitrage",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic segmented arbitrage decisions without I/O.",
            &[],
            &[
                "rust/crates/strategy/src/arbitrage.rs",
                "rust/crates/strategy/tests/segmented_arbitrage.rs",
            ],
        ),
        capability(
            "strategy.grid",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic fixed-grid planning without I/O.",
            &[],
            &[
                "rust/crates/strategy/src/grid.rs",
                "rust/crates/strategy/tests/grid_planner.rs",
            ],
        ),
        capability(
            "strategy.price-alert",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic price threshold evaluation without I/O.",
            &[],
            &[
                "rust/crates/strategy/src/alert.rs",
                "rust/crates/strategy/tests/price_alert.rs",
            ],
        ),
        capability(
            "strategy.scanner",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic virtual-grid simulation and volatility scoring.",
            &[],
            &[
                "rust/crates/strategy/src/virtual_grid.rs",
                "rust/crates/strategy/tests/virtual_grid_golden.rs",
            ],
        ),
        capability(
            "strategy.volume-maker",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic maker-volume decisions without I/O.",
            &[],
            &[
                "rust/crates/strategy/src/volume_maker.rs",
                "rust/crates/strategy/tests/volume_maker.rs",
            ],
        ),
    ]
}

fn capability(
    id: &str,
    area: CapabilityArea,
    level: CapabilityLevel,
    scope: CapabilityScope,
    summary: &str,
    blockers: &[&str],
    evidence: &[&str],
) -> Capability {
    Capability {
        id: id.to_owned(),
        area,
        level,
        scope,
        summary: summary.to_owned(),
        blockers: blockers.iter().map(|value| (*value).to_owned()).collect(),
        evidence: evidence.iter().map(|value| (*value).to_owned()).collect(),
    }
}

fn scope(environments: &[CapabilityEnvironment], access: CapabilityAccess) -> CapabilityScope {
    CapabilityScope {
        environments: environments.to_vec(),
        access,
    }
}

fn validate_capability_id(id: &str) -> Result<(), CapabilityError> {
    let valid = !id.is_empty()
        && id.len() <= 96
        && id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(CapabilityError::InvalidId(id.to_owned()))
    }
}

fn validate_text(field: &'static str, value: &str) -> Result<(), CapabilityError> {
    if value.trim().is_empty() || value.len() > MAX_CAPABILITY_TEXT_BYTES {
        Err(CapabilityError::InvalidText { field })
    } else {
        Ok(())
    }
}

fn validate_environments(capability: &Capability) -> Result<(), CapabilityError> {
    if capability.scope.environments.is_empty()
        || capability
            .scope
            .environments
            .windows(2)
            .any(|window| window[0] >= window[1])
    {
        return Err(CapabilityError::InvalidEnvironments(capability.id.clone()));
    }
    Ok(())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CapabilityError {
    #[error("unsupported capability schema version {0}")]
    UnsupportedSchema(u16),
    #[error("paper-only releases cannot enable live trading")]
    InconsistentReleaseStage,
    #[error("capability count {0} must be between 1 and {MAX_CAPABILITIES}")]
    InvalidCount(usize),
    #[error("capability ID {0:?} is invalid")]
    InvalidId(String),
    #[error("capability IDs must be unique and sorted; found {0:?} out of order")]
    UnstableOrdering(String),
    #[error("capability field {field} is empty or exceeds {MAX_CAPABILITY_TEXT_BYTES} bytes")]
    InvalidText { field: &'static str },
    #[error("capability {0} must contain at least one evidence path")]
    MissingEvidence(String),
    #[error("unavailable capability {0} must name at least one blocker")]
    MissingBlocker(String),
    #[error("capability {0} environments must be non-empty, unique, and sorted")]
    InvalidEnvironments(String),
    #[error("capability {0} advertises live authority while live is disabled")]
    LiveAuthorityAdvertised(String),
}
