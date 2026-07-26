use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CAPABILITY_SCHEMA_VERSION: u16 = 2;
const MAX_CAPABILITIES: usize = 64;
const MAX_ADAPTERS: usize = 16;
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

/// One column in the operator-facing exchange adapter support matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AdapterFacet {
    PublicData,
    TestnetProtocol,
    Authenticated,
    Reconcile,
    Live,
}

impl fmt::Display for AdapterFacet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PublicData => "public-data",
            Self::TestnetProtocol => "testnet-protocol",
            Self::Authenticated => "authenticated",
            Self::Reconcile => "reconcile",
            Self::Live => "live",
        })
    }
}

/// Strength of the evidence behind one adapter facet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AdapterSupportLevel {
    Implemented,
    ProtocolOnly,
    RequestOnly,
    ConfigOnly,
    Unavailable,
    NotApplicable,
}

impl fmt::Display for AdapterSupportLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Implemented => "implemented",
            Self::ProtocolOnly => "protocol-only",
            Self::RequestOnly => "request-only",
            Self::ConfigOnly => "config-only",
            Self::Unavailable => "unavailable",
            Self::NotApplicable => "not-applicable",
        })
    }
}

/// Evidence and limitations for one cell in the adapter support matrix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterFacetSupport {
    pub level: AdapterSupportLevel,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
}

/// One exchange row consumed by CLI and the future Integrations page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterSupport {
    pub id: String,
    pub name: String,
    pub public_data: AdapterFacetSupport,
    pub testnet_protocol: AdapterFacetSupport,
    pub authenticated: AdapterFacetSupport,
    pub reconcile: AdapterFacetSupport,
    pub live: AdapterFacetSupport,
}

impl AdapterSupport {
    /// Returns the evidence cell for one matrix facet.
    #[must_use]
    pub fn facet(&self, facet: AdapterFacet) -> &AdapterFacetSupport {
        match facet {
            AdapterFacet::PublicData => &self.public_data,
            AdapterFacet::TestnetProtocol => &self.testnet_protocol,
            AdapterFacet::Authenticated => &self.authenticated,
            AdapterFacet::Reconcile => &self.reconcile,
            AdapterFacet::Live => &self.live,
        }
    }

    fn facets(&self) -> [(AdapterFacet, &AdapterFacetSupport); 5] {
        [
            (AdapterFacet::PublicData, &self.public_data),
            (AdapterFacet::TestnetProtocol, &self.testnet_protocol),
            (AdapterFacet::Authenticated, &self.authenticated),
            (AdapterFacet::Reconcile, &self.reconcile),
            (AdapterFacet::Live, &self.live),
        ]
    }
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
    pub adapters: Vec<AdapterSupport>,
    pub capabilities: Vec<Capability>,
}

impl CapabilityManifest {
    /// Finds an adapter row by its stable ID.
    pub fn adapter(&self, id: &str) -> Option<&AdapterSupport> {
        self.adapters
            .binary_search_by_key(&id, |adapter| adapter.id.as_str())
            .ok()
            .map(|index| &self.adapters[index])
    }

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
        validate_adapters(self)?;

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
        validate_adapter_capability_alignment(self)
    }
}

/// Returns the single operator-facing capability source for this build.
#[must_use]
pub fn current_capability_manifest() -> CapabilityManifest {
    let adapters = adapter_support_matrix();
    let capabilities = [
        foundation_capabilities(),
        exchange_capabilities(&adapters),
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
        adapters,
        capabilities,
    };
    debug_assert!(manifest.validate().is_ok());
    manifest
}

fn adapter_support_matrix() -> Vec<AdapterSupport> {
    vec![
        config_only_adapter(
            "backpack",
            "Backpack",
            "rust/config/exchanges/backpack_config.yaml",
        ),
        binance_adapter(),
        config_only_adapter("edgex", "EdgeX", "rust/config/exchanges/edgex_config.yaml"),
        config_only_adapter("grvt", "GRVT", "rust/config/exchanges/grvt_config.yaml"),
        hyperliquid_adapter(),
        config_only_adapter(
            "lighter",
            "Lighter",
            "rust/config/exchanges/lighter_config.yaml",
        ),
        legacy_only_adapter("okx", "OKX"),
        paper_adapter(),
        config_only_adapter(
            "paradex",
            "Paradex",
            "rust/config/exchanges/paradex_config.yaml",
        ),
        legacy_only_adapter("variational", "Variational"),
    ]
}

fn binance_adapter() -> AdapterSupport {
    let execution_evidence = [
        "rust/crates/exchange/src/binance_testnet.rs",
        "rust/crates/exchange/src/binance_testnet_exchange.rs",
        "rust/crates/exchange/tests/binance_testnet_protocol.rs",
        "rust/crates/exchange/tests/binance_testnet_exchange_contract.rs",
        "rust/crates/apps/src/command.rs",
        "rust/crates/apps/tests/command_smoke.rs",
        "rust/crates/apps/src/testnet_lifecycle.rs",
        "rust/crates/apps/tests/testnet_lifecycle_cli_contract.rs",
        "rust/crates/apps/src/testnet_reconciliation.rs",
        "rust/crates/apps/tests/testnet_reconciliation_contract.rs",
        "rust/crates/apps/tests/testnet_reconciliation_cli_contract.rs",
        "rust/crates/apps/src/testnet_soak.rs",
        "rust/crates/apps/tests/testnet_soak_contract.rs",
        "rust/crates/apps/tests/testnet_soak_cli_contract.rs",
    ];
    let credentialed_evidence_blocker = [
        "The executable adapter, durable order-lifecycle owner, signed account reconciliation gate, and read-only soak harness have deterministic coverage, but credentialed Binance Testnet lifecycle/reconciliation evidence and a completed 24-hour soak are not checked in.",
    ];
    AdapterSupport {
        id: "binance".to_owned(),
        name: "Binance".to_owned(),
        public_data: adapter_facet(
            AdapterSupportLevel::Implemented,
            &[],
            &[
                "rust/crates/exchange/src/binance.rs",
                "rust/crates/exchange/tests/binance_public_contract.rs",
            ],
        ),
        testnet_protocol: adapter_facet(
            AdapterSupportLevel::Implemented,
            &credentialed_evidence_blocker,
            &execution_evidence,
        ),
        authenticated: adapter_facet(
            AdapterSupportLevel::Implemented,
            &credentialed_evidence_blocker,
            &execution_evidence,
        ),
        reconcile: adapter_facet(
            AdapterSupportLevel::Implemented,
            &credentialed_evidence_blocker,
            &execution_evidence,
        ),
        live: external_live_unavailable(),
    }
}

fn hyperliquid_adapter() -> AdapterSupport {
    let protocol_evidence = [
        "rust/crates/exchange/src/hyperliquid_testnet.rs",
        "rust/crates/exchange/tests/hyperliquid_testnet_protocol.rs",
    ];
    AdapterSupport {
        id: "hyperliquid".to_owned(),
        name: "Hyperliquid".to_owned(),
        public_data: adapter_facet(
            AdapterSupportLevel::Unavailable,
            &["No Rust market-data adapter emits Hyperliquid domain snapshots."],
            &[
                "rust/crates/exchange/src/hyperliquid_testnet.rs",
                "docs/internal/research/upstream-repository-alignment.md",
            ],
        ),
        testnet_protocol: adapter_facet(
            AdapterSupportLevel::ProtocolOnly,
            &[
                "Only deterministic request/response contracts are verified; no credentialed testnet lifecycle has run.",
            ],
            &protocol_evidence,
        ),
        authenticated: adapter_facet(
            AdapterSupportLevel::ProtocolOnly,
            &[
                "The injectable signer seam is tested, but real credentials and official signing vectors are not verified.",
            ],
            &protocol_evidence,
        ),
        reconcile: adapter_facet(
            AdapterSupportLevel::RequestOnly,
            &[
                "Account request routes are covered, but response parsing and authoritative reconciliation receipts are not implemented.",
            ],
            &protocol_evidence,
        ),
        live: external_live_unavailable(),
    }
}

fn paper_adapter() -> AdapterSupport {
    let paper_evidence = ["rust/crates/exchange/src/paper.rs"];
    AdapterSupport {
        id: "paper".to_owned(),
        name: "PaperExchange".to_owned(),
        public_data: adapter_facet(
            AdapterSupportLevel::NotApplicable,
            &["PaperExchange consumes explicitly injected market snapshots."],
            &paper_evidence,
        ),
        testnet_protocol: adapter_facet(
            AdapterSupportLevel::NotApplicable,
            &["PaperExchange has no remote protocol or testnet endpoint."],
            &paper_evidence,
        ),
        authenticated: adapter_facet(
            AdapterSupportLevel::NotApplicable,
            &["PaperExchange is process-local and does not authenticate to a venue."],
            &paper_evidence,
        ),
        reconcile: adapter_facet(
            AdapterSupportLevel::Implemented,
            &[],
            &[
                "rust/crates/exchange/src/paper.rs",
                "rust/crates/exchange/tests/paper_exchange_contract.rs",
            ],
        ),
        live: adapter_facet(
            AdapterSupportLevel::NotApplicable,
            &["PaperExchange cannot create mainnet authority."],
            &paper_evidence,
        ),
    }
}

fn config_only_adapter(id: &str, name: &str, config_path: &str) -> AdapterSupport {
    let config_evidence = [
        "rust/crates/config/src/auth.rs",
        config_path,
        "rust/crates/config/tests/config_compatibility.rs",
    ];
    let unavailable_evidence = [
        config_path,
        "docs/internal/research/upstream-repository-alignment.md",
    ];
    AdapterSupport {
        id: id.to_owned(),
        name: name.to_owned(),
        public_data: adapter_facet(
            AdapterSupportLevel::ConfigOnly,
            &["Rust can parse venue configuration, but no market-data adapter is implemented."],
            &config_evidence,
        ),
        testnet_protocol: adapter_facet(
            AdapterSupportLevel::Unavailable,
            &["No Rust testnet request/response protocol is implemented."],
            &unavailable_evidence,
        ),
        authenticated: adapter_facet(
            AdapterSupportLevel::ConfigOnly,
            &["Credential fields can be loaded and redacted, but no private API consumes them."],
            &config_evidence,
        ),
        reconcile: adapter_facet(
            AdapterSupportLevel::Unavailable,
            &["No Rust open-order, position, or balance reconciliation is implemented."],
            &unavailable_evidence,
        ),
        live: external_live_unavailable(),
    }
}

fn legacy_only_adapter(id: &str, name: &str) -> AdapterSupport {
    let evidence = [
        "docs/internal/research/upstream-repository-alignment.md",
        "docs/internal/plans/2026-07-24-project-alignment-web-goal-plan.md",
    ];
    let unavailable = || {
        adapter_facet(
            AdapterSupportLevel::Unavailable,
            &[
                "Only the frozen Python adapter exists; no current Rust adapter or configuration contract is implemented.",
            ],
            &evidence,
        )
    };
    AdapterSupport {
        id: id.to_owned(),
        name: name.to_owned(),
        public_data: unavailable(),
        testnet_protocol: unavailable(),
        authenticated: unavailable(),
        reconcile: unavailable(),
        live: external_live_unavailable(),
    }
}

fn external_live_unavailable() -> AdapterFacetSupport {
    adapter_facet(
        AdapterSupportLevel::Unavailable,
        &[
            "Mainnet authority is disabled until signing, testnet, account-risk, reconciliation, and recovery gates pass.",
        ],
        &[
            "rust/crates/runtime/src/mode.rs",
            "rust/crates/runtime/tests/execution_contract.rs",
            "rust/crates/exchange/src/unsupported.rs",
            "rust/crates/apps/tests/command_smoke.rs",
        ],
    )
}

fn adapter_facet(
    level: AdapterSupportLevel,
    blockers: &[&str],
    evidence: &[&str],
) -> AdapterFacetSupport {
    AdapterFacetSupport {
        level,
        blockers: blockers.iter().map(|value| (*value).to_owned()).collect(),
        evidence: evidence.iter().map(|value| (*value).to_owned()).collect(),
    }
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
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Offline, CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Loopback operator Web control plane with coherent read models, resumable payload-free events, and an explicit bearer-protected paper-task submit mode.",
            &[
                "Write authority is limited to configured replay-backed Grid/Arbitrage paper owners; reconciliation, direct order submission, testnet mutation, and mainnet authority are not exposed.",
            ],
            &[
                "docs/design-system.md",
                "rust/crates/control-plane/src/lib.rs",
                "rust/crates/control-plane/src/submit.rs",
                "rust/crates/control-plane/tests/read_contract.rs",
                "rust/crates/web-app/src/lib.rs",
                "rust/crates/web-app/src/paper_dispatcher.rs",
                "rust/crates/web-app/tests/paper_dispatcher_contract.rs",
                "rust/crates/web/src/api.rs",
                "rust/crates/web/src/app.rs",
                "rust/crates/web/src/server.rs",
                "rust/crates/web/tests/http_contract.rs",
                "rust/crates/web/tests/ui_contract.rs",
                "docs/internal/plans/2026-07-24-project-alignment-web-goal-plan.md",
            ],
        ),
    ]
}

fn exchange_capabilities(adapters: &[AdapterSupport]) -> Vec<Capability> {
    let binance_public = adapter_cell(adapters, "binance", AdapterFacet::PublicData);
    let binance_testnet = adapter_cell(adapters, "binance", AdapterFacet::TestnetProtocol);
    let hyperliquid_testnet = adapter_cell(adapters, "hyperliquid", AdapterFacet::TestnetProtocol);
    let paper_reconcile = adapter_cell(adapters, "paper", AdapterFacet::Reconcile);
    let external_live = adapter_cell(adapters, "binance", AdapterFacet::Live);
    vec![
        adapter_capability(
            "exchange.binance-public",
            CapabilityLevel::ReadOnly,
            scope(
                &[CapabilityEnvironment::Mainnet],
                CapabilityAccess::MarketData,
            ),
            "One-shot Binance Spot public book-ticker snapshots.",
            binance_public,
        ),
        adapter_capability(
            "exchange.binance-testnet-protocol",
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Testnet],
                CapabilityAccess::TestnetTrading,
            ),
            "Executable Binance Spot and USD-M testnet trading and reconcile adapter with deterministic smoke coverage.",
            binance_testnet,
        ),
        adapter_capability(
            "exchange.hyperliquid-testnet-protocol",
            CapabilityLevel::ContractOnly,
            scope(
                &[CapabilityEnvironment::Testnet],
                CapabilityAccess::TestnetTrading,
            ),
            "Injectable-signer Hyperliquid Spot and perpetual testnet request contracts.",
            hyperliquid_testnet,
        ),
        adapter_capability(
            "exchange.paper",
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Deterministic process-local order, fill, reservation, cancel, and reconcile model.",
            paper_reconcile,
        ),
        adapter_capability(
            "exchange.private-live",
            CapabilityLevel::Unavailable,
            scope(
                &[CapabilityEnvironment::Mainnet],
                CapabilityAccess::MainnetTrading,
            ),
            "Authenticated private exchange market, account, and trading adapters.",
            external_live,
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
            "Bounded planned, completed, partial, and incomplete execution JSONL records with a cross-process single-writer lease.",
            &[],
            &[
                "rust/crates/runtime/src/history.rs",
                "rust/crates/runtime/tests/history_writer_lock_contract.rs",
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
            &[
                "A journal-backed paper-only reservation authority owns conservative available/pending/uncertain/committed capacity. The previous 'no cross-process writer exclusion' gap is closed by the shared history writer lease, but this authority still does not own exchange equity, margin, mark-to-market, position truth, or any testnet/mainnet state.",
            ],
            &[
                "rust/crates/strategy/src/risk.rs",
                "rust/crates/runtime/src/paper_account.rs",
                "rust/crates/runtime/tests/paper_account_contract.rs",
                "rust/crates/runtime/tests/paper_account_read_model_contract.rs",
                "docs/internal/audits/RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md",
            ],
        ),
    ]
}

fn runtime_execution_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "runtime.arbitrage",
            CapabilityArea::Runtime,
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Two-leg segmented paper execution through both the one-shot CLI and a recoverable exact-pair owner reached through the trusted CLI/Web submit path.",
            &[
                "The continuous owner currently consumes only an explicit replay-backed profile; nonterminal restart remains deliberately fail-closed and no testnet/mainnet order authority is implied.",
            ],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/paper_arbitrage_saga.rs",
                "rust/crates/apps/src/paper_arbitrage_task.rs",
                "rust/crates/apps/src/paper_profile.rs",
                "rust/crates/apps/tests/paper_arbitrage_saga_contract.rs",
                "rust/crates/apps/tests/paper_arbitrage_task_contract.rs",
                "rust/crates/apps/tests/trusted_submit_cli_contract.rs",
                "rust/crates/web-app/src/paper_dispatcher.rs",
                "rust/crates/web-app/tests/paper_dispatcher_contract.rs",
                "rust/crates/runtime/tests/arbitrage_paper_slice.rs",
            ],
        ),
        capability(
            "runtime.continuous",
            CapabilityArea::Runtime,
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Supervised replay-backed Grid and exact-pair Arbitrage paper owners with durable start/status/stop/cancel facts and bounded graceful shutdown.",
            &[
                "External continuous trading sources and automatic nonterminal restart are not enabled; the Testnet soak path is read-only, and all mainnet authority remains unavailable.",
            ],
            &[
                "rust/crates/runtime/src/market_supervisor.rs",
                "rust/crates/runtime/tests/market_supervisor_contract.rs",
                "rust/crates/apps/src/continuous_monitor.rs",
                "rust/crates/apps/tests/continuous_monitor_task_contract.rs",
                "rust/crates/runtime/src/task_read_model.rs",
                "rust/crates/runtime/tests/task_read_model_contract.rs",
                "rust/crates/apps/src/paper_grid_task.rs",
                "rust/crates/apps/src/paper_arbitrage_task.rs",
                "rust/crates/apps/src/paper_profile.rs",
                "rust/crates/web-app/src/paper_dispatcher.rs",
                "rust/crates/web-app/tests/paper_dispatcher_contract.rs",
            ],
        ),
        capability(
            "runtime.grid",
            CapabilityArea::Runtime,
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Fixed-snapshot grid planning plus a recoverable continuous paper owner that emits one durable operation per crossed level.",
            &[
                "The continuous owner currently consumes only an explicit replay-backed profile and does not imply a real external feed or testnet/mainnet authority.",
            ],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/paper_grid_task.rs",
                "rust/crates/apps/src/paper_profile.rs",
                "rust/crates/apps/tests/command_smoke.rs",
                "rust/crates/apps/tests/paper_grid_task_contract.rs",
                "rust/crates/web-app/src/paper_dispatcher.rs",
                "rust/crates/web-app/tests/paper_dispatcher_contract.rs",
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
            "runtime.market-data",
            CapabilityArea::Runtime,
            CapabilityLevel::ReadOnly,
            scope(
                &[
                    CapabilityEnvironment::Offline,
                    CapabilityEnvironment::Paper,
                    CapabilityEnvironment::Mainnet,
                ],
                CapabilityAccess::MarketData,
            ),
            "Bounded exact-universe market book with freshness, continuity, deterministic replay, subscription gaps, and credential-free Binance Spot polling.",
            &[
                "Only Binance Spot public polling is implemented; the exact-pair composer accepts two source adapters, but no second real venue or executable bootstrap is available.",
            ],
            &[
                "rust/crates/runtime/src/market_data.rs",
                "rust/crates/runtime/src/market_polling.rs",
                "rust/crates/runtime/src/market_supervisor.rs",
                "rust/crates/runtime/tests/market_data_contract.rs",
                "rust/crates/runtime/tests/market_supervisor_contract.rs",
                "rust/crates/apps/src/continuous_monitor.rs",
                "rust/crates/apps/tests/continuous_monitor_task_contract.rs",
                "rust/crates/runtime/src/task_read_model.rs",
                "rust/crates/runtime/tests/task_read_model_contract.rs",
            ],
        ),
        capability(
            "runtime.monitor",
            CapabilityArea::Runtime,
            CapabilityLevel::ReadOnly,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Exact-pair continuous read-only arbitrage composition with journal-first monitor facts, durable source-status checkpoints, bounded stop, and a Web-visible task projection.",
            &[
                "The CLI service bootstrap is replay-backed, only Binance has a real public adapter, and restart recovery projects prior facts without automatically resuming external sources.",
            ],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/monitor.rs",
                "rust/crates/apps/src/continuous_monitor.rs",
                "rust/crates/apps/tests/monitor_contract.rs",
                "rust/crates/apps/tests/monitor_replay_cli.rs",
                "rust/crates/apps/tests/continuous_monitor_task_contract.rs",
                "rust/crates/runtime/src/monitor_read_model.rs",
                "rust/crates/runtime/src/task_read_model.rs",
                "rust/crates/runtime/tests/task_read_model_contract.rs",
                "rust/crates/control-plane/tests/monitor_projection_contract.rs",
                "rust/crates/control-plane/tests/task_projection_contract.rs",
                "rust/crates/web/tests/http_contract.rs",
                "rust/crates/web/tests/ui_contract.rs",
            ],
        ),
        capability(
            "runtime.price-alert",
            CapabilityArea::Runtime,
            CapabilityLevel::ReadOnly,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Bounded multi-symbol price-alert evaluation with durable samples, cooldowns, acknowledgements, a stable read model, isolated local delivery adapters, and a replay-backed CLI serve/status/stop task host with durable task-lifecycle facts.",
            &[
                "The CLI service bootstrap is replay-backed only: no external continuous market source is wired into the price-alert task host, and restart recovery projects prior facts without automatically resuming external sources.",
                "The JSONL alert journal has no rotation or compaction; delivery replay is intentionally disabled, and remote acknowledgement or sound output is not implemented.",
            ],
            &[
                "rust/crates/apps/src/alert/mod.rs",
                "rust/crates/apps/src/alert/journal.rs",
                "rust/crates/apps/src/alert/notification.rs",
                "rust/crates/apps/src/continuous_alert.rs",
                "rust/crates/apps/tests/alert_runtime_contract.rs",
                "rust/crates/apps/tests/alert_serve_cli_contract.rs",
                "rust/crates/runtime/src/alert_read_model.rs",
                "rust/crates/runtime/src/task_read_model.rs",
                "rust/crates/runtime/tests/alert_read_model_contract.rs",
                "rust/crates/runtime/tests/task_read_model_contract.rs",
                "rust/crates/control-plane/tests/alert_projection_contract.rs",
                "rust/crates/web/tests/http_contract.rs",
            ],
        ),
        scanner_capability(),
        testnet_lifecycle_capability(),
        testnet_reconciliation_capability(),
        testnet_soak_capability(),
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

fn scanner_capability() -> Capability {
    capability(
        "runtime.scanner",
        CapabilityArea::Runtime,
        CapabilityLevel::ReadOnly,
        scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
        "Bounded deterministic virtual-grid replay with explicit benchmark/APR ranking, a validated scanner configuration schema, a replay-backed CLI serve/status/stop task host with durable task-lifecycle facts, durable projection, and a read-only Web view.",
        &[
            "The CLI service bootstrap is replay-backed only: no real-time market discovery or external continuous market source is wired into the scanner task host, and no continuous supervisor, automatic restart, terminal UI, or 24-hour market enrichment is implemented.",
            "Rankings are offline historical estimates, not current market freshness, investment advice, or trading authority.",
            "The JSONL journal now enforces a cross-process single-writer lease, but it still has no rotation or compaction.",
        ],
        &[
            "rust/crates/apps/src/scanner.rs",
            "rust/crates/apps/src/continuous_scanner.rs",
            "rust/crates/apps/tests/virtual_grid_scanner_contract.rs",
            "rust/crates/apps/tests/scanner_cli_contract.rs",
            "rust/crates/config/src/scanner.rs",
            "rust/crates/strategy/src/virtual_grid.rs",
            "rust/crates/runtime/src/scanner_read_model.rs",
            "rust/crates/runtime/src/task_read_model.rs",
            "rust/crates/runtime/tests/scanner_read_model_contract.rs",
            "rust/crates/control-plane/tests/scanner_projection_contract.rs",
            "rust/crates/web/tests/http_contract.rs",
            "rust/crates/web/tests/ui_contract.rs",
        ],
    )
}

fn testnet_lifecycle_capability() -> Capability {
    capability(
        "runtime.testnet-lifecycle",
        CapabilityArea::Runtime,
        CapabilityLevel::Available,
        scope(
            &[CapabilityEnvironment::Testnet],
            CapabilityAccess::TestnetTrading,
        ),
        "Explicitly acknowledged durable Binance Testnet submit-query-cancel owner with UUID query-first recovery and bounded polling.",
        &[
            "A credentialed Spot open-order, controlled partial-fill, and kill-and-restart lifecycle remain external release evidence and have not been produced in this workspace.",
            "Mainnet order authority remains unavailable.",
        ],
        &[
            "rust/crates/apps/src/command.rs",
            "rust/crates/apps/src/testnet_lifecycle.rs",
            "rust/crates/apps/tests/testnet_lifecycle_cli_contract.rs",
            "rust/crates/exchange/src/binance_testnet_exchange.rs",
            "rust/crates/exchange/tests/binance_testnet_exchange_contract.rs",
        ],
    )
}

fn testnet_reconciliation_capability() -> Capability {
    capability(
        "runtime.testnet-reconciliation",
        CapabilityArea::Runtime,
        CapabilityLevel::Available,
        scope(
            &[CapabilityEnvironment::Testnet],
            CapabilityAccess::TestnetTrading,
        ),
        "Report-first clean-account gate comparing stable double-sampled Binance Testnet balances, open orders, and positions to one exact committed Paper reservation.",
        &[
            "A credentialed Spot and USD-M account comparison and applied Paper transition remain external release evidence and have not been produced in this workspace.",
            "The comparator intentionally supports one configured Binance product/instrument and settlement asset per run; mixed-exchange reservations fail closed.",
            "Mainnet account and order authority remain unavailable.",
        ],
        &[
            "rust/crates/apps/src/command.rs",
            "rust/crates/apps/src/testnet_reconciliation.rs",
            "rust/crates/apps/tests/testnet_reconciliation_contract.rs",
            "rust/crates/apps/tests/testnet_reconciliation_cli_contract.rs",
            "rust/crates/exchange/src/binance_testnet.rs",
            "rust/crates/exchange/src/binance_testnet_exchange.rs",
            "rust/crates/exchange/tests/binance_testnet_protocol.rs",
            "rust/crates/exchange/tests/binance_testnet_exchange_contract.rs",
            "rust/crates/runtime/src/paper_account.rs",
        ],
    )
}

fn testnet_soak_capability() -> Capability {
    capability(
        "runtime.testnet-soak",
        CapabilityArea::Runtime,
        CapabilityLevel::ReadOnly,
        scope(
            &[CapabilityEnvironment::Testnet],
            CapabilityAccess::TestnetTrading,
        ),
        "Durable Binance Testnet soak owner that cycles Spot and USD-M book tickers plus authenticated reconciliation without submitting or cancelling orders.",
        &[
            "A passing 24-hour credentialed run with an observed kill-and-restart drill remains external release evidence and has not been produced in this workspace.",
        ],
        &[
            "rust/crates/apps/src/command.rs",
            "rust/crates/apps/src/testnet_soak.rs",
            "rust/crates/apps/tests/testnet_soak_contract.rs",
            "rust/crates/apps/tests/testnet_soak_cli_contract.rs",
        ],
    )
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

/// Reported for an adapter the matrix does not list.
///
/// Claiming no capability is the fail-closed answer: an absent row must never
/// be read as permission to do something.
static UNLISTED_ADAPTER_FACET: AdapterFacetSupport = AdapterFacetSupport {
    level: AdapterSupportLevel::Unavailable,
    blockers: Vec::new(),
    evidence: Vec::new(),
};

fn adapter_cell<'a>(
    adapters: &'a [AdapterSupport],
    id: &str,
    facet: AdapterFacet,
) -> &'a AdapterFacetSupport {
    // A linear scan over the ten-row static matrix costs nothing and, unlike a
    // binary search, does not turn a mis-ordered literal into a startup panic
    // for every consumer of the infallible manifest constructor.
    adapters
        .iter()
        .find(|adapter| adapter.id == id)
        .map_or(&UNLISTED_ADAPTER_FACET, |adapter| adapter.facet(facet))
}

fn adapter_capability(
    id: &str,
    level: CapabilityLevel,
    scope: CapabilityScope,
    summary: &str,
    support: &AdapterFacetSupport,
) -> Capability {
    Capability {
        id: id.to_owned(),
        area: CapabilityArea::Exchange,
        level,
        scope,
        summary: summary.to_owned(),
        blockers: support.blockers.clone(),
        evidence: support.evidence.clone(),
    }
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

fn validate_adapters(manifest: &CapabilityManifest) -> Result<(), CapabilityError> {
    if manifest.adapters.is_empty() || manifest.adapters.len() > MAX_ADAPTERS {
        return Err(CapabilityError::InvalidAdapterCount(
            manifest.adapters.len(),
        ));
    }

    let mut previous_id: Option<&str> = None;
    for adapter in &manifest.adapters {
        validate_capability_id(&adapter.id)?;
        if previous_id.is_some_and(|previous| previous >= adapter.id.as_str()) {
            return Err(CapabilityError::UnstableAdapterOrdering(adapter.id.clone()));
        }
        previous_id = Some(&adapter.id);
        validate_text("adapter name", &adapter.name)?;

        for (facet, support) in adapter.facets() {
            if support.evidence.is_empty() {
                return Err(CapabilityError::AdapterMissingEvidence {
                    adapter: adapter.id.clone(),
                    facet,
                });
            }
            for evidence in &support.evidence {
                validate_text("adapter evidence", evidence)?;
            }
            for blocker in &support.blockers {
                validate_text("adapter blocker", blocker)?;
            }
            if matches!(
                support.level,
                AdapterSupportLevel::ProtocolOnly
                    | AdapterSupportLevel::RequestOnly
                    | AdapterSupportLevel::ConfigOnly
                    | AdapterSupportLevel::Unavailable
                    | AdapterSupportLevel::NotApplicable
            ) && support.blockers.is_empty()
            {
                return Err(CapabilityError::AdapterMissingBlocker {
                    adapter: adapter.id.clone(),
                    facet,
                });
            }
        }

        if !manifest.live_trading_enabled
            && !matches!(
                adapter.live.level,
                AdapterSupportLevel::Unavailable | AdapterSupportLevel::NotApplicable
            )
        {
            return Err(CapabilityError::AdapterLiveAuthorityAdvertised(
                adapter.id.clone(),
            ));
        }
    }
    Ok(())
}

fn validate_adapter_capability_alignment(
    manifest: &CapabilityManifest,
) -> Result<(), CapabilityError> {
    let alignments = [
        (
            "exchange.binance-public",
            "binance",
            AdapterFacet::PublicData,
            CapabilityLevel::ReadOnly,
        ),
        (
            "exchange.binance-testnet-protocol",
            "binance",
            AdapterFacet::TestnetProtocol,
            CapabilityLevel::Available,
        ),
        (
            "exchange.hyperliquid-testnet-protocol",
            "hyperliquid",
            AdapterFacet::TestnetProtocol,
            CapabilityLevel::ContractOnly,
        ),
        (
            "exchange.paper",
            "paper",
            AdapterFacet::Reconcile,
            CapabilityLevel::Available,
        ),
        (
            "exchange.private-live",
            "binance",
            AdapterFacet::Live,
            CapabilityLevel::Unavailable,
        ),
    ];

    for (capability_id, adapter_id, facet, expected_level) in alignments {
        let capability = manifest
            .capability(capability_id)
            .ok_or_else(|| CapabilityError::AdapterCapabilityDrift(capability_id.to_owned()))?;
        let support = manifest
            .adapter(adapter_id)
            .map(|adapter| adapter.facet(facet))
            .ok_or_else(|| CapabilityError::AdapterCapabilityDrift(capability_id.to_owned()))?;
        if capability.level != expected_level
            || capability.blockers != support.blockers
            || capability.evidence != support.evidence
        {
            return Err(CapabilityError::AdapterCapabilityDrift(
                capability_id.to_owned(),
            ));
        }
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
    #[error("adapter count {0} must be between 1 and {MAX_ADAPTERS}")]
    InvalidAdapterCount(usize),
    #[error("adapter IDs must be unique and sorted; found {0:?} out of order")]
    UnstableAdapterOrdering(String),
    #[error("adapter {adapter} facet {facet} must contain at least one evidence path")]
    AdapterMissingEvidence {
        adapter: String,
        facet: AdapterFacet,
    },
    #[error("adapter {adapter} facet {facet} must explain its incomplete support level")]
    AdapterMissingBlocker {
        adapter: String,
        facet: AdapterFacet,
    },
    #[error("adapter {0} advertises live authority while live is disabled")]
    AdapterLiveAuthorityAdvertised(String),
    #[error("derived exchange capability {0} drifted from its adapter matrix cell")]
    AdapterCapabilityDrift(String),
}
