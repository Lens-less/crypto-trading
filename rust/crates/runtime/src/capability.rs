use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CAPABILITY_SCHEMA_VERSION: u16 = 3;
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
    Research,
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
            Self::Research => "research",
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
    TestnetReadOnly,
    TestnetTrading,
    MainnetTrading,
}

impl fmt::Display for CapabilityAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Local => "local",
            Self::MarketData => "market-data",
            Self::PaperTrading => "paper-trading",
            Self::TestnetReadOnly => "testnet-read-only",
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
    let mut capabilities = [
        foundation_capabilities(),
        exchange_capabilities(&adapters),
        state_and_risk_capabilities(),
        research_capabilities(),
        runtime_execution_capabilities(),
        runtime_validation_capabilities(),
        strategy_capabilities(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    capabilities.sort_by(|left, right| left.id.cmp(&right.id));
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
        binance_adapter(),
        hyperliquid_adapter(),
        paper_adapter(),
        unsupported_venues_adapter(),
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
        "The executable adapter, durable lifecycle/stream owner, signed account reconciliation gate, and acknowledgement-gated Testnet soak lifecycle have deterministic coverage, but credentialed Binance Testnet lifecycle/reconciliation evidence and a completed 24-hour soak are not checked in.",
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
            AdapterSupportLevel::Implemented,
            &[
                "Snapshots come from credential-free HTTP polling of the public info endpoint (perpetual impact prices plus an hourly funding-rate side feed), not a realtime stream; freshness is bounded by the poll cadence.",
            ],
            &[
                "rust/crates/exchange/src/hyperliquid_public.rs",
                "rust/crates/exchange/tests/hyperliquid_public_contract.rs",
                "rust/crates/runtime/src/market_polling.rs",
                "rust/crates/runtime/tests/hyperliquid_polling_contract.rs",
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
                "rust/crates/apps/src/command.rs",
            ],
        ),
        live: adapter_facet(
            AdapterSupportLevel::NotApplicable,
            &["PaperExchange cannot create mainnet authority."],
            &paper_evidence,
        ),
    }
}

fn unsupported_venues_adapter() -> AdapterSupport {
    let evidence = [
        "rust/crates/config/src/auth.rs",
        "rust/crates/config/tests/config_compatibility.rs",
        "rust/config/legacy/exchanges/backpack_config.yaml",
        "rust/config/legacy/exchanges/edgex_config.yaml",
        "rust/config/legacy/exchanges/grvt_config.yaml",
        "rust/config/legacy/exchanges/lighter_config.yaml",
        "rust/config/legacy/exchanges/paradex_config.yaml",
        "docs/internal/research/upstream-repository-alignment.md",
        "docs/internal/plans/2026-07-24-project-alignment-web-goal-plan.md",
    ];
    let unsupported = || {
        adapter_facet(
            AdapterSupportLevel::Unavailable,
            &[
                "Backpack, EdgeX, GRVT, Lighter, and Paradex still appear as compatibility-only venue configs, while OKX and Variational remain frozen legacy references; none of these venues has an operator-supported Rust market-data adapter, testnet protocol, authenticated private API, or reconciliation path.",
            ],
            &evidence,
        )
    };
    AdapterSupport {
        id: "unsupported-venues".to_owned(),
        name: "Unsupported venues".to_owned(),
        public_data: unsupported(),
        testnet_protocol: unsupported(),
        authenticated: unsupported(),
        reconcile: unsupported(),
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
            &[
                "rust/crates/config/src/lib.rs",
                "rust/crates/apps/src/command.rs",
            ],
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
    let hyperliquid_public = adapter_cell(adapters, "hyperliquid", AdapterFacet::PublicData);
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
            "exchange.hyperliquid-public",
            CapabilityLevel::ReadOnly,
            scope(
                &[CapabilityEnvironment::Mainnet],
                CapabilityAccess::MarketData,
            ),
            "One-shot Hyperliquid perpetual asset-context snapshots with an hourly funding-rate side channel.",
            hyperliquid_public,
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
            "Bounded planned, completed, partial, and incomplete execution JSONL records with a cross-process single-writer lease and sealed-segment rotation.",
            &[
                "Rotation seals full journal files into read-only segments with no compaction by design, preserving the replayable fact chain; once the bounded segment or chain-byte budget is reached, appends still fail closed.",
            ],
            &[
                "rust/crates/runtime/src/history.rs",
                "rust/crates/runtime/tests/history_writer_lock_contract.rs",
                "rust/crates/runtime/tests/history_rotation_contract.rs",
                "rust/crates/runtime/tests/execution_contract.rs",
                "rust/crates/apps/src/command.rs",
            ],
        ),
        capability(
            "risk.account-authority",
            CapabilityArea::Risk,
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Journal-backed paper account-level risk authority: exact FIFO lots, immediate taker-fee and realized-PnL settlement for fully filled paper receipts, reduce-only capacity, settled-equity balance thresholds, per-symbol/global exposure caps, UTC-midnight daily trade caps, owner position clocks, durable pause/resume, and a latching kill switch.",
            &[
                "The authority is paper-scoped only: exact settlement currently covers synchronous fully filled taker receipts, not resting-maker lifecycle, funding, unrealized mark-to-market, margin/liquidation mechanics, or any testnet/mainnet account truth; those remain mandatory gates for live authority.",
                "Close directives are demands recorded as durable facts; consumers stop the paper owner rather than submitting exchange orders, and the kill switch has deliberately no disengage transition.",
            ],
            &[
                "rust/crates/strategy/src/account_risk.rs",
                "rust/crates/strategy/src/risk.rs",
                "rust/crates/strategy/tests/account_risk.rs",
                "rust/crates/config/src/account_risk.rs",
                "rust/crates/runtime/src/account_risk.rs",
                "rust/crates/runtime/src/paper_account.rs",
                "rust/crates/runtime/tests/account_risk_contract.rs",
                "rust/crates/runtime/tests/paper_account_contract.rs",
                "rust/crates/apps/src/paper_grid_task.rs",
                "rust/crates/apps/src/paper_arbitrage_task.rs",
                "rust/crates/control-plane/src/submit.rs",
                "rust/crates/web-app/src/paper_dispatcher.rs",
                "rust/crates/web/tests/http_contract.rs",
            ],
        ),
    ]
}

fn research_capabilities() -> Vec<Capability> {
    vec![
        capability(
            "research.backtest",
            CapabilityArea::Research,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Formal frozen-protocol research CLI plus a deterministic single-instrument SimClock/EventTape kernel; bar-driven candidate implementations are shared with the paper owner, and datasets, costs, selection, and holdout boundaries remain provenance-locked.",
            &[
                "The first hourly protocol is terminally data-admission-aborted because the official monthly history is not a contiguous UTC-hour series; no selection or holdout evaluation was run.",
                "The earlier daily frozen experiment produced no passing holdout configuration, so this capability provides reproducible research mechanics rather than a validated edge.",
                "identified perpetual production-snapshot seams fail closed until a real margin/liquidation/funding model exists; the current ledger must not be read as realistic derivatives PnL.",
                "This kernel is not a profitability claim: multi-instrument portfolios, queue priority, depth impact, latency, partial fills, funding, and parity with every paper execution path remain open.",
            ],
            &[
                "rust/crates/backtest/src/bin/crypto-trading-research.rs",
                "rust/crates/backtest/src/research_runner_shared.rs",
                "rust/crates/backtest/src/engine.rs",
                "rust/crates/backtest/src/ledger.rs",
                "rust/crates/backtest/src/walk_forward.rs",
                "rust/crates/strategy/src/bar.rs",
                "rust/crates/strategy/src/bar_research.rs",
                "rust/crates/apps/src/paper_bar_task.rs",
                "rust/crates/backtest/tests/backtest_contract.rs",
                "rust/crates/backtest/tests/bar_strategy_shared_contract.rs",
                "rust/crates/apps/tests/paper_bar_task_contract.rs",
                "docs/research/strategy-evaluation-1h-2026-08-12.md",
            ],
        ),
        capability(
            "research.indicators",
            CapabilityArea::Research,
            CapabilityLevel::Unavailable,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Internal library-only Decimal indicator kernels for ATR, EMA, EWMA realized volatility, rolling z-score, and performance-metric primitives, covered by golden vectors.",
            &[
                "Unavailable as a product capability: no shipped binary links this crate, no supported CLI or HTTP entry point exists, and continuous strategy configuration does not consume these indicators.",
                "Order-book imbalance and microprice indicators are not implemented.",
            ],
            &[
                "rust/crates/indicators/src/atr.rs",
                "rust/crates/indicators/src/ema.rs",
                "rust/crates/indicators/src/ewma_volatility.rs",
                "rust/crates/indicators/src/metrics.rs",
                "rust/crates/indicators/src/zscore.rs",
                "rust/crates/indicators/tests/indicators_contract.rs",
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
            "Two-leg segmented paper execution through both the one-shot CLI and a recoverable exact-pair owner reached through the trusted CLI/Web submit path, plus an optional history-decision (natural-spread) gate backfilled from the durable spread-history journal.",
            &[
                "The continuous owner currently consumes only an explicit replay-backed profile; nonterminal restart remains deliberately fail-closed and no testnet/mainnet order authority is implied.",
                "The history-decision mode is paper/replay only: a second venue (Hyperliquid public polling) now provides an hourly funding-rate input channel, but recorded spread-history facts still carry no funding samples, so funding-aware judgements stay degraded (funding_degraded).",
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
                "rust/crates/runtime/src/spread_history.rs",
                "rust/crates/runtime/src/spread_history_read_model.rs",
                "rust/crates/runtime/tests/spread_history_contract.rs",
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
                "Paper owners still lack external continuous trading sources and automatic nonterminal restart; the separate Testnet soak host has an explicitly acknowledged UUID-bound lifecycle with query-first pending recovery, while all mainnet authority remains unavailable.",
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
            "Fixed-snapshot grid planning plus a recoverable continuous paper owner that emits one durable operation per crossed level and translates grid-protection directives (freeze, scalp, reset, exit) into durable facts and bounded paper actions.",
            &[
                "The continuous owner currently consumes only an explicit replay-backed profile and does not imply a real external feed or testnet/mainnet authority; protection directives act on the paper owner's own virtual position, not on exchange truth.",
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

fn market_data_capability() -> Capability {
    capability(
        "runtime.market-data",
        CapabilityArea::Runtime,
        CapabilityLevel::ReadOnly,
        scope(
            &[
                CapabilityEnvironment::Offline,
                CapabilityEnvironment::Paper,
                CapabilityEnvironment::Testnet,
                CapabilityEnvironment::Mainnet,
            ],
            CapabilityAccess::MarketData,
        ),
        "Bounded exact-universe market book with explicit timestamp provenance, venue sequence metadata, cross-venue skew rejection, deterministic replay, subscription gaps, a Binance Spot Testnet bookTicker WebSocket source, and Hyperliquid perpetual polling with an hourly funding-rate side feed.",
        &[
            "The Binance leg is a realtime Testnet WebSocket with bounded queues, ping/pong liveness, reconnect backoff, and sequence regression fail-closed; Hyperliquid remains credential-free HTTP polling, so cross-venue freshness is still bounded by its poll cadence.",
            "The explicit --live-transport polling path remains a degraded fallback, and the Hyperliquid funding side feed drives no decision yet.",
        ],
        &[
            "rust/crates/runtime/src/market_data.rs",
            "rust/crates/runtime/src/market_stream.rs",
            "rust/crates/runtime/src/market_polling.rs",
            "rust/crates/runtime/src/market_supervisor.rs",
            "rust/crates/runtime/tests/market_data_contract.rs",
            "rust/crates/runtime/tests/market_stream_contract.rs",
            "rust/crates/runtime/tests/market_supervisor_contract.rs",
            "rust/crates/runtime/tests/hyperliquid_polling_contract.rs",
            "rust/crates/exchange/tests/binance_stream_contract.rs",
            "rust/crates/apps/src/continuous_monitor.rs",
            "rust/crates/apps/tests/continuous_monitor_task_contract.rs",
            "rust/crates/apps/tests/monitor_live_transport_contract.rs",
            "rust/crates/runtime/src/task_read_model.rs",
            "rust/crates/runtime/tests/task_read_model_contract.rs",
        ],
    )
}

fn runtime_validation_capabilities() -> Vec<Capability> {
    vec![
        market_data_capability(),
        capability(
            "runtime.monitor",
            CapabilityArea::Runtime,
            CapabilityLevel::ReadOnly,
            scope(
                &[
                    CapabilityEnvironment::Offline,
                    CapabilityEnvironment::Testnet,
                    CapabilityEnvironment::Mainnet,
                ],
                CapabilityAccess::MarketData,
            ),
            "Exact-pair continuous read-only arbitrage composition with journal-first monitor facts, durable source-status checkpoints, a dedicated spread-history journal, a default Binance Spot Testnet WebSocket leg, a Hyperliquid polling leg, bounded stop, and a Web-visible task projection.",
            &[
                "The CLI service bootstrap defaults to replay; explicit --live starts only the credential-free binance+hyperliquid pair, and --live-transport polling must be selected deliberately to use the degraded REST fallback.",
                "This monitor is observational only and grants no Testnet or mainnet order authority.",
            ],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/monitor.rs",
                "rust/crates/apps/src/continuous_monitor.rs",
                "rust/crates/runtime/src/spread_history.rs",
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
                "This surface is maintenance-frozen: keep the existing replay-backed evidence path available, but do not widen it with new venue, notification, or automation scope until the shared bar-driven strategy and realtime data seams land.",
                "The CLI service bootstrap is replay-backed only: no external continuous market source is wired into the price-alert task host, and restart recovery projects prior facts without automatically resuming external sources.",
                "The JSONL alert journal rotates through bounded sealed segments with no compaction by design; delivery replay is intentionally disabled, and remote acknowledgement or sound output is not implemented.",
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
                "rust/crates/runtime/tests/history_rotation_contract.rs",
                "rust/crates/control-plane/tests/alert_projection_contract.rs",
                "rust/crates/web/tests/http_contract.rs",
            ],
        ),
        scanner_capability(),
        testnet_lifecycle_capability(),
        testnet_reconciliation_capability(),
        testnet_reconciliation_apply_capability(),
        testnet_soak_capability(),
        capability(
            "runtime.volume-maker",
            CapabilityArea::Runtime,
            CapabilityLevel::Available,
            scope(
                &[CapabilityEnvironment::Paper],
                CapabilityAccess::PaperTrading,
            ),
            "Validated volume-maker configuration plus a recoverable replay-backed paper owner: serve requires an explicit account-risk configuration; virtual maker quotes and imbalance market cycles become independent single-leg reservations with reduce-only closes, account-risk admission, durable hourly statistics facts, and a CLI validate/serve/status/stop task host.",
            &[
                "This surface is maintenance-frozen: keep the current replay-backed paper evidence intact, but do not widen venue coverage, automation, or execution scope until the shared strategy/runtime seam is refocused.",
                "The CLI service bootstrap is replay-backed only: no external continuous market source is wired into the volume-maker task host, and no testnet/mainnet order authority is implied.",
                "The owner keeps no resting orders: limit-mode quotes are virtual and execute only when a later observation crosses them, so legacy post-only resting semantics are simulated, not reproduced; each serve run plans a fresh paper account generation and restart on a foreign generation fails closed.",
            ],
            &[
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/paper_volume_maker_task.rs",
                "rust/crates/apps/tests/paper_volume_maker_task_contract.rs",
                "rust/crates/strategy/src/volume_maker.rs",
                "rust/crates/strategy/tests/volume_maker.rs",
                "rust/crates/config/src/supporting.rs",
                "rust/crates/runtime/src/task_read_model.rs",
                "rust/crates/runtime/tests/task_read_model_contract.rs",
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
            "This surface is maintenance-frozen: preserve the deterministic replay/read-model contract, but do not widen it with new market discovery, scheduling, or venue scope until the shared research/runtime seam is rebuilt.",
            "The CLI service bootstrap is replay-backed only: no real-time market discovery or external continuous market source is wired into the scanner task host, and no continuous supervisor, automatic restart, terminal UI, or 24-hour market enrichment is implemented.",
            "Rankings are offline historical estimates, not current market freshness, investment advice, or trading authority.",
            "A sparse price jump credits every crossed virtual level as a deterministic fill; no order-book depth, queue priority, latency, partial-fill, or gap-liquidity model exists, so rankings are not execution-quality or profitability evidence.",
            "The JSONL journal enforces a cross-process single-writer lease and rotates through bounded sealed segments with no compaction by design; a full segment chain still fails closed.",
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
            "rust/crates/runtime/tests/history_rotation_contract.rs",
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
            CapabilityAccess::TestnetReadOnly,
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

fn testnet_reconciliation_apply_capability() -> Capability {
    capability(
        "runtime.testnet-reconciliation-apply",
        CapabilityArea::Runtime,
        CapabilityLevel::Available,
        scope(
            &[CapabilityEnvironment::Paper, CapabilityEnvironment::Testnet],
            CapabilityAccess::PaperTrading,
        ),
        "Explicitly acknowledged local Paper reconciliation transition driven by stable double-sampled Binance Testnet read-only evidence; it releases one exact reservation or records failure and never submits or cancels a venue order.",
        &[
            "A credentialed apply against real Binance Testnet account evidence remains an external supervised release gate and has not been produced in this workspace.",
            "Write authority is limited to the exact selected Paper account, reservation, and validated reconciliation proof.",
            "Mainnet account and order authority remain unavailable.",
        ],
        &[
            "rust/crates/apps/src/command.rs",
            "rust/crates/apps/src/testnet_reconciliation.rs",
            "rust/crates/apps/tests/testnet_reconciliation_contract.rs",
            "rust/crates/apps/tests/testnet_reconciliation_cli_contract.rs",
            "rust/crates/runtime/src/paper_account.rs",
            "rust/crates/runtime/tests/paper_account_contract.rs",
        ],
    )
}

fn testnet_soak_capability() -> Capability {
    capability(
        "runtime.testnet-soak",
        CapabilityArea::Runtime,
        CapabilityLevel::Available,
        scope(
            &[CapabilityEnvironment::Testnet],
            CapabilityAccess::TestnetTrading,
        ),
        "Durable Binance Spot Testnet owner behind the existing soak host. Its default mode is read-only and cycles bookTicker WebSocket, signed user-data WebSocket API, and owner-backed stable REST reconciliation. Supplying the complete exact lifecycle configuration plus the existing acknowledgement permits one UUID-bound Testnet lifecycle only after a fresh private-stream subscription ACK; restart accepts only a pending durable campaign and recovers it query-first without new submit authority.",
        &[
            "Read-only soak evidence does not count as campaign recovery. The production verifier additionally requires a same-task continuous_testnet_campaign_recovery_verified fact with a fresh exact-ID query delta immediately bound to the observed unclean restart.",
            "Fresh lifecycle mode is opt-in, Testnet-only, and acknowledgement-gated; incomplete configuration and fresh, completed, or failed recovery attempts fail before remote I/O.",
            "A passing 24-hour credentialed run with an observed lifecycle kill-and-query-first-restart drill remains external release evidence and has not been produced in this workspace.",
            "No mainnet endpoint or mainnet submit authority is available.",
        ],
        &[
            "rust/crates/apps/src/command.rs",
            "rust/crates/apps/src/continuous_testnet.rs",
            "rust/crates/apps/src/testnet_soak.rs",
            "rust/crates/runtime/src/binance_user_data.rs",
            "rust/crates/runtime/src/market_stream.rs",
            "rust/crates/apps/tests/testnet_soak_contract.rs",
            "rust/crates/apps/tests/testnet_soak_cli_contract.rs",
            "rust/crates/apps/tests/testnet_stream_soak_contract.rs",
            "rust/crates/apps/tests/continuous_testnet_owner_contract.rs",
            "rust/crates/runtime/tests/binance_user_data_contract.rs",
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
            "Deterministic segmented and history-mode (natural-spread median) arbitrage decisions without I/O.",
            &[
                "History-mode funding terms stay degraded (funding_degraded=true): the Hyperliquid public feed now exposes an hourly funding-rate channel, but no recorded spread sample carries funding input yet, so evaluations remain replay/paper scoped.",
            ],
            &[
                "rust/crates/strategy/src/arbitrage.rs",
                "rust/crates/strategy/src/arbitrage_history.rs",
                "rust/crates/strategy/tests/segmented_arbitrage.rs",
                "rust/crates/strategy/tests/arbitrage_history.rs",
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/paper_arbitrage_task.rs",
            ],
        ),
        capability(
            "strategy.grid",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic fixed-grid and martingale planning plus pure grid-protection state machines (scalping, capital protection, take profit, price lock, stop loss) without I/O.",
            &[
                "The planner does not consume configured quantity_precision, price_decimals, margin_mode, leverage, fee_rate, follow_timeout, or follow_distance; venue normalization, margin, fees, and follow scheduling must be supplied by a future execution model before these plans can support trading or PnL claims.",
            ],
            &[
                "rust/crates/strategy/src/grid.rs",
                "rust/crates/strategy/src/grid_protection.rs",
                "rust/crates/strategy/tests/grid_planner.rs",
                "rust/crates/strategy/tests/grid_protection.rs",
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/paper_grid_task.rs",
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
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/alert/mod.rs",
            ],
        ),
        capability(
            "strategy.scanner",
            CapabilityArea::Strategy,
            CapabilityLevel::Available,
            scope(&[CapabilityEnvironment::Offline], CapabilityAccess::Local),
            "Deterministic virtual-grid simulation and volatility scoring.",
            &[
                "Sparse price jumps deterministically fill every crossed pending level without depth, queue, latency, partial-fill, or gap-liquidity modeling; this scorer is not execution-quality or profitability evidence.",
            ],
            &[
                "rust/crates/strategy/src/virtual_grid.rs",
                "rust/crates/strategy/tests/virtual_grid_golden.rs",
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/scanner.rs",
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
                "rust/crates/apps/src/command.rs",
                "rust/crates/apps/src/paper_volume_maker_task.rs",
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
    // A linear scan over the tiny static matrix costs nothing and, unlike a
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
            "exchange.hyperliquid-public",
            "hyperliquid",
            AdapterFacet::PublicData,
            CapabilityLevel::ReadOnly,
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
