use std::fmt::Write as _;
use std::path::{Component, Path};

use crypto_trading_runtime::{
    AdapterFacet, AdapterSupportLevel, CapabilityAccess, CapabilityArea, CapabilityEnvironment,
    CapabilityLevel, CapabilityManifest, ReleaseStage, current_capability_manifest,
};

#[test]
fn current_manifest_is_deterministic_valid_and_live_manual() {
    let manifest = current_capability_manifest();

    manifest.validate().unwrap();
    assert_eq!(manifest.schema_version, 4);
    assert_eq!(manifest.release_stage, ReleaseStage::LiveManual);
    assert!(manifest.live_trading_enabled);

    let ids = manifest
        .capabilities
        .iter()
        .map(|capability| capability.id.as_str())
        .collect::<Vec<_>>();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "capabilities must have stable ID ordering");

    for capability in &manifest.capabilities {
        if capability.scope.access == CapabilityAccess::MainnetTrading {
            assert!(
                !capability.blockers.is_empty(),
                "{} must document its operator gates and what stays closed",
                capability.id
            );
        }
    }
    // The only advertised mainnet order authority is the operator-supervised
    // one-shot lifecycle plus its adapter; autonomous strategy execution
    // (runtime.live) stays unavailable.
    let advertised = manifest
        .capabilities
        .iter()
        .filter(|capability| {
            capability.scope.access == CapabilityAccess::MainnetTrading
                && capability.level != CapabilityLevel::Unavailable
        })
        .map(|capability| capability.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        advertised,
        ["exchange.binance-mainnet", "runtime.live-lifecycle"]
    );
}

#[test]
fn manifest_distinguishes_strategy_logic_from_runtime_authority() {
    let manifest = current_capability_manifest();

    let grid_strategy = manifest.capability("strategy.grid").unwrap();
    assert_eq!(grid_strategy.area, CapabilityArea::Strategy);
    assert_eq!(grid_strategy.level, CapabilityLevel::Available);
    assert_eq!(
        grid_strategy.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );
    assert_eq!(grid_strategy.scope.access, CapabilityAccess::Local);
    // The pure protection subsystem (scalping, capital protection, take
    // profit, price lock, stop loss) is part of the offline grid strategy
    // capability and must stay backed by its module and golden tests.
    assert!(grid_strategy.summary.contains("grid-protection"));
    assert!(
        grid_strategy
            .evidence
            .iter()
            .any(|evidence| evidence.ends_with("strategy/src/grid_protection.rs"))
    );
    assert!(
        grid_strategy
            .evidence
            .iter()
            .any(|evidence| evidence.ends_with("strategy/tests/grid_protection.rs"))
    );
    assert!(
        grid_strategy.blockers.iter().any(|blocker| {
            blocker.contains("quantity_precision")
                && blocker.contains("leverage")
                && blocker.contains("fee_rate")
                && blocker.contains("must be supplied by a future execution model")
        }),
        "parsed-but-unmodeled execution controls must stay explicit"
    );

    let grid_runtime = manifest.capability("runtime.grid").unwrap();
    assert_eq!(grid_runtime.area, CapabilityArea::Runtime);
    assert_eq!(grid_runtime.level, CapabilityLevel::Available);
    assert_eq!(
        grid_runtime.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert_eq!(grid_runtime.scope.access, CapabilityAccess::PaperTrading);
    // The paper owner consumes protection directives; the manifest must say so
    // and must scope them to the owner's virtual position.
    assert!(grid_runtime.summary.contains("grid-protection directives"));
    assert!(
        grid_runtime
            .blockers
            .iter()
            .any(|blocker| blocker.contains("virtual position"))
    );

    let web = manifest.capability("control-plane.web").unwrap();
    assert_eq!(web.level, CapabilityLevel::Available);
    assert_eq!(
        web.scope.environments,
        vec![CapabilityEnvironment::Offline, CapabilityEnvironment::Paper]
    );
    assert_eq!(web.scope.access, CapabilityAccess::PaperTrading);
    assert!(
        web.blockers
            .iter()
            .any(|blocker| blocker.contains("replay-backed"))
    );

    let live_runtime = manifest.capability("runtime.live").unwrap();
    assert_eq!(live_runtime.level, CapabilityLevel::Unavailable);
    assert_eq!(
        live_runtime.scope.environments,
        vec![CapabilityEnvironment::Mainnet]
    );
    assert_eq!(live_runtime.scope.access, CapabilityAccess::MainnetTrading);
}

#[test]
fn continuous_capabilities_separate_monitor_market_reads_from_paper_owner_authority() {
    let manifest = current_capability_manifest();

    let monitor_runtime = manifest.capability("runtime.monitor").unwrap();
    assert_eq!(monitor_runtime.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        monitor_runtime.scope.environments,
        vec![
            CapabilityEnvironment::Offline,
            CapabilityEnvironment::Testnet,
            CapabilityEnvironment::Mainnet
        ]
    );
    assert_eq!(monitor_runtime.scope.access, CapabilityAccess::MarketData);
    assert!(
        monitor_runtime
            .blockers
            .iter()
            .any(|blocker| blocker.contains("--live-transport polling"))
    );
    assert!(
        monitor_runtime
            .evidence
            .contains(&"rust/crates/web/tests/ui_contract.rs".to_owned())
    );

    let continuous = manifest.capability("runtime.continuous").unwrap();
    assert_eq!(continuous.level, CapabilityLevel::Available);
    assert_eq!(continuous.scope.access, CapabilityAccess::PaperTrading);
    assert_eq!(
        continuous.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert!(
        continuous
            .blockers
            .iter()
            .any(|blocker| blocker.contains("external continuous trading sources"))
    );
    assert!(
        continuous
            .evidence
            .contains(&"rust/crates/runtime/src/task_read_model.rs".to_owned())
    );
}

#[test]
fn market_data_reflects_one_streaming_and_one_polling_venue() {
    let manifest = current_capability_manifest();

    let market_data = manifest.capability("runtime.market-data").unwrap();
    assert_eq!(market_data.level, CapabilityLevel::ReadOnly);
    assert_eq!(market_data.scope.access, CapabilityAccess::MarketData);
    assert_eq!(
        market_data.scope.environments,
        vec![
            CapabilityEnvironment::Offline,
            CapabilityEnvironment::Paper,
            CapabilityEnvironment::Testnet,
            CapabilityEnvironment::Mainnet
        ]
    );
    assert!(
        market_data
            .evidence
            .contains(&"rust/crates/runtime/src/market_supervisor.rs".to_owned())
    );
    assert!(
        market_data
            .summary
            .contains("Hyperliquid perpetual polling"),
        "the second real venue must be visible in the market-data summary"
    );
    assert!(
        market_data.summary.contains("timestamp provenance")
            && market_data.summary.contains("cross-venue skew rejection"),
        "time provenance and pair-skew safety must stay visible in the capability contract"
    );
    assert!(
        market_data.summary.contains("Testnet bookTicker WebSocket"),
        "the Binance realtime transport must be visible in the capability contract"
    );
    assert!(
        market_data
            .blockers
            .iter()
            .any(|blocker| blocker.contains("Hyperliquid remains credential-free HTTP polling")),
        "the remaining polling limitation must stay an explicit blocker"
    );
    assert!(
        market_data
            .evidence
            .contains(&"rust/crates/runtime/tests/hyperliquid_polling_contract.rs".to_owned())
    );
    assert!(
        market_data
            .evidence
            .contains(&"rust/crates/apps/src/continuous_monitor.rs".to_owned())
    );
}

#[test]
fn testnet_soak_is_acknowledgement_gated_and_keeps_external_evidence_explicit() {
    let manifest = current_capability_manifest();
    let lifecycle = manifest.capability("runtime.testnet-lifecycle").unwrap();
    let reconciliation = manifest
        .capability("runtime.testnet-reconciliation")
        .unwrap();
    let reconciliation_apply = manifest
        .capability("runtime.testnet-reconciliation-apply")
        .unwrap();
    let soak = manifest.capability("runtime.testnet-soak").unwrap();

    assert_eq!(lifecycle.level, CapabilityLevel::Available);
    assert_eq!(
        lifecycle.scope.environments,
        vec![CapabilityEnvironment::Testnet]
    );
    assert_eq!(lifecycle.scope.access, CapabilityAccess::TestnetTrading);
    assert!(lifecycle.summary.contains("query-first recovery"));
    assert!(
        lifecycle
            .blockers
            .iter()
            .any(|blocker| blocker.contains("controlled partial-fill"))
    );
    assert!(
        lifecycle
            .evidence
            .contains(&"rust/crates/apps/src/testnet_lifecycle.rs".to_owned())
    );

    assert_eq!(reconciliation.level, CapabilityLevel::Available);
    assert_eq!(
        reconciliation.scope.environments,
        vec![CapabilityEnvironment::Testnet]
    );
    assert_eq!(
        reconciliation.scope.access,
        CapabilityAccess::TestnetReadOnly
    );
    assert!(reconciliation.summary.contains("balances"));
    assert!(reconciliation.summary.contains("open orders"));
    assert!(reconciliation.summary.contains("positions"));
    assert!(
        reconciliation
            .blockers
            .iter()
            .any(|blocker| blocker.contains("credentialed Spot and USD-M"))
    );
    assert!(
        reconciliation
            .evidence
            .contains(&"rust/crates/apps/src/testnet_reconciliation.rs".to_owned())
    );

    assert_eq!(reconciliation_apply.level, CapabilityLevel::Available);
    assert_eq!(
        reconciliation_apply.scope.environments,
        vec![CapabilityEnvironment::Paper, CapabilityEnvironment::Testnet]
    );
    assert_eq!(
        reconciliation_apply.scope.access,
        CapabilityAccess::PaperTrading
    );
    assert!(reconciliation_apply.summary.contains("Paper"));
    assert!(reconciliation_apply.summary.contains("never submits"));
    assert!(
        reconciliation_apply
            .blockers
            .iter()
            .any(|blocker| blocker.contains("credentialed apply"))
    );

    assert_eq!(soak.level, CapabilityLevel::Available);
    assert_eq!(
        soak.scope.environments,
        vec![CapabilityEnvironment::Testnet]
    );
    assert_eq!(soak.scope.access, CapabilityAccess::TestnetTrading);
    assert!(
        soak.summary.contains("signed user-data WebSocket API")
            && soak.summary.contains("existing acknowledgement")
            && soak.summary.contains("query-first")
    );
    assert!(
        soak.blockers
            .iter()
            .any(|blocker| blocker.contains("24-hour credentialed run"))
    );

    let binance = manifest.adapter("binance").unwrap();
    assert!(
        binance
            .testnet_protocol
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not checked in"))
    );
}

#[test]
fn paper_owner_evidence_does_not_overstate_full_account_or_external_authority() {
    let manifest = current_capability_manifest();
    let account = manifest.capability("risk.account-authority").unwrap();
    // The account-level risk authority is available, but only for the paper
    // environment: testnet/mainnet account truth stays an explicit blocker.
    assert_eq!(account.level, CapabilityLevel::Available);
    assert_eq!(
        account.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert_eq!(account.scope.access, CapabilityAccess::PaperTrading);
    for summary_term in [
        "exact FIFO lots",
        "realized-PnL settlement",
        "reduce-only capacity",
        "exposure caps",
        "daily trade caps",
        "pause/resume",
        "kill switch",
    ] {
        assert!(
            account.summary.contains(summary_term),
            "summary must state {summary_term}"
        );
    }
    for evidence in [
        "rust/crates/strategy/src/account_risk.rs",
        "rust/crates/config/src/account_risk.rs",
        "rust/crates/runtime/src/account_risk.rs",
        "rust/crates/runtime/src/paper_account.rs",
        "rust/crates/runtime/tests/account_risk_contract.rs",
        "rust/crates/control-plane/src/submit.rs",
        "rust/crates/web-app/src/paper_dispatcher.rs",
    ] {
        assert!(
            account.evidence.contains(&evidence.to_owned()),
            "missing evidence {evidence}"
        );
    }
    assert!(
        account
            .blockers
            .iter()
            .any(|blocker| blocker.contains("paper-scoped only")
                && blocker.contains("testnet/mainnet")),
        "the paper-only boundary must stay an explicit blocker"
    );
    assert!(
        account
            .blockers
            .iter()
            .any(|blocker| blocker.contains("no disengage transition")),
        "the latching kill switch must stay explicit"
    );

    let arbitrage = manifest.capability("runtime.arbitrage").unwrap();
    assert_eq!(arbitrage.level, CapabilityLevel::Available);
    assert!(arbitrage.summary.contains("trusted CLI/Web submit path"));
    assert!(
        arbitrage
            .evidence
            .contains(&"rust/crates/apps/src/paper_arbitrage_saga.rs".to_owned())
    );
    let continuous = manifest.capability("runtime.continuous").unwrap();
    assert_eq!(continuous.level, CapabilityLevel::Available);
    assert!(
        continuous
            .evidence
            .contains(&"rust/crates/apps/src/paper_arbitrage_task.rs".to_owned())
    );
    assert!(
        continuous
            .blockers
            .iter()
            .any(|blocker| blocker.contains("automatic nonterminal restart"))
    );
}

#[test]
fn research_backtest_is_a_formal_but_non_profitable_offline_capability() {
    let manifest = current_capability_manifest();
    let backtest = manifest.capability("research.backtest").unwrap();
    assert_eq!(backtest.area, CapabilityArea::Research);
    assert_eq!(backtest.level, CapabilityLevel::Available);
    assert_eq!(
        backtest.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );
    assert_eq!(backtest.scope.access, CapabilityAccess::Local);
    assert!(
        backtest
            .summary
            .contains("Formal frozen-protocol research CLI")
    );
    assert!(
        backtest
            .blockers
            .iter()
            .any(|blocker| blocker.contains("data-admission-aborted")
                && blocker.contains("no selection or holdout evaluation"))
    );
    assert!(
        backtest
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not a profitability claim"))
    );
    assert!(
        backtest.blockers.iter().any(|blocker| {
            blocker.contains("identified perpetual")
                && blocker.contains("fail closed")
                && blocker.contains("margin/liquidation/funding model")
        }),
        "identified perpetual snapshot seams must stay explicitly blocked until a real derivatives model exists"
    );

    let indicators = manifest.capability("research.indicators").unwrap();
    assert_eq!(indicators.area, CapabilityArea::Research);
    assert_eq!(indicators.level, CapabilityLevel::Unavailable);
    assert_eq!(
        indicators.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );
    assert_eq!(indicators.scope.access, CapabilityAccess::Local);
    assert!(indicators.summary.contains("library-only"));
    assert!(indicators.summary.contains("EWMA realized volatility"));
    assert!(
        indicators
            .blockers
            .iter()
            .any(|blocker| blocker.contains("no shipped binary")
                && blocker.contains("no supported CLI or HTTP entry point"))
    );
    assert!(
        indicators
            .blockers
            .iter()
            .any(|blocker| blocker.contains("microprice"))
    );
}

#[test]
fn arbitrage_history_decision_facts_stay_paper_scoped_and_funding_degraded() {
    let manifest = current_capability_manifest();

    let strategy = manifest.capability("strategy.arbitrage").unwrap();
    assert_eq!(strategy.level, CapabilityLevel::Available);
    assert!(strategy.summary.contains("history-mode"));
    for evidence in [
        "rust/crates/strategy/src/arbitrage_history.rs",
        "rust/crates/strategy/tests/arbitrage_history.rs",
    ] {
        assert!(strategy.evidence.contains(&evidence.to_owned()));
    }
    assert!(
        strategy
            .blockers
            .iter()
            .any(|blocker| blocker.contains("funding_degraded")),
        "the missing funding data source must stay an explicit blocker"
    );

    let arbitrage = manifest.capability("runtime.arbitrage").unwrap();
    assert!(arbitrage.summary.contains("history-decision"));
    assert!(arbitrage.summary.contains("spread-history journal"));
    for evidence in [
        "rust/crates/runtime/src/spread_history.rs",
        "rust/crates/runtime/src/spread_history_read_model.rs",
        "rust/crates/runtime/tests/spread_history_contract.rs",
    ] {
        assert!(arbitrage.evidence.contains(&evidence.to_owned()));
    }
    assert!(
        arbitrage
            .blockers
            .iter()
            .any(|blocker| blocker.contains("second venue")
                && blocker.contains("funding_degraded")),
        "waiting for a second venue's public funding data must stay explicit"
    );

    let monitor = manifest.capability("runtime.monitor").unwrap();
    assert!(monitor.summary.contains("spread-history journal"));
    assert!(
        monitor
            .evidence
            .contains(&"rust/crates/runtime/src/spread_history.rs".to_owned())
    );
}

#[test]
fn journal_rotation_is_reflected_without_advertising_compaction() {
    let manifest = current_capability_manifest();
    let rotation_evidence = "rust/crates/runtime/tests/history_rotation_contract.rs".to_owned();

    let history = manifest.capability("history.execution-jsonl").unwrap();
    assert!(history.summary.contains("sealed-segment rotation"));
    assert!(history.evidence.contains(&rotation_evidence));
    assert!(
        history
            .blockers
            .iter()
            .any(|blocker| blocker.contains("no compaction by design"))
    );
}

#[test]
fn mainnet_market_data_does_not_grant_live_trading_authority() {
    let manifest = current_capability_manifest();
    let public_data = manifest.capability("exchange.binance-public").unwrap();

    assert_eq!(public_data.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        public_data.scope.environments,
        vec![CapabilityEnvironment::Mainnet]
    );
    assert_eq!(public_data.scope.access, CapabilityAccess::MarketData);
    // Market data stays read access even in the live-manual stage.
    assert_eq!(public_data.scope.access, CapabilityAccess::MarketData);
}

#[test]
fn live_manual_posture_validation_fails_closed_in_every_direction() {
    // Disabling live trading while a mainnet capability stays advertised must
    // be rejected: the flag and the capability list cannot drift apart.
    let mut disabled = current_capability_manifest();
    disabled.live_trading_enabled = false;
    disabled.release_stage = ReleaseStage::PaperOnly;
    assert!(disabled.validate().is_err());

    // A live-capable capability must document its operator gates.
    let mut gateless = current_capability_manifest();
    gateless
        .capabilities
        .iter_mut()
        .find(|capability| capability.id == "runtime.live-lifecycle")
        .unwrap()
        .blockers
        .clear();
    assert!(gateless.validate().is_err());

    // live_trading_enabled without any available mainnet-trading capability
    // is an incoherent posture.
    let mut hollow = current_capability_manifest();
    for capability in &mut hollow.capabilities {
        if capability.scope.access == CapabilityAccess::MainnetTrading {
            capability.level = CapabilityLevel::Unavailable;
        }
    }
    assert!(hollow.validate().is_err());

    // The stage flag and the boolean must agree.
    let mut incoherent = current_capability_manifest();
    incoherent.live_trading_enabled = false;
    assert!(incoherent.validate().is_err());
}

#[test]
fn adapter_matrix_separates_implementation_from_protocol_and_config_evidence() {
    let manifest = current_capability_manifest();
    let ids = manifest
        .adapters
        .iter()
        .map(|adapter| adapter.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["binance", "hyperliquid", "paper"]);

    let binance = manifest.adapter("binance").unwrap();
    assert_eq!(binance.public_data.level, AdapterSupportLevel::Implemented);
    assert_eq!(
        binance.testnet_protocol.level,
        AdapterSupportLevel::Implemented
    );
    assert_eq!(
        binance.authenticated.level,
        AdapterSupportLevel::Implemented
    );
    assert_eq!(binance.reconcile.level, AdapterSupportLevel::Implemented);
    // Binance is the single live-capable adapter: exactly the acknowledged
    // one-shot Spot lifecycle plus read reconcile, with the boundary spelled
    // out in its blockers.
    assert_eq!(binance.live.level, AdapterSupportLevel::Implemented);
    assert!(
        binance.live.blockers.iter().any(|blocker| {
            blocker.contains("one-shot")
                && blocker.contains("no autonomous strategy")
                && blocker.contains("no market orders")
                && blocker.contains("no margin")
        }),
        "the live facet must state exactly what stays closed: {:?}",
        binance.live.blockers
    );
    assert!(
        binance
            .live
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not checked in")),
        "external supervised-run evidence must stay an explicit operator gate"
    );
    assert_eq!(
        manifest
            .capability("exchange.binance-public")
            .unwrap()
            .evidence,
        binance.public_data.evidence
    );
    assert_eq!(
        manifest
            .capability("exchange.binance-testnet-protocol")
            .unwrap()
            .level,
        CapabilityLevel::Available
    );
    assert_eq!(
        manifest
            .capability("exchange.binance-mainnet")
            .unwrap()
            .evidence,
        binance.live.evidence
    );

    let paper = manifest.adapter("paper").unwrap();
    assert_eq!(paper.public_data.level, AdapterSupportLevel::NotApplicable);
    assert_eq!(paper.reconcile.level, AdapterSupportLevel::Implemented);
    assert_eq!(paper.live.level, AdapterSupportLevel::NotApplicable);
    assert_eq!(
        manifest.capability("exchange.paper").unwrap().evidence,
        paper.reconcile.evidence
    );

    let hyperliquid = manifest.adapter("hyperliquid").unwrap();
    assert_eq!(
        hyperliquid.live.level,
        AdapterSupportLevel::Unavailable,
        "hyperliquid must not advertise live support"
    );
}

#[test]
fn hyperliquid_public_data_is_implemented_polling_without_extra_authority() {
    let manifest = current_capability_manifest();

    let hyperliquid = manifest.adapter("hyperliquid").unwrap();
    assert_eq!(
        hyperliquid.public_data.level,
        AdapterSupportLevel::Implemented
    );
    assert!(
        hyperliquid
            .public_data
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not a realtime stream")),
        "implemented public data must still state its polling cadence limitation"
    );
    assert_eq!(
        manifest
            .capability("exchange.hyperliquid-public")
            .unwrap()
            .evidence,
        hyperliquid.public_data.evidence
    );
    assert_eq!(
        manifest
            .capability("exchange.hyperliquid-public")
            .unwrap()
            .level,
        CapabilityLevel::ReadOnly
    );
    assert_eq!(
        hyperliquid.testnet_protocol.level,
        AdapterSupportLevel::ProtocolOnly
    );
    assert_eq!(
        hyperliquid.reconcile.level,
        AdapterSupportLevel::RequestOnly
    );
    assert_eq!(hyperliquid.live.level, AdapterSupportLevel::Unavailable);
}

#[test]
fn every_adapter_cell_has_checked_in_evidence_and_incomplete_cells_explain_the_gap() {
    let manifest = current_capability_manifest();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .unwrap();
    let facets = [
        AdapterFacet::PublicData,
        AdapterFacet::TestnetProtocol,
        AdapterFacet::Authenticated,
        AdapterFacet::Reconcile,
        AdapterFacet::Live,
    ];

    for adapter in &manifest.adapters {
        for facet in facets {
            let support = adapter.facet(facet);
            assert!(
                !support.evidence.is_empty(),
                "{}/{} lacks evidence",
                adapter.id,
                facet
            );
            for evidence in &support.evidence {
                assert!(
                    repository.join(evidence).is_file(),
                    "{}/{} evidence path does not exist: {evidence}",
                    adapter.id,
                    facet
                );
            }
            if support.level != AdapterSupportLevel::Implemented {
                assert!(
                    !support.blockers.is_empty(),
                    "{}/{} must explain incomplete support",
                    adapter.id,
                    facet
                );
            }
        }
    }
}

#[test]
fn every_capability_has_checked_in_file_evidence() {
    let manifest = current_capability_manifest();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .unwrap();

    for capability in &manifest.capabilities {
        assert!(
            !capability.evidence.is_empty(),
            "{} lacks evidence",
            capability.id
        );
        for evidence in &capability.evidence {
            let relative = Path::new(evidence);
            assert!(
                !relative.is_absolute()
                    && relative
                        .components()
                        .all(|component| matches!(component, Component::Normal(_))),
                "{} evidence must be a normalized repository-relative path: {evidence}",
                capability.id
            );
            assert!(
                repository.join(relative).is_file(),
                "{} evidence is not a checked-in file: {evidence}",
                capability.id
            );
        }
    }
}

#[test]
fn every_available_capability_names_a_shipped_application_boundary() {
    const SHIPPED_APPLICATION_BOUNDARIES: [&str; 3] = [
        "rust/crates/apps/src/",
        "rust/crates/web-app/src/",
        "rust/crates/web/src/",
    ];

    let manifest = current_capability_manifest();
    for capability in manifest
        .capabilities
        .iter()
        .filter(|capability| capability.level == CapabilityLevel::Available)
    {
        assert!(
            capability.evidence.iter().any(|evidence| {
                SHIPPED_APPLICATION_BOUNDARIES
                    .iter()
                    .any(|boundary| evidence.starts_with(boundary))
            }),
            "{} is available but names no shipped CLI/Web implementation boundary",
            capability.id
        );
    }
}

#[test]
fn adapter_matrix_validation_fails_closed_for_live_or_evidence_drift() {
    // With live trading disabled, no adapter may advertise a live facet.
    let mut live = current_capability_manifest();
    live.live_trading_enabled = false;
    live.release_stage = ReleaseStage::PaperOnly;
    for capability in &mut live.capabilities {
        if capability.scope.access == CapabilityAccess::MainnetTrading {
            capability.level = CapabilityLevel::Unavailable;
        }
    }
    assert!(live.validate().is_err());

    let mut evidence = current_capability_manifest();
    evidence
        .adapters
        .iter_mut()
        .find(|adapter| adapter.id == "binance")
        .unwrap()
        .public_data
        .evidence
        .clear();
    assert!(evidence.validate().is_err());

    let mut drift = current_capability_manifest();
    drift
        .capabilities
        .iter_mut()
        .find(|capability| capability.id == "exchange.binance-public")
        .unwrap()
        .evidence
        .push("docs/adapter-support.md".to_owned());
    assert!(drift.validate().is_err());
}

#[test]
fn checked_in_adapter_markdown_is_generated_from_the_manifest_contract() {
    const START: &str = "<!-- adapter-matrix:start -->";
    const END: &str = "<!-- adapter-matrix:end -->";
    let document = include_str!("../../../../docs/adapter-support.md");
    let checked_in = document
        .split_once(START)
        .unwrap()
        .1
        .split_once(END)
        .unwrap()
        .0
        .trim()
        .replace("\r\n", "\n");

    assert_eq!(
        checked_in,
        render_adapter_matrix(&current_capability_manifest())
    );
}

#[test]
fn manifest_serialization_is_a_stable_machine_contract() {
    let manifest = current_capability_manifest();
    let value = serde_json::to_value(&manifest).unwrap();

    assert_eq!(value["schema_version"], 4);
    assert_eq!(value["release_stage"], "live-manual");
    assert_eq!(value["live_trading_enabled"], true);
    assert_eq!(
        value["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == "exchange.binance-public")
            .unwrap()["level"],
        "read-only"
    );
    assert_eq!(
        value["adapters"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == "binance")
            .unwrap()["testnet_protocol"]["level"],
        "implemented"
    );

    let mut invalid_stage = value;
    invalid_stage["release_stage"] = serde_json::json!("live-ready");
    assert!(serde_json::from_value::<CapabilityManifest>(invalid_stage).is_err());
}

fn render_adapter_matrix(manifest: &CapabilityManifest) -> String {
    let mut output = String::from(
        "| Adapter | Public data | Testnet protocol | Authenticated | Reconcile | Live |\n\
         | --- | --- | --- | --- | --- | --- |",
    );
    for adapter in &manifest.adapters {
        write!(
            output,
            "\n| {} | {} | {} | {} | {} | {} |",
            adapter.name,
            adapter.public_data.level,
            adapter.testnet_protocol.level,
            adapter.authenticated.level,
            adapter.reconcile.level,
            adapter.live.level
        )
        .unwrap();
    }
    output
}
