use std::fmt::Write as _;
use std::path::Path;

use crypto_trading_runtime::{
    AdapterFacet, AdapterSupportLevel, CapabilityAccess, CapabilityArea, CapabilityEnvironment,
    CapabilityLevel, CapabilityManifest, ReleaseStage, current_capability_manifest,
};

#[test]
fn current_manifest_is_deterministic_valid_and_live_closed() {
    let manifest = current_capability_manifest();

    manifest.validate().unwrap();
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.release_stage, ReleaseStage::PaperOnly);
    assert!(!manifest.live_trading_enabled);

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
            assert_eq!(
                capability.level,
                CapabilityLevel::Unavailable,
                "{} must not advertise live authority",
                capability.id
            );
            assert!(
                !capability.blockers.is_empty(),
                "{} must explain why live remains closed",
                capability.id
            );
        }
    }
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

    let grid_runtime = manifest.capability("runtime.grid").unwrap();
    assert_eq!(grid_runtime.area, CapabilityArea::Runtime);
    assert_eq!(grid_runtime.level, CapabilityLevel::Available);
    assert_eq!(
        grid_runtime.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert_eq!(grid_runtime.scope.access, CapabilityAccess::PaperTrading);

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
fn continuous_capabilities_separate_monitor_reads_from_paper_owner_authority() {
    let manifest = current_capability_manifest();

    let monitor_runtime = manifest.capability("runtime.monitor").unwrap();
    assert_eq!(monitor_runtime.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        monitor_runtime.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );
    assert!(
        monitor_runtime
            .blockers
            .iter()
            .any(|blocker| blocker.contains("without automatically resuming external sources"))
    );
    assert!(
        monitor_runtime
            .evidence
            .contains(&"rust/crates/web/tests/ui_contract.rs".to_owned())
    );

    let price_alert = manifest.capability("runtime.price-alert").unwrap();
    assert_eq!(price_alert.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        price_alert.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );
    assert_eq!(price_alert.scope.access, CapabilityAccess::Local);
    assert!(
        price_alert
            .evidence
            .contains(&"rust/crates/runtime/src/alert_read_model.rs".to_owned())
    );
    assert!(
        price_alert
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not yet registered in the durable task lifecycle"))
    );

    let market_data = manifest.capability("runtime.market-data").unwrap();
    assert_eq!(market_data.level, CapabilityLevel::ReadOnly);
    assert_eq!(market_data.scope.access, CapabilityAccess::MarketData);
    assert_eq!(
        market_data.scope.environments,
        vec![
            CapabilityEnvironment::Offline,
            CapabilityEnvironment::Paper,
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
            .blockers
            .iter()
            .any(|blocker| blocker.contains("no second real venue or executable bootstrap"))
    );
    assert!(
        market_data
            .evidence
            .contains(&"rust/crates/apps/src/continuous_monitor.rs".to_owned())
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
            .any(|blocker| blocker.contains("External continuous trading sources"))
    );
    assert!(
        continuous
            .evidence
            .contains(&"rust/crates/runtime/src/task_read_model.rs".to_owned())
    );
}

#[test]
fn testnet_soak_is_read_only_and_keeps_external_release_evidence_explicit() {
    let manifest = current_capability_manifest();
    let soak = manifest.capability("runtime.testnet-soak").unwrap();

    assert_eq!(soak.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        soak.scope.environments,
        vec![CapabilityEnvironment::Testnet]
    );
    assert_eq!(soak.scope.access, CapabilityAccess::TestnetTrading);
    assert!(
        soak.summary
            .contains("without submitting or cancelling orders")
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
    assert_eq!(account.level, CapabilityLevel::Unavailable);
    assert!(
        account
            .evidence
            .contains(&"rust/crates/runtime/src/paper_account.rs".to_owned())
    );
    assert!(
        account
            .blockers
            .iter()
            .any(|blocker| blocker.contains("paper-only reservation"))
    );
    assert!(
        account
            .blockers
            .iter()
            .any(|blocker| blocker.contains("gap is closed"))
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
fn scanner_read_only_facts_do_not_advertise_current_or_trading_authority() {
    let manifest = current_capability_manifest();
    let scanner = manifest.capability("runtime.scanner").unwrap();

    assert_eq!(scanner.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        scanner.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );
    assert_eq!(scanner.scope.access, CapabilityAccess::Local);
    for evidence in [
        "rust/crates/apps/src/scanner.rs",
        "rust/crates/runtime/src/scanner_read_model.rs",
        "rust/crates/web/tests/ui_contract.rs",
    ] {
        assert!(scanner.evidence.contains(&evidence.to_owned()));
    }
    assert!(
        scanner
            .blockers
            .iter()
            .any(|blocker| blocker.contains("CLI/service bootstrap"))
    );
    assert!(
        scanner
            .blockers
            .iter()
            .any(|blocker| blocker.contains("offline historical estimates"))
    );
    assert!(!manifest.live_trading_enabled);
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
    assert!(!manifest.live_trading_enabled);

    let mut invalid = manifest;
    let live_runtime = invalid
        .capabilities
        .iter_mut()
        .find(|capability| capability.id == "runtime.live")
        .unwrap();
    live_runtime.level = CapabilityLevel::Available;
    assert!(invalid.validate().is_err());
}

#[test]
fn adapter_matrix_separates_implementation_from_protocol_and_config_evidence() {
    let manifest = current_capability_manifest();
    let ids = manifest
        .adapters
        .iter()
        .map(|adapter| adapter.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [
            "backpack",
            "binance",
            "edgex",
            "grvt",
            "hyperliquid",
            "lighter",
            "okx",
            "paper",
            "paradex",
            "variational",
        ]
    );

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
    assert_eq!(binance.live.level, AdapterSupportLevel::Unavailable);
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

    let hyperliquid = manifest.adapter("hyperliquid").unwrap();
    assert_eq!(
        hyperliquid.public_data.level,
        AdapterSupportLevel::Unavailable
    );
    assert_eq!(
        hyperliquid.testnet_protocol.level,
        AdapterSupportLevel::ProtocolOnly
    );
    assert_eq!(
        hyperliquid.reconcile.level,
        AdapterSupportLevel::RequestOnly
    );

    let backpack = manifest.adapter("backpack").unwrap();
    assert_eq!(backpack.public_data.level, AdapterSupportLevel::ConfigOnly);
    assert_eq!(
        backpack.authenticated.level,
        AdapterSupportLevel::ConfigOnly
    );
    assert_eq!(backpack.reconcile.level, AdapterSupportLevel::Unavailable);

    let paper = manifest.adapter("paper").unwrap();
    assert_eq!(paper.public_data.level, AdapterSupportLevel::NotApplicable);
    assert_eq!(paper.reconcile.level, AdapterSupportLevel::Implemented);
    assert_eq!(paper.live.level, AdapterSupportLevel::NotApplicable);
    assert_eq!(
        manifest.capability("exchange.paper").unwrap().evidence,
        paper.reconcile.evidence
    );

    for adapter in manifest
        .adapters
        .iter()
        .filter(|adapter| adapter.id != "paper")
    {
        assert_eq!(
            adapter.live.level,
            AdapterSupportLevel::Unavailable,
            "{} must not advertise live support",
            adapter.id
        );
    }
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
fn adapter_matrix_validation_fails_closed_for_live_or_evidence_drift() {
    let mut live = current_capability_manifest();
    live.adapters
        .iter_mut()
        .find(|adapter| adapter.id == "binance")
        .unwrap()
        .live
        .level = AdapterSupportLevel::Implemented;
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

    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["release_stage"], "paper-only");
    assert_eq!(value["live_trading_enabled"], false);
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
