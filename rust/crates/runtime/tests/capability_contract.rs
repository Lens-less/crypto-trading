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
    assert_eq!(grid_runtime.level, CapabilityLevel::PaperOnce);
    assert_eq!(
        grid_runtime.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert_eq!(grid_runtime.scope.access, CapabilityAccess::PaperTrading);

    let monitor_runtime = manifest.capability("runtime.monitor").unwrap();
    assert_eq!(monitor_runtime.level, CapabilityLevel::ReadOnly);
    assert_eq!(
        monitor_runtime.scope.environments,
        vec![CapabilityEnvironment::Offline]
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
            .any(|blocker| blocker.contains("Only Binance Spot public polling"))
    );

    let continuous = manifest.capability("runtime.continuous").unwrap();
    assert_eq!(continuous.level, CapabilityLevel::Unavailable);
    assert_eq!(continuous.scope.access, CapabilityAccess::MainnetTrading);
    assert!(
        continuous
            .blockers
            .iter()
            .any(|blocker| blocker.contains("Only a read-only market-source supervisor"))
    );

    let web = manifest.capability("control-plane.web").unwrap();
    assert_eq!(web.level, CapabilityLevel::ReadOnly);
    assert_eq!(web.scope.environments, vec![CapabilityEnvironment::Offline]);
    assert_eq!(web.scope.access, CapabilityAccess::Local);
    assert!(web.blockers.is_empty());

    let live_runtime = manifest.capability("runtime.live").unwrap();
    assert_eq!(live_runtime.level, CapabilityLevel::Unavailable);
    assert_eq!(
        live_runtime.scope.environments,
        vec![CapabilityEnvironment::Mainnet]
    );
    assert_eq!(live_runtime.scope.access, CapabilityAccess::MainnetTrading);
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
        AdapterSupportLevel::ProtocolOnly
    );
    assert_eq!(
        binance.authenticated.level,
        AdapterSupportLevel::ProtocolOnly
    );
    assert_eq!(binance.reconcile.level, AdapterSupportLevel::RequestOnly);
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
            .blockers,
        binance.testnet_protocol.blockers
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
        "protocol-only"
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
