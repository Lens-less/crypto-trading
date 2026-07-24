use crypto_trading_runtime::{
    CapabilityAccess, CapabilityArea, CapabilityEnvironment, CapabilityLevel, CapabilityManifest,
    ReleaseStage, current_capability_manifest,
};

#[test]
fn current_manifest_is_deterministic_valid_and_live_closed() {
    let manifest = current_capability_manifest();

    manifest.validate().unwrap();
    assert_eq!(manifest.schema_version, 1);
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
    assert_eq!(monitor_runtime.level, CapabilityLevel::ValidateOnly);

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
fn manifest_serialization_is_a_stable_machine_contract() {
    let manifest = current_capability_manifest();
    let value = serde_json::to_value(&manifest).unwrap();

    assert_eq!(value["schema_version"], 1);
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

    let mut invalid_stage = value;
    invalid_stage["release_stage"] = serde_json::json!("live-ready");
    assert!(serde_json::from_value::<CapabilityManifest>(invalid_stage).is_err());
}
