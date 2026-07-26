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
            .evidence
            .contains(&"rust/crates/apps/tests/alert_serve_cli_contract.rs".to_owned())
    );
    assert!(
        price_alert
            .blockers
            .iter()
            .any(|blocker| blocker.contains("replay-backed only"))
    );
    assert!(
        !price_alert
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not yet registered in the durable task lifecycle"))
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
fn market_data_reflects_two_polling_venues_without_realtime_claims() {
    let manifest = current_capability_manifest();

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
            .summary
            .contains("Hyperliquid perpetual polling"),
        "the second real venue must be visible in the market-data summary"
    );
    assert!(
        market_data
            .blockers
            .iter()
            .any(|blocker| blocker.contains("credential-free HTTP polling rather than realtime")),
        "the polling (non-realtime) limitation must stay an explicit blocker"
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
fn testnet_soak_is_read_only_and_keeps_external_release_evidence_explicit() {
    let manifest = current_capability_manifest();
    let lifecycle = manifest.capability("runtime.testnet-lifecycle").unwrap();
    let reconciliation = manifest
        .capability("runtime.testnet-reconciliation")
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
        CapabilityAccess::TestnetTrading
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
    // The account-level risk authority is available, but only for the paper
    // environment: testnet/mainnet account truth stays an explicit blocker.
    assert_eq!(account.level, CapabilityLevel::Available);
    assert_eq!(
        account.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert_eq!(account.scope.access, CapabilityAccess::PaperTrading);
    for summary_term in [
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

    for id in ["runtime.price-alert", "runtime.scanner"] {
        let capability = manifest.capability(id).unwrap();
        assert!(
            !capability
                .blockers
                .iter()
                .any(|blocker| blocker.contains("no rotation")),
            "{id} must not claim the journal lacks rotation once sealed segments ship"
        );
        assert!(
            capability
                .blockers
                .iter()
                .any(|blocker| blocker.contains("no compaction by design")),
            "{id} must keep the no-compaction design decision explicit"
        );
        assert!(capability.evidence.contains(&rotation_evidence));
    }
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
        "rust/crates/apps/src/continuous_scanner.rs",
        "rust/crates/apps/tests/scanner_cli_contract.rs",
        "rust/crates/config/src/scanner.rs",
        "rust/crates/runtime/src/scanner_read_model.rs",
        "rust/crates/runtime/src/task_read_model.rs",
        "rust/crates/web/tests/ui_contract.rs",
    ] {
        assert!(scanner.evidence.contains(&evidence.to_owned()));
    }
    assert!(
        scanner.summary.contains("scanner configuration schema"),
        "{}",
        scanner.summary
    );
    assert!(
        !scanner.blockers.iter().any(
            |blocker| blocker.contains("not implemented; the existing CLI remains fail-closed")
        ),
        "the config-schema/CLI-bootstrap blocker must be gone once the task host ships"
    );
    assert!(
        scanner
            .blockers
            .iter()
            .any(|blocker| blocker.contains("replay-backed only"))
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
fn volume_maker_paper_owner_is_available_without_external_or_resting_authority() {
    let manifest = current_capability_manifest();

    let strategy = manifest.capability("strategy.volume-maker").unwrap();
    assert_eq!(strategy.level, CapabilityLevel::Available);
    assert_eq!(
        strategy.scope.environments,
        vec![CapabilityEnvironment::Offline]
    );

    let runtime = manifest.capability("runtime.volume-maker").unwrap();
    assert_eq!(runtime.level, CapabilityLevel::Available);
    assert_eq!(
        runtime.scope.environments,
        vec![CapabilityEnvironment::Paper]
    );
    assert_eq!(runtime.scope.access, CapabilityAccess::PaperTrading);
    for summary_term in [
        "replay-backed paper owner",
        "single-leg reservations",
        "account-risk admission",
        "hourly statistics facts",
        "validate/serve/status/stop",
    ] {
        assert!(
            runtime.summary.contains(summary_term),
            "summary must state {summary_term}"
        );
    }
    for evidence in [
        "rust/crates/apps/src/paper_volume_maker_task.rs",
        "rust/crates/apps/tests/paper_volume_maker_task_contract.rs",
        "rust/crates/strategy/src/volume_maker.rs",
        "rust/crates/runtime/src/task_read_model.rs",
    ] {
        assert!(
            runtime.evidence.contains(&evidence.to_owned()),
            "missing evidence {evidence}"
        );
    }
    assert!(
        runtime
            .blockers
            .iter()
            .any(|blocker| blocker.contains("replay-backed only")
                && blocker.contains("no testnet/mainnet order authority")),
        "the replay-only and no-external-authority boundary must stay explicit"
    );
    assert!(
        runtime
            .blockers
            .iter()
            .any(|blocker| blocker.contains("no resting orders")),
        "the virtual-quote simulation boundary must stay explicit"
    );
    assert!(
        !runtime
            .blockers
            .iter()
            .any(|blocker| blocker.contains("not implemented")),
        "the unfinished-runtime blocker must be gone once the paper owner ships"
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
