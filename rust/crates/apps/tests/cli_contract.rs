use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use crypto_trading_cli::{Cli, Command, ExchangeChoice, LogLevel};

#[test]
fn top_level_help_has_readable_utf8_product_text() {
    let help = Cli::command().render_help().to_string();

    assert!(help.contains("多交易所策略自动化系统"), "{help}");
    assert!(!help.contains('�'), "{help}");
}

#[test]
fn grid_keeps_the_legacy_positional_config_and_debug_flag() {
    let cli = Cli::try_parse_from([
        "crypto-trading",
        "grid",
        "config/grid/lighter-long-perp-btc.yaml",
        "--debug",
    ])
    .unwrap();

    let Command::Grid(args) = cli.command else {
        panic!("expected grid command");
    };
    assert_eq!(
        args.config,
        PathBuf::from("config/grid/lighter-long-perp-btc.yaml")
    );
    assert!(args.debug);
    assert_eq!(
        args.history_path,
        PathBuf::from("var/history/grid-paper.jsonl")
    );
}

#[test]
fn grid_price_and_once_are_an_atomic_cli_contract() {
    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "grid",
            "config/grid/lighter-long-perp-btc.yaml",
            "--price",
            "100000",
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "grid",
            "config/grid/lighter-long-perp-btc.yaml",
            "--once",
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "grid",
            "config/grid/lighter-long-perp-btc.yaml",
            "--price",
            "100000",
            "--once",
        ])
        .is_ok()
    );
}

#[test]
fn arbitrage_defaults_match_main_unified() {
    let cli = Cli::try_parse_from(["crypto-trading", "arbitrage"]).unwrap();
    let Command::Arbitrage(args) = cli.command else {
        panic!("expected arbitrage command");
    };
    assert_eq!(
        args.config,
        PathBuf::from("config/arbitrage/arbitrage_segmented.yaml")
    );
    assert_eq!(
        args.monitor_config,
        PathBuf::from("config/arbitrage/monitor_v2.yaml")
    );
    assert_eq!(
        args.history_path,
        PathBuf::from("var/history/arbitrage-paper.jsonl")
    );
}

#[test]
fn arbitrage_once_requires_complete_explicit_price_and_depth_pairs() {
    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "arbitrage",
            "--once",
            "--left-bid",
            "99",
            "--left-ask",
            "100",
            "--right-bid",
            "101",
            "--right-ask",
            "102",
        ])
        .is_err()
    );

    assert!(
        Cli::try_parse_from([
            "crypto-trading",
            "arbitrage",
            "--once",
            "--left-bid",
            "99",
            "--left-ask",
            "100",
            "--right-bid",
            "101",
            "--right-ask",
            "102",
            "--left-bid-quantity",
            "1",
            "--left-ask-quantity",
            "1",
            "--right-bid-quantity",
            "1",
            "--right-ask-quantity",
            "1",
        ])
        .is_ok()
    );
}

#[test]
fn arbitrage_accepts_an_explicit_strategy_key_for_different_legs() {
    let cli = Cli::try_parse_from([
        "crypto-trading",
        "arbitrage",
        "--once",
        "--strategy-key",
        "LIGHTER_ETH_SPOT_PERP",
        "--left-bid",
        "99",
        "--left-ask",
        "100",
        "--right-bid",
        "101",
        "--right-ask",
        "102",
        "--left-bid-quantity",
        "1",
        "--left-ask-quantity",
        "1",
        "--right-bid-quantity",
        "1",
        "--right-ask-quantity",
        "1",
    ])
    .unwrap();
    let Command::Arbitrage(args) = cli.command else {
        panic!("expected arbitrage command");
    };
    assert_eq!(
        args.market.strategy_key.as_deref(),
        Some("LIGHTER_ETH_SPOT_PERP")
    );
}

#[test]
fn scanner_rejects_unknown_exchanges_and_has_stable_defaults() {
    let cli = Cli::try_parse_from(["crypto-trading", "scanner"]).unwrap();
    let Command::Scanner(args) = cli.command else {
        panic!("expected scanner command");
    };
    assert_eq!(args.exchange, ExchangeChoice::Lighter);
    assert_eq!(args.log_level, LogLevel::Info);
    assert!(args.duration.is_none());

    assert!(Cli::try_parse_from(["crypto-trading", "scanner", "--exchange", "unknown"]).is_err());
}

#[test]
fn monitor_symbols_accept_the_legacy_comma_separated_form() {
    let cli = Cli::try_parse_from([
        "crypto-trading",
        "monitor",
        "--symbols",
        "BTC-USDC-PERP,ETH-USDC-PERP",
        "--no-ui",
    ])
    .unwrap();
    let Command::Monitor(args) = cli.command else {
        panic!("expected monitor command");
    };
    assert_eq!(args.symbols, vec!["BTC-USDC-PERP", "ETH-USDC-PERP"]);
    assert!(args.no_ui);
}

#[test]
fn capabilities_supports_human_and_machine_readable_contracts() {
    let cli = Cli::try_parse_from(["crypto-trading", "capabilities", "--json"]).unwrap();
    let Command::Capabilities(args) = cli.command else {
        panic!("expected capabilities command");
    };
    assert!(args.json);

    let cli = Cli::try_parse_from(["crypto-trading", "capabilities"]).unwrap();
    let Command::Capabilities(args) = cli.command else {
        panic!("expected capabilities command");
    };
    assert!(!args.json);
}

#[test]
fn testnet_smoke_keeps_public_and_authenticated_probes_explicit() {
    let cli = Cli::try_parse_from([
        "crypto-trading",
        "testnet-smoke",
        "--call-book-ticker",
        "--call-reconcile",
        "--spot-symbol",
        "BTC-USDC-SPOT",
        "--perpetual-symbol",
        "BTC-USDC-PERP",
        "--wire-symbol",
        "BTCUSDT",
        "--timeout-ms",
        "15000",
        "--json",
    ])
    .unwrap();
    let Command::TestnetSmoke(args) = cli.command else {
        panic!("expected testnet-smoke command");
    };
    assert!(args.call_book_ticker);
    assert!(args.call_reconcile);
    assert_eq!(args.spot_symbol, "BTC-USDC-SPOT");
    assert_eq!(args.perpetual_symbol, "BTC-USDC-PERP");
    assert_eq!(args.wire_symbol, "BTCUSDT");
    assert_eq!(args.timeout_ms, 15_000);
    assert!(args.json);
}
