use std::path::PathBuf;

use clap::Parser;
use crypto_trading_cli::{Cli, Command, ExchangeChoice, LogLevel};

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
fn arbitrage_once_requires_a_complete_explicit_price_pair() {
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
        ])
        .is_ok()
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
