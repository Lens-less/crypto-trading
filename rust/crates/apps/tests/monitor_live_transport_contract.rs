use clap::Parser;
use crypto_trading_cli::cli::{Cli, Command, MonitorLiveTransport, MonitorMode};
use std::path::PathBuf;

#[test]
fn live_monitor_defaults_to_stream_and_requires_explicit_polling_degradation() {
    let replay = Cli::try_parse_from([
        "crypto-trading",
        "monitor",
        "--mode",
        "replay",
        "--replay",
        "fixture.jsonl",
    ])
    .unwrap();
    let Command::Monitor(replay) = replay.command else {
        panic!("expected monitor command");
    };
    assert_eq!(
        replay.config,
        PathBuf::from("config/arbitrage/monitor_v2.yaml")
    );

    let stream = Cli::try_parse_from([
        "crypto-trading",
        "monitor",
        "--live",
        "--task-id",
        "stream-default",
    ])
    .unwrap();
    let Command::Monitor(stream) = stream.command else {
        panic!("expected monitor command");
    };
    assert!(matches!(stream.mode, MonitorMode::Serve));
    assert_eq!(stream.live_transport, MonitorLiveTransport::Stream);
    assert_eq!(
        stream.config,
        PathBuf::from("config/arbitrage/monitor-live-testnet.yaml")
    );

    let polling = Cli::try_parse_from([
        "crypto-trading",
        "monitor",
        "--mode",
        "serve",
        "--live",
        "--live-transport",
        "polling",
        "--task-id",
        "polling-degradation",
    ])
    .unwrap();
    let Command::Monitor(polling) = polling.command else {
        panic!("expected monitor command");
    };
    assert_eq!(polling.live_transport, MonitorLiveTransport::Polling);
}
