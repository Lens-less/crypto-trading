# Crypto Trading Web Control Plane

This binary is the trusted composition root for the local HTTP/SSE and Web adapters.
By default it is read-only and bearer-protected. An explicit replay-backed paper write mode can be enabled for one
bounded grid profile and one bounded arbitrage profile, but it still has no live-trading flag and
cannot bind a mainnet exchange handle.

From the `rust/` workspace:

```powershell
$env:CRYPTO_TRADING_WEB_TOKEN = '<generated secret>'
cargo +1.89.0 run -p crypto-trading-web-app --bin crypto-trading-web -- `
  --history-path fixtures/m2-operator-journal.jsonl `
  --journal-id 44444444-4444-4444-8444-444444444444 `
  --port 8787 `
  --bearer-token-env CRYPTO_TRADING_WEB_TOKEN
```

Then open `http://127.0.0.1:8787/overview`.

The read API now requires bearer authentication by default. Put a 32–512 byte token in an
uppercase environment variable and pass only its name:

```powershell
$env:CRYPTO_TRADING_WEB_TOKEN = '<generated secret>'
New-Item -ItemType Directory -Force data | Out-Null
if (-not (Test-Path data/paper-control.jsonl)) {
  New-Item -ItemType File data/paper-control.jsonl | Out-Null
}
cargo +1.89.0 run -p crypto-trading-web-app --bin crypto-trading-web -- `
  --history-path data/paper-control.jsonl `
  --journal-id 44444444-4444-4444-8444-444444444444 `
  --bearer-token-env CRYPTO_TRADING_WEB_TOKEN
```

The shell remains data-free and same-origin. The browser keeps the supplied token only in memory
and uses authenticated `fetch` requests for JSON and event-stream reads.

If you must preserve the legacy unauthenticated local-only read mode for a throwaway operator
session, opt in explicitly with `--allow-open-loopback-read-api`. This escape hatch does not relax
paper-write authentication.

To opt into the loopback-only trusted submit route, you must also provide a bearer env var plus at
least one replay-backed paper profile plus a shared `--paper-account-risk-config`. The first
surface supports one grid profile and one exact-pair arbitrage profile at most:

```powershell
$env:CRYPTO_TRADING_WEB_TOKEN = '<generated secret>'
cargo +1.89.0 run -p crypto-trading-web-app --bin crypto-trading-web -- `
  --history-path fixtures/m2-operator-journal.jsonl `
  --journal-id 44444444-4444-4444-8444-444444444444 `
  --bearer-token-env CRYPTO_TRADING_WEB_TOKEN `
  --enable-paper-writes `
  --paper-account-risk-config config/paper/account-risk.example.yaml `
  --paper-grid-task-id paper-grid-owner `
  --paper-grid-strategy-id grid.strategy `
  --paper-grid-strategy-revision grid.v1 `
  --paper-grid-config config/grid/paper-once-btc.yaml `
  --paper-grid-replay fixtures/m4-grid-paper-replay.jsonl `
  --paper-arbitrage-task-id paper-arbitrage-owner `
  --paper-arbitrage-strategy-id arb.strategy `
  --paper-arbitrage-strategy-revision arb.v1 `
  --paper-arbitrage-config config/arbitrage/paper-once-eth.yaml `
  --paper-arbitrage-monitor-config config/arbitrage/paper-monitor-eth.yaml `
  --paper-arbitrage-replay fixtures/m4-arbitrage-paper-replay.jsonl
```

This mode remains honest about its inputs:
- Every start command must match the configured task id, strategy id, and strategy revision exactly.
- Market data comes only from the supplied finite JSONL replay fixtures mirrored into process-local
  `PaperExchange` adapters.
- Task status still comes only from `GET /api/v1/tasks`; the in-memory dispatcher registry is never
  treated as operator truth.
- Normal process shutdown first closes command admission and durably stops every running paper
  owner. HTTP drain keeps its 60s budget, while owner cleanup keeps an independent 125s hard cap
  aligned with the task-host stop contract, so a valid owner grace is not truncated by the process
  wrapper itself. A rejected, unknown, or over-budget owner shutdown makes the process exit with an
  error and requires inspection through the task projection.
