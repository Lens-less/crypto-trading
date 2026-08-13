# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Because this project's user-visible surface is *authority* — what the software
is permitted to do — every release carries an **Authority** section stating
whether that permission widened or narrowed. An empty Authority section means
the software may do exactly what it could do before.

## [Unreleased]

### Fixed
- Alert delivery counters now advance only after the corresponding terminal
  fact is durable. Shutdown uses one absolute deadline for the whole batch and
  records cancellation/panic outcomes without letting memory get ahead of the
  journal.
- Interrupted account-risk admissions expire from durable envelope time and
  are compensated with an explicit fact. New v2 facts bind an exact ticket to
  the Paper reservation; legacy v1 facts derive a stable ticket and lease so
  old journals remain restartable instead of permanently exhausting admission
  capacity.
- REST market observations no longer masquerade as venue-timestamped data or
  turn optional Binance wire update IDs into polling delivery gaps. Timestamp
  provenance, venue sequence metadata, future tolerance, and cross-venue skew
  are explicit and independently enforced.
- Journal chain snapshots can no longer silently omit a segment sealed while
  the chain was being captured. The reader re-inspects the sealed chain after
  freezing the active file and retries the capture when a rotation landed in
  between, so read models never project a chain with a hole in the middle.
- The journal writer now repairs only recoverable active-file crash tails before
  append or rotation: a JSON suffix cut off by EOF is quarantined and truncated,
  while a complete `DecisionRecord` missing only its newline has that terminator
  restored and synchronized. Complete malformed records and every sealed-segment
  inconsistency still fail closed, so recovery cannot launder durable corruption.
- The paper exchange fills resting limit orders at their own limit price when a
  later snapshot trades through the level. Resting orders previously executed
  at the new touch (bid/ask), granting maker orders taker-style price
  improvement on every inter-snapshot gap and systematically overstating paper
  PnL for maker-style strategies. Submit-time crossings still fill at the
  touch, which is correct taker semantics.

- Signed exchange requests no longer follow HTTP redirects. `reqwest` preserves
  custom headers across redirects, so a `3xx` from the venue would have
  replayed the `X-MBX-APIKEY` header and the signed query string to whatever
  host the response named. A redirect now surfaces as an ordinary non-success
  response.
- Binance Spot cancel responses are correlated on `origClientOrderId`. Spot
  reports the cancelled order's identity there and reuses `clientOrderId` for
  the cancel request itself, so every single-order cancel previously failed as
  "not an owned UUID" and every cancel-all classified its own orders as
  foreign. The Spot cancel path could not have completed against the real
  venue.
- USD-M cancel-all acknowledgements are read from the response body. USD-M
  reports refusals inside an HTTP 200, so a refused cancellation was reported
  to the operator as a completed one.
- An implausible venue clock is rejected instead of adopted. The clock offset
  is bounded at 60 seconds and observation timestamps are clamped to the same
  bound, so a wrong `serverTime` can no longer poison every later signed
  request, and a far-future `Date` header can no longer permanently block
  reconciliation for an account.
- `execute_cancel_all` rejects a missing symbol instead of panicking through an
  `unreachable!()` on the cancel path.
- The Paper account authority and the journal writer now derive their lock key
  the same way, so two spellings of one journal path cannot hold two locks.
- `current_capability_manifest()` can no longer panic at startup. An adapter
  row absent from the static matrix reports `unavailable` — the fail-closed
  answer — rather than aborting every consumer.
- Concurrent polling emits honest completion order, schedules the next round
  from completion, preserves `Retry-After`, distinguishes backpressure from
  rejection, and coalesces Hyperliquid due routes into one universe request.
  Broadcast lag retains its bounded oldest-to-latest window and exposes an
  exact cumulative drop count.
- Rolling z-score and performance moments use numerically stable online
  updates. Backtest win rate and profit factor use closed quantity with both
  entry and exit fees, while annualization is derived from tape duration.

### Changed

- Paper account execution now settles exact fees and FIFO realized PnL for
  fully filled synchronous taker receipts, releases closed-lot capacity,
  rechecks reduce-only inventory before append, and keeps negative settled
  equity projectable. This changes Paper accounting semantics; legacy
  reservation-only journals remain explicitly distinguishable and replayable.
- Control-plane state now projects its seven read models in one pagination
  pass, reuses a validated resident projection for unchanged bounded journals,
  and bypasses the cache for large snapshots. Account-risk admission composes
  risk, Paper account, and pending admission state in one pass over a frozen
  journal generation.
- Journal segment discovery uses bounded canonical probes for `.1` through
  `.64`, so unrelated numeric-suffix neighbours cannot make append cost depend
  on parent-directory cardinality. Canonical gaps, non-files and chain overflow
  still fail closed.
- Account-risk day rollover is now monotonic: replaying an older observation
  cannot move the active UTC risk day backwards or reset a newer day's trade
  count.
- Account-risk admission now conservatively includes admitted notional until a
  matching paper reservation consumes it, closing the gap in which concurrent
  owners could all pass the same exposure limit before their reservations were
  durable.
- ATR and EMA now expose warm-up as `None`, seed on the configured window mean,
  and mutate state only after checked arithmetic succeeds.
- The research backtester rejects unsupported maker/limit execution, enforces
  single-instrument tape identity and spot buying power, prices takers at the
  opposing touch, and runs independent strategy selection for each
  walk-forward training window while returning only out-of-sample results.
- `research.backtest` is now an available offline capability through the
  frozen-protocol `crypto-trading-research` binary; `research.indicators`
  remains an unavailable library-only capability. Availability is explicitly
  not an edge claim: the daily experiment had no passing configuration and the
  first hourly protocol stopped at data admission before selection or holdout.
- Martingale sizing treats the deepest adverse grid level as the largest order
  for both long and short grids. This intentionally follows the documented
  strategy meaning instead of the legacy Python short-grid index formula.
- Short-grid scalping places reduce-only take-profit orders beyond breakeven on
  the profitable side. This intentionally corrects the legacy Python formula,
  which subtracted the short offset and could place the exit on the loss side.
- CI triggers on a denylist rather than an allowlist of paths. Test fixtures,
  the `Dockerfile`, and everything under `deploy/` were previously excluded, so
  a change to any of them could ship with a green check and no gate having run.
- CI additionally builds the container image, validates the Compose manifest,
  lints operator shell scripts, enforces `cargo deny` license/ban/source
  policy, and fails on `rustdoc` warnings.
- Journal fixtures are pinned to LF so the Windows CI leg reads the same bytes
  the checksums were computed over.
- CI builds the frontend bundle once and passes it to Rust and browser jobs;
  the Rust matrix keeps Ubuntu MSRV/stable plus Windows MSRV, while docs-only
  and frozen-archive changes no longer trigger the full matrix.
- Config-only and legacy-only venue rows are collapsed into one fail-closed
  `unsupported-venues` capability. Volume maker, price alert, and scanner were
  first maintenance-frozen and later removed entirely (see Removed); the
  `rust/config/legacy/` compatibility samples were removed with them. The
  adapter matrix now covers Binance (public/testnet/mainnet), Hyperliquid
  (public), and Paper only.

### Added

- A journal-backed account-risk authority durably records admission tickets,
  consumes or compensates them through explicit facts, and caches the replayed
  projection for subsequent decisions. Grid and arbitrage owners use the same
  admission boundary before creating Paper reservations.
- Binance Spot MAINNET support with authority separated at the type level:
  `BinanceMainnetReadEndpoints` and `BinanceMainnetTradeEndpoints` (hosts
  `api.binance.com`, `ws-api.binance.com`, `stream.binance.com`), read-only
  and trade adapters, and dedicated credential families
  (`BINANCE_MAINNET_READ_API_KEY/SECRET`, `BINANCE_MAINNET_TRADE_API_KEY/SECRET`;
  `BINANCE_API_KEY/SECRET` stays Testnet-only).
- `live-reconcile`: a read-only Binance Spot MAINNET balance, open-order, and
  optional exchangeInfo report over the read-authority adapter, which cannot
  submit or cancel orders at the type level.
- `live-lifecycle`: one operator-acknowledged Binance Spot MAINNET LIMIT order
  lifecycle (submit → query → cancel) behind the exact phrase
  `I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE` and a required
  `--max-notional` cap, with venue-truth admission from exchangeInfo filters
  and signed balances, spot-no-short, foreign-open-order refusal by default,
  journal-first PLANNED before any mutation, query-first recovery with zero
  blind resubmits, and a journal-latched kill switch on unsafe terminal
  states.
- Cross-platform graceful shutdown covers Ctrl-C, Unix SIGTERM, and Windows
  console close/shutdown signals. Signals are registered before any owner can
  start, SSE streams terminate immediately, all owners drain concurrently
  under one 60-second deadline, and Compose grants a 70-second stop window.
  CI exercises two authenticated writable Paper owners and verifies exactly-once
  durable stop facts after SIGTERM.
- Scoped structured tracing reports application lifecycle, owner transitions,
  first-entry degraded states, and failures without logging credentials,
  payloads, or every successful journal append. Container defaults enable
  these records without turning on dependency noise.
- A no-dependency journal scaling harness measures append cost with 0, 100,
  and 10,000 unrelated sibling files. Crash-tail quarantine is bounded by file
  count and bytes, synchronized before truncation, and inspected off the async
  executor.
- Decimal-only incremental ATR, EMA, EWMA realized volatility, rolling z-score,
  Sharpe, Sortino, drawdown, win-rate, and profit-factor kernels, plus a
  deterministic single-instrument event-tape backtester with fee/slippage
  assumptions, raw trades/equity curves, and out-of-sample walk-forward
  windows.
- A pure bar-strategy contract and shared momentum, Donchian, volatility,
  buy-and-hold, and cash implementations consumed directly by both the
  backtest evaluator and a paper bar owner.
- Reconnecting Binance Spot Testnet `bookTicker` and signed user-data
  WebSocket sources with bounded buffering, ping/pong, jittered backoff,
  connection generations, gap/regression detection, and query-first account
  recovery. The read-only Testnet soak gate now requires market stream, user
  stream, and authenticated REST reconciliation evidence.
- `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, issue and pull
  request templates, `CODEOWNERS`, and `dependabot.yml`.
- `rust/deny.toml` supply-chain policy covering licenses, duplicate versions,
  and crate provenance.

### Removed

- The unreferenced frontend placeholder component, unused design previews,
  duplicate backtest SHA-256 implementation, per-venue config-only capability
  rows, and duplicate active G-series automation tree.
- The maintenance-frozen volume-maker, price-alert, and scanner surfaces:
  their CLI commands, configs, task hosts, and read models. The
  implementations remain in git history.
- The frozen `archive/python-legacy/` evidence tree. `archive/README.md`
  stays as a tombstone; git history is the archive.
- `rust/config/legacy/` and dead-venue grid/arbitrage configuration samples,
  plus the `docs/internal/history/g-series-2026-08-12` snapshot tree.

### Authority

- **Widened: Binance Spot MAINNET manual lifecycle.** The capability manifest
  (schema 4) now reports `release_stage: live-manual` and
  `live_trading_enabled: true`. The only path with mainnet order authority is
  the one-shot `live-lifecycle` command, gated by an exact acknowledgement
  phrase, dedicated mainnet trade environment credentials, and a required
  `--max-notional` cap; `live-reconcile` grants read-only mainnet observation
  behind a separate read-only credential family. Autonomous strategy live
  execution (`runtime.live` for grid/arbitrage) remains unavailable pending
  the strategy promotion gate.
- Narrowed: the volume-maker, price-alert, and scanner surfaces and their
  Paper write paths were removed outright, and the Python legacy tree can no
  longer be executed from the working copy.
- Widened for Paper only: settling closed lots releases simulated capacity, so
  a Paper owner can complete more round trips under the same configured
  exposure ceiling.
- Testnet read authority widened to public and signed WebSocket observations;
  the continuous Testnet owner remains explicit, journaled, kill-switchable,
  and query-first.

## [0.1.0] — unreleased

The first tagged release consolidates the Rust-first rewrite. Highlights from
the development history, grouped by the boundary each one established:

### Added

- Deterministic strategy kernel for fixed grid, segmented arbitrage, price
  alert, volume maker, and virtual grid, separated from I/O.
- `PaperExchange` covering order status, book-depth consumption, GTC/IOC/FOK
  semantics, in-process spot-sell inventory and reduce-only reservations,
  partial-fill contraction, and cancel release.
- Append-only JSONL operation journal with sequence numbers, FNV boundary
  anchors, bounded paging, resume cursors, and a cross-process writer lease.
- Operator read models projecting execution, monitor, alert, scanner, task, and
  Paper-account truth from the journal.
- Read-only Web control plane on `127.0.0.1:8787` with bearer authentication,
  rate limiting, SSE notification, and a no-build embedded UI.
- Capability manifest as the single authority for what each adapter may do,
  projected to `docs/adapter-support.md` and held in sync by a contract test.
- Binance Testnet order-lifecycle and account-reconciliation owners, with
  journal-first planning, query-first recovery, and cumulative query budgets
  that survive restarts.
- Container image, Compose deployment, backup/restore drill, and the
  production-candidate runbook.

### Authority

- Mainnet trading disabled. Live adapters, continuous mainnet operation,
  authoritative account risk, and multi-leg failure compensation all fail
  closed. Binance Testnet order submission is the only path with order
  authority and requires an exact acknowledgement phrase.

[Unreleased]: https://github.com/Lens-less/crypto-trading/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Lens-less/crypto-trading/releases/tag/v0.1.0
