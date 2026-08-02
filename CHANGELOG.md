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

- Paper account execution no longer treats `available + committed exposure` as
  immutable equity. Fully filled synchronous taker receipts now settle exact
  fees and FIFO realized PnL, release closed-lot capacity, recheck reduce-only
  inventory before append, and keep negative settled equity projectable. Old
  reservation-only journals remain explicitly distinguishable and replayable.
- Account-risk admission is now a durable ticket that is either consumed by a
  reservation or explicitly cancelled. Grid, arbitrage, and volume-maker
  compensation paths propagate failures instead of leaking admitted notional;
  same-timestamp and reserve-after-read races fail closed into recovery.
- REST market observations no longer masquerade as venue-timestamped data or
  turn optional Binance wire update IDs into polling delivery gaps. Timestamp
  provenance, venue sequence metadata, future tolerance, and cross-venue skew
  are explicit and independently enforced.
- Journal chain snapshots can no longer silently omit a segment sealed while
  the chain was being captured. The reader re-inspects the sealed chain after
  freezing the active file and retries the capture when a rotation landed in
  between, so read models never project a chain with a hole in the middle.
- The journal writer refuses to append to — or seal — an active file whose last
  byte is not a record terminator (`HistoryError::PartialTail`). A crash-left
  partial record previously merged with the next append into one malformed
  mid-chain line (or was frozen into a sealed segment readers reject), turning
  a detectable, recoverable tail into permanent corruption.
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

### Changed

- Control-plane state now projects its seven read models in one pagination
  pass, and account-risk admission composes risk, Paper account, and pending
  admission state in one pass over a frozen journal generation.
- Journal segment discovery enumerates the directory once and blocking file
  inspection runs outside async executor threads, removing the fixed 63-probe
  append tax while preserving partial-tail and chain-gap failure semantics.
- Account-risk day rollover is now monotonic: replaying an older observation
  cannot move the active UTC risk day backwards or reset a newer day's trade
  count.
- Account-risk admission now conservatively includes admitted notional until a
  matching paper reservation consumes it, closing the gap in which concurrent
  owners could all pass the same exposure limit before their reservations were
  durable.
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

### Added

- Cross-platform graceful shutdown covers Ctrl-C, Unix SIGTERM, and Windows
  console close/shutdown signals. Compose grants a 70-second stop window and CI
  verifies a container health check followed by a clean SIGTERM exit.
- Structured tracing now reports market polling, exchange dispatch, journal
  append/rotation, projection replay, latency, and failure outcomes at their
  existing I/O seams.
- Decimal-only incremental ATR, EMA, EWMA realized volatility, rolling z-score,
  Sharpe, Sortino, drawdown, win-rate, and profit-factor kernels, plus a
  deterministic single-instrument event-tape backtester with fee/slippage
  assumptions, raw trades/equity curves, and out-of-sample walk-forward
  windows.
- `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, issue and pull
  request templates, `CODEOWNERS`, and `dependabot.yml`.
- `rust/deny.toml` supply-chain policy covering licenses, duplicate versions,
  and crate provenance.

### Authority

- No change. Mainnet remains disabled in the capability manifest, and the only
  path with order authority remains Binance Testnet behind an exact
  acknowledgement phrase.

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
