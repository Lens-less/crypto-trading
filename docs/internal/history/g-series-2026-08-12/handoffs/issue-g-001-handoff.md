# G-001 Handoff — Recover Interrupted Trade-Safety Changes

## Current Status

- Status: done; local acceptance completed by the root Goal
- Claim token: cleared after acceptance (`54374ae9-d39e-4c96-b10f-9ae76d11ce6b` was the completed claim)
- Claimed by thread: `019ff1a7-229a-71d1-94c6-548f93748f08`
- Last heartbeat: 2026-08-12T01:07:37+08:00
- Repository: `<repo-root>`
- Worktree: intentionally dirty with uncommitted audit fixes; do not reset,
  discard, or overwrite existing edits.
- Previous workers `authoritative_binance_rules` and
  `enforce_risk_buying_power` were interrupted on 2026-08-12.

## Source Documents

- `README.md`
- `docs/runbooks/production-candidate.md`
- `docs/automation/goal-automation-runbook.md`
- `docs/automation/goal-board.md`
- `rust/crates/runtime/src/capability.rs`

## Completed and Already Verified Work

- Binance market orders fetch a public bid/ask before mutation; public quote
  failure stops before submit.
- Account double-sampling canonicalizes collection order.
- Ambiguous cancel keys use the durable wire-symbol identity.
- Testnet lifecycle restart is query-first; rate-limit evidence is durable.
- Reconciliation outcomes are terminal, conflicting proofs fail closed, and
  identical proof replay is idempotent.
- Legacy account-risk replay no longer consumes reduce-only admissions or
  silently over-consumes pending risk.
- Arbitrage legs require a shared canonical hedge identity.
- Identified perpetual backtests fail closed without a real derivatives margin,
  funding, and liquidation model.
- Health, authenticated reads, and trusted submit have independent rate-limit
  buckets; compose shutdown grace covers owner cleanup.
- Capability schema v3 distinguishes authenticated Testnet read-only access.
- Frontend Router and vulnerable transitive dependencies were upgraded; lint,
  typecheck, 236 tests, build, and `pnpm audit --audit-level moderate` passed.
- Runtime capability contract passed 20 tests; web API fixture was regenerated.

## Interrupted Work to Recover

### Authoritative Binance instrument rules

Partial edits exist in:

- `rust/crates/exchange/src/instrument.rs`
- `rust/crates/exchange/src/binance_testnet.rs`
- `rust/crates/exchange/src/binance_testnet_exchange.rs`
- `rust/crates/exchange/src/lib.rs`
- `rust/crates/exchange/tests/binance_testnet_protocol.rs`
- `rust/crates/exchange/tests/remote_contract_foundations.rs`
- `rust/crates/exchange/tests/fixtures/binance_spot_exchange_info.json`
- `rust/crates/exchange/tests/fixtures/binance_usdm_exchange_info.json`
- `rust/crates/apps/src/command.rs`
- `rust/crates/apps/tests/testnet_lifecycle_cli_contract.rs`

The interrupted worker last reported:

- exchange protocol parser tests were green;
- a market-order `MARKET_LOT_SIZE` regression test used a quantity that hit the
  minimum before the intended step-size assertion;
- `build_binance_symbol_catalog` had an `anyhow::Result` versus
  `ExchangeError` return mismatch;
- the rule-validation and apps lifecycle lanes had not yet gone green.

Treat current compiler/test output as authoritative because files may have
advanced beyond this report.

### Buying-power and batch risk controls

Partial, unverified edits exist in:

- `rust/crates/strategy/src/risk.rs`
- `rust/crates/strategy/tests/risk_engine.rs`
- `rust/crates/apps/src/command.rs`
- `rust/crates/apps/src/paper_arbitrage_task.rs`
- related arbitrage task/command tests

The intended safety contract is:

- opening exposure cannot exceed conservative `min(equity,
  available_balance)` buying power at 1x;
- batch opening notional accumulates across legs;
- true reductions remain allowed, but crossing through flat charges only the
  excess opening amount;
- spot cannot open a short position;
- continuous paper uses settled account equity/balance rather than a fabricated
  large value;
- one-shot synthetic snapshots use an explicit conservative budget;
- malformed, negative, stale, or non-finite financial state fails closed.

## Commands Already Run

- Baseline Rust workspace tests passed before edits.
- Baseline frontend tests passed before edits.
- `cargo audit` passed with no RustSec findings.
- Frontend lint, typecheck, 236 tests, production build, and dependency audit
  passed after dependency upgrades and fixture regeneration.

## Exact Next Prompt

Use the G-001 one-shot prompt in
`docs/automation/goal-automation-runbook.md`. First reread the worktree and run
targeted compile/tests to discover the current failure set. Finish both
interrupted slices without reverting any completed audit fix, then update this
handoff and the board with evidence.

## Current Worker Log

- 2026-08-12T00:31:56+08:00 — The root Goal compared the board before
  writing, claimed G-001 with attempt 1, and completed a full read of the
  runbook, board, handoff, README, production-candidate runbook, applicable
  instructions, tracked diff, and untracked worktree files. Current compile and
  test output will replace every stale failure claim above.
- Safety boundary: local compile/tests, deterministic mocks, and public
  read-only documentation/data only. No order/cancel dispatch, credentials,
  mainnet authority, dependency additions, commits, pushes, or PRs.
- 2026-08-12T00:39:04+08:00 — Current evidence superseded the interrupted
  failures: focused exchange tests passed 42/42, focused strategy tests passed
  45/45, focused apps contracts passed 89/89, lifecycle library tests passed
  11/11, and remote dispatch passed 6/6. Two stale tests were corrected without
  changing production authority: capability text now expects schema v3, and a
  reservation-failure fixture now funds exactly 201.5 raw opening notional so
  it reaches (and fails at) the cost-bearing reservation seam. Affected Clippy
  is the current red loop with six style/shape diagnostics in the interrupted
  exchangeInfo implementation.
- 2026-08-12T00:49:37+08:00 — `cargo +1.89.0 fmt --all -- --check` and
  affected-crate `clippy --all-targets --all-features -- -D warnings` are
  green after behavior-preserving parser/helper extraction. Independent review
  then identified current safety gaps: high limit prices were not used for
  opening buying-power valuation, negative account truth could pass on a pure
  reduction, exchange metadata did not prove standard-symbol asset identity,
  and metadata bootstrap could prevent query-first recovery. Fresh regression
  tests and minimal fixes are now the active loop; no external venue call was
  made.
- 2026-08-12T01:03:30+08:00 — The new exchange red tests proved that the
  parser accepted an ETH canonical label for BTCUSDT and silently ignored an
  unknown filter; both now fail closed. Authoritative metadata requires exact
  base/quote/wire/product identity, explicit Spot market flags and
  `avgPriceMins`, rejects unsupported filters and undocumented USD-M
  `NOTIONAL`, and blocks MARKET notional mutation without an authoritative
  venue reference. Fresh campaigns alone fetch metadata; a durable `planned`
  campaign builds query/cancel-only recovery authority. CLI contracts are
  loopback-only and the Windows nonblocking test-server race is removed.
- Risk red tests now cover high buy-limit valuation, typed negative equity and
  available balance, and unflagged true reductions above the position cap.
  Opening notional uses the conservative valuation price and checked
  arithmetic; batch, cross-flat excess, Spot short, settled post-loss budget,
  and one-shot cumulative synthetic-budget contracts are green.
- Current exact green evidence: exchange contracts 53/53; strategy contracts
  48/48; apps command/saga/task/CLI contracts 93/93; lifecycle library 12/12;
  `cargo +1.89.0 fmt --all -- --check`; and affected exchange/strategy/apps
  `clippy --all-targets --all-features -- -D warnings`. `git diff --check`
  reports no whitespace error (only existing Windows line-ending notices).
  Independent acceptance verification then repeated the affected suites and
  passed: exchange 53/53, apps command/saga/task/CLI/reconciliation
  41/12/35/5/11, affected-crate strict Clippy, Rust formatting, and diff
  whitespace checks. The verifier found no unmet G-001 local criterion.
- 2026-08-12T01:07:37+08:00 — G-001 was marked `done`, its active claim was
  cleared, and G-002 was claimed in dependency order. External Testnet
  lifecycle evidence, credential lifecycle, reconciliation against a real
  venue, and the 24-hour soak remain explicit supervised gates; none was run
  or inferred from the local acceptance result.
