# Release Readiness and Offline Strategy Evaluation - 2026-08-12

## Outcome

All G-001 through G-006 **local acceptance criteria are satisfied**. The final
worktree passed the required Rust, frontend, browser, fixture, dependency,
diff-hygiene, secret-pattern, capability, and deterministic-reproduction
checks. Two independent read-only reviews found no remaining blocking defect.

This is not a real-money readiness claim. The five required classifications are:

| Classification | Result | Meaning |
| --- | --- | --- |
| Code quality passed | **Yes - local evidence** | The complete runbook gate set passed on this Windows workspace with Rust 1.89.0 and Node 22. |
| Offline strategy `promising` | **No** | All three frozen family winners failed the untouched final holdout under both 1x and 2x modeled costs. The search is closed. |
| Paper observable | **Yes for the existing platform; no candidate promotion** | Deterministic Paper journals, task recovery, trusted-submit, API, and browser-observability contracts passed. No G-005 candidate qualified to enter a Paper observation campaign. |
| Credentialed Testnet evidence | **No** | No credential was accepted and no external order or cancel was sent. Lifecycle, reconciliation, and soak remain supervised gates. |
| Mainnet ready | **No** | `live_trading_enabled=false` and `runtime.live=unavailable` were reverified. The architecture intentionally exposes no mainnet authority. |

The repository is therefore locally verified for the scoped safety and research
work, but it is **not approved for Testnet release evidence or mainnet use**.

## Safety Boundary Observed

- No mainnet or Testnet credential was read, accepted, printed, or persisted.
- No external order or cancel was sent, including to Binance Testnet.
- Only deterministic mocks, local Paper/replay paths, public read-only data, and
  frozen offline artifacts were used.
- No dependency was added, no test was removed or weakened, and no risk check
  was relaxed to obtain a green result.
- No commit, push, pull request, publish, or deployment was performed.
- The final capability projection reports `live_trading_enabled=false` and
  `runtime.live` at level `unavailable`.

## Integrated Changes

### G-001 - Authoritative venue rules and conservative admission

- `rust/crates/exchange/src/instrument.rs`,
  `rust/crates/exchange/src/binance_testnet.rs`, and
  `rust/crates/exchange/src/binance_testnet_exchange.rs` now bind Binance Spot
  and USD-M submissions to fetched `exchangeInfo` identity and authoritative
  price, lot-size, notional, precision, and trading-status rules.
- The parser fails closed on unknown, missing, duplicate, disabled, mismatched,
  or non-trading metadata. The Spot/USD-M fixtures and protocol/adapter
  contracts live under `rust/crates/exchange/tests/`.
- High-level MARKET submission no longer substitutes `bookTicker` for an
  authoritative market-notional reference; unsupported notional validation is
  rejected before transport.
- `rust/crates/strategy/src/risk.rs`, `rust/crates/runtime/src/account_risk.rs`,
  and `rust/crates/runtime/src/paper_account.rs` enforce conservative buying
  power, batch reservation, reduction-only behavior, cross-zero prevention,
  and Spot no-short invariants. Their focused regressions are in the adjacent
  strategy/runtime contract suites.

### G-002 - Recovery, reconciliation, and integrated safety consistency

- `rust/crates/apps/src/testnet_lifecycle.rs` and
  `rust/crates/apps/src/command.rs` persist the exact Binance wire symbol and
  rate-limit deadline. Recovery is query-first and does not require fresh
  mutation metadata for an already durable plan.
- Reconciliation failure proofs are exact-replay idempotent even when current
  account state later changes. Testnet read authority and acknowledged local
  Paper apply authority remain separate capability rows.
- Executable arbitrage remains same-literal-symbol only until multi-symbol
  account admission/replay exists; canonical Spot/Perp reasoning remains in the
  pure strategy boundary.
- Web read/submit rate-limit isolation, loopback/bearer trusted submit, embedded
  bundle origin sanitization, and shutdown budget are protected in
  `rust/crates/web*`, `frontend/`, and `deploy/compose.yaml`.

### G-003 - Current primary-source strategy research

- `docs/research/strategy-candidates-2026-08-12.md` records the dated,
  primary-source shortlist and rejection reasoning: slow time-series momentum,
  long-only Donchian, and capped volatility target, plus cash and buy-and-hold
  baselines.
- The shortlist was fixed before market samples or holdout results were
  inspected. Perpetual evaluation remains fail closed without margin,
  maintenance margin, liquidation, funding, and contract-multiplier models.

### G-004/G-005 - Leakage-resistant evaluation and bounded experiment

- `rust/crates/backtest/src/spot_data.rs`, `evaluation.rs`, `candidates.rs`,
  `experiment.rs`, and their public contract tests add ordered provenance,
  causal close-to-next-open execution, embargoed walk-forward windows, a
  consuming terminal holdout, separated component costs, exact 1x/2x
  sensitivity, deterministic uncertainty, and a bounded registry.
- `rust/crates/backtest/examples/g005_evaluation.rs` is an offline example, not
  a shipped trading command. It validates the frozen provenance lock, persists
  and syncs the selection artifact before the type-state transition opens the
  final holdout, and writes only aggregate/provenance artifacts.
- `scripts/prepare-g005-btcusdt.ps1` prepares the public read-only cache;
  `artifacts/strategy-evaluation/` contains the frozen provenance and aggregate
  outputs. Raw ZIP/CSV price data is not stored in the repository.
- Conservative Decimal affordability was repaired at the evaluator boundary;
  the ledger stayed strict. Unchanged target exposure is a true no-op, and
  buy-and-hold enters once instead of repeatedly trading decimal dust.

### Documentation and execution evidence

- `docs/automation/goal-board.md` and all six issue handoffs record claim,
  heartbeat, files, decisions, commands, results, risks, and dependency-ordered
  transitions.
- `.workflow/ultracode/trading-safety-strategy-evaluation-20260812/` records the
  bounded review/implementation packets and final integration evidence.
- `docs/runbooks/production-candidate.md` was aligned with the tested recovery,
  limiter, and supervised evidence contracts.

## Simplifications and Important Decisions

- Removed the high-level Binance MARKET `bookTicker` substitution rather than
  inventing price authority.
- Reused durable lifecycle identity for recovery instead of rebuilding it from
  mutable CLI mapping.
- Kept the existing same-event quote simulator intact and added a separate
  causal bar-evaluation seam, avoiding a silent semantic reinterpretation.
- Reused the existing ledger and shared `SpotBar` type; no duplicate accounting
  or bar domain was introduced.
- Used consuming type states for selection persistence and final holdout access
  instead of relying only on operator discipline.
- Kept research execution in a crate example rather than advertising a new
  application/runtime capability.
- Rejected unsupported perpetual, multi-symbol executable arbitrage, stale
  metadata, fabricated reference prices, and post-holdout tuning rather than
  adding permissive fallbacks.

## Offline Experiment Evidence

### Frozen protocol

- Protocol: `g005-btcusdt-spot-20260812-v1`.
- Plan fingerprint:
  `269a49923ad9b019bfefd9b6a451363de3362fea4bdace4d33a7e42a8817edf5`.
- Data: 103 ordered official Binance Spot BTCUSDT monthly archives, 3,134
  contiguous closed daily bars from 2018-01-01 through 2026-07-31 UTC.
- Split: 1,095 training bars, one embargo bar, 182 test bars, 182-bar step,
  nine selection OOS windows, and one untouched 365-bar final holdout.
- Registry: exactly 22 preregistered configurations across five families,
  including the two mandatory baselines.
- 1x per-side costs: 10 bps fee, 2 bps half-spread proxy, 4 bps slippage proxy,
  and 4 bps latency proxy. The 2x schedule doubles each component.
- Selection was persisted before the consuming holdout transition. After the
  holdout was opened, no strategy, parameter, family, threshold, split, cost,
  or selection rule was changed.

### Untouched final holdout

| Configuration | 1x net return | 2x net return | Profit factor | Max drawdown | Disposition |
| --- | ---: | ---: | ---: | ---: | --- |
| `cash` | 0.00000 | 0.00000 | N/A | 0.00% | baseline |
| `buy-and-hold` | -0.45893 | -0.46109 | 0.00000 | 52.97% | baseline |
| `tsm-lb028-rb007` | -0.16332 | -0.18967 | 0.42919 | 28.70% | failed |
| `donchian-lb020` | -0.22347 | -0.25093 | 0.21180 | 29.99% | failed |
| `vol-lb020-t20-b20-rb007` | -0.29634 | -0.30410 | 0.06104 | 36.90% | failed |

Each family winner failed all four holdout-dependent promotion conditions:
positive 1x net return, profit factor at least 1.2, drawdown no worse than 20%,
and positive 2x return. **No candidate passed.** This is negative offline
evidence and no further search is permitted in this experiment cycle.

### Deterministic identity

- Provenance lock SHA-256:
  `5eb95ab4efeddc2656c6cd2863a48a50c685758ef458a5102bcc64c5047c2d3f`.
- Selection JSON SHA-256:
  `579ba0527ba00c3a84820c0e24988262f17be3140e8f36c24fac78dc7206c7e2`.
- Results JSON SHA-256:
  `89b18ab6024370a9eb079bcc77416141e12fb0d3da1a43701f18750e91bd0cff`.
- Generated report SHA-256:
  `d0948ada1dc3efd08d0a32f451dfce3bcce7e410979034e42f45510296a86ddf`.

The exact locked runner command was executed again during G-006. All three
generated artifacts remained byte-identical at the hashes above.

## Final Verification Evidence

All commands below were run against the current dirty worktree, and their
outputs and exit status were read.

### Rust

| Command | Result |
| --- | --- |
| `cargo +1.89.0 fmt --all -- --check` | passed |
| `cargo +1.89.0 check --workspace --all-targets --all-features --locked` | passed |
| `cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings` | passed |
| `cargo +1.89.0 test --workspace --all-targets --all-features --locked --quiet` | passed; every test binary green |
| `cargo +1.89.0 test --doc --workspace --all-features --locked --quiet` | passed |
| `RUSTDOCFLAGS=-D warnings cargo +1.89.0 doc --no-deps --workspace --all-features --locked` | passed |
| `cargo +1.89.0 build --release --workspace --all-features --locked` | passed |
| `cargo +1.89.0 test --locked -p crypto-trading-web --test api_fixture_contract` | passed, 2/2 |
| `cargo +1.89.0 run --quiet --locked -- capabilities --json` plus assertions | passed: live false, mainnet capability unavailable |

### Frontend and browser

| Command | Result |
| --- | --- |
| `corepack pnpm install --frozen-lockfile` | passed; lockfile unchanged |
| `corepack pnpm typecheck` | passed |
| `corepack pnpm lint` | passed |
| `corepack pnpm test -- --run` | passed, 23 files / 236 tests |
| `corepack pnpm build` | passed, production bundle generated |
| `corepack pnpm exec vitest run src/lib/api-fixtures.test.ts` | passed, 9/9 cross-schema fixture tests |
| rebuild embedded Web binary, then `corepack pnpm e2e` | passed, 6/6 Playwright contracts |

### Supply chain, hygiene, and reproducibility

| Check | Result |
| --- | --- |
| active dependency proof: `cargo tree --workspace --target all --all-features --locked --prefix none` | passed; no active `rkyv` package |
| `cargo audit --file Cargo.lock` from the Rust workspace | passed; 230 locked packages scanned |
| `node scripts/check-lockfile-registry.mjs` | passed; only `registry.npmjs.org` |
| `pnpm audit --prod --audit-level=high` and full `--audit-level=moderate` | passed; no known vulnerability |
| `pnpm licenses list --json \| node scripts/check-licenses.mjs` | passed; 269 packages allowed |
| `git diff --check` | passed; only Windows line-ending notices |
| masked high-confidence worktree secret-pattern scan | passed; zero matching files and no values emitted |
| exact G-005 offline command plus before/after SHA-256 comparison | passed; all artifact bytes unchanged |
| independent final diff review | passed; no P0-P3 blocker |
| independent security review | passed; no confirmed blocking security finding |

`cargo-deny` is not installed in this local environment and was not installed
to manufacture evidence. The tracked CI supply-chain job installs it and runs
`check bans licenses sources`. RustSec advisory `RUSTSEC-2026-0235` is a
documented exception for an inactive optional `rust_decimal` lockfile edge; the
all-target/all-feature graph proves `rkyv` unreachable, and CI expires the
exception on 2026-11-04. Enabling any `rkyv` feature before upgrading remains
forbidden.

## Model and Evidence Limits

- Daily OHLCV cannot reconstruct historical executable spread, order-book
  depth, queue position, market impact, private fee tier, or capacity. Spread,
  slippage, and latency are explicit proxies.
- Fractional BTC is deterministic research arithmetic. Current
  `exchangeInfo` was not projected backward as historical lot-size or
  minimum-notional truth.
- Terminal liquidation and next-open fill conventions are evaluation models,
  not guaranteed fills.
- The research covers one Spot asset, long-or-cash strategies, and one frozen
  time range. It excludes taxes, custody, portfolio interaction, outages, and
  operational capacity.
- The nine walk-forward windows overlap economically; deterministic bootstrap
  intervals are uncertainty diagnostics, not independent-sample guarantees.
- Official public archives can later be corrected. This result is tied to the
  exact ordered manifests and hashes above.
- Perpetuals remain unsupported and fail closed.

## Gates Explicitly Not Satisfied

The following are required before a supervised Testnet release candidate can
advance, and none was run in this unattended Goal:

- credential creation, least-privilege/IP restriction, storage, rotation, and
  revocation rehearsal;
- Binance Testnet new-order/open-order/controlled-partial-fill lifecycle with
  real acknowledgements;
- kill/restart query-first recovery against the same durable evidence file;
- real account reconciliation for every product in scope;
- 24-hour soak with forced termination, recovery, and clean stop;
- journal backup and restore drill;
- archive of redacted command arguments, candidate binary hashes, Testnet CLI
  JSON, and journals under human supervision.

The complete tagged-release process also still requires the repository's
cross-platform CI matrix and its `cargo-deny`, container-build, and operator
script checks. These were not fabricated as local evidence.

Mainnet remains a separate unavailable authority. Testnet evidence, even when
eventually obtained, must not be interpreted as permission to enable mainnet.

## Final Disposition

- G-001 through G-006: locally accepted with evidence.
- Engineering diff: locally green and independently reviewed.
- Offline strategy result: **no candidate passed**; no Paper promotion.
- Supervised Testnet readiness evidence: absent and still required.
- Mainnet readiness: **no**.

