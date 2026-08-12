# G-004 Handoff - Leakage-Resistant Evaluation Seam

## Current Status

- Status: done under the root Goal
- Claim token: cleared after acceptance
- Claimed at: 2026-08-12T02:08:17+08:00
- Claimed by thread: `019ff1a7-229a-71d1-94c6-548f93748f08`
- Last heartbeat: 2026-08-12T02:38:24+08:00
- Attempt: 1
- Output: `docs/research/evaluation-seam-2026-08-12.md`

## Source Documents

- `docs/automation/goal-automation-runbook.md`
- `docs/automation/goal-board.md`
- `docs/automation/handoffs/issue-g-003-handoff.md`
- `docs/research/strategy-candidates-2026-08-12.md`
- `.workflow/ultracode/trading-safety-strategy-evaluation-20260812/eval-contract.md`
- `docs/runbooks/production-candidate.md`
- `README.md`

## Scope and Acceptance

- Build the minimum leakage-resistant, cost-aware evaluation seam for the
  accepted G-003 shortlist.
- Keep the seam deterministic and offline: frozen data provenance, walk-forward
  splits, embargo, one untouched final holdout, and explicit 1x/2x cost
  schedules.
- Provide the smallest pure adapters for B-0 cash/no-trade, B-1 BTC
  buy-and-hold, C-1 slow BTC time-series momentum, C-2 causal Donchian
  breakout, and C-3 capped volatility-controlled BTC exposure.
- Do not add new strategy families, optimizer machinery, dependency growth,
  live/Testnet authority, or any post-holdout tuning.
- Preserve fail-closed behavior for perpetual, leverage, funding, borrow,
  liquidation, and maker/resting-limit assumptions.

## Safety Boundary

- `live_trading_enabled=false`; mainnet remains unavailable.
- No credentials and no external order or cancel, including Testnet.
- Public read-only data, deterministic mock, loopback, offline replay, and
  paper execution only.
- No holdout inspection before configuration selection is frozen.
- No post-holdout retuning, p-hacking, or hidden benchmark drift.

## Worker Log

- 2026-08-12T02:08:17+08:00 - G-003 research was accepted after the dated
  shortlist recorded three Spot candidate families, cash/buy-and-hold baselines,
  source URLs, evaluation boundaries, and a hard no-pivot search budget.
- 2026-08-12T02:08:17+08:00 - G-004 has been claimed. Next step is to inspect
  the existing backtest/data/seam code paths and identify the smallest boundary
  that can enforce provenance, embargo, costs, and final-holdout discipline
  without expanding scope.
- 2026-08-12T02:15:29+08:00 - Root inspection and three independent read-only
  reviews agreed not to reinterpret the existing same-event quote simulator.
  The chosen boundary is a separate pure bar seam: verified Binance Spot kline
  provenance, close-decision/next-open execution, embargoed walk-forward
  windows, a consuming final-holdout gate, componentwise 1x/2x costs, and
  long-or-cash exposure. Two new contract files were written first. Their
  initial `cargo test --no-run` failed on the deliberately missing types and
  errors, establishing the expected red state before implementation.
- 2026-08-12T02:38:24+08:00 - The red/green campaign closed the evaluation
  seam. Public contracts now bind every run to a checksum-verified Binance Spot
  dataset; execute completed-close signals only at the next open; enforce
  complete embargoed OOS windows and one terminal holdout; cap long exposure by
  buying power; charge fee, half-spread, slippage, and latency separately; and
  liquidate every strategy at the common terminal close.
- 2026-08-12T02:38:24+08:00 - Strict semantic review found and then verified
  closed two additional leakage defects: raw bars can no longer be passed to
  the causal evaluator, and the holdout can no longer accept a free-form
  strategy or post-hoc cash/cost assumptions. `RegisteredConfiguration` freezes
  a concrete bounded `SpotStrategyConfig`; `EvaluationProtocol` freezes initial
  cash and the 1x schedule and internally returns fresh-state 1x/2x results.
- 2026-08-12T02:38:24+08:00 - Acceptance evidence: `cargo +1.89.0 test
  --locked -p crypto-trading-backtest --all-targets --all-features --
  --nocapture` passed 46 tests; strict all-target/all-feature Clippy passed with
  `-D warnings`; `cargo +1.89.0 fmt --all -- --check` and the scoped `git diff
  --check` passed. Independent semantic and acceptance reviewers both accepted
  the code seam; the latter's only remaining item was this bookkeeping close.

## Changed Files

- `rust/crates/backtest/src/lib.rs`
- `rust/crates/backtest/src/spot_data.rs`
- `rust/crates/backtest/src/sha256.rs`
- `rust/crates/backtest/src/evaluation.rs`
- `rust/crates/backtest/src/candidates.rs`
- `rust/crates/backtest/tests/spot_data_contract.rs`
- `rust/crates/backtest/tests/evaluation_contract.rs`
- `rust/crates/backtest/tests/candidate_adapters_contract.rs`
- `docs/research/evaluation-seam-2026-08-12.md`
- `docs/automation/goal-board.md`
- `docs/automation/handoffs/issue-g-004-handoff.md`

## Decisions and Exact Verification

- Kept the existing same-event quote engine unchanged and added one pure Spot
  bar seam, avoiding a semantic rewrite of previously verified tests.
- Added no dependency; the decompressed-content SHA-256 implementation is local
  and verified against a known vector.
- Kept perpetual evaluation fail closed because margin, maintenance,
  liquidation, funding, and contract-multiplier truth are absent.
- Recorded one independent hand-worked round trip: `1000` initial cash, buy at
  adverse `100.2`, sell at adverse `109.78`, costs `0.62998`, ending equity
  `1009.37002`.
- Red proof: the focused final-holdout contract failed to compile while
  `EvaluationProtocol` and `CostSensitivityEvaluation` were absent.
- Green proof: the focused test, all 11 evaluation contracts, all 46 backtest
  tests, strict Clippy, formatting, and scoped diff checks passed.

## Risks and Next Step

- Daily OHLCV has no executable historical spread, depth, queue, or private fee
  truth. G-005 must treat declared costs as proxies and report both 1x and 2x.
- The type-state boundary gates the normal runner but cannot prevent a caller
  from deliberately retaining a separate copy of raw data. G-005 must record
  exact execution order and stop all tuning after the single holdout opening.
- Selection/holdout protocol equality is now representable but must be recorded
  by the G-005 machine-readable report; the experiment driver must use the same
  `EvaluationProtocol` for every OOS and final-holdout evaluation.
- G-004 is complete. G-005 is claimed and must build only thin offline
  orchestration, freeze its registry before final holdout, and honestly report
  that no candidate passed if the pre-registered threshold is missed.
