# G-005 Handoff - Bounded Offline Strategy Experiments

## Current Status

- Status: done; accepted locally and handed to G-006
- Claim token: cleared at 2026-08-12T03:19:41+08:00
- Claimed at: 2026-08-12T02:38:24+08:00
- Claimed by thread: `019ff1a7-229a-71d1-94c6-548f93748f08`
- Last heartbeat: 2026-08-12T03:19:41+08:00
- Completed at: 2026-08-12T03:19:41+08:00
- Attempt: 1
- Planned outputs:
  - `artifacts/strategy-evaluation/`
  - `docs/research/strategy-evaluation-2026-08-12.md`

## Source Documents

- `docs/automation/goal-automation-runbook.md`
- `docs/automation/goal-board.md`
- `docs/automation/handoffs/issue-g-003-handoff.md`
- `docs/automation/handoffs/issue-g-004-handoff.md`
- `docs/research/strategy-candidates-2026-08-12.md`
- `docs/research/evaluation-seam-2026-08-12.md`
- `.workflow/ultracode/trading-safety-strategy-evaluation-20260812/eval-contract.md`

## Scope and Acceptance

- Freeze the exact BTCUSDT Spot dataset provenance, split boundaries, embargo,
  terminal holdout, initial cash, component costs, baselines, candidate
  registry, selection rule, metrics, and promotion threshold before the final
  holdout is evaluated.
- Evaluate at most the three pre-registered candidate families and the two
  mandatory baselines. Never exceed five families or twenty configurations per
  family and never add a family after inspecting results.
- Report net return, volatility, Sharpe/Sortino with uncertainty, drawdown,
  turnover, profit factor, trade count, exposure, window/regime stability,
  benchmark delta, and componentwise 1x/2x sensitivity.
- Open the final holdout exactly once after selection is frozen. Do not retune,
  pivot, rerank on holdout, or inspect additional periods afterward.
- Mark a candidate `promising` only if every conjunctive runbook threshold is
  satisfied; otherwise record `no candidate passed` without further search.
- Save deterministic machine-readable output and a dated Markdown report, and
  reproduce the selected candidate and mandatory baselines with an exact locked
  offline command.

## Safety Boundary

- `live_trading_enabled=false`; mainnet remains unavailable.
- No credentials and no external order or cancel, including Testnet.
- Only public read-only official data retrieval and offline evaluation are in
  scope. No raw price data or secrets may be persisted in reports.
- Perpetual, short, borrow, funding, liquidation, maker, resting-limit, and L2
  execution paths remain fail closed.
- No dependency addition, commit, push, PR, or large raw dataset in the repo.

## Worker Log

- 2026-08-12T02:38:24+08:00 - G-004 passed 46 backtest tests, strict Clippy,
  formatting, scoped diff checks, and two independent reviews. Its claim was
  cleared before this claim was written.
- 2026-08-12T02:38:24+08:00 - G-005 was claimed with the final holdout still
  unopened. Root began independent read-only audits for the frozen experiment
  protocol, official multi-archive provenance, minimal runner seam, and public
  test plan. These lanes may not inspect price results or propose additional
  searches.
- 2026-08-12T02:47:46+08:00 - Four read-only audits were reconciled without
  reading price results. Protocol `g005-btcusdt-spot-20260812-v1` is frozen in
  `docs/research/strategy-evaluation-preregistration-2026-08-12.md`: 103 full
  monthly archives from 2018-01 through 2026-07, 3134 expected bars, nine
  1095/1/182 OOS windows, one 365-day holdout, `10/2/4/4` bps 1x costs, 22
  configurations, deterministic window bootstrap, one winner per candidate
  family, and the exact conjunctive promotion rule. No raw market row or
  strategy metric was inspected before this freeze.
- 2026-08-12T02:53:26+08:00 - The public read-only preparation script fetched
  and verified all 103 official monthly ZIPs and sibling checksums into
  `C:\Users\28340\AppData\Local\Temp\crypto-trading-g005-btcusdt-v1`.
  The small repository lock records 103 unique ZIP/content digests, 3134 total
  bars, the exact millisecond-to-microsecond transition, and SHA-256
  `5eb95ab4efeddc2656c6cd2863a48a50c685758ef458a5102bcc64c5047c2d3f`.
  No price row or strategy result was printed or inspected. The verified merge
  seam and weekly volatility-target cadence regressions are now green.
- 2026-08-12T03:09:50+08:00 - Composite ordered provenance, immutable plan and
  dataset fingerprints, exact 22-configuration budget enforcement,
  deterministic seeded bootstrap, pre-holdout family selection, consuming
  single-use holdout evaluation, and conjunctive `promising` decisions are
  covered by public contracts. A decimal-division regression was fixed by
  conservatively stepping unaffordable quantities down, and unchanged targets
  now produce no rebalance; this prevents buy-and-hold dust churn without
  weakening the ledger. All 60 backtest tests and strict all-target/all-feature
  Clippy pass. No real selection or holdout result has been opened.
- 2026-08-12T03:19:41+08:00 - After independent semantic review passed,
  the locked offline runner persisted the complete selection JSON before the
  consuming holdout transition, then evaluated only cash, buy-and-hold, and
  the three preselected family winners. Repeated exact commands produced
  byte-identical selection (`579ba052...c7e2`), final results
  (`89b18ab6...0cff`), and Markdown (`d0948ada...6ddf`). All three candidate
  winners were negative at 1x and 2x costs and failed the conjunctive rule;
  `docs/research/strategy-evaluation-2026-08-12.md` records the required
  conclusion: no candidate passed. The search is closed without retuning.

## Risks and Next Step

- Daily klines do not provide historical executable spread/depth, private fee,
  or capacity truth. Costs remain declared conservative proxies and capacity is
  unproven.
- Fractional quantities intentionally do not claim historical lot-size truth;
  current `exchangeInfo` is never projected backward into the sample.
- Next: G-006 must rerun the full repository gates, audit the complete diff and
  generated artifacts, and classify code quality, offline research, Paper,
  Testnet, and mainnet readiness separately. No strategy or protocol tuning is
  permitted after this handoff.
