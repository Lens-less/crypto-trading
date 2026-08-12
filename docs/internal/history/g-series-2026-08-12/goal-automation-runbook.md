# Goal Automation Runbook — Trading Safety and Strategy Evaluation

## Source Documents

- `README.md`
- `docs/runbooks/production-candidate.md`
- `docs/automation/goal-board.md`
- `docs/automation/handoffs/issue-g-001-handoff.md`
- `rust/crates/runtime/src/capability.rs`
- the current `git diff` and all repository test contracts

No `CONTEXT.md` or `docs/adr/*.md` file was present when this runbook was
generated. If one appears, read it before editing.

## Non-Negotiable Safety Boundary

- Never enable mainnet, weaken `live_trading_enabled = false`, accept mainnet
  credentials, or remove the current live fail-closed gates.
- Unattended work may use offline replay, paper execution, deterministic mocks,
  and public read-only market data only.
- Do not send any external order or cancel, including Testnet, while the user is
  asleep. Credentialed Testnet lifecycle/reconciliation/24-hour soak evidence
  remains an explicit human-supervised release gate.
- Never expose, print, persist, or search broadly for secrets.
- Do not claim profitability from in-sample results, sparse virtual fills, or a
  derivatives backtest without margin, liquidation, fees, slippage, and funding.
- Preserve the dirty worktree. Do not use reset, checkout-discard, mass rewrite,
  or destructive cleanup.
- No new dependencies unless the user later explicitly authorizes them.

## Operating Model

The board is the source of truth. Claim one issue, execute it, verify it, update
its handoff, then advance to the next dependency. Default to a fresh Goal worker
per issue. The single-Goal sleep prompt below is an explicit user-requested
exception: its root Goal may execute all issues sequentially and use bounded
native subagents for independent review/test lanes.

Do not create per-issue recurring worker automations or worker-automation
fan-out. If a continuation handoff is missing or unusable, mark the issue
blocked and do not launch a continuation until repository evidence reconstructs
a valid handoff.

Use this sentence in every worker Goal:

> Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.

## Issue G-001 — Recover and Finish Interrupted Trade-Safety Changes

Blocked by: none.

What to build: recover the current partially edited worktree, finish the
authoritative Binance Spot/USD-M `exchangeInfo` rule path and the conservative
buying-power/batch-risk path, and restore a green targeted baseline.

Acceptance criteria:

- Spot/USD-M symbol rules are loaded from the correct public Testnet
  `exchangeInfo` endpoint before any mutation and exact-match the requested
  product/symbol/status.
- `PRICE_FILTER`, `LOT_SIZE`, `MARKET_LOT_SIZE`, `MIN_NOTIONAL`, and `NOTIONAL`
  semantics—including optional max bounds and market-application flags—are
  represented and tested; missing/invalid/unknown required metadata fails
  closed before submit.
- No static permissive fallback can place a Testnet order when metadata fetch or
  parsing fails.
- Risk authorization enforces conservative buying power, accumulates batch
  openings, handles reductions/cross-flat correctly, and forbids opening Spot
  shorts.
- Continuous and one-shot callers provide honest account budgets.
- Targeted exchange, strategy risk, arbitrage task, command, and lifecycle tests
  pass; affected-crate clippy passes with `-D warnings`.

One-shot Goal prompt:

```text
Goal: Complete Issue 1 — G-001 interrupted trade-safety recovery
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: complete issue G-001 in C:\Users\28340\Desktop\crypto-trading by recovering the interrupted authoritative Binance-rule and buying-power implementations already present in the dirty worktree. Do not revert or overwrite other audit fixes.
Read first: docs/automation/goal-board.md, docs/automation/handoffs/issue-g-001-handoff.md, docs/automation/goal-automation-runbook.md, README.md, docs/runbooks/production-candidate.md, and any applicable AGENTS.md.
Issue boundary: only finish/integrate the two interrupted safety slices and the tests/docs directly required for them. Out of scope: new strategy research, mainnet enablement, external order/cancel calls, new dependencies, broad refactors, and commits/pushes.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Start by rereading the current diff and running targeted compile/tests; current output overrides the old handoff. Follow red-green-refactor. Validate official Binance filter semantics against primary Binance documentation. Fail closed on uncertainty and never mutate an exchange during verification.
Verification: run all targeted exchange/strategy/apps contracts named in the acceptance criteria, cargo fmt --check for the workspace or affected files, and clippy -D warnings for affected crates. Record exact commands/results.
Board: compare-before-write a G-001 claim, heartbeat it, update docs/automation/handoffs/issue-g-001-handoff.md, mark done only when every criterion is evidenced, then clear the claim.
Final report: changed files, fixed defects, red/green evidence, remaining risks, and the next eligible issue. Do not claim completion while any targeted test or compiler check fails.
```

## Issue G-002 — Review the Integrated Safety Diff and Run Engineering Gates

Blocked by: G-001.

What to build: review every uncommitted change for safety regressions, simplify
only where behavior is protected, close any evidence-backed defect, and run the
full engineering gate set.

Acceptance criteria:

- The diff preserves mainnet fail-closed behavior and no Testnet mutation occurs
  in tests.
- Reconciliation, lifecycle recovery, arbitrage identity, risk replay,
  instrument rules, rate-limit isolation, and capability claims are internally
  consistent.
- `cargo +1.89.0 fmt --all -- --check`, workspace check, clippy with
  `-D warnings`, all workspace/all-target/all-feature tests, doc tests, and
  release build pass with `--locked` where supported.
- Frontend frozen install, lint, typecheck, tests, build, and dependency audit
  pass; Playwright runs if the installed environment supports it, otherwise the
  exact missing prerequisite is recorded.
- `cargo audit`, `git diff --check`, fixture contracts, and a secret-pattern scan
  pass without exposing values.

One-shot Goal prompt:

```text
Goal: Complete Issue 2 — G-002 integrated safety review and gates
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: complete G-002 by reviewing the entire current trade-safety diff and making the repository fully green without expanding trading authority.
Read first: docs/automation/goal-board.md, the G-001 handoff, this runbook, README.md, docs/runbooks/production-candidate.md, the current git diff, and applicable AGENTS.md.
Issue boundary: diff review, evidence-backed fixes, simplification protected by tests, and full repository verification. Out of scope: strategy hunting/optimization, external exchange mutations, mainnet, dependencies, commits, and unrelated redesign.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Review before editing. Add regression tests before any newly discovered fix. Run the complete gate list in the acceptance criteria, read every failure, and iterate until green or a genuine external prerequisite is proven.
Board: claim G-002 with compare-before-write, heartbeat it, write docs/automation/handoffs/issue-g-002-handoff.md, and mark done only with exact evidence.
Final report: findings fixed, files changed, every command/result, remaining external gates, and next issue. Never suppress a failing test or weaken a safety contract to get green.
```

## Issue G-003 — Research Current Strategy Candidates

Blocked by: G-002.

What to build: a dated, cited shortlist of three to five candidate strategy
families that can be honestly evaluated with available or obtainable public
data. Include simple baselines and reject candidates that require unavailable
L2 queue, borrow, liquidation, or funding truth.

Acceptance criteria:

- Research uses current primary sources: papers/preprints, exchange/data-provider
  documentation, and original repositories; record access/publication dates.
- Each candidate states hypothesis, instruments, cadence, required data,
  execution assumptions, turnover/capacity risk, failure regimes, and falsifying
  test.
- At least one simple trend/buy-and-hold baseline is included.
- No candidate is called profitable before out-of-sample evaluation.
- Output is saved as `docs/research/strategy-candidates-2026-08-12.md`.

One-shot Goal prompt:

```text
Goal: Complete Issue 3 — G-003 current strategy research
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: complete G-003 by researching and ranking 3-5 current, testable crypto strategy families plus simple baselines, using high-trust primary sources current to the actual run date.
Read first: docs/automation/goal-board.md, G-002 handoff, this runbook, README.md, runtime capability manifest, backtest/scanner limitations, and applicable AGENTS.md.
Issue boundary: research, feasibility assessment, and a dated Markdown artifact. Out of scope: coding strategies, parameter optimization, claims of profitability, mainnet/Testnet mutation, paid/private data, and new dependencies.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Browse because “latest” is time-sensitive. Prefer papers, official exchange/data docs, and original source repositories. Explicitly reject strategies whose execution truth cannot be modeled. Save citations and a falsification plan.
Verification: every factual current claim has a direct primary-source link; every candidate maps to available data and a credible evaluation seam.
Board: claim/heartbeat G-003, write its handoff, and mark done only after the research artifact satisfies all criteria.
Final report: candidate ranking, rejected families and why, data gaps, and recommended candidate(s) for G-004. Research evidence is not investment advice or a production trading claim.
```

## Issue G-004 — Build a Leakage-Resistant, Cost-Aware Evaluation Seam

Blocked by: G-003.

What to build: the smallest reusable evaluation path needed for the selected
candidates, with deterministic data provenance, walk-forward splits, realistic
cost sensitivity, and benchmark comparison.

Acceptance criteria:

- Raw data provenance, venue/product, timestamps, missing intervals, timezone,
  and checksum are recorded; incomplete data fails closed.
- No look-ahead, survivorship, label, or train/test leakage; use purged or
  embargoed walk-forward evaluation where signals overlap.
- Fees, bid/ask, slippage/latency proxy, turnover, and at least 1x/2x cost
  sensitivity are modeled.
- Perpetual strategies remain rejected unless a tested margin, maintenance,
  liquidation, funding, and contract-size model is implemented. Prefer Spot for
  this unattended cycle.
- Deterministic tests prove split boundaries, cost accounting, and rejection of
  unsupported data/markets.

One-shot Goal prompt:

```text
Goal: Complete Issue 4 — G-004 credible evaluation seam
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: complete G-004 by implementing the minimum credible evaluation seam for the G-003 shortlist, favoring Spot and preserving every existing fail-closed boundary.
Read first: docs/automation/goal-board.md, G-003 research/handoff, this runbook, backtest source/tests, capability manifest, and applicable AGENTS.md.
Issue boundary: data ingestion/provenance, leakage-resistant splits, costs, benchmarks, deterministic evaluation tests, and only the smallest strategy adapters needed. Out of scope: live/Testnet orders, mainnet, dashboard redesign, unbounded optimization, new dependencies, and weakening perpetual rejection.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Write red tests first. Reuse existing types and avoid broad abstractions. Treat missing/ambiguous market data as an error. Do not implement derivatives evaluation unless all required financial mechanics are covered and tested.
Verification: targeted tests plus relevant crate/full workspace checks; reproduce one tiny hand-worked ledger/split example exactly; document dataset checksum and assumptions.
Board: claim/heartbeat G-004, update its handoff, and mark done only with code/test/data evidence.
Final report: files, model assumptions, tests, known model risk, and exact experiment entry point for G-005.
```

## Issue G-005 — Run Bounded Experiments and Select or Reject Candidates

Blocked by: G-004.

What to build: reproducible experiment artifacts for the shortlisted candidates,
with a hard search budget and an untouched final holdout.

Acceptance criteria:

- Pre-register parameter ranges and metrics before reading final holdout results;
  evaluate no more than five families and no more than twenty configurations per
  family in this cycle.
- Report net return, volatility, Sharpe/Sortino with uncertainty, max drawdown,
  turnover, profit factor, trade count, exposure, regime/window stability,
  benchmark delta, and 1x/2x cost sensitivity.
- A candidate is merely `promising` only if the untouched out-of-sample result
  is positive after costs, median OOS Sharpe is at least 1.0, profit factor at
  least 1.2, max drawdown no worse than 20%, at least 60% of walk-forward
  windows are positive, and performance remains positive at 2x modeled costs.
- Do not retune after viewing the final holdout. If none pass, conclude that no
  candidate passed; do not p-hack or keep searching.
- Save machine-readable results and a dated Markdown report under
  `artifacts/strategy-evaluation/` and `docs/research/` without committing large
  raw datasets.

One-shot Goal prompt:

```text
Goal: Complete Issue 5 — G-005 bounded strategy experiments
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: complete G-005 by running the pre-registered bounded offline/paper experiments from G-004 and honestly selecting or rejecting every candidate.
Read first: docs/automation/goal-board.md, G-003/G-004 artifacts and handoffs, this runbook, dataset provenance, and applicable AGENTS.md.
Issue boundary: deterministic offline experiments, statistical/cost robustness analysis, artifacts, and conclusions. Out of scope: external orders/cancels, mainnet/Testnet mutation, post-holdout retuning, unlimited searches, fabricated data, and profitability promises.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Freeze the search plan before final holdout evaluation. Use independent subagents only for reproducibility review or result validation, not extra unregistered parameter searches. Negative results are valid.
Verification: rerun best and baseline configurations from a clean deterministic command; validate checksums; confirm all metrics are net of modeled costs and no unsupported derivative path ran.
Board: claim/heartbeat G-005, update its handoff, and mark done only after reproducibility evidence and honest pass/fail decisions exist.
Final report: exact commands, dataset/window, assumptions, metrics, sensitivity, candidate disposition, and why results do or do not justify further paper observation. Never call this production-ready.
```

## Issue G-006 — Final Verification and Release-Readiness Report

Blocked by: G-005.

What to build: a final evidence-backed report that separates repository quality,
research evidence, paper readiness, Testnet release gates, and mainnet blockers.

Acceptance criteria:

- Re-run affected and full engineering gates after all strategy changes.
- `git diff --check`, dependency audits, fixture contracts, and deterministic
  experiment reproduction pass.
- Document changed files, simplifications, fixed defects, strategy findings,
  model limitations, and all unverified external evidence.
- Mainnet remains unavailable. Credentialed Testnet lifecycle/reconciliation and
  a 24-hour soak are explicitly `not run` unless a human supervised them.

One-shot Goal prompt:

```text
Goal: Complete Issue 6 — G-006 final verification and report
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: complete G-006 by independently verifying the final worktree and producing an honest release-readiness/research report.
Read first: the board, every issue handoff/artifact, this runbook, README.md, production runbook, capability manifest, current diff, and applicable AGENTS.md.
Issue boundary: final review, verification, documentation, and risk classification. Out of scope: new features, external trading mutations, mainnet enablement, credential handling, commits/pushes, and improving results after holdout.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Run the full gates and read outputs. Distinguish “tests passed,” “offline strategy looked promising,” “paper-observable,” “Testnet evidence,” and “mainnet-ready”; never collapse these levels.
Board: claim/heartbeat G-006, write its handoff, mark done only when all local criteria pass, then clear the claim and set overall phase complete or externally blocked.
Final report: outcome first, exact verification evidence, changed files/simplifications, strategy metrics with caveats, and remaining external release gates. Do not claim real-money readiness.
```

## Single-Goal Sleep Prompt

The user explicitly requested one Goal prompt that can run unattended. Paste the
following into Goal mode:

```text
在 C:\Users\28340\Desktop\crypto-trading 持续完成“交易安全收口 + 最新策略离线评估”目标，直到 docs/automation/goal-board.md 中 G-001 到 G-006 均满足验收标准，或只剩无法伪造的外部人工门禁。

先完整读取 docs/automation/goal-automation-runbook.md、docs/automation/goal-board.md、docs/automation/handoffs/issue-g-001-handoff.md、README.md、docs/runbooks/production-candidate.md、当前 git diff，以及遇到的所有 AGENTS.md。当前工作树包含大量已经验证的安全修复和两条被中断的半成品；不得 reset、checkout 丢弃、覆盖他人改动或从头重做。先认领 G-001，依据当前编译/测试输出接管并完成 Binance 权威 exchangeInfo 规则和保守资金购买力控制；随后严格按依赖顺序推进 G-002 至 G-006。每完成一项就更新 board、claim、heartbeat 和对应 handoff，记录文件、决策、命令、结果、风险与下一步。

Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly. 可使用原生子代理并行做互不冲突的审查、研究、测试和验证，但根 Goal 负责整合；不要创建每个 issue 的循环 automation，不要重复启动已有 fresh claim 的 worker。

必须自主推进：先读证据，再红测，再最小修复，再运行定向与全量验证；失败就诊断并继续，不因一次失败停下，不为了变绿而删除测试、放宽风控、吞掉错误或修改验收标准。不得新增依赖。未经用户另行明确授权，不提交、不推送、不建 PR。

绝对安全边界：保持 live_trading_enabled=false 和所有 mainnet fail-closed 门禁；不得接受或使用 mainnet 凭证；用户睡眠期间不得向任何外部交易所发送订单或撤单（包括 Testnet）。只允许离线回放、paper、确定性 mock 和公开只读行情。Testnet 凭证生命周期、真实对账和 24 小时 soak 若无人工监督，必须如实记为外部门禁，不能伪造或用 mainnet 替代。任何密钥不得输出或落盘。

“最新策略”必须联网核查截至实际运行日的高可信一手来源（论文/预印本、交易所或数据方官方文档、原始仓库），先产出有引用的 3–5 个候选和简单基线，再只为数据与执行假设可信的候选建立评估。优先 Spot；现有永续回测在没有真实保证金、维持保证金、强平、资金费率和合约乘数模型时必须继续 fail closed。禁止把 VirtualGrid 跳空跨级即成交、样本内收益或无成本结果当作实盘证据。

实验必须防未来函数、幸存者偏差和训练/测试泄漏，使用预注册参数范围、walk-forward/embargo、独立最终 holdout、真实费率/点差/滑点/延迟代理、1x/2x 成本敏感性及基准。单轮最多 5 个策略族、每族最多 20 个配置；查看最终 holdout 后不得再调参。只有未触碰 holdout 在扣费后为正、OOS 中位 Sharpe>=1.0、profit factor>=1.2、max drawdown<=20%、至少 60% walk-forward 窗口为正且 2x 成本下仍为正，才能标记为“promising”；这仍不是实盘就绪或收益承诺。若没有候选通过，必须诚实结论“没有通过”，不要继续 p-hack。

最终必须运行并读取：Rust fmt/check/clippy(-D warnings)/workspace all-target all-feature tests/doc tests/release build（locked）、fixture contracts、cargo audit、前端 frozen install/lint/typecheck/test/build/audit（环境允许则 Playwright）、git diff --check、秘密模式扫描，以及最佳策略和基线的确定性复现。完成报告要区分：代码质量通过、离线研究 promising、paper 可观察、Testnet 有证据、mainnet 就绪；最后一项在当前架构下必须保持否。只有所有本地验收标准有证据且已把外部门禁如实列出时，才结束 Goal。
```

## Recurring Controller Automation Prompt

Use this only if a recurring controller is configured. It coordinates and does
not implement code:

```text
Every 10 minutes, read C:\Users\28340\Desktop\crypto-trading\docs\automation\goal-board.md and the active issue handoff. Coordinate only; do not implement code in this recurring controller thread.

Reread the board immediately before claiming. If a fresh claim exists, do not launch a duplicate; inspect the worker if thread tools exist and update last_heartbeat from real progress. Treat a claim as stale only after two missed checks, an uninspectable thread, or explicit context-loss evidence. If duplicate workers exist, keep the worker matching claim_token canonical and require evidence merge from superseded workers.

When an eligible issue has no fresh claim, write a unique claim_token, claimed_at, claimed_by_thread, last_heartbeat, and increment attempt_count before launch. Create a fresh Goal worker from that issue’s one-shot prompt without forking controller history. Never create per-issue recurring automations or fan-out workers. If thread creation/inspection is unavailable, mark the issue awaiting_manual_launch and write the exact one-shot prompt instead of implementing it.

For continuation, require a usable non-stale handoff plus board and source docs. If missing/contradictory, mark blocked and request recovery evidence. Mark done only when every acceptance criterion has exact verification evidence. Record genuine blockers. Pause when all issues are done or only human-supervised external Testnet gates remain. Never enable mainnet or send external orders/cancels.
```

## Stop and Pause Conditions

- Stop successfully when G-001 through G-006 are done with evidence.
- Pause, do not improvise, for missing authority that would enable mainnet,
  expose credentials, create external orders/cancels, purchase data, add a new
  dependency, or make an irreversible external change.
- Treat unavailable human-supervised Testnet evidence as a documented release
  blocker, not a reason to weaken the gate.
- If the same non-external blocker survives three evidence-backed recovery
  attempts, write a precise handoff and mark the issue blocked.
