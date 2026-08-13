# Goal Automation Runbook — Live Trading V1

> **Status (2026-08-13, `live-v1` branch).** Several deliverables tracked by
> this runbook are now implemented: authority-typed Binance Spot mainnet
> endpoints (separate read and trade types), credential separation
> (`BINANCE_MAINNET_READ_API_KEY/SECRET`, `BINANCE_MAINNET_TRADE_API_KEY/SECRET`;
> `BINANCE_API_KEY/SECRET` remains Testnet-only), the read-only
> `live-reconcile` report, the operator-acknowledged one-shot `live-lifecycle`
> mainnet order owner, and the capability promotion to
> `release_stage: live-manual`. Still open: the credentialed external evidence
> gates (real mainnet shadow observation and a supervised lifecycle run with
> archived redacted evidence) and the strategy promotion gate — autonomous
> strategy live execution remains unavailable. The body below is preserved as
> written on 2026-08-12 and is not updated to match later code.

## Source Documents

- `docs/internal/specs/LIVE_TRADING_V1_SPEC.md` — normative product and safety spec
- `docs/automation/live-trading-v1/goal-board.md` — execution source of truth
- `docs/automation/live-trading-v1/handoffs/README.md` — handoff contract
- `docs/reports/release-readiness-2026-08-12.md` — current evidence baseline
- `docs/runbooks/production-candidate.md` — existing supervised Testnet gates
- `docs/adapter-support.md` — current adapter authority matrix
- `README.md` and `rust/README.md`
- `rust/crates/runtime/src/capability.rs`
- `rust/crates/exchange/src/lib.rs`
- the current `git diff` and applicable `AGENTS.md`

No `CONTEXT.md` or `docs/adr/*.md` existed when this runbook was generated. If
either appears, every new worker must read it before editing.

## Operating Model

The controller coordinates. Fresh one-shot Goal sessions implement one issue at
a time. The board and handoff files carry state; workers do not depend on the
controller transcript. A worker must claim an issue immediately before launch,
heartbeat the claim while active, record exact verification evidence, and clear
the claim only when the issue is done or intentionally reset.

Do not create per-issue recurring automations. Do not fork controller history
into a worker. If thread creation is unavailable, mark the issue
`awaiting_manual_launch` and paste its exact prompt for a human to launch.

Testnet mutation, credentialed mainnet shadow, release promotion, mainnet
canary, and strategy promotion are supervised issues. The controller must not
launch them unattended.

Every worker prompt contains this execution rule:

> Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.

## Global Safety Boundary

- Preserve the dirty worktree; never reset, discard, or overwrite unrelated
  user changes.
- Do not add a dependency without explicit user authorization. If WebSocket
  support needs a new crate, document the narrow candidate and pause that issue
  for approval; do not implement WebSocket framing or TLS manually.
- No secret may be requested in chat, included in a Goal prompt, passed on a
  command line, printed, persisted, or committed.
- L-001 through L-010 and L-013 use deterministic mocks, loopback transports,
  Paper/Testnet fixtures, and public read-only data only. They must not send an
  external order or cancel.
- L-011 may mutate Binance Testnet only in a human-supervised session using
  locally injected Testnet credentials and the exact confirmations in the
  production-candidate runbook.
- L-012 may use a mainnet read-only key locally but cannot possess `TRADE`
  permission and cannot mutate the account.
- L-014 is the only issue allowed to submit a mainnet order, and only after the
  user explicitly authorizes the exact account, symbol, maximum notional, time
  window, and one-shot procedure in that session.
- L-015 cannot run until a strategy ID and separate evidence artifact exist.
- Mainnet uncertainty, foreign activity, stale state, or missing evidence fails
  closed; never weaken a test or gate to advance the board.

## Dependency Graph

```text
L-001
  ├─ L-002 ─ L-004 ─┐
  └─ L-003 ─────────┼─ L-006 ─ L-007 ─ L-008 ─ L-009 ─ L-010
            L-005 ──┘                                  │
                                                       L-011
                                                         │
                                                       L-012
                                                         │
                                                       L-013
                                                         │
                                                       L-014
                                                         │
                                                       L-015
```

## Issue L-001 — Lock Scope and Establish a Safe Baseline

Blocked by: none.

Build a reviewed implementation baseline for Binance Spot, one dedicated
account, and `BTCUSDT`; record the current dirty-worktree inventory, Cargo
dependency/binary graph, authority matrix, configuration decisions, and the
exact tests that protect current fail-closed behavior.

Acceptance criteria:

- The Spec defaults and all unresolved operator inputs are explicitly recorded.
- Existing G-001 through G-006 history remains untouched.
- A live/verify/research module map identifies keep, exclude, and archive
  candidates without deleting source.
- Current capabilities still report Paper-only/live unavailable.
- Baseline targeted and workspace gate commands/results are recorded.
- No external authenticated call or new dependency occurs.

One-shot Goal prompt:

```text
Goal: Complete L-001 — lock the Live Trading V1 scope and establish a safe baseline.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: in <repo-root>, turn docs/internal/specs/LIVE_TRADING_V1_SPEC.md into an evidence-backed implementation baseline without widening trading authority.
Read first: docs/internal/specs/LIVE_TRADING_V1_SPEC.md, docs/automation/live-trading-v1/goal-board.md, this runbook, docs/reports/release-readiness-2026-08-12.md, docs/adapter-support.md, current git diff/status, Cargo manifests, and applicable AGENTS.md.
Issue boundary: scope lock, dirty-worktree inventory, binary/dependency/module map, configuration decision record, and baseline tests only. Out of scope: adapters, WebSockets, external authenticated calls, order mutation, dependency additions, deletion, commits, and pushes.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: rerun capabilities JSON assertions, relevant existing capability/config contracts, cargo metadata/tree evidence, git diff --check, and any narrow documentation validation. Prove mainnet stays unavailable.
Board: compare-before-write a unique L-001 claim, heartbeat it, write docs/automation/live-trading-v1/handoffs/issue-l-001-handoff.md, and mark done only when every criterion has exact evidence.
Final report: outcome first, inspected/changed files, baseline commands/results, decisions, unresolved operator inputs, risks, and the next eligible issue.
```

## Issue L-002 — Add a Mainnet Read-Only Shadow Adapter

Blocked by: L-001.

Add explicit Binance mainnet read authority with exact-host validation,
credential separation/redaction, public instrument metadata, signed account
balances, open orders, and exact-order query. It must be impossible for this
adapter/build to submit or cancel.

Acceptance criteria:

- Testnet, mainnet-read, and mainnet-trade types and endpoints cannot be mixed.
- Only official mainnet Spot hosts or explicit loopback mocks are admitted.
- Read credentials use dedicated configuration and are redacted through all
  error/debug/telemetry paths.
- Stable account/open-order snapshots have monotonic observation watermarks.
- Compile-fail, API-surface, or equivalent tests prove no mutation method is
  reachable from read authority.
- Deterministic signature, response-bound, timeout, clock, rate-limit, and
  malformed-response contracts pass without real credentials.

One-shot Goal prompt:

```text
Goal: Complete L-002 — implement the authority-safe Binance Spot mainnet read-only shadow adapter.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: add mainnet public/account/open-order/query observation while making trading mutation unconstructable from read-only authority.
Read first: the Live V1 Spec sections 6, 8, 10, and 21; the board; L-001 handoff; this runbook; existing endpoint, Binance Testnet protocol/adapter, secret config, and exchange contracts; official Binance Spot REST security/current endpoint documentation; applicable AGENTS.md.
Issue boundary: explicit endpoints/types, read credentials, metadata, signed balance/open-order/exact-order reads, redaction, and deterministic tests. Out of scope: WebSockets, submit/cancel, live owner, external authenticated calls, new dependencies, UI, pruning, and capability promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: red tests first; run affected config/exchange contracts, sentinel-secret scans, fmt/check/strict clippy for affected crates, and prove capabilities still deny live trading.
Board: claim/heartbeat L-002, update docs/automation/live-trading-v1/handoffs/issue-l-002-handoff.md, and mark done only with exact evidence.
Final report: authority boundary, changed files, tests/commands, official-doc assumptions, remaining gaps, and next eligible issue.
```

## Issue L-003 — Add a Production Market Stream

Blocked by: L-001.

Implement a bounded Binance Spot `bookTicker` stream with connection
generation, freshness, ping/pong, planned rotation/reconnect, backoff, and
degradation semantics. Add diff depth only if separately justified; do not
pretend a ticker stream is a full order book.

Acceptance criteria:

- Exact-symbol subscription and acknowledgement are validated.
- Event and local receive provenance plus connection generation are retained.
- Disconnect, stale age, decode burst, queue overflow, and sequence/generation
  uncertainty immediately make the feed non-tradable.
- Reconnect uses bounded backoff and never replays stale cached readiness.
- Deterministic fake-server tests cover ping, close, reconnect, malformed and
  oversized messages, overflow, and time control.
- If no approved WebSocket crate exists, the issue pauses with a dependency
  decision; no custom protocol/TLS implementation is introduced.

One-shot Goal prompt:

```text
Goal: Complete L-003 — implement a bounded, freshness-aware Binance Spot market stream.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: provide a production-quality BTCUSDT book-ticker subscription whose readiness fails closed on every gap, stale interval, overflow, disconnect, or reconnect generation change.
Read first: Live V1 Spec sections 6, 9, 15, and 18; board; L-001 handoff; this runbook; ExchangeHandle subscription types; existing public adapters; official Binance Spot WebSocket stream documentation; applicable AGENTS.md.
Issue boundary: public market WebSocket transport, parser, bounded queue, lifecycle, freshness/readiness, and deterministic fake-server tests. Out of scope: private stream, signed REST, order mutation, full L2 unless explicitly approved, strategies, UI redesign, and capability promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Dependency rule: reuse an approved existing dependency. If none exists, document the narrow dependency choice and stop for explicit approval; never hand-roll WebSocket framing or TLS.
Verification: run focused stream/parser/time/backpressure tests, affected crate fmt/check/strict clippy, and a bounded public read-only smoke only if deterministic tests are already green.
Board: claim/heartbeat L-003, write its handoff, and mark done only when every feed failure makes readiness false with evidence.
Final report: changed files, connection/freshness model, commands/results, dependency decision, resource bounds, risks, and next issue.
```

## Issue L-004 — Add User Data Stream and Reconciliation

Blocked by: L-002.

Consume Binance Spot balance and `executionReport` events, deduplicate/reorder
them safely, and reconcile stream state against fresh signed REST snapshots.
Real credential use is deferred to L-012.

Acceptance criteria:

- Subscription authentication and every required event field are bounded and
  parsed without exposing credentials.
- Duplicate, delayed, out-of-order, conditional, partial-fill, and terminal
  events are idempotent and cannot regress cumulative state.
- Disconnect or stream termination blocks admission immediately.
- Startup/reconnect/ambiguity reconciliation compares balances, owned open
  orders, and unresolved IDs before readiness.
- Foreign/unowned orders fail closed without automatic cancellation/adoption.
- Deterministic stream + REST race tests cover gap and recovery behavior.

One-shot Goal prompt:

```text
Goal: Complete L-004 — implement the Binance Spot User Data Stream and authoritative reconciliation loop.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: build a private-event projection for balances and execution reports that becomes ready only after a stable REST reconciliation and fails closed on stream uncertainty.
Read first: Live V1 Spec sections 6, 10, 12, and 18; board; L-002 handoff; this runbook; existing Testnet reconciliation and journal event models; official Binance User Data Stream and REST docs; applicable AGENTS.md.
Issue boundary: private subscription/auth model, bounded event parsing, dedupe/order rules, stream lifecycle, stable REST reconcile, and deterministic mock tests. Out of scope: real credentials, submit/cancel, risk admission, strategy, pruning, deployment, and live promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: red tests for duplicate/reordered/partial/terminal events and disconnect races; affected tests plus fmt/check/strict clippy; sentinel-secret scan; prove an unreconciled or foreign order state is not ready.
Board: claim/heartbeat L-004, update its handoff, and mark done only with state-transition and reconciliation evidence.
Final report: event model, changed files, tests/commands, uncertainty behavior, remaining external evidence, and next eligible issue.
```

## Issue L-005 — Implement the Gated Mainnet Trade Protocol

Blocked by: L-002.

Implement Spot limit submit/query/cancel protocol and mainnet trade authority
types behind a build/runtime gate that remains disabled. All verification uses
loopback/mock transports; no mainnet or Testnet mutation occurs.

Acceptance criteria:

- Only `LIMIT` and approved `LIMIT_MAKER` intents are representable in V1.
- Intent/client identity is exact, durable, and bound to account/product/symbol.
- Current `exchangeInfo` rules are enforced before submit.
- Timeout, connection loss after dispatch, 5xx, and `-1007` become unknown
  outcome; a quarantine prohibits submit until query/reconcile advances.
- Clock skew and 429/418 `Retry-After` handling are bounded and durable.
- Mainnet hosts are unreachable from Testnet authority and vice versa.
- Capability output and production commands still expose no live mutation.

One-shot Goal prompt:

```text
Goal: Complete L-005 — implement the Binance Spot mainnet trade protocol behind a disabled promotion gate.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: add fully tested limit submit/query/cancel request and response contracts while preserving zero reachable mainnet mutation in the current build/runtime.
Read first: Live V1 Spec sections 6, 8, 11, 12, and 18; board; L-002 handoff; this runbook; Binance Testnet protocol/adapter/lifecycle recovery; bounded exchange wrapper; official Binance REST trading/general docs; applicable AGENTS.md.
Issue boundary: authority types, intent/client identity, filters, signed submit/query/cancel protocol, error classification, quarantine, rate/clock handling, and deterministic transport tests. Out of scope: any external order/cancel, live owner, account risk, UI, pruning, strategy, and enabling live capability.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: test-first protocol/signature/fixture/fault contracts, affected fmt/check/strict clippy, capability assertions, endpoint-host negative tests, and a scan proving no real credential or external mutation path ran.
Board: claim/heartbeat L-005, update its handoff, and mark done only when ambiguous dispatch cannot lead to blind resubmit.
Final report: changed files, protocol authority, test matrix/commands, official assumptions, remaining runtime gates, and next issue.
```

## Issue L-006 — Build Venue-Backed Account Risk

Blocked by: L-003, L-004, and L-005.

Create the V1 account risk authority from fresh market/account truth, owned
orders, fills, fees, filters, and exact intents. Paper values and synthetic
budgets cannot authorize mainnet risk.

Acceptance criteria:

- All required live risk limits from Spec section 13 are represented,
  validated, journaled, and fail closed when absent.
- Buying power, base capacity, reservations, partial fills, and commissions are
  conservative and idempotent.
- Spot shorts, cross-zero orders, stale inputs, foreign orders, unresolved
  operations, over-limit notional/position/loss, and missing fees are rejected.
- Batch/concurrent intent races cannot exceed limits; V1 canary permits at most
  one open/in-flight order.
- Replay and reconciliation rebuild identical risk state.

One-shot Goal prompt:

```text
Goal: Complete L-006 — implement venue-backed Binance Spot account risk authority.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: replace Paper/synthetic admission for the live path with conservative decisions based only on fresh Binance market/account/order/fill truth.
Read first: Live V1 Spec sections 6, 8, 10, 12, 13, and 21; board; L-003/L-004/L-005 handoffs; this runbook; existing paper account/risk and strategy risk contracts; applicable AGENTS.md.
Issue boundary: live risk config/types, account projection, reservations, fills/fees, exact admission decisions, replay/reconcile, and tests. Out of scope: external orders, kill-switch orchestration, strategies, UI, binary pruning, and capability promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: red tests for every rejection/invariant and concurrent race; hand-work one buy, partial fill, fee, remainder cancel, and sell case; run affected workspace tests plus fmt/check/strict clippy and deterministic replay equivalence.
Board: claim/heartbeat L-006, update its handoff, and mark done only with exact risk-input/output evidence.
Final report: invariants, changed files, worked example, commands/results, conservative assumptions, remaining risks, and next issue.
```

## Issue L-007 — Implement the Journaled Live Execution Owner

Blocked by: L-005 and L-006.

Build a single-owner state machine that boots unarmed, replays, reconciles,
proves both streams fresh, journals before I/O, and recovers query-first after
crash or ambiguity. External mutation remains disabled.

Acceptance criteria:

- Single writer/account lease prevents duplicate owners.
- Startup never becomes ready before replay + reconciliation + stream freshness.
- Every submit/cancel has planned/dispatched/observed facts and exact identity.
- Crash tests at every transition recover without a duplicate submit/cancel.
- Partial fill and commission settlement are exactly once under replay.
- Bounded deadlines/queues/shutdown and `recovery_required` are explicit.
- Only an exact release permit and operator intent can arm the owner; current
  capability still refuses construction of that permit.

One-shot Goal prompt:

```text
Goal: Complete L-007 — implement the durable single-owner live execution state machine without enabling external mutation.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: connect the gated trade protocol and venue-backed risk authority through a journal-first owner that is crash/replay/reconciliation safe.
Read first: Live V1 Spec sections 6, 12, 14, 17, and 21; board; L-005/L-006 handoffs; this runbook; existing testnet lifecycle, paper owners, journal/history, bounded exchange and execution runtime; applicable AGENTS.md.
Issue boundary: live owner lifecycle, lease, durable facts, risk admission, submit/query/cancel orchestration, recovery, shutdown, and deterministic fault tests. Out of scope: external order/cancel, strategy loops, kill switch implementation, UI, deployment, pruning, and promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: process/fault tests kill at every state boundary, lose responses/events, duplicate/reorder events, contend writers, and replay journals; run affected/full tests plus fmt/check/strict clippy; prove no live permit is constructable.
Board: claim/heartbeat L-007, update its handoff, and mark done only when every ambiguity is query-first or recovery_required.
Final report: state machine, files, failure matrix, exact commands/results, unresolved operational risks, and next issue.
```

## Issue L-008 — Make the Kill Switch Operational

Blocked by: L-007.

Connect the latching risk switch to live admission, exact owned-order
cancellation, query-first recovery, and full reconciliation. V1 does not
automatically liquidate Spot holdings.

Acceptance criteria:

- Engagement is durable and blocks new admission before network I/O.
- Only exact owned orders are cancelled; foreign orders stop the workflow.
- Ambiguous cancels are queried; clean state requires authoritative proof that
  no owned order remains.
- A crash during kill resumes the kill workflow and cannot re-arm.
- Missing/stale account state, cancel failure, or reconciliation divergence
  ends `recovery_required` and stays latched.
- Operator status exposes progress without secrets.

One-shot Goal prompt:

```text
Goal: Complete L-008 — implement the latching live kill-switch cancellation and reconciliation workflow.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: make kill-switch engagement immediately block risk and durably drive exact owned-order cancellation plus authoritative final reconciliation.
Read first: Live V1 Spec sections 6, 14, 15, 18, and 22; board; L-007 handoff; this runbook; existing paper kill switch, trusted-submit controls, lifecycle cancellation/recovery, and journal contracts; applicable AGENTS.md.
Issue boundary: kill admission latch, owned-order enumeration/cancel/query/reconcile state machine, restart behavior, status projection, and deterministic tests. Out of scope: external mutation, automatic liquidation, strategy, deployment, pruning, and live promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: red tests for kill-before-submit races, partial fills, foreign orders, ambiguous cancel, stream loss, crash/restart, double engagement, and failed reconciliation; run affected/full tests, fmt/check/strict clippy.
Board: claim/heartbeat L-008, update its handoff, and mark done only when killed_clean requires venue proof and re-arm is impossible in-process.
Final report: workflow, changed files, failure evidence, commands/results, operator limitations, and next issue.
```

## Issue L-009 — Split Binaries and Prune the Live Artifact

Blocked by: L-007 and L-008.

Separate live, verify, and research authority. Exclude non-live modules from the
production artifact while retaining Testnet, Paper, replay, and research source
for validation.

Acceptance criteria:

- `crypto-trading-live` has one explicit Binance Spot entry point and no Paper,
  Testnet, research, scanner, alert, unused strategy, or unused venue commands.
- `crypto-trading-verify` retains Paper/Testnet/fault/reconcile/soak/restore.
- research libraries are not linked by the live binary.
- Cargo tree, binary symbols/CLI snapshots, and API route tests prove exclusion.
- Existing verification functionality remains green.
- `archive/python-legacy` disposition is documented; destructive deletion is
  not performed in this issue.

One-shot Goal prompt:

```text
Goal: Complete L-009 — split production/verification/research artifacts and remove non-live reachability from the production binary.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: create a minimal Binance Spot live artifact while preserving the repository's Testnet, Paper, replay, fault-injection, and research evidence surfaces in separate artifacts.
Read first: Live V1 Spec sections 3, 7, 16, and 21; board; L-007/L-008 handoffs; this runbook; workspace/app/web manifests, CLI/API routes, Dockerfile/compose, and applicable AGENTS.md.
Issue boundary: Cargo features/packages/binaries, command and route reachability, frontend page inclusion, deployment target selection, dependency-tree tests, and documentation. Out of scope: deleting validation source, external calls, strategy promotion, capability promotion, dependency additions, commits, and pushes.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: build/test every artifact, inspect cargo trees and CLI help/API route snapshots, run full existing Paper/Testnet regressions in verify, prove live lacks excluded reachability, and run fmt/check/strict clippy/release builds.
Board: claim/heartbeat L-009, update its handoff, and mark done only with dependency and reachability evidence.
Final report: changed files, simplifications/exclusions, artifact matrix, commands/results, retained source rationale, remaining risks, and next issue.
```

## Issue L-010 — Harden Operations, Deployment, and Observability

Blocked by: L-009.

Package the live process with mandatory authentication, redacted projections,
bounded resources, immutable evidence IDs, secure container settings, readiness
states, alertable recovery, and backup/restore procedures.

Acceptance criteria:

- Live control plane is loopback + authenticated; open-read escape hatch is not
  compiled/reachable in the live artifact.
- Health/liveness/readiness/authority are distinct and accurately projected.
- All Spec section 15 observations exist and secrets are sentinel-tested.
- Container is read-only except the private journal mount, drops capabilities,
  runs as non-root where supported, and records image/binary digest.
- Startup/shutdown, journal lease, time skew, resource bounds, log rotation,
  backup/restore, and rollback drills pass locally.
- No live capability is promoted and no external mutation occurs.

One-shot Goal prompt:

```text
Goal: Complete L-010 — harden the live operator plane, deployment artifact, observability, and recovery drills.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: make the minimal live binary operable and auditable without granting it live order authority.
Read first: Live V1 Spec sections 15, 17, 18, 20, 21, and 22; board; L-009 handoff; this runbook; current web/control-plane/deploy/runbook code and tests; applicable AGENTS.md.
Issue boundary: authenticated loopback API, truthful states/capabilities, redacted metrics/projections, resource bounds, container hardening, backup/restore/rollback/startup/shutdown drills, and tests/docs. Out of scope: external order/cancel, credentialed shadow, strategy, capability promotion, remote proxy deployment, and new dependencies without approval.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: API/auth/secret tests, browser operator flows, container/config checks, restore drill, failure-mode tests, full Rust/frontend gates, audits, diff check, and capabilities proof.
Board: claim/heartbeat L-010, update its handoff, and mark done only with exact operational evidence.
Final report: artifact/deployment changes, commands/results, screenshots only if secret-free, simplifications, remaining host assumptions, and next issue.
```

## Issue L-011 — Produce Supervised Binance Testnet Evidence

Blocked by: L-010.

Run the real credentialed Testnet lifecycle, controlled partial fill,
kill/restart, reconciliation, 24-hour soak, and backup/restore gates. This issue
requires a human-supervised session and locally injected Testnet credentials.

Acceptance criteria:

- Exact candidate binary/config hashes are captured before the first call.
- Open/cancel, partial-fill/cancel, and kill/restart campaigns pass with no
  duplicate submit and authoritative final state.
- Ambiguous outcomes, rate limits, and clock skew follow documented contracts.
- Clean account reconciliation and the complete 24-hour soak pass.
- Backup/restore reproduces the same durable projection.
- Evidence is redacted, hashed, and contains no credentials/environment dump.

One-shot Goal prompt:

```text
Goal: Complete L-011 — run the human-supervised Binance Spot Testnet release gates and produce immutable redacted evidence.
This is a one-shot supervised worker launch prompt, not a recurring automation prompt. Do not run it unattended.
Objective: execute the exact candidate's Testnet open/cancel, controlled partial-fill, kill/restart query-first recovery, account reconciliation, 24-hour soak, and backup/restore procedures.
Read first: Live V1 Spec sections 18 and 20; board; L-010 handoff; this runbook; docs/runbooks/production-candidate.md; candidate capability output; applicable AGENTS.md.
Issue boundary: supervised Binance Testnet mutation and evidence collection only. Out of scope: mainnet credentials/calls, code redesign, widened limits, strategy execution, commits, pushes, and any order not explicitly described by the runbook.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Credential rule: the human sets Testnet-only secrets in the local process environment; never request, display, persist, echo, inspect broadly, or archive them. Reconfirm exact campaign IDs, symbol, quantity, price, account cleanliness, and Testnet endpoint before each mutation.
Verification: read every CLI exit/result and journal sequence; prove one submit per campaign, terminal reconciliation, 24 active hours with forced restart, clean stop, restore equivalence, redaction, and hashes. Stop on ambiguity that cannot be resolved query-first.
Board: the controller must mark awaiting_manual_launch until supervision is present; then claim/heartbeat L-011, write its handoff, and mark done only with artifact paths/hashes and every criterion satisfied.
Final report: outcome, redacted campaign identifiers, binary/config/evidence hashes, exact commands with secrets omitted, results, incidents, and next issue.
```

## Issue L-012 — Run Mainnet Read-Only Shadow Soak

Blocked by: L-011.

Use a mainnet `USER_DATA` key that cannot trade to run a 24-hour market/account
stream and REST-reconciliation soak. No mutation authority may be present.

Acceptance criteria:

- Preflight independently proves the configured key/process has no reachable
  trade path and uses a dedicated account.
- Market and private streams, connection rotation/reconnect, freshness, REST
  snapshots, clock handling, and rate limits remain healthy for 24 active hours.
- Stream projections and stable REST snapshots reconcile without unexplained
  balance/order divergence.
- Zero submit/cancel/withdraw/transfer requests are emitted.
- Resource usage remains bounded and the final evidence package is redacted and
  hashed.

One-shot Goal prompt:

```text
Goal: Complete L-012 — run the human-supervised 24-hour Binance Spot mainnet read-only shadow soak.
This is a one-shot supervised worker launch prompt, not a recurring automation prompt. It must not possess or request a TRADE-enabled key.
Objective: validate real mainnet market/account observation, stream lifecycle, and REST reconciliation for the exact dedicated account without any mutation authority.
Read first: Live V1 Spec sections 8, 9, 10, 18.3, and 20; board; L-011 handoff; this runbook; shadow runbook/config; applicable AGENTS.md.
Issue boundary: read-only mainnet public/private streams, signed reads, stable reconciliation, 24-hour soak, failure drills that do not mutate, and evidence. Out of scope: submit/cancel, trade key, strategy, code feature work except evidence-backed blocker fixes that restart the full gate, and capability promotion.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Credential rule: the human injects a separate read-only key locally; never print, persist, or archive it. Preflight the binary, config scope, endpoint, account, and absence of mutation routes before connecting.
Verification: prove 24 active hours, reconnect/rotation, bounded queues/resources, fresh watermarks, REST/stream agreement, zero mutation attempts, secret-free artifacts, and exact hashes. Any foreign activity or divergence fails the gate.
Board: mark awaiting_manual_launch until supervised credentials are available; claim/heartbeat L-012, update its handoff, and mark done only with immutable evidence.
Final report: outcome, scope, durations/counts, redacted evidence hashes, failures/recoveries, remaining risks, and next issue.
```

## Issue L-013 — Independent Promotion Review and Canary Build

Blocked by: L-012.

Independently review code and evidence, close verified defects, and produce a
scope-bound `mainnet_canary` build whose live permit still requires a one-session
operator acknowledgement. This issue does not place orders.

Acceptance criteria:

- Every Spec acceptance criterion that precedes the canary has evidence.
- Independent architecture, security, failure-recovery, and test reviews have
  no unresolved critical/high finding.
- Full engineering, supply-chain, reachability, secret, restore, and capability
  gates pass on the exact candidate.
- Release evidence manifest binds commit/diff policy, binary/image hashes,
  configuration fingerprint, account/product/symbol/caps, and expiry.
- Capability advertises only the exact canary authority in the promoted live
  artifact; all other artifacts remain non-live.
- No external mutation occurs.

One-shot Goal prompt:

```text
Goal: Complete L-013 — independently review all Live V1 evidence and produce the exact mainnet-canary candidate without placing an order.
This is a one-shot worker launch prompt, not a recurring automation prompt.
Objective: verify the implementation and evidence end to end, repair only evidence-backed defects test-first, and create a scope-bound canary build/release manifest that still requires explicit runtime operator authorization.
Read first: entire Live V1 Spec; board; all L-001–L-012 handoffs/evidence; this runbook; current diff; capability source; deployment/runbooks; applicable AGENTS.md.
Issue boundary: independent review, narrowly justified fixes, complete gates, release manifest, capability/build-scope promotion, and canary runbook dry-run. Out of scope: real credentials, external order/cancel, strategy promotion, broader scope, dependencies without approval, commits/pushes unless separately requested.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: full Rust/frontend/browser/audit/diff/secret/release-build gates; binary dependency/route inspection; deterministic failure/restart/kill/restore suites; validate every evidence hash and capability claim. A missing external artifact blocks promotion.
Board: claim/heartbeat L-013, update its handoff, and mark done only when the exact candidate and manifest are reproducible and no high-risk finding remains.
Final report: promotion decision, findings/fixes, complete command evidence, artifact hashes, exact canary scope/expiry, residual risks, and whether L-014 may be manually launched.
```

## Issue L-014 — Run One Mainnet Canary Lifecycle

Blocked by: L-013 and explicit same-session user authorization.

Perform exactly one smallest-valid Binance Spot limit-order lifecycle under the
approved notional cap. No strategy loop runs. The issue ends with authoritative
account reconciliation or `recovery_required`.

Acceptance criteria:

- Human approves exact account, symbol, side, price, quantity, maximum notional,
  time window, client ID, and cancellation plan in the active session.
- Trade credential is locally injected, IP-restricted, withdrawal-disabled, and
  minimally funded; no secret is exposed.
- Preflight matches the release evidence ID, binary hash, config fingerprint,
  clean reconciliation, fresh streams, current filters, and risk headroom.
- Exactly one submit occurs; fills/fees/cancel settle idempotently.
- Final REST + stream reconciliation proves terminal order and account state.
- Any ambiguity stops; it never creates a replacement order.

One-shot Goal prompt:

```text
Goal: Complete L-014 — run exactly one human-supervised Binance Spot mainnet canary order lifecycle.
This is a one-shot supervised worker launch prompt, not a recurring automation prompt. Do not begin without explicit same-session user authorization of the exact intent.
Objective: use the L-013 canary artifact to place at most one smallest-exchange-valid LIMIT/LIMIT_MAKER order within the approved cap, observe/query it, cancel any remainder when planned, and finish with authoritative reconciliation.
Read first: Live V1 Spec sections 6, 11–14, 18.4, 20–22; board; L-013 handoff/manifest; canary runbook; current capability/preflight output; applicable AGENTS.md.
Issue boundary: one exact mainnet Spot canary lifecycle and redacted evidence only. Out of scope: strategy loop, second order, market order, replacement order, multiple symbols, transfers/withdrawals, code changes during the campaign, commits, and scope widening.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Authorization rule: before any mutation, present a secret-free preflight containing account alias, symbol, side, price, quantity, maximum notional, client ID, binary/config/evidence hashes, risk headroom, endpoint, and expiry; require the user's exact approval in this active session. Keys are set locally and never requested or displayed.
Verification: read journal and venue receipts after each state; prove exactly one submit, query-first ambiguity handling, correct fills/fees/cancel, kill-switch availability, and final stable account reconciliation. Stop in recovery_required on unresolved uncertainty.
Board: keep L-014 blocked until authorization; then claim/heartbeat it, write its handoff, and mark done only with redacted artifact hashes and reconciled terminal evidence.
Final report: outcome first, exact approved intent without secrets, state sequence, fills/fees, final reconciliation, evidence hashes, incidents, and whether manual live authority remains paused.
```

## Issue L-015 — Promote One Proven Automated Strategy

Blocked by: L-014, a user-selected `STRATEGY_ID`, and a separate accepted
strategy evidence artifact. The current three offline candidates failed, so
this issue is intentionally blocked at runbook creation.

Acceptance criteria:

- Exact strategy, symbol, cadence, inputs, order types, limits, and falsification
  rules are approved in a spec amendment.
- One production strategy seam is shared across offline, Paper, shadow, and live
  execution without hidden semantic changes.
- Leakage-resistant offline evidence, extended Paper observation, Testnet, and
  mainnet shadow decisions meet preregistered gates; negative results stop.
- Strategy can only emit intents; the same venue-backed risk/live owner retains
  final authority.
- A supervised strategy canary stays within stricter limits than manual live.

One-shot Goal prompt:

```text
Goal: Complete L-015 — promote one explicitly selected and independently proven strategy to bounded Binance Spot live execution.
This is a one-shot supervised worker launch prompt, not a recurring automation prompt. It is invalid until STRATEGY_ID and an accepted strategy evidence artifact are named.
Objective: bind exactly one approved strategy to the existing live intent boundary only after its offline, Paper, Testnet, and shadow evidence passes a preregistered promotion amendment.
Read first: Live V1 Spec plus the approved strategy amendment; board; L-014 handoff; strategy research/evaluation artifacts; live owner/risk contracts; this runbook; applicable AGENTS.md.
Issue boundary: one strategy adapter, semantic-parity tests, bounded observation/canary, operator controls, and evidence. Out of scope: searching for a strategy during implementation, post-holdout tuning, multiple strategies/symbols/venues, wider limits, profitability promises, and unattended first activation.
Use $ultracode for this issue if the work is non-trivial, multi-file, multi-phase, risky, or benefits from subagents. For tiny single-file or documentation-only changes, execute directly and verify narrowly.
Verification: freeze the strategy spec and thresholds before final evidence; prove identical decision inputs across evaluation/Paper/shadow/live; run full engineering and risk/recovery gates; execute only a separately authorized supervised bounded strategy canary. Negative evidence means reject and stop.
Board: keep blocked until prerequisites are file-backed; then claim/heartbeat L-015, update its handoff, and mark done only with exact strategy identity, reproducible evidence, and reconciled canary results.
Final report: promotion or rejection, strategy identity, evidence metrics with caveats, parity proof, commands/results, live limits, remaining risks, and rollback state.
```

## Recurring Controller Automation Prompt

Paste this into a recurring controller only. It coordinates and does not
implement code or perform external trading operations.

```text
Every 10 minutes, coordinate the Live Trading V1 program in <repo-root>. Read docs/automation/live-trading-v1/goal-board.md, docs/automation/live-trading-v1/goal-automation-runbook.md, the active handoff, and docs/internal/specs/LIVE_TRADING_V1_SPEC.md. Do not implement code in this recurring controller thread.

Reread the board immediately before claiming. Use compare-before-write: if another fresh claim exists, do not launch a duplicate. A claim is stale only after two missed controller checks, an uninspectable worker, or explicit context-loss evidence. Heartbeat only from observed worker progress. If duplicate workers exist, keep the one matching claim_token canonical, mark others superseded, and require evidence merge.

When the next dependency-satisfied non-supervised issue has no fresh claim, write a unique claim_token, claimed_at, claimed_by_thread, last_heartbeat, and increment attempt_count. Launch one fresh Goal worker from that issue's exact one-shot prompt without forking controller history. Never create per-issue recurring automations or worker fan-out. If fresh-thread tools are unavailable, mark awaiting_manual_launch and copy the exact prompt instead of implementing it.

For continuation, require a usable non-stale handoff plus the board and source docs. If the handoff is missing, empty, contradictory, or too stale, mark blocked and request recovery evidence; do not reconstruct authority from controller memory.

L-011, L-012, L-014, and L-015 are supervised. Never launch them automatically. Mark awaiting_manual_launch only when their file-backed prerequisites are met, and leave credentials plus mutation authorization to the human-supervised worker session. Never request or expose secrets. Never send an external order/cancel from the controller.

Mark done only when every issue acceptance criterion has exact commands, results, and artifact paths/hashes in its handoff. Record blockers honestly. Pause when all eligible non-supervised issues are done, a required user authorization is pending, only supervised gates remain, or the same genuine blocker survives three evidence-backed recovery attempts.
```

## Claim Guardrail

- Claim immediately before launching a worker and reread the board immediately
  before writing.
- A fresh claim prevents duplicate launch.
- Update `last_heartbeat` from real worker output, handoff edits, or completion
  evidence—not from elapsed time alone.
- Increment `attempt_count` for each fresh or continuation worker.
- A claim becomes stale after two missed checks, an uninspectable worker, or
  explicit context loss/confusion.
- The worker matching the current `claim_token` is canonical; duplicate workers
  are superseded and their evidence must be reviewed before discard.
- Clear/mark a claim `cleared` only when done or intentionally reset.

## Stop and Pause Conditions

- Stop successfully when L-001 through L-014 are done with evidence and L-015
  is either completed or explicitly left blocked pending a strategy decision.
- Pause for any authority that would use Testnet/mainnet credentials, mutate an
  exchange, add a dependency, widen venue/product/symbol/limits, deploy remotely,
  commit/push/publish, or delete user data/source beyond this spec.
- Stop an active trading procedure immediately on endpoint/account/config hash
  mismatch, secret exposure, foreign activity, stale data, unresolved order,
  journal degradation, rate-ban risk, or kill-switch failure.
- Three failed evidence-backed recovery attempts on the same non-external
  blocker require a precise handoff and `blocked` status; do not weaken gates.
