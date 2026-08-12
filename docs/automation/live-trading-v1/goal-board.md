# Live Trading V1 Goal Board

## Current Status

- Active issue: none
- Active thread: none
- Active handoff: none
- Last automation check: not started
- Automation state: dormant
- Overall phase: blocked pending D2=`no`; no L-series issue is currently eligible for launch
- Baseline scope: Binance Spot / dedicated account / BTCUSDT / no leverage
- Manual mainnet goal: not approved under the current default D2 decision
- Automated strategy: blocked pending an explicit `STRATEGY_ID`, promotion evidence, and a future D2 reversal

## Session Policy

- Controller thread: coordination only.
- Worker thread policy: one fresh one-shot Goal session per issue.
- Continuation policy: use a fresh session from the issue handoff when context is stale or compressed.
- Source of truth: this board, `goal-automation-runbook.md`, issue handoffs, the Live V1 Spec, and current verification output.
- Per-issue handoff files are created only when an issue is actively claimed.
- D2 gate: default `no`; keep this tree dormant and do not claim or launch L-001 through L-015 unless that decision changes and the board is explicitly re-opened.
- Claim guardrail: compare-before-write and claim before launching a worker.
- Worker automation policy: do not create per-issue recurring automations.
- Supervised issues: L-011, L-012, L-014, and L-015 are never launched unattended.
- Secret policy: keys are locally injected in supervised sessions and never written to prompts, board, handoffs, logs, or Git.
- Mutation policy: only L-011 may mutate Testnet; only L-014/L-015 may mutate mainnet after explicit same-session authorization.

## Issues

| Issue | Title | Status | Blocked by | Thread | Handoff | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| L-001 | Lock scope and establish a safe baseline | pending | — | — | — | Recommended first Goal. Preserve the dirty worktree and current G-series history. |
| L-002 | Add a mainnet read-only shadow adapter | pending | L-001 | — | — | Deterministic mocks only; no real credential use. |
| L-003 | Add a production market stream | pending | L-001 | — | — | A new WebSocket crate requires explicit approval if none is already approved. |
| L-004 | Add User Data Stream and reconciliation | pending | L-002 | — | — | Real read-key evidence is deferred to L-012. |
| L-005 | Implement the gated mainnet trade protocol | pending | L-002 | — | — | Loopback/mock transport only; capability stays closed. |
| L-006 | Build venue-backed account risk | pending | L-003, L-004, L-005 | — | — | Paper/synthetic budgets cannot authorize live risk. |
| L-007 | Implement the journaled live execution owner | pending | L-005, L-006 | — | — | No external mutation; crash/replay evidence required. |
| L-008 | Make the kill switch operational | pending | L-007 | — | — | Cancel owned orders and reconcile; no automatic liquidation. |
| L-009 | Split binaries and prune the live artifact | pending | L-007, L-008 | — | — | Exclude from production; retain verification/research source. |
| L-010 | Harden operations, deployment, and observability | pending | L-009 | — | — | Live authority remains closed. |
| L-011 | Produce supervised Binance Testnet evidence | pending | L-010 | — | — | Later becomes `awaiting_manual_launch`; Testnet-only credentials. |
| L-012 | Run mainnet read-only shadow soak | pending | L-011 | — | — | Later becomes `awaiting_manual_launch`; read-only key must not trade. |
| L-013 | Independent promotion review and canary build | pending | L-012 | — | — | Produces candidate/manifest but sends no order. |
| L-014 | Run one mainnet canary lifecycle | blocked | L-013 + explicit user authorization | — | — | One exact supervised order maximum; no strategy loop. |
| L-015 | Promote one proven automated strategy | blocked | L-014 + `STRATEGY_ID` + strategy evidence | — | — | Current frozen strategy candidates all failed; negative evidence is binding. |

## Issue Claims

| Issue | claim_token | claimed_at | claimed_by_thread | last_heartbeat | attempt_count | Notes |
| --- | --- | --- | --- | --- | ---: | --- |
| L-001 | — | — | — | — | 0 | Unclaimed. |
| L-002 | — | — | — | — | 0 | Unclaimed. |
| L-003 | — | — | — | — | 0 | Unclaimed. |
| L-004 | — | — | — | — | 0 | Unclaimed. |
| L-005 | — | — | — | — | 0 | Unclaimed. |
| L-006 | — | — | — | — | 0 | Unclaimed. |
| L-007 | — | — | — | — | 0 | Unclaimed. |
| L-008 | — | — | — | — | 0 | Unclaimed. |
| L-009 | — | — | — | — | 0 | Unclaimed. |
| L-010 | — | — | — | — | 0 | Unclaimed. |
| L-011 | — | — | — | — | 0 | Supervised only. |
| L-012 | — | — | — | — | 0 | Supervised read-only credential use. |
| L-013 | — | — | — | — | 0 | Unclaimed. |
| L-014 | — | — | — | — | 0 | Blocked until exact same-session authorization. |
| L-015 | — | — | — | — | 0 | Blocked until strategy identity/evidence exist. |

## Status Values

- `pending` - dependencies or execution remain.
- `in_progress` - one canonical claimed worker is active.
- `awaiting_manual_launch` - file-backed prerequisites pass, but supervision or a fresh manual Goal launch is required.
- `blocked` - a named external decision/authority/evidence is missing or recovery is exhausted.
- `done` - every acceptance criterion has exact evidence in the handoff.

## Completion Rule

An issue is `done` only when every acceptance criterion in
`goal-automation-runbook.md` is evidenced by commands/results and artifact paths
or hashes. Tests passing does not substitute for an external Testnet/shadow/
canary gate. Credentials do not imply authority. Missing evidence is recorded as
blocked; it is never fabricated or replaced by a broader mainnet action.

The platform milestone is complete when L-001 through L-014 are done. L-015 may
remain blocked without invalidating the platform milestone, but the system must
then remain manual-only and advertise no automated strategy authority.
