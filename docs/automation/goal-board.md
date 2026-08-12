# Crypto Trading Goal Board

## Current Status

- Active issue: none
- Active thread: root Goal `019ff1a7-229a-71d1-94c6-548f93748f08` completed
- Active handoff: `docs/automation/handoffs/issue-g-006-handoff.md`
- Last automation check: 2026-08-12T03:36:48+08:00
- Overall phase: G-001 through G-006 locally accepted; supervised Testnet and tagged-release gates remain external, and mainnet remains unavailable

## Session Policy

- Controller thread: coordination only unless the user pastes the explicit single-Goal sleep prompt from the runbook.
- Worker thread policy: one fresh session per issue by default.
- Continuation policy: start a fresh session from the handoff when context is stale or compressed.
- Source of truth: this board, handoff files, the current worktree, and verification output.
- Claim guardrail: claim an issue before launching a worker.
- Worker automation policy: do not create per-issue recurring worker automations.
- Safety boundary: unattended work may use offline replay, paper execution, public read-only market data, and deterministic test doubles only. It must never enable mainnet or send an external order/cancel.

## Issues

| Issue | Title | Status | Thread | Handoff | Notes |
| --- | --- | --- | --- | --- | --- |
| G-001 | Recover and finish interrupted trade-safety changes | done | `019ff1a7-229a-71d1-94c6-548f93748f08` | `handoffs/issue-g-001-handoff.md` | Local acceptance passed: authoritative Spot/USD-M metadata, recovery-safe lifecycle routing, and conservative buying-power/batch/reduction controls have targeted test and lint evidence. |
| G-002 | Review the integrated safety diff and run engineering gates | done | `019ff1a7-229a-71d1-94c6-548f93748f08` | `handoffs/issue-g-002-handoff.md` | Integrated findings repaired test-first; complete Rust/frontend/Playwright/fixture/audit/diff/secret gates passed with mainnet closed and no external mutation. |
| G-003 | Research current strategy candidates from primary sources | done | `019ff1a7-229a-71d1-94c6-548f93748f08` | `handoffs/issue-g-003-handoff.md` | Dated primary-source research accepted three testable Binance Spot families plus cash and buy-and-hold baselines without inspecting market samples or holdout data. |
| G-004 | Build a leakage-resistant, cost-aware evaluation seam | done | `019ff1a7-229a-71d1-94c6-548f93748f08` | `handoffs/issue-g-004-handoff.md` | Provenance-bound Spot data, causal next-open execution, embargoed walk-forward, concrete registry/holdout gating, frozen 1x/2x protocol, adapters, deterministic ledger tests, lint, and two independent reviews passed. |
| G-005 | Run bounded experiments and select or reject candidates | done | `019ff1a7-229a-71d1-94c6-548f93748f08` | `handoffs/issue-g-005-handoff.md` | Frozen 22-config selection and single holdout completed reproducibly; all three family winners failed the conjunctive rule, so no candidate passed and the search is closed. |
| G-006 | Final verification and release-readiness report | done | `019ff1a7-229a-71d1-94c6-548f93748f08` | `handoffs/issue-g-006-handoff.md` | Final local gates and two independent reviews passed; report records no promising candidate, no Testnet evidence, and no mainnet readiness. |

## Issue Claims

| Issue | claim_token | claimed_at | claimed_by_thread | last_heartbeat | attempt_count | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| G-001 | — | — | — | — | 1 | Closed at 2026-08-12T01:07:37+08:00 after independent G-001 verification passed. |
| G-002 | — | — | — | — | 1 | Closed and claim cleared at 2026-08-12T01:51:46+08:00 after all local G-002 acceptance gates passed. |
| G-003 | — | — | — | — | 1 | Research artifact accepted and claim cleared at 2026-08-12T02:08:24+08:00; shortlist, baselines, seam contract, and rejection rules are recorded in `docs/research/strategy-candidates-2026-08-12.md`. |
| G-004 | — | — | — | — | 1 | Closed and claim cleared at 2026-08-12T02:38:24+08:00 after the backtest crate passed 46 tests, strict Clippy, formatting, deterministic proofs, and independent acceptance/semantic review. |
| G-005 | — | — | — | — | 1 | Closed and claim cleared at 2026-08-12T03:19:41+08:00 after 66 backtest all-target tests, strict Clippy, persisted-before-holdout gating, deterministic artifact reproduction, and the honest conclusion that no candidate passed. |
| G-006 | — | — | — | — | 1 | Closed and claim cleared at 2026-08-12T03:36:48+08:00 after every local acceptance gate, deterministic reproduction, final report, and independent review passed. |

## Status Values

- pending
- in_progress
- awaiting_manual_launch
- blocked
- done

## Completion Rule

An issue is `done` only when every acceptance criterion in
`docs/automation/goal-automation-runbook.md` has evidence. A passing unit test
does not override an unclosed safety boundary. If external Binance Testnet
credentials, a 24-hour soak, or real operator evidence is unavailable, record
that release gate as blocked; never fabricate it and never substitute mainnet.
