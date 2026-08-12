# Live Trading V1 Handoff Contract

Each one-shot Goal worker creates or updates exactly one file:

```text
docs/automation/live-trading-v1/handoffs/issue-l-<number>-handoff.md
```

The handoff is a compact recovery record, not a transcript or duplicate of the
Spec/runbook. A continuation worker starts from the board, this handoff, source
documents, and current repository evidence.

Use this template:

```md
# L-XXX — <Title> Handoff

## Status

- Status: in_progress | blocked | done
- Claim token:
- Canonical worker thread:
- Last heartbeat:
- Repository: C:\Users\28340\Desktop\crypto-trading

## Source Documents

- docs/internal/specs/LIVE_TRADING_V1_SPEC.md
- docs/automation/live-trading-v1/goal-automation-runbook.md
- docs/automation/live-trading-v1/goal-board.md
- <issue-specific paths>

## Files Inspected or Changed

- <path — inspected/changed and why>

## Decisions

- <decision, evidence, and rejected alternative where useful>

## Commands and Results

| Command | Result | Evidence path/hash |
| --- | --- | --- |
| `<exact command with secrets omitted>` | pass/fail | `<path or hash>` |

## Acceptance Criteria

| Criterion | Status | Evidence |
| --- | --- | --- |
| `<criterion>` | pass/fail/not-run | `<command, path, or reason>` |

## Safety and Secret Check

- External calls made:
- Exchange mutations made:
- Credential class used (never the value):
- Secret scan result:
- Mainnet capability before/after:

## Blockers and Remaining Risks

- <exact blocker or risk>

## Exact Continuation Prompt

<One-shot prompt that tells a fresh worker to read this handoff, board, runbook,
Spec, current diff, and applicable AGENTS.md; preserve issue scope; run the
remaining verification; update the same claim and handoff.>
```

Rules:

- Never include a key, secret, signature, bearer token, raw environment dump,
  or unredacted account identifier.
- Record external actions exactly, including when none occurred.
- A `done` handoff has no missing acceptance evidence.
- If the handoff is missing, empty, contradictory, or too stale to recover
  safely, the controller marks the issue blocked instead of launching a guessed
  continuation.
- A continuation prompt is one-shot, not a recurring automation prompt.
