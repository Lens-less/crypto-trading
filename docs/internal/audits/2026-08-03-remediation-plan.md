# Post-remediation corrective plan

> Date: 2026-08-03
> Source audit: [`2026-08-03-post-remediation-reaudit.md`](2026-08-03-post-remediation-reaudit.md)
> Governing spec: [`2026-08-02-sub-hf-quant-evolution-spec.md`](../specs/2026-08-02-sub-hf-quant-evolution-spec.md)

## Outcome

Restore a green, truthful, crash-safe baseline before publishing the local ten-commit remediation series. The work must preserve the improvements already verified by the re-audit: FIFO capacity release, disk-before-memory mutation, path/lock safety, graceful signal handling, crash-tail recovery, and concurrent market polling.

## Invariants

1. A durable journal remains the source of truth. In-memory counters and projections may never claim a transition that was not durably recorded, and cancellation must be observable.
2. Journal hot-path work is bounded by configured chain limits, not by unrelated directory contents or process lifetime.
3. Cold replay is self-recovering for interrupted admissions and fails closed only for facts that are actually corrupt or unsafe.
4. Capability and documentation claims cannot exceed executable entry points. Library-only functionality must be labelled as such.
5. Backtest output is an explicitly scoped research estimate: metrics use round trips, timing is derived from the tape, and unsupported execution assumptions are rejected or surfaced.
6. Concurrent polling preserves monotonic receipt semantics and a lower bound between completed poll rounds.
7. Production defaults emit actionable lifecycle/error telemetry without exposing order payloads, credentials, or unbounded high-volume records.

## Sequencing

### Phase 1 — blockers and red gate

- Replace unbounded sealed-segment directory enumeration with bounded probing and isolate crash-tail quarantine artifacts.
- Make notification shutdown persistence-aware: independent worker grace, durable status transitions, and explicit cancellation accounting.
- Add bounded admission recovery semantics based on durable timestamps; reject new risk without poisoning unrelated paper-account reads/writes.
- Make research capability levels and evidence truthful, then enforce generic evidence/entry-point invariants.

### Phase 2 — runtime correctness and operations

- Strengthen projection freshness/cross-checking and bound replay/index state without weakening fail-closed behavior.
- Add explicit risk-event validation, projection status propagation, and honest collateral/balance semantics.
- Preserve receive-time ordering across concurrent routes, enforce completion-based polling intervals, and account for lag.
- Set observable deployment defaults, cover the real writable SIGTERM path, and correct release-note claims.

### Phase 3 — research correctness

- Stabilize rolling statistics and make indicator warm-up explicit.
- Calculate trade metrics from closed round trips, derive annualization from event timestamps, and separate unavailable metrics from calculation failure.
- Validate single-instrument tapes, opposing-side execution prices, buying power, and executable order/fill rules.
- Add a runnable, explicitly research-only interface or downgrade claims where no executable interface exists.

### Phase 4 — durability and remaining audit items

- Add bounded snapshot/checkpoint and retention behavior only with reader-safe publication and recovery tests.
- Close deterministic identity, policy-fingerprint, registry lifecycle, async blocking, and dead-telemetry gaps.
- Record explicit dispositions for findings that cannot be implemented without an approved dependency or a materially different product contract.

## Public test seams

- `JsonlHistory` append/read/rotation/recovery APIs.
- `NotificationDispatcher` delivery, status, and stop APIs.
- Paper/risk authority refresh, snapshot, admission, reserve, and cold replay APIs.
- Capability manifest plus CLI contract output.
- Backtest engine/report and indicator public APIs.
- Market polling/supervisor observable streams and task state.
- Container/CLI process lifecycle for SIGTERM.

Each defect is handled as a vertical red–green slice. Targeted tests run during implementation; formatting, Clippy with warnings denied, all workspace targets/features, repository static checks, and focused performance probes run before publication.
