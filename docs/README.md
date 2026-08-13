# Documentation

## Start here

- [Repository README](../README.md) — what this project is, what it is not, and
  the quick start. Read the warning at the top first.
- [`rust/README.md`](../rust/README.md) — the command surface, configuration
  classification, and safety boundaries in detail.

## Operating the software

- [`internal/specs/LIVE_TRADING_V1_SPEC.md`](internal/specs/LIVE_TRADING_V1_SPEC.md)
  — implementation and promotion contract for the minimal Binance Spot live
  runtime. Mainnet remains gated until its evidence requirements pass.
- [`automation/live-trading-v1/goal-automation-runbook.md`](automation/live-trading-v1/goal-automation-runbook.md)
  — the canonical but currently dormant automation tree. Its local board is
  [`automation/live-trading-v1/goal-board.md`](automation/live-trading-v1/goal-board.md).
- [`adapter-support.md`](adapter-support.md) — which exchange can do what. This
  is a human-readable projection of the capability manifest in
  `rust/crates/runtime/src/capability.rs`, held in sync by a contract test.
  The machine-readable authority is `crypto-trading capabilities --json`.
  `implemented` does not mean production-ready.
- [`runbooks/production-candidate.md`](runbooks/production-candidate.md) — the
  deployment contract, the four release gates (Testnet order lifecycle, account
  reconciliation, 24-hour soak, backup/restore drill), and rollback.
- [`reports/project-refocus-acceptance-2026-08-12.md`](reports/project-refocus-acceptance-2026-08-12.md)
  — the W0–W3 acceptance record, including the failed hourly data-admission
  result and the external gates that were deliberately not fabricated.
- [`reports/open-source-live-readiness-2026-08-13.md`](reports/open-source-live-readiness-2026-08-13.md)
  — the current public-repository acceptance and live-readiness disposition,
  including reviewed false positives, branch-integration scope, and the
  credentialed gates that remain closed.
- [`releasing.md`](releasing.md) — how a version is cut.

## Contributing

- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — local gates, hard boundaries,
  dependency and testing rules.
- [`../SECURITY.md`](../SECURITY.md) — threat model and private disclosure.
- [`../CHANGELOG.md`](../CHANGELOG.md) — version history, with an explicit
  statement per release of whether authority widened.
- [`design-system.md`](design-system.md) — the visual single source of truth for
  the Web control plane. Change this before changing a page.

## Internal working notes

[`internal/`](internal/) holds development history: dated audits, superseded
specifications, execution plans, and upstream research. These are kept as
evidence for how the current boundaries were arrived at. They are **snapshots,
not living documents** — where they disagree with the code, the code and the
capability manifest win. Paths, commands, prompts, and credentials-shaped
placeholders inside these snapshots are historical text only and must not be
executed or treated as current operator instructions.

- [`internal/audits/`](internal/audits/) — dated project audits and their
  remediation records. `RUST_PROJECT_AUDIT_REMEDIATION_2026-07-17.md` is the
  authoritative NO-GO list as of that date.
- [`internal/specs/`](internal/specs/) — refactor plan and hardening
  specifications.
- [`internal/plans/`](internal/plans/) — milestone execution plans.
- [`internal/research/`](internal/research/) — provenance evidence for the
  removed Python legacy archive and upstream comparison. The archive itself was
  removed from the working tree on 2026-08-13 and remains in Git history; see
  [`../archive/README.md`](../archive/README.md).
