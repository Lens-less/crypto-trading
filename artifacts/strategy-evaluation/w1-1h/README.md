# W1 1h Strategy Evaluation Artifacts

This directory is reserved for the frozen artifacts of protocol
`w1-btcusdt-spot-1h-20260812-v1`.

Allowed persistent files:

- `w1-btcusdt-1h-provenance.tsv`
- `w1-btcusdt-spot-1h-20260812-v1-data-admission.json`
- `w1-btcusdt-spot-1h-20260812-v1-selection.json`
- `w1-btcusdt-spot-1h-20260812-v1-results.json`
- `w1-btcusdt-spot-1h-20260812-v1-report.md`
- supporting checksum or verification notes that do not reveal raw price rows

Constraints:

- Raw ZIP and CSV market data stay in an external temp cache only.
- Selection must be persisted before any consuming holdout-open step.
- Negative results are first-class artifacts and must not be deleted or replaced
  with a retuned rerun under the same protocol id.
- A data-admission abort is terminal for the frozen protocol. It must not be
  relabeled as a strategy result, and it must not produce selection or holdout
  artifacts.
