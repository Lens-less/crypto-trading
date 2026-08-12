# BTCUSDT 1h evaluation conclusion — data admission aborted

Protocol: `w1-btcusdt-spot-1h-20260812-v1`

Frozen preregistration commit: `855d0fd8652db74e7c18de393e2d78f3abbeae5d`

Terminal state: `aborted_at_data_admission`

## Conclusion

The hourly experiment did not reach strategy evaluation. The first official
monthly archive already violated the frozen zero-gap contract: January 2018
contains 743 rows instead of 744 and omits the `2018-01-04T04:00:00Z` open.
Its official and observed ZIP SHA-256 is
`b649198039645124717f334443ae550d1060661ee03c5fb605cf067fab26dc85`.
The preparation script therefore stopped before writing a provenance lock.

A source-shape-only audit then verified the official checksums for all 103
monthly archives. It found 75,096 raw rows for 75,216 UTC calendar hours,
including 43 off-grid rows and 163 missing canonical hour opens. There were no
duplicate opens. Twenty-two months have a row-count mismatch, 24 months fail
the complete hourly shape contract, and the last discontinuity occurs in March
2023. The clean suffix is too short for the frozen nine-window geometry.

No candidate return, selection metric, strategy ranking, or holdout metric was
computed. The selection transition and final-holdout evaluation remained
closed. This is a data-admission result, not evidence for or against any of the
22 strategies. The Edge gate remains closed.

The machine-readable evidence is
[`w1-btcusdt-spot-1h-20260812-v1-data-admission.json`](../../artifacts/strategy-evaluation/w1-1h/w1-btcusdt-spot-1h-20260812-v1-data-admission.json).

## Why the protocol is not repaired in place

Three apparent repairs were rejected:

- Starting after the last gap leaves only 20,650 pre-holdout hours, 44,966
  hours short of the frozen selection geometry.
- Flat carry-forward bars would alter momentum, Donchian, and volatility
  features and could invent fills while the venue was unavailable.
- Replacing failed months with minute data still leaves source-empty hours and
  requires time-aware lookbacks, rebalancing, and execution rules. That changes
  the frozen evaluation semantics.

A future v2 may preregister month-level atomic replacement for the 24 malformed
months and an explicit `Observed`/`Missing` time model. It must use a new
protocol id and freeze those rules before reading replacement archives. It is
not a continuation or retune of v1.
