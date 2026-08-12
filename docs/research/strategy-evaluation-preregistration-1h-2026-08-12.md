# W1 BTCUSDT Spot 1h Strategy Evaluation Pre-registration - 2026-08-12

## Freeze Declaration

This document freezes experiment protocol `w1-btcusdt-spot-1h-20260812-v1`
before any 1h archive payload, strategy result, candidate ranking, or
final-holdout value is evaluated. The protocol may be implemented or aborted,
but its dataset range, archive list, configurations, costs, split, selection
rule, uncertainty method, promotion thresholds, and holdout cohort must not be
changed after results are observed. A required change ends this cycle as a
protocol failure rather than silently starting another search.

This W1 rerun keeps the G-005 market, symbol, family count, configuration
count, cost model, promotion rule, and holdout calendar unchanged. The only
allowed design change is cadence: daily bars become hourly bars, and every
economic horizon that was expressed in bars in G-005 is translated by an exact
24x multiplier so that the underlying lookback, rebalance, embargo, selection,
and holdout time spans do not shrink.

At freeze time no raw 1h price row, OOS metric, candidate ranking, or
final-holdout result had been inspected. Only the official Binance monthly
archive URL pattern and month labels were used to define the immutable source
registry.

## Frozen Dataset Contract

- Venue/product/symbol: Binance Spot `BTCUSDT`.
- Cadence/timezone: closed `1h` klines in UTC.
- Inclusive range: `2018-01-01 00:00:00Z` through `2026-07-31 23:00:00Z`.
- Expected bars: `75216`.
- Sources: exactly 103 official monthly ZIP archives, ordered from
  `BTCUSDT-1h-2018-01.zip` through `BTCUSDT-1h-2026-07.zip`, each under
  `https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h/` and each
  verified against its sibling `.CHECKSUM` file.
- Timestamp units: the preparation step must validate the raw endpoint fields
  against Binance's public-data format evolution. Archives through `2024-12`
  are expected in milliseconds; archives from `2025-01` onward are expected in
  microseconds. The observed unit for every archive must be persisted in the
  provenance lock before any normalization; a mismatch aborts the experiment.
- Dataset retrieval/seal evidence is intentionally deferred until the dedicated
  preparation command runs. Before any evaluation, that command must persist a
  per-archive provenance lock containing URL, retrieval time, official ZIP
  SHA-256, observed ZIP SHA-256, decompressed CSV SHA-256, unit, endpoints, and
  row count. The lock becomes the authoritative dataset contract for all later
  selection and holdout runs.
- Every archive is parsed independently, then verified parts are merged only if
  their normalized hourly bars are exactly contiguous and instrument/cadence
  metadata agree.
- The aggregate dataset fingerprint is computed from the ordered component
  manifests and not from price results. A missing archive, digest mismatch,
  wrong row count, gap, overlap, duplicate, still-open bar, mixed product, or
  unexpected timestamp unit aborts the experiment.
- Raw ZIP/CSV files remain in a task-specific external temp cache and are never
  added to the repository. Only provenance locks, checksums, reports, and
  aggregate metrics may persist under `artifacts/strategy-evaluation/w1-1h/`.

## Frozen Evaluation Protocol

- Initial cash: `10000 USDT`.
- Quantity model: deterministic fractional BTC with no historical lot-size or
  minimum-notional backfill. Current `exchangeInfo` is not historical truth.
- Execution: completed close decision, earliest fill at the next hourly open,
  long-or-cash only, adverse taker execution, and common terminal liquidation.
- 1x per-side costs:
  - taker fee: `10 bps`;
  - half-spread proxy: `2 bps`;
  - adverse slippage proxy: `4 bps`;
  - adverse latency proxy: `4 bps`.
- 2x costs are the mechanical componentwise double: `20/4/8/8 bps`.
- The same `EvaluationProtocol` instance and fingerprint apply to every OOS and
  terminal-holdout run.
- Perpetual, short, leverage, borrow, maker/resting-limit, funding,
  liquidation, contract-multiplier, and L2/queue paths remain unsupported and
  fail closed.

## Frozen Split Geometry

- `training_len = 26280`
- `test_len = 4368`
- `step_len = 4368`
- `embargo_len = 24`
- `final_holdout_len = 8760`

This is the exact 24x bar translation of the G-005 `1095/182/182/1/365`
geometry. It still produces nine complete, non-overlapping OOS windows before
the final holdout. The final selection window ends on
`2025-06-26 23:00:00Z`; the unused terminal selection buffer ends on
`2025-07-31 23:00:00Z`. The untouched final holdout remains
`2025-08-01 00:00:00Z` through `2026-07-31 23:00:00Z`. The buffer is not
another tuning window.

If fewer than nine exact windows or a different final-holdout range is
produced, the experiment aborts instead of adapting the geometry.

## Frozen Search Registry

The full selection registry is 22 configurations across exactly five families.
The Donchian ensemble mentioned as a provisional ceiling in G-003 remains
excluded before data evaluation because no separately reviewed bounded adapter
exists; it cannot be added later.

Every bar-count parameter below is the exact 24x translation of its G-005 daily
predecessor. No shorter hourly lookback, rebalance cadence, embargo, or holdout
may be substituted after freeze.

### Mandatory baselines

- `cash`
- `buy-and-hold`

### Slow time-series momentum - five configurations

- `tsm-lb672-rb168`
- `tsm-lb1344-rb168`
- `tsm-lb2016-rb168`
- `tsm-lb2688-rb168`
- `tsm-lb4032-rb168`

Each uses the sign of the completed trailing return and a fixed 168-bar
rebalance cadence, preserving the original seven-day economic rebalance window.

### Long-only Donchian - three configurations

- `donchian-lb480`
- `donchian-lb1440`
- `donchian-lb2880`

### Capped volatility target - twelve configurations

- `vol-lb480-t10-b00-rb168`
- `vol-lb480-t10-b20-rb168`
- `vol-lb480-t15-b00-rb168`
- `vol-lb480-t15-b20-rb168`
- `vol-lb480-t20-b00-rb168`
- `vol-lb480-t20-b20-rb168`
- `vol-lb1440-t10-b00-rb168`
- `vol-lb1440-t10-b20-rb168`
- `vol-lb1440-t15-b00-rb168`
- `vol-lb1440-t15-b20-rb168`
- `vol-lb1440-t20-b00-rb168`
- `vol-lb1440-t20-b20-rb168`

Targets remain `10%`, `15%`, or `20%` annualized volatility; bands remain `0%`
or `20%`; every configuration uses the same fixed 168-bar rebalance cadence.
Insufficient or zero-variance history maps to cash.

No additional family, symbol, multi-asset extension, intrabar feature, or
parameter sweep is allowed inside this cycle. Negative results must be archived
as-is rather than trigger tuning.

## Frozen OOS Aggregation and Uncertainty

For every configuration and cost level, report every window plus:

- median and worst net return;
- median Sharpe and Sortino;
- deterministic 95% percentile bootstrap intervals for the median Sharpe and
  Sortino, resampling the nine window metrics with replacement for exactly
  `10000` replicates using base seed `0x4750303520260812`;
- positive-window fraction;
- worst window maximum drawdown;
- median turnover, trade count, and average exposure;
- median net-return delta against both cash and cost-matched buy-and-hold;
- median 2x net return and the complete component-cost sensitivity.

Unavailable ratios remain unavailable; they are never converted to zero or
infinity. A candidate with fewer than six available OOS Sharpe observations is
ineligible for a family winner. The bootstrap method is an empirical
window-level uncertainty diagnostic, not a claim of independent or normally
distributed returns.

## Frozen Family-Winner Rule

Baselines do not compete. Within each candidate family:

1. a configuration is eligible only when its median 1x and median 2x OOS net
   returns are both strictly positive and its median OOS Sharpe is available;
2. eligible configurations are ranked by, in order:
   - median 1x OOS Sharpe descending;
   - positive-window fraction descending;
   - median 1x net-return delta versus buy-and-hold descending;
   - worst-window maximum drawdown ascending;
   - median turnover ascending;
   - identifier ascending;
3. the first row is the sole family winner. If no row is eligible, the family
   is rejected before holdout.

The terminal-holdout registry contains `cash`, `buy-and-hold`, and at most one
winner from each of the three candidate families. Freezing all 22
configurations into holdout or selecting a winner from holdout is forbidden.

## Frozen Promotion Rule

After the single consuming holdout evaluation, a family winner is labeled
`promising` only when all six conditions hold:

1. final-holdout 1x net return is strictly positive after costs;
2. median selection-OOS Sharpe is at least `1.0`;
3. final-holdout 1x profit factor is available and at least `1.2`;
4. final-holdout 1x maximum drawdown is no worse than `20%`;
5. at least `60%` of selection OOS windows have positive 1x net return; and
6. final-holdout 2x net return remains strictly positive.

Missing metrics fail the condition. Benchmark outperformance is reported but is
not silently substituted for any runbook threshold. If no winner satisfies all
six conditions, the required conclusion is `no candidate passed`, the negative
result must remain archived, and no further search or tuning is allowed inside
this cycle.

## Frozen Artifact and Execution Order

The deterministic experiment fingerprint covers the ordered source manifests,
dataset endpoints/count, split, cash/cost protocol, complete 22-config
registry, winner rule, uncertainty method, thresholds, and runner version.

The runner must execute exactly:

1. run `scripts/prepare-w1-btcusdt-1h.ps1` to verify and parse every monthly
   archive independently, then persist the frozen provenance lock under
   `artifacts/strategy-evaluation/w1-1h/`;
2. construct and persist the frozen plan/provenance fingerprint;
3. evaluate the two baselines and all 20 candidate configurations only on the
   nine OOS window samples using the shared protocol;
4. persist the complete selection artifact and its family winners/rejections;
5. freeze only the two baselines plus family winners;
6. consume that persisted frozen state and evaluate the cohort on the final
   holdout once;
7. persist deterministic machine-readable results and the dated Markdown report
   under `artifacts/strategy-evaluation/w1-1h/`; and
8. rerun the locked command and compare the result bytes/fingerprint.

Persisting selection before the single holdout-open call is mandatory. A
one-shot in-memory transition that can be reopened, recomputed, or silently
reselected is forbidden. Reports must not contain raw price rows, credentials,
or claims of Paper, Testnet, mainnet, investment, or production readiness.
