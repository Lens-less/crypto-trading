# G-005 BTCUSDT Spot Offline Strategy Evaluation - 2026-08-12

## Outcome

No candidate passed the frozen `promising` rule. The bounded search is closed:
no parameter, family, threshold, split, cost, or strategy change was made after
the final holdout was opened, and no further search is permitted in this cycle.

The three preselected family winners all lost money on the untouched 365-bar
final holdout at both 1x and 2x modeled costs. They also missed the frozen
profit-factor and maximum-drawdown thresholds. This is negative offline research
evidence, not a profitability claim or a reason to advance a strategy to Paper,
Testnet, or mainnet.

## Frozen Protocol and Data

- Protocol: `g005-btcusdt-spot-20260812-v1`.
- Plan fingerprint:
  `269a49923ad9b019bfefd9b6a451363de3362fea4bdace4d33a7e42a8817edf5`.
- Dataset: Binance Spot `BTCUSDT` closed daily klines in UTC, 2018-01-01
  through 2026-07-31 inclusive.
- Provenance: 103 ordered official monthly archives and sibling checksums,
  3134 verified contiguous bars.
- Split: `1095` training bars, `1` embargo bar, `182` test bars, `182`-bar
  step, nine selection OOS windows, and one terminal `365`-bar holdout
  (2025-08-01 through 2026-07-31).
- Initial cash: `10000 USDT`.
- 1x per-side costs: fee `10` bps, half-spread proxy `2` bps, slippage proxy
  `4` bps, latency proxy `4` bps. The 2x case doubles every component.
- Registry: exactly 22 preregistered configurations: cash, cost-matched
  buy-and-hold, five slow time-series momentum variants, three long-only
  Donchian variants, and twelve capped-volatility variants.
- Uncertainty: 10,000 deterministic window-level bootstrap resamples of the
  median, seed `0x4750303520260812`. These intervals are diagnostics over nine
  windows and do not assume independent observations.

The complete frozen protocol was recorded in
`docs/research/strategy-evaluation-preregistration-2026-08-12.md` before any
strategy result was inspected. The selection artifact was written and synced
successfully before the consuming type transition made final-holdout data
available.

## Selection Results

The mandatory baselines and one winner per eligible candidate family formed the
sealed holdout cohort. Values below are selection-OOS aggregates at 1x costs
unless marked otherwise.

| Configuration | Median return | Median Sharpe (95% bootstrap) | Median Sortino (95% bootstrap) | Positive windows | Worst drawdown | Median 2x return | Median delta vs buy-and-hold |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `buy-and-hold` | 0.39237 | 1.34415 (-0.25365, 2.03780) | 1.98524 (-0.34844, 3.39092) | 77.78% | 60.25% | 0.38681 | 0.00000 |
| `tsm-lb028-rb007` | 0.17949 | 1.19457 (-1.22876, 1.67263) | 2.05983 (-1.55015, 2.85745) | 77.78% | 41.66% | 0.16077 | -0.09655 |
| `donchian-lb020` | 0.14103 | 1.08375 (-0.94892, 1.72467) | 1.78430 (-1.15454, 2.77392) | 66.67% | 46.74% | 0.11462 | -0.25133 |
| `vol-lb020-t20-b20-rb007` | 0.13727 | 1.15513 (-0.70342, 1.86906) | 1.74970 (-0.91679, 3.24479) | 77.78% | 25.80% | 0.12883 | -0.23789 |

The wide intervals and negative lower bounds are material uncertainty evidence.
All three family winners also trailed buy-and-hold on median selection return;
benchmark outperformance was reported but was not substituted for any promotion
threshold.

## Untouched Final Holdout

| Configuration | 1x net return | 2x net return | 1x annualized volatility | 1x Sharpe | 1x Sortino | Profit factor | Max drawdown | Trades | Avg exposure | Promising |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `cash` | 0.00000 | 0.00000 | 0.00000 | N/A | N/A | N/A | 0.00% | 0 | 0.00% | baseline |
| `buy-and-hold` | -0.45893 | -0.46109 | 0.43144 | -1.15496 | -1.53562 | 0.00000 | 52.97% | 2 | 100.00% | baseline |
| `tsm-lb028-rb007` | -0.16332 | -0.18967 | 0.23605 | -0.63859 | -0.83209 | 0.42919 | 28.70% | 16 | 39.45% | no |
| `donchian-lb020` | -0.22347 | -0.25093 | 0.18027 | -1.31553 | -1.63068 | 0.21180 | 29.99% | 20 | 24.93% | no |
| `vol-lb020-t20-b20-rb007` | -0.29634 | -0.30410 | 0.24048 | -1.34418 | -1.73424 | 0.06104 | 36.90% | 21 | 57.13% | no |

Each candidate passed the selection median-Sharpe and positive-window gates, but
failed all four holdout-dependent gates: positive 1x return, profit factor at
least 1.2, maximum drawdown no worse than 20%, and positive 2x return. Therefore
the required conjunctive result is `no candidate passed`.

## Deterministic Artifacts and Reproduction

- Provenance lock SHA-256:
  `5eb95ab4efeddc2656c6cd2863a48a50c685758ef458a5102bcc64c5047c2d3f`.
- Selection JSON SHA-256:
  `579ba0527ba00c3a84820c0e24988262f17be3140e8f36c24fac78dc7206c7e2`.
- Final results JSON SHA-256:
  `89b18ab6024370a9eb079bcc77416141e12fb0d3da1a43701f18750e91bd0cff`.
- Generated Markdown SHA-256:
  `d0948ada1dc3efd08d0a32f451dfce3bcce7e410979034e42f45510296a86ddf`.

From `rust/`, the exact locked offline command was:

```powershell
cargo +1.89.0 run --locked -p crypto-trading-backtest --example g005_evaluation -- --cache <temporary-cache-dir>/crypto-trading-g005-btcusdt-v1 --provenance-lock ../artifacts/strategy-evaluation/g005-btcusdt-provenance.tsv --output-dir ../artifacts/strategy-evaluation
```

Repeated executions produced byte-identical selection JSON, final JSON, and
generated Markdown at the hashes above. The machine-readable artifacts are:

- `artifacts/strategy-evaluation/g005-btcusdt-spot-20260812-v1-selection.json`
- `artifacts/strategy-evaluation/g005-btcusdt-spot-20260812-v1-results.json`
- `artifacts/strategy-evaluation/g005-btcusdt-spot-20260812-v1-report.md`

## Model Limits and Readiness Boundary

- Official archives may be corrected later; this result is tied to the ordered
  frozen manifests and digests above.
- Daily OHLCV does not contain executable historical spread, depth, queue,
  market impact, private fee tier, or capacity truth. Spread, slippage, and
  latency are explicit conservative proxies, not reconstructed order books.
- Fractional BTC quantities are deterministic research arithmetic. Current
  `exchangeInfo` was not projected backward as historical lot-size or
  minimum-notional truth.
- Terminal liquidation and close-to-next-open decisions are common evaluation
  conventions, not guarantees of executable fills.
- The evaluation is Spot-only, long-or-cash, single-asset, and excludes taxes,
  custody, operational outages, and portfolio interactions.
- Perpetuals remain fail closed because margin, maintenance margin,
  liquidation, funding, and contract-multiplier models are absent.
- This result is offline-only. It provides no Paper observation, credentialed
  Testnet lifecycle/reconciliation evidence, 24-hour soak evidence, or mainnet
  readiness. Mainnet remains disabled.
