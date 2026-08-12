# Current Spot Strategy Candidates — 2026-08-11

## Decision

This is a research shortlist, not a profitability finding or investment advice.
No market-data sample, calibration period, walk-forward result, or final holdout
was inspected while writing it. No credentials or external order/cancel path was
used.

Advance exactly three Binance Spot, long-or-cash, single-instrument families to
the G-004 evaluation seam:

1. slow BTC time-series momentum;
2. causal long-only Donchian breakout/trailing exit;
3. capped volatility-controlled BTC exposure.

Use cash/no-trade and BTC buy-and-hold as mandatory baselines. Start with
`BTCUSDT` daily bars so all candidates can share one auditable Spot data and
execution contract. Do not advance cross-sectional, short, maker/grid,
perpetual, funding, or machine-learning families in this cycle.

Nothing in the source literature establishes that these adaptations will pass
this repository's untouched-holdout criteria. A negative result is expected to
be recorded as `no candidate passed`.

## Research Method and Source Cutoff

- Search cutoff and access date: **2026-08-11 (Asia/Shanghai)**.
- Included evidence: publisher papers, working papers/preprints, official
  Binance documentation and repositories, and one author's original research
  repository used as reproducibility/negative-evidence context.
- Excluded as evidence: vendor summaries, social posts, strategy-sales pages,
  anonymous performance screenshots, and secondary implementations.
- A recent source is not presumed reliable merely because it is recent. Source
  claims below motivate falsifiable tests; their reported returns are not copied
  into this project's conclusions.

### Primary-source ledger

| Source | Source date / status | Relevance and limitation |
| --- | --- | --- |
| [Kang & Ryu, “Time-series momentum and market timing in Bitcoin”](https://link.springer.com/article/10.1057/s41283-026-00234-7) | Published 2026-07-10, peer-reviewed version of record | Reports that a slow signal with a 12-week baseline horizon outperformed faster alternatives in its sample and identifies Binance Public Data as the data source. The full method is subscription content, so C-1 is a transparent family-level adaptation, not a claimed reproduction. |
| [Zarattini, Pagani & Barbon, “Catching Crypto Trends”](https://concretumgroup.com/wp-content/uploads/2026/02/Catching-Crypto-Trends.pdf) | First version 2025-04-04; cited PDF version 2025-04-09 | Defines long-only daily Donchian entry, trailing-midpoint exits, volatility sizing, a liquid-universe rotation, and cost sensitivity. Its data are aggregated CoinMarketCap data, not Binance venue data; C-2 therefore tests only a causal Binance Spot adaptation. |
| [Grobys et al., “Cryptocurrency momentum has (not) its moments”](https://link.springer.com/article/10.1007/s11408-025-00474-9) | Published 2025-03-27, open access | Finds severe, idiosyncratic momentum crashes and warns that volatility management does not remove the heavy-tail uncertainty. It supports testing C-3 as a risk-control hypothesis, not assuming it creates alpha. |
| [Han, Kang & Ryu, “Momentum in the Cryptocurrency Market: A Comprehensive Analysis under Realistic Assumptions”](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4675565) | Posted 2024-01-16; revised 2026-03-26 | Reports stronger time-series than cross-sectional evidence after incorporating realistic constraints. It is a working paper and much of its liquidation analysis concerns leveraged portfolios that this Spot-only cycle deliberately excludes. |
| [Bui & Nguyen, “Systematic Trend-Following with Adaptive Portfolio Construction”](https://arxiv.org/abs/2602.11708) | arXiv v1, 2026-02-12 | Current high-complexity trend candidate screened and rejected as shipped: it uses 6-hour data, monthly parameter optimization, market-cap inputs, and a 70/30 long-short allocation. The displayed abstract/body also describe different evaluation endpoints. Its fixed, long-only components do not justify importing the complete method. |
| [Jadouli, “Predictive Extrema, Unprofitable Policies”](https://arxiv.org/abs/2607.19453) | arXiv v1, 2026-07-21 | Recent Binance Spot negative study: predictive scores did not imply positive policies after assumed costs, and its self-audit found consumed dates, missing artifacts, same-close execution, and an unpurged horizon. It is cautionary evidence, not a universal claim about ML. |
| [Jadouli, original Quantbot research repository](https://github.com/AyoubJadouli/Quantbot-Research-Framework) | Mutable original repository, accessed 2026-08-11 | Exposes the paper source and a research-only, chronological, fail-closed workflow, while explicitly noting that heavy result/data payloads were removed and no archival release exists. It is not imported as code or dependency. |
| [Binance Spot REST API](https://developers.binance.com/en/docs/products/spot/rest-api) and [official Spot API repository](https://github.com/binance/binance-spot-api-docs) | Mutable official documentation/repository, accessed 2026-08-11 | Defines public klines and current `exchangeInfo`. Klines have explicit open/close timestamps; `exchangeInfo` is current state, not point-in-time historical metadata. |
| [Binance Public Data repository](https://github.com/binance/binance-public-data) | Mutable official repository, accessed 2026-08-11 | Provides daily/monthly Spot kline archives and per-file checksums. It states that Spot archive timestamps switch to microseconds from 2025-01-01 and that archived files can later be replaced after corrections. |
| [Binance Spot Commission FAQ](https://developers.binance.com/en/docs/products/spot/faqs/commission_faq) | Official page last modified 2026-08-11; accessed 2026-08-11 | Says actual commission depends on account, symbol, side, discounts, tax, and special rates and is queried through authenticated account/test-order APIs. Its numerical examples are explicitly fictional, so this unattended study must use a declared conservative cost model rather than claim an account's true fee. |

## Shared Data and Execution Contract for G-004/G-005

All three candidates and both baselines must use the same contract.

### Venue, product, and data

- Venue/product: Binance **Spot** only.
- Initial instrument/cadence: `BTCUSDT`, UTC daily klines.
- Source: immutable local copies of official monthly/daily archive ZIPs plus
  their published `.CHECKSUM` files. Record source URLs, retrieval timestamp,
  archive checksum, decompressed-content checksum, parser version, symbol,
  cadence, and exact first/last closed bar.
- Normalize the documented millisecond/microsecond boundary explicitly. Reject
  timestamps whose declared scale, open/close duration, or UTC ordering is
  inconsistent.
- Reject missing, duplicated, overlapping, non-monotonic, or still-open bars.
  Do not interpolate prices or silently forward-fill a tradability decision.
- Freeze the downloaded bytes before any experiment. If Binance later replaces
  an archive/checksum, it becomes a new dataset identifier rather than silently
  changing a prior result.
- Public klines do not provide historical bid/ask or queue depth. Current
  `bookTicker` must not be backfilled as historical spread, and current
  `exchangeInfo` must not be represented as historical listing/rule truth.

Starting with one continuously observed major Spot pair sharply reduces, but
does not eliminate, survivorship and venue-selection bias. Results apply only
to the frozen Binance `BTCUSDT` sample and must not be generalized to a
cross-sectional crypto universe.

### Causal timing

- A decision at bar `t` may read only bars whose close timestamp is at or
  before `t`'s close.
- The earliest modeled execution is the next bar's open. Same-close execution
  is forbidden.
- Training features whose outcome horizon crosses a split boundary require an
  embargo at least as long as that horizon. The final holdout remains a single,
  untouched terminal interval and is opened only once after configuration
  selection is frozen.
- Corporate-calendar concepts such as weekdays are not allowed to drop crypto
  weekend observations. All scheduling is UTC and continuous.

### Costs, fills, capacity, and cash

- Taker market execution only. Maker/resting-limit fills remain unsupported.
- Model fee, half-spread proxy, adverse slippage, and decision/execution latency
  separately, then report their sum. Because authenticated account commission
  truth is unavailable and prohibited here, label the fee a conservative
  assumption, not a Binance account fact.
- Pre-register one 1x all-in cost schedule and an exact 2x schedule before the
  final holdout. Every baseline and candidate uses the same side-by-side cost
  schedules. A round trip pays two sides.
- Execute at next-open plus adverse cost, never at an intrabar best price. Gaps
  through a signal/exit level fill at the next available modeled price.
- Keep gross exposure in `[0, 1]`: no leverage, shorting, margin, borrow,
  derivatives, or implicit negative cash. Unallocated USDT earns zero in this
  cycle unless a separately sourced cash-rate model is pre-registered. Results
  are denominated in USDT; the cash baseline is not a claim that USDT is a
  risk-free USD deposit, and quote-asset/depeg risk remains outside this price
  tape.
- Kline quote volume is not executable depth. Report turnover and a conservative
  participation diagnostic, but label capacity **unproven**; do not extrapolate
  a small offline account result to institutional size.

## Mandatory Baselines

### B-0 — Cash / no trade

- Hold 100% USDT, with zero assumed cash yield and zero trading cost.
- Purpose: prove that abstention is always available and catch simulations that
  manufacture returns without fills.

### B-1 — BTC buy-and-hold

- Buy once at the first causally eligible next-bar open using the 1x/2x cost
  model, hold at at most 1x exposure, and liquidate at the common terminal
  convention for a cost-matched comparison.
- Purpose: measure whether active timing adds value rather than merely inheriting
  BTC's drift. Buy-and-hold is a benchmark, not a claim that BTC is suitable for
  live investment.

## Candidate C-1 — Slow BTC Time-Series Momentum

**Rank:** 1. **Advance:** yes.

- **Hypothesis:** a slow positive trailing BTC return contains enough persistence
  to justify long Spot exposure, while moving to cash after a negative trend can
  reduce large drawdowns without trading so frequently that costs dominate.
- **Instrument/cadence:** Binance Spot `BTCUSDT`; daily source bars; signal and
  rebalance on a fixed weekly UTC schedule.
- **Signal family:** sign of a completed trailing return; long at most 100% when
  positive, otherwise cash. A provisional, not-yet-run family ceiling is
  `{4, 8, 12, 16, 24}` trailing weeks (five configurations). Freeze the exact
  set in G-005 before opening the final holdout.
- **Data:** closed daily OHLCV; only close history is required for the signal,
  while the next daily open supplies the execution reference.
- **Execution assumption:** next-open taker fills; trade only when the target
  state changes at the weekly decision point; no short position when the signal
  is negative.
- **Turnover/capacity risk:** expected turnover is low to moderate, but clustered
  reversals can cause repeated round trips. Daily volume does not prove that a
  chosen order size can fill at the modeled price.
- **Likely failure regimes:** sideways/choppy markets, abrupt V-shaped reversals,
  overnight/next-bar gaps, and structural weakening of BTC trend persistence.
- **Falsifier:** reject the family if it misses any runbook promising threshold,
  loses at 2x costs, depends on one window/regime, or fails to improve a
  cost-matched benchmark on repeated out-of-sample windows. The 12-week result
  in Kang & Ryu is motivation, not a privileged post-hoc winner here.

## Candidate C-2 — Causal Long-Only Donchian Breakout

**Rank:** 2. **Advance:** yes.

- **Hypothesis:** a completed close breaking above a prior rolling high marks a
  persistent trend, while a monotonically non-decreasing channel-midpoint exit
  can truncate part of the downside.
- **Instrument/cadence:** Binance Spot `BTCUSDT`; daily decisions.
- **Signal family:** calculate the entry channel from bars ending before the
  signal close, enter long only after a strict breakout, and update the trailing
  exit only from completed data. A provisional family ceiling is fixed-window
  `{20, 60, 120}` days plus one equal-weight vote/ensemble of those windows
  (four configurations). No best-looking paper window is imported.
- **Data:** closed daily OHLCV. The published paper defines its channel from
  closes; this adaptation must make the lag explicit so the current close cannot
  create and execute its own boundary.
- **Execution assumption:** next-open taker entry/exit. A gap past the channel or
  trailing level fills at that next open plus adverse costs; no optimistic stop
  price or intrabar path is assumed.
- **Turnover/capacity risk:** faster windows can whipsaw and the ensemble can
  create fractional rebalancing. BTC daily volume is only a coarse capacity
  diagnostic.
- **Likely failure regimes:** range-bound markets, false breakouts, rapid crash
  and rebound, and gaps that make the trailing exit materially worse than its
  signal level.
- **Falsifier:** reject if the result is not stable across the small predeclared
  window set, fails the untouched holdout/runbook thresholds, or turns negative
  under 2x costs. Also reject any implementation whose same-close or intrabar
  fills reproduce the paper more favorably than the causal next-open contract.

## Candidate C-3 — Capped Volatility-Controlled BTC Exposure

**Rank:** 3. **Advance:** yes, as a risk-control hypothesis.

- **Hypothesis:** reducing BTC exposure when lagged realized volatility is high
  may lower drawdown and improve net risk-adjusted performance, even if it does
  not improve raw return.
- **Instrument/cadence:** Binance Spot `BTCUSDT`; daily returns, with fixed
  weekly or monthly rebalance points.
- **Signal family:** target weight is a deterministic inverse-lagged-volatility
  function capped to `[0, 1]`. No low-volatility leverage is allowed. A
  provisional ceiling is realized-volatility windows `{20, 60}` days, target
  volatilities `{10%, 15%, 20%}`, and rebalance bands `{0%, 20%}` for at most
  12 configurations. Freeze or reduce this set before the holdout.
- **Data:** lagged close-to-close returns only; `t`'s target weight cannot use
  `t+1` or full-sample volatility. Treat zero/insufficient volatility history as
  no-trade, not infinite exposure.
- **Execution assumption:** next-open taker rebalance, exact 1x/2x costs on
  changed notional, non-negative cash, fractional BTC quantity subject to the
  evaluation's explicit quantization policy.
- **Turnover/capacity risk:** frequent small weight changes can consume the
  entire risk benefit through fees and slippage; the rebalance band must be
  selected only in training/validation.
- **Likely failure regimes:** sudden crashes following calm periods, V-shaped
  recoveries while exposure is low, persistent bull markets where buy-and-hold
  dominates, and unstable realized-volatility estimates.
- **Falsifier:** reject if drawdown reduction is absent or too small to offset
  lower return/cost, if risk metrics are unstable under block/bootstrap
  uncertainty, if tail losses remain unacceptable, or if any runbook promising
  threshold/2x-cost condition fails. Grobys et al. specifically cautions that
  volatility scaling did not remove extreme tail uncertainty.

## Screened but Rejected for This Cycle

| Family | Disposition | Fail-closed reason |
| --- | --- | --- |
| Bui–Nguyen AdaptiveTrend as published | Reject | Requires a broad cross-section, monthly Sharpe/grid selection, market-cap data, short allocation, and execution assumptions outside the current single-Spot seam. Importing only its best-looking settings would add search bias. |
| Cross-sectional momentum or reversal | Reject | Primary studies conflict materially; canonical portfolios require short/borrow, and a long-only rewrite is not the researched payoff. Point-in-time universe/listing/market-cap truth and multi-asset execution are not yet available. Grobys et al. also documents single-coin short-leg crashes and undefined tail variance. |
| Perpetual momentum, basis/cash-and-carry, and funding capture | Reject | The repository intentionally lacks complete initial/maintenance margin, liquidation, funding, contract-multiplier, mark/index price, and leverage mechanics. Existing perpetual backtests must remain fail closed. |
| Market making, order-book imbalance, latency/cross-venue arbitrage | Reject | Requires synchronized L2 books, queue position, venue latency, missed/partial fills, inventory truth, and often transfer/borrow truth. OHLCV or current `bookTicker` cannot supply these facts. |
| Grid and resting-limit strategies, including VirtualGrid | Reject | Candle high/low crossing a level does not prove order sequence, queue priority, or a fill. Gap-crossing multiple levels cannot be booked as automatic executions. The current backtest kernel correctly rejects maker/resting-limit assumptions. |
| Intraday candle ML, deep learning, and reinforcement learning | Reject | Would expand dependencies/search space and needs a separate prediction-to-policy protocol. The July 2026 negative audit shows why high AUC and best validation rows are not executable evidence, while its own repository still lacks immutable complete data/artifact release. |
| News, social, on-chain, wallet-flow, and sentiment timing | Reject | Adds timestamp alignment, revision, availability, licensing, and survivorship inputs not covered by the public Spot kline provenance contract. Some recent proposals also depend on private/order-flow or derivatives truth. |
| Sub-hour mean reversion/scalping | Reject | Historical spread and latency are unavailable, turnover is high, and current primary evidence reports weak/unstable short-horizon effects after costs. A slower daily seam should be falsified first. |

## Mapping to the Existing Repository

The Rust backtest kernel already provides deterministic single-instrument tapes,
Spot inventory/buying-power checks, taker fees/slippage, equity accounting, and a
basic walk-forward runner. It also correctly rejects maker fills and identified
perpetual tapes without a derivatives model.

G-004 still needs the minimum credible seam before any candidate is run:

1. a checksum-bound Binance Spot kline manifest/parser with strict interval and
   timestamp validation;
2. an explicit decision-at-close / execution-at-next-open boundary so the
   current event cannot both create and fill a signal;
3. train/validation/test windows with forecast-horizon embargo plus one terminal
   final holdout that the selector cannot inspect;
4. separate fee/spread/slippage/latency inputs and deterministic 1x/2x cost
   projection;
5. turnover, exposure, profit factor, trade count, benchmark delta, window
   stability, and uncertainty outputs required by the runbook;
6. small, pure adapters for B-0, B-1, C-1, C-2, and C-3 only; and
7. regressions proving missing bars, mixed instruments, current/open bars,
   unsupported maker orders, and all perpetual tapes fail closed.

Do not add a multi-asset portfolio engine, generic optimizer, downloader daemon,
new dependency, or live/Testnet authority for these candidates.

## G-005 Search and Promotion Guardrail

The provisional ceilings above are deliberately below the runbook limit of 20
configurations per family. G-005 may evaluate at most these three families and
must freeze its exact configuration list, metrics, walk-forward boundaries,
embargo, cost schedules, benchmarks, and terminal holdout before the terminal
holdout is read. Validation can reduce the list; it cannot expand it after any
holdout result.

`promising` retains the runbook's complete conjunctive definition: untouched
holdout net return positive after costs; median OOS Sharpe at least 1.0; profit
factor at least 1.2; maximum drawdown no worse than 20%; at least 60% of
walk-forward windows positive; and return still positive at 2x modeled costs.
Passing would mean only offline promising—not paper-observable, Testnet-proven,
mainnet-ready, or a return promise.
