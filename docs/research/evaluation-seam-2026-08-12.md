# Leakage-Resistant Spot Evaluation Seam — 2026-08-12

## Decision

G-004 adds a separate, deterministic daily-bar evaluation seam in
`crypto-trading-backtest`. It does not change the existing quote/event engine,
whose strategies intentionally decide and fill on the same event. The new seam
is the only approved path for the G-003 kline candidates because it observes a
completed close and cannot fill that decision before the next bar open.

The seam is Binance Spot, single-instrument, long-or-cash, taker-only, and
offline. Perpetual input is rejected. It does not query current `exchangeInfo`,
current book ticker, private commission truth, Testnet, or mainnet.

## Frozen Data Contract

`SpotKlineDataset::parse_csv` accepts decompressed official Binance 12-column
Spot kline CSV bytes plus a manifest and archive checksum evidence.

The manifest records:

- official archive URL and retrieval time;
- venue, Spot product, symbol, cadence, UTC timezone, and timestamp unit;
- published archive SHA-256 and decompressed-content SHA-256;
- parser version;
- expected first open, last close, and exact bar count.

The parser computes SHA-256 over the exact CSV bytes locally. The separately
observed archive digest must match the manifest's published digest. It then
rejects malformed columns or numbers, invalid OHLC/volume state, a wrong
millisecond/microsecond declaration, an incorrect close tick, missing,
duplicate, overlapping, or non-contiguous bars, endpoint/count drift, a
non-official/non-Spot/non-UTC manifest, and a terminal bar that was not closed
at the dataset seal time.

This preserves the archive bytes as the historical truth. Current listing
status, current trading filters, and current spread are deliberately not
backfilled into history. The single `BTCUSDT` venue sample still has venue and
survivorship limitations; those remain model risk, not hidden metadata.

## Causal Execution Contract

For an evaluation range:

1. a strategy receives only bars through a completed close;
2. its target in `[0, 1]` becomes pending;
3. that target can first rebalance at the next bar open;
4. an OOS range may use completed pre-range history and therefore can trade its
   first open, but starts from cash rather than inheriting an in-sample fill;
5. any remaining long position is liquidated at the common terminal close with
   the same adverse taker cost schedule.

The terminal close is a reporting convention, not a strategy-generated
same-close fill. It is applied identically to every candidate and the passive
baseline. A 100% target is capped at the maximum quantity affordable after
adverse price impact and fees, so cash cannot become negative. Short, leverage,
maker, borrow, margin, liquidation, funding, and contract-size semantics are
not approximated.

## Costs and Metrics

Each side declares four independent basis-point components:

- taker fee;
- half-spread proxy;
- adverse slippage;
- adverse decision/execution-latency proxy.

The non-fee components move a buy price upward and a sell price downward. Fees
are charged on adverse fill notional. The ledger and report retain every
component and their exact sum. `CostSchedule::doubled` doubles each component,
not a fitted aggregate, providing the fixed 2x sensitivity schedule required by
G-005.

Reports include ending equity, net return, component and total costs, reference
notional turnover, trade count, average gross exposure, daily-frequency
annualized volatility, Sharpe, Sortino, profit factor, win rate, and maximum
drawdown. Window stability, benchmark delta, and uncertainty are cohort-level
G-005 aggregates rather than single-run fields.

## Walk-Forward and Holdout Discipline

`EvaluationPlan` creates rolling training, embargo, and OOS test ranges and
stops them before one terminal holdout. A plan with no complete OOS window
fails closed. `SelectionPhase` exposes only bars before the holdout. Freezing
consumes that phase and requires a non-empty, unique registry capped at five
families and twenty configurations per family, plus one `EvaluationProtocol`
that fixes initial cash and the componentwise 1x cost schedule. Opening the
final holdout then consumes the frozen phase; each registered configuration is
rebuilt with fresh state and evaluated internally at both 1x and 2x costs.

The type boundary prevents the normal runner from opening the holdout before
configuration registration or opening one frozen phase twice. It cannot stop a
caller from deliberately retaining a separate copy of raw data, so G-005 must
also record the exact process order and must not rerun selection after reporting
holdout results.

## Bounded Adapters

The pure adapters are exactly the G-003 set:

- B-0 `CashStrategy`;
- B-1 `BuyAndHoldStrategy`;
- C-1 `SlowTimeSeriesMomentum` using a completed trailing return and fixed
  rebalance cadence;
- C-2 `LongOnlyDonchian` using a strict current-close breakout over prior
  completed closes and a non-decreasing midpoint exit;
- C-3 `CappedVolatilityTarget` using completed-close sample volatility, a hard
  1x cap, and a fixed rebalance band; zero observed variance maps to cash.

Invalid lookbacks, targets, bands, target exposures, duplicate registrations,
and search-budget excess are typed failures.

## Hand-Worked Deterministic Proof

The contract fixture uses three daily bars, initial cash `1000`, and one
round-trip quantity of `1`:

- reference buy `100`, reference sell `110`;
- fee `10 bps`, half-spread `5 bps`, slippage `10 bps`, latency `5 bps`;
- buy fill `100.2`, sell fill `109.78`;
- fees `0.1002 + 0.10978 = 0.20998`;
- half-spread `0.105`, slippage `0.21`, latency `0.105`;
- total modeled costs `0.62998`;
- ending equity `1009.37002`, net return `0.00937002`, turnover `0.21`.

The exact ledger, split, next-open, terminal-exit, deterministic-replay, and
1x/2x contracts run with:

```powershell
cd rust
cargo +1.89.0 test --locked -p crypto-trading-backtest --all-targets --all-features -- --nocapture
cargo +1.89.0 clippy --locked -p crypto-trading-backtest --all-targets --all-features -- -D warnings
```

## Exact G-005 Entry Point

The experiment driver must remain a thin offline consumer of this sequence:

1. `Sha256Digest::from_bytes` and `SpotKlineDataset::parse_csv`;
2. `EvaluationSplitConfig` and `SelectionPhase::new`;
3. one `EvaluationProtocol::new(initial_cash, one_x_costs)` plus the fixed
   concrete G-003 configuration registry;
4. `EvaluationProtocol::evaluate` over every provenance-bound OOS
   `window_sample`, recording both 1x and 2x results from fresh strategy state;
5. configuration and protocol freeze with `SelectionPhase::freeze`;
6. one consuming `open_final_holdout` call followed only by
   `evaluate_registered(identifier)` for the frozen cohort.

G-005 will add only the local file/report orchestration around this API and save
machine-readable output under `artifacts/strategy-evaluation/`. It must not add
an exchange adapter or alter these execution semantics after reading holdout
results.

`SelectedExperiment::persist_selection_with` is an explicit trust boundary,
not proof of durable storage. The type-state transition proves only that the
supplied callback returned success before holdout access became possible. A
production runner must write the selection artifact atomically, sync it before
returning success, and verify the rerun hashes; a no-op callback such as
`|_, _| Ok(())` is suitable only for tests that exercise ordering semantics.

## Known Model Risk

- Daily klines contain no historical bid/ask, queue, or executable depth;
  spread, slippage, latency, and capacity remain declared proxies.
- The fee is a conservative assumption, not authenticated account truth.
- Terminal liquidation at the last close is artificial but common and
  cost-matched.
- Independent OOS windows cash-start; they do not inherit an in-sample
  position. Pre-window completed history is available only for signal state.
- USDT yield and depeg risk are not modeled.
- Results can support an offline `promising` label only after G-005; they do not
  establish Paper, Testnet, mainnet, or investment readiness.
