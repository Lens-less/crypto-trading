# G-003 Handoff - Current Strategy Candidate Research

## Current Status

- Status: done under the root Goal
- Claim token: `b0c84124-0e81-4c09-a93f-23cf055559d5`
- Claimed at: 2026-08-12T01:51:46+08:00
- Claimed by thread: `019ff1a7-229a-71d1-94c6-548f93748f08`
- Last heartbeat: 2026-08-12T02:08:24+08:00
- Attempt: 1
- Output: `docs/research/strategy-candidates-2026-08-12.md`

## Source Documents

- `docs/automation/goal-automation-runbook.md`
- `docs/automation/goal-board.md`
- `docs/automation/handoffs/issue-g-002-handoff.md`
- `.workflow/ultracode/trading-safety-strategy-evaluation-20260812/eval-contract.md`
- the runtime capability manifest and current backtest/scanner limitations

## Scope and Acceptance

- Browse current primary sources as of 2026-08-12: papers/preprints, official
  exchange/data documentation, and original repositories. Record publication
  and access dates with direct links.
- Select three to five strategy families plus at least one simple trend or
  buy-and-hold baseline that are honestly testable with public Spot data.
- For every candidate record hypothesis, instruments, cadence, required data,
  execution assumptions, turnover/capacity risk, failure regimes, and a
  falsifying test.
- Reject families that require unavailable L2 queue, borrow, liquidation,
  funding, contract multiplier, or private account truth.
- Do not implement strategies, tune parameters, inspect a final holdout, call a
  candidate profitable, or create any external trading authority in G-003.

## Safety Boundary

- `live_trading_enabled=false`; mainnet remains unavailable.
- No credentials and no external order or cancel, including Testnet.
- Public read-only internet research is allowed; all execution evidence remains
  offline, Paper, deterministic mock, or loopback-only.
- Research is not investment advice and is not evidence of profitability.

## Worker Log

- 2026-08-12T01:51:46+08:00 - The root Goal compared the board, closed the
  fully evidenced G-002 claim, and claimed G-003 with attempt 1. The next action
  is parallel primary-source research over independent candidate families,
  followed by root synthesis and feasibility rejection against the actual
  evaluation/backtest constraints.
- 2026-08-12T02:02:42+08:00 - Root browsing checked current publisher papers,
  preprints, official Binance Spot/API/data/commission documentation, and an
  original research repository. A first complete dated artifact now advances
  slow time-series momentum, causal Donchian trend, and capped volatility
  control on one Binance Spot instrument, with cash/buy-and-hold baselines. It
  explicitly rejects short, cross-sectional, maker/grid, perpetual/funding,
  L2/latency, and ML expansion for this cycle. No market-data sample, tuning
  window, or final holdout was read. Independent research lanes are being
  reconciled before acceptance.
- 2026-08-12T02:08:24+08:00 - Official Binance public-market-data and fee
  sources were rechecked against the current docs: public Spot market data only
  URLs, `data-api.binance.vision`, `exchangeInfo`, `klines`, `trades`,
  `aggTrades`, `depth`, `bookTicker`, and commission endpoints. The archive
  paths exposed to this cycle are daily/monthly Spot `trades`, `aggTrades`, and
  `klines` with published `.CHECKSUM` files; there is no official historical
  `bookTicker` or point-in-time `exchangeInfo` archive, so spread, queue, and
  listing-universe survivorship remain unmodeled and must stay called out as
  limitations.
- 2026-08-12T02:08:24+08:00 - The final research artifact was reviewed as
  complete: it now records the primary-source ledger, the Spot-only execution
  contract, the mandatory baselines, the three advanceable candidate families,
  the rejected families, and the hard G-005 search guardrail. No market-data
  sample, tuning window, or final holdout was inspected.

## Risks and Next Step

- Recent papers can overstate results through leakage, unavailable microstructure
  inputs, unrealized costs, or derivatives mechanics; source recency is not a
  quality substitute.
- Prefer Spot and public OHLCV/trade/top-of-book data. Perpetual candidates stay
  rejected unless all required financial mechanics already exist and are
  tested, which they currently do not.
- G-003 is complete. Start G-004 from the new evaluation-seam handoff and keep
  the final holdout untouched until G-005.
