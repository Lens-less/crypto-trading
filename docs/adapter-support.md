# Rust adapter support matrix

This matrix is a human-readable projection of
`crypto-trading capabilities --json`. A contract test compares the rows below
with the versioned runtime manifest, so documentation cannot silently claim
more authority than the CLI or future Web Integrations page.

Status meanings:

- `implemented`: the current Rust adapter path exists and has deterministic
  contract coverage.
- `protocol-only`: offline request/response contracts exist, but real
  credentials and a complete testnet lifecycle have not been verified.
- `request-only`: request routes and payload construction exist, but response
  parsing and an authoritative reconciliation receipt do not.
- `config-only`: Rust can parse or redact the venue configuration, but no
  adapter consumes it.
- `unavailable`: the current Rust system has no supported path for that facet.
- `not-applicable`: the facet does not apply to the process-local paper model.

<!-- adapter-matrix:start -->
| Adapter | Public data | Testnet protocol | Authenticated | Reconcile | Live |
| --- | --- | --- | --- | --- | --- |
| Binance | implemented | implemented | implemented | implemented | implemented |
| Hyperliquid | implemented | protocol-only | protocol-only | request-only | unavailable |
| PaperExchange | not-applicable | not-applicable | not-applicable | implemented | not-applicable |
<!-- adapter-matrix:end -->

`implemented` is not synonymous with production-ready. Binance public data has
a read-only Spot Testnet `bookTicker` WebSocket source with bounded buffering,
ping/pong liveness, reconnect backoff, and update-ID regression checks. The
explicit polling fallback retains optional update IDs as source sequences and
marks REST snapshots with local-receipt-time provenance. Hyperliquid public
data remains credential-free polling of perpetual asset contexts (impact
prices plus an hourly funding-rate side channel), without a venue event
timestamp. The runtime rejects cross-venue pairs beyond an explicit skew bound;
none of these read paths grants order/account authority. Binance's Testnet
lifecycle owner has deterministic
submit-query-cancel/query-first recovery coverage, and its report-first account
gate compares signed balances, open orders, and positions to one exact
committed Paper reservation. Real credentialed Spot/USD-M reconciliation,
open-order, partial-fill, restart, and 24-hour soak evidence remain external
release gates. PaperExchange is process-local.

Binance `live: implemented` means exactly one authority: the
operator-acknowledged one-shot Spot LIMIT order lifecycle (`live-lifecycle`,
which requires an exact acknowledgement phrase and a `--max-notional` cap)
plus the read-only signed `live-reconcile` report over dedicated mainnet read
credentials. There is no autonomous strategy live execution, no market
orders, no margin, no USDⓈ-M product, and no multi-symbol owner loop;
`ExecutionMode::Live` for grid/arbitrage keeps failing closed, and a
credentialed supervised mainnet run remains external release evidence.
Hyperliquid still reports `live: unavailable`, and manifest validation
rejects any live claim whenever `live_trading_enabled` is false.

Venues without an operator-supported Rust adapter path (for example Backpack,
EdgeX, GRVT, Lighter, Paradex, OKX, and Variational) are out of scope for the
live V1 effort: their configuration samples were removed from the working tree
and the manifest no longer lists them, so documentation cannot advertise a
wider venue matrix than the runtime can actually honor.

The journal-backed Paper account above the adapter now settles fully filled
synchronous taker receipts into exact FIFO lots, immediate fees, realized PnL,
settled equity, and reduce-only capacity. This does not widen adapter authority:
resting-maker callbacks, funding, mark-to-market, margin/liquidation rules,
queue/depth impact, and external account truth remain unavailable.

`research.backtest` is now an available offline capability through the formal
`crypto-trading-research` binary. Its frozen candidate registry consumes the
same pure bar-strategy implementations as the paper bar owner. Availability is
not an edge claim: the earlier daily experiment had no passing configuration,
and the first hourly protocol stopped at data admission before selection or
holdout because the official history is not a contiguous UTC-hour series.
`research.indicators` remains an unavailable library-only product capability.
