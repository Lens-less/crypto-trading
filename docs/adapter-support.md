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
| Backpack | config-only | unavailable | config-only | unavailable | unavailable |
| Binance | implemented | implemented | implemented | implemented | unavailable |
| EdgeX | config-only | unavailable | config-only | unavailable | unavailable |
| GRVT | config-only | unavailable | config-only | unavailable | unavailable |
| Hyperliquid | implemented | protocol-only | protocol-only | request-only | unavailable |
| Lighter | config-only | unavailable | config-only | unavailable | unavailable |
| OKX | unavailable | unavailable | unavailable | unavailable | unavailable |
| PaperExchange | not-applicable | not-applicable | not-applicable | implemented | not-applicable |
| Paradex | config-only | unavailable | config-only | unavailable | unavailable |
| Variational | unavailable | unavailable | unavailable | unavailable | unavailable |
<!-- adapter-matrix:end -->

`implemented` is not synonymous with production-ready. Binance public data is
read-only credential-free REST polling; optional update IDs are retained as
source sequences, while the documented response has no venue event timestamp
and is therefore marked with explicit local-receipt-time provenance.
Hyperliquid public data is read-only credential-free polling of the perpetual
asset contexts (impact prices plus an hourly funding-rate side channel), also
without a venue event timestamp. The runtime rejects cross-venue pairs beyond
an explicit skew bound, but neither adapter is a realtime stream or grants
order/account authority. Binance's Testnet lifecycle owner has deterministic
submit-query-cancel/query-first recovery coverage, and its report-first account
gate compares signed balances, open orders, and positions to one exact
committed Paper reservation. Real credentialed Spot/USD-M reconciliation,
open-order, partial-fill, restart, and 24-hour soak evidence remain external
release gates. PaperExchange is process-local. Every external venue still
reports `live: unavailable`, and the manifest validation rejects any live claim
while `live_trading_enabled` is false.

The journal-backed Paper account above the adapter now settles fully filled
synchronous taker receipts into exact FIFO lots, immediate fees, realized PnL,
settled equity, and reduce-only capacity. This does not widen adapter authority:
resting-maker callbacks, funding, mark-to-market, margin/liquidation rules,
queue/depth impact, and external account truth remain unavailable.

Offline research support is separately exposed as `research.indicators` and
`research.backtest` in the capability manifest. The current backtest is a
deterministic single-instrument kernel; it does not yet share the production
market-event/strategy adapter seam and is not evidence of paper/live parity or
profitability.
