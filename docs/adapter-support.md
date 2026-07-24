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
| Binance | implemented | protocol-only | protocol-only | request-only | unavailable |
| EdgeX | config-only | unavailable | config-only | unavailable | unavailable |
| GRVT | config-only | unavailable | config-only | unavailable | unavailable |
| Hyperliquid | unavailable | protocol-only | protocol-only | request-only | unavailable |
| Lighter | config-only | unavailable | config-only | unavailable | unavailable |
| OKX | unavailable | unavailable | unavailable | unavailable | unavailable |
| PaperExchange | not-applicable | not-applicable | not-applicable | implemented | not-applicable |
| Paradex | config-only | unavailable | config-only | unavailable | unavailable |
| Variational | unavailable | unavailable | unavailable | unavailable | unavailable |
<!-- adapter-matrix:end -->

`implemented` is not synonymous with production-ready. Binance public data is
read-only and one-shot; PaperExchange is process-local. Every external venue
still reports `live: unavailable`, and the manifest validation rejects any
live claim while `live_trading_enabled` is false.
