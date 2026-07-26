# Offline exchange hardening specification

## Goal

Make the repository safe and deterministic to exercise without real exchange
credentials while preparing Binance Spot, Binance USDⓈ-M perpetual, and
Hyperliquid Spot/perpetual integrations for later testnet verification.

## Required behavior

1. Checked-in executable configuration must pass its own loader; no configuration
   may rely on an undocumented companion-file merge.
2. Standard symbols and exchange wire symbols must use an explicit, bounded,
   bidirectional catalog keyed by exchange and market type. Adapters must not
   guess how to split concatenated symbols such as `BTCUSDT`.
3. Instrument rules must be reusable by remote adapters and must fail closed
   when an exact exchange/symbol/market rule is missing.
4. Binance endpoint selection must distinguish Spot from USDⓈ-M and accept only:
   - official testnet/demo HTTPS hosts, or
   - explicitly constructed loopback HTTP endpoints for offline tests.
5. Hyperliquid endpoint selection must accept only the official testnet HTTPS
   host or an explicitly constructed loopback HTTP endpoint.
6. Authenticated protocols must expose injectable signer seams. Tests use
   deterministic signers and assert the exact payload presented for signing.
   Secrets and private keys must never appear in request diagnostics.
7. Offline protocol tests must cover request construction, both product types,
   response parsing, malformed responses, rejected responses, transport
   failures, and ambiguous order outcomes.
8. Mainnet trading and CLI live execution remain fail-closed. This task does not
   silently convert a testnet-capable protocol into mainnet authority.

## Acceptance checks

- Binance Spot and USDⓈ-M use their correct testnet hosts and route families.
- Hyperliquid requests use the testnet `/info` and `/exchange` surfaces.
- Order type, side, time-in-force, reduce-only, client-order-id, and exact decimal
  values survive request construction.
- A transport failure or server-side failure after an order/cancel dispatch is
  represented as an ambiguous outcome requiring reconciliation.
- Read-only response parsing cannot create a symbol or market type that was not
  present in the explicit catalog.
- `cargo fmt --check`, `cargo check`, `cargo clippy`, the full workspace test
  suite, documentation tests, and dependency audit pass.

## Explicitly deferred until credentials and dependency approval exist

- Real HMAC-SHA256 signing for Binance.
- Real secp256k1/Keccak/EIP-712 signing for Hyperliquid.
- Real testnet order placement, cancellation, reconciliation, WebSocket
  reconnect/backfill, rate-limit behavior, and exchange-specific permission
  checks.

The deferred items cannot be truthfully proven from mocks. They require
least-privilege testnet credentials, approved cryptographic dependencies, and
network access to the exchanges' test environments.
