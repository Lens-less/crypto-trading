# Legacy Config Archive

`exchanges/` holds compatibility-only exchange-auth samples and market metadata
for venues that no longer appear on the active operator surface.

W3 moves Backpack, EdgeX, GRVT, Lighter, and Paradex here so
`config/exchanges/` only retains operator-supported Binance and Hyperliquid
profiles. These legacy files remain parseable by `config-check`, but no Rust
runtime adapter consumes them for market data, authenticated APIs, or live
trading authority.
