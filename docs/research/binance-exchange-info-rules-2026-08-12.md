# Binance exchangeInfo rules for Testnet mutation

Prepared: 2026-08-12

Scope: Binance Spot Testnet and USD-M Futures Testnet `exchangeInfo` routes, symbol selection, and filter semantics, with a conservative comparison to the current G-001 implementation.

## Official sources

- Spot Testnet General Info: https://developers.binance.com/en/docs/products/spot/testnet/general-info
- Spot Testnet REST API: https://developers.binance.com/en/docs/products/spot/testnet/rest-api
- Spot Testnet Filters: https://developers.binance.com/en/docs/products/spot/testnet/filters (last modified 2026-08-11)
- USD-M General Info: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info
- USD-M Market Data REST API: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
- USD-M Public Endpoints Info: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition (last modified 2026-08-11)

## Official evidence

### Spot Testnet

- Binance says the Spot Test Network uses the REST base endpoint `https://testnet.binance.vision/api`. The testnet general-info page also says only `/api` endpoints are available on the Spot Test Network. Source: https://developers.binance.com/en/docs/products/spot/testnet/general-info
- The Spot REST API documents `GET /api/v3/exchangeInfo` as the exchange-information route. Source: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general
- The Spot exchangeInfo endpoint accepts `symbol`, `symbols`, `permissions`, `showPermissionSets`, and `symbolStatus`. The docs say invalid `symbol` or `symbols` values cause an error, and `symbolStatus` cannot be combined with `symbol` or `symbols`. Source: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general

### USD-M Futures Testnet

- Binance says the USD-M testnet REST base endpoint is `https://demo-fapi.binance.com`. Source: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info
- The USD-M market-data docs document `GET /fapi/v1/exchangeInfo` as the exchange-information route, and the response includes `symbols[*].symbol`, `pair`, `contractType`, `status`, and `filters`. Source: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
- The USD-M public-endpoints page enumerates `contractType` values including `PERPETUAL` and `status` values including `TRADING`. Source: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition

### Filter semantics

- On Spot, `PRICE_FILTER` explicitly supports zero disabling for `minPrice`, `maxPrice`, and `tickSize`. For enabled parts, Binance says price must be within the enabled bounds and satisfy `price % tickSize == 0`; the documented residue check is zero-anchored, not offset by `minPrice`. Source: https://developers.binance.com/en/docs/products/spot/testnet/filters
- On Spot, `LOT_SIZE` defines `minQty`, `maxQty`, and `stepSize` for quantity and requires `quantity % stepSize == 0`; `MARKET_LOT_SIZE` defines the same three fields for MARKET orders. The docs list `MARKET_LOT_SIZE` as a symbol filter, but they do not state that every symbol must have it. Source: https://developers.binance.com/en/docs/products/spot/testnet/filters
- On Spot, `MIN_NOTIONAL` uses `minNotional`, `applyToMarket`, and `avgPriceMins`. For MARKET orders, Binance uses a reference price if one exists, otherwise VWAP over the preceding `avgPriceMins` minutes, and if `avgPriceMins` is 0 it uses the last price. Source: https://developers.binance.com/en/docs/products/spot/testnet/filters
- On Spot, `NOTIONAL` uses `minNotional`, `maxNotional`, `applyMinToMarket`, `applyMaxToMarket`, and `avgPriceMins`. The same reference-price / VWAP / last-price fallback applies for MARKET orders. Source: https://developers.binance.com/en/docs/products/spot/testnet/filters
- The current Spot REST API also documents public `GET /api/v3/referencePrice`; its `referencePrice` can be null, in which case the filter fallback still matters. Source: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market
- On USD-M, the public filter docs currently document `MIN_NOTIONAL` only, with a `notional` field and the note that MARKET orders use mark price. Source: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition
- On the USD-M public docs available on 2026-08-11, `NOTIONAL`, `applyMinToMarket`, `applyMaxToMarket`, and `avgPriceMins` are not documented on the public USD-M filter pages. Source: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition and https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data

## Current implementation comparison

- The repository hard-codes the official testnet hosts as Spot `https://testnet.binance.vision` and USD-M `https://demo-fapi.binance.com`, and `rest_url` only resolves fixed API paths under those origins. Source: [endpoint.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/endpoint.rs#L22), [endpoint.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/endpoint.rs#L29), [endpoint.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/endpoint.rs#L74)
- `build_exchange_info_request` builds an exact-symbol request by appending `symbol=<wire_symbol>` to `/api/v3/exchangeInfo` for Spot or `/fapi/v1/exchangeInfo` for USD-M. Source: [binance_testnet.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/binance_testnet.rs#L731)
- `parse_exchange_info_symbol` hard-fails unless it receives exactly one matching wire symbol with `TRADING` status, authoritative `baseAsset`/`quoteAsset` matching the canonical standard symbol, and, for USD-M, matching `pair` plus `PERPETUAL` contract type. Source: [binance_testnet.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/binance_testnet.rs)
- `parse_exchange_info_rules` requires `PRICE_FILTER`, `LOT_SIZE`, and exactly one of `MIN_NOTIONAL` or `NOTIONAL`; treats `MARKET_LOT_SIZE` as optional; rejects duplicate/conflicting filters and every unsupported `filterType`; requires the documented Spot market flags and `avgPriceMins`; and rejects undocumented USD-M `NOTIONAL`. Source: [binance_testnet.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/binance_testnet.rs)
- The rule validator applies the documented zero-anchored tick/step formulas, carries `avgPriceMins`, and rejects a MARKET order locally whenever an applied notional constraint would need an authoritative venue reference that the adapter does not yet fetch. This prevents the earlier top-of-book approximation from reaching an order route. Source: [instrument.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/instrument.rs)
- A genuinely fresh lifecycle fetches and preflights authoritative metadata before its durable submit branch. Once a matching `planned` fact exists, recovery deliberately skips metadata bootstrap and constructs query/cancel-only authority, so an exchangeInfo outage or later trading halt cannot block query-first cleanup. Source: [command.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/src/command.rs), [testnet_lifecycle.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/src/testnet_lifecycle.rs)

## Inference and ambiguity

- The Spot and USD-M docs clearly support exact symbol selection by returning exchangeInfo data for a specific symbol, but they do not require the client to submit an exchangeInfo query by `symbol`. The current implementation does use `symbol`, which is conservative and compatible with the Spot docs. Source: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general and [binance_testnet.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/binance_testnet.rs#L731)
- `MARKET_LOT_SIZE` appears in the docs as a symbol filter, but the docs do not say it is mandatory on every symbol. Treating it as optional in the parser is conservative. Source: https://developers.binance.com/en/docs/products/spot/testnet/filters and https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition
- The current USD-M parser rejects `NOTIONAL` because the current public USD-M docs do not document it. Widening that contract requires new primary-source evidence and regression tests. Source: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition and [binance_testnet.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/binance_testnet.rs)

## Safest fail-closed parent action

- Keep the exact wire/canonical asset/status/product gates and unsupported-filter rejection intact. They are aligned with the official identity data and deliberately block mutation when the local adapter cannot prove all returned semantics. Source: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general, https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data, and [binance_testnet.rs](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/binance_testnet.rs)
- Keep MARKET mutation blocked when an applied notional rule requires the official reference-price/VWAP/mark-price mechanism. Adding the appropriate public reference route is a future widening of authority and must be separately sourced and tested; bookTicker is not an equivalent substitute.
- Keep current-metadata parsing out of an already-planned recovery path. Recovery must query and cancel by the durable client/wire identity even when new submissions are halted.

## Bottom line

- Confirmed: fresh Binance Testnet mutation routes through the official product-specific exchangeInfo host/path and exact wire, asset, status, product, and supported-filter gates.
- Confirmed: zero-anchored tick/step residue checks match the published formulas; Spot market flags and `avgPriceMins` are represented; unknown filters and undocumented USD-M `NOTIONAL` fail closed.
- Confirmed: applied market-notional semantics without an authoritative venue reference stop locally, while a durable recovery remains query-first without refetching current trading metadata.
- Recommendation: only widen the supported filter/reference-price set after new primary-source review and deterministic regression coverage.
