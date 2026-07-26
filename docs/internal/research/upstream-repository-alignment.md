# Upstream Repository Alignment

Sources pinned for this snapshot:
- `cryptocj520/crypto-trading-open` HEAD `620737399bfe3c331f9989fc77d631536f2e89df`
- `shy3130/tickflow-stock-panel` HEAD `60fe9e6fa61dd774968d483cb8466b4b485e7ad0`
- Local workspace HEAD `58d2626d1199881a6331a4df2aed9084c26288c8`

Legend:
- **definite** = directly observed in source
- **inferred** = reasonable synthesis from multiple sources
- **needs verification** = plausible but not fully sampled in this pass

## 1) `crypto-trading-open`

**Repo URL:** <https://github.com/cryptocj520/crypto-trading-open>

### Functional scope

| Conclusion | Status | Evidence |
|---|---|---|
| The project is a multi-exchange crypto automation system centered on segmented arbitrage, with grid trading, volume making, price alerts, and market monitoring as first-class modes. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| The repo is organized around scripted entrypoints that load a shared orchestrator/runtime layer rather than a single monolith. | inferred | [`main_unified.py`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/main_unified.py), [`run_arbitrage_monitor_v2.py`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/run_arbitrage_monitor_v2.py) |
| Supported exchange coverage is broad: Hyperliquid, Backpack, Lighter, Paradex, Binance, OKX, EdgeX, GRVT, Variational. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |

### Architecture and run constraints

| Conclusion | Status | Evidence |
|---|---|---|
| The README explicitly describes a layered architecture with adapters, data aggregation, events, logging, and config management. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| Runtime entrypoints depend on `.env`, `dotenv`, and explicit config files; `main_unified.py` forces certifi-backed SSL roots before other network imports. | definite | [`main_unified.py`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/main_unified.py) |
| Supported environments are Python 3.8+ with a recommendation for Python 3.12, on Linux/macOS/Windows, with optional `tmux` for process management. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| The repo carries split dependency manifests for the main environment, Python 3.12, and a Lighter spot-only environment. | definite | [`requirements.txt`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/requirements.txt), [`requirements-py312.txt`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/requirements-py312.txt), [`requirements-lighter-spot.txt`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/requirements-lighter-spot.txt) |
| Exchange API secrets are expected under `config/exchanges/`, one YAML per exchange. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |

### Capability matrix

| Area | Capability | Status | Evidence |
|---|---|---|---|
| Exchange | Live and testnet exchange adapters across the named venues. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| Strategy | Segmented arbitrage, historical spread/funding arbitrage, grid trading, volume maker, and price alert flows. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md), [`main_unified.py`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/main_unified.py), [`run_arbitrage_monitor_v2.py`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/run_arbitrage_monitor_v2.py) |
| Data | Historical spread recorder, funding-rate based decisioning, config-driven market snapshots, and a multi-file config system. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| Risk | Price stability checks, counterparty liquidity checks, reduce-only management, error-avoidance controls, dynamic slippage protection. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| Notification | Price alert / terminal UI style feedback is present; external push integration was not confirmed in the sampled surfaces. | needs verification | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| Deployment | Local scripts, per-mode venvs, optional `tmux`, and a `docker/` area for packaging/docs. | definite | [`README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |

### Alignment take

- The repo is a trading execution system, not a web analytics panel.
- The strongest signal is the orchestration model: each mode is an entrypoint into a shared service layer, with config-driven behavior and explicit fail-closed constraints.
- The current Rust refactor is already closer to this repo than to the tickflow panel in terms of domain shape.

## 2) `tickflow-stock-panel`

**Repo URL:** <https://github.com/shy3130/tickflow-stock-panel>

### Web information architecture

| Conclusion | Status | Evidence |
|---|---|---|
| The product is a self-hosted A-share quant workbench for selection, monitoring, backtesting, analysis, and review. | definite | [`README.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/README.md) |
| The frontend page surface includes `Dashboard`, `Watchlist`, `Indices`, `Screener`, `Backtest`, `Monitor`, `Review`, `Settings`, `StockAnalysis`, `Financials`, `ConceptAnalysis`, `IndustryAnalysis`, `LimitUpLadder`, plus auth/onboarding and utility pages. | definite | [`frontend/src/pages`](https://github.com/shy3130/tickflow-stock-panel/tree/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/frontend/src/pages) |
| The component taxonomy mirrors the domain: `data`, `ext-data`, `financials`, `monitor`, `screener`, `signals`, `stock-analysis`, `stock-table`, `virtual-list`, plus chart, toast, modal, and layout primitives. | definite | [`frontend/src/components`](https://github.com/shy3130/tickflow-stock-panel/tree/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/frontend/src/components) |

### Tech stack and data interactions

| Conclusion | Status | Evidence |
|---|---|---|
| Backend stack: FastAPI, Pydantic v2, APScheduler, and `sse-starlette`. | definite | [`README.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/README.md) |
| Data stack: Polars for compute, DuckDB for query, Parquet for storage, vectorbt for backtesting. | definite | [`README.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/README.md) |
| Frontend stack: React 18, Vite, TypeScript, Tailwind, Tanstack Query, Lightweight Charts, ECharts, and dnd-kit. | definite | [`README.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/README.md) |
| The main data source is TickFlow, but the system also supports custom HTTP-backed data sources that map `daily`, `adj_factor`, and `realtime` datasets into internal standard fields. | definite | [`docs/custom-data-source.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/custom-data-source.md) |
| The custom-data-source design is API-driven and reloadable at runtime through `POST /api/settings/data-sources/reload`. | definite | [`docs/custom-data-source.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/custom-data-source.md) |
| The pipeline runs on a schedule, with a 15:30 CST post-market refresh for daily K, enriched recomputation, and monitor evaluation. | definite | [`docs/features.md`](https://raw.githubusercontent.com/shy3130/tickflow-stock-panel/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/features.md) |
| Deployment is explicitly supported in dev mode and Docker; Docker is a two-stage build that copies frontend dist into the backend image. | definite | [`docs/deployment.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/deployment.md) |

### What is worth borrowing

| Borrow candidate | Status | Why |
|---|---|---|
| Page taxonomy and IA separation by task: screening, backtest, analysis, monitor, settings, data, review. | definite | It gives a usable mental model for a panel without flattening everything into one dashboard. |
| The data-source abstraction and runtime reload path. | definite | It cleanly separates ingestion from the analytics surface and keeps extension points obvious. |
| SSE-backed long-running job feedback and persisted task/alert history. | definite | Useful if the Rust project gets a web/API layer for monitoring and backtests. |
| Strong component slicing by domain, not by generic UI widget type. | definite | Reduces page-level coupling and matches the product vocabulary. |

### What should not be borrowed wholesale

| Do not borrow | Status | Reason |
|---|---|---|
| The `stock-sdk` scraping plugin path as a default dependency. | definite | The deployment doc flags it as a compliance-sensitive exception and excludes it from default Docker builds. [`docs/deployment.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/deployment.md) |
| The entire React/Tailwind/chart stack before the Rust backend exposes equivalent API contracts. | inferred | It would front-load UI cost without resolving missing execution/data semantics. |
| The AI strategy prompt surface before the strategy schema is stable. | inferred | The strategy guide is powerful, but it is a better fit after the core data model and execution semantics are fixed. [`backend/app/strategy/prompts/strategy-guide.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/backend/app/strategy/prompts/strategy-guide.md) |

## 3) Current Rust refactor sample

**Workspace path:** `C:\Users\28340\Desktop\crypto-trading\rust`

### Observed surface

| Conclusion | Status | Evidence |
|---|---|---|
| The Rust refactor is a workspace with `apps`, `config`, `domain`, `exchange`, `runtime`, and `strategy` crates. | definite | [`rust/Cargo.toml`](C:/Users/28340/Desktop/crypto-trading/rust/Cargo.toml) |
| The public command surface is CLI-first: `grid`, `arbitrage`, `monitor`, `volume-maker`, `price-alert`, `scanner`, `config-check`. | definite | [`rust/crates/apps/src/cli.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/src/cli.rs) |
| The command runner deliberately fail-closes live and continuous paths; only paper one-shot execution is allowed today. | definite | [`rust/crates/apps/src/command.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/src/command.rs) |
| The runtime layer provides execution batching and append-only JSONL history, with explicit live-mode acknowledgement checks. | definite | [`rust/crates/runtime/src/execution.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/runtime/src/execution.rs), [`rust/crates/runtime/tests/runtime_contract.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/runtime/tests/runtime_contract.rs) |
| The exchange layer is typed and bounded, with paper execution, remote transport abstractions, testnet protocols, and an `UnsupportedLiveExchange` seam. | definite | [`rust/crates/exchange/src/lib.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/lib.rs), [`rust/crates/exchange/src/paper.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/paper.rs), [`rust/crates/exchange/src/remote.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/remote.rs) |
| The strategy layer already contains core machines for arbitrage, grid, alerts, risk, virtual grid, and volume maker. | definite | [`rust/crates/strategy/src/lib.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/src/lib.rs) |

### What the tests prove

| Conclusion | Status | Evidence |
|---|---|---|
| CLI contract tests lock the supported subcommands and their strict argument combinations. | definite | [`rust/crates/apps/tests/cli_contract.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/tests/cli_contract.rs) |
| Integration tests enforce fail-closed behavior for missing live runtimes, unavailable monitor/volume-maker/price-alert/scanner modes, and history-file failures. | definite | [`rust/crates/apps/tests/command_smoke.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/tests/command_smoke.rs) |
| Strategy tests already cover segmented arbitrage, alerts, risk, and volume-maker logic as pure deterministic engines. | definite | [`rust/crates/strategy/tests/segmented_arbitrage.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/tests/segmented_arbitrage.rs), [`rust/crates/strategy/tests/price_alert.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/tests/price_alert.rs), [`rust/crates/strategy/tests/risk_engine.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/tests/risk_engine.rs), [`rust/crates/strategy/tests/volume_maker.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/tests/volume_maker.rs) |

## 4) Gap matrix

### P0

| Gap | Why it is P0 | Evidence |
|---|---|---|
| Live / continuous execution is still missing. `grid`, `arbitrage`, `monitor`, `volume-maker`, `price-alert`, and `scanner` all fail closed except for paper one-shot and config validation. | This blocks parity with the crypto trading upstream if the goal is operational execution rather than just strategy math. | [`rust/crates/apps/src/command.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/src/command.rs), [`rust/README.md`](C:/Users/28340/Desktop/crypto-trading/rust/README.md) |
| There is no web/API panel equivalent to `tickflow-stock-panel`'s dashboard, watchlist, screener, backtest, analysis, monitor, review, and settings flow. | This is a hard gap only if the target product includes an operator-facing UI. | [`rust/Cargo.toml`](C:/Users/28340/Desktop/crypto-trading/rust/Cargo.toml), [`rust/crates/apps/src/cli.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/apps/src/cli.rs), [`tickflow-stock-panel/frontend/src/pages`](https://github.com/shy3130/tickflow-stock-panel/tree/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/frontend/src/pages) |

### P1

| Gap | Why it is P1 | Evidence |
|---|---|---|
| Exchange support is still narrow relative to `crypto-trading-open`; the Rust sample has a typed exchange boundary plus paper/testnet/unsupported-live seams, but not the broad live venue matrix. | Execution coverage and venue parity are the core product delta versus the Python upstream. | [`rust/crates/exchange/src/lib.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/lib.rs), [`crypto-trading-open/README.md`](https://github.com/cryptocj520/crypto-trading-open/blob/620737399bfe3c331f9989fc77d631536f2e89df/README.md) |
| The Rust tree has history persistence, but not a scheduled data pipeline, enriched store, or backtest/reporting stack like TickFlow. | This matters if the project must support analytics, monitoring, or operator review, not just order logic. | [`rust/crates/runtime/src/history.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/runtime/src/history.rs), [`tickflow-stock-panel/docs/features.md`](https://raw.githubusercontent.com/shy3130/tickflow-stock-panel/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/features.md) |
| Notifications are not yet a product surface in the Rust tree. | `tickflow-stock-panel` demonstrates alerting and review UX; the Rust tree currently only has alert math, not delivery or presentation. | [`rust/crates/strategy/src/alert.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/src/alert.rs), [`tickflow-stock-panel/docs/features.md`](https://raw.githubusercontent.com/shy3130/tickflow-stock-panel/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/features.md) |

### P2

| Gap | Why it is P2 | Evidence |
|---|---|---|
| The Rust code already has strong core boundaries, so the best next step is to preserve those seams and add only the thinnest possible operator surface around them. | The existing crate split is a good foundation; overbuilding UI or plugin layers too early would add churn. | [`rust/Cargo.toml`](C:/Users/28340/Desktop/crypto-trading/rust/Cargo.toml), [`rust/crates/strategy/src/lib.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/strategy/src/lib.rs), [`rust/crates/exchange/src/lib.rs`](C:/Users/28340/Desktop/crypto-trading/rust/crates/exchange/src/lib.rs) |
| Tickflow-style data-source extensibility is attractive, but only after the Rust project has a stable ingestion contract. | Otherwise the extension surface will just mirror changing internals. | [`tickflow-stock-panel/docs/custom-data-source.md`](https://github.com/shy3130/tickflow-stock-panel/blob/60fe9e6fa61dd774968d483cb8466b4b485e7ad0/docs/custom-data-source.md) |

## 5) Recommendations

1. Keep the Rust refactor as the execution core, not a UI clone.
2. If a panel is required, add a thin web/API layer around the existing Rust crates instead of porting the entire TickFlow frontend verbatim.
3. Port the `tickflow-stock-panel` data-source abstraction only if analytics/backtesting is in scope; otherwise it is unnecessary complexity.
4. Treat `crypto-trading-open` as the execution parity benchmark and close the live/continuous runtime gap before broadening features.
5. Do not default to `stock-sdk`-style scraping integrations unless the compliance and deployment model is explicitly approved.

## 6) Bottom line

- **`crypto-trading-open`** is the better reference for execution and trading-domain parity.
- **`tickflow-stock-panel`** is the better reference for operator UX, analytics IA, and data-source extensibility.
- **The current Rust refactor** is already strong on typed boundaries, deterministic paper execution, and config validation, but it is still missing the live/continuous runtime and the entire operator-facing web surface.
