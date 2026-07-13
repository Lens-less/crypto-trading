# Rust refactor plan

## Goal

Replace the Python-first runtime with a Rust-first, testable trading engine while
preserving the existing YAML configuration files and the observable behavior of
the main command-line entry points. The Python tree remains available as a
read-only compatibility reference until the Rust replacement passes its gates.

## Baseline

- Python source: 259 files, about 109,700 lines.
- `python -m compileall -q .`: passes.
- `python -m pytest -q`: fails during collection because
  `tools/test_apikey_direct.py` exits when the optional Lighter SDK is absent.
- The legacy tree contains duplicated backup files, broad interfaces, silent
  exception handling, and documented `TODO`/`NotImplemented` paths. Those are
  not treated as behavior to preserve.

## Compatibility seams

The migration is verified through these public seams:

1. Configuration: existing grid, arbitrage, monitor, volume-maker, price-alert,
   exchange, and symbol-conversion YAML can be loaded and validated.
2. Strategy: market snapshots produce deterministic grid, arbitrage, alert, and
   volume decisions without performing I/O.
3. Exchange: one typed asynchronous interface covers market data, account data,
   order submission, cancellation, and subscriptions; production and paper
   adapters satisfy the same interface.
4. Runtime: one `crypto-trading` binary replaces the Python launch scripts with
   subcommands and safe-by-default paper execution.
5. History: observable spread/decision records are written as stable JSONL.

## Module design

- `domain`: money-safe types, orders, positions, market snapshots, and symbols.
- `config`: compatibility deserialization, defaults, validation, and redacted
  credential handling.
- `strategy`: deep modules for grid planning, segmented arbitrage, price alerts,
  volume making, and volatility scoring. Their interface accepts state plus a
  snapshot and returns decisions.
- `exchange`: the external seam. `PaperExchange` is the deterministic adapter
  used by tests; HTTP/WebSocket adapters own protocol-specific translation.
- `risk`: centralized pre-trade checks and kill-switch state.
- `runtime`: adapter preflight, routing, explicit partial outcomes, live
  fail-closed authority, and history output.
- `cli`: argument compatibility and operator-facing output.

## Cleanup order

1. Add Rust tests for each agreed seam and observe them fail before adding the
   corresponding implementation.
2. Build vertical slices: configuration -> strategy decision -> paper adapter ->
   CLI execution.
3. Add live market-data adapters only behind the exchange seam. Private live
   order execution remains unavailable until explicit credentials, mandatory
   risk authorization, signing vectors, testnet proof, reconciliation, and a
   live-mode acknowledgement are all present.
4. Run formatting, compilation, Clippy with warnings denied, unit tests,
   integration tests, and offline CLI smoke tests.
5. Review the diff against both repository standards and this plan.
6. Remove the Python runtime and backup files only after feature-parity gates can
   be demonstrated. Until then, Rust is the default runtime and Python is marked
   legacy rather than silently deleted.

## Acceptance gates

- `cargo +1.85.0 check --workspace --all-targets --all-features`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets --all-features`
- Existing representative YAML files validate through the Rust CLI.
- Paper-mode grid and arbitrage scenarios execute end to end without network or
  credentials.
- No secret value appears in logs or serialized diagnostics.
- Live trading cannot be enabled accidentally.

## Known migration risks

- Several exchanges depend on Python-only or vendor-specific signing SDKs.
  Protocol adapters must be verified against official test environments before
  being considered production-compatible.
- The legacy test suite does not provide reliable regression coverage, so
  preserved behavior is limited to explicit configuration and strategy
  contracts captured by new tests.
- Financial arithmetic must remain decimal; binary floating point is permitted
  only for display-only percentages where the legacy contract already uses it.
- YAML numbers pass through `serde_yaml`; values requiring more precision than
  its numeric representation must be quoted so their decimal text is preserved.
