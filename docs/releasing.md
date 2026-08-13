# Releasing

No crate in this workspace is published to crates.io; every `Cargo.toml`
carries `publish = false`. A release is a git tag, a GitHub release, and a
container image.

The release artifact is a statement about **authority** — what that build is
permitted to do. The capability manifest is attached to every release so that
statement is machine-readable and archived, not just described in prose.

## 1. Prepare

1. Confirm `CHANGELOG.md` has an `Unreleased` section covering every
   user-visible change, including its **Authority** line. If authority did not
   change, say so explicitly.
2. Rename `Unreleased` to the new version with today's date, and open a fresh
   `Unreleased` section.
3. Bump `version` in `[workspace.package]` in `rust/Cargo.toml`. All crates
   inherit it.
4. Run `cargo update --workspace` only if a dependency bump is intended;
   otherwise leave `Cargo.lock` alone.

## 2. Gate

CI is split across four lanes, and the current matrix is not symmetric:

- frontend bundle: Ubuntu only, once;
- frontend quality gates: Ubuntu and Windows;
- Rust verify: Ubuntu `1.89.0`, Ubuntu stable, and Windows `1.89.0`;
- Rust quality, clean-build, audit, supply-chain, and deployment: Ubuntu only.

Run the matching local commands first. Do not invent a Windows-stable lane;
CI does not have one.

```bash
# Frontend bundle and browser contract
cd frontend
corepack enable
pnpm install --frozen-lockfile
pnpm build
pnpm typecheck
pnpm lint
pnpm test -- --run
pnpm exec playwright install --with-deps chromium
pnpm e2e

# Rust verify and quality gates
cd rust
cargo +1.89.0 check --workspace --all-targets --all-features --locked
cargo +1.89.0 test --workspace --all-targets --all-features --locked
cargo +stable check --workspace --all-targets --all-features --locked
cargo +stable test --workspace --all-targets --all-features --locked
cargo +stable fmt --all -- --check
cargo +stable clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +stable doc --no-deps --workspace --all-features --locked
cargo +stable build --release --workspace --all-features --locked

# Rust clean-build lane
cargo +1.89.0 fmt --all -- --check
cargo +1.89.0 check --workspace --all-targets --all-features --locked
cargo +1.89.0 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.89.0 test --workspace --all-targets --all-features --locked
cargo +1.89.0 test --doc --workspace --all-features --locked
cargo +1.89.0 build --release --workspace --all-features --locked
cargo audit --file Cargo.lock
cargo deny --manifest-path Cargo.toml check bans licenses sources
```

`rust/.cargo/audit.toml` contains one time-bounded exception for
`RUSTSEC-2026-0235`. It is acceptable only while CI proves that `rkyv` is absent
from the workspace's all-targets/all-features dependency graph. The audit job
expires that exception on 2026-11-04; enabling any `rust_decimal`/`rkyv`
integration requires removing the exception or upgrading to a patched `rkyv`
line first.

Then the deployment surface:

```bash
docker build --tag crypto-trading:release-candidate .
shellcheck deploy/*.sh
```

## 3. Environment gates

A tagged release additionally requires the gates in
[`runbooks/production-candidate.md`](runbooks/production-candidate.md), with
their evidence archived:

- [ ] Binance Testnet order lifecycle: open-order, controlled partial fill, and
      kill/restart recovery, each as a separate campaign. Archive the journal,
      process logs, command arguments, and verifier output together.
- [ ] Binance Testnet account reconciliation, for every product in scope.
- [ ] 24-hour soak, with one forced-termination recovery drill and a clean
      stop. This is an external credentialed run, not a local harness result.
      Archive the journal, process logs, status captures, evidence bundle, and
      checksum manifest together.
- [ ] Journal backup and restore drill. Treat it as a release gate, not an ad
      hoc maintenance task.
- [ ] Binance Mainnet manual lifecycle: a `live-reconcile` shadow baseline and
      one supervised `live-lifecycle` run on a dedicated minimal-notional
      account, with redacted evidence archived. This gate runs only after all
      four Testnet-side gates above have passed.

A local deterministic harness does not substitute for credentialed Testnet
evidence, backup/restore evidence, or the 24-hour soak. Autonomous strategy
live execution remains closed regardless of these gates; the only mainnet
order authority in a release is the one-shot acknowledged `live-lifecycle`
path. Archive the candidate binary checksums, redacted command arguments, CLI
JSON outputs, and journals. **Never archive credentials or an environment
dump.**

## 4. Tag and publish

```bash
git tag --sign "v$VERSION" --message "crypto-trading v$VERSION"
git push origin "v$VERSION"
```

Create the GitHub release from the tag with:

- The `CHANGELOG.md` section for this version as the body.
- `cargo run --locked -- capabilities --json` output attached as
  `capabilities-v$VERSION.json`. This is the archived, machine-readable
  statement of what the release may do.
- Release binaries for Linux and Windows (`crypto-trading` and
  `crypto-trading-web`) with a `SHA256SUMS` file.

## 5. After release

Verify the published artifacts reproduce the recorded checksums, and confirm
the deployed image reports the expected `journal_id`,
`release_stage: "live-manual"`, and `live_trading_enabled: true` through the
authenticated startup probe described in the runbook.

To roll back, keep the data volume and journal UUID unchanged, deploy the prior
image digest, and repeat the system projection check. **Never roll back by
truncating or editing the append-only journal.**
