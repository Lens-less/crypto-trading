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

The full matrix must pass on both Ubuntu and Windows, on both `1.89.0` and
stable. CI covers this; run it locally first:

```bash
cd rust
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test --doc --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace --all-features --locked
cargo build --release --workspace --all-features --locked
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

A tagged release additionally requires the four gates in
[`runbooks/production-candidate.md`](runbooks/production-candidate.md), with
their evidence archived:

- [ ] Binance Testnet order lifecycle: open-order, controlled partial fill, and
      kill/restart recovery, each as a separate campaign.
- [ ] Binance Testnet account reconciliation, for every product in scope.
- [ ] 24-hour soak, with one forced-termination recovery drill and a clean stop.
- [ ] Journal backup and restore drill.

A local deterministic harness does not substitute for credentialed Testnet
evidence. Archive the candidate binary checksums, redacted command arguments,
CLI JSON outputs, and journals. **Never archive credentials or an environment
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
the deployed image reports the expected `journal_id` and
`live_trading_enabled: false` through the authenticated startup probe described
in the runbook.

To roll back, keep the data volume and journal UUID unchanged, deploy the prior
image digest, and repeat the system projection check. **Never roll back by
truncating or editing the append-only journal.**
