## What this changes

<!-- One paragraph. What capability or constraint does this establish? -->

## Safety boundaries

<!--
State the effect on what the software is permitted to do. If none, say
"No change to trading authority." If this widens authority, say exactly which
venue, product, and operation, and which gate holds it closed.
-->

## Verified

<!-- What you actually ran and observed. Not what you expect to work. -->

## Not covered

<!-- Known gaps, untested paths, and risks a reviewer should weigh. -->

## Checklist

- [ ] The full local gate passes (`fmt`, `check`, `clippy -D warnings`, `test`,
      `test --doc`, `build --release`), all with `--locked`.
- [ ] New behaviour has a contract test; new error paths are asserted on typed
      variants, not on message text.
- [ ] Live and mainnet paths still fail closed.
- [ ] No credentials in code, logs, `--json` output, journals, fixtures, or
      test snapshots.
- [ ] If the adapter surface moved: the capability manifest and
      `docs/adapter-support.md` were updated together, and the contract test
      that keeps them in sync passes.
- [ ] If a CLI command, flag, or its capability column changed: the tables in
      `README.md` and `rust/README.md` were updated in the same commit.
- [ ] If a dependency was added or bumped: the reason and the rejected
      alternatives are recorded above, and `Cargo.lock` is committed.
- [ ] `CHANGELOG.md` has an entry under `Unreleased`, including its Authority
      line.
