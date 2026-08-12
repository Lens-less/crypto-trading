# Open-Source and Live-Readiness Acceptance — 2026-08-13

## Outcome

The repository-local public-release gate is **accepted**. The active tree no
longer publishes workstation paths or market-manipulation language, it includes
a safe environment template and full-history secret scanning, and its local
code, browser, dependency, and workflow gates pass.

Real-money trading is **not accepted**. Mainnet authority remains unavailable,
the risk layer still lacks authoritative exchange balances/open orders/Spot
inventory, no strategy has passed an untouched holdout, and no credentialed
24-hour Testnet evidence was created during this acceptance. Those are product
and operator gates, not omissions to bypass in order to make CI green.

## Review of the supplied assessment

| Assessment | Disposition | Evidence or action |
| --- | --- | --- |
| Public documentation exposed local Windows user-profile paths | Accurate and fixed | Active documentation now uses repository-relative links or `<repo-root>` / `<temporary-cache-dir>` placeholders. A repository hygiene gate rejects recurrence outside the immutable Python snapshot. |
| Public language described volume simulation with manipulative wording | Accurate and fixed | Active README, Rust README, UI label, and compatibility configs now describe offline Paper volume simulation. Both README languages prohibit wash trading, artificial volume, market manipulation, and exchange-ToS violations. |
| Dedicated secret scanning and a root `.env.example` were missing | Accurate and fixed | `.env.example`, `.github/workflows/secret-scan.yml`, and `.gitleaksignore` were added. The scanner covers full history, while a separate job checks paths and active product language. |
| The Python archive and `docs/internal/` needed classification | Partly stale | Both already declared snapshot/evidence status. Their notices were strengthened to make old commands, prompts, and credentials-shaped placeholders non-operational. The byte-frozen Python tree itself was not rewritten. |
| REST execution deduplication still trusted Binance field `I` | Partly stale, boundary defect fixed | Runtime normalization already replaced `I` with `t`, but the exchange parser still exposed the wrong field. Parsing now owns `t`, treats Binance's `-1` non-trade sentinel as no trade identity, and the redundant runtime JSON reparse was deleted. |
| The Testnet kill switch was scoped to one owner/campaign | Accurate and fixed | The single-writer account journal is now the durable kill-switch scope. A new owner and campaign cannot regain network or submit authority after a latched or cleanup-pending kill fact. |
| The earlier nine P1 stream/soak/research defects remained blockers | Stale at the reviewed tip | Commit `e025c0d` had already repaired those defects and rerun the local acceptance gates. The dated independent report is retained as historical evidence and now links here. |
| No mainnet adapter, exchange-truth risk authority, or promotable strategy exists | Accurate and intentionally unresolved | Capability authority remains fail-closed. Existing daily holdouts failed and the hourly protocol stopped at data admission. No mainnet or autonomous strategy authority was added. |

## Integrated changes

### Correctness and safety

- Binance execution reports use trade ID `t` at the protocol parser; ignore
  field `I` cannot become an execution fingerprint. Non-trade sentinel `-1`
  remains a valid event with no trade identity.
- Continuous Testnet kill facts project across owner and campaign identifiers
  within the same account journal. Pending cleanup is recovered before any new
  lifecycle may run, and a clean latch rejects before remote I/O.
- The Hyperliquid batch-fan-out contract now proves its actual invariant with
  one accepted stub request and three ordered observations. A scheduler-sensitive
  wall-clock assertion and its obsolete delayed-server helper were removed.

### Public repository and supply chain

- Active documentation and research references use portable paths.
- The legacy volume-maker command remains parse-compatible but is described
  only as offline Paper simulation; the immutable Python snapshot is clearly
  historical and non-operational.
- Gitleaks scanned every locally available Git ref. Ten generic-key matches
  were inspected: two
  were non-authentication work-claim identifiers and eight were deterministic
  loopback test tokens. Suppressions are exact fingerprints containing the
  introducing commit, path, rule, and line; there is no broad path or regex
  allowlist.
- Dependabot payloads were integrated into the current workflows and lockfile:
  `actions/checkout` 7.0.1, `actions/setup-node` 7.0.0,
  `actions/upload-artifact` 7.0.1, the reviewed `dtolnay/rust-toolchain`
  revision, and the reviewed Cargo minor/patch updates through `async-trait`
  0.1.92, `clap` 4.6.6, and `thiserror` 2.0.20. No new direct dependency was
  added.

## Repository-local verification

| Gate | Result |
| --- | --- |
| Rust format, workspace/all-target/all-feature check, and Clippy with `-D warnings` on 1.89.0 | PASS |
| Rust workspace/all-target/all-feature tests, doc tests, warning-free docs, and release build | PASS — every discovered test binary green |
| Focused Binance parser/user-data/Testnet-owner contracts | PASS — 5 + 12 + 10 tests |
| Frontend frozen install, typecheck, lint, unit tests, and production bundle | PASS — 22 files / 239 tests |
| Browser contract against the embedded Rust binary | PASS — 6 Playwright tests |
| RustSec and Cargo license/ban/source policy | PASS — 243 locked dependencies scanned; deny policies OK |
| Frontend production advisory, registry, and license policy | PASS — no known high-severity vulnerability; 269 packages allowed |
| Full-history Gitleaks | PASS — all locally available refs; no unreviewed finding |
| GitHub Actions static validation | PASS — all workflow files accepted by Actionlint 1.7.12 |
| Public repository hygiene and diff whitespace | PASS |

Docker, Compose, Linux signal behavior, and ShellCheck are exercised by the
checked-in GitHub workflow rather than claimed from this Windows host. The
post-push workflow run is the integration authority for those platform gates.

## Gates that remain closed

| Gate | State | Required evidence |
| --- | --- | --- |
| Credentialed Binance Testnet lifecycle and private stream | CLOSED | Operator-supplied Testnet-only credentials; submit/query/cancel/reconcile evidence with redacted logs. |
| 24-hour Testnet soak and kill/restart | CLOSED | A complete schema-v2 evidence bundle, private-stream density/gap proof, unclean restart, clean stop, and externally anchored digest. |
| Backup/restore and alert delivery | CLOSED | Controlled-host drill with retained journal, restored projection, NTP evidence, and delivered alerts. |
| Mainnet read shadow | CLOSED | A separately reviewed read-only implementation and 24-hour account-truth comparison. Current binaries accept no mainnet credentials. |
| Mainnet canary | CLOSED | All preceding gates plus explicit independent human approval for one minimum-size limit order. Ambiguity must remain query-only. |
| Automated strategy promotion | CLOSED | A preregistered data protocol and untouched holdout that passes the repository's promotion criteria. Current edge is zero. |

The accepted operational posture is therefore: public repository and offline
engineering evidence **yes**; Paper and explicitly acknowledged Testnet code
paths **locally verified**; credentialed Testnet release evidence **not yet**;
mainnet and autonomous real-money trading **no**.
