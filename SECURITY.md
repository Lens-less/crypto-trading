# Security Policy

This project executes trading logic and, on two gated paths, submits real
orders using real API credentials: an acknowledged Binance Testnet order
lifecycle, and one operator-acknowledged Binance Spot MAINNET order lifecycle.
Security reports are treated as the highest-priority class of issue.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report privately through GitHub's private vulnerability reporting on this
repository (Security → Report a vulnerability); that is the primary channel.
There is no dedicated security email address. If private reporting is
unavailable to you, contact the maintainer through the repository owner's
GitHub profile, or open a public issue containing only the words "security
report, requesting a private channel" and no technical detail, and a
maintainer will open one.

Please include: the affected version or commit, the component, what an attacker
can achieve, and a reproduction. Redact every API key, secret, account
identifier, and journal record before sending. A report containing live
credentials will be deleted and you will be asked to rotate them and resend.

We aim to acknowledge within 5 working days and to agree a disclosure timeline
with you. The default coordinated-disclosure window is 90 days. Credit is given
in the changelog unless you ask otherwise.

## Supported versions

The project is pre-1.0. Only `main` is supported; there are no backports to
earlier tags. A fix ships as a new release.

## Threat model

The software is designed to run as one operator's local process:

- Credentials are read **only** from the process environment and are
  separated by authority: `BINANCE_API_KEY`/`BINANCE_API_SECRET` are
  Testnet-only, `BINANCE_MAINNET_READ_API_KEY`/`BINANCE_MAINNET_READ_API_SECRET`
  grant read-only mainnet access (`live-reconcile`), and
  `BINANCE_MAINNET_TRADE_API_KEY`/`BINANCE_MAINNET_TRADE_API_SECRET` are the
  only credentials the mainnet order path accepts. Credentials are never read
  from `.env` files, never accepted as command-line arguments, and never
  written to the journal.
- The Web control plane binds `127.0.0.1` only and authenticates with a bearer
  token supplied through an environment variable named by `--bearer-token-env`.
  It does not accept mainnet credentials.
- Mainnet trade authority exists in exactly one command: `live-lifecycle`,
  which runs one supervised Spot LIMIT order lifecycle
  (submit → query → cancel) and is gated by the exact acknowledgement phrase
  `I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE`, the dedicated mainnet
  trade environment credentials, and a required `--max-notional` cap enforced
  before any journal write or network call. Autonomous strategy live execution
  (`runtime.live`) remains disabled in the capability manifest; no supported
  configuration turns it on.

## In scope

- Credential disclosure through any output: logs, `--json` payloads, error
  messages, `Debug` output, the journal, HTTP responses, or test snapshots.
- Any path that constructs autonomous strategy live execution authority, or
  mainnet trading authority outside the single `live-lifecycle` command.
- Bypassing the exact acknowledgement phrases that gate order authority
  (`--acknowledge-risk`, `--acknowledge-testnet-lifecycle`,
  `--acknowledge-live-lifecycle`, `--apply-reconciliation`).
- Defeating the `live-lifecycle` safeguards: evading the `--max-notional` cap,
  submitting more than one order per acknowledged run, bypassing the
  journal-first PLANNED record or the query-first recovery rule, or clearing
  the journal kill-switch latch without operator action.
- Breaking credential separation: any way for Testnet credentials to reach a
  mainnet endpoint, or for the read-only mainnet credential family to obtain
  trade authority.
- Defeating journal integrity: forging sequence numbers or FNV boundary
  anchors, or evading the cross-process writer lease.
- Escaping the bounded-resource guards (configuration file size, response body
  size, journal size, batch and queue limits) to exhaust memory or disk.
- Reaching the Web control plane from another host, or from a browser page via
  DNS rebinding or a missing origin check.
- Authentication or rate-limit bypass on any Web route.

## Out of scope

- The removed Python predecessor project. Its tree was deleted from the
  working copy in 2026-08; git history is the archive (see
  [`archive/README.md`](archive/README.md)). Nothing builds, imports, or runs
  from it.
- Findings that require an attacker to already have local code execution as the
  operator's user account.
- Losses arising from trading decisions, strategy behaviour, or market
  conditions. This software carries no warranty and is not investment advice.
- Deliberately documented limitations, including the absence of journal
  rotation and the fact that paper results only account for configured fees
  and exclude funding, slippage, and queue priority. See the warning in
  [`README.md`](README.md).
