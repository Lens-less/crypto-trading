# Security Policy

This project executes trading logic and, on one gated path, submits real
orders to Binance Testnet using real API credentials. Security reports are
treated as the highest-priority class of issue.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report privately through GitHub's private vulnerability reporting on this
repository (Security → Report a vulnerability). If that is unavailable to you,
open a public issue containing only the words "security report, requesting a
private channel" and no technical detail, and a maintainer will open one.

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

- Credentials are read **only** from the process environment
  (`BINANCE_API_KEY`, `BINANCE_API_SECRET`, and the `<EXCHANGE>_<FIELD>`
  loader family). They are never read from `.env` files, never accepted as
  command-line arguments, and never written to the journal.
- The Web control plane binds `127.0.0.1` only and authenticates with a bearer
  token supplied through an environment variable named by `--bearer-token-env`.
- Mainnet trading is disabled in the capability manifest. No supported
  configuration turns it on.

## In scope

- Credential disclosure through any output: logs, `--json` payloads, error
  messages, `Debug` output, the journal, HTTP responses, or test snapshots.
- Any path that constructs live or mainnet trading authority while the
  capability manifest reports `live_trading_enabled: false`.
- Bypassing the exact acknowledgement phrases that gate order authority
  (`--acknowledge-risk`, `--acknowledge-testnet-lifecycle`,
  `--apply-reconciliation`).
- Defeating journal integrity: forging sequence numbers or FNV boundary
  anchors, or evading the cross-process writer lease.
- Escaping the bounded-resource guards (configuration file size, response body
  size, journal size, batch and queue limits) to exhaust memory or disk.
- Reaching the Web control plane from another host, or from a browser page via
  DNS rebinding or a missing origin check.
- Authentication or rate-limit bypass on any Web route.

## Out of scope

- `archive/python-legacy/`. It is a frozen, byte-verified evidence copy of a
  predecessor project. Nothing builds, imports, or runs from it.
- Findings that require an attacker to already have local code execution as the
  operator's user account.
- Losses arising from trading decisions, strategy behaviour, or market
  conditions. This software carries no warranty and is not investment advice.
- Deliberately documented limitations, including the absence of journal
  rotation and the fact that paper results exclude fees, funding, slippage, and
  queue priority. See the warning in [`README.md`](README.md).
