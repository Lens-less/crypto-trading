# Production-candidate runbook

This runbook packages the local operator control plane without enabling
mainnet trading. The checked-in capability manifest remains the authority:
`live_trading_enabled` must stay `false`.

## Host contract

- Linux host with Docker Engine and Compose.
- Host networking is intentional: the process still binds only
  `127.0.0.1:8787`, so it is not reachable from another host without an
  explicitly reviewed reverse proxy.
- The container root filesystem is read-only. `/var/lib/crypto-trading` is the
  only persistent writable mount. `/etc/crypto-trading` is mounted read-only.
- Secrets are process environment variables. Do not put tokens or exchange
  credentials in the image, Compose file, journal, or checked-in `.env` files.
- The journal UUID identifies one durable generation. Rotate the UUID whenever
  the journal is replaced or compacted.

## Build and start

Create the journal before startup, generate a random bearer token with at least
32 bytes, and export deployment values in the invoking shell:

```sh
install -d -m 0700 /srv/crypto-trading/data
touch /srv/crypto-trading/data/operations.jsonl
chmod 0600 /srv/crypto-trading/data/operations.jsonl

export CRYPTO_TRADING_DATA_DIR=/srv/crypto-trading/data
export CRYPTO_TRADING_JOURNAL_ID="$(uuidgen)"
export CRYPTO_TRADING_WEB_TOKEN="$(openssl rand -hex 32)"

docker compose -f deploy/compose.yaml build --pull
docker compose -f deploy/compose.yaml up -d
curl --fail \
  -H "Authorization: Bearer $CRYPTO_TRADING_WEB_TOKEN" \
  http://127.0.0.1:8787/api/v1/system
```

The startup probe must report `live_trading_enabled: false`, the expected
`journal_id`, and a non-degraded projection before promotion.

## Operations

```sh
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml logs --tail=200 operator
docker compose -f deploy/compose.yaml restart operator
docker compose -f deploy/compose.yaml down
```

Treat projection conflicts, `recovery_required`, a journal-integrity error, or a
capability-manifest validation error as release blockers. Do not work around a
failed startup by replacing the journal UUID or deleting journal records.

## Backup and restore release gate

The journal is append-only, but copying an actively changing tail is not a
transactional snapshot. Quiesce the writer or take a filesystem snapshot before
running the drill. The script independently:

1. refuses to overwrite an existing backup;
2. rejects a source whose size changes during the copy;
3. records and verifies a SHA-256 byte manifest;
4. restores into a newly created drill directory;
5. starts the bounded read model against the restored copy, which replays and
   validates the journal's sequence and FNV boundary anchors;
6. saves the projected `/api/v1/system` result as evidence.

Build the verifier first, then run:

```sh
cargo build \
  --manifest-path rust/Cargo.toml \
  --release \
  --locked \
  --package crypto-trading-web-app \
  --bin crypto-trading-web

deploy/journal-backup-restore-drill.sh \
  /srv/crypto-trading/data/operations.jsonl \
  /srv/crypto-trading/backups \
  /srv/crypto-trading/restore-drills \
  "$CRYPTO_TRADING_JOURNAL_ID" \
  rust/target/release/crypto-trading-web
```

Archive the emitted backup path, checksum manifest, restore directory,
`system.json`, and verifier log with the release evidence. The restored copy is
never moved over the live journal.

## Promotion and rollback

Promotion requires all repository quality gates, the backup/restore drill, and
the planned testnet lifecycle/soak evidence. A local deterministic harness is
not a substitute for credentialed Binance Testnet evidence or the 24-hour soak.

To roll back, keep the data volume and journal UUID unchanged, deploy the prior
image digest, and repeat the system projection check. Never roll back by
truncating or editing the append-only journal.
