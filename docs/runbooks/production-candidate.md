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

## Binance Testnet 24-hour soak gate

This gate runs the CLI host directly on the Linux candidate host. It is
read-only: the three rotating samples are Spot `bookTicker`, USD-M
`bookTicker`, and authenticated reconciliation. The host never submits or
cancels an order. Mainnet remains disabled.

Build the exact candidate binary, create a private evidence directory, and
provide Binance Testnet credentials only through the process environment:

```sh
cargo build \
  --manifest-path rust/Cargo.toml \
  --release \
  --locked \
  --package crypto-trading-apps \
  --bin crypto-trading

umask 077
install -d -m 0700 /srv/crypto-trading/soak

export BINANCE_API_KEY='...'
export BINANCE_API_SECRET='...'
export SOAK_BIN="$PWD/rust/target/release/crypto-trading"
export SOAK_TASK_ID='binance-testnet-24h'
export SOAK_HISTORY='/srv/crypto-trading/soak/binance-testnet-24h.jsonl'
export SOAK_CONTROL_PORT='55124'
export SOAK_PID_FILE='/srv/crypto-trading/soak/binance-testnet-24h.pid'

start_soak() {
  suffix="$1"
  nohup "$SOAK_BIN" testnet-soak \
    --mode serve \
    --task-id "$SOAK_TASK_ID" \
    --history-path "$SOAK_HISTORY" \
    --interval-ms 300000 \
    --probe-timeout-ms 15000 \
    --failure-threshold 3 \
    --control-port "$SOAK_CONTROL_PORT" \
    --timeout-ms 10000 \
    >"/srv/crypto-trading/soak/${suffix}.stdout.log" \
    2>"/srv/crypto-trading/soak/${suffix}.stderr.log" &
  SOAK_PID=$!
  printf '%s\n' "$SOAK_PID" >"$SOAK_PID_FILE"
  kill -0 "$SOAK_PID"
}

capture_status() {
  destination="$1"
  attempts=0
  until "$SOAK_BIN" testnet-soak \
    --mode status \
    --task-id "$SOAK_TASK_ID" \
    --history-path "$SOAK_HISTORY" \
    --control-port "$SOAK_CONTROL_PORT" \
    >"$destination"
  do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      return 1
    fi
    sleep 1
  done
  cat "$destination"
}

start_soak initial
capture_status /srv/crypto-trading/soak/status-before-kill.txt
```

Wait until status reports at least three successful probes. Perform exactly one
forced-termination recovery drill, validate the numeric PID before signalling
it, and restart with the same task ID, journal, and port:

```sh
SOAK_PID="$(cat "$SOAK_PID_FILE")"
case "$SOAK_PID" in
  ''|*[!0-9]*) echo "invalid soak PID" >&2; exit 1 ;;
esac
kill -0 "$SOAK_PID"
kill -9 "$SOAK_PID"
wait "$SOAK_PID" 2>/dev/null || true
if kill -0 "$SOAK_PID" 2>/dev/null; then
  echo "soak process survived kill -9" >&2
  exit 1
fi

start_soak restarted
capture_status /srv/crypto-trading/soak/status-after-restart.txt
```

The verifier counts only active segments that contain probe facts, so downtime
between kill and restart cannot inflate the 24-hour claim. After accumulated
active probe time exceeds 24 hours, stop through the control host and verify the
conservative production policy:

```sh
"$SOAK_BIN" testnet-soak \
  --mode stop \
  --task-id "$SOAK_TASK_ID" \
  --history-path "$SOAK_HISTORY" \
  --control-port "$SOAK_CONTROL_PORT" \
  | tee /srv/crypto-trading/soak/status-after-stop.txt

SOAK_PID="$(cat "$SOAK_PID_FILE")"
wait "$SOAK_PID"

if ! "$SOAK_BIN" testnet-soak \
  --mode verify \
  --task-id "$SOAK_TASK_ID" \
  --history-path "$SOAK_HISTORY" \
  --minimum-successes 288 \
  > /srv/crypto-trading/soak/evidence.json \
  2> /srv/crypto-trading/soak/verify.stderr.log
then
  cat /srv/crypto-trading/soak/evidence.json
  cat /srv/crypto-trading/soak/verify.stderr.log >&2
  exit 1
fi
cat /srv/crypto-trading/soak/evidence.json
```

The JSON must report `requirements_met: true`, at least 86,400 observed active
seconds, a clean stop, one or more unclean restarts, the configured minimum
success count, and nonzero counts for all three sample kinds. Archive the
journal, both process logs, the three status captures, `evidence.json`, and the
candidate binary checksum. Do not archive credentials or process-environment
dumps.

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
