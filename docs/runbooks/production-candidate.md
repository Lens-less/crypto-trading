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
curl --fail http://127.0.0.1:8787/api/v1/health
curl --fail \
  -H "Authorization: Bearer $CRYPTO_TRADING_WEB_TOKEN" \
  http://127.0.0.1:8787/api/v1/system
```

The unauthenticated readiness probe is deliberately data-free and must report
healthy. The authenticated startup probe must report
`live_trading_enabled: false`, the expected `journal_id`, and a non-degraded
projection before promotion.

## Operations

```sh
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml logs --tail=200 operator
docker compose -f deploy/compose.yaml restart operator
docker compose -f deploy/compose.yaml down
```

Compose probes `/api/v1/health` every 30 seconds and rotates container JSON logs
at 10 MiB with five retained files. The authenticated `/api/v1/risk` and
`/api/v1/settings` projections are the operator source for Paper account
reservations, effective paths, configured credential state, and the configured
240-request/60-second per-bucket Web threshold. The unauthenticated readiness
probe, authenticated read routes, and the trusted Paper submit route (when
enabled) consume independent buckets with that same threshold. They never
return credential values. Treat HTTP `429` plus `Retry-After` as backpressure;
do not bypass it with additional clients.

Treat projection conflicts, `recovery_required`, a journal-integrity error, or a
capability-manifest validation error as release blockers. Do not work around a
failed startup by replacing the journal UUID or deleting journal records.

## Binance Testnet order-lifecycle gate

Run this gate before the soak. It is the only candidate path with order
authority, and that authority is limited to Binance Testnet. Each campaign
persists its UUID client order ID before submission, uses signed single-order
queries as the recovery authority, cancels the order, and records the final
cancelled state. There is no `--live` option.

A fresh campaign first fetches the product-specific public `exchangeInfo` and
requires an exact wire/base/quote/status/product identity plus only locally
supported filter semantics. Missing, conflicting, or unknown metadata stops
before the durable submit plan. Applied MARKET notional rules also stop locally
until the adapter has an authoritative venue reference-price implementation;
bookTicker is not substituted. After `planned` is durable, a restart does not
refetch current trading metadata: it constructs query/cancel-only authority so
an exchangeInfo outage or later trading halt cannot block recovery of the
persisted client UUID. The planned fact also binds the exact Binance wire
symbol; a recovery invocation uses that durable mapping even if its current CLI
mapping differs, preventing a client UUID from being redirected to another
instrument. A durable HTTP rate-limit deadline is enforced before recovery
network I/O, so an immediate restart cannot bypass `Retry-After`.

Build the exact candidate and create a private, dedicated evidence file:

```sh
cargo build \
  --manifest-path rust/Cargo.toml \
  --release \
  --locked \
  --package crypto-trading-apps \
  --bin crypto-trading

umask 077
install -d -m 0700 /srv/crypto-trading/lifecycle

export BINANCE_API_KEY='...'
export BINANCE_API_SECRET='...'
export LIFECYCLE_BIN="$PWD/rust/target/release/crypto-trading"
export LIFECYCLE_HISTORY='/srv/crypto-trading/lifecycle/binance-testnet.jsonl'
export LIFECYCLE_CAMPAIGN='binance-spot-open-001'
export LIFECYCLE_CLIENT_ID="$(uuidgen)"
export LIFECYCLE_PRICE='<POST_ONLY_PRICE>'

"$LIFECYCLE_BIN" testnet-lifecycle \
  --acknowledge-testnet-lifecycle \
  'I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE' \
  --campaign-id "$LIFECYCLE_CAMPAIGN" \
  --client-order-id "$LIFECYCLE_CLIENT_ID" \
  --history-path "$LIFECYCLE_HISTORY" \
  --market spot \
  --side buy \
  --quantity 0.001 \
  --price "$LIFECYCLE_PRICE" \
  --time-in-force post-only \
  --expected-observation open \
  --poll-interval-ms 2000 \
  --maximum-queries 30 \
  --timeout-ms 10000 \
  --json \
  | tee /srv/crypto-trading/lifecycle/open-order-result.json
```

Replace the price placeholder with a non-marketable price that satisfies the
current Testnet instrument filters. Never paste the placeholder into a real
invocation. The result must say `authority: "testnet"`,
`mainnet_enabled: false`, and `final_status: "cancelled"`. The journal must
contain, in order, `testnet_lifecycle_planned`, a submit observation, at least
one signed query observation that proves the expected state, cancel planned, a
final cancelled query observation, and `testnet_lifecycle_completed`. A normal
cancel response also emits `testnet_lifecycle_cancel_observed`. If cancel
dispatch is ambiguous, `testnet_lifecycle_outcome_unknown` followed by the
authoritative final cancelled query is valid; the cancel response fact is then
intentionally absent.

Create a second campaign and client UUID for the controlled partial-fill case.
Use `--time-in-force gtc --expected-observation partially-filled` and arrange a
small fill from an independently controlled Testnet account or test fixture
while the lifecycle is polling. Use a quantity and price that satisfy the
current filters and leave a cancellable remainder. If the partial fill is not
observed within the bounded query budget, the owner cancels any known open
order, records failure, and exits nonzero; that is not passing evidence.
Cleanup intent records whether the expected observation was already proven, so
a restart during failed cleanup cannot turn that campaign into a passing one.

For the restart drill, use a third campaign with the partial-fill expectation
and a long enough polling interval to intervene. Start it in the foreground,
wait until the journal contains `testnet_lifecycle_submit_observed`, then send
`SIGKILL`. Rerun the *identical* command with the same campaign ID, client UUID,
intent, and history path. The resumed invocation must append
`testnet_lifecycle_resumed`, query by the persisted UUID before any mutation,
avoid a second submit, cancel the recovered order, and finish cancelled:

```sh
jq -r 'select(.details.campaign_id == env.LIFECYCLE_CAMPAIGN) | .decision' \
  "$LIFECYCLE_HISTORY"
```

Count exactly one `testnet_lifecycle_planned` and one
`testnet_lifecycle_submit_observed` for the recovered campaign. A process killed
after the plan record but before a submit receipt is intentionally fail-closed:
the next run queries first and may report `outcome_unknown`; it never guesses
that resubmission is safe. Use a new campaign only after an authoritative query
or operator reconciliation proves the old UUID has no order.

The polling interval and maximum query count are part of the durable campaign
identity. Query attempts are planned in the journal before network I/O and the
budget is cumulative across restarts; changing either policy for the same
campaign fails closed. If the campaign exhausts its budget, stop rerunning it,
reconcile the persisted client UUID manually, and retain the unresolved
campaign as failed release evidence.

Archive the candidate checksum, redacted command arguments, CLI JSON outputs,
and journal. Exercise and record Spot open-order, controlled partial-fill, and
kill/restart recovery. A timeout or ambiguous submit/cancel must be followed by
the signed query-first path. A timestamp-skew response gets one clock-sync
retry. Treat venue rate limiting as a failed gate and retry later according to
the venue response; do not increase the bounded query budget to overwhelm it.
Never archive credentials or an environment dump.

## Binance Testnet account-reconciliation gate

Run this gate after the order-lifecycle campaign has cancelled its test order
and before the 24-hour soak. It is the first real consumer of the Paper account
reconciliation transition: one stable double-sampled signed Binance Testnet
product snapshot is compared with one exact committed Paper reservation. The command is
report-only unless the exact apply acknowledgement is present, and it never
enables mainnet.

Stop the Paper owner before this gate. `testnet-reconcile` holds the journal's
cross-process writer lease while it freezes local state, samples the venue, and
optionally appends one transition. Record the exact journal generation,
starting capacity, account, reservation, product, symbol mapping, and
settlement asset:

```bash
export BINANCE_API_KEY='<TESTNET_ONLY_KEY>'
export BINANCE_API_SECRET='<TESTNET_ONLY_SECRET>'
export RECONCILE_BIN="$PWD/rust/target/release/crypto-trading"
export PAPER_HISTORY='/srv/crypto-trading/journal/paper-grid.jsonl'
export PAPER_JOURNAL_ID='<JOURNAL_UUID>'
export PAPER_ACCOUNT_ID='paper-main'
export PAPER_INITIAL_AVAILABLE='<EXACT_STARTING_CAPACITY>'
export PAPER_RESERVATION_ID='<COMMITTED_RESERVATION_UUID>'

"$RECONCILE_BIN" testnet-reconcile \
  --history-path "$PAPER_HISTORY" \
  --journal-id "$PAPER_JOURNAL_ID" \
  --account-id "$PAPER_ACCOUNT_ID" \
  --initial-available "$PAPER_INITIAL_AVAILABLE" \
  --reservation-id "$PAPER_RESERVATION_ID" \
  --market spot \
  --settlement-asset USDT \
  --spot-symbol BTC-USDT-SPOT \
  --perpetual-symbol BTC-USDT-PERP \
  --wire-symbol BTCUSDT \
  --timeout-ms 15000 \
  --json > /srv/crypto-trading/evidence/testnet-reconcile-report.json
```

A passing report has `matches: true`, zero owned/foreign orders, zero
positions, no mismatch codes, and a 16-character FNV-1a proof digest. The
adapter takes two complete consecutive balance/order/position samples and
rejects observed state drift. The selected Testnet settlement balance must
equal the Paper account's projected availability after releasing this
reservation; wallet and available balances must converge, Spot locked balance
must be zero, and every non-settlement asset balance must be zero. This
deliberately strict clean-account gate does not perform multi-asset valuation
or infer ownership for unrelated venue activity.

Re-sample and apply the result only with the exact acknowledgement:

```bash
"$RECONCILE_BIN" testnet-reconcile \
  --history-path "$PAPER_HISTORY" \
  --journal-id "$PAPER_JOURNAL_ID" \
  --account-id "$PAPER_ACCOUNT_ID" \
  --initial-available "$PAPER_INITIAL_AVAILABLE" \
  --reservation-id "$PAPER_RESERVATION_ID" \
  --market spot \
  --settlement-asset USDT \
  --spot-symbol BTC-USDT-SPOT \
  --perpetual-symbol BTC-USDT-PERP \
  --wire-symbol BTCUSDT \
  --timeout-ms 15000 \
  --apply-reconciliation \
  'I APPLY VERIFIED BINANCE TESTNET RECONCILIATION' \
  --json > /srv/crypto-trading/evidence/testnet-reconcile-applied.json
```

For USD-M, use `--market usdm` and make the committed reservation's only lane
match the configured perpetual symbol. Run and archive both product variants
when both products are in the release scope. A matching applied result records
`released`; a mismatching applied result durably records `failure_recorded`,
keeps committed exposure held, prints the proof, and exits non-zero. Missing
assets, non-zero untracked assets, any open or foreign order, any non-flat
position, unknown wire symbols, rate limiting, partial or unstable HTTP
samples, projection degradation, or balance differences are release blockers.
An applied reconciliation outcome is terminal for that reservation: replaying
the identical proof is idempotent, while a different digest, a newer snapshot,
or an opposite outcome is rejected. Investigate and retain the original
evidence; never try to reverse a recorded failure with a later release proof.
Never edit the journal to force a match, and never archive credentials.

## Binance Testnet 24-hour soak gate

This gate runs the CLI host directly on the Linux candidate host. The default
mode is owner-backed but read-only: the three rotating samples are the Spot
Testnet `bookTicker` WebSocket, the signed Spot Testnet user-data WebSocket API,
and two matching authenticated REST reconciliations performed by that same
`ContinuousTestnetOwner`. Read-only mode does not claim lifecycle-recovery
evidence. The public stream uses
`wss://stream.testnet.binance.vision`; the private stream uses
`wss://ws-api.testnet.binance.vision`. Mainnet remains disabled.

An AC-R3 campaign uses the optional exact lifecycle group. On a fresh journal,
all fields and the existing acknowledgement are mandatory; the owner waits for
a fresh private-stream subscription ACK before its only submit. On restart,
the same exact fields reconstruct the durable intent, but no acknowledgement is
required: only a pending plan is accepted and the first venue operation is an
exact-client-ID query. Completed, failed, partial, or conflicting configurations
fail closed. Omitting the whole group keeps the host read-only.

Use a new evidence journal for this owner-backed v2 run. Legacy v1
`binance_testnet_read_only_soak` records are intentionally not admitted to the
AC-R3 verifier because they cannot prove owner-driven lifecycle recovery.

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
export SOAK_CAMPAIGN_ID='binance-testnet-24h-lifecycle-001'
export SOAK_CLIENT_ORDER_ID='replace-with-a-new-uuid-v4'
# Derive these immediately before the run from Testnet bookTicker and current
# exchangeInfo filters. Price must be post-only, deliberately away from the
# opposite best quote, tick-aligned, and satisfy minNotional with quantity.
export SOAK_PRICE='replace-with-current-safe-testnet-price'
export SOAK_QUANTITY='replace-with-filter-valid-testnet-quantity'
: "${SOAK_PRICE:?set a current filter-valid Testnet post-only price}"
: "${SOAK_QUANTITY:?set a current filter-valid Testnet quantity}"

start_soak() {
  suffix="$1"
  acknowledgement="${2-}"
  ack_args=()
  if [ -n "$acknowledgement" ]; then
    ack_args=(--acknowledge-testnet-lifecycle "$acknowledgement")
  fi
  nohup "$SOAK_BIN" testnet-soak \
    --mode serve \
    --task-id "$SOAK_TASK_ID" \
    --history-path "$SOAK_HISTORY" \
    --interval-ms 300000 \
    --probe-timeout-ms 15000 \
    --failure-threshold 3 \
    --control-port "$SOAK_CONTROL_PORT" \
    --timeout-ms 10000 \
    "${ack_args[@]}" \
    --recovery-campaign-id "$SOAK_CAMPAIGN_ID" \
    --recovery-client-order-id "$SOAK_CLIENT_ORDER_ID" \
    --recovery-market spot \
    --recovery-side buy \
    --recovery-quantity "$SOAK_QUANTITY" \
    --recovery-price "$SOAK_PRICE" \
    --recovery-time-in-force post-only \
    --recovery-expected-observation open \
    --recovery-reduce-only false \
    --recovery-poll-interval-ms 2000 \
    --recovery-maximum-queries 30 \
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

start_soak initial 'I AUTHORIZE BINANCE TESTNET ORDER LIFECYCLE'
capture_status /srv/crypto-trading/soak/status-before-kill.txt
```

Use a dedicated isolated Testnet account. Before starting, prove it has no
unrelated open orders or positions; otherwise the owner's stable reconciliation
fails closed. Do not reuse a price from this document or from an earlier run.

The kill drill must interrupt the exact lifecycle after
`testnet_lifecycle_planned` is durable and before it becomes terminal. Use a
supervised Testnet fault/latency window and capture the journal observation; do
not edit the journal or invent a pending state. If `testnet_lifecycle_completed`
or `testnet_lifecycle_failed` wins the race, stop and begin a new campaign with
a new campaign ID and UUID. Validate the numeric PID before signalling it:

```sh
SOAK_PID="$(cat "$SOAK_PID_FILE")"
case "$SOAK_PID" in
  ''|*[!0-9]*) echo "invalid soak PID" >&2; exit 1 ;;
esac
kill -0 "$SOAK_PID"
grep -q '"decision":"testnet_lifecycle_planned"' "$SOAK_HISTORY"
if grep -qE '"decision":"testnet_lifecycle_(completed|failed)"' "$SOAK_HISTORY"; then
  echo "lifecycle became terminal before the kill drill" >&2
  exit 1
fi
kill -9 "$SOAK_PID"
wait "$SOAK_PID" 2>/dev/null || true
if kill -0 "$SOAK_PID" 2>/dev/null; then
  echo "soak process survived kill -9" >&2
  exit 1
fi

# Same exact lifecycle fields, deliberately without submit acknowledgement.
start_soak restarted
capture_status /srv/crypto-trading/soak/status-after-restart.txt
grep -q '"decision":"continuous_testnet_campaign_recovery_verified"' "$SOAK_HISTORY"
grep -q '"query_first":true' "$SOAK_HISTORY"
grep -Eq '"query_delta":[1-9][0-9]*' "$SOAK_HISTORY"
grep -q '"decision":"continuous_testnet_user_stream_subscribed"' "$SOAK_HISTORY"
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
success count, and nonzero `market_stream`, `user_data_stream`, and
`authenticated_reconcile` counts. It must also report
`owner_campaign_recovery_verified: true`; this is accepted only when a
same-task, UUID-valid, positive exact-query delta is immediately paired with
the unclean restart. Fixed-timestamp offline tests prove this verifier contract,
not a credentialed 24-hour run. Archive the
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

Build the verifier first, then run it. The drill consumes only the JSON API,
so building without `frontend/dist/` is acceptable here (the binary then serves
a placeholder shell instead of the operator UI); the deployable container image
always builds and embeds the frontend bundle inside the Dockerfile. To verify
with the embedded UI, run `pnpm install --frozen-lockfile && pnpm build` in
`frontend/` before the cargo build:

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
never moved over the live journal. The drill starts its short-lived verifier
with the explicit `--allow-open-loopback-read-api` escape hatch; normal operator
control-plane deployments remain bearer-protected by default.

## Promotion and rollback

Promotion requires all repository quality gates, healthy and non-degraded Web
projections, the backup/restore drill, both credentialed Testnet reconciliation
products, the three credentialed Testnet lifecycle cases above, and the 24-hour
soak evidence. A local deterministic harness is not a substitute for
credentialed Binance Testnet evidence or the 24-hour soak.

To roll back, keep the data volume and journal UUID unchanged, deploy the prior
image digest, and repeat the system projection check. Never roll back by
truncating or editing the append-only journal.
