# Production-candidate runbook

This runbook packages the local operator control plane and defines the
release gates. The checked-in capability manifest remains the authority: it
reports `release_stage: "live-manual"` and `live_trading_enabled: true`, and
the only mainnet order authority it grants is the operator-supervised
one-shot `live-lifecycle` command covered by the
[mainnet manual lifecycle gate](#binance-mainnet-manual-lifecycle-gate) at
the end of this runbook. The deployed Web control plane itself has no
trading authority and does not accept mainnet credentials. Autonomous
strategy live execution remains unavailable.

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

The unauthenticated `/api/v1/health` probe is deliberately data-free and
reports liveness only. The authenticated startup probe must report
`release_stage: "live-manual"`, `live_trading_enabled: true`, the expected
`journal_id`, and a non-degraded projection before promotion. Any other
release stage or a manifest validation error is a release blocker.

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

## Observability and alerting

The Web process exposes protected `GET /api/v1/metrics`. Every long-running
task host, including `testnet-soak serve`, also exposes protected
`GET /metrics` on its loopback control port. Authenticate the former with
`CRYPTO_TRADING_WEB_TOKEN` and the latter with
`CRYPTO_TRADING_TASK_CONTROL_TOKEN`; never put either token in the scrape URL.
These endpoints report process-local state, so scrape the endpoint belonging to
the actual owner rather than a separate Web process. Keep `/api/v1/health` as
liveness only; do not infer trading readiness or degradation from it. The
checked-in baseline alert rules are
[`deploy/prometheus-alerts.yml`](../../deploy/prometheus-alerts.yml); that file
is the authoritative alert-policy source.

For example, scrape the owner process itself (replace the port with the
configured Testnet soak control port):

```sh
curl --fail \
  -H "Authorization: Bearer $CRYPTO_TRADING_TASK_CONTROL_TOKEN" \
  http://127.0.0.1:49152/metrics
```

The checked-in minimum alert set is:

1. Fire immediately if `crypto_trading_process_up != 1`, if the metrics scrape
   fails, or if the process restarts without an operator-recorded clean stop.
2. Fire on stale streams or transport churn when
   `crypto_trading_stream_observed` is zero, when
   `time() - crypto_trading_stream_last_frame_timestamp_seconds{stream="market"}`
   exceeds 5 seconds, or when the corresponding `user_data` expression exceeds
   the 60-second transport-watchdog budget. The user-data threshold measures
   transport liveness (including Pong), not account-state freshness.
3. Fire immediately when
   `crypto_trading_owner_phase{phase="recovery_required"} == 1` or when
   `increase(crypto_trading_journal_append_failure_total[5m]) > 0`.
4. Fire when `abs(crypto_trading_clock_skew_milliseconds) > 1000` persists for
   one minute, and warn whenever
   `increase(crypto_trading_rest_status_total{class="429"}[5m]) > 0`.

Reconnect/gap counters, REST latency/request totals, Binance used-weight and
order-count headers, and successful journal-append totals are exported for
dashboards and capacity drill-down; the checked-in rules do not currently
alert on those series. Add site-specific thresholds in the deployment layer
and preserve this baseline unchanged so repository contract tests can detect
policy drift.

The 240-request / 60-second Web threshold protects only the local read API; it
is not a Binance venue budget. The exchange client must obey its separate
conservative request-weight/order-count budget and any exported `Retry-After`
deadline. Retain those response headers in the evidence bundle so recovery
decisions can be replayed.

## Binance Testnet order-lifecycle gate

Run this gate before the soak. It is the only candidate path with Binance
Testnet order authority; mainnet order authority exists only in the separate
[mainnet manual lifecycle gate](#binance-mainnet-manual-lifecycle-gate), which
requires every Testnet gate in this runbook to pass first. Each campaign
persists its UUID client order ID before submission, uses signed single-order
queries as the recovery authority, cancels the order, and records the final
cancelled state. There is no `--live` option on this command.

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
`wss://ws-api.testnet.binance.vision`. This gate grants no mainnet authority
and reads only Testnet credentials.

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
success count, the enforced per-kind minimum, and a maximum gap no greater than
the policy for each of `market_stream`, `user_data_stream`, and
`authenticated_reconcile`. It must also report
`owner_campaign_recovery_verified: true`; this is accepted only when a
same-task, UUID-valid, positive exact-query delta is immediately paired with
the unclean restart. `monotonic_elapsed_verified` and
`integrity_chain_verified` must both be true; archive the reported integrity
head and `source_sha256` alongside the verifier output.

The SHA-256 chain detects truncation or modification only after its head and
source digest have been anchored outside the journal. It is not an identity
signature: an operator with write access to every file could recompute it.
Before moving the bundle off the candidate host, create one manifest over the
journal, both process logs, the three status captures, `evidence.json`, and the
candidate binary, then protect that manifest with the organisation's signing
or immutable-storage control. Fixed-clock offline tests prove the verifier
contract, not a credentialed 24-hour run. The bundle must come from the
operator-controlled host with real Testnet credentials; this runbook does not
simulate or replace it. Never archive credentials or process-environment dumps.

`continuous_testnet_killed_clean` proves two identical snapshots with zero
owned/foreign open orders and zero positions, and records the owner's observed
balance projection. The generic `ReconcileReceipt` does not carry authoritative
Spot balances, so this fact explicitly records
`spot_balance_authority: unavailable_in_reconcile_receipt`; it must not be used
as a substitute for the separate account-reconciliation gate above.

## Backup and restore release gate

The journal is append-only, but copying an actively changing tail is not a
transactional snapshot. The drill takes the same OS-level writer lease used by
the runtime and fails immediately when an owner is active; quiesce the writer
or take a filesystem snapshot before running it. The script independently:

1. refuses to overwrite an existing backup;
2. refuses a symlinked writer lock, requires the writer lease, and rejects a
   source whose size or SHA-256 changes during the copy;
3. records and verifies a SHA-256 byte manifest;
4. restores into a newly created drill directory;
5. starts the bounded read model against the restored copy, which replays and
   validates the journal's sequence and FNV boundary anchors;
6. saves the projected `/api/v1/system` result as evidence.

Run this drill on every release candidate and whenever the journal generation
changes. Keep the emitted backup path, checksum manifest, restore directory,
`system.json`, and verifier log with the release evidence until a newer
successful release package supersedes them.

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

For a scheduled Linux host, install
`deploy/systemd/crypto-trading-journal-backup.service` and `.timer`, place these
non-secret values in `/etc/crypto-trading/journal-backup.env`, then enable the
timer. The scheduled run fails (and must alert through the service manager)
while the owner still holds its writer lease; arrange an explicit quiescent
window or a filesystem-snapshot job rather than weakening that check.

```text
CRYPTO_TRADING_BACKUP_JOURNAL=/srv/crypto-trading/data/operations.jsonl
CRYPTO_TRADING_BACKUP_DIR=/srv/crypto-trading/backups
CRYPTO_TRADING_RESTORE_DRILL_DIR=/srv/crypto-trading/restore-drills
CRYPTO_TRADING_JOURNAL_ID=replace-with-the-journal-uuid
CRYPTO_TRADING_WEB_BINARY=/opt/crypto-trading/bin/crypto-trading-web
```

Store daily backups and manifests on immutable or versioned storage for at
least 35 days, and retain each release-candidate evidence bundle until a newer
candidate has passed both backup and restore. Apply retention in the storage
backend; these scripts intentionally never delete the last known-good backup.

## Host time synchronization gate

The exported `crypto_trading_clock_skew_milliseconds` value measures the
exchange-observed offset and the checked-in alert fires above one second. Also
monitor the host time service independently: on systemd hosts,
`timedatectl show --property=NTPSynchronized --value` must remain `yes`, and the
chrony/systemd-timesyncd service must be healthy. Disable mutation authority if
either the host synchronization check or the exchange-skew alert fails; do not
fix timestamp errors by widening `recvWindow`.

## Binance Mainnet manual lifecycle gate

This is the only gate with mainnet order authority. It proves one supervised
Binance Spot MAINNET LIMIT order lifecycle — submit, signed query, cancel,
final query — under real venue conditions, with real funds at risk up to the
declared notional cap. It grants no strategy any authority and is not
continuous operation.

### Prerequisites

Do not start this gate until all of the following hold:

- Every Testnet gate above has passed on the same candidate binary: the three
  order-lifecycle cases, both account-reconciliation products, the 24-hour
  soak, and the backup/restore drill. The host time synchronization gate is
  green.
- A dedicated Binance mainnet account used only for this gate, funded with
  only the minimal quote balance the lifecycle needs. Do not use an account
  with existing positions, open orders, or unrelated balances.
- A read-only shadow observation baseline captured with `live-reconcile`
  (below) proving expected balances, zero open orders on the target symbol,
  and the current exchangeInfo filters. The trade credentials must not exist
  in the environment during this step.
- A deliberately minimal order: a tick-aligned, non-marketable post-only
  price derived from the current book, the smallest quantity that satisfies
  the venue's minNotional, and a `--max-notional` cap set just above
  `price × quantity` — never a generous round number.

### Read-only shadow observation

`live-reconcile` accepts only the read-only credential family and constructs
an adapter type that cannot submit or cancel orders. Capture the baseline
before, and a closing report after, the lifecycle:

```sh
export BINANCE_MAINNET_READ_API_KEY='...'
export BINANCE_MAINNET_READ_API_SECRET='...'

"$LIFECYCLE_BIN" live-reconcile \
  --spot-symbol BTC-USDT-SPOT \
  --wire-symbol BTCUSDT \
  --include-exchange-info \
  --timeout-ms 10000 \
  --json > /srv/crypto-trading/evidence/live-reconcile-before.json
```

The baseline must show zero open orders on the symbol (the lifecycle refuses
foreign open orders by default) and balances that match the dedicated
account's expected funding. Use the reported exchangeInfo filters to derive
the price, quantity, and notional cap; never reuse values from this document
or from an earlier run.

### The acknowledged lifecycle

Run the lifecycle in the foreground with an operator watching. Credentials
come only from the dedicated mainnet **trade** environment variables; the
Testnet and mainnet-read families are not accepted on this path:

```sh
umask 077
install -d -m 0700 /srv/crypto-trading/live

export BINANCE_MAINNET_TRADE_API_KEY='...'
export BINANCE_MAINNET_TRADE_API_SECRET='...'
export LIVE_HISTORY='/srv/crypto-trading/live/binance-mainnet.jsonl'
export LIVE_CAMPAIGN='binance-mainnet-spot-001'
export LIVE_CLIENT_ID="$(uuidgen)"
export LIVE_PRICE='<FILTER_VALID_POST_ONLY_PRICE>'
export LIVE_QUANTITY='<FILTER_VALID_MINIMAL_QUANTITY>'
export LIVE_MAX_NOTIONAL='<CAP_JUST_ABOVE_PRICE_TIMES_QUANTITY>'

"$LIFECYCLE_BIN" live-lifecycle \
  --acknowledge-live-lifecycle \
  'I AUTHORIZE BINANCE MAINNET SPOT ORDER LIFECYCLE' \
  --campaign-id "$LIVE_CAMPAIGN" \
  --client-order-id "$LIVE_CLIENT_ID" \
  --history-path "$LIVE_HISTORY" \
  --side buy \
  --quantity "$LIVE_QUANTITY" \
  --price "$LIVE_PRICE" \
  --max-notional "$LIVE_MAX_NOTIONAL" \
  --time-in-force post-only \
  --expected-observation open \
  --spot-symbol BTC-USDT-SPOT \
  --wire-symbol BTCUSDT \
  --poll-interval-ms 2000 \
  --maximum-queries 30 \
  --timeout-ms 10000 \
  --json \
  | tee /srv/crypto-trading/live/lifecycle-result.json
```

The command refuses before any journal write or network call when the
acknowledgement phrase is not exact or `price × quantity` exceeds
`--max-notional`. Before submitting it re-derives venue truth: current
exchangeInfo filters, signed balances (a SELL requires sufficient base-asset
balance — there is no spot short), and the symbol's open orders.

A passing run's journal contains, in order: `live_lifecycle_planned`,
`live_lifecycle_admission_observed`, `live_lifecycle_submit_observed`, at
least one `live_lifecycle_query_observed` proving the expected state,
`live_lifecycle_cancel_planned`, `live_lifecycle_cancel_observed` (or
`live_lifecycle_outcome_unknown` followed by the authoritative final
cancelled query), and `live_lifecycle_completed`.

Archive the candidate checksum, redacted command arguments (never the
credentials or an environment dump), the CLI JSON output, the journal, and a
closing `live-reconcile` report proving zero open orders and reconciled
balances. Redact account identifiers before the bundle leaves the operator
host.

### Rollback and recovery

- **Query-first, never resubmit.** After a crash, timeout, or ambiguous
  response, rerun the *identical* command with the same campaign ID, client
  UUID, and history path. The resumed invocation appends
  `live_lifecycle_resumed` and issues a signed single-order query by the
  persisted UUID before any mutation; it never submits a second order for
  the campaign.
- **Exhausted or ambiguous campaigns stay failed.** If the cumulative query
  budget runs out or the final state cannot be proven, stop rerunning.
  Reconcile the persisted client UUID through `live-reconcile` and, if an
  order is still open, cancel it manually on the venue. Retain the
  unresolved campaign journal as failed gate evidence; do not start a new
  campaign until an authoritative query proves the old UUID has no order.
- **The kill-switch latch is terminal for the history file.** An unsafe
  terminal outcome appends `live_lifecycle_kill_switch_engaged`, and every
  later campaign on the same history path fails closed. Never edit or
  replace the journal to clear it. Resolve the venue state manually, record
  the operator decision alongside the archived evidence, and only then start
  a new campaign on a new history path.
- Remove the trade credentials from the environment as soon as the run and
  its closing reconcile are complete.

## Promotion and rollback

Promotion requires all repository quality gates, healthy and non-degraded Web
projections, the backup/restore drill, both credentialed Testnet reconciliation
products, the three credentialed Testnet lifecycle cases above, and the 24-hour
soak evidence. A local deterministic harness is not a substitute for
credentialed Binance Testnet evidence or the 24-hour soak. The mainnet manual
lifecycle gate builds on all of them and is the only mainnet order authority
in the release; autonomous strategy live execution remains closed pending the
strategy promotion gate and is not unlocked by passing any gate in this
runbook.

To roll back, keep the data volume and journal UUID unchanged, deploy the prior
image digest, and repeat the system projection check. Never roll back by
truncating or editing the append-only journal.
