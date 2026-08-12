#!/bin/sh
set -eu

usage() {
    echo "usage: $0 JOURNAL BACKUP_DIR DRILL_DIR JOURNAL_ID WEB_BINARY" >&2
    exit 64
}

[ "$#" -eq 5 ] || usage

journal=$1
backup_root=$2
drill_root=$3
journal_id=$4
web_binary=$5

[ -f "$journal" ] || {
    echo "journal is not a regular file: $journal" >&2
    exit 66
}
[ -x "$web_binary" ] || {
    echo "web verifier is not executable: $web_binary" >&2
    exit 66
}
command -v curl >/dev/null 2>&1 || {
    echo "curl is required for the restore projection check" >&2
    exit 69
}
command -v sha256sum >/dev/null 2>&1 || {
    echo "sha256sum is required for byte-integrity verification" >&2
    exit 69
}
command -v flock >/dev/null 2>&1 || {
    echo "flock is required to prove the journal writer is quiescent" >&2
    exit 69
}

mkdir -p "$backup_root" "$drill_root"

journal_abs=$(cd "$(dirname "$journal")" && pwd -P)/$(basename "$journal")
backup_abs=$(cd "$backup_root" && pwd -P)
drill_abs=$(cd "$drill_root" && pwd -P)
lock_path="$journal_abs.jsonl.lock"

[ ! -L "$lock_path" ] || {
    echo "refusing symlinked journal writer lock: $lock_path" >&2
    exit 65
}
exec 9>>"$lock_path"
flock -n 9 || {
    echo "journal writer lease is active; quiesce the owner or use a filesystem snapshot" >&2
    exit 75
}

[ "$backup_abs" != "$drill_abs" ] || {
    echo "backup and drill directories must be distinct" >&2
    exit 65
}

stamp=$(date -u +%Y%m%dT%H%M%SZ)
backup="$backup_abs/operations-$stamp.jsonl"
manifest="$backup.sha256"

[ ! -e "$backup" ] || {
    echo "refusing to overwrite existing backup: $backup" >&2
    exit 73
}

size_before=$(wc -c <"$journal_abs")
source_hash_before=$(sha256sum "$journal_abs" | awk '{print $1}')
copy_tmp="$backup.tmp"
cp -- "$journal_abs" "$copy_tmp"
size_after=$(wc -c <"$journal_abs")
source_hash_after=$(sha256sum "$journal_abs" | awk '{print $1}')
if [ "$size_before" -ne "$size_after" ] || [ "$source_hash_before" != "$source_hash_after" ]; then
    rm -f -- "$copy_tmp"
    echo "journal changed during backup; retry after quiescing writes or taking a filesystem snapshot" >&2
    exit 75
fi
mv -- "$copy_tmp" "$backup"
(cd "$backup_abs" && sha256sum "$(basename "$backup")" >"$(basename "$manifest")")

restore_dir=$(mktemp -d "$drill_abs/restore.XXXXXX")
restored="$restore_dir/operations.jsonl"
cp -- "$backup" "$restored"
expected_hash=$(sha256sum "$backup" | awk '{print $1}')
restored_hash=$(sha256sum "$restored" | awk '{print $1}')
[ "$expected_hash" = "$restored_hash" ] || {
    echo "restored copy does not match the backup manifest" >&2
    exit 74
}
cp -- "$manifest" "$restore_dir/backup.sha256"

drill_port=${CRYPTO_TRADING_DRILL_PORT:-18787}
log="$restore_dir/verifier.log"
"$web_binary" \
    --history-path "$restored" \
    --journal-id "$journal_id" \
    --port "$drill_port" \
    --allow-open-loopback-read-api >"$log" 2>&1 &
verifier_pid=$!

cleanup() {
    # shellcheck disable=SC2317  # invoked indirectly via the trap below
    kill "$verifier_pid" >/dev/null 2>&1 || true
    # shellcheck disable=SC2317
    wait "$verifier_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

attempt=0
while [ "$attempt" -lt 50 ]; do
    if curl --fail --silent --show-error \
        "http://127.0.0.1:$drill_port/api/v1/system" \
        >"$restore_dir/system.json"; then
        echo "backup: $backup"
        echo "manifest: $manifest"
        echo "verified restore: $restore_dir"
        exit 0
    fi
    if ! kill -0 "$verifier_pid" >/dev/null 2>&1; then
        echo "restored journal failed bounded replay; see $log" >&2
        exit 70
    fi
    attempt=$((attempt + 1))
    sleep 0.1
done

echo "restored journal verifier did not become ready; see $log" >&2
exit 70
