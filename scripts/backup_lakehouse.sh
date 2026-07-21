#!/usr/bin/env bash
# Nightly lakehouse backup: the only second copy of Bronze/Silver/Gold + the
# catalog Postgres data dir. Introduced after the Jul 10-19 2026 outage audit
# found the entire lakehouse existed as a single copy on a removable exFAT
# drive that had faulted twice in nine days.
#
# Mirror semantics: rsync -a WITHOUT --delete — Bronze is append-only and a
# source-side deletion (or a broken/empty mount) must never propagate to the
# backup. Old Silver compaction leftovers accumulating on the mirror is an
# acceptable cost for that safety.
set -uo pipefail

SRC_ROOT="${HEBER_VOLUME_ROOT:-/Volumes/heber}"
DST_ROOT="${HEBER_BACKUP_ROOT:-/Volumes/Raw Data/heber-backup}"
LOG_DIR="${HEBER_PROJECT_DIR:-/Users/jacobmcmillan/Empire/Heber}/logs"
MARKER="$DST_ROOT/.last-backup-ok"
LOG="$LOG_DIR/heber-backup_$(date '+%Y-%m-%d').log"

mkdir -p "$LOG_DIR"
exec >> "$LOG" 2>&1
echo "=== backup start $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

# Guard both ends: never run against a broken source mount (would produce a
# no-op that looks like success) and never run without the target drive.
if [[ ! -f "$SRC_ROOT/data/.heber-sentinel" ]]; then
    echo "ERROR: source sentinel missing — heber volume not mounted; aborting"
    exit 1
fi
if [[ ! -d "$(dirname "$DST_ROOT")" ]]; then
    echo "ERROR: backup target drive not mounted at $(dirname "$DST_ROOT"); aborting"
    exit 1
fi
mkdir -p "$DST_ROOT"

FAIL=0
for sub in data/bronze data/silver data/gold postgres; do
    if [[ ! -d "$SRC_ROOT/$sub" ]]; then
        echo "WARNING: $SRC_ROOT/$sub missing — skipped"
        continue
    fi
    echo "--- rsync $sub $(date -u '+%H:%M:%SZ')"
    mkdir -p "$DST_ROOT/$sub"
    # --exclude '._*': AppleDouble sidecars on exFAT stat() EPERM and abort rsync.
    if ! rsync -a --exclude '._*' --exclude '*.tmp' "$SRC_ROOT/$sub/" "$DST_ROOT/$sub/"; then
        echo "ERROR: rsync failed for $sub (exit $?)"
        FAIL=1
    fi
done

if [[ "$FAIL" -eq 0 ]]; then
    date -u '+%Y-%m-%dT%H:%M:%SZ' > "$MARKER"
    # Twin marker on the heber volume: the dataflow-health container mounts
    # only /Volumes/heber/data, so the backup_freshness check reads this one.
    mkdir -p "$SRC_ROOT/data/ops"
    date -u '+%Y-%m-%dT%H:%M:%SZ' > "$SRC_ROOT/data/ops/backup-last-ok"
    echo "=== backup OK $(date -u '+%H:%M:%SZ'); marker updated"
else
    echo "=== backup FAILED $(date -u '+%H:%M:%SZ'); marker NOT updated"
    exit 1
fi
