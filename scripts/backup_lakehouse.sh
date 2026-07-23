#!/usr/bin/env bash
# Nightly lakehouse backup: the only second copy of Bronze/Silver/Gold + the
# catalog Postgres database. Introduced after the Jul 10-19 2026 outage audit
# found the entire lakehouse existed as a single copy on a removable exFAT
# drive that had faulted twice in nine days.
#
# Bronze is millions of tiny immutable .jsonl.gz files. Copying them one-by-one
# onto the exFAT drive never completes — each file costs a create/fsync/rename,
# and at a few files/sec the run takes days. Instead we bundle each
# provider/feed/dt day-partition into a single .tar (sequential write, one dest
# file per day) and only re-archive a day whose contents changed. Bronze is
# append-only but a past day can still receive late backfill, so change is
# detected by "any file newer than the existing archive", not by day age.
#
# Silver/Gold are large Parquet files (few, big) — rsync mirrors them fine.
# Postgres is dumped with pg_dump for a consistent logical backup; rsyncing the
# live data directory would capture a torn, unrestorable copy.
#
# No --delete anywhere: a source-side deletion or a broken/empty mount must
# never propagate to the backup.
set -uo pipefail

SRC_ROOT="${HEBER_VOLUME_ROOT:-/Volumes/heber}"
DST_ROOT="${HEBER_BACKUP_ROOT:-/Volumes/Raw Data/heber-backup}"
LOG_DIR="${HEBER_PROJECT_DIR:-/Users/jacobmcmillan/Empire/Heber}/logs"
MARKER="$DST_ROOT/.last-backup-ok"
LOG="$LOG_DIR/heber-backup_$(date '+%Y-%m-%d').log"

mkdir -p "$LOG_DIR"
exec >> "$LOG" 2>&1
echo "=== backup start $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
# ponytail: no run-lock. launchd single-instances the scheduled job, and every
# archive is written to a per-pid temp then atomically renamed — so even a manual
# overlapping run can only waste drive I/O, never corrupt an archive. Add a lock
# only if overlapping manual runs become a real problem.

# Guard both ends: never run against a broken source mount (a no-op that looks
# like success) and never run without the target drive.
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

# --- Bronze: one .tar per provider/feed/dt day-partition, incremental --------
BRONZE_SRC="$SRC_ROOT/data/bronze"
BRONZE_DST="$DST_ROOT/data/bronze"
if [[ -d "$BRONZE_SRC" ]]; then
    echo "--- bronze archive $(date -u '+%H:%M:%SZ')"
    archived=0
    skipped=0
    # -prune stops the walk at the dt= level, so find never enumerates the
    # millions of files inside — only the (provider,feed,dt) day directories.
    while IFS= read -r -d '' dtdir; do
        rel="${dtdir#"$BRONZE_SRC"/}"                 # provider=P/feed=F/dt=D
        archive="$BRONZE_DST/$rel.tar"
        # Append-only bronze: a new file bumps its hour= directory's mtime, so
        # checking directories (not the millions of leaf files) is enough to
        # detect change — including late backfill into an already-archived day.
        if [[ -f "$archive" && -z "$(find "$dtdir" -type d -newer "$archive" 2>/dev/null | head -n 1)" ]]; then
            skipped=$((skipped + 1))
            continue
        fi
        mkdir -p "$(dirname "$archive")"
        tmp="$archive.tmp.$$"
        # Files are already gzip-compressed — tar without -z (no CPU wasted
        # re-compressing). Skip AppleDouble sidecars (stat() EPERM on exFAT).
        # Write to a temp then atomically rename.
        if tar --exclude '._*' --exclude '*.tmp' -cf "$tmp" \
               -C "$(dirname "$dtdir")" "$(basename "$dtdir")" 2>>"$LOG"; then
            mv -f "$tmp" "$archive"
            archived=$((archived + 1))
        else
            rm -f "$tmp"
            echo "ERROR: tar failed for bronze/$rel"
            FAIL=1
        fi
    done < <(find "$BRONZE_SRC" -type d -name 'dt=*' -prune -print0 2>/dev/null)

    # Non-partition bronze content (e.g. _quarantine) — small, mirror as-is.
    while IFS= read -r -d '' extra; do
        name="$(basename "$extra")"
        if ! rsync -a --exclude '._*' --exclude '*.tmp' "$extra/" "$BRONZE_DST/$name/"; then
            echo "ERROR: rsync failed for bronze/$name"
            FAIL=1
        fi
    done < <(find "$BRONZE_SRC" -mindepth 1 -maxdepth 1 -type d -not -name 'provider=*' -print0 2>/dev/null)

    echo "    bronze: $archived archived, $skipped unchanged"
else
    echo "WARNING: $BRONZE_SRC missing — skipped"
fi

# --- Silver / Gold: large Parquet, rsync mirror ------------------------------
for sub in data/silver data/gold; do
    if [[ ! -d "$SRC_ROOT/$sub" ]]; then
        echo "WARNING: $SRC_ROOT/$sub missing — skipped"
        continue
    fi
    echo "--- rsync $sub $(date -u '+%H:%M:%SZ')"
    mkdir -p "$DST_ROOT/$sub"
    # --exclude '._*': AppleDouble sidecars on exFAT stat() EPERM and abort rsync.
    if ! rsync -a --exclude '._*' --exclude '*.tmp' "$SRC_ROOT/$sub/" "$DST_ROOT/$sub/"; then
        echo "ERROR: rsync failed for $sub"
        FAIL=1
    fi
done

# --- Postgres catalog: consistent logical dump via the running container -----
# The catalog is rebuildable from the lake, so a dump failure is a warning, not
# a backup failure — but a torn copy of the live data dir would be worse than
# none, so we never fall back to rsyncing it.
if [[ "${HEBER_BACKUP_SKIP_POSTGRES:-0}" != "1" ]]; then
    pg_container="${HEBER_BACKUP_PG_CONTAINER:-heber-postgres}"
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$pg_container"; then
        echo "--- pg_dump $pg_container $(date -u '+%H:%M:%SZ')"
        mkdir -p "$DST_ROOT/postgres"
        tmp="$DST_ROOT/postgres/heber_catalog.sql.gz.tmp"
        if docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-heber_dev_password}" "$pg_container" \
               pg_dump -U "${POSTGRES_USER:-heber}" "${POSTGRES_DB:-heber_catalog}" 2>>"$LOG" \
               | gzip > "$tmp"; then
            mv -f "$tmp" "$DST_ROOT/postgres/heber_catalog.sql.gz"
        else
            rm -f "$tmp"
            echo "WARNING: pg_dump failed — catalog not backed up (rebuildable from lake)"
        fi
    else
        echo "WARNING: container '$pg_container' not running — catalog dump skipped"
    fi
fi

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
