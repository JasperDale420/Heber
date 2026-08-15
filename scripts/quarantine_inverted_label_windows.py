"""Move alert-label rows whose outcome predates their own alert out of Gold.

``ts_available`` on a label row is the outcome time, and for an expiry that is
``window_end``. The timezone relabel in ``MarketCalendar.add_trading_hours``
(fixed in b71d63e2) left some window_ends earlier than the alert that opened
them, so those rows carry ``ts_available < ts_event``.

That breaks the zero-leakage contract the lakehouse is built on:
``HeberReader.read_asof`` pushes ``ts_available <= asof_time`` into the scan, so
a backtest asking "what did I know at T" is handed an outcome that had not been
observed — indeed could not have been, since its window closed before it opened.
A quality flag cannot prevent that; only removing the rows from the ``dt=``
partitions can. Every one of them is an ``expired`` label over a zero-or-negative
observation window, so none carries a usable outcome.

The rows are moved, not deleted, to a sibling ``_quarantine`` tree — the same
shape ``meta_label_features`` uses for its all-Greeks-null rows. Readers only
walk ``dt=`` partitions, so quarantined rows disappear from queries while
staying on disk for inspection.

A full copy of every rewritten fragment is flushed to
``_quarantine/<reason>/_source_backup/`` before anything is touched, and the
quarantined rows land before the source is replaced. Interrupted at any point,
every row remains readable from at least one file — duplicated at worst, never
lost — and re-running repairs it.

Run scripts/flag_truncated_label_windows.py first — a row that stays in the
canonical tree should already carry its truncation flag.

Usage:
    uv run python scripts/quarantine_inverted_label_windows.py                 # dry run
    uv run python scripts/quarantine_inverted_label_windows.py --write
    uv run python scripts/quarantine_inverted_label_windows.py --write --expect-quarantined N
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from filelock import Timeout

sys.path.insert(0, str(Path(__file__).parent.parent))

from heber.config import settings
from heber.gold.partition_lock import LOCK_TIMEOUT_SECONDS, partition_lock

GOLD_LABELS = settings.gold_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1"
QUARANTINE_REASON = "window_end_before_alert"


def partition_dates() -> list[str]:
    """Canonical ``dt=`` partitions only — never the sibling ``_quarantine`` tree."""
    return sorted(d.name.replace("dt=", "") for d in GOLD_LABELS.iterdir() if d.name.startswith("dt="))


def partition_fragments(dt_str: str) -> list[Path]:
    """Readable parquet fragments in a partition.

    ``._*`` are AppleDouble sidecars that stat() with EPERM on the bind-mounted
    volume, and ``.tmp`` files are partial writes not yet promoted.
    """
    partition = GOLD_LABELS / f"dt={dt_str}"
    if not partition.is_dir():
        return []
    return sorted(
        f
        for f in partition.rglob("*.parquet")
        if f.is_file() and not f.name.startswith("._") and not f.name.endswith(".tmp")
    )


def inverted_mask(df: pd.DataFrame) -> pd.Series:
    """True where the outcome timestamp precedes the alert that produced it."""
    if "ts_available" not in df.columns or "ts_event" not in df.columns:
        return pd.Series(False, index=df.index)
    available = pd.to_datetime(df["ts_available"], utc=True, errors="coerce")
    event = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    return (available < event).fillna(False)


def _fsync_path(path: Path) -> None:
    """Push data to the device, not just to the drive's write cache.

    The lakehouse volume is exFAT and does not journal metadata, so a rename
    whose data has not landed publishes a zero-byte file. On macOS plain
    ``fsync`` only reaches the drive's cache; ``F_FULLFSYNC`` is the documented
    way to ask for the platter. It is unsupported on some filesystems, which
    report ``ENOTSUP`` — fall back rather than fail the move.
    """
    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            fcntl.fcntl(fd, getattr(fcntl, "F_FULLFSYNC", 51))
        except OSError:
            os.fsync(fd)
    finally:
        os.close(fd)


def _write_durably(df: pd.DataFrame, target: Path) -> None:
    """Write one parquet file, then fsync it and its directory."""
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f".{target.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        df.to_parquet(temp_path, index=False, compression="snappy")
        _fsync_path(temp_path)
        os.replace(temp_path, target)
        _fsync_path(target.parent)
    finally:
        temp_path.unlink(missing_ok=True)


def quarantine_partition(dt_str: str, write: bool) -> tuple[int, int, list[str]]:
    """Move one partition's inverted rows. Returns (rows, moved, errors)."""
    partition = GOLD_LABELS / f"dt={dt_str}"
    # A partition that is not there is not "nothing to do" — it is a date whose
    # leaking rows this run cannot see, and the run must not claim to have
    # covered it.
    if not partition.is_dir():
        return 0, 0, [f"dt={dt_str}: partition directory missing"]

    if not write:
        return _scan_partition(dt_str, write=False)

    try:
        with partition_lock(partition, timeout=LOCK_TIMEOUT_SECONDS):
            return _scan_partition(dt_str, write=True)
    except Timeout:
        return 0, 0, [f"dt={dt_str}: partition lock timeout — another writer holds it"]


def _scan_partition(dt_str: str, write: bool) -> tuple[int, int, list[str]]:
    rows = moved = 0
    errors: list[str] = []

    for fragment in partition_fragments(dt_str):
        try:
            df = pd.read_parquet(fragment)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{fragment.name}: unreadable: {exc}")
            continue

        rows += len(df)
        inverted = inverted_mask(df)
        count = int(inverted.sum())
        if not count:
            continue

        moved += count
        if not write:
            continue

        try:
            # The retained rows exist only inside the fragment about to be
            # replaced, on a volume with no metadata journal, so a full copy of
            # it goes down first and stays. Then the quarantined rows, then the
            # rewrite. Interrupted at any point, every row is still readable
            # from at least one file — duplicated at worst, never lost — and a
            # re-run resolves it.
            #
            # Paths are keyed by the fragment's location under the partition,
            # not its stem: rglob reaches nested layouts (`hour=14/part-a`,
            # `hour=15/part-a`) whose stems collide.
            relative = fragment.relative_to(GOLD_LABELS / f"dt={dt_str}")
            reason_root = GOLD_LABELS / "_quarantine" / QUARANTINE_REASON
            _write_durably(df, reason_root / "_source_backup" / f"dt={dt_str}" / relative)
            _write_durably(df[inverted].reset_index(drop=True), reason_root / f"dt={dt_str}" / relative)

            retained = df[~inverted].reset_index(drop=True)
            if retained.empty:
                # A zero-row parquet can carry Arrow `null` column types that
                # refuse to merge with the real fragments beside it, so the
                # fragment goes rather than staying behind empty. Every row of
                # it is already in the quarantine tree and the backup.
                fragment.unlink()
            else:
                _write_durably(retained, fragment)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{fragment.name}: move failed: {exc}")

    return rows, moved, errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Persist the move (default: dry run)")
    parser.add_argument("--dates", nargs="*", help="Restrict to these partitions (YYYY-MM-DD)")
    # The lakehouse lives on a network volume that has come back partially
    # mounted before. Pin the dry run's totals to the write run so a short read
    # fails instead of reporting a clean pass over a smaller cohort.
    parser.add_argument("--expect-rows", type=int, help="Fail unless exactly this many rows are scanned")
    parser.add_argument("--expect-quarantined", type=int, help="Fail unless exactly this many rows move")
    args = parser.parse_args()

    dates = args.dates or partition_dates()

    print("=" * 78)
    print("QUARANTINE LABEL ROWS WHOSE OUTCOME PREDATES THEIR ALERT")
    print(f"  dataset    : {GOLD_LABELS}")
    print(f"  reason     : {QUARANTINE_REASON}")
    print(f"  partitions : {len(dates)}")
    print(f"  mode       : {'WRITE' if args.write else 'DRY RUN'}")
    print("=" * 78)

    tot_rows = tot_moved = 0
    errors: list[tuple[str, str]] = []
    changed: list[tuple[str, int, int]] = []

    for dt_str in dates:
        rows, moved, errs = quarantine_partition(dt_str, args.write)
        errors.extend((dt_str, e) for e in errs)
        tot_rows += rows
        tot_moved += moved
        if moved:
            changed.append((dt_str, rows, moved))

    print(f"\n{'dt':<12}{'rows':>10}{'moved':>10}")
    for dt_str, rows, moved in changed:
        print(f"{dt_str:<12}{rows:>10}{moved:>10}")

    print("\n" + "-" * 78)
    print(f"  rows scanned       : {tot_rows}")
    print(f"  rows quarantined   : {tot_moved}")
    print(f"  partitions changed : {len(changed)}")

    if errors:
        print(f"\n  !! {len(errors)} fragment(s) could not be processed:")
        for dt_str, err in errors:
            print(f"     {dt_str}: {err}")

    mismatches = [
        f"{label}: expected {expected}, saw {actual}"
        for label, expected, actual in (
            ("rows scanned", args.expect_rows, tot_rows),
            ("rows quarantined", args.expect_quarantined, tot_moved),
        )
        if expected is not None and expected != actual
    ]
    if mismatches:
        print("\n  !! cohort mismatch — refusing to report success:")
        for line in mismatches:
            print(f"     {line}")

    if not args.write:
        print("\n  *** DRY RUN — nothing moved. Pass --write to persist. ***")
        sys.exit(1 if errors or mismatches else 0)

    manifest = {
        "run_at": datetime.now(UTC).isoformat(),
        "reason": QUARANTINE_REASON,
        "rows_scanned": tot_rows,
        "rows_quarantined": tot_moved,
        "partitions_changed": {dt: {"rows": r, "moved": m} for dt, r, m in changed},
        "partitions_failed": dict(errors),
        "cohort_mismatches": mismatches,
    }
    manifest_path = GOLD_LABELS / f"_inverted_window_quarantine-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  manifest: {manifest_path}")

    # A fragment that could not be processed still leaks into as-of reads, so
    # the run has not achieved what it claims — fail loudly.
    if errors or mismatches:
        sys.exit(1)


if __name__ == "__main__":
    main()
