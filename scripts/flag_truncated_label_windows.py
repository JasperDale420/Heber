"""Retro-flag Gold alert-label rows whose observation window closed early.

``MarketCalendar.add_trading_hours`` reset its cursor to the current session's
open on every boundary crossing, so it burned one session's minutes per
iteration against the start date instead of advancing. A 720 trading-hour LEAP
window landed at open + 5h on the alert day and a 120-hour SWING window at
open + 3h; a separate timezone relabel pushed some windows to before the alert
itself. Fixed in b71d63e2 — but every label already written was resolved against
the short window.

The damage is visible on the rows themselves: ``window_duration_hours`` is
wall-clock ``window_end - alert_time``, and the calendar only ever advances,
skipping non-trading time rather than consuming it, so a correct window always
spans at least its nominal count of hours in wall clock. Anything shorter could
not have come from an intact calendar.

This appends ``label_window_truncated`` to the affected rows so a consumer can
exclude or down-weight them. It only ever adds to ``quality_flags``; no other
column is touched and no row is moved, added, or dropped. ``horizon`` and
``window_duration_hours`` stay on every row, so a consumer can recompute the
shortfall and apply a rule of its own.

Each partition is rewritten under the shared Gold partition lock, which
``scripts/compact_gold.py`` also takes: compaction merges a partition's
fragments and deletes the originals, so interleaving the two would either drop
the flags or resurrect a deleted fragment alongside its compacted copy.

Usage:
    uv run python scripts/flag_truncated_label_windows.py                  # dry run
    uv run python scripts/flag_truncated_label_windows.py --write
    uv run python scripts/flag_truncated_label_windows.py --write --dates 2026-03-11
    uv run python scripts/flag_truncated_label_windows.py --write --expect-rows N --expect-flagged M
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from filelock import Timeout

from heber.config import settings
from heber.gold.partition_lock import LOCK_TIMEOUT_SECONDS, partition_lock
from heber.ml.datasets import (
    QUALITY_FLAG_LABEL_WINDOW_TRUNCATED,
    add_label_window_flags,
    label_window_truncated_mask,
    quality_flag_series,
)

GOLD_LABELS = settings.gold_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1"


def partition_dates() -> list[str]:
    """Canonical ``dt=`` partitions only — never a sibling ``_quarantine`` tree."""
    return sorted(d.name.replace("dt=", "") for d in GOLD_LABELS.iterdir() if d.name.startswith("dt="))


def partition_fragments(dt_str: str) -> list[Path]:
    """Readable parquet fragments in a partition.

    ``._*`` are AppleDouble sidecars that stat() with EPERM on the bind-mounted
    volume, and ``.tmp`` files are partial writes the label writer has not yet
    promoted.
    """
    partition = GOLD_LABELS / f"dt={dt_str}"
    if not partition.is_dir():
        return []
    return sorted(
        f
        for f in partition.rglob("*.parquet")
        if f.is_file() and not f.name.startswith("._") and not f.name.endswith(".tmp")
    )


def _replace_atomically(df: pd.DataFrame, target: Path) -> None:
    """Rewrite one fragment in place, durably.

    The lakehouse volume is exFAT and does not journal metadata, so a rename
    whose data has not reached the platter publishes a zero-byte file. Both the
    file and its directory are flushed before and after the swap.

    ``LabelWriter`` needs no coordination here — it only ever promotes *new*
    uniquely-named fragments into a partition and never rewrites an existing
    one. The compactor does, and is held off by the partition lock.
    """
    temp_path = target.with_name(f".{target.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        df.to_parquet(temp_path, index=False, compression="snappy")
        fd = os.open(temp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temp_path, target)
        dir_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        temp_path.unlink(missing_ok=True)


def flag_partition(dt_str: str, write: bool) -> tuple[int, int, list[str]]:
    """Flag one partition. Returns (rows, newly_flagged, errors)."""
    partition = GOLD_LABELS / f"dt={dt_str}"
    # A partition that is not there is not "nothing to do" — it is a date whose
    # contamination this run cannot see, and the run must not claim to have
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
    rows = newly = 0
    errors: list[str] = []

    for fragment in partition_fragments(dt_str):
        try:
            df = pd.read_parquet(fragment)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{fragment.name}: unreadable: {exc}")
            continue

        rows += len(df)
        before = quality_flag_series(df)
        already = before.apply(lambda flags: QUALITY_FLAG_LABEL_WINDOW_TRUNCATED in flags)
        fresh = int((label_window_truncated_mask(df) & ~already).sum())
        if not fresh:
            continue

        newly += fresh
        if write:
            try:
                _replace_atomically(add_label_window_flags(df), fragment)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{fragment.name}: write failed: {exc}")

    return rows, newly, errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Persist flags (default: dry run)")
    parser.add_argument("--dates", nargs="*", help="Restrict to these partitions (YYYY-MM-DD)")
    # The lakehouse lives on a network volume that has come back partially
    # mounted before. Pin the dry run's totals to the write run so a short read
    # fails instead of reporting a clean pass over a smaller cohort.
    parser.add_argument("--expect-rows", type=int, help="Fail unless exactly this many rows are scanned")
    parser.add_argument("--expect-flagged", type=int, help="Fail unless exactly this many rows are newly flagged")
    args = parser.parse_args()

    dates = args.dates or partition_dates()

    print("=" * 78)
    print("RETRO-FLAG TRUNCATED ALERT-LABEL WINDOWS")
    print(f"  dataset    : {GOLD_LABELS}")
    print(f"  partitions : {len(dates)}")
    print(f"  mode       : {'WRITE' if args.write else 'DRY RUN'}")
    print("=" * 78)

    tot_rows = tot_flagged = 0
    errors: list[tuple[str, str]] = []
    changed: list[tuple[str, int, int]] = []

    for dt_str in dates:
        rows, flagged, errs = flag_partition(dt_str, args.write)
        errors.extend((dt_str, e) for e in errs)
        tot_rows += rows
        tot_flagged += flagged
        if flagged:
            changed.append((dt_str, rows, flagged))

    print(f"\n{'dt':<12}{'rows':>10}{'flagged':>10}")
    for dt_str, rows, flagged in changed:
        print(f"{dt_str:<12}{rows:>10}{flagged:>10}")

    print("\n" + "-" * 78)
    print(f"  rows scanned       : {tot_rows}")
    print(f"  flagged {QUALITY_FLAG_LABEL_WINDOW_TRUNCATED:<11}: {tot_flagged}")
    print(f"  partitions changed : {len(changed)}")

    if errors:
        print(f"\n  !! {len(errors)} fragment(s) could not be processed:")
        for dt_str, err in errors:
            print(f"     {dt_str}: {err}")

    mismatches = [
        f"{label}: expected {expected}, saw {actual}"
        for label, expected, actual in (
            ("rows scanned", args.expect_rows, tot_rows),
            ("rows flagged", args.expect_flagged, tot_flagged),
        )
        if expected is not None and expected != actual
    ]
    if mismatches:
        print("\n  !! cohort mismatch — refusing to report success:")
        for line in mismatches:
            print(f"     {line}")

    if not args.write:
        print("\n  *** DRY RUN — no data written. Pass --write to persist. ***")
        sys.exit(1 if errors or mismatches else 0)

    # Record what was flagged next to the data — otherwise nothing downstream
    # can tell why a row carries the flag or reproduce the run.
    manifest = {
        "run_at": datetime.now(UTC).isoformat(),
        "rows_scanned": tot_rows,
        QUALITY_FLAG_LABEL_WINDOW_TRUNCATED: tot_flagged,
        "partitions_changed": {dt: {"rows": r, "flagged": f} for dt, r, f in changed},
        "partitions_failed": dict(errors),
        "cohort_mismatches": mismatches,
    }
    manifest_path = GOLD_LABELS / f"_label_window_migration-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  manifest: {manifest_path}")

    # A fragment that could not be processed is unflagged contamination, so the
    # run has not achieved what it claims — fail loudly rather than exiting clean.
    if errors or mismatches:
        sys.exit(1)


if __name__ == "__main__":
    main()
