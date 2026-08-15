"""Retro-flag Gold meta_label_features rows whose enrichment was not point-in-time.

Before the freshness gate landed, ``EnrichmentBackfillScanner`` re-enriched rows up
to three days old on an hourly cycle, refetching enrichment live and writing present-day
values onto past alerts. Those rows carry real numbers, so nothing detects them by
inspection — but ``ts_available`` records when the row was written, and the enrichment
on it was fetched at that moment, so ``ts_available - alert_time`` bounds how stale it
was when captured.

The measure spans enrichment *and* the write that followed, so it is an upper bound and
the flagged set is a superset of the truly contaminated one. ``ts_available`` and
``alert_time`` stay on every row, so a training run can recompute the exact lag and
apply a stricter threshold than the one used here.

This appends a provenance flag to the affected rows so a training run can exclude or
down-weight them. It only ever adds to ``quality_flags``; no other column is touched
and no row is moved, added, or dropped.

Usage:
    uv run python scripts/flag_legacy_enrichment.py                  # dry run
    uv run python scripts/flag_legacy_enrichment.py --write
    uv run python scripts/flag_legacy_enrichment.py --write --dates 2026-03-11
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from heber.config import settings
from heber.ml.datasets import (
    QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE,
    QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN,
    add_enrichment_provenance_flags,
    provenance_bound,
    quality_flag_series,
)

GOLD_FEATURES = settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1"


def partition_dates() -> list[str]:
    """Canonical ``dt=`` partitions only — never the sibling ``_quarantine`` tree."""
    return sorted(d.name.replace("dt=", "") for d in GOLD_FEATURES.iterdir() if d.name.startswith("dt="))


def flag_partition(dt_str: str, max_age: timedelta, write: bool) -> tuple[int, int, int, str | None]:
    """Flag one partition. Returns (rows, newly_late, newly_unknown, error)."""
    out_file = GOLD_FEATURES / f"dt={dt_str}" / "data.parquet"
    if not out_file.exists():
        return 0, 0, 0, "missing"

    def _classify(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
        before = quality_flag_series(df)
        flagged = add_enrichment_provenance_flags(df, max_age=max_age)
        after = quality_flag_series(flagged)
        new_late = sum(
            1
            for i in df.index
            if QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE in after[i]
            and QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE not in before[i]
        )
        new_unknown = sum(
            1
            for i in df.index
            if QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN in after[i]
            and QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN not in before[i]
        )
        return flagged, new_late, new_unknown

    if not write:
        try:
            df = pd.read_parquet(out_file)
        except Exception as exc:  # noqa: BLE001
            return 0, 0, 0, f"unreadable: {exc}"
        _, new_late, new_unknown = _classify(df)
        return len(df), new_late, new_unknown, None

    # heber-watch writes these same partitions live, so the read, the flagging and
    # the replace all happen under the lock the Gold writer takes. Reading first
    # and locking only for the write would let a row appended in between be lost
    # by the rename.
    from filelock import FileLock, Timeout

    lock_file = out_file.with_suffix(".parquet.lock")
    try:
        with FileLock(lock_file, timeout=30):
            try:
                df = pd.read_parquet(out_file)
            except Exception as exc:  # noqa: BLE001
                return 0, 0, 0, f"unreadable: {exc}"

            flagged, new_late, new_unknown = _classify(df)
            if not (new_late or new_unknown):
                return len(df), 0, 0, None

            # Written directly rather than through persist_features_to_gold: that
            # path re-runs the all-Greeks-null quarantine split, which would
            # relocate legacy rows out of the canonical partition as a side effect
            # of adding a flag.
            temp_path = out_file.with_name(f".{out_file.name}.tmp-{os.getpid()}-{uuid4().hex}")
            try:
                flagged.to_parquet(temp_path, index=False)
                os.replace(temp_path, out_file)
            finally:
                temp_path.unlink(missing_ok=True)
    except Timeout:
        return 0, 0, 0, "lock timeout"

    return len(df), new_late, new_unknown, None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Persist flags (default: dry run)")
    parser.add_argument("--dates", nargs="*", help="Restrict to these partitions (YYYY-MM-DD)")
    parser.add_argument(
        "--bound-minutes",
        type=int,
        default=int(provenance_bound().total_seconds() // 60),
        help="Capture lag above which enrichment is considered stale (default: the live gate's bound)",
    )
    args = parser.parse_args()

    max_age = timedelta(minutes=args.bound_minutes)
    dates = args.dates or partition_dates()

    print("=" * 78)
    print("RETRO-FLAG LEGACY ENRICHMENT PROVENANCE")
    print(f"  bound      : {args.bound_minutes} min (matches the live freshness gate)")
    print(f"  partitions : {len(dates)}")
    print(f"  mode       : {'WRITE' if args.write else 'DRY RUN'}")
    print("=" * 78)

    tot_rows = tot_late = tot_unknown = 0
    errors: list[tuple[str, str]] = []
    changed: list[tuple[str, int, int, int]] = []

    for dt_str in dates:
        rows, late, unknown, err = flag_partition(dt_str, max_age, args.write)
        if err:
            errors.append((dt_str, err))
            continue
        tot_rows += rows
        tot_late += late
        tot_unknown += unknown
        if late or unknown:
            changed.append((dt_str, rows, late, unknown))

    print(f"\n{'dt':<12}{'rows':>8}{'late':>8}{'unknown':>10}")
    for dt_str, rows, late, unknown in changed:
        print(f"{dt_str:<12}{rows:>8}{late:>8}{unknown:>10}")

    print("\n" + "-" * 78)
    print(f"  rows scanned          : {tot_rows}")
    print(f"  flagged {QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE:<22}: {tot_late}")
    print(f"  flagged {QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN:<22}: {tot_unknown}")
    print(f"  partitions changed    : {len(changed)}")

    if errors:
        print(f"\n  !! {len(errors)} partition(s) could not be processed:")
        for dt_str, err in errors:
            print(f"     {dt_str}: {err}")

    if not args.write:
        print("\n  *** DRY RUN — no data written. Pass --write to persist. ***")
        sys.exit(1 if errors else 0)

    # Record what was flagged, under which bound, next to the data — otherwise
    # nothing downstream can tell why a row carries the flag or reproduce the run.
    manifest = {
        "run_at": datetime.now(UTC).isoformat(),
        "bound_minutes": args.bound_minutes,
        "rows_scanned": tot_rows,
        QUALITY_FLAG_ENRICHMENT_CAPTURED_LATE: tot_late,
        QUALITY_FLAG_ENRICHMENT_PROVENANCE_UNKNOWN: tot_unknown,
        "partitions_changed": {dt: {"rows": r, "late": late, "unknown": unk} for dt, r, late, unk in changed},
        "partitions_failed": dict(errors),
    }
    manifest_path = GOLD_FEATURES / f"_provenance_migration-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  manifest: {manifest_path}")

    # A partition that could not be processed is unflagged contamination, so the
    # run has not achieved what it claims — fail loudly rather than exiting clean.
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
