#!/usr/bin/env python3
"""Clear (delete) whole Silver day-partitions so a backfill can rewrite them cleanly.

Motivation: the compactor dedupes exact ``event_id`` collisions, but it cannot
remove *stale* rows that a re-backfill leaves behind with a different content
hash — e.g. the old contract-anonymous ``oi_change`` rows (``option_symbol`` NULL)
that predate the named backfill era. Recovering such a day cleanly means
delete-then-write: drop the partition, then republish it through the backfill
stream. Wiped Silver is recoverable from immutable Bronze (and re-fetchable from
the source), so the delete is not a point of no return.

Guardrails (this deletes data, so they are not optional):
  * the resolved target must live strictly under the configured Silver root
  * the leaf directory must be a ``dt=YYYY-MM-DD`` hive partition
  * ``feed`` / ``instrument_type`` must be simple tokens (no separators, no ``..``)
  * dry-run is the default; deletion requires an explicit ``--apply``

Usage:
    .venv/bin/python scripts/clear_silver_partition.py --feed oi_change \
        --days 2026-07-01 2026-07-02            # dry-run report
    .venv/bin/python scripts/clear_silver_partition.py --feed oi_change \
        --days 2026-07-01 --apply               # actually delete
"""

import argparse
import re
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger("heber-clear-partition")

_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_]+$")


def partition_path(silver_root: Path, feed: str, instrument_type: str, day: str) -> Path:
    """Resolve the partition directory, rejecting any unsafe component."""
    if not _TOKEN_RE.match(feed):
        raise ValueError(f"unsafe feed token: {feed!r}")
    if not _TOKEN_RE.match(instrument_type):
        raise ValueError(f"unsafe instrument_type token: {instrument_type!r}")
    if not _DT_RE.match(day):
        raise ValueError(f"day must be YYYY-MM-DD, got {day!r}")

    silver_root = silver_root.resolve()
    path = (silver_root / f"feed={feed}" / f"instrument_type={instrument_type}" / f"dt={day}").resolve()

    # Defence in depth: the resolved path must stay under the Silver root and
    # must be a dt= leaf — never the feed/type/root dir itself.
    if silver_root not in path.parents:
        raise ValueError(f"refusing path outside silver root: {path}")
    if not path.name.startswith("dt="):
        raise ValueError(f"refusing non-dt partition path: {path}")
    return path


def _partition_stats(path: Path) -> tuple[int, int]:
    """Return (file_count, row_count) for the parquet files in a partition."""
    files = [f for f in path.rglob("*.parquet") if not f.name.startswith(".")]
    rows = 0
    for f in files:
        try:
            rows += pq.ParquetFile(f).metadata.num_rows
        except Exception as exc:  # noqa: BLE001 — a corrupt file must not hide the others
            logger.warning("clear_partition_stat_failed", file=str(f), error=str(exc))
    return len(files), rows


def clear_partitions(
    silver_root: Path,
    feed: str,
    instrument_type: str,
    days: list[str],
    apply: bool,
) -> list[dict]:
    """Report (and, when ``apply``, delete) the given day-partitions.

    Returns one record per day: ``{day, path, files, rows, action}`` where
    ``action`` is ``deleted`` / ``would-delete`` / ``missing``.
    """
    report: list[dict] = []
    for day in days:
        path = partition_path(silver_root, feed, instrument_type, day)
        if not path.exists():
            logger.info("clear_partition_missing", feed=feed, day=day, path=str(path))
            report.append({"day": day, "path": str(path), "files": 0, "rows": 0, "action": "missing"})
            continue

        files, rows = _partition_stats(path)
        if apply:
            shutil.rmtree(path)
            action = "deleted"
            logger.info("clear_partition_deleted", feed=feed, day=day, path=str(path), files=files, rows=rows)
        else:
            action = "would-delete"
            logger.info("clear_partition_dry_run", feed=feed, day=day, path=str(path), files=files, rows=rows)
        report.append({"day": day, "path": str(path), "files": files, "rows": rows, "action": action})
    return report


def main() -> None:
    from heber.config import settings

    ap = argparse.ArgumentParser(description="Clear Silver day-partitions (delete-then-write recovery).")
    ap.add_argument("--feed", required=True, help="Silver feed, e.g. oi_change")
    ap.add_argument("--instrument-type", default="equity")
    ap.add_argument("--days", nargs="+", required=True, help="ISO dates (YYYY-MM-DD) to clear")
    ap.add_argument("--apply", action="store_true", help="actually delete (default: dry-run report only)")
    args = ap.parse_args()

    report = clear_partitions(Path(settings.silver_path), args.feed, args.instrument_type, args.days, args.apply)
    total_files = sum(r["files"] for r in report)
    total_rows = sum(r["rows"] for r in report)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] feed={args.feed} instrument_type={args.instrument_type} days={len(report)}")
    for r in report:
        print(f"  {r['day']}  {r['action']:<12} files={r['files']:>4} rows={r['rows']:>8}")
    print(f"  TOTAL files={total_files} rows={total_rows}")
    if not args.apply:
        print("  (dry-run — re-run with --apply to delete)")


if __name__ == "__main__":
    main()
