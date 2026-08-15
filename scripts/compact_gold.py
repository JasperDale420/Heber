#!/usr/bin/env python3
"""Compact fragmented Gold layer parquet files.

Reads all parquet fragments within each dt= partition, concatenates them into a
single compacted file, and removes the originals.  Safe to run repeatedly
(idempotent) — partitions with 0 or 1 files are skipped automatically.

Skips macOS resource fork files (._*) and .DS_Store.

Usage:
    # Compact everything with >1 file per partition (default)
    uv run python scripts/compact_gold.py

    # Compact only a specific dataset
    uv run python scripts/compact_gold.py --dataset labels_alert_barriers

    # Only compact partitions with >= 5 fragments
    uv run python scripts/compact_gold.py --min-files 5

    # Dry-run — show what would be compacted without writing
    uv run python scripts/compact_gold.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from heber.gold.partition_lock import LOCK_TIMEOUT_SECONDS, partition_lock

GOLD_ROOT = Path("/Volumes/heber/data/gold")
COMPACTED_FILENAME = "compacted.parquet"

# Standard timestamp resolution for Gold layer (microseconds, UTC)
_TARGET_TS_TYPE = pa.timestamp("us", tz="UTC")


def log(msg: str) -> None:
    ts = datetime.now(UTC).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _unify_timestamp_resolution(table: pa.Table) -> pa.Table:
    """Cast all timestamp columns to microsecond resolution for schema compatibility.

    Different Gold writers may produce us vs ns timestamps.  Unifying to us
    before concat prevents ArrowInvalid schema-mismatch errors.
    """
    new_columns: list[tuple[str, pa.Array]] = []
    changed = False
    for i, field in enumerate(table.schema):
        if pa.types.is_timestamp(field.type) and field.type != _TARGET_TS_TYPE:
            new_columns.append((field.name, pc.cast(table.column(i), _TARGET_TS_TYPE)))
            changed = True
        else:
            new_columns.append((field.name, table.column(i)))
    if not changed:
        return table
    return pa.table(dict(new_columns))


def find_parquet_files(partition_dir: Path) -> list[Path]:
    """Return non-resource-fork parquet files in a partition directory."""
    return sorted(f for f in partition_dir.iterdir() if f.suffix == ".parquet" and not f.name.startswith("._"))


def compact_partition(partition_dir: Path, *, dry_run: bool = False) -> tuple[int, int, int]:
    """Compact a single dt= partition.

    Merging fragments and deleting the originals is not safe alongside another
    rewriter of the same partition (see scripts/flag_truncated_label_windows.py),
    so the write path holds the shared partition lock across the whole read,
    merge and delete. A dry run only reads metadata and takes nothing.

    Returns (files_before, files_after, rows).
    """
    if dry_run:
        return _compact_partition_unlocked(partition_dir, dry_run=True)
    with partition_lock(partition_dir, timeout=LOCK_TIMEOUT_SECONDS):
        return _compact_partition_unlocked(partition_dir, dry_run=False)


def _compact_partition_unlocked(partition_dir: Path, *, dry_run: bool = False) -> tuple[int, int, int]:
    parquet_files = find_parquet_files(partition_dir)
    files_before = len(parquet_files)

    if files_before <= 1:
        return files_before, files_before, 0

    if dry_run:
        total_rows = 0
        for f in parquet_files:
            try:
                meta = pq.read_metadata(f)
                total_rows += meta.num_rows
            except Exception:
                pass
        return files_before, 1, total_rows

    tables: list[pa.Table] = []
    for f in parquet_files:
        try:
            t = pq.read_table(f)
            tables.append(_unify_timestamp_resolution(t))
        except Exception as exc:
            log(f"  SKIP {f.name}: {exc}")

    if not tables:
        return files_before, files_before, 0

    merged = pa.concat_tables(tables, promote_extras=True)
    out_path = partition_dir / COMPACTED_FILENAME
    pq.write_table(merged, out_path, use_dictionary=True, compression="snappy")

    removed = 0
    for f in parquet_files:
        if f != out_path:
            f.unlink()
            removed += 1

    return files_before, 1, merged.num_rows


def compact_dataset(dataset_dir: Path, *, min_files: int = 2, dry_run: bool = False) -> dict:
    """Compact all partitions in a dataset directory tree.

    Walks recursively to find dt= partition directories at any nesting level.
    """
    stats = {
        "dataset": dataset_dir.name,
        "partitions_compacted": 0,
        "partitions_skipped": 0,
        "files_before": 0,
        "files_after": 0,
        "total_rows": 0,
    }

    # Find all dt= directories recursively
    dt_dirs: list[Path] = []
    for root, dirs, _files in os.walk(dataset_dir):
        for d in dirs:
            if d.startswith("dt="):
                dt_dirs.append(Path(root) / d)

    for dt_dir in sorted(dt_dirs):
        parquet_files = find_parquet_files(dt_dir)
        if len(parquet_files) < min_files:
            stats["partitions_skipped"] += 1
            stats["files_before"] += len(parquet_files)
            stats["files_after"] += len(parquet_files)
            continue

        label = f"{dataset_dir.name}/{dt_dir.name}"
        prefix = "[DRY-RUN] " if dry_run else ""
        log(f"{prefix}Compacting {label}: {len(parquet_files)} files...")

        before, after, rows = compact_partition(dt_dir, dry_run=dry_run)

        stats["partitions_compacted"] += 1
        stats["files_before"] += before
        stats["files_after"] += after
        stats["total_rows"] += rows

        log(f"  -> {rows} rows in {after} file (was {before} fragments)")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact Gold layer parquet partitions")
    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Compact only this dataset (e.g. labels_alert_barriers). Default: all datasets.",
    )
    parser.add_argument(
        "--min-files",
        type=int,
        default=2,
        help="Minimum number of files in a partition to trigger compaction (default: 2).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be compacted without actually writing files.",
    )
    args = parser.parse_args()

    if not GOLD_ROOT.exists():
        log(f"ERROR: Gold root not found: {GOLD_ROOT}")
        sys.exit(1)

    # Discover dataset directories
    if args.dataset:
        target = GOLD_ROOT / f"dataset={args.dataset}"
        if not target.exists():
            log(f"ERROR: Dataset not found: {target}")
            sys.exit(1)
        dataset_dirs = [target]
    else:
        dataset_dirs = sorted(d for d in GOLD_ROOT.iterdir() if d.is_dir() and d.name.startswith("dataset="))

    if not dataset_dirs:
        log("No datasets found in Gold layer.")
        sys.exit(0)

    t0 = time.monotonic()
    mode = "[DRY-RUN] " if args.dry_run else ""
    log(f"{mode}Gold compaction starting — {len(dataset_dirs)} dataset(s), min-files={args.min_files}")

    grand_total = {
        "partitions_compacted": 0,
        "partitions_skipped": 0,
        "files_before": 0,
        "files_after": 0,
    }

    for ds_dir in dataset_dirs:
        stats = compact_dataset(ds_dir, min_files=args.min_files, dry_run=args.dry_run)

        if stats["partitions_compacted"] > 0:
            log(
                f"  {stats['dataset']}: compacted {stats['partitions_compacted']} partitions, "
                f"{stats['files_before']} -> {stats['files_after']} files"
            )

        grand_total["partitions_compacted"] += stats["partitions_compacted"]
        grand_total["partitions_skipped"] += stats["partitions_skipped"]
        grand_total["files_before"] += stats["files_before"]
        grand_total["files_after"] += stats["files_after"]

    elapsed = time.monotonic() - t0
    log(
        f"{mode}Done in {elapsed:.1f}s — "
        f"compacted {grand_total['partitions_compacted']} partitions, "
        f"skipped {grand_total['partitions_skipped']}, "
        f"files {grand_total['files_before']} -> {grand_total['files_after']}"
    )


if __name__ == "__main__":
    main()
