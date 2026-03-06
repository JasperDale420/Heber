#!/usr/bin/env python3
"""One-off script to completely deduplicate existing Heber Parquet datasets.

Iterates over every Parquet file in the `data/silver` (and optionally `data/gold`)
directories. It loads the files, checks for `event_id` duplicates (keeping the earliest
`ts_ingest`), and rewrites the file if duplicates are found.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings

logger = structlog.get_logger("heber-dedupe-cleanup")


def parse_args():
    parser = argparse.ArgumentParser(description="Force deduplicate Heber Parquet files")
    parser.add_argument(
        "--layer",
        type=str,
        choices=["silver", "gold"],
        default="silver",
        help="Storage layer to clean up (default: silver)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report duplicates without modifying files")
    return parser.parse_args()


def process_file(file_path: Path, dry_run: bool) -> tuple[int, int]:
    """Process a single file, dropping duplicates if necessary.

    Returns:
        (rows_processed, duplicates_removed)
    """
    if file_path.name.startswith("."):
        # Skip sidecar/hidden files
        return 0, 0

    try:
        df = pd.read_parquet(file_path, engine="pyarrow")
    except Exception as e:
        logger.error("failed_to_read_parquet", file=str(file_path), error=str(e))
        return 0, 0

    initial_rows = len(df)
    if initial_rows <= 1:
        return initial_rows, 0

    if "event_id" not in df.columns:
        return initial_rows, 0

    has_ts_ingest = "ts_ingest" in df.columns

    if has_ts_ingest:
        df = df.sort_values("ts_ingest", ascending=True)

    df = df.drop_duplicates(subset=["event_id"], keep="first")

    if has_ts_ingest:
        df = df.sort_index()

    deduped_rows = len(df)
    duplicates_removed = initial_rows - deduped_rows

    if duplicates_removed > 0:
        logger.info("duplicates_found", file=file_path.name, removed=duplicates_removed, remaining=deduped_rows)

        if not dry_run:
            # Overwrite the file.
            # We write to a temporary file in the same directory, then atomic rename
            # to prevent corruption if interrupted.
            temp_path = file_path.with_name(f".{file_path.name}.tmp")
            final_table = pa.Table.from_pandas(df, preserve_index=False)

            try:
                with pq.ParquetWriter(temp_path, final_table.schema, compression="snappy") as writer:
                    writer.write_table(final_table, row_group_size=250_000)
                temp_path.replace(file_path)
            except Exception as e:
                logger.error("failed_to_write_deduped", file=str(file_path), error=str(e))
                if temp_path.exists():
                    temp_path.unlink()

    return initial_rows, duplicates_removed


def main():
    args = parse_args()

    layer_dir = settings.data_root / args.layer
    if not layer_dir.exists():
        logger.error(f"Directory {layer_dir} does not exist.")
        sys.exit(1)

    logger.info(f"Starting deduplication scan on {layer_dir} (dry_run={args.dry_run})")

    total_files = 0
    total_rows = 0
    total_duplicates_removed = 0

    # Iterate through all parquet files recursively
    for parquet_file in layer_dir.rglob("*.parquet"):
        total_files += 1
        rows, removed = process_file(parquet_file, args.dry_run)
        total_rows += rows
        total_duplicates_removed += removed

    logger.info(
        "deduplication_scan_complete",
        layer=args.layer,
        files_scanned=total_files,
        rows_scanned=total_rows,
        duplicates_removed=total_duplicates_removed,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
