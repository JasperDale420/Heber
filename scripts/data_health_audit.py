#!/usr/bin/env python3
"""Heber data health audit — reads 1 recent file per feed to check column quality.

Uses row-group-level reads to handle mixed string/dictionary column encodings
in compacted parquet files.

Outputs progress to stdout and writes full results to /Volumes/heber/data/audit_report.txt.

Usage:
    uv run python scripts/data_health_audit.py
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DATA_ROOT = Path("/Volumes/heber/data")
SILVER_ROOT = DATA_ROOT / "silver"
GOLD_ROOT = DATA_ROOT / "gold"
REPORT_PATH = DATA_ROOT / "audit_report.txt"

NUMERIC_TYPES = {
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
    pa.float16(),
    pa.float32(),
    pa.float64(),
}


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(REPORT_PATH, "a") as f:
        f.write(line + "\n")


def safe_read_parquet(fpath: Path) -> pa.Table | None:
    """Read a parquet file, handling mixed string/dictionary encodings.

    Compacted files may contain row groups where the same column uses
    different encodings (e.g., string vs dictionary<string>). PyArrow's
    read_table fails to merge these. We read row-group by row-group and
    cast dictionary columns to plain strings before concatenation.
    """
    try:
        return pq.read_table(fpath)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        pass  # Fall through to row-group approach

    try:
        pf = pq.ParquetFile(fpath)
        tables = []
        for i in range(pf.metadata.num_row_groups):
            rg = pf.read_row_group(i)
            # Cast any dictionary columns to plain string
            for col_idx, field in enumerate(rg.schema):
                if pa.types.is_dictionary(field.type):
                    rg = rg.set_column(
                        col_idx,
                        field.name,
                        rg.column(col_idx).cast(pa.string()),
                    )
            tables.append(rg)
        if not tables:
            return None
        return pa.concat_tables(tables, promote_options="default")
    except Exception as e:
        log(f"  ERROR: {e}")
        return None


def audit_one_file(feed_label: str, fpath: Path, total_files: int) -> None:
    """Read a single parquet file and report column health."""
    log(f"\n{'=' * 60}")
    log(f"  {feed_label}")
    log(f"  Total files in feed: {total_files}")
    log(f"  Sampling: {fpath.name}")
    log(f"{'=' * 60}")

    table = safe_read_parquet(fpath)
    if table is None:
        log("  ERROR: could not read file")
        return

    # Cast any remaining dictionary columns
    for i, field in enumerate(table.schema):
        if pa.types.is_dictionary(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))

    n = table.num_rows
    log(f"  Rows: {n:,}")
    log(f"  Columns: {len(table.column_names)}")

    critical = []
    healthy = []

    for col_name in table.column_names:
        col = table.column(col_name)
        dtype = str(table.schema.field(col_name).type)
        null_pct = (col.null_count / n * 100) if n > 0 else 0

        zero_pct = 0.0
        if col.type in NUMERIC_TYPES:
            non_null = pc.drop_null(col)
            if len(non_null) > 0:
                z = pc.sum(pc.equal(non_null, 0)).as_py()
                zero_pct = (z / len(non_null) * 100) if z else 0

        status = "OK"
        if null_pct >= 99.9:
            status = "ALL_NULL"
        elif null_pct >= 95:
            status = "MOSTLY_NULL"
        elif zero_pct >= 95 and zero_pct > 0:
            status = "ALL_ZERO"

        entry = f"    {col_name:30s} [{dtype:15s}] null={null_pct:6.1f}% zero={zero_pct:6.1f}%"
        if status != "OK":
            critical.append((entry, status))
        else:
            healthy.append(entry)

    if critical:
        log(f"\n  ⚠️  CRITICAL ({len(critical)}):")
        for entry, status in critical:
            log(f"{entry} -> {status}")
    else:
        log(f"\n  ✅ ALL {len(table.column_names)} COLUMNS HEALTHY")

    if healthy and critical:
        log(f"\n  Healthy ({len(healthy)}):")
        for entry in healthy:
            log(entry)


def get_parquet_files(directory: Path) -> list[Path]:
    """List parquet files, excluding macOS resource forks."""
    return sorted(f for f in directory.rglob("*.parquet") if not f.name.startswith("._"))


def main() -> None:
    start = time.time()
    REPORT_PATH.write_text("")

    log("=" * 60)
    log("  HEBER DATA HEALTH AUDIT")
    log(f"  Started: {datetime.now().isoformat()}")
    log(f"  Data root: {DATA_ROOT}")
    log("=" * 60)

    # Silver feeds
    silver_feeds = sorted(d for d in SILVER_ROOT.iterdir() if d.is_dir() and d.name.startswith("feed="))
    log(f"\n📂 Silver layer: {len(silver_feeds)} feeds")

    for feed_dir in silver_feeds:
        feed_name = feed_dir.name.replace("feed=", "")
        files = get_parquet_files(feed_dir)
        if not files:
            log(f"\n  {feed_name}: NO PARQUET FILES")
            continue
        target = files[-1]
        audit_one_file(f"silver/{feed_name}", target, len(files))

    # Gold datasets
    log("\n\n📂 Gold layer")
    for top in sorted(GOLD_ROOT.iterdir()):
        if not top.is_dir():
            continue
        for sub in sorted(top.iterdir()):
            if not sub.is_dir():
                continue
            ds_name = sub.name.replace("dataset=", "")
            files = get_parquet_files(sub)
            if not files:
                log(f"\n  gold/{top.name}/{ds_name}: NO PARQUET FILES")
                continue
            target = files[-1]
            audit_one_file(f"gold/{top.name}/{ds_name}", target, len(files))

    elapsed = time.time() - start
    log(f"\n\n{'=' * 60}")
    log(f"  AUDIT COMPLETE in {elapsed:.0f}s")
    log(f"  Report saved to: {REPORT_PATH}")
    log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
