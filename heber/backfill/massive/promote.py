"""Promote backfill temp partitions to canonical, idempotently (DuckDB streaming dedup by event_id)."""

import os
import uuid
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


def promote_date(storage_root: str, dataset_dir: str, dt_iso: str) -> int:
    part = Path(storage_root) / "silver" / dataset_dir / f"dt={dt_iso}"
    temp_files = list(part.glob("_backfill_*/*.parquet"))
    canonical = part / "part-00000.parquet"
    if not temp_files:
        return pq.read_metadata(canonical).num_rows if canonical.exists() else 0
    lock = part / ".promote.lock"
    try:
        lock.touch(exist_ok=False)
    except FileExistsError as e:
        raise RuntimeError(f"promotion already in progress for {dt_iso}") from e
    try:
        staging = part / f".staging_{uuid.uuid4().hex}.parquet"
        sources = [str(f) for f in temp_files] + ([str(canonical)] if canonical.exists() else [])
        con = duckdb.connect()
        try:
            con.execute(
                "COPY (SELECT * EXCLUDE(rn) FROM (SELECT *, row_number() OVER "
                "(PARTITION BY event_id ORDER BY ts_ingest DESC) AS rn "
                "FROM read_parquet($files)) WHERE rn = 1) TO $out (FORMAT PARQUET)",
                {"files": sources, "out": str(staging)},
            )
        finally:
            con.close()
        os.replace(staging, canonical)  # atomic commit FIRST
        for d in {f.parent for f in temp_files}:  # cleanup LAST
            for f in d.glob("*.parquet"):
                f.unlink(missing_ok=True)
            d.rmdir()
        return pq.read_metadata(canonical).num_rows
    finally:
        lock.unlink(missing_ok=True)
