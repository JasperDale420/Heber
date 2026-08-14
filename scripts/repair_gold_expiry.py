#!/usr/bin/env python
"""Repair out-of-range ``expiry`` values in Gold ``meta_label_features`` partitions.

A pre-2026-06-30 backfill path wrote ``expiry`` as a raw ``YYYYMMDD`` integer into
a ``date32`` column. pyarrow reads such a value as a day count, so ``20260819``
becomes year 57442 and pandas raises ``year must be in 1..9999`` for the whole
partition — every row in it disappears from model training.

Each bad value is repaired only when re-reading it as ``YYYYMMDD`` agrees with the
expiry embedded in the row's OCC symbol. If any row in a partition fails that
cross-check the partition is left untouched, because a partial repair is worse
than a known-bad file.

Dry run by default:

    uv run python scripts/repair_gold_expiry.py
    uv run python scripts/repair_gold_expiry.py --write --dates 2026-04-14 2026-05-06
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from heber.config import settings

# date32 day counts outside this window are not plausible option expiries and
# indicate the YYYYMMDD-as-day-count bug rather than genuine data.
MIN_PLAUSIBLE_DAY = (date(1990, 1, 1) - date(1970, 1, 1)).days
MAX_PLAUSIBLE_DAY = (date(2100, 1, 1) - date(1970, 1, 1)).days

# OCC symbol: <root><YYMMDD><C|P><8-digit strike>, root is variable length.
OCC_RE = re.compile(r"^(?P<root>[A-Z0-9.]+?)(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def occ_expiry(instrument_key: object, occ_symbol: object) -> date | None:
    """Extract the expiry encoded in a row's OCC symbol, if one is present."""
    for raw in (occ_symbol, instrument_key):
        if not isinstance(raw, str) or not raw:
            continue
        symbol = raw.split(":")[-1].strip().upper()
        match = OCC_RE.match(symbol)
        if not match:
            continue
        try:
            return datetime.strptime(match.group("yymmdd"), "%y%m%d").date()
        except ValueError:
            continue
    return None


def find_bad_rows(table: pa.Table) -> list[int] | None:
    """Return indices of out-of-range date32 expiry values, or None if N/A."""
    if "expiry" not in table.column_names:
        return None
    if not pa.types.is_date32(table.schema.field("expiry").type):
        return None
    days = table.column("expiry").cast(pa.int32()).to_pylist()
    return [i for i, v in enumerate(days) if v is not None and not (MIN_PLAUSIBLE_DAY <= v <= MAX_PLAUSIBLE_DAY)]


def plan_repair(table: pa.Table, indices: list[int]) -> tuple[dict[int, date], list[str]]:
    """Map row index -> verified expiry. Any unverifiable row becomes a problem."""
    days = table.column("expiry").cast(pa.int32()).to_pylist()
    keys = table.column("instrument_key").to_pylist() if "instrument_key" in table.column_names else [None] * len(days)
    occs = table.column("occ_symbol").to_pylist() if "occ_symbol" in table.column_names else [None] * len(days)

    repairs: dict[int, date] = {}
    problems: list[str] = []
    for i in indices:
        raw = days[i]
        try:
            candidate = datetime.strptime(str(raw), "%Y%m%d").date()
        except ValueError:
            problems.append(f"row {i}: raw value {raw} is not a YYYYMMDD date")
            continue

        from_occ = occ_expiry(keys[i], occs[i])
        if from_occ is None:
            problems.append(f"row {i}: raw {raw} -> {candidate}, but no OCC symbol to verify against")
        elif from_occ != candidate:
            problems.append(f"row {i}: raw {raw} -> {candidate}, but OCC symbol says {from_occ}")
        else:
            repairs[i] = candidate
    return repairs, problems


def apply_repair(table: pa.Table, repairs: dict[int, date]) -> pa.Table:
    """Return a copy of the table with repaired expiry values, pinned to date32."""
    days = table.column("expiry").cast(pa.int32()).to_pylist()
    values: list[date | None] = []
    for i, raw in enumerate(days):
        if i in repairs:
            values.append(repairs[i])
        elif raw is None:
            values.append(None)
        else:
            values.append(date(1970, 1, 1) + pd.Timedelta(days=raw).to_pytimedelta())
    index = table.schema.get_field_index("expiry")
    return table.set_column(index, pa.field("expiry", pa.date32()), pa.array(values, type=pa.date32()))


def verify(original: pa.Table, repaired: pa.Table, out_file: Path) -> list[str]:
    """Confirm the rewritten file preserved everything except the repaired values."""
    problems: list[str] = []
    if original.num_rows != repaired.num_rows:
        problems.append(f"row count changed: {original.num_rows} -> {repaired.num_rows}")

    for name in original.column_names:
        if name == "expiry":
            continue
        if name not in repaired.column_names:
            problems.append(f"column {name} disappeared")
        elif original.column(name) != repaired.column(name):
            problems.append(f"column {name} changed")

    published = pq.ParquetFile(out_file)
    if not pa.types.is_date32(published.schema_arrow.field("expiry").type):
        problems.append("published expiry column is not date32")
    if published.metadata.num_rows != original.num_rows:
        problems.append(f"published row count {published.metadata.num_rows} != {original.num_rows}")

    try:
        frame = pd.read_parquet(out_file)
    except Exception as exc:  # the exact failure this script exists to remove
        problems.append(f"pandas still cannot read the file: {exc}")
        return problems

    if len(frame) != original.num_rows:
        problems.append(f"pandas read {len(frame)} rows, expected {original.num_rows}")
    if "alert_id" in original.column_names:
        before = set(original.column("alert_id").to_pylist())
        if set(frame["alert_id"]) != before:
            problems.append("alert_id set changed")
    return problems


def _fsync_dir(directory: Path) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def stage_repaired(table: pa.Table, out_file: Path) -> Path:
    """Write the replacement to a staging file and flush it to disk.

    /Volumes/heber is exFAT with no journaling, so a rename without an explicit
    flush can publish a zero-byte file. Staging separately from publishing also
    lets the caller validate the replacement before it becomes the live file.
    """
    temp_path = out_file.with_name(f".{out_file.name}.repair-{os.getpid()}")
    pq.write_table(table, temp_path, compression="snappy")
    with temp_path.open("rb+") as handle:
        os.fsync(handle.fileno())
    return temp_path


def publish(temp_path: Path, out_file: Path) -> None:
    os.replace(temp_path, out_file)
    _fsync_dir(out_file.parent)
    # Writing on exFAT leaves an AppleDouble xattr sidecar next to the file.
    # It holds no lake data and pyarrow's directory walk crashes on it, so the
    # repair must not leave new ones behind.
    out_file.with_name(f"._{out_file.name}").unlink(missing_ok=True)


def restore(backup_path: Path, out_file: Path) -> None:
    """Put the verified backup back in place after a failed repair."""
    temp_path = out_file.with_name(f".{out_file.name}.restore-{os.getpid()}")
    shutil.copy2(backup_path, temp_path)
    with temp_path.open("rb+") as handle:
        os.fsync(handle.fileno())
    os.replace(temp_path, out_file)
    _fsync_dir(out_file.parent)


def _repair_locked(path: Path, result: dict, *, backup_dir: Path | None) -> dict:
    """Read, plan, back up, publish and verify — all while holding the lock.

    Everything happens inside the lock because the live writer appends to these
    same files: a table read before the lock is taken can be stale by the time
    it is published, which would silently drop a concurrently written alert.
    """
    partition = path.parent.name
    table = pq.ParquetFile(path).read()
    indices = find_bad_rows(table)
    if indices is None:
        result["status"] = "skipped_not_date32"
        return result
    if not indices:
        return result

    result["bad_rows"] = len(indices)
    repairs, problems = plan_repair(table, indices)
    result["repairs"] = {str(i): str(v) for i, v in repairs.items()}

    if problems:
        result["status"] = "unverified"
        result["problems"] = problems
        return result

    backup_path: Path | None = None
    if backup_dir is not None:
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"{partition}-{path.name}"
        shutil.copy2(path, backup_path)
        if sha256(backup_path) != sha256(path):
            result["status"] = "backup_failed"
            result["problems"] = ["backup checksum did not match source"]
            return result
        result["backup"] = str(backup_path)
        result["source_sha256"] = sha256(path)

    temp_path = stage_repaired(apply_repair(table, repairs), path)
    try:
        staged_problems = verify(table, pq.ParquetFile(temp_path).read(), temp_path)
        if staged_problems:
            result["status"] = "staging_failed"
            result["problems"] = staged_problems
            return result
        publish(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)

    verify_problems = verify(table, pq.ParquetFile(path).read(), path)
    if verify_problems:
        result["status"] = "verify_failed"
        result["problems"] = verify_problems
        if backup_path is not None:
            restore(backup_path, path)
            result["problems"].append("original restored from backup")
        else:
            result["problems"].append("NO BACKUP TAKEN — the bad file is still live")
        return result

    result["status"] = "repaired"
    result["output_sha256"] = sha256(path)
    return result


def repair_partition(path: Path, *, write: bool, backup_dir: Path | None, lock_timeout: float = 30) -> dict:
    """Inspect one partition file; repair it when --write and all rows verify."""
    result: dict = {"partition": path.parent.name, "path": str(path), "status": "ok", "bad_rows": 0}

    if not write:
        table = pq.ParquetFile(path).read()
        indices = find_bad_rows(table)
        if indices is None:
            result["status"] = "skipped_not_date32"
            return result
        if not indices:
            return result
        result["bad_rows"] = len(indices)
        repairs, problems = plan_repair(table, indices)
        result["repairs"] = {str(i): str(v) for i, v in repairs.items()}
        result["status"] = "unverified" if problems else "would_repair"
        if problems:
            result["problems"] = problems
        return result

    from filelock import FileLock, Timeout

    try:
        with FileLock(str(path.with_suffix(".parquet.lock")), timeout=lock_timeout):
            return _repair_locked(path, result, backup_dir=backup_dir)
    except Timeout:
        result["status"] = "locked"
        result["problems"] = ["could not acquire the partition write lock"]
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dates", nargs="*", help="dt= partitions to repair (default: scan all)")
    parser.add_argument("--write", action="store_true", help="apply repairs (default: dry run)")
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=Path.home() / "heber-repair-backups",
        help="where originals are copied before mutation (keep this off the lakehouse volume)",
    )
    args = parser.parse_args()

    if not args.dataset_root.exists():
        print(f"dataset root does not exist: {args.dataset_root}", file=sys.stderr)
        return 2

    if args.dates:
        globbed = [p for d in args.dates for p in sorted((args.dataset_root / f"dt={d}").glob("*.parquet"))]
    else:
        globbed = sorted(args.dataset_root.glob("dt=*/*.parquet"))
    # AppleDouble sidecars are not parquet files; pyarrow chokes on them.
    files = [p for p in globbed if not p.name.startswith("._")]

    if not files:
        print("no partition files matched", file=sys.stderr)
        return 2

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = (args.backup_dir / run_id) if args.write else None

    results = [repair_partition(p, write=args.write, backup_dir=backup_dir) for p in files]
    interesting = [r for r in results if r["bad_rows"] or r["status"] not in {"ok", "skipped_not_date32"}]

    for r in interesting:
        print(f"[{r['status']}] {r['partition']}  bad_rows={r['bad_rows']}")
        for key, value in r.get("repairs", {}).items():
            print(f"    row {key} -> {value}")
        for problem in r.get("problems", []):
            print(f"    ! {problem}")

    print(f"\nscanned {len(files)} files, {len(interesting)} needed attention")

    failed = [
        r
        for r in results
        if r["status"] in {"unverified", "staging_failed", "verify_failed", "backup_failed", "locked"}
    ]
    if args.write and backup_dir is not None and interesting:
        manifest = backup_dir / "manifest.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps({"run_id": run_id, "results": interesting}, indent=2))
        print(f"manifest: {manifest}")
    if not args.write and any(r["status"] == "would_repair" for r in results):
        print("dry run — re-run with --write to apply")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
