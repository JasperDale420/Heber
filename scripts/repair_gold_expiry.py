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
import json
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from heber.config import settings
from heber.ml.gold_rewrite import rewrite_partition

# date32 day counts outside this window are not plausible option expiries and
# indicate the YYYYMMDD-as-day-count bug rather than genuine data.
MIN_PLAUSIBLE_DAY = (date(1990, 1, 1) - date(1970, 1, 1)).days
MAX_PLAUSIBLE_DAY = (date(2100, 1, 1) - date(1970, 1, 1)).days

# OCC symbol: <root><YYMMDD><C|P><8-digit strike>, root is variable length.
OCC_RE = re.compile(r"^(?P<root>[A-Z0-9.]+?)(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")


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


class UnverifiedRepair(Exception):
    """A bad value could not be confirmed against its row's OCC symbol."""


def verify(original: pa.Table, repaired: pa.Table) -> list[str]:
    """Confirm the rewrite preserved everything except the repaired values."""
    problems: list[str] = []
    if original.num_rows != repaired.num_rows:
        problems.append(f"row count changed: {original.num_rows} -> {repaired.num_rows}")
        return problems
    if not pa.types.is_date32(repaired.schema.field("expiry").type):
        problems.append("repaired expiry column is not date32")

    for name in original.column_names:
        if name == "expiry":
            continue
        if name not in repaired.column_names:
            problems.append(f"column {name} disappeared")
        elif original.column(name) != repaired.column(name):
            problems.append(f"column {name} changed")

    bad = find_bad_rows(repaired)
    if bad:
        problems.append(f"{len(bad)} out-of-range expiry values remain")

    # The failure this script exists to remove is a pandas one: pyarrow reads an
    # out-of-range date32 happily and only the conversion to pandas raises. So
    # the repair is not proven until the consumer's own conversion succeeds.
    try:
        frame = repaired.to_pandas()
    except Exception as exc:
        problems.append(f"pandas still cannot read the repaired table: {exc}")
        return problems

    if len(frame) != original.num_rows:
        problems.append(f"pandas read {len(frame)} rows, expected {original.num_rows}")
    if "alert_id" in original.column_names:
        if set(frame["alert_id"]) != set(original.column("alert_id").to_pylist()):
            problems.append("alert_id set changed")
    return problems


def repair_partition(path: Path, *, write: bool, backup_dir: Path | None, lock_timeout: float = 30) -> dict:
    """Inspect one partition file; repair it when --write and all rows verify."""
    result: dict = {"partition": path.parent.name, "path": str(path), "status": "ok", "bad_rows": 0}
    seen: dict = {}

    def plan(table: pa.Table) -> pa.Table | None:
        indices = find_bad_rows(table)
        if indices is None:
            seen["status"] = "skipped_not_date32"
            return None
        if not indices:
            return None
        seen["bad_rows"] = len(indices)
        repairs, problems = plan_repair(table, indices)
        seen["repairs"] = {str(i): str(v) for i, v in repairs.items()}
        if problems:
            seen["problems"] = problems
            raise UnverifiedRepair("; ".join(problems))
        return apply_repair(table, repairs)

    if not write:
        table = pq.ParquetFile(path).read()
        try:
            plan(table)
        except UnverifiedRepair:
            pass
        result.update({k: v for k, v in seen.items() if k != "status"})
        if seen.get("status") == "skipped_not_date32":
            result["status"] = "skipped_not_date32"
        elif "problems" in seen:
            result["status"] = "unverified"
        elif seen.get("bad_rows"):
            result["status"] = "would_repair"
        return result

    outcome = rewrite_partition(
        path,
        plan=plan,
        validate=verify,
        backup_dir=backup_dir,
        lock_timeout=lock_timeout,
    )
    result.update({k: v for k, v in seen.items() if k != "status"})
    result.update({k: v for k, v in outcome.items() if k != "partition"})
    if outcome["status"] == "plan_failed" and "problems" in seen:
        result["status"] = "unverified"
        result["problems"] = seen["problems"]
    elif outcome["status"] == "skipped":
        result["status"] = seen.get("status", "ok")
    elif outcome["status"] == "rewritten":
        result["status"] = "repaired"
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

    # Allowlist the good outcomes: anything else — including restore_failed,
    # where the live file may be an unverified replacement — must exit non-zero
    # so a scheduled run cannot report success over unrecovered training data.
    ok_statuses = {"ok", "repaired", "would_repair", "skipped", "skipped_not_date32"}
    failed = [r for r in results if r["status"] not in ok_statuses]
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
