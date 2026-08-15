#!/usr/bin/env python
"""Normalize Gold ``meta_label_features`` partitions to the declared column types.

Partitions were written with pandas-inferred types, which drift: a column that
was all-null in one day's rows became Arrow ``null``, ``expiry`` was variously
``date32``/``int64``/``string``, and ``alert_time`` was written at two different
timestamp resolutions. pyarrow cannot unify those across partitions, so a
whole-dataset read fails — and ``MetaLabelDatasetBuilder`` swallowed the error
and returned an empty DataFrame, meaning training silently saw no rows at all.

This rewrites each partition so every declared column carries its type from
``heber.schemas.gold.META_LABEL_FEATURES_TYPES``. Undeclared columns are left
exactly as they are, and no rows are added or removed.

Dry run by default:

    uv run python scripts/normalize_gold_schema.py
    uv run python scripts/normalize_gold_schema.py --write
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from heber.config import settings
from heber.ml.datasets import normalize_expiry
from heber.ml.gold_rewrite import rewrite_partition
from heber.schemas.gold import META_LABEL_FEATURES_TYPES


def convert_column(column: pa.ChunkedArray, target: pa.DataType) -> pa.ChunkedArray:
    """Convert one column to its declared type without inventing values."""
    if column.type == target:
        return column
    # An all-null column carries no values, only a placeholder type.
    if pa.types.is_null(column.type):
        return pa.chunked_array([pa.nulls(len(column), type=target)])
    # expiry needs parsing rather than casting: it exists on disk as YYYYMMDD
    # integers and as date strings, neither of which casts to date32.
    if pa.types.is_date32(target):
        return pa.chunked_array([pa.array([normalize_expiry(v) for v in column.to_pylist()], type=target)])
    return column.cast(target)


def plan_normalization(table: pa.Table) -> pa.Table | None:
    """Return the retyped table, or None when the partition is already correct."""
    changed = False
    columns = []
    for name in table.column_names:
        column = table.column(name)
        target = META_LABEL_FEATURES_TYPES.get(name)
        if target is None or column.type == target:
            columns.append(column)
            continue
        columns.append(convert_column(column, target))
        changed = True
    if not changed:
        return None
    fields = [pa.field(n, c.type) for n, c in zip(table.column_names, columns, strict=True)]
    return pa.Table.from_arrays([c.combine_chunks() for c in columns], schema=pa.schema(fields))


def _values_equal(before: pa.ChunkedArray, after: pa.ChunkedArray) -> bool:
    """Compare values across a type change, tolerating only the intended ones."""
    if before.null_count != after.null_count:
        return False
    if pa.types.is_null(before.type):
        return after.null_count == len(after)
    if before.type == after.type:
        return before.equals(after)
    if pa.types.is_date32(after.type):
        # expiry: the parsed date must round-trip to the original representation.
        return [normalize_expiry(v) for v in before.to_pylist()] == after.to_pylist()
    if pa.types.is_timestamp(before.type) and pa.types.is_timestamp(after.type):
        return pc.all(pc.equal(before.cast(after.type), after)).as_py() is not False
    return before.cast(after.type).equals(after)


def validate(original: pa.Table, candidate: pa.Table) -> list[str]:
    """Reject any rewrite that changed the data rather than only its typing."""
    problems: list[str] = []
    if original.num_rows != candidate.num_rows:
        problems.append(f"row count changed: {original.num_rows} -> {candidate.num_rows}")
        return problems
    if original.column_names != candidate.column_names:
        problems.append("column set changed")
        return problems

    for name in original.column_names:
        target = META_LABEL_FEATURES_TYPES.get(name)
        if target is not None and candidate.column(name).type != target:
            problems.append(f"{name}: expected {target}, got {candidate.column(name).type}")
        if not _values_equal(original.column(name), candidate.column(name)):
            problems.append(f"{name}: values changed")
    return problems


def whole_root_status(tables: list[pa.Table]) -> str:
    """Report whether the partitions combine, which is what training needs."""
    try:
        unified = pa.concat_tables(tables, promote_options="permissive")
    except Exception as exc:
        return f"WHOLE-DATASET READ FAILS: {exc.__class__.__name__}: {exc}"
    return f"whole-dataset read OK: {unified.num_rows} rows, {unified.num_columns} columns"


def describe(path: Path) -> dict:
    """Report which declared columns are mistyped in a partition."""
    schema = pq.ParquetFile(path).schema_arrow
    drift = {
        field.name: (str(field.type), str(META_LABEL_FEATURES_TYPES[field.name]))
        for field in schema
        if field.name in META_LABEL_FEATURES_TYPES and field.type != META_LABEL_FEATURES_TYPES[field.name]
    }
    return {"partition": path.parent.name, "path": str(path), "drift": drift}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dates", nargs="*", help="dt= partitions to normalize (default: all)")
    parser.add_argument("--write", action="store_true", help="apply changes (default: dry run)")
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

    if not args.write:
        drifting = 0
        problems = 0
        converted: list[pa.Table] = []
        for path in files:
            entry = describe(path)
            original = pq.ParquetFile(path).read()
            candidate = plan_normalization(original)
            if candidate is None:
                converted.append(original)
                continue
            drifting += 1
            summary = ", ".join(f"{c} {a}->{b}" for c, (a, b) in sorted(entry["drift"].items()))
            found = validate(original, candidate)
            if found:
                problems += 1
                print(f"[PROBLEM] {entry['partition']}: {summary}")
                for problem in found:
                    print(f"    ! {problem}")
            else:
                print(f"[would_normalize] {entry['partition']}: {summary}")
            converted.append(candidate)

        print(f"\nscanned {len(files)} files, {drifting} need normalizing, {problems} would fail validation")
        print(whole_root_status(converted))
        if problems:
            return 1
        if drifting:
            print("dry run — re-run with --write to apply")
        return 0

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = args.backup_dir / f"schema-{run_id}"

    results = []
    for path in files:
        result = rewrite_partition(
            path,
            plan=plan_normalization,
            validate=validate,
            backup_dir=backup_dir,
        )
        results.append(result)
        if result["status"] not in {"ok", "skipped"}:
            print(f"[{result['status']}] {result['partition']}")
            for problem in result.get("problems", []):
                print(f"    ! {problem}")

    rewritten = [r for r in results if r["status"] == "rewritten"]
    # Anything outside the good outcomes — restore_failed especially — must exit
    # non-zero rather than let automation treat it as a completed migration.
    failed = [r for r in results if r["status"] not in {"ok", "skipped", "rewritten"}]
    print(f"\nscanned {len(files)} files, rewrote {len(rewritten)}, failed {len(failed)}")
    print(whole_root_status([pq.ParquetFile(p).read() for p in files]))

    if rewritten or failed:
        backup_dir.mkdir(parents=True, exist_ok=True)
        manifest = backup_dir / "manifest.json"
        manifest.write_text(json.dumps({"run_id": run_id, "results": rewritten + failed}, indent=2))
        print(f"manifest: {manifest}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
