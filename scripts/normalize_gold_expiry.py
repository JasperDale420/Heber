#!/usr/bin/env python3
"""Rewrite Gold ``expiry`` columns to a single Arrow ``date32`` type.

Producers used to write ``expiry`` as whatever they held — a ``date``, an ISO
string, or a ``YYYYMMDD`` int — so the same column exists under three Arrow
types across ``dt=`` partitions. A read of the whole dataset cannot merge those
and raises ``SchemaContractError``. The writers now funnel through one coercion;
this backfills the partitions written before that.

This rewrites live files, so:

  * Only the ``expiry`` field is replaced. Every other column keeps its exact
    Arrow type and values — the table is edited in Arrow, never round-tripped
    through pandas, which silently renarrows large_string and dictionary types.
  * The original is kept beside the rewritten file as ``<name>.pre-expiry-migration``
    (invisible to readers, which only glob ``*.parquet``). Rolling back is a
    rename. Delete them once you are satisfied.
  * Each partition is taken under the same ``data.parquet.lock`` the live watch
    writer uses, so a concurrent append cannot be overwritten. Still, prefer to
    stop ``heber-watch`` / ``heber-backfill-consumer`` for the run.
  * A file that cannot be read aborts the whole run rather than being skipped.
  * Files already ``date32`` are left alone, so a re-run is a no-op and an
    interrupted run resumes where it stopped.

Values that are not a recognisable date become null — they were never readable
as dates. Numbers that are not exactly a ``YYYYMMDD`` integer are treated the
same way rather than truncated into a plausible-looking date. The report counts
both per file, so the loss is explicit before you apply it.

Usage:
    .venv/bin/python scripts/normalize_gold_expiry.py            # dry-run report
    .venv/bin/python scripts/normalize_gold_expiry.py --apply    # rewrite
"""

import argparse
import os
import shutil
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from filelock import FileLock

from heber.ml.datasets import coerce_expiry_to_date
from heber.writer.utils import fsync_directory, publish_file_atomically

logger = structlog.get_logger("heber-normalize-expiry")

BACKUP_SUFFIX = ".pre-expiry-migration"
DEFAULT_LOCK_TIMEOUT = 30.0


def resolve_dataset_root(root: Path, expected: Path) -> Path:
    """Resolve the target root, refusing anything but the dataset being migrated."""
    resolved = root.resolve()
    if resolved != expected.resolve():
        raise ValueError(f"refusing {resolved}: expected {expected.resolve()}")
    return resolved


def _rewrite_expiry(path: Path, table: pa.Table, values: list) -> None:
    """Replace only the ``expiry`` column, keeping the original as a backup.

    The original is *copied* rather than moved aside: ``path`` stays present
    and readable for the whole operation, so a crash at any point leaves a
    partition that still reads (either the old file or the new one), never a
    hole. An existing backup means a previous run stopped mid-flight — refuse
    rather than overwrite the only pristine copy.
    """
    backup_path = path.with_name(path.name + BACKUP_SUFFIX)
    if backup_path.exists():
        raise RuntimeError(f"backup already exists, resolve it before re-running: {backup_path}")

    idx = table.schema.get_field_index("expiry")
    coerced = pa.array([coerce_expiry_to_date(v) for v in values], type=pa.date32())
    new_table = table.set_column(idx, pa.field("expiry", pa.date32()), coerced)

    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        pq.write_table(new_table, temp_path)
        # Read the replacement back before it can displace anything.
        check = pq.read_schema(temp_path)
        if check.field("expiry").type != pa.date32() or pq.read_metadata(temp_path).num_rows != table.num_rows:
            raise RuntimeError(f"rewritten file failed verification: {temp_path}")
        shutil.copy2(path, backup_path)
        # The backup becomes the only surviving copy of the original once the
        # publish below replaces it — flush its bytes and directory entry
        # durable first, or a crash right after the replace can leave the
        # replace done and the backup missing or truncated, with no way to
        # recover the pre-migration file. copy2 does not fsync on its own.
        with open(backup_path, "rb") as backup_fd:
            os.fsync(backup_fd.fileno())
        fsync_directory(path.parent)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    publish_file_atomically(temp_path, path)
    # Flush the replacement's own directory entry too, so the rename itself
    # cannot be lost on this non-journaling volume once success is reported.
    fsync_directory(path.parent)


def _target_files(root: Path, include_quarantine: bool) -> list[Path]:
    """Parquet files to consider — canonical ``dt=`` partitions unless asked otherwise.

    ``HeberReader`` excludes the ``_quarantine`` subtree, so its files cannot
    take part in a read and rewriting them is dead work (this dataset holds
    ~10k of them against 120 canonical partitions).
    """
    files = [f for f in sorted(root.rglob("*.parquet")) if not f.name.startswith(".")]
    if include_quarantine:
        return files
    return [f for f in files if "_quarantine" not in f.parts]


def normalize_expiry(
    root: Path,
    apply: bool,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
    include_quarantine: bool = False,
) -> list[dict]:
    """Report (and, when ``apply``, rewrite) parquet files with a non-date32 expiry.

    Returns one record per file: ``{path, from_type, rows, unparseable, action}``
    where ``action`` is ``rewritten`` / ``would-rewrite`` / ``ok`` /
    ``no-expiry-column``.

    Raises ``RuntimeError`` on an unreadable file and ``filelock.Timeout`` when
    a partition is held by the live writer — a partial dataset is a worse
    outcome than a stopped migration.
    """
    report: list[dict] = []
    for path in _target_files(root, include_quarantine):
        try:
            schema = pq.read_schema(path)
        except Exception as exc:
            raise RuntimeError(f"unreadable parquet file, aborting migration: {path}") from exc

        if "expiry" not in schema.names:
            report.append(
                {"path": str(path), "from_type": None, "rows": 0, "unparseable": 0, "action": "no-expiry-column"}
            )
            continue

        from_type = schema.field("expiry").type
        if from_type == pa.date32():
            report.append({"path": str(path), "from_type": str(from_type), "rows": 0, "unparseable": 0, "action": "ok"})
            continue

        lock = FileLock(str(path.with_suffix(".parquet.lock")), timeout=lock_timeout)
        with lock:
            table = pq.read_table(path)
            values = table.column("expiry").to_pylist()
            unparseable = sum(
                1 for raw in values if raw is not None and not pd.isna(raw) and coerce_expiry_to_date(raw) is None
            )

            if apply:
                _rewrite_expiry(path, table, values)
                action = "rewritten"
            else:
                action = "would-rewrite"

        logger.info(
            "normalize_expiry_partition",
            path=str(path),
            from_type=str(from_type),
            rows=table.num_rows,
            unparseable=unparseable,
            action=action,
        )
        report.append(
            {
                "path": str(path),
                "from_type": str(from_type),
                "rows": table.num_rows,
                "unparseable": unparseable,
                "action": action,
            }
        )
    return report


def main() -> None:
    from heber.config import settings

    expected_root = Path(settings.gold_path) / "dataset=meta_label_features" / "project=watch" / "version=v1"

    ap = argparse.ArgumentParser(description="Normalize the Gold meta_label_features expiry column to date32.")
    ap.add_argument("--root", type=Path, default=expected_root, help="must be the meta_label_features version root")
    ap.add_argument("--apply", action="store_true", help="actually rewrite (default: dry-run report only)")
    ap.add_argument(
        "--include-quarantine",
        action="store_true",
        help="also sweep the _quarantine subtree (readers exclude it, so this is rarely useful)",
    )
    args = ap.parse_args()

    root = resolve_dataset_root(args.root, expected_root)
    report = normalize_expiry(root, args.apply, include_quarantine=args.include_quarantine)

    mode = "APPLY" if args.apply else "DRY-RUN"
    changed = [r for r in report if r["action"] in ("rewritten", "would-rewrite")]
    print(f"[{mode}] root={root}  files={len(report)}  needing rewrite={len(changed)}")
    for r in changed:
        rel = Path(r["path"]).relative_to(root)
        print(f"  {str(rel):<60} {r['from_type']:<10} rows={r['rows']:>7} unparseable={r['unparseable']:>5}")
    by_action: dict[str, int] = {}
    for r in report:
        by_action[r["action"]] = by_action.get(r["action"], 0) + 1
    print("  " + "  ".join(f"{k}={v}" for k, v in sorted(by_action.items())))
    total_rows = sum(r["rows"] for r in changed)
    total_unparseable = sum(r["unparseable"] for r in changed)
    print(f"  TOTAL rows to rewrite={total_rows} unparseable={total_unparseable}")
    if args.apply:
        print(f"  originals kept as *{BACKUP_SUFFIX} — delete them once verified")
    else:
        print("  (dry-run — re-run with --apply to rewrite)")


if __name__ == "__main__":
    main()
