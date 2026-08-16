"""Rebuild gold/dataset=ticker_base_rates from scratch, replacing the old output.

Before ``fix(ml): exclude label_window_truncated rows from ticker_base_rates``,
every write pooled SWING/LEAP alerts whose labels were resolved against a
collapsed ~3-5h barrier-check window in with genuine intraday outcomes. Measured
against production: of the 10,822 (ticker, day) rows the old code wrote across
84 files / 42 partitions (2026-04-15 onward), **10,678 (98.7%) don't exist at
all** under the corrected pipeline — those tickers' history was almost entirely
truncated SWING/LEAP alerts. The 144 that do survive are still wrong (median
win-rate error 11.8 points, up to 88.7; median 1,076 phantom prior alerts
counted per row).

Orion asof-joins this dataset onto every alert it scores or trains
(``EQUITY_TICKER_RATES_FEATURES``) via its own reader
(``orion/clients/heber_reader.py``), which is **not** ``HeberReader`` — it globs
every ``.parquet`` under ``dataset=ticker_base_rates`` directly, with no version
pinning and no ``_``-prefixed-directory exclusion at all. That rules out the
usual "write a clean v2, let version resolution pick it up" fix: Orion would
union v1 and v2. It also means the backup this script makes cannot live
anywhere under ``dataset=ticker_base_rates/`` — not even in a `_`-prefixed
subdirectory — or Orion would read it right back in. The backup goes under
``gold/_migrations/ticker_base_rates_rebuild/<run>/``, a sibling of
``dataset=ticker_base_rates`` entirely outside it.

``write_gold`` only ever appends a new uniquely-named part-file — there is no
in-place replace. The naive order (back old files out, then write new ones)
means a live reader can see the dataset partially or entirely empty for the
whole gap between those two steps, and any interruption in that gap leaves it
that way durably. This does the opposite: new files are written **first**,
straight into the live ``dt=`` partitions via the same atomic
temp-file-then-rename pattern already used elsewhere in this codebase (e.g.
``heber/ml/datasets.py::_atomic_write_parquet``), while every old file is still
sitting right where it always was. Only after every new file is confirmed
on-disk with the exact row count expected are the *old* fragments moved out to
the backup. At no point does the dataset appear empty, partial, or under-count
— the worst a concurrent reader (or a crash) can ever see is old and new data
coexisting, which is no different in character from the multi-fragment
partitions this dataset already has today. A partition with no surviving rows
under the fix is removed once its old fragments are gone, not left as an empty
stub.

This is a one-shot manual migration, not a crash-resumable pipeline: if it is
interrupted, inspect the manifest and the partition contents by hand before
deciding whether to re-run rather than assuming it is safe to retry blindly —
a second run would write a second copy of the corrected rows alongside
whatever is already live.

The whole dataset version directory is held under a lock for the duration, but
that only fences a second invocation of *this* script — the live scheduled
pipeline (``TickerBaseRatesPipeline.run()`` in
``heber/features/pipelines/ticker_base_rates.py``, run nightly by
``heber/gold_poller/service.py``) does not check it and writes lock-free by
design. Add ``ticker_base_rates`` to ``HEBER_GOLD_POLLER_DISABLED_PIPELINES``
before running with ``--apply`` and remove it again afterward; this is an
operational precaution, not something this script enforces. Its blast radius
is bounded by the write-new-first ordering above: once the fix (this
pipeline's own PR) is deployed, anything the scheduler appends during a
rebuild is already correct data, so a missed disable step means an extra
coexisting file for one day at worst, never lost or corrupted data.

Usage:
    .venv/bin/python scripts/rebuild_ticker_base_rates.py                 # dry run
    .venv/bin/python scripts/rebuild_ticker_base_rates.py --apply
    .venv/bin/python scripts/rebuild_ticker_base_rates.py --apply --expect-old-rows 56053 --expect-new-rows 749
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from filelock import FileLock, Timeout

sys.path.insert(0, str(Path(__file__).parent.parent))

from heber.config import settings
from heber.features.pipelines.ticker_base_rates import EXPECTED_OUTPUT_COLUMNS, compute_ticker_base_rates
from heber.reader import HeberReader

DATASET = "ticker_base_rates"
LOCK_TIMEOUT_SECONDS = 300


class RowCountMismatch(RuntimeError):
    """The rows actually on disk after a write don't match what was written.

    Raised before any old data is touched — a bad write must never be
    compounded by then removing the still-good old data on top of it.
    """


@contextmanager
def dataset_lock(version_root: Path, timeout: float = LOCK_TIMEOUT_SECONDS):
    """Exclusive lock over the whole dataset version being rebuilt.

    Lives beside ``version_root``, not inside it — a lock file under a
    hive-partitioned tree that pyarrow auto-walks would break dataset open the
    same way an in-partition lock file does.
    """
    lock_root = version_root.parent / "_locks"
    lock_root.mkdir(parents=True, exist_ok=True)
    lock = FileLock(lock_root / f"{version_root.name}.lock", timeout=timeout)
    with lock:
        yield


@dataclass
class RebuildReport:
    old_files_backed_up: int
    old_rows: int
    new_files_written: int
    new_rows: int
    partitions_removed: list[str]


def _existing_fragments(version_root: Path) -> list[Path]:
    """Real data fragments only — skip AppleDouble sidecars and partial writes."""
    if not version_root.is_dir():
        return []
    return sorted(
        f for f in version_root.glob("dt=*/*.parquet") if not f.name.startswith("._") and not f.name.endswith(".tmp")
    )


def _write_new_partition(version_root: Path, dt: object, group: pd.DataFrame) -> Path:
    """Write one dt= partition's corrected rows, atomically, into the live tree.

    Temp-file-then-rename: the ``.tmp`` suffix keeps it invisible to every
    reader in this codebase until the write is complete, matching the
    convention ``heber/ml/datasets.py`` and the label-window migration
    scripts already use.
    """
    partition_dir = version_root / f"dt={dt}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    final_path = partition_dir / f"part-{ts}-{uuid.uuid4().hex[:8]}.parquet"
    temp_path = final_path.with_suffix(".parquet.tmp")

    table = pa.Table.from_pandas(group.reset_index(drop=True), preserve_index=False)
    pq.write_table(table, str(temp_path), compression="snappy", use_dictionary=False)
    os.replace(temp_path, final_path)
    return final_path


def rebuild(
    version_root: Path,
    corrected: pd.DataFrame,
    backup_root: Path,
    write: bool,
) -> RebuildReport:
    """Replace every row in ``version_root`` with ``corrected``.

    ``corrected`` is the full historical recompute — the direct output of
    ``compute_ticker_base_rates`` over the same date range the dataset already
    covers, not a delta. New files are written to the live tree and verified
    on disk *before* any old fragment is touched, so old data is never removed
    on top of a write that silently failed, and the dataset is never observed
    empty or partial.
    """
    old_fragments = _existing_fragments(version_root)
    old_rows = sum(len(pd.read_parquet(f)) for f in old_fragments)
    new_rows = len(corrected)

    if not write:
        return RebuildReport(len(old_fragments), old_rows, 0, new_rows, [])

    new_files: list[Path] = []
    verified_new_rows = 0
    if not corrected.empty:
        by_dt = pd.to_datetime(corrected["ts_event"], utc=True).dt.date
        for dt, group in corrected.groupby(by_dt):
            written = _write_new_partition(version_root, dt, group)
            # Verified per-partition, immediately: a short or empty write left
            # sitting in a live dt= partition can break schema unification for
            # any reader of it, even with the (still-correct) old data right
            # beside it — the same failure the label-window quarantine script
            # hit before it started removing empty fragments instead of
            # leaving them behind. A bad file is deleted on the spot rather
            # than left for a human to find later.
            actual_rows = len(pd.read_parquet(written))
            if actual_rows != len(group):
                written.unlink()
                raise RowCountMismatch(
                    f"wrote {actual_rows} rows to {written.name} but expected {len(group)} "
                    "— removed the bad file and aborted before touching any old data"
                )
            new_files.append(written)
            verified_new_rows += actual_rows

    # Only now, with the corrected replacement confirmed live and correct,
    # does the old (contaminated) data get moved out.
    backed_up = 0
    touched_partitions: set[Path] = set()
    for fragment in old_fragments:
        partition = fragment.parent
        touched_partitions.add(partition)
        target = backup_root / partition.name / fragment.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(fragment), str(target))
        backed_up += 1

    removed: list[str] = []
    for partition in touched_partitions:
        if partition.is_dir() and not any(partition.iterdir()):
            partition.rmdir()
            removed.append(partition.name)

    return RebuildReport(backed_up, old_rows, len(new_files), verified_new_rows, removed)


def _load_corrected(reader: HeberReader, start: str, end: str, labels_project: str) -> pd.DataFrame:
    """Recompute the full corrected dataset for [start, end], same shape
    ``TickerBaseRatesPipeline.run`` produces but without its append-only write."""
    label_start = pd.Timestamp(start, tz="UTC") - pd.Timedelta(days=90)
    labels = reader.read_gold(
        dataset="labels_alert_barriers",
        project=labels_project,
        time_range=(label_start, end),
        prune_by_dt=True,
    )
    if labels.empty:
        return pd.DataFrame(columns=EXPECTED_OUTPUT_COLUMNS)

    df = compute_ticker_base_rates(labels, window_days=90)
    if df.empty:
        return df

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC").replace(hour=23, minute=59, second=59)
    return df[(df["ts_event"] >= start_ts) & (df["ts_event"] <= end_ts)].reset_index(drop=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Persist the rebuild (default: dry run)")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")
    parser.add_argument("--labels-project", default="watch", help="Source labels dataset project")
    parser.add_argument("--start", help="Override auto-detected start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Override auto-detected end date (YYYY-MM-DD)")
    parser.add_argument("--expect-old-rows", type=int, help="Fail unless exactly this many old rows are found")
    parser.add_argument("--expect-new-rows", type=int, help="Fail unless exactly this many corrected rows are written")
    args = parser.parse_args(argv)

    version_root = settings.gold_path / f"dataset={DATASET}" / f"project={args.project}" / f"version={args.version}"
    fragments = _existing_fragments(version_root)
    if not fragments:
        print(f"No existing fragments under {version_root} — nothing to rebuild.")
        sys.exit(1)

    dates = sorted(f.parent.name.removeprefix("dt=") for f in fragments)
    start = args.start or dates[0]
    end = args.end or dates[-1]

    print("=" * 78)
    print("REBUILD gold/dataset=ticker_base_rates")
    print(f"  dataset root : {version_root}")
    print(f"  date range   : {start} .. {end}  (auto-detected from {len(fragments)} existing fragments)")
    print(f"  mode         : {'APPLY' if args.apply else 'DRY RUN'}")
    print("=" * 78)

    reader = HeberReader()
    corrected = _load_corrected(reader, start, end, args.labels_project)

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    # Sibling of dataset=ticker_base_rates, never a descendant — Orion's own
    # reader globs everything under the dataset root with no exclusion rule,
    # so a backup living inside it would still leak back into Orion's reads.
    backup_root = settings.gold_path / "_migrations" / f"{DATASET}_rebuild" / run_id

    try:
        with dataset_lock(version_root):
            # Plan pass first (write=False, read-only) so a cohort mismatch is
            # caught and reported *before* the destructive call is ever made —
            # not just checked afterward, by which point the damage is done.
            plan = rebuild(
                version_root=version_root,
                corrected=corrected,
                backup_root=backup_root,
                write=False,
            )

            mismatches = [
                f"{label}: expected {expected}, saw {actual}"
                for label, expected, actual in (
                    ("old rows", args.expect_old_rows, plan.old_rows),
                    ("corrected rows", args.expect_new_rows, plan.new_rows),
                )
                if expected is not None and expected != actual
            ]

            print(f"\n  old fragments   : {plan.old_files_backed_up}  ({plan.old_rows} rows)")
            print(f"  corrected rows  : {plan.new_rows}")

            if mismatches:
                print("\n  !! cohort mismatch — refusing to proceed:")
                for line in mismatches:
                    print(f"     {line}")
                sys.exit(1)

            if not args.apply:
                print("\n  *** DRY RUN — nothing written. Pass --apply to persist. ***")
                sys.exit(0)

            report = rebuild(
                version_root=version_root,
                corrected=corrected,
                backup_root=backup_root,
                write=True,
            )
    except Timeout:
        print("\n  !! could not acquire the dataset lock — another writer holds it.")
        sys.exit(1)
    except RowCountMismatch as exc:
        print(f"\n  !! {exc}")
        print("  Old data was left untouched.")
        sys.exit(1)

    print(f"  partitions removed (no surviving rows): {len(report.partitions_removed)}")
    print(f"  backup          : {backup_root}")

    manifest = {
        "run_at": datetime.now(UTC).isoformat(),
        "date_range": [start, end],
        "old_files_backed_up": report.old_files_backed_up,
        "old_rows": report.old_rows,
        "new_files_written": report.new_files_written,
        "new_rows": report.new_rows,
        "partitions_removed": report.partitions_removed,
        "backup_root": str(backup_root),
    }
    manifest_path = backup_root / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  manifest: {manifest_path}")


if __name__ == "__main__":
    main()
