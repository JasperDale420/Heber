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

``write_gold`` only ever appends a new uniquely-named part-file per ``dt=``
partition — there is no in-place replace. So the sequence is: back up every
existing file (moved, not deleted — same-volume rename, not a copy), remove
any partition left with no files, then call the same ``write_gold`` the live
pipeline uses to write the corrected historical output fresh. A partition with
no surviving rows under the fix is removed entirely rather than left as an
empty stub.

The whole dataset version directory is held under a lock for the duration —
concurrent with a scheduled nightly run, this would race it (see run() in
``heber/features/pipelines/ticker_base_rates.py``, which is lock-free and does
not check this lock). Add ``ticker_base_rates`` to
``HEBER_GOLD_POLLER_DISABLED_PIPELINES`` before running with ``--apply`` and
remove it again afterward.

Usage:
    .venv/bin/python scripts/rebuild_ticker_base_rates.py                 # dry run
    .venv/bin/python scripts/rebuild_ticker_base_rates.py --apply
    .venv/bin/python scripts/rebuild_ticker_base_rates.py --apply --expect-old-rows 56053 --expect-new-rows 749
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from filelock import FileLock, Timeout

sys.path.insert(0, str(Path(__file__).parent.parent))

from heber.config import settings
from heber.features.pipelines.ticker_base_rates import compute_ticker_base_rates
from heber.reader import HeberReader

DATASET = "ticker_base_rates"
LOCK_TIMEOUT_SECONDS = 300


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


def rebuild(
    version_root: Path,
    corrected: pd.DataFrame,
    backup_root: Path,
    reader: HeberReader,
    project: str,
    version: str,
    write: bool,
) -> RebuildReport:
    """Replace every row in ``version_root`` with ``corrected``.

    ``corrected`` is the full historical recompute — the direct output of
    ``compute_ticker_base_rates`` over the same date range the dataset already
    covers, not a delta. Old fragments move to ``backup_root`` before anything
    new is written, so an interruption at any point leaves every row readable
    from at least one place — duplicated at worst, never lost.
    """
    fragments = _existing_fragments(version_root)
    old_rows = sum(len(pd.read_parquet(f)) for f in fragments)
    new_rows = len(corrected)

    if not write:
        return RebuildReport(len(fragments), old_rows, 0, new_rows, [])

    backed_up = 0
    touched_partitions: set[Path] = set()
    for fragment in fragments:
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

    new_files_written = 0
    if not corrected.empty:
        # write_gold groups by date(ts_event) and writes one file per group.
        new_files_written = pd.to_datetime(corrected["ts_event"], utc=True).dt.date.nunique()
        reader.write_gold(dataset=DATASET, df=corrected, project=project, version=version)

    return RebuildReport(backed_up, old_rows, new_files_written, new_rows, removed)


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
        return pd.DataFrame()

    df = compute_ticker_base_rates(labels, window_days=90)
    if df.empty:
        return df

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC").replace(hour=23, minute=59, second=59)
    return df[(df["ts_event"] >= start_ts) & (df["ts_event"] <= end_ts)].reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Persist the rebuild (default: dry run)")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")
    parser.add_argument("--labels-project", default="watch", help="Source labels dataset project")
    parser.add_argument("--start", help="Override auto-detected start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Override auto-detected end date (YYYY-MM-DD)")
    parser.add_argument("--expect-old-rows", type=int, help="Fail unless exactly this many old rows are found")
    parser.add_argument("--expect-new-rows", type=int, help="Fail unless exactly this many corrected rows are written")
    args = parser.parse_args()

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
            report = rebuild(
                version_root=version_root,
                corrected=corrected,
                backup_root=backup_root,
                reader=reader,
                project=args.project,
                version=args.version,
                write=args.apply,
            )
    except Timeout:
        print("\n  !! could not acquire the dataset lock — another writer holds it.")
        sys.exit(1)

    print(f"\n  old fragments   : {report.old_files_backed_up}  ({report.old_rows} rows)")
    print(f"  corrected rows  : {report.new_rows}")
    if args.apply:
        print(f"  partitions removed (no surviving rows): {len(report.partitions_removed)}")
        print(f"  backup          : {backup_root}")

    mismatches = [
        f"{label}: expected {expected}, saw {actual}"
        for label, expected, actual in (
            ("old rows", args.expect_old_rows, report.old_rows),
            ("corrected rows", args.expect_new_rows, report.new_rows),
        )
        if expected is not None and expected != actual
    ]
    if mismatches:
        print("\n  !! cohort mismatch — refusing to report success:")
        for line in mismatches:
            print(f"     {line}")

    if not args.apply:
        print("\n  *** DRY RUN — nothing written. Pass --apply to persist. ***")
        sys.exit(1 if mismatches else 0)

    manifest = {
        "run_at": datetime.now(UTC).isoformat(),
        "date_range": [start, end],
        "old_files_backed_up": report.old_files_backed_up,
        "old_rows": report.old_rows,
        "new_rows": report.new_rows,
        "partitions_removed": report.partitions_removed,
        "backup_root": str(backup_root),
        "cohort_mismatches": mismatches,
    }
    manifest_path = backup_root / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  manifest: {manifest_path}")

    if mismatches:
        sys.exit(1)


if __name__ == "__main__":
    main()
