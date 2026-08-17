"""The compactor must make forward progress on both shapes of partition it meets.

Two distinct stalls, both measured on the live lakehouse:

1. Files individually larger than the batch byte budget. The first file is
   appended unconditionally, so a single 80-139 MB file fills the batch alone,
   `len(small_files) <= 1` discards it, and nothing on disk changes — so the
   next scan repeats it. 297 `massive_taq_*` partitions were capped this way
   every day for a week with no progress.

2. Partitions with hundreds of tiny files. `MAX_FILES_PER_BATCH = 50` caps a
   batch by count regardless of size, and the loop runs hourly, so a day that
   lands 274 x 12 KB files drains at 50/hour while the writer keeps adding.
   `dt=2026-08-07` still held 273 uncompacted parts five days on, which is why
   reads of recent data are slow.

3. Partitions with *tens of thousands* of files, which the compactor never
   reached at all. Two costs both scaled with file count rather than partition
   count. `rglob("*")` yields every file and asks each one `is_dir()`, a stat
   per file: measured at 8 entries/s against `feed=quotes`, whose 825,080 files
   would take ~28 hours to walk past — and `feed=quotes` had never appeared in
   a compactor log line, nor had one cycle ever completed. Then selection
   stat'd every file in a partition before merging any: exFAT directories are
   unindexed, so a stat inside a 35,955-entry directory measured 53.7ms, or
   ~32 minutes to select one batch, repeated every cycle because only
   `MAX_FILES_PER_BATCH` of them get merged per visit.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.writer import compactor as compactor_module
from heber.writer.compactor import MAX_BATCH_COMPRESSED_BYTES, Compactor


def _write(part: Path, name: str, *, rows: int = 1, pad: int = 0) -> Path:
    part.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "event_id": [f"{name}-{i}" for i in range(rows)],
            "value": [1.0] * rows,
            # Distinct payloads: identical strings dedupe to almost nothing on disk.
            "blob": [f"{i:08d}" + "".join(chr(65 + (i + j) % 26) for j in range(pad)) for i in range(rows)]
            if pad
            else [""] * rows,
        }
    )
    path = part / name
    pq.write_table(pa.Table.from_pandas(frame), str(path), compression="none")
    return path


def test_many_tiny_files_compact_in_one_pass(tmp_path: Path) -> None:
    """A day's worth of small files must not drain 50 per hour.

    The byte budget is the memory guard; a file-count cap that ignores size
    just throttles the common case — 274 files of 12 KB is 3.3 MB, two orders
    of magnitude inside the budget.
    """
    part = tmp_path / "dt=2026-08-07"
    for i in range(274):
        _write(part, f"part-{i:04d}.parquet")

    selected = Compactor()._collect_small_files(part)

    assert selected is not None, "nothing selected from a partition of 274 small files"
    files, _ = selected
    assert len(files) == 274, f"only {len(files)} of 274 selected; a full day still needs many passes"


def test_batch_still_bounded_by_bytes(tmp_path: Path) -> None:
    """Raising the file cap must not remove the memory guard."""
    part = tmp_path / "dt=2026-08-07"
    # ~4 MB each, so the 50 MB budget binds well before any file-count limit.
    for i in range(30):
        _write(part, f"part-{i:02d}.parquet", rows=200, pad=20_000)

    selected = Compactor()._collect_small_files(part)

    assert selected is not None
    files, sizes = selected
    assert sum(sizes[f] for f in files) <= MAX_BATCH_COMPRESSED_BYTES
    assert len(files) < 30, "byte budget did not bind"


def test_oversized_files_do_not_stall_the_partition(tmp_path: Path, monkeypatch) -> None:
    """A file bigger than the whole batch budget must not poison selection.

    Previously the first file was added unconditionally, so one oversized file
    filled the batch, the batch collapsed to a single entry, and the partition
    was discarded on every cycle forever. The compactable remainder must still
    be selected.
    """
    monkeypatch.setattr(compactor_module, "MAX_BATCH_COMPRESSED_BYTES", 10_000)
    part = tmp_path / "dt=2024-06-01"
    _write(part, "part-huge.parquet", rows=400, pad=2_000)  # far over the budget
    for i in range(3):
        _write(part, f"part-small-{i}.parquet")

    selected = Compactor()._collect_small_files(part)

    assert selected is not None, "oversized file stalled the whole partition"
    files, _ = selected
    assert all("huge" not in f.name for f in files), "oversized file must be excluded, not batched"
    assert len(files) == 3


def test_partition_of_only_oversized_files_is_skipped_quietly(tmp_path: Path, monkeypatch) -> None:
    """Nothing to do is a legitimate answer — but it must not look like work pending."""
    monkeypatch.setattr(compactor_module, "MAX_BATCH_COMPRESSED_BYTES", 10_000)
    part = tmp_path / "dt=2024-06-02"
    for i in range(2):
        _write(part, f"part-huge-{i}.parquet", rows=400, pad=2_000)

    assert Compactor()._collect_small_files(part) is None


# ── stall 3: cost that scales with file count ────────────────────────────────


def test_selection_stats_only_what_it_intends_to_merge(tmp_path: Path, monkeypatch) -> None:
    """Selecting a batch must cost the batch, not the partition.

    A stat inside a 35,955-entry exFAT directory measured 53.7ms, so statting
    the whole partition to pick 1,000 files spent ~32 minutes per visit and
    repeated it on every cycle. The partition never drained.
    """
    monkeypatch.setattr(compactor_module, "MAX_FILES_PER_BATCH", 10)
    part = tmp_path / "instrument_type=option" / "dt=2026-06-18" / "hour=14"
    for i in range(200):
        _write(part, f"part-{i:05d}.parquet")

    stat_calls: list[Path] = []
    real_stat = compactor_module.Compactor._safe_stat_size

    def _counting_stat(path: Path, partition_path: Path):
        stat_calls.append(path)
        return real_stat(path, partition_path)

    monkeypatch.setattr(compactor_module.Compactor, "_safe_stat_size", staticmethod(_counting_stat))

    selected = Compactor()._collect_small_files(part)

    assert selected is not None
    files, _ = selected
    assert len(files) == 10, "a full batch must still be selected"
    assert len(stat_calls) < 50, (
        f"stat'd {len(stat_calls)} of 200 files to select 10 — cost still scales with the partition"
    )


def test_walk_yields_partition_directories_and_never_files(tmp_path: Path) -> None:
    """The scan must not ask every file whether it is a directory.

    `rglob("*")` yields files as well as directories and stats each one to find
    out which it is. Against feed=quotes that is 825,080 stats — measured at 8
    entries/s, so the partitions behind them were never reached.
    """
    part = tmp_path / "feed=quotes" / "instrument_type=option" / "dt=2026-06-18" / "hour=14"
    for i in range(5):
        _write(part, f"part-{i}.parquet")
    (tmp_path / "feed=quotes" / "._sidecar_dir").mkdir(parents=True, exist_ok=True)

    found = list(Compactor()._iter_partition_dirs(tmp_path))

    assert part in found, "a nested partition must still be reached"
    assert all(p.is_dir() for p in found), "the walk yielded a file"
    assert not any(p.name.startswith(".") for p in found), "AppleDouble sidecar directory was walked"


def test_layer_root_is_never_itself_a_partition(tmp_path: Path, monkeypatch) -> None:
    """Stray Parquet files at the Silver root must not be merged and deleted.

    `rglob("*")` never yields the directory it starts from, so the root was
    never a compaction target. A walk that yields its own starting point makes
    any two loose `silver/*.parquet` files look like a partition — they would
    be schema-unified, deduplicated against each other, written as one file,
    and the originals unlinked. Compaction deletes its sources, so widening
    what counts as a partition destroys data.
    """
    data_root = tmp_path
    (data_root / ".heber-sentinel").touch()
    silver = data_root / "silver"
    silver.mkdir()
    _write(silver, "stray-a.parquet")
    _write(silver, "stray-b.parquet")
    monkeypatch.setattr(compactor_module.settings, "data_root", data_root)

    result = Compactor().scan_and_compact("silver")

    assert (silver / "stray-a.parquet").exists(), "root-level file was deleted by compaction"
    assert (silver / "stray-b.parquet").exists(), "root-level file was deleted by compaction"
    assert result["files_merged"] == 0
    assert not list(silver.glob("compacted-*.parquet")), "the layer root was treated as a partition"


def test_a_wide_dataset_cannot_starve_a_deep_one(tmp_path: Path) -> None:
    """One dataset's shape must not decide when another is reached.

    `massive_bars` holds 11,010 per-ticker directories and `feed=quotes` keeps
    its partitions four levels down. Breadth-first put all 11,010 and their
    children ahead of the first `feed=quotes` partition — 26 minutes into a
    cycle it had still not arrived. Depth-first drained whichever dataset it
    entered first instead. Either way one dataset's shape starved another.
    """
    wide = tmp_path / "feed=wide"  # a Heber dataset with thousands of flat children (massive_* is not walked at all)
    for i in range(300):
        (wide / f"instrument_key=equity:T{i:04d}").mkdir(parents=True)
    deep = tmp_path / "feed=quotes" / "instrument_type=option" / "dt=2026-06-18" / "hour=14"
    deep.mkdir(parents=True)

    order = list(Compactor()._iter_partition_dirs(tmp_path))
    reached_at = order.index(deep)

    assert reached_at < 50, (
        f"the deep partition was reached only after {reached_at} directories; a wide dataset is delaying it"
    )


def test_a_wide_branch_cannot_starve_its_sibling_inside_one_dataset(tmp_path: Path) -> None:
    """Fairness has to hold at every level, not just between datasets.

    Being fair only across top-level datasets moves the same starvation one
    level down: `feed=quotes/instrument_type=option` holds thousands of
    directories, and draining it before touching `instrument_type=equity`
    delays that sibling by every directory *and every compaction* in the first
    subtree — hours, on the live lakehouse.
    """
    feed = tmp_path / "feed=quotes"
    for branch in ("instrument_type=option", "instrument_type=equity"):
        for i in range(150):
            (feed / branch / f"dt=2026-06-{i:03d}").mkdir(parents=True)

    order = list(Compactor()._iter_partition_dirs(tmp_path))
    first_partitions = [p for p in order if p.name.startswith("dt=")][:20]
    branches = {p.parent.name for p in first_partitions}

    assert branches == {"instrument_type=option", "instrument_type=equity"}, (
        f"the first 20 partitions all came from {branches}; one branch is being drained before its sibling is touched"
    )


def test_quarantined_files_are_never_compacted(tmp_path: Path) -> None:
    """Quarantine holds evidence. Compaction deletes what it merges.

    `silver/_quarantine/` is where damaged or suspect files are moved to be
    kept, and the walk descended into it — reached in production once the scan
    was fast enough to get there. Today each quarantined file happens to sit
    alone in its directory, so the `<= 1` guard saves them; two in one
    directory would be merged into a new file and the originals unlinked.
    `_`-prefixed paths are ignored by pyarrow and Hive tooling for exactly this
    reason, so the walk ignores them too.
    """
    quarantined = tmp_path / "_quarantine" / "2026-04-29" / "feed=data" / "dt=2026-02-11"
    _write(quarantined, "compacted-a.parquet")
    _write(quarantined, "compacted-b.parquet")
    live = tmp_path / "feed=data" / "dt=2026-02-11"
    _write(live, "part-0.parquet")

    found = list(Compactor()._iter_partition_dirs(tmp_path))

    assert live in found, "the live partition must still be walked"
    assert not any("_quarantine" in p.parts for p in found), "the walk descended into quarantine"


def test_walk_cost_is_independent_of_how_many_files_a_partition_holds(tmp_path: Path) -> None:
    """Adding files to a partition must not add work to the walk."""
    few = tmp_path / "few" / "dt=2026-01-01"
    many = tmp_path / "many" / "dt=2026-01-01"
    _write(few, "part-0.parquet")
    for i in range(150):
        _write(many, f"part-{i:04d}.parquet")

    found = list(Compactor()._iter_partition_dirs(tmp_path))

    # few/, few/dt=, many/, many/dt=  — the root itself is never yielded
    assert len(found) == 4, f"walk yielded {len(found)} entries; it is counting files, not directories"


def test_atlas_owned_massive_trees_are_never_walked(tmp_path: Path) -> None:
    """Atlas keeps its own silver trees under the lake root, named ``massive_*``
    (``massive_bars``, ``massive_intraday_daily``, ``massive_taq_*`` and their
    staged ``_v2_<stamp>``, ``.sealing`` and ``_pre_v1_<stamp>`` generations).
    Their layouts are Atlas's contract — a flat ``year=*.parquet`` directory is
    read by year-file name, so merging it would silently drop years — and the
    hourly walk over ``massive_bars``' 11k one-file dirs left a lock file in
    each and held handles a cache swap has to wait out. Heber's own Massive
    backfill also promotes one canonical ``dt=*/part-00000.parquet`` there, so
    the trees are reserved from generic compaction as a cross-repo boundary.
    """
    for name in (
        "massive_bars",
        "massive_bars_v2_20260816b.sealing",
        "massive_intraday_daily_pre_v1_20260816b",
        "massive_taq_secagg_1s",
    ):
        _write(tmp_path / name / "sub", "a.parquet")
        _write(tmp_path / name / "sub", "b.parquet")
    live = tmp_path / "feed=data" / "dt=2026-02-11"
    _write(live, "part-0.parquet")

    found = list(Compactor()._iter_partition_dirs(tmp_path))

    assert live in found, "the live partition must still be walked"
    assert not any(part.startswith("massive_") for p in found for part in p.relative_to(tmp_path).parts), (
        f"the walk entered an Atlas-owned tree: {[str(p.relative_to(tmp_path)) for p in found]}"
    )


def test_scan_and_compact_leaves_atlas_trees_untouched_and_still_compacts_feeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end: a full scan merges an ordinary feed partition but neither
    merges, locks, nor writes anything inside a ``massive_*`` dataset."""
    data_root = tmp_path
    (data_root / ".heber-sentinel").touch()
    silver = data_root / "silver"
    atlas = silver / "massive_intraday_daily"
    _write(atlas, "year=2003.parquet")
    _write(atlas, "year=2026.parquet")
    feed = silver / "feed=data" / "dt=2026-02-11"
    _write(feed, "part-0.parquet")
    _write(feed, "part-1.parquet")
    monkeypatch.setattr(compactor_module.settings, "data_root", data_root)

    result = Compactor().scan_and_compact("silver")

    assert sorted(p.name for p in atlas.iterdir()) == ["year=2003.parquet", "year=2026.parquet"], (
        f"the Atlas tree was modified: {sorted(p.name for p in atlas.iterdir())}"
    )
    assert not (atlas / ".compaction.lock").exists()
    assert result["files_merged"] == 2, result
    assert not (feed / "part-0.parquet").exists() and list(feed.glob("compacted-*.parquet")), (
        "the ordinary feed partition was not compacted"
    )
