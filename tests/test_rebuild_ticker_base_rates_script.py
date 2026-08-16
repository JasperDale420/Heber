"""The ticker_base_rates rebuild must fully replace the dataset, never blend,
and must never leave a live reader looking at an empty or under-verified tree.

``write_gold`` only ever appends a uniquely-named part-file, so re-running the
now-fixed pipeline over history the normal way would leave the old
contaminated files sitting alongside the new clean ones — Orion's reader globs
every file under ``dataset=ticker_base_rates`` with no version pinning and no
underscore-directory exclusion, so it would union both. This script backs the
old files out to a location entirely outside the ``dataset=`` tree (so neither
Heber's reader nor Orion's can ever see them) — but only *after* the corrected
replacement is written and verified live: new files land first (old data
stays in place the whole time), so at no point does the dataset appear empty
or partial, even under a crash mid-run.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

from heber.reader import HeberReader

pytestmark = pytest.mark.integration

_SCRIPT = Path(__file__).parent.parent / "scripts" / "rebuild_ticker_base_rates.py"


@pytest.fixture
def script():
    spec = importlib.util.spec_from_file_location("rebuild_ticker_base_rates", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_old_partition(version_root: Path, dt_str: str, rows: int, *files: str) -> None:
    """Simulate the old, unfiltered pipeline's append-only output."""
    partition = version_root / f"dt={dt_str}"
    partition.mkdir(parents=True, exist_ok=True)
    for name in files:
        pd.DataFrame(
            {
                "instrument_key": [f"equity:X{i}" for i in range(rows)],
                "ts_event": pd.to_datetime([f"{dt_str}T15:00:00Z"] * rows, utc=True),
                "ts_available": pd.to_datetime([f"{dt_str}T15:00:00Z"] * rows, utc=True),
                "ticker_win_rate_90d": [0.1] * rows,
                "ticker_alert_frequency": [999] * rows,
                "ticker_flow_predictability": [0.8] * rows,
            }
        ).to_parquet(partition / name, index=False)


def _corrected(*specs: tuple[str, str, float]) -> pd.DataFrame:
    """A stand-in for compute_ticker_base_rates' fixed output."""
    return pd.DataFrame(
        [
            {
                "instrument_key": ticker,
                "ts_event": pd.Timestamp(dt, tz="UTC"),
                "ts_available": pd.Timestamp(dt, tz="UTC"),
                "ticker_win_rate_90d": rate,
                "ticker_alert_frequency": 3,
                "ticker_flow_predictability": abs(rate - 0.5) * 2,
            }
            for ticker, dt, rate in specs
        ]
    )


def _reader_and_root(tmp_path: Path) -> tuple[HeberReader, Path]:
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    return HeberReader(data_root=tmp_path), root


def _backup_root(tmp_path: Path) -> Path:
    return tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run"


def test_dry_run_touches_nothing(script, tmp_path: Path) -> None:
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    before = (root / "dt=2026-04-15" / "part-old.parquet").read_bytes()

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=_backup_root(tmp_path),
        write=False,
    )

    assert report.old_rows == 5
    assert report.new_rows == 1
    assert (root / "dt=2026-04-15" / "part-old.parquet").read_bytes() == before
    assert not (tmp_path / "gold" / "_migrations").exists()


def test_apply_backs_up_old_and_writes_corrected(script, tmp_path: Path) -> None:
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    backup_root = _backup_root(tmp_path)

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=backup_root,
        write=True,
    )

    assert (report.old_rows, report.new_rows, report.old_files_backed_up) == (5, 1, 1)

    live = pd.read_parquet(root / "dt=2026-04-15")
    live = live[live["ticker_alert_frequency"] != 999]  # old file already backed out, but be explicit
    assert len(live) == 1
    assert live["instrument_key"].iloc[0] == "equity:AAPL"
    assert live["ticker_win_rate_90d"].iloc[0] == 0.4

    backed_up = pd.read_parquet(backup_root / "dt=2026-04-15")
    assert len(backed_up) == 5
    assert (backed_up["ticker_alert_frequency"] == 999).all()


def test_new_data_is_live_before_old_data_is_removed(script, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The critical ordering: if the run is interrupted before old data is
    backed out, the dataset must show old+new coexisting — never empty."""
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")

    real_move = script.shutil.move
    seen_new_file_before_old_removed = {"value": False}

    def spying_move(src, dst):
        # By the time anything is moved to backup, the new corrected file
        # must already be live and readable in the partition.
        live_files = [f for f in (root / "dt=2026-04-15").glob("*.parquet") if not f.name.startswith("._")]
        seen_new_file_before_old_removed["value"] = any(
            "ticker_alert_frequency" in pd.read_parquet(f).columns
            and (pd.read_parquet(f)["ticker_alert_frequency"] != 999).any()
            for f in live_files
        )
        raise RuntimeError("simulated crash during old-fragment backup")

    monkeypatch.setattr(script.shutil, "move", spying_move)

    with pytest.raises(RuntimeError, match="simulated crash"):
        script.rebuild(
            version_root=root,
            corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
            backup_root=_backup_root(tmp_path),
            write=True,
        )

    assert seen_new_file_before_old_removed["value"] is True
    # Old data is also still physically present (never removed): the dataset
    # was never empty at any point up to the simulated crash.
    still_present = pd.read_parquet(root / "dt=2026-04-15")
    assert (still_present["ticker_alert_frequency"] == 999).any()
    monkeypatch.setattr(script.shutil, "move", real_move)


def test_backup_is_outside_the_dataset_tree_orion_globs(script, tmp_path: Path) -> None:
    """Orion's reader recursively globs everything under dataset=ticker_base_rates
    with no version or underscore-directory exclusion at all — so the backup
    must not be a descendant of it, or Orion would read the old rows anyway."""
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    backup_root = _backup_root(tmp_path)

    script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=backup_root,
        write=True,
    )

    dataset_root = tmp_path / "gold" / "dataset=ticker_base_rates"
    assert not backup_root.is_relative_to(dataset_root)

    # Simulate Orion's exact walk and confirm the old row is gone from it.
    orion_visible = [f for f in dataset_root.rglob("*.parquet") if not f.name.startswith("._")]
    assert len(orion_visible) == 1
    assert pd.read_parquet(orion_visible[0])["ticker_alert_frequency"].iloc[0] != 999


def test_a_partition_with_no_surviving_rows_is_removed_entirely(script, tmp_path: Path) -> None:
    """10,678 of 10,822 (ticker, day) pairs in production have zero correct
    counterpart — most days a stale empty dt= dir would just be confusing."""
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    _write_old_partition(root, "2026-04-16", 3, "part-old.parquet")

    script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),  # nothing survives for 04-16
        backup_root=_backup_root(tmp_path),
        write=True,
    )

    assert (root / "dt=2026-04-15").exists()
    assert not (root / "dt=2026-04-16").exists()


def test_old_files_survive_across_multiple_fragments_in_one_partition(script, tmp_path: Path) -> None:
    """Production has partitions with up to 4 append-only fragments."""
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 2, "part-a.parquet", "part-b.parquet", "part-c.parquet")
    backup_root = _backup_root(tmp_path)

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=backup_root,
        write=True,
    )

    assert report.old_files_backed_up == 3
    assert len(pd.read_parquet(backup_root / "dt=2026-04-15")) == 6


def test_sidecars_and_partial_writes_are_backed_up_but_not_counted(script, tmp_path: Path) -> None:
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 2, "part-old.parquet")
    (root / "dt=2026-04-15" / "._part-old.parquet").write_bytes(b"appledouble junk")
    (root / "dt=2026-04-15" / "part-partial.parquet.tmp").write_bytes(b"half a write")

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=_backup_root(tmp_path),
        write=True,
    )

    assert report.old_files_backed_up == 1
    assert report.old_rows == 2


def test_a_second_rebuild_cannot_run_while_the_first_holds_the_lock(script, tmp_path: Path) -> None:
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    root.mkdir(parents=True)

    with script.dataset_lock(root, timeout=0.05):
        with pytest.raises(script.Timeout):
            with script.dataset_lock(root, timeout=0.05):
                pass


def test_new_output_schema_matches_expected_columns(script, tmp_path: Path) -> None:
    from heber.features.pipelines.ticker_base_rates import EXPECTED_OUTPUT_COLUMNS

    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 1, "part-old.parquet")

    script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=_backup_root(tmp_path),
        write=True,
    )

    live = pd.read_parquet(root / "dt=2026-04-15")
    assert sorted(live.columns) == sorted(EXPECTED_OUTPUT_COLUMNS)


def test_write_failure_aborts_before_any_old_data_is_touched(
    script, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the on-disk row count after writing doesn't match what was asked
    for, old (still-usable) data must not be removed on top of a bad write."""
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")

    real_write_new_partition = script._write_new_partition

    def truncating_write_new_partition(version_root, dt, group):
        # Simulate a write that silently drops rows.
        return real_write_new_partition(version_root, dt, group.iloc[0:0])

    monkeypatch.setattr(script, "_write_new_partition", truncating_write_new_partition)

    with pytest.raises(script.RowCountMismatch):
        script.rebuild(
            version_root=root,
            corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
            backup_root=_backup_root(tmp_path),
            write=True,
        )

    # Old data is untouched -- still exactly what it was before the run.
    still_there = pd.read_parquet(root / "dt=2026-04-15")
    assert len(still_there) == 5
    assert (still_there["ticker_alert_frequency"] == 999).all()


def test_cohort_mismatch_is_caught_before_main_calls_the_destructive_path(
    script, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--expect-old-rows/--expect-new-rows must gate BEFORE rebuild(write=True)
    is ever called, not just report failure after the fact."""
    reader, root = _reader_and_root(tmp_path)
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")

    calls: list[bool] = []
    real_rebuild = script.rebuild

    def spying_rebuild(*args, **kwargs):
        calls.append(kwargs.get("write", False))
        return real_rebuild(*args, **kwargs)

    monkeypatch.setattr(script, "rebuild", spying_rebuild)
    monkeypatch.setattr(
        script,
        "_load_corrected",
        lambda *a, **k: _corrected(("equity:AAPL", "2026-04-15", 0.4)),
    )
    monkeypatch.setattr(script, "HeberReader", lambda *a, **k: reader)
    monkeypatch.setattr(script.settings, "data_root", tmp_path)

    with pytest.raises(SystemExit) as exc_info:
        script.main(["--apply", "--project", "watch", "--version", "v1", "--expect-old-rows", "999999"])

    assert exc_info.value.code != 0
    # rebuild() was only ever called in plan mode (write=False) -- the
    # mismatch was caught before any destructive call was made.
    assert calls == [False]
    still_there = pd.read_parquet(root / "dt=2026-04-15")
    assert len(still_there) == 5
