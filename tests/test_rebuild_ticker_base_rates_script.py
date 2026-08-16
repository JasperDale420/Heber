"""The ticker_base_rates rebuild must fully replace the dataset, never blend.

``write_gold`` only ever appends a uniquely-named part-file, so re-running the
now-fixed pipeline over history the normal way would leave the old
contaminated files sitting alongside the new clean ones — Orion's reader globs
every file under ``dataset=ticker_base_rates`` with no version pinning and no
underscore-directory exclusion, so it would union both. This script backs the
old files out to a location entirely outside the ``dataset=`` tree (so neither
Heber's reader nor Orion's can ever see them) before writing the replacement.
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


def test_dry_run_touches_nothing(script, tmp_path: Path) -> None:
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    before = (root / "dt=2026-04-15" / "part-old.parquet").read_bytes()
    reader = HeberReader(data_root=tmp_path)

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run",
        reader=reader,
        project="watch",
        version="v1",
        write=False,
    )

    assert report.old_rows == 5
    assert report.new_rows == 1
    assert (root / "dt=2026-04-15" / "part-old.parquet").read_bytes() == before
    assert not (tmp_path / "gold" / "_migrations").exists()


def test_apply_backs_up_old_and_writes_corrected(script, tmp_path: Path) -> None:
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    backup_root = tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run"
    reader = HeberReader(data_root=tmp_path)

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=backup_root,
        reader=reader,
        project="watch",
        version="v1",
        write=True,
    )

    assert (report.old_rows, report.new_rows, report.old_files_backed_up) == (5, 1, 1)

    live = pd.read_parquet(root / "dt=2026-04-15")
    assert len(live) == 1
    assert live["instrument_key"].iloc[0] == "equity:AAPL"
    assert live["ticker_win_rate_90d"].iloc[0] == 0.4

    backed_up = pd.read_parquet(backup_root / "dt=2026-04-15")
    assert len(backed_up) == 5
    assert (backed_up["ticker_alert_frequency"] == 999).all()


def test_backup_is_outside_the_dataset_tree_orion_globs(script, tmp_path: Path) -> None:
    """Orion's reader recursively globs everything under dataset=ticker_base_rates
    with no version or underscore-directory exclusion at all — so the backup
    must not be a descendant of it, or Orion would read the old rows anyway."""
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    backup_root = tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run"
    reader = HeberReader(data_root=tmp_path)

    script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=backup_root,
        reader=reader,
        project="watch",
        version="v1",
        write=True,
    )

    dataset_root = tmp_path / "gold" / "dataset=ticker_base_rates"
    assert not backup_root.is_relative_to(dataset_root)

    # Orion's reader has no version pinning and no underscore-directory
    # exclusion — it just globs every .parquet under dataset=ticker_base_rates.
    # Simulate exactly that walk and confirm the old row is gone from it.
    orion_visible = [f for f in dataset_root.rglob("*.parquet") if not f.name.startswith("._")]
    assert len(orion_visible) == 1
    assert pd.read_parquet(orion_visible[0])["ticker_alert_frequency"].iloc[0] != 999


def test_a_partition_with_no_surviving_rows_is_removed_entirely(script, tmp_path: Path) -> None:
    """10,678 of 10,822 (ticker, day) pairs in production have zero correct
    counterpart — most days a stale empty dt= dir would just be confusing."""
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 5, "part-old.parquet")
    _write_old_partition(root, "2026-04-16", 3, "part-old.parquet")

    script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),  # nothing survives for 04-16
        backup_root=tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run",
        reader=HeberReader(data_root=tmp_path),
        project="watch",
        version="v1",
        write=True,
    )

    assert (root / "dt=2026-04-15").exists()
    assert not (root / "dt=2026-04-16").exists()


def test_old_files_survive_across_multiple_fragments_in_one_partition(script, tmp_path: Path) -> None:
    """Production has partitions with up to 4 append-only fragments."""
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 2, "part-a.parquet", "part-b.parquet", "part-c.parquet")
    backup_root = tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run"

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=backup_root,
        reader=HeberReader(data_root=tmp_path),
        project="watch",
        version="v1",
        write=True,
    )

    assert report.old_files_backed_up == 3
    assert len(pd.read_parquet(backup_root / "dt=2026-04-15")) == 6


def test_sidecars_and_partial_writes_are_backed_up_but_not_counted(script, tmp_path: Path) -> None:
    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 2, "part-old.parquet")
    (root / "dt=2026-04-15" / "._part-old.parquet").write_bytes(b"appledouble junk")
    (root / "dt=2026-04-15" / "part-partial.parquet.tmp").write_bytes(b"half a write")

    report = script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run",
        reader=HeberReader(data_root=tmp_path),
        project="watch",
        version="v1",
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

    root = tmp_path / "gold" / "dataset=ticker_base_rates" / "project=watch" / "version=v1"
    _write_old_partition(root, "2026-04-15", 1, "part-old.parquet")

    script.rebuild(
        version_root=root,
        corrected=_corrected(("equity:AAPL", "2026-04-15", 0.4)),
        backup_root=tmp_path / "gold" / "_migrations" / "ticker_base_rates_rebuild" / "run",
        reader=HeberReader(data_root=tmp_path),
        project="watch",
        version="v1",
        write=True,
    )

    live = pd.read_parquet(root / "dt=2026-04-15")
    assert sorted(live.columns) == sorted(EXPECTED_OUTPUT_COLUMNS)
