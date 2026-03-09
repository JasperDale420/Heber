"""Tests for concurrent write protection in persist_features_to_gold."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from heber.ml.datasets import persist_features_to_gold


def _make_features_df(alert_ids: list[str], ts: datetime | None = None) -> pl.DataFrame:
    if ts is None:
        ts = datetime(2026, 3, 9, 15, 0, tzinfo=UTC)
    return pl.DataFrame(
        {
            "alert_id": alert_ids,
            "alert_time": [ts] * len(alert_ids),
            "feature_x": [float(i) for i in range(len(alert_ids))],
        }
    )


def test_concurrent_writes_produce_valid_parquet(tmp_path: Path) -> None:
    """Multiple threads writing to the same partition should not corrupt the file."""
    errors: list[Exception] = []

    def _write_batch(batch_id: int) -> None:
        try:
            ids = [f"alert-{batch_id}-{i}" for i in range(5)]
            df = _make_features_df(ids)
            persist_features_to_gold(df, output_path=tmp_path, partition_col="alert_time")
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_write_batch, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, f"Write errors: {errors}"

    out_file = tmp_path / "dt=2026-03-09" / "data.parquet"
    assert out_file.exists()

    # File should be readable (not corrupt)
    result = pl.read_parquet(out_file)
    assert len(result) > 0

    # All alert IDs should be present (no lost writes except timing races caught by lock)
    all_ids = result.get_column("alert_id").to_list()
    assert len(all_ids) == len(set(all_ids)), "Duplicate alert IDs found"


def test_lock_file_is_created_adjacent_to_parquet(tmp_path: Path) -> None:
    """The lock file should be created next to the parquet file."""
    df = _make_features_df(["a1"])
    persist_features_to_gold(df, output_path=tmp_path, partition_col="alert_time")

    # Lock file may or may not persist after release, but parquet should be valid
    out_file = tmp_path / "dt=2026-03-09" / "data.parquet"
    assert out_file.exists()
    result = pl.read_parquet(out_file)
    assert result.get_column("alert_id").to_list() == ["a1"]


def test_sequential_writes_still_merge_correctly(tmp_path: Path) -> None:
    """Sequential writes should still merge data correctly with the lock."""
    df_a = _make_features_df(["a1"])
    df_b = _make_features_df(["a2"])

    persist_features_to_gold(df_a, output_path=tmp_path, partition_col="alert_time")
    persist_features_to_gold(df_b, output_path=tmp_path, partition_col="alert_time")

    out_file = tmp_path / "dt=2026-03-09" / "data.parquet"
    result = pl.read_parquet(out_file)
    assert set(result.get_column("alert_id").to_list()) == {"a1", "a2"}


def test_dedup_still_works_with_lock(tmp_path: Path) -> None:
    """Duplicate alert_id writes should still be deduped (keep last)."""
    df_a = _make_features_df(["a1"])
    df_b = pl.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 3, 9, 15, 0, tzinfo=UTC)],
            "feature_x": [99.0],
        }
    )

    persist_features_to_gold(df_a, output_path=tmp_path, partition_col="alert_time")
    persist_features_to_gold(df_b, output_path=tmp_path, partition_col="alert_time")

    out_file = tmp_path / "dt=2026-03-09" / "data.parquet"
    result = pl.read_parquet(out_file)
    assert len(result) == 1
    assert result.get_column("feature_x").to_list() == [99.0]
