"""Tests for scripts/repair_gold_expiry.py.

The script rewrites Gold training data in place, so its safety properties matter
as much as its repair logic: it must never publish while another writer holds
the partition lock, and must never leave a failed repair as the live file.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "repair_gold_expiry.py"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("repair_gold_expiry", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["repair_gold_expiry"] = module
    spec.loader.exec_module(module)
    return module


repair_gold_expiry = _load_script()


def _write_partition(path: Path, rows: list[dict]) -> Path:
    """Write a partition whose expiry column is date32, bad day counts included."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "alert_id": pa.array([r["alert_id"] for r in rows], type=pa.string()),
            "instrument_key": pa.array([r["instrument_key"] for r in rows], type=pa.string()),
            "occ_symbol": pa.array([r.get("occ_symbol") for r in rows], type=pa.string()),
            "alert_time": pa.array([r["alert_time"] for r in rows], type=pa.timestamp("ns", tz="UTC")),
            # Raw day counts, cast without conversion — this is how the bad rows exist on disk.
            "expiry": pa.array([r["expiry_days"] for r in rows], type=pa.int32()).cast(pa.date32()),
        }
    )
    pq.write_table(table, path, compression="snappy")
    return path


def _row(alert_id: str, occ: str, expiry_days: int) -> dict:
    return {
        "alert_id": alert_id,
        "instrument_key": f"option:{occ}",
        "occ_symbol": occ,
        "alert_time": datetime(2026, 4, 14, 13, 30, tzinfo=UTC),
        "expiry_days": expiry_days,
    }


@pytest.fixture
def partition(tmp_path: Path) -> Path:
    """A partition with one good row and one YYYYMMDD-as-day-count row."""
    return _write_partition(
        tmp_path / "dt=2026-04-14" / "data.parquet",
        [
            _row("good-1", "AAPL260417C00200000", (date(2026, 4, 17) - date(1970, 1, 1)).days),
            _row("bad-1", "VIX260819C00022000", 20260819),
        ],
    )


@pytest.mark.unit
class TestOccExpiry:
    def test_parses_expiry_from_occ_symbol(self) -> None:
        assert repair_gold_expiry.occ_expiry("option:VIX260819C00022000", None) == date(2026, 8, 19)
        assert repair_gold_expiry.occ_expiry(None, "SPXW260511C07400000") == date(2026, 5, 11)

    def test_returns_none_for_non_occ_keys(self) -> None:
        assert repair_gold_expiry.occ_expiry("equity:AAPL", None) is None
        assert repair_gold_expiry.occ_expiry(None, None) is None


@pytest.mark.integration
class TestRepairPartition:
    def test_unreadable_partition_becomes_readable(self, partition: Path) -> None:
        with pytest.raises(ValueError, match="year must be in"):
            pd.read_parquet(partition)

        result = repair_gold_expiry.repair_partition(partition, write=True, backup_dir=None)

        assert result["status"] == "repaired"
        frame = pd.read_parquet(partition).set_index("alert_id")
        assert frame.loc["bad-1", "expiry"] == date(2026, 8, 19)
        assert frame.loc["good-1", "expiry"] == date(2026, 4, 17)

    def test_dry_run_leaves_the_file_untouched(self, partition: Path) -> None:
        before = repair_gold_expiry.sha256(partition)

        result = repair_gold_expiry.repair_partition(partition, write=False, backup_dir=None)

        assert result["status"] == "would_repair"
        assert repair_gold_expiry.sha256(partition) == before

    def test_refuses_to_guess_when_the_occ_symbol_disagrees(self, tmp_path: Path) -> None:
        """A bad value whose OCC symbol says something else is not repairable."""
        path = _write_partition(
            tmp_path / "dt=2026-04-14" / "data.parquet",
            [_row("bad-1", "VIX260101C00022000", 20260819)],
        )
        before = repair_gold_expiry.sha256(path)

        result = repair_gold_expiry.repair_partition(path, write=True, backup_dir=None)

        assert result["status"] == "unverified"
        assert "OCC symbol says 2026-01-01" in result["problems"][0]
        assert repair_gold_expiry.sha256(path) == before, "an unverified partition must not be rewritten"

    def test_does_not_touch_the_file_while_another_writer_holds_the_lock(self, partition: Path) -> None:
        """The lock is taken before the file is read, so a concurrent append
        cannot be overwritten by a stale in-memory table."""
        from filelock import FileLock

        before = repair_gold_expiry.sha256(partition)
        with FileLock(str(partition.with_suffix(".parquet.lock")), timeout=5):
            result = repair_gold_expiry.repair_partition(partition, write=True, backup_dir=None, lock_timeout=0.5)

        assert result["status"] == "locked"
        assert repair_gold_expiry.sha256(partition) == before

    def test_failed_verification_restores_the_original(self, partition: Path, tmp_path: Path, monkeypatch) -> None:
        before = repair_gold_expiry.sha256(partition)
        calls = {"n": 0}
        real_verify = repair_gold_expiry.verify

        def fail_after_publish(original, repaired, out_file):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            # First call validates the staged file; fail only the post-publish check.
            return real_verify(original, repaired, out_file) if calls["n"] == 1 else ["injected failure"]

        monkeypatch.setattr(repair_gold_expiry, "verify", fail_after_publish)

        result = repair_gold_expiry.repair_partition(partition, write=True, backup_dir=tmp_path / "backups")

        assert result["status"] == "verify_failed"
        assert "original restored from backup" in result["problems"]
        assert repair_gold_expiry.sha256(partition) == before, "the failed repair must not stay live"
