"""Tests for the fail-loud Greek-null guard in persist_features_to_gold.

The guard prevents the documented gateway-401 cascade: when enrichment
returned 401, every Greek column landed as null and the audit only warned.
Now those rows are diverted to a quarantine partition that downstream
readers (which only scan ``dt=`` partitions) never load.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.dataset as pads
import pytest

from heber.ml.datasets import (
    _REQUIRED_GREEK_COLUMNS,
    persist_features_to_gold,
)


def _make_row(*, alert_id: str, greeks_present: bool) -> dict:
    base = {
        "alert_id": alert_id,
        "alert_time": datetime(2026, 3, 20, 16, 5, tzinfo=UTC),
        "symbol": "AAPL",
        "underlying": "AAPL",
        "strike": 200.0,
        "put_call": "C",
        "premium": 12000.0,
        "volume": 100.0,
        "iv_rank": 50.0 if greeks_present else None,
        "gex": 1.5 if greeks_present else None,
        "vex": 0.5 if greeks_present else None,
        "max_pain_strike": 195.0,
        "max_pain_distance_pct": 2.0,
        "market_tide_net_premium": 100.0,
        "market_tide_direction": "bullish",
    }
    for col in _REQUIRED_GREEK_COLUMNS:
        base[col] = 0.1 if greeks_present else None
    return base


def _load_parquet_files(directory: Path) -> pd.DataFrame:
    """Read every *.parquet file in a directory (ignoring lock/tmp/sidecars)."""
    if not directory.exists():
        return pd.DataFrame()
    files = sorted(p for p in directory.glob("*.parquet") if not p.name.startswith("."))
    if not files:
        return pd.DataFrame()
    ds = pads.dataset([str(p) for p in files], format="parquet")
    return ds.to_table().to_pandas()


def _load_canonical_partition(out_path: Path, dt_str: str) -> pd.DataFrame:
    return _load_parquet_files(out_path / f"dt={dt_str}")


def _load_quarantine_partition(out_path: Path, dt_str: str) -> pd.DataFrame:
    return _load_parquet_files(out_path / "_quarantine" / "all_greeks_null" / f"dt={dt_str}")


class TestGreeksNullGuard:
    def test_rows_with_all_greeks_null_are_quarantined(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                _make_row(alert_id="good-1", greeks_present=True),
                _make_row(alert_id="bad-1", greeks_present=False),
                _make_row(alert_id="bad-2", greeks_present=False),
            ]
        )
        persist_features_to_gold(df, tmp_path)

        canonical = _load_canonical_partition(tmp_path, "2026-03-20")
        assert sorted(canonical["alert_id"].tolist()) == ["good-1"], (
            "Only the row with Greek enrichment should reach the canonical partition"
        )

        quarantined = _load_quarantine_partition(tmp_path, "2026-03-20")
        assert sorted(quarantined["alert_id"].tolist()) == ["bad-1", "bad-2"]

    def test_partition_with_zero_clean_rows_skips_canonical_write(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                _make_row(alert_id="bad-1", greeks_present=False),
                _make_row(alert_id="bad-2", greeks_present=False),
            ]
        )
        persist_features_to_gold(df, tmp_path)

        canonical = tmp_path / "dt=2026-03-20"
        # Should NOT create the canonical partition directory at all.
        assert not canonical.exists()

        quarantined = _load_quarantine_partition(tmp_path, "2026-03-20")
        assert len(quarantined) == 2

    def test_rows_with_one_greek_present_are_kept(self, tmp_path: Path) -> None:
        """Single-Greek presence (e.g. delta only) keeps the row in canonical.

        Some upstream feeds omit individual Greeks; the guard only triggers
        when ALL configured Greeks are null.
        """
        row = _make_row(alert_id="partial-1", greeks_present=False)
        row["delta"] = 0.42  # Only delta survived enrichment.
        persist_features_to_gold(pd.DataFrame([row]), tmp_path)

        canonical = _load_canonical_partition(tmp_path, "2026-03-20")
        assert canonical["alert_id"].tolist() == ["partial-1"]

        quarantined = _load_quarantine_partition(tmp_path, "2026-03-20")
        assert quarantined.empty

    def test_empty_dataframe_is_noop(self, tmp_path: Path) -> None:
        persist_features_to_gold(pd.DataFrame(), tmp_path)
        assert list(tmp_path.iterdir()) == []

    def test_emits_error_log_on_quarantine(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from heber.ml import datasets as datasets_module

        error_calls: list[tuple[str, dict]] = []

        def _capture_error(message: str, **kwargs: object) -> None:
            error_calls.append((message, kwargs))

        monkeypatch.setattr(datasets_module.logger, "error", _capture_error)
        monkeypatch.setattr(datasets_module.logger, "warning", lambda *a, **k: None)
        monkeypatch.setattr(datasets_module.logger, "info", lambda *a, **k: None)

        df = pd.DataFrame([_make_row(alert_id="bad-1", greeks_present=False)])
        persist_features_to_gold(df, tmp_path)

        quarantine_logs = [c for c in error_calls if "quarantined" in c[0]]
        assert quarantine_logs, "Quarantine should emit an ERROR log"
        _, fields = quarantine_logs[0]
        assert fields["rows"] == 1
        assert fields["date"] == "2026-03-20"
        assert fields["affected_columns"] == list(_REQUIRED_GREEK_COLUMNS)
