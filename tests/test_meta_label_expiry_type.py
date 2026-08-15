"""Gold ``meta_label_features`` must write ``expiry`` as Arrow ``date32`` everywhere.

Producers historically wrote ``expiry`` as ``date32``, ``int64`` or ``string``
depending on which path created the row. A full-dataset read of the mixed
partitions raises ``SchemaConflictError`` (string vs numeric cannot be unified
without silently changing the values), so every write path must funnel through
one coercion.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.ml.datasets import persist_features_to_gold

ALERT_TIME = datetime(2026, 2, 17, 15, 0, tzinfo=UTC)


def _feature_row(alert_id: str, expiry: object) -> dict:
    """Minimal Gold feature row. ``delta`` is set so the row is not quarantined."""
    return {
        "alert_id": alert_id,
        "alert_time": ALERT_TIME,
        "symbol": "AAPL",
        "underlying": "AAPL",
        "occ_symbol": "AAPL260320C00200000",
        "instrument_key": "option:AAPL260320C00200000",
        "strike": 200.0,
        "put_call": "C",
        "expiry": expiry,
        "delta": 0.55,
        "ts_event": ALERT_TIME,
        "ts_available": ALERT_TIME,
    }


def _written_expiry_type(output_path: Path) -> pa.DataType:
    files = sorted(output_path.glob("dt=*/*.parquet"))
    assert files, f"no partition written under {output_path}"
    assert len(files) == 1, f"expected one partition file, got {files}"
    return pq.read_schema(files[0]).field("expiry").type


def _written_frame(output_path: Path) -> pd.DataFrame:
    files = sorted(output_path.glob("dt=*/*.parquet"))
    return pd.read_parquet(files[0])


class TestPersistFunnelExpiryType:
    """The shared write funnel coerces every accepted expiry form to date32."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("2026-03-20", date(2026, 3, 20)),
            ("20260320", date(2026, 3, 20)),
            (20260320, date(2026, 3, 20)),
            (date(2026, 3, 20), date(2026, 3, 20)),
            (datetime(2026, 3, 20, 16, 0, tzinfo=UTC), date(2026, 3, 20)),
        ],
    )
    def test_expiry_written_as_date32(self, tmp_path: Path, raw: object, expected: date) -> None:
        persist_features_to_gold(pd.DataFrame([_feature_row("a1", raw)]), tmp_path)

        assert _written_expiry_type(tmp_path) == pa.date32()
        assert _written_frame(tmp_path)["expiry"].iloc[0] == expected

    def test_unparseable_expiry_becomes_null_not_string(self, tmp_path: Path) -> None:
        persist_features_to_gold(pd.DataFrame([_feature_row("a1", "not-a-date")]), tmp_path)

        assert _written_expiry_type(tmp_path) == pa.date32()
        assert pd.isna(_written_frame(tmp_path)["expiry"].iloc[0])

    def test_all_null_expiry_column_still_typed_date32(self, tmp_path: Path) -> None:
        """An all-null partition must not fall back to Arrow ``null`` type."""
        persist_features_to_gold(pd.DataFrame([_feature_row("a1", None)]), tmp_path)

        assert _written_expiry_type(tmp_path) == pa.date32()

    def test_quarantined_rows_written_as_date32(self, tmp_path: Path) -> None:
        """Quarantined rows share the dataset root, so they share the contract.

        ``HeberReader`` walks the whole ``version=`` root — the ``_quarantine``
        subtree is not excluded — so a string-typed expiry there recreates the
        very conflict this coercion exists to prevent.
        """
        row = _feature_row("all-greeks-null", "2026-03-20")
        row["delta"] = None

        persist_features_to_gold(pd.DataFrame([row]), tmp_path)

        quarantined = sorted(tmp_path.glob("_quarantine/*/dt=*/*.parquet"))
        assert len(quarantined) == 1, f"expected one quarantine file, got {quarantined}"
        assert pq.read_schema(quarantined[0]).field("expiry").type == pa.date32()

    def test_mixed_expiry_types_in_one_frame_write_as_date32(self, tmp_path: Path) -> None:
        frame = pd.DataFrame(
            [
                _feature_row("a1", "2026-03-20"),
                _feature_row("a2", 20260417),
                _feature_row("a3", date(2026, 5, 15)),
            ]
        )

        persist_features_to_gold(frame, tmp_path)

        assert _written_expiry_type(tmp_path) == pa.date32()
        assert sorted(_written_frame(tmp_path)["expiry"]) == [
            date(2026, 3, 20),
            date(2026, 4, 17),
            date(2026, 5, 15),
        ]

    def test_append_normalizes_existing_string_typed_partition(self, tmp_path: Path) -> None:
        """Appending to a legacy string-typed partition rewrites it as date32."""
        partition = tmp_path / "dt=2026-02-17"
        partition.mkdir(parents=True)
        legacy = pd.DataFrame([_feature_row("legacy", "2026-03-20")])
        legacy.to_parquet(partition / "data.parquet", index=False)
        assert pq.read_schema(partition / "data.parquet").field("expiry").type == pa.string()

        persist_features_to_gold(pd.DataFrame([_feature_row("new", date(2026, 4, 17))]), tmp_path)

        assert _written_expiry_type(tmp_path) == pa.date32()
        written = _written_frame(tmp_path).set_index("alert_id")["expiry"]
        assert written["legacy"] == date(2026, 3, 20)
        assert written["new"] == date(2026, 4, 17)


class TestProducerPaths:
    """Each producer that reaches Gold writes date32, whatever it holds in memory."""

    def test_live_watch_path_writes_date32(self, tmp_path: Path) -> None:
        from heber.watch.features import AlertFeatures
        from heber.watch.features import persist_features_to_gold as persist_live

        features = AlertFeatures(
            alert_id="live-1",
            alert_time=ALERT_TIME,
            symbol="AAPL",
            occ_symbol="AAPL260320C00200000",
            underlying="AAPL",
            strike=200.0,
            expiry="2026-03-20",  # type: ignore[arg-type]  # upstream sometimes hands us a string
            put_call="C",
            days_to_expiry=31,
            premium=5000.0,
            volume=100.0,
            open_interest=500.0,
            volume_oi_ratio=0.2,
            alert_type="SWEEP",
            side="ask",
            aggressor="buyer",
            spot_price=195.0,
            contract_price=8.5,
            delta=0.55,
        )

        persist_live(features, output_path=tmp_path)

        assert _written_expiry_type(tmp_path) == pa.date32()
        assert _written_frame(tmp_path)["expiry"].iloc[0] == date(2026, 3, 20)

    def test_backfill_path_writes_date32(self, tmp_path: Path) -> None:
        from heber.watch.backfill_scanner import EnrichmentBackfillScanner

        scanner = EnrichmentBackfillScanner(
            feature_extractor=AsyncMock(),
            features_output_path=tmp_path,
            calendar=MagicMock(),
        )

        scanner._patch_partition(_feature_row("backfill-1", "20260320"))

        assert _written_expiry_type(tmp_path) == pa.date32()
        assert _written_frame(tmp_path)["expiry"].iloc[0] == date(2026, 3, 20)
