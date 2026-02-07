from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from heber.watch import consumer as consumer_module
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.features import AlertFeatures, persist_features_to_gold


def _sample_features() -> AlertFeatures:
    return AlertFeatures(
        alert_id="a1",
        alert_time=datetime(2026, 2, 7, 15, 0, tzinfo=UTC),
        symbol="AAPL",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        strike=100.0,
        expiry=date(2026, 2, 20),
        put_call="C",
        days_to_expiry=13,
        premium=10000.0,
        volume=100.0,
        open_interest=200.0,
        volume_oi_ratio=0.5,
        alert_type="SWEEP",
        side="ask",
        aggressor="ask",
        spot_price=200.0,
        contract_price=1.25,
    )


def test_persist_features_to_gold_writes_partitioned_file(tmp_path) -> None:  # noqa: ANN001
    features = _sample_features()

    persist_features_to_gold(features, output_path=tmp_path)

    out_file = tmp_path / "dt=2026-02-07" / "data.parquet"
    assert out_file.exists()
    df = pl.read_parquet(out_file)
    assert df.get_column("alert_id").to_list() == ["a1"]


@pytest.mark.asyncio
async def test_consumer_extract_and_store_features_persists_to_gold(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = AlertWatchConsumer(
        redis_client=SimpleNamespace(),
        watch_manager=SimpleNamespace(),
    )

    mocked_features = _sample_features()
    consumer.feature_extractor.extract = AsyncMock(return_value=mocked_features)  # type: ignore[method-assign]
    persist_mock = MagicMock()
    monkeypatch.setattr(consumer_module, "persist_features_to_gold", persist_mock)

    alert = {
        "id": "a1",
        "underlying": "AAPL",
        "occ_symbol": "AAPL260220C00100000",
        "put_call": "C",
        "expiry": "2026-02-20",
        "strike": 100.0,
        "spot_px": 200.0,
        "contract_px": 1.25,
        "premium": 10000.0,
        "volume": 100.0,
        "open_interest": 200.0,
        "alert_type": "SWEEP",
        "ts_event": datetime(2026, 2, 7, 15, 0, tzinfo=UTC),
    }

    await consumer._extract_and_store_features(alert, watch_id="w1")

    persist_mock.assert_called_once_with(mocked_features)
