from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from filelock import Timeout

from heber.ml.datasets import persist_features_to_gold as persist_feature_frame_to_gold
from heber.watch import consumer as consumer_module
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.features import PENDING_FEATURES_KEY, AlertFeatures, persist_features_to_gold
from heber.watch.writer import WatchService


class _PendingFeatureRedis:
    """Small bytes-mode Redis fake for the pending-feature hash."""

    def __init__(self) -> None:
        self.pending: dict[bytes, bytes] = {}
        self.fail_hdel = False

    def set(self, *_args, **_kwargs) -> bool:  # noqa: ANN002, ANN003
        return True

    def hset(self, name: str, key: str, value: str) -> int:
        assert name == PENDING_FEATURES_KEY
        encoded_key = key.encode()
        is_new = encoded_key not in self.pending
        self.pending[encoded_key] = value.encode()
        return int(is_new)

    def hgetall(self, name: str) -> dict[bytes, bytes]:
        assert name == PENDING_FEATURES_KEY
        return dict(self.pending)

    def hdel(self, name: str, key: str) -> int:
        assert name == PENDING_FEATURES_KEY
        if self.fail_hdel:
            raise ConnectionError("simulated clear failure")
        return int(self.pending.pop(key.encode(), None) is not None)


def _sample_features() -> AlertFeatures:
    # Populate Greeks so the row passes the fail-loud guard in
    # persist_features_to_gold (which quarantines rows where ALL of
    # delta/gamma/theta/vega/iv are null — the documented gateway-401
    # cascade fingerprint).
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
        delta=0.55,
        gamma=0.02,
        theta=-0.05,
        vega=0.10,
        iv=0.30,
    )


def _sample_alert() -> dict:
    return {
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


def test_persist_features_to_gold_writes_partitioned_file(tmp_path) -> None:  # noqa: ANN001
    features = _sample_features()

    persist_features_to_gold(features, output_path=tmp_path)

    out_file = tmp_path / "dt=2026-02-07" / "data.parquet"
    assert out_file.exists()
    df = pd.read_parquet(out_file)
    assert df["alert_id"].tolist() == ["a1"]


def test_live_watch_can_surface_lock_timeout_without_changing_bulk_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:  # noqa: ANN001
    """Only the strict live path converts a skipped lock into a failed write."""

    class _AlwaysTimesOut:
        def __enter__(self):  # noqa: ANN204
            raise Timeout("simulated.lock")

        def __exit__(self, *_args):  # noqa: ANN002, ANN204
            return False

    monkeypatch.setattr("filelock.FileLock", lambda *_args, **_kwargs: _AlwaysTimesOut())
    frame = pd.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "delta": [0.5],
            "gamma": [0.02],
            "theta": [-0.05],
            "vega": [0.1],
            "iv": [0.3],
        }
    )

    # Historical batch callers deliberately retain the best-effort timeout.
    persist_feature_frame_to_gold(frame, tmp_path)

    # The live wrapper opts into a failure signal so it retains its pending hash field.
    with pytest.raises(OSError, match="2026-02-07"):
        persist_feature_frame_to_gold(frame, tmp_path, raise_on_lock_timeout=True)


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


@pytest.mark.asyncio
async def test_gold_write_failure_survives_restart_and_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """An I/O failure after watch creation must leave a self-contained retry row."""
    redis_client = _PendingFeatureRedis()
    first_consumer = AlertWatchConsumer(redis_client=redis_client, watch_manager=SimpleNamespace())
    first_consumer.feature_extractor.extract = AsyncMock(return_value=_sample_features())  # type: ignore[method-assign]

    persist_mock = MagicMock(side_effect=[OSError("disk full"), None])
    monkeypatch.setattr(consumer_module, "persist_features_to_gold", persist_mock)

    # Persistence still does not fail the already-created watch path.
    await first_consumer._extract_and_store_features(_sample_alert(), watch_id="w1")

    assert list(redis_client.pending) == [b"a1"]

    # A new consumer instance models a service restart: no in-memory state from
    # the failed attempt is needed to reconstruct and persist the feature row.
    restarted_consumer = AlertWatchConsumer(redis_client=redis_client, watch_manager=SimpleNamespace())
    recovered = await restarted_consumer.retry_pending_feature_writes()

    assert recovered == 1
    assert redis_client.pending == {}
    assert persist_mock.call_count == 2
    retried_features = persist_mock.call_args_list[1].args[0]
    assert isinstance(retried_features, AlertFeatures)
    assert retried_features.alert_id == "a1"


@pytest.mark.asyncio
async def test_successful_gold_write_with_failed_marker_clear_is_safe_to_replay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A crash-equivalent clear failure retains an idempotent retry obligation."""
    redis_client = _PendingFeatureRedis()
    redis_client.fail_hdel = True
    first_consumer = AlertWatchConsumer(redis_client=redis_client, watch_manager=SimpleNamespace())
    first_consumer.feature_extractor.extract = AsyncMock(return_value=_sample_features())  # type: ignore[method-assign]
    monkeypatch.setattr(
        consumer_module,
        "persist_features_to_gold",
        lambda features: persist_features_to_gold(features, output_path=tmp_path),
    )

    await first_consumer._extract_and_store_features(_sample_alert(), watch_id="w1")

    assert list(redis_client.pending) == [b"a1"]

    redis_client.fail_hdel = False
    restarted_consumer = AlertWatchConsumer(redis_client=redis_client, watch_manager=SimpleNamespace())
    recovered = await restarted_consumer.retry_pending_feature_writes()

    assert recovered == 1
    assert redis_client.pending == {}
    persisted = pd.read_parquet(tmp_path / "dt=2026-02-07" / "data.parquet")
    assert persisted["alert_id"].tolist() == ["a1"]


@pytest.mark.asyncio
async def test_extraction_failure_remains_non_blocking_and_creates_no_retry_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client = _PendingFeatureRedis()
    consumer = AlertWatchConsumer(redis_client=redis_client, watch_manager=SimpleNamespace())
    consumer.feature_extractor.extract = AsyncMock(side_effect=ValueError("malformed enrichment"))  # type: ignore[method-assign]
    persist_mock = MagicMock()
    monkeypatch.setattr(consumer_module, "persist_features_to_gold", persist_mock)

    await consumer._extract_and_store_features(_sample_alert(), watch_id="w1")

    assert redis_client.pending == {}
    persist_mock.assert_not_called()


@pytest.mark.asyncio
async def test_service_tick_retries_features_without_blocking_pending_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two durability lanes fail independently on the shared service tick."""
    service = WatchService.__new__(WatchService)
    service._running = True
    service.consumer = SimpleNamespace(
        retry_pending_feature_writes=AsyncMock(side_effect=ConnectionError("redis read failed"))
    )
    service.checker = SimpleNamespace(check_all=MagicMock())
    service.manager = SimpleNamespace(get_pending_outcomes=MagicMock(return_value=[]))
    service.writer = MagicMock()

    async def _stop_after_first_tick(_seconds: float) -> None:
        service._running = False

    monkeypatch.setattr(asyncio, "sleep", _stop_after_first_tick)

    await service._check_and_write_loop()

    service.consumer.retry_pending_feature_writes.assert_awaited_once()
    service.checker.check_all.assert_called_once()


@pytest.mark.asyncio
async def test_consumer_rejects_junk_suffixed_expiry_instead_of_truncating(monkeypatch: pytest.MonkeyPatch) -> None:
    """A malformed raw expiry must not be silently truncated into a fake clean date.

    The old conversion (``str(alert["expiry"])[:10]``) sliced off anything past
    the 10th character, so "2026-02-20junk" parsed as a valid 2026-02-20 with
    no trace anything was wrong. It must now be rejected and the row skipped,
    same as any other malformed feature-extraction input.
    """
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
        "expiry": "2026-02-20junk",
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

    consumer.feature_extractor.extract.assert_not_called()
    persist_mock.assert_not_called()
