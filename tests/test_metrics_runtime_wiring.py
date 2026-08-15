from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.models.envelope import EventEnvelope
from heber.ops import metrics as metrics_module
from heber.writer import bronze as bronze_module
from heber.writer import compactor as compactor_module
from heber.writer import consumer as consumer_module
from heber.writer import silver as silver_module
from heber.writer import utils as writer_utils_module


def _build_envelope() -> EventEnvelope:
    now = datetime(2026, 2, 7, 14, 0, tzinfo=UTC)
    return EventEnvelope(
        event_id="evt-1",
        provider="alpaca",
        feed="bars",
        source="websocket",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
        ts_event=now,
        ts_ingest=now,
        ts_available=now,
        payload={
            "t": now.isoformat(),
            "o": "100.0",
            "h": "101.0",
            "l": "99.0",
            "c": "100.5",
            "v": "1000",
            "n": "10",
            "vw": "100.3",
        },
    )


def test_consumer_process_records_core_metrics(monkeypatch) -> None:
    consumer = consumer_module.EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    calls: dict[str, list[dict[str, object]]] = {"received": [], "processed": [], "latency": []}

    monkeypatch.setattr(
        consumer_module,
        "record_event_received",
        lambda **kwargs: calls["received"].append(kwargs),
    )
    monkeypatch.setattr(
        consumer_module,
        "record_event_processed",
        lambda **kwargs: calls["processed"].append(kwargs),
    )
    monkeypatch.setattr(
        consumer_module,
        "record_ingest_latency",
        lambda **kwargs: calls["latency"].append(kwargs),
    )

    payload = _build_envelope().model_dump(mode="json")
    event_data = {b"data": json.dumps(payload).encode("utf-8")}

    success, error, retryable = consumer._process_event_once(event_data)

    assert success is True
    assert error is None
    assert retryable is True
    assert calls["received"] == [{"feed": "bars", "provider": "alpaca"}]
    assert calls["processed"] == [{"feed": "bars", "provider": "alpaca", "status": "success"}]
    assert len(calls["latency"]) == 1


@pytest.mark.asyncio
async def test_consumer_iteration_records_batch_size_metric(monkeypatch) -> None:
    consumer = consumer_module.EventConsumer()
    consumer.redis = AsyncMock()
    consumer.redis.xreadgroup.return_value = [(b"heber:events", [(b"1-0", {}), (b"2-0", {})])]
    consumer.redis.xack = AsyncMock()
    consumer._process_stream_messages = AsyncMock(return_value=(["1-0", "2-0"], []))
    consumer._flush_layers = MagicMock()

    batch_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        consumer_module,
        "record_batch_processed",
        lambda **kwargs: batch_calls.append(kwargs),
    )

    await consumer._consume_iteration()

    assert batch_calls == [{"feed": "mixed", "batch_size": 2}]
    consumer.redis.xack.assert_awaited_once()


def test_silver_flush_records_write_metrics(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(silver_module.settings, "data_root", tmp_path)

    write_calls: list[dict[str, object]] = []
    error_calls: list[dict[str, object]] = []
    # The single record_write for a Silver file lives in write_silver_parquet.
    # SilverWriter._flush_partition used to record it a second time, inflating
    # every Silver files/rows/bytes metric 2x — patch the real call site so the
    # exactly-once assertion below is a genuine guard against that returning.
    monkeypatch.setattr(writer_utils_module, "record_write", lambda **kwargs: write_calls.append(kwargs))
    monkeypatch.setattr(silver_module, "record_write_error", lambda **kwargs: error_calls.append(kwargs))

    writer = silver_module.SilverWriter()
    writer.write(_build_envelope())
    writer.flush()

    assert len(write_calls) == 1
    assert write_calls[0]["layer"] == "silver"
    assert write_calls[0]["dataset"] == "bars"
    assert write_calls[0]["rows"] == 1
    assert write_calls[0]["bytes_written"] > 0
    assert error_calls == []


def test_compactor_records_success_metrics(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(compactor_module.settings, "data_root", tmp_path)
    partition_path = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-07"
    partition_path.mkdir(parents=True, exist_ok=True)

    sample = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    pq.write_table(sample, partition_path / "part-1.parquet")
    pq.write_table(sample, partition_path / "part-2.parquet")

    compaction_calls: list[dict[str, object]] = []
    monkeypatch.setattr(compactor_module, "record_compaction", lambda **kwargs: compaction_calls.append(kwargs))

    compactor = compactor_module.Compactor()
    merged = compactor.compact_partition(partition_path)

    assert merged == 2
    assert len(compaction_calls) == 1
    assert compaction_calls[0]["dataset"] == "bars"
    assert compaction_calls[0]["status"] == "success"
    assert compaction_calls[0]["files_merged"] == 2


def test_bronze_flush_records_write_metrics(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bronze_module.settings, "data_root", tmp_path)

    write_calls: list[dict[str, object]] = []
    error_calls: list[dict[str, object]] = []
    monkeypatch.setattr(bronze_module, "record_write", lambda **kwargs: write_calls.append(kwargs))
    monkeypatch.setattr(bronze_module, "record_write_error", lambda **kwargs: error_calls.append(kwargs))

    writer = bronze_module.BronzeWriter()
    writer.write(_build_envelope())
    writer.flush()

    assert len(write_calls) == 1
    assert write_calls[0]["layer"] == "bronze"
    assert write_calls[0]["dataset"] == "bars"
    assert write_calls[0]["rows"] == 1
    assert write_calls[0]["bytes_written"] > 0
    assert error_calls == []


def test_record_write_updates_last_write_timestamp_metric() -> None:
    dataset = "slice1_writer_last_write_test"
    metrics_module.record_write(
        layer="silver",
        dataset=dataset,
        rows=1,
        bytes_written=128,
        duration_seconds=0.01,
    )

    observed = metrics_module.writer_last_write_unixtime.labels(layer="silver", dataset=dataset)._value.get()
    assert observed > 0
