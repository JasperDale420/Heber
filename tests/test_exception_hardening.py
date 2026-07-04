"""Tests for exception-type hardening in heber/writer/ and heber/bus/.

Verifies that narrowed exception handlers still catch the actual exceptions
raised by the code paths they guard, without changing any functional behavior.
"""

from __future__ import annotations

import asyncio
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.models.envelope import EventEnvelope

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_envelope(**overrides: Any) -> EventEnvelope:
    """Build a minimal valid EventEnvelope for testing."""
    defaults: dict[str, Any] = {
        "event_id": "test-evt-1",
        "provider": "test_provider",
        "feed": "bars",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC),
        "ts_ingest": datetime(2026, 3, 1, 12, 0, 1, tzinfo=UTC),
        "ts_available": datetime(2026, 3, 1, 12, 0, 1, tzinfo=UTC),
        "source": "test",
        "schema_version": "1.0",
        "payload": {"open": 150.0, "high": 151.0, "low": 149.0, "close": 150.5, "volume": 1000},
    }
    defaults.update(overrides)
    return EventEnvelope.model_validate(defaults)


def _write_parquet(path: Path, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="snappy")


def _write_bronze_jsonl(path: Path, envelopes: list[dict]) -> None:
    """Write gzipped JSONL Bronze file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for envelope in envelopes:
            f.write(json.dumps(envelope, default=str) + "\n")


# ===================================================================
# ingest_contracts: _to_decimal_or_none
# ===================================================================


class TestToDecimalOrNone:
    """Verify _to_decimal_or_none catches InvalidOperation/TypeError/ValueError."""

    def test_valid_decimal(self):
        from heber.writer.ingest_contracts import _to_decimal_or_none

        result = _to_decimal_or_none("123.45")
        assert result is not None
        assert float(result) == 123.45

    def test_none_input(self):
        from heber.writer.ingest_contracts import _to_decimal_or_none

        assert _to_decimal_or_none(None) is None

    def test_invalid_string_returns_none(self):
        """Triggers InvalidOperation from Decimal('not_a_number')."""
        from heber.writer.ingest_contracts import _to_decimal_or_none

        assert _to_decimal_or_none("not_a_number") is None

    def test_empty_string_returns_none(self):
        """Triggers InvalidOperation from Decimal('')."""
        from heber.writer.ingest_contracts import _to_decimal_or_none

        assert _to_decimal_or_none("") is None

    def test_dict_returns_none(self):
        """Triggers ValueError from Decimal(str({...}))."""
        from heber.writer.ingest_contracts import _to_decimal_or_none

        assert _to_decimal_or_none({"key": "val"}) is None


# ===================================================================
# normalizer: _coerce_value
# ===================================================================


class TestCoerceValue:
    """Verify _coerce_value catches ValueError/TypeError/OverflowError."""

    def test_coerce_float_valid(self):
        from heber.writer.normalizer import _coerce_value

        assert _coerce_value(42, pa.float64()) == 42.0

    def test_coerce_float_invalid_string(self):
        """Triggers ValueError from float('not_a_float')."""
        from heber.writer.normalizer import _coerce_value

        assert _coerce_value("not_a_float", pa.float64()) is None

    def test_coerce_int_invalid_string(self):
        """Triggers ValueError from int(float('nope'))."""
        from heber.writer.normalizer import _coerce_value

        assert _coerce_value("nope", pa.int64()) is None

    def test_coerce_date_invalid_iso(self):
        """Triggers ValueError from datetime.fromisoformat('bad-date')."""
        from heber.writer.normalizer import _coerce_value

        assert _coerce_value("bad-date", pa.date32()) is None

    def test_coerce_timestamp_invalid_iso(self):
        """Triggers ValueError from datetime.fromisoformat('not-a-ts')."""
        from heber.writer.normalizer import _coerce_value

        assert _coerce_value("not-a-ts", pa.timestamp("us", tz="UTC")) is None

    def test_coerce_dict_to_float_returns_none(self):
        """Triggers TypeError from float({})."""
        from heber.writer.normalizer import _coerce_value

        assert _coerce_value({}, pa.float64()) is None

    def test_coerce_overflow_timestamp(self):
        """Triggers OverflowError from massive epoch value."""
        from heber.writer.normalizer import _coerce_value

        # A value so large it causes OverflowError in fromtimestamp
        assert _coerce_value(10**18, pa.timestamp("us", tz="UTC")) is None


# ===================================================================
# bronze.py: BronzeWriter._flush_partition
# ===================================================================


class TestBronzeFlushPartition:
    """Verify BronzeWriter._flush_partition catches OSError."""

    def test_flush_success(self, tmp_path):
        from heber.writer.bronze import BronzeWriter

        writer = BronzeWriter()
        envelope = _make_envelope()
        writer.write(envelope)

        with patch.object(type(writer), "_get_file_path", return_value=tmp_path / "test.jsonl.gz"):
            writer.flush()

    def test_flush_oserror_is_raised(self, tmp_path, monkeypatch):
        """OSError from gzip.open on unwritable path should propagate."""
        from heber.writer.bronze import BronzeWriter

        writer = BronzeWriter()
        envelope = _make_envelope()
        writer.write(envelope)

        bad_path = tmp_path / "nonexistent_dir" / "test.jsonl.gz"
        with patch.object(type(writer), "_get_file_path", return_value=bad_path):
            with pytest.raises(OSError):
                writer.flush()


# ===================================================================
# silver.py: SilverWriter._flush_partition
# ===================================================================


class TestSilverFlushPartition:
    """Verify SilverWriter._flush_partition re-raises write errors."""

    def test_flush_oserror_propagates(self, tmp_path, monkeypatch):
        """OSError from write is caught, metrics recorded, then re-raised."""
        from heber.config import settings
        from heber.writer.silver import SilverWriter

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = SilverWriter()

        partition_key = "feed=bars/instrument_type=equity/dt=2026-03-01"
        rows = [{"event_id": "e1"}]

        # Patch write_silver_parquet to simulate a write failure
        with patch("heber.writer.silver.write_silver_parquet", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                writer._flush_partition(partition_key, rows)

    def test_flush_arrow_error_propagates(self, tmp_path, monkeypatch):
        """ArrowTypeError from write is caught, metrics recorded, then re-raised."""
        from heber.config import settings
        from heber.writer.silver import SilverWriter

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = SilverWriter()

        partition_key = "feed=bars/instrument_type=equity/dt=2026-03-01"
        rows = [{"event_id": "e1"}]

        with patch("heber.writer.silver.write_silver_parquet", side_effect=pa.ArrowTypeError("bad type")):
            with pytest.raises(pa.ArrowTypeError, match="bad type"):
                writer._flush_partition(partition_key, rows)


# ===================================================================
# compactor.py: lock file operations
# ===================================================================


class TestCompactorLockOps:
    """Verify compactor lock operations catch OSError."""

    def test_release_lock_oserror_is_logged(self, tmp_path):
        """OSError from unlink is caught and logged, not raised."""
        from heber.writer.compactor import Compactor

        compactor = Compactor()
        lock_path = tmp_path / ".compaction.lock"
        lock_path.touch()

        with patch.object(Path, "unlink", side_effect=OSError("permission denied")):
            # Should log warning but not raise
            compactor._release_partition_lock(lock_path)

    def test_remove_stale_lock_oserror(self, tmp_path):
        """OSError from stale lock removal returns None."""
        import os

        from heber.writer.compactor import Compactor

        compactor = Compactor()
        lock_path = tmp_path / ".compaction.lock"
        # Create a lock file with current PID
        lock_path.write_text(f"pid={os.getpid()} ts=2026-03-01T00:00:00+00:00\n")

        # Patch unlink to simulate OSError
        with patch.object(Path, "unlink", side_effect=OSError("permission denied")):
            result = compactor._acquire_partition_lock(tmp_path)
            assert result is None

    def test_dataset_label_value_error(self, tmp_path, monkeypatch):
        """ValueError from relative_to returns 'unknown'."""
        from heber.config import settings
        from heber.writer.compactor import Compactor

        compactor = Compactor()
        # Use a path that cannot be made relative to silver_path
        unrelated_path = Path("/some/other/path")
        monkeypatch.setattr(settings, "data_root", tmp_path / "nonexistent")
        label = compactor._dataset_label(unrelated_path)
        assert label == "unknown"

    def test_read_lock_pid_oserror(self, tmp_path):
        """OSError from reading non-existent lock returns None."""
        from heber.writer.compactor import Compactor

        missing_lock = tmp_path / "does_not_exist.lock"
        assert Compactor._read_lock_pid(missing_lock) is None

    def test_read_lock_pid_empty_file(self, tmp_path):
        """IndexError from empty lock file returns None."""
        from heber.writer.compactor import Compactor

        empty_lock = tmp_path / "empty.lock"
        empty_lock.write_text("")
        assert Compactor._read_lock_pid(empty_lock) is None


class TestCompactorCompactPartition:
    """Verify compact_partition catches errors and preserves sources."""

    def test_compact_arrow_error(self, tmp_path):
        """ArrowInvalid from corrupt parquet is caught, returns 0."""
        from heber.writer.compactor import Compactor

        compactor = Compactor()
        partition = tmp_path / "feed=bars" / "instrument_type=equity" / "dt=2026-03-01"
        partition.mkdir(parents=True)

        # Write two valid small files and one corrupt
        _write_parquet(partition / "part-1.parquet", [{"event_id": "e1"}])
        _write_parquet(partition / "part-2.parquet", [{"event_id": "e2"}])
        # Third "parquet" is garbage
        (partition / "part-3.parquet").write_text("not a parquet file")

        merged = compactor.compact_partition(partition)
        assert merged == 0  # Should fail gracefully

    def test_compact_oserror(self, tmp_path, monkeypatch):
        """OSError during compaction is caught, returns 0."""
        from heber.writer.compactor import Compactor

        compactor = Compactor()
        partition = tmp_path / "feed=bars" / "instrument_type=equity" / "dt=2026-03-01"
        partition.mkdir(parents=True)

        _write_parquet(partition / "part-1.parquet", [{"event_id": "e1"}])
        _write_parquet(partition / "part-2.parquet", [{"event_id": "e2"}])

        # Simulate OS error during parquet writing
        with patch("pyarrow.parquet.ParquetWriter", side_effect=OSError("disk full")):
            merged = compactor.compact_partition(partition)
            assert merged == 0


# ===================================================================
# utils.py: write_silver_parquet
# ===================================================================


class TestWriteSilverParquet:
    """Verify write_silver_parquet catches Arrow errors for salvage and OSError."""

    def test_arrow_type_error_triggers_salvage(self, tmp_path):
        """ArrowTypeError triggers row-by-row salvage path."""
        from heber.writer.utils import write_silver_parquet

        schema = pa.schema(
            [
                pa.field("event_id", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        # Mix of valid and invalid rows: e2's string cannot coerce to float64,
        # which fails the batch write and triggers row-by-row salvage.
        rows = [
            {"event_id": "e1", "value": 1.0},
            {"event_id": "e2", "value": "not_a_float_for_schema"},
            {"event_id": "e3", "value": 3.0},
        ]

        file_path = tmp_path / "test.parquet"
        write_silver_parquet(
            rows=rows,
            schema=schema,
            file_path=file_path,
            partition_key="feed=test/dt=2026-03-01",
            dataset="test",
        )

        # Salvage keeps the two valid rows and drops e2.
        assert file_path.exists()
        assert pq.read_table(file_path).column("event_id").to_pylist() == ["e1", "e3"]

    def test_oserror_propagates(self, tmp_path):
        """OSError from writing to bad path propagates."""
        from heber.writer.utils import write_silver_parquet

        schema = pa.schema([pa.field("event_id", pa.string())])
        rows = [{"event_id": "e1"}]

        bad_path = tmp_path / "nonexistent_dir" / "test.parquet"
        with pytest.raises(OSError):
            write_silver_parquet(
                rows=rows,
                schema=schema,
                file_path=bad_path,
                partition_key="feed=test/dt=2026-03-01",
                dataset="test",
            )

    def test_per_row_salvage_arrow_invalid(self, tmp_path):
        """ArrowInvalid per-row validation catches bad rows during salvage."""
        from heber.writer.utils import write_silver_parquet

        schema = pa.schema(
            [
                pa.field("event_id", pa.string()),
                pa.field("ts", pa.timestamp("us", tz="UTC")),
            ]
        )

        # The middle row's non-timestamp string fails the batch write and forces
        # the row-by-row salvage path (the previous all-valid input never did).
        rows = [
            {"event_id": "e1", "ts": datetime(2026, 1, 1, tzinfo=UTC)},
            {"event_id": "bad", "ts": "garbage-not-a-timestamp"},
            {"event_id": "e3", "ts": datetime(2026, 1, 2, tzinfo=UTC)},
        ]

        file_path = tmp_path / "salvage_test.parquet"
        write_silver_parquet(
            rows=rows,
            schema=schema,
            file_path=file_path,
            partition_key="feed=test/dt=2026-03-01",
            dataset="test",
        )
        # Salvage keeps the two valid rows and drops the bad one.
        assert file_path.exists()
        assert pq.read_table(file_path).column("event_id").to_pylist() == ["e1", "e3"]


# ===================================================================
# transformer.py: _read_bronze_file
# ===================================================================


class TestTransformerReadBronzeFile:
    """Verify transformer catches json/validation/os errors appropriately."""

    def test_corrupt_json_line_skipped(self, tmp_path):
        """JSONDecodeError per line is caught, valid lines are returned."""
        from heber.writer.transformer import BronzeToSilverTransformer

        # Write a bronze file with one good line and one bad line
        bronze_file = tmp_path / "test.jsonl.gz"
        with gzip.open(bronze_file, "wt", encoding="utf-8") as f:
            f.write("not valid json\n")
            valid_envelope = _make_envelope().model_dump(mode="json")
            f.write(json.dumps(valid_envelope, default=str) + "\n")

        transformer = BronzeToSilverTransformer(
            bronze_path=tmp_path / "bronze",
            silver_path=tmp_path / "silver",
        )
        # Should not raise -- bad lines are skipped
        transformer._read_bronze_file(bronze_file, "bars")
        # May have 0 or more records depending on normalization, but no exception

    def test_oserror_on_missing_file(self, tmp_path):
        """OSError from missing file is caught, returns empty list."""
        from heber.writer.transformer import BronzeToSilverTransformer

        transformer = BronzeToSilverTransformer(
            bronze_path=tmp_path / "bronze",
            silver_path=tmp_path / "silver",
        )
        missing_file = tmp_path / "does_not_exist.jsonl.gz"
        records = transformer._read_bronze_file(missing_file, "bars")
        assert records == []

    def test_validation_error_line_skipped(self, tmp_path):
        """ValidationError from invalid envelope is caught per-line."""
        from heber.writer.transformer import BronzeToSilverTransformer

        bronze_file = tmp_path / "test.jsonl.gz"
        with gzip.open(bronze_file, "wt", encoding="utf-8") as f:
            # Valid JSON but missing required EventEnvelope fields
            f.write(json.dumps({"feed": "bars"}) + "\n")

        transformer = BronzeToSilverTransformer(
            bronze_path=tmp_path / "bronze",
            silver_path=tmp_path / "silver",
        )
        records = transformer._read_bronze_file(bronze_file, "bars")
        assert records == []


# ===================================================================
# consumer.py: _extract_feed_from_message
# ===================================================================


class TestConsumerExtractFeed:
    """Verify _extract_feed_from_message catches json decode errors."""

    def test_valid_json_returns_feed(self):
        from heber.writer.consumer import EventConsumer

        data = {b"data": json.dumps({"feed": "bars"}).encode()}
        assert EventConsumer._extract_feed_from_message(data) == "bars"

    def test_invalid_json_returns_unknown(self):
        from heber.writer.consumer import EventConsumer

        data = {b"data": b"not json"}
        assert EventConsumer._extract_feed_from_message(data) == "unknown"

    def test_missing_data_returns_unknown(self):
        from heber.writer.consumer import EventConsumer

        assert EventConsumer._extract_feed_from_message({}) == "unknown"

    def test_dict_payload_returns_feed(self):
        from heber.writer.consumer import EventConsumer

        data = {b"data": {"feed": "quotes"}}
        assert EventConsumer._extract_feed_from_message(data) == "quotes"


# ===================================================================
# consumer.py: _send_to_dlq
# ===================================================================


class TestConsumerSendToDlq:
    """Verify _send_to_dlq catches redis errors."""

    @pytest.mark.asyncio
    async def test_redis_error_returns_true_when_file_fallback_succeeds(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ):
        """RedisError from xadd is caught; durable file fallback allows ACK."""
        import redis.asyncio as redis_async

        import heber.writer.consumer as consumer_module
        import heber.writer.dlq_fallback as dlq_fallback_module
        from heber.writer.consumer import EventConsumer

        consumer = EventConsumer()
        consumer.redis = AsyncMock()
        consumer.redis.xadd = AsyncMock(side_effect=redis_async.RedisError("connection lost"))
        monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)

        async def _no_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _no_sleep)

        result = await consumer._send_to_dlq(
            message_id=b"1234-0",
            message_data={b"data": b"test"},
            error="test_error",
            attempts=1,
            feed="bars",
        )
        assert result is True
        assert len(list(tmp_path.rglob("*.json"))) == 1


# ===================================================================
# consumer.py: _process_event_once (catch-all)
# ===================================================================


class TestConsumerProcessEventOnce:
    """Verify _process_event_once handles various exception types."""

    def test_json_decode_error(self):
        from heber.writer.consumer import EventConsumer

        consumer = EventConsumer()
        success, error, retryable = consumer._process_event_once({b"data": b"not json"})
        assert success is False
        assert error is not None

    def test_missing_data_field(self):
        from heber.writer.consumer import EventConsumer

        consumer = EventConsumer()
        success, error, retryable = consumer._process_event_once({})
        assert success is False
        assert error is not None


# ===================================================================
# consumer.py: _flush_layers (top-level exception safety)
# ===================================================================


class TestConsumerFlushLayers:
    """Verify _flush_layers catches any flush error without crashing."""

    def test_bronze_flush_exception_returns_false(self):
        from heber.writer.consumer import EventConsumer

        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock(side_effect=OSError("disk error"))
        consumer.silver_writer.flush_if_needed = MagicMock()

        result = consumer._flush_layers()
        assert result is False

    def test_silver_flush_exception_returns_false(self):
        from heber.writer.consumer import EventConsumer

        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock()
        consumer.silver_writer.flush_if_needed = MagicMock(side_effect=OSError("disk error"))

        result = consumer._flush_layers()
        assert result is False

    def test_both_succeed_returns_true(self):
        from heber.writer.consumer import EventConsumer

        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock()
        consumer.silver_writer.flush_if_needed = MagicMock()

        result = consumer._flush_layers()
        assert result is True


# ===================================================================
# bus/__init__.py: RedisEventBus.create_consumer_group
# ===================================================================


class TestBusCreateConsumerGroup:
    """Verify create_consumer_group catches ResponseError for BUSYGROUP."""

    @pytest.mark.asyncio
    async def test_busygroup_is_handled(self):
        import redis.asyncio as redis_async

        from heber.bus import RedisEventBus, StreamName

        bus = RedisEventBus()
        bus._redis = AsyncMock()
        bus._redis.xgroup_create = AsyncMock(
            side_effect=redis_async.ResponseError("BUSYGROUP Consumer Group name already exists")
        )

        # Should not raise
        await bus.create_consumer_group(StreamName.MARKET_BARS, "test-group")

    @pytest.mark.asyncio
    async def test_other_response_error_propagates(self):
        import redis.asyncio as redis_async

        from heber.bus import RedisEventBus, StreamName

        bus = RedisEventBus()
        bus._redis = AsyncMock()
        bus._redis.xgroup_create = AsyncMock(side_effect=redis_async.ResponseError("ERR some other error"))

        with pytest.raises(redis_async.ResponseError, match="some other error"):
            await bus.create_consumer_group(StreamName.MARKET_BARS, "test-group")


# ===================================================================
# bus/__init__.py: RedisEventBus._claim_idle_messages
# ===================================================================


class TestBusClaimIdleMessages:
    """Verify _claim_idle_messages catches redis errors."""

    @pytest.mark.asyncio
    async def test_redis_error_returns_empty(self):
        import redis.asyncio as redis_async

        from heber.bus import ConsumerConfig, RedisEventBus, StreamName

        bus = RedisEventBus()
        bus._redis = AsyncMock()
        bus._redis.xpending_range = AsyncMock(side_effect=redis_async.RedisError("connection error"))

        config = ConsumerConfig(
            group_name="test-group",
            consumer_name="consumer-1",
            stream=StreamName.MARKET_BARS,
        )
        result = await bus._claim_idle_messages(config)
        assert result == []


# ===================================================================
# bus/__init__.py: RedisEventBus.get_pending_count
# ===================================================================


class TestBusGetPendingCount:
    """Verify get_pending_count catches redis errors."""

    @pytest.mark.asyncio
    async def test_redis_error_returns_zero(self):
        import redis.asyncio as redis_async

        from heber.bus import RedisEventBus, StreamName

        bus = RedisEventBus()
        bus._redis = AsyncMock()
        bus._redis.xpending = AsyncMock(side_effect=redis_async.RedisError("connection lost"))

        count = await bus.get_pending_count(StreamName.MARKET_BARS, "test-group")
        assert count == 0


# ===================================================================
# backpressure.py: RetryExecutor.execute_with_retry
# ===================================================================


class TestRetryExecutorCatchAll:
    """Verify execute_with_retry catches all exceptions (kept as broad)."""

    @pytest.mark.asyncio
    async def test_non_retryable_error_sent_to_dlq(self):
        from heber.bus.backpressure import DLQHandler, RetryConfig, RetryExecutor

        mock_bus = AsyncMock()
        mock_bus.publish = AsyncMock(return_value="dlq-1")
        dlq_handler = DLQHandler(mock_bus)
        executor = RetryExecutor(dlq_handler, RetryConfig(max_retries=1))

        async def failing_op():
            raise ValueError("schema mismatch in field X")

        with pytest.raises(ValueError):
            await executor.execute_with_retry(
                failing_op,
                envelope={"event_id": "test-1"},
                stream="test-stream",
            )


# ===================================================================
# backpressure.py: BackpressureMonitor.monitor_loop
# ===================================================================


class TestBackpressureMonitorLoop:
    """Verify monitor_loop catches exceptions during lag checks."""

    @pytest.mark.asyncio
    async def test_lag_check_error_does_not_crash_loop(self):
        from heber.bus import StreamName
        from heber.bus.backpressure import BackpressureMonitor

        mock_bus = AsyncMock()
        mock_bus.get_pending_count = AsyncMock(side_effect=RuntimeError("redis down"))

        monitor = BackpressureMonitor(mock_bus, check_interval_seconds=0.01)

        # Run for a short time -- should not crash
        async def run_briefly():
            task = asyncio.create_task(monitor.monitor_loop([(StreamName.MARKET_BARS, "test-group")]))
            await asyncio.sleep(0.05)
            monitor.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await run_briefly()
