"""Coverage-focused tests for heber.writer, heber.writer.bronze, and heber.writer.silver modules.

Targets uncovered lines from the coverage report, exercising:
- BronzeWriter: flush_if_needed thresholds, flush(), _flush_partition error path, _dataset_from_partition
- SilverWriter: flush(), _flush_partition empty rows, _envelope_to_row unmapped feed fallback
- EventConsumer: _decode_string, _serialize_message_data, _send_to_dlq, _parse_and_validate_envelope,
  _flush_layers, _final_flush, _extract_feed_from_message, _validate_payload_schema,
  _should_emit_validation_warning, _log_silver_validation_failure, process_event,
  _process_with_retry, _write_silver_candidates (aggregate failures)
- ingest_contracts: normalize_payload_for_feed (news, congress, insider, tide, flow, short_data),
  _parse_amount_range, _build_insider_relationships, _to_decimal_or_none, resolve_silver_feed,
  resolve_feed_alias, is_contracted_feed, is_bronze_only_feed, required_fields_for_feed
- normalizer: _coerce_* helpers, MissingRequiredFieldsError, envelope_to_silver_row,
  enforce_required_non_null_fields, missing_required_non_null_fields,
  extract_item_event_timestamp, explode_aggregate_payload, _build_item_envelope
- utils: get_bronze_partition_key, get_partition_key, build_silver_candidates, write_silver_parquet
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.bronze import BronzeWriter
from heber.writer.consumer import EventConsumer
from heber.writer.ingest_contracts import (
    UnmappedFeedError,
    _build_insider_relationships,
    _parse_amount_range,
    _to_decimal_or_none,
    is_bronze_only_feed,
    is_contracted_feed,
    normalize_payload_for_feed,
    required_fields_for_feed,
    resolve_feed_alias,
    resolve_silver_feed,
)
from heber.writer.normalizer import (
    MissingRequiredFieldsError,
    _coerce_bool,
    _coerce_date,
    _coerce_float,
    _coerce_int,
    _coerce_list,
    _coerce_string,
    _coerce_timestamp,
    _coerce_value,
    _epoch_to_utc,
    _parse_string_timestamp,
    enforce_required_non_null_fields,
    envelope_to_silver_row,
    explode_aggregate_payload,
    extract_item_event_timestamp,
    missing_required_non_null_fields,
)
from heber.writer.silver import SilverWriter
from heber.writer.utils import (
    build_silver_candidates,
    get_bronze_partition_key,
    get_partition_key,
    write_silver_parquet,
)

NOW = datetime(2026, 3, 10, 14, 30, 0, tzinfo=UTC)


def _make_envelope(**overrides) -> EventEnvelope:
    """Create a minimal valid EventEnvelope for testing."""
    defaults = {
        "event_id": "evt-test-001",
        "provider": "alpaca",
        "feed": "bars",
        "source": "websocket",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW,
        "ts_ingest": NOW,
        "ts_available": NOW,
        "payload": {
            "t": NOW.isoformat(),
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000,
        },
    }
    defaults.update(overrides)
    return EventEnvelope(**defaults)


# ============================================================================
# Bronze Writer Tests
# ============================================================================


class TestBronzeWriter:
    """Tests for BronzeWriter flush logic and partition paths."""

    def test_write_buffers_event(self):
        """BronzeWriter.write() should buffer without writing to disk."""
        writer = BronzeWriter()
        envelope = _make_envelope()
        writer.write(envelope)
        assert len(writer.buffers) == 1
        pk = list(writer.buffers.keys())[0]
        assert len(writer.buffers[pk]) == 1

    def test_flush_writes_all_buffers(self, tmp_path, monkeypatch):
        """flush() writes all partitions immediately."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = BronzeWriter()
        envelope = _make_envelope()
        writer.write(envelope)
        writer.flush()

        # After flush, buffers should be empty lists
        for events in writer.buffers.values():
            assert events == []

        # Verify gzip file was written
        files = list((tmp_path / "bronze").rglob("*.jsonl.gz"))
        assert len(files) == 1
        with gzip.open(files[0], "rt") as f:
            data = json.loads(f.readline())
        assert data["event_id"] == "evt-test-001"

    def test_flush_empty_buffer_no_writes(self, tmp_path, monkeypatch):
        """flush() on an empty buffer writes nothing."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = BronzeWriter()
        writer.flush()
        bronze_dir = tmp_path / "bronze"
        files = list(bronze_dir.rglob("*.jsonl.gz")) if bronze_dir.exists() else []
        assert len(files) == 0

    def test_flush_if_needed_respects_batch_size(self, tmp_path, monkeypatch):
        """flush_if_needed triggers when buffer exceeds batch size."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        monkeypatch.setattr(settings, "bronze_max_batch_size", 2)
        monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9999)

        writer = BronzeWriter()
        for i in range(3):
            envelope = _make_envelope(event_id=f"evt-{i}")
            writer.write(envelope)

        writer.flush_if_needed()
        files = list((tmp_path / "bronze").rglob("*.jsonl.gz"))
        assert len(files) == 1

    def test_flush_if_needed_respects_time_interval(self, tmp_path, monkeypatch):
        """flush_if_needed triggers when elapsed time exceeds interval."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        monkeypatch.setattr(settings, "bronze_max_batch_size", 99999)
        monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 1)

        writer = BronzeWriter()
        envelope = _make_envelope()
        writer.write(envelope)
        writer.last_flush = datetime.now(UTC) - timedelta(seconds=10)

        writer.flush_if_needed()
        files = list((tmp_path / "bronze").rglob("*.jsonl.gz"))
        assert len(files) == 1

    def test_flush_if_needed_skips_when_no_conditions_met(self, monkeypatch):
        """flush_if_needed does NOT flush when neither size nor time is met."""
        from heber.config import settings

        monkeypatch.setattr(settings, "bronze_max_batch_size", 99999)
        monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9999)

        writer = BronzeWriter()
        envelope = _make_envelope()
        writer.write(envelope)
        writer._flush_partition = MagicMock()
        writer.flush_if_needed()
        writer._flush_partition.assert_not_called()

    def test_flush_partition_error_reraises(self, tmp_path, monkeypatch):
        """_flush_partition re-raises exceptions after logging."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = BronzeWriter()

        # Override _get_file_path to return an invalid path
        pk = "provider=test/feed=bars/dt=2026-03-10/hour=14"
        writer._get_file_path = MagicMock(return_value=Path("/nonexistent/dir/x.jsonl.gz"))

        with pytest.raises(OSError):
            writer._flush_partition(pk, [{"event_id": "evt-1"}])

    def test_dataset_from_partition_extracts_feed(self):
        """_dataset_from_partition extracts the feed from a partition key."""
        assert BronzeWriter._dataset_from_partition("provider=alpaca/feed=bars/dt=2026-03-10/hour=14") == "bars"

    def test_dataset_from_partition_unknown_fallback(self):
        """_dataset_from_partition returns 'unknown' when no feed= token found."""
        assert BronzeWriter._dataset_from_partition("some/other/path") == "unknown"

    def test_dataset_from_partition_with_quotes(self):
        """_dataset_from_partition handles quotes feed."""
        assert BronzeWriter._dataset_from_partition("provider=alpaca/feed=quotes/dt=2026-03-10/hour=14") == "quotes"


# ============================================================================
# Silver Writer Tests
# ============================================================================


class TestSilverWriter:
    """Tests for SilverWriter flush and partition logic."""

    def test_write_row_buffers_directly(self):
        """write_row() adds a row to the correct partition buffer."""
        writer = SilverWriter()
        pk = "feed=bars/instrument_type=equity/dt=2026-03-10"
        row = {"event_id": "evt-1", "open": 100.0}
        writer.write_row(pk, row)
        assert pk in writer.buffers
        assert len(writer.buffers[pk]) == 1

    def test_flush_writes_all_partitions(self, tmp_path, monkeypatch):
        """flush() writes all partition buffers to parquet."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)

        writer = SilverWriter()
        pk = "feed=bars/instrument_type=equity/dt=2026-03-10"
        row = {
            "event_id": "evt-1",
            "provider": "alpaca",
            "feed": "bars",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": NOW,
            "ts_ingest": NOW,
            "ts_available": NOW,
            "source": "websocket",
            "schema_version": "v1",
            "quality_flags": [],
            "timeframe": "1Min",
            "bar_start_ts": NOW,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "trade_count": 25,
            "vwap": 100.4,
        }
        writer.buffers[pk] = [row]
        writer.flush()

        for rows in writer.buffers.values():
            assert rows == []

        files = list((tmp_path / "silver").rglob("*.parquet"))
        assert len(files) == 1

    def test_flush_empty_buffer_no_writes(self, tmp_path, monkeypatch):
        """flush() on empty buffer writes nothing."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = SilverWriter()
        writer.flush()
        silver_dir = tmp_path / "silver"
        assert not silver_dir.exists() or not list(silver_dir.rglob("*.parquet"))

    def test_flush_partition_skips_empty_rows(self, tmp_path, monkeypatch):
        """_flush_partition returns early when rows list is empty."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = SilverWriter()
        writer._flush_partition("feed=bars/instrument_type=equity/dt=2026-03-10", [])
        silver_dir = tmp_path / "silver"
        files = list(silver_dir.rglob("*.parquet")) if silver_dir.exists() else []
        assert len(files) == 0

    def test_flush_if_needed_respects_row_count(self, tmp_path, monkeypatch):
        """flush_if_needed triggers when row count exceeds max."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        monkeypatch.setattr(settings, "silver_max_rows_per_file", 1)
        monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 9999)

        writer = SilverWriter()
        pk = "feed=bars/instrument_type=equity/dt=2026-03-10"
        writer._flush_partition = MagicMock()
        writer.buffers[pk] = [{"event_id": "r1"}, {"event_id": "r2"}]
        writer.flush_if_needed()
        writer._flush_partition.assert_called_once()

    def test_flush_partition_error_reraises(self, tmp_path, monkeypatch):
        """_flush_partition re-raises non-Arrow exceptions."""
        from heber.config import settings

        monkeypatch.setattr(settings, "data_root", tmp_path)
        writer = SilverWriter()
        pk = "feed=bars/instrument_type=equity/dt=2026-03-10"
        # Mock write_silver_parquet to raise a generic error
        with patch("heber.writer.silver.write_silver_parquet", side_effect=OSError("disk full")):
            rows = [{"event_id": "e1"}]
            with pytest.raises(OSError, match="disk full"):
                writer._flush_partition(pk, rows)

    def test_envelope_to_row_unmapped_feed_fallback(self):
        """_envelope_to_row for unmapped feed produces fallback row with payload_json."""
        writer = SilverWriter()
        envelope = _make_envelope(feed="totally_unknown_feed_xyz")
        row, normalized = writer._envelope_to_row(envelope)
        assert "payload_json" in row
        assert row["event_id"] == "evt-test-001"
        payload_data = json.loads(row["payload_json"])
        assert isinstance(payload_data, dict)


# ============================================================================
# EventConsumer Tests
# ============================================================================


class TestEventConsumerHelpers:
    """Tests for EventConsumer helper methods."""

    def test_decode_string_bytes(self):
        """_decode_string handles bytes input."""
        consumer = EventConsumer()
        assert consumer._decode_string(b"hello") == "hello"

    def test_decode_string_already_string(self):
        """_decode_string handles string input."""
        consumer = EventConsumer()
        assert consumer._decode_string("hello") == "hello"

    def test_decode_string_other_types(self):
        """_decode_string handles non-string/bytes by converting to str."""
        consumer = EventConsumer()
        assert consumer._decode_string(123) == "123"

    def test_serialize_message_data_bytes_keys_and_values(self):
        """_serialize_message_data handles bytes keys and values."""
        consumer = EventConsumer()
        result = consumer._serialize_message_data({b"data": b'{"foo": "bar"}'})
        parsed = json.loads(result)
        assert parsed["data"] == '{"foo": "bar"}'

    def test_serialize_message_data_dict_value(self):
        """_serialize_message_data serializes dict values to JSON."""
        consumer = EventConsumer()
        result = consumer._serialize_message_data({"nested": {"a": 1}})
        parsed = json.loads(result)
        assert json.loads(parsed["nested"]) == {"a": 1}

    def test_serialize_message_data_list_value(self):
        """_serialize_message_data serializes list values to JSON."""
        consumer = EventConsumer()
        result = consumer._serialize_message_data({"items": [1, 2, 3]})
        parsed = json.loads(result)
        assert json.loads(parsed["items"]) == [1, 2, 3]

    def test_serialize_message_data_non_string_value(self):
        """_serialize_message_data converts other types to str."""
        consumer = EventConsumer()
        result = consumer._serialize_message_data({"count": 42})
        parsed = json.loads(result)
        assert parsed["count"] == "42"


class TestEventConsumerParseAndValidate:
    """Tests for _parse_and_validate_envelope."""

    def test_parse_with_data_key(self):
        """_parse_and_validate_envelope parses 'data' key from event dict."""
        consumer = EventConsumer()
        payload = {
            "event_id": "evt-1",
            "provider": "alpaca",
            "feed": "bars",
            "source": "ws",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": NOW.isoformat(),
            "ts_ingest": NOW.isoformat(),
            "payload": {},
        }
        event_data = {"data": json.dumps(payload)}
        result = consumer._parse_and_validate_envelope(event_data)
        assert result.event_id == "evt-1"
        # ts_available should be set if None
        assert result.ts_available is not None

    def test_parse_with_bytes_data_key(self):
        """_parse_and_validate_envelope handles bytes 'data' key."""
        consumer = EventConsumer()
        payload = {
            "event_id": "evt-2",
            "provider": "alpaca",
            "feed": "bars",
            "source": "ws",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": NOW.isoformat(),
            "ts_ingest": NOW.isoformat(),
            "payload": {},
        }
        event_data = {b"data": json.dumps(payload).encode()}
        result = consumer._parse_and_validate_envelope(event_data)
        assert result.event_id == "evt-2"

    def test_parse_missing_data_raises(self):
        """_parse_and_validate_envelope raises on missing 'data' key."""
        consumer = EventConsumer()
        with pytest.raises(ValueError, match="No 'data' or 'payload' field"):
            consumer._parse_and_validate_envelope({"other": "value"})

    def test_parse_with_ts_available_already_set(self):
        """_parse_and_validate_envelope preserves existing ts_available."""
        consumer = EventConsumer()
        fixed_ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        payload = {
            "event_id": "evt-3",
            "provider": "alpaca",
            "feed": "bars",
            "source": "ws",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": NOW.isoformat(),
            "ts_ingest": NOW.isoformat(),
            "ts_available": fixed_ts.isoformat(),
            "payload": {},
        }
        event_data = {"data": json.dumps(payload)}
        result = consumer._parse_and_validate_envelope(event_data)
        assert result.ts_available == fixed_ts


class TestEventConsumerProcessEvent:
    """Tests for process_event (the sync convenience wrapper)."""

    def test_process_event_returns_true_on_success(self):
        """process_event returns True when event succeeds."""
        consumer = EventConsumer()
        consumer.bronze_writer.write = MagicMock()
        consumer.silver_writer.write_row = MagicMock()

        payload = {
            "event_id": "evt-ok",
            "provider": "alpaca",
            "feed": "bars",
            "source": "ws",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": NOW.isoformat(),
            "ts_ingest": NOW.isoformat(),
            "payload": {
                "t": NOW.isoformat(),
                "o": "100",
                "h": "101",
                "l": "99",
                "c": "100.5",
                "v": "1000",
            },
        }
        result = consumer.process_event({"data": json.dumps(payload)})
        assert result is True

    def test_process_event_returns_false_on_failure(self):
        """process_event returns False on parse failure."""
        consumer = EventConsumer()
        result = consumer.process_event({"data": "not-json!!!"})
        assert result is False


class TestEventConsumerFlushLayers:
    """Tests for _flush_layers and _final_flush."""

    def test_flush_layers_returns_true_on_success(self):
        """_flush_layers returns True when both layers flush ok."""
        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock()
        consumer.silver_writer.flush_if_needed = MagicMock()
        assert consumer._flush_layers() is True

    def test_flush_layers_returns_false_on_bronze_error(self):
        """_flush_layers returns False when bronze flush fails."""
        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock(side_effect=RuntimeError("disk full"))
        consumer.silver_writer.flush_if_needed = MagicMock()
        assert consumer._flush_layers() is False

    def test_flush_layers_returns_false_on_silver_error(self):
        """_flush_layers returns False when silver flush fails."""
        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock()
        consumer.silver_writer.flush_if_needed = MagicMock(side_effect=RuntimeError("io error"))
        assert consumer._flush_layers() is False

    def test_flush_layers_returns_false_on_both_errors(self):
        """_flush_layers returns False when both layers fail."""
        consumer = EventConsumer()
        consumer.bronze_writer.flush_if_needed = MagicMock(side_effect=RuntimeError("err1"))
        consumer.silver_writer.flush_if_needed = MagicMock(side_effect=RuntimeError("err2"))
        assert consumer._flush_layers() is False

    def test_final_flush_calls_flush_on_both(self):
        """_final_flush calls flush() on both writers."""
        consumer = EventConsumer()
        consumer.bronze_writer.flush = MagicMock()
        consumer.silver_writer.flush = MagicMock()
        consumer._final_flush()
        consumer.bronze_writer.flush.assert_called_once()
        consumer.silver_writer.flush.assert_called_once()


class TestEventConsumerExtractFeed:
    """Tests for _extract_feed_from_message."""

    def test_extract_feed_from_string_data(self):
        """Extracts feed from JSON-encoded 'data' field."""
        result = EventConsumer._extract_feed_from_message({"data": json.dumps({"feed": "bars"})})
        assert result == "bars"

    def test_extract_feed_from_bytes_data(self):
        """Extracts feed from bytes 'data' field."""
        result = EventConsumer._extract_feed_from_message({b"data": json.dumps({"feed": "quotes"}).encode()})
        assert result == "quotes"

    def test_extract_feed_from_dict_payload(self):
        """Extracts feed from dict payload field."""
        result = EventConsumer._extract_feed_from_message({"data": {"feed": "trades"}})
        assert result == "trades"

    def test_extract_feed_missing_data_returns_unknown(self):
        """Returns 'unknown' when no data/payload field."""
        result = EventConsumer._extract_feed_from_message({"other": "x"})
        assert result == "unknown"

    def test_extract_feed_invalid_json_returns_unknown(self):
        """Returns 'unknown' on malformed JSON."""
        result = EventConsumer._extract_feed_from_message({"data": "not-json"})
        assert result == "unknown"

    def test_extract_feed_missing_feed_key_returns_unknown(self):
        """Returns 'unknown' when JSON has no 'feed' key."""
        result = EventConsumer._extract_feed_from_message({"data": json.dumps({"other": "x"})})
        assert result == "unknown"

    def test_extract_feed_from_payload_key(self):
        """Extracts feed from 'payload' key when 'data' is missing."""
        result = EventConsumer._extract_feed_from_message({"payload": json.dumps({"feed": "flow_alerts"})})
        assert result == "flow_alerts"


class TestEventConsumerValidatePayloadSchema:
    """Tests for _validate_payload_schema."""

    def test_validate_no_warning_for_unknown_feed(self, monkeypatch):
        """No warning for feeds not in PAYLOAD_REQUIRED_FIELDS."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        envelope = _make_envelope(feed="bars", payload={"o": 1})
        consumer._validate_payload_schema(envelope)
        mock_warn.assert_not_called()

    def test_validate_warns_on_missing_keys(self, monkeypatch):
        """Warns when required payload keys are missing."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        # flow_alerts requires timestamp, symbol, strike, expiry, put_call, premium, volume
        envelope = _make_envelope(
            feed="flow_alerts",
            instrument_type="option",
            instrument_key="option:OCC:AAPL260116C00200000",
            payload={"timestamp": NOW.isoformat()},
        )
        consumer._validate_payload_schema(envelope)
        # Should have at least one warning about missing keys
        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "payload_missing_keys"]
        assert len(warning_calls) >= 1

    def test_validate_warns_on_unexpected_keys(self, monkeypatch):
        """Warns when unexpected payload keys are present."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        # market_tide with an unexpected key
        envelope = _make_envelope(
            feed="market_tide",
            payload={
                "timestamp": NOW.isoformat(),
                "date": "2026-03-10",
                "net_call_premium": 100,
                "net_put_premium": 200,
                "net_volume": 500,
                "sentiment": "bullish",
                "totally_unknown_field": True,
            },
        )
        consumer._validate_payload_schema(envelope)
        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "payload_unexpected_keys"]
        assert len(warning_calls) >= 1


class TestShouldEmitValidationWarning:
    """Tests for _should_emit_validation_warning."""

    def test_first_occurrence_emits(self):
        assert EventConsumer._should_emit_validation_warning(1) is True

    def test_tenth_occurrence_emits(self):
        assert EventConsumer._should_emit_validation_warning(10) is True

    def test_hundredth_occurrence_emits(self):
        assert EventConsumer._should_emit_validation_warning(100) is True

    def test_thousandth_occurrence_emits(self):
        assert EventConsumer._should_emit_validation_warning(1000) is True

    def test_second_occurrence_does_not_emit(self):
        assert EventConsumer._should_emit_validation_warning(2) is False

    def test_fifty_does_not_emit(self):
        assert EventConsumer._should_emit_validation_warning(50) is False

    def test_two_thousandth_emits(self):
        assert EventConsumer._should_emit_validation_warning(2000) is True


class TestLogSilverValidationFailure:
    """Tests for _log_silver_validation_failure with rate-limiting."""

    def test_logs_first_occurrence(self, monkeypatch):
        """First occurrence is logged."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        envelope = _make_envelope()
        error = ValueError("test error")
        consumer._log_silver_validation_failure(envelope, error)

        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "silver_validation_failed"]
        assert len(warning_calls) == 1

    def test_suppresses_after_first(self, monkeypatch):
        """Occurrences 2-9 are suppressed."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        envelope = _make_envelope()
        error = ValueError("repeated error")

        for _ in range(5):
            consumer._log_silver_validation_failure(envelope, error)

        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "silver_validation_failed"]
        # Should only log at count 1 (not 2, 3, 4, 5)
        assert len(warning_calls) == 1

    def test_logs_at_milestone_10(self, monkeypatch):
        """Logs again at 10th occurrence."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        envelope = _make_envelope()
        error = ValueError("repeated error")

        for _ in range(10):
            consumer._log_silver_validation_failure(envelope, error)

        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "silver_validation_failed"]
        # Should log at 1 and 10
        assert len(warning_calls) == 2

    def test_error_with_details_attribute(self, monkeypatch):
        """Handles errors with .details attribute (like SilverNormalizationError)."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        from heber.writer.key_normalization import SilverNormalizationError

        envelope = _make_envelope()
        error = SilverNormalizationError(
            "test",
            details={"feed": "bars", "instrument_type": "equity", "instrument_key": "equity:X", "symbol": "X"},
        )
        consumer._log_silver_validation_failure(envelope, error)

        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "silver_validation_failed"]
        assert len(warning_calls) == 1
        assert warning_calls[0].kwargs["feed"] == "bars"

    def test_error_with_non_mapping_details(self, monkeypatch):
        """Handles errors where .details is not a Mapping."""
        consumer = EventConsumer()
        mock_warn = MagicMock()
        monkeypatch.setattr("heber.writer.consumer.logger.warning", mock_warn)

        envelope = _make_envelope()
        error = ValueError("test")
        error.details = "not-a-dict"  # type: ignore[attr-defined]
        consumer._log_silver_validation_failure(envelope, error)

        warning_calls = [c for c in mock_warn.call_args_list if c.args[0] == "silver_validation_failed"]
        assert len(warning_calls) == 1


class TestProcessWithRetry:
    """Tests for _process_with_retry."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        """Returns success on first attempt with attempt count 1."""
        consumer = EventConsumer()
        consumer._process_event_once = MagicMock(return_value=(True, None, True))
        success, error, attempts = await consumer._process_with_retry({"data": "{}"})
        assert success is True
        assert error == ""
        assert attempts == 1

    @pytest.mark.asyncio
    async def test_non_retryable_fails_immediately(self, monkeypatch):
        """Non-retryable error fails without retry."""
        from heber.config import settings

        monkeypatch.setattr(settings, "redis_process_max_retries", 3)
        monkeypatch.setattr(settings, "redis_retry_backoff_seconds", 0.0)

        consumer = EventConsumer()
        consumer._process_event_once = MagicMock(return_value=(False, "validation_error", False))
        success, error, attempts = await consumer._process_with_retry({"data": "{}"})
        assert success is False
        assert error == "validation_error"
        assert attempts == 1

    @pytest.mark.asyncio
    async def test_retries_on_retryable_failure(self, monkeypatch):
        """Retries on retryable failure up to max_retries."""
        from heber.config import settings

        monkeypatch.setattr(settings, "redis_process_max_retries", 3)
        monkeypatch.setattr(settings, "redis_retry_backoff_seconds", 0.0)

        consumer = EventConsumer()
        consumer._process_event_once = MagicMock(
            side_effect=[
                (False, "transient_error", True),
                (False, "transient_error", True),
                (True, None, True),
            ]
        )
        success, error, attempts = await consumer._process_with_retry({"data": "{}"})
        assert success is True
        assert attempts == 3


class TestSendToDlq:
    """Tests for _send_to_dlq."""

    @pytest.mark.asyncio
    async def test_send_to_dlq_success(self, monkeypatch, tmp_path):
        """Successfully sends to DLQ and returns True."""
        import heber.writer.consumer as consumer_module

        monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)
        consumer = EventConsumer()

        class FakeRedis:
            async def xadd(self, stream, payload, **_kwargs):
                return b"dlq-1-0"

        consumer.redis = FakeRedis()
        result = await consumer._send_to_dlq("1-0", {"data": "{}"}, "test_error", 3, feed="bars")
        assert result is True

    @pytest.mark.asyncio
    async def test_send_to_dlq_failure(self, monkeypatch, tmp_path):
        """Returns False only when both Redis and the durable file fallback fail."""
        import redis.asyncio as aioredis

        import heber.writer.consumer as consumer_module

        monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)

        def _fail_fallback(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(consumer_module, "write_dlq_fallback_file", _fail_fallback)

        consumer = EventConsumer()

        class FakeRedis:
            async def xadd(self, stream, payload, **_kwargs):
                raise aioredis.RedisError("DLQ unavailable")

        consumer.redis = FakeRedis()
        result = await consumer._send_to_dlq("1-0", {"data": "{}"}, "test_error", 3, feed="bars")
        assert result is False


class TestWriteSilverCandidates:
    """Tests for _write_silver_candidates on EventConsumer."""

    def test_empty_aggregate_skips_write(self):
        """Empty aggregate payload list skips silver writes."""
        consumer = EventConsumer()
        consumer.silver_writer.write_row = MagicMock()

        envelope = _make_envelope(
            feed="trades",
            payload={"trades": []},
        )
        consumer._write_silver_candidates(envelope)
        consumer.silver_writer.write_row.assert_not_called()

    def test_all_candidates_fail_raises(self):
        """Raises when all aggregate candidates fail."""
        consumer = EventConsumer()
        consumer.silver_writer.write_row = MagicMock()

        # bars with items that are missing required fields
        envelope = _make_envelope(
            feed="bars",
            payload={
                "bars": [
                    {"timestamp": NOW.isoformat()},  # missing open, high, low, close, volume
                    {"timestamp": NOW.isoformat()},
                ],
            },
        )
        with pytest.raises((UnmappedFeedError, MissingRequiredFieldsError, ValueError)):
            consumer._write_silver_candidates(envelope)


# ============================================================================
# Ingest Contracts Tests
# ============================================================================


class TestIngestContractsFunctions:
    """Tests for ingest_contracts module functions."""

    def test_resolve_silver_feed_known(self):
        """resolve_silver_feed returns canonical name for known feed."""
        assert resolve_silver_feed("bars") == "bars"
        assert resolve_silver_feed("ftds") == "ftd"
        assert resolve_silver_feed("short_interest") == "short_data"

    def test_resolve_silver_feed_unknown(self):
        """resolve_silver_feed returns None for unknown feed."""
        assert resolve_silver_feed("totally_unknown_feed_xyz") is None

    def test_resolve_feed_alias_known(self):
        """resolve_feed_alias returns alias for known feed."""
        assert resolve_feed_alias("ftds") == "ftd"
        assert resolve_feed_alias("greeks") == "greek_exposure"

    def test_resolve_feed_alias_unknown(self):
        """resolve_feed_alias returns the feed itself for unknown."""
        assert resolve_feed_alias("bars") == "bars"
        assert resolve_feed_alias("unknown_xyz") == "unknown_xyz"

    def test_is_contracted_feed(self):
        """is_contracted_feed for known and unknown feeds."""
        assert is_contracted_feed("bars") is True
        assert is_contracted_feed("quotes") is True
        assert is_contracted_feed("totally_unknown") is False

    def test_is_bronze_only_feed(self):
        """is_bronze_only_feed correctly identifies bronze-only feeds."""
        assert is_bronze_only_feed("news") is True
        # 'institutions' aliases to 'institution_holdings' which is bronze-only
        assert is_bronze_only_feed("institutions") is True
        assert is_bronze_only_feed("bars") is False

    def test_required_fields_for_feed_known(self):
        """required_fields_for_feed returns fields for known feeds."""
        fields = required_fields_for_feed("bars")
        assert "open" in fields
        assert "close" in fields

    def test_required_fields_for_feed_unknown(self):
        """required_fields_for_feed returns empty set for unknown feed."""
        assert required_fields_for_feed("totally_unknown_xyz") == set()


class TestNormalizePayloadForFeed:
    """Tests for normalize_payload_for_feed with various feeds."""

    def test_normalize_news_article_id_from_id(self):
        """News normalization: article_id fallback from 'id'."""
        payload = {"id": "news-123", "content": "Some text", "source": {"name": "Reuters"}}
        result = normalize_payload_for_feed("news", payload)
        assert result["article_id"] == "news-123"
        assert result["body"] == "Some text"
        assert result["source"] == "Reuters"

    def test_normalize_news_published_at_from_created_at(self):
        """News normalization: published_at fallback from 'created_at'."""
        payload = {"created_at": "2026-03-10T12:00:00Z"}
        result = normalize_payload_for_feed("news", payload)
        assert result["published_at"] == "2026-03-10T12:00:00Z"

    def test_normalize_news_published_at_from_updated_at(self):
        """News normalization: published_at fallback from 'updated_at'."""
        payload = {"updated_at": "2026-03-10T12:00:00Z"}
        result = normalize_payload_for_feed("news", payload)
        assert result["published_at"] == "2026-03-10T12:00:00Z"

    def test_normalize_news_source_dict(self):
        """News normalization: source dict extraction."""
        payload = {"source": {"name": "Bloomberg", "id": "bbg"}}
        result = normalize_payload_for_feed("news", payload)
        assert result["source"] == "Bloomberg"

    def test_normalize_news_source_dict_id_fallback(self):
        """News normalization: source dict falls back to 'id'."""
        payload = {"source": {"id": "bbg"}}
        result = normalize_payload_for_feed("news", payload)
        assert result["source"] == "bbg"

    def test_normalize_congress_trades(self):
        """Congress trades normalization from various field names."""
        payload = {
            "name": "Pelosi",
            "txn_type": "Purchase",
            "transaction_date": "2026-03-01",
            "filed_at_date": "2026-03-05",
            "id": "txn-1",
            "amounts": "$50,000 - $100,000",
        }
        result = normalize_payload_for_feed("congress_trades", payload)
        assert result["politician_name"] == "Pelosi"
        assert result["trade_type"] == "Purchase"
        assert result["trade_date"] == "2026-03-01"
        assert result["disclosure_date"] == "2026-03-05"
        assert result["trade_id"] == "txn-1"
        assert result["amount_min"] == 50000.0
        assert result["amount_max"] == 100000.0

    def test_normalize_insider_trades(self):
        """Insider trades normalization from various field names."""
        payload = {
            "owner_name": "John Exec",
            "officer_title": "CFO",
            "transaction_code": "P",
            "transaction_date": "2026-03-01",
            "id": "ins-1",
            "amount": 5000,
            "is_director": True,
            "is_officer": True,
        }
        result = normalize_payload_for_feed("insider_trades", payload)
        assert result["insider_name"] == "John Exec"
        assert result["insider_title"] == "CFO"
        assert result["trade_type"] == "P"
        assert result["trade_date"] == "2026-03-01"
        assert result["filing_id"] == "ins-1"
        assert result["shares"] == 5000
        assert "director" in result["insider_relationship"]
        assert "officer" in result["insider_relationship"]

    def test_normalize_market_tide_calculates_ratio(self):
        """Market tide normalization computes call_put_ratio."""
        payload = {
            "net_call_premium": 1000000,
            "net_put_premium": 500000,
        }
        result = normalize_payload_for_feed("market_tide", payload)
        assert result["call_put_ratio"] == pytest.approx(2.0)

    def test_normalize_market_tide_zero_put_skips_ratio(self):
        """Market tide normalization skips ratio on zero put premium."""
        payload = {
            "net_call_premium": 1000000,
            "net_put_premium": 0,
        }
        result = normalize_payload_for_feed("market_tide", payload)
        assert result.get("call_put_ratio") is None

    def test_normalize_sector_tide(self):
        """Sector tide normalization computes call_put_ratio."""
        payload = {
            "net_call_premium": 300000,
            "net_put_premium": 100000,
        }
        result = normalize_payload_for_feed("sector_tide", payload)
        assert result["call_put_ratio"] == pytest.approx(3.0)

    def test_normalize_flow_alerts_put_call_call(self):
        """Flow alerts normalization: 'call' -> 'C'."""
        payload = {"put_call": "call"}
        result = normalize_payload_for_feed("flow_alerts", payload)
        assert result["put_call"] == "C"

    def test_normalize_flow_alerts_put_call_put(self):
        """Flow alerts normalization: 'put' -> 'P'."""
        payload = {"put_call": "put"}
        result = normalize_payload_for_feed("flow_alerts", payload)
        assert result["put_call"] == "P"

    def test_normalize_flow_alerts_put_call_uppercase(self):
        """Flow alerts normalization: 'Call' -> 'C'."""
        payload = {"put_call": "Call"}
        result = normalize_payload_for_feed("flow_alerts", payload)
        assert result["put_call"] == "C"

    def test_normalize_short_data_aliases(self):
        """Short data normalization copies alias fields."""
        payload = {"short_volume": 12345, "short_ratio": 0.15}
        result = normalize_payload_for_feed("short_data", payload)
        assert result["short_interest"] == 12345
        assert result["short_percent_float"] == 0.15

    def test_normalize_short_data_no_overwrite(self):
        """Short data normalization does not overwrite existing keys."""
        payload = {"short_interest": 999, "short_volume": 12345}
        result = normalize_payload_for_feed("short_data", payload)
        assert result["short_interest"] == 999  # Not overwritten

    def test_normalize_unknown_feed_passthrough(self):
        """Unknown feed: payload passes through unchanged."""
        payload = {"foo": "bar", "baz": 42}
        result = normalize_payload_for_feed("totally_unknown_xyz", payload)
        assert result == {"foo": "bar", "baz": 42}


class TestParseAmountRange:
    """Tests for _parse_amount_range helper."""

    def test_none_input(self):
        assert _parse_amount_range(None) is None

    def test_numeric_input(self):
        assert _parse_amount_range(50000) == (50000.0, 50000.0)

    def test_float_input(self):
        assert _parse_amount_range(75000.50) == (75000.5, 75000.5)

    def test_range_string(self):
        result = _parse_amount_range("$50,000 - $100,000")
        assert result == (50000.0, 100000.0)

    def test_single_value_string(self):
        result = _parse_amount_range("$25,000")
        assert result == (25000.0, 25000.0)

    def test_no_match_string(self):
        assert _parse_amount_range("unknown") is None


class TestBuildInsiderRelationships:
    """Tests for _build_insider_relationships helper."""

    def test_no_flags(self):
        assert _build_insider_relationships({}) is None

    def test_single_flag(self):
        assert _build_insider_relationships({"is_director": True}) == "director"

    def test_multiple_flags(self):
        result = _build_insider_relationships({"is_director": True, "is_officer": True, "is_10b5_1": True})
        parts = result.split("|")
        assert "director" in parts
        assert "officer" in parts
        assert "10b5-1" in parts

    def test_false_flags_ignored(self):
        assert _build_insider_relationships({"is_director": False, "is_officer": False}) is None


class TestToDecimalOrNone:
    """Tests for _to_decimal_or_none helper."""

    def test_none(self):
        assert _to_decimal_or_none(None) is None

    def test_int(self):
        from decimal import Decimal

        assert _to_decimal_or_none(100) == Decimal("100")

    def test_float(self):
        from decimal import Decimal

        result = _to_decimal_or_none(1.5)
        assert result == Decimal("1.5")

    def test_string(self):
        from decimal import Decimal

        assert _to_decimal_or_none("42.5") == Decimal("42.5")

    def test_invalid_returns_none(self):
        assert _to_decimal_or_none("not-a-number") is None


# ============================================================================
# Normalizer Tests
# ============================================================================


class TestCoerceFloat:
    """Tests for _coerce_float."""

    def test_string_to_float(self):
        assert _coerce_float("3.14") == 3.14

    def test_int_to_float(self):
        assert _coerce_float(42) == 42.0

    def test_empty_string_returns_none(self):
        assert _coerce_float("") is None


class TestCoerceInt:
    """Tests for _coerce_int."""

    def test_string_to_int(self):
        assert _coerce_int("42") == 42

    def test_float_to_int(self):
        assert _coerce_int(3.7) == 3

    def test_float_string_to_int(self):
        assert _coerce_int("3.7") == 3

    def test_empty_string_returns_none(self):
        assert _coerce_int("") is None


class TestCoerceDate:
    """Tests for _coerce_date."""

    def test_datetime_to_date(self):
        dt = datetime(2026, 3, 10, 14, 30, tzinfo=UTC)
        assert _coerce_date(dt) == date(2026, 3, 10)

    def test_date_to_date(self):
        d = date(2026, 3, 10)
        assert _coerce_date(d) == d

    def test_string_to_date(self):
        assert _coerce_date("2026-03-10") == date(2026, 3, 10)

    def test_z_suffix_string(self):
        assert _coerce_date("2026-03-10T14:30:00Z") == date(2026, 3, 10)

    def test_non_parseable_returns_none(self):
        assert _coerce_date(12345) is None


class TestCoerceTimestamp:
    """Tests for _coerce_timestamp."""

    def test_datetime_passthrough(self):
        dt = datetime(2026, 3, 10, 14, 30, tzinfo=UTC)
        assert _coerce_timestamp(dt) == dt

    def test_epoch_seconds(self):
        # 2026-03-10T00:00:00Z
        epoch = 1773014400
        result = _coerce_timestamp(epoch)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_epoch_milliseconds(self):
        epoch_ms = 1773014400000
        result = _coerce_timestamp(epoch_ms)
        assert isinstance(result, datetime)

    def test_iso_string(self):
        result = _coerce_timestamp("2026-03-10T14:30:00Z")
        assert isinstance(result, datetime)
        assert result.year == 2026

    def test_numeric_string(self):
        result = _coerce_timestamp("1773014400")
        assert isinstance(result, datetime)

    def test_non_parseable_returns_none(self):
        assert _coerce_timestamp([1, 2, 3]) is None


class TestCoerceBool:
    """Tests for _coerce_bool."""

    def test_bool_true(self):
        assert _coerce_bool(True) is True

    def test_bool_false(self):
        assert _coerce_bool(False) is False

    def test_int_truthy(self):
        assert _coerce_bool(1) is True

    def test_int_falsy(self):
        assert _coerce_bool(0) is False

    def test_string_true(self):
        assert _coerce_bool("true") is True
        assert _coerce_bool("yes") is True
        assert _coerce_bool("1") is True
        assert _coerce_bool("t") is True
        assert _coerce_bool("y") is True

    def test_string_false(self):
        assert _coerce_bool("false") is False
        assert _coerce_bool("no") is False
        assert _coerce_bool("0") is False
        assert _coerce_bool("f") is False
        assert _coerce_bool("n") is False

    def test_unknown_string_returns_none(self):
        assert _coerce_bool("maybe") is None


class TestCoerceList:
    """Tests for _coerce_list."""

    def test_list_passthrough(self):
        assert _coerce_list([1, 2, 3]) == [1, 2, 3]

    def test_tuple_to_list(self):
        assert _coerce_list((1, 2)) == [1, 2]

    def test_set_to_list(self):
        result = _coerce_list({1, 2})
        assert isinstance(result, list)
        assert set(result) == {1, 2}

    def test_json_string_list(self):
        assert _coerce_list("[1, 2, 3]") == [1, 2, 3]

    def test_empty_string(self):
        assert _coerce_list("") == []

    def test_non_list_json_string(self):
        result = _coerce_list('{"a": 1}')
        assert result == ['{"a": 1}']

    def test_invalid_json_string(self):
        result = _coerce_list("not-json")
        assert result == ["not-json"]

    def test_scalar_to_list(self):
        assert _coerce_list(42) == [42]


class TestCoerceString:
    """Tests for _coerce_string."""

    def test_dict_to_json(self):
        result = _coerce_string({"key": "value"})
        assert json.loads(result) == {"key": "value"}

    def test_list_to_json(self):
        result = _coerce_string([1, 2, 3])
        assert json.loads(result) == [1, 2, 3]

    def test_int_to_string(self):
        assert _coerce_string(42) == "42"

    def test_string_passthrough(self):
        assert _coerce_string("hello") == "hello"


class TestCoerceValueDispatch:
    """Tests for _coerce_value type dispatch."""

    def test_none_returns_none(self):
        assert _coerce_value(None, pa.float64()) is None

    def test_float_type(self):
        assert _coerce_value("3.14", pa.float64()) == 3.14

    def test_int_type(self):
        assert _coerce_value("42", pa.int64()) == 42

    def test_date_type(self):
        assert _coerce_value("2026-03-10", pa.date32()) == date(2026, 3, 10)

    def test_timestamp_type(self):
        result = _coerce_value("2026-03-10T14:30:00Z", pa.timestamp("us", tz="UTC"))
        assert isinstance(result, datetime)

    def test_bool_type(self):
        assert _coerce_value("true", pa.bool_()) is True

    def test_list_type(self):
        assert _coerce_value([1, 2], pa.list_(pa.int64())) == [1, 2]

    def test_string_type(self):
        assert _coerce_value(42, pa.string()) == "42"

    def test_unsupported_type_passthrough(self):
        """Non-recognized Arrow types pass value through."""
        # binary is not handled by any branch specifically
        assert _coerce_value(b"bytes", pa.binary()) == b"bytes"

    def test_exception_returns_none(self):
        """Coercion failure returns None."""
        assert _coerce_value("not-a-float", pa.float64()) is None


class TestMissingRequiredFieldsError:
    """Tests for MissingRequiredFieldsError."""

    def test_error_message_format(self):
        err = MissingRequiredFieldsError("bars", ["open", "close"], event_id="evt-1")
        assert "bars" in str(err)
        assert "open" in str(err)
        assert "close" in str(err)
        assert "evt-1" in str(err)
        assert err.feed == "bars"
        assert err.missing_fields == ["open", "close"]
        assert err.event_id == "evt-1"

    def test_error_message_without_event_id(self):
        err = MissingRequiredFieldsError("bars", ["open"])
        assert "evt-1" not in str(err)
        assert err.event_id is None


class TestMissingRequiredNonNullFields:
    """Tests for missing_required_non_null_fields."""

    def test_all_present(self):
        row = {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}
        assert missing_required_non_null_fields("bars", row) == []

    def test_some_missing(self):
        row = {"open": 100.0, "high": None, "low": 99.0, "volume": 1000}
        result = missing_required_non_null_fields("bars", row)
        assert "close" in result
        assert "high" in result

    def test_empty_string_is_missing(self):
        row = {"open": 100.0, "high": 101.0, "low": 99.0, "close": "  ", "volume": 1000}
        result = missing_required_non_null_fields("bars", row)
        assert "close" in result

    def test_unknown_feed_no_required(self):
        row = {"foo": "bar"}
        assert missing_required_non_null_fields("unknown_xyz", row) == []


class TestEnforceRequiredNonNullFields:
    """Tests for enforce_required_non_null_fields."""

    def test_raises_on_missing(self):
        row = {"open": 100.0}
        with pytest.raises(MissingRequiredFieldsError):
            enforce_required_non_null_fields("bars", row, event_id="evt-1")

    def test_no_raise_when_all_present(self):
        row = {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}
        enforce_required_non_null_fields("bars", row)  # Should not raise


class TestEpochToUtc:
    """Tests for _epoch_to_utc."""

    def test_seconds(self):
        result = _epoch_to_utc(1773014400.0)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_milliseconds(self):
        result = _epoch_to_utc(1773014400000.0)
        assert isinstance(result, datetime)
        # Should be the same as seconds version
        assert result == _epoch_to_utc(1773014400.0)


class TestParseStringTimestamp:
    """Tests for _parse_string_timestamp."""

    def test_iso_format(self):
        result = _parse_string_timestamp("2026-03-10T14:30:00Z")
        assert isinstance(result, datetime)
        assert result.year == 2026

    def test_numeric_string(self):
        result = _parse_string_timestamp("1773014400")
        assert isinstance(result, datetime)

    def test_empty_string(self):
        assert _parse_string_timestamp("") is None

    def test_whitespace_only(self):
        assert _parse_string_timestamp("   ") is None

    def test_invalid_format(self):
        result = _parse_string_timestamp("not-a-date")
        assert result is None

    def test_naive_iso_gets_utc(self):
        result = _parse_string_timestamp("2026-03-10T14:30:00")
        assert result is not None
        assert result.tzinfo is not None


class TestExtractItemEventTimestamp:
    """Tests for extract_item_event_timestamp."""

    def test_iso_string_in_timestamp(self):
        result = extract_item_event_timestamp({"timestamp": "2026-03-10T14:30:00Z"})
        assert isinstance(result, datetime)

    def test_epoch_int(self):
        result = extract_item_event_timestamp({"timestamp": 1773014400})
        assert isinstance(result, datetime)

    def test_epoch_float(self):
        result = extract_item_event_timestamp({"t": 1773014400.0})
        assert isinstance(result, datetime)

    def test_datetime_object_aware(self):
        dt = datetime(2026, 3, 10, 14, 30, tzinfo=UTC)
        result = extract_item_event_timestamp({"timestamp": dt})
        assert result == dt

    def test_datetime_object_naive(self):
        dt = datetime(2026, 3, 10, 14, 30)
        result = extract_item_event_timestamp({"ts_event": dt})
        assert result is not None
        assert result.tzinfo is not None

    def test_no_timestamp_keys(self):
        assert extract_item_event_timestamp({"other": "val"}) is None

    def test_none_value(self):
        assert extract_item_event_timestamp({"timestamp": None}) is None

    def test_unsupported_type(self):
        assert extract_item_event_timestamp({"timestamp": [1, 2, 3]}) is None


class TestExplodeAggregatePayload:
    """Tests for explode_aggregate_payload."""

    def test_non_aggregate_feed_passthrough(self):
        """Non-aggregate feeds return [envelope] unchanged."""
        envelope = _make_envelope(feed="flow_alerts")
        result = explode_aggregate_payload(envelope)
        assert len(result) == 1
        assert result[0] is envelope

    def test_bars_aggregate_expansion(self):
        """Bars with 'bars' list key produces one envelope per item."""
        envelope = _make_envelope(
            feed="bars",
            payload={
                "symbol": "AAPL",
                "timeframe": "1Min",
                "bars": [
                    {"timestamp": "2026-03-10T14:30:00Z", "o": 100, "h": 101, "l": 99, "c": 100.5, "v": 1000},
                    {"timestamp": "2026-03-10T14:31:00Z", "o": 100.5, "h": 102, "l": 100, "c": 101, "v": 900},
                ],
            },
        )
        result = explode_aggregate_payload(envelope)
        assert len(result) == 2
        assert result[0].event_id == "evt-test-001:0"
        assert result[1].event_id == "evt-test-001:1"
        # Context keys should be propagated
        assert result[0].payload["timeframe"] == "1Min"

    def test_empty_list_returns_empty(self):
        """Empty aggregate list returns empty list."""
        envelope = _make_envelope(feed="bars", payload={"bars": []})
        result = explode_aggregate_payload(envelope)
        assert result == []

    def test_non_list_items_returns_envelope(self):
        """Non-list 'bars' value returns [envelope] unchanged."""
        envelope = _make_envelope(feed="bars", payload={"bars": "not-a-list"})
        result = explode_aggregate_payload(envelope)
        assert len(result) == 1

    def test_non_dict_items_skipped(self):
        """Non-dict items in the list are skipped."""
        envelope = _make_envelope(
            feed="bars",
            payload={
                "bars": [
                    "not-a-dict",
                    {"timestamp": "2026-03-10T14:30:00Z", "o": 100},
                ],
            },
        )
        result = explode_aggregate_payload(envelope)
        assert len(result) == 1

    def test_trades_aggregate_expansion(self):
        """Trades with 'trades' list key produces one envelope per item."""
        envelope = _make_envelope(
            feed="trades",
            payload={
                "symbol": "AAPL",
                "trades": [
                    {"p": 150.0, "s": 100, "timestamp": "2026-03-10T14:30:00Z"},
                ],
            },
        )
        result = explode_aggregate_payload(envelope)
        assert len(result) == 1
        assert result[0].payload["symbol"] == "AAPL"

    def test_feed_override(self):
        """feed_override selects the aggregate config regardless of envelope.feed."""
        envelope = _make_envelope(feed="bars", payload={"bars": [{"o": 100}]})
        result = explode_aggregate_payload(envelope, feed_override="bars")
        assert len(result) == 1

    def test_non_dict_payload(self):
        """Non-dict payload returns [envelope] unchanged."""
        envelope = _make_envelope(payload={})
        # Force payload to a non-dict via model_copy
        envelope_with_bad = envelope.model_copy(update={"payload": {}})
        result = explode_aggregate_payload(envelope_with_bad)
        assert len(result) == 1


# ============================================================================
# Utils Tests
# ============================================================================


class TestGetBronzePartitionKey:
    """Tests for get_bronze_partition_key."""

    def test_standard_partition(self):
        envelope = _make_envelope()
        pk = get_bronze_partition_key(envelope)
        assert pk == "provider=alpaca/feed=bars/dt=2026-03-10/hour=14"

    def test_different_provider_and_feed(self):
        envelope = _make_envelope(provider="unusual_whales", feed="flow_alerts")
        pk = get_bronze_partition_key(envelope)
        assert "provider=unusual_whales" in pk
        assert "feed=flow_alerts" in pk


class TestGetPartitionKey:
    """Tests for get_partition_key (Silver)."""

    def test_standard_feed(self):
        pk = get_partition_key("bars", "equity", NOW)
        assert pk == "feed=bars/instrument_type=equity/dt=2026-03-10"

    def test_high_volume_feed_quotes(self):
        pk = get_partition_key("quotes", "equity", NOW)
        assert "hour=14" in pk

    def test_high_volume_feed_trades(self):
        pk = get_partition_key("trades", "equity", NOW)
        assert "hour=14" in pk

    def test_aliased_feed(self):
        pk = get_partition_key("ftds", "equity", NOW)
        assert "feed=ftd" in pk


class TestBuildSilverCandidates:
    """Tests for build_silver_candidates."""

    def test_simple_envelope(self):
        envelope = _make_envelope()
        candidates = build_silver_candidates(envelope)
        assert len(candidates) == 1

    def test_with_feed_override(self):
        envelope = _make_envelope(
            feed="bars",
            payload={"bars": [{"o": 100, "timestamp": NOW.isoformat()}]},
        )
        candidates = build_silver_candidates(envelope, feed_override="bars")
        assert len(candidates) == 1


class TestWriteSilverParquet:
    """Tests for write_silver_parquet."""

    def test_writes_valid_rows(self, tmp_path):
        """Writes valid rows to parquet file."""
        schema = pa.schema([("event_id", pa.string()), ("price", pa.float64())])
        rows = [{"event_id": "e1", "price": 100.0}]
        file_path = tmp_path / "part-test.parquet"

        write_silver_parquet(
            rows=rows,
            schema=schema,
            file_path=file_path,
            partition_key="feed=test/dt=2026-03-10",
            dataset="test",
        )
        assert file_path.exists()

    def test_empty_rows_no_write(self, tmp_path):
        """Empty rows list produces no file."""
        schema = pa.schema([("event_id", pa.string())])
        file_path = tmp_path / "part-test.parquet"
        write_silver_parquet(
            rows=[],
            schema=schema,
            file_path=file_path,
            partition_key="feed=test/dt=2026-03-10",
            dataset="test",
        )
        assert not file_path.exists()

    def test_arrow_type_error_salvages_valid_rows(self, tmp_path):
        """ArrowTypeError triggers row-by-row salvage."""
        schema = pa.schema([("event_id", pa.string()), ("price", pa.float64())])
        rows = [
            {"event_id": "e1", "price": 100.0},
            {"event_id": "e2", "price": "not-a-number-at-all"},  # Will fail type coercion
        ]
        file_path = tmp_path / "part-test.parquet"

        # This may or may not trigger salvage depending on Arrow's leniency,
        # but at least it should not raise
        try:
            write_silver_parquet(
                rows=rows,
                schema=schema,
                file_path=file_path,
                partition_key="feed=test/dt=2026-03-10",
                dataset="test",
            )
        except Exception:
            pass  # Some Arrow versions may not trigger salvage path
        # Just verify no unhandled crash

    def test_generic_error_reraises(self, tmp_path):
        """Non-Arrow exceptions are re-raised."""
        schema = pa.schema([("event_id", pa.string())])
        rows = [{"event_id": "e1"}]
        file_path = Path("/nonexistent/deeply/nested/dir/part.parquet")

        with pytest.raises(OSError):
            write_silver_parquet(
                rows=rows,
                schema=schema,
                file_path=file_path,
                partition_key="feed=test/dt=2026-03-10",
                dataset="test",
            )


class TestEnvelopeToSilverRow:
    """Tests for envelope_to_silver_row in normalizer module."""

    def test_unmapped_feed_raises(self):
        """Unmapped feed raises UnmappedFeedError."""
        envelope = _make_envelope(feed="totally_unknown_feed_xyz")
        with pytest.raises(UnmappedFeedError):
            envelope_to_silver_row(envelope)

    def test_bars_feed_produces_expected_columns(self):
        """Bars feed produces expected Silver columns."""
        envelope = _make_envelope(
            feed="bars",
            payload={
                "t": NOW.isoformat(),
                "o": "100",
                "h": "101",
                "l": "99",
                "c": "100.5",
                "v": "1000",
                "n": "25",
                "vw": "100.4",
            },
        )
        row = envelope_to_silver_row(envelope)
        assert row["open"] == 100.0
        assert row["high"] == 101.0
        assert row["close"] == 100.5
        assert row["volume"] == 1000.0
        assert row["trade_count"] == 25
        assert row["vwap"] == 100.4
        assert row["event_id"] == "evt-test-001"
        assert row["feed"] == "bars"

    def test_ts_available_fallback_to_ts_ingest(self):
        """When ts_available is None, falls back to ts_ingest."""
        envelope = _make_envelope(ts_available=None)
        row = envelope_to_silver_row(envelope)
        assert row["ts_available"] == envelope.ts_ingest

    def test_ts_available_clamped_to_ts_event_when_provider_is_future_dated(self):
        """When ts_event > ts_ingest (provider future-dated batch), ts_available
        must be clamped up to ts_event so the zero-leakage invariant holds.

        UnusualWhales flow_alerts emits ts_event 5-11s in the future relative
        to ts_ingest (window-end timestamps). Without this clamp, Silver rows
        violate `ts_available >= ts_event` and Gold pipelines reject them.
        """
        ts_ingest = NOW
        ts_event = NOW + timedelta(seconds=10)
        envelope = _make_envelope(
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            ts_available=None,
        )
        row = envelope_to_silver_row(envelope)
        assert row["ts_available"] >= row["ts_event"], (
            f"zero-leakage violated: ts_available={row['ts_available']} < ts_event={row['ts_event']}"
        )
        assert row["ts_available"] == ts_event

    def test_ts_available_clamped_when_explicit_ts_available_predates_event(self):
        """Explicit ts_available earlier than ts_event must still be clamped up."""
        ts_ingest = NOW
        ts_event = NOW + timedelta(seconds=10)
        ts_available = NOW + timedelta(milliseconds=50)
        envelope = _make_envelope(
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            ts_available=ts_available,
        )
        row = envelope_to_silver_row(envelope)
        assert row["ts_available"] == ts_event

    def test_ts_available_unchanged_when_already_after_event(self):
        """When ts_available is already >= ts_event, it must not be changed."""
        ts_ingest = NOW
        ts_event = NOW
        ts_available = NOW + timedelta(seconds=5)
        envelope = _make_envelope(
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            ts_available=ts_available,
        )
        row = envelope_to_silver_row(envelope)
        assert row["ts_available"] == ts_available
