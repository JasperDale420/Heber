from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import heber.writer.consumer as consumer_module
from heber.writer.consumer import EventConsumer

NOW = datetime(2026, 2, 11, 15, 0, tzinfo=UTC)


def _bars_payload(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "provider": "alpaca",
        "feed": "bars",
        "source": "rest",
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


def test_duplicate_event_id_is_dropped_before_any_second_write() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write_row = MagicMock()

    event_data = {"data": json.dumps(_bars_payload("evt-duplicate-bars"))}

    first_success, _, _ = consumer._process_event_once(event_data)
    second_success, second_error, second_retryable = consumer._process_event_once(event_data)

    assert first_success is True
    assert second_success is True
    assert second_error is None
    assert second_retryable is True
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write_row.assert_called_once()


def test_duplicate_event_id_records_drop_metric_and_status(monkeypatch) -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write_row = MagicMock()

    dedupe_drop_mock = MagicMock()
    processed_mock = MagicMock()
    log_info_mock = MagicMock()

    monkeypatch.setattr(consumer_module, "record_dedupe_drop", dedupe_drop_mock)
    monkeypatch.setattr(consumer_module, "record_event_processed", processed_mock)
    monkeypatch.setattr(consumer_module.logger, "info", log_info_mock)

    event_data = {"data": json.dumps(_bars_payload("evt-duplicate-metric"))}

    consumer._process_event_once(event_data)
    consumer._process_event_once(event_data)

    dedupe_drop_mock.assert_called_once_with(feed="bars")
    assert any(call.kwargs.get("status") == "dropped" for call in processed_mock.call_args_list)
    assert any(call.args and call.args[0] == "consumer_dedupe_dropped" for call in log_info_mock.call_args_list)


def test_failed_event_is_not_registered_as_duplicate() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()

    bad_event = {
        "data": json.dumps(
            {
                "event_id": "evt-invalid-repeat",
                "provider": "unusual_whales",
                "feed": "insider_trades",
                "source": "rest",
                "instrument_type": "equity",
                "instrument_key": "equity:",
                "symbol": "",
                "ts_event": NOW.isoformat(),
                "ts_ingest": NOW.isoformat(),
                "payload": {
                    "ticker": "",
                    "transaction_date": NOW.date().isoformat(),
                },
            }
        )
    }

    first_success, first_error, first_retryable = consumer._process_event_once(bad_event)
    second_success, second_error, second_retryable = consumer._process_event_once(bad_event)

    assert first_success is False
    assert first_error is not None
    assert first_retryable is False
    assert second_success is False
    assert second_error is not None
    assert second_retryable is False
    assert consumer.bronze_writer.write.call_count == 2
