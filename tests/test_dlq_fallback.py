"""Unit tests for the durable DLQ file fallback path."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

import heber.watch.consumer as watch_consumer_module
import heber.writer.consumer as consumer_module
import heber.writer.dlq_fallback as dlq_fallback_module
from heber.watch.consumer import AlertWatchConsumer
from heber.writer.consumer import EventConsumer
from heber.writer.dlq_fallback import (
    count_fallback_files_for_today,
    try_xadd_with_retry,
    write_dlq_fallback_file,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_write_dlq_fallback_file_atomic_write(tmp_path) -> None:
    path = write_dlq_fallback_file(
        fallback_root=tmp_path,
        stream="heber:events:dlq",
        event_id="evt-1234",
        payload={"foo": "bar", "n": 1},
    )

    assert path.exists()
    assert path.suffix == ".json"
    assert path.parent.name.startswith("dt=")
    assert json.loads(path.read_text()) == {"foo": "bar", "n": 1}

    sibling_temps = [p for p in path.parent.iterdir() if p.suffix == ".tmp"]
    assert sibling_temps == []


def test_write_dlq_fallback_file_filename_safe_for_colon_streams(tmp_path) -> None:
    path = write_dlq_fallback_file(
        fallback_root=tmp_path,
        stream="heber:events:dlq",
        event_id="evt/with:weird chars",
        payload={"ok": True},
    )

    assert ":" not in path.name
    assert " " not in path.name


def test_write_dlq_fallback_file_is_deterministic_for_same_event(tmp_path) -> None:
    p1 = write_dlq_fallback_file(tmp_path, "stream", "evt-1", {"v": 1})
    p2 = write_dlq_fallback_file(tmp_path, "stream", "evt-1", {"v": 2})

    assert p1 == p2
    # Second write replaces the first atomically; payload reflects v=2.
    assert json.loads(p2.read_text()) == {"v": 2}


def test_count_fallback_files_for_today_empty_dir(tmp_path) -> None:
    assert count_fallback_files_for_today(tmp_path / "does-not-exist") == 0


def test_count_fallback_files_for_today_counts_only_json(tmp_path) -> None:
    write_dlq_fallback_file(tmp_path, "stream", "evt-a", {})
    write_dlq_fallback_file(tmp_path, "stream", "evt-b", {})
    # An unrelated file in the dt=YYYY-MM-DD partition should be ignored.
    today_dir = next(p for p in tmp_path.iterdir() if p.name.startswith("dt="))
    (today_dir / "notes.txt").write_text("ignore me")

    assert count_fallback_files_for_today(tmp_path) == 2


@pytest.mark.asyncio
async def test_try_xadd_with_retry_succeeds_on_second_attempt() -> None:
    sleep_calls: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    call = AsyncMock(side_effect=[RuntimeError("boom"), "1-0"])

    success, result, last_error = await try_xadd_with_retry(
        call,
        retry_delays=(0.5, 1.0, 2.0),
        sleep=_capture_sleep,
    )

    assert success is True
    assert result == "1-0"
    assert last_error is None
    assert call.await_count == 2
    assert sleep_calls == [0.5]


@pytest.mark.asyncio
async def test_try_xadd_with_retry_exhausts_all_attempts() -> None:
    sleep_calls: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    call = AsyncMock(side_effect=RuntimeError("always fails"))

    success, result, last_error = await try_xadd_with_retry(
        call,
        retry_delays=(0.5, 1.0, 2.0),
        sleep=_capture_sleep,
    )

    assert success is False
    assert result is None
    assert isinstance(last_error, RuntimeError)
    assert call.await_count == 4
    assert sleep_calls == [0.5, 1.0, 2.0]


# ---------------------------------------------------------------------------
# Writer consumer integration
# ---------------------------------------------------------------------------


class _AlwaysFailingRedis:
    async def xadd(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("dlq unavailable")


class _FlakyRedis:
    """Fails the first ``fail_count`` calls, then succeeds."""

    def __init__(self, fail_count: int = 1) -> None:
        self.fail_count = fail_count
        self.calls = 0
        self.added: list[tuple] = []

    async def xadd(self, stream: str, payload: dict):  # noqa: ANN001
        self.calls += 1
        if self.calls <= self.fail_count:
            raise RuntimeError("transient redis error")
        self.added.append((stream, payload))
        return "9-9"


async def _noop_sleep(_delay: float) -> None:
    return None


@pytest.mark.asyncio
async def test_send_to_dlq_writes_file_when_redis_always_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    consumer = EventConsumer()
    consumer.redis = _AlwaysFailingRedis()
    monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)
    monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _noop_sleep)

    critical_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "critical", critical_mock)

    moved = await consumer._send_to_dlq(
        message_id="42-0",
        message_data={b"data": b'{"foo":"bar"}'},
        error="boom",
        attempts=3,
        feed="flow_alerts",
    )

    assert moved is True
    fallback_files = list(tmp_path.rglob("*.json"))
    assert len(fallback_files) == 1

    content = json.loads(fallback_files[0].read_text())
    assert content["source_message_id"] == "42-0"
    assert content["error"] == "boom"
    assert content["attempts"] == "3"
    payload_body = json.loads(content["payload"])
    assert payload_body["data"] == '{"foo":"bar"}'

    critical_mock.assert_called_once()
    log_event_name = critical_mock.call_args.args[0]
    assert log_event_name == "dlq_file_fallback_used"
    assert critical_mock.call_args.kwargs["event_id"] == "42-0"
    assert "fallback_path" in critical_mock.call_args.kwargs


@pytest.mark.asyncio
async def test_send_to_dlq_skips_fallback_when_redis_succeeds_on_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    consumer = EventConsumer()
    flaky = _FlakyRedis(fail_count=1)
    consumer.redis = flaky
    monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)
    monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _noop_sleep)

    critical_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "critical", critical_mock)

    moved = await consumer._send_to_dlq(
        message_id="7-0",
        message_data={b"data": b"{}"},
        error="boom",
        attempts=3,
        feed="bars",
    )

    assert moved is True
    assert flaky.calls == 2
    assert list(tmp_path.rglob("*.json")) == []
    critical_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Watch consumer integration
# ---------------------------------------------------------------------------


class _AlwaysFailingSyncRedis:
    def xadd(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("dlq unavailable")


class _FlakySyncRedis:
    def __init__(self, fail_count: int = 1) -> None:
        self.fail_count = fail_count
        self.calls = 0
        self.added: list[tuple] = []

    def xadd(self, stream: str, payload: dict):  # noqa: ANN001
        self.calls += 1
        if self.calls <= self.fail_count:
            raise RuntimeError("transient redis error")
        self.added.append((stream, payload))
        return "1-2"


class _NoopManager:
    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        return None


@pytest.mark.asyncio
async def test_watch_dead_letter_writes_file_when_redis_always_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    consumer = AlertWatchConsumer(
        _AlwaysFailingSyncRedis(),
        _NoopManager(),
        max_process_retries=2,
        retry_backoff_seconds=0.0,
        dlq_stream_name="heber:watch:dlq",
    )
    monkeypatch.setattr(watch_consumer_module.settings, "dlq_fallback_dir", tmp_path)
    monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _noop_sleep)

    critical_mock = MagicMock()
    monkeypatch.setattr(watch_consumer_module.logger, "critical", critical_mock)

    ackable = await consumer._dead_letter_message(
        msg_id="9-1",
        data={b"data": b"{}"},
        attempts=2,
        error="processing_failed",
    )

    assert ackable is True
    fallback_files = list(tmp_path.rglob("*.json"))
    assert len(fallback_files) == 1

    content = json.loads(fallback_files[0].read_text())
    assert content["origin_message_id"] == "9-1"
    assert content["error"] == "processing_failed"

    critical_mock.assert_called_once()
    log_event_name = critical_mock.call_args.args[0]
    assert log_event_name == "dlq_file_fallback_used"
    assert critical_mock.call_args.kwargs["event_id"] == "9-1"


@pytest.mark.asyncio
async def test_watch_dead_letter_skips_fallback_when_redis_succeeds_on_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    flaky = _FlakySyncRedis(fail_count=1)
    consumer = AlertWatchConsumer(
        flaky,
        _NoopManager(),
        max_process_retries=1,
        retry_backoff_seconds=0.0,
        dlq_stream_name="heber:watch:dlq",
    )
    monkeypatch.setattr(watch_consumer_module.settings, "dlq_fallback_dir", tmp_path)
    monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _noop_sleep)

    critical_mock = MagicMock()
    monkeypatch.setattr(watch_consumer_module.logger, "critical", critical_mock)

    ackable = await consumer._dead_letter_message(
        msg_id="3-3",
        data={b"data": b"{}"},
        attempts=1,
        error="processing_failed",
    )

    assert ackable is True
    assert flaky.calls == 2
    assert list(tmp_path.rglob("*.json")) == []
    critical_mock.assert_not_called()


def test_log_fallback_backlog_emits_info(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    write_dlq_fallback_file(tmp_path, "stream", "evt-1", {})
    write_dlq_fallback_file(tmp_path, "stream", "evt-2", {})

    info_mock = MagicMock()
    monkeypatch.setattr(dlq_fallback_module.logger, "info", info_mock)

    dlq_fallback_module.log_fallback_backlog(tmp_path, service="heber-test")

    info_mock.assert_called_once()
    _msg, kwargs = info_mock.call_args.args, info_mock.call_args.kwargs
    assert kwargs["file_count"] == 2
    assert kwargs["service"] == "heber-test"
