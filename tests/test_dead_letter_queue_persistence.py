from __future__ import annotations

from pathlib import Path

from heber.ops.reliability import DeadLetterQueue


def _sample_error(message: str) -> RuntimeError:
    return RuntimeError(message)


def test_dead_letter_queue_persists_and_recovers_events(tmp_path: Path) -> None:
    persist_path = tmp_path / "dlq" / "events.json"

    queue = DeadLetterQueue(max_size=10, persist_path=persist_path)
    queue.add(
        event_id="event-1",
        payload={"foo": "bar"},
        error=_sample_error("broken payload"),
        feed="bars",
        provider="alpaca",
    )

    recovered = DeadLetterQueue(max_size=10, persist_path=persist_path)
    assert len(recovered) == 1
    event = recovered.peek(1)[0]
    assert event.event_id == "event-1"
    assert event.original_payload == {"foo": "bar"}
    assert event.error_type == "RuntimeError"
    assert event.error_message == "broken payload"


def test_dead_letter_queue_persists_attempt_updates(tmp_path: Path) -> None:
    persist_path = tmp_path / "dlq" / "events.json"

    queue = DeadLetterQueue(max_size=10, persist_path=persist_path)
    queue.add(
        event_id="event-2",
        payload={"id": 2},
        error=_sample_error("first failure"),
        feed="quotes",
        provider="uw",
    )
    queue.add(
        event_id="event-2",
        payload={"id": 2},
        error=_sample_error("second failure"),
        feed="quotes",
        provider="uw",
    )

    recovered = DeadLetterQueue(max_size=10, persist_path=persist_path)
    assert len(recovered) == 1
    event = recovered.peek(1)[0]
    assert event.attempts == 2
    assert event.error_message == "second failure"


def test_dead_letter_queue_pop_updates_persisted_state(tmp_path: Path) -> None:
    persist_path = tmp_path / "dlq" / "events.json"

    queue = DeadLetterQueue(max_size=10, persist_path=persist_path)
    queue.add(
        event_id="event-3",
        payload={"id": 3},
        error=_sample_error("failed"),
        feed="trades",
        provider="alpaca",
    )
    popped = queue.pop()

    assert popped is not None
    recovered = DeadLetterQueue(max_size=10, persist_path=persist_path)
    assert len(recovered) == 0
    assert recovered.stats["reprocessed"] == 1
