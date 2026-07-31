"""Gold poller result-state and retry regression tests."""

from __future__ import annotations

import asyncio
import errno
import threading
import time
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

import heber.gold_poller.service as service
from heber.config import Settings
from heber.gold_poller.service import GoldFeaturePoller

_START = date(2026, 7, 29)
_END = date(2026, 7, 30)


def _entry(name: str = "sample") -> dict:
    return {
        "name": name,
        "module": "tests.unit._isolation_pipelines",
        "class": "HealthyPipeline",
        "datasets": None,
        "gold_datasets": ["sample_features"],
        "silver_sources": ("bars",),
    }


def _poller() -> GoldFeaturePoller:
    poller = GoldFeaturePoller()
    poller._settings = SimpleNamespace(
        gold_poller_retry_max=3,
        gold_poller_retry_backoff_seconds=0.0,
        gold_poller_pipeline_timeout_seconds=900,
    )
    return poller


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["source_discovery", "pipeline"])
def test_blocked_process_start_respects_gold_deadline_and_fences_late_child(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    start_entered = threading.Event()
    release_start = threading.Event()

    class _EmptyQueue:
        def get(self, *, timeout: float) -> None:
            raise service.queue.Empty

        def get_nowait(self) -> None:
            raise service.queue.Empty

    class _LateProcess:
        exitcode = None

        def __init__(self) -> None:
            self.alive = False
            self.killed = False

        def start(self) -> None:
            start_entered.set()
            release_start.wait()
            self.alive = True

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            self.alive = False

        def kill(self) -> None:
            self.killed = True
            self.alive = False

        def join(self, _timeout: float | None = None) -> None:
            return None

    process = _LateProcess()

    class _Context:
        def Queue(self) -> _EmptyQueue:
            return _EmptyQueue()

        def Process(self, **_kwargs) -> _LateProcess:
            return process

    monkeypatch.setattr(service.multiprocessing, "get_context", lambda _method: _Context())
    poller = _poller()
    errors: list[BaseException] = []

    def _execute() -> None:
        try:
            if operation == "source_discovery":
                poller._execute_source_discovery_isolated(
                    [_entry()],
                    _START,
                    _END,
                    run_deadline=time.monotonic() + 0.05,
                )
            else:
                poller._execute_pipeline_isolated(
                    _entry(),
                    _START,
                    _END,
                    max_runtime_seconds=0.05,
                )
        except BaseException as exc:  # noqa: BLE001 - assert the worker outcome below
            errors.append(exc)

    worker = threading.Thread(target=_execute)
    worker.start()
    assert start_entered.wait(1.0)
    worker.join(0.2)
    completed_within_deadline = not worker.is_alive()
    release_start.set()
    worker.join(1.0)

    assert completed_within_deadline is True
    assert errors and isinstance(errors[0], service._PipelineTimeoutError)
    assert len(poller._unreaped_children) == 1
    poller._unreaped_children[0][1].join(1.0)
    assert poller._reap_unreaped_children() is True
    assert process.alive is False


@pytest.mark.unit
async def test_expected_silver_input_turns_zero_output_into_error(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = _poller()
    monkeypatch.setattr(
        poller,
        "_execute_pipeline_isolated",
        lambda *_args, **_kwargs: {"sample_features": {"status": "no_data", "rows": 0}},
    )

    result = await poller._run_pipeline(
        _entry(),
        _START,
        _END,
        expected_source_partitions={"bars": ("2026-07-30",)},
        run_deadline=float("inf"),
    )

    assert result["status"] == "error"
    assert result["attempts"] == 1


@pytest.mark.unit
async def test_no_source_partitions_preserves_no_data(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = _poller()
    monkeypatch.setattr(
        poller,
        "_execute_pipeline_isolated",
        lambda *_args, **_kwargs: {"sample_features": {"status": "no_data", "rows": 0}},
    )

    result = await poller._run_pipeline(
        _entry(),
        _START,
        _END,
        expected_source_partitions={"bars": ()},
        run_deadline=float("inf"),
    )

    assert result["status"] == "no_data"


@pytest.mark.unit
async def test_timeout_is_terminal_and_does_not_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = _poller()
    calls = 0

    def _timeout(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise TimeoutError("timed out")

    monkeypatch.setattr(poller, "_execute_pipeline_isolated", _timeout)

    result = await poller._run_pipeline(
        _entry(),
        _START,
        _END,
        expected_source_partitions={"bars": ()},
        run_deadline=float("inf"),
    )

    assert calls == 1
    assert result["status"] == "error"
    assert result["attempts"] == 1
    assert result["terminal"] is True


@pytest.mark.unit
async def test_transient_os_error_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = _poller()
    calls = 0

    def _flaky(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError(errno.ETIMEDOUT, "temporary I/O failure")
        return {"sample_features": {"status": "success", "rows": 1}}

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(poller, "_execute_pipeline_isolated", _flaky)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    result = await poller._run_pipeline(
        _entry(),
        _START,
        _END,
        expected_source_partitions={"bars": ()},
        run_deadline=float("inf"),
    )

    assert calls == 2
    assert result["status"] == "success"
    assert result["attempts"] == 2


@pytest.mark.unit
async def test_non_transient_os_error_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    poller = _poller()
    calls = 0

    def _permission_denied(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise OSError(errno.EACCES, "permission denied")

    monkeypatch.setattr(poller, "_execute_pipeline_isolated", _permission_denied)

    result = await poller._run_pipeline(
        _entry(),
        _START,
        _END,
        expected_source_partitions={"bars": ()},
        run_deadline=float("inf"),
    )

    assert calls == 1
    assert result["status"] == "error"
    assert result["terminal"] is True


@pytest.mark.unit
def test_each_scheduled_pipeline_has_a_silver_expectation_source() -> None:
    enabled = [
        entry for entry in service.PIPELINE_REGISTRY if entry["name"] not in service._SCHEDULE_DISABLED_PIPELINES
    ]

    assert len(enabled) == 15
    assert all(entry.get("silver_sources") or entry.get("gold_sources") for entry in enabled)


@pytest.mark.unit
def test_pipeline_source_registry_matches_each_pipeline_reader_contract() -> None:
    entries = {entry["name"]: entry for entry in service.PIPELINE_REGISTRY}

    assert entries["equity_features"]["silver_sources"] == ("bars", "quotes", "flow_alerts")
    assert entries["gex_regime"]["silver_sources"] == ("greek_exposure",)
    assert entries["darkpool"]["silver_sources"] == ("darkpool", "flow_alerts")
    assert entries["flow_toxicity"]["silver_sources"] == ("flow_alerts",)
    assert entries["market_intel"]["silver_sources"] == (
        "greek_exposure",
        "iv_rank",
        "market_tide",
        "sector_tide",
        "ftd",
    )
    assert entries["market_tide_context"]["silver_sources"] == ("market_tide",)
    assert entries["sector_flow"]["silver_sources"] == ("flow_alerts", "sector_tide")
    assert entries["oi_momentum"]["silver_sources"] == ("oi_change",)
    assert entries["straddle_momentum"]["silver_sources"] == ("option_chain_snapshot",)
    assert entries["trend_scan"]["silver_sources"] == ("bars",)
    assert entries["flow_context"]["silver_sources"] == ("flow_alerts",)
    assert entries["market_regime"]["silver_sources"] == ("bars", "quotes", "treasury_yields")
    assert entries["market_regime"]["gold_sources"] == (
        {"dataset": "momentum_features", "project_from_settings": True},
    )
    assert entries["iv_surface"]["silver_sources"] == ("iv_term_structure",)
    assert entries["flow_normalization"]["silver_sources"] == ("flow_alerts",)
    assert entries["ticker_base_rates"]["gold_sources"] == ({"dataset": "labels_alert_barriers", "project": "watch"},)


@pytest.mark.unit
def test_snapshot_tracks_declared_gold_input_partitions() -> None:
    class _SnapshotReader:
        def completed_silver_partitions(self, *_args, **_kwargs) -> frozenset[str]:
            return frozenset()

        def completed_gold_partitions(self, dataset: str, **kwargs) -> frozenset[str]:
            assert dataset == "labels_alert_barriers"
            assert kwargs["project"] == "watch"
            return frozenset({"2026-07-30"})

    poller = _poller()
    poller._reader = _SnapshotReader()
    snapshot = poller._capture_expected_source_partitions(
        [
            {
                "name": "ticker_base_rates",
                "gold_sources": ({"dataset": "labels_alert_barriers", "project": "watch"},),
            }
        ],
        _START,
        _END,
    )

    assert snapshot == {"ticker_base_rates": {"gold:labels_alert_barriers": ("2026-07-30",)}}


@pytest.mark.unit
def test_snapshot_uses_the_poller_project_for_declared_gold_source() -> None:
    class _SnapshotReader:
        def completed_silver_partitions(self, *_args, **_kwargs) -> frozenset[str]:
            return frozenset()

        def completed_gold_partitions(self, dataset: str, **kwargs) -> frozenset[str]:
            assert dataset == "momentum_features"
            assert kwargs["project"] == "orion"
            return frozenset({"2026-07-30"})

    poller = _poller()
    poller._settings.gold_poller_project = "orion"
    poller._reader = _SnapshotReader()
    snapshot = poller._capture_expected_source_partitions(
        [
            {
                "name": "market_regime",
                "gold_sources": ({"dataset": "momentum_features", "project_from_settings": True},),
            }
        ],
        _START,
        _END,
    )

    assert snapshot == {"market_regime": {"gold:momentum_features": ("2026-07-30",)}}


class _FrozenDateTime(datetime):
    _frozen: datetime

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls._frozen if tz is None else cls._frozen.astimezone(tz)


@pytest.mark.unit
async def test_run_budget_marks_unstarted_pipeline_error_and_keeps_disabled_distinct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime._frozen = datetime(2026, 7, 30, 16, 40, tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setattr(service, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        service,
        "PIPELINE_REGISTRY",
        [_entry("complete"), _entry("disabled"), _entry("over_budget")],
    )

    poller = GoldFeaturePoller()
    poller._settings = SimpleNamespace(
        gold_poller_lookback_days=1,
        gold_poller_disabled_pipeline_set={"disabled"},
        gold_poller_run_budget_seconds=1,
    )

    async def _success(*_args, **_kwargs):
        return {"status": "success", "rows": 1}

    async def _snapshot(*_args, **_kwargs):
        return {"complete": {}, "disabled": {}, "over_budget": {}}

    ticks = iter((0.0, 0.0, 2.0, 2.0))
    monkeypatch.setattr(poller, "_run_pipeline", _success)
    monkeypatch.setattr(poller, "_capture_expected_source_partitions_by_deadline", _snapshot)

    def _monotonic() -> float:
        return next(ticks, 2.0)

    monkeypatch.setattr(service.time, "monotonic", _monotonic)

    await poller._run_all_pipelines()

    results = poller._run_history[-1]["results"]
    assert results["complete"]["status"] == "success"
    assert results["disabled"]["status"] == "disabled"
    assert results["over_budget"]["status"] == "error"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("gold_poller_pipeline_timeout_seconds", 901),
        ("gold_poller_run_budget_seconds", 5401),
    ],
)
def test_settings_reject_gold_deadlines_above_hard_operational_ceilings(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


class _CrossMidnightDateTime(datetime):
    values: list[datetime] = []

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        current = cls.values.pop(0) if cls.values else datetime(2026, 7, 31, 0, 1, tzinfo=ZoneInfo("America/New_York"))
        return current if tz is None else current.astimezone(tz)


@pytest.mark.unit
async def test_run_watermark_stays_on_requested_session_after_midnight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _CrossMidnightDateTime.values = [
        datetime(2026, 7, 30, 23, 59, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 7, 31, 0, 1, tzinfo=ZoneInfo("America/New_York")),
    ]
    monkeypatch.setattr(service, "datetime", _CrossMidnightDateTime)
    monkeypatch.setattr(service, "PIPELINE_REGISTRY", [{"name": "stub", "datasets": None}])

    poller = GoldFeaturePoller()
    poller._settings = SimpleNamespace(
        gold_poller_lookback_days=1,
        gold_poller_disabled_pipeline_set=set(),
        gold_poller_run_budget_seconds=5400,
    )
    monkeypatch.setattr(poller, "_capture_expected_source_partitions", lambda *_args: {"stub": {}})

    async def _success(*_args, **_kwargs):
        return {"status": "success", "rows": 1}

    monkeypatch.setattr(poller, "_run_pipeline", _success)

    await poller._run_all_pipelines()

    assert poller._last_run_date == date(2026, 7, 30)


@pytest.mark.unit
async def test_blocking_source_discovery_exhausts_the_single_run_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenDateTime._frozen = datetime(2026, 7, 30, 16, 40, tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setattr(service, "datetime", _FrozenDateTime)
    monkeypatch.setattr(service, "PIPELINE_REGISTRY", [_entry("first"), _entry("second")])

    poller = GoldFeaturePoller()
    poller._settings = SimpleNamespace(
        gold_poller_lookback_days=1,
        gold_poller_disabled_pipeline_set=set(),
        gold_poller_run_budget_seconds=0.01,
    )

    async def _should_not_run(*_args, **_kwargs):
        raise AssertionError("pipeline execution must not start after source discovery exhausts the run deadline")

    def _timeout_discovery(*_args, **_kwargs):
        raise service._PipelineTimeoutError("Gold source discovery exceeded the run budget")

    monkeypatch.setattr(poller, "_execute_source_discovery_isolated", _timeout_discovery)
    monkeypatch.setattr(poller, "_run_pipeline", _should_not_run)

    await poller._run_all_pipelines()

    results = poller._run_history[-1]["results"]
    assert all(result["status"] == "error" for result in results.values())
    assert {result["reason"] for result in results.values()} == {"source_discovery_timeout"}
    assert all(result["terminal"] is True for result in results.values())


@pytest.mark.unit
def test_unreaped_source_discovery_child_fences_later_discovery_until_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _EmptyQueue:
        def get(self, *, timeout: float):
            raise service.queue.Empty

        def get_nowait(self):
            raise service.queue.Empty

    class _BlockingProcess:
        def __init__(self, **_kwargs) -> None:
            self.alive = True
            self.terminated = False
            self.killed = False
            self.join_timeouts: list[float] = []
            self.exitcode = None

        def start(self) -> None:
            return None

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def join(self, timeout: float) -> None:
            self.join_timeouts.append(timeout)

    class _Context:
        def __init__(self) -> None:
            self.processes: list[_BlockingProcess] = []

        def Queue(self) -> _EmptyQueue:
            return _EmptyQueue()

        def Process(self, **kwargs) -> _BlockingProcess:
            process = _BlockingProcess(**kwargs)
            self.processes.append(process)
            return process

    context = _Context()
    clock = iter((0.0, 0.0, 1.0, 1.0, 1.0))
    monkeypatch.setattr(service.multiprocessing, "get_context", lambda _method: context)
    monkeypatch.setattr(service.time, "monotonic", lambda: next(clock, 1.0))

    poller = _poller()
    with pytest.raises(service._PipelineTimeoutError, match="source discovery"):
        poller._execute_source_discovery_isolated([_entry()], _START, _END, run_deadline=1.0)

    with pytest.raises(service._TerminalPipelineError, match="unreaped child"):
        poller._execute_source_discovery_isolated([_entry()], _START, _END, run_deadline=1.0)

    assert len(context.processes) == 1
    assert context.processes[0].killed is True

    context.processes[0].alive = False
    assert poller._reap_unreaped_children() is True
    assert context.processes[0].join_timeouts[-1] == 0


@pytest.mark.unit
def test_source_discovery_rejects_an_invalid_child_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    class _ResultQueue:
        def get(self, *, timeout: float) -> dict[str, object]:
            return {"ok": True, "sources": []}

    class _CompletedProcess:
        exitcode = 0

        def start(self) -> None:
            return None

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float) -> None:
            return None

    class _Context:
        def Queue(self) -> _ResultQueue:
            return _ResultQueue()

        def Process(self, **_kwargs) -> _CompletedProcess:
            return _CompletedProcess()

    monkeypatch.setattr(service.multiprocessing, "get_context", lambda _method: _Context())
    monkeypatch.setattr(service.time, "monotonic", lambda: 0.0)

    with pytest.raises(service._TerminalPipelineError, match="invalid source-partition snapshot"):
        _poller()._execute_source_discovery_isolated([_entry()], _START, _END, run_deadline=1.0)


@pytest.mark.unit
def test_unreaped_pipeline_child_fences_successors_until_a_later_run_reaps_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _EmptyQueue:
        def get(self, *, timeout: float) -> None:
            raise service.queue.Empty

        def get_nowait(self) -> None:
            raise service.queue.Empty

    class _StatsQueue:
        def get(self, *, timeout: float) -> dict[str, object]:
            return {"ok": True, "stats": {"sample_features": {"status": "success", "rows": 1}}}

    class _Process:
        def __init__(self, *, stays_alive_after_kill: bool) -> None:
            self.alive = True
            self.stays_alive_after_kill = stays_alive_after_kill
            self.killed = False
            self.join_timeouts: list[float] = []
            self.exitcode = None

        def start(self) -> None:
            return None

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            self.killed = True
            if not self.stays_alive_after_kill:
                self.alive = False

        def join(self, timeout: float) -> None:
            self.join_timeouts.append(timeout)

    class _Context:
        def __init__(self) -> None:
            self.queues = [_EmptyQueue(), _StatsQueue()]
            self.processes: list[_Process] = []

        def Queue(self):
            return self.queues[len(self.processes)]

        def Process(self, **_kwargs) -> _Process:
            process = _Process(stays_alive_after_kill=not self.processes)
            self.processes.append(process)
            return process

    context = _Context()
    clock = iter((0.0, 0.0, 1.0, 1.0, 1.0))
    monkeypatch.setattr(service.multiprocessing, "get_context", lambda _method: context)
    monkeypatch.setattr(service.time, "monotonic", lambda: next(clock, 1.0))

    poller = _poller()
    with pytest.raises(service._PipelineTimeoutError, match="exceeded"):
        poller._execute_pipeline_isolated(_entry(), _START, _END, max_runtime_seconds=1.0)

    assert len(context.processes) == 1
    assert context.processes[0].killed is True

    with pytest.raises(service._TerminalPipelineError, match="unreaped child"):
        poller._execute_pipeline_isolated(_entry("successor"), _START, _END, max_runtime_seconds=1.0)

    assert len(context.processes) == 1

    context.processes[0].alive = False
    stats = poller._execute_pipeline_isolated(_entry("later_run"), _START, _END, max_runtime_seconds=1.0)

    assert stats["sample_features"]["rows"] == 1
    assert len(context.processes) == 2
    assert context.processes[0].join_timeouts[-1] == 0


@pytest.mark.unit
async def test_unreaped_child_fences_the_current_scheduler_run_until_reaped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StuckProcess:
        def __init__(self) -> None:
            self.alive = True
            self.join_timeouts: list[float] = []

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout: float) -> None:
            self.join_timeouts.append(timeout)

    _FrozenDateTime._frozen = datetime(2026, 7, 30, 16, 40, tzinfo=ZoneInfo("America/New_York"))
    monkeypatch.setattr(service, "datetime", _FrozenDateTime)
    monkeypatch.setattr(service, "PIPELINE_REGISTRY", [_entry("first"), _entry("second")])

    poller = GoldFeaturePoller()
    poller._settings = SimpleNamespace(
        gold_poller_lookback_days=1,
        gold_poller_disabled_pipeline_set=set(),
        gold_poller_run_budget_seconds=5400,
    )
    stuck = _StuckProcess()
    poller._unreaped_children = [("pipeline:stuck", stuck)]

    async def _must_not_discover(*_args, **_kwargs):
        raise AssertionError("fenced run must not spawn source discovery")

    async def _must_not_run(*_args, **_kwargs):
        raise AssertionError("fenced run must not start a successor pipeline")

    monkeypatch.setattr(poller, "_capture_expected_source_partitions_by_deadline", _must_not_discover)
    monkeypatch.setattr(poller, "_run_pipeline", _must_not_run)

    await poller._run_all_pipelines()

    results = poller._run_history[-1]["results"]
    assert {result["reason"] for result in results.values()} == {"unreaped_child_fence"}

    stuck.alive = False
    started: list[str] = []

    async def _discovered(*_args, **_kwargs):
        return {"first": {}, "second": {}}

    async def _ran(entry, *_args, **_kwargs):
        started.append(entry["name"])
        return {"status": "success", "rows": 1}

    monkeypatch.setattr(poller, "_capture_expected_source_partitions_by_deadline", _discovered)
    monkeypatch.setattr(poller, "_run_pipeline", _ran)

    await poller._run_all_pipelines()

    assert started == ["first", "second"]
    assert stuck.join_timeouts == [0]


@pytest.mark.unit
def test_stuck_child_cleanup_uses_only_remaining_deadline_for_each_join(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StuckProcess:
        def __init__(self) -> None:
            self.join_timeouts: list[float] = []
            self.terminate_called = False
            self.kill_called = False

        def is_alive(self) -> bool:
            return True

        def terminate(self) -> None:
            self.terminate_called = True

        def kill(self) -> None:
            self.kill_called = True

        def join(self, timeout: float) -> None:
            self.join_timeouts.append(timeout)

    process = _StuckProcess()
    clock = iter((0.0, 0.25))
    monkeypatch.setattr(service.time, "monotonic", lambda: next(clock, 1.0))

    assert service._stop_process_by_deadline(process, deadline=1.0) is False
    assert process.terminate_called is True
    assert process.kill_called is True
    assert process.join_timeouts == [1.0, 0.75]
