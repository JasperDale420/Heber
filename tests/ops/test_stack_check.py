"""Host-side stack alarm: container / upstream / coverage / watchdog detectors.

These exercise the pure evaluation surface with injected docker, HTTP, log and
clock inputs, so the suite needs no Docker daemon, no network and no wall clock.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

import heber.ops.stack_check as stack_check_module
from heber.health_monitor.models import Severity, Status
from heber.ops.stack_check import (
    ContainerState,
    CoverageProbe,
    IngestProbe,
    evaluate,
    parse_inspect_output,
    parse_watchdog_actions,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 8, 12, 18, 0, 0, tzinfo=UTC)


def _running(name: str, *, restarts: int = 0, started_min_ago: int = 600) -> ContainerState:
    return ContainerState(
        name=name,
        status="running",
        restart_count=restarts,
        started_at=NOW - timedelta(minutes=started_min_ago),
    )


def _all_healthy(names: list[str]) -> list[ContainerState]:
    return [_running(n) for n in names]


def _eval(**overrides):
    names = overrides.pop("names", ["heber-consumer", "data-gateway-redis"])
    kwargs = {
        "daemon_up": True,
        "expected": names,
        "containers": _all_healthy(names),
        "coverage": CoverageProbe(status="ok", detail="age=120s"),
        "watchdog_actions": [],
        "volume_mounted": True,
        "now": NOW,
    }
    kwargs.update(overrides)
    return evaluate(**kwargs)


def _by_feed(results, check_name):
    return {r.feed: r for r in results if r.check_name == check_name}


# --- docker daemon -----------------------------------------------------------


def test_daemon_down_reports_exactly_one_container_level_critical():
    """One root cause must not fan out into a per-container alert storm."""
    results = _eval(daemon_up=False, containers=[])

    daemon = [r for r in results if r.check_name == "stack_docker_daemon"]
    assert len(daemon) == 1
    assert daemon[0].status is Status.FAIL
    assert daemon[0].severity is Severity.P0_CRITICAL
    assert not _by_feed(results, "stack_container")
    assert not _by_feed(results, "stack_container_flapping")


def test_daemon_down_skips_the_coverage_probe():
    results = _eval(daemon_up=False, containers=[], coverage=None)
    assert not _by_feed(results, "stack_coverage")


def test_daemon_up_emits_a_pass_so_recovery_can_fire():
    """Without a PASS the notifier never clears state or sends RECOVERED."""
    results = _eval()
    daemon = [r for r in results if r.check_name == "stack_docker_daemon"]
    assert len(daemon) == 1
    assert daemon[0].status is Status.PASS


# --- container state ---------------------------------------------------------


@pytest.mark.parametrize("status", ["exited", "dead", "created"])
def test_stopped_container_is_critical(status):
    names = ["heber-consumer"]
    results = _eval(
        names=names,
        containers=[ContainerState(name="heber-consumer", status=status, restart_count=0, started_at=NOW)],
    )
    res = _by_feed(results, "stack_container")["heber-consumer"]
    assert res.status is Status.FAIL
    assert res.severity is Severity.P0_CRITICAL
    assert "heber-consumer" in res.message


def test_paused_container_is_critical_and_named_as_paused():
    """Paused containers have been observed still reporting 'Up' while doing nothing."""
    names = ["heber-watch"]
    results = _eval(
        names=names,
        containers=[ContainerState(name="heber-watch", status="paused", restart_count=0, started_at=NOW)],
    )
    res = _by_feed(results, "stack_container")["heber-watch"]
    assert res.status is Status.FAIL
    assert "paused" in res.message.lower()


def test_missing_container_is_critical():
    names = ["heber-consumer"]
    results = _eval(names=names, containers=[])
    res = _by_feed(results, "stack_container")["heber-consumer"]
    assert res.status is Status.FAIL
    assert "missing" in res.message.lower()


def test_upstream_down_is_critical():
    """The 2026-08-12 outage: data-gateway-redis stayed exited, heber-* crash-looped."""
    names = ["data-gateway-redis"]
    results = _eval(
        names=names,
        containers=[ContainerState(name="data-gateway-redis", status="exited", restart_count=0, started_at=NOW)],
    )
    res = _by_feed(results, "stack_container")["data-gateway-redis"]
    assert res.status is Status.FAIL
    assert res.severity is Severity.P0_CRITICAL


def test_all_running_emits_pass_per_container():
    names = ["heber-consumer", "heber-watch"]
    results = _eval(names=names)
    per = _by_feed(results, "stack_container")
    assert set(per) == set(names)
    assert all(r.status is Status.PASS for r in per.values())


# --- restart looping ---------------------------------------------------------


def test_restarting_status_is_flapping():
    """`restarting` is Docker's direct crash-loop signal, independent of counters."""
    names = ["heber-consumer"]
    results = _eval(
        names=names,
        containers=[ContainerState(name="heber-consumer", status="restarting", restart_count=105, started_at=NOW)],
    )
    res = _by_feed(results, "stack_container_flapping")["heber-consumer"]
    assert res.status is Status.FAIL
    assert res.severity is Severity.P0_CRITICAL


def test_repeated_restarts_with_a_recent_start_is_flapping():
    names = ["heber-watch"]
    results = _eval(names=names, containers=[_running("heber-watch", restarts=7, started_min_ago=2)])
    res = _by_feed(results, "stack_container_flapping")["heber-watch"]
    assert res.status is Status.FAIL


def test_a_single_restart_is_not_flapping():
    """One OOM-kill that recovered is not a crash-loop."""
    names = ["heber-watch"]
    results = _eval(names=names, containers=[_running("heber-watch", restarts=1, started_min_ago=2)])
    assert _by_feed(results, "stack_container_flapping")["heber-watch"].status is Status.PASS


def test_many_restarts_but_stable_for_hours_is_not_flapping():
    names = ["heber-watch"]
    results = _eval(names=names, containers=[_running("heber-watch", restarts=105, started_min_ago=180)])
    assert _by_feed(results, "stack_container_flapping")["heber-watch"].status is Status.PASS


# --- catalog coverage --------------------------------------------------------


def test_coverage_stale_is_critical():
    results = _eval(coverage=CoverageProbe(status="stale", detail="age=90000s"))
    res = _by_feed(results, "stack_coverage")["coverage"]
    assert res.status is Status.FAIL
    assert res.severity is Severity.P0_CRITICAL


@pytest.mark.parametrize("detail", ["connection refused", "timed out", "HTTP 503"])
def test_coverage_unreachable_is_critical_not_swallowed(detail):
    results = _eval(coverage=CoverageProbe(status=None, detail=detail))
    res = _by_feed(results, "stack_coverage")["coverage"]
    assert res.status is Status.FAIL


def test_coverage_ok_passes():
    assert _by_feed(_eval(), "stack_coverage")["coverage"].status is Status.PASS


def test_missing_volume_suppresses_the_coverage_alert():
    """Volume gone is one root cause; don't also page about the coverage it feeds."""
    results = _eval(volume_mounted=False, coverage=CoverageProbe(status=None, detail="connection refused"))
    assert not _by_feed(results, "stack_coverage")
    assert _by_feed(results, "stack_volume")["volume"].status is Status.FAIL


def test_mounted_volume_passes():
    assert _by_feed(_eval(), "stack_volume")["volume"].status is Status.PASS


# --- watchdog reporting ------------------------------------------------------


def test_watchdog_actions_are_reported_as_a_warning():
    results = _eval(watchdog_actions=["unpausing service(s): heber-watch"])
    res = _by_feed(results, "stack_watchdog")["watchdog"]
    assert res.status is Status.FAIL, "must be FAIL — the notifier ignores Status.WARN entirely"
    assert res.severity is Severity.P1_WARNING
    assert "heber-watch" in res.message


def test_no_watchdog_action_passes():
    assert _by_feed(_eval(), "stack_watchdog")["watchdog"].status is Status.PASS


# --- watchdog log parsing ----------------------------------------------------


def test_parses_every_watchdog_action_line():
    log = "\n".join(
        [
            "2026-08-12T17:58:00Z reconciling down service(s): heber-consumer",
            "2026-08-12T17:58:01Z restarting unhealthy service(s): heber-watch",
            "2026-08-12T17:58:02Z unpausing service(s): heber-compactor",
            "2026-08-12T17:58:03Z starting upstream service(s): data-gateway-redis",
        ]
    )
    actions = parse_watchdog_actions(log, now=NOW, lookback=timedelta(minutes=10))
    assert len(actions) == 4


def test_skip_lines_are_not_watchdog_actions():
    """The watchdog logs a timestamped 'skipping' line every tick while Docker boots."""
    log = "2026-08-12T17:59:00Z docker daemon unavailable — skipping"
    assert parse_watchdog_actions(log, now=NOW, lookback=timedelta(minutes=10)) == []


def test_old_watchdog_lines_are_ignored():
    log = "2026-08-11T10:00:00Z reconciling down service(s): heber-consumer"
    assert parse_watchdog_actions(log, now=NOW, lookback=timedelta(minutes=10)) == []


def test_unparsable_watchdog_lines_are_skipped_not_fatal():
    log = "garbage without a timestamp\n\n2026-08-12T17:58:00Z unpausing service(s): heber-watch"
    assert len(parse_watchdog_actions(log, now=NOW, lookback=timedelta(minutes=10))) == 1


# --- docker inspect parsing --------------------------------------------------


def test_parses_tab_separated_inspect_lines():
    out = "/heber-consumer\trunning\t0\t2026-08-12T10:00:00.123456789Z\n"
    states = parse_inspect_output(out)
    assert states[0].name == "heber-consumer", "the leading slash must be stripped"
    assert states[0].status == "running"
    assert states[0].restart_count == 0
    assert states[0].started_at == datetime(2026, 8, 12, 10, 0, 0, 123456, tzinfo=UTC)


def test_parses_multiple_containers_from_one_batched_call():
    out = "/heber-consumer\trunning\t0\t2026-08-12T10:00:00Z\n/heber-watch\tpaused\t3\t2026-08-12T09:00:00Z\n"
    assert [s.name for s in parse_inspect_output(out)] == ["heber-consumer", "heber-watch"]


def test_inspect_parser_tolerates_docker_zero_time():
    """Docker reports a never-started container as the zero time."""
    out = "/heber-consumer\tcreated\t0\t0001-01-01T00:00:00Z\n"
    assert parse_inspect_output(out)[0].started_at is None


def test_inspect_parser_skips_malformed_lines():
    out = "oops\n/heber-watch\trunning\t0\t2026-08-12T10:00:00Z\n"
    assert [s.name for s in parse_inspect_output(out)] == ["heber-watch"]


# --- volume-scoped suppression -----------------------------------------------

_HEBER = ["heber-postgres", "heber-consumer", "heber-watch"]
_UPSTREAM = ["data-gateway", "data-gateway-redis"]


def _eval_volume_gone(**overrides):
    names = _HEBER + _UPSTREAM
    containers = overrides.pop("containers", None)
    return _eval(
        names=names,
        containers=containers if containers is not None else _all_healthy(names),
        volume_mounted=False,
        **overrides,
    )


def test_missing_volume_suppresses_downstream_container_noise():
    """One root cause, one page.

    After the 2026-08-12 boot the exFAT volume was under repair for 76 minutes.
    Every heber service bind-mounts it, so all of them were down as a direct
    consequence. Paging per container turns one incident into a fan-out.
    """
    containers = [ContainerState(n, "exited", 0, None) for n in _HEBER] + _all_healthy(_UPSTREAM)
    results = _eval_volume_gone(containers=containers)

    heber_alerts = [r for r in results if r.check_name == "stack_container" and r.feed in _HEBER]
    assert not heber_alerts, "volume-caused container failures must not each page"


def test_missing_volume_still_reports_upstream_containers():
    """data-gateway and data-gateway-redis do not use the volume.

    Redis is what buffers live events while Heber is down (heber:events is capped
    at 500K and evicts unread entries), so its state stays actionable.
    """
    containers = [ContainerState(n, "exited", 0, None) for n in _HEBER + _UPSTREAM]
    results = _eval_volume_gone(containers=containers)

    upstream = _by_feed(results, "stack_container")
    assert upstream["data-gateway-redis"].status is Status.FAIL
    assert upstream["data-gateway"].status is Status.FAIL


def test_missing_volume_still_reports_a_paused_heber_container():
    """A pause has nothing to do with the volume — it is an independent incident."""
    containers = [
        ContainerState("heber-watch", "paused", 0, None),
        *[ContainerState(n, "exited", 0, None) for n in ("heber-postgres", "heber-consumer")],
        *_all_healthy(_UPSTREAM),
    ]
    results = _eval_volume_gone(containers=containers)

    watch = _by_feed(results, "stack_container").get("heber-watch")
    assert watch is not None, "a paused container must stay visible during a volume outage"
    assert watch.status is Status.FAIL
    assert "PAUSED" in watch.message


def test_heber_postgres_is_treated_as_volume_dependent():
    """It bind-mounts ${HEBER_VOLUME_ROOT}/postgres/data, despite not matching heber-* by role."""
    from heber.ops.stack_check import VOLUME_DEPENDENT_CONTAINERS

    assert "heber-postgres" in VOLUME_DEPENDENT_CONTAINERS


def test_unknown_containers_default_to_reported_not_suppressed():
    """Fail open: a container nobody classified must page rather than vanish."""
    containers = [ContainerState("some-new-service", "exited", 0, None)]
    results = evaluate(
        daemon_up=True,
        expected=["some-new-service"],
        containers=containers,
        coverage=None,
        watchdog_actions=[],
        volume_mounted=False,
        ingest=None,
        now=NOW,
    )
    assert _by_feed(results, "stack_container")["some-new-service"].status is Status.FAIL


# --- ingest progress ---------------------------------------------------------


def _ingest(age_seconds: float | None, detail: str = "") -> IngestProbe:
    last = None if age_seconds is None else NOW - timedelta(seconds=age_seconds)
    return IngestProbe(last_success=last, detail=detail)


def test_recent_ingest_passes():
    results = _eval(ingest=_ingest(30))
    assert _by_feed(results, "stack_ingest")["ingest"].status is Status.PASS


def test_ingest_within_an_aof_reload_is_not_an_outage():
    """data-gateway-redis takes ~77s to reload; the consumer now waits that out."""
    results = _eval(ingest=_ingest(90))
    assert _by_feed(results, "stack_ingest")["ingest"].status is Status.PASS


def test_stalled_ingest_is_critical():
    """The gap the container healthcheck cannot see: alive, looping, consuming nothing."""
    results = _eval(ingest=_ingest(600))
    res = _by_feed(results, "stack_ingest")["ingest"]
    assert res.status is Status.FAIL
    assert res.severity is Severity.P0_CRITICAL


def test_unreadable_ingest_metric_is_critical_not_swallowed():
    results = _eval(ingest=_ingest(None, detail="ConnectError: connection refused"))
    assert _by_feed(results, "stack_ingest")["ingest"].status is Status.FAIL


def test_missing_volume_suppresses_the_ingest_alert():
    """With the volume gone the consumer cannot write; that is the volume outage."""
    results = _eval(volume_mounted=False, ingest=_ingest(9999))
    assert not _by_feed(results, "stack_ingest")


def test_ingest_probe_parses_the_gauge(monkeypatch):
    body = (
        "# HELP heber_consumer_last_xread_success_unixtime doc\n"
        "# TYPE heber_consumer_last_xread_success_unixtime gauge\n"
        "heber_consumer_loop_heartbeat_unixtime 1.7e+09\n"
        "heber_consumer_last_xread_success_unixtime 1786738000.0\n"
    )
    monkeypatch.setattr(
        stack_check_module.httpx,
        "get",
        lambda *_a, **_kw: SimpleNamespace(status_code=200, text=body),
    )

    probe = stack_check_module.probe_ingest()

    assert probe.last_success == datetime.fromtimestamp(1786738000.0, tz=UTC)


def test_ingest_probe_treats_an_untouched_gauge_as_no_read_yet(monkeypatch):
    """A just-started consumer sits at 0.0; reporting that as 1970 is a false outage."""
    monkeypatch.setattr(
        stack_check_module.httpx,
        "get",
        lambda *_a, **_kw: SimpleNamespace(status_code=200, text="heber_consumer_last_xread_success_unixtime 0.0\n"),
    )

    probe = stack_check_module.probe_ingest()

    assert probe.last_success is None
    assert "no successful read yet" in probe.detail


def test_ingest_probe_survives_an_unreachable_metrics_port(monkeypatch):
    def _boom(*_a, **_kw):
        raise stack_check_module.httpx.ConnectError("connection refused")

    monkeypatch.setattr(stack_check_module.httpx, "get", _boom)

    probe = stack_check_module.probe_ingest()

    assert probe.last_success is None
    assert "ConnectError" in probe.detail
