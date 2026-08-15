"""Host-side stack alarm — container, upstream, coverage and watchdog detectors.

Every other Heber failure detector runs as a container in the stack it watches,
so a stack-wide outage takes the alarm down with it. On 2026-08-12 a Docker
Desktop restart left ``data-gateway`` and ``data-gateway-redis`` stopped while
the heber-* services crash-looped against the dead Redis for 1h40m, ingesting
nothing, and nothing reached the user.

This module runs from the native launchd ``alert-check`` job, outside Docker,
and talks to the daemon from the host. It reports; ``heber_docker_watchdog.sh``
repairs. When the watchdog has acted, that is surfaced too, so a stack that is
being silently propped up every two minutes is visible rather than invisible.

``evaluate`` is pure — docker, HTTP, log text and the clock are all injected —
so the detectors are testable without a daemon, a network or a wall clock.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import structlog

from heber.config import Settings
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops.dataflow_health import _MOUNT_SENTINEL_NAME, _PROM_METRIC_RE

logger = structlog.get_logger(__name__)

# Container names from docker-compose.yml, plus Heber's upstream. The upstream
# lives in the Data-Gateway compose project and carries `restart: unless-stopped`,
# which does not bring it back when the daemon restarts — the exact hole that
# produced the 2026-08-12 outage. Kept in step with SERVICES/UPSTREAM in
# scripts/heber_docker_watchdog.sh.
EXPECTED_CONTAINERS: tuple[str, ...] = (
    "heber-postgres",
    "heber-catalog",
    "heber-consumer",
    "heber-backfill-consumer",
    "heber-compactor",
    "heber-watch",
    "heber-gold-poller",
    "heber-dataflow-health",
    "heber-health-monitor",
    "data-gateway",
    "data-gateway-redis",
)

# Containers that bind-mount the lakehouse volume and therefore cannot start
# while it is absent. When the volume goes, all of these fail as a direct
# consequence — reporting each one turns one incident into a fan-out, so their
# stopped/missing noise is folded into the single stack_volume alert.
#
# heber-postgres is listed explicitly: it mounts ${HEBER_VOLUME_ROOT}/postgres/data,
# so a name-prefix rule that reasoned about "the heber app services" would miss
# the one whose failure cascades furthest via depends_on: service_healthy.
#
# Membership is opt-in, so a container nobody classified is still reported. A
# forgotten alert is recoverable; a silently suppressed one is not.
VOLUME_DEPENDENT_CONTAINERS: frozenset[str] = frozenset(
    {
        "heber-postgres",
        "heber-catalog",
        "heber-consumer",
        "heber-backfill-consumer",
        "heber-compactor",
        "heber-watch",
        "heber-gold-poller",
        "heber-dataflow-health",
        "heber-health-monitor",
    }
)

# A container that has restarted this many times AND started very recently is
# looping rather than recovering. `restarting` status is the direct signal; this
# catches the window between two restarts, when status reads `running`.
FLAP_RESTART_THRESHOLD = 3
FLAP_RECENT_START = timedelta(minutes=10)

# Docker never wedges for long on a healthy host, but a daemon that is mid-restart
# can accept a connection and never answer. launchd will not start the next
# StartInterval run while this one is alive, so an unbounded call would silence
# the alarm permanently rather than for one cycle.
DOCKER_TIMEOUT_SECONDS = 15.0
COVERAGE_TIMEOUT_SECONDS = 10.0
INGEST_TIMEOUT_SECONDS = 10.0

# Every service that consumes the event stream, with the loopback port its
# Prometheus endpoint is published on (docker-compose.yml) and the gauge holding
# its last successful XREADGROUP. Both must be covered: they retry an
# unavailable Redis rather than crash-looping now, so neither announces a stall
# by dying any more, and both were down together in the 08-08 and 08-11 episodes.
INGEST_SOURCES: tuple[tuple[str, str, str], ...] = (
    ("heber-consumer", "http://127.0.0.1:9090/", "heber_consumer_last_xread_success_unixtime"),
    ("heber-watch", "http://127.0.0.1:9091/", "heber_watch_last_xread_success_unixtime"),
)

# How long a consumer may go without a successful XREADGROUP round-trip before
# this is an outage rather than a blip.
#
# Floor: data-gateway-redis takes ~77s to reload its AOF and consumers now wait
# that out. Ceiling pressure: the gauge advances when a read RETURNS, so the gap
# also spans processing of the batch it returned — and a large batch fanned
# across many partitions on the exFAT mount has historically outrun the 180s
# healthcheck window. 10 minutes clears a worst-case drain without alerting,
# while still catching a real stall within two alert-check cycles.
INGEST_STALE_SECONDS = 600.0

# Every line heber_docker_watchdog.sh emits when it actually repairs something.
# Its "docker daemon unavailable — skipping" line is timestamped too but is not
# an action, so it is matched by exclusion rather than by a catch-all.
_WATCHDOG_ACTIONS = (
    "reconciling down service(s)",
    "restarting unhealthy service(s)",
    "unpausing service(s)",
    "starting upstream service(s)",
)
_WATCHDOG_LINE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(?P<body>.+)$")

# Docker reports a container that never started as the zero time.
_DOCKER_ZERO_TIME = "0001-01-01T00:00:00Z"


@dataclass(frozen=True)
class ContainerState:
    name: str
    status: str
    restart_count: int
    started_at: datetime | None


@dataclass(frozen=True)
class CoverageProbe:
    """Result of GET /health/coverage. ``status`` is None when unreachable."""

    status: str | None
    detail: str


@dataclass(frozen=True)
class IngestProbe:
    """One consumer's last successful XREADGROUP round-trip.

    ``last_success`` is None when the value could not be read. ``degraded`` marks
    the benign reasons for that — a container built before the gauge existed, or
    one that has not completed its first read yet — which are a warning to act on
    at leisure, not a page.
    """

    service: str
    last_success: datetime | None
    detail: str
    degraded: bool = False


def _result(
    check_name: str,
    feed: str,
    *,
    ok: bool,
    message: str,
    severity: Severity = Severity.P0_CRITICAL,
    now: datetime,
    **details: object,
) -> CheckResult:
    return CheckResult(
        check_name=check_name,
        feed=feed,
        severity=severity,
        # Status.WARN is not dispatched by DiscordNotifier at all — it only acts
        # on FAIL/ERROR and PASS. Severity, not status, carries the "this is a
        # warning" signal.
        status=Status.PASS if ok else Status.FAIL,
        message=message,
        details=dict(details),
        ts_checked=now,
    )


def _parse_docker_time(raw: str) -> datetime | None:
    """Docker emits RFC3339 with nanoseconds; fromisoformat only takes 3 or 6 digits."""
    raw = raw.strip()
    if not raw or raw == _DOCKER_ZERO_TIME:
        return None
    normalized = raw.replace("Z", "+00:00")
    if match := re.search(r"\.(\d+)", normalized):
        normalized = normalized.replace(f".{match.group(1)}", f".{match.group(1)[:6]:0<6}")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def parse_inspect_output(stdout: str) -> list[ContainerState]:
    """Parse the tab-separated `docker inspect --format` lines, skipping junk."""
    states: list[ContainerState] = []
    for line in stdout.splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        name, status, restarts, started = parts
        try:
            restart_count = int(restarts)
        except ValueError:
            continue
        states.append(
            ContainerState(
                name=name.lstrip("/"),
                status=status.strip(),
                restart_count=restart_count,
                started_at=_parse_docker_time(started),
            )
        )
    return states


def parse_watchdog_actions(log_text: str, *, now: datetime, lookback: timedelta) -> list[str]:
    """Return watchdog repair lines newer than ``now - lookback``."""
    cutoff = now - lookback
    actions: list[str] = []
    for line in log_text.splitlines():
        match = _WATCHDOG_LINE.match(line.strip())
        if not match:
            continue
        body = match.group("body")
        if not any(action in body for action in _WATCHDOG_ACTIONS):
            continue
        ts = _parse_docker_time(match.group("ts"))
        if ts is not None and ts >= cutoff:
            actions.append(body)
    return actions


def evaluate(
    *,
    daemon_up: bool,
    expected: Sequence[str],
    containers: Sequence[ContainerState],
    coverage: CoverageProbe | None,
    watchdog_actions: Sequence[str],
    volume_mounted: bool,
    now: datetime,
    ingest: Sequence[IngestProbe] | None = None,
) -> list[CheckResult]:
    """Turn collected stack facts into check results. Pure."""
    results: list[CheckResult] = [
        _result(
            "stack_volume",
            "volume",
            ok=volume_mounted,
            message=(
                "Lakehouse volume is mounted"
                if volume_mounted
                else "Ingest unavailable: lakehouse volume is NOT mounted — every heber service "
                "bind-mounts it, so their failures below are suppressed as consequences of this"
            ),
            now=now,
        )
    ]

    results.append(
        _result(
            "stack_docker_daemon",
            "docker",
            ok=daemon_up,
            message="Docker daemon reachable" if daemon_up else "Docker daemon is unreachable from the host",
            now=now,
        )
    )

    # One dead daemon would otherwise fan out into a per-container alert for
    # every expected name, plus a coverage alert. Report the root cause only.
    if daemon_up:
        results.extend(_container_results(expected, containers, now=now, volume_mounted=volume_mounted))
        # A missing volume already explains a failing catalog; don't page twice.
        if coverage is not None and volume_mounted:
            results.append(_coverage_result(coverage, now=now))
        # Same reasoning: with the volume gone the consumer cannot write, so a
        # stalled ingest is that outage, not a second one. A consumer that is not
        # running is likewise already reported by stack_container — its ingest
        # being stopped is the same fact stated twice.
        if ingest is not None and volume_mounted:
            running = {c.name for c in containers if c.status == "running"}
            results.extend(_ingest_result(probe, now=now) for probe in ingest if probe.service in running)

    results.append(
        _result(
            "stack_watchdog",
            "watchdog",
            ok=not watchdog_actions,
            message=(
                "Docker watchdog has not intervened"
                if not watchdog_actions
                else "Docker watchdog intervened: " + "; ".join(watchdog_actions)
            ),
            severity=Severity.P1_WARNING,
            now=now,
            actions=list(watchdog_actions),
        )
    )
    return results


def _container_results(
    expected: Sequence[str],
    containers: Sequence[ContainerState],
    *,
    now: datetime,
    volume_mounted: bool = True,
) -> list[CheckResult]:
    by_name = {c.name: c for c in containers}
    results: list[CheckResult] = []
    for name in expected:
        state = by_name.get(name)
        status = state.status if state else "missing"
        running = status == "running"
        paused = status == "paused"

        # With the volume gone, a volume-dependent container being down is the
        # volume alert restated. A PAUSE is not — nothing about an absent mount
        # pauses a container, so that stays visible as its own incident.
        if not volume_mounted and name in VOLUME_DEPENDENT_CONTAINERS and not paused:
            continue

        if paused:
            message = f"Container {name} is PAUSED (reports 'Up' but processes nothing)"
        elif running:
            message = f"Container {name} is running"
        else:
            message = f"Container {name} is {status} (expected running)"
        results.append(
            _result(
                "stack_container",
                name,
                ok=running,
                message=message,
                now=now,
                container_status=status,
            )
        )
        results.append(_flapping_result(name, state, now=now))
    return results


def _ingest_result(ingest: IngestProbe, *, now: datetime) -> CheckResult:
    """Page when a consumer stops making Redis round-trips.

    The container healthcheck reads the run-loop heartbeat, which the loop
    refreshes on every iteration including ones that caught a Redis error and
    slept — so a consumer spinning against a dead upstream reports healthy
    indefinitely. Now that startup retries instead of crash-looping, this gauge
    is the only thing that distinguishes waiting-for-Redis from consuming.
    """
    if ingest.last_success is None:
        # A stale image or a consumer still on its first read is a deploy
        # nuisance, not an ingest outage. Paging P0 for it would train exactly
        # the alert-fatigue this whole change exists to remove.
        return _result(
            "stack_ingest",
            ingest.service,
            ok=False,
            message=f"{ingest.service} ingest progress unreadable: {ingest.detail}",
            severity=Severity.P1_WARNING if ingest.degraded else Severity.P0_CRITICAL,
            now=now,
        )
    age = (now - ingest.last_success).total_seconds()
    ok = age <= INGEST_STALE_SECONDS
    return _result(
        "stack_ingest",
        ingest.service,
        ok=ok,
        message=(
            f"{ingest.service} is reading the stream (last success {age:.0f}s ago)"
            if ok
            else f"{ingest.service} has not read the stream in {age:.0f}s "
            f"(limit {INGEST_STALE_SECONDS:.0f}s) — it may be alive but stalled on Redis"
        ),
        now=now,
        age_seconds=round(age, 1),
    )


def _flapping_result(name: str, state: ContainerState | None, *, now: datetime) -> CheckResult:
    """`restarting` is the live crash-loop signal; the counter catches the gap between restarts.

    A high restart count alone means nothing — `docker compose up` recreates the
    container and resets it to zero, and a container that restarted often but has
    been stable for hours has recovered. Only a recent start makes it a loop.
    """
    if state is None:
        return _result("stack_container_flapping", name, ok=True, message=f"{name}: not inspected", now=now)

    recently_started = state.started_at is not None and (now - state.started_at) < FLAP_RECENT_START
    looping = state.status == "restarting" or (state.restart_count >= FLAP_RESTART_THRESHOLD and recently_started)
    return _result(
        "stack_container_flapping",
        name,
        ok=not looping,
        message=(
            f"Container {name} is restart-looping ({state.restart_count} restarts, status={state.status})"
            if looping
            else f"Container {name} is not restart-looping"
        ),
        now=now,
        restart_count=state.restart_count,
        container_status=state.status,
    )


def _coverage_result(coverage: CoverageProbe, *, now: datetime) -> CheckResult:
    ok = coverage.status == "ok"
    return _result(
        "stack_coverage",
        "coverage",
        ok=ok,
        message=(
            f"Catalog coverage is fresh ({coverage.detail})"
            if ok
            else f"Catalog coverage unhealthy: {coverage.status or 'unreachable'} ({coverage.detail})"
        ),
        now=now,
        coverage_status=coverage.status,
    )


# ----- collectors (impure) -----


def probe_docker(names: Sequence[str]) -> tuple[bool, list[ContainerState]]:
    """Inspect every expected container in one call.

    Only the four fields the detectors need are requested. `--format '{{json .}}'`
    would pull in `.Config.Env`, which carries gateway API keys and the Postgres
    password, into a process whose stdout is a launchd log file.
    """
    try:
        proc = subprocess.run(  # noqa: S603
            [
                "docker",
                "inspect",
                "--format",
                "{{.Name}}\t{{.State.Status}}\t{{.RestartCount}}\t{{.State.StartedAt}}",
                *names,
            ],
            capture_output=True,
            text=True,
            timeout=DOCKER_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        logger.warning("stack_check_docker_inspect_failed", exc_info=True)
        return False, []

    states = parse_inspect_output(proc.stdout)
    # A missing container makes `docker inspect` exit non-zero while still
    # printing the ones it found, so a non-zero exit is only a dead daemon when
    # nothing at all came back.
    return bool(states), states


def probe_ingest(service: str, metrics_url: str, metric_name: str) -> IngestProbe:
    """Scrape one consumer's last successful XREADGROUP timestamp.

    Read from the host rather than inside the stack, for the same reason the rest
    of this module is: a check that runs in the container it watches goes down
    with it. The ports are published to loopback in docker-compose.yml.
    """
    try:
        resp = httpx.get(metrics_url, timeout=INGEST_TIMEOUT_SECONDS)
    except (httpx.HTTPError, OSError) as exc:
        return IngestProbe(service, last_success=None, detail=f"{type(exc).__name__}: {exc}"[:200])
    if resp.status_code != 200:
        return IngestProbe(service, last_success=None, detail=f"HTTP {resp.status_code}")

    for line in resp.text.splitlines():
        if not line.startswith(metric_name):
            continue
        match = _PROM_METRIC_RE.match(line.strip())
        if match is None or match.group("name") != metric_name:
            continue
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        # 0 is the gauge's untouched default: the process is up but has not
        # completed a read yet. Reporting that as 1970 would be a false outage.
        if value <= 0:
            return IngestProbe(service, last_success=None, detail="no successful read yet", degraded=True)
        return IngestProbe(service, last_success=datetime.fromtimestamp(value, tz=UTC), detail="")

    # Also the shape of a stale deploy: the gauge only exists in images built
    # after it was added, so say so rather than let it read as a dead consumer.
    return IngestProbe(
        service,
        last_success=None,
        detail=f"{metric_name} absent from metrics output (image predates this metric? rebuild {service})",
        degraded=True,
    )


def probe_all_ingest() -> list[IngestProbe]:
    """Scrape every stream consumer listed in INGEST_SOURCES."""
    return [probe_ingest(service, url, metric) for service, url, metric in INGEST_SOURCES]


def probe_coverage(catalog_url: str) -> CoverageProbe:
    """GET /health/coverage, derived from the catalog origin.

    The configured URL already points at `/health`, so its path is discarded
    rather than appended to — otherwise this requests `/health/health/coverage`.
    """
    parts = urlsplit(catalog_url)
    url = f"{parts.scheme}://{parts.netloc}/health/coverage"
    try:
        resp = httpx.get(url, timeout=COVERAGE_TIMEOUT_SECONDS)
    except (httpx.HTTPError, OSError) as exc:
        return CoverageProbe(status=None, detail=f"{type(exc).__name__}: {exc}"[:200])
    if resp.status_code != 200:
        return CoverageProbe(status=None, detail=f"HTTP {resp.status_code}")
    try:
        body = resp.json()
    except ValueError:
        return CoverageProbe(status=None, detail="non-JSON response")
    age = body.get("coverage_age_seconds")
    return CoverageProbe(status=str(body.get("status")), detail=f"age={age}s")


def read_watchdog_actions(log_path: Path, *, now: datetime, lookback: timedelta) -> list[str]:
    try:
        # The log is append-only and small; only the tail can be within lookback.
        text = "\n".join(log_path.read_text(errors="replace").splitlines()[-200:])
    except OSError:
        return []
    return parse_watchdog_actions(text, now=now, lookback=lookback)


def _volume_is_mounted(settings: Settings) -> bool:
    """Test the sentinel file, not the directory.

    A broken mount can leave the mount POINT behind as an empty placeholder
    directory, so ``is_dir()`` answers yes for a volume that is gone — which is
    the failure this check exists to catch. The sentinel lives on the real
    volume and disappears with it. ``stat`` itself raises EPERM on a zombie
    mount, so that is treated as absent rather than allowed to propagate.
    """
    try:
        return (Path(settings.data_root) / _MOUNT_SENTINEL_NAME).exists()
    except OSError:
        return False


def run_stack_checks(settings: Settings, *, now: datetime | None = None) -> list[CheckResult]:
    """Collect stack facts and evaluate them. Never raises."""
    now = now or datetime.now(UTC)
    daemon_up, containers = probe_docker(EXPECTED_CONTAINERS)
    volume_mounted = _volume_is_mounted(settings)
    coverage = probe_coverage(settings.health_catalog_url) if daemon_up and volume_mounted else None
    ingest = probe_all_ingest() if daemon_up and volume_mounted else None
    watchdog_log = Path(__file__).resolve().parents[2] / "logs" / "native" / "docker-watchdog.out.log"
    return evaluate(
        daemon_up=daemon_up,
        expected=EXPECTED_CONTAINERS,
        containers=containers,
        coverage=coverage,
        ingest=ingest,
        watchdog_actions=read_watchdog_actions(
            watchdog_log,
            now=now,
            # Two alert-check intervals, so an action cannot fall between cycles.
            lookback=timedelta(seconds=2 * settings.alert_liveness_check_interval_seconds),
        ),
        volume_mounted=volume_mounted,
        now=now,
    )
