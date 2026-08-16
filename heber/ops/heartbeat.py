"""Off-machine dead-man heartbeat.

Every monitor Heber runs lives on the machine it monitors, so none of them can
report the machine being dead, the job being unloaded, or the process wedging.
A dead-man inverts that: the job pings an external check on each successful
cycle, and the *absence* of pings is what raises the alarm. It is the only
signal that survives the host.

``/fail`` is appended when the job knows it is broken, so a live-but-failing
process alerts immediately instead of waiting out the external grace period.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from datetime import datetime
from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

TIMEOUT_SECONDS = 10.0

# The beat is recorded as a variable on the private repo and read by a scheduled
# workflow on GitHub's runners — off-machine, using the `gh` login that already
# exists. Nothing is published: a private repo variable is visible only to
# accounts with access to the repo.
GITHUB_VARIABLE = "HEBER_ALERT_HEARTBEAT"
GITHUB_TIMEOUT_SECONDS = 30.0


def ping(url: str, *, ok: bool, client: httpx.Client | None = None) -> bool:
    """Ping the dead-man check. Returns whether the ping was delivered.

    Never raises: the alarm has to keep working when its own monitoring is down.
    An empty URL disables the heartbeat.
    """
    url = url.strip()
    if not url:
        return False
    target = url if ok else f"{url.rstrip('/')}/fail"
    owned = client is None
    client = client or httpx.Client(timeout=TIMEOUT_SECONDS)
    try:
        client.get(target)
        return True
    except (httpx.HTTPError, OSError):
        # Deliberately not an alert: a heartbeat that cannot be delivered is
        # indistinguishable to the external service from a job that has died,
        # which is precisely the outcome we want.
        logger.warning("heartbeat_ping_failed", ok=ok, exc_info=True)
        return False
    finally:
        if owned:
            client.close()


def ping_github(
    repo: str,
    *,
    ok: bool,
    now: datetime,
    runner: Callable[..., Any] = subprocess.run,
) -> bool:
    """Record the beat as a repo variable. Returns whether it was recorded.

    Written with the `gh` CLI so the existing keyring login is used and no token
    has to be copied into a config file. Never raises: a heartbeat that cannot
    be recorded looks, to the watcher, exactly like a job that has died — which
    is the correct outcome, not something to crash the alarm over.
    """
    repo = repo.strip()
    if not repo:
        return False
    body = f"{now.isoformat()} {'ok' if ok else 'fail'}"
    try:
        proc = runner(
            ["gh", "variable", "set", GITHUB_VARIABLE, "--repo", repo, "--body", body],
            capture_output=True,
            text=True,
            timeout=GITHUB_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        logger.warning("heartbeat_github_failed", repo=repo, exc_info=True)
        return False
    if proc.returncode != 0:
        logger.warning("heartbeat_github_rejected", repo=repo, stderr=(proc.stderr or "")[:200])
        return False
    return True
