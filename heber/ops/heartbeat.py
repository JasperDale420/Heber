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

import httpx
import structlog

logger = structlog.get_logger(__name__)

TIMEOUT_SECONDS = 10.0


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
