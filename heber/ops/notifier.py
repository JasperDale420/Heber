"""Discord notifier for critical data-quality alerts.

Severity-gated, with a debounce (N consecutive failing cycles before the first
alert) so single-cycle flaps — transient bind-mount read errors, deadline-edge
blips — never reach Discord, plus a per-(check, feed) cooldown for repeat
reminders on a sustained outage and a recovery note when a previously-alerting
feed returns to healthy. Network and IO errors are swallowed (logged) so a
broken webhook never crashes the monitor. State persists atomically under
``${data_root}/ops/alerts``; callers may select a service-specific filename.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import structlog
from prometheus_client import Counter

from heber.config import Settings
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops.metrics import _get_or_create

logger = structlog.get_logger(__name__)

# _get_or_create takes the Prometheus class first (see heber/ops/metrics.py and
# heber/health_monitor/metrics.py for the calling convention).
alerts_sent_total = _get_or_create(
    Counter,
    "heber_alerts_sent_total",
    "Critical data-quality alerts sent",
    ["check_name", "outcome"],
)


def _severity_meets(sev: Severity, minimum: Severity) -> bool:
    return sev == minimum or sev.is_more_severe_than(minimum)


class DiscordNotifier:
    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.Client | None = None,
        state_path: Path | None = None,
    ) -> None:
        self._enabled = settings.alert_discord_enabled
        self._webhook = settings.alert_discord_webhook_url
        self._min_severity = Severity(settings.alert_min_severity)
        self._cooldown = settings.alert_cooldown_seconds
        self._send_recovery = settings.alert_send_recovery
        self._debounce_cycles = settings.alert_debounce_cycles
        self._client = client
        self._state_path = state_path or (Path(settings.data_root) / "ops" / "alerts" / "state.json")
        self._state: dict[str, dict[str, Any]] = self._load_state()

    # ----- state -----
    def _load_state(self) -> dict[str, dict[str, Any]]:
        try:
            return json.loads(self._state_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_state(self) -> None:
        tmp_path: Path | None = None
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(
                prefix=f".{self._state_path.name}.",
                suffix=".tmp",
                dir=self._state_path.parent,
            )
            tmp_path = Path(tmp_name)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(self._state, default=str))
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(self._state_path)
            directory_fd = os.open(self._state_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            if tmp_path is not None:
                with contextlib.suppress(FileNotFoundError):
                    tmp_path.unlink()
            logger.warning("alert_state_save_failed", path=str(self._state_path), exc_info=True)

    # ----- dispatch -----
    def dispatch(self, results: list[CheckResult], now: datetime | None = None) -> None:
        if not self._enabled or not self._webhook:
            return
        now = now or datetime.now(UTC)
        changed = False
        for r in results:
            key = f"{r.check_name}|{r.feed or ''}"
            if r.status in (Status.FAIL, Status.ERROR) and _severity_meets(r.severity, self._min_severity):
                changed |= self._handle_fail(key, r, now)
            elif r.status == Status.PASS:
                changed |= self._handle_pass(key, r)
        if changed:
            self._save_state()

    def _handle_fail(self, key: str, r: CheckResult, now: datetime) -> bool:
        """Accrue a failure; alert once the debounce streak clears. Returns True if state changed."""
        prev = self._state.get(key)
        if prev and prev.get("last_status") == "fail":
            # Already alerting -> repeat reminder only after the cooldown elapses.
            try:
                last = datetime.fromisoformat(prev["last_sent_ts"])
            except (KeyError, ValueError):
                last = None
            if last is not None and (now - last).total_seconds() < self._cooldown:
                return False
            if self._post(f"🚨 CRITICAL — {r.message}", r.check_name):
                self._state[key] = {"last_sent_ts": now.isoformat(), "last_status": "fail"}
                return True
            return False
        # Not yet alerting -> count consecutive failing cycles before the first alert.
        streak = int((prev or {}).get("fail_streak", 0)) + 1
        if streak >= self._debounce_cycles and self._post(f"🚨 CRITICAL — {r.message}", r.check_name):
            self._state[key] = {"last_sent_ts": now.isoformat(), "last_status": "fail"}
        else:
            self._state[key] = {"fail_streak": streak, "last_status": "pending"}
        return True

    def _handle_pass(self, key: str, r: CheckResult) -> bool:
        """Clear state on recovery; only send a note if we had actually alerted. Returns True if state changed."""
        prev = self._state.get(key)
        if not prev:
            return False
        if prev.get("last_status") == "fail" and self._send_recovery:
            self._post(f"✅ RECOVERED — {r.message}", r.check_name)
        self._state.pop(key, None)  # also resets a sub-threshold (pending) streak silently
        return True

    def _post(self, content: str, check_name: str) -> bool:
        client = self._client or httpx.Client(timeout=10.0)
        try:
            resp = client.post(self._webhook, json={"content": content})
            resp.raise_for_status()
            alerts_sent_total.labels(check_name=check_name, outcome="sent").inc()
            logger.info("alert_sent", check_name=check_name, content=content)
            return True
        except (httpx.HTTPError, OSError, TypeError, ValueError):
            alerts_sent_total.labels(check_name=check_name, outcome="error").inc()
            logger.error("alert_send_failed", check_name=check_name, exc_info=True)
            return False
        finally:
            if self._client is None:
                client.close()

    def send_test(self, text: str) -> bool:
        """Send an ad-hoc test message (used by `heber alert-test`)."""
        if not self._webhook:
            logger.error("alert_test_no_webhook")
            return False
        return self._post(text, "alert_test")
