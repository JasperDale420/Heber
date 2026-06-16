"""Tier 1 — Stream Health Check.

Monitors Redis stream reachability, consumer group status, consumer lag
(pending messages), and DLQ depth.  Runs every ~30 s.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import structlog

from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Thresholds
PENDING_WARN = 1_000
PENDING_CRITICAL = 10_000
DLQ_WARN = 1


def _now_et() -> datetime:
    """Return the current time in US/Eastern.  Patchable in tests."""
    return datetime.now(tz=ET)


async def run_stream_health_checks(ctx: CheckContext) -> list[CheckResult]:
    """Run all Tier 1 stream health checks and return results."""
    now = _now_et()
    results: list[CheckResult] = []

    # --- 1. Stream reachability ---------------------------------------------------
    try:
        stream_info = await ctx.redis.xinfo_stream(ctx.settings.redis_stream_name)
    except Exception as exc:
        severity = ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now)
        results.append(
            CheckResult(
                check_name="stream_reachable",
                feed=None,
                severity=severity,
                status=Status.FAIL,
                message=f"Redis stream unreachable: {exc}",
                details={"stream": ctx.settings.redis_stream_name, "error": str(exc)},
                ts_checked=now,
            )
        )
        return results

    results.append(
        CheckResult(
            check_name="stream_reachable",
            feed=None,
            severity=ctx.calendar.adjust_severity(Severity.P2_INFO, now),
            status=Status.PASS,
            message="Redis stream reachable",
            details={
                "stream": ctx.settings.redis_stream_name,
                "length": stream_info.get("length", 0),
            },
            ts_checked=now,
        )
    )

    # --- 2. Consumer group exists -------------------------------------------------
    try:
        groups = await ctx.redis.xinfo_groups(ctx.settings.redis_stream_name)
    except Exception as exc:
        severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
        results.append(
            CheckResult(
                check_name="consumer_group",
                feed=None,
                severity=severity,
                status=Status.WARN,
                message=f"Could not read consumer groups: {exc}",
                details={"error": str(exc)},
                ts_checked=now,
            )
        )
        return results

    group_names = [
        g.get("name", b"").decode() if isinstance(g.get("name"), bytes) else g.get("name", "") for g in groups
    ]
    target_group = ctx.settings.redis_consumer_group

    if target_group not in group_names:
        severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
        results.append(
            CheckResult(
                check_name="consumer_group",
                feed=None,
                severity=severity,
                status=Status.WARN,
                message=f"Consumer group '{target_group}' not found",
                details={"expected": target_group, "found": group_names},
                ts_checked=now,
            )
        )
    else:
        # --- 3. Pending message count (consumer lag) ------------------------------
        group_info = next(
            g
            for g in groups
            if (g.get("name", b"").decode() if isinstance(g.get("name"), bytes) else g.get("name", "")) == target_group
        )
        pending = group_info.get("pending", 0)

        if pending >= PENDING_CRITICAL:
            sev = Severity.P0_CRITICAL
            status = Status.FAIL
            msg = f"Consumer lag critical: {pending} pending messages"
        elif pending >= PENDING_WARN:
            sev = Severity.P1_WARNING
            status = Status.WARN
            msg = f"Consumer lag high: {pending} pending messages"
        else:
            sev = Severity.P2_INFO
            status = Status.PASS
            msg = f"Consumer lag healthy: {pending} pending messages"

        severity = ctx.calendar.adjust_severity(sev, now)
        results.append(
            CheckResult(
                check_name="consumer_lag",
                feed=None,
                severity=severity,
                status=status,
                message=msg,
                details={"group": target_group, "pending": pending},
                ts_checked=now,
            )
        )

    # --- 4. DLQ depth -------------------------------------------------------------
    try:
        dlq_len = await ctx.redis.xlen(ctx.settings.redis_dlq_stream_name)
    except Exception:
        # DLQ stream may not exist yet — that's fine
        dlq_len = 0

    if dlq_len >= DLQ_WARN:
        sev = Severity.P1_WARNING
        status = Status.WARN
        msg = f"DLQ has {dlq_len} messages"
    else:
        sev = Severity.P2_INFO
        status = Status.PASS
        msg = "DLQ empty"

    severity = ctx.calendar.adjust_severity(sev, now)
    results.append(
        CheckResult(
            check_name="dlq_depth",
            feed=None,
            severity=severity,
            status=status,
            message=msg,
            details={"dlq_stream": ctx.settings.redis_dlq_stream_name, "depth": dlq_len},
            ts_checked=now,
        )
    )

    return results
