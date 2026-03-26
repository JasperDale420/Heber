"""Data models for health check results."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from heber.config import Settings
from heber.reader.core import HeberReader


class Severity(str, Enum):
    P0_CRITICAL = "critical"
    P1_WARNING = "warning"
    P2_INFO = "info"

    def is_more_severe_than(self, other: Severity) -> bool:
        order = {Severity.P0_CRITICAL: 0, Severity.P1_WARNING: 1, Severity.P2_INFO: 2}
        return order[self] < order[other]


class Status(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    ERROR = "error"

    @property
    def is_healthy(self) -> bool:
        return self == Status.PASS


@dataclass
class CheckResult:
    check_name: str
    feed: str | None
    severity: Severity
    status: Status
    message: str
    details: dict[str, Any]
    ts_checked: datetime
    instrument_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "feed": self.feed,
            "severity": self.severity.value,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "ts_checked": self.ts_checked.isoformat(),
            "instrument_key": self.instrument_key,
        }

    def to_flat_row(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "feed": self.feed or "",
            "severity": self.severity.value,
            "status": self.status.value,
            "message": self.message,
            "details_json": json.dumps(self.details, default=str),
            "ts_checked": self.ts_checked,
            "instrument_key": self.instrument_key or "",
        }


@dataclass
class CheckContext:
    settings: Settings
    reader: HeberReader
    redis: Any  # redis.asyncio.Redis
    calendar: Any  # MarketCalendar
    store: Any  # HealthStore
