"""Implementation Slices (PRD §61).

Ordered implementation slices for incremental delivery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class SliceStatus(str, Enum):
    """Implementation slice status."""

    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    BLOCKED = "blocked"


@dataclass
class ImplementationSlice:
    """Implementation slice definition (PRD §61)."""

    number: int
    name: str
    description: str
    datasets: list[str]
    dependencies: list[int] = field(default_factory=list)  # Slice numbers
    status: SliceStatus = SliceStatus.NOT_STARTED

    def to_dict(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "name": self.name,
            "description": self.description,
            "datasets": self.datasets,
            "dependencies": self.dependencies,
            "status": self.status.value,
        }


# Implementation slices from PRD §61
DEFAULT_SLICES: list[ImplementationSlice] = [
    ImplementationSlice(
        number=1,
        name="Core Market Data",
        description="Essential OHLCV bars, quotes, trades for equities",
        datasets=["bars", "quotes", "trades", "bars_daily"],
        dependencies=[],
        status=SliceStatus.COMPLETED,
    ),
    ImplementationSlice(
        number=2,
        name="Options Chain",
        description="Option quotes and trades with Greeks",
        datasets=["option_quotes", "option_trades"],
        dependencies=[1],
    ),
    ImplementationSlice(
        number=3,
        name="Alternative Data",
        description="Flow alerts, dark pool, congress trades, lobbying",
        datasets=["flow_alerts", "darkpool", "congress_trades", "lobbying"],
        dependencies=[1],
    ),
    ImplementationSlice(
        number=4,
        name="News & Filings",
        description="News events, SEC filings metadata",
        datasets=["news_events", "filing_events"],
        dependencies=[1],
    ),
    ImplementationSlice(
        number=5,
        name="Fundamentals",
        description="Company info, financials, ratios",
        datasets=["company_info", "income_statement", "balance_sheet", "cash_flow", "ratios"],
        dependencies=[1],
    ),
    ImplementationSlice(
        number=6,
        name="Economic & FX",
        description="Economic indicators, forex, crypto",
        datasets=[
            "economic_indicators",
            "interest_rate",
            "treasury_yield",
            "forex_rates",
            "crypto_bars",
            "crypto_quotes",
        ],
        dependencies=[1],
    ),
    ImplementationSlice(
        number=7,
        name="Gold Layer",
        description="Derived features, labels, ML-ready datasets",
        datasets=["gold_features", "gold_labels"],
        dependencies=[1, 2, 3, 5],
    ),
    ImplementationSlice(
        number=8,
        name="Hot Store",
        description="ClickHouse for real-time queries",
        datasets=["*"],  # All datasets replicated
        dependencies=[1, 2, 3],
    ),
]


class SliceManager:
    """Manage implementation slices."""

    def __init__(
        self,
        slices: list[ImplementationSlice] | None = None,
    ):
        self.slices = {s.number: s for s in (slices or DEFAULT_SLICES)}

    def get_slice(self, number: int) -> ImplementationSlice | None:
        """Get slice by number."""
        return self.slices.get(number)

    def list_all(self) -> list[dict[str, Any]]:
        """List all slices."""
        return [s.to_dict() for s in sorted(self.slices.values(), key=lambda x: x.number)]

    def get_ready_slices(self) -> list[ImplementationSlice]:
        """Get slices ready to implement (dependencies met)."""
        ready = []
        for slice_ in self.slices.values():
            if slice_.status == SliceStatus.NOT_STARTED:
                deps_met = all(
                    self.slices.get(dep, ImplementationSlice(0, "", "", [])).status == SliceStatus.COMPLETED
                    for dep in slice_.dependencies
                )
                if deps_met:
                    ready.append(slice_)
        return ready

    def mark_completed(self, number: int) -> bool:
        """Mark a slice as completed."""
        slice_ = self.slices.get(number)
        if slice_:
            slice_.status = SliceStatus.COMPLETED
            return True
        return False

    def mark_in_progress(self, number: int) -> bool:
        """Mark a slice as in progress."""
        slice_ = self.slices.get(number)
        if slice_:
            slice_.status = SliceStatus.IN_PROGRESS
            return True
        return False

    def get_completion_status(self) -> dict[str, int]:
        """Get count of slices by status."""
        status_counts: dict[str, int] = {s.value: 0 for s in SliceStatus}
        for slice_ in self.slices.values():
            status_counts[slice_.status.value] += 1
        return status_counts

    def generate_report(self) -> dict[str, Any]:
        """Generate implementation status report."""
        status = self.get_completion_status()
        total = len(self.slices)
        completed = status.get("completed", 0)

        return {
            "summary": {
                "total_slices": total,
                "completed": completed,
                "in_progress": status.get("in_progress", 0),
                "not_started": status.get("not_started", 0),
                "blocked": status.get("blocked", 0),
                "progress": f"{completed / total * 100:.0f}%",
            },
            "ready_slices": [s.to_dict() for s in self.get_ready_slices()],
            "slices": self.list_all(),
            "generated_at": datetime.now(UTC).isoformat(),
        }
