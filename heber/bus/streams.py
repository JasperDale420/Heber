"""Event Bus Stream Configuration (PRD §60).

Stream definitions per dataset, consumer group mapping.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class StreamPriority(str, Enum):
    """Stream processing priority."""

    CRITICAL = "critical"  # Real-time market data
    HIGH = "high"  # Options, flow alerts
    NORMAL = "normal"  # Fundamentals, economic
    LOW = "low"  # Historical backfill


@dataclass
class StreamConfig:
    """Redis Stream configuration for a dataset."""

    name: str
    dataset: str
    priority: StreamPriority
    consumer_group: str
    max_len: int = 100000  # Stream length limit
    ttl_hours: int = 24

    @property
    def stream_key(self) -> str:
        """Redis stream key."""
        return f"heber:stream:{self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "dataset": self.dataset,
            "priority": self.priority.value,
            "consumer_group": self.consumer_group,
            "stream_key": self.stream_key,
            "max_len": self.max_len,
            "ttl_hours": self.ttl_hours,
        }


@dataclass
class ConsumerGroupConfig:
    """Consumer group configuration."""

    name: str
    streams: list[str]
    consumers_per_group: int = 3
    read_batch_size: int = 100
    block_ms: int = 5000

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "streams": self.streams,
            "consumers_per_group": self.consumers_per_group,
            "read_batch_size": self.read_batch_size,
            "block_ms": self.block_ms,
        }


# Stream inventory per PRD §60
DEFAULT_STREAMS: list[StreamConfig] = [
    # Market Data (Critical)
    StreamConfig("bars", "bars", StreamPriority.CRITICAL, "market-data-consumer"),
    StreamConfig("quotes", "quotes", StreamPriority.CRITICAL, "market-data-consumer"),
    StreamConfig("trades", "trades", StreamPriority.CRITICAL, "market-data-consumer"),
    StreamConfig("bars_daily", "bars_daily", StreamPriority.NORMAL, "market-data-consumer"),
    # Options (High)
    StreamConfig("option_quotes", "option_quotes", StreamPriority.HIGH, "options-consumer"),
    StreamConfig("option_trades", "option_trades", StreamPriority.HIGH, "options-consumer"),
    # Alternative (High)
    StreamConfig("flow_alerts", "flow_alerts", StreamPriority.HIGH, "alt-data-consumer"),
    StreamConfig("darkpool_trades", "darkpool_trades", StreamPriority.HIGH, "alt-data-consumer"),
    StreamConfig("congress_trades", "congress_trades", StreamPriority.NORMAL, "alt-data-consumer"),
    StreamConfig("lobbying", "lobbying", StreamPriority.LOW, "alt-data-consumer"),
    # Fundamentals (Normal)
    StreamConfig("company_info", "company_info", StreamPriority.NORMAL, "fundamentals-consumer"),
    StreamConfig("financials", "income_statement", StreamPriority.NORMAL, "fundamentals-consumer"),
    # Economic (Normal)
    StreamConfig("economic", "economic_indicators", StreamPriority.NORMAL, "economic-consumer"),
    # Forex & Crypto (Normal)
    StreamConfig("forex", "forex_rates", StreamPriority.NORMAL, "fxcrypto-consumer"),
    StreamConfig("crypto", "crypto_bars", StreamPriority.NORMAL, "fxcrypto-consumer"),
]

# Consumer group definitions
DEFAULT_CONSUMER_GROUPS: list[ConsumerGroupConfig] = [
    ConsumerGroupConfig(
        name="market-data-consumer",
        streams=["bars", "quotes", "trades", "bars_daily"],
        consumers_per_group=5,
        read_batch_size=500,
    ),
    ConsumerGroupConfig(
        name="options-consumer",
        streams=["option_quotes", "option_trades"],
        consumers_per_group=3,
        read_batch_size=200,
    ),
    ConsumerGroupConfig(
        name="alt-data-consumer",
        streams=["flow_alerts", "darkpool_trades", "congress_trades", "lobbying"],
        consumers_per_group=2,
        read_batch_size=100,
    ),
    ConsumerGroupConfig(
        name="fundamentals-consumer",
        streams=["company_info", "financials"],
        consumers_per_group=2,
        read_batch_size=50,
    ),
    ConsumerGroupConfig(
        name="economic-consumer",
        streams=["economic"],
        consumers_per_group=1,
        read_batch_size=50,
    ),
    ConsumerGroupConfig(
        name="fxcrypto-consumer",
        streams=["forex", "crypto"],
        consumers_per_group=2,
        read_batch_size=100,
    ),
]


class StreamRegistry:
    """Registry of all event bus streams."""

    def __init__(
        self,
        streams: list[StreamConfig] | None = None,
        consumer_groups: list[ConsumerGroupConfig] | None = None,
    ):
        self.streams = {s.name: s for s in (streams or DEFAULT_STREAMS)}
        self.consumer_groups = {g.name: g for g in (consumer_groups or DEFAULT_CONSUMER_GROUPS)}

    def get_stream(self, name: str) -> StreamConfig | None:
        """Get stream config by name."""
        return self.streams.get(name)

    def get_consumer_group(self, name: str) -> ConsumerGroupConfig | None:
        """Get consumer group config by name."""
        return self.consumer_groups.get(name)

    def list_streams(self) -> list[dict[str, Any]]:
        """List all streams."""
        return [s.to_dict() for s in self.streams.values()]

    def list_consumer_groups(self) -> list[dict[str, Any]]:
        """List all consumer groups."""
        return [g.to_dict() for g in self.consumer_groups.values()]

    def get_streams_by_priority(self, priority: StreamPriority) -> list[StreamConfig]:
        """Get streams by priority level."""
        return [s for s in self.streams.values() if s.priority == priority]

    def get_streams_for_consumer(self, consumer_group: str) -> list[StreamConfig]:
        """Get streams for a consumer group."""
        return [s for s in self.streams.values() if s.consumer_group == consumer_group]

    def generate_report(self) -> dict[str, Any]:
        """Generate stream configuration report."""
        by_priority = {
            "critical": len(self.get_streams_by_priority(StreamPriority.CRITICAL)),
            "high": len(self.get_streams_by_priority(StreamPriority.HIGH)),
            "normal": len(self.get_streams_by_priority(StreamPriority.NORMAL)),
            "low": len(self.get_streams_by_priority(StreamPriority.LOW)),
        }

        return {
            "summary": {
                "total_streams": len(self.streams),
                "total_consumer_groups": len(self.consumer_groups),
                "by_priority": by_priority,
            },
            "streams": self.list_streams(),
            "consumer_groups": self.list_consumer_groups(),
            "generated_at": datetime.now(UTC).isoformat(),
        }
