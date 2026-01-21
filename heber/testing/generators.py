"""Test Data Generators (PRD §50).

Synthetic data generation for testing.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class TestDataConfig:
    """Configuration for test data generation."""

    symbols: list[str]
    start_date: datetime
    end_date: datetime
    seed: int = 42

    @property
    def date_range(self) -> list[datetime]:
        """Generate list of dates in range."""
        dates = []
        current = self.start_date
        while current <= self.end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates


class SyntheticDataGenerator:
    """Generate synthetic market data for testing."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def generate_event_id(self) -> str:
        """Generate unique event ID."""
        return str(uuid.uuid4())

    def generate_bar(
        self,
        symbol: str,
        ts_event: datetime,
        base_price: float = 100.0,
    ) -> dict[str, Any]:
        """Generate a single OHLCV bar.

        Args:
            symbol: Stock symbol
            ts_event: Event timestamp
            base_price: Base price to vary around

        Returns:
            Bar data dictionary
        """
        # Generate realistic price movement
        volatility = self.rng.uniform(0.005, 0.02)
        open_price = base_price * (1 + self.rng.uniform(-volatility, volatility))
        high_price = open_price * (1 + self.rng.uniform(0, volatility))
        low_price = open_price * (1 - self.rng.uniform(0, volatility))
        close_price = self.rng.uniform(low_price, high_price)
        volume = self.rng.randint(100000, 10000000)

        return {
            "event_id": self.generate_event_id(),
            "symbol": symbol,
            "ts_event": ts_event.isoformat(),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume,
            "vwap": round((high_price + low_price + close_price) / 3, 2),
        }

    def generate_trade(
        self,
        symbol: str,
        ts_event: datetime,
        base_price: float = 100.0,
    ) -> dict[str, Any]:
        """Generate a single trade event."""
        price = base_price * (1 + self.rng.uniform(-0.01, 0.01))
        size = self.rng.randint(1, 10000)

        return {
            "event_id": self.generate_event_id(),
            "symbol": symbol,
            "ts_event": ts_event.isoformat(),
            "price": round(price, 2),
            "size": size,
            "exchange": self.rng.choice(["NYSE", "NASDAQ", "ARCA", "BATS"]),
            "conditions": self.rng.choice(["@", "F", "I", "T"]),
        }

    def generate_quote(
        self,
        symbol: str,
        ts_event: datetime,
        base_price: float = 100.0,
    ) -> dict[str, Any]:
        """Generate a single quote event."""
        spread = self.rng.uniform(0.01, 0.10)
        bid_price = base_price * (1 + self.rng.uniform(-0.01, 0.01))
        ask_price = bid_price + spread

        return {
            "event_id": self.generate_event_id(),
            "symbol": symbol,
            "ts_event": ts_event.isoformat(),
            "bid_price": round(bid_price, 2),
            "ask_price": round(ask_price, 2),
            "bid_size": self.rng.randint(100, 10000),
            "ask_size": self.rng.randint(100, 10000),
            "bid_exchange": self.rng.choice(["NYSE", "NASDAQ"]),
            "ask_exchange": self.rng.choice(["NYSE", "NASDAQ"]),
        }

    def generate_bars_batch(
        self,
        config: TestDataConfig,
        base_prices: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a batch of bars for testing.

        Args:
            config: Test data configuration
            base_prices: Base prices per symbol

        Returns:
            List of bar events
        """
        bars = []
        base_prices = base_prices or {s: 100.0 + i * 10 for i, s in enumerate(config.symbols)}

        for date in config.date_range:
            for symbol in config.symbols:
                # Generate market hours bars (9:30 - 16:00)
                market_open = date.replace(hour=9, minute=30, second=0, microsecond=0)
                for minute in range(0, 390, 1):  # 6.5 hours in minutes
                    ts_event = market_open + timedelta(minutes=minute)
                    bar = self.generate_bar(symbol, ts_event, base_prices[symbol])
                    bars.append(bar)

        return bars

    def generate_golden_dataset(
        self,
        dataset_type: str,
        num_records: int = 100,
        symbols: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a curated golden dataset for testing.

        Args:
            dataset_type: Type of data (bars, trades, quotes)
            num_records: Number of records to generate
            symbols: Symbols to generate data for

        Returns:
            List of events
        """
        symbols = symbols or ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
        base_time = datetime(2025, 1, 15, 10, 0, 0, tzinfo=UTC)
        records = []

        generator_map = {
            "bars": self.generate_bar,
            "trades": self.generate_trade,
            "quotes": self.generate_quote,
        }

        generator = generator_map.get(dataset_type, self.generate_bar)

        for i in range(num_records):
            symbol = symbols[i % len(symbols)]
            ts_event = base_time + timedelta(seconds=i)
            record = generator(symbol, ts_event)
            records.append(record)

        return records


@dataclass
class TestFixture:
    """Reusable test fixture with known values."""

    name: str
    description: str
    data: list[dict[str, Any]]
    expected_results: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "record_count": len(self.data),
            "expected_results": self.expected_results,
        }


# Pre-built test fixtures
SIMPLE_BARS_FIXTURE = TestFixture(
    name="simple_bars",
    description="5 bars for AAPL on 2025-01-15",
    data=[
        {
            "event_id": "bar-001",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:00:00Z",
            "open": 100,
            "high": 102,
            "low": 99,
            "close": 101,
            "volume": 1000000,
        },
        {
            "event_id": "bar-002",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:01:00Z",
            "open": 101,
            "high": 103,
            "low": 100,
            "close": 102,
            "volume": 1100000,
        },
        {
            "event_id": "bar-003",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:02:00Z",
            "open": 102,
            "high": 104,
            "low": 101,
            "close": 103,
            "volume": 1200000,
        },
        {
            "event_id": "bar-004",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:03:00Z",
            "open": 103,
            "high": 105,
            "low": 102,
            "close": 104,
            "volume": 1300000,
        },
        {
            "event_id": "bar-005",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:04:00Z",
            "open": 104,
            "high": 106,
            "low": 103,
            "close": 105,
            "volume": 1400000,
        },
    ],
    expected_results={
        "total_volume": 6000000,
        "avg_close": 103.0,
        "last_close": 105,
    },
)

LEAKAGE_TEST_FIXTURE = TestFixture(
    name="leakage_test",
    description="Bars with known ts_available for leakage testing",
    data=[
        {
            "event_id": "lk-001",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:00:00Z",
            "ts_available": "2025-01-15T10:01:00Z",
            "close": 100,
        },
        {
            "event_id": "lk-002",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:01:00Z",
            "ts_available": "2025-01-15T10:02:00Z",
            "close": 101,
        },
        {
            "event_id": "lk-003",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:02:00Z",
            "ts_available": "2025-01-15T10:03:00Z",
            "close": 102,
        },
        # Future data - should NOT be returned when querying asof 10:01:30
        {
            "event_id": "lk-004",
            "symbol": "AAPL",
            "ts_event": "2025-01-15T10:03:00Z",
            "ts_available": "2025-01-15T10:04:00Z",
            "close": 103,
        },
    ],
    expected_results={
        "asof_10_01_30_count": 1,  # Only lk-001 available
        "asof_10_02_30_count": 2,  # lk-001 and lk-002 available
    },
)

DEFAULT_FIXTURES: dict[str, TestFixture] = {
    "simple_bars": SIMPLE_BARS_FIXTURE,
    "leakage_test": LEAKAGE_TEST_FIXTURE,
}


class FixtureRegistry:
    """Registry of test fixtures."""

    def __init__(self, fixtures: dict[str, TestFixture] | None = None):
        self.fixtures = fixtures or DEFAULT_FIXTURES

    def get(self, name: str) -> TestFixture | None:
        """Get fixture by name."""
        return self.fixtures.get(name)

    def list_all(self) -> list[str]:
        """List all fixture names."""
        return list(self.fixtures.keys())

    def add(self, fixture: TestFixture) -> None:
        """Add a fixture to the registry."""
        self.fixtures[fixture.name] = fixture
