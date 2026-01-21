"""Data Source Inventory (PRD §55-57).

Provider capabilities, dataset catalog, and data boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class ProviderPriority(int, Enum):
    """Provider priority level."""

    PRIMARY = 1
    SECONDARY = 2
    TERTIARY = 3


class StorageBoundary(str, Enum):
    """Storage boundary (PRD §56)."""

    HEBER = "heber"  # Structured data (Silver layer)
    DOCUMENT_STORE = "document_store"  # Unstructured data


@dataclass
class DataProvider:
    """Data provider definition (PRD §55.1)."""

    name: str
    capabilities: list[str]
    priority: ProviderPriority
    streaming: bool
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "capabilities": self.capabilities,
            "priority": self.priority.value,
            "streaming": self.streaming,
            "notes": self.notes,
        }


# Default providers from PRD §55.1
DEFAULT_PROVIDERS: list[DataProvider] = [
    DataProvider(
        name="Alpaca",
        capabilities=["bars", "quotes", "trades", "options", "crypto", "news"],
        priority=ProviderPriority.PRIMARY,
        streaming=True,
        notes="Primary market data provider",
    ),
    DataProvider(
        name="Unusual Whales",
        capabilities=["flow_alerts", "darkpool_trades", "congress", "lobbying"],
        priority=ProviderPriority.PRIMARY,
        streaming=False,
        notes="Alternative data provider",
    ),
    DataProvider(
        name="Finnhub",
        capabilities=["bars", "quotes", "news", "sentiment"],
        priority=ProviderPriority.SECONDARY,
        streaming=True,
    ),
    DataProvider(
        name="Alpha Vantage",
        capabilities=["forex", "crypto", "economic_indicators"],
        priority=ProviderPriority.TERTIARY,
        streaming=False,
    ),
    DataProvider(
        name="yFinance",
        capabilities=["historical_bars"],
        priority=ProviderPriority.SECONDARY,
        streaming=False,
        notes="Fallback for historical data",
    ),
    DataProvider(
        name="News API",
        capabilities=["news_articles", "headlines"],
        priority=ProviderPriority.PRIMARY,
        streaming=False,
    ),
    DataProvider(
        name="SEC Edgar",
        capabilities=["10-K", "10-Q", "8-K", "13F", "company_info"],
        priority=ProviderPriority.PRIMARY,
        streaming=False,
    ),
]


@dataclass
class DataTypeSpec:
    """Data type specification (PRD §55.2)."""

    name: str
    storage: StorageBoundary
    format: str
    query_pattern: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "storage": self.storage.value,
            "format": self.format,
            "query_pattern": self.query_pattern,
        }


# Default data types from PRD §55.2
DEFAULT_DATA_TYPES: list[DataTypeSpec] = [
    DataTypeSpec("market_data", StorageBoundary.HEBER, "Parquet", "Columnar analytics, ASOF"),
    DataTypeSpec("options", StorageBoundary.HEBER, "Parquet", "Columnar analytics"),
    DataTypeSpec("flow_darkpool", StorageBoundary.HEBER, "Parquet", "Columnar analytics"),
    DataTypeSpec("fundamentals", StorageBoundary.HEBER, "Parquet", "Point-in-time lookups"),
    DataTypeSpec("economic_indicators", StorageBoundary.HEBER, "Parquet", "Time-series analysis"),
    DataTypeSpec("forex_crypto", StorageBoundary.HEBER, "Parquet", "Time-series analysis"),
    DataTypeSpec("news_metadata", StorageBoundary.HEBER, "Parquet", "Event-driven joins"),
    DataTypeSpec("news_body", StorageBoundary.DOCUMENT_STORE, "JSON/Text", "Full-text search"),
    DataTypeSpec("sec_filings", StorageBoundary.DOCUMENT_STORE, "JSON/Text", "Full-text search, RAG"),
]


@dataclass
class DatasetSpec:
    """Dataset specification (PRD §57)."""

    name: str
    category: str
    description: str
    providers: list[str]
    implemented: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "description": self.description,
            "providers": self.providers,
            "implemented": self.implemented,
        }


# Dataset catalog from PRD §57
DEFAULT_DATASETS: list[DatasetSpec] = [
    # Market Data
    DatasetSpec("bars", "market_data", "OHLCV minute bars", ["Alpaca", "Finnhub"], True),
    DatasetSpec("quotes", "market_data", "Level 1 quotes", ["Alpaca"], True),
    DatasetSpec("trades", "market_data", "Individual trades", ["Alpaca"], True),
    DatasetSpec("bars_daily", "market_data", "Daily OHLCV bars", ["Alpaca", "yFinance"], False),
    # Options
    DatasetSpec("option_quotes", "options", "Option chain quotes", ["Alpaca"], False),
    DatasetSpec("option_trades", "options", "Option trades", ["Alpaca"], False),
    # Alternative
    DatasetSpec("congress_trades", "alternative", "Congress trading activity", ["Unusual Whales"], False),
    DatasetSpec("lobbying", "alternative", "Lobbying disclosures", ["Unusual Whales"], False),
    DatasetSpec("flow_alerts", "alternative", "Options flow alerts", ["Unusual Whales"], False),
    DatasetSpec("darkpool_trades", "alternative", "Dark pool transactions", ["Unusual Whales"], False),
    # Fundamentals
    DatasetSpec("company_info", "fundamentals", "Company metadata", ["SEC Edgar"], False),
    DatasetSpec("income_statement", "fundamentals", "Income statements", ["Alpha Vantage"], False),
    DatasetSpec("balance_sheet", "fundamentals", "Balance sheets", ["Alpha Vantage"], False),
    DatasetSpec("cash_flow", "fundamentals", "Cash flow statements", ["Alpha Vantage"], False),
    DatasetSpec("ratios", "fundamentals", "Financial ratios", ["Alpha Vantage"], False),
    # Economic
    DatasetSpec("gdp", "economic", "Gross Domestic Product", ["Alpha Vantage"], False),
    DatasetSpec("cpi", "economic", "Consumer Price Index", ["Alpha Vantage"], False),
    DatasetSpec("unemployment", "economic", "Unemployment rate", ["Alpha Vantage"], False),
    DatasetSpec("interest_rate", "economic", "Fed funds rate", ["Alpha Vantage"], False),
    DatasetSpec("treasury_yield", "economic", "Treasury yields", ["Alpha Vantage"], False),
    # Forex & Crypto
    DatasetSpec("forex_rates", "forex_crypto", "Currency exchange rates", ["Alpha Vantage"], False),
    DatasetSpec("crypto_bars", "forex_crypto", "Cryptocurrency OHLCV", ["Alpaca"], False),
    DatasetSpec("crypto_quotes", "forex_crypto", "Cryptocurrency quotes", ["Alpaca"], False),
    # News
    DatasetSpec("news_articles", "news", "News article metadata", ["News API", "Alpaca"], False),
    DatasetSpec("news_sentiment", "news", "Sentiment scores", ["Finnhub"], False),
]


class ProviderRegistry:
    """Registry of data providers."""

    def __init__(
        self,
        providers: list[DataProvider] | None = None,
    ):
        self.providers = {p.name: p for p in (providers or DEFAULT_PROVIDERS)}

    def get(self, name: str) -> DataProvider | None:
        """Get provider by name."""
        return self.providers.get(name)

    def list_all(self) -> list[dict[str, Any]]:
        """List all providers."""
        return [p.to_dict() for p in self.providers.values()]

    def get_by_capability(self, capability: str) -> list[DataProvider]:
        """Get providers that support a capability."""
        return [p for p in self.providers.values() if capability in p.capabilities]

    def get_primary(self) -> list[DataProvider]:
        """Get primary providers."""
        return [p for p in self.providers.values() if p.priority == ProviderPriority.PRIMARY]


class DatasetCatalog:
    """Catalog of available datasets."""

    def __init__(
        self,
        datasets: list[DatasetSpec] | None = None,
        data_types: list[DataTypeSpec] | None = None,
    ):
        self.datasets = {d.name: d for d in (datasets or DEFAULT_DATASETS)}
        self.data_types = {d.name: d for d in (data_types or DEFAULT_DATA_TYPES)}

    def get(self, name: str) -> DatasetSpec | None:
        """Get dataset by name."""
        return self.datasets.get(name)

    def list_all(self) -> list[dict[str, Any]]:
        """List all datasets."""
        return [d.to_dict() for d in self.datasets.values()]

    def list_by_category(self, category: str) -> list[DatasetSpec]:
        """List datasets by category."""
        return [d for d in self.datasets.values() if d.category == category]

    def list_implemented(self) -> list[DatasetSpec]:
        """List implemented datasets."""
        return [d for d in self.datasets.values() if d.implemented]

    def list_pending(self) -> list[DatasetSpec]:
        """List pending datasets."""
        return [d for d in self.datasets.values() if not d.implemented]

    def get_storage_boundary(self, data_type: str) -> StorageBoundary | None:
        """Get storage boundary for a data type."""
        spec = self.data_types.get(data_type)
        return spec.storage if spec else None

    def generate_report(self) -> dict[str, Any]:
        """Generate dataset catalog report."""
        implemented = self.list_implemented()
        pending = self.list_pending()

        by_category: dict[str, list[str]] = {}
        for d in self.datasets.values():
            if d.category not in by_category:
                by_category[d.category] = []
            by_category[d.category].append(d.name)

        return {
            "summary": {
                "total_datasets": len(self.datasets),
                "implemented": len(implemented),
                "pending": len(pending),
                "completion": f"{len(implemented) / len(self.datasets) * 100:.1f}%",
            },
            "by_category": by_category,
            "storage_boundaries": {
                "heber": [d.name for d in self.data_types.values() if d.storage == StorageBoundary.HEBER],
                "document_store": [
                    d.name for d in self.data_types.values() if d.storage == StorageBoundary.DOCUMENT_STORE
                ],
            },
            "generated_at": datetime.now(UTC).isoformat(),
        }
