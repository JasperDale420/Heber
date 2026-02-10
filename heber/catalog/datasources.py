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

# Provider name constants
PROVIDER_ALPACA = "Alpaca"
PROVIDER_UNUSUAL_WHALES = "Unusual Whales"
PROVIDER_FINNHUB = "Finnhub"
PROVIDER_ALPHA_VANTAGE = "Alpha Vantage"
PROVIDER_YFINANCE = "yFinance"
PROVIDER_NEWS_API = "News API"
PROVIDER_SEC_EDGAR = "SEC Edgar"


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
        name=PROVIDER_ALPACA,
        capabilities=["bars", "quotes", "trades", "options", "crypto", "news"],
        priority=ProviderPriority.PRIMARY,
        streaming=True,
        notes="Primary market data provider",
    ),
    DataProvider(
        name=PROVIDER_UNUSUAL_WHALES,
        capabilities=["flow_alerts", "darkpool", "congress", "lobbying"],
        priority=ProviderPriority.PRIMARY,
        streaming=False,
        notes="Alternative data provider",
    ),
    DataProvider(
        name=PROVIDER_FINNHUB,
        capabilities=["bars", "quotes", "news", "sentiment"],
        priority=ProviderPriority.SECONDARY,
        streaming=True,
    ),
    DataProvider(
        name=PROVIDER_ALPHA_VANTAGE,
        capabilities=["forex", "crypto", "economic_indicators"],
        priority=ProviderPriority.TERTIARY,
        streaming=False,
    ),
    DataProvider(
        name=PROVIDER_YFINANCE,
        capabilities=["historical_bars"],
        priority=ProviderPriority.SECONDARY,
        streaming=False,
        notes="Fallback for historical data",
    ),
    DataProvider(
        name=PROVIDER_NEWS_API,
        capabilities=["news_articles", "headlines"],
        priority=ProviderPriority.PRIMARY,
        streaming=False,
    ),
    DataProvider(
        name=PROVIDER_SEC_EDGAR,
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


# Dataset catalog — aligned with all 44 Silver schemas
DEFAULT_DATASETS: list[DatasetSpec] = [
    # Core Market Data
    DatasetSpec("bars", "market_data", "OHLCV minute/daily bars", [PROVIDER_ALPACA], True),
    DatasetSpec("quotes", "market_data", "Level 1 bid/ask quotes", [PROVIDER_ALPACA], True),
    DatasetSpec("trades", "market_data", "Individual trade prints", [PROVIDER_ALPACA], True),
    DatasetSpec("orderbook", "market_data", "Order book snapshots", [PROVIDER_ALPACA], True),
    DatasetSpec(
        "news", "market_data", "News articles with headlines and summaries", [PROVIDER_ALPACA, PROVIDER_NEWS_API], True
    ),
    # Options Flow
    DatasetSpec("flow_alerts", "options_flow", "Unusual options flow alerts", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("darkpool", "options_flow", "Dark pool trade prints", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("hottest_chain", "options_flow", "Most active option contracts", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("net_premium_tick", "options_flow", "Net premium ticks (call vs put)", [PROVIDER_UNUSUAL_WHALES], True),
    # Sentiment
    DatasetSpec("sector_tide", "sentiment", "Sector-level options sentiment", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("market_tide", "sentiment", "Market-wide options sentiment", [PROVIDER_UNUSUAL_WHALES], True),
    # Greeks / Analytics
    DatasetSpec("greek_exposure", "analytics", "GEX/DEX/Vanna/Charm exposure", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("max_pain", "analytics", "Max pain strike and OI distribution", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("iv_rank", "analytics", "IV rank and percentile", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("iv_term_structure", "analytics", "IV term structure by expiry", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("volatility_stats", "analytics", "Realized vs implied volatility", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("oi_change", "analytics", "Open interest changes (call/put)", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("seasonality", "analytics", "Historical monthly return seasonality", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("volume_profile", "analytics", "Option volume profile by price", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("group_flow", "analytics", "Grouped GEX/DEX flow by sector/index", [PROVIDER_UNUSUAL_WHALES], True),
    # Options Deep Data
    DatasetSpec("option_contract", "options", "Option contract reference data (SCD)", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec(
        "option_history", "options", "Historical option contract OHLCV + Greeks", [PROVIDER_UNUSUAL_WHALES], True
    ),
    DatasetSpec(
        "option_chain_snapshot",
        "options",
        "Full option chain snapshots with IV/volume",
        [PROVIDER_UNUSUAL_WHALES],
        True,
    ),
    # ETF
    DatasetSpec("etf_holding", "etf", "ETF constituent holdings and weights", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("etf_flow", "etf", "ETF fund flow data (inflows/outflows)", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec(
        "etf_metadata", "etf", "ETF fund metadata (issuer, AUM, expense ratio)", [PROVIDER_UNUSUAL_WHALES], True
    ),
    DatasetSpec(
        "etf_sector_weights", "etf", "ETF sector/country/asset class breakdowns", [PROVIDER_UNUSUAL_WHALES], True
    ),
    # Short / FTD
    DatasetSpec(
        "short_data", "short_interest", "Short interest, days to cover, float %", [PROVIDER_UNUSUAL_WHALES], True
    ),
    DatasetSpec("ftd", "short_interest", "Failures to deliver from SEC data", [PROVIDER_UNUSUAL_WHALES], True),
    # Screeners
    DatasetSpec("most_active", "screener", "Most actively traded symbols", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("mover", "screener", "Biggest price movers (gainers/losers)", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec(
        "screener_result", "screener", "Stock screener with fundamentals overlay", [PROVIDER_UNUSUAL_WHALES], True
    ),
    # Reference / Fundamentals
    DatasetSpec(
        "earnings", "fundamentals", "Earnings dates, EPS estimates and actuals", [PROVIDER_UNUSUAL_WHALES], True
    ),
    DatasetSpec(
        "corporate_action", "fundamentals", "Dividends, splits, corporate actions", [PROVIDER_UNUSUAL_WHALES], True
    ),
    DatasetSpec(
        "stock_fundamentals",
        "fundamentals",
        "Company fundamentals snapshot (P/E, mcap)",
        [PROVIDER_UNUSUAL_WHALES],
        True,
    ),
    DatasetSpec(
        "analyst_ratings", "fundamentals", "Analyst ratings, price targets, upgrades", [PROVIDER_UNUSUAL_WHALES], True
    ),
    # Alternative Data
    DatasetSpec("congress_trades", "alternative", "Congressional trading disclosures", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("insider_trades", "alternative", "SEC Form 4 insider trading filings", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("insider_flow", "alternative", "Aggregated insider buy/sell flow", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("institution_holdings", "alternative", "13F institutional holdings", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec(
        "institution_activity", "alternative", "Institutional portfolio activity", [PROVIDER_UNUSUAL_WHALES], True
    ),
    DatasetSpec("politician_trades", "alternative", "Politician trading disclosures", [PROVIDER_UNUSUAL_WHALES], True),
    # Economic / Market
    DatasetSpec("economic_events", "economic", "Economic calendar (GDP, CPI, etc.)", [PROVIDER_UNUSUAL_WHALES], True),
    DatasetSpec("market_indicators", "economic", "Market-wide indicators and breadth", [PROVIDER_UNUSUAL_WHALES], True),
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
