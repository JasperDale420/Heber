"""Catalog seed data and functions for datasets, schema versions, and feed mappings.

Idempotent — safe to re-run on every startup. Uses upsert patterns so existing rows
are updated rather than duplicated.

Extracted from scripts/seed_catalog.py so the catalog lifespan can import cleanly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from datetime import date as date_type
from pathlib import Path

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from heber.catalog.db import DataCoverage, Dataset, DatasetVersion, FeedMapping
from heber.config import settings
from heber.schemas.silver import SILVER_SCHEMAS

logger = structlog.get_logger(__name__)

_DATE_PARTITION_RE = __import__("re").compile(r"^dt=(\d{4}-\d{2}-\d{2})$")

# ── Dataset Descriptions ─────────────────────────────────────────────────────

DATASET_DESCRIPTIONS: dict[str, str] = {
    # Core Market Data
    "bars": "OHLCV minute/daily bars from Alpaca",
    "quotes": "Level 1 bid/ask quotes from Alpaca",
    "trades": "Individual trade prints from Alpaca",
    # Options Flow
    "flow_alerts": "Unusual options flow alerts from Unusual Whales",
    "darkpool": "Dark pool trade prints from Unusual Whales",
    # Sentiment
    "sector_tide": "Sector-level options sentiment from Unusual Whales",
    "market_tide": "Market-wide options sentiment from Unusual Whales",
    # Core Analytics
    "greek_exposure": "GEX/DEX/Vanna/Charm exposure by strike from Unusual Whales",
    "max_pain": "Max pain strike and OI distribution from Unusual Whales",
    "net_premium_tick": "Net premium ticks (call vs put) from Unusual Whales",
    "hottest_chain": "Most active option contracts from Unusual Whales",
    # Reference Data
    "earnings": "Earnings dates, EPS estimates and actuals",
    "corporate_action": "Dividends, splits, and other corporate actions",
    # Screeners
    "most_active": "Most actively traded symbols by volume",
    "mover": "Biggest price movers (gainers/losers)",
    "screener_result": "Stock screener results with fundamentals overlay",
    # Advanced Analytics
    "iv_rank": "IV rank and percentile for equities from Unusual Whales",
    "iv_term_structure": "Implied volatility term structure by expiry",
    "volatility_stats": "Realized vs implied volatility statistics",
    "oi_change": "Open interest changes (call/put) from Unusual Whales",
    "historic_option_volume": "Historic option volume/open interest by expiry",
    # ETF Feeds
    "etf_holding": "ETF constituent holdings and weights",
    "etf_flow": "ETF fund flow data (inflows/outflows)",
    # Short / FTD
    "short_data": "Short interest, days to cover, percent float",
    "ftd": "Failures to deliver from SEC data",
    # Seasonality
    "seasonality": "Historical monthly return seasonality statistics",
    # Reference Data (SCD)
    "option_contract": "Option contract reference data (OCC symbol, expiry, strike)",
    "news": "News articles with headlines, summaries, and source metadata",
    "orderbook": "Order book snapshots (bids/asks as JSON)",
    # Alternative Data
    "congress_trades": "Congressional trading activity disclosures",
    "insider_trades": "SEC Form 4 insider trading filings",
    "insider_flow": "Aggregated insider buy/sell flow by sector",
    "institution_holdings": "13F institutional holdings filings",
    "institution_activity": "Institutional portfolio activity summary",
    "politician_trades": "Politician trading disclosures",
    # Market Analytics
    "analyst_ratings": "Analyst ratings, price targets, and upgrades/downgrades",
    "stock_fundamentals": "Company fundamentals snapshot (market cap, P/E, etc.)",
    "economic_events": "Economic calendar events (GDP, CPI, etc.)",
    "market_indicators": "Market-wide indicators and breadth metrics",
    # Options Deep Data
    "option_history": "Historical option contract OHLCV and Greeks",
    "option_chain_snapshot": "Full option chain snapshots with IV and volume",
    "volume_profile": "Option volume profile by price level",
    "group_flow": "Grouped GEX/DEX flow by sector/index",
    # ETF Deep Data
    "etf_metadata": "ETF fund metadata (issuer, expense ratio, AUM)",
    "etf_sector_weights": "ETF sector/country/asset class weight breakdowns",
}

# ── Feed Mappings (provider → gateway_feed → silver_dataset) ──────────────────

FEED_MAPPING_SEEDS: list[dict[str, str]] = [
    # Alpaca
    {"provider": "alpaca", "gateway_feed": "bars", "silver_dataset_name": "bars"},
    {"provider": "alpaca", "gateway_feed": "quotes", "silver_dataset_name": "quotes"},
    {"provider": "alpaca", "gateway_feed": "trades", "silver_dataset_name": "trades"},
    {"provider": "alpaca", "gateway_feed": "news", "silver_dataset_name": "news"},
    {"provider": "alpaca", "gateway_feed": "option_trades", "silver_dataset_name": "trades"},
    {"provider": "alpaca", "gateway_feed": "crypto_bars", "silver_dataset_name": "bars"},
    {"provider": "alpaca", "gateway_feed": "crypto_trades", "silver_dataset_name": "trades"},
    # Unusual Whales — Options flow
    {"provider": "unusual_whales", "gateway_feed": "flow_alerts", "silver_dataset_name": "flow_alerts"},
    {"provider": "unusual_whales", "gateway_feed": "flow", "silver_dataset_name": "flow_alerts"},
    {"provider": "unusual_whales", "gateway_feed": "ticker_flow", "silver_dataset_name": "flow_alerts"},
    {"provider": "unusual_whales", "gateway_feed": "darkpool", "silver_dataset_name": "darkpool"},
    {"provider": "unusual_whales", "gateway_feed": "darkpool_ticker", "silver_dataset_name": "darkpool"},
    {"provider": "unusual_whales", "gateway_feed": "hottest_chains", "silver_dataset_name": "hottest_chain"},
    # Unusual Whales — Sentiment
    {"provider": "unusual_whales", "gateway_feed": "sector_tide", "silver_dataset_name": "sector_tide"},
    {"provider": "unusual_whales", "gateway_feed": "market_tide", "silver_dataset_name": "market_tide"},
    # Unusual Whales — Analytics
    {"provider": "unusual_whales", "gateway_feed": "greek_exposure", "silver_dataset_name": "greek_exposure"},
    {"provider": "unusual_whales", "gateway_feed": "greeks", "silver_dataset_name": "greek_exposure"},
    {"provider": "unusual_whales", "gateway_feed": "gex", "silver_dataset_name": "greek_exposure"},
    {"provider": "unusual_whales", "gateway_feed": "max_pain", "silver_dataset_name": "max_pain"},
    {"provider": "unusual_whales", "gateway_feed": "net_premium_ticks", "silver_dataset_name": "net_premium_tick"},
    {"provider": "unusual_whales", "gateway_feed": "iv_rank", "silver_dataset_name": "iv_rank"},
    {"provider": "unusual_whales", "gateway_feed": "volatility", "silver_dataset_name": "volatility_stats"},
    {"provider": "unusual_whales", "gateway_feed": "oi_change", "silver_dataset_name": "oi_change"},
    {"provider": "unusual_whales", "gateway_feed": "seasonality", "silver_dataset_name": "seasonality"},
    # Unusual Whales — ETF
    {"provider": "unusual_whales", "gateway_feed": "etf_holdings", "silver_dataset_name": "etf_holding"},
    {"provider": "unusual_whales", "gateway_feed": "etf_flows", "silver_dataset_name": "etf_flow"},
    {"provider": "unusual_whales", "gateway_feed": "etf_info", "silver_dataset_name": "etf_metadata"},
    {"provider": "unusual_whales", "gateway_feed": "etf_sectors", "silver_dataset_name": "etf_sector_weights"},
    # Unusual Whales — Short / FTD
    {"provider": "unusual_whales", "gateway_feed": "short_interest", "silver_dataset_name": "short_data"},
    {"provider": "unusual_whales", "gateway_feed": "short_volume", "silver_dataset_name": "short_data"},
    {"provider": "unusual_whales", "gateway_feed": "ftd", "silver_dataset_name": "ftd"},
    {"provider": "unusual_whales", "gateway_feed": "ftds", "silver_dataset_name": "ftd"},
    {
        "provider": "unusual_whales",
        "gateway_feed": "historic_option_volume",
        "silver_dataset_name": "historic_option_volume",
    },
    # Unusual Whales — Alternative Data
    {"provider": "unusual_whales", "gateway_feed": "congress_trades", "silver_dataset_name": "congress_trades"},
    {"provider": "unusual_whales", "gateway_feed": "insider_trades", "silver_dataset_name": "insider_trades"},
    {"provider": "unusual_whales", "gateway_feed": "insider_flow", "silver_dataset_name": "insider_flow"},
    {
        "provider": "unusual_whales",
        "gateway_feed": "institution_holdings",
        "silver_dataset_name": "institution_holdings",
    },
    {"provider": "unusual_whales", "gateway_feed": "institutions", "silver_dataset_name": "institution_holdings"},
    {
        "provider": "unusual_whales",
        "gateway_feed": "institution_activity",
        "silver_dataset_name": "institution_activity",
    },
    {"provider": "unusual_whales", "gateway_feed": "politician_trades", "silver_dataset_name": "politician_trades"},
    # Unusual Whales — Market Analytics
    {"provider": "unusual_whales", "gateway_feed": "analyst_ratings", "silver_dataset_name": "analyst_ratings"},
    {"provider": "unusual_whales", "gateway_feed": "stock_fundamentals", "silver_dataset_name": "stock_fundamentals"},
    {"provider": "unusual_whales", "gateway_feed": "earnings", "silver_dataset_name": "earnings"},
    {"provider": "unusual_whales", "gateway_feed": "corporate_actions", "silver_dataset_name": "corporate_action"},
    {"provider": "unusual_whales", "gateway_feed": "economic_events", "silver_dataset_name": "economic_events"},
    {"provider": "unusual_whales", "gateway_feed": "market_indicators", "silver_dataset_name": "market_indicators"},
    # Unusual Whales — Screeners
    {"provider": "unusual_whales", "gateway_feed": "most_active", "silver_dataset_name": "most_active"},
    {"provider": "unusual_whales", "gateway_feed": "movers", "silver_dataset_name": "mover"},
    {"provider": "unusual_whales", "gateway_feed": "screener", "silver_dataset_name": "screener_result"},
    # Unusual Whales — Options Deep
    {"provider": "unusual_whales", "gateway_feed": "option_history", "silver_dataset_name": "option_history"},
    {"provider": "unusual_whales", "gateway_feed": "option_chain", "silver_dataset_name": "option_chain_snapshot"},
    {"provider": "unusual_whales", "gateway_feed": "volume_profile", "silver_dataset_name": "volume_profile"},
    {"provider": "unusual_whales", "gateway_feed": "group_flow", "silver_dataset_name": "group_flow"},
    # Reference
    {"provider": "unusual_whales", "gateway_feed": "option_contracts", "silver_dataset_name": "option_contract"},
    # Alpaca orderbook
    {"provider": "alpaca", "gateway_feed": "orderbook", "silver_dataset_name": "orderbook"},
]


def _arrow_type_to_str(arrow_type) -> str:
    """Convert Arrow type to human-readable string for schema JSON."""
    return str(arrow_type)


def _schema_to_json(schema) -> dict:
    """Serialize an Arrow schema to a JSON-friendly dict."""
    return {
        "fields": [
            {"name": field.name, "type": _arrow_type_to_str(field.type), "nullable": field.nullable} for field in schema
        ]
    }


async def seed_datasets(session: AsyncSession, dry_run: bool = False) -> int:
    """Seed the datasets table with all Silver feeds."""
    storage_root = str(settings.silver_path)
    count = 0

    for feed_name in SILVER_SCHEMAS:
        existing = await session.execute(select(Dataset).where(Dataset.dataset_name == feed_name))
        dataset = existing.scalar_one_or_none()

        description = DATASET_DESCRIPTIONS.get(feed_name, f"Silver dataset: {feed_name}")

        if dataset:
            dataset.description = description
            dataset.storage_root = storage_root
            dataset.is_active = True
            dataset.updated_at = datetime.now(UTC)
            logger.debug("dataset_updated", name=feed_name)
        else:
            dataset = Dataset(
                dataset_name=feed_name,
                layer="silver",
                owner="shared",
                description=description,
                storage_root=storage_root,
                path_template="feed={feed}/instrument_type={instrument_type}/dt={dt}",
                partition_cols=["feed", "instrument_type", "dt"],
                primary_keys=["event_id"],
                is_active=True,
            )
            session.add(dataset)
            logger.debug("dataset_created", name=feed_name)

        count += 1

    if not dry_run:
        await session.commit()

    logger.info("datasets_seeded", count=count, dry_run=dry_run)
    return count


async def seed_schema_versions(session: AsyncSession, dry_run: bool = False) -> int:
    """Seed dataset_versions with v1 schemas for all Silver feeds."""
    count = 0

    for feed_name, schema in SILVER_SCHEMAS.items():
        schema_json = _schema_to_json(schema)

        existing = await session.execute(
            select(DatasetVersion)
            .where(DatasetVersion.dataset_name == feed_name)
            .where(DatasetVersion.schema_version == "v1")
        )
        version = existing.scalar_one_or_none()

        if version:
            version.schema_json = schema_json
            version.is_current = True
            logger.debug("schema_version_updated", dataset=feed_name, version="v1")
        else:
            version = DatasetVersion(
                dataset_name=feed_name,
                schema_version="v1",
                schema_json=schema_json,
                is_current=True,
            )
            session.add(version)
            logger.debug("schema_version_created", dataset=feed_name, version="v1")

        count += 1

    if not dry_run:
        await session.commit()

    logger.info("schema_versions_seeded", count=count, dry_run=dry_run)
    return count


async def seed_feed_mappings(session: AsyncSession, dry_run: bool = False) -> int:
    """Seed feed_mappings with provider→gateway_feed→silver_dataset entries."""
    count = 0

    for mapping in FEED_MAPPING_SEEDS:
        existing = await session.execute(
            select(FeedMapping)
            .where(FeedMapping.provider == mapping["provider"])
            .where(FeedMapping.gateway_feed == mapping["gateway_feed"])
        )
        feed_map = existing.scalar_one_or_none()

        if feed_map:
            feed_map.silver_dataset_name = mapping["silver_dataset_name"]
            logger.debug("feed_mapping_updated", **mapping)
        else:
            feed_map = FeedMapping(**mapping)
            session.add(feed_map)
            logger.debug("feed_mapping_created", **mapping)

        count += 1

    if not dry_run:
        await session.commit()

    logger.info("feed_mappings_seeded", count=count, dry_run=dry_run)
    return count


async def discover_datasets_from_disk(session: AsyncSession) -> int:
    """Scan Silver directory and auto-register unknown datasets and feed mappings.

    Walks ``settings.silver_path`` for Hive-style ``feed=X`` directories. Any
    feed found on disk that is not already registered in the ``datasets`` table
    gets a new Dataset row (sensible defaults) and an identity FeedMapping
    (``provider="discovered"``).

    Returns the number of newly registered datasets.
    """
    silver_root = settings.silver_path
    if not silver_root.exists():
        logger.warning("auto_discover_skipped", reason="silver_path_not_found", path=str(silver_root))
        return 0

    # Collect feed names present on disk
    disk_feeds: set[str] = set()
    for entry in silver_root.iterdir():
        # Skip macOS resource fork files and non-directories
        if entry.name.startswith(".") or entry.name.startswith("._"):
            continue
        if entry.is_dir() and entry.name.startswith("feed="):
            disk_feeds.add(entry.name.split("=", 1)[1])

    if not disk_feeds:
        logger.info("auto_discover_no_feeds_on_disk", path=str(silver_root))
        return 0

    # Find what's already registered in the DB
    existing_result = await session.execute(select(Dataset.dataset_name))
    known_datasets: set[str] = {row[0] for row in existing_result.all()}

    new_feeds = disk_feeds - known_datasets
    if not new_feeds:
        logger.info("auto_discover_complete", disk_feeds=len(disk_feeds), new=0)
        return 0

    # Register each unknown feed
    count = 0
    for feed_name in sorted(new_feeds):
        # Create Dataset
        dataset = Dataset(
            dataset_name=feed_name,
            layer="silver",
            owner="shared",
            description=f"Auto-discovered Silver dataset: {feed_name}",
            storage_root=str(silver_root),
            path_template="feed={feed}/instrument_type={instrument_type}/dt={dt}",
            partition_cols=["feed", "instrument_type", "dt"],
            primary_keys=["event_id"],
            is_active=True,
        )
        session.add(dataset)

        # Create default identity FeedMapping so resolve_feed can find it
        feed_map = FeedMapping(
            provider="discovered",
            gateway_feed=feed_name,
            silver_dataset_name=feed_name,
        )
        session.add(feed_map)

        logger.info("auto_discovered_dataset", dataset=feed_name)
        count += 1

    await session.commit()
    logger.info("auto_discover_complete", disk_feeds=len(disk_feeds), new=count)
    return count


def _scan_partition_dates(feed_dir: Path) -> tuple[str, str, int] | None:
    """Scan a feed directory for dt= partitions and return (min_date, max_date, count)."""
    dates: list[str] = []
    for sub in feed_dir.rglob("dt=*"):
        if not sub.is_dir():
            continue
        match = _DATE_PARTITION_RE.match(sub.name)
        if match:
            dates.append(match.group(1))
    if not dates:
        return None
    return min(dates), max(dates), len(dates)


async def seed_coverage_from_disk(session: AsyncSession) -> int:
    """Scan Silver partition directories and populate data_coverage with date ranges.

    Walks ``settings.silver_path`` for ``feed={name}`` directories, then scans
    each for ``dt=YYYY-MM-DD`` subdirectories (at any nesting depth below the feed).
    For each feed, upserts a single aggregate DataCoverage record with
    ``instrument_key="__all__"`` containing the observed min/max dates.

    Returns the number of coverage records upserted.
    """
    silver_root = settings.silver_path
    if not silver_root.exists():
        logger.warning("coverage_scan_skipped", reason="silver_path_not_found", path=str(silver_root))
        return 0

    upserted = 0
    for entry in sorted(silver_root.iterdir()):
        if not entry.is_dir() or entry.name.startswith(".") or not entry.name.startswith("feed="):
            continue

        feed_name = entry.name.split("=", 1)[1]
        scan = _scan_partition_dates(entry)
        if scan is None:
            continue

        dt_min_str, dt_max_str, partition_count = scan
        upserted += await _upsert_coverage(
            session,
            feed_name,
            dt_min_str,
            dt_max_str,
            partition_count,
        )

    if upserted:
        await session.commit()

    logger.info("coverage_seeded_from_disk", upserted=upserted)
    return upserted


async def _upsert_coverage(
    session: AsyncSession,
    feed_name: str,
    dt_min_str: str,
    dt_max_str: str,
    partition_count: int,
) -> int:
    """Upsert a single aggregate DataCoverage record for a feed."""
    dt_min = date_type.fromisoformat(dt_min_str)
    dt_max = date_type.fromisoformat(dt_max_str)

    existing = await session.execute(
        select(DataCoverage)
        .where(DataCoverage.dataset_name == feed_name)
        .where(DataCoverage.instrument_key == "__all__")
    )
    coverage = existing.scalar_one_or_none()

    if coverage:
        coverage.dt_min = dt_min
        coverage.dt_max = dt_max
        coverage.approx_row_count = partition_count
        coverage.last_updated_ts = datetime.now(UTC)
    else:
        coverage = DataCoverage(
            dataset_name=feed_name,
            instrument_key="__all__",
            dt_min=dt_min,
            dt_max=dt_max,
            approx_row_count=partition_count,
        )
        session.add(coverage)

    logger.debug(
        "coverage_upserted",
        dataset=feed_name,
        dt_min=dt_min_str,
        dt_max=dt_max_str,
        partitions=partition_count,
    )
    return 1
