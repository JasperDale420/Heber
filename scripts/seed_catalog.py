"""Seed the Heber Catalog DB with all 44 Silver datasets, feed mappings, and coverage.

Idempotent — safe to re-run. Uses upsert patterns so existing rows are updated.

Usage:
    python scripts/seed_catalog.py              # seed datasets + feed_mappings + schema versions
    python scripts/seed_catalog.py --scan       # also scan Silver parquet for data_coverage
    python scripts/seed_catalog.py --dry-run    # preview without writing to DB
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from heber.catalog.db import Base, DataCoverage, Dataset, DatasetVersion, FeedMapping
from heber.config import settings
from heber.schemas.silver import SILVER_SCHEMAS

logger = structlog.get_logger(__name__)

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
    # Unusual Whales — Options flow
    {"provider": "unusual_whales", "gateway_feed": "flow_alerts", "silver_dataset_name": "flow_alerts"},
    {"provider": "unusual_whales", "gateway_feed": "darkpool", "silver_dataset_name": "darkpool"},
    {"provider": "unusual_whales", "gateway_feed": "hottest_chains", "silver_dataset_name": "hottest_chain"},
    # Unusual Whales — Sentiment
    {"provider": "unusual_whales", "gateway_feed": "sector_tide", "silver_dataset_name": "sector_tide"},
    {"provider": "unusual_whales", "gateway_feed": "market_tide", "silver_dataset_name": "market_tide"},
    # Unusual Whales — Analytics
    {"provider": "unusual_whales", "gateway_feed": "greek_exposure", "silver_dataset_name": "greek_exposure"},
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
    {"provider": "unusual_whales", "gateway_feed": "ftd", "silver_dataset_name": "ftd"},
    # Unusual Whales — Alternative Data
    {"provider": "unusual_whales", "gateway_feed": "congress_trades", "silver_dataset_name": "congress_trades"},
    {"provider": "unusual_whales", "gateway_feed": "insider_trades", "silver_dataset_name": "insider_trades"},
    {"provider": "unusual_whales", "gateway_feed": "insider_flow", "silver_dataset_name": "insider_flow"},
    {
        "provider": "unusual_whales",
        "gateway_feed": "institution_holdings",
        "silver_dataset_name": "institution_holdings",
    },
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
    """Seed the datasets table with all 44 Silver feeds."""
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


def _collect_coverage_from_disk(silver_root: Path) -> dict[tuple[str, str], dict]:
    """Walk Silver partition directories and aggregate coverage stats."""
    coverage_map: dict[tuple[str, str], dict] = {}

    for feed_dir in silver_root.iterdir():
        if not feed_dir.is_dir() or not feed_dir.name.startswith("feed="):
            continue
        feed_name = feed_dir.name.split("=", 1)[1]
        _scan_feed_directory(feed_name, feed_dir, coverage_map)

    return coverage_map


def _scan_feed_directory(feed_name: str, feed_dir: Path, coverage_map: dict[tuple[str, str], dict]) -> None:
    """Scan instrument_type and dt partitions within a feed directory."""
    for instrument_dir in feed_dir.iterdir():
        if not instrument_dir.is_dir() or not instrument_dir.name.startswith("instrument_type="):
            continue

        instrument_type = instrument_dir.name.split("=", 1)[1]
        instrument_key = f"{instrument_type}:*"

        for dt_dir in instrument_dir.iterdir():
            if not dt_dir.is_dir() or not dt_dir.name.startswith("dt="):
                continue

            dt_str = dt_dir.name.split("=", 1)[1]
            row_count = _count_parquet_rows(dt_dir)
            key = (feed_name, instrument_key)

            if key not in coverage_map:
                coverage_map[key] = {"dt_min": dt_str, "dt_max": dt_str, "row_count": row_count}
            else:
                entry = coverage_map[key]
                entry["dt_min"] = min(entry["dt_min"], dt_str)
                entry["dt_max"] = max(entry["dt_max"], dt_str)
                entry["row_count"] += row_count


def _count_parquet_rows(dt_dir: Path) -> int:
    """Sum row counts from parquet file metadata in a date partition."""
    total = 0
    for pf in dt_dir.glob("*.parquet"):
        try:
            meta = pq.read_metadata(pf)
            total += meta.num_rows
        except Exception:
            continue
    return total


async def scan_coverage(session: AsyncSession, dry_run: bool = False) -> int:
    """Scan Silver parquet files and populate data_coverage table."""
    silver_root = settings.silver_path
    if not silver_root.exists():
        logger.warning("silver_path_not_found", path=str(silver_root))
        return 0

    coverage_map = _collect_coverage_from_disk(silver_root)
    count = 0

    for (feed_name, instrument_key), entry in coverage_map.items():
        existing = await session.execute(
            select(DataCoverage)
            .where(DataCoverage.dataset_name == feed_name)
            .where(DataCoverage.instrument_key == instrument_key)
        )
        coverage = existing.scalar_one_or_none()

        dt_min = datetime.strptime(entry["dt_min"], "%Y-%m-%d")
        dt_max = datetime.strptime(entry["dt_max"], "%Y-%m-%d")

        if coverage:
            coverage.dt_min = dt_min
            coverage.dt_max = dt_max
            coverage.approx_row_count = entry["row_count"]
            coverage.last_updated_ts = datetime.now(UTC)
        else:
            coverage = DataCoverage(
                dataset_name=feed_name,
                instrument_key=instrument_key,
                dt_min=dt_min,
                dt_max=dt_max,
                approx_row_count=entry["row_count"],
            )
            session.add(coverage)

        count += 1

    if not dry_run:
        await session.commit()

    logger.info("data_coverage_seeded", entries=count, dry_run=dry_run)
    return count


async def main(scan: bool = False, dry_run: bool = False) -> None:
    """Run all seed operations."""
    engine = create_async_engine(settings.postgres_url, echo=False)
    async_session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Ensure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session_factory() as session:
        datasets = await seed_datasets(session, dry_run=dry_run)
        versions = await seed_schema_versions(session, dry_run=dry_run)
        mappings = await seed_feed_mappings(session, dry_run=dry_run)

        coverage = 0
        if scan:
            coverage = await scan_coverage(session, dry_run=dry_run)

    await engine.dispose()

    print(f"\nCatalog seed complete {'(DRY RUN)' if dry_run else ''}:")
    print(f"  Datasets:         {datasets}")
    print(f"  Schema versions:  {versions}")
    print(f"  Feed mappings:    {mappings}")
    if scan:
        print(f"  Coverage entries: {coverage}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the Heber Catalog database")
    parser.add_argument("--scan", action="store_true", help="Scan Silver parquet files for data coverage")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
    args = parser.parse_args()

    asyncio.run(main(scan=args.scan, dry_run=args.dry_run))
