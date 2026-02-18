"""Seed the Heber Catalog DB with all Silver datasets, feed mappings, and coverage.

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

from heber.catalog.db import Base, DataCoverage
from heber.catalog.seeds import (
    discover_datasets_from_disk,
    seed_datasets,
    seed_feed_mappings,
    seed_schema_versions,
)
from heber.config import settings

logger = structlog.get_logger(__name__)


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


async def main(scan: bool = False, discover: bool = False, dry_run: bool = False) -> None:
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

    discovered = 0
    if discover:
        async with async_session_factory() as session:
            discovered = await discover_datasets_from_disk(session)

    await engine.dispose()

    print(f"\nCatalog seed complete {'(DRY RUN)' if dry_run else ''}:")
    print(f"  Datasets:         {datasets}")
    print(f"  Schema versions:  {versions}")
    print(f"  Feed mappings:    {mappings}")
    if scan:
        print(f"  Coverage entries: {coverage}")
    if discover:
        print(f"  Discovered:       {discovered}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the Heber Catalog database")
    parser.add_argument("--scan", action="store_true", help="Scan Silver parquet files for data coverage")
    parser.add_argument("--discover", action="store_true", help="Auto-discover datasets from Silver directory")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
    args = parser.parse_args()

    asyncio.run(main(scan=args.scan, discover=args.discover, dry_run=args.dry_run))
