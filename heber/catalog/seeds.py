"""Catalog seed data and functions for datasets, schema versions, and feed mappings.

Idempotent — safe to re-run on every startup. Uses upsert patterns so existing rows
are updated rather than duplicated.

Extracted from scripts/seed_catalog.py so the catalog lifespan can import cleanly.
"""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from datetime import date as date_type
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from sqlalchemy import delete, select
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
    # Company financial statements (quarterly + annual, 20+ year history)
    "income_statement": "Company income statements by fiscal period from Unusual Whales",
    "balance_sheet": "Company balance sheets by fiscal period from Unusual Whales",
    "cash_flow": "Company cash flow statements by fiscal period from Unusual Whales",
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
    {"provider": "alpaca", "gateway_feed": "option_chain_snapshot", "silver_dataset_name": "option_chain_snapshot"},
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
    # Unusual Whales — Company financial statements (backfill driver emits snake_case;
    # hyphenated REST-path names alias to the same Silver dataset via FEED_ALIASES).
    {"provider": "unusual_whales", "gateway_feed": "income_statement", "silver_dataset_name": "income_statement"},
    {"provider": "unusual_whales", "gateway_feed": "income-statement", "silver_dataset_name": "income_statement"},
    {"provider": "unusual_whales", "gateway_feed": "balance_sheet", "silver_dataset_name": "balance_sheet"},
    {"provider": "unusual_whales", "gateway_feed": "balance-sheet", "silver_dataset_name": "balance_sheet"},
    {"provider": "unusual_whales", "gateway_feed": "cash_flow", "silver_dataset_name": "cash_flow"},
    {"provider": "unusual_whales", "gateway_feed": "cash-flow", "silver_dataset_name": "cash_flow"},
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


def _arrow_type_to_str(arrow_type: pa.DataType) -> str:
    """Convert Arrow type to human-readable string for schema JSON."""
    return str(arrow_type)


def _schema_to_json(schema: pa.Schema) -> dict[str, list[dict[str, str | bool]]]:
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


def _scan_silver_feeds_blocking(silver_root: Path) -> set[str]:
    """Blocking helper to scan for feed directories."""
    if not silver_root.exists():
        return set()

    disk_feeds: set[str] = set()
    try:
        for entry in silver_root.iterdir():
            # Skip macOS resource fork files and non-directories
            if entry.name.startswith(".") or entry.name.startswith("._"):
                continue
            if entry.is_dir() and entry.name.startswith("feed="):
                disk_feeds.add(entry.name.split("=", 1)[1])
    except OSError:
        logger.warning("catalog_feed_scan_oserror", path=str(silver_root), exc_info=True)
    return disk_feeds


async def discover_datasets_from_disk(session: AsyncSession) -> int:
    """Scan Silver directory and auto-register unknown datasets and feed mappings.

    Walks ``settings.silver_path`` for Hive-style ``feed=X`` directories. Any
    feed found on disk that is not already registered in the ``datasets`` table
    gets a new Dataset row (sensible defaults) and an identity FeedMapping
    (``provider="discovered"``).

    Returns the number of newly registered datasets.
    """
    silver_root = settings.silver_path

    # Run blocking I/O in thread
    if not await asyncio.to_thread(lambda: silver_root.exists()):
        logger.warning("auto_discover_skipped", reason="silver_path_not_found", path=str(silver_root))
        return 0

    # Collect feed names present on disk (in thread)
    disk_feeds = await asyncio.to_thread(_scan_silver_feeds_blocking, silver_root)

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


class _DirectoryListing(NamedTuple):
    """What one directory contributed to the partition walk."""

    subdirs: list[tuple[Path, str | None]]
    files: list[tuple[str, Path]]
    mtimes: list[tuple[str, float]]


class _FeedWalk(NamedTuple):
    """What a whole feed's walk found."""

    files_by_date: dict[str, list[Path]]
    mtime_by_date: dict[str, float]
    dirs_scanned: int


def _list_one_directory(directory: Path, date_str: str | None) -> _DirectoryListing:
    """List a single directory for the partition walk.

    ``date_str`` is the ``dt=`` partition this directory sits under, carried
    down so files inside ``hour=`` sub-partitions are attributed to their date.

    Also reports each subdirectory's mtime against the date it belongs to, so
    the caller can tell whether a partition has changed since it was last
    counted. Zero-byte Parquet files are skipped by size rather than opened:
    pyarrow raises ``ArrowInvalid: Parquet file size is 0 bytes`` on them, and
    the compactor and ``HeberReader`` already filter them the same way.
    """
    subdirs: list[tuple[Path, str | None]] = []
    files: list[tuple[str, Path]] = []
    mtimes: list[tuple[str, float]] = []

    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                # Skips macOS AppleDouble sidecars, both the ._ files and the
                # ._ directories whose contents must not be counted.
                if entry.name.startswith("."):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    match = _DATE_PARTITION_RE.match(entry.name)
                    child_date = match.group(1) if match else date_str
                    subdirs.append((Path(entry.path), child_date))
                    if child_date is not None:
                        try:
                            mtimes.append((child_date, entry.stat().st_mtime))
                        except OSError:
                            # Unknown mtime must never look unchanged.
                            mtimes.append((child_date, float("inf")))
                elif date_str is not None and entry.name.endswith(".parquet"):
                    # Deliberately no stat() here. Zero-byte files are filtered
                    # at read time instead: this ran on every file on every
                    # pass, including the ones a reuse pass never opens, and on
                    # the lakehouse mount a per-file stat costs 4.24ms against
                    # 0.15ms for the scandir entry itself — 29x, measured. With
                    # parallelism per directory, one leaf holding ~800 files
                    # serialised 800 stats while occupying a worker, which is
                    # why feed=quotes (~825k files) never once finished a walk.
                    files.append((date_str, Path(entry.path)))
    except OSError as exc:
        logger.warning("catalog_partition_scan_oserror", path=str(directory), error=str(exc)[:200])

    return _DirectoryListing(subdirs, files, mtimes)


def _walk_partition_files(feed_dir: Path, pool: ThreadPoolExecutor) -> _FeedWalk:
    """Walk a feed directory, mapping each dt= date to the Parquet files under it.

    Each level of the tree is listed concurrently, because the cost here is
    per-open latency on the lakehouse mount rather than CPU.
    """
    files_by_date: dict[str, list[Path]] = {}
    mtime_by_date: dict[str, float] = {}
    dirs_scanned = 0

    level: list[tuple[Path, str | None]] = [(feed_dir, None)]
    while level:
        dirs_scanned += len(level)
        next_level: list[tuple[Path, str | None]] = []
        for listing in pool.map(lambda item: _list_one_directory(*item), level):
            next_level.extend(listing.subdirs)
            for date_str, path in listing.files:
                files_by_date.setdefault(date_str, []).append(path)
            for date_str, mtime in listing.mtimes:
                if mtime > mtime_by_date.get(date_str, 0.0):
                    mtime_by_date[date_str] = mtime
        level = next_level

    return _FeedWalk(files_by_date, mtime_by_date, dirs_scanned)


def _read_row_count(path: Path) -> int | None:
    """Row count from a Parquet footer, or None if the footer cannot be read.

    None rather than 0 so an unreadable file is distinguishable from an empty
    one: both contribute no rows, but only one of them means coverage is now
    undercounting.

    The zero-byte check lives here rather than in the walk. A file of zero bytes
    is a write that never landed, not corruption, and pyarrow raises on it —
    checking here keeps the two apart while paying the stat only for files
    being opened anyway, where 4ms disappears beside a 106ms footer read.
    """
    try:
        if path.stat().st_size == 0:
            return 0
    except OSError:
        # Size unknown is not the same as empty; let the footer read say why.
        pass
    try:
        return int(pq.read_metadata(path).num_rows)
    except Exception as exc:
        # No traceback: a corrupt file is expected debris, and rendering a
        # stack into every JSON log line cost more than reading the footer.
        logger.warning("catalog_parquet_metadata_unreadable", path=str(path), error=str(exc)[:200])
        return None


# A partition is only treated as unchanged if it is older than the recorded
# count by this margin. The lakehouse mount's mtimes were measured running
# ~0.5s ahead of the container clock, and exFAT stores local time, so the two
# clocks are close but not identical. Without a margin, a file landing in the
# same moment a directory was walked could read as older than the pass that
# missed it and be skipped from then on — a permanent silent undercount. The
# cost of being wrong in the other direction is re-reading one partition.
_MTIME_SKEW_MARGIN_SECONDS = 60.0


def _counted_before(recorded_ts: datetime) -> float:
    """Epoch cutoff below which a partition counts as unchanged since ``recorded_ts``."""
    if recorded_ts.tzinfo is None:
        recorded_ts = recorded_ts.replace(tzinfo=UTC)
    return recorded_ts.timestamp() - _MTIME_SKEW_MARGIN_SECONDS


def _scan_partition_dates(
    feed_dir: Path,
    recorded: dict[str, tuple[datetime, int]] | None = None,
) -> list[tuple[str, int]] | None:
    """Scan a feed directory for dt= partitions and return per-date row counts.

    Returns a list of (date_str, row_count) tuples — one per date found, summed
    across every ``instrument_type=`` and ``hour=`` partition holding that date.
    Row counts come from Parquet footers; no data is loaded.

    The lakehouse sits on an exFAT bind mount where each directory open costs
    ~28ms and each footer read ~106ms, so this walk — not the database write —
    is the whole cost of a coverage pass. Both are latency, not bandwidth, so
    directories are listed a level at a time in parallel and footers are read
    in parallel.

    ``recorded`` maps a date to the (timestamp, row count) already in
    ``data_coverage``. A date whose directories have not been modified since
    that timestamp holds the same files and therefore the same rows, so its
    footers are not read again. This is what keeps the pass proportional to
    what changed: ``feed=quotes`` alone holds ~825k Parquet files, and reading
    every footer every five minutes cost ~2h12m of a ~2h30m pass. Nothing in
    Heber rewrites a Parquet file in place — Silver writes new part files and
    the compactor writes a temp file then renames — so a directory's mtime is
    a sound signal that its contents changed.
    """
    started = time.monotonic()

    workers = settings.catalog_coverage_scan_workers
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="coverage-scan") as pool:
        walk = _walk_partition_files(feed_dir, pool)

        totals: dict[str, int] = {}
        reused = 0
        to_read: list[tuple[str, Path]] = []
        for date_str, paths in walk.files_by_date.items():
            prior = (recorded or {}).get(date_str)
            if prior is not None and walk.mtime_by_date.get(date_str, float("inf")) < _counted_before(prior[0]):
                totals[date_str] = prior[1]
                reused += 1
                continue
            to_read.extend((date_str, path) for path in paths)

        counts = list(pool.map(_read_row_count, [path for _, path in to_read]))

    unreadable = 0
    skipped_empty = 0
    for (date_str, _), rows in zip(to_read, counts, strict=True):
        if rows is None:
            unreadable += 1
            continue
        if rows == 0:
            # A write that never landed, or a genuinely empty part file. Benign
            # either way, and deliberately not counted as unreadable.
            skipped_empty += 1
        totals[date_str] = totals.get(date_str, 0) + rows

    results = [(date_str, rows) for date_str, rows in sorted(totals.items()) if rows > 0]

    # The reuse decision assumes this container's clock and the mount's agree to
    # within _MTIME_SKEW_MARGIN_SECONDS — measured at ~0.5s. Drift in the
    # dangerous direction (this clock ahead of the filesystem's) would silently
    # skip partitions, so the newest mtime seen is reported rather than assumed:
    # a value going sharply negative means the margin no longer covers reality.
    newest_mtime = max(walk.mtime_by_date.values(), default=None)

    logger.info(
        "coverage_feed_scanned",
        feed=feed_dir.name.removeprefix("feed="),
        dates=len(results),
        files=len(to_read),
        dirs=walk.dirs_scanned,
        reused_dates=reused,
        skipped_empty=skipped_empty,
        unreadable=unreadable,
        newest_mtime_age_seconds=round(time.time() - newest_mtime, 1) if newest_mtime else None,
        elapsed_seconds=round(time.monotonic() - started, 2),
    )

    return results or None


async def _load_recorded_coverage(session: AsyncSession) -> dict[str, dict[str, tuple[datetime, int]]]:
    """Per-date coverage already recorded, as {feed: {date: (counted_at, rows)}}.

    One query. This is what lets a pass skip partitions nothing has touched
    since they were last counted.
    """
    result = await session.execute(
        select(
            DataCoverage.dataset_name,
            DataCoverage.instrument_key,
            DataCoverage.last_updated_ts,
            DataCoverage.approx_row_count,
        ).where(DataCoverage.instrument_key.startswith("dt:"))
    )

    recorded: dict[str, dict[str, tuple[datetime, int]]] = {}
    for dataset_name, instrument_key, last_updated_ts, row_count in result.all():
        # Only per-date rows this scan writes may seed the skip decision.
        # `CatalogService.update_coverage` and backfill also write coverage,
        # keyed by real instrument keys and stamped when they were written; a
        # write-time stamp reaching the reuse path could freeze a date short.
        if not instrument_key.startswith("dt:"):
            continue
        if last_updated_ts is None or row_count is None:
            continue
        recorded.setdefault(dataset_name, {})[instrument_key.removeprefix("dt:")] = (last_updated_ts, row_count)

    # Close the read's transaction before returning. The walk that follows runs
    # for minutes, and leaving this SELECT's transaction open across it would
    # reintroduce the idle-in-transaction timeout that froze coverage for 24
    # days — the walk must hold no transaction at all.
    await session.rollback()
    return recorded


def _present_feed_names() -> set[str]:
    """Feed names that currently have a directory under Silver."""
    return {
        e.name.split("=", 1)[1]
        for e in settings.silver_path.iterdir()
        if e.name.startswith("feed=") and not e.name.startswith(".")
    }


async def _prune_vanished_feeds(session: AsyncSession, recorded_feeds: set[str]) -> bool:
    """Drop coverage for feeds with no directory left. Returns whether anything was pruned.

    Coverage is derived entirely from disk and rebuilt every pass, so a row for
    a feed that has no directory is stale metadata the catalog is presenting as
    fact — `bars_1m`, `data` and `stocks` sat there for months after the feeds
    were gone. It also makes per-feed staleness unusable, because the oldest
    coverage is then always a decommissioned feed.

    The listing is taken fresh rather than reusing the walk, so an interrupted
    pass cannot mistake feeds it never reached for feeds that vanished. An empty
    or failed listing prunes nothing: that is a missing mount, not a decommission.
    """
    if not recorded_feeds:
        return False
    try:
        present = await asyncio.to_thread(_present_feed_names)
    except OSError:
        logger.warning("coverage_prune_skipped", reason="feed_listing_failed", exc_info=True)
        return False
    if not present:
        logger.warning("coverage_prune_skipped", reason="empty_feed_listing", recorded=len(recorded_feeds))
        return False

    vanished = sorted(recorded_feeds - present)
    if not vanished:
        return False

    await session.execute(delete(DataCoverage).where(DataCoverage.dataset_name.in_(vanished)))
    await session.commit()
    logger.info("coverage_pruned_vanished_feeds", feeds=vanished, count=len(vanished))
    return True


async def seed_coverage_from_disk(session: AsyncSession, reuse_recorded: bool = True) -> int:
    """Scan Silver partition directories and populate data_coverage with accurate row counts.

    Walks ``settings.silver_path`` for ``feed={name}`` directories, then scans
    each for ``dt=YYYY-MM-DD`` subdirectories (at any nesting depth below the feed).

    For each feed, creates:
    - One aggregate ``__all__`` record with dt_min, dt_max, and total row count
    - One per-date ``dt:YYYY-MM-DD`` record so coverage queries return individual dates

    Row counts are read from Parquet file metadata (footer only, no data loaded).

    Returns the number of coverage records upserted.
    """
    silver_root = settings.silver_path
    if not await asyncio.to_thread(lambda: silver_root.exists()):
        logger.warning("coverage_scan_skipped", reason="silver_path_not_found", path=str(silver_root))
        return 0

    # Phase 1 — walk everything first, touching no session.
    #
    # This used to alternate: scan a feed, upsert its rows, scan the next. The
    # transaction opened by the first upsert then sat idle for the length of
    # every later scan, and Postgres here runs
    # idle_in_transaction_session_timeout = 5min. 232 of 235 discovery passes
    # failed in one container lifetime, always at an upsert, leaving
    # data_coverage frozen since 2026-07-20. Collecting first means no
    # transaction is open while the slow work happens.
    # Stamped onto every row this pass writes, and compared against partition
    # mtimes on the next pass. It must be the moment the pass *started*: rows
    # are written minutes later, and a file that landed mid-pass would then
    # look older than its own coverage row and be skipped from then on.
    scan_started_at = datetime.now(UTC)

    # ``reuse_recorded=False`` counts every partition from its footers, ignoring
    # what is already recorded. The first pass of a process always does this.
    #
    # Reuse is only sound against timestamps that mean "counted as of", and rows
    # written before this scheme existed were stamped when they were *written* —
    # for a pass that walked for hours and wrote at the end, that stamp sits
    # long after the files it counted. Trusting one would permanently skip any
    # partition whose last write landed mid-pass. Recounting once per process
    # also bounds the damage of any future mistake in the mtime comparison to a
    # single container lifetime, rather than letting a wrong count persist.
    recorded = await _load_recorded_coverage(session) if reuse_recorded else {}

    entries = await asyncio.to_thread(lambda: sorted(silver_root.iterdir()))
    manifest: list[tuple[str, list[tuple[str, int]]]] = []
    failed_feeds = 0
    upserted = 0

    for entry in entries:
        # String checks first to avoid stat() on macOS AppleDouble resource fork files
        if entry.name.startswith(".") or not entry.name.startswith("feed="):
            continue
        try:
            if not entry.is_dir():
                continue
        except OSError:
            logger.debug("coverage_scan_skip_stat", path=str(entry), exc_info=True)
            continue

        # Per feed, for the same reason Phase 2 commits per feed: one feed that
        # cannot be walked must not discard every feed behind it and leave
        # coverage frozen until the staleness alarm fires.
        feed_name = entry.name.split("=", 1)[1]
        try:
            scan_results = await asyncio.to_thread(_scan_partition_dates, entry, recorded.get(feed_name))
        except Exception as exc:
            failed_feeds += 1
            logger.warning("coverage_feed_scan_failed", feed=entry.name, error=str(exc)[:200], exc_info=True)
            continue

        if scan_results is None:
            continue
        manifest.append((feed_name, scan_results))
        upserted += await _write_feed_coverage(session, feed_name, scan_results, scan_started_at)

    logger.info("coverage_scan_complete", feeds=len(manifest), failed_feeds=failed_feeds, upserted=upserted)

    # Only after a pass that walked everything: a run cut short partway through
    # has not seen the feeds it never reached, and must not read them as gone.
    if not failed_feeds:
        await _prune_vanished_feeds(session, await _recorded_feed_names(session))

    logger.info("coverage_seeded_from_disk", upserted=upserted)
    return upserted


async def _recorded_feed_names(session: AsyncSession) -> set[str]:
    """Feed names that currently hold coverage rows."""
    result = await session.execute(select(DataCoverage.dataset_name).distinct())
    return {name for (name,) in result.all()}


async def _write_feed_coverage(
    session: AsyncSession,
    feed_name: str,
    scan_results: list[tuple[str, int]],
    scan_started_at: datetime,
) -> int:
    """Write one feed's coverage and commit. Returns rows upserted.

    Called as each feed finishes walking rather than after all of them. The
    walk is the entire cost of a pass — ``feed=quotes`` alone is hours on this
    mount — and deferring every write until the last feed meant a pass that
    could not finish published nothing at all. Coverage sat 14h stale through a
    scan that was working the whole time, and each restart began again at the
    first feed, so the feeds late in the alphabet were never reached.
    Publishing per feed makes progress durable and survives a restart.

    The commit is what keeps the transaction short. It is opened by the first
    upsert here and closed immediately, so no transaction is ever held open
    across the next feed's walk — the condition that failed 232 of 235 passes
    in a single container lifetime against a 5-minute
    ``idle_in_transaction_session_timeout``.
    """
    all_dates = [d for d, _ in scan_results]
    upserted = await _upsert_coverage(
        session,
        feed_name,
        instrument_key="__all__",
        dt_min_str=min(all_dates),
        dt_max_str=max(all_dates),
        row_count=sum(r for _, r in scan_results),
        counted_at=scan_started_at,
    )
    for date_str, row_count in scan_results:
        upserted += await _upsert_coverage(
            session,
            feed_name,
            instrument_key=f"dt:{date_str}",
            dt_min_str=date_str,
            dt_max_str=date_str,
            row_count=row_count,
            counted_at=scan_started_at,
        )
    await session.commit()
    return upserted


async def _upsert_coverage(
    session: AsyncSession,
    feed_name: str,
    instrument_key: str,
    dt_min_str: str,
    dt_max_str: str,
    row_count: int,
    counted_at: datetime,
) -> int:
    """Upsert a DataCoverage record for a feed.

    ``counted_at`` is the moment the pass began, not the moment of the write:
    the next pass compares partition mtimes against it to decide what it can
    skip, and a write-time stamp would sit after files that landed mid-pass.
    """
    dt_min = date_type.fromisoformat(dt_min_str)
    dt_max = date_type.fromisoformat(dt_max_str)

    existing = await session.execute(
        select(DataCoverage)
        .where(DataCoverage.dataset_name == feed_name)
        .where(DataCoverage.instrument_key == instrument_key)
    )
    coverage = existing.scalar_one_or_none()

    if coverage:
        # Last write wins, deliberately, even when this pass started earlier
        # than the row it replaces. A refusal to overwrite a newer timestamp
        # would silently neuter the verification pass: the five-minute refresh
        # restamps rows throughout verification's multi-hour walk, so every one
        # of its writes would be skipped and no legacy count would ever be
        # corrected. It is also backwards — verification counts by reading
        # footers, while the refresh carries a recorded count forward, so the
        # older-stamped value is the more accurate one.
        #
        # Writing an older stamp is self-correcting: reuse only applies when a
        # partition's mtime predates the stamp, so a partition that changed
        # since is simply recounted on the next pass.
        coverage.dt_min = dt_min
        coverage.dt_max = dt_max
        coverage.approx_row_count = row_count
        coverage.last_updated_ts = counted_at
    else:
        coverage = DataCoverage(
            dataset_name=feed_name,
            instrument_key=instrument_key,
            dt_min=dt_min,
            dt_max=dt_max,
            approx_row_count=row_count,
            last_updated_ts=counted_at,
        )
        session.add(coverage)

    logger.debug(
        "coverage_upserted",
        dataset=feed_name,
        instrument_key=instrument_key,
        dt_min=dt_min_str,
        dt_max=dt_max_str,
        rows=row_count,
    )
    return 1
