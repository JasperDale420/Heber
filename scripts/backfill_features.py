"""Backfill Gold meta_label_features for dates that have outcomes but no features.

Reads Silver flow_alerts, reconstructs FlowAlertRecord objects, runs them through
AlertFeatureExtractor.extract(), and persists results to Gold.

Usage:
    # Dry run (report only):
    uv run python scripts/backfill_features.py

    # With Data Gateway enrichment (full features):
    HEBER_WATCH_GATEWAY_URL=http://localhost:8080 uv run python scripts/backfill_features.py --write

    # Without enrichment (base features only, NaN for Greeks/vol/returns):
    uv run python scripts/backfill_features.py --write

    # Specific dates:
    uv run python scripts/backfill_features.py --write --dates 2026-01-28 2026-01-29
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import structlog

# Ensure Heber src is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from heber.config import settings
from heber.ml.datasets import persist_features_to_gold
from heber.models.silver import FlowAlertRecord
from heber.watch.features import (
    DEFAULT_FEATURES_OUTPUT_PATH,
    AlertFeatureExtractor,
    AlertFeatures,
    backfill_uw_fields,
)

logger = structlog.get_logger(__name__)

# Paths
SILVER_FLOW_ALERTS = settings.data_root / "silver" / "feed=flow_alerts" / "instrument_type=option"
GOLD_OUTCOMES = settings.gold_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1"
GOLD_FEATURES = settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1"


def discover_missing_dates() -> list[str]:
    """Find dates that have outcomes but no features in Gold."""
    outcome_dates = set()
    if GOLD_OUTCOMES.exists():
        for d in GOLD_OUTCOMES.iterdir():
            if d.name.startswith("dt="):
                outcome_dates.add(d.name.replace("dt=", ""))

    feature_dates = set()
    if GOLD_FEATURES.exists():
        for d in GOLD_FEATURES.iterdir():
            if d.name.startswith("dt="):
                feature_dates.add(d.name.replace("dt=", ""))

    missing = sorted(outcome_dates - feature_dates)
    return missing


def discover_partial_dates() -> dict[str, tuple[int, int]]:
    """Find dates that have features but fewer event_ids than outcomes."""
    result = {}
    if not GOLD_OUTCOMES.exists() or not GOLD_FEATURES.exists():
        return result

    for d in GOLD_FEATURES.iterdir():
        if not d.name.startswith("dt="):
            continue
        dt_str = d.name.replace("dt=", "")
        outcome_dir = GOLD_OUTCOMES / d.name
        if not outcome_dir.exists():
            continue

        # Count unique alert_ids in each
        try:
            outcome_ids = set()
            for f in outcome_dir.glob("*.parquet"):
                pf = pq.ParquetFile(f)
                t = pf.read(columns=["alert_id"])
                outcome_ids.update(t.column("alert_id").to_pylist())

            feature_ids = set()
            for f in d.glob("*.parquet"):
                pf = pq.ParquetFile(f)
                t = pf.read(columns=["alert_id"])
                feature_ids.update(t.column("alert_id").to_pylist())

            missing_count = len(outcome_ids - feature_ids)
            if missing_count > 0:
                result[dt_str] = (missing_count, len(outcome_ids))
        except Exception:
            continue

    return result


def read_silver_alerts(dt_str: str) -> list[dict]:
    """Read Silver flow_alerts for a specific date, returning raw rows."""
    partition = SILVER_FLOW_ALERTS / f"dt={dt_str}"
    if not partition.exists():
        logger.warning("No Silver flow_alerts for date", date=dt_str)
        return []

    rows = []
    for f in sorted(partition.glob("*.parquet")):
        pf = pq.ParquetFile(f)
        table = pf.read()
        for i in range(table.num_rows):
            row = {col: table.column(col)[i].as_py() for col in table.column_names}
            rows.append(row)

    return rows


def read_existing_feature_ids(dt_str: str) -> set[str]:
    """Read alert_ids already present in Gold features for a date."""
    partition = GOLD_FEATURES / f"dt={dt_str}"
    if not partition.exists():
        return set()

    ids = set()
    for f in partition.glob("*.parquet"):
        try:
            pf = pq.ParquetFile(f)
            t = pf.read(columns=["alert_id"])
            ids.update(t.column("alert_id").to_pylist())
        except Exception:
            continue
    return ids


def read_outcome_alert_ids(dt_str: str) -> set[str]:
    """Read alert_ids from Gold outcomes for a date."""
    partition = GOLD_OUTCOMES / f"dt={dt_str}"
    if not partition.exists():
        return set()

    ids = set()
    for f in partition.glob("*.parquet"):
        try:
            pf = pq.ParquetFile(f)
            t = pf.read(columns=["alert_id"])
            ids.update(t.column("alert_id").to_pylist())
        except Exception:
            continue
    return ids


def silver_row_to_flow_alert(row: dict) -> FlowAlertRecord:
    """Convert a Silver parquet row dict into a FlowAlertRecord."""
    # Map Silver columns to FlowAlertRecord fields
    ts_event = row.get("ts_event")
    if isinstance(ts_event, datetime) and ts_event.tzinfo is None:
        ts_event = ts_event.replace(tzinfo=UTC)

    ts_ingest = row.get("ts_ingest")
    if isinstance(ts_ingest, datetime) and ts_ingest.tzinfo is None:
        ts_ingest = ts_ingest.replace(tzinfo=UTC)

    ts_available = row.get("ts_available")
    if isinstance(ts_available, datetime) and ts_available.tzinfo is None:
        ts_available = ts_available.replace(tzinfo=UTC)

    expiry = row.get("expiry")
    if isinstance(expiry, datetime):
        expiry = expiry.date()

    # Build quality_flags from stored value
    quality_flags = row.get("quality_flags")
    if quality_flags is None:
        quality_flags = []

    return FlowAlertRecord(
        event_id=row["event_id"],
        provider=row.get("provider", "unusual_whales"),
        feed=row.get("feed", "flow_alerts"),
        instrument_type=row.get("instrument_type", "option"),
        instrument_key=row.get("instrument_key", ""),
        symbol=row.get("symbol", ""),
        ts_event=ts_event,
        ts_ingest=ts_ingest,
        ts_available=ts_available,
        source=row.get("source", "rest"),
        schema_version=row.get("schema_version", "v1"),
        quality_flags=quality_flags,
        underlying=row.get("underlying", row.get("symbol", "")),
        occ_symbol=row.get("occ_symbol"),
        expiry=expiry,
        strike=float(row.get("strike", 0)),
        put_call=row.get("put_call", "C"),
        premium=float(row.get("premium", 0)),
        volume=float(row.get("volume", 0)),
        open_interest=row.get("open_interest"),
        spot_px=row.get("spot_px"),
        contract_px=row.get("contract_px"),
        alert_type=row.get("alert_type", "UNKNOWN"),
        side=row.get("side"),
        aggressor=row.get("aggressor"),
        is_sweep=row.get("is_sweep"),
        is_unusual=row.get("is_unusual"),
        trade_count=row.get("trade_count"),
        volume_oi_ratio=row.get("volume_oi_ratio"),
        total_ask_side_prem=row.get("total_ask_side_prem"),
        total_bid_side_prem=row.get("total_bid_side_prem"),
        has_floor=row.get("has_floor"),
        has_multileg=row.get("has_multileg"),
        has_singleleg=row.get("has_singleleg"),
        all_opening_trades=row.get("all_opening_trades"),
        total_size=row.get("total_size"),
        expiry_count=row.get("expiry_count"),
    )


async def backfill_date(
    dt_str: str,
    extractor: AlertFeatureExtractor,
    write: bool = False,
) -> tuple[int, int, int]:
    """Backfill features for a single date.

    Returns:
        (processed, skipped, errors) counts.
    """
    # Get outcome alert_ids for this date
    outcome_ids = read_outcome_alert_ids(dt_str)
    if not outcome_ids:
        logger.info("No outcomes for date, skipping", date=dt_str)
        return 0, 0, 0

    # Get already-existing feature ids
    existing_ids = read_existing_feature_ids(dt_str)

    # Read Silver alerts
    silver_rows = read_silver_alerts(dt_str)
    if not silver_rows:
        logger.warning("No Silver flow_alerts for date", date=dt_str)
        return 0, 0, 0

    # Filter to alerts that have outcomes but no features
    needed_ids = outcome_ids - existing_ids
    to_process = [r for r in silver_rows if r.get("event_id") in needed_ids]

    logger.info(
        "Date backfill plan",
        date=dt_str,
        outcomes=len(outcome_ids),
        existing_features=len(existing_ids),
        needed=len(needed_ids),
        silver_available=len(silver_rows),
        to_process=len(to_process),
    )

    processed = 0
    skipped = 0
    errors = 0
    batch_features: list[dict] = []

    for row in to_process:
        try:
            alert = silver_row_to_flow_alert(row)
            features: AlertFeatures = await extractor.extract(alert)
            batch_features.append(dict(features.__dict__))
            processed += 1
        except Exception as exc:
            errors += 1
            logger.warning(
                "Failed to extract features",
                event_id=row.get("event_id"),
                error=str(exc),
            )

    # Persist batch
    if write and batch_features:
        features_df = pd.DataFrame(batch_features)
        persist_features_to_gold(
            features_df=features_df,
            output_path=DEFAULT_FEATURES_OUTPUT_PATH,
            partition_col="alert_time",
        )
        logger.info("Wrote features to Gold", date=dt_str, rows=len(batch_features))
    elif batch_features:
        logger.info("Dry run — would write features", date=dt_str, rows=len(batch_features))

    return processed, skipped, errors


def fill_nulls_from_silver(write: bool = False) -> None:
    """Fill null GEX/VEX/market_tide fields in existing Gold features from Silver data.

    Reads all Gold meta_label_features partitions, identifies rows with null
    GEX/VEX/market_tide fields, and fills them using asof-joins against
    Silver greek_exposure and market_tide feeds.
    """
    import pyarrow.dataset as ds

    print("\n" + "=" * 70)
    print("FILL NULLS FROM SILVER")
    print("=" * 70)

    if not GOLD_FEATURES.exists():
        print("No Gold meta_label_features found.")
        return

    # Read all Gold features
    try:
        gold_ds = ds.dataset(str(GOLD_FEATURES), format="parquet", partitioning=ds.partitioning(flavor="hive"))
        gold_df = gold_ds.to_table().to_pandas()
    except Exception as e:
        print(f"Failed to read Gold features: {e}")
        return

    if gold_df.empty:
        print("Gold features dataset is empty.")
        return

    # Ensure required columns exist with NaN defaults
    for col in ["gex", "vex", "max_pain_strike", "max_pain_distance_pct", "market_tide_net_premium"]:
        if col not in gold_df.columns:
            gold_df[col] = float("nan")
    if "market_tide_direction" not in gold_df.columns:
        gold_df["market_tide_direction"] = None

    # Count nulls before
    null_counts_before = {
        "gex": int(gold_df["gex"].isna().sum()),
        "vex": int(gold_df["vex"].isna().sum()),
        "market_tide_net_premium": int(gold_df["market_tide_net_premium"].isna().sum()),
        "market_tide_direction": int(gold_df["market_tide_direction"].isna().sum()),
    }
    total_nulls = sum(null_counts_before.values())

    print(f"\nTotal Gold rows: {len(gold_df)}")
    print("Null fields before backfill:")
    for field, count in null_counts_before.items():
        print(f"  {field}: {count}")

    if total_nulls == 0:
        print("\nNo null fields to backfill.")
        return

    # Run backfill
    filled_df = backfill_uw_fields(gold_df)

    # Count nulls after
    null_counts_after = {
        "gex": int(filled_df["gex"].isna().sum()),
        "vex": int(filled_df["vex"].isna().sum()),
        "market_tide_net_premium": int(filled_df["market_tide_net_premium"].isna().sum()),
        "market_tide_direction": int(filled_df["market_tide_direction"].isna().sum()),
    }

    print("\nNull fields after backfill:")
    for field, count in null_counts_after.items():
        filled = null_counts_before[field] - count
        print(f"  {field}: {count} (filled {filled})")

    if not write:
        print("\n*** DRY RUN — pass --write to persist ***")
        return

    # Rewrite affected Gold partitions
    persist_features_to_gold(
        features_df=filled_df,
        output_path=GOLD_FEATURES,
        partition_col="alert_time",
    )
    print(f"\nWrote {len(filled_df)} rows back to Gold.")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Gold meta_label_features from Silver flow_alerts")
    parser.add_argument("--write", action="store_true", help="Actually write to Gold (default: dry run)")
    parser.add_argument("--dates", nargs="*", help="Specific dates to backfill (YYYY-MM-DD)")
    parser.add_argument("--include-partial", action="store_true", help="Also backfill dates with partial features")
    parser.add_argument("--gateway-url", default=None, help="Data Gateway URL for enrichment")
    parser.add_argument("--fill-nulls", action="store_true", help="Fill null GEX/VEX/tide fields from Silver data")
    args = parser.parse_args()

    # Handle --fill-nulls mode separately
    if args.fill_nulls:
        fill_nulls_from_silver(write=args.write)
        return

    print("=" * 70)
    print("FEATURE BACKFILL")
    print("=" * 70)

    # Discover missing dates
    missing = discover_missing_dates()
    print(f"\nDates with outcomes but NO features: {len(missing)}")
    for d in missing:
        outcome_count = len(read_outcome_alert_ids(d))
        silver_count = len(read_silver_alerts(d))
        print(f"  {d}: {outcome_count} outcomes, {silver_count} silver alerts")

    if args.include_partial:
        partial = discover_partial_dates()
        print(f"\nDates with PARTIAL features: {len(partial)}")
        for d, (missing_count, total) in sorted(partial.items()):
            print(f"  {d}: {missing_count}/{total} missing")

    # Determine which dates to process
    if args.dates:
        dates_to_process = args.dates
    else:
        dates_to_process = list(missing)
        if args.include_partial:
            dates_to_process.extend(sorted(partial.keys()))

    if not dates_to_process:
        print("\nNo dates to backfill!")
        return

    # Set up extractor
    gateway_url = args.gateway_url or os.environ.get("HEBER_WATCH_GATEWAY_URL")
    gateway_api_key = os.environ.get("HEBER_WATCH_GATEWAY_API_KEY")

    if gateway_url:
        print(f"\nData Gateway: {gateway_url} (full enrichment)")
    else:
        print("\nNo Data Gateway — base features only (Greeks/vol/returns will be NaN)")

    extractor = AlertFeatureExtractor(
        gateway_url=gateway_url,
        gateway_api_key=gateway_api_key,
    )

    if not args.write:
        print("\n*** DRY RUN — pass --write to persist ***")

    # Process each date
    total_processed = 0
    total_skipped = 0
    total_errors = 0

    for dt_str in sorted(dates_to_process):
        processed, skipped, errors = await backfill_date(dt_str, extractor, write=args.write)
        total_processed += processed
        total_skipped += skipped
        total_errors += errors

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Dates processed: {len(dates_to_process)}")
    print(f"  Features extracted: {total_processed}")
    print(f"  Skipped (already exist): {total_skipped}")
    print(f"  Errors: {total_errors}")
    if not args.write:
        print("\n  *** DRY RUN — no data written. Pass --write to persist. ***")


if __name__ == "__main__":
    asyncio.run(main())
