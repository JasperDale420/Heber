"""Enrichment Backfill Scanner — periodic re-enrichment of Gold feature rows.

Scans recent Gold feature partitions for rows with null enrichment fields
(Greeks, GEX, IV rank, max pain, market tide), re-enriches them via the
Data Gateway, and patches the parquet files using dedup-on-alert_id.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import structlog
from prometheus_client import Counter, Histogram

from heber.ml.datasets import normalize_expiry

if TYPE_CHECKING:
    from heber.calendar.market import MarketCalendar
    from heber.watch.features import AlertFeatureExtractor

logger = structlog.get_logger(__name__)

# Fields that should be non-null if enrichment succeeded.
ENRICHABLE_FIELDS: tuple[str, ...] = (
    "delta",
    "gamma",
    "theta",
    "vega",
    "iv",
    "iv_rank",
    "gex",
    "vex",
    "max_pain_strike",
    "max_pain_distance_pct",
    "market_tide_net_premium",
    "market_tide_direction",
)

# Prometheus metrics
backfill_scanned_total = Counter(
    "heber_enrichment_backfill_scanned_total",
    "Rows scanned by enrichment backfill",
)
backfill_patched_total = Counter(
    "heber_enrichment_backfill_patched_total",
    "Rows successfully re-enriched by backfill",
)
backfill_failed_total = Counter(
    "heber_enrichment_backfill_failed_total",
    "Rows that failed re-enrichment",
)
backfill_duration_seconds = Histogram(
    "heber_enrichment_backfill_duration_seconds",
    "Duration of a backfill scan cycle",
    buckets=[10, 30, 60, 120, 300, 600, 1800],
)


def _coerce_expiry_to_date(val: object) -> date | None:
    """Convert an expiry value to a ``datetime.date`` (Arrow ``date32`` on write).

    The live watch writer persists ``expiry`` as a ``date`` (date32); the backfill
    path must match, or a gold dataset spanning both writers' ``dt=`` partitions
    raises ``ArrowNotImplementedError: Unsupported cast from int64 to date32`` on
    read (2026-06-30 incident).
    """
    coerced = normalize_expiry(val)
    if coerced is None and val is not None:
        logger.warning("Cannot coerce expiry to date", raw_value=val)
    return coerced


def _is_polars_panic_exception(exc: BaseException) -> bool:
    """Return True when a BaseException is a polars/pyo3 panic wrapper."""
    return exc.__class__.__name__ == "PanicException" or exc.__class__.__module__ == "pyo3_runtime"


class EnrichmentBackfillScanner:
    """Scans Gold feature partitions for null enrichment fields and re-enriches."""

    def __init__(
        self,
        feature_extractor: AlertFeatureExtractor,
        features_output_path: Path,
        *,
        interval_seconds: int = 3600,
        lookback_days: int = 3,
        batch_size: int = 50,
        calendar: MarketCalendar | None = None,
    ) -> None:
        self.feature_extractor = feature_extractor
        self.features_output_path = features_output_path
        self.interval_seconds = interval_seconds
        self.lookback_days = lookback_days
        self.batch_size = batch_size
        self._running = False

        if calendar is None:
            from heber.calendar.market import MarketCalendar

            calendar = MarketCalendar()
        self.calendar = calendar

    async def run(self) -> None:
        """Run the scanner loop: sleep → check market hours → scan → repeat."""
        self._running = True
        logger.info(
            "Enrichment backfill scanner started",
            interval_seconds=self.interval_seconds,
            lookback_days=self.lookback_days,
            batch_size=self.batch_size,
        )
        while self._running:
            await asyncio.sleep(self.interval_seconds)

            if not self._running:
                break

            if not self.calendar.is_market_open():
                logger.debug("Enrichment backfill skipped: market closed")
                continue

            try:
                await self.scan_and_backfill()
            except Exception:
                logger.error(
                    "Enrichment backfill scan failed",
                    exc_info=True,
                )

    def stop(self) -> None:
        """Stop the scanner."""
        self._running = False

    async def scan_and_backfill(self) -> dict:
        """Scan recent partitions, re-enrich incomplete rows, patch parquet.

        Returns:
            Summary dict with scanned, patched, and failed counts.
        """
        start = time.monotonic()
        summary = {"scanned": 0, "patched": 0, "failed": 0}

        df = await asyncio.to_thread(self._read_recent_partitions)
        if df is None or df.empty:
            logger.debug("Enrichment backfill: no partitions found")
            return summary

        incomplete = self._find_incomplete_rows(df)
        summary["scanned"] = len(df)
        backfill_scanned_total.inc(len(df))

        if incomplete.empty:
            logger.info(
                "Enrichment backfill: all rows complete",
                scanned=len(df),
            )
            return summary

        logger.info(
            "Enrichment backfill found incomplete rows",
            incomplete=len(incomplete),
            scanned=len(df),
        )

        # Limit to batch_size
        batch = incomplete.head(self.batch_size)

        for row in batch.to_dict(orient="records"):
            try:
                updated = await self._re_enrich_row(row)
                if updated is not None:
                    await asyncio.to_thread(
                        self._patch_partition,
                        updated,
                    )
                    summary["patched"] += 1
                    backfill_patched_total.inc()
                else:
                    summary["failed"] += 1
                    backfill_failed_total.inc()
            except Exception:
                logger.warning(
                    "Re-enrichment failed for row",
                    alert_id=row.get("alert_id"),
                    exc_info=True,
                )
                summary["failed"] += 1
                backfill_failed_total.inc()

        elapsed = time.monotonic() - start
        backfill_duration_seconds.observe(elapsed)

        logger.info(
            "Enrichment backfill cycle complete",
            elapsed_seconds=round(elapsed, 2),
            **summary,
        )
        return summary

    def _read_recent_partitions(self) -> pd.DataFrame | None:
        """Read Gold feature parquet for the last N days."""
        today = date.today()
        frames: list[pd.DataFrame] = []

        for offset in range(self.lookback_days):
            dt = today - timedelta(days=offset)
            dt_str = dt.strftime("%Y-%m-%d")
            partition_path = self.features_output_path / f"dt={dt_str}" / "data.parquet"

            if not partition_path.exists():
                continue

            try:
                frame = pd.read_parquet(partition_path)
                frames.append(frame)
            except BaseException as exc:
                if not isinstance(exc, Exception) and not _is_polars_panic_exception(exc):
                    raise
                logger.warning(
                    "Failed to read feature partition",
                    path=str(partition_path),
                    error_type=f"{exc.__class__.__module__}.{exc.__class__.__name__}",
                    exc_info=True,
                )

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _find_incomplete_rows(df: pd.DataFrame) -> pd.DataFrame:
        """Return rows where at least one enrichable field is null."""
        available_fields = [f for f in ENRICHABLE_FIELDS if f in df.columns]
        if not available_fields:
            return df.iloc[0:0]

        mask = df[available_fields].isna().any(axis=1)
        return df[mask].reset_index(drop=True)

    async def _re_enrich_row(self, row: dict) -> dict | None:
        """Reconstruct a FlowAlertRecord and re-run enrichment.

        Returns:
            Updated feature dict, or None on failure.
        """
        from heber.models.silver import FlowAlertRecord

        alert_time = row.get("alert_time")
        if isinstance(alert_time, str):
            alert_time = datetime.fromisoformat(alert_time)
        if alert_time is None:
            alert_time = datetime.now(UTC)
        elif alert_time.tzinfo is None:
            alert_time = alert_time.replace(tzinfo=UTC)

        expiry = row.get("expiry")
        if isinstance(expiry, str):
            expiry = date.fromisoformat(expiry[:10])
        if expiry is None:
            expiry = date.today()

        try:
            record = FlowAlertRecord(
                event_id=row["alert_id"],
                ts_event=alert_time,
                ts_ingest=datetime.now(UTC),
                ts_available=datetime.now(UTC),
                instrument_type="option",
                instrument_key=f"option:{row.get('occ_symbol', '')}",
                symbol=row.get("occ_symbol") or row["underlying"],
                provider="unusual_whales",
                feed="flow_alerts",
                source="enrichment_backfill",
                underlying=row["underlying"],
                occ_symbol=row.get("occ_symbol"),
                expiry=expiry,
                strike=row.get("strike", 0.0),
                put_call=row["put_call"],
                premium=row.get("premium", 0.0),
                volume=row.get("volume", 0.0),
                open_interest=row.get("open_interest"),
                spot_px=row.get("spot_price"),
                contract_px=row.get("contract_price"),
                alert_type=row.get("alert_type", "UNKNOWN"),
                side=row.get("side"),
                aggressor=row.get("aggressor"),
            )

            features = await self.feature_extractor.extract(record)
            return features.to_dict()
        except Exception:
            logger.warning(
                "Failed to re-enrich row",
                alert_id=row.get("alert_id"),
                underlying=row.get("underlying"),
                exc_info=True,
            )
            return None

    def _patch_partition(self, updated_row: dict) -> None:
        """Write updated feature row back to parquet using dedup-on-alert_id."""
        from heber.ml.datasets import persist_features_to_gold

        # Add Gold contract fields if missing.
        if "instrument_key" not in updated_row or updated_row.get("instrument_key") is None:
            occ = updated_row.get("occ_symbol")
            if occ:
                updated_row["instrument_key"] = f"option:{occ}"
            else:
                underlying = updated_row.get("underlying", updated_row.get("symbol", "UNKNOWN"))
                updated_row["instrument_key"] = f"equity:{underlying}"

        if "ts_event" not in updated_row or updated_row.get("ts_event") is None:
            updated_row["ts_event"] = updated_row.get("alert_time")

        if "ts_available" not in updated_row or updated_row.get("ts_available") is None:
            updated_row["ts_available"] = datetime.now(UTC)

        features_df = pd.DataFrame([updated_row])

        # Coerce alert_time to datetime if needed
        if "alert_time" in features_df.columns:
            features_df["alert_time"] = pd.to_datetime(features_df["alert_time"], utc=True)

        # Coerce expiry to a date (date32) — matches the live watch writer so a
        # gold dataset spanning live + backfill partitions reads cleanly.
        # Enrichment returns date strings like "2026-04-18" or "20260418".
        if "expiry" in features_df.columns:
            features_df["expiry"] = features_df["expiry"].apply(_coerce_expiry_to_date)

        # Coerce ts_event / ts_available to datetime if present as strings
        for ts_col in ("ts_event", "ts_available"):
            if ts_col in features_df.columns:
                features_df[ts_col] = pd.to_datetime(features_df[ts_col], utc=True)

        persist_features_to_gold(
            features_df=features_df,
            output_path=self.features_output_path,
            partition_col="alert_time",
        )
