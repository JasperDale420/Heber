"""Enrichment Backfill Scanner — periodic re-enrichment of Gold feature rows.

Scans recent Gold feature partitions for rows with null enrichment fields
(Greeks, GEX, IV rank, max pain, market tide), re-enriches them via the
Data Gateway, and fills the still-null fields in place.

The patch is strictly additive. Every enrichable field comes from a gateway
route that answers "as of now", so a re-enrichment can only ever contribute a
value the row is missing — it can never improve one already captured live at
alert time. Overwriting is therefore always a loss, whether the fresh value is
null (stale alert, gateway failure) or a present-day reading stamped onto a
past observation.
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


def _is_null(value: object) -> bool:
    """Scalar null test covering None, NaN, pd.NA and NaT.

    Parquet nulls surface as any of those depending on column dtype, and plain
    truthiness would wrongly treat a legitimate ``0.0`` delta as missing.
    """
    return bool(pd.isna(value))


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
        summary = {"scanned": 0, "patched": 0, "failed": 0, "skipped": 0}

        df = await asyncio.to_thread(self._read_recent_partitions)
        if df is None or df.empty:
            logger.debug("Enrichment backfill: no partitions found")
            return summary

        max_age = self.feature_extractor.live_enrichment_max_age
        incomplete = self._find_incomplete_rows(df, max_age)
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
                filled = await self._re_enrich_row(row, max_age)
                if filled is None:
                    summary["failed"] += 1
                    backfill_failed_total.inc()
                elif not filled:
                    summary["skipped"] += 1
                elif await asyncio.to_thread(self._patch_partition, row, filled):
                    summary["patched"] += 1
                    backfill_patched_total.inc()
                else:
                    summary["skipped"] += 1
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
        """Read Gold feature parquet for the last N days.

        Partitions are enumerated in UTC because that is what the writer's
        ``dt=`` derivation uses; a local date would drift off the current
        partition whenever the two calendars disagree.
        """
        today = datetime.now(UTC).date()
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
    def _find_incomplete_rows(df: pd.DataFrame, max_age: timedelta | None) -> pd.DataFrame:
        """Return rows where at least one enrichable field is null and is still fillable.

        Selection fails closed on three counts:

        - Rows older than ``max_age`` are excluded. Past that bound the extractor
          refuses every live-only step, so re-enrichment cannot contribute a value
          — it can only return nulls and stale-source flags.
        - ``max_age is None`` (the extractor's age gate switched off) excludes
          everything. Without the gate a re-enrichment would stamp present-day
          market state onto an alert up to ``lookback_days`` old.
        - Rows with a missing or unparseable ``alert_time`` are excluded, since
          their age cannot be established.
        - Future-dated rows are excluded. Flow alerts can carry a timestamp a few
          seconds ahead of wall clock, and patching one would stamp an
          availability time earlier than the event it describes.

        Rows flagged ``greeks_no_point_in_time_source`` are excluded for the same
        reason: no as-of source exists for them.
        """
        from heber.ml.datasets import quality_flag_series
        from heber.watch.features import QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE

        available_fields = [f for f in ENRICHABLE_FIELDS if f in df.columns]
        if not available_fields or max_age is None or "alert_time" not in df.columns:
            return df.iloc[0:0]

        alert_time = pd.to_datetime(df["alert_time"], utc=True, errors="coerce")
        age = pd.Timestamp.now(tz=UTC) - alert_time
        fresh = alert_time.notna() & (age >= pd.Timedelta(0)) & (age <= max_age)

        mask = df[available_fields].isna().any(axis=1)
        unrecoverable = quality_flag_series(df).apply(
            lambda flags: QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE in flags
        )
        return df[mask & fresh & ~unrecoverable].reset_index(drop=True)

    async def _re_enrich_row(self, row: dict, max_age: timedelta | None) -> dict | None:
        """Reconstruct a FlowAlertRecord and re-run enrichment.

        Returns:
            The enrichable fields the fresh extract can fill (empty when there is
            nothing to add), or None on failure.
        """
        from heber.models.silver import FlowAlertRecord
        from heber.watch.features import feature_row_for_gold

        alert_time = pd.to_datetime(row.get("alert_time"), utc=True, errors="coerce")
        if pd.isna(alert_time) or max_age is None:
            return {}

        # A batch takes minutes of gateway calls to work through, so a row that
        # was fresh at selection can cross the bound before its turn. Re-check
        # here: extracting past the bound yields nulls and stale-source flags
        # that would misdescribe the values the row already holds.
        age = pd.Timestamp.now(tz=UTC) - alert_time
        if age > max_age or age < pd.Timedelta(0):
            logger.debug(
                "Skipping re-enrichment: alert outside the live enrichment window",
                alert_id=row.get("alert_id"),
                age_seconds=int(age.total_seconds()),
            )
            return {}

        alert_time = alert_time.to_pydatetime()

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
            fresh = feature_row_for_gold(features)
        except Exception:
            logger.warning(
                "Failed to re-enrich row",
                alert_id=row.get("alert_id"),
                underlying=row.get("underlying"),
                exc_info=True,
            )
            return None

        # Only the enrichable whitelist crosses over, and only where the row is
        # actually missing a value. Everything else the fresh extract carries is
        # either identity/source data reconstructed from this very row (with
        # fabricated defaults where the row was null) or a present-day reading
        # that must not displace what was captured at alert time.
        return {
            field_: fresh[field_]
            for field_ in ENRICHABLE_FIELDS
            if field_ in fresh and not _is_null(fresh[field_]) and _is_null(row.get(field_))
        }

    def _patch_partition(self, row: dict, filled: dict) -> bool:
        """Fill the row's still-null enrichment fields in place.

        The read-modify-write runs under the same partition lock the canonical
        writer uses, and re-checks nullness against the on-disk row rather than
        the scan snapshot — a live write for the same ``alert_id`` can land in
        the minutes between the two, and must not be reverted.

        Returns:
            True when the partition was rewritten.
        """
        from filelock import FileLock, Timeout

        from heber.ml.datasets import _atomic_write_parquet

        alert_id = row.get("alert_id")
        dt = pd.to_datetime(row["alert_time"], utc=True).date()
        out_file = self.features_output_path / f"dt={dt:%Y-%m-%d}" / "data.parquet"
        lock_file = out_file.with_suffix(".parquet.lock")

        try:
            with FileLock(lock_file, timeout=10):
                if not out_file.exists():
                    return False

                current = pd.read_parquet(out_file)
                if "ts_available" not in current.columns:
                    # Without an availability column the fill could never be held
                    # back from a point-in-time read, so leave the row alone.
                    logger.warning(
                        "Skipping backfill patch: partition has no ts_available column",
                        alert_id=alert_id,
                        path=str(out_file),
                    )
                    return False

                targets = current.index[current["alert_id"] == alert_id]
                if targets.empty:
                    logger.warning(
                        "Backfill target row vanished from partition",
                        alert_id=alert_id,
                        path=str(out_file),
                    )
                    return False

                applied: list[str] = []
                for idx in targets:
                    for field_, value in filled.items():
                        if field_ in current.columns and _is_null(current.at[idx, field_]):
                            current.at[idx, field_] = value
                            applied.append(field_)

                if not applied:
                    return False

                # A value fetched now only becomes queryable now. Keeping the
                # original ts_available would backdate it and reintroduce the
                # look-ahead the zero-leakage contract exists to prevent. It is
                # clamped to the event time so the Gold invariant
                # ts_available >= ts_event holds even for a future-dated alert.
                now = pd.Timestamp.now(tz=UTC)
                event_col = "ts_event" if "ts_event" in current.columns else "alert_time"
                for idx in targets:
                    event = pd.to_datetime(current.at[idx, event_col], utc=True, errors="coerce")
                    current.at[idx, "ts_available"] = now if pd.isna(event) else max(now, event)

                _atomic_write_parquet(current, out_file)
        except Timeout:
            logger.warning(
                "Could not acquire partition lock, skipping backfill patch",
                alert_id=alert_id,
                path=str(out_file),
            )
            return False

        logger.info(
            "Backfilled enrichment fields",
            alert_id=alert_id,
            fields=sorted(set(applied)),
            path=str(out_file),
        )
        return True
