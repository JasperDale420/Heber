"""Self-healing EOD feed reconciliation.

After the end-of-day UnusualWhales publish, a daily feed's Silver partition can be
missing for the day — e.g. heber-consumer was down during the publish and the
capped Redis stream evicted the unread entries (the 2026-06-25 oi_change loss).
This re-pulls each missing feed from UW via the Data-Gateway backfill API so the
gap self-heals from source instead of staying lost.

Off by default (``HEBER_EOD_RECONCILE_ENABLED``). The poller gates this to run once
per trading day, after the feeds' real EOD deadline (default 17:45 ET, past the
17:30 liveness deadline) so in-flight EOD data is not mistaken for a gap.

Fire-and-forget: the gateway re-pulls from UW and republishes to ``heber:events``;
the consumer ingests asynchronously. ``iv_term_structure`` is intentionally not a
default target — its UW provider method is snapshot-only and cannot resolve a past
date (see Data-Gateway backfill).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import structlog

from heber.config import Settings
from heber.core.http_client import create_async_http_client
from heber.reader import HeberReader
from heber.watch.gateway import gateway_auth_headers

logger = structlog.get_logger(__name__)

BACKFILL_ROUTE = "/api/v1/backfill"


def _feed_has_rows_today(reader: HeberReader, feed: str, today: date) -> bool:
    """True if ``feed`` has at least one Silver row stamped today (UTC day window).

    A read error is treated as "present" so a transient read blip never triggers
    a spurious backfill — the liveness alarm already covers a genuinely dark feed.
    """
    day_start = datetime(today.year, today.month, today.day, tzinfo=UTC)
    now = datetime.now(UTC)
    try:
        df = reader.read_silver(
            feed,
            time_range=(day_start.isoformat(), now.isoformat()),
            columns=["ts_event"],
            prune_by_dt=True,
        )
    except Exception:
        logger.warning("eod_reconcile_read_error", feed=feed, exc_info=True)
        return True
    return df is not None and not df.empty


def detect_missing_eod_feeds(reader: HeberReader, feeds: list[str], today: date) -> list[str]:
    """Return the subset of ``feeds`` with no Silver rows for ``today``."""
    return [feed for feed in feeds if not _feed_has_rows_today(reader, feed, today)]


async def reconcile_eod_feeds(settings: Settings, reader: HeberReader, *, today: date) -> dict[str, Any]:
    """Detect missing EOD feeds for ``today`` and trigger a gateway backfill for each.

    Returns a summary dict ``{date, missing, triggered, failed}``.
    """
    feeds = settings.eod_reconcile_feed_list
    symbols = settings.eod_reconcile_symbol_list
    missing = detect_missing_eod_feeds(reader, feeds, today)

    summary: dict[str, Any] = {"date": today.isoformat(), "missing": missing, "triggered": [], "failed": []}
    if not missing:
        logger.info("eod_reconcile_no_gaps", date=today.isoformat(), feeds=feeds)
        return summary
    if not symbols:
        # Gateway backfill requires an explicit symbol list — nothing to send.
        logger.warning("eod_reconcile_no_symbols", missing=missing)
        summary["failed"] = list(missing)
        return summary

    headers = gateway_auth_headers(settings.watch_gateway_api_key)
    async with create_async_http_client(base_url=settings.watch_gateway_url, timeout=30.0) as client:
        for feed in missing:
            body = {
                "provider": "unusual_whales",
                "feed": feed,
                "symbols": symbols,
                "start": today.isoformat(),
                "end": today.isoformat(),
            }
            try:
                resp = await client.post(BACKFILL_ROUTE, json=body, headers=headers)
                resp.raise_for_status()
                summary["triggered"].append(feed)
                logger.info(
                    "eod_reconcile_backfill_triggered",
                    feed=feed,
                    date=today.isoformat(),
                    symbol_count=len(symbols),
                )
            except Exception:
                summary["failed"].append(feed)
                logger.error("eod_reconcile_backfill_failed", feed=feed, date=today.isoformat(), exc_info=True)

    logger.info(
        "eod_reconcile_complete",
        date=today.isoformat(),
        missing=missing,
        triggered=summary["triggered"],
        failed=summary["failed"],
    )
    return summary
