"""Heber CLI - Command line interface for Heber Data Lakehouse."""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from heber import __version__
from heber.ops.notifier import DiscordNotifier


def _cmd_info(args: argparse.Namespace) -> int:
    """Handle the 'info' subcommand."""
    print(f"Heber Data Lakehouse v{__version__}")
    if args.verbose:
        print("\nComponents:")
        print("  - Storage: Parquet (Bronze JSONL.gz / Silver+Gold Parquet)")
        print("  - Catalog: Postgres (heber.catalog)")
    return 0


def _is_research_artifact(feed_dir: Path) -> bool:
    """Return True for non-Heber-managed feed dirs (Atlas hypothesis materializations).

    Atlas writes hypothesis outputs directly to ``silver/feed=atlas_features_*/``
    with an ``_atlas_materialization_meta.json`` marker. They share storage but
    are NOT Bronze→Silver pipeline outputs and pollute dataset listings/audits.
    """
    if (feed_dir / "_atlas_materialization_meta.json").exists():
        return True
    if feed_dir.name.startswith("feed=atlas_features_"):
        return True
    return False


def _cmd_datasets(args: argparse.Namespace) -> int:
    """Handle the 'datasets' subcommand."""
    try:
        from heber.config import settings

        layer = args.layer or "silver"
        base = settings.data_root / layer
        if not base.exists():
            print(f"No {layer} data found at {base}", file=sys.stderr)
            return 1
        feeds = sorted(
            {p.name.split("=")[1] for p in base.glob("feed=*") if p.is_dir() and not _is_research_artifact(p)}
        )
        for feed in feeds:
            print(f"  {feed}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def _cmd_versions(args: argparse.Namespace) -> int:
    """Handle the 'versions' subcommand."""
    try:
        from heber.reader import HeberReader

        reader = HeberReader()
        versions = reader.list_gold_versions(args.dataset)
        for v in versions:
            print(f"  {v}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def _cmd_backfill(args: argparse.Namespace) -> int:
    """Handle the 'backfill' subcommand."""
    from datetime import datetime

    from heber.writer.transformer import BronzeToSilverTransformer

    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None
    until = datetime.strptime(args.until, "%Y-%m-%d") if args.until else None

    transformer = BronzeToSilverTransformer()

    if args.feed:
        print(f"Backfilling feed: {args.feed}")
        count = transformer.transform(args.feed, since=since, until=until)
        print(f"Transformed {count} records")
    else:
        print("Backfilling all feeds from Bronze to Silver...")
        stats = transformer.transform_all(since=since, until=until)
        for feed, count in sorted(stats.items()):
            print(f"  {feed}: {count} records")
    return 0


def _cmd_health_dataflow(args: argparse.Namespace) -> int:
    """Handle the 'health-dataflow' subcommand."""
    import heber.ops.dataflow_health as dataflow_health

    try:
        if args.loop:
            dataflow_health.run_dataflow_health_loop(
                window_seconds=args.window_seconds,
                mode=args.mode,
                interval_seconds=args.interval_seconds,
                consumer_metrics_url=args.consumer_metrics_url,
                watch_metrics_url=args.watch_metrics_url,
                report_dir=args.report_dir,
            )
            return 0

        report = dataflow_health.run_dataflow_health_once(
            window_seconds=args.window_seconds,
            mode=args.mode,
            consumer_metrics_url=args.consumer_metrics_url,
            watch_metrics_url=args.watch_metrics_url,
            report_dir=args.report_dir,
        )
        print(json.dumps(report))
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def _cmd_health_daily(args: argparse.Namespace) -> int:
    """Handle the 'health-daily' subcommand."""
    from datetime import datetime

    from heber.ops.daily_health import run_daily_health

    try:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
        report = run_daily_health(
            report_date=report_date,
            force=args.force,
            verbose=args.verbose,
        )
        if args.verbose:
            print(json.dumps(report, indent=2, default=str))
        else:
            status = report["overall_status"]
            rd = report.get("report_date", "unknown")
            summary = report.get("summary", {})
            ok, w, f = summary.get("ok", 0), summary.get("warn", 0), summary.get("fail", 0)
            print(f"{rd}: {status} (ok={ok} warn={w} fail={f})")
        return 0 if report["overall_status"] in ("ok", "skipped") else 1
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def _cmd_alert_calibrate(args: argparse.Namespace) -> int:
    """Print suggested per-feed liveness floors from healthy history."""
    from heber.ops.calibrate_floors import calibrate_json

    print("Suggested HEBER_ALERT_FLOOR_OVERRIDES (paste into .env):")
    print(calibrate_json(days_back=args.days_back, ratio=args.ratio))
    return 0


def _cmd_alert_test(args: argparse.Namespace) -> int:
    """Send a test message through the Discord notifier."""
    from heber.config import get_settings

    settings = get_settings()
    notifier = DiscordNotifier(settings)
    text = args.message or "✅ Heber alert-test — data-quality alerting is wired up."
    ok = notifier.send_test(text)
    print("Sent." if ok else "Failed to send (check HEBER_ALERT_DISCORD_* settings).")
    return 0 if ok else 1


def _cmd_alert_check(args: argparse.Namespace) -> int:
    """Run one liveness cycle and dispatch any criticals to Discord, then exit.

    Built to be scheduled (launchd StartInterval) so the alarm runs as a fast,
    isolated process rather than a loop sharing the multi-tier monitor's event
    loop. Cooldown/recovery state lives on disk, so throttling works across runs.
    """
    import asyncio
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from heber.config import get_settings
    from heber.health_monitor.calendar import HealthCalendar
    from heber.health_monitor.checks.liveness import run_liveness_checks
    from heber.health_monitor.models import CheckContext
    from heber.reader import HeberReader

    settings = get_settings()
    ctx = CheckContext(
        settings=settings,
        reader=HeberReader(),
        redis=None,
        calendar=HealthCalendar(),
        store=None,
    )
    now = datetime.now(ZoneInfo("America/New_York"))
    results = asyncio.run(run_liveness_checks(ctx, now=now))

    DiscordNotifier(settings).dispatch(results)

    criticals = [r for r in results if r.status.value in ("fail", "error")]
    print(f"alert-check: {len(results)} checks, {len(criticals)} critical")
    for r in results:
        print(f"  {r.status.value.upper():4} {r.feed}: {r.message}")
    return 0


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _cmd_massive_daily(args: argparse.Namespace) -> int:
    """Sync publishable Massive stock flat files into the raw vendor archive."""
    import structlog

    from heber.backfill.massive.daily import MassiveDailySync

    try:
        sync = MassiveDailySync(
            archive_root=args.archive_root,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
            api_key=args.api_key,
        )
        result = sync.run(
            target_date=_parse_iso_date(args.date) if args.date else None,
            start_date=_parse_iso_date(args.start_date) if args.start_date else None,
            end_date=_parse_iso_date(args.end_date) if args.end_date else None,
            sync_corporate_actions=not args.skip_corporate_actions,
        )
        print(json.dumps(result, default=str))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        structlog.get_logger(__name__).error("massive_daily_cli_failed", error=str(e), exc_info=True)
        print(f"Error: {e}", file=sys.stderr)
        return 1


_SUBCOMMAND_HANDLERS = {
    "info": _cmd_info,
    "datasets": _cmd_datasets,
    "versions": _cmd_versions,
    "backfill": _cmd_backfill,
    "health-dataflow": _cmd_health_dataflow,
    "health-daily": _cmd_health_daily,
    "alert-calibrate": _cmd_alert_calibrate,
    "alert-test": _cmd_alert_test,
    "alert-check": _cmd_alert_check,
    "massive-daily": _cmd_massive_daily,
}


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="heber",
        description="Heber Data Lakehouse CLI",
    )
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"heber {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Info command
    info_parser = subparsers.add_parser("info", help="Show Heber info")
    info_parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    # Datasets command
    datasets_parser = subparsers.add_parser("datasets", help="List datasets")
    datasets_parser.add_argument("--layer", choices=["bronze", "silver", "gold"], help="Filter by layer")

    # Versions command
    versions_parser = subparsers.add_parser("versions", help="List Gold versions")
    versions_parser.add_argument("dataset", help="Dataset name")

    # Backfill command
    backfill_parser = subparsers.add_parser("backfill", help="Backfill Silver from Bronze")
    backfill_parser.add_argument("--feed", help="Specific feed to backfill")
    backfill_parser.add_argument("--since", help="Start date (YYYY-MM-DD)")
    backfill_parser.add_argument("--until", help="End date (YYYY-MM-DD)")

    from heber.config import get_settings

    s = get_settings()
    health_dataflow_parser = subparsers.add_parser("health-dataflow", help="Verify Gateway->Ingest->Storage flow")
    health_dataflow_parser.add_argument("--window-seconds", type=int, default=s.health_freshness_seconds)
    health_dataflow_parser.add_argument("--consumer-metrics-url", default=s.health_consumer_metrics_url)
    health_dataflow_parser.add_argument("--watch-metrics-url", default=s.health_watch_metrics_url)
    health_dataflow_parser.add_argument("--report-dir", default=str(s.health_report_dir))
    health_dataflow_parser.add_argument("--loop", action="store_true")
    health_dataflow_parser.add_argument("--interval-seconds", type=int, default=s.health_interval_seconds)
    health_dataflow_parser.add_argument("--mode", choices=["manual", "scheduled"], default="manual")

    # Daily health report
    health_daily_parser = subparsers.add_parser("health-daily", help="Run daily end-of-day health report")
    health_daily_parser.add_argument("--date", help="Report date (YYYY-MM-DD, default: today)")
    health_daily_parser.add_argument("--force", action="store_true", help="Run even on non-trading days")
    health_daily_parser.add_argument("--verbose", "-v", action="store_true", help="Print full JSON report")

    # Alert calibrate command
    alert_cal_parser = subparsers.add_parser("alert-calibrate", help="Suggest liveness floors from history")
    alert_cal_parser.add_argument("--days-back", type=int, default=50, help="Reference day age in days")
    alert_cal_parser.add_argument("--ratio", type=float, default=0.3, help="Floor as fraction of median rate")

    # Alert test command
    alert_test_parser = subparsers.add_parser("alert-test", help="Send a test Discord alert")
    alert_test_parser.add_argument("--message", help="Custom message text", default=None)

    # Alert check command (one-shot; scheduled via launchd StartInterval)
    subparsers.add_parser("alert-check", help="Run one liveness cycle and alert on criticals")

    massive_daily_parser = subparsers.add_parser(
        "massive-daily",
        help="Sync Massive stock aggregates and daily corporate actions into raw storage",
    )
    massive_daily_parser.add_argument("--date", help="One trading date to sync (YYYY-MM-DD)")
    massive_daily_parser.add_argument("--start-date", help="First trading date for catch-up sync (YYYY-MM-DD)")
    massive_daily_parser.add_argument("--end-date", help="Last trading date for catch-up sync (YYYY-MM-DD)")
    massive_daily_parser.add_argument(
        "--archive-root",
        default=str(s.data_root / "_vendor_raw" / "massive"),
        help="Massive raw archive root",
    )
    massive_daily_parser.add_argument("--bucket", default="flatfiles", help="Massive S3 bucket name")
    massive_daily_parser.add_argument(
        "--endpoint-url",
        default="https://files.massive.com",
        help="Massive S3-compatible endpoint URL",
    )
    massive_daily_parser.add_argument(
        "--api-key",
        default=None,
        help="Massive REST API key; defaults to MASSIVE_API_KEY",
    )
    massive_daily_parser.add_argument(
        "--skip-corporate-actions",
        action="store_true",
        help="Only sync flat files; skip splits/dividends/ticker snapshot",
    )

    args = parser.parse_args()

    handler = _SUBCOMMAND_HANDLERS.get(args.command)
    if handler:
        return handler(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
