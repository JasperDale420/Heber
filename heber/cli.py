"""Heber CLI - Command line interface for Heber Data Lakehouse."""

import argparse
import json
import sys

from heber import __version__


def _cmd_info(args: argparse.Namespace) -> int:
    """Handle the 'info' subcommand."""
    print(f"Heber Data Lakehouse v{__version__}")
    if args.verbose:
        print("\nComponents:")
        print("  - Storage: Apache Iceberg")
        print("  - Versioning: lakeFS")
        print("  - Schema Registry: Apicurio")
        print("  - Catalog: OpenMetadata")
    return 0


def _cmd_datasets(args: argparse.Namespace) -> int:
    """Handle the 'datasets' subcommand."""
    try:
        from heber.sdk.client import HeberClient

        client = HeberClient()
        datasets = client.list_datasets(layer=args.layer)
        for ds in datasets:
            print(f"  {ds.get('name', ds)}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def _cmd_versions(args: argparse.Namespace) -> int:
    """Handle the 'versions' subcommand."""
    try:
        from heber.sdk.client import HeberClient

        client = HeberClient()
        versions = client.list_gold_versions(args.dataset)
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
    from heber.ops import dataflow_health

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


_SUBCOMMAND_HANDLERS = {
    "info": _cmd_info,
    "datasets": _cmd_datasets,
    "versions": _cmd_versions,
    "backfill": _cmd_backfill,
    "health-dataflow": _cmd_health_dataflow,
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

    args = parser.parse_args()

    handler = _SUBCOMMAND_HANDLERS.get(args.command)
    if handler:
        return handler(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
