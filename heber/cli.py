"""Heber CLI - Command line interface for Heber Data Lakehouse."""

import argparse
import sys

from heber import __version__


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

    args = parser.parse_args()

    if args.command == "info":
        print(f"Heber Data Lakehouse v{__version__}")
        if args.verbose:
            print("\nComponents:")
            print("  - Storage: Apache Iceberg")
            print("  - Versioning: lakeFS")
            print("  - Schema Registry: Apicurio")
            print("  - Catalog: OpenMetadata")
        return 0

    elif args.command == "datasets":
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

    elif args.command == "versions":
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

    elif args.command == "backfill":
        import asyncio
        from datetime import datetime

        from heber.writer.transformer import BronzeToSilverTransformer

        since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None
        until = datetime.strptime(args.until, "%Y-%m-%d") if args.until else None

        transformer = BronzeToSilverTransformer()

        if args.feed:
            print(f"Backfilling feed: {args.feed}")
            count = asyncio.run(transformer.transform(args.feed, since=since, until=until))
            print(f"Transformed {count} records")
        else:
            print("Backfilling all feeds from Bronze to Silver...")
            stats = asyncio.run(transformer.transform_all(since=since, until=until))
            for feed, count in sorted(stats.items()):
                print(f"  {feed}: {count} records")
        return 0

    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
