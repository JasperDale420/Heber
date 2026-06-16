#!/usr/bin/env python3
"""Sweep missing Massive REST datasets into vendor-raw storage."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from heber.backfill.massive.rest_backlog import DEFAULT_DATASETS, sweep_default_datasets


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture Massive REST backlog pages as raw gzip JSON")
    parser.add_argument(
        "--out-root",
        default="/Volumes/heber/data/_vendor_raw/massive/massive_rest_backlog",
        help="Output directory for raw REST backlog pages",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=list(DEFAULT_DATASETS),
        choices=list(DEFAULT_DATASETS),
        help="Dataset names to sweep",
    )
    parser.add_argument("--force", action="store_true", help="Delete existing pages for selected datasets and re-run")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after N new pages per dataset")
    args = parser.parse_args()

    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        print("MASSIVE_API_KEY is required", file=sys.stderr)
        return 2

    summaries = sweep_default_datasets(
        api_key=api_key,
        out_root=Path(args.out_root),
        dataset_names=args.datasets,
        force=args.force,
        max_pages=args.max_pages,
    )
    print(json.dumps([summary.__dict__ | {"out_dir": str(summary.out_dir)} for summary in summaries], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
