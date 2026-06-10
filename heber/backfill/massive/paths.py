"""Archive layout helpers for Massive vendor-raw flat files."""

from datetime import date
from pathlib import Path


def archive_path(archive_root: str, dataset: str, d: date) -> Path:
    """{archive_root}/us_stocks_sip/{dataset}/YYYY/MM/YYYY-MM-DD.csv.gz"""
    return Path(archive_root) / "us_stocks_sip" / dataset / f"{d:%Y}" / f"{d:%m}" / f"{d:%Y-%m-%d}.csv.gz"
