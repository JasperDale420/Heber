"""Persist health check results and baselines as Parquet in the Gold layer."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.health_monitor.models import CheckResult

logger = structlog.get_logger(__name__)

HEALTH_DATASET = "data_health"
BASELINE_DATASET = "data_health_baselines"

HEALTH_SCHEMA = pa.schema(
    [
        ("check_name", pa.string()),
        ("feed", pa.string()),
        ("severity", pa.string()),
        ("status", pa.string()),
        ("message", pa.string()),
        ("details_json", pa.string()),
        ("ts_checked", pa.timestamp("us", tz="UTC")),
        ("instrument_key", pa.string()),
    ]
)


class HealthStore:
    """Read and write health check results and baselines to the Gold layer."""

    def __init__(self, data_root: Path | None = None) -> None:
        if data_root is None:
            from heber.config import get_settings

            data_root = get_settings().data_root
        self.data_root = Path(data_root)

    def _health_partition(self, report_date: date) -> Path:
        return self.data_root / "gold" / f"dataset={HEALTH_DATASET}" / f"dt={report_date.isoformat()}"

    def _baseline_partition(self, report_date: date) -> Path:
        return self.data_root / "gold" / f"dataset={BASELINE_DATASET}" / f"dt={report_date.isoformat()}"

    def write_results(self, results: list[CheckResult], report_date: date) -> None:
        """Write health check results as a Parquet file in the Gold layer."""
        if not results:
            return
        rows = [r.to_flat_row() for r in results]
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, schema=HEALTH_SCHEMA)
        partition = self._health_partition(report_date)
        partition.mkdir(parents=True, exist_ok=True)
        out_path = partition / f"health_{uuid4().hex[:8]}.parquet"
        pq.write_table(table, out_path)
        logger.info("health_results_written", path=str(out_path), count=len(results))

    def read_results(self, report_date: date) -> pd.DataFrame:
        """Read health check results for a given date."""
        partition = self._health_partition(report_date)
        if not partition.exists():
            return pd.DataFrame(columns=[f.name for f in HEALTH_SCHEMA])
        files = list(partition.glob("*.parquet"))
        if not files:
            return pd.DataFrame(columns=[f.name for f in HEALTH_SCHEMA])
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    def write_baseline(self, df: pd.DataFrame, report_date: date) -> None:
        """Write volume/statistics baseline data for a given date."""
        partition = self._baseline_partition(report_date)
        partition.mkdir(parents=True, exist_ok=True)
        out_path = partition / "baseline.parquet"
        df.to_parquet(out_path, index=False)

    def read_baselines(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Read baseline data across a date range."""
        frames: list[pd.DataFrame] = []
        current = start_date
        while current <= end_date:
            partition = self._baseline_partition(current)
            if partition.exists():
                for f in partition.glob("*.parquet"):
                    frame = pd.read_parquet(f)
                    frame["dt"] = current.isoformat()
                    frames.append(frame)
            current += timedelta(days=1)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
