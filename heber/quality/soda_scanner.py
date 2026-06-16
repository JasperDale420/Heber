"""Soda Core Data Quality Integration.

This module provides data quality validation using Soda Core,
replacing custom quality contracts with SodaCL YAML configurations.

Phase 3 of OSS Migration Roadmap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram

from heber.config import settings

logger = structlog.get_logger(__name__)

# Metrics
quality_scans = Counter(
    "heber_quality_scans_total",
    "Total Soda quality scans",
    ["dataset", "status"],
)

quality_checks_passed = Gauge(
    "heber_quality_checks_passed",
    "Number of checks passed in last scan",
    ["dataset"],
)

quality_checks_failed = Gauge(
    "heber_quality_checks_failed",
    "Number of checks failed in last scan",
    ["dataset"],
)

quality_scan_duration = Histogram(
    "heber_quality_scan_duration_seconds",
    "Time to run Soda scan",
    ["dataset"],
    buckets=[1, 5, 10, 30, 60, 120],
)


@dataclass
class SodaConfig:
    """Soda Core configuration.

    Environment Variables:
        SODA_CHECKS_DIR: Directory containing check YAML files
        HEBER_SILVER_PATH: Path to Silver Parquet files (for DuckDB)
    """

    checks_dir: Path = Path("soda/checks")
    silver_path: Path = field(default_factory=lambda: settings.silver_path)

    @classmethod
    def from_env(cls) -> SodaConfig:
        """Load configuration from environment."""
        return cls(
            checks_dir=settings.soda_checks_dir,
            silver_path=settings.silver_path,
        )


@dataclass
class ScanResult:
    """Results from a Soda scan."""

    dataset: str
    passed: int
    warned: int
    failed: int
    errors: int
    duration_seconds: float
    timestamp: datetime
    details: list[dict[str, Any]]

    @property
    def success(self) -> bool:
        """Check if scan passed (no failures or errors)."""
        return self.failed == 0 and self.errors == 0


class SodaQualityScanner:
    """Data quality scanner using Soda Core.

    Uses Soda's SodaCL checks for data quality validation.

    Example:
        scanner = SodaQualityScanner()

        # Run checks for a dataset
        result = scanner.scan("bars")

        if not result.success:
            logger.error("Quality check failed", failures=result.failed)
    """

    def __init__(self, config: SodaConfig | None = None) -> None:
        """Initialize the scanner.

        Args:
            config: Optional configuration. If None, loads from environment.
        """
        if config is None:
            config = SodaConfig.from_env()

        self.config = config
        self._soda_scan: Any = None

    def _get_check_file(self, dataset: str) -> Path:
        """Get the check file path for a dataset."""
        return self.config.checks_dir / f"{dataset}.yaml"

    def scan(
        self,
        dataset: str,
        variables: dict[str, str] | None = None,
    ) -> ScanResult:
        """Run Soda checks for a dataset.

        Args:
            dataset: Dataset name (e.g., "bars", "quotes")
            variables: Optional variables to pass to checks

        Returns:
            ScanResult with check outcomes
        """
        from soda.scan import Scan

        start_time = datetime.now(UTC)

        # Create scan
        scan = Scan()
        scan.set_scan_definition_name(f"heber_{dataset}")
        scan.set_data_source_name("heber_silver")

        # Add DuckDB data source configuration inline
        scan.add_configuration_yaml_str(f"""
data_source heber_silver:
  type: duckdb
  path: ":memory:"
  files:
    - "{self.config.silver_path / dataset / "**/*.parquet"}"
""")

        # Add check file
        check_file = self._get_check_file(dataset)
        if check_file.exists():
            scan.add_sodacl_yaml_file(str(check_file))
        else:
            logger.warning("check_file_not_found", dataset=dataset, path=str(check_file))

        # Add variables
        if variables:
            for key, value in variables.items():
                scan.add_variables({key: value})

        # Add current time for freshness checks
        scan.add_variables({"CURRENT_TIME": datetime.now(UTC).isoformat()})

        try:
            # Execute scan
            scan.execute()

            duration = (datetime.now(UTC) - start_time).total_seconds()

            # Parse results
            check_results = scan.get_checks_fail() + scan.get_checks_warn() + scan.get_checks_pass()

            result = ScanResult(
                dataset=dataset,
                passed=len(scan.get_checks_pass()),
                warned=len(scan.get_checks_warn()),
                failed=len(scan.get_checks_fail()),
                errors=1 if scan.has_error_logs() else 0,
                duration_seconds=duration,
                timestamp=datetime.now(UTC),
                details=[
                    {
                        "name": check.name,
                        "outcome": check.outcome.name if check.outcome else "unknown",
                    }
                    for check in check_results
                ],
            )

            # Update metrics
            status = "success" if result.success else "failure"
            quality_scans.labels(dataset=dataset, status=status).inc()
            quality_checks_passed.labels(dataset=dataset).set(result.passed)
            quality_checks_failed.labels(dataset=dataset).set(result.failed)
            quality_scan_duration.labels(dataset=dataset).observe(duration)

            logger.info(
                "soda_scan_completed",
                dataset=dataset,
                passed=result.passed,
                warned=result.warned,
                failed=result.failed,
                duration_ms=int(duration * 1000),
            )

            return result

        except Exception as e:
            duration = (datetime.now(UTC) - start_time).total_seconds()
            quality_scans.labels(dataset=dataset, status="error").inc()
            logger.error("soda_scan_failed", dataset=dataset, error=str(e), exc_info=True)

            return ScanResult(
                dataset=dataset,
                passed=0,
                warned=0,
                failed=0,
                errors=1,
                duration_seconds=duration,
                timestamp=datetime.now(UTC),
                details=[{"error": str(e)}],
            )

    def scan_all(
        self,
        datasets: list[str] | None = None,
    ) -> dict[str, ScanResult]:
        """Run scans for multiple datasets.

        Args:
            datasets: List of dataset names. If None, scans all with check files.

        Returns:
            Dictionary of dataset -> ScanResult
        """
        if datasets is None:
            # Discover datasets from check files
            if self.config.checks_dir.exists():
                datasets = [f.stem for f in self.config.checks_dir.glob("*.yaml")]
            else:
                datasets = []

        results = {}
        for dataset in datasets:
            results[dataset] = self.scan(dataset)

        return results
