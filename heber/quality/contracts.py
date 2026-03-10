"""Data Quality Contracts and Validation (PRD §33).

Provides data quality contracts, validation, and automated quality gates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import numpy as np
import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


class QualityMetric(StrEnum):
    """Quality metric types."""

    FILL_RATE = "fill_rate"
    NON_NULL_RATE = "non_null_rate"
    MAX_LAG_HOURS = "max_lag_hours"
    MAX_GAP_SECONDS = "max_gap_seconds"
    VALUE_RANGE = "value_range"


@dataclass
class QualityContract:
    """A single data quality contract (PRD §33.1).

    Attributes:
        metric: The quality metric type
        threshold: Min or max value depending on metric
        columns: Columns to check (for non_null_rate)
        description: Human-readable description
    """

    metric: QualityMetric
    threshold: float
    columns: list[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric.value,
            "threshold": self.threshold,
            "columns": self.columns,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QualityContract:
        return cls(
            metric=QualityMetric(data["metric"]),
            threshold=data["threshold"],
            columns=data.get("columns", []),
            description=data.get("description", ""),
        )


@dataclass
class QualityViolation:
    """A quality contract violation."""

    contract: str
    metric: QualityMetric
    actual: float
    expected: float
    affected_symbols: list[str] = field(default_factory=list)
    affected_dates: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "metric": self.metric.value,
            "actual": self.actual,
            "expected": self.expected,
            "affected_symbols": self.affected_symbols,
            "affected_dates": self.affected_dates,
        }


@dataclass
class QualityReport:
    """Quality validation report (PRD §33.2)."""

    dataset: str
    layer: str
    time_range: tuple[datetime, datetime]
    passed: bool
    violations: list[QualityViolation] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "layer": self.layer,
            "time_range": [self.time_range[0].isoformat(), self.time_range[1].isoformat()],
            "passed": self.passed,
            "violations": [v.to_dict() for v in self.violations],
            "metrics": self.metrics,
        }


class DataQualityValidator:
    """Validate data against quality contracts (PRD §33)."""

    def __init__(self, contracts: dict[str, list[QualityContract]] | None = None):
        """Initialize validator with contracts.

        Args:
            contracts: Dict mapping dataset names to list of contracts
        """
        self.contracts = contracts or {}

    def add_contract(self, dataset: str, contract: QualityContract) -> None:
        """Add a quality contract for a dataset."""
        if dataset not in self.contracts:
            self.contracts[dataset] = []
        self.contracts[dataset].append(contract)

    def check_fill_rate(
        self,
        df: pd.DataFrame,
        expected_days: int,
    ) -> tuple[float, list[str], list[str]]:
        """Check fill rate (rows per symbol per day).

        Returns:
            Tuple of (fill_rate, affected_symbols, affected_dates)
        """
        if df.empty:
            return 0.0, [], []

        date_col = "ts_event" if "ts_event" in df.columns else df.columns[0]
        if pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df = df.copy()
            df["_date"] = df[date_col].dt.date
        else:
            df = df.copy()
            df["_date"] = pd.to_datetime(df[date_col]).dt.date

        symbol_col = "instrument_key" if "instrument_key" in df.columns else "symbol"
        if symbol_col not in df.columns:
            return 1.0, [], []

        # Count unique days per symbol
        day_counts = df.groupby(symbol_col)["_date"].nunique()
        fill_rates = day_counts / expected_days

        affected_symbols = fill_rates[fill_rates < 0.95].index.tolist()

        # Get affected dates
        all_dates = set(df["_date"].unique())
        affected_dates = []
        for symbol in affected_symbols:
            symbol_dates = set(df[df[symbol_col] == symbol]["_date"].unique())
            missing = all_dates - symbol_dates
            affected_dates.extend([str(d) for d in missing])

        return fill_rates.mean(), affected_symbols, list(set(affected_dates))

    def check_non_null_rate(
        self,
        df: pd.DataFrame,
        columns: list[str],
        threshold: float = 0.99,
    ) -> tuple[float, list[str]]:
        """Check non-null rate for specified columns.

        Returns:
            Tuple of (non_null_rate, columns_below_threshold)
        """
        if df.empty:
            return 0.0, columns

        existing_cols = [c for c in columns if c in df.columns]
        if not existing_cols:
            return 1.0, []

        rates = {}
        for col in existing_cols:
            rates[col] = df[col].notna().mean()

        mean_rate = np.mean(list(rates.values()))
        below_threshold = [c for c, r in rates.items() if r < threshold]

        return mean_rate, below_threshold

    def check_freshness(
        self,
        df: pd.DataFrame,
        max_lag_hours: float,
        current_time: datetime | None = None,
    ) -> tuple[float, bool]:
        """Check data freshness (max lag from current time).

        Returns:
            Tuple of (actual_lag_hours, is_fresh)
        """
        if df.empty:
            return float("inf"), False

        current_time = current_time or datetime.now(UTC)

        ts_col = "ts_event" if "ts_event" in df.columns else df.columns[0]
        if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            df = df.copy()
            df[ts_col] = pd.to_datetime(df[ts_col])

        latest = df[ts_col].max()
        if pd.isna(latest):
            return float("inf"), False

        if latest.tzinfo is None:
            latest = latest.tz_localize("UTC")

        lag = current_time - latest
        lag_hours = lag.total_seconds() / 3600

        return lag_hours, lag_hours <= max_lag_hours

    def check_gaps(
        self,
        df: pd.DataFrame,
        max_gap_seconds: float,
    ) -> tuple[float, list[str]]:
        """Check for data gaps.

        Returns:
            Tuple of (max_gap_found, dates_with_gaps)
        """
        if df.empty or len(df) < 2:
            return 0.0, []

        ts_col = "ts_event" if "ts_event" in df.columns else df.columns[0]
        df = df.sort_values(ts_col).copy()

        if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
            df[ts_col] = pd.to_datetime(df[ts_col])

        df["_gap"] = df[ts_col].diff().dt.total_seconds()

        max_gap = df["_gap"].max()
        if pd.isna(max_gap):
            return 0.0, []

        gap_rows = df[df["_gap"] > max_gap_seconds]
        dates_with_gaps = gap_rows[ts_col].dt.strftime("%Y-%m-%d").unique().tolist()

        return max_gap, dates_with_gaps

    def _validate_fill_rate(
        self,
        contract: QualityContract,
        df: pd.DataFrame,
        expected_days: int,
        metrics: dict[str, float],
        violations: list[QualityViolation],
    ) -> None:
        """Validate fill rate contract."""
        rate, symbols, dates = self.check_fill_rate(df, expected_days)
        metrics["fill_rate"] = rate

        if rate < contract.threshold:
            violations.append(
                QualityViolation(
                    contract="fill_rate",
                    metric=QualityMetric.FILL_RATE,
                    actual=rate,
                    expected=contract.threshold,
                    affected_symbols=symbols,
                    affected_dates=dates,
                )
            )

    def _validate_non_null_rate(
        self,
        contract: QualityContract,
        df: pd.DataFrame,
        metrics: dict[str, float],
        violations: list[QualityViolation],
    ) -> None:
        """Validate non-null rate contract."""
        rate, cols = self.check_non_null_rate(df, contract.columns, threshold=contract.threshold)
        metrics["non_null_rate"] = rate

        if rate < contract.threshold:
            violations.append(
                QualityViolation(
                    contract="completeness",
                    metric=QualityMetric.NON_NULL_RATE,
                    actual=rate,
                    expected=contract.threshold,
                    affected_symbols=cols,
                )
            )

    def _validate_freshness(
        self,
        contract: QualityContract,
        df: pd.DataFrame,
        metrics: dict[str, float],
        violations: list[QualityViolation],
    ) -> None:
        """Validate freshness contract."""
        lag, is_fresh = self.check_freshness(df, contract.threshold)
        metrics["lag_hours"] = lag

        if not is_fresh:
            violations.append(
                QualityViolation(
                    contract="freshness",
                    metric=QualityMetric.MAX_LAG_HOURS,
                    actual=lag,
                    expected=contract.threshold,
                )
            )

    def _validate_gaps(
        self,
        contract: QualityContract,
        df: pd.DataFrame,
        metrics: dict[str, float],
        violations: list[QualityViolation],
    ) -> None:
        """Validate gap duration contract."""
        gap, dates = self.check_gaps(df, contract.threshold)
        metrics["max_gap_seconds"] = gap

        if gap > contract.threshold:
            violations.append(
                QualityViolation(
                    contract="gap_duration",
                    metric=QualityMetric.MAX_GAP_SECONDS,
                    actual=gap,
                    expected=contract.threshold,
                    affected_dates=dates,
                )
            )

    def validate(
        self,
        dataset: str,
        layer: str,
        df: pd.DataFrame,
        time_range: tuple[datetime, datetime],
    ) -> QualityReport:
        """Validate data against all contracts for a dataset.

        Args:
            dataset: Dataset name
            layer: Data layer (bronze, silver, gold)
            df: DataFrame to validate
            time_range: Time range of the data

        Returns:
            QualityReport with pass/fail status and violations
        """
        violations: list[QualityViolation] = []
        metrics: dict[str, float] = {}

        contracts = self.contracts.get(dataset, [])

        # Calculate expected days from time range
        expected_days = (time_range[1] - time_range[0]).days

        for contract in contracts:
            if contract.metric == QualityMetric.FILL_RATE:
                self._validate_fill_rate(contract, df, expected_days, metrics, violations)
            elif contract.metric == QualityMetric.NON_NULL_RATE:
                self._validate_non_null_rate(contract, df, metrics, violations)
            elif contract.metric == QualityMetric.MAX_LAG_HOURS:
                self._validate_freshness(contract, df, metrics, violations)
            elif contract.metric == QualityMetric.MAX_GAP_SECONDS:
                self._validate_gaps(contract, df, metrics, violations)

        passed = len(violations) == 0

        logger.info(
            "Quality validation complete",
            dataset=dataset,
            layer=layer,
            passed=passed,
            num_violations=len(violations),
        )

        return QualityReport(
            dataset=dataset,
            layer=layer,
            time_range=time_range,
            passed=passed,
            violations=violations,
            metrics=metrics,
        )


# Default contracts for common datasets
DEFAULT_CONTRACTS: dict[str, list[QualityContract]] = {
    "bars": [
        QualityContract(
            metric=QualityMetric.FILL_RATE,
            threshold=0.95,
            description="At least 95% of expected trading days have data",
        ),
        QualityContract(
            metric=QualityMetric.NON_NULL_RATE,
            threshold=0.99,
            columns=["open", "high", "low", "close", "volume"],
            description="OHLCV columns 99% non-null",
        ),
        QualityContract(
            metric=QualityMetric.MAX_LAG_HOURS,
            threshold=2.0,
            description="Data available within 2 hours of market close",
        ),
        QualityContract(
            metric=QualityMetric.MAX_GAP_SECONDS,
            threshold=86400,
            description="No gaps longer than 1 trading day",
        ),
    ],
    "trades": [
        QualityContract(
            metric=QualityMetric.NON_NULL_RATE,
            threshold=0.99,
            columns=["price", "size", "ts_event"],
        ),
    ],
    "quotes": [
        QualityContract(
            metric=QualityMetric.NON_NULL_RATE,
            threshold=0.99,
            columns=["bid_px", "ask_px", "bid_sz", "ask_sz"],
        ),
    ],
}


def create_default_validator() -> DataQualityValidator:
    """Create a validator with default contracts."""
    return DataQualityValidator(contracts=DEFAULT_CONTRACTS)
