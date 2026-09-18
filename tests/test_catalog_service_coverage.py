"""Unit tests for CatalogService.update_coverage's merge logic.

update_coverage is the only place that widens a DataCoverage row's date
range and accumulates its row count as new partitions land. Neither branch
(merge into an existing row vs. create a fresh one) had any test coverage,
so a regression here (e.g. narrowing the range instead of widening it, or
overwriting the row count instead of accumulating it) would ship silently.

Uses a mocked AsyncSession in the same style as
tests/test_coverage_skips_unchanged_partitions.py's seed_coverage_from_disk
tests, since CatalogService requires a real AsyncSession type.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from heber.catalog.db import DataCoverage
from heber.catalog.service import CatalogService

pytestmark = pytest.mark.unit


def _service_with_existing(existing: DataCoverage | None) -> tuple[CatalogService, MagicMock]:
    session = MagicMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.add = MagicMock()
    return CatalogService(session), session


async def test_update_coverage_widens_date_range_and_accumulates_row_count() -> None:
    """An existing row's dt_min/dt_max must widen (never narrow), and its row
    count must accumulate rather than being overwritten."""
    existing = DataCoverage(
        dataset_name="bars",
        instrument_key="equity:AAPL",
        dt_min=date(2026, 1, 5),
        dt_max=date(2026, 1, 10),
        approx_row_count=50,
    )
    service, session = _service_with_existing(existing)

    result = await service.update_coverage(
        dataset_name="bars",
        instrument_key="equity:AAPL",
        dt_min=date(2026, 1, 1),  # earlier than existing dt_min
        dt_max=date(2026, 1, 8),  # earlier than existing dt_max
        approx_row_count=20,
    )

    assert result.dt_min == date(2026, 1, 1), "dt_min must widen to the earlier date"
    assert result.dt_max == date(2026, 1, 10), "dt_max must not narrow to an earlier date"
    assert result.approx_row_count == 70, "row count must accumulate, not overwrite"
    session.commit.assert_awaited_once()


async def test_update_coverage_row_count_untouched_when_not_provided() -> None:
    """Passing approx_row_count=None must leave the existing count as-is."""
    existing = DataCoverage(
        dataset_name="bars",
        instrument_key="equity:AAPL",
        dt_min=date(2026, 1, 5),
        dt_max=date(2026, 1, 10),
        approx_row_count=50,
    )
    service, _session = _service_with_existing(existing)

    result = await service.update_coverage(
        dataset_name="bars",
        instrument_key="equity:AAPL",
        dt_min=date(2026, 1, 1),
        dt_max=date(2026, 1, 12),
        approx_row_count=None,
    )

    assert result.approx_row_count == 50


async def test_update_coverage_creates_new_row_when_none_exists() -> None:
    """When no coverage row exists yet, a new one is added rather than merged."""
    service, session = _service_with_existing(None)

    await service.update_coverage(
        dataset_name="quotes",
        instrument_key="equity:MSFT",
        dt_min=date(2026, 2, 1),
        dt_max=date(2026, 2, 1),
        approx_row_count=10,
    )

    session.add.assert_called_once()
    added = session.add.call_args[0][0]
    assert isinstance(added, DataCoverage)
    assert added.dataset_name == "quotes"
    assert added.instrument_key == "equity:MSFT"
    assert added.dt_min == date(2026, 2, 1)
    assert added.approx_row_count == 10
