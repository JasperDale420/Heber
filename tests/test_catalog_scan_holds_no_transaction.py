"""The coverage scan must not hold a transaction open while it walks the disk.

`seed_coverage_from_disk` interleaved work: for each feed it ran a blocking
`rglob` + Parquet-footer scan, then issued upserts on a session shared with
`discover_datasets_from_disk`, committing once at the very end. The transaction
therefore sat idle for the length of every feed's scan, and Postgres here runs
`idle_in_transaction_session_timeout = 5min`.

232 of 235 discovery passes failed in a single container lifetime, always at an
upsert, and `data_coverage` has been frozen since 2026-07-20.

Whatever severs the connection — the idle timeout, a pool recycle, the network
— the fix is the same shape: do all the walking first, then open a transaction
and write. These tests pin that ordering rather than the diagnosis.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.catalog import seeds


def _feed(base: Path, feed: str, dt: str, rows: int = 3) -> None:
    part = base / f"feed={feed}" / "instrument_type=equity" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(pd.DataFrame({"x": range(rows)}))
    pq.write_table(table, str(part / "part.parquet"))


@pytest.fixture
def silver(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "silver"
    for feed in ("alpha", "beta", "gamma"):
        _feed(root, feed, "2026-08-01")
        _feed(root, feed, "2026-08-02")
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)
    return root


def _recording_session(order: list[str]) -> MagicMock:
    session = MagicMock()

    async def _execute(*_a, **_k):
        order.append("sql")
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        return result

    async def _commit():
        order.append("commit")

    session.execute = AsyncMock(side_effect=_execute)
    session.commit = AsyncMock(side_effect=_commit)
    session.add = MagicMock()
    return session


async def test_all_scanning_precedes_any_sql(silver: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No statement may run until every feed has been walked.

    While SQL is interleaved with scanning, the transaction opened by the first
    upsert stays idle across every later scan.
    """
    order: list[str] = []
    real_scan = seeds._scan_partition_dates

    def _spy(entry):
        order.append("scan")
        return real_scan(entry)

    monkeypatch.setattr(seeds, "_scan_partition_dates", _spy)

    await seeds.seed_coverage_from_disk(_recording_session(order))

    assert "scan" in order and "sql" in order, f"nothing happened: {order}"
    last_scan = max(i for i, step in enumerate(order) if step == "scan")
    first_sql = min(i for i, step in enumerate(order) if step == "sql")
    assert last_scan < first_sql, (
        f"SQL ran before scanning finished, so the transaction idles during later scans: {order[: first_sql + 3]}"
    )


async def test_every_feed_is_still_recorded(silver: Path) -> None:
    """Reordering must not lose coverage rows."""
    session = _recording_session([])

    upserted = await seeds.seed_coverage_from_disk(session)

    # 3 feeds x (1 aggregate + 2 per-date rows)
    assert upserted == 9


async def test_scan_failure_does_not_leave_a_transaction_open(silver: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A walk that raises must not have started a transaction it cannot finish."""
    order: list[str] = []

    def _boom(_entry):
        order.append("scan")
        raise OSError("volume vanished mid-scan")

    monkeypatch.setattr(seeds, "_scan_partition_dates", _boom)
    session = _recording_session(order)

    with pytest.raises(OSError):
        await seeds.seed_coverage_from_disk(session)

    assert "sql" not in order, f"a statement ran despite the scan failing: {order}"
