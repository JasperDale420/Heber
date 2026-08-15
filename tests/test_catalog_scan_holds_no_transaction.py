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
        result.all.return_value = []
        return result

    async def _commit():
        order.append("commit")

    async def _rollback():
        order.append("rollback")

    session.execute = AsyncMock(side_effect=_execute)
    session.commit = AsyncMock(side_effect=_commit)
    session.rollback = AsyncMock(side_effect=_rollback)
    session.add = MagicMock()
    return session


async def test_no_transaction_is_held_open_across_the_walk(silver: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No transaction may still be open when the next feed's walk begins.

    This pins the property, not one implementation of it. It used to require
    that no statement ran between the first and last scan at all — walk
    everything, then write. That does satisfy the property, but it also meant a
    pass that could not finish wrote nothing: on the production mount the walk
    is ~20h for ``feed=quotes`` alone, so coverage sat 14h stale through a scan
    that was working the whole time, and every restart began again at the first
    feed. Writing each feed and committing it before the next walk starts keeps
    the transaction just as short while making progress durable.

    What must never happen is a statement left uncommitted across a scan — that
    is the idle transaction Postgres kills at ``idle_in_transaction_session_timeout``,
    which failed 232 of 235 passes in a single container lifetime.
    """
    order: list[str] = []
    real_scan = seeds._scan_partition_dates

    def _spy(entry, recorded=None):
        order.append("scan")
        return real_scan(entry, recorded)

    monkeypatch.setattr(seeds, "_scan_partition_dates", _spy)

    await seeds.seed_coverage_from_disk(_recording_session(order))

    assert "scan" in order and "sql" in order, f"nothing happened: {order}"

    open_since: int | None = None
    for i, step in enumerate(order):
        if step == "sql":
            open_since = open_since if open_since is not None else i
        elif step in ("commit", "rollback"):
            open_since = None
        elif step == "scan":
            assert open_since is None, (
                f"a transaction opened at step {open_since} was still open when a walk "
                f"started at step {i} — it would idle for the length of that walk: {order}"
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

    def _boom(_entry, _recorded=None):
        order.append("scan")
        raise OSError("volume vanished mid-scan")

    monkeypatch.setattr(seeds, "_scan_partition_dates", _boom)
    session = _recording_session(order)

    upserted = await seeds.seed_coverage_from_disk(session)

    assert upserted == 0
    # The pre-walk read is the only statement, and it was closed before the
    # walk began — nothing is left open for the failure to strand.
    assert order.count("sql") == 1, f"more than the recorded-coverage read ran: {order}"
    assert order.index("rollback") < order.index("scan"), f"read not closed before the walk: {order}"
    assert "commit" not in order, f"a write committed despite the scan failing: {order}"


async def test_one_feed_failing_does_not_discard_the_others(silver: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A feed whose walk raises is logged and skipped; the rest still land.

    Phase 2 already commits per feed so a late failure keeps what was written.
    Phase 1 needs the same property: without it one exotic error in the first
    feed throws away every feed behind it, and coverage stays frozen until the
    staleness alarm fires — the exact outcome this scan exists to avoid.
    """
    real_scan = seeds._scan_partition_dates

    def _boom_on_alpha(entry: Path, recorded=None):
        if entry.name == "feed=alpha":
            raise RuntimeError("something exotic")
        return real_scan(entry, recorded)

    monkeypatch.setattr(seeds, "_scan_partition_dates", _boom_on_alpha)

    upserted = await seeds.seed_coverage_from_disk(_recording_session([]))

    # beta and gamma survive: 2 feeds x (1 aggregate + 2 per-date rows)
    assert upserted == 6
