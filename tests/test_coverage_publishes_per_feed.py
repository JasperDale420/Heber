"""Coverage must be published per feed, not withheld until every feed is walked.

The walk is the whole cost of a pass — ``feed=quotes`` alone is hours on the
production mount — so deferring all writes to the end meant a pass that could
not finish published nothing at all. Coverage sat 14h stale through a scan that
was working the entire time, and each restart began again at the first feed, so
feeds late in the alphabet were never reached.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from heber.catalog import seeds

pytestmark = pytest.mark.unit


class _Session:
    """Records the interleaving of writes and commits."""

    def __init__(self):
        self.events: list[str] = []

    async def commit(self):
        self.events.append("commit")


@pytest.fixture
def _fake_lakehouse(tmp_path, monkeypatch):
    silver = tmp_path / "silver"
    for feed in ("aaa", "mmm", "zzz"):
        (silver / f"feed={feed}").mkdir(parents=True)
    # silver_path is derived from data_root, so patch the root.
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)
    return silver


async def test_each_feed_is_committed_as_it_is_walked(_fake_lakehouse):
    session = _Session()
    walked: list[str] = []

    def scan(feed_dir: Path, _recorded):
        walked.append(feed_dir.name)
        session.events.append(f"walk:{feed_dir.name}")
        return [("2026-08-15", 10)]

    async def upsert(_s, feed_name, **_kw):
        session.events.append(f"write:{feed_name}")
        return 1

    with (
        patch.object(seeds, "_scan_partition_dates", scan),
        patch.object(seeds, "_upsert_coverage", upsert),
        patch.object(seeds, "_load_recorded_coverage", return_value={}),
    ):
        await seeds.seed_coverage_from_disk(session, reuse_recorded=False)

    # The last feed's walk must come AFTER the first feed's commit — otherwise a
    # pass that dies partway through leaves nothing recorded.
    first_commit = session.events.index("commit")
    last_walk = max(i for i, e in enumerate(session.events) if e.startswith("walk:"))
    assert first_commit < last_walk, f"all writes deferred to the end: {session.events}"


async def test_a_feed_that_cannot_be_walked_keeps_the_others(_fake_lakehouse):
    """One unwalkable feed must not discard the feeds already published."""
    session = _Session()

    def scan(feed_dir: Path, _recorded):
        if "mmm" in feed_dir.name:
            raise OSError("mount blipped")
        return [("2026-08-15", 10)]

    async def upsert(_s, feed_name, **_kw):
        session.events.append(f"write:{feed_name}")
        return 1

    with (
        patch.object(seeds, "_scan_partition_dates", scan),
        patch.object(seeds, "_upsert_coverage", upsert),
        patch.object(seeds, "_load_recorded_coverage", return_value={}),
    ):
        upserted = await seeds.seed_coverage_from_disk(session, reuse_recorded=False)

    written = {e.split(":", 1)[1] for e in session.events if e.startswith("write:")}
    assert written == {"aaa", "zzz"}
    # Two rows per surviving feed: the `__all__` aggregate and one `dt:` row.
    assert upserted == 4
