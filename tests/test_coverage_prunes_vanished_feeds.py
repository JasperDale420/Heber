"""Coverage rows for feeds that no longer exist on disk must not linger.

The table advertised `bars_1m` (5 months old), `data` and `stocks` (3.5 months)
long after those directories were gone. Besides being a lie about what the
lakehouse holds, it makes per-feed staleness unusable: the oldest coverage is
always a decommissioned feed, so a feed that genuinely stopped being scanned
can never be spotted.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from heber.catalog import seeds

pytestmark = pytest.mark.unit


class _Session:
    def __init__(self):
        self.deleted: list[str] = []
        self.committed = 0

    async def execute(self, stmt):
        self.deleted.append(str(stmt))

        class _R:
            rowcount = 1

            def scalar_one_or_none(self_inner):
                return None

            def all(self_inner):
                return []

        return _R()

    async def commit(self):
        self.committed += 1

    def add(self, _obj):
        pass


def _silver(tmp_path: Path, feeds: list[str]) -> Path:
    root = tmp_path / "silver"
    for f in feeds:
        (root / f"feed={f}").mkdir(parents=True)
    return root


async def test_a_vanished_feed_is_pruned(tmp_path, monkeypatch):
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)
    _silver(tmp_path, ["alpha"])
    session = _Session()

    # `zombie` has a coverage row but no directory left.
    pruned = await seeds._prune_vanished_feeds(session, {"alpha", "zombie"})

    assert pruned is True
    assert session.deleted, "no DELETE was issued for the vanished feeds"


async def test_nothing_is_pruned_when_every_feed_is_present(tmp_path, monkeypatch):
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)
    _silver(tmp_path, ["alpha", "beta"])
    session = _Session()

    pruned = await seeds._prune_vanished_feeds(session, {"alpha", "beta"})

    assert pruned is False
    assert not session.deleted


async def test_an_empty_listing_never_wipes_the_table(tmp_path, monkeypatch):
    """A mount blip returns no directories; that must not delete every row."""
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)
    session = _Session()

    with patch.object(seeds, "_present_feed_names", return_value=set()):
        pruned = await seeds._prune_vanished_feeds(session, {"alpha", "beta"})

    assert pruned is False
    assert not session.deleted, "an empty feed listing deleted coverage rows"


async def test_pruning_is_skipped_when_the_listing_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)
    session = _Session()

    with patch.object(seeds, "_present_feed_names", side_effect=OSError("mount gone")):
        pruned = await seeds._prune_vanished_feeds(session, {"alpha"})

    assert pruned is False
    assert not session.deleted
