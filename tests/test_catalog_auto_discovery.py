"""Tests for catalog auto-discovery of Silver datasets."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from heber.catalog.db import Dataset, FeedMapping


@pytest.fixture
def silver_dir(tmp_path: Path) -> Path:
    """Create a mock Silver directory with feed partitions.

    The silver_path property is ``data_root / "silver"``, so we create
    ``tmp_path / silver`` and set ``data_root`` to ``tmp_path``.
    """
    silver = tmp_path / "silver"
    silver.mkdir()
    # Known feed (in SILVER_SCHEMAS)
    (silver / "feed=bars").mkdir()
    (silver / "feed=bars" / "instrument_type=stock").mkdir()
    (silver / "feed=bars" / "instrument_type=stock" / "dt=2024-01-01").mkdir()
    # Unknown feed (not in SILVER_SCHEMAS)
    (silver / "feed=custom_alpha").mkdir()
    (silver / "feed=custom_alpha" / "instrument_type=stock").mkdir()
    # macOS resource fork to ignore
    (silver / "._resource").mkdir()
    # Non-feed directory to ignore
    (silver / "some_other_dir").mkdir()
    return tmp_path  # return the data_root, not silver itself


@pytest.fixture
def mock_session():
    """Create a mock async session with configurable query results."""
    session = AsyncMock()
    return session


def _patch_data_root(monkeypatch: pytest.MonkeyPatch, data_root: Path) -> None:
    """Monkeypatch settings.data_root so silver_path resolves correctly."""
    from heber.catalog import seeds

    monkeypatch.setattr(seeds.settings, "data_root", data_root)


def _build_session(mock_result: MagicMock | None = None) -> AsyncMock:
    """Build an async session mock with sync add() semantics."""
    session = AsyncMock()
    session.execute = AsyncMock() if mock_result is None else AsyncMock(return_value=mock_result)
    session.add = MagicMock()
    return session


class TestDiscoverDatasetsFromDisk:
    """Tests for the discover_datasets_from_disk function."""

    @pytest.mark.asyncio
    async def test_discovers_unknown_feeds(self, silver_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown feed dirs on disk get registered as datasets + feed mappings."""
        from heber.catalog import seeds

        _patch_data_root(monkeypatch, silver_dir)

        # Simulate DB already has "bars" but not "custom_alpha"
        mock_result = MagicMock()
        mock_result.all.return_value = [("bars",)]

        session = _build_session(mock_result)

        count = await seeds.discover_datasets_from_disk(session)

        assert count == 1
        # Should add 2 items: 1 Dataset + 1 FeedMapping
        assert session.add.call_count == 2

        added_objects = [call.args[0] for call in session.add.call_args_list]
        dataset = next(obj for obj in added_objects if isinstance(obj, Dataset))
        mapping = next(obj for obj in added_objects if isinstance(obj, FeedMapping))

        assert dataset.dataset_name == "custom_alpha"
        assert dataset.layer == "silver"
        assert dataset.is_active is True

        assert mapping.provider == "discovered"
        assert mapping.gateway_feed == "custom_alpha"
        assert mapping.silver_dataset_name == "custom_alpha"

        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_new_feeds(self, silver_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When all disk feeds are already registered, nothing is added."""
        from heber.catalog import seeds

        _patch_data_root(monkeypatch, silver_dir)

        # DB already has both feeds
        mock_result = MagicMock()
        mock_result.all.return_value = [("bars",), ("custom_alpha",)]

        session = _build_session(mock_result)

        count = await seeds.discover_datasets_from_disk(session)

        assert count == 0
        session.add.assert_not_called()
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_silver_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When silver_path doesn't exist, returns 0 gracefully."""
        from heber.catalog import seeds

        # Point to a data_root with no "silver" subdir
        nonexistent = tmp_path / "no_data"
        nonexistent.mkdir()
        _patch_data_root(monkeypatch, nonexistent)

        session = _build_session()
        count = await seeds.discover_datasets_from_disk(session)

        assert count == 0
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ignores_macos_resource_forks(self, silver_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """macOS resource fork dirs (._*) are skipped."""
        from heber.catalog import seeds

        _patch_data_root(monkeypatch, silver_dir)

        # DB has bars and custom_alpha already
        mock_result = MagicMock()
        mock_result.all.return_value = [("bars",), ("custom_alpha",)]

        session = _build_session(mock_result)

        count = await seeds.discover_datasets_from_disk(session)

        assert count == 0

    @pytest.mark.asyncio
    async def test_ignores_non_feed_dirs(self, silver_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Directories not matching feed=X pattern are skipped."""
        from heber.catalog import seeds

        _patch_data_root(monkeypatch, silver_dir)

        # Only bars and custom_alpha should be found, not "some_other_dir"
        mock_result = MagicMock()
        mock_result.all.return_value = [("bars",), ("custom_alpha",)]

        session = _build_session(mock_result)

        count = await seeds.discover_datasets_from_disk(session)

        assert count == 0

    @pytest.mark.asyncio
    async def test_empty_silver_dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty silver directory produces zero results."""
        from heber.catalog import seeds

        # Create data_root with empty silver subdir
        data_root = tmp_path / "empty_data"
        (data_root / "silver").mkdir(parents=True)
        _patch_data_root(monkeypatch, data_root)

        session = _build_session()
        count = await seeds.discover_datasets_from_disk(session)

        assert count == 0
        session.execute.assert_not_awaited()
