"""Regression checks for Hot Store unified implementation paths."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_legacy_hotstore_facade_points_to_unified_sync_module() -> None:
    facade = (ROOT / "heber" / "writer" / "hotstore.py").read_text(encoding="utf-8")

    assert "Compatibility facade for legacy writer.hotstore imports" in facade
    assert "from heber.hotstore.sync import" in facade
    assert "HotStoreWriter = HotStoreSync" in facade
    assert "HotStoreSyncer = HotStoreSync" in facade


def test_hotstore_paths_do_not_reference_clickhouse_driver() -> None:
    files = [
        ROOT / "heber" / "writer" / "hotstore.py",
        ROOT / "heber" / "hotstore" / "sync.py",
        ROOT / "heber" / "hotstore" / "client.py",
    ]
    contents = "\n".join(path.read_text(encoding="utf-8") for path in files)

    assert "clickhouse_driver" not in contents
    assert "clickhouse_connect" in contents
