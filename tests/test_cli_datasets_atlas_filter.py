"""CLI `datasets` command must hide research-artifact feed dirs.

Atlas writes hypothesis materializations directly to
``silver/feed=atlas_features_hyp_<hash>_<config>/`` with an
``_atlas_materialization_meta.json`` marker file. They are NOT
Heber-managed Silver datasets and pollute the inventory listing —
``heber datasets --layer silver`` should hide them.
"""

from __future__ import annotations

import sys

from heber import cli as cli_module


def _seed_silver_layout(root):
    """Create a Silver layout with one real feed and two atlas materializations."""
    silver = root / "silver"
    # Real Silver feed with normal partitions.
    real = silver / "feed=bars" / "instrument_type=equity" / "dt=2026-04-29"
    real.mkdir(parents=True)
    (real / "part-1.parquet").touch()

    # Atlas materialization with marker file (the canonical signal).
    atlas_marked = silver / "feed=atlas_features_hyp_aaaa_bbbb"
    atlas_marked.mkdir(parents=True)
    (atlas_marked / "_atlas_materialization_meta.json").write_text("{}")
    (atlas_marked / "part-x.parquet").touch()

    # Another atlas-shaped dir, no marker — should still be hidden by name prefix
    # (there's a long tail of these that may or may not have written the marker).
    atlas_unmarked = silver / "feed=atlas_features_hyp_cccc_dddd"
    atlas_unmarked.mkdir(parents=True)
    (atlas_unmarked / "part-y.parquet").touch()


def test_datasets_command_hides_atlas_materializations(monkeypatch, capsys, tmp_path):
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    _seed_silver_layout(tmp_path)
    monkeypatch.setattr(sys, "argv", ["heber", "datasets", "--layer", "silver"])

    exit_code = cli_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "bars" in captured.out
    assert "atlas_features_hyp_aaaa_bbbb" not in captured.out
    assert "atlas_features_hyp_cccc_dddd" not in captured.out
