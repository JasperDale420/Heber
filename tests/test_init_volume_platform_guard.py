"""Regression tests for cross-platform init_volume behavior."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_init_volume_uses_explicit_platform_and_tool_checks() -> None:
    script = (ROOT / "scripts/init_volume.sh").read_text(encoding="utf-8")

    assert 'HOST_OS="$(uname -s)"' in script
    assert 'if [ "$HOST_OS" = "Darwin" ]; then' in script
    assert "command -v dot_clean" in script
    assert "Skipping resource fork cleanup" in script


def test_init_volume_does_not_use_implicit_dot_clean_fallback() -> None:
    script = (ROOT / "scripts/init_volume.sh").read_text(encoding="utf-8")

    assert "dot_clean -m" in script
    assert "|| true" not in script
