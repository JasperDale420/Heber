from __future__ import annotations

from pathlib import Path

import pytest

from heber.watch import __main__ as watch_main


def test_ensure_writable_output_path_creates_path(tmp_path: Path) -> None:
    output_path = tmp_path / "gold" / "labels_alert_barriers"

    resolved = watch_main._ensure_writable_output_path(str(output_path))

    assert resolved == output_path
    assert output_path.exists()


def test_ensure_writable_output_path_returns_none_for_empty_input() -> None:
    assert watch_main._ensure_writable_output_path("") is None


def test_ensure_writable_output_path_raises_on_permission_error(tmp_path: Path) -> None:
    blocked = tmp_path / "blocked"
    blocked.mkdir(parents=True, exist_ok=True)
    blocked.chmod(0o500)

    try:
        with pytest.raises(PermissionError):
            watch_main._ensure_writable_output_path(str(blocked))
    finally:
        blocked.chmod(0o700)
