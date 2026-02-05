"""Regression test to prevent naive UTC timestamp usage."""

from __future__ import annotations

from pathlib import Path


def test_heber_sources_do_not_use_datetime_utcnow() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    heber_root = repo_root / "heber"

    offenders: list[str] = []
    for path in heber_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "datetime.utcnow(" in text:
            offenders.append(str(path.relative_to(repo_root)))

    assert offenders == [], f"Found naive datetime.utcnow usage in: {offenders}"
