from __future__ import annotations

from pathlib import Path

from heber.config import Settings


def test_settings_honors_heber_gold_path_env(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_GOLD_ROOT", raising=False)
    monkeypatch.setenv("HEBER_GOLD_PATH", "/tmp/heber-gold-root")

    settings = Settings(_env_file=None)

    assert settings.gold_path == Path("/tmp/heber-gold-root")


def test_settings_gold_path_defaults_to_data_root(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_GOLD_ROOT", raising=False)
    monkeypatch.delenv("HEBER_GOLD_PATH", raising=False)
    monkeypatch.setenv("HEBER_DATA_ROOT", "/tmp/heber-data")

    settings = Settings(_env_file=None)

    assert settings.gold_path == Path("/tmp/heber-data") / "gold"
