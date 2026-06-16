from heber.config import settings
from heber.quality.soda_scanner import SodaConfig


def test_soda_config_defaults_to_settings_silver_path() -> None:
    config = SodaConfig()
    assert config.silver_path == settings.silver_path


def test_soda_config_from_env_uses_settings_fallback(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_SILVER_PATH", raising=False)
    config = SodaConfig.from_env()
    assert config.silver_path == settings.silver_path
