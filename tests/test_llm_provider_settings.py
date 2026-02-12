"""Regression tests for LLM provider settings and key aliases."""

from __future__ import annotations

from heber.config import Settings


def _clear_llm_env(monkeypatch) -> None:
    for key in (
        "HEBER_LLM_PROVIDER",
        "LLM_PROVIDER",
        "HEBER_LLM_MODEL",
        "LLM_MODEL",
        "HEBER_LLM_BASE_URL",
        "LLM_BASE_URL",
        "HEBER_LLM_API_KEY",
        "LLM_API_KEY",
        "OPENAI_API_KEY",
        "DASHSCOPE_API_KEY",
        "HEBER_LLM_QWEN_REGION",
        "LLM_QWEN_REGION",
    ):
        monkeypatch.delenv(key, raising=False)


def test_llm_settings_defaults() -> None:
    settings = Settings(_env_file=None)
    assert settings.llm_provider == "openai"
    assert settings.llm_model == "gpt-4o-mini"
    assert settings.llm_effective_base_url is None


def test_llm_settings_accept_openai_key_alias(monkeypatch) -> None:
    _clear_llm_env(monkeypatch)
    monkeypatch.setenv("OPENAI_API_KEY", "openai-token-example")

    settings = Settings(_env_file=None)
    assert settings.llm_api_key == "openai-token-example"  # pragma: allowlist secret


def test_llm_settings_qwen_defaults_to_intl_endpoint(monkeypatch) -> None:
    _clear_llm_env(monkeypatch)
    monkeypatch.setenv("HEBER_LLM_PROVIDER", "qwen")

    settings = Settings(_env_file=None)
    assert settings.llm_effective_base_url == "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"


def test_llm_settings_qwen_region_variants(monkeypatch) -> None:
    _clear_llm_env(monkeypatch)
    monkeypatch.setenv("HEBER_LLM_PROVIDER", "qwen")
    monkeypatch.setenv("HEBER_LLM_QWEN_REGION", "us")
    settings = Settings(_env_file=None)
    assert settings.llm_effective_base_url == "https://dashscope-us.aliyuncs.com/compatible-mode/v1"

    monkeypatch.setenv("HEBER_LLM_QWEN_REGION", "cn")
    settings = Settings(_env_file=None)
    assert settings.llm_effective_base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"


def test_llm_settings_base_url_override(monkeypatch) -> None:
    _clear_llm_env(monkeypatch)
    monkeypatch.setenv("HEBER_LLM_PROVIDER", "qwen")
    monkeypatch.setenv("HEBER_LLM_BASE_URL", "https://example.invalid/v1")

    settings = Settings(_env_file=None)
    assert settings.llm_effective_base_url == "https://example.invalid/v1"


def test_llm_settings_accept_dashscope_key_alias(monkeypatch) -> None:
    _clear_llm_env(monkeypatch)
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dashscope-token-example")

    settings = Settings(_env_file=None)
    assert settings.llm_api_key == "dashscope-token-example"  # pragma: allowlist secret
