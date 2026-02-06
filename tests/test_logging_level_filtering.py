"""Regression tests for structured logging level filtering."""

from __future__ import annotations

import logging

import pytest
import structlog

from heber.ops.logging import configure_logging, get_logger


@pytest.fixture(autouse=True)
def _reset_logging_state() -> None:
    root_logger = logging.getLogger()
    original_level = root_logger.level
    yield
    structlog.reset_defaults()
    root_logger.setLevel(original_level)


@pytest.mark.parametrize("json_output", [True, False])
def test_info_level_filters_debug_messages(capsys: pytest.CaptureFixture[str], json_output: bool) -> None:
    configure_logging(log_level="INFO", json_output=json_output)
    logger = get_logger("test-info-filter")

    logger.debug("debug_should_be_filtered")
    logger.info("info_should_be_emitted")

    output = capsys.readouterr().out
    assert "info_should_be_emitted" in output
    assert "debug_should_be_filtered" not in output
    assert logging.getLogger().level == logging.INFO


@pytest.mark.parametrize("json_output", [True, False])
def test_debug_level_emits_debug_messages(capsys: pytest.CaptureFixture[str], json_output: bool) -> None:
    configure_logging(log_level="DEBUG", json_output=json_output)
    logger = get_logger("test-debug-filter")

    logger.debug("debug_should_be_emitted")

    output = capsys.readouterr().out
    assert "debug_should_be_emitted" in output
    assert logging.getLogger().level == logging.DEBUG


def test_invalid_level_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Invalid log level"):
        configure_logging(log_level="TRACE")
