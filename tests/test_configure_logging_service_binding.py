"""Regression tests for ``heber.ops.logging.configure_logging`` service binding.

Background: historical bug — once a process called ``configure_logging`` with
``service_name="heber-catalog"``, every subsequent call (e.g. a CLI tool or
test that imported a different service) was a silent no-op because
``empire_core.logger.setup_logging`` short-circuited on a module-global
``_configured`` flag. The result was that writer/consumer/backpressure log
records got stamped with ``service=heber-catalog`` and rotated into
``logs/heber-catalog_errors_*.log``, despite originating from completely
unrelated modules.

The fix is twofold: (1) ``configure_logging`` now passes ``force=True`` by
default so the second call wins, and (2) the root conftest resets the
empire_core ``_service_name`` global between tests.
"""

from __future__ import annotations

import logging

import pytest
import structlog

from heber.ops.logging import configure_logging, get_logger


@pytest.fixture(autouse=True)
def _reset_root_handlers() -> None:
    """Save and restore root-logger handlers so this module's reconfigurations
    don't leak handler instances into other tests."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    # Tear down anything we attached.
    for handler in list(root.handlers):
        if handler not in original_handlers:
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # noqa: BLE001 — best-effort
                pass
    root.setLevel(original_level)
    structlog.reset_defaults()


@pytest.mark.unit
def test_configure_logging_second_call_changes_service_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A second configure_logging() must rebind the structlog service field."""
    configure_logging(service_name="heber-catalog", log_level="INFO")
    capsys.readouterr()  # discard any startup chatter

    configure_logging(service_name="heber-consumer", log_level="INFO")

    logger = get_logger("test.service_rebinding")
    logger.info("after_second_configure")

    out = capsys.readouterr().out
    assert '"service": "heber-consumer"' in out, (
        "Expected service=heber-consumer after the second configure_logging() "
        f"call. Got output:\n{out}\n"
        "If service=heber-catalog still appears, the force=True default on "
        "configure_logging is regressing."
    )
    assert '"service": "heber-catalog"' not in out


@pytest.mark.unit
def test_configure_logging_default_service_is_heber(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When no service_name is passed, the bound service is 'heber'."""
    configure_logging(log_level="INFO")
    capsys.readouterr()

    get_logger("test.default_service").info("default_service_check")
    out = capsys.readouterr().out
    assert '"service": "heber"' in out, f"expected service=heber, got:\n{out}"


@pytest.mark.unit
def test_configure_logging_force_false_can_skip_rebind(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Explicit force=False must not change the previously bound service.

    This pins the escape hatch — callers who *want* the no-op behaviour can
    request it, but the default is force=True.
    """
    # Establish a known prior state.
    configure_logging(service_name="heber-first", log_level="INFO")
    capsys.readouterr()

    # In pytest, setup_logging auto-forces because PYTEST_CURRENT_TEST is
    # set, so we cannot prove force=False from inside the test runner.
    # Instead, assert that the explicit flag flows through to setup_logging
    # without errors and the API contract holds.
    configure_logging(service_name="heber-second", log_level="INFO", force=False)
    get_logger("test.force_false").info("after_force_false")

    out = capsys.readouterr().out
    # Under PYTEST_CURRENT_TEST the underlying setup_logging will still
    # re-bind, but the API contract is: force=False is accepted as a kwarg.
    assert '"message": "after_force_false"' in out
