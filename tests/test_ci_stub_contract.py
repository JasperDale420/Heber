"""Regression coverage for GitHub CI dependency stubs."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


def test_ci_empire_core_stub_matches_logger_contract(
    tmp_path: Path,
    capsys,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    workspace = tmp_path / "Heber"
    workspace.mkdir()

    subprocess.run(
        [str(repo_root / ".github/scripts/create-ci-stubs.sh")],
        cwd=workspace,
        check=True,
    )

    empire_core_root = tmp_path / "empire-core"
    sys.path.insert(0, str(empire_core_root))
    for module_name in list(sys.modules):
        if module_name == "empire_core" or module_name.startswith("empire_core.") or module_name == "heber.ops.logging":
            sys.modules.pop(module_name)
    try:
        logger_module = importlib.import_module("empire_core.logger")
        heber_logging_module = importlib.import_module("heber.ops.logging")
    finally:
        sys.path.remove(str(empire_core_root))

    for helper_name in (
        "setup_logging",
        "get_logger",
        "bind_context",
        "unbind_context",
        "clear_context",
        "log_error",
        "log_retry",
    ):
        assert callable(getattr(logger_module, helper_name))

    assert callable(heber_logging_module.unbind_context)

    heber_logging_module.configure_logging(service_name="heber-ci-stub", log_level="INFO")
    test_logger = heber_logging_module.get_logger("test.ci_stub_contract")
    test_logger.debug("debug_should_be_filtered")
    test_logger.info("info_should_be_emitted")

    output = capsys.readouterr().out
    assert '"service": "heber-ci-stub"' in output
    assert '"message": "info_should_be_emitted"' in output
    assert "debug_should_be_filtered" not in output
