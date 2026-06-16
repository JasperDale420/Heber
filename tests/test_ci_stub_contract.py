"""Regression coverage for GitHub CI dependency stubs."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


def test_ci_empire_core_stub_exports_logger_helpers(tmp_path: Path) -> None:
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
