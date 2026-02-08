from __future__ import annotations

import importlib
import warnings

import heber.watch.models as watch_models


def test_watch_models_do_not_emit_class_config_deprecation_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        importlib.reload(watch_models)

    class_config_warnings = [
        warning
        for warning in caught
        if warning.category.__name__ == "PydanticDeprecatedSince20"
        and "class-based `config` is deprecated" in str(warning.message)
    ]
    assert not class_config_warnings
