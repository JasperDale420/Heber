"""The nightly poller must be able to write a different Gold version per pipeline.

`gold_poller_version` is one global setting, and `read_gold` resolves to the
semver-latest version directory. Regenerating a dataset into `version=v2` while
the poller keeps writing `v1` therefore freezes that dataset: every reader
resolves v2, every nightly write lands in v1, and nothing says so.

The two settings that existed could not express the fix. Leaving the global at
`v1` strands `darkpool_features` and `market_regime_features` at v2. Setting it
to `v2` makes all 24 Gold datasets start a fresh v2 — and the 22 that only have
v1 would get a seven-day v2 shadowing their entire history, which is the
failure mode the regeneration plan was written to avoid.

Keyed on pipeline name rather than dataset name because `version` is a
constructor argument on the pipeline, which is the granularity that actually
exists — and because `gold_poller_disabled_pipelines` already keys that way.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from heber.config import Settings
from heber.gold_poller.child import _instantiate_pipeline
from heber.gold_poller.service import (
    PIPELINE_REGISTRY,
    _validate_pipeline_version_overrides,
)


def _settings(**kw: str) -> Settings:
    return Settings(**kw)  # type: ignore[arg-type]


def _entry(name: str) -> dict:
    for e in PIPELINE_REGISTRY:
        if e["name"] == name:
            return e
    raise AssertionError(f"no pipeline named {name!r}")


class TestVersionResolution:
    def test_unset_gives_every_pipeline_the_global_version(self) -> None:
        s = _settings(gold_poller_version="v1")

        assert s.gold_poller_version_for("darkpool") == "v1"
        assert s.gold_poller_version_for("market_regime") == "v1"

    def test_named_pipeline_overrides_the_global(self) -> None:
        s = _settings(gold_poller_version="v1", gold_poller_pipeline_versions="darkpool=v2")

        assert s.gold_poller_version_for("darkpool") == "v2"
        assert s.gold_poller_version_for("momentum") == "v1", "an unlisted pipeline must not move"

    def test_several_overrides_and_surrounding_whitespace(self) -> None:
        s = _settings(
            gold_poller_version="v1",
            gold_poller_pipeline_versions=" darkpool = v2 , market_regime=v3 ",
        )

        assert s.gold_poller_version_for("darkpool") == "v2"
        assert s.gold_poller_version_for("market_regime") == "v3"

    def test_unknown_pipeline_name_is_rejected_by_the_poller(self) -> None:
        """A typo must not silently write to the wrong version forever.

        Checked by the poller, not by Settings: importing the pipeline registry
        from config creates a cycle that breaks every service on import (see
        TestConfigImportsCleanly). Settings validates only the shape.
        """
        s = _settings(gold_poller_pipeline_versions="darkpool_features=v2")

        assert s.gold_poller_pipeline_version_map == {"darkpool_features": "v2"}, "shape check should accept it"
        with pytest.raises(ValueError, match="darkpool_features"):
            _validate_pipeline_version_overrides(s)

    def test_malformed_entry_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _settings(gold_poller_pipeline_versions="darkpool")


class TestWiring:
    """Pins that the resolver is actually used, not merely present.

    A settings-only test would pass with `_instantiate_pipeline` still reading
    the global, which is exactly the bug.
    """

    def test_instantiated_pipeline_carries_its_override(self) -> None:
        s = _settings(gold_poller_version="v1", gold_poller_pipeline_versions="darkpool=v2")

        pipeline = _instantiate_pipeline(_entry("darkpool"), s)

        assert pipeline.version == "v2"

    def test_unlisted_pipeline_still_gets_the_global(self) -> None:
        s = _settings(gold_poller_version="v1", gold_poller_pipeline_versions="darkpool=v2")

        pipeline = _instantiate_pipeline(_entry("market_regime"), s)

        assert pipeline.version == "v1"


class TestConfigImportsCleanly:
    """The override must not break importing config — this feature once did.

    Validating pipeline names inside `Settings` meant config imported the
    poller's registry, and the poller imports config. With the env var set,
    every service died at import — including `heber-consumer`, which never
    touches Gold:

        ImportError: cannot import name 'settings' from partially
        initialized module 'heber.config' (most likely due to a circular
        import)

    The whole suite stayed green through it, because test modules import
    `heber.gold_poller.service` at the top and `sys.modules` then short-circuits
    the lazy import — the reverse of production order. These run in a
    subprocess for that reason: importing in-process proves nothing.
    """

    @pytest.mark.parametrize(
        "module",
        ["heber.config", "heber.writer.consumer", "heber.reader.core", "heber.gold_poller.service"],
    )
    def test_module_imports_with_the_override_set(self, module: str) -> None:
        result = subprocess.run(
            [sys.executable, "-c", f"import {module}"],
            env={"PATH": "/usr/bin:/bin", "HEBER_GOLD_POLLER_PIPELINE_VERSIONS": "darkpool=v2"},
            capture_output=True,
            text=True,
            timeout=120,
        )

        assert result.returncode == 0, f"{module} failed to import with the override set:\n{result.stderr[-2000:]}"
