"""Regression test: MetaLabelScorer must not log "Model not loaded" on every
score() call when the model is unavailable.

Before this change, the scorer emitted a WARNING per score request, which
generated 192 records/day in production at perfectly even intervals — i.e. one
per score attempt. The new behaviour is a one-shot WARNING with a more
diagnostic event key (`meta_model_unavailable`), and silent None returns
afterwards. Graceful degradation (returning None) is unchanged.
"""

from __future__ import annotations

import pytest

from heber.ml import inference as inference_module
from heber.ml.inference import MetaLabelScorer


class _RecordingLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def _record(self, level: str, event: str, **kwargs: object) -> None:
        self.calls.append((level, event, dict(kwargs)))

    def debug(self, event: str, **kwargs: object) -> None:
        self._record("debug", event, **kwargs)

    def info(self, event: str, **kwargs: object) -> None:
        self._record("info", event, **kwargs)

    def warning(self, event: str, **kwargs: object) -> None:
        self._record("warning", event, **kwargs)

    def error(self, event: str, **kwargs: object) -> None:
        self._record("error", event, **kwargs)

    def exception(self, event: str, **kwargs: object) -> None:
        self._record("exception", event, **kwargs)


class _Alert:
    """Bare-minimum stand-in for FlowAlertRecord — only event_id is used
    here because score() short-circuits at the model-None check.
    """

    def __init__(self, event_id: str) -> None:
        self.event_id = event_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_score_missing_model_logs_warning_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Many score() calls with no model should produce exactly one warning."""
    recorder = _RecordingLogger()
    monkeypatch.setattr(inference_module, "logger", recorder)

    scorer = MetaLabelScorer()
    assert scorer._model is None  # precondition: no model loaded

    results = []
    for i in range(50):
        results.append(await scorer.score(_Alert(event_id=f"evt-{i}")))

    # All calls return None (graceful degradation preserved).
    assert all(r is None for r in results)

    # And we logged exactly one WARNING about the missing model, not 50.
    missing_warnings = [c for c in recorder.calls if c[0] == "warning" and c[1] == "meta_model_unavailable"]
    assert len(missing_warnings) == 1, (
        f"Expected exactly one meta_model_unavailable WARNING across 50 "
        f"score() calls; got {len(missing_warnings)}. The one-shot guard "
        f"in MetaLabelScorer._warn_missing_model_once is regressing — see "
        f"heber/ml/inference.py."
    )

    # And we counted the skipped scores so observability isn't lost.
    assert scorer._missing_model_skips == 50


@pytest.mark.unit
@pytest.mark.asyncio
async def test_score_missing_model_legacy_string_not_emitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The legacy noisy message text must not be emitted any more."""
    recorder = _RecordingLogger()
    monkeypatch.setattr(inference_module, "logger", recorder)

    scorer = MetaLabelScorer()
    await scorer.score(_Alert(event_id="evt-1"))
    await scorer.score(_Alert(event_id="evt-2"))

    legacy = [c for c in recorder.calls if "Model not loaded" in c[1]]
    assert not legacy, (
        "The legacy 'Model not loaded, cannot score' WARNING reappeared. "
        "It should be replaced by a one-shot 'meta_model_unavailable' emit."
    )
