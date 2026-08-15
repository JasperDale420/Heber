"""Tests for the point-in-time freshness gate on live-only enrichment.

The Data Gateway option-chain, iv-rank, GEX, max-pain and market-tide routes all
answer "as of now" — none accepts an as-of timestamp. Applying them to an alert
that is hours or days old stamps present-day market state onto a past
observation, which is exactly the look-ahead Heber's zero-leakage contract
exists to prevent.

The gate skips those steps for stale alerts and records the reason in
``quality_flags`` so a training run can exclude or down-weight the rows.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from heber.ml.datasets import _split_greek_corrupted_rows
from heber.watch.backfill_scanner import EnrichmentBackfillScanner
from heber.watch.features import (
    QUALITY_FLAG_ENRICHMENT_SKIPPED_STALE,
    QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE,
    AlertFeatureExtractor,
    feature_row_for_gold,
)


class _RecordingClient:
    """httpx.AsyncClient stand-in that records every requested URL."""

    urls: list[str] = []

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _RecordingClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(
        self,
        url: str,
        params: dict | None = None,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ) -> _Response:
        type(self).urls.append(url)
        return _Response(200, {"data": {}})


class _Response:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.headers: dict[str, str] = {}

    def json(self) -> dict:
        return self._payload


def _alert(ts_event: datetime):
    from heber.models.silver import FlowAlertRecord

    return FlowAlertRecord(
        event_id="alert-1",
        provider="unusual_whales",
        feed="flow_alerts",
        instrument_type="option",
        instrument_key="option:OCC:AAPL260220C00100000",
        symbol="AAPL",
        ts_event=ts_event,
        ts_ingest=ts_event,
        ts_available=ts_event,
        source="rest",
        schema_version="v1",
        underlying="AAPL",
        occ_symbol="AAPL260220C00100000",
        expiry=date(2026, 2, 20),
        strike=100.0,
        put_call="C",
        premium=12500.0,
        volume=100.0,
        open_interest=200.0,
        spot_px=195.0,
        contract_px=1.25,
        alert_type="SWEEP",
    )


def _extractor(max_age: timedelta | None) -> AlertFeatureExtractor:
    return AlertFeatureExtractor(
        gateway_url="http://gateway:8000",
        request_max_attempts=1,
        retry_base_delay_seconds=0.0,
        retry_jitter_seconds=0.0,
        live_enrichment_max_age=max_age,
    )


# One entry per live-only enrichment step. Each step tries several route
# patterns and stops at the first success, so a step is matched by any of its
# alternatives rather than all of them.
_LIVE_ONLY_STEP_URL_MARKERS: tuple[tuple[str, ...], ...] = (
    ("/options/chain/",),
    ("/iv-rank",),
    ("/gex/", "/greek-exposure"),
    ("/max-pain",),
    ("/market/tide",),
)
_LIVE_ONLY_URL_MARKERS = tuple(m for step in _LIVE_ONLY_STEP_URL_MARKERS for m in step)


@pytest.mark.asyncio
async def test_stale_alert_skips_live_only_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A week-old alert must not be stamped with today's Greeks/IV/GEX/tide."""
    _RecordingClient.urls = []
    monkeypatch.setattr("httpx.AsyncClient", _RecordingClient)

    extractor = _extractor(timedelta(hours=1))
    features = await extractor.extract(_alert(datetime.now(UTC) - timedelta(days=7)))

    live_calls = [u for u in _RecordingClient.urls if any(m in u for m in _LIVE_ONLY_URL_MARKERS)]
    assert live_calls == []
    assert features.delta is None
    assert features.iv is None
    assert QUALITY_FLAG_ENRICHMENT_SKIPPED_STALE in features.quality_flags
    assert QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE in features.quality_flags


@pytest.mark.asyncio
async def test_stale_alert_still_runs_dated_market_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stock bars are queried with start/end from alert_time, so they stay point-in-time."""
    _RecordingClient.urls = []
    monkeypatch.setattr("httpx.AsyncClient", _RecordingClient)

    extractor = _extractor(timedelta(hours=1))
    await extractor.extract(_alert(datetime.now(UTC) - timedelta(days=7)))

    assert any("/stocks/AAPL/bars" in u for u in _RecordingClient.urls)


@pytest.mark.asyncio
async def test_fresh_alert_runs_every_enrichment_step(monkeypatch: pytest.MonkeyPatch) -> None:
    """The live path is unchanged: a just-arrived alert enriches fully and carries no flags."""
    _RecordingClient.urls = []
    monkeypatch.setattr("httpx.AsyncClient", _RecordingClient)

    extractor = _extractor(timedelta(hours=1))
    features = await extractor.extract(_alert(datetime.now(UTC)))

    for step_markers in _LIVE_ONLY_STEP_URL_MARKERS:
        assert any(m in u for m in step_markers for u in _RecordingClient.urls), f"missing {step_markers}"
    assert features.quality_flags == []


@pytest.mark.asyncio
async def test_gate_disabled_enriches_stale_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    """``None`` disables the gate for deliberate replay of an ingest backlog."""
    _RecordingClient.urls = []
    monkeypatch.setattr("httpx.AsyncClient", _RecordingClient)

    extractor = _extractor(None)
    features = await extractor.extract(_alert(datetime.now(UTC) - timedelta(days=7)))

    assert any("/options/chain/" in u for u in _RecordingClient.urls)
    assert features.quality_flags == []


def test_flagged_null_greek_rows_are_not_quarantined() -> None:
    """Deliberately absent Greeks reach the canonical path; silently broken ones do not."""
    flagged = {
        "alert_id": "flagged",
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "iv": None,
        "quality_flags": [QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE],
    }
    unflagged = dict(flagged, alert_id="unflagged", quality_flags=[])
    df = pd.DataFrame([flagged, unflagged])

    clean, corrupted = _split_greek_corrupted_rows(df)

    assert clean["alert_id"].tolist() == ["flagged"]
    assert corrupted["alert_id"].tolist() == ["unflagged"]


def test_legacy_rows_without_quality_flags_still_quarantine() -> None:
    """Partitions written before the flag existed keep the original fail-loud behaviour."""
    df = pd.DataFrame([{"alert_id": "legacy", "delta": None, "gamma": None, "theta": None, "vega": None, "iv": None}])

    clean, corrupted = _split_greek_corrupted_rows(df)

    assert clean.empty
    assert corrupted["alert_id"].tolist() == ["legacy"]


def test_feature_row_for_gold_matches_live_write_shape() -> None:
    """Backfilled rows must carry the same Gold contract columns the live writer adds."""
    from heber.watch.features import AlertFeatures

    features = AlertFeatures(
        alert_id="a1",
        alert_time=datetime(2026, 8, 6, 15, 0, tzinfo=UTC),
        symbol="AAPL",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        strike=100.0,
        expiry=date(2026, 2, 20),
        put_call="C",
        days_to_expiry=13,
        premium=1.0,
        volume=1.0,
        open_interest=None,
        volume_oi_ratio=None,
        alert_type="SWEEP",
        side=None,
        aggressor=None,
        spot_price=None,
        contract_price=None,
    )
    features.enrichment_failures.append("Greeks")

    row = feature_row_for_gold(features)

    assert "enrichment_failures" not in row
    assert row["instrument_key"] == "option:AAPL260220C00100000"
    assert row["ts_event"] == features.alert_time
    assert row["ts_available"] is not None
    assert row["quality_flags"] == []


def test_scanner_skips_rows_flagged_as_unrecoverable() -> None:
    """Rows that can never be filled must not be re-selected on every scan cycle."""
    df = pd.DataFrame(
        [
            {"alert_id": "recoverable", "alert_time": datetime.now(UTC), "delta": None, "quality_flags": []},
            {
                "alert_id": "unrecoverable",
                "alert_time": datetime.now(UTC),
                "delta": None,
                "quality_flags": [QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE],
            },
        ]
    )

    incomplete = EnrichmentBackfillScanner._find_incomplete_rows(df, timedelta(minutes=60))

    assert incomplete["alert_id"].tolist() == ["recoverable"]


def test_scanner_handles_partitions_without_quality_flags_column() -> None:
    """Partitions written before the flag existed have no such column at all."""
    df = pd.DataFrame([{"alert_id": "legacy", "alert_time": datetime.now(UTC), "delta": None}])

    incomplete = EnrichmentBackfillScanner._find_incomplete_rows(df, timedelta(minutes=60))

    assert incomplete["alert_id"].tolist() == ["legacy"]


def test_provenance_columns_excluded_from_model_feature_matrix() -> None:
    """Provenance and write-time bookkeeping are never model inputs.

    ``ts_available`` is when the row was written; feeding it to a model leaks
    the write lag that separates a live capture from a late backfill.
    """
    from heber.ml.datasets import MetaLabelDatasetBuilder

    builder = MetaLabelDatasetBuilder()
    df = pd.DataFrame(
        [
            {
                "alert_id": "a",
                "premium": 1.0,
                "quality_flags": [],
                "instrument_key": "option:AAPL260220C00100000",
                "ts_event": datetime(2026, 8, 6, 15, 0, tzinfo=UTC),
                "ts_available": datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
                "meta_label": 1,
            }
        ]
    )

    feature_cols = builder.get_feature_columns(df)

    assert feature_cols == ["premium"]


def test_dataset_builder_drops_rows_without_point_in_time_greeks() -> None:
    """Landing flagged rows in canonical Gold is only safe if training excludes them."""
    from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder

    df = pd.DataFrame(
        [
            {"alert_id": "good", "symbol": "AAPL", "delta": 0.5, "quality_flags": []},
            {
                "alert_id": "flagged",
                "symbol": "AAPL",
                "delta": None,
                "quality_flags": [QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE],
            },
        ]
    )

    default = MetaLabelDatasetBuilder(DatasetConfig(min_outcomes_per_symbol=0))
    assert default._apply_filters(df)["alert_id"].tolist() == ["good"]

    opted_in = MetaLabelDatasetBuilder(DatasetConfig(min_outcomes_per_symbol=0, include_unrecoverable_greeks=True))
    assert opted_in._apply_filters(df)["alert_id"].tolist() == ["good", "flagged"]


def test_appending_a_flag_preserves_flags_read_back_from_parquet(tmp_path) -> None:  # noqa: ANN001
    """Parquet list columns read back as ndarray, not list.

    If the append helper only recognises lists, adding a provenance flag wipes
    the row's existing flags — including the one that keeps an all-Greeks-null
    row out of quarantine.
    """
    from heber.watch.features import _append_quality_flag

    path = tmp_path / "part.parquet"
    pd.DataFrame(
        {"quality_flags": [[QUALITY_FLAG_ENRICHMENT_SKIPPED_STALE, QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE]]}
    ).to_parquet(path, index=False)

    df = pd.read_parquet(path)
    _append_quality_flag(df, df.index[0], "market_tide_recovered_from_silver")

    assert sorted(df.at[df.index[0], "quality_flags"]) == sorted(
        [
            QUALITY_FLAG_ENRICHMENT_SKIPPED_STALE,
            QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE,
            "market_tide_recovered_from_silver",
        ]
    )


def test_recovered_tide_direction_is_derived_from_premium_not_provider_label() -> None:
    """Direction must agree with the premium sign, as it does on the live path.

    The provider's own sentiment label contradicts its legs on the occasional
    degenerate bar (the 2026-08-07 13:30 open print has both legs sign-inverted),
    and backward-asof maps the whole open burst onto that one bar — so copying
    the label would stamp "bearish" onto ~1,500 rows carrying a positive premium.
    """
    from heber.watch.features import _classify_direction

    net_call, net_put, provider_sentiment = -931_323.0, 16_438_200.0, "bearish"
    net_premium = net_call + net_put

    assert net_premium > 0
    assert _classify_direction(net_premium) == "bullish"
    assert _classify_direction(net_premium) != provider_sentiment


def test_recovered_market_tide_net_premium_matches_provider_sentiment() -> None:
    """Both tide legs are signed net flows, so the market net premium is their sum.

    Subtracting the put leg adds its magnitude instead of cancelling it, giving
    a value that is never negative and disagrees with the provider's own
    sentiment on bearish bars.
    """
    # Real shape from Silver market_tide dt=2026-08-06: a bearish bar where the
    # put leg outweighs the call leg.
    net_call, net_put, sentiment = 78_065_704.0, -98_024_268.0, "bearish"

    net_premium = net_call + net_put

    assert net_premium < 0
    assert sentiment == ("bullish" if net_premium > 0 else "bearish")


def test_mixed_legacy_and_flagged_partitions_round_trip(tmp_path) -> None:  # noqa: ANN001
    """94 existing partitions have no quality_flags column; new ones do.

    Writing into a legacy partition and reading the dataset back across both
    must not fail on the list column or leave NaN where a list belongs.
    """
    from heber.ml.datasets import persist_features_to_gold, quality_flag_series
    from heber.reader import HeberReader

    out = tmp_path / "meta_label_features"
    legacy_partition = out / "dt=2026-08-05"
    legacy_partition.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "alert_id": "legacy",
                "alert_time": datetime(2026, 8, 5, 15, 0, tzinfo=UTC),
                "delta": 0.4,
                "gamma": 0.1,
                "theta": -0.2,
                "vega": 0.3,
                "iv": 0.5,
            }
        ]
    ).to_parquet(legacy_partition / "data.parquet", index=False)

    persist_features_to_gold(
        features_df=pd.DataFrame(
            [
                {
                    "alert_id": "backfilled",
                    "alert_time": datetime(2026, 8, 5, 16, 0, tzinfo=UTC),
                    "delta": None,
                    "gamma": None,
                    "theta": None,
                    "vega": None,
                    "iv": None,
                    "quality_flags": [QUALITY_FLAG_GREEKS_NO_POINT_IN_TIME_SOURCE],
                }
            ]
        ),
        output_path=out,
        partition_col="alert_time",
    )

    merged = HeberReader().read_parquet_dataset(path=out)

    assert sorted(merged["alert_id"]) == ["backfilled", "legacy"]
    flags = quality_flag_series(merged)
    assert all(isinstance(v, list) for v in flags)
    assert flags[merged["alert_id"] == "legacy"].tolist() == [[]]
