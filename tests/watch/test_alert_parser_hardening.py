"""Unit tests for the heber-watch alert parser & entry-price fallback.

Covers the regressions tracked under the P2 data-quality cleanup:

- ``unrecognized_put_call`` warnings emitted with rich context (raw value
  repr, event_id, occ_symbol) and the parser tolerating new variants
  (whitespace, dict wrappers, OCC-symbol fallback, dropping unknown ints
  while still recognising compound strings like ``"call_sweep"``).
- ``Alert missing required fields`` log carrying ``event_id``, ``provider``,
  ``feed``, the ``missing_fields`` list, and the *keys* present in the
  payload (no values, to avoid leaking PII).
- ``Could not parse alert`` log enriched with the same diagnostic context.
- ``_resolve_entry_price`` fallback chain: contract_px → gateway quote →
  alert bid/ask mid → alert last → exhausted.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from heber.watch import consumer as consumer_module
from heber.watch.consumer import AlertWatchConsumer


class _NoopRedis:
    pass


class _CaptureManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def has_alert_claim_async(self, alert_id: str) -> bool:
        return False

    async def create_watch_async(self, **kwargs: Any):  # noqa: ANN401
        self.calls.append(kwargs)
        return SimpleNamespace(watch_id=f"watch-{len(self.calls)}")


def _consumer() -> AlertWatchConsumer:
    return AlertWatchConsumer(redis_client=_NoopRedis(), watch_manager=_CaptureManager())


def _stream_data(payload: dict) -> dict[bytes, bytes]:
    return {b"data": json.dumps(payload).encode("utf-8")}


# ---------------------------------------------------------------------------
# _normalize_put_call: new variants accepted, unrecognized values logged richly
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNormalizePutCallVariants:
    def test_call_lowercase_with_whitespace(self) -> None:
        assert AlertWatchConsumer._normalize_put_call("  call  ") == "C"

    def test_compound_strings_starting_with_letter(self) -> None:
        assert AlertWatchConsumer._normalize_put_call("call_sweep") == "C"
        assert AlertWatchConsumer._normalize_put_call("PUT_BLOCK") == "P"

    def test_single_char_aliases(self) -> None:
        assert AlertWatchConsumer._normalize_put_call("c") == "C"
        assert AlertWatchConsumer._normalize_put_call("p") == "P"

    def test_dict_wrapper(self) -> None:
        assert AlertWatchConsumer._normalize_put_call({"option_type": "put"}) == "P"
        assert AlertWatchConsumer._normalize_put_call({"type": "call"}) == "C"
        assert AlertWatchConsumer._normalize_put_call({"value": "C"}) == "C"

    def test_occ_symbol_fallback_when_value_unrecognized(self) -> None:
        # raw value carries garbage but occ symbol is a clean OCC contract
        assert (
            AlertWatchConsumer._normalize_put_call(
                "unknown",
                occ_symbol="AAPL260116C00200000",
            )
            == "C"
        )
        assert (
            AlertWatchConsumer._normalize_put_call(
                123,
                occ_symbol="AAPL260116P00200000",
            )
            == "P"
        )

    def test_empty_string_returns_none_without_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warnings: list[tuple[str, dict]] = []
        monkeypatch.setattr(
            consumer_module.logger,
            "warning",
            lambda msg, **kw: warnings.append((msg, kw)),
        )
        assert AlertWatchConsumer._normalize_put_call("   ") is None
        # whitespace-only string should NOT trigger the warning — the raw
        # value was provided but normalized to empty, which is best handled
        # silently by downstream callers (they have other defaults).
        assert all(call[0] != "unrecognized_put_call" for call in warnings)

    def test_unrecognized_value_logs_full_context(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warnings: list[tuple[str, dict]] = []
        monkeypatch.setattr(
            consumer_module.logger,
            "warning",
            lambda msg, **kw: warnings.append((msg, kw)),
        )
        result = AlertWatchConsumer._normalize_put_call(
            1,
            occ_symbol="not-an-occ-symbol",
            event_id="evt-123",
        )
        assert result is None
        # exactly one warning, with the structured fields the task requires
        unrecognized = [c for c in warnings if c[0] == "unrecognized_put_call"]
        assert len(unrecognized) == 1
        fields = unrecognized[0][1]
        assert fields["raw_value"] == "1"  # repr(1) == "1"
        assert fields["raw_type"] == "int"
        assert fields["event_id"] == "evt-123"
        assert fields["occ_symbol"] == "not-an-occ-symbol"


# ---------------------------------------------------------------------------
# _resolve_put_call: derives put/call from OCC symbol when explicit fields missing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolvePutCallFromOcc:
    def test_resolves_from_option_chain_when_fields_missing(self) -> None:
        consumer = _consumer()
        parsed = {
            "id": "a1",
            "underlying": "AAPL",
            "option_chain": "AAPL260116P00200000",
        }
        assert consumer._resolve_put_call(parsed) == "P"

    def test_explicit_field_wins_over_occ(self) -> None:
        consumer = _consumer()
        parsed = {
            "id": "a1",
            "underlying": "AAPL",
            "option_chain": "AAPL260116C00200000",
            "put_call": "PUT",  # explicit overrides
        }
        assert consumer._resolve_put_call(parsed) == "P"

    def test_falls_back_to_default_when_nothing_resolves(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Suppress warning side-effect for the assertion only.
        monkeypatch.setattr(consumer_module.logger, "warning", lambda *a, **kw: None)
        consumer = _consumer()
        parsed: dict[str, Any] = {"id": "a1", "underlying": "AAPL"}
        # Historical default behaviour: when no signal exists at all, default to "C".
        assert consumer._resolve_put_call(parsed) == "C"


# ---------------------------------------------------------------------------
# "Alert missing required fields" log includes diagnostic context
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAlertMissingRequiredFieldsLogging:
    def test_log_carries_event_id_provider_feed_and_keys(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warnings: list[tuple[str, dict]] = []
        monkeypatch.setattr(
            consumer_module.logger,
            "warning",
            lambda msg, **kw: warnings.append((msg, kw)),
        )

        consumer = _consumer()
        result = consumer._build_alert_record(
            {
                "provider": "unusual_whales",
                "feed": "flow_alerts",
                "option_chain": "AAPL260116C00200000",
                # NOTE: no `id`/`event_id`/`alert_id`, no `underlying`/`ticker`/`symbol`
                #       — exactly the payload shape we want to surface.
            },
            batch_index=2,
        )

        assert result is None
        missing_logs = [c for c in warnings if c[0] == "Alert missing required fields"]
        assert len(missing_logs) == 1
        fields = missing_logs[0][1]
        # event_id resolves to None because no id field was present.
        assert fields["event_id"] is None
        assert fields["provider"] == "unusual_whales"
        assert fields["feed"] == "flow_alerts"
        assert fields["missing_fields"] == ["id", "underlying"]
        assert fields["has_occ_symbol"] is True
        assert fields["batch_index"] == 2
        # Keys are present, values are NOT — so we get a sorted list of strings.
        assert "option_chain" in fields["payload_keys"]
        assert "feed" in fields["payload_keys"]
        # Ensure the payload values themselves are not in the log.
        for value in fields.values():
            assert value != "AAPL260116C00200000"


# ---------------------------------------------------------------------------
# "Could not parse alert" log carries diagnostic context
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCouldNotParseAlertLogging:
    @pytest.mark.asyncio
    async def test_log_includes_provider_feed_and_payload_keys(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warnings: list[tuple[str, dict]] = []
        monkeypatch.setattr(
            consumer_module.logger,
            "warning",
            lambda msg, **kw: warnings.append((msg, kw)),
        )
        # Silence the summary INFO emit so it doesn't clutter test output.
        monkeypatch.setattr(consumer_module.logger, "info", lambda *a, **kw: None)

        consumer = _consumer()
        # payload that decodes fine but yields no alerts: items=[] is a list,
        # but each item is missing the required fields — so _parse_alerts
        # returns []. The _process_alert handler then emits "Could not parse alert".
        ok, retryable, status = await consumer._process_alert(
            "9-7",
            _stream_data(
                {
                    "feed": "flow_alerts",
                    "provider": "unusual_whales",
                    "payload": {"items": []},
                }
            ),
        )

        assert ok is False
        assert status == "alert_parse_failed"
        parse_logs = [c for c in warnings if c[0] == "Could not parse alert"]
        assert len(parse_logs) == 1
        fields = parse_logs[0][1]
        assert fields["msg_id"] == "9-7"
        assert fields["provider"] == "unusual_whales"
        assert fields["feed"] == "flow_alerts"
        assert "items" in fields["payload_keys"]


# ---------------------------------------------------------------------------
# _resolve_entry_price: documented fallback chain
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveEntryPriceFallbackChain:
    @pytest.mark.asyncio
    async def test_prefers_alert_contract_px(self) -> None:
        consumer = _consumer()
        consumer._get_entry_price = AsyncMock(return_value=999.0)  # type: ignore[method-assign]
        price, source = await consumer._resolve_entry_price(
            {
                "id": "a1",
                "occ_symbol": "AAPL260116C00200000",
                "contract_px": 1.25,
                "bid": 1.10,
                "ask": 1.30,
            }
        )
        assert price == 1.25
        assert source == "alert_contract_px"
        consumer._get_entry_price.assert_not_called()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_falls_through_to_gateway_quote(self) -> None:
        consumer = _consumer()
        consumer._get_entry_price = AsyncMock(return_value=2.50)  # type: ignore[method-assign]
        price, source = await consumer._resolve_entry_price(
            {
                "id": "a1",
                "occ_symbol": "AAPL260116C00200000",
                "contract_px": 0.0,  # missing
            }
        )
        assert price == 2.50
        assert source == "gateway_quote"

    @pytest.mark.asyncio
    async def test_falls_through_to_alert_bid_ask_mid(self) -> None:
        consumer = _consumer()
        consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
        price, source = await consumer._resolve_entry_price(
            {
                "id": "a1",
                "occ_symbol": "AAPL260116C00200000",
                "contract_px": None,
                "bid": 1.00,
                "ask": 1.40,
            }
        )
        assert price == pytest.approx(1.20)
        assert source == "alert_bid_ask_mid"

    @pytest.mark.asyncio
    async def test_skips_inverted_bid_ask(self) -> None:
        # crossed/invalid market: bid > ask should NOT be silently used as a mid
        consumer = _consumer()
        consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
        price, source = await consumer._resolve_entry_price(
            {
                "id": "a1",
                "occ_symbol": "AAPL260116C00200000",
                "contract_px": None,
                "bid": 2.00,
                "ask": 1.00,
                "last": 1.75,
            }
        )
        assert price == 1.75
        assert source == "alert_last_price"

    @pytest.mark.asyncio
    async def test_falls_through_to_alert_last(self) -> None:
        consumer = _consumer()
        consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
        price, source = await consumer._resolve_entry_price(
            {
                "id": "a1",
                "occ_symbol": "AAPL260116C00200000",
                "contract_px": 0.0,
                "last": 3.14,
            }
        )
        assert price == 3.14
        assert source == "alert_last_price"

    @pytest.mark.asyncio
    async def test_exhausted_returns_none_and_caller_logs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        consumer = _consumer()
        consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]

        warnings: list[tuple[str, dict]] = []
        monkeypatch.setattr(
            consumer_module.logger,
            "warning",
            lambda msg, **kw: warnings.append((msg, kw)),
        )

        price, source = await consumer._resolve_entry_price({"id": "a1", "occ_symbol": "AAPL260116C00200000"})
        assert price is None
        assert source == "exhausted"
        # Caller responsibility (verified separately): logs "Could not get entry price"
        # with the fallback_used label. _resolve_entry_price itself does NOT log
        # so test that here — the warning will fire in _process_parsed_alert.
        assert all(call[0] != "Could not get entry price" for call in warnings)


# ---------------------------------------------------------------------------
# Integration: _process_parsed_alert emits enriched "Could not get entry price"
# log including the fallback_used label.
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestEntryPriceMissingLogging:
    @pytest.mark.asyncio
    async def test_log_includes_fallback_used_label(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        manager = _CaptureManager()
        consumer = AlertWatchConsumer(redis_client=_NoopRedis(), watch_manager=manager)
        consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
        consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]
        # No calendar -> no stale-window short circuit.
        consumer.manager.calendar = None  # type: ignore[attr-defined]

        warnings: list[tuple[str, dict]] = []
        monkeypatch.setattr(
            consumer_module.logger,
            "warning",
            lambda msg, **kw: warnings.append((msg, kw)),
        )
        monkeypatch.setattr(consumer_module.logger, "info", lambda *a, **kw: None)

        await consumer._process_parsed_alert(
            {
                "id": "a-no-price",
                "occ_symbol": "AAPL260116C00200000",
                "underlying": "AAPL",
                "put_call": "C",
                "expiry": "2026-01-16",
                "strike": 200.0,
                "spot_px": 195.0,
                "contract_px": 0.0,
                "ts_event": None,
            }
        )

        # The watch was still created (fallback floor=1.0) and the warning fired.
        assert len(manager.calls) == 1
        missing_logs = [c for c in warnings if c[0] == "Could not get entry price"]
        assert len(missing_logs) == 1
        fields = missing_logs[0][1]
        assert fields["event_id"] == "a-no-price"
        assert fields["occ_symbol"] == "AAPL260116C00200000"
        assert fields["underlying"] == "AAPL"
        assert fields["fallback_used"] == "exhausted"
