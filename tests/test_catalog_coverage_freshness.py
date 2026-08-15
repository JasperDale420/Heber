"""Stale catalog coverage must be visible somewhere.

`data_coverage` has been frozen at 2026-07-20 for 24 days while
`catalog_periodic_scan_error` fired 232 times in a single container lifetime —
and `/health` returned 200 throughout, because it only runs `SELECT 1`.
`dataflow_health`'s `catalog_health` check polls that same endpoint, so the
one report that runs every five minutes reported the catalog as fine.

Freshness is reported on its own route rather than folded into `/health`.
`/health` is the container healthcheck and gates `heber-consumer` and
`heber-backfill-consumer` via `depends_on: service_healthy`, and the host
watchdog restarts anything unhealthy — so failing it on stale data would
restart the catalog in a loop over a data-freshness problem and could block
ingest from starting. Liveness and freshness are different questions.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from heber.catalog import api as catalog_api


def _session_returning(last_updated: datetime | None, rows: int = 14303, stalest: str = "quotes"):
    """Stub async_session whose scalar/first answers the coverage query.

    The query reports the OLDEST per-feed scan and names that feed, so the row
    is (min_last_updated_ts, feed_count, stalest_feed_name).
    """

    @asynccontextmanager
    async def _stub():
        session = AsyncMock()
        result = MagicMock()
        result.first.return_value = (last_updated, rows, stalest)
        result.scalar_one_or_none.return_value = last_updated
        session.execute = AsyncMock(return_value=result)
        yield session

    return _stub


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(catalog_api.settings, "metrics_port", 0)
    return TestClient(catalog_api.app)


def test_fresh_coverage_reports_ok(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(catalog_api, "async_session", _session_returning(datetime.now(UTC)))

    resp = client.get("/health/coverage")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["coverage_age_seconds"] < 60
    assert body["rows"] == 14303


def test_stale_coverage_reports_unhealthy(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """The live condition: 24 days stale while every other signal is green."""
    monkeypatch.setattr(catalog_api, "async_session", _session_returning(datetime.now(UTC) - timedelta(days=24)))

    resp = client.get("/health/coverage")

    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "stale"
    assert body["coverage_age_seconds"] > 20 * 86400


def test_never_scanned_reports_unhealthy(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty coverage table is not 'fresh'."""
    monkeypatch.setattr(catalog_api, "async_session", _session_returning(None, rows=0))

    resp = client.get("/health/coverage")

    assert resp.status_code == 503
    assert resp.json()["coverage_age_seconds"] is None


def test_liveness_route_is_unchanged_by_stale_coverage(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """/health gates ingest startup and drives watchdog restarts.

    Stale data must not make it fail, or a freshness problem becomes a restart
    loop and blocks `heber-consumer` from starting.
    """
    monkeypatch.setattr(catalog_api, "async_session", _session_returning(datetime.now(UTC) - timedelta(days=24)))

    resp = client.get("/health")

    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


class TestDataflowReportSurfacesStaleCoverage:
    """The 5-minute report must carry the signal, or nothing looks at it.

    `_catalog_health_check` polls `/health`, which stayed 200 for 24 days of
    frozen coverage — so the report that runs every five minutes said the
    catalog was fine the whole time.
    """

    def test_report_carries_the_coverage_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Asserts the generated report, not just that the function exists.

        An earlier version of this test only checked for the attribute, so
        deleting the call that wires it into the report broke nothing.
        """
        from heber.ops import dataflow_health as dfh

        monkeypatch.setattr(
            dfh,
            "_catalog_coverage_check",
            lambda _s: {"id": "catalog_coverage", "status": "fail", "severity": "critical"},
        )
        monkeypatch.setattr(dfh, "_is_market_open", lambda _ts: False)

        report = dfh.generate_dataflow_report(
            window_seconds=900, mode="manual", now=datetime(2026, 2, 12, 15, 0, tzinfo=UTC)
        )

        ids = [c["id"] for c in report["checks"]]
        assert "catalog_coverage" in ids, f"report does not carry the coverage check: {ids}"

    def test_coverage_check_fails_when_the_endpoint_reports_stale(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from heber.config import get_settings
        from heber.ops import dataflow_health

        class _Resp:
            status_code = 503

            @staticmethod
            def json() -> dict:
                return {"status": "stale", "coverage_age_seconds": 2_073_600, "rows": 14303}

        class _Client:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def get(self, _url):
                return _Resp()

        monkeypatch.setattr(dataflow_health, "create_http_client", lambda **_: _Client())

        result = dataflow_health._catalog_coverage_check(get_settings())

        assert result["status"] == "fail"
        assert result["id"] == "catalog_coverage"


def test_the_stalest_feed_is_named(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Knowing coverage is stale is not much use without knowing which feed."""
    monkeypatch.setattr(
        catalog_api,
        "async_session",
        _session_returning(datetime.now(UTC) - timedelta(days=2), stalest="quotes"),
    )

    body = client.get("/health/coverage").json()

    assert body["status"] == "stale"
    assert body["stalest_feed"] == "quotes"


def test_one_busy_feed_cannot_mask_a_neglected_one(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """The old query took max() over all rows, so any single write read as fresh.

    That is how the walk stalling at feed=quotes stayed invisible: every feed
    after it alphabetically went unscanned while earlier feeds kept the maximum
    current. Reporting the oldest per-feed scan is what makes that visible.
    """
    monkeypatch.setattr(
        catalog_api,
        "async_session",
        _session_returning(datetime.now(UTC) - timedelta(days=3), stalest="trades"),
    )

    resp = client.get("/health/coverage")

    assert resp.status_code == 503
    assert resp.json()["stalest_feed"] == "trades"
