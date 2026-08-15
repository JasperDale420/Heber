"""Off-machine dead-man heartbeat for the host-side alert job."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from heber.ops.heartbeat import ping

pytestmark = pytest.mark.unit

URL = "https://hc-ping.example/abc-123"


def test_success_pings_the_bare_url():
    client = MagicMock()
    assert ping(URL, ok=True, client=client) is True
    client.get.assert_called_once_with(URL)


def test_failure_pings_the_fail_endpoint():
    """A live process that knows it is broken should not wait out the grace period."""
    client = MagicMock()
    ping(URL, ok=False, client=client)
    client.get.assert_called_once_with(f"{URL}/fail")


def test_empty_url_disables_the_ping():
    client = MagicMock()
    assert ping("", ok=True, client=client) is False
    client.get.assert_not_called()


def test_whitespace_url_disables_the_ping():
    client = MagicMock()
    assert ping("   ", ok=True, client=client) is False
    client.get.assert_not_called()


def test_a_dead_heartbeat_service_never_breaks_the_caller():
    """The alarm must survive its own monitoring being down."""
    client = MagicMock()
    client.get.side_effect = httpx.ConnectError("no route")
    assert ping(URL, ok=True, client=client) is False


def test_a_trailing_slash_does_not_produce_a_double_slash_fail_url():
    client = MagicMock()
    ping(f"{URL}/", ok=False, client=client)
    client.get.assert_called_once_with(f"{URL}/fail")


# --- GitHub repo-variable heartbeat ------------------------------------------


from datetime import UTC, datetime  # noqa: E402

from heber.ops.heartbeat import GITHUB_VARIABLE, ping_github  # noqa: E402

T0 = datetime(2026, 8, 14, 9, 30, 0, tzinfo=UTC)
REPO = "owner/repo"


def _runner(returncode=0, exc=None):
    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        if exc:
            raise exc
        return MagicMock(returncode=returncode, stderr="")

    run.calls = calls
    return run


def test_records_a_healthy_beat_with_its_timestamp():
    run = _runner()
    assert ping_github(REPO, ok=True, now=T0, runner=run) is True
    cmd = run.calls[0]
    assert cmd[:3] == ["gh", "variable", "set"]
    assert GITHUB_VARIABLE in cmd
    assert "--repo" in cmd and REPO in cmd
    body = cmd[cmd.index("--body") + 1]
    assert body == "2026-08-14T09:30:00+00:00 ok"


def test_records_a_failing_beat():
    """The watcher must distinguish 'alarm is broken' from 'alarm is gone'."""
    run = _runner()
    ping_github(REPO, ok=False, now=T0, runner=run)
    body = run.calls[0][run.calls[0].index("--body") + 1]
    assert body.endswith(" fail")


def test_empty_repo_disables_the_beat():
    run = _runner()
    assert ping_github("", ok=True, now=T0, runner=run) is False
    assert run.calls == []


def test_a_nonzero_gh_exit_is_reported_not_raised():
    run = _runner(returncode=1)
    assert ping_github(REPO, ok=True, now=T0, runner=run) is False


def test_gh_being_missing_or_hanging_never_breaks_the_caller():
    for exc in (FileNotFoundError("no gh"), OSError("boom")):
        assert ping_github(REPO, ok=True, now=T0, runner=_runner(exc=exc)) is False


def test_the_body_is_parseable_by_the_watcher():
    """The workflow splits on whitespace: field 1 ISO timestamp, field 2 status."""
    run = _runner()
    ping_github(REPO, ok=True, now=T0, runner=run)
    body = run.calls[0][run.calls[0].index("--body") + 1]
    stamp, status = body.split()
    assert datetime.fromisoformat(stamp) == T0
    assert status in ("ok", "fail")
