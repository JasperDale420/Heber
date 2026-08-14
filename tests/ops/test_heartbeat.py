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
