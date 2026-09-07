"""Shared urllib-based HTTP retry helpers for Massive REST capture modules.

``corporate_actions.py`` (daily splits/dividends/tickers capture) and
``rest_backlog.py`` (resumable backlog sweep) both hit the Massive REST API
with the same Bearer-auth GET, the same transient-error retry policy, and the
same gzip-JSON page writer. This module holds the one copy so the two capture
paths cannot silently drift apart.
"""

from __future__ import annotations

import gzip
import json
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

BASE_URL = "https://api.massive.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
HttpGet = Callable[[str, dict[str, str], float], dict[str, Any]]


def urllib_get_json(url: str, headers: dict[str, str], timeout: float) -> dict[str, Any]:
    """Default ``HttpGet`` implementation: plain stdlib GET returning parsed JSON."""
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return cast(dict[str, Any], json.loads(resp.read().decode("utf-8")))


def write_json_gz(path: Path, payload: dict[str, Any]) -> None:
    """Atomically write ``payload`` as gzip-compressed JSON to ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)
    os.replace(tmp, path)


def fetch_with_retry(
    http_get: HttpGet,
    url: str,
    *,
    api_key: str,
    timeout_seconds: float,
    max_retries: int,
    max_sleep_seconds: float,
) -> dict[str, Any]:
    """GET ``url`` with Bearer auth, retrying transient failures with capped exponential backoff."""
    headers = {"Authorization": f"Bearer {api_key}"}
    for attempt in range(max_retries):
        try:
            return http_get(url, headers, timeout_seconds)
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                time.sleep(min(max_sleep_seconds, 2**attempt))
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(min(max_sleep_seconds, 2**attempt))
                continue
            raise
    raise RuntimeError(f"exhausted Massive REST retries for {url[:120]}")


def validate_next_url(next_url: str, api_host: str) -> None:
    """Raise if a Massive ``next_url`` pagination link points somewhere unexpected."""
    parsed = urlsplit(next_url)
    if parsed.netloc and parsed.netloc != api_host:
        raise RuntimeError(f"Massive next_url host changed unexpectedly: {parsed.netloc}")
