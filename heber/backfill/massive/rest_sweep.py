#!/usr/bin/env python3
"""Plan 2: sweep Massive REST corporate actions + ticker universe into the vendor-raw archive.

Captures the inputs that make the bar corpus survivorship-free and self-adjustable:
  - tickers_active   : current US-stock universe
  - tickers_inactive : DELISTED tickers (with delisted_utc) — the survivorship universe
  - splits           : every stock split (adjustment factors)
  - dividends        : every dividend (adjustment + yield)

Stdlib-only (urllib) so it runs with plain ``python3`` — no env sync needed, and it never
imports numpy/pandas. Writes one gzipped raw JSON page per request under
``{out}/{dataset}/page_NNNNN.json.gz`` plus a manifest; a ``.done`` marker makes each dataset
resumable (a completed dataset is skipped on re-run). Auth via the Bearer token in
``MASSIVE_API_KEY``. next_url pagination is host-pinned to api.massive.com.
"""

import argparse
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlencode, urlsplit

BASE = "https://api.massive.com"
API_HOST = urlsplit(BASE).netloc

# dataset name -> (path, query params). Bulk = no ticker filter.
DATASETS: dict[str, tuple[str, dict[str, str]]] = {
    "tickers_active": ("/v3/reference/tickers", {"market": "stocks", "active": "true", "limit": "1000"}),
    "tickers_inactive": ("/v3/reference/tickers", {"market": "stocks", "active": "false", "limit": "1000"}),
    "splits": ("/v3/reference/splits", {"limit": "1000", "sort": "execution_date", "order": "asc"}),
    "dividends": ("/v3/reference/dividends", {"limit": "1000", "sort": "ex_dividend_date", "order": "asc"}),
    "financials": ("/vX/reference/financials", {"limit": "100", "sort": "filing_date", "order": "asc"}),
    "ipos": ("/vX/reference/ipos", {"limit": "1000"}),
}

RETRYABLE = {429, 500, 502, 503, 504}


def fetch(url: str, key: str, max_retries: int = 7) -> dict:
    """GET a page with Bearer auth, retrying transient failures with exponential backoff."""
    for attempt in range(max_retries):
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}"})
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in RETRYABLE and attempt < max_retries - 1:
                wait = min(60, 2**attempt)
                print(f"  retry HTTP {e.code} in {wait}s", flush=True)
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(min(60, 2**attempt))
                continue
            raise
    raise RuntimeError(f"exhausted retries for {url[:90]}")


def sweep(name: str, path: str, params: dict[str, str], out_root: Path, key: str) -> None:
    out = out_root / name
    if (out / ".done").exists():
        print(f"[{name}] already complete — skipping", flush=True)
        return
    out.mkdir(parents=True, exist_ok=True)
    url: str | None = f"{BASE}{path}?{urlencode(params)}"
    page = 0
    total = 0
    while url:
        data = fetch(url, key)
        status = data.get("status")
        if status not in ("OK", "DELAYED", None):
            raise RuntimeError(f"[{name}] bad status {status!r}: {str(data)[:200]}")
        results = data.get("results") or []
        total += len(results)
        (out / f"page_{page:05d}.json.gz").write_bytes(gzip.compress(json.dumps(data).encode("utf-8")))
        page += 1
        if page % 25 == 0:
            print(f"[{name}] page {page}, {total} records so far", flush=True)
        nxt = data.get("next_url")
        if nxt and urlsplit(nxt).netloc != API_HOST:
            raise RuntimeError(f"[{name}] next_url host mismatch: {nxt}")
        url = nxt or None
    (out / "manifest.json").write_text(json.dumps({"dataset": name, "pages": page, "records": total}))
    (out / ".done").write_text("")
    print(f"[{name}] DONE — {page} pages, {total} records", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Sweep Massive REST corporate actions + ticker universe")
    ap.add_argument("--out", required=True, help="archive root for corp-actions JSON")
    ap.add_argument("--datasets", nargs="*", default=list(DATASETS), choices=list(DATASETS))
    args = ap.parse_args()

    key = os.environ.get("MASSIVE_API_KEY")
    if not key:
        sys.exit("MASSIVE_API_KEY not set")

    out_root = Path(args.out)
    for name in args.datasets:
        path, params = DATASETS[name]
        sweep(name, path, params, out_root, key)
    print("ALL DONE", flush=True)


if __name__ == "__main__":
    main()
