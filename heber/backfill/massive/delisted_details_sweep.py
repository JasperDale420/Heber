#!/usr/bin/env python3
"""Recover delisted tickers' details (incl. sic_code/sector) via the as-of ``date=`` param.

The plain ``/v3/reference/tickers/{t}`` endpoint 404s for delisted tickers, but querying with
``date=<a day during the ticker's trading life>`` returns the full historical detail (sector/SIC,
name, etc.). Reads the captured ``tickers_inactive`` universe (which carries ``delisted_utc``) and
fetches each delisted ticker as-of the day before its delisting. Writes ``delisted_details.jsonl.gz``.
Threaded, resumable, stdlib-only — this makes sector/industry features survivorship-consistent.
"""

import argparse
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

BASE = "https://api.massive.com"


def fetch(url: str, key: str, max_retries: int = 5):
    for attempt in range(max_retries):
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode("utf-8")), 200
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None, 404
            if e.code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                time.sleep(min(30, 2**attempt))
                continue
            return None, e.code
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(min(30, 2**attempt))
                continue
            return None, -1
    return None, -1


def load_delisted(corp_root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in sorted((corp_root / "tickers_inactive").glob("page_*.json.gz")):
        data = json.loads(gzip.decompress(p.read_bytes()).decode("utf-8"))
        for r in data.get("results") or []:
            if r.get("ticker") and r.get("delisted_utc"):
                out[r["ticker"]] = r["delisted_utc"]
    return out


def asof_date(delisted_utc: str) -> str:
    d = datetime.fromisoformat(delisted_utc.replace("Z", "+00:00")).date() - timedelta(days=1)
    return d.isoformat()


def load_done(path: Path) -> set[str]:
    done: set[str] = set()
    if path.exists():
        for line in gzip.decompress(path.read_bytes()).decode("utf-8").splitlines():
            try:
                done.add(json.loads(line)["_ticker"])
            except (ValueError, KeyError):
                continue
    return done


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corp-root", required=True)
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()
    key = os.environ["MASSIVE_API_KEY"]

    corp = Path(args.corp_root)
    out_path = corp / "delisted_details.jsonl.gz"
    universe = load_delisted(corp)
    done = load_done(out_path)
    todo = [(t, du) for t, du in universe.items() if t not in done]
    print(f"delisted={len(universe)} done={len(done)} todo={len(todo)}", flush=True)

    c = {"n": 0, "ok": 0, "miss": 0, "err": 0}

    def work(item: tuple[str, str]):
        t, du = item
        d, code = fetch(f"{BASE}/v3/reference/tickers/{t}?date={asof_date(du)}", key)
        return t, d, code

    with gzip.open(out_path, "at", encoding="utf-8") as fh, ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in as_completed([ex.submit(work, it) for it in todo]):
            t, d, code = fut.result()
            c["n"] += 1
            if d and d.get("results"):
                fh.write(json.dumps(dict(d["results"], _ticker=t)) + "\n")
                c["ok"] += 1
            else:
                fh.write(json.dumps({"_ticker": t, "_status": code}) + "\n")
                c["miss" if code == 404 else "err"] += 1
            if c["n"] % 2000 == 0:
                fh.flush()
                print(f"  {c['n']}/{len(todo)} ok={c['ok']} miss={c['miss']} err={c['err']}", flush=True)

    (corp / "delisted_details_manifest.json").write_text(json.dumps({"delisted": len(universe), **c}))
    print(f"DONE ok={c['ok']} miss={c['miss']} err={c['err']}", flush=True)


if __name__ == "__main__":
    main()
