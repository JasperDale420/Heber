#!/usr/bin/env python3
"""Plan 2 (per-ticker): sweep Ticker Details (sector/SIC) + Ticker Events (symbol changes).

Reads the captured ticker universe (tickers_active + tickers_inactive page JSON) and pulls, per
ticker, ``/v3/reference/tickers/{t}`` (details: sic_code, cik, list_date, ...) and
``/vX/reference/tickers/{t}/events`` (symbol-change history). Threaded for throughput at the
unlimited paid rate. Writes:
  {out}/ticker_details.jsonl.gz  — one details record per ticker (incl. a stub for 404s)
  {out}/ticker_events.jsonl.gz   — one record per ticker that HAS events
Resumable: tickers already present in ticker_details.jsonl.gz are skipped. Stdlib-only.
"""

import argparse
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = "https://api.massive.com"


def fetch(url: str, key: str, max_retries: int = 5):
    """Return (json_or_None, http_code). 404 -> (None, 404); exhausted -> (None, code/-1)."""
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


def load_universe(corp_root: Path) -> list[str]:
    tickers: set[str] = set()
    for ds in ("tickers_active", "tickers_inactive"):
        for p in sorted((corp_root / ds).glob("page_*.json.gz")):
            data = json.loads(gzip.decompress(p.read_bytes()).decode("utf-8"))
            for r in data.get("results") or []:
                if r.get("ticker"):
                    tickers.add(r["ticker"])
    return sorted(tickers)


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
    ap.add_argument("--corp-root", required=True, help="massive_corp_actions dir (has tickers_*/)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()
    key = os.environ["MASSIVE_API_KEY"]

    corp_root = Path(args.corp_root)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    details_path = out / "ticker_details.jsonl.gz"
    events_path = out / "ticker_events.jsonl.gz"

    tickers = load_universe(corp_root)
    done = load_done(details_path)
    todo = [t for t in tickers if t not in done]
    print(f"universe={len(tickers)} done={len(done)} todo={len(todo)}", flush=True)

    def work(t: str):
        det, dc = fetch(f"{BASE}/v3/reference/tickers/{t}", key)
        ev, ec = fetch(f"{BASE}/vX/reference/tickers/{t}/events", key)
        return t, det, dc, ev

    c = {"n": 0, "details": 0, "events": 0, "errors": 0}
    with gzip.open(details_path, "at", encoding="utf-8") as dfh, gzip.open(events_path, "at", encoding="utf-8") as efh:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for fut in as_completed([ex.submit(work, t) for t in todo]):
                t, det, dc, ev = fut.result()
                c["n"] += 1
                if det and det.get("results"):
                    rec = dict(det["results"], _ticker=t)
                    dfh.write(json.dumps(rec) + "\n")
                    c["details"] += 1
                else:
                    dfh.write(json.dumps({"_ticker": t, "_status": dc}) + "\n")
                    if dc not in (200, 404):
                        c["errors"] += 1
                evlist = ((ev or {}).get("results") or {}).get("events")
                if evlist:
                    efh.write(json.dumps(dict(ev["results"], _ticker=t)) + "\n")
                    c["events"] += 1
                if c["n"] % 1000 == 0:
                    dfh.flush()
                    efh.flush()
                    print(f"  {c['n']}/{len(todo)} det={c['details']} ev={c['events']} err={c['errors']}", flush=True)

    (out / "ticker_meta_manifest.json").write_text(json.dumps({"universe": len(tickers), **c}))
    print(f"DONE details={c['details']} events={c['events']} errors={c['errors']}", flush=True)


if __name__ == "__main__":
    main()
