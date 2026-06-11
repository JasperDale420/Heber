#!/usr/bin/env python3
"""Microbenchmark for the Redis-backed dedupe store (audit F-6 follow-up).

Measures the per-event cost of the exact-match dedupe path that
``EventConsumer`` would pay if ``dedupe_redis_enabled`` were turned on, and
projects that cost as a fraction of the single-threaded consumer's wall-clock
budget at the average and peak ingest rates.

What is measured (10k unique events each):

1. ``RedisDedupeStore.add`` / ``.contains`` in isolation.
2. ``EventDeduplicator.check_and_register`` with a Redis backing store
   (Bloom + Redis) vs. the Bloom-only baseline.
3. A pipelined ``add`` variant (redis-py ``pipeline()``, batch 100) — the
   alternative W10 would implement if the synchronous path is too slow.

Overhead projection (the consumer is a single asyncio loop, so each added
second of work directly displaces ingest throughput):

    overhead_percent = added_seconds_per_event * events_per_second * 100

at AVG_EVENTS_PER_SEC (1,215) and PEAK_EVENTS_PER_SEC (5,000). The <5% gate
from the audit decides enable / enable-with-pipelining / keep-off.

Redis selection (``--url``, default ``redis://localhost:6379``):

  - If the chosen URL is reachable, the benchmark runs against it directly.
    The native deployment Redis is the LIVE Data-Gateway event bus, so this
    script NEVER calls ``FLUSHDB`` and NEVER touches streams. All keys it
    writes live under the dedicated ``heber:dedupe:bench:`` prefix and are
    DELETEd (by SCAN over that prefix) before and after every phase.
  - If the URL is unreachable it starts an ephemeral ``redis:7-alpine``
    container on host port 16399 and tears it down on exit.
"""

import argparse
import subprocess
import time
import uuid
from contextlib import contextmanager

import redis as redis_sync

from heber.ops.reliability import EventDeduplicator, RedisDedupeStore

N_EVENTS = 10_000
PIPELINE_BATCH = 100
TTL_SECONDS = 60  # short TTL: bench keys self-expire even if cleanup is skipped

AVG_EVENTS_PER_SEC = 1_215
PEAK_EVENTS_PER_SEC = 5_000
OVERHEAD_GATE_PERCENT = 5.0

# Dedicated prefix so bench keys never collide with real dedupe keys
# (``heber:dedupe:``) and can be deleted precisely on the live event bus.
BENCH_KEY_PREFIX = "heber:dedupe:bench:"

DEFAULT_URL = "redis://localhost:6379"
EPHEMERAL_PORT = 16399
EPHEMERAL_URL = f"redis://localhost:{EPHEMERAL_PORT}"
EPHEMERAL_NAME = "bench-dedupe-redis"


def _ping(url: str) -> bool:
    try:
        client = redis_sync.Redis.from_url(url, socket_connect_timeout=1)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


@contextmanager
def redis_endpoint(url: str):
    """Yield a reachable redis URL, starting an ephemeral container if needed."""
    if _ping(url):
        print(f"Using existing Redis at {url}")
        yield url
        return

    print(f"{url} unreachable — starting ephemeral container '{EPHEMERAL_NAME}'")
    subprocess.run(
        ["docker", "rm", "-f", EPHEMERAL_NAME],
        check=False,
        capture_output=True,
    )
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            EPHEMERAL_NAME,
            "-p",
            f"{EPHEMERAL_PORT}:6379",
            "redis:7-alpine",
        ],
        check=True,
        capture_output=True,
    )
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            if _ping(EPHEMERAL_URL):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("ephemeral Redis did not become ready within 30s")
        print(f"Using ephemeral Redis at {EPHEMERAL_URL}")
        yield EPHEMERAL_URL
    finally:
        print(f"Tearing down ephemeral container '{EPHEMERAL_NAME}'")
        subprocess.run(
            ["docker", "rm", "-f", EPHEMERAL_NAME],
            check=False,
            capture_output=True,
        )


def _ids(n: int) -> list[str]:
    return [uuid.uuid4().hex for _ in range(n)]


def _clear_bench_keys(client: redis_sync.Redis) -> int:
    """Delete only ``heber:dedupe:bench:*`` keys — never FLUSHDB the live bus."""
    deleted = 0
    cursor = 0
    while True:
        cursor, keys = client.scan(cursor=cursor, match=f"{BENCH_KEY_PREFIX}*", count=1000)
        if keys:
            deleted += client.delete(*keys)
        if cursor == 0:
            break
    return deleted


def _report(label: str, elapsed: float, n: int) -> float:
    """Print ops/sec + per-event microseconds. Returns seconds-per-event."""
    per_event = elapsed / n
    ops_per_sec = n / elapsed
    print(f"  {label:<38} {ops_per_sec:>12,.0f} ops/s   {per_event * 1e6:>9.2f} us/event")
    return per_event


def bench_store_primitives(url: str, ids: list[str]) -> None:
    print("\n[1] RedisDedupeStore primitives (10k events)")
    client = redis_sync.Redis.from_url(url)
    _clear_bench_keys(client)
    store = RedisDedupeStore(redis_url=url, ttl_seconds=TTL_SECONDS, key_prefix=BENCH_KEY_PREFIX, client=client)

    start = time.perf_counter()
    for eid in ids:
        store.add(eid)
    _report("add (SET nx ex)", time.perf_counter() - start, len(ids))

    start = time.perf_counter()
    for eid in ids:
        store.contains(eid)
    _report("contains (EXISTS)", time.perf_counter() - start, len(ids))
    _clear_bench_keys(client)
    client.close()


def bench_pipelined_add(url: str, ids: list[str]) -> float:
    print(f"\n[2] Pipelined add (batch {PIPELINE_BATCH}, 10k events)")
    client = redis_sync.Redis.from_url(url)
    _clear_bench_keys(client)

    start = time.perf_counter()
    for i in range(0, len(ids), PIPELINE_BATCH):
        pipe = client.pipeline(transaction=False)
        for eid in ids[i : i + PIPELINE_BATCH]:
            pipe.set(f"{BENCH_KEY_PREFIX}{eid}", 1, ex=TTL_SECONDS, nx=True)
        pipe.execute()
    per_event = _report("pipelined add", time.perf_counter() - start, len(ids))
    _clear_bench_keys(client)
    client.close()
    return per_event


def bench_deduplicator(url: str, ids: list[str]) -> tuple[float, float]:
    print("\n[3] EventDeduplicator.check_and_register (10k unique events)")

    # Bloom-only baseline.
    dedup_bloom = EventDeduplicator(backing_store=None)
    start = time.perf_counter()
    for eid in ids:
        dedup_bloom.check_and_register(eid)
    bloom_per_event = _report("bloom-only baseline", time.perf_counter() - start, len(ids))

    # Bloom + Redis backing store.
    client = redis_sync.Redis.from_url(url)
    _clear_bench_keys(client)
    store = RedisDedupeStore(redis_url=url, ttl_seconds=TTL_SECONDS, key_prefix=BENCH_KEY_PREFIX, client=client)
    dedup_redis = EventDeduplicator(backing_store=store)
    start = time.perf_counter()
    for eid in ids:
        dedup_redis.check_and_register(eid)
    redis_per_event = _report("bloom + redis", time.perf_counter() - start, len(ids))
    _clear_bench_keys(client)
    client.close()

    return bloom_per_event, redis_per_event


def project_overhead(label: str, added_seconds_per_event: float) -> None:
    print(f"\n[projection] {label}: added {added_seconds_per_event * 1e6:.2f} us/event")
    for rate, name in ((AVG_EVENTS_PER_SEC, "avg"), (PEAK_EVENTS_PER_SEC, "peak")):
        overhead_pct = added_seconds_per_event * rate * 100
        verdict = "OK" if overhead_pct < OVERHEAD_GATE_PERCENT else "OVER GATE"
        print(
            f"    {name:>4} {rate:>5,} ev/s -> "
            f"{added_seconds_per_event * 1e6:.2f}us * {rate:,} = {overhead_pct:6.2f}%  [{verdict}]"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"Redis URL to benchmark (default {DEFAULT_URL}). If unreachable, "
        "an ephemeral redis:7-alpine container is used instead.",
    )
    args = parser.parse_args()

    ids = _ids(N_EVENTS)
    with redis_endpoint(args.url) as url:
        bench_store_primitives(url, ids)
        pipelined_per_event = bench_pipelined_add(url, ids)
        bloom_per_event, redis_per_event = bench_deduplicator(url, ids)

    added_sync = redis_per_event - bloom_per_event
    # Pipelined register adds the Bloom cost (still synchronous) plus the
    # amortized pipelined Redis write, minus the bloom-only baseline.
    added_pipelined = (bloom_per_event + pipelined_per_event) - bloom_per_event

    print("\n=== Overhead vs bloom-only baseline ===")
    project_overhead("synchronous register (bloom + redis)", added_sync)
    project_overhead("pipelined register (batch 100)", added_pipelined)

    print("\nGate: enable if projected overhead < 5% at both avg and peak rates.")


if __name__ == "__main__":
    main()
