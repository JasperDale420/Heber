# Audit — stranded pending deliveries, 2026-07-29

Captured with `scripts/debug/audit_stranded_pel.py` at 2026-07-30T01:10:18Z. Read-only:
nothing was claimed, acknowledged, or purged, because the pending-entry list (PEL)
is the only surviving record of these deliveries.

## Result

| Stream | Group | Pending | Unrecoverable | Still recoverable | Retention |
|---|---|---|---|---|---|
| `heber:events` | `heber-writers` | 32,000 | **32,000** | 0 | 91.4 min |
| `heber:events` | `watch-consumer` | 2,745 | **2,745** | 0 | 91.4 min |
| `heber:events:backfill` | `heber-backfill-writers` | 5,000 | 0 | **5,000** | 7,202.9 min (5 days) |

**34,745 deliveries are permanently unrecoverable.** Their stream entries have been
trimmed, so `XCLAIM`/`XAUTOCLAIM` cannot return them and the payloads are gone.

**5,000 deliveries on the backfill stream are still recoverable** and have been held by
a dead consumer (`consumer-20260723213008-45314fed`) since 2026-07-23 — six days. That
stream carries a 2,000,000 MAXLEN against much lower throughput, so its retention window
is 5 days rather than 91 minutes. These 5,000 are rescuable by a working recovery drain
and are the concrete payoff for that fix.

## Affected windows

- `heber-writers` — 2026-07-29 16:12:10Z to 19:29:25Z (a ~3h17m span inside the
  2026-07-29 session, 12:12–15:29 ET).
- `watch-consumer` — 2026-07-22 16:53:09Z to 2026-07-29 19:30:05Z. A week of
  accumulation, consistent with that consumer having no recovery path at all.

Ownership of the live-stream backlog:

| Consumer | Pending | Note |
|---|---|---|
| `consumer-20260729172140-548ec393` | 15,000 | the **currently running** consumer |
| `consumer-20260729161620-9de8b65b` | 10,000 | dead |
| `consumer-20260727161438-e5cb0d99` | 7,000 | dead |

The running consumer holding 15,000 of its own stranded entries confirms this is not
merely a dead-consumer-cleanup problem: a live process orphans its own batches.

## Corroboration of the diagnosed mechanism

An earlier sample the same day recorded 36,800 pending for `heber-writers`, with
`consumer-20260727161438-e5cb0d99` holding 11,800. Four hours later that consumer holds
7,000 — a reduction of exactly 4,800.

Four hours is 48 recovery cycles at `redis_recover_interval_seconds=300`, and
48 × `redis_claim_batch_size=100` = 4,800.

That is an exact match for the predicted failure: the drain purges exactly one batch of
dangling entries per cycle and then breaks out of its loop, because
`_recover_pending_batch` returns `0` when `XCLAIM` claims nothing and
`_recover_pending_messages` treats `0` as "drain complete". At 100 per 5 minutes against
a 91-minute retention window, every orphaned batch on the live stream is trimmed long
before recovery reaches it.

## What this audit cannot establish

Which specific events were lost, from which feeds, and how many rows are missing.

A stream id carries only the XADD time. Once the entry is trimmed, the feed, symbol and
payload are gone, and a PEL entry proves delivery — not that the write failed. Some of
these were very likely written to Bronze and simply died before the acknowledgement.
Comparing Bronze row counts against neighbouring hours cannot separate a real loss from a
quiet market, an upstream outage, or a feed that was never sent.

Establishing per-feed loss would require a provider-side re-pull over the two windows
above and a diff against Bronze. That is separate work and is not attempted here. Any
figure produced without it would be a guess presented as a measurement.

## Standing constraint

Do not run `XAUTOCLAIM`, `XCLAIM`, or `XGROUP DELCONSUMER` against these groups until
this capture is preserved. `XGROUP DELCONSUMER` in particular destroys a consumer's
pending entries along with the consumer.
