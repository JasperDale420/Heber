# Postmortem: July 10–19 volume drop, power outage, and 6.5-day total outage

**Date written:** 2026-07-19
**Window:** 2026-07-10 18:15 PDT → 2026-07-19 07:30 PDT
**Status at writing:** Heber stack recovered and ingesting; open P0s remain (see Remediation)
**Method:** 7 parallel log sweeps across all services + host logs + on-disk integrity checks,
synthesized and then adversarially reviewed by 3 independent verification passes
(causality / completeness / impact-accuracy). Corrections from that review are
incorporated below and called out in "Adversarial review corrections."

---

## Summary

Two distinct incidents, back to back:

1. **Jul 10 evening (PDT): transient failure of the external exFAT volume `/Volumes/heber`.**
   Host-side writes began failing with EPERM at 18:15 PDT; by 19:00 the volume was in a
   zombie/unmounted state for containers too. heber-postgres PANICked at 19:03 PDT and
   **stayed dead for 8.5 days** (Docker could not recreate its bind mount while the volume
   was gone, and nothing retried after the volume auto-remounted ~19:30 PDT). Every other
   service self-recovered. **No lakehouse data was lost** — writer buffers retained all
   events across 30 minutes of failed flushes and wrote them at recovery (verified at
   row level).

2. **Jul 12 ~17:50 PDT: abrupt machine power-off** (no shutdown records in `last`;
   mid-write log truncation across all services). Machine stayed off ~6.5 days spanning
   trading days **Mon Jul 13 – Fri Jul 17**. On boot (Jul 19 06:24 PDT) the external
   volume did not mount for ~22 minutes, so every container binding `/Volumes/heber`
   failed to start; the `docker-watchdog` LaunchAgent brought the stack back at 07:20 PDT
   once the volume was mounted. Redis (no persistence) restarted **empty**.

**Catalog was serving 100% HTTP 500s for ~46.8 hours while reporting "healthy"** (its
/health does not exercise the DB), and the dataflow-health monitor reported `overall_status=ok`
every 5 minutes for the entire dead-postgres weekend (no postgres check + market-closed
mode skips freshness checks). Monitoring was structurally blind to both incidents.

---

## Timeline (condensed; PDT primary)

| Time (PDT) | Event |
|---|---|
| Jun 26 21:43 | PRE-EXISTING: heber-postgres missing-relation-file errors begin (`base/16384/6104`); two crash recoveries that day |
| Jul 5–10 (daily) | PRE-EXISTING: live stream `heber:events` pegged at 100% consumer lag with MAXLEN eviction during market hours (UW bulk backfill flooding); last occurrence Jul 10 08:13 PDT |
| Jul 6–10 (daily) | PRE-EXISTING: gold-poller `market_regime` pipeline fails every EOD run (1800s timeout / SIGKILL); health-monitor crash-loops (32 silent deaths); compactor killed 6× |
| Jul 10 09:17 | Commit 96ad9e7 (heber-backfill-consumer, P0 eviction fix) lands — **never deployed; container never created** |
| Jul 10 18:15:47 | **INCIDENT 1 START:** host-side writes to `/Volumes/heber` begin failing EPERM (container writes still OK for ~45 min) |
| Jul 10 19:00:23 | Volume failure reaches containers: postgres EPERM on `pg_filenode.map`; consumer Bronze/Silver flushes fail (1,969 ERRORs over 30 min); compactor sees "0 partitions"; orion_heber_sync silently syncs 0 |
| Jul 10 19:03:20 | heber-postgres PANIC on WAL write → exit 139. Docker restart fails: `mkdir /host_mnt/Volumes/heber: permission denied`. **Dead until Jul 19.** |
| Jul 10 19:04+ | heber-catalog begins 545 consecutive 500s (~46.8 h) while /health returns 200 (16,444×) |
| Jul 10 ~19:28–19:33 | Volume auto-remounts. Consumer self-recovers **without restart** at 19:30:19, flushing all buffered events (verified: zero event loss) |
| Jul 12 ~17:50 | **INCIDENT 2:** abrupt power loss mid-write. Last on-disk data: crypto events through 17:49:57 PDT (00:49:57 UTC Jul 13). No torn files at the boundary (gzip CRC + parquet footers verified) |
| Jul 13–17 | Machine off through 5 trading days. No ingestion, no Gold runs, no monitoring, all trading systems dark (markets: no positions were open; all systems paper/flat) |
| Jul 19 06:24 | Boot. `/Volumes/heber` absent; all bind-mount containers fail to start; `com.empire.mount-heber` (5-min auto-remount agent) fails to mount it for ~22 min |
| Jul 19 06:39 | data-gateway + data-gateway-redis up. **Redis empty** — persistence disabled (`--save '' --appendonly no`); all pre-outage stream/DLQ/consumer-group state gone |
| Jul 19 ~06:47 | Volume mounted (mount-heber agent and/or user). Docker `/host_mnt` resync lags ~33 more minutes |
| Jul 19 07:20 | docker-watchdog starts the full Heber stack. Postgres completes WAL crash recovery cleanly. Consumer recreates group at id `0`, drains the full boot backlog (0 failures, verified back to the first second of the fresh stream) |
| Jul 19 07:28 | Watchdog reconcile **destroys `cerberus_trader`** (was in a broken-create state) and recreates kairos/orion_heber_sync. Cerberus container no longer exists |

## Root causes

- **RC1 (Jul 10):** external exFAT USB volume transiently rejected writes / detached
  (~75 min host-side, ~30 min container-side). Underlying trigger (USB reset, exFAT
  driver fault, power blip) **unconfirmed** — macOS unified logs were not conclusive
  from container-side evidence alone. Host-side EPERM preceding container failures by
  45 minutes is unexplained; a host-scoped cause (e.g. TCC/FDA revocation) has not been
  excluded.
- **RC2 (Jul 12):** abrupt machine power-off (user-reported power outage; `last` shows
  no clean shutdown; all logs truncate mid-write). Kernel panic / forced power-off not
  strictly excluded, but power outage is consistent with all evidence.

## What turned faults into an 8.5-day outage (contributing factors)

1. **Postgres data dir on the removable exFAT volume** — no journaling, EPERM zombie-mount
   semantics, chronically slow fsync (15–36 s checkpoints are the *baseline*). A transient
   volume fault became a database PANIC; a missing mount became a failed restart forever.
2. **Docker restart cannot survive a missing bind-mount source** and nothing re-reconciles
   when the mount returns (the watchdog does now, but postgres stayed dead Jul 10→13
   while the machine was still up).
3. **Redis has persistence fully disabled** (`save ''`, `appendonly no`, allkeys-lru) —
   any restart destroys the event stream, DLQ contents, and consumer-group offsets.
   Mitigated this time only because consumer lag was ~0 at power-off.
4. **Health checks don't exercise real dependencies:** catalog /health never touches the
   DB (46.8 h of healthy-while-500ing); dataflow-health has no postgres/catalog check and
   skips freshness checks when `market_open=false` (574 consecutive "ok" reports while
   postgres was dead); consumer logged 30 min of continuous ERRORs without escalating.
5. **No mount-liveness signal anywhere.** The volume drop appears only as scattered
   per-service symptoms. Silent no-op patterns make it worse: compactor treats a broken
   mount as an empty lakehouse ("partitions_scanned: 0"); orion_heber_sync `[ -d ]` guards
   produce "synced silver=0 gold=0" with zero error lines.
6. **The mount-heber auto-remount agent failed silently** during both windows (no log
   retained; volume stayed unmounted 22 min post-boot despite a 5-min interval).
7. **The P0 eviction fix (96ad9e7) was committed but never deployed** — the
   heber-backfill-consumer container has never existed. (Separately, the adversarial
   review showed the eviction failures stopped ~1 h *before* the commit landed, so the
   fix has never actually been exercised.)

## What worked

- **Writer buffer retention + skip-ACK/redeliver + dedupe:** zero event loss across the
  30-min flush outage; buffered events (115 bars + 18 trades in the window) all landed.
- **tmp-then-rename atomic writes + gzip CRC:** zero torn files at either power-cut
  boundary (all boundary files verified; full-tree scan still to be finished).
- **DLQ durable fallback** (12 routings, all malformed-ticker events, all preserved on disk).
- **docker-watchdog** brought the whole stack back autonomously once the volume returned.
- **Postgres WAL crash recovery** completed cleanly on Jul 19 despite exFAT.

## Impact

- **Data gap (real, needs backfill):** all feeds **Jul 13 00:50 UTC → Jul 19 13:39 UTC** —
  5 trading days (equity/option/UW feeds) + 6.5 days of crypto. Gap exists at source;
  Redis held nothing.
- **Gold layer hole Jul 11–18:** poller `lookback_days=1` (verified in code) will *not*
  self-heal; manual backfill required. `market_regime_features` additionally stale since
  ≥ Jul 6 (its pipeline failed every run all week).
- **Catalog:** 46.8 h of 100% API failure; one coverage-seed COMMIT (Jul 10 19:03:20)
  definitively lost; coverage metadata re-derivable from disk.
- **Redis state loss:** stream/DLQ/offsets/gateway cache unrecoverable (mitigated: lag ~0
  at power-off; in-flight loss bounded to ≲30 s of crypto events by on-disk evidence).
- **Trading: no financial impact** — weekend power-off, all systems paper/flat, no open
  positions or in-flight orders in local records. *Caveat from review:* broker-side
  verification (Alpaca account state) was not performed; one `duplicate key on
  processed_fills` insert appeared in orion_timescaledb at boot and is unexplained.
- **Forensics:** cerberus_trader docker logs (Jul 3–10) destroyed with the container;
  kairos pre-outage docker logs lost to recreation; docker json logs corrupt at the
  power-loss boundary on all then-running containers (forward reads/`--since` silently
  return nothing — use `--tail`).

## Adversarial review corrections (methodology notes)

The 3-lens review **refuted** or corrected the following before publication:

- ~~"Permanent ~30-min crypto gap 02:00–02:30 UTC Jul 11"~~ — **false.** File-write
  timestamps were mistaken for event-time coverage; the recovery flush files contain the
  complete window (verified row-level, Bronze=Silver=115 rows, all 30 minutes, 7 symbols).
- ~~"Commit 96ad9e7 ended the eviction problem"~~ — temporally impossible (last failure
  preceded the commit); and the fix was never deployed at all.
- "Machine power loss" was asserted with more confidence than container evidence
  supports; now grounded in `last` (no clean shutdown) + user report.
- Post-recovery postgres "36.5 s checkpoint" is not new degradation — it is the chronic
  exFAT baseline (36.6 s checkpoints observed Jun 27, pre-incident).
- OOM attributions for health-monitor/compactor/gold-poller kills are **suspected, not
  proven** (`Memory=0` means no cgroup OOMKilled flag; the poller's own 1800 s timeout
  handler may be the SIGKILL source).
- Scope misses now recorded as open items: Heber-massive native services, Orion host
  services, 3Roses/binance launchd jobs, homebrew Postgres on :5432, EmpireUI,
  broker-side verification, LAN-exposed Redis on 0.0.0.0:6379.

## Chronic issues discovered (pre-existing, not outage-caused)

| Sev | Issue |
|---|---|
| P1 | Live-stream MAXLEN eviction during UW bulk backfill destroyed un-consumed live events every trading day Jul 5–10; the fix (96ad9e7) exists but has never run |
| P1 | heber-health-monitor: 32 silent deaths in 5 days; compactor: 6; gold-poller `market_regime`: failed every EOD run (timeout/SIGKILL, cause unproven) |
| P1 | Misdated Bronze partition `dt=2023-06-23/hour=20`: 804 files dating back to ≥ Apr 29, one poisoned alpaca crypto record re-emitted ~every 2 h — still happening post-recovery |
| P2 | `iv_term_structure` Silver: `call_iv`/`put_iv` 100% null on every write (field-mapping gap; bulk of 18k null warnings) |
| P2 | `meta_label_features` Gold quarantined every market day (all Greek columns null; upstream uw_market_tide 502s / alpaca options-chain timeouts) |
| P2 | heber-postgres: missing relation file `base/16384/6104` since Jun 27 breaks `data_coverage` updates; daily 12:58 UTC `FATAL: database "heber" does not exist` from a misconfigured job |
| P2 | Redis: no persistence + allkeys-lru on the only copy of in-flight lakehouse data; published on 0.0.0.0:6379 (LAN-reachable, unauthenticated); Heber docs still say localhost:6380 |
| P3 | UW darkpool tickers with trailing `=` (AAC=, SAMO=, VII=) fail instrument-key validation → DLQ (needs normalization decision) |
| P3 | Kairos pytest runs write simulated errors into production logs/audit dirs (materially confused this investigation) |
| P3 | Cerberus `ledger.db` (18.5 GB bind-mounted) is not a valid SQLite file (bad header, untouched since Apr 20) |

## Open verification items

1. Trigger for the Jul 10 volume EPERM (macOS unified logs around 18:15 PDT; check
   Heber-massive host writer logs for the same window).
2. Postgres integrity pass (amcheck/pg_dump test) + repair/REINDEX of `base/16384/6104`.
3. Broker-side flat-ness verification (Alpaca orders/positions over the window) + explain
   the `processed_fills` duplicate-key insert at boot.
4. Finish the full-tree `.tmp`/zero-byte scan (zero hits so far, walk incomplete).
5. Why mount-heber failed to mount for 22 min post-boot (its log is empty).
6. Confirm/deny OOM as the health-monitor & compactor killer (add memory limits + observe).

## Remediation (feeds the recovery plan)

**P0 — before Monday 09:30 ET open (2026-07-20):**
- Recreate `cerberus_trader` (container destroyed; `docker compose up` from Cerberus repo).
- Deploy `heber-backfill-consumer` (committed Jul 10, never created) via `scripts/deploy.sh`.
- Load `com.empire.kairos.live-exit-monitor` (plist exists, not loaded — will not fire Monday).
- Kick the exit-78 launchd jobs now that the volume is back: 3× Heber-massive, chronos, binancews (hippocrates is dead-project noise).
- Restart/verify EmpireUI (nothing listening on 5173/8004).

**P1 — durability (this week):**
- Enable Redis AOF (`appendonly yes`) and drop allkeys-lru for the stream instance; bind to 127.0.0.1.
- Catalog /health must exercise the DB; dataflow-health needs postgres/catalog + mount-liveness checks that run in market-closed mode.
- Mount-liveness canary (sentinel file check) + loud failure in compactor/orion_heber_sync on empty-mount.
- Postgres: run integrity pass; decide migration off exFAT (internal APFS or a docker volume inside the VM disk image).
- Backfill: UW/equity/option feeds Jul 13–17 via `heber:events:backfill`; Gold pipelines Jul 11–18 (`lookback` override); crypto Jul 13–19 from Alpaca.
- mount-heber agent: add logging + alerting on repeated mount failure.

**P2 — hygiene:**
- Trace + purge the `dt=2023-06-23` poisoned record; quarantine misdated partitions (guard: reject events with ts_event outside sane bounds at the writer).
- Fix `iv_term_structure` call_iv/put_iv mapping; UW `=`‑suffix ticker normalization decision.
- Memory limits for health-monitor/compactor/gold-poller (turns silent kills into observable OOMKilled).
- Isolate Kairos test fixtures from production logs; fix the `database "heber"` misconfigured job; update stale Redis docs (6380 → data-gateway-redis:6379).
