# Finding — the Docker VM is killing Redis by running out of memory

Established 2026-07-30 from the Docker VM's own kernel log. This closes the
question the capacity instrumentation was built to answer, on its first run.

## What the kernel says

```
Out of memory: Killed process 65813 (redis-server) total-vm:1494060kB, anon-rss:1413504kB
oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=...,mems_allowed=0,global_oom,
         task_memcg=/docker/55c9ffc4...,task=redis-server,pid=66446,uid=999
```

In the retained ring buffer:

| Victim | Kills |
|---|---|
| `redis-server` | 7 |
| `python` (Heber containers, uid 10000) | 3 |

| Constraint | Count | Meaning |
|---|---|---|
| `CONSTRAINT_NONE` + `global_oom` | 9 | **the whole VM ran out of memory** |
| `CONSTRAINT_MEMCG` | 1 | a single container hit its own `mem_limit` |

Nine of ten kills are VM-wide, not container-limit. Redis is simply the largest
resident process, so the kernel picks it.

## Why the earlier evidence was misleading

`docker inspect data-gateway-redis` reports:

```
OOMKilled=false   ExitCode=0   RestartCount=297
```

`OOMKilled` is only set for **cgroup** kills — a container exceeding its own
`mem_limit`. A global VM OOM is invisible to it. That is why the container
metadata said "not an OOM" while the kernel was killing it for exactly that
reason, and why the Redis log contains no shutdown lines: SIGKILL leaves no
chance to log.

## Capacity

| | |
|---|---|
| Host RAM | 48 GB |
| Docker VM | **8 GB** |
| Container `mem_limit` total | 13.5 GB |
| Redis `maxmemory` | 4 GB |
| Measured idle usage | ~6.3 GB of 7.5 GB usable (~84%) |

The Data-Gateway compose file already carries the warning, written when the cap
was last raised:

> `PREREQ: confirm the Docker VM has the RAM to back 4GB before deploying.`

That prerequisite was never satisfied. The VM has been oversubscribed since.

## Cost per restart is worse than previously recorded

A restart on 2026-07-30 05:32 took **165 seconds to load the RDB base alone**
(363,127 keys), and about **5.5 minutes** before the container reported healthy.
An earlier measurement in the same investigation showed 39 seconds at 305,665
keys — the reload cost is growing with the dataset. Every consumer sees
`BusyLoadingError` for that entire window.

## What this explains

- The ~1,600 transient Redis errors across the consumers
- The 78 `Check/write error` tracebacks in `heber-watch`
- The gold pipelines dying with `exitcode=-9` — the same kernel, same cause
- Why `heber:events` entries age out of the 91-minute retention window while
  consumers are stalled

## Recommended action

Raise the Docker Desktop VM from 8 GB to 20 GB. The host has 48 GB, so this is
headroom that already exists. It requires a Docker Desktop restart, which
restarts every container.

The instrumentation stays in place afterwards to confirm the kills actually stop
rather than assuming they did — `heber_consumer` and `redis-events.log` will show
it either way.

Note what this does **not** fix: the AOF reload cost grows with the dataset, so a
restart from any cause is now a multi-minute outage. That is worth attention
separately from the memory question.

## Collectors

- `scripts/redis_event_collector.sh` — long-running `docker events` attach,
  records die/kill/start with exit code and signal. A deliberate restart shows
  `signal=15`; an OOM kill does not.
- `scripts/redis_capacity_watch.sh` — 60s samples of per-container memory, Redis
  persistence and fork stats, and a **self-verifying** kernel OOM probe. The
  probe reports whether it actually ran, because an absence of OOM lines from a
  broken probe would send the next investigation down the wrong path.
