"""Heber configuration using Pydantic Settings."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal, NamedTuple
from urllib.parse import unquote, urlsplit

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Fallback Postgres password used only when HEBER_POSTGRES_PASSWORD is unset.
# Safe for local dev; a model validator rejects it outside the dev environment.
DEV_POSTGRES_PASSWORD = "heber_dev_password"  # pragma: allowlist secret — dev-only fallback, rejected outside dev


def _postgres_url_uses_dev_password(postgres_url: str) -> bool:
    """Return True when the URL password exactly matches the dev fallback."""
    try:
        password = urlsplit(postgres_url).password
    except ValueError:
        return False
    if password is None:
        return False
    return unquote(password) == DEV_POSTGRES_PASSWORD


# ---------------------------------------------------------------------------
# Typed section accessors (NamedTuples)
#
# These provide a grouped, dot-accessible view over flat Settings fields.
# They are returned by @property methods on Settings and do NOT replace the
# flat fields — all existing ``settings.field_name`` access still works.
# ---------------------------------------------------------------------------


class StorageConfig(NamedTuple):
    """Grouped view of storage / path settings."""

    data_root: Path
    volume_root: Path
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    quarantine_path: str


class RedisConfig(NamedTuple):
    """Grouped view of Redis event-bus settings."""

    url: str
    stream_name: str
    consumer_group: str
    dlq_stream_name: str
    claim_idle_ms: int
    claim_batch_size: int
    process_max_retries: int
    retry_backoff_seconds: float
    read_batch_size: int
    read_block_ms: int
    process_concurrency: int


class PostgresConfig(NamedTuple):
    """Grouped view of Postgres catalog settings."""

    url: str


class WriterConfig(NamedTuple):
    """Grouped view of Bronze/Silver writer tuning settings."""

    bronze_flush_interval_seconds: int
    bronze_max_batch_size: int
    silver_target_file_size_mb: int
    silver_max_rows_per_file: int
    silver_max_flush_time_seconds: int
    silver_row_group_size_mb: int


class GoldPollerConfig(NamedTuple):
    """Grouped view of Gold feature poller settings."""

    enabled: bool
    eod_hour: int
    eod_minute: int
    check_interval_seconds: int
    retry_max: int
    retry_backoff_seconds: float
    project: str
    version: str
    lookback_days: int
    disabled_pipelines: str
    disabled_pipeline_set: set[str]


class WatchConfig(NamedTuple):
    """Grouped view of watch consumer settings."""

    redis_url: str
    gateway_url: str
    gateway_api_key: str | None
    gateway_legacy_fallback_enabled: bool
    enrichment_timeout_seconds: float
    enrichment_option_chain_timeout_seconds: float
    enrichment_backfill_enabled: bool
    enrichment_backfill_interval: int
    enrichment_backfill_lookback_days: int
    enrichment_backfill_batch_size: int


class HealthMonitorConfig(NamedTuple):
    """Grouped view of health monitor settings."""

    enabled: bool
    stream_check_interval_seconds: int
    partition_check_interval_seconds: int
    volume_baseline_days: int
    stats_baseline_days: int
    volume_warn_ratio: float
    volume_critical_ratio: float
    null_rate_threshold: float
    psi_threshold: float
    leakage_sample_size: int


class DataflowHealthConfig(NamedTuple):
    """Grouped view of dataflow health verification settings."""

    consumer_metrics_url: str
    watch_metrics_url: str
    freshness_seconds: int
    report_dir: Path
    interval_seconds: int


class CatalogConfig(NamedTuple):
    """Grouped view of catalog settings."""

    url: str
    auto_discover: bool
    discover_interval_seconds: int


class LLMConfig(NamedTuple):
    """Grouped view of LLM provider settings."""

    provider: str
    model: str
    base_url: str | None
    api_key: str
    qwen_region: str
    effective_base_url: str | None


class Settings(BaseSettings):
    """Heber application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_prefix="HEBER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Storage
    data_root: Path = Field(
        default=Path("/Volumes/heber/data"),
        description="Root path for Bronze/Silver/Gold data",
    )
    volume_root: Path = Field(
        default=Path("/Volumes/heber"),
        description="Root path for external volume",
    )

    # Postgres (Catalog)
    postgres_url: str = Field(
        default_factory=lambda: (
            f"postgresql+asyncpg://heber:{os.environ.get('HEBER_POSTGRES_PASSWORD', DEV_POSTGRES_PASSWORD)}"
            f"@localhost:5433/heber_catalog"
        ),
        description="PostgreSQL connection URL for Catalog DB",
    )

    # Redis (Event Bus) — the actual instance is data-gateway-redis on 6379;
    # the old 6380 default pointed at a Redis that no longer exists anywhere.
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL for event streams (data-gateway-redis)",
    )
    redis_stream_name: str = Field(
        default="heber:events",
        description="Redis stream name for incoming events",
    )
    redis_consumer_group: str = Field(
        default="heber-writers",
        description="Redis consumer group name",
    )
    redis_dlq_stream_name: str = Field(
        default="heber:events:dlq",
        description="Redis stream for failed consumer messages",
    )
    redis_dlq_max_stream_len: int = Field(
        default=100_000,
        ge=1000,
        description="Approximate MAXLEN cap applied to the DLQ stream on XADD. The "
        "DLQ lives on a cache-mode Redis; this bounds the best-effort reprocessing "
        "queue so it cannot grow unbounded. Durable on-disk files remain the audit record.",
    )
    dlq_fallback_dir: Path | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_DLQ_FALLBACK_DIR"),
        description="Directory for durable DLQ file fallback when Redis xadd fails; "
        "defaults to <data_root>/dlq_fallback",
    )
    redis_claim_idle_ms: int = Field(
        default=60_000,
        description="Minimum idle time before claiming pending stream messages",
    )
    redis_claim_batch_size: int = Field(
        default=100,
        description="Max pending messages to claim per recovery batch (a recovery cycle drains in batches until empty)",
    )
    redis_recover_interval_seconds: int = Field(
        default=300,
        ge=0,
        description=(
            "How often the running consumer re-runs pending-message recovery to "
            "reclaim messages stranded by a dead consumer; 0 disables periodic recovery "
            "(startup recovery still runs)"
        ),
    )
    redis_process_max_retries: int = Field(
        default=3,
        description="Retry attempts for a stream message before DLQ",
    )
    redis_retry_backoff_seconds: float = Field(
        default=0.25,
        description="Base backoff delay between processing retries",
    )
    redis_read_batch_size: int = Field(
        default=500,
        ge=10,
        le=5000,
        description="Max messages per XREADGROUP call (higher = more throughput during backfill)",
    )
    redis_read_block_ms: int = Field(
        default=2000,
        ge=100,
        le=10000,
        description="XREADGROUP block timeout in ms (longer allows larger batches to fill)",
    )
    redis_process_concurrency: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Max messages processed concurrently within each XREADGROUP batch",
    )

    # Writer transport. Redis remains the default until the JetStream consumer
    # has passed the production replay and fault-injection gates.
    ingest_transport: Literal["redis", "jetstream"] = Field(default="redis")
    ingest_lane: Literal["live", "backfill"] = Field(default="live")
    nats_url: str = Field(default="nats://localhost:4222")
    nats_username: str | None = Field(default=None)
    nats_password: SecretStr | None = Field(default=None)
    jetstream_live_stream_name: str = Field(default="HEBER_LIVE", min_length=1)
    jetstream_backfill_stream_name: str = Field(default="HEBER_BACKFILL", min_length=1)
    jetstream_live_durable_name: str = Field(default="heber-live-writers", min_length=1)
    jetstream_backfill_durable_name: str = Field(default="heber-backfill-writers", min_length=1)
    jetstream_ack_wait_seconds: int = Field(default=300, ge=1)
    jetstream_max_ack_pending: int = Field(
        default=0,
        ge=0,
        description="Maximum unacknowledged JetStream messages; 0 derives twice the Redis read batch size",
    )
    jetstream_reconnect_backoff_seconds: float = Field(default=1.0, ge=0.1, le=60)
    backfill_proof_max_expected_records: int = Field(
        default=5000,
        ge=1,
        description="Largest accepted backfill chunk proof; must fit inside JetStream max_ack_pending",
    )
    backfill_proof_ttl_seconds: int = Field(
        default=86400,
        ge=60,
        description="Expiry for incomplete Redis proof accumulators; final acknowledgements do not expire",
    )
    backfill_readiness_check_timeout_seconds: float = Field(default=5.0, ge=0.1, le=60)

    dedupe_redis_enabled: bool = Field(
        default=False,
        description="Verify Bloom-filter dedupe hits against an exact Redis store "
        "(eliminates false-positive drops, but issues a synchronous Redis SET per "
        "event on register). Kept OFF: benchmarked against the live Data-Gateway "
        "event bus (redis://localhost:6379, ~4.5k ops/s real traffic, "
        "scripts/debug/bench_dedupe_store.py), synchronous register added "
        "~2.1-5.6 ms/event (170-680% overhead at 1,215 ev/s avg, far over the 5% "
        "gate). A pipelined batch-100 register was ~10-12 us/event when the bus was "
        "idle but degraded to ~375-400 us/event under contention — 5.2% to 188% at "
        "the 5,000 ev/s peak — so even batched registration does not reliably clear "
        "the <10 us/event (5% at peak) gate. False-positive drops are instead "
        "absorbed by Bloom saturation fail-open plus exact dedupe in the compactor",
    )
    dedupe_redis_ttl_seconds: int = Field(
        default=7200,
        ge=60,
        description="TTL for exact dedupe keys in Redis (should cover the Bloom rotation window)",
    )

    # API
    api_port: int = Field(default=8080)
    catalog_url: str = Field(
        default="http://localhost:8085/api/v1",
        description="Catalog API base URL used by SDK clients",
    )
    feast_repo_path: Path = Field(
        default=Path("features"),
        validation_alias=AliasChoices("HEBER_FEAST_REPO_PATH", "FEAST_REPO_PATH"),
        description="Path to Feast feature repository",
    )

    # LLM (OpenAI-compatible providers)
    llm_provider: Literal["openai", "qwen"] = Field(
        default="openai",
        validation_alias=AliasChoices("HEBER_LLM_PROVIDER", "LLM_PROVIDER"),
        description="LLM provider selector for OpenAI-compatible clients",
    )
    llm_model: str = Field(
        default="glm-5-turbo",
        validation_alias=AliasChoices("HEBER_LLM_MODEL", "LLM_MODEL"),
        description="Default chat model name",
    )
    llm_base_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_LLM_BASE_URL", "LLM_BASE_URL"),
        description="Optional override for OpenAI-compatible base URL",
    )
    llm_api_key: str = Field(
        default="",
        validation_alias=AliasChoices(
            "HEBER_LLM_API_KEY",
            "LLM_API_KEY",
            "OPENAI_API_KEY",
            "DASHSCOPE_API_KEY",
        ),
        description="API key for OpenAI-compatible model providers",
    )
    llm_qwen_region: Literal["intl", "us", "cn"] = Field(
        default="intl",
        validation_alias=AliasChoices("HEBER_LLM_QWEN_REGION", "LLM_QWEN_REGION"),
        description="Default Qwen endpoint region (intl/us/cn) when provider=qwen",
    )

    # Writer settings (PRD §7.5 - File sizing, batching, compaction)
    bronze_flush_interval_seconds: int = Field(default=30, description="Max time before flushing Bronze")
    bronze_max_batch_size: int = Field(default=10000, description="Max events per Bronze file")
    writer_flush_max_workers: int = Field(
        default=8,
        ge=1,
        le=64,
        description=(
            "Max concurrent partition writes per flush cycle. A backfill batch scatters records "
            "across hundreds of date partitions; writing them serially to the slow macOS bind mount "
            "starves the consumer and lets the capped stream evict live feeds. Set to 1 for the old "
            "serial behavior."
        ),
    )
    writer_max_unacked_seconds: float = Field(
        default=30.0,
        ge=1.0,
        le=45.0,
        description=(
            "How long processed messages may stay unacknowledged, waiting for a flush to make them "
            "durable, before a full flush is forced. Matched to the Bronze/Silver flush intervals so "
            "the backstop normally coincides with the natural flush and file counts are unchanged. "
            "Must stay well below redis_claim_idle_ms or the consumer's own recovery drain will "
            "reclaim messages it is still holding."
        ),
    )
    writer_max_unacked_messages: int = Field(
        default=50_000,
        ge=1_000,
        le=1_000_000,
        description=(
            "How many processed messages may stay unacknowledged before a full flush is forced. "
            "Bounds how far the Redis pending list can grow toward the stream's retention window, "
            "past which un-acknowledged entries are trimmed and can never be re-read. Lowering this "
            "is always safe — it only costs more, smaller files."
        ),
    )

    # Silver file sizing targets (PRD §7.5)
    silver_target_file_size_mb: int = Field(default=256, description="Target Parquet file size (128-512 MB)")
    silver_max_rows_per_file: int = Field(default=1_000_000, description="Max rows per file (250k-2M)")
    silver_max_flush_time_seconds: int = Field(default=30, description="Max seconds before flush (5-30s)")
    silver_min_rows_per_flush: int = Field(
        default=50,
        ge=1,
        le=10000,
        description="Min rows before flushing a Silver partition (prevents tiny files during backfill)",
    )
    silver_row_group_size_mb: int = Field(default=128, description="Parquet row group size (64-256 MB)")

    # Environment
    environment: Literal["dev", "staging", "prod"] = Field(default="dev")

    # Catalog auto-discovery
    catalog_auto_discover: bool = Field(
        default=True,
        description="Scan Silver directory on startup and auto-register unknown datasets",
    )
    catalog_discover_interval_seconds: int = Field(
        default=300,
        description="Seconds between periodic Silver directory discovery scans (0 to disable periodic scan)",
    )

    # Gold layer paths (used by feature_views/_paths.py)
    gold_root: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "HEBER_GOLD_ROOT",
            "HEBER_GOLD_PATH",
            "GOLD_ROOT",
            "GOLD_PATH",
        ),
        description="Override for gold data root (defaults to data_root/gold)",
    )
    gold_project: str = Field(default="*", description="Glob pattern for gold project dirs")
    gold_version: str = Field(default="*", description="Glob pattern for gold version dirs")

    # Soda quality checks
    soda_checks_dir: Path = Field(
        default=Path("soda/checks"),
        validation_alias=AliasChoices("HEBER_SODA_CHECKS_DIR", "SODA_CHECKS_DIR"),
    )

    # Backfill service
    backfill_host: str = Field(
        default="127.0.0.1",
        description="Bind address for the backfill HTTP service. Loopback by "
        "default — the catalog/backfill APIs have no authentication, so they "
        "must not listen on LAN interfaces. Containers override to 0.0.0.0 "
        "(exposure is controlled by host-side port publishing).",
    )
    backfill_port: int = Field(default=8080)
    backfill_log_level: str = Field(default="info")

    # Watch consumer
    watch_redis_url: str = Field(
        default="redis://localhost:6379",
        validation_alias=AliasChoices("HEBER_WATCH_REDIS_URL", "HEBER_REDIS_URL"),
    )
    watch_gateway_url: str = Field(
        default="http://localhost:8080",
        validation_alias=AliasChoices("HEBER_WATCH_GATEWAY_URL", "DATA_GATEWAY_URL"),
    )
    watch_gateway_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "HEBER_WATCH_GATEWAY_API_KEY",
            "DATA_GATEWAY_API_KEY",
            "GATEWAY_API_KEY",
        ),
    )
    watch_gateway_legacy_fallback_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "HEBER_WATCH_GATEWAY_LEGACY_FALLBACK_ENABLED",
            "WATCH_GATEWAY_LEGACY_FALLBACK_ENABLED",
        ),
        description="Enable legacy unprefixed gateway route fallback after /api/v1 routes",
    )

    # Enrichment HTTP timeouts
    watch_enrichment_timeout_seconds: float = Field(
        default=10.0,
        ge=1.0,
        le=120.0,
        description="Default HTTP timeout for enrichment requests (seconds)",
    )
    watch_enrichment_option_chain_timeout_seconds: float = Field(
        default=30.0,
        ge=5.0,
        le=120.0,
        description="HTTP timeout for option chain enrichment requests (seconds); "
        "higher than the default because large chains (QQQ, SPY) can take 6-7s",
    )

    # Enrichment backfill scanner
    enrichment_backfill_enabled: bool = Field(
        default=True,
        description="Enable periodic re-enrichment of Gold feature rows with null fields",
    )
    enrichment_backfill_interval: int = Field(
        default=3600,
        description="Seconds between enrichment backfill scans",
    )
    enrichment_backfill_lookback_days: int = Field(
        default=3,
        description="Number of days back to scan for incomplete feature rows",
    )
    enrichment_backfill_batch_size: int = Field(
        default=50,
        description="Max rows to re-enrich per scan cycle",
    )

    # Gold Feature Poller
    gold_poller_enabled: bool = Field(
        default=True,
        description="Enable the Gold feature poller service",
    )
    gold_poller_eod_hour: int = Field(
        default=16,
        description="Hour (ET) to trigger EOD Gold feature refresh (0-23)",
    )
    gold_poller_eod_minute: int = Field(
        default=35,
        description="Minute past the hour to trigger EOD Gold feature refresh",
    )
    gold_poller_check_interval_seconds: int = Field(
        default=60,
        ge=10,
        le=600,
        description="How often the poller checks if a run is due (seconds)",
    )
    gold_poller_retry_max: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Max retries per pipeline on failure",
    )
    gold_poller_retry_backoff_seconds: float = Field(
        default=30.0,
        description="Base backoff between retries (multiplied by attempt number)",
    )
    gold_poller_pipeline_timeout_seconds: int = Field(
        default=1800,
        ge=60,
        le=7200,
        description="Hard wall-clock cap per pipeline; the isolated subprocess is killed past this",
    )
    gold_poller_project: str = Field(
        default="watch",
        description="Gold project namespace for poller-generated datasets",
    )
    gold_poller_version: str = Field(
        default="v1",
        description="Gold version namespace for poller-generated datasets",
    )
    gold_poller_lookback_days: int = Field(
        default=1,
        ge=1,
        le=30,
        description="Number of days back to recompute on each run (ensures gap-fill)",
    )
    gold_poller_disabled_pipelines: str = Field(
        default="",
        description="Comma-separated pipeline names to skip (e.g. 'trend_scan,ticker_base_rates')",
    )

    @property
    def gold_poller_disabled_pipeline_set(self) -> set[str]:
        """Parse disabled pipelines into a set."""
        if not self.gold_poller_disabled_pipelines:
            return set()
        return {p.strip() for p in self.gold_poller_disabled_pipelines.split(",") if p.strip()}

    # Post-EOD self-heal: re-pull daily UW feeds whose Silver partition never landed
    # (e.g. evicted from the capped stream while the consumer was down) via the
    # Data-Gateway backfill API. On by default (set HEBER_EOD_RECONCILE_ENABLED=false to opt out).
    eod_reconcile_enabled: bool = Field(
        default=True,
        description="Enable the post-EOD self-heal reconcile (re-pull missing daily UW feeds via gateway backfill)",
    )
    eod_reconcile_hour: int = Field(
        default=17,
        ge=0,
        le=23,
        description="ET hour to run the EOD reconcile — after the feeds' EOD deadline so in-flight data is not flagged",
    )
    eod_reconcile_minute: int = Field(
        default=45,
        ge=0,
        le=59,
        description="ET minute to run the EOD reconcile",
    )
    eod_reconcile_feeds: str = Field(
        default="oi_change,historic_option_volume",
        description=(
            "Comma-separated daily UW feeds to self-heal if today's Silver is missing. "
            "iv_rank and iv_term_structure are excluded — their UW provider methods are "
            "snapshot-only: a re-pull returns the current value stamped now, landing under "
            "today's partition instead of reconstructing the missing date, so back-dated "
            "recovery of those feeds is futile."
        ),
    )
    eod_reconcile_symbols: str = Field(
        default=(
            "SPY,QQQ,IWM,DIA,AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AMD,NFLX,JPM,GS,BAC,"
            "XLF,XLE,XLK,XLV,XLI,XLP,XLU,XLB,XLRE,XLC,GLD,TLT,HYG"
        ),
        description=(
            "Comma-separated symbols for the gateway backfill (it requires an explicit list); "
            "defaults to the core ticker set"
        ),
    )

    @property
    def eod_reconcile_feed_list(self) -> list[str]:
        return [f.strip() for f in self.eod_reconcile_feeds.split(",") if f.strip()]

    @property
    def eod_reconcile_symbol_list(self) -> list[str]:
        return [s.strip() for s in self.eod_reconcile_symbols.split(",") if s.strip()]

    # Health Monitor
    health_monitor_enabled: bool = Field(
        default=True,
        description="Enable the data health monitor service",
    )
    health_stream_check_interval_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Tier 1 stream health check interval (seconds)",
    )
    health_partition_check_interval_seconds: int = Field(
        default=900,
        ge=60,
        le=3600,
        description="Tier 2 partition completeness check interval (seconds)",
    )
    health_volume_baseline_days: int = Field(
        default=5,
        ge=1,
        le=30,
        description="Days of history for volume baseline comparison",
    )
    health_stats_baseline_days: int = Field(
        default=30,
        ge=7,
        le=90,
        description="Days of history for statistical baseline comparison",
    )
    health_volume_warn_ratio: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Volume ratio threshold for warning (e.g., 0.5 = 50% of baseline)",
    )
    health_volume_critical_ratio: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="Volume ratio threshold for critical alert",
    )
    health_null_rate_threshold: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Null rate threshold for ML feature alerts (e.g., 0.05 = 5%)",
    )
    health_psi_threshold: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="Population Stability Index threshold for label drift detection",
    )
    health_leakage_sample_size: int = Field(
        default=0,
        ge=0,
        description="Rows to sample for zero-leakage audit (0 = full scan)",
    )

    # Critical-feed data-quality alerting (HEBER_ALERT_*)
    alert_discord_enabled: bool = Field(
        default=False,
        description="Enable Discord alerts for critical data-quality failures",
    )
    alert_discord_webhook_url: str = Field(
        default="",
        description="Discord webhook URL for critical data-quality alerts",
    )
    alert_min_severity: str = Field(
        default="critical",
        description="Minimum severity to send an alert (critical|warning|info)",
    )
    alert_cooldown_seconds: int = Field(
        default=3600,
        ge=0,
        description="Minimum seconds between repeat alerts for the same (check, feed)",
    )
    alert_send_recovery: bool = Field(
        default=True,
        description="Send a one-line recovery note when a previously-alerting feed returns to healthy",
    )
    alert_debounce_cycles: int = Field(
        default=2,
        ge=1,
        description=(
            "Consecutive failing liveness cycles required before the first alert. "
            "Suppresses single-cycle flaps (transient bind-mount read errors, "
            "deadline-edge blips); 1 = alert on the first failure"
        ),
    )
    alert_liveness_check_interval_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Interval (seconds) for the per-feed liveness loop",
    )
    alert_floor_overrides: dict[str, int] = Field(
        default_factory=dict,
        description="Per-feed floor overrides for liveness (JSON env); floor 0 disables that feed",
    )

    # Quarantine
    quarantine_path: str = Field(default="quarantine")

    # Metrics
    metrics_port: int | None = Field(default=None, description="Prometheus metrics port")

    # Daily health report
    daily_health_report_dir: Path = Field(
        default=Path("/Volumes/heber/data/ops/daily-health"),
        description="Directory for daily health JSON reports",
    )
    daily_health_expected_symbol_count: int = Field(
        default=500,
        description="Expected minimum distinct symbols in bars partition",
    )
    daily_health_expected_feeds: list[str] = Field(
        default=["bars", "quotes", "trades", "flow_alerts"],
        description="Feeds expected to have partitions each trading day",
    )

    # Dataflow health verification
    health_consumer_metrics_url: str = Field(
        default="http://localhost:9090/metrics",
        description="Metrics endpoint for heber-consumer dataflow health checks",
    )
    health_watch_metrics_url: str = Field(
        default="http://localhost:9091/metrics",
        description="Metrics endpoint for heber-watch dataflow health checks",
    )
    health_catalog_url: str = Field(
        default="http://localhost:8085/health",
        description="Catalog /health URL for dataflow health checks (DB-exercising)",
    )
    heartbeat_url: str = Field(
        default="",
        description=(
            "Off-machine dead-man heartbeat URL (e.g. healthchecks.io check). "
            "Pinged after each dataflow-health cycle; /fail appended on overall "
            "failure. Empty disables. The only monitoring that survives machine death."
        ),
    )
    backup_freshness_hours: int = Field(
        default=30,
        description="Max age (hours) of the lakehouse backup marker before backup_freshness fails",
    )
    health_freshness_seconds: int = Field(
        default=900,
        description="Maximum allowed freshness window (seconds) for dataflow checks",
    )
    health_report_dir: Path = Field(
        default=Path("/data/ops/dataflow-health"),
        description="Directory for persisted dataflow health JSON reports",
    )
    health_interval_seconds: int = Field(
        default=300,
        description="Scheduled interval (seconds) for recurring dataflow checks",
    )

    # Ops
    service_name: str = Field(
        default="heber",
        validation_alias=AliasChoices("HEBER_SERVICE_NAME", "SERVICE_NAME"),
    )
    instance_id: str = Field(
        default="unknown",
        validation_alias=AliasChoices("HEBER_INSTANCE_ID", "INSTANCE_ID", "HOSTNAME"),
    )
    service_version: str = Field(
        default="0.1.0",
        validation_alias=AliasChoices("HEBER_SERVICE_VERSION", "SERVICE_VERSION"),
    )
    log_level: str = Field(
        default="INFO",
        validation_alias=AliasChoices("HEBER_LOG_LEVEL", "LOG_LEVEL"),
        description="Global logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    shutdown_timeout_seconds: int = Field(default=30)

    # OpenTelemetry
    otel_endpoint: str = Field(
        default="http://localhost:4317",
        validation_alias=AliasChoices("HEBER_OTEL_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"),
    )

    # OpenMetadata catalog
    openmetadata_host: str = Field(
        default="http://localhost:8585",
        validation_alias=AliasChoices("HEBER_OPENMETADATA_HOST", "OPENMETADATA_HOST"),
    )
    openmetadata_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("HEBER_OPENMETADATA_API_KEY", "OPENMETADATA_API_KEY"),
    )

    @model_validator(mode="after")
    def _reject_default_postgres_password_outside_dev(self) -> "Settings":
        """Fail fast if a non-dev environment still uses the dev Postgres password.

        The ``postgres_url`` default falls back to ``DEV_POSTGRES_PASSWORD`` when
        ``HEBER_POSTGRES_PASSWORD`` is unset. That fallback is convenient for local
        development but must never reach staging/prod, so we refuse to start.
        """
        if self.environment != "dev" and _postgres_url_uses_dev_password(self.postgres_url):
            raise ValueError(
                f"Refusing to start in environment='{self.environment}' with the default "
                f"development Postgres password. Set HEBER_POSTGRES_PASSWORD (or a full "
                f"HEBER_POSTGRES_URL) to real credentials for non-dev environments."
            )
        return self

    @model_validator(mode="after")
    def _validate_ingest_transport(self) -> "Settings":
        """Derive safe consumer limits and reject incomplete JetStream credentials."""
        if self.jetstream_max_ack_pending == 0:
            self.jetstream_max_ack_pending = self.redis_read_batch_size * 2
            if self.ingest_lane == "backfill":
                self.jetstream_max_ack_pending = max(
                    self.jetstream_max_ack_pending,
                    self.backfill_proof_max_expected_records,
                )

        if self.ingest_transport != "jetstream":
            return self

        credentials_present = (
            self.nats_username is not None
            and bool(self.nats_username.strip())
            and self.nats_password is not None
            and bool(self.nats_password.get_secret_value())
        )
        if not credentials_present:
            raise ValueError("JetStream transport requires HEBER_NATS_USERNAME and HEBER_NATS_PASSWORD")
        if not self.nats_url.startswith(("nats://", "tls://")):
            raise ValueError("HEBER_NATS_URL must use nats:// or tls://")
        if self.ingest_lane == "backfill" and self.jetstream_max_ack_pending < self.backfill_proof_max_expected_records:
            raise ValueError(
                "jetstream_max_ack_pending must be at least "
                "HEBER_BACKFILL_PROOF_MAX_EXPECTED_RECORDS for the backfill lane"
            )
        return self

    @model_validator(mode="after")
    def _reject_unacked_hold_longer_than_claim_idle(self) -> "Settings":
        """Refuse a deferral window that lets recovery reclaim in-flight messages.

        Processed messages stay pending until a flush makes them durable. Once a
        pending message exceeds ``redis_claim_idle_ms`` it becomes claimable — by
        this consumer's own recovery drain, or by another consumer sharing the
        group — and gets processed a second time while the first copy is still
        buffered. The hold must therefore finish comfortably inside the claim
        window, with room for processing, the flush itself, and scheduler jitter.
        """
        claim_idle_seconds = self.redis_claim_idle_ms / 1000
        safe_ceiling = claim_idle_seconds * 0.75
        if self.writer_max_unacked_seconds > safe_ceiling:
            raise ValueError(
                f"writer_max_unacked_seconds={self.writer_max_unacked_seconds} exceeds 75% of "
                f"redis_claim_idle_ms ({claim_idle_seconds}s → {safe_ceiling}s). Messages held "
                f"that long become claimable while still buffered, causing duplicate writes. "
                f"Lower writer_max_unacked_seconds or raise redis_claim_idle_ms."
            )
        return self

    @property
    def bronze_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_path(self) -> Path:
        if self.gold_root:
            return self.gold_root
        return self.data_root / "gold"

    @property
    def dlq_fallback_path(self) -> Path:
        if self.dlq_fallback_dir:
            return self.dlq_fallback_dir
        return self.data_root / "dlq_fallback"

    @property
    def jetstream_stream_name(self) -> str:
        """Return the stream assigned to this writer lane."""
        if self.ingest_lane == "backfill":
            return self.jetstream_backfill_stream_name
        return self.jetstream_live_stream_name

    @property
    def jetstream_durable_name(self) -> str:
        """Return the durable consumer assigned to this writer lane."""
        if self.ingest_lane == "backfill":
            return self.jetstream_backfill_durable_name
        return self.jetstream_live_durable_name

    @property
    def llm_effective_base_url(self) -> str | None:
        """Resolve base URL for the configured OpenAI-compatible provider."""
        if self.llm_base_url:
            return self.llm_base_url

        if self.llm_provider == "qwen":
            qwen_endpoints = {
                "intl": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
                "us": "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
                "cn": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            }
            return qwen_endpoints[self.llm_qwen_region]

        return None

    # ------------------------------------------------------------------
    # Typed section accessors
    # ------------------------------------------------------------------

    @property
    def storage(self) -> StorageConfig:
        """Grouped storage / path settings."""
        return StorageConfig(
            data_root=self.data_root,
            volume_root=self.volume_root,
            bronze_path=self.bronze_path,
            silver_path=self.silver_path,
            gold_path=self.gold_path,
            quarantine_path=self.quarantine_path,
        )

    @property
    def redis(self) -> RedisConfig:
        """Grouped Redis event-bus settings."""
        return RedisConfig(
            url=self.redis_url,
            stream_name=self.redis_stream_name,
            consumer_group=self.redis_consumer_group,
            dlq_stream_name=self.redis_dlq_stream_name,
            claim_idle_ms=self.redis_claim_idle_ms,
            claim_batch_size=self.redis_claim_batch_size,
            process_max_retries=self.redis_process_max_retries,
            retry_backoff_seconds=self.redis_retry_backoff_seconds,
            read_batch_size=self.redis_read_batch_size,
            read_block_ms=self.redis_read_block_ms,
            process_concurrency=self.redis_process_concurrency,
        )

    @property
    def postgres(self) -> PostgresConfig:
        """Grouped Postgres catalog settings."""
        return PostgresConfig(url=self.postgres_url)

    @property
    def writer(self) -> WriterConfig:
        """Grouped Bronze/Silver writer tuning settings."""
        return WriterConfig(
            bronze_flush_interval_seconds=self.bronze_flush_interval_seconds,
            bronze_max_batch_size=self.bronze_max_batch_size,
            silver_target_file_size_mb=self.silver_target_file_size_mb,
            silver_max_rows_per_file=self.silver_max_rows_per_file,
            silver_max_flush_time_seconds=self.silver_max_flush_time_seconds,
            silver_row_group_size_mb=self.silver_row_group_size_mb,
        )

    @property
    def gold_poller(self) -> GoldPollerConfig:
        """Grouped Gold feature poller settings."""
        return GoldPollerConfig(
            enabled=self.gold_poller_enabled,
            eod_hour=self.gold_poller_eod_hour,
            eod_minute=self.gold_poller_eod_minute,
            check_interval_seconds=self.gold_poller_check_interval_seconds,
            retry_max=self.gold_poller_retry_max,
            retry_backoff_seconds=self.gold_poller_retry_backoff_seconds,
            project=self.gold_poller_project,
            version=self.gold_poller_version,
            lookback_days=self.gold_poller_lookback_days,
            disabled_pipelines=self.gold_poller_disabled_pipelines,
            disabled_pipeline_set=self.gold_poller_disabled_pipeline_set,
        )

    @property
    def watch(self) -> WatchConfig:
        """Grouped watch consumer settings."""
        return WatchConfig(
            redis_url=self.watch_redis_url,
            gateway_url=self.watch_gateway_url,
            gateway_api_key=self.watch_gateway_api_key,
            gateway_legacy_fallback_enabled=self.watch_gateway_legacy_fallback_enabled,
            enrichment_timeout_seconds=self.watch_enrichment_timeout_seconds,
            enrichment_option_chain_timeout_seconds=self.watch_enrichment_option_chain_timeout_seconds,
            enrichment_backfill_enabled=self.enrichment_backfill_enabled,
            enrichment_backfill_interval=self.enrichment_backfill_interval,
            enrichment_backfill_lookback_days=self.enrichment_backfill_lookback_days,
            enrichment_backfill_batch_size=self.enrichment_backfill_batch_size,
        )

    @property
    def health_monitor(self) -> HealthMonitorConfig:
        """Grouped health monitor settings."""
        return HealthMonitorConfig(
            enabled=self.health_monitor_enabled,
            stream_check_interval_seconds=self.health_stream_check_interval_seconds,
            partition_check_interval_seconds=self.health_partition_check_interval_seconds,
            volume_baseline_days=self.health_volume_baseline_days,
            stats_baseline_days=self.health_stats_baseline_days,
            volume_warn_ratio=self.health_volume_warn_ratio,
            volume_critical_ratio=self.health_volume_critical_ratio,
            null_rate_threshold=self.health_null_rate_threshold,
            psi_threshold=self.health_psi_threshold,
            leakage_sample_size=self.health_leakage_sample_size,
        )

    @property
    def dataflow_health(self) -> DataflowHealthConfig:
        """Grouped dataflow health verification settings."""
        return DataflowHealthConfig(
            consumer_metrics_url=self.health_consumer_metrics_url,
            watch_metrics_url=self.health_watch_metrics_url,
            freshness_seconds=self.health_freshness_seconds,
            report_dir=self.health_report_dir,
            interval_seconds=self.health_interval_seconds,
        )

    @property
    def catalog(self) -> CatalogConfig:
        """Grouped catalog settings."""
        return CatalogConfig(
            url=self.catalog_url,
            auto_discover=self.catalog_auto_discover,
            discover_interval_seconds=self.catalog_discover_interval_seconds,
        )

    @property
    def llm(self) -> LLMConfig:
        """Grouped LLM provider settings."""
        return LLMConfig(
            provider=self.llm_provider,
            model=self.llm_model,
            base_url=self.llm_base_url,
            api_key=self.llm_api_key,
            qwen_region=self.llm_qwen_region,
            effective_base_url=self.llm_effective_base_url,
        )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience alias
settings = get_settings()
