# Heber Technical Debt Audit

Date: 2026-02-05

This audit is based on static code inspection only. No services were run and no tests or linters were executed.

## Scope

Audit Pass 1 (2026-02-05, files reviewed directly):
- README.md
- CHANGELOG.md
- pyproject.toml
- docker-compose.yml
- Dockerfile
- CLAUDE.md
- codebase.md
- heber/config.py
- heber/models/envelope.py
- heber/sdk/client.py
- heber/writer/consumer.py
- heber/writer/bronze.py
- heber/writer/silver.py
- heber/writer/compactor.py
- heber/writer/hotstore.py
- heber/hotstore/client.py
- heber/hotstore/sync.py
- heber/firewall/asof.py
- heber/catalog/api.py
- heber/catalog/db.py
- tests/test_placeholder.py
- tests/test_edge_cases.py

Audit Pass 2 (2026-02-05, files reviewed directly):
- docs/architecture.md
- docs/operations/backup-dr-runbook.md
- docs/operations/cost-estimates.md
- docs/operations/deployment.md
- docs/operations/monitoring.md
- docs/operations/network-topology.md
- docs/operations/troubleshooting.md
- heber/watch/__init__.py
- heber/watch/__main__.py
- heber/watch/checker.py
- heber/watch/consumer.py
- heber/watch/features.py
- heber/watch/manager.py
- heber/watch/models.py
- heber/watch/poller.py
- heber/watch/writer.py
- heber/ml/datasets.py
- heber/ml/inference.py
- heber/ml/trainer.py
- heber/quality/contracts.py
- heber/quality/soda_scanner.py
- heber/quality/tests.py
- heber/sre/capacity.py
- heber/sre/chaos.py
- heber/sre/error_budget.py
- heber/sre/oncall.py
- heber/sre/runbooks.py
- heber/sre/slo.py
- heber/sre/tests.py
- heber/testing/ci_gates.py
- heber/testing/environments.py
- heber/testing/framework.py
- heber/testing/generators.py
- heber/testing/leakage.py
- heber/testing/performance.py
- heber/testing/tests.py
- heber/testing/tests_framework.py
- heber/storage/iceberg_catalog.py
- heber/storage/iceberg_writer.py
- heber/universe/survivor_bias.py
- heber/universe/tests.py
- heber/catalog/service.py
- heber/catalog/datasources.py
- heber/bus/__init__.py
- heber/bus/backpressure.py
- heber/bus/dedupe.py
- heber/bus/streams.py
- heber/schema/registry_client.py
- heber/schemas/additional.py

Audit Pass 3 (2026-02-05, files reviewed directly):
- features/entities.py
- features/feature_store.yaml
- features/feature_views/__init__.py
- features/feature_views/alert_labels.py
- features/feature_views/flow.py
- features/feature_views/labels.py
- features/feature_views/microstructure.py
- features/feature_views/momentum.py
- features/feature_views/volatility.py
- heber/features/__init__.py
- heber/features/pipelines/__init__.py
- heber/features/pipelines/alert_labels.py
- heber/features/templates/__init__.py
- heber/features/templates/alert_labels.py
- heber/features/templates/cross_asset.py
- heber/features/templates/flow.py
- heber/features/templates/labels.py
- heber/features/templates/microstructure.py
- heber/features/templates/momentum.py
- heber/features/templates/tests.py
- heber/features/templates/volatility.py

Audit Pass 4 (2026-02-05, files reviewed directly):
- heber/ops/__init__.py
- heber/ops/alerting.py
- heber/ops/circuit_breaker.py
- heber/ops/gap_resolutions.py
- heber/ops/health.py
- heber/ops/lifecycle.py
- heber/ops/logging.py
- heber/ops/metrics.py
- heber/ops/reliability.py
- heber/ops/slices.py
- heber/ops/tests_remaining.py
- heber/ops/tracing.py

Audit Pass 5 (2026-02-05, files reviewed directly):
- heber/firewall/__init__.py
- heber/firewall/scd.py
- heber/firewall/validation.py
- heber/firewall/tests.py
- heber/models/__init__.py
- heber/models/silver.py

Audit Pass 6 (2026-02-05, files reviewed directly):
- heber/gold/__init__.py
- heber/gold/label_tests.py
- heber/gold/labels.py
- heber/gold/split_tests.py
- heber/gold/splits.py
- heber/gold/tests.py
- heber/retention/__init__.py

Audit Pass 7 (2026-02-05, files reviewed directly):
- heber/feast/__init__.py
- heber/feast/materialization.py
- heber/feast/tests.py

Audit Pass 8 (2026-02-05, files reviewed directly):
- scripts/backup/clickhouse-backup.sh
- scripts/backup/validate-catalog-backup.sh
- scripts/docker-build.sh
- scripts/docker-push.sh
- scripts/init_volume.sh
- scripts/security-scan.sh
- docs/Alpaca_market_data_endpoints.md
- docs/Alpaca_trading_endpoints.md
- docs/UW_endpoints.md
- docs/catalog_api.md
- docs/configuration.md
- docs/data_contract.md
- docs/hot_store.md
- docs/iceberg_migration.md
- docs/labeling_strategy.md
- docs/schema_registry.md
- docs/schemaaudit.md
- docs/sdk.md

Audit Pass 9 (2026-02-05, files reviewed directly):
- heber/versioning/__init__.py
- heber/calendar/market.py

Audit Pass 10 (2026-02-05, files reviewed directly):
- heber/hotstore/tables.py
- heber/schemas/tests_additional.py

Audit Pass 11 (2026-02-05, files reviewed directly):
- infrastructure/terraform/main.tf
- infrastructure/terraform/environments/dev/main.tf
- infrastructure/terraform/environments/staging/main.tf
- infrastructure/terraform/environments/prod/main.tf
- k8s/base/configmap.yaml
- k8s/base/deployments/backfill.yaml
- k8s/base/deployments/catalog.yaml
- k8s/base/deployments/compactor.yaml
- k8s/base/deployments/consumer.yaml
- k8s/base/deployments/hotloader.yaml
- k8s/base/deployments/writer.yaml
- k8s/base/hpa/catalog.yaml
- k8s/base/hpa/consumer.yaml
- k8s/base/hpa/writer.yaml
- k8s/base/kustomization.yaml
- k8s/base/namespace.yaml
- k8s/base/pdb/catalog.yaml
- k8s/base/pdb/consumer.yaml
- k8s/base/pdb/hotloader.yaml
- k8s/base/pdb/writer.yaml
- k8s/base/secrets/cluster-secret-store.yaml
- k8s/base/secrets/external-secret.yaml
- k8s/base/secrets/secrets-local.yaml.example
- k8s/base/services/catalog.yaml
- k8s/base/services/consumer.yaml
- k8s/base/services/hotloader.yaml
- k8s/base/services/writer.yaml
- k8s/overlays/dev/kustomization.yaml
- k8s/overlays/staging/kustomization.yaml
- k8s/overlays/prod/kustomization.yaml

Audit Pass 12 (2026-02-05, files reviewed directly):
- heber/backfill/__init__.py
- heber/backtest/__init__.py
- heber/backtest/integration.py
- heber/backtest/tests.py

Audit Pass 13 (2026-02-06, files reviewed directly):
- heber/ops/logging.py
- heber/ops/reliability.py

Audit Pass 14 (2026-02-06, files reviewed directly):
- heber/versioning/__init__.py
- k8s/base/hpa/catalog.yaml
- k8s/base/hpa/consumer.yaml
- k8s/base/hpa/writer.yaml
- k8s/base/deployments/catalog.yaml
- k8s/base/deployments/consumer.yaml
- k8s/base/deployments/writer.yaml
- k8s/base/deployments/compactor.yaml
- k8s/base/deployments/hotloader.yaml
- k8s/base/deployments/backfill.yaml

Audit Pass 15 (2026-02-06, files reviewed directly):
- scripts/backup/clickhouse-backup.sh
- scripts/backup/validate-catalog-backup.sh
- scripts/security-scan.sh

Audit Pass 16 (2026-02-06, files reviewed directly):
- heber/ops/tracing.py
- scripts/init_volume.sh
- docs/labeling_strategy.md
- docs/data_contract.md

Audit Pass 17 (2026-02-06, files reviewed directly):
- heber/versioning/__init__.py
- k8s/base/hpa/catalog.yaml
- k8s/base/hpa/consumer.yaml
- k8s/base/hpa/writer.yaml
- k8s/base/deployments/catalog.yaml
- k8s/base/deployments/consumer.yaml
- k8s/base/deployments/writer.yaml
- k8s/base/deployments/compactor.yaml
- k8s/base/deployments/hotloader.yaml
- k8s/base/deployments/backfill.yaml
- heber/catalog/api.py
- heber/ops/metrics.py
- heber/writer/consumer.py
- heber/writer/compactor.py
- heber/writer/hotstore.py
- heber/hotstore/sync.py
- heber/backfill/__init__.py

Audit Pass 18 (2026-02-06, files reviewed directly):
- heber/ops/logging.py
- heber/ops/reliability.py
- docs/UW_endpoints.md

Audit Pass 19 (2026-02-06, files reviewed directly):
- scripts/backup/clickhouse-backup.sh
- scripts/backup/validate-catalog-backup.sh
- scripts/security-scan.sh
- docs/labeling_strategy.md
- docs/data_contract.md

Audit Pass 20 (2026-02-06, files reviewed directly):
- k8s/base/deployments/backfill.yaml
- k8s/base/deployments/hotloader.yaml
- heber/backfill/__init__.py
- heber/writer/hotstore.py

Audit Pass 21 (2026-02-06, files reviewed directly):
- heber/ops/health.py
- heber/ops/metrics.py
- heber/ops/alerting.py
- heber/ops/__init__.py
- k8s/base/deployments/catalog.yaml
- k8s/base/deployments/consumer.yaml
- k8s/base/deployments/writer.yaml
- k8s/base/hpa/catalog.yaml
- k8s/base/hpa/consumer.yaml
- k8s/base/hpa/writer.yaml

Audit Pass 22 (2026-02-06, files reviewed directly):
- heber/calendar/market.py
- heber/hotstore/tables.py
- heber/schemas/tests_additional.py

Audit Pass 23 (2026-02-06, files reviewed directly):
- heber/calendar/market.py
- tests/test_market_calendar_timezones.py

Audit Pass 24 (2026-02-06, files reviewed directly):
- heber/schemas/tests_additional.py

Audit Pass 25 (2026-02-06, files reviewed directly):
- heber/calendar/market.py
- tests/test_market_calendar_timezones.py

Audit Pass 26 (2026-02-06, files reviewed directly):
- heber/hotstore/tables.py
- heber/hotstore/sync.py
- tests/test_hotstore_unification.py

Audit Pass 27 (2026-02-06, files reviewed directly):
- scripts/security-scan.sh
- README.md

Audit Pass 28 (2026-02-06, files reviewed directly):
- scripts/backup/validate-catalog-backup.sh

Audit Pass 29 (2026-02-06, files reviewed directly):
- scripts/backup/clickhouse-backup.sh
- docs/operations/backup-dr-runbook.md

Audit Pass 30 (2026-02-06, files reviewed directly):
- docs/labeling_strategy.md
- docs/data_contract.md
- heber/firewall/validation.py
- heber/schemas/silver.py
- heber/sdk/client.py
- heber/watch/writer.py

Audit Pass 31 (2026-02-06, files reviewed directly):
- docs/UW_endpoints.md

Audit Pass 32 (2026-02-06, files reviewed directly):
- heber/ops/logging.py
- tests/test_logging_level_filtering.py
- docs/configuration.md

Audit Pass 33 (2026-02-06, files reviewed directly):
- heber/ops/reliability.py
- tests/test_event_deduplicator_rotation.py

Audit Pass 34 (2026-02-06, files reviewed directly):
- heber/versioning/__init__.py
- tests/test_lakefs_namespace_config.py
- docs/configuration.md

Audit Pass 35 (2026-02-06, files reviewed directly):
- k8s/base/hpa/catalog.yaml
- k8s/base/hpa/consumer.yaml
- k8s/base/hpa/writer.yaml
- k8s/base/deployments/backfill.yaml
- k8s/base/deployments/consumer.yaml
- k8s/base/deployments/writer.yaml
- k8s/base/deployments/compactor.yaml
- k8s/base/deployments/hotloader.yaml
- tests/test_k8s_hpa_probe_conformance.py

Audit Pass 36 (2026-02-06, files reviewed directly):
- heber/ops/tracing.py
- tests/test_tracing_no_otel.py

Audit Pass 37 (2026-02-06, files reviewed directly):
- scripts/init_volume.sh
- docs/configuration.md
- tests/test_init_volume_platform_guard.py

Audit Pass 38 (2026-02-06, files reviewed directly):
- heber/backfill/__main__.py
- heber/writer/hotstore.py
- tests/test_worker_entrypoint_services.py
- tests/test_runtime_entrypoints.py

Audit Pass 39 (2026-02-06, files reviewed directly):
- heber/ops/metrics.py
- heber/catalog/api.py
- heber/writer/consumer.py
- heber/writer/compactor.py
- heber/writer/hotstore.py
- heber/backfill/__main__.py
- tests/test_metrics_exporter_alignment.py

Audit Pass 40 (2026-02-06, files reviewed directly):
- heber/versioning/__init__.py
- tests/test_lakefs_operation_metrics.py

Audit Pass 41 (2026-02-06, files reviewed directly):
- infrastructure/terraform/environments/dev/main.tf
- infrastructure/terraform/environments/staging/main.tf
- infrastructure/terraform/environments/prod/main.tf
- infrastructure/terraform/environments/dev/backend.hcl
- infrastructure/terraform/environments/staging/backend.hcl
- infrastructure/terraform/environments/prod/backend.hcl
- tests/test_terraform_environment_config.py

Audit Pass 42 (2026-02-06, files reviewed directly):
- heber/backfill/__init__.py
- tests/test_backfill_writer_reliability.py

Audit Pass 43 (2026-02-06, files reviewed directly):
- heber/backfill/__init__.py
- tests/test_backfill_job_persistence.py

Audit Pass 44 (2026-02-06, files reviewed directly):
- heber/backfill/__init__.py
- tests/test_backfill_gap_detector_layout.py

Audit Pass 45 (2026-02-07, files reviewed directly):
- heber/backtest/integration.py
- heber/backtest/tests.py

Audit Pass 46 (2026-02-07, files reviewed directly):
- heber/backtest/integration.py
- heber/backtest/tests.py

Audit Pass 47 (2026-02-07, files reviewed directly):
- heber/retention/__init__.py
- tests/test_retention_gold_layout.py

Audit Pass 48 (2026-02-07, files reviewed directly):
- heber/retention/__init__.py
- tests/test_retention_gold_layout.py

Audit Pass 49 (2026-02-07, files reviewed directly):
- heber/gold/labels.py
- heber/gold/label_tests.py

Audit Pass 50 (2026-02-07, files reviewed directly):
- heber/ops/reliability.py
- tests/test_dead_letter_queue_persistence.py

Audit Pass 51 (2026-02-07, files reviewed directly):
- heber/firewall/scd.py
- heber/firewall/validation.py
- tests/test_firewall_scd_and_validation.py

Audit Pass 52 (2026-02-07, files reviewed directly):
- heber/models/silver.py
- heber/schemas/silver.py
- tests/test_silver_model_schema_alignment.py

Audit Pass 53 (2026-02-07, files reviewed directly):
- heber/config.py
- heber/feast/materialization.py
- tests/test_feast_materialization_behavior.py
- tests/test_sdk_catalog_defaults.py

Audit Pass 54 (2026-02-07, files reviewed directly):
- heber/ops/metrics.py
- heber/writer/consumer.py
- heber/writer/silver.py
- heber/writer/compactor.py
- tests/test_metrics_runtime_wiring.py

Audit Pass 55 (2026-02-07, files reviewed directly):
- heber/watch/models.py
- heber/watch/poller.py
- tests/test_watch_async_redis.py

Audit Pass 56 (2026-02-07, files reviewed directly):
- heber/models/envelope.py
- heber/writer/consumer.py
- tests/test_writer_consumer_reliability.py

Audit Pass 57 (2026-02-07, files reviewed directly):
- heber/watch/features.py
- tests/test_watch_feature_timezones.py

Audit Pass 58 (2026-02-07, files reviewed directly):
- heber/watch/gateway.py
- heber/watch/poller.py
- heber/watch/consumer.py
- heber/watch/features.py
- tests/test_watch_gateway_paths.py

Audit Pass 59 (2026-02-07, files reviewed directly):
- heber/ml/datasets.py
- heber/watch/features.py
- heber/watch/consumer.py
- tests/test_meta_label_dataset_paths.py
- tests/test_watch_feature_persistence.py

Audit Pass 60 (2026-02-07, files reviewed directly):
- heber/ml/trainer.py
- heber/ml/inference.py
- heber/watch/features.py
- tests/test_meta_feature_order_contract.py

Audit Pass 61 (2026-02-07, files reviewed directly):
- heber/quality/soda_scanner.py
- heber/quality/contracts.py
- heber/quality/tests.py
- tests/test_quality_soda_contracts.py

Audit Pass 62 (2026-02-07, files reviewed directly):
- heber/testing/framework.py
- heber/testing/tests_framework.py
- heber/testing/environments.py
- tests/test_testing_environment_defaults.py

Audit Pass 63 (2026-02-07, files reviewed directly):
- heber/storage/iceberg_catalog.py
- tests/test_iceberg_partition_spec_contract.py

Audit Pass 64 (2026-02-07, files reviewed directly):
- heber/bus/backpressure.py
- tests/test_backpressure_quarantine_paths.py

Audit Pass 65 (2026-02-07, files reviewed directly):
- heber/writer/hotstore.py
- heber/hotstore/sync.py
- heber/hotstore/client.py
- tests/test_hotstore_facade_alignment.py
- tests/test_hotstore_unification.py

Audit Pass 66 (2026-02-07, files reviewed directly):
- infrastructure/terraform/main.tf
- infrastructure/terraform/modules/*/main.tf
- tests/test_terraform_module_sources.py
- tests/test_terraform_root_module_contract.py

Audit Pass 67 (2026-02-07, files reviewed directly):
- k8s/base/kustomization.yaml
- k8s/base/deployments/*.yaml
- k8s/overlays/dev/kustomization.yaml
- k8s/overlays/staging/kustomization.yaml
- k8s/overlays/prod/kustomization.yaml
- tests/test_k8s_hpa_probe_conformance.py
- tests/test_k8s_kustomize_image_tags.py

Audit Pass 68 (2026-02-07, files reviewed directly):
- k8s/base/kustomization.yaml
- k8s/base/namespace.yaml
- k8s/base/serviceaccount.yaml
- k8s/base/secrets/cluster-secret-store.yaml
- k8s/base/secrets/external-secret.yaml
- k8s/base/deployments/*.yaml
- k8s/overlays/dev/kustomization.yaml
- k8s/overlays/staging/kustomization.yaml
- k8s/overlays/prod/kustomization.yaml
- tests/test_k8s_namespace_prerequisites.py

Audit Pass 69 (2026-02-07, files reviewed directly):
- k8s/base/deployments/*.yaml
- Dockerfile
- tests/test_runtime_entrypoints.py
- tests/test_worker_entrypoint_services.py
- tests/test_k8s_hpa_probe_conformance.py

Audit Pass 70 (2026-02-07, files reviewed directly):
- heber/hotstore/tables.py
- tests/test_hotstore_unification.py
- tests/test_hotstore_facade_alignment.py

Audit Pass 71 (2026-02-07, files reviewed directly):
- heber/ops/health.py
- heber/ops/reliability.py
- heber/ops/tests_remaining.py
- tests/test_ops_health_checks.py

Audit Pass 72 (2026-02-07, files reviewed directly):
- heber/watch/manager.py
- heber/watch/models.py
- heber/watch/consumer.py
- heber/watch/poller.py
- tests/test_watch_manager_redis_bytes.py
- tests/test_watch_async_redis.py
- tests/test_watch_consumer_reliability.py

Audit Pass 73 (2026-02-07, files reviewed directly):
- heber/watch/checker.py
- heber/watch/poller.py
- tests/test_watch_zero_price_handling.py
- tests/test_watch_async_redis.py
- tests/test_watch_consumer_reliability.py
- tests/test_watch_manager_redis_bytes.py

Audit Pass 74 (2026-02-07, files reviewed directly):
- heber/watch/writer.py
- tests/test_watch_writer_file_collisions.py
- tests/test_watch_zero_price_handling.py
- tests/test_watch_async_redis.py
- tests/test_watch_consumer_reliability.py
- tests/test_watch_manager_redis_bytes.py

Not yet audited in this run (recommend a future pass):
- heber/watch/__main__.py line-by-line re-audit for runtime lifecycle/shutdown edge cases.

## Remediation Updates

Updated: 2026-02-07

- `TD-015` addressed via `T-01`: Redis pending claims are consumed instead of dropped.
- `TD-016` addressed via `T-02`: meta-label writer and dataset builder columns are aligned.
- `TD-034` addressed via `T-03`: template `ts_available` values are source-derived.
- `TD-033` addressed via `T-04`: feature view schemas and offline paths align with Gold outputs and include schema/path regression coverage.
- `TD-001` addressed via `T-05`: pytest discovery now includes in-package tests under `heber/` and legacy test file names (`tests.py`, `tests_*.py`).
- `TD-003` and `TD-074` addressed via `T-06`: Docker/Kubernetes runtime commands now point to existing modules, replacing stale references to missing paths.
- `TD-073` addressed via `T-07`: Terraform local module paths now exist under `infrastructure/terraform/modules/*`, with tests that assert module source paths resolve.
- `TD-002` addressed via `T-08`: SDK default catalog URL now uses `HEBER_CATALOG_URL` (`http://localhost:8085/api/v1`) to match docker-compose host routing.
- `TD-004` and `TD-071` addressed via `T-09`: Hot Store sync/write implementation is unified under `heber.hotstore.sync` with a single `clickhouse-connect` client path and sync-safe table creation helpers.
- `TD-008` addressed via `T-10`: writer consumer now retries processing failures, claims idle pending messages at startup, and routes unrecoverable records to a Redis DLQ stream.
- `TD-005` addressed via `T-11`: Silver writer flush interval now respects `silver_max_flush_time_seconds` rather than Bronze flush configuration.
- `TD-006` addressed via `T-12`: all `heber/` runtime modules now use timezone-aware UTC timestamps (`datetime.now(UTC)`) in place of naive `datetime.utcnow()`, with a regression test guarding against reintroduction.
- `TD-007` addressed via `T-13`: compactor now streams small-file merges into a temp parquet, atomically promotes the merged file, and only deletes source files after successful promotion, with regression tests for failure safety.
- `TD-009` addressed via `T-14`: Silver Arrow schemas now live in shared `heber.schemas.silver` instead of inline writer constants; writer/transformer import the shared module with regression tests guarding against schema re-duplication.
- `TD-011` addressed via `T-15`: Hot Store event sync now buffers quote/trade/bar writes and flushes by row/time thresholds instead of one insert per event, with shutdown flush and regression tests.
- `TD-010` addressed via `T-16`: host defaults now align with docker-compose exposed ports (`HEBER_POSTGRES_URL` on `localhost:5433`, `HEBER_REDIS_URL` on `localhost:6380`) across settings, docs, and `.env.example`.
- `TD-012` addressed via `T-17`: Catalog startup now runs SQLAlchemy `create_all` only in `dev`, and Alembic migration scaffolding with an initial baseline revision is included for non-dev schema management.
- `TD-017` addressed via `T-18`: watch consumer/poller async flows now offload blocking Redis/manager operations via async wrappers and `asyncio.to_thread`, reducing event-loop stall risk.
- `TD-018` addressed via `T-19`: watch consumer now applies bounded retry/backoff for flow-alert processing, dead-letters terminal failures to Redis, and only ACKs after processing success or successful DLQ write.
- `TD-030` addressed via `T-20`: stream keys now use the unified `heber:events` namespace across bus enums/config helpers, watch consumer stream defaults, and operations runbook/troubleshooting commands.
- `TD-035` and `TD-036` addressed via `T-21`: alert labels pipeline now normalizes underlying symbols to canonical instrument keys and reads intraday bars from `bars` using `timeframe=5Min` filtering instead of querying a non-existent `bars_5min` dataset.
- `TD-037` addressed via `T-22`: alert-label intraday windows now use minute-based durations (5-minute bars) for `ts_available` and SPY-relative return horizons instead of day-based offsets.
- `TD-038` addressed via `T-23`: flow-feature windows now operate on normalized UTC `ts_event` values with time-window rolling semantics and regression coverage for timestamp normalization + 24h window boundaries.
- `TD-040` addressed via `T-24`: lifecycle async shutdown waits now short-circuit on pre-signaled shutdown and handle event-creation races so waits do not hang.
- `TD-041` addressed via `T-25`: lifecycle shutdown timeout paths now emit `shutdown_completed{status=\"timeout\"}` and return failure status instead of reporting success.
- `TD-042` addressed via `T-26`: `configure_logging()` now validates/normalizes `log_level`, applies stdlib root logger level, and enforces structlog filtering with regression tests for INFO/DEBUG behavior across JSON and console output modes.
- `TD-043` addressed via `T-27`: `EventDeduplicator` now rotates Bloom filters on a bounded interval, carries at most one prior window for recent duplicate detection, and exports rotation stats with regression coverage for pre-/post-rotation behavior.
- `TD-039` addressed via `T-33`: tracing decorators now avoid unconditional `SpanKind` access when OpenTelemetry is unavailable, and a regression test confirms `@traced` execution works with tracing disabled.
- `TD-061` addressed via `T-34`: `init_volume.sh` now checks host OS and `dot_clean` availability explicitly before cleanup, emits explicit skip reasons on unsupported hosts, and avoids implicit `|| true` fallback semantics.
- `TD-086` and `TD-087` addressed via `T-37`/`T-38`: backfill now has an executable `python -m heber.backfill` service module, and hotloader now exposes a real long-running CLI runtime in `python -m heber.writer.hotstore` with one-shot sync mode for controlled runs/tests.
- `TD-088` addressed via `T-40`: all deployments marked for Prometheus scraping are now backed by entrypoints that call `start_metrics_server_from_env`, with static conformance tests tying scrape annotations/ports to metrics-enabled runtimes.
- `TD-075` and `TD-076` addressed via `T-29`: k8s HPA manifests now use built-in CPU/memory resource metrics (removing stale custom-metric dependencies), and worker deployments now use exec-based probes tied to actual process entrypoints instead of non-existent HTTP health routes.
- `TD-066` addressed via `T-28`: `LakeFSConfig` now supports configurable storage namespace base/template fields and repository creation resolves namespaces from config/env instead of hardcoded literals, with regression tests for namespace resolution.
- `TD-068` addressed via `T-41`: `MarketCalendar` now normalizes datetime inputs to UTC before exchange conversion (naive inputs assumed UTC), and calendar timezone regression tests cover naive/aware/pandas timestamp inputs.
- `TD-069` addressed via `T-42`: `MarketCalendar(include_extended=True)` is now explicitly rejected with a clear `NotImplementedError`, removing misleading no-op behavior.
- `TD-070` addressed via `T-43`: Hot Store DDL now includes `quality_flags` and `lineage` base columns, and sync insert paths/tests were updated to keep writes compatible.
- `TD-072` addressed via `T-44`: additional schema registry tests now assert required contract names and lookup behavior instead of a brittle fixed total count.
- `TD-067` addressed via `T-45`: lakeFS versioning operations now emit consistent success/error/duration metrics for `create_tag`, `list_tags`, `merge`, and `diff`, including repository/branch resolution failure paths with regression tests.
- `TD-079` addressed via `T-46`: Terraform environment modules now take region from `var.aws_region`, backend blocks are partial (`backend "s3" {}`), and per-environment `backend.hcl` files remove hardcoded region keys while preserving state bucket/key/lock defaults.
- `TD-080` and `TD-082` addressed via `T-47`: backfill writes now persist raw records into Bronze partitions, update catalog dataset/coverage metadata on successful chunk writes, and fail fast when `pyarrow` is unavailable instead of silently dropping writes.
- `TD-081` addressed via `T-48`: backfill jobs and chunk progress now persist to disk and reload on startup, including resume-friendly recovery of stale `running` state after process restarts.
- `TD-083` addressed via `T-49`: gap detection now scans both legacy (`silver/{provider}_{feed}/dt=*`) and canonical (`silver/feed={feed}/instrument_type=*/dt=*`) Silver layouts so missing-date detection reflects actual partition paths.
- `TD-084` addressed via `T-50`: backtest data-loader label reads now pass explicit `label_version` values to `read_gold()` (defaulting to `"latest"`), removing unpinned label-version loads.
- `TD-085` addressed via `T-51`: backtest experiment config/results now carry dataset as-of timestamps (overall and per-fold metadata) to strengthen reproducibility of historical runs.
- `TD-051` and `TD-055` addressed via `T-52`: retention now scans Gold key-value layouts (`dataset=.../(project|type)=.../version=...[/dt=...]`) and version-pruning order now uses semantic-version-aware sorting instead of lexicographic ordering.
- `TD-052` and `TD-053` addressed via `T-53`: retention scheduler now enforces policies for `HOT_STORE` and `DLQ` layers, and reaper defaults now resolve storage roots from configured `HEBER_DATA_ROOT`/shared settings instead of hardcoded `/data/heber`.
- `TD-050` and `TD-054` addressed via `T-54`: label reads now resolve latest versions with semantic-version-aware ordering and fail closed when `ts_available` is missing.
- `TD-044` addressed via `T-55`: `DeadLetterQueue` now supports persistent storage and reload semantics so failed events survive process restarts.
- `TD-045` and `TD-046` addressed via `T-56`: SCD reference joins now handle both suffixed and unsuffixed validity columns, and strict Gold validation now fails only on hard leakage violations (not warning-only timestamp checks).
- `TD-047`, `TD-048`, and `TD-049` addressed via `T-57`: Silver models now normalize `lineage` serialization, apply release-aware `schema_version` defaults, and align date typing between Pydantic and Arrow schemas.
- `TD-056`, `TD-057`, and `TD-058` addressed via `T-58`: Feast helpers now default repo path from configuration, report best-effort materialization row counts instead of `-1` placeholders, and support tag value/key:value matching in feature search.
- `TD-014` addressed via `T-59`: metrics recording is now wired into core consumer/silver/compactor runtime paths with explicit exporter startup and regression coverage for metric emission.
- `TD-031` and `TD-032` addressed via `T-60`: watch model timestamp defaults are now timezone-aware UTC, and poller quote fetches now respect per-horizon cadence gates to avoid over-polling long-horizon watches.
- `TD-013` addressed via `T-61`: consumer processing now enforces canonical instrument-key format checks before Bronze/Silver writes, with regression coverage for invalid-key rejection.
- `TD-019` addressed via `T-62`: watch feature extraction now normalizes alert timestamps to US/Eastern (with naive-as-UTC behavior) before computing time-of-day and market-session timing features.
- `TD-020` addressed via `T-63`: watch-service Data Gateway calls now use shared endpoint construction with `/api/v1`-first routing and legacy fallback for consistent cross-module behavior.
- `TD-021` and `TD-022` addressed via `T-64`: meta-label dataset defaults now resolve from configured Gold root with legacy-path fallback, and watch feature extraction now persists feature rows into Gold partitions during ingestion.
- `TD-023` addressed via `T-65`: trained feature order is now persisted in model metadata and inference scoring uses that stored ordering for feature-vector construction.
- `TD-024` and `TD-025` addressed via `T-66`: Soda scanner default Silver path now resolves from shared settings (`settings.silver_path`) and non-null per-column reporting now uses each contract threshold.
- `TD-026` and `TD-027` addressed via `T-67`: E2E framework schedule API is now verified present by regression tests, and testing-environment local service defaults now align with docker-compose host port mappings.
- `TD-028` addressed via `T-68`: Iceberg Silver table creation now passes a concrete `PartitionSpec`/`DayTransform` object instead of list-form partition definitions, with regression coverage to prevent API drift.
- `TD-029` addressed via `T-69`: quarantine partition extraction now prefers canonical top-level envelope `provider`/`feed` fields with legacy `meta` fallback, with regression tests for both payload shapes.
- `TD-073` addressed via `T-70`: Terraform root-module output references are now regression-tested against local module outputs in addition to module-source existence checks.
- `TD-077` addressed via `T-71`: overlay image transformers now target the base-rewritten image name (`ghcr.io/jacobmcmillan/heber`) so env-specific tags override correctly, with regression checks for both kustomization contracts and rendered output tags.
- `TD-078` addressed via `T-72`: base kustomize resources now include `serviceaccount.yaml`, `secrets/cluster-secret-store.yaml`, and `secrets/external-secret.yaml`, with rendered-overlay tests asserting namespace-scoped runtime prerequisites (`heber` service account and `heber-secrets` external secret wiring).
- `TD-074` addressed via `T-73`: deployment entrypoint conformance coverage now validates all base deployment command modules (`catalog`, `consumer`, `writer`, `compactor`, `hotloader`, `backfill`) against importable Python modules.
- `TD-065` addressed via `T-32`: filesystem Trivy scan now uses explicit `--exit-code 1` gating and script control flow reports/returns failure for HIGH/CRITICAL findings.
- `TD-060` addressed via `T-31`: catalog backup validation now guarantees test-instance cleanup via `EXIT` trap, including failure paths.
- `TD-059` addressed via `T-30`: clickhouse backup script now reports config-driven remote destination/entry instead of a hardcoded S3 path not enforced by the command.
- `TD-062` addressed via `T-35`: labeling strategy docs now reference `heber/firewall/validation.py` and the current `validate_train_test_split` argument contract.
- `TD-063` addressed via `T-36`: data contract docs now reference `heber/schemas/silver.py` and concrete Gold key-value path conventions used by SDK/label writers.
- `TD-064` addressed via `T-39`: UW endpoint summary now derives from table statuses and no longer claims stale in-progress/not-started totals.
- Audit Pass 17 revalidated `TD-066`, `TD-067`, `TD-075`, and `TD-076` as still open, and added `TD-086` and `TD-087` for k8s worker entrypoint runtime failures.
- Audit Pass 18 revalidated `TD-042`, `TD-043`, and `TD-064` as still open (logging level filtering, dedupe rotation policy, and UW endpoint tracker drift).
- Audit Pass 19 revalidated `TD-059`, `TD-060`, `TD-062`, `TD-063`, and `TD-065` as still open (backup/security script hardening + docs alignment drift).
- Audit Pass 20 revalidated `TD-086` and `TD-087` as still open (k8s worker entrypoints still fail/exit immediately).
- Audit Pass 21 revalidated `TD-075` and `TD-076` as still open, and added `TD-088` for Prometheus scrape/metrics-server wiring drift.
- Audit Pass 22 revalidated `TD-068`, `TD-069`, `TD-070`, and `TD-072` as still open (calendar timezone handling, unused market-hours flag, Hot Store provenance-column drift, and brittle schema-count assertions).
- Audit Pass 22 revalidated `TD-071` as resolved via `T-09` (`create_all_tables()` now supports sync clients and `create_all_tables_async()` handles awaitable execution).
- Audit Pass 23 revalidated `TD-068` as resolved via `T-41` and focused timezone regression coverage.
- Audit Pass 24 revalidated `TD-072` as resolved via `T-44`; schema registry tests are now growth-tolerant.
- Audit Pass 25 revalidated `TD-069` as resolved via `T-42`; extended-hours mode is now explicit and non-silent.
- Audit Pass 26 revalidated `TD-070` as resolved via `T-43`; Hot Store DDL and insert mappings now include provenance/quality base fields.
- Audit Pass 27 revalidated `TD-065` as resolved via `T-32`; filesystem security findings now fail the scan script.
- Audit Pass 28 revalidated `TD-060` as resolved via `T-31`; backup-validation test instances are now cleaned up on failure.
- Audit Pass 29 revalidated `TD-059` as resolved via `T-30`; remote-destination output now reflects effective clickhouse-backup behavior.
- Audit Pass 30 revalidated `TD-062` and `TD-063` as resolved via `T-35`/`T-36`; labeling and data-contract docs now match current code paths and path conventions.
- Audit Pass 31 revalidated `TD-064` as resolved via `T-39`; UW endpoint summary counts now match table rows.
- Audit Pass 32 revalidated `TD-042` as resolved via `T-26`; log-level filtering is now enforced and covered by regression tests.
- Audit Pass 33 revalidated `TD-043` as resolved via `T-27`; dedupe Bloom state is now bounded by rotation with regression coverage.
- Audit Pass 34 revalidated `TD-066` as resolved via `T-28`; lakeFS repository creation now uses configurable storage namespace resolution.
- Audit Pass 35 revalidated `TD-075` and `TD-076` as resolved via `T-29`; HPA metric sources and worker probe types now match runtime behavior.
- Audit Pass 36 revalidated `TD-039` as resolved via `T-33`; tracing decorators now remain safe without OpenTelemetry.
- Audit Pass 37 revalidated `TD-061` as resolved via `T-34`; volume-init cleanup now uses explicit cross-platform/tool checks.
- Audit Pass 38 revalidated `TD-086` and `TD-087` as resolved via `T-37`/`T-38`; worker deployment entrypoint modules now execute with service-mode runtime behavior.
- Audit Pass 39 revalidated `TD-088` as resolved via `T-40`; scraped deployments now map to metrics-exporter startup in service entrypoints.
- Audit Pass 40 revalidated `TD-067` as resolved via `T-45`; lakeFS operation metrics now cover `create_tag`/`list_tags`/`merge`/`diff` success and error paths.
- Audit Pass 41 revalidated `TD-079` as resolved via `T-46`; Terraform environment region/backend settings now support override without editing `main.tf`.
- Audit Pass 42 revalidated `TD-080` and `TD-082` as resolved via `T-47`; backfill now writes Bronze + catalog coverage metadata and no longer silently succeeds without `pyarrow`.
- Audit Pass 43 revalidated `TD-081` as resolved via `T-48`; backfill job state now survives restarts and resumes from persisted progress.
- Audit Pass 44 revalidated `TD-083` as resolved via `T-49`; gap detection now unions dates across legacy and canonical Silver storage layouts.
- Audit Pass 45 revalidated `TD-084` as resolved via `T-50`; backtest label reads now honor explicit label-version pinning.
- Audit Pass 46 revalidated `TD-085` as resolved via `T-51`; backtest experiment metadata now records as-of cutoffs for reproducibility.
- Audit Pass 47 revalidated `TD-051` and `TD-055` as resolved via `T-52`; Gold retention scan paths and version-pruning order now match actual Gold layout/version semantics.
- Audit Pass 48 revalidated `TD-052` and `TD-053` as resolved via `T-53`; retention policy execution now includes `HOT_STORE`/`DLQ` and uses configuration-aligned storage roots by default.
- Audit Pass 49 revalidated `TD-050` and `TD-054` as resolved via `T-54`; label latest-version selection and point-in-time fail-closed guards now align with intended behavior.
- Audit Pass 50 revalidated `TD-044` as resolved via `T-55`; DLQ entries now persist across restarts with replay-safe queue semantics.
- Audit Pass 51 revalidated `TD-045` and `TD-046` as resolved via `T-56`; firewall SCD join and strict Gold validation now match expected runtime semantics.
- Audit Pass 52 revalidated `TD-047`, `TD-048`, and `TD-049` as resolved via `T-57`; Silver model defaults and field types now match schema contracts with regression coverage.
- Audit Pass 53 revalidated `TD-056`, `TD-057`, and `TD-058` as resolved via `T-58`; Feast helpers now use configuration-driven defaults, value-aware tag filtering, and non-placeholder materialization counts.
- Audit Pass 54 revalidated `TD-014` as resolved via `T-59`; core ingestion/storage runtime paths now emit concrete metrics rather than placeholder-only counters.
- Audit Pass 55 revalidated `TD-031` and `TD-032` as resolved via `T-60`; watch timestamps now use aware UTC defaults and poller cadence now honors horizon intervals.
- Audit Pass 56 revalidated `TD-013` as resolved via `T-61`; invalid instrument keys are now rejected before persistence and covered by consumer reliability tests.
- Audit Pass 57 revalidated `TD-019` as resolved via `T-62`; watch timing features now derive from market-timezone-normalized alert timestamps.
- Audit Pass 58 revalidated `TD-020` as resolved via `T-63`; watch modules now share Data Gateway route construction with API-prefix-first fallback behavior.
- Audit Pass 59 revalidated `TD-021` and `TD-022` as resolved via `T-64`; meta-label paths now follow configured Gold roots and feature rows persist to Gold at watch-ingest time.
- Audit Pass 60 revalidated `TD-023` as resolved via `T-65`; inference now consumes persisted training feature order rather than relying on hardcoded feature extraction order.
- Audit Pass 61 revalidated `TD-024` and `TD-025` as resolved via `T-66`; Soda scanner defaults now follow configured Silver root semantics and completeness reporting now uses contract-specific thresholds.
- Audit Pass 62 revalidated `TD-026` and `TD-027` as resolved via `T-67`; framework schedule API coverage is now explicit and testing environment defaults now match compose host-port expectations.
- Audit Pass 63 revalidated `TD-028` as resolved via `T-68`; Iceberg Silver table creation now builds partition specs with PyIceberg `PartitionSpec` objects.
- Audit Pass 64 revalidated `TD-029` as resolved via `T-69`; quarantine partition paths now align with canonical envelope fields and preserve legacy fallback compatibility.
- Audit Pass 65 revalidated `TD-004` as resolved via `T-09`; Hot Store facade/runtime paths remain unified on `clickhouse-connect` with regression coverage to prevent reintroduction of divergent client stacks.
- Audit Pass 66 revalidated `TD-073` as resolved via `T-07`/`T-70`; Terraform module sources and root output wiring contracts now have regression coverage.
- Audit Pass 67 revalidated `TD-077` as resolved via `T-71`; kustomize overlay image tag overrides now apply correctly across `dev`, `staging`, and `prod`.
- Audit Pass 68 revalidated `TD-078` as resolved via `T-72`; overlay renders now include the required namespace-scoped runtime prerequisites for service accounts and secrets.
- Audit Pass 69 revalidated `TD-074` as resolved via `T-06`/`T-73`; deployment runtime-entrypoint commands remain aligned with importable modules across all base worker/service deployments.
- Audit Pass 70 revalidated `TD-071` as resolved via `T-09`/`T-74`; sync and async Hot Store table-creation helpers now enforce execution-mode boundaries with regression coverage.
- Audit Pass 71 revalidated and remediated `TD-089`; PostgreSQL readiness checks now execute SQLAlchemy 2.x-compatible statements with regression coverage.
- Audit Pass 72 revalidated and remediated `TD-090`; watch lookups now normalize Redis byte IDs so active/symbol watch queries remain correct with default Redis client decoding behavior.
- Audit Pass 73 revalidated and remediated `TD-091`; zero-price option snapshots now remain valid inputs for return/barrier evaluation instead of being dropped by truthiness checks.
- Audit Pass 74 revalidated and remediated `TD-092`; label writer partition files now use collision-safe names so repeated flushes in the same second do not overwrite prior output.

## Executive Summary

The core architecture is clear, but several operational hazards and correctness gaps remain. The most urgent issues are test discovery (most in-package tests are not being executed), mismatched service ports (SDK defaults do not match docker-compose), invalid Dockerfile targets, inconsistent Hot Store implementations, a broken meta-label training pipeline (label columns and paths do not match), an event-bus claim path that can silently drop messages, and a Feast/feature pipeline mismatch (feature views do not align with Gold layout or computed columns). In ops, tracing is not safe to disable (decorators crash when OpenTelemetry is missing), async shutdown signaling can hang, and deduplication can permanently drop valid events due to unbounded Bloom false positives. In the firewall/models layer, SCD joins can reference missing columns, Gold build validation treats warnings as hard failures, and Silver schemas drift between Pydantic models and Arrow definitions (lineage types, schema versions, and date representations). Feast helper path/count/search issues from prior passes are now remediated. In lakeFS versioning logic, repository creation is hardcoded to a fixed S3 namespace. In infrastructure manifests, Terraform references missing modules and Kubernetes configs reference images/commands that do not exist in this repo, while HPAs and probes assume metrics/health endpoints that are not implemented. In backfill/backtest, APIs allow unbounded background tasks with no persistence or cancellation signaling, and backtest reproducibility does not capture data as-of cutoffs. There are also multiple time-handling risks and data pipeline resiliency gaps that could lead to leakage or data loss.

## Findings Summary

Severity key: High, Medium, Low

| ID | Severity | Area | Summary |
| --- | --- | --- | --- |
| TD-001 | High | Testing | Pytest only discovers `tests/`, so in-package tests under `heber/` are not executed. |
| TD-002 | High | SDK/Config | SDK default catalog URL uses port 8080 but docker-compose exposes 8085. |
| TD-003 | High | Docker | Dockerfile stages reference non-existent modules. |
| TD-004 | Low | Hot Store | Hot Store remains unified under `heber.hotstore.sync` with facade compatibility and no `clickhouse_driver` drift. |
| TD-005 | Medium | Silver Writer | Flush interval uses Bronze setting instead of Silver setting. |
| TD-006 | Medium | Time Handling | Widespread use of naive `datetime.utcnow()` despite UTC expectations. |
| TD-007 | Medium | Compaction | Compactor loads all files into memory and deletes originals without atomic swap. |
| TD-008 | Medium | Ingestion | Redis consumer has no DLQ or pending-entries recovery. |
| TD-009 | Medium | Schemas | Schema definitions duplicated and hardcoded in `heber/writer/silver.py`. |
| TD-010 | Medium | Local Dev | Config defaults (Redis/Postgres) do not align with docker-compose host ports. |
| TD-011 | Medium | Hot Store | HotStoreSync inserts one row per event with no batching. |
| TD-012 | Medium | Catalog DB | No Alembic migrations; tables are created at runtime. |
| TD-013 | Low | Validation | Consumer now enforces instrument key format validation before Bronze/Silver persistence. |
| TD-014 | Low | Observability | Core ingestion/storage runtimes now emit concrete Prometheus metrics and exporter entrypoints are wired across services. |
| TD-015 | High | Event Bus | Claimed pending messages are not yielded to consumers, risking silent drops. |
| TD-016 | High | ML | Meta-label dataset builder expects columns not produced by the label writer. |
| TD-017 | Medium | Watch Service | Uses synchronous Redis calls inside async loops; can block the event loop. |
| TD-018 | Medium | Watch Service | Acks watch-stream messages even when processing fails; no DLQ. |
| TD-019 | Medium | Features | Watch timing features now normalize alert timestamps to US/Eastern before time-of-day extraction. |
| TD-020 | Medium | Integration | Watch modules now share consistent Data Gateway endpoint construction with API-prefix-first fallback. |
| TD-021 | Medium | ML | Meta-label dataset defaults now resolve from configured Gold root, with legacy path fallback support. |
| TD-022 | Medium | ML | Watch feature extraction now persists feature rows to Gold partitions consumed by dataset builder. |
| TD-023 | Medium | ML | Feature order is now persisted with trained models and reused during inference scoring. |
| TD-024 | Low | Data Quality | Soda scanner defaults now resolve Silver root from shared settings (`settings.silver_path`) with env override support. |
| TD-025 | Low | Data Quality | Non-null per-column threshold reporting now uses each contract threshold (no hard-coded 0.99). |
| TD-026 | Low | Testing | `E2ETestSuite.get_schedule()` is present and now explicitly covered by regression tests. |
| TD-027 | Low | Environment | Testing environment local-service defaults now align with docker-compose host port mappings. |
| TD-028 | Low | Iceberg | `create_silver_table` now builds a concrete PyIceberg `PartitionSpec` with `DayTransform` for `ts_event`. |
| TD-029 | Low | Backpressure | Quarantine partition routing now uses canonical top-level envelope `provider`/`feed` with legacy fallback. |
| TD-030 | Medium | Streams | Stream naming diverges (`stream:*` vs `heber:events`) across code/docs. |
| TD-031 | Low | Watch Models | Watch model timestamp defaults now use timezone-aware UTC values with regression coverage. |
| TD-032 | Low | Watch Poller | Poller quote fetches now honor per-horizon cadence gates to avoid long-horizon over-polling. |
| TD-033 | High | Feast/Features | Feature views hardcode paths and schemas that do not match Gold layout or computed columns. |
| TD-034 | High | Features | Feature templates set `ts_available` to current time, breaking point-in-time correctness. |
| TD-035 | Medium | Label Pipeline | Alert label pipeline queries bars using raw symbols, not canonical instrument keys. |
| TD-036 | Medium | Label Pipeline | Pipeline references `bars_5min` dataset that is not defined in Silver schemas. |
| TD-037 | Medium | Label Pipeline | Intraday labeling uses day-based windows for availability and SPY returns. |
| TD-038 | Medium | Flow Features | Time-window rolling uses `on=ts_event` on Series and lacks datetime normalization. |
| TD-039 | Medium | Tracing | Tracing decorator crashes when OpenTelemetry is not installed, despite intended noop behavior. |
| TD-040 | Medium | Lifecycle | Async shutdown wait can hang if shutdown happens before the async event is created. |
| TD-041 | Low | Lifecycle | Shutdown timeouts are logged but still reported as success in metrics. |
| TD-042 | Low | Logging | `configure_logging()` accepts a log level but does not apply it. |
| TD-043 | Medium | Reliability | Bloom filter dedupe has no TTL/rotation, causing rising false positives and potential drops without a backing store. |
| TD-044 | Low | Reliability | Dead-letter queue now supports persistence and startup reload, preventing restart-time loss of failed events. |
| TD-045 | Medium | Firewall | SCD joins now resolve validity columns from suffixed or unsuffixed names, avoiding runtime failures from suffix assumptions. |
| TD-046 | Low | Firewall | Strict Gold validation now raises only on hard leakage gates, while warning-only checks remain non-fatal. |
| TD-047 | Medium | Models | Silver models now normalize dict lineage to canonical JSON strings, matching string-backed schema storage. |
| TD-048 | Low | Models | Silver models now apply release-aware default schema versions (`v2`..`v6`) instead of defaulting all datasets to `v1`. |
| TD-049 | Low | Models | Expiry/date fields are now consistently typed as dates across Silver models and canonical Arrow schemas. |
| TD-050 | Low | Gold Labels | `read_label()` now resolves latest version using semantic-version-aware ordering instead of lexicographic directory sort. |
| TD-051 | Medium | Retention | Gold retention scanning now discovers canonical key-value layouts (`dataset=.../(project|type)=.../version=...[/dt=...]`) and records version metadata for pruning. |
| TD-052 | Low | Retention | Reaper now executes retention policies for `HOT_STORE` and `DLQ` layers alongside Bronze/Silver/Gold. |
| TD-053 | Low | Retention | Reaper defaults now resolve storage roots from configured `HEBER_DATA_ROOT` / shared settings instead of hardcoded `/data/heber`. |
| TD-054 | Medium | Gold Labels | `read_label()` now fails closed when `ts_available` is missing, preventing point-in-time guard bypass. |
| TD-055 | Low | Retention | Version pruning now uses semantic-version-aware ordering (with fallback) instead of pure lexicographic sorting. |
| TD-056 | Low | Feast | Feast helper defaults now resolve repo path from settings/env (`HEBER_FEAST_REPO_PATH`, legacy `FEAST_REPO_PATH`) instead of a hardcoded literal. |
| TD-057 | Low | Feast | Materialization now reports row counts from Feast responses when available, with offline-source estimation fallback instead of `-1` placeholders. |
| TD-058 | Low | Feast | `search_features()` now supports key, value, and `key:value` tag filters (case-insensitive) rather than key-only matches. |
| TD-059 | Low | Scripts | ClickHouse backup script output now reflects config-managed remote destination without misleading hardcoded S3 path. |
| TD-060 | Medium | Scripts | Catalog backup validation cleanup now runs on all exit paths (success/failure). |
| TD-061 | Low | Scripts | Volume init script assumes macOS (`dot_clean`) without platform checks. |
| TD-062 | Low | Docs | Labeling strategy docs now reference the current split-validation module and signature. |
| TD-063 | Low | Docs | Data contract docs now reference canonical Silver schema source and concrete Gold path conventions. |
| TD-064 | Low | Docs | UW endpoint summary counts now match table statuses (`✅ 76`, `⚪ 1`, `🔄 0`, `❌ 0`). |
| TD-065 | Low | Scripts | Filesystem security scan now fails on HIGH/CRITICAL secret/misconfig findings. |
| TD-066 | Medium | Versioning | lakeFS repo creation hardcodes S3 namespace (`s3://heber-lakehouse/{repo}`) and ignores config. |
| TD-067 | Low | Versioning | lakeFS metrics are missing for tag/list/diff operations and error paths are not consistently instrumented. |
| TD-068 | Medium | Calendar | Market calendar naive datetime handling was remediated in `T-41` via UTC normalization and regression tests. |
| TD-069 | Low | Calendar | `include_extended=True` is now explicitly rejected to avoid silent no-op behavior. |
| TD-070 | Low | Hot Store | Hot Store DDL and sync mappings now preserve base provenance/quality fields (`quality_flags`, `lineage`). |
| TD-071 | Low | Hot Store | Hot Store table-creation helpers now preserve explicit sync/async boundaries, reject mode misuse, and are covered by regression tests. |
| TD-072 | Low | Testing | Additional schema registry tests are now growth-tolerant and verify required schema contracts. |
| TD-073 | Low | Infra | Terraform root module local sources and referenced module outputs are covered by regression checks. |
| TD-074 | Low | K8s | Deployment runtime-entrypoint commands are regression-tested across all base deployments to ensure module paths remain importable. |
| TD-075 | Medium | K8s | HPA targets custom metrics that are not exported by current metrics definitions. |
| TD-076 | Medium | K8s | Liveness/readiness probes expect `/health` and `/ready` endpoints on metrics ports that are not implemented. |
| TD-077 | Low | K8s | Overlay image rewrite rules and rendered manifests are regression-tested to ensure env-specific tags apply after base image-name rewrites. |
| TD-078 | Low | K8s | Base resources and rendered overlays now include/validate namespace-scoped service account + external-secret prerequisites for deployment secret/config wiring. |
| TD-079 | Low | Infra | Terraform backends and module configs are hardcoded to `us-east-1` without variable override. |
| TD-080 | Medium | Backfill | Backfill writes only Silver temp partitions and never updates Bronze, coverage, or catalog metadata. |
| TD-081 | Medium | Backfill | Backfill jobs are in-memory only; jobs and progress are lost on restart. |
| TD-082 | Medium | Backfill | `BackfillWriter._write_parquet()` silently drops data when `pyarrow` is missing. |
| TD-083 | Low | Backfill | Gap detection assumes `silver/{provider}_{feed}/dt=*` layout, which may not match actual partitions. |
| TD-084 | Low | Backtest | Label reads use `read_gold()` without a version parameter, which may load unintended versions. |
| TD-085 | Low | Backtest | Experiment results omit dataset as-of timestamps, weakening reproducibility. |
| TD-086 | Medium | K8s | Backfill deployment runs `python -m heber.backfill`, but the package has no `__main__`, so the container exits immediately with module-execution errors. |
| TD-087 | Medium | K8s | Hotloader deployment runs `python -m heber.writer.hotstore`, but that module is a compatibility facade with no long-running entrypoint, so the container exits immediately. |
| TD-088 | Medium | Observability | Deployments advertise Prometheus scraping on port 9090, but service entrypoints do not start a metrics HTTP server, so scrape targets are non-functional. |
| TD-089 | Medium | Ops Health | PostgreSQL readiness check executed raw SQL string (`conn.execute(\"SELECT 1\")`), which fails under SQLAlchemy 2.x and can report false `not_ready` status. |
| TD-090 | Medium | Watch Service | Watch manager assumed Redis set members were `str`; default `redis.from_url` responses are often `bytes`, which can silently break active/symbol watch retrieval. |
| TD-091 | Medium | Watch Service | Zero-valued quote fields (`0.0`) were treated as falsy in poller/checker return-path logic, which could suppress `-100%` outcomes and miss SL barrier classification. |
| TD-092 | Medium | Watch Service | Label writer used second-granularity parquet filenames, so multiple flushes in the same second could overwrite prior partition files and lose labels. |

## Detailed Findings

**TD-001: Pytest only discovers `tests/`, so in-package tests are not executed.**
Evidence: `pyproject.toml` restricts `testpaths` to `tests`, while there are multiple test modules under `heber/` such as `heber/catalog/tests_access_control.py` and `heber/ops/tests_remaining.py`. This means those tests never run in CI or locally by default. This hides regressions in critical logic.
Recommendation: Expand `testpaths` to include `heber` or move internal tests into `tests/` with a consistent naming convention. Add a minimal CI check that confirms expected test modules are collected.

**TD-002: SDK default catalog URL uses port 8080 but docker-compose exposes 8085.**
Evidence: `heber/config.py` defaults `api_port` to 8080 and `heber/sdk/client.py` uses `settings.api_port`. `docker-compose.yml` maps catalog 8080 to host 8085 and `README.md` advertises 8085. This mismatch breaks the default SDK in a typical docker-compose local setup.
Recommendation: Add `HEBER_CATALOG_URL` and use it as the SDK default. Align README and `.env.example` with the new behavior.

**TD-003: Dockerfile stages reference non-existent modules.**
Evidence: `Dockerfile` stages reference `heber.bus.consumer`, `heber.writer.service`, and `heber.writer.compaction`, which are not present. This makes those build targets fail and creates confusion for deployment.
Recommendation: Fix stage commands to point to existing modules (`heber.writer.consumer`, `heber.writer.compactor`) or remove the stages entirely if unused.

**TD-004: Hot Store has two divergent implementations with inconsistent clients and async behavior.**
Evidence: `heber/hotstore/client.py` uses `clickhouse-connect` synchronously. `heber/writer/hotstore.py` uses `clickhouse_driver` and defines async methods that `await` a non-async client. `heber/hotstore/sync.py` uses a third approach with direct inserts. This creates runtime inconsistency and unclear ownership of the Hot Store pipeline.
Recommendation: Pick one Hot Store implementation, unify on a single ClickHouse client, and enforce a consistent async model. Deprecate or delete the unused path.
Update 2026-02-07: Revalidated in `T-09`/Pass 65. `heber/writer/hotstore.py` now remains a compatibility facade over `heber.hotstore.sync`, and Hot Store runtime/client paths use `clickhouse_connect` only.
Revalidated 2026-02-07 (Pass 65): Resolved. Regression tests now enforce facade alignment and prevent reintroduction of `clickhouse_driver` references.

**TD-005: Silver writer flush interval uses Bronze setting.**
Evidence: `heber/writer/silver.py` uses `settings.bronze_flush_interval_seconds` inside `flush_if_needed`. This is likely a bug and creates unexpected flush timing for Silver files.
Recommendation: Replace with `settings.silver_max_flush_time_seconds` and add a regression test around flush timing.

**TD-006: Naive time handling risks incorrect UTC semantics.**
Evidence: `datetime.utcnow()` is used in `heber/writer/bronze.py`, `heber/writer/silver.py`, `heber/writer/compactor.py`, and `heber/catalog/api.py`. These values are naive (no timezone). Some schemas expect `timestamp('us', tz='UTC')` which will mismatch or silently coerce.
Recommendation: Replace with `datetime.now(UTC)` and enforce timezone-aware timestamps in `EventEnvelope` validation. Add a guard that rejects naive datetimes for `ts_event`, `ts_ingest`, and `ts_available`.

**TD-007: Compactor loads all small files into memory and deletes originals without atomic swap.**
Evidence: `heber/writer/compactor.py` reads all parquet files into memory, concatenates, writes a new file, and deletes originals immediately. A crash between write and delete will corrupt data or lose lineage.
Recommendation: Use streaming compaction or chunked merges, write to a temp file, and replace atomically. Consider using a manifest or lock file.

**TD-008: Redis consumer has no DLQ or pending-entry recovery.**
Evidence: `heber/writer/consumer.py` logs failed events but does not push to a dead-letter stream or attempt to claim pending entries on startup. Failed events can remain stuck in the group without visibility.
Recommendation: Add a DLQ stream, implement a retry/backoff policy, and add a startup sweep of `XPENDING`/`XCLAIM` for stale messages.

**TD-009: Schema definitions are duplicated and hardcoded in `heber/writer/silver.py`.**
Evidence: `SILVER_SCHEMAS` is a large in-code dictionary while schema information also exists in docs and likely in `heber/schemas/`. This will drift over time and complicates schema evolution.
Recommendation: Move schemas into a single source of truth (JSON/YAML or a schema registry client) and generate both Arrow schemas and documentation from it.

**TD-010: Local dev ports are inconsistent between config and docker-compose.**
Evidence: `docker-compose.yml` exposes Postgres on 5433 and Redis on 6380, but `heber/config.py` defaults to 5432 and 6379. This breaks local usage from host processes unless the user manually sets env vars.
Recommendation: Align defaults with compose host ports or explicitly document and populate `.env.example` with correct values for local dev.

**TD-011: HotStoreSync inserts one row per event without batching.**
Evidence: `heber/hotstore/sync.py` calls `client.insert` per event. This will be slow under load and could violate the 5-minute SLA.
Recommendation: Buffer and batch inserts in the sync path and add a flush policy similar to the Silver writer.

**TD-012: Catalog DB has no migration strategy.**
Evidence: `heber/catalog/api.py` calls `Base.metadata.create_all` on startup and there are no Alembic migrations in the repo. This is brittle for production schema changes.
Recommendation: Add Alembic migrations and remove auto-create for non-dev environments.

**TD-013: Instrument key validation is defined but not enforced.**
Evidence: `validate_instrument_key` exists in `heber/models/envelope.py`, but `EventConsumer` does not enforce it. Invalid instrument keys can silently flow into Silver.
Recommendation: Validate in the consumer and attach a quality flag or reject invalid events.
Update 2026-02-07: Remediated in `T-61` by enforcing `EventEnvelope.is_valid_instrument_key()` in `EventConsumer` before Bronze/Silver writes, failing invalid events.
Revalidated 2026-02-07 (Pass 56): Resolved. Invalid instrument keys are now rejected in ingestion and covered by regression tests.

**TD-014: Observability is partially stubbed.**
Evidence: Several modules reference metrics counters but there is no wiring to a metrics server or standard export path. Some modules have metrics placeholders and log-only paths.
Recommendation: Add a consistent metrics export strategy, likely via Prometheus, and register metrics in service entrypoints.
Update 2026-02-07: Remediated in `T-59` by wiring shared metrics recording helpers into consumer event processing (`received/processed/batch/latency`), Silver flush writes (`rows/bytes/duration/error`), and compactor runs (`success/error/files/bytes/duration`) while retaining exporter startup via service entrypoints.
Revalidated 2026-02-07 (Pass 54): Resolved. Primary ingestion and storage services now emit concrete runtime metrics with regression tests for instrumentation paths.

**TD-015: Claimed pending messages are not yielded to consumers (possible silent drops).**
Evidence: `heber/bus/__init__.py` calls `_claim_idle_messages()` inside `RedisEventBus.consume()` but discards the returned messages. Claimed messages are never processed, yet are removed from other consumers.
Recommendation: Yield claimed messages in the consume loop or merge them into the next batch. Add an integration test that simulates XPENDING/XCLAIM behavior.

**TD-016: Meta-label dataset builder expects columns not produced by the label writer.**
Evidence: `heber/ml/datasets.py` expects `outcome` and `hit_tp_first`, but `heber/watch/checker.py`/`outcome_to_label_row()` writes `outcome_reason` and `contract_hit_tp_first`. Joins and labeling will be empty or incorrect.
Recommendation: Align the label writer output with the dataset builder expectations (or vice versa) and add a small end-to-end test from watch outcome → dataset build.

**TD-017: Watch service uses synchronous Redis calls inside async loops.**
Evidence: `heber/watch/consumer.py` and `heber/watch/manager.py` use sync `redis` clients inside `async def` loops. This can block the event loop and stall poller/checker tasks.
Recommendation: Switch to `redis.asyncio` for the watch service or run sync calls in a thread executor. Add backpressure metrics for watch processing latency.

**TD-018: Watch consumer acknowledges messages even when processing fails; no DLQ.**
Evidence: `heber/watch/consumer.py` unconditionally XACKs each message after `_process_alert`, even if parsing/processing failed. Failed alerts are silently dropped and no DLQ exists for the watch stream.
Recommendation: Only ack on success, and add a DLQ stream for watch failures with retry/backoff similar to `heber/bus/backpressure.py`.

**TD-019: Time-of-day features are computed without timezone conversion.**
Evidence: `heber/watch/features.py` uses `alert.ts_event.hour` and assumes ET market hours. If `ts_event` is UTC (likely), time features are wrong.
Recommendation: Normalize timestamps to a market timezone before feature extraction (e.g., convert UTC to US/Eastern) and document the assumption.
Update 2026-02-07: Remediated in `T-62` by normalizing alert timestamps to `America/New_York` before computing hour/minute/day and market-open/close timing features, with naive timestamps treated as UTC.
Revalidated 2026-02-07 (Pass 57): Resolved. Watch timing features now use market-timezone normalized timestamps with regression coverage.

**TD-020: Data Gateway endpoint paths are inconsistent.**
Evidence: `SnapshotPoller` uses `/api/v1/alpaca/options/quotes`, while feature enrichment uses `/alpaca/...` and `/uw/...` paths. These may not exist on the same gateway.
Recommendation: Centralize Data Gateway base paths and versioning in config and align all callers.
Update 2026-02-07: Remediated in `T-63` by adding shared watch-service Data Gateway URL candidate construction and migrating poller/consumer/features HTTP calls to API-prefix-first routing with legacy fallback.
Revalidated 2026-02-07 (Pass 58): Resolved. Watch modules now use a single endpoint-construction strategy with regression coverage for fallback ordering.

**TD-021: Meta-label builder defaults to `/tmp/heber/gold` instead of configured data root.**
Evidence: `heber/ml/datasets.py` default paths point to `/tmp/heber/gold`, while the system’s data root is `/Volumes/heber/data` via `Settings`.
Recommendation: Use `HEBER_DATA_ROOT` or `settings.gold_path` and ensure the builder follows environment configuration.
Update 2026-02-07: Remediated in `T-64` by switching default outcomes/features paths to `settings.gold_path`-based canonical dataset paths and adding legacy-path fallback support for historical layouts.
Revalidated 2026-02-07 (Pass 59): Resolved. Meta-label dataset reads now default to configured Gold root semantics.

**TD-022: Feature persistence to Gold is not wired; builder expects Parquet that is never written.**
Evidence: Features are stored in Redis (`heber/watch/features.py`) but no job writes features to Gold; `MetaLabelDatasetBuilder` expects Parquet under `meta_labels/features`.
Recommendation: Add a persistence step that writes features to Gold (or update the builder to read from Redis + outcomes).
Update 2026-02-07: Remediated in `T-64` by persisting extracted watch features into Gold date partitions during alert processing and adding append-safe partition writes in dataset persistence helpers.
Revalidated 2026-02-07 (Pass 59): Resolved. Feature rows now land in Gold partitions consumed by `MetaLabelDatasetBuilder`.

**TD-023: Inference feature ordering is not tied to training feature order.**
Evidence: Training uses `MetaLabelDatasetBuilder.get_feature_columns()` (dataframe column order), while inference uses `AlertFeatures.numeric_feature_names()` (fixed order). These can diverge.
Recommendation: Persist the feature name order with the model (e.g., in model metadata) and enforce the same order at inference.
Update 2026-02-07: Remediated in `T-65` by storing training feature names in model config artifacts and applying that saved order during inference feature-vector construction.
Revalidated 2026-02-07 (Pass 60): Resolved. Inference now uses persisted training feature order when available, with regression tests covering save/load and scoring behavior.

**TD-024: Soda scanner default Silver path is likely wrong.**
Evidence: `SodaConfig.silver_path` defaults to `/Volumes/heber/silver` but the data root is `/Volumes/heber/data/silver`.
Recommendation: Default to `settings.silver_path` or set `HEBER_SILVER_PATH` in `.env.example`.
Update 2026-02-07: Remediated in `T-66` by defaulting `SodaConfig.silver_path` and `from_env()` fallback to `settings.silver_path` while preserving `HEBER_SILVER_PATH` override behavior.
Revalidated 2026-02-07 (Pass 61): Resolved. Soda scanner defaults now align with configured Silver root semantics (`/Volumes/heber/data/silver` by default).

**TD-025: Non-null rate uses a hard-coded 0.99 threshold for per-column reporting.**
Evidence: `DataQualityValidator.check_non_null_rate()` flags columns below 0.99 regardless of the contract threshold. This can produce false violations when contract thresholds differ.
Recommendation: Use the contract threshold when identifying columns below threshold.
Update 2026-02-07: Remediated in `T-66` by adding threshold-aware non-null column classification and passing each contract threshold through validation paths.
Revalidated 2026-02-07 (Pass 61): Resolved. Per-column completeness reporting now respects contract thresholds and no longer uses a hard-coded 0.99 cutoff.

**TD-026: `tests_framework.py` references a missing method.**
Evidence: `TestE2ETestSuite.test_get_schedule()` calls `E2ETestSuite.get_schedule()` but the method does not exist in `heber/testing/framework.py`.
Recommendation: Implement `get_schedule()` or remove the test to keep test suites consistent.
Update 2026-02-07: Remediated in `T-67` by revalidating `E2ETestSuite.get_schedule()` in framework tests and adding explicit regression coverage under `tests/test_testing_environment_defaults.py`.
Revalidated 2026-02-07 (Pass 62): Resolved. Schedule API exists and is now directly guarded by regression tests.

**TD-027: Testing environment defaults diverge from docker-compose.**
Evidence: `heber/testing/environments.py` defines local services on ports 5432/6379, while `docker-compose.yml` uses 5433/6380. This causes confusion in local testing guidance.
Recommendation: Align testing defaults with compose ports or document the difference.
Update 2026-02-07: Remediated in `T-67` by aligning `DEFAULT_LOCAL_SERVICES` host mappings to `5433:5432` (Postgres) and `6380:6379` (Redis), plus regression coverage for environment defaults.
Revalidated 2026-02-07 (Pass 62): Resolved. Testing environment defaults now match compose host-port conventions.

**TD-028: Iceberg partition spec format may not match PyIceberg API.**
Evidence: `create_silver_table()` passes a list to `partition_spec`. PyIceberg typically expects a `PartitionSpec` object; this may break at runtime.
Recommendation: Use `PartitionSpec` builders from PyIceberg and add a smoke test that initializes tables.
Update 2026-02-07: Remediated in `T-68` by introducing `_build_daily_partition_spec()` in `heber/storage/iceberg_catalog.py` and wiring `create_silver_table()` to pass a concrete `PartitionSpec(PartitionField(..., DayTransform(), ...))`.
Revalidated 2026-02-07 (Pass 63): Resolved. Partition spec wiring now follows PyIceberg object-style API expectations.

**TD-029: Quarantine paths read provider/feed from `envelope.meta`.**
Evidence: `heber/bus/backpressure.py` expects `envelope["meta"]["provider"]`/`["feed"]`, but `EventEnvelope` stores `provider` and `feed` at the top level.
Recommendation: Use top-level fields or normalize envelope format before quarantine writes.
Update 2026-02-07: Remediated in `T-69` by resolving quarantine partition keys from top-level `provider`/`feed` first, with compatibility fallback to legacy `meta` fields when needed.
Revalidated 2026-02-07 (Pass 64): Resolved. Quarantine path partitioning now aligns with canonical envelope structure without breaking legacy payloads.

**TD-030: Stream naming diverges across modules and docs.**
Evidence: `heber/bus` uses `stream:*` names, writer/consumer uses `heber:events`, and ops docs reference `stream:market.bars`. This split-brain naming leads to non-wired components.
Recommendation: Standardize on one stream naming convention and update docs, bus config, and consumers together.
Update 2026-02-05: Remediated in `T-20` by standardizing bus and stream registry keys to `heber:events:*` and aligning watch/ops references.

**TD-031: Watch model timestamps use naive `datetime.utcnow()` defaults.**
Evidence: `heber/watch/models.py` sets `created_at` and `updated_at` with `datetime.utcnow()` (naive), while other parts expect timezone-aware UTC.
Recommendation: Use `datetime.now(UTC)` or enforce timezone-aware defaults across models.
Update 2026-02-07: Remediated in `T-60` by switching `AlertWatch` timestamp defaults to `datetime.now(UTC)` and adding regression coverage.
Revalidated 2026-02-07 (Pass 55): Resolved. Default watch timestamps are now UTC-aware.

**TD-032: Watch poller ignores per-horizon intervals.**
Evidence: `SnapshotPoller.run()` uses the minimum interval from `POLL_CONFIG` for all watches, which over-polls swing/LEAP horizons.
Recommendation: Respect per-watch polling intervals or schedule per-horizon polling loops.
Update 2026-02-07: Remediated in `T-60` by introducing per-watch due checks before quote fetches based on each watch horizon interval.
Revalidated 2026-02-07 (Pass 55): Resolved. Poller now skips not-yet-due long-horizon watches.

**TD-033: Feast feature views hardcode paths and schemas that do not match Gold layout or computed columns.**
Evidence: Feature views in `features/feature_views/*` point to `/data/gold/dataset=.../type=...` while the Gold layout is `dataset/project/version/dt`. Several schemas do not match template outputs (e.g., `microstructure.py` view expects `bid_ask_spread_pct` and `quoted_depth_*` while templates output `spread_bps` and `bid_depth`/`ask_depth`; `flow.py` view expects `net_call_premium` but templates output `call_premium_24h`; `volatility.py` view expects `bollinger_upper/lower` but templates output `bb_width_20`). Feast registry paths are also hardcoded to `/data/feast/...`.
Recommendation: Make paths configurable via settings/env, and align feature view schemas to the actual Gold outputs (or update templates/pipelines to produce the expected columns). Add a schema/contract test that validates view fields exist in a sample Gold file.

**TD-034: Feature templates set `ts_available` to the current time.**
Evidence: `compute_momentum_features`, `compute_volatility_features`, `compute_microstructure_features`, `compute_flow_features`, and `compute_relative_features` set `ts_available = pd.Timestamp.now(tz="UTC")`, which is not tied to source data availability.
Recommendation: Derive `ts_available` from input data (e.g., max of input `ts_available` or `ts_event` + processing lag) to preserve point-in-time correctness.

**TD-035: Alert labels pipeline queries bars using raw symbols, not canonical instrument keys.**
Evidence: `AlertLabelsPipeline._load_bars()` passes `instrument_keys=symbols` where symbols are `["AAPL", "SPY", ...]`, but Silver bars use canonical keys like `equity:AAPL`.
Recommendation: Map symbols to canonical instrument keys (prefix with `equity:` or use `HeberClient.resolve_instrument`) before querying Silver.
Update 2026-02-06: Remediated in `T-21` by canonicalizing alert underlyings and normalizing bar `instrument_key` values to `equity:*` keys with legacy fallback filters.

**TD-036: Alert labels pipeline references a non-existent dataset.**
Evidence: `_load_intraday_bars()` queries dataset `bars_5min`, which is not present in Silver schemas or writer outputs.
Recommendation: Either implement `bars_5min` ingestion or use existing bars with a timeframe column/filter.
Update 2026-02-06: Remediated in `T-21` by switching intraday loads to `dataset=\"bars\"` and filtering for 5-minute `timeframe` values.

**TD-037: Intraday label windows are computed in days, not minutes.**
Evidence: In `alert_labels.py`, `ts_available` uses `timedelta(days=max_window_bars // 24)` and SPY-relative returns use `timedelta(days=max_window_bars)`. For intraday horizons (24 five-minute bars), this becomes 1–24 days instead of ~2 hours.
Recommendation: Track bar duration explicitly and compute windows in minutes/hours for intraday labels.
Update 2026-02-06: Remediated in `T-22` by deriving intraday windows from 5-minute bar counts for both `ts_available` and SPY-relative end-time calculations.

**TD-038: Flow feature rolling windows may be incorrect.**
Evidence: `compute_flow_features()` uses `df["premium"].rolling(..., on="ts_event")` on a Series (the `on` parameter is ignored or invalid for Series) and does not normalize `ts_event` to datetime.
Recommendation: Convert `ts_event` to datetime and use DataFrame-level rolling with `on=ts_event`, or set a DatetimeIndex for correct time-window rolling.
Update 2026-02-06: Remediated in `T-23` by normalizing `ts_event` to UTC datetimes before indexing/rolling and adding regression tests that enforce true 24-hour time-window behavior.

**TD-039: Tracing decorator crashes when OpenTelemetry is not installed.**
Evidence: In `heber/ops/tracing.py`, the `traced()` decorator sets `span_kind = SpanKind.INTERNAL` before checking `OTEL_AVAILABLE`. When OpenTelemetry is missing, `SpanKind` is undefined and any call to a `@traced` function raises `NameError`, despite the `_NoopTracer` fallback.
Recommendation: Guard `SpanKind` usage behind `OTEL_AVAILABLE` and default to `None` for noop tracing, or define a safe fallback enum when OpenTelemetry is not installed.
Revalidated 2026-02-06 (Pass 16): Still open. `traced()` still initializes `span_kind` with `SpanKind` before the OpenTelemetry availability guard.
Update 2026-02-06: Remediated in `T-33` by deferring `SpanKind` assignment until after `OTEL_AVAILABLE` checks and defaulting to `None` in no-OpenTelemetry mode.
Revalidated 2026-02-06 (Pass 36): Resolved. `@traced` functions execute safely when OpenTelemetry is unavailable.

**TD-040: Async shutdown wait can hang if shutdown is signaled early.**
Evidence: `LifecycleManager.initiate_shutdown()` sets `_async_shutdown_event` only if it already exists. If shutdown happens before `async_wait_for_shutdown()` is called, a new event is created and awaited forever even though shutdown already occurred.
Recommendation: In `async_wait_for_shutdown()`, return immediately if `self._shutdown_event.is_set()` or create `_async_shutdown_event` and set it when shutdown is already in progress.
Update 2026-02-06: Remediated in `T-24` by returning immediately when shutdown is already signaled and by setting newly created async shutdown events if a shutdown race occurred before creation.

**TD-041: Shutdown timeouts are logged but still reported as success.**
Evidence: `execute_shutdown()` logs `drain_timeout` when the deadline passes but still increments `shutdown_completed` with status `success` and returns True.
Recommendation: Increment `shutdown_completed` with `status="timeout"` and return False (or a distinct status) when draining exceeds the configured deadline.
Update 2026-02-06: Remediated in `T-25` by reporting timeout status in metrics/logging and returning `False` when in-flight draining exceeds the shutdown deadline.

**TD-042: `configure_logging()` accepts a log level but does not apply it.**
Evidence: `configure_logging()` has a `log_level` argument but does not set stdlib logging levels or apply it to structlog. This results in no effective filtering.
Recommendation: Wire log level into Python `logging` configuration (or structlog filtering) and document expected values.
Revalidated 2026-02-06 (Pass 13): Still open. `configure_logging()` continues to ignore `log_level` and uses `PrintLoggerFactory` without level filtering.
Revalidated 2026-02-06 (Pass 18): Still open. `configure_logging()` still accepts but does not consume `log_level` in logger/filter configuration.
Update 2026-02-06: Remediated in `T-26` by validating/normalizing `log_level`, applying root logger level configuration, and using structlog filtering (`make_filtering_bound_logger`) with regression tests for INFO/DEBUG behavior in JSON and console output modes.
Revalidated 2026-02-06 (Pass 32): Resolved. `configure_logging(log_level=...)` now enforces effective filtering and fails fast on invalid level names.

**TD-043: Bloom filter deduplication has no TTL/rotation.**
Evidence: `EventDeduplicator` uses a Bloom filter that grows in false-positive rate over time. When no backing store is configured, Bloom matches are treated as hard duplicates, which will drop valid events increasingly as the filter saturates.
Recommendation: Add time-based rotation (rolling Bloom filters), a TTL backing store, or a periodic reset strategy. If no backing store is configured, consider treating Bloom matches as “suspect” instead of hard duplicates.
Revalidated 2026-02-06 (Pass 13): Still open. `EventDeduplicator` does not rotate/reset Bloom state and has no default persistent backing store implementation.
Revalidated 2026-02-06 (Pass 18): Still open. Reliability module still has unbounded in-memory Bloom lifetime with hard-drop behavior in no-backing-store mode.
Update 2026-02-06: Remediated in `T-27` by adding rolling Bloom-filter rotation with bounded memory windows, rotation telemetry, and regression tests validating duplicate detection before and after rotation boundaries.
Revalidated 2026-02-06 (Pass 33): Resolved. No-backing-store mode now bounds hard-drop risk to the active/previous rotation windows instead of an unbounded process lifetime.

**TD-044: In-memory DLQ is non-persistent.**
Evidence: `DeadLetterQueue` stores failed events in a process-local list. On restart, all queued failures are lost, and there is no disk or stream persistence.
Recommendation: Back the DLQ with Redis/stream storage or write to disk and implement a replay path.
Update 2026-02-07: Remediated in `T-55` by adding persisted DLQ storage with on-startup reload and queue mutation persistence on add/retry/pop paths.
Revalidated 2026-02-07 (Pass 50): Resolved. Failed events now survive process restarts and remain replayable.

**TD-045: SCD join expects suffixed validity columns that may not exist.**
Evidence: `join_with_reference_asof()` always filters on `valid_from{suffix}`/`valid_to{suffix}`. Polars only applies suffixes on name collisions; if the left table does not have `valid_from` or `valid_to`, the reference columns are unsuffixed and the filter will fail with missing columns.
Recommendation: Normalize reference validity columns before the join (e.g., rename to fixed names) or detect whether suffixing occurred and use the correct column names.
Update 2026-02-07: Remediated in `T-56` by resolving validity columns from available joined schema names (`valid_from_ref`/`valid_to_ref` or unsuffixed fallback) before applying as-of filters.
Revalidated 2026-02-07 (Pass 51): Resolved. SCD joins now work whether Polars suffixes reference validity columns or not.

**TD-046: Gold build validation treats warning-level violations as hard failures.**
Evidence: `validate_gold_build()` labels the `max_ts_event_used > max_ts_available_used` check as a warning, but appends it to `violations` and raises when `strict=True`.
Recommendation: Separate warnings from hard violations or only raise for the strict gates.
Update 2026-02-07: Remediated in `T-56` by separating hard leakage violations from warning messages and raising in strict mode only when hard gates fail.
Revalidated 2026-02-07 (Pass 51): Resolved. Warning-only metadata inconsistencies no longer fail strict validation.

**TD-047: `lineage` type mismatch between models and Arrow schema.**
Evidence: `SilverBase.lineage` is `dict[str, Any] | None` but `SILVER_BASE_SCHEMA` defines `lineage` as `pa.string()` with “JSON serialized” comment. This mismatch creates inconsistent serialization expectations across ingestion and writing.
Recommendation: Standardize lineage as a structured type (e.g., JSON/struct) and enforce serialization in one place, or update the Pydantic model to store serialized JSON consistently.
Update 2026-02-07: Remediated in `T-57` by normalizing dict lineage values to deterministic JSON strings (`sort_keys=True`) during `SilverBase` model validation.
Revalidated 2026-02-07 (Pass 52): Resolved. Model lineage now matches string-backed schema expectations and is covered by regression tests.

**TD-048: `schema_version` defaults to `v1` across all models.**
Evidence: `SilverBase.schema_version` defaults to `v1` even for v2/v3/v4 datasets (news, filings, alternative data). Unless overridden at write time, stored rows will be mislabeled.
Recommendation: Set per-dataset defaults in each model or enforce schema_version injection in the writer based on dataset.
Update 2026-02-07: Remediated in `T-57` by adding release-cohort schema-version defaults in `SilverBase` for v2-v6 dataset families while preserving explicit overrides.
Revalidated 2026-02-07 (Pass 52): Resolved. Default schema versions now match intended dataset release cohorts.

**TD-049: Date fields are inconsistently typed across Silver schemas.**
Evidence: Some models use `date` (e.g., `expiry` in options), while others use `str` for dates (e.g., `expiry` in `MaxPainRecord`, `HottestChainRecord`, `IVTermStructureRecord`). This leads to inconsistent parsing and schema drift across datasets.
Recommendation: Standardize date representation (prefer `date`/`datetime`) and enforce normalization in ingestion.
Update 2026-02-07: Remediated in `T-57` by converting remaining string expiry fields to `date` in Pydantic models and aligning canonical Arrow schemas (`max_pain`, `hottest_chain`, `iv_term_structure`) to `pa.date32()`.
Revalidated 2026-02-07 (Pass 52): Resolved. Date typing is now consistent between model contracts and canonical Arrow schemas.

**TD-050: `read_label()` picks latest version by lexicographic sort.**
Evidence: `read_label()` sorts `version=` directories by name and picks the highest string. Versions like `v1.10.0` will sort before `v1.2.0`, yielding an older dataset as “latest.”
Recommendation: Parse semantic versions or use metadata (created_at) to choose the newest version.
Update 2026-02-07: Remediated in `T-54` by introducing semantic-version-aware latest-version selection in `read_label()` with deterministic fallback behavior.
Revalidated 2026-02-07 (Pass 49): Resolved. Label reads now choose the newest semver release when no explicit version is provided.

**TD-051: Retention scanning does not match Gold layout.**
Evidence: `ReaperWorker.scan_partitions()` expects partitions under `<storage_root>/<layer>/<dataset>/dt=*` and never populates `PartitionInfo.version`. Gold datasets are written as `dataset=.../type=.../version=...` (and labels have no `dt=`), so Gold partitions are never discovered and version pruning cannot work.
Recommendation: Implement Gold-specific scanning that walks `dataset=.../type=.../version=...` and records version identifiers and optional `dt` partitions.
Update 2026-02-07: Remediated in `T-52` by adding Gold-specific scan paths for both `project=*/version=*` and `type=*/version=*` layouts, including label datasets without `dt=` partitions, and by populating `PartitionInfo.version`.
Revalidated 2026-02-07 (Pass 47): Resolved. Gold retention scanning now discovers canonical dataset/version paths and feeds version-aware pruning.

**TD-052: Hot Store and DLQ retention policies are never applied.**
Evidence: `ReaperScheduler._process_dataset()` only evaluates Bronze, Silver, and Gold layers; `HOT_STORE` and `DLQ` are defined but ignored.
Recommendation: Include Hot Store and DLQ layers in the reaper or remove the unused policies to avoid false safety assumptions.
Update 2026-02-07: Remediated in `T-53` by extending scheduler layer processing to include `DataLayer.HOT_STORE` and `DataLayer.DLQ` with explicit per-layer policy fields on dataset retention config.
Revalidated 2026-02-07 (Pass 48): Resolved. Reaper now scans and applies retention policy actions across all defined data layers.

**TD-053: Retention uses hardcoded storage root defaults.**
Evidence: `DEFAULT_STORAGE_ROOT = "/data/heber"` and `create_reaper()` default paths do not reference `Settings` or `HEBER_DATA_ROOT`, while other parts of the system use `/Volumes/heber/data`.
Recommendation: Wire retention defaults to the shared configuration and document expected paths.
Update 2026-02-07: Remediated in `T-53` by resolving default storage root from explicit args, `HEBER_DATA_ROOT`, then shared `settings.data_root`, with archive defaults derived from that resolved root.
Revalidated 2026-02-07 (Pass 48): Resolved. `create_reaper()` / `ReaperWorker` now follow configuration-aligned defaults instead of hardcoded `/data/heber`.

**TD-054: `read_label()` bypasses ts_available guard if column is missing.**
Evidence: `read_label()` only filters by `ts_available` when the column exists. A malformed or externally-written label dataset without `ts_available` will return all rows, including future data.
Recommendation: Require `ts_available` for label datasets (raise or warn) and fail closed in training contexts.
Update 2026-02-07: Remediated in `T-54` by failing closed (`ValueError` by default) when `ts_available` is absent, with an explicit compatibility flag for non-strict callers.
Revalidated 2026-02-07 (Pass 49): Resolved. Point-in-time label reads no longer return unfiltered rows when `ts_available` is missing.

**TD-055: Retention version pruning uses lexicographic ordering.**
Evidence: `find_expired_versions()` sorts version keys as strings. This can delete or keep the wrong versions for semver patterns.
Recommendation: Parse semantic versions or use explicit creation timestamps to decide which versions to retain.
Update 2026-02-07: Remediated in `T-52` by adding semantic-version-aware sort keys (with lexicographic fallback) for Gold version pruning.
Revalidated 2026-02-07 (Pass 47): Resolved. Version pruning now retains newer semver releases correctly (for example `v1.10.0` over `v1.2.0`).

**TD-056: Default Feast repo path is hardcoded.**
Evidence: `DEFAULT_REPO_PATH = "features/"` and the helpers default to that location, ignoring any configured environment or settings for the repo path.
Recommendation: Allow repo path to be set via config/env (e.g., `HEBER_FEAST_REPO_PATH`) and use that as the default.
Update 2026-02-07: Remediated in `T-58` by wiring Feast helper defaults to `settings.feast_repo_path` with support for both `HEBER_FEAST_REPO_PATH` and legacy `FEAST_REPO_PATH`.
Revalidated 2026-02-07 (Pass 53): Resolved. Default repo path is now configuration-driven instead of hardcoded.

**TD-057: Materialization does not report row counts.**
Evidence: `materialize_features()` returns `-1` for each view and does not surface actual row counts, making monitoring or alerting on materialization health impossible.
Recommendation: Capture row counts from Feast logs/metrics or implement a lightweight count query after materialization where feasible.
Update 2026-02-07: Remediated in `T-58` by extracting per-view counts from Feast materialization responses when available and adding a file-source row-count estimation fallback for views lacking direct counts.
Revalidated 2026-02-07 (Pass 53): Resolved. Materialization results no longer rely on `-1` placeholders.

**TD-058: `search_features()` matches tags by key only.**
Evidence: `search_features()` checks `t in view_tags` where `view_tags` is a dict, so it only matches tag keys, not values. This can miss intended matches or produce false positives.
Recommendation: Support key:value tag filters or compare against values explicitly.
Update 2026-02-07: Remediated in `T-58` by adding case-insensitive tag matching for key-only, value-only, and `key:value` filter expressions.
Revalidated 2026-02-07 (Pass 53): Resolved. Feature search now evaluates tag values in addition to keys.

**TD-059: ClickHouse backup script logs S3 bucket/prefix but doesn’t enforce them.**
Evidence: `scripts/backup/clickhouse-backup.sh` defines `S3_BUCKET` and `S3_PREFIX` but never passes them to `clickhouse-backup`. The printed S3 path may not match the actual upload destination, which is controlled by clickhouse-backup’s own config.
Recommendation: Pass bucket/prefix via the clickhouse-backup config/env or remove the misleading output.
Revalidated 2026-02-06 (Pass 15): Still open. Script output advertises `S3_BUCKET/S3_PREFIX`, but backup/upload commands still rely on external clickhouse-backup config only.
Revalidated 2026-02-06 (Pass 19): Still open. Script still only logs bucket/prefix while `create`/`upload` calls do not pass destination overrides.
Revalidated 2026-02-06 (Pass 29): Resolved. Script removed misleading hardcoded S3 path output and now reports config-driven remote backup entry.

**TD-060: Catalog backup validation can leak the test DB instance on failure.**
Evidence: `validate-catalog-backup.sh` uses `set -euo pipefail`, so if restore or validation queries fail, the cleanup section that deletes the test instance is skipped. This can leave `heber-catalog-backup-test` running indefinitely.
Recommendation: Add a `trap` to ensure cleanup on exit and capture/handle validation failures before teardown.
Revalidated 2026-02-06 (Pass 15): Still open. Script still lacks a `trap`/finally cleanup guard around restore and validation steps.
Revalidated 2026-02-06 (Pass 19): Still open. Cleanup still only runs on success path; no guaranteed teardown trap exists.
Revalidated 2026-02-06 (Pass 28): Resolved. Script now uses `EXIT` trap cleanup and preserves failure status while tearing down test instances.

**TD-061: Volume init script assumes macOS tooling.**
Evidence: `scripts/init_volume.sh` always executes `dot_clean` for multiple directories without checking platform/tool availability. On non-macOS hosts the cleanup is effectively skipped with shell errors suppressed by `|| true`, and there is no explicit cross-platform branch.
Recommendation: Guard `dot_clean` behind an OS/tool check or provide a no-op fallback for non-macOS hosts.
Revalidated 2026-02-06 (Pass 16): Still open. Script still runs `dot_clean` unconditionally and relies on `|| true` rather than explicit platform detection.
Update 2026-02-06: Remediated in `T-34` by adding explicit host OS and `dot_clean` detection, explicit skip messaging, and non-fatal per-directory cleanup handling without implicit shell fallback.
Revalidated 2026-02-06 (Pass 37): Resolved. macOS cleanup is now guarded explicitly, and non-macOS hosts skip cleanly with clear logs.

**TD-062: Labeling docs reference outdated API location/signature.**
Evidence: `docs/labeling_strategy.md` points to `heber/firewall/splits.py` and shows a `validate_train_test_split` signature that does not exist; the current function lives in `heber/firewall/validation.py` with different parameters.
Recommendation: Update the docs to match the current module path and function signature.
Revalidated 2026-02-06 (Pass 16): Still open. The snippet still points to `heber/firewall/splits.py` with stale parameter names.
Revalidated 2026-02-06 (Pass 19): Still open. Train/test split snippet still references the stale module path and signature.
Revalidated 2026-02-06 (Pass 30): Resolved. Snippet now references `heber/firewall/validation.py` with current `purge_window`/`embargo_window` parameters.

**TD-063: Data contract docs drift from current schema sources and concrete Gold partition path conventions.**
Evidence: `docs/data_contract.md` still lists `heber/writer/silver.py` as the Silver schema source, while canonical Arrow schemas are now defined in `heber/schemas/silver.py`. It also documents Gold partitioning in abstract (`dataset/project/version/dt`) without the key-value path convention used by writers (`dataset=.../project=.../version=.../dt=...`), which creates avoidable interpretation drift.
Recommendation: Update `docs/data_contract.md` to reference `heber/schemas/silver.py` as the canonical schema source and show concrete key-value Gold path examples that match `write_gold()` / label-writer output.
Revalidated 2026-02-06 (Pass 16): Still open. Source-module references and Gold path notation remain partially stale.
Revalidated 2026-02-06 (Pass 19): Still open. Schema source reference and Gold path notation remain unaligned with current implementation conventions.
Revalidated 2026-02-06 (Pass 30): Resolved. Doc now references `heber/schemas/silver.py` and concrete Gold key-value path conventions.

**TD-064: UW endpoint coverage summary conflicts with its own tables.**
Evidence: `docs/UW_endpoints.md` summary section still reports “Complete (11)”, “In Progress (8)”, and “Not Started (~80+)”, but the endpoint tables above are overwhelmingly marked ✅. The summary buckets are not synchronized with table statuses.
Recommendation: Derive summary counts from the table data (or remove manual totals/status buckets) to avoid recurrent drift.
Revalidated 2026-02-06 (Pass 18): Still open. Summary totals and status buckets still conflict with table-level status rows.
Revalidated 2026-02-06 (Pass 31): Resolved. Summary now reflects current table-derived status counts.

**TD-065: Security scan doesn’t fail on filesystem findings.**
Evidence: `scripts/security-scan.sh` runs `trivy fs` without `--exit-code`, so secrets/misconfig findings do not fail the script.
Recommendation: Add `--exit-code 1` and optionally `--severity` to make failures actionable in CI.
Revalidated 2026-02-06 (Pass 15): Still open. Image scan uses `--exit-code`, but `trivy fs` invocation still omits it.
Revalidated 2026-02-06 (Pass 19): Still open. Filesystem scan path still omits `--exit-code`, so high/critical findings will not block execution.
Revalidated 2026-02-06 (Pass 27): Resolved. `trivy fs` now uses `--exit-code 1` with explicit failure handling.

**TD-066: lakeFS repo creation hardcodes the storage namespace.**
Evidence: `LakeFSVersionManager._get_repo()` always creates repositories with `storage_namespace="s3://heber-lakehouse/{repo}"`, ignoring environment or configuration (e.g., MinIO, different bucket, or lakeFS defaults).
Recommendation: Add a configurable storage namespace (e.g., `LAKEFS_STORAGE_NAMESPACE`) and use it when creating repositories.
Revalidated 2026-02-06 (Pass 14): Still open. Repository creation path still hardcodes `s3://heber-lakehouse/{repo}` and `LakeFSConfig` has no storage namespace field.
Revalidated 2026-02-06 (Pass 17): Still open. Version manager continues to hardcode `storage_namespace` and lacks a configurable namespace field in `LakeFSConfig`.
Update 2026-02-06: Remediated in `T-28` by adding `LAKEFS_STORAGE_NAMESPACE_BASE` and `LAKEFS_STORAGE_NAMESPACE_TEMPLATE` support in `LakeFSConfig` and routing repository creation through config-driven namespace resolution.
Revalidated 2026-02-06 (Pass 34): Resolved. Repository creation no longer hardcodes `s3://heber-lakehouse/{repo}` and now respects configuration for environment-specific namespaces.

**TD-067: lakeFS metrics coverage is incomplete.**
Evidence: Metrics are emitted for `create_branch` and `commit`, but not for `create_tag`, `list_tags`, `diff`, or `merge` error paths. This makes operational monitoring partial and inconsistent.
Recommendation: Instrument all lakeFS operations (success/failure/duration) consistently.
Revalidated 2026-02-06 (Pass 17): Still open. `lakefs_operations`/`lakefs_operation_duration` remain wired only for `create_branch` and `commit`.
Update 2026-02-06: Remediated in `T-45` by adding consistent operation metrics for `create_tag`, `list_tags`, `merge`, and `diff`, including repository-resolution failure paths, with dedicated regression coverage.
Revalidated 2026-02-06 (Pass 40): Resolved. The remaining lakeFS operations now emit success/error counters and duration histograms consistently.

**TD-068: Market calendar crashes on naive datetimes.**
Evidence: `MarketCalendar` calls `pd.Timestamp(dt).tz_convert(ET)` in multiple methods. If `dt` is naive (no timezone), pandas raises. Callers may pass naive datetimes (common in this repo).
Recommendation: Normalize inputs by assuming UTC when tzinfo is missing (or require tz-aware inputs and validate early with a clear error).
Revalidated 2026-02-06 (Pass 22): Still open. Converting naive timestamps still raises `TypeError` (`tz-naive Timestamp`).
Revalidated 2026-02-06 (Pass 23): Resolved. Calendar methods now normalize inputs to UTC and accept naive `datetime`/`pd.Timestamp` values.

**TD-069: `include_extended` flag is unused.**
Evidence: `MarketCalendar.include_extended` is stored but never used to expand the trading session to include pre/post-market. Methods always rely on the default exchange calendar schedule.
Recommendation: Either wire in extended hours support or remove the flag to avoid misleading behavior.
Revalidated 2026-02-06 (Pass 22): Still open. `include_extended` appears in constructor/docs state only and is not used by session logic.
Revalidated 2026-02-06 (Pass 25): Resolved. `include_extended=True` now raises a clear `NotImplementedError`.

**TD-070: Hot Store DDL omits some base columns.**
Evidence: `heber/hotstore/tables.py` defines Hot Store tables without `quality_flags` or `lineage` columns that exist in Silver base schema. This prevents storing provenance/quality flags in Hot Store and creates schema drift.
Recommendation: Decide which base columns must be preserved in Hot Store and add them (or document the intentional omission).
Revalidated 2026-02-06 (Pass 22): Still open. Current DDL still omits `quality_flags` and `lineage`.
Revalidated 2026-02-06 (Pass 26): Resolved. Hot Store DDL and sync inserts now include `quality_flags` and `lineage`.

**TD-071: Hot Store DDL creation assumes async client.**
Evidence: The original table-bootstrap helper awaited `client.execute(...)` in a path used with the synchronous `clickhouse_connect` client, so client-mode mismatches could fail at runtime.
Recommendation: Provide separate sync/async helpers or normalize on a single client and call pattern.
Revalidated 2026-02-06 (Pass 22): Resolved. `create_all_tables()` now supports sync clients and `create_all_tables_async()` handles awaitable execution.
Revalidated 2026-02-07 (Pass 70): Resolved. Sync helper now fails fast when it receives an awaitable execute result (without leaking un-awaited coroutine warnings), and regression tests cover both sync misuse and async execution paths.

**TD-072: Additional schema tests hardcode the schema count.**
Evidence: `tests_additional.py` asserts `len(schemas) == 16`. As new schemas are added, the test will fail even if behavior is correct.
Recommendation: Assert on minimum required schemas or specific known names rather than total count.
Revalidated 2026-02-06 (Pass 22): Still open. Test continues to assert an exact schema count.
Revalidated 2026-02-06 (Pass 24): Resolved. Tests now assert required schema contracts and registry lookup behavior.

**TD-073: Terraform root-module wiring can drift from local module output contracts.**
Evidence: `infrastructure/terraform/main.tf` now references local modules that exist under `infrastructure/terraform/modules/*`. The remaining debt risk is root-module references drifting from the outputs each local module actually exports.
Recommendation: Keep static contract tests that validate both module source-path existence and root `module.<name>.<output>` references. Add `terraform validate` in CI when Terraform CLI is available.
Revalidated 2026-02-07 (Pass 66): Resolved. Local module paths are present and regression tests now cover root-to-module output wiring contracts (`tests/test_terraform_module_sources.py`, `tests/test_terraform_root_module_contract.py`).

**TD-074: Kubernetes deployments reference non-existent module entrypoints.**
Evidence: `k8s/base/deployments/consumer.yaml` runs `python -m heber.bus.consumer`, and writer/compactor run `heber.writer.service` and `heber.writer.compaction`. These module paths do not exist in the repo (writer is `consumer.py`/`compactor.py`).
Recommendation: Update commands to valid module paths (e.g., `heber.writer.consumer`, `heber.writer.compactor`) and verify entrypoints.
Update 2026-02-07: Revalidated and expanded in `T-73` by extending runtime-entrypoint regression coverage to all base deployments (`catalog`, `consumer`, `writer`, `compactor`, `hotloader`, `backfill`) and asserting referenced Python modules remain importable.
Revalidated 2026-02-07 (Pass 69): Resolved. Base deployment commands no longer reference legacy missing modules and command-module mappings remain covered by regression tests.

**TD-075: HPA targets custom metrics that are not exported.**
Evidence: HPAs reference `heber_consumer_lag_seconds`, `heber_writer_pending_batch_rows`, and `heber_catalog_request_latency_p99_seconds`. Only `heber_consumer_lag_seconds` exists in `ops/metrics.py`, and the other two metrics are not defined.
Recommendation: Export the needed metrics or change the HPA configuration to CPU/memory scaling or existing metrics.
Revalidated 2026-02-06 (Pass 14): Still open. HPA manifests still reference missing `heber_writer_pending_batch_rows` and `heber_catalog_request_latency_p99_seconds` metrics.
Revalidated 2026-02-06 (Pass 17): Still open. Metrics module still does not define `heber_writer_pending_batch_rows` or `heber_catalog_request_latency_p99_seconds`.
Revalidated 2026-02-06 (Pass 21): Still open. HPA manifests continue to reference unavailable writer/catalog custom metrics.
Update 2026-02-06: Remediated in `T-29` by replacing custom pod-metric targets with built-in CPU/memory resource metrics across catalog/consumer/writer HPAs and adding regression checks for metric-type drift.
Revalidated 2026-02-06 (Pass 35): Resolved. HPA manifests no longer depend on missing custom metrics.

**TD-076: Probes target endpoints that are not implemented.**
Evidence: Deployments probe `/health` and `/ready` on the metrics port for consumer/writer/compactor/hotloader. Those services do not expose HTTP health endpoints in the codebase.
Recommendation: Add health endpoints or update probes to use a TCP or exec check, or to an actual HTTP server if one exists.
Revalidated 2026-02-06 (Pass 14): Still open. Writer/consumer/compactor/hotloader processes still run non-HTTP module entrypoints while deployments continue probing HTTP `/health` and `/ready` on metrics ports.
Revalidated 2026-02-06 (Pass 17): Still open. Catalog exposes `/health`, but worker modules still do not run HTTP health servers on probed ports.
Revalidated 2026-02-06 (Pass 21): Still open. Consumer/writer deployments still probe HTTP health endpoints on a metrics port with no health server process.
Update 2026-02-06: Remediated in `T-29` by switching worker liveness/readiness probes to exec checks that validate the expected process entrypoint in `/proc/1/cmdline`, with regression coverage to prevent HTTP-probe drift.
Revalidated 2026-02-06 (Pass 35): Resolved. Worker manifests no longer probe non-existent HTTP health endpoints.

**TD-077: Overlay image-tag overrides can fail when image transformer names do not match rewritten base image names.**
Evidence: Base kustomization rewrites `name: heber` to `ghcr.io/jacobmcmillan/heber`, while overlays also targeted `name: heber`. `kubectl kustomize` output showed `prod` and `staging` images still rendered as `ghcr.io/jacobmcmillan/heber:latest` instead of overlay tags.
Recommendation: Ensure overlay image transformers target the already rewritten base image name, and add regression coverage that validates rendered images for each overlay.
Update 2026-02-07: Remediated in `T-71` by updating overlay image rules to `name: ghcr.io/jacobmcmillan/heber` (`dev`, `staging`, `prod`) and adding static/rendered conformance checks in `tests/test_k8s_kustomize_image_tags.py`.
Revalidated 2026-02-07 (Pass 67): Resolved. Rendered overlays now preserve env-specific tags (`latest`, `staging`, `v1.0.0`) and no longer collapse to base `latest`.

**TD-078: Namespace-scoped runtime prerequisites can drift across overlays.**
Evidence: Deployments reference `serviceAccountName: heber` and `secretRef: heber-secrets`, but base resources previously omitted `ServiceAccount` and External Secrets manifests from kustomize resources, so overlay renders did not include those prerequisites.
Recommendation: Keep prerequisite resources in base kustomization and guard with rendered-overlay regression tests.
Update 2026-02-07: Remediated in `T-72` by adding `serviceaccount.yaml`, `secrets/cluster-secret-store.yaml`, and `secrets/external-secret.yaml` to base kustomize resources, plus rendered-overlay prerequisite checks in `tests/test_k8s_namespace_prerequisites.py`.
Revalidated 2026-02-07 (Pass 68): Resolved. `kubectl kustomize` overlay renders now include `ServiceAccount/ExternalSecret/ClusterSecretStore` resources aligned with deployment `serviceAccountName` + `secretRef` usage.

**TD-079: Terraform environment settings are hardcoded.**
Evidence: Each env `main.tf` pins `region = "us-east-1"` and backend config is fixed. This makes multi-region deployment or account reuse harder.
Recommendation: Parameterize region and backend settings via variables or separate workspace configs.
Update 2026-02-06: Remediated in `T-46` by switching env modules to `var.aws_region`, making backend blocks partial, and moving per-env backend defaults into `backend.hcl` files without hardcoded region keys.
Revalidated 2026-02-06 (Pass 41): Resolved. Environment configs now support region/backend override workflows without changing source manifests.

**TD-080: Backfill does not update Bronze or Catalog metadata.**
Evidence: `BackfillWriter.write_batch()` writes only to Silver temp partitions and logs that compactor will merge. It does not write Bronze, nor does it update catalog coverage or schema metadata.
Recommendation: Add an explicit Bronze write path (or document why it’s skipped), and update catalog coverage once backfill completes.
Update 2026-02-06: Remediated in `T-47` by adding Bronze raw-write output in `BackfillWriter.write_batch()` and catalog metadata/coverage updates during chunk processing in `BackfillCoordinator`.
Revalidated 2026-02-06 (Pass 42): Resolved. Backfill chunks now produce Bronze artifacts and trigger catalog dataset/coverage updates on successful writes.

**TD-081: Backfill jobs are in-memory only.**
Evidence: `BackfillCoordinator` stores jobs in a process-local dict. On restart, in-flight jobs and progress are lost; the API is described as in-memory only in docs.
Recommendation: Persist backfill state in the catalog DB or Redis and add resume/retry support.
Update 2026-02-06: Remediated in `T-48` by persisting backfill jobs/progress to disk under the storage root and loading them at coordinator startup.
Revalidated 2026-02-06 (Pass 43): Resolved. Restarted coordinators now recover prior jobs and continue incomplete chunks.

**TD-082: Missing `pyarrow` silently drops backfill writes.**
Evidence: `_write_parquet()` catches `ImportError` and logs `pyarrow_not_available` but does not raise, so the backfill job continues and reports progress even though nothing was written.
Recommendation: Fail fast when `pyarrow` is missing, or track a failed write and mark the job as failed.
Update 2026-02-06: Remediated in `T-47` by changing `_write_parquet()` to raise a runtime failure when `pyarrow` is unavailable.
Revalidated 2026-02-06 (Pass 42): Resolved. Missing `pyarrow` now fails the write path instead of silently reporting success.

**TD-083: Gap detection assumes a storage layout that may not exist.**
Evidence: `GapDetector.detect_gaps()` reads `silver/{provider}_{feed}/dt=*`, while other components use feed/instrument_type/dt or dataset-based layouts. This can incorrectly report full gaps.
Recommendation: Align gap detection with actual Silver partition layout and/or use the Catalog to discover coverage.
Update 2026-02-06: Remediated in `T-49` by scanning both legacy provider-feed paths and canonical `feed=.../instrument_type=...` Silver partition trees for `dt=*` directories.
Revalidated 2026-02-06 (Pass 44): Resolved. Gap detection now correctly recognizes available dates across both supported Silver layouts.

**TD-084: Backtest labels use `read_gold()` without a version.**
Evidence: `BacktestDataLoader` passes `label_dataset` into `read_gold()` without specifying `version`. If the label dataset is versioned, this may read an unintended or incompatible version.
Recommendation: Add a label version parameter (or reuse `label_version`) and pass it to `read_gold()`.
Update 2026-02-07: Remediated in `T-50` by adding `label_version` support to `BacktestDataLoader` and wiring it into label `read_gold()` calls.
Revalidated 2026-02-07 (Pass 45): Resolved. Backtest label reads now pass explicit version pinning (default `"latest"`).

**TD-085: Backtest reproducibility omits data as-of cutoffs.**
Evidence: `ExperimentConfig` and results capture dataset names and versions but do not persist the as-of timestamp used for feature/label reads, which is critical for reproducibility.
Recommendation: Record `asof_time` per split or overall experiment in the config/results metadata.
Update 2026-02-07: Remediated in `T-51` by adding feature/label as-of metadata to experiment config/checklists/results and per-fold as-of timestamp support in experiment tracking logs.
Revalidated 2026-02-07 (Pass 46): Resolved. Backtest artifacts now include reproducibility-critical as-of timestamps.

**TD-086: Backfill deployment entrypoint is not executable.**
Evidence: `k8s/base/deployments/backfill.yaml` runs `python -m heber.backfill`, but `heber/backfill/` has no `__main__.py`. Running the command locally returns: `No module named heber.backfill.__main__; 'heber.backfill' is a package and cannot be directly executed`.
Recommendation: Add a concrete executable backfill entrypoint (e.g., `heber.backfill.main`) and update the deployment command to that module; then align probes with the actual service mode.
Revalidated 2026-02-06 (Pass 20): Still open. Deployment command is unchanged and module execution still fails with missing `__main__`.
Update 2026-02-06: Remediated in `T-37` by adding `heber/backfill/__main__.py` with a runnable FastAPI service entrypoint (including `/health` and `/ready`) for `python -m heber.backfill`.
Revalidated 2026-02-06 (Pass 38): Resolved. Backfill deployment module now executes in service mode instead of failing at startup.

**TD-087: Hotloader deployment command exits immediately.**
Evidence: `k8s/base/deployments/hotloader.yaml` runs `python -m heber.writer.hotstore`, but `heber/writer/hotstore.py` is a compatibility re-export with no `main()` loop. Executing it exits immediately, so pods will churn under restart policy.
Recommendation: Add a real hotloader service entrypoint (e.g., sync loop wrapper around `HotStoreSync.run_sync_loop`) and point deployment command/probes to that runtime.
Revalidated 2026-02-06 (Pass 20): Still open. Deployment still invokes facade module, and `python -m heber.writer.hotstore` still exits immediately.
Update 2026-02-06: Remediated in `T-38` by adding a real CLI runtime in `heber.writer.hotstore` that executes a continuous sync loop (plus `--once` mode), instead of exiting after import/re-export.
Revalidated 2026-02-06 (Pass 38): Resolved. `python -m heber.writer.hotstore` now runs as a service entrypoint with sync-loop behavior.

**TD-088: Prometheus scrape annotations/ports are not backed by running exporters.**
Evidence: Deployments annotate `prometheus.io/scrape: "true"` with `prometheus.io/port: "9090"` (catalog/consumer/writer and other workers), but runtime entrypoints do not call `start_metrics_server()` from `heber.ops.metrics`. Catalog runs only Uvicorn on 8080, and worker modules run non-HTTP loops without starting a Prometheus HTTP endpoint.
Recommendation: Start a metrics server on the advertised port in each service entrypoint (or remove/adjust scrape annotations/ports to match reality), and add an integration check that verifies `/metrics` reachability per deployment.
Update 2026-02-06: Remediated in `T-40` by wiring `start_metrics_server_from_env` into catalog lifecycle and worker entrypoints (`consumer`, `compactor`, `hotloader`, `backfill`) that are annotated for scrape.
Revalidated 2026-02-06 (Pass 39): Resolved. Scrape-annotated deployments now map to metrics-enabled runtime entrypoints, with regression tests guarding deployment-to-entrypoint alignment.

**TD-089: PostgreSQL readiness probe uses SQLAlchemy 1.x execution pattern.**
Evidence: `create_postgres_check()` called `conn.execute("SELECT 1")`. With SQLAlchemy 2.x (the repo baseline), executing a raw SQL string raises `ObjectNotExecutableError`, causing the dependency check to report `ERROR` even with a healthy database.
Recommendation: Execute a SQLAlchemy `text()` statement (`conn.execute(text("SELECT 1"))`) and keep regression coverage for healthy and failing connection paths.
Update 2026-02-07: Remediated in `T-75` by switching readiness SQL execution to `text("SELECT 1")` and adding targeted regression tests for success/failure paths.
Revalidated 2026-02-07 (Pass 71): Resolved. PostgreSQL checks now succeed with SQLAlchemy 2.x-compatible execution semantics.

**TD-090: WatchManager does not normalize Redis byte IDs.**
Evidence: `WatchManager.get_active_watches()` / `get_watches_for_symbol()` iterate `smembers(...)` results and pass member IDs directly into `get_watch()`. With `redis.from_url(...)` default response decoding, set members are returned as `bytes`, which produced malformed watch keys (`b'...'`) and lookup misses.
Recommendation: Normalize watch IDs from Redis set membership (`bytes` -> UTF-8 `str`) before key construction, and add regression coverage for byte-response Redis clients.
Update 2026-02-07: Remediated in `T-76` by normalizing Redis IDs in `WatchManager` and adding regression tests covering active/symbol lookups with byte-set responses.
Revalidated 2026-02-07 (Pass 72): Resolved. Active watch and symbol index queries now remain correct under byte-decoding Redis clients.

**TD-091: Zero-valued quote handling is filtered out by truthiness checks.**
Evidence: `SnapshotPoller._create_snapshot()` and `BarrierChecker.check_watch()` previously used truthy checks for prices (`if bid and ask`, `if mid`, `if snap.mid_px`). Valid market values of `0.0` were treated as missing, so return paths could drop `-100%` values and fail to classify stop-loss outcomes correctly.
Recommendation: Use explicit `is not None` checks for price presence and add regression tests for zero-price quote/snapshot paths.
Update 2026-02-07: Remediated in `T-77` by switching to explicit `None` checks in poller/checker return logic and adding focused regression coverage for zero-price SL classification and snapshot return computation.
Revalidated 2026-02-07 (Pass 73): Resolved. Zero-valued prices now propagate through return computations and barrier detection as expected.

**TD-092: Label writer parquet filenames can collide within the same second.**
Evidence: `LabelWriter._write_to_parquet()` used `part-%Y%m%d%H%M%S.parquet`. Multiple flushes targeting the same partition in one second produced the same filename and overwrote earlier output.
Recommendation: Make output file names collision-safe (for example, microseconds plus unique suffix) and add a regression test that forces same-second writes.
Update 2026-02-07: Remediated in `T-78` by adding a unique suffix to parquet part filenames and adding a same-second collision regression test.
Revalidated 2026-02-07 (Pass 74): Resolved. Repeated flushes within a single second no longer overwrite prior partition files.

## Suggested Remediation Plan

Phase 1 (Stabilize correctness, 1-2 days):
- Fix TD-001, TD-002, TD-003, TD-005, TD-015, TD-016, TD-033, TD-034, TD-039.
- Add minimal regression tests for Silver flush and SDK default URL.

Phase 2 (Operational reliability, 2-4 days):
- Fix TD-006, TD-007, TD-008, TD-009, TD-011, TD-030, TD-035..TD-038, TD-040..TD-043, TD-066, TD-071, TD-075, TD-076, TD-086, TD-087, TD-088, TD-089, TD-090, TD-091, TD-092.
- Add a DLQ stream and pending-entries recovery policy.

Phase 3 (Performance and maintainability, 3-7 days):
- Continue periodic conformance re-audits for resolved items (including `TD-071`) to prevent regressions.
- Keep rendered-overlay and runtime-entrypoint conformance checks in CI.

## Open Questions for Future Audits

- How is schema evolution governed and enforced in production?
- What are the SLAs and current performance baselines for ingestion and Hot Store?
- Are there existing CI checks on GitHub Actions beyond linting and tests?
