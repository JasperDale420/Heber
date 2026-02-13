# Developer Notes

## Gotchas

- Heber often reads from Data-Gateway Redis, not Heber-local Redis. Verify `HEBER_REDIS_URL` per runtime context.
- Container paths and host paths differ. Confirm `HEBER_VOLUME_ROOT`, `HEBER_DATA_ROOT`, and `HEBER_GOLD_PATH` in Docker.
- Mixed historical Parquet schemas can break compaction if not schema-unioned.

## Performance Considerations

- Compaction cadence and partition size strongly impact read performance.
- Watch enrichment can generate burst traffic; use retry/backoff/throttle carefully.
- Avoid expensive full partition scans during high-frequency loops.

## Debugging Tips

- Start with service logs for `heber-watch`, `heber-consumer`, and `heber-compactor`.
- Correlate by `event_id`, `alert_id`, `symbol`, and dataset partition paths.
- Validate by injecting controlled Redis stream events when RCA requires deterministic reproduction.

## Historical Context

- Bronze is authoritative raw data.
- Silver is strict normalized data for analytics and model inputs.
- Gold is for features/labels and model-ready artifacts.
- Dataflow health checks provide passive proof that Gateway -> Ingest -> Storage is functioning.
