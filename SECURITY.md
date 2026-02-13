# Security

## Reporting Vulnerabilities

Report security issues privately to repository maintainers. Do not publish exploitable details in public issues.

## Security Architecture

- Credentials are loaded from environment variables.
- Ingestion boundaries validate data before storage.
- `ts_available` semantics protect against time leakage in research/backtests.

## Credential Handling

- Never commit secrets.
- Use `.env` for local development and keep `.env.example` as placeholders only.
- Rotate any credential immediately if exposed.

## Dependency Security

Use project checks before merge:

```bash
pre-commit run --all-files
```

This includes secret scanning and static checks configured for the repository.

## Safety-Critical Paths

Changes touching these paths require elevated review:

- `/Users/jacobmcmillan/Empire/Heber/heber/writer/consumer.py`
- `/Users/jacobmcmillan/Empire/Heber/heber/watch/consumer.py`
- `/Users/jacobmcmillan/Empire/Heber/heber/watch/features.py`
- `/Users/jacobmcmillan/Empire/Heber/heber/config.py`

Any change that could affect order/risk behavior in downstream trading repos must preserve existing safeguards and data integrity.
