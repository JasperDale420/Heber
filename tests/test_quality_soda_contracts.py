from datetime import datetime

import pandas as pd

from heber.config import settings
from heber.quality.contracts import DataQualityValidator, QualityContract, QualityMetric
from heber.quality.soda_scanner import SodaConfig


def test_soda_config_defaults_to_settings_silver_path() -> None:
    config = SodaConfig()
    assert config.silver_path == settings.silver_path


def test_soda_config_from_env_uses_settings_fallback(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_SILVER_PATH", raising=False)
    config = SodaConfig.from_env()
    assert config.silver_path == settings.silver_path


def test_non_null_column_reporting_respects_contract_threshold() -> None:
    validator = DataQualityValidator()
    validator.add_contract(
        "bars",
        QualityContract(
            metric=QualityMetric.NON_NULL_RATE,
            threshold=0.90,
            columns=["open", "close"],
        ),
    )

    # open: 90% non-null (at threshold), close: 80% non-null (below threshold)
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, None],
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, None, None],
        }
    )

    report = validator.validate(
        dataset="bars",
        layer="silver",
        df=df,
        time_range=(datetime(2024, 1, 1), datetime(2024, 1, 2)),
    )

    assert report.passed is False
    assert len(report.violations) == 1
    assert report.violations[0].affected_symbols == ["close"]
