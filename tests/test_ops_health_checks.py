from sqlalchemy import create_engine

from heber.ops.health import HealthStatus, create_postgres_check


def test_create_postgres_check_uses_sqlalchemy_executable_sql() -> None:
    engine = create_engine("sqlite:///:memory:")
    check = create_postgres_check(engine)

    result = check()

    assert result.name == "postgres"
    assert result.status == HealthStatus.OK
    assert result.message is None


def test_create_postgres_check_reports_connection_failure() -> None:
    class _BrokenConnectionContext:
        def __enter__(self):
            raise RuntimeError("db unavailable")

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    class _BrokenEngine:
        def connect(self) -> _BrokenConnectionContext:
            return _BrokenConnectionContext()

    check = create_postgres_check(_BrokenEngine())

    result = check()

    assert result.name == "postgres"
    assert result.status == HealthStatus.ERROR
    assert result.message is not None
    assert "db unavailable" in result.message
