from heber.testing.environments import EnvironmentManager
from heber.testing.framework import E2ETestSuite


def test_e2e_suite_exposes_schedule_api() -> None:
    suite = E2ETestSuite()
    schedule = suite.get_schedule()

    assert "on_merge_to_main" in schedule
    assert "nightly" in schedule
    assert len(schedule["on_merge_to_main"]) == 3


def test_local_service_ports_align_with_compose_defaults() -> None:
    manager = EnvironmentManager()
    services = {service["name"]: service for service in manager.get_local_services()}

    assert "5433:5432" in services["postgres"]["ports"]
    assert "6380:6379" in services["redis"]["ports"]
