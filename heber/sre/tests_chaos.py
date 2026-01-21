"""Tests for Chaos Engineering and Capacity Planning (PRD §41-42)."""

from heber.sre.capacity import (
    CapacityForecast,
    CapacityPlanner,
    ScalingTrigger,
)
from heber.sre.chaos import (
    ChaosExperiment,
    ChaosRegistry,
    ExperimentFrequency,
    ExperimentStatus,
    SuccessCriterion,
)


class TestChaosExperiment:
    """Test ChaosExperiment dataclass."""

    def test_to_dict(self):
        exp = ChaosExperiment(
            name="Test Experiment",
            target="test-pod",
            hypothesis="When X happens, Y should occur",
            expected_outcome="Y occurs",
            procedure=["Step 1", "Step 2"],
            success_criteria=[
                SuccessCriterion("No data loss"),
            ],
        )

        d = exp.to_dict()

        assert d["name"] == "Test Experiment"
        assert len(d["procedure"]) == 2

    def test_to_markdown(self):
        exp = ChaosExperiment(
            name="Test Experiment",
            target="test-pod",
            hypothesis="When X happens, Y should occur",
            expected_outcome="Y occurs",
            procedure=["Step 1", "Step 2"],
            success_criteria=[
                SuccessCriterion("No data loss"),
            ],
        )

        md = exp.to_markdown()

        assert "Test Experiment" in md
        assert "Hypothesis" in md
        assert "Step 1" in md


class TestChaosRegistry:
    """Test ChaosRegistry."""

    def test_default_experiments_loaded(self):
        registry = ChaosRegistry()

        assert len(registry.experiments) >= 7
        assert "Kill Consumer Pod" in registry.experiments

    def test_get_by_name(self):
        registry = ChaosRegistry()

        exp = registry.get("Kill Writer Pod")

        assert exp is not None
        assert exp.target == "heber-writer"

    def test_list_by_frequency(self):
        registry = ChaosRegistry()

        weekly = registry.list_by_frequency(ExperimentFrequency.WEEKLY)
        monthly = registry.list_by_frequency(ExperimentFrequency.MONTHLY)

        assert len(weekly) >= 3
        assert len(monthly) >= 2

    def test_get_schedule(self):
        registry = ChaosRegistry()

        schedule = registry.get_schedule()

        assert "weekly" in schedule
        assert "monthly" in schedule
        assert len(schedule["weekly"]) >= 1

    def test_start_and_complete_run(self):
        registry = ChaosRegistry()

        run = registry.start_run("Kill Consumer Pod")

        assert run is not None
        assert run.status == ExperimentStatus.RUNNING

        registry.complete_run(
            run,
            results={"No data loss": True, "Recovery within 30 seconds": True},
        )

        assert run.status == ExperimentStatus.PASSED
        assert run.duration_seconds is not None

    def test_complete_run_failed(self):
        registry = ChaosRegistry()
        run = registry.start_run("Kill Consumer Pod")

        registry.complete_run(
            run,
            results={"No data loss": True, "Recovery within 30 seconds": False},
        )

        assert run.status == ExperimentStatus.FAILED

    def test_export_all_markdown(self):
        registry = ChaosRegistry()

        md = registry.export_all_markdown()

        assert "Chaos Engineering Runbooks" in md
        assert "Kill Consumer Pod" in md


class TestScalingTrigger:
    """Test ScalingTrigger."""

    def test_is_triggered_true(self):
        trigger = ScalingTrigger("cpu", 70, "%", 15, "Scale up")

        assert trigger.is_triggered(75)

    def test_is_triggered_false(self):
        trigger = ScalingTrigger("cpu", 70, "%", 15, "Scale up")

        assert not trigger.is_triggered(65)


class TestCapacityPlanner:
    """Test CapacityPlanner."""

    def test_default_baselines_loaded(self):
        planner = CapacityPlanner()

        baseline = planner.get_baseline("events_per_day")

        assert baseline is not None
        assert baseline.value == 50_000_000

    def test_check_triggers(self):
        planner = CapacityPlanner()

        triggered = planner.check_triggers(
            {
                "consumer_cpu": 85,  # Over 70% threshold
                "consumer_lag": 30,  # Under 60s threshold
            }
        )

        assert len(triggered) == 1
        assert triggered[0][0] == "consumer_cpu"

    def test_get_scaling_recommendations(self):
        planner = CapacityPlanner()

        recommendations = planner.get_scaling_recommendations(
            {
                "consumer_cpu": 85,
                "writer_memory": 90,
            }
        )

        assert len(recommendations) == 2
        assert any(r["metric"] == "consumer_cpu" for r in recommendations)

    def test_project_cost(self):
        planner = CapacityPlanner()

        projection = planner.project_cost(3.0)

        assert projection.scenario == "3.0x volume"
        assert projection.projected_cost_monthly > projection.base_cost_monthly
        assert projection.delta > 0

    def test_get_bottleneck(self):
        planner = CapacityPlanner()

        bottleneck = planner.get_bottleneck("Compactor")

        assert bottleneck is not None
        assert bottleneck.cpu_bound == "High"
        assert bottleneck.memory_bound == "Very High"

    def test_generate_report(self):
        planner = CapacityPlanner()

        report = planner.generate_report()

        assert "baselines" in report
        assert "triggers" in report
        assert "forecasts" in report
        assert "bottlenecks" in report
        assert "cost_projections" in report


class TestCapacityForecast:
    """Test CapacityForecast."""

    def test_to_dict(self):
        forecast = CapacityForecast("Q1 2026", 50_000_000, 1.8, 6, 0)

        d = forecast.to_dict()

        assert d["quarter"] == "Q1 2026"
        assert d["events_per_day_fmt"] == "50M"


def run_all_chaos_capacity_tests() -> dict[str, bool]:
    """Run all chaos and capacity tests."""
    results = {}

    test_classes = [
        TestChaosExperiment,
        TestChaosRegistry,
        TestScalingTrigger,
        TestCapacityPlanner,
        TestCapacityForecast,
    ]

    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nChaos & Capacity Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_chaos_capacity_tests()
