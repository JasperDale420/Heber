"""Tests for Streams, Slices, and Gap Resolutions (PRD §60, §61, §62)."""

from heber.bus.streams import (
    StreamConfig,
    StreamPriority,
    StreamRegistry,
)
from heber.ops.gap_resolutions import (
    GapResolutionRegistry,
)
from heber.ops.slices import (
    SliceManager,
    SliceStatus,
)


class TestStreamRegistry:
    """Test StreamRegistry."""

    def test_list_streams(self):
        registry = StreamRegistry()

        streams = registry.list_streams()

        assert len(streams) == 15

    def test_get_stream(self):
        registry = StreamRegistry()

        bars = registry.get_stream("bars")

        assert bars is not None
        assert bars.priority == StreamPriority.CRITICAL

    def test_list_consumer_groups(self):
        registry = StreamRegistry()

        groups = registry.list_consumer_groups()

        assert len(groups) == 6

    def test_get_streams_by_priority(self):
        registry = StreamRegistry()

        critical = registry.get_streams_by_priority(StreamPriority.CRITICAL)

        assert len(critical) == 3

    def test_get_streams_for_consumer(self):
        registry = StreamRegistry()

        market = registry.get_streams_for_consumer("market-data-consumer")

        assert len(market) == 4

    def test_stream_key(self):
        config = StreamConfig("test", "test_dataset", StreamPriority.NORMAL, "test-consumer")

        assert config.stream_key == "heber:events:test"

    def test_generate_report(self):
        registry = StreamRegistry()

        report = registry.generate_report()

        assert report["summary"]["total_streams"] == 15
        assert report["summary"]["total_consumer_groups"] == 6

    def test_darkpool_stream_uses_canonical_name(self):
        registry = StreamRegistry()

        canonical = registry.get_stream("darkpool")
        legacy = registry.get_stream("darkpool_trades")

        assert canonical is not None
        assert canonical.dataset == "darkpool"
        assert legacy is None


class TestSliceManager:
    """Test SliceManager."""

    def test_list_all(self):
        manager = SliceManager()

        slices = manager.list_all()

        assert len(slices) == 8

    def test_get_slice(self):
        manager = SliceManager()

        slice1 = manager.get_slice(1)

        assert slice1 is not None
        assert slice1.name == "Core Market Data"

    def test_get_ready_slices(self):
        manager = SliceManager()

        ready = manager.get_ready_slices()

        assert len(ready) >= 4

    def test_mark_completed(self):
        manager = SliceManager()

        result = manager.mark_completed(2)

        assert result is True
        assert manager.get_slice(2).status == SliceStatus.COMPLETED

    def test_get_completion_status(self):
        manager = SliceManager()

        status = manager.get_completion_status()

        assert "completed" in status
        assert "not_started" in status

    def test_generate_report(self):
        manager = SliceManager()

        report = manager.generate_report()

        assert "summary" in report
        assert "slices" in report
        assert report["summary"]["total_slices"] == 8

    def test_slice_3_uses_canonical_darkpool_dataset_name(self):
        manager = SliceManager()
        slice3 = manager.get_slice(3)

        assert slice3 is not None
        assert "darkpool" in slice3.datasets
        assert "darkpool_trades" not in slice3.datasets


class TestGapResolutionRegistry:
    """Test GapResolutionRegistry."""

    def test_list_all(self):
        registry = GapResolutionRegistry()

        all_decisions = registry.list_all()

        assert len(all_decisions) == 6

    def test_get_by_category(self):
        registry = GapResolutionRegistry()

        data_model = registry.get_by_category("data_model")

        assert len(data_model) == 3

    def test_get_by_id(self):
        registry = GapResolutionRegistry()

        decision = registry.get_by_id("DM-001")

        assert decision is not None
        assert "partition" in decision.decision.lower()

    def test_generate_report(self):
        registry = GapResolutionRegistry()

        report = registry.generate_report()

        assert report["summary"]["total_decisions"] == 18
        assert len(report["summary"]["by_category"]) == 6
