"""Regression tests for dead code cleanup.

Verifies that modules still import and function correctly after removing
unused functions and classes identified as dead code.
"""

import importlib


class TestCoreErrorsModuleAfterCleanup:
    """Verify heber.core.errors still imports after removing ErrorCode and HeberError."""

    def test_module_imports(self):
        """The errors module should still be importable."""
        mod = importlib.import_module("heber.core.errors")
        assert mod is not None

    def test_no_errorcode_class(self):
        """ErrorCode was dead code and should be removed."""
        mod = importlib.import_module("heber.core.errors")
        assert not hasattr(mod, "ErrorCode")

    def test_no_hebererror_class(self):
        """HeberError was dead code and should be removed."""
        mod = importlib.import_module("heber.core.errors")
        assert not hasattr(mod, "HeberError")


class TestAlertLabelsModuleAfterCleanup:
    """Verify alert_labels still works after removing analysis helpers."""

    def test_module_imports(self):
        """The alert_labels template module should still be importable."""
        mod = importlib.import_module("heber.features.templates.alert_labels")
        assert mod is not None

    def test_compute_barrier_labels_exists(self):
        """compute_barrier_labels is the primary API and must remain."""
        mod = importlib.import_module("heber.features.templates.alert_labels")
        assert hasattr(mod, "compute_barrier_labels")
        assert callable(mod.compute_barrier_labels)

    def test_compute_multi_horizon_labels_exists(self):
        """compute_multi_horizon_labels is used and must remain."""
        mod = importlib.import_module("heber.features.templates.alert_labels")
        assert hasattr(mod, "compute_multi_horizon_labels")
        assert callable(mod.compute_multi_horizon_labels)

    def test_no_compute_win_rate_by_feature(self):
        """compute_win_rate_by_feature was dead code and should be removed."""
        mod = importlib.import_module("heber.features.templates.alert_labels")
        assert not hasattr(mod, "compute_win_rate_by_feature")

    def test_no_compute_regime_analysis(self):
        """compute_regime_analysis was dead code and should be removed."""
        mod = importlib.import_module("heber.features.templates.alert_labels")
        assert not hasattr(mod, "compute_regime_analysis")

    def test_core_classes_still_present(self):
        """Core dataclasses/enums must survive cleanup."""
        mod = importlib.import_module("heber.features.templates.alert_labels")
        for name in ("AlertHorizon", "VixRegime", "BarrierConfig", "ContractBarrierConfig", "SlippageModel"):
            assert hasattr(mod, name), f"{name} should still be present"


class TestSodaScannerModuleAfterCleanup:
    """Verify soda_scanner still works after removing get_quality_scanner."""

    def test_module_imports(self):
        """The soda_scanner module should still be importable."""
        mod = importlib.import_module("heber.quality.soda_scanner")
        assert mod is not None

    def test_soda_quality_scanner_exists(self):
        """SodaQualityScanner class must remain (it is the core implementation)."""
        mod = importlib.import_module("heber.quality.soda_scanner")
        assert hasattr(mod, "SodaQualityScanner")

    def test_no_get_quality_scanner(self):
        """get_quality_scanner singleton was dead code and should be removed."""
        mod = importlib.import_module("heber.quality.soda_scanner")
        assert not hasattr(mod, "get_quality_scanner")

    def test_no_singleton_variable(self):
        """The _scanner singleton variable should be removed."""
        mod = importlib.import_module("heber.quality.soda_scanner")
        assert not hasattr(mod, "_scanner")
