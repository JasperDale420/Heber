"""Tests for Survivor Bias Handling (PRD §35)."""

from datetime import date

import pandas as pd
import pytest

from heber.universe.survivor_bias import (
    DelistReason,
    InstrumentLifecycle,
    UniverseManager,
    UniverseSnapshot,
    create_universe_manager_from_dataframe,
)


class TestInstrumentLifecycle:
    """Test InstrumentLifecycle."""
    
    def test_is_active_before_listing(self):
        inst = InstrumentLifecycle(
            instrument_key="equity:AAPL",
            list_date=date(2000, 1, 1),
        )
        
        assert not inst.is_active(date(1999, 12, 31))
        assert inst.is_active(date(2000, 1, 1))
    
    def test_is_active_after_delisting(self):
        inst = InstrumentLifecycle(
            instrument_key="equity:XYZ",
            list_date=date(2000, 1, 1),
            delist_date=date(2023, 6, 15),
            delist_reason=DelistReason.BANKRUPTCY,
        )
        
        assert inst.is_active(date(2023, 6, 14))
        assert not inst.is_active(date(2023, 6, 15))
    
    def test_will_delist_within(self):
        inst = InstrumentLifecycle(
            instrument_key="equity:XYZ",
            list_date=date(2000, 1, 1),
            delist_date=date(2023, 6, 30),
        )
        
        # 30 days before delist
        assert inst.will_delist_within(date(2023, 6, 1), 30)
        # 31 days before delist
        assert not inst.will_delist_within(date(2023, 5, 30), 30)


class TestUniverseManager:
    """Test UniverseManager."""
    
    def setup_method(self):
        """Setup test universe."""
        self.manager = UniverseManager([
            InstrumentLifecycle("equity:AAPL", date(1980, 12, 12)),  # Never delisted
            InstrumentLifecycle("equity:MSFT", date(1986, 3, 13)),  # Never delisted
            InstrumentLifecycle("equity:ENRN", date(1985, 1, 1), date(2001, 12, 2), DelistReason.BANKRUPTCY),
            InstrumentLifecycle("equity:TWTR", date(2013, 11, 7), date(2022, 10, 28), DelistReason.ACQUISITION),
            InstrumentLifecycle("equity:NVDA", date(1999, 1, 22)),  # Never delisted
        ])
    
    def test_get_universe_current(self):
        """Test universe before any delistings."""
        universe = self.manager.get_universe(date(2000, 1, 1))
        
        assert "equity:AAPL" in universe
        assert "equity:MSFT" in universe
        assert "equity:ENRN" in universe
        assert "equity:TWTR" not in universe  # Not listed yet
    
    def test_get_universe_after_enron_bankruptcy(self):
        """Test universe after Enron delisted."""
        universe = self.manager.get_universe(date(2002, 1, 1))
        
        assert "equity:AAPL" in universe
        assert "equity:ENRN" not in universe  # Delisted
    
    def test_get_universe_after_twitter_acquisition(self):
        """Test universe after Twitter acquired."""
        universe = self.manager.get_universe(date(2023, 1, 1))
        
        assert "equity:AAPL" in universe
        assert "equity:TWTR" not in universe  # Acquired
    
    def test_filter_dataframe_basic(self):
        """Test basic DataFrame filtering."""
        df = pd.DataFrame({
            "instrument_key": ["equity:AAPL", "equity:ENRN", "equity:NVDA"],
            "value": [100, 50, 200],
        })
        
        # Before Enron bankruptcy
        result = self.manager.filter_dataframe(
            df,
            asof_date=date(2001, 1, 1),
            exclude_future_delistings=False,
        )
        
        assert len(result) == 3  # All included
    
    def test_filter_dataframe_exclude_future_delistings(self):
        """Test filtering with future delist exclusion."""
        df = pd.DataFrame({
            "instrument_key": ["equity:AAPL", "equity:ENRN", "equity:NVDA"],
            "value": [100, 50, 200],
        })
        
        # Before Enron bankruptcy, but exclude future delistings
        result = self.manager.filter_dataframe(
            df,
            asof_date=date(2001, 1, 1),
            exclude_future_delistings=True,
        )
        
        # ENRN is active but will delist later, so excluded
        assert len(result) == 2
        assert "equity:ENRN" not in result["instrument_key"].values
    
    def test_filter_dataframe_mark_delistings(self):
        """Test marking upcoming delistings."""
        df = pd.DataFrame({
            "instrument_key": ["equity:AAPL", "equity:TWTR"],
            "value": [100, 75],
        })
        
        # 15 days before Twitter acquisition
        result = self.manager.filter_dataframe(
            df,
            asof_date=date(2022, 10, 13),
            exclude_future_delistings=False,
            mark_delistings=True,
            delist_warning_days=30,
        )
        
        assert "will_delist_within_30d" in result.columns
    
    def test_get_delisted_instruments(self):
        """Test getting delisted instruments in range."""
        delisted = self.manager.get_delisted_instruments(
            start_date=date(2001, 1, 1),
            end_date=date(2002, 12, 31),
        )
        
        assert len(delisted) == 1
        assert delisted[0].instrument_key == "equity:ENRN"
    
    def test_get_newly_listed_instruments(self):
        """Test getting newly listed instruments in range."""
        listed = self.manager.get_newly_listed_instruments(
            start_date=date(2013, 1, 1),
            end_date=date(2013, 12, 31),
        )
        
        assert len(listed) == 1
        assert listed[0].instrument_key == "equity:TWTR"


class TestCreateFromDataFrame:
    """Test creating UniverseManager from DataFrame."""
    
    def test_create_from_dataframe(self):
        df = pd.DataFrame({
            "instrument_key": ["equity:ABC", "equity:XYZ"],
            "list_date": ["2010-01-01", "2015-06-15"],
            "delist_date": [None, "2020-03-01"],
            "delist_reason": [None, "bankruptcy"],
        })
        
        manager = create_universe_manager_from_dataframe(df)
        
        assert len(manager.instruments) == 2
        assert manager.instruments["equity:XYZ"].delist_reason == DelistReason.BANKRUPTCY


class TestUniverseSnapshot:
    """Test UniverseSnapshot."""
    
    def test_snapshot_to_dict(self):
        snapshot = UniverseSnapshot(
            asof_date=date(2024, 1, 15),
            instrument_keys=["equity:AAPL", "equity:MSFT"],
        )
        
        d = snapshot.to_dict()
        
        assert d["asof_date"] == "2024-01-15"
        assert d["count"] == 2


def run_all_survivor_bias_tests() -> dict[str, bool]:
    """Run all survivor bias tests."""
    results = {}
    
    test_classes = [
        TestInstrumentLifecycle,
        TestUniverseManager,
        TestCreateFromDataFrame,
        TestUniverseSnapshot,
    ]
    
    for test_class in test_classes:
        instance = test_class()
        if hasattr(instance, "setup_method"):
            instance.setup_method()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    if hasattr(instance, "setup_method"):
                        instance.setup_method()
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nSurvivor Bias Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_survivor_bias_tests()
