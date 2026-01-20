"""Survivor Bias Handling (PRD §35).

Provides utilities for point-in-time universe filtering,
delisting tracking, and survivor bias prevention.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, UTC
from enum import Enum
from typing import Any

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


class DelistReason(str, Enum):
    """Reason for instrument delisting (PRD §35.2)."""
    
    BANKRUPTCY = "bankruptcy"
    MERGER = "merger"
    ACQUISITION = "acquisition"
    VOLUNTARY = "voluntary"
    REGULATORY = "regulatory"
    OTHER = "other"


@dataclass
class InstrumentLifecycle:
    """Instrument lifecycle metadata (PRD §35.2).
    
    Attributes:
        instrument_key: Canonical instrument identifier
        list_date: Date when instrument was listed
        delist_date: Date when instrument was delisted (None if active)
        delist_reason: Reason for delisting (None if active)
    """
    
    instrument_key: str
    list_date: date
    delist_date: date | None = None
    delist_reason: DelistReason | None = None
    
    def is_active(self, asof_date: date) -> bool:
        """Check if instrument was active on given date."""
        if asof_date < self.list_date:
            return False
        if self.delist_date and asof_date >= self.delist_date:
            return False
        return True
    
    def will_delist_within(self, asof_date: date, days: int) -> bool:
        """Check if instrument will delist within N days of asof_date."""
        if not self.delist_date:
            return False
        days_until_delist = (self.delist_date - asof_date).days
        return 0 < days_until_delist <= days
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument_key": self.instrument_key,
            "list_date": self.list_date.isoformat(),
            "delist_date": self.delist_date.isoformat() if self.delist_date else None,
            "delist_reason": self.delist_reason.value if self.delist_reason else None,
        }


class UniverseManager:
    """Manage point-in-time universe filtering (PRD §35.3).
    
    Prevents survivor bias by providing universe snapshots
    as they existed at specific points in time.
    """
    
    def __init__(self, instruments: list[InstrumentLifecycle] | None = None):
        """Initialize universe manager.
        
        Args:
            instruments: List of instrument lifecycle records
        """
        self.instruments: dict[str, InstrumentLifecycle] = {}
        if instruments:
            for inst in instruments:
                self.instruments[inst.instrument_key] = inst
    
    def add_instrument(self, instrument: InstrumentLifecycle) -> None:
        """Add or update an instrument."""
        self.instruments[instrument.instrument_key] = instrument
    
    def get_universe(
        self,
        asof_date: date | str,
        filter_criteria: dict[str, Any] | None = None,
    ) -> list[str]:
        """Get universe as it existed on a specific date (PRD §35.3).
        
        Args:
            asof_date: Point-in-time date for universe snapshot
            filter_criteria: Optional filter (asset_class, exchange, etc.)
            
        Returns:
            List of instrument keys that were active on asof_date
        """
        if isinstance(asof_date, str):
            asof_date = date.fromisoformat(asof_date)
        
        active = []
        for key, inst in self.instruments.items():
            if inst.is_active(asof_date):
                active.append(key)
        
        logger.info(
            "Universe snapshot",
            asof_date=str(asof_date),
            total_instruments=len(self.instruments),
            active_instruments=len(active),
        )
        
        return active
    
    def filter_dataframe(
        self,
        df: pd.DataFrame,
        asof_date: date | str,
        instrument_key_col: str = "instrument_key",
        exclude_future_delistings: bool = True,
        mark_delistings: bool = False,
        delist_warning_days: int = 30,
    ) -> pd.DataFrame:
        """Filter DataFrame to point-in-time universe (PRD §35.4).
        
        Args:
            df: DataFrame to filter
            asof_date: Point-in-time date for filtering
            instrument_key_col: Column containing instrument keys
            exclude_future_delistings: If True, exclude symbols that delist after asof_date
            mark_delistings: If True, add column indicating upcoming delistings
            delist_warning_days: Days ahead to mark delistings
            
        Returns:
            Filtered DataFrame
        """
        if isinstance(asof_date, str):
            asof_date = date.fromisoformat(asof_date)
        
        if df.empty:
            return df
        
        # Get active universe
        active_keys = set(self.get_universe(asof_date))
        
        # Filter to active instruments
        result = df[df[instrument_key_col].isin(active_keys)].copy()
        
        # Additional filtering for future delistings
        if exclude_future_delistings:
            keys_to_exclude = set()
            for key in result[instrument_key_col].unique():
                inst = self.instruments.get(key)
                if inst and inst.delist_date and inst.delist_date > asof_date:
                    keys_to_exclude.add(key)
            result = result[~result[instrument_key_col].isin(keys_to_exclude)]
        
        # Optionally mark upcoming delistings
        if mark_delistings:
            def check_delist(key: str) -> bool:
                inst = self.instruments.get(key)
                if inst:
                    return inst.will_delist_within(asof_date, delist_warning_days)
                return False
            
            result[f"will_delist_within_{delist_warning_days}d"] = result[instrument_key_col].apply(
                check_delist
            )
        
        logger.info(
            "DataFrame filtered to universe",
            original_rows=len(df),
            filtered_rows=len(result),
            asof_date=str(asof_date),
        )
        
        return result
    
    def get_delisted_instruments(
        self,
        start_date: date | str,
        end_date: date | str,
    ) -> list[InstrumentLifecycle]:
        """Get instruments that delisted within a date range.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            List of instruments that delisted in the range
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)
        
        delisted = []
        for inst in self.instruments.values():
            if inst.delist_date:
                if start_date <= inst.delist_date <= end_date:
                    delisted.append(inst)
        
        return delisted
    
    def get_newly_listed_instruments(
        self,
        start_date: date | str,
        end_date: date | str,
    ) -> list[InstrumentLifecycle]:
        """Get instruments that were listed within a date range.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            List of instruments that were listed in the range
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)
        
        listed = []
        for inst in self.instruments.values():
            if start_date <= inst.list_date <= end_date:
                listed.append(inst)
        
        return listed


@dataclass
class UniverseSnapshot:
    """Immutable snapshot of universe at a point in time."""
    
    asof_date: date
    instrument_keys: list[str]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "asof_date": self.asof_date.isoformat(),
            "instrument_keys": self.instrument_keys,
            "created_at": self.created_at.isoformat(),
            "count": len(self.instrument_keys),
        }


def create_universe_manager_from_dataframe(
    df: pd.DataFrame,
    instrument_key_col: str = "instrument_key",
    list_date_col: str = "list_date",
    delist_date_col: str = "delist_date",
    delist_reason_col: str = "delist_reason",
) -> UniverseManager:
    """Create UniverseManager from a DataFrame of instrument metadata.
    
    Args:
        df: DataFrame with instrument lifecycle data
        instrument_key_col: Column with instrument keys
        list_date_col: Column with list dates
        delist_date_col: Column with delist dates
        delist_reason_col: Column with delist reasons
        
    Returns:
        UniverseManager populated with instruments
    """
    instruments = []
    
    for _, row in df.iterrows():
        list_date = row[list_date_col]
        if isinstance(list_date, str):
            list_date = date.fromisoformat(list_date)
        elif isinstance(list_date, datetime):
            list_date = list_date.date()
        
        delist_date = None
        if pd.notna(row.get(delist_date_col)):
            delist_date = row[delist_date_col]
            if isinstance(delist_date, str):
                delist_date = date.fromisoformat(delist_date)
            elif isinstance(delist_date, datetime):
                delist_date = delist_date.date()
        
        delist_reason = None
        if pd.notna(row.get(delist_reason_col)):
            try:
                delist_reason = DelistReason(row[delist_reason_col])
            except ValueError:
                delist_reason = DelistReason.OTHER
        
        instruments.append(InstrumentLifecycle(
            instrument_key=row[instrument_key_col],
            list_date=list_date,
            delist_date=delist_date,
            delist_reason=delist_reason,
        ))
    
    return UniverseManager(instruments)
