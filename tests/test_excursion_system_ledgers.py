"""Excursion analytics only reads ledgers from live trading systems."""

from __future__ import annotations

import pytest

from heber.features.pipelines.excursion_analytics import SYSTEM_LEDGER_PATHS

LIVE_SYSTEMS = {"3Roses", "Kairos", "Cerberus", "Orion", "options-bot"}


@pytest.mark.unit
def test_ledger_paths_cover_exactly_the_live_systems() -> None:
    assert set(SYSTEM_LEDGER_PATHS) == LIVE_SYSTEMS


@pytest.mark.unit
@pytest.mark.parametrize("dead_system", ["whalehunter", "trading-bot"])
def test_dead_systems_are_not_scanned(dead_system: str) -> None:
    assert dead_system not in SYSTEM_LEDGER_PATHS
