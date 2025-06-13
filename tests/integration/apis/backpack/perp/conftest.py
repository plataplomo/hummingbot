"""Perpetual trading specific fixtures for Backpack integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def bp_perp_test_config() -> dict[str, list[str] | list[Decimal] | Decimal | int]:
    """Backpack perp test configuration."""
    return {
        "symbols": ["SOL-PERP", "BTC-PERP", "ETH-PERP"],
        "min_position_size": Decimal("0.01"),
        "test_sizes": [Decimal("0.01"), Decimal("0.1"), Decimal("1.0")],
        "max_leverage": 10,
    }
