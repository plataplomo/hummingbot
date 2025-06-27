"""Spot trading specific fixtures for Backpack integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def bp_spot_test_config() -> dict[str, list[str] | list[Decimal] | Decimal]:
    """Backpack spot test configuration.

    Returns:
        dict[str, list[str] | list[Decimal] | Decimal]: Configuration for spot tests.
    """
    return {
        "symbols": ["SOL_USDC", "BTC_USDC"],
        "min_order_size": Decimal("0.01"),
        "test_quantities": [Decimal("0.01"), Decimal("0.1"), Decimal("1.0")],
    }
