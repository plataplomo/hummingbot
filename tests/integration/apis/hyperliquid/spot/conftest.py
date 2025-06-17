"""Spot trading specific fixtures for Hyperliquid integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def hl_spot_test_config() -> dict[str, list[str] | list[Decimal] | Decimal]:
    """Hyperliquid spot test configuration."""
    return {
        # REMOVED HARDCODED SYMBOLS - SECURITY VIOLATION
        # Must get available spot symbols from exchange API
        "min_transfer_amount": Decimal("0.01"),
        "test_amounts": [Decimal("0.01"), Decimal("0.1"), Decimal("1.0")],
    }
