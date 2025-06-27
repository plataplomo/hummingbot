"""Perpetual trading specific fixtures for Hyperliquid integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def hl_perp_test_config() -> dict[str, list[str] | list[Decimal] | Decimal | int]:
    """Hyperliquid perp test configuration.

    Returns:
        Dict with perp test configuration parameters
    """
    return {
        # REMOVED HARDCODED SYMBOLS - SECURITY VIOLATION
        # Must get available perp symbols from exchange API
        "min_position_size": Decimal("0.001"),
        "test_sizes": [Decimal("0.001"), Decimal("0.01"), Decimal("0.1")],
        "max_leverage": 20,
    }
