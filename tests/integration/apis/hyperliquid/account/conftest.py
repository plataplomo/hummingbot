"""Account management specific fixtures for Hyperliquid integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def hl_account_test_config() -> dict[str, Decimal]:
    """Hyperliquid account test configuration.

    Returns:
        Dict with account test configuration parameters
    """
    return {
        "min_equity": Decimal("0.01"),
        "min_margin": Decimal("0.005"),
    }
