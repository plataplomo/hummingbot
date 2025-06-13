"""Account management specific fixtures for Backpack integration tests."""

from __future__ import annotations

from decimal import Decimal

import pytest


@pytest.fixture
def bp_account_test_config() -> dict[str, Decimal]:
    """Backpack account test configuration."""
    return {
        "min_equity": Decimal("0.01"),
        "min_margin": Decimal("0.005"),
    }