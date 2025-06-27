"""Shared pytest fixtures for integration tests."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest


@pytest.fixture
def spot_test_symbols() -> list[str]:
    """Common spot trading symbols for testing.

    Returns:
        List of spot trading symbols
    """
    return ["SOL_USDC", "BTC_USDC", "ETH_USDC"]


@pytest.fixture
def perp_test_symbols() -> list[str]:
    """Common perp symbols for testing.

    Returns:
        List of perpetual futures symbols
    """
    return ["SOL_USDC_PERP", "BTC_USDC_PERP", "ETH_USDC_PERP"]


@pytest.fixture
def precision_test_amounts() -> list[Decimal]:
    """Test amounts for precision validation.

    Returns:
        List of Decimal amounts for testing precision
    """
    return [
        Decimal("0.00000001"),  # Dust
        Decimal("0.1"),  # Small
        Decimal(100),  # Normal
        Decimal("999999.99"),  # Large
    ]


@pytest.fixture
def mock_order_response() -> dict[str, Any]:
    """Mock order response for testing.

    Returns:
        Dict containing mock order response data
    """
    return {
        "client_order_id": "test-123",
        "exchange_order_id": "exchange-456",
        "status": "NEW",
        "quantity_requested": "1.0",
        "quantity_filled": "0.0",
    }
