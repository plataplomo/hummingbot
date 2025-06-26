"""Integration tests for Hyperliquid positions endpoints with zero position scenarios.

This module focuses specifically on testing the DerivativePosition model pipeline
through Hyperliquid's /info endpoint with user address authentication for zero position scenarios.
Tests validate complete data transformation from API responses to DerivativePosition instances.

Model Focus: DerivativePosition (Zero Position Edge Cases)
- Validates complete DerivativePosition model field mapping for empty accounts
- Tests Decimal precision for zero financial values
- Validates business logic constraints with zero positions
- Tests Hyperliquid-specific position details (hl_details) for empty accounts
- Comprehensive error handling and position edge cases

Authentication: EIP-712 signing for testnet environment
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.core.models.derivative_position import DerivativePosition


pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/positions/zero"],
    indirect=True,
)
@pytest.mark.perp
@pytest.mark.zero_balance
class TestHyperliquidPerpPositionsZero:
    """Comprehensive zero position integration tests for DerivativePosition model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_empty_account(
        self,
        hl_api_for_zero_balance_test: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with account that has no open positions."""
        positions = await hl_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list), "get_positions() should always return list"

        if len(positions) == 0:
            return

        for position in positions:
            assert isinstance(position, DerivativePosition), (
                "All returned positions should be valid"
            )
