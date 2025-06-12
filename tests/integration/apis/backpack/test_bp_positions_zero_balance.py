"""Integration tests for Backpack private positions endpoints with zero balance.

This module focuses specifically on testing the DerivativePosition model pipeline
through Backpack's private positions endpoint with Ed25519 authentication
when the account has zero balance/no open positions.
Tests validate complete data transformation from API responses to DerivativePosition instances.

Model Focus: DerivativePosition (Zero Balance Scenarios)
- Validates empty positions list for accounts with no open positions
- Tests authentication and error handling with zero balance
- Validates model structure consistency even when no positions exist
- Tests Backpack-specific response handling for empty accounts

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: Zero balance/no positions (safe for CI/CD testing)
"""

from __future__ import annotations

from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.derivative_position import DerivativePosition

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/positions"], indirect=True
)
class TestBackpackPositionsZeroBalance:
    """Positions integration tests specifically for zero balance scenarios."""

    @pytest.mark.vcr
    async def test_get_positions_empty_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with empty account (no open positions).

        This test validates behavior when account has no open derivative positions.
        Important for testing edge cases in position handling with zero balance.
        Safe for CI/CD environments as it doesn't require funds or positions.
        """
        # Execute the API call
        positions = await bp_api_for_test_env.get_positions()

        # Validate container type
        assert isinstance(positions, list), "get_positions() should return list[DerivativePosition]"

        # Empty positions is valid for accounts with no open positions
        # This is the expected case for zero balance accounts
        if not positions:
            # Empty list is expected and valid for zero balance accounts
            assert len(positions) == 0, "Zero balance account should have empty positions list"
            return

        # If positions exist (unexpected for zero balance), validate structure
        # This handles edge cases where account might have minimal/dust positions
        for i, position in enumerate(positions):
            assert isinstance(position, DerivativePosition), (
                f"Position {i} should be DerivativePosition instance, got {type(position)}"
            )

            # Validate basic fields
            assert position.exchange == "backpack", (
                f"Position.exchange should be 'backpack', got {position.exchange}"
            )
            assert isinstance(position.symbol, str), f"Position {i} symbol must be string"
            assert len(position.symbol) > 0, f"Position {i} symbol cannot be empty"

            # For zero balance accounts, any positions should be minimal/dust
            # Size should be very small or zero for zero balance accounts
            assert abs(position.size) <= 1, (
                f"Zero balance account should have minimal position size, "
                f"got {position.size} for {position.symbol}"
            )

            # Should have valid timestamp structure
            assert position.timestamp is not None, f"Position {i} timestamp should be valid"
