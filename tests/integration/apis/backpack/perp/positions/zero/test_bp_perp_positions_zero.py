"""Integration tests for Backpack private positions endpoints with zero balance."""

from __future__ import annotations

from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.core.models.derivative_position import DerivativePosition

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/positions"], indirect=True
)
class TestBackpackPerpPositionsZero:
    """Positions integration tests specifically for zero balance scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_empty_account(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with empty account (no open positions)."""
        positions = await bp_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list)

        if not positions:
            assert len(positions) == 0
            return

        for _, position in enumerate(positions):
            assert isinstance(position, DerivativePosition)
            assert position.exchange == "backpack"
            assert isinstance(position.symbol, str)
            assert len(position.symbol) > 0

            assert abs(position.size) <= 1
            assert position.timestamp is not None
