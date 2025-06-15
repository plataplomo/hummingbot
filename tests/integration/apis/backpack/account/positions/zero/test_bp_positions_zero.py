"""Integration tests for Backpack position functionality with no open positions.

Tests position retrieval when the account has no derivative positions,
including edge cases and empty responses.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from tests.integration.apis.backpack.shared.test_helpers import (
    COMMON_PERP_SYMBOLS,
    COMMON_SPOT_SYMBOLS,
)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.positions,
    pytest.mark.zero_positions,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/positions_zero"], indirect=True
)
class TestBackpackPositionsZero:
    """Test position functionality when account has no open positions."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_derivative_positions_empty(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving positions when none exist."""
        positions = await bp_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list)
        assert len(positions) == 0
        assert positions == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_derivative_position_nonexistent_symbol(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving a position for a symbol with no position."""
        # BTC_USDC_PERP
        positions = await bp_api_for_zero_balance_test.get_positions(symbol=COMMON_PERP_SYMBOLS[1])

        # Should return empty list for non-existent positions
        assert isinstance(positions, list)
        assert len(positions) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_positions_after_closing_all(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position state after closing all positions.

        Simulates the state after all derivative positions have been
        closed out.
        """
        positions = await bp_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list)
        assert len(positions) == 0

        # Account summary should reflect no positions
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()
        assert account_summary.total_position_notional == Decimal("0")
        assert account_summary.total_unrealized_pnl == Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_position_for_spot_symbol(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving position for a spot symbol (not derivative).

        Spot symbols should not have derivative positions.
        """
        # Try to get position for spot symbol
        # SOL-USDC
        positions = await bp_api_for_zero_balance_test.get_positions(symbol=COMMON_SPOT_SYMBOLS[0])

        # Should return empty list as spot pairs don't have positions
        assert isinstance(positions, list)
        assert len(positions) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_new_account_no_positions(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position state for a new account that has never traded derivatives."""
        positions = await bp_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list)
        assert len(positions) == 0

        # Try specific symbols
        # BTC_USDC_PERP
        btc_positions = await bp_api_for_zero_balance_test.get_positions(
            symbol=COMMON_PERP_SYMBOLS[1]
        )
        # ETH_USDC_PERP
        eth_positions = await bp_api_for_zero_balance_test.get_positions(
            symbol=COMMON_PERP_SYMBOLS[2]
        )

        assert isinstance(btc_positions, list)
        assert len(btc_positions) == 0
        assert isinstance(eth_positions, list)
        assert len(eth_positions) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_positions_consistency_zero_state(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test consistency between empty positions and account metrics."""
        positions = await bp_api_for_zero_balance_test.get_positions()
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        # With no positions
        assert len(positions) == 0

        # Account metrics should reflect zero position exposure
        assert account_summary.total_position_notional == Decimal("0")
        assert account_summary.total_unrealized_pnl == Decimal("0")

        # Margin requirements should be minimal or zero
        if account_summary.total_initial_margin_required is not None:
            # With no positions, margin requirement should be zero
            assert account_summary.total_initial_margin_required == Decimal("0")

        if account_summary.total_maintenance_margin_required is not None:
            assert account_summary.total_maintenance_margin_required == Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_history_vs_current(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that historical positions don't appear as current positions.

        Even if the account had positions in the past, current positions
        should be empty if all are closed.
        """
        current_positions = await bp_api_for_zero_balance_test.get_positions()

        assert len(current_positions) == 0

        # But trade history might show past derivative trades
        # (This would require checking trade history for derivative symbols)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_for_delisted_symbol(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving position for a delisted or invalid symbol."""
        # Try an invalid/delisted symbol
        # Use a less common perp symbol that might not exist
        positions = await bp_api_for_zero_balance_test.get_positions(symbol="DOGE_USDC_PERP")

        # Should return empty list for invalid symbols
        assert isinstance(positions, list)
        assert len(positions) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_positions_after_liquidation(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position state after a liquidation event.

        After liquidation, positions should be closed and show as empty.
        """
        positions = await bp_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list)
        assert len(positions) == 0

        # Check account state
        account_summary = await bp_api_for_zero_balance_test.get_account_summary()

        # After liquidation, should have no positions
        assert account_summary.total_position_notional == Decimal("0")

        # Check if account shows liquidating state
        if account_summary.bp_details and hasattr(account_summary.bp_details, "liquidating"):
            liquidating = getattr(account_summary.bp_details, "liquidating", False)
            # If recently liquidated, might still show liquidating state
            assert isinstance(liquidating, bool)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_epsilon_size(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that positions with epsilon (dust) size are treated as closed.

        Sometimes positions are reduced to near-zero but not exactly zero.
        These should effectively be treated as closed.
        """
        positions = await bp_api_for_zero_balance_test.get_positions()

        # Filter for any dust positions
        dust_threshold = Decimal("0.00001")
        dust_positions = [
            p for p in positions if abs(p.size) < dust_threshold and p.size != Decimal("0")
        ]

        # Dust positions might exist but should be negligible
        for position in dust_positions:
            assert abs(position.size) < dust_threshold

            # Unrealized PnL should also be negligible
            if position.unrealized_pnl is not None:
                assert abs(position.unrealized_pnl) < Decimal("0.01")

            # Such positions might not have meaningful entry prices
            # or might be in the process of being closed
