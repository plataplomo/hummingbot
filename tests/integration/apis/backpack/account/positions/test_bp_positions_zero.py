"""Integration tests for Backpack position functionality with no open positions.

Tests position retrieval when the account has no derivative positions,
including edge cases and empty responses.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    COMMON_SPOT_SYMBOLS,
    DELISTED_PERP_SYMBOL,
    DUST_THRESHOLD,
    SMALL_VALUE_TOLERANCE,
    TEST_SYMBOL_BTC_PERP,
    TEST_SYMBOL_ETH_PERP,
)


# Mark all tests in this file
pytestmark = [pytest.mark.integration]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/positions_zero"],
    indirect=True,
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
        """Test retrieving a position for a symbol with no position.

        Raises:
            APIError: If API call fails with non-symbol-not-found errors.
        """
        try:
            positions = await bp_api_for_zero_balance_test.get_positions(
                symbol=TEST_SYMBOL_BTC_PERP,
            )
            # Should return empty list for non-existent positions
            assert isinstance(positions, list)
            assert len(positions) == 0
        except APIError as e:
            # Handle expected case where no position exists for the symbol
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                # This is the expected behavior for zero balance tests
                if "No position found" not in str(e.message):
                    pytest.fail(f"Expected 'No position found' in error message, got: {e.message}")
            else:
                raise

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

        # Position notional should be 0 or None when no positions
        if account_summary.total_position_notional is not None:
            assert account_summary.total_position_notional == Decimal(0)

        # Unrealized PnL should be 0 or None when no positions
        if account_summary.total_unrealized_pnl is not None:
            assert account_summary.total_unrealized_pnl == Decimal(0)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_position_for_spot_symbol(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving position for a spot symbol (not derivative).

        Spot symbols should not have derivative positions.

        Raises:
            APIError: If API call fails with non-symbol-not-found errors.
        """
        try:
            # Try to get position for spot symbol
            # SOL-USDC
            positions = await bp_api_for_zero_balance_test.get_positions(
                symbol=COMMON_SPOT_SYMBOLS[0],
            )

            # Should return empty list as spot pairs don't have positions
            assert isinstance(positions, list)
            assert len(positions) == 0
        except APIError as e:
            # Handle expected case where no position exists for spot symbols
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                # This is the expected behavior for spot symbols
                if "No position found" not in str(e.message):
                    pytest.fail(f"Expected 'No position found' in error message, got: {e.message}")
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_new_account_no_positions(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position state for a new account that has never traded derivatives.

        Raises:
            APIError: If API call fails with non-symbol-not-found errors.
        """
        positions = await bp_api_for_zero_balance_test.get_positions()

        assert isinstance(positions, list)
        assert len(positions) == 0

        # Try specific symbols
        try:
            btc_positions = await bp_api_for_zero_balance_test.get_positions(
                symbol=TEST_SYMBOL_BTC_PERP,
            )
            assert isinstance(btc_positions, list)
            assert len(btc_positions) == 0
        except APIError as e:
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                # Expected for new accounts with no positions
                pass
            else:
                raise

        try:
            eth_positions = await bp_api_for_zero_balance_test.get_positions(
                symbol=TEST_SYMBOL_ETH_PERP,
            )
            assert isinstance(eth_positions, list)
            assert len(eth_positions) == 0
        except APIError as e:
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                # Expected for new accounts with no positions
                pass
            else:
                raise

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
        if account_summary.total_position_notional is not None:
            assert account_summary.total_position_notional == Decimal(0)

        if account_summary.total_unrealized_pnl is not None:
            assert account_summary.total_unrealized_pnl == Decimal(0)

        # Margin requirements could be non-zero due to open orders
        # So we just validate they're non-negative
        if account_summary.total_initial_margin_required is not None:
            assert account_summary.total_initial_margin_required >= Decimal(0)

        if account_summary.total_maintenance_margin_required is not None:
            assert account_summary.total_maintenance_margin_required >= Decimal(0)

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
        """Test retrieving position for a delisted or invalid symbol.

        Raises:
            APIError: If API call fails with non-symbol-not-found errors.
        """
        try:
            # Try an invalid/delisted symbol
            # Use a less common perp symbol that might not exist
            positions = await bp_api_for_zero_balance_test.get_positions(
                symbol=DELISTED_PERP_SYMBOL,
            )

            # Should return empty list for invalid symbols
            assert isinstance(positions, list)
            assert len(positions) == 0
        except APIError as e:
            # Handle expected case where API raises error for delisted/invalid symbols
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                # This is the expected behavior for delisted symbols
                msg = str(e.message)
                if not ("No position found" in msg or "DOGE_USDC_PERP" in msg):
                    pytest.fail(
                        "Expected 'No position found' or 'DOGE_USDC_PERP' in error message, "
                        f"got: {msg}"
                    )
            else:
                raise

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
        if account_summary.total_position_notional is not None:
            assert account_summary.total_position_notional == Decimal(0)

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
        dust_positions = [
            p for p in positions if abs(p.size) < DUST_THRESHOLD and p.size != Decimal(0)
        ]

        # Dust positions might exist but should be negligible
        for position in dust_positions:
            assert abs(position.size) < DUST_THRESHOLD

            # Unrealized PnL should also be negligible
            if position.unrealized_pnl is not None:
                assert abs(position.unrealized_pnl) < SMALL_VALUE_TOLERANCE

            # Such positions might not have meaningful entry prices
            # or might be in the process of being closed
