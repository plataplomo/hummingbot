"""Integration tests for Backpack position functionality with open positions.

Tests position retrieval and management when the account has active
derivative positions.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import GetMarketsArgs, PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models import BackpackPositionDetails, DerivativePosition
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    BREAK_EVEN_PRICE_TOLERANCE_PERCENT,
    DEFAULT_TEST_SYMBOL_PERP,
    PNL_TOLERANCE,
    get_minimal_order_size,
    is_valid_margin_fraction,
    is_within_ratio_bounds,
    is_within_tolerance,
    validate_pnl_direction,
    wait_for_condition,
)


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.positions,
    pytest.mark.positive_positions,
    pytest.mark.timing,
]

logger = get_logger(__name__)


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/positions_positive"],
    indirect=True,
)
class TestBackpackPositionsPositive:
    """Test position functionality when account has open positions."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_derivative_positions_multiple(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving multiple open derivative positions."""
        positions = await bp_api_for_test_env.get_positions()

        assert isinstance(positions, list)

        if len(positions) > 0:
            # Verify each position
            for position in positions:
                assert isinstance(position, DerivativePosition)
                assert position.exchange == "backpack"

                # Required fields
                assert isinstance(position.symbol, str)
                assert len(position.symbol) > 0
                assert isinstance(position.side, OrderSide)
                assert isinstance(position.size, Decimal)
                assert position.size != Decimal(0)

                # Entry price should be set for non-zero positions
                if position.size != Decimal(0):
                    assert position.entry_price is not None
                    assert isinstance(position.entry_price, Decimal)
                    assert position.entry_price > Decimal(0)

                # Mark price
                assert position.mark_price is not None
                assert isinstance(position.mark_price, Decimal)
                assert position.mark_price > Decimal(0)

                # PnL
                assert isinstance(position.unrealized_pnl, Decimal)
                assert isinstance(position.realized_pnl, Decimal)

                # Timestamp
                assert isinstance(position.timestamp, datetime)

                # Backpack details
                assert position.bp_details is not None
                assert isinstance(position.bp_details, BackpackPositionDetails)

    async def _get_test_symbol(self, bp_api: BackpackAPI, default_symbol: str) -> str:
        """Get a valid perpetual symbol for testing.

        Returns:
            str: A valid perpetual symbol for testing.

        Raises:
            AssertionError: If no perpetual symbols are available for testing.
        """
        markets = await bp_api.get_markets(GetMarketsArgs())
        perp_symbols = [m.symbol for m in markets if "_PERP" in m.symbol]

        if default_symbol not in perp_symbols:
            # Use first available perpetual symbol instead
            if perp_symbols:
                symbol = perp_symbols[0]
                logger.info(
                    "using_available_perp_symbol",
                    symbol=symbol,
                    message="Using available perp symbol",
                )
                return symbol
            raise AssertionError("No perpetual symbols available for testing")

        return default_symbol

    async def _check_position_exists(
        self,
        bp_api: BackpackAPI,
        symbol: str,
    ) -> tuple[bool, list[DerivativePosition]]:
        """Check if a position exists for the given symbol.

        Returns:
            tuple[bool, list[DerivativePosition]]: A tuple containing whether position
                exists and the list of positions.

        Raises:
            APIError: If API call fails with non-symbol-not-found errors.
        """
        try:
            positions = await bp_api.get_positions(symbol=symbol)
            assert isinstance(positions, list)
            return len(positions) > 0, positions
        except APIError as e:
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                # No position exists for this symbol
                return False, []
            raise

    async def _open_test_position(self, bp_api: BackpackAPI, symbol: str) -> None:
        """Open a test position for the given symbol.

        Raises:
            ValueError: If cannot determine market price for the symbol.
        """
        side = OrderSide.BUY  # Open a long position

        # Get current market price for size calculation
        ticker = await bp_api.get_ticker(symbol)
        market_price = ticker.price or ticker.ask
        if market_price is None:
            raise ValueError(f"Cannot determine market price for {symbol}")

        # Get minimal order size for market order (will open position immediately)
        test_quantity = await get_minimal_order_size(bp_api, symbol, side, market_price)

        args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=test_quantity,
            time_in_force=TimeInForce.IOC,
        )

        # Place market order to open position
        await bp_api.place_order(args)

        # Wait for position to be reflected
        async def position_exists() -> bool:
            positions = await bp_api.get_positions(symbol=symbol)
            return len(positions) > 0 and positions[0].size != Decimal(0)

        await wait_for_condition(
            position_exists,
            timeout_seconds=5.0,
            poll_interval=0.1,
            message=f"Position for {symbol} was not created",
        )

    async def _close_position(self, bp_api: BackpackAPI, symbol: str) -> None:
        """Close any open positions for the given symbol."""
        try:
            # Get current positions to determine close side
            current_positions = await bp_api.get_positions(symbol=symbol)
            for position in current_positions:
                if position.size != Decimal(0):
                    # Close position with opposite side
                    close_side = OrderSide.SELL if position.side == OrderSide.BUY else OrderSide.BUY
                    close_quantity = abs(position.size)

                    close_args = PlaceOrderArgs(
                        symbol=symbol,
                        side=close_side,
                        order_type=OrderType.MARKET,
                        quantity=close_quantity,
                        time_in_force=TimeInForce.IOC,
                    )

                    await bp_api.place_order(close_args)
        except (APIError, ValueError, TypeError) as cleanup_error:
            pytest.fail(
                f"Failed to clean up position for {symbol}: {cleanup_error}. "
                "Position cleanup is critical to prevent test contamination.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_derivative_position_by_symbol(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving a specific position by symbol.

        Dynamically creates position if needed.

        Raises:
            AssertionError: If position validation fails.
            APIError: If API call fails or symbol is not found.
        """
        symbol = DEFAULT_TEST_SYMBOL_PERP
        opened_position = False

        try:
            # Get valid test symbol
            symbol = await self._get_test_symbol(bp_api_for_test_env, symbol)

            # Check if position already exists
            position_exists, positions = await self._check_position_exists(
                bp_api_for_test_env,
                symbol,
            )

            if not position_exists:
                # No position exists, create one for testing
                await self._open_test_position(bp_api_for_test_env, symbol)
                opened_position = True

                # Re-fetch positions
                positions = await bp_api_for_test_env.get_positions(symbol=symbol)

            # Validate the position
            assert len(positions) > 0, f"Should have at least one position for {symbol}"

            # All returned positions should be for the specified symbol
            for position in positions:
                assert isinstance(position, DerivativePosition)
                assert position.symbol == symbol
                assert position.exchange == "backpack"

        except APIError as e:
            # Handle case where symbol doesn't exist or other API errors
            if e.code == APIErrorCode.SYMBOL_NOT_FOUND.value:
                raise AssertionError(f"Symbol {symbol} not found: {e.message}") from e
            raise

        finally:
            # Clean up: close any position we opened
            if opened_position:
                await self._close_position(bp_api_for_test_env, symbol)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_long_side_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test long position side and size consistency."""
        positions = await bp_api_for_test_env.get_positions()

        long_positions = [p for p in positions if p.side == OrderSide.BUY]

        for position in long_positions:
            # Long positions should have positive size
            assert position.size > Decimal(0)

            # Entry price should be set
            assert position.entry_price is not None
            assert position.entry_price > Decimal(0)

            # Unrealized PnL calculation check
            if position.mark_price and position.entry_price and position.unrealized_pnl is not None:
                # Validate PnL direction matches expectations
                assert validate_pnl_direction(
                    position.unrealized_pnl,
                    OrderSide.BUY,
                    position.entry_price,
                    position.mark_price,
                ), (
                    f"PnL direction incorrect for long position: "
                    f"PnL={position.unrealized_pnl}, entry={position.entry_price}, "
                    f"mark={position.mark_price}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_short_side_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test short position side and size consistency."""
        positions = await bp_api_for_test_env.get_positions()

        short_positions = [p for p in positions if p.side == OrderSide.SELL]

        for position in short_positions:
            # Short positions should have negative size
            assert position.size < Decimal(0)

            # Entry price should be set
            assert position.entry_price is not None
            assert position.entry_price > Decimal(0)

            # Unrealized PnL calculation check
            if position.mark_price and position.entry_price and position.unrealized_pnl is not None:
                # Validate PnL direction matches expectations
                assert validate_pnl_direction(
                    position.unrealized_pnl,
                    OrderSide.SELL,
                    position.entry_price,
                    position.mark_price,
                ), (
                    f"PnL direction incorrect for short position: "
                    f"PnL={position.unrealized_pnl}, entry={position.entry_price}, "
                    f"mark={position.mark_price}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_margin_requirements(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position margin requirement calculations."""
        positions = await bp_api_for_test_env.get_positions()

        for position in positions:
            if position.bp_details:
                # Check IMF and MMF values
                if hasattr(position.bp_details, "imf_base"):
                    imf = getattr(position.bp_details, "imf_base", None)
                    if imf is not None:
                        assert isinstance(imf, Decimal)
                        assert is_valid_margin_fraction(imf)

                if hasattr(position.bp_details, "mmf_base"):
                    mmf = getattr(position.bp_details, "mmf_base", None)
                    if mmf is not None:
                        assert isinstance(mmf, Decimal)
                        assert is_valid_margin_fraction(mmf)

                # IMF should be >= MMF
                if hasattr(position.bp_details, "imf_base") and hasattr(
                    position.bp_details,
                    "mmf_base",
                ):
                    imf = getattr(position.bp_details, "imf_base", None)
                    mmf = getattr(position.bp_details, "mmf_base", None)
                    if imf is not None and mmf is not None:
                        assert imf >= mmf

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_liquidation_price(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position liquidation price calculations."""
        positions = await bp_api_for_test_env.get_positions()

        for position in positions:
            if position.liquidation_price is not None:
                assert isinstance(position.liquidation_price, Decimal)

                # Some positions may have liquidation_price of 0, which can happen
                # for positions with very low leverage or high collateral ratios
                if position.liquidation_price > Decimal(0):
                    # Liquidation price should make sense relative to entry
                    if position.entry_price:
                        if position.side == OrderSide.BUY:
                            # Long position liquidates below entry
                            assert position.liquidation_price < position.entry_price
                        else:
                            # Short position liquidates above entry
                            assert position.liquidation_price > position.entry_price
                elif position.liquidation_price == Decimal(0):
                    # Zero liquidation price can be valid for very low-leverage positions
                    # or positions with very high collateral ratios
                    assert position.size != Decimal(0), (
                        "Position with zero liquidation price should have non-zero size"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_cumulative_funding(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cumulative funding payment tracking."""
        positions = await bp_api_for_test_env.get_positions()

        for position in positions:
            if position.bp_details and hasattr(position.bp_details, "cumulative_funding"):
                funding = getattr(position.bp_details, "cumulative_funding", None)
                if funding is not None:
                    assert isinstance(funding, Decimal)
                    # Funding can be positive or negative

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_break_even_price(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position break-even price calculations."""
        positions = await bp_api_for_test_env.get_positions()

        for position in positions:
            if position.bp_details and hasattr(position.bp_details, "break_even_price"):
                break_even = getattr(position.bp_details, "break_even_price", None)
                if break_even is not None:
                    assert isinstance(break_even, Decimal)
                    assert break_even > Decimal(0)

                    # Break-even should be close to entry price
                    # (differs by fees and funding)
                    if position.entry_price:
                        assert is_within_tolerance(
                            break_even,
                            position.entry_price,
                            tolerance_percent=BREAK_EVEN_PRICE_TOLERANCE_PERCENT,
                        ), (
                            f"Break-even price too far from entry: "
                            f"break_even={break_even}, entry={position.entry_price}"
                        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_notional_value(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position notional value calculations."""
        positions = await bp_api_for_test_env.get_positions()

        for position in positions:
            # Calculate expected notional
            if position.mark_price:
                expected_notional = abs(position.size * position.mark_price)

                # Check if bp_details has notional value
                if position.bp_details and hasattr(position.bp_details, "net_exposure_notional"):
                    notional = getattr(position.bp_details, "net_exposure_notional", None)
                    if notional is not None:
                        assert isinstance(notional, Decimal)
                        assert notional >= Decimal(0)

                        # Should be close to calculated value
                        if expected_notional > Decimal(0):
                            assert is_within_ratio_bounds(notional, expected_notional), (
                                f"Notional value mismatch: "
                                f"actual={notional}, expected={expected_notional}"
                            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_positions_consistency_with_account_summary(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test consistency between positions and account summary."""
        positions = await bp_api_for_test_env.get_positions()
        account_summary = await bp_api_for_test_env.get_account_summary()

        # Calculate total unrealized PnL from positions
        total_unrealized_pnl = Decimal(
            sum(p.unrealized_pnl for p in positions if p.unrealized_pnl is not None),
        )

        # Calculate total notional from positions
        total_notional = Decimal(
            sum(abs(p.size * p.mark_price) for p in positions if p.mark_price is not None),
        )

        # Account summary should reflect position data
        if len(positions) > 0:
            # Total position notional should match
            if (
                account_summary.total_position_notional is not None
                and account_summary.total_position_notional > Decimal(0)
                and total_notional > Decimal(0)
            ):
                assert is_within_ratio_bounds(
                    account_summary.total_position_notional,
                    total_notional,
                ), (
                    f"Position notional mismatch: "
                    f"summary={account_summary.total_position_notional}, "
                    f"calculated={total_notional}"
                )

            # Unrealized PnL should match
            if (
                total_unrealized_pnl != Decimal(0)
                and account_summary.total_unrealized_pnl is not None
            ):
                assert is_within_tolerance(
                    account_summary.total_unrealized_pnl,
                    total_unrealized_pnl,
                    tolerance=PNL_TOLERANCE,
                ), (
                    f"PnL mismatch: summary={account_summary.total_unrealized_pnl}, "
                    f"calculated={total_unrealized_pnl}"
                )
