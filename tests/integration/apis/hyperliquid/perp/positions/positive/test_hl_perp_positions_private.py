"""Integration tests for Hyperliquid private position endpoints.

This module focuses specifically on testing the DerivativePosition model pipeline
through Hyperliquid's private /exchange endpoints with EIP-712 authentication.
Tests validate complete data transformation for position state-changing operations.

Model Focus: DerivativePosition (Write Operations)
- Tests position-affecting order operations
- Tests leverage management operations (when implemented)
- Tests position closure operations
- Validates EIP-712 cryptographic authentication
- Comprehensive error handling for position management operations

Authentication: EIP-712 signing for all /exchange endpoint operations
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    PlaceOrderArgs,
)
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from tests.integration.apis.hyperliquid.shared.test_helpers import (
    HyperliquidTestHelpers,
    get_minimal_test_quantity,
)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/positions/positive"], indirect=True
)
class TestHyperliquidPerpPositionsPrivate:
    """Comprehensive private position integration tests for /exchange endpoint operations.

    This class tests only /exchange endpoint operations (signed with EIP-712) that affect
    positions:
    - Position-affecting order operations (place_order that opens/modifies positions)
    - Position closure operations
    - Leverage management operations (when implemented)

    These operations require cryptographic authentication and modify position state.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_opening_order_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful position opening through place_order() with comprehensive validation.

        This test validates the complete pipeline from EIP-712 authenticated order placement
        to position creation and DerivativePosition model validation.
        Uses dynamic symbol discovery to ensure test works with available assets.
        """
        # Get available trading symbols from exchange (fail-fast approach)
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=1
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations."
            )

        test_symbol = available_symbols[0]

        # Get minimal viable order size using real market data
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Define order parameters to open a position
        place_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,  # Market order for immediate position opening
            quantity=minimal_quantity,
            time_in_force=TimeInForce.IOC,  # Immediate or cancel for quick execution
        )

        # Execute the order placement
        placed_order = await hl_api_for_test_env.place_order(place_args)

        # Validate order was placed successfully
        assert placed_order.exchange_order_id is not None, "Order should have exchange-generated ID"

        # Get positions to validate position was created/modified
        positions = await hl_api_for_test_env.get_positions()

        # Find the position for the traded asset
        test_position = None
        for position in positions:
            if position.symbol == test_symbol:
                test_position = position
                break

        # If position was created/modified, validate it
        if test_position is not None:
            assert isinstance(test_position, DerivativePosition), (
                "Position should be DerivativePosition instance"
            )

            # Validate core position fields
            assert test_position.exchange == "hyperliquid", (
                f"Position.exchange should be 'hyperliquid', got {test_position.exchange}"
            )
            assert test_position.symbol == test_symbol, (
                f"Position symbol should match traded asset, got {test_position.symbol}"
            )

            # Validate Decimal precision for financial fields
            assert isinstance(test_position.size, Decimal), (
                f"size must be Decimal, got {type(test_position.size)}"
            )
            if test_position.entry_price is not None:
                assert isinstance(test_position.entry_price, Decimal), (
                    f"entry_price must be Decimal, got {type(test_position.entry_price)}"
                )
            if test_position.unrealized_pnl is not None:
                assert isinstance(test_position.unrealized_pnl, Decimal), (
                    f"unrealized_pnl must be Decimal, got {type(test_position.unrealized_pnl)}"
                )

            # Validate business logic constraints
            assert test_position.size != Decimal("0"), (
                f"Position size should be non-zero after opening trade, got {test_position.size}"
            )
            assert test_position.entry_price is not None and test_position.entry_price > Decimal(
                "0"
            ), f"entry_price must be positive, got {test_position.entry_price}"

            # Validate position side matches order side
            if test_position.size > Decimal("0"):
                assert placed_order.side == OrderSide.BUY, (
                    "Long position should result from BUY order"
                )
            elif test_position.size < Decimal("0"):
                assert placed_order.side == OrderSide.SELL, (
                    "Short position should result from SELL order"
                )

        # Clean up - attempt to close position if one was opened
        if test_position is not None and test_position.size != Decimal("0"):
            # Place opposite order to close position
            close_side = OrderSide.SELL if test_position.size > Decimal("0") else OrderSide.BUY
            close_quantity = abs(test_position.size)

            close_args = PlaceOrderArgs(
                symbol=test_symbol,
                side=close_side,
                order_type=OrderType.MARKET,
                quantity=close_quantity,
                time_in_force=TimeInForce.IOC,
            )

            try:
                await hl_api_for_test_env.place_order(close_args)
            except APIError:
                # If close fails, that's acceptable for test cleanup
                pass

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_closure_order_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful position closure through place_order() with position validation.

        This validates the complete position lifecycle: open → verify → close → verify closure.
        """
        # Get available trading symbols from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=1
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations."
            )

        test_symbol = available_symbols[0]

        # Get minimal viable order size
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Step 1: First open a position
        open_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,
            time_in_force=TimeInForce.IOC,
        )

        open_order = await hl_api_for_test_env.place_order(open_args)
        assert open_order.exchange_order_id is not None, "Opening order should succeed"

        # Step 2: Verify position was opened
        positions_after_open = await hl_api_for_test_env.get_positions()
        test_position_after_open = None
        for position in positions_after_open:
            if position.symbol == test_symbol and position.size != Decimal("0"):
                test_position_after_open = position
                break

        # Step 3: Close the position if it was opened
        if test_position_after_open is not None:
            close_side = (
                OrderSide.SELL if test_position_after_open.size > Decimal("0") else OrderSide.BUY
            )
            close_quantity = abs(test_position_after_open.size)

            close_args = PlaceOrderArgs(
                symbol=test_symbol,
                side=close_side,
                order_type=OrderType.MARKET,
                quantity=close_quantity,
                time_in_force=TimeInForce.IOC,
            )

            close_order = await hl_api_for_test_env.place_order(close_args)
            assert close_order.exchange_order_id is not None, "Closing order should succeed"

            # Step 4: Verify position was closed or reduced
            positions_after_close = await hl_api_for_test_env.get_positions()
            test_position_after_close = None
            for position in positions_after_close:
                if position.symbol == test_symbol:
                    test_position_after_close = position
                    break

            # Position should either be gone or have zero/reduced size
            if test_position_after_close is not None:
                assert abs(test_position_after_close.size) < abs(test_position_after_open.size), (
                    f"Position should be reduced after close: "
                    f"before={test_position_after_open.size}, "
                    f"after={test_position_after_close.size}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_affecting_order_insufficient_margin_error(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position-affecting order with insufficient margin error.

        This validates that our HyperliquidErrorMapper correctly maps margin-related errors
        when attempting to open positions that exceed available margin.
        """
        # Get available trading symbols from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=1
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations."
            )

        test_symbol = available_symbols[0]

        # Get unreasonably large quantity from helper (not hardcoded)
        large_quantity = await HyperliquidTestHelpers.get_unreasonably_large_quantity(
            hl_api_for_test_env, test_symbol
        )

        # Create order with unrealistically large quantity to trigger insufficient margin
        large_position_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=large_quantity,
            time_in_force=TimeInForce.IOC,
        )

        # Should raise APIError with margin-related code
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(large_position_args)

        # Validate error mapping
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.MAX_POSITION_EXCEEDED.value,
        ], f"Should map to margin-related error code, got {api_error.code}"

        # Check for common margin-related error phrases
        message_lower = api_error.message.lower()
        assert any(
            phrase in message_lower
            for phrase in ["insufficient", "margin", "balance", "risk", "limit"]
        ), f"Error message should indicate margin/risk issue: {api_error.message}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position operations with edge cases around decimal precision.

        This validates handling of very small position sizes, dust amounts,
        and precision edge cases that might occur in real trading.
        """
        # Get available trading symbols from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=1
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations."
            )

        test_symbol = available_symbols[0]

        # Get market constraints to determine smallest valid quantity
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_test_env, test_symbol
        )
        # Use minimum exchange step size as the small quantity
        small_quantity = constraints["step_size"]

        # Test very small position size
        small_position_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=small_quantity,
            time_in_force=TimeInForce.IOC,
        )

        try:
            # Attempt to open small position
            await hl_api_for_test_env.place_order(small_position_args)

            # If successful, validate precision is maintained in resulting position
            positions = await hl_api_for_test_env.get_positions()
            test_position = None
            for position in positions:
                if position.symbol == test_symbol and position.size != Decimal("0"):
                    test_position = position
                    break

            if test_position is not None:
                # Validate that small quantities maintain proper decimal representation
                size_str = str(test_position.size)
                assert "E" not in size_str.upper() or "E-" in size_str.upper(), (
                    f"Scientific notation should be negative exponent if used: {size_str}"
                )

                # Validate precision constraints
                assert isinstance(test_position.size, Decimal), (
                    f"Position size must be Decimal, got {type(test_position.size)}"
                )

                # Clean up small position
                if abs(test_position.size) > Decimal("0"):
                    close_side = (
                        OrderSide.SELL if test_position.size > Decimal("0") else OrderSide.BUY
                    )
                    close_args = PlaceOrderArgs(
                        symbol=test_symbol,
                        side=close_side,
                        order_type=OrderType.MARKET,
                        quantity=abs(test_position.size),
                        time_in_force=TimeInForce.IOC,
                    )

                    try:
                        await hl_api_for_test_env.place_order(close_args)
                    except APIError:
                        # If close fails, that's acceptable for test cleanup
                        pass

        except APIError as e:
            # If exchange rejects due to minimum position size, that's also valid behavior
            if any(phrase in e.message.lower() for phrase in ["minimum", "size", "precision"]):
                pytest.skip(f"Exchange has minimum position size requirements: {e.message}")
            else:
                raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_invalid_symbol_error(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position-affecting order with invalid asset/symbol error."""
        # Get minimal viable order size for a valid symbol to understand exchange constraints
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=1
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test invalid symbol handling."
            )

        test_symbol = available_symbols[0]
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Create order with non-existent asset to open position
        invalid_asset_args = PlaceOrderArgs(
            symbol="INVALID_POSITION_ASSET",  # Non-existent asset
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,  # Use real exchange quantity constraints
            time_in_force=TimeInForce.IOC,
        )

        # Should raise APIError with appropriate error code
        with pytest.raises(APIError) as exc_info:
            await hl_api_for_test_env.place_order(invalid_asset_args)

        # Validate error structure
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INVALID_SYMBOL.value,
            APIErrorCode.SYMBOL_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], f"Should map to symbol-related error code, got {api_error.code}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_position_operations_consistency(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test multiple position operations to validate consistency and state management.

        This validates that multiple operations on the same position maintain consistent
        DerivativePosition model state and proper decimal precision throughout.
        """
        # Get available trading symbols from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=1
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations."
            )

        test_symbol = available_symbols[0]

        # Get minimal viable order size
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Calculate additional position size (50% of minimal for testing accumulation)
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_test_env, test_symbol
        )
        step_size = constraints["step_size"]

        # Round additional quantity to valid step size
        from decimal import ROUND_UP

        additional_steps = (minimal_quantity * Decimal("0.5") / step_size).quantize(
            Decimal("1"), rounding=ROUND_UP
        )
        additional_quantity = max(
            additional_steps * step_size, step_size
        )  # Ensure at least one step

        # Step 1: Open initial position
        initial_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,
            time_in_force=TimeInForce.IOC,
        )

        initial_order = await hl_api_for_test_env.place_order(initial_args)
        assert initial_order.exchange_order_id is not None, "Initial order should succeed"

        # Step 2: Add to position
        add_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,  # Same side to add to position
            order_type=OrderType.MARKET,
            quantity=additional_quantity,
            time_in_force=TimeInForce.IOC,
        )

        add_order = await hl_api_for_test_env.place_order(add_args)
        assert add_order.exchange_order_id is not None, "Add order should succeed"

        # Step 3: Verify position reflects both operations
        positions_after_add = await hl_api_for_test_env.get_positions()
        test_position = None
        for position in positions_after_add:
            if position.symbol == test_symbol and position.size != Decimal("0"):
                test_position = position
                break

        if test_position is not None:
            # Validate position accumulation (should be at least the initial quantity)
            assert test_position.size >= minimal_quantity, (
                f"Position should reflect at least initial size: {test_position.size} >= {minimal_quantity}"
            )

            # Validate decimal precision maintained
            assert isinstance(test_position.size, Decimal), "Size must remain Decimal"
            assert isinstance(test_position.entry_price, Decimal), "Entry price must remain Decimal"

            # Step 4: Clean up - close entire position
            close_args = PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=abs(test_position.size),
                time_in_force=TimeInForce.IOC,
            )

            try:
                await hl_api_for_test_env.place_order(close_args)
            except APIError:
                # If close fails, that's acceptable for test cleanup
                pass
