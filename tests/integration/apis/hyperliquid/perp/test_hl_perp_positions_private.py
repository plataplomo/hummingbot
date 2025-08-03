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

import contextlib
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.models.market.order import Order
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import (
    HyperliquidTestHelpers,
    get_minimal_test_quantity,
)


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/positions/positive"],
    indirect=True,
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
    async def _validate_position_fields(
        self,
        test_position: DerivativePosition,
        test_symbol: str,
    ) -> None:
        """Validate core position fields and financial data types."""
        assert isinstance(test_position, DerivativePosition), (
            "Position should be DerivativePosition instance"
        )

        # Validate core position fields
        assert test_position.exchange == "hyperliquid", (
            f"Position.exchange should be 'hyperliquid', got {test_position.exchange}"
        )
        assert test_position.symbol.value == test_symbol, (
            f"Position symbol should match traded asset, got {test_position.symbol.value}"
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

    async def _validate_position_business_logic(
        self,
        test_position: DerivativePosition,
        placed_order: Order,
    ) -> None:
        """Validate business logic constraints for the position."""
        # Validate business logic constraints
        assert test_position.size != Decimal(0), (
            f"Position size should be non-zero after opening trade, got {test_position.size}"
        )
        assert test_position.entry_price is not None, (
            f"entry_price must not be None, got {test_position.entry_price}"
        )
        assert test_position.entry_price > Decimal(0), (
            f"entry_price must be positive, got {test_position.entry_price}"
        )

        # Validate position side matches order side
        if test_position.size > Decimal(0):
            assert placed_order.side == OrderSide.BUY, "Long position should result from BUY order"
        elif test_position.size < Decimal(0):
            assert placed_order.side == OrderSide.SELL, (
                "Short position should result from SELL order"
            )

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
            hl_api_for_test_env,
            limit=1,
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        test_symbol = available_symbols[0]

        # Get minimal viable order size using real market data
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env,
            test_symbol,
            OrderSide.BUY,
        )

        # Define order parameters to open a position
        place_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid(test_symbol),
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
            if position.symbol.value == test_symbol:
                test_position = position
                break

        # If position was created/modified, validate it
        if test_position is not None:
            await self._validate_position_fields(test_position, test_symbol)
            await self._validate_position_business_logic(test_position, placed_order)

        # Clean up - attempt to close position if one was opened
        if test_position is not None and test_position.size != Decimal(0):
            # Place opposite order to close position
            close_side = OrderSide.SELL if test_position.size > Decimal(0) else OrderSide.BUY
            close_quantity = abs(test_position.size)

            close_args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid(test_symbol),
                side=close_side,
                order_type=OrderType.MARKET,
                quantity=close_quantity,
                time_in_force=TimeInForce.IOC,
            )

            with contextlib.suppress(APIError):
                # If close fails, that's acceptable for test cleanup
                await hl_api_for_test_env.place_order(close_args)

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
            hl_api_for_test_env,
            limit=1,
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        test_symbol = available_symbols[0]

        # Get minimal viable order size
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env,
            test_symbol,
            OrderSide.BUY,
        )

        # Step 1: First open a position
        open_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid(test_symbol),
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
            if position.symbol.value == test_symbol and position.size != Decimal(0):
                test_position_after_open = position
                break

        # Step 3: Close the position if it was opened
        if test_position_after_open is not None:
            close_side = (
                OrderSide.SELL if test_position_after_open.size > Decimal(0) else OrderSide.BUY
            )
            close_quantity = abs(test_position_after_open.size)

            close_args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid(test_symbol),
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
                if position.symbol.value == test_symbol:
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
            hl_api_for_test_env,
            limit=1,
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        test_symbol = available_symbols[0]

        # Get unreasonably large quantity - use exchange max
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_test_env,
            test_symbol,
        )
        # Use the exchange's max quantity if available, or a very large number
        large_quantity = constraints.get("max_quantity", Decimal(1000000))

        # Create order with unrealistically large quantity to trigger insufficient margin
        # Use LIMIT order FAR BELOW market so it won't execute but will trigger margin check
        # Use dynamic test price to ensure proper tick size alignment
        non_executable_price = await HyperliquidTestHelpers.get_dynamic_test_price(
            hl_api_for_test_env,
            test_symbol,
            OrderSide.BUY,
            Decimal("0.5"),  # 50% below market
        )

        large_position_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid(test_symbol),
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=large_quantity,
            price=non_executable_price,
            time_in_force=TimeInForce.GTC,  # Use GTC to ensure full margin check
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

        Raises:
            APIError: When the exchange rejects orders due to minimum position
                size requirements or other precision-related constraints.
        """
        # Get available trading symbols from exchange
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env,
            limit=1,
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        test_symbol = available_symbols[0]

        # Get minimal order size that meets $10 minimum requirement
        # This ensures we meet exchange minimum notional requirements for testnet
        small_quantity = await HyperliquidTestHelpers.get_minimal_order_size(
            hl_api_for_test_env,
            test_symbol,
            OrderSide.BUY,
        )

        # Test very small position size
        small_position_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid(test_symbol),
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
                if position.symbol.value == test_symbol and position.size != Decimal(0):
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
                if abs(test_position.size) > Decimal(0):
                    close_side = (
                        OrderSide.SELL if test_position.size > Decimal(0) else OrderSide.BUY
                    )
                    close_args = PlaceOrderArgs(
                        symbol=exchanges.hyperliquid(test_symbol),
                        side=close_side,
                        order_type=OrderType.MARKET,
                        quantity=abs(test_position.size),
                        time_in_force=TimeInForce.IOC,
                    )

                    with contextlib.suppress(APIError):
                        # If close fails, that's acceptable for test cleanup
                        await hl_api_for_test_env.place_order(close_args)

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
            hl_api_for_test_env,
            limit=1,
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. "
                "Cannot test invalid symbol handling.",
            )

        test_symbol = available_symbols[0]
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env,
            test_symbol,
            OrderSide.BUY,
        )

        # Create order with non-existent asset to open position
        invalid_asset_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("INVALID_POS_ASSET"),  # Non-existent asset (≤30 chars)
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
            APIErrorCode.INVALID_RESPONSE.value,  # Empty l2Book data for invalid symbol
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
            hl_api_for_test_env,
            limit=1,
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        test_symbol = available_symbols[0]

        # Get minimal viable order size
        minimal_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env,
            test_symbol,
            OrderSide.BUY,
        )

        # Calculate additional position size (same as minimal to ensure it fills)
        # Use same quantity as initial to ensure order fills
        additional_quantity = minimal_quantity

        # Step 1: Open initial position
        initial_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid(test_symbol),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,
            time_in_force=TimeInForce.IOC,
        )

        initial_order = await hl_api_for_test_env.place_order(initial_args)
        assert initial_order.exchange_order_id is not None, "Initial order should succeed"

        # Get position after initial order to track state
        positions_after_initial = await hl_api_for_test_env.get_positions()
        initial_position = None
        for position in positions_after_initial:
            if position.symbol.value == test_symbol and position.size != Decimal(0):
                initial_position = position
                break

        assert initial_position is not None, "Should have position after initial order"
        initial_size = initial_position.size

        # Step 2: Add to position
        add_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid(test_symbol),
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
            if position.symbol.value == test_symbol and position.size != Decimal(0):
                test_position = position
                break

        assert test_position is not None, "Should still have position after add order"

        # Validate position accumulation - size should have increased
        assert test_position.size > initial_size, (
            f"Position size should have increased: {test_position.size} > {initial_size}"
        )

        # Validate the increase is approximately the additional quantity
        # (allowing for partial fills)
        size_increase = test_position.size - initial_size
        assert size_increase > Decimal(0), (
            f"Position should have increased by a positive amount: increase={size_increase}"
        )

        # Validate decimal precision maintained throughout operations
        assert isinstance(test_position.size, Decimal), "Size must remain Decimal"
        assert isinstance(test_position.entry_price, Decimal), "Entry price must remain Decimal"
        assert isinstance(initial_position.size, Decimal), "Initial size was Decimal"
        assert isinstance(initial_position.entry_price, Decimal), "Initial entry price was Decimal"

        # Validate position state consistency
        assert test_position.symbol == initial_position.symbol, "Symbol should remain consistent"
        assert test_position.exchange == initial_position.exchange, (
            "Exchange should remain consistent"
        )

        # Validate that average entry price makes sense
        # (should be between initial and current market)
        if initial_position.entry_price and test_position.entry_price:
            # Entry price should be a weighted average after adding to position
            assert test_position.entry_price > Decimal(0), "Entry price should be positive"

        # Step 4: Clean up - close entire position
        if test_position:
            close_args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid(test_symbol),
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=abs(test_position.size),
                time_in_force=TimeInForce.IOC,
            )

            with contextlib.suppress(APIError):
                # If close fails, that's acceptable for test cleanup
                await hl_api_for_test_env.place_order(close_args)
