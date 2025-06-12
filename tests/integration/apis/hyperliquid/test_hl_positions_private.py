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

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/positions_private"], indirect=True
)
class TestHyperliquidPositionsPrivate:
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
        Uses testnet asset and small size to manage risk during recording.
        """
        # Define order parameters to open a position (use testnet asset, small size)
        place_args = PlaceOrderArgs(
            symbol="PURP",  # Common testnet asset on Hyperliquid
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,  # Market order for immediate position opening
            quantity=Decimal("0.1"),  # Small size for testnet
            time_in_force=TimeInForce.IOC,  # Immediate or cancel for quick execution
        )

        # Execute the order placement
        placed_order = await hl_api_for_test_env.place_order(place_args)

        # Validate order was placed successfully
        assert placed_order.exchange_order_id is not None, "Order should have exchange-generated ID"

        # Get positions to validate position was created/modified
        positions = await hl_api_for_test_env.get_positions()

        # Find the position for the traded asset
        purp_position = None
        for position in positions:
            if position.symbol == "PURP":
                purp_position = position
                break

        # If position was created/modified, validate it
        if purp_position is not None:
            assert isinstance(purp_position, DerivativePosition), (
                "Position should be DerivativePosition instance"
            )

            # Validate core position fields
            assert purp_position.exchange == "hyperliquid", (
                f"Position.exchange should be 'hyperliquid', got {purp_position.exchange}"
            )
            assert purp_position.symbol == "PURP", (
                f"Position symbol should match traded asset, got {purp_position.symbol}"
            )

            # Validate Decimal precision for financial fields
            assert isinstance(purp_position.size, Decimal), (
                f"size must be Decimal, got {type(purp_position.size)}"
            )
            if purp_position.entry_price is not None:
                assert isinstance(purp_position.entry_price, Decimal), (
                    f"entry_price must be Decimal, got {type(purp_position.entry_price)}"
                )
            if purp_position.unrealized_pnl is not None:
                assert isinstance(purp_position.unrealized_pnl, Decimal), (
                    f"unrealized_pnl must be Decimal, got {type(purp_position.unrealized_pnl)}"
                )

            # Validate business logic constraints
            assert purp_position.size != Decimal("0"), (
                f"Position size should be non-zero after opening trade, got {purp_position.size}"
            )
            assert purp_position.entry_price is not None and purp_position.entry_price > Decimal(
                "0"
            ), f"entry_price must be positive, got {purp_position.entry_price}"

            # Validate position side matches order side
            if purp_position.size > Decimal("0"):
                assert placed_order.side == OrderSide.BUY, (
                    "Long position should result from BUY order"
                )
            elif purp_position.size < Decimal("0"):
                assert placed_order.side == OrderSide.SELL, (
                    "Short position should result from SELL order"
                )

        # Clean up - attempt to close position if one was opened
        if purp_position is not None and purp_position.size != Decimal("0"):
            # Place opposite order to close position
            close_side = OrderSide.SELL if purp_position.size > Decimal("0") else OrderSide.BUY
            close_quantity = abs(purp_position.size)

            close_args = PlaceOrderArgs(
                symbol="PURP",
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
        # Step 1: First open a position
        open_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.IOC,
        )

        open_order = await hl_api_for_test_env.place_order(open_args)
        assert open_order.exchange_order_id is not None, "Opening order should succeed"

        # Step 2: Verify position was opened
        positions_after_open = await hl_api_for_test_env.get_positions()
        purp_position_after_open = None
        for position in positions_after_open:
            if position.symbol == "PURP" and position.size != Decimal("0"):
                purp_position_after_open = position
                break

        # Step 3: Close the position if it was opened
        if purp_position_after_open is not None:
            close_side = (
                OrderSide.SELL if purp_position_after_open.size > Decimal("0") else OrderSide.BUY
            )
            close_quantity = abs(purp_position_after_open.size)

            close_args = PlaceOrderArgs(
                symbol="PURP",
                side=close_side,
                order_type=OrderType.MARKET,
                quantity=close_quantity,
                time_in_force=TimeInForce.IOC,
            )

            close_order = await hl_api_for_test_env.place_order(close_args)
            assert close_order.exchange_order_id is not None, "Closing order should succeed"

            # Step 4: Verify position was closed or reduced
            positions_after_close = await hl_api_for_test_env.get_positions()
            purp_position_after_close = None
            for position in positions_after_close:
                if position.symbol == "PURP":
                    purp_position_after_close = position
                    break

            # Position should either be gone or have zero/reduced size
            if purp_position_after_close is not None:
                assert abs(purp_position_after_close.size) < abs(purp_position_after_open.size), (
                    f"Position should be reduced after close: "
                    f"before={purp_position_after_open.size}, "
                    f"after={purp_position_after_close.size}"
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
        # Create order with unrealistically large quantity to trigger insufficient margin
        large_position_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("999999.0"),  # Unrealistically large for testnet
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
        # Test very small position size
        small_position_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.000001"),  # Very small quantity
            time_in_force=TimeInForce.IOC,
        )

        try:
            # Attempt to open small position
            await hl_api_for_test_env.place_order(small_position_args)

            # If successful, validate precision is maintained in resulting position
            positions = await hl_api_for_test_env.get_positions()
            purp_position = None
            for position in positions:
                if position.symbol == "PURP" and position.size != Decimal("0"):
                    purp_position = position
                    break

            if purp_position is not None:
                # Validate that small quantities maintain proper decimal representation
                size_str = str(purp_position.size)
                assert "E" not in size_str.upper() or "E-" in size_str.upper(), (
                    f"Scientific notation should be negative exponent if used: {size_str}"
                )

                # Validate precision constraints
                assert isinstance(purp_position.size, Decimal), (
                    f"Position size must be Decimal, got {type(purp_position.size)}"
                )

                # Clean up small position
                if abs(purp_position.size) > Decimal("0"):
                    close_side = (
                        OrderSide.SELL if purp_position.size > Decimal("0") else OrderSide.BUY
                    )
                    close_args = PlaceOrderArgs(
                        symbol="PURP",
                        side=close_side,
                        order_type=OrderType.MARKET,
                        quantity=abs(purp_position.size),
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
        # Create order with non-existent asset to open position
        invalid_asset_args = PlaceOrderArgs(
            symbol="INVALID_POSITION_ASSET",  # Non-existent asset
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1"),
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
        # Step 1: Open initial position
        initial_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.IOC,
        )

        initial_order = await hl_api_for_test_env.place_order(initial_args)
        assert initial_order.exchange_order_id is not None, "Initial order should succeed"

        # Step 2: Add to position
        add_args = PlaceOrderArgs(
            symbol="PURP",
            side=OrderSide.BUY,  # Same side to add to position
            order_type=OrderType.MARKET,
            quantity=Decimal("0.05"),
            time_in_force=TimeInForce.IOC,
        )

        add_order = await hl_api_for_test_env.place_order(add_args)
        assert add_order.exchange_order_id is not None, "Add order should succeed"

        # Step 3: Verify position reflects both operations
        positions_after_add = await hl_api_for_test_env.get_positions()
        purp_position = None
        for position in positions_after_add:
            if position.symbol == "PURP" and position.size != Decimal("0"):
                purp_position = position
                break

        if purp_position is not None:
            # Validate position accumulation
            assert purp_position.size > Decimal("0.1"), (
                f"Position should reflect accumulated size: {purp_position.size}"
            )

            # Validate decimal precision maintained
            assert isinstance(purp_position.size, Decimal), "Size must remain Decimal"
            assert isinstance(purp_position.entry_price, Decimal), "Entry price must remain Decimal"

            # Step 4: Clean up - close entire position
            close_args = PlaceOrderArgs(
                symbol="PURP",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=abs(purp_position.size),
                time_in_force=TimeInForce.IOC,
            )

            try:
                await hl_api_for_test_env.place_order(close_args)
            except APIError:
                # If close fails, that's acceptable for test cleanup
                pass
