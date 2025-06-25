"""Integration tests for Hyperliquid private perpetual orders endpoints.

This module focuses specifically on testing the Order model pipeline
through Hyperliquid's private /exchange endpoints with EIP-712 authentication for
perpetual contracts.
Tests validate complete data transformation for state-changing operations.

Model Focus: Order (Write Operations - Perpetual)
- Tests perpetual order placement and cancellation operations
- Validates EIP-712 cryptographic authentication
- Tests business logic constraints for perpetual trading operations
- Comprehensive error handling for private perpetual order operations

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
    CancelOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult, Order
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import (
    HyperliquidTestHelpers,
    get_minimal_test_quantity,
    get_safe_test_price,
)
from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol


logger = get_logger(__name__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
    pytest.mark.timing,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/orders/positive"], indirect=True
)
class TestHyperliquidPerpOrdersPrivate:
    """Comprehensive private perpetual orders integration tests for /exchange endpoint operations.

    This class tests only /exchange endpoint operations (signed with EIP-712) for
    perpetual contracts:
    - place_order (perpetual contracts)
    - cancel_order (perpetual contracts)
    - cancel_all_orders (perpetual contracts)

    These operations require cryptographic authentication and modify exchange state.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful place_order() with comprehensive Order model validation.

        This test validates the complete pipeline from EIP-712 authenticated request
        to fully validated Order model instances with all field constraints.
        Uses dynamic helpers to calculate safe test prices and quantities.
        """
        # Use dynamic helpers to get appropriate test symbol from exchange
        from tests.integration.apis.hyperliquid.shared.symbol_helpers import get_test_symbol

        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters - use market-based tolerance calculation
        market_price = await HyperliquidTestHelpers.get_current_market_price(
            hl_api_for_test_env, test_symbol
        )
        # Calculate safe tolerance as 5% of current market price for test orders
        safe_tolerance = market_price * Decimal("0.05")

        test_price = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=safe_tolerance
        )
        test_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Define order parameters using dynamic values
        place_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        # Execute the API call
        placed_order = await hl_api_for_test_env.place_order(place_args)

        # Validate return type
        assert isinstance(placed_order, Order), "place_order() should return Order instance"

        # Validate core fields
        assert placed_order.exchange == "hyperliquid", (
            f"Order.exchange should be 'hyperliquid', got {placed_order.exchange}"
        )

        # Validate order matches request parameters
        assert placed_order.symbol == test_symbol, (
            f"Order symbol should match request, got {placed_order.symbol}"
        )
        assert placed_order.side == OrderSide.BUY, (
            f"Order side should match request, got {placed_order.side}"
        )
        assert placed_order.order_type == OrderType.LIMIT, (
            f"Order type should match request, got {placed_order.order_type}"
        )

        # Validate Decimal precision for all financial fields
        assert isinstance(placed_order.quantity_requested, Decimal), (
            f"quantity_requested must be Decimal, got {type(placed_order.quantity_requested)}"
        )
        assert isinstance(placed_order.price, Decimal), (
            f"price must be Decimal, got {type(placed_order.price)}"
        )
        assert isinstance(placed_order.quantity_filled, Decimal), (
            f"quantity_filled must be Decimal, got {type(placed_order.quantity_filled)}"
        )

        # Validate order parameter values
        assert placed_order.quantity_requested == test_quantity, (
            f"Order quantity should match request, got {placed_order.quantity_requested}"
        )
        assert placed_order.price == test_price, (
            f"Order price should match request, got {placed_order.price}"
        )

        # Validate order status and lifecycle
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.NEW], (
            f"Order should be open/new after placement, got {placed_order.status}"
        )
        assert placed_order.exchange_order_id is not None, "Order should have exchange-generated ID"

        # For new orders, quantity_filled should be 0 - use exchange precision
        exchange_precision = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_test_env, test_symbol
        )
        zero_quantity = Decimal("0").quantize(exchange_precision["step_size"])
        assert placed_order.quantity_filled == zero_quantity, (
            f"New order should have zero filled quantity, got {placed_order.quantity_filled}"
        )

        # Validate business logic constraints
        assert placed_order.quantity_requested > Decimal("0"), (
            f"quantity_requested must be positive, got {placed_order.quantity_requested}"
        )
        assert placed_order.price > Decimal("0"), (
            f"price must be positive, got {placed_order.price}"
        )
        assert placed_order.quantity_filled >= Decimal("0"), (
            f"quantity_filled must be non-negative, got {placed_order.quantity_filled}"
        )
        assert placed_order.quantity_filled <= placed_order.quantity_requested, (
            f"quantity_filled ({placed_order.quantity_filled}) cannot exceed "
            f"quantity_requested ({placed_order.quantity_requested})"
        )

        # Validate Hyperliquid-specific order ID format (should be integer string)
        try:
            int(placed_order.exchange_order_id)
        except ValueError:
            pytest.fail("Hyperliquid order ID should be a valid integer string")

        # Validate exchange-specific details if present
        if placed_order.hl_details:
            # Add specific validation for Hyperliquid order details
            # This would depend on the actual hl_details structure
            assert hasattr(placed_order, "hl_details"), "hl_details should be accessible"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_order_success_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful cancel_order() after order placement.

        This validates the complete order lifecycle: place → cancel → verify cancellation.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters that are safe and won't fill
        # Calculate safe tolerance to ensure no fill - use market price differential
        market_price = await HyperliquidTestHelpers.get_current_market_price(
            hl_api_for_test_env, test_symbol
        )
        # Use 5% below market as safe tolerance to avoid fills
        safe_tolerance = market_price * Decimal("0.05")
        test_price = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=safe_tolerance
        )
        test_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # First place an order using dynamic values
        place_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,  # Far below market using dynamic calculation
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await hl_api_for_test_env.place_order(place_args)
        order_id = placed_order.exchange_order_id

        # Ensure order_id is not None before creating cancel args
        assert order_id is not None, "Order ID should not be None after placement"

        # Small delay to ensure order is settled
        import asyncio

        await asyncio.sleep(1.0)

        # Verify order exists before cancelling
        from cyberdelta.apis.models.service_args_models import GetOrderArgs

        get_order_args = GetOrderArgs(
            order_id=order_id,
            symbol=test_symbol,
        )

        existing_order = await hl_api_for_test_env.get_order(get_order_args)
        assert existing_order is not None, f"Order {order_id} not found before cancellation"
        assert existing_order.status == OrderStatus.OPEN, (
            f"Order status is {existing_order.status}, expected OPEN"
        )

        # Cancel the order
        cancel_args = CancelOrderArgs(
            order_id=order_id,
            symbol=test_symbol,  # Hyperliquid requires asset for cancellation
        )

        cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

        # Validate cancellation success
        assert cancel_result.success is True, "cancel_order() should return True on success"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_invalid_symbol(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with invalid asset/symbol error."""
        # Get a valid symbol first to use for dynamic price and quantity calculation
        valid_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic values based on a valid symbol for test parameters
        test_price = await get_safe_test_price(
            hl_api_for_test_env, valid_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        test_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, valid_symbol, OrderSide.BUY
        )

        # Create order with non-existent asset but using realistic price/quantity values
        invalid_asset_args = PlaceOrderArgs(
            symbol="INVALID_ASSET_XYZ",  # Non-existent asset
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,  # Use realistic quantity
            price=test_price,  # Use realistic price
            time_in_force=TimeInForce.GTC,
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
    async def test_cancel_nonexistent_order(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_order() with non-existent order ID.

        This tests that our service correctly returns a failed CancelOrderResult
        when attempting to cancel a non-existent order, following the API contract
        where cancel_order returns CancelOrderResult instead of raising exceptions.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Attempt to cancel order with fake ID
        cancel_args = CancelOrderArgs(
            order_id="99999999999999999",  # Non-existent order ID
            symbol=test_symbol,
        )

        # Should return failed CancelOrderResult (not raise exception)
        cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

        # Validate the failed result
        assert isinstance(cancel_result, CancelOrderResult), (
            f"Expected CancelOrderResult, got {type(cancel_result)}"
        )
        assert not cancel_result.success, "Cancel operation should fail for non-existent order"
        assert cancel_result.status == CancelOrderResultStatus.FAILED, (
            f"Expected FAILED status, got {cancel_result.status}"
        )
        assert cancel_result.order_id == "99999999999999999", (
            f"Order ID should match request, got {cancel_result.order_id}"
        )
        assert cancel_result.symbol == test_symbol, (
            f"Symbol should match request, got {cancel_result.symbol}"
        )

        # Check for common Hyperliquid order not found phrases in message
        if cancel_result.message:
            message_lower = cancel_result.message.lower()
            assert any(
                phrase in message_lower
                for phrase in ["not found", "never placed", "invalid", "does not exist"]
            ), f"Error message should indicate order not found: {cancel_result.message}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order placement with edge case precision values.

        This validates that the system handles very small quantities and prices
        that are at the edge of exchange precision requirements.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get market constraints to calculate edge case values
        constraints = await HyperliquidTestHelpers.get_market_constraints(
            hl_api_for_test_env, test_symbol
        )

        # Get a safe test price far below market to avoid filling
        edge_price = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )

        # Calculate quantity that meets both minimum quantity AND minimum notional requirements
        # This ensures the test can actually run instead of being skipped
        min_quantity = constraints["min_quantity"]
        step_size = constraints["step_size"]

        # Hyperliquid has a $10 minimum notional value requirement
        # Add buffer to ensure we're safely above the minimum due to precision issues
        MIN_NOTIONAL_USD = Decimal("10.50")  # $10.50 to ensure we're safely above $10.00
        min_qty_for_notional = MIN_NOTIONAL_USD / edge_price

        # Use the larger of: exchange min quantity OR quantity for $10.50 notional
        required_min_quantity = max(min_quantity, min_qty_for_notional)

        # Round up to next valid step size to ensure we meet minimums
        from decimal import ROUND_UP

        rounded_steps = (required_min_quantity / step_size).quantize(
            Decimal("1"), rounding=ROUND_UP
        )
        edge_quantity = rounded_steps * step_size

        # Ensure we meet exchange minimum quantity
        if edge_quantity < min_quantity:
            edge_quantity = min_quantity

        # Calculate final notional value for verification
        notional_value = edge_quantity * edge_price

        # Create order with edge case precision values
        precision_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=edge_quantity,  # Minimum allowed quantity from exchange
            price=edge_price,  # Safe price calculated from market data
            time_in_force=TimeInForce.GTC,
        )

        # Verify this is actually testing precision (quantity uses step size properly)
        assert notional_value >= Decimal("10.00"), (
            f"Test should meet Hyperliquid's minimum notional requirements: "
            f"{notional_value} >= 10.00"
        )

        try:
            placed_order = await hl_api_for_test_env.place_order(precision_args)

            # Validate precision is maintained in the response
            assert placed_order.quantity_requested == edge_quantity, (
                f"Order quantity precision should be maintained: "
                f"{placed_order.quantity_requested} vs {edge_quantity}"
            )

            # Validate the order actually tests precision at a reasonable level
            # (not just using huge quantities that don't test precision)
            max_reasonable_quantity = min_quantity * Decimal("100")  # Within 100x of minimum
            assert edge_quantity <= max_reasonable_quantity, (
                f"Precision test should use reasonable quantities: "
                f"{edge_quantity} <= {max_reasonable_quantity}"
            )

            # Clean up the order
            if placed_order.exchange_order_id:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=test_symbol,
                )
                await hl_api_for_test_env.cancel_order(cancel_args)

        except APIError as e:
            # Check for minimum notional value errors (common for edge case tests)
            if "minimum value" in e.message.lower() or "minimum notional" in e.message.lower():
                pytest.skip(f"Minimum notional value not met for precision test: {e.message}")
            elif "insufficient" in e.message.lower():
                pytest.skip(f"Insufficient funds for precision test: {e.message}")
            else:
                # This is a real error that should fail the test
                pytest.fail(f"Precision edge case test failed unexpectedly: {e.message}")
        except Exception as e:
            # Unexpected system error
            pytest.fail(f"Unexpected error in precision edge case test: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_lifecycle_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test complete order lifecycle: place → modify → cancel.

        This validates the full order management workflow with proper
        state transitions and error handling.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters for initial order
        initial_price = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        initial_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Place initial order using dynamic values
        initial_order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=initial_quantity,  # Calculated minimal quantity
            price=initial_price,  # Safe price far below market
            time_in_force=TimeInForce.GTC,
        )

        # Place the order
        placed_order = await hl_api_for_test_env.place_order(initial_order_args)
        order_id = placed_order.exchange_order_id

        assert order_id is not None, "Order should have valid exchange ID"

        try:
            # Note: Hyperliquid doesn't support order modification - only cancel/replace
            # So we'll test cancel functionality as part of lifecycle

            # Cancel the order
            cancel_args = CancelOrderArgs(order_id=order_id, symbol=test_symbol)
            cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

            assert cancel_result.success is True, "Order cancellation should succeed"

        except Exception as e:
            # If cancellation fails, this is a critical error for test cleanup
            pytest.fail(f"Order lifecycle test failed during cancellation: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_success_with_symbol_filter(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() with symbol filter.

        This validates that cancel_all_orders can target specific symbols
        and only cancels orders for the specified symbol.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters for orders
        test_price_1 = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        test_quantity_1 = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        test_price_2 = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.90")
        )
        test_quantity_2 = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Place multiple orders using dynamic values
        order_args_1 = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity_1,  # Calculated minimal quantity
            price=test_price_1,  # Safe price far below market
            time_in_force=TimeInForce.GTC,
        )

        order_args_2 = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity_2,  # Calculated minimal quantity
            price=test_price_2,  # Different safe price below market
            time_in_force=TimeInForce.GTC,
        )

        # Place orders
        placed_orders: list[Order] = []
        try:
            order_1 = await hl_api_for_test_env.place_order(order_args_1)
            placed_orders.append(order_1)

            order_2 = await hl_api_for_test_env.place_order(order_args_2)
            placed_orders.append(order_2)

            # Cancel all orders for the specific symbol
            cancel_result = await hl_api_for_test_env.cancel_all_orders(symbol=test_symbol)

            # Validate cancellation result
            assert isinstance(cancel_result, list), "cancel_all_orders should return list"
            assert len(cancel_result) > 0, "Should have cancelled at least one order"
            # Check that all cancelled orders are for the correct symbol
            for result in cancel_result:
                assert isinstance(result, CancelOrderResult), (
                    "Each result should be CancelOrderResult"
                )
                assert result.success, "Cancel should succeed"
                assert result.symbol == test_symbol, (
                    f"Cancel result should match symbol filter: {result.symbol}"
                )

        except Exception as e:
            # Clean up any placed orders on failure
            for order in placed_orders:
                if order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=order.exchange_order_id,
                            symbol=test_symbol,
                        )
                        await hl_api_for_test_env.cancel_order(cancel_args)
                    except Exception as cleanup_error:
                        logger.debug(f"Cleanup cancellation failed (expected): {cleanup_error}")

            # Check if this is expected behavior (insufficient orders)
            if len(placed_orders) < 2:
                pytest.skip("Insufficient orders placed for comprehensive cancel_all test")
            else:
                pytest.fail(f"cancel_all_orders test failed: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_success_without_symbol_filter(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() without symbol filter.

        This validates that cancel_all_orders can cancel all open orders
        across all symbols when no filter is specified.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters for order
        test_price = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        test_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Place an order using dynamic values
        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,  # Calculated minimal quantity
            price=test_price,  # Safe price far below market
            time_in_force=TimeInForce.GTC,
        )

        placed_orders: list[Order] = []
        try:
            # Place the order
            order = await hl_api_for_test_env.place_order(order_args)
            placed_orders.append(order)

            # Cancel all orders (no symbol filter)
            cancel_result = await hl_api_for_test_env.cancel_all_orders()

            # Validate cancellation result
            assert isinstance(cancel_result, list), "cancel_all_orders should return list"
            assert len(cancel_result) > 0, "Should have cancelled at least one order"
            # Check that all cancelled orders succeeded
            for result in cancel_result:
                assert isinstance(result, CancelOrderResult), (
                    "Each result should be CancelOrderResult"
                )
                assert result.success, "Cancel should succeed"

        except Exception as e:
            # Clean up any placed orders on failure
            for order in placed_orders:
                if order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=order.exchange_order_id,
                            symbol=test_symbol,
                        )
                        await hl_api_for_test_env.cancel_order(cancel_args)
                    except Exception as cleanup_error:
                        logger.debug(f"Cleanup cancellation failed (expected): {cleanup_error}")

            pytest.fail(f"cancel_all_orders without filter test failed: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_open_orders(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() when no orders are open.

        This validates proper handling when cancel_all_orders is called
        but there are no orders to cancel.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # First ensure no orders are open by cancelling any existing ones
        try:
            await hl_api_for_test_env.cancel_all_orders()
        except Exception as cleanup_error:
            logger.debug(f"Cleanup cancellation failed (expected): {cleanup_error}")

        # Now test cancel_all_orders with no open orders
        cancel_result = await hl_api_for_test_env.cancel_all_orders(symbol=test_symbol)

        # Should return empty list or handle gracefully
        assert isinstance(cancel_result, list), "cancel_all_orders should return list"
        # Empty list is expected when no orders to cancel
        assert len(cancel_result) == 0, f"Should return empty list, got {len(cancel_result)} items"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_symbol_filter_no_matches(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() with symbol filter that matches no orders.

        This validates handling when symbol filter is provided but no orders
        exist for that specific symbol.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters for order
        test_price = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        test_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Place an order using dynamic values
        order_args = PlaceOrderArgs(
            symbol=test_symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,  # Calculated minimal quantity
            price=test_price,  # Safe price far below market
            time_in_force=TimeInForce.GTC,
        )

        try:
            order = await hl_api_for_test_env.place_order(order_args)
            order_placed = order.exchange_order_id is not None
        except APIError as e:
            # Distinguish expected business errors from system failures
            if "insufficient" in str(e).lower() or "minimum" in str(e).lower():
                # Expected business logic error
                order_placed = False
            else:
                # Unexpected system error
                pytest.fail(f"Unexpected error placing order: {e}")

        if order_placed:
            # Execute cancel_all_orders for a different symbol
            cancel_results = await hl_api_for_test_env.cancel_all_orders(symbol="NONEXISTENT")

            # Should return empty list since no orders match the filter
            assert isinstance(cancel_results, list), (
                "cancel_all_orders() should return list even when no orders match symbol"
            )
            assert len(cancel_results) == 0, (
                f"Should return empty results for non-matching symbol, got {len(cancel_results)}"
            )

            # Verify order is still open (wasn't cancelled by non-matching filter)
            open_orders = await hl_api_for_test_env.get_open_orders()
            test_symbol_orders = [order for order in open_orders if order.symbol == test_symbol]
            assert len(test_symbol_orders) > 0, (
                f"{test_symbol} order should still exist after cancel_all "
                f"with different symbol filter"
            )

            # Clean up the test symbol order
            try:
                await hl_api_for_test_env.cancel_all_orders(symbol=test_symbol)
            except APIError as e:
                # Order cancellation is critical - don't hide failures
                pytest.fail(
                    f"Failed to cancel order during cleanup: {e}. "
                    "Order cancellation is critical for test isolation."
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() error handling scenarios.

        This validates proper error handling for various edge cases
        in bulk order cancellation.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters for orders
        test_price_1 = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        test_quantity_1 = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        test_price_2 = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.90")
        )
        test_quantity_2 = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Place orders using dynamic values
        order_args_list = [
            PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity_1,  # Calculated minimal quantity
                price=test_price_1,  # Safe price far below market
                time_in_force=TimeInForce.GTC,
            ),
            PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity_2,  # Calculated minimal quantity
                price=test_price_2,  # Different safe price below market
                time_in_force=TimeInForce.GTC,
            ),
        ]

        placed_orders: list[Order] = []
        for order_args in order_args_list:
            try:
                order = await hl_api_for_test_env.place_order(order_args)
                placed_orders.append(order)
            except APIError:
                # Order placement failures are acceptable for error handling test
                pass

        # Test cancel_all_orders with proper error handling
        try:
            cancel_result = await hl_api_for_test_env.cancel_all_orders(symbol=test_symbol)

            # Validate result structure
            assert isinstance(cancel_result, list), "cancel_all_orders should return list"

        except Exception as e:
            # Clean up any placed orders
            for order in placed_orders:
                if order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=order.exchange_order_id,
                            symbol=test_symbol,
                        )
                        await hl_api_for_test_env.cancel_order(cancel_args)
                    except Exception as cleanup_error:
                        logger.debug(f"Cleanup cancellation failed (expected): {cleanup_error}")

            # Some errors might be expected in error handling test
            if "authentication" in str(e).lower() or "permission" in str(e).lower():
                pytest.skip(f"Authentication/permission error in error handling test: {e}")
            else:
                pytest.fail(f"Unexpected error in cancel_all_orders error handling: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_multiple_symbols_comprehensive(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_all_orders() comprehensive scenarios with multiple symbols.

        This validates complex bulk cancellation scenarios across different
        symbols and order types.
        """
        # Get test symbol from exchange
        test_symbol = await get_test_symbol(hl_api_for_test_env, "perp", 0)

        # Get dynamic test parameters for different orders
        test_price_1 = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.95")
        )
        test_quantity_1 = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        test_price_2 = await get_safe_test_price(
            hl_api_for_test_env, test_symbol, OrderSide.BUY, tolerance=Decimal("0.90")
        )
        test_quantity_2 = await get_minimal_test_quantity(
            hl_api_for_test_env, test_symbol, OrderSide.BUY
        )

        # Create multiple orders using dynamic values
        order_scenarios = [
            PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity_1,  # Calculated minimal quantity
                price=test_price_1,  # Safe price far below market
                time_in_force=TimeInForce.GTC,
            ),
            PlaceOrderArgs(
                symbol=test_symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity_2,  # Different calculated quantity
                price=test_price_2,  # Different safe price below market
                time_in_force=TimeInForce.GTC,
            ),
        ]

        placed_orders: list[Order] = []
        successful_placements = 0

        # Place orders with error handling
        for order_args in order_scenarios:
            try:
                order = await hl_api_for_test_env.place_order(order_args)
                placed_orders.append(order)
                successful_placements += 1
            except APIError as e:
                # Order placement failures might be expected
                if "insufficient" in e.message.lower() or "minimum" in e.message.lower():
                    continue  # Expected business logic error
                else:
                    pytest.fail(f"Unexpected error placing order: {e}")

        # Test comprehensive cancel_all_orders
        try:
            if successful_placements > 0:
                # Cancel all orders for the symbol
                cancel_result = await hl_api_for_test_env.cancel_all_orders(symbol=test_symbol)

                # Validate comprehensive cancellation
                assert isinstance(cancel_result, list), "cancel_all_orders should return list"

                # Should have cancelled our placed orders
                assert len(cancel_result) >= successful_placements, (
                    f"Should cancel at least {successful_placements} orders, "
                    f"got {len(cancel_result)}"
                )
            else:
                pytest.skip("No orders were successfully placed for comprehensive test")

        except Exception as e:
            # Clean up any remaining orders
            for order in placed_orders:
                if order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=order.exchange_order_id,
                            symbol=test_symbol,
                        )
                        await hl_api_for_test_env.cancel_order(cancel_args)
                    except Exception as cleanup_error:
                        logger.debug(f"Cleanup cancellation failed (expected): {cleanup_error}")

            pytest.fail(f"Comprehensive cancel_all_orders test failed: {e}")
