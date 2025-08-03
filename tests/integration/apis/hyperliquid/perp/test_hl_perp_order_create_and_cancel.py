"""Integration tests for Hyperliquid perpetual order creation and cancellation.

This module tests a simple order lifecycle: creating a limit order with a random
symbol and then cancelling that specific order. Tests use dynamic market data
and proper test helpers that adhere to TESTING_SECURITY_RULES.md requirements.

Model Focus: Order (Create and Cancel - Perpetual)
- Tests placement of a single buy limit order with dynamic random symbol
- Validates individual order cancellation operation
- Comprehensive error handling for order lifecycle operations
- Uses real market data with no hardcoded values

Authentication: EIP-712 signing for all /exchange endpoint operations
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.models.market.order import Order
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import (
    HyperliquidTestHelpers,
    get_minimal_test_quantity,
    get_safe_test_price,
)


logger = get_logger(__name__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.timing,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/hyperliquid/perp/orders/create_cancel"],
    indirect=True,
)
class TestHyperliquidPerpOrderCreateAndCancel:
    """Integration tests for simple order create and cancel operations.

    This class tests the basic order management workflow:
    - Create a single limit order with a random symbol
    - Cancel that specific order

    All operations use real market data and dynamic test helpers to ensure
    robustness across different market conditions and account states.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_create_limit_order_with_random_symbol(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test 1: Create a buy limit order with a random symbol using test helpers.

        This test validates the placement of a single order with a dynamically
        selected symbol using market data and proper test helpers.
        """
        # Get available symbols from exchange using dynamic discovery
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env,
            limit=5,
        )

        if not available_symbols:
            pytest.skip(
                "No perpetual symbols available from the exchange. "
                "This test requires at least one available perpetual symbol.",
            )

        # Select a random symbol (first available for deterministic VCR)
        symbol = available_symbols[0]
        logger.info(
            "test_symbol_selected",
            symbol=symbol,
            selection_method="first_available",
            message="Selected symbol for test",
        )

        try:
            # Get dynamic test parameters for safe order placement
            market_price = await HyperliquidTestHelpers.get_current_market_price(
                hl_api_for_test_env,
                symbol,
            )

            # Use 10% below market for buy order to avoid accidental fills
            test_price = await get_safe_test_price(
                hl_api_for_test_env,
                symbol,
                OrderSide.BUY,
                Decimal(10),
            )

            test_quantity = await get_minimal_test_quantity(
                hl_api_for_test_env,
                symbol,
                OrderSide.BUY,
            )

            logger.info(
                "order_parameters_debug",
                symbol=symbol,
                test_price=str(test_price),
                test_quantity=str(test_quantity),
                market_price=str(market_price),
                message="Order parameters for limit order placement",
            )

            # Define order parameters using dynamic values
            place_args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid(symbol),
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity,
                price=test_price,
                time_in_force=TimeInForce.GTC,
            )

            # Execute the API call
            placed_order = await hl_api_for_test_env.place_order(place_args)

            # Validate the placed order
            assert isinstance(placed_order, Order), "place_order() should return Order instance"
            assert placed_order.exchange == "hyperliquid", (
                f"Order.exchange should be 'hyperliquid', got {placed_order.exchange}"
            )
            assert placed_order.symbol.value == symbol, (
                f"Order symbol should match request, got {placed_order.symbol.value}"
            )
            assert placed_order.side == OrderSide.BUY, (
                f"Order side should match request, got {placed_order.side}"
            )
            assert placed_order.order_type == OrderType.LIMIT, (
                f"Order type should match request, got {placed_order.order_type}"
            )
            assert placed_order.exchange_order_id is not None, (
                "Order should have exchange-generated ID"
            )
            assert placed_order.status in [OrderStatus.OPEN, OrderStatus.NEW], (
                f"Order should be open/new after placement, got {placed_order.status}"
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

            # Verify exact values match
            assert placed_order.quantity_requested == test_quantity, (
                f"Quantity mismatch: requested {test_quantity}, "
                f"got {placed_order.quantity_requested}"
            )
            assert placed_order.price == test_price, (
                f"Price mismatch: requested {test_price}, got {placed_order.price}"
            )

            logger.info(
                "order_placed_successfully",
                exchange_order_id=placed_order.exchange_order_id,
                symbol=symbol,
                message="Successfully placed order",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(f"Failed to place limit order for {symbol}: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_the_created_order(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test 2: Cancel an existing open order.

        This test finds the most recent open order (potentially from the previous test)
        and validates the cancellation functionality.
        """
        # Get existing open orders
        try:
            open_orders = await hl_api_for_test_env.get_open_orders()
            logger.info(
                "open_orders_found",
                count=len(open_orders),
                message="Found open orders for cancellation test",
            )

            if not open_orders:
                # No existing orders, create one to cancel
                logger.info("No open orders found, creating one to test cancellation")

                available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
                    hl_api_for_test_env,
                    limit=5,
                )

                if not available_symbols:
                    pytest.skip(
                        "No perpetual symbols available from the exchange. "
                        "This test requires at least one available perpetual symbol.",
                    )

                symbol = available_symbols[0]

                # Get market price for minimal test quantity calculation
                await HyperliquidTestHelpers.get_current_market_price(hl_api_for_test_env, symbol)

                test_price = await get_safe_test_price(
                    hl_api_for_test_env,
                    symbol,
                    OrderSide.BUY,
                    Decimal(10),
                )

                test_quantity = await get_minimal_test_quantity(
                    hl_api_for_test_env,
                    symbol,
                    OrderSide.BUY,
                )

                place_args = PlaceOrderArgs(
                    symbol=exchanges.hyperliquid(symbol),
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=test_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                )

                placed_order = await hl_api_for_test_env.place_order(place_args)
                logger.info(
                    "order_created_for_cancellation",
                    exchange_order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                    message="Created order to cancel",
                )
                order_to_cancel = placed_order
            else:
                # Use the most recent open order
                # Sort by created_at (most recent first) if available, otherwise use last in list
                if hasattr(open_orders[0], "created_at") and open_orders[0].created_at:
                    order_to_cancel = max(open_orders, key=lambda x: x.created_at or 0)
                else:
                    order_to_cancel = open_orders[-1]  # Last order in list

                logger.info(
                    "existing_order_selected_for_cancellation",
                    exchange_order_id=order_to_cancel.exchange_order_id,
                    symbol=order_to_cancel.symbol,
                    message="Using existing order for cancellation test",
                )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(f"Failed to get open orders or create order: {e}")

        # Now cancel the selected order
        order_id = order_to_cancel.exchange_order_id
        symbol = order_to_cancel.symbol

        assert order_id is not None, "Order ID should not be None"
        logger.info(
            "attempting_order_cancellation",
            order_id=order_id,
            symbol=symbol,
            message="Attempting to cancel order",
        )

        try:
            # Brief wait to ensure order is processed
            await asyncio.sleep(1)

            # Optional: Verify order exists before cancelling
            open_orders = await hl_api_for_test_env.get_open_orders()
            order_exists = any(order.exchange_order_id == order_id for order in open_orders)

            if not order_exists:
                pytest.skip(
                    f"Order {order_id} not found in open orders. "
                    "Order may have been filled or already cancelled.",
                )

            # Cancel the order
            cancel_args = CancelOrderArgs(
                order_id=order_id,
                symbol=symbol,
            )

            cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

            # Validate cancellation success
            assert cancel_result.success is True, (
                "cancel_order() should return successful CancelOrderResult"
            )
            assert cancel_result.order_id == order_id, (
                "Cancel result should contain correct order ID"
            )
            assert cancel_result.symbol == symbol, "Cancel result should contain correct symbol"
            logger.info(
                "order_cancelled_successfully",
                order_id=order_id,
                symbol=symbol,
                message="Successfully cancelled order",
            )

            # Brief wait for exchange to process cancellation
            await asyncio.sleep(2)

            # Verify order is no longer in open orders
            updated_open_orders = await hl_api_for_test_env.get_open_orders()
            order_still_exists = any(
                order.exchange_order_id == order_id for order in updated_open_orders
            )

            assert not order_still_exists, (
                f"Order {order_id} should not appear in open orders after cancellation"
            )

        except APIError as e:
            # Distinguish expected business errors from system errors
            error_message = str(e).lower()
            if "not found" in error_message or "does not exist" in error_message:
                pytest.skip(f"Order {order_id} already cancelled or not found: {e}")
            elif "order not open" in error_message:
                pytest.skip(f"Order {order_id} is not in cancellable state: {e}")
            else:
                pytest.fail(f"Unexpected API error during order cancellation: {e}")
        except (ValueError, TypeError, KeyError) as e:
            pytest.fail(f"System error during order cancellation: {e}")
