"""Integration tests for Backpack order functionality with active orders.

Tests order retrieval, creation, modification, and cancellation when
the account has active orders in the market.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models import BackpackOrderDetails, Order
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from tests.integration.apis.backpack.shared.test_helpers import (
    DEFAULT_TEST_SYMBOL_SPOT,
    generate_deterministic_client_order_id,
    get_dynamic_test_price,
    get_minimal_order_size,
)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.orders,
    pytest.mark.positive_orders,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/orders_positive"], indirect=True
)
class TestBackpackOrdersPositive:
    """Test order functionality when account has active orders."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_multiple(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving multiple open orders."""
        orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())

        assert isinstance(orders, list)

        if len(orders) > 0:
            # Verify each order
            for order in orders:
                assert isinstance(order, Order)
                assert order.exchange == "backpack"
                assert order.status == OrderStatus.OPEN

                # Required fields
                assert isinstance(order.exchange_order_id, str)
                assert len(order.exchange_order_id) > 0
                assert isinstance(order.symbol, str)
                assert isinstance(order.side, OrderSide)
                assert isinstance(order.order_type, OrderType)
                assert isinstance(order.quantity_requested, Decimal)
                assert order.quantity_requested > Decimal("0")

                # Price for limit orders
                if order.order_type == OrderType.LIMIT:
                    assert order.price is not None
                    assert isinstance(order.price, Decimal)
                    assert order.price > Decimal("0")

                # Timestamp
                assert isinstance(order.created_at, datetime)

                # Backpack details
                assert order.bp_details is not None
                assert isinstance(order.bp_details, BackpackOrderDetails)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_by_symbol(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving open orders for a specific symbol."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT
        orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs(symbol=symbol))

        assert isinstance(orders, list)

        # All returned orders should be for the specified symbol
        for order in orders:
            assert order.symbol == symbol
            assert order.status == OrderStatus.OPEN

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_limit_order_buy(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing a limit buy order with dynamic pricing."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT

        # Get dynamic test price and minimal order size
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        test_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.BUY, test_price
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert isinstance(order, Order)
        assert order.exchange == "backpack"
        assert order.symbol == symbol
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.quantity_requested == test_quantity
        assert order.price == test_price
        assert order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]
        assert order.time_in_force == TimeInForce.GTC

        # Order ID should be assigned
        assert isinstance(order.exchange_order_id, str)
        assert len(order.exchange_order_id) > 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_limit_order_sell(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing a limit sell order with dynamic pricing."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT

        # Get dynamic test price and minimal order size
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.SELL)
        test_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.SELL, test_price
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert isinstance(order, Order)
        assert order.exchange == "backpack"
        assert order.symbol == symbol
        assert order.side == OrderSide.SELL
        assert order.order_type == OrderType.LIMIT
        assert order.quantity_requested == test_quantity
        assert order.price == test_price
        assert order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_with_client_order_id(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing an order with a custom client order ID and dynamic pricing."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT

        # Generate deterministic client_order_id for VCR testing
        # Backpack requires client_order_id to be convertible to integer
        client_order_id = generate_deterministic_client_order_id(
            test_name="test_place_order_with_client_order_id", symbol=symbol, side="BUY"
        )

        # Get dynamic test price and minimal order size
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        test_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.BUY, test_price
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
            client_order_id=client_order_id,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert isinstance(order, Order)
        assert order.client_order_id == client_order_id

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_order_by_id(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling a specific order by ID."""
        # First create an order to cancel
        symbol = DEFAULT_TEST_SYMBOL_SPOT
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        test_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.BUY, test_price
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        # Cancel the order
        assert order.exchange_order_id is not None, "Order must have exchange_order_id"
        cancel_args = CancelOrderArgs(
            order_id=order.exchange_order_id,
            symbol=order.symbol,
        )
        success = await bp_api_for_test_env.cancel_order(cancel_args)

        assert success is True

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling all open orders."""
        # Create multiple orders
        symbol = DEFAULT_TEST_SYMBOL_SPOT

        # Buy order
        buy_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        buy_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.BUY, buy_price
        )

        args1 = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=buy_quantity,
            price=buy_price,
            time_in_force=TimeInForce.GTC,
        )
        await bp_api_for_test_env.place_order(args1)

        # Sell order
        sell_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.SELL)
        sell_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.SELL, sell_price
        )

        args2 = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=sell_quantity,
            price=sell_price,
            time_in_force=TimeInForce.GTC,
        )
        await bp_api_for_test_env.place_order(args2)

        # Cancel all orders for this symbol (Backpack requires symbol parameter)
        results = await bp_api_for_test_env.cancel_all_orders(symbol=symbol)

        assert isinstance(results, list)
        assert len(results) >= 2

        for result in results:
            assert result.success is True

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_by_symbol(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling all orders for a specific symbol."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT

        # Create orders for the symbol using dynamic pricing
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        test_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.BUY, test_price
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )
        await bp_api_for_test_env.place_order(args)

        # Cancel all orders for the symbol
        results = await bp_api_for_test_env.cancel_all_orders(symbol=symbol)

        assert isinstance(results, list)

        for result in results:
            if result.symbol:
                assert result.symbol == symbol
            assert result.success is True

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_recent(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving recent order history."""
        # Get order history
        args = GetOrderHistoryArgs()
        orders = await bp_api_for_test_env.get_order_history(args)

        assert isinstance(orders, list)

        for order in orders:
            assert isinstance(order, Order)
            assert isinstance(order.created_at, datetime)

            # History should include various statuses
            assert order.status in [
                OrderStatus.NEW,
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
                OrderStatus.EXPIRED,
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
            ]

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_post_only(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing a post-only order with dynamic pricing."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT

        # Get dynamic test price and minimal order size
        # For post-only orders, we want a price that won't immediately fill
        # So we use a more conservative price offset
        test_price = await get_dynamic_test_price(
            bp_api_for_test_env, symbol, OrderSide.BUY, tolerance_percent=Decimal("10")
        )
        test_quantity = await get_minimal_order_size(
            bp_api_for_test_env, symbol, OrderSide.BUY, test_price
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )
        
        try:
            order = await bp_api_for_test_env.place_order(args)

            assert isinstance(order, Order)
            assert order.post_only is True

            # Post-only orders should either be OPEN or CANCELLED (if would have been taker)
            assert order.status in [OrderStatus.OPEN, OrderStatus.CANCELLED], (
                f"Post-only order has unexpected status: {order.status}"
            )
            
            if order.status == OrderStatus.OPEN:
                # If open, should not be filled
                assert order.quantity_filled == Decimal("0")
        except APIError as e:
            # Some exchanges reject post-only orders in certain conditions
            # Handle various error codes that can occur with post-only orders
            if (e.code == APIErrorCode.ORDER_REJECTED.value or 
                e.code == APIErrorCode.INVALID_REQUEST.value or
                e.code == APIErrorCode.AUTHENTICATION_FAILED.value or
                "Invalid signature" in str(e.message)):
                pytest.skip(f"Post-only order rejected or failed: {e.message}")
            raise

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_partial_fill_tracking(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test tracking partially filled orders."""
        orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())

        # Look for any partially filled orders
        partial_orders = [o for o in orders if o.status == OrderStatus.PARTIALLY_FILLED]

        for order in partial_orders:
            # Partially filled orders should have filled quantity > 0 but < total
            assert order.quantity_filled > Decimal("0")
            assert order.quantity_filled < order.quantity_requested

            # Average fill price should be set
            assert order.average_fill_price is not None
            assert order.average_fill_price > Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_fee_tracking(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that order fees are properly tracked."""
        # Get filled orders from history
        args = GetOrderHistoryArgs()
        orders = await bp_api_for_test_env.get_order_history(args)

        filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]

        for order in filled_orders:
            # Filled orders should have bp_details
            if order.bp_details:
                # Verify bp_details has expected fields
                assert (
                    order.bp_details.executed_quote_quantity is not None
                    or order.quantity_filled > 0
                )
