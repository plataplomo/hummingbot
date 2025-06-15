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
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models import BackpackOrderDetails, Order
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce

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
        symbol = "SOL-USDC"
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
        """Test placing a limit buy order."""
        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("50.0"),  # Low price to avoid immediate fill
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert isinstance(order, Order)
        assert order.exchange == "backpack"
        assert order.symbol == "SOL-USDC"
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.quantity_requested == Decimal("0.1")
        assert order.price == Decimal("50.0")
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
        """Test placing a limit sell order."""
        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("200.0"),  # High price to avoid immediate fill
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert isinstance(order, Order)
        assert order.exchange == "backpack"
        assert order.symbol == "SOL-USDC"
        assert order.side == OrderSide.SELL
        assert order.order_type == OrderType.LIMIT
        assert order.quantity_requested == Decimal("0.1")
        assert order.price == Decimal("200.0")
        assert order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_with_client_order_id(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing an order with a custom client order ID."""
        import uuid

        client_order_id = f"test_{uuid.uuid4().hex[:16]}"

        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("50.0"),
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
        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("50.0"),
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
        args1 = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("50.0"),
            time_in_force=TimeInForce.GTC,
        )
        await bp_api_for_test_env.place_order(args1)

        args2 = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("200.0"),
            time_in_force=TimeInForce.GTC,
        )
        await bp_api_for_test_env.place_order(args2)

        # Cancel all orders
        results = await bp_api_for_test_env.cancel_all_orders()

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
        symbol = "SOL-USDC"

        # Create orders for the symbol
        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("50.0"),
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
        """Test placing a post-only order."""
        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("50.0"),
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert isinstance(order, Order)
        assert order.post_only is True

        # Post-only orders should not immediately fill
        assert order.status == OrderStatus.OPEN
        assert order.quantity_filled == Decimal("0")

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
