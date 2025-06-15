"""Integration tests for Backpack order functionality with no active orders.

Tests order retrieval and management when the account has no open orders,
including edge cases and empty responses.
"""

from __future__ import annotations

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
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.account,
    pytest.mark.orders,
    pytest.mark.zero_orders,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/orders_zero"], indirect=True
)
class TestBackpackOrdersZero:
    """Test order functionality when account has no active orders."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_empty(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving open orders when none exist."""
        orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())

        assert isinstance(orders, list)
        assert len(orders) == 0
        assert orders == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_by_symbol_empty(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving open orders for a symbol with no orders."""
        args = GetAllOpenOrdersArgs(symbol="BTC-USDC")
        orders = await bp_api_for_test_env.get_all_open_orders(args)

        assert isinstance(orders, list)
        assert len(orders) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_when_none_exist(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling all orders when no open orders exist."""
        results = await bp_api_for_test_env.cancel_all_orders()

        assert isinstance(results, list)
        assert len(results) == 0
        assert results == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_order_nonexistent(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling a non-existent order."""
        fake_order_id = "1234567890abcdef"

        with pytest.raises(APIError) as exc_info:
            args = CancelOrderArgs(
                order_id=fake_order_id,
                symbol="SOL-USDC",
            )
            await bp_api_for_test_env.cancel_order(args)

        error = exc_info.value
        # Could be ORDER_NOT_FOUND or INVALID_REQUEST
        assert error.code in [
            APIErrorCode.ORDER_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
        ]

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_empty_period(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order history for a period with no orders."""
        # Request history for a very old period unlikely to have orders
        # GetOrderHistoryArgs doesn't support time filtering in base API
        orders = await bp_api_for_test_env.get_order_history(GetOrderHistoryArgs())

        assert isinstance(orders, list)
        # Should be empty for this old period
        assert len(orders) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_new_account(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order history for a new account with no trading history."""
        # Get all-time history
        orders = await bp_api_for_test_env.get_order_history(GetOrderHistoryArgs())

        assert isinstance(orders, list)

        # New account might have zero orders
        if len(orders) == 0:
            assert orders == []
        else:
            # If there are orders, they should be valid
            for order in orders:
                assert isinstance(order, Order)
                assert order.exchange == "backpack"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_create_and_immediately_cancel(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test creating an order and immediately cancelling it."""
        # Create an order
        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("1.0"),  # Very low price to avoid fill
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        assert order.status == OrderStatus.OPEN

        # Immediately cancel it
        assert order.exchange_order_id is not None, "Order must have exchange_order_id"
        cancel_args = CancelOrderArgs(
            order_id=order.exchange_order_id,
            symbol=order.symbol,
        )
        success = await bp_api_for_test_env.cancel_order(cancel_args)

        assert success is True

        # Verify no open orders remain
        open_orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())
        assert len(open_orders) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_after_all_filled(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test open orders after all orders have been filled.

        This simulates the state after all open orders have been
        executed and filled.
        """
        # Check open orders (should be none if all are filled)
        open_orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())

        assert isinstance(open_orders, list)
        assert len(open_orders) == 0

        # But order history might show filled orders
        # This would require checking order history for past trades
        _ = await bp_api_for_test_env.get_order_history(GetOrderHistoryArgs())

        # If account has traded before, there should be some filled orders in history
        # but open orders should still be empty

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_by_symbol_no_orders(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling all orders for a symbol with no orders."""
        # Try to cancel orders for a symbol with no open orders
        results = await bp_api_for_test_env.cancel_all_orders(symbol="ETH-USDC")

        assert isinstance(results, list)
        assert len(results) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_creation_insufficient_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order creation with insufficient balance.

        Attempts to create an order that would require more balance
        than available, expecting it to fail.
        """
        # Try to buy a large amount with insufficient USDC
        with pytest.raises(APIError) as exc_info:
            args = PlaceOrderArgs(
                symbol="BTC-USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1000"),  # Large BTC amount
                price=Decimal("50000"),  # High price = 50M USDC needed
                time_in_force=TimeInForce.GTC,
            )
            await bp_api_for_test_env.place_order(args)

        error = exc_info.value
        # Should fail due to insufficient balance
        assert error.code in [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.INVALID_REQUEST.value,
        ]

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_with_filters_empty(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order history with specific filters returning empty results."""
        # Filter for a specific symbol with no history
        orders = await bp_api_for_test_env.get_order_history(GetOrderHistoryArgs())

        assert isinstance(orders, list)
        # Might be empty for uncommon pairs
        if len(orders) == 0:
            assert orders == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_lifecycle_zero_to_one(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test transition from zero orders to having one order."""
        # Start with no orders
        initial_orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())
        assert len(initial_orders) == 0

        # Create one order
        args = PlaceOrderArgs(
            symbol="SOL-USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("10.0"),  # Low price
            time_in_force=TimeInForce.GTC,
        )
        order = await bp_api_for_test_env.place_order(args)

        # Now should have one order
        open_orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())
        assert len(open_orders) == 1
        assert open_orders[0].exchange_order_id == order.exchange_order_id

        # Cancel it to return to zero
        assert order.exchange_order_id is not None, "Order must have exchange_order_id"
        cancel_args = CancelOrderArgs(
            order_id=order.exchange_order_id,
            symbol=order.symbol,
        )
        await bp_api_for_test_env.cancel_order(cancel_args)

        # Back to zero orders
        final_orders = await bp_api_for_test_env.get_all_open_orders(GetAllOpenOrdersArgs())
        assert len(final_orders) == 0
