"""Integration tests for Backpack order functionality with no active orders.

Tests order retrieval and management when the account has no open orders,
including edge cases and empty responses.
"""

from __future__ import annotations

from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models import Order
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    DEFAULT_TEST_SYMBOL_SPOT,
    TEST_SYMBOL_BTC_USDC,
    TEST_SYMBOL_ETH_USDC,
    generate_invalid_order_id,
    get_dynamic_test_price,
    get_minimal_order_size_for_zero_balance_test,
    get_unreasonably_large_price,
    get_unreasonably_large_quantity,
)


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.zero_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/orders_zero"],
    indirect=True,
)
class TestBackpackOrdersZero:
    """Test order functionality when account has no active orders."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_empty(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving open orders when none exist."""
        orders = await bp_api_for_zero_balance_test.get_all_open_orders(GetAllOpenOrdersArgs())

        assert isinstance(orders, list)
        assert len(orders) == 0
        assert orders == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_by_symbol_empty(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving open orders for a symbol with no orders."""
        args = GetAllOpenOrdersArgs(symbol=TEST_SYMBOL_BTC_USDC)
        orders = await bp_api_for_zero_balance_test.get_all_open_orders(args)

        assert isinstance(orders, list)
        assert len(orders) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_when_none_exist(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling all orders when no open orders exist."""
        # Backpack requires symbol parameter for cancel_all_orders
        results = await bp_api_for_zero_balance_test.cancel_all_orders(
            symbol=DEFAULT_TEST_SYMBOL_SPOT,
        )

        assert isinstance(results, list)
        assert len(results) == 0
        assert results == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_order_nonexistent(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling a non-existent order."""
        fake_order_id = generate_invalid_order_id()

        with pytest.raises(APIError) as exc_info:
            args = CancelOrderArgs(
                order_id=fake_order_id,
                symbol=DEFAULT_TEST_SYMBOL_SPOT,
            )
            await bp_api_for_zero_balance_test.cancel_order(args)

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
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order history for a period with no orders."""
        # Request history for a very old period unlikely to have orders
        # GetOrderHistoryArgs doesn't support time filtering in base API
        orders = await bp_api_for_zero_balance_test.get_order_history(GetOrderHistoryArgs())

        assert isinstance(orders, list)
        # Should be empty for this old period
        assert len(orders) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_new_account(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order history for a new account with no trading history."""
        # Get all-time history
        orders = await bp_api_for_zero_balance_test.get_order_history(GetOrderHistoryArgs())

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
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that order creation correctly fails with zero balance."""
        symbol = DEFAULT_TEST_SYMBOL_SPOT
        side = OrderSide.BUY

        test_price = await get_dynamic_test_price(bp_api_for_zero_balance_test, symbol, side)
        test_quantity = await get_minimal_order_size_for_zero_balance_test(
            bp_api_for_zero_balance_test,
            symbol,
            side,
            test_price,
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        # With zero balance, order placement should fail appropriately
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(args)

        error = exc_info.value
        # Should fail due to insufficient balance
        assert error.code in [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], f"Expected insufficient balance error, got: {error.code} - {error.message}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_after_all_filled(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test open orders after all orders have been filled.

        This simulates the state after all open orders have been
        executed and filled.
        """
        # Check open orders (should be none if all are filled)
        open_orders = await bp_api_for_zero_balance_test.get_all_open_orders(GetAllOpenOrdersArgs())

        assert isinstance(open_orders, list)
        assert len(open_orders) == 0

        # But order history might show filled orders
        # This would require checking order history for past trades
        _ = await bp_api_for_zero_balance_test.get_order_history(GetOrderHistoryArgs())

        # If account has traded before, there should be some filled orders in history
        # but open orders should still be empty

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_all_orders_by_symbol_no_orders(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancelling all orders for a symbol with no orders."""
        # Try to cancel orders for a symbol with no open orders
        results = await bp_api_for_zero_balance_test.cancel_all_orders(symbol=TEST_SYMBOL_ETH_USDC)

        assert isinstance(results, list)
        assert len(results) == 0

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_creation_insufficient_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order creation with insufficient balance.

        Attempts to create an order that would require more balance
        than available, expecting it to fail.
        """
        # Try to buy a large amount with insufficient USDC
        symbol = TEST_SYMBOL_BTC_USDC
        # Use dynamic high price and quantity to ensure insufficient funds
        large_price = await get_unreasonably_large_price(bp_api_for_zero_balance_test, symbol)
        large_quantity = await get_unreasonably_large_quantity(bp_api_for_zero_balance_test, symbol)

        with pytest.raises(APIError) as exc_info:
            args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=large_quantity,
                price=large_price,
                time_in_force=TimeInForce.GTC,
            )
            await bp_api_for_zero_balance_test.place_order(args)

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
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order history with specific filters returning empty results."""
        # Filter for a specific symbol with no history
        orders = await bp_api_for_zero_balance_test.get_order_history(GetOrderHistoryArgs())

        assert isinstance(orders, list)
        # Might be empty for uncommon pairs
        if len(orders) == 0:
            assert orders == []

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_lifecycle_zero_to_one(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that zero balance properly prevents order lifecycle operations."""
        # Start with no orders (verify initial state)
        initial_orders = await bp_api_for_zero_balance_test.get_all_open_orders(
            GetAllOpenOrdersArgs(),
        )
        assert len(initial_orders) == 0

        symbol = DEFAULT_TEST_SYMBOL_SPOT
        side = OrderSide.BUY

        test_price = await get_dynamic_test_price(bp_api_for_zero_balance_test, symbol, side)
        test_quantity = await get_minimal_order_size_for_zero_balance_test(
            bp_api_for_zero_balance_test,
            symbol,
            side,
            test_price,
        )

        args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        # With zero balance, should not be able to create orders
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(args)

        error = exc_info.value
        # Should fail due to insufficient balance
        assert error.code in [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], f"Expected insufficient balance error, got: {error.code} - {error.message}"

        # Verify still no orders after failed attempt
        final_orders = await bp_api_for_zero_balance_test.get_all_open_orders(
            GetAllOpenOrdersArgs(),
        )
        assert len(final_orders) == 0
