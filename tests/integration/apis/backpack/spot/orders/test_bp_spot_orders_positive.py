"""Integration tests for Backpack spot orders endpoints with positive balance.

This module focuses specifically on testing the Order model pipeline
through Backpack's spot order endpoints with Ed25519 authentication
when the account has sufficient funds for spot order operations.
Tests validate complete data transformation from API responses to Order instances.

Model Focus: Order (Spot Trading - Successful Operations)
- Validates complete Order model field mapping for spot markets
- Tests Decimal precision for financial values (quantities, prices)
- Validates business logic constraints and order state transitions
- Tests Backpack-specific order details (bp_details with order management info)
- Comprehensive spot order lifecycle with successful placements and cancellations

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: Positive USDC balance (successful spot order scenarios)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    TEST_SYMBOL_BTC_USDC,
    TEST_SYMBOL_SOL_USDC,
    get_current_market_price,
    get_market_constraints,
    get_minimal_order_size,
)


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.spot,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]

logger = get_logger(__name__)


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/spot/orders/positive"], indirect=True
)
class TestBackpackSpotOrdersPositiveBalance:
    """Comprehensive private orders integration tests with positive balance for operations."""

    # NOTE: Basic order placement/cancellation is covered in /account/orders/
    # This module focuses on SPOT-SPECIFIC order types and features

    # NOTE: Basic cancellation is covered in /account/orders/

    # NOTE: Order history is covered in /account/orders/

    # NOTE: Open orders query is covered in /account/orders/

    # NOTE: Moved to extreme_precision_orders test - removing duplication

    # NOTE: Multiple order operations are covered in /account/orders/

    # NOTE: Symbol format validation is covered in the edge case tests below

    # NOTE: Order lifecycle is covered in /account/orders/

    # NOTE: Backpack-specific details are covered in /account/orders/

    # NOTE: Date range validation is covered in /account/orders/

    # =============================================================================
    # ENHANCED ORDER TYPE TESTS - ALL SUPPORTED ORDER TYPES
    # =============================================================================

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test market order placement and immediate execution.

        Market orders should execute immediately at best available price.
        Tests the complete lifecycle from placement to execution.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.BUY

        # Get minimal order size for market order
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=current_price,
        )

        # Place market order (no price needed for market orders)
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Market orders should execute immediately
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.MARKET, "Should be market order"
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"

        # Market orders typically fill immediately
        assert placed_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED], (
            f"Market order should be filled, got {placed_order.status}"
        )

        # Should have execution price
        if placed_order.status == OrderStatus.FILLED:
            assert placed_order.quantity_filled == placed_order.quantity_requested, (
                "Fully filled market order should have equal filled/requested quantities"
            )

        logger.info(
            f"✓ Market order executed: {placed_order.exchange_order_id}, "
            f"status: {placed_order.status}, filled: {placed_order.quantity_filled}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_stop_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test stop market order placement and validation.

        Stop market orders trigger at a specific price and execute as market orders.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Stop loss on existing position

        # Get current market price and set trigger below for stop loss
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]

        # Calculate trigger price and quantize to proper tick size
        trigger_price = (current_price * Decimal("0.95")).quantize(tick_size)

        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=trigger_price,
        )

        # Place stop market order
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.STOP_MARKET,
            quantity=minimal_quantity,
            stop_price=trigger_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate stop order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.STOP_MARKET, "Should be stop market order"
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"

        # Stop orders should be pending trigger (not filled immediately)
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Stop order should be pending, got {placed_order.status}"
        )

        # Clean up
        if placed_order.exchange_order_id and placed_order.status != OrderStatus.FILLED:
            try:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_test_env.cancel_order(cancel_args)
                logger.info(f"✓ Stop market order cleaned up: {placed_order.exchange_order_id}")
            except Exception as e:
                # Order cancellation should work if order was placed successfully
                pytest.fail(
                    f"Failed to cancel stop order {placed_order.exchange_order_id}: {e}. "
                    "If we can place an order, we should be able to cancel it."
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_stop_limit_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test stop limit order placement and validation.

        Stop limit orders trigger at a specific price and become limit orders.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Stop loss

        # Get current market price
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]

        # Calculate trigger and limit prices and quantize to proper tick size
        trigger_price = (current_price * Decimal("0.95")).quantize(
            tick_size
        )  # 5% below for stop loss
        limit_price = (trigger_price * Decimal("0.99")).quantize(
            tick_size
        )  # Slightly below trigger

        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=limit_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.STOP_LIMIT,
            quantity=minimal_quantity,
            price=limit_price,
            stop_price=trigger_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate stop limit order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.STOP_LIMIT, "Should be stop limit order"
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"
        assert placed_order.price == limit_price, "Limit price should match"

        # Stop orders should be pending trigger
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Stop limit order should be pending, got {placed_order.status}"
        )

        # Clean up
        if placed_order.exchange_order_id and placed_order.status != OrderStatus.FILLED:
            try:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_test_env.cancel_order(cancel_args)
                logger.info(f"✓ Stop limit order cleaned up: {placed_order.exchange_order_id}")
            except Exception as e:
                # Order cancellation should work if order was placed successfully
                pytest.fail(
                    f"Failed to cancel stop limit order {placed_order.exchange_order_id}: {e}. "
                    "If we can place an order, we should be able to cancel it."
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_take_profit_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test take profit market order placement and validation.

        Take profit market orders trigger above current price and execute as market orders.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Taking profit on long position

        # Get current market price and set trigger above for take profit
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]

        # Calculate trigger price and quantize to proper tick size
        trigger_price = (current_price * Decimal("1.05")).quantize(
            tick_size
        )  # 5% above current price

        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=trigger_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.TAKE_PROFIT_MARKET,
            quantity=minimal_quantity,
            stop_price=trigger_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate take profit order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.TAKE_PROFIT_MARKET, (
            "Should be take profit market order"
        )
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"

        # Take profit orders should be pending trigger
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Take profit order should be pending, got {placed_order.status}"
        )

        # Clean up
        if placed_order.exchange_order_id and placed_order.status != OrderStatus.FILLED:
            try:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_test_env.cancel_order(cancel_args)
                logger.info(
                    f"✓ Take profit market order cleaned up: {placed_order.exchange_order_id}"
                )
            except Exception as e:
                # Order cancellation should work if order was placed successfully
                pytest.fail(
                    f"Failed to cancel take profit order {placed_order.exchange_order_id}: {e}. "
                    "If we can place an order, we should be able to cancel it."
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_take_profit_limit_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test take profit limit order placement and validation.

        Take profit limit orders trigger at a specific price and become limit orders.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Taking profit

        # Get current market price
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]

        # Calculate trigger and limit prices and quantize to proper tick size
        trigger_price = (current_price * Decimal("1.05")).quantize(
            tick_size
        )  # 5% above for take profit
        limit_price = (trigger_price * Decimal("1.01")).quantize(
            tick_size
        )  # Slightly above trigger

        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=limit_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.TAKE_PROFIT_LIMIT,
            quantity=minimal_quantity,
            price=limit_price,
            stop_price=trigger_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate take profit limit order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.TAKE_PROFIT_LIMIT, (
            "Should be take profit limit order"
        )
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"
        assert placed_order.price == limit_price, "Limit price should match"

        # Take profit orders should be pending trigger
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Take profit limit order should be pending, got {placed_order.status}"
        )

        # Clean up
        if placed_order.exchange_order_id and placed_order.status != OrderStatus.FILLED:
            try:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_test_env.cancel_order(cancel_args)
                logger.info(
                    f"✓ Take profit limit order cleaned up: {placed_order.exchange_order_id}"
                )
            except Exception as e:
                # Order cancellation should work if order was placed successfully
                pytest.fail(
                    f"Failed to cancel take profit limit order "
                    f"{placed_order.exchange_order_id}: {e}. "
                    "If we can place an order, we should be able to cancel it."
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_order_with_stop_loss_workflow(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test realistic trading workflow: place market order, then add stop loss.

        This simulates a real trading scenario where a trader:
        1. Enters position with market order
        2. Immediately sets stop loss protection
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        entry_side = OrderSide.BUY

        # Step 1: Place market order to enter position
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        entry_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=entry_side,
            price=current_price,
        )

        entry_args = PlaceOrderArgs(
            symbol=symbol,
            side=entry_side,
            order_type=OrderType.MARKET,
            quantity=entry_quantity,
            time_in_force=TimeInForce.GTC,
        )

        entry_order = await bp_api_for_test_env.place_order(entry_args)

        # Validate entry order
        assert entry_order.order_type == OrderType.MARKET, "Entry should be market order"
        assert entry_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED], (
            f"Market order should execute immediately, got {entry_order.status}"
        )

        logger.info(f"✓ Entry order executed: {entry_order.exchange_order_id}")

        # Step 2: Place stop loss order (assuming we now have position)
        if entry_order.status == OrderStatus.FILLED:
            # Set stop loss 5% below entry price (assuming filled at market price)
            constraints = await get_market_constraints(bp_api_for_test_env, symbol)
            tick_size = constraints["tick_size"]
            stop_price = (current_price * Decimal("0.95")).quantize(tick_size)

            stop_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.SELL,  # Opposite side to close position
                order_type=OrderType.STOP_MARKET,
                quantity=entry_quantity,  # Same quantity to close position
                stop_price=stop_price,
                time_in_force=TimeInForce.GTC,
            )

            try:
                stop_order = await bp_api_for_test_env.place_order(stop_args)

                # Validate stop loss order
                assert stop_order.order_type == OrderType.STOP_MARKET, "Should be stop market order"
                assert stop_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
                    f"Stop order should be pending, got {stop_order.status}"
                )

                logger.info(f"✓ Stop loss placed: {stop_order.exchange_order_id}")

                # Clean up stop loss
                if stop_order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=stop_order.exchange_order_id,
                            symbol=symbol,
                        )
                        await bp_api_for_test_env.cancel_order(cancel_args)
                        logger.info("✓ Stop loss order cancelled")
                    except Exception as e:
                        # Stop loss cancellation should work if the order was placed
                        pytest.fail(
                            f"Failed to cancel stop loss order: {e}. "
                            "If we can place an order, we should be able to cancel it."
                        )

            except Exception as e:
                # Stop loss placement may fail if we don't have a position
                if "position" in str(e).lower() or "balance" in str(e).lower():
                    logger.info(f"Stop loss correctly rejected - no position: {e}")
                else:
                    pytest.fail(
                        f"Stop loss placement failed with unexpected error: {e}. "
                        "Expected position-related error but got something else."
                    )

        logger.info("✓ Market order + stop loss workflow test completed")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_all_order_types_parameter_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test parameter validation for all order types.

        Validates that each order type correctly handles required and optional parameters.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)

        # Get market constraints for price quantization
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]

        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=current_price,
        )

        # Test each order type with minimal valid parameters
        order_type_tests: list[dict[str, Any]] = [
            {
                "order_type": OrderType.MARKET,
                "required_params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.MARKET,
                    "quantity": minimal_quantity,
                    "time_in_force": TimeInForce.GTC,
                },
                "optional_params": {},
            },
            {
                "order_type": OrderType.LIMIT,
                "required_params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": minimal_quantity,
                    "price": (current_price * Decimal("0.95")).quantize(tick_size),
                    "time_in_force": TimeInForce.GTC,
                },
                "optional_params": {},
            },
            {
                "order_type": OrderType.STOP_MARKET,
                "required_params": {
                    "symbol": symbol,
                    "side": OrderSide.SELL,
                    "order_type": OrderType.STOP_MARKET,
                    "quantity": minimal_quantity,
                    "stop_price": (current_price * Decimal("0.95")).quantize(tick_size),
                    "time_in_force": TimeInForce.GTC,
                },
                "optional_params": {},
            },
            {
                "order_type": OrderType.STOP_LIMIT,
                "required_params": {
                    "symbol": symbol,
                    "side": OrderSide.SELL,
                    "order_type": OrderType.STOP_LIMIT,
                    "quantity": minimal_quantity,
                    "price": (current_price * Decimal("0.94")).quantize(tick_size),
                    "stop_price": (current_price * Decimal("0.95")).quantize(tick_size),
                    "time_in_force": TimeInForce.GTC,
                },
                "optional_params": {},
            },
        ]

        placed_orders: list[Order] = []

        for test_case in order_type_tests:
            try:
                # Combine required and optional parameters
                all_params: dict[str, Any] = {
                    **test_case["required_params"],
                    **test_case["optional_params"],
                }
                place_args = PlaceOrderArgs(**all_params)

                placed_order = await bp_api_for_test_env.place_order(place_args)
                placed_orders.append(placed_order)

                # Validate order was placed with correct type
                assert placed_order.order_type == test_case["order_type"], (
                    f"Order type mismatch: expected {test_case['order_type']}, "
                    f"got {placed_order.order_type}"
                )

                logger.info(
                    f"✓ {test_case['order_type'].value} order validation passed: "
                    f"{placed_order.exchange_order_id}"
                )

            except Exception as e:
                # Order type tests should work with adequate balance
                pytest.fail(
                    f"Order type {test_case['order_type'].value} test failed: {e}. "
                    "All order types should be placeable with adequate balance."
                )

        # Clean up all placed orders
        for order in placed_orders:
            if order.exchange_order_id and order.status not in [
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
            ]:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=symbol,
                    )
                    await bp_api_for_test_env.cancel_order(cancel_args)
                except Exception as e:
                    # Order cancellation should work
                    pytest.fail(
                        f"Failed to cancel order {order.exchange_order_id}: {e}. "
                        "Order cancellation is a critical operation that must work."
                    )

        logger.info(f"✓ Parameter validation completed for {len(placed_orders)} order types")

    # =============================================================================
    # EDGE CASE AND PRECISION TESTS
    # =============================================================================

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_extreme_precision_orders(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test orders with extreme precision values and edge cases.

        Tests boundary conditions for decimal precision, very small quantities,
        and price precision limits.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC

        # Get market constraints for precision testing
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]
        step_size = constraints["step_size"]
        min_quantity = constraints.get("min_quantity", step_size)

        current_price = await get_current_market_price(bp_api_for_test_env, symbol)

        precision_test_cases: list[dict[str, Any]] = [
            {
                "name": "minimum_quantity_edge",
                "quantity": min_quantity,
                "price": (current_price * Decimal("0.95")).quantize(tick_size),
            },
            {
                "name": "minimum_plus_one_step",
                "quantity": min_quantity + step_size,
                "price": (current_price * Decimal("0.96")).quantize(tick_size),
            },
            {
                "name": "price_minimum_tick",
                "quantity": min_quantity * Decimal("2"),
                "price": (current_price * Decimal("0.95")).quantize(tick_size),
            },
            {
                "name": "maximum_precision_quantity",
                "quantity": (min_quantity * Decimal("1.123456789")).quantize(step_size),
                "price": (current_price * Decimal("0.94")).quantize(tick_size),
            },
        ]

        placed_orders: list[Order] = []

        for test_case in precision_test_cases:
            try:
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=test_case["quantity"],
                    price=test_case["price"],
                    time_in_force=TimeInForce.GTC,
                )

                placed_order = await bp_api_for_test_env.place_order(place_args)
                placed_orders.append(placed_order)

                # Validate precision was maintained
                assert placed_order.quantity_requested == test_case["quantity"], (
                    f"Precision test {test_case['name']}: quantity precision not maintained"
                )
                assert placed_order.price == test_case["price"], (
                    f"Precision test {test_case['name']}: price precision not maintained"
                )

                logger.info(
                    f"✓ Precision test '{test_case['name']}' passed: "
                    f"qty={placed_order.quantity_requested}, price={placed_order.price}"
                )

            except Exception as e:
                # Precision tests should work with valid market constraints
                pytest.fail(
                    f"Precision test '{test_case['name']}' failed: {e}. "
                    "Precision handling is critical for trading operations."
                )

        # Clean up
        for order in placed_orders:
            if order.exchange_order_id:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=symbol,
                    )
                    await bp_api_for_test_env.cancel_order(cancel_args)
                except Exception as e:
                    # Order cancellation should work
                    pytest.fail(
                        f"Failed to cancel precision test order: {e}. "
                        "Order cancellation is a critical operation that must work."
                    )

        logger.info(f"✓ Extreme precision tests completed: {len(placed_orders)} orders tested")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_large_order_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test large order scenarios that should succeed with sufficient balance.

        Tests larger orders that should be accepted by the exchange when balance permits.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC

        # Get constraints and current price
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        min_quantity = constraints.get("min_quantity", Decimal("0.01"))
        tick_size = constraints["tick_size"]
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)

        # Test progressively larger orders
        large_order_multipliers = [10, 50, 100]  # 10x, 50x, 100x minimum
        placed_orders: list[Order] = []

        for multiplier in large_order_multipliers:
            try:
                large_quantity = min_quantity * Decimal(str(multiplier))
                test_price = (current_price * Decimal("0.9")).quantize(
                    tick_size
                )  # Well below market

                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=large_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                )

                placed_order = await bp_api_for_test_env.place_order(place_args)
                placed_orders.append(placed_order)

                # Validate large order was accepted
                assert placed_order.quantity_requested == large_quantity, (
                    f"Large order {multiplier}x: quantity mismatch"
                )
                assert placed_order.status == OrderStatus.OPEN, (
                    f"Large order {multiplier}x should be open, got {placed_order.status}"
                )

                logger.info(
                    f"✓ Large order {multiplier}x minimum accepted: "
                    f"qty={large_quantity}, id={placed_order.exchange_order_id}"
                )

            except Exception as e:
                # Large orders may fail due to balance/limits - validate the error type
                if (
                    "balance" in str(e).lower()
                    or "insufficient" in str(e).lower()
                    or "limit" in str(e).lower()
                ):
                    logger.info(f"Large order {multiplier}x correctly rejected due to limits: {e}")
                    break  # Expected - stop testing larger orders
                else:
                    pytest.fail(
                        f"Large order {multiplier}x failed with unexpected error: {e}. "
                        "Expected balance/limit error but got something else."
                    )

        # Clean up
        for order in placed_orders:
            if order.exchange_order_id:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=symbol,
                    )
                    await bp_api_for_test_env.cancel_order(cancel_args)
                except Exception as e:
                    # Order cancellation should work
                    pytest.fail(
                        f"Failed to cancel large order: {e}. "
                        "Order cancellation is a critical operation that must work."
                    )

        logger.info(f"✓ Large order edge case tests completed: {len(placed_orders)} orders tested")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_symbols_order_management(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order management across multiple trading pairs.

        Validates that order operations work correctly across different symbols.
        """
        _ = custom_vcr_config

        # Test with common spot symbols
        test_symbols = [TEST_SYMBOL_SOL_USDC, TEST_SYMBOL_BTC_USDC]  # Start with 2 symbols
        placed_orders: list[Order] = []

        for symbol in test_symbols:
            try:
                # Get symbol-specific parameters
                current_price = await get_current_market_price(bp_api_for_test_env, symbol)

                # Get market constraints for price quantization
                constraints = await get_market_constraints(bp_api_for_test_env, symbol)
                tick_size = constraints["tick_size"]

                minimal_quantity = await get_minimal_order_size(
                    api=bp_api_for_test_env,
                    symbol=symbol,
                    side=OrderSide.BUY,
                    price=current_price,
                )

                test_price = (current_price * Decimal("0.95")).quantize(
                    tick_size
                )  # 5% below market

                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=minimal_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                )

                placed_order = await bp_api_for_test_env.place_order(place_args)
                placed_orders.append(placed_order)

                # Validate order for this symbol
                assert placed_order.symbol == symbol, f"Symbol mismatch: expected {symbol}"
                assert placed_order.status == OrderStatus.OPEN, f"Order for {symbol} should be open"

                logger.info(f"✓ Order placed for {symbol}: {placed_order.exchange_order_id}")

            except Exception as e:
                # Multi-symbol order placement should work
                pytest.fail(
                    f"Failed to place order for {symbol}: {e}. "
                    "Multi-symbol trading is a core requirement."
                )

        # Validate we can query orders across symbols
        if placed_orders:
            try:
                all_open_orders = await bp_api_for_test_env.get_open_orders()
                placed_order_ids = {order.exchange_order_id for order in placed_orders}
                open_order_ids = {order.exchange_order_id for order in all_open_orders}

                # Check that our orders appear in open orders
                for order_id in placed_order_ids:
                    assert order_id in open_order_ids, f"Order {order_id} should be in open orders"

                logger.info(f"✓ All {len(placed_orders)} orders found in open orders query")

            except Exception as e:
                # Open orders query should work
                pytest.fail(
                    f"Failed to validate open orders: {e}. "
                    "Querying open orders is a critical operation."
                )

        # Clean up all orders
        for order in placed_orders:
            if order.exchange_order_id:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=order.symbol,
                    )
                    await bp_api_for_test_env.cancel_order(cancel_args)
                except Exception as e:
                    # Order cancellation should work
                    pytest.fail(
                        f"Failed to cancel order for {order.symbol}: {e}. "
                        "Order cancellation is a critical operation that must work."
                    )

        logger.info(
            f"✓ Multi-symbol order management completed: {len(test_symbols)} symbols tested"
        )
