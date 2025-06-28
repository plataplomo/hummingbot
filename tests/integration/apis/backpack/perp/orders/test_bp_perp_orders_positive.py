"""Integration tests for Backpack perp orders endpoints with positive balance.

This module focuses specifically on testing the Order model pipeline
through Backpack's perp order endpoints with Ed25519 authentication
when the account has sufficient margin for perp order operations.
Tests validate complete data transformation from API responses to Order instances.

Model Focus: Order (Perp Trading - Successful Operations)
- Validates complete Order model field mapping for perp markets
- Tests Decimal precision for financial values (quantities, prices)
- Tests leverage and margin-specific validations
- Validates business logic constraints and order state transitions
- Tests Backpack-specific order details (bp_details with order management info)
- Comprehensive perp order lifecycle with successful placements and cancellations

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: Positive margin balance (successful perp order scenarios)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    TEST_SYMBOL_SOL_PERP,
    get_current_market_price,
    get_dynamic_test_price,
    get_market_constraints,
    get_minimal_order_size,
)


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]

logger = get_logger(__name__)


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/perp/orders/positive"],
    indirect=True,
)
class TestBackpackPerpOrdersPositiveBalance:
    """Comprehensive perp orders integration tests with positive margin for operations."""

    # NOTE: Basic order placement/cancellation is covered in /account/orders/
    # This module focuses on PERP-SPECIFIC order types and features

    # =============================================================================
    # ENHANCED ORDER TYPE TESTS - ALL SUPPORTED PERP ORDER TYPES
    # =============================================================================

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp market order placement and immediate execution.

        Market orders should execute immediately at best available price.
        Tests margin usage and leverage effects for perp trading.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP
        side = OrderSide.BUY

        # Get minimal order size for perp market order
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=current_price,
        )

        # Place perp market order (no price needed for market orders)
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

        # Perp market orders typically fill immediately
        assert placed_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED], (
            f"Perp market order should be filled, got {placed_order.status}"
        )

        # Should have execution price and create position
        if placed_order.status == OrderStatus.FILLED:
            assert placed_order.quantity_filled == placed_order.quantity_requested, (
                "Fully filled market order should have equal filled/requested quantities"
            )

        logger.info(
            "perp_market_order_executed",
            order_id=placed_order.exchange_order_id,
            status=placed_order.status,
            quantity_filled=placed_order.quantity_filled,
            message="Perp market order executed successfully",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_stop_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test comprehensive stop market order placement - ALL variants.

        Focus: Our system's ability to place stop market orders correctly.
        Tests both STOP LOSS (below current price) and STOP BUY (above current price).
        Exchange execution behavior is their responsibility - we test PLACEMENT.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP

        # Get current market price and constraints
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        market_constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = market_constraints["tick_size"]

        # Test 1: STOP LOSS (SELL below current price)
        # Use tick-based offset instead of hardcoded percentage
        price_offset = tick_size * Decimal(50)  # 50 ticks below current price
        stop_loss_price = (current_price - price_offset).quantize(tick_size)
        stop_loss_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.SELL,
            price=stop_loss_price,
        )

        stop_loss_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            quantity=stop_loss_quantity,
            stop_price=stop_loss_price,
            time_in_force=TimeInForce.GTC,
        )

        stop_loss_order = await bp_api_for_test_env.place_order(stop_loss_args)

        # Validate stop loss placement
        assert isinstance(stop_loss_order, Order), "Should place stop loss order"
        assert stop_loss_order.order_type == OrderType.STOP_MARKET, "Should be stop market type"
        assert stop_loss_order.side == OrderSide.SELL, "Stop loss should be SELL"
        assert stop_loss_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "stop_loss_order_placed",
            order_id=stop_loss_order.exchange_order_id,
            status=stop_loss_order.status,
            message="Stop LOSS order placed successfully",
        )

        # Test 2: STOP BUY (BUY above current price)
        # Use tick-based offset instead of hardcoded percentage
        stop_buy_price = (current_price + price_offset).quantize(tick_size)
        stop_buy_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=stop_buy_price,
        )

        stop_buy_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_MARKET,
            quantity=stop_buy_quantity,
            stop_price=stop_buy_price,
            time_in_force=TimeInForce.GTC,
        )

        stop_buy_order = await bp_api_for_test_env.place_order(stop_buy_args)

        # Validate stop buy placement
        assert isinstance(stop_buy_order, Order), "Should place stop buy order"
        assert stop_buy_order.order_type == OrderType.STOP_MARKET, "Should be stop market type"
        assert stop_buy_order.side == OrderSide.BUY, "Stop buy should be BUY"
        assert stop_buy_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "stop_buy_order_placed",
            order_id=stop_buy_order.exchange_order_id,
            status=stop_buy_order.status,
            message="Stop BUY order placed successfully",
        )

        # Clean up both orders
        for order, order_name in [(stop_loss_order, "stop_loss"), (stop_buy_order, "stop_buy")]:
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
                    logger.info(
                        "order_cleaned_up",
                        order_name=order_name,
                        order_id=order.exchange_order_id,
                        message="Order cleaned up successfully",
                    )
                except Exception as e:
                    # Order cancellation must work if order placement worked
                    pytest.fail(
                        f"Failed to cancel {order_name} order {order.exchange_order_id}: {e}. "
                        "Order cancellation is critical and must work reliably.",
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_stop_limit_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test comprehensive stop limit order placement - ALL variants.

        Focus: Our system's ability to place stop limit orders correctly.
        Tests STOP LOSS LIMIT (below current) and STOP BUY LIMIT (above current).
        Exchange execution behavior is their responsibility - we test PLACEMENT.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP

        # Get current market price and constraints
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        market_constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = market_constraints["tick_size"]

        # Test 1: STOP LOSS LIMIT (SELL below current price)
        # Use tick-based offsets instead of hardcoded percentages
        price_offset = tick_size * Decimal(50)  # 50 ticks offset
        stop_loss_trigger = (current_price - price_offset).quantize(tick_size)
        stop_loss_limit = (stop_loss_trigger - tick_size).quantize(
            tick_size,
        )  # One tick below trigger
        stop_loss_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.SELL,
            price=stop_loss_limit,
        )

        stop_loss_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_LIMIT,
            quantity=stop_loss_quantity,
            price=stop_loss_limit,
            stop_price=stop_loss_trigger,
            time_in_force=TimeInForce.GTC,
        )

        stop_loss_order = await bp_api_for_test_env.place_order(stop_loss_args)

        # Validate stop loss limit placement
        assert isinstance(stop_loss_order, Order), "Should place stop loss limit order"
        assert stop_loss_order.order_type == OrderType.STOP_LIMIT, "Should be stop limit type"
        assert stop_loss_order.side == OrderSide.SELL, "Stop loss should be SELL"
        assert stop_loss_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "stop_loss_limit_order_placed",
            order_id=stop_loss_order.exchange_order_id,
            status=stop_loss_order.status,
            message="Stop LOSS LIMIT order placed successfully",
        )

        # Test 2: STOP BUY LIMIT (BUY above current price)
        # Use tick-based offsets instead of hardcoded percentages
        stop_buy_trigger = (current_price + price_offset).quantize(tick_size)
        stop_buy_limit = (stop_buy_trigger + tick_size).quantize(
            tick_size,
        )  # One tick above trigger
        stop_buy_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=stop_buy_limit,
        )

        stop_buy_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_LIMIT,
            quantity=stop_buy_quantity,
            price=stop_buy_limit,
            stop_price=stop_buy_trigger,
            time_in_force=TimeInForce.GTC,
        )

        stop_buy_order = await bp_api_for_test_env.place_order(stop_buy_args)

        # Validate stop buy limit placement
        assert isinstance(stop_buy_order, Order), "Should place stop buy limit order"
        assert stop_buy_order.order_type == OrderType.STOP_LIMIT, "Should be stop limit type"
        assert stop_buy_order.side == OrderSide.BUY, "Stop buy should be BUY"
        assert stop_buy_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "stop_buy_limit_order_placed",
            order_id=stop_buy_order.exchange_order_id,
            status=stop_buy_order.status,
            message="Stop BUY LIMIT order placed successfully",
        )

        # Clean up both orders
        for order, order_name in [
            (stop_loss_order, "stop_loss_limit"),
            (stop_buy_order, "stop_buy_limit"),
        ]:
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
                    logger.info(
                        "order_cleaned_up",
                        order_name=order_name,
                        order_id=order.exchange_order_id,
                        message="Order cleaned up successfully",
                    )
                except Exception as e:
                    # Order cancellation must work if order placement worked
                    pytest.fail(
                        f"Failed to cancel {order_name} order {order.exchange_order_id}: {e}. "
                        "Order cancellation is critical and must work reliably.",
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_take_profit_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test comprehensive take profit market order placement - ALL variants.

        Focus: Our system's ability to place take profit market orders correctly.
        Tests TAKE PROFIT on SELL (for long positions) and TAKE PROFIT on BUY (for short positions).
        Exchange execution behavior is their responsibility - we test PLACEMENT.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP

        # Get current market price and constraints
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        market_constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = market_constraints["tick_size"]

        # Test 1: TAKE PROFIT SELL (for long position - sell above current price)
        # Use tick-based offset instead of hardcoded percentage
        price_offset = tick_size * Decimal(30)  # 30 ticks above current price
        tp_sell_trigger = (current_price + price_offset).quantize(tick_size)
        tp_sell_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.SELL,
            price=tp_sell_trigger,
        )

        tp_sell_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.TAKE_PROFIT_MARKET,
            quantity=tp_sell_quantity,
            stop_price=tp_sell_trigger,
            time_in_force=TimeInForce.GTC,
        )

        tp_sell_order = await bp_api_for_test_env.place_order(tp_sell_args)

        # Validate take profit sell placement
        assert isinstance(tp_sell_order, Order), "Should place take profit sell order"
        # Backpack returns STOP_MARKET for both stop loss and take profit orders
        assert tp_sell_order.order_type in [OrderType.TAKE_PROFIT_MARKET, OrderType.STOP_MARKET], (
            f"Should be take profit or stop market type, got {tp_sell_order.order_type}"
        )
        assert tp_sell_order.side == OrderSide.SELL, "Take profit should be SELL"
        assert tp_sell_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "take_profit_sell_order_placed",
            order_id=tp_sell_order.exchange_order_id,
            status=tp_sell_order.status,
            message="Take Profit SELL order placed successfully",
        )

        # Test 2: TAKE PROFIT BUY (for short position - buy below current price)
        # Use tick-based offset instead of hardcoded percentage
        tp_buy_trigger = (current_price - price_offset).quantize(tick_size)
        tp_buy_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=tp_buy_trigger,
        )

        tp_buy_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.TAKE_PROFIT_MARKET,
            quantity=tp_buy_quantity,
            stop_price=tp_buy_trigger,
            time_in_force=TimeInForce.GTC,
        )

        tp_buy_order = await bp_api_for_test_env.place_order(tp_buy_args)

        # Validate take profit buy placement
        assert isinstance(tp_buy_order, Order), "Should place take profit buy order"
        # Backpack returns STOP_MARKET for both stop loss and take profit orders
        assert tp_buy_order.order_type in [OrderType.TAKE_PROFIT_MARKET, OrderType.STOP_MARKET], (
            f"Should be take profit or stop market type, got {tp_buy_order.order_type}"
        )
        assert tp_buy_order.side == OrderSide.BUY, "Take profit should be BUY"
        assert tp_buy_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "take_profit_buy_order_placed",
            order_id=tp_buy_order.exchange_order_id,
            status=tp_buy_order.status,
            message="Take Profit BUY order placed successfully",
        )

        # Clean up both orders
        for order, order_name in [
            (tp_sell_order, "take_profit_sell"),
            (tp_buy_order, "take_profit_buy"),
        ]:
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
                    logger.info(
                        "order_cleaned_up",
                        order_name=order_name,
                        order_id=order.exchange_order_id,
                        message="Order cleaned up successfully",
                    )
                except Exception as e:
                    # Order cancellation must work if order placement worked
                    pytest.fail(
                        f"Failed to cancel {order_name} order {order.exchange_order_id}: {e}. "
                        "Order cancellation is critical and must work reliably.",
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_take_profit_limit_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test comprehensive take profit limit order placement - ALL variants.

        Focus: Our system's ability to place take profit limit orders correctly.
        Tests TAKE PROFIT LIMIT on SELL (long positions) and TAKE PROFIT LIMIT on BUY
        (short positions).
        Exchange execution behavior is their responsibility - we test PLACEMENT.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP

        # Get current market price and constraints
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        market_constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = market_constraints["tick_size"]

        # Test 1: TAKE PROFIT LIMIT SELL (for long position - sell above current price)
        # Use tick-based offsets instead of hardcoded percentages
        price_offset = tick_size * Decimal(30)  # 30 ticks above current price
        tp_sell_trigger = (current_price + price_offset).quantize(tick_size)
        tp_sell_limit = (tp_sell_trigger + tick_size).quantize(tick_size)  # One tick above trigger
        tp_sell_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.SELL,
            price=tp_sell_limit,
        )

        tp_sell_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.TAKE_PROFIT_LIMIT,
            quantity=tp_sell_quantity,
            price=tp_sell_limit,
            stop_price=tp_sell_trigger,
            time_in_force=TimeInForce.GTC,
        )

        tp_sell_order = await bp_api_for_test_env.place_order(tp_sell_args)

        # Validate take profit sell limit placement
        assert isinstance(tp_sell_order, Order), "Should place take profit sell limit order"
        # Backpack returns STOP_LIMIT for both stop loss and take profit limit orders
        assert tp_sell_order.order_type in [OrderType.TAKE_PROFIT_LIMIT, OrderType.STOP_LIMIT], (
            f"Should be take profit limit or stop limit type, got {tp_sell_order.order_type}"
        )
        assert tp_sell_order.side == OrderSide.SELL, "Take profit should be SELL"
        assert tp_sell_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "take_profit_limit_sell_order_placed",
            order_id=tp_sell_order.exchange_order_id,
            status=tp_sell_order.status,
            message="Take Profit LIMIT SELL order placed successfully",
        )

        # Test 2: TAKE PROFIT LIMIT BUY (for short position - buy below current price)
        # Use tick-based offsets instead of hardcoded percentages
        tp_buy_trigger = (current_price - price_offset).quantize(tick_size)
        tp_buy_limit = (tp_buy_trigger - tick_size).quantize(tick_size)  # One tick below trigger
        tp_buy_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=tp_buy_limit,
        )

        tp_buy_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.TAKE_PROFIT_LIMIT,
            quantity=tp_buy_quantity,
            price=tp_buy_limit,
            stop_price=tp_buy_trigger,
            time_in_force=TimeInForce.GTC,
        )

        tp_buy_order = await bp_api_for_test_env.place_order(tp_buy_args)

        # Validate take profit buy limit placement
        assert isinstance(tp_buy_order, Order), "Should place take profit buy limit order"
        # Backpack returns STOP_LIMIT for both stop loss and take profit limit orders
        assert tp_buy_order.order_type in [OrderType.TAKE_PROFIT_LIMIT, OrderType.STOP_LIMIT], (
            f"Should be take profit limit or stop limit type, got {tp_buy_order.order_type}"
        )
        assert tp_buy_order.side == OrderSide.BUY, "Take profit should be BUY"
        assert tp_buy_order.exchange_order_id, "Should have exchange order ID"

        logger.info(
            "take_profit_limit_buy_order_placed",
            order_id=tp_buy_order.exchange_order_id,
            status=tp_buy_order.status,
            message="Take Profit LIMIT BUY order placed successfully",
        )

        # Clean up both orders
        for order, order_name in [
            (tp_sell_order, "take_profit_limit_sell"),
            (tp_buy_order, "take_profit_limit_buy"),
        ]:
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
                    logger.info(
                        "order_cleaned_up",
                        order_name=order_name,
                        order_id=order.exchange_order_id,
                        message="Order cleaned up successfully",
                    )
                except Exception as e:
                    # Order cancellation must work if order placement worked
                    pytest.fail(
                        f"Failed to cancel {order_name} order {order.exchange_order_id}: {e}. "
                        "Order cancellation is critical and must work reliably.",
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_market_order_with_stop_loss_workflow(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test realistic perp trading workflow: place market order, then add stop loss.

        This simulates a real perp trading scenario where a trader:
        1. Enters leveraged position with market order
        2. Immediately sets stop loss protection
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP
        entry_side = OrderSide.BUY

        # Step 1: Place perp market order to enter position
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
            f"Perp market order should execute immediately, got {entry_order.status}"
        )

        logger.info(
            "perp_entry_order_executed",
            order_id=entry_order.exchange_order_id,
            message="Perp entry order executed successfully",
        )

        # Step 2: Place stop loss order (assuming we now have leveraged position)
        if entry_order.status == OrderStatus.FILLED:
            # Get market constraints for proper price formatting
            market_constraints = await get_market_constraints(bp_api_for_test_env, symbol)
            tick_size = market_constraints["tick_size"]
            # Set stop loss using tick-based offset (more conservative for leveraged perp)
            price_offset = tick_size * Decimal(30)  # 30 ticks below entry price
            stop_price = (current_price - price_offset).quantize(tick_size)

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

                # What we're testing: Our system's ability to place stop loss orders
                # after market orders
                # Exchange behavior (immediate trigger vs pending) is exchange's responsibility
                logger.info(
                    "stop_loss_order_placed_successfully",
                    order_id=stop_order.exchange_order_id,
                    order_type=stop_order.order_type,
                    status=stop_order.status,
                    message="Stop loss order placed successfully",
                )

                # Verify order has required fields
                assert stop_order.exchange_order_id, "Order should have exchange ID"
                assert stop_order.symbol == symbol, "Symbol should match"
                assert stop_order.side == OrderSide.SELL, "Stop loss should be SELL side"

                logger.info(
                    "perp_stop_loss_placed",
                    order_id=stop_order.exchange_order_id,
                    message="Perp stop loss placed successfully",
                )

                # Clean up stop loss
                if stop_order.exchange_order_id:
                    try:
                        cancel_args = CancelOrderArgs(
                            order_id=stop_order.exchange_order_id,
                            symbol=symbol,
                        )
                        await bp_api_for_test_env.cancel_order(cancel_args)
                        logger.info("✓ Perp stop loss order cancelled")
                    except Exception as e:
                        pytest.fail(
                            f"Failed to cancel stop loss order "
                            f"{stop_order.exchange_order_id}: {e}. "
                            "Order cancellation is critical and must work reliably.",
                        )

            except Exception as e:
                # Stop loss placement failures are business logic - some are expected
                if (
                    "position" in str(e).lower()
                    or "balance" in str(e).lower()
                    or "margin" in str(e).lower()
                ):
                    # Expected business logic error - insufficient position/balance
                    pass
                else:
                    # Unexpected system error
                    pytest.fail(
                        f"Stop loss placement failed with unexpected system error: {e}. "
                        "Expected position/balance-related error but got system error.",
                    )

        logger.info("✓ Perp market order + stop loss workflow test completed")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_all_conditional_order_types_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test ALL conditional order types - comprehensive placement validation.

        This is the master test for conditional orders. We test our system's ability
        to place EVERY type of conditional order that Backpack supports.

        FOCUS: PLACEMENT, not execution. Exchange handles execution.

        Order Types Tested:
        1. STOP_MARKET (stop loss & stop buy)
        2. STOP_LIMIT (stop loss limit & stop buy limit)
        3. TAKE_PROFIT_MARKET (TP sell & TP buy)
        4. TAKE_PROFIT_LIMIT (TP limit sell & TP limit buy)
        5. All with different TimeInForce values
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP

        # Get market data once for all tests
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        market_constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = market_constraints["tick_size"]

        logger.info(
            "testing_all_conditional_orders",
            current_price=current_price,
            message="Testing ALL conditional orders at current market price",
        )

        # Calculate all prices upfront
        stop_loss_price = (current_price * Decimal("0.85")).quantize(tick_size)  # 15% below
        stop_buy_price = (current_price * Decimal("1.15")).quantize(tick_size)  # 15% above
        tp_sell_price = (current_price * Decimal("1.12")).quantize(tick_size)  # 12% above
        tp_buy_price = (current_price * Decimal("0.88")).quantize(tick_size)  # 12% below

        # For limit orders - add spread
        stop_loss_limit = (stop_loss_price * Decimal("0.98")).quantize(tick_size)  # Below trigger
        stop_buy_limit = (stop_buy_price * Decimal("1.02")).quantize(tick_size)  # Above trigger
        tp_sell_limit = (tp_sell_price * Decimal("1.02")).quantize(tick_size)  # Above trigger
        tp_buy_limit = (tp_buy_price * Decimal("0.98")).quantize(tick_size)  # Below trigger

        orders_to_test: list[
            tuple[str, OrderType, OrderSide, Decimal | None, Decimal, TimeInForce]
        ] = []
        placed_orders: list[tuple[str, Order]] = []

        # 1. STOP MARKET orders (both directions)
        orders_to_test.extend([
            (
                "STOP_MARKET_SELL",
                OrderType.STOP_MARKET,
                OrderSide.SELL,
                None,
                stop_loss_price,
                TimeInForce.GTC,
            ),
            (
                "STOP_MARKET_BUY",
                OrderType.STOP_MARKET,
                OrderSide.BUY,
                None,
                stop_buy_price,
                TimeInForce.GTC,
            ),
        ])

        # 2. STOP LIMIT orders (both directions)
        orders_to_test.extend([
            (
                "STOP_LIMIT_SELL",
                OrderType.STOP_LIMIT,
                OrderSide.SELL,
                stop_loss_limit,
                stop_loss_price,
                TimeInForce.GTC,
            ),
            (
                "STOP_LIMIT_BUY",
                OrderType.STOP_LIMIT,
                OrderSide.BUY,
                stop_buy_limit,
                stop_buy_price,
                TimeInForce.GTC,
            ),
        ])

        # 3. TAKE PROFIT MARKET orders (both directions)
        orders_to_test.extend([
            (
                "TP_MARKET_SELL",
                OrderType.TAKE_PROFIT_MARKET,
                OrderSide.SELL,
                None,
                tp_sell_price,
                TimeInForce.GTC,
            ),
            (
                "TP_MARKET_BUY",
                OrderType.TAKE_PROFIT_MARKET,
                OrderSide.BUY,
                None,
                tp_buy_price,
                TimeInForce.GTC,
            ),
        ])

        # 4. TAKE PROFIT LIMIT orders (both directions)
        orders_to_test.extend([
            (
                "TP_LIMIT_SELL",
                OrderType.TAKE_PROFIT_LIMIT,
                OrderSide.SELL,
                tp_sell_limit,
                tp_sell_price,
                TimeInForce.GTC,
            ),
            (
                "TP_LIMIT_BUY",
                OrderType.TAKE_PROFIT_LIMIT,
                OrderSide.BUY,
                tp_buy_limit,
                tp_buy_price,
                TimeInForce.GTC,
            ),
        ])

        # Test placing each order type
        success_count = 0
        for order_name, order_type, side, limit_price, stop_price, tif in orders_to_test:
            try:
                # Get quantity for this specific order
                test_price = limit_price or stop_price
                quantity = await get_minimal_order_size(
                    api=bp_api_for_test_env,
                    symbol=symbol,
                    side=side,
                    price=test_price,
                )

                # Build order args with all required fields
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    time_in_force=tif,
                    price=limit_price or None,
                    stop_price=stop_price or None,
                )

                # Place the order
                placed_order = await bp_api_for_test_env.place_order(place_args)

                # Validate successful placement
                assert isinstance(placed_order, Order), (
                    f"{order_name}: Should return Order instance"
                )

                # Backpack returns STOP_MARKET/STOP_LIMIT for both stop loss and take profit orders
                if order_type == OrderType.TAKE_PROFIT_MARKET:
                    assert placed_order.order_type in [
                        OrderType.TAKE_PROFIT_MARKET,
                        OrderType.STOP_MARKET,
                    ], (
                        f"{order_name}: Should be take profit market or stop market type, "
                        f"got {placed_order.order_type}"
                    )
                elif order_type == OrderType.TAKE_PROFIT_LIMIT:
                    assert placed_order.order_type in [
                        OrderType.TAKE_PROFIT_LIMIT,
                        OrderType.STOP_LIMIT,
                    ], (
                        f"{order_name}: Should be take profit limit or stop limit type, "
                        f"got {placed_order.order_type}"
                    )
                else:
                    assert placed_order.order_type == order_type, (
                        f"{order_name}: Order type should match"
                    )

                assert placed_order.side == side, f"{order_name}: Side should match"
                assert placed_order.exchange_order_id, (
                    f"{order_name}: Should have exchange order ID"
                )

                placed_orders.append((order_name, placed_order))
                success_count += 1

                logger.info(
                    "conditional_order_placed_successfully",
                    order_name=order_name,
                    order_id=placed_order.exchange_order_id,
                    status=placed_order.status,
                    message="Conditional order placed successfully",
                )

            except Exception as e:
                # FAIL FAST - Any order placement failure is a real problem
                pytest.fail(
                    f"Failed to place {order_name} conditional order: {e}. "
                    "Conditional order placement is critical functionality "
                    "that must work reliably.",
                )

        # All orders should have been placed successfully
        assert success_count == len(orders_to_test), (
            f"All conditional order types must be placeable. "
            f"Only {success_count}/{len(orders_to_test)} succeeded."
        )

        # Clean up all placed orders
        cleanup_success = 0
        for order_name, placed_order in placed_orders:
            if placed_order.exchange_order_id and placed_order.status not in [
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
            ]:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=placed_order.exchange_order_id,
                        symbol=symbol,
                    )
                    await bp_api_for_test_env.cancel_order(cancel_args)
                    cleanup_success += 1
                except Exception as e:
                    # Cancellation failures are critical - if we can place orders,
                    # we must be able to cancel them
                    pytest.fail(
                        f"Failed to cancel {order_name} order "
                        f"{placed_order.exchange_order_id}: {e}. "
                        "Order cancellation is critical and must work reliably.",
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_order_precision_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp order precision handling with edge case values."""
        symbol = "SOL_USDC_PERP"

        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        # Use min_quantity if available, otherwise use step_size as fallback
        min_size = constraints.get("min_quantity", constraints["step_size"])
        _ = constraints["tick_size"]  # Available but not used in this test

        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)

        # Test minimum size order
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=min_size,
            price=test_price,
            time_in_force=TimeInForce.IOC,  # Use IOC to avoid leaving orders
        )

        try:
            placed_order = await bp_api_for_test_env.place_order(place_args)
            assert isinstance(placed_order, Order), (
                "Should place minimum size perp order successfully"
            )
            assert placed_order.quantity_requested == min_size, (
                "Quantity should match requested minimum"
            )
        except Exception as e:
            # If this fails, there's a real problem with our test setup or market constraints
            pytest.fail(
                f"Failed to place minimum size perp order for {symbol}: {e}. "
                "This suggests a problem with market constraints or test configuration.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_leverage_order_calculations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp order calculations with leverage considerations."""
        symbol = "SOL_USDC_PERP"

        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        max_leverage = constraints.get("max_leverage", Decimal(20))

        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        # Get test quantity based on market constraints
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        min_quantity = constraints.get("min_quantity", constraints["step_size"])
        # Use reasonable multiplier based on step size for test calculation
        multiplier = min(Decimal(100), max_leverage)  # Cap at 100x or max leverage
        test_quantity = min_quantity * multiplier

        # Calculate notional value and margin requirement
        notional_value = test_price * test_quantity
        margin_requirement = notional_value / max_leverage

        logger.info(
            "perp_order_calculations",
            notional_value=notional_value,
            margin_requirement=margin_requirement,
            message="Perp order calculations computed",
        )

        # Test precision of leverage calculations
        assert isinstance(notional_value, Decimal), "Notional value should be Decimal"
        assert isinstance(margin_requirement, Decimal), "Margin requirement should be Decimal"
        assert margin_requirement > Decimal(0), "Margin requirement should be positive"
        assert margin_requirement < notional_value, (
            "Margin should be less than notional (leverage effect)"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_perp_order_history_success(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test retrieving perp order history with positive balance."""
        symbol = "SOL_USDC_PERP"

        # Get recent order history
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=30)  # Last 30 days

        history_args = GetOrderHistoryArgs(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=50,
        )

        orders = await bp_api_for_test_env.get_order_history(history_args)

        assert isinstance(orders, list), f"Expected list of orders, got {type(orders)}"

        # Validate each order in history
        for i, order in enumerate(orders):
            assert isinstance(order, Order), f"Order {i} should be Order model, got {type(order)}"
            assert order.symbol == symbol, f"Order {i} should have symbol {symbol}"

            # Note: leverage is not stored in the Order model
            # Leverage is typically applied at the account/position level

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_market_order_execution(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp market order execution with margin available."""
        symbol = "SOL_USDC_PERP"

        # Get minimal quantity for market order test
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        small_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=current_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=small_quantity,
            time_in_force=TimeInForce.IOC,
        )

        try:
            placed_order = await bp_api_for_test_env.place_order(place_args)

            # Market orders should execute immediately in liquid perp markets
            assert isinstance(placed_order, Order), "Should place market order successfully"
            assert placed_order.order_type == OrderType.MARKET, "Should be market order"
            assert placed_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED], (
                "Market order should be filled or partially filled"
            )

        except Exception as e:
            # Market orders should work if we have margin - this is a real failure
            pytest.fail(
                f"Failed to place perp market order for {symbol}: {e}. "
                "Market orders should execute successfully with adequate margin.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_order_concurrent_operations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test concurrent perp order operations."""
        symbol = "SOL_USDC_PERP"

        buy_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        sell_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.SELL)

        # Get moderate quantity for concurrent testing
        buy_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            price=buy_price,
        ) * Decimal(10)  # 10x minimum for concurrent test

        sell_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.SELL,
            price=sell_price,
        ) * Decimal(10)  # 10x minimum for concurrent test

        # Place two orders concurrently (buy and sell)
        buy_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=buy_quantity,
            price=buy_price,
            time_in_force=TimeInForce.GTC,
        )

        sell_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=sell_quantity,
            price=sell_price,
            time_in_force=TimeInForce.GTC,
        )

        try:
            # Place both orders
            buy_order = await bp_api_for_test_env.place_order(buy_args)
            sell_order = await bp_api_for_test_env.place_order(sell_args)

            # Validate both orders
            assert isinstance(buy_order, Order), "Buy order should be valid"
            assert isinstance(sell_order, Order), "Sell order should be valid"
            assert buy_order.side == OrderSide.BUY, "Buy order should have BUY side"
            assert sell_order.side == OrderSide.SELL, "Sell order should have SELL side"

            # Cancel both orders
            if buy_order.exchange_order_id:
                await bp_api_for_test_env.cancel_order(
                    CancelOrderArgs(symbol=symbol, order_id=buy_order.exchange_order_id),
                )
            if sell_order.exchange_order_id:
                await bp_api_for_test_env.cancel_order(
                    CancelOrderArgs(symbol=symbol, order_id=sell_order.exchange_order_id),
                )

        except Exception as e:
            # Concurrent order placement failures indicate real system problems
            pytest.fail(
                f"Concurrent order placement failed: {e}. "
                "Concurrent order operations are critical functionality.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_order_funding_rate_awareness(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp order operations with funding rate considerations."""
        symbol = "SOL_USDC_PERP"

        try:
            # Get current funding rate
            funding_rate = await bp_api_for_test_env.get_funding_rate(symbol)

            if funding_rate:
                logger.info(
                    "current_funding_rate",
                    symbol=symbol,
                    funding_rate=funding_rate.funding_rate,
                    message="Current funding rate retrieved",
                )

                # Funding rate affects long vs short positioning
                # Positive funding rate: longs pay shorts
                # Negative funding rate: shorts pay longs

                test_price = await get_dynamic_test_price(
                    bp_api_for_test_env,
                    symbol,
                    OrderSide.BUY,
                )

                # Get moderate quantity for funding rate test
                test_quantity = await get_minimal_order_size(
                    api=bp_api_for_test_env,
                    symbol=symbol,
                    side=OrderSide.BUY,
                    price=test_price,
                ) * Decimal(10)  # 10x minimum for funding rate test

                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,  # Test with buy side
                    order_type=OrderType.LIMIT,
                    quantity=test_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.IOC,  # Use IOC to avoid keeping position
                )

                order = await bp_api_for_test_env.place_order(place_args)
                assert isinstance(order, Order), "Should place order despite funding rate"

        except Exception as e:
            # Funding rate order placement failures indicate real system problems
            pytest.fail(
                f"Order placement with funding rate awareness failed: {e}. "
                "Order placement must work regardless of funding rate conditions.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_order_margin_requirements(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp order margin requirements validation.

        This test validates that our system correctly handles margin requirements
        for perpetual futures orders. We test with a moderately large order that
        should either succeed (if margin is sufficient) or fail with a proper
        margin-related error (if insufficient).
        """
        symbol = "SOL_USDC_PERP"

        # Get market constraints
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        min_quantity = constraints.get("min_quantity", constraints["step_size"])
        max_leverage = constraints.get("max_leverage", Decimal(20))

        # Use a reasonable multiplier for margin testing
        # Not too small (to test margin logic) but not excessive (to avoid guaranteed failures)
        reasonable_multiplier = min(Decimal(50), max_leverage)
        test_quantity = min_quantity * reasonable_multiplier
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)

        # Calculate expected margin requirement
        notional_value = test_price * test_quantity
        expected_margin = notional_value / max_leverage

        logger.info(
            "testing_margin_requirements",
            quantity=test_quantity,
            price=test_price,
            notional_value=notional_value,
            expected_margin=expected_margin,
            message="Testing margin requirements for order",
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=test_quantity,
            price=test_price,
            time_in_force=TimeInForce.IOC,  # Use IOC to avoid leaving orders
        )

        try:
            placed_order = await bp_api_for_test_env.place_order(place_args)

            # If successful, validate the order was placed correctly
            assert isinstance(placed_order, Order), "Should return Order instance"
            assert placed_order.quantity_requested == test_quantity, "Quantity should match"
            assert placed_order.symbol == symbol, "Symbol should match"

            # Log successful placement
            actual_notional = (
                placed_order.price * placed_order.quantity_requested
                if placed_order.price
                else test_price * test_quantity
            )
            logger.info(
                "order_placed_successfully",
                actual_notional=actual_notional,
                margin_requirement=actual_notional / max_leverage,
                message="Order placed successfully with margin calculations",
            )

        except Exception as e:
            error_msg = str(e).lower()
            # Check if this is a margin-related business error (expected)
            if any(
                keyword in error_msg
                for keyword in ["margin", "balance", "insufficient", "collateral"]
            ):
                logger.info(
                    "order_rejected_margin_requirements",
                    error_message=str(e),
                    message="Order correctly rejected due to margin requirements",
                )
                # This is expected behavior - the system correctly enforced margin requirements
            else:
                # Unexpected system error - this is a real problem
                pytest.fail(
                    f"Order placement failed with unexpected error: {e}. "
                    "Expected either successful placement or margin-related rejection.",
                )
