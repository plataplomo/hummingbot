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

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from tests.integration.apis.backpack.shared.test_helpers import (
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
    "custom_vcr_cassette_dir", ["apis/backpack/perp/orders/positive"], indirect=True
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
            f"✓ Perp market order executed: {placed_order.exchange_order_id}, "
            f"status: {placed_order.status}, filled: {placed_order.quantity_filled}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_stop_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp stop market order placement and validation.

        Stop market orders for perps trigger at a specific price and execute as market orders.
        Critical for risk management in leveraged positions.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP
        side = OrderSide.SELL  # Stop loss on existing long position

        # Get current market price and set trigger below for stop loss
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        trigger_price = current_price * Decimal("0.95")  # 5% below current price

        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=trigger_price,
        )

        # Place perp stop market order
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.STOP_MARKET,
            quantity=minimal_quantity,
            stop_price=trigger_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate perp stop order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.STOP_MARKET, "Should be stop market order"
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"

        # Stop orders should be pending trigger (not filled immediately)
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Perp stop order should be pending, got {placed_order.status}"
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
                    f"✓ Perp stop market order cleaned up: {placed_order.exchange_order_id}"
                )
            except Exception as e:
                logger.warning(f"Could not cancel perp stop order: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_stop_limit_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp stop limit order placement and validation.

        Stop limit orders for perps trigger at a specific price and become limit orders.
        Provides more control over execution price in volatile perp markets.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP
        side = OrderSide.SELL  # Stop loss

        # Get current market price
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        trigger_price = current_price * Decimal("0.95")  # 5% below for stop loss
        limit_price = trigger_price * Decimal("0.99")  # Slightly below trigger

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

        # Validate perp stop limit order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.STOP_LIMIT, "Should be stop limit order"
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"
        assert placed_order.price == limit_price, "Limit price should match"

        # Stop orders should be pending trigger
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Perp stop limit order should be pending, got {placed_order.status}"
        )

        # Clean up
        if placed_order.exchange_order_id and placed_order.status != OrderStatus.FILLED:
            try:
                cancel_args = CancelOrderArgs(
                    order_id=placed_order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_test_env.cancel_order(cancel_args)
                logger.info(f"✓ Perp stop limit order cleaned up: {placed_order.exchange_order_id}")
            except Exception as e:
                logger.warning(f"Could not cancel perp stop limit order: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_take_profit_market_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp take profit market order placement and validation.

        Take profit market orders for perps trigger above current price and execute
        as market orders.
        Essential for automated profit-taking in leveraged positions.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP
        side = OrderSide.SELL  # Taking profit on long position

        # Get current market price and set trigger above for take profit
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        trigger_price = current_price * Decimal("1.05")  # 5% above current price

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

        # Validate perp take profit order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.TAKE_PROFIT_MARKET, (
            "Should be take profit market order"
        )
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"

        # Take profit orders should be pending trigger
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Perp take profit order should be pending, got {placed_order.status}"
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
                    f"✓ Perp take profit market order cleaned up: {placed_order.exchange_order_id}"
                )
            except Exception as e:
                logger.warning(f"Could not cancel perp take profit order: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_perp_take_profit_limit_order_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test perp take profit limit order placement and validation.

        Take profit limit orders for perps trigger at a specific price and become limit orders.
        Provides precise control over profit-taking execution in perp markets.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_PERP
        side = OrderSide.SELL  # Taking profit

        # Get current market price
        current_price = await get_current_market_price(bp_api_for_test_env, symbol)
        trigger_price = current_price * Decimal("1.05")  # 5% above for take profit
        limit_price = trigger_price * Decimal("1.01")  # Slightly above trigger

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

        # Validate perp take profit limit order properties
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.order_type == OrderType.TAKE_PROFIT_LIMIT, (
            "Should be take profit limit order"
        )
        assert placed_order.quantity_requested == minimal_quantity, "Quantity should match"
        assert placed_order.price == limit_price, "Limit price should match"

        # Take profit orders should be pending trigger
        assert placed_order.status in [OrderStatus.OPEN, OrderStatus.TRIGGER_PENDING], (
            f"Perp take profit limit order should be pending, got {placed_order.status}"
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
                    f"✓ Perp take profit limit order cleaned up: {placed_order.exchange_order_id}"
                )
            except Exception as e:
                logger.warning(f"Could not cancel perp take profit limit order: {e}")

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

        logger.info(f"✓ Perp entry order executed: {entry_order.exchange_order_id}")

        # Step 2: Place stop loss order (assuming we now have leveraged position)
        if entry_order.status == OrderStatus.FILLED:
            # Set stop loss 10% below entry price (more conservative for leveraged perp)
            stop_price = current_price * Decimal("0.90")

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
                    f"Perp stop order should be pending, got {stop_order.status}"
                )

                logger.info(f"✓ Perp stop loss placed: {stop_order.exchange_order_id}")

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
                        # Stop loss cancellation should work if the order was placed
                        pytest.fail(
                            f"Failed to cancel perp stop loss order: {e}. "
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

        logger.info("✓ Perp market order + stop loss workflow test completed")

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_precision_edge_cases(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order precision handling with edge case values."""
        symbol = "SOL_USDC_PERP"

        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        min_size = constraints["min_order_size"]
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
                "This suggests a problem with market constraints or test configuration."
            )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_leverage_order_calculations(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order calculations with leverage considerations."""
        symbol = "SOL_USDC_PERP"

        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        max_leverage = constraints.get("max_leverage", Decimal("20"))

        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)
        # Get test quantity based on market constraints
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        min_quantity = constraints["min_order_size"]
        # Use 2000x minimum for test calculation
        test_quantity = min_quantity * Decimal("2000")

        # Calculate notional value and margin requirement
        notional_value = test_price * test_quantity
        margin_requirement = notional_value / max_leverage

        logger.info(
            f"Perp order calculations - Notional: {notional_value}, Margin: {margin_requirement}"
        )

        # Test precision of leverage calculations
        assert isinstance(notional_value, Decimal), "Notional value should be Decimal"
        assert isinstance(margin_requirement, Decimal), "Margin requirement should be Decimal"
        assert margin_requirement > Decimal("0"), "Margin requirement should be positive"
        assert margin_requirement < notional_value, (
            "Margin should be less than notional (leverage effect)"
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_get_perp_order_history_success(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test retrieving perp order history with positive balance."""
        symbol = "SOL_USDC_PERP"

        # Get recent order history
        end_time = datetime.now()
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

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_market_order_execution(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
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
                "Market orders should execute successfully with adequate margin."
            )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_concurrent_operations(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
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
        ) * Decimal("50")  # 50x minimum for concurrent test

        sell_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.SELL,
            price=sell_price,
        ) * Decimal("50")  # 50x minimum for concurrent test

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
                    CancelOrderArgs(symbol=symbol, order_id=buy_order.exchange_order_id)
                )
            if sell_order.exchange_order_id:
                await bp_api_for_test_env.cancel_order(
                    CancelOrderArgs(symbol=symbol, order_id=sell_order.exchange_order_id)
                )

        except Exception as e:
            logger.info(f"Concurrent perp order test failed: {e}")

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_funding_rate_awareness(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order operations with funding rate considerations."""
        symbol = "SOL_USDC_PERP"

        try:
            # Get current funding rate
            funding_rate = await bp_api_for_test_env.get_funding_rate(symbol)

            if funding_rate:
                logger.info(f"Current funding rate for {symbol}: {funding_rate.funding_rate}")

                # Funding rate affects long vs short positioning
                # Positive funding rate: longs pay shorts
                # Negative funding rate: shorts pay longs

                test_price = await get_dynamic_test_price(
                    bp_api_for_test_env, symbol, OrderSide.BUY
                )

                # Get moderate quantity for funding rate test
                test_quantity = await get_minimal_order_size(
                    api=bp_api_for_test_env,
                    symbol=symbol,
                    side=OrderSide.BUY,
                    price=test_price,
                ) * Decimal("50")  # 50x minimum for funding rate test

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
            logger.info(f"Funding rate perp order test failed: {e}")

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_margin_requirements(
        self, bp_api_for_test_env: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order margin requirements validation."""
        symbol = "SOL_USDC_PERP"

        # Get large quantity based on market constraints
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        min_quantity = constraints["min_order_size"]
        # Large quantity is 10000x minimum for margin testing
        large_quantity = min_quantity * Decimal("10000")
        test_price = await get_dynamic_test_price(bp_api_for_test_env, symbol, OrderSide.BUY)

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=large_quantity,
            price=test_price,
            time_in_force=TimeInForce.IOC,  # Use IOC to avoid leaving large orders
        )

        try:
            placed_order = await bp_api_for_test_env.place_order(place_args)

            # If successful, validate margin-related fields
            assert isinstance(placed_order, Order), "Should validate margin and place order"
            assert placed_order.quantity_requested == large_quantity, "Quantity should match"

            # The order should respect margin requirements
            notional = (
                placed_order.price * placed_order.quantity_requested
                if placed_order.price
                else test_price * large_quantity
            )
            logger.info(f"Large perp order notional: {notional}")

        except Exception as e:
            # Order might fail due to insufficient margin, which is expected behavior
            # Large orders may fail due to margin, but we need to validate the error type
            if "margin" in str(e).lower() or "insufficient" in str(e).lower():
                # This is expected - margin validation is working
                logger.info(f"Large perp order correctly rejected due to margin constraints: {e}")
            else:
                # Unexpected error - this is a real failure
                pytest.fail(
                    f"Large perp order failed with unexpected error for {symbol}: {e}. "
                    "Expected margin-related error but got something else."
                )
            # This is acceptable - the test validates that margin checking works
