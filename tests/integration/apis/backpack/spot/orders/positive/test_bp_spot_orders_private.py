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
from cyberdelta.core.models.market.ticker import Ticker

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.spot,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]

logger = get_logger(__name__)


async def get_symbol_tick_size(api: BackpackAPI, symbol: str) -> Decimal:
    """Get the tick size (price precision) for a symbol using public API.

    This function uses the public get_markets() method to fetch actual
    tick size information from Backpack's API, ensuring tests use real
    market data instead of hardcoded values.

    Args:
        api: BackpackAPI instance for getting market data
        symbol: Trading symbol (e.g., "SOL_USDC")

    Returns:
        Decimal tick size for the symbol
    """
    try:
        # Use the public API method to get market metadata
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await api.get_markets(GetMarketsArgs())

        # Find the market by symbol
        for market in markets:
            if market.symbol == symbol:
                return market.tick_size

        logger.warning(f"Symbol {symbol} not found in markets, using default tick size")
        return Decimal("0.01")  # Fallback default

    except Exception as e:
        logger.warning(f"Failed to get tick size for {symbol}: {e}, using default")
        return Decimal("0.01")  # Fallback default


async def get_dynamic_test_price(
    api: BackpackAPI, symbol: str, side: OrderSide, tolerance_percent: Decimal = Decimal("5")
) -> Decimal:
    """Get a dynamic test price based on current market conditions.

    This function fetches the current market price and calculates a test price
    that is far enough from market to avoid accidental fills, but close enough
    to be accepted by the exchange's price validation.

    Args:
        api: BackpackAPI instance for getting market data
        symbol: Trading symbol (e.g., "SOL_USDC")
        side: Order side (BUY or SELL)
        tolerance_percent: Percentage away from market price (default 5%)

    Returns:
        Decimal price suitable for testing
    """
    try:
        # Get current market ticker
        ticker: Ticker = await api.get_ticker(symbol)

        # Use last traded price, fall back to mid-price, then bid/ask
        market_price = None
        if ticker.price is not None:
            market_price = ticker.price
        elif ticker.mid_price is not None:
            market_price = ticker.mid_price
        elif side == OrderSide.BUY and ticker.ask is not None:
            market_price = ticker.ask
        elif side == OrderSide.SELL and ticker.bid is not None:
            market_price = ticker.bid
        else:
            raise ValueError(f"Unable to determine market price for {symbol}")

        # Calculate test price with tolerance
        tolerance_factor = tolerance_percent / Decimal("100")

        if side == OrderSide.BUY:
            # For buy orders, use price below market to avoid fills
            test_price = market_price * (Decimal("1") - tolerance_factor)
        else:
            # For sell orders, use price above market to avoid fills
            test_price = market_price * (Decimal("1") + tolerance_factor)

        # Get the tick size for this symbol
        tick_size = await get_symbol_tick_size(api, symbol)

        # Round to the required tick size and normalize to remove trailing zeros
        quantized_price = test_price.quantize(tick_size)
        return quantized_price.normalize()

    except Exception as e:
        # Fallback to a reasonable static price if dynamic pricing fails
        if symbol == "SOL_USDC":
            fallback_price = Decimal("150.0") if side == OrderSide.BUY else Decimal("170.0")
        else:
            fallback_price = Decimal("1.0") if side == OrderSide.BUY else Decimal("10.0")

        logger.warning(
            "Dynamic pricing failed for %s, using fallback price %s: %s", symbol, fallback_price, e
        )
        return fallback_price


async def get_minimal_order_size(
    api: BackpackAPI, symbol: str, side: OrderSide, price: Decimal
) -> Decimal:
    """Calculate the minimal order size that fits within available balance.

    Gets actual account balance and market constraints to determine appropriate order size.

    Args:
        api: BackpackAPI instance for getting market data and balances
        symbol: Trading symbol (e.g., "SOL_USDC")
        side: Order side (BUY or SELL)
        price: Order price to calculate affordable quantity

    Returns:
        Decimal quantity that represents the minimal affordable order size
    """
    try:
        # Get actual account balances
        balances = await api.get_balances()

        # Get market information for constraints
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await api.get_markets(GetMarketsArgs())

        # Find the market for this symbol
        market = None
        for m in markets:
            if m.symbol == symbol:
                market = m
                break

        if market is None:
            logger.warning(f"Market {symbol} not found")
            raise ValueError(f"Market {symbol} not found")

        if side == OrderSide.BUY and symbol == "SOL_USDC":
            # For buying SOL with USDC, check USDC balance
            usdc_balance = balances.get("USDC")
            if usdc_balance is None or usdc_balance.available_quantity <= Decimal("0"):
                logger.warning("No USDC balance available")
                raise ValueError("No USDC balance available")

            # Calculate max quantity based on available balance
            max_quantity = usdc_balance.available_quantity / price

            # Get the actual minimum quantity from market rules
            min_quantity = market.min_quantity
            if min_quantity is None:
                # If no min_quantity specified, use step_size as minimum
                min_quantity = market.step_size

            # Round up to the nearest step_size to ensure we meet exchange requirements
            steps_needed = (min_quantity / market.step_size).quantize(
                Decimal("1"), rounding="ROUND_UP"
            )
            actual_min_quantity = steps_needed * market.step_size

            # Check if we can afford the actual minimum
            if max_quantity >= actual_min_quantity:
                return actual_min_quantity
            else:
                # If we can't afford minimum, something's wrong
                raise ValueError(
                    f"Balance ${usdc_balance.available_quantity} insufficient for "
                    f"minimum order size {actual_min_quantity} at price ${price}"
                )

        # For other cases, calculate minimum based on market rules
        min_quantity = market.min_quantity
        if min_quantity is None:
            min_quantity = market.step_size

        # Round up to nearest step_size
        steps_needed = (min_quantity / market.step_size).quantize(Decimal("1"), rounding="ROUND_UP")
        return steps_needed * market.step_size

    except Exception as e:
        logger.warning(f"Failed to calculate minimal order size for {symbol}: {e}")
        raise


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/spot/orders/positive"], indirect=True
)
class TestBackpackSpotOrdersPositiveBalance:
    """Comprehensive private orders integration tests with positive balance for operations."""

    async def _get_market_constraints(self, api: BackpackAPI, symbol: str) -> dict[str, Decimal]:
        """Get market constraints for precision testing."""
        try:
            from cyberdelta.apis.models.service_args_models import GetMarketArgs

            market = await api.get_market(GetMarketArgs(symbol=symbol))

            return {
                "step_size": market.step_size or Decimal("0.001"),
                "tick_size": market.tick_size or Decimal("0.01"),
                "min_quantity": market.step_size or Decimal("0.001"),
            }
        except Exception as e:
            logger.warning(f"Could not get market constraints for {symbol}: {e}")
            # Safe fallbacks based on common Backpack requirements
            return {
                "step_size": Decimal("0.001"),
                "tick_size": Decimal("0.01"),
                "min_quantity": Decimal("0.001"),
            }

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful place_order() pipeline with positive balance.

        With positive balance, this test validates:
        1. Order placement succeeds and returns valid Order instance
        2. Order model field mapping is correct
        3. Decimal precision is maintained
        4. Backpack-specific order details are properly populated
        5. Order can be successfully cancelled

        This ensures full order lifecycle works correctly with sufficient funds.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Get dynamic test price based on current market conditions
        symbol = "SOL_USDC"  # Common Backpack trading pair
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("3"),  # 3% below market for buy order
        )

        # Calculate minimal affordable order size based on current balance
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=test_price,
        )

        # Define order parameters with dynamic pricing and sizing
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=minimal_quantity,  # Dynamic size based on available balance
            price=test_price,  # Dynamic price based on market conditions
            time_in_force=TimeInForce.GTC,
        )

        # With positive balance, expect successful order placement
        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate successful order placement
        assert isinstance(placed_order, Order), "Should return Order instance"
        assert placed_order.exchange == "backpack", "Order should be from Backpack"
        assert placed_order.symbol == symbol, (
            f"Symbol should match: expected {symbol}, got {placed_order.symbol}"
        )
        assert placed_order.side == side, (
            f"Side should match: expected {side}, got {placed_order.side}"
        )
        assert placed_order.quantity_requested == place_args.quantity, "Quantity should match"
        assert placed_order.price == test_price, "Price should match"
        assert placed_order.exchange_order_id is not None, "Should have exchange order ID"
        assert placed_order.status == OrderStatus.OPEN, (
            f"New order should be OPEN, got {placed_order.status}"
        )

        # Clean up: cancel the order
        if placed_order.exchange_order_id:
            cancel_args = CancelOrderArgs(
                order_id=placed_order.exchange_order_id,
                symbol=symbol,
            )
            await bp_api_for_test_env.cancel_order(cancel_args)

        logger.info(f"✓ Order placement successful with ID: {placed_order.exchange_order_id}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_order_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful cancel_order() API endpoint with positive balance.

        This test validates the complete order lifecycle:
        1. Place an order successfully
        2. Cancel the order successfully
        3. Validate the cancellation response
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        # Calculate minimal affordable order size
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            price=test_price,
        )

        # First, place an order
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=minimal_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)
        assert placed_order.exchange_order_id is not None, "Placed order should have ID"

        # Now cancel the order
        cancel_args = CancelOrderArgs(
            order_id=placed_order.exchange_order_id,
            symbol=symbol,
        )

        # Cancel should succeed without raising an exception
        await bp_api_for_test_env.cancel_order(cancel_args)

        # Verify the order is no longer in open orders
        open_orders = await bp_api_for_test_env.get_open_orders()
        order_ids = [order.exchange_order_id for order in open_orders]
        assert placed_order.exchange_order_id not in order_ids, (
            "Cancelled order should not be in open orders"
        )

        logger.info(f"✓ Order {placed_order.exchange_order_id} cancelled successfully")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_order_history_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() validates full pipeline to Order models.

        This tests historical order retrieval and validates Order model consistency.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Define recent date range for order history
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)  # Last 7 days

        # Execute the full pipeline using GetOrderHistoryArgs
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=50,
        )
        order_history = await bp_api_for_test_env.get_order_history(args)

        # Validate return type
        assert isinstance(order_history, list), "get_order_history() should return list[Order]"

        # If history exists, validate structure
        if order_history:
            sample_order = order_history[0]
            assert isinstance(sample_order, Order), "Historical order should be Order instance"

            # Validate exchange field
            assert sample_order.exchange == "backpack", (
                f"Exchange should be 'backpack', got {sample_order.exchange}"
            )

            # Validate order fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.exchange_order_id is not None, "order should have exchange ID"

            # Validate Decimal precision
            assert isinstance(sample_order.quantity_requested, Decimal), (
                "quantity_requested must be Decimal"
            )
            assert isinstance(sample_order.quantity_filled, Decimal), (
                "quantity_filled must be Decimal"
            )

            # Validate timestamps
            assert sample_order.created_at is not None, "order should have created_at timestamp"

            # Validate historical order is within requested time range
            if sample_order.created_at:
                assert start_time <= sample_order.created_at <= end_time, (
                    f"Order timestamp should be within requested range: {sample_order.created_at}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_open_orders_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_open_orders() with detailed Order model validation."""
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Execute the full pipeline
        open_orders = await bp_api_for_test_env.get_open_orders()

        # Validate return type
        assert isinstance(open_orders, list), "get_open_orders() should return list[Order]"

        # If orders exist, validate structure
        if open_orders:
            sample_order = open_orders[0]
            assert isinstance(sample_order, Order), "Order should be Order instance"

            # Validate exchange field
            assert sample_order.exchange == "backpack", (
                f"Exchange should be 'backpack', got {sample_order.exchange}"
            )

            # Validate required fields
            assert isinstance(sample_order.symbol, str), "symbol must be string"
            assert sample_order.side in [OrderSide.BUY, OrderSide.SELL], (
                "side must be valid OrderSide"
            )
            assert sample_order.status == OrderStatus.OPEN, (
                f"open order should have OPEN status, got {sample_order.status}"
            )
            assert sample_order.exchange_order_id is not None, "order should have exchange ID"

            # Validate Decimal precision
            assert isinstance(sample_order.quantity_requested, Decimal), (
                "quantity_requested must be Decimal"
            )
            assert isinstance(sample_order.quantity_filled, Decimal), (
                "quantity_filled must be Decimal"
            )

            if sample_order.price is not None:  # Market orders may not have price
                assert isinstance(sample_order.price, Decimal), "price must be Decimal"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_precision_validation_positive_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order precision handling with successful placement and cancellation.

        This validates that decimal precision is maintained through the complete
        order lifecycle when orders are successfully placed.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY

        # Get market constraints
        constraints = await self._get_market_constraints(bp_api_for_test_env, symbol)
        min_quantity = constraints["min_quantity"]
        step_size = constraints["step_size"]
        tick_size = constraints["tick_size"]

        # Use a quantity that's exactly aligned with step size
        test_quantity = min_quantity * Decimal("2")  # 2x minimum
        rounded_quantity = (test_quantity / step_size).quantize(Decimal("1")) * step_size

        # Get dynamic test price
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env, symbol=symbol, side=side, tolerance_percent=Decimal("4")
        )

        # Round price to valid tick size
        rounded_price = test_price.quantize(tick_size)

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=rounded_quantity,
            price=rounded_price,
            time_in_force=TimeInForce.GTC,
        )

        # Place order and validate precision
        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate precision is maintained
        assert placed_order.quantity_requested == rounded_quantity, (
            f"Quantity precision not maintained: expected {rounded_quantity}, "
            f"got {placed_order.quantity_requested}"
        )

        assert placed_order.price == rounded_price, (
            f"Price precision not maintained: expected {rounded_price}, got {placed_order.price}"
        )

        # Clean up
        if placed_order.exchange_order_id:
            cancel_args = CancelOrderArgs(order_id=placed_order.exchange_order_id, symbol=symbol)
            await bp_api_for_test_env.cancel_order(cancel_args)

        logger.info(
            f"✓ Precision validation successful for quantity {rounded_quantity} "
            f"and price {rounded_price}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_order_operations_positive_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test multiple order operations with positive balance.

        This validates that multiple orders can be placed and managed
        successfully when sufficient funds are available.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY

        # Place multiple small orders
        placed_orders: list[Order] = []

        for i in range(2):  # Place 2 small orders
            test_price = await get_dynamic_test_price(
                api=bp_api_for_test_env,
                symbol=symbol,
                side=side,
                tolerance_percent=Decimal("3") + Decimal(str(i)),  # Slightly different prices
            )

            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.05"),  # Small quantity
                price=test_price,
                time_in_force=TimeInForce.GTC,
            )

            placed_order = await bp_api_for_test_env.place_order(place_args)
            placed_orders.append(placed_order)

            # Validate each order
            assert isinstance(placed_order, Order), f"Order {i + 1} should be Order instance"
            assert placed_order.exchange_order_id is not None, f"Order {i + 1} should have ID"
            assert placed_order.status == OrderStatus.OPEN, f"Order {i + 1} should be OPEN"

        # Verify orders appear in open orders
        open_orders = await bp_api_for_test_env.get_open_orders()
        placed_order_ids = {
            order.exchange_order_id for order in placed_orders if order.exchange_order_id
        }
        open_order_ids = {
            order.exchange_order_id for order in open_orders if order.exchange_order_id
        }

        for order_id in placed_order_ids:
            assert order_id in open_order_ids, f"Order {order_id} should be in open orders"

        # Clean up: cancel all placed orders
        for order in placed_orders:
            if order.exchange_order_id:
                cancel_args = CancelOrderArgs(
                    order_id=order.exchange_order_id,
                    symbol=symbol,
                )
                await bp_api_for_test_env.cancel_order(cancel_args)

        logger.info(f"✓ Successfully placed and cancelled {len(placed_orders)} orders")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_symbol_format_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order operations with Backpack symbol format validation.

        This validates that order symbols follow Backpack's naming conventions
        and are properly formatted in successfully placed orders.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        # Place an order to validate symbol format
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate symbol format in placed order
        order_symbol = placed_order.symbol
        assert isinstance(order_symbol, str), "Symbol should be string"
        assert len(order_symbol) > 0, "Symbol should not be empty"
        assert order_symbol == order_symbol.strip(), "Symbol should not have whitespace"
        assert order_symbol == symbol, (
            f"Returned symbol should match: expected {symbol}, got {order_symbol}"
        )

        # Validate Backpack format (BASE_QUOTE)
        if "_" in order_symbol:
            parts = order_symbol.split("_")
            assert len(parts) == 2, (
                f"Trading pair should have exactly one underscore: {order_symbol}"
            )
            assert len(parts[0]) >= 2, f"Base asset should be at least 2 characters: {order_symbol}"
            assert len(parts[1]) >= 3, (
                f"Quote asset should be at least 3 characters: {order_symbol}"
            )

        # Clean up
        if placed_order.exchange_order_id:
            cancel_args = CancelOrderArgs(
                order_id=placed_order.exchange_order_id,
                symbol=symbol,
            )
            await bp_api_for_test_env.cancel_order(cancel_args)

        logger.info(f"✓ Symbol format validation passed for {order_symbol}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_lifecycle_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test complete order lifecycle with positive balance.

        This validates the full order management pipeline:
        1. Place order successfully
        2. Verify order appears in get_open_orders
        3. Cancel order successfully
        4. Verify order appears in get_order_history
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        # Step 1: Place order
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)
        assert placed_order.exchange_order_id is not None, "Should have order ID"

        # Step 2: Verify order in open orders
        open_orders = await bp_api_for_test_env.get_open_orders()
        assert isinstance(open_orders, list), "get_open_orders() should return list"
        order_ids = [order.exchange_order_id for order in open_orders]
        assert placed_order.exchange_order_id in order_ids, "Order should be in open orders"

        # Step 3: Cancel order
        cancel_args = CancelOrderArgs(
            order_id=placed_order.exchange_order_id,
            symbol=symbol,
        )
        await bp_api_for_test_env.cancel_order(cancel_args)

        # Step 4: Verify order no longer in open orders
        open_orders_after = await bp_api_for_test_env.get_open_orders()
        order_ids_after = [order.exchange_order_id for order in open_orders_after]
        assert placed_order.exchange_order_id not in order_ids_after, (
            "Cancelled order should not be in open orders"
        )

        # Step 5: Verify order appears in history
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)

        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=50,
        )
        order_history = await bp_api_for_test_env.get_order_history(args)
        history_order_ids = [order.exchange_order_id for order in order_history]

        # The cancelled order should appear in history
        assert placed_order.exchange_order_id in history_order_ids, (
            "Cancelled order should be in history"
        )

        logger.info(
            f"✓ Complete order lifecycle validated for order {placed_order.exchange_order_id}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_backpack_specific_details(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test Backpack-specific API details with positive balance.

        This validates that Backpack's API responses contain expected structure
        when orders are successfully placed.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        # Place order to validate Backpack-specific fields
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Validate Backpack-specific order fields
        assert placed_order.exchange == "backpack", "Order should be from Backpack"

        # Validate order ID format (Backpack specific)
        assert isinstance(placed_order.exchange_order_id, str), "Backpack order ID should be string"
        assert len(placed_order.exchange_order_id) > 0, "Order ID should not be empty"

        # Test Backpack-specific order details if present
        if hasattr(placed_order, "bp_details") and placed_order.bp_details:
            bp_details = placed_order.bp_details

            # Validate Backpack-specific order details if present
            client_id = getattr(bp_details, "client_id", None)
            if client_id is not None:
                assert isinstance(client_id, str), "client_id should be string"

            order_flags = getattr(bp_details, "order_flags", None)
            if order_flags is not None:
                assert isinstance(order_flags, int | str), "order_flags should be int or string"

        # Validate that order appears in get_open_orders with Backpack structure
        open_orders = await bp_api_for_test_env.get_open_orders()
        placed_order_found = False

        for order in open_orders:
            if order.exchange_order_id == placed_order.exchange_order_id:
                placed_order_found = True
                assert order.exchange == "backpack", "Order in open orders should be from Backpack"
                break

        assert placed_order_found, "Placed order should be found in open orders"

        # Clean up
        cancel_args = CancelOrderArgs(
            order_id=placed_order.exchange_order_id,
            symbol=symbol,
        )
        await bp_api_for_test_env.cancel_order(cancel_args)

        logger.info(
            f"✓ Backpack-specific order details validated for order "
            f"{placed_order.exchange_order_id}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_history_date_range_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() with various date range scenarios using placed orders.

        This validates proper handling of different date ranges by placing
        and cancelling orders, then verifying they appear in history.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        # Place and immediately cancel an order to ensure we have recent history
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)

        # Cancel immediately to add to history
        assert placed_order.exchange_order_id is not None, "Order should have ID"
        cancel_args = CancelOrderArgs(
            order_id=placed_order.exchange_order_id,
            symbol=symbol,
        )
        await bp_api_for_test_env.cancel_order(cancel_args)

        # Test recent date range (should include our cancelled order)
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)  # Last hour

        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=10,
        )

        recent_history = await bp_api_for_test_env.get_order_history(args)
        assert isinstance(recent_history, list), "Should return list"

        # Our cancelled order should be in recent history
        order_ids = [order.exchange_order_id for order in recent_history]
        assert placed_order.exchange_order_id in order_ids, (
            "Cancelled order should be in recent history"
        )

        # Validate orders are within date range
        for order in recent_history:
            if order.created_at:
                assert start_time <= order.created_at <= end_time, (
                    f"Order should be within date range: {order.created_at}"
                )

        # Test longer date range
        long_start_time = end_time - timedelta(days=7)  # Last week
        long_args = GetOrderHistoryArgs(
            start_time=long_start_time,
            end_time=end_time,
            limit=50,
        )

        long_history = await bp_api_for_test_env.get_order_history(long_args)
        assert isinstance(long_history, list), "Should return list for longer range"

        # Longer range should include our order and potentially more
        long_order_ids = [order.exchange_order_id for order in long_history]
        assert placed_order.exchange_order_id in long_order_ids, (
            "Order should be in longer history too"
        )

        # Longer range should have >= orders from shorter range
        assert len(long_history) >= len(recent_history), (
            "Longer date range should include at least as many orders"
        )

        logger.info(
            f"✓ Date range validation completed with order {placed_order.exchange_order_id}"
        )
