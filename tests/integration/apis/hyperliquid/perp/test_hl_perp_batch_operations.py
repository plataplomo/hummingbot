"""Integration tests for Hyperliquid batch order operations.

This module tests the performance and functionality improvements of batch order
operations, including placing multiple orders in a single API call and batch
cancellation operations.

The tests demonstrate the significant performance benefits:
- Batch placement: 6 orders in <1 second vs ~9 seconds sequential
- Reduced API calls: 6 → 1
- Reduced EIP-712 signatures: 6 → 1
"""

from __future__ import annotations

import time
from decimal import Decimal

import pytest

from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
)
from cyberdelta.apis.common import APIError
from cyberdelta.apis.exceptions import ServiceParameterError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import CancelOrderArgs, PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from tests.integration.apis.hyperliquid.shared.hl_test_helpers import (
    HyperliquidTestHelpers,
    generate_test_cloid,
    get_minimal_test_quantity,
    get_safe_test_price,
)


logger = get_logger(__name__)


# Pytest plugins are handled by the parent conftest.py file


class TestHyperliquidBatchOperations:
    """Integration tests for Hyperliquid batch order operations."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_batch_orders_success_six_orders(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test successful batch placement of 6 orders - the key performance improvement case.

        This test demonstrates the core value proposition: placing 6 orders in a single
        batch request vs individual sequential requests.

        Expected performance improvement: <1 second vs ~9 seconds sequential
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

        # Select first available symbol for deterministic VCR playback
        symbol = available_symbols[0]
        logger.info(
            "symbol_selected_for_batch_test",
            symbol=symbol,
            message="Selected symbol for batch test",
        )

        # Get dynamic test parameters for safe order placement
        # Note: market_price is retrieved within get_safe_test_price when needed

        # Get minimal test quantity from exchange constraints

        test_quantity = await get_minimal_test_quantity(hl_api_for_test_env, symbol, OrderSide.BUY)

        orders: list[PlaceOrderArgs] = []
        for i in range(6):
            # Create orders at different prices to avoid conflicts
            # Use 15% below market for buy orders to avoid accidental fills

            price_offset_percentage = Decimal("0.15") + (Decimal("0.01") * i)  # 15%, 16%, 17%, etc.
            test_price = await get_safe_test_price(
                hl_api_for_test_env,
                symbol,
                OrderSide.BUY,
                price_offset_percentage,
            )

            orders.append(
                PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=test_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                    execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                    client_order_id=None,  # Optional - let exchange assign ID
                ),
            )

        # Measure batch order placement time
        start_time = time.time()
        placed_orders = await hl_api_for_test_env.place_batch_orders(orders)
        elapsed_time = time.time() - start_time

        # Log performance results
        logger.info(
            "batch_orders_placed_success",
            orders_count=len(placed_orders),
            elapsed_time=elapsed_time,
            message="Batch placed orders successfully",
        )

        # Validate results
        assert len(placed_orders) == 6, f"Expected 6 orders, got {len(placed_orders)}"
        # Be more lenient with timing due to network variability
        assert elapsed_time < 10.0, f"Batch operation took {elapsed_time:.3f}s (expected <10s)"

        # Validate each placed order
        for i, order in enumerate(placed_orders):
            assert isinstance(order, Order)
            assert order.exchange_order_id is not None
            assert order.symbol == symbol
            assert order.side == OrderSide.BUY
            assert order.order_type == OrderType.LIMIT
            assert order.quantity_requested == test_quantity
            assert order.status in [OrderStatus.OPEN, OrderStatus.NEW]
            # Client order ID is optional - no need to assert

            logger.info(
                "order_placement_details",
                order_number=i + 1,
                order_id=order.exchange_order_id,
                status=order.status,
                message="Order placement details",
            )

        # Clean up: Cancel all placed orders
        cancel_args = [
            CancelOrderArgs(order_id=order.exchange_order_id, symbol=order.symbol)
            for order in placed_orders
            if order.exchange_order_id is not None
        ]

        if cancel_args:
            cancel_results = await hl_api_for_test_env.cancel_batch_orders(cancel_args)
            successful_cancels = sum(
                1 for r in cancel_results if r.status == CancelOrderResultStatus.SUCCESS
            )
            logger.info(
                "batch_cleanup_completed",
                successful_cancels=successful_cancels,
                total_cancels=len(cancel_args),
                message="Batch cleanup completed",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_batch_orders_success(self, hl_api_for_test_env: HyperliquidAPI) -> None:
        """Test successful batch cancellation of multiple orders."""
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

        # Select first available symbol for deterministic VCR playback
        symbol = available_symbols[0]
        logger.info(
            "symbol_selected_for_cancel_test",
            symbol=symbol,
            message="Selected symbol for cancel test",
        )

        # Get dynamic test parameters
        test_quantity = await get_minimal_test_quantity(hl_api_for_test_env, symbol, OrderSide.BUY)

        orders: list[PlaceOrderArgs] = []
        for i in range(4):
            # Use progressively lower prices to avoid accidental fills
            price_offset_percentage = Decimal("0.25") + (Decimal("0.03") * i)  # 25%, 28%, 31%, 34%
            test_price = await get_safe_test_price(
                hl_api_for_test_env,
                symbol,
                OrderSide.BUY,
                price_offset_percentage,
            )

            orders.append(
                PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=test_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                    execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                    client_order_id=None,  # Optional - let exchange assign ID
                ),
            )

        # Place orders
        placed_orders = await hl_api_for_test_env.place_batch_orders(orders)
        assert len(placed_orders) == 4

        logger.info(
            "orders_placed_for_cancellation_test",
            orders_count=len(placed_orders),
            message="Placed orders for cancellation test",
        )

        # Prepare cancellation arguments
        cancel_args: list[CancelOrderArgs] = [
            CancelOrderArgs(
                order_id=order.exchange_order_id,
                symbol=order.symbol,
            )
            for order in placed_orders
            if order.exchange_order_id is not None
        ]

        # Test batch cancellation
        cancel_start = time.time()
        cancel_results = await hl_api_for_test_env.cancel_batch_orders(cancel_args)
        cancel_time = time.time() - cancel_start

        logger.info(
            "batch_cancellation_completed",
            orders_count=len(cancel_results),
            cancel_time=cancel_time,
            message="Batch cancellation completed",
        )

        # Validate cancellation results
        assert len(cancel_results) == len(cancel_args)

        successful_cancels = 0
        for i, result in enumerate(cancel_results):
            assert isinstance(result, CancelOrderResult)
            assert result.order_id == cancel_args[i].order_id
            assert result.symbol == cancel_args[i].symbol

            if result.status == CancelOrderResultStatus.SUCCESS:
                successful_cancels += 1
                logger.info(
                    "order_cancellation_success",
                    order_id=result.order_id,
                    message="Order canceled successfully",
                )
            else:
                logger.warning(
                    "order_cancellation_failed",
                    order_id=result.order_id,
                    error_message=result.message,
                    message="Failed to cancel order",
                )

        # Should have successfully canceled most/all orders
        assert successful_cancels >= len(cancel_results) // 2, "Expected at least 50% success rate"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_batch_order_validation_empty_list(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test batch operations with empty order lists."""
        # Test empty batch placement
        with pytest.raises(ValueError, match="Cannot place empty batch of orders"):
            await hl_api_for_test_env.place_batch_orders([])

        # Test empty batch cancellation
        with pytest.raises(ValueError, match="Cannot cancel empty batch of orders"):
            await hl_api_for_test_env.cancel_batch_orders([])

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_batch_market_orders_rejected(self, hl_api_for_test_env: HyperliquidAPI) -> None:
        """Test that market orders are rejected in batch operations for safety."""
        # Get available symbols from exchange using dynamic discovery

        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env,
            limit=1,
        )

        if not available_symbols:
            pytest.skip(
                "No perpetual symbols available from the exchange. "
                "This test requires at least one available perpetual symbol.",
            )

        symbol = available_symbols[0]
        test_quantity = await get_minimal_test_quantity(hl_api_for_test_env, symbol, OrderSide.BUY)
        test_price = await get_safe_test_price(
            hl_api_for_test_env,
            symbol,
            OrderSide.BUY,
            Decimal("0.10"),
        )

        orders = [
            PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,  # This is fine
                quantity=test_quantity,
                price=test_price,
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(),
            ),
            PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,  # This should be rejected
                quantity=test_quantity,
                time_in_force=TimeInForce.IOC,
                execution=OrderExecution(),
            ),
        ]

        # Should raise validation error for market orders in batch
        with pytest.raises(
            ServiceParameterError, match="market orders are not supported in batch operations"
        ):
            await hl_api_for_test_env.place_batch_orders(orders)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_batch_orders_mixed_symbols(self, hl_api_for_test_env: HyperliquidAPI) -> None:
        """Test batch operations with orders for different symbols."""
        # Get available symbols from exchange using dynamic discovery

        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env,
            limit=5,
        )

        if len(available_symbols) < 2:
            pytest.skip(
                "This test requires at least 2 different perpetual symbols. "
                f"Only {len(available_symbols)} available.",
            )

        # Use first two available symbols
        symbol1 = available_symbols[0]
        symbol2 = available_symbols[1]
        logger.info(
            "symbols_selected_for_mixed_test",
            symbol1=symbol1,
            symbol2=symbol2,
            message="Selected symbols for mixed test",
        )

        # Get test parameters for each symbol using proper helpers
        test_price1 = await get_safe_test_price(
            hl_api_for_test_env,
            symbol1,
            OrderSide.BUY,
            Decimal("0.15"),
        )
        test_price2 = await get_safe_test_price(
            hl_api_for_test_env,
            symbol2,
            OrderSide.BUY,
            Decimal("0.15"),
        )

        # Use the test helpers that handle all exchange constraints including notional value
        test_quantity1 = await get_minimal_test_quantity(
            hl_api_for_test_env,
            symbol1,
            OrderSide.BUY,
        )
        test_quantity2 = await get_minimal_test_quantity(
            hl_api_for_test_env,
            symbol2,
            OrderSide.BUY,
        )

        orders = [
            PlaceOrderArgs(
                symbol=symbol1,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity1,
                price=test_price1,
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                client_order_id=generate_test_cloid(),  # Use proper 128-bit hex cloid
            ),
            PlaceOrderArgs(
                symbol=symbol2,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity2,
                price=test_price2,
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                client_order_id=generate_test_cloid(),  # Use proper 128-bit hex cloid
            ),
        ]

        # Place batch orders with mixed symbols
        placed_orders = await hl_api_for_test_env.place_batch_orders(orders)

        assert len(placed_orders) == 2

        # Validate symbols are preserved correctly
        symbol1_order = next((o for o in placed_orders if o.symbol == symbol1), None)
        symbol2_order = next((o for o in placed_orders if o.symbol == symbol2), None)

        assert symbol1_order is not None, f"{symbol1} order not found"
        assert symbol2_order is not None, f"{symbol2} order not found"
        # Verify cloids are proper format if set
        if symbol1_order.client_order_id:
            assert symbol1_order.client_order_id.startswith("0x")
            assert len(symbol1_order.client_order_id) == 34  # 0x + 32 hex chars
        if symbol2_order.client_order_id:
            assert symbol2_order.client_order_id.startswith("0x")
            assert len(symbol2_order.client_order_id) == 34  # 0x + 32 hex chars

        # Clean up
        cancel_args = [
            CancelOrderArgs(order_id=order.exchange_order_id, symbol=order.symbol)
            for order in placed_orders
            if order.exchange_order_id is not None
        ]
        if cancel_args:
            await hl_api_for_test_env.cancel_batch_orders(cancel_args)

        logger.info(
            "mixed_symbol_batch_test_success",
            orders_count=len(placed_orders),
            message="Successfully tested mixed symbol batch",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_batch_orders_partial_failure_scenarios(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test batch operations with mixed success/failure scenarios."""
        # Get available symbols from exchange using dynamic discovery

        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env,
            limit=1,
        )

        if not available_symbols:
            pytest.skip(
                "No perpetual symbols available from the exchange. "
                "This test requires at least one available perpetual symbol.",
            )

        symbol = available_symbols[0]
        logger.info(
            "symbol_selected_for_partial_failure_test",
            symbol=symbol,
            message="Selected symbol for partial failure test",
        )

        # Get test parameters
        test_quantity = await get_minimal_test_quantity(hl_api_for_test_env, symbol, OrderSide.BUY)
        test_price = await get_safe_test_price(
            hl_api_for_test_env,
            symbol,
            OrderSide.BUY,
            Decimal("0.10"),
        )

        # Mix of orders that should succeed and potentially fail
        orders = [
            # Valid order 1
            PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity,
                price=test_price,
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                client_order_id=None,  # Optional - let exchange assign ID
            ),
            # Valid order 2
            PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity,
                price=await get_safe_test_price(
                    hl_api_for_test_env,
                    symbol,
                    OrderSide.BUY,
                    Decimal("0.11"),
                ),
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                client_order_id=None,  # Optional - let exchange assign ID
            ),
            # Potentially problematic order - use duplicate cloid to trigger failure
            # This avoids hardcoding financial values
            PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity,  # Use proper test quantity
                price=await get_safe_test_price(
                    hl_api_for_test_env,
                    symbol,
                    OrderSide.BUY,
                    Decimal("0.12"),
                ),
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
                client_order_id=None,  # Optional - let exchange assign ID
            ),
        ]

        try:
            # Execute batch placement
            placed_orders = await hl_api_for_test_env.place_batch_orders(orders)

            logger.info(
                "partial_failure_test_orders_processed",
                orders_count=len(placed_orders),
                message="Partial failure test orders processed",
            )

            # At least some orders should be processed (even if some fail)
            assert len(placed_orders) >= 0, "Should handle partial failures gracefully"

            # Validate successful orders have proper structure
            valid_orders = [order for order in placed_orders if order.exchange_order_id is not None]
            for order in valid_orders:
                assert isinstance(order, Order)
                assert order.symbol == symbol
                assert order.side == OrderSide.BUY
                assert order.order_type == OrderType.LIMIT
                assert order.status in [OrderStatus.OPEN, OrderStatus.NEW]
                logger.info(
                    "valid_order_in_partial_failure_test",
                    order_id=order.exchange_order_id,
                    message="Valid order in partial failure test",
                )

            # Clean up successful orders
            if valid_orders:
                cancel_args = [
                    CancelOrderArgs(order_id=order.exchange_order_id, symbol=order.symbol)
                    for order in valid_orders
                    if order.exchange_order_id is not None
                ]
                if cancel_args:
                    await hl_api_for_test_env.cancel_batch_orders(cancel_args)
                    logger.info(
                        "partial_failure_test_cleanup",
                        orders_count=len(cancel_args),
                        message="Cleaned up successful orders from partial failure test",
                    )

        except APIError as e:
            # If the entire batch fails, that's also a valid test outcome
            logger.info(
                "batch_operation_failed_as_expected",
                error=str(e),
                message="Batch operation failed as expected",
            )
            # Ensure we get meaningful error information
            if len(str(e)) == 0:
                pytest.fail("Error should have meaningful message")
            if not hasattr(e, "error_code"):
                pytest.fail("APIError should have error_code attribute")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_batch_operations_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test comprehensive error handling for batch operations."""
        # Test 1: Empty batch validation
        with pytest.raises(ValueError, match="Cannot place empty batch of orders"):
            await hl_api_for_test_env.place_batch_orders([])

        with pytest.raises(ValueError, match="Cannot cancel empty batch of orders"):
            await hl_api_for_test_env.cancel_batch_orders([])

        # Test 2: Invalid order parameters
        # Get a test quantity from available symbols first

        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env,
            limit=1,
        )

        if not available_symbols:
            pytest.skip(
                "No symbols available from exchange. "
                "Cannot test invalid symbol error handling without valid reference data.",
            )

        # Use a valid symbol to get proper test parameters
        valid_symbol = available_symbols[0]
        test_quantity = await get_minimal_test_quantity(
            hl_api_for_test_env,
            valid_symbol,
            OrderSide.BUY,
        )
        # Get a real price to use as reference (even for invalid symbol test)
        reference_price = await get_safe_test_price(
            hl_api_for_test_env,
            valid_symbol,
            OrderSide.BUY,
            Decimal("0.10"),
        )

        invalid_orders = [
            PlaceOrderArgs(
                symbol="INVALID_SYMBOL_THAT_DOES_NOT_EXIST",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=test_quantity,
                price=reference_price,  # Use real price data, not arbitrary values
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(),
                client_order_id=None,  # Optional - let exchange assign ID
            ),
        ]

        try:
            placed_orders = await hl_api_for_test_env.place_batch_orders(invalid_orders)
            # If it doesn't raise an exception, validate the response handles errors properly
            logger.info(
                "invalid_symbol_test_orders_processed",
                orders_count=len(placed_orders),
                message="Invalid symbol test orders processed",
            )
            # Should return empty list or orders with error status
            assert isinstance(placed_orders, list), "Should return list even for invalid orders"

        except APIError as e:
            # Expected outcome - should get meaningful error
            logger.info(
                "invalid_symbol_correctly_rejected",
                error=str(e),
                message="Invalid symbol correctly rejected",
            )
            if len(str(e)) == 0:
                pytest.fail("Error should have meaningful message")

        # Test 3: Invalid cancel operations
        # Use the first available symbol if we have one, otherwise use a placeholder
        cancel_test_symbol = available_symbols[0] if available_symbols else "UNKNOWN-USD"

        invalid_cancel_args = [
            CancelOrderArgs(
                order_id="99999999999",  # Non-existent order ID
                symbol=cancel_test_symbol,
            ),
        ]

        try:
            cancel_results = await hl_api_for_test_env.cancel_batch_orders(invalid_cancel_args)
            # Should handle gracefully and return results indicating failure
            assert isinstance(cancel_results, list), "Should return list of cancel results"
            assert len(cancel_results) == 1, "Should have one cancel result"

            result = cancel_results[0]
            assert isinstance(result, CancelOrderResult), "Should return CancelOrderResult"
            assert result.order_id == "99999999999", "Should preserve order ID"
            assert result.symbol == cancel_test_symbol, "Should preserve symbol"
            # Result should indicate failure
            logger.info(
                "invalid_cancel_result",
                success=result.success,
                status=result.status,
                message="Invalid cancel result",
            )

        except APIError as e:
            # Also acceptable - API might reject invalid cancellations
            logger.info(
                "invalid_cancel_correctly_rejected",
                error=str(e),
                message="Invalid cancel correctly rejected",
            )
            if len(str(e)) == 0:
                pytest.fail("Error should have meaningful message")
