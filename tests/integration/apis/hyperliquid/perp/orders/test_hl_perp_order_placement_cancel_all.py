"""Integration tests for comprehensive Hyperliquid perpetual order lifecycle operations.

This module tests the complete order management lifecycle through Hyperliquid's
private /exchange endpoints with EIP-712 authentication for perpetual contracts.
Tests validate order placement, individual cancellation, batch cancellation,
and cancel-all business logic operations.

Model Focus: Order (Comprehensive Lifecycle - Perpetual)
- Tests placement of multiple buy limit orders with dynamic random symbols
- Validates individual order cancellation operations
- Tests batch cancellation of multiple orders simultaneously
- Validates cancel-all business logic across all open orders
- Comprehensive error handling for complex order lifecycle scenarios

Authentication: EIP-712 signing for all /exchange endpoint operations
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from cyberdelta.core.models.market.order import CancelOrderResult, Order
from tests.integration.apis.hyperliquid.shared.test_helpers import (
    HyperliquidTestHelpers,
    get_minimal_test_quantity,
    get_safe_test_price,
)

logger = get_logger(__name__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/orders/comprehensive"], indirect=True
)
class TestHyperliquidPerpOrdersComprehensive:
    """Comprehensive perpetual orders integration tests for complete order lifecycle operations.

    This class tests the full order management workflow including:
    - Multiple order placement with dynamic symbol selection
    - Individual order cancellation
    - Batch order cancellation
    - Cancel-all business logic validation

    All operations use real market data and dynamic test helpers to ensure
    robustness across different market conditions and account states.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_create_six_buy_limit_orders_with_random_symbols(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test 1: Create 6 buy limit orders with random symbols using test helpers.

        This test validates the placement of multiple orders across different symbols
        using dynamic market data and proper test helpers that adhere to
        TESTING_SECURITY_RULES.md requirements.
        """
        # Get available symbols from exchange using dynamic discovery
        available_symbols = await HyperliquidTestHelpers.get_available_perp_symbols(
            hl_api_for_test_env, limit=6
        )

        if len(available_symbols) < 6:
            pytest.skip(
                f"Insufficient symbols available for test: got {len(available_symbols)}, need 6. "
                "This test requires 6 available perpetual symbols from the exchange."
            )

        placed_orders: list[Order] = []

        try:
            # Place 6 buy limit orders across different symbols
            for i, symbol in enumerate(available_symbols):
                # Get dynamic test parameters for safe order placement
                market_price = await HyperliquidTestHelpers.get_current_market_price(
                    hl_api_for_test_env, symbol
                )
                # Use varying tolerance to create different price levels (5% to 15% below market)
                tolerance_percent = Decimal("5") + (Decimal("2") * i)  # 5%, 7%, 9%, 11%, 13%, 15%
                safe_tolerance = market_price * (tolerance_percent / Decimal("100"))

                test_price = await get_safe_test_price(
                    hl_api_for_test_env, symbol, OrderSide.BUY, tolerance=safe_tolerance
                )
                test_quantity = await get_minimal_test_quantity(
                    hl_api_for_test_env, symbol, OrderSide.BUY
                )

                # Define order parameters using dynamic values
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=test_quantity,
                    price=test_price,
                    time_in_force=TimeInForce.GTC,
                )

                # Execute the API call
                placed_order = await hl_api_for_test_env.place_order(place_args)
                placed_orders.append(placed_order)

                # Validate each placed order
                assert isinstance(placed_order, Order), "place_order() should return Order instance"
                assert placed_order.exchange == "hyperliquid", (
                    f"Order.exchange should be 'hyperliquid', got {placed_order.exchange}"
                )
                assert placed_order.symbol == symbol, (
                    f"Order symbol should match request, got {placed_order.symbol}"
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
                    f"quantity_requested must be Decimal, got "
                    f"{type(placed_order.quantity_requested)}"
                )
                assert isinstance(placed_order.price, Decimal), (
                    f"price must be Decimal, got {type(placed_order.price)}"
                )
                assert isinstance(placed_order.quantity_filled, Decimal), (
                    f"quantity_filled must be Decimal, got {type(placed_order.quantity_filled)}"
                )

                # Small delay between orders to avoid rate limiting
                await asyncio.sleep(0.5)

            # Validate all 6 orders were successfully placed
            assert len(placed_orders) == 6, f"Should have placed 6 orders, got {len(placed_orders)}"

            # Validate all orders have unique symbols
            order_symbols = [order.symbol for order in placed_orders]
            unique_symbols = set(order_symbols)
            assert len(unique_symbols) == 6, (
                f"All orders should have different symbols, got duplicates: {order_symbols}"
            )

            # Validate all orders have unique exchange order IDs
            order_ids = [order.exchange_order_id for order in placed_orders]
            unique_ids = set(order_ids)
            assert len(unique_ids) == 6, (
                f"All orders should have unique IDs, got duplicates: {order_ids}"
            )

        except Exception as e:
            # Clean up any placed orders on failure
            await self._cleanup_orders_on_failure(hl_api_for_test_env, placed_orders)
            pytest.fail(f"Failed to place 6 buy limit orders: {e}")

        # Store placed orders for subsequent tests
        self._test_orders: list[Order] = placed_orders

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_one_order(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test 2: Cancel 1 order from the previously placed orders.

        This test validates individual order cancellation functionality
        using a real order ID from the previous test.
        """
        # Ensure we have orders from the previous test
        if not hasattr(self, "_test_orders") or not self._test_orders:
            pytest.skip(
                "No orders available from previous test. "
                "This test depends on orders from "
                "test_create_six_buy_limit_orders_with_random_symbols."
            )

        # Select the first order for cancellation
        order_to_cancel = self._test_orders[0]
        order_id = order_to_cancel.exchange_order_id
        symbol = order_to_cancel.symbol

        assert order_id is not None, "Order ID should not be None"

        try:
            # Verify order exists before cancelling
            open_orders = await hl_api_for_test_env.get_open_orders()
            order_exists = any(order.exchange_order_id == order_id for order in open_orders)

            if not order_exists:
                pytest.skip(
                    f"Order {order_id} not found in open orders. "
                    "Order may have been filled or already cancelled."
                )

            # Cancel the order
            cancel_args = CancelOrderArgs(
                order_id=order_id,
                symbol=symbol,
            )

            cancel_result = await hl_api_for_test_env.cancel_order(cancel_args)

            # Validate cancellation success
            assert cancel_result is True, "cancel_order() should return True on success"

            # Verify order is no longer in open orders using public API polling
            await self._verify_order_cancellation_using_public_api(
                hl_api_for_test_env, order_id, timeout_seconds=30
            )

            # Remove cancelled order from our tracking list
            self._test_orders.remove(order_to_cancel)

        except APIError as e:
            # Distinguish expected business errors from system errors
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                pytest.skip(f"Order {order_id} already cancelled or not found: {e}")
            else:
                pytest.fail(f"Unexpected API error during order cancellation: {e}")
        except Exception as e:
            pytest.fail(f"System error during order cancellation: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_two_orders_simultaneously(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test 3: Cancel 2 orders at the same time using concurrent cancellation.

        This test validates batch cancellation functionality by cancelling
        two orders simultaneously using asyncio.gather.
        """
        # Ensure we have at least 2 orders remaining
        if not hasattr(self, "_test_orders") or len(self._test_orders) < 2:
            pytest.skip(
                "Insufficient orders available for batch cancellation test. "
                f"Need at least 2 orders, have {len(getattr(self, '_test_orders', []))}"
            )

        # Select two orders for simultaneous cancellation
        orders_to_cancel = self._test_orders[:2]

        try:
            # Verify both orders exist before cancelling
            open_orders = await hl_api_for_test_env.get_open_orders()
            open_order_ids = {order.exchange_order_id for order in open_orders}

            orders_available: list[Order] = []
            for order in orders_to_cancel:
                if order.exchange_order_id and order.exchange_order_id in open_order_ids:
                    orders_available.append(order)

            if len(orders_available) < 2:
                pytest.skip(
                    f"Insufficient open orders for batch cancellation. "
                    f"Need 2 open orders, found {len(orders_available)}"
                )

            # Prepare cancellation arguments for both orders
            order_1: Order = orders_available[0]
            order_2: Order = orders_available[1]

            if not order_1.exchange_order_id or not order_2.exchange_order_id:
                pytest.skip("Orders missing exchange_order_id for batch cancellation")

            cancel_args_1 = CancelOrderArgs(
                order_id=order_1.exchange_order_id,
                symbol=order_1.symbol,
            )
            cancel_args_2 = CancelOrderArgs(
                order_id=order_2.exchange_order_id,
                symbol=order_2.symbol,
            )

            # Cancel both orders simultaneously
            cancel_results = await asyncio.gather(
                hl_api_for_test_env.cancel_order(cancel_args_1),
                hl_api_for_test_env.cancel_order(cancel_args_2),
                return_exceptions=True,
            )

            # Validate both cancellations - testing boolean return values
            # Convert tuple to list for processing
            results_list = list(cancel_results)
            cancellation_results = self._process_cancellation_results(results_list)

            # Validate that we got meaningful results
            total_attempts = cancellation_results["successful"] + cancellation_results["failed"]
            assert total_attempts == 2, (
                f"Expected 2 cancellation attempts, got {total_attempts} "
                f"(successful: {cancellation_results['successful']}, "
                f"failed: {cancellation_results['failed']})"
            )

            # At least one cancellation should succeed for this test to be meaningful
            assert cancellation_results["successful"] >= 1, (
                f"At least one order should be successfully cancelled. "
                f"Got {cancellation_results['successful']} successes, "
                f"{cancellation_results['failed']} failures"
            )

            # Verify successfully cancelled orders are no longer in open orders
            await self._handle_successful_cancellations(
                hl_api_for_test_env,
                results_list,
                orders_available,
                cancellation_results["successful"],
            )

        except Exception as e:
            pytest.fail(f"Error during simultaneous order cancellation: {e}")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_remaining_orders_with_cancel_all(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test 4: Cancel remaining orders using cancel_all business logic.

        This test validates the cancel_all_orders functionality which should
        cancel all remaining open orders from our test session. Based on research,
        Hyperliquid doesn't have a native cancel_all API, so this tests our
        business logic that fetches open orders and cancels them individually.
        """
        try:
            # Get current open orders to see what we're working with
            initial_open_orders = await hl_api_for_test_env.get_open_orders()

            if not initial_open_orders:
                pytest.skip(
                    "No open orders found for cancel_all test. "
                    "All orders may have been cancelled in previous tests."
                )

            # Track order IDs before cancellation
            initial_order_ids = {
                order.exchange_order_id for order in initial_open_orders if order.exchange_order_id
            }
            initial_order_count = len(initial_open_orders)

            # Execute cancel_all_orders (business logic implementation)
            # This calls our business logic that fetches open orders and cancels each individually
            cancel_results = await hl_api_for_test_env.cancel_all_orders()

            # Validate cancel_all returns appropriate structure
            assert isinstance(cancel_results, list), (
                "cancel_all_orders should return list[CancelOrderResult]"
            )

            # Validate each cancelled order result structure and boolean success values
            validation_results = self._validate_cancel_all_results(
                cancel_results, initial_order_ids
            )
            total_attempts = len(cancel_results)
            successful_count = validation_results["successful_count"]
            failed_count = validation_results["failed_count"]
            successful_results = validation_results["successful_results"]

            # At least some operations should have been attempted
            assert total_attempts > 0, "Expected at least one cancellation attempt"

            # Validate success/failure counts make sense
            assert successful_count + failed_count == total_attempts, (
                f"Success ({successful_count}) + failed ({failed_count}) should equal "
                f"total attempts ({total_attempts})"
            )

            # Verify only successfully cancelled orders are no longer present
            if successful_count > 0:
                successfully_cancelled_ids = {
                    result.order_id for result in successful_results if result.order_id
                }

                if successfully_cancelled_ids:
                    await self._verify_all_orders_cancelled_using_public_api(
                        hl_api_for_test_env, successfully_cancelled_ids, timeout_seconds=30
                    )

            # Clear our tracking list since we've completed the cancel_all test
            if hasattr(self, "_test_orders"):
                self._test_orders.clear()

            # Final validation - total results should not exceed initial order count
            assert total_attempts <= initial_order_count, (
                f"Cannot attempt to cancel more orders ({total_attempts}) "
                f"than were initially open ({initial_order_count})"
            )

            # At least some cancellations should have been attempted if there were orders
            if initial_order_count > 0:
                assert total_attempts > 0, (
                    f"Expected at least one cancellation attempt for "
                    f"{initial_order_count} open orders"
                )

        except APIError as e:
            # Distinguish business errors from system errors
            if "no orders" in str(e).lower() or "nothing to cancel" in str(e).lower():
                pytest.skip(f"No orders available to cancel: {e}")
            else:
                pytest.fail(f"API error during cancel_all_orders: {e}")
        except Exception as e:
            pytest.fail(f"System error during cancel_all_orders: {e}")

    # Helper Methods

    def _process_cancellation_results(
        self, cancel_results: list[bool | BaseException]
    ) -> dict[str, int]:
        """Process cancellation results and return success/failure counts."""
        successful_cancellations = 0
        failed_cancellations = 0

        for i, result in enumerate(cancel_results):
            if isinstance(result, Exception):
                # Check if this is an expected business error
                if "not found" in str(result).lower():
                    # Order already cancelled - count as expected failure
                    failed_cancellations += 1
                    continue
                else:
                    pytest.fail(f"Unexpected error cancelling order {i + 1}: {result}")
            elif isinstance(result, bool):
                # Test that cancel_order returns boolean
                if result is True:
                    successful_cancellations += 1
                elif result is False:
                    failed_cancellations += 1
                else:
                    pytest.fail(f"cancel_order should return boolean, got {type(result)}: {result}")
            else:
                pytest.fail(
                    f"Unexpected cancellation result type for order {i + 1}: "
                    f"{type(result)} = {result}"
                )

        return {"successful": successful_cancellations, "failed": failed_cancellations}

    async def _handle_successful_cancellations(
        self,
        api: HyperliquidAPI,
        cancel_results: list[bool | BaseException],
        orders_available: list[Order],
        successful_count: int,
    ) -> None:
        """Handle verification and cleanup of successfully cancelled orders."""
        if successful_count > 0:
            successfully_cancelled_order_ids: set[str] = set()
            for _i, (result, order) in enumerate(
                zip(cancel_results, orders_available, strict=False)
            ):
                if isinstance(result, bool) and result is True and order.exchange_order_id:
                    successfully_cancelled_order_ids.add(order.exchange_order_id)

            # Use public API polling to verify cancellation
            if successfully_cancelled_order_ids:
                await self._verify_all_orders_cancelled_using_public_api(
                    api, successfully_cancelled_order_ids, timeout_seconds=15
                )

        # Remove only successfully cancelled orders from tracking list
        for _i, (result, order) in enumerate(zip(cancel_results, orders_available, strict=False)):
            if isinstance(result, bool) and result is True and order in self._test_orders:
                self._test_orders.remove(order)

    def _validate_cancel_all_results(
        self, cancel_results: list[CancelOrderResult], initial_order_ids: set[str]
    ) -> dict[str, Any]:
        """Validate cancel_all results and categorize by success/failure."""
        successful_results: list[CancelOrderResult] = []
        failed_results: list[CancelOrderResult] = []

        for cancel_result in cancel_results:
            assert isinstance(cancel_result, CancelOrderResult), (
                "Each cancelled order should be CancelOrderResult instance"
            )

            # Test the boolean success property
            assert isinstance(cancel_result.success, bool), (
                f"CancelOrderResult.success must be boolean, got {type(cancel_result.success)}"
            )

            # Categorize results by success/failure
            if cancel_result.success:
                successful_results.append(cancel_result)
                # Successful results should have order information
                if cancel_result.order_id:
                    assert cancel_result.order_id in initial_order_ids, (
                        f"Successfully cancelled order {cancel_result.order_id} "
                        "was not in initial open orders"
                    )
            else:
                failed_results.append(cancel_result)
                # Failed results should have error information
                assert cancel_result.message, "Failed cancellation should include error message"

        return {
            "successful_results": successful_results,
            "failed_results": failed_results,
            "successful_count": len(successful_results),
            "failed_count": len(failed_results),
        }

    async def _cleanup_orders_on_failure(
        self, api: HyperliquidAPI, placed_orders: list[Order]
    ) -> None:
        """Clean up placed orders when test fails."""
        for order in placed_orders:
            if order.exchange_order_id:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=order.symbol,
                    )
                    await api.cancel_order(cancel_args)
                except Exception as cleanup_error:
                    logger.warning(
                        f"Failed to cleanup order {order.exchange_order_id}: {cleanup_error}"
                    )

        # Wait for cleanup to complete using public API
        try:
            # Get remaining orders after cleanup attempts
            await asyncio.sleep(2)  # Brief wait for cleanup to process
        except Exception as cleanup_error:
            logger.warning(f"Failed to complete cleanup wait: {cleanup_error}")

    async def _verify_order_cancellation_using_public_api(
        self, api: HyperliquidAPI, order_id: str, timeout_seconds: int = 30
    ) -> None:
        """Verify order cancellation using only public API methods.

        Polls get_open_orders() to verify the specified order is no longer present.
        """
        start_time = time.time()
        attempt = 0

        while time.time() - start_time < timeout_seconds:
            try:
                open_orders = await api.get_open_orders()
                order_still_exists = any(
                    order.exchange_order_id == order_id for order in open_orders
                )

                if not order_still_exists:
                    return  # Order successfully cancelled

                # Adaptive polling interval: shorter initially, longer as time passes
                attempt += 1
                if attempt <= 3:
                    interval = 0.5  # 0.5s for first 3 attempts
                elif attempt <= 10:
                    interval = 1.0  # 1s for next 7 attempts
                else:
                    interval = 2.0  # 2s for remaining attempts

                await asyncio.sleep(interval)

            except Exception as e:
                # If we can't check order status, that's a system error
                pytest.fail(f"Failed to verify order cancellation using public API: {e}")

        # If we get here, the order is still present after timeout
        pytest.fail(
            f"Order {order_id} still appears in open orders after {timeout_seconds} seconds. "
            "Order cancellation verification failed."
        )

    async def _verify_all_orders_cancelled_using_public_api(
        self, api: HyperliquidAPI, expected_cancelled_order_ids: set[str], timeout_seconds: int = 30
    ) -> None:
        """Verify all specified orders are cancelled using only public API methods.

        Polls get_open_orders() to verify none of the specified orders are still present.
        """
        start_time = time.time()
        attempt = 0

        while time.time() - start_time < timeout_seconds:
            try:
                open_orders = await api.get_open_orders()
                current_order_ids: set[str | None] = {
                    order.exchange_order_id for order in open_orders
                }
                # Filter out None values for intersection
                valid_current_order_ids: set[str] = {
                    oid for oid in current_order_ids if oid is not None
                }

                # Check if any of our expected cancelled orders are still present
                still_open_orders = expected_cancelled_order_ids.intersection(
                    valid_current_order_ids
                )

                if not still_open_orders:
                    return  # All orders successfully cancelled

                # Adaptive polling interval
                attempt += 1
                if attempt <= 3:
                    interval = 0.5  # 0.5s for first 3 attempts
                elif attempt <= 10:
                    interval = 1.0  # 1s for next 7 attempts
                else:
                    interval = 2.0  # 2s for remaining attempts

                await asyncio.sleep(interval)

            except Exception as e:
                # If we can't check order status, that's a system error
                pytest.fail(f"Failed to verify all orders cancelled using public API: {e}")

        # If we get here, some orders are still present after timeout
        final_open_orders = await api.get_open_orders()
        final_order_ids: set[str | None] = {order.exchange_order_id for order in final_open_orders}
        valid_final_order_ids: set[str] = {oid for oid in final_order_ids if oid is not None}
        remaining_orders = expected_cancelled_order_ids.intersection(valid_final_order_ids)

        pytest.fail(
            f"{len(remaining_orders)} orders still appear in open orders after "
            f"{timeout_seconds} seconds: {remaining_orders}. "
            f"cancel_all_orders verification failed."
        )

    def setup_method(self) -> None:
        """Setup method to initialize test state."""
        self._test_orders = []

    def teardown_method(self) -> None:
        """Teardown method to clean up test state."""
        if hasattr(self, "_test_orders"):
            del self._test_orders
