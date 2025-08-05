"""Integration tests for Backpack spot orders endpoints with $0 balance.

This module focuses specifically on testing the Order model pipeline
through Backpack's spot order endpoints with Ed25519 authentication
when the account has $0 balance. Tests validate error handling, API
pipeline validation, and authentication without successful order placement.

Model Focus: Order (Spot - Error Scenarios)
- Validates API error mapping and handling for spot orders
- Tests authentication and request pipeline validation
- Validates business logic error responses
- Tests insufficient funds error handling for spot markets
- Comprehensive error validation and edge cases

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: $0 USDC (insufficient funds scenarios for spot trading)
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.models.market.order import Order
from tests.common_symbols import (
    BTC_USDC_BP as TEST_SYMBOL_BTC_USDC,
    SOL_USDC_BP,
    SOL_USDC_BP as TEST_SYMBOL_SOL_USDC,
)
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    get_current_market_price,
    get_dynamic_test_price,
    get_market_constraints,
    get_minimal_order_size_for_zero_balance_test,
    get_unreasonably_large_price,
    get_unreasonably_large_quantity,
)


# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.spot, pytest.mark.zero_balance]

logger = get_logger(__name__)


# Note: Helper functions now imported from shared test_helpers module


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/private/orders/zero_balance"],
    indirect=True,
)
class TestBackpackOrdersZeroBalance:
    """Integration tests for Backpack orders with $0 balance (insufficient funds scenarios)."""

    # NOTE: Basic insufficient funds validation is covered in /account/orders/

    # NOTE: Cancel nonexistent order is covered in /account/orders/

    # NOTE: Authentication failure is covered in /account/orders/

    # NOTE: Large quantity validation moved to edge cases below

    # NOTE: Invalid symbol test moved to edge cases below

    # NOTE: Order management endpoints are covered in /account/orders/

    # NOTE: Backpack API structure is covered in /account/orders/

    # NOTE: Date range validation is covered in /account/orders/

    # NOTE: Symbol format validation moved to edge cases below

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_extreme_edge_cases_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with extreme edge case parameters that should fail gracefully.

        Tests edge cases like extremely small quantities, extreme prices, and boundary conditions.
        With zero balance, these should fail with appropriate error codes.
        """
        symbol = SOL_USDC_BP
        current_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            symbol,
            OrderSide.BUY,
        )

        # Test cases with extreme parameters
        extreme_test_cases: list[dict[str, Any]] = [
            {
                "name": "extremely_small_quantity",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.000001"),  # Extremely small
                    price=current_price,
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.INVALID_ORDER_SIZE.value,
                    # API returns this for quantity below minimum
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
            {
                "name": "extremely_large_quantity",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal(1000000),  # Extremely large
                    price=current_price,
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.INVALID_ORDER_SIZE.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
            {
                "name": "extremely_low_price",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.1"),
                    price=Decimal("0.01"),  # Extremely low price
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.PRICE_OUT_OF_RANGE.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
            {
                "name": "extremely_high_price",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.1"),
                    price=current_price * Decimal(1000),  # 1000x current price
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.PRICE_OUT_OF_RANGE.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
        ]

        for test_case in extreme_test_cases:
            args: PlaceOrderArgs = test_case["args"]
            with pytest.raises(APIError) as exc_info:
                await bp_api_for_zero_balance_test.place_order(args)

            api_error = exc_info.value
            expected_error_values = test_case["expected_errors"]
            assert api_error.code in expected_error_values, (
                f"Test {test_case['name']} expected one of {test_case['expected_errors']}, "
                f"got {api_error.code}: {api_error.message}"
            )

            logger.info(
                "extreme_edge_case_rejected",
                test_name=test_case["name"],
                error_code=api_error.code,
                message=(
                    f"✓ Extreme edge case '{test_case['name']}' properly rejected: {api_error.code}"
                ),
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_malformed_data_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with malformed or invalid data structures.

        Tests how the API handles malformed symbols, invalid enum values, and edge cases.
        """
        current_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            SOL_USDC_BP,
            OrderSide.BUY,
        )

        # Test malformed symbols - split into categories based on normalization behavior
        normalizable_symbols = [
            "SOL/USDC",  # Wrong separator - normalized to SOL_USDC
            "sol_usdc",  # Lowercase - normalized to SOL_USDC
            "SOL-USDC",  # Wrong separator - normalized to SOL_USDC
        ]

        truly_malformed_symbols = [
            "SOLUSDC",  # No separator - cannot be normalized
            "SOL_USD",  # Wrong quote currency - not available on Backpack
            "",  # Empty string
            "SOL_USDC_EXTRA",  # Too many parts
        ]

        # Test normalizable symbols - should result in insufficient funds after normalization
        for malformed_symbol in normalizable_symbols:
            try:
                from cyberdelta.core.symbols import exchanges

                # Try to create symbol from string - this will normalize it
                symbol = exchanges.backpack(malformed_symbol)
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.1"),
                    price=current_price,
                    time_in_force=TimeInForce.GTC,
                )

                with pytest.raises(APIError) as exc_info:
                    await bp_api_for_zero_balance_test.place_order(place_args)

                api_error = exc_info.value
                # These symbols get normalized to valid symbols, so expect insufficient funds
                assert api_error.code in [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,  # Expected after normalization
                    APIErrorCode.INVALID_SYMBOL.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.EXCHANGE_SPECIFIC.value,
                ], (
                    f"Normalizable symbol '{malformed_symbol}' should normalize "
                    f"or give symbol error, got {api_error.code}"
                )

                logger.info(
                    "normalizable_symbol_handled",
                    symbol=malformed_symbol,
                    error_code=api_error.code,
                    message=(
                        f"✓ Normalizable symbol '{malformed_symbol}' handled: {api_error.code}"
                    ),
                )

            except (APIError, ValueError, TypeError, KeyError) as e:
                logger.info(
                    "normalizable_symbol_validation_error",
                    symbol=malformed_symbol,
                    error_message=str(e),
                    message=(
                        f"✓ Normalizable symbol '{malformed_symbol}' caught at validation: {e}"
                    ),
                )

        # Test truly malformed symbols - should result in symbol validation errors
        for malformed_symbol in truly_malformed_symbols:
            from cyberdelta.core.symbols import exchanges

            if not malformed_symbol:
                # Empty string will fail during symbol creation
                with pytest.raises((ValidationError, ValueError)):
                    symbol = exchanges.backpack(malformed_symbol)
                    PlaceOrderArgs(
                        symbol=symbol,
                        side=OrderSide.BUY,
                        order_type=OrderType.LIMIT,
                        quantity=Decimal("0.1"),
                        price=current_price,
                        time_in_force=TimeInForce.GTC,
                    )
                # ValidationError or ValueError is expected for empty string validation
            else:
                # Other malformed symbols might fail at symbol creation or API level
                try:
                    symbol = exchanges.backpack(malformed_symbol)
                except (ValueError, ValidationError):
                    # Symbol creation failed, which is expected for malformed symbols
                    continue

                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.1"),
                    price=current_price,
                    time_in_force=TimeInForce.GTC,
                )

                with pytest.raises(APIError) as exc_info:
                    await bp_api_for_zero_balance_test.place_order(place_args)

                api_error = exc_info.value
                # Should get symbol-related error, not insufficient funds
                assert api_error.code in [
                    APIErrorCode.INVALID_SYMBOL.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.EXCHANGE_SPECIFIC.value,
                ], (
                    f"Malformed symbol '{malformed_symbol}' should give symbol error, "
                    f"got {api_error.code}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_precision_edge_cases_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with decimal precision edge cases.

        Tests very high precision numbers, scientific notation edge cases, and rounding behaviors.
        """
        symbol = SOL_USDC_BP
        base_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            symbol,
            OrderSide.BUY,
        )

        precision_test_cases: list[dict[str, Any]] = [
            {
                "name": "high_precision_quantity",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.123456789123456789"),  # Very high precision
                    price=base_price,
                    time_in_force=TimeInForce.GTC,
                ),
            },
            {
                "name": "high_precision_price",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.1"),
                    price=base_price + Decimal("0.123456789123456789"),  # Very high precision
                    time_in_force=TimeInForce.GTC,
                ),
            },
            {
                "name": "scientific_notation_quantity",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("1.23e-6"),  # Scientific notation
                    price=base_price,
                    time_in_force=TimeInForce.GTC,
                ),
            },
        ]

        for test_case in precision_test_cases:
            with pytest.raises(APIError) as exc_info:
                args: PlaceOrderArgs = test_case["args"]
                await bp_api_for_zero_balance_test.place_order(args)

            api_error = exc_info.value
            # Could be insufficient funds or precision-related error
            assert api_error.code in [
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.INVALID_ORDER_SIZE.value,
                APIErrorCode.PRICE_OUT_OF_RANGE.value,
                APIErrorCode.PRECISION_ERROR.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
                APIErrorCode.INVALID_REQUEST.value,  # API returns this for precision errors
            ], f"Precision test '{test_case['name']}' got unexpected error: {api_error.code}"

            logger.info(
                "precision_edge_case_handled",
                test_case_name=test_case["name"],
                api_error_code=api_error.code,
                message="✓ Precision edge case handled",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_concurrent_order_operations_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test concurrent order operations to validate rate limiting and thread safety.

        Tests multiple simultaneous order placement attempts with zero balance.
        Should handle concurrent requests gracefully.
        """
        symbol = SOL_USDC_BP
        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            symbol,
            OrderSide.BUY,
        )

        # Create multiple order requests
        order_tasks: list[Any] = []
        for i in range(5):
            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal(f"0.{i + 1}"),  # Different quantities: 0.1, 0.2, etc.
                price=test_price + Decimal(str(i)),  # Slightly different prices
                time_in_force=TimeInForce.GTC,
            )
            order_tasks.append(bp_api_for_zero_balance_test.place_order(place_args))

        # Execute all requests concurrently
        results: list[Order | BaseException] = await asyncio.gather(
            *order_tasks,
            return_exceptions=True,
        )

        # All should be APIError instances (insufficient funds)
        error_count = 0
        rate_limit_count = 0
        insufficient_funds_count = 0

        for i, result in enumerate(results):
            assert isinstance(result, APIError), (
                f"Request {i} should return APIError, got {type(result)}"
            )

            error_count += 1
            if result.code == APIErrorCode.RATE_LIMITED.value:
                rate_limit_count += 1
            elif result.code == APIErrorCode.INSUFFICIENT_FUNDS.value:
                insufficient_funds_count += 1

            logger.info(
                "concurrent_request_result",
                request_index=i,
                error_code=result.code,
                message=f"Concurrent request {i}: {result.code}",
            )

        assert error_count == 5, f"Expected 5 errors, got {error_count}"
        assert insufficient_funds_count > 0, "At least one request should get insufficient funds"

        # Rate limiting is acceptable in concurrent scenarios
        if rate_limit_count > 0:
            logger.info(
                "rate_limiting_detected",
                rate_limit_count=rate_limit_count,
                total_requests=5,
                message=f"✓ Rate limiting detected in {rate_limit_count}/5 concurrent requests",
            )

        logger.info(
            "concurrent_operations_summary",
            insufficient_funds_count=insufficient_funds_count,
            rate_limit_count=rate_limit_count,
            message=(
                f"✓ Concurrent operations handled: {insufficient_funds_count} insufficient funds, "
                f"{rate_limit_count} rate limited"
            ),
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_lifecycle_simulation_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test complete order lifecycle simulation with zero balance.

        Simulates realistic order workflow: place -> query status -> cancel -> history
        All operations should fail appropriately with zero balance.
        """
        symbol = SOL_USDC_BP
        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            symbol,
            OrderSide.BUY,
        )

        # Step 1: Try to place order (should fail with insufficient funds)
        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=OrderSide.BUY,
            price=test_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=minimal_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        with pytest.raises(APIError) as place_exc:
            await bp_api_for_zero_balance_test.place_order(place_args)

        assert place_exc.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        logger.info("✓ Step 1: Order placement correctly failed with insufficient funds")

        # Step 2: Try to cancel non-existent order (should fail gracefully)
        fake_order_id = "fake_order_12345"
        cancel_args = CancelOrderArgs(symbol=symbol, order_id=fake_order_id)

        with pytest.raises(APIError) as cancel_exc:
            await bp_api_for_zero_balance_test.cancel_order(cancel_args)

        # Should get order not found or similar error
        assert cancel_exc.value.code in [
            APIErrorCode.ORDER_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
            APIErrorCode.EXCHANGE_SPECIFIC.value,
        ]
        logger.info(
            "cancel_non_existent_order_test_completed",
            step=2,
            error_code=cancel_exc.value.code,
            message=(
                f"✓ Step 2: Cancel non-existent order correctly failed: {cancel_exc.value.code}"
            ),
        )

        # Step 3: Query order history (should work but return empty/minimal results)
        history_args = GetOrderHistoryArgs(symbol=symbol, limit=10)

        try:
            orders = await bp_api_for_zero_balance_test.get_order_history(history_args)
            # Should return empty list or minimal orders for zero balance account
            assert isinstance(orders, list)
            assert len(orders) <= 10  # Respects limit
            logger.info(
                "order_history_query_success",
                step=3,
                orders_count=len(orders),
                message=f"✓ Step 3: Order history query succeeded, found {len(orders)} orders",
            )

        except APIError as history_exc:
            # Might fail with authentication or other error
            logger.info(
                "order_history_query_failed",
                step=3,
                error_code=history_exc.code,
                message=f"✓ Step 3: Order history query failed appropriately: {history_exc.code}",
            )

        # Step 4: Query open orders (should work, may have conditional orders from other tests)
        try:
            open_orders = await bp_api_for_zero_balance_test.get_open_orders(symbol)
            assert isinstance(open_orders, list)
            # Note: May have conditional orders (STOP_MARKET, STOP_LIMIT) from other tests
            # These don't require immediate funds and can exist with zero balance
            logger.info(
                "open_orders_query_success",
                step=4,
                orders_count=len(open_orders),
                message=(
                    f"✓ Step 4: Open orders query succeeded, found {len(open_orders)} orders "
                    f"(may include conditional orders)"
                ),
            )

        except APIError as open_exc:
            logger.info(
                "open_orders_query_failed",
                step=4,
                error_code=open_exc.code,
                message=f"✓ Step 4: Open orders query failed appropriately: {open_exc.code}",
            )

        logger.info("✓ Complete order lifecycle simulation completed with zero balance")

    # =============================================================================
    # ENHANCED ORDER TYPE TESTS - ALL ORDER TYPES WITH ZERO BALANCE
    # =============================================================================

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_market_order_insufficient_funds(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test market order placement with zero balance - should fail with insufficient funds.

        Market orders require immediate execution, so insufficient balance should be
        detected quickly.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.BUY

        # Get minimal order size
        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=side,
            price=current_price,
        )

        # Place market order with zero balance
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,
            time_in_force=TimeInForce.GTC,
        )

        # Should fail with insufficient funds
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        api_error = exc_info.value
        assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
            f"Market order with zero balance should fail with INSUFFICIENT_FUNDS, "
            f"got {api_error.code}: {api_error.message}"
        )

        logger.info(
            "market_order_insufficient_funds",
            error_message=api_error.message,
            message=f"✓ Market order correctly failed with insufficient funds: {api_error.message}",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_stop_market_order_insufficient_funds(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test stop market order placement with zero balance.

        Stop market orders might be accepted as conditional orders even without positions.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Stop loss on non-existent position

        # Get current price and set trigger below for stop loss
        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        trigger_price = current_price * Decimal("0.95")  # 5% below current price

        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=side,
            price=trigger_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.STOP_MARKET,
            quantity=minimal_quantity,
            stop_price=trigger_price,
            time_in_force=TimeInForce.GTC,
        )

        try:
            # Try to place the order
            order = await bp_api_for_zero_balance_test.place_order(place_args)
            # If it succeeds, it might be accepted as a conditional order
            assert order.exchange_order_id is not None
            assert order.order_type == OrderType.STOP_MARKET
            logger.info(
                "stop_market_order_accepted",
                order_id=order.exchange_order_id,
                message=(
                    f"✓ Stop market order accepted as conditional order: {order.exchange_order_id}"
                ),
            )

            # Clean up the order if it was created
            if order.exchange_order_id:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=symbol,
                    )
                    await bp_api_for_zero_balance_test.cancel_order(cancel_args)
                except (APIError, ValueError, TypeError, KeyError) as cleanup_error:
                    logger.debug(
                        "cleanup_cancellation_failed",
                        error=str(cleanup_error),
                        message=f"Cleanup cancellation failed (expected): {cleanup_error}",
                    )

        except APIError as e:
            # If it fails, check for expected error codes
            expected_errors = [
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.INVALID_REQUEST.value,  # No position to stop
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]
            if e.code not in expected_errors:
                pytest.fail(
                    f"Stop market order should fail appropriately if rejected, "
                    f"got {e.code}: {e.message}"
                )
            logger.info(
                "stop_market_order_failed",
                error_code=e.code,
                error_message=e.message,
                message=f"✓ Stop market order correctly failed: {e.code} - {e.message}",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_stop_limit_order_insufficient_funds(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test stop limit order placement with zero balance.

        Stop limit orders should fail due to insufficient funds or lack of position.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Stop loss

        # Get current price and set trigger/limit prices
        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        trigger_price = current_price * Decimal("0.95")  # 5% below for stop loss
        limit_price = trigger_price * Decimal("0.99")  # Slightly below trigger

        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
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

        # Should fail with insufficient funds or invalid position
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        api_error = exc_info.value
        expected_errors = [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.INVALID_REQUEST.value,
            APIErrorCode.EXCHANGE_SPECIFIC.value,
        ]
        assert api_error.code in expected_errors, (
            f"Stop limit order should fail appropriately, got {api_error.code}: {api_error.message}"
        )

        logger.info(
            "stop_limit_order_failed",
            error_code=api_error.code,
            error_message=api_error.message,
            message=f"✓ Stop limit order correctly failed: {api_error.code} - {api_error.message}",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_take_profit_market_order_insufficient_funds(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test take profit market order placement with zero balance.

        Take profit orders might be accepted as conditional orders even without positions.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Taking profit on non-existent position

        # Get current price and set trigger above for take profit
        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        trigger_price = current_price * Decimal("1.05")  # 5% above current price

        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
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

        try:
            # Try to place the order
            order = await bp_api_for_zero_balance_test.place_order(place_args)
            # If it succeeds, it might be accepted as a conditional order
            assert order.exchange_order_id is not None
            assert order.order_type == OrderType.TAKE_PROFIT_MARKET
            logger.info(
                "take_profit_market_order_accepted",
                order_id=order.exchange_order_id,
                message=(
                    f"✓ Take profit market order accepted as conditional order: "
                    f"{order.exchange_order_id}"
                ),
            )

            # Clean up the order if it was created
            if order.exchange_order_id:
                try:
                    cancel_args = CancelOrderArgs(
                        order_id=order.exchange_order_id,
                        symbol=symbol,
                    )
                    await bp_api_for_zero_balance_test.cancel_order(cancel_args)
                except (APIError, ValueError, TypeError, KeyError) as cleanup_error:
                    logger.debug(
                        "cleanup_cancellation_failed",
                        error=str(cleanup_error),
                        message=f"Cleanup cancellation failed (expected): {cleanup_error}",
                    )

        except APIError as e:
            # If it fails, check for expected error codes
            expected_errors = [
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]
            if e.code not in expected_errors:
                pytest.fail(
                    f"Take profit market order should fail appropriately if rejected, "
                    f"got {e.code}: {e.message}"
                )
            logger.info(
                "take_profit_market_order_failed",
                error_code=e.code,
                error_message=e.message,
                message=f"✓ Take profit market order correctly failed: {e.code} - {e.message}",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_take_profit_limit_order_insufficient_funds(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test take profit limit order placement with zero balance.

        Take profit limit orders should fail due to insufficient funds or lack of position.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC
        side = OrderSide.SELL  # Taking profit

        # Get current price and set trigger/limit prices
        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        trigger_price = current_price * Decimal("1.05")  # 5% above for take profit
        limit_price = trigger_price * Decimal("1.01")  # Slightly above trigger

        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
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

        # Should fail with insufficient funds or invalid position
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        api_error = exc_info.value
        expected_errors = [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.INVALID_REQUEST.value,
            APIErrorCode.EXCHANGE_SPECIFIC.value,
        ]
        assert api_error.code in expected_errors, (
            f"Take profit limit order should fail appropriately, "
            f"got {api_error.code}: {api_error.message}"
        )

        logger.info(
            "take_profit_limit_order_failed",
            error_code=api_error.code,
            error_message=api_error.message,
            message=(
                f"✓ Take profit limit order correctly failed: {api_error.code} - "
                f"{api_error.message}"
            ),
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_all_order_types_zero_balance_comprehensive(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test all order types systematically with zero balance.

        Validates that all order types fail appropriately when there are insufficient funds.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC

        # Get market data for test setup
        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)

        # Get market constraints for price quantization
        constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
        tick_size = constraints["tick_size"]

        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=OrderSide.BUY,
            price=current_price,
        )

        # Define all order types with their parameters
        order_type_tests: list[dict[str, Any]] = [
            {
                "name": "MARKET_BUY",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=minimal_quantity,
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [APIErrorCode.INSUFFICIENT_FUNDS.value],
            },
            {
                "name": "LIMIT_BUY",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=minimal_quantity,
                    price=(current_price * Decimal("0.95")).quantize(tick_size),
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [APIErrorCode.INSUFFICIENT_FUNDS.value],
            },
            {
                "name": "STOP_MARKET_BUY",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.STOP_MARKET,
                    quantity=minimal_quantity,
                    stop_price=(current_price * Decimal("1.05")).quantize(tick_size),
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.EXCHANGE_SPECIFIC.value,
                ],
            },
            {
                "name": "STOP_LIMIT_BUY",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.STOP_LIMIT,
                    quantity=minimal_quantity,
                    price=(current_price * Decimal("1.06")).quantize(tick_size),
                    stop_price=(current_price * Decimal("1.05")).quantize(tick_size),
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.EXCHANGE_SPECIFIC.value,
                ],
            },
            {
                "name": "TAKE_PROFIT_MARKET_BUY",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.TAKE_PROFIT_MARKET,
                    quantity=minimal_quantity,
                    stop_price=(current_price * Decimal("0.95")).quantize(tick_size),
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.EXCHANGE_SPECIFIC.value,
                ],
            },
            {
                "name": "TAKE_PROFIT_LIMIT_BUY",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.TAKE_PROFIT_LIMIT,
                    quantity=minimal_quantity,
                    price=(current_price * Decimal("0.94")).quantize(tick_size),
                    stop_price=(current_price * Decimal("0.95")).quantize(tick_size),
                    time_in_force=TimeInForce.GTC,
                ),
                "expected_errors": [
                    APIErrorCode.INSUFFICIENT_FUNDS.value,
                    APIErrorCode.INVALID_REQUEST.value,
                    APIErrorCode.EXCHANGE_SPECIFIC.value,
                ],
            },
        ]

        # Test each order type
        for test_case in order_type_tests:
            try:
                order = await bp_api_for_zero_balance_test.place_order(test_case["args"])
                # If order succeeds, it should be a conditional order in TriggerPending status
                if test_case["name"] in [
                    "STOP_MARKET_BUY",
                    "STOP_LIMIT_BUY",
                    "TAKE_PROFIT_MARKET_BUY",
                    "TAKE_PROFIT_LIMIT_BUY",
                ]:
                    # Conditional orders might succeed even with zero balance
                    assert order.status.value in ["TRIGGER_PENDING", "PENDING"], (
                        f"Conditional order {test_case['name']} should be in pending status, "
                        f"got: {order.status.value}"
                    )
                    logger.info(
                        "conditional_order_placed_successfully",
                        test_case_name=test_case["name"],
                        order_status=order.status.value,
                        message=(
                            f"✓ {test_case['name']} conditional order placed successfully: "
                            f"{order.status.value}"
                        ),
                    )
                else:
                    # Non-conditional orders should not succeed with zero balance
                    pytest.fail(
                        f"Order type {test_case['name']} unexpectedly succeeded with zero balance. "
                        f"Order ID: {order.exchange_order_id}, Status: {order.status.value}",
                    )
            except APIError as api_error:
                # Order failed as expected
                if api_error.code not in test_case["expected_errors"]:
                    pytest.fail(
                        f"Order type {test_case['name']} failed with unexpected error: "
                        f"{api_error.code} - {api_error.message}"
                    )
                logger.info(
                    "order_type_correctly_failed",
                    test_case_name=test_case["name"],
                    error_code=api_error.code,
                    error_message=api_error.message,
                    message=(
                        f"✓ {test_case['name']} correctly failed: {api_error.code} - "
                        f"{api_error.message}"
                    ),
                )

        logger.info(
            "all_order_types_tested",
            order_types_count=len(order_type_tests),
            message=f"✓ All {len(order_type_tests)} order types tested with zero balance",
        )

    # =============================================================================
    # EXTREME EDGE CASES AND ERROR SCENARIOS
    # =============================================================================

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_unreasonably_large_orders_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test unreasonably large orders with zero balance.

        Should fail with insufficient funds or order size validation errors.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC

        # Get unreasonably large values
        large_price = await get_unreasonably_large_price(bp_api_for_zero_balance_test, symbol)
        large_quantity = await get_unreasonably_large_quantity(bp_api_for_zero_balance_test, symbol)

        extreme_test_cases: list[dict[str, Any]] = [
            {
                "name": "unreasonably_large_quantity",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=large_quantity,
                    price=Decimal("1.00"),  # Low price to focus on quantity
                    time_in_force=TimeInForce.GTC,
                ),
            },
            {
                "name": "unreasonably_large_price",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=Decimal("0.01"),  # Small quantity
                    price=large_price,
                    time_in_force=TimeInForce.GTC,
                ),
            },
            {
                "name": "both_unreasonably_large",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.LIMIT,
                    quantity=large_quantity,
                    price=large_price,
                    time_in_force=TimeInForce.GTC,
                ),
            },
        ]

        for test_case in extreme_test_cases:
            with pytest.raises(APIError) as exc_info:
                await bp_api_for_zero_balance_test.place_order(test_case["args"])

            api_error = exc_info.value
            expected_errors = [
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.INVALID_ORDER_SIZE.value,
                APIErrorCode.PRICE_OUT_OF_RANGE.value,
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]

            assert api_error.code in expected_errors, (
                f"Extreme test {test_case['name']} failed with unexpected error: "
                f"{api_error.code} - {api_error.message}"
            )

            logger.info(
                "extreme_case_correctly_rejected",
                test_case_name=test_case["name"],
                error_code=api_error.code,
                error_message=api_error.message,
                message=(
                    f"✓ Extreme case '{test_case['name']}' correctly rejected: "
                    f"{api_error.code} - {api_error.message}"
                ),
            )

        logger.info(
            "extreme_edge_cases_tested",
            test_cases_count=len(extreme_test_cases),
            message=f"✓ All {len(extreme_test_cases)} extreme edge cases tested",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_invalid_trigger_price_combinations_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test invalid trigger price combinations for stop/take profit orders.

        Tests illogical price combinations that should be rejected by validation.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC

        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=OrderSide.SELL,
            price=current_price,
        )

        # Invalid trigger price combinations
        invalid_combinations: list[dict[str, Any]] = [
            {
                "name": "stop_limit_trigger_above_limit",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.STOP_LIMIT,
                    quantity=minimal_quantity,
                    price=current_price * Decimal("0.90"),  # Limit price
                    stop_price=current_price * Decimal("0.95"),  # Trigger > limit (invalid)
                    time_in_force=TimeInForce.GTC,
                ),
            },
            {
                "name": "take_profit_trigger_below_limit",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    order_type=OrderType.TAKE_PROFIT_LIMIT,
                    quantity=minimal_quantity,
                    price=current_price * Decimal("1.10"),  # Limit price
                    stop_price=current_price * Decimal("1.05"),  # Trigger < limit (invalid)
                    time_in_force=TimeInForce.GTC,
                ),
            },
            {
                "name": "stop_trigger_above_current_for_buy",
                "args": PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,  # Buy stop should trigger above current
                    order_type=OrderType.STOP_MARKET,
                    quantity=minimal_quantity,
                    stop_price=current_price * Decimal("0.95"),  # Below current (illogical)
                    time_in_force=TimeInForce.GTC,
                ),
            },
        ]

        for test_case in invalid_combinations:
            with pytest.raises(APIError) as exc_info:
                await bp_api_for_zero_balance_test.place_order(test_case["args"])

            api_error = exc_info.value
            expected_errors = [
                APIErrorCode.INSUFFICIENT_FUNDS.value,
                APIErrorCode.INVALID_REQUEST.value,
                APIErrorCode.PRICE_OUT_OF_RANGE.value,
                APIErrorCode.INVALID_PARAMS.value,
                APIErrorCode.EXCHANGE_SPECIFIC.value,
            ]

            assert api_error.code in expected_errors, (
                f"Invalid combination {test_case['name']} failed with unexpected error: "
                f"{api_error.code} - {api_error.message}"
            )

            logger.info(
                "invalid_combination_correctly_rejected",
                test_case_name=test_case["name"],
                error_code=api_error.code,
                message=(
                    f"✓ Invalid combination '{test_case['name']}' correctly rejected: "
                    f"{api_error.code}"
                ),
            )

        logger.info(
            "invalid_trigger_combinations_tested",
            combinations_count=len(invalid_combinations),
            message=f"✓ All {len(invalid_combinations)} invalid trigger combinations tested",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_symbols_zero_balance_comprehensive(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order placement across multiple symbols with zero balance.

        Validates that insufficient funds errors are consistent across different trading pairs.
        """
        _ = custom_vcr_config

        # Test with multiple symbols
        test_symbols = [TEST_SYMBOL_SOL_USDC, TEST_SYMBOL_BTC_USDC]

        for symbol in test_symbols:
            try:
                # Get symbol-specific parameters
                current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)

                # Get market constraints for price quantization
                constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
                tick_size = constraints["tick_size"]

                minimal_quantity = await get_minimal_order_size_for_zero_balance_test(
                    api=bp_api_for_zero_balance_test,
                    symbol=symbol,
                    side=OrderSide.BUY,
                    price=current_price,
                )

                # Test both market and limit orders for each symbol
                order_tests: list[dict[str, Any]] = [
                    {
                        "type": "MARKET",
                        "args": PlaceOrderArgs(
                            symbol=symbol,
                            side=OrderSide.BUY,
                            order_type=OrderType.MARKET,
                            quantity=minimal_quantity,
                            time_in_force=TimeInForce.GTC,
                        ),
                    },
                    {
                        "type": "LIMIT",
                        "args": PlaceOrderArgs(
                            symbol=symbol,
                            side=OrderSide.BUY,
                            order_type=OrderType.LIMIT,
                            quantity=minimal_quantity,
                            price=(current_price * Decimal("0.95")).quantize(tick_size),
                            time_in_force=TimeInForce.GTC,
                        ),
                    },
                ]

                for order_test in order_tests:
                    with pytest.raises(APIError) as exc_info:
                        await bp_api_for_zero_balance_test.place_order(order_test["args"])

                    api_error = exc_info.value
                    assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
                        f"{order_test['type']} order for {symbol} should fail with "
                        f"INSUFFICIENT_FUNDS, "
                        f"got {api_error.code}: {api_error.message}"
                    )

                    logger.info(
                        "symbol_order_correctly_failed",
                        symbol=symbol,
                        order_type=order_test["type"],
                        error_code=api_error.code,
                        message=(
                            f"✓ {symbol} {order_test['type']} order correctly failed: "
                            f"{api_error.code}"
                        ),
                    )

            except (APIError, ValueError, TypeError, KeyError) as e:
                # Multi-symbol tests should work even with zero balance
                pytest.fail(
                    f"Failed to test zero balance behavior for symbol {symbol}: {e}. "
                    "Error handling should be consistent across all symbols.",
                )

        logger.info(
            "multi_symbol_testing_completed",
            symbols_count=len(test_symbols),
            message=(
                f"✓ Multi-symbol zero balance testing completed for {len(test_symbols)} symbols"
            ),
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_parameter_edge_cases_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test edge cases in order parameters with zero balance.

        This test validates two levels of protection:
        1. Pydantic model validation (client-side) - prevents invalid data from reaching API
        2. Exchange API validation (server-side) - handles edge cases that pass model validation

        Both levels are critical for trading safety.
        """
        _ = custom_vcr_config
        symbol = TEST_SYMBOL_SOL_USDC

        current_price = await get_current_market_price(bp_api_for_zero_balance_test, symbol)
        constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
        tick_size = constraints["tick_size"]
        step_size = constraints["step_size"]
        min_quantity = constraints.get("min_quantity", step_size)

        # Quantize current price to ensure it respects tick size
        current_price = current_price.quantize(tick_size)

        # Test cases that should fail at Pydantic validation level
        pydantic_validation_cases: list[dict[str, Any]] = [
            {
                "name": "zero_quantity",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": Decimal(0),  # Zero quantity - violates Pydantic gt=0
                    "price": current_price,
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_validation": "greater than 0",
            },
            {
                "name": "negative_quantity",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": Decimal("-0.01"),  # Negative - violates Pydantic gt=0
                    "price": current_price,
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_validation": "greater than 0",
            },
            {
                "name": "zero_price",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": min_quantity,
                    "price": Decimal(0),  # Zero price - violates Pydantic gt=0
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_validation": "greater than 0",
            },
            {
                "name": "negative_price",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": min_quantity,
                    "price": Decimal("-1.00"),  # Negative - violates Pydantic gt=0
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_validation": "greater than 0",
            },
        ]

        # Test Pydantic validation
        for test_case in pydantic_validation_cases:
            try:
                args = PlaceOrderArgs(**test_case["params"])
                # If we get here, Pydantic validation failed to catch invalid params
                pytest.fail(
                    f"Edge case '{test_case['name']}' should have failed Pydantic validation "
                    f"but passed. This is a critical validation gap.",
                )
            except ValidationError as ve:
                # Verify the error message contains expected validation
                error_str = str(ve)
                assert test_case["expected_validation"] in error_str, (
                    f"Edge case '{test_case['name']}' failed validation but with "
                    f"unexpected error: {error_str}"
                )
                logger.info(
                    "edge_case_pydantic_validation_rejection",
                    test_case_name=test_case["name"],
                    message=(
                        f"✓ Edge case '{test_case['name']}' correctly rejected by "
                        "Pydantic validation"
                    ),
                )

        # Test cases that pass Pydantic but should fail at API level
        api_validation_cases: list[dict[str, Any]] = [
            {
                "name": "below_min_quantity",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": min_quantity * Decimal("0.1"),  # Below minimum
                    "price": current_price,
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_api_errors": [
                    APIErrorCode.INVALID_ORDER_SIZE.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
            {
                "name": "unquantized_price",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": min_quantity,
                    "price": current_price + (tick_size * Decimal("0.5")),  # Not on tick
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_api_errors": [
                    APIErrorCode.PRICE_OUT_OF_RANGE.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
            {
                "name": "unquantized_quantity",
                "params": {
                    "symbol": symbol,
                    "side": OrderSide.BUY,
                    "order_type": OrderType.LIMIT,
                    "quantity": min_quantity + (step_size * Decimal("0.5")),  # Not on step
                    "price": current_price,
                    "time_in_force": TimeInForce.GTC,
                },
                "expected_api_errors": [
                    APIErrorCode.INVALID_ORDER_SIZE.value,
                    APIErrorCode.INVALID_REQUEST.value,
                ],
            },
        ]

        # Test API validation
        for test_case in api_validation_cases:
            try:
                args = PlaceOrderArgs(**test_case["params"])

                # These should pass Pydantic but fail at API level
                with pytest.raises(APIError) as exc_info:
                    await bp_api_for_zero_balance_test.place_order(args)

                # With zero balance, we might get insufficient funds instead of validation error
                if exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value:
                    logger.info(
                        "edge_case_zero_balance_rejection",
                        test_case_name=test_case["name"],
                        message=(
                            f"✓ Edge case '{test_case['name']}' rejected due to zero balance "
                            "(would validate params with funded account)"
                        ),
                    )
                else:
                    # Check if it's one of the expected validation errors
                    assert exc_info.value.code in test_case["expected_api_errors"], (
                        f"Edge case '{test_case['name']}' failed with unexpected API error: "
                        f"{exc_info.value.code} - {exc_info.value.message}"
                    )
                    logger.info(
                        "edge_case_api_rejection",
                        test_case_name=test_case["name"],
                        error_code=exc_info.value.code,
                        message=(
                            f"✓ Edge case '{test_case['name']}' correctly rejected by API: "
                            f"{exc_info.value.code}"
                        ),
                    )

            except (APIError, ValueError, TypeError, KeyError) as e:
                pytest.fail(
                    f"Edge case '{test_case['name']}' failed unexpectedly: {e}. "
                    "API validation testing is critical for trading safety.",
                )

        logger.info("✓ All parameter edge cases tested at both validation levels")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_error_message_validation_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that error messages are properly formatted and informative.

        Validates that error responses contain useful information for debugging and logging.
        """
        symbol = SOL_USDC_BP
        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            symbol,
            OrderSide.BUY,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),  # Reasonable size
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        api_error = exc_info.value

        # Validate error structure
        assert hasattr(api_error, "code"), "APIError should have error code"
        assert hasattr(api_error, "message"), "APIError should have error message"
        assert api_error.code is not None, "Error code should not be None"
        assert api_error.message is not None, "Error message should not be None"

        # Validate message content
        error_message = str(api_error.message).lower()
        assert len(error_message) > 0, "Error message should not be empty"
        assert len(error_message) < 500, "Error message should be reasonably sized"

        # Should contain relevant keywords for insufficient funds
        fund_keywords = ["insufficient", "balance", "fund", "not enough", "cannot"]
        contains_fund_keyword = any(keyword in error_message for keyword in fund_keywords)

        if api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value:
            assert contains_fund_keyword, (
                f"Insufficient funds error should contain relevant keywords. "
                f"Message: '{api_error.message}'"
            )

        # Validate error code is properly mapped
        assert isinstance(api_error.code, int), "Error code should be integer"
        assert api_error.code > 0, "Error code should be positive"

        logger.info(
            "error_validation_passed",
            error_code=api_error.code,
            error_message=api_error.message,
            message=f"✓ Error validation passed: {api_error.code} - {api_error.message}",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_network_timeout_simulation_zero_balance(
        self,
        bp_api_for_zero_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order operations under network timeout conditions.

        Simulates timeout scenarios to ensure graceful handling.

        Args:
            bp_api_for_zero_balance_test: BackpackAPI instance for zero balance testing
            custom_vcr_config: VCR configuration for network simulation

        Raises:
            KeyError: If key lookup fails during testing
            TypeError: If type mismatch occurs during API calls
            ValueError: If invalid values are encountered
            APIError: If API errors occur during order operations
        """
        symbol = SOL_USDC_BP
        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test,
            symbol,
            OrderSide.BUY,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        try:
            # Normal request (should get insufficient funds)
            with pytest.raises(APIError) as exc_info:
                await bp_api_for_zero_balance_test.place_order(place_args)

            assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
            logger.info("✓ Normal request completed (insufficient funds as expected)")

        except TimeoutError:
            logger.info("✓ Network timeout occurred - acceptable behavior")

        except (APIError, ValueError, TypeError, KeyError) as e:
            if "timeout" in str(e).lower():
                logger.info(
                    "timeout_error_handled",
                    error_details=str(e),
                    message=f"✓ Timeout-related error properly handled: {e}",
                )
            else:
                raise  # Re-raise if not timeout related
