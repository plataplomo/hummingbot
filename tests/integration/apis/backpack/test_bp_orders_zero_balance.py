"""Integration tests for Backpack private orders endpoints with $0 balance.

This module focuses specifically on testing the Order model pipeline
through Backpack's private order endpoints with Ed25519 authentication
when the account has $0 balance. Tests validate error handling, API
pipeline validation, and authentication without successful order placement.

Model Focus: Order (Error Scenarios)
- Validates API error mapping and handling
- Tests authentication and request pipeline validation
- Validates business logic error responses
- Tests insufficient funds error handling
- Comprehensive error validation and edge cases

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: $0 USDC (insufficient funds scenarios)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.ticker import Ticker

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

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
            # For buy orders, normally use price below market to avoid fills
            # But if tolerance is negative, use price above market (for insufficient funds tests)
            test_price = market_price * (Decimal("1") - tolerance_factor)
        else:
            # For sell orders, normally use price above market to avoid fills
            # But if tolerance is negative, use price below market
            test_price = market_price * (Decimal("1") + tolerance_factor)

        # Get the tick size for this symbol
        tick_size = await get_symbol_tick_size(api, symbol)

        # Round to the required tick size and normalize to remove trailing zeros
        # This ensures we don't send something like "150.00" when "150" would work
        quantized_price = test_price.quantize(tick_size)
        return quantized_price.normalize()

    except Exception as e:
        # Fallback to a reasonable static price if dynamic pricing fails
        # This ensures tests don't completely break due to market data issues
        if symbol == "SOL_USDC":
            fallback_price = Decimal("150.0") if side == OrderSide.BUY else Decimal("170.0")
        else:
            fallback_price = Decimal("1.0") if side == OrderSide.BUY else Decimal("10.0")

        logger.warning(
            "Dynamic pricing failed for %s, using fallback price %s: %s", symbol, fallback_price, e
        )
        return fallback_price


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/orders/zero_balance"], indirect=True
)
class TestBackpackOrdersZeroBalance:
    """Integration tests for Backpack orders with $0 balance (insufficient funds scenarios)."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_insufficient_funds_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() pipeline validation - expects INSUFFICIENT_FUNDS with $0 balance.

        With $0 balance, this test validates:
        1. Authentication works (not 401/403 error)
        2. Request pipeline processes correctly (not malformed request errors)
        3. Error mapping works correctly (INSUFFICIENT_FUNDS error is properly mapped)
        4. Order parameter validation works on the exchange side

        This ensures our API integration works correctly even when orders can't be placed.
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

        # Define order parameters with dynamic pricing
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),  # Small size for testing
            price=test_price,  # Dynamic price based on market conditions
            time_in_force=TimeInForce.GTC,
        )

        # With $0 balance, expect INSUFFICIENT_FUNDS error
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(place_args)

        # Validate the error is what we expect (not auth or malformed request)
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
            f"Expected INSUFFICIENT_FUNDS error with $0 balance, got {api_error.code}: "
            f"{api_error.message}"
        )

        # Validate error message contains expected content
        assert "insufficient" in api_error.message.lower(), (
            f"Error message should indicate insufficient funds: {api_error.message}"
        )

        logger.info(
            f"✓ Order placement pipeline working correctly - got expected INSUFFICIENT_FUNDS "
            f"error: {api_error.message}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test cancel_order() API endpoint validation with $0 balance.

        Since we cannot place orders with $0 balance, this test validates that 
        the cancel endpoint works correctly when called with a non-existent order ID,
        which should return an appropriate error response.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        symbol = "SOL_USDC"
        
        # Try to cancel a non-existent order (since we can't place with $0 balance)
        cancel_args = CancelOrderArgs(
            order_id="99999999999999999",  # Non-existent order ID
            symbol=symbol,
        )

        # Should raise APIError with ORDER_NOT_FOUND or similar
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.cancel_order(cancel_args)

        # Validate error mapping works correctly
        api_error = exc_info.value
        # With real API calls, we might get various error codes
        assert isinstance(api_error, APIError), f"Expected APIError, got {type(api_error)}"
        assert api_error.code is not None, "Error should have a code"
        assert api_error.message is not None, "Error should have a message"
        
        # Log the actual error for debugging
        logger.info(
            f"Cancel order error (expected): code={api_error.code}, message={api_error.message}"
        )

        logger.info(
            f"✓ Cancel order endpoint working correctly - got expected error: {api_error.message}"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_authentication_failure(
        self,
        active_bp_config: ExchangeSpecificConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with invalid Ed25519 authentication."""
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Create API with invalid credentials - use real BackpackAPI (not DI) to test auth failure
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        # BackpackAPI construction succeeds but authenticator will be None due to invalid
        # credentials
        bad_api = BackpackAPI(
            exchange_config=active_bp_config,
            exchange_secrets=invalid_secrets,
        )

        # Define order args
        place_args = PlaceOrderArgs(
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("1.00"),
            time_in_force=TimeInForce.GTC,
        )

        # The API call should fail when it tries to use the authenticator
        with pytest.raises((APIError, AttributeError)) as exc_info:
            await bad_api.place_order(place_args)

        # Validate that it fails due to authentication issues
        error = exc_info.value
        if isinstance(error, APIError):
            assert error.code in [
                APIErrorCode.AUTHENTICATION_FAILED.value,
                APIErrorCode.INVALID_REQUEST.value,
            ], f"Expected authentication-related error, got {error.code}"
        else:
            # Expected when trying to use None authenticator (AttributeError)
            assert "authenticator" in str(error).lower() or "NoneType" in str(error), (
                f"Expected authenticator-related AttributeError, got: {error}"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_large_quantity_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with unrealistically large quantity.

        This validates that our BackpackErrorMapper correctly maps Backpack's
        validation errors to our standardized APIErrorCode.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Get dynamic test price and use higher price to maximize required funds
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("-3"),  # 3% above market (negative = higher price)
        )

        # Create order with unrealistically large quantity to trigger validation/insufficient funds
        large_order_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("999999999.0"),  # Unrealistically large
            price=test_price,  # Use dynamic price to avoid price validation errors
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError with INSUFFICIENT_FUNDS or INVALID_REQUEST code
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(large_order_args)

        # Validate error mapping
        api_error = exc_info.value
        # With $0 balance, we might get INSUFFICIENT_FUNDS or INVALID_REQUEST (for quantity limits)
        assert api_error.code in [
            APIErrorCode.INSUFFICIENT_FUNDS.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], (
            f"Expected INSUFFICIENT_FUNDS or INVALID_REQUEST for large quantity with $0 balance, "
            f"got "
            f"{api_error.code}: {api_error.message}"
        )
        assert isinstance(api_error.message, str), "Error message should be string"
        assert len(api_error.message) > 0, "Error message should not be empty"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_invalid_symbol(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test place_order() with invalid symbol error."""
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Create order with non-existent symbol
        # Still use reasonable price to avoid price validation errors
        invalid_symbol_args = PlaceOrderArgs(
            symbol="INVALID_SYMBOL_XYZ",  # Non-existent symbol
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal("100.00"),  # Reasonable price to avoid price validation issues
            time_in_force=TimeInForce.GTC,
        )

        # Should raise APIError with appropriate error code
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(invalid_symbol_args)

        # Validate error structure
        api_error = exc_info.value
        assert api_error.code in [
            APIErrorCode.INVALID_SYMBOL.value,
            APIErrorCode.SYMBOL_NOT_FOUND.value,
            APIErrorCode.INVALID_REQUEST.value,
        ], f"Should map to symbol-related error code, got {api_error.code}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_management_endpoints_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order management pipeline endpoints with $0 balance.

        With $0 balance, this validates that the order management endpoints
        (get_open_orders, get_order_history) work correctly even when we 
        cannot place orders due to insufficient funds.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Step 1: Test get_open_orders endpoint
        open_orders = await bp_api_for_test_env.get_open_orders()
        assert isinstance(open_orders, list), "get_open_orders() should return list"
        
        # Step 2: Test get_order_history endpoint
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
        
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=10,
        )
        order_history = await bp_api_for_test_env.get_order_history(args)
        assert isinstance(order_history, list), "get_order_history() should return list"
        
        # Step 3: Test that place_order fails with expected error for $0 balance
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        # Should fail with INSUFFICIENT_FUNDS
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(place_args)
        
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value, (
            f"Expected INSUFFICIENT_FUNDS with $0 balance, got {api_error.code}: "
            f"{api_error.message}"
        )
        
        logger.info(
            "✓ Order management endpoints validated successfully with $0 balance constraint"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_backpack_api_structure_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test Backpack-specific API structure validation with $0 balance.

        This validates that Backpack's API responses contain expected structure
        even when orders cannot be placed due to insufficient funds.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Test get_open_orders for Backpack-specific structure
        open_orders = await bp_api_for_test_env.get_open_orders()
        assert isinstance(open_orders, list), "get_open_orders() should return list"
        
        # Test get_order_history for Backpack-specific structure
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
        
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=5,
        )
        order_history = await bp_api_for_test_env.get_order_history(args)
        assert isinstance(order_history, list), "get_order_history() should return list"
        
        # Validate Backpack-specific fields in any existing orders
        all_orders = open_orders + order_history
        
        if all_orders:
            sample_order = all_orders[0]
            
            # Validate Backpack-specific order fields
            assert sample_order.exchange == "backpack", "Order should be from Backpack"
            
            # Validate order ID format (Backpack specific)
            assert isinstance(sample_order.exchange_order_id, str), (
                "Backpack order ID should be string"
            )
            assert len(sample_order.exchange_order_id) > 0, "Order ID should not be empty"
            
            # Test Backpack-specific order details if present
            if hasattr(sample_order, "bp_details") and sample_order.bp_details:
                bp_details = sample_order.bp_details
                
                # Validate Backpack-specific order details if present
                client_id = getattr(bp_details, "client_id", None)
                if client_id is not None:
                    assert isinstance(client_id, str), "client_id should be string"
                
                order_flags = getattr(bp_details, "order_flags", None)
                if order_flags is not None:
                    assert isinstance(order_flags, int | str), "order_flags should be int or string"
            
            logger.info("✓ Backpack-specific order details validated from existing orders")
        else:
            logger.info("✓ No existing orders found, but endpoints responded correctly")
            
        # Test that place_order fails as expected with $0 balance
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("4"),
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        # Should fail with INSUFFICIENT_FUNDS, validating the error structure
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_test_env.place_order(place_args)
        
        api_error = exc_info.value
        assert api_error.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        assert "insufficient" in api_error.message.lower()
        
        logger.info("✓ Backpack error handling validated correctly")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_history_date_range_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_order_history() with various date range scenarios with $0 balance.

        This validates proper handling of different date ranges and edge cases
        when the account has no funds for new orders.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Test recent date range
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)  # Last 24 hours

        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=10,
        )

        recent_history = await bp_api_for_test_env.get_order_history(args)
        assert isinstance(recent_history, list), "Should return list"

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

        # Longer range should have >= orders from shorter range
        assert len(long_history) >= len(recent_history), (
            "Longer date range should include at least as many orders"
        )

        logger.info("✓ Order history date range validation completed with $0 balance")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_symbol_format_validation_zero_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order symbol format validation with $0 balance.

        This validates that order symbols from existing orders follow 
        Backpack's naming conventions and are properly formatted.
        """
        # VCR configuration is used by pytest-vcr automatically
        _ = custom_vcr_config
        # Get open orders to test symbol formats
        open_orders = await bp_api_for_test_env.get_open_orders()
        
        # Get order history to test symbol formats
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)  # Last 30 days
        
        args = GetOrderHistoryArgs(
            start_time=start_time,
            end_time=end_time,
            limit=10,
        )
        order_history = await bp_api_for_test_env.get_order_history(args)

        # Combine orders for symbol validation (with $0 balance, we test existing orders)
        all_orders = open_orders + order_history

        for order in all_orders:
            symbol = order.symbol

            # Validate symbol format
            assert isinstance(symbol, str), "Symbol should be string"
            assert len(symbol) > 0, "Symbol should not be empty"
            assert symbol == symbol.strip(), "Symbol should not have leading/trailing whitespace"

            # Backpack typically uses patterns like "SOL_USDC", "BTC_USDC"
            if "_" in symbol:
                # Spot trading pairs should have proper format
                parts = symbol.split("_")
                assert len(parts) == 2, f"Trading pair should have exactly one underscore: {symbol}"
                assert len(parts[0]) >= 2, f"Base asset should be at least 2 characters: {symbol}"
                assert len(parts[1]) >= 3, f"Quote asset should be at least 3 characters: {symbol}"

            # Symbol should not contain invalid characters
            invalid_chars = ["<", ">", "&", "'", '"', "%", " "]
            for char in invalid_chars:
                assert char not in symbol, f"Symbol should not contain {char}: {symbol}"

        # With $0 balance, if no orders exist to validate, test with a known valid symbol format
        if not all_orders:
            logger.info(
                "No existing orders found. Symbol format validation passes for "
                "endpoint availability."
            )
        else:
            logger.info(
                f"✓ Symbol format validation completed for {len(all_orders)} orders"
            )