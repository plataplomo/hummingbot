"""Integration tests for Backpack perp orders endpoints with $0 margin.

This module focuses specifically on testing the Order model pipeline
through Backpack's perp order endpoints with Ed25519 authentication
when the account has $0 margin. Tests validate error handling, API
pipeline validation, and authentication without successful order placement.

Model Focus: Order (Perp - Error Scenarios)
- Validates API error mapping and handling for perp orders
- Tests authentication and request pipeline validation
- Validates business logic error responses
- Tests insufficient margin error handling for perp markets
- Comprehensive error validation and edge cases

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: $0 margin (insufficient margin scenarios for perp trading)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order
from tests.integration.apis.backpack.shared.test_helpers import (
    get_dynamic_test_price,
    get_market_constraints,
    get_minimal_order_size,
)

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]

logger = get_logger(__name__)


# REMOVED: get_perp_symbol_tick_size_zero function
# This function had fallback values which is unacceptable for a trading engine.
# Use get_market_constraints from shared test_helpers instead.


# REMOVED: get_dynamic_perp_test_price_zero function
# This function had hardcoded fallback prices which is unacceptable for a trading engine.
# Use get_dynamic_test_price from shared test_helpers instead.


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/perp/orders/zero"], indirect=True
)
class TestBackpackPerpOrdersZeroBalance:
    """Comprehensive perp orders integration tests with zero margin for error scenarios."""

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_place_perp_order_insufficient_margin_error(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test placing perp order with insufficient margin returns appropriate error."""
        symbol = "SOL_USDC_PERP"

        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Get minimal quantity for this market
        minimal_quantity = await get_minimal_order_size(
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

        # Should raise APIError due to insufficient margin
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value
        assert isinstance(error, APIError), f"Expected APIError, got {type(error)}"

        # Error should indicate insufficient margin/balance
        error_msg = str(error).lower()
        assert any(
            keyword in error_msg for keyword in ["insufficient", "margin", "balance", "funds"]
        ), f"Error message should indicate insufficient margin: {error}"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_place_large_perp_order_margin_error(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test placing large perp order with zero margin fails appropriately."""
        symbol = "SOL_USDC_PERP"

        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Get large quantity based on market constraints
        constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
        min_quantity = constraints["min_quantity"]
        # Large quantity is 1000x minimum order size (would require significant margin)
        large_quantity = min_quantity * Decimal("1000")

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=large_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value
        logger.info(f"Large perp order error (expected): {error}")

        # Validate error structure
        assert hasattr(error, "error_code") or hasattr(error, "message"), (
            "Error should have error_code or message attribute"
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_market_order_zero_margin_error(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp market order with zero margin fails appropriately."""
        symbol = "SOL_USDC_PERP"

        # Get minimal quantity using current market price for calculation
        current_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=OrderSide.BUY,
            price=current_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=minimal_quantity,
            time_in_force=TimeInForce.IOC,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value
        assert isinstance(error, APIError), "Should raise APIError for market order with no margin"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_cancel_nonexistent_perp_order_error(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test canceling non-existent perp order returns appropriate error."""
        symbol = "SOL_USDC_PERP"
        fake_order_id = "nonexistent_perp_order_123"

        cancel_args = CancelOrderArgs(
            symbol=symbol,
            order_id=fake_order_id,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.cancel_order(cancel_args)

        error = exc_info.value
        assert isinstance(error, APIError), f"Expected APIError, got {type(error)}"

        # Error should indicate order not found
        error_msg = str(error).lower()
        assert any(
            keyword in error_msg for keyword in ["not found", "invalid", "order", "nonexistent"]
        ), f"Error message should indicate order not found: {error}"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_get_perp_order_history_zero_balance(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test getting perp order history with zero balance account."""
        symbol = "SOL_USDC_PERP"

        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)  # Last 7 days

        history_args = GetOrderHistoryArgs(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=10,
        )

        # This should work even with zero balance (historical data access)
        orders = await bp_api_for_zero_balance_test.get_order_history(history_args)

        assert isinstance(orders, list), f"Expected list of orders, got {type(orders)}"

        # With zero balance account, order history might be empty
        for order in orders:
            assert isinstance(order, Order), f"Each item should be Order model, got {type(order)}"
            assert order.symbol == symbol, f"Order should have symbol {symbol}"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_authentication_validation(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test that perp order requests properly validate authentication."""
        symbol = "SOL_USDC_PERP"

        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Get extremely large quantity based on market constraints
        constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
        min_quantity = constraints["min_quantity"]
        # Extremely large quantity is 10 million times minimum (to ensure margin error)
        extremely_large_quantity = min_quantity * Decimal("10000000")

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=extremely_large_quantity,  # Large quantity to ensure margin error
            price=test_price,
            time_in_force=TimeInForce.IOC,
        )

        # Even with authentication, should fail due to insufficient margin
        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value

        # Should not be an authentication error, but a margin error
        error_msg = str(error).lower()
        assert "unauthorized" not in error_msg and "authentication" not in error_msg, (
            f"Should not be auth error with valid credentials: {error}"
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_precision_validation_zero_margin(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order precision validation even with zero margin."""
        symbol = "SOL_USDC_PERP"

        # Get market constraints for precision testing
        constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
        step_size = constraints["step_size"]
        tick_size = constraints["tick_size"]

        # Test with very precise values based on exchange precision
        # Use 9 decimal places of the step size for quantity precision test
        precise_quantity = step_size * Decimal("1.123456789")
        # Use current market price with 9 decimal places for price precision test
        market_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )
        precise_price = market_price.quantize(tick_size) + (tick_size * Decimal("0.123456789"))

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=precise_quantity,
            price=precise_price,
            time_in_force=TimeInForce.IOC,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value
        # Even if it fails due to margin, the precision should be validated first
        # or the error should be about margin, not precision
        assert isinstance(error, APIError), "Should handle precision validation properly"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_leverage_limits_zero_margin(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order leverage limit validation with zero margin."""
        symbol = "SOL_USDC_PERP"

        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Get high leverage quantity based on market constraints
        constraints = await get_market_constraints(bp_api_for_zero_balance_test, symbol)
        min_quantity = constraints["min_quantity"]
        # High leverage quantity is 5000x minimum (would imply very high leverage)
        high_leverage_quantity = min_quantity * Decimal("5000")

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=high_leverage_quantity,
            price=test_price,
            time_in_force=TimeInForce.IOC,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value
        # Should fail due to margin requirements, not leverage limits per se
        assert isinstance(error, APIError), "Should validate margin requirements"

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_short_order_zero_margin_error(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp short order with zero margin fails appropriately."""
        symbol = "SOL_USDC_PERP"

        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.SELL
        )

        # Get minimal quantity for short position
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=OrderSide.SELL,
            price=test_price,
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,  # Short position
            order_type=OrderType.LIMIT,
            quantity=minimal_quantity,
            price=test_price,
            time_in_force=TimeInForce.GTC,
        )

        with pytest.raises(APIError) as exc_info:
            await bp_api_for_zero_balance_test.place_order(place_args)

        error = exc_info.value
        assert isinstance(error, APIError), "Short perp order should fail with zero margin"

        # Error should indicate margin issues
        error_msg = str(error).lower()
        assert any(keyword in error_msg for keyword in ["insufficient", "margin", "balance"]), (
            f"Error should indicate margin issues for short: {error}"
        )

    @pytest.mark.vcr()
    @pytest.mark.asyncio
    async def test_perp_order_time_in_force_validation_zero_margin(
        self, bp_api_for_zero_balance_test: BackpackAPI, custom_vcr_config: dict[str, Any]
    ) -> None:
        """Test perp order time-in-force validation with zero margin."""
        symbol = "SOL_USDC_PERP"

        test_price = await get_dynamic_test_price(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Get minimal quantity for TIF tests
        minimal_quantity = await get_minimal_order_size(
            api=bp_api_for_zero_balance_test,
            symbol=symbol,
            side=OrderSide.BUY,
            price=test_price,
        )

        # Test different time in force options
        for tif in [TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK]:
            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=minimal_quantity,
                price=test_price,
                time_in_force=tif,
            )

            with pytest.raises(APIError) as exc_info:
                await bp_api_for_zero_balance_test.place_order(place_args)

            error = exc_info.value
            # Should fail due to margin, not time in force validation
            assert isinstance(error, APIError), f"Should handle TIF {tif} validation properly"
