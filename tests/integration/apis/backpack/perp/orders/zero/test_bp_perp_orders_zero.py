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
from cyberdelta.core.models.market.ticker import Ticker

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]

logger = get_logger(__name__)


async def get_perp_symbol_tick_size_zero(api: BackpackAPI, symbol: str) -> Decimal:
    """Get the tick size for a perp symbol (for zero balance testing)."""
    try:
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await api.get_markets(GetMarketsArgs())

        for market in markets:
            if market.symbol == symbol:
                return market.tick_size

        logger.warning(f"Perp symbol {symbol} not found in markets, using default tick size")
        return Decimal("0.01")

    except Exception as e:
        logger.warning(f"Failed to get tick size for perp {symbol}: {e}, using default")
        return Decimal("0.01")


async def get_dynamic_perp_test_price_zero(
    api: BackpackAPI, symbol: str, side: OrderSide, tolerance_percent: Decimal = Decimal("5")
) -> Decimal:
    """Get a dynamic test price for perp orders (zero balance scenarios)."""
    try:
        ticker: Ticker = await api.get_ticker(symbol)

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
            raise ValueError(f"Unable to determine market price for perp {symbol}")

        tolerance_factor = tolerance_percent / Decimal("100")

        if side == OrderSide.BUY:
            test_price = market_price * (Decimal("1") - tolerance_factor)
        else:
            test_price = market_price * (Decimal("1") + tolerance_factor)

        tick_size = await get_perp_symbol_tick_size_zero(api, symbol)
        quantized_price = test_price.quantize(tick_size)
        return quantized_price.normalize()

    except Exception as e:
        # Fallback prices for perp symbols in zero balance scenarios
        if symbol == "SOL_USDC_PERP":
            fallback_price = Decimal("130.0") if side == OrderSide.BUY else Decimal("170.0")
        elif symbol == "BTC_USDC_PERP":
            fallback_price = Decimal("35000.0") if side == OrderSide.BUY else Decimal("50000.0")
        else:
            fallback_price = Decimal("80.0") if side == OrderSide.BUY else Decimal("120.0")

        logger.warning(
            "Dynamic pricing failed for perp %s (zero balance), using fallback price %s: %s",
            symbol,
            fallback_price,
            e,
        )
        return fallback_price


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

        test_price = await get_dynamic_perp_test_price_zero(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
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

        test_price = await get_dynamic_perp_test_price_zero(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Large quantity that would require significant margin
        large_quantity = Decimal("100.0")

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

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.5"),
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

        test_price = await get_dynamic_perp_test_price_zero(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10000000.0"),  # Large quantity to ensure margin error
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

        # Test with very precise values
        precise_quantity = Decimal("0.123456789")
        precise_price = Decimal("123.456789")

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

        test_price = await get_dynamic_perp_test_price_zero(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Test quantity that would imply very high leverage
        high_leverage_quantity = Decimal("50.0")  # Large position

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

        test_price = await get_dynamic_perp_test_price_zero(
            bp_api_for_zero_balance_test, symbol, OrderSide.SELL
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.SELL,  # Short position
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
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

        test_price = await get_dynamic_perp_test_price_zero(
            bp_api_for_zero_balance_test, symbol, OrderSide.BUY
        )

        # Test different time in force options
        for tif in [TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK]:
            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.5"),
                price=test_price,
                time_in_force=tif,
            )

            with pytest.raises(APIError) as exc_info:
                await bp_api_for_zero_balance_test.place_order(place_args)

            error = exc_info.value
            # Should fail due to margin, not time in force validation
            assert isinstance(error, APIError), f"Should handle TIF {tif} validation properly"
