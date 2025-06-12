"""Integration tests for Backpack endpoints that require positive account balance.

IMPORTANT: These tests require a Backpack account with positive balance (USDC/SOL).
They will FAIL if run on an account with $0 balance, which is expected behavior.

This module should only be run in environments where:
- The test account has sufficient funds for order placement
- Real money testing is acceptable and approved
- Risk controls are in place

Model Focus: Full Order Lifecycle, Account Operations
- Order placement, cancellation, and history retrieval
- Account summary with positive equity scenarios
- Position management tests
- Balance operations requiring funds

Authentication: Ed25519 signing for API authentication
VCR: Records both success and error responses with sensitive data filtering
Balance: REQUIRES positive balance for successful operations
"""

from __future__ import annotations

from datetime import datetime
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
from cyberdelta.core.models.margin_account import MarginAccountSummary

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

logger = get_logger(__name__)


async def get_symbol_tick_size(api: BackpackAPI, symbol: str) -> Decimal:
    """Get the tick size (price precision) for a symbol using public API."""
    try:
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs
        markets = await api.get_markets(GetMarketsArgs())

        for market in markets:
            if market.symbol == symbol:
                return market.tick_size

        logger.warning(f"Symbol {symbol} not found in markets, using default tick size")
        return Decimal("0.01")

    except Exception as e:
        logger.warning(f"Failed to get tick size for {symbol}: {e}, using default")
        return Decimal("0.01")


async def get_dynamic_test_price(
    api: BackpackAPI, symbol: str, side: OrderSide, tolerance_percent: Decimal = Decimal("5")
) -> Decimal:
    """Get a dynamic test price based on current market conditions."""
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
            raise ValueError(f"Unable to determine market price for {symbol}")

        tolerance_factor = tolerance_percent / Decimal("100")

        if side == OrderSide.BUY:
            test_price = market_price * (Decimal("1") - tolerance_factor)
        else:
            test_price = market_price * (Decimal("1") + tolerance_factor)

        tick_size = await get_symbol_tick_size(api, symbol)
        quantized_price = test_price.quantize(tick_size)
        return quantized_price.normalize()

    except Exception as e:
        if symbol == "SOL_USDC":
            fallback_price = Decimal("150.0") if side == OrderSide.BUY else Decimal("170.0")
        else:
            fallback_price = Decimal("1.0") if side == OrderSide.BUY else Decimal("10.0")

        logger.warning(
            "Dynamic pricing failed for %s, using fallback price %s: %s", symbol, fallback_price, e
        )
        return fallback_price


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/positive_balance"], indirect=True
)
class TestBackpackPositiveBalance:
    """Tests that require positive account balance - will fail with $0 balance accounts."""

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
            return {
                "step_size": Decimal("0.001"),
                "tick_size": Decimal("0.01"), 
                "min_quantity": Decimal("0.001"),
            }

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_place_order_success_with_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful order placement when account has sufficient balance.
        
        This test REQUIRES positive USDC balance and will fail with $0 balance.
        """
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("3"),
        )

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.001"),  # Minimal quantity
            price=test_price,
            time_in_force=TimeInForce.IOC,  # Immediate or Cancel
        )

        # This should succeed with positive balance
        placed_order = await bp_api_for_test_env.place_order(place_args)

        assert isinstance(placed_order, Order)
        assert placed_order.symbol == symbol
        assert placed_order.side == side
        assert placed_order.order_type == OrderType.LIMIT
        assert placed_order.quantity_requested == Decimal("0.001")
        assert placed_order.price == test_price

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_account_summary_with_positive_equity(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test account summary when account has positive equity.
        
        This test validates margin account behavior with actual funds.
        """
        account_summary = await bp_api_for_test_env.get_account_summary()

        assert isinstance(account_summary, MarginAccountSummary)
        assert account_summary.total_equity > Decimal("0"), (
            "This test requires positive account equity"
        )
        assert account_summary.available_equity >= Decimal("0")
        assert account_summary.total_equity >= account_summary.available_equity

        # Validate equity relationships make sense for funded account
        if account_summary.total_initial_margin_required is not None:
            assert account_summary.total_initial_margin_required >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_order_lifecycle_with_funds(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test complete order lifecycle: place -> cancel -> history.
        
        This requires sufficient balance for order placement.
        """
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=side,
            tolerance_percent=Decimal("5"),
        )

        # Step 1: Place order
        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.001"),
            price=test_price,
            time_in_force=TimeInForce.GTC,  # Good Till Cancel
        )

        placed_order = await bp_api_for_test_env.place_order(place_args)
        exchange_order_id = placed_order.exchange_order_id

        # Step 2: Cancel order (if still open)
        try:
            if exchange_order_id is not None:
                cancel_args = CancelOrderArgs(symbol=symbol, order_id=exchange_order_id)
                cancel_success = await bp_api_for_test_env.cancel_order(cancel_args)
                
                # cancel_order returns bool indicating success
                assert isinstance(cancel_success, bool)
                logger.info(f"Cancel order result: {cancel_success}")
        except Exception as e:
            # Order might have already filled or expired
            logger.info(f"Cancel failed (expected for filled/expired orders): {e}")

        # Step 3: Verify in history
        history_args = GetOrderHistoryArgs(
            symbol=symbol,
            limit=10,
        )
        order_history = await bp_api_for_test_env.get_order_history(history_args)
        
        # Should find our order in history
        order_ids_in_history = [order.exchange_order_id for order in order_history if order.exchange_order_id is not None]
        assert exchange_order_id in order_ids_in_history, (
            f"Order {exchange_order_id} should appear in order history"
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_orders_with_balance(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing multiple orders when account has sufficient balance."""
        symbol = "SOL_USDC"
        
        # Place multiple small orders
        order_ids = []
        for i in range(2):  # Keep it small for testing
            test_price = await get_dynamic_test_price(
                api=bp_api_for_test_env,
                symbol=symbol,
                side=OrderSide.BUY,
                tolerance_percent=Decimal("5") + Decimal(str(i)),  # Slightly different prices
            )

            place_args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.001"),
                price=test_price,
                time_in_force=TimeInForce.IOC,  # Immediate or Cancel to avoid lingering orders
            )

            try:
                placed_order = await bp_api_for_test_env.place_order(place_args)
                order_ids.append(placed_order.order_id)
            except Exception as e:
                logger.info(f"Order {i} failed (acceptable for testing): {e}")

        # Should have placed at least one order successfully
        assert len(order_ids) >= 1, "Should successfully place at least one order with positive balance"

    @pytest.mark.vcr 
    @pytest.mark.asyncio
    async def test_precision_validation_with_funds(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test order precision validation with actual balance for execution."""
        symbol = "SOL_USDC"
        constraints = await self._get_market_constraints(bp_api_for_test_env, symbol)
        
        test_price = await get_dynamic_test_price(
            api=bp_api_for_test_env,
            symbol=symbol,
            side=OrderSide.BUY,
            tolerance_percent=Decimal("3"),
        )

        # Test with proper precision (should succeed)
        proper_quantity = constraints["min_quantity"]
        proper_price = test_price.quantize(constraints["tick_size"])

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=proper_quantity,
            price=proper_price,
            time_in_force=TimeInForce.IOC,
        )

        # Should succeed with positive balance and proper precision
        placed_order = await bp_api_for_test_env.place_order(place_args)
        assert placed_order.quantity == proper_quantity
        assert placed_order.price == proper_price 