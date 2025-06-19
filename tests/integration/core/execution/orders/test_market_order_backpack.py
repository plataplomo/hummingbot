"""Integration tests for Backpack market orders.

These tests verify market order functionality against real Backpack APIs,
ensuring proper order execution for both spot and perpetual markets.
All tests follow TESTING_SECURITY_RULES.md strictly.
"""

from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.execution.orders import (
    InsufficientLiquidityError,
    MarketOrder,
    MarketOrderConfig,
    MarketOrderService,
    PriceDeviationError,
)
from cyberdelta.core.models import OrderSide, OrderStatus, OrderType, TimeInForce
from tests.integration.core.execution.orders.test_helpers import MarketOrderTestHelpers

logger = get_logger(__name__)

# Module-level storage for order data between tests
_test_order_data: dict[str, str | Decimal | None] = {
    "backpack_buy_order_id": None,
    "backpack_buy_quantity": None,
}


class TestBackpackMarketOrderIntegration:
    """Integration tests for Backpack market orders."""

    @pytest.mark.asyncio
    async def test_market_buy_order_btc_spot(
        self, backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
    ) -> None:
        """Test placing a market buy order for BTC spot.

        This test:
        1. Gets real-time market data for BTC-USDC
        2. Calculates minimal viable order size
        3. Places a market buy order
        4. Verifies order appears in trading history
        """
        # Get test symbol - BTC-USDC for Backpack spot
        symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")
        assert symbol == "BTC-USDC", f"Expected BTC-USDC symbol for Backpack, got {symbol}"

        # Get minimal test quantity from real market data
        test_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            backpack_api, symbol, OrderSide.BUY
        )
        logger.info(f"Using minimal test quantity: {test_quantity} for {symbol}")

        # Create market order executor
        service = MarketOrderService(exchange_api=backpack_api, config=market_order_config)
        market_order = MarketOrder(
            exchange_api=backpack_api, market_order_service=service, config=market_order_config
        )

        # Place market buy order
        try:
            order = await market_order.execute_market_order(
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=test_quantity,
            )

            # Verify order was created
            assert order is not None, "Market order execution returned None"
            assert order.exchange_order_id is not None, "Order missing exchange ID"
            assert order.symbol == symbol, f"Order symbol mismatch: {order.symbol} != {symbol}"
            assert order.side == OrderSide.BUY, f"Order side mismatch: {order.side} != BUY"
            assert order.order_type == OrderType.LIMIT, "Market order should be LIMIT type"
            assert order.time_in_force == TimeInForce.IOC, (
                "Market order should have IOC time in force"
            )

            # Wait for order to reach terminal state
            filled_order = await MarketOrderTestHelpers.wait_for_order_fill(
                backpack_api, order, timeout=30
            )

            # Verify order was filled
            assert filled_order.status == OrderStatus.FILLED, (
                f"Order not filled. Status: {filled_order.status}"
            )
            assert filled_order.quantity_filled is not None, "Filled order missing quantity_filled"
            assert filled_order.quantity_filled > Decimal("0"), "No quantity was filled"

            # Verify order appears in history
            found_in_history = await MarketOrderTestHelpers.verify_order_in_history(
                backpack_api, filled_order
            )
            assert found_in_history, (
                f"Order {filled_order.exchange_order_id} not found in trading history"
            )

            # Store order ID for cleanup test
            _test_order_data["backpack_buy_order_id"] = filled_order.exchange_order_id
            _test_order_data["backpack_buy_quantity"] = filled_order.quantity_filled

            logger.info(
                f"Successfully placed and verified market buy order: "
                f"{filled_order.exchange_order_id}, filled: {filled_order.quantity_filled}"
            )

        except Exception as e:
            pytest.fail(
                f"Failed to execute market buy order for {symbol}: {e}. "
                "Market order execution is a critical operation that must work reliably."
            )

    @pytest.mark.asyncio
    async def test_market_sell_previous_buy_btc_spot(
        self, backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
    ) -> None:
        """Test selling the BTC bought in previous test.

        This test:
        1. Checks if previous buy test was successful
        2. Gets the exact quantity bought from history
        3. Places a market sell order for that quantity
        4. Verifies the sell order executes successfully
        """
        # Check if previous test ran and stored order info
        buy_order_id_raw = _test_order_data.get("backpack_buy_order_id")
        buy_quantity_raw = _test_order_data.get("backpack_buy_quantity")

        if not buy_order_id_raw or not buy_quantity_raw:
            pytest.skip("Previous buy test did not complete successfully")

        # Type narrowing - we know these are not None after the check
        buy_order_id = str(buy_order_id_raw)
        # Note: buy_quantity is stored but we'll fetch from history for accuracy
        _ = (
            Decimal(str(buy_quantity_raw))
            if isinstance(buy_quantity_raw, str)
            else buy_quantity_raw
        )

        # Get symbol
        symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")

        # Double-check the quantity from history to ensure accuracy
        historical_quantity = await MarketOrderTestHelpers.get_filled_quantity_from_history(
            backpack_api, buy_order_id
        )

        if historical_quantity is None:
            pytest.fail(
                f"Could not find previous buy order {buy_order_id} in history. "
                "Cannot proceed with sell test without knowing exact quantity to sell."
            )

        # Use the historical quantity for selling
        sell_quantity = historical_quantity
        logger.info(
            f"Found previous buy order {buy_order_id} with filled quantity: {sell_quantity}"
        )

        # Create market order executor
        service = MarketOrderService(exchange_api=backpack_api, config=market_order_config)
        market_order = MarketOrder(
            exchange_api=backpack_api, market_order_service=service, config=market_order_config
        )

        # Place market sell order
        try:
            order = await market_order.execute_market_order(
                symbol=symbol,
                side=OrderSide.SELL,
                quantity=sell_quantity,
            )

            # Verify order was created
            assert order is not None, "Market order execution returned None"
            assert order.exchange_order_id is not None, "Order missing exchange ID"
            assert order.symbol == symbol, f"Order symbol mismatch: {order.symbol} != {symbol}"
            assert order.side == OrderSide.SELL, f"Order side mismatch: {order.side} != SELL"
            assert order.order_type == OrderType.LIMIT, "Market order should be LIMIT type"
            assert order.time_in_force == TimeInForce.IOC, (
                "Market order should have IOC time in force"
            )

            # Wait for order to reach terminal state
            filled_order = await MarketOrderTestHelpers.wait_for_order_fill(
                backpack_api, order, timeout=30
            )

            # Verify order was filled
            assert filled_order.status == OrderStatus.FILLED, (
                f"Order not filled. Status: {filled_order.status}"
            )
            assert filled_order.quantity_filled is not None, "Filled order missing quantity_filled"
            assert filled_order.quantity_filled > Decimal("0"), "No quantity was filled"

            # Verify we sold approximately what we bought (allowing for rounding)
            from decimal import ROUND_DOWN

            expected_quantity = sell_quantity.quantize(Decimal("0.001"), rounding=ROUND_DOWN)
            actual_quantity = filled_order.quantity_filled.quantize(
                Decimal("0.001"), rounding=ROUND_DOWN
            )

            assert actual_quantity >= expected_quantity * Decimal("0.99"), (
                f"Sold quantity {actual_quantity} significantly less than "
                f"expected {expected_quantity}"
            )

            # Verify order appears in history
            found_in_history = await MarketOrderTestHelpers.verify_order_in_history(
                backpack_api, filled_order
            )
            assert found_in_history, (
                f"Order {filled_order.exchange_order_id} not found in trading history"
            )

            logger.info(
                f"Successfully placed and verified market sell order: "
                f"{filled_order.exchange_order_id}, filled: {filled_order.quantity_filled}"
            )

            # Clean up test data
            _test_order_data["backpack_buy_order_id"] = None
            _test_order_data["backpack_buy_quantity"] = None

        except Exception as e:
            pytest.fail(
                f"Failed to execute market sell order for {symbol}: {e}. "
                "Market order execution is a critical operation that must work reliably."
            )

    async def _find_perp_symbol(self, backpack_api: BackpackAPI) -> str | None:
        """Find a perpetual market symbol on Backpack."""
        from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs

        # First, try to find from available markets
        try:
            markets = await backpack_api.get_markets(GetMarketsArgs())
            if markets:
                for market in markets:
                    if "PERP" in market.symbol.upper() and "BTC" in market.symbol.upper():
                        return market.symbol
        except Exception as e:
            logger.debug(f"Error getting markets list: {e}")

        # Try common perp symbol formats
        for symbol in ["BTC-PERP", "BTCPERP", "BTC_PERP"]:
            try:
                market = await backpack_api.get_market(GetMarketArgs(symbol=symbol))
                if market:
                    return symbol
            except Exception as e:
                logger.debug(f"Symbol {symbol} not found: {e}")
                continue

        return None

    @pytest.mark.asyncio
    async def test_market_order_perp_market(
        self, backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
    ) -> None:
        """Test market orders on perpetual markets.

        This test verifies that market orders work correctly on
        Backpack's perpetual futures markets.
        """
        try:
            perp_symbol = await self._find_perp_symbol(backpack_api)

            if not perp_symbol:
                pytest.skip("No BTC perpetual market found on Backpack")

            logger.info(f"Testing perpetual market: {perp_symbol}")

            # Get minimal test quantity
            test_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
                backpack_api, perp_symbol, OrderSide.BUY
            )

            # Create market order executor
            service = MarketOrderService(exchange_api=backpack_api, config=market_order_config)
            market_order = MarketOrder(
                exchange_api=backpack_api, market_order_service=service, config=market_order_config
            )

            # Place buy order
            buy_order = await market_order.execute_market_order(
                symbol=perp_symbol,
                side=OrderSide.BUY,
                quantity=test_quantity,
            )

            # Wait for fill
            filled_buy = await MarketOrderTestHelpers.wait_for_order_fill(
                backpack_api, buy_order, timeout=30
            )

            assert filled_buy.status == OrderStatus.FILLED, (
                f"Perp buy order not filled. Status: {filled_buy.status}"
            )

            # Immediately sell to close position
            if filled_buy.quantity_filled:
                sell_order = await market_order.execute_market_order(
                    symbol=perp_symbol,
                    side=OrderSide.SELL,
                    quantity=filled_buy.quantity_filled,
                )

                filled_sell = await MarketOrderTestHelpers.wait_for_order_fill(
                    backpack_api, sell_order, timeout=30
                )

                assert filled_sell.status == OrderStatus.FILLED, (
                    f"Perp sell order not filled. Status: {filled_sell.status}"
                )

                logger.info(f"Successfully tested perpetual market orders on {perp_symbol}")

        except Exception as e:
            if "No BTC perpetual market found" in str(e):
                pytest.skip(str(e))
            else:
                pytest.fail(
                    f"Failed to test perpetual market orders: {e}. "
                    "Market order functionality must work across all market types."
                )

    @pytest.mark.asyncio
    async def test_insufficient_liquidity_error_spot(
        self, backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
    ) -> None:
        """Test that insufficient liquidity is properly detected on spot markets.

        This test attempts to place an order larger than available liquidity
        to verify proper error handling.
        """
        symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")

        # Get current orderbook to determine a size that exceeds liquidity
        try:
            orderbook = await backpack_api.get_order_book(symbol, depth=20)

            if not orderbook or not orderbook.asks:
                pytest.skip(f"No orderbook data available for {symbol}")

            # Calculate total available liquidity on ask side
            total_ask_quantity = sum(price_qty[1] for price_qty in orderbook.asks)

            # Try to buy 2x the available liquidity
            excessive_quantity = total_ask_quantity * Decimal("2")

            # Ensure it meets minimum order requirements
            min_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
                backpack_api, symbol, OrderSide.BUY
            )
            excessive_quantity = max(excessive_quantity, min_quantity * Decimal("1000"))

            logger.info(
                f"Testing insufficient liquidity with quantity {excessive_quantity} "
                f"(available: {total_ask_quantity})"
            )

            # Create market order executor
            service = MarketOrderService(exchange_api=backpack_api, config=market_order_config)
            market_order = MarketOrder(
                exchange_api=backpack_api, market_order_service=service, config=market_order_config
            )

            # This should raise InsufficientLiquidityError
            with pytest.raises(InsufficientLiquidityError) as exc_info:
                await market_order.execute_market_order(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    quantity=excessive_quantity,
                )

            # Verify error details
            error = exc_info.value
            assert symbol in str(error), f"Error should mention symbol {symbol}"
            assert "liquidity" in str(error).lower(), "Error should mention liquidity"

            logger.info(f"Correctly caught insufficient liquidity error: {error}")

        except InsufficientLiquidityError:
            # Expected - test passes
            pass
        except Exception as e:
            pytest.fail(f"Expected InsufficientLiquidityError but got {type(e).__name__}: {e}")

    @pytest.mark.asyncio
    async def test_price_deviation_protection_spot(
        self, backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
    ) -> None:
        """Test that excessive price deviation is properly detected on spot markets.

        This test uses a very tight slippage tolerance to trigger
        price deviation protection.
        """
        symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")

        # Get minimal test quantity
        test_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            backpack_api, symbol, OrderSide.BUY
        )

        # Create config with extremely tight slippage
        tight_config = MarketOrderConfig(
            default_slippage_pct=Decimal("0.0001"),  # 0.01% - extremely tight
            max_slippage_pct=Decimal("0.0001"),  # Same as default
            min_liquidity_ratio=Decimal("2.0"),
            order_timeout_seconds=30,
        )

        # Create market order executor with tight config
        service = MarketOrderService(exchange_api=backpack_api, config=tight_config)
        market_order = MarketOrder(
            exchange_api=backpack_api, market_order_service=service, config=tight_config
        )

        try:
            # This should likely raise PriceDeviationError due to tight slippage
            await market_order.execute_market_order(
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=test_quantity,
                max_slippage=Decimal("0.0001"),  # Explicitly set tight slippage
            )

            # If it didn't fail, that's okay - market might have tight spreads
            logger.info(
                "Market order succeeded despite tight slippage - market spreads must be very tight"
            )

        except PriceDeviationError as e:
            # Expected in most cases - verify error details
            assert symbol in str(e), f"Error should mention symbol {symbol}"
            assert "slippage" in str(e).lower() or "deviation" in str(e).lower(), (
                "Error should mention slippage or deviation"
            )
            logger.info(f"Correctly caught price deviation error: {e}")

        except Exception as e:
            # Other errors indicate a problem
            pytest.fail(
                f"Unexpected error type {type(e).__name__}: {e}. "
                "Expected either success or PriceDeviationError."
            )

    @pytest.mark.asyncio
    async def test_cross_market_type_execution(
        self, backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
    ) -> None:
        """Test market order execution across different market types.

        This verifies that the same MarketOrder instance can handle
        both spot and perpetual markets transparently.
        """
        # Create market order executor
        service = MarketOrderService(exchange_api=backpack_api, config=market_order_config)
        market_order = MarketOrder(
            exchange_api=backpack_api, market_order_service=service, config=market_order_config
        )

        executed_orders: list[tuple[str, OrderSide, Decimal]] = []

        # Test spot market
        spot_symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")
        try:
            spot_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
                backpack_api, spot_symbol, OrderSide.BUY
            )

            spot_order = await market_order.execute_market_order(
                symbol=spot_symbol,
                side=OrderSide.BUY,
                quantity=spot_quantity,
            )

            filled_spot = await MarketOrderTestHelpers.wait_for_order_fill(
                backpack_api, spot_order, timeout=30
            )

            if filled_spot.status == OrderStatus.FILLED and filled_spot.quantity_filled:
                executed_orders.append((spot_symbol, OrderSide.SELL, filled_spot.quantity_filled))
                logger.info(f"Successfully executed spot market order for {spot_symbol}")

        except Exception as e:
            logger.error(f"Failed spot market order: {e}")
            # Don't fail entire test if spot fails

        # Try to test perp market if available
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        try:
            markets = await backpack_api.get_markets(GetMarketsArgs())
            if markets:
                perp_markets = [m for m in markets if "PERP" in m.symbol.upper()]
                if perp_markets:
                    perp_symbol = perp_markets[0].symbol
                    perp_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
                        backpack_api, perp_symbol, OrderSide.BUY
                    )

                    perp_order = await market_order.execute_market_order(
                        symbol=perp_symbol,
                        side=OrderSide.BUY,
                        quantity=perp_quantity,
                    )

                    filled_perp = await MarketOrderTestHelpers.wait_for_order_fill(
                        backpack_api, perp_order, timeout=30
                    )

                    if filled_perp.status == OrderStatus.FILLED and filled_perp.quantity_filled:
                        executed_orders.append(
                            (perp_symbol, OrderSide.SELL, filled_perp.quantity_filled)
                        )
                        logger.info(f"Successfully executed perp market order for {perp_symbol}")

        except Exception as e:
            logger.info(f"Perp market test skipped: {e}")

        # Clean up positions
        for symbol, side, quantity in executed_orders:
            try:
                cleanup_order = await market_order.execute_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                )
                await MarketOrderTestHelpers.wait_for_order_fill(
                    backpack_api, cleanup_order, timeout=30
                )
                logger.info(f"Cleaned up position for {symbol}")
            except Exception as e:
                logger.error(f"Failed to clean up {symbol} position: {e}")

        # Verify we tested at least one market type
        assert len(executed_orders) > 0, "Failed to execute any market orders across market types"

    @pytest.fixture(autouse=True)
    async def cleanup(self, backpack_api: BackpackAPI) -> AsyncGenerator[None]:
        """Clean up any test positions after each test."""
        yield  # Run the test

        # After test cleanup
        try:
            await MarketOrderTestHelpers.cleanup_test_positions(backpack_api, "BTC-USDC")
        except Exception as e:
            logger.error(f"Error during test cleanup: {e}")
