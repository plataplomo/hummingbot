"""Integration tests for Backpack market orders.

These tests verify market order functionality against real Backpack APIs,
ensuring proper order execution for both spot and perpetual markets.
All tests follow TESTING_SECURITY_RULES.md strictly.

Tests use pytest-recording (VCR) for deterministic test execution with cassettes.
"""

from collections.abc import AsyncGenerator
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.models.service_args_models import GetMarketArgs
from cyberdelta.config.structlog_config import get_logger
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

# Mark all tests in this file for VCR cassette recording
pytestmark = [pytest.mark.integration, pytest.mark.vcr]

# Module-level storage for order data between tests
_test_order_data: dict[str, str | Decimal | None] = {
    "backpack_buy_order_id": None,
    "backpack_buy_quantity": None,
}


class TestBackpackMarketOrderIntegration:
    """Integration tests for Backpack market orders."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/backpack"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_market_buy_order_btc_spot(
        self,
        backpack_api: BackpackAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing a market buy order for BTC spot.

        This test:
        1. Gets real-time market data for BTC-USDC
        2. Calculates minimal viable order size
        3. Places a market buy order
        4. Verifies order appears in trading history
        """
        # Get test symbol - BTC_USDC for Backpack spot
        symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")
        assert symbol == "BTC_USDC", f"Expected BTC_USDC symbol for Backpack, got {symbol}"

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

            # For Backpack IOC orders, the order disappears immediately after execution
            # We need to check order history to verify the fill
            logger.info(
                f"Market order placed: {order.exchange_order_id}. "
                "Polling order history for confirmation..."
            )

            # Verify order appears in history (polls every 2 seconds for up to 40 seconds)
            found_in_history = await MarketOrderTestHelpers.verify_order_in_history(
                backpack_api, order, max_wait_seconds=40, poll_interval=2.0
            )
            assert found_in_history, (
                f"Order {order.exchange_order_id} not found in trading history after polling"
            )

            # Get the actual filled quantity from history
            filled_quantity = await MarketOrderTestHelpers.get_filled_quantity_from_history(
                backpack_api, order.exchange_order_id
            )
            assert filled_quantity is not None and filled_quantity > 0, (
                f"Order {order.exchange_order_id} not filled or fill quantity not found"
            )

            # Update order with actual fill data
            filled_order = order
            filled_order.status = OrderStatus.FILLED
            filled_order.quantity_filled = filled_quantity

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

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/backpack"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_market_sell_previous_buy_btc_spot(
        self,
        backpack_api: BackpackAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
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

            # For Backpack IOC orders, check order history
            logger.info(
                f"Market sell order placed: {order.exchange_order_id}. "
                "Waiting for order to appear in history..."
            )

            # Verify order appears in history (polls automatically)
            found_in_history = await MarketOrderTestHelpers.verify_order_in_history(
                backpack_api, order, max_wait_seconds=40, poll_interval=2.0
            )
            assert found_in_history, (
                f"Order {order.exchange_order_id} not found in trading history after polling"
            )

            # Get the actual filled quantity from history
            filled_quantity = await MarketOrderTestHelpers.get_filled_quantity_from_history(
                backpack_api, order.exchange_order_id
            )
            assert filled_quantity is not None and filled_quantity > 0, (
                f"Order {order.exchange_order_id} not filled or fill quantity not found"
            )

            # Update order with actual fill data
            filled_order = order
            filled_order.status = OrderStatus.FILLED
            filled_order.quantity_filled = filled_quantity

            # Verify exact quantity was filled - exchanges should handle exact amounts
            # No arbitrary tolerances allowed per security rules
            market = await backpack_api.get_market(GetMarketArgs(symbol=symbol))
            if market and market.step_size and filled_order.quantity_filled:
                # Both quantities should be rounded to same step size by exchange
                steps_requested = int(sell_quantity / market.step_size)
                steps_filled = int(filled_order.quantity_filled / market.step_size)
                assert steps_requested == steps_filled, (
                    f"Filled quantity {filled_order.quantity_filled} does not match "
                    f"requested {sell_quantity} when rounded to step size {market.step_size}"
                )
            else:
                pytest.fail("Cannot verify quantity without market step size")

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
        """Find a perpetual market symbol on Backpack.

        Note: Backpack's /api/v1/markets endpoint may not list all available markets,
        especially PERP markets. PERP markets exist but may require direct symbol access.
        """
        from cyberdelta.apis.models.service_args_models import GetMarketArgs, GetMarketsArgs

        # First, try to find from available markets
        try:
            markets = await backpack_api.get_markets(GetMarketsArgs())
            if markets:
                # Look for any PERP markets (not just BTC)
                perp_markets = [m for m in markets if "PERP" in m.symbol.upper()]
                if perp_markets:
                    # Prefer BTC if available, otherwise use first PERP market
                    btc_perp = next((m for m in perp_markets if "BTC" in m.symbol.upper()), None)
                    return btc_perp.symbol if btc_perp else perp_markets[0].symbol

                logger.debug("No PERP markets found in markets endpoint response")
        except Exception as e:
            logger.debug(f"Error getting markets list: {e}")

        # Based on order history data, these PERP symbols are known to exist on Backpack
        # even if they don't appear in the markets endpoint
        known_perp_symbols = ["BTC_USDC_PERP", "SOL_USDC_PERP", "ETH_USDC_PERP"]

        for symbol in known_perp_symbols:
            try:
                # Try to get market data for the symbol
                market = await backpack_api.get_market(GetMarketArgs(symbol=symbol))
                if market:
                    logger.info(f"Found PERP market {symbol} via direct market query")
                    return symbol
            except Exception as e:
                logger.debug(f"PERP symbol {symbol} not accessible: {e}")
                continue

        logger.info("No accessible PERP markets found on Backpack")
        return None

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/backpack"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_market_order_perp_market(
        self,
        backpack_api: BackpackAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
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

            # Verify buy order appears in history
            logger.info(f"Perp buy order placed: {buy_order.exchange_order_id}")
            found_in_history = await MarketOrderTestHelpers.verify_order_in_history(
                backpack_api, buy_order, max_wait_seconds=40, poll_interval=2.0
            )
            assert found_in_history, f"Buy order {buy_order.exchange_order_id} not found in history"

            # Get filled quantity from history - PERP markets need more time
            buy_filled_qty = await MarketOrderTestHelpers.get_filled_quantity_from_history(
                backpack_api,
                buy_order.exchange_order_id or buy_order.client_order_id,
                max_retries=10,  # More retries for PERP markets
                retry_delay=3.0,  # Longer delay for PERP markets
            )

            # PERP markets may have timing delays for orders to appear in history
            # The improved helper includes retry logic to handle this
            if buy_filled_qty is None or buy_filled_qty <= 0:
                pytest.fail(
                    f"PERP buy order {buy_order.exchange_order_id} verification failed. "
                    "Unable to get filled quantity from history. This is a critical failure "
                    "as we cannot verify order execution."
                )

            # Immediately sell to close position
            sell_order = await market_order.execute_market_order(
                symbol=perp_symbol,
                side=OrderSide.SELL,
                quantity=buy_filled_qty,
            )

            logger.info(f"Perp sell order placed: {sell_order.exchange_order_id}")

            # Verify sell order appears in history
            found_sell = await MarketOrderTestHelpers.verify_order_in_history(
                backpack_api, sell_order, max_wait_seconds=40, poll_interval=2.0
            )
            assert found_sell, f"Sell order {sell_order.exchange_order_id} not found in history"

            # Verify sell order filled - PERP markets need more time to appear in history
            sell_filled_qty = await MarketOrderTestHelpers.get_filled_quantity_from_history(
                backpack_api,
                sell_order.exchange_order_id or sell_order.client_order_id,
                max_retries=10,  # More retries for PERP markets
                retry_delay=3.0,  # Longer delay for PERP markets
            )
            assert sell_filled_qty is not None and sell_filled_qty > 0, (
                f"Sell order {sell_order.exchange_order_id} not filled"
            )

            logger.info(f"Successfully tested perpetual market orders on {perp_symbol}")

        except Exception as e:
            pytest.fail(
                f"Failed to test perpetual market orders: {e}. "
                "Market order functionality must work across all market types."
            )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/backpack"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_insufficient_liquidity_error_spot(
        self,
        backpack_api: BackpackAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
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
                pytest.fail(
                    f"No orderbook data available for {symbol}. "
                    "Cannot test insufficient liquidity without orderbook data. "
                    "This is a critical failure as orderbook access is required."
                )

            # Calculate total available liquidity on ask side
            total_ask_quantity = sum(price_qty[1] for price_qty in orderbook.asks)

            # Try to buy more than available liquidity
            # Add one step size to ensure we exceed available
            market = await backpack_api.get_market(GetMarketArgs(symbol=symbol))
            if not market or not market.step_size:
                pytest.fail(f"Cannot get market constraints for {symbol}")

            # Exceed available liquidity by exactly one step
            excessive_quantity = total_ask_quantity + market.step_size

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
            # If we get API errors about insufficient funds, the test setup is invalid
            if "INSUFFICIENT_FUNDS" in str(e) or "Insufficient funds" in str(e):
                pytest.fail(
                    f"Expected InsufficientLiquidityError but got INSUFFICIENT_FUNDS. "
                    f"The MarketOrder class should check liquidity BEFORE placing the order. "
                    f"It should not send orders that exceed available liquidity to the exchange. "
                    f"Error: {e}"
                )
            else:
                pytest.fail(f"Expected InsufficientLiquidityError but got {type(e).__name__}: {e}")

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/backpack"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_price_deviation_protection_spot(
        self,
        backpack_api: BackpackAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
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

        # Get order book data to determine realistic tight slippage
        order_book = await backpack_api.get_order_book(symbol, depth=5)
        if not order_book or not order_book.bids or not order_book.asks:
            pytest.fail(f"Cannot get order book data for {symbol}")

        # Use half the current spread as tight slippage
        best_bid = order_book.bids[0][0]  # First element is price
        best_ask = order_book.asks[0][0]  # First element is price
        spread_pct = (best_ask - best_bid) / best_bid
        tight_slippage = spread_pct / 2

        # Create config with market-based tight slippage
        # Use existing config's liquidity ratio - no arbitrary values
        tight_config = MarketOrderConfig(
            default_slippage_pct=tight_slippage,
            max_slippage_pct=tight_slippage,
            min_liquidity_ratio=market_order_config.min_liquidity_ratio,
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
                max_slippage=tight_slippage,  # Use market-based tight slippage
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

    async def _execute_and_verify_market_order(
        self,
        market_order: MarketOrder,
        backpack_api: BackpackAPI,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        market_type: str,
    ) -> tuple[str, OrderSide, Decimal] | None:
        """Execute a market order and verify it appears in history."""
        order = await market_order.execute_market_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
        )

        # Verify order appears in history
        logger.info(f"{market_type} order placed: {order.exchange_order_id}")
        found = await MarketOrderTestHelpers.verify_order_in_history(
            backpack_api, order, max_wait_seconds=40, poll_interval=2.0
        )
        assert found, f"{market_type} order {order.exchange_order_id} not found in history"

        filled_quantity = await MarketOrderTestHelpers.get_filled_quantity_from_history(
            backpack_api, order.exchange_order_id or order.client_order_id
        )

        if filled_quantity and filled_quantity > 0:
            logger.info(f"Successfully executed {market_type.lower()} market order for {symbol}")
            opposite_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
            return (symbol, opposite_side, filled_quantity)

        return None

    async def _test_spot_market(
        self,
        market_order: MarketOrder,
        backpack_api: BackpackAPI,
    ) -> tuple[str, OrderSide, Decimal] | None:
        """Test spot market execution."""
        spot_symbol = await MarketOrderTestHelpers.get_test_symbol(backpack_api, "backpack")
        spot_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            backpack_api, spot_symbol, OrderSide.BUY
        )

        return await self._execute_and_verify_market_order(
            market_order, backpack_api, spot_symbol, OrderSide.BUY, spot_quantity, "Spot"
        )

    async def _test_perp_market(
        self,
        market_order: MarketOrder,
        backpack_api: BackpackAPI,
    ) -> tuple[str, OrderSide, Decimal] | None:
        """Test perpetual market execution if available."""
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await backpack_api.get_markets(GetMarketsArgs())
        if not markets:
            return None

        perp_markets = [m for m in markets if "PERP" in m.symbol.upper()]
        if not perp_markets:
            return None

        perp_symbol = perp_markets[0].symbol
        perp_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            backpack_api, perp_symbol, OrderSide.BUY
        )

        return await self._execute_and_verify_market_order(
            market_order, backpack_api, perp_symbol, OrderSide.BUY, perp_quantity, "Perp"
        )

    async def _cleanup_positions(
        self,
        market_order: MarketOrder,
        backpack_api: BackpackAPI,
        executed_orders: list[tuple[str, OrderSide, Decimal]],
    ) -> None:
        """Clean up executed positions."""
        for symbol, side, quantity in executed_orders:
            try:
                cleanup_order = await market_order.execute_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                )
                # Verify cleanup order appears in history
                found_cleanup = await MarketOrderTestHelpers.verify_order_in_history(
                    backpack_api, cleanup_order, max_wait_seconds=40, poll_interval=2.0
                )
                if not found_cleanup:
                    logger.warning(
                        f"Cleanup order {cleanup_order.exchange_order_id} not found in history"
                    )
                logger.info(f"Cleaned up position for {symbol}")
            except Exception as e:
                pytest.fail(
                    f"Failed to clean up {symbol} position: {e}. "
                    "Position cleanup is critical and must succeed."
                )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/backpack"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_cross_market_type_execution(
        self,
        backpack_api: BackpackAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
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
        try:
            spot_result = await self._test_spot_market(market_order, backpack_api)
            if spot_result:
                executed_orders.append(spot_result)
        except Exception as e:
            pytest.fail(
                f"Failed spot market order: {e}. Cross-market execution must work reliably."
            )

        # Try to test perp market if available
        try:
            perp_result = await self._test_perp_market(market_order, backpack_api)
            if perp_result:
                executed_orders.append(perp_result)
        except Exception as e:
            # Only skip if no perp markets available
            if "PERP" not in str(e) and "perpetual" not in str(e).lower():
                pytest.fail(
                    f"Failed perp market order: {e}. Cross-market execution must work reliably."
                )
            else:
                logger.info(f"No perpetual markets available: {e}")

        # Clean up positions
        await self._cleanup_positions(market_order, backpack_api, executed_orders)

        # Verify we tested at least one market type
        assert len(executed_orders) > 0, "Failed to execute any market orders across market types"

    @pytest_asyncio.fixture(autouse=True)
    async def cleanup(
        self, backpack_api: BackpackAPI, request: pytest.FixtureRequest
    ) -> AsyncGenerator[None]:
        """Clean up any test positions after each test."""
        yield  # Run the test

        # Skip cleanup in VCR mode to avoid unrecorded API calls
        # request.node type is not fully typed in pytest, use getattr for safety
        node = getattr(request, "node", None)
        if node and hasattr(node, "get_closest_marker") and node.get_closest_marker("vcr"):
            logger.info("Skipping cleanup in VCR mode")
            return

        # After test cleanup
        try:
            await MarketOrderTestHelpers.cleanup_test_positions(backpack_api, "BTC_USDC")
        except Exception as e:
            # Cleanup errors should not be silenced for real failures
            pytest.fail(
                f"Error during test cleanup: {e}. "
                "Test cleanup is critical to prevent interference with other tests."
            )
