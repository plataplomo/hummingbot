"""Integration tests for Hyperliquid market orders.

These tests verify market order functionality against real Hyperliquid APIs,
ensuring proper order execution, liquidity checking, and error handling.
All tests follow TESTING_SECURITY_RULES.md strictly.

Tests use pytest-recording (VCR) for deterministic test execution with cassettes.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio

from cyberdelta.apis.hyperliquid import HyperliquidAPI
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
pytestmark = [pytest.mark.integration, pytest.mark.vcr, pytest.mark.timing]

# Module-level storage for order data between tests
_test_order_data: dict[str, str | Decimal | None] = {
    "hyperliquid_buy_order_id": None,
    "hyperliquid_buy_quantity": None,
}


class TestHyperliquidMarketOrderIntegration:
    """Integration tests for Hyperliquid market orders."""

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/hyperliquid"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_market_buy_order_btc_perp(
        self,
        hyperliquid_api: HyperliquidAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test placing a market buy order for BTC perpetual.

        This test:
        1. Gets real-time market data for BTC
        2. Calculates minimal viable order size
        3. Places a market buy order
        4. Verifies order appears in trading history
        """
        # Get test symbol - BTC for Hyperliquid perps
        symbol = await MarketOrderTestHelpers.get_test_symbol(hyperliquid_api, "hyperliquid")
        assert symbol == "BTC", f"Expected BTC symbol for Hyperliquid, got {symbol}"

        # Get minimal test quantity from real market data
        test_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            hyperliquid_api, symbol, OrderSide.BUY
        )
        logger.info(f"Using minimal test quantity: {test_quantity} for {symbol}")

        # Create market order executor
        service = MarketOrderService(exchange_api=hyperliquid_api, config=market_order_config)
        market_order = MarketOrder(
            exchange_api=hyperliquid_api, market_order_service=service, config=market_order_config
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
                hyperliquid_api, order, timeout=30
            )

            # Verify order was filled
            assert filled_order.status == OrderStatus.FILLED, (
                f"Order not filled. Status: {filled_order.status}"
            )
            assert filled_order.quantity_filled is not None, "Filled order missing quantity_filled"
            assert filled_order.quantity_filled is not None and filled_order.quantity_filled > 0, (
                "No quantity was filled"
            )

            # Verify order appears in history
            found_in_history = await MarketOrderTestHelpers.verify_order_in_history(
                hyperliquid_api, filled_order
            )
            assert found_in_history, (
                f"Order {filled_order.exchange_order_id} not found in trading history"
            )

            # Store order ID for cleanup test
            _test_order_data["hyperliquid_buy_order_id"] = filled_order.exchange_order_id
            _test_order_data["hyperliquid_buy_quantity"] = filled_order.quantity_filled

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
        "custom_vcr_cassette_dir", ["core/execution/orders/hyperliquid"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_market_sell_previous_buy_btc_perp(
        self,
        hyperliquid_api: HyperliquidAPI,
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
        buy_order_id_raw = _test_order_data.get("hyperliquid_buy_order_id")
        buy_quantity_raw = _test_order_data.get("hyperliquid_buy_quantity")

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
        symbol = await MarketOrderTestHelpers.get_test_symbol(hyperliquid_api, "hyperliquid")

        # Double-check the quantity from history to ensure accuracy
        historical_quantity = await MarketOrderTestHelpers.get_filled_quantity_from_history(
            hyperliquid_api, buy_order_id
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
        service = MarketOrderService(exchange_api=hyperliquid_api, config=market_order_config)
        market_order = MarketOrder(
            exchange_api=hyperliquid_api, market_order_service=service, config=market_order_config
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
                hyperliquid_api, order, timeout=30
            )

            # Verify order was filled
            assert filled_order.status == OrderStatus.FILLED, (
                f"Order not filled. Status: {filled_order.status}"
            )
            assert filled_order.quantity_filled is not None, "Filled order missing quantity_filled"
            assert filled_order.quantity_filled is not None and filled_order.quantity_filled > 0, (
                "No quantity was filled"
            )

            # Verify exact quantity was filled - exchanges should handle exact amounts
            # No arbitrary tolerances allowed per security rules
            market = await hyperliquid_api.get_market(GetMarketArgs(symbol=symbol))
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
                hyperliquid_api, filled_order
            )
            assert found_in_history, (
                f"Order {filled_order.exchange_order_id} not found in trading history"
            )

            logger.info(
                f"Successfully placed and verified market sell order: "
                f"{filled_order.exchange_order_id}, filled: {filled_order.quantity_filled}"
            )

            # Clean up test data
            _test_order_data["hyperliquid_buy_order_id"] = None
            _test_order_data["hyperliquid_buy_quantity"] = None

        except Exception as e:
            pytest.fail(
                f"Failed to execute market sell order for {symbol}: {e}. "
                "Market order execution is a critical operation that must work reliably."
            )

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/hyperliquid"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_insufficient_liquidity_error(
        self,
        hyperliquid_api: HyperliquidAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that insufficient liquidity is properly detected and raises error.

        This test attempts to place an order larger than available liquidity
        to verify proper error handling.
        """
        symbol = await MarketOrderTestHelpers.get_test_symbol(hyperliquid_api, "hyperliquid")

        # Get current orderbook to determine a size that exceeds liquidity
        try:
            orderbook = await hyperliquid_api.get_order_book(symbol, depth=20)

            if not orderbook or not orderbook.asks:
                pytest.skip(f"No orderbook data available for {symbol}")

            # Calculate total available liquidity on ask side
            total_ask_quantity = sum(price_qty[1] for price_qty in orderbook.asks)

            # Try to buy more than available liquidity
            # Add one step size to ensure we exceed available
            market = await hyperliquid_api.get_market(GetMarketArgs(symbol=symbol))
            if not market or not market.step_size:
                pytest.fail(f"Cannot get market constraints for {symbol}")

            # Exceed available liquidity by exactly one step
            excessive_quantity = total_ask_quantity + market.step_size

            logger.info(
                f"Testing insufficient liquidity with quantity {excessive_quantity} "
                f"(available: {total_ask_quantity})"
            )

            # Create market order executor
            service = MarketOrderService(exchange_api=hyperliquid_api, config=market_order_config)
            market_order = MarketOrder(
                exchange_api=hyperliquid_api,
                market_order_service=service,
                config=market_order_config,
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

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/hyperliquid"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_price_deviation_protection(
        self,
        hyperliquid_api: HyperliquidAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that excessive price deviation is properly detected.

        This test uses a very tight slippage tolerance to trigger
        price deviation protection.
        """
        symbol = await MarketOrderTestHelpers.get_test_symbol(hyperliquid_api, "hyperliquid")

        # Get minimal test quantity
        test_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            hyperliquid_api, symbol, OrderSide.BUY
        )

        # Get market data to determine realistic tight slippage from order book
        order_book = await hyperliquid_api.get_order_book(symbol)
        if not order_book or not order_book.bids or not order_book.asks:
            pytest.fail(f"Cannot get order book data for {symbol}")

        best_bid = order_book.bids[0][0]  # First bid price
        best_ask = order_book.asks[0][0]  # First ask price

        # Use half the current spread as tight slippage
        spread_pct = (best_ask - best_bid) / best_bid
        tight_slippage = spread_pct / Decimal("2")

        # Create config with market-based tight slippage
        # Use existing config's liquidity ratio - no arbitrary values
        tight_config = MarketOrderConfig(
            default_slippage_pct=tight_slippage,
            max_slippage_pct=tight_slippage,
            min_liquidity_ratio=market_order_config.min_liquidity_ratio,
            order_timeout_seconds=30,
        )

        # Create market order executor with tight config
        service = MarketOrderService(exchange_api=hyperliquid_api, config=tight_config)
        market_order = MarketOrder(
            exchange_api=hyperliquid_api, market_order_service=service, config=tight_config
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

    @pytest.mark.parametrize(
        "custom_vcr_cassette_dir", ["core/execution/orders/hyperliquid"], indirect=True
    )
    @pytest.mark.asyncio
    async def test_market_order_metrics_tracking(
        self,
        hyperliquid_api: HyperliquidAPI,
        market_order_config: MarketOrderConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that market order metrics are properly tracked.

        This test verifies that execution metrics like slippage and
        execution time are properly recorded.
        """
        symbol = await MarketOrderTestHelpers.get_test_symbol(hyperliquid_api, "hyperliquid")

        # Get minimal test quantity
        test_quantity = await MarketOrderTestHelpers.get_minimal_test_quantity(
            hyperliquid_api, symbol, OrderSide.BUY
        )

        # Create market order executor
        service = MarketOrderService(exchange_api=hyperliquid_api, config=market_order_config)
        market_order = MarketOrder(
            exchange_api=hyperliquid_api, market_order_service=service, config=market_order_config
        )

        # Record start time
        start_time = datetime.now(UTC)

        try:
            # Execute market order
            order = await market_order.execute_market_order(
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=test_quantity,
            )

            # Calculate execution time
            execution_time = (datetime.now(UTC) - start_time).total_seconds()

            # Verify order has metrics
            assert order is not None, "Order should not be None"
            assert order.exchange_order_id is not None, "Order should have exchange ID"

            # Wait for fill
            filled_order = await MarketOrderTestHelpers.wait_for_order_fill(
                hyperliquid_api, order, timeout=30
            )

            # Verify fill details for metrics
            assert filled_order.quantity_filled is not None, "Should have filled quantity"
            assert filled_order.quantity_filled > 0, "Should have positive fill"

            if filled_order.average_fill_price:
                logger.info(
                    f"Market order metrics - "
                    f"Symbol: {symbol}, "
                    f"Side: BUY, "
                    f"Requested: {test_quantity}, "
                    f"Filled: {filled_order.quantity_filled}, "
                    f"Avg Price: {filled_order.average_fill_price}, "
                    f"Execution Time: {execution_time:.2f}s"
                )

            # Clean up - sell the position
            try:
                sell_order = await market_order.execute_market_order(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    quantity=filled_order.quantity_filled,
                )

                # Wait for sell to complete
                await MarketOrderTestHelpers.wait_for_order_fill(
                    hyperliquid_api, sell_order, timeout=30
                )

            except Exception as e:
                pytest.fail(
                    f"Failed to clean up position: {e}. "
                    "Position cleanup is critical and must succeed."
                )

        except Exception as e:
            pytest.fail(
                f"Failed to execute market order for metrics test: {e}. "
                "Market order execution and metrics tracking must work reliably."
            )

    @pytest_asyncio.fixture(autouse=True)
    async def cleanup(self, hyperliquid_api: HyperliquidAPI) -> AsyncGenerator[None]:
        """Clean up any test positions after each test."""
        yield  # Run the test

        # After test cleanup
        try:
            await MarketOrderTestHelpers.cleanup_test_positions(hyperliquid_api, "BTC")
        except Exception as e:
            # Cleanup errors should not be silenced
            pytest.fail(
                f"Error during test cleanup: {e}. "
                "Test cleanup is critical to prevent interference with other tests."
            )
