"""Test helpers for market order integration tests.

This module provides utilities for testing market orders across different exchanges,
ensuring all tests use real market data and follow security rules.
"""

import asyncio
from decimal import Decimal

import pytest

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetMarketArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order, OrderSide, OrderStatus
from cyberdelta.core.models.market import Market


logger = get_logger(__name__)

# Mark this module as using timing operations
pytestmark = pytest.mark.timing


class MarketOrderTestHelpers:
    """Helper utilities for market order integration tests."""

    @staticmethod
    def _get_hyperliquid_symbol(markets: list[Market]) -> str:
        """Get test symbol for Hyperliquid exchange.

        Returns:
            Preferred test symbol for Hyperliquid trading

        Raises:
            RuntimeError: If no perpetual markets are available.
        """
        # Filter for perpetual markets
        perp_markets = [m for m in markets if m.market_type == "Perpetual"]
        if not perp_markets:
            raise RuntimeError(
                "No perpetual markets available on Hyperliquid. "
                "Market order tests require perpetual markets.",
            )

        # Prefer BTC market if available, otherwise use first available
        btc_markets = [m for m in perp_markets if "BTC" in m.symbol.upper()]
        if btc_markets:
            return btc_markets[0].symbol

        # Return first available perpetual symbol
        return perp_markets[0].symbol

    @staticmethod
    def _get_backpack_symbol(markets: list[Market]) -> str:
        """Get test symbol for Backpack exchange.

        Returns:
            Preferred test symbol for Backpack trading

        Raises:
            RuntimeError: If no markets are available.
        """
        # Debug: print available market types
        market_types = {m.market_type for m in markets}
        logger.info(
            "available_market_types_backpack",
            market_types=list(market_types),
            message="Available market types on Backpack",
        )

        # Filter for spot markets - Backpack returns "SPOT" in uppercase
        spot_markets = [m for m in markets if m.market_type.upper() == "SPOT"]
        if not spot_markets:
            # If no spot markets, try to use any available market for testing
            logger.warning(
                "no_spot_markets_found",
                available_markets=[(m.symbol, m.market_type) for m in markets[:5]],
                message="No spot markets found. Available markets",
            )
            if markets:
                # Use first available market as fallback
                logger.info(
                    "using_first_available_market",
                    symbol=markets[0].symbol,
                    message="Using first available market",
                )
                return markets[0].symbol

            raise RuntimeError(
                f"No markets available on Backpack. Market types found: {market_types}",
            )

        # Prefer BTC_USDC if available
        btc_usdc_markets = [m for m in spot_markets if m.symbol == "BTC_USDC"]
        if btc_usdc_markets:
            return btc_usdc_markets[0].symbol

        # Otherwise look for any BTC market
        btc_markets = [m for m in spot_markets if "BTC" in m.symbol.upper()]
        if btc_markets:
            return btc_markets[0].symbol

        # Return first available spot symbol
        return spot_markets[0].symbol

    @staticmethod
    async def get_test_symbol(exchange_api: ExchangeAPI, exchange_name: str) -> str:
        """Get appropriate test symbol for the exchange.

        Args:
            exchange_api: Exchange API instance
            exchange_name: Name of the exchange

        Returns:
            Test symbol appropriate for the exchange

        Raises:
            RuntimeError: If unable to determine test symbol
        """
        try:
            # Get available markets from the exchange dynamically
            from cyberdelta.apis.models.service_args_models import GetMarketsArgs

            markets = await exchange_api.get_markets(GetMarketsArgs())
            if not markets:
                raise RuntimeError(
                    f"No markets available from {exchange_name}. "
                    "Cannot run tests without available markets.",
                )

            # For Hyperliquid, get perpetual markets
            if exchange_name == "hyperliquid":
                return MarketOrderTestHelpers._get_hyperliquid_symbol(markets)

            # For Backpack, get spot markets
            if exchange_name == "backpack":
                return MarketOrderTestHelpers._get_backpack_symbol(markets)

            raise RuntimeError(f"Unknown exchange: {exchange_name}")

        except Exception as e:
            raise RuntimeError(
                f"Failed to get test symbol from {exchange_name}: {e}. "
                "Market order tests require dynamic symbol discovery from exchange.",
            ) from e

    @staticmethod
    async def get_minimal_test_quantity(
        exchange_api: ExchangeAPI,
        symbol: str,
        side: OrderSide,
    ) -> Decimal:
        """Get minimal test quantity that meets exchange requirements.

        Args:
            exchange_api: Exchange API instance
            symbol: Trading symbol
            side: Order side

        Returns:
            Minimal quantity for testing

        Raises:
            RuntimeError: If unable to determine minimal quantity
        """
        try:
            # For Hyperliquid, use specialized helpers that handle $10 minimum notional requirement
            from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI

            if isinstance(exchange_api, HyperliquidAPI):
                from tests.integration.apis.hyperliquid.shared.hl_test_helpers import (
                    HyperliquidTestHelpers,
                )

                minimal_quantity: Decimal = await HyperliquidTestHelpers.get_minimal_order_size(
                    exchange_api,
                    symbol,
                    side,
                )

                logger.info(
                    "using_hyperliquid_minimal_order_size",
                    symbol=symbol,
                    minimal_quantity=str(minimal_quantity),
                    message="Using Hyperliquid minimal order size (meets $10 minimum notional)",
                )

                return minimal_quantity

            # For other exchanges, use exchange minimum
            # Get market constraints from exchange - NO FALLBACKS
            market = await exchange_api.get_market(GetMarketArgs(symbol=symbol))
            if not market:
                raise RuntimeError(f"Unable to get market info for {symbol}")

            if not market.min_quantity:
                raise RuntimeError(
                    f"Exchange did not provide min_quantity for {symbol}. "
                    "Cannot proceed without exchange constraints.",
                )

            if not market.step_size:
                raise RuntimeError(
                    f"Exchange did not provide step_size for {symbol}. "
                    "Cannot proceed without exchange constraints.",
                )

            # Use exactly the exchange minimum - no buffers, no calculations
            assert market.min_quantity is not None  # Already checked above
            minimal_quantity = market.min_quantity

            logger.info(
                "using_exchange_minimum_quantity",
                symbol=symbol,
                minimal_quantity=str(minimal_quantity),
                step_size=str(market.step_size),
                message="Using exchange minimum quantity",
            )

            return minimal_quantity

        except Exception as e:
            raise RuntimeError(
                f"Failed to get minimal test quantity for {symbol}: {e}. "
                "Market order tests require real market data.",
            ) from e

    @staticmethod
    async def wait_for_order_fill(
        exchange_api: ExchangeAPI,
        order: Order,
        timeout: int = 30,
    ) -> Order:
        """Wait for order to fill or reach terminal state.

        Args:
            exchange_api: Exchange API instance
            order: Order to monitor
            timeout: Maximum seconds to wait

        Returns:
            Updated order with final status

        Raises:
            TimeoutError: If order doesn't reach terminal state in time
        """
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < timeout:
            # Get updated order status
            try:
                # Try to get order by ID - Backpack requires symbol parameter
                args = GetOrderArgs(
                    order_id=order.exchange_order_id or order.client_order_id,
                    symbol=order.symbol,
                )
                updated_order = await exchange_api.get_order(args)

                if updated_order:
                    # Check if order is in terminal state
                    terminal_states = {
                        OrderStatus.FILLED,
                        OrderStatus.CANCELED,
                        OrderStatus.REJECTED,
                        OrderStatus.EXPIRED,
                    }

                    if updated_order.status in terminal_states:
                        return updated_order

                    # For market orders, partial fill is also acceptable
                    if updated_order.status == OrderStatus.PARTIALLY_FILLED:
                        # Wait a bit more to see if it fills completely
                        await asyncio.sleep(0.5)
                        continue

            except Exception as e:
                logger.debug(
                    "error_checking_order_status",
                    error=str(e),
                    message="Error checking order status",
                )

            await asyncio.sleep(0.1)

        raise TimeoutError(
            f"Order {order.client_order_id} did not reach terminal state within {timeout} seconds",
        )

    @staticmethod
    async def verify_order_in_history(
        exchange_api: ExchangeAPI,
        order: Order,
        max_wait_seconds: int = 40,
        poll_interval: float = 2.0,
    ) -> bool:
        """Verify that order appears in trading history.

        Args:
            exchange_api: Exchange API instance
            order: Order to verify
            max_wait_seconds: Maximum seconds to wait
            poll_interval: Seconds between polling attempts

        Returns:
            True if order found in history, False otherwise
        """
        import time

        loop_start_time = time.time()

        while time.time() - loop_start_time < max_wait_seconds:
            try:
                # Get recent order history - Hyperliquid requires start_time and end_time
                from datetime import UTC, datetime, timedelta

                end_time = datetime.now(UTC)
                start_time = end_time - timedelta(hours=1)  # Look back 1 hour

                history_args = GetOrderHistoryArgs(
                    limit=50,
                    start_time=start_time,
                    end_time=end_time,
                )
                orders = await exchange_api.get_order_history(history_args)

                logger.debug(
                    "retrieved_orders_from_history",
                    orders_count=len(orders) if orders else 0,
                    looking_for_order_id=order.exchange_order_id,
                    message="Retrieved orders from history",
                )

                if orders:
                    # Check if our order is in the history
                    for i, historical_order in enumerate(orders):
                        logger.debug(
                            "order_history_item",
                            order_number=i + 1,
                            order_id=historical_order.exchange_order_id,
                            status=historical_order.status.value,
                            symbol=historical_order.symbol,
                            message="Order history item",
                        )

                        # Compare both as strings to handle type mismatches
                        if str(historical_order.exchange_order_id) == str(order.exchange_order_id):
                            logger.info(
                                "order_found_in_history",
                                order_id=order.exchange_order_id,
                                position=i + 1,
                                message="✅ Order found in history",
                            )
                            return True
                        if (
                            historical_order.client_order_id
                            and order.client_order_id
                            and str(historical_order.client_order_id) == str(order.client_order_id)
                        ):
                            logger.info(
                                "order_found_by_client_id",
                                client_order_id=order.client_order_id,
                                position=i + 1,
                                message="✅ Order found in history by client ID",
                            )
                            return True
                else:
                    logger.debug("No orders returned from get_order_history")

                # Also check trades history
                trades_args = GetTradeHistoryArgs(limit=20)
                trades = await exchange_api.get_trade_history(trades_args)

                if trades:
                    for trade in trades:
                        if trade.order_id == order.exchange_order_id:
                            logger.info(
                                "order_found_in_trades",
                                order_id=order.exchange_order_id,
                                message="Order found in trades",
                            )
                            return True

            except Exception as e:
                elapsed = int(time.time() - loop_start_time)
                logger.debug(
                    "error_checking_order_history",
                    elapsed_seconds=elapsed,
                    error=str(e),
                    message=f"Error checking order history after {elapsed}s",
                )

            # Wait before next attempt
            await asyncio.sleep(poll_interval)

        logger.warning(
            "verify_order_in_history_timeout",
            max_wait_seconds=max_wait_seconds,
            order_id=order.exchange_order_id,
            message=(
                f"verify_order_in_history timed out after {max_wait_seconds}s "
                f"for order {order.exchange_order_id}"
            ),
        )
        return False

    @staticmethod
    async def _check_order_history(
        exchange_api: ExchangeAPI,
        order_id: str,
        history_args: GetOrderHistoryArgs,
    ) -> Decimal | None:
        """Check order history for filled quantity.

        Returns:
            Filled quantity if found in order history, None otherwise.
        """
        orders = await exchange_api.get_order_history(history_args)

        if not orders:
            return None

        logger.debug(
            "retrieved_orders_from_history",
            orders_count=len(orders),
            message=f"Retrieved {len(orders)} orders from history",
        )

        for historical_order in orders:
            # Log order details for debugging
            logger.debug(
                "checking_order_details",
                exchange_id=historical_order.exchange_order_id,
                client_id=historical_order.client_order_id,
                filled=(
                    str(historical_order.quantity_filled)
                    if historical_order.quantity_filled
                    else None
                ),
                message="Checking order details",
            )

            order_matches = order_id in {
                historical_order.exchange_order_id,
                historical_order.client_order_id,
            }

            if order_matches:
                if historical_order.quantity_filled and historical_order.quantity_filled > 0:
                    logger.info(
                        "order_found_with_filled_quantity",
                        order_id=order_id,
                        filled_quantity=str(historical_order.quantity_filled),
                        message=(
                            f"Found order {order_id} with filled quantity: "
                            f"{historical_order.quantity_filled}"
                        ),
                    )
                    return historical_order.quantity_filled
                logger.debug(
                    "found_order_but_zero_quantity",
                    order_id=order_id,
                    quantity_filled=str(historical_order.quantity_filled),
                    message="Found order but quantity_filled is zero",
                )

        return None

    @staticmethod
    async def _check_trade_history(
        exchange_api: ExchangeAPI,
        order_id: str,
    ) -> Decimal | None:
        """Check trade history for filled quantity.

        Returns:
            Total filled quantity from trade history, None if no matching trades found.
        """
        try:
            trades_args = GetTradeHistoryArgs(limit=50)
            trades = await exchange_api.get_trade_history(trades_args)

            if not trades:
                return None

            logger.debug(
                "retrieved_trades_from_history",
                trades_count=len(trades),
                message="Retrieved trades from history",
            )
            total_filled = Decimal(0)
            matching_trades = 0

            for trade in trades:
                logger.debug(
                    "checking_trade",
                    trade_order_id=trade.order_id,
                    trade_quantity=str(trade.quantity),
                    trade_symbol=trade.symbol,
                    message="Checking trade",
                )
                if trade.order_id == order_id:
                    total_filled += trade.quantity
                    matching_trades += 1

            if total_filled > 0:
                logger.info(
                    "trades_found_for_order",
                    matching_trades_count=matching_trades,
                    order_id=order_id,
                    total_filled=str(total_filled),
                    message="Found trades for order",
                )
                return total_filled
            logger.debug(
                "no_matching_trades_found",
                order_id=order_id,
                message="No matching trades found for order",
            )

        except Exception as trade_error:
            # Trades endpoint may not be available for all market types (e.g., PERP markets)
            logger.debug(
                "trade_history_access_failed",
                order_id=order_id,
                error=str(trade_error),
                message="Trade history access failed for order",
            )

        return None

    @staticmethod
    async def get_filled_quantity_from_history(
        exchange_api: ExchangeAPI,
        order_id: str,
        max_retries: int = 5,
        retry_delay: float = 2.0,
    ) -> Decimal | None:
        """Get the filled quantity of a previous order from history.

        For PERP markets especially, orders may take time to appear in history,
        so this method includes retry logic with delays.

        Args:
            exchange_api: Exchange API instance
            order_id: Order ID to look up (exchange_order_id or client_order_id)
            max_retries: Maximum number of retry attempts
            retry_delay: Seconds to wait between retries

        Returns:
            Filled quantity if found, None otherwise
        """
        import asyncio
        from datetime import UTC, datetime, timedelta

        for attempt in range(max_retries):
            try:
                logger.debug(
                    "looking_for_order_in_history",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    order_id=order_id,
                    message="Looking for order in history",
                )

                # Prepare history args with time window
                end_time = datetime.now(UTC)
                start_time = end_time - timedelta(hours=1)  # Look back 1 hour
                history_args = GetOrderHistoryArgs(
                    limit=50,
                    start_time=start_time,
                    end_time=end_time,
                )

                # Check order history
                order_quantity = await MarketOrderTestHelpers._check_order_history(
                    exchange_api,
                    order_id,
                    history_args,
                )
                if order_quantity is not None:
                    return order_quantity

                # Check trade history
                trade_quantity = await MarketOrderTestHelpers._check_trade_history(
                    exchange_api,
                    order_id,
                )
                if trade_quantity is not None:
                    return trade_quantity

                # If this is not the last attempt, wait before retrying
                if attempt < max_retries - 1:
                    logger.debug(
                        "order_not_found_waiting_retry",
                        order_id=order_id,
                        retry_delay=retry_delay,
                        next_attempt=attempt + 2,
                        message="Order not found in history, waiting before retry",
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.warning(
                        "order_not_found_after_retries",
                        order_id=order_id,
                        max_retries=max_retries,
                        message="Order not found in history after retries",
                    )

            except Exception as e:
                logger.error(
                    "error_getting_filled_quantity",
                    attempt=attempt + 1,
                    error=str(e),
                    message="Error getting filled quantity from history",
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)

        return None

    @staticmethod
    async def cleanup_test_positions(
        exchange_api: ExchangeAPI,
        symbol: str,
    ) -> None:
        """Clean up any test positions after test completion.

        Args:
            exchange_api: Exchange API instance
            symbol: Symbol to clean up
        """
        try:
            # Cancel any open orders first
            open_orders = await exchange_api.get_open_orders(symbol)
            if open_orders:
                for order in open_orders:
                    if order.symbol == symbol:
                        try:
                            cancel_args = CancelOrderArgs(
                                order_id=order.exchange_order_id or order.client_order_id,
                            )
                            await exchange_api.cancel_order(cancel_args)
                            logger.info(
                                "cancelled_open_order",
                                order_id=order.exchange_order_id,
                                message="Cancelled open order",
                            )
                        except Exception as e:
                            raise RuntimeError(
                                f"Failed to cancel order {order.exchange_order_id}: {e}. "
                                "Order cancellation is critical for test cleanup.",
                            ) from e

            # Note: We don't close positions as that might affect other tests
            # or real trading strategies running on the same account

        except Exception as e:
            raise RuntimeError(
                f"Critical error during test cleanup: {e}. "
                "Test cleanup must succeed to prevent interference.",
            ) from e
