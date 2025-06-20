"""Test helpers for market order integration tests.

This module provides utilities for testing market orders across different exchanges,
ensuring all tests use real market data and follow security rules.
"""

import asyncio
from decimal import Decimal

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetMarketArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import Order, OrderSide, OrderStatus

logger = get_logger(__name__)


class MarketOrderTestHelpers:
    """Helper utilities for market order integration tests."""

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
                    "Cannot run tests without available markets."
                )

            # For Hyperliquid, get perpetual markets
            if exchange_name == "hyperliquid":
                # Filter for perpetual markets
                perp_markets = [m for m in markets if m.market_type == "Perpetual"]
                if not perp_markets:
                    raise RuntimeError(
                        "No perpetual markets available on Hyperliquid. "
                        "Market order tests require perpetual markets."
                    )

                # Prefer BTC market if available, otherwise use first available
                btc_markets = [m for m in perp_markets if "BTC" in m.symbol.upper()]
                if btc_markets:
                    return btc_markets[0].symbol

                # Return first available perpetual symbol
                return perp_markets[0].symbol

            # For Backpack, get spot markets
            elif exchange_name == "backpack":
                # Debug: print available market types
                market_types = set(m.market_type for m in markets)
                logger.info(f"Available market types on Backpack: {market_types}")

                # Filter for spot markets - Backpack returns "SPOT" in uppercase
                spot_markets = [m for m in markets if m.market_type.upper() == "SPOT"]
                if not spot_markets:
                    # If no spot markets, try to use any available market for testing
                    logger.warning(
                        f"No spot markets found. Available markets: "
                        f"{[(m.symbol, m.market_type) for m in markets[:5]]}"
                    )
                    if markets:
                        # Use first available market as fallback
                        logger.info(f"Using first available market: {markets[0].symbol}")
                        return markets[0].symbol

                    raise RuntimeError(
                        f"No markets available on Backpack. Market types found: {market_types}"
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

            else:
                raise RuntimeError(f"Unknown exchange: {exchange_name}")

        except Exception as e:
            raise RuntimeError(
                f"Failed to get test symbol from {exchange_name}: {e}. "
                "Market order tests require dynamic symbol discovery from exchange."
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
                from tests.integration.apis.hyperliquid.shared.test_helpers import (
                    HyperliquidTestHelpers,
                )

                minimal_quantity = await HyperliquidTestHelpers.get_minimal_order_size(
                    exchange_api, symbol, side
                )

                logger.info(
                    f"Using Hyperliquid minimal order size for {symbol}: {minimal_quantity} "
                    f"(meets $10 minimum notional requirement)"
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
                    "Cannot proceed without exchange constraints."
                )

            if not market.step_size:
                raise RuntimeError(
                    f"Exchange did not provide step_size for {symbol}. "
                    "Cannot proceed without exchange constraints."
                )

            # Use exactly the exchange minimum - no buffers, no calculations
            minimal_quantity = market.min_quantity

            logger.info(
                f"Using exchange minimum quantity for {symbol}: {minimal_quantity} "
                f"(step_size: {market.step_size})"
            )

            return minimal_quantity

        except Exception as e:
            raise RuntimeError(
                f"Failed to get minimal test quantity for {symbol}: {e}. "
                "Market order tests require real market data."
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
                    order_id=order.exchange_order_id or order.client_order_id, symbol=order.symbol
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
                logger.debug(f"Error checking order status: {e}")

            await asyncio.sleep(0.1)

        raise TimeoutError(
            f"Order {order.client_order_id} did not reach terminal state within {timeout} seconds"
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
                    limit=50, start_time=start_time, end_time=end_time
                )
                orders = await exchange_api.get_order_history(history_args)

                logger.debug(
                    f"Retrieved {len(orders) if orders else 0} orders from history, looking for {order.exchange_order_id}"
                )

                if orders:
                    # Check if our order is in the history
                    for i, historical_order in enumerate(orders):
                        logger.debug(
                            f"Order {i + 1}: {historical_order.exchange_order_id} (status: {historical_order.status}, symbol: {historical_order.symbol})"
                        )

                        # Compare both as strings to handle type mismatches
                        if str(historical_order.exchange_order_id) == str(order.exchange_order_id):
                            logger.info(
                                f"✅ Order {order.exchange_order_id} found in history at position {i + 1}"
                            )
                            return True
                        if (
                            historical_order.client_order_id
                            and order.client_order_id
                            and str(historical_order.client_order_id) == str(order.client_order_id)
                        ):
                            logger.info(
                                f"✅ Order {order.client_order_id} found in history by client ID at position {i + 1}"
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
                            logger.info(f"Order {order.exchange_order_id} found in trades")
                            return True

            except Exception as e:
                elapsed = int(time.time() - loop_start_time)
                logger.debug(f"Error checking order history after {elapsed}s: {e}")

            # Wait before next attempt
            await asyncio.sleep(poll_interval)

        logger.warning(
            f"verify_order_in_history timed out after {max_wait_seconds}s for order {order.exchange_order_id}"
        )
        return False

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

        for attempt in range(max_retries):
            try:
                logger.debug(
                    f"Attempt {attempt + 1}/{max_retries}: Looking for order {order_id} in history"
                )

                # Check order history first - this is more reliable for all market types
                # Hyperliquid requires start_time and end_time parameters
                from datetime import UTC, datetime, timedelta
                
                end_time = datetime.now(UTC)
                start_time = end_time - timedelta(hours=1)  # Look back 1 hour
                
                history_args = GetOrderHistoryArgs(
                    limit=50,
                    start_time=start_time,
                    end_time=end_time
                )
                orders = await exchange_api.get_order_history(history_args)

                if orders:
                    logger.debug(f"Retrieved {len(orders)} orders from history")
                    for historical_order in orders:
                        # Log order details for debugging
                        logger.debug(
                            f"Checking order: exchange_id={historical_order.exchange_order_id}, "
                            f"client_id={historical_order.client_order_id}, "
                            f"filled={historical_order.quantity_filled}"
                        )

                        if (
                            historical_order.exchange_order_id == order_id
                            or historical_order.client_order_id == order_id
                        ):
                            if (
                                historical_order.quantity_filled
                                and historical_order.quantity_filled > 0
                            ):
                                logger.info(
                                    f"Found order {order_id} with filled quantity: "
                                    f"{historical_order.quantity_filled}"
                                )
                                return historical_order.quantity_filled
                            else:
                                logger.debug(
                                    f"Found order {order_id} but quantity_filled is {historical_order.quantity_filled}"
                                )

                # Also try trades - but handle potential 404s for PERP markets gracefully
                try:
                    trades_args = GetTradeHistoryArgs(limit=50)
                    trades = await exchange_api.get_trade_history(trades_args)

                    if trades:
                        logger.debug(f"Retrieved {len(trades)} trades from history")
                        total_filled = Decimal("0")
                        matching_trades = 0

                        for trade in trades:
                            logger.debug(
                                f"Checking trade: order_id={trade.order_id}, "
                                f"quantity={trade.quantity}, symbol={trade.symbol}"
                            )
                            if trade.order_id == order_id:
                                total_filled += trade.quantity
                                matching_trades += 1

                        if total_filled > 0:
                            logger.info(
                                f"Found {matching_trades} trades for order {order_id} totaling: {total_filled}"
                            )
                            return total_filled
                        else:
                            logger.debug(f"No matching trades found for order {order_id}")

                except Exception as trade_error:
                    # Trades endpoint may not be available for all market types (e.g., PERP markets)
                    logger.debug(f"Trade history access failed for order {order_id}: {trade_error}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_retries - 1:
                    logger.debug(
                        f"Order {order_id} not found in history, waiting {retry_delay}s before retry {attempt + 2}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.warning(
                        f"Order {order_id} not found in history after {max_retries} attempts"
                    )

            except Exception as e:
                logger.error(
                    f"Error getting filled quantity from history (attempt {attempt + 1}): {e}"
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
                                order_id=order.exchange_order_id or order.client_order_id
                            )
                            await exchange_api.cancel_order(cancel_args)
                            logger.info(f"Cancelled open order: {order.exchange_order_id}")
                        except Exception as e:
                            raise RuntimeError(
                                f"Failed to cancel order {order.exchange_order_id}: {e}. "
                                "Order cancellation is critical for test cleanup."
                            ) from e

            # Note: We don't close positions as that might affect other tests
            # or real trading strategies running on the same account

        except Exception as e:
            raise RuntimeError(
                f"Critical error during test cleanup: {e}. "
                "Test cleanup must succeed to prevent interference."
            ) from e
