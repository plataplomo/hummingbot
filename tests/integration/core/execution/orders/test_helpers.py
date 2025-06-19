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
        # Get real symbol from exchange based on exchange type
        if exchange_name == "hyperliquid":
            # Hyperliquid uses symbols like "BTC", "ETH"
            return "BTC"
        elif exchange_name == "backpack":
            # Backpack uses symbols like "BTC-USDC", "ETH-USDC"
            return "BTC-USDC"
        else:
            raise RuntimeError(f"Unknown exchange: {exchange_name}")

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
            # Get current market price
            ticker = await exchange_api.get_ticker(symbol)
            if not ticker or not ticker.price:
                raise RuntimeError(f"Unable to get current price for {symbol}")

            current_price = ticker.price

            # Get market constraints
            market = await exchange_api.get_market(GetMarketArgs(symbol=symbol))
            if not market:
                raise RuntimeError(f"Unable to get market info for {symbol}")

            min_quantity = market.min_quantity or Decimal("0.001")
            step_size = market.step_size or Decimal("0.001")

            # Calculate quantity for minimum notional value
            # Most exchanges require at least $10-20 notional
            MIN_NOTIONAL = Decimal("20.00")  # $20 for safety
            min_qty_for_notional = MIN_NOTIONAL / current_price

            # Use the larger of exchange minimum or notional minimum
            required_quantity = max(min_quantity, min_qty_for_notional)

            # Round up to step size
            from decimal import ROUND_UP

            steps = (required_quantity / step_size).quantize(Decimal("1"), rounding=ROUND_UP)
            final_quantity = steps * step_size

            # Add 10% buffer for price movements during test
            final_quantity = final_quantity * Decimal("1.1")

            # Ensure we're not using too much for testing (cap at $100 notional)
            max_notional = Decimal("100.00")
            max_quantity = max_notional / current_price

            if final_quantity > max_quantity:
                final_quantity = max_quantity
                # Round down to step size for max
                from decimal import ROUND_DOWN

                steps = (final_quantity / step_size).quantize(Decimal("1"), rounding=ROUND_DOWN)
                final_quantity = steps * step_size

            logger.info(
                f"Minimal test quantity for {symbol}: {final_quantity} "
                f"(price: {current_price}, notional: ${final_quantity * current_price})"
            )

            return final_quantity

        except Exception as e:
            raise RuntimeError(
                f"Failed to calculate minimal test quantity for {symbol}: {e}. "
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
                # Try to get order by ID
                args = GetOrderArgs(order_id=order.exchange_order_id or order.client_order_id)
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
        max_attempts: int = 10,
    ) -> bool:
        """Verify that order appears in trading history.

        Args:
            exchange_api: Exchange API instance
            order: Order to verify
            max_attempts: Maximum number of attempts

        Returns:
            True if order found in history, False otherwise
        """
        for attempt in range(max_attempts):
            try:
                # Get recent order history
                history_args = GetOrderHistoryArgs(limit=20)
                orders = await exchange_api.get_order_history(history_args)

                if orders:
                    # Check if our order is in the history
                    for historical_order in orders:
                        if historical_order.exchange_order_id == order.exchange_order_id:
                            logger.info(f"Order {order.exchange_order_id} found in history")
                            return True
                        if historical_order.client_order_id == order.client_order_id:
                            logger.info(f"Order {order.client_order_id} found in history")
                            return True

                # Also check trades history
                trades_args = GetTradeHistoryArgs(limit=20)
                trades = await exchange_api.get_trade_history(trades_args)

                if trades:
                    for trade in trades:
                        if trade.order_id == order.exchange_order_id:
                            logger.info(f"Order {order.exchange_order_id} found in trades")
                            return True

            except Exception as e:
                logger.debug(f"Error checking order history (attempt {attempt + 1}): {e}")

            # Wait before next attempt
            await asyncio.sleep(0.5)

        return False

    @staticmethod
    async def get_filled_quantity_from_history(
        exchange_api: ExchangeAPI,
        order_id: str,
    ) -> Decimal | None:
        """Get the filled quantity of a previous order from history.

        Args:
            exchange_api: Exchange API instance
            order_id: Order ID to look up

        Returns:
            Filled quantity if found, None otherwise
        """
        try:
            # Check order history
            history_args = GetOrderHistoryArgs(limit=50)
            orders = await exchange_api.get_order_history(history_args)

            if orders:
                for historical_order in orders:
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

            # Also check trades
            trades_args = GetTradeHistoryArgs(limit=50)
            trades = await exchange_api.get_trade_history(trades_args)

            if trades:
                total_filled = Decimal("0")
                for trade in trades:
                    if trade.order_id == order_id:
                        total_filled += trade.quantity

                if total_filled > 0:
                    logger.info(f"Found trades for order {order_id} totaling: {total_filled}")
                    return total_filled

        except Exception as e:
            logger.error(f"Error getting filled quantity from history: {e}")

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
                            logger.debug(f"Error cancelling order: {e}")

            # Note: We don't close positions as that might affect other tests
            # or real trading strategies running on the same account

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
