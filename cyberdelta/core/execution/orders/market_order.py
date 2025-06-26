"""Market Order Executor.

This module implements the main MarketOrder class that executes market orders
using aggressive IoC (Immediate-or-Cancel) limit orders.
"""

import asyncio
from decimal import Decimal

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.execution.orders.market_order_config import MarketOrderConfig
from cyberdelta.core.execution.orders.market_order_errors import MarketOrderError
from cyberdelta.core.execution.orders.market_order_service import MarketOrderService
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType, TimeInForce


logger = get_logger(__name__)


class MarketOrder:
    """Executes market orders using aggressive IoC limit orders.

    This class provides the main interface for executing market orders
    on exchanges that don't support native market orders (like Hyperliquid).
    It uses aggressive pricing with IoC limit orders to simulate market order behavior.
    """

    def __init__(
        self,
        exchange_api: ExchangeAPI,
        market_order_service: MarketOrderService,
        config: MarketOrderConfig | None = None,
    ) -> None:
        """Initialize MarketOrder executor.

        Args:
            exchange_api: Exchange API instance for order placement
            market_order_service: Service for calculating aggressive prices
            config: Market order configuration
        """
        self._exchange = exchange_api
        self._market_order_service = market_order_service
        self._config = config or MarketOrderConfig()

    async def execute_market_order(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        max_slippage: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        """Execute a market order with safety checks.

        This method:
        1. Validates market orders are enabled
        2. Calculates aggressive price using order book data
        3. Places an IoC limit order at the aggressive price
        4. Returns the order result

        Args:
            symbol: Trading symbol
            side: Order side (BUY/SELL)
            quantity: Order quantity
            max_slippage: Optional maximum slippage override
            client_order_id: Optional client order ID

        Returns:
            Order: Executed order with status (FILLED/PARTIALLY_FILLED/CANCELLED)

        Raises:
            MarketOrderError: If market orders are disabled or execution fails
            InsufficientLiquidityError: If insufficient liquidity
            PriceDeviationError: If price deviation exceeds limits
            APIError: If API calls fail
        """
        # 1. Validate configuration
        if not self._config.enabled:
            raise MarketOrderError("Market orders are disabled in configuration")

        logger.info(
            f"Executing market order: {side.value} {quantity} {symbol} "
            f"(max_slippage: {max_slippage})",
        )

        try:
            # 2. Round quantity to exchange step size
            rounded_quantity = await self._market_order_service.round_to_step_size(quantity, symbol)

            # 3. Calculate aggressive price
            aggressive_price = await self._market_order_service.calculate_aggressive_price(
                symbol=symbol,
                side=side,
                quantity=rounded_quantity,
                max_slippage=max_slippage,
            )

            logger.info(
                f"Calculated aggressive price for {symbol}: {aggressive_price} "
                f"(side: {side.value}, quantity: {quantity} -> {rounded_quantity})",
            )

            # 4. Prepare IoC limit order
            order_args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=OrderType.LIMIT,  # Using limit order with IoC
                quantity=rounded_quantity,
                price=aggressive_price,
                time_in_force=TimeInForce.IOC,  # Immediate-or-Cancel
                client_order_id=client_order_id,
                reduce_only=False,  # Market orders typically aren't reduce-only
            )

            # 4. Place the order with timeout
            order = await asyncio.wait_for(
                self._exchange.place_order(order_args),
                timeout=self._config.order_timeout_seconds,
            )

            # 5. Log execution result
            self._log_execution_result(order)

            return order

        except TimeoutError as e:
            logger.error(
                "market_order_timeout",
                action="execute",
                timeout_seconds=self._config.order_timeout_seconds,
                message=f"Market order timed out after {self._config.order_timeout_seconds}s",
            )
            raise MarketOrderError(
                f"Market order timed out after {self._config.order_timeout_seconds}s",
            ) from e
        except Exception as e:
            logger.error(
                "market_order_execution_failed",
                action="execute",
                error=str(e),
                message=f"Market order execution failed: {e}",
                exc_info=True,
            )
            raise

    def _log_execution_result(self, order: Order) -> None:
        """Log the execution result for monitoring.

        Args:
            order: Executed order
        """
        if order.status == OrderStatus.FILLED:
            fill_price = order.price or "unknown"
            logger.info(
                f"Market order FILLED: {order.symbol} {order.side.value} "
                f"{order.quantity_requested} @ {fill_price}",
            )
        elif order.status == OrderStatus.PARTIALLY_FILLED:
            filled = order.quantity_filled or Decimal(0)
            fill_rate = (
                (filled / order.quantity_requested * 100) if order.quantity_requested > 0 else 0
            )
            logger.warning(
                f"Market order PARTIALLY FILLED: {order.symbol} {order.side.value} "
                f"{filled}/{order.quantity_requested} ({fill_rate:.1f}%)",
            )
        else:
            logger.warning(
                f"Market order NOT FILLED: {order.symbol} {order.side.value} "
                f"{order.quantity_requested} - status: {order.status.value}",
            )

    async def execute_market_order_with_retry(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        max_slippage: Decimal | None = None,
        client_order_id: str | None = None,
        max_retries: int = 1,
    ) -> Order:
        """Execute market order with retry logic for partial fills.

        Args:
            symbol: Trading symbol
            side: Order side
            quantity: Order quantity
            max_slippage: Optional maximum slippage
            client_order_id: Optional client order ID
            max_retries: Maximum number of retries for partial fills

        Returns:
            Order: Final order result

        Raises:
            Same as execute_market_order
        """
        remaining_quantity = quantity
        total_filled = Decimal(0)
        orders: list[Order] = []

        for attempt in range(max_retries + 1):
            if remaining_quantity <= Decimal(0):
                break

            logger.info(
                f"Market order attempt {attempt + 1}/{max_retries + 1}: "
                f"remaining quantity {remaining_quantity}",
            )

            order = await self.execute_market_order(
                symbol=symbol,
                side=side,
                quantity=remaining_quantity,
                max_slippage=max_slippage,
                client_order_id=f"{client_order_id}_{attempt}" if client_order_id else None,
            )

            orders.append(order)

            if order.quantity_filled:
                total_filled += order.quantity_filled
                remaining_quantity -= order.quantity_filled

            if order.status == OrderStatus.FILLED:
                break

        # Return the last order with updated total filled quantity
        if orders:
            final_order = orders[-1]
            # Update the quantity_filled to reflect total across all attempts
            if total_filled > Decimal(0):
                final_order.quantity_filled = total_filled
            return final_order

        raise MarketOrderError("No orders were executed")

    def validate_order_parameters(self, symbol: str, side: OrderSide, quantity: Decimal) -> None:
        """Validate order parameters before execution.

        Args:
            symbol: Trading symbol
            side: Order side
            quantity: Order quantity

        Raises:
            ValueError: If parameters are invalid
        """
        if not symbol:
            raise ValueError("Symbol must be a non-empty string")

        if quantity <= Decimal(0):
            raise ValueError("Quantity must be a positive Decimal")

        if not quantity.is_finite():
            raise ValueError("Quantity must be finite")
