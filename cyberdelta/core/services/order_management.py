"""Order management service for ExecutionHandler refactoring.

This module provides centralized order placement, monitoring, and cancellation
with retry logic, error handling, and circuit breaker integration.
"""

from __future__ import annotations

import asyncio
import random
import time
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
    PositionIntent,
)
from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.models import Order, OrderStatus
from cyberdelta.core.services.interfaces import (
    BaseAsyncService,
    ExecutionError,
    ExecutionErrorType,
    ExecutionResult,
    IErrorHandler,
    IOrderService,
    OrderRequest,
    OrderServiceConfig,
)
from cyberdelta.core.symbols.api import symbol
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


class OrderManagementService(BaseAsyncService, IOrderService):
    """Service for managing order operations with retry logic and error handling."""

    def __init__(
        self,
        api_clients: dict[str, ExchangeAPI],
        error_handler: IErrorHandler,
        circuit_breaker: CircuitBreakerSystem | None = None,
        config: OrderServiceConfig | None = None,
        logger: TraceLevelLogger | None = None,
    ) -> None:
        """Initialize order management service.

        Args:
            api_clients: Dictionary of exchange API clients
            error_handler: Error handling service
            circuit_breaker: Optional circuit breaker system
            config: Service configuration
            logger: Optional logger instance
        """
        super().__init__(logger)
        self.api_clients = api_clients
        self.error_handler = error_handler
        self.circuit_breaker = circuit_breaker
        self.config = config or OrderServiceConfig()
        self.logger = logger or get_logger(__name__)

    async def place_order_with_retry(self, request: OrderRequest) -> ExecutionResult:
        """Place order with retry logic and error handling.

        Args:
            request: Order placement request

        Returns:
            ExecutionResult with order information or error details
        """
        # Check circuit breaker
        circuit_breaker_result = self._check_circuit_breaker(request.exchange_id)
        if circuit_breaker_result:
            return circuit_breaker_result

        # Validate API client exists
        api_client = self.api_clients.get(request.exchange_id)
        if not api_client:
            return await self.error_handler.handle_validation_error(
                f"No API client available for exchange: {request.exchange_id}",
                {"exchange_id": request.exchange_id},
            )

        # Execute retry loop (api_client is guaranteed to be non-None here)
        return await self._execute_order_with_retries(request, api_client)

    def _check_circuit_breaker(self, exchange_id: str) -> ExecutionResult | None:
        """Check circuit breaker and return error result if tripped.

        Args:
            exchange_id: Exchange identifier to check

        Returns:
            ExecutionResult with error if circuit breaker is tripped, None otherwise
        """
        if not self.circuit_breaker:
            return None

        can_execute, reason = self.circuit_breaker.can_execute(exchange_id)
        if not can_execute:
            return ExecutionResult.error_result(
                ExecutionError(
                    error_type=ExecutionErrorType.CIRCUIT_BREAKER_ERROR,
                    message=reason or "Circuit breaker active",
                    details={
                        "exchange_id": exchange_id,
                        "operation": "order placement",
                    },
                    recoverable=True,
                    retry_suggested=False,
                    exchange_id=exchange_id,
                )
            )
        return None

    async def _execute_order_with_retries(
        self, request: OrderRequest, api_client: ExchangeAPI
    ) -> ExecutionResult:
        """Execute order placement with retry logic.

        Args:
            request: Order placement request
            api_client: Exchange API client

        Returns:
            ExecutionResult with order information or error details
        """
        start_time = time.time()
        last_exception = None

        # Retry loop with exponential backoff
        for attempt in range(self.config.max_retries + 1):
            try:
                # Attempt order placement
                result = await self._attempt_order_placement(
                    request, api_client, attempt, start_time
                )
                if result:
                    return result

            except APIError as e:
                last_exception = e
                retry_result = await self._handle_api_error_retry(e, request, attempt)
                if retry_result:
                    return retry_result

            except (ValueError, TypeError, AttributeError, ConnectionError) as e:
                # For unexpected errors, don't retry
                return await self.error_handler.handle_system_error(
                    e, "order placement", recoverable=False
                )

        # All retries exhausted
        return await self._handle_retries_exhausted(last_exception, request.exchange_id)

    async def _attempt_order_placement(
        self, request: OrderRequest, api_client: ExchangeAPI, attempt: int, start_time: float
    ) -> ExecutionResult | None:
        """Attempt a single order placement.

        Args:
            request: Order placement request
            api_client: Exchange API client
            attempt: Current attempt number
            start_time: Start time of the operation

        Returns:
            ExecutionResult with order information if successful, None if failed
        """
        self.logger.info(
            "Placing order",
            exchange_id=request.exchange_id,
            symbol=request.symbol,
            side=request.side,
            quantity=str(request.quantity),
            order_type=request.order_type,
            attempt=attempt + 1,
            max_retries=self.config.max_retries + 1,
        )

        # Place order through API client
        order = await self._place_order_internal(api_client, request)

        # Log success and update circuit breaker
        if self.circuit_breaker:
            self.circuit_breaker.record_api_success(request.exchange_id, "place_order")

        elapsed_time = time.time() - start_time
        self.logger.info(
            "Order placed successfully",
            exchange_id=request.exchange_id,
            order_id=order.client_order_id,
            symbol=request.symbol,
            elapsed_time=elapsed_time,
            attempts=attempt + 1,
        )

        return ExecutionResult.success_result(order)

    async def _handle_api_error_retry(
        self, error: APIError, request: OrderRequest, attempt: int
    ) -> ExecutionResult | None:
        """Handle API error and determine if retry should occur.

        Args:
            error: The API error that occurred
            request: Order placement request
            attempt: Current attempt number

        Returns:
            ExecutionResult with error details if no retry should occur, None to continue retrying
        """
        self.logger.warning(
            "Order placement failed",
            exchange_id=request.exchange_id,
            symbol=request.symbol,
            attempt=attempt + 1,
            error=str(error),
            error_code=getattr(error, "code", "unknown"),
        )

        # Check if error is recoverable
        if not self._is_retryable_error(error):
            return await self.error_handler.handle_api_error(
                error, "order placement", request.exchange_id
            )

        # Apply exponential backoff if we have more retries
        if attempt < self.config.max_retries:
            delay = self._calculate_retry_delay(attempt)
            self.logger.info(
                "Retrying order placement",
                exchange_id=request.exchange_id,
                delay_seconds=delay,
                next_attempt=attempt + 2,
            )
            await asyncio.sleep(delay)

        return None  # Continue with retries

    async def _handle_retries_exhausted(
        self, last_exception: Exception | None, exchange_id: str
    ) -> ExecutionResult:
        """Handle the case when all retries are exhausted.

        Args:
            last_exception: The last exception that occurred
            exchange_id: Exchange identifier

        Returns:
            ExecutionResult with error details for exhausted retries
        """
        if isinstance(last_exception, APIError):
            return await self.error_handler.handle_api_error(
                last_exception, "order placement (retries exhausted)", exchange_id
            )
        return await self.error_handler.handle_system_error(
            last_exception or Exception("Unknown error"),
            "order placement (retries exhausted)",
            recoverable=False,
        )

    async def get_order_status(
        self,
        order_id: str,
        exchange_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionResult:
        """Get order status with proper error handling.

        Args:
            order_id: Order identifier
            exchange_id: Exchange identifier
            symbol: Optional symbol for context
            client_order_id: Optional client order ID

        Returns:
            ExecutionResult with order status or error details
        """
        # Check circuit breaker
        if self.circuit_breaker:
            can_execute, reason = self.circuit_breaker.can_execute(exchange_id)
            if not can_execute:
                return ExecutionResult.error_result(
                    ExecutionError(
                        error_type=ExecutionErrorType.CIRCUIT_BREAKER_ERROR,
                        message=reason or "Circuit breaker active",
                        details={"exchange_id": exchange_id, "operation": "order status check"},
                        recoverable=True,
                        retry_suggested=False,
                        exchange_id=exchange_id,
                    )
                )

        # Validate API client exists
        api_client = self.api_clients.get(exchange_id)
        if not api_client:
            return await self.error_handler.handle_validation_error(
                f"No API client available for exchange: {exchange_id}",
                {"exchange_id": exchange_id, "order_id": order_id},
            )

        try:
            self.logger.debug(
                "Getting order status", exchange_id=exchange_id, order_id=order_id, symbol=symbol
            )

            # Get order status through API client
            order = await self._get_order_status_internal(
                api_client, order_id, symbol, client_order_id
            )

            # Log success and update circuit breaker
            if self.circuit_breaker:
                self.circuit_breaker.record_api_success(exchange_id, "get_order_status")

            return ExecutionResult.success_result(order)

        except APIError as e:
            return await self.error_handler.handle_api_error(e, "order status check", exchange_id)
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            return await self.error_handler.handle_system_error(
                e, "order status check", recoverable=True
            )

    async def cancel_order(
        self,
        order_id: str,
        exchange_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionResult:
        """Cancel an existing order.

        Args:
            order_id: Order identifier
            exchange_id: Exchange identifier
            symbol: Optional symbol for context
            client_order_id: Optional client order ID

        Returns:
            ExecutionResult with cancellation status
        """
        # Check circuit breaker
        if self.circuit_breaker:
            can_execute, reason = self.circuit_breaker.can_execute(exchange_id)
            if not can_execute:
                return ExecutionResult.error_result(
                    ExecutionError(
                        error_type=ExecutionErrorType.CIRCUIT_BREAKER_ERROR,
                        message=reason or "Circuit breaker active",
                        details={"exchange_id": exchange_id, "operation": "order cancellation"},
                        recoverable=True,
                        retry_suggested=False,
                        exchange_id=exchange_id,
                    )
                )

        # Validate API client exists
        api_client = self.api_clients.get(exchange_id)
        if not api_client:
            return await self.error_handler.handle_validation_error(
                f"No API client available for exchange: {exchange_id}",
                {"exchange_id": exchange_id, "order_id": order_id},
            )

        try:
            self.logger.info(
                "Cancelling order", exchange_id=exchange_id, order_id=order_id, symbol=symbol
            )

            # Cancel order through API client
            result = await self._cancel_order_internal(
                api_client, order_id, symbol, client_order_id
            )

            # Log success and update circuit breaker
            if self.circuit_breaker:
                self.circuit_breaker.record_api_success(exchange_id, "cancel_order")

            return ExecutionResult.success_result(result)

        except APIError as e:
            return await self.error_handler.handle_api_error(e, "order cancellation", exchange_id)
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            return await self.error_handler.handle_system_error(
                e, "order cancellation", recoverable=True
            )

    async def monitor_order_until_terminal(
        self,
        order_id: str,
        exchange_id: str,
        timeout_seconds: float = 60.0,
        poll_interval: float = 2.0,
    ) -> ExecutionResult:
        """Monitor order until it reaches a terminal state.

        Args:
            order_id: Order identifier
            exchange_id: Exchange identifier
            timeout_seconds: Maximum time to wait
            poll_interval: Polling interval in seconds

        Returns:
            ExecutionResult with final order state or timeout error
        """
        start_time = time.time()

        self.logger.info(
            "Starting order monitoring",
            exchange_id=exchange_id,
            order_id=order_id,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
        )

        while time.time() - start_time < timeout_seconds:
            # Get current order status
            status_result = await self.get_order_status(order_id, exchange_id)
            if not status_result.success:
                return status_result

            order = status_result.data
            if not isinstance(order, Order):
                return await self.error_handler.handle_system_error(
                    Exception(f"Invalid order data type: {type(order)}"),
                    "order monitoring",
                    recoverable=False,
                )

            # Check if order is in terminal state
            if order.status in {
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
            }:
                elapsed_time = time.time() - start_time
                self.logger.info(
                    "Order reached terminal state",
                    exchange_id=exchange_id,
                    order_id=order_id,
                    final_status=order.status,
                    elapsed_time=elapsed_time,
                )
                return ExecutionResult.success_result(order)

            # Wait before next poll
            await asyncio.sleep(poll_interval)

        # Timeout reached
        elapsed_time = time.time() - start_time
        return await self.error_handler.handle_timeout_error(
            "order monitoring",
            timeout_seconds,
            {"order_id": order_id, "exchange_id": exchange_id, "elapsed_time": elapsed_time},
        )

    async def _place_order_internal(self, api_client: ExchangeAPI, request: OrderRequest) -> Order:
        """Internal method to place order through API client.

        Args:
            api_client: Exchange API client
            request: Order placement request

        Returns:
            Order object from successful placement
        """
        # Prepare order arguments based on the API client interface
        order_args = {
            "symbol": request.symbol,
            "side": request.side,
            "quantity": request.quantity,
            "order_type": request.order_type,
            "price": request.price,
            "time_in_force": request.time_in_force,
            "reduce_only": request.reduce_only,
            "post_only": request.post_only,
            "client_order_id": request.client_order_id,
        }

        # Remove None values to avoid API issues
        filtered_args = {k: v for k, v in order_args.items() if v is not None}

        # Create Symbol from string symbol
        # TODO: OrderRequest should be updated to use Symbol
        exchange_name = (
            ExchangeName.HYPERLIQUID
            if api_client.exchange_name == "hyperliquid"
            else ExchangeName.BACKPACK
        )
        exchange_symbol = symbol(value=str(filtered_args["symbol"]), exchange=exchange_name)

        place_order_args = PlaceOrderArgs(
            symbol=exchange_symbol,
            side=OrderSide(filtered_args["side"]),
            order_type=OrderType(filtered_args["order_type"]),
            quantity=Decimal(str(filtered_args["quantity"])),
            time_in_force=TimeInForce(filtered_args["time_in_force"]),
            price=Decimal(str(filtered_args["price"]))
            if filtered_args.get("price") is not None
            else None,
            execution=OrderExecution(
                position_intent=(
                    PositionIntent.REDUCE_ONLY
                    if filtered_args.get("reduce_only")
                    else PositionIntent.OPEN_OR_INCREASE
                ),
                liquidity_requirement=(
                    LiquidityRequirement.POST_ONLY
                    if filtered_args.get("post_only")
                    else LiquidityRequirement.ANY
                ),
            ),
            client_order_id=str(filtered_args["client_order_id"])
            if filtered_args.get("client_order_id") is not None
            else None,
        )

        # Place order through API client
        return await api_client.place_order(place_order_args)

    async def _get_order_status_internal(
        self,
        api_client: ExchangeAPI,
        order_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        """Internal method to get order status through API client.

        Args:
            api_client: Exchange API client
            order_id: Order identifier
            symbol: Optional symbol
            client_order_id: Optional client order ID

        Returns:
            Order object with current status

        Raises:
            APIError: If status check fails
        """
        # Prepare arguments for status check
        # Create Symbol from string if symbol is provided
        exchange_symbol = None
        if symbol:
            exchange_name = (
                ExchangeName.HYPERLIQUID
                if api_client.exchange_name == "hyperliquid"
                else ExchangeName.BACKPACK
            )
            exchange_symbol = symbol(value=str(symbol), exchange=exchange_name)

        get_order_args = GetOrderArgs(
            order_id=order_id, symbol=exchange_symbol, client_order_id=client_order_id
        )

        # Get order status through API client
        result = await api_client.get_order_status(get_order_args)
        if result is None:
            msg = "Order not found"
            raise APIError(msg, "ORDER_NOT_FOUND")
        return result

    async def _cancel_order_internal(
        self,
        api_client: ExchangeAPI,
        order_id: str,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        """Internal method to cancel order through API client.

        Args:
            api_client: Exchange API client
            order_id: Order identifier
            symbol: Optional symbol
            client_order_id: Optional client order ID

        Returns:
            True if cancellation was successful
        """
        # Prepare arguments for cancellation
        # Create Symbol from string if symbol is provided
        exchange_symbol = None
        if symbol:
            exchange_name = (
                ExchangeName.HYPERLIQUID
                if api_client.exchange_name == "hyperliquid"
                else ExchangeName.BACKPACK
            )
            exchange_symbol = symbol(value=str(symbol), exchange=exchange_name)

        cancel_order_args = CancelOrderArgs(
            order_id=order_id, symbol=exchange_symbol, client_order_id=client_order_id
        )

        # Cancel order through API client
        result = await api_client.cancel_order(cancel_order_args)
        return result.success

    def _is_retryable_error(self, error: APIError) -> bool:
        """Check if an API error is retryable.

        Args:
            error: The API error to check

        Returns:
            True if the error can be retried
        """
        # Use the error's built-in retryable check if available
        if hasattr(error, "is_retryable"):
            return error.is_retryable

        # Fallback to checking error patterns
        error_message = str(error).lower()
        retryable_patterns = [
            "timeout",
            "rate limit",
            "temporarily unavailable",
            "try again",
            "server error",
            "connection",
            "network",
        ]

        return any(pattern in error_message for pattern in retryable_patterns)

    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay for retry attempts.

        Args:
            attempt: The current attempt number (0-based)

        Returns:
            Delay in seconds
        """
        # Exponential backoff with jitter
        base_delay = self.config.retry_delay_base_seconds
        max_delay = self.config.max_retry_delay_seconds

        # Calculate exponential delay
        delay = base_delay * (2**attempt)

        # Apply max delay cap
        delay = min(delay, max_delay)

        # Add small amount of jitter to prevent thundering herd
        jitter = delay * 0.1 * random.random()  # noqa: S311

        return float(delay + jitter)
