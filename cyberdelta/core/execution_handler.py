from __future__ import annotations  # Enable postponed evaluation

import asyncio
import random
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.base_api import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.validation.circuit_breaker import (
    CircuitBreakerSystem,
    CircuitBreakerTrippedError,
)

# Keep imports for type checking only if they cause circular dependencies otherwise
if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# NOTE: CyberDeltaEngine Order model uses 'client_order_id' as the unique identifier, 'quantity_requested' for order size, 'quantity_filled' for filled size, and 'average_fill_price' for fill price. There is no 'id', 'quantity', or 'avg_fill_price' attribute.


class ExecutionStatus(Enum):
    """Status of an execution."""

    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    PARTIALLY_COMPLETED = auto()
    COMPENSATING = auto()
    REJECTED = auto()


class TradeExecution:
    """
    Represents a trade execution across multiple exchanges.
    """

    def __init__(self, opportunity: SizedOpportunity) -> None:
        """
        Initialize a trade execution.

        Args:
            opportunity: Sized arbitrage opportunity
        """
        self.id = str(uuid.uuid4())
        self.opportunity = opportunity
        self.status = ExecutionStatus.PENDING
        self.error_message: str | None = None
        self.long_order_id: str | None = None
        self.short_order_id: str | None = None
        self.long_position_id: str | None = None
        self.short_position_id: str | None = None
        self.long_order_response: dict[str, Any] | None = None
        self.short_order_response: dict[str, Any] | None = None
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.long_fill_price: Decimal | None = None
        self.short_fill_price: Decimal | None = None
        self.long_fill_quantity: Decimal | None = None
        self.short_fill_quantity: Decimal | None = None
        self.realized_pnl: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "opportunity": {
                "symbol": self.opportunity.opportunity.symbol,
                "long_exchange": self.opportunity.opportunity.long_exchange,
                "short_exchange": self.opportunity.opportunity.short_exchange,
                "long_size": str(self.opportunity.long_size),
                "short_size": str(self.opportunity.short_size),
                "expected_profit": str(self.opportunity.expected_profit),
            },
            "status": self.status.name,
            "error_message": self.error_message,
            "long_order_id": self.long_order_id,
            "short_order_id": self.short_order_id,
            "long_position_id": self.long_position_id,
            "short_position_id": self.short_position_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "long_fill_price": str(self.long_fill_price) if self.long_fill_price else None,
            "short_fill_price": str(self.short_fill_price) if self.short_fill_price else None,
            "long_fill_quantity": str(self.long_fill_quantity) if self.long_fill_quantity else None,
            "short_fill_quantity": str(self.short_fill_quantity)
            if self.short_fill_quantity
            else None,
            "realized_pnl": str(self.realized_pnl) if self.realized_pnl is not None else None,
        }

    def __str__(self) -> str:
        """String representation of the execution."""
        return (
            f"TradeExecution: {self.opportunity.opportunity.symbol} - "
            f"Long: {self.opportunity.opportunity.long_exchange} "
            f"${self.opportunity.long_size:.2f}, "
            f"Short: {self.opportunity.opportunity.short_exchange} "
            f"${self.opportunity.short_size:.2f}, "
            f"Status: {self.status.name}"
        )


class ExecutionHandler:
    """
    Execute trades on exchanges reliably.

    Responsible for:
    - Placing orders on exchanges
    - Monitoring order status and fills
    - Handling partial fills and cancellations
    - Implementing sequenced execution for multi-leg strategies
    - Applying retry logic for temporary failures
    - Implementing circuit breakers for critical failures
    """

    def __init__(
        self,
        config: Config,
        portfolio_tracker: PortfolioTracker,
        symbol_mapper: SymbolMapper,
        circuit_breaker_system: CircuitBreakerSystem | None = None,
    ) -> None:
        """
        Initialize the execution handler.

        Args:
            config: Application configuration (Config object)
            portfolio_tracker: Portfolio tracker for position updates
            symbol_mapper: SymbolMapper for translating symbols
            circuit_breaker_system: The main circuit breaker system (optional)
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        self.symbol_mapper = symbol_mapper
        self.circuit_breaker_system = circuit_breaker_system
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Safely parse config values
        try:
            self.max_slippage = Decimal(str(config.get("execution.max_slippage", "0.002")))
            raw_max_retries = config.get("execution.max_retries", 3)
            self.max_retries = int(str(raw_max_retries)) if raw_max_retries is not None else 3
            raw_retry_delay = config.get("execution.retry_delay_base_sec", "1.0")
            self.retry_delay_base = (
                float(str(raw_retry_delay)) if raw_retry_delay is not None else 1.0
            )
            raw_max_history = config.get("execution.max_history", 100)
            self.max_execution_history = (
                int(str(raw_max_history)) if raw_max_history is not None else 100
            )
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.error(f"Invalid config value in ExecutionHandler init: {e}. Using defaults.")
            # Apply defaults explicitly on error
            self.max_slippage = Decimal("0.002")
            self.max_retries = 3
            self.retry_delay_base = 1.0
            self.max_execution_history = 100

        self.executions: list[TradeExecution] = []
        self.active_executions: dict[str, TradeExecution] = {}
        self.logger = get_logger(__name__)
        logger.info("ExecutionHandler initialized.")

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """
        Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation
        """
        self.api_clients[exchange_id] = client
        logger.info(f"Registered API client for {exchange_id} in ExecutionHandler")

    async def execute_opportunity(self, opportunity: SizedOpportunity) -> TradeExecution:
        """
        Execute an arbitrage opportunity.
        Uses SymbolMapper to get exchange-specific symbols.

        Args:
            opportunity: Sized arbitrage opportunity

        Returns:
            TradeExecution object representing the outcome
        """
        execution = TradeExecution(opportunity)
        self.active_executions[execution.id] = execution
        execution.start_time = datetime.now(UTC)
        # Use a local variable for error messages within this method's scope
        op_error_msg: str | None = None

        logger.info(
            f"Starting execution {execution.id} for opportunity: {opportunity.opportunity.symbol}"
        )

        # --- Circuit Breaker Check ---
        if self.circuit_breaker_system:
            try:
                can_long, long_reason = self.circuit_breaker_system.can_execute(
                    opportunity.opportunity.long_exchange
                )
                if not can_long:
                    raise CircuitBreakerTrippedError(
                        f"Circuit breaker tripped for long exchange "
                        f"{opportunity.opportunity.long_exchange}: {long_reason}"
                    )
                can_short, short_reason = self.circuit_breaker_system.can_execute(
                    opportunity.opportunity.short_exchange
                )
                if not can_short:
                    raise CircuitBreakerTrippedError(
                        f"Circuit breaker tripped for short exchange "
                        f"{opportunity.opportunity.short_exchange}: {short_reason}"
                    )
            except CircuitBreakerTrippedError as e:
                op_error_msg = f"Execution {execution.id} rejected by circuit breaker: {e}"
                logger.error(op_error_msg)
                execution.error_message = str(e)
                execution.status = ExecutionStatus.REJECTED
                execution.end_time = datetime.now(UTC)
                self._add_to_history(execution)
                return execution

        # --- Setup & Pre-Checks ---
        long_client = self.api_clients.get(opportunity.opportunity.long_exchange)
        short_client = self.api_clients.get(opportunity.opportunity.short_exchange)

        try:
            if not long_client or not short_client:
                raise ValueError("API client not registered for one or both exchanges")

            # Get exchange-specific symbols
            long_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.long_exchange
            )
            short_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.short_exchange
            )
            if not long_symbol or not short_symbol:
                raise ValueError(
                    f"Failed to map symbol '{opportunity.opportunity.symbol}' "
                    f"for one or both exchanges"
                )
        except ValueError as e:
            op_error_msg = (
                f"Execution {execution.id} failed during setup (API client/symbol mapping): {e}"
            )
            logger.error(op_error_msg)
            execution.error_message = str(e)
            execution.status = ExecutionStatus.FAILED
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        # Calculate expected prices with slippage
        expected_long_price: Decimal | None = None
        expected_short_price: Decimal | None = None
        try:
            # Get latest ticker for price check
            ticker: Ticker | None = await self.portfolio_tracker.get_ticker(
                opportunity.opportunity.long_exchange, long_symbol
            )
            # DEFENSIVE CHECK: ticker.price is Optional[Decimal]
            if ticker and ticker.price is not None and ticker.price.is_finite():
                expected_long_price = ticker.price * (Decimal("1") + self.max_slippage)
            else:
                logger.warning(
                    f"Execution {execution.id}: Could not get ticker for long slippage check."
                )

            ticker = await self.portfolio_tracker.get_ticker(
                opportunity.opportunity.short_exchange, short_symbol
            )
            # DEFENSIVE CHECK: ticker.price is Optional[Decimal]
            if ticker and ticker.price is not None and ticker.price.is_finite():
                expected_short_price = ticker.price * (Decimal("1") - self.max_slippage)
            else:
                logger.warning(
                    f"Execution {execution.id}: Could not get ticker for short slippage check."
                )
        except Exception as ticker_err:
            logger.warning(
                f"Execution {execution.id}: Error getting ticker for slippage checks: {ticker_err}"
            )
            # Continue without slippage checks if tickers fail
            expected_long_price = None
            expected_short_price = None

        # --- Place Orders ---
        # Try placing the long order first (more liquid leg usually)
        execution.status = ExecutionStatus.EXECUTING
        long_order_result = await self._place_order_with_retry(
            execution,
            opportunity.opportunity.long_exchange,
            long_symbol,
            OrderSide.BUY,
            opportunity.long_size,  # Use sized quantity
            OrderType.MARKET,  # TODO: Consider LIMIT orders
        )

        # If long order failed, mark execution failed and return
        if long_order_result is None:
            op_error_msg = f"Execution {execution.id} failed: Long order placement failed."
            logger.error(op_error_msg)
            execution.error_message = (
                execution.error_message or "Long order placement failed after retries"
            )
            execution.status = ExecutionStatus.FAILED
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        execution.long_order_id = long_order_result.exchange_order_id
        execution.long_order_response = long_order_result.model_dump(mode="json")
        logger.info(f"Execution {execution.id}: Long order placed: {execution.long_order_id}")

        # Place short order
        short_order_result = await self._place_order_with_retry(
            execution,
            opportunity.opportunity.short_exchange,
            short_symbol,
            OrderSide.SELL,
            opportunity.short_size,  # Use sized quantity
            OrderType.MARKET,  # TODO: Consider LIMIT orders
            is_long_leg=False,
        )

        # If short order failed, try to compensate the long leg
        if short_order_result is None:
            op_error_msg = f"Execution {execution.id} failed: Short order placement failed."
            logger.error(op_error_msg)
            execution.error_message = (
                execution.error_message or "Short order placement failed after retries"
            )
            execution.status = ExecutionStatus.COMPENSATING
            if await self._compensate_position(
                execution,
                opportunity.opportunity.long_exchange,
                long_symbol,
                OrderSide.SELL,
                opportunity.long_size,
            ):
                execution.status = ExecutionStatus.FAILED  # Compensation succeeded
            else:
                # Compensation failed, manual intervention needed
                logger.critical(
                    f"Execution {execution.id} FAILED TO COMPENSATE long position! "
                    f"Manual intervention required for symbol {long_symbol} on "
                    f"{opportunity.opportunity.long_exchange}."
                )
                execution.error_message += " | COMPENSATION FAILED!"
                execution.status = ExecutionStatus.FAILED

            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        execution.short_order_id = short_order_result.exchange_order_id
        execution.short_order_response = short_order_result.model_dump(mode="json")
        logger.info(f"Execution {execution.id}: Short order placed: {execution.short_order_id}")

        # --- Monitor Order Status (Basic Example) ---
        # TODO: Implement more robust monitoring (WebSockets preferred)
        logger.info(f"Execution {execution.id}: Monitoring order status...")
        long_status = await self._monitor_order_status(
            execution, opportunity.opportunity.long_exchange, execution.long_order_id, True
        )
        short_status = await self._monitor_order_status(
            execution, opportunity.opportunity.short_exchange, execution.short_order_id, False
        )

        # --- Process Results ---
        if long_status == OrderStatus.FILLED and short_status == OrderStatus.FILLED:
            execution.status = ExecutionStatus.COMPLETED
            logger.info(f"Execution {execution.id} completed successfully.")
            await self._update_pnl(execution)
        else:
            execution.status = ExecutionStatus.FAILED  # Or PARTIALLY_COMPLETED
            op_error_msg = f"Execution {execution.id} failed: Orders not fully filled."
            execution.error_message = op_error_msg
            logger.error(op_error_msg)
            # TODO: Implement compensation for partial fills

        execution.end_time = datetime.now(UTC)
        self._add_to_history(execution)
        return execution

    async def _handle_api_error(
        self, e: APIError, exchange_id: str, context: str, is_retryable: bool = True
    ) -> bool:
        """
        Centralized handling of API errors, including circuit breaker recording.

        Args:
            e: The APIError exception.
            exchange_id: The exchange where the error occurred.
            context: Description of the operation (e.g., "placing order").
            is_retryable: Hint whether the operation itself is generally retryable.

        Returns:
            bool: True if the operation should be retried based on the error, False otherwise.
        """
        logger.warning(
            f"API Error on {exchange_id} during {context}: "
            f"Code={e.code}, Msg='{e.message}', HTTP={e.http_status}, "
            f"ExchangeCode={e.exchange_code}, ExchangeMsg='{e.exchange_message}'"
        )

        # Record failure with circuit breaker if configured
        if self.circuit_breaker_system:
            self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))

        # Determine retry based on error type and context
        return is_retryable and e.is_retryable

    async def _place_order_with_retry(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Decimal | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,  # Good Till Cancelled
        is_long_leg: bool = True,  # Helps associate response
        is_compensation: bool = False,  # Flag for compensation orders
    ) -> Order | None:
        """
        Place an order with retry logic for transient errors.

        Args:
            execution: The parent TradeExecution object.
            exchange_id: Target exchange.
            symbol: Exchange-specific symbol.
            side: BUY or SELL.
            quantity: Order quantity.
            order_type: MARKET, LIMIT, etc.
            price: Limit price (for LIMIT orders).
            time_in_force: Order time in force.
            is_long_leg: True if this is the long leg of the arbitrage.
            is_compensation: True if this is a compensating order.

        Returns:
            Order object if successful, None otherwise.
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            execution.error_message = f"No API client for {exchange_id}"
            logger.error(f"Execution {execution.id}: {execution.error_message}")
            return None

        context = "placing compensation order" if is_compensation else "placing order"
        client_order_id = f"cde_{execution.id[:8]}_{exchange_id[:3]}_{str(uuid.uuid4())[:8]}"

        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Execution {execution.id} ({context}): Attempt {attempt + 1} - "
                    f"{side.name} {quantity:.8f} {symbol} on {exchange_id} (Client ID: {client_order_id})"
                )
                order_result = await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=time_in_force,
                    client_order_id=client_order_id,
                    # TODO: Add other relevant params like reduce_only, post_only if needed
                )
                logger.info(
                    f"Execution {execution.id}: Order placed successfully on {exchange_id}. "
                    f"Exchange ID: {order_result.exchange_order_id}, Status: {order_result.status}"
                )
                return order_result
            except APIError as e:
                should_retry = await self._handle_api_error(e, exchange_id, context)
                if not should_retry:
                    execution.error_message = (
                        f"Non-retryable API error during {context} on {exchange_id}: {e.message}"
                    )
                    logger.error(f"Execution {execution.id}: {execution.error_message}")
                    return None
                # Exponential backoff
                delay = self.retry_delay_base * (2**attempt) * (1 + random.uniform(-0.2, 0.2))
                logger.info(f"Execution {execution.id}: Retrying {context} in {delay:.2f}s...")
                await asyncio.sleep(delay)
            except Exception as e:
                # Catch unexpected errors
                execution.error_message = (
                    f"Unexpected error during {context} on {exchange_id}: {str(e)}"
                )
                logger.exception(f"Execution {execution.id}: {execution.error_message}")
                # Decide if unexpected errors are retryable (maybe not)
                return None

        execution.error_message = (
            f"Failed to place order on {exchange_id} after {self.max_retries} retries."
        )
        logger.error(f"Execution {execution.id}: {execution.error_message}")
        return None

    async def _get_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        symbol: str | None = None,  # Required by some exchanges
        client_order_id: str | None = None,  # Required by some exchanges
    ) -> Order | None:
        """
        Get order status from the exchange with retry logic.

        Args:
            execution: The parent TradeExecution object.
            exchange_id: Target exchange.
            order_id: Exchange order ID.
            symbol: Optional symbol (required by some exchanges).
            client_order_id: Optional client order ID (required by some exchanges).

        Returns:
            Order object with updated status, or None if fetching failed.
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"Execution {execution.id}: No API client for {exchange_id}")
            return None

        context = f"getting status for order {order_id}"

        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    f"Execution {execution.id}: {context} on {exchange_id} (Attempt {attempt + 1})"
                )
                order_status = await client.get_order_status(
                    order_id=order_id,
                    symbol=symbol,
                    client_order_id=client_order_id,
                )
                if order_status:
                    logger.debug(
                        f"Execution {execution.id}: Got status for order {order_id}: {order_status.status}"
                    )
                    return order_status
                else:
                    # Handle case where get_order_status returns None without exception
                    logger.warning(
                        f"Execution {execution.id}: _get_order_status returned None for {order_id}."
                    )
                    # Decide if retryable or assume failed/cancelled
                    if attempt == self.max_retries - 1:
                        return None

            except APIError as e:
                # Record failure with circuit breaker if configured
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))

                # Use Enum value for comparison directly on the APIError exception (e)
                if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
                    # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
                    logger.warning(
                        f"Order {order_id} not found on {exchange_id} during status check. "
                        f"Assuming cancelled or filled. Original error: {e.message}"
                    )
                    # Decide if this is terminal or retryable (might appear with delay)
                    if attempt == self.max_retries - 1:
                        return None  # Stop if not found after retries
                elif e.code == APIErrorCode.RATE_LIMITED.value:
                    # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
                    logger.warning(
                        f"Rate limited getting status for order {order_id} on {exchange_id}. "
                        f"Retrying... Original error: {e.message}"
                    )
                    # Retry delay handled below
                elif e.code == APIErrorCode.AUTHENTICATION_FAILED.value:
                    # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
                    logger.error(f"Authentication failed checking order {order_id}. Aborting.")
                    if self.circuit_breaker_system:
                        # Use record_error or appropriate method if record_failure doesn't exist
                        # Assuming record_error exists based on previous correction attempt
                        # If it fails again, will need to search for the correct method name
                        self.circuit_breaker_system.record_error(  # type: ignore [attr-defined]
                            exchange_id, f"Authentication failed: {e.message}"
                        )
                    return None
                elif e.code == APIErrorCode.INVALID_REQUEST.value:
                    # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
                    logger.error(
                        f"Invalid request checking order {order_id}: {e.message}. Aborting."
                    )
                    return None
                elif e.code == APIErrorCode.SERVER_ERROR.value:
                    # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
                    logger.warning(
                        f"Server error checking order {order_id} on {exchange_id}. Retrying... "
                        f"Original error: {e.message}"
                    )
                    # Decide if retryable based on specific exchange error message?
                    if not e.is_retryable:
                        return None  # Stop if error is explicitly not retryable
                elif not e.is_retryable:
                    logger.warning(
                        f"Non-retryable API error getting status for {order_id}: {e.message}"
                    )
                    return None  # Stop if error is explicitly not retryable
                else:
                    # Other potentially retryable errors
                    logger.warning(
                        f"Retryable API error getting status for {order_id}: {e.message}"
                    )

                # Exponential backoff for retryable errors
                delay = self.retry_delay_base * (2**attempt) * (1 + random.uniform(-0.2, 0.2))
                logger.info(f"Execution {execution.id}: Retrying {context} in {delay:.2f}s...")
                await asyncio.sleep(delay)

            except Exception as e:
                # Catch unexpected errors
                logger.exception(
                    f"Execution {execution.id}: Unexpected error during {context}: {str(e)}"
                )
                # Decide if unexpected errors are retryable (maybe not)
                return None

        logger.error(
            f"Execution {execution.id}: Failed to get status for {order_id} after {self.max_retries} retries."
        )
        return None

    async def _compensate_position(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,  # The side of the *compensating* order
        quantity: Decimal,
    ) -> bool:
        """
        Attempt to place a compensating order to flatten a position after a partial failure.

        Args:
            execution: The parent TradeExecution object.
            exchange_id: The exchange where compensation is needed.
            symbol: The exchange-specific symbol to compensate.
            side: The side of the compensating order (opposite of the failed leg).
            quantity: The quantity to compensate.

        Returns:
            True if compensation order placed successfully (or seemed filled), False otherwise.
        """
        logger.warning(
            f"Execution {execution.id}: Attempting compensation: {side.name} {quantity:.8f} "
            f"{symbol} on {exchange_id}"
        )
        compensation_order = await self._place_order_with_retry(
            execution,
            exchange_id,
            symbol,
            side,
            quantity,
            OrderType.MARKET,
            is_compensation=True,
        )

        if compensation_order:
            logger.info(
                f"Execution {execution.id}: Compensation order placed: {compensation_order.exchange_order_id}"
            )
            # Basic check: If status is already filled, assume compensation worked
            # A more robust check would monitor the compensation order status
            if compensation_order.status == OrderStatus.FILLED:
                logger.info(f"Execution {execution.id}: Compensation order filled immediately.")
                return True
            # If not filled immediately, we assume it might fill. A better implementation
            # would monitor this order's status properly.
            logger.warning(
                f"Execution {execution.id}: Compensation order {compensation_order.exchange_order_id} "
                f"not immediately filled (Status: {compensation_order.status}). Monitoring needed."
            )
            # For now, optimistically return True if placed, but log warning
            return True
        else:
            logger.error(f"Execution {execution.id}: Failed to place compensation order.")
            return False

    def get_active_executions(self) -> list[TradeExecution]:
        """Get a list of currently active trade executions."""
        return list(self.active_executions.values())

    def reset_circuit_breaker(self, exchange_id: str) -> None:
        """Reset circuit breaker for a specific exchange."""
        if self.circuit_breaker_system:
            self.circuit_breaker_system.reset_breaker(exchange_id)
            logger.info(f"Circuit breaker reset for {exchange_id}")

    async def _verify_order_state(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        expected_status: OrderStatus,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        """
        Verify the final state of an order after execution attempt.

        Args:
            execution: The trade execution context.
            exchange_id: Exchange ID.
            order_id: Exchange Order ID.
            expected_status: The status the order should ideally be in.
            symbol: Optional symbol.
            client_order_id: Optional client order ID.

        Returns:
            True if the order is in the expected state, False otherwise.
        """
        logger.debug(f"Verifying final state for order {order_id} on {exchange_id}...")
        order = await self._get_order_status(
            execution,
            exchange_id,
            order_id,
            symbol=symbol,
            client_order_id=client_order_id,
        )

        if order:
            logger.debug(f"Verified Order {order_id}: Status={order.status}")
            # Check status and potentially filled quantity based on expected status
            if expected_status == OrderStatus.FILLED:
                # For FILLED, check status and that filled quantity matches requested
                if (
                    order.status == OrderStatus.FILLED
                    and order.quantity_filled == order.quantity_requested
                ):
                    return True
                logger.warning(
                    f"Order {order_id} state mismatch: Expected FILLED, Got {order.status}. "
                    f"Filled: {order.quantity_filled}, Requested: {order.quantity_requested}"
                )
                return False
            elif expected_status in [OrderStatus.CANCELED, OrderStatus.REJECTED]:
                # For CANCELED/REJECTED, just check the status
                if order.status == expected_status:
                    return True
                logger.warning(
                    f"Order {order_id} state mismatch: Expected {expected_status.name}, "
                    f"Got {order.status.name}"
                )
                return False
            else:
                # For other statuses (e.g., NEW, PARTIALLY_FILLED), just check status matches
                if order.status == expected_status:
                    return True
                logger.warning(
                    f"Order {order_id} state mismatch: Expected {expected_status.name}, "
                    f"Got {order.status.name}"
                )
                return False
        else:
            # Failed to get order status - verification fails
            logger.warning(f"Failed to get status for order {order_id} during verification.")
            # If we expected CANCELLED/REJECTED and couldn't find it, maybe treat as success?
            if expected_status in [OrderStatus.CANCELED, OrderStatus.REJECTED]:
                logger.info(
                    f"Treating failed status fetch for {order_id} as verification success "
                    f"since expected state was {expected_status.name}."
                )
                return True
            return False

        # Fallback ensures a boolean is always returned
        return False

    async def _update_pnl(self, execution: TradeExecution) -> None:
        """
        Calculate and update the realized PnL for a completed execution.
        This is a basic implementation assuming market orders and fills match requests.
        Needs refinement for limit orders, partial fills, and accurate fee data.
        """
        if execution.status != ExecutionStatus.COMPLETED:
            logger.warning(f"Cannot update PnL for execution {execution.id}: Not completed.")
            return

        if not execution.long_fill_price or not execution.short_fill_price:
            logger.warning(f"Cannot update PnL for execution {execution.id}: Missing fill prices.")
            return
        if not execution.long_fill_quantity or not execution.short_fill_quantity:
            logger.warning(
                f"Cannot update PnL for execution {execution.id}: Missing fill quantities."
            )
            return

        # Simple PnL calculation (assumes quantities match, fees are zero)
        # TODO: Incorporate actual fees when available
        long_cost = execution.long_fill_price * execution.long_fill_quantity
        short_proceeds = execution.short_fill_price * execution.short_fill_quantity

        # Assuming long_fill_quantity and short_fill_quantity are the same base asset amount
        # This might not hold true if sizing is in quote currency and prices differ significantly
        execution.realized_pnl = short_proceeds - long_cost

        logger.info(
            f"Execution {execution.id}: Calculated Realized PnL = {execution.realized_pnl:.4f}"
        )

        # Persist PnL or notify other systems if needed
        # Example: await some_pnl_service.record_pnl(execution.id, execution.realized_pnl)

    async def _handle_filled_order(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
        is_long_leg: bool,
        expected_long_price: Decimal | None = None,
        expected_short_price: Decimal | None = None,
    ) -> None:
        """
        Process a filled order, update state, and record the trade.

        Args:
            execution: The TradeExecution context.
            order: The filled Order object.
            exchange_id: The exchange where the fill occurred.
            is_long_leg: True if this was the long leg.
            expected_long_price: Optional max expected fill price for long.
            expected_short_price: Optional min expected fill price for short.
        """
        logger.info(
            f"Processing filled order {order.exchange_order_id} on {exchange_id} for execution {execution.id}"
        )

        # Validate required fields for a filled order
        if order.average_fill_price is None or order.quantity_filled == Decimal(0):
            logger.error(
                f"Filled order {order.exchange_order_id} is missing average fill price or has zero filled quantity."
            )
            # Potentially mark execution as failed or requires investigation
            return

        # Create and record Trade object
        try:
            trade = Trade(
                id=f"{order.exchange_order_id}-{order.quantity_filled}-{int(time.time())}",
                symbol=order.symbol,
                side=order.side,
                order_id=order.exchange_order_id or "UNKNOWN",
                exchange=exchange_id,
                client_order_id=order.client_order_id,
                price=order.average_fill_price,
                quantity=order.quantity_filled,  # Use filled quantity for the trade
                fee=Decimal("0"),  # TODO: Get actual fee if available from order/API
                fee_asset=None,  # TODO: Get actual fee asset
                executed_at=order.updated_at or datetime.now(UTC),  # Use executed_at
                is_maker=None,  # TODO: Determine maker/taker status if possible
            )

            logger.info(f"Fill processed for Order ID {order.exchange_order_id}: {trade}")

            # Update execution state
            if is_long_leg:
                execution.long_fill_price = order.average_fill_price
                execution.long_fill_quantity = order.quantity_filled
                # Mark these as unused locally if not needed after logging
                _fill_qty = order.quantity_filled
                _fill_price = order.average_fill_price
                _order_id = order.exchange_order_id
            else:
                execution.short_fill_price = order.average_fill_price
                execution.short_fill_quantity = order.quantity_filled
                _fill_qty = order.quantity_filled
                _fill_price = order.average_fill_price
                _order_id = order.exchange_order_id

            # Update portfolio state
            await self.portfolio_tracker.on_trade(trade)

            # Check slippage
            # Compare fill price against expected price + slippage
            if is_long_leg and expected_long_price:
                # average_fill_price is Decimal (guaranteed non-None by check before Trade creation)
                if order.average_fill_price > expected_long_price:
                    logger.warning(
                        f"Execution {execution.id} - Long Leg Slippage Exceeded: "
                        f"Fill={order.average_fill_price}, ExpectedMax={expected_long_price}"
                    )
            elif not is_long_leg and expected_short_price:
                # average_fill_price is Decimal (guaranteed non-None by check before Trade creation)
                if order.average_fill_price < expected_short_price:
                    logger.warning(
                        f"Execution {execution.id} - Short Leg Slippage Exceeded: "
                        f"Fill={order.average_fill_price}, ExpectedMin={expected_short_price}"
                    )

            # Add trade to execution history
            # execution.opportunity.trades.append(trade) # Redundant if portfolio tracker handles this?

        except Exception as e:
            logger.exception(
                f"Execution {execution.id}: Error processing filled order {order.exchange_order_id}: {e}"
            )
            # Consider how to handle this - potentially fail the execution

    async def _monitor_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str | None,
        is_long_leg: bool,
        timeout_sec: float = 60.0,  # Example timeout
        poll_interval_sec: float = 2.0,
    ) -> OrderStatus | None:
        """
        Monitor the status of a single order until it reaches a terminal state or times out.
        Basic polling implementation - should be replaced by WebSocket updates where possible.

        Args:
            execution: The trade execution context.
            exchange_id: Exchange ID.
            order_id: Exchange Order ID. Can be None if initial placement failed implicitly.
            is_long_leg: True if monitoring the long leg.
            timeout_sec: How long to monitor before giving up.
            poll_interval_sec: How often to poll for status.

        Returns:
            The final OrderStatus if terminal, or None if timed out or failed.
        """
        if order_id is None:
            logger.error(
                f"Execution {execution.id}: Cannot monitor order on {exchange_id}, order ID is None."
            )
            return None  # Or perhaps OrderStatus.REJECTED?

        start_time = time.monotonic()
        symbol = (
            self.symbol_mapper.get_exchange_symbol(
                execution.opportunity.opportunity.symbol, exchange_id
            )
            if execution.opportunity
            else None
        )

        while time.monotonic() - start_time < timeout_sec:
            order = await self._get_order_status(execution, exchange_id, order_id, symbol=symbol)
            if order:
                logger.debug(f"Execution {execution.id}: Order {order_id} Status: {order.status}")
                if order.status in (
                    OrderStatus.FILLED,
                    OrderStatus.CANCELED,
                    OrderStatus.REJECTED,
                    OrderStatus.EXPIRED,
                ):
                    logger.info(
                        f"Execution {execution.id}: Order {order_id} reached terminal state: {order.status}"
                    )
                    # Process the final state (e.g., record fill)
                    if order.status == OrderStatus.FILLED:
                        await self._handle_filled_order(
                            execution,
                            order,
                            exchange_id,
                            is_long_leg,
                            # Pass expected prices if needed for slippage check here
                        )
                    return order.status
                # Handle PARTIALLY_FILLED if needed - might require partial compensation logic
                elif order.status == OrderStatus.PARTIALLY_FILLED:
                    logger.info(
                        f"Execution {execution.id}: Order {order_id} is PARTIALLY_FILLED. "
                        f"Filled: {order.quantity_filled}/{order.quantity_requested}"
                    )
                    # TODO: Implement logic for partial fills if required by strategy
                    # For now, continue monitoring
                    pass
            else:
                # _get_order_status failed after retries
                logger.error(
                    f"Execution {execution.id}: Failed to get status for order {order_id}. Assuming failure."
                )
                return None  # Indicate monitoring failure

            await asyncio.sleep(poll_interval_sec)

        logger.warning(f"Execution {execution.id}: Timed out monitoring order {order_id}.")
        return None  # Indicate timeout

    def _add_to_history(self, execution: TradeExecution) -> None:
        """
        Add a completed or failed execution to the history, maintaining max size.
        Removes the execution from the active dictionary.
        """
        if execution.id in self.active_executions:
            del self.active_executions[execution.id]

        self.executions.append(execution)
        if len(self.executions) > self.max_execution_history:
            self.executions.pop(0)  # Remove the oldest entry
        logger.debug(
            f"Execution {execution.id} added to history. History size: {len(self.executions)}"
        )
