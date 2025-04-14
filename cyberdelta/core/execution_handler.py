from __future__ import annotations  # Enable postponed evaluation

import asyncio
import random
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, cast # Added cast for timestamp if needed

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    Trade,
    Ticker,
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
            "long_fill_quantity": str(self.long_fill_quantity)
            if self.long_fill_quantity
            else None,
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
        self.max_slippage: Decimal = Decimal(
            str(config.get("execution.max_slippage", "0.002"))
        )
        self.max_retries: int = config.get("execution.max_retries", 3)
        self.retry_delay_base: float = float(
            config.get("execution.retry_delay_base_sec", "1.0")
        )
        self.max_execution_history: int = config.get("execution.max_history", 100)
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
            f"Starting execution {execution.id} for "
            f"opportunity: {opportunity.opportunity.symbol}"
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
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        # --- Ticker Fetch & Slippage Check (Optional but Recommended) ---
        try:
            if self.config.get("execution.check_slippage", True) and long_client and short_client:
                ticker_tasks = [
                    long_client.get_ticker(long_symbol),
                    short_client.get_ticker(short_symbol),
                ]
                # gather returns list of results or exceptions
                ticker_results: list[Ticker | BaseException] = await asyncio.gather(
                    *ticker_tasks, return_exceptions=True
                )

                long_ticker_result = ticker_results[0]
                short_ticker_result = ticker_results[1]

                long_ticker: Ticker | None = None
                short_ticker: Ticker | None = None

                if isinstance(long_ticker_result, Ticker):
                    long_ticker = long_ticker_result
                elif isinstance(long_ticker_result, BaseException):
                    logger.warning(
                        f"Execution {execution.id}: Failed to fetch long ticker: "
                        f"{long_ticker_result}"
                    )

                if isinstance(short_ticker_result, Ticker):
                    short_ticker = short_ticker_result
                elif isinstance(short_ticker_result, BaseException):
                     logger.warning(
                        f"Execution {execution.id}: Failed to fetch short ticker: "
                        f"{short_ticker_result}"
                    )

                if long_ticker and short_ticker:
                    # Add assert isinstance to help mypy narrow the type
                    assert isinstance(long_ticker, Ticker)
                    assert isinstance(short_ticker, Ticker)
                    # Use correct attribute names 'ask' and 'bid'
                    long_price = long_ticker.ask
                    short_price = short_ticker.bid
                    if long_price is None or short_price is None:
                        raise ValueError("Missing ask/bid price in ticker data")
                    # Basic slippage check (can be more sophisticated)
                    # Example check (commented out):
                    # if (short_price - long_price) / long_price < (
                    #     opportunity.expected_profit_pct - self.max_slippage
                    # ):
                    #     raise ValueError(
                    #         "Potential slippage exceeds tolerance based on current ticker prices"
                    #     )
                else:
                    logger.warning(
                        f"Execution {execution.id}: Ticker data missing for "
                        f"{opportunity.opportunity.symbol}. Cannot verify slippage."
                    )
        except (APIError, ValueError, Exception) as e:
            op_error_msg = (
                f"Execution {execution.id}: Failed during pre-execution checks "
                f"(ticker fetch/slippage): {e}"
            )
            logger.error(op_error_msg)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        # --- Size Validation ---
        try:
            if opportunity.long_size <= Decimal(0) or opportunity.short_size <= Decimal(0):
                raise ValueError(
                    f"Execution {execution.id}: Invalid size for "
                    f"{opportunity.opportunity.symbol}. Long: {opportunity.long_size}, "
                    f"Short: {opportunity.short_size}"
                )
        except ValueError as e:
            op_error_msg = f"Execution {execution.id} failed during setup (invalid size): {e}"
            logger.error(op_error_msg)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        # --- Order Placement ---
        execution.status = ExecutionStatus.EXECUTING
        long_order_result: Order | None = None
        short_order_result: Order | None = None
        compensating_exchange_id: str | None = None
        compensating_symbol: str | None = None
        compensating_side: OrderSide | None = None
        compensating_size: Decimal | None = None

        try:
            execution_mode = self.config.get("execution.mode", "sequential")

            if execution_mode == "sequential":
                logger.info(f"Execution {execution.id}: Placing orders sequentially...")
                # Place long order first
                long_order_result = await self._place_order_with_retry(
                    execution=execution,
                    exchange_id=opportunity.opportunity.long_exchange,
                    symbol=long_symbol,
                    side=OrderSide.BUY,
                    quantity=opportunity.long_size,
                    order_type=OrderType.MARKET,
                    is_long_leg=True,
                )
                if long_order_result and long_order_result.status == OrderStatus.FILLED:
                    logger.info(
                        f"Execution {execution.id}: Long leg filled successfully."
                    )
                    # Proceed to place the short order only if the long leg filled
                    try:
                        short_order_result = await self._place_order_with_retry(
                            execution=execution,
                            exchange_id=opportunity.opportunity.short_exchange,
                            symbol=short_symbol,
                            side=OrderSide.SELL,
                            quantity=opportunity.short_size,
                            order_type=OrderType.MARKET,
                            is_long_leg=False,
                        )
                        if (
                            short_order_result
                            and short_order_result.status == OrderStatus.FILLED
                        ):
                            logger.info(
                                f"Execution {execution.id}: Short leg filled successfully."
                            )
                            execution.status = ExecutionStatus.COMPLETED
                        else:
                            logger.warning(
                                f"Execution {execution.id}: Short leg failed or not filled. "
                                "Attempting compensation for long leg."
                            )
                            # Set up compensation details
                            compensating_exchange_id = opportunity.opportunity.long_exchange
                            compensating_symbol = long_symbol
                            compensating_side = OrderSide.SELL # Opposite of original long
                            # Compensate filled amount
                            if long_order_result and long_order_result.filled_quantity is not None:
                                compensating_size = long_order_result.filled_quantity
                            else:
                                compensating_size = None # Cannot compensate if size unknown
                                logger.error(
                                    f"Execution {execution.id}: Cannot determine compensation "
                                    f"size for long leg (Order: {long_order_result})."
                                )

                            # Handle compensation logic here
                            op_error_msg = (
                                f"Execution {execution.id}: Short leg failed. Long leg needs "
                                "compensation."
                            )
                            execution.status = ExecutionStatus.COMPENSATING
                    except Exception as short_e:
                         logger.exception(
                             f"Execution {execution.id}: Error placing short leg: {short_e}"
                         )
                         op_error_msg = (
                             f"Execution {execution.id}: Error placing short leg. "
                             "Compensation needed."
                         )
                         execution.status = ExecutionStatus.COMPENSATING
                         compensating_exchange_id = opportunity.opportunity.long_exchange
                         compensating_symbol = long_symbol
                         compensating_side = OrderSide.SELL
                         compensating_size = (
                             long_order_result.filled_quantity
                             if long_order_result and long_order_result.filled_quantity is not None
                             else None
                         )
                         if compensating_size is None:
                             logger.error(
                                 f"Execution {execution.id}: Cannot determine compensation size "
                                 f"for long leg after error (Order: {long_order_result})."
                             )


                else:
                    logger.warning(
                        f"Execution {execution.id}: Long leg failed or not filled. "
                        "Aborting execution."
                    )
                    op_error_msg = f"Execution {execution.id}: Long leg failed."
                    execution.status = ExecutionStatus.FAILED

            elif execution_mode == "concurrent":
                logger.info(f"Execution {execution.id}: Placing orders concurrently...")
                long_task = self._place_order_with_retry(
                    execution=execution,
                    exchange_id=opportunity.opportunity.long_exchange,
                    symbol=long_symbol,
                    side=OrderSide.BUY,
                    quantity=opportunity.long_size,
                    order_type=OrderType.MARKET,
                    is_long_leg=True,
                )
                short_task = self._place_order_with_retry(
                    execution=execution,
                    exchange_id=opportunity.opportunity.short_exchange,
                    symbol=short_symbol,
                    side=OrderSide.SELL,
                    quantity=opportunity.short_size,
                    order_type=OrderType.MARKET,
                    is_long_leg=False,
                )
                # gather returns list of results or exceptions
                # Rename variable to avoid clash with ticker_results
                order_results: list[Order | BaseException | None] = await asyncio.gather(
                    long_task, short_task, return_exceptions=True
                )

                long_order_result = order_results[0] if isinstance(order_results[0], Order) else None
                short_order_result = order_results[1] if isinstance(order_results[1], Order) else None

                long_filled = long_order_result and long_order_result.status == OrderStatus.FILLED
                short_filled = short_order_result and short_order_result.status == OrderStatus.FILLED

                if long_filled:
                    logger.info(
                        f"Execution {execution.id}: Long leg filled successfully."
                    )
                else:
                    logger.warning(
                        f"Execution {execution.id}: Long leg failed or not filled "
                        "(concurrent)."
                    )
                    if isinstance(order_results[0], BaseException):
                         logger.error(f"Execution {execution.id}: Long leg error: {order_results[0]}")


                if short_filled:
                    logger.info(
                        f"Execution {execution.id}: Short leg filled successfully."
                    )
                else:
                    logger.warning(
                        f"Execution {execution.id}: Short leg failed or not filled "
                        "(concurrent)."
                    )
                    if isinstance(order_results[1], BaseException):
                         logger.error(f"Execution {execution.id}: Short leg error: {order_results[1]}")


                # Determine final status and compensation needs
                if long_filled and short_filled:
                    # Both filled - success
                    op_error_msg = None  # Clear any previous error
                    execution.status = ExecutionStatus.COMPLETED
                elif long_filled and not short_filled:
                    op_error_msg = (
                        f"Execution {execution.id}: Long leg filled, Short leg failed. "
                        "Compensation needed."
                    )
                    execution.status = ExecutionStatus.COMPENSATING
                    compensating_exchange_id = opportunity.opportunity.long_exchange
                    compensating_symbol = long_symbol
                    compensating_side = OrderSide.SELL
                    compensating_size = (
                        long_order_result.filled_quantity
                        if long_order_result and long_order_result.filled_quantity is not None
                        else None
                    )
                    if compensating_size is None:
                         logger.error(
                             f"Execution {execution.id}: Cannot determine compensation size "
                             f"for long leg (Order: {long_order_result})."
                         )

                elif not long_filled and short_filled:
                    op_error_msg = (
                        f"Execution {execution.id}: Short leg filled, Long leg failed. "
                        "Compensation needed."
                    )
                    execution.status = ExecutionStatus.COMPENSATING
                    compensating_exchange_id = opportunity.opportunity.short_exchange
                    compensating_symbol = short_symbol
                    compensating_side = OrderSide.BUY # Opposite of original short
                    compensating_size = (
                        short_order_result.filled_quantity
                        if short_order_result and short_order_result.filled_quantity is not None
                        else None
                    )
                    if compensating_size is None:
                         logger.error(
                             f"Execution {execution.id}: Cannot determine compensation size "
                             f"for short leg (Order: {short_order_result})."
                         )

                elif not long_filled and not short_filled:
                    op_error_msg = f"Execution {execution.id}: Both legs failed to fill."
                    execution.status = ExecutionStatus.FAILED
                # These should not happen if gather worked correctly
                elif long_order_result is None and isinstance(order_results[0], BaseException):
                    op_error_msg = f"Execution {execution.id}: Long leg failed with exception: {order_results[0]}"
                    execution.status = ExecutionStatus.FAILED
                elif short_order_result is None and isinstance(order_results[1], BaseException):
                    op_error_msg = f"Execution {execution.id}: Short leg failed with exception: {order_results[1]}"
                    execution.status = ExecutionStatus.FAILED
                else: # Should be unreachable
                     op_error_msg = f"Execution {execution.id}: Unknown concurrent execution outcome."
                     execution.status = ExecutionStatus.FAILED

            else:
                raise ValueError(f"Unsupported execution mode: {execution_mode}")

        except (APIError, ValueError, CircuitBreakerTrippedError) as e:
            # Catch potential errors during the placement logic itself
            op_error_msg = f"Execution {execution.id} failed during order placement: {e}"
            logger.exception(op_error_msg) # Log with stack trace
            execution.status = ExecutionStatus.FAILED
            # Determine if compensation is needed based on partial success
            if long_order_result and long_order_result.status == OrderStatus.FILLED:
                 execution.status = ExecutionStatus.COMPENSATING
                 compensating_exchange_id = opportunity.opportunity.long_exchange
                 compensating_symbol = long_symbol
                 compensating_side = OrderSide.SELL
                 if long_order_result.filled_quantity is not None:
                     compensating_size = long_order_result.filled_quantity
                 else:
                     compensating_size = None # Cannot compensate if size unknown
                     logger.error(
                         f"Execution {execution.id}: Cannot determine compensation size "
                         f"for long leg (Order: {long_order_result})."
                     )
                 op_error_msg += " Compensation needed for long leg."
            elif short_order_result and short_order_result.status == OrderStatus.FILLED:
                 execution.status = ExecutionStatus.COMPENSATING
                 compensating_exchange_id = opportunity.opportunity.short_exchange
                 compensating_symbol = short_symbol
                 compensating_side = OrderSide.BUY
                 if short_order_result.filled_quantity is not None:
                     compensating_size = short_order_result.filled_quantity
                 else:
                     compensating_size = None # Cannot compensate if size unknown
                     logger.error(
                         f"Execution {execution.id}: Cannot determine compensation size "
                         f"for short leg (Order: {short_order_result})."
                     )

                 op_error_msg += " Compensation needed for short leg."


        # --- Compensation Logic ---
        if execution.status == ExecutionStatus.COMPENSATING:
            logger.warning(f"Execution {execution.id}: Entering compensation phase. {op_error_msg}")
            if (
                compensating_exchange_id and compensating_symbol and
                compensating_side and compensating_size is not None and
                compensating_size > 0
            ):
                compensating_client = self.api_clients.get(compensating_exchange_id)
                if compensating_client:
                    try:
                        comp_success = await self._compensate_position(
                            execution=execution,
                            exchange_id=compensating_exchange_id,
                            symbol=compensating_symbol,
                            side=compensating_side,
                            quantity=compensating_size,
                        )
                        if not comp_success:
                            logger.warning(
                                f"Execution {execution.id}: Compensation failed for "
                                f"{compensating_exchange_id}."
                            )
                            # TODO: Potentially escalate this failure (e.g., alert)
                            # Mark as failed if compensation fails
                            execution.status = ExecutionStatus.FAILED
                        else:
                             logger.info(f"Execution {execution.id}: Compensation attempt finished.")
                             # Status remains COMPENSATING until verified?
                             # Or move to PARTIALLY_COMPLETED/FAILED?
                             # For now, assume _compensate_position handles logging success/failure
                             pass # Status is already COMPENSATING
                    except Exception as comp_e:
                        logger.exception(
                            f"Execution {execution.id}: Error during compensation call: {comp_e}"
                        )
                        # Mark as failed if compensation errors out
                        execution.status = ExecutionStatus.FAILED
                else:
                    logger.warning(
                        f"Execution {execution.id}: Cannot compensate, API client "
                        f"missing for {compensating_exchange_id}"
                    )
                    execution.status = ExecutionStatus.FAILED # Cannot compensate
            else:
                 logger.error(f"Execution {execution.id}: Missing details required for compensation.")
                 execution.status = ExecutionStatus.FAILED


        # --- Finalization ---
        # Note: Specific errors like APIError, ValueError, CircuitBreaker are handled earlier

        # Final status update and logging
        # Don't overwrite COMPENSATING status here
        if op_error_msg and execution.status != ExecutionStatus.COMPENSATING:
            logger.error(op_error_msg)
            execution.error_message = op_error_msg # Store the formatted error
            if execution.status not in [ExecutionStatus.FAILED, ExecutionStatus.REJECTED]:
                 execution.status = ExecutionStatus.FAILED # Default if error_msg set

        # Update PnL if completed successfully
        if execution.status == ExecutionStatus.COMPLETED:
             await self._update_pnl(execution)


        execution.end_time = datetime.now(UTC)
        self._add_to_history(execution)
        logger.info(
            f"Execution {execution.id} finished with status: {execution.status.name}"
        )
        return execution

    async def _place_order_with_retry(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Decimal | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC, # Good Till Cancelled
        is_long_leg: bool = True, # Helps associate response
        is_compensation: bool = False, # Flag for compensation orders
    ) -> Order | None:
        """Place an order with retry logic."""
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                f"Execution {execution.id}: Failed to get API client for {exchange_id} "
                f"during order placement."
            )
            return None
        if quantity <= Decimal(0):
             logger.error(
                f"Execution {execution.id}: Invalid quantity {quantity} for order placement "
                f"on {exchange_id}."
             )
             return None

        order: Order | None = None
        for attempt in range(self.max_retries):
            client_order_id = (
                f"{'COMP' if is_compensation else 'ARB'}_{execution.id[:8]}_"
                f"{exchange_id[:3]}_{'L' if is_long_leg else 'S'}_{attempt}"
            )
            try:
                logger.debug(
                    f"Execution {execution.id}: Placing order (Attempt {attempt + 1}): "
                    f"{exchange_id} {symbol} {side.name} {quantity} {order_type.name} "
                    f"ClientOID: {client_order_id}"
                )
                # --- Circuit Breaker Check (Per Attempt) ---
                if self.circuit_breaker_system:
                    can_exec, reason = self.circuit_breaker_system.can_execute(exchange_id)
                    if not can_exec:
                        raise CircuitBreakerTrippedError(
                            f"Circuit breaker tripped for {exchange_id} before "
                            f"attempt {attempt + 1}: {reason}"
                        )

                order = await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,  # Optional, for limit orders
                    time_in_force=time_in_force,
                    client_order_id=client_order_id,
                )

                if order:
                    logger.info( # Changed to info as success is expected outcome here
                        f"Execution {execution.id}: Order {order.id if order else 'N/A'} "
                        f"placed successfully on attempt {attempt + 1}."
                    )
                    # Store order response in execution object
                    # Safely access raw_data if the attribute exists on the order object
                    raw_data = getattr(order, 'raw_data', None)
                    if is_long_leg:
                        execution.long_order_id = order.id
                        # Store raw if available
                        execution.long_order_response = raw_data or {}
                    else:
                        execution.short_order_id = order.id
                        execution.short_order_response = raw_data or {}

                    # Check initial status - might be filled immediately
                    if order.status == OrderStatus.FILLED:
                        logger.info( # Changed to info
                            f"Execution {execution.id}: Order {order.id} filled immediately. "
                            f"Status: {order.status.name}"
                        )
                        await self._handle_filled_order(
                            execution, order, exchange_id, is_long_leg
                        )
                    elif order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                         logger.info( # Changed to info
                            f"Execution {execution.id}: Order {order.id} status is "
                            f"{order.status.name}. Monitoring..."
                         )
                         # Monitoring might happen elsewhere or be triggered later
                         pass
                    elif order.status in [
                        OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED
                    ]:
                         logger.warning(
                             f"Execution {execution.id}: Order {order.id} immediately "
                             f"{order.status.name}."
                         )
                         if self.circuit_breaker_system:
                              error_code = APIErrorCode.ORDER_REJECTED # Generic code
                              self.circuit_breaker_system.record_api_error(
                                  exchange_id, str(error_code)
                              )

                    # Return the successfully placed (though maybe not filled) order
                    return order
                else:
                    # Should not happen if place_order doesn't return None on success
                    logger.error(f"Execution {execution.id}: place_order returned None unexpectedly.")
                    # Consider retry?

            except CircuitBreakerTrippedError as e:
                 logger.error(
                     f"Execution {execution.id}: Circuit breaker tripped during place_order: {e}"
                 )
                 # Record failure?
                 if self.circuit_breaker_system: # Should always exist if error is raised
                      # Use record_critical_failure as record_failure doesn't exist
                      self.circuit_breaker_system.record_critical_failure(
                          exchange_id, f"Circuit breaker tripped: {e}"
                      )
                 # Do not retry if circuit breaker is tripped
                 break
            except APIError as e:
                logger.warning(
                    f"Execution {execution.id}: Order placement failed "
                    f"(Attempt {attempt + 1}). Error: {e}"
                )
                # Record API error for circuit breaker
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))
                if not e.is_retryable or attempt == self.max_retries - 1:
                    break  # Don't retry non-retryable errors or after max attempts
                delay = self.retry_delay_base * (2**attempt) + random.uniform(0, 0.1)
                logger.warning(
                    f"Execution {execution.id}: Retrying order placement in "
                    f"{delay:.2f} seconds..."
                )
                await asyncio.sleep(delay)
            except Exception as e:
                logger.exception(
                    f"Execution {execution.id}: Unexpected error during order placement "
                    f"attempt {attempt + 1}: {e}"
                )
                # Record generic failure for circuit breaker
                if self.circuit_breaker_system:
                     # Use record_critical_failure as record_failure doesn't exist
                     self.circuit_breaker_system.record_critical_failure(
                         exchange_id, f"Unexpected placement error: {e}"
                     )
                if attempt == self.max_retries - 1:
                    break # Stop after max attempts on unexpected errors
                # Optional: Retry on unexpected errors? Could be risky.
                delay = self.retry_delay_base * (2**attempt) + random.uniform(0, 0.1)
                await asyncio.sleep(delay)


        if not order or order.status not in [
            OrderStatus.OPEN, OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED
        ]:
             logger.error(
                f"Execution {execution.id}: Order placement failed after {self.max_retries} "
                f"attempts for {symbol} on {exchange_id}."
             )
             # Ensure status reflects failure if no valid order object returned
             return None

        # Should only return None if all retries failed and resulted in no valid order object
        return order


    async def _get_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        symbol: str | None = None, # Required by some exchanges
        client_order_id: str | None = None # Required by some exchanges
    ) -> Order | None:
        """Get order status with retry logic."""
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(f"Execution {execution.id}: API client not found for {exchange_id}")
            return None

        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    f"Attempt {attempt + 1}/{self.max_retries}: Getting status for order "
                    f"{order_id} on {exchange_id}"
                )
                 # --- Circuit Breaker Check (Per Attempt) ---
                if self.circuit_breaker_system:
                    can_exec, reason = self.circuit_breaker_system.can_execute(exchange_id)
                    if not can_exec:
                        raise CircuitBreakerTrippedError(
                            f"Circuit breaker tripped for {exchange_id} before status "
                            f"check attempt {attempt + 1}: {reason}"
                        )

                # Assume client.get_order_status exists and returns Order | None
                # Add type check for client if necessary, though structure implies it exists
                if not hasattr(client, 'get_order_status'):
                     # This error cannot be fixed here as it requires changing ExchangeAPI base class
                     logger.error(
                         f"Client for {exchange_id} lacks get_order_status method. "
                         "Cannot check order status."
                     )
                     return None # Cannot proceed

                order_status_result: Order | None = await client.get_order_status(
                    order_id=order_id,
                    symbol=symbol,  # Pass symbol if required by the API
                    client_order_id=client_order_id,  # Pass if available/needed
                )
                return order_status_result # Return status on success (or None if not found by API)
            except CircuitBreakerTrippedError as e:
                 logger.error(
                     f"Execution {execution.id}: Circuit breaker tripped during "
                     f"get_order_status: {e}"
                 )
                 if self.circuit_breaker_system:
                      # Use record_critical_failure
                      self.circuit_breaker_system.record_critical_failure(
                          exchange_id, f"Circuit breaker tripped: {e}"
                      )
                 break # Do not retry
            except APIError as e:
                logger.warning(
                    f"Execution {execution.id}: Failed to get status for order {order_id} "
                    f"(Attempt {attempt + 1}). Error: {e}"
                )
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))

                if e.code == APIErrorCode.ORDER_NOT_FOUND:
                    logger.warning(
                        f"Execution {execution.id}: Order {order_id} not found on "
                        f"{exchange_id} (Attempt {attempt + 1})."
                    )
                    # Decide if this is terminal or retryable (might appear with delay)
                    if attempt == self.max_retries - 1:
                        break # Stop if not found after retries
                elif e.code == APIErrorCode.RATE_LIMITED: # Corrected enum name
                    logger.warning(
                        f"Execution {execution.id}: Rate limit hit getting status for {order_id}. "
                        f"Retrying..."
                    )
                    # Retry delay handled below
                elif e.code == APIErrorCode.AUTHENTICATION_FAILED: # Corrected enum name
                    logger.warning(
                        f"Execution {execution.id}: Authentication error getting status for "
                        f"{order_id}. Check API keys."
                    )
                    break # Authentication errors are usually not retryable
                elif e.code == APIErrorCode.INVALID_REQUEST: # Corrected enum name
                     logger.warning(
                        f"Execution {execution.id}: Invalid request getting status for {order_id}. "
                        f"Check parameters."
                     )
                     break # Invalid requests are not retryable
                elif e.code == APIErrorCode.SERVER_ERROR: # Using SERVER_ERROR
                     logger.warning(
                        f"Execution {execution.id}: Exchange server error getting status for "
                        f"{order_id}: {e.message}"
                     )
                     # Decide if retryable based on specific exchange error message?
                     if not e.is_retryable:
                         break
                elif not e.is_retryable:
                    break # Stop if error is explicitly not retryable

                if attempt == self.max_retries - 1:
                    break # Stop after max attempts

                delay = self.retry_delay_base * (2**attempt) + random.uniform(0, 0.1)
                logger.warning(
                    f"Execution {execution.id}: Retrying status check in {delay:.2f} seconds..."
                )
                await asyncio.sleep(delay)

            except Exception as e:
                logger.exception(
                    f"Execution {execution.id}: Unexpected error getting status for order "
                    f"{order_id} (Attempt {attempt + 1}): {e}"
                )
                if self.circuit_breaker_system:
                     # Use record_critical_failure
                     self.circuit_breaker_system.record_critical_failure(
                         exchange_id, f"Unexpected status check error: {e}"
                     )
                if attempt == self.max_retries - 1:
                    break
                # Optional: Retry on unexpected errors?
                delay = self.retry_delay_base * (2**attempt) + random.uniform(0, 0.1)
                await asyncio.sleep(delay)

        # Ensure function returns None if loop finishes without success
        logger.error(
            f"Execution {execution.id}: Failed to get status for order {order_id} after "
            f"{self.max_retries} attempts."
        )
        return None

    async def _compensate_position(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide, # The side of the *compensating* order
        quantity: Decimal,
    ) -> bool:
        """Place a compensating order with retry logic."""
        logger.info(
            f"Execution {execution.id}: Attempting compensation: Placing "
            f"{side.name} order for {quantity} {symbol} on {exchange_id}"
        )
        if quantity <= Decimal(0):
             logger.error(f"Execution {execution.id}: Invalid quantity {quantity} for compensation.")
             return False

        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                f"Execution {execution.id}: API client not found for {exchange_id} "
                "for compensation."
            )
            return False

        compensating_order: Order | None = None
        for attempt in range(self.max_retries):
            try:
                 # Use a distinct client order ID for compensation
                 comp_client_order_id = (
                     f"COMP_{execution.id[:8]}_{exchange_id[:3]}_{attempt}"
                 )
                 logger.debug(f"Placing compensation order (Attempt {attempt+1}): {comp_client_order_id}")

                 # --- Circuit Breaker Check (Per Attempt) ---
                 if self.circuit_breaker_system:
                    can_exec, reason = self.circuit_breaker_system.can_execute(exchange_id)
                    if not can_exec:
                        raise CircuitBreakerTrippedError(
                            f"Circuit breaker tripped for {exchange_id} before "
                            f"compensation attempt {attempt + 1}: {reason}"
                        )

                 compensating_order = await self._place_order_with_retry(
                    execution=execution, # Pass execution context
                    exchange_id=exchange_id,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    order_type=OrderType.MARKET, # Usually compensate with MARKET
                    is_long_leg=False, # Doesn't strictly apply, but needed by signature
                    is_compensation=True # Flag might be useful
                 )

                 if compensating_order and compensating_order.status == OrderStatus.FILLED:
                    logger.info(
                        f"Execution {execution.id}: Compensation successful for {exchange_id}."
                    )
                    # Update portfolio tracker for the compensating trade?
                    # await self._handle_filled_order(
                    #    execution, compensating_order, exchange_id, is_long_leg=?
                    # ) # Need to know which leg failed
                    return True
                 elif compensating_order:
                     # Order placed but not filled immediately, needs monitoring
                     logger.warning(
                         f"Execution {execution.id}: Compensation order "
                         f"{compensating_order.id} placed but status is "
                         f"{compensating_order.status.name}. Needs monitoring."
                     )
                     # TODO: Implement monitoring for compensation orders or handle failure explicitly
                     # Treat as failed for now if not immediately filled
                     return False
                 else:
                     # _place_order_with_retry returned None (failed placement)
                     logger.error(
                         f"Execution {execution.id}: _place_order_with_retry failed "
                         "for compensation."
                     )
                     # Retry logic is handled within _place_order_with_retry

            except CircuitBreakerTrippedError as e:
                 logger.error(
                     f"Execution {execution.id}: Circuit breaker tripped during compensation: {e}"
                 )
                 if self.circuit_breaker_system:
                      # Use record_critical_failure
                      self.circuit_breaker_system.record_critical_failure(
                          exchange_id, f"Circuit breaker tripped: {e}"
                      )
                 return False # Do not retry compensation if breaker tripped
            except APIError as e:
                logger.warning(
                    f"Execution {execution.id}: Compensation order placement failed "
                    f"(Attempt {attempt + 1}). Error: {e}"
                )
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))
                if not e.is_retryable or attempt == self.max_retries - 1:
                    break
                delay = self.retry_delay_base * (2**attempt) + random.uniform(0, 0.1)
                logger.warning(
                    f"Execution {execution.id}: Retrying compensation order in "
                    f"{delay:.2f} seconds..."
                )
                await asyncio.sleep(delay)
            except Exception as e:
                logger.exception(
                    f"Execution {execution.id}: Unexpected error during compensation "
                    f"attempt {attempt + 1}: {e}"
                )
                if self.circuit_breaker_system:
                     # Use record_critical_failure
                     self.circuit_breaker_system.record_critical_failure(
                         exchange_id, f"Unexpected compensation error: {e}"
                     )
                if attempt == self.max_retries - 1:
                    break
                delay = self.retry_delay_base * (2**attempt) + random.uniform(0, 0.1)
                await asyncio.sleep(delay)


        logger.error(
            f"Execution {execution.id}: Failed to place compensation order after "
            f"{self.max_retries} attempts for {exchange_id}."
        )
        return False

    # --- Helper Methods ---

    def get_active_executions(self) -> list[TradeExecution]:
        """Get a list of currently active executions."""
        return list(self.active_executions.values())

    def reset_circuit_breaker(self, exchange_id: str) -> None:
        """Manually reset the circuit breaker for an exchange."""
        if self.circuit_breaker_system:
            self.circuit_breaker_system.reset_breaker(exchange_id)
            logger.info(f"Manual reset requested for circuit breaker: {exchange_id}")
        else:
            logger.warning("Circuit breaker system not configured, cannot reset.")

    async def _verify_order_state(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        expected_status: OrderStatus,
        symbol: str | None = None,
        client_order_id: str | None = None
    ) -> bool:
        """Verify the order reached the expected terminal state."""
        logger.debug(f"Verifying final state for order {order_id} on {exchange_id}")
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                f"Execution {execution.id}: Failed to get API client for {exchange_id} "
                f"during verification."
            )
            return False
        try:
            # Pass client_order_id if available
            # Assume client.get_order_status exists and returns Order | None
            if not hasattr(client, 'get_order_status'):
                 # This error cannot be fixed here as it requires changing ExchangeAPI base class
                 logger.error(
                     f"Client for {exchange_id} lacks get_order_status method. "
                     "Cannot verify order state."
                 )
                 return False # Cannot verify

            order: Order | None = await client.get_order_status(
                order_id=order_id,
                symbol=symbol,  # Pass symbol if required
                client_order_id=client_order_id,  # Pass if available
            )
            if not order:
                logger.warning(
                    f"Execution {execution.id}: Order {order_id} not found during "
                    f"verification on {exchange_id}."
                )
                # If expected status was CANCELED/REJECTED, maybe not found is OK? Depends.
                return expected_status in [OrderStatus.CANCELED, OrderStatus.REJECTED]
            if order.status == expected_status:
                logger.debug(f"Order {order_id} verified with status {expected_status.name}")
                return True
            else:
                logger.warning(
                    f"Execution {execution.id}: Order {order_id} status mismatch. "
                    f"Expected {expected_status.name}, got {order.status.name}."
                )
                # Potentially update portfolio tracker with the actual final state
                self.portfolio_tracker.update_order(exchange_id, order)
                return False
        except Exception as e:
            logger.exception(
                f"Execution {execution.id}: Error verifying order {order_id} on "
                f"{exchange_id}: {e}"
            )
            return False

    async def _update_pnl(self, execution: TradeExecution) -> None:
        """Calculate and potentially store PnL for a completed execution."""
        # Requires fill information from both legs
        if execution.status != ExecutionStatus.COMPLETED:
            return

        try:
            logger.debug(f"Execution {execution.id}: Attempting to calculate PnL.")
            # Ensure we have the necessary fill info (might be in execution or response)
            long_id = execution.long_order_id
            short_id = execution.short_order_id
            long_filled_qty_raw = execution.long_fill_quantity or (
                execution.long_order_response.get("filled_quantity")
                if execution.long_order_response else None
            )
            long_avg_price_raw = execution.long_fill_price or (
                 execution.long_order_response.get("avg_fill_price")
                 if execution.long_order_response else None
            )
            short_filled_qty_raw = execution.short_fill_quantity or (
                 execution.short_order_response.get("filled_quantity")
                 if execution.short_order_response else None
            )
            short_avg_price_raw = execution.short_fill_price or (
                 execution.short_order_response.get("avg_fill_price")
                 if execution.short_order_response else None
            )

            if not all([
                long_id,
                short_id,
                long_filled_qty_raw,
                long_avg_price_raw,
                short_filled_qty_raw,
                short_avg_price_raw,
            ]):
                logger.warning(
                    f"Execution {execution.id}: Missing required data for PnL calculation. "
                    f"IDs: ({long_id}, {short_id}), Long Fill: ({long_filled_qty_raw} @ "
                    f"{long_avg_price_raw}), Short Fill: ({short_filled_qty_raw} @ {short_avg_price_raw})"
                )
                return

            # Convert to Decimal, handling potential string inputs from responses
            try:
                long_filled_qty_dec = Decimal(str(long_filled_qty_raw))
                long_avg_price_dec = Decimal(str(long_avg_price_raw))
                short_filled_qty_dec = Decimal(str(short_filled_qty_raw))
                short_avg_price_dec = Decimal(str(short_avg_price_raw))
            except (InvalidOperation, TypeError) as conv_e:
                logger.error(
                    f"Execution {execution.id}: Failed to convert fill data to Decimal: "
                    f"{conv_e}"
                )
                return

            if long_filled_qty_dec != short_filled_qty_dec:
                logger.warning(
                    f"Execution {execution.id}: Mismatched filled quantities "
                    f"({long_filled_qty_dec} vs {short_filled_qty_dec}). "
                    f"PnL calculation might be inaccurate."
                )
                # Using the smaller quantity might be safer for PnL calculation
                # qty_for_pnl = min(long_filled_qty_dec, short_filled_qty_dec)
                # realized_pnl = (short_avg_price_dec - long_avg_price_dec) * qty_for_pnl

            # PnL = (Revenue from Short) - (Cost of Long)
            realized_pnl = (short_avg_price_dec * short_filled_qty_dec) - (
                long_avg_price_dec * long_filled_qty_dec
            ) # Use respective quantities for now

            logger.info(f"Execution {execution.id}: Calculated Realized PnL: {realized_pnl:.4f}")
            execution.realized_pnl = realized_pnl # Store it on the execution object
            # TODO: Store PnL elsewhere if needed (PortfolioTracker, database)
            # self.portfolio_tracker.record_pnl(execution.id, realized_pnl)

        except Exception as e:
            logger.exception(f"Execution {execution.id}: Failed to calculate PnL: {e}")


    async def _handle_filled_order(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
        is_long_leg: bool
    ) -> None:
        """Process a filled order, update execution state, and notify portfolio tracker."""
        logger.info(
            f"Execution {execution.id}: Handling filled order {order.id} on "
            f"{exchange_id} ({'LONG' if is_long_leg else 'SHORT'})"
        )

        # Ensure necessary fill info exists
        if order.filled_quantity is None or order.avg_fill_price is None:
            logger.error(
                f"Execution {execution.id}: Order {order.id} status is FILLED but "
                f"missing fill quantity or price. Cannot process fill."
            )
            return

        # Update TradeExecution object
        fill_qty = order.filled_quantity
        fill_price = order.avg_fill_price
        order_id = order.id

        if is_long_leg:
            execution.long_fill_quantity = fill_qty
            execution.long_fill_price = fill_price
            execution.long_order_id = order_id # Ensure ID is set
        else:
            execution.short_fill_quantity = fill_qty
            execution.short_fill_price = fill_price
            execution.short_order_id = order_id # Ensure ID is set

        # Create and record Trade object
        try:
            trade_id = (
                f"trade_{execution.id[:8]}_{exchange_id[:3]}_"
                f"{'L' if is_long_leg else 'S'}"
            )
            # Safely access optional Order attributes
            fee = getattr(order, 'fee', Decimal(0)) or Decimal(0)
            order_timestamp_dt = getattr(order, 'last_update_time', None) or datetime.now(UTC)

            # Convert datetime to integer timestamp (milliseconds) for Trade model
            order_timestamp_ms: int
            try:
                # Ensure order_timestamp_dt is a datetime object before conversion
                if isinstance(order_timestamp_dt, datetime):
                     order_timestamp_ms = int(order_timestamp_dt.timestamp() * 1000)
                else:
                     # Handle cases where it might be None or something else unexpected
                     logger.warning(
                         f"Execution {execution.id}: Invalid type for order timestamp "
                         f"({type(order_timestamp_dt)}). Using current time."
                     )
                     order_timestamp_ms = int(time.time() * 1000)

            except Exception as ts_err:
                 logger.error(
                     f"Execution {execution.id}: Failed to convert order timestamp "
                     f"{order_timestamp_dt} to milliseconds: {ts_err}"
                 )
                 # Fallback to current time in ms if conversion fails
                 order_timestamp_ms = int(time.time() * 1000)


            trade = Trade(
                id=trade_id,
                order_id=order_id,
                exchange=exchange_id,
                symbol=order.symbol,
                side=order.side,
                quantity=fill_qty,
                price=fill_price,
                fee=fee,
                timestamp=order_timestamp_ms, # Pass integer timestamp
            )
            # self.portfolio_tracker.record_trade(trade) # Assuming this method exists
            logger.debug(f"Execution {execution.id}: Recorded trade {trade.id} for order {order_id}")
        except Exception as e:
            logger.exception(
                f"Execution {execution.id}: Failed to create or record Trade object "
                f"for order {order_id}: {e}"
            )


    async def _monitor_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        is_long_leg: bool,
        timeout_sec: float = 60.0 # Example timeout
    ) -> OrderStatus | None:
        """
        Monitor the status of a single order until it reaches a terminal state or times out.
        Updates the portfolio tracker with intermediate statuses.
        Handles filled orders via _handle_filled_order.
        """
        start_time = time.monotonic()
        # Get symbol and client_order_id from the initial response if possible
        order_response = (
            execution.long_order_response
            if is_long_leg
            else execution.short_order_response
        )
        symbol = order_response.get("symbol") if order_response else None
        client_order_id = order_response.get("client_order_id") if order_response else None

        terminal_states = [
            OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED
        ]

        while time.monotonic() - start_time < timeout_sec:
            try:
                order = await self._get_order_status(
                    execution, exchange_id, order_id, symbol, client_order_id
                )
                if not order:
                    logger.warning(
                        f"Execution {execution.id}: Order {order_id} not found "
                        f"during monitoring."
                    )
                    # Decide if this is terminal or needs retry/timeout
                    # If not found after some time, might be an issue.
                    await asyncio.sleep(self.retry_delay_base * 2) # Wait longer if not found
                    continue

                current_status = order.status
                logger.debug(f"Execution {execution.id}: Order {order_id} status: {current_status.name}")
                # Corrected: Pass exchange_id to update_order
                self.portfolio_tracker.update_order(exchange_id, order)

                if current_status in terminal_states:
                    logger.info(
                        f"Execution {execution.id}: Order {order_id} reached terminal "
                        f"state: {current_status.name}"
                    )
                    if current_status == OrderStatus.FILLED:
                         await self._handle_filled_order(
                             execution, order, exchange_id, is_long_leg
                         )
                    elif current_status in [
                        OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED
                    ]:
                         # Record failure for circuit breaker if appropriate
                         if self.circuit_breaker_system:
                              error_code = APIErrorCode.ORDER_REJECTED # Generic code
                              # Pass error message string
                              self.circuit_breaker_system.record_api_error(
                                  exchange_id, str(error_code)
                              )
                    return current_status
                elif current_status == OrderStatus.PARTIALLY_FILLED:
                    logger.info(
                        f"Execution {execution.id}: Order {order_id} partially filled "
                        f"({order.filled_quantity}/{order.quantity}). "
                        f"Continuing monitoring."
                    )
                    # Update execution state if needed (e.g., store partial fill info)
                    # await self._handle_partial_fill(
                    #    execution, order, exchange_id, is_long_leg
                    # ) # If specific handling needed

                # Wait before checking again
                await asyncio.sleep(self.retry_delay_base)

            except CircuitBreakerTrippedError as e:
                 logger.error(
                     f"Execution {execution.id}: Circuit breaker tripped monitoring "
                     f"order {order_id}: {e}"
                 )
                 return OrderStatus.FAILED # Treat as failure if monitoring stopped by breaker
            except APIError as e:
                 logger.warning(
                     f"Execution {execution.id}: API error monitoring order {order_id}: "
                     f"{e}. Retrying..."
                 )
                 if not e.is_retryable:
                     # E701 fixed: Moved return to its own line
                     return OrderStatus.FAILED # Stop if not retryable
                 await asyncio.sleep(self.retry_delay_base * 2) # Longer delay for API errors
            except Exception as e:
                logger.exception(
                    f"Execution {execution.id}: Unexpected error monitoring order "
                    f"{order_id}: {e}"
                )
                # Decide whether to continue or fail based on the error
                return OrderStatus.FAILED # Fail on unexpected errors during monitoring

        logger.warning(
            f"Execution {execution.id}: Timeout monitoring order {order_id} on {exchange_id}."
        )
        return None # Indicate timeout

    def _add_to_history(self, execution: TradeExecution) -> None:
        """Add execution to history, maintaining max size."""
        if execution.id in self.active_executions:
            del self.active_executions[execution.id]
        self.executions.append(execution)
        if len(self.executions) > self.max_execution_history:
            self.executions.pop(0) # Remove oldest
