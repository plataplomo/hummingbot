from __future__ import annotations  # Enable postponed evaluation

import asyncio
import random
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
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
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem, CircuitBreakerTrippedError

# Keep imports for type checking only if they cause circular dependencies otherwise
if TYPE_CHECKING:
    # Example: If importing PortfolioTracker directly caused issues
    # from cyberdelta.core.portfolio_tracker import PortfolioTracker
    pass  # Keep this block if needed for other type-checking-only imports

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

        # Orders
        self.long_order_id: str | None = None
        self.short_order_id: str | None = None

        # Positions - Assuming position IDs are strings
        self.long_position_id: str | None = None
        self.short_position_id: str | None = None

        # Order response details
        self.long_order_response: dict[str, Any] | None = None
        self.short_order_response: dict[str, Any] | None = None

        # Timestamps
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None

        # Execution details - Use Decimal for precision
        self.long_fill_price: Decimal | None = None
        self.short_fill_price: Decimal | None = None
        self.long_fill_quantity: Decimal | None = None
        self.short_fill_quantity: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "opportunity": {
                "symbol": self.opportunity.opportunity.symbol,
                "long_exchange": self.opportunity.opportunity.long_exchange,
                "short_exchange": self.opportunity.opportunity.short_exchange,
                "long_size": self.opportunity.long_size,
                "short_size": self.opportunity.short_size,
                "expected_profit": self.opportunity.expected_profit,
            },
            "status": self.status.name,
            "error_message": self.error_message,
            "long_order_id": self.long_order_id,
            "short_order_id": self.short_order_id,
            "long_position_id": self.long_position_id,
            "short_position_id": self.short_position_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "long_fill_price": self.long_fill_price,
            "short_fill_price": self.short_fill_price,
            "long_fill_quantity": self.long_fill_quantity,
            "short_fill_quantity": self.short_fill_quantity,
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

        # API clients
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Execution parameters - Ensure defaults are appropriate
        self.max_slippage: Decimal = Decimal(
            str(config.get("execution.max_slippage", "0.002"))
        )  # 0.2%
        self.max_retries: int = config.get("execution.max_retries", 3)
        self.retry_delay_base: float = float(
            config.get("execution.retry_delay_base_sec", "1.0")
        )  # seconds

        # Execution history (keep last N)
        self.max_execution_history: int = config.get("execution.max_history", 100)
        self.executions: list[TradeExecution] = []

        # Currently active executions
        self.active_executions: dict[str, TradeExecution] = {}

        # Initialize logger
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
            Trade execution result
        """
        # Create execution record
        execution = TradeExecution(opportunity=opportunity)
        self.active_executions[execution.id] = execution
        execution_id = execution.id
        internal_symbol = opportunity.opportunity.symbol  # Use the internal symbol from opportunity

        try:
            # Define exchanges early
            long_exchange = opportunity.opportunity.long_exchange
            short_exchange = opportunity.opportunity.short_exchange

            # --- Get Exchange Specific Symbols using Mapper ---
            long_exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                internal_symbol, long_exchange
            )
            short_exchange_symbol = self.symbol_mapper.get_exchange_symbol(
                internal_symbol, short_exchange
            )

            # --- Prepare Error Message ---
            if not long_exchange_symbol or not short_exchange_symbol:
                error_msg = (
                    f"Execution failed: Cannot map internal symbol '{internal_symbol}' "
                    f"to exchange symbols. Long ({long_exchange}): '{long_exchange_symbol}', "
                    f"Short ({short_exchange}): '{short_exchange_symbol}'. "
                    f"Check configuration."
                )
                logger.error(f"Execution {execution.id}: {error_msg}")
                execution.status = ExecutionStatus.REJECTED
                execution.error_message = error_msg
                execution.end_time = datetime.now(UTC)
                return execution
            # --- End Symbol Mapping ---

            # --- Circuit Breaker Checks (Using Internal Symbol for consistency) ---
            if self.circuit_breaker_system:
                logger.debug(
                    f"Execution {execution.id}: Checking circuit breakers for "
                    f"internal symbol '{internal_symbol}'..."
                )
                # Check Long Exchange
                # Use can_execute instead of allow_request
                can_proceed, reason = self.circuit_breaker_system.can_execute(
                    long_exchange, internal_symbol
                )
                if not can_proceed:
                    error_msg = (
                        f"Execution rejected by circuit breaker: Long exchange "
                        f"({long_exchange}) breaker tripped: {reason}"
                    )
                    logger.warning(f"Execution {execution.id}: {error_msg}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = error_msg
                    execution.end_time = datetime.now(UTC)
                    return execution

                # Check Short Exchange
                # Use can_execute instead of allow_request
                can_proceed, reason = self.circuit_breaker_system.can_execute(
                    short_exchange, internal_symbol
                )
                if not can_proceed:
                    error_msg = (
                        f"Execution rejected by circuit breaker: Short exchange "
                        f"({short_exchange}) breaker tripped: {reason}"
                    )
                    logger.warning(f"Execution {execution.id}: {error_msg}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = error_msg
                    execution.end_time = datetime.now(UTC)
                    return execution

                # Check Global
                # Use can_execute instead of allow_request
                can_proceed, reason = self.circuit_breaker_system.can_execute(
                    exchange="global"  # Assuming global doesn't need symbol
                )
                if not can_proceed:
                    error_msg = (
                        f"Execution rejected by circuit breaker: Global breaker tripped: {reason}"
                    )
                    logger.warning(f"Execution {execution.id}: {error_msg}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = error_msg
                    execution.end_time = datetime.now(UTC)
                    return execution
                logger.debug(f"Execution {execution.id}: Circuit breakers OK.")
            else:
                logger.warning(
                    "No main circuit breaker system provided to ExecutionHandler. Skipping checks."
                )
            # --- End Circuit Breaker Checks ---

            # Get API clients
            long_client = self.api_clients.get(long_exchange)
            short_client = self.api_clients.get(short_exchange)
            if not long_client or not short_client:
                error_msg = (
                    f"Missing API client for {long_exchange if not long_client else short_exchange}"
                )
                logger.error(f"Execution {execution.id}: {error_msg}")
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                return execution

            # --- Start Ticker/Price Checks for Quantity Calculation ---
            logger.debug(
                f"Execution {execution.id}: Fetching tickers using exchange symbols: "
                f"{long_exchange_symbol}, {short_exchange_symbol}"
            )
            long_ticker: Ticker | None = None
            short_ticker: Ticker | None = None
            try:
                # Use EXCHANGE symbols for API calls
                long_ticker = await long_client.get_ticker(long_exchange_symbol)
                short_ticker = await short_client.get_ticker(short_exchange_symbol)
            except APIError as e:
                error_msg = f"API Error fetching initial tickers: {e}"
                logger.error(f"Execution {execution.id}: {error_msg}")
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                # Record API error with circuit breaker
                if self.circuit_breaker_system:
                    exchange_failed = (
                        long_exchange if long_ticker is None else short_exchange
                    )  # Best guess
                    # Use record_api_error instead of record_failure
                    self.circuit_breaker_system.record_api_error(exchange_failed, error_msg)
                return execution
            except Exception as e:
                error_msg = f"Unexpected error fetching initial tickers: {e}"
                logger.exception(
                    f"Execution {execution.id}: {error_msg}"
                )  # Use exception for stack trace
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                # Record failure with circuit breaker
                if self.circuit_breaker_system:
                    exchange_failed = long_exchange if long_ticker is None else short_exchange
                    # Use record_api_error instead of record_failure
                    self.circuit_breaker_system.record_api_error(exchange_failed, error_msg)
                return execution

            if (
                not long_ticker
                or not short_ticker
                or long_ticker.price is None
                or short_ticker.price is None
            ):
                error_msg = "Failed to retrieve valid ticker data for quantity calculation."
                logger.error(f"Execution {execution.id}: {error_msg}")
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                # Optionally record CB error here too if desired
                return execution
            # --- End Ticker/Price Checks ---

            # Calculate order quantities based on USD sizes and current prices
            try:
                # Ensure prices are Decimal
                long_price_dec = Decimal(str(long_ticker.price))
                short_price_dec = Decimal(str(short_ticker.price))
                if long_price_dec <= 0 or short_price_dec <= 0:
                    raise ValueError("Ticker price must be positive")

                # Quantities are USD size / price
                long_quantity = (opportunity.long_size / long_price_dec).quantize(
                    Decimal("0.000001")
                )  # Adjust precision as needed
                short_quantity = (opportunity.short_size / short_price_dec).quantize(
                    Decimal("0.000001")
                )  # Adjust precision as needed

                if long_quantity <= 0 or short_quantity <= 0:
                    raise ValueError("Calculated order quantity must be positive")

            except (InvalidOperation, ValueError, TypeError) as e:
                error_msg = f"Error calculating order quantities: {e}"
                logger.error(
                    f"Execution {execution.id}: {error_msg} "
                    f"(Long Price: {long_ticker.price}, Short Price: {short_ticker.price})",
                    exc_info=True,
                )
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                return execution

            logger.info(
                f"Execution {execution.id}: Calculated Quantities - "
                f"Long ({long_exchange}/{long_exchange_symbol}): {long_quantity}, "
                f"Short ({short_exchange}/{short_exchange_symbol}): {short_quantity}"
            )

            # Set start time and status
            execution.start_time = datetime.now(UTC)
            execution.status = ExecutionStatus.EXECUTING

            # --- Place Orders Concurrently (or Sequentially if needed) ---
            # Configuration for execution type
            execution_type = self.config.get("execution.order_placement_type", "concurrent").lower()
            use_market_orders = self.config.get("execution.use_market_orders", True)
            order_type_to_use = OrderType.MARKET if use_market_orders else OrderType.LIMIT

            # --- Prepare Order Parameters (Common) ---
            # Note: Parameters for _place_order_with_retry are passed directly below.
            # Limit prices would need separate calculation if using LIMIT orders.
            # The parameter dictionaries `long_order_params` and `short_order_params`
            # were previously defined here but were unused, so they have been removed.

            # Placeholder for placed order objects
            long_order_result: Order | None = None
            short_order_result: Order | None = None

            # Generate unique client order IDs
            long_client_order_id = f"cde_long_{execution.id[:8]}_{int(time.time() * 1000)}"
            short_client_order_id = f"cde_short_{execution.id[:8]}_{int(time.time() * 1000)}"

            # --- Execute Orders ---
            if execution_type == "sequential":
                logger.info(f"Execution {execution.id}: Placing orders sequentially...")
                # Example: Place long first, then short (adjust logic as needed)
                long_order_result = await self._place_order_with_retry(
                    client=long_client,
                    exchange_id=long_exchange,
                    symbol=long_exchange_symbol,
                    side=OrderSide.BUY,
                    order_type=order_type_to_use,
                    quantity=long_quantity,
                    price=long_price_dec if order_type_to_use == OrderType.LIMIT else None,
                    time_in_force=TimeInForce.GTC if order_type_to_use == OrderType.LIMIT else None,
                    retry_delay=self.retry_delay_base,
                    max_retries=self.max_retries,
                    reduce_only=False,
                    client_order_id=long_client_order_id, # Pass client order id
                )
                if long_order_result and long_order_result.status not in (
                    OrderStatus.FAILED,
                    OrderStatus.REJECTED,
                    OrderStatus.CANCELED,
                ):
                    short_order_result = await self._place_order_with_retry(
                        client=short_client,
                        exchange_id=short_exchange,
                        symbol=short_exchange_symbol,
                        side=OrderSide.SELL,
                        order_type=order_type_to_use,
                        quantity=short_quantity,
                        price=short_price_dec if order_type_to_use == OrderType.LIMIT else None,
                        time_in_force=TimeInForce.GTC
                        if order_type_to_use == OrderType.LIMIT
                        else None,
                        retry_delay=self.retry_delay_base,
                        max_retries=self.max_retries,
                        reduce_only=False,
                        client_order_id=short_client_order_id, # Pass client order id
                    )
                else:
                    logger.warning(
                        f"Execution {execution.id}: Skipping short order placement "
                        f"due to long order failure/rejection."
                    )
                    # Handle compensation/cleanup if long order partially filled or failed
                    if (
                        long_order_result
                        and long_order_result.filled_quantity is not None
                        and long_order_result.filled_quantity > Decimal(0)
                    ):
                        logger.warning(
                            f"Execution {execution.id}: Long order failed but partially "
                            f"filled ({long_order_result.filled_quantity}). "
                            f"Attempting compensation."
                        )
                        # Remove original_failed_side argument
                        comp_success = await self._compensate_position(
                            client=long_client,
                            exchange_id=long_exchange,
                            internal_symbol=internal_symbol,
                            quantity=long_order_result.filled_quantity,
                            original_order=long_order_result, # Pass the original order
                        )
                        if comp_success:
                            logger.info("Compensated for partially filled long order.")
                        else:
                            logger.error("FAILED to compensate for partially filled long order.")
                            # Record critical failure?

            elif execution_type == "concurrent":
                logger.info(f"Execution {execution.id}: Placing orders concurrently...")
                results = await asyncio.gather(
                    self._place_order_with_retry(
                        client=long_client,
                        exchange_id=long_exchange,
                        symbol=long_exchange_symbol,
                        side=OrderSide.BUY,
                        order_type=order_type_to_use,
                        quantity=long_quantity,
                        price=long_price_dec if order_type_to_use == OrderType.LIMIT else None,
                        time_in_force=TimeInForce.GTC
                        if order_type_to_use == OrderType.LIMIT
                        else None,
                        retry_delay=self.retry_delay_base,
                        max_retries=self.max_retries,
                        reduce_only=False,
                        client_order_id=long_client_order_id, # Pass client order id
                    ),
                    self._place_order_with_retry(
                        client=short_client,
                        exchange_id=short_exchange,
                        symbol=short_exchange_symbol,
                        side=OrderSide.SELL,
                        order_type=order_type_to_use,
                        quantity=short_quantity,
                        price=short_price_dec if order_type_to_use == OrderType.LIMIT else None,
                        time_in_force=TimeInForce.GTC
                        if order_type_to_use == OrderType.LIMIT
                        else None,
                        retry_delay=self.retry_delay_base,
                        max_retries=self.max_retries,
                        reduce_only=False,
                        client_order_id=short_client_order_id, # Pass client order id
                    ),
                    return_exceptions=True,  # Important to catch errors from either leg
                )

                # Process results
                if isinstance(results[0], Order):
                    long_order_result = results[0]
                elif isinstance(results[0], Exception):
                    logger.error(
                        f"Execution {execution.id}: Error placing long order: {results[0]}",
                        exc_info=results[0],
                    )
                    # Optionally record CB error
                    if self.circuit_breaker_system:
                        # Use record_api_error instead of record_failure
                        self.circuit_breaker_system.record_api_error(
                            long_exchange, f"Concurrent order placement failed: {results[0]}"
                        )

                if isinstance(results[1], Order):
                    short_order_result = results[1]
                elif isinstance(results[1], Exception):
                    logger.error(
                        f"Execution {execution.id}: Error placing short order: {results[1]}",
                        exc_info=results[1],
                    )
                    # Optionally record CB error
                    if self.circuit_breaker_system:
                        # Use record_api_error instead of record_failure
                        self.circuit_breaker_system.record_api_error(
                            short_exchange, f"Concurrent order placement failed: {results[1]}"
                        )

            else:
                raise ValueError(f"Unsupported execution.order_placement_type: {execution_type}")

            # --- Process Order Results ---
            execution.long_order_id = long_order_result.id if long_order_result else None  # Use .id
            execution.short_order_id = (
                short_order_result.id if short_order_result else None
            )  # Use .id

            # Check results and handle failures/partial fills
            long_status = long_order_result.status if long_order_result else OrderStatus.FAILED
            short_status = short_order_result.status if short_order_result else OrderStatus.FAILED

            logger.info(
                f"Execution {execution.id}: Order Placement Results - Long: "
                f"{long_status.name}, Short: {short_status.name}"
            )

            # Simplistic status check - requires refinement for monitoring fills
            # TODO: Implement robust order monitoring for fill status and quantities

            if long_status == OrderStatus.FAILED or short_status == OrderStatus.FAILED:
                # --- Handle Failure / Compensation ---
                execution.status = ExecutionStatus.FAILED
                execution.error_message = "One or both order placements failed."
                logger.error(f"Execution {execution.id}: {execution.error_message}")

                # Determine which leg(s) might need compensation
                needs_compensation = False
                compensation_tasks = []

                # If long leg failed but short leg might have filled (partially or fully)
                if long_status == OrderStatus.FAILED and short_status != OrderStatus.FAILED:
                    logger.warning(
                        f"Execution {execution.id}: Long leg failed, attempting "
                        f"compensation for short position."
                    )
                    # Check short order status more accurately
                    # Assuming we need to close the short position placed by short_order_result
                    if (
                        short_order_result
                        and short_order_result.filled_quantity is not None
                        and short_order_result.filled_quantity > Decimal(0)
                    ):
                        needs_compensation = True
                        compensation_tasks.append(
                            # Remove original_failed_side argument
                            self._compensate_position(
                                client=short_client,
                                exchange_id=short_exchange,
                                internal_symbol=internal_symbol,
                                quantity=short_order_result.filled_quantity,
                                original_order=short_order_result, # Pass the original order
                            )
                        )

                # If short leg failed but long leg might have filled
                elif short_status == OrderStatus.FAILED and long_status != OrderStatus.FAILED:
                    logger.warning(
                        f"Execution {execution.id}: Short leg failed, attempting "
                        f"compensation for long position."
                    )
                    # Assuming we need to close the long position placed by long_order_result
                    if (
                        long_order_result
                        and long_order_result.filled_quantity is not None
                        and long_order_result.filled_quantity > Decimal(0)
                    ):
                        needs_compensation = True
                        compensation_tasks.append(
                            # Remove original_failed_side argument
                            self._compensate_position(
                                client=long_client,
                                exchange_id=long_exchange,
                                internal_symbol=internal_symbol,
                                quantity=long_order_result.filled_quantity,
                                original_order=long_order_result, # Pass the original order
                            )
                        )

                # If both failed, no compensation usually needed unless one partially
                # filled before failing
                elif long_status == OrderStatus.FAILED and short_status == OrderStatus.FAILED:
                    logger.info(
                        f"Execution {execution.id}: Both order placements failed. "
                        f"No compensation needed."
                    )

                if needs_compensation:
                    execution.status = ExecutionStatus.COMPENSATING
                    logger.info(f"Execution {execution.id}: Running compensation tasks...")
                    compensation_results = await asyncio.gather(
                        *compensation_tasks, return_exceptions=True
                    )
                    # Check compensation results
                    if all(res is True for res in compensation_results):
                        logger.info(f"Execution {execution.id}: Compensation successful.")
                        execution.error_message += " Compensation successful."
                    else:
                        logger.error(
                            f"Execution {execution.id}: Compensation failed. "
                            f"Manual intervention likely required. Results: {compensation_results}"
                        )
                        execution.error_message += " Compensation FAILED."
                        if self.circuit_breaker_system:
                            # Use record_critical_failure
                            self.circuit_breaker_system.record_critical_failure(
                                "global", f"Compensation failed for execution {execution.id}"
                            )
                    # Set final status to FAILED after compensation attempt
                    execution.status = ExecutionStatus.FAILED

                execution.end_time = datetime.now(UTC)

            # If orders placed (or partially filled), need monitoring
            # Placeholder for success - real implementation needs fill monitoring
            elif long_status not in [OrderStatus.FAILED] and short_status not in [
                OrderStatus.FAILED
            ]:
                logger.info(
                    f"Execution {execution.id}: Both orders submitted. "
                    f"Monitoring required for fills."
                )
                # TODO: Implement monitoring loop or callback mechanism
                # For now, assume immediate completion if orders are not FAILED
                # Update execution record with potential fill data (even if monitoring is needed)
                execution.status = ExecutionStatus.COMPLETED  # Placeholder
                execution.long_fill_quantity = (
                    long_order_result.filled_quantity if long_order_result else Decimal(0)
                )
                execution.short_fill_quantity = (
                    short_order_result.filled_quantity if short_order_result else Decimal(0)
                )
                # Use avg_fill_price field if available, else price
                execution.long_fill_price = long_order_result.avg_fill_price if long_order_result and long_order_result.avg_fill_price is not None else (long_order_result.price if long_order_result else None)
                execution.short_fill_price = (
                    short_order_result.avg_fill_price if short_order_result and short_order_result.avg_fill_price is not None else (short_order_result.price if short_order_result else None)
                )
                execution.end_time = datetime.now(UTC)
                # Record successful execution for CBs
                if self.circuit_breaker_system:
                    # Use record_api_success instead of record_success
                    self.circuit_breaker_system.record_api_success(long_exchange, internal_symbol)
                    self.circuit_breaker_system.record_api_success(short_exchange, internal_symbol)

            # Calculate PnL based on order fills
            await self._update_pnl(execution)

            # Record execution completion
            self.executions.append(execution)
            if len(self.executions) > self.max_execution_history:
                self.executions.pop(0)

            # Remove from active executions regardless of final status
            if execution_id in self.active_executions:
                del self.active_executions[execution_id]

            logger.info(
                f"Execution {execution.id} processed. Final status: {execution.status.name}"
            )

        except CircuitBreakerTrippedError as cbt_err:
            logger.warning(f"Execution rejected by circuit breaker: {cbt_err}")
            execution.status = ExecutionStatus.REJECTED
            execution.error_message = str(cbt_err)
            # Add to executions for historical tracking
            self.executions.append(execution)
            if execution_id in self.active_executions:
                del self.active_executions[execution_id] # Remove from active
            return execution  # Return early with rejected status

        except Exception as e:
            logger.error(
                f"Error during execution {execution.id}: {e}",
                exc_info=True,
            )
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            execution.end_time = datetime.now(UTC)
            # Add to executions for historical tracking and remove from active
            self.executions.append(execution)
            if execution_id in self.active_executions:
                del self.active_executions[execution_id]
            return execution  # Return early with failed status

        finally:
            # Cleanup resources if needed
            pass

        # Return the final execution state (moved out of finally block)
        return execution

    async def _place_order_with_retry(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: TimeInForce | None = None,
        retry_delay: float = 0.5,
        max_retries: int = 3,
        reduce_only: bool = False,
        client_order_id: str | None = None, # Added client_order_id
    ) -> Order:
        """
        Place an order with retry logic.

        Args:
            client: ExchangeAPI client
            exchange_id: Exchange identifier
            symbol: Symbol to trade
            side: Order side (BUY/SELL)
            order_type: Order type (LIMIT/MARKET)
            quantity: Order quantity
            price: Order price (for LIMIT orders)
            time_in_force: Time in force (GTC/IOC/FOK)
            retry_delay: Delay between retries in seconds
            max_retries: Maximum number of retries
            reduce_only: Whether the order should only reduce position
            client_order_id: Optional client-provided order ID

        Returns:
            Order object (with FAILED status if unsuccessful)
        """
        # Ensure price is Decimal if provided
        if price is not None and not isinstance(price, Decimal):
            try:
                price = Decimal(str(price))
            except (InvalidOperation, TypeError, ValueError) as e:
                self.logger.error(f"Invalid price provided: {price} - {e}")
                # Instantiate using NEW field names
                return Order(
                    id=str(uuid.uuid4()), # Use id
                    symbol=symbol,
                    side=side,
                    type=order_type, # Use type
                    quantity=quantity,
                    price=None,
                    status=OrderStatus.FAILED,
                    time=datetime.now(UTC), # Use time
                    client_order_id=client_order_id,
                    avg_fill_price=None,
                )

        # For market orders without a specified time_in_force, default to TimeInForce.GTC
        # For LIMIT orders, GTC is usually required or a good default.
        if time_in_force is None:
             time_in_force = TimeInForce.GTC

        # Generate client_order_id if not provided
        if not client_order_id:
             client_order_id = f"cde_{exchange_id[:3]}_{int(time.time() * 1000)}_{random.randint(100,999)}"

        # Try to place the order, with retries
        retry = 0
        last_exception: Exception | None = None
        while retry <= max_retries:
            # --- Circuit Breaker Check ---
            if self.circuit_breaker_system and \
               not self.circuit_breaker_system.can_execute(exchange_id, symbol): # Use can_execute
                logger.warning(f"Circuit breaker tripped for {exchange_id}. Aborting order placement.")
                last_exception = CircuitBreakerTrippedError(f"Circuit breaker tripped for {exchange_id}")
                break # Exit retry loop immediately
            # -----------------------------
            try:
                order = await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    time_in_force=time_in_force,
                    price=price,
                    reduce_only=reduce_only,
                    client_order_id=client_order_id, # Pass it here
                )

                self.logger.info(
                    f"Order placed: {order.id} on {exchange_id} for {symbol}, " # Use .id
                    f"{side.name}, {quantity}"
                )
                # --- Circuit Breaker Success ---
                if self.circuit_breaker_system:
                    # Use record_api_success instead of record_success
                    self.circuit_breaker_system.record_api_success(exchange_id, symbol)
                # -----------------------------
                return order  # Success case

            except APIError as e:
                last_exception = e
                # --- Circuit Breaker Failure ---
                if self.circuit_breaker_system:
                    # Use record_api_error instead of record_failure
                    self.circuit_breaker_system.record_api_error(exchange_id, f"API Error: {e.code.name}")
                # -----------------------------

                self.logger.warning(
                    f"API error placing order on {exchange_id} for {symbol}, "
                    f"{side.name}, {quantity}: {e}"
                )

                # Check for non-retryable errors
                if e.code in [APIErrorCode.INSUFFICIENT_FUNDS, APIErrorCode.QUANTITY_OUT_OF_RANGE, APIErrorCode.ORDER_REJECTED]:
                    self.logger.error(f"Non-retryable API error: {e.code.name} - {e.message}")
                    break # Exit retry loop

                # Check if we've reached max retries
                if retry >= max_retries:
                    self.logger.error(
                        f"Max retries ({max_retries}) exceeded placing order on {exchange_id}"
                    )
                    break # Exit retry loop

                # Wait with exponential backoff before retrying
                backoff_time = retry_delay * (2**retry) + random.uniform(0, retry_delay * 0.1) # Add jitter
                self.logger.info(f"Retrying order placement in {backoff_time:.2f}s...")
                await asyncio.sleep(backoff_time)
                retry += 1  # Increment retry counter
                continue  # Continue the loop for retry

            except CircuitBreakerTrippedError as cbt_err:
                 last_exception = cbt_err
                 logger.error(f"Order placement aborted due to circuit breaker: {cbt_err}")
                 break # Exit retry loop

            except Exception as e:
                last_exception = e
                # Handle unexpected errors
                self.logger.error(f"Unexpected error placing order: {e}", exc_info=True)
                # --- Circuit Breaker Failure ---
                if self.circuit_breaker_system:
                    # Use record_api_error instead of record_failure
                    self.circuit_breaker_system.record_api_error(exchange_id, f"Unexpected Error: {e}")
                # -----------------------------
                # Decide if retry makes sense for unexpected errors, maybe not for all.
                # For now, break the loop on unexpected errors.
                break # Exit retry loop

        # After loop (due to break or max retries) - Create FAILED or REJECTED order object
        final_status = OrderStatus.REJECTED if isinstance(last_exception, APIError) and last_exception.code == APIErrorCode.ORDER_REJECTED else OrderStatus.FAILED
        # Instantiate using NEW field names
        return Order(
            id=str(uuid.uuid4()), # Use id
            symbol=symbol,
            side=side,
            type=order_type, # Use type
            quantity=quantity,
            price=price,
            status=final_status,
            time=datetime.now(UTC), # Use time
            client_order_id=client_order_id,
            avg_fill_price=None, # Added default
        )


    async def _get_order_status(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        order_id: str,
        symbol: str,  # Added symbol for context
        client_order_id: str | None = None, # Added client_order_id
    ) -> Order | None:
        """
        Get the current status of an order.

        Args:
            client: ExchangeAPI client
            exchange_id: Exchange identifier
            order_id: Order identifier
            symbol: Symbol the order is for
            client_order_id: Optional client order ID for lookup

        Returns:
            Order object if found, None otherwise
        """
        # Note: Retries might be better handled in the calling function
        # (_verify_order_state). Assume get_order handles transient errors.
        try:
            # Use get_order_status which might use client_order_id as fallback
            # Assuming get_order_status exists on the concrete client implementations
            order = await client.get_order_status(order_id=order_id, client_order_id=client_order_id)

            # If we have the order, process it
            if order:
                # Ensure we actually have an Order object before proceeding
                if isinstance(order, Order):
                    logger.debug(
                        f"Got order status for {order_id}: {order.status.name}, "
                        f"Filled: {order.filled_quantity}"
                    )
                    # Update portfolio tracker order state
                    self.portfolio_tracker.update_order(exchange_id, order)
                    # --- ADDED: Update position based on fill from status check ---
                    if (
                        order.status
                        in (
                            OrderStatus.FILLED,
                            OrderStatus.PARTIALLY_FILLED,
                        )
                        and order.filled_quantity is not None
                        and order.filled_quantity > Decimal("0")
                    ):
                        try:
                            # Construct Trade object with correct timestamp type (int)
                            trade_id = (
                                f"trade_{order.id}_{int(time.time() * 1000)}_status"  # Use .id
                            )
                            # Convert datetime to int timestamp for Trade
                            timestamp_int = int(time.time() * 1000)
                            trade = Trade(
                                id=trade_id,
                                order_id=order.id,  # Use .id (Trade model still uses order_id)
                                exchange=exchange_id,
                                symbol=self.symbol_mapper.get_internal_symbol(symbol, exchange_id)
                                or symbol,  # Map back to internal symbol
                                side=order.side,
                                price=order.avg_fill_price if order.avg_fill_price is not None else (order.price if order.price is not None else Decimal("0")), # Use avg_fill_price if available
                                quantity=order.filled_quantity,
                                timestamp=timestamp_int,
                                fee=None, # Order object does not have fee
                                fee_asset=None, # Order object does not have fee_currency
                            )
                            # Process the trade through portfolio tracker
                            self.portfolio_tracker.process_trade(exchange_id, trade)
                            logger.info(
                                f"Created and processed trade from order status update for {order.id}" # Use .id
                            )
                        except Exception as e:
                            logger.error(
                                f"Error creating trade from order status: {e}", exc_info=True
                            )

                    return order
                else:
                    # Log if 'order' is not an Order object (e.g., if API returned unexpected dict)
                    logger.error(
                        f"Retrieved data for order {order_id} on {exchange_id} "
                        f"is not a valid Order object: {type(order)}"
                    )
                    return None  # Return None as we couldn't process it
            else:
                logger.warning(f"get_order_status returned None for ID {order_id} on {exchange_id}")
                return None
        except APIError as e:
            logger.error(
                f"API Error getting order status for {order_id} ({symbol}) on {exchange_id}: {e}"
            )
            # Record API error with circuit breaker
            if self.circuit_breaker_system:
                # Use record_api_error instead of record_failure
                self.circuit_breaker_system.record_api_error(
                    exchange_id, f"Get order status failed: {e}"
                )
            # Re-raise or return None based on desired handling
            # For now, returning None to indicate failure to retrieve status
            return None
        except AttributeError as ae:
             # Specifically catch if get_order_status doesn't exist on the client
             if "get_order_status" in str(ae):
                  logger.error(f"Exchange client for {exchange_id} does not implement get_order_status.")
                  # Decide handling: maybe try get_open_orders / get_order_history as fallback?
                  # For now, return None.
                  return None
             else:
                  logger.error(
                      f"Unexpected AttributeError getting order status for {order_id} ({symbol}) "
                      f"on {exchange_id}: {ae}",
                      exc_info=True,
                  )
                  if self.circuit_breaker_system:
                      self.circuit_breaker_system.record_api_error(
                          exchange_id, f"Unexpected error getting order status: {ae}"
                      )
                  return None

        except Exception as e:
            logger.error(
                f"Unexpected error getting order status for {order_id} ({symbol}) "
                f"on {exchange_id}: {e}",
                exc_info=True,
            )
            # Record generic error with circuit breaker
            if self.circuit_breaker_system:
                # Use record_api_error instead of record_error if it doesn't exist
                self.circuit_breaker_system.record_api_error(
                    exchange_id, f"Unexpected error getting order status: {e}"
                )
            return None

    async def _compensate_position(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        internal_symbol: str,  # Changed to internal symbol for context
        # original_failed_side: OrderSide, # This parameter seems incorrect/unused later - REMOVED
        quantity: Decimal,
        original_order: Order, # Pass the original order for context
    ) -> bool:
        """Attempts to compensate for a partially filled or failed order leg."""
        # --- Log current position size before compensation ---
        # Use internal symbol to get position from tracker
        current_position = self.portfolio_tracker.get_position(exchange_id, internal_symbol)
        current_pos_size = current_position.size if current_position else Decimal("0")
        logger.debug(
            f"Compensation PRE-CHECK for {exchange_id}/{internal_symbol}: "
            f"Current pos size: {current_pos_size}. "
            f"(Original Order ID: {original_order.id})" # Use .id
        )

        # Determine the side of the position established by the original order
        original_side = original_order.side
        compensating_side = OrderSide.SELL if original_side == OrderSide.BUY else OrderSide.BUY

        # Adjust quantity based on current position to avoid over-compensation
        # If we were BUYing (long) and failed, current_pos_size should be positive or zero.
        # If we were SELLing (short) and failed, current_pos_size should be negative or zero.
        if original_side == OrderSide.BUY and current_pos_size < quantity:
             logger.warning(f"Compensation quantity ({quantity}) exceeds current long position size ({current_pos_size}). Adjusting.")
             quantity = current_pos_size
        elif original_side == OrderSide.SELL and abs(current_pos_size) < quantity:
             logger.warning(f"Compensation quantity ({quantity}) exceeds current short position size ({current_pos_size}). Adjusting.")
             quantity = abs(current_pos_size)


        logger.info(
            f"Attempting compensation on {exchange_id} for internal symbol '{internal_symbol}' "
            f"(Original Order ID: {original_order.id}, Original Side: {original_side.name}) " # Use .id
            f"-> Compensating Side: {compensating_side.name}, Adjusted Quantity: {quantity}"
        )

        # Check if quantity is effectively zero after adjustment
        if quantity <= Decimal("1E-12"):  # Use a small threshold
            logger.info(
                f"Compensation quantity for {exchange_id}/{internal_symbol} "
                f"is negligible ({quantity}). Skipping compensation order."
            )
            return True  # Consider it successful as no action needed

        # --- Use SymbolMapper to get the required exchange-specific symbol ---
        exchange_symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange_id)
        if not exchange_symbol:
            logger.error(
                f"Symbol mapping failed during compensation: Cannot find "
                f"exchange symbol for internal '{internal_symbol}' on "
                f"'{exchange_id}'. Compensation ABORTED."
            )
            # Record critical failure? Depends on policy.
            if self.circuit_breaker_system:
                # Use record_critical_failure
                self.circuit_breaker_system.record_critical_failure(
                    exchange_id, f"Symbol mapping failed during compensation for {internal_symbol}"
                )
            return False  # Cannot proceed without the correct symbol
        # -------------------------------------------------------------------

        logger.debug(f"Compensation using exchange symbol: '{exchange_symbol}'")

        use_limit_compensation = self.config.get("execution.compensation.use_limit_orders", False)
        price_offset_config_key = "execution.compensation.limit_price_offset_pct"
        default_offset = 0.05
        price_offset_str = str(self.config.get(price_offset_config_key, default_offset))
        price_offset_dec = Decimal(price_offset_str)
        price_offset_fraction = price_offset_dec / Decimal("100.0")

        limit_price = None
        order_type_to_use = OrderType.MARKET

        if use_limit_compensation:
            try:
                # --- Use the mapped exchange_symbol for ticker lookup ---
                ticker = await client.get_ticker(exchange_symbol)
                # -------------------------------------------------------
                if (
                    ticker
                    and ticker.bid is not None
                    and ticker.ask is not None
                    and ticker.bid > 0
                    and ticker.ask > 0
                ):
                    bid_price = Decimal(str(ticker.bid))
                    ask_price = Decimal(str(ticker.ask))

                    # Compensating side already determined above
                    if compensating_side == OrderSide.SELL:
                        # Selling to close long: set limit slightly below current bid
                        limit_price = bid_price * (Decimal("1") - price_offset_fraction)
                    else:  # compensating_side == OrderSide.BUY
                        # Buying to close short: set limit slightly above current ask
                        limit_price = ask_price * (Decimal("1") + price_offset_fraction)

                    if limit_price > 0:
                        order_type_to_use = OrderType.LIMIT
                        logger.info(
                            f"Calculated compensating limit price for '{exchange_symbol}': "
                            f"{limit_price:.4f} "
                            f"(Comp Side: {compensating_side.name}, Offset: "
                            f"{price_offset_dec:.3f}%)"
                        )
                    else:
                        logger.warning(
                            f"Calculated invalid limit price ({limit_price}) "
                            f"for '{exchange_symbol}' compensation. "
                            f"Falling back to MARKET order."
                        )
                else:
                    logger.warning(
                        f"Could not get valid ticker bid/ask for '{exchange_symbol}' "
                        f"on {exchange_id} to set limit price. "
                        f"Falling back to MARKET order."
                    )
            except Exception as ticker_err:
                logger.error(
                    f"Error getting ticker for limit price calculation on "
                    f"{exchange_id} for '{exchange_symbol}': {ticker_err}. "
                    f"Falling back to MARKET order.",
                    exc_info=True,
                )
        else:
            logger.info(f"Market order configured for compensation for '{exchange_symbol}'.")

        # --- Place the Compensating Order ---
        try:
            logger.info(
                f"Placing compensating order: {compensating_side.name} "
                f"{quantity} {exchange_symbol}@{order_type_to_use.name} "
                f"on {exchange_id}"
            )

            # --- Add final check of quantity before placing ---
            logger.debug(
                f"Final check before placing compensation order for "
                f"{exchange_symbol}: Quantity={quantity}, ReduceOnly=True"
            )

            # Place the compensating order (MUST be reduce_only)
            compensating_order = await self._place_order_with_retry(
                client=client,
                exchange_id=exchange_id,
                symbol=exchange_symbol,  # Use mapped exchange symbol
                side=compensating_side,
                order_type=order_type_to_use,
                quantity=quantity,
                price=limit_price,  # Will be None for MARKET orders
                client_order_id=f"comp_{original_order.id[:20]}_{int(time.time())}", # Use .id
                reduce_only=True,  # CRITICAL: Compensation orders must be reduce-only
            )

            if compensating_order:
                logger.info(
                    f"Compensation order submitted for {exchange_symbol}: ID "
                    f"{compensating_order.id}, Status: {compensating_order.status.name}"  # Use .id
                )
                # TODO: Need to monitor the status of this compensating order
                # For now, assume success if placed without error.
                # A robust implementation would wait for fill confirmation.
                if compensating_order.status not in (
                    OrderStatus.FAILED,
                    OrderStatus.REJECTED,
                    OrderStatus.CANCELED,
                ):
                    logger.info(
                        f"Compensation order {compensating_order.id} placed " # Use .id
                        f"successfully for {exchange_symbol}."
                    )
                    return True
                else:
                    logger.error(
                        f"Compensation order {compensating_order.id} for " # Use .id
                        f"{exchange_symbol} failed/rejected/cancelled. "
                        f"Status: {compensating_order.status.name}"
                    )
                    return False
            else:
                logger.error(
                    f"Failed to place compensating order for "
                    f"{exchange_symbol} on {exchange_id} after retries."
                )
                return False

        except Exception as comp_err:
            logger.exception(
                f"Unexpected error during compensation order placement for "
                f"{exchange_symbol} on {exchange_id}: {comp_err}"
            )
            # Record critical failure
            if self.circuit_breaker_system:
                # Use record_critical_failure
                self.circuit_breaker_system.record_critical_failure(
                    exchange_id, f"Compensation placement failed for {exchange_symbol}: {comp_err}"
                )
            return False

    def get_execution_history(self) -> list[TradeExecution]:
        """
        Get the execution history.

        Returns:
            List of trade executions
        """
        return self.executions.copy()

    def get_active_executions(self) -> list[TradeExecution]:
        """
        Get the currently active executions.

        Returns:
            List of active trade executions
        """
        return list(self.active_executions.values())

    def reset_circuit_breaker(self, exchange_id: str) -> None:
        """Reset the circuit breaker for a specific exchange via the system."""
        if self.circuit_breaker_system:
            success = self.circuit_breaker_system.reset_exchange_breakers(exchange_id)
            if success > 0:
                logger.info(f"Successfully reset circuit breakers for {exchange_id} via system.")
            else:
                logger.warning(
                    f"Attempted to reset circuit breakers for {exchange_id}, "
                    f"but none were found/reset."
                )
        else:
            logger.warning("Circuit breaker system not available, cannot reset for {exchange_id}")

    async def _verify_order_state(
        self, execution_id: str, exchange: str, order_id: str
    ) -> Order | None:
        """Verify order state on exchange."""
        try:
            client = self.api_clients.get(exchange)
            if not client:
                self.logger.error(f"No API client found for {exchange}")
                return None

            execution = self.active_executions.get(execution_id)
            symbol = ""
            client_order_id = None # Fetch client_order_id if possible
            if execution:
                # Get symbol from opportunity
                internal_symbol = execution.opportunity.opportunity.symbol
                symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange) or ""
                # Try to get client_order_id from execution record if possible
                # This assumes client_order_id was stored on the execution object or reconstructible
                # Example placeholder logic:
                # if execution.long_order_id == order_id:
                #      client_order_id = execution.long_client_order_id # If stored
                # elif execution.short_order_id == order_id:
                #      client_order_id = execution.short_client_order_id # If stored
                pass


            # Use get_order_status method instead of trying to call get_order directly
            order = await self._get_order_status(client, exchange, order_id, symbol, client_order_id) # Pass client_order_id
            return order
        except Exception as e:
            self.logger.error(f"Error checking order status: {e}")
            return None

    async def _update_pnl(self, execution: TradeExecution) -> None:
        """
        Update profit and loss calculations based on execution fills.

        Args:
            execution: Trade execution
        """
        logger.debug(f"Updating PnL for execution {execution.id}")

        # Skip if not complete or partially complete
        if execution.status not in (
            ExecutionStatus.COMPLETED,
            ExecutionStatus.PARTIALLY_COMPLETED,
        ):
            logger.debug(
                f"Skipping PnL update for execution {execution.id} "
                f"with status {execution.status.name}"
            )
            return

        try:
            # Only calculate PnL if we have the necessary data
            if (
                execution.long_fill_price is not None
                and execution.short_fill_price is not None
                and execution.long_fill_quantity is not None
                and execution.short_fill_quantity is not None
            ):
                # Define key variables
                long_exchange = execution.opportunity.opportunity.long_exchange
                short_exchange = execution.opportunity.opportunity.short_exchange
                long_fill_price = execution.long_fill_price
                short_fill_price = execution.short_fill_price
                # Use the minimum filled quantity to calculate PnL (cannot exceed what was filled)
                min_filled_qty = min(execution.long_fill_quantity, execution.short_fill_quantity)

                # Calculate gross PnL (before fees)
                # Formula: For funding rate arb, it's (short price - long price) * quantity
                gross_pnl = (short_fill_price - long_fill_price) * min_filled_qty

                # Estimate fees (simplified, can be refined later)
                # Assume a percentage fee on both sides
                long_fee_rate = Decimal(
                    str(self.config.get(f"exchanges.{long_exchange}.taker_fee", "0.001"))
                )  # Default 0.1%
                short_fee_rate = Decimal(
                    str(self.config.get(f"exchanges.{short_exchange}.taker_fee", "0.001"))
                )  # Default 0.1%

                long_fee = long_fill_price * min_filled_qty * long_fee_rate
                short_fee = short_fill_price * min_filled_qty * short_fee_rate
                estimated_fees = long_fee + short_fee

                # Net PnL
                net_pnl = gross_pnl - estimated_fees

                # Get internal symbol
                symbol = execution.opportunity.opportunity.symbol # F841: Unused variable - Fixed: Use symbol below

                # Correctly update portfolio tracker with the PnL
                # Fix: Update with correct method that actually exists
                # If record_pnl is unavailable, use a different approach like
                # _update_realized_pnl # E501 fix
                # This is a placeholder - adjust based on available methods # E501 fix

                # Option 1: If record_pnl is available (uncomment)
                # """
                # self.portfolio_tracker.record_pnl(
                #     strategy=execution.opportunity.opportunity.__class__.__name__,
                #     symbol=symbol, # Use the symbol variable
                #     amount=net_pnl,
                #     timestamp=datetime.now(UTC),
                #     details={
                #         "execution_id": execution.id,
                #         "long_exchange": long_exchange,
                #         "short_exchange": short_exchange,
                #         "long_fill_price": str(long_fill_price),
                #         "short_fill_price": str(short_fill_price),
                #         "filled_quantity": str(min_filled_qty),
                #         "gross_pnl": str(gross_pnl),
                #         "estimated_fees": str(estimated_fees),
                #     },
                # )
                # """

                # Option 2: Alternative using _update_realized_pnl (uncomment if this method exists)
                # self.portfolio_tracker._update_realized_pnl(net_pnl) # Assuming this method exists

                # Option 3: Log PNL if no direct update method is available
                logger.info(f"Calculated Net PnL of {net_pnl:.4f} for execution {execution.id} (Symbol: {symbol})")

            else:
                logger.warning(
                    f"Cannot calculate PnL for execution {execution.id}: missing fill data"
                )
        except Exception as e:
            logger.error(f"Error recording PnL for execution {execution.id}: {e}")

    async def _handle_filled_order(
        self, client: ExchangeAPI, exchange_id: str, order: Order, execution: TradeExecution
    ) -> bool:
        """
        Handle order that has been filled.

        Args:
            client: API client
            exchange_id: Exchange identifier
            order: Order object
            execution: Execution context

        Returns:
            True if handled successfully, False otherwise
        """
        symbol = order.symbol
        internal_symbol = self.symbol_mapper.get_internal_symbol(symbol, exchange_id) or symbol

        # Update execution record with fill details
        if exchange_id == execution.opportunity.opportunity.long_exchange:
            execution.long_order_id = order.id # Use .id
            execution.long_fill_quantity = order.filled_quantity
            # Use avg_fill_price if available, else price
            execution.long_fill_price = order.avg_fill_price if order.avg_fill_price is not None else order.price

        elif exchange_id == execution.opportunity.opportunity.short_exchange:
            execution.short_order_id = order.id # Use .id
            execution.short_fill_quantity = order.filled_quantity
            # Use avg_fill_price if available, else price
            execution.short_fill_price = order.avg_fill_price if order.avg_fill_price is not None else order.price

        # Process trade through portfolio tracker
        # Construct trade object from order details
        timestamp_int = int(time.time() * 1000)  # Correct timestamp type (int)
        # Use avg_fill_price if available, else price, ensure not None
        trade_price = order.avg_fill_price if order.avg_fill_price is not None else order.price
        if trade_price is None or order.filled_quantity is None:
             logger.error(f"Cannot process trade for order {order.id}: Missing price or filled quantity.") # Use .id
             return False

        trade = Trade(
            id=f"trade_{order.id}_{timestamp_int}", # Use .id
            order_id=order.id, # Use .id (Trade model still uses order_id)
            exchange=exchange_id,
            symbol=internal_symbol,
            side=order.side,
            price=trade_price,
            quantity=order.filled_quantity,
            timestamp=timestamp_int,
            fee=None, # Order object does not have fee
            fee_asset=None, # Order object does not have fee_currency
        )

        try:
            # Process trade to update positions
            self.portfolio_tracker.process_trade(exchange_id, trade)
            logger.info(
                f"Processed trade for order {order.id} on {exchange_id}: " # Use .id
                f"{order.side.name} {internal_symbol} @ {trade_price}"
            )
            return True
        except Exception as e:
            logger.error(f"Error processing trade: {e}", exc_info=True)
            return False

    async def _monitor_order_status(
        self, execution_id: str, exchange_id: str, order_id: str
    ) -> tuple[bool, Order | None]:
        """
        Monitor an order's status on an exchange.
        Args:
            execution_id: Execution ID for context
            exchange_id: Exchange identifier
            order_id: Order identifier
        Returns:
            (success, order) tuple - success indicates if monitoring was successful
            order is the final order state (or None if unavailable)
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                f"Execution {execution_id}: Cannot monitor order {order_id} "
                f"on {exchange_id} - no API client found."
            )
            return False, None

        execution = self.active_executions.get(execution_id)
        if not execution:
            logger.error(f"Cannot find execution with ID {execution_id} to monitor order status.")
            return False, None

        # Get the original symbol
        internal_symbol = execution.opportunity.opportunity.symbol
        exchange_symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange_id)
        if not exchange_symbol:
            logger.error(
                f"Cannot map symbol {internal_symbol} to exchange {exchange_id} for monitoring."
            )
            return False, None

        # Try to get client_order_id from execution record if possible
        client_order_id = None
        if execution.long_order_id == order_id:
             # Assuming client_order_id was stored or reconstructible
             # client_order_id = ... (e.g., execution.long_client_order_id if stored)
             pass
        elif execution.short_order_id == order_id:
             # client_order_id = ... (e.g., execution.short_client_order_id if stored)
             pass

        # Define terminal states
        terminal_states = [
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        ]

        # Initialize
        check_count = 0
        max_checks = 30  # Limit the number of status checks
        order = None

        # Monitor loop
        while check_count < max_checks:
            try:
                # Get current order status using our _get_order_status method
                order = await self._get_order_status(client, exchange_id, order_id, exchange_symbol, client_order_id) # Pass client_order_id

                if not order:
                    logger.warning(
                        f"Execution {execution_id}: No order found for ID {order_id} on "
                        f"{exchange_id}."
                    )
                    await asyncio.sleep(2.0)  # Wait before retrying
                    check_count += 1
                    continue

                # Update portfolio tracker with latest order state
                # self.portfolio_tracker.update_order(exchange_id, order) # Already done in _get_order_status

                # Check if we've reached a terminal state
                if order.status in terminal_states or order.status in [OrderStatus.FAILED]:
                    logger.info(
                        f"Execution {execution_id}: Order {order_id} on {exchange_id} "
                        f"reached terminal state {order.status.name}."
                    )
                    break

                # If partially filled, trade processing is handled within _get_order_status now
                if order.status == OrderStatus.PARTIALLY_FILLED and order.filled_quantity:
                    logger.info(
                        f"Execution {execution_id}: Order {order_id} on {exchange_id} "
                        f"partially filled: {order.filled_quantity} of {order.quantity}."
                    )


                # Wait before next check (with some randomization to avoid API rate limits)
                wait_time = 1.0 + (random.random() * 0.5)
                await asyncio.sleep(wait_time)
                check_count += 1

            except APIError as e:
                logger.error(
                    f"Execution {execution_id}: API error monitoring order {order_id} on "
                    f"{exchange_id}: {e}"
                )
                if self.circuit_breaker_system:
                    # Use record_api_error instead of record_failure
                    self.circuit_breaker_system.record_api_error(
                        exchange_id, f"Monitor order status: {e}"
                    )
                await asyncio.sleep(2.0)  # Wait longer after an API error
                check_count += 1

            except Exception as e:
                logger.error(
                    f"Execution {execution_id}: Unexpected error monitoring order "
                    f"{order_id} on {exchange_id}: {e}",
                    exc_info=True,
                )
                await asyncio.sleep(2.0)
                check_count += 1

        # Check if we reached max attempts without a terminal state
        if (
            check_count >= max_checks
            and order
            and order.status not in terminal_states
            and order.status not in [OrderStatus.FAILED]
        ):
            logger.warning(
                f"Execution {execution_id}: Reached maximum status checks for order "
                f"{order_id} on {exchange_id}. "
                f"Last status: {order.status.name if order else 'Unknown'}"
            )

        # Final order state (or None if we couldn't get it)
        return True, order
