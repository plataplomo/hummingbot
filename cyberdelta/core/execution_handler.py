from __future__ import annotations  # Enable postponed evaluation

import asyncio
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI

# Import models needed at runtime directly
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType, Position, Ticker
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper, SymbolMappingError
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
        self.execution_history: list[TradeExecution] = []

        # Currently active executions
        self.active_executions: dict[str, TradeExecution] = {}

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
        internal_symbol = opportunity.opportunity.symbol # Use the internal symbol from opportunity

        try:
            # Define exchanges early
            long_exchange = opportunity.opportunity.long_exchange
            short_exchange = opportunity.opportunity.short_exchange

            # --- Get Exchange Specific Symbols using Mapper ---
            long_exchange_symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, long_exchange)
            short_exchange_symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, short_exchange)

            if not long_exchange_symbol or not short_exchange_symbol:
                error_msg = (
                    f"Execution failed: Cannot map internal symbol '{internal_symbol}' to exchange symbols. "
                    f"Long ({long_exchange}): '{long_exchange_symbol}', Short ({short_exchange}): '{short_exchange_symbol}'. "
                    f"Check configuration."
                )
                logger.error(f"Execution {execution.id}: {error_msg}")
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                execution.end_time = datetime.now(UTC)
                # No CB recording needed here as it's a config/setup issue, not API failure
                return execution
            # --- End Symbol Mapping ---

            # --- Circuit Breaker Checks (Using Internal Symbol for consistency) ---
            if self.circuit_breaker_system:
                logger.debug(f"Execution {execution.id}: Checking circuit breakers for internal symbol '{internal_symbol}'...")
                # Check Long Exchange
                can_proceed, reason = self.circuit_breaker_system.can_execute(
                    long_exchange, internal_symbol # Use internal symbol
                )
                if not can_proceed:
                    error_msg = f"Execution rejected by circuit breaker: Long exchange ({long_exchange}) breaker tripped: {reason}"
                    logger.warning(f"Execution {execution.id}: {error_msg}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = error_msg
                    execution.end_time = datetime.now(UTC)
                    return execution

                # Check Short Exchange
                can_proceed, reason = self.circuit_breaker_system.can_execute(
                    short_exchange, internal_symbol # Use internal symbol
                )
                if not can_proceed:
                    error_msg = f"Execution rejected by circuit breaker: Short exchange ({short_exchange}) breaker tripped: {reason}"
                    logger.warning(f"Execution {execution.id}: {error_msg}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = error_msg
                    execution.end_time = datetime.now(UTC)
                    return execution

                # Check Global
                can_proceed, reason = self.circuit_breaker_system.can_execute(
                    exchange="global" # Assuming global doesn't need symbol
                )
                if not can_proceed:
                    error_msg = f"Execution rejected by circuit breaker: Global breaker tripped: {reason}"
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
                error_msg = f"Missing API client for {long_exchange if not long_client else short_exchange}"
                logger.error(f"Execution {execution.id}: {error_msg}")
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                return execution

            # --- Start Ticker/Price Checks for Quantity Calculation ---
            logger.debug(f"Execution {execution.id}: Fetching tickers using exchange symbols: {long_exchange_symbol}, {short_exchange_symbol}")
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
                    exchange_failed = long_exchange if long_ticker is None else short_exchange # Best guess
                    self.circuit_breaker_system.record_api_error(exchange_failed, error_msg)
                return execution
            except Exception as e:
                error_msg = f"Unexpected error fetching initial tickers: {e}"
                logger.exception(f"Execution {execution.id}: {error_msg}") # Use exception for stack trace
                execution.status = ExecutionStatus.FAILED
                execution.error_message = error_msg
                 # Record failure with circuit breaker
                if self.circuit_breaker_system:
                    exchange_failed = long_exchange if long_ticker is None else short_exchange
                    self.circuit_breaker_system.record_api_error(exchange_failed, error_msg)
                return execution

            if not long_ticker or not short_ticker or long_ticker.price is None or short_ticker.price is None:
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
                long_quantity = (opportunity.long_size / long_price_dec).quantize(Decimal("0.000001")) # Adjust precision as needed
                short_quantity = (opportunity.short_size / short_price_dec).quantize(Decimal("0.000001")) # Adjust precision as needed

                if long_quantity <= 0 or short_quantity <= 0:
                    raise ValueError("Calculated order quantity must be positive")

            except (InvalidOperation, ValueError, TypeError) as e:
                 error_msg = f"Error calculating order quantities: {e}"
                 logger.error(f"Execution {execution.id}: {error_msg} (Long Price: {long_ticker.price}, Short Price: {short_ticker.price})", exc_info=True)
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
            # Note: Limit prices would need to be fetched/calculated just before placement if using LIMIT
            # For MARKET orders, price is None.
            long_order_params = {
                "client": long_client,
                "exchange_id": long_exchange,
                "symbol": long_exchange_symbol, # Use exchange symbol
                "side": OrderSide.BUY,
                "order_type": order_type_to_use,
                "quantity": long_quantity,
                "price": None, # Set if LIMIT order
                # Add other params like time_in_force if needed
            }
            short_order_params = {
                "client": short_client,
                "exchange_id": short_exchange,
                "symbol": short_exchange_symbol, # Use exchange symbol
                "side": OrderSide.SELL,
                "order_type": order_type_to_use,
                "quantity": short_quantity,
                "price": None, # Set if LIMIT order
                 # Add other params like time_in_force if needed
            }

            # Placeholder for placed order objects
            long_order_result: Order | None = None
            short_order_result: Order | None = None

            # --- Execute Orders --- 
            if execution_type == "sequential":
                logger.info(f"Execution {execution.id}: Placing orders sequentially...")
                # Example: Place long first, then short (adjust logic as needed)
                long_order_result = await self._place_order_with_retry(**long_order_params)
                if long_order_result and long_order_result.status not in (
                    OrderStatus.FAILED, OrderStatus.REJECTED, OrderStatus.CANCELLED
                ):
                    short_order_result = await self._place_order_with_retry(**short_order_params)
                else:
                    logger.warning(f"Execution {execution.id}: Skipping short order placement due to long order failure/rejection.")
                    # Handle compensation/cleanup if long order partially filled or failed

            elif execution_type == "concurrent":
                logger.info(f"Execution {execution.id}: Placing orders concurrently...")
                results = await asyncio.gather(
                    self._place_order_with_retry(**long_order_params),
                    self._place_order_with_retry(**short_order_params),
                    return_exceptions=True # Important to catch errors from either leg
                )

                # Process results
                if isinstance(results[0], Order):
                    long_order_result = results[0]
                elif isinstance(results[0], Exception):
                    logger.error(f"Execution {execution.id}: Error placing long order: {results[0]}", exc_info=results[0])
                    # Optionally record CB error
                    if self.circuit_breaker_system:
                       self.circuit_breaker_system.record_api_error(long_exchange, f"Concurrent order placement failed: {results[0]}")

                if isinstance(results[1], Order):
                    short_order_result = results[1]
                elif isinstance(results[1], Exception):
                    logger.error(f"Execution {execution.id}: Error placing short order: {results[1]}", exc_info=results[1])
                     # Optionally record CB error
                    if self.circuit_breaker_system:
                       self.circuit_breaker_system.record_api_error(short_exchange, f"Concurrent order placement failed: {results[1]}")

            else:
                 raise ValueError(f"Unsupported execution.order_placement_type: {execution_type}")

            # --- Process Order Results --- 
            execution.long_order_id = long_order_result.id if long_order_result else None
            execution.short_order_id = short_order_result.id if short_order_result else None

            # Check results and handle failures/partial fills
            long_status = long_order_result.status if long_order_result else OrderStatus.FAILED
            short_status = short_order_result.status if short_order_result else OrderStatus.FAILED

            logger.info(f"Execution {execution.id}: Order Placement Results - Long: {long_status.name}, Short: {short_status.name}")

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
                    logger.warning(f"Execution {execution.id}: Long leg failed, attempting compensation for short position.")
                    # Check short order status more accurately
                    # Assuming we need to close the short position placed by short_order_result
                    if short_order_result and short_order_result.filled_quantity > Decimal(0):
                         needs_compensation = True
                         compensation_tasks.append(self._compensate_position(
                             client=short_client,
                             exchange_id=short_exchange,
                             internal_symbol=internal_symbol, # Use internal symbol for compensation logic context
                             original_failed_side=OrderSide.BUY, # Original failed leg was BUY (long)
                             quantity=short_order_result.filled_quantity,
                         ))

                # If short leg failed but long leg might have filled
                elif short_status == OrderStatus.FAILED and long_status != OrderStatus.FAILED:
                    logger.warning(f"Execution {execution.id}: Short leg failed, attempting compensation for long position.")
                    # Assuming we need to close the long position placed by long_order_result
                    if long_order_result and long_order_result.filled_quantity > Decimal(0):
                         needs_compensation = True
                         compensation_tasks.append(self._compensate_position(
                            client=long_client,
                            exchange_id=long_exchange,
                            internal_symbol=internal_symbol, # Use internal symbol for compensation logic context
                            original_failed_side=OrderSide.SELL, # Original failed leg was SELL (short)
                            quantity=long_order_result.filled_quantity,
                         ))
                
                # If both failed, no compensation usually needed unless one partially filled before failing
                elif long_status == OrderStatus.FAILED and short_status == OrderStatus.FAILED:
                     logger.info(f"Execution {execution.id}: Both order placements failed. No compensation needed.")
                

                if needs_compensation:
                    execution.status = ExecutionStatus.COMPENSATING
                    logger.info(f"Execution {execution.id}: Running compensation tasks...")
                    compensation_results = await asyncio.gather(*compensation_tasks, return_exceptions=True)
                    # Check compensation results
                    if all(res is True for res in compensation_results):
                        logger.info(f"Execution {execution.id}: Compensation successful.")
                        execution.error_message += " Compensation successful."
                    else:
                        logger.error(f"Execution {execution.id}: Compensation failed. Manual intervention likely required. Results: {compensation_results}")
                        execution.error_message += " Compensation FAILED."
                        if self.circuit_breaker_system:
                           self.circuit_breaker_system.record_critical_failure("global", f"Compensation failed for execution {execution.id}")
                    # --- ADDED: Set final status to FAILED after compensation attempt --- 
                    execution.status = ExecutionStatus.FAILED
                    # --------------------------------------------------------------------

                execution.end_time = datetime.now(UTC)

            # If orders placed (or partially filled), need monitoring
            # Placeholder for success - real implementation needs fill monitoring
            elif long_status != OrderStatus.FAILED and short_status != OrderStatus.FAILED:
                logger.info(f"Execution {execution.id}: Both orders submitted. Monitoring required for fills.")
                 # TODO: Implement monitoring loop or callback mechanism
                # For now, assume immediate completion for testing/prototype
                execution.status = ExecutionStatus.COMPLETED # Placeholder
                execution.long_fill_quantity = long_quantity # Placeholder
                execution.short_fill_quantity = short_quantity # Placeholder
                execution.long_fill_price = long_price_dec # Placeholder - use actual fill price
                execution.short_fill_price = short_price_dec # Placeholder - use actual fill price
                execution.end_time = datetime.now(UTC)
                # Record successful execution for CBs
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_success(long_exchange, internal_symbol)
                    self.circuit_breaker_system.record_success(short_exchange, internal_symbol)


        except CircuitBreakerTrippedError as e:
             error_msg = f"Execution aborted by Circuit Breaker: {e}"
             logger.warning(f"Execution {execution.id}: {error_msg}")
             execution.status = ExecutionStatus.REJECTED
             execution.error_message = error_msg
             execution.end_time = datetime.now(UTC)
             # No need to record failure again, CB already tripped

        except APIError as e:
            error_msg = f"API Error during execution {execution.id}: {e}"
            logger.error(error_msg, exc_info=True)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = error_msg
            execution.end_time = datetime.now(UTC)
             # Record API error with circuit breaker
            if self.circuit_breaker_system:
                # Try to determine which exchange failed if possible
                exchange_failed = e.exchange_code if e.exchange_code else "unknown"
                self.circuit_breaker_system.record_api_error(exchange_failed, str(e))

        except Exception as e:
            error_msg = f"Unexpected error during execution {execution.id}: {e}"
            logger.exception(error_msg) # Log with stack trace
            execution.status = ExecutionStatus.FAILED
            execution.error_message = error_msg
            execution.end_time = datetime.now(UTC)
            # Record generic failure with circuit breaker
            if self.circuit_breaker_system:
               # Need to determine which exchange context to use if possible
               # Defaulting to global or a placeholder
               self.circuit_breaker_system.record_critical_failure("global", f"Unexpected execution error: {e}")


        finally:
            # Update history
            self.execution_history.append(execution)
            if len(self.execution_history) > self.max_execution_history:
                self.execution_history.pop(0)

            # Remove from active executions
            if execution_id in self.active_executions:
                 del self.active_executions[execution_id]

            logger.info(f"Execution {execution.id} finished with status: {execution.status.name}")

            # Return the final execution state
            return execution

    async def _place_order_with_retry(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        symbol: str, # THIS SHOULD BE THE EXCHANGE-SPECIFIC SYMBOL
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: str = "GTC",
        retry_count: int = 0,
        reduce_only: bool = False,
    ) -> Order | None:
        """
        Place an order with retry logic.
        Assumes 'symbol' is the correct exchange-specific symbol.
        Args:
            client: ExchangeAPI client
            exchange_id: Exchange identifier
            symbol: The exchange-specific trading symbol.
            side: BUY or SELL
            order_type: LIMIT or MARKET
            quantity: Order quantity
            price: Order price (for LIMIT orders)
            time_in_force: Time-in-force for the order
            retry_count: Current retry attempt count
            reduce_only: Whether the order is a reduce-only order

        Returns:
            The placed order object if successful, None otherwise.
        """
        error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                # Calculate retry delay with exponential backoff
                if attempt > 0:
                    delay = self.retry_delay_base * (2 ** (attempt - 1))
                    logger.info(
                        f"Retrying order placement (attempt {attempt + 1}/{self.max_retries}) "
                        f"after {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)

                # Place the order using the provided exchange-specific symbol
                logger.debug(f"Placing order attempt {attempt+1}: {exchange_id} {symbol} {side.name} {quantity} {order_type.name} ...")
                order = await client.place_order(
                    symbol=symbol, # Pass the exchange-specific symbol directly
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=time_in_force,
                    reduce_only=reduce_only,
                )

                if order:
                    logger.info(f"Order placed successfully on {exchange_id}: ID {order.id}, Symbol {symbol}, Status {order.status.name}")
                    # Update portfolio tracker (uses order details including symbol)
                    self.portfolio_tracker.update_order(exchange_id, order)
                    return order
                else:
                     # Should not happen if place_order adheres to return type hint, but handle defensively
                     logger.warning(f"place_order returned None for {exchange_id}/{symbol} attempt {attempt+1}")
                     # Treat as failure for retry logic
                     raise APIError("place_order returned None unexpectedly", code=APIErrorCode.UNKNOWN, exchange_code=exchange_id)

            except APIError as e:
                logger.warning(
                    f"API Error placing order on {exchange_id} (Attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                error = e # Store error for potential re-raise
            except Exception as e:
                logger.error(
                    f"Unexpected error placing order on {exchange_id} (Attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                error = e # Store error for potential re-raise

            # Exponential backoff
            if attempt < self.max_retries - 1:
                delay = self.retry_delay_base * (2**attempt)
                logger.info(f"Retrying order placement in {delay:.2f} seconds...")
                await asyncio.sleep(delay)

        # If all retries fail
        logger.error(f"Failed to place order {side.name} {quantity} {symbol} on {exchange_id} after {self.max_retries} attempts. Last error: {error}")
        # Record failure if circuit breaker exists and the error wasn't an APIError
        if self.circuit_breaker_system and not isinstance(error, APIError):
            exchange_for_failure = exchange_id  # Use the exchange ID from the failed retry
            error_msg_for_failure = f"Generic failure placing order after retries: {error}"
            # Replace generic record_failure with record_api_error
            self.circuit_breaker_system.record_api_error(
                exchange_for_failure, error_msg_for_failure
            )

        # Return None after all retries failed
        return None

    async def _get_order_status(
        self, client: ExchangeAPI, exchange_id: str, order_id: str, symbol: str # Added symbol for context
    ) -> Order | None:
        """
        Get the status of an order.
        Args:
            client: ExchangeAPI client
            exchange_id: Exchange identifier
            order_id: Order identifier
            symbol: The exchange-specific symbol (for logging/context).

        Returns:
            Order object if found, None otherwise.
        """
        # Note: Retries might be better handled in the calling function (_verify_order_state)
        # For now, keeping retry logic here is complex. Assume get_order_status handles its own transient errors.
        try:
            logger.debug(f"Getting order status for ID: {order_id} ({symbol}) on {exchange_id}")
            order = await client.get_order_status(order_id=order_id, symbol=symbol) # Pass symbol if required by API

            if order:
                logger.debug(f"Got order status for {order_id}: {order.status.name}, Filled: {order.filled_quantity}")
                # Update portfolio tracker (uses order details including symbol)
                self.portfolio_tracker.update_order(exchange_id, order)
                return order
            else:
                logger.warning(f"get_order_status returned None for ID {order_id} on {exchange_id}")
                return None
        except APIError as e:
            logger.error(
                f"API Error getting order status for {order_id} ({symbol}) on {exchange_id}: {e}"
            )
            # Record API error with circuit breaker
            if self.circuit_breaker_system:
                self.circuit_breaker_system.record_api_error(exchange_id, f"Get order status failed: {e}")
            # Re-raise or return None based on desired handling
            # For now, returning None to indicate failure to retrieve status
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error getting order status for {order_id} ({symbol}) on {exchange_id}: {e}",
                exc_info=True,
            )
             # Record generic failure with circuit breaker
            if self.circuit_breaker_system:
               self.circuit_breaker_system.record_api_error(exchange_id, f"Unexpected error getting order status: {e}")
            return None

    async def _compensate_position(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        internal_symbol: str, # Changed to internal symbol for context
        original_failed_side: OrderSide,
        quantity: Decimal,
    ) -> bool:
        """Attempts to compensate for a partially filled or failed order leg."""
        logger.info(
            f"Attempting compensation on {exchange_id} for internal symbol '{internal_symbol}' "
            f"(original failed side: {original_failed_side.value}) for quantity {quantity}"
        )

        # --- Use SymbolMapper to get the required exchange-specific symbol ---
        exchange_symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange_id)
        if not exchange_symbol:
             logger.error(
                 f"Symbol mapping failed during compensation: Cannot find exchange symbol for "
                 f"internal symbol '{internal_symbol}' on '{exchange_id}'. Compensation ABORTED."
             )
             # Record critical failure? Depends on policy.
             if self.circuit_breaker_system:
                 self.circuit_breaker_system.record_critical_failure(exchange_id, f"Symbol mapping failed during compensation for {internal_symbol}")
             return False # Cannot proceed without the correct symbol
        # -------------------------------------------------------------------

        logger.debug(f"Compensation using exchange symbol: '{exchange_symbol}'")

        use_limit_compensation = self.config.get("execution.compensation.use_limit_orders", False)
        price_offset_config_key = "execution.compensation.limit_price_offset_pct"
        default_offset = 0.05
        price_offset_str = str(
            self.config.get(price_offset_config_key, default_offset)
        )
        price_offset_dec = Decimal(price_offset_str)
        price_offset_fraction = price_offset_dec / Decimal("100.0")

        limit_price = None
        order_type_to_use = OrderType.MARKET

        if use_limit_compensation:
            try:
                # --- Use the mapped exchange_symbol for ticker lookup ---
                ticker = await client.get_ticker(exchange_symbol)
                # -------------------------------------------------------
                if ticker and ticker.bid is not None and ticker.ask is not None and ticker.bid > 0 and ticker.ask > 0:
                     bid_price = Decimal(str(ticker.bid))
                     ask_price = Decimal(str(ticker.ask))

                     # Determine compensation side
                     # If original failed leg was SELL, we are LONG, need to SELL to close.
                     # If original failed leg was BUY, we are SHORT, need to BUY to close.
                     compensating_side = OrderSide.SELL if original_failed_side == OrderSide.SELL else OrderSide.BUY

                     if compensating_side == OrderSide.SELL:
                        # Selling to close long: set limit slightly below current bid
                        limit_price = bid_price * (Decimal("1") - price_offset_fraction)
                     else: # compensating_side == OrderSide.BUY
                        # Buying to close short: set limit slightly above current ask
                        limit_price = ask_price * (Decimal("1") + price_offset_fraction)

                     if limit_price > 0:
                        order_type_to_use = OrderType.LIMIT
                        logger.info(
                            f"Calculated compensating limit price for '{exchange_symbol}': {limit_price:.4f} "
                            f"(Comp Side: {compensating_side.name}, Offset: {price_offset_dec:.3f}%)"
                        )
                     else:
                         logger.warning(
                             f"Calculated invalid limit price ({limit_price}) for '{exchange_symbol}' "
                             f"compensation. Falling back to MARKET order."
                         )
                else:
                    logger.warning(
                        f"Could not get valid ticker bid/ask for '{exchange_symbol}' on {exchange_id} to set limit price. "
                        f"Falling back to MARKET order."
                    )
            except Exception as ticker_err:
                logger.error(
                    f"Error getting ticker for limit price calculation on {exchange_id} for '{exchange_symbol}': "
                    f"{ticker_err}. Falling back to MARKET order.", exc_info=True
                )
        else:
            logger.info(f"Market order configured for compensation for '{exchange_symbol}'.")

        # --- Place the Compensating Order --- 
        try:
            # Determine the side for the compensating order (opposite of the position established)
            compensating_side = (
                OrderSide.SELL if original_failed_side == OrderSide.SELL else OrderSide.BUY
            )
            logger.info(
                f"Placing compensating order: {compensating_side.name} {quantity} {exchange_symbol}@{order_type_to_use.name} on {exchange_id}"
            )

            # Place the compensating order (MUST be reduce_only)
            compensating_order = await self._place_order_with_retry(
                client=client,
                exchange_id=exchange_id,
                symbol=exchange_symbol, # Use mapped exchange symbol
                side=compensating_side,
                order_type=order_type_to_use,
                quantity=quantity,
                price=limit_price, # Will be None for MARKET orders
                reduce_only=True, # CRITICAL: Compensation orders must be reduce-only
            )

            if compensating_order:
                 logger.info(f"Compensation order submitted successfully for {exchange_symbol}: ID {compensating_order.id}, Status: {compensating_order.status.name}")
                 # TODO: Need to monitor the status of this compensating order
                 # For now, assume success if placed without error.
                 # A robust implementation would wait for fill confirmation.
                 if compensating_order.status not in (OrderStatus.FAILED, OrderStatus.REJECTED, OrderStatus.CANCELED):
                    logger.info(f"Compensation order {compensating_order.id} placed successfully for {exchange_symbol}.")
                    return True
                 else:
                    logger.error(f"Compensation order {compensating_order.id} for {exchange_symbol} failed/rejected/cancelled. Status: {compensating_order.status.name}")
                    return False
            else:
                logger.error(f"Failed to place compensating order for {exchange_symbol} on {exchange_id} after retries.")
                return False

        except Exception as comp_err:
            logger.exception(
                f"Unexpected error during compensation order placement for {exchange_symbol} on {exchange_id}: {comp_err}"
            )
            # Record critical failure
            if self.circuit_breaker_system:
               self.circuit_breaker_system.record_critical_failure(exchange_id, f"Compensation placement failed for {exchange_symbol}: {comp_err}")
            return False

    def get_execution_history(self) -> list[TradeExecution]:
        """
        Get the execution history.

        Returns:
            List of trade executions
        """
        return self.execution_history.copy()

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
    ) -> Order | None:  # Changed
        """
        Verify the final state of an order after execution attempt.

        Args:
            execution_id: The ID of the trade execution context.
            exchange: Exchange identifier.
            order_id: Order identifier.

        Returns:
            The final Order object if successfully retrieved, else None.
        """
        api_client = self.api_clients.get(exchange)
        if not api_client:
            self.logger.warning("API client not found for verification", exchange=exchange)
            return None

        try:
            # Corrected: get_order -> get_order_status (assuming this is the method)
            # Need to verify the actual method in ExchangeAPI
            # order_data = await api_client.get_order(order_id)
            order_data = await api_client.get_order_status(order_id=order_id)
            if order_data:
                # Assuming get_order_status returns data parsable into an Order object
                # Corrected: Ensure return matches annotation
                parsed_order = api_client.parse_order(
                    order_data
                )  # Or use a dedicated status parsing method
                return parsed_order
            else:
                self.logger.warning(
                    "Order not found on exchange during verification", order_id=order_id
                )
                return None
        except APIError as e:
            # Indentation fixed
            self.logger.error("API error during order verification", order_id=order_id, error=e)
            # Handle specific errors if needed (e.g., OrderNotFound)
            return None
        except Exception:
            # Indentation fixed
            self.logger.exception("Unexpected error during order verification", order_id=order_id)
            return None

    async def _update_pnl(self, execution: TradeExecution) -> None:
        """Updates PNL based on filled order information."""
        if execution.status != ExecutionStatus.COMPLETED or not execution.order:
            return

        order = execution.order
        if order.status == OrderStatus.FILLED and order.avg_fill_price is not None:
            position = self.portfolio_tracker.get_position(order.symbol)
            if position and position.entry_price is not None:
                # Corrected type hint needed for pnl_contribution
                pnl_contribution: Decimal | None = None
                # Calculate PNL (ensure Decimal math)
                entry_price = position.entry_price  # Already Decimal
                avg_fill_price = order.avg_fill_price  # Already Decimal
                filled_quantity = order.filled_quantity  # Already Decimal

                # Corrected: Assign Decimal result to Optional[Decimal]
                if order.side == OrderSide.BUY:
                    pnl_contribution = (avg_fill_price - entry_price) * filled_quantity
                else:  # SELL
                    pnl_contribution = (entry_price - avg_fill_price) * filled_quantity

                if pnl_contribution is not None:
                    # Update realized PNL in portfolio tracker
                    # This logic might need refinement based on how PNL is tracked
                    self.portfolio_tracker.update_realized_pnl(order.symbol, pnl_contribution)
                    self.logger.info(
                        "Updated realized PNL", order_id=order.id, pnl=pnl_contribution
                    )
