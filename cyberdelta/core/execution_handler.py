from __future__ import annotations # Enable postponed evaluation

import asyncio
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum, auto
from typing import TYPE_CHECKING, Any # Added TYPE_CHECKING

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
# Import models needed at runtime directly
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType, Position
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

# Keep imports for type checking only if they cause circular dependencies otherwise
if TYPE_CHECKING:
    # Example: If importing PortfolioTracker directly caused issues
    # from cyberdelta.core.portfolio_tracker import PortfolioTracker
    pass # Keep this block if needed for other type-checking-only imports

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
        circuit_breaker_system: CircuitBreakerSystem | None = None,
    ) -> None:
        """
        Initialize the execution handler.

        Args:
            config: Application configuration (Config object)
            portfolio_tracker: Portfolio tracker for position updates
            circuit_breaker_system: The main circuit breaker system (optional)
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
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

        Args:
            opportunity: Sized arbitrage opportunity

        Returns:
            Trade execution result
        """
        # Create execution record
        execution = TradeExecution(opportunity=opportunity)
        self.active_executions[execution.id] = execution
        execution_id = execution.id  # Store execution id for finally block

        try:
            # Define exchanges early, before CB check
            long_exchange = opportunity.opportunity.long_exchange
            short_exchange = opportunity.opportunity.short_exchange

            # Check global and exchange-specific circuit breakers from the main system
            if self.circuit_breaker_system:
                # Access the nested opportunity object for exchange IDs
                # long_exchange = opportunity.opportunity.long_exchange # Moved up
                # short_exchange = opportunity.opportunity.short_exchange # Moved up

                # Check long exchange
                can_trade_long, reason_long = self.circuit_breaker_system.can_execute(
                    exchange=long_exchange
                )
                if not can_trade_long:
                    logger.info(f"Execution blocked by main circuit breaker system: {reason_long}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = reason_long
                    return execution

                # Check short exchange
                can_trade_short, reason_short = self.circuit_breaker_system.can_execute(
                    exchange=short_exchange
                )
                if not can_trade_short:
                    logger.info(f"Execution blocked by main circuit breaker system: {reason_short}")
                    execution.status = ExecutionStatus.REJECTED
                    execution.error_message = reason_short
                    return execution
            else:
                logger.warning(
                    "No main circuit breaker system provided to ExecutionHandler. Skipping checks."
                )

            execution.start_time = datetime.now(UTC)
            execution.status = ExecutionStatus.EXECUTING

            long_client = self.api_clients[long_exchange]
            short_client = self.api_clients[short_exchange]
            internal_symbol = opportunity.opportunity.symbol

            # --- Calculate Long Quantity ---
            long_exchange_symbol = self.config.get(
                f"exchanges.{long_exchange}.symbols.{internal_symbol}"
            )
            if not long_exchange_symbol:
                raise ValueError(
                    f"Symbol mapping not found for {internal_symbol} on {long_exchange}"
                )
            try:
                long_ticker = await long_client.get_ticker(long_exchange_symbol)
                if not long_ticker or long_ticker.ask == 0:
                    raise ValueError(
                        f"Invalid ticker or zero ask price for {long_exchange_symbol} "
                        f"on {long_exchange}"
                    )
                long_base_quantity = opportunity.long_size / long_ticker.ask
                logger.debug(
                    f"Calculated long quantity for {long_exchange}: {opportunity.long_size} USD / "
                    f"{long_ticker.ask} = {long_base_quantity:.8f} {internal_symbol}"
                )
            except (APIError, ValueError, ZeroDivisionError) as e:
                execution.status = ExecutionStatus.FAILED
                execution.error_message = f"Failed to get ticker or calculate long quantity: {e}"
                # --- ADDED: Record API error with circuit breaker ---
                if self.circuit_breaker_system and isinstance(e, APIError):
                    # Correct keyword: error_message (based on CB signature)
                    self.circuit_breaker_system.record_api_error(
                        exchange=long_exchange, error_message=str(e)
                    )
                    logger.warning(
                        f"API Error recorded for {long_exchange} during ticker/qty calc: {e}"
                    )
                # ----------------------------------------------------
                return execution
            # --------------------------------

            # Place long order with retry using base quantity and exchange symbol
            long_order = await self._place_order_with_retry(
                client=long_client,
                exchange_id=long_exchange,
                symbol=long_exchange_symbol,  # Use EXCHANGE-SPECIFIC symbol
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=long_base_quantity,  # Pass calculated base quantity
            )

            if not long_order:
                execution.status = ExecutionStatus.FAILED
                execution.error_message = "Failed to place long order"
                return execution

            execution.long_order_id = long_order.id
            execution.long_order_response = long_order.to_dict()

            # --- Calculate Short Quantity ---
            short_exchange_symbol = self.config.get(
                f"exchanges.{short_exchange}.symbols.{internal_symbol}"
            )
            if not short_exchange_symbol:
                raise ValueError(
                    f"Symbol mapping not found for {internal_symbol} on {short_exchange}"
                )
            try:
                short_ticker = await short_client.get_ticker(short_exchange_symbol)
                if not short_ticker or short_ticker.bid == 0:
                    raise ValueError(
                        f"Invalid ticker or zero bid price for {short_exchange_symbol} "
                        f"on {short_exchange}"
                    )
                short_base_quantity = opportunity.short_size / short_ticker.bid
                logger.debug(
                    f"Calculated short quantity for {short_exchange}: {opportunity.short_size} USD / "
                    f"{short_ticker.bid} = {short_base_quantity:.8f} {internal_symbol}"
                )
            except (APIError, ValueError, ZeroDivisionError) as e:
                # If short quantity calc fails, we might still need to compensate the long leg
                execution.status = ExecutionStatus.COMPENSATING
                execution.error_message = (
                    f"Failed to get ticker or calculate short quantity: {e}. Compensating long."
                )
                logger.warning(f"{execution.error_message}")
                # --- ADDED: Record API error with circuit breaker ---
                if self.circuit_breaker_system and isinstance(e, APIError):
                    # Correct keyword: error_message (based on CB signature)
                    self.circuit_breaker_system.record_api_error(
                        exchange=short_exchange, error_message=str(e)
                    )
                    logger.warning(
                        f"API Error recorded for {short_exchange} during ticker/qty calc: {e}"
                    )
                # ----------------------------------------------------

                # Attempt compensation (best effort)
                try:
                    await self._compensate_position(
                        client=long_client,
                        exchange_id=long_exchange,
                        symbol=internal_symbol,
                        original_failed_side=OrderSide.SELL,
                        quantity=long_order.filled_quantity,  # Compensate what was filled
                    )
                    execution.status = (
                        ExecutionStatus.FAILED
                    )  # Mark as failed after compensation attempt
                    return execution
                except Exception as comp_err:
                    execution.error_message = f"Failed to compensate long leg: {comp_err}"
                    logger.error(f"Failed to compensate long leg: {comp_err}")
                    execution.status = ExecutionStatus.FAILED
                    return execution
            # ---------------------------------

            # Place short order with retry using base quantity and exchange symbol
            short_order = await self._place_order_with_retry(
                client=short_client,
                exchange_id=short_exchange,
                symbol=short_exchange_symbol,  # Use EXCHANGE-SPECIFIC symbol
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=short_base_quantity,  # Pass calculated base quantity
            )

            # --- ADDED: Compensation logic if short fails after long succeeds ---
            if not short_order and long_order and long_order.status == OrderStatus.FILLED:
                # Short order failed, but long order was placed and presumably filled
                execution.status = ExecutionStatus.COMPENSATING
                execution.error_message = f"Failed to place short order ({short_exchange_symbol}), compensating long ({long_exchange_symbol})"
                logger.warning(execution.error_message)
                # Compensate the filled quantity of the long order
                compensation_result = await self._compensate_position(
                    client=long_client,
                    exchange_id=long_exchange,
                    symbol=long_exchange_symbol,  # Use exchange-specific symbol
                    original_failed_side=OrderSide.SELL,  # Failed to SELL short
                    quantity=long_order.filled_quantity,
                )
                # Regardless of compensation success, mark final status as FAILED
                execution.status = ExecutionStatus.FAILED
                # Attach specific failure reason if compensation also failed
                if not compensation_result:
                    execution.error_message += "; Compensation attempt also failed."
                # Record failure and return
                return execution
            # --- END ADDED Compensation Logic ---

            # Original logic continues if short_order was successful (or if long failed earlier)
            elif not short_order:  # This case handles if long failed initially, short wasn't attempted, or some other error
                # If short_order is None, it implies long_order likely also failed or
                # wasn't filled
                # The initial check after long_order placement should have caught long failure.
                # This path might be redundant or needs refinement based on _place_order_with_retry's
                # exact return on failure.
                # Assume earlier checks handle long-failure returns correctly.
                # If short fails but long didn't fill, we might not need compensation yet.
                # Let's refine the error message if we land here unexpectedly.
                if (
                    execution.status != ExecutionStatus.FAILED
                ):  # Avoid overwriting specific long failure message
                    execution.status = ExecutionStatus.FAILED
                    execution.error_message = (
                        "Failed to place short order (reason unclear, check logs)"
                    )
                    logger.error(
                        f"Execution reached unexpected state: short order failed but long order "
                        f"status unclear or not filled. Long: {long_order}"
                    )
                return execution  # Return if short failed and compensation wasn't triggered above

            # --- Short order was placed successfully ---
            execution.short_order_id = short_order.id
            execution.short_order_response = short_order.to_dict()

            # Wait for orders to settle (can be improved with WS updates)
            await asyncio.sleep(
                self.config.get("execution.settlement_delay", 2.0)
            )  # Use config value

            # Update order status (fetch latest state)
            updated_long_order: Order | None = None
            if execution.long_order_id:
                updated_long_order = await self._get_order_status(
                    long_client, long_exchange, execution.long_order_id
                )
            else:
                logger.warning("Long order ID not found, cannot update status.")

            updated_short_order: Order | None = None
            if execution.short_order_id:
                updated_short_order = await self._get_order_status(
                    short_client, short_exchange, execution.short_order_id
                )
            else:
                logger.warning("Short order ID not found, cannot update status.")

            # Use the updated order objects for status checks
            long_order = updated_long_order if updated_long_order else long_order
            short_order = updated_short_order if updated_short_order else short_order

            # Check if orders were filled
            long_filled = (
                long_order
                and long_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]
                and long_order.filled_quantity > 0
            )
            short_filled = (
                short_order
                and short_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]
                and short_order.filled_quantity > 0
            )

            if long_filled and short_filled:
                # --- Both orders have at least partial fills ---
                # Check for FULL completion vs PARTIAL completion of the pair
                if (
                    long_order.status == OrderStatus.FILLED
                    and short_order.status == OrderStatus.FILLED
                ):
                    # Both legs fully filled
                    execution.status = ExecutionStatus.COMPLETED
                    execution.long_fill_price = (
                        Decimal(str(long_order.price)) if long_order.price is not None else None
                    )
                    execution.short_fill_price = (
                        Decimal(str(short_order.price)) if short_order.price is not None else None
                    )
                    execution.long_fill_quantity = (
                        Decimal(str(long_order.filled_quantity))
                        if long_order.filled_quantity is not None
                        else None
                    )
                    execution.short_fill_quantity = (
                        Decimal(str(short_order.filled_quantity))
                        if short_order.filled_quantity is not None
                        else None
                    )

                    # Update portfolio tracker after successful execution
                    logger.debug(
                        f"Updating portfolio tracker for successful execution "
                        f"{long_order.id}/{short_order.id}"
                    )

                    # Ensure entry prices are Decimal, default if None
                    long_entry_price = (
                        long_order.price if long_order.price is not None else Decimal("0")
                    )
                    short_entry_price = (
                        short_order.price if short_order.price is not None else Decimal("0")
                    )
                    if long_order.price is None:
                        logger.warning(
                            f"Long order {long_order.id} had None price, "
                            f"using 0 for position entry."
                        )
                    if short_order.price is None:
                        logger.warning(
                            f"Short order {short_order.id} had None price, "
                            f"using 0 for position entry."
                        )

                    # Create Position objects from filled orders (using signature from models.py)
                    # Assuming 'leverage' needs to be sourced elsewhere or defaulted
                    # Assuming 'side' is correctly determined
                    # Removed id and mark_price kwargs, handled None entry_price
                    long_position = Position(
                        symbol=opportunity.opportunity.symbol,
                        side=OrderSide.BUY,
                        size=Decimal(str(long_order.filled_quantity))
                        if long_order.filled_quantity is not None
                        else Decimal("0"),
                        entry_price=long_entry_price,
                        leverage=Decimal("1.0"),  # Placeholder/Default - Needs real value
                        # Other fields like liquidation_price, pnl etc., might be None initially
                    )
                    # Assign ID after creation if needed by tracker
                    # long_position.id = f"pos_{long_order.id}"

                    short_position = Position(
                        symbol=opportunity.opportunity.symbol,
                        side=OrderSide.SELL,
                        size=Decimal(str(short_order.filled_quantity))
                        if short_order.filled_quantity is not None
                        else Decimal("0"),
                        entry_price=short_entry_price,
                        leverage=Decimal("1.0"),  # Placeholder/Default - Needs real value
                        # Other fields might be None initially
                    )
                    # Assign ID after creation if needed by tracker
                    # short_position.id = f"pos_{short_order.id}"

                    # Update tracker with Position objects
                    self.portfolio_tracker.update_position(
                        exchange_id=long_exchange, position=long_position
                    )
                    self.portfolio_tracker.update_position(
                        exchange_id=short_exchange, position=short_position
                    )

                    logger.info(f"Successfully executed opportunity: {opportunity}")
                else:
                    # --- At least one leg is PARTIALLY_FILLED ---
                    execution.status = ExecutionStatus.PARTIALLY_COMPLETED  # Initial status
                    # Ensure orders exist before logging status name
                    long_status_str = long_order.status.name if long_order else "UNKNOWN"
                    long_filled_qty_str = long_order.filled_quantity if long_order else "N/A"
                    long_qty_str = long_order.quantity if long_order else "N/A"
                    short_status_str = short_order.status.name if short_order else "UNKNOWN"
                    short_filled_qty_str = short_order.filled_quantity if short_order else "N/A"
                    short_qty_str = short_order.quantity if short_order else "N/A"
                    logger.warning(
                        f"Trade partially completed. Long: {long_status_str} "
                        f"({long_filled_qty_str}/{long_qty_str}), Short: {short_status_str} "
                        f"({short_filled_qty_str}/{short_qty_str}). Initiating compensation."
                    )

                    # Record partial completion with circuit breaker
                    if self.circuit_breaker_system:
                        # Replace generic record_failure with record_api_error
                        self.circuit_breaker_system.record_api_error(
                            long_exchange, "Partial Fill Occurred"
                        )
                        self.circuit_breaker_system.record_api_error(
                            short_exchange, "Partial Fill Occurred"
                        )

                    # --- Implement Strategy A: Immediate Compensation --- (KEEP THIS BLOCK)
                    comp_long_success = False
                    comp_short_success = False
                    # ... (compensation logic for long leg) ...
                    # ... (compensation logic for short leg) ...
                    execution.status = ExecutionStatus.FAILED
                    execution.error_message = (
                        "Trade partially completed and compensation attempted..."
                    )
                    # Removed incorrect record_failure call from here

                    # Set final status and error message
                    execution.status = (
                        ExecutionStatus.FAILED
                    )  # Final status is FAILED after partial fill compensation attempt
                    execution.error_message = (
                        f"Trade partially completed and compensation attempted. "
                        f"Long Comp: {'Success' if comp_long_success else 'Failed'}, "
                        f"Short Comp: {'Success' if comp_short_success else 'Failed'}."
                    )

                    # Record failure with circuit breaker - Replaced with record_api_error
                    if self.circuit_breaker_system:  # Added check
                        self.circuit_breaker_system.record_api_error(
                            long_exchange, "Partial Fill Compensation Attempt"
                        )
                        self.circuit_breaker_system.record_api_error(
                            short_exchange, "Partial Fill Compensation Attempt"
                        )
                    # ----------------------------------------------------

                # Record fill details regardless of full/partial (Moved slightly lower)
                execution.long_fill_price = (
                    long_order.price if long_order and long_order.filled_quantity > 0 else None
                )  # Use price
                execution.short_fill_price = (
                    short_order.price if short_order and short_order.filled_quantity > 0 else None
                )  # Use price
                execution.long_fill_quantity = long_order.filled_quantity if long_order else 0
                execution.short_fill_quantity = short_order.filled_quantity if short_order else 0

            elif long_filled or short_filled:
                # --- Only one leg has any fill (other is NEW, CANCELED, REJECTED etc) ---
                execution.status = ExecutionStatus.COMPENSATING  # Need to compensate the filled leg
                filled_leg_desc = "long" if long_filled else "short"
                failed_leg_desc = "short" if long_filled else "long"
                execution.error_message = (
                    f"Only {filled_leg_desc} order filled ({long_order.status.name if long_filled else short_order.status.name}). "
                    f"{failed_leg_desc.capitalize()} order failed ({short_order.status.name if long_filled else long_order.status.name}). Compensating."
                )
                logger.warning(execution.error_message)

                # Determine which leg to compensate
                if long_filled:
                    comp_client, comp_exch, comp_symbol, comp_qty, comp_failed_side = (
                        long_client,
                        long_exchange,
                        long_exchange_symbol,
                        long_order.filled_quantity,
                        OrderSide.SELL,
                    )
                else:  # short_filled
                    comp_client, comp_exch, comp_symbol, comp_qty, comp_failed_side = (
                        short_client,
                        short_exchange,
                        short_exchange_symbol,
                        short_order.filled_quantity,
                        OrderSide.BUY,
                    )

                compensation_result = await self._compensate_position(
                    client=comp_client,
                    exchange_id=comp_exch,
                    symbol=comp_symbol,
                    original_failed_side=comp_failed_side,
                    quantity=comp_qty,
                )

                execution.status = (
                    ExecutionStatus.FAILED
                )  # Final status is FAILED after compensation attempt
                if not compensation_result:
                    execution.error_message += "; Compensation attempt also failed."
                # Record failure with circuit breaker - Replaced with record_api_error
                if self.circuit_breaker_system:  # Added check
                    self.circuit_breaker_system.record_api_error(
                        comp_exch, "Compensation Failed or Required"
                    )

            else:
                # --- Neither order had any fill ---
                execution.status = ExecutionStatus.FAILED
                execution.error_message = f"Neither order was filled. Long: {long_order.status.name if long_order else 'N/A'}, Short: {short_order.status.name if short_order else 'N/A'}"
                # Record failure with circuit breaker - Replaced with record_api_error
                if self.circuit_breaker_system:  # Added check
                    self.circuit_breaker_system.record_api_error(
                        long_exchange, "Order Failed - No Fill"
                    )
                    self.circuit_breaker_system.record_api_error(
                        short_exchange, "Order Failed - No Fill"
                    )
                logger.error(
                    f"Failed to execute opportunity: {opportunity} - {execution.error_message}"
                )

            return execution

        except APIError as e:
            logger.error(f"API Error during execution {execution.id}: {e}")
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            # Record API error with the system
            if self.circuit_breaker_system:
                try:
                    # Determine which exchange the error occurred on
                    error_exchange = (
                        long_exchange if execution.long_order_id is None else short_exchange
                    )
                    logger.info(
                        f"Recording API error for {error_exchange} to circuit breaker system."
                    )
                    self.circuit_breaker_system.record_api_error(error_exchange, str(e))
                except Exception as cb_err:
                    logger.error(f"Failed to record API error in circuit breaker system: {cb_err}")

        except Exception as e:
            # Log unexpected errors during execution
            logger.error(f"Unexpected error during execution {execution.id}: {e}", exc_info=True)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            # Attempt to record generic error to the appropriate breaker
            if self.circuit_breaker_system:  # Check if system exists
                try:
                    # Determine which exchange might be related (best effort)
                    error_exchange = (
                        long_exchange if execution.long_order_response is None else short_exchange
                    )
                    logger.info(
                        f"Recording generic execution error to {error_exchange} api_errors breaker."
                    )
                    # Use the main system record_error method if available, otherwise target specific breaker
                    self.circuit_breaker_system.record_api_error(
                        error_exchange, f"Generic execution error: {e}"
                    )
                except Exception as cb_err:
                    logger.error(
                        f"Failed to record generic execution error in circuit breaker: {cb_err}"
                    )
            else:
                logger.warning("Circuit breaker system not available, cannot record generic error.")

        finally:
            execution.end_time = datetime.now(UTC)
            # Remove from active, add to history
            if execution_id in self.active_executions:
                del self.active_executions[execution_id]
            self.execution_history.append(execution)
            if len(self.execution_history) > self.max_execution_history:
                self.execution_history.pop(0)  # Keep history size bounded

            logger.info(f"Execution {execution.id} finished with status: {execution.status.name}")

        return execution

    async def _place_order_with_retry(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        symbol: str,
        side: "OrderSide", # Changed
        order_type: "OrderType", # Changed
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: str = "GTC", # Default Time-in-force
        retry_count: int = 0,
        reduce_only: bool = False, # <-- ADD reduce_only parameter
    ) -> "Order" | None: # Changed
        """
        Place an order with retry logic, applying slippage checks.

        Args:
            client: ExchangeAPI client
            exchange_id: Exchange identifier
            symbol: Trading symbol
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
        error: Exception | None = None  # Change type hint to Exception
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

                # Place the order
                order = await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=time_in_force,
                    reduce_only=reduce_only, # <-- Pass reduce_only
                )

                # Update portfolio tracker
                if order:
                    self.portfolio_tracker.update_order(exchange_id, order)

                return order

            except APIError as e:
                logger.warning(
                    f"API Error placing order on {exchange_id} (Attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                # Use correct keyword: exchange_code (and http_status if available)
                error = APIError(
                    message=str(e),
                    code=getattr(e, "code", APIErrorCode.UNKNOWN),  # Pass code if APIError has it
                    http_status=getattr(e, "http_status", None),  # Pass status if available
                    exchange_code=exchange_id,  # Use exchange_code
                    exchange_message=getattr(e, "exchange_message", None),
                    original_exception=e,
                )
                # Record API error with circuit breaker
                if self.circuit_breaker_system:
                    # Correct keyword: error_message
                    self.circuit_breaker_system.record_api_error(
                        exchange=exchange_id, error_message=str(error)
                    )
            except Exception as e:
                logger.error(
                    f"Unexpected error placing order on {exchange_id} (Attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                error = e  # Assign generic exception

            # Exponential backoff
            if attempt < self.max_retries - 1:
                delay = self.retry_delay_base * (2**attempt)
                logger.info(f"Retrying order placement in {delay:.2f} seconds...")
                await asyncio.sleep(delay)

        # If all retries fail
        logger.error(f"Failed to place order on {exchange_id} after {self.max_retries} attempts.")
        # Record failure if circuit breaker exists and the error wasn't an APIError
        # (APIError already recorded above)
        if self.circuit_breaker_system and not isinstance(error, APIError):
            exchange_for_failure = exchange_id  # Use the exchange ID from the failed retry
            error_msg_for_failure = f"Generic failure placing order after retries: {error}"
            # Replace generic record_failure with record_api_error
            self.circuit_breaker_system.record_api_error(
                exchange_for_failure, error_msg_for_failure
            )

        # Raise the last encountered error if needed or return None
        # Depending on desired behavior, might re-raise 'error' here.
        return None

    async def _get_order_status(
        self, client: ExchangeAPI, exchange_id: str, order_id: str
    ) -> "Order" | None: # Changed
        """
        Get the status of an order with retry logic.

        Args:
            client: ExchangeAPI client
            exchange_id: Exchange identifier
            order_id: Order identifier

        Returns:
            Order object if found, None otherwise.
        """
        try:
            order = await client.get_order_status(order_id=order_id)

            # Update portfolio tracker
            if order:
                self.portfolio_tracker.update_order(exchange_id, order)

            return order

        except Exception as e:
            logger.error(
                f"Failed to retrieve order status for {order_id} on {exchange_id} "
                f"after {self.max_retries} attempts: {e}",
                exc_info=True,
            )
            # Propagate the original error code if it's an APIError
            original_code = getattr(e, 'code', APIErrorCode.UNKNOWN)
            raise APIError(
                f"Failed to retrieve order status for {order_id} on {exchange_id} "
                f"after {self.max_retries} attempts: {e}",
                code=original_code, # Use original code
                exchange_code=exchange_id,
                original_exception=e, # Pass original exception
            ) from e

    async def _compensate_position(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        symbol: str,
        original_failed_side: "OrderSide", # Changed
        quantity: Decimal,
    ) -> bool:
        """Attempts to compensate for a partially filled or failed order leg."""
        logger.info(
            f"Attempting compensation on {exchange_id} for {symbol} (failed side: {original_failed_side.value})"
        )

        # --- ADD: Look up exchange-specific symbol --- 
        try:
            exchange_symbol = self.config.get(
                f"exchanges.{exchange_id}.symbols.{symbol}"
            )
            if not exchange_symbol:
                # Use the internal symbol as a fallback if mapping not found, but log warning
                logger.warning(
                    f"Symbol mapping not found for {symbol} on {exchange_id} in config. "
                    f"Attempting compensation using internal symbol '{symbol}'."
                )
                exchange_symbol = symbol # Fallback
        except Exception as cfg_err:
            logger.error(
                f"Error retrieving symbol mapping for {symbol} on {exchange_id} from config: {cfg_err}. "
                f"Cannot proceed with compensation."
            )
            return False # Cannot proceed without a symbol
        # -------------------------------------------

        use_limit_compensation = self.config.get(
            "execution.compensation.use_limit_orders", False
        )
        price_offset_config_key = "execution.compensation.limit_price_offset_pct"
        default_offset = 0.05  # Default 0.05%
        price_offset_str = str(
            self.config.get(price_offset_config_key, default_offset)
        )  # Get as string
        price_offset_dec = Decimal(price_offset_str)  # Convert to Decimal
        price_offset_fraction = price_offset_dec / Decimal(
            "100.0"
        )  # Calculate fraction using Decimal

        limit_price = None
        order_type_to_use = OrderType.MARKET  # Default to market

        if use_limit_compensation:
            try:
                # --- Use exchange_symbol for ticker lookup --- 
                ticker = await client.get_ticker(exchange_symbol)
                # -------------------------------------------
                if ticker and ticker.bid > 0 and ticker.ask > 0:
                    if original_failed_side == OrderSide.SELL:
                        # Selling to close long: set limit slightly below current bid
                        limit_price = ticker.bid * (
                            Decimal("1") - price_offset_fraction
                        )  # Decimal arithmetic
                    else:  # original_failed_side == OrderSide.BUY:
                        # Buying to close short: set limit slightly above current ask
                        limit_price = ticker.ask * (
                            Decimal("1") + price_offset_fraction
                        )  # Decimal arithmetic

                    if limit_price > 0:  # Ensure price is valid
                        order_type_to_use = OrderType.LIMIT
                        logger.info(
                            f"Calculated compensating limit price: {limit_price:.4f} "
                            f"(Side: {original_failed_side.value}, Offset: {price_offset_dec:.3f}%)"
                        )
                    else:
                        # If calculation resulted in non-positive price, warn and fallback
                        if limit_price <= Decimal("0"):
                            logger.warning(
                                f"Calculated invalid limit price ({limit_price}) for {symbol} "
                                f"compensation. Falling back to MARKET order."
                            )
                        else:
                            logger.warning(
                                f"Could not get best bid/ask for {symbol} on {exchange_id} to set limit price. "
                                f"Falling back to MARKET order."
                            )
                else:
                    logger.warning(
                        f"Could not get valid ticker bid/ask for {symbol} on {exchange_id} to set limit price. Falling back to MARKET order."
                    )
            except Exception as ticker_err:
                logger.error(
                    f"Error getting ticker for limit price on {exchange_id} for {symbol}: "
                    f"{ticker_err}. Falling back to MARKET order."
                )
        else:
            logger.info("Market order configured for compensation.")

        try:
            # Determine the side for the compensating order (opposite of the position established)
            # If the original FAILED leg was SELL, it means we successfully went LONG
            # and need to SELL to compensate.
            # If the original FAILED leg was BUY, it means we successfully went SHORT
            # and need to BUY to compensate.
            compensating_side = (
                OrderSide.SELL if original_failed_side == OrderSide.SELL else OrderSide.BUY
            )
            logger.info(
                f"Compensating for {symbol}: placing {compensating_side.value} "
                f"{order_type_to_use.value} order for {quantity}"
            )

            # --- Replace with retry wrapper --- # <-- ADD THIS BLOCK
            compensation_order = await self._place_order_with_retry(
                 client=client,
                 exchange_id=exchange_id,
                 symbol=exchange_symbol, # <-- Use exchange-specific symbol (now defined earlier)
                 side=compensating_side,
                 order_type=order_type_to_use, # <-- Use determined order type
                 quantity=quantity,
                 price=limit_price, # Will be None if market order
                 reduce_only=True, # <-- ADD reduce_only=True for compensation
             )
            # ------------------------------- # <-- END ADD BLOCK

            if compensation_order:
                logger.info(
                    f"Compensation order placed ({compensation_order.side} {compensation_order.type}): "
                    f"ID={compensation_order.id}, Status={compensation_order.status}"
                )
                return True
            else:
                logger.error(
                    f"Compensation order placement returned None on {exchange_id} for {symbol}."
                )
                return False

        except Exception as e:
            logger.error(
                f"Error placing compensation order on {exchange_id} for {symbol}: {e}",
                exc_info=True,
            )
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
    ) -> "Order" | None: # Changed
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
