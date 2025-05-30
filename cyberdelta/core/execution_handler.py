from __future__ import annotations  # Enable postponed evaluation

import asyncio
import random
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.base.exchange_api import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.config.config_models import AppSettings
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    Trade,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.circuit_breaker import (
    CircuitBreakerSystem,
    CircuitBreakerTrippedError,
)

# Keep imports for type checking only if they cause circular dependencies otherwise
if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# NOTE: CyberDeltaEngine Order model uses 'client_order_id' as the unique identifier,
# 'quantity_requested' for order size, 'quantity_filled' for filled size, and
# 'average_fill_price' for fill price. There is no 'id', 'quantity',
# or 'avg_fill_price' attribute.


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
        app_settings: AppSettings,
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
        self.app_settings = app_settings
        self.portfolio_tracker = portfolio_tracker
        self.symbol_mapper = symbol_mapper
        self.circuit_breaker_system = circuit_breaker_system
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Configuration values from AppSettings
        self.max_slippage = self.app_settings.execution.max_slippage_pct
        self.max_retries = self.app_settings.execution.max_retries
        self.retry_delay_base = float(self.app_settings.execution.retry_delay_base_sec)
        # TODO: Add execution history configuration to AppSettings when needed
        self.max_execution_history = 100  # Default history size

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
                missing_client_exchange_name = (
                    opportunity.opportunity.long_exchange
                    if not long_client
                    else opportunity.opportunity.short_exchange
                )
                op_error_msg = (
                    f"Execution {execution.id} failed: Missing API client for "
                    f"'{missing_client_exchange_name}'"
                )
                logger.error(op_error_msg)
                execution.error_message = op_error_msg
                execution.status = ExecutionStatus.FAILED
                execution.end_time = datetime.now(UTC)
                self._add_to_history(execution)
                return execution

            # Get exchange-specific symbols
            long_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.long_exchange
            )
            short_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.short_exchange
            )
            if not long_symbol or not short_symbol:
                missing_leg = "long" if not long_symbol else "short"
                missing_symbol_exchange_name = (
                    opportunity.opportunity.long_exchange
                    if not long_symbol
                    else opportunity.opportunity.short_exchange
                )
                op_error_msg = (
                    f"Execution {execution.id} failed: Could not map symbol "
                    f"'{opportunity.opportunity.symbol}' for {missing_leg} leg on exchange "
                    f"'{missing_symbol_exchange_name}'."
                )
                logger.error(op_error_msg)
                execution.error_message = op_error_msg
                execution.status = ExecutionStatus.FAILED
                execution.end_time = datetime.now(UTC)
                self._add_to_history(execution)
                return execution

        except (
            APIError
        ) as e:  # Catches errors from symbol_mapper or client.get if they raised APIError
            op_error_msg = (
                f"APIError during pre-check/setup for execution {execution.id} "
                f"for {opportunity.opportunity.symbol}: {e}"
            )
            logger.error(op_error_msg, exc_info=True)
            execution.error_message = str(e)
            execution.status = ExecutionStatus.FAILED
            if self.circuit_breaker_system:
                # Determine which exchange string ID to use for recording the error
                # e.exchange_code is an int/str code, not the exchange_id string.
                # Default to long_exchange if unclear, as this is a pre-check phase error.
                failed_exchange_str = opportunity.opportunity.long_exchange
                logger.warning(
                    f"APIError in pre-check for exec {execution.id}. Attributing to: "
                    f"{failed_exchange_str}"
                )
                self.circuit_breaker_system.record_api_error(failed_exchange_str, str(e))
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution
        except Exception as e:
            op_error_msg = (
                f"Unexpected error during pre-check/setup for execution {execution.id} "
                f"for {opportunity.opportunity.symbol}: {e}"
            )
            logger.exception(op_error_msg)
            execution.error_message = str(e)
            execution.status = ExecutionStatus.FAILED
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            return execution

        # If pre-checks pass, proceed to place orders
        # This is the main execution logic that interacts with exchanges.
        try:
            await self._place_orders_for_opportunity(execution, long_symbol, short_symbol)
            # _place_orders_for_opportunity should update execution.status
            # and error_message internally.
        except APIError as e:
            # This catch block handles API errors specifically from _place_orders_for_opportunity
            op_error_msg = (
                f"APIError during order placement for execution {execution.id} "
                f"for {opportunity.opportunity.symbol}: {e}"
            )
            logger.error(op_error_msg, exc_info=True)  # Log with traceback for APIError
            execution.error_message = str(e)
            execution.status = ExecutionStatus.FAILED
            if self.circuit_breaker_system:
                # Determine which exchange caused the error
                long_not_filled = False
                if execution.long_order_response is not None:
                    long_not_filled = not execution.long_order_response.get("is_filled", True)

                short_not_filled = False
                if execution.short_order_response is not None:
                    short_not_filled = not execution.short_order_response.get("is_filled", True)

                if execution.long_order_id and not execution.short_order_id and long_not_filled:
                    failed_exchange_str = opportunity.opportunity.long_exchange
                elif execution.short_order_id and short_not_filled:
                    failed_exchange_str = opportunity.opportunity.short_exchange
                else:
                    failed_exchange_str = opportunity.opportunity.long_exchange  # Default to long
                    logger.warning(
                        f"Could not definitively determine failing exchange for APIError in exec "
                        f"{execution.id} (Error: {e}). Defaulting to long exchange: "
                        f"{failed_exchange_str}"
                    )
                self.circuit_breaker_system.record_api_error(failed_exchange_str, str(e))
        except Exception as e:
            # Catch-all for other unexpected errors from _place_orders_for_opportunity
            op_error_msg = (
                f"Unexpected error during order placement for execution {execution.id} "
                f"for {opportunity.opportunity.symbol}: {e}"
            )
            logger.exception(op_error_msg)  # Use logger.exception for full traceback
            execution.error_message = str(e)
            execution.status = ExecutionStatus.FAILED

        # --- Post-Execution Processing ---
        # Ensure end_time is set if not already by failure paths
        if execution.end_time is None:
            execution.end_time = datetime.now(UTC)

        # Log final status before adding to history
        if execution.status == ExecutionStatus.FAILED:
            logger.error(
                f"Execution {execution.id} ultimately FAILED. Reason: {execution.error_message}"
            )
        elif execution.status == ExecutionStatus.REJECTED:
            logger.warning(
                f"Execution {execution.id} was REJECTED. Reason: {execution.error_message}"
            )
        else:
            logger.info(f"Execution {execution.id} finished with status: {execution.status.name}")

        self._add_to_history(execution)
        return execution

    async def _place_orders_for_opportunity(
        self, execution: TradeExecution, long_symbol: str, short_symbol: str
    ) -> None:
        """
        Places orders for both legs of the opportunity.
        Handles sequential placement and compensation.
        This is a reconstructed method body.
        """
        logger.info(
            f"Execution {execution.id}: Placing orders for "
            f"{execution.opportunity.opportunity.symbol}"
        )
        opportunity = execution.opportunity.opportunity  # Convenience
        sized_opp = execution.opportunity  # Convenience

        # --- Calculate Base Asset Quantities ---
        base_asset_quantity_long: Decimal | None = None
        if opportunity.long_price and opportunity.long_price > Decimal("0"):
            base_asset_quantity_long = sized_opp.long_size / opportunity.long_price
        else:
            err_msg = (
                f"Execution {execution.id}: Cannot derive long base asset quantity. "
                f"Size: {sized_opp.long_size}, Price: {opportunity.long_price}"
            )
            logger.error(err_msg)
            execution.error_message = err_msg
            execution.status = ExecutionStatus.FAILED
            return

        base_asset_quantity_short: Decimal | None = None
        if opportunity.short_price and opportunity.short_price > Decimal("0"):
            base_asset_quantity_short = sized_opp.short_size / opportunity.short_price
        else:
            err_msg = (
                f"Execution {execution.id}: Cannot derive short base asset quantity. "
                f"Size: {sized_opp.short_size}, Price: {opportunity.short_price}"
            )
            logger.error(err_msg)
            execution.error_message = err_msg
            execution.status = ExecutionStatus.FAILED
            return

        # The variables are initialized to `Decimal | None = None`.
        # If the preceding logic correctly returns when prices are invalid, this block is
        # unreachable.
        # Removing this unreachable block as per analysis.
        # if base_asset_quantity_long is None or base_asset_quantity_short is None:

        # Determine default TimeInForce for initial legs
        # TODO: Add execution.default_time_in_force to AppSettings when needed
        tif_config_str = "IOC"  # Default to IOC
        try:
            default_tif = TimeInForce(tif_config_str)
        except ValueError:
            logger.warning(f"Invalid TimeInForce config '{tif_config_str}', using IOC")
            default_tif = TimeInForce.IOC

        # --- Place Long Order ---
        execution.status = ExecutionStatus.EXECUTING
        long_order_result: Order | None = await self._place_order_with_retry(
            execution=execution,
            exchange_id=opportunity.long_exchange,
            symbol=long_symbol,
            side=OrderSide.BUY,
            quantity=base_asset_quantity_long,
            order_type=OrderType.MARKET,  # TODO: Configurable order type
            is_long_leg=True,
            time_in_force=default_tif,
            reduce_only=False,
            post_only=False,
        )

        if long_order_result is None or long_order_result.status != OrderStatus.FILLED:
            err_msg = (
                f"Execution {execution.id}: Long order placement failed or not filled. "
                f"Status: {long_order_result.status if long_order_result else 'None'}"
            )
            logger.error(err_msg)
            execution.error_message = execution.error_message or err_msg
            execution.status = ExecutionStatus.FAILED
            # No compensation needed if the first leg fails before any fill
            return

        execution.long_order_id = long_order_result.exchange_order_id
        execution.long_order_response = long_order_result.model_dump(mode="json")
        execution.long_fill_price = long_order_result.average_fill_price
        execution.long_fill_quantity = long_order_result.quantity_filled
        logger.info(
            f"Execution {execution.id}: Long order placed and filled: {execution.long_order_id}"
        )
        await self._handle_filled_order(
            execution,
            long_order_result,
            opportunity.long_exchange,
            is_long_leg=True,
            expected_long_price=opportunity.long_price,
        )

        # --- Place Short Order ---
        short_order_result: Order | None = None
        try:
            short_order_result = await self._place_order_with_retry(
                execution=execution,
                exchange_id=opportunity.short_exchange,
                symbol=short_symbol,
                side=OrderSide.SELL,
                quantity=base_asset_quantity_short,
                order_type=OrderType.MARKET,  # TODO: Configurable order type
                is_long_leg=False,
                time_in_force=default_tif,
                reduce_only=False,
                post_only=False,
            )
        except APIError as e_short_leg:
            err_msg = (
                f"Execution {execution.id}: Short order placement failed with APIError: "
                f"{e_short_leg.message}"
            )
            logger.error(err_msg)
            execution.error_message = e_short_leg.message  # Preserve specific API error
            execution.status = ExecutionStatus.COMPENSATING  # Mark for compensation
            # Short leg failed, proceed to compensation if long leg was filled (which it was)
            # This block is reached only if _place_order_with_retry re-raises the APIError.
            # Ensure circuit breaker for short leg is also recorded here.
            if self.circuit_breaker_system:
                self.circuit_breaker_system.record_api_error(
                    opportunity.short_exchange, str(e_short_leg.code)
                )

        if short_order_result is None or short_order_result.status != OrderStatus.FILLED:
            # This block will be entered if _place_order_with_retry returned None
            # (e.g. max retries exhausted without non-APIError failure, unlikely with current logic)
            # OR if an APIError was caught above and short_order_result remained None,
            # OR if it returned an order that was not FILLED (e.g. CANCELED by exchange quickly).

            if (
                execution.status != ExecutionStatus.COMPENSATING
            ):  # If not already marked by APIError catch
                err_msg_detail = (
                    f"Status: {short_order_result.status.name}"
                    if short_order_result
                    else "No order object returned (likely due to retries or pre-APIError issue)"
                )
                err_msg = (
                    f"Execution {execution.id}: Short order placement failed or not filled. "
                    f"{err_msg_detail}"
                )
                logger.error(err_msg)
                if execution.error_message is None:  # Only set if not already set by APIError
                    execution.error_message = err_msg
                execution.status = ExecutionStatus.COMPENSATING

            logger.info(f"Execution {execution.id}: Attempting to compensate long leg...")
            compensated = False
            try:
                compensated = await self._compensate_position(
                    execution=execution,
                    exchange_id=opportunity.long_exchange,
                    symbol=long_symbol,
                    side=OrderSide.SELL,  # Compensate by selling the long
                    quantity=base_asset_quantity_long,
                )
            except Exception as e_comp:
                logger.critical(
                    f"Execution {execution.id}: Exception during _compensate_position call: "
                    f"{e_comp}",
                    exc_info=True,
                )
                execution.error_message = (
                    (
                        execution.error_message
                        + " | Compensation attempt raised error: "
                        + str(e_comp)
                    )
                    if execution.error_message
                    else "Compensation attempt raised error: " + str(e_comp)
                )

            if compensated:
                logger.info(f"Execution {execution.id}: Compensation of long leg successful.")
                execution.error_message = (
                    (execution.error_message + " | Long leg compensated.")
                    if execution.error_message
                    else "Long leg compensated."
                )
            else:
                logger.critical(
                    f"Execution {execution.id}: COMPENSATION OF LONG LEG FAILED! "
                    f"Manual intervention required for {long_symbol} on "
                    f"{opportunity.long_exchange}."
                )
                execution.error_message = (
                    (execution.error_message + " | COMPENSATION FAILED!")
                    if execution.error_message
                    else "COMPENSATION FAILED!"
                )
            execution.status = ExecutionStatus.FAILED  # Final status is FAILED regardless of comp.
            return

        # This part is only reached if short_order_result IS FILLED
        execution.short_order_id = short_order_result.exchange_order_id
        execution.short_order_response = short_order_result.model_dump(mode="json")
        execution.short_fill_price = short_order_result.average_fill_price
        execution.short_fill_quantity = short_order_result.quantity_filled
        logger.info(
            f"Execution {execution.id}: Short order placed and filled: {execution.short_order_id}"
        )

        await self._handle_filled_order(
            execution,
            short_order_result,
            opportunity.short_exchange,
            is_long_leg=False,
            expected_short_price=opportunity.short_price,
        )

        # Both legs successful
        execution.status = ExecutionStatus.COMPLETED  # Or PENDING_MONITORING if separate
        logger.info(f"Execution {execution.id}: Both legs placed and filled successfully.")
        # Monitoring and PNL calculation might happen outside this method after it returns

        if execution.status == ExecutionStatus.COMPLETED:
            if self.circuit_breaker_system:
                # Record success for both exchanges involved in the opportunity
                self.circuit_breaker_system.record_api_success(
                    opportunity.long_exchange,
                    context=(
                        f"Execution {execution.id} completed for long leg on "
                        f"{opportunity.long_exchange}"
                    ),
                )
                self.circuit_breaker_system.record_api_success(
                    opportunity.short_exchange,
                    context=(
                        f"Execution {execution.id} completed for short leg on "
                        f"{opportunity.short_exchange}"
                    ),
                )

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
        time_in_force: TimeInForce = TimeInForce.GTC,
        reduce_only: bool = False,
        post_only: bool = False,
        is_long_leg: bool = True,
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
            reduce_only: True if this is a reduce_only order

        Returns:
            Order object if successful, None otherwise.
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            execution.error_message = f"No API client for {exchange_id}"
            logger.error(f"Execution {execution.id}: {execution.error_message}")
            return None

        context = "placing compensation order" if reduce_only else "placing order"
        client_order_id = f"cde_{execution.id[:8]}_{exchange_id[:3]}_{str(uuid.uuid4())[:8]}"

        last_api_error_for_reraise: APIError | None = None

        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Execution {execution.id} ({context}): Attempt {attempt + 1} - "
                    f"{side.name} {quantity:.8f} {symbol} on {exchange_id} "
                    f"(Client ID: {client_order_id})"
                )

                # Create PlaceOrderArgs object for the API call
                place_order_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    time_in_force=time_in_force,
                    price=price,
                    client_order_id=client_order_id,
                    reduce_only=reduce_only,
                    post_only=post_only,
                )

                order_result = await client.place_order(place_order_args)
                logger.info(
                    f"Execution {execution.id}: Order placed successfully on {exchange_id}. "
                    f"Exchange ID: {order_result.exchange_order_id}, Status: {order_result.status}"
                )
                # Record success with circuit breaker if configured
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_api_success(
                        exchange_id, context=f"Order {order_result.exchange_order_id} placed"
                    )
                return order_result
            except APIError as e:
                last_api_error_for_reraise = e  # Store the API error
                should_retry = await self._handle_api_error(e, exchange_id, context)
                if not should_retry:
                    execution.error_message = (
                        f"Non-retryable API error during {context} on {exchange_id}: {e.message}"
                    )
                    logger.error(f"Execution {execution.id}: {execution.error_message}")
                    raise  # Re-raise current exception (e)
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

        # If loop finishes, it means all retries were exhausted for an APIError,
        # or another exception occurred.
        # The error_message should already be set by the last attempt or the
        # unexpected exception block.
        # If the reason for exhausting retries was specifically an APIError,
        # re-raise it as per test expectation.
        if last_api_error_for_reraise:
            # Ensure error_message reflects this final attempt if not already set by
            # non-retryable path
            if not execution.error_message or "Non-retryable" not in execution.error_message:
                execution.error_message = (
                    f"Failed to place order on {exchange_id} after "
                    f"{self.max_retries} retries due to: {last_api_error_for_reraise.message}"
                )
            logger.error(
                f"Execution {execution.id}: Exhausted retries. Last API error: "
                f"{last_api_error_for_reraise}"
            )
            raise last_api_error_for_reraise
        else:
            # This path should ideally not be hit if an APIError occurred and was stored.
            # If it's another exception, it would have been raised or returned None from the loop.
            # If it's just max_retries without a specific APIError stored
            # (e.g. unexpected error returned None),
            # set a generic message if not already set.
            if not execution.error_message:
                execution.error_message = (
                    f"Failed to place order on {exchange_id} after "
                    f"{self.max_retries} retries (unknown reason)."
                )
            logger.error(f"Execution {execution.id}: {execution.error_message}")

        return None  # Fallback, though raising last_api_error_for_reraise is preferred if it exists

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
                        f"Execution {execution.id}: Got status for order {order_id}: "
                        f"{order_status.status}"
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
                        # Use record_api_error method from CircuitBreakerSystem
                        self.circuit_breaker_system.record_api_error(
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
            f"Execution {execution.id}: Failed to get status for {order_id} after "
            f"{self.max_retries} retries."
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
        logger.info(
            f"_compensate_position: Attempting to compensate {quantity} {symbol} "
            f"on {exchange_id} with {side.name} order for execution {execution.id}"
        )
        # Configuration values from AppSettings
        compensation_config = self.app_settings.execution.compensation
        use_limit_orders_config = compensation_config.use_limit_orders
        limit_price_offset_pct = compensation_config.limit_price_offset_pct

        # Log the config values being used AFTER they are defined
        logger.info(
            f"_compensate_position: use_limit_orders_config = {use_limit_orders_config} "
            f"(type: {type(use_limit_orders_config)})"
        )
        logger.info(
            f"_compensate_position: limit_price_offset_pct = {limit_price_offset_pct} "
            f"(type: {type(limit_price_offset_pct)})"
        )

        # reduce_only = True  # Compensation orders should always be reduce_only # Unused variable
        order_type = OrderType.MARKET
        price = None

        if use_limit_orders_config:
            # This block determines if a LIMIT order should be used for compensation
            try:
                # Get ticker to calculate limit price
                ticker = await self.api_clients[exchange_id].get_ticker(symbol)
                if ticker:
                    if side == OrderSide.BUY and ticker.bid:
                        price = ticker.bid * (Decimal(1) + limit_price_offset_pct)
                        order_type = OrderType.LIMIT
                    elif side == OrderSide.SELL and ticker.ask:
                        price = ticker.ask * (Decimal(1) - limit_price_offset_pct)
                        order_type = OrderType.LIMIT
                    else:
                        logger.warning(
                            f"Execution {execution.id}: Could not determine limit price for "
                            f"compensation on {exchange_id} for {symbol}. Missing bid/ask."
                        )
                else:
                    logger.warning(
                        f"Execution {execution.id}: Could not get ticker for "
                        f"{exchange_id} {symbol} to calculate compensation limit price."
                    )
            except (InvalidOperation, APIError, ValueError) as e:
                logger.warning(
                    f"Execution {execution.id}: Error processing limit price for compensation "
                    f"on {exchange_id} for {symbol}: {e}. Defaulting to MARKET."
                )
                order_type = OrderType.MARKET  # Fallback to MARKET on error
                price = None
        # If not using limit orders or if there was an issue, it remains MARKET with price=None

        compensation_order = await self._place_order_with_retry(
            execution,
            exchange_id,
            symbol,
            side,
            quantity,
            order_type,
            price,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,  # CRITICAL for compensation
        )

        if compensation_order:
            logger.info(
                f"Execution {execution.id}: Compensation order placed: "
                f"{compensation_order.exchange_order_id}"
            )
            # Basic check: If status is already filled, assume compensation worked
            # A more robust check would monitor the compensation order status
            if compensation_order.status == OrderStatus.FILLED:
                logger.info(f"Execution {execution.id}: Compensation order filled immediately.")
                return True
            # If not filled immediately, we assume it might fill. A better implementation
            # would monitor this order's status properly.
            logger.warning(
                f"Execution {execution.id}: Compensation order "
                f"{compensation_order.exchange_order_id} "
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
        Handles a filled order, updating execution details and portfolio.
        Centralized logic for processing fills for both initial and compensation orders.
        """
        logger.info(
            f"[{exchange_id}] Handling filled order "
            f"{order.exchange_order_id or order.client_order_id} for "
            f"execution {execution.id}. is_long_leg: {is_long_leg}"
        )

        if order.status != OrderStatus.FILLED:
            logger.warning(
                f"Execution {execution.id}: Unexpected order status: {order.status} for "
                f"filled order {order.exchange_order_id or order.client_order_id} on {exchange_id}"
            )
            return

        # After confirming FILLED status, average_fill_price and quantity_filled should be valid.
        # Add checks for robustness and to satisfy type checkers.
        if (
            order.average_fill_price is None
            # order.quantity_filled is None, # This check is redundant as quantity_filled is Decimal
            or order.quantity_filled == Decimal(0)  # quantity_filled can be 0 if not filled.
        ):
            logger.error(
                f"Execution {execution.id}: Filled order "
                f"{order.exchange_order_id or order.client_order_id} on {exchange_id} has missing "
                f"avg_fill_price ({order.average_fill_price}) or zero quantity_filled "
                f"({order.quantity_filled})."  # Simplified qty check
            )
            execution.status = ExecutionStatus.FAILED
            execution.error_message = (
                f"Filled order {order.exchange_order_id or order.client_order_id} had "
                f"inconsistent data (price/qty)."
            )
            return

        # Update execution details with now-guaranteed Decimal values
        if is_long_leg:
            execution.long_fill_price = order.average_fill_price
            execution.long_fill_quantity = order.quantity_filled
        else:
            execution.short_fill_price = order.average_fill_price
            execution.short_fill_quantity = order.quantity_filled

        # Check slippage (using guaranteed Decimal for order.average_fill_price)
        if is_long_leg and expected_long_price is not None:
            if order.average_fill_price > expected_long_price * (Decimal(1) + self.max_slippage):
                logger.warning(
                    f"Slippage detected for long leg of execution {execution.id}: "
                    f"Fill price {order.average_fill_price} > Expected {expected_long_price}"
                )
        elif not is_long_leg and expected_short_price is not None:
            if order.average_fill_price < expected_short_price * (Decimal(1) - self.max_slippage):
                logger.warning(
                    f"Slippage detected for short leg of execution {execution.id}: "
                    f"Fill price {order.average_fill_price} < Expected {expected_short_price}"
                )

        # Process trades associated with the order
        if order.trades:  # order.trades is list[Trade]
            logger.info(
                f"Execution {execution.id}: Processing {len(order.trades)} trade(s) from order "
                f"{order.exchange_order_id or order.client_order_id}."
            )
            for trade_from_order in order.trades:
                try:
                    # Ensure the trade from order has the correct exchange_id if not already set or
                    # different.
                    # This might be redundant if mappers always set it correctly.
                    # For now, we assume portfolio_tracker uses the exchange_id passed to it.
                    if trade_from_order.exchange != exchange_id:
                        logger.warning(
                            f"Trade {trade_from_order.id} on order "
                            f"{order.exchange_order_id or order.client_order_id} has exchange "
                            f"'{trade_from_order.exchange}', but handling context is for "
                            f"'{exchange_id}'. Passing '{exchange_id}' to portfolio_tracker."
                        )

                    await self.portfolio_tracker.process_trade(exchange_id, trade_from_order)
                    logger.info(
                        f"Processed trade {trade_from_order.id} from order "
                        f"{order.exchange_order_id or order.client_order_id} for exec "
                        f"{execution.id}"
                    )
                except Exception as e_process_trade:
                    logger.error(
                        f"Execution {execution.id}: Failed to process trade {trade_from_order.id} "
                        f"from order {order.exchange_order_id or order.client_order_id} on "
                        f"{exchange_id}: {e_process_trade}",
                        exc_info=True,
                    )
                    execution.status = ExecutionStatus.FAILED
                    execution.error_message = (
                        f"Failed to process constituent trade {trade_from_order.id} for order "
                        f"{order.exchange_order_id or order.client_order_id}."
                    )
                    return  # Stop further processing for this order if a trade fails.
        elif order.quantity_filled > Decimal(0):
            # Fallback: Order is FILLED, has aggregate fill data, but no individual trades.
            # Create a synthetic Trade object.
            logger.info(
                f"Execution {execution.id}: Order "
                f"{order.exchange_order_id or order.client_order_id} is FILLED with aggregate data "
                f"but no individual trades. Creating a synthetic trade."
            )
            try:
                # Ensure average_fill_price and quantity_filled are not None before use,
                # which is guaranteed by the elif condition.
                synthetic_trade = Trade(
                    id=f"synth_{order.exchange_order_id or order.client_order_id}_"
                    f"{uuid.uuid4().hex[:8]}",
                    symbol=self.symbol_mapper.get_internal_symbol(order.symbol, exchange_id)
                    or order.symbol,
                    side=order.side,
                    order_id=str(order.exchange_order_id or order.client_order_id),
                    exchange=exchange_id,
                    client_order_id=order.client_order_id,
                    price=order.average_fill_price,  # Not None here
                    quantity=order.quantity_filled,  # Not None here
                    fee=Decimal(
                        "0.0"
                    ),  # Default to 0.0 for synthetic trade if Mypy complains about None
                    fee_asset=None,  # Cannot get from Order model; Trade model allows None
                    executed_at=order.updated_at or datetime.now(UTC),
                    is_maker=False,  # Default for synthetic trade; Trade model defaults to False
                )
                await self.portfolio_tracker.process_trade(exchange_id, synthetic_trade)
                logger.info(
                    f"Synthetic trade processed for order {synthetic_trade.order_id}, "
                    f"exec {execution.id}"
                )
            except Exception as e_synth_trade:
                logger.error(
                    f"Execution {execution.id}: Failed to create or process synthetic Trade for "
                    f"order {order.exchange_order_id or order.client_order_id} on {exchange_id}: "
                    f"{e_synth_trade}",
                    exc_info=True,
                )
                execution.status = ExecutionStatus.FAILED
                execution.error_message = (
                    f"Failed to process synthetic trade for order "
                    f"{order.exchange_order_id or order.client_order_id}."
                )
                return
        else:
            # Order is FILLED, but no trades and no/invalid aggregate fill price/quantity to
            # create a synthetic trade.
            # This is an inconsistent state. (Handled by the check above for avg_price None or
            # qty 0)
            # This specific 'else' branch might be less likely to be hit if the prior check
            # for (order.average_fill_price is None or order.quantity_filled == Decimal(0))
            # already caught the inconsistent state for a FILLED order.
            # However, keeping a log here for any edge cases.
            logger.error(
                f"Execution {execution.id}: Order "
                f"{order.exchange_order_id or order.client_order_id} is FILLED but has no trades "
                f"and insufficient/invalid aggregate data (avg_price: {order.average_fill_price}, "
                f"qty_filled: {order.quantity_filled}) to process. This is an inconsistent state."
            )
            # The error message and status would have been set by the earlier check if
            # avg_fill_price was None or quantity_filled was zero.
            # If execution reaches here, it implies order.status was FILLED,
            # order.trades was empty, AND (average_fill_price was None OR quantity_filled was <=0)
            # The primary inconsistent state check for FILLED orders is now before this block.
            if (
                execution.status != ExecutionStatus.FAILED
            ):  # Only update if not already set to FAILED
                execution.status = ExecutionStatus.FAILED
                execution.error_message = (
                    f"FILLED Order {order.exchange_order_id or order.client_order_id} has "
                    f"inconsistent data (no trades and invalid aggregate fill info)."
                )
            return

        logger.info(f"Finished handling filled order for execution {execution.id}")

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
                f"Execution {execution.id}: Cannot monitor order on {exchange_id}, "
                f"order ID is None."
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
                        f"Execution {execution.id}: Order {order_id} reached terminal "
                        f"state: {order.status}"
                    )
                    # Process the final state (e.g., record fill)
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
                    f"Execution {execution.id}: Failed to get status for order {order_id}. "
                    f"Assuming failure."
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
